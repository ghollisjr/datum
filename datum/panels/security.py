"""Security panel — server logins, roles, and database user mapping.

MSSQL separates server-level logins from per-database users; PostgreSQL
has a single role that may or may not be able to log in.  Both are
handled here through the driver's "principal" methods.

Passwords reach this module in the form payload.  They are written into
generated DDL, so they are masked out of anything shown back to the user
and never included in a preview.

Sub-panels:
    security       — Login/role list (default)
    form           — Create/edit wizard
    user-mappings  — Databases a login is a user in (MSSQL)
"""

import base64
import binascii
import json
import re

# Field keys whose values must never be echoed back to the user.
_SECRET_KEYS = ("password",)

_SECRET_MASK = "********"


def _decode_payload(args):
    """Decode a base64-encoded JSON form payload sent from Emacs."""
    if not args:
        raise ValueError("no form data submitted")
    try:
        raw = base64.b64decode(args[0], validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as err:
        raise ValueError(f"could not decode form payload: {err}") from err
    try:
        return json.loads(raw)
    except json.JSONDecodeError as err:
        raise ValueError(f"invalid form JSON: {err}") from err


def _clean_fields(fields):
    """Drop descriptor keys with no value, so the payload stays small."""
    return [{k: v for k, v in spec.items() if v is not None}
            for spec in fields]


def _db_error(err):
    """Render a driver exception as a single readable line."""
    args = getattr(err, "args", ())
    message = args[1] if len(args) > 1 and isinstance(args[1], str) else str(err)
    message = message.lstrip()
    while message.startswith("["):
        _, _, message = message.partition("]")
        message = message.lstrip()
    message = message.replace("\\n", "\n").split("\n", 1)[0]
    message = re.sub(r"\s*\(\d+\)\s*\(SQL\w+\)\s*$", "", message)
    return " ".join(message.split()).rstrip(";")[:300] or str(err)[:300]


def _mask_secrets(text, opts):
    """Replace any secret value from OPTS wherever it appears in TEXT."""
    for key in _SECRET_KEYS:
        secret = opts.get(key)
        if secret:
            text = text.replace(str(secret), _SECRET_MASK)
    return text


def _refresh_list(cursor, driver):
    """Re-send the principal list so the panel reflects the change.

    Sub-panels carry no auto-refresh timer, so without this the buffer
    keeps showing what was true before the action.
    """
    from .. import envelope

    try:
        envelope.admin_panel(get_data(cursor, driver, []))
    except Exception:
        # A refresh failing must not mask the action that succeeded.
        pass


def _refresh_mappings(cursor, driver, login):
    """Re-send the user-mapping panel for LOGIN."""
    from .. import envelope

    try:
        envelope.admin_panel(_mapping_panel(cursor, driver, login))
    except Exception:
        pass


def _refresh_permissions(cursor, driver, principal, database):
    """Re-send the permissions panel for PRINCIPAL."""
    from .. import envelope

    try:
        envelope.admin_panel(
            _permissions_panel(cursor, driver, principal, database))
    except Exception:
        pass


def _commit(cursor):
    try:
        cursor.connection.commit()
    except Exception:
        pass


def get_data(cursor, driver, args):
    """Return the principal list as a panel result dict."""
    if not driver.supports_security:
        return {
            "panel": "security",
            "headers": ["note"],
            "rows": [[f"Login management is not supported on "
                      f"{driver.dialect_name}"]],
            "row_id": None,
            "actions": [],
            "info": None,
        }

    if args and args[0] == "mappings" and len(args) >= 2:
        return _mapping_panel(cursor, driver, " ".join(args[1:]))

    headers, rows = driver.list_principals(cursor)
    noun = driver.principal_noun
    actions = [
        {"key": "N", "label": f"New {noun}", "command": "new-principal"},
        {"key": "E", "label": f"Edit {noun}", "command": "edit-principal"},
        {"key": "D", "label": f"Drop {noun}", "command": "drop-check"},
    ]
    if driver.supports_user_mapping:
        actions.append({"key": "U", "label": "Database users",
                        "command": "mappings"})
    if driver.supports_object_permissions:
        actions.append({"key": "P", "label": "Permissions",
                        "command": "permissions"})
    return {
        "panel": "security",
        "headers": headers,
        "rows": rows,
        "row_id": 0,
        "actions": actions,
        "info": None,
    }


def run_action(cursor, driver, action_name, args):
    """Execute a security action."""
    from .. import envelope

    if not driver.supports_security:
        envelope.error(f"Login management is not supported on "
                       f"{driver.dialect_name}")
        return

    try:
        if action_name == "new-principal":
            _principal_form(cursor, driver, args, editing=False)
        elif action_name == "edit-principal":
            _principal_form(cursor, driver, args, editing=True)
        elif action_name in ("create-principal", "alter-principal"):
            _apply_principal(cursor, driver, args, action_name)
        elif action_name in ("preview-create", "preview-alter"):
            _preview_principal(cursor, driver, args, action_name)
        elif action_name == "drop-check":
            _drop_check(cursor, driver, args)
        elif action_name == "drop-principal":
            _drop_principal(cursor, driver, args)
        elif action_name == "mappings":
            envelope.admin_panel(
                _mapping_panel(cursor, driver, " ".join(args)))
        elif action_name == "new-mapping":
            _mapping_form(cursor, driver, args)
        elif action_name == "add-mapping":
            _add_mapping(cursor, driver, args)
        elif action_name == "edit-mapping":
            _mapping_roles_form(cursor, driver, args)
        elif action_name == "set-mapping-roles":
            _set_mapping_roles(cursor, driver, args)
        elif action_name == "remove-mapping":
            _remove_mapping(cursor, driver, args)
        elif action_name == "permissions":
            _permissions(cursor, driver, args)
        elif action_name == "new-permission":
            _permission_scope_form(cursor, driver, args)
        elif action_name == "permission-edit":
            _permission_form(cursor, driver, args)
        elif action_name == "set-permissions":
            _set_permissions(cursor, driver, args)
        elif action_name == "revoke-permission":
            _revoke_permission(cursor, driver, args)
        else:
            envelope.error(f"Unknown security action: {action_name}")
    except ValueError as err:
        envelope.error(f":admin security {action_name} — {err}")
    except Exception as err:
        envelope.error(f":admin security {action_name} — {_db_error(err)}")


# --- Logins and roles ---

def _principal_form(cursor, driver, args, editing):
    """Send the create or edit form for a login/role."""
    from .. import envelope

    payload = {}
    name = ""
    if args:
        try:
            payload = _decode_payload(args)
            name = (payload.get("name_original")
                    or (payload.get("values") or {}).get("name_original")
                    or "")
        except ValueError:
            name = " ".join(args)
        if not name and not editing:
            name = ""

    current = None
    if editing:
        if not name:
            envelope.error(f"no {driver.principal_noun} given")
            return
        driver.validate_identifier(name)
        current = driver.principal_settings(cursor, name)
        if not current:
            envelope.error(f"{driver.principal_noun.capitalize()} not found: "
                           f"{name}")
            return

    values = dict(payload.get("values") or {})
    if editing:
        values["name_original"] = name
    # A password is never sent back out, so an edit form always starts
    # blank regardless of what was typed before.
    values.pop("password", None)

    noun = driver.principal_noun
    envelope.admin_panel({
        "panel": "security",
        "sub_panel": "form",
        "title": (f"Edit {noun}: {name}" if editing else f"New {noun}"),
        "form": {
            "fields": _clean_fields(driver.principal_options(cursor, current)),
            "values": values,
            "submit_action": ("alter-principal" if editing
                              else "create-principal"),
            "submit_label": "Apply Changes" if editing else f"Create {noun}",
            "preview_action": ("preview-alter" if editing
                               else "preview-create"),
            "notes": (["Only the settings you change are applied.",
                       "Leave the password blank to keep the current one."]
                      if editing else []),
        },
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
        "context": {"principal": name},
    })


def _coerce(driver, cursor, opts, current):
    """Coerce the submitted form against its descriptors."""
    specs = {f["key"]: f for f in driver.principal_options(cursor, current)}
    for key, spec in specs.items():
        value = opts.get(key)
        if spec.get("required") and not str(value or "").strip():
            raise ValueError(f"{spec['label']} is required")
        if spec.get("type") == "int" and value not in (None, ""):
            try:
                opts[key] = int(value)
            except (TypeError, ValueError):
                raise ValueError(
                    f"{spec['label']} must be a number "
                    f"(got {value!r})") from None
        if spec.get("type") == "multi":
            opts[key] = list(value or [])
    return opts


def _principal_statements(cursor, driver, args, action_name):
    """Return (name, statements, opts) for the submitted form."""
    opts = _decode_payload(args)
    if action_name.endswith("alter"):
        name = (opts.get("name_original") or "").strip()
        if not name:
            raise ValueError(
                f"the form did not say which {driver.principal_noun} to alter")
        driver.validate_identifier(name)
        current = driver.principal_settings(cursor, name)
        if not current:
            raise ValueError(f"{driver.principal_noun} not found: {name}")
        opts = _coerce(driver, cursor, opts, current)
        return name, driver.sql_alter_principal(name, opts, current), opts
    opts = _coerce(driver, cursor, opts, None)
    name = opts.get("name", "")
    driver.validate_identifier(name)
    return name, driver.sql_create_principal(opts), opts


def _preview_principal(cursor, driver, args, action_name):
    """Show the statements without running them, with the password masked."""
    from .. import envelope

    name, stmts, opts = _principal_statements(
        cursor, driver, args,
        "alter" if action_name.endswith("alter") else "create")
    if not stmts:
        envelope.info(f"No changes to apply to {name}")
        return
    text = _mask_secrets(";\n\n".join(stmts) + ";", opts)
    envelope.definition(f"{driver.principal_noun.capitalize()}: {name}", text)


def _apply_principal(cursor, driver, args, action_name):
    """Create or alter a login/role."""
    from .. import envelope

    creating = action_name == "create-principal"
    name, stmts, opts = _principal_statements(
        cursor, driver, args, "create" if creating else "alter")
    if not stmts:
        envelope.info(f"No changes to apply to {name}")
        return

    for index, sql in enumerate(stmts):
        try:
            cursor.execute(sql)
            _commit(cursor)
        except Exception as err:
            # The failing statement may embed the password, so it is not
            # quoted back the way the database panel does.
            envelope.error(
                f"Applied {index} of {len(stmts)} statement(s) for "
                f"'{name}' before failing — "
                f"{_mask_secrets(_db_error(err), opts)}")
            return
    verb = "Created" if creating else "Updated"
    envelope.info(f"{verb} {driver.principal_noun}: {name}")
    _refresh_list(cursor, driver)


def _drop_check(cursor, driver, args):
    """Show the drop confirmation, with the principal's session count."""
    from .. import envelope

    if not args:
        envelope.error(f"drop requires a {driver.principal_noun} name")
        return
    name = " ".join(args)
    driver.validate_identifier(name)
    noun = driver.principal_noun

    sessions = -1
    try:
        sql, params = driver.sql_principal_sessions(name)
        cursor.execute(sql, params)
        row = cursor.fetchone()
        sessions = int(row[0]) if row else 0
    except Exception:
        sessions = -1

    notes = [f"{noun.capitalize()}: {name}"]
    notes.append(f"Active sessions: {sessions}" if sessions >= 0
                 else "Active sessions: (could not determine)")
    if sessions > 0:
        notes += ["",
                  f"A {noun} holding a live connection cannot be dropped "
                  f"unless you enable Force below."]

    envelope.admin_panel({
        "panel": "security",
        "sub_panel": "form",
        "title": f"Drop {noun}: {name}",
        "form": {
            "fields": [
                {"key": "force", "label": "Force", "type": "bool",
                 "default": sessions > 0,
                 "help": f"end the {noun}'s sessions first"},
            ],
            "values": {"name": name},
            "submit_action": "drop-principal",
            "submit_label": f"Drop {noun}",
            "notes": notes,
            "confirm_text": name,
            "danger": True,
        },
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
        "context": {"principal": name},
    })


def _drop_principal(cursor, driver, args):
    """Drop a login/role, optionally ending its sessions first."""
    from .. import envelope

    if not args:
        envelope.error(f"drop requires a {driver.principal_noun} name")
        return
    try:
        opts = _decode_payload(args)
        name = (opts.get("name") or "").strip()
        force = bool(opts.get("force"))
    except ValueError:
        # Also usable directly with a bare name, for scripting.
        name, force = " ".join(args), False
    if not name:
        envelope.error(f"drop requires a {driver.principal_noun} name")
        return
    driver.validate_identifier(name)

    for sql in driver.sql_drop_principal(name, force=force):
        cursor.execute(sql)
        _commit(cursor)
    envelope.info(f"Dropped {driver.principal_noun}: {name}")
    _refresh_list(cursor, driver)


# --- Database user mapping ---

def _mapping_panel(cursor, driver, login):
    """Return the panel listing databases a login is a user in."""
    if not driver.supports_user_mapping:
        return {
            "panel": "security",
            "headers": ["note"],
            "rows": [[f"{driver.dialect_name} has no separate database "
                      f"users: a role that can log in is already both"]],
            "row_id": None,
            "actions": [],
            "info": None,
        }
    login = login.strip()
    driver.validate_identifier(login)
    headers, rows = driver.list_user_mappings(cursor, login)
    return {
        "panel": "security",
        "sub_panel": "user-mappings",
        "title": f"Database Users: {login}",
        "headers": headers,
        "rows": rows,
        "row_id": 0,
        "actions": [
            {"key": "N", "label": "Map to database", "command": "new-mapping"},
            {"key": "E", "label": "Edit roles", "command": "edit-mapping"},
            {"key": "D", "label": "Remove mapping",
             "command": "remove-mapping"},
        ] + ([{"key": "P", "label": "Permissions in this database",
               "command": "permissions"}]
             if driver.supports_object_permissions else []),
        "info": f"Databases {login} is a user in",
        "parent_panel": "security",
        "context": {"login": login},
    }


def _mapping_form(cursor, driver, args):
    """Send the form for mapping a login into a database."""
    from .. import envelope

    payload = _decode_payload(args) if args else {}
    login = (payload.get("login")
             or (payload.get("values") or {}).get("login") or "")
    if not login:
        envelope.error("no login given")
        return
    driver.validate_identifier(login)

    envelope.admin_panel({
        "panel": "security",
        "sub_panel": "form",
        "title": f"Map {login} to a Database",
        "form": {
            "fields": _clean_fields(
                driver.user_mapping_options(cursor, login)),
            "values": {"login": login},
            "submit_action": "add-mapping",
            "submit_label": "Create User",
            "notes": [f"Creates a database user for login {login}."],
        },
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
        "context": {"login": login},
    })


def _add_mapping(cursor, driver, args):
    """Create a database user for a login."""
    from .. import envelope

    opts = _decode_payload(args)
    login = (opts.get("login") or "").strip()
    database = (opts.get("database") or "").strip()
    if not (login and database):
        envelope.error("add-mapping requires a login and a database")
        return
    driver.validate_identifier(login)
    driver.validate_identifier(database)

    for sql in driver.sql_add_user_mapping(login, opts):
        cursor.execute(sql)
        _commit(cursor)
    envelope.info(f"Mapped {login} into {database}")
    _refresh_mappings(cursor, driver, login)


def _mapping_roles_form(cursor, driver, args):
    """Edit a user's roles in one database.

    Roles are per-database, so they cannot be offered when the mapping is
    first created — the database is not chosen yet.  This is the step
    where it is known.
    """
    from .. import envelope

    payload = _decode_payload(args) if args else {}
    login = (payload.get("login") or "").strip()
    database = (payload.get("database") or "").strip()
    if not (login and database):
        envelope.error("edit-mapping requires a login and a database")
        return
    driver.validate_identifier(login)
    driver.validate_identifier(database)

    # The user may be named differently from the login it maps to.
    username = login
    _headers, rows = driver.list_user_mappings(cursor, login)
    for row in rows:
        if row[0] == database and row[1]:
            username = row[1]
            break

    current = driver.user_roles(cursor, database, username)
    envelope.admin_panel({
        "panel": "security",
        "sub_panel": "form",
        "title": f"Roles for {username} in {database}",
        "form": {
            "fields": _clean_fields(
                driver.user_role_options(cursor, database, current)),
            "values": {"login": login, "database": database,
                       "username": username},
            "submit_action": "set-mapping-roles",
            "submit_label": "Apply Roles",
            "notes": [f"Database roles held by {username} in {database}."],
        },
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
        "context": {"login": login},
    })


def _set_mapping_roles(cursor, driver, args):
    """Reconcile a user's roles in one database."""
    from .. import envelope

    opts = _decode_payload(args)
    database = (opts.get("database") or "").strip()
    username = (opts.get("username") or opts.get("login") or "").strip()
    if not (database and username):
        envelope.error("the form did not say which user to change")
        return
    driver.validate_identifier(database)
    driver.validate_identifier(username)

    current = driver.user_roles(cursor, database, username)
    wanted = list(opts.get("roles") or [])
    stmts = driver.sql_set_user_roles(database, username, wanted, current)
    if not stmts:
        envelope.info(f"No role changes for {username} in {database}")
        return
    for sql in stmts:
        cursor.execute(sql)
        _commit(cursor)
    envelope.info(f"Updated roles for {username} in {database}")
    _refresh_mappings(cursor, driver, (opts.get("login") or username))


def _remove_mapping(cursor, driver, args):
    """Drop a login's database user."""
    from .. import envelope

    payload = _decode_payload(args) if args else {}
    login = (payload.get("login") or "").strip()
    database = (payload.get("database") or "").strip()
    if not (login and database):
        envelope.error("remove-mapping requires a login and a database")
        return
    driver.validate_identifier(login)
    driver.validate_identifier(database)

    for sql in driver.sql_remove_user_mapping(login, database):
        cursor.execute(sql)
        _commit(cursor)
    envelope.info(f"Removed {login} from {database}")
    _refresh_mappings(cursor, driver, login)


# --- Object-level permissions ---

# The panel shows a scope by name; the driver takes the key.
_SCOPE_KEYS = {"Server": "server", "Database": "database",
               "Schema": "schema", "Object": "object"}


def _row_scope(scope_label, column):
    """Return the driver's scope key for a row of the permissions panel.

    A row carrying a column name is a grant on that column, however the
    catalog classes it.
    """
    if column:
        return "column"
    return _SCOPE_KEYS.get(scope_label, scope_label.lower())


def _require_permissions(driver):
    from .. import envelope

    if not driver.supports_object_permissions:
        envelope.error(f"Permissions are not supported on "
                       f"{driver.dialect_name}")
        return False
    return True


def _permissions_panel(cursor, driver, principal, database=None):
    """Return the panel listing what PRINCIPAL may do."""
    principal = principal.strip()
    driver.validate_identifier(principal)
    if database:
        driver.validate_identifier(database)
    headers, rows = driver.list_permissions(cursor, principal, database)
    where = f"in {database}" if database else driver.permission_root_label
    info = f"Permissions held by {principal} {where}".strip()
    if not rows:
        info = f"{principal} holds no permissions {where}".strip()
    elif any(r[0] == "REVOKE" for r in rows):
        # Revoking one column of a table that is granted as a whole
        # leaves a REVOKE row behind, and that row is what keeps the
        # column out of the grant.  It is state, not leftover noise.
        info += ("  A REVOKE row excludes that column from a grant held "
                 "on the whole object.")
    return {
        "panel": "security",
        "sub_panel": "permissions",
        "title": f"Permissions: {principal}"
                 + (f" in {database}" if database else ""),
        "headers": headers,
        "rows": rows,
        # The securable is what the cursor should stay on across a
        # refresh; several rows share one, which is close enough.
        "row_id": 3,
        "actions": [
            {"key": "G", "label": "Grant or deny", "command": "new-permission"},
            {"key": "R", "label": "Revoke this one",
             "command": "revoke-permission"},
        ],
        "info": info,
        "parent_panel": "security",
        "context": {"principal": principal, "database": database or ""},
    }


def _permissions(cursor, driver, args):
    """Show the permissions panel for a principal."""
    from .. import envelope

    if not _require_permissions(driver):
        return
    payload = _decode_payload(args) if args else {}
    if not payload:
        # Sent as a bare name from the principal list.
        payload = {"principal": " ".join(args)}
    principal = (payload.get("principal") or "").strip()
    database = (payload.get("database") or "").strip() or None
    if not principal:
        envelope.error("permissions requires a principal")
        return
    envelope.admin_panel(
        _permissions_panel(cursor, driver, principal, database))


def _permission_scope_form(cursor, driver, args):
    """Ask which securable to change before offering its permissions.

    The permissions that mean anything depend on the scope — SELECT on a
    table, USAGE on a schema, CONNECT on a database — so the scope has
    to be settled before the list can be offered.
    """
    from .. import envelope

    if not _require_permissions(driver):
        return
    payload = _decode_payload(args) if args else {}
    principal = (payload.get("principal") or "").strip()
    database = (payload.get("database") or "").strip() or None
    if not principal:
        envelope.error("no principal given")
        return
    driver.validate_identifier(principal)

    scopes = driver.permission_scopes(database)
    if not scopes:
        envelope.error("nothing can be granted in this context")
        return
    # Every securable the scopes can name, so the field can complete
    # whichever scope is picked.
    securables = sorted({name
                         for scope, _label in scopes
                         for name in driver.securable_choices(
                             cursor, scope, database)})
    fields = [
        {"key": "scope", "label": "Scope", "type": "choice",
         "default": scopes[-1][0] if len(scopes) > 1 else scopes[0][0],
         "choices": [list(s) for s in scopes], "required": True},
        {"key": "securable", "label": "Securable",
         "type": "completing" if securables else "string",
         "default": "", "completions": securables or None,
         "help": "the schema or object to grant on; "
                 "leave empty for the server or database itself"},
        {"key": "column", "label": "Column", "type": "string",
         "default": "",
         "help": "only for the column scope"},
    ]
    envelope.admin_panel({
        "panel": "security",
        "sub_panel": "form",
        "title": f"Grant to {principal}",
        "form": {
            "fields": _clean_fields(fields),
            "values": {"principal": principal, "database": database or ""},
            "submit_action": "permission-edit",
            "submit_label": "Choose Permissions",
            "notes": ["The permissions on offer depend on the scope, so "
                      "this step settles the scope first."],
        },
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
        "context": {"principal": principal, "database": database or ""},
    })


def _permission_form(cursor, driver, args):
    """Offer the permissions valid at a scope, ticked as they stand."""
    from .. import envelope

    if not _require_permissions(driver):
        return
    opts = _decode_payload(args)
    principal = (opts.get("principal") or "").strip()
    database = (opts.get("database") or "").strip() or None
    scope = (opts.get("scope") or "").strip()
    securable = (opts.get("securable") or "").strip()
    column = (opts.get("column") or "").strip()
    if not (principal and scope):
        envelope.error("the form did not say what to change")
        return
    driver.validate_identifier(principal)
    if scope in ("server", "database") and not securable:
        securable = database or ""
    if scope not in ("server", "database") and not securable:
        envelope.error(f"the {scope} scope needs a securable")
        return
    if scope == "column" and not column:
        envelope.error("the column scope needs a column")
        return

    current = driver.current_permissions(cursor, principal, scope, securable,
                                         database, column or None)
    fields = driver.permission_options(scope, current)
    if not fields:
        envelope.error(f"nothing can be granted at the {scope} level")
        return
    target = securable or (database or "the server")
    if column:
        target = f"{target} ({column})"
    notes = [f"Permissions for {principal} on {target}.",
             "Ticked is granted; unticking revokes."]
    if driver.supports_deny:
        notes.append("Denying outranks any grant the principal gets from "
                     "a role, and wins over a tick in both lists.")
    envelope.admin_panel({
        "panel": "security",
        "sub_panel": "form",
        "title": f"Permissions: {principal} on {target}",
        "form": {
            "fields": _clean_fields(fields),
            "values": {"principal": principal, "database": database or "",
                       "scope": scope, "securable": securable,
                       "column": column},
            "submit_action": "set-permissions",
            "submit_label": "Apply",
            "notes": notes,
        },
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
        "context": {"principal": principal, "database": database or ""},
    })


def _set_permissions(cursor, driver, args):
    """Reconcile a principal's permissions on one securable."""
    from .. import envelope

    if not _require_permissions(driver):
        return
    opts = _decode_payload(args)
    principal = (opts.get("principal") or "").strip()
    database = (opts.get("database") or "").strip() or None
    scope = (opts.get("scope") or "").strip()
    securable = (opts.get("securable") or "").strip()
    column = (opts.get("column") or "").strip() or None
    if not (principal and scope):
        envelope.error("the form did not say what to change")
        return
    driver.validate_identifier(principal)

    current = driver.current_permissions(cursor, principal, scope, securable,
                                         database, column)
    stmts = driver.sql_set_permissions(
        principal, scope, securable,
        list(opts.get("granted") or []), list(opts.get("denied") or []),
        current, database, column)
    if not stmts:
        envelope.info(f"No permission changes for {principal}")
        _refresh_permissions(cursor, driver, principal, database)
        return
    for sql in stmts:
        cursor.execute(sql)
        _commit(cursor)
    envelope.info(f"Updated permissions for {principal} on "
                  f"{securable or database or 'the server'}")
    _refresh_permissions(cursor, driver, principal, database)


def _revoke_permission(cursor, driver, args):
    """Take back the single permission the cursor is on."""
    from .. import envelope

    if not _require_permissions(driver):
        return
    opts = _decode_payload(args)
    principal = (opts.get("principal") or "").strip()
    database = (opts.get("database") or "").strip() or None
    permission = (opts.get("permission") or "").strip()
    securable = (opts.get("securable") or "").strip()
    column = (opts.get("column") or "").strip() or None
    scope = _row_scope((opts.get("scope") or "").strip(), column)
    if not (principal and permission):
        envelope.error("revoke-permission requires a principal "
                       "and a permission")
        return
    driver.validate_identifier(principal)

    for sql in driver.sql_revoke_permission(principal, scope, securable,
                                            permission, database, column):
        cursor.execute(sql)
        _commit(cursor)
    envelope.info(f"Revoked {permission} on {securable} from {principal}")
    _refresh_permissions(cursor, driver, principal, database)
