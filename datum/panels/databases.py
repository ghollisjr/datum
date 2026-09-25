"""Database administration panel — create, drop, and inspect databases.

Supported on MSSQL and PostgreSQL.  Other dialects report the operation
as unsupported rather than emitting SQL that cannot work there.

Sub-panels:
    databases      — Database list (default)
    form           — Wizard form (create / drop confirmation)
"""

import base64
import binascii
import json
import re


def _decode_payload(args):
    """Decode a base64-encoded JSON form payload sent from Emacs.

    The form payload is base64-encoded on the wire because the command
    splitter does not understand backslash escapes, so a value holding a
    double quote would otherwise be split into several arguments.
    """
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


def get_data(cursor, driver, args):
    """Return the database list as a panel result dict."""
    if not driver.supports_database_ddl:
        return {
            "panel": "databases",
            "headers": ["note"],
            "rows": [[f"Database administration is not supported on "
                      f"{driver.dialect_name}"]],
            "row_id": None,
            "actions": [],
            "info": None,
        }
    return _database_list(cursor, driver)


def run_action(cursor, driver, action_name, args):
    """Execute a database action."""
    from .. import envelope

    if not driver.supports_database_ddl:
        envelope.error(f"Database administration is not supported on "
                       f"{driver.dialect_name}")
        return

    try:
        if action_name == "new-database":
            _new_database_form(cursor, driver, args)
        elif action_name == "browse-path":
            _browse_path(cursor, driver, args)
        elif action_name == "edit-database":
            _edit_database_form(cursor, driver, args)
        elif action_name == "alter-database":
            _alter_database(cursor, driver, args)
        elif action_name == "preview-alter":
            _preview_alter(cursor, driver, args)
        elif action_name == "create-database":
            _create_database(cursor, driver, args)
        elif action_name == "preview-create":
            _preview_create(driver, args)
        elif action_name == "drop-check":
            _drop_check(cursor, driver, args)
        elif action_name == "drop-database":
            _drop_database(cursor, driver, args)
        else:
            envelope.error(f"Unknown database action: {action_name}")
    except ValueError as err:
        # Raised by identifier validation and option parsing.
        envelope.error(f":admin databases {action_name} — {err}")
    except Exception as err:
        # Database errors (duplicate name, permission denied, database in
        # use).  Reported here rather than letting admin.py stringify the
        # raw driver exception.
        envelope.error(f":admin databases {action_name} — {_db_error(err)}")


def _db_error(err):
    """Render a driver exception as a single readable line.

    pyodbc raises with args of (sqlstate, "[sqlstate] message ...").
    The bracketed state and the trailing driver noise are stripped so the
    panel shows the server's actual complaint.
    """
    args = getattr(err, "args", ())
    message = args[1] if len(args) > 1 and isinstance(args[1], str) else str(err)
    # Strip the leading bracketed tags.  MSSQL nests several of them:
    # "[42000] [Microsoft][ODBC Driver 18...][SQL Server]real message".
    message = message.lstrip()
    while message.startswith("["):
        _, _, message = message.partition("]")
        message = message.lstrip()
    # PostgreSQL puts driver framing after the first newline; MSSQL appends
    # it inline as "(1801) (SQLExecDirectW)".
    message = message.replace("\\n", "\n").split("\n", 1)[0]
    message = re.sub(r"\s*\(\d+\)\s*\(SQL\w+\)\s*$", "", message)
    return " ".join(message.split()).rstrip(";")[:300] or str(err)[:300]


# --- Listing ---

def _database_list(cursor, driver):
    """List databases with owner, size, and status."""
    if driver.dialect_name == "mssql":
        sql = """
            SELECT d.name                                   AS [Database],
                   SUSER_SNAME(d.owner_sid)                 AS [Owner],
                   d.state_desc                             AS [Status],
                   d.recovery_model_desc                    AS [Recovery],
                   d.collation_name                         AS [Collation],
                   CAST(CAST(SUM(mf.size) * 8.0 / 1024 AS DECIMAL(18,1))
                        AS VARCHAR)                         AS [Size MB],
                   CONVERT(VARCHAR(19), d.create_date, 120) AS [Created]
            FROM sys.databases d
            LEFT JOIN sys.master_files mf ON mf.database_id = d.database_id
            GROUP BY d.name, d.owner_sid, d.state_desc, d.recovery_model_desc,
                     d.collation_name, d.create_date
            ORDER BY d.name
        """
    else:
        sql = """
            SELECT d.datname                                AS "Database",
                   r.rolname                                AS "Owner",
                   pg_encoding_to_char(d.encoding)          AS "Encoding",
                   d.datcollate                             AS "Collation",
                   CASE WHEN d.datconnlimit = -1 THEN 'unlimited'
                        ELSE d.datconnlimit::text END       AS "Conn Limit",
                   CASE WHEN has_database_privilege(d.datname, 'CONNECT')
                        THEN pg_size_pretty(pg_database_size(d.datname))
                        ELSE '' END                         AS "Size",
                   (SELECT COUNT(*) FROM pg_stat_activity a
                     WHERE a.datname = d.datname)::text     AS "Sessions"
            FROM pg_database d
            LEFT JOIN pg_roles r ON d.datdba = r.oid
            WHERE NOT d.datistemplate
            ORDER BY d.datname
        """
    cursor.execute(sql)
    headers = [col[0] for col in cursor.description]
    rows = [[str(v) if v is not None else "" for v in row]
            for row in cursor.fetchall()]

    return {
        "panel": "databases",
        "headers": headers,
        "rows": rows,
        "row_id": 0,  # Database name column
        "actions": [
            {"key": "N", "label": "New database", "command": "new-database"},
            {"key": "E", "label": "Edit settings", "command": "edit-database"},
            {"key": "D", "label": "Drop database", "command": "drop-check"},
        ],
        "info": None,
    }


# --- Create ---

def _clean_fields(fields):
    """Drop descriptor keys with no value, so the form payload stays small."""
    return [{k: v for k, v in spec.items() if v is not None}
            for spec in fields]


def _new_database_form(cursor, driver, args=None):
    """Send the CREATE DATABASE wizard form.

    The cursor is passed through so lookup fields (owner, template,
    collation) are populated from the server being administered.  ARGS may
    carry values from a form the user left to browse for a path, so the
    rebuilt form comes back filled in rather than blank.
    """
    from .. import envelope

    values = {}
    if args:
        values = _decode_payload(args).get("values") or {}

    envelope.admin_panel({
        "panel": "databases",
        "sub_panel": "form",
        "title": f"Create Database ({driver.dialect_name})",
        "form": {
            "fields": _clean_fields(driver.database_options(cursor)),
            "values": values,
            "submit_action": "create-database",
            "submit_label": "Create Database",
            "preview_action": "preview-create",
            "notes": [],
        },
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
        "context": {},
    })


def _settings_context(cursor, driver, args):
    """Resolve the database whose settings are being edited.

    Returns (name, current_settings).  The name comes from the row the
    user acted on, or from the payload when the form is being rebuilt.
    """
    payload = {}
    name = ""
    if args:
        first = args[0]
        # The row action passes a bare name; the form passes a payload.
        try:
            payload = _decode_payload(args)
            name = (payload.get("name_original")
                    or (payload.get("values") or {}).get("name_original")
                    or "")
        except ValueError:
            name = " ".join(args)
            payload = {}
        if not name:
            name = first
    if not name:
        raise ValueError("no database given")
    driver.validate_identifier(name)
    return name, driver.database_settings(cursor, name), payload


def _edit_database_form(cursor, driver, args):
    """Send the database settings form, filled in with what is in force."""
    from .. import envelope

    if not driver.supports_database_alter:
        envelope.error(f"Editing database settings is not supported on "
                       f"{driver.dialect_name}")
        return

    name, current, payload = _settings_context(cursor, driver, args)
    if not current:
        envelope.error(f"Database not found: {name}")
        return

    fields = _clean_fields(driver.settings_options(cursor, current))
    # Values from a form being rebuilt win over what the server reports.
    values = dict(payload.get("values") or {})
    values["name_original"] = name

    envelope.admin_panel({
        "panel": "databases",
        "sub_panel": "form",
        "title": f"Database Settings: {name}",
        "form": {
            "fields": fields,
            "values": values,
            "submit_action": "alter-database",
            "submit_label": "Apply Changes",
            "preview_action": "preview-alter",
            "notes": [f"Editing {name} on {driver.dialect_name}.",
                      "Only the settings you change are applied."],
        },
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
        "context": {"database": name},
    })


def _alter_statements(cursor, driver, args):
    """Return (name, statements) for the submitted settings form."""
    opts = _decode_payload(args)
    name = (opts.get("name_original") or "").strip()
    if not name:
        raise ValueError("the form did not say which database to alter")
    driver.validate_identifier(name)
    current = driver.database_settings(cursor, name)
    if not current:
        raise ValueError(f"database not found: {name}")

    specs = {f["key"]: f for f in driver.settings_options(cursor, current)}
    for key, spec in specs.items():
        if spec.get("required") and not str(opts.get(key) or "").strip():
            raise ValueError(f"{spec['label']} is required")
        if spec.get("type") == "int" and opts.get(key) not in (None, ""):
            try:
                opts[key] = int(opts[key])
            except (TypeError, ValueError):
                raise ValueError(
                    f"{spec['label']} must be a number "
                    f"(got {opts[key]!r})") from None
    return name, driver.sql_alter_database(name, opts, current)


def _preview_alter(cursor, driver, args):
    """Show the ALTER statements without running them."""
    from .. import envelope

    name, stmts = _alter_statements(cursor, driver, args)
    if not stmts:
        envelope.info(f"No changes to apply to {name}")
        return
    envelope.definition(f"ALTER DATABASE {name}", ";\n\n".join(stmts) + ";")


def _alter_database(cursor, driver, args):
    """Apply the submitted settings changes."""
    from .. import envelope

    if not driver.supports_database_alter:
        envelope.error(f"Editing database settings is not supported on "
                       f"{driver.dialect_name}")
        return

    name, stmts = _alter_statements(cursor, driver, args)
    if not stmts:
        envelope.info(f"No changes to apply to {name}")
        return
    for index, sql in enumerate(stmts):
        try:
            cursor.execute(sql)
            _commit(cursor)
        except Exception as err:
            envelope.error(
                f"Applied {index} of {len(stmts)} changes to '{name}' before "
                f"failing — {_db_error(err)}. Statement: "
                f"{' '.join(sql.split())[:160]}")
            return
    envelope.info(f"Applied {len(stmts)} change(s) to {name}")


def _browse_path(cursor, driver, args):
    """List a directory on the server for the path browser.

    The payload carries the directory to list, the form field being filled
    and the values already entered, so the form can be restored intact
    when the user picks a path or backs out.
    """
    from .. import envelope

    if not driver.supports_path_browse:
        envelope.error(f"Browsing server paths is not supported on "
                       f"{driver.dialect_name}")
        return

    payload = _decode_payload(args) if args else {}
    path = (payload.get("path") or "").strip()
    if not path:
        path = driver.default_paths(cursor).get("data") or "/"

    try:
        entries = driver.browse_path(cursor, path)
    except Exception as err:
        # A directory the server cannot read is a normal outcome of
        # browsing, not a wizard failure.
        envelope.error(f"Cannot list {path} — {_db_error(err)}")
        return

    rows = [["/" if e["is_dir"] else "", e["name"], e["path"]]
            for e in entries]
    parent = driver.parent_path(path, cursor)
    if parent:
        rows.insert(0, ["/", "..", parent])

    # Some servers report a missing or unreadable directory as an empty
    # listing rather than an error, which would otherwise look like an
    # empty directory.
    note = ""
    if not entries:
        note = "  (empty, missing, or not readable by the server)"

    envelope.admin_panel({
        "panel": "databases",
        "sub_panel": "path-browser",
        "title": f"Browse: {path}",
        "headers": ["Dir", "Name", "Path"],
        "rows": rows,
        "row_id": 2,  # full path column
        "actions": [],
        "info": (f"RET opens a directory, s selects {path!r}, "
                 f"q returns to the form{note}"),
        "context": {
            "path": path,
            "field": payload.get("field"),
            "values": payload.get("values") or {},
            "return_action": payload.get("return_action") or "new-database",
        },
    })


def _parse_options(driver, args):
    """Decode the submitted value map and coerce it against the field specs."""
    opts = _decode_payload(args)
    specs = {f["key"]: f for f in driver.database_options()}
    for key, spec in specs.items():
        value = opts.get(key)
        if spec.get("required") and not (value or "").strip():
            raise ValueError(f"{spec['label']} is required")
        if spec.get("type") == "int" and value not in (None, ""):
            try:
                opts[key] = int(value)
            except (TypeError, ValueError):
                raise ValueError(
                    f"{spec['label']} must be a number (got {value!r})") from None
    return opts


def _preview_create(driver, args):
    """Show the DDL that would be run, without executing it."""
    from .. import envelope

    opts = _parse_options(driver, args)
    stmts = driver.sql_create_database(opts)
    envelope.definition(f"CREATE DATABASE {opts.get('name', '')}",
                        ";\n\n".join(stmts) + ";")


def _create_database(cursor, driver, args):
    """Execute the CREATE DATABASE statements."""
    from .. import envelope

    opts = _parse_options(driver, args)
    name = opts["name"]
    stmts = driver.sql_create_database(opts)
    for index, sql in enumerate(stmts):
        try:
            cursor.execute(sql)
            _commit(cursor)
        except Exception as err:
            if index == 0:
                raise
            # CREATE DATABASE succeeded but a follow-up ALTER did not, so
            # the database exists in a partially configured state.  Say so
            # explicitly instead of reporting a bare failure.
            envelope.error(
                f"Database '{name}' was created, but a follow-up statement "
                f"failed — {_db_error(err)}. Statement: "
                f"{' '.join(sql.split())[:160]}")
            return
    envelope.info(f"Created database: {name}")


# --- Drop ---

def _current_database(cursor, driver):
    """Return the name of the database the session is connected to."""
    cursor.execute(driver.sql_current_database)
    row = cursor.fetchone()
    return row[0] if row else None


def _drop_check(cursor, driver, args):
    """Show the drop confirmation form with session count and generated SQL."""
    from .. import envelope

    if not args:
        envelope.error("drop-database requires a database name")
        return
    name = " ".join(args)
    driver.validate_identifier(name)

    current = _current_database(cursor, driver)
    if current and current.lower() == name.lower():
        fallback = driver.safe_fallback_database()
        # Only name the fallback when it is somewhere else to go.
        if fallback and fallback.lower() != name.lower():
            hint = f"Switch first with :use {fallback}"
        else:
            hint = "Switch to a different database first"
        envelope.error(f"Cannot drop '{name}' — it is the current database. "
                       f"{hint}")
        return

    sessions = 0
    try:
        sql, params = driver.sql_database_sessions(name)
        cursor.execute(sql, params)
        row = cursor.fetchone()
        sessions = int(row[0]) if row else 0
    except Exception:
        # Session inspection is advisory; a failure here must not block the
        # drop, since the user still gets the force option below.
        sessions = -1

    notes = [f"Database:        {name}"]
    if sessions >= 0:
        notes.append(f"Active sessions: {sessions}")
    else:
        notes.append("Active sessions: (could not determine)")
    if sessions > 0:
        notes.append("")
        notes.append("Other sessions are connected.  The drop will fail "
                     "unless you enable Force below.")

    envelope.admin_panel({
        "panel": "databases",
        "sub_panel": "form",
        "title": f"Drop Database: {name}",
        "form": {
            "fields": [
                {"key": "force", "label": "Force", "type": "bool",
                 "default": sessions > 0,
                 "help": ("evict active sessions first"
                          if driver.dialect_name == "mssql"
                          else "terminate active backends first")},
            ],
            "values": {"name": name},
            "submit_action": "drop-database",
            "submit_label": "Drop Database",
            "notes": notes,
            "confirm_text": name,
            "danger": True,
        },
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
        "context": {"database": name},
    })


def _drop_database(cursor, driver, args):
    """Execute the DROP DATABASE statements."""
    from .. import envelope

    opts = _decode_payload(args)
    name = (opts.get("name") or "").strip()
    if not name:
        envelope.error("drop-database requires a database name")
        return
    driver.validate_identifier(name)

    current = _current_database(cursor, driver)
    if current and current.lower() == name.lower():
        envelope.error(f"Cannot drop '{name}' — it is the current database")
        return

    force = bool(opts.get("force"))
    for sql in driver.sql_drop_database(name, force=force):
        cursor.execute(sql)
        _commit(cursor)
    envelope.info(f"Dropped database: {name}")


def _commit(cursor):
    """Commit when the connection is not in autocommit mode."""
    try:
        cursor.connection.commit()
    except Exception:
        # Autocommit connections raise on explicit commit; DDL has already
        # taken effect in that case.
        pass
