"""Schema panel — schemas and tables of the current database.

Both MSSQL and PostgreSQL.  Unlike the databases and security panels,
this one works inside whichever database the session is connected to, so
switching with :use changes what it shows.

Sub-panels:
    schema — Schema list (default)
    tables — Tables in one schema
    form   — Create wizard
"""

import base64
import binascii
import json
import re


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


def _commit(cursor):
    try:
        cursor.connection.commit()
    except Exception:
        pass


def _current_database(cursor, driver):
    try:
        cursor.execute(driver.sql_current_database)
        row = cursor.fetchone()
        return row[0] if row else ""
    except Exception:
        return ""


def get_data(cursor, driver, args):
    """Return the schema list as a panel result dict."""
    if not driver.supports_schema_ddl:
        return {
            "panel": "schema",
            "headers": ["note"],
            "rows": [[f"Schema management is not supported on "
                      f"{driver.dialect_name}"]],
            "row_id": None,
            "actions": [],
            "info": None,
        }
    if args and args[0] == "tables" and len(args) >= 2:
        return _tables_panel(cursor, driver, " ".join(args[1:]))
    return _schema_list(cursor, driver)


def run_action(cursor, driver, action_name, args):
    """Execute a schema or table action."""
    from .. import envelope

    if not driver.supports_schema_ddl:
        envelope.error(f"Schema management is not supported on "
                       f"{driver.dialect_name}")
        return

    try:
        if action_name == "new-schema":
            _schema_form(cursor, driver)
        elif action_name == "create-schema":
            _create_schema(cursor, driver, args)
        elif action_name == "drop-schema":
            _drop_schema_check(cursor, driver, args)
        elif action_name == "do-drop-schema":
            _do_drop_schema(cursor, driver, args)
        elif action_name == "tables":
            envelope.admin_panel(
                _tables_panel(cursor, driver, " ".join(args)))
        elif action_name == "new-table":
            _table_form(cursor, driver, args)
        elif action_name in ("create-table", "preview-table"):
            _create_table(cursor, driver, args,
                          preview=action_name.startswith("preview"))
        elif action_name == "edit-table":
            _edit_table_form(cursor, driver, args)
        elif action_name in ("alter-table", "preview-alter-table",
                             "confirm-alter-table"):
            _alter_table(cursor, driver, args, action_name)
        elif action_name == "drop-table":
            _drop_table(cursor, driver, args)
        else:
            envelope.error(f"Unknown schema action: {action_name}")
    except ValueError as err:
        envelope.error(f":admin schema {action_name} — {err}")
    except Exception as err:
        envelope.error(f":admin schema {action_name} — {_db_error(err)}")


# --- Schemas ---

def _schema_list(cursor, driver):
    headers, rows = driver.list_schemas_detail(cursor)
    database = _current_database(cursor, driver)
    return {
        "panel": "schema",
        "headers": headers,
        "rows": rows,
        "row_id": 0,
        "actions": [
            {"key": "N", "label": "New schema", "command": "new-schema"},
            {"key": "T", "label": "Tables", "command": "tables"},
            {"key": "D", "label": "Drop schema", "command": "drop-schema"},
        ],
        "info": (f"Schemas in {database}" if database else None),
    }


def _refresh_schemas(cursor, driver):
    from .. import envelope

    try:
        envelope.admin_panel(_schema_list(cursor, driver))
    except Exception:
        pass


def _schema_form(cursor, driver):
    from .. import envelope

    envelope.admin_panel({
        "panel": "schema",
        "sub_panel": "form",
        "title": f"New Schema in {_current_database(cursor, driver)}",
        "form": {
            "fields": _clean_fields(driver.schema_options(cursor)),
            "values": {},
            "submit_action": "create-schema",
            "submit_label": "Create Schema",
            "notes": [],
        },
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
        "context": {},
    })


def _create_schema(cursor, driver, args):
    from .. import envelope

    opts = _decode_payload(args)
    name = (opts.get("name") or "").strip()
    if not name:
        envelope.error("Schema Name is required")
        return
    driver.validate_identifier(name)
    for sql in driver.sql_create_schema(opts):
        cursor.execute(sql)
        _commit(cursor)
    envelope.info(f"Created schema: {name}")
    _refresh_schemas(cursor, driver)


def _drop_schema_check(cursor, driver, args):
    """Confirm dropping a schema, reporting what it holds."""
    from .. import envelope

    if not args:
        envelope.error("drop-schema requires a schema name")
        return
    name = " ".join(args)
    driver.validate_identifier(name)
    _headers, rows = driver.list_tables_detail(cursor, name)

    notes = [f"Schema: {name}", f"Tables: {len(rows)}"]
    fields = []
    if rows:
        if driver.dialect_name == "postgres":
            notes += ["", "Cascade also drops everything the schema holds."]
            fields.append(
                {"key": "cascade", "label": "Cascade", "type": "bool",
                 "default": False,
                 "help": "drop the tables in it as well"})
        else:
            notes += ["", "SQL Server has no cascade: the schema's objects "
                          "must be dropped or moved first."]

    envelope.admin_panel({
        "panel": "schema",
        "sub_panel": "form",
        "title": f"Drop Schema: {name}",
        "form": {
            "fields": fields,
            "values": {"name": name},
            "submit_action": "do-drop-schema",
            "submit_label": "Drop Schema",
            "notes": notes,
            "confirm_text": name,
            "danger": True,
        },
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
        "context": {},
    })


def _do_drop_schema(cursor, driver, args):
    from .. import envelope

    opts = _decode_payload(args)
    name = (opts.get("name") or "").strip()
    if not name:
        envelope.error("drop-schema requires a schema name")
        return
    driver.validate_identifier(name)
    for sql in driver.sql_drop_schema(name, cascade=bool(opts.get("cascade"))):
        cursor.execute(sql)
        _commit(cursor)
    envelope.info(f"Dropped schema: {name}")
    _refresh_schemas(cursor, driver)


# --- Tables ---

def _tables_panel(cursor, driver, schema):
    schema = schema.strip()
    driver.validate_identifier(schema)
    headers, rows = driver.list_tables_detail(cursor, schema)
    return {
        "panel": "schema",
        "sub_panel": "tables",
        "title": f"Tables: {schema}",
        "headers": headers,
        "rows": rows,
        "row_id": 0,
        "actions": [
            {"key": "N", "label": "New table", "command": "new-table"},
            {"key": "E", "label": "Alter columns", "command": "edit-table"},
            {"key": "D", "label": "Drop table", "command": "drop-table"},
        ],
        "info": (f"Tables in {schema}" if rows
                 else f"No tables in {schema}"),
        "parent_panel": "schema",
        "context": {"schema": schema},
    }


def _refresh_tables(cursor, driver, schema):
    from .. import envelope

    try:
        envelope.admin_panel(_tables_panel(cursor, driver, schema))
    except Exception:
        pass


def _schema_from(driver, args):
    """Return the schema named by a row action or a form payload."""
    if not args:
        raise ValueError("no schema given")
    try:
        payload = _decode_payload(args)
    except ValueError:
        payload = {}
    schema = (payload.get("schema")
              or (payload.get("values") or {}).get("schema") or "")
    if not schema:
        schema = " ".join(args)
        payload = {}
    driver.validate_identifier(schema)
    return schema, payload


def _table_form(cursor, driver, args):
    from .. import envelope

    schema, payload = _schema_from(driver, args)
    values = dict(payload.get("values") or {})
    values["schema"] = schema
    envelope.admin_panel({
        "panel": "schema",
        "sub_panel": "form",
        "title": f"New Table in {schema}",
        "form": {
            "fields": _clean_fields(driver.table_options(cursor, schema)),
            "values": values,
            "submit_action": "create-table",
            "submit_label": "Create Table",
            "preview_action": "preview-table",
            "notes": ["Use [INS] and [DEL] to add and remove columns."],
        },
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
        "context": {"schema": schema},
    })


def _create_table(cursor, driver, args, preview=False):
    from .. import envelope

    opts = _decode_payload(args)
    schema = (opts.get("schema") or "").strip()
    name = (opts.get("name") or "").strip()
    if not schema:
        envelope.error("the form did not say which schema to create in")
        return
    if not name:
        envelope.error("Table Name is required")
        return
    driver.validate_identifier(schema)
    driver.validate_identifier(name)

    stmts = driver.sql_create_table(schema, opts)
    if preview:
        envelope.definition(f"CREATE TABLE {schema}.{name}",
                            ";\n\n".join(stmts) + ";")
        return
    for sql in stmts:
        cursor.execute(sql)
        _commit(cursor)
    envelope.info(f"Created table: {schema}.{name}")
    _refresh_tables(cursor, driver, schema)


def _edit_table_form(cursor, driver, args):
    """Send the table editor, prefilled with the current columns."""
    from .. import envelope

    payload = _decode_payload(args) if args else {}
    schema_name = (payload.get("schema") or "").strip()
    table = (payload.get("table") or "").strip()
    if not (schema_name and table):
        envelope.error("edit-table requires a schema and a table")
        return
    driver.validate_identifier(schema_name)
    driver.validate_identifier(table)

    columns = driver.list_table_columns(cursor, schema_name, table)
    if not columns:
        envelope.error(f"Table not found: {schema_name}.{table}")
        return

    fields = _clean_fields(driver.table_options(cursor, schema_name))
    # The name is fixed here: renaming is not something a column diff can
    # express, since it cannot be told from a drop plus an add.
    fields = [f for f in fields if f["key"] != "name"]
    for field in fields:
        if field["key"] == "columns":
            field["default"] = columns
            # These rows stand for columns that exist, so each carries
            # what it was called when the form opened.  That is what
            # lets an edited name be applied as a rename rather than as
            # a drop and an add.
            field["track_identity"] = True

    envelope.admin_panel({
        "panel": "schema",
        "sub_panel": "form",
        "title": f"Alter Table: {schema_name}.{table}",
        "form": {
            "fields": fields,
            "values": {"schema": schema_name, "table": table},
            "submit_action": "alter-table",
            "submit_label": "Apply Changes",
            "preview_action": "preview-alter-table",
            "notes": [
                "Only the columns you change are altered.",
                "Editing a name renames the column and keeps its data; "
                "deleting a row and inserting another does not.",
            ],
        },
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
        "context": {"schema": schema_name},
    })


def _alter_table(cursor, driver, args, action_name):
    """Preview, confirm, or apply the column changes."""
    from .. import envelope

    opts = _decode_payload(args)
    schema_name = (opts.get("schema") or "").strip()
    table = (opts.get("table") or "").strip()
    if not (schema_name and table):
        envelope.error("the form did not say which table to alter")
        return
    driver.validate_identifier(schema_name)
    driver.validate_identifier(table)

    current = driver.list_table_columns(cursor, schema_name, table)
    stmts = driver.sql_alter_table(schema_name, table, opts, current)
    if not stmts:
        envelope.info(f"No changes to apply to {schema_name}.{table}")
        return

    if action_name == "preview-alter-table":
        envelope.definition(f"ALTER TABLE {schema_name}.{table}",
                            ";\n\n".join(stmts) + ";")
        return

    # Dropping a column destroys its data, so the plan is shown and has
    # to be confirmed before anything runs.
    added, dropped, _changed, _renamed = driver._diff_columns(
        opts, current)
    if dropped and action_name != "confirm-alter-table":
        _confirm_alter(driver, schema_name, table, opts, dropped, stmts,
                       added)
        return

    for index, sql in enumerate(stmts):
        try:
            cursor.execute(sql)
            _commit(cursor)
        except Exception as err:
            envelope.error(
                f"Applied {index} of {len(stmts)} change(s) to "
                f"{schema_name}.{table} before failing — {_db_error(err)}")
            return
    envelope.info(f"Applied {len(stmts)} change(s) to "
                  f"{schema_name}.{table}")
    _refresh_tables(cursor, driver, schema_name)


def _confirm_alter(driver, schema_name, table, opts, dropped, stmts,
                   added=None):
    """Ask before running a plan that drops columns."""
    from .. import envelope

    names = ", ".join(str(row[0]) for row in dropped)
    notes = [f"Table: {schema_name}.{table}",
             "",
             f"These columns will be DROPPED, losing their data: {names}",
             ""]
    # Someone who meant to rename says so by editing the name in place,
    # which never reaches here.
    if added:
        notes += ["To rename a column instead, cancel and edit its name "
                  "in place rather than removing and re-adding it.",
                  ""]
    notes += [" ".join(sql.split())[:110] for sql in stmts]

    values = dict(opts)
    envelope.admin_panel({
        "panel": "schema",
        "sub_panel": "form",
        "title": f"Drop columns from {schema_name}.{table}?",
        "form": {
            "fields": [],
            "values": values,
            "submit_action": "confirm-alter-table",
            "submit_label": "Apply Anyway",
            "notes": notes,
            "confirm_text": table,
            "danger": True,
        },
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
        "context": {"schema": schema_name},
    })


def _drop_table(cursor, driver, args):
    from .. import envelope

    payload = _decode_payload(args) if args else {}
    schema = (payload.get("schema") or "").strip()
    name = (payload.get("table") or "").strip()
    if not (schema and name):
        envelope.error("drop-table requires a schema and a table")
        return
    driver.validate_identifier(schema)
    driver.validate_identifier(name)
    for sql in driver.sql_drop_table(schema, name):
        cursor.execute(sql)
        _commit(cursor)
    envelope.info(f"Dropped table: {schema}.{name}")
    _refresh_tables(cursor, driver, schema)
