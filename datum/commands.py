"""Command handler for datum.

This module deals with built-in commands (:rows, :reconnect, etc.) and
processing of custom queries.
"""
from . import connect
from . import envelope
from . import exporter
from . import importer
from . import printer
from string import Formatter as _Formatter
import os

_config = {}
_driver = None


def _quote_name(name):
    """Bracket-quote a SQL identifier if it contains dots or spaces."""
    if '.' in name or ' ' in name:
        return f"[{name}]"
    return name


from .utils import split_identifier as _split_identifier

_help_text = """
--Available commands--
:help                  Prints this command list.

:rows [number]         How many rows to print. 0 = all rows.
:chars [number]        Max characters per column. 0 = no limit.
:null [string]         String to show for NULL values. OFF = empty string.
:newline [string]      Replacement for newlines in values. OFF = keep as-is.
:tab [string]          Replacement for tabs in values. OFF = keep as-is.
:timeout [number]      Command timeout in seconds.

:out <path> [:force]   Export the NEXT query's results to a file.
                       Format is inferred from extension: .csv .parquet .json
                       Use :force to overwrite an existing file.

:in <path> <table> [:insert|:replace] [:batch N]
                       Import a file into a SQL table.
                       Default: error if table exists.
                       :insert — append to existing table.
                       :replace — drop, recreate, and insert.
                       :batch N — rows per batch (default 1000).

:type [dialect]        Show or set the SQL dialect (mssql, postgres, ansi).

:databases [pattern]   List databases. Filter with SQL LIKE wildcards.
:schemas [pattern]     List schemas. Filter with SQL LIKE wildcards.
:tables [pattern]      List tables/views. Filter with SQL LIKE wildcards.
:routines [pattern]    List stored procedures and functions.
:columns <table>       List columns for a table (schema.table or table).
:running               List currently running queries.
:user                  Show the current database user.
:version               Show the server version.
:use <database>        Switch to a different database (where supported).
:pwd                   Show current user, server, database, and version.

:definition <name>     Show DDL/source for a table, view, proc, function,
                       database, or schema. Supports dotted names.

:refresh               Silently refresh all introspection (autocomplete).
:refresh-db <name>     Introspect another database for cross-db completion (MSSQL).
:reconnect             Force a new connection, discarding the old one.
:csv [path]            Legacy: export all output to CSV. No arg to disable.
:script [path]         Read and run a SQL script file.
"""


def initialize_module(config):
    """Initialize this module with a reference to the global config."""
    global _config
    _config = config


def set_driver(driver):
    """Set the active SQL dialect driver. Called from datum.initialize()."""
    global _driver
    _driver = driver


def handle(user_input):
    """Handle a datum command."""
    global _builtins
    command_name, *args = user_input.strip().split(" ")
    output_query = ""
    if command_name in _builtins:
        return _builtins[command_name](args)
    elif command_name[1:] in _config["custom_commands"]:
        return prepare_query(_config["custom_commands"][command_name[1:]])
    else:
        print("Invalid command. Use :help for a list of available commands.")
    return output_query


# --- Existing commands (unchanged) ---

def help_text(args):
    """Built-in :help command."""
    global _help_text, _config
    print(_help_text)
    if _config["custom_commands"]:
        print('Commands declared in the "queries" section of the ',
              'configuration file:')
        line = ""
        for key in _config["custom_commands"].keys():
            if len(line) + len(key) > 79:
                print(line[:-1])
                line = key + ", "
            else:
                line += key + ", "
        print(line[:-2])


def rows(args):
    """Built-in :rows command."""
    global _config
    if args:
        try:
            new_value = int(args[0])
            if new_value < 0:
                raise ValueError("Why are you trying to break me...")
            _config["rows_to_print"] = new_value
        except ValueError:
            pass
    display_value = ("ALL" if not _config["rows_to_print"] else
                     _config["rows_to_print"])
    print('Printing', display_value, 'rows of each resultset.')


def chars(args):
    """Built-in :chars command."""
    global _config
    if args:
        try:
            new_value = int(args[0])
            if new_value < 0:
                raise ValueError("Why are you trying to break me...")
            _config["column_display_length"] = new_value
        except ValueError:
            pass
    if not _config["column_display_length"]:
        print('Printing ALL characters of each column.')
    else:
        print('Printing a maximum of', _config["column_display_length"],
              'characters of each column.')


def null(args):
    """Built-in :null command."""
    global _config
    if args and args[0] == "OFF":
        _config["null_string"] = ""
    elif args and args[0] != "OFF":
        _config["null_string"] = args[0]
    print('Using the string "', _config["null_string"],
          '" to print NULL values.', sep='')


def newline(args):
    """Built-in :newline command."""
    global _config
    if args and args[0] == "OFF":
        _config["newline_replacement"] = "\n"
    elif args and args[0] != "OFF":
        _config["newline_replacement"] = args[0]
    if _config["newline_replacement"] == "\n":
        print('Printing newlines with no conversion (might break the display',
              'of query output).')
    else:
        print('Using the string "', _config["newline_replacement"],
              '" to print literal new lines in values.', sep='')


def tab(args):
    """Built-in :tab command."""
    global _config
    if args and args[0] == "OFF":
        _config["tab_replacement"] = "\t"
    elif args and args[0] != "OFF":
        _config["tab_replacement"] = args[0]
    if _config["tab_replacement"] == "\t":
        print('Printing tabs with no conversion (might break the display of',
              'query output).')
    else:
        print('Using the string "', _config["tab_replacement"],
              '" to print literal tabs in values.', sep='')


def timeout(args):
    """Built-in :timeout command."""
    global _config
    connection = connect.get_connection()
    if args:
        try:
            new_value = int(args[0])
            if new_value < 0:
                raise ValueError("Why are you trying to break me...")
            connection.timeout = new_value
            _config["command_timeout"] = new_value
        except ValueError:
            pass
    print("Command timeout set to", connection.timeout, "seconds.")


def csv_setup(args):
    """Built-in :csv command (legacy session-level CSV export)."""
    global _config
    if args:
        filename = ""
        try:
            filename, _ = _args_to_abspath(args)
            open(filename, 'a').close()
            _config["csv_path"] = filename
        except Exception:
            print('ERROR opening file "', filename, '". Invalid path?', sep="")
            return
        print('CSV target "', _config["csv_path"], '"', sep="")
    else:
        _config["csv_path"] = None
        print("Disabled CSV writing")


def read_script(args):
    """Built-in :script command."""
    if args:
        filename = ""
        try:
            filename, exists = _args_to_abspath(args)
            if not exists:
                print('File "', filename, '" does not exist', sep="")
                return
            with open(filename, 'r', encoding='utf-8') as script:
                text = script.read().strip()
                print('Loaded script file "', filename, '"', sep="")
                return prepare_query(text)
        except Exception:
            print('ERROR reading file "', filename, '"', sep="")
            return
    else:
        print('No input path provided', sep="")
        return


def reconnect(args):
    """Built-in :reconnect command."""
    connect.get_connection(force_new=True)
    print("Opened new connection.")
    return


# --- New :out command ---

def out(args):
    """Built-in :out command — set next-query export target."""
    if not args:
        print(":out requires a file path. Example: :out /tmp/results.csv")
        return
    # Split off flags (:force) from the path
    flags = [a for a in args if a.startswith(":")]
    path_parts = [a for a in args if not a.startswith(":")]
    if not path_parts:
        print(":out requires a file path.")
        return
    path = os.path.abspath(" ".join(path_parts))
    force = ":force" in flags
    exporter.set_out_target(path, force=force)
    flag_note = " (overwrite enabled)" if force else " (will error if file exists)"
    print(f":out target set to: {path}{flag_note}")
    print("Run your query now. The next query's results will be exported.")


# --- New :in command ---

def in_import(args):
    """Built-in :in command — import a file into a SQL table."""
    global _driver
    if len(args) < 2:
        print(":in requires a file path and a table name. "
              "Example: :in /tmp/data.csv my_table")
        return
    # Extract :batch N before splitting flags/non-flags
    batch_size = 1000
    filtered_args = []
    i = 0
    while i < len(args):
        if args[i] == ":batch" and i + 1 < len(args):
            try:
                batch_size = int(args[i + 1])
            except ValueError:
                print(f":in - invalid batch size: {args[i + 1]}")
                return
            i += 2
            continue
        filtered_args.append(args[i])
        i += 1
    flags = [a for a in filtered_args if a.startswith(":")]
    non_flags = [a for a in filtered_args if not a.startswith(":")]
    if len(non_flags) < 2:
        print(":in requires a file path and a table name.")
        return
    # Last non-flag token is the table name, everything before is the path
    table_name = non_flags[-1]
    path = os.path.abspath(" ".join(non_flags[:-1]))
    mode = None
    if ":insert" in flags:
        mode = ":insert"
    elif ":replace" in flags or ":force" in flags:
        mode = ":replace"
    importer.run(path, table_name, mode, connect.get_connection(), _driver,
                 batch_size)


# --- New :type command ---

def sql_type(args):
    """Built-in :type command — show or override the SQL dialect."""
    global _driver
    from .drivers import get_driver
    if not args:
        print(f"Current SQL dialect: {_driver.dialect_name}")
        return
    new_type = args[0].lower()
    new_driver = get_driver(sql_type=new_type)
    if new_driver.dialect_name == "ansi" and new_type != "ansi":
        print(f"Unknown dialect '{new_type}'. Known: mssql, postgres, mysql, ansi.")
        return
    _driver = new_driver
    envelope.dialect(_driver.dialect_name)
    print(f"SQL dialect set to: {_driver.dialect_name}")


# --- Introspection commands ---

def _run_introspect(sql, kind, label, params=None, silent=False):
    """Run an introspection query, print results, and send an envelope.

    sql can be a plain SQL string, or when params is given, a parameterized
    query string with ? placeholders (executed with params via ODBC).

    When silent is True, skip all print output but still execute the query
    and send the envelope.
    """
    try:
        cursor = connect.get_connection().cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        rows = cursor.fetchall()
        if not rows:
            if not silent:
                print(f"(no {label} found)")
            return
        if not silent:
            # Use the standard printer for consistent formatting
            column_names = [printer.text_formatter(col[0]) for col in cursor.description]
            column_widths, print_ready = printer.format_rows(column_names, rows)
            print()
            printer.print_rows(column_widths, print_ready)
        # Send envelope for Emacs-side state.
        # For columns, send full rows [col_name, type, nullable, default]
        # so Emacs can store detailed metadata; for everything else, just names.
        if kind.startswith("columns:"):
            items = [[str(col) if col is not None else None for col in row]
                     for row in rows]
        else:
            items = [str(row[0]) for row in rows]
        envelope.introspect(kind, items)
    except Exception as err:
        if not silent:
            print(f"Error running {label} query: {err}")


def databases(args):
    """Built-in :databases command."""
    global _driver
    if args:
        sql, params = _driver.sql_list_databases_like(args[0])
        _run_introspect(sql, "databases", "databases", params)
    else:
        _run_introspect(_driver.sql_list_databases, "databases", "databases")


def schemas(args):
    """Built-in :schemas command."""
    global _driver
    if args:
        sql, params = _driver.sql_list_schemas_like(args[0])
        _run_introspect(sql, "schemas", "schemas", params)
    else:
        _run_introspect(_driver.sql_list_schemas, "schemas", "schemas")


def tables(args):
    """Built-in :tables command.

    The tables query returns (schema, table_name, table_type).  We send both
    bare table names and schema-qualified names as completion candidates.
    """
    global _driver
    try:
        cursor = connect.get_connection().cursor()
        if args:
            sql, params = _driver.sql_list_tables_like(args[0])
            cursor.execute(sql, params)
        else:
            cursor.execute(_driver.sql_list_tables)
        rows = cursor.fetchall()
        if not rows:
            print("(no tables found)")
            return
        # Use the standard printer for consistent formatting
        column_names = [printer.text_formatter(col[0]) for col in cursor.description]
        column_widths, print_ready = printer.format_rows(column_names, rows)
        print()
        printer.print_rows(column_widths, print_ready)
        # Send both bare and schema-qualified names for default schema,
        # schema-qualified only for other schemas.
        default_schema = _driver.default_schema
        items = []
        for row in rows:
            schema = str(row[0]) if len(row) > 2 else None
            table_name = str(row[1]) if len(row) > 2 else str(row[0])
            qname = _quote_name(table_name)
            if schema:
                items.append(f"{schema}.{qname}")
                if default_schema is None or schema == default_schema:
                    items.append(qname)
            else:
                items.append(qname)
        envelope.introspect("tables", sorted(set(items)))
    except Exception as err:
        print(f"Error running tables query: {err}")


def routines(args):
    """Built-in :routines command.

    The routines query returns (schema, routine_name, routine_type).  We send
    both bare routine names and schema-qualified names as completion candidates.
    """
    global _driver
    try:
        cursor = connect.get_connection().cursor()
        if args:
            sql, params = _driver.sql_list_routines_like(args[0])
            cursor.execute(sql, params)
        else:
            cursor.execute(_driver.sql_list_routines)
        rows = cursor.fetchall()
        if not rows:
            print("(no routines found)")
            return
        column_names = [printer.text_formatter(col[0]) for col in cursor.description]
        column_widths, print_ready = printer.format_rows(column_names, rows)
        print()
        printer.print_rows(column_widths, print_ready)
        default_schema = _driver.default_schema
        items = []
        for row in rows:
            schema = str(row[0]) if len(row) > 2 else None
            routine_name = str(row[1]) if len(row) > 2 else str(row[0])
            qname = _quote_name(routine_name)
            if schema:
                items.append(f"{schema}.{qname}")
                if default_schema is None or schema == default_schema:
                    items.append(qname)
            else:
                items.append(qname)
        envelope.introspect("routines", sorted(set(items)))
        # Send routine types (FUNCTION vs PROCEDURE) for completion behavior
        type_pairs = []
        for row in rows:
            schema = str(row[0]) if len(row) > 2 else None
            routine_name = str(row[1]) if len(row) > 2 else str(row[0])
            routine_type = str(row[2]) if len(row) > 2 else "PROCEDURE"
            qname = _quote_name(routine_name)
            if schema:
                type_pairs.append([f"{schema}.{qname}", routine_type])
                if default_schema is None or schema == default_schema:
                    type_pairs.append([qname, routine_type])
            else:
                type_pairs.append([qname, routine_type])
        envelope.introspect("routine-types", type_pairs)
        # Fetch routine parameter signatures for eldoc display
        try:
            sig_cursor = connect.get_connection().cursor()
            sig_cursor.execute(_driver.sql_routine_signatures)
            sig_rows = sig_cursor.fetchall()
            if sig_rows:
                pairs = []
                for sig_row in sig_rows:
                    schema = str(sig_row[0])
                    rname = str(sig_row[1])
                    sig = str(sig_row[2]) if sig_row[2] is not None else ""
                    pairs.append([f"{schema}.{rname}", sig])
                    if default_schema is None or schema == default_schema:
                        pairs.append([rname, sig])
                envelope.introspect("routine-sigs", pairs)
        except Exception:
            pass  # signature fetch is best-effort
    except Exception as err:
        print(f"Error running routines query: {err}")


def columns(args):
    """Built-in :columns command."""
    global _driver
    if not args:
        print(":columns requires a table name. Example: :columns dbo.users")
        return
    # Check for :silent flag
    silent = ":silent" in args
    filtered_args = [a for a in args if a != ":silent"]
    if not filtered_args:
        print(":columns requires a table name. Example: :columns dbo.users")
        return
    table_name = filtered_args[0]
    parts = _split_identifier(table_name)
    if len(parts) > 1:
        schema = parts[0]
    else:
        schema = _driver.default_schema
    table = parts[-1]
    sql, params = _driver.sql_list_columns(schema, table)
    _run_introspect(sql, f"columns:{table_name}", f"columns for {table_name}",
                    params=params, silent=silent)


def running(args):
    """Built-in :running command."""
    global _driver
    import re

    sections = []

    # --- Running Queries ---
    queries_text = "(none)"
    try:
        cursor = connect.get_connection().cursor()
        cursor.execute(_driver.sql_running_queries)
        rows = cursor.fetchall()
        if rows:
            clean_rows = []
            for row in rows:
                row = list(row)
                if row[-1] and isinstance(row[-1], str):
                    row[-1] = re.sub(r'\s+', ' ', row[-1]).strip()
                clean_rows.append(tuple(row))
            column_names = [printer.text_formatter(col[0])
                            for col in cursor.description]
            column_widths, print_ready = printer.format_rows(column_names,
                                                             clean_rows)
            queries_text = "\n".join(printer.format_row(column_widths, row)
                                     for row in print_ready)
    except Exception as err:
        queries_text = f"(error: {err})"

    sections.append(f"--- Running Queries ---\n{queries_text}")

    # --- Running Jobs (MSSQL only, best-effort) ---
    if _driver.dialect_name == "mssql":
        try:
            cursor = connect.get_connection().cursor()
            cursor.execute(_driver.sql_running_jobs)
            rows = cursor.fetchall()
            if rows:
                column_names = [printer.text_formatter(col[0])
                                for col in cursor.description]
                column_widths, print_ready = printer.format_rows(column_names,
                                                                rows)
                jobs_text = "\n".join(printer.format_row(column_widths, row)
                                      for row in print_ready)
            else:
                jobs_text = "(none)"
            sections.append(f"--- Running Jobs ---\n{jobs_text}")
        except Exception:
            pass  # silently omit if msdb access denied

    text = "\n\n".join(sections)
    print()
    print(text)
    envelope.running_text(text)


def current_user(args):
    """Built-in :user command."""
    global _driver
    try:
        cursor = connect.get_connection().cursor()
        cursor.execute(_driver.sql_current_user)
        row = cursor.fetchone()
        if row:
            user = str(row[0])
            print(f"Current user: {user}")
            envelope.meta("user", user)
    except Exception as err:
        print(f"Error getting current user: {err}")


def version(args):
    """Built-in :version command."""
    global _driver
    try:
        cursor = connect.get_connection().cursor()
        cursor.execute(_driver.sql_server_version)
        row = cursor.fetchone()
        if row:
            ver = str(row[0]).split("\n")[0]  # first line only
            print(f"Server version: {ver}")
            envelope.meta("version", ver)
    except Exception as err:
        print(f"Error getting server version: {err}")


def pwd(args):
    """Built-in :pwd command — show current login, server, database, version."""
    global _driver
    cursor = connect.get_connection().cursor()
    info = {}
    for label, sql in [("User", _driver.sql_current_user),
                       ("Database", _driver.sql_current_database),
                       ("Server", _driver.sql_server_version)]:
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
            if row:
                info[label] = str(row[0]).split("\n")[0]
        except Exception:
            info[label] = "(unavailable)"
    server_or_dsn = connect.get_server_or_dsn()
    print(f"  User:     {info.get('User', '?')}")
    print(f"  Server:   {server_or_dsn}")
    print(f"  Database: {info.get('Database', '?')}")
    print(f"  Version:  {info.get('Server', '?')}")


def use_database(args):
    """Built-in :use command — switch database.

    On databases that support USE (e.g. MSSQL), sends the SQL command directly.
    On others (e.g. PostgreSQL), reconnects with the new database name.
    """
    global _driver
    if not args:
        print(":use requires a database name.")
        return
    db = args[0]
    if _driver.dialect_name in ("postgres", "sqlite", "ansi"):
        try:
            connect.switch_database(db)
            print(f"Reconnected to database: {db}")
            envelope.meta("database", db)
        except Exception as err:
            print(f"Error switching database: {err}")
    else:
        try:
            cursor = connect.get_connection().cursor()
            cursor.execute(f"USE {db}")
            print(f"Switched to database: {db}")
            envelope.meta("database", db)
        except Exception as err:
            print(f"Error switching database: {err}")


# --- Helpers ---

def prepare_query(template):
    """Replace {} placeholders in a query template with user input."""
    f = _Formatter()
    kwargs_keys = {item[1] for item in f.parse(template) if item[1]}
    kwargs = {}
    for key in kwargs_keys:
        value = input(f"{key}>")
        kwargs[key] = value
    if kwargs:
        print()
    formatted_query = template.format(**kwargs)
    print("Command query:\n", formatted_query)
    return formatted_query


def _args_to_abspath(args):
    """Join args and return as an absolute path. Also confirms existence."""
    filename = " ".join(args)
    filename = os.path.abspath(filename)
    return (filename, os.path.exists(filename))


def _synthesize_create_table(schema, name, column_rows):
    """Build a CREATE TABLE statement from INFORMATION_SCHEMA column rows.

    Each row: (column_name, data_type, is_nullable,
               char_max_length, numeric_precision, numeric_scale, column_default)
    """
    lines = []
    for row in column_rows:
        col_name = str(row[0])
        data_type = str(row[1]).upper()
        is_nullable = str(row[2])
        char_max_len = row[3]
        num_precision = row[4]
        num_scale = row[5]
        col_default = row[6]

        # Build type with length/precision
        if char_max_len is not None:
            length = str(char_max_len) if int(char_max_len) > 0 else "MAX"
            type_str = f"{data_type}({length})"
        elif num_precision is not None and num_scale is not None:
            type_str = f"{data_type}({num_precision},{num_scale})"
        else:
            type_str = data_type

        null_str = "NULL" if is_nullable == "YES" else "NOT NULL"
        default_str = f" DEFAULT {col_default}" if col_default else ""
        lines.append(f"    {col_name} {type_str} {null_str}{default_str}")

    cols = ",\n".join(lines)
    return f"CREATE TABLE [{schema}].[{name}] (\n{cols}\n);"


def refresh_db(args):
    """Built-in :refresh-db command — introspect a specific remote database.

    Only works for MSSQL (cross-database three-part names).  Sends
    xdb:<database>: prefixed envelopes for schemas, tables, routines,
    routine-types, and routine-sigs.
    """
    global _driver
    if not args:
        print(":refresh-db requires a database name.")
        return
    if _driver.dialect_name != "mssql":
        print(":refresh-db is only supported on MSSQL.")
        return
    database = args[0]
    conn = connect.get_connection()

    # Schemas
    try:
        cursor = conn.cursor()
        cursor.execute(_driver.sql_list_schemas_in_db(database))
        rows = cursor.fetchall()
        if rows:
            envelope.introspect(f"xdb:{database}:schemas",
                                [str(r[0]) for r in rows])
    except Exception:
        pass

    # Tables
    try:
        cursor = conn.cursor()
        cursor.execute(_driver.sql_list_tables_in_db(database))
        rows = cursor.fetchall()
        if rows:
            items = []
            for row in rows:
                schema = str(row[0])
                table_name = str(row[1])
                items.append(f"{database}.{schema}.{_quote_name(table_name)}")
            envelope.introspect(f"xdb:{database}:tables", sorted(set(items)))
    except Exception:
        pass

    # Routines + types + signatures
    try:
        cursor = conn.cursor()
        cursor.execute(_driver.sql_list_routines_in_db(database))
        rows = cursor.fetchall()
        if rows:
            items = []
            type_pairs = []
            for row in rows:
                schema = str(row[0])
                routine_name = str(row[1])
                routine_type = str(row[2])
                qualified = f"{database}.{schema}.{_quote_name(routine_name)}"
                items.append(qualified)
                type_pairs.append([qualified, routine_type])
            envelope.introspect(f"xdb:{database}:routines", sorted(set(items)))
            envelope.introspect(f"xdb:{database}:routine-types", type_pairs)
            # Signatures (best-effort)
            try:
                sig_cursor = conn.cursor()
                sig_cursor.execute(
                    _driver.sql_routine_signatures_in_db(database))
                sig_rows = sig_cursor.fetchall()
                if sig_rows:
                    pairs = []
                    for sig_row in sig_rows:
                        schema = str(sig_row[0])
                        rname = str(sig_row[1])
                        sig = (str(sig_row[2])
                               if sig_row[2] is not None else "")
                        pairs.append(
                            [f"{database}.{schema}.{_quote_name(rname)}",
                             sig])
                    envelope.introspect(f"xdb:{database}:routine-sigs",
                                        pairs)
            except Exception:
                pass
    except Exception:
        pass

    # Signal that all xdb envelopes have been sent
    envelope.introspect(f"xdb:{database}:done", [])


def refresh_databases(args):
    """Built-in :refresh-databases — refresh database list."""
    global _driver
    conn = connect.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(_driver.sql_list_databases)
        rows = cursor.fetchall()
        if rows:
            envelope.introspect("databases", [str(r[0]) for r in rows])
    except Exception:
        pass
    return ""


def refresh_schemas(args):
    """Built-in :refresh-schemas — refresh schema list."""
    global _driver
    conn = connect.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(_driver.sql_list_schemas)
        rows = cursor.fetchall()
        if rows:
            envelope.introspect("schemas", [str(r[0]) for r in rows])
    except Exception:
        pass
    return ""


def refresh_tables(args):
    """Built-in :refresh-tables — refresh table list."""
    global _driver
    conn = connect.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(_driver.sql_list_tables)
        rows = cursor.fetchall()
        if rows:
            default_schema = _driver.default_schema
            items = []
            for row in rows:
                schema = str(row[0]) if len(row) > 2 else None
                table_name = str(row[1]) if len(row) > 2 else str(row[0])
                qname = _quote_name(table_name)
                if schema:
                    items.append(f"{schema}.{qname}")
                    if default_schema is None or schema == default_schema:
                        items.append(qname)
                else:
                    items.append(qname)
            envelope.introspect("tables", sorted(set(items)))
    except Exception:
        pass
    return ""


def refresh_routines(args):
    """Built-in :refresh-routines — refresh routine list and signatures."""
    global _driver
    conn = connect.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(_driver.sql_list_routines)
        rows = cursor.fetchall()
        if rows:
            default_schema = _driver.default_schema
            items = []
            for row in rows:
                schema = str(row[0]) if len(row) > 2 else None
                routine_name = str(row[1]) if len(row) > 2 else str(row[0])
                qname = _quote_name(routine_name)
                if schema:
                    items.append(f"{schema}.{qname}")
                    if default_schema is None or schema == default_schema:
                        items.append(qname)
                else:
                    items.append(qname)
            envelope.introspect("routines", sorted(set(items)))
            # Routine types (best-effort)
            type_pairs = []
            for row in rows:
                schema = str(row[0]) if len(row) > 2 else None
                routine_name = str(row[1]) if len(row) > 2 else str(row[0])
                routine_type = str(row[2]) if len(row) > 2 else "PROCEDURE"
                qname = _quote_name(routine_name)
                if schema:
                    type_pairs.append([f"{schema}.{qname}", routine_type])
                    if default_schema is None or schema == default_schema:
                        type_pairs.append([qname, routine_type])
                else:
                    type_pairs.append([qname, routine_type])
            envelope.introspect("routine-types", type_pairs)
            # Routine signatures (best-effort)
            try:
                sig_cursor = conn.cursor()
                sig_cursor.execute(_driver.sql_routine_signatures)
                sig_rows = sig_cursor.fetchall()
                if sig_rows:
                    pairs = []
                    for sig_row in sig_rows:
                        schema = str(sig_row[0])
                        rname = str(sig_row[1])
                        sig = str(sig_row[2]) if sig_row[2] is not None else ""
                        qrname = _quote_name(rname)
                        pairs.append([f"{schema}.{qrname}", sig])
                        if default_schema is None or schema == default_schema:
                            pairs.append([qrname, sig])
                    envelope.introspect("routine-sigs", pairs)
            except Exception:
                pass
    except Exception:
        pass
    return ""


def refresh(args):
    """Built-in :refresh command — silently re-run all introspection queries.

    Sends envelope updates for databases, schemas, tables, routines, and
    routine signatures without printing any output to the SQLi buffer.
    Each query is independent — one failure won't block the rest.
    """
    refresh_databases(args)
    refresh_schemas(args)
    refresh_tables(args)
    refresh_routines(args)


def definition(args):
    """Built-in :definition command — show DDL/source for a SQL object."""
    global _driver
    if not args:
        envelope.error(":definition requires an object name. "
                       "Example: :definition dbo.my_table")
        return

    raw_name = args[0]

    # Parse dotted name, respecting quoted segments
    parts = _split_identifier(raw_name)

    database = None
    schema = None
    name = None

    if len(parts) == 3:
        database, schema, name = parts
    elif len(parts) == 2:
        schema, name = parts
    elif len(parts) == 1:
        name = parts[0]
    else:
        envelope.error(f":definition — cannot parse '{raw_name}'")
        return

    try:
        cursor = connect.get_connection().cursor()

        # If no schema given, check if it's a database or schema name first
        if schema is None:
            # Check database
            try:
                sql, params = _driver.sql_check_database(name)
                cursor.execute(sql, params)
                row = cursor.fetchone()
                if row:
                    col_names = [col[0] for col in cursor.description]
                    lines = [f"-- Database: {name}", ""]
                    for i, col in enumerate(col_names):
                        lines.append(f"--   {col}: {row[i]}")
                    text = "\n".join(lines)
                    envelope.definition(name, text)
                    return
            except Exception:
                pass

            # Check schema
            try:
                sql, params = _driver.sql_check_schema(name)
                cursor.execute(sql, params)
                row = cursor.fetchone()
                if row:
                    col_names = [col[0] for col in cursor.description]
                    lines = [f"-- Schema: {name}", ""]
                    for i, col in enumerate(col_names):
                        lines.append(f"--   {col}: {row[i]}")
                    text = "\n".join(lines)
                    envelope.definition(name, text)
                    return
            except Exception:
                pass

            # Default schema
            schema = _driver.default_schema

        # Resolve object type
        sql, params = _driver.sql_resolve_object_type(schema, name)
        cursor.execute(sql, params)
        row = cursor.fetchone()
        if not row:
            envelope.error(f":definition — object '{raw_name}' not found")
            return

        object_type = str(row[0])
        display_name = f"{schema}.{name}"

        # Fetch definition
        sql, params = _driver.sql_get_definition(schema, name, object_type)
        cursor.execute(sql, params)

        if object_type == "TABLE":
            rows = cursor.fetchall()
            if not rows:
                envelope.error(f":definition — no columns found for {display_name}")
                return
            text = _synthesize_create_table(schema, name, rows)
        else:
            row = cursor.fetchone()
            if not row or not row[0]:
                envelope.error(f":definition — no source found for {display_name}")
                return
            text = str(row[0]).rstrip().rstrip(";\r\n \t").rstrip() + ";"

        envelope.definition(display_name, text)

    except Exception as err:
        envelope.error(f":definition error: {err}")


_builtins = {
    ":help":       help_text,
    ":rows":       rows,
    ":chars":      chars,
    ":null":       null,
    ":newline":    newline,
    ":tab":        tab,
    ":timeout":    timeout,
    ":csv":        csv_setup,
    ":script":     read_script,
    ":reconnect":  reconnect,
    ":out":        out,
    ":in":         in_import,
    ":type":       sql_type,
    ":databases":  databases,
    ":schemas":    schemas,
    ":tables":     tables,
    ":routines":   routines,
    ":columns":    columns,
    ":running":    running,
    ":user":       current_user,
    ":version":    version,
    ":use":        use_database,
    ":pwd":        pwd,
    ":definition": definition,
    ":refresh":            refresh,
    ":refresh-databases":  refresh_databases,
    ":refresh-schemas":    refresh_schemas,
    ":refresh-tables":     refresh_tables,
    ":refresh-routines":   refresh_routines,
    ":refresh-db":         refresh_db,
}
