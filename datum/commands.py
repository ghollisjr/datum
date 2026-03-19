"""Command handler for datum.

This module deals with built-in commands (:rows, :reconnect, etc.) and
processing of custom queries.
"""
from . import connect
from . import envelope
from . import exporter
from . import importer
from string import Formatter as _Formatter
import os

_config = {}
_driver = None

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

:in <path> <table> [:insert|:replace]
                       Import a file into a SQL table.
                       Default: error if table exists.
                       :insert — append to existing table.
                       :replace — drop, recreate, and insert.

:type [dialect]        Show or set the SQL dialect (mssql, postgres, ansi).

:databases             List all databases on the server.
:schemas               List all schemas in the current database.
:tables                List all tables and views.
:columns <table>       List columns for a table (schema.table or table).
:running               List currently running queries.
:user                  Show the current database user.
:version               Show the server version.
:use <database>        Switch to a different database (where supported).
:pwd                   Show current user, server, database, and version.

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
    flags = [a for a in args if a.startswith(":")]
    non_flags = [a for a in args if not a.startswith(":")]
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
    importer.run(path, table_name, mode, connect.get_connection(), _driver)


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
        print(f"Unknown dialect '{new_type}'. Known: mssql, postgres, ansi.")
        return
    _driver = new_driver
    envelope.dialect(_driver.dialect_name)
    print(f"SQL dialect set to: {_driver.dialect_name}")


# --- Introspection commands ---

def _run_introspect(sql, kind, label):
    """Run an introspection query, print results, and send an envelope."""
    try:
        cursor = connect.get_connection().cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        if not rows:
            print(f"(no {label} found)")
            return
        # Print as a simple table
        headers = [col[0] for col in cursor.description]
        col_widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0))
                      for i, h in enumerate(headers)]
        fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
        print(fmt.format(*headers))
        print("  ".join("-" * w for w in col_widths))
        for row in rows:
            print(fmt.format(*[str(v) for v in row]))
        # Send envelope for Emacs-side state
        items = [str(row[0]) for row in rows]
        envelope.introspect(kind, items)
    except Exception as err:
        print(f"Error running {label} query: {err}")


def databases(args):
    """Built-in :databases command."""
    global _driver
    _run_introspect(_driver.sql_list_databases, "databases", "databases")


def schemas(args):
    """Built-in :schemas command."""
    global _driver
    _run_introspect(_driver.sql_list_schemas, "schemas", "schemas")


def tables(args):
    """Built-in :tables command.

    The tables query returns (schema, table_name, table_type).  We send both
    bare table names and schema-qualified names as completion candidates.
    """
    global _driver
    try:
        cursor = connect.get_connection().cursor()
        cursor.execute(_driver.sql_list_tables)
        rows = cursor.fetchall()
        if not rows:
            print("(no tables found)")
            return
        headers = [col[0] for col in cursor.description]
        col_widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0))
                      for i, h in enumerate(headers)]
        fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
        print(fmt.format(*headers))
        print("  ".join("-" * w for w in col_widths))
        for row in rows:
            print(fmt.format(*[str(v) for v in row]))
        # Send both bare and qualified names for completion
        items = []
        for row in rows:
            schema = str(row[0]) if len(row) > 2 else None
            table_name = str(row[1]) if len(row) > 2 else str(row[0])
            items.append(table_name)
            if schema:
                items.append(f"{schema}.{table_name}")
        envelope.introspect("tables", sorted(set(items)))
    except Exception as err:
        print(f"Error running tables query: {err}")


def columns(args):
    """Built-in :columns command."""
    global _driver
    if not args:
        print(":columns requires a table name. Example: :columns dbo.users")
        return
    table_name = args[0]
    parts = table_name.split(".")
    if len(parts) > 1:
        schema = parts[0]
    else:
        schema = "dbo" if _driver.dialect_name == "mssql" else "public"
    table = parts[-1]
    sql = _driver.sql_list_columns(schema, table)
    _run_introspect(sql, f"columns:{table_name}", f"columns for {table_name}")


def running(args):
    """Built-in :running command."""
    global _driver
    _run_introspect(_driver.sql_running_queries, "running", "running queries")


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
    if _driver.dialect_name in ("postgres", "ansi"):
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
    ":columns":    columns,
    ":running":    running,
    ":user":       current_user,
    ":version":    version,
    ":use":        use_database,
    ":pwd":        pwd,
}
