"""REPL loop module."""

from . import connect
from . import environment
from . import printer
from . import exporter
from . import commands
from . import envelope
from .drivers import get_driver, dialect_from_driver

import traceback

config = None
_driver = None


def initialize(args):
    """Instantiate the global config, and init the sub-modules with it."""
    global config, _driver
    environment.resolve_envvar_args(args)
    config = environment.get_config_dict(args["--config"])
    connect.initialize_module(args, config)
    printer.initialize_module(config)
    exporter.initialize_module(config)
    commands.initialize_module(config)

    # Detect SQL dialect and notify Emacs.
    _driver = get_driver(
        sql_type=args.get("--sql-type"),
        conn_string=connect.get_conn_string(),
        dsn=args.get("--dsn"),
    )
    if _driver.dialect_name == "ansi":
        envelope.warn("Could not determine SQL dialect — using ANSI SQL for "
                      "introspection. Supply --sql-type for best results.")
    envelope.dialect(_driver.dialect_name)
    commands.set_driver(_driver)

    # Connection is deferred to query_loop() so the prompt appears immediately.


def query_loop():
    """Query loop for Datum."""
    global config, _driver
    print("Connecting...", flush=True)
    try:
        connect.get_connection()
    except Exception as err:
        print(f"---CONNECTION ERROR---\n{err}\n---CONNECTION ERROR---", flush=True)
        return

    prompt_header = connect.show_connection_banner_and_get_prompt_header()

    # Emit metadata Emacs can use to populate the mode line.
    envelope.meta("server", connect.get_server_or_dsn())
    _emit_current_db_and_user()

    print(prompt_header)
    query = prompt_for_query_or_command()
    row_count = 0

    while query not in (":exit", ":quit"):
        try:
            if query.startswith(":"):
                query = commands.handle(query)
            if query:
                cursor = connect.get_connection().cursor()
                params = prompt_parameters(query)
                cursor.execute(query, params)
                row_count = cursor.rowcount
                if exporter.has_out_target():
                    exporter.export_out_target(cursor)
                elif config["csv_path"]:
                    exporter.export_cursor_results(cursor)
                else:
                    printer.print_cursor_results(cursor)
                print("\nRows affected:", row_count)
        except Exception as err:
            code = err.args[0] if err.args else ""
            message = traceback.format_exc()
            if len(err.args) > 1:
                code, message, *_ = err.args
                if f'[{code}] [Oracle]' in message:
                    message = message[0:message.index("\n")]
            print("---ERROR---\n"
                  "Code:", code, "\n"
                  "Message:", message, "\n"
                  "---ERROR---", flush=True)
        print("\n", prompt_header, sep="")
        query = prompt_for_query_or_command()


def _emit_current_db_and_user():
    """Best-effort: emit current database and user as metadata envelopes."""
    global _driver
    try:
        cursor = connect.get_connection().cursor()
        cursor.execute(_driver.sql_current_database)
        row = cursor.fetchone()
        if row:
            envelope.meta("database", str(row[0]))
    except Exception:
        pass
    try:
        cursor = connect.get_connection().cursor()
        cursor.execute(_driver.sql_current_user)
        row = cursor.fetchone()
        if row:
            envelope.meta("user", str(row[0]))
    except Exception:
        pass


def prompt_for_query_or_command():
    """Read the user's input, waiting for query terminators or commands."""
    global config
    lines = []
    prompt = "csv>" if config["csv_path"] else ">"
    print(prompt, flush=True, end="")
    lines.append(input())
    while True:
        last = lines[-1]
        if last.strip()[-2:] == ";;":
            return '\n'.join(lines)[:-1]
        if last.strip().upper() == 'GO':
            return '\n'.join(lines[:-1])
        if last.startswith(":"):
            return last
        lines.append(input(prompt))


def prompt_parameters(query):
    """Analyze the query text and read as many parameters as needed."""
    param_count = query.count(" ?")
    param_count += query.count(",?")
    param_count += query.count("=?")
    params = []
    if param_count > 0:
        print()
    for param_num in range(1, param_count + 1):
        params.append(input(f"{param_num}>"))
    return params
