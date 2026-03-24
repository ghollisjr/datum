"""Command line tool to query databases via ODBC.

Usage:
    datum (-h | --help)
    datum --list-drivers
    datum --conn-string=<connection_string> [--sql-type=<type>] [--config=<path>]
          [--query=<sql> | --command=<cmd>] [--format=<fmt>]
    datum (--driver=<odbc_driver> | --dsn=<dsn>)
          [--server=<server> --database=<database>]
          [--user=<username> --pass=<password> --integrated]
          [--param <name=value>]...
          [--sql-type=<type>]
          [--config=<path>]
          [--query=<sql> | --command=<cmd>] [--format=<fmt>]

Options:
  -h --help             Show this screen.

To print the list of drivers recognized and exit:
    datum --list-drivers

To provide a known connection string just use:
  --conn-string=<connection_string>

Else it will be built using the individual parameters, start with how and what
to connect to:

  --dsn=<dsn>            If using a connection defined in a DSN, specify the
                         name here.
  --driver=<driver>      The ODBC driver to use, required if not using DSN.

  --server=<server>      Server to connect to. Omit for SQLite.
  --database=<database>  Database to open. Can be omitted if it is declared in
                         a DSN.

Then for security, if needed (can be skipped for SQLite or if DSN, etc.):

  --integrated           Use Integrated Security (MSSQL).
  --user=<username>      SQL Login user.
  --pass=<password>      SQL Login password.

Since ODBC is extensible, and drivers can support arbitrary parameters, they
can be added in pairs using:

  --param <name=value>   You can add as many as needed.

Optional parameters:

  --sql-type=<type>      SQL dialect to use for introspection commands.
                         Supported values: mssql, postgres, ansi.
                         If omitted, datum will attempt to detect the dialect
                         from the connection string or DSN. Supply this for
                         best results when auto-detection may be ambiguous.

  --config=<path>        Path to the INI file that declares config values and
                         custom commands. Can be a full path, or just a name,
                         in which case it is assumed the file is in the dir
                         $XDG_CONFIG_HOME/datum [default: config.ini]

Non-interactive mode (execute and exit):

  --query=<sql>          Execute a SQL query and print results to stdout.
  --command=<cmd>        Execute a datum command (e.g. :tables, :columns users).
  --format=<fmt>         Output format: table (default), json, or csv.

If the value for any parameter starts with ENV= then the contents of an env var
are used. For example: --pass=ENV=DB_SECRET would get the value for <password>
from $DB_SECRET. It is supported in --param values too.

"""
from docopt import docopt
from . import datum
from . import drivers
from . import envelope
import sys


def main():
    """Name is pretty descriptive, I think..."""
    args = docopt(__doc__)
    if args["--list-drivers"]:
        drivers.print_list()
        return

    query = args.get("--query")
    command = args.get("--command")
    fmt = args.get("--format") or "table"

    if query or command:
        envelope.set_mode("stderr")
        try:
            datum.initialize(args)
            datum.run_single(query=query, command=command, fmt=fmt)
        except Exception as e:
            print(str(e), file=sys.stderr)
            sys.exit(1)
    else:
        datum.initialize(args)
        datum.query_loop()


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print("Error: ", e, "\n")
