"""Contains all the setup for new connections to the database."""
import pyodbc
import struct

# module "local" variables
_connection = None
_conn_string = None
_driver = None
_dsn = None
_server = None
_database = None
_user = None
_pass = None
_integrated = False
_timeout = 0
_params = None
_bcp = False
_bcp_extra = []

# The first newline here is useful for spacing later
_header_message = """
Special commands are prefixed with ":". For example, use ":exit" or ":quit" to
finish your session. Use ":help" to list available commands.
Everything else is sent directly to the server using ODBC when you type "GO" in
a new line or ";;" at the end of a query.
"""


def initialize_module(docopt_args, config):
    """Construct/deconstruct the connection string for the current session."""
    global _conn_string, _driver, _dsn, _server, _database, _user, _pass
    global _integrated, _timeout, _params, _bcp, _bcp_extra
    _bcp = docopt_args.get("--bcp", False)
    _bcp_extra = docopt_args.get("--bcp-extra") or []
    _conn_string = docopt_args["--conn-string"]
    if docopt_args["--conn-string"]:
        # We will use the connection string as-is
        # but attempt to extract server/dsn and db name for the prompt
        components = _conn_string.split(";")
        for piece in components:
            if '=' not in piece:
                continue
            key, value = piece.split("=", 1)
            # Match the keyword exactly rather than searching the whole
            # piece: "TrustServerCertificate=yes" contains "server", and a
            # host or database name could contain "uid" or "pwd".
            key = key.strip().lower()
            if key == 'dsn':
                _dsn = value
            elif key == 'server':
                _server = value
            elif key == 'database':
                _database = value
            elif key == 'trusted_connection' and value.lower() in ('yes', '1', 'true'):
                _integrated = True
            elif key == 'uid':
                _user = value
            elif key == 'pwd':
                _pass = value
        _timeout = config["command_timeout"]
        return

    _driver = docopt_args["--driver"]
    _dsn = docopt_args["--dsn"]
    _server = docopt_args["--server"]
    _database = docopt_args["--database"]
    _user = docopt_args["--user"]
    _pass = docopt_args["--pass"]
    _integrated = docopt_args["--integrated"]
    _params = ";".join(docopt_args["--param"])
    _timeout = config["command_timeout"]
    _build_connection_string()


def show_connection_banner_and_get_prompt_header():
    """Show the connection banner and return the prompt header.

    Honestly, most of these function names are descriptive enough on their own,
    and I love that. But I also hate Flymake warnings, so, here we are.
    """
    global _server, _database, _dsn, _header_message
    # If the server name isn't explicit, then use the DSN name. Even then,
    # something like SQLite might not have server nor DSN, so show "-"
    print_server = _server or _dsn or "-"
    print('Connected to server', print_server, end=" ")
    if _database:
        print('database', _database)
    print(_header_message)
    return print_server + ("/" + _database if _database else "")


def get_conn_string():
    """Return the current connection string (for dialect detection)."""
    return _conn_string


def get_server_or_dsn():
    """Return the server name or DSN for mode line display.
    Falls back to parsing the connection string if no explicit server/DSN."""
    if _server:
        return _server
    if _dsn:
        return _dsn
    # Try to extract from the connection string
    if _conn_string:
        for piece in _conn_string.split(";"):
            if "=" in piece and "server" in piece.lower():
                return piece.split("=", 1)[1]
    return "-"


def use_bcp():
    """Return True if --bcp was passed on the command line."""
    return _bcp


def get_bcp_args():
    """Build bcp command-line args from the ODBC connection string.

    Maps ODBC connection string keys to bcp flags.  Returns a list of
    strings (e.g. ["-S", "server", "-d", "db", "-T", "-u"]) or None if
    the connection string can't provide enough info for bcp.
    """
    if not _conn_string:
        return None

    # Parse connection string into a dict (case-insensitive keys)
    opts = {}
    for piece in _conn_string.split(";"):
        if "=" not in piece:
            continue
        key, value = piece.split("=", 1)
        opts[key.strip().lower()] = value.strip()

    args = []

    # Server (required)
    server = opts.get("server")
    if not server:
        return None
    args += ["-S", server]

    # Database
    db = opts.get("database")
    if db:
        args += ["-d", db]

    # Authentication
    if opts.get("trusted_connection", "").lower() in ("yes", "1", "true"):
        args.append("-T")
    else:
        uid = opts.get("uid")
        pwd = opts.get("pwd")
        if uid:
            args += ["-U", uid]
        if pwd:
            args += ["-P", pwd]

    # Append any user-supplied extra bcp arguments (--bcp-extra).
    # Flags like -u (trust cert) and -Y (encrypt) vary by bcp version,
    # so they are left to the user to specify per-connection via
    # sql-datum-bcp-extra in Emacs.
    args += _bcp_extra

    return args


def get_connection(force_new=False):
    """Use the module's information to return a live connection to the DB.

    With force_new=True, it will create a new connection even if one already
    exists. That's how :reconnect works.
    """
    global _conn_string, _connection, _timeout

    if _connection and not force_new:
        return _connection

    # As per https://github.com/mkleehammer/pyodbc/issues/43 we don't need to
    # explicitly close the old connection, if there was one. So we don't check.
    _connection = pyodbc.connect(_conn_string, autocommit=True)
    _connection.add_output_converter(-155, _handle_datetimeoffset)
    try:
        _connection.timeout = _timeout
    except Exception:
        pass
    return _connection


def switch_database(db):
    """Change the database and reconnect.

    For databases like PostgreSQL that don't support USE, this rebuilds the
    connection string with the new database and opens a fresh connection.
    """
    import re
    global _database, _conn_string
    _database = db
    # If built from individual params, rebuild cleanly
    if _params is not None:
        _build_connection_string()
    else:
        # Raw --conn-string: replace or append Database=
        if re.search(r'Database=[^;]*', _conn_string, re.IGNORECASE):
            _conn_string = re.sub(r'Database=[^;]*', f'Database={db}',
                                  _conn_string, flags=re.IGNORECASE)
        else:
            _conn_string += f";Database={db}"
    get_connection(force_new=True)


def _build_connection_string():
    global _conn_string, _driver, _dsn, _server, _database, _user, _pass
    global _integrated, _params
    _conn_string = ""
    if _dsn:
        _conn_string += f"DSN={_dsn};"
    if _driver:
        _conn_string += f"Driver={_driver};"
    if _server:
        _conn_string += f"Server={_server};"
    if _database:
        _conn_string += f"Database={_database};"
    if _integrated:
        _conn_string += "Trusted_Connection=Yes;"
    if _user:
        _conn_string += f"Uid={_user};"
    if _pass:
        _conn_string += f"Pwd={_pass};"
    _conn_string += _params


# source:
# https://github.com/mkleehammer/pyodbc/wiki/Using-an-Output-Converter-function
def _handle_datetimeoffset(dto_value):
    # see also:
    # https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
    tup = struct.unpack("<6hI2h", dto_value)
    tweaked = [tup[i] // 100 if i == 6 else tup[i] for i in range(len(tup))]
    t = "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:07d} {:+03d}:{:02d}"
    return t.format(*tweaked)
