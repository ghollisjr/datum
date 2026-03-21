"""Driver registry and auto-detection for datum.

Detection order:
  1. Explicit --sql-type argument supplied at startup.
  2. Infer from the ODBC driver name in the connection string.
  3. Infer from the DSN name via pyodbc.dataSources().
  4. Fall back to AnsiDriver and emit a warning envelope.
"""

import re
import pyodbc

from .base import AnsiDriver
from .mssql import MSSQLDriver
from .mysql import MySQLDriver
from .oracle import OracleDriver
from .postgres import PostgreSQLDriver
from .sqlite import SQLiteDriver

# Map lower-cased substrings found in driver/DSN names to driver classes.
_DRIVER_HINTS = {
    "sql server":   MSSQLDriver,
    "sqlserver":    MSSQLDriver,
    "mssql":        MSSQLDriver,
    "postgresql":   PostgreSQLDriver,
    "postgres":     PostgreSQLDriver,
    "psql":         PostgreSQLDriver,
    "mysql":        MySQLDriver,
    "mariadb":      MySQLDriver,
    "sqlite":       SQLiteDriver,
    "sqlite3":      SQLiteDriver,
    "oracle":       OracleDriver,
}

# Explicit user-supplied type names accepted by --sql-type / :type command.
_EXPLICIT_MAP = {
    "mssql":      MSSQLDriver,
    "sqlserver":  MSSQLDriver,
    "postgres":   PostgreSQLDriver,
    "postgresql": PostgreSQLDriver,
    "mysql":      MySQLDriver,
    "mariadb":    MySQLDriver,
    "sqlite":     SQLiteDriver,
    "sqlite3":    SQLiteDriver,
    "oracle":     OracleDriver,
    "ansi":       AnsiDriver,
}


def get_driver(sql_type=None, conn_string=None, dsn=None):
    """Return the best available driver instance.

    sql_type: explicit override string (e.g. 'mssql', 'postgres').
    conn_string: the full ODBC connection string, used for heuristic detection.
    dsn: the DSN name, used as a secondary heuristic.

    Always returns a driver instance — never raises. Falls back to AnsiDriver.
    """
    # 1. Explicit override
    if sql_type:
        driver_class = _EXPLICIT_MAP.get(sql_type.lower())
        if driver_class:
            return driver_class()
        # Unknown explicit type — warn but don't crash, fall through.

    # 2. Infer from connection string Driver= value
    if conn_string:
        driver_match = re.search(r'Driver=\{?([^};]+)\}?', conn_string, re.IGNORECASE)
        if driver_match:
            driver_name = driver_match.group(1).lower()
            for hint, cls in _DRIVER_HINTS.items():
                if hint in driver_name:
                    return cls()

        # Also check Server= for port hints (5432 → postgres)
        server_match = re.search(r'Server=([^;]+)', conn_string, re.IGNORECASE)
        if server_match:
            server = server_match.group(1)
            if ':5432' in server or ',5432' in server:
                return PostgreSQLDriver()
            if ':1433' in server or ',1433' in server:
                return MSSQLDriver()
            if ':3306' in server or ',3306' in server:
                return MySQLDriver()
            if ':1521' in server or ',1521' in server:
                return OracleDriver()

    # 3. Infer from DSN name via pyodbc.dataSources()
    if dsn:
        try:
            sources = pyodbc.dataSources()
            # sources is {dsn_name: driver_name}
            driver_name = sources.get(dsn, "").lower()
            for hint, cls in _DRIVER_HINTS.items():
                if hint in driver_name:
                    return cls()
        except Exception:
            pass  # dataSources() can fail in some environments

    # 4. Fallback
    return AnsiDriver()


def dialect_from_driver(driver):
    """Return the dialect name string for use in envelopes and mode line."""
    return driver.dialect_name


def print_list():
    """Print the list of ODBC drivers available on this system.

    Replaces the old drivers.py module function, kept for backwards
    compatibility with the --list-drivers flag in __main__.py.
    """
    template = '"{0}"'
    drivers_list = [template.format(d) for d in pyodbc.drivers()]
    print("Drivers available:\n", "\n".join(drivers_list), sep="")
