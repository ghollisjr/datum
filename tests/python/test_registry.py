"""Unit tests for datum driver registry and auto-detection.

Tests get_driver() with explicit types, driver hints, port detection,
and DSN detection (with mocked pyodbc.dataSources).
"""

import pytest
from unittest.mock import patch, MagicMock

from datum.drivers import get_driver, _DRIVER_HINTS, _EXPLICIT_MAP
from datum.drivers.mysql import MySQLDriver
from datum.drivers.mssql import MSSQLDriver
from datum.drivers.postgres import PostgreSQLDriver
from datum.drivers.base import AnsiDriver


# ---------------------------------------------------------------------------
# Explicit sql_type
# ---------------------------------------------------------------------------

class TestExplicitType:
    def test_mysql(self):
        d = get_driver(sql_type="mysql")
        assert isinstance(d, MySQLDriver)

    def test_mariadb(self):
        d = get_driver(sql_type="mariadb")
        assert isinstance(d, MySQLDriver)

    def test_mssql(self):
        d = get_driver(sql_type="mssql")
        assert isinstance(d, MSSQLDriver)

    def test_sqlserver(self):
        d = get_driver(sql_type="sqlserver")
        assert isinstance(d, MSSQLDriver)

    def test_postgres(self):
        d = get_driver(sql_type="postgres")
        assert isinstance(d, PostgreSQLDriver)

    def test_postgresql(self):
        d = get_driver(sql_type="postgresql")
        assert isinstance(d, PostgreSQLDriver)

    def test_ansi(self):
        d = get_driver(sql_type="ansi")
        assert isinstance(d, AnsiDriver)

    def test_case_insensitive(self):
        d = get_driver(sql_type="MySQL")
        assert isinstance(d, MySQLDriver)

    def test_unknown_falls_back(self):
        d = get_driver(sql_type="nosuchdb")
        assert isinstance(d, AnsiDriver)


# ---------------------------------------------------------------------------
# Driver hints from connection string
# ---------------------------------------------------------------------------

class TestDriverHints:
    def test_mysql_odbc_driver(self):
        d = get_driver(conn_string="Driver={MySQL ODBC 8.0 Unicode Driver};Server=localhost;")
        assert isinstance(d, MySQLDriver)

    def test_mariadb_odbc_driver(self):
        d = get_driver(conn_string="Driver={MariaDB ODBC 3.1 Driver};Server=localhost;")
        assert isinstance(d, MySQLDriver)

    def test_sql_server_driver(self):
        d = get_driver(conn_string="Driver={ODBC Driver 17 for SQL Server};Server=myhost;")
        assert isinstance(d, MSSQLDriver)

    def test_postgres_driver(self):
        d = get_driver(conn_string="Driver={PostgreSQL Unicode};Server=localhost;")
        assert isinstance(d, PostgreSQLDriver)


# ---------------------------------------------------------------------------
# Port detection from Server= in connection string
# ---------------------------------------------------------------------------

class TestPortDetection:
    def test_mysql_port_colon(self):
        d = get_driver(conn_string="Driver={Generic};Server=db.example.com:3306;")
        assert isinstance(d, MySQLDriver)

    def test_mysql_port_comma(self):
        d = get_driver(conn_string="Driver={Generic};Server=db.example.com,3306;")
        assert isinstance(d, MySQLDriver)

    def test_postgres_port_colon(self):
        d = get_driver(conn_string="Driver={Generic};Server=db.example.com:5432;")
        assert isinstance(d, PostgreSQLDriver)

    def test_postgres_port_comma(self):
        d = get_driver(conn_string="Driver={Generic};Server=db.example.com,5432;")
        assert isinstance(d, PostgreSQLDriver)

    def test_mssql_port_colon(self):
        d = get_driver(conn_string="Driver={Generic};Server=db.example.com:1433;")
        assert isinstance(d, MSSQLDriver)

    def test_mssql_port_comma(self):
        d = get_driver(conn_string="Driver={Generic};Server=db.example.com,1433;")
        assert isinstance(d, MSSQLDriver)


# ---------------------------------------------------------------------------
# DSN detection (mock pyodbc.dataSources)
# ---------------------------------------------------------------------------

class TestDSNDetection:
    @patch("datum.drivers.pyodbc")
    def test_mysql_dsn(self, mock_pyodbc):
        mock_pyodbc.dataSources.return_value = {
            "my_dsn": "MySQL ODBC 8.0 Unicode Driver"
        }
        d = get_driver(dsn="my_dsn")
        assert isinstance(d, MySQLDriver)

    @patch("datum.drivers.pyodbc")
    def test_postgres_dsn(self, mock_pyodbc):
        mock_pyodbc.dataSources.return_value = {
            "pg_dsn": "PostgreSQL Unicode"
        }
        d = get_driver(dsn="pg_dsn")
        assert isinstance(d, PostgreSQLDriver)

    @patch("datum.drivers.pyodbc")
    def test_mssql_dsn(self, mock_pyodbc):
        mock_pyodbc.dataSources.return_value = {
            "ms_dsn": "ODBC Driver 17 for SQL Server"
        }
        d = get_driver(dsn="ms_dsn")
        assert isinstance(d, MSSQLDriver)

    @patch("datum.drivers.pyodbc")
    def test_unknown_dsn_fallback(self, mock_pyodbc):
        mock_pyodbc.dataSources.return_value = {
            "weird_dsn": "SomeUnknownDriver"
        }
        d = get_driver(dsn="weird_dsn")
        assert isinstance(d, AnsiDriver)

    @patch("datum.drivers.pyodbc")
    def test_dsn_not_found_fallback(self, mock_pyodbc):
        mock_pyodbc.dataSources.return_value = {}
        d = get_driver(dsn="nonexistent")
        assert isinstance(d, AnsiDriver)

    @patch("datum.drivers.pyodbc")
    def test_datasources_exception_fallback(self, mock_pyodbc):
        mock_pyodbc.dataSources.side_effect = Exception("ODBC error")
        d = get_driver(dsn="my_dsn")
        assert isinstance(d, AnsiDriver)


# ---------------------------------------------------------------------------
# Fallback behavior
# ---------------------------------------------------------------------------

class TestFallback:
    def test_no_args_returns_ansi(self):
        d = get_driver()
        assert isinstance(d, AnsiDriver)

    def test_empty_conn_string(self):
        d = get_driver(conn_string="")
        assert isinstance(d, AnsiDriver)


# ---------------------------------------------------------------------------
# Verify maps are consistent
# ---------------------------------------------------------------------------

class TestMaps:
    def test_explicit_map_keys(self):
        expected = {"mssql", "sqlserver", "postgres", "postgresql",
                    "mysql", "mariadb", "sqlite", "sqlite3", "oracle", "ansi"}
        assert set(_EXPLICIT_MAP.keys()) == expected

    def test_driver_hints_keys(self):
        expected = {"sql server", "sqlserver", "mssql",
                    "postgresql", "postgres", "psql",
                    "mysql", "mariadb",
                    "sqlite", "sqlite3",
                    "oracle"}
        assert set(_DRIVER_HINTS.keys()) == expected
