"""Unit tests for datum SQL dialect drivers.

These tests import driver classes directly and verify SQL generation,
type maps, and properties — no database connection needed.
"""

import pytest

from datum.drivers.mysql import MySQLDriver
from datum.drivers.mssql import MSSQLDriver
from datum.drivers.postgres import PostgreSQLDriver
from datum.drivers.sqlite import SQLiteDriver
from datum.drivers.base import AnsiDriver, BaseDriver


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mysql():
    return MySQLDriver()

@pytest.fixture
def mssql():
    return MSSQLDriver()

@pytest.fixture
def postgres():
    return PostgreSQLDriver()

@pytest.fixture
def sqlite():
    return SQLiteDriver()

@pytest.fixture
def ansi():
    return AnsiDriver()

ALL_DRIVERS = [MySQLDriver, MSSQLDriver, PostgreSQLDriver, SQLiteDriver, AnsiDriver]


# ---------------------------------------------------------------------------
# dialect_name
# ---------------------------------------------------------------------------

class TestDialectName:
    def test_mysql(self, mysql):
        assert mysql.dialect_name == "mysql"

    def test_mssql(self, mssql):
        assert mssql.dialect_name == "mssql"

    def test_postgres(self, postgres):
        assert postgres.dialect_name == "postgres"

    def test_sqlite(self, sqlite):
        assert sqlite.dialect_name == "sqlite"

    def test_ansi(self, ansi):
        assert ansi.dialect_name == "ansi"


# ---------------------------------------------------------------------------
# default_schema
# ---------------------------------------------------------------------------

class TestDefaultSchema:
    def test_mysql_none(self, mysql):
        assert mysql.default_schema is None

    def test_mssql_dbo(self, mssql):
        assert mssql.default_schema == "dbo"

    def test_postgres_public(self, postgres):
        assert postgres.default_schema == "public"

    def test_sqlite_none(self, sqlite):
        assert sqlite.default_schema is None

    def test_ansi_public(self, ansi):
        assert ansi.default_schema == "public"


# ---------------------------------------------------------------------------
# SQL introspection properties return non-empty strings
# ---------------------------------------------------------------------------

class TestSQLProperties:
    @pytest.mark.parametrize("driver_cls", ALL_DRIVERS)
    def test_sql_list_databases(self, driver_cls):
        d = driver_cls()
        assert isinstance(d.sql_list_databases, str)
        assert len(d.sql_list_databases.strip()) > 0

    @pytest.mark.parametrize("driver_cls", ALL_DRIVERS)
    def test_sql_current_database(self, driver_cls):
        d = driver_cls()
        assert isinstance(d.sql_current_database, str)
        assert len(d.sql_current_database.strip()) > 0

    @pytest.mark.parametrize("driver_cls", ALL_DRIVERS)
    def test_sql_list_schemas(self, driver_cls):
        d = driver_cls()
        assert isinstance(d.sql_list_schemas, str)
        assert len(d.sql_list_schemas.strip()) > 0

    @pytest.mark.parametrize("driver_cls", ALL_DRIVERS)
    def test_sql_current_schema(self, driver_cls):
        d = driver_cls()
        assert isinstance(d.sql_current_schema, str)
        assert len(d.sql_current_schema.strip()) > 0

    @pytest.mark.parametrize("driver_cls", ALL_DRIVERS)
    def test_sql_list_tables(self, driver_cls):
        d = driver_cls()
        assert isinstance(d.sql_list_tables, str)
        assert len(d.sql_list_tables.strip()) > 0

    @pytest.mark.parametrize("driver_cls", ALL_DRIVERS)
    def test_sql_current_user(self, driver_cls):
        d = driver_cls()
        assert isinstance(d.sql_current_user, str)
        assert len(d.sql_current_user.strip()) > 0

    @pytest.mark.parametrize("driver_cls", ALL_DRIVERS)
    def test_sql_server_version(self, driver_cls):
        d = driver_cls()
        assert isinstance(d.sql_server_version, str)
        assert len(d.sql_server_version.strip()) > 0

    @pytest.mark.parametrize("driver_cls", ALL_DRIVERS)
    def test_sql_running_queries(self, driver_cls):
        d = driver_cls()
        assert isinstance(d.sql_running_queries, str)
        assert len(d.sql_running_queries.strip()) > 0


# ---------------------------------------------------------------------------
# *_like() methods return (sql_string, [param]) tuples
# ---------------------------------------------------------------------------

class TestLikeMethods:
    @pytest.mark.parametrize("driver_cls", ALL_DRIVERS)
    def test_sql_list_databases_like(self, driver_cls):
        d = driver_cls()
        result = d.sql_list_databases_like("%test%")
        assert isinstance(result, tuple)
        assert len(result) == 2
        sql, params = result
        assert isinstance(sql, str)
        assert isinstance(params, list)
        assert params == ["%test%"]

    @pytest.mark.parametrize("driver_cls", ALL_DRIVERS)
    def test_sql_list_schemas_like(self, driver_cls):
        d = driver_cls()
        result = d.sql_list_schemas_like("%pub%")
        assert isinstance(result, tuple)
        sql, params = result
        assert isinstance(sql, str)
        assert params == ["%pub%"]

    @pytest.mark.parametrize("driver_cls", ALL_DRIVERS)
    def test_sql_list_tables_like(self, driver_cls):
        d = driver_cls()
        result = d.sql_list_tables_like("%cust%")
        assert isinstance(result, tuple)
        sql, params = result
        assert isinstance(sql, str)
        assert params == ["%cust%"]


class TestMySQLLikeMethods:
    def test_routines_like(self, mysql):
        sql, params = mysql.sql_list_routines_like("%format%")
        assert isinstance(sql, str)
        assert params == ["%format%"]


class TestMSSQLLikeMethods:
    def test_routines_like(self, mssql):
        sql, params = mssql.sql_list_routines_like("%proc%")
        assert isinstance(sql, str)
        assert params == ["%proc%"]


class TestPostgresLikeMethods:
    def test_routines_like(self, postgres):
        sql, params = postgres.sql_list_routines_like("%func%")
        assert isinstance(sql, str)
        assert params == ["%func%"]


# ---------------------------------------------------------------------------
# sql_resolve_object_type
# ---------------------------------------------------------------------------

class TestResolveObjectType:
    def test_mysql_schema_none(self, mysql):
        sql, params = mysql.sql_resolve_object_type(None, "customers")
        assert isinstance(sql, str)
        assert "?" in sql
        # schema=None: params = [name, name] (one for tables, one for routines)
        assert params == ["customers", "customers"]

    def test_mysql_schema_explicit(self, mysql):
        sql, params = mysql.sql_resolve_object_type("mydb", "customers")
        assert isinstance(sql, str)
        # schema explicit: params = [schema, name, schema, name]
        assert params == ["mydb", "customers", "mydb", "customers"]

    def test_mssql(self, mssql):
        sql, params = mssql.sql_resolve_object_type("dbo", "orders")
        assert isinstance(sql, str)
        assert params == ["dbo", "orders"]

    def test_postgres(self, postgres):
        sql, params = postgres.sql_resolve_object_type("public", "users")
        assert isinstance(sql, str)
        assert params == ["public", "users", "public", "users"]


# ---------------------------------------------------------------------------
# sql_get_definition — verify different branches for TABLE/VIEW/FUNCTION
# ---------------------------------------------------------------------------

class TestGetDefinition:
    def test_mysql_table(self, mysql):
        sql, params = mysql.sql_get_definition(None, "customers", "TABLE")
        assert "COLUMN_NAME" in sql
        assert params == ["customers"]

    def test_mysql_table_with_schema(self, mysql):
        sql, params = mysql.sql_get_definition("mydb", "customers", "TABLE")
        assert "COLUMN_NAME" in sql
        assert params == ["mydb", "customers"]

    def test_mysql_view(self, mysql):
        sql, params = mysql.sql_get_definition(None, "my_view", "VIEW")
        assert "VIEW_DEFINITION" in sql
        assert params == ["my_view"]

    def test_mysql_function(self, mysql):
        sql, params = mysql.sql_get_definition(None, "my_func", "FUNCTION")
        assert "ROUTINE_DEFINITION" in sql
        assert params == ["my_func"]

    def test_mssql_table(self, mssql):
        sql, params = mssql.sql_get_definition("dbo", "orders", "TABLE")
        assert "COLUMN_NAME" in sql
        assert params == ["dbo", "orders"]

    def test_mssql_procedure(self, mssql):
        sql, params = mssql.sql_get_definition("dbo", "my_proc", "PROCEDURE")
        assert "sql_modules" in sql
        assert params == ["dbo", "my_proc"]

    def test_postgres_table(self, postgres):
        sql, params = postgres.sql_get_definition("public", "users", "TABLE")
        assert "column_name" in sql
        assert params == ["public", "users"]

    def test_postgres_view(self, postgres):
        sql, params = postgres.sql_get_definition("public", "my_view", "VIEW")
        assert "pg_get_viewdef" in sql
        assert params == ["public", "my_view"]

    def test_postgres_function(self, postgres):
        sql, params = postgres.sql_get_definition("public", "my_func", "FUNCTION")
        assert "pg_get_functiondef" in sql
        assert params == ["public", "my_func"]


# ---------------------------------------------------------------------------
# Type maps — spot-check key mappings
# ---------------------------------------------------------------------------

class TestTypeMaps:
    def test_mysql_type_map(self, mysql):
        assert mysql.python_type_to_sql("int64") == "BIGINT"
        assert mysql.python_type_to_sql("string") == "LONGTEXT"
        assert mysql.python_type_to_sql("bool") == "TINYINT(1)"
        assert mysql.python_type_to_sql("float64") == "DOUBLE"
        assert mysql.python_type_to_sql("binary") == "BLOB"
        assert mysql.python_type_to_sql("large_binary") == "LONGBLOB"
        assert mysql.python_type_to_sql("decimal128") == "DECIMAL(38,10)"
        assert mysql.python_type_to_sql("timestamp[us]") == "DATETIME(6)"
        # Unknown type fallback
        assert mysql.python_type_to_sql("unknown_type") == "TEXT"

    def test_mssql_type_map(self, mssql):
        assert mssql.python_type_to_sql("int64") == "BIGINT"
        assert mssql.python_type_to_sql("string") == "NVARCHAR(MAX)"
        assert mssql.python_type_to_sql("bool") == "BIT"
        assert mssql.python_type_to_sql("float64") == "FLOAT"
        assert mssql.python_type_to_sql("binary") == "VARBINARY(MAX)"
        assert mssql.python_type_to_sql("timestamp[s]") == "DATETIME2"
        # Unknown type fallback
        assert mssql.python_type_to_sql("unknown_type") == "NVARCHAR(MAX)"

    def test_postgres_type_map(self, postgres):
        assert postgres.python_type_to_sql("int64") == "BIGINT"
        assert postgres.python_type_to_sql("string") == "TEXT"
        assert postgres.python_type_to_sql("bool") == "BOOLEAN"
        assert postgres.python_type_to_sql("float64") == "DOUBLE PRECISION"
        assert postgres.python_type_to_sql("binary") == "BYTEA"
        assert postgres.python_type_to_sql("timestamp[us]") == "TIMESTAMP"
        # Unknown type fallback
        assert postgres.python_type_to_sql("unknown_type") == "TEXT"

    def test_ansi_type_map(self, ansi):
        assert ansi.python_type_to_sql("int64") == "BIGINT"
        assert ansi.python_type_to_sql("string") == "NVARCHAR(MAX)"
        assert ansi.python_type_to_sql("bool") == "BOOLEAN"
        assert ansi.python_type_to_sql("float64") == "DOUBLE PRECISION"
        assert ansi.python_type_to_sql("binary") == "BLOB"
        assert ansi.python_type_to_sql("decimal128") == "DECIMAL(38,10)"
        # Unknown type fallback
        assert ansi.python_type_to_sql("unknown_type") == "NVARCHAR(MAX)"


# ---------------------------------------------------------------------------
# sql_list_routines (property) — non-empty SQL
# ---------------------------------------------------------------------------

class TestListRoutines:
    def test_mysql(self, mysql):
        sql = mysql.sql_list_routines
        assert isinstance(sql, str)
        assert "ROUTINE" in sql

    def test_mssql(self, mssql):
        sql = mssql.sql_list_routines
        assert isinstance(sql, str)
        assert "sys.objects" in sql

    def test_postgres(self, postgres):
        sql = postgres.sql_list_routines
        assert isinstance(sql, str)
        assert "pg_proc" in sql


# ---------------------------------------------------------------------------
# sql_routine_signatures (property) — non-empty SQL
# ---------------------------------------------------------------------------

class TestRoutineSignatures:
    def test_mysql(self, mysql):
        sql = mysql.sql_routine_signatures
        assert isinstance(sql, str)
        assert "GROUP_CONCAT" in sql

    def test_mssql(self, mssql):
        sql = mssql.sql_routine_signatures
        assert isinstance(sql, str)
        assert "sys.parameters" in sql

    def test_postgres(self, postgres):
        sql = postgres.sql_routine_signatures
        assert isinstance(sql, str)
        assert "pg_get_function_identity_arguments" in sql


# ---------------------------------------------------------------------------
# sql_list_columns
# ---------------------------------------------------------------------------

class TestListColumns:
    def test_mysql_schema_none(self, mysql):
        sql, params = mysql.sql_list_columns(None, "orders")
        assert "COLUMN_NAME" in sql
        assert "DATABASE()" in sql
        assert params == ["orders"]

    def test_mysql_schema_explicit(self, mysql):
        sql, params = mysql.sql_list_columns("mydb", "orders")
        assert "COLUMN_NAME" in sql
        assert params == ["mydb", "orders"]


# ---------------------------------------------------------------------------
# sql_check_database / sql_check_schema
# ---------------------------------------------------------------------------

class TestCheckDatabaseSchema:
    def test_mysql_check_database(self, mysql):
        sql, params = mysql.sql_check_database("mydb")
        assert isinstance(sql, str)
        assert params == ["mydb", "mydb"]

    def test_mysql_check_schema(self, mysql):
        sql, params = mysql.sql_check_schema("mydb")
        assert isinstance(sql, str)
        assert params == ["mydb"]

    def test_mssql_check_database(self, mssql):
        sql, params = mssql.sql_check_database("master")
        assert "sys.databases" in sql
        assert params == ["master"]

    def test_mssql_check_schema(self, mssql):
        sql, params = mssql.sql_check_schema("dbo")
        assert "sys.schemas" in sql
        assert params == ["dbo"]

    def test_postgres_check_database(self, postgres):
        sql, params = postgres.sql_check_database("mydb")
        assert "pg_database" in sql
        assert params == ["mydb"]

    def test_postgres_check_schema(self, postgres):
        sql, params = postgres.sql_check_schema("public")
        assert "pg_namespace" in sql
        assert params == ["public"]


# ---------------------------------------------------------------------------
# MSSQL-specific: cross-database queries
# ---------------------------------------------------------------------------

class TestMSSQLCrossDB:
    def test_list_schemas_in_db(self, mssql):
        sql = mssql.sql_list_schemas_in_db("other_db")
        assert "[other_db]" in sql
        assert "sys.schemas" in sql

    def test_list_tables_in_db(self, mssql):
        sql = mssql.sql_list_tables_in_db("other_db")
        assert "[other_db]" in sql

    def test_list_routines_in_db(self, mssql):
        sql = mssql.sql_list_routines_in_db("other_db")
        assert "[other_db]" in sql

    def test_routine_signatures_in_db(self, mssql):
        sql = mssql.sql_routine_signatures_in_db("other_db")
        assert "[other_db]" in sql


# ---------------------------------------------------------------------------
# Database DDL generation (admin wizards)
# ---------------------------------------------------------------------------

class TestDDLIdentifierSafety:
    """Generated DDL cannot use bound parameters for identifiers, so the
    escaping below is the only thing standing between a typed name and
    injected SQL."""

    @pytest.mark.parametrize("driver_cls", ALL_DRIVERS)
    def test_rejects_control_characters(self, driver_cls):
        d = driver_cls()
        for bad in ("a\nb", "a\rb", "a\tb", "a\x00b"):
            with pytest.raises(ValueError):
                d.quote_ddl_identifier(bad)

    @pytest.mark.parametrize("driver_cls", ALL_DRIVERS)
    def test_rejects_empty_and_overlong(self, driver_cls):
        d = driver_cls()
        for bad in ("", "   ", "x" * 129):
            with pytest.raises(ValueError):
                d.quote_ddl_identifier(bad)

    def test_mssql_escapes_closing_bracket(self, mssql):
        assert mssql.quote_ddl_identifier("a]b") == "[a]]b]"

    def test_postgres_escapes_double_quote(self, postgres):
        assert postgres.quote_ddl_identifier('a"b') == '"a""b"'

    def test_mssql_injection_stays_inside_identifier(self, mssql):
        out = mssql.quote_ddl_identifier("x]; DROP DATABASE [master]; --")
        assert out.startswith("[") and out.endswith("]")
        # Every interior ] is doubled, so none of them closes the identifier.
        assert out[1:-1].count("]") % 2 == 0

    def test_literal_escapes_single_quote(self, mssql, postgres):
        assert mssql.quote_ddl_literal("O'Brien") == "'O''Brien'"
        assert postgres.quote_ddl_literal("O'Brien") == "'O''Brien'"


class TestDatabaseDDLSupport:
    def test_mssql_and_postgres_support_database_ddl(self, mssql, postgres):
        assert mssql.supports_database_ddl
        assert postgres.supports_database_ddl

    def test_other_drivers_opt_out(self, mysql, sqlite, ansi):
        for d in (mysql, sqlite, ansi):
            assert not d.supports_database_ddl
            assert d.database_options() == []
            with pytest.raises(NotImplementedError):
                d.sql_create_database({"name": "x"})

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_options_are_well_formed(self, driver_cls):
        for spec in driver_cls().database_options():
            assert spec["key"] and spec["label"]
            assert spec["type"] in ("string", "int", "bool", "choice",
                                    "completing", "text", "path")
            if spec["type"] == "choice":
                assert spec.get("choices"), spec["key"]
                for choice in spec["choices"]:
                    assert len(choice) == 2

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_lookup_fields_degrade_without_a_cursor(self, driver_cls):
        # Validation calls database_options() with no cursor; a lookup
        # field must fall back to free text rather than an empty menu.
        for spec in driver_cls().database_options():
            if spec["type"] == "choice":
                assert spec.get("choices"), spec["key"]
            assert spec["type"] != "completing" or spec.get("completions")

    def test_lookup_failures_are_not_fatal(self, mssql):
        class Boom:
            def execute(self, *a):
                raise RuntimeError("no such table")

        # A server that rejects the lookup query still yields a usable form.
        specs = mssql.database_options(Boom())
        assert [s for s in specs if s["key"] == "name"]
        assert all(s["type"] != "choice" or s.get("choices") for s in specs)

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_name_is_the_only_required_field(self, driver_cls):
        required = [s["key"] for s in driver_cls().database_options()
                    if s.get("required")]
        assert required == ["name"]


class TestMSSQLDatabaseDDL:
    def test_minimal_create(self, mssql):
        assert mssql.sql_create_database({"name": "Foo"}) == \
            ["CREATE DATABASE [Foo]"]

    def test_file_clauses_only_when_directory_given(self, mssql):
        # SIZE/FILEGROWTH cannot appear without NAME/FILENAME.
        sql = mssql.sql_create_database(
            {"name": "Foo", "data_size_mb": 512, "log_size_mb": 64})[0]
        assert "ON PRIMARY" not in sql and "LOG ON" not in sql

    def test_full_create(self, mssql):
        # The form collects directories; file names are composed here.
        stmts = mssql.sql_create_database({
            "name": "Foo", "owner": "sa", "collation": "Latin1_General_BIN",
            "recovery_model": "SIMPLE",
            "data_dir": "D:\\Data", "data_size_mb": 512,
            "data_growth": 128, "log_dir": "E:\\Logs"})
        assert "ON PRIMARY (NAME = [Foo], FILENAME = 'D:\\Data\\Foo.mdf'" \
            in stmts[0]
        assert "SIZE = 512MB" in stmts[0] and "FILEGROWTH = 128MB" in stmts[0]
        assert "LOG ON (NAME = [Foo_log], FILENAME = 'E:\\Logs\\Foo_log.ldf'" \
            in stmts[0]
        assert "COLLATE Latin1_General_BIN" in stmts[0]
        assert stmts[1] == "ALTER DATABASE [Foo] SET RECOVERY SIMPLE"
        assert stmts[2].startswith("ALTER AUTHORIZATION ON DATABASE::[Foo]")

    def test_posix_directories_compose_with_slashes(self, mssql):
        # SQL Server on Linux: the separator follows the directory given,
        # not the platform Emacs happens to be running on.
        sql = mssql.sql_create_database(
            {"name": "Foo", "data_dir": "/var/opt/mssql/data"})[0]
        assert "FILENAME = '/var/opt/mssql/data/Foo.mdf'" in sql

    def test_log_directory_alone_is_ignored(self, mssql):
        # LOG ON is only legal following an ON clause.
        sql = mssql.sql_create_database(
            {"name": "Foo", "log_dir": "E:\\Logs"})[0]
        assert "LOG ON" not in sql

    def test_rejects_unknown_recovery_model(self, mssql):
        with pytest.raises(ValueError):
            mssql.sql_create_database({"name": "Foo",
                                       "recovery_model": "NONSENSE"})

    def test_drop_without_force(self, mssql):
        assert mssql.sql_drop_database("Foo") == ["DROP DATABASE [Foo]"]

    def test_drop_with_force_evicts_sessions_first(self, mssql):
        stmts = mssql.sql_drop_database("Foo", force=True)
        assert "SINGLE_USER WITH ROLLBACK IMMEDIATE" in stmts[0]
        assert stmts[-1] == "DROP DATABASE [Foo]"

    def test_fallback_database(self, mssql):
        assert mssql.safe_fallback_database() == "master"


class TestPostgresDatabaseDDL:
    def test_minimal_create(self, postgres):
        assert postgres.sql_create_database({"name": "foo"}) == \
            ['CREATE DATABASE "foo"']

    def test_identifier_vs_literal_clauses(self, postgres):
        sql = postgres.sql_create_database({
            "name": "foo", "owner": "bob", "template": "template0",
            "encoding": "UTF8", "lc_collate": "C"})[0]
        # OWNER/TEMPLATE take identifiers; ENCODING/LC_* take literals.
        assert 'OWNER = "bob"' in sql
        assert 'TEMPLATE = "template0"' in sql
        assert "ENCODING = 'UTF8'" in sql
        assert "LC_COLLATE = 'C'" in sql

    def test_connection_limit_omitted_when_unlimited(self, postgres):
        for value in (-1, "-1", "", None):
            sql = postgres.sql_create_database(
                {"name": "foo", "connection_limit": value})[0]
            assert "CONNECTION LIMIT" not in sql

    def test_connection_limit_emitted_when_set(self, postgres):
        sql = postgres.sql_create_database(
            {"name": "foo", "connection_limit": 5})[0]
        assert "CONNECTION LIMIT = 5" in sql

    def test_drop_with_force_terminates_backends(self, postgres):
        stmts = postgres.sql_drop_database("foo", force=True)
        # pg_terminate_backend keeps this working before PG13, where
        # DROP DATABASE ... WITH (FORCE) does not exist.
        assert "pg_terminate_backend" in stmts[0]
        assert "pg_backend_pid()" in stmts[0]
        assert stmts[-1] == 'DROP DATABASE "foo"'

    def test_fallback_database(self, postgres):
        assert postgres.safe_fallback_database() == "postgres"


# ---------------------------------------------------------------------------
# Server-side path browsing
# ---------------------------------------------------------------------------

class TestPathHelpers:
    """Path arithmetic happens server-side because the separator depends
    on the server's OS, which the Emacs client cannot know."""

    def test_mssql_and_postgres_support_browsing(self, mssql, postgres):
        assert mssql.supports_path_browse
        assert postgres.supports_path_browse

    def test_other_drivers_opt_out(self, mysql, sqlite, ansi):
        for d in (mysql, sqlite, ansi):
            assert not d.supports_path_browse
            with pytest.raises(NotImplementedError):
                d.browse_path(None, "/tmp")

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_join_follows_the_directory_separator(self, driver_cls):
        d = driver_cls()
        assert d.join_path("/var/opt/mssql/data", "a.mdf") == \
            "/var/opt/mssql/data/a.mdf"
        assert d.join_path("D:\\Data", "a.mdf") == "D:\\Data\\a.mdf"
        # A trailing separator must not double up.
        assert d.join_path("/var/data/", "a.mdf") == "/var/data/a.mdf"
        assert d.join_path("D:\\Data\\", "a.mdf") == "D:\\Data\\a.mdf"

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_parent_path(self, driver_cls):
        d = driver_cls()
        assert d.parent_path("/var/opt/mssql/data") == "/var/opt/mssql"
        assert d.parent_path("/var/opt/mssql/data/") == "/var/opt/mssql"
        assert d.parent_path("D:\\Data\\Sub") == "D:\\Data"
        # Roots have no parent to climb to.
        assert d.parent_path("/var") == "/"
        assert d.parent_path("/") is None
        assert d.parent_path("") is None
        assert d.parent_path(None) is None

    def test_windows_drive_root_keeps_its_separator(self, mssql):
        # "C:" is not a usable path; "C:\" is.
        assert mssql.parent_path("C:\\Data") == "C:\\"

    def test_default_paths_without_a_cursor(self, mssql, postgres):
        assert mssql.default_paths(None) == {}
        assert postgres.default_paths(None) == {}

    def test_default_paths_survive_a_failing_server(self, mssql, postgres):
        class Boom:
            def execute(self, *a):
                raise RuntimeError("permission denied")

        assert mssql.default_paths(Boom()) == {}
        assert postgres.default_paths(Boom()) == {}


# ---------------------------------------------------------------------------
# File size limits and growth units
# ---------------------------------------------------------------------------

class TestMSSQLFileLimits:
    def test_maxsize_defaults_to_unlimited(self, mssql):
        sql = mssql.sql_create_database(
            {"name": "Foo", "data_dir": "D:\\Data"})[0]
        assert "MAXSIZE = UNLIMITED" in sql

    def test_maxsize_when_given(self, mssql):
        sql = mssql.sql_create_database(
            {"name": "Foo", "data_dir": "D:\\Data", "data_max_mb": 4096})[0]
        assert "MAXSIZE = 4096MB" in sql
        assert "MAXSIZE = UNLIMITED" not in sql.split("LOG ON")[0]

    def test_growth_in_percent(self, mssql):
        sql = mssql.sql_create_database(
            {"name": "Foo", "data_dir": "D:\\Data",
             "data_growth": 10, "data_growth_unit": "%"})[0]
        assert "FILEGROWTH = 10%" in sql

    def test_growth_defaults_to_megabytes(self, mssql):
        sql = mssql.sql_create_database(
            {"name": "Foo", "data_dir": "D:\\Data", "data_growth": 64})[0]
        assert "FILEGROWTH = 64MB" in sql

    def test_maxsize_precedes_filegrowth(self, mssql):
        # SQL Server rejects the file spec if these are the other way round.
        sql = mssql.sql_create_database(
            {"name": "Foo", "data_dir": "D:\\Data",
             "data_growth": 64, "data_max_mb": 1024})[0]
        assert sql.index("MAXSIZE") < sql.index("FILEGROWTH")

    def test_log_limits_are_independent(self, mssql):
        sql = mssql.sql_create_database(
            {"name": "Foo", "data_dir": "D:\\D", "data_max_mb": 100,
             "log_dir": "E:\\L", "log_max_mb": 50,
             "log_growth": 5, "log_growth_unit": "%"})[0]
        data, log = sql.split("LOG ON")
        assert "MAXSIZE = 100MB" in data
        assert "MAXSIZE = 50MB" in log and "FILEGROWTH = 5%" in log

    def test_compatibility_level(self, mssql):
        stmts = mssql.sql_create_database(
            {"name": "Foo", "compatibility_level": "150"})
        assert any("SET COMPATIBILITY_LEVEL = 150" in s for s in stmts)

    def test_rejects_non_numeric_compatibility_level(self, mssql):
        with pytest.raises(ValueError):
            mssql.sql_create_database(
                {"name": "Foo", "compatibility_level": "150; DROP"})


# ---------------------------------------------------------------------------
# Altering an existing database
# ---------------------------------------------------------------------------

class TestDatabaseAlter:
    def test_mssql_and_postgres_support_alter(self, mssql, postgres):
        assert mssql.supports_database_alter
        assert postgres.supports_database_alter

    def test_other_drivers_opt_out(self, mysql, sqlite, ansi):
        for d in (mysql, sqlite, ansi):
            assert not d.supports_database_alter
            assert d.database_settings(None, "x") == {}
            assert d.settings_options(None, {}) == []
            with pytest.raises(NotImplementedError):
                d.sql_alter_database("x", {}, {})

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_unchanged_settings_emit_nothing(self, driver_cls):
        d = driver_cls()
        current = {"name": "Foo", "owner": "sa", "recovery_model": "FULL",
                   "compatibility_level": "160", "collation": "X",
                   "user_access": "MULTI_USER", "read_only": False,
                   "auto_shrink": False, "auto_close": False,
                   "auto_create_stats": True, "auto_update_stats": True,
                   "connection_limit": -1, "tablespace": "pg_default",
                   "allow_connections": True}
        # Submitting the form untouched must not take locks for nothing.
        assert d.sql_alter_database("Foo", dict(current), current) == []

    def test_mssql_only_changed_settings(self, mssql):
        current = {"name": "Foo", "owner": "sa", "recovery_model": "FULL",
                   "compatibility_level": "160", "collation": "X",
                   "user_access": "MULTI_USER", "read_only": False,
                   "auto_shrink": False, "auto_create_stats": True}
        opts = dict(current, recovery_model="SIMPLE", auto_shrink=True)
        stmts = mssql.sql_alter_database("Foo", opts, current)
        assert len(stmts) == 2
        assert any("SET RECOVERY SIMPLE" in s for s in stmts)
        assert any("SET AUTO_SHRINK ON" in s for s in stmts)

    def test_mssql_read_only_toggles_both_ways(self, mssql):
        on = mssql.sql_alter_database("Foo", {"read_only": True},
                                      {"read_only": False})
        off = mssql.sql_alter_database("Foo", {"read_only": False},
                                       {"read_only": True})
        assert "SET READ_ONLY" in on[0]
        assert "SET READ_WRITE" in off[0]

    def test_mssql_rename_comes_last(self, mssql):
        current = {"name": "Foo", "recovery_model": "FULL"}
        stmts = mssql.sql_alter_database(
            "Foo", {"name": "Bar", "recovery_model": "SIMPLE"}, current)
        # Every other statement addresses the original name, so renaming
        # first would leave them pointing at a database that is gone.
        assert "MODIFY NAME" in stmts[-1]
        assert all("[Foo]" in s for s in stmts)

    def test_postgres_rename_comes_last(self, postgres):
        stmts = postgres.sql_alter_database(
            "foo", {"name": "bar", "connection_limit": 5},
            {"name": "foo", "connection_limit": -1})
        assert "RENAME TO" in stmts[-1]
        assert all('"foo"' in s for s in stmts)

    def test_postgres_allow_connections(self, postgres):
        stmts = postgres.sql_alter_database(
            "foo", {"allow_connections": False}, {"allow_connections": True})
        assert "ALLOW_CONNECTIONS false" in stmts[0]

    def test_alter_rejects_bad_values(self, mssql):
        with pytest.raises(ValueError):
            mssql.sql_alter_database("Foo", {"recovery_model": "BOGUS"},
                                     {"recovery_model": "FULL"})
        with pytest.raises(ValueError):
            mssql.sql_alter_database("Foo", {"user_access": "BOGUS"},
                                     {"user_access": "MULTI_USER"})
        with pytest.raises(ValueError):
            mssql.sql_alter_database("Foo", {"compatibility_level": "x"},
                                     {"compatibility_level": "160"})

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_settings_options_are_well_formed(self, driver_cls):
        d = driver_cls()
        for spec in d.settings_options(None, {}):
            assert spec["key"] and spec["label"]
            assert spec["type"] in ("string", "int", "bool", "choice",
                                    "completing", "text", "path")
            if spec["type"] == "choice":
                assert spec.get("choices"), spec["key"]


# ---------------------------------------------------------------------------
# Database file management
# ---------------------------------------------------------------------------

class TestFileManagement:
    def test_only_mssql_manages_files(self, mssql, postgres, mysql, sqlite,
                                      ansi):
        # PostgreSQL manages its own storage; tablespaces are server-level
        # rather than per-database, so there is nothing honest to show.
        assert mssql.supports_file_management
        for d in (postgres, mysql, sqlite, ansi):
            assert not d.supports_file_management
            assert d.database_files(None, "x") == []
            with pytest.raises(NotImplementedError):
                d.sql_add_file("x", {})

    def test_add_data_file(self, mssql):
        sql = mssql.sql_add_file("Foo", {
            "logical": "Foo_2", "file_type": "ROWS", "filegroup": "SECONDARY",
            "directory": "D:\\Data", "size_mb": 16, "growth": 8,
            "max_mb": 128})[0]
        assert "ALTER DATABASE [Foo] ADD FILE" in sql
        assert "NAME = [Foo_2]" in sql
        assert "FILENAME = 'D:\\Data\\Foo_2.ndf'" in sql
        assert "SIZE = 16MB" in sql
        assert "MAXSIZE = 128MB" in sql
        assert "FILEGROWTH = 8MB" in sql
        assert "TO FILEGROUP [SECONDARY]" in sql

    def test_add_log_file_uses_ldf_and_no_filegroup(self, mssql):
        sql = mssql.sql_add_file("Foo", {
            "logical": "Foo_log2", "file_type": "LOG", "filegroup": "PRIMARY",
            "directory": "E:\\Logs", "size_mb": 8})[0]
        assert "ADD LOG FILE" in sql
        assert "Foo_log2.ldf" in sql
        # A log file belongs to no filegroup; naming one is an error.
        assert "FILEGROUP" not in sql

    def test_add_file_requires_a_directory(self, mssql):
        with pytest.raises(ValueError):
            mssql.sql_add_file("Foo", {"logical": "Foo_2"})

    def test_add_file_validates_the_logical_name(self, mssql):
        with pytest.raises(ValueError):
            mssql.sql_add_file("Foo", {"logical": "bad\nname",
                                       "directory": "D:\\Data"})

    def test_zero_growth_disables_autogrowth(self, mssql):
        sql = mssql.sql_add_file("Foo", {
            "logical": "Foo_2", "directory": "D:\\Data", "growth": 0})[0]
        assert "FILEGROWTH = 0" in sql

    def test_modify_file_emits_nothing_when_unchanged(self, mssql):
        current = {"logical": "Foo_2", "size_mb": 16, "growth": 8,
                   "growth_unit": "MB", "max_mb": 128}
        assert mssql.sql_modify_file("Foo", dict(current), current) == []

    def test_modify_file_has_no_filename(self, mssql):
        current = {"logical": "Foo_2", "size_mb": 16, "growth": 8,
                   "growth_unit": "MB", "max_mb": 128}
        sql = mssql.sql_modify_file(
            "Foo", dict(current, size_mb=32), current)[0]
        assert "MODIFY FILE" in sql and "NAME = [Foo_2]" in sql
        # A file cannot be relocated by MODIFY FILE, so FILENAME is omitted.
        assert "FILENAME" not in sql
        assert "SIZE = 32MB" in sql

    def test_remove_file_empties_it_first(self, mssql):
        # REMOVE FILE fails while the file still holds pages, and a plain
        # shrink does not empty one.
        stmts = mssql.sql_remove_file("Foo", "Foo_2")
        assert "EMPTYFILE" in stmts[0]
        assert "[Foo]..sp_executesql" in stmts[0]
        assert stmts[-1] == "ALTER DATABASE [Foo] REMOVE FILE [Foo_2]"

    def test_remove_file_without_emptying(self, mssql):
        assert mssql.sql_remove_file("Foo", "Foo_2", empty_first=False) == \
            ["ALTER DATABASE [Foo] REMOVE FILE [Foo_2]"]

    def test_remove_file_validates_names(self, mssql):
        with pytest.raises(ValueError):
            mssql.sql_remove_file("Foo", "bad\nname")

    def test_shrink_runs_in_the_target_database(self, mssql):
        sql = mssql.sql_shrink_file("Foo", "Foo_2", 64)[0]
        # DBCC SHRINKFILE only acts on the current database, and a USE
        # would move the shared session, so it goes through sp_executesql.
        assert "[Foo]..sp_executesql" in sql
        assert "DBCC SHRINKFILE" in sql
        assert "''Foo_2''" in sql and "64" in sql

    def test_shrink_validates_both_names(self, mssql):
        with pytest.raises(ValueError):
            mssql.sql_shrink_file("Foo", "bad\nname", 64)
        with pytest.raises(ValueError):
            mssql.sql_shrink_file("bad\nname", "Foo_2", 64)

    def test_file_options_differ_for_add_and_edit(self, mssql):
        add = {f["key"] for f in mssql.file_options(None, "Foo")}
        edit = {f["key"] for f in mssql.file_options(
            None, "Foo", {"size_mb": 8, "growth": 8,
                          "growth_unit": "MB", "max_mb": 0})}
        assert {"logical", "file_type", "directory"} <= add
        # Name, path, type and filegroup are fixed once a file exists.
        assert not ({"logical", "file_type", "directory", "filegroup"} & edit)
        assert {"size_mb", "growth", "growth_unit", "max_mb"} == edit


# ---------------------------------------------------------------------------
# Logins, roles and database users
# ---------------------------------------------------------------------------

class TestSecurity:
    def test_capabilities(self, mssql, postgres, mysql, sqlite, ansi):
        assert mssql.supports_security and postgres.supports_security
        # Only MSSQL separates a server login from a per-database user.
        assert mssql.supports_user_mapping
        assert not postgres.supports_user_mapping
        for d in (mysql, sqlite, ansi):
            assert not d.supports_security
            with pytest.raises(NotImplementedError):
                d.sql_create_principal({"name": "x"})

    def test_nouns_match_the_dialect(self, mssql, postgres):
        assert mssql.principal_noun == "login"
        assert postgres.principal_noun == "role"

    def test_mssql_sql_login_requires_a_password(self, mssql):
        with pytest.raises(ValueError):
            mssql.sql_create_principal(
                {"name": "bob", "login_type": "SQL_LOGIN"})

    def test_mssql_windows_login_needs_no_password(self, mssql):
        sql = mssql.sql_create_principal(
            {"name": "DOMAIN\\bob", "login_type": "WINDOWS_LOGIN"})[0]
        assert "FROM WINDOWS" in sql
        assert "PASSWORD" not in sql

    def test_mssql_create_login_with_roles(self, mssql):
        stmts = mssql.sql_create_principal({
            "name": "bob", "login_type": "SQL_LOGIN", "password": "s3cret",
            "check_policy": True, "default_database": "appdb",
            "roles": ["dbcreator"]})
        assert "CREATE LOGIN [bob] WITH PASSWORD = 's3cret'" in stmts[0]
        assert "CHECK_POLICY = ON" in stmts[0]
        assert "DEFAULT_DATABASE = [appdb]" in stmts[0]
        assert "ALTER SERVER ROLE [dbcreator] ADD MEMBER [bob]" in stmts

    def test_blank_password_leaves_it_alone(self, mssql, postgres):
        # A blank field must mean "keep the current password", never
        # "set the password to empty".
        assert not any(
            "PASSWORD" in s for s in
            mssql.sql_alter_principal("bob", {"password": ""}, {}))
        assert not any(
            "PASSWORD" in s for s in
            postgres.sql_alter_principal("bob", {"password": ""}, {}))

    def test_role_membership_is_diffed(self, mssql):
        stmts = mssql.sql_alter_principal(
            "bob", {"roles": ["dbcreator", "processadmin"]},
            {"roles": ["dbcreator", "diskadmin"]})
        assert any("[processadmin] ADD MEMBER [bob]" in s for s in stmts)
        assert any("[diskadmin] DROP MEMBER [bob]" in s for s in stmts)
        # An unchanged membership must not be re-granted.
        assert not any("[dbcreator]" in s for s in stmts)

    def test_postgres_membership_is_diffed(self, postgres):
        stmts = postgres.sql_alter_principal(
            "bob", {"roles": ["analysts"]}, {"roles": ["admins"]})
        assert any('GRANT "analysts" TO "bob"' in s for s in stmts)
        assert any('REVOKE "admins" FROM "bob"' in s for s in stmts)

    def test_postgres_flags_only_when_changed(self, postgres):
        current = {"can_login": True, "superuser": False, "create_db": False,
                   "create_role": False, "inherit": True, "replication": False}
        assert postgres.sql_alter_principal("bob", dict(current), current) == []
        stmts = postgres.sql_alter_principal(
            "bob", dict(current, create_db=True), current)
        assert "CREATEDB" in stmts[0] and "NOSUPERUSER" not in stmts[0]

    def test_rename_comes_last(self, mssql, postgres):
        ms = mssql.sql_alter_principal(
            "bob", {"name": "bobby", "disabled": True},
            {"name": "bob", "disabled": False})
        assert "WITH NAME" in ms[-1] and all("[bob]" in s for s in ms)
        pg = postgres.sql_alter_principal(
            "bob", {"name": "bobby", "create_db": True},
            {"name": "bob", "create_db": False})
        assert "RENAME TO" in pg[-1] and all('"bob"' in s for s in pg)

    def test_force_drop_ends_sessions_first(self, mssql, postgres):
        ms = mssql.sql_drop_principal("bob", force=True)
        assert "KILL" in ms[0] and ms[-1] == "DROP LOGIN [bob]"
        assert mssql.sql_drop_principal("bob") == ["DROP LOGIN [bob]"]
        pg = postgres.sql_drop_principal("bob", force=True)
        assert "pg_terminate_backend" in pg[0]
        assert pg[-1] == 'DROP ROLE "bob"'

    def test_user_mapping_runs_in_the_target_database(self, mssql):
        stmts = mssql.sql_add_user_mapping(
            "bob", {"database": "appdb", "username": "bob",
                    "roles": ["db_datareader"]})
        # CREATE USER and ALTER ROLE only act on the current database.
        assert all("[appdb]..sp_executesql" in s for s in stmts)
        assert "CREATE USER" in stmts[0]
        assert any("db_datareader" in s for s in stmts)

    def test_user_roles_are_diffed(self, mssql):
        stmts = mssql.sql_set_user_roles(
            "appdb", "bob", ["db_datareader"], ["db_datawriter"])
        assert any("ADD MEMBER" in s and "db_datareader" in s for s in stmts)
        assert any("DROP MEMBER" in s and "db_datawriter" in s for s in stmts)
        assert mssql.sql_set_user_roles("appdb", "bob", ["a"], ["a"]) == []

    def test_principal_names_are_validated(self, mssql, postgres):
        for d in (mssql, postgres):
            with pytest.raises(ValueError):
                d.sql_create_principal({"name": "bad\nname",
                                        "password": "x",
                                        "login_type": "SQL_LOGIN"})
            with pytest.raises(ValueError):
                d.sql_drop_principal("bad\nname", force=True)

    def test_password_is_escaped_not_interpolated(self, mssql, postgres):
        # A quote in a password must not break out of the literal.
        ms = mssql.sql_create_principal(
            {"name": "bob", "login_type": "SQL_LOGIN",
             "password": "it's'; DROP LOGIN [sa]--"})[0]
        assert "''s''" in ms
        pg = postgres.sql_create_principal(
            {"name": "bob", "password": "it's"})[0]
        assert "'it''s'" in pg

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_principal_options_are_well_formed(self, driver_cls):
        for spec in driver_cls().principal_options(None):
            assert spec["key"] and spec["label"]
            assert spec["type"] in ("string", "int", "bool", "choice",
                                    "completing", "multi", "password",
                                    "text", "path")
            if spec["type"] == "choice":
                assert spec.get("choices") is not None, spec["key"]

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_password_field_exists_and_starts_blank(self, driver_cls):
        specs = {f["key"]: f for f in driver_cls().principal_options(None)}
        assert specs["password"]["type"] == "password"
        assert specs["password"]["default"] == ""


# ---------------------------------------------------------------------------
# Backup and restore
# ---------------------------------------------------------------------------

class TestBackupRestore:
    def test_only_mssql_backs_up_through_sql(self, mssql, postgres, mysql,
                                             sqlite, ansi):
        # pg_dump and pg_basebackup are external programs, so there is
        # nothing to drive over a connection.
        assert mssql.supports_backup
        for d in (postgres, mysql, sqlite, ansi):
            assert not d.supports_backup
            with pytest.raises(NotImplementedError):
                d.sql_backup("Foo", {})

    def test_full_backup(self, mssql):
        sql = mssql.sql_backup("Foo", {
            "backup_type": "FULL", "directory": "D:\\Backups",
            "filename": "Foo.bak", "overwrite": True,
            "compression": True, "checksum": True})[0]
        assert "BACKUP DATABASE [Foo] TO DISK = 'D:\\Backups\\Foo.bak'" in sql
        assert "INIT" in sql and "NOINIT" not in sql
        assert "COMPRESSION" in sql and "CHECKSUM" in sql

    def test_append_rather_than_overwrite(self, mssql):
        sql = mssql.sql_backup("Foo", {
            "directory": "D:\\B", "filename": "Foo.bak",
            "overwrite": False})[0]
        assert "NOINIT" in sql

    def test_differential_and_log(self, mssql):
        diff = mssql.sql_backup("Foo", {
            "backup_type": "DIFFERENTIAL", "directory": "D:\\B",
            "filename": "Foo.bak"})[0]
        assert "BACKUP DATABASE" in diff and "DIFFERENTIAL" in diff
        log = mssql.sql_backup("Foo", {
            "backup_type": "LOG", "directory": "D:\\B",
            "filename": "Foo.trn"})[0]
        assert log.startswith("BACKUP LOG [Foo]")

    def test_verify_is_a_separate_statement(self, mssql):
        stmts = mssql.sql_backup("Foo", {
            "directory": "D:\\B", "filename": "Foo.bak", "verify": True})
        assert len(stmts) == 2
        assert stmts[1].startswith("RESTORE VERIFYONLY")

    def test_rejects_unknown_backup_type(self, mssql):
        with pytest.raises(ValueError):
            mssql.sql_backup("Foo", {"backup_type": "SIDEWAYS",
                                     "directory": "D:\\B",
                                     "filename": "Foo.bak"})

    def test_file_name_may_not_escape_the_directory(self, mssql):
        # The name goes into a string literal, so a separator in it would
        # silently write somewhere other than the chosen directory.
        for bad in ("../escape.bak", "sub/Foo.bak", "sub\\Foo.bak"):
            with pytest.raises(ValueError):
                mssql.sql_backup("Foo", {"directory": "D:\\B",
                                         "filename": bad})

    def test_backup_requires_directory_and_name(self, mssql):
        with pytest.raises(ValueError):
            mssql.sql_backup("Foo", {"directory": "D:\\B"})
        with pytest.raises(ValueError):
            mssql.sql_backup("Foo", {"filename": "Foo.bak"})

    def _files(self):
        return [{"logical": "Foo", "physical": "X:\\old\\Foo.mdf",
                 "type": "D"},
                {"logical": "Foo_log", "physical": "X:\\old\\Foo_log.ldf",
                 "type": "L"}]

    def test_restore_moves_every_file(self, mssql):
        sql = mssql.sql_restore("Foo", {
            "target": "Bar", "source": "D:\\B\\Foo.bak", "position": 1,
            "data_dir": "D:\\Data", "log_dir": "E:\\Logs",
            "replace": True, "recovery": True}, self._files())[1]
        # Without an explicit MOVE the restore writes over the paths
        # recorded in the backup, which belong to the source database.
        assert "MOVE 'Foo' TO 'D:\\Data\\Bar.mdf'" in sql
        assert "MOVE 'Foo_log' TO 'E:\\Logs\\Bar_log.ldf'" in sql
        assert "REPLACE" in sql and "RECOVERY" in sql

    def test_restore_guards_the_exclusive_access_step(self, mssql):
        stmts = mssql.sql_restore("Foo", {
            "target": "Bar", "source": "D:\\B\\Foo.bak",
            "data_dir": "D:\\Data", "log_dir": "D:\\Data",
            "replace": True, "recovery": True}, self._files())
        # The target is commonly created by the restore itself, and
        # ALTER DATABASE on a database that does not exist is an error.
        assert stmts[0].startswith("IF DB_ID('Bar') IS NOT NULL")
        assert "SINGLE_USER" in stmts[0]
        assert stmts[-1].startswith("IF DB_ID('Bar') IS NOT NULL")
        assert "MULTI_USER" in stmts[-1]

    def test_restore_without_replace_takes_no_lock(self, mssql):
        stmts = mssql.sql_restore("Foo", {
            "target": "Bar", "source": "D:\\B\\Foo.bak",
            "data_dir": "D:\\D", "log_dir": "D:\\D",
            "replace": False, "recovery": True}, self._files())
        assert len(stmts) == 1
        assert "SINGLE_USER" not in stmts[0]
        assert "REPLACE" not in stmts[0]

    def test_norecovery_leaves_it_restoring(self, mssql):
        stmts = mssql.sql_restore("Foo", {
            "target": "Bar", "source": "D:\\B\\Foo.bak",
            "data_dir": "D:\\D", "log_dir": "D:\\D",
            "replace": True, "recovery": False}, self._files())
        assert any("NORECOVERY" in s for s in stmts)
        # Nothing may bring it online while further logs are expected.
        assert not any("MULTI_USER" in s for s in stmts)

    def test_restore_requires_a_source(self, mssql):
        with pytest.raises(ValueError):
            mssql.sql_restore("Foo", {"target": "Bar"}, self._files())

    def test_restore_validates_the_target(self, mssql):
        with pytest.raises(ValueError):
            mssql.sql_restore("Foo", {"target": "bad\nname",
                                      "source": "D:\\B\\Foo.bak"},
                              self._files())

    def test_backup_options_are_well_formed(self, mssql):
        for spec in mssql.backup_options(None, "Foo"):
            assert spec["key"] and spec["label"]
            assert spec["type"] in ("string", "int", "bool", "choice",
                                    "completing", "multi", "password",
                                    "text", "path")
        for spec in mssql.restore_options(None, "Foo"):
            assert spec["key"] and spec["label"]


# ---------------------------------------------------------------------------
# Schemas and tables
# ---------------------------------------------------------------------------

class TestSchemaAndTableDDL:
    def test_capabilities(self, mssql, postgres, mysql, sqlite, ansi):
        assert mssql.supports_schema_ddl and postgres.supports_schema_ddl
        for d in (mysql, sqlite, ansi):
            assert not d.supports_schema_ddl
            with pytest.raises(NotImplementedError):
                d.sql_create_schema({"name": "x"})

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_create_schema(self, driver_cls):
        d = driver_cls()
        assert "CREATE SCHEMA" in d.sql_create_schema({"name": "app"})[0]
        owned = d.sql_create_schema({"name": "app", "owner": "bob"})[0]
        assert "AUTHORIZATION" in owned

    def test_postgres_cascade_is_opt_in(self, postgres):
        assert postgres.sql_drop_schema("app") == ['DROP SCHEMA "app"']
        assert "CASCADE" in postgres.sql_drop_schema("app", cascade=True)[0]

    def test_mssql_has_no_cascade(self, mssql):
        # SQL Server has no CASCADE; asking for it must not invent one.
        assert mssql.sql_drop_schema("app", cascade=True) == \
            ["DROP SCHEMA [app]"]

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_column_types_are_offered(self, driver_cls):
        types = driver_cls().column_types()
        assert len(types) > 10
        assert all(len(t) == 2 for t in types)

    def test_create_table_with_primary_key(self, mssql):
        sql = mssql.sql_create_table("dbo", {
            "name": "orders",
            "columns": [["id", "INT IDENTITY(1,1)", False, True, ""],
                        ["label", "NVARCHAR(255)", False, False, ""],
                        ["amt", "DECIMAL(18,2)", True, False, "0"]]})[0]
        assert "CREATE TABLE [dbo].[orders]" in sql
        assert "[id] INT IDENTITY(1,1) NOT NULL" in sql
        assert "[amt] DECIMAL(18,2) DEFAULT 0" in sql
        assert "CONSTRAINT [PK_orders] PRIMARY KEY ([id])" in sql

    def test_composite_primary_key(self, postgres):
        sql = postgres.sql_create_table("public", {
            "name": "pairs",
            "columns": [["a", "INTEGER", False, True, ""],
                        ["b", "INTEGER", False, True, ""]]})[0]
        assert 'PRIMARY KEY ("a", "b")' in sql

    def test_no_primary_key_emits_no_constraint(self, postgres):
        sql = postgres.sql_create_table("public", {
            "name": "flat",
            "columns": [["a", "INTEGER", True, False, ""]]})[0]
        assert "PRIMARY KEY" not in sql

    def test_table_needs_at_least_one_column(self, mssql):
        with pytest.raises(ValueError):
            mssql.sql_create_table("dbo", {"name": "empty", "columns": []})

    def test_blank_rows_are_skipped(self, mssql):
        # The list widget leaves an empty row behind when one is cleared.
        sql = mssql.sql_create_table("dbo", {
            "name": "t",
            "columns": [["id", "INT", False, True, ""],
                        ["", "", True, False, ""]]})[0]
        assert sql.count("\n  ") == 2  # the column and the constraint

    def test_unknown_column_type_is_refused(self, mssql):
        with pytest.raises(ValueError):
            mssql.sql_create_table("dbo", {
                "name": "t",
                "columns": [["id", "INT) ; DROP TABLE x --", False, False,
                             ""]]})

    def test_column_names_are_validated(self, postgres):
        with pytest.raises(ValueError):
            postgres.sql_create_table("public", {
                "name": "t",
                "columns": [["bad\nname", "INTEGER", True, False, ""]]})

    def test_drop_table_is_qualified(self, mssql, postgres):
        assert mssql.sql_drop_table("dbo", "t") == ["DROP TABLE [dbo].[t]"]
        assert postgres.sql_drop_table("public", "t") == \
            ['DROP TABLE "public"."t"']


class TestDefaultExpressions:
    """A DEFAULT is an expression, so it can be neither quoted like a
    literal nor bound as a parameter.  Only shapes that cannot carry a
    statement are accepted."""

    @pytest.mark.parametrize("value", [
        "0", "-1", "3.14", "'hello'", "'it''s'", "NULL", "TRUE",
        "GETDATE()", "now()", "CURRENT_TIMESTAMP",
        # A server may normalise a numeric default into these forms, and
        # the editor has to accept back what it was given.
        "12345678901234.", "-1.", ".5", "-0.25"])
    def test_accepted(self, mssql, value):
        assert mssql.validate_default(value) == value

    @pytest.mark.parametrize("value", [
        "0); DROP TABLE x --",
        "'a' || (SELECT 1)",
        "GETDATE(); DELETE FROM t",
        "1+1",
        "foo(1)",
        "'unclosed",
        "(SELECT max(id) FROM t)",
        "1.2.3", ".", "-", "1e5"])
    def test_refused(self, mssql, value):
        with pytest.raises(ValueError):
            mssql.validate_default(value)

    def test_refused_defaults_reach_the_user(self, postgres):
        with pytest.raises(ValueError) as err:
            postgres.sql_create_table("public", {
                "name": "t",
                "columns": [["a", "INTEGER", True, False,
                             "0); DROP TABLE x --"]]})
        assert "DEFAULT" in str(err.value)


# ---------------------------------------------------------------------------
# Altering an existing table
# ---------------------------------------------------------------------------

class TestAlterTable:
    def _current(self):
        return [["id", "INT", False, True, ""],
                ["label", "NVARCHAR(50)", True, False, ""],
                ["amt", "DECIMAL(18,2)", True, False, "0"]]

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_unchanged_columns_emit_nothing(self, driver_cls):
        d = driver_cls()
        current = self._current()
        assert d.sql_alter_table("s", "t", {"columns": current},
                                 current) == []

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_a_function_default_is_not_a_change(self, driver_cls):
        # SQL Server echoes GETDATE() back as getdate(); comparing case
        # sensitively would rewrite the default on every submission.
        d = driver_cls()
        before = [["made", "DATE", True, False, "getdate()"]]
        after = [["made", "DATE", True, False, "GETDATE()"]]
        assert d.sql_alter_table("s", "t", {"columns": after}, before) == []

    def test_a_quoted_default_is_compared_exactly(self, mssql):
        # A string's case is data, not syntax.
        before = [["a", "INT", True, False, "'Foo'"]]
        after = [["a", "INT", True, False, "'foo'"]]
        assert mssql.sql_alter_table("s", "t", {"columns": after}, before)

    def test_add_column(self, mssql, postgres):
        current = self._current()
        added = current + [["note", "VARCHAR(50)", True, False, "'n/a'"]]
        ms = mssql.sql_alter_table("s", "t", {"columns": added}, current)
        assert any("ADD [note] VARCHAR(50) DEFAULT 'n/a'" in s for s in ms)
        pg_current = [["id", "INTEGER", False, True, ""]]
        pg_added = pg_current + [["note", "VARCHAR(50)", True, False, ""]]
        pg = postgres.sql_alter_table("s", "t", {"columns": pg_added},
                                      pg_current)
        assert any("ADD COLUMN" in s for s in pg)

    def test_mssql_changes_type_and_nullability_together(self, mssql):
        current = self._current()
        changed = [r[:] for r in current]
        changed[1][1], changed[1][2] = "NVARCHAR(MAX)", False
        stmts = mssql.sql_alter_table("s", "t", {"columns": changed},
                                      current)
        # One ALTER COLUMN carries both here.
        assert len(stmts) == 1
        assert "ALTER COLUMN [label] NVARCHAR(MAX) NOT NULL" in stmts[0]

    def test_postgres_separates_type_and_nullability(self, postgres):
        current = [["label", "VARCHAR(50)", True, False, ""]]
        changed = [["label", "TEXT", False, False, ""]]
        stmts = postgres.sql_alter_table("s", "t", {"columns": changed},
                                         current)
        # PostgreSQL needs one statement each.
        assert len(stmts) == 2
        assert any("TYPE TEXT" in s for s in stmts)
        assert any("SET NOT NULL" in s for s in stmts)

    def test_postgres_default_set_and_dropped(self, postgres):
        current = [["a", "INTEGER", True, False, "0"]]
        assert "DROP DEFAULT" in postgres.sql_alter_table(
            "s", "t", {"columns": [["a", "INTEGER", True, False, ""]]},
            current)[0]
        assert "SET DEFAULT 5" in postgres.sql_alter_table(
            "s", "t", {"columns": [["a", "INTEGER", True, False, "5"]]},
            current)[0]

    def test_mssql_drops_the_default_constraint_first(self, mssql):
        # A column carrying a default cannot be dropped while its
        # constraint stands, and the name is server-generated.
        current = self._current()
        remaining = [r for r in current if r[0] != "amt"]
        stmts = mssql.sql_alter_table("s", "t", {"columns": remaining},
                                      current)
        assert "default_constraints" in stmts[0]
        assert "DROP COLUMN [amt]" in stmts[-1]

    def test_postgres_drops_a_column_directly(self, postgres):
        current = [["a", "INTEGER", True, False, "0"]]
        stmts = postgres.sql_alter_table("s", "t", {"columns": []}, current)
        assert len(stmts) == 1 and "DROP COLUMN" in stmts[0]

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_editing_a_name_renames_and_keeps_the_column(self, driver_cls):
        # A row submitted from the editor says what it was called when
        # the form opened, so an edited name is a rename.
        d = driver_cls()
        kind = "INTEGER" if d.dialect_name == "postgres" else "INT"
        current = [["old", kind, True, False, ""]]
        edited = [["new", kind, True, False, "", "old"]]
        added, dropped, changed, renamed = d._diff_columns(
            {"columns": edited}, current)
        assert renamed == [("old", edited[0])]
        assert not added and not dropped and not changed

        stmts = d.sql_alter_table("s", "t", {"columns": edited}, current)
        assert len(stmts) == 1
        assert "DROP COLUMN" not in stmts[0]
        assert ("sp_rename" in stmts[0] if d.dialect_name == "mssql"
                else "RENAME COLUMN" in stmts[0])

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_removing_a_row_and_adding_one_is_not_a_rename(self, driver_cls):
        # An inserted row carries no original, so it is a new column and
        # the one it replaced is dropped.
        d = driver_cls()
        kind = "INTEGER" if d.dialect_name == "postgres" else "INT"
        current = [["old", kind, True, False, ""]]
        replaced = [["new", kind, True, False, "", ""]]
        added, dropped, _changed, renamed = d._diff_columns(
            {"columns": replaced}, current)
        assert not renamed
        assert [r[0] for r in added] == ["new"]
        assert [r[0] for r in dropped] == ["old"]

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_a_rename_and_a_retype_together(self, driver_cls):
        d = driver_cls()
        old_kind = "INTEGER" if d.dialect_name == "postgres" else "INT"
        new_kind = "TEXT" if d.dialect_name == "postgres" else "NVARCHAR(MAX)"
        current = [["old", old_kind, True, False, ""]]
        edited = [["new", new_kind, True, False, "", "old"]]
        stmts = d.sql_alter_table("s", "t", {"columns": edited}, current)
        # The rename comes first, so the type change names the column by
        # what it is now called.
        assert len(stmts) == 2
        assert ("sp_rename" in stmts[0] if d.dialect_name == "mssql"
                else "RENAME COLUMN" in stmts[0])
        assert "new" in stmts[1] and "old" not in stmts[1]

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_rows_without_an_original_still_match_by_name(self, driver_cls):
        # What a scripted call and the create form send.
        d = driver_cls()
        kind = "INTEGER" if d.dialect_name == "postgres" else "INT"
        current = [["a", kind, True, False, ""]]
        assert d.sql_alter_table("s", "t", {"columns": current},
                                 current) == []
        renamed = [["b", kind, True, False, ""]]
        _added, dropped, _changed, renamed_pairs = d._diff_columns(
            {"columns": renamed}, current)
        assert not renamed_pairs
        assert [r[0] for r in dropped] == ["a"]

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_unknown_types_and_defaults_are_refused(self, driver_cls):
        d = driver_cls()
        current = [["a", "INTEGER" if d.dialect_name == "postgres"
                    else "INT", True, False, ""]]
        with pytest.raises(ValueError):
            d.sql_alter_table("s", "t", {"columns": [
                ["b", "INT); DROP TABLE x --", True, False, ""]]}, current)
        with pytest.raises(ValueError):
            d.sql_alter_table("s", "t", {"columns": [
                ["b", "INTEGER" if d.dialect_name == "postgres" else "INT",
                 True, False, "0); DROP TABLE x --"]]}, current)


class TestRenameColumn:
    """Offered separately from the column editor because a list of
    columns cannot express a rename — by name alone it is the same as
    dropping one and adding another."""

    def test_mssql_uses_sp_rename(self, mssql):
        sql = mssql.sql_rename_column("dbo", "t", "old", "new")[0]
        assert sql == "EXEC sp_rename 'dbo.t.old', 'new', 'COLUMN'"

    def test_postgres_uses_alter_table(self, postgres):
        sql = postgres.sql_rename_column("public", "t", "old", "new")[0]
        assert sql == 'ALTER TABLE "public"."t" RENAME COLUMN "old" TO "new"'

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_every_name_is_validated(self, driver_cls):
        d = driver_cls()
        for args in (("bad\nschema", "t", "a", "b"),
                     ("s", "bad\ntable", "a", "b"),
                     ("s", "t", "bad\ncol", "b"),
                     ("s", "t", "a", "bad\nnew")):
            with pytest.raises(ValueError):
                d.sql_rename_column(*args)

    def test_other_drivers_opt_out(self, mysql, sqlite, ansi):
        for d in (mysql, sqlite, ansi):
            with pytest.raises(NotImplementedError):
                d.sql_rename_column("s", "t", "a", "b")


class TestDefaultsSurviveTheServer:
    def test_a_normalised_numeric_can_be_resubmitted(self, mssql):
        """A default the server hands back must pass validation.

        SQL Server stores a large numeric default as ((12345678901234.)),
        with a trailing dot.  Rejecting that shape would leave the column
        uneditable through the very editor that read it.
        """
        stored = mssql._unwrap_default("((12345678901234.))")
        assert stored == "12345678901234."
        assert mssql.validate_default(stored) == stored


class TestChunkedPayload:
    """A pty in canonical mode truncates input at 4095 bytes without
    saying so, and a form payload — a wide table's column list — passes
    that easily.  Emacs sends such a payload as :admin-payload chunks
    and then names them with @payload."""

    def _fresh(self):
        from datum import commands
        commands._payload_chunks.clear()
        return commands

    def test_chunks_are_joined_in_order(self, monkeypatch):
        commands = self._fresh()
        seen = {}
        monkeypatch.setattr(commands.admin, "run_action",
                            lambda p, a, d, args: seen.update(args=args))
        for chunk in ("abc", "def", "ghi"):
            commands.admin_payload([chunk])
        commands.admin_action(["schema", "alter-table", "@payload"])
        assert seen["args"] == ["abcdefghi"]

    def test_the_buffer_is_cleared_after_use(self, monkeypatch):
        commands = self._fresh()
        seen = []
        monkeypatch.setattr(commands.admin, "run_action",
                            lambda p, a, d, args: seen.append(args))
        commands.admin_payload(["first"])
        commands.admin_action(["schema", "alter-table", "@payload"])
        # A leftover payload must not attach itself to the next action.
        commands.admin_action(["schema", "alter-table", "@payload"])
        assert seen == [["first"], [""]]

    def test_a_failed_action_still_clears_the_buffer(self, monkeypatch):
        commands = self._fresh()
        monkeypatch.setattr(commands.envelope, "error", lambda *a, **k: None)
        commands.admin_payload(["orphaned"])
        commands.admin_action(["onlyonearg"])
        assert commands._payload_chunks == []

    def test_other_arguments_pass_through(self, monkeypatch):
        commands = self._fresh()
        seen = {}
        monkeypatch.setattr(commands.admin, "run_action",
                            lambda p, a, d, args: seen.update(args=args))
        commands.admin_action(["databases", "files", "appdb"])
        assert seen["args"] == ["appdb"]


class TestColumnTypeValidation:
    """The offered types are suggestions, not the only options: a menu
    can only name the sizes someone thought of, and a column often needs
    one it does not."""

    @pytest.mark.parametrize("sql_type", [
        "INT", "BIGSERIAL", "UUID",
        "NVARCHAR(120)", "VARCHAR(4000)", "CHAR(3)",      # unlisted sizes
        "NVARCHAR(MAX)", "VARBINARY(MAX)",
        "DECIMAL(9,3)", "NUMERIC(38,10)",                 # unlisted precision
        "DOUBLE PRECISION", "TIMESTAMP WITH TIME ZONE",   # multi-word
        "INT IDENTITY(1,1)", "BIGINT IDENTITY(100,5)",
        "numeric(9,3)",                                   # case is the
    ])                                                    #   server's affair
    def test_accepted(self, mssql, sql_type):
        assert mssql.validate_column_type("c", sql_type) == sql_type

    @pytest.mark.parametrize("sql_type", [
        "INT); DROP TABLE x --",
        "NVARCHAR(50) NOT NULL DEFAULT 1",
        "INT--", "INT;", "VARCHAR('a')", "VARCHAR(-1)",
        "INT(1,2,3)", "INT IDENTITY(1,1) EXEC",
        "", "   ", "A" * 150,
    ])
    def test_refused(self, mssql, sql_type):
        with pytest.raises(ValueError):
            mssql.validate_column_type("c", sql_type)

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_an_unlisted_size_reaches_the_ddl(self, driver_cls):
        d = driver_cls()
        size = "NVARCHAR(120)" if d.dialect_name == "mssql" else "VARCHAR(120)"
        assert size not in {t[0] for t in d.column_types()}
        sql = d.sql_create_table("s", {
            "name": "t", "columns": [["a", size, True, False, ""]]})[0]
        assert size in sql

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_the_type_field_completes_rather_than_choosing(self, driver_cls):
        d = driver_cls()
        columns = next(f for f in d.table_options(None, "s")
                       if f["key"] == "columns")
        spec = next(c for c in columns["item"] if c["key"] == "type")
        # A menu of two dozen types has to be scrolled and cannot offer
        # an unlisted size.
        assert spec["type"] == "completing"
        assert len(spec["completions"]) > 10

    @pytest.mark.parametrize("driver_cls", [MSSQLDriver, PostgreSQLDriver])
    def test_altering_to_an_unlisted_size(self, driver_cls):
        d = driver_cls()
        old = "NVARCHAR(50)" if d.dialect_name == "mssql" else "VARCHAR(50)"
        new = "NVARCHAR(400)" if d.dialect_name == "mssql" else "VARCHAR(400)"
        current = [["a", old, True, False, ""]]
        edited = [["a", new, True, False, "", "a"]]
        stmts = d.sql_alter_table("s", "t", {"columns": edited}, current)
        assert any(new in s for s in stmts)


class TestObjectPermissions:
    """GRANT, DENY and REVOKE across the scopes each dialect has."""

    @pytest.fixture
    def mssql(self):
        return MSSQLDriver.__new__(MSSQLDriver)

    @pytest.fixture
    def postgres(self):
        return PostgreSQLDriver.__new__(PostgreSQLDriver)

    @staticmethod
    def _inner(statement):
        """Return the statement MSSQL wraps to run inside a database."""
        marker = "sp_executesql N'"
        if marker in statement:
            body = statement.split(marker, 1)[1][:-1]
            return body.replace("''", "'")
        return statement

    def test_both_dialects_declare_support(self, mssql, postgres):
        assert mssql.supports_object_permissions
        assert postgres.supports_object_permissions

    def test_only_mssql_can_deny(self, mssql, postgres):
        # The SQL standard has no DENY, and neither does PostgreSQL.
        assert mssql.supports_deny
        assert not postgres.supports_deny

    def test_mssql_server_scope_stands_alone(self, mssql):
        # A login's rights inside a database belong to the user it maps
        # to, so the server level offers only itself.
        assert mssql.permission_scopes(None) == [["server", "Server"]]
        keys = [s[0] for s in mssql.permission_scopes("db")]
        assert keys == ["database", "schema", "object", "column"]

    def test_postgres_has_no_server_scope(self, postgres):
        # Roles are cluster-wide but privileges hang off objects, which
        # belong to one database.
        keys = [s[0] for s in postgres.permission_scopes(None)]
        assert "server" not in keys
        assert keys == ["database", "schema", "object", "column"]

    # --- statement shape ---

    def test_mssql_object_grant_uses_object_class(self, mssql):
        sql = mssql.sql_set_permissions(
            "bob", "object", "dbo.customers", ["SELECT"], [], {},
            database="db")
        assert [self._inner(x) for x in sql] == [
            "GRANT SELECT ON OBJECT::[dbo].[customers] TO [bob]"]

    def test_mssql_schema_grant_uses_schema_class(self, mssql):
        sql = mssql.sql_set_permissions(
            "bob", "schema", "dbo", ["EXECUTE"], [], {}, database="db")
        assert [self._inner(x) for x in sql] == [
            "GRANT EXECUTE ON SCHEMA::[dbo] TO [bob]"]

    def test_mssql_database_grant_has_no_on_clause(self, mssql):
        sql = mssql.sql_set_permissions(
            "bob", "database", "", ["CREATE TABLE"], [], {}, database="db")
        assert [self._inner(x) for x in sql] == [
            "GRANT CREATE TABLE TO [bob]"]

    def test_postgres_object_grant_uses_on_table(self, postgres):
        sql = postgres.sql_set_permissions(
            "bob", "object", "public.customers", ["SELECT"], [], {})
        assert sql == ['GRANT SELECT ON TABLE "public"."customers" TO "bob"']

    def test_revoke_takes_the_principal_from_not_to(self, mssql, postgres):
        # GRANT ... TO, but REVOKE ... FROM.
        assert mssql.sql_revoke_permission(
            "bob", "object", "dbo.t", "SELECT")[0].endswith("FROM [bob]")
        assert postgres.sql_revoke_permission(
            "bob", "object", "public.t", "SELECT")[0].endswith('FROM "bob"')

    def test_column_scope_names_the_column(self, mssql, postgres):
        assert [self._inner(x) for x in mssql.sql_set_permissions(
            "bob", "column", "dbo.customers", ["SELECT"], [], {},
            database="db", column="email")] == [
            "GRANT SELECT ([email]) ON OBJECT::[dbo].[customers] TO [bob]"]
        assert postgres.sql_set_permissions(
            "bob", "column", "public.customers", ["SELECT"], [], {},
            column="email") == [
            'GRANT SELECT ("email") ON TABLE "public"."customers" TO "bob"']

    # --- the diff ---

    def test_only_the_difference_is_applied(self, mssql):
        sql = mssql.sql_set_permissions(
            "bob", "object", "dbo.t", ["SELECT", "UPDATE"], [],
            {"SELECT": "GRANT", "DELETE": "GRANT"}, database="db")
        # SELECT is unchanged and must not be reapplied.
        assert not any("GRANT SELECT" in s for s in sql)
        assert any("REVOKE DELETE" in s for s in sql)
        assert any("GRANT UPDATE" in s for s in sql)

    def test_nothing_to_do_yields_no_statements(self, mssql, postgres):
        assert mssql.sql_set_permissions(
            "bob", "object", "dbo.t", ["SELECT"], [],
            {"SELECT": "GRANT"}, database="db") == []
        assert postgres.sql_set_permissions(
            "bob", "object", "public.t", ["SELECT"], [],
            {"SELECT": "GRANT"}) == []

    def test_flipping_grant_to_deny_revokes_first(self, mssql):
        # Without the revoke the two states sit side by side.
        sql = mssql.sql_set_permissions(
            "bob", "object", "dbo.t", [], ["SELECT"], {"SELECT": "GRANT"},
            database="db")
        assert [self._inner(x) for x in sql] == [
            "REVOKE SELECT ON OBJECT::[dbo].[t] FROM [bob]",
            "DENY SELECT ON OBJECT::[dbo].[t] TO [bob]"]

    def test_deny_wins_when_a_permission_is_in_both_lists(self, mssql):
        sql = mssql.sql_set_permissions(
            "bob", "object", "dbo.t", ["SELECT"], ["SELECT"], {},
            database="db")
        assert [self._inner(x) for x in sql] == [
            "DENY SELECT ON OBJECT::[dbo].[t] TO [bob]"]

    def test_postgres_refuses_to_pretend_it_can_deny(self, postgres):
        with pytest.raises(ValueError, match="no DENY"):
            postgres.sql_set_permissions(
                "bob", "object", "public.t", [], ["SELECT"], {})

    # --- validation ---

    def test_a_permission_must_belong_to_its_scope(self, mssql, postgres):
        # USAGE is a PostgreSQL schema privilege, not an MSSQL object one.
        with pytest.raises(ValueError, match="object level"):
            mssql.sql_set_permissions(
                "bob", "object", "dbo.t", ["USAGE"], [], {}, database="db")
        # TRUNCATE is a table privilege, not a schema one.
        with pytest.raises(ValueError, match="schema level"):
            postgres.sql_set_permissions(
                "bob", "schema", "public", ["TRUNCATE"], [], {})

    def test_an_unknown_scope_is_refused(self, mssql):
        with pytest.raises(ValueError, match="not a scope"):
            mssql.sql_set_permissions(
                "bob", "galaxy", "x", ["SELECT"], [], {})

    def test_a_permission_name_is_never_interpolated_raw(self, mssql):
        # The permission is checked against a list rather than quoted,
        # so anything not on it cannot reach the statement at all.
        with pytest.raises(ValueError):
            mssql.sql_set_permissions(
                "bob", "object", "dbo.t", ["SELECT; DROP TABLE x--"], [], {},
                database="db")

    def test_identifiers_are_escaped(self, mssql, postgres):
        sql = self._inner(mssql.sql_set_permissions(
            "bo]b", "object", "dbo.cus]tomers", ["SELECT"], [], {},
            database="db")[0])
        assert "[bo]]b]" in sql and "[cus]]tomers]" in sql
        sql = postgres.sql_set_permissions(
            'bo"b', "object", 'public.cus"t', ["SELECT"], [], {})[0]
        assert 'bo""b' in sql and 'cus""t' in sql

    def test_mssql_runs_database_scoped_statements_in_that_database(self, mssql):
        # A session cannot USE its way around, so each statement is
        # executed inside the database it is about.
        sql = mssql.sql_set_permissions(
            "bob", "object", "dbo.t", ["SELECT"], [], {}, database="payroll")
        assert sql[0].startswith("EXEC [payroll]..sp_executesql")

    def test_server_scope_is_not_wrapped(self, mssql):
        sql = mssql.sql_set_permissions(
            "bob", "server", "", ["VIEW SERVER STATE"], [], {})
        assert sql == ["GRANT VIEW SERVER STATE TO [bob]"]

    # --- the form the panel builds from the driver ---

    def test_the_form_opens_showing_what_is_already_held(self, mssql):
        fields = mssql.permission_options(
            "object", {"SELECT": "GRANT", "DELETE": "DENY"})
        by_key = {f["key"]: f for f in fields}
        assert by_key["granted"]["default"] == ["SELECT"]
        assert by_key["denied"]["default"] == ["DELETE"]

    def test_postgres_form_has_no_denied_list(self, postgres):
        keys = [f["key"] for f in postgres.permission_options("object", {})]
        assert keys == ["granted"]


class TestServerFilesystem:
    """Listing and reading the server's filesystem."""

    @pytest.fixture
    def mssql(self):
        return MSSQLDriver.__new__(MSSQLDriver)

    @pytest.fixture
    def postgres(self):
        return PostgreSQLDriver.__new__(PostgreSQLDriver)

    def test_both_dialects_can_list_and_read(self, mssql, postgres):
        assert mssql.supports_path_browse and mssql.supports_file_read
        assert postgres.supports_path_browse and postgres.supports_file_read

    # --- the boolean the ODBC drivers hand back ---

    def test_a_string_zero_is_false(self, mssql):
        # The PostgreSQL driver returns booleans as "1" and "0", and
        # bool("0") is True -- which reported every file as a directory.
        assert mssql.coerce_bool("0") is False
        assert mssql.coerce_bool("f") is False
        assert mssql.coerce_bool("false") is False
        assert mssql.coerce_bool("") is False

    def test_a_string_one_is_true(self, mssql):
        assert mssql.coerce_bool("1") is True
        assert mssql.coerce_bool("t") is True
        assert mssql.coerce_bool("true") is True

    def test_integers_and_bools_still_work(self, mssql):
        # MSSQL returns integers, which bool() always handled.
        assert mssql.coerce_bool(1) is True
        assert mssql.coerce_bool(0) is False
        assert mssql.coerce_bool(True) is True
        assert mssql.coerce_bool(None) is False

    # --- path arithmetic happens on the server's terms ---

    def test_parent_of_a_posix_root_stays_root(self, mssql):
        assert mssql.parent_path("/") is None
        assert mssql.parent_path("/var") == "/"
        assert mssql.parent_path("/var/opt/mssql") == "/var/opt"

    def test_a_windows_drive_root_keeps_its_separator(self, mssql):
        # "C:" is not a usable path; "C:\" is.
        assert mssql.parent_path("C:\\Data\\SQL") == "C:\\Data"
        assert mssql.parent_path("C:\\Data") == "C:\\"

    # --- Windows has no single root, so a drive root is a top ---

    def test_a_drive_root_is_recognised(self, mssql):
        for path in ("C:", "C:\\", "c:/", "Z:\\"):
            assert mssql.is_drive_root(path), path

    def test_a_unc_share_is_a_top_too(self, mssql):
        # \\server\share is as far up as a share goes; above it is the
        # host, which is not a directory anything can list.
        assert mssql.is_drive_root("\\\\fileserver\\backups")
        assert not mssql.is_drive_root("\\\\fileserver\\backups\\sql")

    def test_ordinary_paths_are_not_drive_roots(self, mssql):
        for path in ("/", "/var", "C:\\Data", "", None):
            assert not mssql.is_drive_root(path), path

    def test_a_drive_root_has_no_parent_directory(self, mssql):
        # The caller offers the drive list instead; that is not this
        # function's business.
        assert mssql.parent_path("C:\\") is None
        assert mssql.parent_path("C:") is None

    def test_walking_up_a_share_stops_at_the_share(self, mssql):
        # It used to walk on to "\\fileserver" and then to "\", neither
        # of which can be listed.
        assert mssql.parent_path("\\\\fs\\backups\\sql") == "\\\\fs\\backups"
        assert mssql.parent_path("\\\\fs\\backups") is None

    def test_only_mssql_claims_to_enumerate_drives(self, mssql, postgres):
        # There is no portable way to ask PostgreSQL.
        assert mssql.supports_drive_list
        assert not postgres.supports_drive_list

    def test_the_drives_path_cannot_collide_with_a_real_one(self, mssql):
        # A real path starts with a separator, a drive letter or a name
        # on both Windows and POSIX -- never a colon.
        assert mssql.DRIVES_PATH.startswith(":")
        assert not mssql.is_drive_root(mssql.DRIVES_PATH)

    def test_joining_follows_the_separator_already_in_the_path(self, mssql):
        assert mssql.join_path("C:\\Data", "x.mdf") == "C:\\Data\\x.mdf"
        assert mssql.join_path("/var/opt", "x.mdf") == "/var/opt/x.mdf"

    # --- reading ---

    def test_mssql_escapes_the_path_it_cannot_parameterise(self, mssql):
        # OPENROWSET takes the path as a literal, so a quote in a name
        # must not be able to close it.
        assert mssql.quote_ddl_literal("/tmp/it's.txt") == "'/tmp/it''s.txt'"

    def test_the_epoch_placeholder_is_not_shown_as_a_date(self):
        # The DMV reports 1601-01-01 for a time the filesystem does not
        # keep, which on Linux is every creation and access time.
        from datum.drivers.mssql import _clean_timestamp
        assert _clean_timestamp("1601-01-01 00:00:00") == ""
        assert _clean_timestamp(None) == ""
        assert _clean_timestamp("2026-09-25 12:39:48") == "2026-09-25 12:39:48"
