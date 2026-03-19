"""Abstract base class for SQL dialect drivers.

Each driver provides:
  - Introspection queries (databases, schemas, tables, columns, running queries,
    current user, server version)
  - Type mapping from Python/pyarrow types to SQL DDL types for :in imports

Concrete implementations live in mssql.py and postgres.py.
The fallback AnsiDriver uses INFORMATION_SCHEMA where possible, which is
supported by most modern databases, but some queries may not work on all
platforms.
"""

from abc import ABC, abstractmethod


class BaseDriver(ABC):
    """Abstract driver. All methods return strings (SQL) or lists/dicts."""

    # Human-readable name shown in the Emacs mode line.
    dialect_name = "unknown"

    # --- Introspection SQL ---
    # Each property returns a SQL string that can be executed directly.
    # Results are always expected as a single-column list unless noted.

    @property
    @abstractmethod
    def sql_list_databases(self):
        """SQL to list all databases on the server. Returns: [(name,)]"""

    @property
    @abstractmethod
    def sql_current_database(self):
        """SQL to get the current database name. Returns: [(name,)]"""

    @property
    @abstractmethod
    def sql_list_schemas(self):
        """SQL to list schemas in the current database. Returns: [(name,)]"""

    @property
    @abstractmethod
    def sql_current_schema(self):
        """SQL to get the current/default schema. Returns: [(name,)]"""

    @property
    @abstractmethod
    def sql_list_tables(self):
        """SQL to list tables in the current database.
        Returns: [(schema, table_name, table_type)]
        table_type is 'TABLE' or 'VIEW'
        """

    @property
    @abstractmethod
    def sql_current_user(self):
        """SQL to get the current user. Returns: [(name,)]"""

    @property
    @abstractmethod
    def sql_server_version(self):
        """SQL to get the server version string. Returns: [(version,)]"""

    @property
    @abstractmethod
    def sql_running_queries(self):
        """SQL to list currently running queries.
        Returns: [(session_id, user, status, duration, sql_text)]
        """

    def sql_list_columns(self, schema, table):
        """SQL to list columns for a given table.
        Returns: [(column_name, data_type, is_nullable, column_default)]
        Default implementation uses INFORMATION_SCHEMA, works on most platforms.
        """
        return f"""
            SELECT COLUMN_NAME,
                   DATA_TYPE,
                   IS_NULLABLE,
                   COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME   = '{table}'
            ORDER BY ORDINAL_POSITION
        """

    # --- Type mapping for :in imports ---

    def python_type_to_sql(self, python_type):
        """Map a Python type name to a SQL DDL type string.

        python_type is a string from pyarrow or Python's type system,
        e.g. 'int64', 'float64', 'string', 'bool', 'date32', 'timestamp[us]'.
        Returns a SQL type string, e.g. 'BIGINT', 'FLOAT', 'NVARCHAR(MAX)'.
        Subclasses should override this for dialect-specific type names.
        """
        return _ANSI_TYPE_MAP.get(python_type, "NVARCHAR(MAX)")


# ANSI SQL type mapping used as the default fallback.
_ANSI_TYPE_MAP = {
    "int8":           "SMALLINT",
    "int16":          "SMALLINT",
    "int32":          "INTEGER",
    "int64":          "BIGINT",
    "uint8":          "SMALLINT",
    "uint16":         "INTEGER",
    "uint32":         "BIGINT",
    "uint64":         "NUMERIC(20,0)",
    "float16":        "REAL",
    "float32":        "REAL",
    "float64":        "DOUBLE PRECISION",
    "bool":           "BOOLEAN",
    "string":         "NVARCHAR(MAX)",
    "large_string":   "NVARCHAR(MAX)",
    "date32":         "DATE",
    "date64":         "DATE",
    "timestamp[s]":   "TIMESTAMP",
    "timestamp[ms]":  "TIMESTAMP",
    "timestamp[us]":  "TIMESTAMP",
    "timestamp[ns]":  "TIMESTAMP",
    "time32[s]":      "TIME",
    "time32[ms]":     "TIME",
    "time64[us]":     "TIME",
    "time64[ns]":     "TIME",
    "binary":         "BLOB",
    "large_binary":   "BLOB",
    "decimal128":     "DECIMAL(38,10)",
}


class AnsiDriver(BaseDriver):
    """Fallback driver using ANSI SQL / INFORMATION_SCHEMA.

    Works on most modern databases for basic introspection but may fail
    on some platforms (e.g. sql_running_queries is not standardized).
    Emits a warning envelope when used so the user knows dialect is unknown.
    """

    dialect_name = "ansi"

    @property
    def sql_list_databases(self):
        # Not in ANSI SQL — this will fail on most platforms gracefully.
        return "SELECT CATALOG_NAME FROM INFORMATION_SCHEMA.SCHEMATA"

    @property
    def sql_current_database(self):
        return "SELECT CURRENT_CATALOG"

    @property
    def sql_list_schemas(self):
        return "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME"

    @property
    def sql_current_schema(self):
        return "SELECT CURRENT_SCHEMA"

    @property
    def sql_list_tables(self):
        return """
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """

    @property
    def sql_current_user(self):
        return "SELECT CURRENT_USER"

    @property
    def sql_server_version(self):
        # No ANSI standard for this — subclasses override.
        return "SELECT 'unknown'"

    @property
    def sql_running_queries(self):
        # No ANSI standard for this — subclasses override.
        return "SELECT 'not supported' AS note"
