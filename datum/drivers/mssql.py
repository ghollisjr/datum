"""Microsoft SQL Server dialect driver."""

from .base import BaseDriver


class MSSQLDriver(BaseDriver):

    dialect_name = "mssql"

    @property
    def sql_list_databases(self):
        return "SELECT name FROM sys.databases ORDER BY name"

    @property
    def sql_current_database(self):
        return "SELECT DB_NAME()"

    @property
    def sql_list_schemas(self):
        return "SELECT name FROM sys.schemas ORDER BY name"

    @property
    def sql_current_schema(self):
        return "SELECT SCHEMA_NAME()"

    @property
    def sql_list_tables(self):
        return """
            SELECT s.name AS table_schema,
                   t.name AS table_name,
                   CASE t.type
                       WHEN 'U' THEN 'TABLE'
                       WHEN 'V' THEN 'VIEW'
                       ELSE t.type
                   END AS table_type
            FROM sys.objects t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.type IN ('U', 'V')
            ORDER BY s.name, t.name
        """

    @property
    def sql_current_user(self):
        return "SELECT SYSTEM_USER"

    @property
    def sql_server_version(self):
        return "SELECT @@VERSION"

    @property
    def sql_running_queries(self):
        return """
            SELECT r.session_id,
                   s.login_name                              AS [user],
                   r.status,
                   DATEDIFF(second, r.start_time, GETDATE()) AS duration_secs,
                   SUBSTRING(t.text, 1, 200)                AS sql_text
            FROM sys.dm_exec_requests r
            JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
            CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
            WHERE r.session_id <> @@SPID
            ORDER BY r.start_time
        """

    def sql_list_databases_like(self, pattern):
        return ("SELECT name FROM sys.databases WHERE name LIKE ? ORDER BY name",
                [pattern])

    def sql_list_schemas_like(self, pattern):
        return ("SELECT name FROM sys.schemas WHERE name LIKE ? ORDER BY name",
                [pattern])

    def sql_list_tables_like(self, pattern):
        return ("""
            SELECT s.name AS table_schema,
                   t.name AS table_name,
                   CASE t.type
                       WHEN 'U' THEN 'TABLE'
                       WHEN 'V' THEN 'VIEW'
                       ELSE t.type
                   END AS table_type
            FROM sys.objects t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.type IN ('U', 'V')
              AND t.name LIKE ?
            ORDER BY s.name, t.name
        """, [pattern])

    def sql_resolve_object_type(self, schema, name):
        return ("""
            SELECT CASE o.type
                       WHEN 'U'  THEN 'TABLE'
                       WHEN 'V'  THEN 'VIEW'
                       WHEN 'P'  THEN 'PROCEDURE'
                       WHEN 'FN' THEN 'FUNCTION'
                       WHEN 'IF' THEN 'FUNCTION'
                       WHEN 'TF' THEN 'FUNCTION'
                   END AS object_type
            FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE s.name = ?
              AND o.name = ?
              AND o.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF')
        """, [schema, name])

    def sql_get_definition(self, schema, name, object_type):
        if object_type == 'TABLE':
            return ("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                       CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION,
                       NUMERIC_SCALE, COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ?
                  AND TABLE_NAME   = ?
                ORDER BY ORDINAL_POSITION
            """, [schema, name])
        else:
            return ("""
                SELECT m.definition
                FROM sys.sql_modules m
                JOIN sys.objects o ON m.object_id = o.object_id
                JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE s.name = ?
                  AND o.name = ?
            """, [schema, name])

    def sql_check_database(self, name):
        return ("""
            SELECT name,
                   database_id,
                   create_date,
                   compatibility_level,
                   collation_name,
                   state_desc,
                   recovery_model_desc
            FROM sys.databases
            WHERE name = ?
        """, [name])

    def sql_check_schema(self, name):
        return ("""
            SELECT s.name AS schema_name,
                   s.schema_id,
                   dp.name AS owner
            FROM sys.schemas s
            LEFT JOIN sys.database_principals dp
                   ON s.principal_id = dp.principal_id
            WHERE s.name = ?
        """, [name])

    def python_type_to_sql(self, python_type):
        return _MSSQL_TYPE_MAP.get(python_type, "NVARCHAR(MAX)")


_MSSQL_TYPE_MAP = {
    "int8":           "SMALLINT",
    "int16":          "SMALLINT",
    "int32":          "INT",
    "int64":          "BIGINT",
    "uint8":          "TINYINT",
    "uint16":         "INT",
    "uint32":         "BIGINT",
    "uint64":         "NUMERIC(20,0)",
    "float16":        "REAL",
    "float32":        "REAL",
    "float64":        "FLOAT",
    "bool":           "BIT",
    "string":         "NVARCHAR(MAX)",
    "large_string":   "NVARCHAR(MAX)",
    "date32":         "DATE",
    "date64":         "DATE",
    "timestamp[s]":   "DATETIME2",
    "timestamp[ms]":  "DATETIME2",
    "timestamp[us]":  "DATETIME2",
    "timestamp[ns]":  "DATETIME2",
    "time32[s]":      "TIME",
    "time32[ms]":     "TIME",
    "time64[us]":     "TIME",
    "time64[ns]":     "TIME",
    "binary":         "VARBINARY(MAX)",
    "large_binary":   "VARBINARY(MAX)",
    "decimal128":     "DECIMAL(38,10)",
}
