"""SQLite dialect driver."""

from .base import BaseDriver


class SQLiteDriver(BaseDriver):

    dialect_name = "sqlite"

    # SQLite has no schema layer.
    default_schema = None

    @property
    def sql_list_databases(self):
        return "PRAGMA database_list"

    @property
    def sql_current_database(self):
        return "SELECT 'main'"

    @property
    def sql_list_schemas(self):
        return "SELECT 'main' AS schema_name"

    @property
    def sql_current_schema(self):
        return "SELECT 'main'"

    @property
    def sql_list_tables(self):
        return """
            SELECT '' AS table_schema,
                   name AS table_name,
                   CASE type
                       WHEN 'table' THEN 'TABLE'
                       WHEN 'view'  THEN 'VIEW'
                       ELSE UPPER(type)
                   END AS table_type
            FROM sqlite_master
            WHERE type IN ('table', 'view')
              AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """

    @property
    def sql_current_user(self):
        return "SELECT 'default'"

    @property
    def sql_server_version(self):
        return "SELECT sqlite_version()"

    @property
    def sql_running_queries(self):
        return "SELECT 'N/A' AS note"

    def sql_list_databases_like(self, pattern):
        return ("SELECT name FROM pragma_database_list WHERE name LIKE ? "
                "ORDER BY name", [pattern])

    def sql_list_schemas_like(self, pattern):
        return ("SELECT 'main' AS schema_name WHERE 'main' LIKE ?",
                [pattern])

    def sql_list_tables_like(self, pattern):
        return ("""
            SELECT '' AS table_schema,
                   name AS table_name,
                   CASE type
                       WHEN 'table' THEN 'TABLE'
                       WHEN 'view'  THEN 'VIEW'
                       ELSE UPPER(type)
                   END AS table_type
            FROM sqlite_master
            WHERE type IN ('table', 'view')
              AND name NOT LIKE 'sqlite_%'
              AND name LIKE ?
            ORDER BY name
        """, [pattern])

    def sql_list_columns(self, schema, table):
        return (f"""
            SELECT name AS column_name,
                   type AS data_type,
                   CASE WHEN "notnull" THEN 'NO' ELSE 'YES' END AS is_nullable,
                   dflt_value AS column_default
            FROM pragma_table_info('{table}')
            ORDER BY cid
        """, [])

    def sql_resolve_object_type(self, schema, name):
        return ("""
            SELECT CASE type
                       WHEN 'table' THEN 'TABLE'
                       WHEN 'view'  THEN 'VIEW'
                       ELSE UPPER(type)
                   END AS object_type
            FROM sqlite_master
            WHERE name = ?
            LIMIT 1
        """, [name])

    def sql_get_definition(self, schema, name, object_type):
        if object_type == 'TABLE':
            return (f"""
                SELECT name AS column_name,
                       type AS data_type,
                       CASE WHEN "notnull" THEN 'NO' ELSE 'YES' END AS is_nullable,
                       NULL AS char_max_length,
                       NULL AS numeric_precision,
                       NULL AS numeric_scale,
                       dflt_value AS column_default
                FROM pragma_table_info('{name}')
                ORDER BY cid
            """, [])
        elif object_type == 'VIEW':
            return ("""
                SELECT sql AS definition
                FROM sqlite_master
                WHERE type = 'view'
                  AND name = ?
            """, [name])
        else:
            raise NotImplementedError(
                f"SQLite does not support {object_type} definitions")

    def sql_check_database(self, name):
        return ("""
            SELECT name, file
            FROM pragma_database_list
            WHERE name = ?
        """, [name])

    def python_type_to_sql(self, python_type):
        return _SQLITE_TYPE_MAP.get(python_type, "TEXT")


_SQLITE_TYPE_MAP = {
    "int8":           "INTEGER",
    "int16":          "INTEGER",
    "int32":          "INTEGER",
    "int64":          "INTEGER",
    "uint8":          "INTEGER",
    "uint16":         "INTEGER",
    "uint32":         "INTEGER",
    "uint64":         "INTEGER",
    "float16":        "REAL",
    "float32":        "REAL",
    "float64":        "REAL",
    "bool":           "INTEGER",
    "string":         "TEXT",
    "large_string":   "TEXT",
    "date32":         "TEXT",
    "date64":         "TEXT",
    "timestamp[s]":   "TEXT",
    "timestamp[ms]":  "TEXT",
    "timestamp[us]":  "TEXT",
    "timestamp[ns]":  "TEXT",
    "time32[s]":      "TEXT",
    "time32[ms]":     "TEXT",
    "time64[us]":     "TEXT",
    "time64[ns]":     "TEXT",
    "binary":         "BLOB",
    "large_binary":   "BLOB",
    "decimal128":     "REAL",
}
