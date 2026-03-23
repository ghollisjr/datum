"""MySQL / MariaDB dialect driver."""

from .base import BaseDriver


class MySQLDriver(BaseDriver):

    dialect_name = "mysql"

    # MySQL has no separate schema layer — database = schema.
    # default_schema = None means "add bare names for all tables"
    # since sql_list_tables only returns current-database objects.
    default_schema = None

    @property
    def sql_list_databases(self):
        return ("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA "
                "ORDER BY SCHEMA_NAME")

    @property
    def sql_current_database(self):
        return "SELECT DATABASE()"

    @property
    def sql_list_schemas(self):
        # MySQL: schema = database
        return ("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA "
                "ORDER BY SCHEMA_NAME")

    @property
    def sql_current_schema(self):
        return "SELECT DATABASE()"

    @property
    def sql_list_tables(self):
        return """
            SELECT TABLE_SCHEMA,
                   TABLE_NAME,
                   CASE TABLE_TYPE
                       WHEN 'BASE TABLE' THEN 'TABLE'
                       WHEN 'VIEW'       THEN 'VIEW'
                       ELSE TABLE_TYPE
                   END AS table_type
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
            ORDER BY TABLE_NAME
        """

    @property
    def sql_current_user(self):
        return "SELECT CURRENT_USER()"

    @property
    def sql_server_version(self):
        return "SELECT VERSION()"

    @property
    def sql_running_queries(self):
        return """
            SELECT ID          AS session_id,
                   USER        AS user,
                   STATE       AS state,
                   TIME        AS duration_secs,
                   LEFT(INFO, 200) AS sql_text
            FROM information_schema.PROCESSLIST
            WHERE ID <> CONNECTION_ID()
            ORDER BY TIME DESC
        """

    def sql_list_databases_like(self, pattern):
        return ("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA "
                "WHERE SCHEMA_NAME LIKE ? "
                "ORDER BY SCHEMA_NAME", [pattern])

    def sql_list_schemas_like(self, pattern):
        return ("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA "
                "WHERE SCHEMA_NAME LIKE ? "
                "ORDER BY SCHEMA_NAME", [pattern])

    def sql_list_tables_like(self, pattern):
        return ("""
            SELECT TABLE_SCHEMA,
                   TABLE_NAME,
                   CASE TABLE_TYPE
                       WHEN 'BASE TABLE' THEN 'TABLE'
                       WHEN 'VIEW'       THEN 'VIEW'
                       ELSE TABLE_TYPE
                   END AS table_type
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME LIKE ?
            ORDER BY TABLE_NAME
        """, [pattern])

    @property
    def sql_list_routines(self):
        return """
            SELECT ROUTINE_SCHEMA,
                   ROUTINE_NAME,
                   ROUTINE_TYPE
            FROM information_schema.ROUTINES
            WHERE ROUTINE_SCHEMA = DATABASE()
            ORDER BY ROUTINE_NAME
        """

    def sql_list_routines_like(self, pattern):
        return ("""
            SELECT ROUTINE_SCHEMA,
                   ROUTINE_NAME,
                   ROUTINE_TYPE
            FROM information_schema.ROUTINES
            WHERE ROUTINE_SCHEMA = DATABASE()
              AND ROUTINE_NAME LIKE ?
            ORDER BY ROUTINE_NAME
        """, [pattern])

    @property
    def sql_routine_signatures(self):
        return """
            SELECT R.ROUTINE_SCHEMA,
                   R.ROUTINE_NAME,
                   COALESCE(
                       GROUP_CONCAT(
                           CONCAT(P.PARAMETER_NAME, ' ', P.DATA_TYPE)
                           ORDER BY P.ORDINAL_POSITION
                           SEPARATOR ', '
                       ),
                       ''
                   ) AS signature
            FROM information_schema.ROUTINES R
            LEFT JOIN information_schema.PARAMETERS P
                   ON P.SPECIFIC_SCHEMA = R.ROUTINE_SCHEMA
                  AND P.SPECIFIC_NAME   = R.SPECIFIC_NAME
                  AND P.ORDINAL_POSITION > 0
            WHERE R.ROUTINE_SCHEMA = DATABASE()
            GROUP BY R.ROUTINE_SCHEMA, R.ROUTINE_NAME
            ORDER BY R.ROUTINE_NAME
        """

    def sql_list_columns(self, schema, table):
        if schema is None:
            return ("""
                SELECT COLUMN_NAME,
                       DATA_TYPE,
                       IS_NULLABLE,
                       COLUMN_DEFAULT
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME   = ?
                ORDER BY ORDINAL_POSITION
            """, [table])
        return ("""
            SELECT COLUMN_NAME,
                   DATA_TYPE,
                   IS_NULLABLE,
                   COLUMN_DEFAULT
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME   = ?
            ORDER BY ORDINAL_POSITION
        """, [schema, table])

    def sql_resolve_object_type(self, schema, name):
        schema_clause = "TABLE_SCHEMA = DATABASE()" if schema is None else "TABLE_SCHEMA = ?"
        params = [name] if schema is None else [schema, name]
        schema_clause2 = "ROUTINE_SCHEMA = DATABASE()" if schema is None else "ROUTINE_SCHEMA = ?"
        params2 = [name] if schema is None else [schema, name]
        return (f"""
            SELECT CASE TABLE_TYPE
                       WHEN 'BASE TABLE' THEN 'TABLE'
                       WHEN 'VIEW'       THEN 'VIEW'
                   END AS object_type
            FROM information_schema.TABLES
            WHERE {schema_clause}
              AND TABLE_NAME = ?
            UNION ALL
            SELECT ROUTINE_TYPE AS object_type
            FROM information_schema.ROUTINES
            WHERE {schema_clause2}
              AND ROUTINE_NAME = ?
            LIMIT 1
        """, params + params2)

    def sql_get_definition(self, schema, name, object_type):
        schema_clause = "TABLE_SCHEMA = DATABASE()" if schema is None else "TABLE_SCHEMA = ?"
        schema_params = [] if schema is None else [schema]
        if object_type == 'TABLE':
            return (f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                       CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION,
                       NUMERIC_SCALE, COLUMN_DEFAULT
                FROM information_schema.COLUMNS
                WHERE {schema_clause}
                  AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """, schema_params + [name])
        elif object_type == 'VIEW':
            view_schema = "TABLE_SCHEMA = DATABASE()" if schema is None else "TABLE_SCHEMA = ?"
            return (f"""
                SELECT VIEW_DEFINITION AS definition
                FROM information_schema.VIEWS
                WHERE {view_schema}
                  AND TABLE_NAME = ?
            """, schema_params + [name])
        else:
            # FUNCTION or PROCEDURE
            routine_schema = "ROUTINE_SCHEMA = DATABASE()" if schema is None else "ROUTINE_SCHEMA = ?"
            return (f"""
                SELECT ROUTINE_DEFINITION AS definition
                FROM information_schema.ROUTINES
                WHERE {routine_schema}
                  AND ROUTINE_NAME = ?
                LIMIT 1
            """, schema_params + [name])

    def sql_check_database(self, name):
        return ("""
            SELECT SCHEMA_NAME AS name,
                   DEFAULT_CHARACTER_SET_NAME AS charset,
                   DEFAULT_COLLATION_NAME AS collation
            FROM information_schema.SCHEMATA
            WHERE SCHEMA_NAME = ?
        """, [name])

    def sql_check_schema(self, name):
        # MySQL: schema = database
        return ("""
            SELECT SCHEMA_NAME AS schema_name,
                   DEFAULT_CHARACTER_SET_NAME AS charset,
                   DEFAULT_COLLATION_NAME AS collation
            FROM information_schema.SCHEMATA
            WHERE SCHEMA_NAME = ?
        """, [name])

    def quote_identifier(self, name):
        return f"`{name}`"

    def python_type_to_sql(self, python_type):
        return _MYSQL_TYPE_MAP.get(python_type, "TEXT")


_MYSQL_TYPE_MAP = {
    "int8":           "TINYINT",
    "int16":          "SMALLINT",
    "int32":          "INT",
    "int64":          "BIGINT",
    "uint8":          "TINYINT UNSIGNED",
    "uint16":         "SMALLINT UNSIGNED",
    "uint32":         "INT UNSIGNED",
    "uint64":         "BIGINT UNSIGNED",
    "float16":        "FLOAT",
    "float32":        "FLOAT",
    "float64":        "DOUBLE",
    "bool":           "TINYINT(1)",
    "string":         "LONGTEXT",
    "large_string":   "LONGTEXT",
    "date32":         "DATE",
    "date64":         "DATE",
    "timestamp[s]":   "DATETIME",
    "timestamp[ms]":  "DATETIME(3)",
    "timestamp[us]":  "DATETIME(6)",
    "timestamp[ns]":  "DATETIME(6)",
    "time32[s]":      "TIME",
    "time32[ms]":     "TIME(3)",
    "time64[us]":     "TIME(6)",
    "time64[ns]":     "TIME(6)",
    "binary":         "BLOB",
    "large_binary":   "LONGBLOB",
    "decimal128":     "DECIMAL(38,10)",
}
