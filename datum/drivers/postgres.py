"""PostgreSQL dialect driver."""

from .base import BaseDriver


class PostgreSQLDriver(BaseDriver):

    dialect_name = "postgres"

    @property
    def sql_list_databases(self):
        return "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname"

    @property
    def sql_current_database(self):
        return "SELECT current_database()"

    @property
    def sql_list_schemas(self):
        return """
            SELECT nspname AS schema_name
            FROM pg_namespace
            WHERE nspname NOT IN ('pg_catalog', 'information_schema')
              AND nspname NOT LIKE 'pg_toast%'
              AND nspname NOT LIKE 'pg_temp%'
            ORDER BY nspname
        """

    @property
    def sql_current_schema(self):
        return "SELECT current_schema()"

    @property
    def sql_list_tables(self):
        return """
            SELECT n.nspname AS table_schema,
                   c.relname AS table_name,
                   CASE c.relkind
                       WHEN 'r' THEN 'TABLE'
                       WHEN 'v' THEN 'VIEW'
                       WHEN 'm' THEN 'VIEW'
                       WHEN 'p' THEN 'TABLE'
                   END AS table_type
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind IN ('r', 'v', 'm', 'p')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY n.nspname, c.relname
        """

    @property
    def sql_current_user(self):
        return "SELECT current_user"

    @property
    def sql_server_version(self):
        return "SELECT version()"

    @property
    def sql_running_queries(self):
        return """
            SELECT pid                                    AS session_id,
                   usename                               AS "user",
                   state,
                   EXTRACT(EPOCH FROM (now() - query_start))::INT AS duration_secs,
                   LEFT(query, 200)                      AS sql_text
            FROM pg_stat_activity
            WHERE state != 'idle'
              AND pid <> pg_backend_pid()
            ORDER BY query_start
        """

    def sql_list_databases_like(self, pattern):
        return ("SELECT datname FROM pg_database "
                "WHERE datistemplate = false AND datname ILIKE ? "
                "ORDER BY datname", [pattern])

    def sql_list_schemas_like(self, pattern):
        return ("""
            SELECT nspname AS schema_name
            FROM pg_namespace
            WHERE nspname NOT IN ('pg_catalog', 'information_schema')
              AND nspname NOT LIKE 'pg_toast%%'
              AND nspname NOT LIKE 'pg_temp%%'
              AND nspname ILIKE ?
            ORDER BY nspname
        """, [pattern])

    def sql_list_tables_like(self, pattern):
        return ("""
            SELECT n.nspname AS table_schema,
                   c.relname AS table_name,
                   CASE c.relkind
                       WHEN 'r' THEN 'TABLE'
                       WHEN 'v' THEN 'VIEW'
                       WHEN 'm' THEN 'VIEW'
                       WHEN 'p' THEN 'TABLE'
                   END AS table_type
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind IN ('r', 'v', 'm', 'p')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND c.relname ILIKE ?
            ORDER BY n.nspname, c.relname
        """, [pattern])

    @property
    def sql_list_routines(self):
        return """
            SELECT n.nspname AS routine_schema,
                   p.proname AS routine_name,
                   CASE p.prokind
                       WHEN 'p' THEN 'PROCEDURE'
                       ELSE 'FUNCTION'
                   END AS routine_type
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY n.nspname, p.proname
        """

    @property
    def sql_routine_signatures(self):
        return """
            SELECT n.nspname AS routine_schema,
                   p.proname AS routine_name,
                   pg_get_function_identity_arguments(p.oid) AS signature
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY n.nspname, p.proname
        """

    def sql_list_routines_like(self, pattern):
        return ("""
            SELECT n.nspname AS routine_schema,
                   p.proname AS routine_name,
                   CASE p.prokind
                       WHEN 'p' THEN 'PROCEDURE'
                       ELSE 'FUNCTION'
                   END AS routine_type
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND p.proname ILIKE ?
            ORDER BY n.nspname, p.proname
        """, [pattern])

    def sql_resolve_object_type(self, schema, name, database=None):
        return ("""
            SELECT CASE
                       WHEN c.relkind = 'r' THEN 'TABLE'
                       WHEN c.relkind = 'v' THEN 'VIEW'
                       WHEN c.relkind = 'm' THEN 'VIEW'
                   END AS object_type
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = ?
              AND c.relname = ?
              AND c.relkind IN ('r', 'v', 'm')
            UNION ALL
            SELECT CASE p.prokind
                       WHEN 'f' THEN 'FUNCTION'
                       WHEN 'p' THEN 'PROCEDURE'
                       WHEN 'a' THEN 'FUNCTION'
                       WHEN 'w' THEN 'FUNCTION'
                   END AS object_type
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname = ?
              AND p.proname = ?
            LIMIT 1
        """, [schema, name, schema, name])

    def sql_get_definition(self, schema, name, object_type, database=None):
        if object_type == 'TABLE':
            return ("""
                SELECT column_name, data_type, is_nullable,
                       character_maximum_length, numeric_precision,
                       numeric_scale, column_default
                FROM information_schema.columns
                WHERE table_schema = ?
                  AND table_name   = ?
                ORDER BY ordinal_position
            """, [schema, name])
        elif object_type == 'VIEW':
            return ("""
                SELECT pg_get_viewdef(c.oid, true) AS definition
                FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = ?
                  AND c.relname = ?
            """, [schema, name])
        else:
            # FUNCTION or PROCEDURE
            return ("""
                SELECT pg_get_functiondef(p.oid) AS definition
                FROM pg_proc p
                JOIN pg_namespace n ON p.pronamespace = n.oid
                WHERE n.nspname = ?
                  AND p.proname = ?
                LIMIT 1
            """, [schema, name])

    def sql_check_database(self, name):
        return ("""
            SELECT d.datname                          AS name,
                   r.rolname                          AS owner,
                   pg_encoding_to_char(d.encoding)    AS encoding,
                   d.datcollate                       AS collation,
                   d.datctype                         AS ctype,
                   t.spcname                          AS tablespace,
                   pg_database_size(d.datname)        AS size_bytes,
                   pg_size_pretty(pg_database_size(d.datname)) AS size,
                   d.datconnlimit                     AS connection_limit,
                   d.datallowconn                     AS allow_connections,
                   d.datistemplate                    AS is_template,
                   age(d.datfrozenxid)                AS xid_age
            FROM pg_database d
            LEFT JOIN pg_roles r      ON d.datdba = r.oid
            LEFT JOIN pg_tablespace t ON d.dattablespace = t.oid
            WHERE d.datname = ?
        """, [name])

    def sql_check_schema(self, name):
        return ("""
            SELECT n.nspname AS schema_name,
                   r.rolname AS owner,
                   obj_description(n.oid) AS comment
            FROM pg_namespace n
            LEFT JOIN pg_roles r ON n.nspowner = r.oid
            WHERE n.nspname = ?
        """, [name])

    def python_type_to_sql(self, python_type):
        return _POSTGRES_TYPE_MAP.get(python_type, "TEXT")


_POSTGRES_TYPE_MAP = {
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
    "string":         "TEXT",
    "large_string":   "TEXT",
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
    "binary":         "BYTEA",
    "large_binary":   "BYTEA",
    "decimal128":     "NUMERIC(38,10)",
}
