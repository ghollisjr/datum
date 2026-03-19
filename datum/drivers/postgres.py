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
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
              AND schema_name NOT LIKE 'pg_toast%'
              AND schema_name NOT LIKE 'pg_temp%'
            ORDER BY schema_name
        """

    @property
    def sql_current_schema(self):
        return "SELECT current_schema()"

    @property
    def sql_list_tables(self):
        return """
            SELECT table_schema,
                   table_name,
                   table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name
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
