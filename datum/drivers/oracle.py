"""Oracle Database dialect driver."""

from .base import BaseDriver


# System schemas to exclude from user-facing listings.
_SYSTEM_SCHEMAS = (
    "'SYS','SYSTEM','DBSNMP','APPQOSSYS','DBSFWUSER',"
    "'REMOTE_SCHEDULER_AGENT','SYSBACKUP','SYSDG','SYSKM','SYSRAC',"
    "'MDSYS','OLAPSYS','ORDDATA','CTXSYS','DVSYS','LBACSYS',"
    "'WMSYS','XDB','OUTLN','GSMADMIN_INTERNAL','ORACLE_OCM','OJVMSYS'"
)


class OracleDriver(BaseDriver):

    dialect_name = "oracle"
    default_schema = None

    @property
    def sql_list_databases(self):
        return "SELECT SYS_CONTEXT('USERENV', 'DB_NAME') AS name FROM DUAL"

    @property
    def sql_current_database(self):
        return "SELECT SYS_CONTEXT('USERENV', 'DB_NAME') FROM DUAL"

    @property
    def sql_list_schemas(self):
        return "SELECT username FROM ALL_USERS ORDER BY username"

    @property
    def sql_current_schema(self):
        return "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL"

    @property
    def sql_list_tables(self):
        return f"""
            SELECT owner AS table_schema,
                   table_name,
                   'TABLE' AS table_type
            FROM ALL_TABLES
            WHERE owner NOT IN ({_SYSTEM_SCHEMAS})
            UNION ALL
            SELECT owner AS table_schema,
                   view_name AS table_name,
                   'VIEW' AS table_type
            FROM ALL_VIEWS
            WHERE owner NOT IN ({_SYSTEM_SCHEMAS})
            ORDER BY 1, 2
        """

    @property
    def sql_current_user(self):
        return "SELECT USER FROM DUAL"

    @property
    def sql_server_version(self):
        return "SELECT version_full AS banner FROM PRODUCT_COMPONENT_VERSION WHERE ROWNUM = 1"

    @property
    def sql_running_queries(self):
        return """
            SELECT s.sid                          AS session_id,
                   s.username                     AS "user",
                   s.status,
                   s.last_call_et                 AS duration_secs,
                   SUBSTR(q.sql_text, 1, 200)     AS sql_text
            FROM GV$SESSION s
            JOIN GV$SQL q ON s.sql_id = q.sql_id AND s.inst_id = q.inst_id
            WHERE s.username IS NOT NULL
              AND s.sid != SYS_CONTEXT('USERENV', 'SID')
        """

    def sql_list_databases_like(self, pattern):
        return ("SELECT name FROM (SELECT SYS_CONTEXT('USERENV', 'DB_NAME') AS name FROM DUAL) "
                "WHERE name LIKE ? ORDER BY name", [pattern])

    def sql_list_schemas_like(self, pattern):
        return ("SELECT username FROM ALL_USERS WHERE username LIKE ? "
                "ORDER BY username", [pattern])

    def sql_list_tables_like(self, pattern):
        return (f"""
            SELECT owner AS table_schema,
                   table_name,
                   'TABLE' AS table_type
            FROM ALL_TABLES
            WHERE owner NOT IN ({_SYSTEM_SCHEMAS})
              AND table_name LIKE ?
            UNION ALL
            SELECT owner AS table_schema,
                   view_name AS table_name,
                   'VIEW' AS table_type
            FROM ALL_VIEWS
            WHERE owner NOT IN ({_SYSTEM_SCHEMAS})
              AND view_name LIKE ?
            ORDER BY 1, 2
        """, [pattern, pattern])

    @property
    def sql_list_routines(self):
        return f"""
            SELECT owner        AS routine_schema,
                   object_name  AS routine_name,
                   object_type  AS routine_type
            FROM ALL_OBJECTS
            WHERE object_type IN ('FUNCTION', 'PROCEDURE', 'PACKAGE')
              AND owner NOT IN ({_SYSTEM_SCHEMAS})
            ORDER BY owner, object_name
        """

    @property
    def sql_routine_signatures(self):
        return f"""
            SELECT owner        AS routine_schema,
                   object_name  AS routine_name,
                   LISTAGG(argument_name || ' ' || data_type, ', ')
                       WITHIN GROUP (ORDER BY position) AS signature
            FROM ALL_ARGUMENTS
            WHERE package_name IS NULL
              AND owner NOT IN ({_SYSTEM_SCHEMAS})
              AND position > 0
            GROUP BY owner, object_name
            ORDER BY owner, object_name
        """

    def sql_list_routines_like(self, pattern):
        return (f"""
            SELECT owner        AS routine_schema,
                   object_name  AS routine_name,
                   object_type  AS routine_type
            FROM ALL_OBJECTS
            WHERE object_type IN ('FUNCTION', 'PROCEDURE', 'PACKAGE')
              AND owner NOT IN ({_SYSTEM_SCHEMAS})
              AND object_name LIKE ?
            ORDER BY owner, object_name
        """, [pattern])

    def _owner(self, schema):
        """Return the uppercase owner, defaulting to USER if schema is None."""
        if schema:
            return schema.upper()
        return None

    def sql_list_columns(self, schema, table, database=None):
        owner = self._owner(schema)
        if owner:
            return ("""
                SELECT column_name,
                       data_type,
                       nullable AS is_nullable,
                       data_default AS column_default
                FROM ALL_TAB_COLUMNS
                WHERE owner = ?
                  AND table_name = ?
                ORDER BY column_id
            """, [owner, table.upper()])
        return ("""
            SELECT column_name,
                   data_type,
                   nullable AS is_nullable,
                   data_default AS column_default
            FROM ALL_TAB_COLUMNS
            WHERE owner = USER
              AND table_name = ?
            ORDER BY column_id
        """, [table.upper()])

    def sql_resolve_object_type(self, schema, name, database=None):
        owner = self._owner(schema)
        if owner:
            return ("""
                SELECT object_type
                FROM ALL_OBJECTS
                WHERE owner = ?
                  AND object_name = ?
                  AND object_type IN ('TABLE', 'VIEW', 'FUNCTION', 'PROCEDURE', 'PACKAGE')
                  AND ROWNUM = 1
            """, [owner, name.upper()])
        else:
            return ("""
                SELECT object_type
                FROM ALL_OBJECTS
                WHERE owner = USER
                  AND object_name = ?
                  AND object_type IN ('TABLE', 'VIEW', 'FUNCTION', 'PROCEDURE', 'PACKAGE')
                  AND ROWNUM = 1
            """, [name.upper()])

    def sql_get_definition(self, schema, name, object_type, database=None):
        owner = self._owner(schema)
        if object_type == 'TABLE':
            if owner:
                return ("""
                    SELECT column_name, data_type, nullable,
                           char_length, data_precision,
                           data_scale, data_default
                    FROM ALL_TAB_COLUMNS
                    WHERE owner = ?
                      AND table_name = ?
                    ORDER BY column_id
                """, [owner, name.upper()])
            else:
                return ("""
                    SELECT column_name, data_type, nullable,
                           char_length, data_precision,
                           data_scale, data_default
                    FROM ALL_TAB_COLUMNS
                    WHERE owner = USER
                      AND table_name = ?
                    ORDER BY column_id
                """, [name.upper()])
        else:
            # VIEW, FUNCTION, PROCEDURE, PACKAGE
            if owner:
                return ("""
                    SELECT text AS definition
                    FROM ALL_SOURCE
                    WHERE owner = ?
                      AND name = ?
                    ORDER BY line
                """, [owner, name.upper()])
            else:
                return ("""
                    SELECT text AS definition
                    FROM ALL_SOURCE
                    WHERE owner = USER
                      AND name = ?
                    ORDER BY line
                """, [name.upper()])

    def sql_check_database(self, name):
        return ("""
            SELECT d.NAME           AS name,
                   d.DBID           AS dbid,
                   d.CREATED        AS created,
                   d.LOG_MODE       AS log_mode,
                   d.OPEN_MODE      AS open_mode,
                   d.PLATFORM_NAME  AS platform,
                   p.VALUE          AS charset
            FROM V$DATABASE d,
                 NLS_DATABASE_PARAMETERS p
            WHERE p.PARAMETER = 'NLS_CHARACTERSET'
              AND d.NAME = ?
        """, [name.upper()])

    def sql_check_schema(self, name):
        return ("""
            SELECT username, created, account_status
            FROM ALL_USERS
            WHERE username = ?
        """, [name.upper()])

    def python_type_to_sql(self, python_type):
        return _ORACLE_TYPE_MAP.get(python_type, "VARCHAR2(4000)")


_ORACLE_TYPE_MAP = {
    "int8":           "NUMBER(4)",
    "int16":          "NUMBER(5)",
    "int32":          "NUMBER(10)",
    "int64":          "NUMBER(19)",
    "uint8":          "NUMBER(3)",
    "uint16":         "NUMBER(5)",
    "uint32":         "NUMBER(10)",
    "uint64":         "NUMBER(20)",
    "float16":        "BINARY_FLOAT",
    "float32":        "BINARY_FLOAT",
    "float64":        "BINARY_DOUBLE",
    "bool":           "NUMBER(1)",
    "string":         "VARCHAR2(4000)",
    "large_string":   "CLOB",
    "date32":         "DATE",
    "date64":         "DATE",
    "timestamp[s]":   "TIMESTAMP",
    "timestamp[ms]":  "TIMESTAMP",
    "timestamp[us]":  "TIMESTAMP",
    "timestamp[ns]":  "TIMESTAMP",
    "time32[s]":      "VARCHAR2(15)",
    "time32[ms]":     "VARCHAR2(15)",
    "time64[us]":     "VARCHAR2(15)",
    "time64[ns]":     "VARCHAR2(15)",
    "binary":         "RAW(2000)",
    "large_binary":   "BLOB",
    "decimal128":     "NUMBER(38,10)",
}
