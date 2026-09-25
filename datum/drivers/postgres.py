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

    # --- Database DDL ---

    supports_database_ddl = True

    def safe_fallback_database(self):
        return "postgres"

    # --- Object-level permissions ---

    supports_object_permissions = True
    # PostgreSQL has no DENY: a privilege is granted or it is revoked.
    supports_deny = False

    # Roles are cluster-wide but privileges are held on objects, and an
    # object belongs to one database, so this is always the connected one.
    permission_root_label = "in the current database"

    _DATABASE_PRIVILEGES = ["CONNECT", "CREATE", "TEMPORARY"]
    _SCHEMA_PRIVILEGES = ["USAGE", "CREATE"]
    _TABLE_PRIVILEGES = ["SELECT", "INSERT", "UPDATE", "DELETE",
                         "TRUNCATE", "REFERENCES", "TRIGGER"]
    _COLUMN_PRIVILEGES = ["SELECT", "INSERT", "UPDATE", "REFERENCES"]

    def permission_scopes(self, database=None):
        return [["database", "Database"], ["schema", "Schema"],
                ["object", "Table or view"],
                ["column", "Column of a table or view"]]

    def permission_choices(self, scope):
        return {
            "database": self._DATABASE_PRIVILEGES,
            "schema": self._SCHEMA_PRIVILEGES,
            "object": self._TABLE_PRIVILEGES,
            "column": self._COLUMN_PRIVILEGES,
        }.get(scope, [])

    def securable_choices(self, cursor, scope, database=None):
        if scope == "database":
            return []
        if scope == "schema":
            sql = ("SELECT nspname FROM pg_namespace "
                   "WHERE nspname NOT LIKE 'pg\\_%' "
                   "AND nspname <> 'information_schema' ORDER BY nspname")
        else:
            sql = ("SELECT n.nspname || '.' || c.relname "
                   "FROM pg_class c JOIN pg_namespace n "
                   "  ON n.oid = c.relnamespace "
                   "WHERE c.relkind IN ('r','v','m','p') "
                   "AND n.nspname NOT LIKE 'pg\\_%' "
                   "AND n.nspname <> 'information_schema' "
                   "ORDER BY 1")
        try:
            cursor.execute(sql)
            return [r[0] for r in cursor.fetchall()]
        except Exception:
            return []

    # aclexplode turns an ACL array into one row per grant, which is the
    # only way to see the grants actually recorded.  The
    # information_schema views expand a table grant across every column,
    # which would bury the explicit column grants among hundreds of
    # rows that were never granted separately.
    _PERMISSION_SQL = """
        SELECT 'GRANT', a.privilege_type, 'Database', d.datname, ''
          FROM pg_database d, aclexplode(d.datacl) a
         WHERE a.grantee = %(role)s AND d.datname = current_database()
        UNION ALL
        SELECT 'GRANT', a.privilege_type, 'Schema', n.nspname, ''
          FROM pg_namespace n, aclexplode(n.nspacl) a
         WHERE a.grantee = %(role)s
        UNION ALL
        SELECT 'GRANT', a.privilege_type, 'Object',
               n.nspname || '.' || c.relname, ''
          FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace,
               aclexplode(c.relacl) a
         WHERE a.grantee = %(role)s
        UNION ALL
        SELECT 'GRANT', a.privilege_type, 'Object',
               n.nspname || '.' || c.relname, att.attname
          FROM pg_attribute att
          JOIN pg_class c ON c.oid = att.attrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace,
               aclexplode(att.attacl) a
         WHERE a.grantee = %(role)s
         ORDER BY 3, 4, 5, 2
    """

    def list_permissions(self, cursor, principal, database=None):
        self.validate_identifier(principal)
        headers = ["State", "Permission", "Scope", "Securable", "Column"]
        # The role has to exist before ::regrole will resolve it.
        cursor.execute("SELECT oid FROM pg_roles WHERE rolname = ?",
                       (principal,))
        row = cursor.fetchone()
        if not row:
            return headers, []
        oid = row[0]
        sql = self._PERMISSION_SQL.replace("%(role)s", "?")
        cursor.execute(sql, (oid,) * sql.count("?"))
        return headers, [[str(c) if c is not None else "" for c in r]
                         for r in cursor.fetchall()]

    def _securable_clause(self, scope, securable, column=None):
        if scope == "database":
            return f" ON DATABASE {self.quote_ddl_identifier(securable)}"
        if scope == "schema":
            return f" ON SCHEMA {self.quote_ddl_identifier(securable)}"
        parts = securable.split(".", 1)
        if len(parts) == 2:
            obj = (f"{self.quote_ddl_identifier(parts[0])}."
                   f"{self.quote_ddl_identifier(parts[1])}")
        else:
            obj = self.quote_ddl_identifier(securable)
        return f" ON TABLE {obj}"

    def _permission_statement(self, verb, principal, scope, securable,
                              permission, column=None):
        self.validate_identifier(principal)
        if permission not in self.permission_choices(scope):
            raise ValueError(f"{permission} is not a privilege that can be "
                             f"granted at the {scope} level")
        target = self.quote_ddl_identifier(principal)
        cols = ""
        if scope == "column":
            self.validate_identifier(column)
            cols = f" ({self.quote_ddl_identifier(column)})"
        preposition = "FROM" if verb == "REVOKE" else "TO"
        return (f"{verb} {permission}{cols}"
                f"{self._securable_clause(scope, securable, column)} "
                f"{preposition} {target}")

    def sql_set_permissions(self, principal, scope, securable, granted,
                            denied, current, database=None, column=None):
        if denied:
            raise ValueError("PostgreSQL has no DENY; revoke instead")
        if scope not in (s[0] for s in self.permission_scopes(database)):
            raise ValueError(f"{scope} is not a scope in this context")
        wanted = set(granted or [])
        held = set(current or {})
        stmts = []
        for name in sorted(held - wanted):
            stmts.append(self._permission_statement(
                "REVOKE", principal, scope, securable, name, column))
        for name in sorted(wanted - held):
            stmts.append(self._permission_statement(
                "GRANT", principal, scope, securable, name, column))
        return stmts

    def sql_revoke_permission(self, principal, scope, securable, permission,
                              database=None, column=None):
        return [self._permission_statement(
            "REVOKE", principal, scope, securable, permission, column)]

    # --- Schemas and tables ---

    supports_schema_ddl = True

    def column_types(self):
        return [[t, t] for t in (
            "INTEGER", "SERIAL", "BIGINT", "BIGSERIAL", "SMALLINT",
            "BOOLEAN", "NUMERIC(18,2)", "REAL", "DOUBLE PRECISION",
            "DATE", "TIMESTAMP", "TIMESTAMPTZ", "TIME", "INTERVAL",
            "TEXT", "VARCHAR(50)", "VARCHAR(255)", "CHAR(1)",
            "UUID", "JSONB", "BYTEA", "INET")]

    def list_schemas_detail(self, cursor):
        cursor.execute(r"""
            SELECT n.nspname,
                   pg_get_userbyid(n.nspowner),
                   (SELECT count(*)::text FROM pg_class c
                     WHERE c.relnamespace = n.oid
                       AND c.relkind IN ('r', 'p'))
            FROM pg_namespace n
            WHERE n.nspname NOT LIKE 'pg\_%'
              AND n.nspname <> 'information_schema'
            ORDER BY n.nspname
        """)
        return (["Schema", "Owner", "Tables"],
                [[str(v) if v is not None else "" for v in row]
                 for row in cursor.fetchall()])

    def schema_options(self, cursor, current=None):
        roles = self._lookup(cursor, r"""
            SELECT rolname FROM pg_roles
            WHERE rolname NOT LIKE 'pg\_%' ORDER BY rolname
        """)
        return [
            {"key": "name", "label": "Schema Name", "type": "string",
             "default": "", "required": True},
            {"key": "owner", "label": "Owner",
             "type": "choice" if roles else "string",
             "default": "",
             "choices": ([["", "(current role)"]] + [[r, r] for r in roles]
                         if roles else None)},
        ]

    def sql_create_schema(self, opts):
        name = self.quote_ddl_identifier(opts.get("name", ""))
        owner = (opts.get("owner") or "").strip()
        sql = f"CREATE SCHEMA {name}"
        if owner:
            sql += f" AUTHORIZATION {self.quote_ddl_identifier(owner)}"
        return [sql]

    def sql_drop_schema(self, name, cascade=False):
        sql = f"DROP SCHEMA {self.quote_ddl_identifier(name)}"
        # CASCADE also drops everything the schema contains, so it is
        # only ever used when asked for explicitly.
        return [sql + " CASCADE" if cascade else sql]

    def list_tables_detail(self, cursor, schema):
        cursor.execute("""
            SELECT c.relname,
                   CASE WHEN c.reltuples < 0 THEN 'unknown'
                        ELSE c.reltuples::bigint::text END,
                   pg_size_pretty(pg_total_relation_size(c.oid)),
                   pg_get_userbyid(c.relowner)
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ? AND c.relkind IN ('r', 'p')
            ORDER BY c.relname
        """, [schema])
        return (["Table", "Rows (est)", "Size", "Owner"],
                [[str(v) if v is not None else "" for v in row]
                 for row in cursor.fetchall()])

    def table_options(self, cursor, schema):
        return [
            {"key": "name", "label": "Table Name", "type": "string",
             "default": "", "required": True},
            {"key": "columns", "label": "Columns", "type": "list",
             "default": [["id", "BIGSERIAL", False, True, ""]],
             "item": [
                 {"key": "name", "label": "Name", "type": "string",
                  "size": 18},
                 # Completing rather than a menu: a menu of two dozen
                 # types has to be scrolled, and it can only offer the
                 # sizes it happens to list.  Typing narrows, and a size
                 # it does not name is still accepted.
                 {"key": "type", "label": "Type", "type": "completing",
                  "size": 22,
                  "completions": [t[0] for t in self.column_types()]},
                 {"key": "nullable", "label": "Null", "type": "bool"},
                 {"key": "primary_key", "label": "PK", "type": "bool"},
                 {"key": "default", "label": "Default", "type": "string",
                  "size": 12},
             ]},
        ]

    def sql_create_table(self, schema, opts):
        name = opts.get("name", "")
        qualified = (f"{self.quote_ddl_identifier(schema)}."
                     f"{self.quote_ddl_identifier(name)}")
        clauses, primary = self._column_clauses(opts)
        if primary:
            keys = ", ".join(self.quote_ddl_identifier(k) for k in primary)
            clauses.append(
                f"CONSTRAINT {self.quote_ddl_identifier('pk_' + name)} "
                f"PRIMARY KEY ({keys})")
        return [f"CREATE TABLE {qualified} (\n  "
                + ",\n  ".join(clauses) + "\n)"]

    def sql_drop_table(self, schema, name):
        return [f"DROP TABLE {self.quote_ddl_identifier(schema)}."
                f"{self.quote_ddl_identifier(name)}"]

    def list_table_columns(self, cursor, schema, table):
        cursor.execute("""
            SELECT c.column_name, c.data_type, c.character_maximum_length,
                   c.numeric_precision, c.numeric_scale, c.is_nullable,
                   COALESCE(c.column_default, ''),
                   CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                  ON ku.constraint_name = tc.constraint_name
                 AND ku.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema = ? AND tc.table_name = ?
            ) pk ON pk.column_name = c.column_name
            WHERE c.table_schema = ? AND c.table_name = ?
            ORDER BY c.ordinal_position
        """, [schema, table, schema, table])
        rows = []
        for (name, base, length, precision, scale, nullable, default,
             is_key) in cursor.fetchall():
            rows.append([name,
                         self._format_type(base, length, precision, scale,
                                           default),
                         nullable == "YES", bool(is_key),
                         self._clean_default(default)])
        return rows

    _TYPE_NAMES = {
        "integer": "INTEGER", "bigint": "BIGINT", "smallint": "SMALLINT",
        "boolean": "BOOLEAN", "real": "REAL",
        "double precision": "DOUBLE PRECISION", "text": "TEXT",
        "date": "DATE", "time without time zone": "TIME",
        "timestamp without time zone": "TIMESTAMP",
        "timestamp with time zone": "TIMESTAMPTZ",
        "interval": "INTERVAL", "uuid": "UUID", "jsonb": "JSONB",
        "bytea": "BYTEA", "inet": "INET",
    }

    def _format_type(self, base, length, precision, scale, default=""):
        """Render a column's type in the vocabulary `column_types` uses."""
        # A serial is an integer with a nextval default; the builder
        # offers it as SERIAL, so it has to read back that way.
        if "nextval(" in (default or ""):
            return {"integer": "SERIAL", "bigint": "BIGSERIAL"}.get(
                base, base.upper())
        if base == "character varying":
            return f"VARCHAR({length})" if length else "VARCHAR(255)"
        if base == "character":
            return f"CHAR({length or 1})"
        if base == "numeric":
            return (f"NUMERIC({precision},{scale})" if precision is not None
                    else "NUMERIC(18,2)")
        return self._TYPE_NAMES.get(base, base.upper())

    @staticmethod
    def _clean_default(default):
        """Strip the type annotation PostgreSQL records on a default.

        A default stored as 'n/a'::text or 0 has to come back as the
        expression the builder would have produced, or every column
        looks changed.
        """
        text = (default or "").strip()
        if not text or "nextval(" in text:
            # A serial's default belongs to the type, not the column.
            return ""
        if "::" in text:
            text = text.split("::", 1)[0].strip()
        return text

    def sql_alter_table(self, schema, table, opts, current):
        qualified = (f"{self.quote_ddl_identifier(schema)}."
                     f"{self.quote_ddl_identifier(table)}")
        added, dropped, changed, renamed = self._diff_columns(
            opts, current)
        stmts = []

        # Renames go first so everything after them addresses the
        # column by the name it now has.
        for origin, row in renamed:
            stmts.extend(self.sql_rename_column(schema, table, origin,
                                                str(row[0]).strip()))

        for row in added:
            name = self.validate_identifier(str(row[0]).strip())
            sql_type = self.validate_column_type(name, row[1])
            clause = (f"ADD COLUMN {self.quote_ddl_identifier(name)} "
                      f"{sql_type}{'' if bool(row[2]) else ' NOT NULL'}")
            default = str(row[4] or "").strip() if len(row) > 4 else ""
            if default:
                clause += f" DEFAULT {self.validate_default(default)}"
            stmts.append(f"ALTER TABLE {qualified} {clause}")

        for before, row in changed:
            name = self.validate_identifier(str(row[0]).strip())
            column = self.quote_ddl_identifier(name)
            # Type, nullability and default are three separate statements
            # here, unlike MSSQL where ALTER COLUMN carries all of them.
            if str(before[1]) != str(row[1]):
                sql_type = self.validate_column_type(name, row[1])
                stmts.append(f"ALTER TABLE {qualified} ALTER COLUMN "
                             f"{column} TYPE {sql_type}")
            if bool(before[2]) != bool(row[2]):
                stmts.append(
                    f"ALTER TABLE {qualified} ALTER COLUMN {column} "
                    f"{'DROP' if bool(row[2]) else 'SET'} NOT NULL")
            old_default = str(before[4] or "").strip()
            new_default = str(row[4] or "").strip() if len(row) > 4 else ""
            if old_default != new_default:
                stmts.append(
                    f"ALTER TABLE {qualified} ALTER COLUMN {column} "
                    + (f"SET DEFAULT {self.validate_default(new_default)}"
                       if new_default else "DROP DEFAULT"))

        for row in dropped:
            name = self.validate_identifier(str(row[0]).strip())
            stmts.append(f"ALTER TABLE {qualified} DROP COLUMN "
                         f"{self.quote_ddl_identifier(name)}")
        return stmts

    def sql_rename_column(self, schema, table, old, new):
        return [f"ALTER TABLE {self.quote_ddl_identifier(schema)}."
                f"{self.quote_ddl_identifier(table)} RENAME COLUMN "
                f"{self.quote_ddl_identifier(old)} TO "
                f"{self.quote_ddl_identifier(new)}"]

    # --- Roles ---
    #
    # PostgreSQL has no separate login and user: a role that may log in
    # serves as both, so there is nothing to map per database.

    supports_security = True
    principal_noun = "role"

    def list_principals(self, cursor):
        cursor.execute(r"""
            SELECT r.rolname,
                   CASE WHEN r.rolcanlogin THEN 'Login' ELSE 'Group' END,
                   CASE WHEN r.rolsuper THEN 'yes' ELSE '' END,
                   CASE WHEN r.rolcreatedb THEN 'yes' ELSE '' END,
                   CASE WHEN r.rolcreaterole THEN 'yes' ELSE '' END,
                   CASE WHEN r.rolconnlimit = -1 THEN 'unlimited'
                        ELSE r.rolconnlimit::text END,
                   COALESCE(to_char(r.rolvaliduntil, 'YYYY-MM-DD'), ''),
                   COALESCE((SELECT string_agg(g.rolname, ', '
                                               ORDER BY g.rolname)
                             FROM pg_auth_members m
                             JOIN pg_roles g ON g.oid = m.roleid
                             WHERE m.member = r.oid), '')
            FROM pg_roles r
            WHERE r.rolname NOT LIKE 'pg\_%'
            ORDER BY r.rolname
        """)
        headers = ["Role", "Type", "Super", "Create DB", "Create Role",
                   "Conn Limit", "Valid Until", "Member Of"]
        rows = [[str(v) if v is not None else "" for v in row]
                for row in cursor.fetchall()]
        return headers, rows

    def principal_settings(self, cursor, name):
        cursor.execute("""
            SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
                   rolinherit, rolreplication, rolconnlimit,
                   COALESCE(to_char(rolvaliduntil, 'YYYY-MM-DD'), '')
            FROM pg_roles WHERE rolname = ?
        """, [name])
        row = cursor.fetchone()
        if not row:
            return {}
        cursor.execute("""
            SELECT g.rolname FROM pg_auth_members m
            JOIN pg_roles g ON g.oid = m.roleid
            JOIN pg_roles r ON r.oid = m.member
            WHERE r.rolname = ? ORDER BY g.rolname
        """, [name])
        return {
            "name": row[0],
            "can_login": bool(row[1]),
            "superuser": bool(row[2]),
            "create_db": bool(row[3]),
            "create_role": bool(row[4]),
            "inherit": bool(row[5]),
            "replication": bool(row[6]),
            "connection_limit": int(row[7]) if row[7] is not None else -1,
            "valid_until": row[8],
            "roles": [r[0] for r in cursor.fetchall()],
        }

    def principal_options(self, cursor, current=None):
        groups = self._lookup(cursor, r"""
            SELECT rolname FROM pg_roles
            WHERE rolname NOT LIKE 'pg\_%' ORDER BY rolname
        """)
        editing = current is not None
        current = current or {}
        if editing:
            # A role cannot be a member of itself.
            groups = [g for g in groups if g != current.get("name")]
        return [
            {"key": "name", "label": "Role Name", "type": "string",
             "default": current.get("name", ""), "required": True},
            {"key": "password", "label": "Password", "type": "password",
             "default": "",
             "help": ("leave blank to keep the current password"
                      if editing else "blank for a role that cannot log in")},
            {"key": "can_login", "label": "Can Log In", "type": "bool",
             "default": current.get("can_login", True)},
            {"key": "superuser", "label": "Superuser", "type": "bool",
             "default": current.get("superuser", False)},
            {"key": "create_db", "label": "Create Databases", "type": "bool",
             "default": current.get("create_db", False)},
            {"key": "create_role", "label": "Create Roles", "type": "bool",
             "default": current.get("create_role", False)},
            {"key": "inherit", "label": "Inherit Privileges", "type": "bool",
             "default": current.get("inherit", True)},
            {"key": "replication", "label": "Replication", "type": "bool",
             "default": current.get("replication", False)},
            {"key": "connection_limit", "label": "Connection Limit",
             "type": "int", "default": current.get("connection_limit", -1),
             "help": "-1 for unlimited"},
            {"key": "valid_until", "label": "Valid Until", "type": "string",
             "default": current.get("valid_until", ""),
             "help": "YYYY-MM-DD; blank for no expiry"},
            {"key": "roles", "label": "Member Of", "type": "multi",
             "default": current.get("roles", []),
             "choices": [[g, g] for g in groups]},
        ]

    _ROLE_FLAGS = (("can_login", "LOGIN", "NOLOGIN"),
                   ("superuser", "SUPERUSER", "NOSUPERUSER"),
                   ("create_db", "CREATEDB", "NOCREATEDB"),
                   ("create_role", "CREATEROLE", "NOCREATEROLE"),
                   ("inherit", "INHERIT", "NOINHERIT"),
                   ("replication", "REPLICATION", "NOREPLICATION"))

    def sql_create_principal(self, opts):
        name = opts.get("name", "")
        role = self.quote_ddl_identifier(name)
        clauses = [on if opts.get(key) else off
                   for key, on, off in self._ROLE_FLAGS]
        password = opts.get("password") or ""
        if password:
            clauses.append(f"PASSWORD {self.quote_ddl_literal(password)}")
        limit = opts.get("connection_limit")
        if limit not in (None, "", -1, "-1"):
            clauses.append(f"CONNECTION LIMIT {int(limit)}")
        valid = (opts.get("valid_until") or "").strip()
        if valid:
            clauses.append(f"VALID UNTIL {self.quote_ddl_literal(valid)}")

        stmts = [f"CREATE ROLE {role} WITH " + " ".join(clauses)]
        for group in opts.get("roles") or []:
            stmts.append(f"GRANT {self.quote_ddl_identifier(group)} TO {role}")
        return stmts

    def sql_alter_principal(self, name, opts, current):
        role = self.quote_ddl_identifier(name)
        stmts = []

        def changed(key):
            return key in opts and opts[key] != current.get(key)

        clauses = [(on if opts[key] else off)
                   for key, on, off in self._ROLE_FLAGS if changed(key)]
        # A blank password field means "leave it alone", not "blank it".
        password = (opts.get("password") or "").strip()
        if password:
            clauses.append(f"PASSWORD {self.quote_ddl_literal(password)}")
        if changed("connection_limit"):
            clauses.append(
                f"CONNECTION LIMIT {int(opts['connection_limit'])}")
        if changed("valid_until"):
            valid = (opts.get("valid_until") or "").strip()
            clauses.append(
                f"VALID UNTIL {self.quote_ddl_literal(valid)}" if valid
                else "VALID UNTIL 'infinity'")
        if clauses:
            stmts.append(f"ALTER ROLE {role} WITH " + " ".join(clauses))

        wanted = set(opts.get("roles") or [])
        held = set(current.get("roles") or [])
        for group in sorted(wanted - held):
            stmts.append(f"GRANT {self.quote_ddl_identifier(group)} TO {role}")
        for group in sorted(held - wanted):
            stmts.append(
                f"REVOKE {self.quote_ddl_identifier(group)} FROM {role}")

        # Renaming last, so the statements above address the original name.
        if changed("name"):
            stmts.append(f"ALTER ROLE {role} RENAME TO "
                         f"{self.quote_ddl_identifier(opts['name'])}")
        return stmts

    def sql_drop_principal(self, name, force=False):
        stmts = []
        if force:
            stmts.append(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE usename = {self.quote_ddl_literal(name)} "
                "AND pid <> pg_backend_pid()")
        stmts.append(f"DROP ROLE {self.quote_ddl_identifier(name)}")
        return stmts

    def sql_principal_sessions(self, name):
        return ("SELECT COUNT(*) FROM pg_stat_activity "
                "WHERE usename = ? AND pid <> pg_backend_pid()", [name])

    # --- Server-side filesystem browsing ---
    #
    # CREATE DATABASE takes no file paths on PostgreSQL, so this is here
    # for TABLESPACE locations rather than the database wizard.  Requires
    # superuser or membership in pg_read_server_files.

    supports_path_browse = True

    def default_paths(self, cursor):
        if cursor is None:
            return {}
        try:
            cursor.execute("SHOW data_directory")
            row = cursor.fetchone()
        except Exception:
            return {}
        return {"data": row[0]} if row and row[0] else {}

    def browse_path(self, cursor, path):
        path = path or self.default_paths(cursor).get("data") or "/"
        # pg_ls_dir returns bare names, so pg_stat_file supplies the type.
        # missing_ok = true keeps a broken symlink from failing the listing.
        cursor.execute("""
            SELECT entry,
                   rtrim(?, '/') || '/' || entry AS full_path,
                   COALESCE(st.isdir, false) AS is_dir,
                   st.size, st.modification
            FROM pg_ls_dir(?) AS entry
            LEFT JOIN LATERAL pg_stat_file(
                rtrim(?, '/') || '/' || entry, true) AS st ON true
            ORDER BY is_dir DESC, entry
        """, [path, path, path])
        return [{"name": name, "path": full,
                 "is_dir": self.coerce_bool(is_dir),
                 "size": None if size is None else int(size),
                 "modified": "" if modified is None else str(modified)}
                for name, full, is_dir, size, modified in cursor.fetchall()]

    # --- Reading a file's contents ---

    supports_file_read = True

    def read_file(self, cursor, path, max_bytes=262144):
        """Return the first MAX_BYTES of PATH as text.

        Read as bytes and decoded here: pg_read_file insists the file is
        valid in the server encoding and refuses anything else outright,
        so a UTF-16 file — which is what Windows tools write — fails on
        its first NUL rather than being shown.
        """
        cursor.execute("SELECT pg_read_binary_file(?, 0, ?)",
                       [path, int(max_bytes)])
        row = cursor.fetchone()
        return self.decode_file_bytes(row[0] if row else None)

    def database_options(self, cursor=None):
        roles = self._lookup(cursor, r"""
            SELECT rolname FROM pg_roles
            WHERE rolname NOT LIKE 'pg\_%' ORDER BY rolname
        """)
        templates = self._lookup(
            cursor, "SELECT datname FROM pg_database "
                    "WHERE datistemplate ORDER BY datname")
        tablespaces = self._lookup(
            cursor, "SELECT spcname FROM pg_tablespace ORDER BY spcname")
        locales = self._lookup(
            cursor, "SELECT DISTINCT collcollate FROM pg_collation "
                    "WHERE collcollate <> '' ORDER BY collcollate")

        def choice(values, blank_label):
            return [["", blank_label]] + [[v, v] for v in values]

        return [
            {"key": "name", "label": "Database Name", "type": "string",
             "default": "", "required": True},
            {"key": "owner", "label": "Owner",
             "type": "choice" if roles else "string",
             "default": "",
             "choices": choice(roles, "(current role)") if roles else None,
             "help": "the owner may drop this database and create schemas in it"},
            {"key": "template", "label": "Template",
             "type": "choice" if templates else "string",
             "default": "template1",
             "choices": [[t, t] for t in templates] if templates else None},
            {"key": "encoding", "label": "Encoding", "type": "choice",
             "default": "UTF8",
             "choices": [[e, e] for e in _PG_ENCODINGS]},
            {"key": "lc_collate", "label": "LC_COLLATE",
             "type": "completing" if locales else "string",
             "default": "", "completions": locales or None,
             "help": "blank inherits; changing it needs template0"},
            {"key": "lc_ctype", "label": "LC_CTYPE",
             "type": "completing" if locales else "string",
             "default": "", "completions": locales or None,
             "help": "blank inherits; changing it needs template0"},
            {"key": "tablespace", "label": "Tablespace",
             "type": "choice" if tablespaces else "string",
             "default": "",
             "choices": (choice(tablespaces, "(default)")
                         if tablespaces else None),
             "help": None if tablespaces else "blank uses the default tablespace"},
            {"key": "connection_limit", "label": "Connection Limit", "type": "int",
             "default": -1, "help": "-1 for unlimited"},
        ]

    def sql_create_database(self, opts):
        db = self.quote_ddl_identifier(opts.get("name", ""))
        clauses = []

        # OWNER, TEMPLATE and TABLESPACE take identifiers; ENCODING and the
        # locale settings take string literals.
        for key, keyword in (("owner", "OWNER"),
                             ("template", "TEMPLATE"),
                             ("tablespace", "TABLESPACE")):
            value = (opts.get(key) or "").strip()
            if value:
                clauses.append(f"{keyword} = {self.quote_ddl_identifier(value)}")

        for key, keyword in (("encoding", "ENCODING"),
                             ("lc_collate", "LC_COLLATE"),
                             ("lc_ctype", "LC_CTYPE")):
            value = (opts.get(key) or "").strip()
            if value:
                clauses.append(f"{keyword} = {self.quote_ddl_literal(value)}")

        limit = opts.get("connection_limit")
        if limit not in (None, "", -1, "-1"):
            clauses.append(f"CONNECTION LIMIT = {int(limit)}")

        sql = f"CREATE DATABASE {db}"
        if clauses:
            sql += "\n  " + "\n  ".join(clauses)
        return [sql]

    def sql_drop_database(self, name, force=False):
        db = self.quote_ddl_identifier(name)
        stmts = []
        if force:
            # DROP DATABASE ... WITH (FORCE) needs PostgreSQL 13+, so
            # terminate backends explicitly to stay compatible with older
            # servers.  The caller runs these in order.
            stmts.append(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE datname = {self.quote_ddl_literal(name)} "
                "AND pid <> pg_backend_pid()")
        stmts.append(f"DROP DATABASE {db}")
        return stmts

    def sql_database_sessions(self, name):
        return ("SELECT COUNT(*) FROM pg_stat_activity "
                "WHERE datname = ? AND pid <> pg_backend_pid()", [name])

    # --- Altering an existing database ---
    #
    # Encoding and locale are fixed at creation time on PostgreSQL, so
    # they are deliberately absent here.

    supports_database_alter = True

    def database_settings(self, cursor, name):
        cursor.execute("""
            SELECT d.datname, r.rolname, d.datconnlimit, t.spcname,
                   d.datallowconn
            FROM pg_database d
            LEFT JOIN pg_roles r ON d.datdba = r.oid
            LEFT JOIN pg_tablespace t ON d.dattablespace = t.oid
            WHERE d.datname = ?
        """, [name])
        row = cursor.fetchone()
        if not row:
            return {}
        return {
            "name": row[0],
            "owner": row[1] or "",
            "connection_limit": int(row[2]) if row[2] is not None else -1,
            "tablespace": row[3] or "",
            "allow_connections": bool(row[4]),
        }

    def settings_options(self, cursor, current):
        roles = self._lookup(cursor, r"""
            SELECT rolname FROM pg_roles
            WHERE rolname NOT LIKE 'pg\_%' ORDER BY rolname
        """)
        tablespaces = self._lookup(
            cursor, "SELECT spcname FROM pg_tablespace ORDER BY spcname")
        return [
            {"key": "name", "label": "Name", "type": "string",
             "default": current.get("name", ""), "required": True,
             "help": "changing this renames the database"},
            {"key": "owner", "label": "Owner",
             "type": "choice" if roles else "string",
             "default": current.get("owner", ""),
             "choices": [[r, r] for r in roles] if roles else None,
             "help": "the owner may drop this database and create schemas in it"},
            {"key": "tablespace", "label": "Tablespace",
             "type": "choice" if tablespaces else "string",
             "default": current.get("tablespace", ""),
             "choices": [[t, t] for t in tablespaces] if tablespaces else None,
             "help": "moving a tablespace rewrites the database files"},
            {"key": "connection_limit", "label": "Connection Limit",
             "type": "int", "default": current.get("connection_limit", -1),
             "help": "-1 for unlimited"},
            {"key": "allow_connections", "label": "Allow Connections",
             "type": "bool", "default": current.get("allow_connections", True)},
        ]

    def sql_alter_database(self, name, opts, current):
        db = self.quote_ddl_identifier(name)
        stmts = []

        def changed(key):
            return key in opts and opts[key] != current.get(key)

        if changed("owner"):
            stmts.append(f"ALTER DATABASE {db} OWNER TO "
                         f"{self.quote_ddl_identifier(opts['owner'])}")
        if changed("connection_limit"):
            stmts.append(f"ALTER DATABASE {db} CONNECTION LIMIT "
                         f"{int(opts['connection_limit'])}")
        if changed("allow_connections"):
            stmts.append(f"ALTER DATABASE {db} ALLOW_CONNECTIONS "
                         f"{'true' if opts['allow_connections'] else 'false'}")
        if changed("tablespace"):
            stmts.append(f"ALTER DATABASE {db} SET TABLESPACE "
                         f"{self.quote_ddl_identifier(opts['tablespace'])}")
        # Renamed last so the statements above address the original name.
        if changed("name"):
            stmts.append(f"ALTER DATABASE {db} RENAME TO "
                         f"{self.quote_ddl_identifier(opts['name'])}")
        return stmts

    def python_type_to_sql(self, python_type):
        return _POSTGRES_TYPE_MAP.get(python_type, "TEXT")


# Server encodings accepted by CREATE DATABASE.  Kept as a literal rather
# than queried: pg_encoding_to_char over a numeric range also returns the
# client-only encodings, which CREATE DATABASE rejects.
_PG_ENCODINGS = [
    "UTF8", "SQL_ASCII", "LATIN1", "LATIN2", "LATIN3", "LATIN4", "LATIN5",
    "LATIN6", "LATIN7", "LATIN8", "LATIN9", "LATIN10",
    "WIN1250", "WIN1251", "WIN1252", "WIN1253", "WIN1254", "WIN1255",
    "WIN1256", "WIN1257", "WIN1258", "WIN866", "WIN874",
    "KOI8R", "KOI8U", "ISO_8859_5", "ISO_8859_6", "ISO_8859_7", "ISO_8859_8",
    "EUC_CN", "EUC_JP", "EUC_KR", "EUC_TW", "EUC_JIS_2004",
]


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
