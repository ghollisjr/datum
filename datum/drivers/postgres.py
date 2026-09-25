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
                   COALESCE((pg_stat_file(rtrim(?, '/') || '/' || entry,
                                          true)).isdir, false) AS is_dir
            FROM pg_ls_dir(?) AS entry
            ORDER BY is_dir DESC, entry
        """, [path, path, path])
        return [{"name": name, "path": full, "is_dir": bool(is_dir)}
                for name, full, is_dir in cursor.fetchall()]

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
             "help": None if roles else "role name; blank uses the current role"},
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
             "choices": [[r, r] for r in roles] if roles else None},
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
