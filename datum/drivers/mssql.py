"""Microsoft SQL Server dialect driver."""

from .base import BaseDriver


class MSSQLDriver(BaseDriver):

    dialect_name = "mssql"
    default_schema = "dbo"

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

    @property
    def sql_running_jobs(self):
        return """
            SELECT
              j.name AS job_name,
              ja.start_execution_date as [start],
              CONVERT(VARCHAR(8), DATEADD(SECOND, DATEDIFF(SECOND, ja.start_execution_date, GETDATE()), 0), 108)
                AS elapsed,
              CONCAT(next_js.step_id, ': ', next_js.step_name) AS step,
              CASE jh.run_status
                  WHEN 0 THEN 'Failed'
                  WHEN 1 THEN 'Succeeded'
                  WHEN 4 THEN 'In Progress'
                  ELSE 'Other'
              END AS status,
              CONCAT(jh.step_id, ': ', jh.step_name) AS last_completed
            FROM msdb.dbo.sysjobactivity ja
            JOIN msdb.dbo.sysjobs j ON ja.job_id = j.job_id
            OUTER APPLY (
                SELECT TOP 1 step_id, step_name, run_status
                FROM msdb.dbo.sysjobhistory
                WHERE job_id = ja.job_id
                  AND step_id > 0
                ORDER BY run_date DESC, run_time DESC
            ) jh
            LEFT JOIN msdb.dbo.sysjobsteps cur_js
                ON cur_js.job_id = ja.job_id
                AND cur_js.step_id = jh.step_id
            LEFT JOIN msdb.dbo.sysjobsteps next_js
                ON next_js.job_id = ja.job_id
                AND next_js.step_id = CASE
                    WHEN cur_js.on_success_action = 3 THEN cur_js.step_id + 1
                    WHEN cur_js.on_success_action = 4 THEN cur_js.on_success_step_id
                    ELSE NULL
                END
            WHERE ja.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions)
              AND ja.start_execution_date IS NOT NULL
              AND ja.stop_execution_date IS NULL
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

    @property
    def sql_list_routines(self):
        return """
            SELECT s.name AS routine_schema,
                   o.name AS routine_name,
                   CASE o.type
                       WHEN 'P'  THEN 'PROCEDURE'
                       WHEN 'FN' THEN 'FUNCTION'
                       WHEN 'IF' THEN 'FUNCTION'
                       WHEN 'TF' THEN 'FUNCTION'
                   END AS routine_type
            FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.type IN ('P', 'FN', 'IF', 'TF')
            ORDER BY s.name, o.name
        """

    @property
    def sql_routine_signatures(self):
        return """
            SELECT s.name AS routine_schema,
                   o.name AS routine_name,
                   ISNULL(
                       STUFF(
                           (SELECT ', ' + p.name + ' '
                                   + TYPE_NAME(p.user_type_id)
                                   + CASE
                                       WHEN TYPE_NAME(p.user_type_id) IN
                                            ('varchar','nvarchar','char','nchar','binary','varbinary')
                                       THEN '(' + CASE WHEN p.max_length = -1 THEN 'MAX'
                                                       WHEN TYPE_NAME(p.user_type_id) IN ('nvarchar','nchar')
                                                       THEN CAST(p.max_length/2 AS VARCHAR)
                                                       ELSE CAST(p.max_length AS VARCHAR) END + ')'
                                       WHEN TYPE_NAME(p.user_type_id) IN ('decimal','numeric')
                                       THEN '(' + CAST(p.precision AS VARCHAR) + ','
                                            + CAST(p.scale AS VARCHAR) + ')'
                                       ELSE ''
                                     END
                                   + CASE WHEN p.is_output = 1 THEN ' OUTPUT' ELSE '' END
                            FROM sys.parameters p
                            WHERE p.object_id = o.object_id
                              AND p.parameter_id > 0
                            ORDER BY p.parameter_id
                            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
                       , 1, 2, ''),
                   '') AS signature
            FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.type IN ('P', 'FN', 'IF', 'TF')
            ORDER BY s.name, o.name
        """

    def sql_list_routines_like(self, pattern):
        return ("""
            SELECT s.name AS routine_schema,
                   o.name AS routine_name,
                   CASE o.type
                       WHEN 'P'  THEN 'PROCEDURE'
                       WHEN 'FN' THEN 'FUNCTION'
                       WHEN 'IF' THEN 'FUNCTION'
                       WHEN 'TF' THEN 'FUNCTION'
                   END AS routine_type
            FROM sys.objects o
            JOIN sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.type IN ('P', 'FN', 'IF', 'TF')
              AND o.name LIKE ?
            ORDER BY s.name, o.name
        """, [pattern])

    def sql_list_columns(self, schema, table, database=None):
        if database:
            db = self.quote_identifier(database)
            return (f"""
                SELECT COLUMN_NAME,
                       DATA_TYPE,
                       IS_NULLABLE,
                       COLUMN_DEFAULT
                FROM {db}.INFORMATION_SCHEMA.COLUMNS
                WHERE LOWER(TABLE_SCHEMA) = LOWER(?)
                  AND LOWER(TABLE_NAME)   = LOWER(?)
                ORDER BY ORDINAL_POSITION
            """, [schema, table])
        return super().sql_list_columns(schema, table)

    def sql_resolve_object_type(self, schema, name, database=None):
        # Temp tables live in tempdb; resolve via OBJECT_ID
        if name.startswith('#'):
            return ("""
                SELECT 'TABLE' AS object_type
                WHERE OBJECT_ID('tempdb..' + ?) IS NOT NULL
            """, [name])
        if database:
            db = self.quote_identifier(database)
            return (f"""
                SELECT CASE o.type
                           WHEN 'U'  THEN 'TABLE'
                           WHEN 'V'  THEN 'VIEW'
                           WHEN 'P'  THEN 'PROCEDURE'
                           WHEN 'FN' THEN 'FUNCTION'
                           WHEN 'IF' THEN 'FUNCTION'
                           WHEN 'TF' THEN 'FUNCTION'
                       END AS object_type
                FROM {db}.sys.objects o
                JOIN {db}.sys.schemas s ON o.schema_id = s.schema_id
                WHERE s.name = ?
                  AND o.name = ?
                  AND o.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF')
            """, [schema, name])
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

    def sql_get_definition(self, schema, name, object_type, database=None):
        # Temp tables: get columns from tempdb via OBJECT_ID
        if name.startswith('#') and object_type == 'TABLE':
            return ("""
                SELECT c.name                       AS COLUMN_NAME,
                       TYPE_NAME(c.user_type_id)     AS DATA_TYPE,
                       CASE c.is_nullable
                           WHEN 1 THEN 'YES' ELSE 'NO'
                       END                           AS IS_NULLABLE,
                       c.max_length                  AS CHARACTER_MAXIMUM_LENGTH,
                       c.precision                   AS NUMERIC_PRECISION,
                       c.scale                       AS NUMERIC_SCALE,
                       d.definition                  AS COLUMN_DEFAULT
                FROM tempdb.sys.columns c
                LEFT JOIN tempdb.sys.default_constraints d
                       ON d.parent_object_id = c.object_id
                      AND d.parent_column_id = c.column_id
                WHERE c.object_id = OBJECT_ID('tempdb..' + ?)
                ORDER BY c.column_id
            """, [name])
        if database:
            db = self.quote_identifier(database)
            if object_type == 'TABLE':
                return (f"""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                           CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION,
                           NUMERIC_SCALE, COLUMN_DEFAULT
                    FROM {db}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = ?
                      AND TABLE_NAME   = ?
                    ORDER BY ORDINAL_POSITION
                """, [schema, name])
            else:
                return (f"""
                    SELECT m.definition
                    FROM {db}.sys.sql_modules m
                    JOIN {db}.sys.objects o ON m.object_id = o.object_id
                    JOIN {db}.sys.schemas s ON o.schema_id = s.schema_id
                    WHERE s.name = ?
                      AND o.name = ?
                """, [schema, name])
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
            SELECT d.name,
                   d.database_id,
                   d.create_date,
                   SUSER_SNAME(d.owner_sid)            AS owner,
                   d.compatibility_level,
                   d.collation_name,
                   d.state_desc                         AS state,
                   d.recovery_model_desc                AS recovery_model,
                   d.log_reuse_wait_desc                AS log_reuse_wait,
                   d.is_auto_close_on,
                   d.is_auto_shrink_on,
                   d.is_read_only,
                   d.snapshot_isolation_state_desc       AS snapshot_isolation,
                   d.is_auto_create_stats_on,
                   d.is_auto_update_stats_on,
                   d.page_verify_option_desc             AS page_verify,
                   df.data_file_name,
                   df.data_file_path,
                   df.data_size_mb,
                   lf.log_file_name,
                   lf.log_file_path,
                   lf.log_size_mb
            FROM sys.databases d
            OUTER APPLY (
                SELECT TOP 1 f.name AS data_file_name,
                       f.physical_name AS data_file_path,
                       CAST(f.size * 8.0 / 1024 AS DECIMAL(18,2)) AS data_size_mb
                FROM sys.master_files f
                WHERE f.database_id = d.database_id AND f.type = 0
                ORDER BY f.file_id
            ) df
            OUTER APPLY (
                SELECT TOP 1 f.name AS log_file_name,
                       f.physical_name AS log_file_path,
                       CAST(f.size * 8.0 / 1024 AS DECIMAL(18,2)) AS log_size_mb
                FROM sys.master_files f
                WHERE f.database_id = d.database_id AND f.type = 1
                ORDER BY f.file_id
            ) lf
            WHERE d.name = ?
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

    def sql_list_schemas_in_db(self, database):
        return f"""
            SELECT name FROM [{database}].sys.schemas ORDER BY name
        """

    def sql_list_tables_in_db(self, database):
        return f"""
            SELECT s.name AS table_schema,
                   t.name AS table_name,
                   CASE t.type
                       WHEN 'U' THEN 'TABLE'
                       WHEN 'V' THEN 'VIEW'
                       ELSE t.type
                   END AS table_type
            FROM [{database}].sys.objects t
            JOIN [{database}].sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.type IN ('U', 'V')
            ORDER BY s.name, t.name
        """

    def sql_list_routines_in_db(self, database):
        return f"""
            SELECT s.name AS routine_schema,
                   o.name AS routine_name,
                   CASE o.type
                       WHEN 'P'  THEN 'PROCEDURE'
                       WHEN 'FN' THEN 'FUNCTION'
                       WHEN 'IF' THEN 'FUNCTION'
                       WHEN 'TF' THEN 'FUNCTION'
                   END AS routine_type
            FROM [{database}].sys.objects o
            JOIN [{database}].sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.type IN ('P', 'FN', 'IF', 'TF')
            ORDER BY s.name, o.name
        """

    def sql_routine_signatures_in_db(self, database):
        return f"""
            SELECT s.name AS routine_schema,
                   o.name AS routine_name,
                   ISNULL(
                       STUFF(
                           (SELECT ', ' + p.name + ' '
                                   + TYPE_NAME(p.user_type_id)
                                   + CASE
                                       WHEN TYPE_NAME(p.user_type_id) IN
                                            ('varchar','nvarchar','char','nchar','binary','varbinary')
                                       THEN '(' + CASE WHEN p.max_length = -1 THEN 'MAX'
                                                       WHEN TYPE_NAME(p.user_type_id) IN ('nvarchar','nchar')
                                                       THEN CAST(p.max_length/2 AS VARCHAR)
                                                       ELSE CAST(p.max_length AS VARCHAR) END + ')'
                                       WHEN TYPE_NAME(p.user_type_id) IN ('decimal','numeric')
                                       THEN '(' + CAST(p.precision AS VARCHAR) + ','
                                            + CAST(p.scale AS VARCHAR) + ')'
                                       ELSE ''
                                     END
                                   + CASE WHEN p.is_output = 1 THEN ' OUTPUT' ELSE '' END
                            FROM [{database}].sys.parameters p
                            WHERE p.object_id = o.object_id
                              AND p.parameter_id > 0
                            ORDER BY p.parameter_id
                            FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')
                       , 1, 2, ''),
                   '') AS signature
            FROM [{database}].sys.objects o
            JOIN [{database}].sys.schemas s ON o.schema_id = s.schema_id
            WHERE o.type IN ('P', 'FN', 'IF', 'TF')
            ORDER BY s.name, o.name
        """

    def quote_identifier(self, name):
        return f"[{name}]"

    def quote_ddl_identifier(self, name):
        name = self.validate_identifier(name)
        return "[" + name.replace("]", "]]") + "]"

    # --- Database DDL ---

    supports_database_ddl = True

    def safe_fallback_database(self):
        return "master"

    # --- Server-side filesystem browsing ---

    supports_path_browse = True

    def path_separator(self, cursor=None):
        # SQL Server runs on Linux as well as Windows; infer the separator
        # from a path the server itself reports rather than assuming.
        default = (self.default_paths(cursor) or {}).get("data", "")
        return "\\" if "\\" in default else "/"

    def default_paths(self, cursor):
        if cursor is None:
            return {}
        try:
            cursor.execute(
                "SELECT CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS NVARCHAR(4000)), "
                "       CAST(SERVERPROPERTY('InstanceDefaultLogPath') AS NVARCHAR(4000))")
            row = cursor.fetchone()
        except Exception:
            return {}
        if not row:
            return {}
        return {"data": (row[0] or "").rstrip("/\\"),
                "log": (row[1] or "").rstrip("/\\")}

    def browse_path(self, cursor, path):
        path = path or self.default_paths(cursor).get("data") or "/"
        # Documented and permission-light, but SQL Server 2017+ only.
        try:
            cursor.execute(
                "SELECT full_filesystem_path, is_directory "
                "FROM sys.dm_os_enumerate_filesystem(?, N'*') "
                "ORDER BY is_directory DESC, full_filesystem_path", [path])
            rows = cursor.fetchall()
            entries = []
            for full, is_dir in rows:
                if not full:
                    continue
                name = full.rstrip("/\\")
                index = max(name.rfind("/"), name.rfind("\\"))
                entries.append({"name": name[index + 1:] if index >= 0 else name,
                                "path": full,
                                "is_dir": bool(is_dir)})
            return entries
        except Exception:
            pass
        # Older servers: xp_dirtree reports names and a file/dir flag only.
        cursor.execute("EXEC master.dbo.xp_dirtree ?, 1, 1", [path])
        entries = []
        for row in cursor.fetchall():
            name, _depth, is_file = row[0], row[1], row[2]
            if not name:
                continue
            entries.append({"name": name,
                            "path": self.join_path(path, name, cursor),
                            "is_dir": not bool(is_file)})
        entries.sort(key=lambda e: (not e["is_dir"], e["name"].lower()))
        return entries

    def database_options(self, cursor=None):
        logins = self._lookup(cursor, """
            SELECT name FROM sys.server_principals
            WHERE type IN ('S', 'U', 'G') AND name NOT LIKE '##%'
            ORDER BY name
        """)
        # ~5500 collations on a stock instance — far too many for a menu,
        # so this is a completing field rather than a choice.
        collations = self._lookup(
            cursor, "SELECT name FROM sys.fn_helpcollations() ORDER BY name")
        defaults = self.default_paths(cursor)
        return [
            {"key": "name", "label": "Database Name", "type": "string",
             "default": "", "required": True},
            {"key": "owner", "label": "Owner",
             "type": "choice" if logins else "string",
             "default": "",
             "choices": ([["", "(creator)"]] + [[n, n] for n in logins]
                         if logins else None),
             "help": None if logins else "login name; blank leaves the creator as owner"},
            {"key": "collation", "label": "Collation",
             "type": "completing" if collations else "string",
             "default": "",
             "completions": collations or None,
             "help": "blank uses the server default"},
            {"key": "recovery_model", "label": "Recovery Model", "type": "choice",
             "default": "", "choices": [["", "Server default"],
                                        ["FULL", "Full"],
                                        ["SIMPLE", "Simple"],
                                        ["BULK_LOGGED", "Bulk-logged"]]},
            {"key": "data_dir", "label": "Data File Directory", "type": "path",
             "default": defaults.get("data", ""),
             "help": "blank uses the server's default data directory"},
            {"key": "data_size_mb", "label": "Data Initial Size", "type": "int",
             "default": 0, "help": "MB; 0 uses the server default"},
            {"key": "data_growth", "label": "Data Autogrowth", "type": "int",
             "default": 0, "help": "0 uses the server default"},
            {"key": "data_growth_unit", "label": "Data Growth Unit",
             "type": "choice", "default": "MB",
             "choices": [["MB", "Megabytes"], ["%", "Percent"]]},
            {"key": "data_max_mb", "label": "Data Max Size", "type": "int",
             "default": 0, "help": "MB; 0 for unlimited growth"},
            {"key": "log_dir", "label": "Log File Directory", "type": "path",
             "default": defaults.get("log", ""),
             "help": "blank uses the server's default log directory"},
            {"key": "log_size_mb", "label": "Log Initial Size", "type": "int",
             "default": 0, "help": "MB; 0 uses the server default"},
            {"key": "log_growth", "label": "Log Autogrowth", "type": "int",
             "default": 0, "help": "0 uses the server default"},
            {"key": "log_growth_unit", "label": "Log Growth Unit",
             "type": "choice", "default": "MB",
             "choices": [["MB", "Megabytes"], ["%", "Percent"]]},
            {"key": "log_max_mb", "label": "Log Max Size", "type": "int",
             "default": 0, "help": "MB; 0 for unlimited growth"},
            {"key": "compatibility_level", "label": "Compatibility Level",
             "type": "choice", "default": "",
             "choices": [["", "Server default"]]
                        + [[str(l), str(l)] for l in self._compat_levels(cursor)]},
        ]

    def _compat_levels(self, cursor):
        """Return the compatibility levels this server accepts, newest first."""
        major = 0
        if cursor is not None:
            try:
                cursor.execute(
                    "SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS INT)")
                row = cursor.fetchone()
                major = int(row[0]) if row and row[0] else 0
            except Exception:
                major = 0
        if not major:
            return []
        # 2008 is 100 and every release since has added ten.
        newest = major * 10
        return list(range(newest, 90, -10))

    def sql_create_database(self, opts):
        name = opts.get("name", "")
        db = self.quote_ddl_identifier(name)
        stmts = []

        # File clauses are only emitted when a path is supplied, since
        # SIZE/FILEGROWTH cannot appear without NAME/FILENAME.
        def file_clause(logical, path, size_mb, growth, growth_unit, max_mb):
            parts = [f"NAME = {self.quote_ddl_identifier(logical)}",
                     f"FILENAME = {self.quote_ddl_literal(path)}"]
            if size_mb:
                parts.append(f"SIZE = {int(size_mb)}MB")
            # MAXSIZE must precede FILEGROWTH in the file spec.
            parts.append(f"MAXSIZE = {int(max_mb)}MB" if max_mb
                         else "MAXSIZE = UNLIMITED")
            if growth:
                if growth_unit == "%":
                    parts.append(f"FILEGROWTH = {int(growth)}%")
                else:
                    parts.append(f"FILEGROWTH = {int(growth)}MB")
            return "(" + ", ".join(parts) + ")"

        create = f"CREATE DATABASE {db}"
        # The form collects directories, which is what a user browses to;
        # the file names follow SQL Server's own convention.
        data_dir = (opts.get("data_dir") or "").strip()
        log_dir = (opts.get("log_dir") or "").strip()
        if data_dir:
            create += "\n  ON PRIMARY " + file_clause(
                name, self.join_path(data_dir, f"{name}.mdf"),
                opts.get("data_size_mb"), opts.get("data_growth"),
                opts.get("data_growth_unit"), opts.get("data_max_mb"))
        # LOG ON is only legal after an ON clause, so a log directory on
        # its own is ignored rather than emitted as invalid SQL.
        if log_dir and data_dir:
            create += "\n  LOG ON " + file_clause(
                f"{name}_log", self.join_path(log_dir, f"{name}_log.ldf"),
                opts.get("log_size_mb"), opts.get("log_growth"),
                opts.get("log_growth_unit"), opts.get("log_max_mb"))
        collation = (opts.get("collation") or "").strip()
        if collation:
            # COLLATE takes a bare identifier, not a string literal.
            create += f"\n  COLLATE {self.validate_identifier(collation)}"
        stmts.append(create)

        compat = str(opts.get("compatibility_level") or "").strip()
        if compat:
            if not compat.isdigit():
                raise ValueError(f"Invalid compatibility level: {compat}")
            stmts.append(
                f"ALTER DATABASE {db} SET COMPATIBILITY_LEVEL = {int(compat)}")

        recovery = (opts.get("recovery_model") or "").strip()
        if recovery:
            if recovery not in ("FULL", "SIMPLE", "BULK_LOGGED"):
                raise ValueError(f"Unknown recovery model: {recovery}")
            stmts.append(f"ALTER DATABASE {db} SET RECOVERY {recovery}")

        owner = (opts.get("owner") or "").strip()
        if owner:
            stmts.append(f"ALTER AUTHORIZATION ON DATABASE::{db} "
                         f"TO {self.quote_ddl_identifier(owner)}")
        return stmts

    def sql_drop_database(self, name, force=False):
        db = self.quote_ddl_identifier(name)
        stmts = []
        if force:
            # Evicts every other session so the DROP cannot be blocked.
            stmts.append(f"ALTER DATABASE {db} SET SINGLE_USER "
                         f"WITH ROLLBACK IMMEDIATE")
        stmts.append(f"DROP DATABASE {db}")
        return stmts

    def sql_database_sessions(self, name):
        return ("SELECT COUNT(*) FROM sys.dm_exec_sessions s "
                "JOIN sys.databases d ON s.database_id = d.database_id "
                "WHERE d.name = ? AND s.session_id <> @@SPID", [name])

    # --- Altering an existing database ---

    supports_database_alter = True

    def database_settings(self, cursor, name):
        cursor.execute("""
            SELECT name, SUSER_SNAME(owner_sid), recovery_model_desc,
                   compatibility_level, collation_name, is_read_only,
                   is_auto_shrink_on, is_auto_close_on,
                   is_auto_create_stats_on, is_auto_update_stats_on,
                   user_access_desc
            FROM sys.databases WHERE name = ?
        """, [name])
        row = cursor.fetchone()
        if not row:
            return {}
        return {
            "name": row[0],
            "owner": row[1] or "",
            "recovery_model": row[2] or "",
            "compatibility_level": str(row[3] or ""),
            "collation": row[4] or "",
            "read_only": bool(row[5]),
            "auto_shrink": bool(row[6]),
            "auto_close": bool(row[7]),
            "auto_create_stats": bool(row[8]),
            "auto_update_stats": bool(row[9]),
            "user_access": row[10] or "MULTI_USER",
        }

    def settings_options(self, cursor, current):
        logins = self._lookup(cursor, """
            SELECT name FROM sys.server_principals
            WHERE type IN ('S', 'U', 'G') AND name NOT LIKE '##%'
            ORDER BY name
        """)
        collations = self._lookup(
            cursor, "SELECT name FROM sys.fn_helpcollations() ORDER BY name")
        levels = self._compat_levels(cursor)
        return [
            {"key": "name", "label": "Name", "type": "string",
             "default": current.get("name", ""), "required": True,
             "help": "changing this renames the database"},
            {"key": "owner", "label": "Owner",
             "type": "choice" if logins else "string",
             "default": current.get("owner", ""),
             "choices": [[n, n] for n in logins] if logins else None},
            {"key": "recovery_model", "label": "Recovery Model",
             "type": "choice", "default": current.get("recovery_model", ""),
             "choices": [["FULL", "Full"], ["SIMPLE", "Simple"],
                         ["BULK_LOGGED", "Bulk-logged"]]},
            {"key": "compatibility_level", "label": "Compatibility Level",
             "type": "choice",
             "default": current.get("compatibility_level", ""),
             "choices": [[str(l), str(l)] for l in levels] or
                        [[current.get("compatibility_level", ""),
                          current.get("compatibility_level", "")]]},
            {"key": "collation", "label": "Collation",
             "type": "completing" if collations else "string",
             "default": current.get("collation", ""),
             "completions": collations or None},
            {"key": "user_access", "label": "Access", "type": "choice",
             "default": current.get("user_access", "MULTI_USER"),
             "choices": [["MULTI_USER", "Multi user"],
                         ["SINGLE_USER", "Single user"],
                         ["RESTRICTED_USER", "Restricted user"]]},
            {"key": "read_only", "label": "Read Only", "type": "bool",
             "default": current.get("read_only", False)},
            {"key": "auto_shrink", "label": "Auto Shrink", "type": "bool",
             "default": current.get("auto_shrink", False)},
            {"key": "auto_close", "label": "Auto Close", "type": "bool",
             "default": current.get("auto_close", False)},
            {"key": "auto_create_stats", "label": "Auto Create Statistics",
             "type": "bool", "default": current.get("auto_create_stats", True)},
            {"key": "auto_update_stats", "label": "Auto Update Statistics",
             "type": "bool", "default": current.get("auto_update_stats", True)},
        ]

    def sql_alter_database(self, name, opts, current):
        db = self.quote_ddl_identifier(name)
        stmts = []

        def changed(key):
            return key in opts and opts[key] != current.get(key)

        # Renaming last would target a name that no longer exists, so the
        # other statements are emitted against the original name first.
        for key, clause in (("read_only", "READ_ONLY"),
                            ("auto_shrink", "AUTO_SHRINK"),
                            ("auto_close", "AUTO_CLOSE"),
                            ("auto_create_stats", "AUTO_CREATE_STATISTICS"),
                            ("auto_update_stats", "AUTO_UPDATE_STATISTICS")):
            if changed(key):
                if key == "read_only":
                    stmts.append(f"ALTER DATABASE {db} SET "
                                 f"{'READ_ONLY' if opts[key] else 'READ_WRITE'}")
                else:
                    stmts.append(f"ALTER DATABASE {db} SET {clause} "
                                 f"{'ON' if opts[key] else 'OFF'}")

        if changed("recovery_model"):
            value = opts["recovery_model"]
            if value not in ("FULL", "SIMPLE", "BULK_LOGGED"):
                raise ValueError(f"Unknown recovery model: {value}")
            stmts.append(f"ALTER DATABASE {db} SET RECOVERY {value}")

        if changed("compatibility_level"):
            value = str(opts["compatibility_level"])
            if not value.isdigit():
                raise ValueError(f"Invalid compatibility level: {value}")
            stmts.append(
                f"ALTER DATABASE {db} SET COMPATIBILITY_LEVEL = {int(value)}")

        if changed("collation"):
            stmts.append(f"ALTER DATABASE {db} COLLATE "
                         f"{self.validate_identifier(opts['collation'])}")

        if changed("user_access"):
            value = opts["user_access"]
            if value not in ("MULTI_USER", "SINGLE_USER", "RESTRICTED_USER"):
                raise ValueError(f"Unknown access mode: {value}")
            stmts.append(f"ALTER DATABASE {db} SET {value}")

        if changed("owner"):
            stmts.append(f"ALTER AUTHORIZATION ON DATABASE::{db} "
                         f"TO {self.quote_ddl_identifier(opts['owner'])}")

        if changed("name"):
            stmts.append(f"ALTER DATABASE {db} MODIFY NAME = "
                         f"{self.quote_ddl_identifier(opts['name'])}")
        return stmts

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
