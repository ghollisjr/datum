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
             "help": "the owner becomes dbo here, with full control"},
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

    # --- Logins and database users ---

    supports_security = True
    supports_user_mapping = True
    principal_noun = "login"

    def list_principals(self, cursor):
        cursor.execute("""
            SELECT sp.name,
                   sp.type_desc,
                   CASE WHEN sp.is_disabled = 1 THEN 'Disabled'
                        ELSE 'Enabled' END,
                   ISNULL(sp.default_database_name, ''),
                   ISNULL(STUFF((SELECT ', ' + r.name
                                 FROM sys.server_role_members rm
                                 JOIN sys.server_principals r
                                   ON r.principal_id = rm.role_principal_id
                                 WHERE rm.member_principal_id = sp.principal_id
                                 FOR XML PATH('')), 1, 2, ''), ''),
                   -- Owning a database makes a login dbo inside it, with
                   -- full control, regardless of any server role.  Left
                   -- off the list it reads as an unprivileged login.
                   ISNULL(STUFF((SELECT ', ' + d.name
                                 FROM sys.databases d
                                 WHERE d.owner_sid = sp.sid
                                 ORDER BY d.name
                                 FOR XML PATH('')), 1, 2, ''), ''),
                   CONVERT(VARCHAR(19), sp.create_date, 120)
            FROM sys.server_principals sp
            WHERE sp.type IN ('S', 'U', 'G') AND sp.name NOT LIKE '##%'
            ORDER BY sp.name
        """)
        headers = ["Login", "Type", "State", "Default Database",
                   "Server Roles", "Owns (dbo)", "Created"]
        rows = [[str(v) if v is not None else "" for v in row]
                for row in cursor.fetchall()]
        return headers, rows

    def _server_roles(self, cursor):
        return self._lookup(cursor,
                            "SELECT name FROM sys.server_principals "
                            "WHERE type = 'R' AND name NOT LIKE '##%' "
                            "ORDER BY name")

    def principal_settings(self, cursor, name):
        cursor.execute("""
            SELECT sp.name, sp.type_desc, sp.is_disabled,
                   ISNULL(sp.default_database_name, ''),
                   ISNULL(sl.is_policy_checked, 0),
                   ISNULL(sl.is_expiration_checked, 0)
            FROM sys.server_principals sp
            LEFT JOIN sys.sql_logins sl ON sl.principal_id = sp.principal_id
            WHERE sp.name = ? AND sp.type IN ('S', 'U', 'G')
        """, [name])
        row = cursor.fetchone()
        if not row:
            return {}
        cursor.execute("""
            SELECT r.name FROM sys.server_role_members rm
            JOIN sys.server_principals r
              ON r.principal_id = rm.role_principal_id
            JOIN sys.server_principals m
              ON m.principal_id = rm.member_principal_id
            WHERE m.name = ? ORDER BY r.name
        """, [name])
        return {
            "name": row[0],
            "login_type": row[1],
            "disabled": bool(row[2]),
            "default_database": row[3],
            "check_policy": bool(row[4]),
            "check_expiration": bool(row[5]),
            "roles": [r[0] for r in cursor.fetchall()],
        }

    def principal_options(self, cursor, current=None):
        databases = self._lookup(
            cursor, "SELECT name FROM sys.databases WHERE state = 0 "
                    "ORDER BY name")
        roles = self._server_roles(cursor)
        editing = current is not None
        current = current or {}
        # A Windows login has no password to set here; the type is fixed
        # once the login exists.
        windows = editing and current.get("login_type") != "SQL_LOGIN"

        fields = [
            {"key": "name", "label": "Login Name", "type": "string",
             "default": current.get("name", ""), "required": True,
             "help": "DOMAIN\\user for a Windows login" if not editing else None},
        ]
        if not editing:
            fields.append(
                {"key": "login_type", "label": "Authentication",
                 "type": "choice", "default": "SQL_LOGIN",
                 "choices": [["SQL_LOGIN", "SQL Server authentication"],
                             ["WINDOWS_LOGIN", "Windows authentication"]]})
        if not windows:
            fields.append(
                {"key": "password", "label": "Password", "type": "password",
                 "default": "",
                 "help": ("leave blank to keep the current password"
                          if editing else "required for SQL authentication")})
            fields.extend([
                {"key": "check_policy", "label": "Enforce Password Policy",
                 "type": "bool", "default": current.get("check_policy", True)},
                {"key": "check_expiration", "label": "Enforce Expiration",
                 "type": "bool",
                 "default": current.get("check_expiration", False)},
            ])
        fields.extend([
            {"key": "default_database", "label": "Default Database",
             "type": "choice" if databases else "string",
             "default": current.get("default_database", "master"),
             "choices": [[d, d] for d in databases] if databases else None},
            {"key": "disabled", "label": "Disabled", "type": "bool",
             "default": current.get("disabled", False)},
            {"key": "roles", "label": "Server Roles", "type": "multi",
             "default": current.get("roles", []),
             "choices": [[r, r] for r in roles]},
        ])
        return fields

    def sql_create_principal(self, opts):
        name = opts.get("name", "")
        login = self.quote_ddl_identifier(name)
        windows = opts.get("login_type") == "WINDOWS_LOGIN"
        stmts = []

        if windows:
            sql = f"CREATE LOGIN {login} FROM WINDOWS"
            clauses = []
        else:
            password = opts.get("password") or ""
            if not password:
                raise ValueError(
                    "a password is required for SQL Server authentication")
            sql = (f"CREATE LOGIN {login} WITH PASSWORD = "
                   f"{self.quote_ddl_literal(password)}")
            clauses = [
                f"CHECK_POLICY = {'ON' if opts.get('check_policy', True) else 'OFF'}",
                f"CHECK_EXPIRATION = "
                f"{'ON' if opts.get('check_expiration') else 'OFF'}",
            ]
        default_db = (opts.get("default_database") or "").strip()
        if default_db:
            clause = (f"DEFAULT_DATABASE = "
                      f"{self.quote_ddl_identifier(default_db)}")
            clauses = ([clause] + clauses if windows else clauses + [clause])
        if clauses:
            sql += (", " if not windows else " WITH ") + ", ".join(clauses)
        stmts.append(sql)

        if opts.get("disabled"):
            stmts.append(f"ALTER LOGIN {login} DISABLE")
        for role in opts.get("roles") or []:
            stmts.append(f"ALTER SERVER ROLE "
                         f"{self.quote_ddl_identifier(role)} "
                         f"ADD MEMBER {login}")
        return stmts

    def sql_alter_principal(self, name, opts, current):
        login = self.quote_ddl_identifier(name)
        stmts = []

        def changed(key):
            return key in opts and opts[key] != current.get(key)

        # A blank password field means "leave it alone", not "blank it".
        password = (opts.get("password") or "").strip()
        if password:
            stmts.append(f"ALTER LOGIN {login} WITH PASSWORD = "
                         f"{self.quote_ddl_literal(password)}")
        for key, clause in (("check_policy", "CHECK_POLICY"),
                            ("check_expiration", "CHECK_EXPIRATION")):
            if changed(key):
                stmts.append(f"ALTER LOGIN {login} WITH {clause} = "
                             f"{'ON' if opts[key] else 'OFF'}")
        if changed("default_database"):
            stmts.append(
                f"ALTER LOGIN {login} WITH DEFAULT_DATABASE = "
                f"{self.quote_ddl_identifier(opts['default_database'])}")
        if changed("disabled"):
            stmts.append(f"ALTER LOGIN {login} "
                         f"{'DISABLE' if opts['disabled'] else 'ENABLE'}")

        wanted = set(opts.get("roles") or [])
        held = set(current.get("roles") or [])
        for role in sorted(wanted - held):
            stmts.append(f"ALTER SERVER ROLE "
                         f"{self.quote_ddl_identifier(role)} ADD MEMBER {login}")
        for role in sorted(held - wanted):
            stmts.append(f"ALTER SERVER ROLE "
                         f"{self.quote_ddl_identifier(role)} DROP MEMBER {login}")

        # Renaming last, so the statements above address the original name.
        if changed("name"):
            stmts.append(f"ALTER LOGIN {login} WITH NAME = "
                         f"{self.quote_ddl_identifier(opts['name'])}")
        return stmts

    def sql_drop_principal(self, name, force=False):
        stmts = []
        if force:
            # KILL takes a literal session id, so the ids are gathered
            # into a batch and executed.
            literal = self.quote_ddl_literal(self.validate_identifier(name))
            stmts.append(
                "DECLARE @kill NVARCHAR(MAX) = N''; "
                "SELECT @kill += N'KILL ' + CAST(session_id AS NVARCHAR(10)) "
                "+ N'; ' FROM sys.dm_exec_sessions "
                f"WHERE login_name = {literal} AND session_id <> @@SPID; "
                "IF LEN(@kill) > 0 EXEC sp_executesql @kill")
        stmts.append(f"DROP LOGIN {self.quote_ddl_identifier(name)}")
        return stmts

    def sql_principal_sessions(self, name):
        return ("SELECT COUNT(*) FROM sys.dm_exec_sessions "
                "WHERE login_name = ? AND session_id <> @@SPID", [name])

    # --- Database user mapping ---

    def list_user_mappings(self, cursor, login):
        # sys.database_principals only ever describes the current
        # database, so each database is asked in turn rather than joined
        # against — a single query would silently report master's users
        # for every row.
        databases = self._lookup(
            cursor, "SELECT name FROM sys.databases WHERE state = 0 "
                    "ORDER BY name")
        headers = ["Database", "User", "Database Roles"]
        rows = []
        for database in databases:
            db = self.quote_ddl_identifier(database)
            try:
                cursor.execute(f"""
                    SELECT dp.name,
                           ISNULL(STUFF((SELECT ', ' + r.name
                                         FROM {db}.sys.database_role_members rm
                                         JOIN {db}.sys.database_principals r
                                           ON r.principal_id = rm.role_principal_id
                                         WHERE rm.member_principal_id
                                               = dp.principal_id
                                         FOR XML PATH('')), 1, 2, ''), '')
                    FROM {db}.sys.database_principals dp
                    WHERE dp.sid = SUSER_SID(?)
                """, [login])
                row = cursor.fetchone()
            except Exception:
                # A database the caller cannot read is skipped rather
                # than failing the whole listing.
                continue
            if row:
                rows.append([database, row[0] or "", row[1] or ""])
        return headers, rows

    def user_roles(self, cursor, database, username):
        """Return the database roles USERNAME holds in DATABASE."""
        db = self.quote_ddl_identifier(database)
        try:
            cursor.execute(f"""
                SELECT r.name
                FROM {db}.sys.database_role_members rm
                JOIN {db}.sys.database_principals r
                  ON r.principal_id = rm.role_principal_id
                JOIN {db}.sys.database_principals m
                  ON m.principal_id = rm.member_principal_id
                WHERE m.name = ? ORDER BY r.name
            """, [username])
            return [r[0] for r in cursor.fetchall()]
        except Exception:
            return []

    def user_role_options(self, cursor, database, current_roles):
        """Field descriptors for setting a user's roles in DATABASE."""
        return [
            {"key": "roles", "label": "Database Roles", "type": "multi",
             "default": list(current_roles or []),
             "choices": [[r, r]
                         for r in self._database_roles(cursor, database)]},
        ]

    def sql_set_user_roles(self, database, username, roles, current_roles):
        """Return statements reconciling USERNAME's roles in DATABASE."""
        self.validate_identifier(database)
        user = self.quote_ddl_identifier(username)
        wanted, held = set(roles or []), set(current_roles or [])
        stmts = []
        for role in sorted(wanted - held):
            stmts.append(self._in_database(
                database, f"ALTER ROLE {self.quote_ddl_identifier(role)} "
                          f"ADD MEMBER {user}"))
        for role in sorted(held - wanted):
            stmts.append(self._in_database(
                database, f"ALTER ROLE {self.quote_ddl_identifier(role)} "
                          f"DROP MEMBER {user}"))
        return stmts

    def _database_roles(self, cursor, database):
        db = self.quote_ddl_identifier(database)
        try:
            cursor.execute(f"SELECT name FROM {db}.sys.database_principals "
                           f"WHERE type = 'R' ORDER BY name")
            return [r[0] for r in cursor.fetchall()]
        except Exception:
            return []

    def user_mapping_options(self, cursor, login):
        databases = self._lookup(
            cursor, "SELECT name FROM sys.databases WHERE state = 0 "
                    "AND name NOT IN ('master','tempdb','model','msdb') "
                    "ORDER BY name")
        return [
            {"key": "database", "label": "Database",
             "type": "choice" if databases else "string",
             "default": databases[0] if databases else "",
             "choices": [[d, d] for d in databases] if databases else None,
             "required": True},
            {"key": "username", "label": "User Name", "type": "string",
             "default": login,
             "help": "defaults to the login name"},
        ]

    # --- Object-level permissions ---

    supports_object_permissions = True
    supports_deny = True

    # Logins are server-wide; a login's rights inside a database belong
    # to the user it maps to, which is a level down.
    permission_root_label = "at server level"

    # Permission names per scope.  Deliberately the ones worth granting
    # by hand rather than everything sys.fn_builtin_permissions lists —
    # that is over two hundred names, most of which nobody grants.
    _SERVER_PERMISSIONS = [
        "CONNECT SQL", "VIEW SERVER STATE", "VIEW ANY DATABASE",
        "VIEW ANY DEFINITION", "ALTER ANY LOGIN", "ALTER ANY DATABASE",
        "ALTER ANY LINKED SERVER", "ALTER TRACE", "CREATE ANY DATABASE",
        "CONNECT ANY DATABASE", "IMPERSONATE ANY LOGIN", "CONTROL SERVER",
    ]
    _DATABASE_PERMISSIONS = [
        "CONNECT", "SELECT", "INSERT", "UPDATE", "DELETE", "EXECUTE",
        "REFERENCES", "VIEW DEFINITION", "ALTER", "CONTROL",
        "CREATE TABLE", "CREATE VIEW", "CREATE PROCEDURE",
        "CREATE FUNCTION", "CREATE SCHEMA", "BACKUP DATABASE",
        "ALTER ANY SCHEMA", "ALTER ANY USER", "ALTER ANY ROLE",
    ]
    _SCHEMA_PERMISSIONS = [
        "SELECT", "INSERT", "UPDATE", "DELETE", "EXECUTE", "REFERENCES",
        "VIEW DEFINITION", "ALTER", "CONTROL", "TAKE OWNERSHIP",
    ]
    _OBJECT_PERMISSIONS = [
        "SELECT", "INSERT", "UPDATE", "DELETE", "EXECUTE", "REFERENCES",
        "VIEW DEFINITION", "ALTER", "CONTROL", "TAKE OWNERSHIP",
    ]
    # Only these reach a single column.
    _COLUMN_PERMISSIONS = ["SELECT", "UPDATE", "REFERENCES"]

    def permission_scopes(self, database=None):
        if database is None:
            return [["server", "Server"]]
        return [["database", "Database"], ["schema", "Schema"],
                ["object", "Table, view or routine"],
                ["column", "Column of a table or view"]]

    def permission_choices(self, scope):
        return {
            "server": self._SERVER_PERMISSIONS,
            "database": self._DATABASE_PERMISSIONS,
            "schema": self._SCHEMA_PERMISSIONS,
            "object": self._OBJECT_PERMISSIONS,
            "column": self._COLUMN_PERMISSIONS,
        }.get(scope, [])

    def securable_choices(self, cursor, scope, database=None):
        if scope in ("server", "database"):
            return []
        db = self.quote_ddl_identifier(database) if database else ""
        prefix = f"{db}." if db else ""
        if scope == "schema":
            sql = (f"SELECT name FROM {prefix}sys.schemas "
                   f"WHERE name NOT IN ('sys','INFORMATION_SCHEMA') "
                   f"ORDER BY name")
        else:
            # Tables, views and the routines worth granting EXECUTE on.
            sql = (f"SELECT SCHEMA_NAME(o.schema_id) + '.' + o.name "
                   f"FROM {prefix}sys.objects o "
                   f"WHERE o.type IN ('U','V','P','FN','IF','TF') "
                   f"AND o.is_ms_shipped = 0 "
                   f"ORDER BY SCHEMA_NAME(o.schema_id), o.name")
        try:
            cursor.execute(sql)
            return [r[0] for r in cursor.fetchall()]
        except Exception:
            return []

    # Class numbers used by sys.database_permissions / server_permissions.
    _CLASS_DATABASE = 0
    _CLASS_OBJECT = 1
    _CLASS_SCHEMA = 3

    def list_permissions(self, cursor, principal, database=None):
        """Return the permissions PRINCIPAL holds, at server or in a database."""
        self.validate_identifier(principal)
        headers = ["State", "Permission", "Scope", "Securable", "Column"]
        if database is None:
            sql = ("SELECT pe.state_desc, pe.permission_name, 'Server', "
                   "       @@SERVERNAME, ''"
                   "  FROM sys.server_permissions pe"
                   "  JOIN sys.server_principals pr"
                   "    ON pr.principal_id = pe.grantee_principal_id"
                   " WHERE pr.name = ?"
                   " ORDER BY pe.permission_name")
            params = (principal,)
        else:
            self.validate_identifier(database)
            db = self.quote_ddl_identifier(database)
            # Every catalog view here is database-scoped, so each one has
            # to be read inside the target database rather than whichever
            # the session happens to sit in.
            sql = (
                f"SELECT pe.state_desc, pe.permission_name,"
                f"       CASE pe.class WHEN 0 THEN 'Database'"
                f"                     WHEN 1 THEN 'Object'"
                f"                     WHEN 3 THEN 'Schema'"
                f"                     ELSE pe.class_desc END,"
                f"       CASE pe.class"
                f"         WHEN 0 THEN ?"
                f"         WHEN 1 THEN SCHEMA_NAME(o.schema_id) + '.' + o.name"
                f"         WHEN 3 THEN s.name"
                f"         ELSE CAST(pe.major_id AS NVARCHAR(32)) END,"
                f"       ISNULL(c.name, '')"
                f"  FROM {db}.sys.database_permissions pe"
                f"  LEFT JOIN {db}.sys.objects o"
                f"    ON o.object_id = pe.major_id AND pe.class = 1"
                f"  LEFT JOIN {db}.sys.columns c"
                f"    ON c.object_id = pe.major_id"
                f"   AND c.column_id = pe.minor_id AND pe.minor_id > 0"
                f"  LEFT JOIN {db}.sys.schemas s"
                f"    ON s.schema_id = pe.major_id AND pe.class = 3"
                f"  JOIN {db}.sys.database_principals pr"
                f"    ON pr.principal_id = pe.grantee_principal_id"
                f" WHERE pr.name = ?"
                f" ORDER BY pe.class, 4, pe.permission_name")
            params = (database, principal)
        cursor.execute(sql, params)
        return headers, [[str(c) if c is not None else "" for c in row]
                         for row in cursor.fetchall()]

    def _securable_clause(self, scope, securable, column=None):
        """Return the ON ... fragment for a scope, or '' where there is none."""
        if scope in ("server", "database"):
            return ""
        if scope == "schema":
            return f" ON SCHEMA::{self.quote_ddl_identifier(securable)}"
        parts = securable.split(".", 1)
        if len(parts) == 2:
            obj = (f"{self.quote_ddl_identifier(parts[0])}."
                   f"{self.quote_ddl_identifier(parts[1])}")
        else:
            obj = self.quote_ddl_identifier(securable)
        return f" ON OBJECT::{obj}"

    def _permission_statement(self, verb, principal, scope, securable,
                              permission, column=None):
        self.validate_identifier(principal)
        if permission not in self.permission_choices(scope):
            raise ValueError(f"{permission} is not a permission that can be "
                             f"granted at the {scope} level")
        target = self.quote_ddl_identifier(principal)
        cols = ""
        if scope == "column":
            self.validate_identifier(column)
            cols = f" ({self.quote_ddl_identifier(column)})"
        # REVOKE takes the principal FROM, GRANT and DENY take it TO.
        preposition = "FROM" if verb == "REVOKE" else "TO"
        return (f"{verb} {permission}{cols}"
                f"{self._securable_clause(scope, securable, column)} "
                f"{preposition} {target}")

    def sql_set_permissions(self, principal, scope, securable, granted,
                            denied, current, database=None, column=None):
        if scope not in (s[0] for s in self.permission_scopes(database)):
            raise ValueError(f"{scope} is not a scope in this context")
        if scope not in ("server", "database"):
            self.validate_identifier(securable.split(".")[-1])
        wanted = {}
        for name in granted or []:
            wanted[name] = "GRANT"
        # DENY outranks GRANT, so a permission named in both is denied.
        for name in denied or []:
            wanted[name] = "DENY"

        stmts = []
        for name in sorted(set(current or {}) - set(wanted)):
            stmts.append(self._permission_statement(
                "REVOKE", principal, scope, securable, name, column))
        for name in sorted(wanted):
            if (current or {}).get(name) == wanted[name]:
                continue
            # Switching between GRANT and DENY needs the old one gone
            # first, or the two rows sit side by side.
            if name in (current or {}):
                stmts.append(self._permission_statement(
                    "REVOKE", principal, scope, securable, name, column))
            stmts.append(self._permission_statement(
                "GRANT" if wanted[name] == "GRANT" else "DENY",
                principal, scope, securable, name, column))
        if database is not None:
            stmts = [self._in_database(database, st) for st in stmts]
        return stmts

    def sql_revoke_permission(self, principal, scope, securable, permission,
                              database=None, column=None):
        stmt = self._permission_statement(
            "REVOKE", principal, scope, securable, permission, column)
        return [self._in_database(database, stmt) if database is not None
                else stmt]

    def _in_database(self, database, statement):
        """Wrap STATEMENT so it runs inside DATABASE.

        CREATE USER and ALTER ROLE only act on the current database, and a
        USE would move the shared session.
        """
        return (f"EXEC {self.quote_ddl_identifier(database)}..sp_executesql "
                f"N{self.quote_ddl_literal(statement)}")

    def sql_add_user_mapping(self, login, opts):
        database = (opts.get("database") or "").strip()
        username = (opts.get("username") or login).strip()
        self.validate_identifier(database)
        inner = (f"CREATE USER {self.quote_ddl_identifier(username)} "
                 f"FOR LOGIN {self.quote_ddl_identifier(login)}")
        stmts = [self._in_database(database, inner)]
        for role in opts.get("roles") or []:
            stmts.append(self._in_database(
                database,
                f"ALTER ROLE {self.quote_ddl_identifier(role)} "
                f"ADD MEMBER {self.quote_ddl_identifier(username)}"))
        return stmts

    def sql_remove_user_mapping(self, login, database):
        self.validate_identifier(database)
        return [self._in_database(
            database, f"DROP USER {self.quote_ddl_identifier(login)}")]

    # --- Schemas and tables ---

    supports_schema_ddl = True

    def column_types(self):
        return [[t, t] for t in (
            "INT", "INT IDENTITY(1,1)", "BIGINT", "BIGINT IDENTITY(1,1)",
            "SMALLINT", "TINYINT", "BIT",
            "DECIMAL(18,2)", "MONEY", "FLOAT", "REAL",
            "DATE", "DATETIME2", "DATETIMEOFFSET", "TIME",
            "NVARCHAR(50)", "NVARCHAR(255)", "NVARCHAR(MAX)",
            "VARCHAR(50)", "VARCHAR(255)", "VARCHAR(MAX)",
            "CHAR(1)", "UNIQUEIDENTIFIER", "VARBINARY(MAX)", "XML")]

    def list_schemas_detail(self, cursor):
        cursor.execute("""
            SELECT s.name,
                   ISNULL(p.name, ''),
                   CAST(COUNT(t.object_id) AS VARCHAR)
            FROM sys.schemas s
            LEFT JOIN sys.database_principals p
              ON p.principal_id = s.principal_id
            LEFT JOIN sys.tables t ON t.schema_id = s.schema_id
            WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
              AND s.name NOT LIKE 'db[_]%'
            GROUP BY s.name, p.name
            ORDER BY s.name
        """)
        return (["Schema", "Owner", "Tables"],
                [[str(v) if v is not None else "" for v in row]
                 for row in cursor.fetchall()])

    def schema_options(self, cursor, current=None):
        owners = self._lookup(
            cursor, "SELECT name FROM sys.database_principals "
                    "WHERE type IN ('S','U','G','R') AND principal_id > 0 "
                    "ORDER BY name")
        return [
            {"key": "name", "label": "Schema Name", "type": "string",
             "default": "", "required": True},
            {"key": "owner", "label": "Owner",
             "type": "choice" if owners else "string",
             "default": "dbo",
             "choices": [[o, o] for o in owners] if owners else None},
        ]

    def sql_create_schema(self, opts):
        name = self.quote_ddl_identifier(opts.get("name", ""))
        owner = (opts.get("owner") or "").strip()
        sql = f"CREATE SCHEMA {name}"
        if owner:
            sql += f" AUTHORIZATION {self.quote_ddl_identifier(owner)}"
        return [sql]

    def sql_drop_schema(self, name, cascade=False):
        # SQL Server has no CASCADE: a schema holding objects must be
        # emptied first, which is the user's decision to make.
        return [f"DROP SCHEMA {self.quote_ddl_identifier(name)}"]

    def list_tables_detail(self, cursor, schema):
        cursor.execute("""
            SELECT t.name,
                   CAST(ISNULL(SUM(p.rows), 0) AS VARCHAR),
                   CAST(CAST(ISNULL(SUM(a.total_pages), 0) * 8 / 1024.0
                             AS DECIMAL(18,1)) AS VARCHAR),
                   CONVERT(VARCHAR(19), t.create_date, 120)
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            LEFT JOIN sys.partitions p
              ON p.object_id = t.object_id AND p.index_id IN (0, 1)
            LEFT JOIN sys.allocation_units a
              ON a.container_id = p.partition_id
            WHERE s.name = ?
            GROUP BY t.name, t.create_date
            ORDER BY t.name
        """, [schema])
        return (["Table", "Rows", "Size MB", "Created"],
                [[str(v) if v is not None else "" for v in row]
                 for row in cursor.fetchall()])

    def table_options(self, cursor, schema):
        return [
            {"key": "name", "label": "Table Name", "type": "string",
             "default": "", "required": True},
            {"key": "columns", "label": "Columns", "type": "list",
             "default": [["id", "INT IDENTITY(1,1)", False, True, ""]],
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
                f"CONSTRAINT {self.quote_ddl_identifier('PK_' + name)} "
                f"PRIMARY KEY ({keys})")
        return [f"CREATE TABLE {qualified} (\n  "
                + ",\n  ".join(clauses) + "\n)"]

    def sql_drop_table(self, schema, name):
        return [f"DROP TABLE {self.quote_ddl_identifier(schema)}."
                f"{self.quote_ddl_identifier(name)}"]

    def list_table_columns(self, cursor, schema, table):
        cursor.execute("""
            SELECT c.name, t.name, c.max_length, c.precision, c.scale,
                   c.is_nullable, c.is_identity,
                   ISNULL(dc.definition, ''),
                   CASE WHEN EXISTS (
                        SELECT 1 FROM sys.index_columns ic
                        JOIN sys.indexes i
                          ON i.object_id = ic.object_id
                         AND i.index_id = ic.index_id
                        WHERE ic.object_id = c.object_id
                          AND ic.column_id = c.column_id
                          AND i.is_primary_key = 1) THEN 1 ELSE 0 END
            FROM sys.columns c
            JOIN sys.types t ON t.user_type_id = c.user_type_id
            LEFT JOIN sys.default_constraints dc
              ON dc.object_id = c.default_object_id
            WHERE c.object_id = OBJECT_ID(?)
            ORDER BY c.column_id
        """, [f"{schema}.{table}"])
        rows = []
        for (name, base, length, precision, scale, nullable, identity,
             default, is_key) in cursor.fetchall():
            rows.append([name,
                         self._format_type(base, length, precision, scale,
                                           identity),
                         bool(nullable), bool(is_key),
                         self._unwrap_default(default)])
        return rows

    @staticmethod
    def _unwrap_default(definition):
        """Strip the parentheses SQL Server wraps a default in.

        A default stored as ((7)) or (getdate()) has to come back as 7 or
        getdate(), or every column would look changed.
        """
        text = (definition or "").strip()
        while text.startswith("(") and text.endswith(")"):
            inner = text[1:-1].strip()
            # Only unwrap a genuinely enclosing pair.
            depth = 0
            for index, ch in enumerate(inner):
                depth += (ch == "(") - (ch == ")")
                if depth < 0:
                    return text
            if depth != 0:
                return text
            text = inner
        return text

    def _format_type(self, base, length, precision, scale, identity=False):
        """Render a column's type in the vocabulary `column_types` uses."""
        base = base.upper()
        if base in ("NVARCHAR", "NCHAR"):
            size = "MAX" if length == -1 else str(length // 2)
            rendered = f"{base}({size})"
        elif base in ("VARCHAR", "CHAR", "VARBINARY", "BINARY"):
            size = "MAX" if length == -1 else str(length)
            rendered = f"{base}({size})"
        elif base in ("DECIMAL", "NUMERIC"):
            rendered = f"{base}({precision},{scale})"
        else:
            rendered = base
        if identity:
            rendered += " IDENTITY(1,1)"
        return rendered

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
            clause = (f"{self.quote_ddl_identifier(name)} {sql_type}"
                      f"{'' if bool(row[2]) else ' NOT NULL'}")
            default = str(row[4] or "").strip() if len(row) > 4 else ""
            if default:
                clause += f" DEFAULT {self.validate_default(default)}"
            stmts.append(f"ALTER TABLE {qualified} ADD {clause}")

        for before, row in changed:
            name = self.validate_identifier(str(row[0]).strip())
            column = self.quote_ddl_identifier(name)
            sql_type = self.validate_column_type(name, row[1])
            # Type and nullability change together in one statement here,
            # unlike PostgreSQL where they are separate.
            if (str(before[1]) != str(row[1])
                    or bool(before[2]) != bool(row[2])):
                stmts.append(
                    f"ALTER TABLE {qualified} ALTER COLUMN {column} "
                    f"{sql_type}{'' if bool(row[2]) else ' NOT NULL'}")
            old_default = str(before[4] or "").strip()
            new_default = str(row[4] or "").strip() if len(row) > 4 else ""
            if old_default != new_default:
                if old_default:
                    stmts.append(self._drop_default_sql(schema, table, name))
                if new_default:
                    stmts.append(
                        f"ALTER TABLE {qualified} ADD DEFAULT "
                        f"{self.validate_default(new_default)} FOR {column}")

        for row in dropped:
            name = self.validate_identifier(str(row[0]).strip())
            # A column with a default cannot be dropped while its
            # constraint stands, and the constraint's name is generated.
            if str(row[4] or "").strip():
                stmts.append(self._drop_default_sql(schema, table, name))
            stmts.append(f"ALTER TABLE {qualified} DROP COLUMN "
                         f"{self.quote_ddl_identifier(name)}")
        return stmts

    def sql_rename_column(self, schema, table, old, new):
        # SQL Server renames through sp_rename rather than ALTER TABLE.
        target = f"{self.validate_identifier(schema)}." \
                 f"{self.validate_identifier(table)}." \
                 f"{self.validate_identifier(old)}"
        return [f"EXEC sp_rename {self.quote_ddl_literal(target)}, "
                f"{self.quote_ddl_literal(self.validate_identifier(new))}, "
                f"'COLUMN'"]

    def _drop_default_sql(self, schema, table, column):
        """Drop a column's default by looking its constraint name up.

        SQL Server generates the name, so it has to be found at run time
        rather than assumed.
        """
        target = self.quote_ddl_literal(f"{schema}.{table}")
        return (
            "DECLARE @c SYSNAME; "
            "SELECT @c = dc.name FROM sys.default_constraints dc "
            "JOIN sys.columns col ON col.default_object_id = dc.object_id "
            f"WHERE dc.parent_object_id = OBJECT_ID({target}) "
            f"AND col.name = {self.quote_ddl_literal(column)}; "
            "IF @c IS NOT NULL EXEC('ALTER TABLE "
            f"{schema}.{table} DROP CONSTRAINT [' + @c + ']')")

    # --- Backup and restore ---

    supports_backup = True

    _BACKUP_TYPES = {"D": "Full", "I": "Differential", "L": "Log",
                     "F": "File", "G": "File Diff", "P": "Partial",
                     "Q": "Partial Diff"}

    def backup_history(self, cursor, database):
        cursor.execute("""
            SELECT TOP 50
                   bs.type,
                   CONVERT(VARCHAR(19), bs.backup_finish_date, 120),
                   CAST(CAST(bs.backup_size / 1048576.0 AS DECIMAL(18,1))
                        AS VARCHAR),
                   bs.user_name,
                   bmf.physical_device_name,
                   CAST(bs.position AS VARCHAR)
            FROM msdb.dbo.backupset bs
            JOIN msdb.dbo.backupmediafamily bmf
              ON bmf.media_set_id = bs.media_set_id
            WHERE bs.database_name = ?
            ORDER BY bs.backup_finish_date DESC
        """, [database])
        headers = ["Type", "Finished", "Size MB", "By", "Device", "Pos"]
        rows = []
        for row in cursor.fetchall():
            rows.append([self._BACKUP_TYPES.get(row[0], row[0] or ""),
                         row[1] or "", row[2] or "", row[3] or "",
                         row[4] or "", row[5] or ""])
        return headers, rows

    def backup_options(self, cursor, database):
        defaults = self.default_paths(cursor)
        backup_dir = defaults.get("data", "")
        try:
            cursor.execute(
                "SELECT CAST(SERVERPROPERTY('InstanceDefaultBackupPath') "
                "AS NVARCHAR(4000))")
            row = cursor.fetchone()
            if row and row[0]:
                backup_dir = row[0].rstrip("/\\")
        except Exception:
            pass
        return [
            {"key": "backup_type", "label": "Backup Type", "type": "choice",
             "default": "FULL",
             "choices": [["FULL", "Full"], ["DIFFERENTIAL", "Differential"],
                         ["LOG", "Transaction log"]]},
            {"key": "directory", "label": "Directory", "type": "path",
             "default": backup_dir, "required": True},
            {"key": "filename", "label": "File Name", "type": "string",
             "default": f"{database}.bak", "required": True},
            {"key": "overwrite", "label": "Overwrite the file", "type": "bool",
             "default": False,
             "help": "off appends this backup to the existing file"},
            {"key": "compression", "label": "Compress", "type": "bool",
             "default": True},
            {"key": "checksum", "label": "Checksum", "type": "bool",
             "default": True},
            {"key": "copy_only", "label": "Copy Only", "type": "bool",
             "default": False,
             "help": "does not disturb the normal backup sequence"},
            {"key": "verify", "label": "Verify Afterwards", "type": "bool",
             "default": False},
        ]

    def sql_backup(self, database, opts):
        db = self.quote_ddl_identifier(database)
        directory = (opts.get("directory") or "").strip()
        filename = (opts.get("filename") or "").strip()
        if not (directory and filename):
            raise ValueError("a directory and file name are required")
        # The file name lands in a string literal, but a path separator
        # in it would silently write somewhere else.
        if "/" in filename or "\\" in filename:
            raise ValueError(
                f"the file name may not contain a path separator: {filename}")
        target = self.join_path(directory, filename)
        device = self.quote_ddl_literal(target)

        backup_type = (opts.get("backup_type") or "FULL").upper()
        if backup_type not in ("FULL", "DIFFERENTIAL", "LOG"):
            raise ValueError(f"Unknown backup type: {backup_type}")

        clauses = ["INIT" if opts.get("overwrite") else "NOINIT"]
        if backup_type == "DIFFERENTIAL":
            clauses.insert(0, "DIFFERENTIAL")
        if opts.get("compression"):
            clauses.append("COMPRESSION")
        if opts.get("checksum"):
            clauses.append("CHECKSUM")
        if opts.get("copy_only"):
            clauses.append("COPY_ONLY")

        verb = "BACKUP LOG" if backup_type == "LOG" else "BACKUP DATABASE"
        stmts = [f"{verb} {db} TO DISK = {device} WITH "
                 + ", ".join(clauses)]
        if opts.get("verify"):
            stmts.append(f"RESTORE VERIFYONLY FROM DISK = {device}")
        return stmts

    def backup_contents(self, cursor, path):
        """Return the backup sets in PATH, newest position last."""
        cursor.execute(
            f"RESTORE HEADERONLY FROM DISK = {self.quote_ddl_literal(path)}")
        columns = {d[0]: i for i, d in enumerate(cursor.description)}
        rows = cursor.fetchall()
        self._drain(cursor)
        sets = []
        for row in rows:
            def value(name):
                index = columns.get(name)
                return row[index] if index is not None else None
            sets.append({
                "position": int(value("Position") or 1),
                "type": {1: "Full", 2: "Log", 5: "Differential",
                         4: "File", 6: "File Diff",
                         7: "Partial"}.get(value("BackupType"),
                                           str(value("BackupType"))),
                "database": value("DatabaseName") or "",
                "finished": str(value("BackupFinishDate") or ""),
            })
        return sets

    def backup_file_list(self, cursor, path, position=1):
        cursor.execute(
            f"RESTORE FILELISTONLY FROM DISK = "
            f"{self.quote_ddl_literal(path)} WITH FILE = {int(position)}")
        columns = {d[0]: i for i, d in enumerate(cursor.description)}
        rows = cursor.fetchall()
        self._drain(cursor)
        files = []
        for row in rows:
            files.append({
                "logical": row[columns["LogicalName"]],
                "physical": row[columns["PhysicalName"]],
                "type": row[columns["Type"]],
            })
        return files

    @staticmethod
    def _drain(cursor):
        """Consume the informational result sets BACKUP/RESTORE emit."""
        try:
            while cursor.nextset():
                pass
        except Exception:
            pass

    def restore_options(self, cursor, database):
        defaults = self.default_paths(cursor)
        return [
            {"key": "source", "label": "Backup File", "type": "path",
             "default": "", "required": True,
             "help": "the .bak file to restore from"},
            {"key": "position", "label": "Backup Set", "type": "int",
             "default": 1,
             "help": "position within the file; 1 is the first"},
            {"key": "target", "label": "Restore As", "type": "string",
             "default": database, "required": True,
             "help": "an existing database is overwritten"},
            {"key": "data_dir", "label": "Data File Directory", "type": "path",
             "default": defaults.get("data", ""),
             "help": "where the restored data files are placed"},
            {"key": "log_dir", "label": "Log File Directory", "type": "path",
             "default": defaults.get("log", ""),
             "help": "where the restored log files are placed"},
            {"key": "replace", "label": "Replace Existing", "type": "bool",
             "default": True},
            {"key": "recovery", "label": "Bring Online", "type": "bool",
             "default": True,
             "help": "off leaves it restoring, for further log restores"},
        ]

    def sql_restore(self, database, opts, file_list):
        target = self.quote_ddl_identifier(opts.get("target") or database)
        source = (opts.get("source") or "").strip()
        if not source:
            raise ValueError("a backup file is required")
        device = self.quote_ddl_literal(source)
        name = (opts.get("target") or database).strip()

        clauses = [f"FILE = {int(opts.get('position') or 1)}"]
        # Every logical file has to be placed explicitly, or the restore
        # tries to write over the paths recorded in the backup — which
        # belong to whichever database was backed up.
        data_dir = (opts.get("data_dir") or "").strip()
        log_dir = (opts.get("log_dir") or "").strip()
        for index, entry in enumerate(file_list or []):
            is_log = entry.get("type") == "L"
            directory = log_dir if is_log else data_dir
            if not directory:
                continue
            suffix = "_log.ldf" if is_log else (
                ".mdf" if index == 0 else f"_{index}.ndf")
            moved = self.join_path(directory, f"{name}{suffix}")
            clauses.append(
                f"MOVE {self.quote_ddl_literal(entry['logical'])} "
                f"TO {self.quote_ddl_literal(moved)}")
        if opts.get("replace"):
            clauses.append("REPLACE")
        clauses.append("RECOVERY" if opts.get("recovery", True)
                       else "NORECOVERY")

        stmts = []
        # A restore needs exclusive access, which an idle connection in
        # the target is enough to deny.  Guarded on existence because the
        # target is commonly a database being created by this restore,
        # and ALTER DATABASE on a missing one is an error.
        existing = self.quote_ddl_literal(name)
        if opts.get("replace"):
            stmts.append(f"IF DB_ID({existing}) IS NOT NULL "
                         f"ALTER DATABASE {target} SET SINGLE_USER "
                         f"WITH ROLLBACK IMMEDIATE")
        stmts.append(f"RESTORE DATABASE {target} FROM DISK = {device} WITH "
                     + ", ".join(clauses))
        if opts.get("replace") and opts.get("recovery", True):
            stmts.append(f"IF DB_ID({existing}) IS NOT NULL "
                         f"ALTER DATABASE {target} SET MULTI_USER")
        return stmts

    # --- Database file management ---

    supports_file_management = True

    def database_files(self, cursor, name):
        # sys.filegroups is scoped to the current database, so both views
        # are addressed inside the target.  Joining the server-wide
        # sys.master_files against the local sys.filegroups silently
        # mislabels every filegroup whose id happens to exist in master.
        db = self.quote_ddl_identifier(name)
        cursor.execute(f"""
            SELECT df.name, df.type_desc, fg.name,
                   df.size * 8 / 1024,
                   CASE WHEN df.is_percent_growth = 1 THEN df.growth
                        ELSE df.growth * 8 / 1024 END,
                   CASE WHEN df.is_percent_growth = 1 THEN '%' ELSE 'MB' END,
                   CASE WHEN df.max_size IN (-1, 268435456) THEN 0
                        ELSE df.max_size * 8 / 1024 END,
                   df.physical_name
            FROM {db}.sys.database_files df
            LEFT JOIN {db}.sys.filegroups fg
                ON fg.data_space_id = df.data_space_id
            ORDER BY df.type, df.file_id
        """)
        return [{"logical": r[0], "type": r[1], "filegroup": r[2] or "",
                 "size_mb": int(r[3] or 0), "growth": int(r[4] or 0),
                 "growth_unit": r[5], "max_mb": int(r[6] or 0), "path": r[7]}
                for r in cursor.fetchall()]

    def database_filegroups(self, cursor, name):
        db = self.quote_ddl_identifier(name)
        try:
            cursor.execute(f"SELECT name FROM {db}.sys.filegroups "
                           f"WHERE type = 'FG' ORDER BY name")
            return [r[0] for r in cursor.fetchall()]
        except Exception:
            return []

    def file_options(self, cursor, name, current=None):
        # Only the size limits can be altered on an existing file; its
        # name, path, type and filegroup are fixed once created.
        if current:
            return [
                {"key": "size_mb", "label": "Size", "type": "int",
                 "default": current.get("size_mb", 0),
                 "help": "MB; a file cannot be shrunk by lowering this"},
                {"key": "growth", "label": "Autogrowth", "type": "int",
                 "default": current.get("growth", 0),
                 "help": "0 disables autogrowth"},
                {"key": "growth_unit", "label": "Growth Unit", "type": "choice",
                 "default": current.get("growth_unit", "MB"),
                 "choices": [["MB", "Megabytes"], ["%", "Percent"]]},
                {"key": "max_mb", "label": "Max Size", "type": "int",
                 "default": current.get("max_mb", 0),
                 "help": "MB; 0 for unlimited"},
            ]
        filegroups = self.database_filegroups(cursor, name)
        defaults = self.default_paths(cursor)
        return [
            {"key": "logical", "label": "Logical Name", "type": "string",
             "default": "", "required": True},
            {"key": "file_type", "label": "File Type", "type": "choice",
             "default": "ROWS",
             "choices": [["ROWS", "Data"], ["LOG", "Log"]]},
            {"key": "filegroup", "label": "Filegroup", "type": "choice",
             "default": "PRIMARY",
             "choices": [[f, f] for f in filegroups] or [["PRIMARY", "PRIMARY"]],
             "help": "data files only; ignored for a log file"},
            {"key": "directory", "label": "Directory", "type": "path",
             "default": defaults.get("data", ""), "required": True},
            {"key": "size_mb", "label": "Initial Size", "type": "int",
             "default": 8, "help": "MB"},
            {"key": "growth", "label": "Autogrowth", "type": "int",
             "default": 64, "help": "0 disables autogrowth"},
            {"key": "growth_unit", "label": "Growth Unit", "type": "choice",
             "default": "MB",
             "choices": [["MB", "Megabytes"], ["%", "Percent"]]},
            {"key": "max_mb", "label": "Max Size", "type": "int",
             "default": 0, "help": "MB; 0 for unlimited"},
        ]

    def _file_spec(self, logical, path, opts, include_name_only=False):
        """Build the (NAME = ..., ...) spec shared by ADD and MODIFY."""
        parts = [f"NAME = {self.quote_ddl_identifier(logical)}"]
        if path:
            parts.append(f"FILENAME = {self.quote_ddl_literal(path)}")
        if opts.get("size_mb"):
            parts.append(f"SIZE = {int(opts['size_mb'])}MB")
        max_mb = opts.get("max_mb")
        parts.append(f"MAXSIZE = {int(max_mb)}MB" if max_mb
                     else "MAXSIZE = UNLIMITED")
        growth = opts.get("growth")
        if growth:
            unit = "%" if opts.get("growth_unit") == "%" else "MB"
            parts.append(f"FILEGROWTH = {int(growth)}{unit}")
        else:
            parts.append("FILEGROWTH = 0")
        return "(" + ", ".join(parts) + ")"

    def sql_add_file(self, name, opts):
        db = self.quote_ddl_identifier(name)
        logical = self.validate_identifier(opts.get("logical", ""))
        directory = (opts.get("directory") or "").strip()
        if not directory:
            raise ValueError("a directory is required for the new file")
        is_log = opts.get("file_type") == "LOG"
        suffix = ".ldf" if is_log else ".ndf"
        path = self.join_path(directory, f"{logical}{suffix}")
        spec = self._file_spec(logical, path, opts)
        if is_log:
            return [f"ALTER DATABASE {db} ADD LOG FILE {spec}"]
        filegroup = (opts.get("filegroup") or "").strip()
        sql = f"ALTER DATABASE {db} ADD FILE {spec}"
        if filegroup:
            sql += f" TO FILEGROUP {self.quote_ddl_identifier(filegroup)}"
        return [sql]

    def sql_modify_file(self, name, opts, current):
        db = self.quote_ddl_identifier(name)
        logical = current["logical"]
        if all(opts.get(k) == current.get(k)
               for k in ("size_mb", "growth", "growth_unit", "max_mb")):
            return []
        return [f"ALTER DATABASE {db} MODIFY FILE "
                f"{self._file_spec(logical, None, opts)}"]

    def sql_remove_file(self, name, logical, empty_first=True):
        self.validate_identifier(name)
        self.validate_identifier(logical)
        stmts = []
        if empty_first:
            # REMOVE FILE fails while the file still holds pages, and a
            # plain shrink does not empty one.  EMPTYFILE migrates the
            # pages to the filegroup's other files first.
            stmts.append(self._in_database(
                name, f"DBCC SHRINKFILE ("
                      f"{self.quote_ddl_literal(logical)}, EMPTYFILE)"))
        stmts.append(f"ALTER DATABASE {self.quote_ddl_identifier(name)} "
                     f"REMOVE FILE {self.quote_ddl_identifier(logical)}")
        return stmts

    def sql_shrink_file(self, name, logical, target_mb):
        # DBCC SHRINKFILE only acts on the current database, so it is run
        # through sp_executesql in the target's own context rather than
        # issuing a USE that would move the shared session.
        self.validate_identifier(name)
        self.validate_identifier(logical)
        inner = (f"DBCC SHRINKFILE ("
                 f"{self.quote_ddl_literal(logical)}, {int(target_mb)})")
        return [f"EXEC {self.quote_ddl_identifier(name)}..sp_executesql "
                f"N{self.quote_ddl_literal(inner)}"]

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
             "choices": [[n, n] for n in logins] if logins else None,
             "help": "the owner becomes dbo here, with full control"},
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
