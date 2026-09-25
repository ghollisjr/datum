"""Abstract base class for SQL dialect drivers.

Each driver provides:
  - Introspection queries (databases, schemas, tables, columns, running queries,
    current user, server version)
  - Type mapping from Python/pyarrow types to SQL DDL types for :in imports

Concrete implementations live in mssql.py and postgres.py.
The fallback AnsiDriver uses INFORMATION_SCHEMA where possible, which is
supported by most modern databases, but some queries may not work on all
platforms.
"""

import re

from abc import ABC, abstractmethod


class BaseDriver(ABC):
    """Abstract driver. All methods return strings (SQL) or lists/dicts."""

    # Human-readable name shown in the Emacs mode line.
    dialect_name = "unknown"

    # Default schema for completion: tables in this schema get bare names.
    # "dbo" for MSSQL, "public" for Postgres, None for MySQL (all bare).
    default_schema = "public"

    # --- Introspection SQL ---
    # Each property returns a SQL string that can be executed directly.
    # Results are always expected as a single-column list unless noted.

    @property
    @abstractmethod
    def sql_list_databases(self):
        """SQL to list all databases on the server. Returns: [(name,)]"""

    @property
    @abstractmethod
    def sql_current_database(self):
        """SQL to get the current database name. Returns: [(name,)]"""

    @property
    @abstractmethod
    def sql_list_schemas(self):
        """SQL to list schemas in the current database. Returns: [(name,)]"""

    @property
    @abstractmethod
    def sql_current_schema(self):
        """SQL to get the current/default schema. Returns: [(name,)]"""

    @property
    @abstractmethod
    def sql_list_tables(self):
        """SQL to list tables in the current database.
        Returns: [(schema, table_name, table_type)]
        table_type is 'TABLE' or 'VIEW'
        """

    @property
    @abstractmethod
    def sql_current_user(self):
        """SQL to get the current user. Returns: [(name,)]"""

    @property
    @abstractmethod
    def sql_server_version(self):
        """SQL to get the server version string. Returns: [(version,)]"""

    @property
    @abstractmethod
    def sql_running_queries(self):
        """SQL to list currently running queries.
        Returns: [(session_id, user, status, duration, sql_text)]
        """

    def sql_list_databases_like(self, pattern):
        """Return (sql, params) for a filtered database list using LIKE."""
        raise NotImplementedError

    def sql_list_schemas_like(self, pattern):
        """Return (sql, params) for a filtered schema list using LIKE."""
        raise NotImplementedError

    def sql_list_tables_like(self, pattern):
        """Return (sql, params) for a filtered table list using LIKE."""
        raise NotImplementedError

    @property
    def sql_list_routines(self):
        """SQL to list routines (procedures and functions).
        Returns: [(schema, routine_name, routine_type)]
        routine_type is 'PROCEDURE' or 'FUNCTION'
        """
        raise NotImplementedError

    def sql_list_routines_like(self, pattern):
        """Return (sql, params) for a filtered routine list using LIKE."""
        raise NotImplementedError

    @property
    def sql_routine_signatures(self):
        """SQL to list routine parameter signatures.
        Returns: [(schema, routine_name, signature_string)]
        One row per routine; signature_string is a formatted parameter list.
        """
        raise NotImplementedError

    def sql_list_columns(self, schema, table, database=None):
        """Return (sql, params) to list columns for a given table.
        Returns: [(column_name, data_type, is_nullable, column_default)]
        Default implementation uses INFORMATION_SCHEMA, works on most platforms.
        DATABASE is accepted but ignored in the base implementation;
        dialect drivers can override to support cross-database queries.
        """
        return ("""
            SELECT COLUMN_NAME,
                   DATA_TYPE,
                   IS_NULLABLE,
                   COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE LOWER(TABLE_SCHEMA) = LOWER(?)
              AND LOWER(TABLE_NAME)   = LOWER(?)
            ORDER BY ORDINAL_POSITION
        """, [schema, table])

    # --- Definition lookup ---

    def sql_resolve_object_type(self, schema, name, database=None):
        """Return (sql, params) to resolve object type.
        Expected result: single row (object_type,) where object_type is
        TABLE, VIEW, PROCEDURE, or FUNCTION.
        database is used for cross-database lookups (MSSQL).
        """
        raise NotImplementedError

    def sql_get_definition(self, schema, name, object_type, database=None):
        """Return (sql, params) to get the definition/source of an object.
        For TABLE: returns column rows (name, type, nullable, default).
        For VIEW/PROCEDURE/FUNCTION: returns a single row (definition_text,).
        database is used for cross-database lookups (MSSQL).
        """
        raise NotImplementedError

    def sql_check_database(self, name):
        """Return (sql, params) to check if name is a database.
        Returns properties rows if it exists.
        """
        raise NotImplementedError

    def sql_check_schema(self, name):
        """Return (sql, params) to check if name is a schema.
        Returns properties rows if it exists.
        """
        raise NotImplementedError

    # --- Identifier quoting ---

    def quote_identifier(self, name):
        """Quote a SQL identifier using the dialect's quoting style.

        Default uses ANSI double-quoting.  Subclasses override for
        dialect-specific quoting (e.g. square brackets for MSSQL,
        backticks for MySQL).
        """
        return f'"{name}"'

    # --- DDL identifier / literal quoting ---
    #
    # DDL cannot use bound parameters for identifiers, so admin wizards
    # interpolate names directly into SQL.  These helpers validate and
    # escape, and must be used for every user-supplied value that lands
    # in generated DDL.

    def validate_identifier(self, name):
        """Validate NAME as a SQL identifier, returning it unchanged.

        Raises ValueError if the name could not be safely embedded in
        generated DDL.  Escaping is handled by `quote_ddl_identifier`;
        this rejects the cases escaping cannot make safe.
        """
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Identifier must be a non-empty string")
        name = name.strip()
        if len(name) > 128:
            raise ValueError(
                f"Identifier too long ({len(name)} chars, max 128): {name[:40]}...")
        for ch in name:
            if ord(ch) < 32 or ord(ch) == 127:
                raise ValueError(
                    f"Identifier contains a control character: {name!r}")
        return name

    def quote_ddl_identifier(self, name):
        """Validate and quote NAME for interpolation into generated DDL.

        Unlike `quote_identifier`, this escapes the closing quote
        character so that names containing it cannot break out of the
        quoted identifier.
        """
        name = self.validate_identifier(name)
        return '"' + name.replace('"', '""') + '"'

    def quote_ddl_literal(self, value):
        """Quote VALUE as a SQL string literal for generated DDL."""
        if value is None:
            return "NULL"
        text = str(value)
        for ch in text:
            if ord(ch) == 0:
                raise ValueError("String literal contains a NUL byte")
        return "'" + text.replace("'", "''") + "'"

    # --- Admin wizard capabilities ---
    #
    # Drivers opt in to each wizard family.  Panels check these flags and
    # report a clear "not supported on <dialect>" instead of emitting SQL
    # that cannot work on the target engine.

    supports_database_ddl = False

    def database_options(self, cursor=None):
        """Return the field descriptors for the CREATE DATABASE form.

        Each descriptor is a dict with keys:
          key         — identifier used in the submitted value map
          label       — form label
          type        — one of: string, int, bool, choice, completing,
                        text, path
          default     — initial value
          choices     — [[value, label], ...] when type is "choice"
          completions — [value, ...] when type is "completing"
          help        — trailing hint shown after the field
          required    — whether the field must be non-empty

        When CURSOR is given, lookup fields are populated from the live
        server so the form offers the owners, templates and collations
        that actually exist there.  Without a cursor the static
        descriptors are returned, which is what validation needs.
        """
        return []

    def _lookup(self, cursor, sql, limit=6000):
        """Run a lookup query, returning a list of names.

        Lookup failures are not fatal: a form with a plain text field is
        better than no form, so the field silently loses its completion
        rather than the whole wizard erroring out.
        """
        if cursor is None:
            return []
        try:
            cursor.execute(sql)
            return [row[0] for row in cursor.fetchmany(limit)
                    if row[0] is not None]
        except Exception:
            return []

    def sql_create_database(self, opts):
        """Return a list of SQL statements creating a database.

        opts is the submitted value map keyed by descriptor `key`.
        """
        raise NotImplementedError(
            f"CREATE DATABASE is not supported on {self.dialect_name}")

    def sql_drop_database(self, name, force=False):
        """Return a list of SQL statements dropping a database.

        When force is true, the statements also evict active sessions.
        """
        raise NotImplementedError(
            f"DROP DATABASE is not supported on {self.dialect_name}")

    # --- Altering an existing database ---

    supports_database_alter = False

    def database_settings(self, cursor, name):
        """Return the current settings of database NAME as a value map.

        Keys match the descriptors returned by `settings_options`, so the
        alter form can be shown already filled in with what is in force.
        """
        return {}

    def settings_options(self, cursor, current):
        """Return the field descriptors for the database settings form.

        CURRENT is the map from `database_settings`; descriptors default
        to those values so an untouched field means "leave as is".
        """
        return []

    def sql_alter_database(self, name, opts, current):
        """Return ALTER statements for the settings that actually changed.

        Comparing against CURRENT keeps an unchanged form from issuing
        statements that would take locks or fail for no reason.
        """
        raise NotImplementedError(
            f"ALTER DATABASE is not supported on {self.dialect_name}")

    def sql_database_sessions(self, name):
        """Return (sql, params) counting active sessions on a database."""
        raise NotImplementedError(
            f"Session inspection is not supported on {self.dialect_name}")

    def safe_fallback_database(self):
        """Return a database that is safe to connect to while dropping another."""
        return None

    # --- Logins, users and roles ---
    #
    # MSSQL separates server-level logins from per-database users;
    # PostgreSQL has a single role that may or may not be able to log in.
    # "Principal" is the term used here for whichever the dialect has.

    supports_security = False

    # Shown as the panel heading and in messages.
    principal_noun = "principal"

    def list_principals(self, cursor):
        """Return (headers, rows) describing the server's principals."""
        return [], []

    def principal_settings(self, cursor, name):
        """Return the current settings of principal NAME as a value map."""
        return {}

    def principal_options(self, cursor, current=None):
        """Field descriptors for creating, or editing when CURRENT is given."""
        return []

    def sql_create_principal(self, opts):
        """Return statements creating a principal."""
        raise NotImplementedError(
            f"Managing logins is not supported on {self.dialect_name}")

    def sql_alter_principal(self, name, opts, current):
        """Return statements for the principal settings that changed."""
        raise NotImplementedError(
            f"Managing logins is not supported on {self.dialect_name}")

    def sql_drop_principal(self, name, force=False):
        """Return statements dropping a principal.

        When force is true, the statements first end any session the
        principal holds — a login with a live connection cannot be
        dropped, which is the usual state of one worth cleaning up.
        """
        raise NotImplementedError(
            f"Managing logins is not supported on {self.dialect_name}")

    def sql_principal_sessions(self, name):
        """Return (sql, params) counting sessions held by a principal."""
        raise NotImplementedError(
            f"Managing logins is not supported on {self.dialect_name}")

    # Per-database user mapping, where the dialect separates the two.
    supports_user_mapping = False

    def list_user_mappings(self, cursor, login):
        """Return (headers, rows) of the databases LOGIN is a user in."""
        return []

    def user_mapping_options(self, cursor, login):
        """Field descriptors for mapping LOGIN into a database."""
        return []

    def sql_add_user_mapping(self, login, opts):
        """Return statements making LOGIN a user of a database."""
        raise NotImplementedError(
            f"User mapping is not supported on {self.dialect_name}")

    def sql_remove_user_mapping(self, login, database):
        """Return statements removing LOGIN as a user of DATABASE."""
        raise NotImplementedError(
            f"User mapping is not supported on {self.dialect_name}")

    # --- Schemas and tables ---

    supports_schema_ddl = False

    # Column types offered by the table builder, as [value, label] pairs.
    def column_types(self):
        return []

    def list_schemas_detail(self, cursor):
        """Return (headers, rows) describing the schemas of this database."""
        return [], []

    def schema_options(self, cursor, current=None):
        """Field descriptors for creating a schema."""
        return []

    def sql_create_schema(self, opts):
        """Return statements creating a schema."""
        raise NotImplementedError(
            f"Schema DDL is not supported on {self.dialect_name}")

    def sql_drop_schema(self, name, cascade=False):
        """Return statements dropping a schema."""
        raise NotImplementedError(
            f"Schema DDL is not supported on {self.dialect_name}")

    def list_tables_detail(self, cursor, schema):
        """Return (headers, rows) describing the tables in SCHEMA."""
        return [], []

    def table_options(self, cursor, schema):
        """Field descriptors for the table builder."""
        return []

    def sql_create_table(self, schema, opts):
        """Return statements creating a table."""
        raise NotImplementedError(
            f"Table DDL is not supported on {self.dialect_name}")

    def sql_drop_table(self, schema, name):
        """Return statements dropping a table."""
        raise NotImplementedError(
            f"Table DDL is not supported on {self.dialect_name}")

    def list_table_columns(self, cursor, schema, table):
        """Return the table's columns as builder rows.

        Each row is [name, type, nullable, primary_key, default], using
        the same type vocabulary as `column_types` so that an untouched
        column compares equal and generates no statement.
        """
        return []

    def sql_alter_table(self, schema, table, opts, current):
        """Return statements for the column changes between CURRENT and
        the submitted rows."""
        raise NotImplementedError(
            f"Table DDL is not supported on {self.dialect_name}")

    def _diff_columns(self, opts, current):
        """Return (added, dropped, changed) comparing rows by name.

        A rename cannot be told from a drop plus an add by looking at
        names alone, so it is reported as both; the caller is expected to
        show the plan before running it.
        """
        def keyed(rows):
            out = {}
            for row in rows or []:
                if row and str(row[0] or "").strip():
                    out[str(row[0]).strip()] = row
            return out

        want, have = keyed(opts.get("columns")), keyed(current)
        added = [want[n] for n in want if n not in have]
        dropped = [have[n] for n in have if n not in want]
        changed = []
        for name, row in want.items():
            if name not in have:
                continue
            before = have[name]
            if (str(row[1]) != str(before[1])
                    or bool(row[2]) != bool(before[2])
                    or bool(row[3]) != bool(before[3])
                    or not self._same_default(
                        row[4] if len(row) > 4 else "",
                        before[4] if len(before) > 4 else "")):
                changed.append((before, row))
        return added, dropped, changed

    @staticmethod
    def _same_default(left, right):
        """Compare two DEFAULT expressions.

        The server may echo a function back in a different case than it
        was written — GETDATE() comes out of SQL Server as getdate() —
        which would otherwise read as a change on every comparison.  A
        quoted string is compared exactly, because its case is data.
        """
        a, b = str(left or "").strip(), str(right or "").strip()
        if "'" in a or "'" in b:
            return a == b
        return a.lower() == b.lower()

    # A DEFAULT is an expression rather than a literal, so it cannot be
    # quoted like one — a bare 0 and GETDATE() both have to pass through
    # unquoted.  It also cannot be parameterised, so only these shapes
    # are accepted: a number, a quoted string, or a bare word optionally
    # called as a zero-argument function.  Anything else is refused
    # rather than interpolated.
    _SAFE_DEFAULT = re.compile(
        r"""^(?:
              -?\d+(?:\.\d+)?          # 42, -1, 3.14
            | '(?:[^']|'')*'           # 'text', with '' escapes
            | [A-Za-z_][A-Za-z0-9_]*   # NULL, TRUE, CURRENT_TIMESTAMP
              (?:\s*\(\s*\))?          #   optionally GETDATE(), now()
            )$""",
        re.VERBOSE)

    def validate_default(self, expression):
        """Return EXPRESSION if it is safe to embed as a column DEFAULT."""
        text = (expression or "").strip()
        if not self._SAFE_DEFAULT.match(text):
            raise ValueError(
                f"unsupported DEFAULT expression: {text!r}. Use a number, a "
                f"quoted string, or a bare word such as NULL or GETDATE()")
        return text

    def _column_clauses(self, opts):
        """Turn the builder's column rows into (clauses, primary_keys).

        Each row is [name, type, nullable, primary_key, default].
        """
        clauses, primary = [], []
        for row in opts.get("columns") or []:
            if not row or not str(row[0] or "").strip():
                continue
            name = self.validate_identifier(str(row[0]).strip())
            sql_type = str(row[1] or "").strip()
            if not sql_type:
                raise ValueError(f"column {name} has no type")
            if sql_type not in {t[0] for t in self.column_types()}:
                raise ValueError(f"column {name} has an unknown type: "
                                 f"{sql_type}")
            nullable = bool(row[2]) if len(row) > 2 else True
            is_key = bool(row[3]) if len(row) > 3 else False
            default = str(row[4] or "").strip() if len(row) > 4 else ""

            clause = (f"{self.quote_ddl_identifier(name)} {sql_type}"
                      f"{'' if nullable else ' NOT NULL'}")
            if default:
                clause += f" DEFAULT {self.validate_default(default)}"
            clauses.append(clause)
            if is_key:
                primary.append(name)
        if not clauses:
            raise ValueError("a table needs at least one column")
        return clauses, primary

    # --- Backup and restore ---
    #
    # Only meaningful where the server can back itself up through SQL.
    # PostgreSQL's tooling (pg_dump, pg_basebackup) are external programs,
    # so there is nothing to drive over a connection.

    supports_backup = False

    def backup_history(self, cursor, database):
        """Return (headers, rows) of previous backups of DATABASE."""
        return [], []

    def backup_options(self, cursor, database):
        """Field descriptors for backing up DATABASE."""
        return []

    def restore_options(self, cursor, database):
        """Field descriptors for restoring over or beside DATABASE."""
        return []

    def sql_backup(self, database, opts):
        """Return statements backing up DATABASE."""
        raise NotImplementedError(
            f"Backup is not supported on {self.dialect_name}")

    def backup_contents(self, cursor, path):
        """Return the backup sets held in the file at PATH."""
        raise NotImplementedError(
            f"Backup is not supported on {self.dialect_name}")

    def backup_file_list(self, cursor, path, position=1):
        """Return the data files recorded in a backup set."""
        raise NotImplementedError(
            f"Backup is not supported on {self.dialect_name}")

    def sql_restore(self, database, opts, file_list):
        """Return statements restoring DATABASE from a backup."""
        raise NotImplementedError(
            f"Restore is not supported on {self.dialect_name}")

    # --- Database file management ---
    #
    # Only meaningful where a database is made of files the administrator
    # sizes and places.  PostgreSQL manages its own storage, so it opts
    # out rather than pretending.

    supports_file_management = False

    def database_files(self, cursor, name):
        """Return the files making up database NAME.

        Each entry is a dict with `logical`, `type`, `filegroup`,
        `size_mb`, `growth`, `growth_unit`, `max_mb` and `path`.
        """
        return []

    def database_filegroups(self, cursor, name):
        """Return the filegroup names available in database NAME."""
        return []

    def file_options(self, cursor, name, current=None):
        """Field descriptors for adding, or editing when CURRENT is given."""
        return []

    def sql_add_file(self, name, opts):
        """Return statements adding a file to database NAME."""
        raise NotImplementedError(
            f"File management is not supported on {self.dialect_name}")

    def sql_modify_file(self, name, opts, current):
        """Return statements resizing or re-limiting an existing file."""
        raise NotImplementedError(
            f"File management is not supported on {self.dialect_name}")

    def sql_remove_file(self, name, logical):
        """Return statements removing a file from database NAME."""
        raise NotImplementedError(
            f"File management is not supported on {self.dialect_name}")

    def sql_shrink_file(self, name, logical, target_mb):
        """Return statements shrinking a file to TARGET_MB."""
        raise NotImplementedError(
            f"File management is not supported on {self.dialect_name}")

    # --- Server-side filesystem browsing ---
    #
    # Data and log files live on the server's filesystem, not the client's,
    # so the paths cannot be completed locally.  Drivers that can enumerate
    # directories over SQL opt in here.  Path arithmetic stays on this side
    # because the separator depends on the server's OS, which the Emacs
    # client has no way to know.

    supports_path_browse = False

    def path_separator(self, cursor=None):
        """Return the server's filesystem path separator."""
        return "/"

    def join_path(self, directory, name, cursor=None):
        """Join NAME onto DIRECTORY using the server's separator.

        The separator is taken from DIRECTORY itself where possible, since
        that string came from the server and so already reflects its OS.
        """
        if not directory:
            return name
        trimmed = directory.rstrip("/\\")
        if "\\" in trimmed:
            sep = "\\"
        elif "/" in trimmed:
            sep = "/"
        else:
            sep = self.path_separator(cursor)
        return trimmed + sep + name

    def parent_path(self, path, cursor=None):
        """Return the parent of PATH, or None at the filesystem root."""
        trimmed = (path or "").rstrip("/\\")
        if not trimmed:
            return None
        index = max(trimmed.rfind("/"), trimmed.rfind("\\"))
        if index < 0:
            return None
        parent = trimmed[:index]
        # Keep the leading separator on a POSIX root, and the trailing one
        # on a Windows drive root ("C:" is not a usable path, "C:\" is).
        if not parent:
            return "/"
        if parent.endswith(":"):
            return parent + "\\"
        return parent

    def browse_path(self, cursor, path):
        """List PATH on the server, newest strategy first.

        Returns a list of dicts with `name`, `path` and `is_dir` keys.
        Raises NotImplementedError when the dialect cannot enumerate.
        """
        raise NotImplementedError(
            f"Browsing server paths is not supported on {self.dialect_name}")

    def default_paths(self, cursor):
        """Return {"data": path, "log": path} defaults, empty when unknown."""
        return {}

    # --- Type mapping for :in imports ---

    def python_type_to_sql(self, python_type):
        """Map a Python type name to a SQL DDL type string.

        python_type is a string from pyarrow or Python's type system,
        e.g. 'int64', 'float64', 'string', 'bool', 'date32', 'timestamp[us]'.
        Returns a SQL type string, e.g. 'BIGINT', 'FLOAT', 'NVARCHAR(MAX)'.
        Subclasses should override this for dialect-specific type names.
        """
        return _ANSI_TYPE_MAP.get(python_type, "NVARCHAR(MAX)")


# ANSI SQL type mapping used as the default fallback.
_ANSI_TYPE_MAP = {
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
    "string":         "NVARCHAR(MAX)",
    "large_string":   "NVARCHAR(MAX)",
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
    "binary":         "BLOB",
    "large_binary":   "BLOB",
    "decimal128":     "DECIMAL(38,10)",
}


class AnsiDriver(BaseDriver):
    """Fallback driver using ANSI SQL / INFORMATION_SCHEMA.

    Works on most modern databases for basic introspection but may fail
    on some platforms (e.g. sql_running_queries is not standardized).
    Emits a warning envelope when used so the user knows dialect is unknown.
    """

    dialect_name = "ansi"

    @property
    def sql_list_databases(self):
        # Not in ANSI SQL — this will fail on most platforms gracefully.
        return "SELECT CATALOG_NAME FROM INFORMATION_SCHEMA.SCHEMATA"

    @property
    def sql_current_database(self):
        return "SELECT CURRENT_CATALOG"

    @property
    def sql_list_schemas(self):
        return "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME"

    @property
    def sql_current_schema(self):
        return "SELECT CURRENT_SCHEMA"

    @property
    def sql_list_tables(self):
        return """
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """

    @property
    def sql_current_user(self):
        return "SELECT CURRENT_USER"

    @property
    def sql_server_version(self):
        # No ANSI standard for this — subclasses override.
        return "SELECT 'unknown'"

    @property
    def sql_running_queries(self):
        # No ANSI standard for this — subclasses override.
        return "SELECT 'not supported' AS note"

    def sql_list_databases_like(self, pattern):
        return ("SELECT CATALOG_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                "WHERE CATALOG_NAME LIKE ? "
                "ORDER BY CATALOG_NAME", [pattern])

    def sql_list_schemas_like(self, pattern):
        return ("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                "WHERE SCHEMA_NAME LIKE ? "
                "ORDER BY SCHEMA_NAME", [pattern])

    def sql_list_tables_like(self, pattern):
        return ("SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE "
                "FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_NAME LIKE ? "
                "ORDER BY TABLE_SCHEMA, TABLE_NAME", [pattern])
