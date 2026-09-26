"""Live tests for the databases admin panel.

These exercise the panel against real servers, covering what unit tests
cannot: that the list queries run, that generated DDL is accepted, that
options actually take effect, and that the drop guards behave.

Requires the docker test databases:

    docker compose up -d postgres mssql

Tests skip automatically when a server or ODBC driver is unavailable,
unless DATUM_STRICT=1 is set.
"""

import base64
import json
import os

import pytest

pyodbc = pytest.importorskip("pyodbc")

# pyodbc pools connections by default, so closing a probe connection
# returns it to the pool rather than disconnecting.  A login still
# holding a pooled session cannot be dropped, which leaks state from one
# test into the next.
pyodbc.pooling = False

from datum.drivers.mssql import MSSQLDriver          # noqa: E402
from datum.drivers.postgres import PostgreSQLDriver  # noqa: E402

STRICT = os.environ.get("DATUM_STRICT") == "1"
TESTDB = "datum_wiz_pytest"

PG_DSN = (
    "Driver={{PostgreSQL Unicode}};Server={host};Port={port};"
    "Database={db};Uid={user};Pwd={pw}".format(
        host=os.environ.get("DATUM_PG_SERVER", "127.0.0.1"),
        port=os.environ.get("DATUM_PG_PORT", "5433"),
        db=os.environ.get("DATUM_PG_DB", "datum_test"),
        user=os.environ.get("DATUM_PG_USER", "postgres"),
        pw=os.environ.get("DATUM_PG_PASS", "datum_test"),
    )
)

MS_DSN = (
    "Driver={{ODBC Driver 18 for SQL Server}};Server={host},{port};"
    "Database=master;Uid={user};Pwd={pw};TrustServerCertificate=yes".format(
        host=os.environ.get("DATUM_MSSQL_SERVER", "127.0.0.1"),
        port=os.environ.get("DATUM_MSSQL_PORT", "1434"),
        user=os.environ.get("DATUM_MSSQL_USER", "sa"),
        pw=os.environ.get("DATUM_MSSQL_PASS", "DatumTest1!"),
    )
)


_UNREACHABLE = {}


def _connect(dsn, label):
    # Remember an unreachable server so the whole module does not pay the
    # connection timeout once per test when the containers are not running.
    if dsn in _UNREACHABLE:
        pytest.skip(f"{label} unavailable: {_UNREACHABLE[dsn]}")
    try:
        return pyodbc.connect(dsn, autocommit=True, timeout=5)
    except Exception as err:  # pragma: no cover - environment dependent
        if STRICT:
            raise
        _UNREACHABLE[dsn] = str(err)[:90]
        pytest.skip(f"{label} unavailable: {_UNREACHABLE[dsn]}")


@pytest.fixture
def captured(monkeypatch):
    """Capture envelope output instead of emitting the wire protocol."""
    from datum import envelope

    events = []
    for kind in ("info", "warn", "error", "admin_panel", "definition"):
        monkeypatch.setattr(
            envelope, kind,
            (lambda k: (lambda *a, **kw: events.append((k, a))))(kind))
    return events


def _kinds(events):
    return [k for k, _ in events]


def _assert_ok(events, refreshes=None):
    """Assert the action succeeded, allowing the panel refresh that
    follows a mutation.

    Sub-panels have no auto-refresh timer, so a mutating action re-sends
    the affected panel; REFRESHES names the sub-panel expected, or None
    when a refresh is not required.
    """
    kinds = _kinds(events)
    assert "error" not in kinds, events
    assert kinds[0] == "info", events
    if refreshes is not None:
        panels = [a[0] for k, a in events if k == "admin_panel"]
        assert panels, f"expected a {refreshes} refresh, got {kinds}"
        assert panels[-1].get("sub_panel") == refreshes or \
            (refreshes == "" and not panels[-1].get("sub_panel")), panels[-1]


def _payload(obj):
    return base64.b64encode(json.dumps(obj).encode()).decode()


def _exists(cursor, dialect, name):
    if dialect == "pg":
        cursor.execute("SELECT COUNT(*) FROM pg_database WHERE datname = ?",
                       [name])
    else:
        cursor.execute("SELECT COUNT(*) FROM sys.databases WHERE name = ?",
                       [name])
    return cursor.fetchone()[0] == 1


@pytest.fixture(params=["pg", "mssql"])
def env(request):
    """Yield (dialect, cursor, driver) with the scratch database removed."""
    dialect = request.param
    if dialect == "pg":
        conn, driver = _connect(PG_DSN, "PostgreSQL"), PostgreSQLDriver()
    else:
        conn, driver = _connect(MS_DSN, "MSSQL"), MSSQLDriver()
    cursor = conn.cursor()

    def cleanup():
        try:
            for stmt in driver.sql_drop_database(TESTDB, force=True):
                cursor.execute(stmt)
        except Exception:
            pass

    cleanup()
    yield dialect, cursor, driver
    cleanup()
    conn.close()


class TestDatabaseList:
    def test_list_returns_rows(self, env):
        from datum.panels import databases
        _, cursor, driver = env
        result = databases.get_data(cursor, driver, [])
        assert result["headers"]
        assert result["rows"]
        assert result["row_id"] == 0
        # Every row must be as wide as the header, or the table renders ragged.
        assert all(len(r) == len(result["headers"]) for r in result["rows"])

    def test_list_offers_create_and_drop(self, env):
        from datum.panels import databases
        _, cursor, driver = env
        commands = {a["command"]
                    for a in databases.get_data(cursor, driver, [])["actions"]}
        assert {"new-database", "drop-check"} <= commands


class TestCreateAndDrop:
    def test_preview_does_not_create(self, env, captured):
        from datum.panels import databases
        dialect, cursor, driver = env
        databases.run_action(cursor, driver, "preview-create",
                             [_payload({"name": TESTDB})])
        assert _kinds(captured) == ["definition"]
        assert not _exists(cursor, dialect, TESTDB)

    def test_create_then_drop(self, env, captured):
        from datum.panels import databases
        dialect, cursor, driver = env

        databases.run_action(cursor, driver, "create-database",
                             [_payload({"name": TESTDB})])
        _assert_ok(captured)
        assert _exists(cursor, dialect, TESTDB)

        captured.clear()
        databases.run_action(cursor, driver, "drop-database",
                             [_payload({"name": TESTDB, "force": False})])
        _assert_ok(captured)
        assert not _exists(cursor, dialect, TESTDB)

    def test_duplicate_create_reports_clean_error(self, env, captured):
        from datum.panels import databases
        _, cursor, driver = env
        databases.run_action(cursor, driver, "create-database",
                             [_payload({"name": TESTDB})])
        captured.clear()
        databases.run_action(cursor, driver, "create-database",
                             [_payload({"name": TESTDB})])
        assert _kinds(captured) == ["error"]
        message = captured[0][1][0]
        assert "already exists" in message
        # The driver framing must be stripped from what the user sees.
        assert "SQLExecDirectW" not in message
        assert "[Microsoft]" not in message

    def test_force_drop_evicts_other_sessions(self, env, captured):
        from datum.panels import databases
        dialect, cursor, driver = env
        databases.run_action(cursor, driver, "create-database",
                             [_payload({"name": TESTDB})])
        captured.clear()

        source = "Database=datum_test" if dialect == "pg" else "Database=master"
        dsn = (PG_DSN if dialect == "pg" else MS_DSN).replace(
            source, f"Database={TESTDB}")
        holder = pyodbc.connect(dsn, autocommit=True, timeout=10)
        holder.cursor().execute("SELECT 1")
        try:
            sql, params = driver.sql_database_sessions(TESTDB)
            cursor.execute(sql, params)
            assert cursor.fetchone()[0] >= 1

            databases.run_action(cursor, driver, "drop-database",
                                 [_payload({"name": TESTDB, "force": True})])
            _assert_ok(captured)
            assert not _exists(cursor, dialect, TESTDB)
        finally:
            try:
                holder.close()
            except Exception:
                pass


class TestDropGuards:
    def test_refuses_to_drop_current_database(self, env, captured):
        from datum.panels import databases
        _, cursor, driver = env
        cursor.execute(driver.sql_current_database)
        current = cursor.fetchone()[0]

        databases.run_action(cursor, driver, "drop-check", [current])
        assert _kinds(captured) == ["error"]
        message = captured[0][1][0]
        assert "current database" in message
        # Never tell the user to switch to the database they are already in.
        assert f":use {current}" not in message

    def test_drop_form_requires_typed_confirmation(self, env, captured):
        from datum.panels import databases
        _, cursor, driver = env
        databases.run_action(cursor, driver, "create-database",
                             [_payload({"name": TESTDB})])
        captured.clear()

        databases.run_action(cursor, driver, "drop-check", [TESTDB])
        assert _kinds(captured) == ["admin_panel"]
        form = captured[0][1][0]["form"]
        assert form["confirm_text"] == TESTDB
        assert form["danger"] is True
        assert form["submit_action"] == "drop-database"


class TestOptionsTakeEffect:
    def test_mssql_files_and_recovery_model(self, captured):
        from datum.panels import databases
        conn = _connect(MS_DSN, "MSSQL")
        cursor, driver = conn.cursor(), MSSQLDriver()
        name = "datum_wiz_files"

        def cleanup():
            try:
                for stmt in driver.sql_drop_database(name, force=True):
                    cursor.execute(stmt)
            except Exception:
                pass

        cleanup()
        try:
            databases.run_action(cursor, driver, "create-database", [_payload({
                "name": name, "recovery_model": "SIMPLE",
                "collation": "Latin1_General_BIN",
                "data_path": f"/var/opt/mssql/data/{name}.mdf",
                "data_size_mb": 16, "data_growth_mb": 8,
                "log_path": f"/var/opt/mssql/data/{name}.ldf",
                "log_size_mb": 8, "log_growth_mb": 8})])
            _assert_ok(captured)

            cursor.execute(
                "SELECT recovery_model_desc, collation_name, "
                "(SELECT COUNT(*) FROM sys.master_files f "
                " WHERE f.database_id = d.database_id) "
                "FROM sys.databases d WHERE name = ?", [name])
            recovery, collation, files = cursor.fetchone()
            assert recovery == "SIMPLE"
            assert collation == "Latin1_General_BIN"
            assert files == 2  # the data file and the log file
        finally:
            cleanup()
            conn.close()

    def test_postgres_locale_and_connection_limit(self, captured):
        from datum.panels import databases
        conn = _connect(PG_DSN, "PostgreSQL")
        cursor, driver = conn.cursor(), PostgreSQLDriver()
        name = "datum_wiz_locale"

        def cleanup():
            try:
                for stmt in driver.sql_drop_database(name, force=True):
                    cursor.execute(stmt)
            except Exception:
                pass

        cleanup()
        try:
            # Changing the locale requires template0 rather than template1.
            databases.run_action(cursor, driver, "create-database", [_payload({
                "name": name, "template": "template0", "encoding": "UTF8",
                "lc_collate": "C", "lc_ctype": "C", "connection_limit": 5})])
            _assert_ok(captured)

            cursor.execute("SELECT datcollate, datctype, datconnlimit "
                           "FROM pg_database WHERE datname = ?", [name])
            collate, ctype, limit = cursor.fetchone()
            assert (collate, ctype, limit) == ("C", "C", 5)
        finally:
            cleanup()
            conn.close()


class TestInputValidation:
    @pytest.mark.parametrize("bad_name", ["", "   ", "bad\nname", "x" * 200])
    def test_rejects_bad_names(self, env, captured, bad_name):
        from datum.panels import databases
        _, cursor, driver = env
        databases.run_action(cursor, driver, "create-database",
                             [_payload({"name": bad_name})])
        assert _kinds(captured) == ["error"], captured

    def test_rejects_malformed_payload(self, env, captured):
        from datum.panels import databases
        _, cursor, driver = env
        databases.run_action(cursor, driver, "create-database",
                             ["!!!not-base64!!!"])
        assert _kinds(captured) == ["error"]

    def test_rejects_non_numeric_int_field(self, env, captured):
        from datum.panels import databases
        dialect, cursor, driver = env
        field = "data_size_mb" if dialect == "mssql" else "connection_limit"
        databases.run_action(cursor, driver, "create-database",
                             [_payload({"name": TESTDB, field: "abc"})])
        assert _kinds(captured) == ["error"]
        assert "must be a number" in captured[0][1][0]


class TestDatabaseSettings:
    """Altering an existing database, against the real servers."""

    def test_edit_form_reflects_the_server(self, env, captured):
        from datum.panels import databases
        _, cursor, driver = env
        databases.run_action(cursor, driver, "create-database",
                             [_payload({"name": TESTDB})])
        captured.clear()

        databases.run_action(cursor, driver, "edit-database", [TESTDB])
        assert _kinds(captured) == ["admin_panel"], captured
        form = captured[0][1][0]["form"]
        assert form["submit_action"] == "alter-database"
        # The name the user acted on has to survive into the submission,
        # otherwise a rename would have nothing to rename from.
        assert form["values"]["name_original"] == TESTDB
        defaults = {f["key"]: f.get("default") for f in form["fields"]}
        assert defaults["name"] == TESTDB

    def test_unchanged_form_applies_nothing(self, env, captured):
        from datum.panels import databases
        _, cursor, driver = env
        databases.run_action(cursor, driver, "create-database",
                             [_payload({"name": TESTDB})])
        databases.run_action(cursor, driver, "edit-database", [TESTDB])
        form = captured[-1][1][0]["form"]
        submission = {f["key"]: f.get("default") for f in form["fields"]}
        submission.update(form["values"])
        captured.clear()

        databases.run_action(cursor, driver, "alter-database",
                             [_payload(submission)])
        _assert_ok(captured)
        assert "No changes" in captured[0][1][0]

    def test_changed_setting_is_applied(self, env, captured):
        from datum.panels import databases
        dialect, cursor, driver = env
        databases.run_action(cursor, driver, "create-database",
                             [_payload({"name": TESTDB})])
        databases.run_action(cursor, driver, "edit-database", [TESTDB])
        form = captured[-1][1][0]["form"]
        submission = {f["key"]: f.get("default") for f in form["fields"]}
        submission.update(form["values"])

        if dialect == "mssql":
            submission["recovery_model"] = "SIMPLE"
            verify = ("SELECT recovery_model_desc FROM sys.databases "
                      "WHERE name = ?", "SIMPLE")
        else:
            submission["connection_limit"] = 11
            verify = ("SELECT datconnlimit FROM pg_database "
                      "WHERE datname = ?", 11)
        captured.clear()

        databases.run_action(cursor, driver, "alter-database",
                             [_payload(submission)])
        _assert_ok(captured)
        cursor.execute(verify[0], [TESTDB])
        assert cursor.fetchone()[0] == verify[1]

    def test_preview_does_not_apply(self, env, captured):
        from datum.panels import databases
        dialect, cursor, driver = env
        databases.run_action(cursor, driver, "create-database",
                             [_payload({"name": TESTDB})])
        databases.run_action(cursor, driver, "edit-database", [TESTDB])
        form = captured[-1][1][0]["form"]
        submission = {f["key"]: f.get("default") for f in form["fields"]}
        submission.update(form["values"])
        key, value, check = (("recovery_model", "SIMPLE",
                              ("SELECT recovery_model_desc FROM sys.databases "
                               "WHERE name = ?", "FULL"))
                             if dialect == "mssql" else
                             ("connection_limit", 11,
                              ("SELECT datconnlimit FROM pg_database "
                               "WHERE datname = ?", -1)))
        submission[key] = value
        captured.clear()

        databases.run_action(cursor, driver, "preview-alter",
                             [_payload(submission)])
        assert _kinds(captured) == ["definition"]
        cursor.execute(check[0], [TESTDB])
        assert cursor.fetchone()[0] == check[1]

    def test_missing_original_name_is_refused(self, env, captured):
        from datum.panels import databases
        _, cursor, driver = env
        databases.run_action(cursor, driver, "alter-database",
                             [_payload({"name": TESTDB})])
        assert _kinds(captured) == ["error"]


class TestMSSQLFileLimitsLive:
    def test_maxsize_and_percent_growth_take_effect(self, captured):
        from datum.panels import databases
        conn = _connect(MS_DSN, "MSSQL")
        cursor, driver = conn.cursor(), MSSQLDriver()
        name = "datum_wiz_limits"

        def cleanup():
            try:
                for stmt in driver.sql_drop_database(name, force=True):
                    cursor.execute(stmt)
            except Exception:
                pass

        cleanup()
        try:
            databases.run_action(cursor, driver, "create-database", [_payload({
                "name": name,
                "data_dir": "/var/opt/mssql/data",
                "log_dir": "/var/opt/mssql/data",
                "data_size_mb": 16, "data_growth": 10,
                "data_growth_unit": "%", "data_max_mb": 128,
                "log_size_mb": 8, "log_growth": 8,
                "log_growth_unit": "MB", "log_max_mb": 64,
                "compatibility_level": "150"})])
            _assert_ok(captured)

            cursor.execute(
                "SELECT mf.name, mf.is_percent_growth, "
                "       mf.max_size * 8 / 1024, d.compatibility_level "
                "FROM sys.master_files mf "
                "JOIN sys.databases d ON d.database_id = mf.database_id "
                "WHERE d.name = ? ORDER BY mf.type", [name])
            rows = cursor.fetchall()
            data_file, log_file = rows[0], rows[1]
            assert data_file[1] is True   # percent growth
            assert data_file[2] == 128    # MAXSIZE in MB
            assert log_file[1] is False   # megabyte growth
            assert log_file[2] == 64
            assert data_file[3] == 150    # compatibility level
        finally:
            cleanup()
            conn.close()


class TestFileManagementLive:
    """File management against a real SQL Server."""

    @pytest.fixture
    def mssql_db(self):
        conn = _connect(MS_DSN, "MSSQL")
        cursor, driver = conn.cursor(), MSSQLDriver()
        name = "datum_wiz_files"

        def cleanup():
            try:
                for stmt in driver.sql_drop_database(name, force=True):
                    cursor.execute(stmt)
            except Exception:
                pass

        cleanup()
        cursor.execute(f"CREATE DATABASE [{name}]")
        yield name, cursor, driver
        cleanup()
        conn.close()

    def test_lists_the_files(self, mssql_db, captured):
        from datum.panels import databases
        name, cursor, driver = mssql_db
        databases.run_action(cursor, driver, "files", [name])
        assert _kinds(captured) == ["admin_panel"], captured
        panel = captured[0][1][0]
        assert panel["sub_panel"] == "files"
        assert panel["context"]["database"] == name
        types = {r[1] for r in panel["rows"]}
        assert types == {"ROWS", "LOG"}

    def test_filegroup_column_matches_the_server(self, mssql_db, captured):
        from datum.panels import databases
        name, cursor, driver = mssql_db
        cursor.execute(f"ALTER DATABASE [{name}] ADD FILEGROUP fg_alpha")
        databases.run_action(cursor, driver, "add-file", [_payload({
            "database": name, "logical": f"{name}_a", "file_type": "ROWS",
            "filegroup": "fg_alpha", "directory": "/var/opt/mssql/data",
            "size_mb": 8, "growth": 8, "max_mb": 0})])
        captured.clear()

        databases.run_action(cursor, driver, "files", [name])
        rows = {r[0]: r[2] for r in captured[0][1][0]["rows"]}
        # sys.filegroups is database-scoped: reading it from master
        # mislabels any filegroup whose id also exists there.
        cursor.execute(
            f"SELECT df.name, ISNULL(fg.name, '') "
            f"FROM [{name}].sys.database_files df "
            f"LEFT JOIN [{name}].sys.filegroups fg "
            f"  ON fg.data_space_id = df.data_space_id")
        for logical, filegroup in cursor.fetchall():
            assert rows[logical] == filegroup

    def test_add_edit_shrink_remove(self, mssql_db, captured):
        from datum.panels import databases
        name, cursor, driver = mssql_db
        logical = f"{name}_extra"

        databases.run_action(cursor, driver, "add-file", [_payload({
            "database": name, "logical": logical, "file_type": "ROWS",
            "filegroup": "PRIMARY", "directory": "/var/opt/mssql/data",
            "size_mb": 16, "growth": 10, "growth_unit": "%",
            "max_mb": 128})])
        _assert_ok(captured)

        entry = next(f for f in driver.database_files(cursor, name)
                     if f["logical"] == logical)
        assert (entry["size_mb"], entry["growth"], entry["growth_unit"],
                entry["max_mb"]) == (16, 10, "%", 128)

        captured.clear()
        databases.run_action(cursor, driver, "modify-file", [_payload({
            "database": name, "logical": logical, "size_mb": 32,
            "growth": 16, "growth_unit": "MB", "max_mb": 256})])
        _assert_ok(captured)
        entry = next(f for f in driver.database_files(cursor, name)
                     if f["logical"] == logical)
        assert (entry["size_mb"], entry["growth_unit"],
                entry["max_mb"]) == (32, "MB", 256)

        captured.clear()
        databases.run_action(cursor, driver, "modify-file", [_payload({
            "database": name, "logical": logical, "size_mb": 32,
            "growth": 16, "growth_unit": "MB", "max_mb": 256})])
        assert "No changes" in captured[0][1][0]

        captured.clear()
        databases.run_action(cursor, driver, "do-shrink-file", [_payload({
            "database": name, "logical": logical, "target_mb": 8})])
        _assert_ok(captured)

        captured.clear()
        databases.run_action(cursor, driver, "remove-file", [_payload({
            "database": name, "logical": logical})])
        _assert_ok(captured)
        assert not any(f["logical"] == logical
                       for f in driver.database_files(cursor, name))

    def test_edit_form_is_prefilled(self, mssql_db, captured):
        from datum.panels import databases
        name, cursor, driver = mssql_db
        databases.run_action(cursor, driver, "edit-file", [_payload({
            "database": name, "logical": name})])
        assert _kinds(captured) == ["admin_panel"], captured
        form = captured[0][1][0]["form"]
        assert form["submit_action"] == "modify-file"
        assert form["values"]["database"] == name
        assert form["values"]["logical"] == name

    def test_removing_a_missing_file_reports_cleanly(self, mssql_db, captured):
        from datum.panels import databases
        name, cursor, driver = mssql_db
        databases.run_action(cursor, driver, "remove-file", [_payload({
            "database": name, "logical": "no_such_file"})])
        assert _kinds(captured) == ["error"]
        assert "SQLExecDirectW" not in captured[0][1][0]


class TestFileManagementUnsupported:
    def test_postgres_explains_instead_of_failing(self, captured):
        from datum.panels import databases
        conn = _connect(PG_DSN, "PostgreSQL")
        try:
            databases.run_action(conn.cursor(), PostgreSQLDriver(), "files",
                                 ["datum_test"])
            assert _kinds(captured) == ["error"]
            assert "tablespaces" in captured[0][1][0]
        finally:
            conn.close()


class TestFileMutationsRefreshThePanel:
    """The file list is a sub-panel, so it has no auto-refresh timer."""

    @pytest.fixture
    def mssql_db(self):
        conn = _connect(MS_DSN, "MSSQL")
        cursor, driver = conn.cursor(), MSSQLDriver()
        name = "datum_wiz_refresh"

        def cleanup():
            try:
                for stmt in driver.sql_drop_database(name, force=True):
                    cursor.execute(stmt)
            except Exception:
                pass

        cleanup()
        cursor.execute(f"CREATE DATABASE [{name}]")
        yield name, cursor, driver
        cleanup()
        conn.close()

    def test_add_and_remove_resend_the_file_list(self, mssql_db, captured):
        from datum.panels import databases
        name, cursor, driver = mssql_db
        logical = f"{name}_extra"
        captured.clear()

        databases.run_action(cursor, driver, "add-file", [_payload({
            "database": name, "logical": logical, "file_type": "ROWS",
            "filegroup": "PRIMARY", "directory": "/var/opt/mssql/data",
            "size_mb": 8, "growth": 8, "max_mb": 0})])
        _assert_ok(captured, refreshes="files")
        panel = [a[0] for k, a in captured if k == "admin_panel"][-1]
        assert any(r[0] == logical for r in panel["rows"])

        captured.clear()
        databases.run_action(cursor, driver, "remove-file", [_payload({
            "database": name, "logical": logical})])
        _assert_ok(captured, refreshes="files")
        panel = [a[0] for k, a in captured if k == "admin_panel"][-1]
        assert not any(r[0] == logical for r in panel["rows"])

    def test_create_and_drop_resend_the_database_list(self, env, captured):
        from datum.panels import databases
        dialect, cursor, driver = env
        databases.run_action(cursor, driver, "create-database",
                             [_payload({"name": TESTDB})])
        _assert_ok(captured, refreshes="")
        panel = [a[0] for k, a in captured if k == "admin_panel"][-1]
        assert any(r[0] == TESTDB for r in panel["rows"])

        captured.clear()
        databases.run_action(cursor, driver, "drop-database",
                             [_payload({"name": TESTDB, "force": True})])
        _assert_ok(captured, refreshes="")
        panel = [a[0] for k, a in captured if k == "admin_panel"][-1]
        assert not any(r[0] == TESTDB for r in panel["rows"])


@pytest.fixture
def backup_db():
    """A small database to back up, and a name to restore it under."""
    conn = _connect(MS_DSN, "MSSQL")
    cursor, driver = conn.cursor(), MSSQLDriver()
    name, target = "datum_bk_src", "datum_bk_dst"

    def cleanup():
        for db in (name, target):
            try:
                for stmt in driver.sql_drop_database(db, force=True):
                    cursor.execute(stmt)
            except Exception:
                pass

    cleanup()
    cursor.execute(f"CREATE DATABASE [{name}]")
    cursor.execute(
        f"EXEC [{name}]..sp_executesql "
        f"N'CREATE TABLE dbo.payload(id int); INSERT dbo.payload VALUES (42)'")
    yield name, target, cursor, driver
    cleanup()
    conn.close()


def _run_backup(cursor, driver, name, **extra):
    """Back NAME up through the panel, returning where it landed."""
    from datum.panels import databases
    opts = {"database": name, "backup_type": "FULL",
            "directory": "/var/opt/mssql/data",
            "filename": f"{name}.bak", "overwrite": True,
            "compression": True, "checksum": True}
    opts.update(extra)
    databases.run_action(cursor, driver, "do-backup", [_payload(opts)])
    return f"/var/opt/mssql/data/{opts['filename']}"


class TestBackupRestoreLive:
    """Backup and restore against a real SQL Server."""

    def test_backup_then_history(self, backup_db, captured):
        from datum.panels import databases
        name, _target, cursor, driver = backup_db
        _run_backup(cursor, driver, name)
        _assert_ok(captured, refreshes="backups")

        captured.clear()
        databases.run_action(cursor, driver, "backups", [name])
        panel = captured[0][1][0]
        assert panel["sub_panel"] == "backups"
        assert any(r[0] == "Full" for r in panel["rows"])

    def test_differential_appends_to_the_file(self, backup_db, captured):
        name, _target, cursor, driver = backup_db
        _run_backup(cursor, driver, name)
        captured.clear()
        _run_backup(cursor, driver, name, backup_type="DIFFERENTIAL",
                     overwrite=False)
        _assert_ok(captured, refreshes="backups")
        panel = [a[0] for k, a in captured if k == "admin_panel"][-1]
        kinds = {r[0] for r in panel["rows"]}
        assert {"Full", "Differential"} <= kinds

    def test_restore_into_a_new_database(self, backup_db, captured):
        from datum.panels import databases
        name, target, cursor, driver = backup_db
        source = _run_backup(cursor, driver, name)
        captured.clear()

        # The target does not exist yet, so the exclusive-access step has
        # to be skipped rather than fail.
        databases.run_action(cursor, driver, "do-restore", [_payload({
            "database": name, "target": target, "source": source,
            "position": 1, "data_dir": "/var/opt/mssql/data",
            "log_dir": "/var/opt/mssql/data",
            "replace": True, "recovery": True})])
        _assert_ok(captured)

        cursor.execute("SELECT state_desc FROM sys.databases WHERE name = ?",
                       [target])
        assert cursor.fetchone()[0] == "ONLINE"
        cursor.execute(f"SELECT id FROM [{target}].dbo.payload")
        assert [r[0] for r in cursor.fetchall()] == [42]
        # Every file must have been moved, or the restore would have
        # tried to write over the source database's own files.
        cursor.execute(
            f"SELECT physical_name FROM [{target}].sys.database_files")
        paths = [r[0] for r in cursor.fetchall()]
        assert all(target in p for p in paths), paths

    def test_restore_over_a_database_holding_a_session(self, backup_db,
                                                       captured):
        from datum.panels import databases
        name, target, cursor, driver = backup_db
        source = _run_backup(cursor, driver, name)
        databases.run_action(cursor, driver, "do-restore", [_payload({
            "database": name, "target": target, "source": source,
            "data_dir": "/var/opt/mssql/data",
            "log_dir": "/var/opt/mssql/data",
            "replace": True, "recovery": True})])
        cursor.execute(
            f"EXEC [{target}]..sp_executesql N'INSERT dbo.payload VALUES (99)'")

        holder = pyodbc.connect(
            MS_DSN.replace("Database=master", f"Database={target}"),
            autocommit=True, timeout=10)
        holder.cursor().execute("SELECT 1")
        captured.clear()
        try:
            databases.run_action(cursor, driver, "do-restore", [_payload({
                "database": name, "target": target, "source": source,
                "data_dir": "/var/opt/mssql/data",
                "log_dir": "/var/opt/mssql/data",
                "replace": True, "recovery": True})])
            _assert_ok(captured)
            # The backup predates the extra row.
            cursor.execute(f"SELECT COUNT(*) FROM [{target}].dbo.payload")
            assert cursor.fetchone()[0] == 1
            # And the database must not be left in single-user mode.
            cursor.execute("SELECT user_access_desc FROM sys.databases "
                           "WHERE name = ?", [target])
            assert cursor.fetchone()[0] == "MULTI_USER"
        finally:
            try:
                holder.close()
            except Exception:
                pass

    def test_preview_does_not_restore(self, backup_db, captured):
        from datum.panels import databases
        name, target, cursor, driver = backup_db
        source = _run_backup(cursor, driver, name)
        captured.clear()

        databases.run_action(cursor, driver, "preview-restore", [_payload({
            "database": name, "target": target, "source": source,
            "data_dir": "/var/opt/mssql/data",
            "log_dir": "/var/opt/mssql/data", "replace": True})])
        assert _kinds(captured) == ["definition"]
        assert "MOVE" in captured[0][1][1]
        cursor.execute("SELECT COUNT(*) FROM sys.databases WHERE name = ?",
                       [target])
        assert cursor.fetchone()[0] == 0

    def test_file_name_escaping_the_directory_is_refused(self, backup_db,
                                                         captured):
        from datum.panels import databases
        name, _target, cursor, driver = backup_db
        captured.clear()
        databases.run_action(cursor, driver, "do-backup", [_payload({
            "database": name, "directory": "/var/opt/mssql/data",
            "filename": "../escape.bak"})])
        assert _kinds(captured) == ["error"]
        assert "path separator" in captured[0][1][0]


class TestBackupUnsupported:
    def test_postgres_explains_instead_of_failing(self, captured):
        from datum.panels import databases
        conn = _connect(PG_DSN, "PostgreSQL")
        try:
            databases.run_action(conn.cursor(), PostgreSQLDriver(),
                                 "backups", ["datum_test"])
            assert _kinds(captured) == ["error"]
            assert "pg_dump" in captured[0][1][0]
        finally:
            conn.close()


class TestARestoreAlwaysReopensTheDatabase:
    """A restore takes the database to SINGLE_USER to get exclusive
    access.  Stopping there because the restore failed locks everybody
    out, which is worse than the failure."""

    def _access(self, cursor, name):
        cursor.execute("SELECT user_access_desc FROM sys.databases "
                       "WHERE name = ?", [name])
        row = cursor.fetchone()
        return row[0] if row else None

    def test_a_failed_restore_leaves_the_database_usable(self, backup_db,
                                                        captured):
        from datum.panels import databases

        name, _target, cursor, driver = backup_db
        _run_backup(cursor, driver, name)
        captured.clear()
        # Past SINGLE_USER, then fails: the files are sent somewhere
        # that cannot be written.
        databases.run_action(cursor, driver, "do-restore", [_payload(
            {"database": name, "target": name,
             "source": f"/var/opt/mssql/data/{name}.bak",
             "position": 1, "replace": True, "recovery": True,
             "data_dir": "/proc/nowhere", "log_dir": "/proc/nowhere"})])
        assert "error" in _kinds(captured), captured
        assert self._access(cursor, name) == "MULTI_USER"

    def test_a_restore_that_works_reopens_it_too(self, backup_db, captured):
        from datum.panels import databases

        name, _target, cursor, driver = backup_db
        _run_backup(cursor, driver, name)
        captured.clear()
        databases.run_action(cursor, driver, "do-restore", [_payload(
            {"database": name, "target": name,
             "source": f"/var/opt/mssql/data/{name}.bak",
             "position": 1, "replace": True, "recovery": True,
             "data_dir": "/var/opt/mssql/data",
             "log_dir": "/var/opt/mssql/data"})])
        assert "error" not in _kinds(captured), captured
        assert self._access(cursor, name) == "MULTI_USER"

    def test_the_reopen_is_shown_in_the_preview(self, backup_db, captured):
        from datum.panels import databases

        name, _target, cursor, driver = backup_db
        _run_backup(cursor, driver, name)
        captured.clear()
        databases.run_action(cursor, driver, "preview-restore", [_payload(
            {"database": name, "target": name,
             "source": f"/var/opt/mssql/data/{name}.bak",
             "position": 1, "replace": True, "recovery": True,
             "data_dir": "/var/opt/mssql/data",
             "log_dir": "/var/opt/mssql/data"})])
        sql = [a for k, a in captured if k == "definition"][0][1]
        # What runs is what is shown, reopening included.
        assert "SINGLE_USER" in sql
        assert "RESTORE DATABASE" in sql
        assert "MULTI_USER" in sql

    def test_nothing_is_reopened_that_was_never_closed(self, backup_db):
        _name, _target, _cursor, driver = backup_db
        # Without REPLACE the restore never takes exclusive access.
        assert driver.sql_restore_cleanup("db", {"replace": False}) == []
        # With NORECOVERY the database is left restoring, where ALTER
        # DATABASE has nothing to say.
        assert driver.sql_restore_cleanup(
            "db", {"replace": True, "recovery": False}) == []
        assert driver.sql_restore_cleanup(
            "db", {"replace": True, "recovery": True})

    def test_a_dialect_with_no_restore_has_nothing_to_reopen(self):
        assert PostgreSQLDriver.__new__(
            PostgreSQLDriver).sql_restore_cleanup("db", {"replace": True}) == []


class TestBackupAndRestoreLeaveTheSessionUsable:
    """Both hold their connection for as long as they run, and the
    session's connection is the one the REPL answers on."""

    def test_a_backup_is_handed_off(self, backup_db, captured, monkeypatch):
        from datum import background
        from datum.panels import databases

        name, _target, cursor, driver = backup_db
        handed = []
        monkeypatch.setattr(background, "is_running", lambda: True)
        monkeypatch.setattr(background, "submit",
                            lambda label, handler, args=(): handed.append(
                                (label, handler)))
        captured.clear()
        _run_backup(cursor, driver, name)
        assert handed, "the backup should have been handed off"
        assert name in handed[0][0]
        said = " ".join(str(a[0]) for k, a in captured if k == "info")
        assert "background" in said, said

    def test_the_form_is_still_checked_before_the_hand_off(self, backup_db,
                                                          captured,
                                                          monkeypatch):
        from datum import background
        from datum.panels import databases

        name, _target, cursor, driver = backup_db
        handed = []
        monkeypatch.setattr(background, "is_running", lambda: True)
        monkeypatch.setattr(background, "submit",
                            lambda *a, **k: handed.append(a))
        captured.clear()
        # A required field left empty: the user is waiting, so this is
        # answered here rather than arriving later from the worker.
        databases.run_action(cursor, driver, "do-backup", [_payload(
            {"database": name, "directory": "", "filename": ""})])
        assert "error" in _kinds(captured), captured
        assert not handed

    def test_a_restore_is_handed_off(self, backup_db, captured, monkeypatch):
        from datum import background
        from datum.panels import databases

        name, _target, cursor, driver = backup_db
        _run_backup(cursor, driver, name)
        handed = []
        monkeypatch.setattr(background, "is_running", lambda: True)
        monkeypatch.setattr(background, "submit",
                            lambda label, handler, args=(): handed.append(
                                (label, handler)))
        captured.clear()
        databases.run_action(cursor, driver, "do-restore", [_payload(
            {"database": name, "target": name,
             "source": f"/var/opt/mssql/data/{name}.bak",
             "position": 1, "replace": True, "recovery": True,
             "data_dir": "/var/opt/mssql/data",
             "log_dir": "/var/opt/mssql/data"})])
        assert handed, "the restore should have been handed off"

        # And the work still does the right thing when the worker runs it.
        class Connection:
            def __init__(self, cur): self._cur = cur
            def cursor(self): return self._cur

        handed[0][1](conn=Connection(cursor))
        assert "error" not in _kinds(captured), captured

    def test_a_preview_is_never_handed_off(self, backup_db, captured,
                                          monkeypatch):
        from datum import background
        from datum.panels import databases

        name, _target, cursor, driver = backup_db
        _run_backup(cursor, driver, name)
        handed = []
        monkeypatch.setattr(background, "is_running", lambda: True)
        monkeypatch.setattr(background, "submit",
                            lambda *a, **k: handed.append(a))
        captured.clear()
        databases.run_action(cursor, driver, "preview-restore", [_payload(
            {"database": name, "target": name,
             "source": f"/var/opt/mssql/data/{name}.bak",
             "position": 1, "replace": True, "recovery": True,
             "data_dir": "/var/opt/mssql/data",
             "log_dir": "/var/opt/mssql/data"})])
        # Nothing runs, and the SQL is what the user is waiting to read.
        assert not handed
        assert [a for k, a in captured if k == "definition"]

    def test_without_a_worker_both_still_run(self, backup_db, captured,
                                             monkeypatch):
        from datum import background

        name, _target, cursor, driver = backup_db
        monkeypatch.setattr(background, "is_running", lambda: False)
        captured.clear()
        _run_backup(cursor, driver, name)
        # Same behaviour, only blocking, which beats not working.
        assert "error" not in _kinds(captured), captured
