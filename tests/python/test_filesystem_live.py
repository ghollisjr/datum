"""Browsing the server's filesystem against the docker test servers.

Skipped when the servers are not reachable, the same as the other live
suites.
"""

import base64
import json
import os

import pytest

pyodbc = pytest.importorskip("pyodbc")
pyodbc.pooling = False

from datum.drivers.mssql import MSSQLDriver          # noqa: E402
from datum.drivers.postgres import PostgreSQLDriver  # noqa: E402

STRICT = os.environ.get("DATUM_STRICT") == "1"

PG_HOST = os.environ.get("DATUM_PG_SERVER", "127.0.0.1")
PG_PORT = os.environ.get("DATUM_PG_PORT", "5433")
MS_HOST = os.environ.get("DATUM_MSSQL_SERVER", "127.0.0.1")
MS_PORT = os.environ.get("DATUM_MSSQL_PORT", "1434")

PG_DSN = (f"Driver={{PostgreSQL Unicode}};Server={PG_HOST};Port={PG_PORT};"
          f"Database=postgres;Uid={os.environ.get('DATUM_PG_USER','postgres')};"
          f"Pwd={os.environ.get('DATUM_PG_PASS','datum_test')}")
MS_DSN = (f"Driver={{ODBC Driver 18 for SQL Server}};Server={MS_HOST},{MS_PORT};"
          f"Database=master;Uid={os.environ.get('DATUM_MSSQL_USER','sa')};"
          f"Pwd={os.environ.get('DATUM_MSSQL_PASS','DatumTest1!')};"
          f"TrustServerCertificate=yes")

_UNREACHABLE = {}


def _connect(dsn, label):
    if label in _UNREACHABLE:
        pytest.skip(_UNREACHABLE[label])
    try:
        return pyodbc.connect(dsn, timeout=8, autocommit=True)
    except Exception as err:
        _UNREACHABLE[label] = f"{label} not reachable: {err}"
        if STRICT:
            pytest.fail(_UNREACHABLE[label])
        pytest.skip(_UNREACHABLE[label])


@pytest.fixture
def captured(monkeypatch):
    from datum import envelope

    events = []
    for kind in ("info", "warn", "error", "admin_panel"):
        monkeypatch.setattr(
            envelope, kind,
            (lambda k: (lambda *a, **kw: events.append((k, a))))(kind))
    return events


@pytest.fixture
def mssql_env():
    conn = _connect(MS_DSN, "MSSQL")
    yield conn.cursor(), MSSQLDriver()
    conn.close()


@pytest.fixture
def pg_env():
    conn = _connect(PG_DSN, "PostgreSQL")
    yield conn.cursor(), PostgreSQLDriver()
    conn.close()


def _payload(obj):
    return base64.b64encode(json.dumps(obj).encode()).decode()


class TestListing:

    def test_mssql_lists_its_data_directory(self, mssql_env):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        panel = filesystem.get_data(cursor, driver, [])
        assert panel["headers"] == ["Type", "Name", "Size", "Modified", "Path"]
        # row_id is the path column, so navigation uses what the server
        # said rather than a path assembled from a guessed separator.
        assert panel["row_id"] == 4
        assert panel["rows"], panel

    def test_the_parent_is_offered_as_a_row(self, mssql_env):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        panel = filesystem.get_data(cursor, driver, ["/var/opt/mssql/log"])
        first = panel["rows"][0]
        assert first[0] == "dir" and first[1] == ".."
        assert first[4] == "/var/opt/mssql"

    def test_files_are_told_from_directories(self, mssql_env):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        panel = filesystem.get_data(cursor, driver, ["/var/opt/mssql"])
        kinds = {r[0] for r in panel["rows"]}
        assert "dir" in kinds
        listing = {r[1]: r[0] for r in panel["rows"]}
        assert listing.get("log") == "dir"

    def test_postgres_tells_its_config_files_from_its_directories(self,
                                                                  pg_env):
        from datum.panels import filesystem

        cursor, driver = pg_env
        cursor.execute("SHOW data_directory")
        data_dir = cursor.fetchone()[0]
        panel = filesystem.get_data(cursor, driver, [data_dir])
        listing = {r[1]: r[0] for r in panel["rows"]}
        # The bug this guards: the PostgreSQL ODBC driver returns
        # booleans as "0"/"1", and bool("0") is True, so every entry
        # was reported as a directory.
        assert listing.get("postgresql.conf") == "file"
        assert listing.get("PG_VERSION") == "file"
        assert listing.get("base") == "dir"

    def test_a_size_is_a_plain_number_so_the_column_sorts(self, mssql_env):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        panel = filesystem.get_data(cursor, driver, ["/var/opt/mssql/log"])
        sizes = [r[2] for r in panel["rows"] if r[0] == "file"]
        assert sizes, panel["rows"]
        assert all(s.isdigit() for s in sizes), sizes

    def test_a_directory_that_cannot_be_listed_says_so(self, mssql_env):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        panel = filesystem.get_data(cursor, driver, ["/no/such/place"])
        # Either an explicit note or simply nothing in it, but never a
        # traceback reaching the user.
        assert "rows" in panel and "info" in panel

    def test_the_panel_carries_the_path_for_refreshing(self, mssql_env):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        panel = filesystem.get_data(cursor, driver, ["/var/opt/mssql/log"])
        assert panel["context"]["path"] == "/var/opt/mssql/log"


class TestReading:

    def test_mssql_reads_a_file_it_owns(self, mssql_env, captured):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        filesystem.run_action(cursor, driver, "view",
                              [_payload({"path": "/var/opt/mssql/log/errorlog"})])
        kinds = [k for k, _ in captured]
        assert "error" not in kinds, captured
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        assert panel["sub_panel"] == "file"
        assert "Microsoft SQL Server" in panel["content"]

    def test_a_file_the_server_cannot_read_is_reported_not_raised(
            self, mssql_env, captured):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        # OPENROWSET reads as the service account, which does not own this.
        filesystem.run_action(cursor, driver, "view",
                              [_payload({"path": "/etc/shadow"})])
        kinds = [k for k, _ in captured]
        assert kinds == ["error"], captured
        assert "Cannot read" in captured[0][1][0]

    def test_postgres_reads_its_config(self, pg_env, captured):
        from datum.panels import filesystem

        cursor, driver = pg_env
        cursor.execute("SHOW data_directory")
        conf = cursor.fetchone()[0] + "/postgresql.conf"
        filesystem.run_action(cursor, driver, "view", [_payload({"path": conf})])
        kinds = [k for k, _ in captured]
        assert "error" not in kinds, captured
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        assert "PostgreSQL configuration file" in panel["content"]

    def test_a_quote_in_a_path_cannot_break_out_of_the_literal(
            self, mssql_env, captured):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        # The path is escaped into the statement rather than bound, so
        # this must fail as a missing file, not as a syntax error.
        filesystem.run_action(
            cursor, driver, "view",
            [_payload({"path": "/tmp/no'; SELECT 1--.txt"})])
        assert [k for k, _ in captured] == ["error"], captured
        assert "Incorrect syntax" not in captured[0][1][0], captured

    def test_opening_a_directory_returns_its_listing(self, mssql_env,
                                                     captured):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        filesystem.run_action(cursor, driver, "open", ["/var/opt/mssql/log"])
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        assert panel["context"]["path"] == "/var/opt/mssql/log"
        assert any(r[1] == "errorlog" for r in panel["rows"]), panel["rows"]
