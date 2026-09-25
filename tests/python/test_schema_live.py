"""Live tests for the schema panel.

Covers what unit tests cannot: that the generated DDL is accepted, that
the created table has the columns and key asked for, and that the panel
re-draws after a change.

Requires the docker test databases:

    docker compose up -d postgres mssql
"""

import base64
import json
import os

import pytest

pyodbc = pytest.importorskip("pyodbc")

# Closing a connection returns it to pyodbc's pool rather than
# disconnecting, which leaks state between tests.
pyodbc.pooling = False

from datum.drivers.mssql import MSSQLDriver          # noqa: E402
from datum.drivers.postgres import PostgreSQLDriver  # noqa: E402

STRICT = os.environ.get("DATUM_STRICT") == "1"
SCHEMA = "datum_sch_pytest"
TABLE = "datum_tbl_pytest"

PG_DSN = ("Driver={{PostgreSQL Unicode}};Server={h};Port={p};"
          "Database=datum_test;Uid={u};Pwd={w}").format(
              h=os.environ.get("DATUM_PG_SERVER", "127.0.0.1"),
              p=os.environ.get("DATUM_PG_PORT", "5433"),
              u=os.environ.get("DATUM_PG_USER", "postgres"),
              w=os.environ.get("DATUM_PG_PASS", "datum_test"))
MS_DSN = ("Driver={{ODBC Driver 18 for SQL Server}};Server={h},{p};"
          "Database=datum_test;Uid={u};Pwd={w};"
          "TrustServerCertificate=yes").format(
              h=os.environ.get("DATUM_MSSQL_SERVER", "127.0.0.1"),
              p=os.environ.get("DATUM_MSSQL_PORT", "1434"),
              u=os.environ.get("DATUM_MSSQL_USER", "sa"),
              w=os.environ.get("DATUM_MSSQL_PASS", "DatumTest1!"))

_UNREACHABLE = {}


def _connect(dsn, label):
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
    from datum import envelope

    events = []
    for kind in ("info", "warn", "error", "admin_panel", "definition"):
        monkeypatch.setattr(
            envelope, kind,
            (lambda k: (lambda *a, **kw: events.append((k, a))))(kind))
    return events


def _kinds(events):
    return [k for k, _ in events]


def _payload(obj):
    return base64.b64encode(json.dumps(obj).encode()).decode()


def _assert_ok(events, refreshes=None):
    kinds = _kinds(events)
    assert "error" not in kinds, events
    assert kinds[0] == "info", events
    if refreshes is not None:
        panels = [a[0] for k, a in events if k == "admin_panel"]
        assert panels, f"expected a {refreshes} refresh, got {kinds}"
        assert panels[-1].get("sub_panel") == refreshes or \
            (refreshes == "" and not panels[-1].get("sub_panel")), panels[-1]


@pytest.fixture(params=["pg", "mssql"])
def env(request):
    dialect = request.param
    if dialect == "pg":
        conn, driver = _connect(PG_DSN, "PostgreSQL"), PostgreSQLDriver()
    else:
        conn, driver = _connect(MS_DSN, "MSSQL"), MSSQLDriver()
    cursor = conn.cursor()

    def cleanup():
        for stmt in ([f'DROP TABLE "{SCHEMA}"."{TABLE}"',
                      f'DROP SCHEMA "{SCHEMA}" CASCADE']
                     if dialect == "pg" else
                     [f"DROP TABLE [{SCHEMA}].[{TABLE}]",
                      f"DROP SCHEMA [{SCHEMA}]"]):
            try:
                cursor.execute(stmt)
            except Exception:
                pass

    cleanup()
    yield dialect, cursor, driver
    cleanup()
    conn.close()


def _columns(dialect):
    if dialect == "mssql":
        return [["id", "INT IDENTITY(1,1)", False, True, ""],
                ["label", "NVARCHAR(255)", False, False, ""],
                ["amt", "DECIMAL(18,2)", True, False, "0"]]
    return [["id", "BIGSERIAL", False, True, ""],
            ["label", "TEXT", False, False, ""],
            ["amt", "NUMERIC(18,2)", True, False, "0"]]


class TestSchemas:
    def test_list_and_create(self, env, captured):
        from datum.panels import schema
        _dialect, cursor, driver = env
        result = schema.get_data(cursor, driver, [])
        assert result["headers"][0] == "Schema"
        assert {a["command"] for a in result["actions"]} == \
            {"new-schema", "tables", "drop-schema"}

        schema.run_action(cursor, driver, "create-schema",
                          [_payload({"name": SCHEMA})])
        _assert_ok(captured, refreshes="")
        panel = [a[0] for k, a in captured if k == "admin_panel"][-1]
        assert any(r[0] == SCHEMA for r in panel["rows"])

    def test_drop_check_counts_tables(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        schema.run_action(cursor, driver, "create-schema",
                          [_payload({"name": SCHEMA})])
        schema.run_action(cursor, driver, "create-table", [_payload(
            {"schema": SCHEMA, "name": TABLE,
             "columns": _columns(dialect)})])
        captured.clear()

        schema.run_action(cursor, driver, "drop-schema", [SCHEMA])
        form = captured[0][1][0]["form"]
        assert form["confirm_text"] == SCHEMA
        assert any("Tables: 1" in n for n in form["notes"])
        # Only PostgreSQL has CASCADE to offer.
        keys = {f["key"] for f in form["fields"]}
        assert ("cascade" in keys) == (dialect == "pg")


class TestTables:
    def test_build_a_table(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        schema.run_action(cursor, driver, "create-schema",
                          [_payload({"name": SCHEMA})])
        captured.clear()

        schema.run_action(cursor, driver, "create-table", [_payload(
            {"schema": SCHEMA, "name": TABLE,
             "columns": _columns(dialect)})])
        _assert_ok(captured, refreshes="tables")
        panel = [a[0] for k, a in captured if k == "admin_panel"][-1]
        assert any(r[0] == TABLE for r in panel["rows"])

        # The columns and the key must match what was asked for.
        cursor.execute(
            "SELECT column_name, is_nullable FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position", [SCHEMA, TABLE])
        cols = {r[0]: r[1] for r in cursor.fetchall()}
        assert list(cols) == ["id", "label", "amt"]
        assert cols["id"] == "NO" and cols["amt"] == "YES"

        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.table_constraints "
            "WHERE table_schema = ? AND table_name = ? "
            "AND constraint_type = 'PRIMARY KEY'", [SCHEMA, TABLE])
        assert cursor.fetchone()[0] == 1

    def test_preview_does_not_create(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        schema.run_action(cursor, driver, "create-schema",
                          [_payload({"name": SCHEMA})])
        captured.clear()

        schema.run_action(cursor, driver, "preview-table", [_payload(
            {"schema": SCHEMA, "name": TABLE,
             "columns": _columns(dialect)})])
        assert _kinds(captured) == ["definition"]
        assert "CREATE TABLE" in captured[0][1][1]
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?", [SCHEMA, TABLE])
        assert cursor.fetchone()[0] == 0

    def test_drop_table_refreshes(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        schema.run_action(cursor, driver, "create-schema",
                          [_payload({"name": SCHEMA})])
        schema.run_action(cursor, driver, "create-table", [_payload(
            {"schema": SCHEMA, "name": TABLE,
             "columns": _columns(dialect)})])
        captured.clear()

        schema.run_action(cursor, driver, "drop-table", [_payload(
            {"schema": SCHEMA, "table": TABLE})])
        _assert_ok(captured, refreshes="tables")
        panel = [a[0] for k, a in captured if k == "admin_panel"][-1]
        assert not any(r[0] == TABLE for r in panel["rows"])

    def test_injection_in_a_default_is_refused(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        schema.run_action(cursor, driver, "create-schema",
                          [_payload({"name": SCHEMA})])
        captured.clear()

        columns = _columns(dialect)
        columns[2][4] = "0); DROP TABLE x --"
        schema.run_action(cursor, driver, "create-table", [_payload(
            {"schema": SCHEMA, "name": TABLE, "columns": columns})])
        assert _kinds(captured) == ["error"]
        assert "DEFAULT" in captured[0][1][0]


class TestAlterTable:
    """The editor reads the current columns back and diffs against them,
    so the round trip has to be exact or every column looks changed."""

    def test_reading_columns_back_shows_no_change(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        schema.run_action(cursor, driver, "create-schema",
                          [_payload({"name": SCHEMA})])
        schema.run_action(cursor, driver, "create-table", [_payload(
            {"schema": SCHEMA, "name": TABLE, "columns": _columns(dialect)})])
        captured.clear()

        current = driver.list_table_columns(cursor, SCHEMA, TABLE)
        assert [r[0] for r in current] == ["id", "label", "amt"]
        # Submitting exactly what was read must produce no statements.
        assert driver.sql_alter_table(SCHEMA, TABLE,
                                      {"columns": current}, current) == []
        schema.run_action(cursor, driver, "alter-table", [_payload(
            {"schema": SCHEMA, "table": TABLE, "columns": current})])
        assert "No changes" in captured[0][1][0]

    def test_editor_is_prefilled_and_drops_the_name_field(self, env,
                                                          captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        schema.run_action(cursor, driver, "create-schema",
                          [_payload({"name": SCHEMA})])
        schema.run_action(cursor, driver, "create-table", [_payload(
            {"schema": SCHEMA, "name": TABLE, "columns": _columns(dialect)})])
        captured.clear()

        schema.run_action(cursor, driver, "edit-table", [_payload(
            {"schema": SCHEMA, "table": TABLE})])
        form = captured[0][1][0]["form"]
        keys = {f["key"] for f in form["fields"]}
        # Renaming cannot be expressed as a column diff, so it is absent.
        assert keys == {"columns"}
        columns = next(f for f in form["fields"]
                       if f["key"] == "columns")["default"]
        assert [r[0] for r in columns] == ["id", "label", "amt"]

    def test_widen_and_add_preserve_data(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        schema.run_action(cursor, driver, "create-schema",
                          [_payload({"name": SCHEMA})])
        schema.run_action(cursor, driver, "create-table", [_payload(
            {"schema": SCHEMA, "name": TABLE, "columns": _columns(dialect)})])
        quoted = (f'"{SCHEMA}"."{TABLE}"' if dialect == "pg"
                  else f"[{SCHEMA}].[{TABLE}]")
        cursor.execute(f"INSERT INTO {quoted} (label) VALUES ('keepme')")
        captured.clear()

        columns = driver.list_table_columns(cursor, SCHEMA, TABLE)
        columns[1][1] = "NVARCHAR(MAX)" if dialect == "mssql" else "TEXT"
        columns.append(["note", "VARCHAR(50)", True, False, "'n/a'"])
        schema.run_action(cursor, driver, "alter-table", [_payload(
            {"schema": SCHEMA, "table": TABLE, "columns": columns})])
        _assert_ok(captured, refreshes="tables")

        cursor.execute(f"SELECT label FROM {quoted}")
        assert [r[0] for r in cursor.fetchall()] == ["keepme"]
        after = {r[0] for r in driver.list_table_columns(cursor, SCHEMA,
                                                         TABLE)}
        assert "note" in after

    def test_dropping_a_column_asks_first(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        schema.run_action(cursor, driver, "create-schema",
                          [_payload({"name": SCHEMA})])
        schema.run_action(cursor, driver, "create-table", [_payload(
            {"schema": SCHEMA, "name": TABLE, "columns": _columns(dialect)})])
        captured.clear()

        columns = [r for r in driver.list_table_columns(cursor, SCHEMA, TABLE)
                   if r[0] != "amt"]
        schema.run_action(cursor, driver, "alter-table", [_payload(
            {"schema": SCHEMA, "table": TABLE, "columns": columns})])

        # Nothing may run yet: dropping a column destroys its data.
        assert _kinds(captured) == ["admin_panel"]
        form = captured[0][1][0]["form"]
        assert form["confirm_text"] == TABLE
        assert form["danger"] is True
        assert any("DROPPED" in n for n in form["notes"])
        assert "amt" in {r[0] for r in driver.list_table_columns(
            cursor, SCHEMA, TABLE)}

        captured.clear()
        schema.run_action(cursor, driver, "confirm-alter-table",
                          [_payload(dict(form["values"]))])
        _assert_ok(captured, refreshes="tables")
        assert "amt" not in {r[0] for r in driver.list_table_columns(
            cursor, SCHEMA, TABLE)}

    def test_preview_does_not_alter(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        schema.run_action(cursor, driver, "create-schema",
                          [_payload({"name": SCHEMA})])
        schema.run_action(cursor, driver, "create-table", [_payload(
            {"schema": SCHEMA, "name": TABLE, "columns": _columns(dialect)})])
        captured.clear()

        columns = driver.list_table_columns(cursor, SCHEMA, TABLE)
        columns.append(["extra", "VARCHAR(50)", True, False, ""])
        schema.run_action(cursor, driver, "preview-alter-table", [_payload(
            {"schema": SCHEMA, "table": TABLE, "columns": columns})])
        assert _kinds(captured) == ["definition"]
        assert "extra" not in {r[0] for r in driver.list_table_columns(
            cursor, SCHEMA, TABLE)}


class TestRenameThroughTheEditor:
    """Editing a name in the column list renames the column and keeps
    its data; removing a row and inserting another does not."""

    def _table(self, cursor, driver, dialect):
        from datum.panels import schema
        schema.run_action(cursor, driver, "create-schema",
                          [_payload({"name": SCHEMA})])
        schema.run_action(cursor, driver, "create-table", [_payload(
            {"schema": SCHEMA, "name": TABLE, "columns": _columns(dialect)})])
        quoted = (f'"{SCHEMA}"."{TABLE}"' if dialect == "pg"
                  else f"[{SCHEMA}].[{TABLE}]")
        cursor.execute(f"INSERT INTO {quoted} (label) VALUES ('keepme')")
        return quoted

    def _rows(self, driver, cursor):
        """Rows as the editor submits them: each carrying its original."""
        return [r + [r[0]]
                for r in driver.list_table_columns(cursor, SCHEMA, TABLE)]

    def test_the_editor_asks_for_identity_tracking(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        self._table(cursor, driver, dialect)
        captured.clear()

        schema.run_action(cursor, driver, "edit-table", [_payload(
            {"schema": SCHEMA, "table": TABLE})])
        columns = next(f for f in captured[0][1][0]["form"]["fields"]
                       if f["key"] == "columns")
        assert columns.get("track_identity") is True

    def test_editing_a_name_keeps_the_data(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        quoted = self._table(cursor, driver, dialect)
        rows = self._rows(driver, cursor)
        rows[1][0] = "title"
        captured.clear()

        schema.run_action(cursor, driver, "alter-table", [_payload(
            {"schema": SCHEMA, "table": TABLE, "columns": rows})])
        # No confirmation: nothing is being dropped.
        _assert_ok(captured, refreshes="tables")

        names = [r[0] for r in driver.list_table_columns(cursor, SCHEMA,
                                                         TABLE)]
        assert "title" in names and "label" not in names
        cursor.execute(f"SELECT title FROM {quoted}")
        assert [r[0] for r in cursor.fetchall()] == ["keepme"]

    def test_removing_and_inserting_still_drops(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        self._table(cursor, driver, dialect)
        kind = "INT" if dialect == "mssql" else "INTEGER"
        rows = [r for r in self._rows(driver, cursor) if r[0] != "label"]
        rows.append(["fresh", kind, True, False, "", ""])
        captured.clear()

        schema.run_action(cursor, driver, "alter-table", [_payload(
            {"schema": SCHEMA, "table": TABLE, "columns": rows})])
        # Data is being destroyed, so it asks first.
        assert _kinds(captured) == ["admin_panel"]
        form = captured[0][1][0]["form"]
        assert form["confirm_text"] == TABLE

        captured.clear()
        schema.run_action(cursor, driver, "confirm-alter-table",
                          [_payload(dict(form["values"]))])
        _assert_ok(captured, refreshes="tables")
        names = [r[0] for r in driver.list_table_columns(cursor, SCHEMA,
                                                         TABLE)]
        assert "fresh" in names and "label" not in names

    def test_renaming_and_retyping_at_once(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        quoted = self._table(cursor, driver, dialect)
        rows = self._rows(driver, cursor)
        rows[1][0] = "title"
        rows[1][1] = "NVARCHAR(MAX)" if dialect == "mssql" else "TEXT"
        captured.clear()

        schema.run_action(cursor, driver, "alter-table", [_payload(
            {"schema": SCHEMA, "table": TABLE, "columns": rows})])
        _assert_ok(captured, refreshes="tables")
        after = {r[0]: r[1] for r in driver.list_table_columns(
            cursor, SCHEMA, TABLE)}
        assert "title" in after
        cursor.execute(f"SELECT title FROM {quoted}")
        assert [r[0] for r in cursor.fetchall()] == ["keepme"]


class TestRenameGuidance:
    """Renaming in the column editor reads as a drop and an add, so both
    the editor and the confirmation name the action that does it
    properly."""

    def _table(self, cursor, driver, dialect):
        from datum.panels import schema
        schema.run_action(cursor, driver, "create-schema",
                          [_payload({"name": SCHEMA})])
        schema.run_action(cursor, driver, "create-table", [_payload(
            {"schema": SCHEMA, "name": TABLE, "columns": _columns(dialect)})])

    def test_the_editor_points_at_the_rename_action(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        self._table(cursor, driver, dialect)
        captured.clear()

        schema.run_action(cursor, driver, "edit-table", [_payload(
            {"schema": SCHEMA, "table": TABLE})])
        notes = " ".join(captured[0][1][0]["form"]["notes"])
        assert "renames the column" in notes

    def test_a_rename_attempt_is_recognised_in_the_confirmation(self, env,
                                                                captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        self._table(cursor, driver, dialect)
        columns = driver.list_table_columns(cursor, SCHEMA, TABLE)
        columns[1][0] = "renamed"
        captured.clear()

        schema.run_action(cursor, driver, "alter-table", [_payload(
            {"schema": SCHEMA, "table": TABLE, "columns": columns})])
        notes = " ".join(captured[0][1][0]["form"]["notes"])
        assert "DROPPED" in notes
        assert "rename" in notes.lower()

    def test_a_plain_drop_does_not_mention_renaming(self, env, captured):
        from datum.panels import schema
        dialect, cursor, driver = env
        self._table(cursor, driver, dialect)
        # Removing a column without adding one is not a rename.
        columns = [r for r in driver.list_table_columns(cursor, SCHEMA, TABLE)
                   if r[0] != "amt"]
        captured.clear()

        schema.run_action(cursor, driver, "alter-table", [_payload(
            {"schema": SCHEMA, "table": TABLE, "columns": columns})])
        notes = " ".join(captured[0][1][0]["form"]["notes"])
        assert "DROPPED" in notes
        assert "rename" not in notes.lower()
