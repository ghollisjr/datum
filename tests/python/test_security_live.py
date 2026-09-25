"""Live tests for the security panel.

Covers what unit tests cannot: that the generated DDL is accepted, that a
created login can actually connect, and that passwords never come back
out of the panel.

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

# Not a credential: the password these tests create a throwaway login
# with, and then drop again.  It is a literal so the suite can assert
# it never comes back out of a panel, a preview, or an edit form.
SECRET = "Str0ng!Passw0rd#42"
MS_LOGIN = "datum_sec_pytest"
PG_ROLE = "datum_sec_pytest"

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


@pytest.fixture
def mssql_env():
    conn = _connect(MS_DSN, "MSSQL")
    cursor, driver = conn.cursor(), MSSQLDriver()

    def cleanup():
        for stmt in (f"EXEC [datum_test]..sp_executesql "
                     f"N'DROP USER [{MS_LOGIN}]'",
                     f"DROP LOGIN [{MS_LOGIN}]"):
            try:
                cursor.execute(stmt)
            except Exception:
                pass

    cleanup()
    yield cursor, driver
    cleanup()
    conn.close()


@pytest.fixture
def pg_env():
    conn = _connect(PG_DSN, "PostgreSQL")
    cursor, driver = conn.cursor(), PostgreSQLDriver()

    def cleanup():
        # A role holding privileges cannot be dropped, and DROP ROLE
        # says so rather than cascading, so the grants go first.
        for stmt in (f"DROP OWNED BY {PG_ROLE}",
                     "DROP OWNED BY datum_sec_group",
                     f"DROP ROLE IF EXISTS {PG_ROLE}",
                     "DROP ROLE IF EXISTS datum_sec_group"):
            try:
                cursor.execute(stmt)
            except Exception:
                pass

    cleanup()
    yield cursor, driver
    cleanup()
    conn.close()


def _create_mssql_login(cursor, driver, **extra):
    from datum.panels import security
    opts = {"name": MS_LOGIN, "login_type": "SQL_LOGIN", "password": SECRET,
            "check_policy": True, "check_expiration": False,
            "default_database": "master", "disabled": False, "roles": []}
    opts.update(extra)
    security.run_action(cursor, driver, "create-principal", [_payload(opts)])
    # Assert here: a silent failure would otherwise surface much later as
    # an unrelated assertion about roles or state.
    assert driver.principal_settings(cursor, MS_LOGIN), \
        f"login {MS_LOGIN} was not created"
    return opts


class TestPasswordHandling:
    """A password must never come back out of the panel."""

    def test_preview_masks_the_password(self, mssql_env, captured):
        from datum.panels import security
        cursor, driver = mssql_env
        security.run_action(cursor, driver, "preview-create", [_payload(
            {"name": MS_LOGIN, "login_type": "SQL_LOGIN", "password": SECRET,
             "check_policy": True, "default_database": "master"})])
        assert _kinds(captured) == ["definition"]
        text = captured[0][1][1]
        assert SECRET not in text
        assert "********" in text

    def test_pg_preview_masks_the_password(self, pg_env, captured):
        from datum.panels import security
        cursor, driver = pg_env
        security.run_action(cursor, driver, "preview-create", [_payload(
            {"name": PG_ROLE, "password": SECRET, "can_login": True})])
        assert _kinds(captured) == ["definition"]
        assert SECRET not in captured[0][1][1]

    def test_edit_form_never_returns_the_password(self, mssql_env, captured):
        from datum.panels import security
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver)
        captured.clear()

        security.run_action(cursor, driver, "edit-principal", [MS_LOGIN])
        form = captured[0][1][0]["form"]
        assert "password" not in form["values"]
        password_field = next(f for f in form["fields"]
                              if f["key"] == "password")
        assert password_field["default"] == ""
        assert SECRET not in json.dumps(captured[0][1][0])


class TestMSSQLLogins:
    def test_created_login_can_connect(self, mssql_env, captured):
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver)
        _assert_ok(captured)

        # The real proof that the DDL was right.
        probe = pyodbc.connect(
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={MS_HOST},{MS_PORT};Database=master;"
            f"Uid={MS_LOGIN};Pwd={SECRET};TrustServerCertificate=yes",
            timeout=8)
        try:
            assert probe.cursor().execute("SELECT 1").fetchone()[0] == 1
        finally:
            probe.close()

    def test_roles_and_state_round_trip(self, mssql_env, captured):
        from datum.panels import security
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver, roles=["dbcreator"])
        captured.clear()

        settings = driver.principal_settings(cursor, MS_LOGIN)
        assert settings["roles"] == ["dbcreator"]
        assert settings["disabled"] is False

        submission = dict(settings, name_original=MS_LOGIN, password="",
                          disabled=True, roles=["processadmin"])
        security.run_action(cursor, driver, "alter-principal",
                            [_payload(submission)])
        _assert_ok(captured)

        after = driver.principal_settings(cursor, MS_LOGIN)
        assert after["disabled"] is True
        assert after["roles"] == ["processadmin"]

    def test_unchanged_form_applies_nothing(self, mssql_env, captured):
        from datum.panels import security
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver)
        settings = driver.principal_settings(cursor, MS_LOGIN)
        captured.clear()

        security.run_action(cursor, driver, "alter-principal", [_payload(
            dict(settings, name_original=MS_LOGIN, password=""))])
        _assert_ok(captured)
        assert "No changes" in captured[0][1][0]

    def test_drop_check_counts_sessions(self, mssql_env, captured):
        from datum.panels import security
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver)
        held = pyodbc.connect(
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={MS_HOST},{MS_PORT};Database=master;"
            f"Uid={MS_LOGIN};Pwd={SECRET};TrustServerCertificate=yes",
            timeout=8)
        held.cursor().execute("SELECT 1")
        captured.clear()
        try:
            security.run_action(cursor, driver, "drop-check", [MS_LOGIN])
            form = captured[0][1][0]["form"]
            assert form["confirm_text"] == MS_LOGIN
            assert form["danger"] is True
            # Force defaults on precisely because a session is held.
            assert form["fields"][0]["default"] is True
            assert any("Active sessions: 1" in n for n in form["notes"])

            captured.clear()
            security.run_action(cursor, driver, "drop-principal", [_payload(
                {"name": MS_LOGIN, "force": True})])
            _assert_ok(captured)
            assert not driver.principal_settings(cursor, MS_LOGIN)
        finally:
            try:
                held.close()
            except Exception:
                pass


class TestUserMappings:
    def test_mapping_listing_reads_each_database(self, mssql_env, captured):
        from datum.panels import security
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver)
        security.run_action(cursor, driver, "add-mapping", [_payload(
            {"login": MS_LOGIN, "database": "datum_test",
             "username": MS_LOGIN})])
        captured.clear()

        # sys.database_principals is database-scoped: a single query from
        # master reports master's users for every row.
        security.run_action(cursor, driver, "mappings", [MS_LOGIN])
        rows = {r[0]: r[1] for r in captured[0][1][0]["rows"]}
        assert rows.get("datum_test") == MS_LOGIN
        assert "master" not in rows

    def test_database_roles_are_diffed(self, mssql_env, captured):
        from datum.panels import security
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver)
        security.run_action(cursor, driver, "add-mapping", [_payload(
            {"login": MS_LOGIN, "database": "datum_test",
             "username": MS_LOGIN})])
        captured.clear()

        security.run_action(cursor, driver, "set-mapping-roles", [_payload(
            {"login": MS_LOGIN, "database": "datum_test",
             "username": MS_LOGIN,
             "roles": ["db_datareader", "db_datawriter"]})])
        _assert_ok(captured)
        assert driver.user_roles(cursor, "datum_test", MS_LOGIN) == \
            ["db_datareader", "db_datawriter"]

        captured.clear()
        security.run_action(cursor, driver, "set-mapping-roles", [_payload(
            {"login": MS_LOGIN, "database": "datum_test",
             "username": MS_LOGIN, "roles": ["db_datareader"]})])
        assert driver.user_roles(cursor, "datum_test", MS_LOGIN) == \
            ["db_datareader"]

        captured.clear()
        security.run_action(cursor, driver, "set-mapping-roles", [_payload(
            {"login": MS_LOGIN, "database": "datum_test",
             "username": MS_LOGIN, "roles": ["db_datareader"]})])
        assert "No role changes" in captured[0][1][0]

    def test_remove_mapping(self, mssql_env, captured):
        from datum.panels import security
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver)
        security.run_action(cursor, driver, "add-mapping", [_payload(
            {"login": MS_LOGIN, "database": "datum_test",
             "username": MS_LOGIN})])
        captured.clear()

        security.run_action(cursor, driver, "remove-mapping", [_payload(
            {"login": MS_LOGIN, "database": "datum_test"})])
        _assert_ok(captured)
        _headers, rows = driver.list_user_mappings(cursor, MS_LOGIN)
        assert not any(r[0] == "datum_test" for r in rows)


class TestPostgresRoles:
    def test_created_role_can_connect(self, pg_env, captured):
        from datum.panels import security
        cursor, driver = pg_env
        security.run_action(cursor, driver, "create-principal", [_payload(
            {"name": PG_ROLE, "password": SECRET, "can_login": True,
             "superuser": False, "create_db": True, "create_role": False,
             "inherit": True, "replication": False,
             "connection_limit": 5, "valid_until": "", "roles": []})])
        _assert_ok(captured)

        probe = pyodbc.connect(
            f"Driver={{PostgreSQL Unicode}};Server={PG_HOST};Port={PG_PORT};"
            f"Database=postgres;Uid={PG_ROLE};Pwd={SECRET}", timeout=8)
        try:
            assert probe.cursor().execute("SELECT 1").fetchone()[0] == 1
        finally:
            probe.close()

        settings = driver.principal_settings(cursor, PG_ROLE)
        assert settings["create_db"] is True
        assert settings["connection_limit"] == 5

    def test_membership_round_trips(self, pg_env, captured):
        from datum.panels import security
        cursor, driver = pg_env
        cursor.execute("CREATE ROLE datum_sec_group")
        security.run_action(cursor, driver, "create-principal", [_payload(
            {"name": PG_ROLE, "password": SECRET, "can_login": True,
             "inherit": True, "roles": ["datum_sec_group"]})])
        assert driver.principal_settings(cursor, PG_ROLE)["roles"] == \
            ["datum_sec_group"]
        captured.clear()

        settings = driver.principal_settings(cursor, PG_ROLE)
        security.run_action(cursor, driver, "alter-principal", [_payload(
            dict(settings, name_original=PG_ROLE, password="", roles=[]))])
        _assert_ok(captured)
        assert driver.principal_settings(cursor, PG_ROLE)["roles"] == []

    def test_no_separate_database_users(self, pg_env, captured):
        from datum.panels import security
        cursor, driver = pg_env
        security.run_action(cursor, driver, "mappings", [PG_ROLE])
        panel = captured[0][1][0]
        assert "no separate database users" in panel["rows"][0][0]

    def test_list_has_no_mapping_action(self, pg_env):
        from datum.panels import security
        cursor, driver = pg_env
        commands = {a["command"]
                    for a in security.get_data(cursor, driver, [])["actions"]}
        assert "mappings" not in commands


class TestOwnershipIsVisible:
    """Owning a database makes a login dbo inside it, with full control,
    without holding any server role.  A login list that showed only
    server roles therefore read as unprivileged when it was not."""

    def test_public_only_login_cannot_create_tables(self, mssql_env,
                                                    captured):
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver)
        from datum.panels import security
        security.run_action(cursor, driver, "add-mapping", [_payload(
            {"login": MS_LOGIN, "database": "datum_test",
             "username": MS_LOGIN})])

        probe = pyodbc.connect(
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={MS_HOST},{MS_PORT};Database=datum_test;"
            f"Uid={MS_LOGIN};Pwd={SECRET};TrustServerCertificate=yes",
            autocommit=True, timeout=8)
        try:
            cur = probe.cursor()
            assert cur.execute("SELECT IS_MEMBER('db_owner')").fetchone()[0] == 0
            with pytest.raises(Exception):
                cur.execute("CREATE TABLE dbo.should_not_exist (id int)")
        finally:
            probe.close()

    def test_owning_a_database_is_listed(self, mssql_env, captured):
        from datum.panels import databases, security
        cursor, driver = mssql_env
        owned = "datum_sec_owned"

        def drop_db():
            try:
                for stmt in driver.sql_drop_database(owned, force=True):
                    cursor.execute(stmt)
            except Exception:
                pass

        drop_db()
        _create_mssql_login(cursor, driver)
        databases.run_action(cursor, driver, "create-database",
                             [_payload({"name": owned, "owner": MS_LOGIN})])
        try:
            headers, rows = driver.list_principals(cursor)
            owns = headers.index("Owns (dbo)")
            roles = headers.index("Server Roles")
            row = next(r for r in rows if r[0] == MS_LOGIN)
            # No server role at all, yet dbo of a database.
            assert row[roles] == ""
            assert owned in row[owns]

            # And the mapping view shows it as dbo/db_owner.
            captured.clear()
            security.run_action(cursor, driver, "mappings", [MS_LOGIN])
            mapped = {r[0]: (r[1], r[2])
                      for r in captured[0][1][0]["rows"]}
            assert mapped[owned] == ("dbo", "db_owner")
        finally:
            drop_db()

    def test_owner_field_warns_in_both_forms(self, mssql_env):
        _cursor, driver = mssql_env
        for specs in (driver.database_options(), driver.settings_options(
                None, {})):
            owner = next(f for f in specs if f["key"] == "owner")
            assert "full control" in (owner.get("help") or "")


class TestMutationsRefreshThePanel:
    """Sub-panels carry no auto-refresh timer, so a mutating action has
    to re-send the affected panel.  Without this a new mapping stayed
    invisible until the buffer was closed and reopened."""

    def test_add_mapping_resends_the_mapping_panel(self, mssql_env,
                                                   captured):
        from datum.panels import security
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver)
        captured.clear()

        security.run_action(cursor, driver, "add-mapping", [_payload(
            {"login": MS_LOGIN, "database": "datum_test",
             "username": MS_LOGIN})])
        _assert_ok(captured, refreshes="user-mappings")
        panel = [a[0] for k, a in captured if k == "admin_panel"][-1]
        # The resent panel must already contain the new mapping.
        assert any(r[0] == "datum_test" and r[1] == MS_LOGIN
                   for r in panel["rows"])

    def test_role_change_resends_the_mapping_panel(self, mssql_env,
                                                   captured):
        from datum.panels import security
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver)
        security.run_action(cursor, driver, "add-mapping", [_payload(
            {"login": MS_LOGIN, "database": "datum_test",
             "username": MS_LOGIN})])
        captured.clear()

        security.run_action(cursor, driver, "set-mapping-roles", [_payload(
            {"login": MS_LOGIN, "database": "datum_test",
             "username": MS_LOGIN, "roles": ["db_datareader"]})])
        _assert_ok(captured, refreshes="user-mappings")
        panel = [a[0] for k, a in captured if k == "admin_panel"][-1]
        assert any("db_datareader" in r[2] for r in panel["rows"])

    def test_remove_mapping_resends_the_mapping_panel(self, mssql_env,
                                                      captured):
        from datum.panels import security
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver)
        security.run_action(cursor, driver, "add-mapping", [_payload(
            {"login": MS_LOGIN, "database": "datum_test",
             "username": MS_LOGIN})])
        captured.clear()

        security.run_action(cursor, driver, "remove-mapping", [_payload(
            {"login": MS_LOGIN, "database": "datum_test"})])
        _assert_ok(captured, refreshes="user-mappings")
        panel = [a[0] for k, a in captured if k == "admin_panel"][-1]
        assert not any(r[0] == "datum_test" for r in panel["rows"])

    def test_create_and_drop_resend_the_login_list(self, mssql_env,
                                                  captured):
        from datum.panels import security
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver)
        _assert_ok(captured, refreshes="")
        panel = [a[0] for k, a in captured if k == "admin_panel"][-1]
        assert any(r[0] == MS_LOGIN for r in panel["rows"])

        captured.clear()
        security.run_action(cursor, driver, "drop-principal",
                            [_payload({"name": MS_LOGIN, "force": True})])
        _assert_ok(captured, refreshes="")
        panel = [a[0] for k, a in captured if k == "admin_panel"][-1]
        assert not any(r[0] == MS_LOGIN for r in panel["rows"])

    def test_a_refresh_failure_does_not_mask_success(self, mssql_env,
                                                     captured, monkeypatch):
        from datum.panels import security
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver)
        captured.clear()

        monkeypatch.setattr(
            driver, "list_user_mappings",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
        security.run_action(cursor, driver, "add-mapping", [_payload(
            {"login": MS_LOGIN, "database": "datum_test",
             "username": MS_LOGIN})])
        # The mapping was made; only the redraw failed.
        assert "error" not in _kinds(captured), captured
        assert _kinds(captured)[0] == "info"


class TestObjectPermissions:
    """Granting, denying and revoking against the real servers."""

    # pg_env connects to the maintenance database, which has none of the
    # sample tables, so these make their own to grant on.
    PG_TABLE = "public.datum_perm_pytest"

    @pytest.fixture
    def pg_table(self, pg_env):
        cursor, _driver = pg_env
        cursor.execute(f"DROP TABLE IF EXISTS {self.PG_TABLE}")
        cursor.execute(f"CREATE TABLE {self.PG_TABLE} (id int, email text)")
        yield self.PG_TABLE
        cursor.execute(f"DROP TABLE IF EXISTS {self.PG_TABLE}")

    def _perm_rows(self, cursor, driver, principal, database=None):
        return driver.list_permissions(cursor, principal, database)[1]

    def test_mssql_grant_deny_and_revoke_on_an_object(self, mssql_env,
                                                      captured):
        cursor, driver = mssql_env
        from datum.panels import security
        _create_mssql_login(cursor, driver)
        security.run_action(cursor, driver, "add-mapping",
                            [_payload({"login": MS_LOGIN,
                                       "database": "datum_test"})])
        captured.clear()

        security.run_action(cursor, driver, "set-permissions", [_payload({
            "principal": MS_LOGIN, "database": "datum_test",
            "scope": "object", "securable": "dbo.customers",
            "granted": ["SELECT", "UPDATE"], "denied": ["DELETE"]})])
        _assert_ok(captured, refreshes="permissions")

        held = {(r[1], r[0]) for r in self._perm_rows(
            cursor, driver, MS_LOGIN, "datum_test") if r[3] == "dbo.customers"}
        assert ("SELECT", "GRANT") in held
        assert ("UPDATE", "GRANT") in held
        assert ("DELETE", "DENY") in held

        # Unticking a permission has to take it away, not leave it.
        captured.clear()
        security.run_action(cursor, driver, "set-permissions", [_payload({
            "principal": MS_LOGIN, "database": "datum_test",
            "scope": "object", "securable": "dbo.customers",
            "granted": ["SELECT"], "denied": []})])
        _assert_ok(captured, refreshes="permissions")
        held = {r[1] for r in self._perm_rows(
            cursor, driver, MS_LOGIN, "datum_test") if r[3] == "dbo.customers"}
        assert held == {"SELECT"}

    def test_mssql_reapplying_the_same_set_changes_nothing(self, mssql_env,
                                                           captured):
        cursor, driver = mssql_env
        from datum.panels import security
        _create_mssql_login(cursor, driver)
        security.run_action(cursor, driver, "add-mapping",
                            [_payload({"login": MS_LOGIN,
                                       "database": "datum_test"})])
        opts = {"principal": MS_LOGIN, "database": "datum_test",
                "scope": "schema", "securable": "dbo",
                "granted": ["SELECT"], "denied": []}
        security.run_action(cursor, driver, "set-permissions", [_payload(opts)])
        captured.clear()
        security.run_action(cursor, driver, "set-permissions", [_payload(opts)])
        messages = [a[0] for k, a in captured if k == "info"]
        assert any("No permission changes" in m for m in messages), captured

    def test_mssql_column_grant_is_separate_from_the_table(self, mssql_env):
        cursor, driver = mssql_env
        from datum.panels import security
        _create_mssql_login(cursor, driver)
        security.run_action(cursor, driver, "add-mapping",
                            [_payload({"login": MS_LOGIN,
                                       "database": "datum_test"})])
        for sql in driver.sql_set_permissions(
                MS_LOGIN, "column", "dbo.customers", ["SELECT"], [], {},
                database="datum_test", column="email"):
            cursor.execute(sql)
        rows = self._perm_rows(cursor, driver, MS_LOGIN, "datum_test")
        assert ["GRANT", "SELECT", "Object", "dbo.customers", "email"] in rows
        # A column grant is not a grant on the whole object.
        assert driver.current_permissions(
            cursor, MS_LOGIN, "object", "dbo.customers",
            database="datum_test") == {}

    def test_mssql_server_permissions_are_listed_separately(self, mssql_env):
        cursor, driver = mssql_env
        _create_mssql_login(cursor, driver)
        cursor.execute(f"GRANT VIEW SERVER STATE TO [{MS_LOGIN}]")
        rows = self._perm_rows(cursor, driver, MS_LOGIN)
        assert any(r[1] == "VIEW SERVER STATE" and r[2] == "Server"
                   for r in rows), rows

    def test_mssql_revoking_one_row_leaves_the_others(self, mssql_env,
                                                      captured):
        cursor, driver = mssql_env
        from datum.panels import security
        _create_mssql_login(cursor, driver)
        security.run_action(cursor, driver, "add-mapping",
                            [_payload({"login": MS_LOGIN,
                                       "database": "datum_test"})])
        security.run_action(cursor, driver, "set-permissions", [_payload({
            "principal": MS_LOGIN, "database": "datum_test",
            "scope": "object", "securable": "dbo.customers",
            "granted": ["SELECT", "UPDATE"], "denied": []})])
        captured.clear()
        security.run_action(cursor, driver, "revoke-permission", [_payload({
            "principal": MS_LOGIN, "database": "datum_test",
            "scope": "Object", "securable": "dbo.customers",
            "column": "", "permission": "UPDATE"})])
        _assert_ok(captured, refreshes="permissions")
        held = {r[1] for r in self._perm_rows(
            cursor, driver, MS_LOGIN, "datum_test") if r[3] == "dbo.customers"}
        assert held == {"SELECT"}

    def test_pg_grant_and_revoke_on_a_table(self, pg_env, pg_table, captured):
        cursor, driver = pg_env
        from datum.panels import security
        cursor.execute(f"CREATE ROLE {PG_ROLE}")
        captured.clear()
        security.run_action(cursor, driver, "set-permissions", [_payload({
            "principal": PG_ROLE, "scope": "object",
            "securable": pg_table,
            "granted": ["SELECT", "INSERT"], "denied": []})])
        _assert_ok(captured, refreshes="permissions")
        held = {r[1] for r in self._perm_rows(cursor, driver, PG_ROLE)
                if r[3] == pg_table}
        assert held == {"SELECT", "INSERT"}

        captured.clear()
        security.run_action(cursor, driver, "revoke-permission", [_payload({
            "principal": PG_ROLE, "scope": "Object",
            "securable": pg_table, "column": "",
            "permission": "INSERT"})])
        _assert_ok(captured, refreshes="permissions")
        held = {r[1] for r in self._perm_rows(cursor, driver, PG_ROLE)
                if r[3] == pg_table}
        assert held == {"SELECT"}

    def test_pg_schema_and_database_scopes(self, pg_env):
        cursor, driver = pg_env
        cursor.execute(f"CREATE ROLE {PG_ROLE}")
        for sql in driver.sql_set_permissions(
                PG_ROLE, "schema", "public", ["USAGE"], [], {}):
            cursor.execute(sql)
        cursor.execute("SELECT current_database()")
        database = cursor.fetchone()[0]
        for sql in driver.sql_set_permissions(
                PG_ROLE, "database", database, ["CONNECT"], [], {}):
            cursor.execute(sql)
        rows = self._perm_rows(cursor, driver, PG_ROLE)
        assert ["GRANT", "USAGE", "Schema", "public", ""] in rows
        assert any(r[2] == "Database" and r[1] == "CONNECT" for r in rows)

    def test_pg_a_role_with_nothing_granted_lists_nothing(self, pg_env):
        cursor, driver = pg_env
        cursor.execute(f"CREATE ROLE {PG_ROLE}")
        assert self._perm_rows(cursor, driver, PG_ROLE) == []

    def test_the_form_opens_ticked_from_the_server(self, mssql_env, captured):
        cursor, driver = mssql_env
        from datum.panels import security
        _create_mssql_login(cursor, driver)
        security.run_action(cursor, driver, "add-mapping",
                            [_payload({"login": MS_LOGIN,
                                       "database": "datum_test"})])
        security.run_action(cursor, driver, "set-permissions", [_payload({
            "principal": MS_LOGIN, "database": "datum_test",
            "scope": "object", "securable": "dbo.customers",
            "granted": ["SELECT"], "denied": ["DELETE"]})])
        captured.clear()
        security.run_action(cursor, driver, "permission-edit", [_payload({
            "principal": MS_LOGIN, "database": "datum_test",
            "scope": "object", "securable": "dbo.customers"})])
        form = [a[0] for k, a in captured if k == "admin_panel"][0]["form"]
        by_key = {f["key"]: f for f in form["fields"]}
        assert by_key["granted"]["default"] == ["SELECT"]
        assert by_key["denied"]["default"] == ["DELETE"]
