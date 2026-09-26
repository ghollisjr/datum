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

    def test_only_the_directory_itself_is_listed(self, mssql_env):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        # sys.dm_os_enumerate_filesystem walks the whole subtree and
        # reports each entry's depth; without filtering on it the panel
        # showed everything underneath rather than one directory.
        # /var/opt/mssql has subdirectories that themselves hold files.
        listed = "/var/opt/mssql"
        entries = driver.browse_path(cursor, listed)
        assert entries, "nothing listed"
        assert any(e["is_dir"] for e in entries), "no subdirectory to recurse into"
        for entry in entries:
            parent = driver.parent_path(entry["path"])
            assert parent == listed, (
                f"{entry['path']} is not directly in {listed}")

    def test_a_deeper_directory_lists_its_own_children(self, mssql_env):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        entries = driver.browse_path(cursor, "/var/opt/mssql/data")
        for entry in entries:
            assert driver.parent_path(entry["path"]) == "/var/opt/mssql/data"

    def test_postgres_lists_one_directory_too(self, pg_env):
        cursor, driver = pg_env
        cursor.execute("SHOW data_directory")
        data_dir = cursor.fetchone()[0]
        for entry in driver.browse_path(cursor, data_dir):
            assert driver.parent_path(entry["path"]) == data_dir

    def test_the_path_is_carried_but_not_drawn(self, mssql_env):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        panel = filesystem.get_data(cursor, driver, ["/var/opt/mssql/log"])
        # The row keeps its path, for navigation and for `w`...
        assert panel["headers"][4] == "Path"
        assert all(r[4] for r in panel["rows"] if r[1] != "..")
        # ...but only the first four columns are drawn, so each line
        # shows a bare name the way dired does.
        assert panel["display_columns"] == 4

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

    def test_a_real_file_comes_back_without_stray_nuls(self, mssql_env):
        # The file is read as bytes and decoded here rather than by the
        # server, so what arrives must be text.  Reading UTF-16 a byte
        # at a time used to leave a NUL after every character.
        cursor, driver = mssql_env
        text = driver.read_file(cursor, "/var/opt/mssql/log/errorlog", 4096)
        assert text and "\x00" not in text

    def test_postgres_reads_bytes_rather_than_server_encoded_text(
            self, pg_env):
        # pg_read_file refuses anything not valid in the server encoding,
        # so a UTF-16 file failed on its first NUL instead of showing.
        cursor, driver = pg_env
        cursor.execute("SHOW data_directory")
        conf = cursor.fetchone()[0] + "/postgresql.conf"
        text = driver.read_file(cursor, conf, 4096)
        assert "PostgreSQL configuration file" in text
        assert "\x00" not in text

    def test_opening_a_directory_returns_its_listing(self, mssql_env,
                                                     captured):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        filesystem.run_action(cursor, driver, "open", ["/var/opt/mssql/log"])
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        assert panel["context"]["path"] == "/var/opt/mssql/log"
        assert any(r[1] == "errorlog" for r in panel["rows"]), panel["rows"]


class TestWindowsDrives:
    """Windows has no single filesystem root, so a drive root is a top.

    The drive list itself can only be exercised for real on a Windows
    server; what is checked here is the navigation around it, which is
    the part that left a Windows user stuck at the top of a drive.
    """

    def test_the_server_reports_its_drives(self, mssql_env):
        cursor, driver = mssql_env
        drives = driver.list_drives(cursor)
        # On Linux the DMV reports the single root, which is honest.
        assert drives, "expected at least one drive"
        assert all(d["path"] for d in drives)
        assert all(d["is_dir"] for d in drives)

    def test_the_drives_view_has_the_shape_the_keys_expect(self, mssql_env):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        panel = filesystem.get_data(cursor, driver, [driver.DRIVES_PATH])
        # Type first and Path last, as in a directory listing, so the
        # same keys work without knowing which view they are in.
        assert panel["headers"][0] == "Type"
        assert panel["headers"][-1] == "Path"
        assert panel["row_id"] == 4
        assert all(r[0] == "dir" for r in panel["rows"]), panel["rows"]

    def test_a_drive_root_offers_a_way_up_to_the_drive_list(self, mssql_env):
        from datum.panels import filesystem

        _cursor, driver = mssql_env
        entry = {"name": "Data", "path": "C:\\Data", "is_dir": True,
                 "size": None, "modified": ""}
        rows = filesystem._rows_for(driver, None, "C:\\", [entry])
        assert rows[0][:2] == ["dir", ".."]
        assert rows[0][4] == driver.DRIVES_PATH
        # And says where it goes, rather than showing a bare sentinel.
        assert rows[0][3] == "drive list"

    def test_a_directory_below_a_drive_goes_up_normally(self, mssql_env):
        from datum.panels import filesystem

        _cursor, driver = mssql_env
        rows = filesystem._rows_for(driver, None, "C:\\Data", [])
        assert rows[0][4] == "C:\\"

    def test_a_posix_root_offers_no_drive_list(self, mssql_env):
        from datum.panels import filesystem

        _cursor, driver = mssql_env
        rows = filesystem._rows_for(driver, None, "/", [])
        assert not rows or rows[0][1] != ".."

    def test_postgres_on_windows_has_no_drive_list(self, pg_env):
        from datum.panels import filesystem

        _cursor, driver = pg_env
        rows = filesystem._rows_for(driver, None, "C:\\", [])
        # Nothing is invented: PostgreSQL cannot enumerate drives.
        assert not rows or rows[0][1] != ".."

    def test_the_drive_list_is_reachable_by_its_path(self, mssql_env):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        panel = filesystem.get_data(cursor, driver, [driver.DRIVES_PATH])
        assert panel["title"] == "Server drives"
        assert panel["context"]["path"] == driver.DRIVES_PATH


# --- Hostile listings ---------------------------------------------------
#
# The Windows bugs this panel shipped with were all reproducible here;
# what was missing was fixtures with anything awkward in them.  Every
# tree below is built through the server, not through docker, so these
# run against any reachable server — a Windows one included.

MS_NASTY = "datum_nasty"
PG_NASTY = "datum_nasty"


def _mssql_data_dir(driver, cursor):
    path = (driver.default_paths(cursor) or {}).get("data")
    if not path:
        pytest.skip("server did not report a data directory")
    return path


@pytest.fixture
def nasty_mssql(mssql_env):
    """A tree with a space in a directory name and a level to not recurse into.

    Built with xp_create_subdir and BACKUP, the only writes a client can
    make through SQL Server.  Idempotent, so re-runs overwrite rather
        than pile up, and nothing needs removing afterwards.
    """
    cursor, driver = mssql_env
    root = driver.join_path(_mssql_data_dir(driver, cursor), MS_NASTY)
    spaced = driver.join_path(root, "with space")
    # Backups are not cheap, and the tree outlives the run, so only
    # build it when it is not already there.
    try:
        existing = {e["name"] for e in driver.browse_path(cursor, root)}
        if {"with space", "top.bak"} <= existing:
            return root, spaced
    except Exception:
        pass
    try:
        for directory in (root, spaced, driver.join_path(spaced, "deeper")):
            cursor.execute("EXEC master.dbo.xp_create_subdir ?", [directory])
        for target in (driver.join_path(root, "top.bak"),
                       driver.join_path(spaced, "a file.bak"),
                       driver.join_path(driver.join_path(spaced, "deeper"),
                                        "buried.bak")):
            cursor.execute(
                f"BACKUP DATABASE model TO DISK = "
                f"{driver.quote_ddl_literal(target)} WITH INIT, FORMAT")
            while cursor.nextset():
                pass
    except Exception as err:
        pytest.skip(f"cannot build the probe tree: {err}")
    return root, spaced


class TestAwkwardNames:
    """A space in a name, and a level below that must stay unlisted."""

    def test_the_listing_stops_at_one_level(self, mssql_env, nasty_mssql):
        cursor, driver = mssql_env
        root, _spaced = nasty_mssql
        names = {e["name"] for e in driver.browse_path(cursor, root)}
        assert names == {"with space", "top.bak"}, names
        # Nothing from below it, however deep the tree goes.
        assert "a file.bak" not in names
        assert "buried.bak" not in names
        assert "deeper" not in names

    def test_a_directory_name_with_a_space_can_be_descended(self, mssql_env,
                                                           nasty_mssql):
        cursor, driver = mssql_env
        root, spaced = nasty_mssql
        entry = next(e for e in driver.browse_path(cursor, root)
                     if e["name"] == "with space")
        assert entry["is_dir"]
        # The path the server reported is what navigation uses, so it has
        # to work as given rather than needing quoting here.
        names = {e["name"] for e in driver.browse_path(cursor, entry["path"])}
        assert names == {"deeper", "a file.bak"}, names

    def test_a_file_name_with_a_space_is_listed_with_its_size(self, mssql_env,
                                                             nasty_mssql):
        cursor, driver = mssql_env
        _root, spaced = nasty_mssql
        entry = next(e for e in driver.browse_path(cursor, spaced)
                     if e["name"] == "a file.bak")
        assert not entry["is_dir"]
        assert entry["size"] and entry["size"] > 0

    def test_walking_back_up_from_a_spaced_directory(self, mssql_env,
                                                     nasty_mssql):
        _cursor, driver = mssql_env
        root, spaced = nasty_mssql
        assert driver.parent_path(spaced) == root.rstrip("/\\")

    def test_a_binary_file_is_named_rather_than_dumped(self, mssql_env,
                                                       nasty_mssql, captured):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        _root, spaced = nasty_mssql
        # A backup sits in the same directory as the logs worth reading,
        # and decodes to almost nothing but control characters.
        raw = driver.read_file(cursor, driver.join_path(spaced, "a file.bak"),
                               2048)
        assert driver.looks_binary(raw)

        filesystem.run_action(cursor, driver, "view", [_payload(
            {"path": driver.join_path(spaced, "a file.bak")})])
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        assert "not text" in panel["info"]
        # And points at the key that fetches it for Emacs to open.
        assert " o " in panel["info"]
        # What reaches the buffer is a sentence, not the bytes.
        assert "not shown here" in panel["content"]
        assert "\x00" not in panel["content"]
        assert len(panel["content"]) < 800

    def test_the_panel_lists_the_awkward_tree_too(self, mssql_env,
                                                  nasty_mssql):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        root, _spaced = nasty_mssql
        panel = filesystem.get_data(cursor, driver, [root])
        rows = {r[1]: r for r in panel["rows"]}
        assert ".." in rows
        assert rows["with space"][0] == "dir"
        assert rows["top.bak"][0] == "file"
        # The path column carries the space unmangled.
        assert rows["with space"][4].endswith("with space")


@pytest.fixture
def nasty_pg(pg_env):
    """Files in the encodings and names that actually caused trouble.

    PostgreSQL can run a program as its own account, which is the only
    way to put arbitrary bytes on the server through SQL.  Superuser
    only, so this skips where it is not allowed.
    """
    cursor, driver = pg_env
    cursor.execute("SHOW data_directory")
    root = cursor.fetchone()[0].rstrip("/") + "/" + PG_NASTY
    spaced = root + "/with space"

    def run(command):
        # COPY feeds the program's stdin, which these ignore; a single
        # quote is doubled for the SQL literal.
        literal = command.replace("'", "''")
        cursor.execute(f"COPY (SELECT 1) TO PROGRAM '{literal}'")

    body = "<root>hello</root>"
    try:
        run(f'mkdir -p "{spaced}/deeper"')
        run(f'printf "{body}\\n" > "{root}/utf8.txt"')
        run(f'printf "{body}\\n" | iconv -f UTF-8 -t UTF-16LE '
            f'> "{root}/utf16le.xml"')
        run(f'printf "{body}\\n" | iconv -f UTF-8 -t UTF-16 '
            f'> "{root}/utf16bom.xml"')
        run(f'printf "\\357\\273\\277{body}\\n" > "{root}/utf8bom.txt"')
        run(f'printf "{body}\\n" > "{root}/a file.txt"')
        run(f'printf "{body}\\n" > "{root}/uni\\303\\247ode.txt"')
        run(f'head -c 2048 /dev/urandom > "{root}/random.bin"')
        run(f'printf "buried\\n" > "{spaced}/deeper/buried.txt"')
    except Exception as err:
        pytest.skip(f"cannot write probe files: {err}")

    yield root, spaced, body

    try:
        run(f'rm -rf "{root}"')
    except Exception:
        pass


class TestEncodingsEndToEnd:
    """Every encoding that broke the viewer, read back off a real server."""

    def _read(self, driver, cursor, path):
        return driver.read_file(cursor, path, 8192)

    def test_plain_utf8(self, pg_env, nasty_pg):
        cursor, driver = pg_env
        root, _spaced, body = nasty_pg
        assert self._read(driver, cursor, f"{root}/utf8.txt").strip() == body

    def test_utf16_without_a_byte_order_mark(self, pg_env, nasty_pg):
        cursor, driver = pg_env
        root, _spaced, body = nasty_pg
        # The case that rendered as double-spaced text, and the one SQL
        # Server's SINGLE_NCLOB refuses outright.
        text = self._read(driver, cursor, f"{root}/utf16le.xml")
        assert "\x00" not in text
        assert text.strip() == body

    def test_utf16_with_a_byte_order_mark(self, pg_env, nasty_pg):
        cursor, driver = pg_env
        root, _spaced, body = nasty_pg
        text = self._read(driver, cursor, f"{root}/utf16bom.xml")
        assert "\x00" not in text
        assert text.strip() == body

    def test_utf8_with_a_byte_order_mark(self, pg_env, nasty_pg):
        cursor, driver = pg_env
        root, _spaced, body = nasty_pg
        # The mark is the file's business and should not show up.
        text = self._read(driver, cursor, f"{root}/utf8bom.txt")
        assert text.strip() == body
        assert not text.startswith("﻿")

    def test_random_bytes_are_named_not_dumped(self, pg_env, nasty_pg,
                                               captured):
        from datum.panels import filesystem

        cursor, driver = pg_env
        root, _spaced, _body = nasty_pg
        filesystem.run_action(cursor, driver, "view",
                              [_payload({"path": f"{root}/random.bin"})])
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        assert "not text" in panel["info"]
        assert len(panel["content"]) < 800

    def test_a_name_with_a_space_lists_and_reads(self, pg_env, nasty_pg):
        cursor, driver = pg_env
        root, _spaced, body = nasty_pg
        entry = next(e for e in driver.browse_path(cursor, root)
                     if e["name"] == "a file.txt")
        # The path the server gave back is what gets used, unquoted.
        assert self._read(driver, cursor, entry["path"]).strip() == body

    def test_a_non_ascii_name_survives_the_round_trip(self, pg_env, nasty_pg):
        cursor, driver = pg_env
        root, _spaced, body = nasty_pg
        entry = next((e for e in driver.browse_path(cursor, root)
                      if e["name"].endswith("ode.txt")
                      and e["name"] != "utf8.txt"), None)
        assert entry, "the non-ascii name did not come back"
        assert self._read(driver, cursor, entry["path"]).strip() == body

    def test_postgres_also_lists_one_level_only(self, pg_env, nasty_pg):
        cursor, driver = pg_env
        root, _spaced, _body = nasty_pg
        names = {e["name"] for e in driver.browse_path(cursor, root)}
        assert "with space" in names
        assert "buried.txt" not in names
        assert "deeper" not in names


class TestCopyingToTheClient:
    """The datum process runs beside Emacs, so a file read over SQL can
    simply be written to the local disk."""

    def test_a_file_arrives_byte_for_byte(self, mssql_env, nasty_mssql,
                                          tmp_path, captured):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        _root, spaced = nasty_mssql
        remote = driver.join_path(spaced, "a file.bak")
        local = str(tmp_path / "copy.bak")

        filesystem.run_action(cursor, driver, "download",
                              [_payload({"path": remote, "local": local})])
        assert "error" not in [k for k, _ in captured], captured
        expected = driver.read_bytes(cursor, remote)
        with open(local, "rb") as handle:
            assert handle.read() == expected
        assert len(expected) > 0

    def test_the_panel_says_where_it_landed(self, mssql_env, nasty_mssql,
                                            tmp_path, captured):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        root, _spaced = nasty_mssql
        local = str(tmp_path / "top.bak")
        filesystem.run_action(cursor, driver, "download", [_payload(
            {"path": driver.join_path(root, "top.bak"), "local": local})])
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        # The client caches by these, so viewing then copying fetches once.
        assert panel["local"] == local
        assert panel["size"] and panel["size"] > 0
        assert panel["sub_panel"] == "downloaded"

    def test_a_file_too_big_is_refused_rather_than_pulled(self, mssql_env,
                                                          nasty_mssql,
                                                          tmp_path, captured):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        root, _spaced = nasty_mssql
        local = str(tmp_path / "nope.bak")
        filesystem.run_action(cursor, driver, "download", [_payload(
            {"path": driver.join_path(root, "top.bak"), "local": local,
             "limit": 16})])
        assert [k for k, _ in captured] == ["error"], captured
        assert not os.path.exists(local)

    def test_a_tree_keeps_its_shape(self, mssql_env, nasty_mssql, tmp_path,
                                    captured):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        root, _spaced = nasty_mssql
        local = str(tmp_path / "tree")
        filesystem.run_action(cursor, driver, "download-tree",
                              [_payload({"path": root, "local": local})])
        assert "error" not in [k for k, _ in captured], captured
        # The directory with a space in its name, and the level below it.
        assert os.path.isfile(os.path.join(local, "top.bak"))
        assert os.path.isfile(os.path.join(local, "with space", "a file.bak"))
        assert os.path.isfile(
            os.path.join(local, "with space", "deeper", "buried.bak"))

    def test_a_tree_over_the_limit_is_refused_before_anything_is_written(
            self, mssql_env, nasty_mssql, tmp_path, captured):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        root, _spaced = nasty_mssql
        local = str(tmp_path / "toobig")
        filesystem.run_action(cursor, driver, "download-tree", [_payload(
            {"path": root, "local": local, "limit": 64})])
        assert [k for k, _ in captured] == ["error"], captured
        assert not os.path.exists(local)

    def test_postgres_copies_too(self, pg_env, nasty_pg, tmp_path, captured):
        from datum.panels import filesystem

        cursor, driver = pg_env
        root, _spaced, body = nasty_pg
        local = str(tmp_path / "utf16.xml")
        filesystem.run_action(cursor, driver, "download", [_payload(
            {"path": f"{root}/utf16le.xml", "local": local})])
        assert "error" not in [k for k, _ in captured], captured
        # The bytes, not a decoding of them: a copy is a copy.
        with open(local, "rb") as handle:
            assert handle.read() == (body + "\n").encode("utf-16-le")

    def test_postgres_copies_a_tree(self, pg_env, nasty_pg, tmp_path,
                                    captured):
        from datum.panels import filesystem

        cursor, driver = pg_env
        root, _spaced, _body = nasty_pg
        local = str(tmp_path / "tree")
        filesystem.run_action(cursor, driver, "download-tree",
                              [_payload({"path": root, "local": local})])
        assert "error" not in [k for k, _ in captured], captured
        assert os.path.isfile(os.path.join(local, "utf8.txt"))
        assert os.path.isfile(
            os.path.join(local, "with space", "deeper", "buried.txt"))

    def test_an_unreadable_file_does_not_lose_the_rest_of_the_tree(
            self, mssql_env, nasty_mssql, tmp_path, captured):
        from datum.panels import filesystem

        cursor, driver = mssql_env
        root, _spaced = nasty_mssql
        local = str(tmp_path / "partial")

        real = driver.read_bytes
        state = {"first": True}

        def flaky(cur, path, offset=0, length=None):
            if state["first"]:
                state["first"] = False
                raise Exception("Access is denied")
            return real(cur, path, offset, length)

        driver.read_bytes = flaky
        try:
            filesystem.run_action(cursor, driver, "download-tree",
                                  [_payload({"path": root, "local": local})])
        finally:
            driver.read_bytes = real

        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        assert "could not be read" in panel["info"], panel["info"]
        assert panel["rows"], "the unreadable one should be named"
        # And the others still arrived.
        assert any(os.path.isfile(os.path.join(dirpath, name))
                   for dirpath, _dirs, names in os.walk(local)
                   for name in names)


class TestWalking:

    def test_mssql_walks_the_whole_subtree(self, mssql_env, nasty_mssql):
        cursor, driver = mssql_env
        root, _spaced = nasty_mssql
        depths = {e["depth"] for e in driver.walk_path(cursor, root)}
        assert depths >= {0, 1, 2}, depths

    def test_postgres_walks_it_too(self, pg_env, nasty_pg):
        cursor, driver = pg_env
        root, _spaced, _body = nasty_pg
        entries = driver.walk_path(cursor, root)
        names = {e["name"] for e in entries}
        assert "buried.txt" in names
        assert {e["depth"] for e in entries} >= {0, 1}

    def test_reading_at_an_offset(self, mssql_env, nasty_mssql):
        cursor, driver = mssql_env
        root, _spaced = nasty_mssql
        path = driver.join_path(root, "top.bak")
        whole = driver.read_bytes(cursor, path, 0, 64)
        assert driver.read_bytes(cursor, path, 8, 8) == whole[8:16]

    def test_which_dialect_can_seek(self, mssql_env, pg_env):
        # Decides whether a download is worth chunking: SQL Server
        # re-reads the file for every call.
        _c, mssql = mssql_env
        _c2, postgres = pg_env
        assert not mssql.seeks_when_reading
        assert postgres.seeks_when_reading
