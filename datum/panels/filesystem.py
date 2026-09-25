"""Server filesystem panel: a read-only listing of the server's disk.

The files a database server cares about — data files, logs, backups —
live on the server's filesystem, which is usually not the machine Emacs
is running on.  Both MSSQL and PostgreSQL can enumerate a directory over
SQL, so this presents that as a listing to walk through, in the spirit
of dired but read-only: nothing here writes, renames or deletes.
"""

# Reading a whole file into a buffer over the wire is not the point; this
# is enough to see what a config or a log says.
_READ_LIMIT = 262144


def _db_error(err):
    """Return the useful part of a driver error."""
    text = str(err)
    marker = "]"
    if text.startswith("(") and marker in text:
        text = text.rsplit(marker, 1)[-1]
    return text.strip().strip("()'\" ") or "failed"


def _decode_payload(args):
    import base64
    import json

    if not args:
        return {}
    return json.loads(base64.b64decode(args[0]).decode("utf-8"))


def _unsupported(driver, what):
    return {
        "panel": "filesystem",
        "headers": ["note"],
        "rows": [[f"{what} is not supported on {driver.dialect_name}"]],
        "row_id": None,
        "actions": [],
        "info": None,
    }


def _start_path(cursor, driver):
    """Return the directory to open when none was named."""
    defaults = driver.default_paths(cursor) or {}
    start = defaults.get("data") or defaults.get("log")
    if start:
        return start
    # With nowhere better to start, the drive list beats guessing "/"
    # on a server whose filesystem has no single root.
    if driver.supports_drive_list:
        return driver.DRIVES_PATH
    return "/"


def _drives_panel(cursor, driver):
    """Return the list of the server's drives.

    Windows has no single filesystem root — each drive is its own tree —
    so this stands in for one, and is where going up from C:\\ arrives.
    """
    try:
        drives = driver.list_drives(cursor)
    except Exception as err:
        return {
            "panel": "filesystem",
            "headers": ["note"],
            "rows": [[f"Cannot list the server's drives: {_db_error(err)}"]],
            "row_id": None,
            "actions": [],
            "info": None,
            "context": {"path": driver.DRIVES_PATH},
        }
    rows = [["dir", d.get("name") or d.get("path") or "",
             "" if d.get("free") is None else str(d["free"]),
             d.get("kind") or "",
             d.get("path") or ""]
            for d in drives]
    return {
        "panel": "filesystem",
        "title": "Server drives",
        # Same shape as a directory listing — type first, path last — so
        # the same keys work here without knowing which view they are in.
        "headers": ["Type", "Name", "Free", "Kind", "Path"],
        "rows": rows,
        "row_id": 4,
        "actions": [
            {"key": "RET", "label": "Open drive", "command": "open"},
            {"key": "w", "label": "Copy path", "command": None},
        ],
        "info": f"{len(rows)} drive{'' if len(rows) == 1 else 's'} "
                f"— Free is in bytes",
        "context": {"path": driver.DRIVES_PATH},
    }


def _rows_for(driver, cursor, path, entries):
    """Return the panel rows for ENTRIES, parent directory first."""
    rows = []
    parent = driver.parent_path(path, cursor)
    to_drives = False
    if not parent and driver.supports_drive_list and driver.is_drive_root(path):
        # Above a drive root is the list of drives, not a directory.
        # A UNC share has no drive above it either, but the drive list is
        # the only navigable top there is, so it is still the way out.
        parent = driver.DRIVES_PATH
        to_drives = True
    if parent and parent != path:
        # A visible way up, the way dired lists "..".  Where that leads
        # somewhere other than a directory, say so rather than leaving a
        # bare sentinel showing in the path column.
        rows.append(["dir", "..", "",
                     "drive list" if to_drives else "", parent])
    for entry in entries:
        is_dir = bool(entry.get("is_dir"))
        size = entry.get("size")
        rows.append([
            "dir" if is_dir else "file",
            entry.get("name") or "",
            # Left as raw bytes so the column sorts as numbers, and
            # blank for directories, whose size is the entry's own.
            "" if (is_dir or size is None) else str(size),
            entry.get("modified") or "",
            entry.get("path") or "",
        ])
    return rows


def get_data(cursor, driver, args):
    """Return a directory listing as a panel result dict."""
    if not driver.supports_path_browse:
        return _unsupported(driver, "Browsing the server filesystem")

    path = " ".join(args).strip() or _start_path(cursor, driver)
    if path == driver.DRIVES_PATH:
        return _drives_panel(cursor, driver)
    try:
        entries = driver.browse_path(cursor, path)
    except Exception as err:
        return {
            "panel": "filesystem",
            "headers": ["note"],
            "rows": [[f"Cannot list {path}: {_db_error(err)}"]],
            "row_id": None,
            "actions": [],
            "info": f"Listing {path} failed",
            "context": {"path": path},
        }

    actions = [
        {"key": "RET", "label": "Open directory", "command": "open"},
        {"key": "^", "label": "Parent directory", "command": "parent"},
        {"key": "w", "label": "Copy path", "command": None},
    ]
    if driver.supports_file_read:
        actions.insert(2, {"key": "v", "label": "View file",
                           "command": "view"})
    rows = _rows_for(driver, cursor, path, entries)
    files = sum(1 for r in rows if r[0] == "file")
    dirs = sum(1 for r in rows if r[0] == "dir" and r[1] != "..")
    return {
        "panel": "filesystem",
        "title": f"Server files: {path}",
        "headers": ["Type", "Name", "Size", "Modified", "Path"],
        "rows": rows,
        # Navigation works off the full path, which the server gave us
        # rather than us assembling it from a separator we guessed.
        "row_id": 4,
        "actions": actions,
        "info": f"{path} — {dirs} director{'y' if dirs == 1 else 'ies'}, "
                f"{files} file{'' if files == 1 else 's'}",
        "context": {"path": path},
    }


def run_action(cursor, driver, action_name, args):
    """Open a directory or read a file."""
    from .. import envelope

    if not driver.supports_path_browse:
        envelope.error(f"Browsing the server filesystem is not supported "
                       f"on {driver.dialect_name}")
        return

    try:
        if action_name == "open":
            envelope.admin_panel(get_data(cursor, driver, args))
        elif action_name == "view":
            _view_file(cursor, driver, args)
        else:
            envelope.error(f"Unknown filesystem action: {action_name}")
    except Exception as err:
        envelope.error(f":admin filesystem {action_name} — {_db_error(err)}")


def _view_file(cursor, driver, args):
    """Send the head of a file for display in a read-only buffer."""
    from .. import envelope

    if not driver.supports_file_read:
        envelope.error(f"Reading server files is not supported on "
                       f"{driver.dialect_name}")
        return
    payload = _decode_payload(args) if args else {}
    path = (payload.get("path") or " ".join(args)).strip()
    if not path:
        envelope.error("view requires a path")
        return

    try:
        text = driver.read_file(cursor, path, _READ_LIMIT) or ""
    except Exception as err:
        # The server reads as its own service account, so a file it does
        # not own is refused however much the caller may be trusted.
        envelope.error(f"Cannot read {path}: {_db_error(err)}")
        return

    truncated = len(text) >= _READ_LIMIT
    envelope.admin_panel({
        "panel": "filesystem",
        "sub_panel": "file",
        "title": f"Server file: {path}",
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": (f"First {_READ_LIMIT // 1024}KB of {path}"
                 if truncated else path),
        "content": text,
        "parent_panel": "filesystem",
        "context": {"path": path},
    })
