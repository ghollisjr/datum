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

# A download is bounded so that a stray RET on a multi-gigabyte backup
# does not quietly try to pull it.  The caller can raise it.
_DOWNLOAD_LIMIT = 256 * 1024 * 1024

# SQL Server re-reads the file for every chunk, so it is read whole;
# PostgreSQL seeks, so it is read in pieces and never held entirely in
# memory.
_CHUNK = 4 * 1024 * 1024


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
        "display_columns": 4,
        "auto_refresh": False,
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
        actions.insert(2, {"key": "v", "label": "View as text",
                           "command": "view"})
        actions.insert(3, {"key": "o", "label": "Open here",
                           "command": "download"})
        actions.insert(4, {"key": "C", "label": "Copy here",
                           "command": "download"})
        actions.insert(5, {"key": "m", "label": "Mark", "command": None})
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
        # The path is carried on every row but not drawn: the directory
        # is named once in the title, and the rows show bare names, the
        # way dired does it.
        "display_columns": 4,
        # A directory is not something to poll: dired does not revert
        # itself either.  g refreshes, and a turns polling on for
        # anyone watching a file being written.
        "auto_refresh": False,
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
        elif action_name == "download":
            _off_the_session(cursor, driver, args, _download, "copy")
        elif action_name == "download-tree":
            _off_the_session(cursor, driver, args, _download_tree,
                             "copy tree")
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

    if driver.looks_binary(text):
        # Better to say what it is than to fill a buffer with it, and
        # point at the key that fetches it whole for Emacs to open.
        size = f"{len(text)} bytes read" if text else "empty"
        envelope.admin_panel({
            "panel": "filesystem",
            "sub_panel": "file",
            "title": f"Server file: {path}",
            "headers": [],
            "rows": [],
            "row_id": None,
            "actions": [],
            "info": f"{path} is not text — press o in the listing to "
                    f"fetch it and let Emacs open it",
            "content": (f"{path}\n\n"
                        f"This does not decode as text ({size}), so it is "
                        f"not shown here.\n\n"
                        f"Press o in the listing to fetch it whole and let "
                        f"Emacs open it — an archive in archive-mode, an "
                        f"image in image-mode, and so on.\n"),
            "parent_panel": "filesystem",
            "context": {"path": path},
        })
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


# --- Copying to the client ---
#
# The datum process runs on the client, beside Emacs, so a file read
# over SQL can simply be written to the local disk.  Nothing has to
# travel through the envelope.

def _fetch_to(cursor, driver, remote, local, limit=_DOWNLOAD_LIMIT):
    """Write REMOTE to LOCAL, returning how many bytes were written."""
    import os

    stat = driver.stat_file(cursor, remote) or {}
    size = stat.get("size")
    if size is not None and size > limit:
        raise ValueError(
            f"{remote} is {size} bytes, over the {limit}-byte limit")

    parent = os.path.dirname(local)
    if parent:
        os.makedirs(parent, exist_ok=True)

    written = 0
    with open(local, "wb") as handle:
        if driver.seeks_when_reading and size and size > _CHUNK:
            offset = 0
            while offset < size:
                chunk = driver.read_bytes(cursor, remote, offset, _CHUNK)
                if not chunk:
                    break
                handle.write(chunk)
                offset += len(chunk)
                written += len(chunk)
        else:
            data = driver.read_bytes(cursor, remote)
            if len(data) > limit:
                raise ValueError(
                    f"{remote} is over the {limit}-byte limit")
            handle.write(data)
            written = len(data)
    return written


def _download(cursor, driver, args):
    """Copy one server file to a path on the client."""
    from .. import envelope

    if not driver.supports_file_read:
        envelope.error(f"Reading server files is not supported on "
                       f"{driver.dialect_name}")
        return
    opts = _decode_payload(args) if args else {}
    remote = (opts.get("path") or "").strip()
    local = (opts.get("local") or "").strip()
    if not (remote and local):
        envelope.error("download requires a server path and a local one")
        return

    try:
        written = _fetch_to(cursor, driver, remote, local,
                            int(opts.get("limit") or _DOWNLOAD_LIMIT))
    except Exception as err:
        envelope.error(f"Cannot copy {remote}: {_db_error(err)}")
        return

    stat = driver.stat_file(cursor, remote) or {}
    # The client caches by these, so that viewing a file and then
    # copying it does not fetch it twice.
    envelope.admin_panel({
        "panel": "filesystem",
        "sub_panel": "downloaded",
        "title": f"Copied {remote}",
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": f"{written} bytes to {local}",
        "local": local,
        "remote": remote,
        "size": stat.get("size"),
        "modified": stat.get("modified") or "",
        # Echoed back so the client knows what it asked for: whether to
        # open what arrived, and where to put it afterwards.
        "then": opts.get("then") or "",
        "final": opts.get("final") or "",
        "context": {"path": remote},
    })


def _download_tree(cursor, driver, args):
    """Copy a server directory, and everything under it, to the client."""
    import os

    from .. import envelope

    opts = _decode_payload(args) if args else {}
    remote = (opts.get("path") or "").strip().rstrip("/\\")
    local = (opts.get("local") or "").strip()
    if not (remote and local):
        envelope.error("download-tree requires a server path and a local one")
        return
    limit = int(opts.get("limit") or _DOWNLOAD_LIMIT)

    try:
        entries = driver.walk_path(cursor, remote)
    except Exception as err:
        envelope.error(f"Cannot read {remote}: {_db_error(err)}")
        return

    total = sum(e.get("size") or 0 for e in entries if not e["is_dir"])
    if total > limit:
        envelope.error(f"{remote} holds {total} bytes, over the "
                       f"{limit}-byte limit")
        return

    copied, failed = 0, []
    for entry in entries:
        relative = _relative_to(driver, remote, entry["path"])
        if relative is None:
            continue
        target = os.path.join(local, *relative)
        if entry["is_dir"]:
            os.makedirs(target, exist_ok=True)
            continue
        try:
            _fetch_to(cursor, driver, entry["path"], target, limit)
            copied += 1
        except Exception as err:
            # One unreadable file should not lose the rest of the tree.
            failed.append(f"{entry['path']}: {_db_error(err)}")

    note = f"{copied} file{'' if copied == 1 else 's'} to {local}"
    if failed:
        note += f" — {len(failed)} could not be read"
    envelope.admin_panel({
        "panel": "filesystem",
        "sub_panel": "downloaded",
        "title": f"Copied {remote}",
        "headers": ["Could not be read"] if failed else [],
        "rows": [[f] for f in failed],
        "row_id": None,
        "actions": [],
        "info": note,
        "local": local,
        "remote": remote,
        "then": opts.get("then") or "",
        "final": opts.get("final") or "",
        "context": {"path": remote},
    })


def _relative_to(driver, root, path):
    """Return PATH under ROOT as a list of segments, or None."""
    trimmed = path.rstrip("/\\")
    root = root.rstrip("/\\")
    if not trimmed.startswith(root):
        return None
    rest = trimmed[len(root):].lstrip("/\\")
    if not rest:
        return None
    # The server's separator, whichever it is.
    return [part for part in rest.replace("\\", "/").split("/") if part]


def _off_the_session(cursor, driver, args, handler, label):
    """Run HANDLER on the background connection, where there is one.

    A copy holds its connection for as long as the file takes to read,
    and the main connection is the interactive session — so running it
    there leaves the session unable to answer anything until the file
    is done.  The background worker has a connection of its own, which
    is what it is for.

    Falls back to running inline when no worker is up, so the behaviour
    is the same, only blocking.
    """
    from .. import background
    from .. import envelope

    if not background.is_running():
        handler(cursor, driver, args)
        return

    def task(conn=None):
        # A cursor of its own, on the worker's own connection.
        handler(conn.cursor(), driver, args)

    payload = _decode_payload(args) if args else {}
    remote = payload.get("path") or ""
    background.submit(f"filesystem {label}", task)
    envelope.info(f"Copying {remote} in the background — "
                  f"the session stays usable")
