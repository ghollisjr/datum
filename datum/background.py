"""Background introspection worker for datum.

Runs introspection queries on a separate pyodbc connection in a
background thread so the main REPL stays responsive.  Work is
submitted via ``submit()`` and executed serially from a queue.
"""

import threading
import queue
import pyodbc
import struct

from . import envelope

_thread = None
_queue = None        # queue.Queue of (task_name, handler, args) tuples
_conn_string = None
_conn = None
_current_db = None   # tracks which database the bg connection should USE
_stop_event = None


def _handle_datetimeoffset(dto_value):
    """Converter for SQL Server datetimeoffset(-155) type."""
    tup = struct.unpack("<6hI2h", dto_value)
    tweaked = [tup[i] // 100 if i == 6 else tup[i] for i in range(len(tup))]
    t = "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:07d} {:+03d}:{:02d}"
    return t.format(*tweaked)


def _ensure_connection():
    """Create or reconnect the background pyodbc connection."""
    global _conn
    if _conn is not None:
        try:
            # Quick liveness check
            _conn.cursor().execute("SELECT 1")
            return _conn
        except Exception:
            _conn = None
    _conn = pyodbc.connect(_conn_string, autocommit=True)
    _conn.add_output_converter(-155, _handle_datetimeoffset)
    # If we know which database we should be on, switch to it
    if _current_db:
        try:
            _conn.cursor().execute(f"USE {_current_db}")
        except Exception:
            pass
    return _conn


def _worker():
    """Background thread main loop — pull tasks from queue and execute."""
    while not _stop_event.is_set():
        try:
            item = _queue.get(timeout=1.0)
        except queue.Empty:
            continue
        if item is None:
            # Poison pill — shut down
            break
        task_name, handler, args = item
        try:
            conn = _ensure_connection()
            handler(conn=conn, *args)
        except Exception as err:
            # If we're shutting down, don't report errors from the
            # connection being closed out from under us.
            if not _stop_event.is_set():
                envelope.error(f"Background {task_name}: {err}")
        finally:
            if not _stop_event.is_set():
                envelope.bg_ready(task_name)
            _queue.task_done()


def start(conn_string):
    """Start the background worker thread.

    conn_string: the ODBC connection string (same as the main connection).
    The background connection is created lazily on first task.
    """
    global _thread, _queue, _conn_string, _stop_event
    if _thread is not None and _thread.is_alive():
        return
    _conn_string = conn_string
    _queue = queue.Queue()
    _stop_event = threading.Event()
    _thread = threading.Thread(target=_worker, daemon=True, name="datum-bg")
    _thread.start()


def stop():
    """Stop the background worker thread and close its connection.

    Closes the connection first to abort any in-flight query (e.g. a
    large introspection on a database with tens of thousands of tables),
    then signals the thread to exit.
    """
    global _thread, _conn, _stop_event
    if _thread is None:
        return
    # Close connection first — this interrupts any blocking
    # cursor.execute() in the worker thread.
    if _conn is not None:
        try:
            _conn.close()
        except Exception:
            pass
        _conn = None
    _stop_event.set()
    # Drain the queue so the worker doesn't process stale tasks
    while not _queue.empty():
        try:
            _queue.get_nowait()
        except queue.Empty:
            break
    _queue.put(None)  # poison pill
    _thread.join(timeout=3.0)
    _thread = None


def is_running():
    """Return True if the background thread is alive."""
    return _thread is not None and _thread.is_alive()


def submit(task_name, handler, args=()):
    """Submit a task to the background worker.

    task_name: descriptive name for progress/error reporting.
    handler:   callable with signature handler(*args, conn=...).
    args:      positional args tuple to pass to handler.
    """
    if _queue is not None:
        _queue.put((task_name, handler, args))


def run_with_cursor(label, work, cursor=None):
    """Run WORK on the worker's own connection, or inline when there is none.

    WORK is called with a cursor.  Anything that holds its connection
    for a long time — reading a large file, backing a database up —
    must not run on the connection the interactive session uses: the
    process reads the next command on that same thread, so the session
    would answer nothing until the work finished.

    Returns True when the work was handed off, False when it ran here.
    """
    if not is_running():
        work(cursor)
        return False

    def task(conn=None):
        work(conn.cursor())

    submit(label, task)
    return True


def switch_database(db):
    """Tell the background thread to switch to a different database.

    The next task (or reconnect) will USE this database.
    If the background connection is live, switch it immediately.
    """
    global _current_db
    _current_db = db
    if _conn is not None:
        try:
            _conn.cursor().execute(f"USE {db}")
        except Exception:
            pass
