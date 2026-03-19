"""Datum's query result exporter.

Supports two export paths:

1. Legacy :csv command — appends results to a session-level CSV path.
   Kept for backwards compatibility.

2. New :out command — exports the next query's results to an explicit file
   path in a specified format (csv, parquet, json), then clears the target
   so subsequent queries go back to normal buffer output.
   Sends envelope messages so Emacs can act on the result.

Supported :out formats (by file extension):
    .csv      stdlib csv — no extra dependencies
    .parquet  pyarrow required
    .json     pyarrow required
"""

import csv
import json
import os
import time
from pyodbc import ProgrammingError

from . import envelope

# pyarrow is optional
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _HAVE_PYARROW = True
except ImportError:
    _HAVE_PYARROW = False

_config = {}

# State for the :out command. Set by commands.py, consumed and cleared here.
_out_target = None   # absolute path string or None
_out_force = False   # True if :force was supplied


def initialize_module(config):
    """Initialize this module with a reference to the global config."""
    global _config
    _config = config


# --- :out command state ---

def set_out_target(path, force=False):
    """Set the next-query export target. Called by the :out command handler."""
    global _out_target, _out_force
    _out_target = path
    _out_force = force


def clear_out_target():
    """Clear the export target after use."""
    global _out_target, _out_force
    _out_target = None
    _out_force = False


def has_out_target():
    """Return True if a :out target is currently set."""
    return _out_target is not None


def export_out_target(cursor):
    """Export cursor results to the :out target file, then clear the target.

    Called from datum.query_loop instead of printer.print_cursor_results
    when has_out_target() is True.
    """
    global _out_target, _out_force
    path = _out_target
    force = _out_force
    clear_out_target()

    if not cursor.description:
        envelope.warn(":out - query returned no resultset, nothing exported.")
        return

    if os.path.exists(path) and not force:
        envelope.error(f":out - file already exists: {path}. "
                       f"Use :out {path} :force to overwrite.")
        return

    fmt = _infer_format(path)
    if fmt is None:
        envelope.error(f":out - unsupported format for path: {path}")
        return

    if fmt in ("parquet", "json") and not _HAVE_PYARROW:
        envelope.error(f":out - pyarrow is required for {fmt} export. "
                       f"Install it with: pip install pyarrow")
        return

    t_start = time.monotonic()
    try:
        if fmt == "csv":
            rows_written = _export_csv(path, cursor)
        else:
            rows_written = _export_arrow(path, cursor, fmt)
    except Exception as err:
        envelope.error(f":out - export failed: {err}")
        return

    elapsed = time.monotonic() - t_start
    envelope.info(f":out - {rows_written} rows exported in {elapsed:.2f}s.")
    envelope.result_file(path, fmt)


def _infer_format(path):
    ext = os.path.splitext(path)[1].lower()
    return {".csv": "csv", ".parquet": "parquet", ".json": "json"}.get(ext)


def _export_csv(path, cursor):
    """Stream cursor results to a CSV file. Returns row count."""
    headers = [col[0] for col in cursor.description]
    rows_written = 0
    batch_size = 10_000
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        rows = cursor.fetchmany(batch_size)
        while rows:
            writer.writerows(rows)
            rows_written += len(rows)
            rows = cursor.fetchmany(batch_size)
    return rows_written


def _export_arrow(path, cursor, fmt):
    """Stream cursor results to a Parquet or JSON file via pyarrow.
    Returns row count.
    """
    headers = [col[0] for col in cursor.description]
    batch_size = 10_000
    rows_written = 0
    batches = []

    rows = cursor.fetchmany(batch_size)
    while rows:
        columns = [pa.array([row[i] for row in rows]) for i in range(len(headers))]
        batch = pa.record_batch(columns, names=headers)
        batches.append(batch)
        rows_written += len(rows)
        rows = cursor.fetchmany(batch_size)

    if not batches:
        return 0

    table = pa.Table.from_batches(batches)

    if fmt == "parquet":
        pq.write_table(table, path)
    elif fmt == "json":
        # Write newline-delimited JSON without requiring pandas
        with open(path, 'w', encoding='utf-8') as f:
            for batch in batches:
                for i in range(batch.num_rows):
                    record = {headers[j]: batch.column(j)[i].as_py()
                              for j in range(len(headers))}
                    f.write(json.dumps(record, default=str) + "\n")

    return rows_written


# --- Legacy :csv export (unchanged) ---

def export_cursor_results(a_cursor):
    """Export to CSV the results of a cursor (legacy :csv command).

    Most queries have one resultset, but if there's more than one, each
    resultset is appended to the same file separated by blank lines.
    """
    global _config
    path = _config["csv_path"]
    try:
        export_resultset(path, a_cursor)
    except ProgrammingError as e:
        if "Previous SQL was not a query." in str(e):
            pass
        else:
            raise e
    while a_cursor.nextset():
        try:
            print()
            export_resultset(path, a_cursor, '\n\n')
        except ProgrammingError as e:
            if "Previous SQL was not a query." in str(e):
                continue
            else:
                raise e


def export_resultset(path, cursor, prefix=None):
    """Export the current resultset to path (legacy :csv helper)."""
    if not cursor.description:
        print('\n(No output to export)')
        return
    batch_size = 100_000
    print('Writing resultset, one ! per', batch_size, 'rows:')
    with open(path, 'a', encoding='utf-8', newline='') as outputfile:
        if prefix:
            outputfile.write(prefix)
        writer = csv.writer(outputfile)
        writer.writerow([column[0] for column in cursor.description])
        rows = cursor.fetchmany(batch_size)
        while rows:
            writer.writerows(rows)
            print("!", end="", flush=True)
            rows = cursor.fetchmany(batch_size)
