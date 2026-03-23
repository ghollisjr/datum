"""datum importer: load tabular files into a SQL table.

Supports CSV (stdlib) and Parquet/JSON (polars or pyarrow, optional).
Uses pyodbc's fast_executemany for efficient bulk insertion.

Backend priority: polars > pyarrow > stdlib csv.

The :in command syntax:
    :in /path/to/file.csv  target_table            # error if table exists
    :in /path/to/file.csv  target_table  :insert   # insert into existing table
    :in /path/to/file.csv  target_table  :replace  # drop, recreate, insert
"""

import csv
import os
import time

from . import envelope
from .utils import split_identifier as _split_identifier

# polars is optional (preferred)
try:
    import polars as pl
    _HAVE_POLARS = True
except ImportError:
    _HAVE_POLARS = False

# pyarrow is optional (fallback)
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.json as pa_json
    import pyarrow.csv as pa_csv
    _HAVE_PYARROW = True
except ImportError:
    _HAVE_PYARROW = False


def run(path, table_name, mode, connection, driver, batch_size=1000):
    """Entry point for :in command.

    path:       absolute path to the source file.
    table_name: destination SQL table name (may include schema, e.g. dbo.users).
    mode:       one of None (default, error if exists), ':insert', ':replace'.
    connection: live pyodbc connection.
    driver:     a BaseDriver instance for type mapping.
    batch_size: number of rows per executemany call (default 1000).
    """
    if not os.path.exists(path):
        envelope.error(f":in - file not found: {path}")
        return

    fmt = _infer_format(path)
    if fmt is None:
        envelope.error(f":in - unsupported file format: {path}")
        return

    if fmt in ("parquet", "json") and not _HAVE_POLARS and not _HAVE_PYARROW:
        envelope.error(f":in - polars or pyarrow is required for {fmt} import. "
                       f"Install with: pip install polars")
        return

    cursor = connection.cursor()

    # Check table existence
    table_exists = _table_exists(cursor, table_name)

    if table_exists is not False:
        if mode is None:
            envelope.error(f":in - table '{table_name}' already exists. "
                           f"Use :insert to append or :replace to recreate.")
            return
        elif mode == ":replace":
            cursor.execute(f"DROP TABLE {table_name}")
            connection.commit()
            table_exists = False
            envelope.info(f"Dropped existing table '{table_name}'.")
        # mode == ":insert": fall through, table stays as-is

    t_start = time.monotonic()

    if _HAVE_POLARS:
        rows_inserted = _import_polars(path, table_name, table_exists,
                                       cursor, connection, driver, fmt,
                                       batch_size)
    elif fmt == "csv":
        rows_inserted = _import_csv(path, table_name, table_exists,
                                    cursor, connection, driver, batch_size)
    else:
        rows_inserted = _import_arrow(path, table_name, table_exists,
                                      cursor, connection, driver, fmt,
                                      batch_size)

    elapsed = time.monotonic() - t_start
    envelope.info(f":in - {rows_inserted} rows inserted into '{table_name}' "
                  f"in {elapsed:.2f}s.")


# --- Format dispatch ---

def _infer_format(path):
    ext = os.path.splitext(path)[1].lower()
    return {".csv": "csv", ".parquet": "parquet", ".json": "json"}.get(ext)


# --- Polars import (preferred when available) ---

# Map polars base type names to the type keys used in driver type maps.
_POLARS_TYPE_MAP = {
    "Int8":     "int8",
    "Int16":    "int16",
    "Int32":    "int32",
    "Int64":    "int64",
    "UInt8":    "uint8",
    "UInt16":   "uint16",
    "UInt32":   "uint32",
    "UInt64":   "uint64",
    "Float32":  "float32",
    "Float64":  "float64",
    "Boolean":  "bool",
    "String":   "string",
    "Utf8":     "string",
    "Date":     "date32",
    "Time":     "time64[us]",
    "Binary":   "binary",
    "LargeBinary": "large_binary",
    "Null":     "string",
}


def _polars_type_str(dtype):
    """Convert a polars DataType to the string key used in driver type maps."""
    name = str(dtype.base_type())
    if name in _POLARS_TYPE_MAP:
        return _POLARS_TYPE_MAP[name]
    s = str(dtype)
    if s.startswith("Datetime"):
        return "timestamp[us]"
    if s.startswith("Duration"):
        return "timestamp[us]"
    if s.startswith("Decimal"):
        return "decimal128"
    return "string"  # safe fallback


def _import_polars(path, table_name, table_exists, cursor, connection, driver, fmt,
                   batch_size=1000):
    """Import a file via polars. Returns row count."""
    if fmt == "csv":
        df = pl.read_csv(path, infer_schema_length=1000)
    elif fmt == "parquet":
        df = pl.read_parquet(path)
    elif fmt == "json":
        df = pl.read_ndjson(path)

    headers = df.columns

    if not table_exists:
        col_types = []
        for name in headers:
            py_type = _polars_type_str(df[name].dtype)
            sql_type = driver.python_type_to_sql(py_type)
            col_types.append((name, sql_type))
        ddl = _build_ddl(table_name, col_types, driver)
        cursor.execute(ddl)
        connection.commit()
        envelope.info(f"Created table '{table_name}'.")

    placeholders = ", ".join(["?"] * len(headers))
    insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
    cursor.fast_executemany = True

    # Stream in batches
    rows_inserted = 0
    total = df.height

    for offset in range(0, total, batch_size):
        chunk = df.slice(offset, batch_size)
        rows = chunk.rows()
        cursor.executemany(insert_sql, rows)
        rows_inserted += len(rows)

    connection.commit()
    return rows_inserted


# --- CSV import (stdlib, no extra deps) ---

def _import_csv(path, table_name, table_exists, cursor, connection, driver,
                batch_size=1000):
    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        headers = next(reader)
        sample_rows = []
        for i, row in enumerate(reader):
            sample_rows.append(row)
            if i >= 99:
                break

    if not table_exists:
        # Infer types from sample: try int, then float, then string.
        col_types = _infer_csv_types(headers, sample_rows, driver)
        ddl = _build_ddl(table_name, col_types, driver)
        cursor.execute(ddl)
        connection.commit()
        envelope.info(f"Created table '{table_name}'.")

    placeholders = ", ".join(["?"] * len(headers))
    insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

    cursor.fast_executemany = True
    rows_inserted = 0

    # Re-open and stream all rows including sample
    with open(path, newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        batch = []
        for row in reader:
            batch.append(row)
            if len(batch) >= batch_size:
                cursor.executemany(insert_sql, batch)
                rows_inserted += len(batch)
                batch = []
        if batch:
            cursor.executemany(insert_sql, batch)
            rows_inserted += len(batch)

    connection.commit()
    return rows_inserted


def _infer_csv_types(headers, sample_rows, driver):
    """Return list of (col_name, sql_type) by sampling up to 100 rows."""
    col_types = []
    for i, name in enumerate(headers):
        values = [row[i] for row in sample_rows if i < len(row) and row[i] != '']
        py_type = "string"
        if values:
            if all(_is_int(v) for v in values):
                py_type = "int64"
            elif all(_is_float(v) for v in values):
                py_type = "float64"
        col_types.append((name, driver.python_type_to_sql(py_type)))
    return col_types


def _is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def _is_float(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


# --- Arrow import (parquet, json) ---

def _import_arrow(path, table_name, table_exists, cursor, connection, driver, fmt,
                  batch_size=1000):
    if fmt == "parquet":
        table = pq.read_table(path)
    else:  # json
        table = pa_json.read_json(path)

    schema = table.schema

    if not table_exists:
        col_types = []
        for i, field in enumerate(schema):
            py_type = _arrow_type_str(field.type)
            sql_type = driver.python_type_to_sql(py_type)
            col_types.append((field.name, sql_type))
        ddl = _build_ddl(table_name, col_types, driver)
        cursor.execute(ddl)
        connection.commit()
        envelope.info(f"Created table '{table_name}'.")

    placeholders = ", ".join(["?"] * len(schema))
    insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
    cursor.fast_executemany = True

    # Stream in batches via record batches to avoid loading everything at once
    rows_inserted = 0
    for batch in table.to_batches(max_chunksize=batch_size):
        rows = [tuple(batch.column(i)[j].as_py()
                      for i in range(batch.num_columns))
                for j in range(batch.num_rows)]
        cursor.executemany(insert_sql, rows)
        rows_inserted += len(rows)

    connection.commit()
    return rows_inserted


def _arrow_type_str(arrow_type):
    """Convert a pyarrow DataType to the string key used in type maps."""
    import pyarrow as pa
    mapping = {
        pa.int8():        "int8",
        pa.int16():       "int16",
        pa.int32():       "int32",
        pa.int64():       "int64",
        pa.uint8():       "uint8",
        pa.uint16():      "uint16",
        pa.uint32():      "uint32",
        pa.uint64():      "uint64",
        pa.float16():     "float16",
        pa.float32():     "float32",
        pa.float64():     "float64",
        pa.bool_():       "bool",
        pa.string():      "string",
        pa.large_string():"large_string",
        pa.date32():      "date32",
        pa.date64():      "date64",
        pa.binary():      "binary",
        pa.large_binary():"large_binary",
    }
    if arrow_type in mapping:
        return mapping[arrow_type]
    # Timestamp and time types need string matching
    s = str(arrow_type)
    if s.startswith("timestamp"):
        # Strip timezone suffix: "timestamp[us, tz=UTC]" → "timestamp[us]"
        base = s.split(",")[0]
        if not base.endswith("]"):
            base += "]"
        return base
    if s.startswith("time32") or s.startswith("time64"):
        return s
    if s.startswith("decimal128"):
        return "decimal128"
    return "string"  # safe fallback


# --- DDL helpers ---

def _build_ddl(table_name, col_types, driver=None):
    """Build a CREATE TABLE statement from (name, sql_type) pairs."""
    def _quote(name):
        if driver:
            return driver.quote_identifier(name)
        return f'"{name}"'  # ANSI fallback
    quoted_table = ".".join(
        _quote(seg) for seg in _split_identifier(table_name)
    )
    cols = ",\n    ".join(f"{_quote(name)} {sql_type}" for name, sql_type in col_types)
    return f"CREATE TABLE {quoted_table} (\n    {cols}\n)"


def _table_exists(cursor, table_name):
    """Check if a table exists. Works across MSSQL, PostgreSQL, and temp tables."""
    parts = _split_identifier(table_name)
    table = parts[-1]
    schema = parts[-2] if len(parts) > 1 else None
    try:
        # SQL Server temp tables (names starting with #) aren't in
        # INFORMATION_SCHEMA — check via OBJECT_ID in tempdb instead.
        if table.startswith("#"):
            cursor.execute(
                "SELECT OBJECT_ID('tempdb..' + ?)", table)
            return cursor.fetchone()[0] is not None
        if schema:
            cursor.execute("""
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """, schema, table)
        else:
            cursor.execute("""
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = ?
            """, table)
        return cursor.fetchone() is not None
    except Exception as exc:
        import logging
        logging.getLogger("datum").warning("_table_exists check failed: %s", exc)
        return None
