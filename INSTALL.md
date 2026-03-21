# Dropping these files into the repo

## Files to copy
Copy the contents of `datum/` over the existing `datum/` directory.
Copy `sql-datum.el` over the existing file.

## One manual step: delete drivers.py
The old `datum/drivers.py` (which only contained `print_list()`) is replaced
by the new `datum/drivers/` package. You must delete the old file:

    rm datum/drivers.py

If you don't, Python will prefer the file over the package directory and
the import will break.

## Optional dependencies
- `pip install pyarrow`  — required for :out .parquet/.json and :in .parquet/.json
- `pip install polars`   — optional but recommended; preferred backend for fast import/export (falls back to pyarrow or stdlib csv)

## New CLI flag
    datum --sql-type=mssql ...
    datum --sql-type=postgres ...
    datum --sql-type=mysql ...
    datum --sql-type=oracle ...
    datum --sql-type=sqlite ...

Accepted values: `mssql`, `postgres`, `mysql`, `oracle`, `sqlite`, `ansi`.
If omitted, datum detects the dialect from the connection string or DSN automatically.
