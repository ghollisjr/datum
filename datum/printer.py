"""Datum's query output printer."""
from collections import defaultdict
from datetime import datetime, date, time
from pyodbc import ProgrammingError
import decimal
import math
import operator as op
import unicodedata

_config = {}

# Cached translation table for text_formatter; rebuilt when config changes.
_trans_table = None
_trans_config_key = None


def display_width(s):
    """Return the display width of string s, counting wide chars as 2."""
    w = 0
    for ch in str(s):
        w += 2 if unicodedata.east_asian_width(ch) in ('W', 'F') else 1
    return w


def pad_to_width(s, width):
    """Pad string s with spaces to reach target display width."""
    s = str(s)
    return s + ' ' * max(0, width - display_width(s))


def initialize_module(config):
    """Initialize this module with a reference to the global config."""
    global _config
    # As of this writing the printer needs _all_ the config parameters to work
    # so let's just keep the whole dict referenced
    _config = config


def _get_trans_table():
    """Return (and cache) the str.maketrans table for the current config."""
    global _trans_table, _trans_config_key
    key = (_config["newline_replacement"], _config["tab_replacement"])
    if _trans_config_key != key:
        _trans_table = str.maketrans({"\n": key[0], "\r": "", "\t": key[1]})
        _trans_config_key = key
    return _trans_table


def print_cursor_results(a_cursor):
    """Print the current cursor resultset and (try to) move to the next one.

    Most queries have a single resulset, but stored procs for example use the
    extra logic.
    Note that the actual printing of output happens in print_resultset().
    """
    try:
        print_resultset(a_cursor)
    except ProgrammingError as e:
        if "Previous SQL was not a query." in str(e):
            pass
        else:
            raise e
    while a_cursor.nextset():
        try:
            print_resultset(a_cursor)
        except ProgrammingError as e:
            if "Previous SQL was not a query." in str(e):
                continue
            else:
                raise e


def format_row(column_widths, row):
    """Format a single row, handling hline strings and data tuples."""
    if isinstance(row, str):
        return row
    parts = [pad_to_width(cell, w) for cell, w in zip(row, column_widths)]
    return "| " + " | ".join(parts) + " |"


def print_rows(column_widths, rows):
    """Print formatted rows, handling hline strings and data tuples."""
    for row in rows:
        if isinstance(row, str):
            print(row)
        else:
            print(format_row(column_widths, row))


def print_resultset(a_cursor):
    """Print the results of cursor (the "current" resultset)."""
    global _config
    rows_to_print = _config["rows_to_print"]
    if rows_to_print:
        odbc_rows = a_cursor.fetchmany(rows_to_print)
    else:
        odbc_rows = a_cursor.fetchall()

    rowcount = a_cursor.rowcount
    # If there are no rows, we still print the column names, as this is useful
    # when exploring how many columns there are and their names in a new DB
    column_names = [text_formatter(column[0]) for column in
                    a_cursor.description]
    column_widths, print_ready = format_rows(column_names, odbc_rows)
    print()  # blank line
    print_rows(column_widths, print_ready)
    # Try to determine if all rows returned were printed
    # MS SQL Server doesn't report the total rows SELECTed,
    # but for example MySql does.
    printed_rows = len(odbc_rows)
    if printed_rows < rows_to_print or rows_to_print == 0:
        # We printed everything via :rows 0, or less than the max to print
        # in which case we can deduct there were no more rows
        rowcount = printed_rows
    if rowcount == -1:
        # Curse you, MS SQL Driver!
        rowcount = "(unknown)"
    # We tried our best! report the numbers
    print("\nRows printed: ", printed_rows, "/", rowcount, sep="")


def text_formatter(value):
    """Format text for printing.

    This function will replace newlines and tabs with the currently configured
    values, and do char width truncation if needed.
    """
    col_width = _config["column_display_length"]
    value = str(value).translate(_get_trans_table())
    if col_width and display_width(value) > col_width:
        # Truncate by walking characters until display width reaches limit
        truncated = []
        w = 0
        limit = col_width - 5  # leave room for "[...]"
        for ch in value:
            cw = 2 if unicodedata.east_asian_width(ch) in ('W', 'F') else 1
            if w + cw > limit:
                break
            truncated.append(ch)
            w += cw
        value = "".join(truncated) + "[...]"
    return value


def format_rows(column_names, raw_rows):
    """Go over all the rows in the results and format them for printing.

    Uses per-column formatter functions built from the first row's types
    to avoid repeated isinstance checks on every cell.
    """
    global _config
    null_string = _config["null_string"]
    null_len = display_width(null_string)
    num_cols = len(column_names)
    column_widths = [0] * num_cols

    if not raw_rows:
        for i, name in enumerate(column_names):
            column_widths[i] = display_width(name)
        separator = ["-" * (w + 2) for w in column_widths]
        hline = "|" + "+".join(separator) + "|"
        return column_widths, [column_names, hline]

    # Build per-column formatters from the first non-None value in each column.
    # This eliminates the isinstance chain for every cell.
    col_formatters = [None] * num_cols
    _text_formatter = text_formatter
    _int_len = int_len
    _decimal_len = decimal_len
    _Decimal = decimal.Decimal

    # Scan for representative types (first non-None value per column)
    type_samples = [None] * num_cols
    found = 0
    for row in raw_rows:
        for i in range(num_cols):
            if type_samples[i] is None and row[i] is not None:
                type_samples[i] = type(row[i])
                found += 1
        if found == num_cols:
            break

    for i in range(num_cols):
        sample_type = type_samples[i]
        if sample_type is bool:
            col_formatters[i] = _fmt_bool
        elif sample_type is time:
            col_formatters[i] = _fmt_isoformat
        elif sample_type is datetime:
            col_formatters[i] = _fmt_isoformat
        elif sample_type is date:
            col_formatters[i] = _fmt_date
        elif sample_type is int:
            col_formatters[i] = _fmt_int
        elif sample_type is float or sample_type is _Decimal:
            col_formatters[i] = _fmt_decimal
        elif sample_type is str:
            col_formatters[i] = _fmt_str
        elif sample_type is bytes:
            col_formatters[i] = _fmt_bytes
        else:
            col_formatters[i] = _fmt_fallback

    # Format all rows
    formatted = []
    for row in raw_rows:
        new_row = [None] * num_cols
        for i in range(num_cols):
            value = row[i]
            if value is None:
                new_row[i] = null_string
                if null_len > column_widths[i]:
                    column_widths[i] = null_len
            else:
                new_value, new_len = col_formatters[i](value)
                new_row[i] = new_value
                if new_len > column_widths[i]:
                    column_widths[i] = new_len
        formatted.append(tuple(new_row))

    for i, name in enumerate(column_names):
        name_len = display_width(name)
        if name_len > column_widths[i]:
            column_widths[i] = name_len

    separator = ["-" * (w + 2) for w in column_widths]
    hline = "|" + "+".join(separator) + "|"
    formatted.insert(0, column_names)
    formatted.insert(1, hline)
    return column_widths, formatted


# --- Per-type formatter functions ---
# Each returns (display_value, display_length).

def _fmt_bool(value):
    return value, 6

def _fmt_isoformat(value):
    s = value.isoformat()
    return s, len(s)

def _fmt_date(value):
    return value.isoformat(), 10

def _fmt_int(value):
    return value, int_len(value)

def _fmt_decimal(value):
    return value, decimal_len(decimal.Decimal(value))

def _fmt_str(value):
    v = text_formatter(value)
    return v, display_width(v)

def _fmt_bytes(value):
    v = text_formatter('0x' + value.hex())
    return v, display_width(v)

def _fmt_fallback(value):
    return "#DatumPrinterBroke#", 19


def int_len(number):
    """Calculate the length, in characters, of an integer."""
    # Source:
    # http://stackoverflow.com/questions/2189800/length-of-an-integer-in-python
    if number > 0:
        digits = int(math.log10(number))+1
    elif number == 0:
        digits = 1
    else:
        digits = int(math.log10(-number))+2  # +1 if you don't count the '-'
    return digits


def decimal_len(decimal_number):
    """Calculate the length, in characters, of a Decimal number."""
    # Use the actual string representation to avoid edge cases with
    # integers-as-Decimal (no decimal point) and scientific notation.
    # Cap at 22 for Oracle which reports all numbers as Decimal.
    return min(22, len(str(decimal_number)))
