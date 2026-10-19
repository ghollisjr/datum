"""Shared pure utilities for datum.

Functions here have no dependencies on other datum modules and can be
imported freely without risk of circular imports.
"""


_QUOTE_PAIRS = [('"', '"'), ('[', ']'), ('`', '`')]


def split_identifier(raw):
    """Split a dotted SQL identifier, respecting double-quote, bracket, and backtick quoting.

    Dots inside "quoted.name", [bracket.name], or `backtick.name` are preserved.
    Each returned part has surrounding quotes/brackets stripped.

    >>> split_identifier('DVF."F$DB.INST"')
    ['DVF', 'F$DB.INST']
    >>> split_identifier('[my.schema].[my.table]')
    ['my.schema', 'my.table']
    >>> split_identifier('dbo.users')
    ['dbo', 'users']
    """
    parts = []
    current = []
    close_expected = None
    for ch in raw:
        if close_expected:
            if ch == close_expected:
                close_expected = None
            else:
                current.append(ch)
        elif ch == '.':
            parts.append(''.join(current))
            current = []
        else:
            opened = False
            for open_ch, close_ch in _QUOTE_PAIRS:
                if ch == open_ch:
                    close_expected = close_ch
                    opened = True
                    break
            if not opened:
                current.append(ch)
    parts.append(''.join(current))
    return parts


def completion_sort_key(name):
    """Return the key NAME should sort under in a completion list.

    The quote characters are not part of what the user types, so they
    must not be part of what the list is ordered by.  A quoted name
    sorted as written comes out ahead of every unquoted one -- `"' is
    ASCII 34, `[' is 91 and `` ` `` is 96, all below the letters -- so
    one table called "test table" sorted ahead of test, test2 and
    test3, and the shortest match was offered last.

    Dots are kept as separators so that a schema-qualified name still
    sorts beside its siblings.
    """
    return ".".join(split_identifier(name)) if name else name


def human_bytes(value):
    """Return VALUE as a short binary size, or "" if it is not known.

    A disk is the one place raw bytes stop being readable: a drive's
    free space runs to twelve digits, and nobody reads that as a size.
    Directory listings keep their byte counts, where the numbers are
    small enough to mean something and sorting wants them bare.  Table
    sizes are the other place this is wanted, and for the same reason.
    """
    if value is None or value == "":
        return ""
    try:
        size = float(value)
    except (TypeError, ValueError):
        return ""
    if size < 1024:
        return f"{int(size)} B"
    for unit in ("KiB", "MiB", "GiB", "TiB", "PiB"):
        size /= 1024.0
        if size < 1024 or unit == "PiB":
            # One decimal reads as a size; three read as a measurement
            # the server is not actually making.
            return f"{size:.1f} {unit}"
    return ""


def parse_human_bytes(text):
    """Return the byte count TEXT names, or None if it names none.

    The inverse of `human_bytes', for adding a column of them up.  Only
    the shapes that function produces are accepted -- anything else
    returns None rather than a number that would quietly be wrong.
    """
    if not text or not isinstance(text, str):
        return None
    parts = text.split()
    if len(parts) != 2:
        return None
    number, unit = parts
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    if unit not in units:
        return None
    try:
        return float(number) * (1024 ** units.index(unit))
    except ValueError:
        return None
