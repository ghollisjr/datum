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
