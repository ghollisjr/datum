"""Shared pure utilities for datum.

Functions here have no dependencies on other datum modules and can be
imported freely without risk of circular imports.
"""


def split_identifier(raw):
    """Split a dotted SQL identifier, respecting double-quote and bracket quoting.

    Dots inside "quoted.name" or [bracket.name] are preserved.
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
    in_dquote = False
    in_bracket = False
    in_backtick = False
    for ch in raw:
        if in_dquote:
            if ch == '"':
                in_dquote = False
            else:
                current.append(ch)
        elif in_bracket:
            if ch == ']':
                in_bracket = False
            else:
                current.append(ch)
        elif in_backtick:
            if ch == '`':
                in_backtick = False
            else:
                current.append(ch)
        elif ch == '"':
            in_dquote = True
        elif ch == '[':
            in_bracket = True
        elif ch == '`':
            in_backtick = True
        elif ch == '.':
            parts.append(''.join(current))
            current = []
        else:
            current.append(ch)
    parts.append(''.join(current))
    return parts
