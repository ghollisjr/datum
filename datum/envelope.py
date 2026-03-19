"""Envelope protocol for structured datum<->Emacs communication.

All messages that Emacs should act on are wrapped in a sigil line:
    ##DATUM:<type>:<payload>##

These lines are intercepted by sql-datum.el's comint preoutput filter and
stripped from the visible buffer. Everything else passes through as-is.

Message types:
    info        Human-readable status message shown in minibuffer.
    warn        Warning shown in minibuffer and mode line.
    error       Error shown in minibuffer.
    result-file Path to a file containing query results, with format suffix.
                Payload: /path/to/file:<format>
    introspect  Structured introspection data for Emacs-side state.
                Payload: <kind>:<json-array>
    dialect     The detected or declared SQL dialect.
                Payload: mssql|postgres|ansi
    meta        Key/value metadata (current db, schema, user, version).
                Payload: <key>:<value>
"""

import json
import sys

_SIGIL = "##DATUM"
_END = "##"


def _send(msg_type, payload):
    """Write a single envelope line to stdout, flushed immediately."""
    print(f"{_SIGIL}:{msg_type}:{payload}{_END}", flush=True)


def info(message):
    """Send an informational message to the Emacs minibuffer."""
    _send("info", message)


def warn(message):
    """Send a warning to the Emacs minibuffer and mode line."""
    _send("warn", message)


def error(message):
    """Send an error message to the Emacs minibuffer."""
    _send("error", message)


def result_file(path, fmt):
    """Notify Emacs that a query result was written to a file.

    fmt is the format string, e.g. 'csv', 'parquet', 'json'.
    """
    _send("result-file", f"{path}:{fmt}")


def introspect(kind, items):
    """Send structured introspection data to Emacs.

    kind is one of: databases, schemas, tables, columns, running, user, version.
    items is a list of strings (or dicts for columns/running).
    """
    _send("introspect", f"{kind}:{json.dumps(items)}")


def dialect(name):
    """Notify Emacs of the detected or declared SQL dialect."""
    _send("dialect", name)


def meta(key, value):
    """Send a key/value metadata pair to Emacs (db, schema, user, version)."""
    _send("meta", f"{key}:{value}")
