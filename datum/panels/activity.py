"""Activity Monitor panel — shows active sessions and queries.

MSSQL: sys.dm_exec_requests + sys.dm_exec_sessions + sys.dm_exec_sql_text
PostgreSQL: pg_stat_activity
Actions: Kill session
"""

import re


def get_data(cursor, driver, args):
    """Return activity monitor data as a panel result dict."""
    dialect = driver.dialect_name

    if dialect == "mssql":
        return _mssql_activity(cursor, args)
    elif dialect == "postgres":
        return _postgres_activity(cursor, args)
    else:
        return {
            "panel": "activity",
            "headers": ["note"],
            "rows": [["Activity monitor not supported for this dialect"]],
            "row_id": None,
            "actions": [],
            "info": f"Dialect '{dialect}' is not supported for activity monitor.",
        }


def run_action(cursor, driver, action_name, args):
    """Execute an activity monitor action."""
    from .. import envelope

    dialect = driver.dialect_name

    if action_name == "kill":
        if not args:
            envelope.error("Kill requires a session ID")
            return
        session_id = args[0]
        if dialect == "mssql":
            cursor.execute(f"KILL {int(session_id)}")
            cursor.connection.commit()
            envelope.info(f"Killed session {session_id}")
        elif dialect == "postgres":
            cursor.execute("SELECT pg_terminate_backend(%s)", [int(session_id)])
            result = cursor.fetchone()
            if result and result[0]:
                envelope.info(f"Terminated backend {session_id}")
            else:
                envelope.warn(f"Could not terminate backend {session_id}")
        else:
            envelope.error(f"Kill not supported for dialect '{dialect}'")
    else:
        envelope.error(f"Unknown activity action: {action_name}")


def _mssql_activity(cursor, args):
    """MSSQL activity monitor using DMVs."""
    sql = """
        SELECT
            r.session_id                                AS [SPID],
            s.login_name                                AS [User],
            DB_NAME(r.database_id)                      AS [Database],
            r.status                                    AS [Status],
            r.command                                   AS [Command],
            r.wait_type                                 AS [Wait Type],
            DATEDIFF(SECOND, r.start_time, GETDATE())   AS [Duration(s)],
            r.cpu_time                                  AS [CPU(ms)],
            r.reads                                     AS [Reads],
            r.writes                                    AS [Writes],
            r.blocking_session_id                       AS [Blocked By],
            SUBSTRING(t.text, 1, 500)                   AS [SQL Text]
        FROM sys.dm_exec_requests r
        JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
        CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
        WHERE r.session_id <> @@SPID
        ORDER BY r.start_time
    """
    cursor.execute(sql)
    headers = [col[0] for col in cursor.description]
    rows = []
    for row in cursor.fetchall():
        row = list(row)
        # Clean SQL text: collapse whitespace
        if row[-1] and isinstance(row[-1], str):
            row[-1] = re.sub(r'\s+', ' ', row[-1]).strip()
        # Convert None to empty string for display
        rows.append([str(v) if v is not None else "" for v in row])

    return {
        "panel": "activity",
        "headers": headers,
        "rows": rows,
        "row_id": 0,  # SPID column
        "actions": [
            {"key": "k", "label": "Kill session", "command": "kill"},
        ],
        "info": None,
    }


def _postgres_activity(cursor, args):
    """PostgreSQL activity monitor using pg_stat_activity."""
    sql = """
        SELECT
            pid                                              AS "PID",
            usename                                          AS "User",
            datname                                          AS "Database",
            state                                            AS "State",
            wait_event_type                                  AS "Wait Type",
            wait_event                                       AS "Wait Event",
            EXTRACT(EPOCH FROM (now() - query_start))::INT   AS "Duration(s)",
            LEFT(query, 500)                                 AS "Query"
        FROM pg_stat_activity
        WHERE state != 'idle'
          AND pid <> pg_backend_pid()
        ORDER BY query_start
    """
    cursor.execute(sql)
    headers = [col[0] for col in cursor.description]
    rows = []
    for row in cursor.fetchall():
        row = list(row)
        # Clean query text
        if row[-1] and isinstance(row[-1], str):
            row[-1] = re.sub(r'\s+', ' ', row[-1]).strip()
        rows.append([str(v) if v is not None else "" for v in row])

    return {
        "panel": "activity",
        "headers": headers,
        "rows": rows,
        "row_id": 0,  # PID column
        "actions": [
            {"key": "k", "label": "Kill session", "command": "kill"},
        ],
        "info": None,
    }
