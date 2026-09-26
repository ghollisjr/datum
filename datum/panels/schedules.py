"""Job Schedule CRUD operations for SQL Agent Jobs.

Handles schedule reading, creation, updating, and deletion via
msdb stored procedures.
"""

import json


def get_schedule(cursor, schedule_name, job_name):
    """Get full schedule details for editing."""
    sql = """
        SELECT
            s.schedule_id,
            s.name                  AS schedule_name,
            s.enabled,
            s.freq_type,
            s.freq_interval,
            s.freq_subday_type,
            s.freq_subday_interval,
            s.freq_relative_interval,
            s.freq_recurrence_factor,
            s.active_start_date,
            s.active_end_date,
            s.active_start_time,
            s.active_end_time
        FROM msdb.dbo.sysschedules s
        JOIN msdb.dbo.sysjobschedules js ON s.schedule_id = js.schedule_id
        WHERE js.job_id = (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = ?)
          AND s.name = ?
    """
    cursor.execute(sql, [job_name, schedule_name])
    row = cursor.fetchone()
    if not row:
        return None

    return {
        "schedule_id": row[0],
        "name": row[1],
        "enabled": row[2],
        "freq_type": row[3],
        "freq_interval": row[4],
        "freq_subday_type": row[5],
        "freq_subday_interval": row[6],
        "freq_relative_interval": row[7],
        "freq_recurrence_factor": row[8],
        "active_start_date": row[9],
        "active_end_date": row[10],
        "active_start_time": row[11],
        "active_end_time": row[12],
    }


def update_schedule(cursor, schedule_data):
    """Update an existing schedule.

    schedule_data is a dict with schedule_id and fields to update.

    The schedule is identified by its id and renamed with @new_name.
    sp_update_schedule's @name is the identity, not the new value, so
    passing a changed name there looked for a schedule that did not
    exist yet -- which is why renaming one never worked.
    """
    sid = schedule_data["schedule_id"]
    sql = """
        EXEC msdb.dbo.sp_update_schedule
            @schedule_id = ?,
            @new_name = ?,
            @enabled = ?,
            @freq_type = ?,
            @freq_interval = ?,
            @freq_subday_type = ?,
            @freq_subday_interval = ?,
            @freq_relative_interval = ?,
            @freq_recurrence_factor = ?,
            @active_start_date = ?,
            @active_end_date = ?,
            @active_start_time = ?,
            @active_end_time = ?
    """
    cursor.execute(sql, [
        sid,
        schedule_data["name"],
        schedule_data.get("enabled", 1),
        schedule_data.get("freq_type", 4),
        schedule_data.get("freq_interval", 1),
        schedule_data.get("freq_subday_type", 1),
        schedule_data.get("freq_subday_interval", 0),
        schedule_data.get("freq_relative_interval", 0),
        schedule_data.get("freq_recurrence_factor", 0),
        schedule_data.get("active_start_date", 20000101),
        schedule_data.get("active_end_date", 99991231),
        schedule_data.get("active_start_time", 0),
        schedule_data.get("active_end_time", 235959),
    ])
    cursor.connection.commit()


def create_schedule(cursor, job_name, schedule_data):
    """Create a new schedule and attach it to a job.

    schedule_data is a dict with schedule parameters.
    """
    # Create the schedule
    sql = """
        EXEC msdb.dbo.sp_add_schedule
            @schedule_name = ?,
            @enabled = ?,
            @freq_type = ?,
            @freq_interval = ?,
            @freq_subday_type = ?,
            @freq_subday_interval = ?,
            @freq_relative_interval = ?,
            @freq_recurrence_factor = ?,
            @active_start_date = ?,
            @active_end_date = ?,
            @active_start_time = ?,
            @active_end_time = ?
    """
    cursor.execute(sql, [
        schedule_data["name"],
        schedule_data.get("enabled", 1),
        schedule_data.get("freq_type", 4),
        schedule_data.get("freq_interval", 1),
        schedule_data.get("freq_subday_type", 1),
        schedule_data.get("freq_subday_interval", 0),
        schedule_data.get("freq_relative_interval", 0),
        schedule_data.get("freq_recurrence_factor", 0),
        schedule_data.get("active_start_date", 20000101),
        schedule_data.get("active_end_date", 99991231),
        schedule_data.get("active_start_time", 0),
        schedule_data.get("active_end_time", 235959),
    ])
    cursor.connection.commit()

    # Attach schedule to job
    cursor.execute(
        "EXEC msdb.dbo.sp_attach_schedule @job_name = ?, @schedule_name = ?",
        [job_name, schedule_data["name"]]
    )
    cursor.connection.commit()


def delete_schedule(cursor, schedule_name):
    """Delete a schedule by name."""
    cursor.execute(
        "EXEC msdb.dbo.sp_delete_schedule @schedule_name = ?, @force_delete = 1",
        [schedule_name]
    )
    cursor.connection.commit()


# Frequency type labels for the Emacs UI
FREQ_TYPES = {
    1: "Once",
    4: "Daily",
    8: "Weekly",
    16: "Monthly",
    32: "Monthly Relative",
    64: "On Agent Start",
    128: "On Idle",
}

# Subday type labels
SUBDAY_TYPES = {
    1: "At specified time",
    2: "Seconds",
    4: "Minutes",
    8: "Hours",
}


def schedule_options(cursor, current=None):
    """Field descriptors for a job schedule.

    The same shape every other wizard uses, so the schedule editor gets
    the field navigation and the rest of it rather than needing a
    renderer of its own.

    Which fields matter depends on the frequency — a weekly schedule
    reads Interval as days of the week, a monthly one as a day of the
    month — so the help says so rather than the form trying to hide and
    show fields as the frequency changes.
    """
    current = current or {}

    def value(key, fallback):
        got = current.get(key)
        return fallback if got in (None, "") else got

    return [
        {"key": "name", "label": "Schedule Name", "type": "string",
         "size": 40, "default": value("name", ""),
         "required": True},
        {"key": "enabled", "label": "Enabled", "type": "bool",
         "default": bool(value("enabled", 1))},
        {"key": "freq_type", "label": "Frequency", "type": "choice",
         "default": str(value("freq_type", 4)),
         "choices": [[str(k), v] for k, v in FREQ_TYPES.items()]},
        {"key": "freq_interval", "label": "Interval", "type": "int",
         "default": value("freq_interval", 1),
         "help": "days for Daily, a day of the month for Monthly, "
                 "a bitmask of weekdays for Weekly"},
        {"key": "freq_subday_type", "label": "Repeats Within the Day",
         "type": "choice", "default": str(value("freq_subday_type", 1)),
         "choices": [[str(k), v] for k, v in SUBDAY_TYPES.items()]},
        {"key": "freq_subday_interval", "label": "Repeat Every",
         "type": "int", "default": value("freq_subday_interval", 0),
         "help": "in the units chosen above"},
        {"key": "freq_relative_interval", "label": "Relative Interval",
         "type": "int", "default": value("freq_relative_interval", 0),
         "help": "for Monthly Relative: 1 first, 2 second, 4 third, "
                 "8 fourth, 16 last"},
        {"key": "freq_recurrence_factor", "label": "Every N Weeks/Months",
         "type": "int", "default": value("freq_recurrence_factor", 0),
         "help": "for Weekly and Monthly"},
        {"key": "active_start_time", "label": "Start Time", "type": "int",
         "default": value("active_start_time", 0), "help": "HHMMSS"},
        {"key": "active_end_time", "label": "End Time", "type": "int",
         "default": value("active_end_time", 235959), "help": "HHMMSS"},
        {"key": "active_start_date", "label": "Start Date", "type": "int",
         "default": value("active_start_date", 0), "help": "YYYYMMDD"},
        {"key": "active_end_date", "label": "End Date", "type": "int",
         "default": value("active_end_date", 99991231), "help": "YYYYMMDD"},
    ]


_NUMERIC = ("freq_type", "freq_interval", "freq_subday_type",
            "freq_subday_interval", "freq_relative_interval",
            "freq_recurrence_factor", "active_start_time",
            "active_end_time", "active_start_date", "active_end_date",
            "schedule_id")


def coerce_schedule(opts):
    """Return OPTS with the numbers as numbers, whatever the form sent."""
    out = dict(opts)
    for key in _NUMERIC:
        if key in out and out[key] not in (None, ""):
            try:
                out[key] = int(str(out[key]).strip())
            except (TypeError, ValueError):
                raise ValueError(f"{key} must be a number "
                                 f"(got {out[key]!r})")
    if not str(out.get("name") or "").strip():
        raise ValueError("a schedule needs a name")
    # The procedures take the name under this key.
    out["schedule_name"] = out["name"]
    return out
