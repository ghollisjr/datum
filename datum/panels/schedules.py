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
    """
    sid = schedule_data["schedule_id"]
    sql = """
        EXEC msdb.dbo.sp_update_schedule
            @schedule_id = ?,
            @name = ?,
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
