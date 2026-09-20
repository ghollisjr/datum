"""SQL Agent Jobs panel — list, detail, and control SQL Agent jobs.

MSSQL only.  Uses msdb system tables for job metadata and
stored procedures for job control.

Sub-panels:
    jobs           — Job list (default)
    job-detail     — Steps, schedules, history for a specific job
    job-history    — Recent execution history for a specific job
"""

import json


def get_data(cursor, driver, args):
    """Return job data as a panel result dict."""
    if driver.dialect_name != "mssql":
        return {
            "panel": "jobs",
            "headers": ["note"],
            "rows": [["SQL Agent Jobs is only available on MSSQL"]],
            "row_id": None,
            "actions": [],
            "info": "SQL Agent Jobs requires a Microsoft SQL Server connection.",
        }

    # Sub-panel routing
    if args and args[0] == "detail" and len(args) >= 2:
        job_name = " ".join(args[1:])
        return _job_detail(cursor, job_name)
    elif args and args[0] == "history" and len(args) >= 2:
        job_name = " ".join(args[1:])
        return _job_history(cursor, job_name)
    else:
        return _job_list(cursor)


def run_action(cursor, driver, action_name, args):
    """Execute a job action."""
    from .. import envelope

    if driver.dialect_name != "mssql":
        envelope.error("SQL Agent Jobs actions are only available on MSSQL")
        return

    if action_name in ("detail", "history"):
        # These are navigation actions — re-fetch as sub-panel
        if not args:
            envelope.error(f"Job {action_name} requires a job name")
            return
        job_name = " ".join(args)
        result = get_data(cursor, driver, [action_name, job_name])
        envelope.admin_panel(result)
        return

    if action_name in ("edit-schedule", "update-schedule",
                        "new-schedule", "delete-schedule"):
        _handle_schedule_action(cursor, action_name, args)
        return

    if action_name in ("edit-step", "update-step",
                        "new-step", "delete-step"):
        _handle_step_action(cursor, action_name, args)
        return

    if not args:
        envelope.error(f"Job action '{action_name}' requires a job name")
        return

    job_name = " ".join(args)

    if action_name == "start-job":
        cursor.execute("EXEC msdb.dbo.sp_start_job @job_name = ?", [job_name])
        cursor.connection.commit()
        envelope.info(f"Started job: {job_name}")

    elif action_name == "stop-job":
        cursor.execute("EXEC msdb.dbo.sp_stop_job @job_name = ?", [job_name])
        cursor.connection.commit()
        envelope.info(f"Stopped job: {job_name}")

    elif action_name == "toggle-enable":
        # Check current enabled state
        cursor.execute(
            "SELECT enabled FROM msdb.dbo.sysjobs WHERE name = ?", [job_name])
        row = cursor.fetchone()
        if not row:
            envelope.error(f"Job not found: {job_name}")
            return
        new_state = 0 if row[0] == 1 else 1
        cursor.execute(
            "EXEC msdb.dbo.sp_update_job @job_name = ?, @enabled = ?",
            [job_name, new_state])
        cursor.connection.commit()
        label = "Enabled" if new_state else "Disabled"
        envelope.info(f"{label} job: {job_name}")

    else:
        envelope.error(f"Unknown job action: {action_name}")


def _handle_schedule_action(cursor, action_name, args):
    """Handle schedule-related actions."""
    from .. import envelope
    from . import schedules

    if action_name == "edit-schedule":
        # args: [schedule_name, job_name]
        if len(args) < 2:
            envelope.error("edit-schedule requires schedule_name and job_name")
            return
        schedule_name = args[0]
        job_name = " ".join(args[1:])
        sched = schedules.get_schedule(cursor, schedule_name, job_name)
        if not sched:
            envelope.error(f"Schedule not found: {schedule_name}")
            return
        # Send schedule data for the Emacs form editor
        envelope.admin_panel({
            "panel": "jobs",
            "sub_panel": "schedule-edit",
            "title": f"Edit Schedule: {schedule_name}",
            "schedule": sched,
            "freq_types": schedules.FREQ_TYPES,
            "subday_types": schedules.SUBDAY_TYPES,
            "headers": [],
            "rows": [],
            "row_id": None,
            "actions": [],
            "info": None,
            "context": {"job_name": job_name, "schedule_name": schedule_name},
        })

    elif action_name == "update-schedule":
        # args: [json-encoded schedule data]
        if not args:
            envelope.error("update-schedule requires schedule data")
            return
        try:
            schedule_data = json.loads(args[0])
        except json.JSONDecodeError as e:
            envelope.error(f"Invalid schedule JSON: {e}")
            return
        schedules.update_schedule(cursor, schedule_data)
        envelope.info(f"Updated schedule: {schedule_data.get('name', '?')}")

    elif action_name == "new-schedule":
        # args: [job_name, json-encoded schedule data]
        if len(args) < 2:
            envelope.error("new-schedule requires job_name and schedule data")
            return
        job_name = args[0]
        try:
            schedule_data = json.loads(args[1])
        except json.JSONDecodeError as e:
            envelope.error(f"Invalid schedule JSON: {e}")
            return
        schedules.create_schedule(cursor, job_name, schedule_data)
        envelope.info(f"Created schedule: {schedule_data.get('name', '?')}")

    elif action_name == "delete-schedule":
        # args: [schedule_name]
        if not args:
            envelope.error("delete-schedule requires a schedule name")
            return
        schedules.delete_schedule(cursor, args[0])
        envelope.info(f"Deleted schedule: {args[0]}")


def _handle_step_action(cursor, action_name, args):
    """Handle step-related actions."""
    from .. import envelope
    from . import steps

    if action_name == "edit-step":
        # args: [step_id, job_name...]
        if len(args) < 2:
            envelope.error("edit-step requires step_id and job_name")
            return
        step_id = args[0]
        job_name = " ".join(args[1:])
        step = steps.get_step(cursor, job_name, step_id)
        if not step:
            envelope.error(f"Step not found: {step_id}")
            return
        envelope.admin_panel({
            "panel": "jobs",
            "sub_panel": "step-edit",
            "title": f"Edit Step: {step['step_name']}",
            "step": step,
            "subsystems": steps.SUBSYSTEMS,
            "step_actions": steps.STEP_ACTIONS,
            "headers": [],
            "rows": [],
            "row_id": None,
            "actions": [],
            "info": None,
            "context": {"job_name": job_name},
        })

    elif action_name == "update-step":
        # args: [json-encoded step data]
        if len(args) < 2:
            envelope.error("update-step requires job_name and step data")
            return
        job_name = args[0]
        try:
            step_data = json.loads(args[1])
        except json.JSONDecodeError as e:
            envelope.error(f"Invalid step JSON: {e}")
            return
        steps.update_step(cursor, job_name, step_data)
        envelope.info(f"Updated step: {step_data.get('step_name', '?')}")

    elif action_name == "new-step":
        # args: [job_name, json-encoded step data]
        if len(args) < 2:
            envelope.error("new-step requires job_name and step data")
            return
        job_name = args[0]
        try:
            step_data = json.loads(args[1])
        except json.JSONDecodeError as e:
            envelope.error(f"Invalid step JSON: {e}")
            return
        steps.create_step(cursor, job_name, step_data)
        envelope.info(f"Created step: {step_data.get('step_name', '?')}")

    elif action_name == "delete-step":
        # args: [step_id, job_name...]
        if len(args) < 2:
            envelope.error("delete-step requires step_id and job_name")
            return
        step_id = args[0]
        job_name = " ".join(args[1:])
        steps.delete_step(cursor, job_name, step_id)
        envelope.info(f"Deleted step {step_id}")


def _job_list(cursor):
    """List all SQL Agent jobs with status information."""
    sql = """
        SELECT
            j.name                                       AS [Job Name],
            CASE j.enabled WHEN 1 THEN 'Yes' ELSE 'No' END AS [Enabled],
            c.name                                       AS [Category],
            CASE
                WHEN ja.start_execution_date IS NOT NULL
                     AND ja.stop_execution_date IS NULL
                THEN 'Executing'
                ELSE 'Idle'
            END                                          AS [Status],
            CASE
                WHEN ja.start_execution_date IS NOT NULL
                     AND ja.stop_execution_date IS NULL
                     AND ja.last_executed_step_id IS NOT NULL
                THEN CAST(ja.last_executed_step_id AS VARCHAR)
                     + ': '
                     + ISNULL(
                         (SELECT step_name FROM msdb.dbo.sysjobsteps
                          WHERE job_id = j.job_id
                            AND step_id = ja.last_executed_step_id),
                         '?')
                ELSE ''
            END                                          AS [Current Step],
            CASE
                WHEN h.run_status = 0 THEN 'Failed'
                WHEN h.run_status = 1 THEN 'Succeeded'
                WHEN h.run_status = 2 THEN 'Retry'
                WHEN h.run_status = 3 THEN 'Canceled'
                ELSE ''
            END                                          AS [Last Result],
            -- For running jobs: show start time; for finished: show last start
            CASE
                WHEN ja.start_execution_date IS NOT NULL
                     AND ja.stop_execution_date IS NULL
                THEN CONVERT(VARCHAR(19), ja.start_execution_date, 120)
                WHEN h.run_date IS NOT NULL
                THEN STUFF(STUFF(CAST(h.run_date AS VARCHAR(8)), 5, 0, '-'), 8, 0, '-')
                     + ' '
                     + STUFF(STUFF(RIGHT('000000' + CAST(h.run_time AS VARCHAR(6)), 6),
                                   3, 0, ':'), 6, 0, ':')
                ELSE ''
            END                                          AS [Last Start],
            -- For running jobs: show elapsed time; for finished: show end time
            CASE
                WHEN ja.start_execution_date IS NOT NULL
                     AND ja.stop_execution_date IS NULL
                THEN CONVERT(VARCHAR(8),
                     DATEADD(SECOND,
                             DATEDIFF(SECOND, ja.start_execution_date, GETDATE()),
                             0), 108)
                WHEN ja.stop_execution_date IS NOT NULL
                THEN CONVERT(VARCHAR(19), ja.stop_execution_date, 120)
                ELSE ''
            END                                          AS [End/Elapsed],
            -- Duration for finished jobs
            CASE
                WHEN h.run_duration IS NOT NULL
                     AND (ja.stop_execution_date IS NOT NULL
                          OR ja.start_execution_date IS NULL)
                THEN STUFF(STUFF(RIGHT('000000' + CAST(h.run_duration AS VARCHAR(6)), 6),
                                 3, 0, ':'), 6, 0, ':')
                ELSE ''
            END                                          AS [Duration],
            ISNULL(
                (SELECT TOP 1
                    STUFF(STUFF(CAST(js2.next_run_date AS VARCHAR(8)), 5, 0, '-'), 8, 0, '-')
                    + ' '
                    + STUFF(STUFF(RIGHT('000000' + CAST(js2.next_run_time AS VARCHAR(6)), 6),
                                  3, 0, ':'), 6, 0, ':')
                 FROM msdb.dbo.sysjobschedules js2
                 WHERE js2.job_id = j.job_id
                   AND js2.next_run_date > 0
                 ORDER BY js2.next_run_date, js2.next_run_time
                ), ''
            )                                            AS [Next Run],
            j.description                                AS [Description]
        FROM msdb.dbo.sysjobs j
        LEFT JOIN msdb.dbo.syscategories c ON j.category_id = c.category_id
        LEFT JOIN msdb.dbo.sysjobactivity ja
            ON ja.job_id = j.job_id
            AND ja.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions)
        OUTER APPLY (
            SELECT TOP 1 run_status, run_date, run_time, run_duration
            FROM msdb.dbo.sysjobhistory
            WHERE job_id = j.job_id AND step_id = 0
            ORDER BY run_date DESC, run_time DESC
        ) h
        ORDER BY
            CASE
                WHEN ja.start_execution_date IS NOT NULL
                     AND ja.stop_execution_date IS NULL
                THEN 0
                ELSE 1
            END,
            j.name
    """
    cursor.execute(sql)
    headers = [col[0] for col in cursor.description]
    rows = [[str(v) if v is not None else "" for v in row]
            for row in cursor.fetchall()]

    return {
        "panel": "jobs",
        "headers": headers,
        "rows": rows,
        "row_id": 0,  # Job Name column
        "actions": [
            {"key": "s", "label": "Start job", "command": "start-job"},
            {"key": "S", "label": "Stop job", "command": "stop-job"},
            {"key": "e", "label": "Enable/Disable toggle", "command": "toggle-enable"},
            {"key": "d", "label": "View detail", "command": "detail"},
            {"key": "H", "label": "View history", "command": "history"},
        ],
        "info": None,
    }


def _job_detail(cursor, job_name):
    """Get detailed info for a specific job: steps and schedules."""
    # Steps
    steps_sql = """
        SELECT
            s.step_id                        AS [Step],
            s.step_name                      AS [Name],
            CASE
                WHEN ja.start_execution_date IS NOT NULL
                     AND ja.stop_execution_date IS NULL
                     AND ja.last_executed_step_id = s.step_id
                THEN 'Running'
                ELSE ''
            END                              AS [Status],
            CASE s.subsystem
                WHEN 'TSQL' THEN 'T-SQL'
                WHEN 'CmdExec' THEN 'OS Cmd'
                WHEN 'SSIS' THEN 'SSIS'
                WHEN 'PowerShell' THEN 'PS'
                ELSE s.subsystem
            END                              AS [Type],
            s.database_name                  AS [Database],
            CASE s.on_success_action
                WHEN 1 THEN 'Quit success'
                WHEN 2 THEN 'Quit fail'
                WHEN 3 THEN 'Next step'
                WHEN 4 THEN 'Go to step ' + CAST(s.on_success_step_id AS VARCHAR)
            END                              AS [On Success],
            CASE s.on_fail_action
                WHEN 1 THEN 'Quit success'
                WHEN 2 THEN 'Quit fail'
                WHEN 3 THEN 'Next step'
                WHEN 4 THEN 'Go to step ' + CAST(s.on_fail_step_id AS VARCHAR)
            END                              AS [On Failure]
        FROM msdb.dbo.sysjobsteps s
        LEFT JOIN msdb.dbo.sysjobactivity ja
            ON ja.job_id = s.job_id
            AND ja.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions)
        WHERE s.job_id = (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = ?)
        ORDER BY s.step_id
    """
    cursor.execute(steps_sql, [job_name])
    step_headers = [col[0] for col in cursor.description]
    step_rows = [[str(v) if v is not None else "" for v in row]
                 for row in cursor.fetchall()]

    # Schedules
    sched_sql = """
        SELECT
            s.name                           AS [Schedule],
            CASE s.enabled WHEN 1 THEN 'Yes' ELSE 'No' END AS [Enabled],
            CASE s.freq_type
                WHEN 1 THEN 'Once'
                WHEN 4 THEN 'Daily'
                WHEN 8 THEN 'Weekly'
                WHEN 16 THEN 'Monthly'
                WHEN 32 THEN 'Monthly relative'
                WHEN 64 THEN 'On Agent start'
                WHEN 128 THEN 'On idle'
                ELSE CAST(s.freq_type AS VARCHAR)
            END                              AS [Frequency],
            s.freq_interval                  AS [Interval],
            STUFF(STUFF(RIGHT('000000' + CAST(s.active_start_time AS VARCHAR(6)), 6),
                        3, 0, ':'), 6, 0, ':') AS [Start Time],
            STUFF(STUFF(RIGHT('000000' + CAST(s.active_end_time AS VARCHAR(6)), 6),
                        3, 0, ':'), 6, 0, ':') AS [End Time]
        FROM msdb.dbo.sysschedules s
        JOIN msdb.dbo.sysjobschedules js ON s.schedule_id = js.schedule_id
        WHERE js.job_id = (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = ?)
        ORDER BY s.name
    """
    cursor.execute(sched_sql, [job_name])
    sched_headers = [col[0] for col in cursor.description]
    sched_rows = [[str(v) if v is not None else "" for v in row]
                  for row in cursor.fetchall()]

    return {
        "panel": "jobs",
        "sub_panel": "detail",
        "title": f"Job Detail: {job_name}",
        "sections": [
            {
                "title": "Steps",
                "headers": step_headers,
                "rows": step_rows,
                "row_id": 0,
                "actions": [
                    {"key": "E", "label": "Edit step", "command": "edit-step"},
                    {"key": "N", "label": "New step", "command": "new-step"},
                    {"key": "D", "label": "Delete step", "command": "delete-step"},
                ],
            },
            {
                "title": "Schedules",
                "headers": sched_headers,
                "rows": sched_rows,
                "row_id": 0,
                "actions": [
                    {"key": "E", "label": "Edit schedule", "command": "edit-schedule"},
                    {"key": "N", "label": "New schedule", "command": "new-schedule"},
                    {"key": "D", "label": "Delete schedule", "command": "delete-schedule"},
                ],
            },
        ],
        "headers": step_headers,
        "rows": step_rows,
        "row_id": None,
        "actions": [],
        "info": f"Job: {job_name}",
        "parent_panel": "jobs",
        "context": {"job_name": job_name},
    }


def _job_history(cursor, job_name):
    """Get recent execution history for a job."""
    sql = """
        SELECT TOP 20
            CASE h.run_status
                WHEN 0 THEN 'Failed'
                WHEN 1 THEN 'Succeeded'
                WHEN 2 THEN 'Retry'
                WHEN 3 THEN 'Canceled'
                WHEN 4 THEN 'In Progress'
            END                              AS [Status],
            CASE WHEN h.step_id = 0
                 THEN '(outcome)'
                 ELSE CAST(h.step_id AS VARCHAR) + ': ' + h.step_name
            END                              AS [Step],
            STUFF(STUFF(CAST(h.run_date AS VARCHAR(8)), 5, 0, '-'), 8, 0, '-')
                + ' '
                + STUFF(STUFF(RIGHT('000000' + CAST(h.run_time AS VARCHAR(6)), 6),
                              3, 0, ':'), 6, 0, ':')
                                             AS [Run Date],
            STUFF(STUFF(RIGHT('000000' + CAST(h.run_duration AS VARCHAR(6)), 6),
                        3, 0, ':'), 6, 0, ':')
                                             AS [Duration],
            h.message                        AS [Message]
        FROM msdb.dbo.sysjobhistory h
        WHERE h.job_id = (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = ?)
        ORDER BY h.run_date DESC, h.run_time DESC, h.step_id
    """
    cursor.execute(sql, [job_name])
    headers = [col[0] for col in cursor.description]
    rows = [[str(v) if v is not None else "" for v in row]
            for row in cursor.fetchall()]

    return {
        "panel": "jobs",
        "sub_panel": "history",
        "title": f"Job History: {job_name}",
        "headers": headers,
        "rows": rows,
        "row_id": None,
        "actions": [],
        "info": f"Recent history for: {job_name}",
        "parent_panel": "jobs",
        "context": {"job_name": job_name},
    }
