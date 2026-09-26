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

    if action_name in ("edit-schedule", "update-schedule", "create-schedule",
                        "new-schedule", "delete-schedule"):
        _handle_schedule_action(cursor, action_name, args)
        return

    if action_name in ("edit-step", "update-step", "create-step",
                        "new-step", "delete-step"):
        _handle_step_action(cursor, action_name, args)
        return

    if action_name in ("new-job", "edit-job", "create-job", "update-job",
                       "drop-job-check", "delete-job"):
        _handle_job_action(cursor, action_name, args)
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

    try:
        _schedule_action(cursor, action_name, args)
    except ValueError as err:
        envelope.error(f":admin jobs {action_name} — {err}")
    except Exception as err:
        envelope.error(f":admin jobs {action_name} — {_job_error(err)}")


def _schedule_action(cursor, action_name, args):
    """Do the work, with errors reported by the caller."""
    from .. import envelope
    from . import schedules

    if action_name in ("edit-schedule", "new-schedule"):
        # The generic form, as the step editor now uses: a renderer of
        # its own meant doing without the field navigation the other
        # wizards have.
        editing = action_name == "edit-schedule"
        if editing:
            if len(args) < 2:
                envelope.error(
                    "edit-schedule requires schedule_name and job_name")
                return
            schedule_name = args[0]
            job_name = " ".join(args[1:])
            current = schedules.get_schedule(cursor, schedule_name, job_name)
            if not current:
                envelope.error(f"Schedule not found: {schedule_name}")
                return
        else:
            job_name = " ".join(args)
            current = None
        if not job_name:
            envelope.error(f"{action_name} requires a job name")
            return

        values = dict(current or {})
        values["job_name"] = job_name
        if editing:
            values["schedule_id"] = current.get("schedule_id")
            # get_schedule reports the name under "name", which
            # dict(current) has already carried across.
            values["name"] = current.get("name", "")
        envelope.admin_panel({
            "panel": "jobs",
            "sub_panel": "form",
            "title": (f"Schedule: {values['name']}" if editing
                      else f"New Schedule for {job_name}"),
            "form": {
                "fields": [{k: v for k, v in field.items() if v is not None}
                           for field in schedules.schedule_options(
                               cursor, current)],
                "values": values,
                "submit_action": ("update-schedule" if editing
                                  else "create-schedule"),
                "submit_label": ("Save Schedule" if editing
                                 else "Create Schedule"),
                "notes": [f"Schedule of job {job_name}.",
                          "Which fields matter depends on the frequency; "
                          "the help under each says which."],
            },
            "headers": [],
            "rows": [],
            "row_id": None,
            "actions": [],
            "info": None,
            "context": {"job_name": job_name},
        })

    elif action_name in ("update-schedule", "create-schedule"):
        opts = schedules.coerce_schedule(_decode_job_payload(args))
        job_name = (opts.get("job_name") or "").strip()
        if action_name == "update-schedule":
            if not opts.get("schedule_id"):
                envelope.error("update-schedule did not say which schedule")
                return
            schedules.update_schedule(cursor, opts)
            envelope.info(f"Updated schedule: {opts.get('name', '?')}")
        else:
            if not job_name:
                envelope.error("create-schedule did not say which job")
                return
            schedules.create_schedule(cursor, job_name, opts)
            envelope.info(f"Created schedule: {opts.get('name', '?')}")
        cursor.connection.commit()
        if job_name:
            _refresh_detail(cursor, job_name)

    elif action_name == "delete-schedule":
        if not args:
            envelope.error("delete-schedule requires a schedule name")
            return
        schedules.delete_schedule(cursor, args[0])
        cursor.connection.commit()
        envelope.info(f"Deleted schedule: {args[0]}")


def _handle_step_action(cursor, action_name, args):
    """Handle step-related actions."""
    from .. import envelope
    from . import steps

    try:
        _step_action(cursor, action_name, args)
    except ValueError as err:
        envelope.error(f":admin jobs {action_name} — {err}")
    except Exception as err:
        envelope.error(f":admin jobs {action_name} — {_job_error(err)}")


def _step_action(cursor, action_name, args):
    """Do the work, with errors reported by the caller."""
    from .. import envelope
    from . import steps

    if action_name in ("edit-step", "new-step"):
        # The generic form, rather than a renderer of its own: that is
        # where the field navigation, the completion and the payload
        # chunking live.  A step's command is long enough that sending
        # it on the command line was losing the end of it.
        editing = action_name == "edit-step"
        if editing:
            if len(args) < 2:
                envelope.error("edit-step requires step_id and job_name")
                return
            step_id = args[0]
            job_name = " ".join(args[1:])
            current = steps.get_step(cursor, job_name, step_id)
            if not current:
                envelope.error(f"Step not found: {step_id}")
                return
        else:
            job_name = " ".join(args)
            step_id = None
            current = None
        if not job_name:
            envelope.error(f"{action_name} requires a job name")
            return

        values = dict(current or {})
        values["job_name"] = job_name
        if editing:
            values["step_id"] = current.get("step_id")
        envelope.admin_panel({
            "panel": "jobs",
            "sub_panel": "form",
            "title": (f"Step: {current['step_name']}" if editing
                      else f"New Step in {job_name}"),
            "form": {
                "fields": [{k: v for k, v in field.items() if v is not None}
                           for field in steps.step_options(cursor, current)],
                "values": values,
                "submit_action": "update-step" if editing else "create-step",
                "submit_label": "Save Step" if editing else "Create Step",
                "notes": [f"Step of job {job_name}."],
            },
            "headers": [],
            "rows": [],
            "row_id": None,
            "actions": [],
            "info": None,
            "context": {"job_name": job_name},
        })

    elif action_name in ("update-step", "create-step"):
        opts = steps.coerce_step(_decode_job_payload(args))
        job_name = (opts.get("job_name") or "").strip()
        if not job_name:
            envelope.error(f"{action_name} did not say which job")
            return
        # Checked here rather than left to fail when the job runs.
        steps.validate_flow(cursor, job_name, opts,
                            creating=action_name == "create-step")
        if action_name == "update-step":
            steps.update_step(cursor, job_name, opts)
            envelope.info(f"Updated step: {opts.get('step_name', '?')}")
        else:
            steps.create_step(cursor, job_name, opts)
            envelope.info(f"Created step: {opts.get('step_name', '?')}")
        cursor.connection.commit()
        _refresh_detail(cursor, job_name)

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

    # sysjobactivity gave the last step to finish, which is the wrong
    # one while the next is running and nothing at all on the first.
    if "Current Step" in headers:
        column = headers.index("Current Step")
        name_column = 0
        for job_name in _jobs_running(cursor):
            _step_id, label = _running_step(cursor, job_name)
            if not label:
                continue
            for row in rows:
                if row[name_column] == job_name:
                    row[column] = label

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
            {"key": "N", "label": "New job", "command": "new-job"},
            {"key": "E", "label": "Edit job", "command": "edit-job"},
            {"key": "D", "label": "Delete job", "command": "drop-job-check"},
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

    # The job's own properties, at the top of its own tree: editing a
    # job is where its steps are, so the two should not be down
    # different paths.
    # Which step is running, asked of the server rather than inferred
    # from the last one to finish.
    running_id, _running_label = _running_step(cursor, job_name)
    if running_id is not None and "Status" in step_headers:
        status_column = step_headers.index("Status")
        step_column = step_headers.index("Step")
        for row in step_rows:
            row[status_column] = ("Running"
                                  if str(row[step_column]) == str(running_id)
                                  else "")

    from . import jobdefs
    from . import steps as steps_module
    flow = steps_module.flow_problems(cursor, job_name)
    properties = jobdefs.get_job(cursor, job_name) or {}
    when = dict(jobdefs.WHEN)
    job_rows = [
        ["Name", properties.get("name", job_name)],
        ["Enabled", "Yes" if properties.get("enabled") else "No"],
        ["Owner", properties.get("owner", "")],
        ["Category", properties.get("category", "")],
        ["Description", properties.get("description", "")],
        ["Write to the event log",
         when.get(properties.get("notify_eventlog", "0"), "")],
        ["Email an operator",
         when.get(properties.get("notify_email", "0"), "")],
        ["Operator", properties.get("operator", "")],
        ["Delete the job", when.get(properties.get("delete_level", "0"), "")],
    ]

    return {
        "panel": "jobs",
        "sub_panel": "detail",
        "title": f"Job: {job_name}",
        "sections": [
            {
                "title": "Job",
                "headers": ["Property", "Value"],
                "rows": job_rows,
                "row_id": None,
                "actions": [
                    {"key": "E", "label": "Edit these", "command": "edit-job"},
                    {"key": "D", "label": "Delete job",
                     "command": "drop-job-check"},
                ],
            },
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
        # A flow that points nowhere is only reported by the server when
        # the job runs, so it is said here instead.
        "info": "  ".join([f"Job: {job_name}"] + flow),
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


def _decode_job_payload(args):
    """Return the base64 JSON payload a job form submits."""
    import base64

    if not args:
        return {}
    return json.loads(base64.b64decode(args[0]).decode("utf-8"))


def _job_form(cursor, current, submit, title, notes):
    """Send the form for a job's own properties."""
    from .. import envelope
    from . import jobdefs

    fields = [{k: v for k, v in field.items() if v is not None}
              for field in jobdefs.job_options(cursor, current)]
    values = dict(current or {})
    if current:
        # What it was called when the form opened, so an edited name
        # reads as a rename rather than as a different job.
        values["name_original"] = current.get("name", "")
    envelope.admin_panel({
        "panel": "jobs",
        "sub_panel": "form",
        "title": title,
        "form": {"fields": fields,
                 "values": values,
                 "submit_action": submit,
                 "submit_label": "Save Job" if current else "Create Job",
                 "notes": notes},
        "headers": [],
        "rows": [],
        "row_id": None,
        "actions": [],
        "info": None,
    })


def _handle_job_action(cursor, action_name, args):
    """Create, edit or delete the job itself.

    Its steps and schedules are edited from the detail view; these are
    the job's own properties, which had no way to be set at all.
    """
    from .. import envelope
    from . import jobdefs

    try:
        if action_name == "new-job":
            _job_form(cursor, None, "create-job", "New Job",
                      ["Steps and schedules are added from the job's "
                       "detail view, with d and then N.",
                       "The job is given the local server to run on, "
                       "which it needs to run at all."])

        elif action_name == "edit-job":
            name = " ".join(args).strip()
            current = jobdefs.get_job(cursor, name)
            if not current:
                envelope.error(f"Job not found: {name}")
                return
            _job_form(cursor, current, "update-job", f"Job: {name}",
                      ["Changing the name renames the job rather than "
                       "making another."])

        elif action_name == "create-job":
            opts = _decode_job_payload(args)
            name = jobdefs.create_job(cursor, opts)
            cursor.connection.commit()
            envelope.info(f"Created job: {name}")
            _refresh_jobs(cursor)

        elif action_name == "update-job":
            opts = _decode_job_payload(args)
            name = jobdefs.update_job(cursor, opts)
            cursor.connection.commit()
            envelope.info(f"Updated job: {name}")
            _refresh_jobs(cursor)

        elif action_name == "drop-job-check":
            name = " ".join(args).strip()
            summary = jobdefs.job_summary(cursor, name)
            if summary is None:
                envelope.error(f"Job not found: {name}")
                return
            # The same shape the other dangerous actions use: a form
            # that says what goes, and wants the name typed back.
            envelope.admin_panel({
                "panel": "jobs",
                "sub_panel": "form",
                "title": f"Delete Job: {name}",
                "form": {
                    "fields": [],
                    "values": {"name": name},
                    "submit_action": "delete-job",
                    "submit_label": "Delete Job",
                    "notes": [
                        f"Deleting {name} takes its steps, its schedules "
                        f"and its history with it.",
                        "",
                        f"Steps: {summary['steps']}",
                        f"Schedules: {summary['schedules']}",
                        f"History rows: {summary['history']}",
                    ],
                    "confirm_text": name,
                    "danger": True,
                },
                "headers": [],
                "rows": [],
                "row_id": None,
                "actions": [],
                "info": None,
                "context": {"job_name": name},
            })

        elif action_name == "delete-job":
            # Sent as a payload when it comes from the confirmation
            # form, and as a bare name when asked for directly.
            opts = {}
            try:
                opts = _decode_job_payload(args)
            except Exception:
                opts = {}
            name = jobdefs.delete_job(
                cursor, (opts.get("name") or " ".join(args)).strip())
            cursor.connection.commit()
            envelope.info(f"Deleted job: {name}")
            _refresh_jobs(cursor)

    except ValueError as err:
        envelope.error(f":admin jobs {action_name} — {err}")
    except Exception as err:
        envelope.error(f":admin jobs {action_name} — {_job_error(err)}")


def _job_error(err):
    """Return the useful part of an msdb procedure error."""
    text = str(err)
    if text.startswith("(") and "]" in text:
        text = text.rsplit("]", 1)[-1]
    return text.strip().strip("()'\" ") or "failed"


def _refresh_jobs(cursor):
    """Re-send the job list so the panel reflects the change."""
    from .. import envelope

    try:
        envelope.admin_panel(_job_list(cursor))
    except Exception:
        pass


def _refresh_detail(cursor, job_name):
    """Re-send the job detail so the steps shown reflect the change."""
    from .. import envelope

    try:
        envelope.admin_panel(_job_detail(cursor, job_name))
    except Exception:
        pass


def _running_step(cursor, job_name):
    """Return (step_id, label) for the step a job is running, or (None, "").

    sysjobactivity only records the last step to *finish*, so using it
    names the wrong step for the whole time the next one is running --
    and names nothing at all for a job still on its first step.
    sp_help_job reports the step actually executing, which is what SSMS
    shows.  It is a procedure rather than a view, so it is asked only
    about jobs already known to be running.
    """
    try:
        cursor.execute("EXEC msdb.dbo.sp_help_job @job_name = ?, "
                       "@job_aspect = N'JOB'", [job_name])
        columns = [column[0] for column in cursor.description]
        row = cursor.fetchone()
        while cursor.nextset():
            pass
    except Exception:
        return None, ""
    if not row or "current_execution_step" not in columns:
        return None, ""
    label = row[columns.index("current_execution_step")]
    if not label:
        return None, ""
    label = str(label).strip()
    # Reported as "2 (slow)".
    head = label.split(" ", 1)[0]
    try:
        return int(head), label
    except ValueError:
        return None, label


def _jobs_running(cursor):
    """Return the names of the jobs executing right now."""
    try:
        cursor.execute("""
            SELECT CAST(j.name AS NVARCHAR(128))
            FROM msdb.dbo.sysjobactivity ja
            JOIN msdb.dbo.sysjobs j ON j.job_id = ja.job_id
            WHERE ja.start_execution_date IS NOT NULL
              AND ja.stop_execution_date IS NULL
        """)
        return [row[0] for row in cursor.fetchall()]
    except Exception:
        return []
