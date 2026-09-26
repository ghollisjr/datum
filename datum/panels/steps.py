"""Job Step CRUD operations for SQL Agent Jobs.

Handles step reading, creation, updating, and deletion via
msdb stored procedures.
"""

import json


# Subsystem labels for the Emacs UI
SUBSYSTEMS = {
    "TSQL": "T-SQL",
    "CmdExec": "OS Command",
    "PowerShell": "PowerShell",
    "SSIS": "SSIS",
}

# Step action labels
STEP_ACTIONS = {
    1: "Quit with success",
    2: "Quit with failure",
    3: "Go to next step",
    4: "Go to step...",
}


def get_step(cursor, job_name, step_id):
    """Get full step details for editing."""
    sql = """
        SELECT
            step_id,
            step_name,
            subsystem,
            command,
            database_name,
            retry_attempts,
            retry_interval,
            on_success_action,
            on_success_step_id,
            on_fail_action,
            on_fail_step_id
        FROM msdb.dbo.sysjobsteps
        WHERE job_id = (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = ?)
          AND step_id = ?
    """
    cursor.execute(sql, [job_name, int(step_id)])
    row = cursor.fetchone()
    if not row:
        return None

    return {
        "step_id": row[0],
        "step_name": row[1],
        "subsystem": row[2],
        "command": row[3],
        "database_name": row[4],
        "retry_attempts": row[5],
        "retry_interval": row[6],
        "on_success_action": row[7],
        "on_success_step_id": row[8],
        "on_fail_action": row[9],
        "on_fail_step_id": row[10],
    }


def update_step(cursor, job_name, step_data):
    """Update an existing job step.

    step_data is a dict with step_id and fields to update.
    """
    sql = """
        EXEC msdb.dbo.sp_update_jobstep
            @job_name = ?,
            @step_id = ?,
            @step_name = ?,
            @subsystem = ?,
            @command = ?,
            @database_name = ?,
            @retry_attempts = ?,
            @retry_interval = ?,
            @on_success_action = ?,
            @on_success_step_id = ?,
            @on_fail_action = ?,
            @on_fail_step_id = ?
    """
    cursor.execute(sql, [
        job_name,
        step_data["step_id"],
        step_data["step_name"],
        step_data.get("subsystem", "TSQL"),
        step_data.get("command", ""),
        step_data.get("database_name", ""),
        step_data.get("retry_attempts", 0),
        step_data.get("retry_interval", 0),
        step_data.get("on_success_action", 3),
        step_data.get("on_success_step_id", 0),
        step_data.get("on_fail_action", 2),
        step_data.get("on_fail_step_id", 0),
    ])
    cursor.connection.commit()


def create_step(cursor, job_name, step_data):
    """Create a new job step.

    step_data is a dict with step parameters.
    """
    sql = """
        EXEC msdb.dbo.sp_add_jobstep
            @job_name = ?,
            @step_name = ?,
            @subsystem = ?,
            @command = ?,
            @database_name = ?,
            @retry_attempts = ?,
            @retry_interval = ?,
            @on_success_action = ?,
            @on_success_step_id = ?,
            @on_fail_action = ?,
            @on_fail_step_id = ?
    """
    cursor.execute(sql, [
        job_name,
        step_data["step_name"],
        step_data.get("subsystem", "TSQL"),
        step_data.get("command", ""),
        step_data.get("database_name", ""),
        step_data.get("retry_attempts", 0),
        step_data.get("retry_interval", 0),
        step_data.get("on_success_action", 3),
        step_data.get("on_success_step_id", 0),
        step_data.get("on_fail_action", 2),
        step_data.get("on_fail_step_id", 0),
    ])
    cursor.connection.commit()


def delete_step(cursor, job_name, step_id):
    """Delete a job step by step_id."""
    cursor.execute(
        "EXEC msdb.dbo.sp_delete_jobstep @job_name = ?, @step_id = ?",
        [job_name, int(step_id)]
    )
    cursor.connection.commit()


def step_options(cursor, current=None):
    """Field descriptors for a job step.

    The same shape every other wizard uses, so the step editor gets the
    navigation, the completion and the payload chunking the generic form
    already has, instead of a bespoke renderer of its own.
    """
    current = current or {}
    try:
        cursor.execute("SELECT name FROM sys.databases WHERE state = 0 "
                       "ORDER BY name")
        databases = [row[0] for row in cursor.fetchall()]
    except Exception:
        databases = []
    return [
        {"key": "step_name", "label": "Step Name", "type": "string",
         "size": 40, "default": current.get("step_name", ""),
         "required": True},
        {"key": "subsystem", "label": "Type", "type": "choice",
         "default": current.get("subsystem", "TSQL"),
         "choices": [[k, v] for k, v in SUBSYSTEMS.items()]},
        {"key": "database_name", "label": "Database",
         "type": "completing" if databases else "string",
         "default": current.get("database_name") or "master",
         "completions": databases or None,
         "help": "for a T-SQL step"},
        {"key": "command", "label": "Command", "type": "text",
         "default": current.get("command", ""),
         "help": "the script this step runs"},
        {"key": "retry_attempts", "label": "Retry Attempts", "type": "int",
         "default": current.get("retry_attempts", 0)},
        {"key": "retry_interval", "label": "Retry Interval (minutes)",
         "type": "int", "default": current.get("retry_interval", 0)},
        # A new step is appended, so it is the last one, and "go to the
        # next step" would point at nothing -- which the server accepts
        # and then fails on at run time.  So a new step quits, and a
        # chain is made by saying so on the step before it.
        {"key": "on_success_action", "label": "On Success", "type": "choice",
         "default": str(current.get("on_success_action", 1)),
         "choices": [[str(k), v] for k, v in STEP_ACTIONS.items()]},
        {"key": "on_success_step_id", "label": "On Success, Go To Step",
         "type": "int", "default": current.get("on_success_step_id", 0),
         "help": "only for \"Go to step...\""},
        {"key": "on_fail_action", "label": "On Failure", "type": "choice",
         "default": str(current.get("on_fail_action", 2)),
         "choices": [[str(k), v] for k, v in STEP_ACTIONS.items()]},
        {"key": "on_fail_step_id", "label": "On Failure, Go To Step",
         "type": "int", "default": current.get("on_fail_step_id", 0),
         "help": "only for \"Go to step...\""},
    ]


def coerce_step(opts):
    """Return OPTS with the numbers as numbers, whatever the form sent."""
    out = dict(opts)
    for key in ("retry_attempts", "retry_interval", "on_success_action",
                "on_success_step_id", "on_fail_action", "on_fail_step_id",
                "step_id"):
        if key in out and out[key] not in (None, ""):
            try:
                out[key] = int(str(out[key]).strip())
            except (TypeError, ValueError):
                raise ValueError(f"{key} must be a number "
                                 f"(got {out[key]!r})")
    if not str(out.get("step_name") or "").strip():
        raise ValueError("a step needs a name")
    return out


# Step actions that send the job somewhere else: 3 is the next step, 4
# is a named one.  Both can point at a step that is not there.
_GO_TO_NEXT = 3
_GO_TO_STEP = 4


def _existing_step_ids(cursor, job_name):
    cursor.execute("""
        SELECT s.step_id FROM msdb.dbo.sysjobsteps s
        JOIN msdb.dbo.sysjobs j ON j.job_id = s.job_id
        WHERE j.name = ? ORDER BY s.step_id
    """, [job_name])
    return [int(row[0]) for row in cursor.fetchall()]


def validate_flow(cursor, job_name, opts, creating):
    """Refuse a step whose flow points at a step that is not there.

    A last step told to go to the next one has nowhere to go, and the
    job fails at run time with nothing to say about why -- the server
    accepts the step happily and only complains when it runs.  Going to
    a step by number has the same problem when the number is not there.
    """
    ids = _existing_step_ids(cursor, job_name)
    if creating:
        # Appended, so it becomes the last step.
        step_id = (max(ids) + 1) if ids else 1
        ids = ids + [step_id]
    else:
        try:
            step_id = int(opts.get("step_id"))
        except (TypeError, ValueError):
            raise ValueError("the form did not say which step to change")
    last = max(ids) if ids else step_id

    for key, target_key, when in (
            ("on_success_action", "on_success_step_id", "succeeds"),
            ("on_fail_action", "on_fail_step_id", "fails")):
        action = opts.get(key)
        if action is None:
            continue
        action = int(action)
        if action == _GO_TO_NEXT and step_id >= last:
            raise ValueError(
                f"Step {step_id} is the last step, so \"go to the next "
                f"step\" when it {when} has nowhere to go — the job would "
                f"fail at run time. Use \"quit with success\" or \"quit "
                f"with failure\" instead, or add the step it should go to "
                f"first.")
        if action == _GO_TO_STEP:
            try:
                target = int(opts.get(target_key) or 0)
            except (TypeError, ValueError):
                raise ValueError(f"{target_key} must be a number")
            if target not in ids:
                known = ", ".join(str(i) for i in ids) or "none"
                raise ValueError(
                    f"Step {step_id} is told to go to step {target} when it "
                    f"{when}, and there is no such step. This job has: "
                    f"{known}.")


def flow_problems(cursor, job_name):
    """Return what is wrong with a job's step flow, in plain words.

    Says it for a job that already has the fault, which the server will
    not mention until the job runs and fails.
    """
    cursor.execute("""
        SELECT s.step_id, CAST(s.step_name AS NVARCHAR(128)),
               s.on_success_action, s.on_success_step_id,
               s.on_fail_action, s.on_fail_step_id
        FROM msdb.dbo.sysjobsteps s
        JOIN msdb.dbo.sysjobs j ON j.job_id = s.job_id
        WHERE j.name = ? ORDER BY s.step_id
    """, [job_name])
    rows = cursor.fetchall()
    if not rows:
        return []
    ids = [int(r[0]) for r in rows]
    last = max(ids)
    problems = []
    for step_id, name, ok_action, ok_step, fail_action, fail_step in rows:
        step_id = int(step_id)
        for action, target, when in ((int(ok_action or 0), int(ok_step or 0),
                                      "succeeds"),
                                     (int(fail_action or 0), int(fail_step or 0),
                                      "fails")):
            if action == _GO_TO_NEXT and step_id >= last:
                problems.append(
                    f"Step {step_id} ({name}) goes to the next step when it "
                    f"{when}, but it is the last step — the job will fail "
                    f"there.")
            elif action == _GO_TO_STEP and target not in ids:
                problems.append(
                    f"Step {step_id} ({name}) goes to step {target} when it "
                    f"{when}, and there is no such step.")
    return problems
