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
