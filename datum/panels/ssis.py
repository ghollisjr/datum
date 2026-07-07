"""SSIS Packages panel — list, inspect, and run SSIS packages.

MSSQL only.  Requires SSISDB catalog to be configured on the server.

Sub-panels:
    ssis              — Package list (default)
    ssis executions   — Execution history for a specific package
"""


def get_data(cursor, driver, args):
    """Return SSIS package data as a panel result dict."""
    if driver.dialect_name != "mssql":
        return {
            "panel": "ssis",
            "headers": ["note"],
            "rows": [["SSIS Packages is only available on MSSQL"]],
            "row_id": None,
            "actions": [],
            "info": "SSIS Packages requires a Microsoft SQL Server connection.",
        }

    # Check if SSISDB exists
    try:
        cursor.execute(
            "SELECT DB_ID('SSISDB')")
        row = cursor.fetchone()
        if not row or row[0] is None:
            return {
                "panel": "ssis",
                "headers": ["note"],
                "rows": [["SSISDB catalog not found on this server"]],
                "row_id": None,
                "actions": [],
                "info": "SSISDB is not configured. Deploy SSIS packages to enable this panel.",
            }
    except Exception:
        return {
            "panel": "ssis",
            "headers": ["note"],
            "rows": [["Could not check for SSISDB catalog"]],
            "row_id": None,
            "actions": [],
            "info": None,
        }

    if args and args[0] == "executions" and len(args) >= 2:
        package_name = " ".join(args[1:])
        return _execution_history(cursor, package_name)
    else:
        return _package_list(cursor)


def run_action(cursor, driver, action_name, args):
    """Execute an SSIS action."""
    from .. import envelope

    if driver.dialect_name != "mssql":
        envelope.error("SSIS actions are only available on MSSQL")
        return

    if action_name == "run-package":
        if len(args) < 3:
            envelope.error(
                "run-package requires folder, project, and package name")
            return
        folder = args[0]
        project = args[1]
        package = args[2]
        _run_package(cursor, folder, project, package)

    elif action_name == "executions":
        if not args:
            envelope.error("executions requires a package name")
            return
        package_name = " ".join(args)
        result = get_data(cursor, driver, ["executions", package_name])
        envelope.admin_panel(result)

    else:
        envelope.error(f"Unknown SSIS action: {action_name}")


def _package_list(cursor):
    """List all SSIS packages with latest execution status."""
    sql = """
        SELECT
            f.name                               AS [Folder],
            proj.name                            AS [Project],
            pkg.name                             AS [Package],
            CASE
                WHEN e.status = 1 THEN 'Created'
                WHEN e.status = 2 THEN 'Running'
                WHEN e.status = 3 THEN 'Canceled'
                WHEN e.status = 4 THEN 'Failed'
                WHEN e.status = 5 THEN 'Pending'
                WHEN e.status = 6 THEN 'Ended unexpectedly'
                WHEN e.status = 7 THEN 'Succeeded'
                WHEN e.status = 8 THEN 'Stopping'
                WHEN e.status = 9 THEN 'Completed'
                ELSE 'Unknown'
            END                                  AS [Last Status],
            CONVERT(VARCHAR(19), e.start_time, 120)   AS [Last Run],
            CONVERT(VARCHAR(19), e.end_time, 120)     AS [End Time],
            pkg.description                      AS [Description]
        FROM SSISDB.catalog.packages pkg
        JOIN SSISDB.catalog.projects proj ON pkg.project_id = proj.project_id
        JOIN SSISDB.catalog.folders f ON proj.folder_id = f.folder_id
        OUTER APPLY (
            SELECT TOP 1 status, start_time, end_time
            FROM SSISDB.catalog.executions ex
            WHERE ex.package_name = pkg.name
              AND ex.project_name = proj.name
              AND ex.folder_name = f.name
            ORDER BY ex.execution_id DESC
        ) e
        ORDER BY f.name, proj.name, pkg.name
    """
    cursor.execute(sql)
    headers = [col[0] for col in cursor.description]
    rows = [[str(v) if v is not None else "" for v in row]
            for row in cursor.fetchall()]

    return {
        "panel": "ssis",
        "headers": headers,
        "rows": rows,
        "row_id": 2,  # Package name column
        "actions": [
            {"key": "r", "label": "Run package", "command": "run-package"},
            {"key": "d", "label": "Execution history", "command": "executions"},
        ],
        "info": None,
    }


def _execution_history(cursor, package_name):
    """Get recent execution history for a package."""
    sql = """
        SELECT TOP 30
            e.execution_id                           AS [ID],
            CASE e.status
                WHEN 1 THEN 'Created'
                WHEN 2 THEN 'Running'
                WHEN 3 THEN 'Canceled'
                WHEN 4 THEN 'Failed'
                WHEN 5 THEN 'Pending'
                WHEN 6 THEN 'Ended unexpectedly'
                WHEN 7 THEN 'Succeeded'
                WHEN 8 THEN 'Stopping'
                WHEN 9 THEN 'Completed'
                ELSE 'Unknown'
            END                                      AS [Status],
            CONVERT(VARCHAR(19), e.start_time, 120)  AS [Start],
            CONVERT(VARCHAR(19), e.end_time, 120)    AS [End],
            e.caller_name                            AS [Caller],
            e.server_name                            AS [Server]
        FROM SSISDB.catalog.executions e
        WHERE e.package_name = ?
        ORDER BY e.execution_id DESC
    """
    cursor.execute(sql, [package_name])
    headers = [col[0] for col in cursor.description]
    rows = [[str(v) if v is not None else "" for v in row]
            for row in cursor.fetchall()]

    return {
        "panel": "ssis",
        "sub_panel": "executions",
        "title": f"Execution History: {package_name}",
        "headers": headers,
        "rows": rows,
        "row_id": None,
        "actions": [],
        "info": f"Recent executions for: {package_name}",
        "parent_panel": "ssis",
        "context": {"package_name": package_name},
    }


def _run_package(cursor, folder, project, package):
    """Run an SSIS package."""
    from .. import envelope

    try:
        # Create execution
        cursor.execute("""
            DECLARE @execution_id BIGINT;
            EXEC SSISDB.catalog.create_execution
                @package_name = ?,
                @project_name = ?,
                @folder_name = ?,
                @use32bitruntime = 0,
                @execution_id = @execution_id OUTPUT;
            SELECT @execution_id;
        """, [package, project, folder])
        row = cursor.fetchone()
        if not row:
            envelope.error("Failed to create SSIS execution")
            return
        execution_id = row[0]

        # Start execution
        cursor.execute(
            "EXEC SSISDB.catalog.start_execution @execution_id = ?",
            [execution_id])
        cursor.connection.commit()
        envelope.info(
            f"Started SSIS package: {folder}/{project}/{package} "
            f"(execution_id={execution_id})")
    except Exception as err:
        envelope.error(f"Failed to run SSIS package: {err}")
