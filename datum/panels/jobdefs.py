"""The job itself: creating, editing and deleting SQL Agent jobs.

Steps and schedules already had this — `steps' and `schedules' — but the
job they belong to could only be started, stopped and toggled.  There
was no way to make one, rename one, or change its owner, category or
notifications.

Everything here goes through the msdb procedures, which is what SSMS
does too.
"""

# How a job reports, and when it removes itself.  The same four values
# serve every notify_level_* column.
WHEN = [["0", "Never"], ["1", "When it succeeds"],
        ["2", "When it fails"], ["3", "Whenever it finishes"]]


def _lookup(cursor, sql, params=()):
    try:
        cursor.execute(sql, params) if params else cursor.execute(sql)
        return [row[0] for row in cursor.fetchall() if row[0]]
    except Exception:
        return []


def categories(cursor):
    """Return the job categories, as SSMS offers them."""
    return _lookup(cursor,
                   "SELECT CAST(name AS NVARCHAR(128)) FROM msdb.dbo.syscategories "
                   "WHERE category_class = 1 ORDER BY name")


def operators(cursor):
    """Return the operators a job can notify, which may be none."""
    return _lookup(cursor,
                   "SELECT CAST(name AS NVARCHAR(128)) FROM msdb.dbo.sysoperators "
                   "ORDER BY name")


def owners(cursor):
    """Return the logins a job can be owned by."""
    return _lookup(cursor, """
        SELECT CAST(name AS NVARCHAR(128)) FROM sys.server_principals
        WHERE type IN ('S', 'U', 'G') AND name NOT LIKE '##%'
          AND name NOT LIKE 'NT SERVICE\\%'
        ORDER BY name
    """)


def get_job(cursor, job_name):
    """Return a job's own properties, for editing."""
    cursor.execute("""
        SELECT CAST(j.name AS NVARCHAR(128)),
               j.enabled,
               CAST(ISNULL(j.description, '') AS NVARCHAR(MAX)),
               CAST(ISNULL(SUSER_SNAME(j.owner_sid), '') AS NVARCHAR(128)),
               CAST(ISNULL(c.name, '') AS NVARCHAR(128)),
               j.notify_level_eventlog,
               j.notify_level_email,
               CAST(ISNULL(o.name, '') AS NVARCHAR(128)),
               j.delete_level
        FROM msdb.dbo.sysjobs j
        LEFT JOIN msdb.dbo.syscategories c ON c.category_id = j.category_id
        LEFT JOIN msdb.dbo.sysoperators o
               ON o.id = j.notify_email_operator_id
        WHERE j.name = ?
    """, [job_name])
    row = cursor.fetchone()
    if not row:
        return None
    return {"name": row[0],
            "enabled": bool(row[1]),
            "description": row[2],
            "owner": row[3],
            "category": row[4],
            "notify_eventlog": str(row[5] or 0),
            "notify_email": str(row[6] or 0),
            "operator": row[7],
            "delete_level": str(row[8] or 0)}


def job_options(cursor, current=None):
    """Field descriptors for a job's own properties.

    Mirrors the General and Notifications pages of SSMS's job dialog.
    Steps and schedules are not here: they are lists rather than
    properties, and the detail view already edits them.
    """
    current = current or {}
    available = categories(cursor)
    who = operators(cursor)
    logins = owners(cursor)
    default_category = (current.get("category")
                        or ("[Uncategorized (Local)]"
                            if "[Uncategorized (Local)]" in available
                            else (available[0] if available else "")))
    fields = [
        {"key": "name", "label": "Name", "type": "string", "size": 40,
         "default": current.get("name", ""), "required": True},
        {"key": "enabled", "label": "Enabled", "type": "bool",
         "default": current.get("enabled", True)},
        {"key": "description", "label": "Description", "type": "text",
         "default": current.get("description", "")},
        {"key": "owner", "label": "Owner",
         "type": "completing" if logins else "string",
         "default": current.get("owner", ""),
         "completions": logins or None,
         "help": "defaults to whoever is connected"},
        {"key": "category", "label": "Category",
         "type": "choice" if available else "string",
         "default": default_category,
         "choices": [[c, c] for c in available] if available else None},
        {"key": "notify_eventlog", "label": "Write to the event log",
         "type": "choice", "default": current.get("notify_eventlog", "0"),
         "choices": WHEN},
        {"key": "notify_email", "label": "Email an operator",
         "type": "choice", "default": current.get("notify_email", "0"),
         "choices": WHEN},
    ]
    if who:
        fields.append(
            {"key": "operator", "label": "Operator to email",
             "type": "choice",
             "default": current.get("operator", ""),
             "choices": [["", "(none)"]] + [[o, o] for o in who]})
    fields.append(
        {"key": "delete_level", "label": "Delete the job",
         "type": "choice", "default": current.get("delete_level", "0"),
         "choices": WHEN,
         "help": "a job that removes itself once it has run"})
    return fields


def _level(opts, key):
    """Return a notify/delete level as an int, whatever the form sent."""
    try:
        return int(str(opts.get(key) or 0))
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be one of 0, 1, 2 or 3")


def create_job(cursor, opts):
    """Create a job, and give it somewhere to run.

    A job with no target server is accepted by sp_add_job and then
    never runs, which is a quiet way to lose an afternoon.  SSMS adds
    the local server for you; so does this.
    """
    name = (opts.get("name") or "").strip()
    if not name:
        raise ValueError("a job needs a name")

    cursor.execute(
        "EXEC msdb.dbo.sp_add_job @job_name = ?, @enabled = ?, "
        "@description = ?, @owner_login_name = ?, @category_name = ?, "
        "@notify_level_eventlog = ?, @notify_level_email = ?, "
        "@notify_email_operator_name = ?, @delete_level = ?",
        [name,
         1 if opts.get("enabled", True) else 0,
         (opts.get("description") or "") or None,
         (opts.get("owner") or "").strip() or None,
         (opts.get("category") or "").strip() or None,
         _level(opts, "notify_eventlog"),
         _level(opts, "notify_email"),
         (opts.get("operator") or "").strip() or None,
         _level(opts, "delete_level")])

    cursor.execute(
        "EXEC msdb.dbo.sp_add_jobserver @job_name = ?, @server_name = ?",
        [name, "(local)"])
    return name


def update_job(cursor, opts):
    """Apply a job's edited properties, renaming it where the name changed."""
    original = (opts.get("name_original") or "").strip()
    name = (opts.get("name") or "").strip()
    if not original:
        raise ValueError("the form did not say which job to change")
    if not name:
        raise ValueError("a job needs a name")

    cursor.execute(
        "EXEC msdb.dbo.sp_update_job @job_name = ?, @new_name = ?, "
        "@enabled = ?, @description = ?, @owner_login_name = ?, "
        "@category_name = ?, @notify_level_eventlog = ?, "
        "@notify_level_email = ?, @notify_email_operator_name = ?, "
        "@delete_level = ?",
        [original,
         name if name != original else None,
         1 if opts.get("enabled", True) else 0,
         opts.get("description") or "",
         (opts.get("owner") or "").strip() or None,
         (opts.get("category") or "").strip() or None,
         _level(opts, "notify_eventlog"),
         _level(opts, "notify_email"),
         (opts.get("operator") or "").strip() or None,
         _level(opts, "delete_level")])
    return name


def delete_job(cursor, job_name):
    """Delete a job, with its steps, schedules and history."""
    name = (job_name or "").strip()
    if not name:
        raise ValueError("no job named")
    cursor.execute("EXEC msdb.dbo.sp_delete_job @job_name = ?", [name])
    return name


def job_summary(cursor, job_name):
    """Return what deleting JOB_NAME would take with it."""
    cursor.execute("""
        SELECT (SELECT COUNT(*) FROM msdb.dbo.sysjobsteps s
                 WHERE s.job_id = j.job_id),
               (SELECT COUNT(*) FROM msdb.dbo.sysjobschedules js
                 WHERE js.job_id = j.job_id),
               (SELECT COUNT(*) FROM msdb.dbo.sysjobhistory h
                 WHERE h.job_id = j.job_id)
        FROM msdb.dbo.sysjobs j WHERE j.name = ?
    """, [job_name])
    row = cursor.fetchone()
    if not row:
        return None
    return {"steps": int(row[0] or 0),
            "schedules": int(row[1] or 0),
            "history": int(row[2] or 0)}
