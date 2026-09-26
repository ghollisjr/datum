"""Creating, editing and deleting SQL Agent jobs against a real server.

Steps and schedules already had this; the job they belong to did not.
"""

import base64
import json
import os

import pytest

pyodbc = pytest.importorskip("pyodbc")
pyodbc.pooling = False

from datum.drivers.mssql import MSSQLDriver  # noqa: E402

STRICT = os.environ.get("DATUM_STRICT") == "1"
MS_HOST = os.environ.get("DATUM_MSSQL_SERVER", "127.0.0.1")
MS_PORT = os.environ.get("DATUM_MSSQL_PORT", "1434")
MS_DSN = (f"Driver={{ODBC Driver 18 for SQL Server}};Server={MS_HOST},{MS_PORT};"
          f"Database=master;Uid={os.environ.get('DATUM_MSSQL_USER','sa')};"
          f"Pwd={os.environ.get('DATUM_MSSQL_PASS','DatumTest1!')};"
          f"TrustServerCertificate=yes")

JOB = "datum_job_pytest"
RENAMED = "datum_job_pytest_renamed"
_UNREACHABLE = {}


def _payload(obj):
    return base64.b64encode(json.dumps(obj).encode()).decode()


@pytest.fixture
def captured(monkeypatch):
    from datum import envelope

    events = []
    for kind in ("info", "warn", "error", "admin_panel", "definition"):
        monkeypatch.setattr(
            envelope, kind,
            (lambda k: (lambda *a, **kw: events.append((k, a))))(kind))
    return events


@pytest.fixture
def agent():
    if "mssql" in _UNREACHABLE:
        pytest.skip(_UNREACHABLE["mssql"])
    try:
        conn = pyodbc.connect(MS_DSN, timeout=8, autocommit=True)
    except Exception as err:
        _UNREACHABLE["mssql"] = f"MSSQL not reachable: {err}"
        if STRICT:
            pytest.fail(_UNREACHABLE["mssql"])
        pytest.skip(_UNREACHABLE["mssql"])
    cursor, driver = conn.cursor(), MSSQLDriver()

    # Schedules live in msdb, not in the job, so deleting the job does
    # not always take them with it -- and two schedules of one name make
    # sp_add_schedule refuse to work by name at all.  So they go too.
    SCHEDULES = ("nightly", "renamed", "x")

    def cleanup():
        for name in (JOB, RENAMED, "datum job pytest spaced"):
            try:
                cursor.execute("EXEC msdb.dbo.sp_delete_job @job_name = ?",
                               [name])
            except Exception:
                pass
        try:
            cursor.execute(
                "SELECT schedule_id FROM msdb.dbo.sysschedules "
                "WHERE name IN (?, ?, ?)", list(SCHEDULES))
            for (schedule_id,) in cursor.fetchall():
                try:
                    cursor.execute(
                        "EXEC msdb.dbo.sp_delete_schedule "
                        "@schedule_id = ?, @force_delete = 1",
                        [schedule_id])
                except Exception:
                    pass
        except Exception:
            pass

    cleanup()
    yield cursor, driver
    cleanup()
    conn.close()


def _make(cursor, driver, **extra):
    from datum.panels import jobs
    opts = {"name": JOB, "enabled": True, "description": "made by a test",
            "owner": "sa", "category": "[Uncategorized (Local)]",
            "notify_eventlog": "0", "notify_email": "0",
            "delete_level": "0"}
    opts.update(extra)
    jobs.run_action(cursor, driver, "create-job", [_payload(opts)])
    return opts


class TestCreatingAJob:

    def test_the_form_offers_the_properties_ssms_does(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        jobs.run_action(cursor, driver, "new-job", [])
        form = [a[0] for k, a in captured if k == "admin_panel"][0]["form"]
        keys = [f["key"] for f in form["fields"]]
        for expected in ("name", "enabled", "description", "owner",
                         "category", "notify_eventlog", "notify_email",
                         "delete_level"):
            assert expected in keys, keys
        # Steps and schedules are lists, not properties; the detail view
        # already edits them, and the form says so.
        assert any("detail" in note for note in form["notes"])

    def test_a_job_is_created(self, agent, captured):
        cursor, driver = agent
        _make(cursor, driver)
        assert "error" not in [k for k, _ in captured], captured
        from datum.panels import jobdefs
        job = jobdefs.get_job(cursor, JOB)
        assert job["name"] == JOB
        assert job["enabled"] is True
        assert job["description"] == "made by a test"

    def test_a_new_job_is_given_somewhere_to_run(self, agent, captured):
        cursor, driver = agent
        _make(cursor, driver)
        # sp_add_job alone leaves a job with no target server: it exists
        # and never runs, which is a quiet way to lose an afternoon.
        cursor.execute("""
            SELECT COUNT(*) FROM msdb.dbo.sysjobservers s
            JOIN msdb.dbo.sysjobs j ON j.job_id = s.job_id
            WHERE j.name = ?""", [JOB])
        assert cursor.fetchone()[0] == 1

    def test_the_panel_is_refreshed_afterwards(self, agent, captured):
        cursor, driver = agent
        _make(cursor, driver)
        panels = [a[0] for k, a in captured if k == "admin_panel"]
        assert panels, captured
        assert panels[-1]["panel"] == "jobs"

    def test_a_job_with_no_name_is_refused(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        jobs.run_action(cursor, driver, "create-job",
                        [_payload({"name": "   "})])
        assert [k for k, _ in captured] == ["error"], captured

    def test_a_nonsense_notify_level_is_refused(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        jobs.run_action(cursor, driver, "create-job", [_payload(
            {"name": JOB, "notify_eventlog": "whenever I feel like it"})])
        assert [k for k, _ in captured] == ["error"], captured


class TestEditingAJob:

    def test_the_form_opens_with_what_the_job_is(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        _make(cursor, driver, notify_eventlog="2")
        captured.clear()
        jobs.run_action(cursor, driver, "edit-job", [JOB])
        form = [a[0] for k, a in captured if k == "admin_panel"][0]["form"]
        assert form["values"]["name"] == JOB
        assert form["values"]["description"] == "made by a test"
        assert form["values"]["notify_eventlog"] == "2"
        # What it was called when the form opened, so an edited name
        # reads as a rename rather than as a different job.
        assert form["values"]["name_original"] == JOB

    def test_properties_are_changed(self, agent, captured):
        from datum.panels import jobdefs, jobs

        cursor, driver = agent
        _make(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "update-job", [_payload(
            {"name_original": JOB, "name": JOB, "enabled": False,
             "description": "edited", "owner": "sa",
             "category": "[Uncategorized (Local)]",
             "notify_eventlog": "3", "notify_email": "0",
             "delete_level": "0"})])
        assert "error" not in [k for k, _ in captured], captured
        job = jobdefs.get_job(cursor, JOB)
        assert job["enabled"] is False
        assert job["description"] == "edited"
        assert job["notify_eventlog"] == "3"

    def test_an_edited_name_renames_rather_than_duplicating(self, agent,
                                                            captured):
        from datum.panels import jobdefs, jobs

        cursor, driver = agent
        _make(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "update-job", [_payload(
            {"name_original": JOB, "name": RENAMED, "enabled": True,
             "description": "x", "owner": "sa",
             "category": "[Uncategorized (Local)]",
             "notify_eventlog": "0", "notify_email": "0",
             "delete_level": "0"})])
        assert "error" not in [k for k, _ in captured], captured
        assert jobdefs.get_job(cursor, RENAMED)
        assert jobdefs.get_job(cursor, JOB) is None

    def test_editing_a_job_that_is_not_there(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        jobs.run_action(cursor, driver, "edit-job", ["no_such_job_at_all"])
        assert [k for k, _ in captured] == ["error"], captured

    def test_the_steps_survive_an_edit(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        _make(cursor, driver)
        cursor.execute(
            "EXEC msdb.dbo.sp_add_jobstep @job_name=?, @step_name=?, "
            "@subsystem=N'TSQL', @command=N'SELECT 1', @database_name=N'master'",
            [JOB, "the only step"])
        captured.clear()
        jobs.run_action(cursor, driver, "update-job", [_payload(
            {"name_original": JOB, "name": RENAMED, "enabled": True,
             "description": "x", "owner": "sa",
             "category": "[Uncategorized (Local)]",
             "notify_eventlog": "0", "notify_email": "0",
             "delete_level": "0"})])
        cursor.execute("""
            SELECT COUNT(*) FROM msdb.dbo.sysjobsteps s
            JOIN msdb.dbo.sysjobs j ON j.job_id = s.job_id
            WHERE j.name = ?""", [RENAMED])
        assert cursor.fetchone()[0] == 1


class TestDeletingAJob:

    def test_the_confirmation_says_what_goes_with_it(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        _make(cursor, driver)
        cursor.execute(
            "EXEC msdb.dbo.sp_add_jobstep @job_name=?, @step_name=?, "
            "@subsystem=N'TSQL', @command=N'SELECT 1', @database_name=N'master'",
            [JOB, "s1"])
        captured.clear()
        jobs.run_action(cursor, driver, "drop-job-check", [JOB])
        form = [a[0] for k, a in captured if k == "admin_panel"][0]["form"]
        # The same shape the other dangerous actions use: the name has
        # to be typed back.
        assert form["confirm_text"] == JOB
        assert form["danger"] is True
        assert form["submit_action"] == "delete-job"
        assert any("Steps: 1" in note for note in form["notes"]), form["notes"]
        # And nothing has been deleted by asking.
        from datum.panels import jobdefs
        assert jobdefs.get_job(cursor, JOB)

    def test_a_job_is_deleted(self, agent, captured):
        from datum.panels import jobdefs, jobs

        cursor, driver = agent
        _make(cursor, driver)
        captured.clear()
        # As the confirmation form submits it.
        jobs.run_action(cursor, driver, "delete-job",
                        [_payload({"name": JOB})])
        assert "error" not in [k for k, _ in captured], captured
        assert jobdefs.get_job(cursor, JOB) is None

    def test_a_bare_name_still_works(self, agent, captured):
        from datum.panels import jobdefs, jobs

        cursor, driver = agent
        _make(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "delete-job", [JOB])
        assert "error" not in [k for k, _ in captured], captured
        assert jobdefs.get_job(cursor, JOB) is None

    def test_deleting_one_that_is_not_there(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        jobs.run_action(cursor, driver, "drop-job-check", ["no_such_job"])
        assert [k for k, _ in captured] == ["error"], captured


class TestOtherDialects:

    def test_jobs_are_mssql_only(self, captured):
        from datum.drivers.postgres import PostgreSQLDriver
        from datum.panels import jobs

        driver = PostgreSQLDriver.__new__(PostgreSQLDriver)
        jobs.run_action(None, driver, "new-job", [])
        assert [k for k, _ in captured] == ["error"], captured
        assert "MSSQL" in captured[0][1][0]


class TestStepsUseTheGenericForm:
    """The step editor had a renderer of its own, which meant it had
    none of the navigation the other wizards had grown, sent the step
    on the command line where a long command was truncated, and built
    its blank form in the client where the databases were not known."""

    def _job(self, cursor, driver):
        from datum.panels import jobs
        jobs.run_action(cursor, driver, "create-job", [_payload(
            {"name": JOB, "enabled": True, "description": "",
             "owner": "sa", "category": "[Uncategorized (Local)]",
             "notify_eventlog": "0", "notify_email": "0",
             "delete_level": "0"})])

    def test_the_new_step_form_comes_from_the_server(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "new-step", [JOB])
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        # The generic form, so it inherits the navigation rather than
        # needing a renderer of its own.
        assert panel["sub_panel"] == "form"
        assert panel["form"]["submit_action"] == "create-step"
        keys = [f["key"] for f in panel["form"]["fields"]]
        for expected in ("step_name", "subsystem", "database_name",
                         "command", "retry_attempts", "on_success_action"):
            assert expected in keys, keys
        # And the databases come from the server, which the client had
        # no way to know when it built this form itself.
        database = next(f for f in panel["form"]["fields"]
                        if f["key"] == "database_name")
        assert database["type"] == "completing"
        assert "master" in database["completions"]

    def test_a_long_command_survives(self, agent, captured):
        from datum.panels import jobs, steps

        cursor, driver = agent
        self._job(cursor, driver)
        # Sent on the command line, this was cut off by the pty at 4095
        # bytes -- and a step's command is the one field that is long.
        command = "-- a realistic body\n" + ("SELECT 1;\n" * 900)
        assert len(command) > 8000
        captured.clear()
        jobs.run_action(cursor, driver, "create-step", [_payload(
            {"job_name": JOB, "step_name": "big", "subsystem": "TSQL",
             "database_name": "master", "command": command,
             "retry_attempts": "0", "retry_interval": "0",
             "on_success_action": "1", "on_success_step_id": "0",
             "on_fail_action": "2", "on_fail_step_id": "0"})])
        assert "error" not in [k for k, _ in captured], captured
        stored = steps.get_step(cursor, JOB, 1)
        assert len(stored["command"]) == len(command)

    def test_the_edit_form_opens_with_the_step(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job(cursor, driver)
        jobs.run_action(cursor, driver, "create-step", [_payload(
            {"job_name": JOB, "step_name": "first", "subsystem": "TSQL",
             "database_name": "master", "command": "SELECT 1",
             "retry_attempts": "3", "retry_interval": "0",
             "on_success_action": "1", "on_success_step_id": "0",
             "on_fail_action": "2", "on_fail_step_id": "0"})])
        captured.clear()
        jobs.run_action(cursor, driver, "edit-step", ["1", JOB])
        form = [a[0] for k, a in captured if k == "admin_panel"][0]["form"]
        assert form["submit_action"] == "update-step"
        assert form["values"]["step_name"] == "first"
        assert form["values"]["retry_attempts"] == 3
        # Which job and which step, so the submit knows what to change.
        assert form["values"]["job_name"] == JOB
        assert form["values"]["step_id"] == 1

    def test_a_step_is_changed(self, agent, captured):
        from datum.panels import jobs, steps

        cursor, driver = agent
        self._job(cursor, driver)
        jobs.run_action(cursor, driver, "create-step", [_payload(
            {"job_name": JOB, "step_name": "first", "subsystem": "TSQL",
             "database_name": "master", "command": "SELECT 1",
             "retry_attempts": "0", "retry_interval": "0",
             "on_success_action": "1", "on_success_step_id": "0",
             "on_fail_action": "2", "on_fail_step_id": "0"})])
        captured.clear()
        jobs.run_action(cursor, driver, "update-step", [_payload(
            {"job_name": JOB, "step_id": "1", "step_name": "renamed",
             "subsystem": "TSQL", "database_name": "master",
             "command": "SELECT 2", "retry_attempts": "7",
             "retry_interval": "1", "on_success_action": "3",
             "on_success_step_id": "0", "on_fail_action": "2",
             "on_fail_step_id": "0"})])
        assert "error" not in [k for k, _ in captured], captured
        step = steps.get_step(cursor, JOB, 1)
        assert step["step_name"] == "renamed"
        assert step["command"] == "SELECT 2"
        assert step["retry_attempts"] == 7

    def test_the_detail_view_is_refreshed(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "create-step", [_payload(
            {"job_name": JOB, "step_name": "s", "subsystem": "TSQL",
             "database_name": "master", "command": "SELECT 1",
             "retry_attempts": "0", "retry_interval": "0",
             "on_success_action": "1", "on_success_step_id": "0",
             "on_fail_action": "2", "on_fail_step_id": "0"})])
        panels = [a[0] for k, a in captured if k == "admin_panel"]
        assert panels and panels[-1].get("sub_panel") == "detail"

    def test_a_number_that_is_not_one_is_refused(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "create-step", [_payload(
            {"job_name": JOB, "step_name": "x", "retry_attempts": "soon"})])
        assert [k for k, _ in captured] == ["error"], captured
        assert "must be a number" in captured[0][1][0]

    def test_a_step_needs_a_name(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "create-step",
                        [_payload({"job_name": JOB, "step_name": "   "})])
        assert [k for k, _ in captured] == ["error"], captured

    def test_a_job_name_with_a_space_still_works(self, agent, captured):
        from datum.panels import jobs, steps

        cursor, driver = agent
        spaced = "datum job pytest spaced"
        try:
            cursor.execute("EXEC msdb.dbo.sp_delete_job @job_name = ?",
                           [spaced])
        except Exception:
            pass
        jobs.run_action(cursor, driver, "create-job", [_payload(
            {"name": spaced, "enabled": True, "description": "",
             "owner": "sa", "category": "[Uncategorized (Local)]",
             "notify_eventlog": "0", "notify_email": "0",
             "delete_level": "0"})])
        captured.clear()
        try:
            # The job name used to be interpolated into the command line,
            # where the first word was taken as the whole name.
            jobs.run_action(cursor, driver, "create-step", [_payload(
                {"job_name": spaced, "step_name": "s", "subsystem": "TSQL",
                 "database_name": "master", "command": "SELECT 1",
                 "retry_attempts": "0", "retry_interval": "0",
                 "on_success_action": "1", "on_success_step_id": "0",
                 "on_fail_action": "2", "on_fail_step_id": "0"})])
            assert "error" not in [k for k, _ in captured], captured
            assert steps.get_step(cursor, spaced, 1)["step_name"] == "s"
        finally:
            try:
                cursor.execute("EXEC msdb.dbo.sp_delete_job @job_name = ?",
                               [spaced])
            except Exception:
                pass


class TestSchedulesUseTheGenericForm:
    """The schedule editor was the other bespoke renderer."""

    def _job(self, cursor, driver):
        from datum.panels import jobs
        jobs.run_action(cursor, driver, "create-job", [_payload(
            {"name": JOB, "enabled": True, "description": "",
             "owner": "sa", "category": "[Uncategorized (Local)]",
             "notify_eventlog": "0", "notify_email": "0",
             "delete_level": "0"})])

    def _schedule(self, cursor, driver, name="nightly", **extra):
        from datum.panels import jobs
        opts = {"job_name": JOB, "name": name, "enabled": True,
                "freq_type": "4", "freq_interval": "1",
                "freq_subday_type": "1", "freq_subday_interval": "0",
                "freq_relative_interval": "0", "freq_recurrence_factor": "0",
                "active_start_time": "23000", "active_end_time": "235959",
                "active_start_date": "20260101",
                "active_end_date": "99991231"}
        opts.update(extra)
        jobs.run_action(cursor, driver, "create-schedule", [_payload(opts)])

    def test_the_new_schedule_form_comes_from_the_server(self, agent,
                                                        captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "new-schedule", [JOB])
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        assert panel["sub_panel"] == "form"
        assert panel["form"]["submit_action"] == "create-schedule"
        keys = [f["key"] for f in panel["form"]["fields"]]
        assert "freq_type" in keys and "active_start_time" in keys

    def test_a_schedule_is_created(self, agent, captured):
        from datum.panels import schedules

        cursor, driver = agent
        self._job(cursor, driver)
        captured.clear()
        self._schedule(cursor, driver)
        assert "error" not in [k for k, _ in captured], captured
        got = schedules.get_schedule(cursor, "nightly", JOB)
        assert got and got["freq_type"] == 4

    def test_the_edit_form_opens_with_the_schedule(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job(cursor, driver)
        self._schedule(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "edit-schedule", ["nightly", JOB])
        form = [a[0] for k, a in captured if k == "admin_panel"][0]["form"]
        # get_schedule reports the name under "name"; reading it from
        # the wrong key left the field blank.
        assert form["values"]["name"] == "nightly"
        assert form["values"]["schedule_id"]
        assert form["values"]["job_name"] == JOB

    def test_a_schedule_can_be_renamed(self, agent, captured):
        from datum.panels import jobs, schedules

        cursor, driver = agent
        self._job(cursor, driver)
        self._schedule(cursor, driver)
        current = schedules.get_schedule(cursor, "nightly", JOB)
        captured.clear()
        # sp_update_schedule's @name identifies the schedule and
        # @new_name renames it; passing the new name as @name looked for
        # one that did not exist, so renaming never worked.
        jobs.run_action(cursor, driver, "update-schedule", [_payload(
            {"job_name": JOB, "schedule_id": current["schedule_id"],
             "name": "renamed", "enabled": False, "freq_type": "8",
             "freq_interval": "2", "freq_subday_type": "1",
             "freq_subday_interval": "0", "freq_relative_interval": "0",
             "freq_recurrence_factor": "1", "active_start_time": "10000",
             "active_end_time": "235959", "active_start_date": "20260101",
             "active_end_date": "99991231"})])
        assert "error" not in [k for k, _ in captured], captured
        assert schedules.get_schedule(cursor, "renamed", JOB)
        assert schedules.get_schedule(cursor, "nightly", JOB) is None

    def test_other_properties_change_too(self, agent, captured):
        from datum.panels import jobs, schedules

        cursor, driver = agent
        self._job(cursor, driver)
        self._schedule(cursor, driver)
        current = schedules.get_schedule(cursor, "nightly", JOB)
        captured.clear()
        jobs.run_action(cursor, driver, "update-schedule", [_payload(
            {"job_name": JOB, "schedule_id": current["schedule_id"],
             "name": "nightly", "enabled": False, "freq_type": "8",
             "freq_interval": "2", "freq_subday_type": "1",
             "freq_subday_interval": "0", "freq_relative_interval": "0",
             "freq_recurrence_factor": "1", "active_start_time": "10000",
             "active_end_time": "235959", "active_start_date": "20260101",
             "active_end_date": "99991231"})])
        got = schedules.get_schedule(cursor, "nightly", JOB)
        assert got["enabled"] == 0
        assert got["freq_type"] == 8
        assert got["active_start_time"] == 10000

    def test_a_bad_number_is_refused(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "create-schedule", [_payload(
            {"job_name": JOB, "name": "x", "freq_type": "weekly-ish"})])
        assert [k for k, _ in captured] == ["error"], captured
        assert "must be a number" in captured[0][1][0]

    def test_a_schedule_needs_a_name(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "create-schedule",
                        [_payload({"job_name": JOB, "name": "  "})])
        assert [k for k, _ in captured] == ["error"], captured

    def test_updating_without_an_id_says_so(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "update-schedule",
                        [_payload({"job_name": JOB, "name": "x"})])
        assert [k for k, _ in captured] == ["error"], captured
        assert "which schedule" in captured[0][1][0]


class TestAJobIsOneTree:
    """Editing a job is where its steps are.

    The job's own properties, its steps and its schedules were down
    separate paths -- E gave a properties form with no sign of the
    steps, which were only reachable through d.
    """

    def _job_with_parts(self, cursor, driver):
        from datum.panels import jobs
        jobs.run_action(cursor, driver, "create-job", [_payload(
            {"name": JOB, "enabled": True, "description": "nightly extract",
             "owner": "sa", "category": "[Uncategorized (Local)]",
             "notify_eventlog": "2", "notify_email": "0",
             "delete_level": "0"})])
        jobs.run_action(cursor, driver, "create-step", [_payload(
            {"job_name": JOB, "step_name": "extract", "subsystem": "TSQL",
             "database_name": "master", "command": "SELECT 1",
             "retry_attempts": "0", "retry_interval": "0",
             "on_success_action": "3", "on_success_step_id": "0",
             "on_fail_action": "2", "on_fail_step_id": "0"})])
        jobs.run_action(cursor, driver, "create-schedule", [_payload(
            {"job_name": JOB, "name": "nightly", "enabled": True,
             "freq_type": "4", "freq_interval": "1",
             "freq_subday_type": "1", "freq_subday_interval": "0",
             "freq_relative_interval": "0", "freq_recurrence_factor": "0",
             "active_start_time": "23000", "active_end_time": "235959",
             "active_start_date": "20260101",
             "active_end_date": "99991231"})])

    def test_the_job_is_the_first_section_of_its_own_tree(self, agent,
                                                          captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job_with_parts(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "detail", [JOB])
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        titles = [s["title"] for s in panel["sections"]]
        # The job first, then what belongs to it.
        assert titles == ["Job", "Steps", "Schedules"], titles

    def test_the_job_section_shows_its_properties(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job_with_parts(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "detail", [JOB])
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        job = next(s for s in panel["sections"] if s["title"] == "Job")
        shown = dict(job["rows"])
        assert shown["Name"] == JOB
        assert shown["Enabled"] == "Yes"
        assert shown["Owner"] == "sa"
        assert shown["Description"] == "nightly extract"
        # The notify levels read as words rather than as numbers.
        assert shown["Write to the event log"] == "When it fails"

    def test_the_job_section_offers_editing_and_deleting(self, agent,
                                                         captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job_with_parts(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "detail", [JOB])
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        job = next(s for s in panel["sections"] if s["title"] == "Job")
        keys = {a["key"]: a["command"] for a in job["actions"]}
        assert keys["E"] == "edit-job"
        assert keys["D"] == "drop-job-check"
        # Not N: a job's section has nothing to add, and its steps and
        # schedules have their own.
        assert "N" not in keys

    def test_the_steps_are_there_in_the_same_view(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job_with_parts(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "detail", [JOB])
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        steps = next(s for s in panel["sections"] if s["title"] == "Steps")
        assert any("extract" in row for row in steps["rows"])
        schedules = next(s for s in panel["sections"]
                         if s["title"] == "Schedules")
        assert any("nightly" in row for row in schedules["rows"])

    def test_the_title_reads_as_the_job_rather_than_a_report(self, agent,
                                                             captured):
        from datum.panels import jobs

        cursor, driver = agent
        self._job_with_parts(cursor, driver)
        captured.clear()
        jobs.run_action(cursor, driver, "detail", [JOB])
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        assert panel["title"] == f"Job: {JOB}"

    def test_a_job_with_nothing_in_it_still_opens(self, agent, captured):
        from datum.panels import jobs

        cursor, driver = agent
        jobs.run_action(cursor, driver, "create-job", [_payload(
            {"name": JOB, "enabled": True, "description": "",
             "owner": "sa", "category": "[Uncategorized (Local)]",
             "notify_eventlog": "0", "notify_email": "0",
             "delete_level": "0"})])
        captured.clear()
        jobs.run_action(cursor, driver, "detail", [JOB])
        assert "error" not in [k for k, _ in captured], captured
        panel = [a[0] for k, a in captured if k == "admin_panel"][0]
        job = next(s for s in panel["sections"] if s["title"] == "Job")
        assert dict(job["rows"])["Name"] == JOB
