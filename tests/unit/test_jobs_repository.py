from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from brickhouse.jobs import (
    InvalidJobDefinitionError,
    JobAttemptState,
    JobDependencyNotReadyError,
    JobRepositoryError,
    JobRunState,
    JobStateTransitionError,
    JobTaskAttemptOutput,
    JobTaskDefinition,
    JobTaskState,
    SubmissionTokenConflictError,
)
from brickhouse.jobs.repository import MAX_TASK_VALUE_JSON_BYTES, MAX_TASK_VALUES_PER_RUN
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


def test_job_definition_and_submission_snapshot_survive_reopen(tmp_path: Path) -> None:
    initialized = initialize_workspace(tmp_path / "workspace")
    task_settings = {"script_path": "files/transform.py", "parameters": {"count": 2}}
    job_settings = {"max_concurrent_runs": 1}
    submitted_settings = {"parameters": {"order_id": 7}}

    with WorkspaceContext.local(initialized.path) as context:
        repo = context.jobs
        first_task = JobTaskDefinition("transform", settings=task_settings)
        task_settings["parameters"]["count"] = 99
        job = repo.create_job(
            "transform-orders",
            [first_task, JobTaskDefinition("readback", depends_on=("transform",))],
            settings=job_settings,
        )
        job_settings["max_concurrent_runs"] = 50
        task_settings["script_path"] = "files/changed.py"
        assert job.tasks[0].settings == {
            "script_path": "files/transform.py",
            "parameters": {"count": 2},
        }
        assert job.settings == {"max_concurrent_runs": 1}

        submission = repo.submit_run(
            job.job_id,
            submitted_settings=submitted_settings,
            submission_token="token-001",
        )
        submitted_settings["parameters"]["order_id"] = 999
        replay = repo.submit_run(
            job.job_id,
            submitted_settings={"parameters": {"order_id": 7}},
            submission_token="token-001",
        )
        assert replay.replayed is True
        assert replay.run.run_id == submission.run.run_id
        assert submission.run.submitted_settings == {"parameters": {"order_id": 7}}
        assert len(repo.list_runs(job.job_id)) == 1
        with pytest.raises(SubmissionTokenConflictError, match="different job settings"):
            repo.submit_run(
                job.job_id,
                submitted_settings={"parameters": {"order_id": 8}},
                submission_token="token-001",
            )

        with sqlite3.connect(context.workspace.metadata_path) as connection:
            with pytest.raises(
                sqlite3.IntegrityError, match="submitted job settings are immutable"
            ):
                connection.execute(
                    "UPDATE job_runs SET submitted_settings_json = '{}' WHERE run_id = ?",
                    (submission.run.run_id,),
                )
            with pytest.raises(
                sqlite3.IntegrityError,
                match="job run (terminal outcome|state transition)",
            ):
                connection.execute(
                    "UPDATE job_runs SET state = 'succeeded', finished_at = 'now' WHERE run_id = ?",
                    (submission.run.run_id,),
                )
            with pytest.raises(sqlite3.IntegrityError, match="invalid job task state transition"):
                connection.execute(
                    "UPDATE job_run_tasks SET state = 'running', started_at = 'now' "
                    "WHERE run_id = ? AND task_key = 'transform'",
                    (submission.run.run_id,),
                )

        empty_job = repo.create_job("undeletable", [JobTaskDefinition("task")])
        with sqlite3.connect(context.workspace.metadata_path) as connection:
            with pytest.raises(sqlite3.IntegrityError, match="job definitions are immutable"):
                connection.execute("DELETE FROM jobs WHERE job_id = ?", (empty_job.job_id,))
            with pytest.raises(
                sqlite3.IntegrityError, match="job definition task count is immutable"
            ):
                connection.execute(
                    "INSERT INTO job_tasks "
                    "(job_id, task_key, task_type, task_order, depends_on_json, settings_json) "
                    "VALUES (?, 'late-task', 'python_script', 1, '[]', '{}')",
                    (empty_job.job_id,),
                )

    with WorkspaceContext.local(initialized.path) as reopened:
        restored_job = reopened.jobs.get_job(job.job_id)
        restored_run = reopened.jobs.get_run(submission.run.run_id)
        assert restored_job.definition_sha256 == job.definition_sha256
        assert restored_job.tasks[1].depends_on == ("transform",)
        assert restored_run.state is JobRunState.QUEUED
        assert restored_run.submitted_settings == {"parameters": {"order_id": 7}}
        assert [task.state for task in reopened.jobs.list_task_runs(restored_run.run_id)] == [
            JobTaskState.PENDING,
            JobTaskState.PENDING,
        ]


def test_invalid_task_graphs_and_secret_settings_are_rejected(tmp_path: Path) -> None:
    initialized = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(initialized.path) as context:
        repo = context.jobs
        with pytest.raises(InvalidJobDefinitionError, match="unique"):
            repo.create_job("duplicate", [JobTaskDefinition("same"), JobTaskDefinition("same")])
        with pytest.raises(InvalidJobDefinitionError, match="missing task"):
            repo.create_job("missing", [JobTaskDefinition("a", depends_on=("unknown",))])
        with pytest.raises(InvalidJobDefinitionError, match="cannot depend on itself"):
            repo.create_job("self", [JobTaskDefinition("a", depends_on=("a",))])
        with pytest.raises(InvalidJobDefinitionError, match="cycle"):
            repo.create_job(
                "cycle",
                [
                    JobTaskDefinition("a", depends_on=("b",)),
                    JobTaskDefinition("b", depends_on=("a",)),
                ],
            )
        too_many = [JobTaskDefinition(f"task-{index}") for index in range(11)]
        with pytest.raises(InvalidJobDefinitionError, match="more than 10"):
            repo.create_job("too-many", too_many)
        with pytest.raises(InvalidJobDefinitionError, match="unsupported job task type"):
            JobTaskDefinition("notebook", task_type="notebook")
        with pytest.raises(InvalidJobDefinitionError, match="credential field"):
            JobTaskDefinition("credential", settings={"nested": {"clientSecret": "never-store"}})
        with pytest.raises(InvalidJobDefinitionError, match="credential field"):
            repo.create_job("credential", [JobTaskDefinition("safe")], settings={"api_key": "x"})
        with pytest.raises(InvalidJobDefinitionError, match="finite JSON"):
            JobTaskDefinition("nan", settings={"value": float("nan")})
        with pytest.raises(InvalidJobDefinitionError, match="JSON object"):
            JobTaskDefinition("wrong-settings", settings=[])  # type: ignore[arg-type]
        with pytest.raises(InvalidJobDefinitionError, match="JSON object"):
            repo.submit_run(1, submitted_settings=[])  # type: ignore[arg-type]


def test_attempt_order_dependencies_and_terminal_state_validation(tmp_path: Path) -> None:
    initialized = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(initialized.path) as context:
        repo = context.jobs
        job = repo.create_job(
            "two-step",
            [
                JobTaskDefinition("write"),
                JobTaskDefinition("read", depends_on=("write",)),
            ],
        )
        run = repo.submit_run(job.job_id).run
        with pytest.raises(JobStateTransitionError, match="every task succeeds"):
            repo.finish_run(run.run_id, JobRunState.SUCCEEDED)
        with pytest.raises(JobDependencyNotReadyError, match="waiting"):
            repo.queue_attempt(run.run_id, "read")

        first = repo.queue_attempt(run.run_id, "write")
        assert first.attempt_number == 1
        started = repo.start_attempt(run.run_id, "write", first.attempt_number)
        assert started.state is JobAttemptState.RUNNING
        with pytest.raises(JobStateTransitionError, match="not queued"):
            repo.start_attempt(run.run_id, "write", first.attempt_number)
        completed = repo.finish_attempt(
            run.run_id,
            "write",
            first.attempt_number,
            exit_code=0,
            termination_reason="completed",
        )
        assert completed.state is JobAttemptState.SUCCEEDED
        assert repo.get_run(run.run_id).state is JobRunState.RUNNING

        failed = repo.queue_attempt(run.run_id, "read")
        repo.start_attempt(run.run_id, "read", failed.attempt_number)
        failed = repo.finish_attempt(
            run.run_id,
            "read",
            failed.attempt_number,
            exit_code=17,
            termination_reason="completed",
        )
        assert failed.state is JobAttemptState.FAILED
        assert failed.exit_code == 17
        assert failed.error_code == "WORKER_EXIT_NONZERO"
        with pytest.raises(JobStateTransitionError, match="every task succeeds"):
            repo.finish_run(run.run_id, JobRunState.SUCCEEDED)

        retry = repo.queue_attempt(run.run_id, "read")
        assert retry.attempt_number == 2
        assert retry.attempt_id != failed.attempt_id
        repo.start_attempt(run.run_id, "read", retry.attempt_number)
        succeeded = repo.finish_attempt(
            run.run_id,
            "read",
            retry.attempt_number,
            exit_code=0,
            termination_reason="completed",
        )
        assert succeeded.state is JobAttemptState.SUCCEEDED
        terminal = repo.finish_run(run.run_id, JobRunState.SUCCEEDED)
        assert terminal.state is JobRunState.SUCCEEDED
        assert terminal.finished_at is not None
        with sqlite3.connect(context.workspace.metadata_path) as connection:
            with pytest.raises(sqlite3.IntegrityError, match="terminal job runs are immutable"):
                connection.execute(
                    "UPDATE job_runs SET error_code = 'ALTERED' WHERE run_id = ?",
                    (run.run_id,),
                )
            with pytest.raises(
                sqlite3.IntegrityError, match="terminal job task attempts are immutable"
            ):
                connection.execute(
                    "UPDATE job_task_attempts SET exit_code = 9 WHERE run_id = ? "
                    "AND task_key = 'read' AND attempt_number = 2",
                    (run.run_id,),
                )
        assert [attempt.attempt_number for attempt in repo.list_attempts(run.run_id, "read")] == [
            1,
            2,
        ]
        with pytest.raises(JobStateTransitionError, match="active run"):
            repo.queue_attempt(run.run_id, "read")
        with pytest.raises(JobStateTransitionError, match="cannot finish"):
            repo.finish_run(run.run_id, JobRunState.FAILED)


def test_task_values_are_attempt_isolated_and_only_visible_to_successful_upstream(
    tmp_path: Path,
) -> None:
    initialized = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(initialized.path) as context:
        repo = context.jobs
        job = repo.create_job(
            "task-values",
            [
                JobTaskDefinition("producer"),
                JobTaskDefinition("reader", depends_on=("producer",)),
                JobTaskDefinition("peer"),
            ],
        )
        run = repo.submit_run(job.job_id).run
        first = repo.queue_attempt(run.run_id, "producer")
        repo.start_attempt(run.run_id, "producer", first.attempt_number)
        assert repo.set_task_value(run.run_id, "producer", 1, "state", "stale") is True
        assert repo.set_task_value(run.run_id, "producer", 1, "failed_only", "hidden") is True
        repo.finish_attempt(
            run.run_id,
            "producer",
            1,
            exit_code=9,
            termination_reason="completed",
        )

        retry = repo.queue_attempt(run.run_id, "producer")
        repo.start_attempt(run.run_id, "producer", retry.attempt_number)
        assert repo.set_task_value(run.run_id, "producer", 2, "state", "updated") is True
        assert repo.set_task_value(run.run_id, "producer", 2, "state", "fresh") is True
        assert repo.set_task_value(run.run_id, "producer", 2, "json_null", None) is True
        repo.finish_attempt(
            run.run_id,
            "producer",
            2,
            exit_code=0,
            termination_reason="completed",
        )

        reader = repo.queue_attempt(run.run_id, "reader")
        repo.start_attempt(run.run_id, "reader", reader.attempt_number)
        assert repo.get_task_value(run.run_id, "reader", 1, "producer", "state") == (
            True,
            "fresh",
        )
        assert repo.get_task_value(run.run_id, "reader", 1, "producer", "json_null") == (
            True,
            None,
        )
        assert repo.get_task_value(run.run_id, "reader", 1, "producer", "failed_only") == (
            False,
            None,
        )
        with pytest.raises(PermissionError, match="declared upstream"):
            repo.get_task_value(run.run_id, "reader", 1, "peer", "state")

        second_run = repo.submit_run(job.job_id).run
        producer = repo.queue_attempt(second_run.run_id, "producer")
        repo.start_attempt(second_run.run_id, "producer", producer.attempt_number)
        repo.finish_attempt(
            second_run.run_id,
            "producer",
            producer.attempt_number,
            exit_code=0,
            termination_reason="completed",
        )
        reader = repo.queue_attempt(second_run.run_id, "reader")
        repo.start_attempt(second_run.run_id, "reader", reader.attempt_number)
        assert repo.get_task_value(
            second_run.run_id,
            "reader",
            reader.attempt_number,
            "producer",
            "state",
        ) == (False, None)


def test_task_value_json_and_per_run_limits_are_enforced(tmp_path: Path) -> None:
    initialized = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(initialized.path) as context:
        repo = context.jobs
        job = repo.create_job("task-value-limits", [JobTaskDefinition("producer")])
        run = repo.submit_run(job.job_id).run
        attempt = repo.queue_attempt(run.run_id, "producer")
        repo.start_attempt(run.run_id, "producer", attempt.attempt_number)

        exact_limit_value = "x" * (MAX_TASK_VALUE_JSON_BYTES - 2)
        assert repo.set_task_value(run.run_id, "producer", 1, "exact", exact_limit_value)
        for index in range(MAX_TASK_VALUES_PER_RUN - 1):
            assert repo.set_task_value(run.run_id, "producer", 1, f"key_{index}", index)
        assert repo.set_task_value(run.run_id, "producer", 1, "exact", "updated")
        with pytest.raises(ValueError, match="250 distinct"):
            repo.set_task_value(run.run_id, "producer", 1, "one_too_many", True)
        with pytest.raises(ValueError, match="49152-byte"):
            repo.set_task_value(
                run.run_id,
                "producer",
                1,
                "too_large",
                "x" * (MAX_TASK_VALUE_JSON_BYTES - 1),
            )
        for invalid in ((1, 2), {1: "not-json"}, float("nan")):
            with pytest.raises(ValueError, match="JSON-compatible"):
                repo.set_task_value(run.run_id, "producer", 1, "invalid", invalid)

        repo.finish_attempt(
            run.run_id,
            "producer",
            1,
            exit_code=0,
            termination_reason="completed",
        )
        with pytest.raises(JobStateTransitionError, match="active running attempt"):
            repo.set_task_value(run.run_id, "producer", 1, "after_finish", True)


def test_interrupted_run_persists_unknown_exit_and_pending_work(tmp_path: Path) -> None:
    initialized = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(initialized.path) as context:
        repo = context.jobs
        job = repo.create_job(
            "interruptible",
            [JobTaskDefinition("active"), JobTaskDefinition("pending")],
        )
        run = repo.submit_run(job.job_id).run
        attempt = repo.queue_attempt(run.run_id, "active")
        repo.start_attempt(run.run_id, "active", attempt.attempt_number)
        interrupted = repo.interrupt_run(run.run_id)
        assert interrupted.state is JobRunState.INTERRUPTED
        assert interrupted.error_code == "WORKSPACE_RESTARTED"
        observed_attempt = repo.get_attempt(run.run_id, "active", attempt.attempt_number)
        assert observed_attempt.state is JobAttemptState.INTERRUPTED
        assert observed_attempt.exit_code is None
        assert observed_attempt.termination_reason == "interrupted"
        tasks = repo.list_task_runs(run.run_id)
        assert [task.state for task in tasks] == [JobTaskState.INTERRUPTED, JobTaskState.PENDING]

    with WorkspaceContext.local(initialized.path) as reopened:
        run = reopened.jobs.get_run(run.run_id)
        assert run.state is JobRunState.INTERRUPTED
        assert reopened.jobs.get_attempt(run.run_id, "active", 1).exit_code is None
        with pytest.raises(JobStateTransitionError, match="cannot be interrupted"):
            reopened.jobs.interrupt_run(run.run_id)


def test_attempt_output_is_bounded_persisted_and_immutable(tmp_path: Path) -> None:
    initialized = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(initialized.path) as context:
        repo = context.jobs
        job = repo.create_job("output", [JobTaskDefinition("task")])
        run = repo.submit_run(job.job_id).run
        attempt = repo.queue_attempt(run.run_id, "task")
        repo.start_attempt(run.run_id, "task", attempt.attempt_number)
        output = JobTaskAttemptOutput(
            run.run_id,
            "task",
            attempt.attempt_number,
            "captured stdout\n",
            "captured stderr\n",
            False,
            False,
        )
        repo.finish_attempt(
            run.run_id,
            "task",
            attempt.attempt_number,
            exit_code=0,
            termination_reason="completed",
            output=output,
        )
        assert repo.get_attempt_output(run.run_id, "task", 1) == output

        with pytest.raises(JobRepositoryError, match="exceeds its byte limit"):
            repo.finish_attempt(
                run.run_id,
                "task",
                attempt.attempt_number,
                exit_code=0,
                termination_reason="completed",
                output=JobTaskAttemptOutput(
                    run.run_id,
                    "task",
                    attempt.attempt_number,
                    "x" * (1024 * 1024 + 1),
                    "",
                    False,
                    False,
                ),
            )

        with sqlite3.connect(context.workspace.metadata_path) as connection:
            with pytest.raises(sqlite3.IntegrityError, match="output is immutable"):
                connection.execute(
                    "UPDATE job_task_attempt_outputs SET stdout = 'changed' "
                    "WHERE run_id = ? AND task_key = 'task' AND attempt_number = 1",
                    (run.run_id,),
                )
            with pytest.raises(sqlite3.IntegrityError, match="output is immutable"):
                connection.execute(
                    "DELETE FROM job_task_attempt_outputs "
                    "WHERE run_id = ? AND task_key = 'task' AND attempt_number = 1",
                    (run.run_id,),
                )


def test_schema_seven_migrates_without_losing_jobs_or_runs(tmp_path: Path) -> None:
    initialized = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(initialized.path) as context:
        job = context.jobs.create_job("before-upgrade", [JobTaskDefinition("task")])
        run = context.jobs.submit_run(job.job_id).run

    with sqlite3.connect(initialized.metadata_path) as connection:
        for trigger in (
            "job_deletions_are_immutable",
            "job_deletions_cannot_be_deleted",
        ):
            connection.execute(f'DROP TRIGGER IF EXISTS "{trigger}"')
        connection.execute("DROP TABLE job_deletions")
        connection.execute("DELETE FROM schema_migrations WHERE version = 12")
        for trigger in (
            "job_attempt_outputs_are_immutable",
            "job_attempt_outputs_cannot_be_deleted",
            "compute_consumer_identity_is_immutable",
            "compute_consumer_state_transitions_are_valid",
            "compute_consumers_cannot_be_deleted",
            "compute_lifecycle_events_are_immutable",
            "compute_lifecycle_events_cannot_be_deleted",
            "job_task_values_require_running_attempt_insert",
            "job_task_values_require_running_attempt_update",
            "job_task_value_identity_is_immutable",
            "job_task_values_cannot_be_deleted",
            "terraform_job_resource_identity_is_immutable",
            "terraform_job_revision_is_monotonic",
            "terraform_job_resource_delete_is_terminal",
            "terraform_job_revisions_are_immutable",
            "terraform_job_revisions_cannot_be_deleted",
            "terraform_job_resources_cannot_be_deleted",
        ):
            connection.execute(f'DROP TRIGGER IF EXISTS "{trigger}"')
        connection.execute("DROP TABLE terraform_job_revisions")
        connection.execute("DROP TABLE terraform_job_resources")
        connection.execute("DELETE FROM schema_migrations WHERE version = 11")
        connection.execute("DROP TABLE job_task_values")
        connection.execute("DROP TABLE job_task_attempt_outputs")
        connection.execute("DELETE FROM schema_migrations WHERE version = 10")
        connection.execute("DELETE FROM schema_migrations WHERE version = 9")
        connection.execute("DELETE FROM schema_migrations WHERE version = 8")
        connection.execute("PRAGMA user_version = 7")

    with WorkspaceContext.local(initialized.path) as upgraded:
        assert upgraded.workspace.metadata_schema_version == 12
        assert upgraded.jobs.get_job(job.job_id).name == "before-upgrade"
        assert upgraded.jobs.get_run(run.run_id).state is JobRunState.QUEUED
        with sqlite3.connect(upgraded.workspace.metadata_path) as connection:
            assert connection.execute("PRAGMA user_version").fetchone() == (12,)
            assert connection.execute(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' "
                "AND name = 'job_task_attempt_outputs'"
            ).fetchone() == (1,)
