from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable, Schema

from brickhouse.tables import (
    DeltaRsTableService,
    InputFormat,
    OperationKeyReuseError,
    SchemaDescriptor,
    StagedInput,
    StagingArea,
    StagingPendingOperationError,
    TableHandle,
    TableOperationState,
    UncertainCommitError,
    WriteMode,
)
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace, open_workspace

ROOT = Path(__file__).resolve().parents[2]


def _stage(
    service: DeltaRsTableService,
    source_directory: Path,
    data: pa.Table,
    operation_id: str,
) -> StagedInput:
    source = source_directory / f"{operation_id}.parquet"
    pq.write_table(data, source)
    schema = SchemaDescriptor.from_delta_json(Schema.from_arrow(data.schema).to_json())
    return service.stage_input(
        source,
        schema,
        InputFormat.PARQUET,
        operation_id=operation_id,
        row_count=data.num_rows,
    )


def _data(values: list[int]) -> pa.Table:
    return pa.table({"id": pa.array(values, type=pa.int64())})


def test_operation_replay_is_deduplicated_and_changed_payload_is_rejected(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "orders")
        handle = TableHandle.from_registration(registration)
        service = DeltaRsTableService(context)

        create = _stage(service, tmp_path, _data([1]), "create-orders")
        first = service.write_staged(handle, create, WriteMode.CREATE)
        replay = service.write_staged(handle, create, WriteMode.CREATE)
        assert first.version == replay.version == 0
        assert DeltaTable(str(handle.location)).transaction_version(handle.table_id) == 0
        assert len(DeltaTable(str(handle.location)).history()) == 1

        changed_create = _stage(service, tmp_path, _data([2]), "create-orders")
        with pytest.raises(OperationKeyReuseError, match="different payload"):
            service.write_staged(handle, changed_create, WriteMode.CREATE)

        append = _stage(service, tmp_path, _data([2]), "append-orders")
        second = service.write_staged(handle, append, WriteMode.APPEND)
        append_replay = service.write_staged(handle, append, WriteMode.APPEND)
        assert second.version == append_replay.version == 1
        assert DeltaTable(str(handle.location)).transaction_version(handle.table_id) == 1

        changed_append = _stage(service, tmp_path, _data([3]), "append-orders")
        with pytest.raises(OperationKeyReuseError, match="different payload"):
            service.write_staged(handle, changed_append, WriteMode.APPEND)

        third = service.write_staged(
            handle,
            _stage(service, tmp_path, _data([3]), "append-orders-again"),
            WriteMode.APPEND,
        )
        assert third.version == 2
        assert DeltaTable(str(handle.location)).transaction_version(handle.table_id) == 2
        assert sorted(row["id"] for row in service.read_table(handle).data.to_pylist()) == [1, 2, 3]

        with sqlite3.connect(workspace.metadata_path) as connection:
            records = connection.execute(
                "SELECT operation_id, transaction_version, base_version, state, commit_version "
                "FROM table_operation_journal WHERE table_id = ? ORDER BY transaction_version",
                (handle.table_id,),
            ).fetchall()
        assert records == [
            ("create-orders", 0, -1, "committed", 0),
            ("append-orders", 1, 0, "committed", 1),
            ("append-orders-again", 2, 1, "committed", 2),
        ]


def test_metadata_schema_17_operation_records_migrate_through_schema_21(
    tmp_path: Path,
    downgrade_from_v21,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "orders")
        table = TableHandle.from_registration(registration)
        DeltaRsTableService(context).write_arrow(
            table,
            _data([1, 2]),
            WriteMode.CREATE,
            operation_id="before-v18-migration",
        )

    with sqlite3.connect(workspace.metadata_path) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        downgrade_from_v21(connection)
        connection.execute("DROP INDEX table_operation_journal_pending_idx")
        connection.execute(
            "ALTER TABLE table_operation_journal RENAME TO table_operation_journal_v18"
        )
        connection.execute(
            "CREATE TABLE table_operation_journal ("
            "table_id TEXT NOT NULL, operation_id TEXT NOT NULL, "
            "fingerprint TEXT NOT NULL CHECK(length(fingerprint) = 64), "
            "mode TEXT NOT NULL CHECK(mode IN ('create', 'append', 'overwrite')), "
            "schema_fingerprint TEXT NOT NULL CHECK(length(schema_fingerprint) = 64), "
            "input_sha256 TEXT NOT NULL CHECK(length(input_sha256) = 64), "
            "transaction_version INTEGER NOT NULL CHECK(transaction_version >= 0), "
            "base_version INTEGER NOT NULL CHECK(base_version >= -1), "
            "row_count INTEGER NOT NULL CHECK(row_count >= 0), "
            "state TEXT NOT NULL CHECK(state IN "
            "('prepared', 'committing', 'committed', 'uncertain')), "
            "commit_version INTEGER CHECK(commit_version IS NULL OR commit_version >= 0), "
            "error_code TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, "
            "PRIMARY KEY(table_id, operation_id), UNIQUE(table_id, transaction_version), "
            "CHECK((state = 'committed' AND commit_version IS NOT NULL) OR "
            "(state != 'committed' AND commit_version IS NULL)), "
            "FOREIGN KEY (table_id) REFERENCES table_registrations(table_id) ON DELETE RESTRICT"
            ")"
        )
        connection.execute(
            "INSERT INTO table_operation_journal "
            "SELECT table_id, operation_id, fingerprint, mode, schema_fingerprint, input_sha256, "
            "transaction_version, base_version, row_count, state, commit_version, error_code, "
            "created_at, updated_at FROM table_operation_journal_v18"
        )
        connection.execute("DROP TABLE table_operation_journal_v18")
        connection.execute(
            "CREATE INDEX table_operation_journal_pending_idx "
            "ON table_operation_journal(table_id, state, transaction_version)"
        )
        connection.execute("DELETE FROM schema_migrations WHERE version = 18")
        connection.execute("DELETE FROM schema_migrations WHERE version = 19")
        connection.execute("DELETE FROM schema_migrations WHERE version = 20")
        connection.execute("DELETE FROM schema_migrations WHERE version = 21")
        connection.execute("PRAGMA user_version = 17")
        connection.execute("PRAGMA foreign_keys = ON")

    assert open_workspace(workspace.path).metadata_schema_version == 17
    with WorkspaceContext.local(workspace.path) as context:
        assert context.workspace.metadata_schema_version == 21
        record = context.table_operations.get(table.table_id, "before-v18-migration")
        assert record is not None
        assert record.state is TableOperationState.COMMITTED
        assert (record.transaction_version, record.commit_version, record.row_count) == (0, 0, 2)
        with sqlite3.connect(context.workspace.metadata_path) as connection:
            assert connection.execute("PRAGMA user_version").fetchone() == (21,)
            assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.skipif(os.name != "posix", reason="the workspace owner lock is POSIX-only")
@pytest.mark.parametrize(
    "crash_boundary",
    [
        pytest.param("before_delta", id="before-delta-commit"),
        pytest.param("before_sqlite_finalize", id="after-delta-before-sqlite-finalize"),
        pytest.param("after_sqlite_finalize", id="after-sqlite-finalize"),
    ],
)
def test_killed_writer_reconciles_each_delta_sqlite_boundary(
    tmp_path: Path,
    crash_boundary: str,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    source = tmp_path / "source.parquet"
    data = _data([42])
    pq.write_table(data, source)
    manifest_path = tmp_path / "staged-manifest.json"

    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "recovered")
        handle = TableHandle.from_registration(registration)
        service = DeltaRsTableService(context)
        schema = SchemaDescriptor.from_delta_json(Schema.from_arrow(data.schema).to_json())
        staged = service.stage_input(
            source,
            schema,
            InputFormat.PARQUET,
            operation_id=f"crash-{crash_boundary}",
            row_count=1,
        )
        manifest_path.write_text(
            json.dumps(
                {
                    "staging_id": staged.staging_id,
                    "workspace_id": staged.workspace_id,
                    "operation_id": staged.operation_id,
                    "path": str(staged.path),
                    "data_format": staged.data_format.value,
                    "schema": staged.schema.json_text,
                    "row_count": staged.row_count,
                    "byte_count": staged.byte_count,
                    "sha256": staged.sha256,
                    "created_at": staged.created_at.isoformat(),
                    "table_id": handle.table_id,
                }
            ),
            encoding="utf-8",
        )

    child_code = r"""
import json
import os
import signal
import sys
from datetime import datetime
from pathlib import Path
from brickhouse.tables import (
    DeltaRsTableService,
    InputFormat,
    SchemaDescriptor,
    StagedInput,
    TableHandle,
    WriteMode,
)
from brickhouse.workspace.context import WorkspaceContext

manifest = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
crash_boundary = sys.argv[3]
staged = StagedInput(
    staging_id=manifest["staging_id"], workspace_id=manifest["workspace_id"],
    operation_id=manifest["operation_id"], path=Path(manifest["path"]),
    data_format=InputFormat(manifest["data_format"]),
    schema=SchemaDescriptor(manifest["schema"]), row_count=manifest["row_count"],
    byte_count=manifest["byte_count"], sha256=manifest["sha256"],
    created_at=datetime.fromisoformat(manifest["created_at"]),
)
with WorkspaceContext.local(sys.argv[1]) as context:
    registration = context.tables.get_by_id(manifest["table_id"])
    handle = TableHandle.from_registration(registration)
    service = DeltaRsTableService(context)
    def terminate_at_boundary():
        os.kill(os.getpid(), signal.SIGKILL)
    if crash_boundary == "before_delta":
        mark_committing = context.table_operations.mark_committing
        def mark_then_terminate(*args, **kwargs):
            mark_committing(*args, **kwargs)
            terminate_at_boundary()
        context.table_operations.mark_committing = mark_then_terminate
    elif crash_boundary == "before_sqlite_finalize":
        def terminate_before_finalize(*args, **kwargs):
            terminate_at_boundary()
        context.table_operations.finalize_committed = terminate_before_finalize
    elif crash_boundary == "after_sqlite_finalize":
        finalize_committed = context.table_operations.finalize_committed
        def finalize_then_terminate(*args, **kwargs):
            result = finalize_committed(*args, **kwargs)
            terminate_at_boundary()
            return result
        context.table_operations.finalize_committed = finalize_then_terminate
    else:
        raise AssertionError(f"unexpected crash boundary: {crash_boundary}")
    service.write_staged(handle, staged, WriteMode.CREATE)
"""
    env = {key: value for key, value in os.environ.items() if not key.startswith("DATABRICKS_")}
    env["PYTHONPATH"] = str(ROOT / "src")
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            child_code,
            str(workspace.path),
            str(manifest_path),
            crash_boundary,
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        check=False,
        timeout=20,
        text=True,
    )
    assert child.returncode == -signal.SIGKILL, child.stderr

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    staged = StagedInput(
        staging_id=manifest["staging_id"],
        workspace_id=manifest["workspace_id"],
        operation_id=manifest["operation_id"],
        path=Path(manifest["path"]),
        data_format=InputFormat(manifest["data_format"]),
        schema=SchemaDescriptor(manifest["schema"]),
        row_count=manifest["row_count"],
        byte_count=manifest["byte_count"],
        sha256=manifest["sha256"],
        created_at=datetime.fromisoformat(manifest["created_at"]),
    )
    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.get_by_id(manifest["table_id"])
        handle = TableHandle.from_registration(registration)
        service = DeltaRsTableService(context)
        pre_replay = context.table_operations.get(handle.table_id, staged.operation_id)
        assert pre_replay is not None
        changed_payload = _stage(service, tmp_path, _data([43]), staged.operation_id)
        if crash_boundary == "before_delta":
            assert pre_replay.state is TableOperationState.COMMITTING
            assert not (handle.location / "_delta_log").exists()
            with pytest.raises(UncertainCommitError):
                service.write_staged(handle, staged, WriteMode.CREATE)
            record = context.table_operations.get(handle.table_id, staged.operation_id)
            assert record is not None and record.state is TableOperationState.UNCERTAIN
            assert not (handle.location / "_delta_log").exists()
            with pytest.raises(OperationKeyReuseError, match="different payload"):
                service.write_staged(handle, changed_payload, WriteMode.CREATE)
            with pytest.raises(StagingPendingOperationError, match="reconciliation"):
                StagingArea(context).cleanup(staged)
            assert staged.path.exists()
            return
        if crash_boundary == "before_sqlite_finalize":
            assert pre_replay.state is TableOperationState.COMMITTING
        else:
            assert pre_replay.state is TableOperationState.COMMITTED
            assert pre_replay.commit_version == 0
        result = service.write_staged(handle, staged, WriteMode.CREATE)
        record = context.table_operations.get(handle.table_id, staged.operation_id)
        assert record is not None
        assert record.state is TableOperationState.COMMITTED
        assert record.commit_version == result.version == 0
        assert service.read_table(handle).data.to_pylist() == [{"id": 42}]
        assert len(DeltaTable(str(handle.location)).history()) == 1
        with pytest.raises(OperationKeyReuseError, match="different payload"):
            service.write_staged(handle, changed_payload, WriteMode.CREATE)
        assert service.read_table(handle).data.to_pylist() == [{"id": 42}]
        assert len(DeltaTable(str(handle.location)).history()) == 1
        StagingArea(context).cleanup(staged)
        StagingArea(context).cleanup(changed_payload)
        assert not staged.path.exists()
        assert not changed_payload.path.exists()


def test_failed_provider_outcome_is_uncertain_and_never_blindly_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "uncertain")
        handle = TableHandle.from_registration(registration)
        service = DeltaRsTableService(context)
        staged = _stage(service, tmp_path, _data([7]), "uncertain-create")
        dispatch_count = 0

        def fail_before_commit(*args: object, **kwargs: object) -> None:
            nonlocal dispatch_count
            dispatch_count += 1
            raise RuntimeError("injected provider failure")

        monkeypatch.setattr(service._delta, "write_deltalake", fail_before_commit)
        with pytest.raises(UncertainCommitError, match="inspect table history"):
            service.write_staged(handle, staged, WriteMode.CREATE)
        record = context.table_operations.get(handle.table_id, staged.operation_id)
        assert record is not None and record.state is TableOperationState.UNCERTAIN

        with pytest.raises(UncertainCommitError):
            service.write_staged(handle, staged, WriteMode.CREATE)
        with pytest.raises(UncertainCommitError):
            service.write_staged(
                handle,
                _stage(service, tmp_path, _data([8]), "later-create"),
                WriteMode.CREATE,
            )
        assert dispatch_count == 1


def test_workspace_schema_three_migrates_through_latest_metadata_version(
    tmp_path: Path,
    downgrade_workspace_from_v13,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with sqlite3.connect(workspace.metadata_path) as connection:
        downgrade_workspace_from_v13(connection)
        for trigger in (
            "job_deletions_are_immutable",
            "job_deletions_cannot_be_deleted",
        ):
            connection.execute(f"DROP TRIGGER {trigger}")
        connection.execute("DROP TABLE job_deletions")
        connection.execute("DELETE FROM schema_migrations WHERE version = 12")
        for trigger in (
            "terraform_job_resource_identity_is_immutable",
            "terraform_job_revision_is_monotonic",
            "terraform_job_resource_delete_is_terminal",
            "terraform_job_revisions_are_immutable",
            "terraform_job_revisions_cannot_be_deleted",
            "terraform_job_resources_cannot_be_deleted",
            "compute_resource_state_transitions_are_valid",
            "compute_resource_generations_are_monotonic",
            "deleted_compute_resources_are_immutable",
            "compute_resource_identity_is_immutable",
            "compute_resources_cannot_be_deleted",
            "released_compute_consumers_are_immutable",
            "compute_consumer_identity_is_immutable",
            "compute_consumer_state_transitions_are_valid",
            "compute_consumers_cannot_be_deleted",
            "compute_lifecycle_events_are_immutable",
            "compute_lifecycle_events_cannot_be_deleted",
            "job_task_values_require_running_attempt_insert",
            "job_task_values_require_running_attempt_update",
            "job_task_value_identity_is_immutable",
            "job_task_values_cannot_be_deleted",
        ):
            connection.execute(f"DROP TRIGGER {trigger}")
        connection.execute("DROP TABLE terraform_job_revisions")
        connection.execute("DROP TABLE terraform_job_resources")
        connection.execute("DELETE FROM schema_migrations WHERE version = 11")
        connection.execute("DROP TABLE job_task_values")
        connection.execute("DROP TABLE compute_lifecycle_events")
        connection.execute("DROP TABLE compute_consumers")
        connection.execute("DROP TABLE compute_resources")
        connection.execute("DELETE FROM schema_migrations WHERE version = 10")
        connection.execute("DROP TABLE job_task_attempt_outputs")
        connection.execute("DELETE FROM schema_migrations WHERE version = 9")
        connection.execute("DELETE FROM schema_migrations WHERE version = 8")
        connection.execute("DELETE FROM schema_migrations WHERE version = 7")
        connection.execute("DROP TRIGGER job_definitions_are_immutable")
        connection.execute("DROP TRIGGER job_task_definitions_are_immutable")
        connection.execute("DROP TRIGGER job_run_submission_is_immutable")
        connection.execute("DROP TRIGGER terminal_job_runs_are_immutable")
        connection.execute("DROP TRIGGER job_run_task_definition_is_immutable")
        connection.execute("DROP TRIGGER terminal_job_tasks_are_immutable")
        connection.execute("DROP TRIGGER job_attempt_identity_is_immutable")
        connection.execute("DROP TRIGGER terminal_job_attempts_are_immutable")
        connection.execute("DROP TABLE job_task_attempts")
        connection.execute("DROP TABLE job_run_tasks")
        connection.execute("DROP TABLE job_runs")
        connection.execute("DROP TABLE job_tasks")
        connection.execute("DROP TABLE jobs")
        connection.execute("DROP TABLE table_operation_journal")
        connection.execute("DELETE FROM schema_migrations WHERE version = 4")
        connection.execute("DROP TABLE worker_attempt_outcomes")
        connection.execute("DELETE FROM schema_migrations WHERE version = 5")
        connection.execute("DELETE FROM schema_migrations WHERE version = 6")
        connection.execute("DELETE FROM schema_migrations WHERE version > 3")
        connection.execute("PRAGMA user_version = 3")

    assert open_workspace(workspace.path).metadata_schema_version == 3
    with WorkspaceContext.local(workspace.path) as context:
        assert context.workspace.metadata_schema_version == 21
        with sqlite3.connect(context.workspace.metadata_path) as connection:
            assert connection.execute("PRAGMA user_version").fetchone() == (21,)
            assert connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            ).fetchall() == [(version,) for version in range(1, 22)]
            assert connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name = 'table_operation_journal'"
            ).fetchone() == ("table_operation_journal",)
            assert connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name = 'worker_attempt_outcomes'"
            ).fetchone() == ("worker_attempt_outcomes",)
            assert connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'job_task_attempts'"
            ).fetchone() == ("job_task_attempts",)
