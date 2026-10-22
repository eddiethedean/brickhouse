from __future__ import annotations

import json
import shutil
from pathlib import Path

import pyarrow as pa
import pytest

from brickhouse.catalog import TableOwnership
from brickhouse.jobs import (
    JobAttemptState,
    JobRunState,
    JobTaskAttemptOutput,
    JobTaskDefinition,
)
from brickhouse.tables import (
    DeltaRsTableService,
    OperationKeyReuseError,
    TableHandle,
    TableMissingError,
    WriteMode,
)
from brickhouse.workspace import (
    WorkspaceContext,
    WorkspaceInUseError,
    WorkspaceSnapshotBusyError,
    WorkspaceSnapshotError,
    initialize_workspace,
    restore_workspace_snapshot,
    snapshot_workspace,
)


def test_quiesced_snapshot_restore_rebases_managed_data_and_preserves_external_refs(
    tmp_path: Path,
) -> None:
    source = initialize_workspace(tmp_path / "source")
    external_location = source.path / "files" / "external_delta"
    (source.path / "files" / "input.txt").write_text("copied workspace file", encoding="utf-8")

    with WorkspaceContext.local(source.path) as context:
        managed = context.tables.create_managed("main", "default", "managed_orders")
        external = context.tables.register_external(
            "main", "default", "external_orders", external_location
        )
        tables = DeltaRsTableService(context)
        for registration, operation_id, values in (
            (managed, "seed-managed", [1, 2]),
            (external, "seed-external", [9]),
        ):
            tables.write_arrow(
                TableHandle.from_registration(registration),
                pa.table({"id": pa.array(values, type=pa.int64())}),
                WriteMode.CREATE,
                operation_id=operation_id,
            )

        job = context.jobs.create_job("snapshot-history", [JobTaskDefinition("task")])
        run = context.jobs.submit_run(job.job_id).run
        attempt = context.jobs.queue_attempt(run.run_id, "task")
        context.jobs.start_attempt(run.run_id, "task", attempt.attempt_number)
        context.jobs.finish_attempt(
            run.run_id,
            "task",
            attempt.attempt_number,
            exit_code=0,
            termination_reason="completed",
            output=JobTaskAttemptOutput(
                run.run_id,
                "task",
                attempt.attempt_number,
                "restored job output",
                "",
                False,
                False,
            ),
        )
        context.jobs.finish_run(run.run_id, JobRunState.SUCCEEDED)

        with pytest.raises(WorkspaceInUseError, match="already owned"):
            snapshot_workspace(source.path, tmp_path / "active-snapshot")

    snapshot = snapshot_workspace(source.path, tmp_path / "snapshot")
    assert snapshot.workspace_id == source.workspace_id
    assert len(snapshot.external_tables) == 1
    external_reference = snapshot.external_tables[0]
    assert external_reference.table_id == external.table_id
    assert external_reference.full_name == "main.default.external_orders"
    assert external_reference.location == str(external_location)
    assert external_reference.available_at_snapshot is True
    assert (snapshot.path / "tables" / managed.table_id / "_delta_log").is_dir()
    assert (snapshot.path / "files" / "input.txt").read_text(encoding="utf-8") == (
        "copied workspace file"
    )
    assert not (snapshot.path / "files" / "external_delta").exists()
    manifest = json.loads((snapshot.path / "brickhouse.snapshot.json").read_text())
    assert manifest["external_table_policy"] == "exclude_bytes_preserve_registration"

    unavailable_location = external_location.with_name("external_delta.unavailable")
    external_location.rename(unavailable_location)
    try:
        restored = restore_workspace_snapshot(snapshot.path, tmp_path / "restored")
        assert restored.workspace.workspace_id == source.workspace_id
        assert restored.external_tables == snapshot.external_tables

        with WorkspaceContext.local(restored.workspace.path) as context:
            managed_registration = context.tables.get("main", "default", "managed_orders")
            external_registration = context.tables.get("main", "default", "external_orders")
            assert managed_registration.canonical_location == (
                restored.workspace.path / "tables" / managed.table_id
            )
            assert managed_registration.ownership is TableOwnership.MANAGED
            assert external_registration.canonical_location == external_location
            assert external_registration.ownership is TableOwnership.EXTERNAL

            tables = DeltaRsTableService(context)
            table_handle = TableHandle.from_registration(managed_registration)
            read = tables.read_table(table_handle)
            assert read.data.to_pylist() == [{"id": 1}, {"id": 2}]
            assert read.snapshot.version == 0
            history = tables.history(table_handle)
            assert len(history) == 1
            assert history[0].version == 0
            operation = context.table_operations.get(managed.table_id, "seed-managed")
            assert operation is not None
            assert operation.state.value == "committed"
            assert operation.commit_version == 0
            replayed_create = tables.write_arrow(
                table_handle,
                pa.table({"id": pa.array([1, 2], type=pa.int64())}),
                WriteMode.CREATE,
                operation_id="seed-managed",
            )
            assert replayed_create.version == 0
            with pytest.raises(OperationKeyReuseError, match="different payload"):
                tables.write_arrow(
                    table_handle,
                    pa.table({"id": pa.array([1, 3], type=pa.int64())}),
                    WriteMode.CREATE,
                    operation_id="seed-managed",
                )

            restored_job = context.jobs.get_job(job.job_id)
            restored_run = context.jobs.get_run(run.run_id)
            restored_attempt = context.jobs.get_attempt(run.run_id, "task", 1)
            restored_output = context.jobs.get_attempt_output(run.run_id, "task", 1)
            assert restored_job.name == "snapshot-history"
            assert restored_run.state is JobRunState.SUCCEEDED
            assert restored_attempt.state is JobAttemptState.SUCCEEDED
            assert restored_output is not None
            assert restored_output.stdout == "restored job output"
            assert (restored.workspace.path / "files" / "input.txt").read_text(
                encoding="utf-8"
            ) == "copied workspace file"
            assert not (restored.workspace.path / "files" / "external_delta").exists()

            with pytest.raises(TableMissingError, match="Delta table does not exist"):
                tables.read_table(TableHandle.from_registration(external_registration))
    finally:
        unavailable_location.rename(external_location)


def test_snapshot_refuses_queued_job_state_after_service_quiesces(tmp_path: Path) -> None:
    source = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(source.path) as context:
        job = context.jobs.create_job("queued-run", [JobTaskDefinition("task")])
        context.jobs.submit_run(job.job_id)

    target = tmp_path / "snapshot"
    with pytest.raises(WorkspaceSnapshotBusyError, match="unfinished record.*job_runs"):
        snapshot_workspace(source.path, target)
    assert not target.exists()


def test_snapshot_destination_cannot_overlap_external_table_storage(tmp_path: Path) -> None:
    source = initialize_workspace(tmp_path / "workspace")
    external_location = tmp_path / "external-zone" / "snapshot" / "external_delta"
    with WorkspaceContext.local(source.path) as context:
        context.tables.register_external("main", "default", "external_orders", external_location)

    target = tmp_path / "external-zone" / "snapshot"
    with pytest.raises(WorkspaceSnapshotError, match="overlaps external table location"):
        snapshot_workspace(source.path, target)
    assert not (tmp_path / "external-zone").exists()


def test_restore_rejects_missing_managed_snapshot_bytes_after_source_is_removed(
    tmp_path: Path,
) -> None:
    source = initialize_workspace(tmp_path / "source")
    with WorkspaceContext.local(source.path) as context:
        registration = context.tables.create_managed("main", "default", "managed_orders")
        DeltaRsTableService(context).write_arrow(
            TableHandle.from_registration(registration),
            pa.table({"id": pa.array([1], type=pa.int64())}),
            WriteMode.CREATE,
            operation_id="snapshot-inventory-create",
        )

    snapshot = snapshot_workspace(source.path, tmp_path / "snapshot")
    assert (
        json.loads((snapshot.path / "brickhouse.snapshot.json").read_text())["managed_tables"][0][
            "materialized"
        ]
        is True
    )
    shutil.rmtree(source.path)
    shutil.rmtree(snapshot.path / "tables" / registration.table_id)

    with pytest.raises(WorkspaceSnapshotError, match="missing from the snapshot"):
        restore_workspace_snapshot(snapshot.path, tmp_path / "restored")
    assert not (tmp_path / "restored").exists()


def test_unmaterialized_managed_registration_restores_without_source_workspace(
    tmp_path: Path,
) -> None:
    source = initialize_workspace(tmp_path / "source")
    with WorkspaceContext.local(source.path) as context:
        registration = context.tables.create_managed("main", "default", "empty_table")

    snapshot = snapshot_workspace(source.path, tmp_path / "snapshot")
    inventory = json.loads((snapshot.path / "brickhouse.snapshot.json").read_text())[
        "managed_tables"
    ]
    assert inventory == [{"table_id": registration.table_id, "materialized": False, "sha256": None}]
    shutil.rmtree(source.path)

    restored = restore_workspace_snapshot(snapshot.path, tmp_path / "restored")
    with WorkspaceContext.local(restored.workspace.path) as context:
        restored_registration = context.tables.get("main", "default", "empty_table")
        assert restored_registration.table_id == registration.table_id
        assert not (restored.workspace.path / "tables" / registration.table_id).exists()
