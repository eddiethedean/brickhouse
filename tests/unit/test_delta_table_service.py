from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable, Schema, write_deltalake

from brickhouse.catalog import TableOwnership, parse_identifier
from brickhouse.tables import (
    DeltaRsTableService,
    IncompatibleTableSchemaError,
    InputFormat,
    SchemaDescriptor,
    SnapshotSelector,
    StagedInput,
    StagingArea,
    StagingError,
    TableAlreadyExistsError,
    TableHandle,
    TableInputError,
    TableLocationNotEmptyError,
    TableMissingError,
    TableReadResult,
    UnsupportedTableFeatureError,
    UnsupportedWriteModeError,
    WriteMode,
)
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


def _schema(data: pa.Table) -> SchemaDescriptor:
    return SchemaDescriptor.from_delta_json(Schema.from_arrow(data.schema).to_json())


def _stage(
    service: DeltaRsTableService,
    context: WorkspaceContext,
    tmp_path: Path,
    data: pa.Table,
    operation_id: str,
    *,
    data_format: InputFormat = InputFormat.PARQUET,
) -> StagedInput:
    suffix = "parquet" if data_format is InputFormat.PARQUET else "arrow"
    source = tmp_path / f"{operation_id}.{suffix}"
    if data_format is InputFormat.PARQUET:
        pq.write_table(data, source)
    else:
        with source.open("wb") as output:
            with ipc.new_file(output, data.schema) as writer:
                writer.write_table(data)
    return service.stage_input(
        source,
        _schema(data),
        data_format,
        operation_id=operation_id,
        row_count=data.num_rows,
    )


def _values(data: pa.Table) -> list[dict[str, object]]:
    return data.to_pylist()


def test_local_service_commits_and_reads_exact_delta_versions(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    initial = pa.table(
        {
            "id": pa.array([0], type=pa.int64()),
            "amount": pa.array([Decimal("1.20")], type=pa.decimal128(12, 2)),
        }
    )
    empty = pa.Table.from_batches([], schema=initial.schema)
    appended = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "amount": pa.array(
                [Decimal("2.30"), Decimal("3.40")],
                type=pa.decimal128(12, 2),
            ),
        }
    )
    overwritten = pa.table(
        {
            "id": pa.array([9], type=pa.int64()),
            "amount": pa.array([Decimal("9.90")], type=pa.decimal128(12, 2)),
        }
    )

    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "orders")
        service = DeltaRsTableService(context)
        handle = service.resolve(parse_identifier("orders"), context.new_namespace_state())
        assert handle.table_id == registration.table_id
        assert handle.ownership is TableOwnership.MANAGED

        create_stage = _stage(service, context, tmp_path, initial, "create-orders")
        created = service.write_staged(handle, create_stage, WriteMode.CREATE)
        assert created.version == 0
        assert created.rows_written == 1
        assert created.schema == _schema(initial)

        create_timestamp_ms = DeltaTable(str(handle.location)).history(limit=1)[0]["timestamp"]
        create_timestamp = datetime.fromtimestamp(create_timestamp_ms / 1000, tz=UTC)
        time.sleep(0.02)

        empty_append = _stage(service, context, tmp_path, empty, "append-empty")
        appended_empty = service.write_staged(handle, empty_append, WriteMode.APPEND)
        assert appended_empty.version == 1
        assert appended_empty.rows_written == 0

        append_stage = _stage(service, context, tmp_path, appended, "append-orders")
        appended_rows = service.write_staged(handle, append_stage, WriteMode.APPEND)
        assert appended_rows.version == 2
        assert appended_rows.rows_written == 2

        empty_overwrite = _stage(service, context, tmp_path, empty, "overwrite-empty")
        overwritten_empty = service.write_staged(handle, empty_overwrite, WriteMode.OVERWRITE)
        assert overwritten_empty.version == 3
        assert overwritten_empty.rows_written == 0

        overwrite_stage = _stage(service, context, tmp_path, overwritten, "overwrite-orders")
        replaced = service.write_staged(handle, overwrite_stage, WriteMode.OVERWRITE)
        assert replaced.version == 4
        assert replaced.rows_written == 1
        assert replaced.schema == created.schema

        latest = service.read_table(handle)
        historical = service.read_table(handle, SnapshotSelector(version=2))
        timestamped = service.read_table(
            handle,
            SnapshotSelector(timestamp=create_timestamp),
        )
        assert isinstance(latest, TableReadResult)
        assert latest.snapshot.version == 4
        assert _values(latest.data) == [{"id": 9, "amount": Decimal("9.90")}]
        assert historical.snapshot.version == 2
        assert sorted(row["id"] for row in _values(historical.data)) == [0, 1, 2]
        assert timestamped.snapshot.version == 0
        assert _values(timestamped.data) == _values(initial)
        assert latest.snapshot.schema == historical.snapshot.schema == created.schema

        history = DeltaTable(str(handle.location)).history()
        assert {entry["brickhouse.operation_id"]: entry["version"] for entry in history} == {
            "create-orders": 0,
            "append-empty": 1,
            "append-orders": 2,
            "overwrite-empty": 3,
            "overwrite-orders": 4,
        }

        staging = StagingArea(context)
        for staged in (
            create_stage,
            empty_append,
            append_stage,
            empty_overwrite,
            overwrite_stage,
        ):
            staging.cleanup(staged)


def test_create_supports_empty_table_and_arrow_ipc_staged_input(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    empty = pa.Table.from_batches(
        [],
        schema=pa.schema(
            [pa.field("id", pa.int64(), nullable=False), pa.field("label", pa.string())]
        ),
    )

    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "empty_table")
        handle = TableHandle.from_registration(registration)
        service = DeltaRsTableService(context)
        staged = _stage(
            service,
            context,
            tmp_path,
            empty,
            "create-empty",
            data_format=InputFormat.ARROW_IPC,
        )

        result = service.write_staged(handle, staged, "create")
        readback = service.read_table(handle)
        assert result.version == 0
        assert result.rows_written == 0
        assert readback.snapshot.schema == _schema(empty)
        assert readback.data.num_rows == 0


def test_write_rejects_invalid_modes_and_incompatible_schema_before_commit(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    original = pa.table({"id": pa.array([1], type=pa.int64())})
    incompatible = pa.table({"id": pa.array(["two"], type=pa.string())})

    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.create_managed("main", "default", "typed")
        handle = TableHandle.from_registration(registration)
        service = DeltaRsTableService(context)
        created = service.write_staged(
            handle,
            _stage(service, context, tmp_path, original, "create-typed"),
            "create",
        )
        with pytest.raises(TableAlreadyExistsError):
            service.write_staged(
                handle,
                _stage(service, context, tmp_path, original, "create-duplicate"),
                "create",
            )
        invalid_mode = _stage(service, context, tmp_path, original, "invalid-mode")
        incompatible_stage = _stage(
            service,
            context,
            tmp_path,
            incompatible,
            "incompatible-schema",
        )

        with pytest.raises(UnsupportedWriteModeError, match="ignore"):
            service.write_staged(handle, invalid_mode, "ignore")
        forged_manifest = replace(incompatible_stage, schema=_schema(original))
        with pytest.raises(TableInputError, match="manifest"):
            service.write_staged(handle, forged_manifest, WriteMode.APPEND)
        with pytest.raises(IncompatibleTableSchemaError, match="schema"):
            service.write_staged(handle, incompatible_stage, WriteMode.APPEND)

        tampered = _stage(service, context, tmp_path, original, "tampered-input")
        tampered.path.write_bytes(b"changed after staging")
        with pytest.raises(StagingError, match="SHA-256"):
            service.write_staged(handle, tampered, WriteMode.APPEND)

        assert created.version == 0
        assert service.read_snapshot(handle).version == 0


def test_append_missing_and_create_nonempty_external_locations_fail_safely(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    source = pa.table({"id": pa.array([1], type=pa.int64())})

    with WorkspaceContext.local(workspace.path) as context:
        service = DeltaRsTableService(context)
        missing_registration = context.tables.create_managed("main", "default", "missing")
        missing_handle = TableHandle.from_registration(missing_registration)
        staged = _stage(service, context, tmp_path, source, "append-missing")
        with pytest.raises(TableMissingError):
            service.write_staged(missing_handle, staged, WriteMode.APPEND)

        nonempty = tmp_path / "external"
        nonempty.mkdir()
        sentinel = nonempty / "keep.txt"
        sentinel.write_text("user data", encoding="utf-8")
        external_registration = context.tables.register_external(
            "main", "default", "external", nonempty
        )
        external_handle = TableHandle.from_registration(external_registration)
        create_stage = _stage(service, context, tmp_path, source, "create-external")
        with pytest.raises(TableLocationNotEmptyError):
            service.write_staged(external_handle, create_stage, WriteMode.CREATE)
        assert sentinel.read_text(encoding="utf-8") == "user data"
        assert not DeltaTable.is_deltatable(str(nonempty))


def test_writes_serialize_across_table_id_aliases_in_one_process(tmp_path: Path) -> None:
    workspace_a = initialize_workspace(tmp_path / "workspace-a")
    workspace_b = initialize_workspace(tmp_path / "workspace-b")
    shared_location = tmp_path / "shared-delta"
    initial = pa.table({"id": pa.array([0], type=pa.int64())})
    first_append = pa.table({"id": pa.array([1], type=pa.int64())})
    second_append = pa.table({"id": pa.array([2], type=pa.int64())})

    with ExitStack() as stack:
        context_a = stack.enter_context(WorkspaceContext.local(workspace_a.path))
        context_b = stack.enter_context(WorkspaceContext.local(workspace_b.path))
        registration_a = context_a.tables.register_external(
            "main", "default", "shared", shared_location
        )
        registration_b = context_b.tables.register_external(
            "main", "default", "shared", shared_location
        )
        handle_a = TableHandle.from_registration(registration_a)
        handle_b = TableHandle.from_registration(registration_b)
        service_a = DeltaRsTableService(context_a)
        service_b = DeltaRsTableService(context_b)

        created = service_a.write_staged(
            handle_a,
            _stage(service_a, context_a, tmp_path, initial, "alias-create"),
            WriteMode.CREATE,
        )
        assert created.version == 0

        staged_a = _stage(service_a, context_a, tmp_path, first_append, "alias-append-a")
        staged_b = _stage(service_b, context_b, tmp_path, second_append, "alias-append-b")
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_a = executor.submit(service_a.write_staged, handle_a, staged_a, WriteMode.APPEND)
            future_b = executor.submit(service_b.write_staged, handle_b, staged_b, WriteMode.APPEND)
            commits = (future_a.result(timeout=5), future_b.result(timeout=5))

        assert sorted(commit.version for commit in commits) == [1, 2]
        rows = service_a.read_table(handle_a).data.to_pylist()
        assert sorted(row["id"] for row in rows) == [0, 1, 2]


def test_partitioned_delta_tables_are_rejected_before_row_reads(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    table_path = tmp_path / "partitioned"
    write_deltalake(
        table_path,
        pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "region": pa.array(["east", "west"], type=pa.string()),
            }
        ),
        partition_by="region",
    )

    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.register_external(
            "main", "default", "partitioned", table_path
        )
        service = DeltaRsTableService(context)
        handle = TableHandle.from_registration(registration)
        with pytest.raises(UnsupportedTableFeatureError, match="partitioned"):
            service.read_table(handle)
