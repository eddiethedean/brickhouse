from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable, write_deltalake

from brickhouse.catalog import TableOwnership
from brickhouse.tables import (
    DEFAULT_DELTA_PROVIDER_PROFILE,
    DeltaProtocol,
    IncompatibleTableSchemaError,
    InputFormat,
    SchemaDescriptor,
    SnapshotSelectionError,
    SnapshotSelector,
    StagingArea,
    StagingError,
    StagingOwnershipError,
    TableAlreadyExistsError,
    TableHandle,
    TableMissingError,
    TableSnapshot,
    UnsupportedDeltaProtocolError,
    UnsupportedTableFeatureError,
    UnsupportedWriteModeError,
    WriteMode,
    validate_write_request,
)
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


def _protocol_from_provider(table: DeltaTable) -> DeltaProtocol:
    protocol = table.protocol()
    return DeltaProtocol(
        min_reader_version=protocol.min_reader_version,
        min_writer_version=protocol.min_writer_version,
        reader_features=tuple(protocol.reader_features or ()),
        writer_features=tuple(protocol.writer_features or ()),
    )


def test_snapshot_contract_captures_real_delta_identity_schema_protocol_and_version(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    table_path = tmp_path / "external-delta"
    first_input = pa.table(
        {
            "order_id": pa.array([1], type=pa.int64()),
            "labels": pa.array([["new", "sale"]], type=pa.list_(pa.string())),
        }
    )
    write_deltalake(table_path, first_input)
    write_deltalake(table_path, first_input, mode="append")

    with WorkspaceContext.local(workspace.path) as context:
        registration = context.tables.register_external("main", "default", "orders", table_path)
        handle = TableHandle.from_registration(registration)
        latest_provider_table = DeltaTable(table_path)
        latest = TableSnapshot(
            table=handle,
            version=latest_provider_table.version(),
            schema=SchemaDescriptor.from_delta_json(latest_provider_table.schema().to_json()),
            protocol=_protocol_from_provider(latest_provider_table),
        )
        DEFAULT_DELTA_PROVIDER_PROFILE.validate_snapshot(latest)

        selector = SnapshotSelector(version=0)
        selected_provider_table = DeltaTable(table_path, version=selector.version)
        selected = TableSnapshot(
            table=handle,
            version=selected_provider_table.version(),
            schema=SchemaDescriptor.from_delta_json(selected_provider_table.schema().to_json()),
            protocol=_protocol_from_provider(selected_provider_table),
        )

        assert registration.ownership is TableOwnership.EXTERNAL
        assert handle.table_id == registration.table_id
        assert handle.catalog_id == registration.catalog_id
        assert handle.schema_id == registration.schema_id
        assert handle.location == table_path.resolve()
        assert handle.full_name == "main.default.orders"
        assert latest.version == 1
        assert selected.version == 0
        assert latest.schema.field_names == ("order_id", "labels")
        assert latest.schema.fingerprint == selected.schema.fingerprint
        assert latest.protocol == DeltaProtocol(1, 2)
        assert latest.captured_at.tzinfo is UTC


def test_snapshot_selector_rejects_invalid_or_ambiguous_requests() -> None:
    assert SnapshotSelector().version is None
    assert SnapshotSelector(version=0).version == 0
    assert SnapshotSelector(timestamp=datetime(2026, 1, 1, tzinfo=UTC)).timestamp == datetime(
        2026, 1, 1, tzinfo=UTC
    )
    with pytest.raises(SnapshotSelectionError, match="zero or greater"):
        SnapshotSelector(version=-1)
    with pytest.raises(SnapshotSelectionError, match="mutually exclusive"):
        SnapshotSelector(version=1, timestamp=datetime(2026, 1, 1, tzinfo=UTC))
    with pytest.raises(SnapshotSelectionError, match="timezone"):
        SnapshotSelector(timestamp=datetime(2026, 1, 1))


def test_delta_provider_profile_rejects_unsupported_protocols_and_features(
    tmp_path: Path,
) -> None:
    handle = TableHandle(
        table_id="table-id",
        catalog_id="catalog-id",
        schema_id="schema-id",
        catalog_name="main",
        schema_name="default",
        name="orders",
        table_format="delta",
        ownership=TableOwnership.EXTERNAL,
        location=tmp_path / "orders",
    )
    schema = SchemaDescriptor('{"type":"struct","fields":[]}')
    with pytest.raises(UnsupportedDeltaProtocolError, match="requires 1/2"):
        DEFAULT_DELTA_PROVIDER_PROFILE.validate_snapshot(
            TableSnapshot(handle, 0, schema, DeltaProtocol(3, 7))
        )
    with pytest.raises(UnsupportedTableFeatureError, match="deletionVectors"):
        DEFAULT_DELTA_PROVIDER_PROFILE.validate_snapshot(
            TableSnapshot(
                handle,
                0,
                schema,
                DeltaProtocol(1, 2, reader_features=("deletionVectors",)),
            )
        )


def test_write_contract_validates_mode_target_protocol_and_schema(tmp_path: Path) -> None:
    handle = TableHandle(
        table_id="table-id",
        catalog_id="catalog-id",
        schema_id="schema-id",
        catalog_name="main",
        schema_name="default",
        name="orders",
        table_format="delta",
        ownership=TableOwnership.MANAGED,
        location=tmp_path / "orders",
    )
    schema = SchemaDescriptor(
        '{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}'
    )
    snapshot = TableSnapshot(handle, 0, schema, DeltaProtocol(1, 2))

    validate_write_request(None, WriteMode.CREATE, schema)
    validate_write_request(snapshot, WriteMode.APPEND, schema)
    validate_write_request(snapshot, WriteMode.OVERWRITE, schema)
    with pytest.raises(TableAlreadyExistsError):
        validate_write_request(snapshot, "create", schema)
    with pytest.raises(TableMissingError):
        validate_write_request(None, "append", schema)
    with pytest.raises(UnsupportedWriteModeError):
        validate_write_request(snapshot, "ignore", schema)
    with pytest.raises(IncompatibleTableSchemaError):
        validate_write_request(
            snapshot,
            "append",
            SchemaDescriptor('{"type":"struct","fields":[]}'),
        )


def test_staging_copies_real_parquet_with_digest_ownership_and_safe_cleanup(
    tmp_path: Path,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    source = tmp_path / "input.parquet"
    table = pa.table({"id": pa.array([1, 2], type=pa.int64())})
    pq.write_table(table, source)
    schema = SchemaDescriptor(
        '{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}}]}'
    )

    with WorkspaceContext.local(workspace.path) as context:
        staging = StagingArea(context)
        staged = staging.stage_file(
            source,
            schema,
            InputFormat.PARQUET,
            operation_id="operation-42",
            row_count=2,
        )
        assert staged.path.is_relative_to(workspace.path / "staging")
        assert staged.path.read_bytes() == source.read_bytes()
        assert pq.read_table(staged.path).equals(table)
        assert staged.byte_count == source.stat().st_size
        assert staged.sha256 == hashlib.sha256(source.read_bytes()).hexdigest()
        assert staged.operation_id == "operation-42"
        if staged.path.stat().st_mode & 0o777 != 0o600:
            pytest.fail("staged input should be mode 0600")

        forged = replace(staged, path=source)
        with pytest.raises(StagingOwnershipError):
            staging.cleanup(forged)
        assert source.exists()
        staging.cleanup(staged)
        assert not staged.path.exists()
        assert source.exists()

        with pytest.raises(StagingError, match="does not exist"):
            staging.stage_file(
                tmp_path / "missing.parquet",
                schema,
                InputFormat.PARQUET,
                operation_id="operation-43",
                row_count=0,
            )
