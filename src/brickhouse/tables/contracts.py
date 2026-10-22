"""Provider-neutral contracts for Delta table identity and snapshots."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from brickhouse.catalog import NamespaceState, ParsedIdentifier, TableOwnership, TableRegistration


class TableServiceError(RuntimeError):
    """Base error for table identity, snapshot, and write validation."""


class SnapshotSelectionError(TableServiceError):
    """A requested snapshot selector is invalid or contradictory."""


class UnsupportedDeltaProtocolError(TableServiceError):
    """A table protocol is outside the declared Delta provider profile."""


class UnsupportedTableFeatureError(UnsupportedDeltaProtocolError):
    """A table requires a Delta feature outside the provider profile."""


class UnsupportedWriteModeError(TableServiceError):
    """A write mode is not part of the initial table-service contract."""


class IncompatibleTableSchemaError(TableServiceError):
    """An append or overwrite input schema differs from the table schema."""


class TableAlreadyExistsError(TableServiceError):
    """A create operation targeted an existing table snapshot."""


class TableMissingError(TableServiceError):
    """An append or overwrite operation targeted a missing table snapshot."""


class WriteMode(StrEnum):
    CREATE = "create"
    APPEND = "append"
    OVERWRITE = "overwrite"


class InputFormat(StrEnum):
    PARQUET = "parquet"
    ARROW_IPC = "arrow-ipc"

    @property
    def suffix(self) -> str:
        return {InputFormat.PARQUET: ".parquet", InputFormat.ARROW_IPC: ".arrow"}[self]


@dataclass(frozen=True, slots=True)
class SchemaDescriptor:
    """Canonical Delta schema JSON; nested field/type metadata stays intact."""

    json_text: str

    def __post_init__(self) -> None:
        try:
            decoded = json.loads(self.json_text)
        except (TypeError, json.JSONDecodeError) as error:
            raise ValueError("schema must be valid Delta schema JSON") from error
        if not isinstance(decoded, dict) or decoded.get("type") != "struct":
            raise ValueError("schema JSON must describe a struct")
        fields = decoded.get("fields")
        if not isinstance(fields, list):
            raise ValueError("schema struct must contain a fields array")
        for field_value in fields:
            if not isinstance(field_value, dict):
                raise ValueError("schema fields must be JSON objects")
            if not isinstance(field_value.get("name"), str) or not field_value["name"]:
                raise ValueError("schema field name must be a non-empty string")
            if not isinstance(field_value.get("type"), (str, dict)):
                raise ValueError("schema field type must be a string or JSON object")
            if not isinstance(field_value.get("nullable"), bool):
                raise ValueError("schema field nullable must be a boolean")
            metadata = field_value.get("metadata", {})
            if not isinstance(metadata, dict):
                raise ValueError("schema field metadata must be a JSON object")
        canonical = json.dumps(decoded, sort_keys=True, separators=(",", ":"))
        object.__setattr__(self, "json_text", canonical)

    @classmethod
    def from_delta_json(cls, value: str) -> SchemaDescriptor:
        """Capture the public JSON form returned by the selected Delta provider."""
        return cls(value)

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self.json_text.encode("utf-8")).hexdigest()

    @property
    def field_names(self) -> tuple[str, ...]:
        decoded = json.loads(self.json_text)
        return tuple(field_value["name"] for field_value in decoded["fields"])


@dataclass(frozen=True, slots=True)
class TableHandle:
    """Stable registered table identity and its canonical local location."""

    table_id: str
    catalog_id: str
    schema_id: str
    catalog_name: str
    schema_name: str
    name: str
    table_format: str
    ownership: TableOwnership
    location: Path

    def __post_init__(self) -> None:
        if self.table_format != "delta":
            raise ValueError(f"unsupported table format: {self.table_format}")
        if not self.table_id or not self.catalog_id or not self.schema_id:
            raise ValueError("table and namespace IDs must be non-empty")
        canonical = self.location.expanduser().resolve(strict=False)
        object.__setattr__(self, "location", canonical)

    @classmethod
    def from_registration(cls, registration: TableRegistration) -> TableHandle:
        return cls(
            table_id=registration.table_id,
            catalog_id=registration.catalog_id,
            schema_id=registration.schema_id,
            catalog_name=registration.catalog_name,
            schema_name=registration.schema_name,
            name=registration.name,
            table_format=registration.table_format,
            ownership=registration.ownership,
            location=registration.canonical_location,
        )

    @property
    def full_name(self) -> str:
        return f"{self.catalog_name}.{self.schema_name}.{self.name}"


@dataclass(frozen=True, slots=True)
class DeltaProtocol:
    """Protocol fields from a Delta snapshot, normalized to immutable tuples."""

    min_reader_version: int
    min_writer_version: int
    reader_features: tuple[str, ...] = ()
    writer_features: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.min_reader_version < 1 or self.min_writer_version < 1:
            raise ValueError("Delta protocol versions must be positive")
        object.__setattr__(self, "reader_features", tuple(sorted(set(self.reader_features))))
        object.__setattr__(self, "writer_features", tuple(sorted(set(self.writer_features))))


@dataclass(frozen=True, slots=True)
class TableSnapshot:
    """A real table identity and the exact schema/protocol at one Delta version."""

    table: TableHandle
    version: int
    schema: SchemaDescriptor
    protocol: DeltaProtocol
    captured_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if self.version < 0:
            raise ValueError("Delta snapshot version must be zero or greater")
        if self.captured_at.tzinfo is None:
            raise ValueError("snapshot capture time must include a timezone")
        object.__setattr__(self, "captured_at", self.captured_at.astimezone(UTC))


@dataclass(frozen=True, slots=True)
class TableReadResult:
    """Arrow data materialized from one exact Delta snapshot."""

    snapshot: TableSnapshot
    # PyArrow is optional at runtime and deliberately remains outside the core package imports.
    data: Any


@dataclass(frozen=True, slots=True)
class CommitResult:
    """One successful provider commit and the exact snapshot it created."""

    snapshot: TableSnapshot
    mode: WriteMode
    operation_id: str
    rows_written: int

    def __post_init__(self) -> None:
        if not self.operation_id:
            raise ValueError("operation_id must be non-empty")
        if self.rows_written < 0:
            raise ValueError("rows_written must be zero or greater")

    @property
    def table(self) -> TableHandle:
        return self.snapshot.table

    @property
    def version(self) -> int:
        return self.snapshot.version

    @property
    def schema(self) -> SchemaDescriptor:
        return self.snapshot.schema


@dataclass(frozen=True, slots=True)
class TableMutationResult:
    """One journaled SQL row mutation and its exact Delta outcome."""

    snapshot: TableSnapshot
    mode: str
    operation_id: str
    rows_affected: int
    commit_version: int | None
    no_op: bool = False

    def __post_init__(self) -> None:
        if self.mode not in {"sql_update", "sql_delete", "sql_merge"}:
            raise ValueError("table mutation mode must be sql_update, sql_delete, or sql_merge")
        if not self.operation_id:
            raise ValueError("operation_id must be non-empty")
        if self.rows_affected < 0:
            raise ValueError("rows_affected must be zero or greater")
        if self.no_op:
            if self.rows_affected != 0 or self.commit_version is not None:
                raise ValueError("a no-op mutation has no affected rows or commit version")
        elif self.commit_version != self.snapshot.version:
            raise ValueError("mutation commit version must match its returned snapshot")

    @property
    def table(self) -> TableHandle:
        return self.snapshot.table

    @property
    def version(self) -> int:
        return self.snapshot.version

    @property
    def schema(self) -> SchemaDescriptor:
        return self.snapshot.schema


@dataclass(frozen=True, slots=True)
class TableHistoryEntry:
    """One committed version reported by the selected Delta provider."""

    version: int
    timestamp: datetime
    operation: str
    operation_parameters: tuple[tuple[str, str], ...] | None
    read_version: int | None
    operation_metrics: tuple[tuple[str, str], ...] | None
    engine_info: str | None

    def __post_init__(self) -> None:
        if self.version < 0:
            raise ValueError("history version must be zero or greater")
        if self.timestamp.tzinfo is None:
            raise ValueError("history timestamp must include a timezone")
        if not self.operation:
            raise ValueError("history operation must be non-empty")
        if self.read_version is not None and self.read_version < 0:
            raise ValueError("history read version must be zero or greater")
        object.__setattr__(self, "timestamp", self.timestamp.astimezone(UTC))
        for field_name in ("operation_parameters", "operation_metrics"):
            pairs = getattr(self, field_name)
            if pairs is not None:
                normalized = tuple(sorted((str(key), str(value)) for key, value in pairs))
                object.__setattr__(self, field_name, normalized)


@dataclass(frozen=True, slots=True)
class StagedInput:
    """Immutable manifest for one copied, workspace-owned input artifact."""

    staging_id: str
    workspace_id: str
    operation_id: str
    path: Path
    data_format: InputFormat
    schema: SchemaDescriptor
    row_count: int
    byte_count: int
    sha256: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class SnapshotSelector:
    """Select a latest snapshot, a numeric version, or a timestamp, never both."""

    version: int | None = None
    timestamp: datetime | None = None

    def __post_init__(self) -> None:
        if self.version is not None and self.timestamp is not None:
            raise SnapshotSelectionError("snapshot version and timestamp are mutually exclusive")
        if self.version is not None and self.version < 0:
            raise SnapshotSelectionError("snapshot version must be zero or greater")
        if self.timestamp is not None:
            if self.timestamp.tzinfo is None:
                raise SnapshotSelectionError("snapshot timestamp must include a timezone")
            object.__setattr__(self, "timestamp", self.timestamp.astimezone(UTC))


@dataclass(frozen=True, slots=True)
class DeltaProviderProfile:
    """The initially selected local Delta protocol and operation envelope."""

    name: str = "delta-rs-1.2.1-local-v1"
    min_reader_version: int = 1
    min_writer_version: int = 2
    supported_reader_features: frozenset[str] = frozenset()
    supported_writer_features: frozenset[str] = frozenset()
    supported_write_modes: frozenset[WriteMode] = frozenset(WriteMode)

    def validate_snapshot(self, snapshot: TableSnapshot) -> None:
        protocol = snapshot.protocol
        if (
            protocol.min_reader_version != self.min_reader_version
            or protocol.min_writer_version != self.min_writer_version
        ):
            raise UnsupportedDeltaProtocolError(
                f"table {snapshot.table.full_name} uses reader/writer protocol "
                f"{protocol.min_reader_version}/{protocol.min_writer_version}; "
                f"profile {self.name} requires "
                f"{self.min_reader_version}/{self.min_writer_version}"
            )
        unsupported_reader = set(protocol.reader_features) - self.supported_reader_features
        unsupported_writer = set(protocol.writer_features) - self.supported_writer_features
        if unsupported_reader or unsupported_writer:
            features = sorted(unsupported_reader | unsupported_writer)
            raise UnsupportedTableFeatureError(
                f"table {snapshot.table.full_name} uses unsupported Delta feature(s): "
                f"{', '.join(features)}"
            )

    def validate_write_mode(self, mode: WriteMode) -> None:
        if mode not in self.supported_write_modes:
            raise UnsupportedWriteModeError(
                f"write mode {mode.value!r} is not supported by profile {self.name}"
            )


DEFAULT_DELTA_PROVIDER_PROFILE = DeltaProviderProfile()


class TableServicePort(Protocol):
    """Table identity, snapshot, staging, and provider mutation operations."""

    def resolve(self, name: ParsedIdentifier, namespace: NamespaceState) -> TableHandle: ...

    def drop_table(self, table: TableHandle) -> None: ...

    def read_snapshot(
        self,
        table: TableHandle,
        selector: SnapshotSelector | None = None,
    ) -> TableSnapshot: ...

    def read_table(
        self,
        table: TableHandle,
        selector: SnapshotSelector | None = None,
    ) -> TableReadResult: ...

    def history(self, table: TableHandle) -> tuple[TableHistoryEntry, ...]: ...

    def stage_input(
        self,
        source: Path,
        schema: SchemaDescriptor,
        data_format: InputFormat,
        *,
        operation_id: str,
        row_count: int,
    ) -> StagedInput: ...

    def validate_write(
        self,
        snapshot: TableSnapshot | None,
        mode: WriteMode,
        input_schema: SchemaDescriptor,
    ) -> None: ...

    def write_staged(
        self,
        table: TableHandle,
        staged: StagedInput,
        mode: WriteMode | str,
    ) -> CommitResult: ...

    def update_rows(
        self,
        table: TableHandle,
        updates: dict[str, str],
        predicate: str,
        *,
        operation_id: str,
        expected_version: int | None = None,
    ) -> TableMutationResult: ...

    def delete_rows(
        self,
        table: TableHandle,
        predicate: str,
        *,
        operation_id: str,
        expected_version: int | None = None,
    ) -> TableMutationResult: ...

    def merge_rows(
        self,
        table: TableHandle,
        source: Any,
        predicate: str,
        *,
        source_alias: str,
        target_alias: str,
        source_key_column: str,
        matched_updates: dict[str, str],
        not_matched_inserts: dict[str, str],
        source_identity: str,
        operation_id: str,
        expected_version: int | None = None,
    ) -> TableMutationResult: ...


def validate_write_request(
    snapshot: TableSnapshot | None,
    mode: WriteMode | str,
    input_schema: SchemaDescriptor,
    *,
    profile: DeltaProviderProfile = DEFAULT_DELTA_PROVIDER_PROFILE,
) -> None:
    """Reject unsupported mode/schema/protocol combinations before provider writes."""
    try:
        selected_mode = mode if isinstance(mode, WriteMode) else WriteMode(mode)
    except ValueError as error:
        raise UnsupportedWriteModeError(f"unsupported table write mode: {mode!r}") from error
    profile.validate_write_mode(selected_mode)
    if selected_mode is WriteMode.CREATE:
        if snapshot is not None:
            raise TableAlreadyExistsError(f"table already exists at {snapshot.table.full_name}")
        return
    if snapshot is None:
        raise TableMissingError(f"write mode {selected_mode.value!r} requires an existing table")
    profile.validate_snapshot(snapshot)
    if input_schema.fingerprint != snapshot.schema.fingerprint:
        raise IncompatibleTableSchemaError(
            f"input schema does not match {snapshot.table.full_name} at Delta version "
            f"{snapshot.version}"
        )
