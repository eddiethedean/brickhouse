"""Local Delta table service backed by the public delta-rs Python API."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from weakref import WeakValueDictionary

from brickhouse.catalog import (
    InvalidTableLocationError,
    ParsedIdentifier,
    TableHasPendingOperationError,
    TableOwnership,
    TableRegistration,
)
from brickhouse.catalog import (
    TableNotFoundError as RegistrationNotFoundError,
)
from brickhouse.tables.contracts import (
    DEFAULT_DELTA_PROVIDER_PROFILE,
    CommitResult,
    DeltaProtocol,
    DeltaProviderProfile,
    InputFormat,
    SchemaDescriptor,
    SnapshotSelectionError,
    SnapshotSelector,
    StagedInput,
    TableHandle,
    TableHistoryEntry,
    TableMissingError,
    TableMutationResult,
    TableReadResult,
    TableServiceError,
    TableSnapshot,
    UnsupportedTableFeatureError,
    UnsupportedWriteModeError,
    WriteMode,
    validate_write_request,
)
from brickhouse.tables.journal import (
    OperationKeyReuseError,
    OperationStateConflictError,
    TableOperationRecord,
    TableOperationState,
)
from brickhouse.tables.staging import StagingArea
from brickhouse.workspace.context import WorkspaceContext

if TYPE_CHECKING:
    from brickhouse.catalog import NamespaceState

_OPERATION_METADATA_KEY = "brickhouse.operation_id"
_FINGERPRINT_METADATA_KEY = "brickhouse.payload_fingerprint"
_MAX_TABLE_HISTORY_ENTRIES = 100
_TABLE_LOCKS: WeakValueDictionary[tuple[str, str], Any] = WeakValueDictionary()
_TABLE_LOCKS_GUARD = threading.Lock()


class DeltaTableProviderError(TableServiceError):
    """The local Delta provider could not complete a supported operation."""


class DeltaProviderUnavailableError(DeltaTableProviderError):
    """The optional delta extra is not installed in the current environment."""


class TableIdentityMismatchError(TableServiceError):
    """A handle no longer matches the table registration in this workspace."""


class TableLocationNotEmptyError(TableServiceError):
    """A new Delta table cannot safely claim a non-empty, non-Delta location."""


class TableInputError(TableServiceError):
    """A staged input's bytes do not match its declared format or manifest."""


class DuplicateMergeSourceKeyError(TableInputError):
    """A selected single-key MERGE source has a null or duplicate key."""


class SnapshotUnavailableError(SnapshotSelectionError):
    """A requested historical Delta version or timestamp is unavailable."""


class StaleTableSnapshotError(SnapshotSelectionError):
    """A write used a descriptor for a Delta version that is no longer current."""

    def __init__(self, expected_version: int, observed_version: int | None) -> None:
        self.expected_version = expected_version
        self.observed_version = observed_version
        super().__init__(
            f"table snapshot changed before write: expected version {expected_version}, "
            f"observed {observed_version}"
        )


@dataclass(frozen=True, slots=True)
class _DeltaMergeSpec:
    source: Any
    source_alias: str
    target_alias: str
    source_key_column: str
    matched_updates: dict[str, str]
    not_matched_inserts: dict[str, str]
    source_identity: str


class UncertainCommitError(DeltaTableProviderError):
    """A provider call did not yield enough history to confirm its commit."""

    def __init__(self, operation_id: str) -> None:
        self.operation_id = operation_id
        super().__init__(
            f"Delta write outcome is uncertain for operation {operation_id!r}; "
            "inspect table history before retrying"
        )


class PendingTableOperationError(DeltaTableProviderError):
    """An unfinished table operation must be reconciled before another action."""

    def __init__(self, operation_id: str) -> None:
        self.operation_id = operation_id
        super().__init__(
            f"table operation {operation_id!r} is unfinished; "
            "reconcile it with the same payload before another table action"
        )


class TableCleanupError(DeltaTableProviderError):
    """Managed table cleanup failed and can be retried while metadata remains registered."""


class DeltaTransactionStateMismatchError(DeltaTableProviderError):
    """The table's persisted Delta application version differs from its SQLite journal."""


class DeltaRsTableService:
    """Create, read, append, overwrite, and mutate local registered Delta tables.

    This service deliberately supports only local filesystem paths and the
    ``DeltaProviderProfile`` protocol envelope. Callers retain staged inputs;
    operation IDs and payload fingerprints persist in the workspace journal.
    """

    def __init__(
        self,
        context: WorkspaceContext,
        *,
        profile: DeltaProviderProfile = DEFAULT_DELTA_PROVIDER_PROFILE,
    ) -> None:
        # Accessing workspace validates that this context currently owns a workspace.
        _ = context.workspace
        self._context = context
        self._profile = profile
        self._staging = StagingArea(context)
        self._journal = context.table_operations
        self._delta, self._arrow, self._parquet, self._ipc = _load_dependencies()

    def resolve(self, name: ParsedIdentifier, namespace: NamespaceState) -> TableHandle:
        """Resolve a parsed one-, two-, or three-part table identifier."""
        quoted = [
            f"`{part.value.replace('`', '``')}`" if part.quoted else part.value
            for part in name.parts
        ]
        registration = namespace.resolve_table(".".join(quoted))
        handle = TableHandle.from_registration(registration)
        self._current_registration(handle)
        return handle

    def drop_table(self, table: TableHandle) -> None:
        """Drop a registration, deleting bytes only for a safe workspace-managed table.

        Managed cleanup happens before SQLite deregistration. If the process stops
        during or after recursive removal, the registration remains available so a
        caller can retry cleanup; a missing managed directory counts as already removed.
        """
        with _table_mutation_lock(table):
            registration = self._current_registration(table)
            unfinished = self._journal.latest_unfinished(table.table_id)
            if unfinished is not None:
                raise PendingTableOperationError(unfinished.operation_id)
            if registration.ownership is TableOwnership.MANAGED:
                self._remove_managed_table_data(registration)
            try:
                self._context.tables._remove_registration(table.table_id)
            except TableHasPendingOperationError as error:
                raise PendingTableOperationError(error.operation_id) from error

    def stage_input(
        self,
        source: Path,
        schema: SchemaDescriptor,
        data_format: InputFormat,
        *,
        operation_id: str,
        row_count: int,
    ) -> StagedInput:
        """Copy one local Parquet or Arrow IPC file into workspace staging."""
        return self._staging.stage_file(
            source,
            schema,
            data_format,
            operation_id=operation_id,
            row_count=row_count,
        )

    def read_snapshot(
        self,
        table: TableHandle,
        selector: SnapshotSelector | None = None,
    ) -> TableSnapshot:
        """Read schema, protocol, identity, and version without scanning rows."""
        provider_table = self._open_snapshot(table, selector)
        return self._capture_snapshot(table, provider_table)

    def read_arrow_schema(self, table: TableHandle) -> tuple[TableSnapshot, Any]:
        """Read a validated snapshot and its Arrow schema without scanning table rows."""
        provider_table = self._open_snapshot(table, None)
        snapshot = self._capture_snapshot(table, provider_table)
        try:
            arrow_schema = self._arrow.schema(provider_table.schema().to_arrow())
        except Exception as error:
            raise DeltaTableProviderError(
                f"could not read the Arrow schema for {table.full_name}"
            ) from error
        if self._schema_from_arrow(arrow_schema).fingerprint != snapshot.schema.fingerprint:
            raise TableInputError(
                f"provider Arrow schema did not match the Delta snapshot for {table.full_name}"
            )
        return snapshot, arrow_schema

    def read_table(
        self,
        table: TableHandle,
        selector: SnapshotSelector | None = None,
    ) -> TableReadResult:
        """Materialize rows from one selected, protocol-validated Delta snapshot."""
        provider_table = self._open_snapshot(table, selector)
        snapshot = self._capture_snapshot(table, provider_table)
        try:
            arrow_table = provider_table.to_pyarrow_table()
        except Exception as error:
            if selector is not None and (
                selector.version is not None or selector.timestamp is not None
            ):
                raise SnapshotUnavailableError(
                    f"requested Delta snapshot rows are unavailable for {table.full_name}"
                ) from error
            raise DeltaTableProviderError(
                f"could not read Delta rows from {table.full_name}"
            ) from error
        actual_schema = self._schema_from_arrow(arrow_table.schema)
        if actual_schema.fingerprint != snapshot.schema.fingerprint:
            raise TableInputError(
                f"provider rows did not match the schema at Delta version {snapshot.version}"
            )
        return TableReadResult(snapshot=snapshot, data=arrow_table)

    def history(self, table: TableHandle) -> tuple[TableHistoryEntry, ...]:
        """Return a bounded newest-first view of committed provider history."""
        provider_table = self._open_snapshot(table, None)
        self._capture_snapshot(table, provider_table)
        try:
            records = provider_table.history(limit=_MAX_TABLE_HISTORY_ENTRIES)
        except Exception as error:
            raise DeltaTableProviderError(
                f"could not read committed Delta history for {table.full_name}"
            ) from error
        if not isinstance(records, list):
            raise DeltaTableProviderError("Delta provider returned malformed history")

        entries: list[TableHistoryEntry] = []
        previous_version: int | None = None
        for record in records:
            if not isinstance(record, dict):
                raise DeltaTableProviderError("Delta provider returned a malformed history row")
            try:
                version_value = record["version"]
                timestamp_value = record["timestamp"]
                operation_value = record["operation"]
                if (
                    isinstance(version_value, bool)
                    or not isinstance(version_value, int)
                    or isinstance(timestamp_value, bool)
                    or not isinstance(timestamp_value, int)
                    or not isinstance(operation_value, str)
                ):
                    raise TypeError("history row has invalid version, timestamp, or operation")
                version = version_value
                if previous_version is not None and version >= previous_version:
                    raise ValueError("provider history is not in strictly descending version order")
                previous_version = version
                read_version = record.get("readVersion")
                if read_version is not None and (
                    isinstance(read_version, bool) or not isinstance(read_version, int)
                ):
                    raise TypeError("history row has an invalid read version")
                entries.append(
                    TableHistoryEntry(
                        version=version,
                        timestamp=datetime.fromtimestamp(timestamp_value / 1000, UTC),
                        operation=operation_value,
                        operation_parameters=_history_string_pairs(
                            record.get("operationParameters")
                        ),
                        read_version=read_version,
                        operation_metrics=_history_string_pairs(record.get("operationMetrics")),
                        engine_info=_history_optional_string(record.get("engineInfo")),
                    )
                )
            except (KeyError, OverflowError, OSError, TypeError, ValueError) as error:
                raise DeltaTableProviderError(
                    "Delta provider returned an invalid committed history row"
                ) from error
        return tuple(entries)

    def validate_write(
        self,
        snapshot: TableSnapshot | None,
        mode: WriteMode,
        input_schema: SchemaDescriptor,
    ) -> None:
        """Apply the selected protocol, write-mode, target, and schema checks."""
        validate_write_request(snapshot, mode, input_schema, profile=self._profile)

    def write_staged(
        self,
        table: TableHandle,
        staged: StagedInput,
        mode: WriteMode | str,
    ) -> CommitResult:
        """Validate a workspace-owned staged file and commit it through delta-rs."""
        with self._staging.protect(staged):
            staged_path = self._staging.verify_content(staged)
            try:
                arrow_table = self._read_staged_arrow(staged_path, staged.data_format)
            except Exception as error:
                if isinstance(error, TableInputError):
                    raise
                raise TableInputError(
                    "staged input is not a readable declared Arrow format"
                ) from error

            actual_schema = self._schema_from_arrow(arrow_table.schema)
            if actual_schema.fingerprint != staged.schema.fingerprint:
                raise TableInputError("staged input schema does not match its staging manifest")
            if arrow_table.num_rows != staged.row_count:
                raise TableInputError("staged input row count does not match its staging manifest")
            return self._write_arrow(
                table,
                arrow_table,
                mode,
                operation_id=staged.operation_id,
                input_sha256=staged.sha256,
                input_kind=staged.data_format.value,
                input_schema=actual_schema,
            )

    def write_arrow(
        self,
        table: TableHandle,
        data: Any,
        mode: WriteMode | str,
        *,
        operation_id: str,
        input_schema: SchemaDescriptor | None = None,
        expected_version: int | None = None,
    ) -> CommitResult:
        """Journal, commit, and return the exact version for an in-memory Arrow table.

        When ``expected_version`` is supplied, compare it with the current Delta
        snapshot under the table mutation lock before preparing the operation.
        """
        if not isinstance(data, self._arrow.Table):
            raise TypeError("data must be a pyarrow.Table")
        if expected_version is not None and (
            isinstance(expected_version, bool)
            or not isinstance(expected_version, int)
            or expected_version < 0
        ):
            raise ValueError("expected_version must be an integer zero or greater")
        schema = input_schema or self._schema_from_arrow(data.schema)
        actual_schema = self._schema_from_arrow(data.schema)
        if actual_schema.fingerprint != schema.fingerprint:
            raise TableInputError("provided input schema does not match the Arrow table")
        return self._write_arrow(
            table,
            data,
            mode,
            operation_id=operation_id,
            input_sha256=self._arrow_sha256(data),
            input_kind="pyarrow-table",
            input_schema=schema,
            expected_version=expected_version,
        )

    def update_rows(
        self,
        table: TableHandle,
        updates: dict[str, str],
        predicate: str,
        *,
        operation_id: str,
        expected_version: int | None = None,
    ) -> TableMutationResult:
        """Apply one native, journaled delta-rs UPDATE to a registered table."""
        if not isinstance(updates, dict) or not updates:
            raise TableInputError("SQL UPDATE requires at least one column assignment")
        if any(not isinstance(name, str) or not name for name in updates):
            raise TableInputError("SQL UPDATE assignment names must be non-empty strings")
        if any(not isinstance(value, str) or not value.strip() for value in updates.values()):
            raise TableInputError("SQL UPDATE values must be non-empty provider expressions")
        return self._mutate_rows(
            table,
            mode="sql_update",
            predicate=predicate,
            updates=updates,
            operation_id=operation_id,
            expected_version=expected_version,
        )

    def delete_rows(
        self,
        table: TableHandle,
        predicate: str,
        *,
        operation_id: str,
        expected_version: int | None = None,
    ) -> TableMutationResult:
        """Apply one native, journaled delta-rs DELETE to a registered table."""
        return self._mutate_rows(
            table,
            mode="sql_delete",
            predicate=predicate,
            updates=None,
            operation_id=operation_id,
            expected_version=expected_version,
        )

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
    ) -> TableMutationResult:
        """Apply one native, journaled single-key Delta MERGE to a registered table."""
        if not isinstance(source, self._arrow.Table):
            raise TypeError("MERGE source must be a pyarrow.Table")
        if not isinstance(source_key_column, str) or source_key_column not in source.column_names:
            raise TableInputError("MERGE source key must name one source column")
        if not isinstance(source_identity, str) or not source_identity.strip():
            raise TableInputError("MERGE source identity must be non-empty")
        if (
            any(
                not isinstance(alias, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", alias) is None
                for alias in (source_alias, target_alias)
            )
            or source_alias.casefold() == target_alias.casefold()
        ):
            raise TableInputError("MERGE aliases must be distinct simple identifiers")
        if not isinstance(matched_updates, dict) or not matched_updates:
            raise TableInputError("MERGE requires at least one matched UPDATE assignment")
        if not isinstance(not_matched_inserts, dict) or not not_matched_inserts:
            raise TableInputError("MERGE requires explicit unmatched INSERT assignments")
        for assignments in (matched_updates, not_matched_inserts):
            if any(not isinstance(name, str) or not name for name in assignments):
                raise TableInputError("MERGE assignment names must be non-empty strings")
            if any(
                not isinstance(value, str) or not value.strip() for value in assignments.values()
            ):
                raise TableInputError("MERGE values must be non-empty provider expressions")
        if not isinstance(predicate, str) or not predicate.strip():
            raise TableInputError("MERGE requires a non-empty key predicate")
        _validate_operation_id(operation_id)
        _validate_unique_merge_source_key(source, source_key_column)
        merge = _DeltaMergeSpec(
            source=source,
            source_alias=source_alias,
            target_alias=target_alias,
            source_key_column=source_key_column,
            matched_updates=dict(matched_updates),
            not_matched_inserts=dict(not_matched_inserts),
            source_identity=source_identity,
        )
        return self._mutate_rows(
            table,
            mode="sql_merge",
            predicate=predicate,
            updates=None,
            merge=merge,
            operation_id=operation_id,
            expected_version=expected_version,
        )

    def _mutate_rows(
        self,
        table: TableHandle,
        *,
        mode: str,
        predicate: str,
        updates: dict[str, str] | None,
        operation_id: str,
        expected_version: int | None,
        merge: _DeltaMergeSpec | None = None,
    ) -> TableMutationResult:
        if mode not in {"sql_update", "sql_delete", "sql_merge"}:
            raise ValueError("unsupported SQL mutation mode")
        _validate_operation_id(operation_id)
        if expected_version is not None and (
            isinstance(expected_version, bool)
            or not isinstance(expected_version, int)
            or expected_version < 0
        ):
            raise ValueError("expected_version must be an integer zero or greater")
        if not isinstance(predicate, str) or not predicate.strip():
            raise TableInputError("SQL row mutations require a non-empty predicate")
        if mode == "sql_update" and (not updates or merge is not None):
            raise ValueError("SQL UPDATE assignments do not match the operation mode")
        if mode == "sql_delete" and (updates is not None or merge is not None):
            raise ValueError("SQL DELETE arguments do not match the operation mode")
        if mode == "sql_merge" and (updates is not None or merge is None):
            raise ValueError("SQL MERGE arguments do not match the operation mode")

        merge_input: dict[str, Any] | None = None
        if merge is not None:
            merge_input = {
                "source_identity": merge.source_identity,
                "source_sha256": self._arrow_sha256(merge.source),
                "source_schema": self._schema_from_arrow(merge.source.schema).fingerprint,
                "source_key_column": merge.source_key_column,
                "source_alias": merge.source_alias,
                "target_alias": merge.target_alias,
                "matched_updates": merge.matched_updates,
                "not_matched_inserts": merge.not_matched_inserts,
            }
        canonical_input = json.dumps(
            {
                "mode": mode,
                "predicate": predicate,
                "updates": updates,
                "merge": merge_input,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        input_sha256 = hashlib.sha256(canonical_input.encode("utf-8")).hexdigest()

        with _table_mutation_lock(table):
            registration = self._current_registration(table)
            location = registration.canonical_location
            existing = self._journal.get(table.table_id, operation_id)
            if existing is not None:
                fingerprint_matches = existing.fingerprint == _mutation_fingerprint(
                    table, mode, input_sha256, self._profile
                )
                committed_payload_matches = (
                    existing.state in {TableOperationState.COMMITTED, TableOperationState.NO_OP}
                    and existing.mode == mode
                    and existing.input_sha256 == input_sha256
                )
                if not fingerprint_matches and not committed_payload_matches:
                    raise OperationKeyReuseError(
                        f"operation ID {operation_id!r} was reused with a different mutation"
                    )
                if existing.mode != mode:
                    raise OperationKeyReuseError(
                        f"operation ID {operation_id!r} was reused with a different mode"
                    )
                if existing.state in (
                    TableOperationState.COMMITTED,
                    TableOperationState.NO_OP,
                ):
                    return cast(TableMutationResult, self._result_from_record(table, existing))
                if existing.state is TableOperationState.FAILED:
                    raise DeltaTableProviderError(
                        f"SQL table operation {operation_id!r} previously failed before commit"
                    )
                if existing.state in (
                    TableOperationState.COMMITTING,
                    TableOperationState.UNCERTAIN,
                ):
                    recovered = self._reconcile_operation(table, existing)
                    if isinstance(recovered, TableMutationResult):
                        return recovered
                    self._mark_uncertain(existing)
                    raise UncertainCommitError(operation_id)
                record = existing
                previous = self._try_open_latest(table)
                snapshot = None if previous is None else self._capture_snapshot(table, previous)
                self._validate_prepared_mutation(table, record, snapshot)
            else:
                unfinished = self._journal.latest_unfinished(table.table_id)
                if unfinished is not None:
                    if unfinished.state in (
                        TableOperationState.COMMITTING,
                        TableOperationState.UNCERTAIN,
                    ):
                        recovered = self._reconcile_operation(table, unfinished)
                        if recovered is None:
                            self._mark_uncertain(unfinished)
                            raise UncertainCommitError(unfinished.operation_id)
                    else:
                        raise PendingTableOperationError(unfinished.operation_id)

                previous = self._try_open_latest(table)
                if previous is None:
                    raise TableMissingError(f"Delta table does not exist: {table.full_name}")
                snapshot = self._capture_snapshot(table, previous)
                if expected_version is not None and snapshot.version != expected_version:
                    raise StaleTableSnapshotError(expected_version, snapshot.version)
                provider_transaction = self._provider_transaction_version(previous, table.table_id)
                latest_transaction = self._journal.latest_transaction_version(table.table_id)
                if provider_transaction != latest_transaction:
                    raise DeltaTransactionStateMismatchError(
                        f"Delta transaction version {provider_transaction!r} disagrees with "
                        f"journal version {latest_transaction!r} for {table.full_name}"
                    )
                record, created = self._journal.prepare(
                    table_id=table.table_id,
                    operation_id=operation_id,
                    fingerprint=_mutation_fingerprint(table, mode, input_sha256, self._profile),
                    mode=mode,
                    schema_fingerprint=snapshot.schema.fingerprint,
                    input_sha256=input_sha256,
                    base_version=snapshot.version,
                    row_count=0,
                )
                if not created:
                    raise OperationStateConflictError(
                        f"operation {operation_id!r} appeared concurrently in the journal"
                    )

            assert snapshot is not None
            self._journal.mark_committing(table.table_id, operation_id)
            commit_properties = self._delta.CommitProperties(
                custom_metadata={
                    _OPERATION_METADATA_KEY: operation_id,
                    _FINGERPRINT_METADATA_KEY: record.fingerprint,
                },
                max_commit_retries=0,
                app_transactions=[
                    self._delta.Transaction(
                        app_id=table.table_id,
                        version=_required_transaction_version(record),
                    )
                ],
            )
            try:
                provider_table = self._delta.DeltaTable(str(location))
                if mode == "sql_update":
                    metrics = provider_table.update(
                        updates=updates,
                        predicate=predicate,
                        error_on_type_mismatch=True,
                        commit_properties=commit_properties,
                    )
                elif mode == "sql_delete":
                    metrics = provider_table.delete(
                        predicate=predicate,
                        commit_properties=commit_properties,
                    )
                else:
                    assert merge is not None
                    metrics = (
                        provider_table.merge(
                            source=merge.source,
                            predicate=predicate,
                            source_alias=merge.source_alias,
                            target_alias=merge.target_alias,
                            error_on_type_mismatch=True,
                            commit_properties=commit_properties,
                        )
                        .when_matched_update(updates=merge.matched_updates)
                        .when_not_matched_insert(updates=merge.not_matched_inserts)
                        .execute()
                    )
            except Exception as error:
                recovered = self._reconcile_operation(table, record)
                if isinstance(recovered, TableMutationResult):
                    return recovered
                if self._operation_state_unchanged(table, record):
                    self._journal.finalize_failed(
                        table.table_id,
                        operation_id,
                        error_code="provider_rejected",
                    )
                    raise DeltaTableProviderError(
                        f"Delta provider rejected SQL {mode.removeprefix('sql_').upper()}"
                    ) from error
                self._mark_uncertain(record)
                raise UncertainCommitError(operation_id) from error

            affected_hint = _mutation_affected_rows(mode, metrics)

            if affected_hint == 0 and self._operation_state_unchanged(table, record):
                no_op = self._journal.finalize_no_op(table.table_id, operation_id)
                return cast(TableMutationResult, self._result_from_record(table, no_op))

            recovered = self._reconcile_operation(
                table,
                record,
                rows_affected_hint=affected_hint,
            )
            if isinstance(recovered, TableMutationResult):
                return recovered
            self._mark_uncertain(record)
            raise UncertainCommitError(operation_id)

    def _validate_prepared_mutation(
        self,
        table: TableHandle,
        record: TableOperationRecord,
        snapshot: TableSnapshot | None,
    ) -> None:
        transaction_version = record.transaction_version
        if transaction_version is None:
            raise OperationStateConflictError("prepared table mutation has no transaction version")
        observed_version = -1 if snapshot is None else snapshot.version
        provider_table = None if snapshot is None else self._delta.DeltaTable(str(table.location))
        observed_transaction = self._provider_transaction_version(provider_table, table.table_id)
        expected_transaction = transaction_version - 1
        expected_previous = None if expected_transaction < 0 else expected_transaction
        latest_transaction = self._journal.latest_transaction_version(table.table_id)
        if (
            observed_version != record.base_version
            or observed_transaction != expected_previous
            or latest_transaction != transaction_version
            or snapshot is None
            or snapshot.schema.fingerprint != record.schema_fingerprint
        ):
            raise DeltaTransactionStateMismatchError(
                f"prepared SQL mutation {record.operation_id!r} no longer matches the table and "
                "journal transaction state"
            )

    def _write_arrow(
        self,
        table: TableHandle,
        data: Any,
        mode: WriteMode | str,
        *,
        operation_id: str,
        input_sha256: str,
        input_kind: str,
        input_schema: SchemaDescriptor,
        expected_version: int | None = None,
    ) -> CommitResult:
        selected_mode = self._normalize_mode(mode)
        _validate_operation_id(operation_id)
        if not isinstance(data, self._arrow.Table):
            raise TypeError("data must be a pyarrow.Table")
        actual_schema = self._schema_from_arrow(data.schema)
        if actual_schema.fingerprint != input_schema.fingerprint:
            raise TableInputError("provided input schema does not match the Arrow table")
        fingerprint = _payload_fingerprint(
            table=table,
            mode=selected_mode,
            schema=input_schema,
            input_sha256=input_sha256,
            input_kind=input_kind,
            row_count=data.num_rows,
            profile=self._profile,
        )

        with _table_mutation_lock(table):
            registration = self._current_registration(table)
            location = registration.canonical_location
            existing = self._journal.get(table.table_id, operation_id)
            if existing is not None:
                fingerprint_matches = existing.fingerprint == fingerprint
                committed_payload_matches = (
                    existing.state is TableOperationState.COMMITTED
                    and existing.mode == selected_mode.value
                    and existing.schema_fingerprint == input_schema.fingerprint
                    and existing.input_sha256 == input_sha256
                    and existing.row_count == data.num_rows
                )
                if not fingerprint_matches and not committed_payload_matches:
                    raise OperationKeyReuseError(
                        f"operation ID {operation_id!r} was reused with a different payload"
                    )
                if existing.state is TableOperationState.COMMITTED:
                    return _require_commit_result(self._result_from_record(table, existing))
                if existing.state in (
                    TableOperationState.COMMITTING,
                    TableOperationState.UNCERTAIN,
                ):
                    recovered = self._reconcile_operation(table, existing)
                    if recovered is not None:
                        return _require_commit_result(recovered)
                    self._mark_uncertain(existing)
                    raise UncertainCommitError(operation_id)
                record = existing
                previous = self._try_open_latest(table)
                self._validate_prepared_operation(
                    table,
                    record,
                    previous,
                    selected_mode,
                    input_schema,
                )
                if expected_version is not None and record.base_version != expected_version:
                    raise StaleTableSnapshotError(expected_version, record.base_version)
            else:
                unfinished = self._journal.latest_unfinished(table.table_id)
                if unfinished is not None:
                    if unfinished.state in (
                        TableOperationState.COMMITTING,
                        TableOperationState.UNCERTAIN,
                    ):
                        recovered = self._reconcile_operation(table, unfinished)
                        if recovered is None:
                            self._mark_uncertain(unfinished)
                            raise UncertainCommitError(unfinished.operation_id)
                    else:
                        raise PendingTableOperationError(unfinished.operation_id)

                previous = self._try_open_latest(table)
                previous_snapshot = (
                    None if previous is None else self._capture_snapshot(table, previous)
                )
                observed_version = None if previous_snapshot is None else previous_snapshot.version
                if expected_version is not None and observed_version != expected_version:
                    raise StaleTableSnapshotError(expected_version, observed_version)
                validate_write_request(
                    previous_snapshot,
                    selected_mode,
                    input_schema,
                    profile=self._profile,
                )
                if previous is None and selected_mode is WriteMode.CREATE:
                    self._ensure_create_location_safe(location)
                base_version = previous.version() if previous is not None else -1
                observed_transaction = self._provider_transaction_version(previous, table.table_id)
                latest_transaction = self._journal.latest_transaction_version(table.table_id)
                if observed_transaction != latest_transaction:
                    raise DeltaTransactionStateMismatchError(
                        f"Delta transaction version {observed_transaction!r} disagrees with "
                        f"journal version {latest_transaction!r} for {table.full_name}"
                    )
                record, created = self._journal.prepare(
                    table_id=table.table_id,
                    operation_id=operation_id,
                    fingerprint=fingerprint,
                    mode=selected_mode.value,
                    schema_fingerprint=input_schema.fingerprint,
                    input_sha256=input_sha256,
                    base_version=base_version,
                    row_count=data.num_rows,
                )
                if not created:
                    raise OperationStateConflictError(
                        f"operation {operation_id!r} appeared concurrently in the journal"
                    )

            self._journal.mark_committing(table.table_id, operation_id)
            provider_mode = {
                WriteMode.CREATE: "error",
                WriteMode.APPEND: "append",
                WriteMode.OVERWRITE: "overwrite",
            }[selected_mode]
            commit_properties = self._delta.CommitProperties(
                custom_metadata={
                    _OPERATION_METADATA_KEY: operation_id,
                    _FINGERPRINT_METADATA_KEY: fingerprint,
                },
                max_commit_retries=0,
                app_transactions=[
                    self._delta.Transaction(
                        app_id=table.table_id,
                        version=record.transaction_version,
                    )
                ],
            )
            try:
                self._delta.write_deltalake(
                    str(location),
                    data,
                    mode=provider_mode,
                    commit_properties=commit_properties,
                )
            except Exception as error:
                recovered = self._reconcile_operation(table, record)
                if recovered is not None:
                    return _require_commit_result(recovered)
                self._mark_uncertain(record)
                raise UncertainCommitError(operation_id) from error

            recovered = self._reconcile_operation(table, record)
            if recovered is not None:
                return _require_commit_result(recovered)
            self._mark_uncertain(record)
            raise UncertainCommitError(operation_id)

    def _validate_prepared_operation(
        self,
        table: TableHandle,
        record: TableOperationRecord,
        previous: Any | None,
        mode: WriteMode,
        schema: SchemaDescriptor,
    ) -> None:
        previous_version = previous.version() if previous is not None else -1
        transaction_version = _required_transaction_version(record)
        expected_transaction = transaction_version - 1
        observed_transaction = self._provider_transaction_version(previous, table.table_id)
        latest_transaction = self._journal.latest_transaction_version(table.table_id)
        expected_previous_transaction = None if expected_transaction < 0 else expected_transaction
        if (
            previous_version != record.base_version
            or observed_transaction != expected_previous_transaction
            or latest_transaction != transaction_version
        ):
            raise DeltaTransactionStateMismatchError(
                f"prepared operation {record.operation_id!r} no longer matches the table and "
                "journal transaction state"
            )
        snapshot = None if previous is None else self._capture_snapshot(table, previous)
        validate_write_request(snapshot, mode, schema, profile=self._profile)
        if previous is None and mode is WriteMode.CREATE:
            self._ensure_create_location_safe(table.location)

    def _provider_transaction_version(
        self,
        provider_table: Any | None,
        table_id: str,
    ) -> int | None:
        if provider_table is None:
            return None
        try:
            return cast(int | None, provider_table.transaction_version(table_id))
        except Exception as error:
            raise DeltaTableProviderError(
                "could not inspect Delta application transaction"
            ) from error

    def _reconcile_operation(
        self,
        table: TableHandle,
        record: TableOperationRecord,
        *,
        rows_affected_hint: int | None = None,
    ) -> CommitResult | TableMutationResult | None:
        try:
            transaction_version = record.transaction_version
            if transaction_version is None:
                return None
            registration = self._current_registration(table)
            location = registration.canonical_location
            if not self._is_delta_table(location):
                return None
            provider_table = self._delta.DeltaTable(str(location))
            observed_transaction = provider_table.transaction_version(table.table_id)
            if observed_transaction != transaction_version:
                return None
            commit_entry = self._find_commit_entry(location, record)
            if commit_entry is None:
                return None
            commit_version = int(commit_entry["version"])
            if commit_version <= record.base_version:
                return None
            committed_table = self._open_at_version(location, commit_version)
            snapshot = self._capture_snapshot(table, committed_table)
            if snapshot.schema.fingerprint != record.schema_fingerprint:
                return None
            rows_affected = rows_affected_hint
            if record.mode in {"sql_update", "sql_delete", "sql_merge"}:
                metrics = commit_entry.get("operationMetrics")
                if record.mode == "sql_merge":
                    history_rows_affected = _merge_history_affected_rows(metrics)
                else:
                    metric_name = (
                        "num_updated_rows" if record.mode == "sql_update" else "num_deleted_rows"
                    )
                    metric_value = metrics.get(metric_name) if isinstance(metrics, dict) else None
                    history_rows_affected = _nonnegative_int(metric_value)
                if history_rows_affected is None or (
                    rows_affected is not None and rows_affected != history_rows_affected
                ):
                    return None
                rows_affected = history_rows_affected
            finalized = self._journal.finalize_committed(
                table.table_id,
                record.operation_id,
                commit_version,
                row_count=rows_affected,
            )
            return self._result_from_record(table, finalized, snapshot=snapshot)
        except Exception:
            # An incomplete provider/journal read is not evidence of non-commit.
            return None

    def _operation_state_unchanged(
        self,
        table: TableHandle,
        record: TableOperationRecord,
    ) -> bool:
        """Return true only when both Delta snapshot and transaction are still at base."""
        try:
            latest = self._try_open_latest(table)
            if latest is None or latest.version() != record.base_version:
                return False
            expected_transaction = (
                None
                if record.transaction_version is None or record.transaction_version == 0
                else record.transaction_version - 1
            )
            return self._provider_transaction_version(latest, table.table_id) == (
                expected_transaction
            )
        except Exception:
            return False

    def _find_commit_version(
        self,
        location: Path,
        record: TableOperationRecord,
    ) -> int | None:
        entry = self._find_commit_entry(location, record)
        return None if entry is None else int(entry["version"])

    def _find_commit_entry(
        self,
        location: Path,
        record: TableOperationRecord,
    ) -> dict[str, Any] | None:
        try:
            history = self._delta.DeltaTable(str(location)).history(limit=100)
        except Exception:
            return None
        entries = [
            entry
            for entry in history
            if entry.get(_OPERATION_METADATA_KEY) == record.operation_id
            and entry.get(_FINGERPRINT_METADATA_KEY) == record.fingerprint
            and int(entry.get("version", -1)) > record.base_version
        ]
        return entries[0] if len(entries) == 1 else None

    def _result_from_record(
        self,
        table: TableHandle,
        record: TableOperationRecord,
        *,
        snapshot: TableSnapshot | None = None,
    ) -> CommitResult | TableMutationResult:
        if record.mode in {"sql_update", "sql_delete", "sql_merge"}:
            return self._mutation_result_from_record(table, record, snapshot=snapshot)
        if record.state in {TableOperationState.NO_OP, TableOperationState.FAILED}:
            raise OperationStateConflictError(
                f"write operation {record.operation_id!r} has terminal state {record.state.value}"
            )
        if record.state is not TableOperationState.COMMITTED or record.commit_version is None:
            raise OperationStateConflictError(
                f"operation {record.operation_id!r} has no confirmed commit version"
            )
        if snapshot is None:
            provider_table = self._open_at_version(table.location, record.commit_version)
            snapshot = self._capture_snapshot(table, provider_table)
        if snapshot.version != record.commit_version:
            raise OperationStateConflictError("journal commit version differs from Delta snapshot")
        if snapshot.schema.fingerprint != record.schema_fingerprint:
            raise OperationStateConflictError("journal schema differs from Delta commit schema")
        return CommitResult(
            snapshot=snapshot,
            mode=WriteMode(record.mode),
            operation_id=record.operation_id,
            rows_written=record.row_count,
        )

    def _mutation_result_from_record(
        self,
        table: TableHandle,
        record: TableOperationRecord,
        *,
        snapshot: TableSnapshot | None = None,
    ) -> TableMutationResult:
        if record.state is TableOperationState.FAILED:
            raise DeltaTableProviderError(
                f"SQL table operation {record.operation_id!r} previously failed before commit"
            )
        if record.state is TableOperationState.NO_OP:
            if record.base_version < 0 or record.transaction_version is not None:
                raise OperationStateConflictError(
                    f"no-op operation {record.operation_id!r} has an invalid journal snapshot"
                )
            if snapshot is None:
                provider_table = self._open_at_version(table.location, record.base_version)
                snapshot = self._capture_snapshot(table, provider_table)
            if (
                snapshot.version != record.base_version
                or snapshot.schema.fingerprint != record.schema_fingerprint
            ):
                raise OperationStateConflictError(
                    "journal no-op snapshot differs from the original Delta snapshot"
                )
            return TableMutationResult(
                snapshot=snapshot,
                mode=record.mode,
                operation_id=record.operation_id,
                rows_affected=0,
                commit_version=None,
                no_op=True,
            )
        if record.state is not TableOperationState.COMMITTED or record.commit_version is None:
            raise OperationStateConflictError(
                f"operation {record.operation_id!r} has no confirmed committed mutation"
            )
        if snapshot is None:
            provider_table = self._open_at_version(table.location, record.commit_version)
            snapshot = self._capture_snapshot(table, provider_table)
        if snapshot.version != record.commit_version:
            raise OperationStateConflictError("journal commit version differs from Delta snapshot")
        if snapshot.schema.fingerprint != record.schema_fingerprint:
            raise OperationStateConflictError("journal schema differs from Delta commit schema")
        return TableMutationResult(
            snapshot=snapshot,
            mode=record.mode,
            operation_id=record.operation_id,
            rows_affected=record.row_count,
            commit_version=record.commit_version,
        )

    def _mark_uncertain(self, record: TableOperationRecord) -> None:
        try:
            self._journal.mark_uncertain(record.table_id, record.operation_id)
        except Exception:
            # A persisted COMMITTING state is already sufficient to block dispatch.
            return

    def _arrow_sha256(self, data: Any) -> str:
        sink = self._arrow.BufferOutputStream()
        with self._ipc.new_stream(sink, data.schema) as writer:
            writer.write_table(data)
        return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()

    def _open_snapshot(
        self,
        table: TableHandle,
        selector: SnapshotSelector | None,
    ) -> Any:
        registration = self._current_registration(table)
        location = registration.canonical_location
        if not self._is_delta_table(location):
            raise TableMissingError(f"Delta table does not exist: {table.full_name}")
        try:
            provider_table = self._delta.DeltaTable(str(location))
            if selector is not None:
                if selector.version is not None:
                    provider_table.load_as_version(selector.version)
                elif selector.timestamp is not None:
                    provider_table.load_as_version(selector.timestamp)
        except Exception as error:
            if selector is not None:
                raise SnapshotUnavailableError(
                    f"requested Delta snapshot is unavailable for {table.full_name}"
                ) from error
            raise DeltaTableProviderError(
                f"could not open Delta snapshot for {table.full_name}"
            ) from error
        return provider_table

    def _try_open_latest(self, table: TableHandle) -> Any | None:
        registration = self._current_registration(table)
        if not self._is_delta_table(registration.canonical_location):
            return None
        try:
            provider_table = self._delta.DeltaTable(str(registration.canonical_location))
        except Exception as error:
            raise DeltaTableProviderError(
                f"could not open registered Delta table {table.full_name}"
            ) from error
        self._capture_snapshot(table, provider_table)
        return provider_table

    def _capture_snapshot(self, table: TableHandle, provider_table: Any) -> TableSnapshot:
        try:
            provider_schema = provider_table.schema().to_json()
            provider_protocol = provider_table.protocol()
            partition_columns = provider_table.metadata().partition_columns
            protocol = DeltaProtocol(
                min_reader_version=provider_protocol.min_reader_version,
                min_writer_version=provider_protocol.min_writer_version,
                reader_features=tuple(provider_protocol.reader_features or ()),
                writer_features=tuple(provider_protocol.writer_features or ()),
            )
            snapshot = TableSnapshot(
                table=table,
                version=provider_table.version(),
                schema=SchemaDescriptor.from_delta_json(provider_schema),
                protocol=protocol,
            )
        except Exception as error:
            raise DeltaTableProviderError(
                f"could not inspect Delta snapshot for {table.full_name}"
            ) from error
        if partition_columns:
            raise UnsupportedTableFeatureError(
                f"partitioned Delta table {table.full_name} is outside the initial local profile"
            )
        self._profile.validate_snapshot(snapshot)
        return snapshot

    def _current_registration(self, table: TableHandle) -> TableRegistration:
        try:
            registration = self._context.tables.get_by_id(table.table_id)
        except RegistrationNotFoundError as error:
            raise TableMissingError(
                f"table registration does not exist: {table.table_id}"
            ) from error
        current_handle = TableHandle.from_registration(registration)
        if current_handle != table:
            raise TableIdentityMismatchError(
                f"table handle no longer matches registration {registration.full_name}"
            )
        try:
            current_location = registration.canonical_location.resolve(strict=False)
        except (OSError, RuntimeError) as error:
            raise InvalidTableLocationError(
                "registered table location cannot be resolved"
            ) from error
        if current_location != registration.canonical_location:
            raise InvalidTableLocationError("registered table location changed through a symlink")

        managed_root = self._managed_root()
        if registration.ownership is TableOwnership.MANAGED:
            expected = managed_root / registration.table_id
            if registration.canonical_location != expected or not expected.is_relative_to(
                managed_root
            ):
                raise InvalidTableLocationError(
                    "managed table location does not match its workspace-owned table ID"
                )
        return registration

    def _managed_root(self) -> Path:
        workspace = self._context.workspace
        path = workspace.path / "tables"
        if path.is_symlink():
            raise InvalidTableLocationError("workspace managed-table root cannot be a symlink")
        try:
            managed_root = path.resolve(strict=True)
        except (OSError, RuntimeError) as error:
            raise InvalidTableLocationError(
                "workspace managed-table root is unavailable"
            ) from error
        if managed_root.parent != workspace.path:
            raise InvalidTableLocationError("managed table root resolves outside the workspace")
        return managed_root

    def _remove_managed_table_data(self, registration: TableRegistration) -> None:
        managed_root = self._managed_root()
        try:
            canonical_table_id = str(uuid.UUID(registration.table_id))
        except (AttributeError, TypeError, ValueError) as error:
            raise InvalidTableLocationError("managed table ID is not a canonical UUID") from error
        if canonical_table_id != registration.table_id:
            raise InvalidTableLocationError("managed table ID is not a canonical UUID")
        expected = managed_root / canonical_table_id
        if registration.canonical_location != expected or not expected.is_relative_to(managed_root):
            raise InvalidTableLocationError(
                "managed table cleanup target does not match its workspace-owned table ID"
            )
        if not shutil.rmtree.avoids_symlink_attacks:
            raise TableCleanupError("safe managed table removal is unavailable on this platform")
        if not all(hasattr(os, flag) for flag in ("O_DIRECTORY", "O_NOFOLLOW")):
            raise TableCleanupError("safe managed table removal requires POSIX directory flags")
        directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        try:
            root_fd = os.open(managed_root, directory_flags)
        except OSError as error:
            raise TableCleanupError("could not securely open the managed table root") from error
        try:
            try:
                target_stat = os.stat(
                    canonical_table_id,
                    dir_fd=root_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                return
            if stat.S_ISLNK(target_stat.st_mode):
                raise InvalidTableLocationError(
                    "managed table cleanup refuses a symlink at the registered table path"
                )
            if not stat.S_ISDIR(target_stat.st_mode):
                raise InvalidTableLocationError("managed table cleanup target is not a directory")
            try:
                shutil.rmtree(canonical_table_id, dir_fd=root_fd)
                os.fsync(root_fd)
            except OSError as error:
                raise TableCleanupError(
                    "managed table cleanup was interrupted; retry drop to finish removal"
                ) from error
        finally:
            os.close(root_fd)

    def _is_delta_table(self, location: Path) -> bool:
        try:
            return bool(self._delta.DeltaTable.is_deltatable(str(location)))
        except Exception as error:
            raise DeltaTableProviderError("could not inspect registered Delta location") from error

    def _ensure_create_location_safe(self, location: Path) -> None:
        if not location.exists():
            return
        if location.is_symlink() or not location.is_dir():
            raise TableLocationNotEmptyError(
                "create requires a missing or empty directory at the registered location"
            )
        try:
            next(location.iterdir())
        except StopIteration:
            return
        except OSError as error:
            raise TableLocationNotEmptyError(
                "could not verify that the registered create location is empty"
            ) from error
        raise TableLocationNotEmptyError(
            "create refuses a non-empty location that is not an existing Delta table"
        )

    def _open_at_version(self, location: Path, version: int) -> Any:
        try:
            return self._delta.DeltaTable(str(location), version=version)
        except Exception as error:
            raise DeltaTableProviderError(
                f"could not reopen committed Delta version {version}"
            ) from error

    def _read_staged_arrow(self, path: Path, data_format: InputFormat) -> Any:
        try:
            selected_format = (
                data_format if isinstance(data_format, InputFormat) else InputFormat(data_format)
            )
        except (TypeError, ValueError) as error:
            raise TableInputError("staged input declares an unsupported Arrow format") from error
        if selected_format is InputFormat.PARQUET:
            return self._parquet.read_table(path)
        try:
            return self._ipc.open_file(path).read_all()
        except (OSError, ValueError, self._arrow.ArrowException):
            # Accept both Arrow IPC file and stream encodings under the same staged format.
            return self._ipc.open_stream(path).read_all()

    def _schema_from_arrow(self, arrow_schema: Any) -> SchemaDescriptor:
        try:
            delta_schema = self._delta.Schema.from_arrow(arrow_schema)
            return SchemaDescriptor.from_delta_json(delta_schema.to_json())
        except Exception as error:
            raise TableInputError(
                "Arrow schema is not supported by the selected Delta provider"
            ) from error

    @staticmethod
    def _normalize_mode(mode: WriteMode | str) -> WriteMode:
        try:
            return mode if isinstance(mode, WriteMode) else WriteMode(mode)
        except (TypeError, ValueError) as error:
            raise UnsupportedWriteModeError(f"unsupported table write mode: {mode!r}") from error


def _validate_operation_id(operation_id: str) -> None:
    if (
        not isinstance(operation_id, str)
        or not operation_id
        or len(operation_id) > 128
        or any(ord(character) < 32 or ord(character) == 127 for character in operation_id)
    ):
        raise TableInputError(
            "operation_id must be a printable non-empty value up to 128 characters"
        )


def _payload_fingerprint(
    *,
    table: TableHandle,
    mode: WriteMode,
    schema: SchemaDescriptor,
    input_sha256: str,
    input_kind: str,
    row_count: int,
    profile: DeltaProviderProfile,
) -> str:
    payload = {
        "table_id": table.table_id,
        "location": str(table.location),
        "mode": mode.value,
        "schema_fingerprint": schema.fingerprint,
        "input_sha256": input_sha256,
        "input_kind": input_kind,
        "row_count": row_count,
        "provider_profile": profile.name,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _mutation_fingerprint(
    table: TableHandle,
    mode: str,
    input_sha256: str,
    profile: DeltaProviderProfile,
) -> str:
    payload = {
        "table_id": table.table_id,
        "location": str(table.location),
        "mode": mode,
        "input_sha256": input_sha256,
        "provider_profile": profile.name,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_unique_merge_source_key(source: Any, column_name: str) -> None:
    """Require non-null, unique scalar source keys before journaling a MERGE."""
    import pyarrow as pa  # type: ignore[import-untyped]

    key_type = source.schema.field(column_name).type
    if not any(
        predicate(key_type)
        for predicate in (
            pa.types.is_boolean,
            pa.types.is_integer,
            pa.types.is_string,
            pa.types.is_large_string,
            pa.types.is_binary,
            pa.types.is_large_binary,
            pa.types.is_fixed_size_binary,
            pa.types.is_date,
            pa.types.is_timestamp,
            pa.types.is_decimal,
        )
    ):
        raise TableInputError("MERGE source key must have a supported scalar Arrow type")
    seen: set[Any] = set()
    for value in source.column(column_name).to_pylist():
        if value is None:
            raise DuplicateMergeSourceKeyError("MERGE source key cannot contain NULL")
        if value in seen:
            raise DuplicateMergeSourceKeyError("MERGE source contains duplicate key values")
        seen.add(value)


def _mutation_affected_rows(mode: str, metrics: Any) -> int | None:
    if not isinstance(metrics, dict):
        return None
    if mode == "sql_merge":
        return _merge_history_affected_rows(metrics)
    metric_name = "num_updated_rows" if mode == "sql_update" else "num_deleted_rows"
    return _nonnegative_int(metrics.get(metric_name))


def _history_string_pairs(value: Any) -> tuple[tuple[str, str], ...] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise TypeError("history map is not a dictionary")
    pairs: list[tuple[str, str]] = []
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, (str, int, float, bool)):
            raise TypeError("history map contains an unsupported key or value")
        pairs.append((key, _history_scalar_string(item)))
    return tuple(sorted(pairs))


def _history_scalar_string(value: str | int | float | bool) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _history_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("history string field has an invalid value")
    return value


def _merge_history_affected_rows(metrics: Any) -> int | None:
    if not isinstance(metrics, dict):
        return None
    updated = _nonnegative_int(metrics.get("num_target_rows_updated"))
    inserted = _nonnegative_int(metrics.get("num_target_rows_inserted"))
    if updated is None or inserted is None:
        return None
    return updated + inserted


def _nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and value.isdecimal():
        return int(value)
    return None


def _required_transaction_version(record: TableOperationRecord) -> int:
    if record.transaction_version is None:
        raise OperationStateConflictError(
            f"operation {record.operation_id!r} has no provider transaction version"
        )
    return record.transaction_version


def _require_commit_result(result: CommitResult | TableMutationResult) -> CommitResult:
    if not isinstance(result, CommitResult):
        raise OperationStateConflictError("a SQL mutation journal record cannot replay as a write")
    return result


@contextmanager
def _table_mutation_lock(table: TableHandle) -> Any:
    """Serialize same-table and location-alias mutations inside this process."""
    keys = sorted({("table_id", table.table_id), ("location", str(table.location))})
    locks: list[Any] = []
    with _TABLE_LOCKS_GUARD:
        for key in keys:
            lock = _TABLE_LOCKS.get(key)
            if lock is None:
                lock = threading.RLock()
                _TABLE_LOCKS[key] = lock
            locks.append(lock)
    for lock in locks:
        lock.acquire()
    try:
        yield
    finally:
        for lock in reversed(locks):
            lock.release()


def _load_dependencies() -> tuple[Any, Any, Any, Any]:
    try:
        from importlib import import_module

        pa = import_module("pyarrow")
        ipc = import_module("pyarrow.ipc")
        pq = import_module("pyarrow.parquet")
        delta = import_module("deltalake")
    except ImportError as error:
        raise DeltaProviderUnavailableError(
            "local Delta table operations require the optional 'brickhouse[delta]' extra"
        ) from error
    return (
        _DeltaAPI(
            delta.DeltaTable,
            delta.Schema,
            delta.write_deltalake,
            delta.CommitProperties,
            delta.Transaction,
        ),
        pa,
        pq,
        ipc,
    )


class _DeltaAPI:
    """Bundle the selected public delta-rs calls without importing it at module load."""

    def __init__(
        self,
        delta_table: Any,
        schema: Any,
        write_deltalake: Any,
        commit_properties: Any,
        transaction: Any,
    ) -> None:
        self.DeltaTable = delta_table
        self.Schema = schema
        self.write_deltalake = write_deltalake
        self.CommitProperties = commit_properties
        self.Transaction = transaction
