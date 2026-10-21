"""Local Delta table service backed by the public delta-rs Python API."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import threading
import uuid
from contextlib import contextmanager
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
    TableMissingError,
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


class SnapshotUnavailableError(SnapshotSelectionError):
    """A requested historical Delta version or timestamp is unavailable."""


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
    """Create, read, append, and overwrite local registered Delta tables.

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
            raise DeltaTableProviderError(
                f"could not read Delta rows from {table.full_name}"
            ) from error
        actual_schema = self._schema_from_arrow(arrow_table.schema)
        if actual_schema.fingerprint != snapshot.schema.fingerprint:
            raise TableInputError(
                f"provider rows did not match the schema at Delta version {snapshot.version}"
            )
        return TableReadResult(snapshot=snapshot, data=arrow_table)

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
        staged_path = self._staging.verify_content(staged)
        try:
            arrow_table = self._read_staged_arrow(staged_path, staged.data_format)
        except Exception as error:
            if isinstance(error, TableInputError):
                raise
            raise TableInputError("staged input is not a readable declared Arrow format") from error

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
    ) -> CommitResult:
        """Journal, commit, and return the exact version for an in-memory Arrow table."""
        if not isinstance(data, self._arrow.Table):
            raise TypeError("data must be a pyarrow.Table")
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
                if existing.fingerprint != fingerprint:
                    raise OperationKeyReuseError(
                        f"operation ID {operation_id!r} was reused with a different payload"
                    )
                if existing.state is TableOperationState.COMMITTED:
                    return self._result_from_record(table, existing)
                if existing.state in (
                    TableOperationState.COMMITTING,
                    TableOperationState.UNCERTAIN,
                ):
                    recovered = self._reconcile_operation(table, existing)
                    if recovered is not None:
                        return recovered
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
                    return recovered
                self._mark_uncertain(record)
                raise UncertainCommitError(operation_id) from error

            recovered = self._reconcile_operation(table, record)
            if recovered is not None:
                return recovered
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
        expected_transaction = record.transaction_version - 1
        observed_transaction = self._provider_transaction_version(previous, table.table_id)
        latest_transaction = self._journal.latest_transaction_version(table.table_id)
        expected_previous_transaction = None if expected_transaction < 0 else expected_transaction
        if (
            previous_version != record.base_version
            or observed_transaction != expected_previous_transaction
            or latest_transaction != record.transaction_version
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
    ) -> CommitResult | None:
        try:
            registration = self._current_registration(table)
            location = registration.canonical_location
            if not self._is_delta_table(location):
                return None
            provider_table = self._delta.DeltaTable(str(location))
            observed_transaction = provider_table.transaction_version(table.table_id)
            if observed_transaction != record.transaction_version:
                return None
            commit_version = self._find_commit_version(location, record)
            if commit_version is None:
                return None
            if commit_version <= record.base_version:
                return None
            committed_table = self._open_at_version(location, commit_version)
            snapshot = self._capture_snapshot(table, committed_table)
            if snapshot.schema.fingerprint != record.schema_fingerprint:
                return None
            finalized = self._journal.finalize_committed(
                table.table_id,
                record.operation_id,
                commit_version,
            )
            return self._result_from_record(table, finalized, snapshot=snapshot)
        except Exception:
            # An incomplete provider/journal read is not evidence of non-commit.
            return None

    def _find_commit_version(
        self,
        location: Path,
        record: TableOperationRecord,
    ) -> int | None:
        try:
            history = self._delta.DeltaTable(str(location)).history(limit=100)
        except Exception:
            return None
        versions = [
            int(entry["version"])
            for entry in history
            if entry.get(_OPERATION_METADATA_KEY) == record.operation_id
            and entry.get(_FINGERPRINT_METADATA_KEY) == record.fingerprint
            and int(entry.get("version", -1)) > record.base_version
        ]
        return versions[0] if len(versions) == 1 else None

    def _result_from_record(
        self,
        table: TableHandle,
        record: TableOperationRecord,
        *,
        snapshot: TableSnapshot | None = None,
    ) -> CommitResult:
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
