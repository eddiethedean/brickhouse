"""Sparkless sessions bound to BrickHouse's Delta table service.

Sparkless is imported lazily and each session owns its provider and namespace
state. Registered table writes go through the table service. Native path writers
are guarded because they bypass that contract.
"""

from __future__ import annotations

import json
import re
import threading
import uuid
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, cast

from brickhouse.catalog import (
    NamespaceNotFoundError,
    TableAlreadyExistsError,
    TableNotFoundError,
    TableOwnership,
    parse_identifier,
)
from brickhouse.tables import (
    DeltaRsTableService,
    SnapshotSelectionError,
    SnapshotSelector,
    TableHandle,
    WriteMode,
)

if TYPE_CHECKING:
    from brickhouse.jobs.execution_context import ResolvedTaskContext
    from brickhouse.workspace.context import WorkspaceContext

ENGINE_PROFILE = "spark35-local-v1"
_DEFAULT_CONFIGURATION = MappingProxyType(
    {
        "spark.sql.ansi.enabled": "true",
        "spark.sql.caseSensitive": "false",
        "spark.sql.session.timeZone": "UTC",
    }
)
_CONFIGURATION_KEYS = frozenset(_DEFAULT_CONFIGURATION)
_DECIMAL_TYPE = re.compile(r"decimal\((\d+),\s*(\d+)\)\Z", re.IGNORECASE)
_PROVIDER_NAME = "brickhouse-local-delta"
_MAX_WRITE_IPC_BYTES = 8 * 1024 * 1024
_MAX_WRITE_ROWS = 100_000
_SERVERLESS_PROFILE = "serverless-local-v1"
_CLASSIC_PROFILES = frozenset({"classic-standard-local-v1", "classic-dedicated-local-v1"})
_CLASSIC_REQUIRED_SETTINGS = frozenset({"spark.sql.ansi.enabled", "spark.sql.session.timeZone"})


class SparklessAdapterError(RuntimeError):
    """Base error for the BrickHouse-to-Sparkless adapter boundary."""


class UnsupportedDeltaSchemaError(SparklessAdapterError, ValueError):
    """A Delta schema cannot be represented by Sparkless' public provider API."""


class UnsupportedSparklessConfigurationError(SparklessAdapterError, ValueError):
    """A session setting is outside the explicit local profile envelope."""


class UnsupportedSparklessSessionOperationError(SparklessAdapterError, NotImplementedError):
    """A session operation is outside the selected local semantic profile."""


class UnsupportedSparklessWriteError(SparklessAdapterError, ValueError):
    """A Sparkless writer operation is not routed through the table service."""


class UnregisteredTableWriteError(UnsupportedSparklessWriteError):
    """A named write does not target an existing registered Delta table."""


class SparklessSessionClosedError(SparklessAdapterError):
    """The adapter session has already been closed."""


class SparklessActionCancelledError(SparklessAdapterError):
    """Sparkless reported that an active action reached a canceled outcome."""


class SparklessCancellationHandle:
    """Thread-safe cancellation handle for one owned Sparkless session."""

    def __init__(self, native_handle: Any) -> None:
        self._native_handle = native_handle

    def cancel_active_actions(self) -> int:
        """Request cancellation and return the number of active actions signaled."""
        callback = getattr(self._native_handle, "cancel_all_active_actions", None)
        if not callable(callback):
            raise SparklessAdapterError(
                "Sparkless cancellation handle has no public active-action operation"
            )
        count = callback()
        if type(count) is not int or count < 0:
            raise SparklessAdapterError(
                "Sparkless cancellation handle returned an invalid active-action count"
            )
        return count


class _BrickHouseExternalCatalogProvider:
    """Resolve registered snapshots through one active workspace context."""

    def __init__(
        self,
        context: WorkspaceContext,
        snapshot_versions: Mapping[str, int] | None = None,
    ) -> None:
        self._context = context
        self._namespace = context.new_namespace_state()
        self._tables = DeltaRsTableService(context)
        versions = dict(snapshot_versions or {})
        if any(
            not isinstance(table_id, str) or not table_id or type(version) is not int or version < 0
            for table_id, version in versions.items()
        ):
            raise SnapshotSelectionError(
                "snapshot versions must map table IDs to non-negative integers"
            )
        self._snapshot_versions = MappingProxyType(versions)
        self._owner_thread = threading.get_ident()

    def resolve_table(self, identifier: str) -> Any | None:
        """Return the current validated snapshot or an exact catalog miss."""
        self._require_owner()
        table = self._resolve_handle(identifier)
        if table is None:
            return None
        selected_version = self._snapshot_versions.get(table.table_id)
        selector = (
            SnapshotSelector(version=selected_version) if selected_version is not None else None
        )
        snapshot = self._tables.read_snapshot(table, selector)
        return self._descriptor(table, snapshot)

    def write_table(self, request: Any) -> Any:
        """Delegate an existing registered table write to the Delta service."""
        self._require_owner()
        mode_value = getattr(request.mode, "value", request.mode)
        try:
            mode = {"append": WriteMode.APPEND, "overwrite": WriteMode.OVERWRITE}[str(mode_value)]
        except KeyError as error:
            raise UnsupportedSparklessWriteError(
                f"Sparkless save mode is not supported: {mode_value!r}"
            ) from error
        selected_format = request.format.casefold() if isinstance(request.format, str) else None
        if request.format is not None and selected_format != "delta":
            raise UnsupportedSparklessWriteError(
                f"Sparkless table format is not supported: {request.format!r}"
            )
        if request.options:
            raise UnsupportedSparklessWriteError(
                "Sparkless writer options are not supported by the local Delta profile"
            )
        if request.partition_by:
            raise UnsupportedSparklessWriteError(
                "partitioned Sparkless table writes are not supported"
            )
        table = self._resolve_handle(request.identifier)
        if table is None:
            raise UnregisteredTableWriteError(
                f"table is not registered with BrickHouse: {request.identifier}"
            )
        if table.table_id in self._snapshot_versions:
            raise UnsupportedSparklessWriteError(
                "a historical-snapshot Sparkless session is read-only"
            )
        current = self.resolve_table(request.identifier)
        if current is None:
            raise UnregisteredTableWriteError(
                f"table is not registered with BrickHouse: {request.identifier}"
            )
        if (
            request.table.table_id != table.table_id
            or Path(request.table.canonical_location).resolve(strict=False) != table.location
        ):
            raise UnsupportedSparklessWriteError(
                "Sparkless writer descriptor no longer matches the registered table"
            )
        if request.table.version is None:
            raise UnsupportedSparklessWriteError(
                "Sparkless writer descriptor is missing a pinned Delta version"
            )
        if len(request.data) > _MAX_WRITE_IPC_BYTES:
            raise UnsupportedSparklessWriteError(
                f"Sparkless Arrow IPC payload exceeds {_MAX_WRITE_IPC_BYTES} bytes"
            )
        try:
            import pyarrow.ipc as ipc  # type: ignore[import-untyped]

            data = ipc.open_stream(request.data).read_all()
        except Exception as error:
            raise UnsupportedSparklessWriteError(
                "Sparkless writer supplied an invalid Arrow IPC stream"
            ) from error
        if data.num_rows > _MAX_WRITE_ROWS:
            raise UnsupportedSparklessWriteError(
                f"Sparkless table write exceeds {_MAX_WRITE_ROWS} rows"
            )

        result = self._tables.write_arrow(
            table,
            data,
            mode,
            operation_id=f"sparkless-{uuid.uuid4().hex}",
            expected_version=request.table.version,
        )
        descriptor = self._descriptor(table, result.snapshot)
        from sparkless.external_catalog import ExternalTableWriteResult

        receipt = ExternalTableWriteResult(descriptor, result.version)
        receipt.validate_against(request.table)
        return receipt

    def create_managed_table(
        self,
        identifier: str,
        dataframe: Any,
        *,
        operation_id: str,
    ) -> Any:
        """Register and create a managed Delta table from a Sparkless DataFrame."""
        self._require_owner()
        arrow_table = _to_arrow(dataframe)
        _validate_arrow_write(arrow_table)
        catalog, schema, name = self._registration_parts(identifier)
        try:
            registration = self._context.tables.create_managed(catalog, schema, name)
        except TableAlreadyExistsError as error:
            # A caller may retry after a process stopped between the Delta commit and
            # receiving its result. Reuse the identity only when this exact operation
            # is already journaled; write_arrow then verifies the payload fingerprint.
            registration = self._context.tables.get(catalog, schema, name)
            if registration.ownership is not TableOwnership.MANAGED or (
                self._context.table_operations.get(registration.table_id, operation_id) is None
            ):
                raise error
        table = TableHandle.from_registration(registration)
        if table.table_id in self._snapshot_versions:
            raise UnsupportedSparklessWriteError(
                "a historical-snapshot Sparkless session is read-only"
            )
        return self._tables.write_arrow(
            table,
            arrow_table,
            WriteMode.CREATE,
            operation_id=operation_id,
        )

    def write_registered_path(
        self,
        dataframe: Any,
        location: str | Path,
        mode: WriteMode | str,
        *,
        operation_id: str,
    ) -> Any:
        """Write only to a registered local Delta location through the table service."""
        self._require_owner()
        try:
            registration = self._context.tables.get_by_location(location)
        except TableNotFoundError as error:
            raise UnregisteredTableWriteError(
                f"path is not a registered BrickHouse Delta table: {location}"
            ) from error
        table = TableHandle.from_registration(registration)
        if table.table_id in self._snapshot_versions:
            raise UnsupportedSparklessWriteError(
                "a historical-snapshot Sparkless session is read-only"
            )
        arrow_table = _to_arrow(dataframe)
        _validate_arrow_write(arrow_table)
        return self._tables.write_arrow(
            table,
            arrow_table,
            mode,
            operation_id=operation_id,
        )

    def _resolve_handle(self, identifier: str) -> TableHandle | None:
        parsed = parse_identifier(identifier)
        try:
            return self._tables.resolve(parsed, self._namespace)
        except TableNotFoundError:
            return None
        except NamespaceNotFoundError:
            # A bad catalog/schema is an explicit error, not a genuine table miss.
            raise

    def _descriptor(self, table: TableHandle, snapshot: Any) -> Any:
        from sparkless.external_catalog import ExternalTableDescriptor

        descriptor = ExternalTableDescriptor(
            table_id=table.table_id,
            canonical_location=str(table.location),
            schema=_sparkless_schema(snapshot.schema.json_text),
            provider=_PROVIDER_NAME,
            version=snapshot.version,
        )
        descriptor.validate()
        return descriptor

    def _registration_parts(self, identifier: str) -> tuple[str, str, str]:
        parsed = parse_identifier(identifier)
        if len(parsed.parts) == 1:
            catalog, schema, name = (
                self._namespace.current_catalog,
                self._namespace.current_schema,
                parsed.parts[0].value,
            )
        elif len(parsed.parts) == 2:
            catalog, schema, name = (
                self._namespace.current_catalog,
                parsed.parts[0].value,
                parsed.parts[1].value,
            )
        else:
            catalog, schema, name = parsed.values
        return catalog, schema, name

    def _require_owner(self) -> None:
        if threading.get_ident() != self._owner_thread:
            raise SparklessAdapterError(
                "Sparkless provider callbacks must run on their creating thread"
            )

    def use_catalog(self, identifier: str) -> None:
        self._namespace.use_catalog(identifier)

    def use_schema(self, identifier: str) -> None:
        self._namespace.use_schema(identifier)


class SparklessSession:
    """Own one native Sparkless session and its registered table provider.

    The ``spark`` property intentionally exposes the native object only from
    this internal adapter package. B010.2 must complete write routing before a
    user-facing session API can export it.
    """

    def __init__(
        self,
        native_session: Any,
        provider: _BrickHouseExternalCatalogProvider,
        configuration: Mapping[str, str],
        compute_profile: str | None = None,
        runtime_profile: str | None = None,
    ) -> None:
        self._native_session = native_session
        self._provider: _BrickHouseExternalCatalogProvider | None = provider
        self._configuration = MappingProxyType(dict(configuration))
        self._compute_profile = compute_profile
        self._runtime_profile = runtime_profile
        self._owner_thread = threading.get_ident()
        self._closed = False
        self._facade: _SparklessSessionFacade | None = _SparklessSessionFacade(self)

    @property
    def configuration(self) -> Mapping[str, str]:
        """Return the immutable effective configuration for this session."""
        return self._configuration

    @property
    def compute_profile(self) -> str | None:
        """Return the resolved local compute profile, if one was applied."""
        return self._compute_profile

    @property
    def runtime_profile(self) -> str | None:
        """Return the mapped local runtime profile, if this is classic compute."""
        return self._runtime_profile

    @property
    def spark(self) -> Any:
        """Return the guarded session facade on its owning thread."""
        self._require_open_owner()
        facade = self._facade
        if facade is None:
            raise SparklessSessionClosedError("Sparkless session is closed")
        return facade

    def table(self, identifier: str) -> Any:
        """Read a registered Delta snapshot through Sparkless' public API."""
        return self.spark.table(identifier)

    def create_managed_table(
        self,
        identifier: str,
        dataframe: Any,
        *,
        operation_id: str,
    ) -> Any:
        """Create a registered managed Delta table from a Sparkless DataFrame."""
        provider = self._require_provider()
        return provider.create_managed_table(
            identifier,
            dataframe,
            operation_id=operation_id,
        )

    def write_path(
        self,
        dataframe: Any,
        location: str | Path,
        mode: WriteMode | str,
        *,
        operation_id: str,
    ) -> Any:
        """Write to a registered local table location through DeltaRsTableService."""
        provider = self._require_provider()
        return provider.write_registered_path(
            dataframe,
            location,
            mode,
            operation_id=operation_id,
        )

    def cancellation_handle(self) -> SparklessCancellationHandle:
        """Return a handle that a control thread may use to cancel this session's actions."""
        self._require_open_owner()
        factory = getattr(self._native_session, "cancellation_handle", None)
        if not callable(factory):
            raise UnsupportedSparklessConfigurationError(
                "active SQL cancellation requires Sparkless 4.15.0 or newer"
            )
        return SparklessCancellationHandle(factory())

    def use_catalog(self, identifier: str) -> None:
        """Set this session's current BrickHouse catalog and reset to default schema."""
        provider = self._require_provider()
        provider.use_catalog(identifier)

    def use_schema(self, identifier: str) -> None:
        """Set this session's current schema, optionally with a catalog qualifier."""
        provider = self._require_provider()
        provider.use_schema(identifier)

    def close(self) -> None:
        """Detach this session's provider and stop its native engine session."""
        if self._closed:
            return
        if threading.get_ident() != self._owner_thread:
            raise SparklessAdapterError(
                "Sparkless sessions must be closed on their creating thread"
            )

        native_session = self._native_session
        first_error: BaseException | None = None
        try:
            native_session.set_external_catalog_provider(None)
        except BaseException as error:  # ensure native resources are still stopped
            first_error = error
        try:
            native_session.stop()
        except BaseException as error:  # preserve the first teardown error
            if first_error is None:
                first_error = error
        finally:
            self._provider = None
            self._native_session = None
            self._facade = None
            self._closed = True
        if first_error is not None:
            raise first_error

    def __enter__(self) -> SparklessSession:
        self._require_open_owner()
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def _require_open_owner(self) -> None:
        if threading.get_ident() != self._owner_thread:
            raise SparklessAdapterError("Sparkless sessions must be used on their creating thread")
        if self._closed:
            raise SparklessSessionClosedError("Sparkless session is closed")

    def _require_provider(self) -> _BrickHouseExternalCatalogProvider:
        self._require_open_owner()
        if self._provider is None:
            raise SparklessSessionClosedError("Sparkless session provider is detached")
        return self._provider

    def _set_configuration(self, key: str, value: str) -> None:
        self._require_open_owner()
        selected = _validate_configuration({**self._configuration, key: value})
        self._native_session.conf.set(key, value)
        self._configuration = MappingProxyType(selected)


class _SparklessSessionFacade:
    """Expose bounded public session operations without a raw writer escape."""

    def __init__(self, owner: SparklessSession) -> None:
        self._owner = owner

    @property
    def conf(self) -> _SparklessConfigurationFacade:
        return _SparklessConfigurationFacade(self._owner)

    @property
    def catalog(self) -> _SparklessCatalogFacade:
        return _SparklessCatalogFacade(self._owner)

    def table(self, identifier: str) -> _SparklessDataFrameFacade:
        self._owner._require_open_owner()
        return cast(
            _SparklessDataFrameFacade,
            _wrap_dataframe(self._owner._native_session.table(identifier), self._owner),
        )

    def createDataFrame(self, *args: Any, **kwargs: Any) -> _SparklessDataFrameFacade:
        self._owner._require_open_owner()
        return cast(
            _SparklessDataFrameFacade,
            _wrap_dataframe(
                self._owner._native_session.createDataFrame(*args, **kwargs), self._owner
            ),
        )

    def cancellation_handle(self) -> SparklessCancellationHandle:
        """Return the public Sparkless control handle for this internal session."""
        return self._owner.cancellation_handle()

    def sql(self, query: str, *args: Any, **kwargs: Any) -> Any:
        self._owner._require_open_owner()
        if not isinstance(query, str):
            raise TypeError("Sparkless SQL query must be a string")
        normalized = query.lstrip().lower()
        profile = self._owner.compute_profile or ENGINE_PROFILE
        if re.match(r"^create\s+(or\s+replace\s+)?global\s+temp(orary)?\s+view\b", normalized):
            raise UnsupportedSparklessSessionOperationError(
                f"global temporary views are unsupported for local Sparkless profile {profile!r}"
            )
        if re.match(r"^(cache|uncache)\s+(lazy\s+)?table\b", normalized):
            raise UnsupportedSparklessSessionOperationError(
                f"SQL table caching is unsupported for local Sparkless profile {profile!r}"
            )
        if _has_unquoted_statement_terminator(query):
            raise UnsupportedSparklessWriteError(
                "multiple or terminated SQL statements are not supported by this adapter"
            )
        if not (
            re.match(r"^(select|with)\b", normalized)
            or re.match(r"^create\s+(or\s+replace\s+)?temp(orary)?\s+view\b", normalized)
        ):
            raise UnsupportedSparklessWriteError(
                "this adapter permits read queries and temporary views only; "
                "use the table service for writes"
            )
        result = self._owner._native_session.sql(query, *args, **kwargs)
        return _wrap_dataframe(result, self._owner)


def _has_unquoted_statement_terminator(query: str) -> bool:
    quote: str | None = None
    line_comment = False
    block_comment = False
    index = 0
    while index < len(query):
        character = query[index]
        following = query[index + 1] if index + 1 < len(query) else ""
        if line_comment:
            if character in "\r\n":
                line_comment = False
            index += 1
            continue
        if block_comment:
            if character == "*" and following == "/":
                block_comment = False
                index += 2
            else:
                index += 1
            continue
        if quote is not None:
            if character == quote and following == quote:
                index += 2
                continue
            if character == "\\" and following in {quote, "\\"}:
                index += 2
                continue
            if character == quote:
                quote = None
            index += 1
            continue
        if character == "-" and following == "-":
            line_comment = True
            index += 2
            continue
        if character == "/" and following == "*":
            block_comment = True
            index += 2
            continue
        if character in {"'", '"', "`"}:
            quote = character
        elif character == ";":
            return True
        index += 1
    return False


class _SparklessConfigurationFacade:
    def __init__(self, owner: SparklessSession) -> None:
        self._owner = owner

    def get(self, key: str, default: str | None = None) -> str:
        self._owner._require_open_owner()
        if not isinstance(key, str) or key not in _CONFIGURATION_KEYS:
            raise UnsupportedSparklessConfigurationError(
                f"unsupported Sparkless session setting: {key!r}"
            )
        if default is None:
            return cast(str, self._owner._native_session.conf.get(key))
        return cast(str, self._owner._native_session.conf.get(key, default))

    def set(self, key: str, value: str) -> None:
        self._owner._set_configuration(key, value)


class _SparklessCatalogFacade:
    def __init__(self, owner: SparklessSession) -> None:
        self._owner = owner

    def currentDatabase(self) -> str:
        provider = self._owner._require_provider()
        return provider._namespace.current_schema

    def setCurrentDatabase(self, name: str) -> None:
        self._owner.use_schema(name)

    def tableExists(self, identifier: str) -> bool:
        self._owner._require_open_owner()
        return cast(bool, self._owner._native_session.catalog.tableExists(identifier))


class _SparklessDataFrameFacade:
    """Delegate DataFrame operations while guarding all writer entry points."""

    def __init__(self, dataframe: Any, owner: SparklessSession) -> None:
        self._dataframe = dataframe
        self._owner = owner

    def toArrow(self, *args: Any, **kwargs: Any) -> Any:
        """Materialize Arrow rows and preserve Sparkless' typed cancel outcome."""
        self._owner._require_open_owner()
        from sparkless.sql import SparklessCancelledError

        try:
            return self._dataframe.toArrow(*args, **kwargs)
        except SparklessCancelledError as error:
            raise SparklessActionCancelledError(
                "Sparkless canceled the active DataFrame action"
            ) from error

    def toArrowBatches(self, *args: Any, **kwargs: Any) -> _SparklessArrowBatchIterator:
        """Stream bounded Arrow batches and translate cancellation to the adapter error."""
        self._owner._require_open_owner()
        return _SparklessArrowBatchIterator(self._dataframe.toArrowBatches(*args, **kwargs))

    @property
    def write(self) -> _SparklessDataFrameWriterFacade:
        self._owner._require_open_owner()
        return _SparklessDataFrameWriterFacade(self._dataframe.write, self._owner)

    def __getattr__(self, name: str) -> Any:
        self._owner._require_open_owner()
        if name in {"cache", "persist", "unpersist"}:
            profile = self._owner.compute_profile or ENGINE_PROFILE
            raise UnsupportedSparklessSessionOperationError(
                f"DataFrame.{name}() is unsupported for local Sparkless profile {profile!r}"
            )
        if name in {"writeTo", "write_to"}:
            raise UnsupportedSparklessWriteError(
                "Sparkless DataFrameWriterV2 is not routed through the table service"
            )
        attribute = getattr(self._dataframe, name)
        if type(attribute).__name__ in {
            "PyDataFrameNaFunctions",
            "PyGroupedData",
            "PyPivotedData",
            "PyRDD",
        }:
            return _SparklessNestedFacade(attribute, self._owner)
        if not callable(attribute):
            return attribute

        def call(*args: Any, **kwargs: Any) -> Any:
            self._owner._require_open_owner()
            return _wrap_dataframe(attribute(*args, **kwargs), self._owner)

        return call


class _SparklessArrowBatchIterator:
    """Preserve the native iterator lifecycle while translating its cancel exception."""

    def __init__(self, iterator: Any) -> None:
        self._iterator = iterator
        self._batches: Any | None = None

    def __enter__(self) -> _SparklessArrowBatchIterator:
        self._batches = self._iterator.__enter__()
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self._iterator.__exit__(*(_exc_info))

    def __iter__(self) -> _SparklessArrowBatchIterator:
        return self

    def __next__(self) -> Any:
        if self._batches is None:
            raise RuntimeError("Arrow batch iterator must be entered before iteration")
        from sparkless.sql import SparklessCancelledError, SparklessError

        try:
            return next(self._batches)
        except SparklessCancelledError as error:
            raise SparklessActionCancelledError(
                "Sparkless canceled the active Arrow batch stream"
            ) from error
        except SparklessError as error:
            if "cancel" not in str(error).casefold():
                raise
            raise SparklessActionCancelledError(
                "Sparkless canceled the active Arrow batch stream"
            ) from error


class _SparklessDataFrameWriterFacade:
    _CHAINABLE = frozenset({"format", "mode", "option", "options", "partitionBy"})
    _BLOCKED = frozenset(
        {
            "csv",
            "insertInto",
            "insert_into",
            "jdbc",
            "json",
            "orc",
            "parquet",
            "save",
            "text",
        }
    )

    def __init__(self, writer: Any, owner: SparklessSession) -> None:
        self._writer = writer
        self._owner = owner
        self._format: str | None = None
        self._mode: str | None = None
        self._has_options = False
        self._has_partitions = False

    def __getattr__(self, name: str) -> Any:
        self._owner._require_open_owner()
        if name in {"saveAsTable", "save_as_table"}:
            return self._save_as_table
        if name in self._BLOCKED:
            return self._reject_write
        if name not in self._CHAINABLE:
            raise UnsupportedSparklessWriteError(
                f"Sparkless writer operation is not supported: {name}"
            )
        operation = getattr(self._writer, name)

        def chain(*args: Any, **kwargs: Any) -> _SparklessDataFrameWriterFacade:
            self._owner._require_open_owner()
            if name == "format":
                selected = args[0] if args else kwargs.get("source")
                self._format = selected if isinstance(selected, str) else None
            elif name == "mode":
                selected = args[0] if args else kwargs.get("saveMode")
                self._mode = str(getattr(selected, "value", selected))
            elif name in {"option", "options"}:
                self._has_options = True
            elif name == "partitionBy":
                self._has_partitions = True
            self._writer = operation(*args, **kwargs)
            return self

        return chain

    def _save_as_table(self, identifier: str, *args: Any, **kwargs: Any) -> Any:
        self._owner._require_open_owner()
        if self._mode not in {"append", "overwrite"}:
            raise UnsupportedSparklessWriteError(
                "Sparkless saveAsTable requires explicit append or overwrite mode"
            )
        selected_format = self._format.casefold() if self._format is not None else None
        if selected_format not in {None, "delta"}:
            raise UnsupportedSparklessWriteError(
                f"Sparkless table format is not supported: {self._format!r}"
            )
        if self._has_options:
            raise UnsupportedSparklessWriteError(
                "Sparkless writer options are not supported by the local Delta profile"
            )
        if self._has_partitions:
            raise UnsupportedSparklessWriteError(
                "partitioned Sparkless table writes are not supported"
            )
        provider = self._owner._require_provider()
        descriptor = provider.resolve_table(identifier)
        if descriptor is None:
            raise UnregisteredTableWriteError(
                f"table is not registered with BrickHouse: {identifier}"
            )
        method_name = "saveAsTable" if hasattr(self._writer, "saveAsTable") else "save_as_table"
        return getattr(self._writer, method_name)(identifier, *args, **kwargs)

    def _reject_write(self, *_args: Any, **_kwargs: Any) -> None:
        self._owner._require_open_owner()
        raise UnsupportedSparklessWriteError(
            "Sparkless path/insert writers bypass the table service; use session.write_path() "
            "for a registered Delta location or saveAsTable() for a registered table"
        )


def _wrap_dataframe(dataframe: Any, owner: SparklessSession) -> Any:
    from sparkless.sql import DataFrame

    if isinstance(dataframe, DataFrame):
        return _SparklessDataFrameFacade(dataframe, owner)
    if type(dataframe).__name__ in {
        "PyDataFrameNaFunctions",
        "PyGroupedData",
        "PyPivotedData",
        "PyRDD",
    }:
        return _SparklessNestedFacade(dataframe, owner)
    return dataframe


class _SparklessNestedFacade:
    """Keep chained Sparkless helper objects behind the DataFrame write guard."""

    def __init__(self, value: Any, owner: SparklessSession) -> None:
        self._value = value
        self._owner = owner

    def __getattr__(self, name: str) -> Any:
        self._owner._require_open_owner()
        attribute = getattr(self._value, name)
        if not callable(attribute):
            return _wrap_dataframe(attribute, self._owner)

        def call(*args: Any, **kwargs: Any) -> Any:
            self._owner._require_open_owner()
            return _wrap_dataframe(attribute(*args, **kwargs), self._owner)

        return call


def _to_arrow(dataframe: Any) -> Any:
    from sparkless.sql import DataFrame

    if isinstance(dataframe, _SparklessDataFrameFacade):
        dataframe = dataframe._dataframe
    if not isinstance(dataframe, DataFrame):
        raise TypeError("dataframe must be a Sparkless DataFrame")
    try:
        import pyarrow as pa

        result = dataframe.toArrow()
    except Exception as error:
        raise SparklessAdapterError(
            "could not materialize the Sparkless DataFrame as Arrow"
        ) from error
    if not isinstance(result, pa.Table):
        raise SparklessAdapterError("Sparkless toArrow() did not return a PyArrow Table")
    return result


def _validate_arrow_write(data: Any) -> None:
    if data.nbytes > _MAX_WRITE_IPC_BYTES:
        raise UnsupportedSparklessWriteError(
            f"Sparkless Arrow input exceeds {_MAX_WRITE_IPC_BYTES} bytes"
        )
    if data.num_rows > _MAX_WRITE_ROWS:
        raise UnsupportedSparklessWriteError(
            f"Sparkless table write exceeds {_MAX_WRITE_ROWS} rows"
        )


class SparklessSessionFactory:
    """Create owner-thread Sparkless sessions over one active local workspace."""

    def __init__(
        self,
        context: WorkspaceContext,
        *,
        engine_profile: str = ENGINE_PROFILE,
    ) -> None:
        if engine_profile != ENGINE_PROFILE:
            raise UnsupportedSparklessConfigurationError(
                f"unsupported Sparkless engine profile: {engine_profile}"
            )
        if not context.is_active:
            # Accessing this property provides the workspace's established error.
            _ = context.workspace
        self._context = context
        self.engine_profile = engine_profile

    def open(
        self,
        *,
        app_name: str = "brickhouse",
        configuration: Mapping[str, str] | None = None,
        snapshot_versions: Mapping[str, int] | None = None,
    ) -> SparklessSession:
        """Start an isolated session, optionally pinned to selected Delta versions."""
        if not isinstance(app_name, str) or not app_name.strip():
            raise UnsupportedSparklessConfigurationError(
                "Sparkless app_name must be a non-empty string"
            )
        return self._open(
            app_name=app_name,
            configuration=configuration,
            snapshot_versions=snapshot_versions,
        )

    def open_for_execution(
        self,
        execution_context: ResolvedTaskContext,
        *,
        app_name: str = "brickhouse",
    ) -> SparklessSession:
        """Apply one validated serverless/classic execution context to a session."""
        from brickhouse.jobs.execution_context import ResolvedTaskContext as ContextType

        if not isinstance(execution_context, ContextType):
            raise UnsupportedSparklessConfigurationError(
                "execution_context must be a resolved BrickHouse task context"
            )
        profile = execution_context.profile
        runtime_profile = execution_context.runtime_profile
        configuration = execution_context.effective_settings
        if profile == _SERVERLESS_PROFILE:
            if runtime_profile is not None:
                raise UnsupportedSparklessConfigurationError(
                    "serverless execution must not select a classic runtime profile"
                )
            missing = _CLASSIC_REQUIRED_SETTINGS - configuration.keys()
            if missing:
                raise UnsupportedSparklessConfigurationError(
                    "serverless execution context is missing its ANSI/timezone defaults"
                )
        elif profile in _CLASSIC_PROFILES:
            if runtime_profile != ENGINE_PROFILE:
                raise UnsupportedSparklessConfigurationError(
                    "classic execution runtime is not mapped to spark35-local-v1"
                )
            missing = _CLASSIC_REQUIRED_SETTINGS - configuration.keys()
            if missing:
                raise UnsupportedSparklessConfigurationError(
                    "classic ANSI/timezone defaults are not inferred; provide both settings "
                    "explicitly for the selected runtime"
                )
        else:
            raise UnsupportedSparklessConfigurationError(
                f"unsupported compute profile for Sparkless sessions: {profile!r}"
            )
        return self._open(
            app_name=app_name,
            configuration=configuration,
            compute_profile=profile,
            runtime_profile=runtime_profile,
        )

    def _open(
        self,
        *,
        app_name: str,
        configuration: Mapping[str, str] | None,
        compute_profile: str | None = None,
        runtime_profile: str | None = None,
        snapshot_versions: Mapping[str, int] | None = None,
    ) -> SparklessSession:
        effective_configuration = _validate_configuration(configuration)
        from sparkless.sql import SparkSession

        native_session = SparkSession(app_name=app_name)
        try:
            provider = _BrickHouseExternalCatalogProvider(self._context, snapshot_versions)
            for key, value in effective_configuration.items():
                native_session.conf.set(key, value)
            native_session.set_external_catalog_provider(provider)
        except BaseException:
            try:
                native_session.set_external_catalog_provider(None)
            finally:
                native_session.stop()
            raise
        return SparklessSession(
            native_session,
            provider,
            effective_configuration,
            compute_profile,
            runtime_profile,
        )


def _validate_configuration(
    configuration: Mapping[str, str] | None,
) -> dict[str, str]:
    values = dict(_DEFAULT_CONFIGURATION)
    if configuration is None:
        return values
    if not isinstance(configuration, Mapping):
        raise UnsupportedSparklessConfigurationError(
            "Sparkless configuration must be a mapping of string keys and values"
        )
    for key, value in configuration.items():
        if not isinstance(key, str) or key not in _CONFIGURATION_KEYS:
            raise UnsupportedSparklessConfigurationError(
                f"unsupported Sparkless session setting: {key!r}"
            )
        if not isinstance(value, str) or any(ord(character) < 32 for character in value):
            raise UnsupportedSparklessConfigurationError(
                f"Sparkless setting {key!r} must have a plain string value"
            )
        values[key] = value
    return values


def _sparkless_schema(schema_json: str) -> Any:
    """Convert the selected Delta type envelope through public Sparkless types."""
    from sparkless.sql import types as T

    try:
        decoded = json.loads(schema_json)
    except (TypeError, json.JSONDecodeError) as error:
        raise UnsupportedDeltaSchemaError("Delta schema is not valid JSON") from error
    if not isinstance(decoded, dict) or decoded.get("type") != "struct":
        raise UnsupportedDeltaSchemaError("Delta schema must have a struct root")
    fields = decoded.get("fields")
    if not isinstance(fields, list):
        raise UnsupportedDeltaSchemaError("Delta struct schema must contain fields")
    return T.StructType([_sparkless_field(field, T) for field in fields])


def _sparkless_field(field: Any, types: Any) -> Any:
    if not isinstance(field, dict):
        raise UnsupportedDeltaSchemaError("Delta schema fields must be objects")
    name = field.get("name")
    nullable = field.get("nullable")
    metadata = field.get("metadata", {})
    if not isinstance(name, str) or not name or not isinstance(nullable, bool):
        raise UnsupportedDeltaSchemaError("Delta fields require a name and nullable flag")
    if not isinstance(metadata, dict) or metadata:
        raise UnsupportedDeltaSchemaError(
            f"Delta field metadata is outside the selected provider envelope: {name!r}"
        )
    return types.StructField(name, _sparkless_type(field.get("type"), types), nullable)


def _sparkless_type(value: Any, types: Any) -> Any:
    if isinstance(value, str):
        decimal = _DECIMAL_TYPE.fullmatch(value)
        if decimal is not None:
            precision, scale = (int(part) for part in decimal.groups())
            try:
                return types.DecimalType(precision, scale)
            except (TypeError, ValueError) as error:
                raise UnsupportedDeltaSchemaError(f"invalid Delta decimal type: {value}") from error
        primitive_types = {
            "binary": types.BinaryType,
            "boolean": types.BooleanType,
            "date": types.DateType,
            "double": types.DoubleType,
            "integer": types.IntegerType,
            "long": types.LongType,
            "short": types.ShortType,
            "string": types.StringType,
            "timestamp": types.TimestampType,
        }
        constructor = primitive_types.get(value.lower())
        if constructor is not None:
            return constructor()
        raise UnsupportedDeltaSchemaError(f"unsupported Delta type: {value}")

    if not isinstance(value, dict):
        raise UnsupportedDeltaSchemaError("Delta nested types must be strings or objects")
    kind = value.get("type")
    if kind == "struct":
        fields = value.get("fields")
        if not isinstance(fields, list):
            raise UnsupportedDeltaSchemaError("nested Delta struct must contain fields")
        return types.StructType([_sparkless_field(field, types) for field in fields])
    if kind == "array":
        contains_null = value.get("containsNull")
        if contains_null is not True:
            raise UnsupportedDeltaSchemaError(
                "Sparkless provider wire format does not retain non-null array elements"
            )
        return types.ArrayType(_sparkless_type(value.get("elementType"), types), True)
    raise UnsupportedDeltaSchemaError(f"unsupported Delta nested type: {kind!r}")
