"""Profile-bound Sparkless sessions for managed Python workers."""

from __future__ import annotations

import base64
import threading
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from brickhouse.runtime.worker_auth import MAX_WORKER_ARROW_BYTES
from brickhouse.runtime.worker_services import WorkerServiceClient, WorkerServiceError


class WorkerSparkSessionError(RuntimeError):
    """Base error for the worker Sparkless session boundary."""


class WorkerSparkSessionConfigurationError(WorkerSparkSessionError):
    """A worker profile or Spark setting is outside the local contract."""


class UnsupportedWorkerSparkOperationError(WorkerSparkSessionError):
    """The requested Spark/JVM operation has no worker-native implementation."""


_SERVERLESS_PROFILE = "serverless-local-v1"
_CLASSIC_PROFILES = {
    "classic-standard-local-v1": "DATA_SECURITY_MODE_STANDARD",
    "classic-dedicated-local-v1": "DATA_SECURITY_MODE_DEDICATED",
}
_ENGINE_PROFILE = "spark35-local-v1"
_ANSI_SETTING = "spark.sql.ansi.enabled"
_CASE_SETTING = "spark.sql.caseSensitive"
_TIMEZONE_SETTING = "spark.sql.session.timeZone"
_DEFAULT_CONFIGURATION = {
    _ANSI_SETTING: "true",
    _CASE_SETTING: "false",
    _TIMEZONE_SETTING: "UTC",
}
_SUPPORTED_CONFIGURATION_KEYS = frozenset(_DEFAULT_CONFIGURATION)
_BLOCKED_SESSION_NAMES = frozenset(
    {
        "sparkContext",
        "spark_context",
        "sc",
        "_sc",
        "_jsc",
        "_jsparkSession",
        "_jvm",
        "_gateway",
        "java_gateway",
        "newSession",
        "read",
        "read_delta",
        "read_delta_with_version",
        "catalog",
    }
)
_BLOCKED_FRAME_NAMES = frozenset(
    {
        "rdd",
        "write",
        "writeTo",
        "write_to",
        "_jdf",
        "_jvm",
        "_sc",
    }
)

_SESSION_CONFIGURATION: Mapping[str, object] | None = None
_ACTIVE_MANAGER: WorkerSparkSessionManager | None = None
_WORKER_SERVICES: WorkerServiceClient | None = None


def configure_worker_spark_session(
    context: Mapping[str, object],
    worker_services: WorkerServiceClient | None = None,
) -> None:
    """Retain the immutable profile snapshot without loading Sparkless yet."""
    global _SESSION_CONFIGURATION, _ACTIVE_MANAGER, _WORKER_SERVICES
    if _ACTIVE_MANAGER is not None:
        raise WorkerSparkSessionConfigurationError("a worker Sparkless session is already active")
    _WORKER_SERVICES = worker_services

    execution_context = context.get("execution_context")
    profile_context = execution_context if isinstance(execution_context, Mapping) else context
    profile_keys = {
        "compute_mode",
        "profile",
        "runtime_profile",
        "access_mode",
        "effective_settings",
    }
    present = profile_keys.intersection(profile_context)
    if not present:
        _SESSION_CONFIGURATION = None
        return
    _SESSION_CONFIGURATION = MappingProxyType(dict(profile_context))


def validate_worker_spark_session_context(context: Mapping[str, object]) -> None:
    """Validate a resolved profile without importing or creating Sparkless state."""
    execution_context = context.get("execution_context")
    profile_context = execution_context if isinstance(execution_context, Mapping) else context
    _resolve_session_configuration(profile_context)


def _resolve_session_configuration(
    profile_context: Mapping[str, object],
) -> Mapping[str, object]:
    mode = profile_context.get("compute_mode")
    profile = profile_context.get("profile")
    runtime_profile = profile_context.get("runtime_profile")
    access_mode = profile_context.get("access_mode")
    settings = profile_context.get("effective_settings")
    if (
        not isinstance(mode, str)
        or not isinstance(profile, str)
        or (runtime_profile is not None and not isinstance(runtime_profile, str))
        or (access_mode is not None and not isinstance(access_mode, str))
        or not isinstance(settings, Mapping)
    ):
        raise WorkerSparkSessionConfigurationError(
            "worker compute profile context is incomplete or malformed"
        )

    normalized_settings: dict[str, str] = {}
    for key, value in settings.items():
        if (
            not isinstance(key, str)
            or key not in _SUPPORTED_CONFIGURATION_KEYS
            or not isinstance(value, str)
            or not value
            or any(ord(character) < 32 for character in value)
        ):
            raise WorkerSparkSessionConfigurationError(
                f"worker Spark setting is unsupported or malformed: {key!r}"
            )
        normalized_settings[key] = value

    if mode == "serverless" and profile == _SERVERLESS_PROFILE:
        if runtime_profile is not None or access_mode is not None:
            raise WorkerSparkSessionConfigurationError(
                "serverless worker profile cannot include classic runtime or access settings"
            )
        if (
            normalized_settings.get(_ANSI_SETTING) != "true"
            or normalized_settings.get(_TIMEZONE_SETTING) != "UTC"
        ):
            raise WorkerSparkSessionConfigurationError(
                "serverless worker profile requires ANSI mode and UTC timezone defaults"
            )
    elif mode == "classic" and profile in _CLASSIC_PROFILES:
        if runtime_profile != _ENGINE_PROFILE:
            raise WorkerSparkSessionConfigurationError(
                "classic worker runtime is not mapped to spark35-local-v1"
            )
        if access_mode != _CLASSIC_PROFILES[profile]:
            raise WorkerSparkSessionConfigurationError(
                "classic worker access mode does not match the selected local profile"
            )
        if _ANSI_SETTING not in normalized_settings or _TIMEZONE_SETTING not in normalized_settings:
            raise WorkerSparkSessionConfigurationError(
                "classic worker profile requires explicit ANSI and timezone settings"
            )
    else:
        raise WorkerSparkSessionConfigurationError(
            f"unsupported worker Spark compute profile: {mode!r}/{profile!r}"
        )

    return MappingProxyType(
        {
            "compute_mode": mode,
            "profile": profile,
            "runtime_profile": runtime_profile,
            "access_mode": access_mode,
            "settings": MappingProxyType(normalized_settings),
        }
    )


def activate_worker_spark_session() -> WorkerSparkSessionManager | None:
    """Create the native session on first supported PySpark SQL import."""
    global _ACTIVE_MANAGER
    if _SESSION_CONFIGURATION is None:
        return None
    if _ACTIVE_MANAGER is None:
        effective_configuration = _resolve_session_configuration(_SESSION_CONFIGURATION)
        _ACTIVE_MANAGER = WorkerSparkSessionManager(effective_configuration)
    return _ACTIVE_MANAGER


def close_worker_spark_session() -> None:
    """Close the active native session on its creating worker thread."""
    manager = _ACTIVE_MANAGER
    if manager is not None:
        manager.close()


def _require_manager() -> WorkerSparkSessionManager:
    manager = activate_worker_spark_session()
    if manager is None:
        raise WorkerSparkSessionConfigurationError(
            "SparkSession creation requires a resolved serverless or classic worker profile"
        )
    return manager


class _BuilderDescriptor:
    def __get__(self, instance: object, owner: type[object]) -> WorkerSparkSessionBuilder:
        del instance, owner
        return _require_manager().builder


class WorkerSparkSession:
    """Import-compatible SparkSession constructor with worker-owned builder state."""

    builder = _BuilderDescriptor()

    def __new__(cls, *args: object, **kwargs: object) -> WorkerSparkSession:
        del cls, args, kwargs
        raise UnsupportedWorkerSparkOperationError(
            "construct sessions with SparkSession.builder.getOrCreate()"
        )

    @classmethod
    def getActiveSession(cls) -> WorkerSparkSessionFacade | None:
        del cls
        manager = activate_worker_spark_session()
        return None if manager is None else manager.session


class WorkerSparkSessionBuilder:
    """Subset of SparkSession builder supported by the local worker profile."""

    def __init__(self, manager: WorkerSparkSessionManager) -> None:
        self._manager = manager

    def config(self, key: object, value: object = None) -> WorkerSparkSessionBuilder:
        if not isinstance(key, str) or not isinstance(value, str):
            raise WorkerSparkSessionConfigurationError(
                "worker SparkSession.builder.config requires one string key and value"
            )
        self._manager.set_configuration(key, value)
        return self

    def getOrCreate(self) -> WorkerSparkSessionFacade:
        return self._manager.session

    def get_or_create(self) -> WorkerSparkSessionFacade:
        return self.getOrCreate()

    def appName(self, name: object) -> WorkerSparkSessionBuilder:
        del name
        raise UnsupportedWorkerSparkOperationError(
            "SparkSession.builder.appName is not mapped by the local worker profile"
        )

    def app_name(self, name: object) -> WorkerSparkSessionBuilder:
        return self.appName(name)

    def master(self, master: object) -> WorkerSparkSessionBuilder:
        del master
        raise UnsupportedWorkerSparkOperationError(
            "SparkSession.builder.master is unsupported without a Spark driver"
        )

    def option(self, key: object, value: object) -> WorkerSparkSessionBuilder:
        del key, value
        raise UnsupportedWorkerSparkOperationError(
            "SparkSession.builder.option is not mapped by the local worker profile"
        )


class WorkerSparkSessionManager:
    def __init__(self, context: Mapping[str, object]) -> None:
        raw_settings = context["settings"]
        if not isinstance(raw_settings, Mapping):  # internal defensive check
            raise WorkerSparkSessionConfigurationError("worker Spark settings are malformed")

        configuration = dict(_DEFAULT_CONFIGURATION)
        configuration.update(raw_settings)
        self._configuration = configuration
        self._owner_thread = threading.get_ident()
        self._closed = False
        try:
            from sparkless.sql import SparkSession as NativeSparkSession
        except ImportError as error:
            raise WorkerSparkSessionConfigurationError(
                "the selected worker profile requires the pinned Sparkless Python package"
            ) from error

        native_session: Any | None = None
        try:
            native_session = NativeSparkSession(app_name="brickhouse-worker")
            if _WORKER_SERVICES is not None:
                native_session.set_external_catalog_provider(
                    _WorkerExternalCatalogProvider(_WORKER_SERVICES)
                )
            for key, value in configuration.items():
                native_session.conf.set(key, value)
        except BaseException as error:
            if native_session is not None:
                try:
                    native_session.stop()
                except BaseException:
                    pass
            raise WorkerSparkSessionConfigurationError(
                "Sparkless could not apply the resolved worker profile settings"
            ) from error
        self._native_session = native_session
        self._session = WorkerSparkSessionFacade(self)
        self.builder = WorkerSparkSessionBuilder(self)

    @property
    def session(self) -> WorkerSparkSessionFacade:
        self._require_owner()
        return self._session

    def _require_owner(self) -> None:
        if threading.get_ident() != self._owner_thread:
            raise UnsupportedWorkerSparkOperationError(
                "Sparkless worker sessions must be used on their creating thread"
            )
        if self._closed:
            raise WorkerSparkSessionError("Sparkless worker session is closed")

    def set_configuration(self, key: str, value: str) -> None:
        self._require_owner()
        if key not in _SUPPORTED_CONFIGURATION_KEYS:
            raise WorkerSparkSessionConfigurationError(
                f"worker Spark setting is unsupported: {key!r}"
            )
        if not value or any(ord(character) < 32 for character in value):
            raise WorkerSparkSessionConfigurationError(
                f"worker Spark setting {key!r} must be a non-empty plain string"
            )
        self._native_session.conf.set(key, value)
        self._configuration[key] = value

    def get_configuration(self, key: str, default: str | None = None) -> str:
        self._require_owner()
        if key not in _SUPPORTED_CONFIGURATION_KEYS:
            raise WorkerSparkSessionConfigurationError(
                f"worker Spark setting is unsupported: {key!r}"
            )
        if default is None:
            return str(self._native_session.conf.get(key))
        return str(self._native_session.conf.get(key, default))

    def wrap(self, value: Any) -> Any:
        if _is_dataframe(value):
            return _WorkerDataFrameFacade(self, value)
        if "GroupedData" in type(value).__name__:
            return WorkerGroupedDataFacade(self, value)
        return value

    def call(self, operation: str, *args: Any, **kwargs: Any) -> Any:
        self._require_owner()
        native_args = tuple(_unwrap(argument) for argument in args)
        native_kwargs = {key: _unwrap(value) for key, value in kwargs.items()}
        result = getattr(self._native_session, operation)(*native_args, **native_kwargs)
        return self.wrap(result)

    def resolve_registered_path(self, path: str) -> str | None:
        self._require_owner()
        if _WORKER_SERVICES is None:
            raise UnsupportedWorkerSparkOperationError(
                "registered table paths require an authenticated job worker service"
            )
        response = _WORKER_SERVICES.call("tables", "resolve_path", {"path": path})
        if response is None:
            return None
        if not isinstance(response, dict) or set(response) != {
            "identifier",
            "table_id",
            "canonical_location",
            "schema_json",
            "provider",
            "version",
        }:
            raise WorkerSparkSessionError("authenticated worker path descriptor is malformed")
        identifier = response["identifier"]
        if not isinstance(identifier, str) or not identifier:
            raise WorkerSparkSessionError("authenticated worker path descriptor is invalid")
        return identifier

    def resolve_registered_name(self, identifier: str) -> str | None:
        self._require_owner()
        if _WORKER_SERVICES is None:
            raise UnsupportedWorkerSparkOperationError(
                "registered tables require an authenticated job worker service"
            )
        response = _WORKER_SERVICES.call("tables", "resolve", {"identifier": identifier})
        if response is None:
            return None
        if not isinstance(response, dict) or set(response) != {
            "identifier",
            "table_id",
            "canonical_location",
            "schema_json",
            "provider",
            "version",
        }:
            raise WorkerSparkSessionError("authenticated worker table descriptor is malformed")
        resolved_identifier = response["identifier"]
        if not isinstance(resolved_identifier, str) or not resolved_identifier:
            raise WorkerSparkSessionError("authenticated worker table descriptor is invalid")
        return resolved_identifier

    def is_registered_table_path(self, path: str) -> bool:
        self._require_owner()
        if _WORKER_SERVICES is None:
            raise UnsupportedWorkerSparkOperationError(
                "registered table paths require an authenticated job worker service"
            )
        result = _WORKER_SERVICES.call("tables", "is_registered_path", {"path": path})
        if type(result) is not bool:
            raise WorkerSparkSessionError(
                "authenticated worker path lookup returned an invalid result"
            )
        return result

    def close(self) -> None:
        global _ACTIVE_MANAGER
        if self._closed:
            return
        if threading.get_ident() != self._owner_thread:
            raise UnsupportedWorkerSparkOperationError(
                "Sparkless worker sessions must be closed on their creating thread"
            )
        first_error: BaseException | None = None
        try:
            self._native_session.set_external_catalog_provider(None)
        except BaseException as error:
            first_error = error
        try:
            self._native_session.stop()
        except BaseException as error:
            if first_error is None:
                first_error = error
        finally:
            self._closed = True
            if _ACTIVE_MANAGER is self:
                _ACTIVE_MANAGER = None
        if first_error is not None:
            raise first_error


class _WorkerConfigurationFacade:
    def __init__(self, manager: WorkerSparkSessionManager) -> None:
        self._manager = manager

    def get(self, key: str, default: str | None = None) -> str:
        return self._manager.get_configuration(key, default)

    def set(self, key: str, value: str) -> None:
        self._manager.set_configuration(key, value)


class WorkerSparkSessionFacade:
    """Bounded public session operations without JVM or unregistered I/O access."""

    def __init__(self, manager: WorkerSparkSessionManager) -> None:
        self._manager = manager

    @property
    def conf(self) -> _WorkerConfigurationFacade:
        self._manager._require_owner()
        return _WorkerConfigurationFacade(self._manager)

    @property
    def sparkContext(self) -> Any:
        raise UnsupportedWorkerSparkOperationError(
            "SparkContext and RDD execution are unavailable in the JVM-free worker"
        )

    def createDataFrame(self, *args: Any, **kwargs: Any) -> Any:
        return self._manager.call("createDataFrame", *args, **kwargs)

    def range(self, *args: Any, **kwargs: Any) -> Any:
        return self._manager.call("range", *args, **kwargs)

    def table(self, identifier: object) -> Any:
        """Read one registered table through the authenticated parent catalog."""
        self._manager._require_owner()
        if not isinstance(identifier, str):
            raise WorkerSparkSessionConfigurationError(
                "SparkSession.table requires one registered table identifier"
            )
        if _WORKER_SERVICES is None:
            raise UnsupportedWorkerSparkOperationError(
                "SparkSession.table requires an authenticated job worker service"
            )
        return self._manager.call("table", identifier)

    def _resolve_registered_path(self, path: str) -> str | None:
        return self._manager.resolve_registered_path(path)

    def _resolve_registered_name(self, identifier: str) -> str | None:
        return self._manager.resolve_registered_name(identifier)

    def _is_registered_table_path(self, path: str) -> bool:
        return self._manager.is_registered_table_path(path)

    def sql(self, query: str) -> Any:
        self._manager._require_owner()
        if not isinstance(query, str):
            raise TypeError("Sparkless SQL query must be a string")
        normalized = query.lstrip().lower()
        if ";" in query:
            raise UnsupportedWorkerSparkOperationError(
                "multiple or terminated SQL statements are unsupported in worker sessions"
            )
        if not (
            normalized.startswith("select ")
            or normalized.startswith("select\n")
            or normalized.startswith("with ")
            or normalized.startswith("with\n")
            or normalized.startswith("create temp view ")
            or normalized.startswith("create temporary view ")
        ):
            raise UnsupportedWorkerSparkOperationError(
                "worker SQL permits read queries and temporary views only"
            )
        return self._manager.call("sql", query)

    def stop(self) -> None:
        self._manager.close()

    def __getattr__(self, name: str) -> Any:
        self._manager._require_owner()
        if name in _BLOCKED_SESSION_NAMES:
            raise UnsupportedWorkerSparkOperationError(
                f"SparkSession.{name} is unsupported by the JVM-free worker profile"
            )
        raise UnsupportedWorkerSparkOperationError(
            f"SparkSession.{name} is not in the selected worker API subset"
        )


class _WorkerDataFrameFacade:
    def __init__(self, manager: WorkerSparkSessionManager, dataframe: Any) -> None:
        self._manager = manager
        self._dataframe = dataframe

    def __getattr__(self, name: str) -> Any:
        self._manager._require_owner()
        if name == "write":
            return _WorkerDataFrameWriterFacade(self._manager, self._dataframe.write)
        if name in _BLOCKED_FRAME_NAMES:
            raise UnsupportedWorkerSparkOperationError(
                f"DataFrame.{name} is unsupported by the JVM-free worker profile"
            )
        try:
            operation = getattr(self._dataframe, name)
        except AttributeError:
            raise
        if not callable(operation):
            return operation

        def invoke(*args: Any, **kwargs: Any) -> Any:
            self._manager._require_owner()
            result = operation(
                *tuple(_unwrap(argument) for argument in args),
                **{key: _unwrap(value) for key, value in kwargs.items()},
            )
            return self._manager.wrap(result)

        return invoke


class _WorkerDataFrameWriterFacade:
    """Expose only a registered Delta append through the parent table service."""

    def __init__(self, manager: WorkerSparkSessionManager, writer: Any) -> None:
        self._manager = manager
        self._writer = writer
        self._format: str | None = None
        self._mode: str | None = None

    def format(self, source: object) -> _WorkerDataFrameWriterFacade:
        self._manager._require_owner()
        if not isinstance(source, str) or source.casefold() != "delta":
            raise UnsupportedWorkerSparkOperationError(
                "worker DataFrameWriter supports only the Delta format"
            )
        self._format = "delta"
        self._writer.format("delta")
        return self

    def mode(self, save_mode: object) -> _WorkerDataFrameWriterFacade:
        self._manager._require_owner()
        value = getattr(save_mode, "value", save_mode)
        if not isinstance(value, str) or value.casefold() != "append":
            raise UnsupportedWorkerSparkOperationError(
                "worker DataFrameWriter supports only append mode"
            )
        self._mode = "append"
        self._writer.mode("append")
        return self

    def saveAsTable(self, identifier: object) -> None:
        self._manager._require_owner()
        if self._format != "delta" or self._mode != "append":
            raise UnsupportedWorkerSparkOperationError(
                "worker writes require format('delta').mode('append')"
            )
        if not isinstance(identifier, str) or not identifier:
            raise UnsupportedWorkerSparkOperationError(
                "worker DataFrameWriter.saveAsTable requires a table identifier"
            )
        self._writer.saveAsTable(identifier)

    def save_as_table(self, identifier: object) -> None:
        """Support the Sparkless snake-case spelling under the same restrictions."""
        self.saveAsTable(identifier)

    def __getattr__(self, name: str) -> Any:
        self._manager._require_owner()
        raise UnsupportedWorkerSparkOperationError(
            f"DataFrameWriter.{name} is unsupported by the JVM-free worker profile"
        )


class WorkerGroupedDataFacade:
    def __init__(self, manager: WorkerSparkSessionManager, grouped: Any) -> None:
        self._manager = manager
        self._grouped = grouped

    def __getattr__(self, name: str) -> Any:
        self._manager._require_owner()
        operation = getattr(self._grouped, name)
        if not callable(operation):
            return operation

        def invoke(*args: Any, **kwargs: Any) -> Any:
            self._manager._require_owner()
            result = operation(
                *tuple(_unwrap(argument) for argument in args),
                **{key: _unwrap(value) for key, value in kwargs.items()},
            )
            return self._manager.wrap(result)

        return invoke


class _WorkerExternalCatalogProvider:
    """Resolve registered table descriptors through the attempt-scoped service."""

    def __init__(self, worker_services: WorkerServiceClient) -> None:
        self._worker_services = worker_services

    def resolve_table(self, identifier: str) -> Any | None:
        if not isinstance(identifier, str) or not identifier:
            raise WorkerSparkSessionConfigurationError(
                "Sparkless external catalog requires a non-empty table identifier"
            )
        try:
            response = self._worker_services.call("tables", "resolve", {"identifier": identifier})
        except (PermissionError, ValueError, WorkerServiceError) as error:
            raise WorkerSparkSessionError("authenticated worker table resolution failed") from error
        if response is None:
            return None
        if not isinstance(response, dict) or set(response) != {
            "identifier",
            "table_id",
            "canonical_location",
            "schema_json",
            "provider",
            "version",
        }:
            raise WorkerSparkSessionError("authenticated worker table descriptor is malformed")
        identifier = response["identifier"]
        table_id = response["table_id"]
        canonical_location = response["canonical_location"]
        schema_json = response["schema_json"]
        provider = response["provider"]
        version = response["version"]
        if (
            not isinstance(identifier, str)
            or not identifier
            or not isinstance(table_id, str)
            or not table_id
            or not isinstance(canonical_location, str)
            or not canonical_location
            or not isinstance(schema_json, str)
            or not isinstance(provider, str)
            or provider != "brickhouse-local-delta"
            or type(version) is not int
            or version < 0
        ):
            raise WorkerSparkSessionError("authenticated worker table descriptor is invalid")

        try:
            from sparkless.external_catalog import ExternalTableDescriptor

            from brickhouse.engines.sparkless import _sparkless_schema

            descriptor = ExternalTableDescriptor(
                table_id=table_id,
                canonical_location=canonical_location,
                schema=_sparkless_schema(schema_json),
                provider=provider,
                version=version,
            )
            descriptor.validate()
        except Exception as error:
            raise WorkerSparkSessionError(
                "authenticated worker table descriptor is unsupported by Sparkless"
            ) from error
        return descriptor

    def write_table(self, request: Any) -> Any:
        """Request one bounded Delta append through the authenticated parent service."""
        if not isinstance(request.identifier, str) or not request.identifier:
            raise WorkerSparkSessionError("authenticated worker table write has no identifier")
        mode = getattr(request.mode, "value", request.mode)
        selected_format = request.format.casefold() if isinstance(request.format, str) else None
        if mode != "append" or selected_format != "delta":
            raise UnsupportedWorkerSparkOperationError(
                "worker table writes support only Delta append mode"
            )
        if request.options or request.partition_by or request.batches is not None:
            raise UnsupportedWorkerSparkOperationError(
                "worker table writes do not support options, partitions, or batch streams"
            )
        if request.data is None or not isinstance(request.data, bytes):
            raise WorkerSparkSessionError("authenticated worker Arrow write payload is missing")
        if len(request.data) > MAX_WORKER_ARROW_BYTES:
            raise WorkerSparkSessionError(
                f"worker Arrow write exceeds the {MAX_WORKER_ARROW_BYTES}-byte limit"
            )
        descriptor = request.table
        if (
            descriptor.provider != "brickhouse-local-delta"
            or not isinstance(descriptor.table_id, str)
            or not descriptor.table_id
            or not isinstance(descriptor.canonical_location, str)
            or not descriptor.canonical_location
            or type(descriptor.version) is not int
            or descriptor.version < 0
        ):
            raise WorkerSparkSessionError("authenticated worker table write descriptor is invalid")
        try:
            from pyarrow import ipc  # type: ignore[import-untyped]

            arrow_table = ipc.open_stream(request.data).read_all()
        except Exception as error:
            raise WorkerSparkSessionError(
                "authenticated worker Arrow write payload is invalid"
            ) from error
        from brickhouse.runtime.worker_auth import MAX_WORKER_ARROW_ROWS

        if arrow_table.num_rows > MAX_WORKER_ARROW_ROWS:
            raise WorkerSparkSessionError(
                f"worker Arrow write exceeds the {MAX_WORKER_ARROW_ROWS}-row limit"
            )
        from sparkless.external_catalog import ExternalTableDescriptor, ExternalTableWriteResult

        payload = {
            "identifier": request.identifier,
            "table_id": descriptor.table_id,
            "canonical_location": descriptor.canonical_location,
            "expected_version": descriptor.version,
            "format": "delta",
            "mode": "append",
            "data_base64": base64.b64encode(request.data).decode("ascii"),
        }
        try:
            response = self._worker_services.call("tables", "write", payload)
        except (PermissionError, ValueError, WorkerServiceError) as error:
            raise WorkerSparkSessionError("authenticated worker table write failed") from error
        if not isinstance(response, dict) or set(response) != {
            "identifier",
            "table_id",
            "canonical_location",
            "schema_json",
            "provider",
            "version",
            "committed_version",
        }:
            raise WorkerSparkSessionError("authenticated worker table write receipt is malformed")
        try:
            schema_json = response["schema_json"]
            if not isinstance(schema_json, str):
                raise ValueError("schema_json must be a string")
            from brickhouse.engines.sparkless import _sparkless_schema

            committed_descriptor = ExternalTableDescriptor(
                table_id=response["table_id"],
                canonical_location=response["canonical_location"],
                schema=_sparkless_schema(schema_json),
                provider=response["provider"],
                version=response["version"],
            )
            committed_descriptor.validate()
            committed_version = response["committed_version"]
            if type(committed_version) is not int or committed_version < 0:
                raise ValueError("committed_version must be a non-negative integer")
            if not isinstance(response["identifier"], str) or not response["identifier"]:
                raise ValueError("identifier must be a non-empty string")
            receipt = ExternalTableWriteResult(committed_descriptor, committed_version)
            receipt.validate_against(request.table)
        except Exception as error:
            raise WorkerSparkSessionError(
                "authenticated worker table write receipt is unsupported by Sparkless"
            ) from error
        return receipt


def _is_dataframe(value: Any) -> bool:
    return (
        callable(getattr(value, "collect", None))
        and hasattr(value, "columns")
        and hasattr(value, "schema")
    )


def _unwrap(value: Any) -> Any:
    if isinstance(value, _WorkerDataFrameFacade):
        return value._dataframe
    if isinstance(value, WorkerGroupedDataFacade):
        return value._grouped
    if isinstance(value, tuple):
        return tuple(_unwrap(item) for item in value)
    if isinstance(value, list):
        return [_unwrap(item) for item in value]
    if isinstance(value, dict):
        return {key: _unwrap(item) for key, item in value.items()}
    return value
