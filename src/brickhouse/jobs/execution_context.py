"""Pre-dispatch resolution for the selected serverless and classic task profiles."""

from __future__ import annotations

import importlib.metadata
import json
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import PurePosixPath
from typing import cast

from brickhouse.compute.models import (
    ClusterConfiguration,
    ComputeNotFoundError,
    ComputeRecord,
    ComputeResourceKind,
    ComputeState,
    InvalidComputeRequestError,
    UnsupportedComputeEffectError,
    normalize_cluster_configuration,
)
from brickhouse.jobs.models import MAX_TASKS_PER_JOB
from brickhouse.runtime import MAX_WORKER_TIMEOUT_SECONDS
from brickhouse.workspace.context import WorkspaceContext

_IDENTIFIER_RE = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,127}\Z")
_REQUIREMENT_RE = re.compile(r"([A-Za-z0-9_.-]+)==([A-Za-z0-9_.+-]+)\Z")
_CREDENTIAL_VALUE_RE = re.compile(
    r"(?i)(?:bearer\s+\S+|(?:access[_-]?token|api[_-]?key|client[_-]?secret|"
    r"password|secret|token)\s*[:=]\s*\S+)"
)
_CREDENTIAL_FLAG_RE = re.compile(
    r"(?i)^--?(?:access[_-]?token|api[_-]?key|client[_-]?secret|password|secret|token)$"
)
_SUPPORTED_ENVIRONMENT_VERSION = "1"
_SUPPORTED_ENVIRONMENT_DEPENDENCIES = {
    "deltalake==1.2.1": ("deltalake", "1.2.1"),
    "pyarrow==23.0.1": ("pyarrow", "23.0.1"),
}
_SESSION_DEFAULTS = {
    "spark.sql.ansi.enabled": "true",
    "spark.sql.session.timeZone": "UTC",
}
_TASK_TYPE_KEYS = frozenset(
    {
        "ai_runtime_task",
        "alert_task",
        "clean_rooms_notebook_task",
        "condition_task",
        "dashboard_task",
        "dbt_cloud_task",
        "dbt_platform_task",
        "dbt_task",
        "for_each_task",
        "gen_ai_compute_task",
        "pipeline_task",
        "power_bi_task",
        "python_operator_task",
        "python_wheel_task",
        "run_job_task",
        "spark_jar_task",
        "spark_submit_task",
        "sql_task",
    }
)
_TASK_FIELDS = frozenset(
    {
        "task_key",
        "description",
        "depends_on",
        "spark_python_task",
        "notebook_task",
        "environment_key",
        "existing_cluster_id",
        "job_cluster_key",
        "new_cluster",
        "disable_auto_optimization",
        "max_retries",
        "min_retry_interval_millis",
        "retry_on_timeout",
        "timeout_seconds",
        "libraries",
        "environment_variables_key",
        "compute",
        "disabled",
        "run_if",
        "condition_task",
        "for_each_task",
    }
    | _TASK_TYPE_KEYS
)


class ComputeMode(StrEnum):
    SERVERLESS = "serverless"
    CLASSIC = "classic"


class ExecutionContextResolutionError(ValueError):
    """Base error for an unsupported or invalid task execution context."""


class InvalidExecutionContextError(ExecutionContextResolutionError):
    """The selected task context is malformed or internally inconsistent."""


class UnsupportedExecutionContextError(ExecutionContextResolutionError):
    """The selected request asks for a local effect not implemented by this profile."""


class ExecutionContextNotFoundError(ExecutionContextResolutionError):
    """A referenced local cluster or named environment does not exist."""


@dataclass(frozen=True, slots=True)
class ResolvedTaskContext:
    """Immutable execution profile snapshot for one submitted Python or notebook task."""

    task_key: str
    run_id: int | None
    task_type: str
    compute_mode: ComputeMode
    profile: str
    runtime_profile: str | None
    access_mode: str | None
    compute_resource_kind: ComputeResourceKind | None
    compute_id: str | None
    compute_key: str | None
    compute_generation: int | None
    autostart: bool
    cluster_configuration_json: str | None = field(repr=False)
    environment_key: str | None
    environment_source: str | None
    environment_profile: str | None
    environment_version: str | None
    dependencies: tuple[str, ...]
    performance_target: str | None
    disable_auto_optimization: bool | None
    timeout_seconds: int | None
    effective_settings_json: str = field(repr=False)
    retry_settings_json: str
    task_arguments_json: str = field(repr=False)

    @property
    def effective_settings(self) -> dict[str, str]:
        """Return a detached copy of the resolved configuration defaults."""
        return cast(dict[str, str], json.loads(self.effective_settings_json))

    @property
    def retry_settings(self) -> dict[str, int | bool | None]:
        """Return a detached copy of the task retry settings."""
        return cast(dict[str, int | bool | None], json.loads(self.retry_settings_json))

    @property
    def task_arguments(self) -> dict[str, object]:
        """Return a detached copy of the validated script/notebook arguments."""
        return cast(dict[str, object], json.loads(self.task_arguments_json))


class ExecutionContextResolver:
    """Resolve only explicitly mapped local profiles before a worker is launched."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._context = context

    def resolve_job(
        self,
        tasks: Sequence[Mapping[str, object]],
        *,
        run_id: int | None = None,
        job_clusters: Sequence[Mapping[str, object]] = (),
        environments: Sequence[Mapping[str, object]] = (),
        notebook_environments: Mapping[str, Mapping[str, object]] | None = None,
        performance_target: str | None = None,
    ) -> tuple[ResolvedTaskContext, ...]:
        """Resolve a job's supported task selectors without provisioning or dispatching."""
        if run_id is not None and (type(run_id) is not int or run_id < 1):
            raise InvalidExecutionContextError("run_id must be a positive integer")
        task_list = _materialize_objects(tasks, label="tasks", minimum=1, maximum=MAX_TASKS_PER_JOB)
        cluster_definitions = _cluster_definitions(job_clusters)
        environment_definitions = _environment_definitions(environments)
        task_keys = [_required_identifier(task, "task_key") for task in task_list]
        if len(set(task_keys)) != len(task_keys):
            raise InvalidExecutionContextError("task_key values must be unique")
        notebook_environment_definitions: dict[str, tuple[str, tuple[str, ...]]] = {}
        if notebook_environments is not None:
            if not isinstance(notebook_environments, Mapping):
                raise InvalidExecutionContextError("notebook environments must be a mapping")
            for task_key, specification in notebook_environments.items():
                if task_key not in task_keys or not isinstance(specification, Mapping):
                    raise InvalidExecutionContextError(
                        "notebook environment metadata must belong to a submitted task"
                    )
                mapped = _environment_definitions(
                    [{"environment_key": "notebook_environment", "spec": specification}]
                )
                notebook_environment_definitions[task_key] = mapped["notebook_environment"]
        shared_usage: dict[str, int] = {}
        for task in task_list:
            key = task.get("job_cluster_key")
            if key is not None:
                if not isinstance(key, str) or not _IDENTIFIER_RE.fullmatch(key):
                    raise InvalidExecutionContextError(
                        "job_cluster_key must be a bounded identifier"
                    )
                shared_usage[key] = shared_usage.get(key, 0) + 1

        results: list[ResolvedTaskContext] = []
        for task, task_key in zip(task_list, task_keys, strict=True):
            results.append(
                self._resolve_task(
                    task,
                    task_key,
                    run_id=run_id,
                    cluster_definitions=cluster_definitions,
                    environment_definitions=environment_definitions,
                    notebook_environment_definitions=notebook_environment_definitions,
                    shared_usage=shared_usage,
                    performance_target=performance_target,
                )
            )
        return tuple(results)

    def _resolve_task(
        self,
        task: dict[str, object],
        task_key: str,
        *,
        run_id: int | None,
        cluster_definitions: dict[str, ClusterConfiguration],
        environment_definitions: dict[str, tuple[str, tuple[str, ...]]],
        notebook_environment_definitions: dict[str, tuple[str, tuple[str, ...]]],
        shared_usage: dict[str, int],
        performance_target: str | None,
    ) -> ResolvedTaskContext:
        unknown = set(task) - _TASK_FIELDS
        if unknown:
            raise UnsupportedExecutionContextError(
                f"unsupported task field(s): {', '.join(sorted(unknown))}"
            )
        for key in _TASK_TYPE_KEYS:
            if task.get(key) is not None:
                raise UnsupportedExecutionContextError(f"task type {key} is unsupported")
        disabled = task.get("disabled")
        if disabled is not None and type(disabled) is not bool:
            raise InvalidExecutionContextError("disabled must be a boolean")
        if disabled is True:
            raise UnsupportedExecutionContextError(
                "disabled tasks are not dispatched by this profile"
            )
        if task.get("run_if") not in (None, "ALL_SUCCESS"):
            raise UnsupportedExecutionContextError("conditional task scheduling is not implemented")
        if task.get("environment_variables_key") is not None:
            raise UnsupportedExecutionContextError(
                "task environment-variable injection is unsupported"
            )
        libraries = task.get("libraries")
        if libraries not in (None, []):
            raise UnsupportedExecutionContextError("task libraries are not installed at dispatch")
        compute_config = task.get("compute")
        if compute_config not in (None, {}):
            raise UnsupportedExecutionContextError("task compute accelerators are unsupported")
        if "condition_task" in task and task["condition_task"] is not None:
            raise UnsupportedExecutionContextError("condition tasks are not implemented")
        if "for_each_task" in task and task["for_each_task"] is not None:
            raise UnsupportedExecutionContextError("for-each task expansion is not implemented")

        task_type, arguments = _task_arguments(task)
        environment_key = task.get("environment_key")
        if environment_key is not None and (
            not isinstance(environment_key, str) or not _IDENTIFIER_RE.fullmatch(environment_key)
        ):
            raise InvalidExecutionContextError("environment_key must be a bounded identifier")
        selectors = [
            key
            for key in ("new_cluster", "existing_cluster_id", "job_cluster_key")
            if task.get(key) is not None
        ]
        if len(selectors) > 1:
            raise InvalidExecutionContextError("a task may select only one compute resource")

        if environment_key is not None and not selectors:
            return self._resolve_serverless(
                task,
                task_key,
                run_id,
                task_type,
                arguments,
                environment_key,
                environment_definitions,
                performance_target,
                environment_source="job",
            )
        if environment_key is None and not selectors and task_type == "notebook":
            notebook_environment = notebook_environment_definitions.get(task_key)
            if notebook_environment is not None:
                return self._resolve_serverless(
                    task,
                    task_key,
                    run_id,
                    task_type,
                    arguments,
                    None,
                    {},
                    performance_target,
                    environment_source="notebook",
                    selected_environment=notebook_environment,
                )
        if not selectors:
            raise InvalidExecutionContextError(
                "task must select a classic cluster or a mapped serverless environment"
            )
        if environment_key is not None:
            raise UnsupportedExecutionContextError(
                "classic Environments mode is not provisioned; environment_key conflicts "
                "with classic compute"
            )
        if performance_target is not None:
            raise UnsupportedExecutionContextError(
                "performance_target is mapped only for serverless tasks"
            )
        return self._resolve_classic(
            task,
            task_key,
            run_id,
            task_type,
            arguments,
            selectors[0],
            cluster_definitions,
            shared_usage,
        )

    def _resolve_serverless(
        self,
        task: dict[str, object],
        task_key: str,
        run_id: int | None,
        task_type: str,
        arguments: dict[str, object],
        environment_key: str | None,
        environment_definitions: dict[str, tuple[str, tuple[str, ...]]],
        performance_target: str | None,
        *,
        environment_source: str,
        selected_environment: tuple[str, tuple[str, ...]] | None = None,
    ) -> ResolvedTaskContext:
        if task.get("disable_auto_optimization") is not True:
            raise UnsupportedExecutionContextError(
                "serverless tasks must set disable_auto_optimization=true until its retry "
                "behavior is implemented"
            )
        if selected_environment is None and environment_key is not None:
            selected_environment = environment_definitions.get(environment_key)
        if selected_environment is None:
            if environment_source == "notebook":
                raise UnsupportedExecutionContextError(
                    "serverless notebook has no mapped notebook or job environment"
                )
            raise ExecutionContextNotFoundError(
                "serverless environment_key is not defined in the job"
            )
        version, dependencies = selected_environment
        effective_performance = performance_target or "STANDARD"
        if effective_performance != "STANDARD":
            raise UnsupportedExecutionContextError(
                "only the standard serverless performance target is mapped locally"
            )
        retry_settings = _retry_settings(task)
        timeout_seconds = _timeout_seconds(task)
        return ResolvedTaskContext(
            task_key=task_key,
            run_id=run_id,
            task_type=task_type,
            compute_mode=ComputeMode.SERVERLESS,
            profile="serverless-local-v1",
            runtime_profile=None,
            access_mode=None,
            compute_resource_kind=None,
            compute_id=None,
            compute_key=None,
            compute_generation=None,
            autostart=False,
            cluster_configuration_json=None,
            environment_key=environment_key,
            environment_source=environment_source,
            environment_profile=f"serverless-python{sys.version_info.major}{sys.version_info.minor}-local-v1",
            environment_version=version,
            dependencies=dependencies,
            performance_target=effective_performance,
            disable_auto_optimization=True,
            timeout_seconds=timeout_seconds,
            effective_settings_json=_json(_SESSION_DEFAULTS),
            retry_settings_json=_json(retry_settings),
            task_arguments_json=_json(arguments),
        )

    def _resolve_classic(
        self,
        task: dict[str, object],
        task_key: str,
        run_id: int | None,
        task_type: str,
        arguments: dict[str, object],
        selector: str,
        cluster_definitions: dict[str, ClusterConfiguration],
        shared_usage: dict[str, int],
    ) -> ResolvedTaskContext:
        retry_settings = _retry_settings(task)
        timeout_seconds = _timeout_seconds(task)
        if task.get("disable_auto_optimization") is not None:
            raise InvalidExecutionContextError(
                "disable_auto_optimization is a serverless-only setting"
            )
        if selector == "existing_cluster_id":
            compute_id = task.get(selector)
            if not isinstance(compute_id, str) or not compute_id:
                raise InvalidExecutionContextError("existing_cluster_id must be a non-empty string")
            try:
                record = self._context.compute.get_cluster(compute_id)
            except ComputeNotFoundError as error:
                raise ExecutionContextNotFoundError(
                    "existing_cluster_id does not resolve to a local all-purpose cluster"
                ) from error
            if record.state not in {ComputeState.RUNNING, ComputeState.TERMINATED}:
                raise InvalidExecutionContextError(
                    f"existing cluster is not available in state {record.state.value}"
                )
            configuration_json = _cluster_configuration_from_record(record)
            return ResolvedTaskContext(
                task_key=task_key,
                run_id=run_id,
                task_type=task_type,
                compute_mode=ComputeMode.CLASSIC,
                profile=record.profile,
                runtime_profile=record.spark_version,
                access_mode=record.access_mode,
                compute_resource_kind=ComputeResourceKind.ALL_PURPOSE,
                compute_id=record.compute_id,
                compute_key=None,
                compute_generation=record.generation,
                autostart=record.state is ComputeState.TERMINATED,
                cluster_configuration_json=configuration_json,
                environment_key=None,
                environment_source=None,
                environment_profile=None,
                environment_version=None,
                dependencies=(),
                performance_target=None,
                disable_auto_optimization=None,
                timeout_seconds=timeout_seconds,
                effective_settings_json=_json(_cluster_session_settings(record.settings)),
                retry_settings_json=_json(retry_settings),
                task_arguments_json=_json(arguments),
            )

        if selector == "new_cluster":
            raw_configuration = task.get("new_cluster")
            if not isinstance(raw_configuration, Mapping):
                raise InvalidExecutionContextError("new_cluster must be an object")
            configuration = _normalize_cluster(raw_configuration)
            compute_key = f"inline_{task_key}"
            resource_kind = ComputeResourceKind.JOB_NEW
        else:
            key = task.get("job_cluster_key")
            if not isinstance(key, str) or not _IDENTIFIER_RE.fullmatch(key):
                raise InvalidExecutionContextError("job_cluster_key must be a bounded identifier")
            configured_cluster = cluster_definitions.get(key)
            if configured_cluster is None:
                raise ExecutionContextNotFoundError(
                    "job_cluster_key does not resolve to a declared cluster"
                )
            configuration = configured_cluster
            compute_key = key
            resource_kind = (
                ComputeResourceKind.JOB_SHARED
                if shared_usage.get(key, 0) > 1
                else ComputeResourceKind.JOB_NEW
            )
        return ResolvedTaskContext(
            task_key=task_key,
            run_id=run_id,
            task_type=task_type,
            compute_mode=ComputeMode.CLASSIC,
            profile=configuration.profile,
            runtime_profile=configuration.runtime_profile,
            access_mode=configuration.access_mode,
            compute_resource_kind=resource_kind,
            compute_id=None,
            compute_key=compute_key,
            compute_generation=None,
            autostart=False,
            cluster_configuration_json=_configuration_json(configuration),
            environment_key=None,
            environment_source=None,
            environment_profile=None,
            environment_version=None,
            dependencies=(),
            performance_target=None,
            disable_auto_optimization=None,
            timeout_seconds=timeout_seconds,
            effective_settings_json=_json(_cluster_session_settings(configuration.settings)),
            retry_settings_json=_json(retry_settings),
            task_arguments_json=_json(arguments),
        )


def _materialize_objects(
    values: Sequence[Mapping[str, object]],
    *,
    label: str,
    minimum: int,
    maximum: int,
) -> list[dict[str, object]]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
        raise InvalidExecutionContextError(f"{label} must be a sequence of objects")
    result: list[dict[str, object]] = []
    for value in values:
        if not isinstance(value, Mapping):
            raise InvalidExecutionContextError(f"{label} entries must be JSON objects")
        result.append(dict(value))
    if not minimum <= len(result) <= maximum:
        raise InvalidExecutionContextError(
            f"{label} must contain between {minimum} and {maximum} entries"
        )
    return result


def _cluster_definitions(
    values: Sequence[Mapping[str, object]],
) -> dict[str, ClusterConfiguration]:
    entries = _materialize_objects(
        values, label="job_clusters", minimum=0, maximum=MAX_TASKS_PER_JOB
    )
    definitions: dict[str, ClusterConfiguration] = {}
    for entry in entries:
        if set(entry) - {"job_cluster_key", "new_cluster", "serverless_compute_id"}:
            raise UnsupportedExecutionContextError("unsupported job cluster definition field")
        key = _required_identifier(entry, "job_cluster_key")
        if key in definitions:
            raise InvalidExecutionContextError("job_cluster_key values must be unique")
        if entry.get("serverless_compute_id") is not None:
            raise UnsupportedExecutionContextError("serverless compute objects are not supported")
        configuration = entry.get("new_cluster")
        if not isinstance(configuration, Mapping):
            raise InvalidExecutionContextError("job cluster must declare a classic new_cluster")
        definitions[key] = _normalize_cluster(configuration)
    return definitions


def _environment_definitions(
    values: Sequence[Mapping[str, object]],
) -> dict[str, tuple[str, tuple[str, ...]]]:
    entries = _materialize_objects(
        values, label="environments", minimum=0, maximum=MAX_TASKS_PER_JOB
    )
    definitions: dict[str, tuple[str, tuple[str, ...]]] = {}
    for entry in entries:
        if set(entry) - {"environment_key", "spec"}:
            raise UnsupportedExecutionContextError("unsupported environment definition field")
        key = _required_identifier(entry, "environment_key")
        if key in definitions:
            raise InvalidExecutionContextError("environment_key values must be unique")
        spec = entry.get("spec")
        if not isinstance(spec, Mapping):
            raise InvalidExecutionContextError("environment spec is required")
        if set(spec) - {
            "environment_version",
            "dependencies",
            "client",
            "java_dependencies",
            "base_environment",
            "project_environment",
        }:
            raise UnsupportedExecutionContextError("unsupported environment spec field")
        if any(
            spec.get(name) is not None
            for name in ("client", "java_dependencies", "base_environment", "project_environment")
        ):
            raise UnsupportedExecutionContextError(
                "only the mapped local Python environment version and dependencies are supported"
            )
        version = spec.get("environment_version")
        if version != _SUPPORTED_ENVIRONMENT_VERSION:
            raise UnsupportedExecutionContextError(
                "environment_version is not mapped to a provisioned local Python environment"
            )
        raw_dependencies = spec.get("dependencies", [])
        if not isinstance(raw_dependencies, Sequence) or isinstance(
            raw_dependencies, (str, bytes, bytearray)
        ):
            raise InvalidExecutionContextError("environment dependencies must be a string list")
        dependencies: list[str] = []
        for dependency in raw_dependencies:
            if not isinstance(dependency, str):
                raise InvalidExecutionContextError("environment dependencies must be strings")
            matched = _REQUIREMENT_RE.fullmatch(dependency)
            expected = _SUPPORTED_ENVIRONMENT_DEPENDENCIES.get(dependency)
            if matched is None or expected is None:
                raise UnsupportedExecutionContextError(
                    "environment dependency is not an exact verified local package pin"
                )
            if _normalized_package_name(matched.group(1)) != _normalized_package_name(expected[0]):
                raise UnsupportedExecutionContextError(
                    "environment dependency is not an exact verified local package pin"
                )
            try:
                installed_version = importlib.metadata.version(expected[0])
            except importlib.metadata.PackageNotFoundError as error:
                raise UnsupportedExecutionContextError(
                    "environment dependency is not installed in the selected local interpreter"
                ) from error
            if installed_version != expected[1]:
                raise UnsupportedExecutionContextError(
                    "environment dependency version differs from the verified local pin"
                )
            dependencies.append(dependency)
        if len(set(dependencies)) != len(dependencies):
            raise InvalidExecutionContextError("environment dependencies cannot contain duplicates")
        definitions[key] = (version, tuple(sorted(dependencies)))
    return definitions


def _task_arguments(task: Mapping[str, object]) -> tuple[str, dict[str, object]]:
    task_types = [
        key for key in ("spark_python_task", "notebook_task") if task.get(key) is not None
    ]
    if len(task_types) != 1:
        raise UnsupportedExecutionContextError(
            "exactly one Spark Python script or Python notebook task is supported"
        )
    task_type = task_types[0]
    raw = task[task_type]
    if not isinstance(raw, Mapping):
        raise InvalidExecutionContextError(f"{task_type} must be an object")
    data = dict(raw)
    if task_type == "spark_python_task":
        if set(data) - {"python_file", "parameters", "source"}:
            raise UnsupportedExecutionContextError("unsupported Spark Python task field")
        path = data.get("python_file")
        parameters = data.get("parameters", [])
        if not isinstance(path, str) or not path:
            raise InvalidExecutionContextError("python_file must be a non-empty path")
        _validate_workspace_path(path, label="python_file")
        if data.get("source") not in (None, "WORKSPACE"):
            raise UnsupportedExecutionContextError(
                "Git-based task source is not provisioned locally"
            )
        validated_parameters = validate_python_parameters(parameters)
        result: dict[str, object] = {
            "python_file": path,
            "parameters": validated_parameters,
        }
        if data.get("source") is not None:
            result["source"] = data["source"]
        return "python_script", result

    if set(data) - {"notebook_path", "base_parameters", "source", "warehouse_id"}:
        raise UnsupportedExecutionContextError("unsupported notebook task field")
    if data.get("warehouse_id") is not None:
        raise UnsupportedExecutionContextError(
            "SQL warehouse notebooks are not Python compute tasks"
        )
    path = data.get("notebook_path")
    if not isinstance(path, str) or not path:
        raise InvalidExecutionContextError("notebook_path must be a non-empty path")
    _validate_workspace_path(path, label="notebook_path")
    if data.get("source") not in (None, "WORKSPACE"):
        raise UnsupportedExecutionContextError(
            "Git-based notebook source is not provisioned locally"
        )
    parameters = validate_notebook_parameters(data.get("base_parameters", {}))
    result = {"notebook_path": path, "base_parameters": parameters}
    if data.get("source") is not None:
        result["source"] = data["source"]
    return "notebook", result


def validate_notebook_parameters(value: object) -> dict[str, str]:
    """Validate the selected task/run-now notebook key-value parameter shape."""
    if not isinstance(value, Mapping) or len(value) > 100:
        raise InvalidExecutionContextError("notebook parameters must be a bounded string map")
    normalized: dict[str, str] = {}
    for key, item in value.items():
        if (
            not isinstance(key, str)
            or not key
            or len(key) > 256
            or not isinstance(item, str)
            or len(item) > 4096
        ):
            raise InvalidExecutionContextError(
                "notebook parameters must contain bounded string pairs"
            )
        if _CREDENTIAL_VALUE_RE.search(key) or _CREDENTIAL_VALUE_RE.search(item):
            raise InvalidExecutionContextError("notebook parameters cannot carry credential values")
        normalized[key] = item
    return normalized


def validate_python_parameters(value: object) -> list[str]:
    """Validate bounded argv values and reject credential-shaped parameter strings."""
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise InvalidExecutionContextError("Python task parameters must be a sequence of strings")
    if len(value) > 100 or any(
        not isinstance(item, str) or len(item) > 4096 or "\x00" in item for item in value
    ):
        raise InvalidExecutionContextError("Python task parameters must be bounded strings")
    parameters = list(value)
    contains_named_credential = any(
        _CREDENTIAL_FLAG_RE.fullmatch(item)
        and index + 1 < len(parameters)
        and not parameters[index + 1].startswith("-")
        for index, item in enumerate(parameters)
    )
    if contains_named_credential or any(_CREDENTIAL_VALUE_RE.search(item) for item in parameters):
        raise InvalidExecutionContextError(
            "Python task parameters exceed limits or contain credentials"
        )
    return parameters


def _validate_workspace_path(value: str, *, label: str) -> None:
    if len(value) > 4096 or "\\" in value or "\x00" in value or not value.startswith("/Workspace/"):
        raise InvalidExecutionContextError(f"{label} must be an absolute local /Workspace path")
    path = PurePosixPath(value)
    if (
        path.as_posix() != value
        or any(part in {"", ".", ".."} for part in path.parts[1:])
        or len(path.parts) < 3
    ):
        raise InvalidExecutionContextError(f"{label} must be a normalized local /Workspace path")


def _retry_settings(task: Mapping[str, object]) -> dict[str, int | bool | None]:
    settings: dict[str, int | bool | None] = {
        "max_retries": None,
        "min_retry_interval_millis": None,
        "retry_on_timeout": None,
    }
    retries = task.get("max_retries")
    if retries is not None and (type(retries) is not int or retries < 0 or retries > 10):
        raise InvalidExecutionContextError("max_retries must be a whole number from 0 to 10")
    interval = task.get("min_retry_interval_millis")
    if interval is not None and (type(interval) is not int or interval < 0 or interval > 3_600_000):
        raise InvalidExecutionContextError(
            "min_retry_interval_millis must be between 0 and 3600000"
        )
    retry_timeout = task.get("retry_on_timeout")
    if retry_timeout is not None and type(retry_timeout) is not bool:
        raise InvalidExecutionContextError("retry_on_timeout must be a boolean")
    settings.update(
        max_retries=retries,
        min_retry_interval_millis=interval,
        retry_on_timeout=retry_timeout,
    )
    return settings


def _timeout_seconds(task: Mapping[str, object]) -> int | None:
    value = task.get("timeout_seconds", 0)
    if value is None:
        value = 0
    if type(value) is not int or value < 0:
        raise InvalidExecutionContextError("timeout_seconds must be a non-negative integer")
    if value > MAX_WORKER_TIMEOUT_SECONDS:
        raise UnsupportedExecutionContextError(
            f"task timeout exceeds the local {MAX_WORKER_TIMEOUT_SECONDS}-second worker limit"
        )
    return value or None


def _normalize_cluster(configuration: Mapping[str, object]) -> ClusterConfiguration:
    if "data_security_mode" not in configuration:
        raise InvalidExecutionContextError(
            "classic job compute must declare an explicit data_security_mode"
        )
    try:
        return normalize_cluster_configuration(configuration)
    except (InvalidComputeRequestError, UnsupportedComputeEffectError) as error:
        if isinstance(error, UnsupportedComputeEffectError):
            raise UnsupportedExecutionContextError(str(error)) from error
        raise InvalidExecutionContextError(str(error)) from error


def _cluster_configuration_from_record(record: ComputeRecord) -> str:
    return _json(
        {
            "cluster_name": record.cluster_name,
            "spark_version": record.spark_version,
            "data_security_mode": record.access_mode,
            "single_user_name": record.single_user_name,
            "autotermination_minutes": record.autotermination_minutes,
            "settings": record.settings,
        }
    )


def _configuration_json(configuration: ClusterConfiguration) -> str:
    return _json(
        {
            "cluster_name": configuration.cluster_name,
            "spark_version": configuration.spark_version,
            "data_security_mode": configuration.access_mode,
            "single_user_name": configuration.single_user_name,
            "autotermination_minutes": configuration.autotermination_minutes,
            "settings": configuration.settings,
        }
    )


def _cluster_session_settings(settings: Mapping[str, object]) -> dict[str, str]:
    raw = settings.get("spark_conf", {})
    if not isinstance(raw, Mapping):
        return {}
    return {str(key): str(value) for key, value in raw.items()}


def _required_identifier(value: Mapping[str, object], field_name: str) -> str:
    result = value.get(field_name)
    if not isinstance(result, str) or not _IDENTIFIER_RE.fullmatch(result):
        raise InvalidExecutionContextError(f"{field_name} must be a bounded identifier")
    return result


def _normalized_package_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).casefold()


def _json(value: object) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        raise InvalidExecutionContextError(
            "execution context must contain finite JSON data"
        ) from error
