"""Versioned local compute resource and lifecycle value types."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import cast

_PROFILE_BY_SPARK_VERSION = {"spark35-local-v1": "spark35-local-v1"}
_ACCESS_PROFILE_BY_MODE = {
    "DATA_SECURITY_MODE_STANDARD": "classic-standard-local-v1",
    "USER_ISOLATION": "classic-standard-local-v1",
    "DATA_SECURITY_MODE_DEDICATED": "classic-dedicated-local-v1",
    "SINGLE_USER": "classic-dedicated-local-v1",
}
_SECRET_KEY_RE = re.compile(r"(?:token|secret|password|credential|privatekey|apikey)$")
MAX_CLUSTER_SETTINGS_BYTES = 64 * 1024


class ComputeResourceKind(StrEnum):
    ALL_PURPOSE = "all_purpose"
    JOB_NEW = "job_new"
    JOB_SHARED = "job_shared"


class ComputeState(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RESTARTING = "RESTARTING"
    TERMINATING = "TERMINATING"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"


class ComputeError(RuntimeError):
    """Base exception for the local compute lifecycle contract."""


class ComputeNotFoundError(ComputeError):
    """A local cluster or run-scoped compute resource does not exist."""


class InvalidComputeRequestError(ComputeError, ValueError):
    """A compute request is malformed or outside the selected local profile."""


class UnsupportedComputeEffectError(InvalidComputeRequestError):
    """A valid SDK field requests an effect the local profile does not provide."""


class ComputeStateTransitionError(ComputeError):
    """A compute or consumer transition is not legal in its current state."""


class ComputeConflictError(ComputeError):
    """A run-scoped compute identity or consumer already has different content."""


@dataclass(frozen=True, slots=True)
class ClusterConfiguration:
    cluster_name: str
    spark_version: str
    runtime_profile: str
    access_mode: str
    profile: str
    single_user_name: str | None
    settings_json: str
    autotermination_minutes: int | None

    @property
    def settings(self) -> dict[str, object]:
        """Return a fresh copy of the selected, simulated cluster settings."""
        return cast(dict[str, object], json.loads(self.settings_json))


@dataclass(frozen=True, slots=True)
class ComputeRecord:
    compute_id: str
    resource_kind: ComputeResourceKind
    profile: str
    state: ComputeState
    run_id: int | None
    compute_key: str | None
    cluster_name: str
    spark_version: str
    access_mode: str
    single_user_name: str | None
    settings_json: str
    autotermination_minutes: int | None
    created_at: str
    updated_at: str
    started_at: str | None
    terminated_at: str | None
    last_activity_at: str
    generation: int
    deleted_at: str | None

    @property
    def settings(self) -> dict[str, object]:
        """Return a fresh copy of persisted selected settings."""
        return cast(dict[str, object], json.loads(self.settings_json))

    @property
    def is_simulated(self) -> bool:
        """All compute allocation and VM lifecycle represented here is simulated."""
        return True


@dataclass(frozen=True, slots=True)
class ComputeConsumerRecord:
    compute_id: str
    run_id: int
    task_key: str
    state: str
    registered_at: str
    started_at: str | None
    released_at: str | None


@dataclass(frozen=True, slots=True)
class ComputeLifecycleEvent:
    event_id: int
    compute_id: str
    event_type: str
    state: ComputeState
    generation: int
    invalidates_sessions: bool
    occurred_at: str
    details_json: str


def normalize_cluster_configuration(
    body: Mapping[str, object],
    *,
    previous: ComputeRecord | None = None,
) -> ClusterConfiguration:
    """Validate supported cluster settings and resolve explicit local mappings."""
    if not isinstance(body, Mapping):
        raise InvalidComputeRequestError("cluster settings must be a JSON object")
    data: dict[str, object] = dict(body)
    allowed = {
        "spark_version",
        "cluster_name",
        "data_security_mode",
        "single_user_name",
        "runtime_engine",
        "num_workers",
        "autoscale",
        "autotermination_minutes",
        "node_type_id",
        "driver_node_type_id",
        "enable_elastic_disk",
        "custom_tags",
        "spark_conf",
        "kind",
        "apply_policy_default_values",
    }
    unsupported = set(data) - allowed
    if unsupported:
        raise UnsupportedComputeEffectError(
            f"unsupported cluster request field(s): {', '.join(sorted(unsupported))}"
        )
    spark_version_value = data.get(
        "spark_version", previous.spark_version if previous is not None else None
    )
    if not isinstance(spark_version_value, str) or not spark_version_value:
        raise InvalidComputeRequestError("spark_version is required")
    runtime_profile = _PROFILE_BY_SPARK_VERSION.get(spark_version_value)
    if runtime_profile is None:
        raise InvalidComputeRequestError(
            "spark_version is not mapped to a provisioned local runtime"
        )
    mode_value = data.get("data_security_mode")
    if mode_value is None and previous is not None:
        mode_value = previous.access_mode
    if mode_value is None:
        mode_value = "DATA_SECURITY_MODE_STANDARD"
    if not isinstance(mode_value, str) or mode_value not in _ACCESS_PROFILE_BY_MODE:
        raise InvalidComputeRequestError(
            "data_security_mode is not mapped by the local compute profile"
        )
    profile = _ACCESS_PROFILE_BY_MODE[mode_value]
    access_mode = (
        "DATA_SECURITY_MODE_STANDARD"
        if profile == "classic-standard-local-v1"
        else "DATA_SECURITY_MODE_DEDICATED"
    )
    runtime_engine = data.get("runtime_engine")
    if runtime_engine not in (None, "STANDARD"):
        raise UnsupportedComputeEffectError(
            "only the standard local runtime engine is provisioned; Photon is unsupported"
        )
    kind = data.get("kind")
    if kind not in (None, "CLASSIC"):
        raise UnsupportedComputeEffectError("only classic all-purpose compute is supported")
    policy_defaults = data.get("apply_policy_default_values")
    if policy_defaults is not None and type(policy_defaults) is not bool:
        raise InvalidComputeRequestError("apply_policy_default_values must be a boolean")
    if policy_defaults:
        raise UnsupportedComputeEffectError("cluster policies are not provisioned locally")

    single_user_name = data.get(
        "single_user_name", previous.single_user_name if previous is not None else None
    )
    if single_user_name is not None and (
        not isinstance(single_user_name, str) or not single_user_name.strip()
    ):
        raise InvalidComputeRequestError("single_user_name must be a non-empty string")
    if profile == "classic-dedicated-local-v1":
        if single_user_name is None:
            raise InvalidComputeRequestError(
                "single_user_name is required for the dedicated local access profile"
            )
        single_user_name = single_user_name.strip()
    if profile == "classic-standard-local-v1" and single_user_name is not None:
        raise InvalidComputeRequestError(
            "single_user_name is not valid for the standard local access profile"
        )

    cluster_name = data.get("cluster_name", previous.cluster_name if previous is not None else "")
    if not isinstance(cluster_name, str) or len(cluster_name) > 256:
        raise InvalidComputeRequestError("cluster_name must be a string of at most 256 characters")
    autotermination = data.get(
        "autotermination_minutes",
        previous.autotermination_minutes if previous is not None else None,
    )
    if autotermination is not None and (
        type(autotermination) is not int
        or autotermination < 0
        or autotermination > 10000
        or 0 < autotermination < 10
    ):
        raise InvalidComputeRequestError(
            "autotermination_minutes must be 0 or a whole number from 10 to 10000"
        )

    settings = dict(previous.settings) if previous is not None else {}
    for key in (
        "num_workers",
        "autoscale",
        "node_type_id",
        "driver_node_type_id",
        "enable_elastic_disk",
        "custom_tags",
        "spark_conf",
    ):
        if key in data:
            settings[key] = data[key]
    if previous is None and "num_workers" not in settings and "autoscale" not in settings:
        raise InvalidComputeRequestError("num_workers or autoscale is required")
    if "num_workers" in settings and "autoscale" in settings:
        raise InvalidComputeRequestError("num_workers and autoscale are mutually exclusive")
    _validate_settings(settings)
    settings_json = _canonical_json(settings)
    if len(settings_json.encode("utf-8")) > MAX_CLUSTER_SETTINGS_BYTES:
        raise InvalidComputeRequestError("cluster settings exceed the 64 KiB limit")
    return ClusterConfiguration(
        cluster_name,
        spark_version_value,
        runtime_profile,
        access_mode,
        profile,
        single_user_name,
        settings_json,
        autotermination,
    )


def _validate_settings(settings: dict[str, object]) -> None:
    num_workers = settings.get("num_workers")
    if num_workers is not None and (
        type(num_workers) is not int or num_workers < 0 or num_workers > 1000
    ):
        raise InvalidComputeRequestError("num_workers must be a whole number from 0 to 1000")
    autoscale = settings.get("autoscale")
    if autoscale is not None:
        if not isinstance(autoscale, dict) or set(autoscale) != {"min_workers", "max_workers"}:
            raise InvalidComputeRequestError("autoscale requires min_workers and max_workers")
        minimum = autoscale["min_workers"]
        maximum = autoscale["max_workers"]
        if (
            type(minimum) is not int
            or type(maximum) is not int
            or minimum < 0
            or maximum < minimum
            or maximum > 1000
        ):
            raise InvalidComputeRequestError("autoscale worker limits are invalid")
    for key in ("node_type_id", "driver_node_type_id"):
        value = settings.get(key)
        if value is not None and (not isinstance(value, str) or len(value) > 128):
            raise InvalidComputeRequestError(f"{key} must be a string of at most 128 characters")
    elastic = settings.get("enable_elastic_disk")
    if elastic is not None and type(elastic) is not bool:
        raise InvalidComputeRequestError("enable_elastic_disk must be a boolean")
    tags = settings.get("custom_tags")
    if tags is not None:
        if not isinstance(tags, dict) or len(tags) > 45:
            raise InvalidComputeRequestError(
                "custom_tags must be an object with at most 45 entries"
            )
        if any(
            not isinstance(key, str)
            or not key
            or len(key) > 75
            or not isinstance(value, str)
            or len(value) > 500
            for key, value in tags.items()
        ):
            raise InvalidComputeRequestError("custom_tags must contain bounded string pairs")
    spark_conf = settings.get("spark_conf")
    if spark_conf is not None:
        if not isinstance(spark_conf, dict):
            raise InvalidComputeRequestError("spark_conf must be an object of string pairs")
        if any(
            not isinstance(key, str)
            or not isinstance(value, str)
            or not key
            or len(key) > 256
            or len(value) > 4096
            for key, value in spark_conf.items()
        ):
            raise InvalidComputeRequestError("spark_conf must contain bounded string pairs")
        if any(_is_secret_key(key) for key in spark_conf):
            raise InvalidComputeRequestError(
                "credential-shaped spark_conf keys cannot be persisted"
            )
        if any("extraJavaOptions" in key or "classpath" in key.casefold() for key in spark_conf):
            raise UnsupportedComputeEffectError("JVM-specific cluster configuration is unsupported")
    if tags is not None and any(_is_secret_key(key) for key in tags):
        raise InvalidComputeRequestError("credential-shaped custom tag keys cannot be persisted")


def _is_secret_key(key: str) -> bool:
    normalized = key.casefold().replace("-", "_").replace("_", "")
    return bool(_SECRET_KEY_RE.search(normalized))


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        raise InvalidComputeRequestError("cluster settings must be finite JSON data") from error
