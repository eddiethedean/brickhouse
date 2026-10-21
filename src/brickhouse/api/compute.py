"""Pinned Databricks SDK cluster lifecycle routes backed by local metadata."""

from __future__ import annotations

import base64
import binascii
import json
from datetime import datetime
from http import HTTPStatus
from urllib.parse import parse_qs, urlsplit

from brickhouse.api.catalog import ApiResponse
from brickhouse.compute.models import (
    ComputeConflictError,
    ComputeNotFoundError,
    ComputeRecord,
    ComputeStateTransitionError,
    InvalidComputeRequestError,
    UnsupportedComputeEffectError,
)
from brickhouse.compute.repository import ComputeRepositoryError
from brickhouse.compute.service import ComputeService
from brickhouse.workspace.context import WorkspaceContext

_PREFIX = "/api/2.1/clusters/"
_PAGE_SCOPE = "clusters"
_MAX_PAGE_SIZE = 1000
_SIMULATED_STATE_MESSAGE = (
    "Cluster lifecycle metadata is simulated; BrickHouse did not provision cloud compute."
)


class ComputeHttpApi:
    """Expose the supported Databricks SDK cluster methods on local metadata."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._service: ComputeService = context.compute

    def dispatch(
        self,
        method: str,
        target: str,
        body: dict[str, object] | None = None,
    ) -> ApiResponse | None:
        parsed = urlsplit(target)
        if not parsed.path.startswith(_PREFIX):
            return None
        route = parsed.path[len(_PREFIX) :]
        query = parse_qs(parsed.query, keep_blank_values=True)
        try:
            if route == "create":
                if method != "POST":
                    return _method_not_allowed("POST")
                _validate_query(query, set())
                record = self._service.create_cluster(_required_body(body))
                return _ok({"cluster_id": record.compute_id})
            if route == "get":
                if method != "GET":
                    return _method_not_allowed("GET")
                _validate_query(query, {"cluster_id"})
                compute_id = _required_query(query, "cluster_id")
                return _ok(_cluster_payload(self._service.get_cluster(compute_id)))
            if route == "list":
                if method != "GET":
                    return _method_not_allowed("GET")
                _validate_query(query, {"page_size", "page_token"})
                return _ok(self._list_clusters(query))
            if route == "edit":
                if method != "POST":
                    return _method_not_allowed("POST")
                _validate_query(query, set())
                data = _required_body(body)
                compute_id = _take_required_string(data, "cluster_id")
                return _ok(_cluster_payload(self._service.edit_cluster(compute_id, data)))
            if route == "start":
                if method != "POST":
                    return _method_not_allowed("POST")
                _validate_query(query, set())
                return _ok(_cluster_payload(self._service.start_cluster(_action_cluster_id(body))))
            if route == "restart":
                if method != "POST":
                    return _method_not_allowed("POST")
                _validate_query(query, set())
                data = _required_body(body)
                if set(data) - {"cluster_id"}:
                    raise UnsupportedComputeEffectError(
                        "restart_user is unsupported because local cluster APIs do not "
                        "emulate identity"
                    )
                return _ok(
                    _cluster_payload(
                        self._service.restart_cluster(_take_required_string(data, "cluster_id"))
                    )
                )
            if route == "delete":
                if method != "POST":
                    return _method_not_allowed("POST")
                _validate_query(query, set())
                record = self._service.terminate_cluster(_action_cluster_id(body))
                return _ok(_cluster_payload(record))
            if route == "permanent-delete":
                if method != "POST":
                    return _method_not_allowed("POST")
                _validate_query(query, set())
                self._service.permanently_delete_cluster(_action_cluster_id(body))
                return _ok({})
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", "API route not found")
        except ComputeNotFoundError as error:
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", str(error))
        except ComputeConflictError as error:
            return _error(HTTPStatus.CONFLICT, "RESOURCE_CONFLICT", str(error))
        except ComputeStateTransitionError as error:
            return _error(HTTPStatus.CONFLICT, "INVALID_STATE", str(error))
        except (UnsupportedComputeEffectError, InvalidComputeRequestError, _InputError) as error:
            return _error(HTTPStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", str(error))
        except ComputeRepositoryError:
            return _error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "compute metadata operation failed",
            )

    def _list_clusters(self, query: dict[str, list[str]]) -> dict[str, object]:
        raw_page_size = _single_query(query, "page_size")
        page_size = _MAX_PAGE_SIZE
        if raw_page_size is not None:
            try:
                page_size = int(raw_page_size)
            except ValueError as error:
                raise _InputError("page_size must be an integer") from error
            if page_size < 1:
                raise _InputError("page_size must be a positive integer")
            page_size = min(page_size, _MAX_PAGE_SIZE)
        token = _single_query(query, "page_token")
        offset = _decode_page_token(token) if token else 0
        total = self._service.count_clusters()
        if offset > total:
            raise _InputError("page_token is outside the current result set")
        page = self._service.list_clusters(page_size=page_size, offset=offset)
        next_offset = offset + len(page)
        result: dict[str, object] = {"clusters": [_cluster_payload(item) for item in page]}
        if next_offset < total:
            result["next_page_token"] = _encode_page_token(next_offset)
        return result


def _cluster_payload(record: ComputeRecord) -> dict[str, object]:
    payload: dict[str, object] = {
        "cluster_id": record.compute_id,
        "cluster_name": record.cluster_name,
        "spark_version": record.spark_version,
        "data_security_mode": record.access_mode,
        "runtime_engine": "STANDARD",
        "state": record.state.value,
        "state_message": _SIMULATED_STATE_MESSAGE,
        "start_time": _epoch_millis(record.started_at),
        "terminated_time": _epoch_millis(record.terminated_at),
        "autotermination_minutes": record.autotermination_minutes,
    }
    if record.single_user_name is not None:
        payload["single_user_name"] = record.single_user_name
    settings = record.settings
    for key in (
        "autoscale",
        "custom_tags",
        "driver_node_type_id",
        "enable_elastic_disk",
        "node_type_id",
        "num_workers",
        "spark_conf",
    ):
        if key in settings:
            payload[key] = settings[key]
    return payload


def _epoch_millis(value: str | None) -> int | None:
    if value is None:
        return None
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def _required_body(body: dict[str, object] | None) -> dict[str, object]:
    if not isinstance(body, dict):
        raise _InputError("request body must be a JSON object")
    return dict(body)


def _action_cluster_id(body: dict[str, object] | None) -> str:
    data = _required_body(body)
    if set(data) != {"cluster_id"}:
        raise _InputError("request body must contain only cluster_id")
    return _take_required_string(data, "cluster_id")


def _take_required_string(data: dict[str, object], key: str) -> str:
    value = data.pop(key, None)
    if not isinstance(value, str) or not value:
        raise _InputError(f"{key} must be a non-empty string")
    return value


def _validate_query(query: dict[str, list[str]], allowed: set[str]) -> None:
    unsupported = set(query) - allowed
    if unsupported:
        raise _InputError(f"unsupported query parameter(s): {', '.join(sorted(unsupported))}")


def _single_query(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    if values is None:
        return None
    if len(values) != 1:
        raise _InputError(f"query parameter {key} must be specified once")
    return values[0]


def _required_query(query: dict[str, list[str]], key: str) -> str:
    value = _single_query(query, key)
    if not value:
        raise _InputError(f"query parameter {key} is required")
    return value


def _encode_page_token(offset: int) -> str:
    return base64.urlsafe_b64encode(json.dumps([_PAGE_SCOPE, offset]).encode()).decode().rstrip("=")


def _decode_page_token(token: str) -> int:
    try:
        raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
        value = json.loads(raw)
    except (ValueError, binascii.Error, json.JSONDecodeError) as error:
        raise _InputError("page_token is invalid") from error
    if (
        not isinstance(value, list)
        or len(value) != 2
        or value[0] != _PAGE_SCOPE
        or not isinstance(value[1], int)
        or isinstance(value[1], bool)
        or value[1] < 0
    ):
        raise _InputError("page_token is invalid for this result set")
    return value[1]


def _error(status: HTTPStatus, code: str, message: str) -> ApiResponse:
    return ApiResponse(status, {"error_code": code, "message": message})


def _ok(payload: dict[str, object]) -> ApiResponse:
    return ApiResponse(HTTPStatus.OK, payload)


def _method_not_allowed(allow: str) -> ApiResponse:
    return ApiResponse(
        HTTPStatus.METHOD_NOT_ALLOWED,
        {
            "error_code": "INVALID_PARAMETER_VALUE",
            "message": "method is not supported",
            "allow": allow,
        },
    )


class _InputError(ValueError):
    pass
