"""Pinned Databricks SQL warehouse routes backed by simulated local records."""

from __future__ import annotations

import base64
import binascii
import json
from http import HTTPStatus
from urllib.parse import parse_qs, unquote, urlsplit

from brickhouse.api.catalog import ApiResponse
from brickhouse.compute.models import (
    ComputeConflictError,
    ComputeNotFoundError,
    InvalidComputeRequestError,
    UnsupportedComputeEffectError,
)
from brickhouse.compute.repository import ComputeRepositoryError
from brickhouse.compute.warehouse import SqlWarehouseLifecycle, WarehouseRecord
from brickhouse.workspace.context import WorkspaceContext

_PREFIX = "/api/2.0/sql/warehouses"
_PAGE_SCOPE = "sql-warehouses"
_MAX_PAGE_SIZE = 1000


class SqlWarehouseHttpApi:
    """Translate the supported SDK warehouse lifecycle into local metadata."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._service: SqlWarehouseLifecycle = context.compute.warehouses

    def dispatch(
        self,
        method: str,
        target: str,
        body: dict[str, object] | None = None,
    ) -> ApiResponse | None:
        parsed = urlsplit(target)
        if parsed.path != _PREFIX and not parsed.path.startswith(f"{_PREFIX}/"):
            return None
        query = parse_qs(parsed.query, keep_blank_values=True)
        route = parsed.path[len(_PREFIX) :].strip("/")
        segments = [unquote(value) for value in route.split("/") if value]
        try:
            if not segments:
                return self._collection(method, query, body)
            if len(segments) == 1:
                return self._member(method, segments[0], query)
            if len(segments) == 2:
                return self._action(method, segments[0], segments[1], query, body)
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", "API route not found")
        except ComputeNotFoundError as error:
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", str(error))
        except ComputeConflictError as error:
            return _error(HTTPStatus.CONFLICT, "RESOURCE_CONFLICT", str(error))
        except (UnsupportedComputeEffectError, InvalidComputeRequestError, _InputError) as error:
            return _error(HTTPStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", str(error))
        except ComputeRepositoryError:
            return _error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "SQL warehouse metadata operation failed",
            )

    def _collection(
        self,
        method: str,
        query: dict[str, list[str]],
        body: dict[str, object] | None,
    ) -> ApiResponse:
        if method == "POST":
            _validate_query(query, set())
            record = self._service.create(_require_body(body))
            return _ok({"id": record.warehouse_id})
        if method == "GET":
            _validate_query(query, {"page_size", "page_token"})
            raw_size = _single_query(query, "page_size")
            page_size = _MAX_PAGE_SIZE
            if raw_size is not None:
                try:
                    page_size = int(raw_size)
                except ValueError as error:
                    raise _InputError("page_size must be an integer") from error
                if page_size < 1:
                    raise _InputError("page_size must be positive")
                page_size = min(page_size, _MAX_PAGE_SIZE)
            raw_token = _single_query(query, "page_token")
            offset = _decode_page_token(raw_token) if raw_token else 0
            total = self._service.count()
            if offset > total:
                raise _InputError("page_token is outside the current result set")
            records = self._service.list_warehouses(page_size=page_size, offset=offset)
            payload: dict[str, object] = {
                "warehouses": [self._warehouse_payload(record) for record in records]
            }
            next_offset = offset + len(records)
            if next_offset < total:
                payload["next_page_token"] = _encode_page_token(next_offset)
            return _ok(payload)
        return _method_not_allowed("GET, POST")

    def _member(
        self,
        method: str,
        warehouse_id: str,
        query: dict[str, list[str]],
    ) -> ApiResponse:
        _validate_query(query, set())
        if method == "GET":
            return _ok(self._warehouse_payload(self._service.get(warehouse_id)))
        if method == "DELETE":
            self._service.delete(warehouse_id)
            return _ok({})
        return _method_not_allowed("GET, DELETE")

    def _action(
        self,
        method: str,
        warehouse_id: str,
        action: str,
        query: dict[str, list[str]],
        body: dict[str, object] | None,
    ) -> ApiResponse:
        if action == "edit":
            _validate_query(query, set())
            if method != "POST":
                return _method_not_allowed("POST")
            record = self._service.edit(warehouse_id, _require_body(body))
            return _ok(self._warehouse_payload(record))
        if action in {"start", "stop"}:
            _validate_query(query, set())
            if method != "POST":
                return _method_not_allowed("POST")
            record = (
                self._service.start(warehouse_id)
                if action == "start"
                else self._service.stop(warehouse_id)
            )
            return _ok(self._warehouse_payload(record))
        return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", "API route not found")

    def _warehouse_payload(self, record: WarehouseRecord) -> dict[str, object]:
        payload: dict[str, object] = {
            "id": record.warehouse_id,
            "name": record.name,
            "warehouse_type": record.warehouse_type,
            "enable_serverless_compute": record.enable_serverless_compute,
            "cluster_size": record.cluster_size,
            "auto_stop_mins": record.auto_stop_mins,
            "enable_photon": record.enable_photon,
            "spot_instance_policy": record.spot_instance_policy,
            "min_num_clusters": record.min_num_clusters,
            "max_num_clusters": record.max_num_clusters,
            "state": record.state.value,
            "num_active_sessions": self._service.count_active_statements(record.warehouse_id),
        }
        if record.statement_timeout is not None:
            payload["statement_timeout"] = record.statement_timeout
        return payload


def _require_body(body: dict[str, object] | None) -> dict[str, object]:
    if not isinstance(body, dict):
        raise _InputError("request body must be a JSON object")
    return dict(body)


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


def _encode_page_token(offset: int) -> str:
    value = json.dumps([_PAGE_SCOPE, offset]).encode()
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


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
        or type(value[1]) is not int
        or value[1] < 0
    ):
        raise _InputError("page_token is invalid for this result set")
    return value[1]


def _error(status: HTTPStatus, code: str, message: str) -> ApiResponse:
    return ApiResponse(status, {"error_code": code, "message": message})


def _ok(payload: dict[str, object]) -> ApiResponse:
    return ApiResponse(HTTPStatus.OK, payload)


def _method_not_allowed(_allow: str) -> ApiResponse:
    return ApiResponse(
        HTTPStatus.METHOD_NOT_ALLOWED,
        {"error_code": "INVALID_PARAMETER_VALUE", "message": "method is not supported"},
    )


class _InputError(ValueError):
    pass
