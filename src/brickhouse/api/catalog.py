"""Small Unity Catalog REST profile backed by local catalog services."""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from http import HTTPStatus
from typing import TypeVar
from urllib.parse import parse_qs, unquote, urlsplit

from brickhouse.catalog.repository import (
    CatalogRecord,
    CatalogRepositoryError,
    NamespaceAlreadyExistsError,
    NamespaceInUseError,
    NamespaceNotFoundError,
    ProtectedNamespaceError,
    SchemaRecord,
)
from brickhouse.catalog.service import CatalogService
from brickhouse.workspace.context import WorkspaceContext

_PREFIX = "/api/2.1/unity-catalog/"
_DEFAULT_PAGE_SIZE = 1000
_PageItem = TypeVar("_PageItem")


@dataclass(frozen=True, slots=True)
class ApiResponse:
    status: HTTPStatus
    payload: object


@dataclass(frozen=True, slots=True)
class ApiBinaryResponse:
    """A local file download whose body must not be JSON encoded."""

    status: HTTPStatus
    body: bytes
    content_type: str = "application/octet-stream"


class CatalogHttpApi:
    """Transport mapping for the SDK catalog/schema methods in B007.3."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._service = CatalogService(context)

    def dispatch(
        self,
        method: str,
        target: str,
        body: dict[str, object] | None = None,
    ) -> ApiResponse | None:
        parsed = urlsplit(target)
        path = parsed.path
        if not path.startswith(_PREFIX):
            return None
        segments = [unquote(item) for item in path.strip("/").split("/")]
        query = parse_qs(parsed.query, keep_blank_values=True)
        try:
            if segments == ["api", "2.1", "unity-catalog", "catalogs"]:
                return self._catalog_collection(method, query, body)
            if segments == ["api", "2.1", "unity-catalog", "schemas"]:
                return self._schema_collection(method, query, body)
            if len(segments) == 5 and segments[:4] == [
                "api",
                "2.1",
                "unity-catalog",
                "catalogs",
            ]:
                return self._catalog_member(method, segments[4], query)
            if len(segments) == 5 and segments[:4] == [
                "api",
                "2.1",
                "unity-catalog",
                "schemas",
            ]:
                return self._schema_member(method, segments[4], query)
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", "API route not found")
        except NamespaceAlreadyExistsError as error:
            return _error(HTTPStatus.CONFLICT, "ALREADY_EXISTS", str(error))
        except NamespaceNotFoundError as error:
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", str(error))
        except NamespaceInUseError as error:
            return _error(HTTPStatus.CONFLICT, "RESOURCE_IN_USE", str(error))
        except ProtectedNamespaceError as error:
            return _error(HTTPStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", str(error))
        except CatalogRepositoryError as error:
            return _error(HTTPStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", str(error))
        except _InputError as error:
            return _error(HTTPStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", str(error))

    def _catalog_collection(
        self,
        method: str,
        query: dict[str, list[str]],
        body: dict[str, object] | None,
    ) -> ApiResponse:
        if method == "POST":
            _validate_query_fields(query, set())
            data = _require_body(body, {"name", "comment"})
            name = _required_string(data, "name")
            comment = _optional_string(data, "comment")
            record = self._service.create_catalog(name, comment=comment)
            return _ok(_catalog_payload(record))
        if method == "GET":
            _validate_query_fields(
                query, {"include_browse", "include_unbound", "max_results", "page_token"}
            )
            _optional_bool_query(query, "include_browse")
            _optional_bool_query(query, "include_unbound")
            page_size = _page_size(query)
            records = self._service.list_catalogs()
            page, next_token = _page(records, "catalogs", query, page_size)
            payload: dict[str, object] = {"catalogs": [_catalog_payload(item) for item in page]}
            if next_token is not None:
                payload["next_page_token"] = next_token
            return _ok(payload)
        return _method_not_allowed("GET, POST")

    def _catalog_member(
        self,
        method: str,
        name: str,
        query: dict[str, list[str]],
    ) -> ApiResponse:
        if method == "GET":
            _validate_query_fields(query, {"include_browse"})
            _optional_bool_query(query, "include_browse")
            return _ok(_catalog_payload(self._service.get_catalog(name)))
        if method == "DELETE":
            _validate_query_fields(query, {"force"})
            _optional_bool_query(query, "force")
            self._service.delete_catalog(name)
            return _ok({})
        _validate_query_fields(query, set())
        return _method_not_allowed("GET, DELETE")

    def _schema_collection(
        self,
        method: str,
        query: dict[str, list[str]],
        body: dict[str, object] | None,
    ) -> ApiResponse:
        if method == "POST":
            _validate_query_fields(query, set())
            data = _require_body(body, {"catalog_name", "name", "comment"})
            catalog_name = _required_string(data, "catalog_name")
            name = _required_string(data, "name")
            comment = _optional_string(data, "comment")
            record = self._service.create_schema(catalog_name, name, comment=comment)
            return _ok(_schema_payload(record))
        if method == "GET":
            _validate_query_fields(
                query, {"catalog_name", "include_browse", "max_results", "page_token"}
            )
            _optional_bool_query(query, "include_browse")
            catalog_name = _required_query_string(query, "catalog_name")
            page_size = _page_size(query)
            records = self._service.list_schemas(catalog_name)
            scope = f"schemas:{catalog_name}"
            page, next_token = _page(records, scope, query, page_size)
            payload: dict[str, object] = {"schemas": [_schema_payload(item) for item in page]}
            if next_token is not None:
                payload["next_page_token"] = next_token
            return _ok(payload)
        return _method_not_allowed("GET, POST")

    def _schema_member(
        self,
        method: str,
        full_name: str,
        query: dict[str, list[str]],
    ) -> ApiResponse:
        parts = full_name.split(".")
        if len(parts) != 2 or not all(parts):
            return _error(
                HTTPStatus.BAD_REQUEST,
                "INVALID_PARAMETER_VALUE",
                "schema full_name must contain catalog and schema names",
            )
        catalog_name, schema_name = parts
        if method == "GET":
            _validate_query_fields(query, {"include_browse"})
            _optional_bool_query(query, "include_browse")
            return _ok(_schema_payload(self._service.get_schema(catalog_name, schema_name)))
        if method == "DELETE":
            _validate_query_fields(query, {"force"})
            _optional_bool_query(query, "force")
            self._service.delete_schema(catalog_name, schema_name)
            return _ok({})
        _validate_query_fields(query, set())
        return _method_not_allowed("GET, DELETE")


def _catalog_payload(record: CatalogRecord) -> dict[str, object]:
    return {
        "catalog_type": "MANAGED_CATALOG",
        "comment": record.comment,
        "full_name": record.name,
        "name": record.name,
    }


def _schema_payload(record: SchemaRecord) -> dict[str, object]:
    return {
        "catalog_name": record.catalog_name,
        "comment": record.comment,
        "full_name": f"{record.catalog_name}.{record.name}",
        "name": record.name,
        "schema_id": record.schema_id,
    }


def _require_body(
    body: dict[str, object] | None,
    allowed: set[str],
) -> dict[str, object]:
    if body is None:
        raise _InputError("JSON object body is required")
    unsupported = set(body) - allowed
    if unsupported:
        raise _InputError(f"unsupported request field(s): {', '.join(sorted(unsupported))}")
    return body


def _required_string(body: dict[str, object], key: str) -> str:
    value = body.get(key)
    if not isinstance(value, str) or not value:
        raise _InputError(f"{key} must be a non-empty string")
    return value


def _optional_string(body: dict[str, object], key: str) -> str | None:
    value = body.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise _InputError(f"{key} must be a string or null")
    return value


def _required_query_string(query: dict[str, list[str]], key: str) -> str:
    value = _single_query_value(query, key)
    if not value:
        raise _InputError(f"query parameter {key} is required")
    return value


def _optional_bool_query(query: dict[str, list[str]], key: str) -> bool | None:
    value = _single_query_value(query, key)
    if value is None:
        return None
    if value.lower() not in {"true", "false"}:
        raise _InputError(f"query parameter {key} must be true or false")
    return value.lower() == "true"


def _validate_query_fields(query: dict[str, list[str]], allowed: set[str]) -> None:
    unsupported = set(query) - allowed
    if unsupported:
        raise _InputError(f"unsupported query parameter(s): {', '.join(sorted(unsupported))}")


def _single_query_value(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    if values is None:
        return None
    if len(values) != 1:
        raise _InputError(f"query parameter {key} must be specified once")
    return values[0]


def _page_size(query: dict[str, list[str]]) -> int | None:
    value = _single_query_value(query, "max_results")
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError as error:
        raise _InputError("max_results must be an integer") from error
    if parsed < 0:
        raise _InputError("max_results must be zero or greater")
    return _DEFAULT_PAGE_SIZE if parsed == 0 else parsed


def _page(
    records: list[_PageItem],
    scope: str,
    query: dict[str, list[str]],
    page_size: int | None,
) -> tuple[list[_PageItem], str | None]:
    token = _single_query_value(query, "page_token")
    offset = _decode_page_token(token, scope) if token else 0
    if offset > len(records):
        raise _InputError("page_token is outside the current result set")
    if page_size is None:
        return records[offset:], None
    page = records[offset : offset + page_size]
    next_offset = offset + len(page)
    next_token = _encode_page_token(scope, next_offset) if next_offset < len(records) else None
    return page, next_token


def _encode_page_token(scope: str, offset: int) -> str:
    raw = json.dumps([scope, offset], separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_page_token(token: str, expected_scope: str) -> int:
    try:
        raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
        value = json.loads(raw)
    except (ValueError, binascii.Error, json.JSONDecodeError) as error:
        raise _InputError("page_token is invalid") from error
    if (
        not isinstance(value, list)
        or len(value) != 2
        or value[0] != expected_scope
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
