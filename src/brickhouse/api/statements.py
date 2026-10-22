"""Pinned Databricks Statement Execution routes for the local profile."""

from __future__ import annotations

import re
from http import HTTPStatus
from urllib.parse import parse_qs, unquote, urlsplit

from brickhouse.api.catalog import ApiResponse
from brickhouse.catalog.repository import CatalogRepositoryError
from brickhouse.compute.models import (
    ComputeConflictError,
    ComputeNotFoundError,
    InvalidComputeRequestError,
)
from brickhouse.sql.parameters import (
    SqlStatementParameter,
    SqlStatementParameterError,
)
from brickhouse.sql.results import MAX_INLINE_RESULT_BYTES, MAX_INLINE_RESULT_ROWS
from brickhouse.sql.sessions import (
    SqlKernelSessionError,
    SqlKernelSessionNotFoundError,
    SqlKernelSessionService,
    UnsupportedSqlKernelSessionOperationError,
)
from brickhouse.sql.statements import (
    SqlStatementChunkNotFoundError,
    SqlStatementExecutionService,
    SqlStatementNotFoundError,
    SqlStatementRepositoryError,
    SqlStatementStateError,
)
from brickhouse.workspace.context import WorkspaceContext

_PREFIX = "/api/2.0/sql/statements"
_SESSIONS_PATH = "/api/2.0/sql/sessions"
_SESSION_FIELDS = {"warehouse_id", "execution_mode", "session_confs", "catalog", "schema"}
_DELETE_SESSION_FIELDS = {"warehouse_id", "session_id"}
_DELETE_STATEMENT_FIELDS = {"statement_id"}
_ALLOWED_REQUEST_FIELDS = {
    "statement",
    "warehouse_id",
    "catalog",
    "schema",
    "disposition",
    "format",
    "row_limit",
    "byte_limit",
    "wait_timeout",
    "on_wait_timeout",
    "parameters",
    "session_id",
    "result_compression",
    "query_tags",
}
_MAX_STATEMENT_PARAMETERS = 100
_MAX_PARAMETER_BYTES = 1_048_576


class SqlStatementHttpApi:
    """Translate pinned SDK calls into persisted local statement outcomes."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._service = SqlStatementExecutionService(context)
        self._sessions = SqlKernelSessionService(context)

    def close(self) -> None:
        """Wait for owned continuations before the workspace context is released."""
        try:
            self._sessions.close_all()
        finally:
            self._service.close()

    def dispatch(
        self,
        method: str,
        target: str,
        body: dict[str, object] | None = None,
    ) -> ApiResponse | None:
        parsed = urlsplit(target)
        if parsed.path == _SESSIONS_PATH:
            if method != "POST":
                return _method_not_allowed("POST")
            return self._create_session(parsed.query, body)
        if parsed.path.startswith(f"{_SESSIONS_PATH}/"):
            return self._delete_session(method, parsed.path, parsed.query, body)
        if parsed.path != _PREFIX and not parsed.path.startswith(f"{_PREFIX}/"):
            return None
        query = parse_qs(parsed.query, keep_blank_values=True)
        segments = [
            unquote(part) for part in parsed.path[len(_PREFIX) :].strip("/").split("/") if part
        ]
        try:
            if not segments:
                if method != "POST":
                    return _method_not_allowed("POST")
                return self._submit(query, body)
            if len(segments) == 1:
                _validate_query(query, set())
                if method == "GET":
                    return _ok(self._service.get(segments[0]))
                if method == "DELETE":
                    data = body or {}
                    unsupported = set(data) - _DELETE_STATEMENT_FIELDS
                    if unsupported:
                        fields = ", ".join(sorted(unsupported))
                        raise _InputError(f"unsupported statement close field(s): {fields}")
                    body_statement_id = _optional_string(data, "statement_id")
                    if body_statement_id is not None and body_statement_id != segments[0]:
                        raise _InputError("statement_id in the path and request body must match")
                    self._service.close_result(segments[0])
                    return ApiResponse(HTTPStatus.OK, None)
                return _method_not_allowed("GET, DELETE")
            if len(segments) == 3 and segments[1] == "result" and segments[2].startswith("chunks"):
                return _error(
                    HTTPStatus.NOT_FOUND,
                    "RESOURCE_DOES_NOT_EXIST",
                    "API route not found",
                )
            if len(segments) == 4 and segments[1:3] == ["result", "chunks"]:
                if method != "GET":
                    return _method_not_allowed("GET")
                _validate_query(query, set())
                try:
                    chunk_index = int(segments[3])
                except ValueError as error:
                    raise _InputError(
                        "result chunk index must be a non-negative integer"
                    ) from error
                if chunk_index < 0:
                    raise _InputError("result chunk index must be a non-negative integer")
                return _ok(self._service.get_chunk(segments[0], chunk_index))
            if len(segments) == 2 and segments[1] == "cancel":
                if method != "POST":
                    return _method_not_allowed("POST")
                _validate_query(query, set())
                self._service.cancel(
                    segments[0],
                    message="SQL statement cancellation was requested by the client",
                )
                return ApiResponse(HTTPStatus.OK, None)
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", "API route not found")
        except (_InputError, SqlStatementParameterError) as error:
            return _error(HTTPStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", str(error))
        except (CatalogRepositoryError, InvalidComputeRequestError) as error:
            return _error(HTTPStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", str(error))
        except UnsupportedSqlKernelSessionOperationError as error:
            return _error(HTTPStatus.NOT_IMPLEMENTED, "UNSUPPORTED_OPERATION", str(error))
        except SqlKernelSessionNotFoundError as error:
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", str(error))
        except SqlKernelSessionError as error:
            return _error(HTTPStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", str(error))
        except ComputeConflictError as error:
            return _error(HTTPStatus.CONFLICT, "RESOURCE_CONFLICT", str(error))
        except ComputeNotFoundError as error:
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", str(error))
        except SqlStatementNotFoundError as error:
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", str(error))
        except SqlStatementChunkNotFoundError as error:
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", str(error))
        except SqlStatementStateError as error:
            return _error(HTTPStatus.CONFLICT, "RESOURCE_CONFLICT", str(error))
        except SqlStatementRepositoryError:
            return _error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR",
                "SQL statement metadata operation failed",
            )

    def _create_session(
        self,
        raw_query: str,
        body: dict[str, object] | None,
    ) -> ApiResponse:
        try:
            _validate_query(parse_qs(raw_query, keep_blank_values=True), set())
            data = _require_body(body)
            unsupported = set(data) - _SESSION_FIELDS
            if unsupported:
                fields = ", ".join(sorted(unsupported))
                raise _InputError(f"unsupported SQL session field(s): {fields}")
            session_confs = data.get("session_confs")
            if session_confs is not None and not isinstance(session_confs, dict):
                raise _InputError("session_confs must be a JSON object")
            record = self._sessions.open(
                warehouse_id=_required_string(data, "warehouse_id"),
                execution_mode=_optional_string(data, "execution_mode") or "FAST",
                catalog=_optional_string(data, "catalog"),
                schema=_optional_string(data, "schema"),
                session_confs=session_confs,
            )
            return _ok({"session_id": record.session_id})
        except UnsupportedSqlKernelSessionOperationError as error:
            return _error(HTTPStatus.NOT_IMPLEMENTED, "UNSUPPORTED_OPERATION", str(error))
        except _InputError as error:
            return _error(HTTPStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", str(error))
        except SqlKernelSessionError as error:
            return _error(HTTPStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", str(error))
        except SqlKernelSessionNotFoundError as error:
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", str(error))
        except ComputeConflictError as error:
            return _error(HTTPStatus.CONFLICT, "RESOURCE_CONFLICT", str(error))
        except ComputeNotFoundError as error:
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", str(error))

    def _delete_session(
        self,
        method: str,
        path: str,
        raw_query: str,
        body: dict[str, object] | None,
    ) -> ApiResponse:
        session_id = unquote(path[len(_SESSIONS_PATH) :].strip("/"))
        if not session_id or "/" in session_id:
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", "API route not found")
        if method != "DELETE":
            return _method_not_allowed("DELETE")
        try:
            query = parse_qs(raw_query, keep_blank_values=True)
            _validate_query(query, {"warehouse_id"})
            query_warehouse_id = query["warehouse_id"][0] if "warehouse_id" in query else None
            data = body or {}
            unsupported = set(data) - _DELETE_SESSION_FIELDS
            if unsupported:
                fields = ", ".join(sorted(unsupported))
                raise _InputError(f"unsupported SQL session field(s): {fields}")
            body_session_id = _optional_string(data, "session_id")
            if body_session_id is not None and body_session_id != session_id:
                raise _InputError("session_id in the path and request body must match")
            body_warehouse_id = _optional_string(data, "warehouse_id")
            if (
                query_warehouse_id is not None
                and body_warehouse_id is not None
                and query_warehouse_id != body_warehouse_id
            ):
                raise _InputError("warehouse_id query and request body values must match")
            warehouse_id = body_warehouse_id or query_warehouse_id
            self._sessions.close(session_id, warehouse_id=warehouse_id)
            return ApiResponse(HTTPStatus.OK, None)
        except _InputError as error:
            return _error(HTTPStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", str(error))
        except SqlKernelSessionError as error:
            return _error(HTTPStatus.BAD_REQUEST, "INVALID_PARAMETER_VALUE", str(error))
        except SqlKernelSessionNotFoundError as error:
            return _error(HTTPStatus.NOT_FOUND, "RESOURCE_DOES_NOT_EXIST", str(error))

    def _submit(
        self,
        query: dict[str, list[str]],
        body: dict[str, object] | None,
    ) -> ApiResponse:
        _validate_query(query, set())
        data = _require_body(body)
        unsupported = set(data) - _ALLOWED_REQUEST_FIELDS
        if unsupported:
            fields = ", ".join(sorted(unsupported))
            raise _InputError(f"unsupported statement request field(s): {fields}")
        statement = _required_string(data, "statement")
        warehouse_id = _required_string(data, "warehouse_id")
        session_id = _optional_string(data, "session_id")
        if len(statement.encode("utf-8")) > 1_048_576:
            raise _InputError("statement text exceeds the 1 MiB local limit")
        catalog = _optional_string(data, "catalog")
        schema = _optional_string(data, "schema")
        if session_id is not None:
            session = self._sessions.get(session_id, warehouse_id=warehouse_id)
            if catalog is None:
                catalog = session.catalog
            if schema is None:
                schema = session.schema
        disposition = _optional_string(data, "disposition")
        result_format = _optional_string(data, "format")
        result_compression = _optional_string(data, "result_compression")
        if disposition not in (None, "INLINE"):
            raise _InputError("the local SQL profile supports only disposition=INLINE")
        if result_format not in (None, "JSON_ARRAY"):
            raise _InputError("the local SQL profile supports only format=JSON_ARRAY")
        if result_compression not in (None, "NONE"):
            raise _InputError("the local SQL profile supports only result_compression=NONE")
        query_tags = data.get("query_tags")
        if query_tags not in (None, []):
            raise _InputError("the local SQL profile does not apply per-statement query_tags")
        row_limit = _optional_bounded_int(
            data,
            "row_limit",
            maximum=MAX_INLINE_RESULT_ROWS,
        )
        byte_limit = _optional_bounded_int(
            data,
            "byte_limit",
            maximum=MAX_INLINE_RESULT_BYTES,
        )
        wait_timeout = _parse_wait_timeout(_optional_string(data, "wait_timeout"))
        on_wait_timeout = _optional_string(data, "on_wait_timeout")
        if on_wait_timeout not in (None, "CONTINUE", "CANCEL"):
            raise _InputError("on_wait_timeout must be CONTINUE or CANCEL")
        parameters = _parse_parameters(data.get("parameters"))
        return _ok(
            self._service.submit(
                sql_text=statement,
                warehouse_id=warehouse_id,
                catalog=catalog,
                schema=schema,
                asynchronous=wait_timeout == 0,
                parameters=parameters,
                row_limit=row_limit,
                byte_limit=byte_limit,
                wait_timeout_seconds=wait_timeout,
                cancel_on_wait_timeout=on_wait_timeout == "CANCEL",
            )
        )


def _require_body(body: dict[str, object] | None) -> dict[str, object]:
    if body is None:
        raise _InputError("statement request body is required")
    return body


def _required_string(body: dict[str, object], field: str) -> str:
    value = body.get(field)
    if not isinstance(value, str) or not value.strip():
        raise _InputError(f"{field} must be a non-empty string")
    return value


def _optional_string(body: dict[str, object], field: str) -> str | None:
    if field not in body or body[field] is None:
        return None
    value = body[field]
    if not isinstance(value, str) or not value.strip():
        raise _InputError(f"{field} must be a non-empty string when provided")
    return value


def _optional_bounded_int(
    body: dict[str, object],
    field: str,
    *,
    maximum: int,
) -> int | None:
    if field not in body or body[field] is None:
        return None
    value = body[field]
    if type(value) is not int or not 1 <= value <= maximum:
        raise _InputError(f"{field} must be an integer between 1 and {maximum}")
    return value


def _validate_query(query: dict[str, list[str]], allowed: set[str]) -> None:
    unsupported = set(query) - allowed
    if unsupported:
        names = ", ".join(sorted(unsupported))
        raise _InputError(f"unsupported statement query parameter(s): {names}")
    if any(len(values) != 1 for values in query.values()):
        raise _InputError("statement query parameters must be specified once")


def _parse_parameters(value: object) -> tuple[SqlStatementParameter, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise _InputError("parameters must be a list of named parameter objects")
    if len(value) > _MAX_STATEMENT_PARAMETERS:
        raise _InputError(f"parameters are limited to {_MAX_STATEMENT_PARAMETERS} items")
    parsed: list[SqlStatementParameter] = []
    total_bytes = 0
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise _InputError(f"parameters[{index}] must be an object")
        unsupported = set(item) - {"name", "type", "value"}
        if unsupported:
            fields = ", ".join(sorted(str(field) for field in unsupported))
            raise _InputError(f"unsupported parameter field(s): {fields}")
        name = item.get("name")
        parameter_type = item.get("type")
        parameter_value = item.get("value")
        if not isinstance(name, str) or not name.strip():
            raise _InputError(f"parameters[{index}].name must be a non-empty string")
        if parameter_type is not None and (
            not isinstance(parameter_type, str) or not parameter_type.strip()
        ):
            raise _InputError(f"parameters[{index}].type must be a non-empty string when provided")
        if parameter_value is not None and not isinstance(parameter_value, str):
            raise _InputError(f"parameters[{index}].value must be a string or null")
        total_bytes += len(name.encode("utf-8"))
        if parameter_type is not None:
            total_bytes += len(parameter_type.encode("utf-8"))
        if parameter_value is not None:
            total_bytes += len(parameter_value.encode("utf-8"))
        if total_bytes > _MAX_PARAMETER_BYTES:
            raise _InputError("statement parameters exceed the 1 MiB local limit")
        parsed.append(
            SqlStatementParameter(
                name=name,
                value=parameter_value,
                type=parameter_type,
            )
        )
    return tuple(parsed)


def _parse_wait_timeout(value: str | None) -> int:
    if value is None:
        return 10
    if value == "0s":
        return 0
    if re.fullmatch(r"(?:[5-9]|[1-4][0-9]|50)s", value) is None:
        raise _InputError("wait_timeout must be 0s or an integer from 5s through 50s")
    return int(value[:-1])


class _InputError(ValueError):
    pass


def _ok(payload: object) -> ApiResponse:
    return ApiResponse(HTTPStatus.OK, payload)


def _error(status: HTTPStatus, code: str, message: str) -> ApiResponse:
    return ApiResponse(status, {"error_code": code, "message": message})


def _method_not_allowed(allowed: str) -> ApiResponse:
    return _error(
        HTTPStatus.METHOD_NOT_ALLOWED,
        "METHOD_NOT_ALLOWED",
        f"allowed methods: {allowed}",
    )
