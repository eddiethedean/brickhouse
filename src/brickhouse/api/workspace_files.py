"""Pinned SDK workspace Python-source import route."""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from email import policy
from email.parser import BytesParser
from http import HTTPStatus
from typing import cast
from urllib.parse import parse_qs, urlsplit

from brickhouse.api.catalog import ApiBinaryResponse, ApiResponse
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.files import (
    MAX_WORKSPACE_FILE_BYTES,
    InvalidWorkspaceFileError,
    UnsupportedWorkspaceFileError,
    WorkspaceFileAlreadyExistsError,
    WorkspaceFileError,
    WorkspaceFileNotFoundError,
    WorkspaceFileService,
)

_IMPORT_PATH = "/api/2.0/workspace/import"
_GET_STATUS_PATH = "/api/2.0/workspace/get-status"
_EXPORT_PATH = "/api/2.0/workspace/export"
_DELETE_PATH = "/api/2.0/workspace/delete"
_MKDIRS_PATH = "/api/2.0/workspace/mkdirs"
MAX_WORKSPACE_IMPORT_REQUEST_BYTES = 1_500_000
_ALLOWED_IMPORT_FIELDS = {"content", "format", "language", "overwrite", "path"}


@dataclass(frozen=True, slots=True)
class _WorkspacePythonImport:
    path: str
    content: bytes
    overwrite: bool


class WorkspaceFileHttpApi:
    """Expose only the SDK workspace Python source import behavior used by jobs."""

    def __init__(self, context: WorkspaceContext) -> None:
        self._files = WorkspaceFileService(context)

    @staticmethod
    def handles_target(target: str) -> bool:
        return urlsplit(target).path == _IMPORT_PATH

    def dispatch_raw(
        self,
        method: str,
        target: str,
        raw_body: bytes,
        content_type: str | None,
    ) -> ApiResponse | ApiBinaryResponse | None:
        parsed = urlsplit(target)
        if parsed.path == _EXPORT_PATH:
            if method != "GET":
                return _method_not_allowed("GET")
            try:
                query = parse_qs(parsed.query, keep_blank_values=True)
                if (
                    set(query) - {"path", "direct_download", "format"}
                    or len(query.get("path", [])) != 1
                    or len(query.get("direct_download", [])) > 1
                    or len(query.get("format", [])) > 1
                ):
                    raise _ImportInputError("workspace export query parameters are invalid")
                direct_download = query.get("direct_download", ["false"])[0].casefold()
                if direct_download not in {"true", "false"}:
                    raise _ImportInputError("direct_download must be true or false")
                export_format = query.get("format", ["RAW"])[0]
                if export_format not in {"RAW", "AUTO"}:
                    raise _ImportInputError("only RAW and AUTO workspace exports are supported")
                export_path = query["path"][0]
                record = self._files.get_workspace_object(_internal_workspace_path(export_path))
                if record.object_type != "FILE":
                    raise UnsupportedWorkspaceFileError(
                        "workspace export supports files only in the local bundle profile"
                    )
                content = record.local_path.read_bytes()
                if direct_download == "true":
                    return ApiBinaryResponse(HTTPStatus.OK, content)
                return ApiResponse(
                    HTTPStatus.OK,
                    {"file_type": "RAW", "content": base64.b64encode(content).decode("ascii")},
                )
            except WorkspaceFileNotFoundError as error:
                return _not_found(str(error))
            except (
                _ImportInputError,
                InvalidWorkspaceFileError,
                UnsupportedWorkspaceFileError,
            ) as error:
                return _error(str(error))
            except (OSError, WorkspaceFileError):
                return _internal_error()

        if parsed.path == _GET_STATUS_PATH:
            if method != "GET":
                return _method_not_allowed("GET")
            try:
                query = parse_qs(parsed.query, keep_blank_values=True)
                if (
                    set(query) - {"path", "return_export_info"}
                    or len(query.get("path", [])) != 1
                    or len(query.get("return_export_info", [])) > 1
                    or query.get("return_export_info", ["false"])[0].casefold()
                    not in {"true", "false"}
                ):
                    raise _ImportInputError(
                        "workspace get-status requires one path and an optional boolean "
                        "return_export_info"
                    )
                requested_path = query["path"][0]
                record = self._files.get_workspace_object(_internal_workspace_path(requested_path))
                payload: dict[str, object] = {
                    "path": requested_path,
                    "object_type": record.object_type,
                }
                if record.object_type == "FILE" and record.local_path.suffix == ".py":
                    payload["language"] = "PYTHON"
                if record.size_bytes is not None:
                    payload["size"] = record.size_bytes
                return ApiResponse(HTTPStatus.OK, payload)
            except WorkspaceFileNotFoundError as error:
                return _not_found(str(error))
            except (
                _ImportInputError,
                InvalidWorkspaceFileError,
                UnsupportedWorkspaceFileError,
            ) as error:
                return _error(str(error))
            except WorkspaceFileError:
                return _internal_error()

        if parsed.path == _DELETE_PATH:
            if method != "POST":
                return _method_not_allowed("POST")
            if parsed.query:
                return _error("workspace delete does not accept query parameters")
            try:
                body = _decode_json(raw_body)
                unknown = set(body) - {"path", "recursive"}
                if unknown:
                    raise _ImportInputError("workspace delete contains unsupported fields")
                delete_path = body.get("path")
                if not isinstance(delete_path, str) or not delete_path:
                    raise _ImportInputError("workspace delete path must be a non-empty string")
                recursive = body.get("recursive", False)
                if type(recursive) is not bool:
                    raise _ImportInputError("recursive must be a boolean")
                self._files.delete_workspace_object(
                    _internal_workspace_path(delete_path), recursive=recursive
                )
                return ApiResponse(HTTPStatus.OK, {})
            except WorkspaceFileNotFoundError as error:
                return _not_found(str(error))
            except (
                _ImportInputError,
                InvalidWorkspaceFileError,
                UnsupportedWorkspaceFileError,
            ) as error:
                return _error(str(error))
            except WorkspaceFileError:
                return _internal_error()

        if parsed.path == _MKDIRS_PATH:
            if method != "POST":
                return _method_not_allowed("POST")
            if parsed.query:
                return _error("workspace mkdirs does not accept query parameters")
            try:
                body = _decode_json(raw_body)
                if set(body) != {"path"}:
                    raise _ImportInputError("workspace mkdirs requires only path")
                directory_path = body.get("path")
                if not isinstance(directory_path, str) or not directory_path:
                    raise _ImportInputError("workspace mkdirs path must be a non-empty string")
                self._files.create_directory(_internal_workspace_path(directory_path))
                return ApiResponse(HTTPStatus.OK, {})
            except (
                _ImportInputError,
                InvalidWorkspaceFileError,
                UnsupportedWorkspaceFileError,
            ) as error:
                return _error(str(error))
            except WorkspaceFileError:
                return _internal_error()

        if parsed.path != _IMPORT_PATH:
            return None
        if method != "POST":
            return ApiResponse(
                HTTPStatus.METHOD_NOT_ALLOWED,
                {"error_code": "METHOD_NOT_ALLOWED", "message": "only POST is supported"},
            )
        if parsed.query:
            return _error("unsupported workspace import query parameter")
        if content_type is None:
            return _error("workspace import requires a supported Content-Type")
        media_type = content_type.split(";", 1)[0].strip().casefold()
        try:
            if media_type == "application/json":
                body = _decode_json(raw_body)
                values = _decode_json_import(body)
            elif media_type == "multipart/form-data":
                values = _decode_multipart_import(raw_body, content_type)
            else:
                return _error("workspace import Content-Type is unsupported")
            if values.path.endswith(".py"):
                file_record = self._files.import_python(
                    values.path,
                    values.content,
                    overwrite=values.overwrite,
                )
            else:
                file_record = self._files.import_bundle_json(
                    values.path,
                    values.content,
                    overwrite=values.overwrite,
                )
            return ApiResponse(HTTPStatus.OK, {"path": file_record.workspace_path})
        except WorkspaceFileAlreadyExistsError as error:
            return ApiResponse(
                HTTPStatus.CONFLICT,
                {"error_code": "ALREADY_EXISTS", "message": str(error)},
            )
        except _ImportInputError as error:
            return _error(str(error))
        except WorkspaceFileNotFoundError as error:
            return _not_found(str(error))
        except InvalidWorkspaceFileError as error:
            return _error(str(error))
        except UnsupportedWorkspaceFileError as error:
            return _error(str(error))
        except WorkspaceFileError:
            return ApiResponse(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"error_code": "INTERNAL_ERROR", "message": "workspace file operation failed"},
            )


def _decode_json(raw_body: bytes) -> dict[str, object]:
    try:
        decoded: object = json.loads(raw_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise _ImportInputError("workspace import body must be valid JSON") from error
    if not isinstance(decoded, dict):
        raise _ImportInputError("workspace import body must be a JSON object")
    return cast(dict[str, object], decoded)


def _decode_json_import(body: dict[str, object]) -> _WorkspacePythonImport:
    unknown = set(body) - _ALLOWED_IMPORT_FIELDS
    if unknown:
        fields = ", ".join(sorted(unknown))
        raise _ImportInputError(f"unsupported workspace import field(s): {fields}")
    content = body.get("content")
    if not isinstance(content, str):
        raise _ImportInputError("content must be base64-encoded source text")
    max_encoded_length = ((MAX_WORKSPACE_FILE_BYTES + 2) // 3) * 4
    if len(content) > max_encoded_length:
        raise _ImportInputError("workspace Python source exceeds the local size limit")
    try:
        decoded = base64.b64decode(content, validate=True)
    except (binascii.Error, ValueError) as error:
        raise _ImportInputError("content must be valid base64") from error
    return _validate_import_fields(body, decoded)


def _decode_multipart_import(raw_body: bytes, content_type: str) -> _WorkspacePythonImport:
    message = BytesParser(policy=policy.default).parsebytes(
        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("ascii") + raw_body
    )
    if not message.is_multipart():
        raise _ImportInputError("multipart workspace import body is malformed")
    values: dict[str, object] = {}
    for part in message.iter_parts():
        field_name = part.get_param("name", header="content-disposition")
        if not isinstance(field_name, str) or field_name not in _ALLOWED_IMPORT_FIELDS:
            raise _ImportInputError("multipart workspace import contains an unsupported field")
        if field_name in values:
            raise _ImportInputError("multipart workspace import fields cannot be repeated")
        payload = part.get_payload(decode=True)
        if payload is None:
            payload = b""
        elif not isinstance(payload, bytes):
            raise _ImportInputError("multipart workspace import fields must be binary data")
        if field_name == "content":
            if len(payload) > MAX_WORKSPACE_FILE_BYTES:
                raise _ImportInputError("workspace Python source exceeds the local size limit")
            values[field_name] = payload
        else:
            try:
                values[field_name] = payload.decode("utf-8")
            except UnicodeDecodeError as error:
                raise _ImportInputError("workspace import fields must be UTF-8") from error
    content = values.pop("content", None)
    if not isinstance(content, bytes):
        raise _ImportInputError("multipart workspace import requires a content file")
    return _validate_import_fields(values, content)


def _validate_import_fields(body: dict[str, object], content: bytes) -> _WorkspacePythonImport:
    path = body.get("path")
    if not isinstance(path, str) or not path:
        raise _ImportInputError("path must be a non-empty string")
    format_value = body.get("format")
    if format_value not in (None, "SOURCE", "RAW"):
        raise _ImportInputError("only SOURCE and RAW workspace imports are supported")
    language = body.get("language")
    if language not in (None, "PYTHON"):
        raise _ImportInputError("only PYTHON workspace source imports are supported")
    overwrite_value = body.get("overwrite", False)
    if isinstance(overwrite_value, str):
        if overwrite_value.casefold() not in {"true", "false"}:
            raise _ImportInputError("overwrite must be true or false")
        overwrite = overwrite_value.casefold() == "true"
    elif type(overwrite_value) is bool:
        overwrite = overwrite_value
    else:
        raise _ImportInputError("overwrite must be a boolean")
    internal_path = _internal_workspace_path(path)
    if format_value == "RAW" and not internal_path.endswith((".py", ".json")):
        raise _ImportInputError("RAW workspace imports support Python source and bundle JSON files")
    if format_value == "SOURCE" and not internal_path.endswith(".py"):
        raise _ImportInputError("SOURCE workspace imports are supported only for Python files")
    return _WorkspacePythonImport(internal_path, content, overwrite)


def _internal_workspace_path(path: str) -> str:
    if path.startswith("/Workspace/"):
        return path
    if path.startswith("/") and not path.startswith("//"):
        return f"/Workspace{path}"
    raise _ImportInputError("workspace path must be an absolute path")


def _method_not_allowed(allow: str) -> ApiResponse:
    return ApiResponse(
        HTTPStatus.METHOD_NOT_ALLOWED,
        {"error_code": "INVALID_PARAMETER_VALUE", "message": f"only {allow} is supported"},
    )


def _not_found(message: str) -> ApiResponse:
    return ApiResponse(
        HTTPStatus.NOT_FOUND,
        {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": message},
    )


def _internal_error() -> ApiResponse:
    return ApiResponse(
        HTTPStatus.INTERNAL_SERVER_ERROR,
        {"error_code": "INTERNAL_ERROR", "message": "workspace file operation failed"},
    )


class _ImportInputError(ValueError):
    """A malformed or unsupported workspace import request."""


def _error(message: str) -> ApiResponse:
    return ApiResponse(
        HTTPStatus.BAD_REQUEST,
        {"error_code": "INVALID_PARAMETER_VALUE", "message": message},
    )
