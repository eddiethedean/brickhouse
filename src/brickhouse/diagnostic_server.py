"""Loopback HTTP endpoints for local BrickHouse diagnostics."""

from __future__ import annotations

import ipaddress
import json
import signal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from brickhouse.api.catalog import ApiBinaryResponse, ApiResponse, CatalogHttpApi
from brickhouse.api.compute import ComputeHttpApi
from brickhouse.api.host_metadata import HostMetadataHttpApi
from brickhouse.api.jobs import JobsHttpApi
from brickhouse.api.statements import SqlStatementHttpApi
from brickhouse.api.warehouses import SqlWarehouseHttpApi
from brickhouse.api.workspace_files import (
    MAX_WORKSPACE_IMPORT_REQUEST_BYTES,
    WorkspaceFileHttpApi,
)
from brickhouse.diagnostics import capability_report
from brickhouse.workspace.context import WorkspaceContext


def require_ipv4_loopback_host(host: str) -> None:
    """Reject network binds because this compatibility server has no caller auth."""
    try:
        address = ipaddress.ip_address(host)
    except ValueError as error:
        raise ValueError(
            "the diagnostics server currently accepts IPv4 loopback hosts only"
        ) from error
    if not isinstance(address, ipaddress.IPv4Address) or not address.is_loopback:
        raise ValueError("the unauthenticated diagnostics server must bind to IPv4 loopback")


class _DiagnosticHttpServer(ThreadingHTTPServer):
    def __init__(
        self,
        address: tuple[str, int],
        handler: type[BaseHTTPRequestHandler],
        jobs_api: JobsHttpApi,
        statement_api: SqlStatementHttpApi,
    ) -> None:
        self._jobs_api = jobs_api
        self._statement_api = statement_api
        super().__init__(address, handler)

    def server_close(self) -> None:
        try:
            try:
                self._statement_api.close()
            finally:
                self._jobs_api.close()
        finally:
            super().server_close()


def create_diagnostic_server(
    context: WorkspaceContext,
    host: str,
    port: int,
) -> ThreadingHTTPServer:
    """Build the loopback server for diagnostics and the local catalog API."""
    require_ipv4_loopback_host(host)
    jobs_api = JobsHttpApi(context)
    statement_api = SqlStatementHttpApi(context)

    def handler_factory(
        active_context: WorkspaceContext,
    ) -> type[BaseHTTPRequestHandler]:
        catalog_api = CatalogHttpApi(active_context)
        compute_api = ComputeHttpApi(active_context)
        warehouse_api = SqlWarehouseHttpApi(active_context)
        host_metadata_api = HostMetadataHttpApi(active_context)
        workspace_files_api = WorkspaceFileHttpApi(active_context)

        class DiagnosticHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                api_response: ApiResponse | ApiBinaryResponse | None = host_metadata_api.dispatch(
                    "GET", self.path
                )
                if api_response is not None:
                    self._send_payload(api_response)
                    return
                api_response = workspace_files_api.dispatch_raw(
                    "GET", self.path, b"", self.headers.get("Content-Type")
                )
                if api_response is None:
                    api_response = jobs_api.dispatch("GET", self.path)
                if api_response is None:
                    api_response = statement_api.dispatch("GET", self.path)
                if api_response is not None:
                    self._send_payload(api_response)
                    return
                api_response = compute_api.dispatch("GET", self.path)
                if api_response is None:
                    api_response = warehouse_api.dispatch("GET", self.path)
                if api_response is None:
                    api_response = catalog_api.dispatch("GET", self.path)
                if api_response is not None:
                    self._send_payload(api_response)
                    return
                route = urlsplit(self.path).path
                unsupported = _unsupported_resource_response(route)
                if unsupported is not None:
                    self._send_payload(unsupported)
                    return
                if route == "/health":
                    payload: object = {
                        "status": "ok",
                        "workspace_format_version": active_context.workspace.config.format_version,
                    }
                elif route == "/version":
                    payload = capability_report(active_context.workspace)["brickhouse_version"]
                elif route == "/capabilities":
                    payload = capability_report(active_context.workspace)
                else:
                    self.send_error(HTTPStatus.NOT_FOUND)
                    return
                self._send_payload(ApiResponse(HTTPStatus.OK, payload))

            def do_POST(self) -> None:
                self._dispatch_mutation("POST")

            def do_DELETE(self) -> None:
                self._dispatch_mutation("DELETE")

            def do_PUT(self) -> None:
                self._dispatch_mutation("PUT")

            def do_PATCH(self) -> None:
                self._dispatch_mutation("PATCH")

            def _dispatch_mutation(self, method: str) -> None:
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                except ValueError:
                    self._send_payload(
                        ApiResponse(
                            HTTPStatus.BAD_REQUEST,
                            {
                                "error_code": "INVALID_PARAMETER_VALUE",
                                "message": "invalid Content-Length",
                            },
                        )
                    )
                    return
                maximum_length = (
                    MAX_WORKSPACE_IMPORT_REQUEST_BYTES
                    if workspace_files_api.handles_target(self.path)
                    else 1_048_576
                )
                if length < 0 or length > maximum_length:
                    self._send_payload(
                        ApiResponse(
                            HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                            {
                                "error_code": "INVALID_PARAMETER_VALUE",
                                "message": "request body is too large",
                            },
                        )
                    )
                    return
                raw_body = self.rfile.read(length) if length else b""
                api_response = workspace_files_api.dispatch_raw(
                    method, self.path, raw_body, self.headers.get("Content-Type")
                )
                if api_response is not None:
                    self._send_payload(api_response)
                    return
                body: dict[str, object] | None = None
                if length:
                    try:
                        decoded: Any = json.loads(raw_body)
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        self._send_payload(
                            ApiResponse(
                                HTTPStatus.BAD_REQUEST,
                                {
                                    "error_code": "INVALID_PARAMETER_VALUE",
                                    "message": "request body must be valid JSON",
                                },
                            )
                        )
                        return
                    if not isinstance(decoded, dict):
                        self._send_payload(
                            ApiResponse(
                                HTTPStatus.BAD_REQUEST,
                                {
                                    "error_code": "INVALID_PARAMETER_VALUE",
                                    "message": "request body must be a JSON object",
                                },
                            )
                        )
                        return
                    body = decoded
                api_response = jobs_api.dispatch(method, self.path, body)
                if api_response is None:
                    api_response = statement_api.dispatch(method, self.path, body)
                if api_response is None:
                    api_response = compute_api.dispatch(method, self.path, body)
                if api_response is None:
                    api_response = warehouse_api.dispatch(method, self.path, body)
                if api_response is None:
                    api_response = catalog_api.dispatch(method, self.path, body)
                if api_response is None:
                    api_response = _unsupported_resource_response(urlsplit(self.path).path)
                if api_response is None:
                    self.send_error(HTTPStatus.NOT_FOUND)
                    return
                self._send_payload(api_response)

            def _send_payload(self, response: ApiResponse | ApiBinaryResponse) -> None:
                if isinstance(response, ApiBinaryResponse):
                    encoded = response.body
                elif response.payload is None:
                    encoded = b""
                else:
                    encoded = json.dumps(response.payload, sort_keys=True).encode("utf-8")
                self.send_response(response.status)
                content_type = (
                    response.content_type
                    if isinstance(response, ApiBinaryResponse)
                    else "application/json; charset=utf-8"
                )
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def log_message(self, format: str, *args: object) -> None:
                # Request paths are user-controlled and diagnostics need no access log.
                return

        return DiagnosticHandler

    server = _DiagnosticHttpServer((host, port), handler_factory(context), jobs_api, statement_api)
    server.daemon_threads = True
    return server


def serve_diagnostics(context: WorkspaceContext, host: str, port: int) -> None:
    """Serve diagnostics and local catalog endpoints until interrupted."""
    require_ipv4_loopback_host(host)
    with create_diagnostic_server(context, host, port) as server:
        previous_sigterm = signal.signal(signal.SIGTERM, _interrupt_on_sigterm)
        print(
            f"BrickHouse diagnostics listening on http://{host}:{server.server_port}",
            flush=True,
        )
        try:
            server.serve_forever(poll_interval=0.2)
        except KeyboardInterrupt:
            # Ctrl-C and SIGTERM both use the same deterministic shutdown path.
            pass
        finally:
            signal.signal(signal.SIGTERM, previous_sigterm)


def _interrupt_on_sigterm(signum: int, frame: object) -> None:
    del signum, frame
    raise KeyboardInterrupt


def _unsupported_resource_response(path: str) -> ApiResponse | None:
    """Return a clear API rejection for known resource families outside this profile."""
    segments = path.strip("/").split("/")
    if (
        len(segments) >= 3
        and segments[0] == "api"
        and segments[1] in {"2.0", "2.1", "2.2"}
        and segments[2] in {"pipelines", "apps", "dashboards"}
    ):
        resource = segments[2]
        return ApiResponse(
            HTTPStatus.BAD_REQUEST,
            {
                "error_code": "INVALID_PARAMETER_VALUE",
                "message": f"{resource} resources are unsupported by the local bundle profile",
            },
        )
    return None
