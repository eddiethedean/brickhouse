from __future__ import annotations

import io
import json
import os
import ssl
import subprocess
import time
from datetime import UTC, datetime, timedelta
from ipaddress import ip_address
from pathlib import Path
from threading import Thread
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest

from brickhouse.diagnostic_server import create_diagnostic_server
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace

_CONNECTOR_PYTHON_ENV = "BRICKHOUSE_SQL_CONNECTOR_PYTHON"
_SESSIONS_PATH = "/api/2.0/sql/sessions"
_STATEMENTS_PATH = "/api/2.0/sql/statements"
_LOCAL_TOKEN = "brickhouse-b0211-synthetic-token"


@pytest.mark.skipif(
    not os.environ.get(_CONNECTOR_PYTHON_ENV),
    reason=(
        "set BRICKHOUSE_SQL_CONNECTOR_PYTHON to the isolated Python environment with "
        "databricks-sql-connector 4.6.0 and databricks-sql-kernel 1.1.0"
    ),
)
def test_pinned_connector_sessions_and_inline_results_over_verified_tls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Keep the venv symlink path: resolving it bypasses that environment's site-packages.
    connector_python = Path(os.environ[_CONNECTOR_PYTHON_ENV]).expanduser()
    assert connector_python.is_file()
    ca_file, certificate_file, key_file = _create_local_tls_files(tmp_path)

    descriptor = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(descriptor.path) as context:
        from brickhouse.sql.service import SqlStatementService

        original_execute = SqlStatementService.execute

        def wait_for_connector_cancel(
            service: SqlStatementService,
            statement: str,
            *,
            operation_id: str | None = None,
            parameters=(),
            cancellation=None,
            row_limit: int | None = None,
            byte_limit: int | None = None,
        ):
            if statement != "SELECT 901 AS cancel_probe":
                return original_execute(
                    service,
                    statement,
                    operation_id=operation_id,
                    parameters=parameters,
                    cancellation=cancellation,
                    row_limit=row_limit,
                    byte_limit=byte_limit,
                )
            deadline = time.monotonic() + 25
            while cancellation is None or not cancellation.cancellation_requested:
                if time.monotonic() >= deadline:
                    raise TimeoutError("connector did not cancel the synchronous SQL request")
                time.sleep(0.005)
            return original_execute(
                service,
                statement,
                operation_id=operation_id,
                parameters=parameters,
                cancellation=cancellation,
                row_limit=row_limit,
                byte_limit=byte_limit,
            )

        # Keep a real connector request running past its advertised 10s synchronous wait.
        # The test barrier lets its documented cross-thread Cursor.cancel() reach the local
        # statement service before the query engine starts.
        monkeypatch.setattr(SqlStatementService, "execute", wait_for_connector_cancel)
        warehouse = context.compute.warehouses.create(
            {
                "name": "b0211-sql-kernel-transport",
                "warehouse_type": "PRO",
                "enable_serverless_compute": True,
                "cluster_size": "2X-Small",
                "auto_stop_mins": 0,
            }
        )
        server = create_diagnostic_server(context, "127.0.0.1", 0)
        requests: list[dict[str, Any]] = []
        session_ids: set[str] = set()
        statement_ids: set[str] = set()
        base_handler = server.RequestHandlerClass

        def observe_request(self: Any, method: str) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(length) if length else b""
            try:
                payload = json.loads(raw_body) if raw_body else None
            except (json.JSONDecodeError, UnicodeDecodeError):
                payload = None
            authorization = self.headers.get("Authorization", "")
            parsed_target = urlsplit(self.path)
            path = parsed_target.path
            query = parse_qs(parsed_target.query, keep_blank_values=True)
            session_id = (
                path.rsplit("/", 1)[-1]
                if method == "DELETE" and path.startswith(f"{_SESSIONS_PATH}/")
                else None
            )
            observed_path = f"{_SESSIONS_PATH}/<session-id>" if session_id is not None else path
            statement_id = None
            if path.startswith(f"{_STATEMENTS_PATH}/"):
                statement_segments = path[len(_STATEMENTS_PATH) :].strip("/").split("/")
                if statement_segments and statement_segments[0]:
                    statement_id = statement_segments[0]
                    observed_path = f"{_STATEMENTS_PATH}/<statement-id>"
                    if len(statement_segments) > 1:
                        observed_path += "/" + "/".join(statement_segments[1:])
            request_record = {
                "method": method,
                "path": observed_path,
                "authorization_present": bool(authorization),
                "authorization_scheme": authorization.split(" ", 1)[0] or None,
                "authorization_matches_synthetic": authorization == f"Bearer {_LOCAL_TOKEN}",
                "body_keys": sorted(payload) if isinstance(payload, dict) else [],
                "warehouse_id": (
                    payload.get("warehouse_id") if isinstance(payload, dict) else None
                ),
                "execution_mode": (
                    payload.get("execution_mode") if isinstance(payload, dict) else None
                ),
                "query_keys": sorted(query),
                "query_warehouse_id_matches": (
                    query.get("warehouse_id") == [warehouse.warehouse_id]
                    if "warehouse_id" in query
                    else None
                ),
                "session_id_matches_path": (
                    payload.get("session_id") == session_id
                    if isinstance(payload, dict) and session_id is not None
                    else None
                ),
                "session_id_matches_created": (
                    payload.get("session_id") in session_ids
                    if isinstance(payload, dict) and "session_id" in payload
                    else None
                ),
                "statement_id_matches_created": (
                    statement_id in statement_ids if statement_id is not None else None
                ),
                "statement": (
                    payload.get("statement")
                    if isinstance(payload, dict) and path == _STATEMENTS_PATH
                    else None
                ),
                "parameters": (
                    payload.get("parameters")
                    if isinstance(payload, dict) and path == _STATEMENTS_PATH
                    else None
                ),
                "disposition": (
                    payload.get("disposition")
                    if isinstance(payload, dict) and path == _STATEMENTS_PATH
                    else None
                ),
                "format": (
                    payload.get("format")
                    if isinstance(payload, dict) and path == _STATEMENTS_PATH
                    else None
                ),
                "wait_timeout": (
                    payload.get("wait_timeout")
                    if isinstance(payload, dict) and path == _STATEMENTS_PATH
                    else None
                ),
                "on_wait_timeout": (
                    payload.get("on_wait_timeout")
                    if isinstance(payload, dict) and path == _STATEMENTS_PATH
                    else None
                ),
                "result_compression": (
                    payload.get("result_compression")
                    if isinstance(payload, dict) and path == _STATEMENTS_PATH
                    else None
                ),
                "row_limit": (
                    payload.get("row_limit")
                    if isinstance(payload, dict) and path == _STATEMENTS_PATH
                    else None
                ),
            }
            requests.append(request_record)
            self._brickhouse_observed_request = request_record
            self.rfile = io.BytesIO(raw_body)
            getattr(base_handler, f"do_{method}")(self)

        def observe_post(self: Any) -> None:
            observe_request(self, "POST")

        def observe_delete(self: Any) -> None:
            observe_request(self, "DELETE")

        def observe_get(self: Any) -> None:
            observe_request(self, "GET")

        def observe_response(self: Any, response: Any) -> None:
            request = getattr(self, "_brickhouse_observed_request", None)
            if request is not None:
                request["response_status"] = int(response.status)
                if int(response.status) >= 400 and isinstance(response.payload, dict):
                    request["response_error_code"] = response.payload.get("error_code")
                    request["response_message"] = response.payload.get("message")
                if isinstance(response.payload, dict):
                    request["response_has_manifest"] = isinstance(
                        response.payload.get("manifest"), dict
                    )
                    request["response_has_result"] = isinstance(
                        response.payload.get("result"), dict
                    )
                    status = response.payload.get("status")
                    if isinstance(status, dict):
                        request["response_state"] = status.get("state")
                        request["response_sql_state"] = status.get("sql_state")
                        error = status.get("error")
                        if isinstance(error, dict):
                            request["response_statement_error_code"] = error.get("error_code")
                if (
                    request["path"] == _SESSIONS_PATH
                    and isinstance(response.payload, dict)
                    and isinstance(response.payload.get("session_id"), str)
                ):
                    session_ids.add(response.payload["session_id"])
                if (
                    request["path"] == _STATEMENTS_PATH
                    and isinstance(response.payload, dict)
                    and isinstance(response.payload.get("statement_id"), str)
                ):
                    statement_ids.add(response.payload["statement_id"])
            base_handler._send_payload(self, response)

        server.RequestHandlerClass = type(
            "ObservedDiagnosticHandler",
            (base_handler,),
            {
                "do_POST": observe_post,
                "do_DELETE": observe_delete,
                "do_GET": observe_get,
                "_send_payload": observe_response,
            },
        )
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(certfile=certificate_file, keyfile=key_file)
        server.socket = tls.wrap_socket(server.socket, server_side=True)
        thread = Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )
        thread.start()
        try:
            https_host = "127.0.0.1"
            child_env = {
                "HOME": str(tmp_path),
                "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                "PYTHONNOUSERSITE": "1",
                "TMPDIR": str(tmp_path),
                "BRICKHOUSE_SQL_LOCAL_TOKEN": _LOCAL_TOKEN,
                "BRICKHOUSE_SQL_LOCAL_HOST": https_host,
                "BRICKHOUSE_SQL_LOCAL_PORT": str(server.server_port),
                "BRICKHOUSE_SQL_LOCAL_PATH": (f"/sql/1.0/warehouses/{warehouse.warehouse_id}"),
                "BRICKHOUSE_SQL_LOCAL_CA": str(ca_file),
            }
            try:
                child = subprocess.run(
                    [str(connector_python), "-c", _connector_probe_script()],
                    env=child_env,
                    capture_output=True,
                    text=True,
                    timeout=32,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                pytest.fail(
                    "pinned connector probe timed out; observed requests: "
                    + json.dumps(requests, sort_keys=True)
                )
            assert _LOCAL_TOKEN not in child.stdout + child.stderr
            assert child.returncode == 0, child.stderr
            outcome = json.loads(child.stdout)
            assert outcome == {
                "connector": "4.6.0",
                "kernel": "1.1.0",
                "pyarrow": "23.0.1",
                "kernel_session_opened": True,
                "kernel_session_closed": True,
                "query_session_opened": True,
                "query_session_closed": True,
                "statement_executed": True,
                "description": [{"name": "answer", "type": "int"}],
                "returned_rows": [[42]],
                "sql_failure": "ServerOperationError",
                "metadata_failure": "ServerOperationError",
                "rollback_failure": "NotSupportedError",
                "connection_after_errors": [[7]],
                "cancel_called": True,
                "cancel_execute_error": "ProgrammingError",
                "cancel_execute_error_text": "Invalid result format",
            }, json.dumps({"outcome": outcome, "requests": requests}, sort_keys=True)
            assert [
                (request["method"], request["path"], request["body_keys"]) for request in requests
            ][:4] == [
                ("POST", _SESSIONS_PATH, ["execution_mode", "warehouse_id"]),
                ("DELETE", f"{_SESSIONS_PATH}/<session-id>", []),
                ("POST", _SESSIONS_PATH, ["warehouse_id"]),
                (
                    "POST",
                    _STATEMENTS_PATH,
                    [
                        "disposition",
                        "format",
                        "on_wait_timeout",
                        "parameters",
                        "session_id",
                        "statement",
                        "wait_timeout",
                        "warehouse_id",
                    ],
                ),
            ]
            assert all(request["authorization_matches_synthetic"] for request in requests)
            assert requests[0]["execution_mode"] == "FAST"
            assert requests[1]["query_warehouse_id_matches"] is True
            assert requests[2]["response_status"] == 200
            statement_request = requests[3]
            assert statement_request["session_id_matches_created"] is True
            assert statement_request["statement"] == "SELECT :value AS answer"
            assert statement_request["parameters"] == [
                {"name": "value", "type": "INT", "value": "42"}
            ]
            assert statement_request["disposition"] == "INLINE"
            assert statement_request["format"] == "JSON_ARRAY"
            assert statement_request["wait_timeout"] == "10s"
            assert statement_request["on_wait_timeout"] == "CONTINUE"
            assert requests[4]["statement_id_matches_created"] is True
            assert requests[4]["response_status"] == 200
            assert requests[4]["method"] == "DELETE"
            failure_request = next(
                request
                for request in requests
                if request["path"] == _STATEMENTS_PATH
                and request["statement"] == "SELECT * FROM b021_missing_table"
            )
            assert failure_request["response_status"] == 200
            assert failure_request["response_state"] == "FAILED"
            assert failure_request["response_statement_error_code"] == "BAD_REQUEST"
            assert failure_request["response_sql_state"] == "42000"
            assert failure_request["response_has_result"] is False
            assert failure_request["response_has_manifest"] is False
            metadata_request = next(
                request
                for request in requests
                if request["path"] == _STATEMENTS_PATH and request["statement"] == "SHOW CATALOGS"
            )
            assert metadata_request["response_state"] == "FAILED"
            assert metadata_request["response_has_result"] is False
            assert metadata_request["response_has_manifest"] is False
            recovered_request = next(
                request
                for request in requests
                if request["path"] == _STATEMENTS_PATH
                and request["statement"] == "SELECT 7 AS answer"
            )
            assert recovered_request["response_state"] == "SUCCEEDED"
            assert any(
                request["path"].endswith("/<statement-id>/cancel")
                and request["method"] == "POST"
                and request["response_status"] == 200
                for request in requests
            ), json.dumps(requests, sort_keys=True)
            assert any(
                request["path"] == f"{_STATEMENTS_PATH}/<statement-id>"
                and request["method"] == "GET"
                and request.get("response_state") == "CANCELED"
                and request.get("response_has_result") is False
                and request.get("response_has_manifest") is False
                for request in requests
            ), json.dumps(requests, sort_keys=True)
            assert any(
                request["path"] == f"{_STATEMENTS_PATH}/<statement-id>"
                and request["method"] == "DELETE"
                and request["response_status"] == 200
                for request in requests
            )
            assert any(
                request["path"] == f"{_SESSIONS_PATH}/<session-id>"
                and request["method"] == "DELETE"
                and request["session_id_matches_path"] is True
                for request in requests
            )
            assert all(request.get("response_status") == 200 for request in requests), json.dumps(
                requests, sort_keys=True
            )
        finally:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()


def _connector_probe_script() -> str:
    return """
import json
from datetime import UTC, datetime, timedelta
from ipaddress import ip_address
import os
import time
from threading import Thread
from importlib.metadata import version
from databricks.sql.exc import NotSupportedError

assert "DATABRICKS_TOKEN" not in os.environ
assert "DATABRICKS_HOST" not in os.environ
assert "BRICKHOUSE_SQL_LOCAL_TOKEN" in os.environ

import databricks.sql as sql

result = {
    "connector": version("databricks-sql-connector"),
    "kernel": version("databricks-sql-kernel"),
    "pyarrow": version("pyarrow"),
    "kernel_session_opened": False,
    "kernel_session_closed": False,
    "query_session_opened": False,
    "query_session_closed": False,
    "statement_executed": False,
    "description": [],
    "returned_rows": [],
    "sql_failure": None,
    "metadata_failure": None,
    "rollback_failure": None,
    "connection_after_errors": [],
    "cancel_called": False,
    "cancel_execute_error": None,
    "cancel_execute_error_text": None,
}
kernel_connection = None
connection = None
try:
    kernel_connection = sql.connect(
        server_hostname=(
            os.environ["BRICKHOUSE_SQL_LOCAL_HOST"]
            + ":"
            + os.environ["BRICKHOUSE_SQL_LOCAL_PORT"]
        ),
        http_path=os.environ["BRICKHOUSE_SQL_LOCAL_PATH"],
        access_token=os.environ["BRICKHOUSE_SQL_LOCAL_TOKEN"],
        use_kernel=True,
        enable_telemetry=False,
        _socket_timeout=8,
        _retry_stop_after_attempts_count=1,
        _tls_trusted_ca_file=os.environ["BRICKHOUSE_SQL_LOCAL_CA"],
    )
    result["kernel_session_opened"] = True
    kernel_connection.close()
    result["kernel_session_closed"] = True

    connection = sql.connect(
        server_hostname=os.environ["BRICKHOUSE_SQL_LOCAL_HOST"],
        http_path=os.environ["BRICKHOUSE_SQL_LOCAL_PATH"],
        access_token=os.environ["BRICKHOUSE_SQL_LOCAL_TOKEN"],
        use_sea=True,
        use_cloud_fetch=False,
        enable_query_result_lz4_compression=False,
        _port=int(os.environ["BRICKHOUSE_SQL_LOCAL_PORT"]),
        enable_telemetry=False,
        _socket_timeout=25,
        _retry_stop_after_attempts_count=1,
        _tls_trusted_ca_file=os.environ["BRICKHOUSE_SQL_LOCAL_CA"],
    )
    result["query_session_opened"] = True
    cursor = connection.cursor()
    cursor.execute("SELECT :value AS answer", {"value": 42})
    result["statement_executed"] = True
    result["description"] = [
        {"name": column[0], "type": str(column[1])} for column in cursor.description
    ]
    result["returned_rows"] = [list(row) for row in cursor.fetchall()]

    try:
        cursor.execute("SELECT * FROM b021_missing_table")
    except Exception as error:
        result["sql_failure"] = type(error).__name__

    try:
        cursor.catalogs()
    except Exception as error:
        result["metadata_failure"] = type(error).__name__

    try:
        connection.rollback()
    except NotSupportedError as error:
        result["rollback_failure"] = type(error).__name__

    cursor.execute("SELECT 7 AS answer")
    result["connection_after_errors"] = [list(row) for row in cursor.fetchall()]

    prior_command_id = cursor.active_command_id
    cancel_attempt = {"called": False}

    def cancel_running_command():
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            command_id = cursor.active_command_id
            if command_id is not None and command_id != prior_command_id:
                cursor.cancel()
                cancel_attempt["called"] = True
                return
            time.sleep(0.01)

    cancellation = Thread(target=cancel_running_command, daemon=True)
    cancellation.start()
    try:
        cursor.execute("SELECT 901 AS cancel_probe")
    except Exception as error:
        result["cancel_execute_error"] = type(error).__name__
        result["cancel_execute_error_text"] = str(error)[:250]
    else:
        result["cancel_execute_error"] = "returned"
    cancellation.join(timeout=3)
    result["cancel_called"] = cancel_attempt["called"]
    connection.close()
    result["query_session_closed"] = True
except Exception as error:
    result["connection_error"] = type(error).__name__
    result["error_text"] = (
        str(error)
        .replace(os.environ["BRICKHOUSE_SQL_LOCAL_TOKEN"], "[redacted]")[:1000]
    )
finally:
    if kernel_connection is not None:
        try:
            kernel_connection.close()
        except Exception:
            pass
    if connection is not None:
        try:
            connection.close()
        except Exception:
            pass
print(json.dumps(result, sort_keys=True))
"""


def _create_local_tls_files(directory: Path) -> tuple[Path, Path, Path]:
    # Use the pinned cryptography dependency rather than the runner's OpenSSL
    # command/configuration; explicitly encode every CA/server extension.
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    now = datetime.now(UTC)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "BrickHouse local SQL test CA")])
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=None,
                decipher_only=None,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False
        )
        .sign(ca_key, hashes.SHA256())
    )
    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")]))
        .issuer_name(ca_name)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=None,
                decipher_only=None,
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ip_address("127.0.0.1"))]), critical=False
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False
        )
        .sign(ca_key, hashes.SHA256())
    )
    ca_file, certificate_file, key_file = (
        directory / name for name in ("ca.pem", "server.pem", "server.key")
    )
    ca_file.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    certificate_file.write_bytes(server.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(
        server_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_file.chmod(0o600)
    return ca_file, certificate_file, key_file
