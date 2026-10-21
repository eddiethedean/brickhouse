"""Capture one pinned Databricks SQL kernel request against local verified TLS.

This is an explicit research probe. It never reads `.env` or Databricks auth
profiles and is deliberately bound to 127.0.0.1. Provide only a synthetic token
through BRICKHOUSE_LOCAL_SQL_PROBE_TOKEN. The capture boundary returns HTTP 401;
it never fabricates a successful SQL response.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import re
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

MAX_REQUEST_BYTES = 1024 * 1024
WAREHOUSE_PATH = "/sql/1.0/warehouses/brickhouse-probe-warehouse"
TOKEN_ENV = "BRICKHOUSE_LOCAL_SQL_PROBE_TOKEN"


def _package_version(name: str) -> str | None:
    try:
        return version(name)
    except PackageNotFoundError:
        return None


def _capture_certificate_files(directory: Path) -> tuple[Path, Path, Path]:
    openssl = shutil.which("openssl")
    if openssl is None:
        raise RuntimeError("openssl is required to create an ephemeral local TLS CA")

    ca_key = directory / "probe-ca.key"
    ca_cert = directory / "probe-ca.pem"
    server_key = directory / "loopback-server.key"
    server_csr = directory / "loopback-server.csr"
    server_cert = directory / "loopback-server.pem"
    extensions = directory / "server-extensions.cnf"
    extensions.write_text(
        "basicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\n"
        "subjectAltName=IP:127.0.0.1\n",
        encoding="ascii",
    )
    commands = [
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-keyout",
            str(ca_key),
            "-out",
            str(ca_cert),
            "-subj",
            "/CN=BrickHouse ephemeral probe CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ],
        [
            openssl,
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(server_key),
            "-out",
            str(server_csr),
            "-subj",
            "/CN=127.0.0.1",
        ],
        [
            openssl,
            "x509",
            "-req",
            "-in",
            str(server_csr),
            "-CA",
            str(ca_cert),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-out",
            str(server_cert),
            "-days",
            "1",
            "-sha256",
            "-extfile",
            str(extensions),
        ],
    ]
    for command in commands:
        subprocess.run(
            command,
            check=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
        )
    return ca_cert, server_cert, server_key


class _CaptureServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], token: str) -> None:
        self.synthetic_token = token
        self.captured: queue.Queue[dict[str, Any]] = queue.Queue()
        super().__init__(server_address, _CaptureHandler)


class _CaptureHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        self._capture_and_reject()

    def do_POST(self) -> None:
        self._capture_and_reject()

    def do_DELETE(self) -> None:
        self._capture_and_reject()

    def _capture_and_reject(self) -> None:
        length_text = self.headers.get("Content-Length", "0")
        try:
            length = int(length_text)
        except ValueError:
            length = MAX_REQUEST_BYTES + 1
        if length < 0 or length > MAX_REQUEST_BYTES:
            self.close_connection = True
            self.send_error(413)
            return
        body = self.rfile.read(length) if length else b""
        try:
            payload = json.loads(body) if body else None
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = None
        statement = payload.get("statement") if isinstance(payload, dict) else None
        warehouse_id = payload.get("warehouse_id") if isinstance(payload, dict) else None
        execution_mode = payload.get("execution_mode") if isinstance(payload, dict) else None
        auth = self.headers.get("Authorization", "")
        match = re.fullmatch(r"(?i:Bearer)\s+(.+)", auth)
        expected = f"Bearer {self.server.synthetic_token}"  # type: ignore[attr-defined]
        request_record: dict[str, Any] = {
            "method": self.command,
            "path": self.path.split("?", 1)[0],
            "host_header": self.headers.get("Host"),
            "authorization_present": bool(auth),
            "authorization_scheme": "Bearer" if match else (auth.split(" ", 1)[0] or None),
            "authorization_matches_synthetic_probe_value": auth == expected,
            "content_type": self.headers.get("Content-Type"),
            "body_bytes": length,
            "body_keys": sorted(payload) if isinstance(payload, dict) else [],
            "warehouse_id": warehouse_id if isinstance(warehouse_id, str) else None,
            "execution_mode": execution_mode if isinstance(execution_mode, str) else None,
            "statement": statement if isinstance(statement, str) else None,
        }
        self.server.captured.put(request_record)  # type: ignore[attr-defined]

        # A rejection is intentional: the request shape/auth can be inspected,
        # while no successful SQL result or server behavior is fabricated.
        response = json.dumps(
            {
                "error_code": "UNAUTHENTICATED",
                "message": "local capture probe intentionally rejects credentials",
            },
            separators=(",", ":"),
        ).encode("utf-8")
        self.send_response(401)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(response)
        self.close_connection = True

    def log_message(self, format: str, *args: object) -> None:
        # Never print request headers or connector diagnostics from this capture.
        return


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--timeout-seconds", type=float, default=8.0, help="Connector request timeout"
    )
    arguments = parser.parse_args()
    token = os.environ.get(TOKEN_ENV, "")
    if not token or len(token) < 8:
        parser.error(f"set {TOKEN_ENV} to a synthetic local-only value of at least 8 characters")
    if "DATABRICKS_TOKEN" in os.environ:
        parser.error("DATABRICKS_TOKEN must be absent from the probe process")
    if not 0.1 <= arguments.timeout_seconds <= 30:
        parser.error("--timeout-seconds must be from 0.1 through 30")

    # Prevent connector logging from writing request details or bearer material.
    logging.disable(logging.CRITICAL)
    try:
        from databricks import sql
    except ImportError as error:
        parser.error(f"install the pinned SQL connector and kernel first: {error}")

    with tempfile.TemporaryDirectory(prefix="brickhouse-sql-probe-") as temporary:
        temporary_path = Path(temporary)
        ca_file, certificate_file, key_file = _capture_certificate_files(temporary_path)
        server = _CaptureServer(("127.0.0.1", 0), token)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(certfile=certificate_file, keyfile=key_file)
        server.socket = tls.wrap_socket(server.socket, server_side=True)
        port = server.server_address[1]
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()

        error_type: str | None = None
        error_message: str | None = None
        returned_rows = False
        connection = None
        try:
            connection = sql.connect(
                server_hostname=f"127.0.0.1:{port}",
                http_path=WAREHOUSE_PATH,
                access_token=token,
                use_kernel=True,
                enable_telemetry=False,
                _socket_timeout=arguments.timeout_seconds,
                _tls_trusted_ca_file=str(ca_file),
            )
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                returned_rows = bool(cursor.fetchall())
        except Exception as error:
            error_type = type(error).__name__
            error_message = str(error).replace(token, "[synthetic credential redacted]")[:1000]
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=2)

        requests: list[dict[str, Any]] = []
        while not server.captured.empty():
            requests.append(server.captured.get_nowait())
        report = {
            "profile": {
                "python": sys.version.split()[0],
                "connector": _package_version("databricks-sql-connector"),
                "kernel": _package_version("databricks-sql-kernel"),
                "pyarrow": _package_version("pyarrow"),
                "host": f"127.0.0.1:{port}",
                "warehouse_http_path": WAREHOUSE_PATH,
                "backend": "kernel / Statement Execution API",
                "tls_verify": True,
                "tls_verify_hostname": True,
                "local_ca_configured": True,
                "token_source": TOKEN_ENV,
                "databricks_token_present": False,
            },
            "capture": {
                "bound_address": list(server.server_address),
                "requests": requests,
                "connector_error_type": error_type,
                "connector_error": error_message,
                "successful_sql_result_returned": returned_rows,
                "response_policy": "HTTP 401 UNAUTHENTICATED; no SQL result returned",
            },
        }
        print(json.dumps(report, indent=2, sort_keys=True))
        if not requests:
            print(
                "probe_error: connector made no request to the loopback TLS boundary",
                file=sys.stderr,
            )
            return 2
        if not any(request["authorization_matches_synthetic_probe_value"] for request in requests):
            print("probe_error: bearer auth did not reach the local capture", file=sys.stderr)
            return 3
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
