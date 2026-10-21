"""Run one bounded, read-only Databricks SQL reference probe.

The host is supplied explicitly. The token is read from ignored local `.env` and
is never printed. The script selects the only 2X-Small serverless warehouse,
requires it to be stopped before the probe, submits one literal-only statement,
and stops the warehouse again if the statement started it.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

_USER_AGENT = "brickhouse-reference-probe/0.1"
_STATEMENT = (
    "SELECT current_timezone() AS session_timezone, "
    "CAST(12.34 AS DECIMAL(12, 2)) AS amount, "
    "CAST('2026-10-16' AS DATE) AS event_date, "
    "CAST('2026-10-16 12:34:56' AS TIMESTAMP) AS event_ts, "
    "CAST(NULL AS STRING) AS optional_text, ARRAY(1, 2) AS item_ids"
)
_IDENTIFIER_STATEMENT = (
    "SELECT current_catalog() AS catalog_context, "
    "current_schema() AS schema_context, "
    "1 AS `simple`, 2 AS `part.with.dot`, 3 AS `mixed``tick`"
)
_POLL_INTERVAL_SECONDS = 2
_MAX_WAIT_SECONDS = 75


class ProbeError(RuntimeError):
    """An error safe to report without including credentials or raw responses."""

    def __init__(self, stage: str, status: int | None = None) -> None:
        super().__init__(stage)
        self.stage = stage
        self.status = status


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        return None


def _load_token(env_file: Path) -> str:
    inherited_token = os.environ.get("DATABRICKS_TOKEN")
    if inherited_token:
        return inherited_token
    if not env_file.is_file():
        raise ProbeError("token_missing")
    for line in env_file.read_text(encoding="utf-8", errors="replace").splitlines():
        item = line.strip()
        if not item or item.startswith("#"):
            continue
        if item.startswith("export "):
            item = item[7:].lstrip()
        key, sep, value = item.partition("=")
        if sep and key.strip() == "DATABRICKS_TOKEN":
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            if value:
                return value
    raise ProbeError("token_missing")


def _normalize_host(host: str) -> str:
    parts = urlsplit(host)
    if (
        parts.scheme != "https"
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
        or parts.query
        or parts.fragment
        or parts.path not in {"", "/"}
    ):
        raise ProbeError("invalid_host")
    return urlunsplit(("https", parts.netloc, "", "", ""))


class _DatabricksApi:
    def __init__(self, host: str, token: str) -> None:
        self._host = host
        self._token = token
        self._opener = build_opener(_NoRedirect())

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._token}",
            "User-Agent": _USER_AGENT,
        }
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = Request(self._host + path, data=data, headers=headers, method=method)
        try:
            with self._opener.open(request, timeout=10) as response:
                payload = json.loads(response.read())
        except HTTPError as error:
            raise ProbeError("http_error", error.code) from None
        except (URLError, TimeoutError, OSError):
            raise ProbeError("transport_error") from None
        except json.JSONDecodeError:
            raise ProbeError("invalid_json") from None
        if not isinstance(payload, dict):
            raise ProbeError("invalid_response")
        return payload


def _select_stopped_free_warehouse(api: _DatabricksApi) -> str:
    payload = api.request("GET", "/api/2.0/sql/warehouses")
    warehouses = payload.get("warehouses")
    if not isinstance(warehouses, list) or len(warehouses) != 1:
        raise ProbeError("expected_one_warehouse")
    warehouse = warehouses[0]
    if not isinstance(warehouse, dict):
        raise ProbeError("invalid_warehouse_record")
    if (
        warehouse.get("state") != "STOPPED"
        or warehouse.get("cluster_size") != "2X-Small"
        or warehouse.get("enable_serverless_compute") is not True
    ):
        raise ProbeError("warehouse_not_in_expected_initial_state")
    warehouse_id = warehouse.get("id")
    if not isinstance(warehouse_id, str) or not warehouse_id:
        raise ProbeError("warehouse_id_missing")
    return warehouse_id


def _warehouse_state(api: _DatabricksApi, warehouse_id: str) -> str:
    payload = api.request("GET", f"/api/2.0/sql/warehouses/{quote(warehouse_id, safe='')}")
    state = payload.get("state")
    if not isinstance(state, str):
        raise ProbeError("warehouse_state_missing")
    return state


def _restore_stopped_state(api: _DatabricksApi, warehouse_id: str) -> bool:
    state = _warehouse_state(api, warehouse_id)
    if state == "STOPPED":
        return True
    if state not in {"RUNNING", "STARTING", "STOPPING"}:
        return False
    if state != "STOPPING":
        api.request("POST", f"/api/2.0/sql/warehouses/{quote(warehouse_id, safe='')}/stop", {})
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if _warehouse_state(api, warehouse_id) == "STOPPED":
            return True
        time.sleep(1)
    return False


def _completed_payload(api: _DatabricksApi, statement_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + _MAX_WAIT_SECONDS
    latest: dict[str, Any] = {}
    while time.monotonic() < deadline:
        latest = api.request("GET", f"/api/2.0/sql/statements/{quote(statement_id, safe='')}")
        status = latest.get("status")
        state = status.get("state") if isinstance(status, dict) else None
        if state in {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}:
            return latest
        time.sleep(_POLL_INTERVAL_SECONDS)
    raise ProbeError("statement_timeout")


def _result_summary(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[Any]]:
    manifest = payload.get("manifest")
    result = payload.get("result")
    if not isinstance(manifest, dict) or not isinstance(result, dict):
        raise ProbeError("result_missing")
    schema = manifest.get("schema")
    columns = schema.get("columns") if isinstance(schema, dict) else None
    rows = result.get("data_array")
    if not isinstance(columns, list) or not isinstance(rows, list):
        raise ProbeError("result_shape_unexpected")
    safe_columns: list[dict[str, Any]] = []
    for column in columns:
        if not isinstance(column, dict):
            raise ProbeError("column_shape_unexpected")
        safe_column = {
            name: column[name]
            for name in ("name", "type_name", "type_text", "type_precision", "type_scale")
            if name in column
        }
        safe_columns.append(safe_column)
    return safe_columns, rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True, help="Explicit HTTPS Databricks workspace URL")
    parser.add_argument(
        "--case",
        choices=("sql-literals", "identifiers"),
        default="sql-literals",
        help="Select one reviewed, read-only statement (default: sql-literals)",
    )
    parser.add_argument("--env-file", type=Path, default=Path(".env"), help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        host = _normalize_host(args.host)
        token = _load_token(args.env_file)
        api = _DatabricksApi(host, token)
        warehouse_id = _select_stopped_free_warehouse(api)
    except ProbeError as error:
        print(
            json.dumps({"probe": "not_started", "stage": error.stage, "http_status": error.status})
        )
        return 1

    statement_id: str | None = None
    summary: dict[str, Any]
    cleanup_ok = False
    try:
        response = api.request(
            "POST",
            "/api/2.0/sql/statements",
            {
                "warehouse_id": warehouse_id,
                "statement": (_IDENTIFIER_STATEMENT if args.case == "identifiers" else _STATEMENT),
                "wait_timeout": "10s",
                "on_wait_timeout": "CONTINUE",
                "format": "JSON_ARRAY",
                "disposition": "INLINE",
            },
        )
        raw_id = response.get("statement_id")
        if not isinstance(raw_id, str) or not raw_id:
            raise ProbeError("statement_id_missing")
        statement_id = raw_id
        response = _completed_payload(api, statement_id)
        status = response.get("status")
        state = status.get("state") if isinstance(status, dict) else "UNKNOWN"
        if state != "SUCCEEDED":
            raise ProbeError(f"statement_{str(state).lower()}")
        schema, rows = _result_summary(response)
        if args.case == "identifiers":
            if len(rows) != 1 or len(rows[0]) < 5:
                raise ProbeError("identifier_result_shape_unexpected")
            rows = [
                [
                    "present" if rows[0][0] is not None else "missing",
                    "present" if rows[0][1] is not None else "missing",
                    *rows[0][2:],
                ]
            ]
        summary = {"probe": "completed", "schema": schema, "rows": rows}
    except ProbeError as error:
        if statement_id is not None:
            try:
                api.request(
                    "POST",
                    f"/api/2.0/sql/statements/{quote(statement_id, safe='')}/cancel",
                    {},
                )
            except ProbeError:
                pass
        summary = {"probe": "failed", "stage": error.stage, "http_status": error.status}
    finally:
        try:
            cleanup_ok = _restore_stopped_state(api, warehouse_id)
        except ProbeError:
            cleanup_ok = False

    summary["warehouse_restored_to_stopped"] = cleanup_ok
    if args.case != "sql-literals":
        summary["case"] = args.case
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary["probe"] == "completed" and cleanup_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
