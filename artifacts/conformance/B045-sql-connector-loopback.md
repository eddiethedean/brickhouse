# B045 — SQL connector/kernel loopback transport feasibility

Date: **2026-10-20**. Task: **done**. Decision: [ADR 0021](../../docs/decisions/0021-sql-kernel-local-transport-adaptation.md).

## Pinned artifacts

The B005.1 profile was installed in a disposable Python 3.11.14 virtual environment. The installed connector and kernel distributions were independently downloaded from PyPI and SHA-256 checked:

| Artifact | Exact version | Inspected wheel | SHA-256 |
|---|---:|---|---|
| `databricks-sql-connector` | `4.6.0` | `databricks_sql_connector-4.6.0-py3-none-any.whl` | `9f2128592052d3aadb7a3c46df0a8ef414402ec6ea7a0fdb8490da737fe43cdc` |
| `databricks-sql-kernel` | `1.1.0` | `databricks_sql_kernel-1.1.0-cp39-abi3-macosx_11_0_arm64.whl` | `8d7bf7aa002dc2f4864170f1e9185ba10fdf5460efb114269ec9337b084bd501` |
| `pyarrow` dependency | `23.0.1` | macOS arm64 CPython 3.11 wheel | Required by the kernel's installed metadata: `pyarrow>=23.0.1,<24` |

`pip check` reported no broken requirements. The connector source in the installed 4.6.0 wheel selects the Rust kernel only when `use_kernel=True`; its kernel client maps `_tls_trusted_ca_file` to the kernel's in-memory CA option. The pinned wrapper does not forward its port setting to the kernel. The official connector documentation describes the kernel as the Statement Execution API path and makes `use_kernel=True` explicit; the default remains Thrift.

## Probe and captured request

The reproducible tool is [scripts/probe_sql_connector_transport.py](../../scripts/probe_sql_connector_transport.py). It creates an ephemeral CA and server certificate for IP SAN `127.0.0.1`, binds only to loopback on a dynamic port, verifies both certificate chain and hostname, disables telemetry, and sets a bounded socket timeout. It rejects `DATABRICKS_TOKEN` in its process and uses only the synthetic `BRICKHOUSE_LOCAL_SQL_PROBE_TOKEN`; it never loads `.env` or workspace profiles. The request's authorization value is compared in memory and is not written to the report.

Sanitized captured request:

```json
{
  "method": "POST",
  "path": "/api/2.0/sql/sessions",
  "host_header": "127.0.0.1:<ephemeral-port>",
  "authorization_present": true,
  "authorization_scheme": "Bearer",
  "authorization_matches_synthetic_probe_value": true,
  "content_type": "application/json",
  "body_keys": ["execution_mode", "warehouse_id"],
  "execution_mode": "FAST",
  "warehouse_id": "brickhouse-probe-warehouse"
}
```

The local boundary returned HTTP 401 `UNAUTHENTICATED`. The pinned connector surfaced `OperationalError: HTTP 401: local capture probe intentionally rejects credentials`. It made no SQL statement request because the session-opening request was rejected. No successful SQL result was returned or fabricated.

**Finding:** kernel session-opening transport is feasible against an explicit loopback HTTPS host with strict local-CA trust, hostname verification, synthetic Bearer auth, and the pinned warehouse path. This does not prove the session response contract or any statement/result behavior. B021 remains blocked from a SQL compatibility claim until session creation, statement submission, errors, cancellation, result schema/batches and lifetime are implemented and verified locally.

## API restrictions and next changes

- The kernel sends `POST /api/2.0/sql/sessions`, with the warehouse path reduced to `warehouse_id` and a separate `execution_mode` field. B011/B021 need an explicit session contract and must not assume a direct Thrift session path.
- The wrapper's `_port` setting is ignored by the kernel backend. This probe uses the non-standard `server_hostname="127.0.0.1:<port>"` form to reach its ephemeral port. The behavior worked in the pinned artifacts but is not a documented public hostname contract. B021 should make loopback host/port selection explicit in its contained adapter or use another supported local binding; it must not infer this behavior for other versions.
- The custom CA uses the private `_tls_trusted_ca_file` option. Keep this dependency confined to the pinned probe adapter until B021 records and tests an explicit containment decision. Never turn off certificate or hostname verification to make local tests pass.
- The authorization header is synthesized locally, appears only in memory at the capture boundary, and is absent from the sanitized report and retained logs. No actual Databricks credential, account, warehouse, `.env`, or network endpoint was used.
- Thrift (`use_kernel=False`) was not exercised and remains a separate transport profile.

## Evidence

Artifact installation and inspection:

```bash
python3 -m venv /tmp/brickhouse-b045-probe
/tmp/brickhouse-b045-probe/bin/python -m pip install \
  'databricks-sql-connector==4.6.0' 'databricks-sql-kernel==1.1.0'
/tmp/brickhouse-b045-probe/bin/python -m pip check
/tmp/brickhouse-b045-probe/bin/python scripts/probe_sql_connector_transport.py --timeout-seconds 4
```

For the actual capture, the process ran with a clean `env -i`; only `PATH`, `PYTHONNOUSERSITE=1`, and a synthetic `BRICKHOUSE_LOCAL_SQL_PROBE_TOKEN` were set. `DATABRICKS_TOKEN` was absent. `pip check` passed; the capture returned the actual local request and 401 described above. No Databricks account request, cloud compute, routine test, or SQL success response was used.
