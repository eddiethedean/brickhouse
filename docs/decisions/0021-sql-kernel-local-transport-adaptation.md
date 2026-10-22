# ADR 0021 — Contained SQL kernel loopback transport probe

- Status: accepted for the B045 research harness only
- Date: 2026-10-20
- Tasks: B045; informs B011 and B021

## Context

The phase 0.1 SQL client profile is `databricks-sql-connector==4.6.0` with the explicit kernel backend `databricks-sql-kernel==1.1.0`. B021 must distinguish the connector's Statement Execution API path from default Thrift, and the local client must verify TLS and isolate credentials. Neither a query result nor a local session contract had been proven.

The pinned connector exposes only private underscore-prefixed arguments for a custom trusted CA. In the kernel backend the connector's configured port is accepted by the wrapper but ignored; the kernel receives the `host` value. The documented connector examples describe `server_hostname` as a hostname, without documenting a custom port in that string.

## Decision

Keep the non-public adaptations inside `scripts/probe_sql_connector_transport.py`. Pin and verify the exact connector/kernel artifacts; force `use_kernel=True`; bind the capture server to `127.0.0.1`; provide `server_hostname="127.0.0.1:<ephemeral-port>"`; and use the pinned connector's private `_tls_trusted_ca_file` option for an ephemeral local CA. Keep certificate verification and hostname verification enabled. `_port` is omitted because the pinned kernel path does not forward it.

The probe requires a synthetic value in `BRICKHOUSE_LOCAL_SQL_PROBE_TOKEN`, rejects a process that contains `DATABRICKS_TOKEN`, disables telemetry, uses a finite request timeout, captures only sanitized request fields, and responds with HTTP 401. It does not load `.env` or auth profiles and cannot send a request anywhere except the explicit loopback address. Temporary keys/certificates are deleted at exit. No successful SQL result is synthesized.

This is a research-only contained adaptation, not a BrickHouse runtime dependency or public configuration contract. B021 must explicitly pin and test any reuse; it must fail closed if the connector/kernel versions or transport behavior differ. A production-facing local host/port or custom CA setting requires a public BrickHouse configuration decision and must preserve verification.

## Consequences

- The pinned kernel reached `/api/2.0/sql/sessions` using verified TLS and a Bearer header; the `http_path` warehouse identifier was sent as `warehouse_id` with `execution_mode="FAST"`.
- The local capture returned HTTP 401 before a SQL statement request. This proves a bounded session-opening transport request, not session compatibility, SQL execution, result schemas, errors, cancellation, or full SQL connector support.
- B011/B021 must implement and verify session-open/close and subsequent statement routes with accurate errors. B021 must select `use_kernel=True` explicitly, pass the local warehouse path and auth explicitly, and provide a contained way to trust the local CA without disabling verification.
- The default Thrift backend remains a separate profile. It is not exercised by this decision.

See the [B045 conformance report](../../artifacts/conformance/B045-sql-connector-loopback.md) for wheel hashes, source review, the captured request/error and exact command.

B021.1 subsequently exercised the pinned kernel profile against BrickHouse's loopback API using the contained TLS/host adaptation. The first request resolved the `warehouse_id` and initially returned HTTP 501 before sessions were implemented. B021.2.1 now verifies the kernel's explicit FAST session create and bodyless close, plus the Python SEA client's warehouse-only create and body close. B021.2.2 verifies a named INT query and inline JSON result through the connector's SEA backend, including terminal statement close. B021.2.3 verifies failed SQL and metadata calls, bounded expiry/eviction, statement/session close, and public synchronous `Cursor.cancel()` over the same loopback TLS boundary. The SEA client exposes terminal cancellation as `ProgrammingError: Invalid result format`; `SHOW CATALOGS` and rollback are explicitly unsupported. The kernel backend's default Arrow-stream request remains unsupported, and Thrift/remote behavior remain unverified. See the [B021.1 report](../../artifacts/conformance/B021.1-sql-kernel-brickhouse-loopback.md), [B021.2.1 report](../../artifacts/conformance/B021.2.1-sql-kernel-session-lifecycle.md), [B021.2.2 report](../../artifacts/conformance/B021.2.2-sql-connector-inline-results.md), and [B021.2.3 report](../../artifacts/conformance/B021.2.3-sql-connector-failures-retention.md).
