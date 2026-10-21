# ADR 0013: Local Delta table service

- Status: accepted for phase 0.1 B008.2
- Date: 2026-10-19
- Tasks: B008.2
- Extends: [ADR 0012](0012-table-snapshot-and-staging-contracts.md)

## Decision

Implement `DeltaRsTableService` using the public Python delta-rs API. Keep the base `brickhouse` distribution free of runtime dependencies and add the exact tested provider versions through the `delta` extra: `deltalake==1.2.1` and `pyarrow==23.0.1`. Import these optional packages only when constructing the provider service.

The service operates only on registered canonical local paths. It verifies table ID, namespace identity, ownership, path stability, workspace staging ownership, staged byte count and SHA-256, Arrow schema, and row count. It supports latest/version/timestamp reads and unpartitioned create/append/overwrite with exact schemas and Delta reader/writer protocol 1/2 without table features. Unsupported protocols/features and partitioned tables fail before scanning data. Provider writes use Delta transactions; BrickHouse does not edit `_delta_log` or perform read/overwrite mutation cycles.

Each write has a printable operation ID stored in Delta commit custom metadata. The service resolves the precise commit version from provider history and opens that exact snapshot for its result. Provider commit retries are disabled. If the write call or history lookup cannot establish the result, the service raises `UncertainCommitError` and directs the caller to inspect history before retrying. In-process mutations acquire locks keyed by both table ID and canonical physical location, so multiple service objects and path aliases serialize within one process.

## Consequences

- The table service is available directly to Python callers but is not yet composed into SQL, Sparkless, the HTTP server, or jobs.
- The table protocol/type profile and tested package versions are narrow. Partitioning, schema evolution, cloud paths, table features, update/delete/merge, and managed deletion are not supported by this service.
- Delta history metadata is not a durable BrickHouse operation journal. Reusing an operation ID can still write duplicate data; B008.3 must add persistent fingerprints, reconciliation, and safe restart behavior before retry safety is claimed.
- The process-local lock does not coordinate external processes. Independent writers, crash recovery, history retention, and cross-process concurrency remain unverified.
- Ordinary tests and runtime provider operations do not load `.env` or contact the personal Databricks account. The account remains reserved for separate bounded reference probes and local test design.

## Evidence

The local Delta service tests and exact dependency versions are recorded in [B008.2 conformance](../../artifacts/conformance/B008.2-local-delta-table-service.md).
