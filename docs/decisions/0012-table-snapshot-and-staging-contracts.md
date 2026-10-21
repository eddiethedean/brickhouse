# ADR 0012: Table snapshot and staging contracts

- Status: accepted for B008.1; extended by [ADR 0013](0013-local-delta-table-service.md)
- Date: 2026-10-19
- Tasks: B008.1

## Decision

Define provider-neutral `TableHandle`, `TableSnapshot`, `SchemaDescriptor`, `DeltaProtocol`, and `SnapshotSelector` values, plus a `TableServicePort` protocol. A handle carries the registered table/catalog/schema IDs, full name, ownership, format, and canonical local location. A snapshot adds one nonnegative Delta version, canonical nested Delta schema JSON, normalized protocol features, and an aware capture timestamp.

The initial local profile accepts exactly Delta reader protocol 1 and writer protocol 2 with no reader or writer features. The public selected `deltalake==1.2.1` API supplies the local snapshot version, schema, and protocol fields. Protocol/feature validation happens before table data access. Snapshot selection accepts either a numeric version or a timezone-aware timestamp, never both.

The internal initial write modes are create, append, and overwrite. Create requires no existing snapshot. Append/overwrite require an existing snapshot with the accepted protocol and an exact schema fingerprint match. Schema evolution and other write modes fail explicitly.

`StagingArea` copies a caller-labeled local Parquet or Arrow IPC file into a random, workspace-owned artifact directory. The directory is mode 0700 and the staged file is mode 0600. The manifest records workspace and operation IDs, schema, row count, byte count, SHA-256, format, and creation time. Cleanup resolves and checks the artifact path and workspace before unlinking only that file. Staging copies bytes; it does not parse them or confirm that the labeled format/schema matches the file.

## Consequences

- At B008.1 completion, these contracts and staging utility did not yet provide table IO. B008.2 adds that local provider service; see ADR 0013.
- B008.1 pinned `deltalake==1.2.1` and `pyarrow==23.0.1` for local fixtures. B008.2 adopts those same exact versions as the optional runtime `delta` extra.
- Cloud storage, advanced Delta features, partitions, schema evolution, provider concurrency, transaction journaling, and exactly-once behavior remain outside this slice.
- The default protocol profile is intentionally narrow. A new feature/version requires explicit tests and a capability update before acceptance.

## Evidence

The real local Delta, snapshot-selector, validation, and staging tests are recorded in [B008.1 conformance](../../artifacts/conformance/B008.1-table-contracts-and-staging.md).
