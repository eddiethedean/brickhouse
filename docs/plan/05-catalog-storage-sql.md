# 05 — Catalog, storage, and SQL contracts

## Implemented metadata slice

B007.1 adds SQLite metadata schema version 2 without changing workspace format version 1. It assigns immutable UUIDs to catalog and schema rows and provides instance-scoped catalog/schema readers plus managed/external table-registration repositories. Table locations are local canonical paths, managed paths reserve `tables/<table-id>` under the workspace, and location uniqueness catches symlink aliases. Metadata schema 1 is upgraded under the workspace owner lock. These repositories reserve identities and ownership only; they do not create directories or Delta tables or read/write data. See [ADR 0009](../decisions/0009-catalog-identity-and-location-registration.md) and [B007.1 evidence](../../artifacts/conformance/B007.1-catalog-identities.md).

B007.2 adds `parse_identifier()` and a `NamespaceState` created per future session. The initial name profile resolves one-part tables in current catalog/schema, two-part tables in current catalog plus explicit schema, and three-part tables by explicit catalog/schema. Backticks can delimit parts containing dots; doubled backticks escape a literal backtick. Lookup is case-insensitive, missing and ambiguous matches are explicit errors, and `use_catalog` resets schema to `default`. This is not SQL parsing, a Sparkless session, SQL Path, or temporary-view resolution. See [ADR 0010](../decisions/0010-identifier-resolution-and-session-namespaces.md), [local evidence](../../artifacts/conformance/B007.2-identifier-resolution.md), and the separate [sanitized reference capture](../../artifacts/conformance/B007.2-identifiers-reference.md).

B007.3 upgrades metadata schema 2 to 3, adding persistent comments and `information_schema` for existing workspaces. The local loopback API implements the exact pinned SDK's catalog/schema create, list, get, and delete calls, including paginated lists and typed duplicate, missing, invalid, and in-use errors. Catalog creation also creates `default` and `information_schema`. The default catalog/schema and system schema are protected; registered tables prevent schema/catalog deletion. Only lowercase unquoted names and comments are accepted; unsupported SDK fields fail explicitly. The SDK test proves local transport and SQLite persistence across reopen, not Databricks workspace parity, permissions, SQL, or Delta data access. See [ADR 0011](../decisions/0011-local-sdk-catalog-schema-profile.md) and [B007.3 evidence](../../artifacts/conformance/B007.3-sdk-catalog-schema.md).

B008.1 defines `TableHandle`, `TableSnapshot`, canonical nested `SchemaDescriptor`, `DeltaProtocol`, `SnapshotSelector`, `StagedInput`, and a `TableServicePort`. B008.2 implements `DeltaRsTableService` through the optional `brickhouse[delta]` extra, pinned to `deltalake==1.2.1` and `pyarrow==23.0.1`. The service resolves registered identities, reads latest/version/timestamp snapshots, and materializes Arrow data. Create, append, and overwrite validate exact schemas and protocol 1/2 with no table features; partitioned tables are rejected. Staged Parquet and Arrow IPC bytes are verified against workspace ownership, SHA-256, byte count, declared schema, and row count before provider writes. In-process mutations lock by both registered table ID and canonical path, including path aliases. B008.3 adds schema-version-4 SQLite operation records with payload fingerprints and monotonic per-table Delta transaction versions. Before dispatch it commits the fingerprint and intent; each provider commit carries the table transaction and operation metadata. Replays return the recorded version when both the transaction and matching history entry are confirmed. Changed-payload key reuse fails. Unconfirmed outcomes stay uncertain and block blind retries. Recovery searches the latest 100 history entries, so a missing or expired record remains uncertain. B008.4 adds local registration removal. External drops remove metadata and preserve all external bytes. Managed drops open the direct workspace `tables` root without following symlinks and recursively remove only the registered UUID child. Cleanup runs before atomic metadata removal; a process interruption leaves the registration so a repeated drop can finish cleanup, including when the directory is already absent. Table mutation history is removed with the registration. Unfinished writes prevent removal. This remains a local provider service only; SQL and Sparkless do not route through it. Cloud storage, schema evolution, partitioning, and cross-workspace/process coordination remain unsupported. See [ADR 0012](../decisions/0012-table-snapshot-and-staging-contracts.md), [ADR 0013](../decisions/0013-local-delta-table-service.md), [ADR 0014](../decisions/0014-delta-operation-journal.md), [ADR 0015](../decisions/0015-ownership-safe-table-removal.md), and the [B008.2](../../artifacts/conformance/B008.2-local-delta-table-service.md), [B008.3](../../artifacts/conformance/B008.3-operation-journal-recovery.md), and [B008.4](../../artifacts/conformance/B008.4-ownership-safe-removal.md) evidence.

## Catalog model

Use SQLite migrations for workspaces, catalogs, schemas, table registrations, volumes, workspace objects, operation journals, jobs, task attempts, and capability/profile state. Table records include stable ID, fully qualified name, format, managed/external ownership, canonical location, creation metadata, and lifecycle state. Delta owns the physical schema and committed data version; a catalog cache must record its observed version.

Normalize identifiers with a documented Spark/Databricks profile. Preserve quoted names and display casing. Resolve current catalog/schema per session/request; enforce duplicate-name rules. Reject invalid traversal and resolve filesystem aliases/symlinks before ownership decisions. A single canonical physical location cannot be registered with conflicting ownership. Locks key on physical identity as well as table ID to prevent alias races.

Bootstrap an explicit default catalog/schema during workspace initialization. Tests create additional namespaces rather than relying on accidental defaults.

## Managed and external tables

Managed tables are real Delta directories under a UUID-based workspace storage path. External tables reference validated locations. `drop_table()` removes metadata for external tables and preserves their files. For managed tables it securely removes the direct UUID child under the workspace-owned `tables` directory before deleting metadata. If recursive deletion fails or the process stops before metadata removal, the registration remains and a repeated drop completes cleanup; an absent directory is safe to finalize. A pending or uncertain write blocks removal. Dropping a table removes its table-scoped operation journal rows in the same SQLite transaction as its registration. Rename changes catalog identity mapping and does not require moving data directories.

Creation with data proceeds through a journaled operation: reserve metadata identity, stage data, commit Delta, finalize metadata. Failure before commit cleans safe staging; failure after commit is reconciled from recorded operation identity. Table visibility must exclude incomplete creations until finalization or an explicit recovery state.

## File namespaces

Distinguish workspace files, DBFS paths, volumes, and raw table locations. Map `dbfs:/`, `/dbfs/`, `/Volumes/catalog/schema/volume/`, and local paths using a single path resolver. The mount root seen by the server can differ from the worker's root; pass explicit mapping, never string-replace arbitrary paths.

Use local files first. fsspec can support Python-facing backends, but Sparkless native readers require native-compatible URIs or a deliberate staging layer. Cloud URI support is not implemented merely by accepting an `s3://` or `abfss://` string.

## Table-service port

Proposed logical operations:

- `resolve(name, context) -> TableHandle`
- `drop_table(handle) -> None`
- `read_snapshot(handle, version?) -> schema + Arrow batches or native scan descriptor`
- `create/append/overwrite(handle, staged_input, write_options, operation_id) -> CommitResult`
- `update/delete/merge(handle, validated_expression_or_plan, operation_id) -> CommitResult`
- `history/describe/optimize/vacuum(handle, options) -> typed result`

`CommitResult` carries table identity, Delta version, physical schema, operation ID, and metrics. Record an input/options fingerprint for idempotency keys; reject reuse with different content. Empty tables, append schema mismatch, overwrite semantics, partition handling, and missing targets must be explicit tests.

Use Delta transactions and provider APIs. For retries after an uncertain response, reconcile an operation ID from provider transaction metadata/history before repeating. If the selected API cannot support reliable reconciliation, return an explicit uncertain-commit state and block automatic retries for that operation; do not advertise exactly-once behavior.

ADR 0008 selects Python `deltalake==1.2.1` for phase 0.1 writes. B004.3 found that `CommitProperties.app_transactions` writes a durable table-scoped transaction identifier that survives a classic checkpoint and can be queried after a process crash. Reissuing `write_deltalake` with the same application ID/version still committed a second append, so the provider call is not idempotent by itself. B008.3 implements the table-scoped application ID, monotonic transaction version, and payload/options fingerprint in the SQLite journal before dispatch. Before a retry, the service checks the provider transaction version and matching operation/fingerprint in retained history; both are required to return a known commit version. The service scans at most the latest 100 history entries. If identity, payload, or exact commit version cannot be reconciled, it preserves an uncertain journal state and stops automatic retry. Transaction retention and log cleanup can remove evidence; cleanup therefore produces uncertainty rather than an inferred not-committed result. These guarantees cover the tested local profile, not concurrent writers from separate workspaces or hosts. See [ADR 0008](../decisions/0008-delta-mutation-and-recovery.md) and [ADR 0014](../decisions/0014-delta-operation-journal.md).

## Initial Delta envelope

Begin with local, unpartitioned tables and a verified primitive/nested-type subset; then add partitioning and schema evolution behind capabilities. Determine minimum reader/writer versions from actual fixtures rather than hardcoding guessed protocol numbers.

Reject unsupported table features before reads or writes. In particular, reading active Parquet files is not sufficient for deletion-vector semantics. Detect unsupported column mapping, deletion vectors, catalog-managed commits, and similar features even if simple scans appear to succeed. Every advertised feature must work through both Sparkless and SQL.

## SQL pipeline

1. Parse the declared Databricks/Spark dialect; preserve parameters as bound values.
2. Classify DDL, table mutations, queries, utility statements, and unsupported syntax using an AST.
3. Bind identifiers to the catalog, resolve request defaults, and check authorization/capabilities.
4. Route catalog changes to catalog services; table mutations to the Delta provider; queries to the declared engine.
5. Produce a unified schema/result stream and typed error envelope.

SQLGlot translation is useful for approved DuckDB execution. Do not use regex substitution for catalog names, external table locations, or predicates. A query can parse and transpile successfully but still differ semantically. Explicitly test null ordering, type coercion, integer overflow, decimal precision, dates/timezones, NaNs, duplicate columns, arrays/maps, and case resolution.

Prefer Sparkless for shared DataFrame/Spark SQL behavior. If DuckDB cannot evaluate an operation with the selected profile's semantics, reject it or route it by a predetermined capability plan before execution. Do not fall back after exceptions.

## Statement Execution API

Implement the pinned client's request/response shapes, warehouse identifiers, positional/named parameter forms where supported, wait/timeout behavior, result schemas, row encoding, chunk indices, cancellation, errors, and result retention. Separate statement lifecycle from query execution even for short queries. Reject result formats/dispositions not yet implemented.

Start with bounded JSON results; add Arrow/CSV and external result links only with actual retrieval and lifetime tests. Do not keep unbounded query data in module-global dictionaries. Result links must resolve to BrickHouse-owned artifacts, not arbitrary file reads.

B011.5 owns warehouse record/lifecycle and admission contracts; B011.6 binds those contracts to actual statements before B011.3–B011.4 qualify cancellation/results. B045 resolves released-connector host/TLS/auth/path feasibility early, while B021 still proves real query/fetch behavior. [Document 16](16-phase-0.1-initial-release.md) sets the finite corpus, fixture limits and completion boundaries.

## Consistency acceptance cases

- A table written by a worker is read identically through SDK SQL and a fresh Sparkless session.
- Schema changes invalidate cached metadata; time-travel reads retain the requested version.
- SQL and DataFrame append share the same Delta history and preserve all committed rows.
- Restart during each create/commit/finalize phase produces a documented recoverable state.
- A retry of the same operation does not duplicate data; changed payload with the same key fails.
- External table drop preserves bytes; managed cleanup cannot delete outside the owned root.
- No advertised Delta feature can be bypassed through a different read/write surface.

## Preparation for later self-hosting

Preserve stable catalog/table/artifact identity separately from the local storage root. Current canonical local paths, ownership checks and journal evidence keep their existing scope. B048 records actual staging/path coupling; it does not replace local storage during 0.1. Emulator phases 0.2–0.9 retain identity, snapshot, protocol and mutation invariants in their own work.

After emulator stability, SH.1 enforces deployed caller identity for queries/results/mutations and SH.4 qualifies root mapping, snapshot access, native IO and bounded shared-storage/artifact transfer across hosts. Worker lease/generation validation must fence service commits without bypassing the Delta provider or uncertain-commit journal. Shared SQLite on an unverified network filesystem, accepted cloud URI strings or Python-only filesystem mocks cannot qualify this behavior. Restore/relocation requires explicit location mappings and managed/external ownership checks. See [document 18](18-self-hosted-platform.md).
