# 05 — Catalog, storage, and SQL contracts

## Catalog model

Use SQLite migrations for workspaces, catalogs, schemas, table registrations, volumes, workspace objects, operation journals, jobs, task attempts, and capability/profile state. Table records include stable ID, fully qualified name, format, managed/external ownership, canonical location, creation metadata, and lifecycle state. Delta owns the physical schema and committed data version; a catalog cache must record its observed version.

Normalize identifiers with a documented Spark/Databricks profile. Preserve quoted names and display casing. Resolve current catalog/schema per session/request; enforce duplicate-name rules. Reject invalid traversal and resolve filesystem aliases/symlinks before ownership decisions. A single canonical physical location cannot be registered with conflicting ownership. Locks key on physical identity as well as table ID to prevent alias races.

Bootstrap an explicit default catalog/schema during workspace initialization. Tests create additional namespaces rather than relying on accidental defaults.

## Managed and external tables

Managed tables are real Delta directories under a UUID-based workspace storage path. External tables reference validated locations. `DROP TABLE` removes metadata for external tables and preserves their files; managed deletion uses a documented lifecycle/tombstone policy. Rename changes catalog identity mapping and does not require moving data directories.

Creation with data proceeds through a journaled operation: reserve metadata identity, stage data, commit Delta, finalize metadata. Failure before commit cleans safe staging; failure after commit is reconciled from recorded operation identity. Table visibility must exclude incomplete creations until finalization or an explicit recovery state.

## File namespaces

Distinguish workspace files, DBFS paths, volumes, and raw table locations. Map `dbfs:/`, `/dbfs/`, `/Volumes/catalog/schema/volume/`, and local paths using a single path resolver. The mount root seen by the server can differ from the worker's root; pass explicit mapping, never string-replace arbitrary paths.

Use local files first. fsspec can support Python-facing backends, but Sparkless native readers require native-compatible URIs or a deliberate staging layer. Cloud URI support is not implemented merely by accepting an `s3://` or `abfss://` string.

## Table-service port

Proposed logical operations:

- `resolve(name, context) -> TableHandle`
- `read_snapshot(handle, version?) -> schema + Arrow batches or native scan descriptor`
- `create/append/overwrite(handle, staged_input, write_options, operation_id) -> CommitResult`
- `update/delete/merge(handle, validated_expression_or_plan, operation_id) -> CommitResult`
- `history/describe/optimize/vacuum(handle, options) -> typed result`

`CommitResult` carries table identity, Delta version, physical schema, operation ID, and metrics. Record an input/options fingerprint for idempotency keys; reject reuse with different content. Empty tables, append schema mismatch, overwrite semantics, partition handling, and missing targets must be explicit tests.

Use Delta transactions and provider APIs. For retries after an uncertain response, reconcile an operation ID from provider transaction metadata/history before repeating. If the selected API cannot support reliable reconciliation, return an explicit uncertain-commit state and block automatic retries for that operation; do not advertise exactly-once behavior.

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

## Consistency acceptance cases

- A table written by a worker is read identically through SDK SQL and a fresh Sparkless session.
- Schema changes invalidate cached metadata; time-travel reads retain the requested version.
- SQL and DataFrame append share the same Delta history and preserve all committed rows.
- Restart during each create/commit/finalize phase produces a documented recoverable state.
- A retry of the same operation does not duplicate data; changed payload with the same key fails.
- External table drop preserves bytes; managed cleanup cannot delete outside the owned root.
- No advertised Delta feature can be bypassed through a different read/write surface.
