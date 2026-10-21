# 01 — Architecture

Status: target architecture. B006.1–B006.3 implement local workspace bootstrap, POSIX ownership, the embedded lifecycle, and a loopback server. B007.1 adds schema-versioned catalog/schema IDs and local table-registration metadata repositories. B007.2 adds a local identifier resolver and a fresh namespace-state primitive per future session. B007.3 adds a bounded local REST profile for pinned SDK catalog/schema operations. B008.1 defines provider-neutral table handles, Delta snapshot/schema/protocol descriptors, write validation, and owned local staging. B008.2–B008.4 add optional delta-rs reads/writes, operation recovery and ownership-safe removal. B014.2 adds workspace Python-source import and pinned-SDK single-task Python execution through fresh local workers. Delta table access, Sparkless, SQL, notebook behavior, full compute lifecycle, and broad Databricks API compatibility remain unimplemented.

## System boundaries

```mermaid
flowchart TB
    clients[Databricks SDK / CLI / Terraform / SQL clients] --> api[BrickHouse HTTP adapters]
    api --> services[Catalog / table / jobs / workspace / secret / compute services]
    services --> meta[(SQLite metadata and run state)]
    services --> delta[Native Delta provider]
    delta --> files[(Shared local storage)]
    services --> supervisor[Job supervisor]
    supervisor --> worker[Fresh Python worker + runtime context]
    worker --> engine[Sparkless adapter]
    worker --> internal[Private worker protocol]
    internal --> services
    api --> planner[SQL parser / binder / capability planner]
    planner --> engine
    planner --> duck[Optional DuckDB query adapter]
    engine --> tables[Catalog and table bridge]
    tables --> services
    duck --> files
    optional[MLflow / notebooks / dbt / vector / streaming] --> services
```

Transport code validates wire requests and calls domain services. Domain services own invariants. Repositories own persistence. Execution providers return typed results. Job processes are consumers of these services, not additional authoritative catalogs.

## Future deployment boundary

The local JVM-free emulator remains primary. [Document 18](18-self-hosted-platform.md) and [ADR 0034](../decisions/0034-emulator-first-self-hosted-platform.md) preserve the current service direction while staging deployment after B049's stability gate. B048 reviews the actual local G1–G4 paths before B023: stable resource IDs and shared services; contained path/staging, interpreter/PID/signal/socket assumptions; and documented future identity entry points. This review adds no remote backend, generic provider framework or no-op security layer.

A future browser or remote transport calls the same domain services. A future worker pool keeps catalog metadata and commits in the authoritative control service; workers do not share a writable SQLite file or own a second catalog. Principal provenance, enforced authorization, authenticated worker leases and alternate data transport need their own later contracts and real tests. Current local locks, private pipes and process identity retain their tested scope; they do not implement distributed fencing.

SQLite with one local metadata owner remains valid for the initial deployment design. Replacing it, adding HA or distributing one query requires a separate need and evidence. Every future executor still runs without Java/Spark or hidden Databricks fallback.

## Authoritative state

| State | Authority |
|---|---|
| Catalog/schema/table identity, ownership, storage location | BrickHouse SQLite metadata |
| Table data, schema evolution, transaction version | Delta transaction log |
| Workspace/volume files | Configured local storage root |
| Job definitions, run state, task values, retry attempts | BrickHouse SQLite metadata |
| Cluster/warehouse identity, simulated infrastructure lifecycle, resolved compute/environment references | BrickHouse SQLite metadata through the workspace compute service |
| Session namespace, temporary views, session config, UDFs | Isolated engine session/worker |
| MLflow experiments/models | MLflow provider; BrickHouse stores references, not duplicate mutable copies |
| Vector index files and engine metadata | Vector provider, with lifecycle references in BrickHouse |

Minilake's managed DuckDB tables are not the physical-table model adopted here. Managed and external BrickHouse Delta tables both use actual Delta files; their ownership and deletion policies differ. DuckDB registrations are reconstructable query views of committed data. Do not let a cached DuckDB table become a second copy of truth.

## Module dependency direction

`api -> services -> ports/domain`; implementations of ports live in `persistence`, `storage`, `engines`, and `runtime`. Domain types cannot import FastAPI, Minilake, Sparkless native classes, or a cloud SDK. Python protocol contracts expose table handles, schema descriptors, Arrow streams, capability descriptors, and explicit errors.

The composition root creates an instance-scoped `WorkspaceContext`. `WorkspaceContext.new_namespace_state()` returns fresh current catalog/schema values for one future session; the workspace context does not hold those mutable values. Do not migrate Minilake module-global mutable dictionaries directly into shared BrickHouse server state. Multiple app instances in one interpreter must remain isolated.

## Deployment and process model

Default: one HTTP server, one metadata writer boundary, and configurable child job processes. SQLite is accessed through repositories in the server; workers use a scoped internal protocol for metadata, secrets, task values, table commits, and heartbeats. Local file access uses validated storage paths. B006.2 provides the POSIX advisory workspace-owner handle; B006.3 composes it into the embedded `WorkspaceContext` and CLI server. On IPv4 loopback the server exposes `/health`, `/version`, `/capabilities`, catalog/schema routes, classic cluster lifecycle routes, Python source import, and a pinned-SDK subset for Python job create/submit/poll/output. B014.3.1 dispatches up to ten tasks sequentially in topological order with persisted dependency gating; B014.3.2.1 adds bounded retries; B014.3.2.2 admits up to four active workers process-wide and enforces each job's declared limit with non-retryable HTTP 409 capacity conflicts. Restart reconciliation, SQL, and a Spark/Sparkless session remain future work. B008.1 adds typed contracts/staging; B008.2–B008.4 provide an explicit optional Delta service that is not composed into HTTP routes or workers. The `run` CLI command still reports that direct script execution is unavailable. The local SDK token is a compatibility value, not an authorization boundary.

The private worker protocol is separate from public Databricks endpoints, uses an unguessable run-scoped token, and is valid only for the worker's lifetime. Tokens and secret values are not passed in URLs or logged. Define internal messages with a schema/version and reject stale workers explicitly.

Each job attempt gets a fresh interpreter using `sys.executable` from a resolved environment, a working directory, immutable run context, and its own engine session. B016.1 uses a worker entry module to inject local `dbutils.fs` and widget helpers without mutating the parent interpreter. B016.3 sends task value requests over a per-attempt authenticated local socket; only the parent process accesses SQLite, and values are stored under run/task/attempt/key. The supervisor captures output, enforces timeout/cancellation, and persists state transitions. Process isolation limits state leakage; it is not a sandbox for hostile code. The DBFS compatibility root is `workspace/files/dbfs`; volume and mount namespaces remain separate future work.

## SQL and execution policy

Parse and bind before choosing an executor. Table/catalog DDL and DML route to their domain services. Sparkless is the semantic default for supported queries. DuckDB is an explicit capability-selected backend for tested operations; SQLGlot can translate an AST for it. Do not retry a failed Sparkless query in DuckDB, especially after a possible mutation.

Pin namespace, timezone, ANSI behavior, and statement snapshot policy at the execution boundary. Return schema and results through one typed result protocol regardless of executor. Arrow conversion is preferred; JSON row conversion may be a bounded temporary path for early fixtures, with no unmeasured zero-copy claim.

## Planned compute profiles

[Document 17](17-serverless-and-classic-compute.md) adds serverless/classic Python and notebook profiles and three SQL warehouse profiles. A workspace-scoped compute service resolves resource IDs and legal simulated lifecycle transitions; B011.5 implements the warehouse slice and B014.4 the classic slice. B014.5 resolves supported SDK selectors, provisioned environment/runtime mappings and settings into immutable attempt context before dispatch. SQL resolves its warehouse through the same service authority. Request models only validate transport shape.

Compute mode, access mode, reference runtime/environment and protocol are independent choices. Both modes target Sparkless and the same table/commit service, but worker-facing effects are still incomplete. Each job attempt retains a fresh child process; shared cluster IDs do not imply shared Python/JVM state. Notebook state belongs to its kernel and is invalidated on reset/compute termination. B014.6 now binds the seven selected Python job contexts to local worker/resource lifetimes and tests cancellation/reaping on all-purpose compute restart or termination; it does not provide Sparkless or table access in those workers. B011.6 and B017.5 separately prove real statement/kernel effects after their resource/context tasks complete; [document 16](16-phase-0.1-initial-release.md) fixes the work order and release case matrix. Unknown resources, unsupported settings or unavailable environments fail explicitly. No new public compute interface or full serverless/classic compatibility is established by this target architecture.

## Mutations and concurrency

All BrickHouse-owned table writes participate in a common table-service contract. Prefer one selected delta-rs implementation/version for mutations. A per-table lock serializes BrickHouse mutation requests initially; Delta optimistic concurrency remains necessary for writers outside that lock.

Dataframe writers in workers stage Arrow or Parquet data in a run-owned location and request a commit from the table service. The service controls creation, append, overwrite, schema evolution, and idempotency. If an early prototype must use a Sparkless native writer directly, it must be confined to an isolated table and cannot satisfy the shared-write milestone until it uses the same commit contract.

## Failure boundaries

Metadata transactions and Delta commits cannot share a SQLite transaction. Use an operation journal with IDs, staged state, committed Delta version, and recovery reconciliation. Document orphan files and uncertain commits; never claim rollback of a Delta commit by deleting metadata. Cancellation and retries must consult the operation journal before reporting success or repeating a write.

On restart, reconcile incomplete metadata operations and child runs. Interrupted work must become recoverable or explicitly failed. Partial execution must never be reported as a successful completed job.
