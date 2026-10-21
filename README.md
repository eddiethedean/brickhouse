# BrickHouse

**A Python package that aims to emulate Databricks locally without a JVM.**

JVM-free Databricks emulation remains the primary roadmap through phases 0.1–0.9. After the emulator is stable, a separate [self-hosted platform track](docs/plan/18-self-hosted-platform.md) plans deployment on users' own infrastructure. That future track does not add deployment runtimes or services to the base local workflow.

The package currently provides importable version metadata, local workspace bootstrap and ownership, an embedded `Workspace.local(...)` lifecycle, persistent catalog/schema IDs and table-registration metadata, a per-session namespace-state primitive, a loopback HTTP server, a partial local Delta table service, and a bounded SDK Python job flow (`0.1.0.dev0`). The pinned Databricks SDK can upload Python source to `/Workspace/...`, create Python script jobs, submit a bounded DAG, poll observed child-process results, and retrieve bounded output for single-task runs. Supported tasks execute sequentially in a stable topological order in fresh local workers; the service admits at most four active runs, binds workers to the selected simulated classic compute lifecycle, and reconciles jobs left behind by a process restart without replaying user code. Workers receive real local `dbutils.fs` operations under `workspace/files/dbfs`, attempt-local widget values, and job-granted in-memory secrets sent over a private one-way pipe; exact secret values are redacted from retained output. `dbutils.jobs.taskValues` uses a private authenticated parent channel and stores finite JSON by run/task/attempt/key, capped at 48 KiB per value and 250 distinct task/key pairs per run; readers are limited to successful declared upstream tasks. Secret values and grants are not persisted and must be configured again after service restart. Filesystem paths are bounded but arbitrary Python access is not sandboxed. Seven selected serverless/classic-standard/classic-dedicated job shapes are exercised against the local Python worker. Spark/Sparkless APIs, job-facing table operations, and Databricks runtime parity are not yet provided. The server also exposes health/version/capability diagnostics, catalog/schema CRUD routes, and classic cluster lifecycle routes backed by SQLite. **Compute IDs and infrastructure lifecycle are simulated; no cloud compute is allocated.** `brickhouse.tables.DeltaRsTableService` uses the optional `brickhouse[delta]` extra for local Delta snapshot reads, create/append/overwrite, durable operation replay recovery, and ownership-aware table removal. It accepts reader/writer protocol 1/2 with no features, unpartitioned tables, and exact schemas. External table drops preserve their files; managed removal only deletes the UUID-named directory directly under the workspace `tables` root. Interrupted managed cleanup can be retried while the registration remains. The provider is not yet connected to Sparkless, SQL, or job execution. `new_namespace_state()` returns independent current catalog/schema values and resolves registered one-, two-, and three-part names; it is not a Sparkless execution session. Research baseline: **2026-10-20**. The developer engine candidate is `sparkless==4.14.0`: its public Python provider hooks are verified, while nullable-struct bugs [#1694](https://github.com/eddiethedean/robin-sparkless/issues/1694) and [#1695](https://github.com/eddiethedean/robin-sparkless/issues/1695) block the selected integration envelope. See [the probe report](artifacts/conformance/B046.3-sparkless-4.14.0-provider.md).

Databricks CLI v1.21.0 also passes an opt-in loopback workflow for one classic single-task Python bundle: validate, deploy, run with actual local worker output, and destroy with immutable job/run history retained. Current-user identity and new-cluster metadata are simulated; this does not establish authentication, serverless/notebook bundle support, or remote workspace behavior. See the [B020.3 conformance report](artifacts/conformance/B020.3-databricks-cli-bundle-v1.21.0.md).

BrickHouse will combine Sparkless execution, selected Minilake-derived API behavior, native Delta Lake operations, and optional ecosystem integrations. The Sparkless maintainer owns this project and can coordinate Sparkless releases. Other dependencies are independently maintained; implementation must not depend on unaccepted upstream changes.

The plan explicitly includes **serverless and classic Python jobs/notebooks**, plus **serverless, pro, and classic SQL warehouses**. The bounded initial subsets are required for 0.1.0: real local execution with mode-specific validation, environments/settings, session behavior and resource lifetimes. The SDK job route binds actual Python workers to selected serverless/classic request contexts and simulated classic resource lifetimes, including cancellation on all-purpose compute restart or termination. It does not provide a Spark/Sparkless session or job-facing Delta read/write effects; the nullable-struct Sparkless blockers remain open. The broader compute profiles remain incomplete; see [the compute behavior contract](docs/plan/17-serverless-and-classic-compute.md) for research, release allocation and limitations.

## Start here

1. Read [AGENTS.md](AGENTS.md) for implementation rules.
2. Read [the product scope](docs/plan/00-vision-and-scope.md), [architecture](docs/plan/01-architecture.md), and [package design](docs/plan/02-packages-and-public-api.md).
3. Follow [the Codex bootstrap instructions](docs/plan/CODEX_BOOTSTRAP.md).
4. Follow [the 0.x release roadmap](ROADMAP.md), starting with **phase 0.1 / version 0.1.0**, and execute [the engineering milestones](docs/plan/08-roadmap.md) using [the task backlog](docs/plan/09-task-backlog.md).
5. Maintain [STATUS.md](STATUS.md), [task state](planning/tasks.json), and [capability evidence](planning/capabilities.json).

## Planning documents

| Document | Purpose |
|---|---|
| [00 — Vision and scope](docs/plan/00-vision-and-scope.md) | Meaning of full emulation, initial users, compatibility profiles |
| [01 — Architecture](docs/plan/01-architecture.md) | Services, execution boundaries, authoritative state |
| [02 — Packages and public API](docs/plan/02-packages-and-public-api.md) | Distribution strategy, internal modules, proposed user interfaces |
| [03 — Dependencies and Minilake](docs/plan/03-dependencies-and-minilake.md) | Adoption strategy, source provenance, independent upstreams |
| [04 — Sparkless changes](docs/plan/04-sparkless-upstream.md) | Engine extension contracts and coordinated delivery |
| [05 — Catalog, storage, SQL](docs/plan/05-catalog-storage-sql.md) | Tables, Delta commits, SQL planning and consistency |
| [06 — Jobs, notebooks, dbutils](docs/plan/06-jobs-notebooks-dbutils.md) | Runtime, lifecycle, imports, utilities, execution isolation |
| [07 — Compatibility and testing](docs/plan/07-compatibility-and-testing.md) | Behavioral oracles, coverage reporting, JVM-free gates |
| [08 — Roadmap](docs/plan/08-roadmap.md) | Milestones, dependencies, release acceptance criteria |
| [09 — Task backlog](docs/plan/09-task-backlog.md) | Implementable work units and acceptance criteria |
| [10 — Streaming and pipelines](docs/plan/10-streaming-and-pipelines.md) | Micro-batches, recovery, Auto Loader, Lakeflow |
| [11 — ML and optional extensions](docs/plan/11-ml-and-extensions.md) | MLflow, dbt, vector search, storage emulators, governance |
| [12 — Delivery and operations](docs/plan/12-delivery-and-operations.md) | Configuration, packaging, persistence, CI, releases |
| [13 — Decisions and risks](docs/plan/13-decisions-and-risks.md) | Adopted defaults, open decisions, evidence gates |
| [14 — Research sources](docs/plan/14-research-sources.md) | Primary sources, observed facts, revalidation guidance |
| [15 — Personal Databricks verification](docs/plan/15-personal-databricks-verification.md) | Occasional real-account probes that guide local tests and sanitized fixtures |
| [16 — Phase 0.1 implementation contract](docs/plan/16-phase-0.1-initial-release.md) | Implementation lanes, seven Python/ten notebook variants, local fixture limits, 27 acceptance families and release evidence |
| [17 — Serverless and classic compute](docs/plan/17-serverless-and-classic-compute.md) | Compute profiles, primary-source research, local execution/simulation boundaries, release allocation and test designs |
| [18 — Self-hosted platform](docs/plan/18-self-hosted-platform.md) | Emulator-first priority, phase-by-phase architecture preparation, post-stability deployment stages and evidence gates |

The plan includes **55 top-level work packages**, **85 required phase 0.1 child tasks**, and **64 capability requirements** (including six planned post-stability deployment capabilities). See the [manifest guide](planning/README.md) for state/evidence rules and the [planning validation report](planning/validation.json) for document checks.

## Intended first end-to-end workflow

Development uses the owner's Databricks Free Edition workspace for occasional bounded reference verification and test design. The first sanitized SQL capture records a literal result covering UTC timezone, decimal, date, timestamp, null, and array types; it is evidence for test design, not BrickHouse compatibility. Routine tests run locally using reviewed fixtures; an optional manual capture workflow may use the supplied credentials. See [the verification plan](docs/plan/15-personal-databricks-verification.md) and [the B005.3 capture](artifacts/conformance/B005.3-databricks-reference-sql.md).

The local SDK can already upload and execute a plain Python script and retrieve its output. The full target workflow creates a catalog and schema, submits a Python job that transforms data using Sparkless, writes a managed Delta table, queries it through the Statement Execution API, reads it by name from another session, and repeats the read after server restart. Sparkless/DataFrame execution, SQL integration and this end-to-end workflow remain unimplemented.

## Development

Install the pinned development tools and run the local checks:

```bash
python -m pip install --require-hashes -r requirements/dev.txt
python scripts/validate_planning.py
python -m ruff check src tests scripts
python -m mypy
python -m pytest
python -m build --no-isolation
```

Install the optional native Delta provider when using the table service:

```bash
python -m pip install '.[delta]'
```

The local lifecycle commands are:

```bash
brickhouse init --workspace ./workspace
brickhouse serve --workspace ./workspace
brickhouse capabilities --format json
brickhouse doctor --workspace ./workspace
```

`serve` accepts IPv4 loopback hosts only and holds the workspace owner lock until Ctrl-C or SIGTERM. The pinned SDK can use its catalog/schema CRUD profile, upload Python source through `/api/2.0/workspace/import`, and use the `/api/2.2/jobs` create/run-now/get/get-output/delete subset for bounded Python script DAGs. Databricks CLI v1.21.0 uses the `/api/2.0/workspace` file status/import/export/mkdirs/delete routes and the same job API for the tested classic single-task Python bundle lifecycle. Bundle destroy removes its managed workspace tree and tombstones the job while preserving immutable definitions and run history. The local current-user identity and new-cluster allocation are simulated; the API does not authenticate the CLI token. Up to ten tasks are dispatched sequentially in stable topological order through fresh local processes; failed dependencies are marked skipped, while independent tasks still run. Task retries use the validated 0–10 retry limit, honor retry delay and timeout policy, and persist each attempt's actual state/output. A job may set `max_concurrent_runs` from 1 through 4, within the four-worker process limit. Capacity conflicts return HTTP 409 (`ResourceConflict`) before a run is created; active and terminal idempotency replays return the original run. The worker receives bounded `python_params` as argv for single-task runs and persists actual exit state and bounded stdout/stderr. B014.6 binds workers to simulated classic compute lifetimes. B016.1 injects attempt-local widgets and a supported `dbutils.fs` subset mapped to `workspace/files/dbfs`; B016.2 adds explicitly granted in-memory secrets, and B016.3 adds attempt-scoped task values. Multi-task output retrieval, Spark/Sparkless sessions and worker-facing table operations remain unsupported; `brickhouse run ./job.py` remains unavailable. The `/api/2.1/unity-catalog` profile supports lowercase unquoted identifiers and comments, paginated listing, duplicate/missing errors, and safe deletion guards. Classic cluster routes persist all-purpose identity, selected settings, lifecycle states and invalidation history; the local `spark35-local-v1` and standard/dedicated access profiles are metadata mappings, not Databricks Runtime parity or a security boundary. Node allocation, restart, and idle shutdown are simulated. Unity Catalog permissions, workspace binding, storage roots, arbitrary properties, and real Databricks runtime behavior are not implemented. The SDK token is not authentication. Provider diagnostics inspect installed distribution metadata without loading optional modules or credentials. The embedded `Workspace.local(directory)` context uses the same ownership lifecycle and exposes catalog/schema, table-registration, job-state and compute-lifecycle repositories. `context.new_namespace_state()` makes a fresh namespace state for each future session; quoted and multipart names resolve to local table registrations. The explicit `DeltaRsTableService` reads local snapshots by latest/version/timestamp and writes staged Parquet or Arrow IPC data through delta-rs. A SQLite journal and provider transaction metadata support exact replay reconciliation after process death; unclear outcomes block blind retries. History reconciliation checks the latest 100 commits. `drop_table()` preserves external files and uses symlink-resistant removal for managed workspace tables, keeping metadata registered if filesystem cleanup is interrupted. The Delta service is not exposed by HTTP or connected to Sparkless/SQL sessions.

Focused local tests cover workspace initialization/migration, lock ownership, catalog/table registration metadata, the optional Delta service profile, process-death replay recovery, ownership-safe removal, diagnostics, and clean server shutdown. CI checks Python 3.11 and 3.12 without Databricks credentials or cloud access.

Regenerate `requirements/dev.txt` only when deliberately updating the developer toolchain: install `requirements/lock-tools.txt`, then run `pip-compile --generate-hashes --strip-extras --output-file requirements/dev.txt requirements/dev.in`.
