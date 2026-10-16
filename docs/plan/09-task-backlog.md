# 09 — Implementation backlog

This document mirrors `planning/tasks.json`: the JSON ledger is canonical for task definitions and state. Keep this readable view synchronized when task definitions change. Initial state: all work is planned; no implementation evidence is claimed.

## Task execution rules

Complete dependencies first. A research task can finish with a documented negative result if its acceptance criteria ask for a decision; an implementation task cannot finish without the promised behavior. `deferred` dependencies do not satisfy `done`. Revise release scope explicitly when needed.

B001–B023 are initial work packages. Split any that cannot be implemented and reviewed coherently into children before starting; parent completion requires every required child and original acceptance criterion. B024–B042 are epics and must be decomposed first. Assign new child IDs such as `B008.1`, preserving the parent requirements and dependency graph. Child tasks inherit the parent prerequisites, not a dependency on the parent itself.

Each completed task records commands/checks, exact versions, result summaries, relevant fixture/report paths, and commit references when available. Source research alone is not evidence that BrickHouse behavior works.

## B001 — Create installable project skeleton

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Dependencies:** None.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** pyproject.toml; src/brickhouse; CLI help/version; development and test configuration.

**Acceptance criteria:**

1. Build wheel and sdist and install the wheel into a clean environment.
2. Import brickhouse and invoke CLI help/version without starting a server or importing optional integrations.
3. Record initial dependency and supported-platform candidates; preserve planning documents.

## B002 — Audit Minilake reuse and provenance

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Dependencies:** B001.

**Plans:** [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [14-research-sources.md](14-research-sources.md).

**Deliverable:** docs/upstream/minilake-inventory.md and a reuse ADR; no wholesale app mounting.

**Acceptance criteria:**

1. Inspect the pinned source and record each proposed file/contract, license, globals, persistence, and Spark dependencies.
2. Choose adaptation versus fresh implementation for the first catalog/jobs/files routes and identify required removals.
3. Specify notices and tests before copying source; record a rationale if no implementation code is reused.

## B003 — Prove Sparkless provider extension design

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Dependencies:** B001.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md).

**Deliverable:** Minimal engine integration probe, proposed public contracts, and coordinated upstream task list.

**Acceptance criteria:**

1. Read current Rust/Python APIs and identify what already exists versus missing S001–S006 hooks.
2. Demonstrate a viable path for external relation binding, writer delegation, typed interchange, and isolated sessions; a temporary probe is clearly separated from production code.
3. Record callback ownership, GIL/thread/reentrancy risks, failure propagation, and the smallest upstream change required.

## B004 — Select native Delta provider and feature envelope

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Dependencies:** B001.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md).

**Deliverable:** Delta-provider ADR, pinned dependency matrix, and interoperability fixtures.

**Acceptance criteria:**

1. Exercise real create/read/append/update/delete/merge with candidate native providers; record unsupported operations explicitly.
2. Cross-read supported types and versions through Sparkless and selected SQL path, including nulls/decimals/timestamps.
3. Probe transaction identity and crash-after-commit reconciliation; choose one mutation provider and document protocol/feature rejection rules.

## B005 — Pin client profiles and capture transport fixtures

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Dependencies:** B001.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [15-personal-databricks-verification.md](15-personal-databricks-verification.md), [ADR 0001](../decisions/0001-personal-databricks-reference-verification.md).

**Deliverable:** Client version matrix; personal-workspace capture profile; bounded reference tooling/manual workflow; sanitized request/response and semantic fixtures.

**Acceptance criteria:**

1. Pin SDK, CLI, Terraform, SQL connector, and initial semantic profile versions after inspecting actual released artifacts.
2. Inspect the SQL connector REST/kernel option and default Thrift path separately; record TLS/auth/result requirements.
3. Define minimal supported workflows and fixture provenance; use the existing personal-account reference authorization only within the bounded capture/resource policy in document 15.
4. Record the personal workspace host alias, observed account features, relevant runtime/warehouse versions, permitted existing compute and scratch namespace, limits, and cleanup policy; never retain credentials or infer a spend budget from the token.
5. Implement named synthetic reference probes outside test discovery and capture at least one small real behavior case with schema/results or errors, exact client/settings provenance, reviewed sanitization, and cleanup evidence; map observations to local regression-test designs without claiming BrickHouse support from remote success.
6. Provide an optional workflow_dispatch capture workflow restricted to reviewed default-branch code and curated probes, with job-scoped DATABRICKS_TOKEN, bounded execution and sanitized artifacts; no automatic PR/push/schedule execution or required CI dependency.
7. Verify fixture tooling works without credentials and ordinary local clients cannot load .env, discover personal credentials, or contact real Databricks hosts even when credentials are inherited; missing configuration fails explicit captures clearly without redirecting local tests.

Decompose the expanded capture/tooling work into bounded child tasks before implementation, preserving all B005 criteria. Remote capture informs tests; normal suites and milestone gates consume reviewed fixtures locally.

## B006 — Implement workspace lifecycle and persistence

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B001.

**Plans:** [01-architecture.md](01-architecture.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Workspace/config services, SQLite migrations, lifecycle CLI and embedded context.

**Acceptance criteria:**

1. Initialize an isolated workspace with explicit default namespace policy and reopen it after restart.
2. Enforce one service owner, validate config/workspace versions, and reject incompatible formats.
3. Two different workspace directories have independent state and clean shutdown.

## B007 — Implement catalog identity and namespace services

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B002, B005, B006.

**Plans:** [01-architecture.md](01-architecture.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md).

**Deliverable:** Instance-scoped catalog repositories/services and initial SDK API routes.

**Acceptance criteria:**

1. Create/list/get/delete catalogs and schemas using the pinned SDK, including pagination/error fixtures where advertised.
2. Resolve quoted/multipart identifiers and per-session defaults without catalog-prefix stripping.
3. Persist stable table registrations and managed/external ownership; reject conflicting canonical locations.

## B008 — Implement native Delta table service

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B004, B007.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Table-service port, native provider, operation journal, staging and commit results.

**Acceptance criteria:**

1. Create/append/overwrite a real local Delta table through the service with typed results and schema checks.
2. Detect unsupported table features before any read/write and reject invalid mode/schema combinations.
3. Record operation fingerprints and commit versions; recover known completed operations and surface uncertain commits without blind retry.
4. Drop external metadata without deleting files; managed deletion cannot escape the owned root.

## B009 — Deliver generic Sparkless integration hooks

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B003.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md).

**Deliverable:** Upstream Sparkless changes and tested commit/artifact reference for required S001–S006 subset.

**Acceptance criteria:**

1. Implement reusable session-scoped catalog/relation/write hooks without BrickHouse/FastAPI/SQLite dependencies in Sparkless.
2. Run focused upstream tests for type preservation, delegation, errors, and session isolation under its repository instructions.
3. Document public hook signatures and artifact availability; dependent integration uses an actual tested artifact.

## B010 — Bind Sparkless sessions to BrickHouse tables

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B008, B009.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md).

**Deliverable:** Sparkless engine adapter, session factory, and table binding.

**Acceptance criteria:**

1. Explicit Python API writes a managed table then reads it by full name in a fresh session.
2. DataFrame writes and supported path saves route through the common table service; unsupported bypasses fail.
3. Verify typed interchange and independent namespaces/temp views/config/UDF state for two concurrent sessions.

## B011 — Implement initial Statement Execution path

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B010.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md).

**Deliverable:** AST parsing/binding, query and DDL routing, statement lifecycle/results API.

**Acceptance criteria:**

1. Pinned SDK executes supported SELECT/DDL over the same registered Delta tables and receives correct schema/row encoding.
2. Bound parameters, error responses, timeouts, polling, cancellation, and bounded JSON results have explicit tested behavior.
3. Reject unsupported syntax/formats before side effects; no exception-driven engine fallback.

## B012 — Prove shared-data and restart workflow

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B011.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md).

**Deliverable:** G1 conformance fixture and report.

**Acceptance criteria:**

1. Write via Sparkless, query via SDK SQL, read in a fresh session, restart the service, and obtain the same committed rows/schema.
2. Run without Java/full PySpark/Docker or cloud network access; capture process/dependency evidence.
3. Exercise empty data, schema mismatch, external table drop, and stale metadata invalidation.

## B013 — Implement process supervisor and worker protocol

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B006.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Fresh-process executor, scoped private protocol, logs and cancellation.

**Acceptance criteria:**

1. Launch the configured Python environment by resolved executable with explicit cwd/context and bounded logs.
2. Cancel and time out a worker including descendant processes; persist actual exit status and clean staging.
3. Prevent session/import/context leakage between attempts and deny invalid worker credentials.

## B014 — Implement durable jobs and workspace files

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B002, B005, B013.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Workspace file transport, job/run/task state machine and SDK routes.

**Acceptance criteria:**

1. Use real SDK upload/submit/poll/output against a Python file with parameters and captured stdout.
2. Persist transitions and task attempts; reject invalid DAGs and propagate failed dependency state.
3. Restart during an active run produces a documented interrupted/recoverable outcome; retries get distinct attempts.

## B015 — Implement managed-worker compatibility bootstrap

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B010, B013.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md).

**Deliverable:** Worker-scoped supported pyspark/delta import facade.

**Acceptance criteria:**

1. Run a representative script using supported pyspark imports against Sparkless and the shared table service.
2. Parent process and other environments retain their own import modules; conflicting/unsupported imports fail clearly.
3. Supported DeltaTable facade methods use the selected native provider; no delta-spark or JVM launch path.

## B016 — Implement scoped dbutils subset

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B014.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md).

**Deliverable:** Files, widgets/parameters, secrets, task values, and output-context adapters.

**Acceptance criteria:**

1. Exercise actual fs and widget behavior with path boundaries, missing values, and task-scoped context.
2. Resolve authorized secrets without persisting them in manifests or returning fabricated SDK values; test missing/denied access.
3. Persist supported task values per run/attempt and enforce serialization/size rules; unsupported methods raise explicit errors.

## B017 — Implement optional Python notebook runner

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B015, B016.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md).

**Deliverable:** nbformat/nbclient runner and explicit Databricks source/magic handling.

**Acceptance criteria:**

1. Execute ipynb and the declared Databricks source subset in fresh kernels with parameters, outputs, and error propagation.
2. Test %run shared-context behavior separately from child notebook execution and return values.
3. Reject unsupported magics/languages/package installation; cancel kernels and release resources reliably.

## B018 — Prove SDK-driven job workflow

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B012, B014, B015, B016, B017.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md).

**Deliverable:** G2 workflow and optional-notebook conformance report.

**Acceptance criteria:**

1. SDK submits a Python transformation that writes Delta; a dependent task and external SQL/session read observe the same result.
2. Exercise parameters, secrets, task output, dependency failure, cancellation, and retry without cross-run state leakage.
3. Run a separate notebook-extra workflow and the base workflow without notebook dependencies, Java, or cloud compute.

## B019 — Expand SQL semantics with explicit routing

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B011.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md).

**Deliverable:** SQL capability corpus, routing policy, and selected mutation/utility support.

**Acceptance criteria:**

1. Add only enumerated operations with fixtures for nulls, coercion, decimals, dates/timezones, collections, and error categories.
2. SQL update/delete/merge use the shared provider and match DataFrame-visible history/schema.
3. Any DuckDB route is selected before execution and has profile-specific evidence; mismatches stay partial/unsupported.

## B020 — Prove one CLI Terraform and bundle workflow

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B014.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Minimal versioned deployment fixtures and required API behavior.

**Acceptance criteria:**

1. Run the selected CLI workspace/job operations against a local server.
2. Apply/read/update/destroy the declared minimal Terraform resources with state/refresh semantics verified.
3. Validate/deploy/run one supported bundle fixture; label simulated infrastructure and reject unsupported resources explicitly.

## B021 — Implement and prove SQL connector REST profile

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B019.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md).

**Deliverable:** Pinned REST-mode SQL connector integration or a documented scope decision.

**Acceptance criteria:**

1. Connect the actual selected connector REST/kernel mode locally with verified TLS/auth configuration.
2. Exercise parameterized execute, fetching/schema, failure, result lifetime, and cancellation where advertised.
3. Record unsupported metadata/driver methods; if the released client cannot target the local service, leave implementation blocked or formally defer the release claim.

## B022 — Harden concurrent writes and crash recovery

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B018, B019.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** G3 failure matrix, reconciliation, backup/restore evidence.

**Acceptance criteria:**

1. Inject failures before/after Delta commit and SQLite finalization; verify recovery and uncertain-state handling.
2. Run concurrent SQL/DataFrame appends including location aliases; no lost updates or duplicated idempotent operations.
3. Exercise restart, safe retry, result/log retention, and coordinated backup/restore with actual files.

## B023 — Prepare alpha release candidate

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B020, B021, B022.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Local distribution artifacts, generated capability docs, examples and release report.

**Acceptance criteria:**

1. Install built wheels on every advertised platform/Python version and run G1–G4 for the declared surface.
2. Run base workflow without JVM/cloud/Docker; run provisioned offline check and advertised extras separately.
3. Confirm notices, migrations, dependency inventory, public Sparkless artifact, truthful limitations, and owner release checklist; publication is a separate authorized action.

## B024 — Expand workflow orchestration

**Milestone:** M4 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B018.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Decomposed epic for advanced DAG control, retries/repair, concurrency and schedules.

**Acceptance criteria:**

1. Specify and test each selected control-flow condition and retry/repair effect on task attempts/values.
2. Persist schedule/run metadata and prevent duplicate dispatch after restart for supported scheduling modes.
3. Keep unsupported task types explicit; do not run cloud/Java task types through silent substitutions.

## B025 — Integrate real MLflow tracking and registry

**Milestone:** M4 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B006, B007.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md).

**Deliverable:** Decomposed ML extra epic with provider lifecycle and registry mapping.

**Acceptance criteria:**

1. Log/retrieve actual experiments, metrics, artifacts and Python model versions through the declared clients.
2. Restart providers and verify authoritative state, artifact paths and name mappings.
3. Keep UC permissions/aliases/registry URI support bounded by fixtures and exclude JVM-dependent flavors.

## B026 — Add local Python model serving

**Milestone:** M4 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B025, B013.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md).

**Deliverable:** Decomposed serving endpoint/worker epic.

**Acceptance criteria:**

1. Load a recorded Python model version and return real schema-validated predictions through declared API routes.
2. Exercise lifecycle, update, load failure, request errors, concurrency limits and cancellation.
3. Do not silently call remote model endpoints; capture native-only runtime evidence.

## B027 — Integrate dbt without duplicate tables

**Milestone:** M4 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B019, B008.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md).

**Deliverable:** Decomposed dbt-duckdb profile and Delta materialization epic.

**Acceptance criteria:**

1. Run an actual sources/refs/models/tests project against the shared committed table view.
2. Implement supported incremental/materialization behavior through the authoritative service and test restart/schema changes.
3. Publish separate dbt-duckdb and dbt-databricks capability claims; unsupported macros fail explicitly.

## B028 — Add local authorization and audit semantics

**Milestone:** M4 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B007, B016.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Decomposed identity/grant/enforcement/audit epic.

**Acceptance criteria:**

1. Define identities/groups/ownership/inheritance and enforce the same decision across SQL/API/worker/file paths.
2. Verify permitted and denied operations with audit events and no unauthorized data results.
3. Document trusted-code filesystem bypass limits; row filters/masks require separate fixtures.

## B029 — Add native vector index workflows

**Milestone:** M4 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B008, B028.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md).

**Deliverable:** Decomposed LanceDB/provider and Databricks vector API epic.

**Acceptance criteria:**

1. Create/query/update/delete a persistent index with deterministic vectors and tested filter/score behavior.
2. Verify dimension errors, restart, authorization, and lifecycle transitions.
3. For Delta-sync support, prove source version tracking and recovery; no remote embeddings by default.

## B030 — Define and execute bounded micro-batches

**Milestone:** M5 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B010, B013.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md).

**Deliverable:** Decomposed streaming ports, bounded source/plan/sink epic.

**Acceptance criteria:**

1. Specify source identity/offset descriptor, batch ID, schema and supported output modes.
2. Execute a real bounded Sparkless batch into the table service with progress and cancellation.
3. Reject unsupported trigger/operator combinations and publish no unproven exactly-once claim.

## B031 — Implement durable checkpoints and replay

**Milestone:** M5 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B030, B022.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md).

**Deliverable:** Decomposed checkpoint, lease and replay epic.

**Acceptance criteria:**

1. Persist offsets only after sink commit and reconcile a committed batch on restart.
2. Fault-inject every batch phase; the advertised source/sink guarantee holds without duplicate rows.
3. Reject competing owners, changed source identity and incompatible checkpoint/plan versions.

## B032 — Add file ingestion and Auto Loader subset

**Milestone:** M5 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B031.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md).

**Deliverable:** Decomposed file discovery/schema registry/cloudFiles epic.

**Acceptance criteria:**

1. Ingest actual local files with durable discovery identity and restart-safe batch assignment.
2. Test file replacement, partial writes, duplicates, schema changes, bad records and backfill.
3. Declare supported cloudFiles options and distinguish polling from cloud notification mode.

## B033 — Implement declarative batch pipeline graph

**Milestone:** M5 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B018, B019.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md).

**Deliverable:** Decomposed dataset definitions, dependency graph and refresh epic.

**Acceptance criteria:**

1. Compile selected decorators into a validated dependency graph and materialize real Delta outputs.
2. Test cycles, dataset failures, refresh and restart with per-dataset version/run status.
3. Keep legacy dlt and newer pipelines imports distinct from the dltHub package; no fake whole-pipeline atomicity.

## B034 — Add expectations and selected CDC semantics

**Milestone:** M5 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B033, B031.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md).

**Deliverable:** Decomposed expectation/CDC/SCD fixtures and implementation epic.

**Acceptance criteria:**

1. Record/drop/fail expectations produce correct data, metrics and run outcomes.
2. Test keys, sequence ties, deletes, null updates, out-of-order records and declared SCD behavior.
3. Replay and partial pipeline failure preserve the documented per-table guarantees.

## B035 — Evaluate and integrate stateful streaming

**Milestone:** M5 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B031.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md).

**Deliverable:** Decomposed native provider selection, windows/state and optional broker epic.

**Acceptance criteria:**

1. Compare provider state/offset semantics against chosen Spark fixtures before committing to a dependency.
2. Implement selected windows/watermarks/dedup or joins with state eviction and restart fixtures.
3. Run G5 and advertise only tested operator/source/sink combinations; optional services remain explicit.

## B036 — Implement native Spark Connect subset

**Milestone:** M6 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B010, B019, B005.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md).

**Deliverable:** Decomposed gRPC/protobuf planning, execution and pure Python client epic.

**Acceptance criteria:**

1. Pin protocol/client versions and map supported unresolved plans to Sparkless with typed Arrow results.
2. Exercise actual unmodified pure Python client sessions, errors, cancellation and resource cleanup.
3. Prove no Spark server/JVM and isolate unsupported plan/UDF/session features; consider wheel split only after boundary criteria.

## B037 — Implement broader SQL driver protocols

**Milestone:** M6 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B019, B005.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md).

**Deliverable:** Decomposed Thrift/metadata/session/result transport epic.

**Acceptance criteria:**

1. Pin actual driver protocol requirements and implement a minimal real connector workflow.
2. Test sessions, parameterization, result pagination/schema, metadata calls and cancellation for advertised methods.
3. Keep REST and Thrift claims separate; JDBC clients are external conformance tools, never a JVM runtime dependency.

## B038 — Research native feature engineering and MLlib coverage

**Milestone:** M6 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B025, B008.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md).

**Deliverable:** Decomposed feasibility/fixture epic for temporal features and selected estimators.

**Acceptance criteria:**

1. Specify point-in-time joins, lineage/lookup and candidate estimator behavior independently.
2. Compare native results/parameters/serialization against versioned oracles for the chosen subset.
3. Record infeasible or incompatible behavior rather than exporting misleading class-name aliases.

## B039 — Add Delta Sharing protocol subset

**Milestone:** M6 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B008, B028.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md).

**Deliverable:** Decomposed sharing server/client and access lifecycle epic.

**Acceptance criteria:**

1. Pin the protocol and demonstrate a real independent client reading an authorized shared snapshot.
2. Validate schema/version/file result lifetime, revocation and unsupported Delta features.
3. Prevent arbitrary filesystem disclosure and preserve no-JVM server execution.

## B040 — Inventory and close remaining platform gaps

**Milestone:** M6 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B005, B007.

**Plans:** [00-vision-and-scope.md](00-vision-and-scope.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [13-decisions-and-risks.md](13-decisions-and-risks.md).

**Deliverable:** Decomposed platform frontier for dashboards/Genie/accounts/Lakebase/other domains.

**Acceptance criteria:**

1. Inventory public observable contracts by product/version and classify native feasibility.
2. For each selected slice, define a real workflow, authority and oracle before implementing.
3. Keep absent proprietary/cloud/Scala/JAR behavior visibly unsupported; simulated infrastructure is labeled.

## B041 — Expand performance, platforms and storage adapters

**Milestone:** M6 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B023.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Decomposed benchmarks/wheel/platform/scaling and optional cloud-storage adapter epic.

**Acceptance criteria:**

1. Record repeatable cold/warm latency, memory, conversion and ETL baselines with data/hardware/version metadata.
2. Add platforms only after native wheel and golden workflow evidence.
3. Improve bottlenecks without changing declared semantics; no unsupported acceleration claims.
4. Prove advertised Python and native storage paths against pinned optional Moto/Azurite services; record URI/API limits and do not infer ADLS Gen2 compatibility.

## B042 — Prepare expanded conformance release

**Milestone:** M6 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B023, B024, B025, B026, B027, B028, B029, B032, B034, B035, B036, B037, B038, B039, B040, B041.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Decomposed broad release readiness epic, scope record and versioned evidence snapshot.

**Acceptance criteria:**

1. Run applicable G1–G6 and report supported/partial/simulated/unsupported cases by domain/profile.
2. Verify migrations, installation, native-only execution, notices, examples and optional-provider version combinations.
3. Retain unresolved frontier requirements; any narrower release explicitly revises scope and leaves deferred tasks open.
