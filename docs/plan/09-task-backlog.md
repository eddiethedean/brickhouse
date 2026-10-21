# 09 — Implementation backlog

This document mirrors `planning/tasks.json`: the JSON ledger is canonical for task definitions and current state. Keep this readable view synchronized when task definitions change. Initial-state labels on original entries describe the planning baseline; current state and evidence live in the ledger.

## Task execution rules

Complete dependencies first. A research task can finish with a documented negative result if its acceptance criteria ask for a decision; an implementation task cannot finish without the promised behavior. `deferred` dependencies do not satisfy `done`. Revise release scope explicitly when needed.

B001–B023 are initial work packages. Split any that cannot be implemented and reviewed coherently into children before starting; parent completion requires every required child and original acceptance criterion. B024–B042 are epics and must be decomposed first. Assign new child IDs such as `B008.1`, preserving the parent requirements and dependency graph. Child tasks inherit the parent prerequisites, not a dependency on the parent itself.

Each completed task records commands/checks, exact versions, result summaries, relevant fixture/report paths, and commit references when available. Source research alone is not evidence that BrickHouse behavior works.

Phase 0.1 parents B001–B023 now have required child tasks listed below and in the ledger. Child entries identify their parent and the one-based original acceptance criteria they cover; all children inherit their parent's prerequisites and never depend on the parent itself. Complete all children and original parent integration checks before marking a parent done. Start implementation at B001.1. See [the phase 0.1 contract](16-phase-0.1-initial-release.md) for checkpoint and release evidence.

The 2026-10-20 compute expansion adds planning task B043 and five bounded implementation children (B011.5, B014.4, B014.5, B017.4, B020.4), bringing required phase 0.1 children to 73 at that decision. Appended criteria preserve all prior requirements. [Document 17](17-serverless-and-classic-compute.md) maps serverless/classic behavior, warehouse types and later environment/access/protocol work to their acceptance owners.

The subsequent delivery refinement adds B044 (planning), B045 (early connector transport research) and B046 (Python provider binding, with three required children). B011.6, B014.6, B017.5 and B020.5 separate workload verification from resource/context/client setup. The current ledger has 85 required child tasks after B014.3 was split into DAG scheduling, retry/concurrency admission and restart recovery; original parent criteria and prerequisites remain required. [ADR 0017](../decisions/0017-phase-0.1-delivery-refinement.md) records the earlier provider/consumer split.

The emulator remains primary through phases 0.1–0.9. B047 records the long-term plan and B048 adds a bounded local architecture review before B023. B049/SH.0 assesses stability after B042; B050–B055 are future deployment epics, strictly after that gate and requiring decomposition before implementation. See [document 18](18-self-hosted-platform.md) and [ADR 0034](../decisions/0034-emulator-first-self-hosted-platform.md). Existing emulator task states, acceptance criteria and evidence remain authoritative.

## B001 — Create installable project skeleton

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Dependencies:** None.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** pyproject.toml; src/brickhouse; CLI help/version; development and test configuration.

**Acceptance criteria:**

1. Build wheel and sdist and install the wheel into a clean environment.
2. Import brickhouse and invoke CLI help/version without starting a server or importing optional integrations.
3. Record initial dependency and supported-platform candidates; preserve planning documents.

**Required children:** B001.1, B001.2, B001.3. All children and the original criteria above are required.

## B002 — Audit Minilake reuse and provenance

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Dependencies:** B001.

**Plans:** [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [14-research-sources.md](14-research-sources.md).

**Deliverable:** docs/upstream/minilake-inventory.md and a reuse ADR; no wholesale app mounting.

**Acceptance criteria:**

1. Inspect the pinned source and record each proposed file/contract, license, globals, persistence, and Spark dependencies.
2. Choose adaptation versus fresh implementation for the first catalog/jobs/files routes and identify required removals.
3. Specify notices and tests before copying source; record a rationale if no implementation code is reused.

**Required children:** B002.1, B002.2. All children and the original criteria above are required.

## B003 — Prove Sparkless provider extension design

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Dependencies:** B001.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md).

**Deliverable:** Minimal engine integration probe, proposed public contracts, and coordinated upstream task list.

**Acceptance criteria:**

1. Read current Rust/Python APIs and identify what already exists versus missing S001–S006 hooks.
2. Demonstrate a viable path for external relation binding, writer delegation, typed interchange, and isolated sessions; a temporary probe is clearly separated from production code.
3. Record callback ownership, GIL/thread/reentrancy risks, failure propagation, and the smallest upstream change required.

**Required children:** B003.1, B003.2, B003.3. All children and the original criteria above are required.

## B004 — Select native Delta provider and feature envelope

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Dependencies:** B001.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md).

**Deliverable:** Delta-provider ADR, pinned dependency matrix, and interoperability fixtures.

**Acceptance criteria:**

1. Exercise real create/read/append/update/delete/merge with candidate native providers; record unsupported operations explicitly.
2. Cross-read supported types and versions through Sparkless and selected SQL path, including nulls/decimals/timestamps.
3. Probe transaction identity and crash-after-commit reconciliation; choose one mutation provider and document protocol/feature rejection rules.

**Required children:** B004.1, B004.2, B004.3. All children and the original criteria above are required.

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

**Required children:** B005.1, B005.2, B005.3, B005.4, B005.5. All children and the original criteria above are required.

## B006 — Implement workspace lifecycle and persistence

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B001.

**Plans:** [01-architecture.md](01-architecture.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Workspace/config services, SQLite migrations, lifecycle CLI and embedded context.

**Acceptance criteria:**

1. Initialize an isolated workspace with explicit default namespace policy and reopen it after restart.
2. Enforce one service owner, validate config/workspace versions, and reject incompatible formats.
3. Two different workspace directories have independent state and clean shutdown.

**Required children:** B006.1, B006.2, B006.3. All children and the original criteria above are required.

## B007 — Implement catalog identity and namespace services

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Current ledger status:** done.

**Dependencies:** B002, B005, B006.

**Plans:** [01-architecture.md](01-architecture.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md).

**Deliverable:** Instance-scoped catalog repositories/services and initial SDK API routes.

**Acceptance criteria:**

1. Create/list/get/delete catalogs and schemas using the pinned SDK, including pagination/error fixtures where advertised.
2. Resolve quoted/multipart identifiers and per-session defaults without catalog-prefix stripping.
3. Persist stable table registrations and managed/external ownership; reject conflicting canonical locations.

**Required children:** B007.1, B007.2, B007.3. All children and the original criteria above are required.

## B008 — Implement native Delta table service

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B004, B007.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Table-service port, native provider, operation journal, staging and commit results.

**Acceptance criteria:**

1. Create/append/overwrite a real local Delta table through the service with typed results and schema checks.
2. Detect unsupported table features before any read/write and reject invalid mode/schema combinations.
3. Record operation fingerprints and commit versions; recover known completed operations and surface uncertain commits without blind retry.
4. Drop external metadata without deleting files; managed deletion cannot escape the owned root.

**Required children:** B008.1, B008.2, B008.3, B008.4. All children and the original criteria above are required.

## B009 — Deliver generic Sparkless integration hooks

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B003.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md).

**Deliverable:** Upstream Sparkless changes and tested commit/artifact reference for required S001–S006 subset.

**Acceptance criteria:**

1. Implement reusable session-scoped catalog/relation/write hooks without BrickHouse/FastAPI/SQLite dependencies in Sparkless.
2. Run focused upstream tests for type preservation, delegation, errors, and session isolation under its repository instructions.
3. Document public hook signatures and artifact availability; dependent integration uses an actual tested artifact.

**Required children:** B009.1, B009.2, B009.3. All children and the original criteria above are required.

## B010 — Bind Sparkless sessions to BrickHouse tables

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B008, B009, B043, B046.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Sparkless engine adapter, session factory, and table binding.

**Acceptance criteria:**

1. Explicit Python API writes a managed table then reads it by full name in a fresh session.
2. DataFrame writes and supported path saves route through the common table service; unsupported bypasses fail.
3. Verify typed interchange and independent namespaces/temp views/config/UDF state for two concurrent sessions.
4. Apply the selected serverless/classic compute and runtime profile to isolated namespace/config/UDF/view state; verify defaults and reject unimplemented mode-specific semantics without claiming Spark Connect parity.

**Required children:** B010.1, B010.2, B010.3. All children and the original criteria above are required.

## B011 — Implement initial Statement Execution path

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B010, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** AST parsing/binding, query and DDL routing, statement lifecycle/results API.

**Acceptance criteria:**

1. Pinned SDK executes supported SELECT/DDL over the same registered Delta tables and receives correct schema/row encoding.
2. Bound parameters, error responses, timeouts, polling, cancellation, and bounded JSON results have explicit tested behavior.
3. Reject unsupported syntax/formats before side effects; no exception-driven engine fallback.
4. Resolve serverless/pro/classic local warehouse identity and type flags before execution; enforce start/stop/admission/cancellation and return real shared-table results, rejecting invalid resources and unsupported settings.

**Required children:** B011.1, B011.2, B011.3, B011.4, B011.5, B011.6. All children and the original criteria above are required.

## B012 — Prove shared-data and restart workflow

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B011.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md).

**Deliverable:** G1 conformance fixture and report.

**Acceptance criteria:**

1. Write via Sparkless, query via SDK SQL, read in a fresh session, restart the service, and obtain the same committed rows/schema.
2. Run without Java/full PySpark/Docker or cloud network access; capture process/dependency evidence.
3. Exercise empty data, schema mismatch, external table drop, and stale metadata invalidation.

**Required children:** B012.1, B012.2. All children and the original criteria above are required.

## B013 — Implement process supervisor and worker protocol

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B006.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [ADR 0019](../decisions/0019-worker-private-authentication.md), [ADR 0020](../decisions/0020-worker-supervision-and-outcomes.md).

**Deliverable:** Fresh-process executor, scoped private protocol, logs and cancellation.

**Acceptance criteria:**

1. Launch the configured Python environment by resolved executable with explicit cwd/context and bounded logs.
2. Cancel and time out a worker including descendant processes; persist actual exit status and clean staging.
3. Prevent session/import/context leakage between attempts and deny invalid worker credentials.

**Required children:** B013.1, B013.2, B013.3. All children and the original criteria above are required.

**Evidence:** B013.1–B013.3 pass their focused real-process suites. Combined evidence covers resolved interpreter/context and bounded output, scoped bearer validation/redaction, timeout/cancellation and descendant cleanup, actual terminal exit persistence, safe staging cleanup, and separate attempt process state. See [B013.3 conformance](../../artifacts/conformance/B013.3-worker-supervision.md). Public job/run routes and restart reconciliation remain B014.

## B014 — Implement durable jobs and workspace files

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B002, B005, B013, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Workspace file transport, job/run/task state machine and SDK routes.

**Acceptance criteria:**

1. Use real SDK upload/submit/poll/output against a Python file with parameters and captured stdout.
2. Persist transitions and task attempts; reject invalid DAGs and propagate failed dependency state.
3. Restart during an active run produces a documented interrupted/recoverable outcome; retries get distinct attempts.
4. Execute the bounded serverless and classic new/existing/shared-compute Python job corpus with immutable resolved context, mapped environments/runtime/access modes, accurate resource lifetimes and pre-dispatch errors; preserve fresh attempt isolation and report unsupported shared driver state.

**Required children:** B014.1, B014.2, B014.3, B014.4, B014.5, B014.6. All children and the original criteria above are required.

**Evidence:** Combined child evidence now covers SDK file upload and Python job execution/output, durable DAG/retry/concurrency/restart state, and all seven selected worker/compute variants. The full 138-test local unit suite passed with Ruff check/format, strict mypy, planning validation and `git diff --check`. See [B014.6 conformance](../../artifacts/conformance/B014.6-job-worker-compute-lifetimes.md) for the final worker/resource integration gate and its limits.

## B015 — Implement managed-worker compatibility bootstrap

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B010, B013, B043.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md).

**Deliverable:** Worker-scoped supported pyspark/delta import facade.

**Acceptance criteria:**

1. Run a representative script using supported pyspark imports against Sparkless and the shared table service.
2. Parent process and other environments retain their own import modules; conflicting/unsupported imports fail clearly.
3. Supported DeltaTable facade methods use the selected native provider; no delta-spark or JVM launch path.
4. Apply compute-profile-specific Python API and configuration gates inside the worker; verify serverless restrictions and classic runtime/access-mode limits, including explicit BrickHouse errors for JVM/RDD functionality absent from the native implementation.

**Required children:** B015.1, B015.2. All children and the original criteria above are required.

## B016 — Implement scoped dbutils subset

**Milestone:** M2 · **Kind:** task · **Initial state:** done

**Dependencies:** B014.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md).

**Deliverable:** Files, widgets/parameters, secrets, task values, and output-context adapters.

**Acceptance criteria:**

1. Exercise actual fs and widget behavior with path boundaries, missing values, and task-scoped context.
2. Resolve authorized secrets without persisting them in manifests or returning fabricated SDK values; test missing/denied access.
3. Persist supported task values per run/attempt and enforce serialization/size rules; unsupported methods raise explicit errors.

**Required children:** B016.1, B016.2, B016.3. All children and the original criteria above are required.

**Evidence:** All three children are complete. The local loopback SDK corpus covers bounded DBFS/widget behavior, job-granted process-memory secrets, and attempt-scoped task values. The 165-test local suite passed; Ruff, formatting, mypy, planning validation, and diff checks passed. See [B016.1](../../artifacts/conformance/B016.1-worker-dbutils-files-widgets.md), [B016.2](../../artifacts/conformance/B016.2-process-memory-scoped-secrets.md), and [B016.3](../../artifacts/conformance/B016.3-attempt-scoped-task-values.md).

## B017 — Implement optional Python notebook runner

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B015, B016, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** nbformat/nbclient runner and explicit Databricks source/magic handling.

**Acceptance criteria:**

1. Execute ipynb and the declared Databricks source subset in fresh kernels with parameters, outputs, and error propagation.
2. Test %run shared-context behavior separately from child notebook execution and return values.
3. Reject unsupported magics/languages/package installation; cancel kernels and release resources reliably.
4. Execute the bounded serverless and classic notebook corpus with selected environment inheritance, isolated sessions, supported settings, all-purpose attachment/reset and compute termination effects; report unimplemented dependency/protocol behavior explicitly.

**Required children:** B017.1, B017.2, B017.3, B017.4, B017.5. All children and the original criteria above are required.

## B018 — Prove SDK-driven job workflow

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B012, B014, B015, B016, B017, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** G2 workflow and optional-notebook conformance report.

**Acceptance criteria:**

1. SDK submits a Python transformation that writes Delta; a dependent task and external SQL/session read observe the same result.
2. Exercise parameters, secrets, task output, dependency failure, cancellation, and retry without cross-run state leakage.
3. Run a separate notebook-extra workflow and the base workflow without notebook dependencies, Java, or cloud compute.
4. Run the synthetic orders workflow under the bounded serverless and classic new/existing/shared-compute profiles and each SQL warehouse type, with separate notebook-extra variants, asserting real schema/data/output, resource lifetime, isolation and restart behavior.

**Required children:** B018.1, B018.2. All children and the original criteria above are required.

## B019 — Expand SQL semantics with explicit routing

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B011, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** SQL capability corpus, routing policy, and selected mutation/utility support.

**Acceptance criteria:**

1. Add only enumerated operations with fixtures for nulls, coercion, decimals, dates/timezones, collections, and error categories.
2. SQL update/delete/merge use the shared provider and match DataFrame-visible history/schema.
3. Any DuckDB route is selected before execution and has profile-specific evidence; mismatches stay partial/unsupported.
4. Define separate serverless and classic runtime/access-mode cases for ANSI/timezone defaults, configuration scope, views/cache and error behavior; deferred-analysis/Connect cases remain explicitly unsupported until B036 proves them.

**Required children:** B019.1, B019.2, B019.3. All children and the original criteria above are required.

## B020 — Prove one CLI Terraform and bundle workflow

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B014, B043.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Minimal versioned deployment fixtures and required API behavior.

**Acceptance criteria:**

1. Run the selected CLI workspace/job operations against a local server.
2. Apply/read/update/destroy the declared minimal Terraform resources with state/refresh semantics verified.
3. Validate/deploy/run one supported bundle fixture; label simulated infrastructure and reject unsupported resources explicitly.
4. Qualify selected classic compute and serverless/pro/classic warehouse resources plus serverless/classic bundle variants with real pinned clients, actual worker/query results, lifecycle errors and cleanup; metadata-only success cannot satisfy workload compatibility.

**Required children:** B020.1, B020.2, B020.3, B020.4, B020.5. All children and the original criteria above are required.

## B021 — Implement and prove SQL connector REST profile

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B019, B045.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Pinned REST-mode SQL connector integration or a documented scope decision.

**Acceptance criteria:**

1. Connect the actual selected connector REST/kernel mode locally with verified TLS/auth configuration.
2. Exercise parameterized execute, fetching/schema, failure, result lifetime, and cancellation where advertised.
3. Record unsupported metadata/driver methods; if the released client cannot target the local service, leave implementation blocked or formally defer the release claim.

**Required children:** B021.1, B021.2. All children and the original criteria above are required.

## B022 — Harden concurrent writes and crash recovery

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B018, B019, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** G3 failure matrix, reconciliation, backup/restore evidence.

**Acceptance criteria:**

1. Inject failures before/after Delta commit and SQLite finalization; verify recovery and uncertain-state handling.
2. Run concurrent SQL/DataFrame appends including location aliases; no lost updates or duplicated idempotent operations.
3. Exercise restart, safe retry, result/log retention, and coordinated backup/restore with actual files.
4. Exercise cancellation, compute/session restart, interrupted writes and safe retries under both required compute modes; preserve committed Delta versions and accurate attempts while simulated infrastructure transitions never fabricate completion.

**Required children:** B022.1, B022.2, B022.3. All children and the original criteria above are required.

## B023 — Prepare alpha release candidate

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B020, B021, B022, B043, B044, B048.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Local distribution artifacts, generated capability docs, examples and release report.

**Acceptance criteria:**

1. Install built wheels on every advertised platform/Python version and run G1–G4 for the declared surface.
2. Run base workflow without JVM/cloud/Docker; run provisioned offline check and advertised extras separately.
3. Confirm notices, migrations, dependency inventory, public Sparkless artifact, truthful limitations, and owner release checklist; publication is a separate authorized action.
4. Include both bounded serverless/classic Python/notebook profiles and serverless/pro/classic warehouse execution in the 0.1.0 scope snapshot and G1-G4 evidence; keep mode/protocol/environment limits explicit and block release or formally revise scope when a required subset is unimplemented.

**Required children:** B023.1, B023.2, B023.3. All children and the original criteria above are required.

## B024 — Expand workflow orchestration

**Milestone:** M4 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B018, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed epic for advanced DAG control, retries/repair, concurrency and schedules.

**Acceptance criteria:**

1. Specify and test each selected control-flow condition and retry/repair effect on task attempts/values.
2. Persist schedule/run metadata and prevent duplicate dispatch after restart for supported scheduling modes.
3. Keep unsupported task types explicit; do not run cloud/Java task types through silent substitutions.
4. Decompose and implement selected serverless performance/admission and automatic optimization retry policies, compute-object selection, and locked environment provisioning/cache/reset including versioned classic dependency modes; verify real local attempts and reject unsupported settings without simulating successful work.

## B025 — Integrate real MLflow tracking and registry

**Milestone:** M4 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B006, B007.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed ML extra epic with provider lifecycle and registry mapping.

**Acceptance criteria:**

1. Log/retrieve actual experiments, metrics, artifacts and Python model versions through the declared clients.
2. Restart providers and verify authoritative state, artifact paths and name mappings.
3. Keep UC permissions/aliases/registry URI support bounded by fixtures and exclude JVM-dependent flavors.

## B026 — Add local Python model serving

**Milestone:** M4 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B025, B013.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed serving endpoint/worker epic.

**Acceptance criteria:**

1. Load a recorded Python model version and return real schema-validated predictions through declared API routes.
2. Exercise lifecycle, update, load failure, request errors, concurrency limits and cancellation.
3. Do not silently call remote model endpoints; capture native-only runtime evidence.

## B027 — Integrate dbt without duplicate tables

**Milestone:** M4 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B019, B008.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed dbt-duckdb profile and Delta materialization epic.

**Acceptance criteria:**

1. Run an actual sources/refs/models/tests project against the shared committed table view.
2. Implement supported incremental/materialization behavior through the authoritative service and test restart/schema changes.
3. Publish separate dbt-duckdb and dbt-databricks capability claims; unsupported macros fail explicitly.

## B028 — Add local authorization and audit semantics

**Milestone:** M4 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B007, B016, B043.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed identity/grant/enforcement/audit epic.

**Acceptance criteria:**

1. Define identities/groups/ownership/inheritance and enforce the same decision across SQL/API/worker/file paths.
2. Verify permitted and denied operations with audit events and no unauthorized data results.
3. Document trusted-code filesystem bypass limits; row filters/masks require separate fixtures.
4. Decompose and enforce classic standard/dedicated compute use and distinct interactive/automated serverless compute-object permissions separately from SQL warehouse ACLs; prove allowed/denied jobs/notebooks/SQL paths with the same identities and audit authority.

## B029 — Add native vector index workflows

**Milestone:** M4 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B008, B028.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed LanceDB/provider and Databricks vector API epic.

**Acceptance criteria:**

1. Create/query/update/delete a persistent index with deterministic vectors and tested filter/score behavior.
2. Verify dimension errors, restart, authorization, and lifecycle transitions.
3. For Delta-sync support, prove source version tracking and recovery; no remote embeddings by default.

## B030 — Define and execute bounded micro-batches

**Milestone:** M5 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B010, B013, B043.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed streaming ports, bounded source/plan/sink epic.

**Acceptance criteria:**

1. Specify source identity/offset descriptor, batch ID, schema and supported output modes.
2. Execute a real bounded Sparkless batch into the table service with progress and cancellation.
3. Reject unsupported trigger/operator combinations and publish no unproven exactly-once claim.
4. Classify and test serverless bounded-trigger acceptance/rejection separately from classic trigger/runtime support; execute supported batches locally and retain explicit mode-specific errors for unsupported combinations.

## B031 — Implement durable checkpoints and replay

**Milestone:** M5 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B030, B022.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed checkpoint, lease and replay epic.

**Acceptance criteria:**

1. Persist offsets only after sink commit and reconcile a committed batch on restart.
2. Fault-inject every batch phase; the advertised source/sink guarantee holds without duplicate rows.
3. Reject competing owners, changed source identity and incompatible checkpoint/plan versions.

## B032 — Add file ingestion and Auto Loader subset

**Milestone:** M5 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B031.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed file discovery/schema registry/cloudFiles epic.

**Acceptance criteria:**

1. Ingest actual local files with durable discovery identity and restart-safe batch assignment.
2. Test file replacement, partial writes, duplicates, schema changes, bad records and backfill.
3. Declare supported cloudFiles options and distinguish polling from cloud notification mode.

## B033 — Implement declarative batch pipeline graph

**Milestone:** M5 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B018, B019, B043.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed dataset definitions, dependency graph and refresh epic.

**Acceptance criteria:**

1. Compile selected decorators into a validated dependency graph and materialize real Delta outputs.
2. Test cycles, dataset failures, refresh and restart with per-dataset version/run status.
3. Keep legacy dlt and newer pipelines imports distinct from the dltHub package; no fake whole-pipeline atomicity.
4. Resolve selected classic and serverless pipeline compute/environment settings through the shared services, apply the versioned mode restrictions, and verify real local batch outputs rather than accepting compute labels alone.

## B034 — Add expectations and selected CDC semantics

**Milestone:** M5 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B033, B031.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed expectation/CDC/SCD fixtures and implementation epic.

**Acceptance criteria:**

1. Record/drop/fail expectations produce correct data, metrics and run outcomes.
2. Test keys, sequence ties, deletes, null updates, out-of-order records and declared SCD behavior.
3. Replay and partial pipeline failure preserve the documented per-table guarantees.

## B035 — Evaluate and integrate stateful streaming

**Milestone:** M5 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B031.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed native provider selection, windows/state and optional broker epic.

**Acceptance criteria:**

1. Compare provider state/offset semantics against chosen Spark fixtures before committing to a dependency.
2. Implement selected windows/watermarks/dedup or joins with state eviction and restart fixtures.
3. Run G5 and advertise only tested operator/source/sink combinations; optional services remain explicit.

## B036 — Implement native Spark Connect subset

**Milestone:** M6 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B010, B019, B005, B043.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed gRPC/protobuf planning, execution and pure Python client epic.

**Acceptance criteria:**

1. Pin protocol/client versions and map supported unresolved plans to Sparkless with typed Arrow results.
2. Exercise actual unmodified pure Python client sessions, errors, cancellation and resource cleanup.
3. Prove no Spark server/JVM and isolate unsupported plan/UDF/session features; consider wheel split only after boundary criteria.
4. Prove reference-specific Spark Connect analysis/error timing, temporary-view rebinding and UDF capture with an unmodified pure Python client for serverless and applicable classic runtimes; keep compute mode independent of protocol and shared driver state.

## B037 — Implement broader SQL driver protocols

**Milestone:** M6 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B019, B005.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed Thrift/metadata/session/result transport epic.

**Acceptance criteria:**

1. Pin actual driver protocol requirements and implement a minimal real connector workflow.
2. Test sessions, parameterization, result pagination/schema, metadata calls and cancellation for advertised methods.
3. Keep REST and Thrift claims separate; JDBC clients are external conformance tools, never a JVM runtime dependency.

## B038 — Research native feature engineering and MLlib coverage

**Milestone:** M6 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B025, B008.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed feasibility/fixture epic for temporal features and selected estimators.

**Acceptance criteria:**

1. Specify point-in-time joins, lineage/lookup and candidate estimator behavior independently.
2. Compare native results/parameters/serialization against versioned oracles for the chosen subset.
3. Record infeasible or incompatible behavior rather than exporting misleading class-name aliases.

## B039 — Add Delta Sharing protocol subset

**Milestone:** M6 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B008, B028.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed sharing server/client and access lifecycle epic.

**Acceptance criteria:**

1. Pin the protocol and demonstrate a real independent client reading an authorized shared snapshot.
2. Validate schema/version/file result lifetime, revocation and unsupported Delta features.
3. Prevent arbitrary filesystem disclosure and preserve no-JVM server execution.

## B040 — Inventory and close remaining platform gaps

**Milestone:** M6 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B005, B007, B043.

**Plans:** [00-vision-and-scope.md](00-vision-and-scope.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [13-decisions-and-risks.md](13-decisions-and-risks.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed platform frontier for dashboards/Genie/accounts/Lakebase/other domains.

**Acceptance criteria:**

1. Inventory public observable contracts by product/version and classify native feasibility.
2. For each selected slice, define a real workflow, authority and oracle before implementing.
3. Keep absent proprietary/cloud/Scala/JAR behavior visibly unsupported; simulated infrastructure is labeled.
4. Retain explicit serverless/classic workload, warehouse, access-mode, runtime/environment and cloud/version gaps after the bounded earlier releases; do not use this frontier epic to defer their required 0.1 behavior silently.

## B041 — Expand performance, platforms and storage adapters

**Milestone:** M6 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B023.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed benchmarks/wheel/platform/scaling and optional cloud-storage adapter epic.

**Acceptance criteria:**

1. Record repeatable cold/warm latency, memory, conversion and ETL baselines with data/hardware/version metadata.
2. Add platforms only after native wheel and golden workflow evidence.
3. Improve bottlenecks without changing declared semantics; no unsupported acceleration claims.
4. Prove advertised Python and native storage paths against pinned optional Moto/Azurite services; record URI/API limits and do not infer ADLS Gen2 compatibility.

## B042 — Prepare expanded conformance release

**Milestone:** M6 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B023, B024, B025, B026, B027, B028, B029, B032, B034, B035, B036, B037, B038, B039, B040, B041, B043.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed broad release readiness epic, scope record and versioned evidence snapshot.

**Acceptance criteria:**

1. Run applicable G1–G6 and report supported/partial/simulated/unsupported cases by domain/profile.
2. Verify migrations, installation, native-only execution, notices, examples and optional-provider version combinations.
3. Retain unresolved frontier requirements; any narrower release explicitly revises scope and leaves deferred tasks open.
4. Consolidate separate serverless/classic workload and three-warehouse-type case counts, version combinations, simulation labels and unresolved protocol/environment/authorization gaps in the release snapshot.

## B001.1 — Create the package and lazy CLI entry point

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B001 · **Parent criteria:** 2, 3.

**Dependencies:** None.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** src/brickhouse, build metadata, and help/version entry points.

**Acceptance criteria:**

1. Create the single-distribution src layout and CLI help/version without importing optional integrations or starting services; preserve all planning documents.

## B001.2 — Build and install distribution artifacts

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B001 · **Parent criteria:** 1, 2.

**Dependencies:** B001.1.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Wheel/sdist and clean-environment import/CLI evidence.

**Acceptance criteria:**

1. Build wheel and sdist, install the wheel into a fresh environment, and record import/help/version results without editable-checkout assumptions.

## B001.3 — Establish development and planning validation tooling

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B001 · **Parent criteria:** 3.

**Dependencies:** B001.2.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Development configuration, manifest checks, and dependency/platform baseline.

**Acceptance criteria:**

1. Record initial dependency/Python/platform candidates and artifact retention policy; provide focused development checks and task/capability/link validation, including child completion rules.

## B002.1 — Inventory pinned Minilake source and coupling

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B002 · **Parent criteria:** 1.

**Dependencies:** B001.

**Plans:** [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Pinned file/license/state/Spark dependency inventory.

**Acceptance criteria:**

1. Inspect the exact pinned source and record proposed contracts/files, licenses, globals, persistence ownership, and Spark launch/write paths.

## B002.2 — Decide reuse boundaries and provenance requirements

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B002 · **Parent criteria:** 2, 3.

**Dependencies:** B001, B002.1.

**Plans:** [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Reuse ADR, owned replacements, notice plan, and contract-test inventory.

**Acceptance criteria:**

1. Choose adaptation or fresh implementation for initial catalog/jobs/files behavior and specify removed paths, required tests, and notices before copying source; justify any no-reuse decision.

## B003.1 — Inspect actual Sparkless extension APIs

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B003 · **Parent criteria:** 1.

**Dependencies:** B001.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Current Rust/Python API inventory mapped to S001-S006.

**Acceptance criteria:**

1. Read the actual engine and bindings, distinguish public existing APIs from missing hooks, and record the inspected source revision.

## B003.2 — Probe external relations writers and typed interchange

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B003 · **Parent criteria:** 2.

**Dependencies:** B001, B003.1.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Runnable isolated feasibility probes for provider binding/write/Arrow contracts.

**Acceptance criteria:**

1. Demonstrate viable catalog/relation binding, delegated writes, and typed interchange using real native data; label temporary adaptations and do not treat proposed signatures as released APIs.

## B003.3 — Resolve session and callback delivery risks

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B003 · **Parent criteria:** 2, 3.

**Dependencies:** B001, B003.2.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Session-isolation probe, callback contract, and smallest upstream task proposal.

**Acceptance criteria:**

1. Exercise independent sessions and record callback ownership, GIL/thread/reentrancy, cancellation/error propagation, and the smallest required upstream change or precise blocker.

## B004.1 — Probe native Delta mutation candidates

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B004 · **Parent criteria:** 1.

**Dependencies:** B001.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Exact-version candidate matrix with real Delta mutation fixtures.

**Acceptance criteria:**

1. Run create/read/append/update/delete/merge on actual files for candidate providers and record unsupported operations rather than fabricating success.

## B004.2 — Cross-read the selected type and protocol envelope

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B004 · **Parent criteria:** 2.

**Dependencies:** B001, B004.1.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Sparkless/SQL cross-read fixtures and feature rejection matrix.

**Acceptance criteria:**

1. Cross-read committed snapshots through Sparkless and the selected SQL path, covering nulls, decimals, timestamps, and the declared scalar/nested subset with exact provider versions.

## B004.3 — Select mutation provider and recovery identity

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B004 · **Parent criteria:** 3.

**Dependencies:** B001, B004.2.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Provider ADR, transaction-identity probe, and recovery policy.

**Acceptance criteria:**

1. Probe crash-after-commit reconciliation and transaction identity; select one mutation provider, protocol/feature limits, and explicit uncertain-outcome behavior where identity cannot be reconciled.

## B005.1 — Pin clients semantic profiles and transport requirements

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B005 · **Parent criteria:** 1, 2, 3.

**Dependencies:** B001.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [15-personal-databricks-verification.md](15-personal-databricks-verification.md), [0001-personal-databricks-reference-verification.md](../decisions/0001-personal-databricks-reference-verification.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Exact client/profile matrix and minimum SDK/deployment/connector workflows.

**Acceptance criteria:**

1. Inspect actual released SDK/CLI/Terraform/SQL connector artifacts, pin versions, separate REST/kernel and default Thrift requirements, and enumerate the minimal workflows and fixture provenance.

## B005.2 — Configure bounded personal-account reference scope

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B005 · **Parent criteria:** 3, 4.

**Dependencies:** B001, B005.1.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [15-personal-databricks-verification.md](15-personal-databricks-verification.md), [0001-personal-databricks-reference-verification.md](../decisions/0001-personal-databricks-reference-verification.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Sanitized capture profile with host alias, observed features, and resource limits.

**Acceptance criteria:**

1. Record the actual personal workspace feature/version scope, permitted existing compute, scratch ownership, limits, cleanup, and credential-variable sources without retaining credentials or assuming a budget.

## B005.3 — Capture and sanitize a real reference behavior

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B005 · **Parent criteria:** 5.

**Dependencies:** B001, B005.2.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [15-personal-databricks-verification.md](15-personal-databricks-verification.md), [0001-personal-databricks-reference-verification.md](../decisions/0001-personal-databricks-reference-verification.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Named probe tooling, reviewed synthetic fixture, and local test design.

**Acceptance criteria:**

1. Execute at least one small synthetic reference operation outside test discovery; retain exact schema/results or errors, client/session provenance, sanitization rules, and cleanup evidence, then map it to a deterministic local regression design.

## B005.4 — Guard local fixtures and clients against remote authentication

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B005 · **Parent criteria:** 7.

**Dependencies:** B001, B005.1.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [15-personal-databricks-verification.md](15-personal-databricks-verification.md), [0001-personal-databricks-reference-verification.md](../decisions/0001-personal-databricks-reference-verification.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Credential-free fixture checks and local-client host/auth isolation checks.

**Acceptance criteria:**

1. Prove fixture tooling runs without personal credentials, ordinary clients do not load .env or discover real-workspace profiles, and inherited credentials cannot redirect local calls; explicit capture fails clearly when configuration is absent.

## B005.5 — Add the optional manual reference-capture workflow

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Parent:** B005 · **Parent criteria:** 6.

**Dependencies:** B001, B005.3, B005.4.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [15-personal-databricks-verification.md](15-personal-databricks-verification.md), [0001-personal-databricks-reference-verification.md](../decisions/0001-personal-databricks-reference-verification.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Restricted workflow_dispatch workflow and sanitized artifact handling.

**Acceptance criteria:**

1. Restrict capture to reviewed default-branch code, curated probes, permitted resources, job-scoped credentials, serialized bounded execution, and sanitized uploads; exclude automatic PR/push/schedule events and required CI dependence.

## B006.1 — Define workspace configuration namespaces and schema

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Parent:** B006 · **Parent criteria:** 1, 2.

**Dependencies:** B001.

**Plans:** [01-architecture.md](01-architecture.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [ADR 0005](../decisions/0005-workspace-bootstrap-format.md).

**Deliverable:** Typed configuration, workspace format, migrations, and namespace policy.

**Acceptance criteria:**

1. Initialize and reopen a real workspace with explicit bootstrap namespaces and supported config/format versions; reject invalid configuration and incompatible formats.

## B006.2 — Enforce service ownership and independent persistence

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Parent:** B006 · **Parent criteria:** 2, 3.

**Dependencies:** B001, B006.1.

**Plans:** [01-architecture.md](01-architecture.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [ADR 0006](../decisions/0006-workspace-ownership-and-migrations.md).

**Deliverable:** Workspace owner lock, transactional SQLite migration runner, and shutdown/reopen evidence.

**Acceptance criteria:**

1. Enforce one service owner including stale-owner recovery, isolate two workspace directories, and verify transactional migrations and deterministic shutdown.

## B006.3 — Expose equivalent CLI and embedded lifecycle

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Parent:** B006 · **Parent criteria:** 1, 3.

**Dependencies:** B001, B006.2.

**Plans:** [01-architecture.md](01-architecture.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** init/serve/run lifecycle composition and embedded Workspace context.

**Acceptance criteria:**

1. Compose CLI and embedded lifecycle through the same instance-scoped services with clean shutdown; expose truthful capability/version/provider diagnostics without marking planned behavior supported.

## B007.1 — Persist stable catalog and table identities

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Current ledger status:** done.

**Parent:** B007 · **Parent criteria:** 3.

**Dependencies:** B002, B005, B006.

**Plans:** [01-architecture.md](01-architecture.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Catalog/schema/table repositories with canonical ownership constraints.

**Acceptance criteria:**

1. Persist stable IDs, canonical table locations, and managed/external ownership; reject aliases with conflicting registrations and preserve identities across reopen.

## B007.2 — Bind identifiers and isolated namespace defaults

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Current ledger status:** done.

**Parent:** B007 · **Parent criteria:** 2.

**Dependencies:** B002, B005, B006, B007.1.

**Plans:** [01-architecture.md](01-architecture.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Identifier parser/resolver and per-session namespace contract.

**Acceptance criteria:**

1. Resolve quoted and multipart identifiers and session defaults without prefix stripping or cross-session namespace leakage; exercise missing and ambiguous targets.

## B007.3 — Implement SDK catalog and schema workflows

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Current ledger status:** done.

**Parent:** B007 · **Parent criteria:** 1.

**Dependencies:** B002, B005, B006, B007.2.

**Plans:** [01-architecture.md](01-architecture.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Catalog/schema service operations and pinned SDK transport checks.

**Acceptance criteria:**

1. Use the real pinned SDK for create/list/get/delete catalogs and schemas, with actual persistence and selected pagination/duplicate/missing/error fixtures.

## B008.1 — Define table handles snapshots and staging

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Current ledger status:** done.

**Parent:** B008 · **Parent criteria:** 1, 2.

**Dependencies:** B004, B007.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Table-service port, typed handles/snapshots, and owned staging.

**Acceptance criteria:**

1. Define real table identity/location/schema/version results, staged inputs, snapshot selection, and provider feature/mode/schema validation before access.

## B008.2 — Commit create append and overwrite through one provider

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B008 · **Parent criteria:** 1, 2.

**Dependencies:** B004, B007, B008.1.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Native create/append/overwrite implementation and real Delta evidence.

**Acceptance criteria:**

1. Create/read/append/overwrite actual local Delta tables through the selected provider; preserve schema and commit versions, including empty input, invalid modes, and incompatible schemas.

## B008.3 — Journal mutation identity and reconcile outcomes

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B008 · **Parent criteria:** 3.

**Dependencies:** B004, B007, B008.2.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Operation fingerprints, durable journal transitions, and restart recovery.

**Acceptance criteria:**

1. Persist operation IDs and commit versions, reconcile known completed operations, reject changed-payload key reuse, and surface uncertain outcomes without automatic blind retry.

## B008.4 — Implement ownership-safe table removal

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B008 · **Parent criteria:** 4.

**Dependencies:** B004, B007, B008.3.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** External deregistration and managed table deletion lifecycle.

**Acceptance criteria:**

1. Drop external registrations without changing their files; verify canonical paths/symlinks cannot redirect managed cleanup outside the owned root and document interrupted cleanup recovery.

## B009.1 — Deliver generic external catalog and relation hooks

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B009 · **Parent criteria:** 1.

**Dependencies:** B003.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Reusable session-scoped Sparkless catalog/relation binding hooks.

**Acceptance criteria:**

1. Implement the smallest proven external lookup/read hooks under Sparkless repository instructions without BrickHouse/FastAPI/SQLite dependencies; exercise real provider resolution and errors.

## B009.2 — Deliver write delegation and typed interchange

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B009 · **Parent criteria:** 1, 2.

**Dependencies:** B003, B009.1.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Reusable Sparkless write and Arrow/native transfer contracts.

**Acceptance criteria:**

1. Implement delegated owned-target writes and typed interchange with focused upstream type-preservation, mode/option, delegation, and failure tests.

## B009.3 — Prove isolation and record the consumed artifact

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B009 · **Parent criteria:** 2, 3.

**Dependencies:** B003, B009.2.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Public hook documentation, isolation results, and actual artifact reference.

**Acceptance criteria:**

1. Run required upstream checks for session/callback/error isolation and document public signatures plus the tested commit/artifact and release-availability status; do not assume an unpublished artifact is available to users.

The tested contract is Rust-only. The current source has no PyO3/Python callback surface; callback GIL, reentrancy, cross-thread, and cancellation behavior remain unsupported and are recorded in [the isolation and availability report](../../artifacts/conformance/B009.3-sparkless-hook-isolation-and-availability.md).

## B010.1 — Compose isolated Sparkless sessions and table bindings

**Milestone:** M1 · **Kind:** task · **Current ledger status:** blocked

**Dependencies:** B008, B009, B043, B046.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [0018-published-sparkless-python-provider.md](../decisions/0018-published-sparkless-python-provider.md).

**Deliverable:** Engine adapter/session factory and typed snapshot reader.

**Acceptance criteria:**

1. Use the actual tested Sparkless artifact to bind shared table snapshots with the selected type envelope and explicit session configuration.

**Parent:** B010 · **Parent criteria:** 3.

## B010.2 — Route DataFrame writes through the table service

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B008, B009, B010.1, B043, B046.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Named-table/path writer delegation and cross-session read fixture.

**Acceptance criteria:**

1. Write a managed table through the explicit Python API, read it by full name in a fresh session, and prove supported path saves share the service while bypasses fail clearly.

**Parent:** B010 · **Parent criteria:** 1, 2.

## B010.3 — Verify independent concurrent session state

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B008, B009, B010.2, B043, B046.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Two-session namespace/config/view/UDF and interchange evidence.

**Acceptance criteria:**

1. Exercise two concurrent sessions with different namespaces, temporary views, config, and UDFs; assert no state leakage and preserved typed results.

**Parent:** B010 · **Parent criteria:** 3, 4.

## B011.1 — Parse bind and route the initial SQL corpus

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B010, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** AST classifier/binder and service/query routing.

**Acceptance criteria:**

1. Implement the selected SELECT/DDL cases over registered tables with bound identifiers; reject unsupported syntax before side effects and prohibit exception-driven executor fallback.

**Parent:** B011 · **Parent criteria:** 1, 3, 4.

## B011.2 — Expose statement lifecycle and typed JSON results

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B010, B011.1, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** SDK submit/poll/result transport with persisted lifecycle state.

**Acceptance criteria:**

1. Use the pinned SDK to submit and poll actual statements, returning the expected schema and JSON row encoding from the authoritative data.

**Parent:** B011 · **Parent criteria:** 1, 2.

## B011.3 — Implement parameter errors timeouts and cancellation

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B010, B011.2, B011.5, B043, B011.6.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Bound-parameter handling and explicit terminal/error semantics.

**Acceptance criteria:**

1. Exercise supported parameter forms, invalid requests, engine errors, wait/timeout behavior, and cancellation without interpolation or fabricated successful completion.

**Parent:** B011 · **Parent criteria:** 2, 3, 4.

## B011.4 — Bound statement results and retention

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B010, B011.3, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Documented row/byte/lifetime limits and result cleanup.

**Acceptance criteria:**

1. Choose recorded result limits and retention, test expiry/oversize behavior, and reject unsupported formats/dispositions without arbitrary filesystem reads or unbounded global result storage.

**Parent:** B011 · **Parent criteria:** 2, 3, 4.

## B012.1 — Prove shared table reads through restart

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Parent:** B012 · **Parent criteria:** 1.

**Dependencies:** B011.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Runnable G1 DataFrame/SDK/fresh-session/restart fixture.

**Acceptance criteria:**

1. Write through Sparkless, query through the real SDK, read in a fresh session, restart the server, and compare committed rows/schema and table identity.

## B012.2 — Prove G1 boundary and native-execution cases

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Parent:** B012 · **Parent criteria:** 2, 3.

**Dependencies:** B011, B012.1.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** G1 negative corpus and dependency/process/network report.

**Acceptance criteria:**

1. Exercise empty data, schema mismatch, external drop, and stale metadata invalidation while proving no Java/full PySpark/Docker or cloud access during the local workflow.

## B013.1 — Launch real workers with explicit immutable context

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B013 · **Parent criteria:** 1.

**Dependencies:** B006.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Resolved-executable child runner and bounded output capture.

**Acceptance criteria:**

1. Launch the configured environment interpreter with explicit cwd/context and recorded log/output limits; capture actual process results without shell/PATH assumptions.

## B013.2 — Authenticate scoped worker service calls

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B013 · **Parent criteria:** 3.

**Dependencies:** B006, B013.1.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0019-worker-private-authentication.md](../decisions/0019-worker-private-authentication.md).

**Deliverable:** Versioned private worker messages and run-scoped credential lifecycle.

**Acceptance criteria:**

1. Validate worker tokens and protocol versions, reject invalid/expired scope, and verify independent attempt contexts without disclosing credentials in logs or URLs.

## B013.3 — Terminate descendants and persist cancellation outcomes

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B013 · **Parent criteria:** 2, 3.

**Dependencies:** B006, B013.2.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [ADR 0020](../decisions/0020-worker-supervision-and-outcomes.md).

**Deliverable:** Timeout/cancellation escalation, process reaping, and staging cleanup.

**Acceptance criteria:**

1. Exercise crash, timeout, and cancellation against real child/descendant processes; persist actual exit outcomes, clean safe staging, and prove no attempt/session/import leakage.

**Evidence:** 70 focused worker, workspace-migration, and local-table checks passed, including real timeout/cancellation, POSIX descendant reaping, crash status persistence, staging cleanup, and two-attempt environment/import/context isolation. See [B013.3 conformance](../../artifacts/conformance/B013.3-worker-supervision.md).

## B014.1 — Persist jobs submissions runs and attempts

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B002, B005, B013, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [0022-durable-job-state.md](../decisions/0022-durable-job-state.md).

**Deliverable:** Job/run/task schema and validated durable state transitions.

**Acceptance criteria:**

1. Persist immutable submitted settings, submission tokens, task attempts, and allowed transitions; reject invalid DAGs and preserve accurate terminal/interrupted state.

**Evidence:** Schema 6 persists immutable definitions/submissions and task snapshots, validates bounded DAGs and token replay, and enforces truthful attempt/run transitions plus interruption semantics. Twenty-one focused local tests passed; see [B014.1 conformance](../../artifacts/conformance/B014.1-durable-job-state.md).

**Parent:** B014 · **Parent criteria:** 2, 3.

## B014.2 — Expose workspace files and SDK Python execution

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B002, B005, B013, B014.1, B014.5, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Workspace file service and SDK upload/submit/poll/output routes.

**Acceptance criteria:**

1. Use the actual SDK to upload a Python file, submit it with parameters, poll execution, and retrieve bounded stdout/output with file path/missing-input checks.

**Evidence:** The pinned SDK loopback flow passed in the 126-test local unit suite; it covers multipart file upload, single-task job submission, observed execution, bounded output, terminal failure, idempotency, workspace reopen, and schema 7-to-8 migration. See [B014.2 conformance](../../artifacts/conformance/B014.2-workspace-files-sdk-jobs.md).

**Parent:** B014 · **Parent criteria:** 1, 4.

## B014.3 — Implement dependencies retries and restart reconciliation

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B002, B005, B013, B014.2, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Bounded DAG dispatch, failure propagation, and attempt recovery.

**Acceptance criteria:**

1. Propagate failed dependencies, allocate distinct retry attempts, enforce declared concurrency limits, and restart during active execution with an explicit interrupted/recoverable outcome rather than fabricated success.

**Evidence:** B014.3.1 proves DAG failure propagation; B014.3.2 proves retries and bounded idempotent admission; B014.3.3 uses an actual service/worker crash and workspace reopen to preserve completed output and mark the unknown attempt interrupted without an exit code or automatic replay. All 135 local unit tests passed. See [B014.3.3 conformance](../../artifacts/conformance/B014.3.3-active-run-recovery.md).

**Parent:** B014 · **Parent criteria:** 2, 3, 4.

**Required children:** B014.3.1, B014.3.2, B014.3.3. All children and the original acceptance criterion above are required.

## B014.3.1 — Schedule DAG tasks and propagate dependency failures

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B002, B005, B013, B014.2, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Topological local dispatch that gates dependent tasks on actual predecessor outcomes.

**Acceptance criteria:**

1. Dispatch eligible tasks only after their dependencies succeed; never dispatch a task whose dependency failed, and persist an explicit skipped or blocked outcome with a truthful run state.

**Evidence:** The actual pinned SDK loopback tests execute a reversed-order DAG, prove dependency gating, report failed dependents as `SKIPPED/UPSTREAM_FAILED` with zero attempts, and continue independent tasks. All 128 local unit tests and the static/planning gates passed. See [B014.3.1 conformance](../../artifacts/conformance/B014.3.1-dag-dispatch.md).

**Parent:** B014.3 · **Parent criteria:** 1.

## B014.3.2 — Enforce job retries and concurrency admission

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B002, B005, B013, B014.2, B043, B014.3.1.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Distinct durable attempts for retries and bounded job-run concurrency.

**Acceptance criteria:**

1. Honor validated retry and max_concurrent_runs settings with a distinct persisted attempt for each retry; enforce capacity before starting a worker and prevent SDK retries or token replay from creating duplicate runs.

**Evidence:** B014.3.2.1 persists distinct attempts and retry outputs. B014.3.2.2 enforces the per-job 1–4 limit and the four-run process cap before admission, returns pinned-SDK `ResourceConflict` at capacity, and replays active/terminal tokens without adding runs. All 133 local unit tests, Ruff, formatting and strict mypy passed. See [B014.3.2.2 conformance](../../artifacts/conformance/B014.3.2.2-run-admission.md).

**Parent:** B014.3 · **Parent criteria:** 1.

**Required children:** B014.3.2.1, B014.3.2.2. All children and the original acceptance criterion above are required.

## B014.3.2.1 — Retry failed Python task attempts with distinct identities

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B002, B005, B013, B014.2, B043, B014.3.1.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Bounded task retry policy that creates a durable, distinct attempt per retry.

**Acceptance criteria:**

1. Honor validated max_retries, min_retry_interval_millis, and retry_on_timeout; create distinct immutable attempts and preserve each actual result/output, retrying eligible worker failures only within the configured limit.

**Evidence:** The pinned SDK loopback flow verified nonzero-exit retry, distinct attempt identities and outputs, configured delay, retry exhaustion, default no-timeout-retry behavior, and explicit timeout retry. See [B014.3.2.1 conformance](../../artifacts/conformance/B014.3.2.1-retry-attempts.md).

**Parent:** B014.3.2 · **Parent criteria:** 1.

## B014.3.2.2 — Enforce per-job run concurrency and idempotent admission

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B002, B005, B013, B014.2, B043, B014.3.1, B014.3.2.1.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Pinned-SDK run admission bounded by declared concurrency with token-safe conflict behavior.

**Acceptance criteria:**

1. Honor max_concurrent_runs within the local process limit; admit or reject before creating a worker, return a non-retryable capacity conflict, and ensure active or terminal idempotency-token replays never create duplicate runs.

**Evidence:** Actual pinned-SDK loopback tests admit two concurrent runs under a per-job limit of two, reject a third without creating a run, exercise the process-wide four-run limit, replay active and terminal idempotency tokens, and reject changed input under a used token. HTTP 409 produces SDK `ResourceConflict`. All 133 unit tests, Ruff, formatting and strict mypy passed. See [B014.3.2.2 conformance](../../artifacts/conformance/B014.3.2.2-run-admission.md).

**Parent:** B014.3.2 · **Parent criteria:** 1.

## B014.3.3 — Reconcile interrupted runs after process restart

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B002, B005, B013, B014.2, B043, B014.3.1, B014.3.2.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Startup recovery that records active work as interrupted without fabricating success or exit status.

**Acceptance criteria:**

1. On workspace reopen, reconcile persisted runs whose worker no longer exists, retain completed attempt output, and record active attempts as interrupted with no fabricated exit code; expose the outcome without automatically replaying user code.

**Evidence:** A pinned-SDK loopback test killed the service and retry worker, reopened the workspace, observed `INTERNAL_ERROR/WORKSPACE_RESTARTED`, verified the interrupted attempt had no exit code, retrieved the earlier persisted output, and replayed the idempotency token without creating another run. The schema 8-to-9 migration and interruption of a legacy running outcome also passed. All 135 local unit tests, Ruff, formatting, strict mypy, planning validation and diff checks passed. See [B014.3.3 conformance](../../artifacts/conformance/B014.3.3-active-run-recovery.md).

**Parent:** B014.3 · **Parent criteria:** 1.

## B015.1 — Scope supported pyspark imports to managed workers

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B010, B013, B043.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md).

**Deliverable:** Worker-only import bootstrap and representative transformation.

**Acceptance criteria:**

1. Run the selected pyspark import script against Sparkless/shared tables while parent/other environments retain their own modules; reject conflicts and unsupported namespaces clearly.

**Parent:** B015 · **Parent criteria:** 1, 2, 4.

## B015.2 — Delegate supported DeltaTable facade operations

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B010, B013, B015.1, B043.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md).

**Deliverable:** Worker-scoped delta.tables subset backed by the common provider.

**Acceptance criteria:**

1. Exercise the selected DeltaTable methods/builders through the table service with canonical paths/commit identity; prohibit delta-spark or JVM execution and independent registered-table writers.

**Parent:** B015 · **Parent criteria:** 3.

## B016.1 — Implement file and widget parameter context

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B016 · **Parent criteria:** 1.

**Dependencies:** B014.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Declared local dbutils.fs/widget methods with per-run state.

**Acceptance criteria:**

1. Exercise real filesystem operations, path boundaries, widget defaults/parameter precedence, missing values, and isolation between runs.

**Evidence:** 21 focused checks passed, including real pinned-SDK loopback runs with fresh widget state, task parameter values and run-now overrides; the complete local unit suite passed 155 tests. Local `dbutils.fs` paths stayed under `workspace/files/dbfs`, and traversal/symlink boundaries, size bounds, missing widgets and explicit unsupported methods were exercised. Ruff check/format, strict mypy, planning validation and diff checks passed. No Databricks credentials/account, cloud compute, Spark or JVM was used. See [B016.1 conformance](../../artifacts/conformance/B016.1-worker-dbutils-files-widgets.md).

## B016.2 — Resolve scoped secrets and redact retained context

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B016 · **Parent criteria:** 2.

**Dependencies:** B014, B016.1.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Job-scoped secret provider and metadata/value separation.

**Acceptance criteria:**

1. Test actual authorized, missing, and denied secret access without persisting values in manifests, leaking unrelated secret state, or returning fabricated SDK values.

**Evidence:** 34 focused checks and the 161-test full local suite passed. A pinned-SDK loopback worker resolved configured job-granted values, saw no unrelated scopes, raised explicit missing/denied errors, and left no raw value in run context, environment, SQLite metadata, or retained output. Ruff check/format, strict mypy, planning validation and diff checks passed. Provider values/grants are process-memory only and no Databricks account/credentials were used. See [B016.2 conformance](../../artifacts/conformance/B016.2-process-memory-scoped-secrets.md).

## B016.3 — Persist task values and output context

**Milestone:** M2 · **Kind:** task · **Initial state:** done

**Parent:** B016 · **Parent criteria:** 3.

**Dependencies:** B014, B016.2.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Run/task/attempt-scoped values and declared output/context adapters.

**Acceptance criteria:**

1. Persist supported task values across dependent tasks with recorded serialization/size/default rules and attempt isolation; expose declared context/output behavior and reject unsupported methods explicitly.

**Evidence:** 43 focused checks and the 165-test local suite passed. The pinned-SDK loopback job verified JSON values, the 48 KiB/value and 250 distinct task/key run limits, default/missing behavior, successful retry-attempt selection, upstream-only reads, unsupported methods, and run isolation. Schema 10 migrations and incomplete-schema rejection passed. Ruff check/format, strict mypy, planning validation, and diff checks passed; no Databricks credentials/account, cloud compute, Spark, or JVM was used. See [B016.3 conformance](../../artifacts/conformance/B016.3-attempt-scoped-task-values.md).

## B017.1 — Execute parameterized ipynb in fresh kernels

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B015, B016, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** nbformat/nbclient runner with outputs and errors.

**Acceptance criteria:**

1. Execute real parameterized ipynb fixtures in fresh kernels with scoped spark/dbutils injection, cell output artifacts, and propagated failures.

**Parent:** B017 · **Parent criteria:** 1, 4.

## B017.2 — Convert source exports and handle supported magics

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B015, B016, B017.1, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Structured Python source converter and explicit magic handlers.

**Acceptance criteria:**

1. Test the declared source format and Python/SQL/Markdown/run handlers, including shared-scope %run; reject unsupported languages, unknown magics, and package installation.

**Parent:** B017 · **Parent criteria:** 1, 2, 3.

## B017.3 — Implement child notebook outcomes and kernel cleanup

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B015, B016, B017.2, B017.4, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Child notebook run/exit contract and reliable cancellation.

**Acceptance criteria:**

1. Prove child notebook parameter/output isolation separately from %run, propagate failures/timeouts/parent cancellation, and reap kernels/descendants without leaked resources.

**Parent:** B017 · **Parent criteria:** 2, 3, 4.

## B018.1 — Prove the SDK dependent Python job workflow

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B012, B014, B015, B016, B017, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Runnable G2 job/Delta/dependent-task/SQL fixture.

**Acceptance criteria:**

1. Submit a parameterized Python transformation through the SDK, resolve a scoped secret, commit Delta, and verify task output plus dependent-task and external SQL/session reads; exercise dependency failure, cancellation, retry, and cross-run isolation.

**Parent:** B018 · **Parent criteria:** 1, 2, 4.

## B018.2 — Prove base and notebook-extra installation paths

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B012, B014, B015, B016, B017, B018.1, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Separate base/no-notebooks and notebook-extra G2 reports.

**Acceptance criteria:**

1. Run the base workflow without notebook dependencies, Java, or cloud compute; independently run the notebook-extra workflow and record real outputs, errors, cancellation, and cleanup.

**Parent:** B018 · **Parent criteria:** 3, 4.

## B019.1 — Define and implement the bounded SQL semantics corpus

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B011, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Versioned query/type/error fixtures and explicit supported cases.

**Acceptance criteria:**

1. Enumerate and implement selected projection/filter/aggregate/join/DDL cases with null, coercion, decimal, date/timezone, collection, and error fixtures; retain mismatches visibly.

**Parent:** B019 · **Parent criteria:** 1, 4.

## B019.2 — Route mutations and snapshot utilities through shared tables

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B011, B019.1, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** SQL update/delete/merge plus selected history/snapshot fixtures.

**Acceptance criteria:**

1. Execute SQL mutations through the selected table service/provider and compare DataFrame-visible history/schema; prove declared history/time-travel cases without bypassing feature validation.

**Parent:** B019 · **Parent criteria:** 2.

## B019.3 — Declare executor capabilities and utility limits

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B011, B019.2, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Predetermined routing matrix and supported/unsupported utility report.

**Acceptance criteria:**

1. Document selected utility/type cases and Sparkless/default routing; any enabled DuckDB path is explicitly selected before execution and has profile-specific evidence, with no silent retry and no separate managed-table copy.

**Parent:** B019 · **Parent criteria:** 1, 3.

## B020.1 — Prove the pinned CLI workspace and job workflow

**Milestone:** M3 · **Kind:** task · **Initial state:** done

**Dependencies:** B005, B014, B043.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md).

**Deliverable:** Versioned CLI fixture against the local BrickHouse server.

**Acceptance criteria:**

1. Run the selected real CLI workspace/file/job operations locally with positive/error cases and cleanup, recording exact client versions.

**Parent:** B020 · **Parent criteria:** 1.

## B020.2 — Prove the minimal Terraform lifecycle

**Milestone:** M3 · **Kind:** task · **Initial state:** done

**Dependencies:** B005, B014, B020.1, B043.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0035-terraform-local-resource-identity.md](../decisions/0035-terraform-local-resource-identity.md).

**Deliverable:** Pinned minimal resources and state/refresh lifecycle evidence.

**Acceptance criteria:**

1. Apply/read/update/refresh/destroy only the resources chosen in B005 against BrickHouse, checking actual local state and lifecycle errors.

**Evidence:** [Pinned Terraform lifecycle report](../../artifacts/conformance/B020.2-terraform-v1.16.6-provider-v1.137.0.md).

**Parent:** B020 · **Parent criteria:** 2.

## B020.3 — Prove bundle validation deployment and execution

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B005, B014, B020.2, B043.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md).

**Deliverable:** One supported bundle fixture and required metadata/error behavior.

**Evidence:** [Databricks CLI v1.21.0 bundle conformance report](../../artifacts/conformance/B020.3-databricks-cli-bundle-v1.21.0.md).

**Acceptance criteria:**

1. Validate/deploy/run the selected bundle, verify real worker output, label required compute metadata simulated, and explicitly reject unsupported resources/task types.

**Parent:** B020 · **Parent criteria:** 3.

## B021.1 — Prove released REST connector transport locally

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B019, B045.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Pinned actual connector with local TLS/auth/transport evidence.

**Acceptance criteria:**

1. Connect the released REST/kernel profile to BrickHouse with recorded TLS/auth configuration and method limits; a failed interoperability probe leaves the promised implementation blocked or triggers a formal scope revision.

**Parent:** B021 · **Parent criteria:** 1, 3.

## B021.2 — Exercise connector query and result lifecycle

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B019, B021.1, B045.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Connector parameter/schema/fetch/error/lifetime/cancellation fixtures.

**Acceptance criteria:**

1. Execute parameterized queries through the actual connector, compare schema/results, and test failures, retention, cancellation where advertised, plus explicit unsupported metadata/driver methods.

**Parent:** B021 · **Parent criteria:** 2, 3.

## B022.1 — Inject journal and commit boundary failures

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B018, B019, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** G3 phase-by-phase crash/reconciliation matrix.

**Acceptance criteria:**

1. Crash before/after Delta commit and SQLite finalization, then restart and verify known/uncertain outcomes and safe idempotent retry using actual files.

**Parent:** B022 · **Parent criteria:** 1, 4.

## B022.2 — Exercise concurrent alias-aware SQL and DataFrame writes

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B018, B019, B022.1, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Concurrent-writer and idempotent-operation evidence.

**Acceptance criteria:**

1. Append through SQL and DataFrame paths including canonical-location aliases, proving no lost committed rows or duplicates from repeated operation IDs.

**Parent:** B022 · **Parent criteria:** 2, 4.

## B022.3 — Verify retention coordinated backup and restore

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B018, B019, B022.2, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Real backup/restore report, recovery checks, and retention policy.

**Acceptance criteria:**

1. Exercise restart/safe retry and result/log/staging retention; restore a coordinated SQLite/Delta/file snapshot and read actual rows/history with an explicit external-table inclusion policy.

**Parent:** B022 · **Parent criteria:** 3.

## B023.1 — Freeze the 0.1.0 scope and distribution inventory

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Parent:** B023 · **Parent criteria:** 3, 4.

**Dependencies:** B020, B021, B022, B043, B044, B048.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Versioned release scope, notices, dependency/client inventory, and limitations.

**Acceptance criteria:**

1. Reconcile completed requirements with per-case capability evidence; verify notices/license decisions, migrations, examples, and a publicly available compatible Sparkless artifact without publishing BrickHouse.

## B023.2 — Gate candidate wheels on platforms and offline execution

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Parent:** B023 · **Parent criteria:** 1, 2, 4.

**Dependencies:** B020, B021, B022, B023.1, B043, B044, B048.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** 0.1.0 wheel/sdist install matrix and consolidated local G1-G4 reports.

**Acceptance criteria:**

1. Install built wheels for every advertised Python/platform candidate and run declared G1-G4 plus JVM/cloud/Docker-free base and provisioned offline checks; gate the notebook extra separately and advertise only passing combinations.

## B023.3 — Assemble the owner-reviewable release candidate

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Parent:** B023 · **Parent criteria:** 3, 4.

**Dependencies:** B020, B021, B022, B023.2, B043, B044, B048.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Local distribution artifacts, generated capability docs, and release report.

**Acceptance criteria:**

1. Assemble actual artifacts and retained evidence, update truthful current-behavior documentation, validate the owner release checklist, and keep publication as a separately authorized action.

## B011.5 — Implement local SQL warehouse identity and lifecycle

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B010, B011.1, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Persistent warehouse records, type/flag validation and simulated lifecycle with admission/invalidation contracts.

**Acceptance criteria:**

1. Use the pinned SDK to create/get/list/update/delete and start/stop serverless, pro, and classic local warehouse records; validate PRO plus enable_serverless_compute semantics, settings and missing IDs, label infrastructure simulated, and exercise persistence and idle transitions with a controlled clock.
2. Test real repository/state/API behavior for admission leases, stop/delete invalidation, missing resource errors and persistence with a controlled clock; expose the service contract consumed by B011.6, without requiring downstream SQL/connector results for this child to complete.

**Parent:** B011 · **Parent criteria:** 4.

## B014.4 — Implement classic compute identity and lifecycle

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B002, B005, B013, B014.1, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Workspace-owned all-purpose and run-scoped job compute records with SDK lifecycle routes.

**Acceptance criteria:**

1. Persist and expose the selected pinned-SDK cluster create/get/list/edit/start/restart/terminate/delete behavior for local all-purpose compute; validate runtime/access-mode mappings and unsupported effects, label infrastructure simulated, and verify identity and legal transitions across restart.
2. Model new/shared job compute per run and persistent all-purpose identity; test first/last-consumer state, autostart/idle rules and durable invalidation events through the compute service using real metadata and a controlled clock. B014.6 consumes that contract to prove actual worker effects.

**Parent:** B014 · **Parent criteria:** 4.

## B014.5 — Resolve serverless and classic execution contexts

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B002, B005, B013, B014.4, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Versioned selector/environment/configuration resolver with immutable mode-aware attempt context.

**Acceptance criteria:**

1. Resolve the pinned serverless environment_key submission and classic new_cluster/existing_cluster_id/job_cluster_key shapes; reject conflicts, missing/deleted resources, invalid runtime/access mode and unsupported task/resource combinations before dispatch.
2. Map only provisioned local environment/client/runtime versions and verified dependencies, apply documented config scopes and serverless defaults, reject unsupported installation/compute-object selectors, and require the initial serverless fixture to disable unimplemented automatic optimization retries.

**Parent:** B014 · **Parent criteria:** 4.

## B017.4 — Bind compute profiles to notebook session lifecycle

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B015, B016, B017.2, B014, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Serverless/classic notebook context, environment precedence and isolated kernel attachment/reset between executions.

**Acceptance criteria:**

1. Execute real local notebook fixtures under serverless and classic standard/dedicated profiles with mapped environments, notebook/job override precedence, supported config defaults and rejection cases; preserve cell state only in its own live kernel.
2. Verify distinct notebook sessions, environment/config rejection before execution, retained state between cells and a fresh context after a normal reset; expose compute attachment/invalidation to B017.3/B017.5, which own interruption during execution and descendant cleanup.

**Parent:** B017 · **Parent criteria:** 4.

## B020.4 — Prove compute and warehouse Terraform resources

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B014, B020.3, B011, B043.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Pinned Terraform lifecycle matrix for selected classic compute and three SQL warehouse resource variants.

**Acceptance criteria:**

1. Apply/read/update/refresh/destroy selected classic cluster and all three SQL warehouse resource variants against local BrickHouse with pinned Terraform/provider versions, validating persisted identity, type flags, errors and cleanup.

**Parent:** B020 · **Parent criteria:** 4.

## B043 — Research and define serverless and classic compute contracts

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005.

**Plans:** [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md).

**Deliverable:** Dated primary-source research, compute behavior matrix, release allocation, and synchronized task/capability contracts.

**Acceptance criteria:**

1. Research current official Databricks documentation and the pinned SDK for serverless jobs/notebooks, classic all-purpose/job compute and access modes, and serverless/pro/classic SQL warehouses; record sources, inspected versions, and unobserved behavior.
2. Define real local execution, mode selection, environment/configuration rules, lifecycle and failure behavior, and the boundary between implemented semantics, simulated infrastructure, and unsupported features without a JVM or cloud fallback.
3. Assign bounded acceptance criteria and local fixture designs to release phases and existing implementation tasks; synchronize the roadmap, backlog, capability inventory, architecture, and reference policy, and pass planning validation without claiming runtime support.

## B011.6 — Bind warehouse admission to real statement execution

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B010, B043, B011.2, B011.5.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Warehouse-bound statement dispatch, admission leases, invalidation and actual shared-table results.

**Acceptance criteria:**

1. Execute actual SDK statements through each local warehouse type using persistent warehouse identity, per-resource/global admission and supported settings; verify schema/rows and committed versions, and reject missing/deleted IDs before engine dispatch.
2. Stop/reset a warehouse during real execution and verify admission release, accurate cancellation/terminal outcomes and committed-data preservation; bind the selected connector warehouse path to the same identity, leaving released-connector proof to B021.

**Parent:** B011 · **Parent criteria:** 1, 2, 4.

## B014.6 — Verify compute lifetimes against real job workers

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B002, B005, B013, B043, B014.3, B014.4, B014.5.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Compute/supervisor integration for the seven required job variants, interruption and committed-data preservation.

**Acceptance criteria:**

1. Run real workers under serverless and classic standard/dedicated new/existing/shared variants from document 16; verify accurate attempt/resource state, shared-job lifetime and persistent all-purpose identity, with fresh imports/session context per attempt.
2. Terminate/restart compute during actual work and verify affected process groups are reaped, terminal/retry outcomes are accurate, committed tables remain readable, and service restart never infers job success from resource metadata.

**Parent:** B014 · **Parent criteria:** 1, 3, 4.

**Evidence:** The pinned SDK loopback test runs actual workers for all seven document 16 variants and verifies accurate run/task/resource states, fresh attempt processes, shared first/last consumers, and persistent all-purpose identity/autostart. Separate restart and termination cases during active user code prove worker/descendant reaping and preserve a committed Delta table across workspace reopen. The service-crash case releases the abandoned compute consumer and keeps the unknown attempt interrupted without an exit code or inferred success. Ten focused compute/job tests and the full 138-test unit suite passed; Ruff check/format, strict mypy, planning validation and diff checks passed. See [B014.6 conformance](../../artifacts/conformance/B014.6-job-worker-compute-lifetimes.md).

## B017.5 — Qualify notebook profiles during compute interruption

**Milestone:** M2 · **Kind:** task · **Initial state:** planned

**Dependencies:** B015, B016, B043, B017.3, B017.4, B014.6.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Real notebook mode/format corpus with in-flight compute reset, cancellation and replacement-kernel evidence.

**Acceptance criteria:**

1. Run the document 16 notebook-extra variants with real kernels and shared-table results, including notebook/job environment precedence and source-export handling for serverless and classic standard/dedicated profiles.
2. Reset/terminate attached compute during cell execution and exercise timeout/parent cancellation, proving actual kernel/descendant cleanup, fresh replacement state, no cross-notebook leakage and preservation of committed Delta data.

**Parent:** B017 · **Parent criteria:** 1, 3, 4.

## B020.5 — Prove both compute modes through bundle workloads

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B014, B043, B020.4, B018.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Versioned serverless/classic bundles with real Python/notebook output, Delta/SQL checks and cleanup.

**Acceptance criteria:**

1. Validate/deploy/run the selected serverless and classic bundle variants using the pinned CLI and mapped local environments/runtime settings; inspect actual Python/notebook outputs and Delta/SQL results from the G2 corpus, not resource-creation responses.
2. Exercise invalid resource/task/settings, update/redeploy and cleanup against persisted local state; report supported execution separately from simulated infrastructure and retain exact client/provider/fixture versions.

**Parent:** B020 · **Parent criteria:** 3, 4.

## B044 — Refine the phase 0.1 execution and release gates

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Dependencies:** B043.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Executable work order, bounded provider/consumer tasks, finite release corpus and local fixture limits for 0.1.0.

**Acceptance criteria:**

1. Review the actual code, task states and dependency graph; make the Sparkless Python provider binding and SQL connector transport feasibility explicit early work without weakening the existing runtime or release requirements.
2. Separate resource/context deliverables from downstream workload verification, decompose oversized compute/client work, and define a finite mode/type/failure corpus, local fixture limits, evidence owners and release stop conditions.
3. Synchronize the release contract, roadmap, backlog, task and capability references, decisions and status; pass planning validation while preserving existing implementation states/evidence and all parent acceptance criteria.

## B045 — Resolve SQL connector transport feasibility early

**Milestone:** M0 · **Kind:** task · **Initial state:** planned

**Dependencies:** B005, B006, B044.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [ADR 0021](../decisions/0021-sql-kernel-local-transport-adaptation.md).

**Deliverable:** Pinned REST/kernel loopback transport capture, TLS/auth configuration and explicit feasible/blocked decision.

**Acceptance criteria:**

1. Inspect the actual pinned connector/kernel artifacts and send a bounded request to a local HTTPS capture boundary with explicit loopback host, local CA/auth, warehouse path and credentials absent; record the real request/error and any host/TLS/auth restrictions without returning fabricated SQL results.
2. Retain exact versions/hashes and a sanitized feasible/blocked transport decision; verify local host/credential isolation and identify the API changes needed by B011/B021. A negative feasibility finding does not establish query compatibility and must keep the B021 release claim blocked or trigger a formal scope revision.

**Evidence:** The pinned connector reached the loopback SQL session endpoint over verified local TLS with synthetic Bearer auth and the warehouse ID derived from `http_path`. The capture boundary returned a real local 401 before SQL execution, so query/result compatibility remains unproven and B021 stays gated on full local execution. Exact wheel hashes, host/port and private CA constraints, API changes and the sanitized request/error are in [B045 conformance](../../artifacts/conformance/B045-sql-connector-loopback.md).

## B046 — Deliver the Sparkless Python provider binding

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B009, B043, B044.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [0018-published-sparkless-python-provider.md](../decisions/0018-published-sparkless-python-provider.md).

**Deliverable:** Tested generic Python provider API, callback ownership/error contract and reproducible candidate wheel for BrickHouse integration.

**Acceptance criteria:**

1. Read current Sparkless code/instructions and define then expose a public session-scoped Python provider registration/lookup/write API over the tested Rust hooks, preserving generic engine boundaries.
2. Verify real Python callbacks, typed decimal/nested interchange, owner-thread/GIL/lifetime/cancellation/error behavior and independent sessions without private-API reliance, silent fallback or unbounded materialization claims.
3. Verify and probe an exact published source/wheel artifact, or build a candidate when no compatible release exists, with a real local Delta-backed provider; record versions/hashes/signatures and remaining limits, and distinguish development artifacts from the public compatible release required by B023.

**Required children:** B046.1, B046.2, B046.3. All children and the original criteria above are required.

## B046.1 — Specify the public Python provider ownership contract

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B009, B043, B044.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [0018-published-sparkless-python-provider.md](../decisions/0018-published-sparkless-python-provider.md).

**Deliverable:** Current-source Python API gap review and bounded registration/callback/error design.

**Acceptance criteria:**

1. Inspect the current Sparkless checkout and its instructions, Python/PyO3 exports and tested Rust provider API; record exact source revision and proposed public signatures, owner-thread/GIL, lifetime, structured errors and bounded IPC limits without treating unpublished APIs as available.

**Parent:** B046 · **Parent criteria:** 1, 2.

## B046.2 — Implement and verify Python provider callbacks

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B009, B043, B044, B046.1.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [0018-published-sparkless-python-provider.md](../decisions/0018-published-sparkless-python-provider.md).

**Deliverable:** Generic PyO3/Python provider binding with real callback and isolation checks.

**Acceptance criteria:**

1. Expose the reviewed registration/read/write contract in Sparkless and test real Python callbacks for typed Arrow/Delta data, provider failures, callback lifetime, reentrancy/owner-thread rules and independent sessions under that checkout's instructions; retain focused test commands and actual source commit.

**Parent:** B046 · **Parent criteria:** 1, 2.

## B046.3 — Verify and probe the published Python provider wheel

**Milestone:** M1 · **Kind:** task · **Initial state:** planned

**Dependencies:** B009, B043, B044, B046.2, B008.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [0018-published-sparkless-python-provider.md](../decisions/0018-published-sparkless-python-provider.md).

**Deliverable:** Verified published wheel/source provenance and real local Delta provider probe for the Python bridge.

**Acceptance criteria:**

1. Hash-verify and install an exact published wheel, or build a candidate when no compatible release exists, then use the public Python API with a real local Delta-backed provider to read/delegate writes, preserve Decimal/nested schemas and committed versions, propagate failures, and close independent sessions with bounded inputs.
2. Record the tested source/wheel hash, dependency versions, public signatures and all owner-thread/materialization limits; make the development artifact reviewable without publishing, and retain B023's requirement for a publicly available compatible artifact before release.

**Parent:** B046 · **Parent criteria:** 2, 3.
## B047 — Plan the self-hosted platform across all release phases

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B044.

**Plans:** [00-vision-and-scope.md](00-vision-and-scope.md), [01-architecture.md](01-architecture.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** An evidence-gated self-hosted deployment roadmap synchronized with the existing local compatibility plan.

**Acceptance criteria:**

1. Preserve phases 0.1 through 0.9 as the primary JVM-free Databricks emulator roadmap; record phase-specific architectural safeguards and a separate post-stability deployment track without displacing existing requirements.
2. Gate every deployment implementation on recorded emulator stability and define trusted-server, isolated-server and worker-pool boundaries, with authentication, authorization, execution isolation, storage, recovery and operational responsibilities separately owned.
3. Synchronize architecture decisions, release and milestone plans, task/backlog and capability ledgers, and validate dependencies, links and preservation of prior claims without implying implemented deployment support.

## B048 — Review local-core boundaries for future deployment

**Milestone:** M3 · **Kind:** task · **Initial state:** planned

**Dependencies:** B047, B018, B022.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [01-architecture.md](01-architecture.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** A code-referenced architecture review of the real local workflow and its future deployment adaptation points.

**Acceptance criteria:**

1. Review the implemented G1-G4 paths for one catalog/table-commit authority and separation of transport, domain services, persistence and local runtime; record concrete code references and existing test evidence.
2. Identify contained local paths, staging, interpreter/PID/signal/socket assumptions and stable workspace/table/job/attempt identities; document future storage/runtime adaptation points without adding speculative backends or changing tested local behavior.
3. Map future principal provenance and enforcement points across API, SQL, workers, files, secrets and results; accurately state that local compatibility tokens, access-mode labels and fresh processes are not network authentication or a sandbox.
4. Resolve violations of existing architectural invariants before release through bounded owning work, retain future-only gaps for SH.0 onward, and attach the review to the B023 scope record without adding deployment infrastructure to 0.1.

## B049 — Establish emulator stability before deployment implementation

**Milestone:** M7 · **Kind:** task · **Initial state:** planned

**Dependencies:** B042, B048.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** A versioned emulator stability baseline and bounded first-deployment scope decision.

**Acceptance criteria:**

1. Select the exact emulator candidate, supported semantic/compute/client corpus, stability criteria and maintenance policy; assess repeated applicable G1-G6, clean-install, upgrade and coordinated-restore evidence with no unresolved release-blocking correctness, corruption or JVM-free execution defects in the declared scope.
2. Record remaining incompatibilities and defects honestly; stability does not require complete Databricks parity, does not follow automatically from phase 0.9 and does not assign a 1.0 release.
3. Select the initial single-host platform, trusted-code threat model, identity/credential approach and supported workload limits before SH.1 decomposition; all deployment implementation remains dependent on this completed gate.
4. Keep emulator maintenance and the local wheel workflow independent of deployment extras, external accounts, orchestration and remote execution; reference-account capture and publication retain their existing authorization boundaries.

## B050 — Deliver authenticated single-server deployment for trusted workloads

**Milestone:** M7 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B049.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [02-packages-and-public-api.md](02-packages-and-public-api.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Decomposed SH.1 service, authentication, secret persistence and single-host operations epic with S1 evidence.

**Acceptance criteria:**

1. Decompose into bounded implementation children after B049; freeze server-trusted-v1 threat model, platform/client matrix and positive, denial and recovery cases without claiming hostile-code isolation.
2. Provide verified encrypted ingress, actual user/service authentication with credential expiry/revocation, and shared authorization/audit decisions across supported API, SQL, worker, file, secret and result paths; no-auth mode cannot bind beyond loopback.
3. Persist scoped secrets using an explicit protected storage and recovery contract, test rotation/restart and principal/run denial, and exclude secret values from config, URLs, logs, ordinary backups and build artifacts.
4. Ship an installable optional single-server deployment artifact with durable state, bounded admission, health/readiness, draining/shutdown and coordinated backup/restore; use the same core services and retain one metadata owner.
5. Pass S1 using real TLS clients, providers, files and jobs on the selected deployment host; document arbitrary Python host-access limits for trusted code authors and prove the base local installation still runs without deployment dependencies.

## B051 — Add a browser workspace and deployed optional integrations

**Milestone:** M7 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B050.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Decomposed SH.2 browser and optional-provider deployment epic with S2 evidence.

**Acceptance criteria:**

1. Decompose and freeze the minimal browser workflow: edit/upload supported source, submit/cancel jobs, inspect authorized outputs, browse catalog data and execute bounded SQL through the same existing services.
2. Test browser session expiry/revocation, CSRF protection where applicable, output rendering safety and cross-principal denial for source, notebooks, results and logs; never create a second catalog, scheduler or UI-only authorization model.
3. Deploy only selected notebook/MLflow/serving/dbt/vector integrations with scoped credentials, authenticated advertised endpoints, explicit provider authority, resource/lifecycle management and coordinated state/artifact restore tests.
4. Pass S2 against real browser/API/provider flows with exact versions; absent extras remain absent, hosted services do not silently call cloud APIs, and the local core remains independently installable.

## B052 — Isolate execution for shared self-hosted use

**Milestone:** M7 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B051.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Decomposed SH.3 execution-boundary and resource-isolation epic with S3 evidence.

**Acceptance criteria:**

1. Decompose against a selected real OS/container/VM boundary and explicit supported host/threat matrix; process isolation or classic/serverless labels alone cannot satisfy server-isolated-v1.
2. Prevent scoped user workloads from accessing control metadata, other attempts, unauthorized table snapshots/files/results/secrets and forbidden host/network resources; test real denial and bypass attempts, including direct Python access.
3. Enforce measured CPU, memory, process, disk and runtime limits plus cancellation/cleanup for every advertised script, kernel and serving worker; preserve actual outcomes and committed Delta data under exhaustion or termination.
4. Pass S3 and the applicable emulator corpus inside the selected isolation mechanism, recording residual platform/shared-kernel risks and keeping any container runtime an explicit deployment-only prerequisite.

## B053 — Execute jobs and durable automation on remote workers

**Milestone:** M7 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B052.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Decomposed SH.4 authenticated worker-pool, storage-transfer and recovery epic with S4 evidence.

**Acceptance criteria:**

1. Decompose worker enrollment/revocation, private versioned transport, environment digests, bounded artifact transfer and placement/admission; retain one authoritative metadata and table-commit service and no shared writable worker SQLite store.
2. Enforce coordinator-issued attempt leases/generations and scoped credentials at state-changing and commit requests; heartbeat loss, partition, stale or duplicate completion, cancel/retry races and worker death cannot fabricate success or authorize stale commits.
3. Qualify explicit shared-storage or transfer paths using the real native Sparkless/Delta IO, distinct server/worker roots, snapshot identity, checksums and credential scope; preserve journal reconciliation after commit-before-response failures.
4. Run selected ingestion/streaming/pipeline workloads with fenced checkpoint ownership and tested reassignment, preserving declared source/sink guarantees and partial-table recovery; external Python effects have no blanket exactly-once guarantee.
5. Pass S4 with a real enrolled second host and documented network failures, actual rows/versions and no JVM or cloud Spark fallback; local process simulations alone cannot establish multi-host support, and job distribution does not imply single-query distribution.

## B054 — Qualify self-hosted operations and recovery

**Milestone:** M7 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B053.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Decomposed SH.5 upgrade, restore, observability and capacity qualification epic with S5 evidence.

**Acceptance criteria:**

1. Decompose a finite supported deployment/platform/storage matrix, documented recovery-point/recovery-time objectives, capacity targets and operational failure cases before qualification.
2. Test clean installation and supported upgrades with preflight/version checks, maintenance/draining and explicit recovery or rollback policy; migration failure must not silently corrupt or open newer state with an older binary.
3. Restore coordinated catalog, Delta, jobs, checkpoints and selected provider state onto another host using validated root/ownership mappings and an explicit external-table and protected-secret recovery policy; read real rows, versions and histories afterwards.
4. Verify credential/certificate rotation, safe retention, structured logs/metrics, health/readiness, disk/memory pressure, overload and soak behavior; publish operator runbooks and measured results.
5. Pass S5 with exact artifacts and a bounded support matrix; state single-control-service availability limits, and do not claim HA or replace SQLite without a separate tested migration/consistency decision.

## B055 — Prepare the self-hosted distribution candidate

**Milestone:** M7 · **Kind:** epic · **Initial state:** planned

**Dependencies:** B054.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [02-packages-and-public-api.md](02-packages-and-public-api.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Decomposed SH.6 distribution, consolidated evidence and operator handoff epic with S6 evidence.

**Acceptance criteria:**

1. Decompose candidate assembly and freeze the exact supported deployment/semantic/compute/client matrix; rerun applicable G1-G6 and S1-S5 from exact built artifacts for every advertised deployment profile.
2. Publish a reviewable local candidate inventory with artifact hashes, native/runtime versions, notices/license decision, migrations, security/recovery reports, runnable examples and operator documentation; actual publication still requires separate owner authorization.
3. Report planned, unsupported, partial, simulated and supported capabilities separately; real worker allocation cannot be inferred from simulated Databricks cluster metadata, and the release does not imply complete Databricks or distributed-query parity.
4. Pass S6 while independently retaining the JVM-free local wheel workflow without containers, identity servers, remote workers or cloud accounts; keep unresolved deployment/frontier requirements open with concrete limitations.
