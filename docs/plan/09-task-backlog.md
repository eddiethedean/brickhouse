# 09 — Implementation backlog

This document mirrors `planning/tasks.json`: the JSON ledger is canonical for task definitions and current state. Keep this readable view synchronized when task definitions change. Status labels are checked against the ledger; dated evidence retains the versions and artifact scope originally tested.

## Task execution rules

Complete dependencies first. A research task can finish with a documented negative result if its acceptance criteria ask for a decision; an implementation task cannot finish without the promised behavior. `deferred` dependencies do not satisfy `done`. Revise release scope explicitly when needed.

B001–B023 are initial work packages. Split any that cannot be implemented and reviewed coherently into children before starting; parent completion requires every required child and original acceptance criterion. B024–B042 are epics and must be decomposed first. Assign new child IDs such as `B008.1`, preserving the parent requirements and dependency graph. Child tasks inherit the parent prerequisites, not a dependency on the parent itself.

Each completed task records commands/checks, exact versions, result summaries, relevant fixture/report paths, and commit references when available. Source research alone is not evidence that BrickHouse behavior works.

Phase 0.1 parents B001–B023 now have required child tasks listed below and in the ledger. Child entries identify their parent and the one-based original acceptance criteria they cover; all children inherit their parent's prerequisites and never depend on the parent itself. Complete all children and original parent integration checks before marking a parent done. Start implementation at B001.1. See [the phase 0.1 contract](16-phase-0.1-initial-release.md) for checkpoint and release evidence.

The 2026-10-20 compute expansion adds planning task B043 and five bounded implementation children (B011.5, B014.4, B014.5, B017.4, B020.4), bringing required phase 0.1 children to 73 at that decision. Appended criteria preserve all prior requirements. [Document 17](17-serverless-and-classic-compute.md) maps serverless/classic behavior, warehouse types and later environment/access/protocol work to their acceptance owners.

The delivery refinement adds B044 planning, B045 early connector research and B046 provider qualification. The ledger retains **120 original implementation children**, two B048 review children, eleven B064 fixes and nine B066 release-preparation children/descendants: **142 phase children**. B064 is complete. B066.2 passed the full Linux/macOS offline platform matrix; B066.3.1 completed final metadata/diagnostics; B066.3.2 completed owner-verified PyPI pending-publisher configuration; B066.4 completed the exact artifact/evidence freeze; all 142 phase children are done. Every original acceptance criterion and prerequisite remains required. [ADR 0017](../decisions/0017-phase-0.1-delivery-refinement.md) records the original provider/consumer split.

The emulator remains primary through phases 0.1–0.9. B047 records the long-term plan and B048 completes a bounded local architecture review before B023; B048.1 closes its unauthenticated network-bind bypass. B049/SH.0 assesses stability after B042; B050–B055 are future deployment epics, strictly after that gate and requiring decomposition before implementation. See [document 18](18-self-hosted-platform.md) and [ADR 0034](../decisions/0034-emulator-first-self-hosted-platform.md). Existing emulator task states, acceptance criteria and evidence remain authoritative.

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

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B001.

**Plans:** [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [14-research-sources.md](14-research-sources.md).

**Deliverable:** docs/upstream/minilake-inventory.md and a reuse ADR; no wholesale app mounting.

**Acceptance criteria:**

1. Inspect the pinned source and record each proposed file/contract, license, globals, persistence, and Spark dependencies.
2. Choose adaptation versus fresh implementation for the first catalog/jobs/files routes and identify required removals.
3. Specify notices and tests before copying source; record a rationale if no implementation code is reused.

**Required children:** B002.1, B002.2. All children and the original criteria above are required.

## B003 — Prove Sparkless provider extension design

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B001.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md).

**Deliverable:** Minimal engine integration probe, proposed public contracts, and coordinated upstream task list.

**Acceptance criteria:**

1. Read current Rust/Python APIs and identify what already exists versus missing S001–S006 hooks.
2. Demonstrate a viable path for external relation binding, writer delegation, typed interchange, and isolated sessions; a temporary probe is clearly separated from production code.
3. Record callback ownership, GIL/thread/reentrancy risks, failure propagation, and the smallest upstream change required.

**Required children:** B003.1, B003.2, B003.3. All children and the original criteria above are required.

## B004 — Select native Delta provider and feature envelope

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B001.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md).

**Deliverable:** Delta-provider ADR, pinned dependency matrix, and interoperability fixtures.

**Acceptance criteria:**

1. Exercise real create/read/append/update/delete/merge with candidate native providers; record unsupported operations explicitly.
2. Cross-read supported types and versions through Sparkless and selected SQL path, including nulls/decimals/timestamps.
3. Probe transaction identity and crash-after-commit reconciliation; choose one mutation provider and document protocol/feature rejection rules.

**Required children:** B004.1, B004.2, B004.3. All children and the original criteria above are required.

## B005 — Pin client profiles and capture transport fixtures

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

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

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B001.

**Plans:** [01-architecture.md](01-architecture.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Workspace/config services, SQLite migrations, lifecycle CLI and embedded context.

**Acceptance criteria:**

1. Initialize an isolated workspace with explicit default namespace policy and reopen it after restart.
2. Enforce one service owner, validate config/workspace versions, and reject incompatible formats.
3. Two different workspace directories have independent state and clean shutdown.

**Required children:** B006.1, B006.2, B006.3. All children and the original criteria above are required.

## B007 — Implement catalog identity and namespace services

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

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

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B008, B009, B043, B046.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Sparkless engine adapter, session factory, and table binding.

**Acceptance criteria:**

1. Explicit Python API writes a managed table then reads it by full name in a fresh session.
2. DataFrame writes and supported path saves route through the common table service; unsupported bypasses fail.
3. Verify typed interchange and independent namespaces/temp views/config/UDF state for two concurrent sessions.
4. Apply the selected serverless/classic compute and runtime profile to isolated namespace/config/UDF/view state; verify defaults and reject unimplemented mode-specific semantics without claiming Spark Connect parity.

**Required children:** B010.1, B010.2, B010.3. All children and the original criteria above are required.

**Evidence:** All three children and the four original criteria passed. The internal Sparkless 4.14.1 adapter uses the shared DeltaRsTableService for selected table operations and verifies typed, isolated coexisting sessions under resolved serverless defaults and one explicit classic-standard local runtime. Close detaches/stops the native session and releases its BrickHouse reference on the owner thread. The full suite passed 201 tests with three opt-in integrations skipped and no PyO3 cross-thread-drop warning; static, build, dependency, planning and diff gates passed. See [B010.1](../../artifacts/conformance/B010.1-sparkless-read-adapter.md), [B010.2](../../artifacts/conformance/B010.2-sparkless-write-adapter.md), and [B010.3](../../artifacts/conformance/B010.3-sparkless-session-isolation.md). SQL and job-worker sessions remain unintegrated.

## B011 — Implement initial Statement Execution path

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done progress.

**Dependencies:** B005, B010, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** AST parsing/binding, query and DDL routing, statement lifecycle/results API.

**Acceptance criteria:**

1. Pinned SDK executes supported SELECT/DDL over the same registered Delta tables and receives correct schema/row encoding.
2. Bound parameters, error responses, timeouts, polling, cancellation, and bounded JSON results have explicit tested behavior.
3. Reject unsupported syntax/formats before side effects; no exception-driven engine fallback.
4. Resolve serverless/pro/classic local warehouse identity and type flags before execution; enforce start/stop/admission/cancellation and return real shared-table results, rejecting invalid resources and unsupported settings.

**Required children:** B011.1, B011.2, B011.3, B011.4, B011.5, B011.6. All children and the original criteria above are required.

## B012 — Prove shared-data and restart workflow

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md).

**Deliverable:** G1 conformance fixture and report.

**Acceptance criteria:**

1. Write via Sparkless, query via SDK SQL, read in a fresh session, restart the service, and obtain the same committed rows/schema.
2. Run without Java/full PySpark/Docker or cloud network access; capture process/dependency evidence.
3. Exercise empty data, schema mismatch, external table drop, and stale metadata invalidation.

**Required children:** B012.1, B012.2. All children and the original criteria above are required.

**Evidence:** B012.1 passed the Sparkless → Delta → SDK SQL → fresh session → local restart workflow and compared table identity, schema, committed version and rows. B012.2.1–B012.2.2 cover empty tables, schema-mismatch rejection, external file ownership and stale metadata rejection. B012.2.3 reran G1 under macOS outbound-network denial except localhost, with Databricks credentials unset and import/process guards active. See [B012.1](../../artifacts/conformance/B012.1-g1-shared-table-restart.md), [B012.2.1](../../artifacts/conformance/B012.2.1-empty-and-schema-mismatch.md), [B012.2.2](../../artifacts/conformance/B012.2.2-external-drop-and-stale-metadata.md), and [B012.2.3](../../artifacts/conformance/B012.2.3-native-only-process-network.md).

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

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B010, B013, B043.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md).

**Deliverable:** Worker-scoped supported pyspark/delta import facade.

**Acceptance criteria:**

1. Run a representative script using supported pyspark imports against Sparkless and the shared table service.
2. Parent process and other environments retain their own import modules; conflicting/unsupported imports fail clearly.
3. Supported DeltaTable facade methods use the selected native provider; no delta-spark or JVM launch path.
4. Apply compute-profile-specific Python API and configuration gates inside the worker; verify serverless restrictions and classic runtime/access-mode limits, including explicit BrickHouse errors for JVM/RDD functionality absent from the native implementation.

**Required children:** B015.1, B015.2. All children and the original criteria above are required.

**Evidence:** All required child slices are complete. Managed workers isolate selected `pyspark` and `delta.tables` imports, apply the tested serverless/classic settings, read registered committed Delta tables through the authenticated parent service, and expose the selected read-only DeltaTable facade. Unsupported namespaces, JVM/RDD APIs, unregistered paths, and mutations fail explicitly. See [B015.1.1](../../artifacts/conformance/B015.1.1-worker-pyspark-imports.md), [B015.1.2](../../artifacts/conformance/B015.1.2-worker-spark-session.md), [B015.1.3](../../artifacts/conformance/B015.1.3-worker-registered-tables.md), and [B015.2](../../artifacts/conformance/B015.2-worker-delta-table.md). The selected loopback profile does not establish remote Databricks behavior.

## B016 — Implement scoped dbutils subset

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

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

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B015, B016, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** nbformat/nbclient runner and explicit Databricks source/magic handling.

**Acceptance criteria:**

1. Execute ipynb and the declared Databricks source subset in fresh kernels with parameters, outputs, and error propagation.
2. Test %run shared-context behavior separately from child notebook execution and return values.
3. Reject unsupported magics/languages/package installation; cancel kernels and release resources reliably.
4. Execute the bounded serverless and classic notebook corpus with selected environment inheritance, isolated sessions, supported settings, all-purpose attachment/reset and compute termination effects; report unimplemented dependency/protocol behavior explicitly.

**Required children:** B017.1, B017.2, B017.3, B017.4, B017.5. All children and the original criteria above are required.

**Evidence:** The five child tasks are complete with criterion-level local evidence: fresh `.ipynb` kernels and failure/output capture; marked Python source conversion and `%run`; child notebook isolation, timeout and cancellation cleanup; serverless/classic profile contexts and normal reset; and SDK notebook dispatch with environment/parameter precedence and active compute interruption. The aggregated SDK regression suite passed 87 tests, Ruff check/format, strict mypy, `pip check`, and `git diff --check`. See [B017.1](../../artifacts/conformance/B017.1-ipynb-kernel.md), [B017.2](../../artifacts/conformance/B017.2-source-format-magics.md), [B017.3](../../artifacts/conformance/B017.3-child-notebook-outcomes.md), [B017.4](../../artifacts/conformance/B017.4-notebook-context.md), and [B017.5](../../artifacts/conformance/B017.5-notebook-compute-interruption.md). The selected behavior is local only and does not establish Databricks service/runtime parity.

## B018 — Prove SDK-driven job workflow

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B012, B014, B015, B016, B017, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** G2 workflow and optional-notebook conformance report.

**Acceptance criteria:**

1. SDK submits a Python transformation that writes Delta; a dependent task and external SQL/session read observe the same result.
2. Exercise parameters, secrets, task output, dependency failure, cancellation, and retry without cross-run state leakage.
3. Run a separate notebook-extra workflow and the base workflow without notebook dependencies, Java, or cloud compute.
4. Run the synthetic orders workflow under the bounded serverless and classic new/existing/shared-compute profiles and each SQL warehouse type, with separate notebook-extra variants, asserting real schema/data/output, resource lifetime, isolation and restart behavior.

**Required children:** B018.1, B018.2. All children and the original criteria above are required.

**Evidence:** B018.1 verifies the parameterized SDK producer/dependent reader, failures, retries, isolation, seven job compute selectors, and three local SQL warehouse profiles. B018.2 verifies separate built-wheel base and notebook-extra installation lanes with credentials, Java, and cloud compute unavailable. The notebook-extra lane also exercises SDK NotebookTask results across seven selectors and three source-export profiles. See [the aggregate B018 report](../../artifacts/conformance/B018-sdk-driven-workflow.md) and the linked child reports. Classic/warehouse resources remain simulated; this is local loopback evidence only.

## B019 — Expand SQL semantics with explicit routing

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** SQL capability corpus, routing policy, and selected mutation/utility support.

**Acceptance criteria:**

1. Add only enumerated operations with fixtures for nulls, coercion, decimals, dates/timezones, collections, and error categories.
2. SQL update/delete/merge use the shared provider and match DataFrame-visible history/schema.
3. Any DuckDB route is selected before execution and has profile-specific evidence; mismatches stay partial/unsupported.
4. Define separate serverless and classic runtime/access-mode cases for ANSI/timezone defaults, configuration scope, views/cache and error behavior; deferred-analysis/Connect cases remain explicitly unsupported until B036 proves them.

**Required children:** B019.1, B019.2, B019.3. All children and the original criteria above are required.

**Evidence:** All three required children are complete. B019.1 provides the enumerated local SQL/profile corpus; B019.2 routes the selected mutations, history and snapshots through the shared Delta provider; B019.3 records the fixed Sparkless/catalog/table-service route and selected utility/type limits, with no DuckDB route or exception-driven retry. The focused routing/provider suite passed 74 tests with Sparkless 4.15.0 and Databricks credential variables unset. The overall C018 semantics capability stays planned because only enumerated local cases are qualified and remote Databricks parity remains unclaimed. See [B019.1](../../artifacts/conformance/B019.1-sql-semantics-corpus.md), [B019.2](../../artifacts/conformance/B019.2-sql-mutations-history-snapshots.md), and [B019.3](../../artifacts/conformance/B019.3-executor-routing.md).

## B020 — Prove one CLI Terraform and bundle workflow

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B005, B014, B043.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Minimal versioned deployment fixtures and required API behavior.

**Acceptance criteria:**

1. Run the selected CLI workspace/job operations against a local server.
2. Apply/read/update/destroy the declared minimal Terraform resources with state/refresh semantics verified.
3. Validate/deploy/run one supported bundle fixture; label simulated infrastructure and reject unsupported resources explicitly.
4. Qualify selected classic compute and serverless/pro/classic warehouse resources plus serverless/classic bundle variants with real pinned clients, actual worker/query results, lifecycle errors and cleanup; metadata-only success cannot satisfy workload compatibility.

**Required children:** B020.1, B020.2, B020.3, B020.4, B020.5. All children and the original criteria above are required.

**Evidence:** All five children are complete. The pinned Databricks CLI workspace/job and classic/serverless bundle subsets passed; Terraform Core/provider apply/read/update/refresh/destroy covered workspace files, jobs, classic compute and serverless/Pro/classic warehouses. The B020.5 G2 bundles proved actual serverless Python and classic notebook worker output, shared Delta/SQL rows, update/redeploy, explicit invalid cases, and cleanup. Simulation and remote-compatibility boundaries are documented in each child report.

## B021 — Implement and prove SQL connector REST profile

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B005, B019, B045.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Pinned REST-mode SQL connector integration or a documented scope decision.

**Acceptance criteria:**

1. Connect the actual selected connector REST/kernel mode locally with verified TLS/auth configuration.
2. Exercise parameterized execute, fetching/schema, failure, result lifetime, and cancellation where advertised.
3. Record unsupported metadata/driver methods; if the released client cannot target the local service, leave implementation blocked or formally defer the release claim.

**Required children:** B021.1, B021.2. All children and the original criteria above are required.

**Evidence:** The released connector reaches the local API over strict TLS with synthetic auth, runs a named-parameter inline query, fetches its typed row, reports SQL and unsupported metadata failures, exercises synchronous cancellation, and closes results/sessions. Result expiry/eviction are covered by the SQL API regression suite. The kernel Arrow-stream/LZ4 query default remains explicitly unsupported, so C031 stays partial; no remote Databricks behavior is inferred. See [B021.2.3 evidence](../../artifacts/conformance/B021.2.3-sql-connector-failures-retention.md).

## B022 — Harden concurrent writes and crash recovery

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B018, B019, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** G3 failure matrix, reconciliation, backup/restore evidence.

**Acceptance criteria:**

1. Inject failures before/after Delta commit and SQLite finalization; verify recovery and uncertain-state handling.
2. Run concurrent SQL/DataFrame appends including location aliases; no lost updates or duplicated idempotent operations.
3. Exercise restart, safe retry, result/log retention, and coordinated backup/restore with actual files.
4. Exercise cancellation, compute/session restart, interrupted writes and safe retries under both required compute modes; preserve committed Delta versions and accurate attempts while simulated infrastructure transitions never fabricate completion.

**Required children:** B022.1, B022.2, B022.3, B022.4. All children and the original criteria above are required.

**Evidence:** All four acceptance criteria have criterion-level local evidence: B022.1 covers real process death around Delta/SQLite commit boundaries; B022.2 covers same-process SQL/DataFrame writes through location aliases and replay; B022.3.1–B022.3.2 cover restart, retention, and coordinated local restore with the external-reference policy; B018.1.2/B017.5/B022.4 cover classic interruption and serverless timeout/retry behavior while preserving committed writes. The combined affected recovery suite passed 141 tests; Ruff check/format, strict mypy across 70 source files, planning validation, and `git diff --check` passed. See [B022.1](../../artifacts/conformance/B022.1-commit-boundary-crashes.md), [B022.2](../../artifacts/conformance/B022.2-concurrent-alias-writes.md), [B022.3.1](../../artifacts/conformance/B022.3.1-restart-retention.md), [B022.3.2](../../artifacts/conformance/B022.3.2-coordinated-workspace-restore.md), and [B022.4](../../artifacts/conformance/B022.4-serverless-timeout-commit.md). The evidence is local only: cross-process/host and network-filesystem locking, external Python side effects, external snapshot writers, cloud provisioning, and Databricks runtime/service parity remain unqualified.

## B023 — Prepare alpha release candidate

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B020, B021, B022, B043, B044, B048.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Local distribution artifacts, generated capability docs, examples and release report.

**Acceptance criteria:**

1. Install built wheels on every advertised platform/Python version and run G1–G4 for the declared surface.
2. Run base workflow without JVM/cloud/Docker; run provisioned offline check and advertised extras separately.
3. Confirm notices, migrations, dependency inventory, public Sparkless artifact, truthful limitations, and owner release checklist; publication is a separate authorized action.
4. Include both bounded serverless/classic Python/notebook profiles and serverless/pro/classic warehouse execution in the 0.1.0 scope snapshot and G1-G4 evidence; keep mode/protocol/environment limits explicit and block release or formally revise scope when a required subset is unimplemented.

**Required children:** B023.1, B023.2, B023.3. All children and the original criteria above are required.

**Evidence (historical development candidate; superseded by B066):** B023.1–B023.3 complete the frozen candidate scope. The [owner-review bundle](../../artifacts/releases/0.1.0/README.md) retains actual `0.1.0.dev0` wheel/sdist artifacts, their hashes, generated capability docs, exact G1–G4 selectors, client/runtime versions, offline guard reports, local recovery/restore evidence, and the owner checklist. The exact current wheel passed 137 base and 175 notebook-extra cases per interpreter on macOS arm64 CPython 3.11.15 and 3.12.13 (624 selected matrix cases); its separate installed-wheel B064 suite passed 99 cases. Historical 132/169 counts remain tied to the prior wheel. Linux x86_64 remains unadvertised pending CI; the project's license remains unselected and blocks publication. No candidate was published.


Current 0.1.0 artifact hashes, platform qualification, Apache-2.0 metadata, and remaining publisher requirements are recorded by B066 in the [release bundle](../../artifacts/releases/0.1.0/README.md). These original B023 results apply only to their dated development artifacts.

## B024 — Expand workflow orchestration

**Milestone:** M4 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B018, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed epic for advanced DAG control, retries/repair, concurrency and schedules.

**Acceptance criteria:**

1. Specify and test each selected control-flow condition and retry/repair effect on task attempts/values.
2. Persist schedule/run metadata and prevent duplicate dispatch after restart for supported scheduling modes.
3. Keep unsupported task types explicit; do not run cloud/Java task types through silent substitutions.
4. Decompose and implement selected serverless performance/admission and automatic optimization retry policies, compute-object selection, and locked environment provisioning/cache/reset including versioned classic dependency modes; verify real local attempts and reject unsupported settings without simulating successful work.

## B025 — Integrate real MLflow tracking and registry

**Milestone:** M4 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B006, B007.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Decomposed ML extra epic with provider lifecycle and registry mapping.

**Acceptance criteria:**

1. Log/retrieve actual experiments, metrics, artifacts and Python model versions through the declared clients.
2. Restart providers and verify authoritative state, artifact paths and name mappings.
3. Keep UC permissions/aliases/registry URI support bounded by fixtures and exclude JVM-dependent flavors.
4. Qualify an exact full MLflow service/environment including matching skinny/tracing packages, explicit local database-backed registry/artifact/endpoints and coordinated provider restore; preserve provider authority and prove ordinary tests cannot discover personal profiles or redirect to cloud tracking.

## B026 — Add local Python model serving

**Milestone:** M4 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B025, B013.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Decomposed serving endpoint/worker epic.

**Acceptance criteria:**

1. Load a recorded Python model version and return real schema-validated predictions through declared API routes.
2. Exercise lifecycle, update, load failure, request errors, concurrency limits and cancellation.
3. Do not silently call remote model endpoints; capture native-only runtime evidence.
4. Qualify the selected native Python flavor/model environment with real model-version/schema/output and failure/cancellation/resource checks; reject Spark flavors and unprovisioned dependencies, and distinguish a Docker Python dependency from actually permitting a container execution backend.

## B027 — Integrate dbt without duplicate tables

**Milestone:** M4 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B019, B008.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Decomposed dbt-duckdb profile and Delta materialization epic.

**Acceptance criteria:**

1. Run an actual sources/refs/models/tests project against the shared committed table view.
2. Implement supported incremental/materialization behavior through the authoritative service and test restart/schema changes.
3. Publish separate dbt-duckdb and dbt-databricks capability claims; unsupported macros fail explicitly.
4. Resolve dbt-duckdb and any separately selected dbt-databricks client profiles in independent hash-pinned environments; the researched dbt-databricks 1.12.7 rejects core SDK 0.153.0 and connector 4.6.0, so do not downgrade or strip bounds to share an environment.
5. Prove registered materializations and incremental writes use the one table/commit service, reread schema/rows/versions after restart and reject unsupported SQL/macros/Python-model paths; a successful dbt-duckdb run cannot qualify dbt-databricks.

## B028 — Add local authorization and audit semantics

**Milestone:** M4 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B007, B016, B043.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Decomposed identity/grant/enforcement/audit epic.

**Acceptance criteria:**

1. Define identities/groups/ownership/inheritance and enforce the same decision across SQL/API/worker/file paths.
2. Verify permitted and denied operations with audit events and no unauthorized data results.
3. Document trusted-code filesystem bypass limits; row filters/masks require separate fixtures.
4. Decompose and enforce classic standard/dedicated compute use and distinct interactive/automated serverless compute-object permissions separately from SQL warehouse ACLs; prove allowed/denied jobs/notebooks/SQL paths with the same identities and audit authority.
5. Use pinned OSS Unity Catalog OpenAPI/generated-client contracts only for explicitly mapped operations through the existing catalog/policy authority; exclude its Java server, prove allowed/denied principal propagation and keep OSS-versus-managed UC differences visible.

## B029 — Add native vector index workflows

**Milestone:** M4 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B008, B028.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed LanceDB/provider and Databricks vector API epic.

**Acceptance criteria:**

1. Create/query/update/delete a persistent index with deterministic vectors and tested filter/score behavior.
2. Verify dimension errors, restart, authorization, and lifecycle transitions.
3. For Delta-sync support, prove source version tracking and recovery; no remote embeddings by default.

## B030 — Define and execute bounded micro-batches

**Milestone:** M5 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B010, B013, B043.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed streaming ports, bounded source/plan/sink epic.

**Acceptance criteria:**

1. Specify source identity/offset descriptor, batch ID, schema and supported output modes.
2. Execute a real bounded Sparkless batch into the table service with progress and cancellation.
3. Reject unsupported trigger/operator combinations and publish no unproven exactly-once claim.
4. Classify and test serverless bounded-trigger acceptance/rejection separately from classic trigger/runtime support; execute supported batches locally and retain explicit mode-specific errors for unsupported combinations.

## B031 — Implement durable checkpoints and replay

**Milestone:** M5 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B030, B022.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed checkpoint, lease and replay epic.

**Acceptance criteria:**

1. Persist offsets only after sink commit and reconcile a committed batch on restart.
2. Fault-inject every batch phase; the advertised source/sink guarantee holds without duplicate rows.
3. Reject competing owners, changed source identity and incompatible checkpoint/plan versions.

## B032 — Add file ingestion and Auto Loader subset

**Milestone:** M5 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B031.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed file discovery/schema registry/cloudFiles epic.

**Acceptance criteria:**

1. Ingest actual local files with durable discovery identity and restart-safe batch assignment.
2. Test file replacement, partial writes, duplicates, schema changes, bad records and backfill.
3. Declare supported cloudFiles options and distinguish polling from cloud notification mode.

## B033 — Implement declarative batch pipeline graph

**Milestone:** M5 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B018, B019, B043.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed dataset definitions, dependency graph and refresh epic.

**Acceptance criteria:**

1. Compile selected decorators into a validated dependency graph and materialize real Delta outputs.
2. Test cycles, dataset failures, refresh and restart with per-dataset version/run status.
3. Keep legacy dlt and newer pipelines imports distinct from the dltHub package; no fake whole-pipeline atomicity.
4. Resolve selected classic and serverless pipeline compute/environment settings through the shared services, apply the versioned mode restrictions, and verify real local batch outputs rather than accepting compute labels alone.

## B034 — Add expectations and selected CDC semantics

**Milestone:** M5 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B033, B031.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed expectation/CDC/SCD fixtures and implementation epic.

**Acceptance criteria:**

1. Record/drop/fail expectations produce correct data, metrics and run outcomes.
2. Test keys, sequence ties, deletes, null updates, out-of-order records and declared SCD behavior.
3. Replay and partial pipeline failure preserve the documented per-table guarantees.

## B035 — Evaluate and integrate stateful streaming

**Milestone:** M5 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B031.

**Plans:** [10-streaming-and-pipelines.md](10-streaming-and-pipelines.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed native provider selection, windows/state and optional broker epic.

**Acceptance criteria:**

1. Compare provider state/offset semantics against chosen Spark fixtures before committing to a dependency.
2. Implement selected windows/watermarks/dedup or joins with state eviction and restart fixtures.
3. Run G5 and advertise only tested operator/source/sink combinations; optional services remain explicit.

## B036 — Implement native Spark Connect subset

**Milestone:** M6 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B010, B019, B005, B043.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Decomposed gRPC/protobuf planning, execution and pure Python client epic.

**Acceptance criteria:**

1. Pin protocol/client versions and map supported unresolved plans to Sparkless with typed Arrow results.
2. Exercise actual unmodified pure Python client sessions, errors, cancellation and resource cleanup.
3. Prove no Spark server/JVM and isolate unsupported plan/UDF/session features; consider wheel split only after boundary criteria.
4. Prove reference-specific Spark Connect analysis/error timing, temporary-view rebinding and UDF capture with an unmodified pure Python client for serverless and applicable classic runtimes; keep compute mode independent of protocol and shared driver state.
5. Qualify an actual pure-Python pyspark-client artifact and resolved protobuf/gRPC/Arrow environment independently from full pyspark; verify packaging/install, namespace isolation, native server execution and exact protocol/error/result scope without inferring semantic parity from the 4.2.0 research candidate.

## B037 — Implement broader SQL driver protocols

**Milestone:** M6 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B019, B005.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed Thrift/metadata/session/result transport epic.

**Acceptance criteria:**

1. Pin actual driver protocol requirements and implement a minimal real connector workflow.
2. Test sessions, parameterization, result pagination/schema, metadata calls and cancellation for advertised methods.
3. Keep REST and Thrift claims separate; JDBC clients are external conformance tools, never a JVM runtime dependency.

## B038 — Research native feature engineering and MLlib coverage

**Milestone:** M6 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B025, B008.

**Plans:** [11-ml-and-extensions.md](11-ml-and-extensions.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed feasibility/fixture epic for temporal features and selected estimators.

**Acceptance criteria:**

1. Specify point-in-time joins, lineage/lookup and candidate estimator behavior independently.
2. Compare native results/parameters/serialization against versioned oracles for the chosen subset.
3. Record infeasible or incompatible behavior rather than exporting misleading class-name aliases.

## B039 — Add Delta Sharing protocol subset

**Milestone:** M6 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B008, B028.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Decomposed sharing server/client and access lifecycle epic.

**Acceptance criteria:**

1. Pin the protocol and demonstrate a real independent client reading an authorized shared snapshot.
2. Validate schema/version/file result lifetime, revocation and unsupported Delta features.
3. Prevent arbitrary filesystem disclosure and preserve no-JVM server execution.
4. Qualify an exact non-Spark Delta Sharing client plus native wrapper/PyArrow/platform matrix, including real snapshot reads, credentials, revocation, expired file/results and feature errors; exclude Spark reader methods/reference JVM serving and do not generalize client metadata to native server parity.

## B040 — Inventory and close remaining platform gaps

**Milestone:** M6 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B005, B007, B043.

**Plans:** [00-vision-and-scope.md](00-vision-and-scope.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [13-decisions-and-risks.md](13-decisions-and-risks.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Decomposed platform frontier for dashboards/Genie/accounts/Lakebase/other domains.

**Acceptance criteria:**

1. Inventory public observable contracts by product/version and classify native feasibility.
2. For each selected slice, define a real workflow, authority and oracle before implementing.
3. Keep absent proprietary/cloud/Scala/JAR behavior visibly unsupported; simulated infrastructure is labeled.
4. Retain explicit serverless/classic workload, warehouse, access-mode, runtime/environment and cloud/version gaps after the bounded earlier releases; do not use this frontier epic to defer their required 0.1 behavior silently.
5. Evaluate explicitly selected native PyIceberg and Delta Kernel candidates with bounded read/snapshot/feature/ownership fixtures and a viable/blocked provider decision before any adoption; keep Delta/Iceberg identities distinct and retain the existing mutation authority without automatic dual writes or new support claims from source inspection.

## B041 — Expand performance, platforms and storage adapters

**Milestone:** M6 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B023.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Decomposed benchmarks/wheel/platform/scaling and optional cloud-storage adapter epic.

**Acceptance criteria:**

1. Record repeatable cold/warm latency, memory, conversion and ETL baselines with data/hardware/version metadata.
2. Add platforms only after native wheel and golden workflow evidence.
3. Improve bottlenecks without changing declared semantics; no unsupported acceleration claims.
4. Prove advertised Python and native storage paths against pinned optional Moto/Azurite services; record URI/API limits and do not infer ADLS Gen2 compatibility.
5. Qualify native/transitive wheel and credential/IO behavior for each selected optional provider/client combination, including Delta Sharing wrapper architecture/libc gaps and Arrow/Kernel version differences; no unadvertised source-build requirement may substitute for the claimed install matrix.

## B042 — Prepare expanded conformance release

**Milestone:** M6 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B023, B024, B025, B026, B027, B028, B029, B032, B034, B035, B036, B037, B038, B039, B040, B041, B043.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Decomposed broad release readiness epic, scope record and versioned evidence snapshot.

**Acceptance criteria:**

1. Run applicable G1–G6 and report supported/partial/simulated/unsupported cases by domain/profile.
2. Verify migrations, installation, native-only execution, notices, examples and optional-provider version combinations.
3. Retain unresolved frontier requirements; any narrower release explicitly revises scope and leaves deferred tasks open.
4. Consolidate separate serverless/classic workload and three-warehouse-type case counts, version combinations, simulation labels and unresolved protocol/environment/authorization gaps in the release snapshot.

## B001.1 — Create the package and lazy CLI entry point

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B001 · **Parent criteria:** 2, 3.

**Dependencies:** None.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** src/brickhouse, build metadata, and help/version entry points.

**Acceptance criteria:**

1. Create the single-distribution src layout and CLI help/version without importing optional integrations or starting services; preserve all planning documents.

## B001.2 — Build and install distribution artifacts

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B001 · **Parent criteria:** 1, 2.

**Dependencies:** B001.1.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Wheel/sdist and clean-environment import/CLI evidence.

**Acceptance criteria:**

1. Build wheel and sdist, install the wheel into a fresh environment, and record import/help/version results without editable-checkout assumptions.

## B001.3 — Establish development and planning validation tooling

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B001 · **Parent criteria:** 3.

**Dependencies:** B001.2.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Development configuration, manifest checks, and dependency/platform baseline.

**Acceptance criteria:**

1. Record initial dependency/Python/platform candidates and artifact retention policy; provide focused development checks and task/capability/link validation, including child completion rules.

## B002.1 — Inventory pinned Minilake source and coupling

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B002 · **Parent criteria:** 1.

**Dependencies:** B001.

**Plans:** [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Pinned file/license/state/Spark dependency inventory.

**Acceptance criteria:**

1. Inspect the exact pinned source and record proposed contracts/files, licenses, globals, persistence ownership, and Spark launch/write paths.

## B002.2 — Decide reuse boundaries and provenance requirements

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B002 · **Parent criteria:** 2, 3.

**Dependencies:** B001, B002.1.

**Plans:** [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Reuse ADR, owned replacements, notice plan, and contract-test inventory.

**Acceptance criteria:**

1. Choose adaptation or fresh implementation for initial catalog/jobs/files behavior and specify removed paths, required tests, and notices before copying source; justify any no-reuse decision.

## B003.1 — Inspect actual Sparkless extension APIs

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B003 · **Parent criteria:** 1.

**Dependencies:** B001.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Current Rust/Python API inventory mapped to S001-S006.

**Acceptance criteria:**

1. Read the actual engine and bindings, distinguish public existing APIs from missing hooks, and record the inspected source revision.

## B003.2 — Probe external relations writers and typed interchange

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B003 · **Parent criteria:** 2.

**Dependencies:** B001, B003.1.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Runnable isolated feasibility probes for provider binding/write/Arrow contracts.

**Acceptance criteria:**

1. Demonstrate viable catalog/relation binding, delegated writes, and typed interchange using real native data; label temporary adaptations and do not treat proposed signatures as released APIs.

## B003.3 — Resolve session and callback delivery risks

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B003 · **Parent criteria:** 2, 3.

**Dependencies:** B001, B003.2.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Session-isolation probe, callback contract, and smallest upstream task proposal.

**Acceptance criteria:**

1. Exercise independent sessions and record callback ownership, GIL/thread/reentrancy, cancellation/error propagation, and the smallest required upstream change or precise blocker.

## B004.1 — Probe native Delta mutation candidates

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B004 · **Parent criteria:** 1.

**Dependencies:** B001.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Exact-version candidate matrix with real Delta mutation fixtures.

**Acceptance criteria:**

1. Run create/read/append/update/delete/merge on actual files for candidate providers and record unsupported operations rather than fabricating success.

## B004.2 — Cross-read the selected type and protocol envelope

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B004 · **Parent criteria:** 2.

**Dependencies:** B001, B004.1.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Sparkless/SQL cross-read fixtures and feature rejection matrix.

**Acceptance criteria:**

1. Cross-read committed snapshots through Sparkless and the selected SQL path, covering nulls, decimals, timestamps, and the declared scalar/nested subset with exact provider versions.

## B004.3 — Select mutation provider and recovery identity

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B004 · **Parent criteria:** 3.

**Dependencies:** B001, B004.2.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Provider ADR, transaction-identity probe, and recovery policy.

**Acceptance criteria:**

1. Probe crash-after-commit reconciliation and transaction identity; select one mutation provider, protocol/feature limits, and explicit uncertain-outcome behavior where identity cannot be reconciled.

## B005.1 — Pin clients semantic profiles and transport requirements

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B005 · **Parent criteria:** 1, 2, 3.

**Dependencies:** B001.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [15-personal-databricks-verification.md](15-personal-databricks-verification.md), [0001-personal-databricks-reference-verification.md](../decisions/0001-personal-databricks-reference-verification.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Exact client/profile matrix and minimum SDK/deployment/connector workflows.

**Acceptance criteria:**

1. Inspect actual released SDK/CLI/Terraform/SQL connector artifacts, pin versions, separate REST/kernel and default Thrift requirements, and enumerate the minimal workflows and fixture provenance.

## B005.2 — Configure bounded personal-account reference scope

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B005 · **Parent criteria:** 3, 4.

**Dependencies:** B001, B005.1.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [15-personal-databricks-verification.md](15-personal-databricks-verification.md), [0001-personal-databricks-reference-verification.md](../decisions/0001-personal-databricks-reference-verification.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Sanitized capture profile with host alias, observed features, and resource limits.

**Acceptance criteria:**

1. Record the actual personal workspace feature/version scope, permitted existing compute, scratch ownership, limits, cleanup, and credential-variable sources without retaining credentials or assuming a budget.

## B005.3 — Capture and sanitize a real reference behavior

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B005 · **Parent criteria:** 5.

**Dependencies:** B001, B005.2.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [15-personal-databricks-verification.md](15-personal-databricks-verification.md), [0001-personal-databricks-reference-verification.md](../decisions/0001-personal-databricks-reference-verification.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Named probe tooling, reviewed synthetic fixture, and local test design.

**Acceptance criteria:**

1. Execute at least one small synthetic reference operation outside test discovery; retain exact schema/results or errors, client/session provenance, sanitization rules, and cleanup evidence, then map it to a deterministic local regression design.

## B005.4 — Guard local fixtures and clients against remote authentication

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B005 · **Parent criteria:** 7.

**Dependencies:** B001, B005.1.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [15-personal-databricks-verification.md](15-personal-databricks-verification.md), [0001-personal-databricks-reference-verification.md](../decisions/0001-personal-databricks-reference-verification.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Credential-free fixture checks and local-client host/auth isolation checks.

**Acceptance criteria:**

1. Prove fixture tooling runs without personal credentials, ordinary clients do not load .env or discover real-workspace profiles, and inherited credentials cannot redirect local calls; explicit capture fails clearly when configuration is absent.

## B005.5 — Add the optional manual reference-capture workflow

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Parent:** B005 · **Parent criteria:** 6.

**Dependencies:** B001, B005.3, B005.4.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [15-personal-databricks-verification.md](15-personal-databricks-verification.md), [0001-personal-databricks-reference-verification.md](../decisions/0001-personal-databricks-reference-verification.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Restricted workflow_dispatch workflow and sanitized artifact handling.

**Acceptance criteria:**

1. Restrict capture to reviewed default-branch code, curated probes, permitted resources, job-scoped credentials, serialized bounded execution, and sanitized uploads; exclude automatic PR/push/schedule events and required CI dependence.

## B006.1 — Define workspace configuration namespaces and schema

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B006 · **Parent criteria:** 1, 2.

**Dependencies:** B001.

**Plans:** [01-architecture.md](01-architecture.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [ADR 0005](../decisions/0005-workspace-bootstrap-format.md).

**Deliverable:** Typed configuration, workspace format, migrations, and namespace policy.

**Acceptance criteria:**

1. Initialize and reopen a real workspace with explicit bootstrap namespaces and supported config/format versions; reject invalid configuration and incompatible formats.

## B006.2 — Enforce service ownership and independent persistence

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B006 · **Parent criteria:** 2, 3.

**Dependencies:** B001, B006.1.

**Plans:** [01-architecture.md](01-architecture.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [ADR 0006](../decisions/0006-workspace-ownership-and-migrations.md).

**Deliverable:** Workspace owner lock, transactional SQLite migration runner, and shutdown/reopen evidence.

**Acceptance criteria:**

1. Enforce one service owner including stale-owner recovery, isolate two workspace directories, and verify transactional migrations and deterministic shutdown.

## B006.3 — Expose equivalent CLI and embedded lifecycle

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B006 · **Parent criteria:** 1, 3.

**Dependencies:** B001, B006.2.

**Plans:** [01-architecture.md](01-architecture.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** init/serve/run lifecycle composition and embedded Workspace context.

**Acceptance criteria:**

1. Compose CLI and embedded lifecycle through the same instance-scoped services with clean shutdown; expose truthful capability/version/provider diagnostics without marking planned behavior supported.

## B007.1 — Persist stable catalog and table identities

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Current ledger status:** done.

**Parent:** B007 · **Parent criteria:** 3.

**Dependencies:** B002, B005, B006.

**Plans:** [01-architecture.md](01-architecture.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Catalog/schema/table repositories with canonical ownership constraints.

**Acceptance criteria:**

1. Persist stable IDs, canonical table locations, and managed/external ownership; reject aliases with conflicting registrations and preserve identities across reopen.

## B007.2 — Bind identifiers and isolated namespace defaults

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Current ledger status:** done.

**Parent:** B007 · **Parent criteria:** 2.

**Dependencies:** B002, B005, B006, B007.1.

**Plans:** [01-architecture.md](01-architecture.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Identifier parser/resolver and per-session namespace contract.

**Acceptance criteria:**

1. Resolve quoted and multipart identifiers and session defaults without prefix stripping or cross-session namespace leakage; exercise missing and ambiguous targets.

## B007.3 — Implement SDK catalog and schema workflows

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Current ledger status:** done.

**Parent:** B007 · **Parent criteria:** 1.

**Dependencies:** B002, B005, B006, B007.2.

**Plans:** [01-architecture.md](01-architecture.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Catalog/schema service operations and pinned SDK transport checks.

**Acceptance criteria:**

1. Use the real pinned SDK for create/list/get/delete catalogs and schemas, with actual persistence and selected pagination/duplicate/missing/error fixtures.

## B008.1 — Define table handles snapshots and staging

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

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

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B008, B009, B043, B046.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [0018-published-sparkless-python-provider.md](../decisions/0018-published-sparkless-python-provider.md).

**Deliverable:** Engine adapter/session factory and typed snapshot reader.

**Acceptance criteria:**

1. Use the actual tested Sparkless artifact to bind shared table snapshots with the selected type envelope and explicit session configuration.

**Parent:** B010 · **Parent criteria:** 3.

**Evidence:** The internal factory binds isolated Sparkless 4.14.1 sessions to registered, version-pinned local Delta snapshots with explicit profile settings and the tested decimal/array/nullable-struct envelope. Five focused tests passed; the full local suite passed 189 tests with three opt-in client integrations skipped. See [B010.1 conformance](../../artifacts/conformance/B010.1-sparkless-read-adapter.md). B010.2 adds selected writer routing through the table service.

## B010.2 — Route DataFrame writes through the table service

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B008, B009, B010.1, B043, B046.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Named-table/path writer delegation and cross-session read fixture.

**Acceptance criteria:**

1. Write a managed table through the explicit Python API, read it by full name in a fresh session, and prove supported path saves share the service while bypasses fail clearly.

**Evidence:** Managed creation, named append/overwrite through Sparkless' public callback, fresh-session reads, and explicit `session.write_path()` writes to a registered location use the same `DeltaRsTableService`. Direct `.save(path)`, insert writers, DataFrameWriterV2, unsupported options, mixed SQL statements, unregistered names, stale snapshot writes, and changed operation payloads fail clearly. Thirteen focused Sparkless/table-service tests passed; the full local suite passed 191 tests with three opt-in CLI/Terraform integrations skipped. Ruff, formatting, strict mypy across 49 source files, `pip check`, planning validation, and package build passed. See [B010.2 conformance](../../artifacts/conformance/B010.2-sparkless-write-adapter.md). B010.3 subsequently completed the parent session-state/profile criteria; see [its conformance report](../../artifacts/conformance/B010.3-sparkless-session-isolation.md).

**Parent:** B010 · **Parent criteria:** 1, 2.

## B010.3 — Verify independent concurrent session state

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B008, B009, B010.2, B043, B046.

**Plans:** [02-packages-and-public-api.md](02-packages-and-public-api.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Two coexisting, profile-bound sessions with isolated namespace/config/view/UDF state and typed interchange evidence.

**Acceptance criteria:**

1. Exercise two coexisting sessions with different namespaces, temporary views, configuration, and distinct callable UDFs; assert no state leakage and preserve each result schema and values.
2. Apply a resolved serverless profile and a mapped classic-standard runtime profile to those sessions; verify serverless ANSI/UTC defaults, require explicit classic ANSI/timezone settings, and reject unmapped settings or runtimes without claiming parallel query or Spark Connect behavior.

**Parent:** B010 · **Parent criteria:** 3, 4.

**Evidence:** Nineteen focused adapter/execution-context tests and the full suite (201 passed, 3 opt-in integrations skipped) passed. The serverless context applies ANSI/UTC defaults; classic-standard `spark35-local-v1` requires explicit ANSI/timezone settings. Two coexisting owner-thread sessions retain separate namespaces, configs, temp views and typed callable UDF results. Unmapped settings, missing classic defaults and unmapped runtimes fail explicitly. Close releases the native session on its owner thread; the full suite completed without the earlier PyO3 cross-thread-drop warning. Ruff, formatting, strict mypy (49 source files), `pip check`, wheel/sdist build, planning validation and diff checks passed. See [B010.3 conformance](../../artifacts/conformance/B010.3-sparkless-session-isolation.md).

## B011.1 — Parse bind and route the initial SQL corpus

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B011 · **Parent criteria:** 1, 3, 4.

**Dependencies:** B005, B010, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** AST classifier/binder and service/query routing.

**Acceptance criteria:**

1. Implement the selected SELECT/DDL cases over registered tables with bound identifiers; reject unsupported syntax before side effects and prohibit exception-driven executor fallback.

**Evidence:** Sixteen local conformance tests passed using Python 3.11.14, SQLGlot 30.24.0, Sparkless 4.14.1, delta-rs 1.2.1 and PyArrow 23.0.1. Real local Delta fixtures cover SELECT projection/filter, COUNT/SUM grouping, one inner equijoin, schema/table DDL, CTAS, canonical identifier binding, no-op/duplicate DDL, and pre-effect rejection/error propagation. No Databricks credentials were available to the test process. See [B011.1 conformance](../../artifacts/conformance/B011.1-sql-initial-corpus.md).

## B011.2 — Expose statement lifecycle and typed JSON results

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done.

**Parent:** B011 · **Parent criteria:** 1, 2.

**Dependencies:** B005, B010, B011.1, B011.5, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** SDK submit/poll/result transport with persisted lifecycle state.

**Acceptance criteria:**

1. Use the pinned SDK to submit and poll actual statements, returning the expected schema and JSON row encoding from the authoritative data.

**Evidence:** Sixty-three local conformance tests passed with databricks-sdk 0.153.0 against loopback, Sparkless 4.14.1, delta-rs 1.2.1, PyArrow 23.0.1, and metadata schema 14. Tests submit/poll actual statements over committed local Delta data, verify typed JSON results, errors, row/byte caps, restart persistence, and v12-to-v14 migration with Databricks credentials unset. See [B011.2 evidence](../../artifacts/conformance/B011.2-sql-statement-api.md).

## B011.3 — Implement parameter errors timeouts and cancellation

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B011 · **Parent criteria:** 2, 3, 4.

**Dependencies:** B005, B010, B011.2, B011.5, B043, B011.6.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Bound-parameter handling and explicit terminal/error semantics.

**Acceptance criteria:**

1. Exercise supported parameter forms, invalid requests, engine errors, wait/timeout behavior, and cancellation without interpolation or fabricated successful completion.

**Required children:** B011.3.1, B011.3.2, B011.3.3. All children are complete. Active cancellation uses Sparkless 4.15.0's public cancellation handle and terminal exception; BrickHouse retains active admission until execution reaches its actual terminal outcome.

## B011.3.1 — Validate and bind named scalar SQL parameters

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B011.3 · **Parent criteria:** 1.

**Dependencies:** B005, B010, B011.2, B011.5, B043, B011.6.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** API validation and typed AST binding for named primitive scalar parameters.

**Acceptance criteria:**

1. Bind named Databricks parameter markers without SQL text interpolation; support documented NULL and selected primitive values; reject positional, missing, duplicate, unused, malformed, and unsupported complex parameters before execution; prove typed results and injection-shaped strings with real local queries.

**Evidence:** Ninety-two focused local tests passed with `databricks-sdk==0.153.0` against loopback, Sparkless 4.14.1, delta-rs 1.2.1, PyArrow 23.0.1, and metadata schema 15. Real parameterized SDK statements verify typed values, NULL, injection-shaped literal data, rejection before statement creation, and pending parameter persistence across workspace reopen. Ruff, format, strict mypy, and planning validation passed with credentials unset. See [B011.3.1 evidence](../../artifacts/conformance/B011.3.1-sql-parameter-binding.md).

## B011.3.2 — Implement hybrid wait-timeout continuation

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B011.3 · **Parent criteria:** 1.

**Dependencies:** B005, B010, B011.2, B011.5, B043, B011.6, B011.3.1.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Pinned Statement Execution hybrid wait behavior with persisted asynchronous continuation.

**Acceptance criteria:**

1. Match omitted 10s, explicit 0s, and valid 5–50s wait settings with `CONTINUE`; return a truthful pending/running statement ID when the wait expires and execute it exactly once; reject invalid timeout and unsupported wait dispositions before admission.

**Evidence:** Thirty-two focused local SQL API/service tests passed with `databricks-sdk==0.153.0` against loopback, Sparkless 4.14.1, delta-rs 1.2.1, PyArrow 23.0.1, SQLite 3.53.4, and metadata schema 15. They verify default 10-second wait, immediate 0-second response, valid 5–50-second `CONTINUE`, exact-once completion after the 5-second wait expires, persisted continuation, polling, and validation before admission. Ruff check/format, strict mypy, and planning validation passed with credentials unset. See [B011.3.2 evidence](../../artifacts/conformance/B011.3.2-sql-wait-timeout.md).

## B011.3.3 — Implement explicit and timeout cancellation

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B011.3 · **Parent criteria:** 1.

**Dependencies:** B005, B010, B011.2, B011.5, B043, B011.6, B011.3.2.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Explicit cancel and `on_wait_timeout=CANCEL` semantics that reconcile real engine outcomes.

**Acceptance criteria:**

1. Cancel queued work immediately and active work only when the engine reports a terminal outcome; retain leases while execution can still mutate data, preserve already committed writes, and test cancel/finish/warehouse-stop races without false cancellation or success.

**Evidence:** Fifty-one focused SQL, warehouse, and Sparkless tests passed against Sparkless 4.15.0. The tests cover queued cancellation, active native cancellation, actual `on_wait_timeout=CANCEL`, warehouse-stop cancellation, safe lease retention, completion/cancellation races, stale-generation admission fencing, and a committed CTAS that remains successful after cancellation arrives beyond the commit fence. Ruff check/format and strict mypy passed with Databricks credential variables unset. See [the cancellation conformance report](../../artifacts/conformance/B011.3.3-sparkless-4.15.0-cancellation.md).

## B011.4 — Bound statement results and retention

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B011 · **Parent criteria:** 2, 3, 4.

**Dependencies:** B005, B010, B011.3, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Documented row/byte/lifetime limits and result cleanup.

**Acceptance criteria:**

1. Choose recorded result limits and retention, test expiry/oversize behavior, and reject unsupported formats/dispositions without arbitrary filesystem reads or unbounded global result storage.

**Evidence:** Sixty-one focused workspace and SQL tests passed with metadata schema 17. Tests verify request-level row/byte prefixes, oversized-result failure without partial retention, 1,000-row chunks, expiry, oldest-first eviction under the 64 MiB workspace budget, legacy schema-16 result migration into bounded chunks, and rejection of unsupported formats/dispositions before admission. Ruff check, Ruff format check, and strict mypy passed; Databricks credentials were unset. See [B011.4 result limits and retention evidence](../../artifacts/conformance/B011.4-sql-result-limits-and-retention.md).

## B012.1 — Prove shared table reads through restart

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B012 · **Parent criteria:** 1.

**Dependencies:** B011.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Runnable G1 DataFrame/SDK/fresh-session/restart fixture.

**Acceptance criteria:**

1. Write through Sparkless, query through the real SDK, read in a fresh session, restart the server, and compare committed rows/schema and table identity.

**Evidence:** One local end-to-end test passed. It writes typed orders through Sparkless into managed Delta version 0, queries through databricks-sdk over loopback, checks a fresh Sparkless session, shuts down/reopens the local service and workspace, then verifies the same table ID/location/schema/rows/version, retained statement result, and a fresh post-restart query. Ruff check/format passed; credentials were unset. See [B012.1 G1 evidence](../../artifacts/conformance/B012.1-g1-shared-table-restart.md).

## B012.2 — Prove G1 boundary and native-execution cases

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B012 · **Parent criteria:** 2, 3.

**Dependencies:** B011, B012.1.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** G1 boundary corpus and native-only process/network report.

**Acceptance criteria:**

1. Exercise empty data, schema mismatch, external drop, and stale metadata invalidation while proving no Java/full PySpark/Docker or cloud access during the local workflow.

**Required children:** B012.2.1, B012.2.2, B012.2.3. All children preserve this acceptance criterion.

**Evidence:** All three children passed. Boundary tests preserve empty-table schemas, reject schema changes before commit, preserve external bytes, and reject stale service/session access. The G1 integration test passed under the loopback-only OS sandbox with Databricks credentials unset and no forbidden dependencies/processes. See the [B012.2.1](../../artifacts/conformance/B012.2.1-empty-and-schema-mismatch.md), [B012.2.2](../../artifacts/conformance/B012.2.2-external-drop-and-stale-metadata.md), and [B012.2.3](../../artifacts/conformance/B012.2.3-native-only-process-network.md) reports.

## B012.2.1 — Verify empty table and schema mismatch boundaries

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B012.2 · **Parent criteria:** 1.

**Dependencies:** B011, B012.1.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Local tests for empty managed Delta tables and rejected schema-mismatch writes.

**Acceptance criteria:**

1. Create and read an empty typed table through the selected Sparkless/Delta path without fabricating rows.
2. Reject a schema-mismatched append or overwrite before commit and preserve the prior Delta version and rows.

**Evidence:** Sparkless 4.15.0 created and read a typed empty managed Delta table at version 0. A schema-mismatched append raised the public `SparklessError` with category `incompatible_table_schema`; the table remained at version 0 with the original row. Ten local adapter tests passed; credentials were unset. See [B012.2.1 evidence](../../artifacts/conformance/B012.2.1-empty-and-schema-mismatch.md).

## B012.2.2 — Verify external drop and stale metadata invalidation

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B012.2 · **Parent criteria:** 1.

**Dependencies:** B011, B008, B010, B012.1.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Local tests for external table ownership and stale Sparkless/catalog resolution.

**Acceptance criteria:**

1. Dropping an external registration preserves all files at its external location.
2. After dropping or invalidating registered metadata, a previously resolved handle/session cannot read or mutate through stale state.

**Evidence:** External deregistration preserved the external directory and sentinel bytes; a captured handle failed reads and writes after deregistration. An already-open Sparkless session rejected a subsequent read and append after its managed table was dropped. Sixteen focused local tests passed; credentials were unset. Revocation of a DataFrame resolved before drop is not claimed. See [B012.2.2 evidence](../../artifacts/conformance/B012.2.2-external-drop-and-stale-metadata.md).

## B012.2.3 — Capture native-only and local-network boundary evidence

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B012.2 · **Parent criteria:** 1.

**Dependencies:** B011, B012.1.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Repeatable local workflow guard and process/dependency evidence report.

**Acceptance criteria:**

1. Run the selected G1 workflow with Databricks credentials unset and only explicit loopback service access permitted.
2. Observe the workflow process/dependency boundary and fail on Java, full PySpark, Docker, or remote-workspace access.

**Evidence:** `scripts/verify_g1_local_only.py` passed the G1 integration test under macOS `sandbox-exec` with outbound access denied except `localhost:*`. It strips Databricks credentials, checks forbidden distributions, and installs audit guards for network, import and process events. Nine connections and nine resolutions all used `127.0.0.1`; no forbidden event was observed. See the [native-only report](../../artifacts/conformance/B012.2.3-native-only-process-network.md) and [machine-readable audit](../../artifacts/conformance/B012.2.3-native-only-network-guard.json).

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

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

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

**Historical evidence (superseded by B064):** B014.3.2.1 persists distinct attempts and retry outputs. The original B014.3.2.2 implementation enforced a per-job 1–4 limit and four-run process cap, returned pinned-SDK `ResourceConflict` at capacity, and replayed active/terminal tokens without adding runs. The current policy is at most two active attempts plus eight FIFO queued runs, per-job limit 1–10, stable reset identity, and restart recovery of undispatched work. See [original B014.3.2.2 evidence](../../artifacts/conformance/B014.3.2.2-run-admission.md) and [B064 current evidence](../../artifacts/conformance/B064-phase-0.1-review-fixes.md).

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

**Historical evidence (superseded by B064):** The pinned-SDK tests verified two active runs under a per-job limit of two, capacity rejection, the original four-run process limit, token replay, and changed-input conflict. The current queue behavior and restart/reset identity are verified in [B064 evidence](../../artifacts/conformance/B064-phase-0.1-review-fixes.md); the earlier test report remains at [B014.3.2.2](../../artifacts/conformance/B014.3.2.2-run-admission.md).

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

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B010, B013, B043.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md).

**Deliverable:** Worker-only import bootstrap and representative transformation.

**Acceptance criteria:**

1. Run the selected pyspark import script against Sparkless/shared tables while parent/other environments retain their own modules; reject conflicts and unsupported namespaces clearly.

**Parent:** B015 · **Parent criteria:** 1, 2, 4.

**Required children:** B015.1.1, B015.1.2, B015.1.3. These slices separately cover the worker import namespace, profile-bound owner-thread SparkSession, and authenticated resolution of shared registered tables.

## B015.1.1 — Install a worker-only PySpark import namespace

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B015.1 · **Parent criteria:** 1.

**Dependencies:** B010, B013, B043.

**Deliverable:** Child-process-only pyspark.sql compatibility modules backed by Sparkless exports.

**Acceptance criteria:**

1. Support the selected pyspark, pyspark.sql, pyspark.sql.functions, pyspark.sql.types, and pyspark.sql.window imports from public Sparkless exports; reject an installed conflicting pyspark package and unsupported namespaces such as pyspark.rdd, pyspark.ml, and pyspark.java_gateway with explicit BrickHouse errors.
2. Run actual scripts in fresh workers to prove import module state does not alter the parent interpreter or another process; do not rewrite user source or add a top-level pyspark distribution.

**Evidence:** Five worker-import cases passed using fresh processes and Sparkless 4.14.1. The selected module exports resolve to the public Sparkless objects; unsupported namespaces and a conflicting import-path package fail explicitly; worker module state does not leak to the parent or another worker. Eleven existing worker tests also passed, including a check that the child retains its configured virtual-environment interpreter. Ruff check/format and strict mypy passed with Databricks credentials unset. See [B015.1.1 conformance](../../artifacts/conformance/B015.1.1-worker-pyspark-imports.md).

## B015.1.2 — Bind imported SparkSession to the selected worker profile

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B015.1 · **Parent criteria:** 1.

**Dependencies:** B010, B013, B043, B015.1.1.

**Deliverable:** Owner-thread Sparkless session facade with validated serverless/classic settings and cleanup.

**Acceptance criteria:**

1. Expose the selected SparkSession builder/creation subset, apply the worker effective settings before user execution, close the native session on its creating thread, and turn unsupported JVM/RDD calls and profile configuration into explicit BrickHouse errors.

**Evidence:** All three required child slices passed: worker-only Sparkless imports, profile-bound owner-thread sessions, and attempt-authenticated registered-table reads. The pinned-SDK loopback test read a real committed table through DataFrame and SQL paths, rejected missing and unregistered tables/paths plus mutation attempts, and retained Delta version 0. Worker auth, secrets, task values, and all seven selected compute variants passed in the focused set; Ruff check/format and strict mypy passed with Databricks credentials unset. See [B015.1.3 conformance](../../artifacts/conformance/B015.1.3-worker-registered-tables.md) and the [B015.1.2 session report](../../artifacts/conformance/B015.1.2-worker-spark-session.md).

## B015.1.3 — Resolve registered tables for worker Sparkless sessions

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B015.1 · **Parent criteria:** 1.

**Dependencies:** B010, B013, B043, B015.1.2.

**Deliverable:** Attempt-authenticated table descriptor lookup backed by the parent catalog and shared local Delta storage.

**Acceptance criteria:**

1. Resolve only registered table identities through the authenticated worker parent service, return version-pinned public Sparkless descriptors, and read shared committed Delta rows in a real SDK job; reject missing or unsupported tables and preserve the common mutation authority.

**Evidence:** Fifty focused tests passed with Databricks credentials unset. A real pinned-SDK loopback job read registered committed rows through `SparkSession.table()` and SQL, rejected a missing table and an actual unregistered Delta path, rejected DataFrame and SQL mutations, and left the common Delta table at version 0. The provider used Sparkless 4.14.1's public external-catalog descriptor and the attempt-authenticated parent service. Ruff check/format and strict mypy passed. No new Sparkless issue was needed; #1705 and #1706 remain the relevant open upstream gaps. See [B015.1.3 conformance](../../artifacts/conformance/B015.1.3-worker-registered-tables.md).

## B015.2 — Delegate supported DeltaTable facade operations

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B010, B013, B015.1, B043.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md).

**Deliverable:** Worker-scoped delta.tables subset backed by the common provider.

**Acceptance criteria:**

1. Exercise the selected DeltaTable methods/builders through the table service with canonical paths/commit identity; prohibit delta-spark or JVM execution and independent registered-table writers.
2. The initial worker surface is limited to DeltaTable.forName, forPath, isDeltaTable, toDF, and alias; mutations, merge builders, arbitrary paths, and JVM-backed behavior must fail explicitly.

**Required children:** B015.2.1, B015.2.2.

These bound the worker import/factory boundary separately from read-only methods and explicit unsupported mutation behavior.

**Evidence:** The selected facade passed 53 focused worker/SDK loopback tests with Databricks credential variables unset. It read committed registered rows through `forName`, `forPath`, `toDF`, and `alias`; verified `isDeltaTable`; rejected temporary views and unregistered paths; and kept the table at Delta version 0 while rejecting mutations. Ruff check/format and strict mypy passed. See [B015.2 conformance](../../artifacts/conformance/B015.2-worker-delta-table.md).

**Parent:** B015 · **Parent criteria:** 3.

## B015.2.1 — Add worker-only DeltaTable imports and registered factories

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B015.2 · **Parent criteria:** 1, 2.

**Dependencies:** B010, B013, B043, B015.1, B015.1.3.

**Deliverable:** A managed-worker-only delta.tables.DeltaTable import and registered-name/path lookup factories.

**Acceptance criteria:**

1. Support only delta.tables.DeltaTable imports inside managed workers; reject conflicting installed Delta Lake packages and unsupported delta namespaces without changing the parent interpreter.
2. Implement forName, forPath, and isDeltaTable using the attempt-authenticated parent service and existing table registrations; reject unregistered paths and unsupported options.

**Evidence:** Worker-only `delta.tables.DeltaTable` imports and the three registered-table factories passed the focused suite; conflicting `delta-spark` and unsupported `delta` namespaces are rejected without replacing the parent interpreter. See [B015.2 conformance](../../artifacts/conformance/B015.2-worker-delta-table.md).

## B015.2.2 — Expose read-only DeltaTable facade methods

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B015.2 · **Parent criteria:** 1, 2.

**Dependencies:** B010, B013, B015.1, B043, B015.2.1.

**Deliverable:** The selected toDF and alias methods backed by the worker Sparkless session, with explicit errors for unsupported mutations/builders.

**Acceptance criteria:**

1. Return the registered table through the existing Sparkless session/provider for toDF and preserve the registered table identity through alias.
2. Reject unsupported DeltaTable mutations and merge builders with named BrickHouse errors; verify the read-only API in an actual SDK loopback job and preserve the committed table version.

**Evidence:** A pinned-SDK loopback worker exercised `toDF` and `alias` over the existing Sparkless session, rejected DeltaTable mutation/builders and worker SQL/DataFrame writes, and verified the committed version stayed at 0. See [B015.2 conformance](../../artifacts/conformance/B015.2-worker-delta-table.md).

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

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B016 · **Parent criteria:** 3.

**Dependencies:** B014, B016.2.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Run/task/attempt-scoped values and declared output/context adapters.

**Acceptance criteria:**

1. Persist supported task values across dependent tasks with recorded serialization/size/default rules and attempt isolation; expose declared context/output behavior and reject unsupported methods explicitly.

**Evidence:** 43 focused checks and the 165-test local suite passed. The pinned-SDK loopback job verified JSON values, the 48 KiB/value and 250 distinct task/key run limits, default/missing behavior, successful retry-attempt selection, upstream-only reads, unsupported methods, and run isolation. Schema 10 migrations and incomplete-schema rejection passed. Ruff check/format, strict mypy, planning validation, and diff checks passed; no Databricks credentials/account, cloud compute, Spark, or JVM was used. See [B016.3 conformance](../../artifacts/conformance/B016.3-attempt-scoped-task-values.md).

## B017.1 — Execute parameterized ipynb in fresh kernels

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B017 · **Parent criteria:** 1, 4.

**Dependencies:** B015, B016, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** nbformat/nbclient runner with outputs and errors.

**Acceptance criteria:**

1. Execute real parameterized ipynb fixtures in fresh kernels with scoped spark/dbutils injection, cell output artifacts, and propagated failures.

**Evidence:** Five real-kernel tests passed using Python 3.11.14, Sparkless 4.14.1, ipykernel 7.4.0, jupyter-client 8.10.0, nbclient 0.11.0 and nbformat 5.11.1. Tagged-cell parameter overrides, local Spark/dbutils behavior, output artifacts, fresh-kernel isolation, parent credential non-inheritance and saved/propagated failures passed. Hash-locked environment installation, `pip check`, focused Ruff/format and strict mypy passed. No Databricks account, real credential, cloud compute, JVM or Spark driver was used. See [B017.1 evidence](../../artifacts/conformance/B017.1-ipynb-kernel.md).

## B017.2 — Convert source exports and handle supported magics

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B017 · **Parent criteria:** 1, 2, 3.

**Dependencies:** B015, B016, B017.1, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Structured Python source converter and explicit magic handlers.

**Acceptance criteria:**

1. Test the declared source format and Python/SQL/Markdown/run handlers, including shared-scope %run; reject unsupported languages, unknown magics, and package installation.

**Evidence:** Eleven focused source-export tests passed with a real Python 3.11.14 kernel, Sparkless 4.14.1, ipykernel 7.4.0, jupyter-client 8.10.0, nbclient 0.11.0, and nbformat 5.11.1. The suite exercised Python/SQL/Markdown/%run handlers, shared Python scope, `_sqldf`, output capture, immutable source files, and pre-kernel rejection for unsupported languages, magics, package installation, invalid paths, and cycles. The combined B017.1/B017.2 run passed 16 tests; Ruff check/format and strict mypy passed. No Databricks account, credentials, cloud compute, JVM, or Spark driver was used. See [B017.2 conformance](../../artifacts/conformance/B017.2-source-format-magics.md).

## B017.3 — Implement child notebook outcomes and kernel cleanup

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B017 · **Parent criteria:** 2, 3, 4.

**Dependencies:** B015, B016, B017.2, B017.4, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Child notebook run/exit contract and reliable cancellation.

**Acceptance criteria:**

1. Prove child notebook parameter/output isolation separately from %run, propagate failures/timeouts/parent cancellation, and reap kernels/descendants without leaked resources.

**Evidence:** Forty-eight focused tests passed with real local kernels. Relative `.ipynb` and marked `.py` children receive fresh kernels and isolated widget arguments/globals/output; string `exit()` values, early termination, failures and total timeouts are covered. Timeout and parent-cell interruption tests confirmed that both child kernels and their spawned subprocesses were reaped. Ruff check/format, strict mypy and `pip check` passed. No Databricks account, credentials, cloud compute, JVM or Spark driver was used. See [B017.3 conformance](../../artifacts/conformance/B017.3-child-notebook-outcomes.md). B017.5 adds compute reset/termination and timeout during active SDK notebook work; see [B017.5 conformance](../../artifacts/conformance/B017.5-notebook-compute-interruption.md).

## B018.1 — Prove the SDK dependent Python job workflow

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B012, B014, B015, B016, B017, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [ADR 0016](../decisions/0016-serverless-and-classic-compute-profiles.md), [ADR 0017](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Runnable G2 conformance suite for data flow, job lifecycle/isolation, compute selectors and warehouse consumers.

**Acceptance criteria:**

1. Submit a parameterized Python transformation through the SDK, resolve a scoped secret, commit Delta, and verify task output plus dependent-task and external SQL/session reads; exercise dependency failure, cancellation, retry, and cross-run isolation.

**Required children:** B018.1.1, B018.1.2, B018.1.3, B018.1.4. All children preserve the parent acceptance criterion.

**Evidence:** All four required children are complete. The current SDK-job, worker-session, and Sparkless-adapter suites passed 45 tests, including parameterized secret-scoped Delta writes, dependent reads, failure/retry/cancellation/isolation behavior, all seven job compute selectors, and G2 reads through all three local warehouse profiles. Credentials were unset; the SDK used local loopback only. Classic compute and warehouse infrastructure remain simulated, and no remote Databricks compatibility is claimed. See the [B018.1 aggregate conformance report](../../artifacts/conformance/B018.1-g2-sdk-workflow.md) and its linked child reports.

**Parent:** B018 · **Parent criteria:** 1, 2, 4.

## B018.1.1 — Prove job-to-Delta dependent data flow

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B018.1 · **Parent criteria:** 1.

**Dependencies:** B012, B014, B015, B016, B017, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [ADR 0016](../decisions/0016-serverless-and-classic-compute-profiles.md), [ADR 0017](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Runnable SDK G2 fixture for parameters, scoped secrets, Delta commit, task output, dependent task and external readers.

**Acceptance criteria:**

1. Submit a parameterized Python script through the pinned SDK, resolve a scoped secret, commit typed Delta rows, capture actual task output, and verify a declared dependent task plus SQL/Sparkless session reads the same committed rows.

**Evidence:** The pinned-SDK loopback producer appended typed rows through the worker's attempt-authenticated parent service. Its dependent task read both the task value and committed table rows; persisted task output, Delta version, schema and data matched a fresh Sparkless session and SDK SQL statement query. The worker write surface is restricted to registered Delta append, 32 KiB of Arrow IPC and 100,000 rows. Forty-three focused SDK job, worker-session and Sparkless-adapter tests passed; credentials were unset and no personal workspace was used. See [B018.1.1 conformance](../../artifacts/conformance/B018.1.1-job-to-delta-data-flow.md) for the initial classic-standard slice and the completed selector/warehouse matrices in [B018.1.3](../../artifacts/conformance/B018.1.3-g2-compute-profiles.md) and [B018.1.4](../../artifacts/conformance/B018.1.4-g2-warehouse-profiles.md).

## B018.1.2 — Verify job failures, cancellation, retry and isolation

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B018.1 · **Parent criteria:** 1.

**Dependencies:** B012, B014, B015, B016, B017, B043, B018.1.1.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [ADR 0016](../decisions/0016-serverless-and-classic-compute-profiles.md), [ADR 0017](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** G2 lifecycle fixture for dependency failure, cancellation, retry attempts and cross-run isolation.

**Acceptance criteria:**

1. Verify truthful dependency-failure propagation, cancellation and distinct retry attempts; prove parameters, widgets, secrets and task values do not leak between independent runs.

**Evidence:** Seven focused SDK job tests passed. Failed upstream work skipped dependent tasks while independent work completed; retries retained distinct attempts/output; classic compute restart and termination canceled active workers, reaped descendants, and preserved prior Delta commits. Parameters, widgets, task values and job-granted secrets remained scoped across runs, with secrets redacted from output and absent from workspace metadata. See [B018.1.2 conformance](../../artifacts/conformance/B018.1.2-job-lifecycle-isolation.md). The cancellation trigger is simulated compute interruption; no public SDK run-cancel route is claimed.

## B018.1.3 — Exercise G2 job compute-selection profiles

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B018.1 · **Parent criteria:** 1.

**Dependencies:** B012, B014, B015, B016, B017, B043, B014.6, B018.1.1.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [ADR 0016](../decisions/0016-serverless-and-classic-compute-profiles.md), [ADR 0017](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Orders job fixture across selected serverless and classic compute selectors with resource-lifetime assertions.

**Acceptance criteria:**

1. Run the G2 orders workflow under serverless and the selected classic new, existing, shared and dedicated compute selectors; assert real schema/data/output and the declared simulated resource lifetime without fabricating job completion.

**Evidence:** The two-task SDK orders workflow passed across serverless and six classic standard/dedicated new, existing, and shared selections. Real task outputs, parameter widgets, granted-secret redaction, dependent task values/reads, typed Delta schema/data through versions 0–7, and fresh Sparkless reads matched. Per-task new resources terminated on release; shared resources terminated after both consumers; existing all-purpose resources remained running; serverless had no classic resource. Compute resources are simulated. Credentials were unset; the pinned SDK used local loopback only. See [B018.1.3 conformance](../../artifacts/conformance/B018.1.3-g2-compute-profiles.md).

## B018.1.4 — Verify G2 output through SQL warehouse profiles

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B018.1 · **Parent criteria:** 1.

**Dependencies:** B012, B014, B015, B016, B017, B043, B011.6, B018.1.1.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [ADR 0016](../decisions/0016-serverless-and-classic-compute-profiles.md), [ADR 0017](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Warehouse-bound SQL readers for the G2 Delta commit across all selected warehouse profiles.

**Acceptance criteria:**

1. Read the exact G2 Delta commit through serverless, pro and classic SQL warehouse profiles; compare schema, values and committed version against the task and fresh Sparkless reads.

**Evidence:** A local SDK Python worker committed typed rows at Delta version 1. The exact versioned snapshot and a fresh Sparkless session agreed with successful SELECT results through serverless, pro, and classic warehouse profiles, including column names/types and values. Each simulated warehouse admission lease released, the table remained at version 1, and local warehouse metadata was deleted. Credentials were unset; SDK traffic used loopback only. See [B018.1.4 conformance](../../artifacts/conformance/B018.1.4-g2-warehouse-profiles.md).


## B018.2 — Prove base and notebook-extra installation paths

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B012, B014, B015, B016, B017, B018.1, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Separate base/no-notebooks and notebook-extra G2 reports.

**Acceptance criteria:**

1. Run the base workflow without notebook dependencies, Java, or cloud compute; independently run the notebook-extra workflow and record real outputs, errors, cancellation, and cleanup.

**Evidence:** The built `brickhouse==0.1.0.dev0` wheel passed the ten-case base G2 job suite in a clean install with Sparkless 4.15.0, delta-rs 1.2.1, PyArrow 23.0.1 and SQLGlot 30.24.0, while `ipykernel`, `jupyter-client`, `nbclient` and `nbformat` were absent. The separate `[notebooks]` install passed 36 cases for kernel output/errors, environment and source handling, child/parent cancellation, cleanup and SDK NotebookTask profiles. Both isolated lanes hid Java, unset Databricks credentials and used the installed wheel plus local loopback only. Dependency checks, wheel/sdist build and lock check passed. See [B018.2 conformance](../../artifacts/conformance/B018.2-installation-lanes.md). The final release candidate gate remains B023.

**Parent:** B018 · **Parent criteria:** 3, 4.

## B019.1 — Define and implement the bounded SQL semantics corpus

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Versioned query/type/error fixtures and explicit supported cases.

**Acceptance criteria:**

1. Enumerate and implement selected projection/filter/aggregate/join/DDL cases with null, coercion, decimal, date/timezone, collection, and error fixtures; retain mismatches visibly.
2. Define and verify separate serverless (serverless-local-v1) and classic standard/dedicated (spark35-local-v1) SQL/session cases for ANSI/timezone defaults, configuration scope, temporary/global views and cache restrictions, and error behavior. Deferred-analysis and Connect cases remain explicitly unsupported until B036 proves them.

**Required children:** B019.1.1, B019.1.2, B019.1.3, B019.1.4, B019.1.5, B019.1.6, B019.1.7. All children and both original/inherited criteria remain required.

**Parent:** B019 · **Parent criteria:** 1, 4.

**Evidence:** All seven required slices originally passed against real local Delta/Sparkless 4.15.0. The SQL statement-service module passed 35 tests against the published Sparkless 4.15.1 wheel; it passed again on 4.15.2 after the published fix. Nested field projection succeeds, and the selected direct UTC-aware timestamp predicate now returns order_id 2 under [PR #1721](https://github.com/eddiethedean/robin-sparkless/pull/1721), closing [follow-up issue #1720](https://github.com/eddiethedean/robin-sparkless/issues/1720). The local corpus retains exact schemas and mismatches, including COUNT int32 and lost UTC Arrow metadata. Connect protocol/deferred analysis remain unsupported pending B036. C018 remains planned and remote Databricks parity is not claimed. See [the aggregate corpus report](../../artifacts/conformance/B019.1-sql-semantics-corpus.md).

## B019.1.1 — Freeze the SQL fixture and scalar query cases

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Versioned typed SQL fixture and projection/filter/null/coercion evidence.

**Acceptance criteria:**

1. Store deterministic input rows, exact Arrow schemas, expected results, and ordering rules for the selected SQL fixture; exercise projection, filtering, null predicates, aliases, and selected scalar coercions through the local SQL service.

**Parent:** B019.1 · **Parent criteria:** 1.

**Evidence:** The versioned orders fixture passed through real local Delta and Sparkless 4.15.0, with exact Arrow schema/data checks for projection, filtering, null detection, decimal values, scalar coercion, aliases, and ordering. The captured reference statement was rejected with HTTP 400 `BAD_REQUEST`; the existing serverless warehouse was restored to `STOPPED`, and no remote output was used as an oracle. See [the conformance report](../../artifacts/conformance/B019.1.1-sql-scalar-cases.md).

## B019.1.2 — Qualify grouped aggregates and decimal results

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B043, B019.1.1.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Exact aggregate values, Arrow result schemas, and decimal/error fixtures.

**Acceptance criteria:**

1. Exercise selected COUNT/SUM/grouping cases with null and empty inputs, recording exact result types, decimal precision/scale, and ANSI overflow or invalid-cast outcomes in the versioned corpus.

**Evidence:** The versioned aggregate fixture passed grouped COUNT/SUM with a null amount, COUNT/SUM over empty input, and the selected invalid BIGINT cast through real local Delta and Sparkless 4.15.0. Exact Arrow schemas retain local COUNT as `int32` and SUM as `decimal128(22,2)`; `ValueError` is recorded for the invalid cast. The bounded `aggregate-literals` Databricks statement returned HTTP 400 `BAD_REQUEST`, so no remote schema or rows are claimed. The existing warehouse was restored to `STOPPED`; no objects were created. See [the conformance report](../../artifacts/conformance/B019.1.2-sql-aggregate-cases.md).

**Parent:** B019.1 · **Parent criteria:** 1.

## B019.1.3 — Qualify date, timestamp, and timezone semantics

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B043, B019.1.1.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Versioned date/timestamp SQL result and error cases for the selected UTC profile.

**Acceptance criteria:**

1. Exercise selected DATE and TIMESTAMP projections, predicates, and literals against exact Arrow schemas/results under the declared UTC profile; record null, invalid conversion, and timezone boundary outcomes without normalizing away type or offset differences.

**Parent:** B019.1 · **Parent criteria:** 1, 2.

**Evidence:** The versioned date/time corpus passed against real local Delta and Sparkless 4.15.1 with exact DATE/TIMESTAMP schemas, date predicate and null behavior, UTC offset normalization and boundary results, and invalid DATE/TIMESTAMP conversions. Historical 4.15.1 behavior raised a timezone-type mismatch; the same retained predicate now passes with the published Sparkless 4.15.2 wheel under upstream [PR #1721](https://github.com/eddiethedean/robin-sparkless/pull/1721), closing issue [#1720](https://github.com/eddiethedean/robin-sparkless/issues/1720). A prior literal-only Databricks capture supplies UTC context but no predicate parity claim. See [the conformance report](../../artifacts/conformance/B019.1.3-date-time-semantics.md).

## B019.1.4 — Qualify nested collection projections

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B043, B019.1.1.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Versioned array/struct SQL fixtures with nested-null preservation and explicit unsupported cases.

**Acceptance criteria:**

1. Project selected nullable arrays and structs through SQL and compare exact nested Arrow schema/value results; explicitly reject unsupported collection types or operators instead of converting them through lossy JSON.

**Parent:** B019.1 · **Parent criteria:** 1.

**Evidence:** The selected nullable array/struct SQL projection passed against Sparkless 4.15.1 and the full statement-service module was requalified on 4.15.2 with exact nested Arrow schema and values, preserving empty arrays, null arrays, nested null fields, and null structs. Nested-field SQL projection returns the selected leaf value under upstream PR #1717; selected array subscript and map-constructor expressions still fail explicitly. No remote Databricks collection result is claimed. See [the conformance report](../../artifacts/conformance/B019.1.4-sql-collections.md).

## B019.1.5 — Qualify selected join semantics and boundaries

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B043, B019.1.1.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Versioned join result/error cases with explicit null-key, alias, and duplicate-column behavior.

**Acceptance criteria:**

1. Exercise the selected inner equijoin across registered tables with null keys and aliased columns; assert exact schema and values, and retain explicit errors for unsupported outer, self, multi-key, or multiple joins.

**Parent:** B019.1 · **Parent criteria:** 1.

**Evidence:** The selected aliased inner equijoin passed on real local Delta/Sparkless 4.15.0 with nullable keys, duplicate matches, exact schema and ordering. Explicit outer, self, multi-key and multiple joins raise typed unsupported errors. Fixed the planner to accept explicit `INNER JOIN`; no remote join behavior is claimed. See [the conformance report](../../artifacts/conformance/B019.1.5-sql-joins.md).

## B019.1.6 — Qualify DDL and stable SQL error categories

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B043, B019.1.1.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Versioned DDL and planner/execution error fixtures with no-side-effect assertions.

**Acceptance criteria:**

1. Exercise selected schema/table CREATE, CTAS, DROP, and IF EXISTS cases with exact catalog/table results; assert stable typed errors for duplicate/missing/invalid/unsupported statements and verify planning failures leave catalog, Delta versions, and rows unchanged.

**Parent:** B019.1 · **Parent criteria:** 1.

**Evidence:** The versioned DDL case passed schema/table CREATE, empty Delta creation, CTAS, DROP, and `IF EXISTS` with exact catalog/schema, Arrow, rows, and commit versions. Duplicate/missing/invalid/unsupported/parse failures are typed; each leaves catalog registrations and source Delta version/schema/rows unchanged. See [the conformance report](../../artifacts/conformance/B019.1.6-sql-ddl-errors.md).

## B019.1.7 — Qualify serverless and classic semantic profiles

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B043, B019.1.1, B019.1.3.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Profile-specific SQL/session defaults, isolation, restriction, and error cases.

**Acceptance criteria:**

1. Verify serverless ANSI/UTC defaults and restrictions separately from classic standard/dedicated spark35-local-v1 settings; exercise configuration and temporary-view scope, explicitly unsupported global-view/cache behavior, and profile-specific errors. Keep deferred analysis and Connect behavior explicitly unsupported pending B036.

**Parent:** B019.1 · **Parent criteria:** 2.

**Evidence:** Selected serverless, classic-standard, and classic-dedicated session profiles passed with exact settings and same-name temporary-view isolation. Global temporary views and SQL/DataFrame cache operations are explicitly unsupported; profile/config errors are tested. Deferred analysis is eager locally and Connect remains unsupported pending B036. No remote classic/Python parity is claimed. See [the conformance report](../../artifacts/conformance/B019.1.7-sql-session-profiles.md).

## B019.2 — Route mutations and snapshot utilities through shared tables

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B019.1, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** SQL update/delete/merge plus selected history/snapshot fixtures.

**Acceptance criteria:**

1. Execute SQL mutations through the selected table service/provider and compare DataFrame-visible history/schema; prove declared history/time-travel cases without bypassing feature validation.

**Required children:** B019.2.1, B019.2.2, B019.2.3, B019.2.4. All children and the original parent criterion remain required.

**Parent:** B019 · **Parent criteria:** 2.

**Evidence:** All required children B019.2.1–B019.2.4 are complete. The shared provider/local SQL regression suite verifies journaled UPDATE/DELETE/MERGE, exact committed DESCRIBE HISTORY ordering and schema, DESCRIBE TABLE schema, and version-pinned historical reads/errors. The combined focused SQL/provider/Sparkless adapter/worker suite passed 128 tests with Databricks credentials unset. See the [B019.2 aggregate report](../../artifacts/conformance/B019.2-sql-mutations-history-snapshots.md).

## B019.2.1 — Implement SQL UPDATE and DELETE through the table service

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B019.1, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Journaled SQL UPDATE/DELETE with exact Delta snapshots and DataFrame-visible results.

**Acceptance criteria:**

1. Route selected single-table UPDATE and DELETE predicates through DeltaRsTableService and its declared Delta provider; verify exact affected rows, Arrow schema, committed Delta versions/history, null/predicate behavior, and unchanged state on rejected or failed operations. No read/overwrite emulation or direct _delta_log edits.

**Evidence:** Focused local tests passed against Sparkless 4.15.0, delta-rs 1.2.1, PyArrow 23.0.1, SQLGlot 30.24.0, and workspace schema 18. Native UPDATE/DELETE history commits carry the journal operation identity and exact affected-row metrics; Sparkless reads the same table version/schema. Zero-row operations and pre-commit provider failures are terminal journal outcomes with no transaction-version gap. A pinned-SDK Statement Execution UPDATE is visible to the next SELECT, and an injected post-commit journal failure recovers from exact Delta history. Schema-17 operation records survive the schema-18 migration. No Databricks reference workspace or cloud compute was used. See [B019.2.1 evidence](../../artifacts/conformance/B019.2.1-sql-update-delete.md).

**Parent:** B019.2 · **Parent criteria:** 1.

## B019.2.2 — Implement single-key SQL MERGE through the table service

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B019.1, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Atomic matched-update/unmatched-insert SQL MERGE with exact snapshot evidence.

**Acceptance criteria:**

1. Route one selected single-key MERGE with matched UPDATE and unmatched INSERT through DeltaRsTableService and the declared Delta provider; verify exact output rows/schema, one committed version, DataFrame visibility, and typed rejection/no-side-effects for unsupported clauses, duplicate source keys, or invalid targets.

**Parent:** B019.2 · **Parent criteria:** 1.

**Evidence:** A registered-table, one-key MERGE passed the focused SQL/provider/workspace regressions with Sparkless 4.15.0, delta-rs 1.2.1, PyArrow 23.0.1, SQLGlot 30.24.0, and metadata schema 19. One native commit produced exact matched-update/unmatched-insert rows, Arrow schema, history metrics, operation identity, and fresh Sparkless visibility. Unsupported clauses, duplicate/null source keys, invalid targets and mappings fail before target commit or journaling. The schema-19 migration preserves schema-18 journal records; Databricks credential variables were unset and no remote workspace was used. See [B019.2.2 evidence](../../artifacts/conformance/B019.2.2-sql-merge.md).

## B019.2.3 — Qualify SQL DESCRIBE and Delta history utilities

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B019.1, B043, B019.2.1, B019.2.2.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Versioned utility result schemas and history rows for real provider commits.

**Acceptance criteria:**

1. Implement selected DESCRIBE TABLE and DESCRIBE HISTORY results from the registered table/provider services; assert exact schemas and committed operation/version ordering after UPDATE, DELETE, and MERGE without deriving history from uncommitted metadata.

**Parent:** B019.2 · **Parent criteria:** 1.

**Evidence:** Selected DESCRIBE TABLE and DESCRIBE HISTORY passed with Sparkless 4.15.0, delta-rs 1.2.1, PyArrow 23.0.1, SQLGlot 30.24.0, and metadata schema 19. DESCRIBE TABLE returns the registered provider field order, exact three-column schema, and supported primitive/nested Spark SQL type text. DESCRIBE HISTORY returns the exact seven-column profile from public delta-rs history and the committed newest-first versions after UPDATE, DELETE, and MERGE; fresh Sparkless reads observe the same target state. Unsupported forms reject without mutation. The focused SQL/provider suite passed 88 tests with Databricks credential variables unset; no remote workspace was used. See [B019.2.3 evidence](../../artifacts/conformance/B019.2.3-sql-describe-history.md).

## B019.2.4 — Qualify SQL version-as-of snapshot reads

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B019.1, B043, B019.2.3.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Versioned SQL time-travel fixtures that share the authoritative table snapshot contract.

**Acceptance criteria:**

1. Implement selected VERSION AS OF SQL reads using TableSnapshot/read_snapshot and compare exact historical schema/rows with direct service and Sparkless reads; invalid, future, expired, or unsupported snapshot selectors must raise typed errors and never silently fall back to latest.

**Parent:** B019.2 · **Parent criteria:** 1.

**Evidence:** Literal integer VERSION AS OF selectors returned the exact historical Arrow schema and rows through SQL and matched direct service/Sparkless reads at the same Delta version. Timestamp/negative/decimal/string/malformed selectors reject; future and vacuum-expired versions raise typed unavailable errors and do not fall back to latest. The combined focused SQL/provider/Sparkless adapter/worker suite passed 128 tests with Databricks credential variables unset. No Databricks workspace or cloud compute was used. See [B019.2.4 evidence](../../artifacts/conformance/B019.2.4-sql-version-as-of.md).


## B019.3 — Declare executor capabilities and utility limits

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B011, B019.2, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Predetermined routing matrix and supported/unsupported utility report.

**Acceptance criteria:**

1. Document selected utility/type cases and Sparkless/default routing; any enabled DuckDB path is explicitly selected before execution and has profile-specific evidence, with no silent retry and no separate managed-table copy.

**Evidence:** The route matrix records SELECT/CTAS through Sparkless plus schema/catalog/table operations through their shared services and one registered Delta authority. Selected scalar, aggregate, date/time, collection, join, describe/history and integer snapshot cases are linked to their exact reports. A new parameterized regression rejects SHOW, EXPLAIN, ANALYZE, OPTIMIZE and VACUUM before table or version changes; existing CTAS error coverage proves query-engine exceptions propagate before registration. No DuckDB executor is enabled or included in the dependency set. The focused SQL/provider/Sparkless suite passed 74 tests on Sparkless 4.15.0 with Databricks credentials unset. See [B019.3 routing evidence](../../artifacts/conformance/B019.3-executor-routing.md).

**Parent:** B019 · **Parent criteria:** 1, 3.

## B020.1 — Prove the pinned CLI workspace and job workflow

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B005, B014, B043.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md).

**Deliverable:** Versioned CLI fixture against the local BrickHouse server.

**Acceptance criteria:**

1. Run the selected real CLI workspace/file/job operations locally with positive/error cases and cleanup, recording exact client versions.

**Parent:** B020 · **Parent criteria:** 1.

## B020.2 — Prove the minimal Terraform lifecycle

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

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

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Parent:** B021 · **Parent criteria:** 1, 3.

**Dependencies:** B005, B019, B045.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Pinned actual connector with local TLS/auth/transport evidence.

**Acceptance criteria:**

1. Connect the released REST/kernel profile to BrickHouse with recorded TLS/auth configuration and method limits; a failed interoperability probe leaves the promised implementation blocked or triggers a formal scope revision.

## B021.2 — Exercise connector query and result lifecycle

**Milestone:** M3 · **Kind:** epic · **Current ledger status:** done

**Parent:** B021 · **Parent criteria:** 2, 3.

**Dependencies:** B005, B019, B021.1, B045.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Connector parameter/schema/fetch/error/lifetime/cancellation fixtures.

**Acceptance criteria:**

1. Execute parameterized queries through the actual connector, compare schema/results, and test failures, retention, cancellation where advertised, plus explicit unsupported metadata/driver methods.

**Required children:** B021.2.1, B021.2.2, B021.2.3. All children and the original criterion above are required.

**Evidence:** All three required children pass for their stated local subsets. See [session evidence](../../artifacts/conformance/B021.2.1-sql-kernel-session-lifecycle.md), [query/result evidence](../../artifacts/conformance/B021.2.2-sql-connector-inline-results.md), and [failure/lifetime evidence](../../artifacts/conformance/B021.2.3-sql-connector-failures-retention.md). The kernel default result profile, other DB-API metadata methods, Thrift, and remote Databricks remain outside the capability claim.

## B021.2.1 — Implement kernel session lifecycle

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Parent:** B021.2 · **Parent criteria:** 1.

**Dependencies:** B005, B019, B021.1, B045.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Thread-safe ephemeral connector sessions bound to the persistent warehouse and namespace.

**Acceptance criteria:**

1. Verify actual pinned-kernel session create/close requests and return the released client's expected session ID shape; isolate each session's warehouse and namespace, reject per-session settings that are not applied by the SQL engine, and reject missing/stopped warehouses and unknown or cross-session IDs without fabricating a session.

## B021.2.2 — Verify connector execution parameters and fetch results

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Parent:** B021.2 · **Parent criteria:** 1.

**Dependencies:** B005, B019, B021.1, B045, B021.2.1.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Connector parameter, schema, inline fetch and typed result fixtures.

**Acceptance criteria:**

1. Execute bounded parameterized queries through the actual released connector, compare description/schema and fetched values with the local Statement Execution API, and retain explicit result-limit and unsupported-format behavior.

## B021.2.3 — Verify connector errors cancellation and method limits

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Parent:** B021.2 · **Parent criteria:** 1.

**Dependencies:** B005, B019, B021.1, B045, B021.2.2.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Connector error, close, retention, cancellation and unsupported-method fixtures.

**Acceptance criteria:**

1. Verify connector-visible SQL failures, result/session close and bounded retention; exercise cancellation where the synchronous client advertises it, and record actual unsupported metadata/driver methods without returning fabricated success.

**Evidence:** The connector integration observes `ServerOperationError` for SQL and `SHOW CATALOGS`, the synchronous cross-thread cancel request and terminal `CANCELED` state, the connector's `ProgrammingError: Invalid result format` for canceled results, and explicit `NotSupportedError` for rollback. Result close/session close return HTTP 200; SQL API regressions verify `CLOSED`, 12-hour expiry, and oldest-first eviction under the 64 MiB budget. See [the conformance report](../../artifacts/conformance/B021.2.3-sql-connector-failures-retention.md).

## B022.1 — Inject journal and commit boundary failures

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B018, B019, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** G3 phase-by-phase crash/reconciliation matrix.

**Acceptance criteria:**

1. Crash before/after Delta commit and SQLite finalization, then restart and verify known/uncertain outcomes and safe idempotent retry using actual files.

**Parent:** B022 · **Parent criteria:** 1.

**Evidence:** A POSIX child process is killed before Delta dispatch, after the Delta commit but before SQLite finalization, and after SQLite finalization. Reopen confirms conservative `UNCERTAIN` before provider dispatch, exact history-based recovery after Delta commit, and version-preserving idempotent replay after SQLite commit. The journal/provider/SQL mutation regression passed 24 tests. See [the crash-boundary report](../../artifacts/conformance/B022.1-commit-boundary-crashes.md).

## B022.2 — Exercise concurrent alias-aware SQL and DataFrame writes

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B018, B019, B022.1, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Concurrent-writer and idempotent-operation evidence.

**Acceptance criteria:**

1. Append through SQL and DataFrame paths including canonical-location aliases, proving no lost committed rows or duplicates from repeated operation IDs.

**Parent:** B022 · **Parent criteria:** 2.

**Evidence:** Two separate workspace catalogs with different table IDs resolve through real symlink/physical paths to one canonical Delta location. Concurrent Sparkless `session.write_path()` append and SQL MERGE unmatched insert serialize to consecutive commits; the stale version-0 MERGE is rejected before journaling and one same-ID retry commits. Both operation IDs replay their original versions; final Delta history contains one commit per mutation and actual rows `[0, 10, 20]`. The focused journal/table/SQL/Sparkless regression suite passed 46 tests with Databricks credentials unset. See [the alias-concurrency report](../../artifacts/conformance/B022.2-concurrent-alias-writes.md).

## B022.3 — Verify retention coordinated backup and restore

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B018, B019, B022.2, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Real backup/restore report, recovery checks, and retention policy.

**Acceptance criteria:**

1. Exercise restart/safe retry and result/log/staging retention; restore a coordinated SQLite/Delta/file snapshot and read actual rows/history with an explicit external-table inclusion policy.

**Required children:** B022.3.1, B022.3.2.

**Parent:** B022 · **Parent criteria:** 3.

**Evidence:** B022.3.1 and B022.3.2 are complete. The focused recovery regression suite passed 141 tests, covering same-payload reconciliation, changed-payload rejection, staged-input and result/log retention, managed row/history and job-history restore, and external-reference behavior. See [B022.3.1](../../artifacts/conformance/B022.3.1-restart-retention.md) and [B022.3.2](../../artifacts/conformance/B022.3.2-coordinated-workspace-restore.md). Snapshot quiescence coordinates BrickHouse owners only; external writers/provider state are not copied or coordinated.

## B022.3.1 — Exercise restart and bounded retention lifecycles

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B018, B019, B022.2, B043, B021.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Restart/replay plus SQL result, worker-log and staged-input retention evidence.

**Acceptance criteria:**

1. Restart a local workspace after committed and uncertain Delta operations; confirm exact same-payload reconciliation, changed-payload rejection, and staged-input cleanup only after a known terminal outcome.
2. Advance controlled retention clocks for SQL result chunks and worker attempt logs; expired payloads/log bytes are released while statement/run/attempt and Delta commit history remain available.

**Parent:** B022.3 · **Parent criteria:** 1.

**Evidence:** Real POSIX process kills cover the three SQLite/Delta boundaries. Restart confirms same-payload recovery, changed-payload rejection, and staged-input cleanup only after a known terminal outcome. Controlled clocks verify 12-hour SQL result expiry and 24-hour worker-log expiry while SQL statement and job/run/attempt history remains. The schema 19-to-20 upgrade preserves existing output. The focused journal, table, workspace, catalog, job, and statement API regression suite passed 71 tests with Databricks credentials unset; Ruff check and format passed. Worker-log expiry is lazy on output retrieval or explicit maintenance, with no background scheduler. See [the B022.3.1 report](../../artifacts/conformance/B022.3.1-restart-retention.md).

## B022.3.2 — Restore a coordinated local workspace snapshot

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B018, B019, B022.2, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Restored local workspace evidence and external-table inclusion policy.

**Owner:** Codex

**Acceptance criteria:**

1. Snapshot a quiesced workspace with SQLite metadata and managed Delta/files, restore it to a separate path, and verify actual rows, Delta history, operation records, and job history.
2. Record and exercise an explicit external-table policy: exclude external bytes by default, preserve their registrations as external references, and report missing external data explicitly rather than copying, deleting, or fabricating it.

**Parent:** B022.3 · **Parent criteria:** 1.

**Evidence:** Three snapshot-specific tests and the combined 141-test affected recovery suite passed. The quiesced snapshot preserved actual managed Delta rows/history, operation records, job/run/attempt/output records, and workspace files after restoring to a separate path. External bytes were excluded while registrations remained absolute references; a missing external table raised `TableMissingError`. Ruff, strict mypy, and diff checks passed. See [the restore report](../../artifacts/conformance/B022.3.2-coordinated-workspace-restore.md). External writers are not coordinated and external provider state is not captured.

## B022.4 — Verify serverless timeout preserves committed Delta state

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B018, B019, B022.1, B022.2, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Serverless worker timeout and committed-write recovery evidence.

**Acceptance criteria:**

1. Run a real pinned-SDK serverless Python worker that commits a Delta append before its timeout; verify the actual terminal attempt, worker cleanup, rows, and provider commit history.
2. Reopen the local workspace and prove timeout did not automatically retry the side-effecting task or duplicate the committed Delta version; combine with the existing classic restart/termination evidence for the parent cross-mode criterion.

**Parent:** B022 · **Parent criteria:** 4.

**Evidence:** Two pinned-SDK loopback cases and the combined 141-test affected recovery suite passed. A serverless-local worker that committed Delta data before timeout was reaped and recorded as timed out; reopen retained the one attempt, logs, rows and provider history without automatic replay. A separate pre-commit failure retried as a distinct attempt and produced one commit. Existing B018.1.2/B017.5 evidence supplies classic compute restart/termination, retry-attempt and notebook interruption comparisons. See [the B022.4 report](../../artifacts/conformance/B022.4-serverless-timeout-commit.md). This does not establish Databricks serverless/classic service or runtime behavior.

## B023.1 — Freeze the 0.1.0 scope and distribution inventory

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Parent:** B023 · **Parent criteria:** 3, 4.

**Dependencies:** B020, B021, B022, B043, B044, B048.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Versioned release scope, notices, dependency/client inventory, and limitations.

**Acceptance criteria:**

1. Reconcile completed requirements with per-case capability evidence; verify notices/license decisions, migrations, examples, and a publicly available compatible Sparkless artifact without publishing BrickHouse.

**B048 attachment:** The supported-scope inventory must retain [the local-core boundary review](../../artifacts/conformance/B048-local-core-boundaries.md) and [the loopback remediation evidence](../../artifacts/conformance/B048.1-loopback-bind-boundary.md), including the explicit no-auth/no-sandbox limit.

**Evidence (historical development candidate; superseded by B066):** [The versioned scope inventory](../../artifacts/conformance/B023.1-alpha-scope.md) records the 0.1.0 package/dependency/client scope, all 67 capability states, schema-20 migration coverage, pinned direct dependency notices and hashes, runnable local orders example, the public Sparkless 4.15.0 artifact and exact locally exercised macOS arm64 wheel hash. Nine focused migration/rollback tests passed; the example integration passed 1 test with `DATABRICKS_TOKEN` and `DATABRICKS_HOST` unset; Ruff and strict mypy passed for the example. B023.2.1–B023.2.3 now record wheel/sdist auditing, macOS arm64 CPython 3.11/3.12 base and notebook-extra install/offline behavior, and G1–G4 evidence; Linux x86_64 remains unadvertised pending CI. BrickHouse's own license remains unselected and blocks public publication. No release artifact was published.


Current 0.1.0 artifact hashes, platform qualification, Apache-2.0 metadata, and remaining publisher requirements are recorded by B066 in the [release bundle](../../artifacts/releases/0.1.0/README.md). These original B023 results apply only to their dated development artifacts.

## B023.2 — Gate candidate wheels on platforms and offline execution

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Parent:** B023 · **Parent criteria:** 1, 2, 4.

**Dependencies:** B020, B021, B022, B023.1, B043, B044, B048.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** 0.1.0 wheel/sdist install matrix and consolidated local G1-G4 reports.

**Acceptance criteria:**

1. Install built wheels for every advertised Python/platform candidate and run declared G1-G4 plus JVM/cloud/Docker-free base and provisioned offline checks; gate the notebook extra separately and advertise only passing combinations.

**Required children:** B023.2.1, B023.2.2, B023.2.3. The platform matrix, clean base/offline gate, and separate notebook-extra gate must all pass for the advertised profiles.

**Evidence (historical development candidate; superseded by B066):** [B023.2.1](../../artifacts/conformance/B023.2.1-candidate-wheel-platform.md) audited clean installs; current wheel/sdist hashes and audit are in the candidate bundle. The current exact candidate passed base G1–G4 in 137 cases per interpreter and notebook-extra G1–G4 in 175 cases per interpreter on macOS arm64 CPython 3.11.15/3.12.13, 624 total. See its [machine-readable current reports](../../artifacts/releases/0.1.0/evidence/conformance/sparkless-4.15.2-current/). The prior 132/169 counts are historical. Linux x86_64 remains unadvertised pending CI; publication remains blocked on the owner's project-license decision.


Current 0.1.0 artifact hashes, platform qualification, Apache-2.0 metadata, and remaining publisher requirements are recorded by B066 in the [release bundle](../../artifacts/releases/0.1.0/README.md). These original B023 results apply only to their dated development artifacts.

## B023.2.1 — Build and audit release candidates across the advertised platform matrix

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Parent:** B023.2 · **Parent criteria:** 1.

**Dependencies:** B020, B021, B022, B023.1, B043, B044, B048.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Hash-identified wheel/sdist and clean-install evidence for every advertised Python/OS/architecture combination.

**Acceptance criteria:**

1. Build the candidate wheel and source distribution from the current checkout; inspect metadata, runtime requirements, package contents, and third-party notices in both artifacts.
2. Install the built wheel in clean environments for every advertised Python/OS/architecture combination; assess CPython 3.11/3.12 Linux x86_64 and macOS arm64 candidates and advertise only combinations with actual passing install evidence.

**Evidence (historical development candidate; superseded by B066):** The built `0.1.0.dev0` wheel and sdist passed metadata/content audit and include every retained notice. Fresh macOS arm64 CPython 3.11.15 and 3.12.13 environments installed the same wheel, passed dependency checks and CLI/import checks, and omitted all four optional notebook packages. The current qualified install set is those two macOS combinations; Linux x86_64 is intentionally not advertised until its new Ubuntu CI matrix runs. See [the candidate report](../../artifacts/conformance/B023.2.1-candidate-wheel-platform.md).


Current 0.1.0 artifact hashes, platform qualification, Apache-2.0 metadata, and remaining publisher requirements are recorded by B066 in the [release bundle](../../artifacts/releases/0.1.0/README.md). These original B023 results apply only to their dated development artifacts.

## B023.2.2 — Qualify the base wheel offline without JVM, cloud, or Docker

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Parent:** B023.2 · **Parent criteria:** 1.

**Dependencies:** B020, B021, B022, B023.1, B043, B044, B048, B023.2.1.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Clean base-wheel G1-G4 and provisioned offline execution reports with credential and process boundaries.

**Acceptance criteria:**

1. Install the base candidate without the notebook extra, provision dependencies before the offline check, then run the declared G1-G4 workflow and process/network gates without Java, full PySpark, delta-spark, Docker, cloud compute, or real Databricks access.
2. Prove the installed-wheel workflow uses loopback only, does not load `.env` or inherit real workspace credentials, and succeeds after external network access is denied.

**Evidence (historical development candidate; superseded by B066):** The historical [base-wheel report](../../artifacts/conformance/B023.2.2-base-wheel-offline.md) records 132 G1–G4 tests per interpreter for its prior wheel hash. The exact current wheel passed 137 base cases per interpreter on macOS arm64 CPython 3.11.15 and 3.12.13; its [current machine-readable reports](../../artifacts/releases/0.1.0/evidence/conformance/sparkless-4.15.2-current/) bind results to the current wheel and source archive. They show loopback-only traffic under outbound-network denial, no `.env` reads or inherited Databricks credentials, no Java/Spark/Docker process or forbidden distribution/import, and absence of optional notebook packages. Terraform uses the provisioned local provider mirror and local RPC socket exception. Linux remains unadvertised pending CI and BrickHouse's own license remains an owner publication blocker.


Current 0.1.0 artifact hashes, platform qualification, Apache-2.0 metadata, and remaining publisher requirements are recorded by B066 in the [release bundle](../../artifacts/releases/0.1.0/README.md). These original B023 results apply only to their dated development artifacts.

## B023.2.3 — Qualify the notebook extra as a separate offline candidate

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Parent:** B023.2 · **Parent criteria:** 1.

**Dependencies:** B020, B021, B022, B023.1, B043, B044, B048, B023.2.1, B023.2.2.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Isolated notebook-extra wheel install and G1-G4 evidence with exact advertised combinations.

**Acceptance criteria:**

1. Install the notebook extra separately from the base candidate and run the declared kernel, notebook job, mode, G1-G4, cleanup, and interruption cases on the advertised platform/Python combinations.
2. Provision all native/runtime dependencies before the offline check; prove notebook execution uses local kernels and workers without Java, full PySpark, delta-spark, Docker, cloud compute, or real Databricks access.
3. Verify the base wheel remains free of notebook runtime packages and advertise only notebook combinations with passing install and behavior evidence.

**Evidence (historical development candidate; superseded by B066):** The historical [notebook-extra report](../../artifacts/conformance/B023.2.3-notebook-extra-offline.md) records 169 G1–G4 tests per interpreter for its prior wheel hash. The exact current wheel with the notebook extra passed 175 selected cases per interpreter on macOS arm64 CPython 3.11.15 and 3.12.13, including 37 notebook/kernel cases. Current machine reports are in the [Sparkless 4.15.2 report directory](../../artifacts/releases/0.1.0/evidence/conformance/sparkless-4.15.2-current/). Both base environments passed `uv pip check` and assertions that optional notebook distributions were absent. The current sandbox reports show only loopback/local Unix-socket traffic, no `.env` reads or inherited workspace credentials, and no Java/Spark/Docker process or full Spark/Delta distribution/import. Linux x86_64 remains unadvertised pending CI; the project license remains an owner decision.


Current 0.1.0 artifact hashes, platform qualification, Apache-2.0 metadata, and remaining publisher requirements are recorded by B066 in the [release bundle](../../artifacts/releases/0.1.0/README.md). These original B023 results apply only to their dated development artifacts.

## B023.3 — Assemble the owner-reviewable release candidate

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Parent:** B023 · **Parent criteria:** 3, 4.

**Dependencies:** B020, B021, B022, B023.2, B043, B044, B048.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [08-roadmap.md](08-roadmap.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Local distribution artifacts, generated capability docs, and release report.

**Acceptance criteria:**

1. Assemble actual artifacts and retained evidence, update truthful current-behavior documentation, validate the owner release checklist, and keep publication as a separately authorized action.

**Evidence (historical development candidate; superseded by B066):** The [0.1.0 owner-review bundle](../../artifacts/releases/0.1.0/README.md) contains the audited `0.1.0.dev0` wheel/sdist, artifact hashes, capability snapshot and generated Markdown, versions and exact G1–G4 selectors, current offline guard reports, recovery evidence, and owner checklist. The exact current wheel passed 624 selected cases across base and notebook-extra lanes on CPython 3.11.15/3.12.13; the separate 99-case B064 installed-wheel run also passed. The distribution audit passed and reports the unselected project license. Linux x86_64 qualification and explicit publication authorization remain open. No account, cloud workload, or publication was used.


Current 0.1.0 artifact hashes, platform qualification, Apache-2.0 metadata, and remaining publisher requirements are recorded by B066 in the [release bundle](../../artifacts/releases/0.1.0/README.md). These original B023 results apply only to their dated development artifacts.

## B011.5 — Implement local SQL warehouse identity and lifecycle

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Parent:** B011 · **Parent criteria:** 4.

**Dependencies:** B005, B010, B011.1, B043.

**Plans:** [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Persistent warehouse records, type/flag validation and simulated lifecycle with admission/invalidation contracts.

**Acceptance criteria:**

1. Use the pinned SDK to create/get/list/update/delete and start/stop serverless, pro, and classic local warehouse records; validate PRO plus enable_serverless_compute semantics, settings and missing IDs, label infrastructure simulated, and exercise persistence and idle transitions with a controlled clock.
2. Test real repository/state/API behavior for admission leases, stop/delete invalidation, missing resource errors and persistence with a controlled clock; expose the service contract consumed by B011.6, without requiring downstream SQL/connector results for this child to complete.

**Implementation evidence (2026-10-20):** Five focused local tests passed with Databricks credential variables unset. The actual pinned SDK 0.153.0 used loopback only to exercise the three warehouse types and their lifecycle; SQLite persistence/migration, type/setting errors, the release's 1 active per warehouse / 2 active workspace-wide / 8 queued per warehouse FIFO admission, lease invalidation, and controlled-clock auto-stop were verified. See [B011.5 conformance](../../artifacts/conformance/B011.5-sql-warehouse-lifecycle.md). All infrastructure states remain simulated; this task does not prove SQL execution or remote Databricks behavior.

## B014.4 — Implement classic compute identity and lifecycle

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B002, B005, B013, B014.1, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Workspace-owned all-purpose and run-scoped job compute records with SDK lifecycle routes.

**Acceptance criteria:**

1. Persist and expose the selected pinned-SDK cluster create/get/list/edit/start/restart/terminate/delete behavior for local all-purpose compute; validate runtime/access-mode mappings and unsupported effects, label infrastructure simulated, and verify identity and legal transitions across restart.
2. Model new/shared job compute per run and persistent all-purpose identity; test first/last-consumer state, autostart/idle rules and durable invalidation events through the compute service using real metadata and a controlled clock. B014.6 consumes that contract to prove actual worker effects.

**Parent:** B014 · **Parent criteria:** 4.

## B014.5 — Resolve serverless and classic execution contexts

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B002, B005, B013, B014.4, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Versioned selector/environment/configuration resolver with immutable mode-aware attempt context.

**Acceptance criteria:**

1. Resolve the pinned serverless environment_key submission and classic new_cluster/existing_cluster_id/job_cluster_key shapes; reject conflicts, missing/deleted resources, invalid runtime/access mode and unsupported task/resource combinations before dispatch.
2. Map only provisioned local environment/client/runtime versions and verified dependencies, apply documented config scopes and serverless defaults, reject unsupported installation/compute-object selectors, and require the initial serverless fixture to disable unimplemented automatic optimization retries.

**Parent:** B014 · **Parent criteria:** 4.

## B017.4 — Bind compute profiles to notebook session lifecycle

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B017 · **Parent criteria:** 4.

**Dependencies:** B015, B016, B017.2, B014, B043.

**Plans:** [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Serverless/classic notebook context, environment precedence and isolated kernel attachment/reset between executions.

**Acceptance criteria:**

1. Execute real local notebook fixtures under serverless and classic standard/dedicated profiles with mapped environments, notebook/job override precedence, supported config defaults and rejection cases; preserve cell state only in its own live kernel.
2. Verify distinct notebook sessions, environment/config rejection before execution, retained state between cells and a fresh context after a normal reset; expose compute attachment/invalidation to B017.3/B017.5, which own interruption during execution and descendant cleanup.

**Evidence:** Twenty-four focused real-kernel tests passed across B017.1, B017.2, and B017.4 using the exact pinned stack and Sparkless 4.14.1. Serverless, classic-standard, and classic-dedicated contexts verified mapped environments, installed dependency pins, session defaults/overrides, invocation/widget parameter precedence, state between cells, isolated sessions after reset, and pre-kernel rejection of invalid profiles/settings/dependencies. Flattened worker context preserves classic compute identity/generation for later supervision. Ruff check/format, strict mypy, and `pip check` passed. No Databricks account, credentials, cloud compute, JVM, or Spark driver was used. See [B017.4 conformance](../../artifacts/conformance/B017.4-notebook-context.md).

## B020.4 — Prove compute and warehouse Terraform resources

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B005, B014, B020.3, B011, B043.

**Plans:** [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Pinned Terraform lifecycle matrix for selected classic compute and three SQL warehouse resource variants.

**Acceptance criteria:**

1. Apply/read/update/refresh/destroy selected classic cluster and all three SQL warehouse resource variants against local BrickHouse with pinned Terraform/provider versions, validating persisted identity, type flags, errors and cleanup.

**Evidence:** Terraform Core 1.16.6 and `databricks/databricks` 1.137.0 passed the selected local lifecycle matrix: one classic cluster and serverless/Pro/classic SQL warehouses applied, refreshed, updated, rejected an invalid type/flag change without mutation, and destroyed cleanly. Focused SDK/warehouse and Terraform integration checks passed 8 tests. Compute and warehouse infrastructure remain simulated. A clean no-drift plan is not claimed because the provider's computed `odbc_params` has no truthful value without an ODBC listener; after warehouse updates, this is the only residual planned field. See [B020.4 conformance](../../artifacts/conformance/B020.4-terraform-compute-warehouses.md).

**Parent:** B020 · **Parent criteria:** 4.

## B043 — Research and define serverless and classic compute contracts

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B005.

**Plans:** [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [14-research-sources.md](14-research-sources.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [0016-serverless-and-classic-compute-profiles.md](../decisions/0016-serverless-and-classic-compute-profiles.md).

**Deliverable:** Dated primary-source research, compute behavior matrix, release allocation, and synchronized task/capability contracts.

**Acceptance criteria:**

1. Research current official Databricks documentation and the pinned SDK for serverless jobs/notebooks, classic all-purpose/job compute and access modes, and serverless/pro/classic SQL warehouses; record sources, inspected versions, and unobserved behavior.
2. Define real local execution, mode selection, environment/configuration rules, lifecycle and failure behavior, and the boundary between implemented semantics, simulated infrastructure, and unsupported features without a JVM or cloud fallback.
3. Assign bounded acceptance criteria and local fixture designs to release phases and existing implementation tasks; synchronize the roadmap, backlog, capability inventory, architecture, and reference policy, and pass planning validation without claiming runtime support.

## B011.6 — Bind warehouse admission to real statement execution

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done.

**Parent:** B011 · **Parent criteria:** 1, 2, 4.

**Dependencies:** B005, B010, B043, B011.2, B011.5.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Warehouse-bound statement dispatch, admission leases, invalidation and actual shared-table results.

**Acceptance criteria:**

1. Execute actual SDK statements through each local warehouse type using persistent warehouse identity, per-resource/global admission and supported settings; verify schema/rows and committed versions, and reject missing/deleted IDs before engine dispatch.
2. Stop/reset a warehouse during real execution and verify admission release, accurate cancellation/terminal outcomes and committed-data preservation; bind the selected connector warehouse path to the same identity, leaving released-connector proof to B021.

**Evidence:** Seventy-one focused local SQL/workspace/job regressions passed with Databricks credentials unset. The pinned SDK executed SELECTs through serverless, pro, and classic warehouse identities against shared committed Delta data; per-resource/global admission, FIFO promotion, overload rejection before engine dispatch, stop cancellation, restart release, and missing/stopped/deleted rejection passed. B045's selected kernel request path maps to the same persisted `warehouse_id`; BrickHouse validates that identity and returns an explicit unsupported response pending B021 session execution. See [B011.6 conformance](../../artifacts/conformance/B011.6-sql-warehouse-statement-binding.md).

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

**Milestone:** M2 · **Kind:** task · **Current ledger status:** done

**Parent:** B017 · **Parent criteria:** 1, 3, 4.

**Dependencies:** B015, B016, B043, B017.3, B017.4, B014.6.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Real notebook mode/format corpus with in-flight compute reset, cancellation and replacement-kernel evidence.

**Acceptance criteria:**

1. Run the document 16 notebook-extra variants with real kernels and shared-table results, including notebook/job environment precedence and source-export handling for serverless and classic standard/dedicated profiles.
2. Reset/terminate attached compute during cell execution and exercise timeout/parent cancellation, proving actual kernel/descendant cleanup, fresh replacement state, no cross-notebook leakage and preservation of committed Delta data.

**Evidence:** The pinned Databricks SDK loopback flow created and ran notebook jobs through all seven serverless/classic compute selections using real kernels and a shared committed Delta table. Base parameters and run-now overrides were applied, notebook environment metadata supplied a fallback, and an explicit job environment took precedence. Marked Python source exports ran under serverless, classic-standard, and classic-dedicated profiles with their PEP 723 environment metadata preserved. Restart/reset, termination, and timeout during actual kernel work reaped the kernel supervisor and spawned child; replacement execution had fresh state and retained the earlier Delta commit. B017.3 separately covers parent-cell cancellation of a child kernel and descendant. The schema upgrade preserved an existing queued job/run, accepted a notebook task after migration, and passed the foreign-key check. The focused regression suite passed 87 tests; Ruff, format, strict mypy, `pip check`, and `git diff --check` passed with Databricks credentials unset. See [B017.5 conformance](../../artifacts/conformance/B017.5-notebook-compute-interruption.md). This qualifies only the selected local profiles; it is not Databricks service/runtime parity.

## B020.5 — Prove both compute modes through bundle workloads

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B005, B014, B043, B020.4, B018.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Versioned serverless/classic bundles with real Python/notebook output, Delta/SQL checks and cleanup.

**Acceptance criteria:**

1. Validate/deploy/run the selected serverless and classic bundle variants using the pinned CLI and mapped local environments/runtime settings; inspect actual Python/notebook outputs and Delta/SQL results from the G2 corpus, not resource-creation responses.
2. Exercise invalid resource/task/settings, update/redeploy and cleanup against persisted local state; report supported execution separately from simulated infrastructure and retain exact client/provider/fixture versions.

**Parent:** B020 · **Parent criteria:** 3, 4.

**Evidence:** The pinned Databricks CLI v1.21.0 ran one serverless-local Python bundle and one classic-standard notebook bundle against a disposable loopback workspace. A serverless worker appended G2 rows 42 and 43 across an update/redeploy; the stable public job ID retained two immutable successful run histories. The classic notebook read and emitted those exact committed rows, and the pinned SDK SQL statement returned their exact schema/values through a local Pro warehouse with serverless enabled. Invalid task/selector/pipeline cases failed without creating state; destroy removed bundle trees/jobs and preserved run history. The classic resource was explicitly marked simulated; serverless created no classic consumer. The expanded job/workspace/notebook/Terraform regression passed 60 tests with token and host variables unset; a notebook interruption test now writes its marker atomically. See [B020.5 evidence](../../artifacts/conformance/B020.5-cli-serverless-classic-bundles.md).

## B044 — Refine the phase 0.1 execution and release gates

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B043.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md).

**Deliverable:** Executable work order, bounded provider/consumer tasks, finite release corpus and local fixture limits for 0.1.0.

**Acceptance criteria:**

1. Review the actual code, task states and dependency graph; make the Sparkless Python provider binding and SQL connector transport feasibility explicit early work without weakening the existing runtime or release requirements.
2. Separate resource/context deliverables from downstream workload verification, decompose oversized compute/client work, and define a finite mode/type/failure corpus, local fixture limits, evidence owners and release stop conditions.
3. Synchronize the release contract, roadmap, backlog, task and capability references, decisions and status; pass planning validation while preserving existing implementation states/evidence and all parent acceptance criteria.

## B045 — Resolve SQL connector transport feasibility early

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B005, B006, B044.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [ADR 0021](../decisions/0021-sql-kernel-local-transport-adaptation.md).

**Deliverable:** Pinned REST/kernel loopback transport capture, TLS/auth configuration and explicit feasible/blocked decision.

**Acceptance criteria:**

1. Inspect the actual pinned connector/kernel artifacts and send a bounded request to a local HTTPS capture boundary with explicit loopback host, local CA/auth, warehouse path and credentials absent; record the real request/error and any host/TLS/auth restrictions without returning fabricated SQL results.
2. Retain exact versions/hashes and a sanitized feasible/blocked transport decision; verify local host/credential isolation and identify the API changes needed by B011/B021. A negative feasibility finding does not establish query compatibility and must keep the B021 release claim blocked or trigger a formal scope revision.

**Evidence:** The pinned connector reached the loopback SQL session endpoint over verified local TLS with synthetic Bearer auth and the warehouse ID derived from `http_path`. The capture boundary returned a real local 401 before SQL execution, so query/result compatibility remains unproven and B021 stays gated on full local execution. Exact wheel hashes, host/port and private CA constraints, API changes and the sanitized request/error are in [B045 conformance](../../artifacts/conformance/B045-sql-connector-loopback.md).

## B046 — Deliver the Sparkless Python provider binding

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B009, B043, B044.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [0018-published-sparkless-python-provider.md](../decisions/0018-published-sparkless-python-provider.md).

**Deliverable:** Tested generic Python provider API, callback ownership/error contract and reproducible candidate wheel for BrickHouse integration.

**Acceptance criteria:**

1. Read current Sparkless code/instructions and define then expose a public session-scoped Python provider registration/lookup/write API over the tested Rust hooks, preserving generic engine boundaries.
2. Verify real Python callbacks, typed decimal/nested interchange, owner-thread/GIL/lifetime/cancellation/error behavior and independent sessions without private-API reliance, silent fallback or unbounded materialization claims.
3. Verify and probe an exact published source/wheel artifact, or build a candidate when no compatible release exists, with a real local Delta-backed provider; record versions/hashes/signatures and remaining limits, and distinguish development artifacts from the public compatible release required by B023.

**Required children:** B046.1, B046.2, B046.3. All children and the original criteria above are required.

**Evidence:** The published Sparkless 4.14.1 wheel passed the exact six nullable-struct cases and 14 selected real local Delta provider checks. The wheel SHA-256, versions, public API signature and limits are recorded in [B046.3 conformance](../../artifacts/conformance/B046.3-sparkless-4.14.1-provider.md). This clears the provider artifact/type blocker; it does not complete B010 writer routing or the release gates.

## B046.1 — Specify the public Python provider ownership contract

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B009, B043, B044.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [0018-published-sparkless-python-provider.md](../decisions/0018-published-sparkless-python-provider.md).

**Deliverable:** Current-source Python API gap review and bounded registration/callback/error design.

**Acceptance criteria:**

1. Inspect the current Sparkless checkout and its instructions, Python/PyO3 exports and tested Rust provider API; record exact source revision and proposed public signatures, owner-thread/GIL, lifetime, structured errors and bounded IPC limits without treating unpublished APIs as available.

**Parent:** B046 · **Parent criteria:** 1, 2.

## B046.2 — Implement and verify Python provider callbacks

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B009, B043, B044, B046.1.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [0018-published-sparkless-python-provider.md](../decisions/0018-published-sparkless-python-provider.md).

**Deliverable:** Generic PyO3/Python provider binding with real callback and isolation checks.

**Acceptance criteria:**

1. Expose the reviewed registration/read/write contract in Sparkless and test real Python callbacks for typed Arrow/Delta data, provider failures, callback lifetime, reentrancy/owner-thread rules and independent sessions under that checkout's instructions; retain focused test commands and actual source commit.

**Parent:** B046 · **Parent criteria:** 1, 2.

## B046.3 — Verify and probe the published Python provider wheel

**Milestone:** M1 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B009, B043, B044, B046.2, B008.

**Plans:** [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), [17-serverless-and-classic-compute.md](17-serverless-and-classic-compute.md), [0017-phase-0.1-delivery-refinement.md](../decisions/0017-phase-0.1-delivery-refinement.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [0018-published-sparkless-python-provider.md](../decisions/0018-published-sparkless-python-provider.md).

**Deliverable:** Verified published wheel/source provenance and real local Delta provider probe for the Python bridge.

**Acceptance criteria:**

1. Hash-verify and install an exact published wheel, or build a candidate when no compatible release exists, then use the public Python API with a real local Delta-backed provider to read/delegate writes, preserve Decimal/nested schemas and committed versions, propagate failures, and close independent sessions with bounded inputs.
2. Record the tested source/wheel hash, dependency versions, public signatures and all owner-thread/materialization limits; make the development artifact reviewable without publishing, and retain B023's requirement for a publicly available compatible artifact before release.

**Parent:** B046 · **Parent criteria:** 2, 3.

**Evidence:** Installed the SHA-256-verified published 4.14.1 wheel in a temporary environment. The combined local Delta/provider probe passed 14/14 checks, and the exact nullable-struct reproducer passed 6/6 checks. No Databricks credentials or account were used; see [B046.3 conformance](../../artifacts/conformance/B046.3-sparkless-4.14.1-provider.md).

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

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B047, B018, B022.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [01-architecture.md](01-architecture.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** A code-referenced architecture review of the real local workflow and its future deployment adaptation points.

**Acceptance criteria:**

1. Review the implemented G1-G4 paths for one catalog/table-commit authority and separation of transport, domain services, persistence and local runtime; record concrete code references and existing test evidence.
2. Identify contained local paths, staging, interpreter/PID/signal/socket assumptions and stable workspace/table/job/attempt identities; document future storage/runtime adaptation points without adding speculative backends or changing tested local behavior.
3. Map future principal provenance and enforcement points across API, SQL, workers, files, secrets and results; accurately state that local compatibility tokens, access-mode labels and fresh processes are not network authentication or a sandbox.
4. Resolve violations of existing architectural invariants before release through bounded owning work, retain future-only gaps for SH.0 onward, and attach the review to the B023 scope record without adding deployment infrastructure to 0.1.

**Required children:** B048.1, B048.2. B048.1 resolves the unauthenticated network-bind bypass; B048.2 records the code-referenced review and attaches it to B023.1. All original criteria remain required.

**Evidence:** The code-referenced G1–G4 review maps service/catalog/commit authority, stable resource identities, local path/process assumptions, and future principal enforcement points. It retains authentication, sandboxing, and remote-worker work for SH.1/SH.3/SH.4. Direct API bind bypass was resolved by B048.1. See [the review](../../artifacts/conformance/B048-local-core-boundaries.md).

## B048.1 — Enforce loopback binds at the diagnostics server boundary

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B018, B022, B047.

**Plans:** [01-architecture.md](01-architecture.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md).

**Deliverable:** Consistent IPv4 loopback-only enforcement for every unauthenticated diagnostics server entry point.

**Acceptance criteria:**

1. Reject non-IPv4-loopback hosts in the direct server factory and serving function before opening a socket or taking a workspace service into network exposure.
2. Use the same host validation for the CLI and direct server APIs; retain successful loopback binding and test invalid IPv4, IPv6, and hostname inputs.
3. Document the loopback-only boundary without claiming network authentication or sandboxing.

**Parent:** B048 · **Parent criteria:** 4.

**Evidence:** The direct factory, serving function, and CLI share numeric IPv4-loopback validation before opening a socket or acquiring workspace state. The focused CLI/server suite passed 13 tests. Ruff, strict mypy (70 source files), and diff checks passed. See [the loopback-boundary report](../../artifacts/conformance/B048.1-loopback-bind-boundary.md).

## B048.2 — Record the local-core boundary review and release attachment

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B047, B018, B022.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [01-architecture.md](01-architecture.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md).

**Deliverable:** Code-referenced G1-G4 local-core review with future identity/runtime/storage gaps attached to the B023 scope inventory.

**Acceptance criteria:**

1. Review the implemented G1-G4 paths for one catalog/table-commit authority and separation of transport, domain services, persistence and local runtime; record code references and existing test evidence.
2. Identify contained local paths, staging, interpreter/PID/signal/socket assumptions and stable workspace/table/job/attempt identities; describe future storage/runtime adaptation points without speculative backends.
3. Map future principal provenance and enforcement points across API, SQL, workers, files, secrets and results; state that local compatibility tokens and fresh processes are not network authentication or a sandbox.
4. Record architectural findings and their disposition, link the completed review from B023.1, and retain only future deployment gaps for SH.1/SH.3/SH.4.

**Parent:** B048 · **Parent criteria:** 1, 2, 3, 4.

**Evidence:** The complete review and B023.1 attachment are recorded in [the local-core boundary report](../../artifacts/conformance/B048-local-core-boundaries.md).

## B049 — Establish emulator stability before deployment implementation

**Milestone:** M7 · **Kind:** task · **Current ledger status:** planned

**Dependencies:** B042, B048.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** A versioned emulator stability baseline and bounded first-deployment scope decision.

**Acceptance criteria:**

1. Select the exact emulator candidate, supported semantic/compute/client corpus, stability criteria and maintenance policy; assess repeated applicable G1-G6, clean-install, upgrade and coordinated-restore evidence with no unresolved release-blocking correctness, corruption or JVM-free execution defects in the declared scope.
2. Record remaining incompatibilities and defects honestly; stability does not require complete Databricks parity, does not follow automatically from phase 0.9 and does not assign a 1.0 release.
3. Select the initial single-host platform, trusted-code threat model, identity/credential approach and supported workload limits before SH.1 decomposition; all deployment implementation remains dependent on this completed gate.
4. Keep emulator maintenance and the local wheel workflow independent of deployment extras, external accounts, orchestration and remote execution; reference-account capture and publication retain their existing authorization boundaries.

## B050 — Deliver authenticated single-server deployment for trusted workloads

**Milestone:** M7 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B049.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [02-packages-and-public-api.md](02-packages-and-public-api.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Decomposed SH.1 service, authentication, secret persistence and single-host operations epic with S1 evidence.

**Acceptance criteria:**

1. Decompose into bounded implementation children after B049; freeze server-trusted-v1 threat model, platform/client matrix and positive, denial and recovery cases without claiming hostile-code isolation.
2. Provide verified encrypted ingress, actual user/service authentication with credential expiry/revocation, and shared authorization/audit decisions across supported API, SQL, worker, file, secret and result paths; no-auth mode cannot bind beyond loopback.
3. Persist scoped secrets using an explicit protected storage and recovery contract, test rotation/restart and principal/run denial, and exclude secret values from config, URLs, logs, ordinary backups and build artifacts.
4. Ship an installable optional single-server deployment artifact with durable state, bounded admission, health/readiness, draining/shutdown and coordinated backup/restore; use the same core services and retain one metadata owner.
5. Pass S1 using real TLS clients, providers, files and jobs on the selected deployment host; document arbitrary Python host-access limits for trusted code authors and prove the base local installation still runs without deployment dependencies.
6. Resolve and pin actual deployed provider/runtime graphs, separate incompatible clients and verify explicit endpoints/artifact provisioning and no credential discovery or hidden JVM/container/cloud execution before promoting a deployed profile.

## B051 — Add a browser workspace and deployed optional integrations

**Milestone:** M7 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B050.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Decomposed SH.2 browser and optional-provider deployment epic with S2 evidence.

**Acceptance criteria:**

1. Decompose and freeze the minimal browser workflow: edit/upload supported source, submit/cancel jobs, inspect authorized outputs, browse catalog data and execute bounded SQL through the same existing services.
2. Test browser session expiry/revocation, CSRF protection where applicable, output rendering safety and cross-principal denial for source, notebooks, results and logs; never create a second catalog, scheduler or UI-only authorization model.
3. Deploy only selected notebook/MLflow/serving/dbt/vector integrations with scoped credentials, authenticated advertised endpoints, explicit provider authority, resource/lifecycle management and coordinated state/artifact restore tests.
4. Pass S2 against real browser/API/provider flows with exact versions; absent extras remain absent, hosted services do not silently call cloud APIs, and the local core remains independently installable.
5. Apply shared deployed identity/artifact policy to selected MLflow/notebook provider APIs and UIs as well as BrickHouse routes, and restore actual provider-owned databases/artifacts without duplicating state in the BrickHouse catalog.

## B052 — Isolate execution for shared self-hosted use

**Milestone:** M7 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B051.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Decomposed SH.3 execution-boundary and resource-isolation epic with S3 evidence.

**Acceptance criteria:**

1. Decompose against a selected real OS/container/VM boundary and explicit supported host/threat matrix; process isolation or classic/serverless labels alone cannot satisfy server-isolated-v1.
2. Prevent scoped user workloads from accessing control metadata, other attempts, unauthorized table snapshots/files/results/secrets and forbidden host/network resources; test real denial and bypass attempts, including direct Python access.
3. Enforce measured CPU, memory, process, disk and runtime limits plus cancellation/cleanup for every advertised script, kernel and serving worker; preserve actual outcomes and committed Delta data under exhaustion or termination.
4. Pass S3 and the applicable emulator corpus inside the selected isolation mechanism, recording residual platform/shared-kernel risks and keeping any container runtime an explicit deployment-only prerequisite.

## B053 — Execute jobs and durable automation on remote workers

**Milestone:** M7 · **Kind:** epic · **Current ledger status:** planned

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

**Milestone:** M7 · **Kind:** epic · **Current ledger status:** planned

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

**Milestone:** M7 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B054.

**Plans:** [18-self-hosted-platform.md](18-self-hosted-platform.md), [0034-emulator-first-self-hosted-platform.md](../decisions/0034-emulator-first-self-hosted-platform.md), [02-packages-and-public-api.md](02-packages-and-public-api.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md).

**Deliverable:** Decomposed SH.6 distribution, consolidated evidence and operator handoff epic with S6 evidence.

**Acceptance criteria:**

1. Decompose candidate assembly and freeze the exact supported deployment/semantic/compute/client matrix; rerun applicable G1-G6 and S1-S5 from exact built artifacts for every advertised deployment profile.
2. Publish a reviewable local candidate inventory with artifact hashes, native/runtime versions, notices/license decision, migrations, security/recovery reports, runnable examples and operator documentation; actual publication still requires separate owner authorization.
3. Report planned, unsupported, partial, simulated and supported capabilities separately; real worker allocation cannot be inferred from simulated Databricks cluster metadata, and the release does not imply complete Databricks or distributed-query parity.
4. Pass S6 while independently retaining the JVM-free local wheel workflow without containers, identity servers, remote workers or cloud accounts; keep unresolved deployment/frontier requirements open with concrete limitations.

## B056 — Plan native Sparkless cluster execution

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B047.

**Plans:** [04-sparkless-upstream.md](04-sparkless-upstream.md), [18-self-hosted-platform.md](18-self-hosted-platform.md), [19-sparkless-cluster-execution.md](19-sparkless-cluster-execution.md), [sparkless-cluster-contracts.md](../upstream/sparkless-cluster-contracts.md), [0037-native-sparkless-cluster-plan.md](../decisions/0037-native-sparkless-cluster-plan.md).

**Deliverable:** A source-grounded distributed Sparkless design, staged upstream work and separate BrickHouse integration gates.

**Acceptance criteria:**

1. Inspect and pin current Sparkless source/contracts read-only; distinguish observed local behavior from proposed cluster interfaces and preserve JVM-free emulator priority.
2. Define partition planning/execution, typed bounded transport/shuffle, recovery, authentication, session isolation and provider-owned commit contracts with explicit unsupported cases and finite evidence gates.
3. Synchronize roadmap, future task/capability ledgers, upstream ownership and validation; gate implementation after emulator stability and BrickHouse integration on both qualified upstream artifacts and remote-worker foundations without expanding existing emulator or self-hosted release requirements.

## B057 — Qualify native Sparkless cluster architecture and contracts

**Milestone:** M8 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B049, B056.

**Plans:** [19-sparkless-cluster-execution.md](19-sparkless-cluster-execution.md), [sparkless-cluster-contracts.md](../upstream/sparkless-cluster-contracts.md), [0037-native-sparkless-cluster-plan.md](../decisions/0037-native-sparkless-cluster-plan.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Decomposed SC.0 feasibility and selection epic with a finite typed operator, partition, transport, sink and threat-model contract.

**Acceptance criteria:**

1. Refresh exact Sparkless source/instructions and decompose bounded feasibility probes after B049; compare extending native coordination with Polars partition operators against explicitly contained native backend options, checking real public APIs, licenses and semantic risks.
2. Freeze the initial typed operator/oracle corpus, versioned physical plan and snapshot/partition rules, numeric memory/batch/transfer/spill/result/retry limits, homogeneous platform/artifact matrix and enrolled-host threat model.
3. Probe typed plans/batches with two actual processes and determine whether provider-owned multi-fragment sink commits can avoid a full coordinator gather; record observed failures and required public upstream hooks without treating current buffered Arrow callbacks as distributed streams.
4. Pass K0 with an evidence-backed viable implementation decision, or record a negative feasibility outcome and keep dependent implementation blocked; no local prototype establishes multi-host execution, JVM-free guarantees or Spark compatibility without their separate tests.
5. Include pinned Ballista/DataFusion native infrastructure in the architecture comparison, measuring semantic/provider/partition/failure/security fit and Rust/Arrow/Polars version boundaries; the Ballista 55.0.0 research candidate requires Rust 1.94 and must not silently replace the Sparkless default or force an unqualified toolchain migration.

## B058 — Implement typed partition planning and native executor prototypes

**Milestone:** M8 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B057.

**Plans:** [19-sparkless-cluster-execution.md](19-sparkless-cluster-execution.md), [sparkless-cluster-contracts.md](../upstream/sparkless-cluster-contracts.md), [0037-native-sparkless-cluster-plan.md](../decisions/0037-native-sparkless-cluster-plan.md), [04-sparkless-upstream.md](04-sparkless-upstream.md).

**Deliverable:** Decomposed SC.1 versioned plan, pinned partition-scan and bounded native execution epic with K1 evidence.

**Acceptance criteria:**

1. Decompose and implement only the viable SC.0 operator/type envelope in Sparkless under its current repository instructions; preserve ordinary local APIs and keep BrickHouse SDK, SQLite and domain types out of generic engine code.
2. Bind one query to exact schema/settings/snapshots, generate disjoint provider-authorized scan fragments and a stage DAG with explicit partitioning/order requirements, typed literals, capabilities and version/hash/environment checks.
3. Execute one supported query across at least two real native executor processes using typed bounded batches, without shipping Python sessions/callbacks or materializing all source rows at the coordinator; prove exact null/decimal/nested types and empty-partition behavior.
4. Pass K1 including invalid plan/version/operator/schema/environment, oversized input and cancellation/resource-bound cases; repartition/coalesce names or duplicated full-query execution cannot substitute for actual partition work.

## B059 — Implement network exchange and distributed query operators

**Milestone:** M8 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B058.

**Plans:** [19-sparkless-cluster-execution.md](19-sparkless-cluster-execution.md), [sparkless-cluster-contracts.md](../upstream/sparkless-cluster-contracts.md), [0037-native-sparkless-cluster-plan.md](../decisions/0037-native-sparkless-cluster-plan.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md).

**Deliverable:** Decomposed SC.2 authenticated exchange, shuffle and global-operator epic with real two-host K2 evidence.

**Acceptance criteria:**

1. Decompose selected hash exchange, two-phase aggregates and inner equijoin with explicit partial/final semantics and bounded broadcast/global merge cases; freeze hash/equality, null/NaN/decimal/timezone and ordering rules.
2. Implement authenticated encrypted versioned executor transport, complete partition block manifests, checksums, maximum message/batch/in-flight bytes, backpressure/deadlines/cancellation, measured memory and spill-disk budgets and explicit skew/oversized-row failure policies.
3. Pass K2 on two actual hosts with overlapping keys, duplicate/null rows and empty partitions so real exchanges are necessary; verify corruption, slow readers, invalid credentials and resource exhaustion cannot produce fabricated empty results.
4. Record actual partition placement, network/spill/result bytes and operator outcomes; no silent local/backend fallback, unbounded coordinator gather or multi-process-only evidence can establish distributed-query support.

## B060 — Implement cluster attempt recovery and provider-owned sink commits

**Milestone:** M8 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B059.

**Plans:** [19-sparkless-cluster-execution.md](19-sparkless-cluster-execution.md), [sparkless-cluster-contracts.md](../upstream/sparkless-cluster-contracts.md), [0037-native-sparkless-cluster-plan.md](../decisions/0037-native-sparkless-cluster-plan.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md).

**Deliverable:** Decomposed SC.3 query lifecycle, fencing, recovery and distributed sink epic with K3 evidence.

**Acceptance criteria:**

1. Decompose generic query/stage/task attempt state distinct from BrickHouse job retries; enforce authenticated worker identities, coordinator leases/generations, bounded retry budgets and one accepted output per logical partition without speculative execution initially.
2. Test executor/network/coordinator failures, missing shuffle blocks, stale/duplicate completion, cancellation races and resource exhaustion; restart may truthfully interrupt queries but must fence old output and reconcile accepted sink outcomes.
3. Implement a provider-owned immutable fragment-manifest sink with content/schema/coverage/ownership and task-winner validation, one logical native commit and exact version receipt; qualify the actual provider API without claiming the current delta-rs callback supports this automatically.
4. Separate stable logical write intent from physical fragment checksums; freeze accepted manifests for an operation key, reject changed replay and reconcile commit-before-response uncertainty before replacement or retry. Workers never independently append retries or edit Delta logs.
5. Pass K3 with actual provider data/history/version checks, no duplicate visible output and bounded distributed-write memory; a bounded coordinator-gather prototype cannot qualify the distributed sink, and unsupported provider paths leave consumers blocked.

## B061 — Qualify a standalone native Sparkless cluster candidate

**Milestone:** M8 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B060.

**Plans:** [19-sparkless-cluster-execution.md](19-sparkless-cluster-execution.md), [sparkless-cluster-contracts.md](../upstream/sparkless-cluster-contracts.md), [0037-native-sparkless-cluster-plan.md](../decisions/0037-native-sparkless-cluster-plan.md), [04-sparkless-upstream.md](04-sparkless-upstream.md), [02-packages-and-public-api.md](02-packages-and-public-api.md).

**Deliverable:** Decomposed SC.4 public engine API, artifact, semantic/failure/security and scale qualification epic with K4 evidence.

**Acceptance criteria:**

1. Decompose a finite standalone public API and optional cluster installation boundary; record native executor/plan/exchange/environment compatibility separately from Python/Rust package versions and test mixed-version rejection.
2. Run K1-K3 and the selected Q01-Q09 typed semantic, snapshot, recovery, security/resource and no-JVM/no-remote-Spark corpus from exact candidate artifacts, including actual cross-host execution and bounded distributed sink behavior.
3. Measure single-node overhead and one/two/more-worker capacity/latency with hardware, input/skew, network, memory/spill and variance; do not promise speedup, autoscaling, HA or full Spark parity from a prototype.
4. Pass K4 with reviewable hash-pinned source/wheel/native artifacts, required upstream checks, licenses/notices and explicit limitations while local Sparkless runs independently; a development candidate is not a published release and publication remains separately authorized.

## B062 — Integrate qualified Sparkless clusters into BrickHouse

**Milestone:** M8 · **Kind:** epic · **Current ledger status:** planned

**Dependencies:** B061, B053.

**Plans:** [19-sparkless-cluster-execution.md](19-sparkless-cluster-execution.md), [sparkless-cluster-contracts.md](../upstream/sparkless-cluster-contracts.md), [0037-native-sparkless-cluster-plan.md](../decisions/0037-native-sparkless-cluster-plan.md), [01-architecture.md](01-architecture.md), [18-self-hosted-platform.md](18-self-hosted-platform.md).

**Deliverable:** Decomposed SC.5 optional engine adapter and two-host job/SQL/table integration epic with K5 evidence.

**Acceptance criteria:**

1. Decompose after both standalone cluster qualification B061 and authenticated isolated remote-worker foundation B053; use actual public Sparkless contracts and explicit engine selection, preserving fresh child job attempts and the local default.
2. Apply one workspace identity/policy, pinned table/snapshot and environment context across SQL/DataFrame plans and query/stage workers; the Sparkless query scheduler consumes granted capacity without duplicating BrickHouse job/resource authority.
3. Route all registered mutations and manifest validation through the authoritative table service and declared native Delta provider; enforce winning attempts, leases/generations, principal scope, uncertain-commit reconciliation and real deployed isolation/resource limits.
4. Pass K5 with two actual hosts, job and SQL results plus fresh-session/restart reads of exact schema/Delta versions, query cancellation/failures and relevant G/S gate regressions; distinguish actual partition placement from simulated Databricks compute metadata.
5. Keep existing emulator/self-hosted releases independent of this optional capability; require a published compatible Sparkless artifact before public BrickHouse cluster installation and retain explicit unsupported UDF/streaming/HA/distributed-transaction cases.

## B063 — Research and plan native open-source component reuse

**Milestone:** M0 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B056.

**Plans:** [03-dependencies-and-minilake.md](03-dependencies-and-minilake.md), [11-ml-and-extensions.md](11-ml-and-extensions.md), [19-sparkless-cluster-execution.md](19-sparkless-cluster-execution.md), [20-open-source-reuse.md](20-open-source-reuse.md), [oss-reuse-inventory.md](../upstream/oss-reuse-inventory.md), [0038-native-open-source-reuse.md](../decisions/0038-native-open-source-reuse.md).

**Deliverable:** Pinned primary-source dependency research and actionable adoption/qualification contracts across emulator, self-hosting and optional cluster plans.

**Acceptance criteria:**

1. Research exact public source/artifact versions, licenses and JVM/runtime boundaries for selected Databricks ecosystem and native alternative components, distinguishing local evidence from research-only candidates.
2. Record concrete reuse, adaptation, rejection and deferred decisions with single-authority interfaces, owned acceptance gates, dependency/source hygiene and finite native-only failure/restart tests; preserve emulator-first priority and existing release behavior requirements.
3. Synchronize affected plans, future task criteria, capability limitations and provenance/validation records without installing unqualified dependencies, modifying upstream source, changing existing successful evidence or claiming new runtime support.


## B064 — Correct phase 0.1 defects found in implementation review

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B023.3.

**Plans:** [01-architecture.md](01-architecture.md), [05-catalog-storage-sql.md](05-catalog-storage-sql.md), [06-jobs-notebooks-dbutils.md](06-jobs-notebooks-dbutils.md), [07-compatibility-and-testing.md](07-compatibility-and-testing.md), [12-delivery-and-operations.md](12-delivery-and-operations.md), [16-phase-0.1-initial-release.md](16-phase-0.1-initial-release.md), and [ADR 0014](../decisions/0014-delta-operation-journal.md).

**Deliverable:** Review defects fixed with regression evidence, a refreshed local 0.1 candidate, and truthful release/status records.

**Acceptance criteria:**

1. SQL joins retain each table binding so aliases resolve to the intended registered table even when source names collide across schemas.
2. Statement result production uses the public Sparkless bounded Arrow batch API and keeps peak result materialization bounded before enforcing row and byte limits.
3. The phase 0.1 INSERT forms covered by the contract append through the shared Delta table service, preserve named parameter binding, and have accurate capability and error behavior.
4. Worker timeout and cancellation supervision includes descendants that hold inherited output pipes after the parent exits; the caller returns within the configured supervision bounds.
5. Job admission runs at most two worker attempts concurrently, persists up to eight queued runs, rejects overflow before creating history, and safely resumes undispatched queue entries after restart.
6. Job reset revisions share the stable public job concurrency identity, so resetting a definition cannot bypass its max_concurrent_runs limit.
7. Snapshot restore validates the captured managed-table data inventory even when the original workspace is unavailable, while preserving valid unmaterialized registrations.
8. A committed operation with identical input replays after managed-table restore without weakening changed-input or changed-table identity checks.
9. SQL JSON timestamp encoding preserves the precision represented by the Arrow timestamp unit, including microseconds.
10. Refresh the local 0.1.0.dev0 candidate and current-behavior documentation from the fixed source, rerun relevant release gates, and record hashes, exact versions, test results and remaining limitations without publishing.
11. Recover interrupted SQL table creation without reporting an unmaterialized registration as a successful `IF NOT EXISTS`; preserve external tables, malformed Delta logs, and unresolved commits.

**Required children:** B064.1, B064.2, B064.3, B064.4, B064.5, B064.6, B064.7, B064.8, B064.9, B064.10, B064.11.

**Evidence:** B064 and all eleven children are complete. The refreshed Sparkless 4.15.2 installed wheel passed 99 focused B064 cases under the offline loopback guard and passed the selected current-candidate G1–G4 matrix (624 cases across base/notebook-extra and two Python versions). The earlier 415-pass/six-skip full source suite used Sparkless 4.15.0 before B064.11 and is historical. The older matrix reports remain tied to their archived wheel hash. See [B064 evidence](../../artifacts/conformance/B064-phase-0.1-review-fixes.md) and the [current candidate reports](../../artifacts/releases/0.1.0/evidence/conformance/sparkless-4.15.2-current/).

## B064.1 — Preserve SQL table aliases across same-named relations

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B023.3.

**Deliverable:** Alias-qualified join projections resolve to their own registered source after table binding, with a regression case using duplicate table names in separate schemas.

**Acceptance criteria:**

1. Alias-qualified join projections resolve to their own registered source after table binding, with a regression case using duplicate table names in separate schemas.

**Parent:** B064 · **Parent criteria:** 1.

**Evidence:** The current Sparkless 4.15.2 candidate wheel passes 99 focused B064 regressions under the offline loopback guard. The earlier 415-pass/six-skip full source-suite result used Sparkless 4.15.0 before B064.11 and is historical. See [B064 evidence](../../artifacts/conformance/B064-phase-0.1-review-fixes.md) and the [current-wheel report](../../artifacts/releases/0.1.0/evidence/conformance/B064-current-wheel-offline.json).

## B064.2 — Bound SQL output materialization before result encoding

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B023.3, B064.1.

**Deliverable:** A query with large join amplification is consumed through public bounded Arrow batches; row and byte truncation/failure behavior stays truthful and result memory is capped before JSON serialization.

**Acceptance criteria:**

1. A query with large join amplification is consumed through public bounded Arrow batches; row and byte truncation/failure behavior stays truthful and result memory is capped before JSON serialization.

**Parent:** B064 · **Parent criteria:** 2.

**Evidence:** The current Sparkless 4.15.2 candidate wheel passes 99 focused B064 regressions under the offline loopback guard. The earlier 415-pass/six-skip full source-suite result used Sparkless 4.15.0 before B064.11 and is historical. See [B064 evidence](../../artifacts/conformance/B064-phase-0.1-review-fixes.md) and the [current-wheel report](../../artifacts/releases/0.1.0/evidence/conformance/B064-current-wheel-offline.json).

## B064.3 — Implement the planned bounded SQL INSERT subset

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B023.3, B064.2.

**Deliverable:** Supported VALUES and SELECT inserts use the registered Delta table service and operation journal, support the selected named parameters, and are visible to DataFrame reads; unsupported forms fail before mutation.

**Acceptance criteria:**

1. Supported VALUES and SELECT inserts use the registered Delta table service and operation journal, support the selected named parameters, and are visible to DataFrame reads; unsupported forms fail before mutation.

**Parent:** B064 · **Parent criteria:** 3.

**Evidence:** The current Sparkless 4.15.2 candidate wheel passes 99 focused B064 regressions under the offline loopback guard. The earlier 415-pass/six-skip full source-suite result used Sparkless 4.15.0 before B064.11 and is historical. See [B064 evidence](../../artifacts/conformance/B064-phase-0.1-review-fixes.md) and the [current-wheel report](../../artifacts/releases/0.1.0/evidence/conformance/B064-current-wheel-offline.json).

## B064.4 — Supervise worker descendants that retain output pipes

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B023.3, B064.3.

**Deliverable:** A parent that exits after spawning a child cannot make a run exceed its deadline or return completed while the owned descendant continues to hold stdout/stderr.

**Acceptance criteria:**

1. A parent that exits after spawning a child cannot make a run exceed its deadline or return completed while the owned descendant continues to hold stdout/stderr.

**Parent:** B064 · **Parent criteria:** 4.

**Evidence:** The current Sparkless 4.15.2 candidate wheel passes 99 focused B064 regressions under the offline loopback guard. The earlier 415-pass/six-skip full source-suite result used Sparkless 4.15.0 before B064.11 and is historical. See [B064 evidence](../../artifacts/conformance/B064-phase-0.1-review-fixes.md) and the [current-wheel report](../../artifacts/releases/0.1.0/evidence/conformance/B064-current-wheel-offline.json).

## B064.5 — Implement durable two-active/eight-queued job admission

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B023.3, B064.4.

**Deliverable:** At most two worker runs dispatch concurrently; up to eight additional runs remain queued in FIFO order, overflow is rejected without a run record, and restart resumes only runs with no dispatched attempts.

**Acceptance criteria:**

1. At most two worker runs dispatch concurrently; up to eight additional runs remain queued in FIFO order, overflow is rejected without a run record, and restart resumes only runs with no dispatched attempts.

**Parent:** B064 · **Parent criteria:** 5.

**Evidence:** The current Sparkless 4.15.2 candidate wheel passes 99 focused B064 regressions under the offline loopback guard. The earlier 415-pass/six-skip full source-suite result used Sparkless 4.15.0 before B064.11 and is historical. See [B064 evidence](../../artifacts/conformance/B064-phase-0.1-review-fixes.md) and the [current-wheel report](../../artifacts/releases/0.1.0/evidence/conformance/B064-current-wheel-offline.json).

## B064.6 — Keep reset revisions under the stable public-job concurrency limit

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B023.3, B064.5.

**Deliverable:** Resetting a Terraform job preserves one stable admission identity across immutable execution revisions and cannot exceed the current definition concurrency setting.

**Acceptance criteria:**

1. Resetting a Terraform job preserves one stable admission identity across immutable execution revisions and cannot exceed the current definition concurrency setting.

**Parent:** B064 · **Parent criteria:** 6.

**Evidence:** The current Sparkless 4.15.2 candidate wheel passes 99 focused B064 regressions under the offline loopback guard. The earlier 415-pass/six-skip full source-suite result used Sparkless 4.15.0 before B064.11 and is historical. See [B064 evidence](../../artifacts/conformance/B064-phase-0.1-review-fixes.md) and the [current-wheel report](../../artifacts/releases/0.1.0/evidence/conformance/B064-current-wheel-offline.json).

## B064.7 — Validate managed snapshot contents from the snapshot itself

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B023.3, B064.6.

**Deliverable:** A managed table recorded as materialized at snapshot time cannot disappear silently during restore when the source workspace is gone; never-materialized managed registrations remain restorable.

**Acceptance criteria:**

1. A managed table recorded as materialized at snapshot time cannot disappear silently during restore when the source workspace is gone; never-materialized managed registrations remain restorable.

**Parent:** B064 · **Parent criteria:** 7.

**Evidence:** The current Sparkless 4.15.2 candidate wheel passes 99 focused B064 regressions under the offline loopback guard. The earlier 415-pass/six-skip full source-suite result used Sparkless 4.15.0 before B064.11 and is historical. See [B064 evidence](../../artifacts/conformance/B064-phase-0.1-review-fixes.md) and the [current-wheel report](../../artifacts/releases/0.1.0/evidence/conformance/B064-current-wheel-offline.json).

## B064.8 — Replay committed operation IDs after workspace restore

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B023.3, B064.7.

**Deliverable:** An identical previously committed create/append/mutation operation safely returns its journaled result after managed paths are rebased; changed input still raises the reuse error.

**Acceptance criteria:**

1. An identical previously committed create/append/mutation operation safely returns its journaled result after managed paths are rebased; changed input still raises the reuse error.

**Parent:** B064 · **Parent criteria:** 8.

**Evidence:** The current Sparkless 4.15.2 candidate wheel passes 99 focused B064 regressions under the offline loopback guard. The earlier 415-pass/six-skip full source-suite result used Sparkless 4.15.0 before B064.11 and is historical. See [B064 evidence](../../artifacts/conformance/B064-phase-0.1-review-fixes.md) and the [current-wheel report](../../artifacts/releases/0.1.0/evidence/conformance/B064-current-wheel-offline.json).

## B064.9 — Preserve SQL timestamp fractional precision

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B023.3, B064.8.

**Deliverable:** Timestamp JSON output retains all precision supported by each Arrow timestamp unit, with explicit checks for millisecond and microsecond values.

**Acceptance criteria:**

1. Timestamp JSON output retains all precision supported by each Arrow timestamp unit, with explicit checks for millisecond and microsecond values.

**Parent:** B064 · **Parent criteria:** 9.

**Evidence:** The current Sparkless 4.15.2 candidate wheel passes 99 focused B064 regressions under the offline loopback guard. The earlier 415-pass/six-skip full source-suite result used Sparkless 4.15.0 before B064.11 and is historical. See [B064 evidence](../../artifacts/conformance/B064-phase-0.1-review-fixes.md) and the [current-wheel report](../../artifacts/releases/0.1.0/evidence/conformance/B064-current-wheel-offline.json).

## B064.10 — Requalify and refresh the local 0.1 release candidate

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B023.3, B064.1, B064.2, B064.3, B064.4, B064.5, B064.6, B064.7, B064.8, B064.9.

**Deliverable:** Focused regression and affected phase gates pass offline; the wheel, source archive, manifest and evidence match the reviewed source and accurately report current limitations; no release is published.

**Acceptance criteria:**

1. Focused regression and affected phase gates pass offline; the wheel, source archive, manifest and evidence match the reviewed source and accurately report current limitations; no release is published.

**Parent:** B064 · **Parent criteria:** 10.

**Evidence:** Rebuilt and audited the refreshed 2026-10-22 0.1.0.dev0 wheel/sdist with Sparkless 4.15.2. The installed wheel passed 99 focused B064 tests offline on macOS arm64 CPython 3.11.15; the SQL statement-service source suite passed 35 focused tests. The selected full G1–G4 candidate matrix then passed 624 cases across the base and notebook-extra lanes on CPython 3.11.15/3.12.13. The earlier 415-pass/six-skip full source suite used Sparkless 4.15.0 before B064.11 and is historical. Prior matrix results remain tied to their exact wheel hash. See [B064 evidence](../../artifacts/conformance/B064-phase-0.1-review-fixes.md), [candidate manifest](../../artifacts/releases/0.1.0/manifest.json), and [current candidate reports](../../artifacts/releases/0.1.0/evidence/conformance/sparkless-4.15.2-current/).

## B064.11 — Recover interrupted SQL table creation

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B023.3, B064.10.

**Deliverable:** SQL CREATE retries recover a managed registration left without a Delta snapshot, while preserving external data, malformed Delta logs, and unresolved provider commits.

**Acceptance criteria:**

1. A failed first write removes its newly created managed registration only when the table service confirms no Delta snapshot and no unresolved operation prevents safe cleanup.
2. After a process interruption, retrying CREATE recovers a managed registration with no Delta log and proceeds through the shared journaled Delta service.
3. `CREATE TABLE IF NOT EXISTS` reports a no-op only for an existing registration that opens as a supported Delta snapshot; external or malformed/unresolved tables fail explicitly and retain their data.
4. Focused regression coverage verifies provider rejection, interruption/retry, valid existing tables, and preservation of external data, malformed Delta logs, and unresolved commits.

**Parent:** B064 · **Parent criteria:** 11.

**Evidence:** The SQL statement service module passed 35 focused local tests on Python 3.11.15, Sparkless 4.15.1, delta-rs 1.2.1, PyArrow 23.0.1, and SQLGlot 30.24.0. The same module passed again on Sparkless 4.15.2 after the selected timestamp predicate fix. Coverage includes provider rejection cleanup, retry after an unmaterialized managed registration, valid existing managed snapshots, and preservation of external data, malformed Delta logs, and unresolved operations. No Databricks credentials or remote workspace were used. See [B064.11 evidence](../../artifacts/conformance/B064.11-sql-create-recovery.md).

## B065 — Add a tag-gated PyPI Trusted Publishing workflow

**Milestone:** M3 · **Kind:** task · **Current ledger status:** in_progress

**Dependencies:** B023.3, B064.

**Deliverable:** A version-tagged GitHub Actions release workflow that runs release checks before building and publishing audited wheel/sdist artifacts through PyPI Trusted Publishing.

**Acceptance criteria:**

1. Trigger only on pushed tags matching v*.*.*; reject malformed version tags and tags that do not exactly match the package version.
2. Require planning, lint, formatting, type, and full test checks plus clean wheel installation checks on Linux/macOS and Python 3.11/3.12 before packaging.
3. Audit the built distributions and require the selected BrickHouse project license plus all readiness gates in the version-matched release checklist; the pushed matching tag supplies publication authorization.
4. Build distributions in a non-publishing job, then publish that exact artifact in a separate job with only the permissions needed for PyPI OIDC Trusted Publishing; store no PyPI API token.
5. Document the required PyPI trusted-publisher configuration and preserve current release limitations; do not publish during workflow setup.

**Evidence:** The release workflow parsed successfully; tag trigger, dependency gates, OIDC permission scope, immutable action and image references, and absence of secret references were checked. The first `v0.1.0` run passed checks, all qualification lanes, and artifact audit, but publishing failed before upload because the SHA-pinned composite tried to pull a nonexistent GHCR image tag; no distribution was uploaded. The publisher now uses the verified official `v1.14.2` image digest directly. The corrected release run has passed checks, candidate build/audit, and three of four qualification lanes; the last macOS 3.11 lane is running. Companion tag CI exposed a test-only Linux `/proc` read race; the helper now handles process exit during the read. The focused test passed (1 passed), and Ruff lint/format checks pass. See [release workflow](../../.github/workflows/release.yml).


## B066 — Prepare and qualify the final 0.1.0 release

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B065.

**Deliverable:** A fully qualified 0.1.0 source revision and audited distribution bundle with complete release evidence and explicit remaining owner authorization.

**Acceptance criteria:**

1. Audit all phase 0.1 requirements and run the full current source suite on Python 3.11 and 3.12 with required optional clients enabled; fix failures without weakening acceptance criteria.
2. Qualify clean base and notebook-extra installations on Linux x86_64 and macOS arm64 under OS-enforced offline networking, including declared G1-G4 client and recovery workflows, with reproducible pinned tooling and retained evidence.
3. Prepare version 0.1.0 metadata, owner-selected project license, retained notices, accurate user documentation and release notes, and verified repository/package/trusted-publisher configuration.
4. Freeze and audit final wheel/sdist artifacts, exact qualification hashes and requirement traceability; complete release-readiness records and pass GitHub CI while preserving separate public-publication authorization.

**Required children:** B066.1, B066.2, B066.3, B066.4.



**Final qualification:** [CI run 39873265129](https://github.com/eddiethedean/brickhouse/actions/runs/39873265129) passed 1,368 installed-wheel offline cases and all four 458-case source suites (1,832), zero required skips. Apache-2.0, version 0.1.0, strict metadata/notices checks, byte-identical sdist rebuild, and owner-verified PyPI pending-publisher configuration are complete. Exact artifacts and reports are in the [release bundle](../../artifacts/releases/0.1.0/README.md). No tag or publication is authorized by preparation.

## B066.1 — Audit requirements and complete current full-suite qualification

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B065.

**Deliverable:** Full Python 3.11/3.12 source-suite results and a requirement-to-test traceability audit with required failures resolved.

**Acceptance criteria:**

1. Audit all phase 0.1 requirements and run the full current source suite on Python 3.11 and 3.12 with required optional clients enabled; fix failures without weakening acceptance criteria.

**Parent:** B066 · **Parent criteria:** 1.


## B066.2 — Automate and pass the complete offline platform matrix

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B065, B066.1.

**Deliverable:** Reproducible Linux/macOS base and notebook-extra G1-G4 qualification in CI with pinned clients, OS network isolation, and retained reports.

**Acceptance criteria:**

1. Qualify clean base and notebook-extra installations on Linux x86_64 and macOS arm64 under OS-enforced offline networking, including declared G1-G4 client and recovery workflows, with reproducible pinned tooling and retained evidence.

**Parent:** B066 · **Parent criteria:** 2.

**Required children:** B066.2.1, B066.2.2, B066.2.3.

## B066.3 — Finalize license and 0.1.0 release metadata

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B065, B066.1.

**Deliverable:** Owner-selected license, version 0.1.0 metadata, current user and release documentation, and verified publication configuration.

**Acceptance criteria:**

1. Prepare version 0.1.0 metadata, owner-selected project license, retained notices, accurate user documentation and release notes, and verified repository/package/trusted-publisher configuration.

**Parent:** B066 · **Parent criteria:** 3.

**Required children:** B066.3.1, B066.3.2.


**Publisher verification:** The owner supplied the saved PyPI Pending publishers entry for brickhouse, GitHub eddiethedean/brickhouse, release.yml, environment pypi. All fields match; the GitHub environment accepts v*.*.* tags only. [Configuration evidence](../../artifacts/releases/0.1.0/evidence/B066/publisher-verification.json) is retained. No OIDC upload, tag or publication occurred.

## B066.4 — Freeze final artifacts and release-readiness evidence

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B065, B066.2, B066.3.1.

**Deliverable:** Audited final artifacts and hash-bound qualification bundle, current requirement evidence, and passing GitHub checks without public publication.

**Acceptance criteria:**

1. Freeze and audit final wheel/sdist artifacts, exact qualification hashes and requirement traceability; complete release-readiness records and pass GitHub CI while preserving separate public-publication authorization.

**Parent:** B066 · **Parent criteria:** 4.

**Evidence:** Native-Python CI run 39869421709 passed 1,368 installed-wheel offline cases and four 455-case complete source suites (1,820 total), zero required skips, on Ubuntu 22.04 x86_64/macOS 26 arm64 and CPython 3.11.17/3.12.15. The final evidence audit passes 26 planning/report regressions and all 17 actual orders workflow variants; it validates all 63 release selectors, 253 unique capability references and current backlog states. Updated source-archive qualification and final freeze passed under B066.4. See the [release bundle](../../artifacts/releases/0.1.0/README.md).


**Final qualification:** [CI run 39873265129](https://github.com/eddiethedean/brickhouse/actions/runs/39873265129) passed 1,368 installed-wheel offline cases and all four 458-case source suites (1,832), zero required skips. Apache-2.0, version 0.1.0, strict metadata/notices checks, byte-identical sdist rebuild, and owner-verified PyPI pending-publisher configuration are complete. Exact artifacts and reports are in the [release bundle](../../artifacts/releases/0.1.0/README.md). No tag or publication is authorized by preparation.

## B066.2.1 — Make pinned client fixtures portable

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B065, B066.1.

**Plans:** [21-release-readiness.md](21-release-readiness.md).

**Deliverable:** Portable verified-TLS connector and separate immutable Terraform mirror/writable cache fixtures.

**Acceptance criteria:**

1. Fix the actual macOS 14 connector HTTP-client construction failure and Linux Terraform mirror/cache collision; retain TLS verification and exact client pins.

**Parent:** B066.2 · **Parent criteria:** 1.

## B066.2.2 — Release statement admission before terminal publication

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B065, B066.1.

**Plans:** [21-release-readiness.md](21-release-readiness.md).

**Deliverable:** Deterministic cancellation/success/failure ordering regression and corrected admission release.

**Acceptance criteria:**

1. Ensure a visible terminal statement response has released its warehouse admission after actual engine termination, including warehouse stop cancellation; prove the ordering with a deterministic barrier regression.

**Parent:** B066.2 · **Parent criteria:** 1.

## B066.2.3 — Pass every required offline qualification lane

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B065, B066.1, B066.2.1, B066.2.2.

**Plans:** [21-release-readiness.md](21-release-readiness.md).

**Deliverable:** Zero-skip installed-wheel and full-source Linux/macOS reports bound to one candidate.

**Acceptance criteria:**

1. Pass all original B066.2 platform, Python, installation, client and recovery requirements after the portability and terminal-ordering fixes, retaining exact hashes and isolation evidence.

**Parent:** B066.2 · **Parent criteria:** 1.

## B066.3.1 — Finalize release metadata and truthful diagnostics

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B065, B066.1.

**Plans:** [21-release-readiness.md](21-release-readiness.md).

**Deliverable:** Apache-2.0 and version 0.1.0 metadata, current user/release documentation, and truthful CLI provider/capability diagnostics.

**Acceptance criteria:**

1. Retain the owner-selected license and notices, align package/runtime metadata and user docs, and verify that doctor/capabilities and the reserved run command describe the implemented Sparkless, SQL, bundle and job surfaces accurately.

**Parent:** B066.3 · **Parent criteria:** 1.

**README follow-up (2026-10-22):** Upgraded the project introduction, navigation, source-install quickstart, SDK example, capability/client tables and documentation links. The exact SDK snippet, orders worker/Delta/SQL output, restart persistence and diagnostic commands passed against a fresh local workspace with real-workspace credentials removed. Static checks validated 21 repository-file links, 7 in-page links and all 8 code blocks. Capability states and the frozen candidate scope retain their recorded evidence. See [the handoff log](../../STATUS.md).

## B066.3.2 — Verify GitHub and PyPI publishing configuration

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B065, B066.1, B066.3.1.

**Plans:** [21-release-readiness.md](21-release-readiness.md).

**Deliverable:** Verified repository/package ownership and exact GitHub environment/PyPI Trusted Publisher configuration.

**Acceptance criteria:**

1. Verify the repository, package-name lookup and tag-only GitHub environment, then configure and verify the PyPI publisher for eddiethedean/brickhouse, release.yml and environment pypi without publishing a distribution.

**Parent:** B066.3 · **Parent criteria:** 1.

**Publisher verification:** The owner supplied the saved PyPI Pending publishers entry for brickhouse, GitHub eddiethedean/brickhouse, release.yml, environment pypi. All fields match; the GitHub environment accepts v*.*.* tags only. [Configuration evidence](../../artifacts/releases/0.1.0/evidence/B066/publisher-verification.json) is retained. No OIDC upload, tag or publication occurred.


## B067 — Publish sourced emulator comparison and reproducible local benchmarks

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B066.

**Plans:** [22-local-emulator-comparison.md](22-local-emulator-comparison.md).

**Deliverable:** README feature/result tables, an opt-in local comparison harness, pinned competitor provenance, and raw benchmark evidence with reproduction instructions.

**Acceptance criteria:**

1. Compare BrickHouse, MiniLake and the closest Databricks workspace emulator using exact upstream revisions and primary sources; explicitly mark planned BrickHouse features and distinguish partial or simulated implementation.
2. Implement a bounded opt-in benchmark harness for genuine shared local API operations and the comparable SQL subset, checking returned bytes/results and recording startup, sample distributions, environment, versions, hashes and unavailable configurations without cloud credentials or fabricated timings.
3. Execute the comparison locally, retain all measured samples and setup limitations, verify harness correctness and planning consistency, and publish the measured results and reproducible methodology in the README and detailed benchmark documentation.

**Evidence:** Both full native runs passed and every measured sample is pooled: 30 trials, 1,500 timed samples across the three products, exact returned bytes/aggregate values, restart-file checks and immutable fixture hashes. The README includes explicit planned-feature markers and generated median/p95 results. [Validation record](../../artifacts/benchmarks/2026-10-22-local-emulators/validation.json) and [reproduction/methodology](../benchmarks/local-emulators.md) retain source profiles, limitations and setup probes. No capability is promoted.


## B068 — Profile the measured BrickHouse SQL request latency

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B067.

**Deliverable:** Opt-in reproducible SQL profiler, verified raw samples and a documented wall-time breakdown.

**Acceptance criteria:**

1. Profile the same real local Delta query and HTTP request used by B067 with cloud credentials removed; retain exact source/dependency/tool provenance and verify every result and unchanged fixture.
2. Separate uninstrumented measurements, lightweight nested wall spans and detailed worker cProfile measurements; quantify instrumentation overhead and avoid double counting concurrent waits or nested calls.
3. Retain raw evidence and explain measured bottlenecks and limits in benchmark documentation; validate profiler accounting and planning consistency without changing runtime behavior or capability states.

**Evidence:** All nine trials and 495 exact-result queries passed. The additive
instrumented 29.60 ms mean attributes 21.09 ms to SQLite calls and 4.98 ms to
Sparkless SQL/Arrow boundaries. All raw samples/spans, overhead measurements,
source/tool hashes, detailed profiles and accounting checks are retained in the
[profile report](../benchmarks/sql-profile.md) and [validation record](../../artifacts/benchmarks/2026-10-22-sql-profile/validation.json). No runtime or capability state changed.


## B069 — Benchmark shared in-memory SQLite metadata against disk controls

**Milestone:** M3 · **Kind:** task · **Current ledger status:** done

**Dependencies:** B068.

**Deliverable:** Opt-in isolated SQLite-storage experiment, real query samples, memory/lifetime proofs and comparison report.

**Acceptance criteria:**

1. Run the B067 real Delta SQL request against fresh disk, disk/shared-cache and named shared-memory metadata configurations in rotating order, retaining all raw samples, exact-result checks, source/dependency/tool identities and cleanup evidence without cloud credentials.
2. Verify independent thread connections share complete seeded metadata, memory writes leave the seed file unchanged and disappear after the last connection closes; retain disk durability and shared-cache differences without advertising a supported runtime configuration.
3. Compare uninstrumented timings and nested wall spans, quantify observed capture overhead and explain disk/cache/memory limits; validate accounting and update benchmark/planning documentation without changing production runtime or capability states.

**Evidence:** All 990 queries in 18 fresh API trials returned the exact expected
rows. The baseline medians are 27.15 ms disk, 11.64 ms disk/shared cache and
9.34 ms memory. Storage/thread/last-close probes, immutable fixture/seed checks,
all samples/spans and tool/source hashes are retained in the [report](../benchmarks/sqlite-memory.md)
and [validation record](../../artifacts/benchmarks/2026-10-22-sqlite-memory/validation.json).
Memory is benchmark-only; no runtime or capability state changed.
