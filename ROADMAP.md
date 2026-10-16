# BrickHouse release roadmap

Updated: 2026-10-16. All phases are planned; implementation has not started.

Each phase targets a `0.x` release series. **Phase 0.1 is the initial release, version `0.1.0`.** Later phases initially target `0.2.0` through `0.9.0`; patch releases within a series deliver fixes and refinements to its declared scope. Release readiness depends on working behavior and evidence, with no calendar dates assigned.

This document defines release grouping. The [task ledger](planning/tasks.json) remains authoritative for task definitions, dependencies, and completion; the [backlog](docs/plan/09-task-backlog.md) lists acceptance criteria. The existing [milestones M0–M6](docs/plan/08-roadmap.md) organize engineering gates within these releases. Every release retains earlier supported behavior and its regression checks. Large work packages and epics must be decomposed before implementation.

## Release phases

| Phase | First version | Release outcome | Work packages | Milestones |
|---|---|---|---|---|
| 0.1 | **0.1.0** | Initial local alpha: shared tables, SQL, Python jobs, client workflows, and recovery | B001–B023 | M0–M3 |
| 0.2 | 0.2.0 | Expanded workflow orchestration, local authorization, and audit | B024, B028 | M4 |
| 0.3 | 0.3.0 | MLflow, Python model serving, and dbt | B025–B027 | M4 |
| 0.4 | 0.4.0 | Native vector indexes and bounded feature-engineering research | B029, B038 | M4, M6 |
| 0.5 | 0.5.0 | Durable micro-batches, file ingestion, and selected stateful streaming | B030–B032, B035 | M5 |
| 0.6 | 0.6.0 | Declarative batch pipelines, expectations, and selected CDC | B033–B034 | M5 |
| 0.7 | 0.7.0 | Native Spark Connect, broader SQL protocols, and Delta Sharing subsets | B036–B037, B039 | M6 |
| 0.8 | 0.8.0 | Measured performance, additional platforms/storage, and platform-gap inventory | B040–B041 | M6 |
| 0.9 | 0.9.0 | Consolidated conformance release across the implemented surface | B042 | M6 |

Independent tasks can proceed when their ledger prerequisites are complete. A release phase does not override those prerequisites, and completing an M6 task in phase 0.4 does not complete milestone M6. Deferred scope remains visible and requires an explicit release-scope and dependency revision.

## Phase 0.1 — Initial local alpha (`0.1.0`)

Deliver an installable local Databricks emulator for a bounded set of operations using the initial `spark35-local-v1` profile.

- **Foundation (B001–B005 / M0):** package, CLI help/version, native dependency and client pins, Minilake reuse decision, Sparkless extension feasibility, Delta-provider selection, and personal-account reference capture tooling.
- **Shared data (B006–B012 / M1):** workspace lifecycle, SQLite catalog, native Delta table service, Sparkless session integration, and Statement Execution over the same committed tables.
- **User workflows (B013–B018 / M2):** isolated child-process jobs, durable run state, workspace files, worker-scoped imports, a bounded dbutils subset, optional Python notebook execution, and SDK-driven dependent jobs.
- **Alpha readiness (B019–B023 / M3):** explicit SQL coverage, one bounded CLI/Terraform/bundle workflow, the SQL connector REST profile, concurrent-write/crash recovery, and clean distribution artifacts with compatibility documentation.

**Release gate:** the real pinned SDK can create a catalog/schema, submit a Python transformation, write a managed Delta table, query it through Statement Execution, read it from another session, and repeat after restart. G1–G4 pass for the declared surface, including cancellation/retry recovery, no duplicate committed writes, wheel installation, and JVM-free/offline execution checks. Publish exact client/provider versions, operation limits, third-party notices, and backup/migration guidance.

The notebook dependency B017 remains required by the current B018 workflow gate even though installation uses an optional extra. Likewise, an unproven SQL connector profile must remain blocked or be formally deferred through a scope/dependency revision. A package skeleton alone does not complete phase 0.1.

## Phase 0.2 — Workflows and local governance (`0.2.0`)

**Work packages:** B024, B028. Expand the existing job runner with selected DAG conditions, retries/repair, concurrency, and schedules. Add identities, groups, ownership, grants, enforcement, and audit events across the supported SQL/API/worker/file paths.

**Release gate:** task attempts and values survive the selected retry/repair scenarios; supported scheduling avoids duplicate dispatch after restart. Permission fixtures prove both allowed and denied access with consistent audit records. Document the trusted-code filesystem boundary and keep unsupported task types explicit.

## Phase 0.3 — ML and dbt integrations (`0.3.0`)

**Work packages:** B025–B027. Add real MLflow tracking/registry storage, local Python-model serving, and a dbt-duckdb workflow using the authoritative Delta table service.

**Release gate:** selected clients log and retrieve actual experiments, artifacts, and model versions across restart; a recorded Python model returns schema-validated predictions with lifecycle/failure checks. A real dbt sources/refs/models/tests project passes with tested materializations. G6 passes for each advertised extra, with provider versions and distinct dbt adapter claims.

## Phase 0.4 — Vectors and feature-engineering feasibility (`0.4.0`)

**Work packages:** B029, B038. Implement persistent native vector index lifecycle/query operations and evaluate a bounded set of temporal feature and estimator contracts. Reuse phase 0.2 authorization and phase 0.3 ML state where required.

**Release gate:** deterministic vector fixtures prove filters, scores, dimensions, updates/deletes, restart, and authorization. Delta-sync claims require source-version/recovery evidence. Feature-engineering research records point-in-time semantics and compares selected native results/serialization with versioned oracles; unsupported or infeasible behavior stays explicit. Completion of research does not itself establish a supported estimator API.

## Phase 0.5 — Durable ingestion and streaming (`0.5.0`)

**Work packages:** B030–B032, B035. Add bounded Sparkless micro-batches, durable checkpoints/replay, local file ingestion with a declared `cloudFiles` subset, and selected stateful operators after provider evaluation.

**Release gate:** G5 proves the advertised source/operator/sink combinations through failures and restart. Checkpoints advance only after sink commit; replay, ownership, file replacement/duplicates, schema changes, and state eviction have fixtures. Publish the actual delivery guarantee for each combination and distinguish polling from cloud notification behavior.

## Phase 0.6 — Declarative pipelines and CDC (`0.6.0`)

**Work packages:** B033–B034. Compile selected dataset decorators into a batch pipeline graph that materializes real Delta outputs. Add record/drop/fail expectations and bounded CDC/SCD behavior using durable replay from phase 0.5.

**Release gate:** cycles, refresh, dataset failures, metrics, and restart are exercised with per-table versions. CDC fixtures cover keys, sequence ties, deletes, null updates, and out-of-order input. Preserve documented per-table recovery guarantees without claiming whole-pipeline atomicity.

## Phase 0.7 — Additional native protocols (`0.7.0`)

**Work packages:** B036–B037, B039. Add a bounded native Spark Connect server, selected SQL driver/Thrift behavior, and a Delta Sharing server/client workflow with access lifecycle checks.

**Release gate:** actual pinned independent clients exercise supported plans, sessions, results, errors, cancellation, and cleanup. Sharing fixtures prove authorized snapshots, schema/version/file lifetime, revocation, and unsupported-feature handling. G4 covers the newly advertised protocols. The server execution path remains JVM-free; external JVM clients used for conformance remain separate tooling.

## Phase 0.8 — Platform coverage and performance (`0.8.0`)

**Work packages:** B040–B041. Inventory the remaining public platform contracts and implement explicitly selected feasible slices. Measure and improve startup, latency, memory, and ETL performance; add platforms and optional storage adapters only after evidence.

**Release gate:** record repeatable datasets, hardware, versions, and benchmark variance. Each added Python/OS/architecture combination has native-wheel and golden-workflow evidence; optional Moto/Azurite paths have pinned URI/API limits. The gap inventory preserves unimplemented dashboards/Genie, account services, Lakebase, proprietary/cloud behavior, and other frontier requirements.

## Phase 0.9 — Expanded conformance release (`0.9.0`)

**Work package:** B042, after all its ledger prerequisites are complete. Consolidate the implemented core, integrations, pipelines, protocols, and platform work into a reproducible release with a versioned scope record.

**Release gate:** applicable G1–G6 pass locally for the advertised surface. Verify installation, migrations, examples, optional-provider combinations, notices, and native execution. Report supported, partial, simulated, unsupported, and unexamined cases by domain/profile, retaining unresolved requirements and earlier reference provenance.

Phase 0.9 does not imply complete Databricks parity. Any future `1.0.0` scope and stability commitment require a separate evidence-backed decision.

## Verification and release rules

- Keep one catalog/table authority, Sparkless as the default semantic engine, explicit DuckDB selection, and isolated job sessions across every phase.
- Use the personal Databricks account for occasional bounded reference probes to verify representative behavior and design tests, following [the capture plan](docs/plan/15-personal-databricks-verification.md). Routine tests, required CI, milestone gates, and release tests run locally using reviewed fixtures without personal credentials or live Databricks calls.
- A phase completes only when its work packages and applicable [G1–G6 gates](docs/plan/07-compatibility-and-testing.md) pass with retained evidence. Update [STATUS.md](STATUS.md), the task ledger, and relevant capability records; planned or simulated behavior cannot become supported through a version label.
- Prepare wheels, scope records, and release reports before publication. Release phases do not authorize publication, paid infrastructure, or expanded cloud spend.
