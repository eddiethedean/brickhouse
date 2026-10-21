# BrickHouse release roadmap

Updated: 2026-10-20. Phase 0.1 implementation is in progress; the task ledger records completed slices. Broad serverless/classic workload compatibility remains incomplete. **JVM-free Databricks emulation is the primary goal. Self-hosted deployment follows emulator stability.**

Each emulator phase targets a `0.x` release series. **Phase 0.1 is the initial release, version `0.1.0`.** Later emulator phases initially target `0.2.0` through `0.9.0`; patch releases within a series deliver fixes and refinements to its declared scope. A separate SH.0–SH.6 track has no assigned package versions and starts only after recorded emulator stability. Release readiness depends on working behavior and evidence, with no calendar dates assigned.

This document defines release grouping. The [task ledger](planning/tasks.json) remains authoritative for task definitions, dependencies, and completion; the [backlog](docs/plan/09-task-backlog.md) lists acceptance criteria. The existing [milestones M0–M6](docs/plan/08-roadmap.md) organize engineering gates within these releases. Every release retains earlier supported behavior and its regression checks. Large work packages and epics must be decomposed before implementation.

## Release phases

| Phase | First version | Release outcome | Work packages | Milestones |
|---|---|---|---|---|
| 0.1 | **0.1.0** | Initial local alpha: shared tables, SQL, bounded serverless/classic jobs and notebooks, client workflows, recovery and bounded architecture review | B001–B023, B043–B048 | M0–M3 |
| 0.2 | 0.2.0 | Expanded workflow orchestration, local authorization, and audit | B024, B028 | M4 |
| 0.3 | 0.3.0 | MLflow, Python model serving, and dbt | B025–B027 | M4 |
| 0.4 | 0.4.0 | Native vector indexes and bounded feature-engineering research | B029, B038 | M4, M6 |
| 0.5 | 0.5.0 | Durable micro-batches, file ingestion, and selected stateful streaming | B030–B032, B035 | M5 |
| 0.6 | 0.6.0 | Declarative batch pipelines, expectations, and selected CDC | B033–B034 | M5 |
| 0.7 | 0.7.0 | Native Spark Connect, broader SQL protocols, and Delta Sharing subsets | B036–B037, B039 | M6 |
| 0.8 | 0.8.0 | Measured performance, additional platforms/storage, and platform-gap inventory | B040–B041 | M6 |
| 0.9 | 0.9.0 | Consolidated conformance release across the implemented surface | B042 | M6 |

Independent tasks can proceed when their ledger prerequisites are complete. A release phase does not override those prerequisites, and completing an M6 task in phase 0.4 does not complete milestone M6. Deferred scope remains visible and requires an explicit release-scope and dependency revision.

## Deployment preparation within emulator phases

[Document 18](docs/plan/18-self-hosted-platform.md) maps every emulator phase to a boundary worth preserving: shared services and contained local runtime/storage in 0.1; identity and audit in 0.2; optional-provider authority in 0.3; index/feature ownership in 0.4; durable checkpoint ownership in 0.5; dataset-level recovery in 0.6; consistent protocol/session policy in 0.7; measured platform/storage limits in 0.8; and reproducible conformance evidence in 0.9. These reviews use the emulator's existing work. They do not add deployment infrastructure to these releases.

B047 records this plan. B048 is a bounded review before B023, using implemented G1–G4 paths and code references; it must not introduce speculative backends or an unaudited security claim. Existing compatibility requirements and all 85 required implementation children remain. [ADR 0034](docs/decisions/0034-emulator-first-self-hosted-platform.md) records the priority and stability barrier.

## Compute behavior across releases

[Document 17](docs/plan/17-serverless-and-classic-compute.md) commits to serverless and classic Python/notebook behavior and serverless/pro/classic SQL warehouses. Phase 0.1 requires bounded local execution under each selected profile, validated compute/environment/runtime selection, settings and lifecycle errors. B043 supplies the source research and planning decision. [The refined release contract](docs/plan/16-phase-0.1-initial-release.md) adds early connector feasibility (B045), the Python provider binding (B046), and separate resource/context and workload-verification tasks. The initial implementation packages now have 85 required children after the bounded B014.3 scheduler splits; B044 records the earlier refinement. Infrastructure records and allocation are explicitly simulated; successful workloads require actual code, queries and committed table data.

Phase 0.2 expands environments, compute permissions and automatic retries; phases 0.5–0.6 carry mode restrictions into streaming/pipelines; phase 0.7 proves native Connect and analysis/binding semantics. Compute mode, access mode, environment/runtime and protocol are separate compatibility dimensions. Missing modes cannot be silently moved into phase 0.8's general gap inventory. [ADR 0016](docs/decisions/0016-serverless-and-classic-compute-profiles.md) records this explicit addition to the release contract.

## Phase 0.1 — Initial local alpha (`0.1.0`)

Deliver an installable local Databricks emulator for a bounded set of operations using the initial `spark35-local-v1` profile.

[The detailed 0.1 implementation contract](docs/plan/16-phase-0.1-initial-release.md) defines the minimum behavior corpus, synthetic golden workflow, failure matrix, and release checklist. B001–B023 are decomposed into required child tasks in the ledger/backlog; implementation began with **B001.1**. B043–B045 provide compute research, delivery refinement and early connector feasibility; B046 owns Python provider artifact and typed-contract qualification; the published 4.14.0 API exists, but nullable-struct bugs remain [recorded blockers](artifacts/conformance/B046.3-sparkless-4.14.0-provider.md). Original parent acceptance criteria and prerequisite edges remain required. [ADR 0017](docs/decisions/0017-phase-0.1-delivery-refinement.md) records the bounded implementation split and evidence ownership.

- **Foundation (B001–B005 / M0):** package, CLI help/version, native dependency and client pins, Minilake reuse decision, Sparkless extension feasibility, Delta-provider selection, and personal-account reference capture tooling.
- **Shared data (B006–B012, B046 / M1):** workspace lifecycle, SQLite catalog, native Delta table service, Sparkless session integration, and Statement Execution bound to serverless/pro/classic local warehouse records over the same committed tables.
- **User workflows (B013–B018 / M2):** isolated child-process jobs, durable run state, workspace files, worker-scoped imports, a bounded dbutils subset, optional Python notebook execution, and SDK-driven dependent jobs under bounded serverless and classic compute profiles.
- **Alpha readiness (B019–B023 / M3):** explicit SQL coverage, one bounded CLI/Terraform/bundle workflow, the SQL connector REST profile, concurrent-write/crash recovery, and clean distribution artifacts with compatibility documentation.

**Release gate:** the real pinned SDK can create a catalog/schema, submit a Python transformation, write a managed Delta table, query it through Statement Execution, read it from another session, and repeat after restart. G1–G4 pass for the declared surface, including cancellation/retry recovery, no duplicate committed writes, wheel installation, and JVM-free/offline execution checks. Publish exact client/provider versions, operation limits, third-party notices, and backup/migration guidance.

The notebook dependency B017 remains required by the current B018 workflow gate even though installation uses an optional extra. Likewise, an unproven SQL connector profile must remain blocked or be formally deferred through a scope/dependency revision. A package skeleton alone does not complete phase 0.1.

Run the release corpus under the selected serverless and classic new/existing/shared-compute profiles and all three warehouse types, including notebook-extra variants and invalid selectors/settings/resources. Publish the limits of local serverless behavior separately from later Connect protocol compatibility. Infrastructure simulation alone cannot pass this gate.

| Internal checkpoint | Work packages | Completion signal |
|---|---|---|
| A — Prerequisites | B013, B043–B046 | Scoped worker supervision, early connector decision, tested Python provider artifact |
| B — Shared data binding | B010 | Typed shared-table reads/writes and isolated sessions |
| C — SQL and warehouses | B011–B012 | Real statements under three warehouse types; G1 restart agreement |
| D — Jobs and utilities | B014–B016 | Seven real Python compute variants, scoped utilities and worker/resource lifetimes |
| E — Notebooks and full workflow | B017–B018 | Ten notebook variants plus base/extra G2 evidence |
| F — SQL corpus and clients | B019–B021 | Enumerated mutations, real Terraform/bundle/connector workflows and G4 |
| G — Recovery and candidate | B022–B023 | G3 plus consolidated candidate install/offline/scope evidence |

These checkpoints organize reviews within phase 0.1; independent ready tasks follow the ledger rather than a strict A-to-G schedule. The minimum demo transforms synthetic decimal-valued orders, writes one managed Delta table, reads it from a dependent job and SQL, then verifies the same committed data after restart. Failure coverage includes uncertain commits, worker crashes/cancellation, alias-concurrent writes, scoped secrets, and restored data.

**Future deployment boundary:** B048 reviews the actual local service/runtime/storage boundaries and identity entry points before B023. It adds no UI, remote workers, network authentication or deployment runtime to 0.1. [Document 18](docs/plan/18-self-hosted-platform.md) records later adaptation points.

## Phase 0.2 — Workflows and local governance (`0.2.0`)

**Work packages:** B024, B028. Expand the existing job runner with selected DAG conditions, retries/repair, concurrency, and schedules. Add locked environment provisioning/cache/reset, selected classic dependency modes, bounded serverless optimization retries, and compute-object policies. Add identities, groups, ownership, grants, enforcement, and audit events across the supported SQL/API/worker/file paths, distinguishing classic compute, interactive/automated serverless compute, and SQL warehouse permissions.

**Release gate:** task attempts and values survive the selected retry/repair scenarios; supported scheduling avoids duplicate dispatch after restart. Permission fixtures prove both allowed and denied access with consistent audit records. Document the trusted-code filesystem boundary and keep unsupported task types explicit.

Environment and compute-policy fixtures must exercise actual local environments, admission and attempts. An accepted access-mode label cannot substitute for an enforced identity decision.

**Future deployment boundary:** document identity provenance and consistent policy/audit across interfaces. Authorization here remains emulator behavior on a trusted machine; authentication infrastructure and code sandboxing are post-stability work.

## Phase 0.3 — ML and dbt integrations (`0.3.0`)

**Work packages:** B025–B027. Add real MLflow tracking/registry storage, local Python-model serving, and a dbt-duckdb workflow using the authoritative Delta table service.

**Release gate:** selected clients log and retrieve actual experiments, artifacts, and model versions across restart; a recorded Python model returns schema-validated predictions with lifecycle/failure checks. A real dbt sources/refs/models/tests project passes with tested materializations. G6 passes for each advertised extra, with provider versions and distinct dbt adapter claims.

**Future deployment boundary:** retain each provider's authority, scoped configuration, artifact locations and backup/lifecycle responsibilities. Hosted optional services and a browser workspace remain post-stability work.

## Phase 0.4 — Vectors and feature-engineering feasibility (`0.4.0`)

**Work packages:** B029, B038. Implement persistent native vector index lifecycle/query operations and evaluate a bounded set of temporal feature and estimator contracts. Reuse phase 0.2 authorization and phase 0.3 ML state where required.

**Release gate:** deterministic vector fixtures prove filters, scores, dimensions, updates/deletes, restart, and authorization. Delta-sync claims require source-version/recovery evidence. Feature-engineering research records point-in-time semantics and compares selected native results/serialization with versioned oracles; unsupported or infeasible behavior stays explicit. Completion of research does not itself establish a supported estimator API.

**Future deployment boundary:** preserve index/model/table ownership and source-version recovery. Vector/feature behavior does not establish shared-host execution isolation.

## Phase 0.5 — Durable ingestion and streaming (`0.5.0`)

**Work packages:** B030–B032, B035. Add bounded Sparkless micro-batches, durable checkpoints/replay, local file ingestion with a declared `cloudFiles` subset, and selected stateful operators after provider evaluation.

Declare serverless bounded-trigger and classic trigger/runtime support independently; keep rejected trigger combinations in the local fixture corpus.

**Release gate:** G5 proves the advertised source/operator/sink combinations through failures and restart. Checkpoints advance only after sink commit; replay, ownership, file replacement/duplicates, schema changes, and state eviction have fixtures. Publish the actual delivery guarantee for each combination and distinguish polling from cloud notification behavior.

**Future deployment boundary:** keep checkpoints and source/batch identity durable and reject competing local owners. Distributed leases and worker reassignment are post-stability requirements.

## Phase 0.6 — Declarative pipelines and CDC (`0.6.0`)

**Work packages:** B033–B034. Compile selected dataset decorators into a batch pipeline graph that materializes real Delta outputs. Add record/drop/fail expectations and bounded CDC/SCD behavior using durable replay from phase 0.5.

Resolve selected classic/serverless pipeline environments and settings through the same compute services and verify real outputs under each advertised mode.

**Release gate:** cycles, refresh, dataset failures, metrics, and restart are exercised with per-table versions. CDC fixtures cover keys, sequence ties, deletes, null updates, and out-of-order input. Preserve documented per-table recovery guarantees without claiming whole-pipeline atomicity.

**Future deployment boundary:** version pipeline definitions/environments and preserve dataset-level commits and partial-run recovery. A future deployment scheduler must consume these contracts rather than replace them.

## Phase 0.7 — Additional native protocols (`0.7.0`)

**Work packages:** B036–B037, B039. Add a bounded native Spark Connect server, selected SQL driver/Thrift behavior, and a Delta Sharing server/client workflow with access lifecycle checks.

B036 also proves reference-specific deferred/eager analysis, temporary-view binding and UDF capture for serverless and applicable classic runtimes; compute mode alone does not select these semantics.

**Release gate:** actual pinned independent clients exercise supported plans, sessions, results, errors, cancellation, and cleanup. Sharing fixtures prove authorized snapshots, schema/version/file lifetime, revocation, and unsupported-feature handling. G4 covers the newly advertised protocols. The server execution path remains JVM-free; external JVM clients used for conformance remain separate tooling.

**Future deployment boundary:** reuse session lifecycle, result ownership, cancellation and local policy across protocol adapters. Public-network exposure needs the later deployed-profile authentication and denial tests.

## Phase 0.8 — Platform coverage and performance (`0.8.0`)

**Work packages:** B040–B041. Inventory the remaining public platform contracts and implement explicitly selected feasible slices. Measure and improve startup, latency, memory, and ETL performance; add platforms and optional storage adapters only after evidence.

**Release gate:** record repeatable datasets, hardware, versions, and benchmark variance. Each added Python/OS/architecture combination has native-wheel and golden-workflow evidence; optional Moto/Azurite paths have pinned URI/API limits. The gap inventory preserves unimplemented dashboards/Genie, account services, Lakebase, proprietary/cloud behavior, and other frontier requirements.

**Future deployment boundary:** qualify real Python/native storage paths and measured resource limits; preserve single-owner local metadata. This phase does not require a replacement database, HA, remote workers or distributed query execution.

## Phase 0.9 — Expanded conformance release (`0.9.0`)

**Work package:** B042, after all its ledger prerequisites are complete. Consolidate the implemented core, integrations, pipelines, protocols, and platform work into a reproducible release with a versioned scope record.

**Release gate:** applicable G1–G6 pass locally for the advertised surface. Verify installation, migrations, examples, optional-provider combinations, notices, and native execution. Report supported, partial, simulated, unsupported, and unexamined cases by domain/profile, retaining unresolved requirements and earlier reference provenance.

Phase 0.9 does not imply complete Databricks parity. Any future `1.0.0` scope and stability commitment require a separate evidence-backed decision.

**Future deployment boundary:** preserve the reproducible emulator candidate, compatibility denominator, defect inventory and install/upgrade/restore evidence for B049. B042 has no dependency on deployment implementation.

## Post-stability self-hosted track

This track starts **after emulator stability**, assessed by B049 after B042. It does not replace phases 0.1–0.9, imply complete Databricks parity or automatically declare 1.0. Stage labels are dependency order, with package versions deliberately unassigned. B050–B055 require bounded decomposition before implementation. [Document 18](docs/plan/18-self-hosted-platform.md) specifies trust profiles, architecture evolution and gate evidence.

| Phase | First version | Outcome | Work packages | Entry prerequisite |
|---|---|---|---|---|
| SH.0 | Unassigned | Recorded emulator stability and bounded deployment scope | B049 | B042, B048 |
| SH.1 | Unassigned | Authenticated single-server service for trusted code authors; S1 | B050 | B049 |
| SH.2 | Unassigned | Browser workspace and deployed optional integrations; S2 | B051 | B050 |
| SH.3 | Unassigned | Tested execution isolation and resource controls for shared use; S3 | B052 | B051 |
| SH.4 | Unassigned | Authenticated remote job workers, data transport and durable automation; S4 | B053 | B052 |
| SH.5 | Unassigned | Upgrade/restore, rotation, observability and capacity qualification; S5 | B054 | B053 |
| SH.6 | Unassigned | Consolidated self-hosted distribution candidate and operator handoff; S6 | B055 | B054 |

The local wheel stays independently usable without a deployment runtime, identity service or cloud account. Every deployment path stays JVM-free, uses Sparkless as its default semantic engine and retains one catalog/table-commit authority. Remote job execution does not imply distributing a single query. High availability, a replacement metadata backend and broader platform/product coverage require separate evidence-backed decisions.

## Verification and release rules

- Keep one catalog/table authority, Sparkless as the default semantic engine, explicit DuckDB selection, and isolated job sessions across every phase.
- Use the personal Databricks account for occasional bounded reference probes to verify representative behavior and design tests, following [the capture plan](docs/plan/15-personal-databricks-verification.md). Routine tests, required CI, milestone gates, and release tests run locally using reviewed fixtures without personal credentials or live Databricks calls.
- A phase completes only when its work packages and applicable [G1–G6 gates](docs/plan/07-compatibility-and-testing.md) pass with retained evidence. Update [STATUS.md](STATUS.md), the task ledger, and relevant capability records; planned or simulated behavior cannot become supported through a version label.
- Prepare wheels, scope records, and release reports before publication. Release phases do not authorize publication, paid infrastructure, or expanded cloud spend.
