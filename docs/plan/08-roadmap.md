# 08 — Milestone roadmap

## Execution model

[The root ROADMAP.md](../../ROADMAP.md) groups this plan into release phases 0.1–0.9. Phase 0.1 ships initially as **0.1.0** and includes M0–M3 / B001–B023 and B043–B048 research/provider/review work; M0 is internal foundation work within that initial release. Later phases split M4–M6 by outcome. Milestones remain engineering evidence gates, and release grouping does not change task prerequisites or acceptance criteria.

[The phase 0.1 contract](16-phase-0.1-initial-release.md) defines 24 implementation packages with 120 required implementation child tasks, plus B048.1/.2 and B064.1–.11 (133 required phase children total), standalone research/planning/review tasks B043–B045 and B047–B048, seven Python job/ten notebook variants, local fixture limits and 27 acceptance families. B064 and all its children are complete: SQL table-creation recovery is included in the refreshed Sparkless 4.15.2 candidate, which passed 99 focused installed-wheel regressions and the selected 624-case G1–G4 matrix. Linux x86_64 and project-license owner gates remain open. [ADR 0017](../decisions/0017-phase-0.1-delivery-refinement.md) records early connector research, the Python provider binding and the resource/context versus workload-verification split. Original parent prerequisites/acceptance criteria remain required, with appended compute criteria; child tasks inherit their parent's prerequisites. B014.3 is split into DAG scheduling, retry/concurrency admission and restart recovery. Follow actual readiness rather than treating checkpoint order as additional dependencies.

Milestones are evidence gates, not calendar estimates. Implement the smallest useful vertical slice, then expand behavior. Task dependencies live in [the task ledger](../../planning/tasks.json); acceptance criteria live in [the backlog](09-task-backlog.md). A later milestone can contain independent work, but cannot claim completion before its prerequisites and gates pass.

| Milestone | Outcome | Tasks | Exit evidence |
|---|---|---|---|
| M0 — Prove the foundation | Installable skeleton; decisions on native providers, upstream hooks, client versions and compute profiles | B001–B005, B043–B045, B047 | Dependency/wheel matrix; Minilake reuse inventory; runnable engine/Delta/client probes; personal-account reference profile and sanitized captures; compute behavior/release contracts recorded |
| M1 — One coherent data workspace | Catalog, Sparkless, Delta, and SQL share real tables | B006–B012, B046 | G1: write by DataFrame, read by SDK SQL and fresh session, restart and repeat; no JVM/cloud |
| M2 — Execute user workflows | Python jobs, workspace files, dbutils, optional notebooks | B013–B018 | G2: real SDK submission and dependent tasks produce persisted Delta output; isolation and error cases pass |
| M3 — Publishable local alpha | Reproducible installation, measured compatibility, failure recovery, selected clients | B019–B023, B048 | G1–G4 plus bounded architecture review for the declared alpha surface, clean-wheel install, supported platforms, versioned limitations |
| M4 — Broaden workspace services | Advanced jobs, ML, dbt, policy, vectors | B024–B029 | G6 per advertised extra; failures and lifecycle behavior demonstrated |
| M5 — Incremental and declarative data | Durable micro-batches, file discovery, batch pipelines, selected CDC/stateful behavior | B030–B035 | G5 with injected failures and explicit delivery/state guarantees |
| M6 — Broader protocols and platform | Connect, Thrift, feature engineering, sharing, platform gaps, performance | B036–B042 | Native client-to-engine workflows, expanded conformance report, no JVM fallback |

M4–M6 entries are epics. Decompose them into bounded tasks with concrete APIs and fixtures before coding. Several are independent branches; sequence by user demand and evidence rather than implementing every optional subsystem before improving core semantics.

## Critical path

```mermaid
flowchart LR
  A[B001 skeleton] --> B[B003 Sparkless probe]
  A --> C[B004 Delta probe]
  A --> D[B006 workspace]
  D --> E[B007 catalog]
  C --> F[B008 table service]
  E --> F
  B --> G[B009 upstream hooks]
  F --> H[B010 session integration]
  G --> H
  G --> P[B046 Python provider binding]
  P --> H
  H --> I[B011 SQL statements]
  I --> J[B012 shared-data gate]
  J --> K[B018 job workflow gate]
  K --> L[B022 recovery]
  L --> M[B023 alpha readiness]
```

This is an overview, not the complete dependency graph. B002 Minilake audit and B005 client research run independently after B001. No source is copied before its provenance decision. Native worker infrastructure can progress while a Sparkless artifact is being prepared.

B005.1–B005.5 establish occasional personal-account reference capture under [document 15](15-personal-databricks-verification.md). Later tasks reuse reviewed observations to design local tests and request a new bounded probe only for an unresolved behavior or relevant change. Missing reference configuration blocks that capture work, not independent ready tasks. Required milestone and release gates run locally; none requires a live Databricks run or a fresh capture on each invocation.

## Alpha boundary

The alpha's required workflow is the README golden path. Implement a narrow, published list of catalog/SQL/job/dbutils operations, then prove them end to end. Notebook support is an optional extra; a release can omit it only by explicitly revising its scope and documenting that limitation, not by marking B017 done without execution evidence.

B020 defines one small CLI/Terraform/bundle deployment workflow; its completion does not imply all provider resources work. B021 proves the SQL connector's REST mode for an exact client version or records a blocker and formally defers that claim. A failed probe is useful research but cannot count as supported client behavior.

The initial alpha now also requires bounded serverless/classic Python and notebook execution plus serverless/pro/classic warehouse profiles. B011.5–B011.6, B014.4–B014.6, B017.4–B017.5 and B020.4–B020.5 implement and qualify these slices. B045 resolves transport feasibility early, and B046 must supply the public Python provider artifact before B010 integration. Broader environments/compute access and retry policies belong to phase 0.2, streaming/pipeline differences to 0.5–0.6, and native Connect semantics to 0.7. Metadata simulation cannot satisfy the local workload gates.

Before release, choose the exact advertised surface in a release scope record. Deferred work stays planned/blocked with its evidence; update the milestone and dependency records transparently. No milestone is complete merely because an unimplemented requirement was hidden.

## Release gates

- Versioned capability report with tests and limitations for each advertised operation.
- One authoritative table visible across all advertised interfaces.
- Clean installation on each advertised Python/OS/architecture combination.
- Base workflow without Java, Spark, Docker, or remote compute; separate offline gate after provisioning.
- Durable restart and uncertain-commit handling, cancellation, bounded logs/results, and cleanup.
- Native dependency versions and third-party notices included.
- Documented backup/restore and migration behavior for the supported workspace version.

A release candidate is a local build and report. Publishing to PyPI/GitHub requires the owner's authorization at that stage. The initial release is version 0.1.0; later version targets follow the root release roadmap. A future 1.0 scope requires its own evidence-backed decision and does not automatically imply full Databricks emulation.

## Post-stability deployment milestone

**M7 — Self-hosted operation** is a separate future track, not a prerequisite for M0–M6. [Document 18](18-self-hosted-platform.md) maps preparation to every emulator phase while preserving their existing outcomes. B047 records the plan; B048 adds a bounded code/evidence architecture review before B023 without deployment implementation.

After B042, B049/SH.0 must record actual emulator stability and bounded deployment scope. Only then do B050–B055 implement SH.1–SH.6: authenticated trusted single-server service (S1), browser/integrations (S2), tested execution isolation (S3), remote workers/durable automation (S4), operational qualification (S5), and a consolidated candidate (S6). All deployment implementation is transitively gated on B049; B042 does not depend on M7. No package versions or dates are assigned to these stages. Existing local/native execution and one-catalog/commit invariants apply throughout.

## Optional native cluster milestone

**M8 — Native distributed queries** is separate from emulator M0–M6 and self-hosted M7. B056 records planning only. B057–B061 / SC.0–SC.4 qualify generic Sparkless partition planning, exchanges, recovery, provider-owned sinks and native artifacts after B049 stability; B062 / SC.5 integrates only after both B061 and B053. [Document 19](19-sparkless-cluster-execution.md) defines K0–K5 and [the upstream specification](../upstream/sparkless-cluster-contracts.md) records actual 4.14.1 gaps. Existing emulator and SH release gates do not depend on M8. No cluster implementation or new 0.1 workload requirement is introduced.

## Open-source reuse qualification across phases

B063 is planning/research only. [Document 20](20-open-source-reuse.md) assigns exact native candidate decisions to the existing work: parser/notebooks/client qualification in 0.1 without new behavior; UC contract/policy in 0.2; MLflow/serving and independently locked dbt profiles in 0.3; native Connect/sharing client graphs in 0.7; optional Iceberg/Kernel and native-platform gaps in 0.8. SH providers retain authentication/state restore and SC.0 explicitly compares Ballista/DataFusion before equivalent infrastructure is built. Existing phase requirements and stability/integration barriers remain intact. Original task criteria/evidence and all 133 required phase children are preserved: 120 implementation children, the two B048 children, and eleven B064 review-fix children.
