# 13 — Decisions and risks

## Adopted planning defaults

These decisions guide implementation until evidence justifies a recorded change. They are not claims of completed software.

| ID | Decision | Rationale / revisit condition |
|---|---|---|
| D001 | One `brickhouse` distribution with internal subpackages and optional extras | Reduce version skew; split only at an independently useful stable boundary |
| D002 | Sparkless is the default semantic engine | Owner can supply generic missing hooks; requires actual upstream artifact and conformance |
| D003 | Selectively adapt pinned Minilake code/contracts | Avoid its competing catalog and Spark launch paths; confirm inventory in B002 |
| D004 | SQLite metadata plus real Delta managed/external tables | Shared table identity and data across interfaces; no duplicate DuckDB physical catalog |
| D005 | Python `deltalake==1.2.1` is the phase 0.1 mutation provider; Sparkless remains the semantic reader | One commit authority; preflight transaction-version checks are required because a raw repeated write duplicates rows. See [ADR 0008](../decisions/0008-delta-mutation-and-recovery.md) |
| D006 | Child process per job, explicit scoped worker context | Isolate state/import facades and enable cancellation; not a hostile-code sandbox |
| D007 | Explicit capabilities and unsupported errors | Compatibility must be observable, testable, and versioned |
| D008 | Start with a Spark 3.5-based local profile | Matches reviewed default; add 4.x separately after evidence |
| D009 | Base installation has no Java/Docker/cloud requirement | Optional services cannot enter a claimed base execution path |
| D010 | Local trusted-user workspace first | Multi-user governance and sandboxed code need additional engineering |
| D011 | Personal Databricks account supplies occasional behavioral reference captures; routine tests stay local | Owner authorized reference verification and test design; remote latency favors sanitized reusable fixtures. See [ADR 0001](../decisions/0001-personal-databricks-reference-verification.md) and [capture plan](15-personal-databricks-verification.md) |
| D012 | Root ROADMAP.md groups work into phases 0.1–0.9; initial phase 0.1 ships as 0.1.0 | Preserve the existing B001–B023 alpha scope and task prerequisites; see [ADR 0002](../decisions/0002-release-phases.md) and [release roadmap](../../ROADMAP.md) |
| D013 | Preserve phase 0.1 scope and refine B001–B023 into 68 required children with criterion coverage | Make implementation bounded and release readiness concrete; see [ADR 0003](../decisions/0003-phase-0.1-execution-contract.md) and [phase contract](16-phase-0.1-initial-release.md) |
| D014 | Explicit serverless/classic workload and three SQL warehouse profiles, with a bounded 0.1 subset and later policy/protocol gates | The owner's requested expansion adds B043 and five required children, bringing the initial count to 73. Keep compute/access/environment/protocol dimensions and real execution separate from infrastructure simulation; see [ADR 0016](../decisions/0016-serverless-and-classic-compute-profiles.md) and [document 17](17-serverless-and-classic-compute.md) |
| D015 | Refine 0.1 into explicit early risks, provider/consumer boundaries, 80 required children and a finite release corpus | Preserve all parent requirements and existing evidence; B045 resolves connector transport, B046 supplies the Python bridge, and document 16 fixes variants/limits/case owners. See [ADR 0017](../decisions/0017-phase-0.1-delivery-refinement.md) |
| D016 | Verify published Sparkless artifacts as exact developer candidates; retain typed/provider gates instead of a redundant source build | The 4.14.0 report preserves the failed nullable-struct cases; the 4.14.1 addendum verifies both fixes and the B010.1 read adapter. See [ADR 0018](../decisions/0018-published-sparkless-python-provider.md), [4.14.0 history](../../artifacts/conformance/B046.3-sparkless-4.14.0-provider.md), and [4.14.1 evidence](../../artifacts/conformance/B046.3-sparkless-4.14.1-provider.md) |
| D017 | Keep worker supervision in the parent and persist observed local outcomes; explicitly limit descendant cleanup guarantees to POSIX process groups | No public job route, durable user log or restart reconciliation exists yet; see [ADR 0020](../decisions/0020-worker-supervision-and-outcomes.md) and [B013.3](../../artifacts/conformance/B013.3-worker-supervision.md) |
| D018 | Contain the pinned SQL kernel's private custom-CA and nonstandard loopback host/port use in the B045 research harness | A real session-open request was captured, but SQL results remain unverified; see [ADR 0021](../decisions/0021-sql-kernel-local-transport-adaptation.md) and [B045](../../artifacts/conformance/B045-sql-connector-loopback.md) |
| D019 | Store immutable job definitions/submissions and truthful task/attempt state in one workspace metadata database, with DB-enforced transitions and explicit interruption | B014.1 implements persistence only; public dispatch, retries, queue recovery and output remain later work. See [ADR 0022](../decisions/0022-durable-job-state.md) and [B014.1](../../artifacts/conformance/B014.1-durable-job-state.md) |
| D020 | Give the pinned Terraform provider a simulated local workspace identity and stable job IDs backed by append-only definition revisions | Preserve BrickHouse job immutability while proving the exact workspace-file and single-task Python-job lifecycle; see [ADR 0035](../decisions/0035-terraform-local-resource-identity.md) and [B020.2](../../artifacts/conformance/B020.2-terraform-v1.16.6-provider-v1.137.0.md) |
| D021 | JVM-free Databricks emulator first; actual self-hosted implementation only after a recorded stability gate | Preserve phases 0.1–0.9, add bounded B048 review before B023, and gate B050–B055 through B049 after B042. See [ADR 0034](../decisions/0034-emulator-first-self-hosted-platform.md) and [document 18](18-self-hosted-platform.md). |
| D022 | Verify one real CLI bundle lifecycle locally with actual Python output and immutable destroy history | Bound B020.3 to a classic single-task Python bundle, simulated identity/compute and explicitly rejected unsupported resources; see [ADR 0036](../decisions/0036-local-cli-bundle-profile.md) and [B020.3](../../artifacts/conformance/B020.3-databricks-cli-bundle-v1.21.0.md) |

## Open decisions with owners and gates

| Question | Task / owner | Required decision artifact |
|---|---|---|
| Which Minilake files, if any, are worth adapting? | B002 / BrickHouse | Module inventory, coupling analysis, notices and selected endpoint contracts |
| Which Sparkless extension shape is viable? | B003+B009 / coordinated Sparkless | Small proven provider interface, concurrency/GIL/error contract, artifact reference |
| Which mutation-provider features and storage targets can BrickHouse safely support? | B004/B008 / BrickHouse | Validate protocol/features, transaction retention, cloud atomicity, external-writer concurrency, and recovery states against the chosen Python provider |
| Which SDK/CLI/Terraform/SQL connector versions? | B005 / BrickHouse | Exact pins and request fixtures; REST connector transport probe |
| Which personal-workspace host, features, and existing compute are permitted for captures? | B005 / BrickHouse+owner configuration | Verified non-secret capture profile, bounded resource policy, and recorded account limitations; token source already owner-supplied |
| How broad is initial SQL? | B019 / BrickHouse | Explicit operation/type corpus with mismatches, no fallback ambiguity |
| How will optional services share identities and data? | M4 epics / BrickHouse | Per-provider authority and lifecycle design before implementation |
| Which streaming provider and delivery guarantees? | B030–B035 / BrickHouse | Replay/state proof for chosen source/sink/operator combinations |
| Project license and distribution ownership? | Owner before public release | Confirmed license, notices, package/repository ownership |
| When is the emulator stable enough to begin self-hosting? | B049 / BrickHouse after B042 | Exact candidate/corpus, repeated conformance and upgrade/restore evidence, defect triage, stability/maintenance policy and bounded deployment scope; phase 0.9 alone is insufficient |
| Which deployment identity, isolation and storage mechanisms? | B050–B054 / BrickHouse after B049 | Finite supported threat/platform matrix, real authentication/denial/isolation and multi-host recovery tests, explicit operational limits; no speculative implementation in emulator phases |

Proceed with reversible work using these defaults. Escalate a decision only when evidence changes scope, creates an external commitment, or requires owner-specific information. Missing final publication details do not block implementation planning.

## Risk register

| Risk | Consequence | Mitigation / evidence gate |
|---|---|---|
| Semantic differences between engines | Locally passing tests give incorrect results | Sparkless default, deterministic routing, differential fixtures, partial capability flags |
| Personal reference account is slow, changes, or lacks a feature | Feedback stalls or one account's behavior is overgeneralized | Occasional small probes, versioned sanitized fixtures, explicit feature/profile limits; routine tests run locally |
| Personal credentials or account data reach routine CI/artifacts | Credential exposure or accidental remote execution | Ignore .env/raw captures, isolate capture-job secrets, bind local clients explicitly, redact before upload |
| Catalog/data divergence after crash | Missing or duplicated table results | Operation journal, versioned handles, recovery and uncertain-commit tests |
| Delta feature mismatch | Silent data corruption or wrong reads | Feature detection/rejection before execution; cross-provider fixtures |
| Upstream APIs too narrow | Integration depends on brittle internals | Generic owned Sparkless hooks; contained external adapters; avoid required external PRs |
| Minilake adaptation expands into an unmanaged fork | Upgrade cost and hidden Spark paths | Small provenance inventory, delete unused execution code, contract tests |
| Arrow conversions materialize large inputs | Memory blowups and slow jobs | Batch bounds, spill/staging, measured type fidelity and memory |
| Process-global state leaks | Different jobs or sessions interfere | Instance-scoped services, child workers, concurrent isolation tests |
| Import facade masks unsupported behavior | Incorrect claims of PySpark compatibility | Worker-only activation, explicit namespaces and error cases |
| Client transport assumptions are wrong | SDK works while SQL connector/dbt fails | Pin real clients; REST/Thrift/Connect tracked independently |
| Retries repeat side effects | Duplicate data or external actions | Idempotent table operations; no blanket job exactly-once claim |
| Delta transaction IDs are mistaken for automatic deduplication | Replayed appends can add duplicate rows | B004.3 reproduced a duplicate on raw same-version replay; B008 must query the table-scoped app transaction before dispatch and persist uncertain outcomes |
| External wheels/services change | Installation or JVM-free guarantee breaks | Version matrix, clean install CI, dependency/process inspection |
| Scope grows faster than evidence | Large facade with little useful execution | Vertical gates, task decomposition, keep deferred frontier visible |
| Serverless/classic labels conceal different runtime or Connect behavior | Wrong errors, defaults, view binding or deployment tests pass locally | Versioned compute/access/environment/protocol combinations; separate workload and simulation capabilities; document 17's positive/error corpus and B036 gate |
| One serverless SQL reference is generalized to Python or classic compute | Unsupported compatibility claims and accidental paid provisioning | Keep oracle gaps explicit, use source research for design only, and retain document 15's existing-resource capture policy |
| Local access mistaken for production security | Service policy bypass via Python/filesystem | Trusted-local profile documented; separate governance/security work |
| Deployment displaces the emulator goal | Initial releases accumulate UI/infrastructure work before correct native behavior | Preserve 0.1–0.9 emulator gates; B048 is review only, and all deployment implementation depends on B049 after B042 |
| Authentication or access-mode labels mistaken for code isolation | A logged-in job bypasses permissions through host files/network | Separate trusted-server and isolated-server profiles; SH.3 requires a real tested execution boundary and explicit residual risks |
| Local process/SQLite assumptions are treated as distributed coordination | Stale workers mutate tables or two control owners corrupt state | Keep local metadata ownership, qualify attempt/checkpoint fencing and native data transport in SH.4; no shared network SQLite or untested HA claim |

## Decision change protocol

Write an ADR under `docs/decisions/` with context, options, evidence, chosen contract, consequences, and migration impact. Link it from this document, affected task entries, and capability records. Preserve the original planning rationale in Git history. A failed experiment can close a research task when its deliverable is a decision; it cannot close an implementation task that promises working behavior.

## Native cluster planning decision

**D023 / [ADR 0037](../decisions/0037-native-sparkless-cluster-plan.md):** plan generic native Sparkless clusters separately from remote job placement, while preserving emulator priority and the B049 stability gate. [Document 19](19-sparkless-cluster-execution.md) and [the upstream specification](../upstream/sparkless-cluster-contracts.md) define optional B057–B062 / SC.0–SC.5. Integration B062 additionally requires both B061's qualified engine and B053's remote-execution foundation; existing emulator/SH gates do not depend on the new track.

Key risks are premature use of local repartition/provider hooks as distributed contracts, coordinator materialization that defeats scale, cross-partition semantic differences, stale task publication, and repeated sink commits. K0–K5 require explicit typed physical plans, measured bounded exchange/spill, real cross-host overlapping-key fixtures, winning-attempt manifests and provider journal reconciliation. Architecture/transport selection remains SC.0's evidence-backed decision; no third-party distributed backend is chosen here.

## Research-backed reuse decision

**D024 / [ADR 0038](../decisions/0038-native-open-source-reuse.md):** retain proven core pins; qualify direct Jupyter components and a real optional MLflow service; isolate incompatible dbt client environments; use UC contracts without its Java server; qualify native sharing wrappers; and evaluate Iceberg/Kernel or Ballista/DataFusion only in their bounded future slices. [Document 20](20-open-source-reuse.md) and [the provenance snapshot](../../planning/oss-reuse.json) record exact candidates and owners.

Specific risks are transitive client constraint conflicts, native wheel/toolchain gaps, shared Arrow/Kernel assumptions, provider credential discovery, duplicate mutable catalogs/stores, and confusing source availability with compatibility. Mitigation is an exact resolved environment per profile plus real native/error/recovery checks before adoption. This planning decision adds no supported capability and does not relax JVM-free or emulator-first requirements.
