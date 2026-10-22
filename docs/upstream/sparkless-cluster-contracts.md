# Sparkless cluster contracts and source baseline

Date: **2026-10-20**. Planning task: B056. Status: inspected source plus proposed work; **no upstream changes**. Architecture: [document 19](../plan/19-sparkless-cluster-execution.md).

## Inspected source

Read-only checkout: `/Volumes/T7/coding/robin-sparkless`; clean at **`ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b`**, package version **4.14.1**. Read README, CONTRIBUTING, engine architecture/adoption guidance and the implementation files below. No AGENTS.md was found inside that checkout during this inventory. Re-read applicable instructions and current code before future implementation; this snapshot does not establish future APIs. No upstream code was built, changed, copied, committed or published.

| Observed contract | Exact source at inspected revision | Consequence for cluster work |
|---|---|---|
| Adoption guide excludes a distributed scheduler, Spark UI and executors | [BEFORE_YOU_ADOPT.md](https://github.com/eddiethedean/robin-sparkless/blob/ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b/docs/BEFORE_YOU_ADOPT.md) | Existing Sparkless is a local foundation; cluster support requires new implementation and evidence |
| Core engine traits separate backend/session/frame operations; `ExprIr` is serializable | [dataframe.rs](https://github.com/eddiethedean/robin-sparkless/blob/ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b/crates/robin-sparkless-core/src/engine/dataframe.rs), [expr.rs](https://github.com/eddiethedean/robin-sparkless/blob/ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b/crates/robin-sparkless-core/src/expr.rs) | Potential generic extension points; not a complete versioned typed physical plan or a partition scheduler |
| `PlanExecutor` accepts a JSON operation list with `Vec<Vec<JsonValue>>` input rows | [plan.rs](https://github.com/eddiethedean/robin-sparkless/blob/ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b/crates/robin-sparkless-core/src/engine/plan.rs) | Do not use this row-materializing signature as the distributed bulk-data contract |
| External table descriptors identify table, canonical location, schema and optional snapshot version; writer payload is `ArrowIpcStream { bytes: Vec<u8> }` | [external_catalog.rs](https://github.com/eddiethedean/robin-sparkless/blob/ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b/crates/robin-sparkless-core/src/engine/external_catalog.rs) | Retain identity/provider delegation; add explicit pinned fragment/bounded sink contracts rather than claiming the current byte vector streams over a cluster |
| Python `PySparkSession` is declared `unsendable`; Python `repartition` and `coalesce` clone the inner frame | [python/src/lib.rs](https://github.com/eddiethedean/robin-sparkless/blob/ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b/python/src/lib.rs) | Create owned sessions where needed; do not transfer Python sessions between threads/hosts or infer real partitioning from existing method names |
| Polars backend implements local facade operations; Rust repartition/coalesce also retain the frame | [dataframe/mod.rs](https://github.com/eddiethedean/robin-sparkless/blob/ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b/crates/robin-sparkless-polars/src/dataframe/mod.rs), [ENGINE_ARCHITECTURE.md](https://github.com/eddiethedean/robin-sparkless/blob/ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b/docs/ENGINE_ARCHITECTURE.md) | Polars is a candidate partition executor; no distributed layout/ordering guarantee follows from current APIs |

The 4.14.1 provider [qualification report](../../artifacts/conformance/B046.3-sparkless-4.14.1-provider.md) provides small typed local evidence, including nullable structs. It supplies neither a cluster benchmark nor a distributed provider guarantee. Current BrickHouse adapter code is a consumer of those public hooks, not the cluster engine.

## Proposed work packages

The labels below identify proposed **contracts**, not methods or released Sparkless APIs. B057–B061 coordinate upstream work from BrickHouse. Before implementation, add linked bounded tasks in the actual Sparkless repository, respecting its current instructions and tests. Keep BrickHouse services/SDK/SQLite dependencies out of generic Sparkless code. New native dependencies require exact artifacts, licenses and an explicit adaptation decision; no third-party distributed backend is selected by this document.

| Contract | Owner stage | Missing behavior to implement | First real failing fixture |
|---|---|---|---|
| CEX01 typed plan and semantic capability envelope | SC.0–SC.1 | Versioned operator/schema/literal/settings/snapshot representation and pre-dispatch validation; explain local/global distribution | A decimal/nested-null plan executes identically on two processes; unknown op/version and incompatible schema fail before execution |
| CEX02 partition scans and batch execution | SC.1 | Provider-bound immutable scan fragments, disjoint ownership and bounded native batch input/output; preserve ordinary local APIs | Two executors scan different row groups of one pinned snapshot, then filter/project without collecting source rows at the coordinator |
| CEX03 exchanges and distribution properties | SC.2 | Versioned hash/equality, backpressure/checksums, complete block manifests, bounded broadcast, spill and global operator planning | Same keys on different hosts yield one correct aggregate/join; corrupt block, skew/memory/disk limit and slow reader fail or spill according to policy |
| CEX04 query/task lifecycle | SC.2–SC.3 | Explicit native coordinator/executor protocol, authenticated leases/generations, retry classification, cancellation and durable winning output state | Kill a real executor after producing a block; a retried/stale task cannot supply two accepted partition outputs |
| CEX05 provider-owned sink manifests | SC.0 feasibility, SC.3 implementation | Manifest schema/ownership/content/coverage checks, fenced acceptance and one provider commit with exact outcome/reconciliation | Kill the response after a real Delta commit; retry reports the same version without appending another copy, or preserves a specific uncertain state |
| CEX06 public API and distribution | SC.4 | Versioned optional cluster configuration/capabilities, exact artifact/protocol matrix, bounded results and operational diagnostics | Clean local install still needs no coordinator; compatible cluster artifacts execute K1–K3 and incompatible versions fail explicitly |

The six future cluster gaps are tracked upstream as [#1699](https://github.com/eddiethedean/robin-sparkless/issues/1699) (CEX01), [#1700](https://github.com/eddiethedean/robin-sparkless/issues/1700) (CEX02), [#1701](https://github.com/eddiethedean/robin-sparkless/issues/1701) (CEX03), [#1702](https://github.com/eddiethedean/robin-sparkless/issues/1702) (CEX04), [#1703](https://github.com/eddiethedean/robin-sparkless/issues/1703) (CEX05), and [#1704](https://github.com/eddiethedean/robin-sparkless/issues/1704) (CEX06). These issues track optional native multi-machine execution work after emulator stability; none is a blocker for the local 0.1 emulator.

SC.0 must decide whether extending the local Polars-backed traits or using an explicit native backend adapter can meet these contracts. Benchmarking or adoption of another engine is not permission to change semantics, use private APIs untested, call managed services or introduce a JVM. Two-process prototype results are feasibility evidence only; cross-host gates require real hosts.

## Delivery and release boundary

Use failing Rust/Python behavior tests before each bounded engine change. Follow Sparkless CONTRIBUTING's required upstream checks for actual implementation changes; these are future requirements, not checks run in B056. Qualify the selected current and oldest supported local APIs, owner-thread/GIL behavior and mixed-version rejection. Carry plan/exchange/environment versions separately from Python/Rust package versions.

Record source commit, wheel/native executable hashes, native provider/Arrow versions, OS/CPU, protocol settings, operator fixtures, fault results and limitations. SC.4 may produce a reviewable source-built candidate. Public BrickHouse cluster installation must use an actual published compatible Sparkless artifact, separately authorized for publication. No issue, PR, release or remote message is sent by this planning task.

## BrickHouse adaptation boundary

SC.5/B062 consumes qualified public contracts after both B061 and B053. BrickHouse resolves identities/settings, starts a fresh job attempt, selects the explicit cluster engine mode and grants bounded query capacity; Sparkless schedules partition tasks inside it. Keep query/task attempts distinct from job retries. Provider reads are pinned and authorized; all registered writes return to BrickHouse's table service and native Delta provider. Neither query executors nor the engine coordinator replace workspace metadata authority.

The existing S001–S008 upstream plan remains unchanged for emulator work. These cluster contracts are an additional future track, with no dependency from the emulator gates or the existing self-hosted distribution B055.

## Concrete native comparison inputs from B063

[The reuse research](../plan/20-open-source-reuse.md) supplies Ballista/DataFusion 55.0.0 and Delta Kernel 0.30.0 as exact static research candidates, with registry/source hashes in [planning/oss-reuse.json](../../planning/oss-reuse.json). B057 must compare scheduler/executor/shuffle reuse and provider integration against extending Sparkless/Polars. Ballista's Rust 1.94 requirement, Arrow/version skew and Spark-specific semantics need real probes. These artifacts are not installed, built, selected or claimed compatible by the source inventory; no upstream API or toolchain change is authorized by merely naming them.
