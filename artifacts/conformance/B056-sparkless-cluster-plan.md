# B056 — Source-grounded native Sparkless cluster plan

Date: **2026-10-20**. Result: **planning task complete; no cluster implementation**.

## Pinned source review

The read-only `/Volumes/T7/coding/robin-sparkless` checkout was clean at commit `ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b`, package version **4.14.1**. The review read its contribution/adoption guidance and the engine, external-catalog, Python session and Polars backend contracts. No code was built, changed, copied, committed or published in that repository.

Observed source at this exact commit:

- The [adoption guide](https://github.com/eddiethedean/robin-sparkless/blob/ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b/docs/BEFORE_YOU_ADOPT.md) says Sparkless is not a full cluster replacement and has no distributed scheduler/executors.
- The core [`PlanExecutor`](https://github.com/eddiethedean/robin-sparkless/blob/ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b/crates/robin-sparkless-core/src/engine/plan.rs) takes JSON operations and a fully materialized `Vec<Vec<JsonValue>>` row collection. It is not a typed bounded distributed transport contract.
- The [external catalog writer contract](https://github.com/eddiethedean/robin-sparkless/blob/ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b/crates/robin-sparkless-core/src/engine/external_catalog.rs) passes Arrow IPC as `Vec<u8>`, which does not by itself provide streaming, flow control, worker ownership or atomic multi-fragment commits.
- The [PyO3 session wrapper](https://github.com/eddiethedean/robin-sparkless/blob/ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b/python/src/lib.rs) declares `PySparkSession` `unsendable`. Python `coalesce` and `repartition` return cloned inner frames, not distributed partitioning operations.
- The public guide says arbitrary Python UDFs are unsupported; the current local UDF probes are not a license to serialize or ship Python callables between workers.

These source facts establish the current local boundary only. They do not establish future APIs, partition execution, multi-host behavior, JVM-free cluster behavior, or Databricks parity.

## Plan recorded

- [Document 19](../../docs/plan/19-sparkless-cluster-execution.md) defines optional SC.0–SC.5 / K0–K5 gates, typed plans and bounded transport, real partition ownership, shuffle/backpressure, fenced attempts, provider-owned sinks, security, resource limits and explicit unsupported operations.
- [The upstream specification](../../docs/upstream/sparkless-cluster-contracts.md) maps observed contracts to missing generic Sparkless behavior and first failing fixtures.
- [ADR 0037](../../docs/decisions/0037-native-sparkless-cluster-plan.md), [the root roadmap](../../ROADMAP.md), task/backlog entries B057–B062, capabilities C065–C067 and the planning validator keep this optional track separate from emulator release phases and SH deployment work.
- Implementation is gated after B049 emulator stability. BrickHouse integration B062 also requires B053 remote-worker foundations. B042, B055 and the 85 existing phase 0.1 implementation children have no cluster dependency.
- B057–B062 are future epics; each must be decomposed before implementation under current repository instructions. No upstream issue/API, implementation artifact, worker infrastructure, package version, release date, cloud spend, or publication is implied.

Planning validation passed with 147 task entries, 62 top-level work packages, 85 phase 0.1 child tasks, 67 capabilities, nine emulator release phases, seven SH stages and six SC stages. The remaining phases are explicitly unassigned. No runtime test, rebuild, Databricks request, cloud workload, upstream edit, or external message was used for B056.
