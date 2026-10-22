# ADR 0037: Plan native Sparkless clusters as a separate future engine track

- Status: accepted for planning; cluster implementation unselected and unimplemented
- Date: 2026-10-20
- Planning task: B056
- Extends: ADR 0034

## Context

The owner requested planning documents to expand Sparkless to run clusters after preserving JVM-free Databricks emulation as BrickHouse's primary goal. SH.4 already plans remote job workers, which do not distribute one query. Read-only inspection of clean Sparkless 4.14.1 confirms local engine traits/IR and provider hooks, but its adoption guide excludes a distributed scheduler/executors and its partition methods do not establish distributed execution.

## Decision

1. Add [document 19](../plan/19-sparkless-cluster-execution.md) and [the source-grounded upstream specification](../upstream/sparkless-cluster-contracts.md). Every new interface is proposed, not a released API. Make no changes in Sparkless for this planning slice.
2. Define optional SC.0–SC.5 after B049 stability: feasibility, partition planning/operators, exchanges/shuffle, scheduling/recovery/provider-owned sinks, a qualified standalone artifact, then BrickHouse integration. B057–B062 are future epics requiring bounded decomposition. B056 is planning only, assigned to 0.1 without changing its 85 implementation children or runtime corpus.
3. Prefer investigation of native coordination/executors with existing Polars partition execution, but require SC.0 to compare actual native options and select a tested design. Preserve local defaults and Sparkless semantics; no hidden backend retry, Java or remote Spark fallback is permitted.
4. Keep generic query execution in Sparkless; keep Databricks-facing services, catalog/commit authority, policy, environments and jobs in BrickHouse. Cluster workers stage output; the owning native provider commits one accepted logical sink result. The current buffered Arrow callback is not a bounded distributed sink.
5. SC.5 requires both B061 and B053. Existing emulator and SH requirements do not depend on SC work; standalone Sparkless cluster qualification does not establish BrickHouse policy or deployed workload isolation. No HA or single-query performance claim follows from multiple hosts.

## Evidence and consequences

SC gates require real partitioned work, exact typed semantics, bounded exchange/spill/results, authenticated/fenced attempts, failure/cancel/commit reconciliation and actual cross-host execution. New capabilities C065–C067 remain planned. Planning validation must cover SC assignments, the stability barrier and integration prerequisites without promoting existing claims.

No runtime, dependency, workspace or upstream source change, rebuild, cloud workload, external message or publication is authorized by this decision. B056 evidence is recorded in [the planning report](../../artifacts/conformance/B056-sparkless-cluster-plan.md).
