# ADR 0016: Explicit serverless and classic compute profiles

- Status: accepted; bounded simulated classic and SQL warehouse lifecycle metadata, warehouse-bound local statement execution, and pre-dispatch context resolution implemented; broader workload implementation pending
- Date: 2026-10-20
- Research task: B043
- Implementation owners: B010–B024, B028, B030–B036, B040, B042 as mapped in document 17

## Context and evidence

The initial C034 requirement described simulated cluster/warehouse metadata without separate serverless and classic workload contracts. The owner requested research and an explicit commitment to emulate both behaviors. Databricks distinguishes serverless jobs/notebooks, classic all-purpose/jobs compute with runtime-dependent access modes, and three SQL warehouse types. Classic compute can also use Spark Connect, so one global serverless/classic switch cannot define protocol or analysis semantics.

[Document 17](../plan/17-serverless-and-classic-compute.md) retains the primary-source review, version limits, behavior matrix, release allocation and test designs. [The B043 report](../../artifacts/conformance/B043-serverless-classic-plan.md) pins inspected SDK sources. Existing personal-account evidence covers serverless SQL only.

## Decision

1. Commit to serverless and classic Python job/notebook behavior, plus serverless/pro/classic SQL warehouse behavior, with a bounded local subset required for **0.1.0**. Append explicit acceptance criteria to the existing implementation packages and add five bounded children (B011.5, B014.4, B014.5, B017.4, B020.4), bringing the required child count to 73. Preserve all prior criteria and prerequisites. B043 is an additional phase 0.1 planning task.
2. Track compute mode, access mode, runtime/environment reference and client protocol separately from the Sparkless semantic profile. Resolve supported SDK wire selectors to immutable domain context; reject conflicts, missing resources and unmapped versions before dispatch.
3. Use one workspace compute metadata service and the existing catalog/table/supervisor authorities. Execute supported jobs and queries locally through real native providers. B014.4 supplies all-purpose/run-scoped classic lifecycle records and pinned SDK routes; B014.5 resolves supported selectors/environments into immutable pre-dispatch context. Neither slice executes work. Mark infrastructure allocation and lifecycle simulation explicitly; resource metadata or context resolution cannot establish successful user-code execution.
4. Keep new workload/environment capability entries C053–C056 `planned`; C034 continues to track infrastructure metadata separately. Record actual mode-specific evidence before changing status. Deferred Connect semantics, absent JVM tasks, cloud performance and shared driver state remain explicit limitations.
5. Deliver broader environment/retry/access policies in phase 0.2, mode-specific streaming/pipelines in 0.5–0.6, and native Connect plus analysis/binding behavior in 0.7. Keep oracle gaps visible and required tests offline. Existing capture authorization and spending limits continue to apply.

## Consequences and alternatives

Metadata-only emulation is insufficient for the requested workload behavior. Treating classic compute as synonymous with Spark Classic would produce incorrect tests for newer standard runtimes. Running real Spark or redirecting execution to Databricks would violate the runtime boundary. The chosen design adds real local behavior with separately testable simulation and compatibility limits.

Classic shared driver/singleton state is intentionally excluded by the fresh-process-per-attempt contract; this requires a partial claim even when shared compute IDs/lifetimes work. Access-mode labels do not promise a sandbox or production security. Full Connect protocol compatibility is independent of the initial local serverless subset.

This is an explicit addition to the 0.1 release acceptance contract. Failure to implement either required subset must remain visible as blocked work or a recorded scope revision. No runtime migration, dependency upgrade, account provisioning, release publication, or cloud expenditure is performed by this planning change.

## Evidence update — 2026-10-21

The initial planned states above were retained until worker-level evidence existed. B018.1.3 and B020.5 now make C053 and C054 partial for the tested local SDK jobs and serverless Python/classic notebook bundle subsets; B018.1.4 and B020.5 make C055 partial for selected local warehouse lifecycle and query behavior. C056 remains planned. These updates do not alter the local-only, simulated-infrastructure, or remote-compatibility limits. See [B020.5 conformance](../../artifacts/conformance/B020.5-cli-serverless-classic-bundles.md) and the corresponding registry entries.
