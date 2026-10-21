# ADR 0030: Bind Python job workers to compute lifetimes

- Status: accepted and implemented for the bounded phase 0.1 local worker profile
- Date: 2026-10-20
- Planning task: B014.6
- Refines: ADR 0016, ADR 0023, ADR 0025, ADR 0029

## Context

B014.4 persisted classic all-purpose and run-scoped compute identities, while B014.5 resolved immutable task contexts. The SDK job dispatcher did not reserve task consumers, start resources, release them on observed outcomes, or connect generation invalidation to a live worker. Compute metadata therefore described lifecycle state without governing actual worker lifetime.

## Decision

1. After a run and its task rows are durably submitted, reserve every selected classic resource and its complete planned consumer set. Group tasks sharing one `job_cluster_key` under one run-scoped compute identity. Serverless tasks retain their resolved environment context and do not receive a fabricated classic cluster ID.
2. Before dispatching a classic task, start its registered consumer and pass the selected simulated resource ID, generation, and state in the fresh worker context. Keep the consumer active across all configured attempts for that task. Release it after terminal task outcome; release any remaining registered consumers when the run settles or is reconciled after service restart.
3. A new/shared resource terminates after its last consumer releases. An all-purpose resource retains its identity and can autostart only when the pre-dispatch resolver marked it as terminated. Compute allocation and state remain explicitly simulated.
4. While a worker uses an all-purpose cluster, monitor its state and generation. An all-purpose restart or termination with active consumers increments/changes the state; the job supervisor observes the invalidation, cancels the run, and reaps the worker process group. A restart/termination with only registered consumers remains rejected. Compute interruption is not automatically retried because user-code side effects may already have committed.
5. Service recovery determines job success from persisted worker outcomes and observed worker processes, never from a resource's lifecycle state. Once the worker PID is gone, an abandoned run is reconciled as interrupted and its planned/active consumers are released.

## Consequences

The pinned-SDK loopback corpus now exercises all seven Python job variants from document 16, including shared first/last consumer behavior, existing all-purpose identity/autostart, fresh worker attempts, compute restart/termination during active user code, descendant cleanup, and preservation of an already committed Delta table. This composition required no workspace metadata migration because the existing durable consumer/event tables were sufficient.

This does not provide cloud compute, Spark/Sparkless sessions, worker-facing table-service reads/writes, shared Python driver state, serverless resource objects, or Databricks runtime parity. The termination monitor covers the selected local all-purpose resource path; worker process-group cleanup is POSIX-tested, and descendants that escape the process group are not covered. See [B014.6 evidence](../../artifacts/conformance/B014.6-job-worker-compute-lifetimes.md).
