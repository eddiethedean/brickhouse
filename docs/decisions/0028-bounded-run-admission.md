# ADR 0028: Bounded local job-run admission

- Status: accepted; implemented for the local SDK Python job profile
- Date: 2026-10-20
- Task: B014.3.2.2
- Follow-up: B014.3.3 restart reconciliation

## Context

The durable repository supports workspace-wide submission tokens, including terminal replay. The SDK job service previously allowed only one active run and mapped admission errors to HTTP 429, a status the pinned SDK can retry. Supporting declared concurrency requires an atomic decision that does not insert a run or start a worker after capacity is exhausted.

## Decision

1. Cap active runs at four per local service process. Accept `max_concurrent_runs` values from 1 through 4 when creating a job and enforce the job's value independently of the process-wide cap.
2. Under the job-service lock, validate and look up a supplied idempotency token before evaluating capacity. An identical active or terminal replay returns the existing run; changed job/settings reuse continues to produce a conflict.
3. Check both the process-wide active run count and the target job's active count before persisting a new submission. A capacity rejection creates neither a run nor a worker.
4. Map capacity and token conflicts to HTTP 409 `RESOURCE_CONFLICT`. The pinned SDK surfaces `ResourceConflict` without retrying the submission as transient overload.
5. Reject instead of queueing when capacity is full. Persisted active-run recovery after process restart is handled separately by B014.3.3.

## Consequences

The local service can execute up to four job runs at once, with each job honoring its declared limit. Admission is scoped to the single owner-locked service process; this does not claim cross-process scheduling or Databricks concurrency parity. Full-process and per-job capacity, active and terminal idempotency replay, changed-token conflict, and run-count stability are covered through the pinned SDK loopback integration. See [B014.3.2.2 evidence](../../artifacts/conformance/B014.3.2.2-run-admission.md).
