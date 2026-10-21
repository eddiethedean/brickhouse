# ADR 0026: Sequential local job DAG dispatch

- Status: accepted; bounded local dispatch implemented
- Date: 2026-10-20
- Task: B014.3.1
- Follow-up work: B014.3.2 retry/concurrency admission and B014.3.3 restart recovery

## Context

B014.1 already stores immutable, validated task graphs and prevents an attempt from being queued until all dependencies have succeeded. B014.2 supplies workspace Python source files, pinned-SDK job routes, fresh local workers and durable attempt output. The SDK route still rejected multi-task jobs, so the repository's dependency contract was not connected to actual execution.

## Decision

1. Accept up to ten validated Python script tasks and preserve each task's resolved execution context and settings.
2. Dispatch one task at a time using a stable topological order. Before each attempt, read the persisted dependency states; queueing remains independently guarded by the repository's transactional dependency check.
3. If a dependency failed, was cancelled, skipped, or interrupted, mark the pending dependent task `skipped` with zero attempts. Report dependency failures as SDK `SKIPPED`/`UPSTREAM_FAILED` and run cancellation as `SKIPPED`/`UPSTREAM_CANCELED`.
4. Continue independent branches after a task failure. Finish the run as failed only after all runnable tasks and dependency skips have truthful terminal states. A successful run requires every task to succeed.
5. Keep unsupported effects explicit: only Python scripts and default `ALL_SUCCESS` dependencies are dispatched; execution stays sequential; retries, `max_concurrent_runs` above one, active-run restart reconciliation, worker-facing compute lifetimes, and multi-task SDK output retrieval remain unsupported. A `python_params` run override applies only to a single-task job.

## Consequences

The local worker service now proves dependency order and failure propagation without adding a second scheduler or changing metadata schema 8. Each task still receives a fresh child process and persisted output. Independent tasks may run after another branch fails, while dependents cannot execute until the predecessor succeeds.

This is not Databricks workload conformance: the pinned SDK is tested against the local loopback adapter, and workers do not receive Spark/Sparkless or Delta table behavior. Retry, capacity, and active-process recovery require their separate child tasks. See [B014.3.1 evidence](../../artifacts/conformance/B014.3.1-dag-dispatch.md).
