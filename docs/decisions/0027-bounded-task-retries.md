# ADR 0027: Bounded task retry attempts

- Status: accepted; bounded task retry behavior implemented
- Date: 2026-10-20
- Task: B014.3.2.1
- Related decision: [ADR 0028](0028-bounded-run-admission.md)

## Context

Execution-context resolution already validates per-task `max_retries`, `min_retry_interval_millis`, and `retry_on_timeout`, but job creation rejected nonzero values. The repository already allocates immutable attempt IDs and permits a failed task to be queued again while preserving earlier attempt rows and output.

## Decision

1. Treat `max_retries` as the number of retries after the first attempt; support the bounded resolver range 0–10. `None` means zero retries.
2. Retry observed nonzero worker exits while the retry count remains. Retry a timed-out worker only when `retry_on_timeout=true`. Do not retry launch failures, cancellation, or interrupted attempts.
3. Wait at least `min_retry_interval_millis` between the failed attempt's completion and the next attempt's start. The wait is interruptible during service shutdown.
4. Persist every attempt with its own UUID, observed exit/termination state, and immutable bounded output. Return the final task result and expose the SDK's zero-based attempt number while repository records remain one-based.
5. Keep timeout retry disabled by default. Never retry a task that succeeded, and never exceed the validated retry count.

## Consequences

Failed intermediate attempts remain queryable even when a later retry succeeds. Dependents observe the final task result because the DAG scheduler dispatches them only after retries finish. External side effects from a failed attempt may repeat; this does not provide exactly-once execution.

Run-level `max_concurrent_runs` admission was implemented separately under the original local four-run cap; B064 later supersedes that cap with durable queueing as recorded in [ADR 0028](0028-bounded-run-admission.md). This local profile uses the pinned SDK against loopback only and makes no Databricks runtime compatibility claim. See [B014.3.2.1 evidence](../../artifacts/conformance/B014.3.2.1-retry-attempts.md).
