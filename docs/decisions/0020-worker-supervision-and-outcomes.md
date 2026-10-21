# ADR 0020 — Worker supervision and durable outcomes

- Status: accepted for the phase 0.1 local worker profile
- Date: 2026-10-20
- Tasks: B013.3; builds on ADR 0019

## Context

B013.1 launches isolated processes but previously waited without a deadline. A worker timeout or cancellation must stop its child and same-attempt descendants, release its credential, clean caller-owned staged input, and preserve the real process exit result. B014 will add public job/attempt management and output retrieval.

## Decision

Keep supervision in the parent process. Accept a finite timeout (up to 24 hours), a cancellation `threading.Event`, and a bounded termination grace (0–60 seconds). POSIX workers start in a new session; timeout/cancellation sends SIGTERM to the process group, waits the grace, sends SIGKILL if the group remains, then reaps the direct child and drains both bounded output pipes. The returned `WorkerResult` records `completed`, `timeout`, or `cancelled`, along with the actual child exit code. Windows can terminate the direct worker, but descendant-group cleanup is not claimed there.

The supervisor can receive a `staging_cleanup` callback. It invokes this after the process is reaped on both normal and interrupted execution and records the exception type if cleanup fails; it never deletes a caller path itself. Pass `StagingArea.cleanup` for a validated `StagedInput` so cleanup preserves workspace ownership.

If a `WorkerAuthenticator` and credential are supplied together, the worker context must match their run/attempt identity and the supervisor revokes every credential for that attempt after termination. It redacts the exact bearer and a truncated matching suffix from captured output before exposing the result.

Workspace metadata migration 5 adds `worker_attempt_outcomes`, keyed by run/attempt. The journal writes `running` before process creation and records the terminal state, actual exit code (or no code if launch failed), termination reason, resolved executable/script/cwd, bounded retained byte counts/truncation flags, timestamps and cleanup error type. It stores no bearer or output contents. Outcomes are immutable once terminal. A worker-owner process crash may leave a `running` row; restart reconciliation belongs to B014.

## Consequences and limits

- Local POSIX cancellation terminates the owned process group and reaps the directly owned child. A descendant that deliberately escapes its inherited process group is outside this guarantee.
- Worker stdout/stderr remains in bounded memory for the caller; the outcome journal stores metadata and exit status, not log content. B014 owns durable job logs and output retrieval.
- This is process lifecycle control, not a hostile-code sandbox. A child can read its scoped environment credential and can start processes that escape the group.
- No job API, retry/restart policy, or automatic stale-attempt recovery is added here.
