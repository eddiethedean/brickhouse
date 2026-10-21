# ADR 0029: Active job restart reconciliation

- Status: accepted; implemented for local Python job runs
- Date: 2026-10-20
- Task: B014.3.3
- Metadata schema: 9

## Context

The job repository already records interrupted run and attempt states without fabricated exit codes, but a restarted service did not know whether a persisted worker was still executing. Marking every active row interrupted at startup could misreport a child process that outlived its service owner; leaving every row active could preserve work that no longer exists indefinitely. Completed attempt output must remain available, and retrying a user's script after restart could duplicate external effects.

## Decision

1. Persist the child worker PID in the durable worker-outcome journal immediately after process creation. Schema 9 adds the PID and an explicit `interrupted` worker outcome; migrations from schemas 1–8 remain transactional.
2. During job-service startup and run polling, inspect active persisted attempts. If the recorded worker still exists, leave its run active and do not launch a replacement. When the PID is gone, reconcile any already observed terminal worker result, mark unobserved worker/job attempts and the run interrupted, and leave the exit code null.
3. Do not replay user code or terminal/idempotent submissions automatically. Keep completed attempt output immutable. When the latest attempt was interrupted before output capture, SDK output retrieval returns the latest earlier attempt that has persisted output.
4. Surface interrupted runs as SDK `INTERNAL_ERROR` with the persisted `WORKSPACE_RESTARTED` message. Pending tasks remain pending as a record that they were never dispatched.

## Consequences and limits

Restarted runs cannot be mistaken for success, and users can inspect prior completed attempt output. A surviving worker is not adopted or killed by the restarted service; its side effects may continue until that process exits, and the run remains active until a later poll reconciles it. This is local PID observation, not distributed worker supervision or exactly-once execution. See [B014.3.3 evidence](../../artifacts/conformance/B014.3.3-active-run-recovery.md).
