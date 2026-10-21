# ADR 0022 — Durable local job state

- Status: accepted for the phase 0.1 persistence layer
- Date: 2026-10-20
- Tasks: B014.1; builds on ADR 0020

## Context

B013 provides isolated process supervision and journals observed worker outcomes, but job submissions, task graphs, retries and run state need a single durable owner. The phase 0.1 job profile is intentionally narrower than Databricks Jobs: a bounded acyclic graph of Python-script tasks, immutable JSON settings, and explicit worker outcomes. Job metadata must survive workspace reopen and must not claim success when an exit status or active process outcome is unknown.

## Decision

Persist job definitions, task definitions, run submissions, run task snapshots and task attempts in the workspace SQLite metadata database (schema 6). Do not introduce a second catalog or job database. Definitions and submitted task/settings snapshots are immutable. Each submission has a SHA-256 request fingerprint; an optional workspace-wide submission token replays only the same job and settings and conflicts if reused for different content. A new run copies the exact task graph and settings snapshot from its immutable job version.

The initial model accepts at most ten `python_script` tasks. Task keys are bounded identifiers, dependencies must exist, and duplicate/self/cyclic dependencies are rejected before persistence. Settings are finite canonical JSON with a 64 KiB per-object limit; credential-shaped setting keys are rejected so callers use scoped secret references instead of storing raw credentials.

Each task attempt has a monotonic per-task number and a distinct UUID. Repository methods and SQLite triggers enforce queued/running/terminal transitions, dependency readiness, truthful process exit requirements, terminal task/run consistency and immutable terminal history. A failed task may be queued for a distinct retry attempt while its run remains active; automatic retry policy is not part of this decision. A workspace restart can mark active runs and attempts `interrupted`. It records no fabricated exit code and leaves undispatched pending tasks pending.

Expose this persistence through the internal `WorkspaceContext.jobs` repository only. Do not add an SDK or HTTP route, worker dispatch, output/log retention, concurrency queue, automatic retries, or stale-run reconciliation here.

## Consequences and limits

- SQLite, rather than an in-memory cache, is authoritative for definitions, submissions and attempts; workspace migration ownership serializes schema upgrades.
- A caller can inspect exact immutable settings and task dependencies after reopen, and a submission token cannot silently create duplicate runs or mutate an existing request.
- Persistence and transition tests do not establish SDK compatibility or successful Python job execution. The repository is not yet composed with B013 or exposed through `serve`/`run`.
- Automatic retries, dependency failure propagation, concurrent admission and recovery of queued or interrupted runs remain B014 follow-up work. Unknown worker state stays interrupted until a later reconciliation policy records evidence.
- The settings-key credential check is a guard against accidental direct persistence, not a secret scanner or a sandbox for trusted local code.
