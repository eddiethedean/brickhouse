# ADR 0033 — Attempt-scoped task values

- **Status:** Accepted
- **Date:** 2026-10-20
- **Task:** B016.3

## Context

Dependent job tasks need a durable way to exchange small JSON values without opening SQLite in user workers or placing mutable state in the immutable worker context. A run may retry a task, so values from a failed attempt must not become the downstream result. Values must also stay within one run and be inaccessible to unrelated tasks.

Databricks documents `dbutils.jobs.taskValues` for Python notebooks, with JSON-serializable values up to 48 KiB and at most 250 task values per run. The phase 0.1 job implementation accepts Python script tasks, so exposing the same bounded methods there is a declared local extension; it is not evidence of notebook or cloud parity.

## Decision

Add metadata schema 10 with `job_task_values`, keyed by `(run_id, task_key, attempt_number, value_key)` and foreign-keyed to the durable task attempt. Database triggers permit inserts and updates only while the matching task attempt, task, and run are active. A key may be updated during one attempt; logical `(task, key)` pairs are counted once toward the per-run limit of 250. Values remain attached to failed attempts for accurate history, but a reader sees only values from a successful attempt of an upstream task. The latest successful attempt is selected.

`JobRepository` validates strict finite JSON with string object keys, canonicalizes it as compact UTF-8 JSON, and caps its encoded representation at 48 KiB. `set` returns `True`. `get` is limited to transitive declared dependencies within the same run. An unknown task raises `ValueError`; a missing key raises `ValueError` unless a non-`None` `default` was supplied. `debugValue` is accepted and validated but does not supply a value inside a job. A value of JSON `null` remains distinguishable from a missing value.

The child sends `task_values.set/get` requests through a versioned `WorkerCallMessage` on a local POSIX socket pair. The bearer token is carried in a separate authorization field and is scoped to one run/attempt; the parent authenticates and dispatches repository calls. The child never opens the metadata database. The private token and descriptor are removed from the worker environment before user code runs, and the attempt credential is revoked when the worker exits. No public HTTP route is added.

The existing supervisor captures each worker's bounded stdout/stderr and persists it with that task attempt after exit. No `dbutils.notebook.exit`, live log stream, child notebook result, or notebook context getter is implemented by this decision. Unsupported methods under `dbutils.jobs` raise `DbutilsOperationNotSupportedError`.

## Limits

- The job-script adapter is a local extension to Databricks' notebook-only documented utility and makes no real-workspace compatibility claim.
- Only `set` and `get` are implemented. `For each` aggregation, dynamic task parameters, notebook results, and other job utilities remain outside this slice.
- Values are durable job metadata, not secret storage; the process-memory secret provider remains the only implemented secret path.
- Workers use trusted Python code. Process isolation and the private channel are not a sandbox against memory inspection or direct filesystem access.
- The private descriptor-passing transport is currently POSIX-only. There is no public HTTP task-value API.

References: [Databricks dbutils taskValues reference](https://docs.databricks.com/aws/en/dev-tools/databricks-utils), [task values guide](https://docs.databricks.com/aws/en/jobs/task-values), and [B016.3 conformance](../../artifacts/conformance/B016.3-attempt-scoped-task-values.md).
