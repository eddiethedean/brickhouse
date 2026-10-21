# ADR 0025 — Bounded SDK Python job flow

**Status:** accepted for phase 0.1 implementation. **Date:** 2026-10-20.

## Context

B014.1 provides durable job/run/task/attempt state, B013 provides an isolated worker process, and B014.5 resolves supported compute/environment selectors. Those pieces need a concrete pinned-SDK transport slice for uploading a workspace script, submitting it, observing the real process outcome, and retrieving bounded logs. This does not complete the serverless/classic workload contract: Sparkless sessions, table effects and compute consumer lifetimes are separate work.

## Decision

Implement the following `databricks-sdk==0.153.0` loopback profile:

1. `POST /api/2.0/workspace/import` accepts multipart SDK requests and a bounded JSON form for `SOURCE`/`PYTHON` files. Paths must be normalized absolute `/Workspace/.../*.py` names. Files are limited to 1 MiB and atomically stored below the workspace-owned `files/` root; symlinks and traversal are rejected.
2. `POST /api/2.2/jobs/create` accepts one `spark_python_task` with a supported local compute/environment selector. It validates file existence before saving the immutable job. Unsupported task fields, more than one task, retry requests and concurrency above one fail with an API error.
3. `POST /api/2.2/jobs/run-now` stores an immutable submission and starts a fresh local Python child process. `python_params` are validated argv values, never shell text. Idempotency-token replays resolve to the original run, including while it is active. The run stores only JSON-safe settings.
4. `GET /api/2.2/jobs/runs/get` maps durable local states to SDK run/task lifecycle and result enums. `GET /api/2.2/jobs/runs/get-output` is available only after execution leaves queued/running state and returns stdout in `logs`, stderr in `error`, and a truncation flag.
5. Schema 8 stores immutable stdout/stderr per attempt, capped at 1 MiB per stream, and adds lifecycle validation triggers. Schema 7 remains readable for transactional upgrade. Server close cancels and reaps local workers before releasing workspace ownership.

## Consequences

The pinned SDK exercises real wire formats, multipart upload, idempotency, state polling and output retrieval without contacting a Databricks workspace. The compatibility claim is limited to the covered SDK calls and local profile. The worker executes plain Python with its isolated environment and run context; this profile does not inject `spark`, use Sparkless/Delta, install packages, or provision compute.

Multi-task scheduling, dependency propagation, retries, cancellation requests, restart reconciliation and classic compute consumer lifetimes remain separate B014 work. Serverless selectors may be resolved by B014.5, but this route does not establish serverless workload behavior. A loopback SDK token remains a compatibility value, not an authorization boundary.

## Evidence

See [B014.2 conformance](../../artifacts/conformance/B014.2-workspace-files-sdk-jobs.md) for the pinned-client local round trip, output bounds, failure mapping, migration coverage and regression checks.
