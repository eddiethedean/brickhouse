# 10 — Streaming, ingestion, and pipelines

## Compute profiles

Apply [document 17's serverless/classic contracts](17-serverless-and-classic-compute.md) when B030–B035 are decomposed. Keep trigger acceptance/rejection and runtime/environment restrictions specific to the selected mode; a trigger valid on classic compute is not automatically valid on serverless. Resolve classic/serverless pipeline compute through the shared services and verify real local batch outputs and recovery. This later work does not change the required bounded Python/SQL profiles in 0.1.

## Build in layers

Sparkless batch execution is the starting point. A package with a streaming API is not automatically a Structured Streaming implementation. Preserve separate capabilities for API shape, processing semantics, state, and recovery.

1. Run a bounded Sparkless transformation over a supplied batch with an explicit schema and batch ID.
2. Add durable source offsets and checkpoints with a single active owner.
3. Add file discovery and ingestion metadata.
4. Compile declarative batch pipeline definitions into a dependency graph.
5. Add CDC, expectations, and selected stateful streaming operators only after their semantics are specified.

Quix Streams is a candidate provider for Kafka/stateful work, not a required base dependency. Evaluate its offset, window, state-store, and restart behavior against BrickHouse's contracts before adopting it. A broker is an explicit optional service. Do not put Kafka setup on the base installation path.

## Micro-batch contract

Define `Source`, `BatchPlan`, `Sink`, and `CheckpointStore` ports. A source produces a stable batch descriptor and a range of source offsets; replaying the descriptor must identify the same input. A plan runs the existing Sparkless engine in a bounded session. A sink returns a durable commit reference. A checkpoint records query identity, source identity, offsets, batch ID, plan/schema version, sink commit reference, and state-store version where relevant.

Record progress only after the corresponding sink commit. On restart, reconcile a previously committed batch before retrying. Exactly-once output can be advertised only for the tested source/sink combination with durable idempotent commits. Arbitrary Python `foreachBatch` effects are at least once unless the caller implements idempotency. A Delta output does not make unrelated HTTP calls or file writes exactly once.

Start with bounded triggers such as an available-data run. Add polling triggers, stop/cancel, progress metrics, and await termination with actual behavior. Reject unsupported trigger/output-mode combinations. Checkpoints must refuse changed source identity or incompatible plans rather than silently restarting from zero.

## State and event time

Watermarks, late events, deduplication, event-time windows, stream-stream joins, and aggregation state need independent fixtures. Define timestamp/timezone behavior, eviction rules, output modes, and restart compatibility for each supported operator. Do not implement a watermark as a filter on the current batch alone.

Checkpoint cleanup requires a retention policy and explicit ownership. A second worker attempting to run the same query must fail or acquire an expired lease safely. Crash tests must cover source discovery, transform, sink commit, checkpoint update, and lease renewal.

## Auto Loader-like file ingestion

Begin with local file discovery through the shared path resolver. Track canonical path plus a stable content identity, schema version, discovery status, and batch assignment. Define behavior for file replacement, duplicates, partial writes, failed files, glob changes, and schema evolution. Use a documented readiness rule rather than reading files still being written.

Implement `cloudFiles` option names only as supported. Schema inference/evolution, rescued data, notification mode, and cloud permissions are separate capabilities. Local directory polling does not establish parity with cloud notification ingestion.

The dltHub `dlt` library may be a useful optional ingestion provider. It is distinct from Databricks DLT and `pyspark.pipelines`; never install it to satisfy a Databricks `import dlt`. Keep any compatibility facade scoped to the worker and error on ambiguous imports.

## Declarative pipelines

First target batch tables/views, dependency extraction, topological execution, run metadata, and repeatable refresh. Define decorators and import surfaces against an explicit Databricks/Lakeflow profile; support legacy names only with fixtures. Store definitions separately from materialized outputs.

Expectations need distinct record, drop, and fail actions with metrics. An expectation failure must affect the correct dataset/run and preserve documented previous outputs. A pipeline refresh is not one atomic transaction across all Delta tables; expose per-dataset commit/run status and recover partial runs honestly.

CDC work requires explicit keys, sequencing, ties, deletes, null update rules, out-of-order events, and SCD behavior. Schema changes and backfills must be planned. Do not label repeated append as CDC or a scheduled Python function as a complete Lakeflow emulator.

## Acceptance gate

Prove restart/replay with deterministic fixtures, including empty batches, duplicate input, late events, changed files, schema change, cancellation, and crash after commit but before checkpoint. Report the exact supported operators, sources, sinks, and guarantees. Keep unsupported operators explicit even if a candidate provider offers a similarly named feature.

## Later deployment reuse

Phases 0.5–0.6 remain local emulator work. Preserve durable query/source/batch/plan identities, checkpoint exclusivity, per-dataset commit evidence and explicit side-effect guarantees so a future scheduler can reuse them. A local owner lock is not a distributed lease. After emulator stability, B053/SH.4 must qualify checkpoint fencing, worker loss/reassignment and commit-before-checkpoint reconciliation across real hosts before claiming deployed durable automation. It must reuse these contracts rather than create a second pipeline scheduler or bypass the table service. See [document 18](18-self-hosted-platform.md).
