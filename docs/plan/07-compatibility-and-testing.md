# 07 — Compatibility and conformance

## Registry contract

`planning/capabilities.json` is an initial requirements inventory. Every entry starts `planned`; source research is not implementation evidence. During development, extend entries down to independently testable behaviors.

Allowed states: `planned`, `unsupported`, `partial`, `supported`, `simulated`, `experimental`. Each claim identifies its profile, exact tested client versions, dependencies, test IDs, limitations, and evidence artifact. `supported` requires passing behavioral tests; `partial` lists excluded cases; `simulated` describes real metadata behavior and absent compute effects.

Expose a generated view through the CLI and `/_brickhouse/capabilities`. Unknown API routes return a stable unsupported error. Maintain the capability registry and public documentation from one source; do not let README checkmarks drift from tests.

## Oracles

| Oracle | What it establishes |
|---|---|
| Local SDK/CLI/Terraform clients | Request/response and client workflow compatibility |
| Sparkless/native provider tests | Internal contracts and local correctness |
| Real PySpark pinned environments | Spark data semantics; not proprietary Databricks runtime behavior |
| Owner's personal Databricks account, occasional authorized captures | Observed Databricks-specific API/runtime semantics used to design local tests |
| Delta protocol fixtures and cross-engine reads | Physical table interoperability |
| Minilake reference tests | Useful regression examples; not the final compatibility authority |

Runtime CI is JVM-free. Use the owner's personal Databricks account for occasional bounded reference probes to verify representative code and resolve test-design questions. [The reference verification plan](15-personal-databricks-verification.md) defines the existing authorization, configuration, capture workflow, and evidence requirements. Routine tests, required CI, milestone gates, and release tests use sanitized stored fixtures locally; they do not execute against Databricks. Separate optional tooling may generate real Spark references or manually capture Databricks observations; the runtime must never call it to complete a user operation.

For each captured fixture record source product/version, date, profile, input schema/data, operation, result/error, ordering requirements, normalization policy, and provenance. Normalize only documented nondeterminism such as IDs/timestamps; do not normalize away type, timezone, or error-class differences. Sort output only when row order is unspecified.

## Test layers

1. Pure domain tests for state transitions, identifier resolution, validation, and policy.
2. Provider tests with real temporary files, Delta logs, Arrow schemas, and committed versions.
3. HTTP contract tests using the real pinned SDK against a running server.
4. Process integration tests exercising worker/kernels, timeouts, logs, and cleanup.
5. Restart/failure injection tests at journal and commit boundaries.
6. Differential semantic tests using reviewed stored oracle results; occasional live reference probes happen outside the test suite and inform those fixtures.
7. Installation tests from built wheels in clean environments, not editable-checkout assumptions.

Do not use mocks to claim data writes, SQL execution, job execution, or durable recovery. Fault injection is appropriate for otherwise rare failures.

## JVM-free and network gates

- Run the base golden workflow in a clean environment without a JRE, full PySpark, delta-spark, or Docker socket.
- Inspect the resolved runtime dependency graph; distinguish allowed pure Python clients from Spark runtime packages.
- Observe child processes and fail on unexpected Java, Spark drivers, or `spark-submit` launches. A missing `java` executable alone is not proof that a remote fallback was absent.
- Disable external network access after dependencies/artifacts are provisioned for the offline gate; permit only loopback/local service traffic.
- Assert no actual Databricks/cloud host is contacted during local conformance runs.
- Run ordinary tests without personal credentials or `.env` loading; explicitly bind SDK clients to loopback BrickHouse and prove inherited credentials cannot redirect them to a real workspace. Reference capture failures never turn local tests into remote runs.
- Separately test optional services for their own native-only execution paths. A Spark MLflow model does not become JVM-free because the tracking server is Python.

## Release workflow gates

**G1 data:** explicit Sparkless session -> managed Delta table -> SDK SQL -> fresh session -> restart.

**G2 jobs:** SDK upload/submit/poll/output -> parameters/secrets -> table mutation -> dependent task -> external read.

**G3 failure:** cancelled/crashed attempt -> persisted terminal state -> safe retry -> no duplicate committed rows.

**G4 clients:** pin-specific CLI, Terraform, bundle, and SQL-connector workflows for advertised compatibility.

**G5 streaming (later):** replay after failure preserves declared offset/state/output guarantees; late-data/window behavior matches fixtures.

**G6 extensions:** each advertised optional extra installs and passes an actual provider workflow with the base stack.

## Coverage reporting

Report case counts by domain/profile, with totals for supported, partial, simulated, unsupported, and unexamined cases. Do not compute a single 'Databricks coverage percentage' from implemented endpoint count. Keep the target universe versioned so adding newly discovered APIs cannot silently preserve an inflated claim.

Track known mismatches with issue IDs and regression fixtures. An expected failure requires a stated reason and capability limitation; removing tests or broadening skips is not a fix.

## Performance

Benchmark cold startup, first job latency, warm query latency, peak resident memory, Arrow conversion cost, and representative ETL sizes. Record hardware, native versions, data schema/size, repetitions, and variance. Establish baselines first; no promised 10x/100x acceleration or strict timing threshold is assumed by this plan.
