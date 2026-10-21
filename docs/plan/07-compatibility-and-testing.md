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

The initial released-client pins and the distinct SQL REST/kernel and Thrift profiles are recorded in the [B005.1 client matrix](../../artifacts/conformance/B005.1-client-profile-matrix.md). The matrix is research evidence, not a compatibility claim. Its `spark35-local-v1` semantic target does not identify an observed Databricks Runtime or warehouse. Revalidate pins before implementation and publication.

B045 verified the exact connector 4.6.0 and kernel 1.1.0 wheels against a local HTTPS capture. It observed one authenticated SQL session-open request and the connector's 401 handling with strict TLS verification; it did not execute SQL. The private custom-CA argument and host-with-port workaround are contained in the research harness under [ADR 0021](../decisions/0021-sql-kernel-local-transport-adaptation.md); no query compatibility claim follows.

B020.1 verifies a selected workflow using the actual [Databricks CLI v1.21.0](../../artifacts/conformance/B020.1-databricks-cli-v1.21.0.md) against an ephemeral loopback server: workspace Python-source import, duplicate/missing-source errors, Python job create/submit/poll/output, and scratch cleanup. This makes C030 partial for that exact CLI profile. The CLI fixture uses a synthetic token because the local API does not authenticate requests; authentication, broader workspace routes, SQL, compute resources, Terraform, bundles, and remote workspace behavior remain unverified.

B020.2 verifies Terraform Core 1.16.6 with `databricks/databricks` 1.137.0 against a disposable loopback workspace. The selected resource subset is `databricks_workspace_file` plus a single-task `databricks_job` that runs an uploaded Python script on an existing simulated local classic cluster. Apply/read/update/refresh/destroy, a rejected update, recovery through refresh, worker output, and resource cleanup are exercised by the [opt-in conformance test](../../tests/integration/test_terraform_v1_16_6_provider_1_137_0.py) and recorded in the [B020.2 report](../../artifacts/conformance/B020.2-terraform-v1.16.6-provider-v1.137.0.md). The local host advertises a deterministic numeric ID only for the provider's workspace routing metadata; this ID is simulated and is not the workspace's UUID or a Databricks account ID. No token from `.env` or a GitHub secret is read. Classic cluster allocation remains simulated; broader Terraform resources, SQL, authentication, and remote workspace behavior remain unverified.

B020.3 verifies Databricks CLI v1.21.0 bundle validate/deploy/run/destroy for one classic single-task Python job against an ephemeral loopback workspace. The CLI uploads and exports bundle JSON/source files, creates local workspace folders, resolves a synthetic current-user identity, executes a real local worker, and destroys the bundle through managed-file cleanup and a job tombstone that preserves run history. The [opt-in conformance test](../../tests/integration/test_databricks_cli_bundle_v1_21_0.py) and [B020.3 report](../../artifacts/conformance/B020.3-databricks-cli-bundle-v1.21.0.md) record the profile and exact boundary. The CLI subprocess receives only a synthetic token and a temporary config/home; no `.env`, GitHub secret, personal Databricks account, cloud compute, Spark, or JVM is used. The user identity and `new_cluster` allocation are simulated. Notebook tasks, pipelines, apps, dashboards, serverless bundles, multi-task output, broad bundle semantics, authentication, and remote workspace behavior remain unverified.

[Document 17](17-serverless-and-classic-compute.md) defines separate serverless/classic workload and SQL warehouse targets. Every claim identifies compute/access mode, environment or runtime reference, semantic profile and protocol as an explicit tested combination. A planned profile list does not claim every cross-product combination. C034 measures infrastructure simulation separately from workload/environment capabilities C053–C056; none is established by B043's source research. A serverless SQL capture cannot stand in for serverless Python, classic compute or Connect evidence.

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

For 0.1.0, G1–G4 include the selected serverless and classic Python/notebook variants and serverless/pro/classic warehouse cases from document 17. Verify real execution, invalid selectors/settings and resource lifetimes; use a controlled clock for infrastructure timing rather than cloud startup sleeps. Missing required mode behavior blocks the gate or requires a recorded scope revision. Full Connect protocol/analysis behavior has its own later B036 gate.

**G5 streaming (later):** replay after failure preserves declared offset/state/output guarantees; late-data/window behavior matches fixtures.

**G6 extensions:** each advertised optional extra installs and passes an actual provider workflow with the base stack.

[Document 16](16-phase-0.1-initial-release.md#release-case-families-and-evidence-owners) fixes 27 planned acceptance families with task owners, seven Python job and ten notebook variants, local test limits and candidate evidence requirements. Family IDs are design identifiers, not passing test IDs. B045 transport research and B046 provider development artifacts resolve prerequisites; runtime support still requires the real downstream workflows.

## Post-stability deployment gates

The emulator remains primary through phases 0.1–0.9. B048 uses real G1–G4 evidence for a bounded architecture review before B023; it does not assert deployed functionality. B049 assesses repeated applicable G1–G6, exact candidate/compatibility scope, defects, upgrades and coordinated restore after B042. Deployment implementation cannot begin before that stability gate. [Document 18](18-self-hosted-platform.md) defines proposed trust profiles and exact evidence responsibilities; the IDs below identify future gates, not passing tests.

| Gate | Owner / stage | Required evidence |
|---|---|---|
| S1 | B050 / SH.1 | Real verified TLS and authentication, service-wide authorization/denial, protected durable secrets and rotation, bounded single-server work and coordinated restore; trusted-code limits explicit |
| S2 | B051 / SH.2 | Actual browser/source/job/SQL workflows, session/output security and cross-principal denial, selected optional-provider lifecycle and state restore |
| S3 | B052 / SH.3 | Actual selected isolation mechanism denies host/other-principal access and enforces resource/cancellation/cleanup limits; applicable emulator corpus still passes |
| S4 | B053 / SH.4 | Real enrolled second host, verified native storage/transfer, fenced attempts/checkpoints/commits, partition/stale-worker/lost-response recovery and selected automation reassignment |
| S5 | B054 / SH.5 | Clean install/upgrade, coordinated restore to another host with measured recovery objectives, rotation, capacity/soak/overload results, monitoring and operator runbooks |
| S6 | B055 / SH.6 | Applicable G1–G6 and S1–S5 from exact distribution artifacts for each claimed deployment profile, plus independently passing local installation |

Use real local services and provisioned test infrastructure. Simulations may inject external failure boundaries but cannot prove isolation, host separation or storage effects. No paid infrastructure or cloud workspace run is authorized by this plan. Absence of qualifying infrastructure leaves the relevant future gate blocked. Deployment/OS/isolation/storage versions are independent of semantic/compute/client versions, and simulated compute metadata remains separately labeled.

## Coverage reporting

Report case counts by domain/profile, with totals for supported, partial, simulated, unsupported, and unexamined cases. Do not compute a single 'Databricks coverage percentage' from implemented endpoint count. Keep the target universe versioned so adding newly discovered APIs cannot silently preserve an inflated claim.

Track known mismatches with issue IDs and regression fixtures. An expected failure requires a stated reason and capability limitation; removing tests or broadening skips is not a fix.

## Performance

Benchmark cold startup, first job latency, warm query latency, peak resident memory, Arrow conversion cost, and representative ETL sizes. Record hardware, native versions, data schema/size, repetitions, and variance. Establish baselines first; no promised 10x/100x acceleration or strict timing threshold is assumed by this plan.
