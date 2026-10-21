# 00 — Vision and scope

## Objective

Build **BrickHouse**, a locally installable Python package that aims to reproduce the observable behavior of Databricks APIs and common data/AI workflows without a JVM. It gets its own repository and release lifecycle. Sparkless provides the engine foundation; Minilake contributes relevant API implementations and design knowledge; native libraries supply storage, SQL translation, notebooks, and optional services.

Full emulation is the long-term direction. It must be tracked as a versioned collection of behavioral contracts. It does not mean that a first release reproduces every Databricks product or that single-machine execution reproduces a distributed cluster's timing, scheduling, or performance. Unimplemented domains remain visible in the roadmap and compatibility denominator.

## Long-term deployment direction and priority

**The primary product is a Databricks emulator without a JVM.** Phases 0.1–0.9 continue to prioritize evidenced emulator behavior. Once the emulator is stable, the same core may support a self-hosted lakehouse workspace on infrastructure users control. Self-deployment comes afterwards; it is not an additional platform requirement for the emulator releases.

[Document 18](18-self-hosted-platform.md) maps each existing phase to architectural boundaries worth preserving and defines the separate SH.0–SH.6 deployment track. B049 assesses actual stability after B042 before any deployment implementation begins. Network authentication, a browser workspace, hostile-code isolation, remote workers and operational qualification belong to that later track. The local wheel must remain usable independently of all deployment tooling. [ADR 0034](../decisions/0034-emulator-first-self-hosted-platform.md) records this priority.

## Intended users and workflows

- Developers running existing Databricks Python transformations and notebook jobs locally.
- Teams exercising Databricks SDK, CLI, Terraform, and bundle deployment flows in CI.
- Developers testing catalog-backed Delta data, job dependencies, SQL statements, secrets, and file operations together.
- ML and AI developers needing local experiments, models, and retrieval workflows with compatible client surfaces.

The first release optimizes for a single developer and modest local datasets. Run multiple isolated BrickHouse instances for concurrent CI work. Multi-user authorization and larger workloads are later capabilities, not assumed properties of the first server.

## Non-negotiable constraints

1. Execution succeeds without a JRE, Spark installation, Docker daemon, Databricks account, or cloud compute for the base workflow.
2. Python with Rust/C/C++ wheels is acceptable; optional Node or native services must be explicit extras/tools. No JVM-containing sidecar may be smuggled into a claimed JVM-free path.
3. Standard installation is a wheel-based Python install. Building Sparkless from source is a development option, not an end-user requirement on supported platforms.
4. API behavior must be implemented or explicitly rejected. Infrastructure metadata may be simulated only where documented as such.
5. Catalog, storage, SQL, and runtime share state through declared contracts.
6. External upstream cooperation is useful but must never be a prerequisite for BrickHouse making progress.

## Compatibility dimensions

Track four dimensions independently:

| Dimension | Example evidence |
|---|---|
| Client/protocol compatibility | A pinned WorkspaceClient creates and polls a job correctly |
| Data semantics | Nulls, decimals, timezones, ordering, and errors match a specified Spark profile |
| Workflow behavior | Failed dependencies, retries, cancellation, secret scope, and restart recovery behave as specified |
| Operational simulation | Cluster IDs and lifecycle transitions exist; no cloud cluster was provisioned |

Use default profile `spark35-local-v1` initially, based on the reviewed Sparkless 3.5 compatibility setting. Add a separate `spark4-local-v1` only after its conformance gates. Do not equate either profile with a complete Databricks Runtime release. Declare SDK, SQL dialect, Delta feature set, and runtime API coverage independently.

Compute emulation explicitly includes **serverless and classic Python jobs/notebooks** and **serverless/pro/classic SQL warehouses**. Track compute mode, access mode, environment/runtime reference and protocol separately from engine semantics. The bounded 0.1 subsets execute real local work; allocation/lifecycle infrastructure is labeled simulated, with broader environment/access/retry and Connect behavior assigned to later phases. [Document 17](17-serverless-and-classic-compute.md) defines the researched behavior contracts and acceptance owners; [ADR 0016](../decisions/0016-serverless-and-classic-compute-profiles.md) records the scope addition.

## Initial release scope

Required: local server lifecycle; catalogs/schemas; managed and external Delta tables; Sparkless DataFrames; bounded SQL through Statement Execution; serverless/classic Python jobs and notebook-extra workflows; three SQL warehouse profiles; scoped parameters/outputs/files/dbutils; restart recovery; capability discovery; pinned SDK/CLI/Terraform/bundle/REST connector workflows. [Document 16](16-phase-0.1-initial-release.md) fixes the exact corpus and release gates.

Next: expanded DAG control flow, environment/access policies, broader SQL/runtime surfaces, MLflow, dbt and local model serving. Notebook installation is optional, but its declared 0.1 workflow is a required separate gate.

Later: durable streaming, Auto Loader-like ingestion, Lakeflow declarative pipelines, Vector Search, feature engineering, fine-grained governance, Spark Connect, and broader SQL-driver protocols.

Keep an explicit frontier for Scala/JVM user code, arbitrary JAR libraries, Spark internals, MLlib parity, distributed execution behavior, Photon-specific behavior, managed-cloud networking, proprietary AI services, dashboards/Genie, Delta Sharing, Lakebase, and account-level infrastructure. These are long-term research tracks or documented incompatibilities. Adding a new native provider may eventually close a subset; shipping the package does not close them automatically.

## Success measures

- Count supported behavior cases by profile and domain, with unsupported/partial/simulated states visible.
- Measure cold server startup, first job latency, notebook startup, memory, and small ETL duration against recorded datasets and hardware.
- Demonstrate no Java/Spark runtime processes and no cloud fallback during the golden workflow.
- Verify data survives restarts and all access surfaces agree on table contents and schema.
- A first public alpha may be useful with limited coverage. Do not advertise complete Databricks compatibility without an explicitly bounded, independently evidenced claim.
