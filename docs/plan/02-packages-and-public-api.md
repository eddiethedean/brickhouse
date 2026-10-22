# 02 — Packages and public API

## Distribution decision

Start with one published distribution, **`brickhouse`**, importing as **`brickhouse`**, in a new repository. Use a `src/` layout, `pyproject.toml`, and a standard wheel backend such as Hatchling. Baseline Python: 3.11+; initially gate supported wheels on Python 3.11/3.12 and Linux x86_64/macOS arm64. Expand support based on dependency wheel evidence.

Avoid separate distributions for every module. Sparkless remains independently released. Minilake-derived code is private implementation with provenance, not a new public `brickhouse-minilake` compatibility promise.

Suggested internal layout:

```text
src/brickhouse/
  __init__.py, cli.py, config.py, capabilities.py
  api/                 # public Databricks and private worker transports
  domain/              # engine-neutral values and error taxonomy
  ports/               # interfaces for engines, tables, secrets, execution
  services/            # instance-scoped application behavior
  persistence/         # SQLite repositories and migrations
  catalog/             # name binding and schema mapping
  storage/             # paths, volumes, staging, Delta provider
  engines/             # Sparkless and optional DuckDB adapters
  runtime/             # supervisor, worker bootstrap, environment handling
  compat/              # worker-scoped pyspark/delta/dbutils facades
  notebooks/           # conversion, magics, execution
  testing/             # pytest plugin, fixtures, assertions
  integrations/        # optional MLflow, dbt, vectors, streaming
  _vendor/minilake/     # only if B002 chooses retained derived modules
```

Source adapted out of `_vendor` must retain per-file provenance in the third-party inventory. `_vendor` is not a reason to bypass architecture rules.

## Future deployment packaging

The emulator's local wheel is the primary install through phases 0.1–0.9. [Document 18](18-self-hosted-platform.md) stages optional deployment artifacts only after B049 establishes stability. Later images/launch tooling must contain exact tested core/native artifacts, retain one distribution until the split criteria below are met, and keep container/orchestration/identity-service requirements out of base installation. Do not publish an empty deployment extra now. Databricks bundle/Terraform client compatibility is distinct from installing and operating BrickHouse itself.

## Extras and dependencies

| Install target | Intended contents |
|---|---|
| `brickhouse` | Local server and core workflows; Sparkless 4.15.2, delta-rs 1.2.1, PyArrow 23.0.1, SQLGlot 30.24.0, SQLite persistence |
| `brickhouse[notebooks]` | nbformat, nbclient, ipykernel, jupyter-client; Papermill only if parameterization needs it |
| `brickhouse[ml]` | MLflow server and Python-model support |
| `brickhouse[dbt]` | dbt-core/dbt-duckdb tested combination |
| `brickhouse[vector]` | Selected embedded vector provider, initially evaluate LanceDB |
| `brickhouse[streaming]` | Future selected streaming provider; do not publish a nonfunctional extra |
| `brickhouse[testing]` | pytest support for external users; test oracle clients remain developer tooling |
| `brickhouse[cloud-test]` | Optional cloud-storage test tooling; external services documented separately |

This table describes the target 0.1 distribution. The base dependency set is now declared with tested Sparkless, delta-rs, PyArrow, and SQLGlot pins; [B018.2](../../artifacts/conformance/B018.2-installation-lanes.md) verifies a built install with no notebook packages and a separate notebook-extra install. B023 must still verify the final release candidate and full base corpus. [Document 16](16-phase-0.1-initial-release.md) defines the candidate installation/evidence gate.

The Databricks SDK is a required development/conformance client; make it a runtime dependency only where the chosen adapter actually uses it. Do not install full PySpark or delta-spark in the base dependency graph. A pure Python Spark Connect client may be an optional conformance dependency once a native Connect server exists.

Potential later distribution names are **`brickhouse-connect`** (large gRPC protocol dependency boundary) and **`brickhouse-streaming`** (independent runtime/provider). Split only when there is an independent installation use case, a stable plugin protocol, separate release needs, and conformance CI for version skew. Do not create them in M0.

## Resolved provider and client environments

[Document 20](20-open-source-reuse.md) and [the research snapshot](../../planning/oss-reuse.json) distinguish existing pins from unqualified candidate versions. Every actual optional extra needs its own resolved/hash-pinned dependency and supported-wheel matrix before publication. Do not make mutually incompatible clients co-installable by removing their bounds or downgrading core evidence pins. The inspected dbt-databricks profile belongs in an independent conformance environment; MLflow must include its selected full service graph, and Delta Sharing must include the exact native wrapper and Arrow combination.

The core and notebook entries above target the bounded 0.1 workflow. ML/dbt/vector/streaming and future protocol/provider integrations remain later extras; the current project metadata is authoritative for what can actually be installed today. No new extra, wheel or dependency requirement is created by this research. The pure Connect candidate is distinct from full PySpark and must stay isolated from host packages/import facades.

## CLI: implemented subset and target commands

The current command set is:

```bash
brickhouse init --workspace ./workspace
brickhouse serve --workspace ./workspace --host 127.0.0.1 --port 8000
brickhouse capabilities --format json
brickhouse doctor
brickhouse run ./jobs/transform.py
```

`init` creates a new version 1 workspace with metadata schema version 21. `serve` requires an initialized workspace, binds to IPv4 loopback, serves `/health`, `/version`, and `/capabilities`, and provides pinned-SDK subsets for catalog/schema operations, classic cluster lifecycle, Python workspace-source import, and Python script job create/submit/poll/output/delete. The Terraform profile supports the tested workspace-file and single-task Python-job lifecycle through provider v1.137.0. Databricks CLI v1.21.0 also supports one tested bundle profile: validate/deploy/run/destroy a classic single-task Python job, using local workspace JSON/Python files, a simulated current-user identity, and simulated new-cluster metadata. Bundle destroy removes managed bundle files and tombstones the job while retaining immutable definitions and run history. Its numeric host-discovery workspace ID is a deterministic simulated routing identity, separate from the workspace UUID; it does not authenticate requests or represent a Databricks account. The job service validates the selected local compute/environment profile, persists a bounded DAG of up to ten tasks, and dispatches eligible uploaded scripts sequentially in stable topological order through fresh child processes. Failed dependencies become skipped tasks and independent branches continue. Per-task retries honor the validated 0–10 retry limit, retry delay, and explicit timeout-retry policy; each attempt and its bounded output are persisted. Per-job `max_concurrent_runs` accepts 1–10; the process dispatches at most two worker attempts and persists up to eight additional runs in FIFO queue. Overflow returns HTTP 409 before a run record is created, and idempotency replays return the existing run. On restart, undispatched queued runs resume; jobs with started attempts are interrupted without inventing exit status or replaying user code. Reset revisions share the stable public job admission identity. Multi-task output retrieval remains unsupported. Single-task requests can pass bounded Python parameters as argv. Job submission reserves classic task consumers in the workspace store. Before dispatch, a new/shared resource starts for its first consumer; all-purpose identity persists and can autostart. Task completion, failure, cancellation, skipped work, and service recovery release consumers; new/shared resources terminate after their last consumer. A generation monitor interrupts an active Python worker after all-purpose compute restarts or terminates, and the supervisor reaps its POSIX process group. Compute IDs and allocation remain simulated. Workers receive the selected bounded Sparkless facade and registered-table DeltaTable reads; authenticated Arrow appends to existing registered Delta tables go through the parent table service. Table creation, overwrite and arbitrary path writes remain unsupported. Metadata schema 21 includes stable job admission identities and durable FIFO dispatch markers; schemas 1–21 upgrade transactionally. Earlier migrations add worker identity/outcomes, task values, immutable job revisions/tombstones and SQL journal operation modes. The server holds the workspace owner lock and releases it on Ctrl-C or SIGTERM. `WorkspaceContext.jobs` remains the internal durable definition/run/task/attempt repository; `WorkspaceContext.compute` stores all-purpose and new/shared job-compute lifecycle state. `run` still exits with a specific unavailable-feature error and never starts the requested script. `capabilities` reports implemented foundations and planned services. `doctor` reports package/runtime versions, provider distribution metadata, and optional workspace validation without importing optional integrations. The local SDK token is required only for client configuration and does not authenticate callers. The catalog API does not implement permissions, workspace binding, storage roots, arbitrary properties, or table data access. A help/version command must not start optional services or load optional integrations.

The implemented embedded interface is limited to workspace ownership and its validated descriptor:

```python
from brickhouse import Workspace

with Workspace.local(directory="./workspace") as bh:
    print(bh.workspace.workspace_id)
```

The public `Workspace.session()` interface remains a target; the internal engine adapter is not yet exposed through `Workspace`:

```python
from brickhouse import Workspace

with Workspace.local(directory="./workspace", profile="spark35-local-v1") as bh:
    with bh.session() as spark:
        df = spark.createDataFrame([{"id": 1, "name": "Alice"}])
        df.write.mode("overwrite").saveAsTable("demo.default.people")
        rows = spark.table("demo.default.people").collect()
```

The internal B010.1–B010.3 factory binds version-pinned Delta snapshots, creates managed tables, delegates existing-table append/overwrite, supports explicit writes to registered local paths through the common Delta service, and applies resolved execution settings to isolated sessions. `DataFrameWriter.saveAsTable()` requires append or overwrite mode; writer options and partitioning fail explicitly. Direct path `save()`, insert operations, and DataFrameWriterV2 are blocked. Tests cover coexisting session namespaces, configuration, temporary views, callable UDF results, serverless defaults, and an explicit classic-standard runtime/settings profile. Classic defaults without explicit values and unmapped runtimes fail clearly. These are internal local profiles, not a public `Workspace.session()` API or Databricks compatibility claim. Selected job workers separately receive a bounded worker-scoped Sparkless/Delta facade. The pinned SDK can already call the bounded local catalog/schema profile:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(host="http://127.0.0.1:8000", token="brickhouse-local", auth_type="pat")
```

The token is initially an API compatibility value, not proof of multi-user security. Worker credentials are separate and actually scoped.

The local profile accepts lowercase unquoted catalog/schema identifiers and optional comments. Catalog creation creates `default` and `information_schema`; namespace lists support SDK pagination. The default catalog/schema and `information_schema` cannot be deleted, and registered tables prevent parent schema/catalog deletion. Duplicate names, missing targets, invalid identifiers, and in-use namespaces return Databricks-style error codes. Other SDK request fields fail explicitly. These local checks use `databricks-sdk==0.153.0`; they do not establish permission behavior or parity with a Databricks workspace.

## Compatibility imports

Three independent modes:

1. **Explicit:** users import Sparkless and request BrickHouse integration. Lowest surprise; first implementation target.
2. **Managed worker:** the BrickHouse launcher enables a capability-limited `pyspark`/`delta` import facade only inside a fresh worker. Reject installation conflicts and unsupported namespaces clearly.
3. **Remote client:** later, an unmodified pure Python Spark Connect client connects to a native BrickHouse gRPC service.

A Python import facade is not a Spark Connect server. Importing `brickhouse` in an arbitrary host process must not monkeypatch global modules or attach to a remote workspace.

## Plugins

Define `brickhouse.providers` entry points only after two real providers establish a useful stable interface. Require provider API version, capability declarations, lifecycle methods, and health status. Optional integrations load lazily and fail with an installation hint if absent. A base installation must run with no imports from MLflow, dbt, LanceDB, or streaming packages.
