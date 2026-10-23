# BrickHouse

**Run selected Databricks workflows locally, without a JVM.**

[![CI](https://github.com/eddiethedean/brickhouse/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/eddiethedean/brickhouse/actions/workflows/ci.yml)
[![Python 3.11–3.12](https://img.shields.io/badge/python-3.11%20%7C%203.12-3776AB?logo=python&logoColor=white)](#compatibility)
[![Status: alpha](https://img.shields.io/badge/status-alpha-orange)](https://github.com/eddiethedean/brickhouse/blob/main/STATUS.md)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/eddiethedean/brickhouse/blob/main/LICENSE)

BrickHouse gives you a persistent local workspace for Python jobs, SQL, Delta
tables, and optional notebooks. Sparkless executes DataFrames and selected Spark
SQL; delta-rs stores real Delta tables. Every execution surface uses the same
catalog and table service, so a job's committed output is available to SQL and
other sessions.

Use it to develop transformations, exercise selected Databricks client workflows,
and design local integration tests. The base workflow needs no Java, Spark
installation, Docker, Databricks account, or cloud compute.

> **0.1.0 alpha:** compatibility is partial and versioned. Compute resource
> lifecycles and access modes are simulated; jobs and queries execute locally.
> Check the [compatibility scope](#compatibility) before adopting a workflow.

[Quickstart](#quickstart) · [Capabilities](#what-you-can-do) ·
[Compare](#feature-comparison) · [Benchmarks](#local-benchmarks) ·
[Compatibility](#compatibility) · [Development](#development) ·
[Documentation](#documentation)

## Quickstart

Use **Python 3.11 or 3.12**. The qualified platforms are **Ubuntu 22.04 x86_64**
and **macOS 26 arm64**. The commands below install from the source checkout.

### 1. Install BrickHouse and the SDK

```sh
git clone https://github.com/eddiethedean/brickhouse.git
cd brickhouse
python -m venv .venv
source .venv/bin/activate
python -m pip install . databricks-sdk==0.153.0
```

The SDK is a client for the example; it is separate from BrickHouse's base runtime.
To enable notebook jobs, also install the optional extra:

```sh
python -m pip install '.[notebooks]'
```

### 2. Start a local workspace

```sh
brickhouse init --workspace ./workspace
brickhouse serve --workspace ./workspace --host 127.0.0.1 --port 8000
```

The server accepts IPv4 loopback connections only. Use a disposable workspace and
trusted code: the API has no inbound authentication, and workers run as your local
user with access to your files and processes.

### 3. Run a job and query its Delta output

In another terminal, from the repository root:

```sh
source .venv/bin/activate
python examples/phase_0_1/run_orders.py --host http://127.0.0.1:8000
```

The example uses the real SDK to create a managed Delta table, upload and run a
Python worker, append typed order rows, and query the committed data through SQL.
Its JSON output includes resource IDs and this `result` value:

```json
[["alice", "2", "15.75"]]
```

See the [example walkthrough](https://github.com/eddiethedean/brickhouse/blob/main/examples/phase_0_1/README.md),
[SDK orchestration](https://github.com/eddiethedean/brickhouse/blob/main/examples/phase_0_1/run_orders.py),
and [worker code](https://github.com/eddiethedean/brickhouse/blob/main/examples/phase_0_1/orders_worker.py).
The run leaves its table and resource history available for inspection. Stop the
server with Ctrl-C; restarting with the same workspace preserves metadata and
committed tables.

### Connect your own SDK code

With the server running, point the qualified SDK at the local endpoint:

```python
from databricks.sdk import WorkspaceClient

client = WorkspaceClient(
    host="http://127.0.0.1:8000",
    token="brickhouse-local",
    auth_type="pat",
)

print([catalog.name for catalog in client.catalogs.list()])  # ['main'] in a fresh workspace
```

The token is a local compatibility value. Jobs are submitted through the SDK Jobs
API; `brickhouse run` is reserved and currently reports an unavailable feature.

## What you can do

Each row describes the selected 0.1.0 subset. Exact operations, limits, and evidence
are recorded in the [capability registry](https://github.com/eddiethedean/brickhouse/blob/main/planning/capabilities.json).

| Surface | Included behavior |
| --- | --- |
| **Tables and SQL** | Persistent catalogs and schemas; managed and external Delta tables; selected queries, parameters, UPDATE/DELETE/MERGE, history, and snapshot reads. |
| **Python jobs** | Fresh child processes, isolated Sparkless sessions, bounded task DAGs, retries, cancellation, timeouts, and retained output. |
| **Job utilities** | Selected local DBFS operations, widgets, scoped secrets, and Python job task values. |
| **Notebooks** | Optional Jupyter runtime for ipynb jobs and the declared Python source notebook subset, including selected magic and child-notebook behavior. |
| **Compute profiles** | Seven selected serverless/classic variants for Python and ipynb jobs, three source notebook profile families, and serverless/Pro/classic SQL warehouse profiles. Resource lifecycles are simulated. |
| **Client workflows** | Selected SDK, CLI, bundle, Terraform, and SQL connector workflows against local services. |
| **Recovery** | Durable operation reconciliation, restart handling, coordinated workspace snapshot/restore, and managed-table ownership checks. |

The runtime has one authority for catalog metadata and table commits. Sparkless
handles the selected DataFrame and SQL semantics; native delta-rs owns table
mutations; SQLite persists workspace metadata and run state. DuckDB is reserved
for a future explicit execution option. A Sparkless failure is surfaced to the
caller.

Supported `pyspark`, `databricks`, and `delta` import compatibility is scoped to job
workers. It does not replace packages in the host environment. Worker table writes
are limited to bounded appends on existing registered tables; embedded and SQL
surfaces have their own declared operation sets.

## Feature comparison

MiniLake and `calvinchengx/databricks-emulator` also expose local Databricks
workspace APIs. This comparison uses source snapshots reviewed on **2026-10-22**.
**◐ Partial** means a selected implemented subset; **⚙ Simulated** means
infrastructure metadata; **🗓 Planned** marks future BrickHouse work.

| Feature | BrickHouse 0.1.0 | [MiniLake 1.8.0 snapshot](https://github.com/dmux/minilake/tree/b2736501ed13c6b069204a202a9e6142a5b7e54c) | [databricks-emulator snapshot](https://github.com/calvinchengx/databricks-emulator/tree/05044929caf78a7f2155d8de218d3e3e4c53a173) |
| --- | --- | --- | --- |
| SQL engine | ◐ Sparkless, selected Spark SQL | DuckDB with dialect rewrites | Attached Spark SQL engine; Sail profile |
| Delta tables | ◐ Native delta-rs; shared catalog and commit service | External Delta reads in DuckDB, writes via Spark; managed DuckDB tables | Sail Delta; UC OSS external registrations |
| Catalog/schema APIs | ◐ Local SQLite catalog | DuckDB catalogs and API metadata | UC OSS sidecar |
| Python jobs and notebooks | ◐ Fresh Python workers; optional ipynb/source notebooks | Spark containers or Python-only subprocess; optional JupyterLab | Python scripts/notebooks through an attached agent |
| Serverless/classic compute | ◐ Selected job/warehouse profiles; ⚙ infrastructure lifecycles | Cluster/warehouse metadata; Spark job containers | Engine-backed session handles |
| SDK, CLI, Terraform, bundles | ◐ Selected workflows for all four | Documented workflows for all four | Selected SDK/CLI/Terraform; bundle deploy not claimed |
| SQL drivers / Spark Connect | ◐ REST/kernel connector; **🗓 Planned** Thrift and Connect | Statement API; Thrift/Connect not documented | Thrift connector and Spark Connect with attached engines |
| Persistence | SQLite metadata, Delta journal/reconciliation | Files/DuckDB plus opt-in JSON state snapshots | File stores; jobs/warehouse state in memory |
| Authentication / grants | Local API unauthenticated; **🗓 Planned** identity/grants | Any token accepted; stored grants not enforced | PAT/OIDC authentication; UC grants unimplemented |
| Browser workspace | **🗓 Planned** after emulator stability | SQL web UI; optional JupyterLab | No workspace UI documented |
| MLflow tracking / registry | **🗓 Planned** phase 0.3 | Not implemented | Tracking/registry metadata subset |
| dbt | **🗓 Planned** phase 0.3, starting with dbt-duckdb | Not implemented | Selected dbt-databricks models and dbt tasks |
| MCP | Not implemented | Optional MCP tools | SQL MCP subset |
| License | Apache-2.0 | MIT | Apache-2.0 |

Other columns summarize upstream documentation and source, rather than a complete
independent conformance audit. Sources: MiniLake's
[README](https://github.com/dmux/minilake/blob/b2736501ed13c6b069204a202a9e6142a5b7e54c/README.md)
and [configuration](https://github.com/dmux/minilake/blob/b2736501ed13c6b069204a202a9e6142a5b7e54c/docs/configuration.md);
the Go emulator's [parity ledger](https://github.com/calvinchengx/databricks-emulator/blob/05044929caf78a7f2155d8de218d3e3e4c53a173/docs/parity.md)
and [engine setup](https://github.com/calvinchengx/databricks-emulator/blob/05044929caf78a7f2155d8de218d3e3e4c53a173/docs/08-jobs-and-spark.md).
BrickHouse's planned entries follow the [roadmap](https://github.com/eddiethedean/brickhouse/blob/main/ROADMAP.md).

## Local benchmarks

Measured **2026-10-22** on an **Apple M4 Pro, 12 logical CPUs, 24 GiB RAM,
macOS 26.5.2 arm64, CPython 3.11.15**, using native host processes. Two complete
runs provide **10 fresh trials** and **250 timed samples per warm workload per
product**. Latency cells show **median / p95**; RSS is a single-process median.

| Workload | BrickHouse | MiniLake | Go emulator |
| --- | --- | --- | --- |
| Fresh workspace → first verified 64 KiB file | 297.10 / 339.35 ms | 370.35 / 583.05 ms | 58.49 / 83.58 ms |
| Warm 64 KiB file import + export | 2.74 / 4.94 ms | 2.73 / 4.99 ms | 3.31 / 5.27 ms |
| Warm filtered/grouped SQL over 10,000 Delta rows | 29.95 / 33.19 ms | 2.59 / 3.17 ms | 4.86 / 5.53 ms |
| Idle core API server RSS (median only) | 44.98 MiB | 98.19 MiB | 15.26 MiB |

A follow-up [SQL profile](docs/benchmarks/sql-profile.md) attributes **71%** of
BrickHouse's instrumented request time to SQLite calls and **17%** to Sparkless
SQL/Arrow boundaries. It retains raw traces, an uninstrumented baseline and
measured profiling overhead; no optimization is included in these results.

A separate [SQLite storage experiment](docs/benchmarks/sqlite-memory.md) measured
**27.15 ms** median with ordinary disk metadata, **11.64 ms** with disk/shared
cache, and **9.34 ms** with shared in-memory metadata (**2.91×** faster than
ordinary disk). Memory metadata is benchmark-only and temporary; Delta files
stay on disk. These configurations do not change the comparison table above.

Startup includes workspace initialization and the first verified script round
trip. Startup/RSS cover the core API process before SQL work; they exclude
optional notebooks and the Go SQL engine/agent processes. The SQL case reads the
same immutable Delta fixture and verifies all 100 aggregate result rows. MiniLake
uses DuckDB; the Go profile attaches native Sail 0.7.1 and a pinned upstream agent.
Table registration, engine startup, installation, and builds are outside warm
query timings.

These are small local workflow measurements with warm filesystem caches. Engines,
durability models, and API paths differ; the figures do not establish general
Spark performance or complete Databricks parity. Both runs, every timed sample,
source/artifact hashes, setup probes, and reproduction instructions are in the
[benchmark report](https://github.com/eddiethedean/brickhouse/blob/main/docs/benchmarks/local-emulators.md)
and [raw evidence](https://github.com/eddiethedean/brickhouse/tree/main/artifacts/benchmarks/2026-10-22-local-emulators).

## Compatibility

The 0.1.0 registry contains **24 partial**, **2 simulated**, and **41 planned**
capabilities. No broad capability is marked fully supported. Unsupported operations
raise explicit errors.

The qualified platform matrix is Ubuntu 22.04 x86_64 and macOS 26 arm64, each on
Python 3.11 and 3.12, with separate base and notebook installations. Windows and
other OS versions are outside the 0.1.0 qualification scope.

| Client | Qualified version | Workflow scope |
| --- | --- | --- |
| Databricks SDK | `0.153.0` | Selected catalog, workspace, compute, jobs, and Statement Execution operations. |
| Databricks CLI | `1.21.0` | Selected workspace/job lifecycle and bundle validate/deploy/run workflows. |
| Terraform / Databricks provider | `1.16.6` / `1.137.0` | Selected file, job, cluster, and warehouse resource lifecycles. |
| SQL connector / SQL kernel | `4.6.0` / `1.1.0` | Selected REST/kernel session and inline JSON result workflows. |

These versions identify tested client subsets. They do not imply that every method
or protocol in those clients is implemented. SQL connector Thrift, Arrow-stream/LZ4
results, broad metadata methods, and remote authentication remain outside the
qualified subset.

The base runtime pins **Sparkless 4.15.2**, **deltalake 1.2.1**, **PyArrow 23.0.1**,
and **SQLGlot 30.24.0**. Sparkless 4.15.2 has a documented SQL `COUNT` output type
difference from DataFrame count; see [upstream issue #1723](https://github.com/eddiethedean/robin-sparkless/issues/1723).
Cluster access modes do not enforce production security policies. BrickHouse
provides local trusted-code execution; distributed Spark, Databricks Runtime, and
hostile-code isolation are outside this release.

Inspect your installation and the declared capability surface with:

```sh
brickhouse --version
brickhouse doctor --workspace ./workspace
brickhouse capabilities --format json
```

The [frozen 0.1.0 candidate](https://github.com/eddiethedean/brickhouse/blob/main/artifacts/releases/0.1.0/README.md)
passed **1,368 installed-wheel offline cases** and **1,832 full-source cases**
across the platform matrix, with **zero required skips**. Its bundle retains exact
artifact hashes, interpreter and dependency versions, and CI reports.

## Development

From the checkout and an activated virtual environment:

```sh
python -m pip install --require-hashes -r requirements/dev.txt
python scripts/validate_planning.py
python -m ruff check src tests scripts
python -m ruff format --check src tests scripts
python -m mypy
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest
```

Opt-in external-client cases skip in an unprovisioned developer environment. They
are mandatory in CI, which provisions exact client versions and qualifies one
built candidate across the full platform matrix. The offline gates enforce
loopback-only networking and fail on required skips. See the
[release readiness guide](https://github.com/eddiethedean/brickhouse/blob/main/docs/plan/21-release-readiness.md)
and [qualification workflow](https://github.com/eddiethedean/brickhouse/blob/main/.github/workflows/qualify.yml)
for the complete setup; Linux requires a non-root runner with passwordless `sudo`,
`unshare`, and `ip`, and macOS uses `sandbox-exec`.

Ordinary tests and required CI use local sanitized fixtures. They never load the
checkout's `.env` or discover real-workspace credentials. Occasional personal
Databricks reference captures are separate and bounded by
[document 15](https://github.com/eddiethedean/brickhouse/blob/main/docs/plan/15-personal-databricks-verification.md).

## Documentation

- **Try a workflow:** [orders example](https://github.com/eddiethedean/brickhouse/blob/main/examples/phase_0_1/README.md).
- **Check current behavior:** [project status](https://github.com/eddiethedean/brickhouse/blob/main/STATUS.md)
  and [capability registry](https://github.com/eddiethedean/brickhouse/blob/main/planning/capabilities.json).
- **Understand the design:** [architecture](https://github.com/eddiethedean/brickhouse/blob/main/docs/plan/01-architecture.md)
  and [serverless/classic compute contracts](https://github.com/eddiethedean/brickhouse/blob/main/docs/plan/17-serverless-and-classic-compute.md).
- **Follow future work:** [release roadmap](https://github.com/eddiethedean/brickhouse/blob/main/ROADMAP.md)
  and [task ledger](https://github.com/eddiethedean/brickhouse/blob/main/planning/tasks.json).
- **Review the release:** [changelog](https://github.com/eddiethedean/brickhouse/blob/main/CHANGELOG.md)
  and [0.1.0 evidence bundle](https://github.com/eddiethedean/brickhouse/blob/main/artifacts/releases/0.1.0/README.md).
- **Report a problem:** [GitHub issues](https://github.com/eddiethedean/brickhouse/issues).

## License

Apache-2.0. See [LICENSE](https://github.com/eddiethedean/brickhouse/blob/main/LICENSE)
and [third-party notices](https://github.com/eddiethedean/brickhouse/blob/main/THIRD_PARTY_NOTICES.md).
