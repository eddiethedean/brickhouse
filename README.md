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
