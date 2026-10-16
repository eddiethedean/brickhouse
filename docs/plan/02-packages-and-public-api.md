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

## Extras and dependencies

| Install target | Intended contents |
|---|---|
| `brickhouse` | Local server and core workflows; Sparkless, native Delta binding as selected in M0, SQL parser/binder dependencies, SQLite persistence |
| `brickhouse[notebooks]` | nbformat, nbclient, ipykernel; Papermill only if parameterization needs it |
| `brickhouse[ml]` | MLflow server and Python-model support |
| `brickhouse[dbt]` | dbt-core/dbt-duckdb tested combination |
| `brickhouse[vector]` | Selected embedded vector provider, initially evaluate LanceDB |
| `brickhouse[streaming]` | Future selected streaming provider; do not publish a nonfunctional extra |
| `brickhouse[testing]` | pytest support for external users; test oracle clients remain developer tooling |
| `brickhouse[cloud-test]` | Optional cloud-storage test tooling; external services documented separately |

The Databricks SDK is a required development/conformance client; make it a runtime dependency only where the chosen adapter actually uses it. Do not install full PySpark or delta-spark in the base dependency graph. A pure Python Spark Connect client may be an optional conformance dependency once a native Connect server exists.

Potential later distribution names are **`brickhouse-connect`** (large gRPC protocol dependency boundary) and **`brickhouse-streaming`** (independent runtime/provider). Split only when there is an independent installation use case, a stable plugin protocol, separate release needs, and conformance CI for version skew. Do not create them in M0.

## Proposed CLI

These are design targets, not runnable commands in this pack:

```bash
brickhouse init --workspace ./workspace
brickhouse serve --workspace ./workspace --host 127.0.0.1 --port 8000
brickhouse run ./jobs/transform.py --profile spark35-local-v1
brickhouse capabilities --format json
brickhouse doctor
```

`serve` opens a workspace created by `init` or creates one explicitly under a documented policy. `run` uses the same job engine as API submission, not a second execution implementation. `doctor` reports package versions, writable paths, native wheel availability, capability profiles, and accidental Java/Spark dependencies. A help/version command must not start optional services.

## Proposed Python interface

```python
from brickhouse import Workspace

with Workspace.local(directory="./workspace", profile="spark35-local-v1") as bh:
    with bh.session() as spark:
        df = spark.createDataFrame([{"id": 1, "name": "Alice"}])
        df.write.mode("overwrite").saveAsTable("demo.default.people")
        rows = spark.table("demo.default.people").collect()
```

`Workspace.local` manages the service lifecycle, while `session()` returns a Sparkless session with BrickHouse catalog/writer hooks. Tests must explicitly create `demo.default` or use the documented bootstrap namespace; this example assumes it exists. The SDK uses the server address and local development token:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(host="http://127.0.0.1:8000", token="brickhouse-local", auth_type="pat")
```

The token is initially an API compatibility value, not proof of multi-user security. Worker credentials are separate and actually scoped.

## Compatibility imports

Three independent modes:

1. **Explicit:** users import Sparkless and request BrickHouse integration. Lowest surprise; first implementation target.
2. **Managed worker:** the BrickHouse launcher enables a capability-limited `pyspark`/`delta` import facade only inside a fresh worker. Reject installation conflicts and unsupported namespaces clearly.
3. **Remote client:** later, an unmodified pure Python Spark Connect client connects to a native BrickHouse gRPC service.

A Python import facade is not a Spark Connect server. Importing `brickhouse` in an arbitrary host process must not monkeypatch global modules or attach to a remote workspace.

## Plugins

Define `brickhouse.providers` entry points only after two real providers establish a useful stable interface. Require provider API version, capability declarations, lifecycle methods, and health status. Optional integrations load lazily and fail with an installation hint if absent. A base installation must run with no imports from MLflow, dbt, LanceDB, or streaming packages.
