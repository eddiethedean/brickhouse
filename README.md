# BrickHouse

BrickHouse runs selected Databricks workflows locally without a JVM. Sparkless
executes DataFrames and Spark SQL; delta-rs stores real Delta tables. SQL,
DataFrames, Python jobs, and optional notebooks share one persistent catalog
and table service.

Version **0.1.0** is an alpha with a deliberately bounded compatibility surface.
It targets local development and test design. It does not provide Databricks
Runtime, distributed Spark, cloud resource allocation, or a security sandbox.

## Install

Use Python 3.11 or 3.12. The release qualification matrix covers Ubuntu 22.04 x86_64
and macOS 26 arm64. Other OS versions and Windows are outside the 0.1.0
qualification scope.

From this checkout:

```sh
python -m venv .venv
source .venv/bin/activate
python -m pip install .
# Optional notebook execution:
python -m pip install '.[notebooks]'
```

The base distribution pins Sparkless 4.15.2, deltalake 1.2.1, PyArrow 23.0.1,
and SQLGlot 30.24.0. Notebook execution adds the pinned Jupyter runtime.
No Java, Spark installation, Docker, or Databricks account is required.

## Run a local workspace

```sh
brickhouse init --workspace ./workspace
brickhouse serve --workspace ./workspace --host 127.0.0.1 --port 8000
```

The service binds to IPv4 loopback only. Its SDK token is a local compatibility
value, not authentication. Use a disposable workspace and trusted code: workers
run as the local user and can access that user's files and processes.

In another terminal, install the qualified SDK and run the included example:

```sh
python -m pip install databricks-sdk==0.153.0
python examples/phase_0_1/run_orders.py --host http://127.0.0.1:8000
```

The [orders example](https://github.com/eddiethedean/brickhouse/blob/main/examples/phase_0_1/README.md) creates a managed Delta table,
submits a Python job, and reads committed data through SQL. It prints
`[["alice", "2", "15.75"]]` and leaves its local resources for inspection.
Stop the service with Ctrl-C. Existing workspace metadata and committed tables
remain available after restart.

```sh
brickhouse --version
brickhouse doctor --workspace ./workspace
brickhouse capabilities --format json
```

Python jobs are submitted through the SDK Jobs API. The `brickhouse run`
command is reserved and currently raises an explicit unavailable-feature error.

## Included behavior

- Persistent local workspace, catalog, schema, and table identity.
- Native Delta create/read/append/overwrite, selected SQL UPDATE/DELETE/MERGE,
  history and snapshot reads, with durable operation reconciliation.
- Isolated Sparkless sessions and fresh child processes for Python jobs.
- Bounded DAG dependencies, retries, cancellation, timeouts, output retention,
  widgets, scoped secrets, and Python job task values.
- Seven selected serverless/classic compute variants for Python and ipynb jobs,
  plus source notebook execution in three profile families.
- Simulated cluster and serverless/Pro/classic SQL warehouse lifecycles, with
  actual local job execution and bounded statement admission.
- Selected SDK, CLI, bundle, Terraform, and SQL connector workflows.
- Coordinated local snapshot/restore, managed-table ownership checks, and
  explicit handling of external table registrations.

Compatibility is versioned and partial. The [capability registry](https://github.com/eddiethedean/brickhouse/blob/main/planning/capabilities.json)
records exact claims and evidence. Unsupported operations raise explicit errors.
Worker import compatibility stays inside child processes; installed `pyspark`,
`databricks`, and `delta` packages are never replaced globally. Cluster access
modes do not enforce production security policies. SQL connector Thrift,
Arrow-stream/LZ4 results, broad metadata methods, and remote authentication
are outside the qualified subset. DuckDB is not a fallback executor.

## Development and qualification

```sh
python -m pip install --require-hashes -r requirements/dev.txt
python scripts/validate_planning.py
python -m ruff check src tests scripts
python -m ruff format --check src tests scripts
python -m mypy
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest
```

The complete release checks provision exact CLI/Terraform/connector versions,
then qualify the built wheel in separate base and notebook environments:

```sh
python -m build --no-isolation
python scripts/audit_distribution.py --artifact-dir dist
python scripts/provision_release_clients.py --destination /tmp/brickhouse-clients --output /tmp/brickhouse-clients.json
python scripts/qualify_release.py --clients /tmp/brickhouse-clients.json --output-dir /tmp/brickhouse-qualification --full-source
```

Linux qualification requires a non-root runner with passwordless `sudo`,
`unshare`, and `ip`; macOS uses `sandbox-exec`. G1–G4 execute with OS-enforced
loopback-only networking. Required skips fail qualification. Ordinary tests
never load the checkout's `.env` or real workspace credentials. Opt-in external-client cases
skip in an unprovisioned developer environment; they are mandatory in CI.

The personal Databricks account is used only for occasional bounded reference
captures under [document 15](https://github.com/eddiethedean/brickhouse/blob/main/docs/plan/15-personal-databricks-verification.md).
Routine tests and required CI run locally with sanitized fixtures.

See [release readiness](https://github.com/eddiethedean/brickhouse/blob/main/docs/plan/21-release-readiness.md), the
[0.1.0 review bundle](https://github.com/eddiethedean/brickhouse/blob/main/artifacts/releases/0.1.0/README.md),
[STATUS](https://github.com/eddiethedean/brickhouse/blob/main/STATUS.md), and the future [ROADMAP](https://github.com/eddiethedean/brickhouse/blob/main/ROADMAP.md).
The tag workflow runs checks before a separate PyPI Trusted Publishing job;
preparing a candidate does not publish it.

## License

Apache-2.0. See [LICENSE](https://github.com/eddiethedean/brickhouse/blob/main/LICENSE) and [third-party notices](https://github.com/eddiethedean/brickhouse/blob/main/THIRD_PARTY_NOTICES.md).
