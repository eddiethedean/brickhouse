# Local Databricks emulator comparison

Measured on **2026-10-22** for B067. This is an opt-in local performance study,
separate from BrickHouse release conformance and ordinary CI.

## Products and source identity

| Product | Measured source/profile |
| --- | --- |
| BrickHouse | 0.1.0 at `dbb2eb94424897af48c3a19055a59a8391152c7f`; built wheel, base dependencies, Sparkless 4.15.2, deltalake 1.2.1, PyArrow 23.0.1. |
| MiniLake | 1.8.0 source snapshot `b2736501ed13c6b069204a202a9e6142a5b7e54c`; DuckDB 1.5.6, PyArrow 26.0.0; persistence enabled, optional notebook runtime off, Spark image prewarm off, Delta extension installed before measurement. |
| calvinchengx/databricks-emulator | Source snapshot `05044929caf78a7f2155d8de218d3e3e4c53a173`, built natively with Go 1.27.2. Core API uses its seeded PAT with TLS disabled for this loopback-only comparison. SQL attaches native Sail 0.7.1, pyspark-client 4.2.0, and the pinned external statement agent described below. No UC OSS sidecar. |

The two alternatives implement local Databricks workspace APIs. Standalone data
engines such as Sparkless, Sail, DuckDB and Polars serve a different layer; Sail
is measured here as the Go emulator's SQL backend. Source snapshot labels do not
mean those commits are separately published package releases.

The [README feature matrix](../../README.md#feature-comparison) is based on pinned
primary sources. Its **🗓 Planned** BrickHouse cells are future roadmap work;
**◐ Partial** and **⚙ Simulated** retain the current capability boundaries.
Upstream feature descriptions are not a complete independent parity audit.

## Hardware and timing method

- Apple M4 Pro, 12 logical CPUs, 24 GiB memory; macOS 26.5.2 arm64.
- CPython 3.11.15 for the controller and all Python services, including the
  native Sail client/agent. Each product has an isolated dependency environment.
- Two complete runs of five trials per product: **30 trials**, **250 timed file
  round trips** and **250 timed SQL statements per product**. Each trial discards
  two warmups for each warm workload, then records 25 serial samples.
- Product order rotates between trials. Only one product workload runs at a time.
  Core API processes are native host processes. Each Go SQL trial additionally
  gets fresh native Sail and statement-agent processes; no Docker or JVM executes
  any measured workload.
- Client timing uses `time.perf_counter()`. Requests use explicit IPv4 loopback
  HTTP with `Connection: close`, no proxy and no connection pooling. End-to-end
  samples include serialization, response parsing and result verification.
- Workspace state and the shared fixture live in temporary directories. Packages
  and dependencies are already installed; filesystem caches are warm and are not
  flushed. This is an interactive desktop, without CPU affinity or a guarantee
  against unrelated OS activity.

Both full runs are retained and pooled. The second followed static typing fixes
in the measurement tools; workload logic, data and product configurations stayed
the same. No complete run was discarded to select a better outcome. Each raw
report records the exact harness and launcher hashes; the initial versions are
archived next to its raw report. The final versions are in `scripts/`.

| Workload | BrickHouse | MiniLake | Go emulator |
| --- | --- | --- | --- |
| Fresh workspace → first verified 64 KiB file | 297.10 / 339.35 ms | 370.35 / 583.05 ms | 58.49 / 83.58 ms |
| Warm 64 KiB file import + export | 2.74 / 4.94 ms | 2.73 / 4.99 ms | 3.31 / 5.27 ms |
| Warm filtered/grouped SQL over 10,000 Delta rows | 29.95 / 33.19 ms | 2.59 / 3.17 ms | 4.86 / 5.53 ms |
| Idle core API server RSS (median only) | 44.98 MiB | 98.19 MiB | 15.26 MiB |

Latency cells are **median / nearest-rank p95**, in milliseconds. RSS is the
median of ten core-process observations, in MiB. With ten startup observations,
the nearest-rank p95 is the largest observed startup; it is not a precise tail
estimate. Pooled raw samples, minima/maxima and all per-trial values are retained.

## What each workload proves

**Fresh workspace to first verified file.** Time starts before initialization
and process launch and ends after a 64 KiB source script has been imported,
exported and checked byte for byte. BrickHouse's separate `init` command is
included; the other products initialize their state as part of server launch.
This is core API readiness, not readiness of every optional execution backend.

**Warm script import/export.** Each sample writes new deterministic 64 KiB
content using `format=SOURCE`, `language=PYTHON`, and overwrite, then exports with
`format=AUTO` and verifies the exact decoded bytes. Different products can classify
the script as a file or notebook; object-type parity is not asserted. After each
trial, the API process is restarted and the latest content is checked again.
This verifies the measured file's restart persistence, not crash consistency or
persistence of all other resource types.

**Warm Delta aggregate.** The controller writes one actual native Delta table
per run: 10,000 rows, unpartitioned, with BIGINT `group_key`, BIGINT `amount`, and
BOOLEAN `active`. The same immutable files are read by all products in that run.
For row index `i`, values are `i % 100`, `i % 1000`, and `i % 3 != 0`.

```sql
SELECT group_key, COUNT(*) AS n, SUM(amount) AS total
FROM bench.default.emulator_bench
WHERE active = true
GROUP BY group_key
ORDER BY group_key
```

The expected 100 rows are calculated independently in Python. Every returned
value and row order is checked, and fixture file hashes must remain unchanged.
BrickHouse uses its embedded external-table registration interface while its
server is stopped; MiniLake registers via SQL `LOCATION`. The Go/Sail query uses
`delta.` followed by the same quoted absolute fixture path, avoiding an
unprovisioned UC OSS sidecar. Registration and backend startup are untimed. Go's
JSON text result is decoded explicitly; the other profiles return `data_array`.
Integer values are normalized from integers or integer strings. Schema/protocol
parity is outside this measurement, including the documented Sparkless SQL COUNT
schema difference. These are read queries, not a write/ETL or Spark job benchmark.

**Idle RSS.** `ps` samples the core API server after its first successful file
round trip and before SQL work. It excludes the controller, optional notebook
kernels, Go's Sail/agent processes and peak query memory. It must not be read as
a total-stack memory comparison. MiniLake may eagerly load components that other
servers defer; the number describes this startup profile.

MiniLake's DuckDB profile had the lowest query latency for this small cached
workload. The Go core API had the shortest startup. Different engines, durability
models, lazy loading and additional HTTP hops affect the results. These figures
do not rank general Spark performance, distributed processing or Databricks cloud
performance. BrickHouse's existing capability claims are unchanged.

## Evidence and setup findings

- [Pooled statistics](../../artifacts/benchmarks/2026-10-22-local-emulators/combined-results.json)
  and [generated table](../../artifacts/benchmarks/2026-10-22-local-emulators/results-table.md).
- [First complete run and measured tool snapshots](../../artifacts/benchmarks/2026-10-22-local-emulators/initial-measurement/)
  and [final complete run](../../artifacts/benchmarks/2026-10-22-local-emulators/measured/results.json).
- [Source, dependency and executable provenance](../../artifacts/benchmarks/2026-10-22-local-emulators/provenance.json)
  and [additional build/native-library identity](../../artifacts/benchmarks/2026-10-22-local-emulators/native-builds.json).
- [Setup probes](../../artifacts/benchmarks/2026-10-22-local-emulators/setup-probes.json)
  retain preflight results, excluded from the measured summary. SOURCE export is
  rejected by BrickHouse's file profile, so all measured exports use AUTO.
  MiniLake reserves catalog name `main`, so named registrations use `bench`.
  BrickHouse rejects SQL LOCATION registration; its embedded registration surface
  is used instead. A redundant schema create in the initial setup adapter was
  removed because BrickHouse creates a default schema with the catalog.
- [Artifact/code hash inventory](../../artifacts/benchmarks/2026-10-22-local-emulators/manifest.json).

All 30 measured trials passed. The harness checks 840 complete file round trips,
30 additional exports after restart, and 810 SQL result sets, including warmups.
Temporary workspaces and owned service/backend processes are cleaned up. No
personal Databricks account, `.env`, inherited cloud credentials, or cloud compute
is used. Dependency and extension provisioning occurs before timing. Ordinary
unit tests read the sanitized reports locally and never install or start a
competitor; pooling rejects incomplete, failed, mismatched or duplicate runs.

## Reproduce

Start from this repository root on macOS arm64. Install `uv` and **Go 1.27.2**
([official downloads](https://go.dev/dl/)); the measured Go archive hash is in
provenance. Python 3.11.15 must be available to `uv` (it can provision it during
setup). Setup needs network access; the measured workloads use loopback/local
files. Exact dependency snapshots are in the evidence directory.

```sh
benchmark_repo="$PWD"
comparison_root="$(mktemp -d "${TMPDIR:-/tmp}/brickhouse-comparison.XXXXXX")"
evidence="$benchmark_repo/artifacts/benchmarks/2026-10-22-local-emulators"

git clone https://github.com/eddiethedean/brickhouse.git "$comparison_root/brickhouse-src"
git -C "$comparison_root/brickhouse-src" checkout dbb2eb94424897af48c3a19055a59a8391152c7f
git clone https://github.com/dmux/minilake.git "$comparison_root/minilake-src"
git -C "$comparison_root/minilake-src" checkout b2736501ed13c6b069204a202a9e6142a5b7e54c
git clone https://github.com/calvinchengx/databricks-emulator.git "$comparison_root/go-src"
git -C "$comparison_root/go-src" checkout 05044929caf78a7f2155d8de218d3e3e4c53a173
git clone https://github.com/calvinchengx/fabric-emulator.git "$comparison_root/agent-src"
git -C "$comparison_root/agent-src" checkout 4a42eb4c5dc1cb3a63e5a5ececb47fb147905b3e

uv venv --python 3.11.15 "$comparison_root/brickhouse-env"
uv venv --python 3.11.15 "$comparison_root/minilake-env"
uv venv --python 3.11.15 "$comparison_root/sail-env"
uv pip install --python "$comparison_root/brickhouse-env/bin/python" -r "$evidence/brickhouse-dependencies.txt"
uv build --wheel --out-dir "$comparison_root/wheels" "$comparison_root/brickhouse-src"
uv pip install --python "$comparison_root/brickhouse-env/bin/python" --no-deps "$comparison_root/wheels/brickhouse-0.1.0-py3-none-any.whl"
uv pip install --python "$comparison_root/minilake-env/bin/python" -r "$evidence/minilake-dependencies.txt"
uv pip install --python "$comparison_root/minilake-env/bin/python" --no-deps "$comparison_root/minilake-src"
uv pip install --python "$comparison_root/sail-env/bin/python" -r "$evidence/sail-agent-dependencies.txt"
(cd "$comparison_root/go-src" && go build -o "$comparison_root/databricks-emulator" ./cmd/databricks-emulator)

BENCH_EXTENSION_DIR="$comparison_root/extensions" "$comparison_root/minilake-env/bin/python" - <<'PYEXT'
import os
from pathlib import Path
import duckdb
extension_dir = Path(os.environ["BENCH_EXTENSION_DIR"])
extension_dir.mkdir()
connection = duckdb.connect(config={"extension_directory": str(extension_dir)})
connection.execute("INSTALL delta")
connection.execute("LOAD delta")
PYEXT
```

Refresh the provenance template for the artifacts actually built on your machine.
Hashes identify the measured artifacts; compiler paths or build environments can
change a binary hash even when source and versions agree.

```sh
BENCH_ROOT="$comparison_root" BENCH_EVIDENCE="$evidence" python - <<'PYPROVENANCE'
import hashlib
import json
import os
from pathlib import Path
import subprocess
root = Path(os.environ["BENCH_ROOT"])
data = json.loads((Path(os.environ["BENCH_EVIDENCE"]) / "provenance.json").read_text())
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
def environment(name):
    code = ('import importlib.metadata as m,json,platform; '
            'print(json.dumps({"python":platform.python_version(),'
            '"packages":{d.metadata["Name"]:d.version for d in m.distributions()}}))')
    return json.loads(subprocess.check_output([str(root / name / "bin/python"), "-c", code], text=True))
data["brickhouse"]["environment"] = environment("brickhouse-env")
data["brickhouse"]["wheel_sha256"] = sha(root / "wheels/brickhouse-0.1.0-py3-none-any.whl")
data["minilake"]["environment"] = environment("minilake-env")
data["minilake"]["extension_files"] = [
    {"path": str(p.relative_to(root / "extensions")), "sha256": sha(p)}
    for p in sorted((root / "extensions").rglob("*")) if p.is_file()
]
data["databricks-emulator"]["binary_sha256"] = sha(root / "databricks-emulator")
data["databricks-emulator"]["native_sql_profile"]["environment"] = environment("sail-env")
(root / "provenance.json").write_text(json.dumps(data, indent=2) + "\n")
PYPROVENANCE

for run in 1 2; do
  "$comparison_root/brickhouse-env/bin/python" "$benchmark_repo/scripts/benchmark_emulators.py" \
    --brickhouse-bin "$comparison_root/brickhouse-env/bin/brickhouse" \
    --minilake-bin "$comparison_root/minilake-env/bin/minilake" \
    --databricks-emulator-bin "$comparison_root/databricks-emulator" \
    --minilake-extension-dir "$comparison_root/extensions" \
    --sail-bin "$comparison_root/sail-env/bin/sail" \
    --sail-python "$comparison_root/sail-env/bin/python" \
    --spark-agent-source "$comparison_root/agent-src/python/spark_agent/agent.py" \
    --provenance "$comparison_root/provenance.json" \
    --output-dir "$comparison_root/results/run-$run" \
    --rounds 5 --iterations 25 --warmups 2
done
"$comparison_root/brickhouse-env/bin/python" -m scripts.summarize_emulator_benchmarks \
  --run "$comparison_root/results/run-1/results.json" \
  --run "$comparison_root/results/run-2/results.json" \
  --output-dir "$comparison_root/results"
```

Keep the output directory to inspect raw samples and the generated table. The
harness removes only its own temporary state and processes; the explicit setup
environments/checkouts remain under `comparison_root` for inspection.

## Explicit benchmark-only agent adaptation decision

For the Go emulator's native SQL profile, use Sail 0.7.1 with pyspark-client 4.2.0
and the external statement agent from fabric-emulator revision
`4a42eb4c5dc1cb3a63e5a5ececb47fb147905b3e`.
[The pinned implementation](https://github.com/calvinchengx/fabric-emulator/blob/4a42eb4c5dc1cb3a63e5a5ececb47fb147905b3e/python/spark_agent/agent.py)
normally binds all interfaces. The benchmark launcher imports its `Handler`, sets
its local agent URL, and binds the standard HTTP server to `127.0.0.1` on a fresh
port. This is an explicit contained use of the pinned external handler interface
for measurement. This native SQL profile does not qualify the published container or its optional integrations. It changes the listener binding only; upstream Spark session,
SQL execution and result-envelope code are retained. No external agent source is
copied into BrickHouse, and the launcher is an opt-in tool, separate from runtime
providers and ordinary CI. Real native Sail/agent/Go HTTP queries must return the
entire deterministic expected result before timings qualify. Each SQL trial gets
fresh engine and agent processes, which are cleaned up with its API process.

External table setup is untimed: BrickHouse uses its embedded registration API,
MiniLake uses SQL LOCATION registration, and the Go/Sail profile reads the same
Delta path directly without a UC OSS sidecar. All execute the same filter/group/
COUNT/SUM/order computation over identical files. Go's agent JSON text envelope
is normalized explicitly; this does not establish standard result-protocol parity.


## Follow-up SQL profiling

[B068's SQL profile](sql-profile.md) measures the BrickHouse side of the same
workload in three separate modes, retaining an uninstrumented baseline, nested
wall spans and detailed cProfile output. SQLite calls account for 71% of the
instrumented request, with repeated namespace/admission/result-retention work
and per-operation connection management. Sparkless SQL/Arrow boundaries account
for 17%; native internals remain unsplit. The original comparison above is
unchanged; the follow-up quantifies profiling overhead and applies no optimization.
