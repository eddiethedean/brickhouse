# 22 — Local emulator comparison and benchmarks

B067 adds a dated, source-pinned feature comparison and an explicitly invoked
local benchmark. It introduces no runtime provider or conformance claim.

## Comparison scope

Compare BrickHouse with MiniLake and calvinchengx/databricks-emulator, the two
identified local Databricks workspace API emulators. Record source revisions,
licenses, primary-source links, installed dependency versions and executable
hashes. Mark future BrickHouse features **🗓 Planned**, selected implemented
subsets **◐ Partial**, and infrastructure metadata **⚙ Simulated**. Upstream
feature descriptions are documentation/source observations, not independently
qualified whole-product parity.

## Bounded measurement contract

- Run native host API processes sequentially on one machine in rotating order.
  Use the same Python version for Python servers and separate dependency environments.
- Measure two complete runs of five fresh-process/fresh-workspace trials per product. Startup runs from
  initialization/process launch to a verified 64 KiB workspace import/export.
  Exclude package installation, builds and fixture provisioning from timed samples.
- In each trial, discard two warmup file round trips, then measure 25 serial 64 KiB
  import/export round trips with exact decoded-byte verification. Vary the content
  to detect stale reads. Record every sample, median and nearest-rank p95.
- Sample core API server RSS after the initial successful round trip and before
  SQL work; identify exclusions such as optional kernels or external engines.
- For compatible provisioned SQL engines, register the same immutable 10,000-row
  Delta fixture through the declared untimed product setup interface, verify a grouped filtered aggregate, discard
  two warmups and record 25 serial Statement Execution round trips per trial.
  Retain exact expected rows and validate all returned rows. Report engine and
  format differences; unsupported/unconfigured cases have no numeric timings.
- Keep errors and failed trials visible. Validate persistence after graceful
  restart for the measured workspace file. Report different durability models.
- Use synthetic data, explicit loopback endpoints and temporary state. Strip
  inherited cloud credentials and avoid local profile/.env discovery. Clean up
  every owned process and temporary workspace. Benchmark setup may download
  pinned local dependencies; no personal Databricks account or cloud workload is used.

## Evidence and interpretation

Pool both full runs after tool typing fixes; retain each exact harness snapshot/hash and every measured sample. Retain configuration, provenance, raw samples, aggregate statistics, correctness
checks, setup limitations and the harness hash. README results must be generated
from the retained data and link to reproduction instructions. These measurements
represent this host and these small workflows; they do not rank distributed
engines, prove Spark semantic parity, measure cloud performance, or qualify a new
BrickHouse release artifact. Ordinary tests and required CI do not download or
run competitor servers.

The native Go SQL profile attaches fresh native Sail and pinned statement-agent processes. Its direct Delta path and JSON text envelope are explicitly adapted for the same computation and exact expected-row verification. The benchmark-only loopback handler adaptation decision is recorded in [the methodology](../benchmarks/local-emulators.md); it adds no BrickHouse runtime dependency.


## B068 — Attribute BrickHouse SQL latency

Profile the same real 10,000-row Delta aggregate and HTTP request with separate
plain, nested-span and cProfile trials in rotating order. Retain every raw
sample/span, exclude warmups from detailed profiles, quantify observed overhead,
verify exact results and fixture integrity, and retain installed source/tool
hashes. Partition concurrent HTTP/worker wall intervals once; do not add blocking
waits or cumulative profile times. Ordinary tests consume sanitized evidence only.
The [completed profile](../benchmarks/sql-profile.md) records SQLite metadata and
connection work as the main measured cost; optimization requires separate changes
and behavioral verification. It changes no runtime provider or capability state.
