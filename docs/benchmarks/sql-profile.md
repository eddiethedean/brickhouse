# BrickHouse SQL request profile

Measured **2026-10-22** on the same Apple M4 Pro / macOS 26.5.2 arm64 /
CPython 3.11.15 host as the [emulator comparison](local-emulators.md).
This profiles the same filtered/grouped query over a real 10,000-row Delta table,
through the same loopback Statement Execution request (`wait_timeout=30s`, inline
JSON, new HTTP connection). It changes no BrickHouse runtime behavior or capability claim.

## Measurements

Three fresh API/workspace trials per mode, rotating mode order: **150 timed
queries per mode**, five excluded warmups per trial. All **495** timed/warmup
queries returned every one of the 100 independently expected aggregate rows.
The fixture's native files remained byte-identical. All nine API processes exited
successfully and temporary state was removed. Cloud credentials were stripped;
no `.env`, personal Databricks workspace, JVM, Docker or competitor process was used.

| Capture mode | Median ms | p95 ms | Mean ms |
| --- | ---: | ---: | ---: |
| Uninstrumented | 27.85 | 30.32 | 28.03 |
| Lightweight wall spans | 29.11 | 32.41 | 29.60 |
| Detailed cProfile | 34.95 | 39.66 | 35.53 |

The spans show an observed **4.5% median / 5.6% mean** increase over the separate
baseline trials; cProfile shows **25.5% median / 26.8% mean**. These differences
include run-order/host variation as well as instrumentation cost. The original
comparison's pooled 29.95 ms median is a different measurement; it is not replaced
or presented as a performance regression/improvement. No optimization was applied.

## Wall-time breakdown

These are **means from the 150 span-instrumented requests**. Means add to the
instrumented request total; per-component medians would not. SQLite time is
attributed to its own category even when called inside catalog/session/engine
hooks. All other categories exclude these nested calls.

| Stage | Mean ms/request | Share |
| --- | ---: | ---: |
| SQLite calls | 21.09 | 71.2% |
| Sparkless SQL boundary | 3.38 | 11.4% |
| Sparkless Arrow boundary | 1.60 | 5.4% |
| SQL planning, catalog and Delta metadata (outside SQLite) | 1.42 | 4.8% |
| Result limits and JSON | 0.72 | 2.4% |
| Session open and close (outside SQLite) | 0.07 | 0.2% |
| Other lifecycle, HTTP and client residual | 1.34 | 4.5% |
| **Total, instrumented** | **29.60** | **100%** |

SQLite's 21.09 ms comprises 12.90 ms in `execute`, 0.03 ms in `executemany`,
3.91 ms opening connections, 2.34 ms closing them, and 1.91 ms committing.
Each request invokes **34 connects, 34 closes, 114 executes, one executemany and
10 commits**. These are API call counts, including reads, PRAGMAs and transaction
control, rather than 114 distinct business queries or 10 data-table mutations.

The largest repeated SQL templates are:

| Metadata query | Calls/request | Mean ms/request |
| --- | ---: | ---: |
| List catalogs | 7 | 2.45 |
| List schemas in a catalog | 7 | 2.43 |
| Read admission lease by statement ID | 8 | 1.82 |
| Find expired retained results | 5 | 1.78 |
| Read warehouse by ID | 5 | 1.45 |

These times are already included in the SQLite total. The identifier resolver
currently lists catalogs/schemas to perform case-insensitive matching; the
request, execution and catalog-provider paths repeat resolution. Repository
operations open and close SQLite connections, while statement/lease/result
lifecycle operations add separate transactions. Result-retention work also runs
on statement repository access. The profile measures this particular state size
and workload, not growth behavior with thousands of retained statements/tables.

The original hypothesis that native session construction was expensive is **not
supported by this capture**: setup/teardown outside SQLite takes about 0.07 ms.
Some catalog work occurs during setup and is counted under SQLite. The measured
Sparkless SQL/Arrow boundaries together take 4.98 ms, including native planning,
Delta scanning, execution and materialization. Python-level hooks cannot split
those native Rust/C++ internals into CPU operators or prove native engine parity
with DuckDB/Sail. Small bounded-result checks and JSON encoding add about 0.72 ms.

## Optimization priorities suggested by the profile

1. Reuse SQLite connections within a request/worker or through a bounded,
   thread-safe ownership policy. Preserve foreign-key configuration, transaction
   boundaries, cancellation, concurrent requests and restart durability.
2. Reduce repeated namespace/admission reads using explicitly scoped resolved
   metadata. Case-insensitive ambiguity, fresh snapshots and invalidation must
   remain correct; unbounded cross-request metadata caching is not justified here.
3. Reduce redundant retention scans and transaction work without weakening result
   expiry, quotas, terminal admission ordering or recovery guarantees.
4. Re-measure after each change. This capture does not establish a safe speedup,
   justify removing durable state, or measure large datasets/concurrent load.

## Capture and accounting

`scripts/profile_sql.py` launches fresh installed-wheel API subprocesses with an
explicit clean environment and synthetic state. Baseline mode installs no hooks.
Span mode uses timer wrappers around selected BrickHouse boundaries and a
`sqlite3.Connection` subclass that delegates unchanged operations. Wrappers live
only in the profiler subprocess; no third-party source is copied or patched.

Raw spans retain `perf_counter_ns` intervals, thread/depth, category and method.
SQLite templates omit all bound parameters. The analyzer partitions the HTTP
handler wall interval at every span boundary: the deepest active hook wins on a
thread, and worker work takes precedence over HTTP-thread waiting. Each instant
is counted once. The separately measured client duration minus handler duration
is retained as the client/transport/verification residual. Every request partition
is checked against its measured duration. Instrumentation bookkeeping is not
subtracted or attributed as application-only time.

Detailed cProfile captures only the 50 timed queries per trial, excluding all
warmups/import initialization. Separate files cover the HTTP handler thread and
`_execute_claimed` on its worker. The continuation's work before/after that method
is covered by wall spans, not by the worker cProfile. HTTP `lock.acquire` time is
mostly waiting for that same worker; adding it to worker time would double count.
Cumulative times overlap; `analysis.json` retains unrounded self/cumulative times
and counts for interpretation, not an additive latency table.

The three development hook smoke probes are excluded: an initial setup failed
because a hook used the wrong planner class name; subsequent small probes checked
hook coverage, warmup exclusion and SQLite attribution before the fixed capture.
They do not qualify the final tool or contribute to any published statistics.
All nine final trials and every timed sample are retained.

## Reproduce

From the repository root, use the already prepared, installed benchmark interpreter
with Sparkless 4.15.2, deltalake 1.2.1, PyArrow 23.0.1 and SQLGlot 30.24.0. The driver
imports `scripts` from this checkout while product execution imports the installed
BrickHouse wheel. It never sets `PYTHONPATH` or reads `.env`.

```sh
/tmp/brickhouse-bench-brickhouse-env-20261022/bin/python -m scripts.profile_sql \
  --output /tmp/brickhouse-sql-profile-new \
  --rounds 3 --iterations 50 --warmups 5
python -m scripts.summarize_sql_profile /tmp/brickhouse-sql-profile-new
```

Output must be a new directory. Install/build preparation and exact source pins
are described in the [comparison recipe](local-emulators.md). These profiling
commands run only when explicitly invoked; ordinary tests consume sanitized
artifacts and never start benchmark servers or discover workspace credentials.

[Retained evidence](../../artifacts/benchmarks/2026-10-22-sql-profile/README.md)
contains raw per-request spans, all client samples, detailed profiles, computed
analysis, complete installed-source hashes and version/provenance records.


## Follow-up SQLite storage experiment

The [disk/cache/memory comparison](sqlite-memory.md) holds the SQL workload and
34-connect/114-execute/10-commit call counts constant. It measures an uninstrumented
27.15 ms disk median, 11.64 ms with a disk-backed shared cache and idle keeper, and
9.34 ms with named shared-memory metadata. The controls suggest cache lifetime
explains much of the gain, with a further reduction when metadata is volatile.
This is benchmark evidence and does not introduce a supported runtime mode.
