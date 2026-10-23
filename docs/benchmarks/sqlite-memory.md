# SQLite disk and memory SQL benchmark

Measured **2026-10-22** on Apple M4 Pro (12 logical CPUs, 24 GiB), macOS 26.5.2
arm64, CPython 3.11.15, SQLite **3.50.4**. Runtime pins remain BrickHouse 0.1.0,
Sparkless 4.15.2, deltalake 1.2.1, PyArrow 23.0.1 and SQLGlot 30.24.0.
The installed runtime Python sources match the checkout recorded in provenance.

This explicitly invoked experiment reruns the [comparison's](local-emulators.md)
filtered/grouped aggregate over the same real, immutable 10,000-row Delta table.
It changes **SQLite workspace metadata storage only**; Delta/Parquet and workspace
files remain on disk. It measures SQL latency, not file/startup/job benchmarks.
The production runtime and original competitor comparison remain unchanged.

## Uninstrumented results

| Metadata configuration | Median ms | p95 ms | Speedup vs disk |
| --- | ---: | ---: | ---: |
| disk | 27.15 | 30.87 | 1.00× |
| disk-shared | 11.64 | 13.95 | 2.33× |
| memory | 9.34 | 10.17 | 2.91× |

Each row pools **150 timed requests**: three fresh workspace/API trials, five
excluded warmups and 50 serial timed queries per trial. Modes rotate across
rounds. The same installed interpreter/engine and physical Delta fixture are used
for every configuration. Timing includes a new loopback HTTP connection, the
Statement Execution request (`wait_timeout=30s`, inline JSON), parsing and exact
verification of all 100 aggregate result rows. No competitor was rerun in this
storage experiment, so these figures are kept separate from the original table.

Memory reduces the median from 27.15 ms to 9.34 ms: **2.91× faster**, about **65.6%
lower latency**. The disk/shared-cache control reaches 11.64 ms (**2.33× faster**)
while retaining disk-backed metadata. Memory is another **19.8% lower latency**
than that control. Much of the observed gain therefore accompanies keeping the
shared schema/page cache alive, rather than requiring volatile metadata. This is
an inference from the controls, not a precise CPU/I/O decomposition.

## What the configurations do

| Configuration | SQLite target | Cache lifetime | Metadata after API exit |
| --- | --- | --- | --- |
| `disk` | Original workspace metadata file | Existing repository policy; short-lived private connections | Committed statements remain on disk |
| `disk-shared` | Same file opened with `cache=shared` | One idle keeper preserves shared cache; repositories still open/close their own handles | Committed statements remain on disk |
| `memory` | Unique `file:<unique-name>?mode=memory&cache=shared` URI | One idle keeper preserves a named RAM database | RAM state disappears; original disk seed is unchanged |

Memory mode copies the complete initialized workspace metadata into RAM via
SQLite's public backup API before API acquisition. Only connections targeting
that exact synthetic metadata path are redirected, including URI read-only
validation handles. Read-only handles retain `query_only`; connection factories,
thread ownership and transaction boundaries are preserved. No transaction,
foreign-key, result-limit, admission, cancellation or retention check is removed.
There is no snapshot/dump back to disk at shutdown. Backup/initialization and
lifetime probes are outside timed requests. Redirector/connection handling costs
are included where applicable.

A plain `:memory:` connection would create a separate database for each handle.
SQLite documents [named in-memory databases and their lifetime](https://www.sqlite.org/inmemorydb.html)
and [shared-cache locking semantics](https://www.sqlite.org/sharedcache.html).
Shared-cache mode has different locking behavior; it is used here as an
experiment/control, not selected as a production architecture. No global SQLite
shared-cache setting or dirty-read mode is enabled.

## Verification and timing attribution

**All 990 queries passed**: 900 timed queries plus 90 warmups across 18 fresh
trials (three configurations × baseline/span capture × three rounds). Every
process exited successfully, and all temporary state was removed. All native
Delta fixture files remained byte-identical. No `.env`, personal account, cloud
compute, JVM or Docker workload was used; child environments strip credentials.

For each API process, two independent post-query threads open fresh connections
and observe all 55 statements as `SUCCEEDED`. Both disk modes retain those 55
statements on disk after API exit. Every memory trial leaves zero statements in
the original seed, preserves its exact SHA-256, reports an empty main-database
filename and `journal_mode=memory`, and observes zero schema objects after its
last connection closes. Disk controls report `journal_mode=delete`; all modes
retain `synchronous=2` and `read_uncommitted=0`.

Separate span trials use the [existing profiler's](sql-profile.md) nested,
non-overlapping wall accounting. Every raw interval and client sample is retained.
Means below are instrumented and are not added to uninstrumented medians:

| Span-instrumented mean | Disk | Disk/shared cache | Memory |
| --- | ---: | ---: | ---: |
| Complete request | 29.79 ms | 12.07 ms | 9.11 ms |
| SQLite calls | 21.04 ms | 4.28 ms | 1.70 ms |
| SQLite commit calls (already included above) | 2.06 ms | 1.68 ms | 0.01 ms |
| Observed mean difference from baseline | +7.2% | −0.4% | −3.4% |

Each mode still makes **34 connection opens, 34 closes, 114 executes, one
executemany and 10 commits per request**. The control does not pool/reuse one
application connection for the query. Keeping SQLite's cache alive accompanies
much lower metadata execution/open/close times; RAM also removes disk journal
commit work for metadata.

The small negative span-versus-baseline differences are retained, not interpreted
as a speed benefit from instrumentation. Trials run separately on an interactive
host with warm filesystem caches, no CPU affinity and possible frequency/activity
variation. Wrapper/factory differences also apply. Baselines determine the
published speedups; spans identify large cost differences. This experiment does
not isolate fsync duration, profile native engine internals, measure cold/large
or concurrent workloads, or qualify restart/job/snapshot semantics for memory.
Memory metadata is currently **benchmark-only**, with no supported CLI setting.

## Reproduce

From the repository root, use the installed benchmark interpreter prepared in the
[comparison recipe](local-emulators.md):

```sh
/tmp/brickhouse-bench-brickhouse-env-20261022/bin/python -m scripts.benchmark_sqlite \
  --output /tmp/brickhouse-sqlite-memory-new \
  --rounds 3 --iterations 50 --warmups 5
python -m scripts.summarize_sqlite_benchmark /tmp/brickhouse-sqlite-memory-new
```

Output must be a new directory. `scripts/benchmark_sqlite.py` is opt-in; ordinary
CI only consumes sanitized evidence and verifies the SQLite routing/lifetime
helpers against real local SQLite files. It never starts the benchmark servers.

[Retained evidence](../../artifacts/benchmarks/2026-10-22-sqlite-memory/README.md)
contains every final trial/sample/span, per-process storage proofs, exact source
and tool hashes, package inventory, SQLite build/version and validation records.
The initial six-configuration smoke check (24 verified queries) is excluded from
all published statistics; no final measured trial was discarded.
