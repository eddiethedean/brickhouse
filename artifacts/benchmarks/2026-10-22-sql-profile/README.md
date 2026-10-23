# 2026-10-22 BrickHouse SQL profiling evidence

See [method, findings and reproduction](../../../docs/benchmarks/sql-profile.md).

- `results.json`: every sample, all nine trials, correctness/cleanup checks,
  exact installed source hashes, versions, host and capture-tool identities.
- `*-spans.jsonl.gz`: complete nested wall spans for every warmup/timed request.
- `*-cprofile.jsonl.gz`: HTTP/claimed-worker envelope spans for every request.
- `*-cprofile.worker.pstats`, `*-cprofile.http.pstats`: unrounded detailed
  profiles of timed queries only. These are trusted local Python pstats artifacts;
  read with the recorded Python version. Their cumulative/wait times overlap.
- `analysis.json`, `breakdown.md`: recomputed accounting, SQL-template/call counts,
  detailed function times and observed overhead. Analyzer identity is retained.
- `provenance.json`: runtime/source identity checks, installed package inventory,
  benchmark reference and explicit excluded setup-probe limitations.
- `validation.json`: focused/static/planning/evidence checks for this slice.
- `manifest.json`: hashes for every evidence file except itself and the two tools.

No engine/runtime code or capability state changed. This is a small local SQL
performance profile; release conformance stays tied to its frozen artifacts.
