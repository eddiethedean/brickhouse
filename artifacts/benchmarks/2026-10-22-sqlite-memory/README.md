# 2026-10-22 SQLite metadata storage experiment

See [findings, controls and reproduction](../../../docs/benchmarks/sqlite-memory.md).

- `results.json`: all 18 final trials, 900 timed/90 warmup verified queries,
  every raw latency, nested partitions, storage proofs and source/tool identities.
- `*.metadata.json`: per-process SQLite settings, disk seed hashes, two fresh
  thread observations and memory lifetime/disk-persistence checks.
- `*-spans.jsonl.gz`: all warmup/timed spans in the nine instrumented trials.
- `analysis.json`, `comparison.md`: recomputed validated results and raw call counts.
- `provenance.json`: installed package and SQLite build inventory, runtime identity,
  prior profile reference and explicitly excluded smoke check.
- `validation.json`: focused/static/planning/accounting verification.
- `manifest.json`: SHA-256 for all other evidence files and measurement tools.

This is a benchmark-only memory configuration. Delta files stay on disk; RAM
metadata disappears at shutdown. Product runtime/capability claims are unchanged.
