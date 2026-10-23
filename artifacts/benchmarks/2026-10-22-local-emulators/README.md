# Local emulator benchmark evidence — 2026-10-22

[Methodology, limitations and reproduction](../../../docs/benchmarks/local-emulators.md).

- [Pooled results](combined-results.json): both complete runs, 30 trials and 1,500 timed samples; no measured trial discarded.
- [Generated README table](results-table.md).
- [First complete run](initial-measurement/results.json) with its exact [harness](initial-measurement/benchmark_emulators.py) and [native agent launcher](initial-measurement/benchmark_sail_agent.py).
- [Final complete run](measured/results.json), bound to the current scripts by their retained hashes.
- [Source, dependency and executable provenance](provenance.json), [native build identities](native-builds.json), and pinned dependency snapshots in this directory.
- [Setup probes](setup-probes.json): preflight findings and corrected configurations, excluded from benchmark statistics.
- [Validation record](validation.json) and [artifact/tool hash inventory](manifest.json).

The comparison measures core API readiness, exact 64 KiB script-byte round trips,
a verified 10,000-row Delta aggregate, and idle core API process RSS. Optional
notebooks, job execution, full-stack startup/memory, crash consistency and general
Databricks/Spark compatibility are outside this measurement. Feature comparison
uses pinned upstream documentation/source, separate from these measured cases.
