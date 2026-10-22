# BrickHouse 0.1.0 — draft release notes

**Draft for owner review.** The retained package artifacts are still versioned
`0.1.0.dev0`; this document does not announce a public release.

## Summary

BrickHouse is a local, JVM-free emulator for a selected set of Databricks
workflows. Phase 0.1 brings together a shared local catalog and Delta table
authority, bounded SQL and DataFrame execution, supervised Python jobs and
optional notebooks, selected client integrations, and local crash/restart
recovery.

## Selected behavior

- Local workspace, catalog, schema, and table identity with persistent metadata.
- Native Delta-backed create/read/append/overwrite, selected SQL mutations and
  history/snapshot reads, and shared committed data across SQL and Sparkless.
- Bounded serverless and classic job/notebook profiles with seven selected
  compute selectors; resource lifecycle metadata is simulated.
- Serverless, Pro, and classic local SQL warehouse profiles.
- A base installation without notebook runtime packages and a separate
  `brickhouse[notebooks]` extra.
- Selected Databricks SDK, CLI, Terraform/bundle, and SQL connector workflows
  against local loopback services.
- Durable operation recovery, bounded result/log retention, and coordinated
  local workspace snapshot/restore with explicit external-table handling.

## Candidate qualification status

The previous candidate wheel (SHA-256 `d17b23f20530998c3b5256a4e9ae37101720080e35cc4252c5519330a5ca8979`) passed the full offline base and notebook-extra G1–G4 matrices on macOS arm64 with CPython 3.11.15 and 3.12.13. Those results are archived and do not qualify the refreshed wheel.

The refreshed wheel (SHA-256 `568516eca2ecf7995fc9c64e383767491b8f361df7c4b2a4983fd8171f20ed29`) pins Sparkless 4.15.2 and passed 99 focused installed-wheel B064 regression tests in a prepared offline base environment on macOS arm64 CPython 3.11.15. The SQL statement-service source suite passed 35 tests against the same Sparkless version. The full current-candidate G1–G4 matrix has not been rerun. The selected nested struct-field SQL projection now passes; the selected UTC-aware Delta timestamp predicate now returns order_id 2 under upstream PR #1721, closing [Sparkless issue #1720](https://github.com/eddiethedean/robin-sparkless/issues/1720). Linux x86_64 and Windows remain unqualified and are not advertised.

## Compatibility limits

The capability registry contains 24 partial, 2 simulated, and 41 planned
entries; none is fully supported. This candidate does not claim remote
Databricks service/runtime compatibility or real cloud compute. SQL connector
Arrow-stream/LZ4 and broader metadata methods, Thrift, and Windows remain
unqualified. See the [capability report](capabilities.md) and [owner
checklist](release-checklist.md) for exact scope and remaining gates.

The project license has not been selected, and Linux x86_64 has not passed CI.
The owner must resolve these gates and separately authorize publication before
these draft notes can describe a public `0.1.0` release.
