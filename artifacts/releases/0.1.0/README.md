# BrickHouse 0.1.0 release candidate

Prepared 2026-10-22. Version **0.1.0**, license **Apache-2.0**. Nothing has been
published or tagged. The saved PyPI pending publisher is owner-configured and
verified; see the [release checklist](release-checklist.md).

## Exact candidate

- [Wheel](candidate/brickhouse-0.1.0-py3-none-any.whl)
- [Source distribution](candidate/brickhouse-0.1.0.tar.gz)
- [SHA-256 manifest](manifest.json) and [distribution/license audit](distribution-audit.json)
- [Scope and qualification matrix](scope.json), [versions](versions.json), and [27-family case map](cases.json)
- [Capability snapshot](capabilities.json) and [rendered report](capabilities.md)
- [Prepared release notes](release-notes.md), [publisher configuration](publishing.json), and [saved publisher verification](evidence/B066/publisher-verification.json)
- [CI reports](evidence/B066/ci-39873265129/) from [run 39873265129](https://github.com/eddiethedean/brickhouse/actions/runs/39873265129)

Wheel SHA-256: `3ba1364cb41cf76c78f087f654a139498683e18ea6749a900defca24322b05e7`. Source SHA-256: `23efd79df8290d49950c3e3234d971c5026740c69adfe83048022d2cbce4f619`.
The source revision qualified by that run is `e3abdcee28d98a2b0298c2cefdd873f976ae488b`; it built one candidate
for every lane. Changes confined to this excluded release bundle do not alter
the wheel or source archive contents.

## Qualification

**1368 installed-wheel cases** passed across all eight Ubuntu 22.04 x86_64/macOS 26
arm64, Python 3.11/3.12, base/notebook lanes. **1832 full-source cases**
passed across all four OS/Python combinations. Required skips: **zero**.
The JSON/XML reports record exact interpreter versions, artifact hashes, installed
imports, notebook separation, and OS network denial. Required clients were
provisioned from pinned, checksum-verified artifacts before offline execution.
Routine checks used no personal Databricks account or credentials.

The real orders workflow covers all seven Python and ipynb compute selections,
three source notebook profile families, all three SQL warehouses, actual native
aggregate output, dependent reads, secrets, resource lifetimes, and reopen checks.
Recovery and deterministic terminal/admission ordering are included.

## Limits and history

67 capability entries remain partial, simulated, or planned; none is fully
supported. This is a bounded local emulator. It does not allocate cloud compute,
provide a worker sandbox, authenticate loopback callers, or establish remote
Databricks Runtime/service parity. See [scope.json](scope.json).

Earlier 0.1.0.dev0 artifact metadata is retained under
[evidence/pre-B066-development-candidate/](evidence/pre-B066-development-candidate/).
Other dated development reports remain tied to their original hashes. They do
not qualify the final candidate. Root source metadata and documents are outside
this bundle; the bundle itself is excluded from source distributions to prevent
recursive packaging and self-referential source hashes.
