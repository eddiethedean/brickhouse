# BrickHouse 0.1.0 owner-review candidate

**Candidate assembled:** 2026-10-22  
**Package version:** `0.1.0.dev0`  
**Target release:** `0.1.0`  
**Publication state:** not published; publication requires a separate owner decision.

This refreshed bundle contains the reviewed source's wheel and sdist, capability snapshot, exact current regression selectors, version inventory, prior qualification evidence, and release checklist. The candidate remains `0.1.0.dev0`; it is not the public `0.1.0` release.

## Candidate artifacts

- [Current wheel](candidate/brickhouse-0.1.0.dev0-py3-none-any.whl)
- [Current source distribution](candidate/brickhouse-0.1.0.dev0.tar.gz)
- [Artifact manifest and SHA-256 values](manifest.json)
- [Distribution metadata and notice audit](distribution-audit.json)
- [Supported install scope](scope.json)
- [Case manifest](cases.json)
- [Dependency and client version inventory](versions.json)
- [Draft release notes](release-notes-draft.md)
- [Owner release checklist](release-checklist.md)

The current wheel SHA-256 is `b8ce3f90fa5956f5e1baae1cfb31c7ccd3279a93ef7fdbfb6c5785319d464d51`; the current source archive SHA-256 is `0499ac91f729f3885711c9a0c598cb3234a7502ed1cae556b2760209db3b91c7`. Both pass the distribution audit, which checks dependency metadata, retained third-party notices, credential-file paths, and recursive release-bundle exclusion. The wheel pins Sparkless 4.15.1. The interim 4.15.0 candidate and its test report are archived under [pre-sparkless-4.15.1-candidate](evidence/pre-sparkless-4.15.1-candidate/).

The prior fully qualified wheel (`d17b23f20530998c3b5256a4e9ae37101720080e35cc4252c5519330a5ca8979`) and sdist are retained under [prior-qualified-candidate](evidence/prior-qualified-candidate/). The historical 132-case base and 169-case notebook-extra G1–G4 runs per interpreter apply only to that prior wheel hash. Their original reports and selectors remain in the evidence folder.

## Current candidate verification

The current installed wheel passed 99 focused B064 review regressions against Sparkless 4.15.1 under the macOS arm64 CPython 3.11.15 loopback-only offline guard. The run confirmed the package imported from its installed wheel, had no notebook extra or full PySpark/Delta Spark distributions, inherited no Databricks credential variables, read no `.env`, used only loopback traffic, and spawned no forbidden processes. The machine-readable [current-wheel report](evidence/conformance/B064-current-wheel-offline.json) records the selectors and guard details. The SQL statement-service source suite also passed 35 tests against Sparkless 4.15.1. The complete repository suite has not been rerun after the B064.11 and dependency changes; the earlier 415-pass/6-skip run is historical.

Ruff check and formatting for the changed SQL test passed; `uv lock --check`, `uv pip check`, planning validation, and the current wheel/sdist audit passed. These checks do not requalify the current artifact for the complete G1–G4 matrix: that matrix is still required against the current hash before release.

The current registry has 67 entries: 24 partial, 2 simulated, and 41 planned. None is fully supported. Warehouse and cluster infrastructure lifecycle remains simulated. Kernel Arrow-stream/LZ4, broader SQL connector methods, Thrift, remote Databricks behavior, and cloud compute remain outside qualified claims. The nested struct SQL field projection is verified on Sparkless 4.15.1; the direct UTC-aware Delta timestamp predicate still raises the old mismatch and is reported as [Sparkless issue #1720](https://github.com/eddiethedean/robin-sparkless/issues/1720).

## Remaining release gates

BrickHouse has no selected project license or root `LICENSE` file. The owner must choose the project's license and update package metadata before public publication. Linux x86_64 has not passed the configured install/offline CI matrix and is not advertised. The current artifact still needs its complete base and notebook-extra offline G1–G4 matrix on each advertised Python version. Windows is also unqualified. No license was inferred, no Databricks token was used, and nothing was published.

See the repository's [release plan](../../../../../docs/plan/16-phase-0.1-initial-release.md), [delivery plan](../../../../../docs/plan/12-delivery-and-operations.md), and [current status](../../../../../STATUS.md).
