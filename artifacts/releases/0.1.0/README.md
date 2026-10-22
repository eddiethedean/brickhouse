# BrickHouse 0.1.0 owner-review candidate

**Candidate assembled:** 2026-10-22  
**Package version:** `0.1.0.dev0`  
**Target release:** `0.1.0`  
**Publication state:** not published; a separate owner decision is required.

This bundle contains the exact current wheel and source archive, capability snapshot, candidate manifests, current machine-readable qualification reports, historical reports tied to earlier hashes, and owner release checklist.

## Candidate artifacts

- [Current wheel](candidate/brickhouse-0.1.0.dev0-py3-none-any.whl)
- [Current source distribution](candidate/brickhouse-0.1.0.dev0.tar.gz)
- [Artifact manifest and SHA-256 values](manifest.json)
- [Distribution metadata and notice audit](distribution-audit.json)
- [Supported install scope](scope.json)
- [Case manifest](cases.json)
- [Dependency and client version inventory](versions.json)
- [Capability snapshot](capabilities.json) and [rendered report](capabilities.md)
- [Draft release notes](release-notes-draft.md)
- [Owner release checklist](release-checklist.md)
- [Current Sparkless 4.15.2 machine reports](evidence/conformance/sparkless-4.15.2-current/)

The wheel SHA-256 is `f6d89bed8e6e9611595c9cce8686383e6225a3fc3376126cbb61df6a51adfcef`. The source archive SHA-256 is `c60910965e351d79f90efdd3073a16840e43a217c3bf83516398dec327dca14a`. The distribution audit passed metadata, Python requirement, retained-notice, credential-path and recursive release-bundle checks; it records that the BrickHouse project license has not been selected.

## Current candidate qualification

The exact wheel was installed in four fresh hash-pinned offline environments: base and `notebooks` extra on macOS 26.5.2 arm64 CPython 3.11.15 and 3.12.13. All passed dependency consistency. The selected G1–G4 matrix passed **624 cases**: **274 base** (137 per interpreter) and **350 notebook-extra** (175 per interpreter). A separate installed-wheel B064 review run passed **99 focused regressions** on CPython 3.11.15. The selected Sparkless 4.15.2 UTC-aware Delta timestamp predicate passed inside G1+G2.

The first 4.15.2 matrix run exposed a stale hardcoded Sparkless 4.15.0 notebook-kernel requirement. BrickHouse now pins the required kernel distribution to 4.15.2 and a regression test checks it against the project runtime pin. A G4 SQL cancellation test adapter was also updated to forward the connector's row and byte limits. The final matrix was rerun against the rebuilt candidate. The archived 132-case base and 169-case notebook-extra results apply only to the prior wheel hash `d17b23f20530998c3b5256a4e9ae37101720080e35cc4252c5519330a5ca8979`.

Every current qualification run used an installed candidate wheel, clean local environments, and the macOS loopback-only network guard. Databricks credential variables were absent, `.env` was not read, and the guard observed no forbidden imports or Java, Spark driver, or Docker processes. No personal Databricks workspace, remote compute, or cloud workload was used.

## Qualified scope and remaining gates

Qualification covers only the selected local G1–G4 cases and macOS arm64 CPython 3.11.15/3.12.13 combinations. It does not establish broad Spark or remote Databricks behavior. Linux x86_64 remains unqualified pending CI; Windows is unqualified. Warehouse and cluster lifecycle metadata is simulated. Kernel Arrow-stream/LZ4 results, broader connector metadata methods, Thrift and other unselected SQL/timestamp forms remain unqualified. The capability registry still has no fully supported entry.

The project license and root `LICENSE` remain owner decisions required before public publication. Review the frozen phase 0.1 scope, repository/package ownership, notices and release notes, then explicitly authorize any public release. Nothing was published.

See the repository [release plan](../../../docs/plan/16-phase-0.1-initial-release.md), [delivery plan](../../../docs/plan/12-delivery-and-operations.md), and [current status](../../../STATUS.md).
