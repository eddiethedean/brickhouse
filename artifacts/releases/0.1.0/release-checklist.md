# 0.1.0 release checklist

Preparation does not authorize publication. The matching pushed version tag is
the separate owner publication action after every readiness prerequisite passes.

- [x] Set package/runtime version 0.1.0 and choose Apache-2.0 under owner authorization.
- [x] Retain root LICENSE, SPDX metadata, and dependency licenses/notices in wheel and sdist.
- [x] Audit all 27 requirement families against actual runnable cases and preserve the frozen acceptance criteria.
- [x] Pass the complete current source suite on Linux/macOS and Python 3.11/3.12 with every pinned external client enabled and zero required skips.
- [x] Pass installed-wheel G1–G4 in all eight OS/Python/base-or-notebook lanes with OS-enforced outbound network denial and zero required skips.
- [x] Verify the native golden aggregate, all required compute/warehouse profiles, failures, admission ordering, recovery, and restart consistency.
- [x] Build one exact candidate for every qualification lane, check dependency consistency and installed imports, and audit both artifacts.
- [x] Check readable user documentation, current capability limits, release notes, and exact version/hash inventories.
- [x] Verify GitHub repository ownership, the tag-only pypi environment, and separate least-privilege OIDC publishing job.
- [x] Look up the brickhouse PyPI name (404 on 2026-10-22); record that lookup does not reserve ownership.
- [x] Verify the owner-configured PyPI pending GitHub Trusted Publisher for eddiethedean/brickhouse, release.yml, environment pypi; retain the saved entry and planning/publishing.json record.
- [ ] Explicitly authorize any public release build and publication.

Exact wheel/source hashes, qualification counts and reports are in [manifest.json](manifest.json), [scope.json](scope.json), and [the passing CI run](https://github.com/eddiethedean/brickhouse/actions/runs/39873265129). Historical development evidence is explicitly archived.
