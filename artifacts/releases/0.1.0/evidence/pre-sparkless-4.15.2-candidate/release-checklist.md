# 0.1.0 owner release checklist

This checklist distinguishes current-artifact verification from historical qualification and remaining owner decisions. It does not authorize publication.

## Current candidate evidence

- [x] Rebuild the `0.1.0.dev0` wheel and sdist from the reviewed checkout.
- [x] Audit wheel/sdist metadata, base dependencies, Python requirement, retained third-party notices, credential-file exclusion, and release-bundle exclusion.
- [x] Install the current base wheel in the prepared offline environment and pass the 99-case focused B064 regression set under the loopback-only guard.
- [x] Run Ruff check/format for the changed SQL test module, `uv lock --check`, `uv pip check`, planning validation, and the current wheel/sdist audit.
- [ ] Run the complete source-tree suite and full strict mypy/static gates after the B064.11 and Sparkless 4.15.1 updates. The earlier 415-pass/6-skip suite predates these changes.
- [x] Generate the capability report from the exact adjacent JSON snapshot.
- [x] Record current wheel/sdist hashes, version inventory, current test selectors, and guard report.
- [x] Keep publication as a separate owner-authorized action.

## Historical qualification retained

- [x] The previous wheel hash passed 132 base G1–G4 cases per interpreter and 169 notebook-extra cases per interpreter on macOS arm64 CPython 3.11.15 and 3.12.13. Those results are explicitly tied to the previous artifact and do not qualify the current wheel.

## Remaining owner and release actions

- [ ] Run full base and notebook-extra offline G1–G4 qualification against the current candidate hash for every advertised Python version.
- [ ] Select BrickHouse's project license; add the root `LICENSE` and corresponding `pyproject.toml` metadata. No license has been selected here.
- [ ] Qualify Linux x86_64 on CPython 3.11 and 3.12 using the configured clean install/offline CI matrix, or formally revise the release support scope.
- [ ] Review package/repository ownership, name availability, third-party notices, candidate limitations, and the frozen phase 0.1 scope.
- [ ] Review and finalize release notes/version metadata after resolving the above gates.
- [ ] Explicitly authorize any public release build and publication.

## Claims kept out of the release

- No Windows or Linux support is claimed.
- Local SDK/CLI/connector behavior does not establish remote Databricks compatibility or real cloud compute behavior.
- Warehouse and cluster lifecycle metadata is simulated.
- The capability registry contains no fully supported capability.
- Routine tests and candidate qualification did not load `.env`, inherit real Databricks credentials, contact the personal workspace, or run cloud work.
