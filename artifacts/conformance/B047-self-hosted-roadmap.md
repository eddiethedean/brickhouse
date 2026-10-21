# B047 — Emulator-first self-hosted roadmap planning

Date: 2026-10-20. Outcome: **planning complete; deployment unimplemented**.

## Scope and priority

The owner requested long-term self-hosted planning across all phases and explicitly reaffirmed that JVM-free Databricks emulation is primary and self-deployment follows afterwards. [Document 18](../../docs/plan/18-self-hosted-platform.md), [ADR 0034](../../docs/decisions/0034-emulator-first-self-hosted-platform.md) and the [roadmap](../../ROADMAP.md) record that priority.

Phases 0.1–0.9 retain their emulator outcomes. Each phase now identifies the relevant identity/service/runtime/storage/recovery boundary to preserve while doing its existing work. B048 adds a bounded architecture review before B023; it neither expands the workload corpus nor introduces deployment infrastructure. All 85 required implementation children remain.

B049/SH.0 assesses actual stability after B042. B050–B055 implement the separate future stages: authenticated trusted single-server service, browser and optional integrations, isolated workloads, remote job workers/automation, operational qualification and distribution candidate. No package versions or dates are assigned. Every deployment implementation is transitively gated on B049. B042 has no deployment dependency. C059–C064 are planned with empty tested-version/test/evidence fields.

## Checks run

- `python scripts/validate_planning.py`: passed for 140 tasks, 55 top-level packages, 85 phase 0.1 children, 64 capabilities, nine emulator phases and seven post-stability stages. Exact final link/file counts are in planning/validation.json.
- `env -u DATABRICKS_TOKEN -u DATABRICKS_HOST PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 /tmp/brickhouse-sparkless-4140/bin/python -m pytest -q tests/unit/test_planning_validation.py`: **10 passed**. Tests reject stability bypass, missing B042 prerequisite, premature implementation, an emulator/deployment cycle, a later-phase emulator dependency, premature deployment version assignment, missing stage and duplicate assignment. The real plan passes; a future deployment child correctly inherits its later stage. Malformed ledger fixtures are in memory only.
- Ruff check and format check on the validator and its regression tests: passed; strict mypy on `scripts/validate_planning.py`: passed.
- `git diff --check`: passed.

## Preservation audit

Compared pre-edit ledgers saved outside the checkout with the final ledgers. All 131 pre-existing task IDs, acceptance criteria, required child lists, dependency edges, plan references and evidence records remain. Only unfinished B023 and its three children gain B048; existing later emulator epics gain guidance references without new deployment criteria. All 58 pre-existing capability states remain. This task adds nine top-level packages and six planned capabilities.

Concurrent implementation in the same checkout completed B020.2, started B020.3 and expanded the already-partial C030 Terraform evidence. Those updates and their ADR 0035/report were retained; they are not B047 results. Planning caused no compatibility promotion or rollback of implementation progress.

Pre-edit ledger SHA-256 values:

- tasks.json: `76c66a51ddaa673ae436d8654e095ff8b507b4602992ee85fc0f7b027c48d5c0`
- capabilities.json: `b1feb608c7d866cfb8d8657b3fe05fab6137feb78ff47a551c9eb1c40bbc41df`

## Limits

Only documentation, planning ledgers, planning validation tooling and its focused regression tests changed in B047. No emulator or deployment runtime test, dependency installation, source/runtime API change, workspace migration, personal credential read, account request, cloud provisioning, upstream change or publication was performed for this task. Local process isolation and compatibility tokens remain insufficient for deployed security. Multi-host execution, high availability and distributing one query remain distinct future claims requiring their own evidence.
