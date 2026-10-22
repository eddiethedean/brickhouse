# B064 — Phase 0.1 review corrections

Date: **2026-10-22** · Status: **focused regressions passed**

## Verification

The corrected source passed 93 focused tests on macOS 26.5.2 arm64, CPython
3.11.15, SQLite 3.50.4, Sparkless 4.15.0, delta-rs 1.2.1, PyArrow 23.0.1,
SQLGlot 30.24.0, and Databricks SDK 0.153.0 against loopback only. The run
unset `DATABRICKS_TOKEN`, `DATABRICKS_HOST`, and
`DATABRICKS_CONFIG_PROFILE`; it did not load `.env` or use a Databricks
workspace or remote compute.

```text
env -u DATABRICKS_TOKEN -u DATABRICKS_HOST -u DATABRICKS_CONFIG_PROFILE \
  PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src .venv/bin/python -m pytest \
  -q --tb=short \
  tests/unit/test_jobs_api_sdk.py::test_reset_revisions_share_stable_job_concurrency_identity \
  tests/unit/test_jobs_api_sdk.py::test_sdk_admits_declared_concurrency_and_replays_tokens_without_duplicates \
  tests/unit/test_jobs_api_sdk.py::test_sdk_persists_eight_fifo_runs_behind_two_active_workers \
  tests/unit/test_jobs_api_sdk.py::test_restart_resumes_undispatched_fifo_runs_without_replaying_started_attempts \
  tests/unit/test_worker_supervision.py \
  tests/unit/test_jobs_repository.py \
  tests/unit/test_workspace_snapshot.py \
  tests/unit/test_delta_operation_journal.py \
  tests/unit/test_sql_statement_service.py \
  tests/unit/test_sql_statement_results.py \
  tests/unit/test_sql_statement_api.py
```

Result: **93 passed**. The run covers same-name SQL tables across schemas,
bounded public Arrow-batch collection under join amplification, SQL `INSERT`
`VALUES`/`SELECT` paths, timestamp precision, descendant-held worker pipes,
two active plus eight FIFO queued runs, reset identity, schema 21 migrations,
snapshot inventory validation, restore-safe operation replay, and changed-input
rejection. The migration suite includes historical schema upgrade and rollback
cases. This is focused source-tree verification; it is not a rerun of the full
offline installed-wheel G1–G4 matrices.

## Behavioral limits

- `max_concurrent_runs` accepts 1–10, while the local process dispatches at most
  two attempts and stores at most eight additional queued runs. Overflow is
  rejected before a run record is created. Only undispatched runs resume after
  restart; runs with started attempts are interrupted without replay.
- SQL results retain only the row/byte-limited prefix while consuming public
  Sparkless Arrow batches to count rows and determine truncation. Engine working
  memory is outside this result-retention bound.
- SQL `INSERT` supports the selected bounded `VALUES` and `SELECT` append forms
  through the journaled shared Delta service. Unsupported forms fail before
  mutation. This does not establish broad SQL or Databricks parity.
- Worker descendant supervision covers the owned POSIX process group. Escaped
  descendants, Windows process cleanup, and malicious-code isolation are not
  claimed.
- Snapshot manifest v2 hashes materialized managed table files and rejects a
  missing or altered captured table even if its source workspace is gone.
  Unmaterialized registrations remain restorable. External table bytes remain
  excluded.
- Metadata schema 21 adds stable job admission identities and durable dispatch
  markers. The workspace format remains version 1.

No package was published and no Databricks account was used.

## Final review rerun (2026-10-22)

After the source-format and strict typing cleanup, the B064 selectors plus
planning-validation regressions passed **113 tests**. The full repository suite
passed **415 tests** with **6 expected skips** for optional external CLI,
Terraform, and SQL connector installations. Both runs unset the Databricks
credential variables. Ruff check, Ruff format check, and strict mypy passed.

```text
env -u DATABRICKS_TOKEN -u DATABRICKS_HOST -u DATABRICKS_CONFIG_PROFILE \
  PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src .venv/bin/python -m pytest \
  -q --tb=short tests
```

The 113-test selector run also included `tests/unit/test_planning_validation.py`.
The 415-test full source-suite and 113-test selector results above used
Sparkless 4.15.0 and predate the B064.11 recovery change and 4.15.1 pin update;
they are retained as historical evidence. The prior 132/169-case offline G1–G4
results remain tied to their archived wheel hash.

## Interim candidate before Sparkless 4.15.1 requalification

The interim candidate wheel `858ce107f49683b09773eb77230ca95ec853c8b7b2bf208ee8e3cd5be5b46ab8` was installed in a fresh CPython 3.11.15 base environment provisioned offline. Its B064 SQL, worker, queue/restart, migration, snapshot, journal, and API selectors passed **94 tests** under the repository's macOS loopback-only network guard. The installed package came from `site-packages`; the guard recorded 264 loopback connections/resolutions, no outbound connections, no `.env` reads, no inherited Databricks variables, no forbidden imports or processes, and no notebook-extra distributions. This artifact predates the Sparkless 4.15.1 pin update and is retained as history.

## Current Sparkless 4.15.1 candidate installed-wheel check

The rebuilt current candidate wheel `b8ce3f90fa5956f5e1baae1cfb31c7ccd3279a93ef7fdbfb6c5785319d464d51` was installed in the prepared offline CPython 3.11.15 base environment. The B064 SQL, worker, queue/restart, migration, snapshot, journal, and API selectors passed **99 tests** with Sparkless 4.15.1 under the macOS arm64 loopback-only network guard. The installed package came from `site-packages`; the machine report records 264 loopback connections/resolutions, no outbound connections, no `.env` reads, no inherited Databricks variables, no forbidden imports or processes, and no notebook-extra distributions. The SQL statement-service source suite also passed 35 tests with Sparkless 4.15.1. See [the current-wheel report](../releases/0.1.0/evidence/conformance/B064-current-wheel-offline.json).

The current-candidate run covers B064's affected installed-wheel behavior and refreshed package contents. It does not repeat the complete G1–G4 base/notebook matrices. Those matrix results belong to the archived previous wheel hash and remain historical in the candidate bundle. The full source suite was not rerun after B064.11 and the Sparkless 4.15.1 dependency update.

## Sparkless 4.15.2 current-candidate qualification (2026-10-22)

The final BrickHouse 0.1.0.dev0 candidate wheel SHA-256 is `f6d89bed8e6e9611595c9cce8686383e6225a3fc3376126cbb61df6a51adfcef`; the source archive hash is recorded in the owner-review bundle. The wheel installs Sparkless 4.15.2 (verified upstream wheel SHA-256 `7d12d6f76212e52586cbe07ab12152f4c7dcf04bd2873532751f1335ee81cfb2`).

The focused installed-wheel B064 selectors passed **99 tests** on macOS arm64 CPython 3.11.15 under the loopback-only offline guard. The report records the installed `site-packages` path, 263 loopback connections/resolutions, no outbound traffic, no `.env` reads, no Databricks credential variables, no forbidden imports/processes, and no notebook-extra distributions. The standalone 35-case SQL statement-service module also passed on Sparkless 4.15.2. See the [B064 installed-wheel report](../releases/0.1.0/evidence/conformance/sparkless-4.15.2-current/B064-current-wheel-offline.json).

The exact candidate then passed selected G1–G4 in four fresh offline lanes: base 137 cases per interpreter and notebook-extra 175 per interpreter on CPython 3.11.15 and 3.12.13, 624 matrix passes total. G1+G2 includes the selected UTC-aware Delta timestamp predicate fixed in Sparkless 4.15.2. The first pass exposed and led to correction of the notebook kernel's stale 4.15.0 requirement; a test now checks alignment with the project runtime pin. G4 also required adapting the connector cancellation test hook to accept the row/byte limit arguments. The final reports all refer to the rebuilt wheel and source hashes above. See the [candidate bundle](../releases/0.1.0/README.md) and its [machine-readable matrix reports](../releases/0.1.0/evidence/conformance/sparkless-4.15.2-current/).

No personal Databricks account, remote compute, or cloud workload was used. This qualifies only the selected local cases on macOS arm64 CPython 3.11/3.12; Linux/Windows and broader or remote Databricks parity remain unqualified. Nothing was published.
