# Project status

Updated: 2026-10-22

- Current version: **0.1.0**, licensed **Apache-2.0**. The owner authorized choosing the project license. Nothing has been tagged or published.
- Implementation: the bounded B001–B023/B046 scope, B048 review, all eleven B064 fixes, B065 release automation, and B066 release preparation are complete. All **142 phase 0.1 children** are done; the ledger has 208 tasks and 66 top-level packages. Future emulator/deployment/cluster work remains planned.
- Final qualification: **1,368 installed-wheel offline cases** and **1,832 full-source cases** (458 per OS/Python pair) pass with zero required skips on Ubuntu 22.04 x86_64/macOS 26 arm64, native CPython 3.11.17/3.12.15. Exact candidate/source hashes, installed runtime versions, source revision and passing CI reports are frozen in the release bundle; historical development reports do not qualify different hashes.
- Requirement audit: [release-requirements.json](planning/release-requirements.json) maps all 27 document-16 case families to 63 actual test selectors. The validator also checks all 253 unique capability test references and every backlog status against the ledger. The orders workflow computes and writes the exact native BIGINT/DECIMAL aggregate across seven Python jobs, seven ipynb jobs, and three source notebook profiles, then verifies all three warehouses and reopened state.
- Capability scope: 67 entries remain 24 partial, 2 simulated, and 41 planned; none is fully supported. Cluster/warehouse lifecycle metadata and access modes are simulated. Actual local workers, queries, Delta data, failures, retries, and restart recovery are tested only within the documented versioned subsets.
- Runtime: one persistent catalog and native delta-rs table service are shared by Sparkless DataFrames, SQL, jobs, and notebooks. Sparkless 4.15.2, deltalake 1.2.1, PyArrow 23.0.1, and SQLGlot 30.24.0 are pinned base dependencies. The optional notebook extra is separate; DuckDB is not a fallback.
- Clients: selected SDK 0.153.0, CLI 1.21.0, Terraform 1.16.6/provider 1.137.0, connector 4.6.0/kernel 1.1.0 cases use real loopback services. Kernel Arrow-stream/LZ4, Thrift, broader metadata, remote authentication, and Windows remain outside qualification. The local API token is not authentication; workers run trusted code as the local user.
- Known upstream limit: SQL COUNT returns int32 while DataFrame count returns int64 on Sparkless 4.15.2, reported as [issue #1723](https://github.com/eddiethedean/robin-sparkless/issues/1723). The frozen SQL fixture records this limitation. The selected UTC Delta timestamp issue #1720 is fixed in 4.15.2 and passes locally.
- Release automation: CI and tag releases share source checks and one-candidate G1–G4 qualification on Linux x86_64/macOS arm64, Python 3.11/3.12, base/notebooks. The tag workflow publishes the exact qualified files through a separate OIDC job after the readiness checklist. The GitHub `pypi` environment accepts `v*.*.*` tags. The owner configured the PyPI pending publisher and supplied its saved confirmation; exact fields match. See [publishing configuration](planning/publishing.json).
- Reference verification: occasional personal Databricks captures are authorized under [document 15](docs/plan/15-personal-databricks-verification.md). Ordinary tests and required CI never use account credentials or the checkout's `.env`. The latest bounded aggregate capture returned BAD_REQUEST, restored the existing warehouse to STOPPED, and yielded no remote schema/rows oracle.
- Future work: [ROADMAP.md](ROADMAP.md) retains emulator phases 0.1–0.9. [Self-hosting](docs/plan/18-self-hosted-platform.md) and [optional cluster execution](docs/plan/19-sparkless-cluster-execution.md) remain gated by emulator stability B049 after B042. B056/B063 are completed planning/research only.
- Release evidence: [0.1.0 review bundle](artifacts/releases/0.1.0/README.md); [final readiness plan](docs/plan/21-release-readiness.md). B066.4 retains the final audited artifacts and passing exact-hash qualification reports.

## Current limitation

Sparkless 4.15.2 retains public active-query cancellation, bounded Arrow batch export, and structured provider errors. B011.3.3 verifies SQL cancellation and terminal outcome handling against 4.15.0; B064.2 uses Sparkless public bounded Arrow batches for SQL result collection and retains only the row/byte-limited output prefix before encoding; this bounds retained result memory, not engine working memory. The separate worker DataFrame-write input adapter still materializes Arrow before its own input checks. B010.1–B010.3 integrate selected internal version-pinned Delta reads/writes and session isolation; B015.1.1–B015.1.3 provide worker-only imports, profile-bound sessions, and authenticated registered-table reads; B015.2 provides the selected read-only DeltaTable facade. Worker writes remain bounded to append on existing registered tables. Native path writes, DataFrameWriterV2, broader DeltaTable mutations, timestamp/parameterized time-travel selectors and other unselected SQL mutations, unsupported options/schemas, and cross-process Delta concurrency remain incomplete or explicitly rejected. SQL DESCRIBE TABLE/HISTORY and literal-integer VERSION AS OF expose only the bounded local profiles documented in B019.2.3/.2.4 and do not claim remote Databricks parity. Classic sessions require explicit ANSI/timezone values for the mapped `spark35-local-v1` alias. No broad Spark or Databricks workspace/runtime parity is claimed. See [Sparkless 4.15.0 cancellation evidence](artifacts/conformance/B011.3.3-sparkless-4.15.0-cancellation.md), [B015.1.3 evidence](artifacts/conformance/B015.1.3-worker-registered-tables.md), and [B015.2 evidence](artifacts/conformance/B015.2-worker-delta-table.md).

## Handoff log

Append an entry after each implementation slice: date, task IDs, changes, test evidence, known limitations, and next ready tasks. Never put credentials or real secret values here.

### 2026-10-22 — Sparkless 4.15.1 requalification

Advanced the exact pin to Sparkless 4.15.1, updated B019.1.3/.1.4 fixtures, and reran all 35 SQL statement-service tests. Nested field projection now succeeds. The direct UTC-aware Delta timestamp predicate still raises the prior type mismatch in the published wheel, so I filed [Sparkless issue #1720](https://github.com/eddiethedean/robin-sparkless/issues/1720) after the #1714 closure. Databricks credentials were unset; no workspace or remote compute was used. The current 0.1 candidate still needs rebuilding and installed-wheel qualification against the new pin.

### 2026-10-22 — Completed B064.11

Added focused local regressions for provider rejection cleanup, retry after an interrupted managed registration, valid existing-table no-op, and preservation of external Delta data, malformed logs, and unresolved journal operations. `tests/unit/test_sql_statement_service.py` passed 35 tests on Sparkless 4.15.0; Ruff check passed and the test file was formatted. No Databricks credentials or remote workspace were used. The candidate wheel predates this source change and still needs a parent-gate refresh.

### 2026-10-16 — Reference verification planning update

Expanded B005's planned acceptance criteria and synchronized the backlog for bounded personal-account reference probes, sanitized fixture provenance, local regression design, and an optional manual GitHub capture workflow. Recorded D011/ADR 0001, updated testing/CI/bootstrap guidance, and ignored .env/raw captures. All implementation tasks and capabilities remain planned; B001 is still next. Documentation/manifest consistency checks are recorded in planning/validation.json; no implementation tests or Databricks calls were run.

### 2026-10-16 — Release phase roadmap

Added root ROADMAP.md with phase 0.1/version 0.1.0 covering the existing B001–B023 alpha requirements, followed by phases 0.2–0.9 covering every later work package. Recorded D012/ADR 0002 and linked release grouping from the README, engineering roadmap, and task manifest. Task dependencies, acceptance criteria, and capability states remain unchanged; B001 is still next. Documentation/manifest consistency checks are recorded in planning/validation.json; no implementation tests or remote runs were needed.

### 2026-10-16 — Phase 0.1 refinement

Preserved the existing 0.1.0 release scope and decomposed B001–B023 into 68 required children with inherited prerequisites and parent-criterion coverage. Added document 16 and D013/ADR 0003 for internal delivery checkpoints, minimum operation/type/client boundaries, the synthetic orders workflow, failure cases, decision owners, and the release checklist. Synchronized the root roadmap, backlog, manifest guide, bootstrap, and capability task references. All 110 task entries and 52 capabilities remain planned; B001.1 is the next implementation leaf. Planning consistency checks are recorded in planning/validation.json; no implementation tests or Databricks calls were run.

### 2026-10-22 — Started B064.11

At this checkpoint B064.11 was still in progress: failed first writes attempted safe managed-table cleanup; retries recovered an unmaterialized managed registration only when it had no Delta log and no unresolved operation; and `CREATE TABLE IF NOT EXISTS` returned no-op only for a supported managed snapshot, with external registrations failing explicitly. Focused regressions were not yet added or run, and candidate artifacts predated the source change. The 2026-10-22 completion entry below records the regression results.

### 2026-10-16 — Started B001.1

Claimed the first ready leaf. Scope: minimal src package, prerelease metadata, stdlib CLI help/version, and import/CLI acceptance checks. Capability claims and runtime behavior remain unchanged.

### 2026-10-16 — Completed B001.1

Added the `src/brickhouse` package, `0.1.0.dev0` metadata, and a stdlib `--help`/`--version` CLI. `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -q tests/unit/test_cli.py` passed (3 checks); disabling host pytest plugin autoload avoids an existing incompatible global pytest-cases installation. No runtime services or capability claims were added.

### 2026-10-16 — Completed B001.2

`python -m build --wheel --sdist` produced `brickhouse-0.1.0.dev0` artifacts. Installed the wheel with `--no-deps --no-index` in a fresh Python 3.11.14 virtual environment; import/version, console help/version and sdist secret exclusion passed. B001.3 is now in progress.

### 2026-10-16 — Completed B001; started B002.1

B001.1–B001.3 passed and parent B001 is done. Added the `src` package/CLI, 0.1.0.dev0 wheel and sdist, hash-locked developer tools, planning validator, and offline/local CI workflow. The wheel installed cleanly without runtime dependencies. Planning validation, Ruff, mypy, and 3 focused tests passed in a fresh Python 3.11/macOS arm64 dev venv. GitHub Python 3.11/3.12 and clean-wheel workflows are configured but not yet run. See artifacts/conformance/B001.1–B001.3 reports. B002.1 is in progress; no Databricks runtime capability has been implemented.

### 2026-10-16 — Completed B002; started B003.1

Audited Minilake 1.8.0 at pinned commit `0597285d1ba33daf76f553b82a32024ded7b78f6`. The inventory records the MIT notice, route/global-state and persistence coupling, DuckDB ownership, and Docker/Spark/`spark-submit` paths. ADR 0004 selects BrickHouse-owned catalog/jobs/files services for phase 0.1; no Minilake implementation was copied. B002.1 audit evidence is in `artifacts/conformance/B002.1-minilake-source-audit.md`. B003.1 now inspects the checked-out Sparkless source for actual released interfaces; no runtime capability changed.

### 2026-10-16 — Completed B003.1; started B003.2

Inspected clean Sparkless 4.13.3 source at `b73067252abd0720ea91ac447bfd3ce027e96a88`. The report maps the Rust/PyO3 APIs to S001–S006: local catalog and Delta path operations exist, while external provider resolution, delegated table writes, Arrow stream interchange, and capability/error contracts are absent. Python Arrow input materializes through `to_pylist`; `newSession` exists but does not establish isolation. B003.2 is building a temporary native probe; no production API was selected and the Sparkless checkout remains unmodified.

### 2026-10-16 — Completed B003.2; filed Sparkless blockers; started B003.3

Built Sparkless 4.13.3 from the inspected commit in a temporary environment. The isolated delta-rs bridge passed for primitive rows, SQL via a three-part temporary view, delegated PyArrow writes, and cross-reading the committed result. It is not a production adapter: collection is unbounded and decimal ingress becomes double. A separate repro showed format(delta).save(path) silently creates a non-Delta file. Created Sparkless issues [#1683](https://github.com/eddiethedean/robin-sparkless/issues/1683) (decimal preservation) and [#1684](https://github.com/eddiethedean/robin-sparkless/issues/1684) (Delta format path writer). B003.3 now tests session isolation; no remote compute or Databricks token was accessed.

### 2026-10-16 — Completed B003; started B004.1

The same-thread Sparkless session probe passed for independent config values and same-name temp views; invalid SQL raised SparklessError. A separate cross-thread transfer reproduced a PyO3 panic because PySparkSession is unsendable. Recorded the explicit session-owner, GIL/callback, reentrancy, error, and cancellation constraints in `artifacts/conformance/B003.3-session-isolation.md`. B003 is complete as a feasibility task; it does not claim production integration or concurrent in-process session safety. B004.1 now exercises native Delta mutation candidates on local files.

### 2026-10-16 — Completed B004.1; started B004.2

The local delta-rs 1.2.1 probe committed create/read, append, update, delete, and merge across versions 0–4. Sparkless 4.13.3 with Rust delta-rs 0.32.4 read each primitive snapshot. This supports delta-rs as a candidate only; typed/protocol compatibility and transaction recovery are not established. See `artifacts/conformance/B004.1-delta-mutations.md`. B004.2 probed null, decimal, timestamp, and declared type interoperability.

### 2026-10-16 — Blocked B004.2; started B005.1

The typed local fixture passed null, bigint, date, UTC-valued timestamp, string, boolean, array, struct, and Sparkless SQL projection reads. A valid decimal(12,2) Delta table panicked in Sparkless 4.13.3 before a DataFrame was returned; filed distinct upstream issue [#1686](https://github.com/eddiethedean/robin-sparkless/issues/1686), in addition to the PyArrow ingress issue #1683. B004.2 is blocked because the required golden workflow depends on decimal preservation. B005.1 independently inspects released client artifacts; no Databricks call was made during this slice.

### 2026-10-16 — Completed B005; started B006.1

Completed B005.4 with five local checks: fixture loading without credentials, ordinary CLI help/version under inherited sentinel credentials without reading `.env`/`.databrickscfg` or opening sockets, and explicit capture failures/token-source behavior. Added `.github/workflows/databricks-reference.yml`, restricted to manual dispatch on `main`, one curated probe, scoped secret injection, serialized five-minute execution, and one-day sanitized output retention. YAML parsed and workflow policy was inspected; GitHub-side dispatch was not run, and the `DATABRICKS_HOST` repository variable remains unconfigured. B005 parent criteria are complete with the remote capture kept separate from local implementation evidence. B006.1 is now active.

### 2026-10-16 — Completed B006.1; started B006.2

Implemented version 1 workspace config and metadata bootstrap in `src/brickhouse/workspace/`. The default is the BrickHouse policy `main.default`, not the personal Databricks account's `workspace` catalog. Initialization persists one catalog/schema and a workspace ID in SQLite, stages files before claiming the target with exclusive directory creation, refuses a raced existing target, and reopens through read-only validation. Thirteen focused tests passed on Python 3.11.14/SQLite 3.53.4, including custom namespaces, malformed/unsupported config and format, path characters, metadata disagreement, and the initialization race. Ruff and mypy checks are recorded in `artifacts/conformance/B006.1-workspace-bootstrap.md`. No CLI, process-owner lock, upgrade migrations, or server lifecycle is included. B006.2 is active.

### 2026-10-19 — Completed B004.2 with Sparkless 4.13.4

Installed the published `sparkless==4.13.4` macOS arm64 wheel in a fresh temporary environment and reran the typed local Delta probe. A real delta-rs Delta snapshot retained decimal(12,2), nullable scalar/date/timestamp/array/struct values in Sparkless DataFrame and SQL reads; PyArrow decimal ingress retained exact `Decimal` values; and `.format("delta").save(path)` created a readable Delta log. The wheel SHA-256, exact versions, commands, and limitations are recorded in `artifacts/conformance/B004.2-typed-interoperability.md`. These results verify the exact old #1683, #1684, and #1686 repro cases against 4.13.4; they do not establish broad type/protocol support or BrickHouse adapter behavior. B004.2 is done; B004.3 is ready after the active B006.2 slice.

### 2026-10-19 — Completed B006.2; started B006.3

Added POSIX `flock` ownership via `acquire_workspace_owner()`, a mode-0600 diagnostic lock file, and a transactional SQLite migration runner. Tests use a real child process to prove second-owner rejection and kernel lock recovery after process termination, real independent workspace databases, orderly context release/reacquire, and migration rollback after an injected local failure. The focused workspace suite passed 17 checks; Ruff, mypy, planning validation, and diff checks passed. ADR 0006 records the POSIX/local filesystem boundary. B006.3 is active to compose the lock/bootstrap into the CLI and embedded lifecycle; B004.3 is also ready.

### 2026-10-19 — Completed B006.3

Added `Workspace.local(directory)` as the embedded workspace context and composed the same context into `brickhouse serve`. The CLI now initializes workspaces, serves loopback-only `/health`, `/version`, and `/capabilities` diagnostics, reports installed Sparkless/delta-rs package metadata without importing providers or loading credentials, and provides a `run` command that clearly fails without executing a script until B013. Ctrl-C and SIGTERM shut the diagnostics server down and release its owner lock. Twenty-six focused local tests passed across CLI, embedded context, workspace bootstrap/ownership, and migration rollback; Ruff, format, strict mypy, and planning validation passed. ADR 0007 records the lifecycle boundary. Catalog, SQL, data-provider integration, sessions, and job execution remain unimplemented.

### 2026-10-19 — Completed B004.3; started B007.1

B004.3 used delta-rs 1.2.1 with Sparkless 4.13.4 on local files. A child process was killed after Delta commit but before SQLite journal finalization; restart recovered the matching table-scoped transaction identity and exact commit version from retained history. A raw same-version retry duplicated the row, so ADR 0008 requires a provider preflight and blocks uncertain retries. The classic checkpoint retained the identity and Sparkless read it. Python delta-rs was selected for phase 0.1 with default protocol reader 1/writer 2; advanced features, cloud storage, external concurrency, and BrickHouse integration remain unverified. B007.1 is active to add stable catalog/table identity repositories and canonical ownership constraints.

### 2026-10-19 — Completed B007.1; started B007.2

Added metadata schema version 2 with immutable catalog/schema UUIDs and managed/external table-registration repositories. Local canonical paths catch symlink aliases; metadata schema 1 upgrades under the owner lock while workspace format stays at version 1. The repositories reserve IDs and locations but do not create or access Delta data. Thirty-one focused tests passed; Ruff, formatting, strict mypy, planning validation, and diff checks passed. ADR 0009 and `artifacts/conformance/B007.1-catalog-identities.md` record the decision and limits. B007.2 is active for quoted/multipart identifier binding and per-context namespace defaults.

### 2026-10-19 — Completed B007.2; started B007.3

Added `parse_identifier()` and independent `NamespaceState` objects with bounded one/two/three-part resolution, quoted dotted/escaped-backtick parts, case-insensitive lookup, explicit missing/ambiguous errors, and atomic namespace changes. One read-only Databricks SQL statement verified context functions and quoted alias results; actual catalog/schema strings were redacted and the existing warehouse returned to STOPPED. Forty-four focused local tests passed; Ruff, format, strict mypy, and planning validation passed. ADR 0010 and `artifacts/conformance/B007.2-*` record profile and limits. B007.3 is active for pinned SDK catalog/schema workflows.

### 2026-10-19 — Completed B007

Implemented the exact pinned `databricks-sdk==0.153.0` catalog/schema create, list, get, and delete calls over the loopback server and SQLite. Metadata schema 3 persists comments and adds `information_schema`; catalog creation creates its `default` and `information_schema` schemas. Paginated lists and duplicate, missing, invalid, and in-use errors pass through the real SDK client. Default/system namespaces and namespaces with registered tables are protected from deletion. The combined namespace/metadata/SDK suite passed 45 local tests; Ruff, format, strict mypy, and planning validation passed. ADR 0011 and `artifacts/conformance/B007.3-sdk-catalog-schema.md` record the exact profile and limits. No Databricks workspace CRUD probe or real table data operation was performed. The next ready slice is B008.1.

### 2026-10-19 — Completed B008.1

Defined provider-neutral table handles, nested schema descriptors, Delta protocol/snapshot records, version-or-time snapshot selection, and the `TableServicePort`. The initial delta-rs profile rejects non-1/2 protocols, table features, unsupported modes, missing/duplicate targets, and schema mismatch before provider writes. `StagingArea` copies caller-labeled Parquet/Arrow IPC bytes into a private workspace artifact with owner metadata, SHA-256, and bounded cleanup. Real local delta-rs 1.2.1 and PyArrow 23.0.1 fixtures verified table identity, a nested schema, versions 0/1, protocol, and a copied readable Parquet input. Fifty focused tests passed; Ruff, format, strict mypy, and planning validation passed. ADR 0012 and `artifacts/conformance/B008.1-table-contracts-and-staging.md` record the contract and limits. No BrickHouse provider-backed Delta read/write path exists yet; B008.2 is next.

### 2026-10-19 — Started B008.2

Claimed the ready provider-integration task after B008.1. The slice will add explicit local Delta table service operations backed by the selected delta-rs provider and real local Delta evidence. Routine tests remain local and do not load Databricks credentials.

### 2026-10-19 — Completed B008.2; started B008.3

Added `DeltaRsTableService` behind the optional `brickhouse[delta]` extra. It reads registered local tables at latest/version/timestamp snapshots and commits staged Parquet/Arrow IPC inputs through delta-rs create/append/overwrite. Tests cover empty table/input commits, exact schema and Delta versions, operation metadata, invalid modes, incompatible schemas, path/manifest integrity, partition rejection, and same-process concurrent writes through external path aliases. The focused suite passed 56 local checks with delta-rs 1.2.1 and PyArrow 23.0.1; Ruff, format, strict mypy, and planning validation passed. No `.env` or Databricks account was accessed. The service is not connected to SQL/Sparkless and does not provide durable operation journaling or safe replay; B008.3 is active.

### 2026-10-19 — Completed B008.3; started B008.4

Added schema-version-4 per-table operation journaling to the local Delta service. Operation fingerprints and monotonic provider transaction versions are persisted before dispatch; exact replay recovers the commit version only when the provider transaction and matching recent Delta history agree. A real child process was killed after commit and the reopened workspace reconciled the operation without duplicate rows. Changed-payload reuse fails, and uncertain outcomes block blind retries. The focused local suite passed 60 checks; strict mypy, Ruff, and formatting passed. ADR 0014 and `artifacts/conformance/B008.3-operation-journal-recovery.md` record the evidence and limits. History lookup is bounded to 100 commits, and cross-workspace/host coordination is not established. B008.4 is active for ownership-safe table removal and interrupted cleanup recovery.

### 2026-10-19 — Completed B008.4; started B009.1

Added `DeltaRsTableService.drop_table()`. External drops remove only local metadata and preserve files. Managed cleanup opens the workspace `tables` directory without following symlinks and removes only the canonical UUID child before atomically deleting metadata and its scoped mutation history. Unfinished operations block removal. A real child process was killed after physical cleanup but before metadata removal; after restart, repeating the drop finalized the registration. The focused local suite passed 65 checks; strict mypy, Ruff, and formatting passed. ADR 0015 and `artifacts/conformance/B008.4-ownership-safe-removal.md` record the behavior and limits. B008 is complete; B009.1 is active for reusable external catalog/relation hooks in Sparkless.

### 2026-10-19 — Completed B009.1; started B009.2

Added and tested Sparkless' generic `ExternalCatalogProvider` descriptor/lookup contract and native Delta relation reads through `table()`, `DataFrameReader.table()`, and SQL. Provider errors remain explicit, schema is checked, multipart misses do not bind to unrelated local tables, and new sessions do not inherit the resolver. The full `make check-crate CRATE=robin-sparkless-polars` gate passed (362 Polars unit tests, all integration binaries, and 7 doctests). Commit `c81e97699aae7d967404d3de342146b6428afa0c` is in open Sparkless PR #1692; the API is not in published 4.13.4. No Databricks account or `.env` was used. B009.2 is active for delegated writes and typed interchange.

### 2026-10-19 — Completed B009.2–B009.3 and B009; blocked B010.1

Extended Sparkless PR #1692 with delegated Rust `saveAsTable()` append/overwrite and SQL `INSERT INTO`, typed Arrow IPC transfer, writer mode/options, validated commit receipts, strict external Delta snapshot reads, and error/no-fallback tests. `make check-crate CRATE=robin-sparkless-polars` passed at commit `4923b857b0208a5d81116e7782f17110d4558eb6`, including formatting, audit, deny, Clippy, unit tests, integration binaries, and doctests. The exact source revision is pushed to the open PR but no released Sparkless artifact contains it. Inspection found that the Sparkless PyO3/Python `SparkSession` does not expose provider registration, so BrickHouse's Python table service cannot supply the new Rust trait. B009.2–B009.3 evidence is in `artifacts/conformance/B009.2-sparkless-write-delegation.md` and `artifacts/conformance/B009.3-sparkless-hook-isolation-and-availability.md`; B010.1 is blocked pending a tested Python binding and callback ownership contract. No Databricks account or `.env` was used.

### 2026-10-19 — Completed B013.1; started B013.2

Added `brickhouse.runtime.run_python_worker()` as an internal process primitive. It resolves the interpreter/script and cwd, disables shell execution, passes a canonical 16 KiB-bounded immutable JSON context, starts without inherited credentials or import paths, drains stdout/stderr concurrently with explicit byte limits, and returns the actual exit code and truncation metadata. Ten real-child-process tests passed; Ruff, formatting, and strict mypy passed. It does not provide CLI/job routes, authentication, persistence, timeout/cancellation, or descendant reaping. Evidence is in `artifacts/conformance/B013.1-python-worker-launch.md`; B013.2 is active for scoped worker service authentication.

### 2026-10-20 — Completed B043: serverless and classic compute planning

Reviewed current official Databricks documentation/APIs and the pinned SDK 0.153.0 source for serverless/classic Python compute, runtime/access/environment differences, Connect analysis behavior and SQL warehouse types. Document 17 and ADR 0016 add explicit 0.1.0 execution criteria, five bounded children, four planned workload/environment capabilities, and later environment/access/retry, streaming/pipeline and Connect gates. Resource simulation cannot substitute for actual jobs or queries. The existing 68 children and original requirements/dependencies are retained; initial implementation now has 73 required children plus B043 research. The account's prior SQL capture cannot verify Python or classic behavior. Research and planning validation evidence are retained in [the B043 report](artifacts/conformance/B043-serverless-classic-plan.md) and planning/validation.json. No personal-account request, credential access, runtime change or Sparkless rebuild was performed for this planning slice; B013.2 remains the active implementation task.

### 2026-10-20 — Completed B044: phase 0.1 delivery refinement

Refined document 16 into concrete work lanes, a seven-Python/ten-notebook mode corpus, three warehouse read paths, selected local limits and 27 planned acceptance families with evidence owners. Added early connector transport research B045 and decomposed Python provider work B046.1–B046.3. B011.6, B014.6 and B017.5 now own real workload verification after resource/context contracts; B020.4 qualifies Terraform resources and B020.5 qualifies bundle workload results. Existing parent criteria/prerequisites, task states and evidence are retained; no runtime capability is promoted. The ledger has 126 tasks, 46 work packages and 80 required children. ADR 0017 and the B044 report record the decision; planning/validation.json records consistency checks. B013.2 remains the active implementation task, and B010.1 remains blocked until the Python bridge is available. No runtime test, account request, dependency installation or upstream modification is part of this planning refinement.

### 2026-10-20 — Verified Sparkless 4.14.0; reported nullable-struct blockers

Pinned the published 4.14.0 developer candidate and verified release/source/wheel provenance without an upstream rebuild or edit. B046.1 records the actual public Python API and ownership limits; B046.2 passed 56 upstream provider tests plus the earlier typed/path repros. Fourteen selected checks used BrickHouse's journaled Delta service for real commits, exact versions, isolation, reopen and process interruption. The separate nullable-struct probe failed five comparisons (one control passed); filed robin-sparkless #1694 for ingestion validity loss and #1695 for collected all-null-field structs. B046.3, parent B046 and B010.1 stay blocked on those exact contracts. ADR 0018 permits verifying published artifacts while retaining every behavioral gate. Capability states remain unchanged; no production adapter or Databricks compatibility is claimed. Independent B013.2 and B045 were ready when this earlier verification began. No `.env` or Databricks account was accessed. See [the conformance report](artifacts/conformance/B046.3-sparkless-4.14.0-provider.md) and planning/validation.json for checks.


### 2026-10-20 — Completed B013 worker supervision; started B045

Completed B013.3 and its parent. The internal runner now enforces timeout/cancellation, terminates same-group POSIX descendants, reaps the owned process, revokes scoped credentials, redacts bearer output, invokes safe staging cleanup, and journals actual worker results in metadata schema 5. A two-attempt real-process check confirms environment/import/context isolation; workspace migrations from schema 3 and local table regressions pass. The focused worker/workspace/table suite passed 70 tests; Ruff, formatting, strict mypy, planning validation and diff checks passed. ADR 0020 and [the B013.3 report](artifacts/conformance/B013.3-worker-supervision.md) document behavior and limits. There is still no public job API, user log retrieval, retry or active-run restart reconciliation; those are B014. B045 is active to probe the pinned SQL connector/kernel through a local HTTPS capture boundary. No Databricks account or `.env` was accessed.


### 2026-10-20 — Completed B045; started B014.1

B045 installed and hash-verified the pinned SQL connector 4.6.0/kernel 1.1.0 artifacts and sent a real request to a local HTTPS capture boundary with strict CA/hostname validation and synthetic Bearer auth. The capture observed `POST /api/2.0/sql/sessions`, `execution_mode=FAST`, and the warehouse ID derived from `http_path`; the local boundary returned 401 before SQL execution. No actual SQL result or Databricks compatibility is claimed. ADR 0021 contains the private custom-CA and loopback port adaptations; the exact wheels, sanitized request/error, and next B011/B021 changes are in [the B045 report](artifacts/conformance/B045-sql-connector-loopback.md). The B045 task is done and C031 remains planned. B014.1 is now the sole ready implementation task and is active for job submission, attempt, and state persistence. No `.env`, real credential, Databricks host, or workspace was used.

### 2026-10-20 — Completed B014.1; started B014.4

Added an internal `WorkspaceContext.jobs` repository and metadata schema 6 for immutable job definitions, submission snapshots/idempotency tokens, bounded acyclic Python task graphs, task attempts and truthful state transitions. SQLite checks/triggers protect terminal outcomes and history; interrupted work retains a null exit code, and undispatched tasks remain pending. Twenty-one focused repository/workspace/migration checks and all 105 unit regressions passed, with Ruff, formatting, strict mypy, planning validation and `git diff --check` passing. ADR 0022 and [the B014.1 report](artifacts/conformance/B014.1-durable-job-state.md) record the state contract. This is not an SDK job route or execution dispatcher; C020/C021 stay planned. B014.4 is the only ready implementation task and is active for classic compute identity/lifecycle. No `.env`, Databricks account, cloud compute or Spark/JVM execution was used.

### 2026-10-20 — Completed B014.4; started B014.5

Added schema 7 for simulated all-purpose and run-scoped classic compute identity, consumer tracking, legal state transitions, durable generation/invalidation events, and logical permanent deletion. The loopback API implements the pinned Databricks SDK 0.153.0 cluster create/get/list/edit/start/restart/terminate/permanent-delete operations. Local runtime/access mappings are explicit; unmapped DBR/access settings, Photon, JVM configuration, cloud-specific settings, and policies fail before persistence. Shared compute starts with its first consumer and terminates after every planned consumer releases; all-purpose compute persists and can autostart, with controlled-clock idle termination. Six focused compute tests and all 111 unit tests passed. Full Ruff check/format, strict mypy (36 source files), planning validation, and `git diff --check` passed. No `.env`, personal Databricks account, cloud compute, Spark, or JVM was used. C057 records only simulated resource metadata; workload support remains planned. ADR 0023 and [the B014.4 report](artifacts/conformance/B014.4-classic-compute-lifecycle.md) document the contract. B014.5 is active for serverless/classic execution-context resolution.

### 2026-10-20 — Completed B014.5; started B014.2

Added immutable pre-dispatch context resolution for pinned serverless environment and classic inline/shared/existing compute selectors. The resolver validates exact local environment/dependency mappings, runtime/access aliases, task and notebook shapes, normalized `/Workspace/...` paths, serverless ANSI/UTC defaults, retry settings, timeout bounds, and unsupported selectors/configuration before dispatch. The serverless initial profile requires `disable_auto_optimization=true`; no environment install, SDK route, workspace-file lookup, or worker execution is claimed. Eleven focused resolver tests passed; Ruff and strict mypy for jobs/runtime passed. Capability C058 records only partial internal resolution; C053–C056 stay planned. ADR 0024 and [the B014.5 report](artifacts/conformance/B014.5-execution-context-resolution.md) record the slice. No `.env`, Databricks account, cloud compute, Spark, or JVM was used. B014.2 is now ready to add workspace file transport and SDK Python job routes.

### 2026-10-20 — Completed B014.2; started B014.3.1

Added safe local Python workspace-file storage and pinned-SDK multipart upload, then composed single-task Python job create/submit/poll/output routes over fresh local workers. Schema 8 persists immutable, bounded stdout/stderr for terminal attempts and upgrades schema 7 transactionally. The actual `databricks-sdk==0.153.0` loopback integration and repository migration/output checks passed within the 126-test unit suite; Ruff, formatting, strict mypy (41 source files), planning validation and `git diff --check` passed. C020 records this as a partial local Python profile. This does not provide Spark/Sparkless or Delta effects, multi-task dispatch, retries, concurrency above one, compute consumer lifetimes, or restart reconciliation. ADR 0025 and [the B014.2 report](artifacts/conformance/B014.2-workspace-files-sdk-jobs.md) record behavior. No `.env`, Databricks account, cloud compute, Spark, or JVM was used. B014.3 was decomposed into DAG/failure propagation (active B014.3.1), retries/concurrency (B014.3.2), and interrupted-run recovery (B014.3.3).

### 2026-10-20 — Completed B014.3.1; started B014.3.2

Composed the durable DAG contract into the SDK worker flow. Job creation accepts up to ten Python script tasks; the service dispatches sequentially in stable topological order, marks failed dependents skipped with no attempt, and continues independent branches. The pinned SDK reports `UPSTREAM_FAILED`, and the overall run fails only after all eligible tasks have truthful terminal states. Two new loopback SDK tests plus repository dependency guards passed; the full 128-test unit suite, Ruff, format, strict mypy (41 source files), planning validation and diff checks passed. C020/C021 record a partial sequential DAG profile. Retry policy, concurrency above one, process-restart reconciliation, multi-task output retrieval and worker compute lifetimes remain unsupported. ADR 0026 and [the B014.3.1 report](artifacts/conformance/B014.3.1-dag-dispatch.md) record behavior. No `.env`, Databricks account, cloud compute, Spark or JVM was used. B014.3.2 is active for distinct retries and bounded job-run concurrency admission.

### 2026-10-20 — Split B014.3.2 before implementation

Separated automatic task retry behavior from job-run concurrency admission while preserving the original acceptance criterion. B014.3.2.1 is active for bounded retries, timeout policy and distinct attempt outputs; B014.3.2.2 follows for per-job concurrency and idempotent SDK admission. B014.3.3 remains dependent on completion of both slices. Planning validation records the updated nested task graph; no runtime code or tests changed in this planning split.

### 2026-10-20 — Completed B014.3.2.1; started B014.3.2.2

Implemented bounded per-task retries using the existing immutable execution context. Nonzero exits retry up to the validated limit; timeouts retry only with `retry_on_timeout=true`; configured delays are interruptible; and every attempt retains a distinct ID, observed outcome and immutable output. SDK task attempts are numbered from zero while repository attempts remain one-based. Local loopback tests passed retry-on-failure, retry-limit, configured-delay, timeout-default and enabled-timeout cases. The final suite passed 131 unit tests with Ruff, formatting, strict mypy, planning validation and diff checks. ADR 0027 and [the B014.3.2.1 report](artifacts/conformance/B014.3.2.1-retry-attempts.md) document scope and limits. This does not make external script side effects exactly once. No `.env`, Databricks account, cloud compute, Spark or JVM was used. B014.3.2.2 is active for run concurrency admission.

### 2026-10-20 — Completed B014.3.2; started B014.3.3

Added per-job `max_concurrent_runs` from one through four and a four-run process limit. Admission checks token replay first under the service lock, then enforces per-job and global capacity before run creation. Active and terminal token replays return the existing run; capacity and changed-token conflicts map to HTTP 409, so the pinned SDK surfaces `ResourceConflict` without retrying submission. The loopback tests cover per-job and process-wide limits, no-run capacity rejection, active/terminal idempotency, and changed-input conflict. All 133 local unit tests passed; Ruff, formatting and strict mypy passed. ADR 0028 and [the B014.3.2.2 report](artifacts/conformance/B014.3.2.2-run-admission.md) document the behavior. Process-restart reconciliation remains active B014.3.3. No `.env`, Databricks account, cloud compute, Spark or JVM was used.

### 2026-10-20 — Completed B016 and B020.1; started B020.2

Completed B014.3.3 restart reconciliation, B016.1–B016.3 worker-scoped DBFS/widgets/secrets/task values, and B020.1. The latter ran the actual Databricks CLI v1.21.0 against an ephemeral loopback server: source import, duplicate/missing-source errors, Python job create/submit/poll/output, and workspace cleanup passed. The macOS arm64 release ZIP hash is recorded in the [B020.1 report](artifacts/conformance/B020.1-databricks-cli-v1.21.0.md). The ordinary suite passed 165 tests with the opt-in CLI integration skipped; the explicit CLI test passed. Ruff, formatting, strict mypy (45 source files), planning validation, and `git diff --check` passed. C027 and C030 are partial for their bounded local subsets; no CLI authentication or remote Databricks compatibility is claimed. B020.2 is active for the minimal Terraform resource lifecycle. Sparkless blockers #1694/#1695 remain outside this work. No `.env`, Databricks credential/account request, cloud compute, Spark, or JVM was used.

### 2026-10-20 — Completed B047: emulator-first self-hosted planning

Preserved the JVM-free Databricks emulator as the primary roadmap through phases 0.1–0.9. Added phase-specific architecture preparation, document 18, ADR 0034/D021 and a separate SH.0–SH.6 deployment track with unassigned versions. B049 assesses stability after B042; every deployment implementation B050–B055 is transitively gated on it. B048 is a bounded review of the real local workflow before B023, with no deployment infrastructure added to 0.1. Existing 85 child tasks and emulator acceptance criteria/evidence remain; new C059–C064 are planned only.

Planning validation passed for 140 tasks, 55 top-level packages, 64 capabilities, nine emulator phases and seven later stages. Ten focused planning-validator tests passed, including eight negative boundary cases and future deployment-child inheritance; Ruff check/format, validator mypy and git diff checks passed. The [B047 report](artifacts/conformance/B047-self-hosted-roadmap.md) records preservation checks and the concurrently completed Terraform work separately. No emulator/deployment runtime tests, account requests, dependency changes or cloud workloads were run for B047. Continue the existing ready emulator work; B048 waits for B018/B022, and self-hosting waits for B042/B049.


### 2026-10-20 — Completed B020.2 and B020.3; no unblocked phase 0.1 leaf remains

Recorded the Terraform Core 1.16.6/provider 1.137.0 workspace-file and existing-cluster Python-job lifecycle, then completed the Databricks CLI v1.21.0 classic single-task Python bundle validate/deploy/run/destroy profile. The bundle test exercises actual local Python worker output, synthetic current-user identity, simulated new-cluster metadata, explicit notebook/pipeline errors, cleanup of the managed `.bundle` tree, and job deletion with immutable run history retained. Schema 12 adds deletion tombstones. ADR 0036, the B020.3 report, capability C030, task ledger and public implementation docs describe the bounded profile.

Focused profile/migration checks passed (62 tests); the full local suite passed (184 passed, three opt-in integrations skipped). Ruff check/format, strict mypy across 46 source files, planning validation (140 tasks, 64 capabilities, 1019 local links; zero currently ready implementation leaves), and `git diff --check` passed. After schema 12 and the cross-version Terraform tombstone fix, the pinned CLI workspace and bundle integrations each passed, and the Terraform provider plus Terraform job API checks passed (3 tests). The explicit integrations use synthetic tokens and loopback only. No `.env`, personal account, cloud compute, Spark, JVM or publication was used. B020.4 waits on B011; B011 depends on B010, whose child B010.1 and B046.3 are blocked by the Sparkless nullable-struct issues.

### 2026-10-20 — Completed B056: native Sparkless cluster plan

Added [document 19](docs/plan/19-sparkless-cluster-execution.md), [the upstream contracts/source inventory](docs/upstream/sparkless-cluster-contracts.md) and ADR 0037/D023 for optional SC.0–SC.5: feasibility, typed partition planning, two-host exchanges/shuffle, fenced recovery/provider sinks, a qualified standalone candidate and BrickHouse integration. Inspected clean Sparkless 4.14.1 source read-only; local traits/IR, buffered provider payloads and no-op partition methods are distinguished from proposed contracts. No upstream code or current runtime behavior changed in this planning slice.

B057–B062 remain planned post-B049 epics requiring decomposition. B062 requires both qualified Sparkless artifacts B061 and the isolated remote-worker foundation B053; current emulator and SH outcomes retain their requirements and no SC dependency. C065–C067 are planned only. All 19 focused planning-validator tests, lint/format, validator mypy and diff checks passed; the [B056 report](artifacts/conformance/B056-sparkless-cluster-plan.md) records 147 tasks, 62 top-level packages, 85 unchanged emulator children, 67 capabilities and preservation checks. Existing concurrent B010 integration progress was retained. No emulator/cluster runtime tests, credentials/account calls, dependency installation, cloud provisioning or publication were performed for B056. Continue ready local emulator work; cluster implementation waits for stability and its actual prerequisites.

### 2026-10-20 — Completed B010 and B056 planning; phase 0.1 conformance audit

Completed B010.3 by applying resolved execution contexts to internal Sparkless sessions and testing two coexisting owner-thread sessions with separate namespaces, configuration, temporary views and typed callable UDF outputs. The serverless profile receives resolved ANSI/UTC settings; classic-standard uses the mapped `spark35-local-v1` alias and requires explicit ANSI/timezone values. Unsupported classic defaults and unmapped runtimes fail clearly. B010.1–B010.3 and parent B010 are done; SQL, job/notebook worker session integration and broader profile coverage remain incomplete.

Completed B056 as planning-only source review of clean Sparkless 4.14.1 commit `ad1b9445bad609ac4bf74d2cf3ad2f68832ff49b`. Added optional SC.0–SC.5 stages after B049 and task/capability/validator coverage for B057–B062 and C065–C067. No Sparkless source, rebuild, cluster runtime, account, or cloud resource was used.

Verification: 19 focused Sparkless/execution-context tests passed; the full suite passed 201 tests with three opt-in CLI/Terraform integrations skipped and no PyO3 cross-thread-drop warning. Ruff check/format, strict mypy across 49 source files, `pip check`, package build, planning validation (147 tasks, 62 work packages, 85 required phase 0.1 children, 67 capabilities, 9 release phases, 7 SH stages, 6 SC stages, 1,121 Markdown links), and diff checks passed. The close path now releases its native session on the owner thread. Test commands had `DATABRICKS_TOKEN` and `DATABRICKS_HOST` unset.

### 2026-10-20 — Completed B063: research-backed native component reuse

Added [document 20](docs/plan/20-open-source-reuse.md), [the exact public source report](docs/upstream/oss-reuse-inventory.md), ADR 0038/D024 and [planning/oss-reuse.json](planning/oss-reuse.json). Research covers 22 Python distribution snapshots, two native crates and a pinned UC source tree. Decisions retain the current native core; prefer direct Jupyter components and a full optional MLflow service; separate incompatible dbt clients; reuse UC contracts without its Java server; qualify sharing's native wrapper/Arrow/wheels; and evaluate optional Iceberg/Kernel and Ballista/DataFusion only in their existing later slices.

Strengthened 11 still-planned epics with concrete environment/authority/native/error/recovery qualification criteria, while preserving all original task criteria, exact prerequisite edges, child lists and prior evidence. All 85 alpha children and 67 capability states/tested versions/test IDs/evidence remain; research limitations were appended only to planned capabilities. No new alpha workload, runtime package, source checkout change, backend selection or compatibility promotion occurred. Existing emulator-first and B049/B061/B053 barriers are unchanged.

The explicit public artifact verifier passed 26 archive/source checks; eight recorded dependency comparisons and 28 focused offline planning/metadata tests passed. Planning validation, Ruff/formatting, strict mypy for the validator/verifier and diff checks passed. See [the B063 report](artifacts/conformance/B063-open-source-reuse.md). No candidate was installed/imported/executed/built, no `.env` or personal credentials were read, and no Databricks workspace/cloud workload or publication was performed. Continue ready emulator work; optional candidates still require actual owner-task runtime qualification.


### 2026-10-20 — Completed B011.2; started B011.6

Added metadata schema 14 for durable SQL statement requests, outcomes and inline JSON results, plus the pinned Databricks SDK statement submit/status/result-chunk routes. Statements use the B011.1 Sparkless and shared catalog/Delta services, validate a persisted simulated running warehouse, preserve schema and scalar/nested JSON_ARRAY values, and recover terminal results after server restart. Synchronous and pollable `0s` requests, typed SQL failures, unsupported format rejection, and 10,000-row/8 MiB result bounds are covered. The sanitized B005.3 reference fixture informed decimal/date/timestamp/null/array checks; no new personal-account request was needed.

The pinned SDK and related local regressions passed 63 tests with credentials unset. Ruff check/format and strict mypy passed for the changed source; planning validation and diff checks are being rerun after task/capability synchronization. The local API remains unauthenticated and does not claim remote SQL parity. B011.6 is active; B015.1 remains independently ready.


### 2026-10-20 — Completed B011.6; started B011.3

Bound SDK SQL statement execution to persistent warehouse identities and durable admission leases. Statements through all three simulated SQL warehouse profiles read the same committed Delta data. Per-warehouse/workspace admission, FIFO promotion, queue overflow, missing/stopped/deleted resources, stop cancellation, restart reconciliation, and connector session-open warehouse identity validation are covered; released connector execution remains B021. The focused regression set passed 71 tests with Databricks credential variables unset. Ruff, format and strict mypy passed for changed Python modules. See [B011.6 evidence](artifacts/conformance/B011.6-sql-warehouse-statement-binding.md). B011.3 is active for parameter, hybrid wait-timeout and cancellation contracts; B011.4 and released connector query/result integration remain open.

### 2026-10-20 — Completed B011.3.2; blocked B011.3.3; started B015.1

Implemented Statement Execution hybrid wait behavior for the selected local profile: the default 10-second wait, immediate `0s` response, and valid 5–50 second waits with `CONTINUE`. A managed continuation owns each statement, atomic claim protects exact-once dispatch, and the API returns the persisted state and ID when a wait expires. Shutdown cancels queued continuations and retains workspace ownership until active Sparkless calls finish. Invalid wait values/dispositions fail before admission. Active cancellation and `on_wait_timeout=CANCEL` remain explicitly unsupported because Sparkless has no public active-query cancellation contract; tracked in [upstream issue #1705](https://github.com/eddiethedean/robin-sparkless/issues/1705).

The focused SQL API/service suite passed 32 tests; Ruff check/format and strict mypy passed with Databricks credentials unset. See [B011.3.2 evidence](artifacts/conformance/B011.3.2-sql-wait-timeout.md). Phase 0.1 has 61 done, 25 planned, one in progress and one blocked child task. B015.1 is active for worker-scoped `pyspark` import compatibility.

### 2026-10-21 — Completed B015.1.1; started B015.1.2

Added a child-process-only lazy import bridge for the selected `pyspark.sql`, functions, types, and window modules backed by Sparkless 4.14.1. It rejects conflicting PySpark packages and unsupported namespaces and leaves the parent interpreter and other workers' module state unchanged. Fixing the worker venv symlink handling was necessary so imports run under the selected BrickHouse environment rather than the base interpreter. Five worker-import cases and eleven worker regressions passed; Ruff, formatting, strict mypy, and planning validation are being checked. See [B015.1.1 evidence](artifacts/conformance/B015.1.1-worker-pyspark-imports.md). No additional upstream Sparkless feature request was needed for this import slice; #1705 and #1706 remain open contracts. Phase 0.1 has 62 done, 26 planned, two in progress, and one blocked child task. B015.1.2 is next for profile-bound Sparkless sessions and owner-thread cleanup.

### 2026-10-21 — Completed B015.1.2; started B015.1.3

The worker now creates a profile-bound Sparkless session when code imports `pyspark.sql`, applies the resolved serverless/classic settings before user operations, and closes the session on its owner thread. The worker facade supports the selected builder, local DataFrame construction/range, and bounded read-only SQL while explicitly rejecting unsupported settings and JVM/RDD APIs. It validates profiles lazily so plain Python jobs keep working without Sparkless or classic Spark configuration. Forty-six focused worker/auth/secrets and seven-profile SDK loopback checks passed; Ruff, formatting, strict mypy, and planning validation are being checked. See [B015.1.2 evidence](artifacts/conformance/B015.1.2-worker-spark-session.md). B015.1.3 is next for attempt-authenticated table descriptors and shared committed Delta reads.

### 2026-10-21 — Completed B015.1.3; started B015.2.1

Added an attempt-authenticated, read-only Sparkless external-catalog provider to managed workers. The parent resolves registered names through the catalog and DeltaRsTableService, returning stable IDs, canonical locations, schema and exact committed version; the worker attaches Sparkless 4.14.1's public descriptor. The pinned SDK loopback job read committed Delta rows through `SparkSession.table()` and read-only SQL, rejected missing names and an actual unregistered Delta path, rejected worker writes, and retained table version 0. Fifty focused worker/auth/secrets/task-value/seven-profile SDK tests passed with Databricks credentials unset; Ruff, formatting, strict mypy and planning validation passed. See [B015.1.3 evidence](artifacts/conformance/B015.1.3-worker-registered-tables.md).

Checked upstream `robin-sparkless`: existing open issues [#1705](https://github.com/eddiethedean/robin-sparkless/issues/1705) and [#1706](https://github.com/eddiethedean/robin-sparkless/issues/1706) cover the active-cancellation and bounded-Arrow gaps. Sparkless 4.14.1's public catalog API covers this worker read slice, so no new issue was warranted. B015.2 is decomposed into the managed-worker import/factory boundary and read-only DeltaTable methods; B015.2.1 is active. Mutation builders remain outside the selected API.

### 2026-10-21 — Completed B015.2 and parent B015

Added a worker-only `delta.tables.DeltaTable` facade with authenticated registered-name/path factories, `isDeltaTable`, and read-only `toDF`/`alias`. It rejects conflicting Delta Lake packages, unsupported namespaces, unregistered paths, and mutation/builders; an SDK loopback worker read committed table rows and left version 0 unchanged. Fifty-three focused worker/SDK tests passed with Databricks credential variables unset; Ruff check/format and strict mypy passed. See [B015.2 evidence](artifacts/conformance/B015.2-worker-delta-table.md). No new Sparkless feature gap surfaced: #1705 and #1706 remain the open cancellation and bounded-Arrow issues. Planning validation passes for 93 child tasks (68 done, 23 planned, 2 blocked), and B017.1 is next ready.

### 2026-10-21 — Completed B017.5 and parent B017

Completed the selected pinned-SDK notebook task profile: seven compute selectors ran actual local kernels against shared committed Delta data; three marked source-export profiles, notebook/job environment fallback, and base/run-now parameter precedence passed. Restart/reset, termination, and timeout during active kernels reaped supervisors and descendants, started fresh replacement state, and preserved prior Delta commits. The schema-16 migration preserved an existing job/run, accepted notebook tasks and passed its foreign-key check. Existing B017.3 parent-cell cancellation evidence completes the cancellation case.

The focused suite passed 87 tests. Ruff check/format, strict mypy across 68 source files, `pip check`, planning validation, and `git diff --check` passed. Databricks credentials were unset; no personal account or cloud compute was used. See [B017.5 evidence](artifacts/conformance/B017.5-notebook-compute-interruption.md). Phase 0.1 remains incomplete: 18 child tasks remain planned, two are blocked, and no implementation leaf is currently ready. #1705–#1707 are existing Sparkless issues; #1705 blocks active SQL cancellation.

### 2026-10-21 — Completed B018.1.3; started B018.1.4

Ran the two-task SDK orders workflow across all seven selected serverless and classic-standard/classic-dedicated job selectors. Producers read parameters/widgets and a granted secret, appended typed rows through the authenticated shared Delta service, and published task values; dependent workers read those values and the committed rows. Task output, schema/data through Delta version 7, and a fresh Sparkless reader matched. Serverless had no classic resource, per-task new resources terminated on release, shared resources stopped after both consumers, and existing all-purpose identities stayed running. Classic lifetimes are simulated. The focused profile test passed with Sparkless 4.15.0 and the pinned SDK 0.153.0; credential variables were unset and only local loopback was used. Updated C020, C053 and C054 to partial for these locally verified subsets while keeping cloud/runtime compatibility and access-control claims out of scope. See [B018.1.3 evidence](artifacts/conformance/B018.1.3-g2-compute-profiles.md). B018.1.4 now owns the three local warehouse readers for the same G2 commit.

### 2026-10-21 — Completed B018

Completed B018.1 and B018.2, then recorded the parent G2 acceptance evidence in [the aggregate report](artifacts/conformance/B018-sdk-driven-workflow.md). The selected SDK Python workflow covers dependency failure, retries, cancellation, isolation, all seven job compute selectors, and serverless/pro/classic warehouse reads of a job-produced Delta commit. The separate notebook-extra lane exercises actual notebook outputs/errors and SDK NotebookTask variants across the seven compute selectors and three source-export profiles. A built base wheel passed 10 selected G2 cases without notebook dependencies; the separate notebook-extra wheel passed 36 cases. Both used Python 3.11.15 with Sparkless 4.15.0 and loopback only; Java and Databricks credentials were unavailable. `uv build`, `uv lock --check`, both `uv pip check` runs, planning validation, Ruff check/format, and diff checks passed. Classic/warehouse resources remain simulated; broader phase 0.1 behavior remains open. B019.1 and B020.4 are ready next.

### 2026-10-21 — Started B019.1 SQL corpus decomposition

Claimed B019.1 after B011 and B043 completed, then decomposed its broad SQL semantics acceptance into seven bounded children: frozen fixture/projection/filter; aggregates/decimals; dates/timezones; collections; joins; DDL/error categories; and serverless/classic profile behavior. Preserved B019.1's parent mapping to B019 criteria 1 and 4 and added an explicit second acceptance criterion for profile-specific ANSI/timezone, configuration/view/cache, and error behavior. B019.1.1 is active for the typed fixture and scalar query cases; B020.4 remains independently ready. No Databricks account or cloud query was used for this planning slice.

### 2026-10-21 — Completed B019.1.1 and started B019.1.2

Added a versioned typed orders fixture and a real local Delta/Sparkless regression for exact projection/filter/null/coercion output. The SQL service file passed 17 tests; capture-isolation passed 5; Ruff check/format and diff checks passed. The optional Databricks reference probe did not return a usable result (`BAD_REQUEST`); the only existing serverless warehouse was restored to `STOPPED`, with no data objects created. C018 remains planned; the single subcase is recorded without promoting broad SQL support. B019.1.2 is next for aggregate and decimal results.


### 2026-10-21 — Completed B019.1.2–B019.1.4; started B019.1.5

Completed the aggregate/decimal corpus with exact grouped and empty-input schemas and an invalid-cast outcome; completed date/time cases with UTC normalization, exact-boundary behavior, and the unadapted aware-column/naive-literal error; and completed typed nested array/struct projection with explicit unsupported collection-expression errors. Reported the two reproduced Sparkless 4.15.0 SQL gaps as [#1714](https://github.com/eddiethedean/robin-sparkless/issues/1714) and [#1715](https://github.com/eddiethedean/robin-sparkless/issues/1715). The SQL service and capture-isolation suites passed 25 tests using local Delta/Sparkless with credential variables unset. Ruff check/format, planning validation (170 tasks, 107 child tasks, 67 capabilities, 1,547 links), and diff checks passed. The earlier bounded aggregate reference statement returned `BAD_REQUEST`; B019.1.3 reused the existing UTC literal oracle, and B019.1.4 made no remote request. C018 remains planned. B019.1.5 is active; B019.1.6, B019.1.7, and B020.4 remain ready.


### 2026-10-21 — Completed B019.1.5; started B019.1.6

The selected aliased inner join passed against two real local Delta tables with nullable keys, duplicate matches, exact output schema, and explicit ordering. The test also verifies typed rejection for outer, self, multi-key, and multiple joins. It exposed a BrickHouse planner bug: explicit SQL `INNER JOIN` was rejected because SQLGlot represents `kind="INNER"`; the planner now accepts that explicit inner kind while keeping unsupported joins rejected. The focused join test passed; the SQL service and capture-isolation suites passed 26 tests, and Ruff check/format, planning validation (167 Markdown files, 1,550 links), and diff checks passed. B019.1.6 is active for DDL/error categories; B019.1.7 and B020.4 remain ready.


### 2026-10-21 — Completed B019.1.6; started B019.1.7

The versioned DDL/error case passed schema and table creation, exact empty Delta schema/version, CTAS rows/schema/version, DROP and `IF EXISTS`. Duplicate table/schema, missing table, invalid column type, unsupported format, and multi-statement errors are typed; each failure left catalog/table registrations and source Delta state unchanged. The focused DDL regression passed locally. B019.1.7 is active to qualify serverless/classic semantic profiles; B020.4 remains ready.


### 2026-10-21 — Completed B019.1; decomposed B019.2

All seven B019.1 SQL corpus children passed local conformance and parent B019.1 is done. The final affected suites passed 39 tests with Databricks credential variables unset; Ruff check/format, diff checks, and planning validation passed. C018 remains planned, Connect/deferred analysis remain unsupported pending B036, and Sparkless issues #1714/#1715 retain timestamp comparison and nested-field SQL gaps. B019.2 was split into UPDATE/DELETE, MERGE, DESCRIBE/HISTORY, and VERSION AS OF children before implementation; B019.2.1 is now active.


### 2026-10-21 — Completed B019.2.1; started B019.2.2

Implemented selected one-table SQL UPDATE and DELETE through the declared delta-rs provider and shared table service. The journal records exact affected-row metrics and committed versions; zero-row operations are terminal no-ops without transaction-version gaps, confirmed pre-commit provider failures are terminal, and uncertain outcomes remain blocked for reconciliation. A pinned SDK loopback UPDATE is visible to a following SELECT, and the schema-18 migration preserves schema-17 journal records. The focused SQL/provider/workspace suite passed 101 tests with Sparkless 4.15.0, delta-rs 1.2.1, Python 3.11.14, and Databricks credential variables unset. Ruff check/format, strict mypy, planning validation, and `git diff --check` passed. No Databricks workspace or cloud compute was used. C018 remains planned; MERGE, DESCRIBE/HISTORY, and VERSION AS OF remain open. See [B019.2.1 evidence](artifacts/conformance/B019.2.1-sql-update-delete.md). B019.2.2 is active for bounded single-key MERGE.


### 2026-10-21 — Completed B019.2.2; started B019.2.3

Implemented one bounded SQL MERGE profile for registered Delta source/target tables with a single equality key, unconditional matched update, and explicit unmatched insert. The source and target schemas must match; the service rejects duplicate or null source keys before target journaling, fingerprints the pinned source snapshot, and executes one native delta-rs commit through the shared table service. Delta history records exact inserted/updated metrics and operation identity; schema-19 migration preserves prior schema-18 journal rows. Exact rows/schema and fresh Sparkless visibility passed. The combined SQL/provider/workspace suite passed 110 tests with Sparkless 4.15.0, delta-rs 1.2.1, Python 3.11.14, and Databricks credentials unset. No Databricks workspace or cloud compute was used. Ruff check/format, strict mypy, planning validation, and diff checks passed for this slice. C018 remains planned; MERGE variants outside this profile, DESCRIBE/HISTORY, and VERSION AS OF remain open. See [B019.2.2 evidence](artifacts/conformance/B019.2.2-sql-merge.md). B019.2.3 is active for SQL DESCRIBE and Delta history utilities.


### 2026-10-21 — Completed B019.2.3; started B019.2.4



### 2026-10-21 — Completed B019.2.4; started B019.3

Implemented literal non-negative integer SQL VERSION AS OF using table-ID-scoped version-pinned Sparkless descriptors and pre-dispatch exact Delta snapshot reads. Direct service, direct Sparkless, and SQL results match schema and historical rows. Invalid and unsupported selectors reject; future and vacuum-expired versions raise `SnapshotUnavailableError` without falling back to latest. The combined SQL/provider/Sparkless adapter/worker regression suite passed 128 tests with Sparkless 4.15.0, delta-rs 1.2.1, Python 3.11.14, and Databricks credentials unset. Ruff check/format, strict mypy, planning validation, and `git diff --check` passed. Timestamp and parameterized selectors remain unsupported; C018 remains planned and no remote SQL parity is claimed. No Databricks workspace or cloud compute was used. See [B019.2.4 evidence](artifacts/conformance/B019.2.4-sql-version-as-of.md) and [B019.2 aggregate evidence](artifacts/conformance/B019.2-sql-mutations-history-snapshots.md). B019.2 is complete; B019.3 is active for executor routing and capability limits.


### 2026-10-21 — Completed B019.3; started B020.4

Recorded the predetermined SQL route matrix: SELECT expressions execute through Sparkless 4.15.0; catalog/schema metadata uses `CatalogService`; registered Delta table state and mutations use `DeltaRsTableService`; DESCRIBE/HISTORY read provider state; and integer VERSION AS OF pins the same registered snapshot for Sparkless. The selected type/utility envelope and explicit unsupported maintenance statements are documented. Five new regression cases reject SHOW, EXPLAIN, ANALYZE, OPTIMIZE and VACUUM without changing registrations or Delta versions; the existing CTAS execution-error case proves query errors propagate before target registration. The focused SQL/provider/Sparkless suite passed 74 tests, with Databricks credentials unset. No DuckDB executor, separate managed-table copy, remote workspace, or cloud execution is used or claimed. Sparkless 4.15.0 is already pinned in `pyproject.toml` and `uv.lock`. See [B019.3 routing evidence](artifacts/conformance/B019.3-executor-routing.md). B019 and its children are complete; dependency-ready B020.4 is now active.


### 2026-10-21 — Completed B020.5 and parent B020; started B021.1

Completed the pinned CLI serverless/classic bundle workload slice. The serverless-local Python bundle validated, deployed, appended typed G2 rows 42 and 43, then updated/redeployed with the same public job ID and retained immutable run history. A classic-standard notebook read and emitted those exact rows; a pinned SDK Pro/serverless-enabled warehouse query returned the exact schema and values. Unsupported task/selector/pipeline cases failed without state mutation, and destroy removed bundle trees/jobs while preserving runs. Local identity and infrastructure allocation remain simulated; the serverless fixture requires `disable_auto_optimization: true`.



### 2026-10-21 — Completed B021.1; starting B021.2

The released SQL connector 4.6.0 with kernel 1.1.0 reached the actual BrickHouse API over verified loopback TLS using the isolated synthetic bearer credential. Its initial `/api/2.0/sql/sessions` request resolved the local warehouse and received HTTP 501 because session lifecycle had not yet been implemented; no statement followed and no rows were returned. The opt-in integration was then extended as B021.2.1 to verify actual session create/close. B021 parent acceptance remains open, and B021.2 was decomposed before implementing sessions. See [B021.1 evidence](artifacts/conformance/B021.1-sql-kernel-brickhouse-loopback.md). No `.env`, account request, cloud compute, Spark, JVM, commit, or push was used.


### 2026-10-21 — Completed B021.2.1; started B021.2.2

Implemented ephemeral connector sessions bound to persistent warehouse IDs and validated catalog/schema state. The actual pinned connector creates a session over strict loopback TLS and closes it with a bodyless DELETE carrying `warehouse_id` in the query string. Unsupported per-session settings fail with 501; missing/stopped warehouses, cross-warehouse IDs, and unknown sessions retain explicit errors. The actual connector integration passed 1 test, the SQL statement API regression passed 28 tests, and Ruff/format plus strict mypy passed for changed modules. C031 is partial for transport/session lifecycle only; SQL execution and results remain open. See [B021.2.1 evidence](artifacts/conformance/B021.2.1-sql-kernel-session-lifecycle.md). No `.env`, account request, cloud compute, Spark, or JVM was used. B021.2.2 is active for connector statements, parameters, schema, and result fetching.


### 2026-10-21 — Completed B021.2.2; started B021.2.3

The released SQL connector's explicit SEA mode now executes a bounded named-INT query against the local Statement Execution API, returns the expected `int` description and row `42`, closes the terminal result, then closes its session. The same integration retains the actual kernel FAST session create/bodyless-close check over strict loopback TLS. BrickHouse binds statement requests to the supplied live session and inherits its catalog/schema; omitted SEA session execution mode defaults to FAST. Terminal statement DELETE releases retained result chunks and reports `CLOSED` while preserving statement history. The connector integration passed 1 test and the full SQL statement API suite passed 28 tests with Databricks credential variables unset; Ruff check/format and strict mypy passed for changed source modules. C031 remains partial: kernel default Arrow-stream/LZ4 statements, connector SQL failure/cancellation behavior, metadata methods and remote Databricks parity remain open. See [B021.2.2 evidence](artifacts/conformance/B021.2.2-sql-connector-inline-results.md). No `.env`, personal account request, cloud compute, Spark, JVM, commit, or push was used. B021.2.3 is active for connector-visible failures, cancellation and remaining method/lifetime checks.


### 2026-10-21 — Completed B021; started B022.1

The released SQL connector 4.6.0/kernel 1.1.0 passed a strict loopback TLS/auth profile for kernel session open/close and SEA query/result behavior. Connector-visible SQL and `SHOW CATALOGS` failures returned no result payload; the same session recovered for a later SELECT. The synchronous SEA client advertised thread-safe `Cursor.cancel()`; the actual cancel request reached BrickHouse after its fixed 10-second wait, and a subsequent poll saw `CANCELED`. Connector 4.6.0 then raised `ProgrammingError: Invalid result format` for that canceled response. Rollback raised the connector's explicit `NotSupportedError`; result/session DELETE passed. The 28-test SQL API suite verifies `CLOSED`, 12-hour expiry and oldest-first 64 MiB retention eviction. The integration passed 1 test (11.43 seconds), and the API suite passed 28 tests (19.54 seconds), with Databricks credential variables unset. The kernel default Arrow-stream/LZ4 profile, other metadata methods, Thrift and remote Databricks behavior remain unqualified, so C031 stays partial. See [B021.2.3 evidence](artifacts/conformance/B021.2.3-sql-connector-failures-retention.md). B022.1 is now active to inject failures across the Delta/SQLite commit boundary.


### 2026-10-21 — Completed B022.1; started B022.2

The local crash matrix killed writers before Delta dispatch, after native Delta commit but before SQLite finalization, and after SQLite finalization. Restart preserved uncertainty before provider dispatch and reconciled exact committed versions on both sides of SQLite finalization. The actual journal, table-service, and SQL mutation regression passed 24 tests with Databricks credentials unset. See [B022.1 evidence](artifacts/conformance/B022.1-commit-boundary-crashes.md). C014 is now partial for this bounded local profile. B022.2 is active for concurrent SQL and DataFrame writes through distinct registrations that resolve to one canonical Delta location.


### 2026-10-21 — Completed B022.2; started B022.3

A deterministic local race held a Sparkless DataFrame append after it acquired the canonical-location lock, then allowed SQL MERGE to capture a stale version-0 snapshot. The DataFrame committed version 1; SQL rejected the stale attempt before journaling, retried once with the same operation ID and committed version 2. Both routes replayed without another provider commit; actual rows were `[0, 10, 20]`, and concurrent native dispatch never exceeded one. The journal, table-service, SQL mutation/MERGE, and Sparkless regression suite passed 46 tests with Databricks credentials unset. See [B022.2 evidence](artifacts/conformance/B022.2-concurrent-alias-writes.md). C014 remains partial: this proves same-process serialization across workspace catalogs, not cross-process or cross-host locking. B022.3 is active.


### 2026-10-21 — Decomposed B022.3 before implementation

The B022.3 acceptance remains intact and is now split into B022.3.1 for restart/replay plus result/log/staging retention, and B022.3.2 for quiesced SQLite/Delta/file snapshot restore with an explicit external-table policy. B022.3.1 is active; no implementation or validation outcome is claimed yet.


### 2026-10-21 — Completed B022.3.1; started B022.3.2

The POSIX restart matrix now verifies same-payload recovery, changed-payload rejection, and staged-input retention while an operation remains uncertain. Cleanup is permitted after exact Delta reconciliation. Worker stdout/stderr expires after 24 hours, clears from the stored output row, and reports `logs_expired` while run/attempt history remains; SQL result chunks retain the existing 12-hour expiry contract. Schema 20 adds the one-way output-expiry migration. The focused journal, table, workspace, catalog, job, and statement API regressions passed 71 tests with Databricks credentials unset; Ruff check and format passed. Worker-log expiry runs lazily on output retrieval or explicit maintenance; no background sweeper or immediate SQLite file shrinking is claimed. See [B022.3.1 evidence](artifacts/conformance/B022.3.1-restart-retention.md). B022.3.2 is active for quiesced snapshot/restore and external-table policy evidence.


### 2026-10-21 — Completed B022; started B048

B022 now has criterion-level evidence for all four acceptance criteria. The snapshot API restores managed Delta rows/history, operation and job history, output, and workspace files to a distinct path; external bytes are excluded and missing references fail explicitly. Serverless-local timeout after a committed append records one timed-out attempt, reaps the worker, and preserves the commit after reopen without replay; a pre-commit failure retry produces a distinct attempt and one commit. Existing B018.1.2/B017.5 checks provide classic interruption/retry and notebook cleanup comparison evidence. The combined affected recovery suite passed 141 tests with Databricks credential variables unset; Ruff check/format, strict mypy across 70 source files, and `git diff --check` passed. Planning validation passes for 180 tasks, 117 required phase 0.1 child tasks, and 1,759 local Markdown links. Cross-process/host locking, external snapshot writers, external Python side effects, cloud compute, and Databricks runtime/service parity remain unqualified. See [B022.3.2](artifacts/conformance/B022.3.2-coordinated-workspace-restore.md) and [B022.4](artifacts/conformance/B022.4-serverless-timeout-commit.md). B048 is active for review of the implemented G1–G4 boundaries and deployment adaptation points.


### 2026-10-21 — Completed B048, B023.1 and B023.2.1; started B023.2.2

B048 and its required children are complete; the loopback-only boundary and no-auth/no-sandbox limitations are included in the release scope. B023.1 records the 0.1.0 package/client inventory, 67 capability states, version-pinned direct dependency notices and hashes, schema-20 migration coverage, the public Sparkless 4.15.0 artifact, and a runnable local orders example. Nine targeted migration/rollback tests passed, the example integration passed 1 test with Databricks token/host variables unset, Ruff and strict mypy passed, and the PyPI metadata confirms the Sparkless release and its wheel tags. B023.2.1 built/audited the wheel and sdist and passed clean installs on macOS arm64 with CPython 3.11/3.12; Linux x86_64 is explicitly unadvertised pending CI. The clean-wheel job now contains the Linux/macOS candidate matrix but was not run. BrickHouse's own license remains an owner choice and blocks publication; no wheel was published. B023.2.2 is active for the clean base/offline workflow. No Databricks account was used for these tasks.


### 2026-10-21 — Completed B023.3 release-candidate assembly

B023.2.2 installed the audited `0.1.0.dev0` base wheel on macOS arm64 CPython 3.11.15 and 3.12.13; each interpreter passed 132 guarded installed-wheel cases: G1 1, G2 10, G3 116, and G4 5. Both candidates imported from `site-packages`, passed `uv pip check` and CLI help/version, and had no PySpark, delta-spark, or notebook packages. OS sandboxing denied external network access while allowing loopback and the per-run local Unix socket used by Terraform; all observed network traffic was loopback. `.env` opens and inherited Databricks credentials were absent. Java/Spark settings were removed; the PATH and Python process guard excluded Java, Spark, and Docker use. Terraform ran with a pre-provisioned local provider mirror and checkpoint requests disabled. See [B023.2.2](artifacts/conformance/B023.2.2-base-wheel-offline.md). B023.2.3 then passed the separate notebook-extra suite and G1–G4 on both combinations, 169 cases per interpreter, including classic notebook bundles and kernel interruption cleanup; see [B023.2.3](artifacts/conformance/B023.2.3-notebook-extra-offline.md). B023.3 assembled the owner-review bundle with the exact qualified wheel, a freshly audited source archive, generated capability report, version and case manifests, and retained conformance/recovery evidence. Linux x86_64 remains unadvertised pending CI, and BrickHouse's own license still blocks publication. No account, cloud compute, or release publication was used.


### 2026-10-22 — Interim B064 review-fix candidate (historical)

Completed B064.1–B064.10. The refreshed wheel passed 94 focused B064 tests in a clean base environment under the offline loopback guard; the full source suite passed 415 tests with six optional-tool skips. Ruff check, formatting, strict mypy, planning validation, and wheel/sdist audit passed. The new artifact hashes and machine guard report are in the [candidate bundle](artifacts/releases/0.1.0/README.md); prior full G1–G4 results remain attached to the previous wheel hash, and the current wheel still needs those complete matrices. No Databricks account or publication was used. See [B064 evidence](artifacts/conformance/B064-phase-0.1-review-fixes.md).


### 2026-10-22 — Completed B064 review fixes and requalified Sparkless 4.15.1 candidate

Fixed all ten review findings, reconciled current behavior docs/capabilities and the task ledger, and rebuilt the development wheel/sdist with Sparkless 4.15.1. The current installed wheel passed 99 focused B064 tests under the prepared offline base environment and loopback-only guard; the SQL statement-service source suite passed 35 focused tests. The 415-pass/6-skip full source-suite result predates B064.11 and Sparkless 4.15.1 and remains historical. The prior candidate remains archived with its exact G1–G4 hashes; those matrix results are not attributed to the refreshed wheel. Linux, project-license choice, complete current-wheel G1–G4 requalification, and public release authorization remain open. The published 4.15.1 UTC-aware timestamp predicate mismatch is tracked in [Sparkless issue #1720](https://github.com/eddiethedean/robin-sparkless/issues/1720). No credentials, Databricks account, cloud compute, commit or push were used.


### 2026-10-22 — Sparkless 4.15.2 dependency update (before full candidate matrix)

Advanced the exact developer pin to the PyPI Sparkless 4.15.2 macOS arm64 wheel, verified at SHA-256 7d12d6f76212e52586cbe07ab12152f4c7dcf04bd2873532751f1335ee81cfb2. Upstream PR #1721 fixes the retained UTC-aware Delta timestamp predicate and closes issue #1720; the selected local query returns order_id 2. The full SQL statement-service module passed 35 tests with Databricks credentials unset. The rebuilt BrickHouse 0.1.0.dev0 wheel passed 99 focused B064 tests in the offline loopback-only base environment. At this checkpoint, full current-wheel G1–G4 qualification remained open; the later entry below records its completion. No Databricks workspace, remote compute, publication, commit, or push was used.


### 2026-10-22 — Full Sparkless 4.15.2 candidate qualification

Rebuilt the 0.1.0.dev0 wheel and source archive after updating the notebook kernel's required Sparkless pin and the SQL connector cancellation test adapter. The final wheel SHA-256 is `f6d89bed8e6e9611595c9cce8686383e6225a3fc3376126cbb61df6a51adfcef`; its source archive hash is recorded in the candidate bundle. The exact installed wheel passed 137 base G1–G4 cases and 175 notebook-extra cases per interpreter on macOS 26.5.2 arm64 CPython 3.11.15 and 3.12.13: 624 selected matrix passes. The separate B064 installed-wheel regression set passed 99 cases. Distribution audit passed. The UTC-aware timestamp predicate fixed in Sparkless 4.15.2 passed within G1+G2.

All runs used clean offline environments with loopback/local Unix sockets only. No `.env` reads, inherited Databricks credentials, personal workspace, cloud compute, Java/Spark, or Docker were used. This qualifies the selected local matrix only; Linux and Windows remain unqualified, broad/remote Databricks parity is not claimed, the project license is unselected, and publication remains unauthorized. See the [current candidate bundle](artifacts/releases/0.1.0/README.md) and [machine-readable reports](artifacts/releases/0.1.0/evidence/conformance/sparkless-4.15.2-current/).


### 2026-10-22 — Final 0.1.0 release readiness started

B066 decomposes final release readiness into four bounded children without replacing original phase requirements. B066.1 is active for the full current source suite and requirement traceability. Remaining work includes portable offline Linux/macOS qualification, final owner-selected license and version metadata, publication configuration, and final artifact/CI evidence. No release tag or publication is authorized by preparation alone.


### 2026-10-22 — B066.1 full-source and requirement qualification passed

Both macOS arm64 CPython 3.11.15/3.12.13 source suites passed 444 tests with zero skips and required CLI/Terraform/connector integrations enabled. The 17 new golden-orders variants verify actual aggregate writes, exact requested BIGINT/DECIMAL(22,2) output, dependent reads, all warehouse profiles, and restart consistency. The 27 release families are mapped in planning/release-requirements.json. The Python 3.12 profile assertion was corrected. B066.2 is active for reproducible Linux/macOS offline qualification. The owner authorized selecting the project license; Apache-2.0 is selected for the final metadata slice.

### 2026-10-22 — B066 portable qualification and release metadata in progress

Added pinned, hash-verified client provisioning and shared Linux/macOS qualification workflows. Linux uses a loopback-only network namespace and drops to the original user before candidate execution; macOS uses sandbox-exec. Required skips fail. Added automated traceability validation for all 27 requirement families and 61 selectors. The owner authorized choosing a license: selected Apache-2.0, retained the official root text and third-party notices, and set metadata/runtime version 0.1.0. Refreshed current user documentation. Final CI qualification and PyPI publisher verification remain pending; no public release was created.

### 2026-10-22 — B066 matrix findings and bounded remediation

CI run 39859177176 proved the Linux network namespace and both OS/Python base G1+G2 paths, then caught a warehouse-stop cancellation publication race, a Linux Terraform mirror/cache collision, and macOS 14 kernel HTTP-client construction failure. B066.2 is decomposed into portable client fixtures, terminal admission ordering, and final matrix qualification. Three deterministic success/failure/cancellation barrier cases plus both native cancellation variants passed locally (5 tests); the connector with explicit cryptography-generated CA/server extensions passed its real verified-TLS integration. The 0.1.0 local wheel passed all 336 selected installed-wheel cases; its subsequent full source run caught one remaining development-version literal (447 passed, 1 failed), now corrected. No failed run qualifies the final release. Sparkless SQL COUNT's int32/DataFrame int64 mismatch is tracked in upstream issue #1723; native DataFrame aggregate output meets the golden types without casts.

### 2026-10-22 — B066.3 metadata/diagnostics complete, publisher access pending

B066.3 is split into metadata/diagnostics and remote publication configuration. B066.3.1 passed 41 focused CLI, isolation, HTTP diagnostics and planning cases; Sparkless and delta-rs are accurately reported as required integrated local subsets, SQL is partial rather than planned, and CLI errors direct Python jobs to the implemented Jobs API. Apache-2.0, 0.1.0 metadata, README/changelog and retained notice audits are prepared. B066.3.2 is blocked only on PyPI owner-account access: browser inventory reports the Mac locked. The tag workflow checks the explicit unverified publisher record and fails before publishing. B066.4 depends on completed metadata plus platform qualification so the concrete review bundle can be frozen independently of that final account action.

### 2026-10-22 — B066.2 complete platform qualification

[CI run 39860440439](https://github.com/eddiethedean/brickhouse/actions/runs/39860440439) passed source checks, the audited candidate build and all four platform/Python jobs. Eight base/notebook lanes passed 1,368 required offline cases, and the complete source suite passed 451 cases per OS/Python pair (1,804 passes), all with zero skips. Pinned verified-TLS connector and Terraform lifecycle tests passed on Linux x86_64 and macOS 14.8.9 arm64. Exact interpreter versions, candidate hashes and JSON/XML isolation reports are retained in the release bundle. B066.2 and its three children are complete. B066.4 is active for the final diagnostics candidate and frozen artifact inventory; B066.3.2 remains blocked on PyPI owner-account access. Bundle-only evidence commits are excluded from push CI because the entire bundle is excluded from both distribution contents; any other source/document/workflow change and every release tag still runs complete qualification.

### 2026-10-22 — Explicit numeric loopback rule for macOS qualification

The final diagnostics run passed the macOS 3.11 base lane but hit an intermittent native CLI `operation not permitted` on a later numeric 127.0.0.1 connection in notebook G4. It is retained as a failed qualification, not accepted as release evidence. The macOS sandbox now explicitly permits numeric IPv4 loopback alongside localhost; outbound networking remains denied. The complete final candidate matrix is rerun with that profile.

### 2026-10-22 — macOS profile compilation and local native-client verification

The numeric-host experiment was rejected by macOS policy compilation (`host must be * or localhost`), so that run is a failed experiment and provides no qualification. The verified replacement retains localhost-only IP access and permits local Unix sockets, including native system-service IPC, consistently with the Linux guard's permitted local Unix sockets. Both final-wheel local G4 lanes passed (5 base + 6 notebook cases); the preflight records macOS EPERM for non-loopback IP access, rather than accepting a timeout or connection refusal as isolation proof. Full CI is rerun with this profile, and failure-only macOS sandbox denial capture is available for diagnosis.

### 2026-10-22 — Current native CPython qualification builds

The macOS 3.11 framework build continued to hit intermittent native CLI loopback denials despite a valid localhost/Unix policy; the final artifact remains unqualified by those failed runs. The official setup-python arm64 manifest stops at 3.11.9/3.12.10, so check-latest alone cannot provide current patched builds. Final lanes now provision exact native CPython 3.11.17/3.12.15 using hash-locked uv 0.13.5, with explicit version/implementation/directory checks and retained executable hashes. The localhost policy uses explicit TCP/UDP filters. Local compilation/EPERM checks and six native notebook-lane G4 cases passed; four pure version-selection/rejection regressions passed. Full final CI remains required.

### 2026-10-22 — Final macOS qualification profile

The current native interpreter did not eliminate macOS 14's intermittent native CLI loopback EPERM. The final OS profile is now pinned to GitHub's standard macOS 26 arm64 image, matching the OS family used by successful local qualification. Ubuntu 22.04 x86_64, both Python minors, all base/notebook selectors, real workload/client/recovery checks and strict offline-denial requirements remain unchanged. macOS 14.8.9 is explicitly unqualified for the final artifact, and failed runs are not accepted or retried as passes. The exact final matrix must pass before B066.4 completes.


### 2026-10-22 — B066.4 final evidence consistency audit

Retained the successful exact native-Python Ubuntu/macOS 26 matrix from CI run 39869421709: 1,368 installed-wheel offline passes and 455 full-source cases per OS/Python pair (1,820 total), zero required skips. Corrected seven stale capability function references, refreshed current tested versions without promoting any capability state, synchronized stale backlog status labels, and made the report renderer defer platform claims to exact hash-bound release evidence. Added regressions for missing capability tests, stale backlog states and old renderer platform claims; all 26 planning/report tests pass. All 17 real orders workflow variants pass with explicit Sparkless function/type imports. Required source checks and strict qualification-tool typing pass. Updated plans distinguish dated development artifacts from the final 0.1.0 candidate. B066.4 remains active until the updated source archive passes CI and is frozen. PyPI account access remains the only external preparation blocker; no tag or publication was created.


### 2026-10-22 — B066.3.2 publisher configuration verified

The owner configured PyPI directly and supplied a screenshot of the saved Pending publishers entry: brickhouse, GitHub eddiethedean/brickhouse, release.yml, environment pypi. All fields match the workflow; the GitHub pypi environment tag-only policy was re-read and verified. Retained the public configuration screenshot and verification record in the release bundle. B066.3.2 and its B066.3 parent are done; this verifies configuration only, with actual OIDC exchange/upload reserved for a separately authorized release. No tag or distribution was published.


### 2026-10-22 — B066 final release preparation complete

All 142 phase 0.1 children and the B066 parent are done. CI run 39873265129 passed the current complete source suite on all four native OS/Python profiles (458 each, 1,832 total) and all eight offline installation lanes (1,368), with zero required skips. Strict Twine 7.0.0 and license/notices audits pass; rebuilding from the exact source archive produces the byte-identical wheel. The final case/capability/status audit passes, and the owner-verified saved PyPI pending publisher matches release.yml and the tag-only pypi environment. The final source metadata records this completed evidence and is checked again in GitHub CI; release bundle updates are excluded from distribution contents. Actual OIDC upload/publication remains a future owner-authorized tag action. No tag, package upload or public release was created. Known Sparkless SQL COUNT schema limitation #1723 and the bounded local compatibility scope remain documented.


### 2026-10-22 — B066.3.1 README upgrade

Reworked the project front page with CI/Python/alpha/license badges, section navigation, a three-step source-install quickstart, expected orders output, a copyable SDK connection, capability and qualified-client tables, and links to current behavior, design and roadmap documents. The serverless/classic simulation boundary, trusted-code execution model and exact release scope remain explicit.

Verified 21 repository-file links, 7 in-page anchor links, shell syntax for all 6 shell blocks, Python syntax and the expected JSON result. A fresh disposable workspace passed CLI init/serve, the exact README SDK snippet (catalog list `['main']`), the included real orders worker/Delta/SQL workflow (`[["alice", "2", "15.75"]]`), the same SQL result after server restart, and version/doctor/capabilities commands. Verification used Sparkless 4.15.2 and SDK 0.153.0 with real-workspace credentials removed. The temporary workspace was removed. Planning validation and whitespace checks passed. This documentation slice promotes no capability; retained release qualification remains bound to the source revision and artifact hashes recorded in the frozen bundle.
