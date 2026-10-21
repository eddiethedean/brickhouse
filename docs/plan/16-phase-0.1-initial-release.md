# 16 — Phase 0.1 implementation and release contract

Updated: **2026-10-20**. Initial release: **0.1.0**. This document defines required behavior and evidence; it does not claim those behaviors are implemented. [STATUS](../../STATUS.md) and [the ledger](../../planning/tasks.json) describe current code and task state.

## Release objective and retained requirements

A developer installs a wheel, starts a local workspace, uses the pinned SDK to submit real Python work under bounded serverless or classic compute profiles, and reads its committed Delta data through SQL, a fresh Sparkless session and a restarted server. The notebook extra and selected CLI/Terraform/bundle/SQL connector workflows have their own real execution checks.

Retain every parent acceptance criterion in B001–B023 and the compute expansion adopted by [ADR 0016](../decisions/0016-serverless-and-classic-compute-profiles.md). Both serverless and classic Python/notebook subsets and serverless/pro/classic SQL warehouses remain required. B043 records compute research; B044 records this refinement; B045 resolves connector transport risk; B046 qualifies the public Python provider binding and selected typed envelope. There are **85 required child tasks** across the 24 implementation packages B001–B023 and B046, including five bounded B014.3 scheduler, retry, concurrency and recovery slices. Standalone B043–B045 and B047–B048 are also assigned to phase 0.1.

[ADR 0017](../decisions/0017-phase-0.1-delivery-refinement.md) separates resource/context completion from workload verification. Original parent criteria and prerequisite edges remain required. A parent completes only when all required children and its integrated criteria pass. The [backlog](09-task-backlog.md) mirrors the ledger. Later scope remains in the [root roadmap](../../ROADMAP.md) and [compute plan](17-serverless-and-classic-compute.md).

## Bounded preparation for the later deployment goal

The primary release objective remains the local JVM-free Databricks emulator. B047 records the longer-term plan; **B048** reviews the real G1–G4 implementation before B023 for shared service/catalog/commit authority, contained local runtime/storage assumptions, stable resource identity and documented future principal entry points. Its deliverable is a code-referenced review and concrete gap disposition, not remote workers, a browser, deployed authentication or speculative provider abstractions. B048 depends on B018/B022, and B023 and its children inherit B048 as an additional prerequisite. All existing parent criteria and **85** required implementation child tasks remain.

Actual self-hosted implementation belongs to SH.1–SH.6, only after B049 assesses stability following the phase 0.9 emulator release. Completing 0.1 or adding local permission semantics does not establish deployed security. [Document 18](18-self-hosted-platform.md) and [ADR 0034](../decisions/0034-emulator-first-self-hosted-platform.md) record the phase-by-phase preparation and separate future track. B047/B048 are standalone planning/review tasks assigned to 0.1 alongside B043–B045; they do not enlarge the execution corpus or add deployment dependencies to installation.

## Current starting point and work order

The recorded foundation includes local workspace/catalog metadata, a partial delta-rs table service, isolated Python worker supervision, durable job state, and the B014.2 pinned-SDK path for Python source upload, one-script dispatch, polling and bounded output. B014.3 is split into DAG scheduling, retry/concurrency admission, and restart recovery tasks. That worker flow runs plain Python and does not expose Spark/Sparkless or Delta session behavior. Public SQL and notebook execution remain unimplemented, and the broader serverless/classic workload contracts remain incomplete. B045 has verified the pinned SQL kernel's first session request against a local TLS/auth boundary; no SQL result or end-to-end connector compatibility is proven. B046.1–B046.2 now verify the released Sparkless 4.14.0 Python provider API. B046.3 and B010.1 remain blocked by nullable-struct ingestion/collection defects #1694/#1695; [the exact report](../../artifacts/conformance/B046.3-sparkless-4.14.0-provider.md) supersedes the historical missing-binding diagnosis. Reuse existing evidence only for its recorded versions and cases.

The following lanes organize work inside 0.1.0; they are not release numbers or calendar promises. Follow exact ledger dependencies. Independent ready work can proceed while an integration lane is blocked.

| Lane | Work order | Completion signal |
|---|---|---|
| A — Resolve prerequisites | Continue B013.2 → B013.3; independently B045 and B046.1 → B046.2 → B046.3 | Scoped worker protocol/cancellation; actual connector transport decision; tested public Python provider API and candidate wheel |
| B — Bind shared data | B010.1 → B010.2 → B010.3 | Real DataFrame reads/writes use one table service; exact types, error propagation, owner-thread rules and independent session state pass |
| C — SQL and warehouses | B011.1 → B011.2 and B011.5 → B011.6 → B011.3 → B011.4; then B012 | Three warehouse types resolve real resource identity and actual statements; G1 reads agree after restart |
| D — Jobs and utilities | After B013: B014.1 → B014.4 → B014.5 → B014.2 → B014.3.1 → B014.3.2.1 → B014.3.2.2 → B014.3.3 → B014.6; then B016.1–B016.3. B015 follows its B010/B013 prerequisites | SDK upload/run/output works; DAG scheduling, bounded retries, concurrency admission, restart recovery, compute lifetimes, fresh attempts, scoped imports/utilities and interruption are real |
| E — Notebooks and full workflow | B017.1 → B017.2 → B017.4 → B017.3 → B017.5; then B018.1–B018.2 | Required kernel/mode/format cases, child notebook semantics and G2 pass; base install still runs without notebook packages |
| F — SQL corpus and clients | B019.1–B019.3 after B011; B020.1–B020.4 after their prerequisites; B020.5 also waits for B018. B021 waits for B045 and B019 | Enumerated mutations/semantics, real client deployment, bundle output and connector results pass |
| G — Recovery and candidate | B022.1–B022.3 after B018/B019; B023.1–B023.3 after B020/B021/B022/B044 | G3 recovery plus consolidated G1–G4, install/offline evidence, supported-scope record and reviewable distribution artifacts |

B045 is an early transport experiment, not an SQL implementation. Use the real pinned connector and local TLS capture/error boundary to answer whether the selected host/auth/path can work. Do not return fake successful statements. A negative result can complete the research decision, but B021 remains blocked until its execution contract works or the release scope is formally revised.

B046 can qualify an exact hash-verified published wheel or a source-built candidate when no compatible release exists, as recorded in [ADR 0018](../decisions/0018-published-sparkless-python-provider.md). Its public Python registration/callback API must preserve owner-thread/GIL/lifetime/error behavior and typed provider results. B023 still requires a publicly available compatible Sparkless artifact; ownership or an open PR alone is insufficient. No release publication is authorized by this plan.

### Provider and consumer completion boundaries

| Provider/context task | Evidence required to complete it | Consumer that proves actual workload effects |
|---|---|---|
| B014.1 durable job state | Immutable definitions/submissions and task snapshots; valid DAGs; idempotency; DB-enforced transitions and truthful interruption | B014.2 SDK flow, B014.3 dispatch/retry/recovery and B014.6 real workers |
| B011.5 warehouse records/lifecycle | Real SQLite/reopen/API tests, legal states, type flags, admission/invalidation contract and controlled-clock idle behavior | B011.6 dispatch/admission and active statements; B011.3 cancellation/errors; B021 released connector |
| B014.4 classic compute records | Real identity/lifecycle APIs, run-scoped versus persistent identity, first/last consumer and invalidation events | B014.6 real worker/resource lifetime and interruption; B022 commit/restart failures |
| B014.5 context resolver | Valid/invalid selectors, provisioned environment mapping and immutable effective settings before dispatch | B014.2 actual SDK workers and B014.6 all required variants |
| B017.4 notebook context | Real kernel context/environment precedence, cell state, normal reset and distinct sessions | B017.3 child cancellation/cleanup; B017.5 interruption while a cell is running |
| B020.4 Terraform resources | Actual provider apply/read/update/refresh/destroy against persisted local resources | B020.5 bundle workloads with actual Python/notebook and Delta/SQL output |

A provider's resource state can be intentionally simulated. That never completes the consumer's job/query success criterion. No child requires unfinished downstream integration as its own completion evidence.

## Installation, versions and execution boundary

One `brickhouse` distribution remains the target. The current dependency-free bootstrap and optional `[delta]` service are intermediate implementation state. For the 0.1 core workflow, the ordinary install must include a tested compatible Sparkless artifact, the selected native Delta provider and the chosen SQL parser/binder dependencies. Keep notebook execution in `brickhouse[notebooks]`; the base workflow must run with those packages absent. Do not introduce a new wheel or an empty extra to satisfy the plan.

Reuse the inspected client pins from [B005.1](../../artifacts/conformance/B005.1-client-profile-matrix.md), revalidating actual artifacts before implementation and publication. Record exact versions/hashes of the candidate, Sparkless, delta-rs, Arrow, parser, SDK, CLI, Terraform/provider, connector/kernel and notebook packages where used. Public reference runtime/environment versions are separate provenance; `spark35-local-v1` does not identify a Databricks Runtime.

Local execution uses the one catalog/table service, native Delta provider, Sparkless default engine and fresh child process per job attempt. Never transfer a session across an unsupported thread boundary. DuckDB requires an explicit tested route selected before execution. No Java, full PySpark, delta-spark, Docker runtime or hidden real-workspace fallback may complete a request. Public local SDK tokens are compatibility values; the private worker token is separately scoped and enforced. Local trusted-code execution is not a sandbox or phase 0.2 authorization.

## Minimum supported corpus

These are selected release targets. Required syntax/type cases must have stored expected schema/results/errors before their implementation can claim compatibility. Unknown parameters, unsupported options and unavailable effects fail explicitly rather than being silently ignored.

| Surface | Required positive cases | Required boundaries and owner |
|---|---|---|
| Workspace/catalog | Init/open/serve/shutdown; SDK catalog/schema CRUD and pagination; stable table identity; quoted one/two/three-part names | Duplicate/missing/protected names, location aliases, incompatible config/metadata, ownership and instance isolation. B006–B008, B023 |
| Tables/DataFrames | Real create/read/append/overwrite; select/filter/group aggregate; managed/external behavior; latest/version/timestamp snapshots | Exact schema/protocol validation; journaled replay; external drop preserves files; managed cleanup stays under its owned root. B008, B010, B012 |
| Typed data | Null, boolean, long, double, string, decimal(12,2), date, UTC timestamp; nullable array and struct fixtures from the selected typed envelope | Exact Arrow/Delta/SQL schema agreement. Maps and other unselected types/features require an explicit unsupported case until separately selected and proved. B046, B010, B019 |
| SQL queries/DDL | Catalog/schema and managed Delta table create/drop; SELECT projection, predicates, COUNT/SUM aggregate, one equijoin, inserts and named bound parameters in the pinned SDK shape | Quoted identifiers, injection-shaped parameter values, missing/duplicate targets, null ordering/coercion/decimal/timezone/collection cases and accurate errors. B011, B019 |
| SQL mutations/utilities | UPDATE, DELETE, single-key MERGE matched update/unmatched insert; history/describe and supported snapshot/time-travel queries | Route through the journaled table service; reread data/history via DataFrames. Schema evolution/partitions/maintenance retain selected per-operation decisions and explicit rejection where unimplemented. B019.2–B019.3, B022 |
| Statements/warehouses | Submit/poll/fetch/cancel/expire bounded INLINE JSON_ARRAY results; serverless/pro/classic identity, flags and supported start/stop/idle/admission | No invented SERVERLESS warehouse enum, ignored warehouse ID/path, unbounded result memory or fake empty result on expiry. Unsupported formats/dispositions reject before mutation. B011.2–B011.6, B021 |
| Jobs | SDK file upload and Python create/submit/poll/output; two-task dependency; one explicit retry; cancellation/timeout/restart; submission token replay | Missing file/selector/environment, invalid DAG/task, failed dependency, changed submission payload, worker crash and truthful attempt/resource state. B013–B014 |
| Managed imports/dbutils | Explicit Sparkless plus worker-scoped supported pyspark/DeltaTable imports; listed fs/widgets/secrets/taskValues subset | Host imports unaffected; real local file changes; missing/denied secrets; parameter precedence; values keyed by run/task/attempt; explicit unsupported methods. B015–B016. B016.1 covers local DBFS and widgets, B016.2 scoped secrets, and B016.3 task values for Python script workers. Databricks taskValues is notebook documented; this is an explicit local job-script extension, not parity. |
| Notebooks | ipynb plus declared Python source exports; Python/SQL/Markdown/run handlers; notebook run/exit | Shared-scope %run versus isolated child invocation; real output/error capture; unknown magics, Scala/R/%pip and unprovisioned dependencies reject; kernels/descendants are reaped. B017 |
| Deployment clients | Pinned CLI workspace/job lifecycle, Terraform selected file/job/cluster/warehouse resources, serverless/classic bundles | Apply/read/update/refresh/destroy and validate/deploy/run must verify actual persisted state/output and cleanup. Select exact resource wire contracts in the child, not by accepting unknown fields. B020.1–B020.5 |
| SQL connector | Actual released REST/kernel client against local verified TLS/auth/path; parameters, schema, fetch batches, errors and lifetime | Explicit backend selection; no disabled TLS verification or inherited cloud credentials to make the case pass. Thrift/native Connect stay later. B045, B021 |

ANSI/timezone defaults and allowed configuration differ by selected compute/runtime profile; retain [document 17's contracts](17-serverless-and-classic-compute.md). Golden workloads set their semantic settings explicitly. Serverless fixtures require the selected UTC/ANSI behavior and disable unimplemented automatic optimization retries. Classic default behavior is never inferred from its compute label or an arbitrary Runtime string.

## Frozen fixture and compute variants

Use one synthetic orders fixture with a deterministic transform, SDK driver, notebook variants, client deployment inputs and reviewed expectations. Proposed location: `examples/phase_0_1/`, with oracle fixtures under `tests/fixtures/oracles/`. These paths are deliverables for the owning tasks; this document creates no placeholder program or passing test ID.

| order_id | customer | amount decimal(12,2) | active |
|---|---|---|---|
| 1 | alice | 10.50 | true |
| 2 | alice | 5.25 | true |
| 3 | bob | null | true |
| 4 | bob | 20.00 | false |

Filter active rows with non-null amount, group by customer, and return alice, order_count 2, total_amount 15.75. Freeze the exact aggregate schema and decimal widening from the selected reviewed semantic oracle before closing B010/B019 case evidence; matching values alone is insufficient. The current literal-only Databricks fixture does not prove aggregate schema. Use stored local/reference evidence where sufficient; a new bounded account probe is optional tooling under [document 15](15-personal-databricks-verification.md), never a required live CI step.

Require the following **seven Python job variants**, each using its own synthetic resource and operation IDs:

| Variant | Compute selection | Required lifecycle assertion |
|---|---|---|
| SL | Serverless with mapped job environment; automatic optimization retries disabled | Isolated attempts and environment/settings validation; no classic cluster is fabricated |
| CSN | Classic standard, new job compute | New run-scoped compute terminates after its consumers |
| CSE | Classic standard, existing all-purpose compute | Same persistent compute ID survives run completion and can autostart |
| CSH | Classic standard, shared job compute key for two tasks | One run-scoped identity spans its first/last consumer; attempts retain fresh process state |
| CDN | Classic dedicated, new job compute | Dedicated profile restrictions and ephemeral run lifetime |
| CDE | Classic dedicated, existing all-purpose compute | Dedicated profile restrictions and persistent identity |
| CDH | Classic dedicated, shared job compute key for two tasks | Dedicated profile restrictions and first/last consumer lifetime |

Runtime/environment versions must resolve through an explicit provisioned local mapping recorded with fixture provenance. Do not install dependencies at job time or treat an arbitrary Databricks version string as equivalent to Sparkless. Each base job uploads the script, uses a parameter and synthetic scoped secret, writes the aggregate, runs a dependent read and returns bounded output. Read every produced table through **each of the three warehouse types**, a fresh Sparkless session and a restarted server. Query schema/data and committed versions must agree.

Run the same seven selections through ipynb jobs in the notebook-extra lane. Add one source-export/conversion fixture for each of the three profiles (serverless, classic standard, classic dedicated): **ten notebook variants total**. All resource selection forms are exercised by ipynb; all three profiles exercise source conversion. Test %run, child notebook run/exit, environment precedence and in-flight interruption as separate focused cases. The base installation lane imports none of the notebook packages.

Managed-import checks cover all three compute profiles in additional focused scripts; the base seven-job corpus uses explicit Sparkless imports. This bounds combinations without treating an untested protocol, type, runtime version or access policy as supported.

## Local fixture limits and overload behavior

These are **chosen local acceptance settings to implement**, not Databricks defaults, new security promises or fields accepted by the current version-1 config. Owning tasks must add any needed configuration/schema changes explicitly and record effective values in evidence. Do not reduce these limits silently to make a failing gate pass.

| Boundary | Selected fixture setting | Assertion and owner |
|---|---|---|
| Attempt context | 16 KiB canonical finite JSON | Existing B013.1 contract; include only allowed context, never credentials. Oversize/invalid input fails before launch. |
| Retained worker output | 1 MiB stdout and 1 MiB stderr | Preserve actual exit status and explicit truncation flags; continue draining pipes so a noisy child cannot deadlock. Existing maximum capture bound remains 64 MiB per stream. B013/B014 |
| Job admission | 2 active worker attempts, 8 queued attempts, at most 10 tasks per fixture job | Enforce queue/concurrency limits and an explicit overload/invalid-request outcome. Persist admitted attempts and recover queues without duplicate dispatch. B014.3/B014.6 |
| SQL admission | 1 active statement per warehouse, 2 active statements workspace-wide, 8 queued per warehouse | Enforce leases/queue limits, release them after error/cancellation/restart, and use accurate pending/terminal state. B011.5–B011.6 |
| SQL results | Hard local cap 10,000 rows or 8 MiB encoded output; chunk cap 1,000 rows or 1 MiB | No silent slicing. Exceeding an unnegotiated hard cap, or a single row exceeding the chunk cap, returns a specific result-limit failure. Exercise multiple chunks with a smaller per-test chunk setting. B011.4/B021 |
| Result expiry | 60 seconds in the fixture, advanced with a controlled clock | Persist statement outcome separately from owned result artifacts; an expired fetch fails explicitly. Cleanup cannot read/delete arbitrary paths or erase commit evidence. B011.4/B022.3 |
| Job/notebook deadlines | Explicit 60-second Python task and 120-second notebook task deadlines | Use real monotonic execution deadlines and bounded process polling. These are fixture-requested timeouts, not serverless query defaults. B013.3/B017.3 |
| Cancellation escalation | 2-second graceful termination, then forced process-group termination with a 2-second reap deadline | Prove descendants/kernels actually exit and preserve real terminal/commit outcomes. Failure to reap is an explicit failed check; timing controls are not cloud-performance parity. B013.3/B014.6/B017.5 |
| Retained logs/staging | 24-hour fixture log retention; retain uncertain-operation inputs until reconciliation | Use a controlled retention clock; delete only owned, safe artifacts. Explicit cleanup/retry policy for failed removals. B022.3 |

Control clocks only for simulated infrastructure, queues/retention and deterministic fixtures. Actual user code, Delta commits, network clients and child processes still execute. A cancel/timeout/result error after a real commit must retain its operation/version evidence; none implies rollback. Numeric defaults may change only through a recorded contract revision and renewed focused evidence.

## Release case families and evidence owners

The IDs below identify **planned acceptance families**, not existing test IDs or passing results. Each owner decomposes them into actual tests and a versioned case manifest; record positive, negative and recovery cases separately. New findings extend the denominator rather than replacing required rows.

| Family | Required assertion | Owners | Gate |
|---|---|---|---|
| P01 | Public Python provider callback/read/write API, exact typed values, lifecycle/errors and reproducible artifact | B046.1–B046.3, B010 | G1 prerequisite |
| P02 | Released connector local host/TLS/auth/path feasibility with actual captured request/error and no fake SQL success | B045 | G4 prerequisite |
| D01 | SDK catalog/schema CRUD/pagination, quoted name resolution and stable table ownership/location | B007, B012 | G1 |
| D02 | Typed DataFrame create/read/append/overwrite, empty input and schema mismatch | B010, B012 | G1 |
| D03 | SQL query/DDL/insert/parameters and exact manifest schema/row encoding | B011, B019.1 | G1 |
| D04 | UPDATE/DELETE/MERGE, describe/history/snapshots and DataFrame-visible committed versions | B019.2 | G1/G3 |
| D05 | Protocol/features/type/path rejection before unsafe access or mutation; no silent executor retry | B010, B019.3 | G1 |
| S01 | Concurrent session namespace/config/view/UDF isolation and profile-specific defaults/restrictions | B010.3, B015.1, B019.1 | G1/G2 |
| W01 | Three warehouse type/flag/resource lifecycles and missing/invalid identifiers | B011.5 | G1/G4 |
| W02 | Real statement admission, queue/overload limits, stop/cancel/restart and consistent results | B011.6, B011.3–B011.4 | G1/G3 |
| J01 | All seven Python job selections produce the orders schema/data/output and correct resource lifetime | B014.2, B014.6, B018.1 | G2 |
| J02 | Invalid DAG/selector/environment/file/task, dependency failure, distinct retry attempts and submission replay | B014.1–B014.5, B018.1 | G2/G3 |
| J03 | Real file/widget/secret/task-value operations, parameter precedence, size limits and scope denial | B016.1–B016.3, B018.1 | G2 |
| J04 | Worker crash/timeout/cancel with descendants, bounded output and no import/context leakage | B013.2–B013.3, B014.6 | G2/G3 |
| N01 | Seven ipynb selections and three source-export profile variants have real results/errors | B017.1–B017.2, B017.5, B018.2 | G2 |
| N02 | %run shared scope versus child run/exit isolation and notebook/job environment precedence | B017.3–B017.4 | G2 |
| N03 | Compute reset/termination or parent cancellation during a cell reaps kernels, starts fresh state and preserves committed data | B017.5 | G2/G3 |
| C01 | Real pinned CLI workspace/file/job success/errors and cleanup | B020.1 | G4 |
| C02 | Real pinned Terraform file/job/cluster/three-warehouse lifecycle and state refresh | B020.2, B020.4 | G4 |
| C03 | Serverless/classic bundle validate/deploy/update/run, actual workload output and cleanup | B020.3, B020.5 | G4 |
| C04 | Released REST/kernel connector parameters/schema/fetch chunks/errors/expiry/cancellation where advertised | B021.1–B021.2 | G4 |
| R01 | Failure before commit, after commit/before journal finalization, and after finalization/before response; identical versus changed operation payload | B022.1 | G3 |
| R02 | Concurrent SQL/DataFrame appends with location aliases preserve rows; identical operation replay does not duplicate commits | B022.2 | G3 |
| R03 | Service/compute/worker restart preserves accurate attempts, queue leases, uncertainty and already committed table data | B014.3, B014.6, B022.1 | G3 |
| R04 | Coordinated backup/restore reads real tables/history/jobs; result/log/staging retention is bounded and ownership-safe | B022.3 | G3 |
| A01 | Candidate installation and base/notebook-extra separation; process/dependency/network checks prove native and provisioned offline execution | B023.1–B023.2 | G1–G4 |
| A02 | Exact scope/client/provider/profile matrix, notices/license/migrations/examples and reviewable artifact hashes | B023.1, B023.3 | G1–G4 |

The initial case manifest has **27 families**. Every combination claimed in a release record must map to actual test IDs, expected schema/result/error, exact versions, a passing report and the candidate/source hash. Family coverage alone does not establish every combination. C034 infrastructure simulation retains separate state/evidence from C053–C056 workload/environment behavior. Preserve existing foundation evidence as historical observations; re-run affected cases when interfaces or dependencies change.

## Failure invariants and stop conditions

- Invalid requests/selectors/unsupported effects fail before worker launch or mutation whenever validation can establish the failure; report the pinned API's specific error mapping and a specific Python error where applicable.
- Terminal execution state follows observed code/commit outcomes. Simulated RUNNING/STOPPED metadata, a skipped unsupported task or a 200 response never proves a successful job/query.
- Idempotency keys identify a specific payload. Preserve known Delta versions and uncertain outcomes; do not replay blindly or promise exactly-once external Python side effects.
- Session/attempt credentials, namespace/config, UDFs/views/imports and notebook state stay isolated. Public SDK token compatibility and access-mode labels do not imply phase 0.2 identity enforcement.
- Missing reference versions, absent account features or denied probes remain visible. The owner's existing SQL evidence cannot establish Python/classic compatibility. Routine tests/required CI never load `.env`, receive personal credentials or contact a real workspace.
- A missing Python binding, unusable connector transport, required mode failure, unresolved aggregate schema, unsafe cancellation/recovery or missing compatible public wheel blocks its consumer/gate. Continue independent ready work; only an explicit recorded scope/dependency revision can narrow the release.

## Candidate assembly and completion checklist

B023 freezes the implemented, evidenced scope after B020/B021/B022. The contract above is the target used to implement it now; B023 cannot silently lower that target. Proposed output: `artifacts/releases/0.1.0/` with scope/case/versions manifests, installation and G1–G4 reports, process/network/offline evidence, recovery/restore report, known limitations, artifact hashes and the owner's publication checklist. These are future concrete deliverables, not files to create empty in this planning slice.

- B001–B023, B043–B048 and all **85** required child tasks are done with criterion-level evidence; no child replaces the parent integration gate. Research can finish with an explicit negative decision only where its criteria allow that; promised implementation still blocks.
- G1–G4 pass from the exact candidate for all declared variants, including separate notebook-extra runs. Keep declared unsupported/later cases visible: full Connect and analysis binding, Thrift, shared driver state, JVM tasks, cloud provisioning/performance and broader access/environment policies.
- Build wheel/sdist, pin a publicly available compatible Sparkless wheel and selected native dependencies, and install cleanly on each advertised Python/OS/architecture combination. Start with Python 3.11/3.12 and Linux x86_64/macOS arm64 candidates; record failures and advertise only the evidenced support set.
- Verify the base workflow without notebook packages, Java/full PySpark/delta-spark/Docker or real Databricks access. Provision dependencies first, then allow only required local traffic for the offline gate; prove inherited credentials cannot redirect clients.
- Verify migration from the actual current workspace/config/metadata versions, ownership, coordinated backup/restore, third-party notices and project license/distribution ownership. A future migration/version is not implemented by writing this document.
- Generate truthful current-behavior documentation and capability reports from evidence. Review artifacts and unresolved limitations. Public version **0.1.0** remains the initial release; publication is a separately authorized action.
