# 04 — Sparkless upstream work

## Boundary

BrickHouse owns Databricks APIs, catalog persistence, orchestration, protocol adapters, notebook context, and capability reporting. Sparkless owns DataFrame semantics, expressions, SQL execution, types, and reusable execution/storage extension hooks.

The initial B003 source inspection used Sparkless 4.13.3 at commit `b73067252abd0720ea91ac447bfd3ce027e96a88`. It has engine-neutral traits under `crates/robin-sparkless-core/src/engine/`, a Polars backend under `crates/robin-sparkless-polars/`, root Rust facades under `src/`, and Python/PyO3 bindings under `python/`. B003.1 found local catalog operations, `newSession`, local Delta IO, and a PyArrow-input path that materializes through `to_pylist`; it did not find an external catalog/provider hook, delegated writer callback, Arrow stream export, or provider error/capability contract. B003.2 demonstrated a temporary primitive-only Python bridge and reproduced Decimal ingress loss and `format("delta").save(path)` writing non-Delta data; see [Sparkless issues #1683](https://github.com/eddiethedean/robin-sparkless/issues/1683) and [#1684](https://github.com/eddiethedean/robin-sparkless/issues/1684). B003.3 proved same-thread config/temp-view isolation but reproduced a PyO3 panic when a session object is transferred to another thread. The exact source evidence and contract constraints are in the [session/callback probe](../../artifacts/conformance/B003.3-session-isolation.md).

On 2026-10-19, the published `sparkless==4.13.4` wheel (tag `v4.13.4`, commit `34049bde128931e21bf34b761cfa10007d62af5e`) passed local reproductions for Decimal Delta reads (#1686), PyArrow Decimal ingress (#1683), and Delta-format path writes (#1684). Exact package hash, inputs, and results are recorded in [B004.2](../../artifacts/conformance/B004.2-typed-interoperability.md). These cases clear the prior feasibility blockers; they do not add BrickHouse's missing external catalog/provider hooks, bounded Arrow interchange, structured provider errors, or thread-safe session ownership.

B009.1–B009.3 implement and verify synchronous Rust external-catalog hooks in [Sparkless PR #1692](https://github.com/eddiethedean/robin-sparkless/pull/1692), tested commit `4923b857b0208a5d81116e7782f17110d4558eb6`, based on 4.13.4. The hooks resolve/read external Delta snapshots, delegate `saveAsTable()` append/overwrite and SQL `INSERT INTO`, and transfer buffered Arrow IPC streams with validated commit receipts. The upstream crate gate passes, and public signatures, test scope, and limits are recorded in [B009.2](../../artifacts/conformance/B009.2-sparkless-write-delegation.md) and [B009.3](../../artifacts/conformance/B009.3-sparkless-hook-isolation-and-availability.md). That availability finding is historical. PR #1692 is now merged; the published 4.14.0 wheel contains the public Python bridge described below.

On 2026-10-20, published **Sparkless 4.14.0**, release commit `e46ddc6b9aa8903b0d98c0401791fffb858c9770`, exposes `set_external_catalog_provider()` and typed public Python descriptors/requests/receipts. B046.1 records the [actual API and ownership contract](../upstream/sparkless-4.14.0-python-provider.md); B046.2 passed 56 focused release tests. [B046.3](../../artifacts/conformance/B046.3-sparkless-4.14.0-provider.md) passed 14 selected real Delta commit/lifecycle checks but found nullable-struct failures: ingestion loses parent validity ([#1694](https://github.com/eddiethedean/robin-sparkless/issues/1694)), and `collect()` collapses a valid all-null-field struct ([#1695](https://github.com/eddiethedean/robin-sparkless/issues/1695)). The artifact/API-absence blocker is superseded; B046.3 and B010.1 remain blocked on this typed envelope.

[ADR 0018](../decisions/0018-published-sparkless-python-provider.md) adopts 4.14.0 as the exact hash-locked developer candidate and permits verification of published artifacts instead of a redundant build. The production adapter remains B010. Callbacks are synchronous and full-frame IPC is buffered before callback bounds apply. Sessions remain unsendable owner-thread objects; arbitrary Python exception types are wrapped. Separate owned sessions and process interruption were probed; cross-thread transfer and asynchronous cancellation remain unsupported. These limits require contained adapter decisions and do not establish Databricks behavior.

## Proposed coordinated changes

| ID | Change | Required contract and evidence |
|---|---|---|
| S001 | External catalog provider | Session-scoped resolution of multipart identifiers to a typed table handle; stable ID, canonical location, schema, version, provider, permissions context; no global resolver |
| S002 | Table write delegation | `saveAsTable`, `insertInto`, supported path saves, and SQL mutation entry points delegate when a provider owns the target; one commit service; preserve schema/mode/options |
| S003 | Arrow import/export | Typed Arrow batches/streams between Sparkless and Python/native providers; preserve nullable nested types, decimals, timezone metadata, and bounded materialization behavior |
| S004 | Session isolation | Context-managed configuration, namespaces, temporary views, UDF registries, and runtime state; concurrent sessions must not inherit each other's settings |
| S005 | Stable errors and capabilities | Structured engine error category and context; supported operation inspection; no string matching of Polars diagnostics for control flow |
| S006 | External relation binding | SQL/DataFrame reads share catalog resolution and table snapshots; provider tables can enter expressions/joins without lossy JSON conversion |
| S007 | Worker import compatibility | Prefer ordinary Sparkless exports; add reusable missing names/semantics upstream while BrickHouse owns worker-only import routing |
| S008 | Future incremental execution | Bounded batch interface, expression reuse, and cancellation points for streaming integration; no claim of full streaming from a no-op API |

S001–S006 are designs to prove, not names of existing APIs. B003 determines the smallest compatible additions. B009 records the tested Rust slice; B046 verifies the published public Python bridge and tracks remaining typed failures. B010 consumes an actual tested release or explicitly recorded development artifact; B023 separately requires a publicly available compatible release.

## Provider design considerations

- Return engine-neutral descriptors from catalog hooks. Keep FastAPI, SQLite, Databricks SDK, and BrickHouse types out of Sparkless.
- A missing external table is distinct from an unavailable provider. Define shadowing: session temp views first where the target profile requires it, then bound namespace/catalog resolution. Never silently strip a catalog prefix and return an unrelated local table.
- Resolve names to stable table identities. Do not interpolate SQL identifiers or filesystem paths from unvalidated user input.
- Python callbacks invoked by Rust must specify GIL handling, reentrancy, cancellation, exception conversion, and thread ownership. Test provider callbacks under concurrent sessions; avoid invoking network callbacks while holding an engine-wide mutex.
- Writer delegation must cover methods users actually call. Reject bypasses until supported. Passing a JVM catalog class name in `spark.conf` must never report that a Java plugin was loaded.
- An external mutation result must include committed version and observed schema. Invalidate cached plans/views by version rather than assuming all session copies remain valid.

## Delta ownership

**B004.3 selected Python `deltalake==1.2.1` as the phase 0.1 mutation provider.** Sparkless remains the semantic engine and reader; registered table writes must delegate through BrickHouse's table service to this one provider. B004.3 tested its transaction identity and checkpoint behavior against Sparkless 4.13.4. This provider is selected for integration but is not yet part of BrickHouse runtime behavior.

Do not require Sparkless and Python deltalake to use numerically identical package versions; their dependency versions differ. Require agreement on Delta protocol and features, proven by bidirectional data tests. B004.3 observed local unpartitioned tables with Delta reader version 1, writer version 2, and no feature lists; the transaction-bearing classic checkpoint was readable in Sparkless 4.13.4. Column mapping, deletion vectors, partitions, timestamp variants, generated columns, CDF, V2 checkpoints, and cloud storage remain unverified and cannot be advertised from this probe.

Delta `SetTransaction` actions are durable recovery identities, but `deltalake` does not suppress a repeated append by itself: replaying the same `app_id`/version produced a second commit and duplicate row in B004.3. The table service must consult `DeltaTable.transaction_version()` before a retry, pair the identity with the SQLite payload fingerprint, and serialize each table's monotonically increasing transaction versions. A matching transaction plus retained operation history can reconcile the commit; absent, expired, conflicting, or unresolvable state stays uncertain and blocks automatic retry. See [ADR 0008](../decisions/0008-delta-mutation-and-recovery.md) and [the crash/retry probe](../../artifacts/conformance/B004.3-transaction-recovery.md).

## Coordinated delivery

1. Write a small failing behavior case demonstrating the missing hook.
2. Implement reusable Sparkless changes under its repository instructions.
3. Run targeted Rust/Python tests and the required upstream checks.
4. Record the commit and artifact used by BrickHouse development.
5. Integrate through a narrow adapter and test both the new and oldest supported Sparkless releases.
6. Require a published compatible Sparkless wheel before a normal public BrickHouse release. Local source overrides are acceptable for development; they are not user installation instructions.

No automatic publication is authorized by this document. If a required upstream artifact is unavailable, keep dependent tasks blocked with a precise contract reference and continue independent work.
