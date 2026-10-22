# ADR 0018: Verify the published Sparkless Python provider

- Status: adopted dependency/API decision; production integration remains B010
- Date: 2026-10-20
- Tasks: B046.1–B046.3; refines ADR 0017's artifact prerequisite

## Context and decision

The owner reported publication of Sparkless 4.14.0. The release source and verified macOS arm64 wheel expose a public Python catalog/read/write callback API. The earlier Rust-only absence finding applies to the historical revision and published 4.13.4, not this release. [The API inventory](../upstream/sparkless-4.14.0-python-provider.md) records signatures, source/wheel hashes and limits; [focused callback verification](../../artifacts/conformance/B046.2-sparkless-4.14.0-python-binding.md) records 56 passing release tests.

Adopt **4.14.0** as the exact engine candidate. Verify the already-published implementation and wheel instead of rebuilding or reimplementing that API. B046's artifact criteria now explicitly accept a hash-verified published source/wheel; a source-built candidate remains the alternative when no compatible release is available. Required callback, real-provider and provenance checks are retained. This supplies a publicly obtainable candidate for B023 to qualify on its own advertised platform matrix; known type defects prevent treating it as a compatible release yet.

## Integration constraints

Use only `sparkless.external_catalog` public descriptors/requests/receipts and the public session registration API. Keep native sessions on their owner thread/process. `close()` must detach the provider, stop the native session, and release BrickHouse's native-session reference on that same owner thread; do not rely on cyclic garbage collection to drop Sparkless' unsendable PyO3 session on an arbitrary thread. Preserve one catalog/table/commit authority; callbacks write through the journaled Delta service. Validate protocol/features, schema and canonical table ownership before returning a native descriptor. Do not classify native diagnostic strings to route operations or silently retry writes.

The writer materializes data before callback dispatch. Small probe/callback bounds are not a bounded-memory engine guarantee. Callback exception wrapping and synchronous execution are explicit limits. In-process async cancellation is unavailable; fresh worker process containment must preserve actual committed outcomes. Path saves, insertInto, SQL UPDATE/DELETE and other undelegated paths need explicit BrickHouse adaptation/rejection and cannot be claimed from the hook's existence.

B046.3 verified 14 selected real Delta commit/lifecycle checks but reproduced parent-null ingestion and all-null-field collection failures, filed as [#1694](https://github.com/eddiethedean/robin-sparkless/issues/1694) and [#1695](https://github.com/eddiethedean/robin-sparkless/issues/1695). [The report](../../artifacts/conformance/B046.3-sparkless-4.14.0-provider.md) replaces the missing-API diagnosis with exact typed-contract blockers; B046.3 and B010.1 remain blocked until they pass. B010 remains responsible for production sessions/writers and subsequent SQL/job/notebook release gates. This decision does not establish Databricks behavior or clear G1–G4 by itself.

This decision authorizes no release publication or cloud workload. No upstream implementation edit or credential change was performed; the owner separately authorized reporting blocking bugs as upstream issues.

## 4.14.1 verification addendum — 2026-10-20

Adopt published **Sparkless 4.14.1** as the current hash-locked developer candidate, superseding 4.14.0 for new integration work. The release fixes the exact #1694 and #1695 nullable-struct cases: the six-case reproducer and 14 selected real local Delta provider checks now pass against the verified macOS arm64 wheel. [B046.3 evidence](../../artifacts/conformance/B046.3-sparkless-4.14.1-provider.md) records wheel SHA-256 `801160abb681dd71e7cf5ce534173da39890e0caa8866d9c99c55e8ee3229893`, versions and remaining upstream limits.

B046.3 and parent B046 are complete. B010.1–B010.3 provide an internal Sparkless adapter for version-pinned Delta reads, managed table creation, registered named append/overwrite, explicit writes to registered local paths, and coexisting session-state isolation through the common table service. The adapter consumes resolved serverless defaults and one explicit classic-standard `spark35-local-v1` configuration; it rejects classic sessions without explicit ANSI/timezone values and unmapped runtimes. The write layer rejects direct native path/insert/DataFrameWriterV2 bypasses and unsupported options. [B010.1 evidence](../../artifacts/conformance/B010.1-sparkless-read-adapter.md), [B010.2 evidence](../../artifacts/conformance/B010.2-sparkless-write-adapter.md), and [B010.3 evidence](../../artifacts/conformance/B010.3-sparkless-session-isolation.md) record the local behavior and limits. SQL, job-worker integration, other classic settings/runtimes, and Databricks behavior remain unverified. This addendum changes no release publication or Databricks workload authorization.

## 4.15.0 update — 2026-10-21

Advance the developer-only Sparkless pin to published **4.15.0**. Its release adds public cooperative cancellation with a terminal `SparklessCancelledError`, a thread-safe session cancellation handle, bounded Arrow batch export, and structured provider errors. B011.3.3 integrates the cancellation API into local SQL statements and verifies active/queued cancellation, timeout cancellation, warehouse-stop races, commit fencing, and committed CTAS preservation. See [the 4.15.0 conformance report](../../artifacts/conformance/B011.3.3-sparkless-4.15.0-cancellation.md).

This update resolves the Sparkless cancellation dependency for B011.3.3. B064.2 subsequently integrates the public bounded Arrow batch API into SQL result collection; this does not change worker input-provider materialization. Existing B010/B015 conformance remains tied to the exact 4.14.1 artifacts recorded in those reports. The API additions establish neither broader Spark semantics nor Databricks parity, and they authorize no cloud workloads or package publication.

## 4.15.1 release update — 2026-10-22

Sparkless **4.15.1** is published on PyPI and tagged upstream as `v4.15.1` (release commit `9b1b620`). Upstream PR [#1716](https://github.com/eddiethedean/robin-sparkless/pull/1716) closes the UTC-aware timestamp comparison report [#1714](https://github.com/eddiethedean/robin-sparkless/issues/1714); PR [#1717](https://github.com/eddiethedean/robin-sparkless/pull/1717) closes nested struct SQL field report [#1715](https://github.com/eddiethedean/robin-sparkless/issues/1715). The upstream changelog documents nested struct resolution in 4.15.1. See the [published PyPI release](https://pypi.org/project/sparkless/4.15.1/) and [upstream release tag](https://github.com/eddiethedean/robin-sparkless/releases/tag/v4.15.1).

BrickHouse advances its exact dependency pin to `sparkless==4.15.1` after rerunning the B019 SQL statement corpus. The published wheel resolves the B019.1.4 nested-field projection. However, BrickHouse's exact real-local-Delta/external-catalog reproduction for B019.1.3 still raises the #1714 timezone mismatch with the PyPI 4.15.1 wheel. Follow-up [Sparkless issue #1720](https://github.com/eddiethedean/robin-sparkless/issues/1720) records the discrepancy. The original fixtures remain versioned historical evidence; their current addenda identify what 4.15.1 changes and what remains unresolved. No Databricks reference request or cloud workload was made for this release update.


## 4.15.2 release update — 2026-10-22

Sparkless **4.15.2** is published on PyPI. Upstream PR [#1721](https://github.com/eddiethedean/robin-sparkless/pull/1721) resolves the published-wheel timestamp mismatch reported in [issue #1720](https://github.com/eddiethedean/robin-sparkless/issues/1720), preserving timestamp instants in Delta comparisons. BrickHouse advances the exact pin to `sparkless==4.15.2`; the verified macOS arm64 CPython 3.8+ ABI3 wheel has SHA-256 `7d12d6f76212e52586cbe07ab12152f4c7dcf04bd2873532751f1335ee81cfb2` ([PyPI release](https://pypi.org/project/sparkless/4.15.2/)).

The B019.1.3 fixture now expects the UTC-aware Delta predicate `event_ts >= TIMESTAMP '2026-10-18 03:30:00'` to return `order_id=2`; that focused case and all 35 SQL statement-service tests pass locally against the published 4.15.2 wheel. No Databricks account or remote compute was used. At the time of this dependency update, the full current BrickHouse candidate matrix had not yet been rerun. The later 2026-10-22 qualification entry records that selected local G1–G4 matrix passing against the exact candidate wheel. Neither result claims general timestamp, SQL, Spark, or Databricks parity; the archived 4.15.1 candidate evidence remains unchanged.
