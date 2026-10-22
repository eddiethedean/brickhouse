# ADR 0014: Durable Delta operation journal and replay recovery

- Status: accepted
- Date: 2026-10-19
- Related: ADR 0008, ADR 0012, ADR 0013
- Implemented by: B008.3, B019.2.1, B019.2.2, B019.2.3

## Context

The delta-rs write call is not idempotent when the same application transaction identifier is submitted again. A process can also die after a Delta commit but before SQLite records the returned version. Retrying from the caller without durable identity can duplicate rows or hide an uncertain commit.

## Decision

Each registered table uses a workspace SQLite journal keyed by table ID and operation ID. Before provider dispatch, BrickHouse durably records the operation fingerprint, schema fingerprint, input digest, mode, row count, base version, and a monotonically allocated transaction version. A write or selected SQL UPDATE/DELETE/MERGE carries the table ID and transaction version as the provider application transaction, plus operation ID and payload fingerprint in commit metadata. For SQL mutations, the input digest covers the canonical operation kind, predicate, and assignment expressions. MERGE also fingerprints its exact registered source snapshot identity, schema, and Arrow bytes.

An exact operation replay returns the original commit result only when the provider reports the expected transaction version and retained Delta history contains the same operation ID and fingerprint at one exact commit version. For a SQL mutation, history also supplies the exact affected-row metric. Reusing an operation ID with different inputs or options fails. When provider outcome or history cannot establish the exact commit, the journal remains uncertain and automatic writes are blocked; absence of evidence is not interpreted as proof that no commit occurred.

Metadata schema version 4 introduced the operation journal. Schema version 18 adds selected UPDATE/DELETE modes and terminal `no_op`/`failed` outcomes; schema version 19 adds the selected MERGE mode while transactionally preserving earlier operation records. A no-op provider result consumes no Delta transaction version. A provider rejection becomes terminal `failed` only if the table version and application transaction are still at the recorded base; if the process disappears before that outcome is persisted, the operation remains uncertain. MERGE history supplies the exact inserted-plus-updated row count. The journal uses SQLite transactions and remains scoped to one workspace and local provider. Delta history reconciliation scans the latest 100 entries.

## Consequences

- A child process may be terminated after the Delta commit, then a reopened workspace can reconcile the exact local commit without a duplicate append.
- A provider error after dispatch is recorded as uncertain; neither the same operation nor a different write is blindly dispatched while reconciliation remains unresolved.
- A different payload cannot reuse the operation ID.
- Successful zero-row SQL mutations preserve their result without manufacturing a Delta commit or consuming a provider transaction version.
- Confirmed pre-commit provider rejection preserves both the Delta snapshot and the next available provider transaction version.
- If the operation's history entry is beyond the scanned window, removed by retention, malformed, or otherwise unavailable, the service remains conservative and reports uncertainty.
- This does not establish atomicity or concurrency safety across distinct workspace databases or separate hosts. Locks serialize aliases within one process, and the workspace owner lock scopes service ownership to one workspace.
- Jobs, external side effects, distributed transactions, and startup-wide automatic reconciliation are not implemented by this decision.

## Restore-safe committed replay (B064.8)

After a workspace snapshot restore rebases managed table paths, an already committed operation ID may replay only when durable journal fields prove semantic identity: the same mode, schema fingerprint, canonical input digest, and row count. The service returns the recorded commit result without relying on the old absolute-path fingerprint. Prepared or uncertain operations retain strict checks; changed payloads or modes still raise `OperationKeyReuseError`. This exception is limited to already committed/no-op history and does not weaken provider reconciliation. See [B064 focused evidence](../../artifacts/conformance/B064-phase-0.1-review-fixes.md).

## Evidence

See [B008.3 local operation journal and recovery evidence](../../artifacts/conformance/B008.3-operation-journal-recovery.md) and [B019.2.1 SQL UPDATE/DELETE evidence](../../artifacts/conformance/B019.2.1-sql-update-delete.md). The evidence includes local Delta commits, exact replay and process-death recovery, operation-key reuse rejection, monotonic transaction versions, uncertain outcomes, provider failure/no-op handling, and schema 3 to 4 / 17 to 18 migration.

The selected single-key MERGE is recorded in [B019.2.2 evidence](../../artifacts/conformance/B019.2.2-sql-merge.md), including one atomic delta-rs commit, source-key validation, exact history metrics, and schema 18 to 19 journal migration. [B019.2.3 evidence](../../artifacts/conformance/B019.2.3-sql-describe-history.md) verifies that SQL `DESCRIBE HISTORY` reads the bounded public delta-rs history view and reports only provider-committed versions; the SQL output does not derive rows from the operation journal.
