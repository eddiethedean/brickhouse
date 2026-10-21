# ADR 0008 — Delta mutation provider and operation recovery

- Status: accepted for phase 0.1 integration design
- Date: 2026-10-19
- Related task: B004.3

## Context

B004.1 exercised native Delta create/read/append/update/delete/merge operations through Python `deltalake==1.2.1`. B004.2 verified typed cross-reads with published Sparkless 4.13.4. B004.3 tested transaction identity, checkpoints, a killed writer process, and a retry against real local Delta tables.

## Decision

Select Python `deltalake==1.2.1` as the single mutation provider for phase 0.1. Sparkless remains the semantic engine and reader. Registered table writes must route through the BrickHouse table service and the selected provider. This is a provider selection, not a claim that the provider is already integrated in BrickHouse.

Use one stable, table-scoped Delta application ID with a strictly increasing transaction version assigned by the per-table serialized SQLite operation journal. Before dispatch, persist the BrickHouse operation ID, payload/options fingerprint, Delta app ID/version, and operation state. Include the Delta `SetTransaction` action and BrickHouse operation metadata in the same mutation commit. Do not allow a later BrickHouse mutation to advance that table's app transaction version until the pending journal row is finalized.

The provider's transaction action is a recovery identity, not an automatic idempotency guard. The local probe shows a raw repeated append with the same app ID/version writes a second commit and duplicates the row. Before retrying, the table service must inspect `DeltaTable.transaction_version(app_id)` and skip dispatch when it matches the journal's expected version and fingerprint.

After `COMMITTING`, an exact app transaction match confirms that the commit occurred. The table service must also resolve the exact Delta table version from retained commit history using the operation metadata. Only after both are reconciled can it finalize the journal and return success. If identity, payload fingerprint, or exact commit version cannot be reconciled, retain an explicit uncertain result and block retries. Do not infer an uncommitted operation from absent history or a transaction lookup that may have expired.

Use local unpartitioned tables with the observed Delta protocol reader version 1/writer version 2 and no table features as the initial provider envelope. A configured `delta.setTransactionRetentionDuration` must exceed the recovery horizon; if a pending operation outlives its transaction identity or commit history, report uncertainty. No broad protocol compatibility claim is made.

## Consequences

- Python delta-rs is the one mutation authority; Sparkless writes to registered tables must delegate through BrickHouse before the integration is considered complete.
- The operation journal, per-table serialization, fingerprint checks, version lookup, and exact Delta commit-version reconciliation are mandatory parts of B008.
- Raw retries through `write_deltalake` are unsafe; `app_transactions` alone does not prevent duplicate writes in the tested version.
- Cloud storage, concurrent external writers, advanced Delta features, and configured transaction retention need separate evidence. An unresolved commit is surfaced as uncertain rather than automatically repeated.
- No `deltalake` runtime dependency or public API claim is added until the adapter integration task selects and tests the package boundary.
