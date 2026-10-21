# ADR 0014: Durable Delta operation journal and replay recovery

- Status: accepted
- Date: 2026-10-19
- Related: ADR 0008, ADR 0012, ADR 0013
- Implemented by: B008.3

## Context

The delta-rs write call is not idempotent when the same application transaction identifier is submitted again. A process can also die after a Delta commit but before SQLite records the returned version. Retrying from the caller without durable identity can duplicate rows or hide an uncertain commit.

## Decision

Each registered table uses a workspace SQLite journal keyed by table ID and operation ID. Before provider dispatch, BrickHouse durably records the operation fingerprint, schema fingerprint, input digest, mode, row count, base version, and a monotonically allocated transaction version. A write carries the table ID and transaction version as the provider application transaction, plus operation ID and payload fingerprint in commit metadata.

An exact operation replay returns the original commit result only when the provider reports the expected transaction version and retained Delta history contains the same operation ID and fingerprint at one exact commit version. Reusing an operation ID with different inputs or options fails. When provider outcome or history cannot establish the exact commit, the journal remains uncertain and automatic writes are blocked; absence of evidence is not interpreted as proof that no commit occurred.

The operation journal is part of metadata schema version 4. It uses SQLite transactions and remains scoped to one workspace and local provider. Delta history reconciliation scans the latest 100 entries.

## Consequences

- A child process may be terminated after the Delta commit, then a reopened workspace can reconcile the exact local commit without a duplicate append.
- A provider error after dispatch is recorded as uncertain; neither the same operation nor a different write is blindly dispatched while reconciliation remains unresolved.
- A different payload cannot reuse the operation ID.
- If the operation's history entry is beyond the scanned window, removed by retention, malformed, or otherwise unavailable, the service remains conservative and reports uncertainty.
- This does not establish atomicity or concurrency safety across distinct workspace databases or separate hosts. Locks serialize aliases within one process, and the workspace owner lock scopes service ownership to one workspace.
- Jobs, external side effects, distributed transactions, and startup-wide automatic reconciliation are not implemented by this decision.

## Evidence

See [B008.3 local operation journal and recovery evidence](../../artifacts/conformance/B008.3-operation-journal-recovery.md). The evidence includes a real local Delta commit, a process killed before SQLite finalization, workspace reopen, exact replay without duplication, operation-key reuse rejection, monotonic transaction versions, an uncertain provider failure, and schema 3 to 4 migration.
