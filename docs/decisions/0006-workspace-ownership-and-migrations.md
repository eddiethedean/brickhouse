# ADR 0006 — Workspace ownership and transactional migrations

- Status: accepted for the 0.1.0 implementation
- Date: 2026-10-19
- Related task: B006.2

## Context

Workspace bootstrap now creates a real versioned SQLite metadata file, but concurrent service processes could still open the same path. A persisted PID alone cannot prove liveness after crashes or PID reuse. Schema changes also need an atomic boundary that includes migration DDL, history, and SQLite `user_version`.

## Decision

Use a persistent `.brickhouse-owner.lock` file and a nonblocking POSIX advisory `flock`. `acquire_workspace_owner()` first validates the workspace, then obtains the exclusive lock and records only the local PID, workspace UUID, and acquisition timestamp with mode `0600`. A second owner receives `WorkspaceInUseError`. Normal context exit clears the diagnostic record and releases the descriptor. Process termination closes the descriptor in the OS; the next process can acquire the same lock and reports the prior record as stale diagnostic data. PID checks do not control recovery.

The lock file is never unlinked during ordinary release, avoiding inode replacement races between processes. The lock applies only to cooperating BrickHouse processes on a POSIX filesystem with working advisory locks. Network filesystems are unsupported until tested. The public owner handle is explicit; ADR 0007 records how B006.3 composes it into CLI and embedded lifecycle.

Apply migrations under `BEGIN IMMEDIATE`. Create the migration-history table, apply each ordered migration, insert its history record, and update `PRAGMA user_version` in the same transaction. Roll back all schema/history/version changes if a migration fails. Reject duplicate, skipped, inconsistent, or newer schema versions. Version 1 is the only migration currently defined; no upgrade from a prior released format is implied.

## Consequences

- Two local service processes cannot both acquire the same workspace lock.
- A process crash releases the kernel lock without a PID liveness timeout; incomplete diagnostic lock contents are overwritten by the next owner.
- Separate workspace paths keep separate lock files and SQLite databases.
- The owner handle and migration runner are implemented and tested. B006.3 composes ownership into the embedded context and diagnostics-only CLI server; data services remain future work.
- Windows and network-filesystem locking behavior are not claimed.
