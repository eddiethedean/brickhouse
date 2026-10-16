# 12 — Delivery, configuration, and operations

## Workspace layout

Proposed workspace-owned state, outside the source checkout:

```text
workspace/
  brickhouse.toml       # non-secret configuration and format version
  metadata.sqlite      # catalog/jobs/operation metadata
  tables/              # managed Delta directories keyed by stable IDs
  files/               # workspace files and local DBFS/volume roots
  staging/             # incomplete inputs and transient result batches
  results/             # bounded statement artifacts
  runs/                # attempt logs, manifests, notebook artifacts
  checkpoints/         # versioned streaming state when supported
  integrations/        # provider-owned state with explicit backup hooks
```

User-configured external table locations remain external. Credentials must not be serialized into run manifests or ordinary config. Protect local secret storage with filesystem permissions and document its trusted-machine threat model; implement encryption only with a real key-management design. Do not claim local tokens make arbitrary notebook execution safe.

## Configuration and lifecycle

Use a typed configuration model with precedence: explicit invocation arguments, designated environment variables, workspace config, defaults. Document the actual supported settings; reject invalid/unknown settings where silent acceptance would imply behavior. Separate compatibility profile from engine selection and client versions.

Bind to loopback by default. One workspace has one active service owner, guarded by a lock with stale-owner recovery. Worker processes call that service for writes. Validate ownership and schema version before opening writable state. A second server must fail clearly rather than run competing migrations.

`init` creates a workspace and bootstrap namespaces idempotently under an explicit policy. `serve` manages API and supervisor startup/shutdown. `run` reuses the supervisor. Embedded `Workspace.local` must have equivalent lifecycle behavior and deterministic cleanup.

All processes must propagate cancellation and exit status. Terminate descendant process groups after a configurable grace period; avoid orphan kernels. Bound queue length, concurrent jobs, query results, logs, and staged files. Reject overloaded requests with a documented error rather than exhausting the host.

## Persistence and recovery

SQLite migrations are versioned and transactional where supported. Refuse to open a newer workspace format with an older binary. Test upgrades from supported prior versions; define backup requirements before destructive migrations. SQLite WAL and filesystem locking assumptions must be documented; network filesystems are unsupported until tested.

Use an operation journal for transitions spanning metadata and Delta/files. Record durable intent, commit identifiers, and finalization. Startup reconciliation must distinguish safe retry, completed operation, and uncertain outcome. Avoid blindly rerunning a Python job whose external effects cannot be reconciled.

Back up a quiesced workspace or use a coordinated snapshot procedure that captures consistent SQLite, Delta logs/data, and provider state. Copying a live SQLite main file alone is not a backup. External tables need a manifest and an explicit inclusion policy; do not delete or duplicate them as an incidental backup action. Restore tests must read real data and job history.

## Observability

Structured events include workspace, request, job/run/task/attempt, statement, table, operation, and Delta version IDs as applicable. Preserve user stdout/stderr separately from service events. Redact known secrets and avoid logging SQL parameter values by default; redaction cannot guarantee arbitrary user code will never reveal secrets.

Expose health/readiness, version/profile/provider diagnostics, and a capability report. Report simulated cluster/warehouse metadata clearly. Metrics include queue time, execution time, failure category, result bytes, and worker resource use where supported. Do not silently send telemetry outside the machine.

## Build and CI

Use a standard Python build backend and a reproducible development lock. Release metadata should declare tested dependency ranges; lock files support reproducibility without forcing every downstream application to use an exact transitive lock. Keep native provider/client version fixtures separately identifiable.

Suggested CI lanes: lint/type checks; domain tests; native table tests; SDK/process integration; clean wheel installation; JVM-free/offline golden path; optional-extra workflows. All consume reference fixtures locally and receive no personal Databricks credentials. The plan requires future implementation tests; this planning pack contains no runnable implementation test suite.

An optional manual `workflow_dispatch` capture workflow uses the owner's personal Databricks account to resolve selected behavior questions and refresh sanitized fixtures, following [document 15](15-personal-databricks-verification.md). The owner reports `DATABRICKS_TOKEN` is already configured as an Actions secret and in ignored local `.env`; access is not verified. Supply `DATABRICKS_HOST` and permitted existing compute/scratch settings separately. Inject the token into the capture job only; constrain execution to reviewed default-branch code and curated probes, serialize scratch use, bound execution, and sanitize before artifact upload. No automatic push/PR/schedule capture or required release dependency. Normal tests and runtime must not load `.env` or inherit real-workspace authentication.

Initial platform candidates are Linux x86_64 and macOS arm64 on Python 3.11/3.12. They become supported only after wheel and workflow gates pass. Test Windows and additional architectures before advertising them. Do not force users to install a Rust compiler to compensate for an untested wheel matrix.

## Distribution and release

Reserve/configure repository and package ownership separately. A package-name lookup does not reserve a PyPI name. Resolve any name collision before publishing, preserving the agreed project identity where possible. The plan does not create a repository, reserve names, or choose final legal ownership/license terms.

A release candidate includes wheels/sdist, install report, capability snapshot, dependency/native version inventory, notices, changelog, migrations, and example workflows. Public examples must execute against that candidate. Publish only with owner authorization; do not embed publishing credentials in project files.

## Supply chain boundary

Pin copied source provenance and audit intentional private-API use. Validate optional native/service artifacts independently. Separate local development dependency overrides from public install requirements. Environment creation for jobs must use an explicit dependency manifest and provisioning policy; arbitrary job-time package installation is unsupported initially.
