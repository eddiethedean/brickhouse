# 18 — Emulator first, self-hosted platform afterwards

Updated: **2026-10-20**. Status: **future planning, not deployed functionality**. Planning task: B047. Decision: [ADR 0034](../decisions/0034-emulator-first-self-hosted-platform.md).

## Priority and entry boundary

BrickHouse's primary goal remains a Databricks behavioral emulator that executes without a JVM. Release phases **0.1–0.9 remain the emulator roadmap**. Their existing compatibility requirements, native execution rules, task dependencies and evidence remain required. Self-hosting must not displace those requirements or make local installation depend on a container runtime, orchestration system, remote worker, identity server or cloud account.

After the emulator is stable, a separate deployment track may turn that same core into a self-hosted lakehouse workspace with explicitly bounded Databricks-compatible workflows. This is an additional operating model, not a claim to distribute Databricks software or reproduce all its products. The future distribution's project license, dependency notices and branding must be resolved before publication under the existing release policy.

Completing phase 0.9 does not automatically declare the emulator stable, release 1.0, or start deployment implementation. **B049 / SH.0** establishes a recorded stability baseline after B042. All deployment implementation depends transitively on that gate. Deployment stages have no assigned package versions or calendar dates. Independent emulator work remains selectable by its existing prerequisites; these stages do not authorize cloud provisioning, publication, spending or reference-account use beyond document 15.

## What each emulator phase contributes

These are reviews of the existing phase's contracts, not requirements to build deployment infrastructure during that phase. Preserve the concrete local implementations until an actual second implementation warrants an abstraction. B048 owns the bounded initial architecture review; later epics record relevant findings when decomposed.

| Emulator phase | Primary outcome retained | Boundary to preserve for later deployment | Evidence and owners |
|---|---|---|---|
| **0.1** | Shared native tables, Sparkless/SQL, bounded jobs/notebooks, clients and recovery | One catalog/commit authority; separate domain, transport, persistence and runtime; stable resource IDs; contained local paths, PIDs and sockets; explicit trusted-local boundary | B048 reviews the implemented G1–G4 paths, records concrete adaptation points and uses existing workload evidence. B023 requires that review. No remote backend, identity enforcement or deployment package is added. |
| **0.2** | Expanded orchestration and local authorization/audit | Carry the same identity decision through API, SQL, worker, file and secret paths; distinguish service policy from arbitrary Python's host access; durable schedule/admission records | B024/B028 retain allowed/denied and recovery fixtures. Document identity provenance and trusted-code bypass limits. Local authorization does not establish authenticated network service or sandboxing. |
| **0.3** | MLflow, Python model serving and dbt | Explicit authority for provider state; bounded provider lifecycle, artifacts, credentials and backup responsibilities; all materializations use the table service | B025–B027/G6 record provider endpoints and state ownership. No new hosted UI, remote model service or deployment supervisor is required. |
| **0.4** | Native vector indexes and feature-engineering research | Scope index/model/table identity and access consistently; track source Delta versions and rebuildable versus authoritative artifacts | B029/B038 retain authorization, restart and source-version evidence. A working index or feature API does not establish an isolated shared service. |
| **0.5** | Durable ingestion, micro-batches and selected stateful operations | Durable source/batch/plan identity, exclusive checkpoint ownership and idempotent sink reconciliation; local ownership is not a distributed lease | B030–B032/B035/G5 record replay, competing-owner rejection and explicit delivery guarantees. Multi-host execution remains later work. |
| **0.6** | Declarative batch pipelines, expectations and CDC | Version definitions/environments; expose dataset-level commits and partial-run recovery; no assumed cross-table transaction | B033/B034/G5 retain reproducible refresh/CDC fixtures and run evidence reusable by a future deployment scheduler. |
| **0.7** | Native Connect, broader SQL protocols and Delta Sharing | Reuse session lifecycle, identity/policy, result ownership and cancellation across protocols; contain protocol adapters | B036/B037/B039/G4 prove exact local clients and authorized sharing. Network authentication and hostile-client exposure require separate deployed-profile tests later. |
| **0.8** | Platform coverage, storage adapters and measured performance | Identify native versus Python IO, credential scope, real provider atomicity and hardware limits; keep local metadata ownership explicit | B040/B041 record measured limits and storage evidence. Accepting a URI does not qualify remote storage; no forced metadata-database replacement, HA or distributed query engine. |
| **0.9** | Consolidated emulator conformance | Reproducible candidate, versioned compatibility denominator, install/migration/restore reports, remaining defects and operational limits | B042/G1–G6 remain emulator gates. Produce the evidence B049 will assess; self-hosted features are not required to finish B042. |

An unresolved future deployment design does not block emulator behavior unless it violates an existing invariant or the bounded B048 review. Do not reserve fake APIs, add no-op authorization hooks, or build generic provider frameworks solely to satisfy this plan.

## Deployment profiles and trust

Names below are proposed deployment profiles, independent of Spark semantics, compute/access labels, SDK versions and Databricks runtime references. They are not current config values or security claims.

| Target profile | Intended users and boundary | Required before claiming it |
|---|---|---|
| `local-trusted-v1` | One trusted developer on a local machine; existing local model | G1–G4 and truthful local limits. Compatibility tokens are not authentication; a child process is not a sandbox. |
| `server-trusted-v1` | Authenticated users in one administrative trust domain; everyone allowed to author arbitrary Python is trusted with the execution host's granted privileges | SH.1. Enforced service authorization, authenticated encrypted ingress and durable scoped secrets. Code authors' direct filesystem/network access remains an explicit limit; no hostile multi-tenant claim. |
| `server-isolated-v1` | Multiple principals executing code within a tested host/platform threat model | SH.3. Real workload isolation, scoped data/secret access, resource limits and negative escape/bypass tests for the selected boundary. Shared-kernel or other residual risks stay explicit. |
| `worker-pool-v1` | The isolated profile with jobs on explicitly enrolled remote machines | SH.4. Authenticated worker enrollment, fenced attempts/commits/checkpoints, artifact transport and failure recovery across hosts. One authoritative control service remains initially. |

Browser users, API users, job owners and service identities must be distinguished. Authenticating a caller does not establish permission to run code or fetch another principal's output. Standard/dedicated/serverless compatibility labels continue to describe selected Databricks behavior; they never substitute for deployment isolation.

## Post-stability stages

These stages are assigned in [ROADMAP.md](../../ROADMAP.md), with canonical criteria in [the ledger](../../planning/tasks.json) and [backlog](09-task-backlog.md). B050–B055 are future epics: decompose them into bounded children with inherited prerequisites, a finite supported platform/client matrix and concrete tests before implementation. An optional dependency cannot become a base install requirement through a deployment extra.

| Stage / owner | Deliverable | Completion gate |
|---|---|---|
| **SH.0 / B049** | Emulator stability and deployment-readiness record | Exact candidate and supported compatibility corpus; repeatable G1–G6 as applicable; migration/restore evidence; explicit defect triage, stability criteria and maintenance policy. No unresolved release-blocking correctness, corruption or native-execution defects for the declared corpus. Select the first single-host threat model and supported hardware/OS before SH.1. This does not mean complete Databricks parity. |
| **SH.1 / B050** | Authenticated single-server service for trusted code authors | S1: real remote client over verified TLS, enforced identities/grants on all advertised surfaces, durable scoped secrets with rotation/restart, bounded admission, durable storage, coordinated restore, startup/shutdown and an installable optional deployment artifact. No-auth mode is loopback-only and cannot accidentally select network exposure. |
| **SH.2 / B051** | Browser workspace and deployed optional services | S2: the same user can upload/edit supported source, submit/cancel a job, inspect authorized output, browse catalog data and execute bounded SQL through the existing services. Session/browser security and denial cases pass. Selected notebook/ML/dbt/vector integrations have authenticated endpoints, explicit provider state and coordinated backup/restore; absent extras stay absent. |
| **SH.3 / B052** | Isolated execution for shared use | S3: a selected real OS/container/VM isolation mechanism denies access to control metadata, other runs' files, unauthorized table snapshots/secrets/results and forbidden network/host resources. CPU/memory/process/disk/runtime limits, cleanup and cancellation are enforced for scripts, kernels and serving workers in scope. Re-run the emulator corpus inside this boundary. |
| **SH.4 / B053** | Remote worker pool and durable automation | S4: an enrolled worker on a second host executes a real job through the same catalog/commit service. Lease expiry, partition, stale/duplicate completion, cancellation and loss after Delta commit are reconciled. Shared data/staging and environment digests work through verified native IO. Selected streaming/pipeline workloads preserve checkpoint ownership and sink guarantees across reassignment. This distributes jobs, not a single query. |
| **SH.5 / B054** | Operational qualification | S5: repeatable clean install, upgrade/preflight, coordinated backup and restore to another host; tested restore-point/recovery-time objectives, retained job/table history, secret rotation, capacity/soak/overload tests, operator metrics and runbooks. Publish a finite support matrix and single-control-service availability limits. HA and control-database replacement remain separate decisions unless actually implemented and tested. |
| **SH.6 / B055** | Reviewable self-hosted distribution candidate | S6: G1–G6 plus S1–S5 for each claimed profile from exact artifacts, with deployment/provider/client versions, artifact hashes/notices, security and recovery reports, examples and operator documentation. Publication remains separately authorized. The local wheel workflow still passes independently. |

The browser workspace is an additional interface to BrickHouse services, not a second catalog or scheduler. A Databricks-compatible Terraform/bundle deployment into the emulator is different from installing and operating BrickHouse itself; existing B020 evidence proves only its selected client flows.

## Architecture evolution contracts

### Services and identity

The API layer authenticates and maps transport values into explicit application context; domain services own authorization decisions and invariants. The same rules must apply to SQL binding, worker requests, file/result retrieval, table commits, notebook operations and optional providers. A future UI calls those services. Do not duplicate rules in a browser or rely exclusively on a reverse proxy for authorization.

During emulator phases, document actual entry points and future principal provenance without pretending the current SDK token authenticates anyone. B028 supplies local policy semantics. SH.1 supplies real authentication, credential lifecycle, encrypted ingress and service enforcement; SH.3 supplies a tested execution boundary that prevents user code from bypassing those rules for the declared threat model.

### Runtime and worker authority

Keep process launch, POSIX signals, local PID identity, private sockets/pipes and local environment resolution in runtime adapters. Current workers remain fresh local children. Domain job/attempt/resource identities and durable outcomes must not be replaced by PID identity. Version private messages and keep credentials attempt-scoped.

A remote worker cannot acquire broad direct metadata credentials or become a second catalog/commit authority. SH.4 requires authenticated enrollment/revocation, coordinator-issued attempt leases and generations, heartbeat/deadline policy and validation at every state-changing/commit request. Lease loss must prevent stale workers from committing through the service; late events remain diagnostic evidence rather than overwriting a newer attempt. A lost response after a real commit stays subject to journal reconciliation. External Python effects still have no blanket exactly-once guarantee.

Shared-cluster compatibility IDs may remain simulated. Actual worker capacity/allocation, placement, queue time and resource use are measured separately. Remote jobs must never introduce Java, Spark drivers or hidden Databricks execution. Distributing one Sparkless query is a separate research track outside these deployment stages.

### Storage and metadata

SQLite on a tested local filesystem and one active workspace/control-service owner remain valid initial choices. Workers communicate with the service; they do not mount a shared writable SQLite database. Do not put SQLite on an unverified network filesystem to claim multi-host support. A replacement metadata backend or HA requires demonstrated need, its own migration/consistency decision and failover tests.

Stable workspace/table/artifact IDs are independent of deployment roots. Existing local canonical paths, symlink defenses and staged-input hashes retain their tested scope. SH.4 must select and test an explicit transfer/shared-storage contract, including server/worker root differences, bounded transfers, snapshot lifetime, URI/credential scope, checksums and cleanup. Python filesystem support does not prove native Sparkless/delta-rs IO works. Do not rewrite existing absolute table locations silently when moving a workspace; restore/relocation must validate an explicit mapping, identity and managed/external ownership policy.

All registered mutations continue through the table service and declared native Delta provider. Preserve optimistic-concurrency and uncertain-commit behavior when transport changes. A shared object store or multiple workers do not create cross-table transactions or solve external-writer concurrency by themselves.

### Packaging and operations

Retain one `brickhouse` distribution until document 02's split criteria are met. Deployment images, optional launch tooling and configuration must identify exact contained wheels/native artifacts and supported platforms. A container runtime can be an explicit deployment prerequisite; it stays absent from the base local workflow. No nonfunctional extra or untested orchestration template is a deliverable.

Separate non-secret config, runtime credentials, authoritative state and rebuildable caches. Define health/readiness, admission/draining, shutdown, logs/metrics, retention, backup consistency and version compatibility per profile. Never copy only a live SQLite main file and call it a workspace backup. Restore must read actual Delta data and histories, job outcomes and selected provider state. Secrets need an explicit protected backup/recovery policy, with no plaintext inclusion in ordinary artifacts.

## Evidence and scope control

S1–S6 are future release gates, not passing test IDs. Each epic freezes exact positive, denial, failure and recovery cases before implementation. Use real local TLS clients, storage, providers and processes; SH.3 needs the actual selected isolation mechanism and SH.4 needs a real second host before a multi-host claim. Local containers/processes can test protocol behavior but cannot alone establish that host boundary. No paid infrastructure is authorized here; if qualifying infrastructure is unavailable, keep the gate blocked and continue independent emulator work.

Record a deployment version/profile matrix alongside the existing semantic/compute/client matrix. C059–C064 start **planned** with no tested versions or evidence. Existing C001–C058 keep their states, versions and evidence. Neither planning completion, a reachable UI nor an HTTP 200 establishes deployed security, operational readiness or Databricks parity.

The [testing plan](07-compatibility-and-testing.md) owns the gate summary and [operations plan](12-delivery-and-operations.md) distinguishes current local behavior from these future contracts. Keep roadmap, ledger/backlog, capability inventory and status synchronized when scope or stage readiness changes.
