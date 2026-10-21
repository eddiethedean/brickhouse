# ADR 0034: Preserve emulator priority and stage self-hosting after stability

- Status: accepted for planning; deployment remains unimplemented
- Date: 2026-10-20
- Planning task: B047
- Refines: ADR 0002, ADR 0017

## Context

The owner requested a long-term self-hosted platform plan across the release roadmap, then explicitly reaffirmed that the primary goal is JVM-free Databricks emulation and self-deployment comes afterwards. Existing phases 0.1–0.9 build emulator behavior; local authorization, client deployment workflows and simulated compute metadata do not establish a secure deployed service.

The current architecture already separates services, storage, transport and local workers. Preserving those boundaries is useful now. Requiring authentication infrastructure, a UI, remote execution or a replacement database during emulator phases would change the project's priority and expand release scope prematurely.

## Decision

1. Keep phases 0.1–0.9 and all their existing behavioral requirements. Add phase-specific architecture review guidance in [document 18](../plan/18-self-hosted-platform.md); it does not require deployment implementation.
2. Add B048 as a bounded architecture review of the completed local G1–G4 paths before B023. It records service/runtime/storage/identity boundaries and concrete future adaptation points. It does not require speculative provider frameworks or migrate the local workspace.
3. Add a separate **SH.0–SH.6** post-emulator track without assigning package versions. B049/SH.0 depends on B042 and records actual emulator stability, remaining defects and the deployment scope. Every deployment implementation epic B050–B055 depends transitively on B049. B042 does not depend on these stages.
4. Stage authenticated single-server use by trusted code authors, a browser workspace and optional services, isolated shared execution, remote workers, operational qualification and a distribution candidate. Identity, authorization, execution isolation, transport, storage and availability have separate evidence requirements. Remote job placement does not imply distributed query execution.
5. Keep Sparkless as default semantic execution, one catalog/table-commit authority, native Delta, fresh job attempts and explicit unsupported behavior in every deployment. Local wheel installation remains independent of containers, orchestration, cloud accounts and deployed identity providers. No JVM backend or remote Spark fallback is permitted.
6. Keep every pre-existing task state/evidence and capability claim unchanged. Append dependencies only to the unfinished B023 release package and its children for B048. Add C059–C064 as planned deployment requirements. Extend planning validation to cover the separate track and the stability barrier.

## Consequences and migration

Self-hosting has an actionable place in the plan without becoming a prerequisite for emulator completion. Later stages require decomposition, real positive/denial/recovery evidence and an explicit supported threat model. SQLite and single-owner local metadata remain the initial contract; remote storage, metadata replacement, HA and single-query distribution require independent tested decisions.

This decision changes planning documents and validation tooling only. It makes no runtime, config, workspace-format or installed-package change, initiates no cloud workload, chooses no final project license and authorizes no publication. Evidence is recorded in [the B047 planning report](../../artifacts/conformance/B047-self-hosted-roadmap.md).
