# Project status

Updated: 2026-10-16

- Stage: planning pack prepared; implementation has not started.
- Active milestone: M0 — foundation and feasibility.
- Next task: B001 — initialize package and development tooling.
- Implemented BrickHouse capabilities: none.
- Verified integrations: none. Research has examined source and documentation only.
- Starting engine reference: Sparkless 4.13.3, source commit `b7306725` in the maintainer's reviewed checkout.
- Starting Minilake reference: source commit `0597285d1ba33daf76f553b82a32024ded7b78f6`, declaring version 1.8.0.
- Dependencies: resolve and record exact compatible versions during M0; research versions are not a tested lockfile.
- Reference verification: owner authorized occasional personal Databricks probes for representative code and test design; routine tests/required CI remain local and consume sanitized fixtures. See [the capture plan](docs/plan/15-personal-databricks-verification.md) and [ADR 0001](docs/decisions/0001-personal-databricks-reference-verification.md).
- Credential configuration: owner reports DATABRICKS_TOKEN in ignored local .env and GitHub Actions secrets. Token/access have not been checked; DATABRICKS_HOST and permitted compute/scratch policy still need configuration before capture. No remote runs or fixture tooling exist yet.

## Decisions to validate first

1. Selected Minilake source adaptation scope and provenance (B002).
2. Sparkless session/catalog/Arrow/writer extension feasibility (B003).
3. One native Delta writer path, protocol envelope, and commit recovery contract (B004).
4. Initial Databricks SDK/CLI/SQL connector version fixtures and personal-account reference capture/isolation contracts (B005; decompose before implementation).

## Handoff log

Append an entry after each implementation slice: date, task IDs, changes, test evidence, known limitations, and next ready tasks. Never put credentials or real secret values here.

### 2026-10-16 — Reference verification planning update

Expanded B005's planned acceptance criteria and synchronized the backlog for bounded personal-account reference probes, sanitized fixture provenance, local regression design, and an optional manual GitHub capture workflow. Recorded D011/ADR 0001, updated testing/CI/bootstrap guidance, and ignored .env/raw captures. All implementation tasks and capabilities remain planned; B001 is still next. Documentation/manifest consistency checks are recorded in planning/validation.json; no implementation tests or Databricks calls were run.
