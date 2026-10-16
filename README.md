# BrickHouse

**Plan for a Python package that aims to emulate Databricks locally without a JVM.**

This repository starter contains design and implementation plans, not working software. All CLI commands, Python interfaces, package extras, and endpoint behavior described as BrickHouse features are proposed until backed by implementation and conformance evidence. Research baseline: **2026-10-16**.

BrickHouse will combine Sparkless execution, selected Minilake-derived API behavior, native Delta Lake operations, and optional ecosystem integrations. The Sparkless maintainer owns this project and can coordinate Sparkless releases. Other dependencies are independently maintained; implementation must not depend on unaccepted upstream changes.

## Start here

1. Read [AGENTS.md](AGENTS.md) for implementation rules.
2. Read [the product scope](docs/plan/00-vision-and-scope.md), [architecture](docs/plan/01-architecture.md), and [package design](docs/plan/02-packages-and-public-api.md).
3. Follow [the Codex bootstrap instructions](docs/plan/CODEX_BOOTSTRAP.md).
4. Execute [the milestone roadmap](docs/plan/08-roadmap.md) using [the task backlog](docs/plan/09-task-backlog.md).
5. Maintain [STATUS.md](STATUS.md), [task state](planning/tasks.json), and [capability evidence](planning/capabilities.json).

## Planning documents

| Document | Purpose |
|---|---|
| [00 — Vision and scope](docs/plan/00-vision-and-scope.md) | Meaning of full emulation, initial users, compatibility profiles |
| [01 — Architecture](docs/plan/01-architecture.md) | Services, execution boundaries, authoritative state |
| [02 — Packages and public API](docs/plan/02-packages-and-public-api.md) | Distribution strategy, internal modules, proposed user interfaces |
| [03 — Dependencies and Minilake](docs/plan/03-dependencies-and-minilake.md) | Adoption strategy, source provenance, independent upstreams |
| [04 — Sparkless changes](docs/plan/04-sparkless-upstream.md) | Engine extension contracts and coordinated delivery |
| [05 — Catalog, storage, SQL](docs/plan/05-catalog-storage-sql.md) | Tables, Delta commits, SQL planning and consistency |
| [06 — Jobs, notebooks, dbutils](docs/plan/06-jobs-notebooks-dbutils.md) | Runtime, lifecycle, imports, utilities, execution isolation |
| [07 — Compatibility and testing](docs/plan/07-compatibility-and-testing.md) | Behavioral oracles, coverage reporting, JVM-free gates |
| [08 — Roadmap](docs/plan/08-roadmap.md) | Milestones, dependencies, release acceptance criteria |
| [09 — Task backlog](docs/plan/09-task-backlog.md) | Implementable work units and acceptance criteria |
| [10 — Streaming and pipelines](docs/plan/10-streaming-and-pipelines.md) | Micro-batches, recovery, Auto Loader, Lakeflow |
| [11 — ML and optional extensions](docs/plan/11-ml-and-extensions.md) | MLflow, dbt, vector search, storage emulators, governance |
| [12 — Delivery and operations](docs/plan/12-delivery-and-operations.md) | Configuration, packaging, persistence, CI, releases |
| [13 — Decisions and risks](docs/plan/13-decisions-and-risks.md) | Adopted defaults, open decisions, evidence gates |
| [14 — Research sources](docs/plan/14-research-sources.md) | Primary sources, observed facts, revalidation guidance |
| [15 — Personal Databricks verification](docs/plan/15-personal-databricks-verification.md) | Occasional real-account probes that guide local tests and sanitized fixtures |

The pack includes **42 work packages** (23 initial tasks and 19 later epics) and **52 initial capability requirements**. Every implementation entry starts as planned. See the [manifest guide](planning/README.md) for state/evidence rules and the [planning validation report](planning/validation.json) for document checks.

## Intended first end-to-end workflow

Development will use the owner's personal Databricks account for occasional reference verification and test design. Routine tests run locally using reviewed fixtures; an optional manual capture workflow may use the supplied credentials. See [the verification plan](docs/plan/15-personal-databricks-verification.md). Access and behavior have not yet been verified.

Create a catalog and schema with the real Databricks SDK pointed at BrickHouse; submit a Python job; transform data using Sparkless; write a managed Delta table; query it through the Statement Execution API; read it by name from another session; restart the server and read the same committed data. The entire local execution path runs without Java, Spark, or a remote compute service.

## Moving this pack into a new repository

Copy the **contents** of this directory to the new `brickhouse` repository root. All internal links are relative. The planning pack includes no implementation or credentials; owner-supplied reference credentials are configured separately under document 15. No remote repository URL, GitHub organization, or filesystem location is assumed. No GitHub repository or PyPI name has been created or reserved by this planning pack.
