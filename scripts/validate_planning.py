"""Validate BrickHouse's task/capability manifests and their Markdown references."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[1]
LINK_RE = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
TASK_HEADING_RE = re.compile(r"^## (B\d{3}(?:\.\d+)*) — (.*?)$", re.MULTILINE)
RANGE_RE = re.compile(r"B(\d{3})(?:–B(\d{3}))?")


class ValidationError(Exception):
    """Raised when a planning invariant fails."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def read_json(relative_path: str) -> dict[str, Any]:
    path = ROOT / relative_path
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
        require(isinstance(parsed, dict), f"Expected an object in {relative_path}")
        return cast(dict[str, Any], parsed)
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(f"Cannot read {relative_path}: {exc}") from exc


def check_markdown_links(markdown_paths: list[Path]) -> int:
    checked = 0
    for source in markdown_paths:
        for target in LINK_RE.findall(source.read_text(encoding="utf-8")):
            if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", target) or target.startswith("#"):
                continue
            path_part = target.split("#", 1)[0].split("?", 1)[0]
            if not path_part:
                continue
            resolved = (source.parent / path_part).resolve()
            require(resolved.is_relative_to(ROOT), f"Link escapes repository: {source}: {target}")
            require(
                resolved.exists(), f"Broken Markdown link: {source.relative_to(ROOT)}: {target}"
            )
            checked += 1
    return checked


def check_backlog(tasks: list[dict[str, Any]]) -> None:
    path = ROOT / "docs/plan/09-task-backlog.md"
    text = path.read_text(encoding="utf-8")
    headings = {task_id: title for task_id, title in TASK_HEADING_RE.findall(text)}
    require(len(headings) == len(TASK_HEADING_RE.findall(text)), "Duplicate backlog task headings")
    require(
        set(headings) == {task["id"] for task in tasks}, "Backlog task headings differ from ledger"
    )

    for task in tasks:
        task_id = task["id"]
        start = text.index(f"## {task_id} — ")
        next_heading = re.search(r"^## B\d", text[start + 1 :], re.MULTILINE)
        end = start + 1 + next_heading.start() if next_heading else len(text)
        section = text[start:end]
        require(headings[task_id] == task["title"], f"Backlog title differs for {task_id}")
        deliverable = re.search(r"^\*\*Deliverable:\*\* (.+)$", section, re.MULTILINE)
        require(deliverable is not None, f"Backlog deliverable missing for {task_id}")
        assert deliverable is not None
        require(
            deliverable.group(1) == task["deliverable"],
            f"Backlog deliverable differs for {task_id}",
        )
        criteria = re.findall(r"^\d+\. (.+)$", section, re.MULTILINE)
        require(
            criteria == task["acceptance_criteria"],
            f"Backlog acceptance criteria differ for {task_id}",
        )
        dependency_line = re.search(r"^\*\*Dependencies:\*\* (.+)$", section, re.MULTILINE)
        expected_dependencies = ", ".join(task["depends_on"]) or "None"
        require(dependency_line is not None, f"Backlog dependencies missing for {task_id}")
        assert dependency_line is not None
        require(
            dependency_line.group(1) == expected_dependencies + ".",
            f"Backlog dependencies differ for {task_id}",
        )
        if task["children"]:
            child_line = "**Required children:** " + ", ".join(task["children"]) + "."
            require(child_line in section, f"Backlog child list differs for {task_id}")
        if "parent_id" in task:
            covered = ", ".join(map(str, task["parent_criteria"]))
            parent_line = f"**Parent:** {task['parent_id']} · **Parent criteria:** {covered}."
            require(parent_line in section, f"Backlog parent/criteria differs for {task_id}")


def task_release_assignments(roadmap: str, top_level_ids: set[str]) -> dict[str, int]:
    assignments: dict[str, int] = {}
    phase_rows = re.findall(r"^\| (0\.\d+) \|.*$", roadmap, re.MULTILINE)
    require(
        phase_rows == [f"0.{number}" for number in range(1, 10)],
        "Expected release phases 0.1 through 0.9",
    )
    for row in re.findall(r"^\| 0\.\d+ \|.*$", roadmap, re.MULTILINE):
        columns = [column.strip() for column in row.split("|")]
        phase_text = columns[1]
        phase = int(phase_text.split(".")[1])
        require(
            columns[2].strip("*") == f"0.{phase}.0", f"Invalid first version for phase {phase_text}"
        )
        for first, last in RANGE_RE.findall(columns[4]):
            for number in range(int(first), int(last or first) + 1):
                task_id = f"B{number:03}"
                require(task_id not in assignments, f"Duplicate release assignment: {task_id}")
                assignments[task_id] = phase

    deployment_rows = re.findall(r"^\| (SH\.\d+) \|.*$", roadmap, re.MULTILINE)
    require(
        deployment_rows == [f"SH.{number}" for number in range(7)],
        "Expected post-stability stages SH.0 through SH.6",
    )
    for row in re.findall(r"^\| SH\.\d+ \|.*$", roadmap, re.MULTILINE):
        columns = [column.strip() for column in row.split("|")]
        stage = int(columns[1].split(".")[1])
        require(columns[2] == "Unassigned", "Deployment versions require a separate scope decision")
        for first, last in RANGE_RE.findall(columns[4]):
            for number in range(int(first), int(last or first) + 1):
                task_id = f"B{number:03}"
                require(task_id not in assignments, f"Duplicate release assignment: {task_id}")
                assignments[task_id] = 10 + stage
    require(
        set(assignments) == top_level_ids,
        "Release roadmap task assignment differs from top-level ledger",
    )
    return assignments


def check_evidence(task: dict[str, Any]) -> None:
    for evidence in task["evidence"]:
        for key in ("date", "kind", "command", "result"):
            require(bool(evidence.get(key)), f"Missing {key} in evidence for {task['id']}")
        artifact = evidence.get("artifact")
        if artifact and not re.match(r"^https?://", artifact):
            require(
                (ROOT / artifact).exists(),
                f"Missing evidence artifact for {task['id']}: {artifact}",
            )


def validate() -> dict[str, Any]:
    manifest = read_json("planning/tasks.json")
    capability_manifest = read_json("planning/capabilities.json")
    tasks = manifest["tasks"]
    capabilities = capability_manifest["capabilities"]
    tasks_by_id = {task["id"]: task for task in tasks}
    capabilities_by_id = {capability["id"]: capability for capability in capabilities}
    require(len(tasks_by_id) == len(tasks), "Duplicate task IDs")
    require(len(capabilities_by_id) == len(capabilities), "Duplicate capability IDs")
    require(manifest["release_roadmap"] == "ROADMAP.md", "Unexpected release_roadmap pointer")

    parents = [task for task in tasks if "parent_id" not in task]
    children = [task for task in tasks if "parent_id" in task]
    for task in tasks:
        task_id = task["id"]
        require(task["status"] in manifest["status_values"], f"Invalid task status: {task_id}")
        require(
            len(set(task["depends_on"])) == len(task["depends_on"]),
            f"Duplicate dependencies: {task_id}",
        )
        require(
            all(dependency in tasks_by_id for dependency in task["depends_on"]),
            f"Unknown dependency: {task_id}",
        )
        require(
            all((ROOT / reference).is_file() for reference in task["plan_refs"]),
            f"Missing plan ref: {task_id}",
        )
        require(
            len(set(task["children"])) == len(task["children"]), f"Duplicate children: {task_id}"
        )
        for child_id in task["children"]:
            require(child_id in tasks_by_id, f"Unknown child {child_id} of {task_id}")
            require(
                tasks_by_id[child_id].get("parent_id") == task_id,
                f"Parent/child mismatch: {task_id}/{child_id}",
            )

        if task["children"]:
            coverage: set[int] = set()
            for child_id in task["children"]:
                coverage.update(tasks_by_id[child_id]["parent_criteria"])
            expected = set(range(1, len(task["acceptance_criteria"]) + 1))
            require(coverage == expected, f"Parent criterion coverage incomplete: {task_id}")
            if task["status"] == "done":
                require(
                    all(tasks_by_id[child]["status"] == "done" for child in task["children"]),
                    f"Open child of done task {task_id}",
                )

        if "parent_id" in task:
            parent_id = task["parent_id"]
            require(parent_id in tasks_by_id, f"Unknown parent: {task_id}")
            parent = tasks_by_id[parent_id]
            require(
                task_id in parent["children"], f"Missing reciprocal parent reference: {task_id}"
            )
            require(
                task["milestone"] == parent["milestone"],
                f"Milestone differs from parent: {task_id}",
            )
            require(
                set(parent["depends_on"]) <= set(task["depends_on"]),
                f"Child misses inherited prerequisite: {task_id}",
            )
            require(parent_id not in task["depends_on"], f"Child depends on parent: {task_id}")
            criteria = task["parent_criteria"]
            require(
                criteria and len(set(criteria)) == len(criteria),
                f"Missing/duplicate parent criteria: {task_id}",
            )
            require(
                all(
                    isinstance(index, int) and 1 <= index <= len(parent["acceptance_criteria"])
                    for index in criteria
                ),
                f"Invalid parent criterion: {task_id}",
            )
            if task["status"] == "done":
                require(bool(task["evidence"]), f"Done child has no evidence: {task_id}")

        if task["status"] == "done":
            require(bool(task["evidence"]), f"Done task has no evidence: {task_id}")
        check_evidence(task)

    # A parent is complete only after all children, so include those implicit edges in cycle checks.
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(task_id: str) -> None:
        require(task_id not in visiting, f"Dependency/parent cycle at {task_id}")
        if task_id in visited:
            return
        visiting.add(task_id)
        task = tasks_by_id[task_id]
        for dependency in task["depends_on"] + task["children"]:
            visit(dependency)
        visiting.remove(task_id)
        visited.add(task_id)

    for task_id in tasks_by_id:
        visit(task_id)

    roadmap = (ROOT / manifest["release_roadmap"]).read_text(encoding="utf-8")
    phase_for = task_release_assignments(roadmap, {task["id"] for task in parents})

    def inherited_phase(task_id: str) -> int:
        if task_id not in phase_for:
            phase_for[task_id] = inherited_phase(tasks_by_id[task_id]["parent_id"])
        return phase_for[task_id]

    for child in children:
        inherited_phase(child["id"])
    for task in tasks:
        require(
            all(
                phase_for[dependency] <= phase_for[task["id"]] for dependency in task["depends_on"]
            ),
            f"Prerequisite is scheduled after {task['id']}",
        )

    require("B042" in tasks_by_id and "B049" in tasks_by_id, "Missing emulator stability gate")
    require(phase_for["B049"] == 10, "Stability review must be assigned to SH.0")
    require(
        "B042" in tasks_by_id["B049"]["depends_on"],
        "Stability review must follow consolidated emulator conformance B042",
    )

    def requires_stability(task_id: str) -> bool:
        if task_id == "B049":
            return True
        return any(
            requires_stability(dependency) for dependency in tasks_by_id[task_id]["depends_on"]
        )

    for task in tasks:
        if phase_for[task["id"]] > 10:
            require(
                requires_stability(task["id"]),
                f"Deployment implementation bypasses emulator stability B049: {task['id']}",
            )
            if task["status"] in {"in_progress", "done"}:
                require(
                    tasks_by_id["B049"]["status"] == "done",
                    f"Deployment work started before emulator stability: {task['id']}",
                )

    for capability in capabilities:
        capability_id = capability["id"]
        require(
            capability["status"] in capability_manifest["status_values"],
            f"Invalid capability status: {capability_id}",
        )
        require(
            len(set(capability["task_ids"])) == len(capability["task_ids"]),
            f"Duplicate task references: {capability_id}",
        )
        require(
            set(capability["task_ids"]) <= set(tasks_by_id),
            f"Unknown task reference: {capability_id}",
        )
        if capability["status"] == "supported":
            require(
                bool(capability["tested_versions"]),
                f"Supported capability has no tested version: {capability_id}",
            )
            require(
                bool(capability["test_ids"]),
                f"Supported capability has no test IDs: {capability_id}",
            )
            require(
                bool(capability["evidence"]),
                f"Supported capability has no evidence: {capability_id}",
            )

    check_backlog(tasks)
    markdown_paths = sorted(ROOT.rglob("*.md"))
    link_count = check_markdown_links(markdown_paths)
    ignored = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    require(
        ".env" in ignored and "!.env.example" in ignored,
        ".env must be ignored while allowing an example",
    )
    require("dist/" in ignored and "build/" in ignored, "Build outputs must be ignored")

    version = re.search(
        r'^__version__ = "([^"]+)"$',
        (ROOT / "src/brickhouse/_version.py").read_text(),
        re.MULTILINE,
    )
    project = read_pyproject()
    require(version is not None, "Package version constant missing")
    assert version is not None
    require(
        project["project"]["version"] == version.group(1),
        "Project metadata and package version differ",
    )

    ready = [
        task["id"]
        for task in tasks
        if task["status"] in {"planned", "in_progress"}
        and task["kind"] == "task"
        and not task["children"]
        and all(tasks_by_id[dependency]["status"] == "done" for dependency in task["depends_on"])
    ]
    return {
        "markdown_files": len(markdown_paths),
        "task_count": len(tasks),
        "work_package_count": len(parents),
        "phase_0_1_child_task_count": sum(phase_for[child["id"]] == 1 for child in children),
        "child_task_count": len(children),
        "capability_count": len(capabilities),
        "release_phase_count": 9,
        "post_stability_stage_count": 7,
        "emulator_stability_task": "B049",
        "local_markdown_links_checked": link_count,
        "initially_ready_implementation_tasks": ready,
    }


def read_pyproject() -> dict[str, Any]:
    import tomllib

    try:
        return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValidationError(f"Cannot read pyproject.toml: {exc}") from exc


def main() -> int:
    try:
        report = validate()
    except (KeyError, TypeError, ValidationError) as exc:
        print(f"Planning validation failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"result": "passed", **report}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
