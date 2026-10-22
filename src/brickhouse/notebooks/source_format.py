"""Structured parser for the local Python Databricks notebook source subset."""

from __future__ import annotations

import json
import shlex
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

MAX_SOURCE_BYTES = 16 * 1024 * 1024
_HEADER = "# Databricks notebook source"
_CELL_SEPARATOR = "# COMMAND ----------"
_MAGIC_PREFIX = "# MAGIC"

SourceCellKind = Literal["python", "sql", "markdown", "run"]


class NotebookSourceFormatError(ValueError):
    """A source notebook is outside BrickHouse's declared Python subset."""


@dataclass(frozen=True, slots=True)
class NotebookSourceCell:
    """One parsed source-export cell with its explicit handler kind."""

    kind: SourceCellKind
    source: str = ""
    target: str | None = None


@dataclass(frozen=True, slots=True)
class DatabricksPythonExport:
    """Validated local Python source export and its cells."""

    path: Path
    cells: tuple[NotebookSourceCell, ...]
    environment_version: str | None = None
    dependencies: tuple[str, ...] = ()


def parse_databricks_python_source(path: str | Path) -> DatabricksPythonExport:
    """Parse a marked `.py` source export without executing notebook code."""
    if not isinstance(path, (str, Path)):
        raise NotebookSourceFormatError("Databricks source notebook path must be a filesystem path")
    candidate = Path(path).expanduser()
    try:
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise NotebookSourceFormatError("Databricks source notebook does not exist") from error
    if not resolved.is_file():
        raise NotebookSourceFormatError("Databricks source notebook path must be a file")
    if resolved.suffix.casefold() != ".py":
        raise NotebookSourceFormatError(
            "only Python Databricks source exports with a .py extension are supported"
        )
    if resolved.stat().st_size > MAX_SOURCE_BYTES:
        raise NotebookSourceFormatError(
            f"Databricks source notebook exceeds the {MAX_SOURCE_BYTES}-byte limit"
        )
    try:
        text = resolved.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeError) as error:
        raise NotebookSourceFormatError(
            "Databricks source notebook must be readable UTF-8"
        ) from error

    lines = text.splitlines()
    if not lines or lines[0].strip() != _HEADER:
        raise NotebookSourceFormatError(
            "Python source must start with '# Databricks notebook source'"
        )
    code_lines, environment_version, dependencies = _extract_environment_metadata(lines[1:])
    raw_cells = _split_source_cells(code_lines)
    cells = tuple(
        parsed for raw_cell in raw_cells if (parsed := _parse_source_cell(raw_cell)) is not None
    )
    return DatabricksPythonExport(
        path=resolved,
        cells=cells,
        environment_version=environment_version,
        dependencies=dependencies,
    )


def notebook_environment_spec(path: str | Path) -> dict[str, object] | None:
    """Read declared Databricks environment metadata from a local notebook asset."""
    candidate = Path(path).expanduser()
    try:
        resolved = candidate.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise NotebookSourceFormatError("notebook environment source does not exist") from error
    if not resolved.is_file():
        raise NotebookSourceFormatError("notebook environment source must be a file")
    if resolved.suffix.casefold() == ".py":
        export = parse_databricks_python_source(resolved)
        if export.environment_version is None and not export.dependencies:
            return None
        result: dict[str, object] = {"dependencies": list(export.dependencies)}
        if export.environment_version is not None:
            result["environment_version"] = export.environment_version
        return result
    if resolved.suffix.casefold() != ".ipynb":
        raise NotebookSourceFormatError("notebook environment source must be .ipynb or .py")
    try:
        document = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise NotebookSourceFormatError(
            "notebook environment source must be valid UTF-8 JSON"
        ) from error
    metadata = document.get("metadata") if isinstance(document, dict) else None
    databricks = (
        metadata.get("application/vnd.databricks.v1+notebook")
        if isinstance(metadata, dict)
        else None
    )
    environment = databricks.get("environmentMetadata") if isinstance(databricks, dict) else None
    if environment is None:
        return None
    if not isinstance(environment, dict):
        raise NotebookSourceFormatError("notebook environment metadata must be an object")
    return dict(environment)


def _extract_environment_metadata(
    lines: list[str],
) -> tuple[list[str], str | None, tuple[str, ...]]:
    """Read the bounded PEP 723 Databricks environment block at the file header."""
    first_content = next((index for index, line in enumerate(lines) if line.strip()), None)
    if first_content is None or lines[first_content].strip() != "# /// script":
        if any(line.strip() == "# /// script" for line in lines):
            raise NotebookSourceFormatError("PEP 723 environment metadata must follow the header")
        return lines, None, ()

    closing = next(
        (
            index
            for index in range(first_content + 1, len(lines))
            if lines[index].strip() == "# ///"
        ),
        None,
    )
    if closing is None:
        raise NotebookSourceFormatError("PEP 723 environment metadata block is not closed")
    metadata_lines: list[str] = []
    for line in lines[first_content + 1 : closing]:
        stripped = line.strip()
        if not stripped:
            metadata_lines.append("")
        elif stripped == "#":
            metadata_lines.append("")
        elif stripped.startswith("# "):
            metadata_lines.append(stripped[2:])
        else:
            raise NotebookSourceFormatError("PEP 723 environment metadata lines must be comments")
    try:
        document = tomllib.loads("\n".join(metadata_lines))
    except tomllib.TOMLDecodeError as error:
        raise NotebookSourceFormatError("PEP 723 environment metadata is invalid TOML") from error
    tool = document.get("tool")
    databricks = tool.get("databricks") if isinstance(tool, dict) else None
    environment = databricks.get("environment") if isinstance(databricks, dict) else None
    if (
        set(document) != {"tool"}
        or not isinstance(tool, dict)
        or set(tool) != {"databricks"}
        or not isinstance(databricks, dict)
        or set(databricks) != {"environment"}
        or not isinstance(environment, dict)
        or set(environment) - {"environment_version", "dependencies"}
    ):
        raise NotebookSourceFormatError(
            "PEP 723 block must contain only Databricks environment data"
        )
    version = environment.get("environment_version")
    dependencies = environment.get("dependencies", [])
    if version is not None and (not isinstance(version, str) or not version):
        raise NotebookSourceFormatError("Databricks environment_version must be a non-empty string")
    if not isinstance(dependencies, list) or any(
        not isinstance(dependency, str) for dependency in dependencies
    ):
        raise NotebookSourceFormatError("Databricks environment dependencies must be strings")
    if len(set(dependencies)) != len(dependencies):
        raise NotebookSourceFormatError("Databricks environment dependencies cannot repeat")
    remaining = lines[:first_content] + lines[closing + 1 :]
    return remaining, version, tuple(dependencies)


def resolve_run_target(
    target: str,
    *,
    relative_to: str | Path,
    allowed_root: str | Path,
) -> Path:
    """Resolve one relative `.py` source export without escaping its source tree."""
    if not isinstance(target, str) or not target or "\x00" in target:
        raise NotebookSourceFormatError("%run requires one non-empty relative notebook path")
    requested = Path(target).expanduser()
    if requested.is_absolute():
        raise NotebookSourceFormatError("%run supports relative local source-notebook paths only")
    if requested.suffix and requested.suffix.casefold() != ".py":
        raise NotebookSourceFormatError("%run supports Python .py source exports only")
    if not requested.suffix:
        requested = requested.with_suffix(".py")

    try:
        base = Path(relative_to).resolve(strict=True)
        root = Path(allowed_root).resolve(strict=True)
        resolved = (base / requested).resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise NotebookSourceFormatError("%run source notebook does not exist") from error
    if not base.is_dir() or not root.is_dir():
        raise NotebookSourceFormatError("%run source roots must be existing directories")
    if resolved != root and root not in resolved.parents:
        raise NotebookSourceFormatError("%run path escapes the allowed source-export directory")
    if not resolved.is_file() or resolved.suffix.casefold() != ".py":
        raise NotebookSourceFormatError("%run target must be a Python .py source export")
    return resolved


def validate_source_export_tree(source_export: DatabricksPythonExport) -> None:
    """Validate every nested `%run` target before starting the notebook kernel."""
    allowed_root = source_export.path.parent.resolve(strict=True)
    visited: set[Path] = set()
    active: set[Path] = set()

    def visit(export: DatabricksPythonExport) -> None:
        if export.path in active:
            raise NotebookSourceFormatError("%run source notebooks cannot contain a cycle")
        if export.path in visited:
            return
        active.add(export.path)
        for cell in export.cells:
            if cell.kind == "run":
                if cell.target is None:
                    raise NotebookSourceFormatError("%run source cell is missing its target path")
                target = resolve_run_target(
                    cell.target,
                    relative_to=export.path.parent,
                    allowed_root=allowed_root,
                )
                visit(parse_databricks_python_source(target))
        active.remove(export.path)
        visited.add(export.path)

    visit(source_export)


def _split_source_cells(lines: list[str]) -> list[list[str]]:
    cells: list[list[str]] = []
    current: list[str] = []
    for line in lines:
        if line.strip() == _CELL_SEPARATOR:
            if current:
                cells.append(current)
            current = []
        else:
            current.append(line)
    if current:
        cells.append(current)
    return cells


def _parse_source_cell(lines: list[str]) -> NotebookSourceCell | None:
    if not any(line.strip() for line in lines):
        return None

    first_index = next(index for index, line in enumerate(lines) if line.strip())
    first_line = lines[first_index].strip()
    if first_line.startswith(_MAGIC_PREFIX):
        directive = first_line[len(_MAGIC_PREFIX) :].lstrip()
        if directive.startswith("%"):
            return _parse_magic_cell(directive, lines[first_index + 1 :], comment_prefix=True)
        raise NotebookSourceFormatError("source cell contains an unknown # MAGIC directive")
    if first_line.startswith("%"):
        return _parse_magic_cell(first_line, lines[first_index + 1 :], comment_prefix=False)

    source = "\n".join(lines).strip("\n")
    if any(line.lstrip().startswith("%") for line in source.splitlines()):
        raise NotebookSourceFormatError(
            "magic commands must be the first line of a source-export cell"
        )
    return NotebookSourceCell(kind="python", source=source)


def _parse_magic_cell(
    directive: str,
    body_lines: list[str],
    *,
    comment_prefix: bool,
) -> NotebookSourceCell:
    magic, _, argument = directive.partition(" ")
    argument = argument.strip()
    if magic in {"%r", "%scala"}:
        raise NotebookSourceFormatError(f"unsupported notebook language magic: {magic}")
    if magic in {"%pip", "%uv"}:
        raise NotebookSourceFormatError("notebook package installation is not supported")
    if magic in {"%sh", "%fs"}:
        raise NotebookSourceFormatError(f"unsupported notebook magic: {magic}")

    body = _strip_magic_prefixes(body_lines) if comment_prefix else body_lines
    source = "\n".join(body).strip("\n")
    if magic == "%python":
        if argument:
            raise NotebookSourceFormatError("%python does not accept arguments")
        return NotebookSourceCell(kind="python", source=source)
    if magic == "%sql":
        if argument:
            raise NotebookSourceFormatError("%sql options are outside the supported subset")
        return NotebookSourceCell(kind="sql", source=source)
    if magic == "%md":
        if argument:
            raise NotebookSourceFormatError("%md options are outside the supported subset")
        return NotebookSourceCell(kind="markdown", source=source)
    if magic == "%run":
        if source.strip():
            raise NotebookSourceFormatError("%run must be the only content in its cell")
        try:
            arguments = shlex.split(argument, posix=True)
        except ValueError as error:
            raise NotebookSourceFormatError("%run path quoting is invalid") from error
        if len(arguments) != 1:
            raise NotebookSourceFormatError("%run requires exactly one relative notebook path")
        return NotebookSourceCell(kind="run", target=arguments[0])
    raise NotebookSourceFormatError(f"unsupported notebook magic: {magic}")


def _strip_magic_prefixes(lines: list[str]) -> list[str]:
    result: list[str] = []
    for line in lines:
        if line.startswith(_MAGIC_PREFIX):
            content = line[len(_MAGIC_PREFIX) :]
            if content.startswith(" "):
                content = content[1:]
            result.append(content)
        elif not line.strip():
            result.append("")
        else:
            result.append(line)
    return result
