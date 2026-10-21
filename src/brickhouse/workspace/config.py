"""Strict, versioned TOML configuration for a local BrickHouse workspace."""

from __future__ import annotations

import json
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SUPPORTED_CONFIG_VERSION = 1
SUPPORTED_FORMAT_VERSION = 1
_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*\Z")
_ROOT_KEYS = {"workspace"}
_WORKSPACE_KEYS = {
    "config_version",
    "format_version",
    "default_catalog",
    "default_schema",
}


class WorkspaceConfigError(ValueError):
    """Base error for invalid or unsupported workspace configuration."""


class InvalidWorkspaceConfigError(WorkspaceConfigError):
    """The workspace config is malformed or violates the version 1 schema."""


class UnsupportedConfigVersionError(WorkspaceConfigError):
    """The config schema version is not supported by this BrickHouse build."""


class UnsupportedWorkspaceFormatError(WorkspaceConfigError):
    """The persisted workspace format is not supported by this BrickHouse build."""


@dataclass(frozen=True, slots=True)
class WorkspaceConfig:
    """Supported configuration values stored in ``brickhouse.toml``."""

    config_version: int = SUPPORTED_CONFIG_VERSION
    format_version: int = SUPPORTED_FORMAT_VERSION
    default_catalog: str = "main"
    default_schema: str = "default"

    def __post_init__(self) -> None:
        if type(self.config_version) is not int:  # bool is not a valid TOML version.
            raise InvalidWorkspaceConfigError("config_version must be an integer")
        if self.config_version != SUPPORTED_CONFIG_VERSION:
            raise UnsupportedConfigVersionError(
                f"config_version {self.config_version} is not supported; "
                f"supported version is {SUPPORTED_CONFIG_VERSION}"
            )
        if type(self.format_version) is not int:
            raise InvalidWorkspaceConfigError("format_version must be an integer")
        if self.format_version != SUPPORTED_FORMAT_VERSION:
            raise UnsupportedWorkspaceFormatError(
                f"workspace format {self.format_version} is not supported; "
                f"supported format is {SUPPORTED_FORMAT_VERSION}"
            )
        self._validate_identifier("default_catalog", self.default_catalog)
        self._validate_identifier("default_schema", self.default_schema)

    @staticmethod
    def _validate_identifier(field: str, value: str) -> None:
        if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
            raise InvalidWorkspaceConfigError(
                f"{field} must be a lowercase unquoted identifier "
                "starting with a letter or underscore"
            )

    @classmethod
    def from_file(cls, path: Path) -> WorkspaceConfig:
        try:
            with path.open("rb") as source:
                parsed = tomllib.load(source)
        except FileNotFoundError as error:
            raise InvalidWorkspaceConfigError("brickhouse.toml is missing") from error
        except tomllib.TOMLDecodeError as error:
            raise InvalidWorkspaceConfigError("brickhouse.toml is not valid TOML") from error

        if not isinstance(parsed, dict) or set(parsed) != _ROOT_KEYS:
            raise InvalidWorkspaceConfigError("config must contain only the [workspace] table")
        section = parsed.get("workspace")
        if not isinstance(section, dict):
            raise InvalidWorkspaceConfigError("[workspace] must be a TOML table")
        unknown = set(section) - _WORKSPACE_KEYS
        missing = _WORKSPACE_KEYS - set(section)
        if unknown:
            raise InvalidWorkspaceConfigError(
                f"unknown [workspace] settings: {', '.join(sorted(unknown))}"
            )
        if missing:
            raise InvalidWorkspaceConfigError(
                f"missing [workspace] settings: {', '.join(sorted(missing))}"
            )
        try:
            return cls(
                config_version=section["config_version"],
                format_version=section["format_version"],
                default_catalog=section["default_catalog"],
                default_schema=section["default_schema"],
            )
        except (TypeError, ValueError) as error:
            if isinstance(error, WorkspaceConfigError):
                raise
            raise InvalidWorkspaceConfigError("invalid [workspace] setting type") from error

    def to_toml(self) -> str:
        """Return the canonical version 1 TOML representation."""
        values: dict[str, Any] = {
            "config_version": self.config_version,
            "format_version": self.format_version,
            "default_catalog": self.default_catalog,
            "default_schema": self.default_schema,
        }
        lines = ["[workspace]"]
        for key, value in values.items():
            serialized = json.dumps(value) if isinstance(value, str) else str(value)
            lines.append(f"{key} = {serialized}")
        return "\n".join(lines) + "\n"
