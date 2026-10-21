"""Local BrickHouse workspace configuration and lifecycle primitives."""

from brickhouse.workspace.config import (
    SUPPORTED_CONFIG_VERSION,
    SUPPORTED_FORMAT_VERSION,
    InvalidWorkspaceConfigError,
    UnsupportedConfigVersionError,
    UnsupportedWorkspaceFormatError,
    WorkspaceConfig,
    WorkspaceConfigError,
)
from brickhouse.workspace.context import WorkspaceContext, WorkspaceContextError
from brickhouse.workspace.lifecycle import (
    SUPPORTED_METADATA_SCHEMA_VERSION,
    Workspace,
    WorkspaceCorruptError,
    WorkspaceError,
    WorkspaceExistsError,
    WorkspaceNotFoundError,
    initialize_workspace,
    open_workspace,
)
from brickhouse.workspace.migrations import Migration, WorkspaceMigrationError
from brickhouse.workspace.ownership import (
    WorkspaceInUseError,
    WorkspaceOwner,
    WorkspaceOwnershipError,
    acquire_workspace_owner,
)

__all__ = [
    "SUPPORTED_CONFIG_VERSION",
    "SUPPORTED_FORMAT_VERSION",
    "SUPPORTED_METADATA_SCHEMA_VERSION",
    "InvalidWorkspaceConfigError",
    "UnsupportedConfigVersionError",
    "UnsupportedWorkspaceFormatError",
    "Workspace",
    "WorkspaceConfigError",
    "WorkspaceConfig",
    "WorkspaceContext",
    "WorkspaceContextError",
    "WorkspaceCorruptError",
    "WorkspaceError",
    "WorkspaceExistsError",
    "WorkspaceNotFoundError",
    "Migration",
    "WorkspaceMigrationError",
    "WorkspaceInUseError",
    "WorkspaceOwner",
    "WorkspaceOwnershipError",
    "acquire_workspace_owner",
    "initialize_workspace",
    "open_workspace",
]
