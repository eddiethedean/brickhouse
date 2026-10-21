"""BrickHouse package metadata and embedded local workspace lifecycle."""

from brickhouse._version import __version__
from brickhouse.workspace.context import WorkspaceContext as Workspace

__all__ = ["Workspace", "__version__"]
