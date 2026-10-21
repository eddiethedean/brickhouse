from __future__ import annotations

from pathlib import Path

import pytest

from brickhouse import Workspace
from brickhouse.workspace.context import WorkspaceContextError
from brickhouse.workspace.lifecycle import initialize_workspace
from brickhouse.workspace.ownership import WorkspaceInUseError, acquire_workspace_owner


def test_embedded_context_acquires_and_releases_the_workspace(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    context = Workspace.local(descriptor.path)

    with pytest.raises(WorkspaceContextError, match="not active"):
        _ = context.workspace

    with context as active:
        assert active is context
        assert active.is_active
        assert active.workspace.workspace_id == descriptor.workspace_id
        with pytest.raises(WorkspaceInUseError):
            acquire_workspace_owner(descriptor.path)

    assert not context.is_active
    with pytest.raises(WorkspaceContextError, match="not active"):
        _ = context.workspace

    owner = acquire_workspace_owner(descriptor.path)
    owner.release()


def test_embedded_context_is_one_shot_and_close_is_idempotent(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    context = Workspace.local(descriptor.path)
    context.close()
    context.close()

    with pytest.raises(WorkspaceContextError, match="already been closed"):
        context.__enter__()
