from __future__ import annotations

import re
from pathlib import Path

from brickhouse.api.host_metadata import HostMetadataHttpApi
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


def test_host_metadata_returns_stable_simulated_workspace_identity(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(descriptor.path) as context:
        api = HostMetadataHttpApi(context)
        first = api.dispatch("GET", "/.well-known/databricks-config")
        second = api.dispatch("GET", "/.well-known/databricks-config")

    assert first is not None and first.status == 200
    assert second is not None and second.payload == first.payload
    assert isinstance(first.payload, dict)
    assert first.payload["host_type"] == "workspace"
    workspace_id = first.payload["workspace_id"]
    assert isinstance(workspace_id, str)
    assert re.fullmatch(r"[1-9][0-9]{0,18}", workspace_id)


def test_host_metadata_rejects_non_get_methods_and_unknown_paths(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(descriptor.path) as context:
        api = HostMetadataHttpApi(context)
        assert api.dispatch("POST", "/.well-known/databricks-config") is not None
        assert api.dispatch("GET", "/.well-known/other") is None


def test_bundle_current_user_is_a_stable_synthetic_identity(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(descriptor.path) as context:
        api = HostMetadataHttpApi(context)
        first = api.dispatch("GET", "/api/2.0/preview/scim/v2/Me")
        second = api.dispatch("GET", "/api/2.0/preview/scim/v2/Me")

    assert first is not None and first.status == 200
    assert second is not None and second.payload == first.payload
    assert isinstance(first.payload, dict)
    assert first.payload["id"] == "brickhouse-local-user"
    assert first.payload["userName"] == "brickhouse-local@invalid"
    assert first.payload["active"] is True


def test_bundle_current_user_rejects_non_get_methods(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(descriptor.path) as context:
        api = HostMetadataHttpApi(context)
        response = api.dispatch("POST", "/api/2.0/preview/scim/v2/Me")

    assert response is not None and response.status == 405
