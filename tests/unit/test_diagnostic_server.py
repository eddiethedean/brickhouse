from __future__ import annotations

from pathlib import Path

import pytest

from brickhouse.diagnostic_server import create_diagnostic_server, serve_diagnostics
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


@pytest.mark.parametrize(
    ("host", "message"),
    [
        ("0.0.0.0", "must bind to IPv4 loopback"),
        ("192.0.2.1", "must bind to IPv4 loopback"),
        ("::1", "must bind to IPv4 loopback"),
        ("localhost", "accepts IPv4 loopback hosts only"),
    ],
)
def test_server_factory_rejects_non_ipv4_loopback_before_binding(
    tmp_path: Path,
    host: str,
    message: str,
) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        with pytest.raises(ValueError, match=message):
            create_diagnostic_server(context, host, 0)


def test_direct_serve_entry_rejects_non_loopback_before_signal_setup(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        with pytest.raises(ValueError, match="must bind to IPv4 loopback"):
            serve_diagnostics(context, "0.0.0.0", 0)


def test_server_factory_still_binds_an_ipv4_loopback_address(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(workspace.path) as context:
        with create_diagnostic_server(context, "127.0.0.1", 0) as server:
            assert server.server_address[0] == "127.0.0.1"
            assert server.server_address[1] > 0
