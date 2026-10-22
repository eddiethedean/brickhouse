from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from threading import Thread

from brickhouse.diagnostic_server import create_diagnostic_server
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


def test_phase_0_1_orders_example_runs_through_local_sdk_worker_sql(tmp_path: Path) -> None:
    workspace = initialize_workspace(tmp_path / "workspace")
    context = WorkspaceContext.local(workspace.path)
    context.__enter__()
    server = create_diagnostic_server(context, "127.0.0.1", 0)
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    host = f"http://127.0.0.1:{server.server_port}"

    environment = dict(os.environ)
    environment.pop("DATABRICKS_TOKEN", None)
    environment.pop("DATABRICKS_HOST", None)
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[2] / "src")
    try:
        completed = subprocess.run(
            [
                sys.executable,
                "examples/phase_0_1/run_orders.py",
                "--host",
                host,
            ],
            check=False,
            capture_output=True,
            cwd=Path(__file__).resolve().parents[2],
            env=environment,
            text=True,
            timeout=60,
        )
        assert completed.returncode == 0, completed.stderr
        result = json.loads(completed.stdout)
        assert result["result"] == [["alice", "2", "15.75"]]
        assert context.tables.get("main", "default", result["table"].rsplit(".", 1)[-1])
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
        context.close()
