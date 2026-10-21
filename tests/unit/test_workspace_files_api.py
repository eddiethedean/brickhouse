from __future__ import annotations

import base64
import json
from pathlib import Path
from urllib.parse import urlencode

from brickhouse.api.catalog import ApiBinaryResponse
from brickhouse.api.workspace_files import WorkspaceFileHttpApi
from brickhouse.workspace.context import WorkspaceContext
from brickhouse.workspace.lifecycle import initialize_workspace


def test_workspace_file_raw_python_import_status_update_and_delete(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(descriptor.path) as context:
        api = WorkspaceFileHttpApi(context)
        public_path = "/Shared/terraform-managed.py"
        internal_file = descriptor.path / "files" / "Shared" / "terraform-managed.py"

        created = api.dispatch_raw(
            "POST",
            "/api/2.0/workspace/import",
            json.dumps(
                {
                    "path": public_path,
                    "content": base64.b64encode(b"print('created')\n").decode("ascii"),
                    "format": "RAW",
                    "overwrite": True,
                }
            ).encode("utf-8"),
            "application/json",
        )
        assert created is not None and created.status == 200
        assert internal_file.read_bytes() == b"print('created')\n"

        status = api.dispatch_raw(
            "GET",
            f"/api/2.0/workspace/get-status?{urlencode({'path': public_path})}",
            b"",
            None,
        )
        assert status is not None and status.status == 200
        assert status.payload == {
            "path": public_path,
            "object_type": "FILE",
            "language": "PYTHON",
            "size": len(b"print('created')\n"),
        }

        updated = api.dispatch_raw(
            "POST",
            "/api/2.0/workspace/import",
            json.dumps(
                {
                    "path": public_path,
                    "content": base64.b64encode(b"print('updated')\n").decode("ascii"),
                    "format": "RAW",
                    "overwrite": True,
                }
            ).encode("utf-8"),
            "application/json",
        )
        assert updated is not None and updated.status == 200
        assert internal_file.read_bytes() == b"print('updated')\n"

        deleted = api.dispatch_raw(
            "POST",
            "/api/2.0/workspace/delete",
            json.dumps({"path": public_path, "recursive": False}).encode("utf-8"),
            "application/json",
        )
        assert deleted is not None and deleted.status == 200
        assert not internal_file.exists()

        absent = api.dispatch_raw(
            "GET",
            f"/api/2.0/workspace/get-status?{urlencode({'path': public_path})}",
            b"",
            None,
        )
        assert absent is not None and absent.status == 404
        assert absent.payload["error_code"] == "RESOURCE_DOES_NOT_EXIST"


def test_workspace_file_raw_import_rejects_unselected_suffix(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(descriptor.path) as context:
        api = WorkspaceFileHttpApi(context)
        unsupported = api.dispatch_raw(
            "POST",
            "/api/2.0/workspace/import",
            json.dumps(
                {
                    "path": "/Shared/not-python.txt",
                    "content": base64.b64encode(b"not a source file").decode("ascii"),
                    "format": "RAW",
                    "overwrite": True,
                }
            ).encode("utf-8"),
            "application/json",
        )
        assert unsupported is not None and unsupported.status == 400


def test_workspace_bundle_json_status_export_and_recursive_cleanup(tmp_path: Path) -> None:
    descriptor = initialize_workspace(tmp_path / "workspace")
    with WorkspaceContext.local(descriptor.path) as context:
        api = WorkspaceFileHttpApi(context)
        bundle_root = "/Workspace/Users/local@invalid/.bundle/example/dev"
        directory = api.dispatch_raw(
            "POST",
            "/api/2.0/workspace/mkdirs",
            json.dumps({"path": f"{bundle_root}/state"}).encode("utf-8"),
            "application/json",
        )
        assert directory is not None and directory.status == 200

        payload = json.dumps({"state_version": 3, "resources": {}}).encode("utf-8")
        imported = api.dispatch_raw(
            "POST",
            "/api/2.0/workspace/import",
            json.dumps(
                {
                    "path": f"{bundle_root}/state/resources.json",
                    "content": base64.b64encode(payload).decode("ascii"),
                    "format": "RAW",
                    "overwrite": True,
                }
            ).encode("utf-8"),
            "application/json",
        )
        assert imported is not None and imported.status == 200

        status = api.dispatch_raw(
            "GET",
            f"/api/2.0/workspace/get-status?{urlencode({'path': f'{bundle_root}/state'})}",
            b"",
            None,
        )
        assert status is not None and status.status == 200
        assert status.payload == {"path": f"{bundle_root}/state", "object_type": "DIRECTORY"}

        export = api.dispatch_raw(
            "GET",
            "/api/2.0/workspace/export?"
            + urlencode({"path": f"{bundle_root}/state/resources.json", "direct_download": True}),
            b"",
            None,
        )
        assert isinstance(export, ApiBinaryResponse)
        assert export.body == payload

        deleted = api.dispatch_raw(
            "POST",
            "/api/2.0/workspace/delete",
            json.dumps({"path": bundle_root, "recursive": True}).encode("utf-8"),
            "application/json",
        )
        assert deleted is not None and deleted.status == 200
        assert not (
            descriptor.path / "files" / "Users" / "local@invalid" / ".bundle" / "example" / "dev"
        ).exists()
