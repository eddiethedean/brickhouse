from __future__ import annotations

import os
from pathlib import Path

import pytest

from brickhouse.runtime.dbutils_local import (
    DbutilsOperationNotSupportedError,
    DbutilsPathError,
    LocalDbutils,
    LocalDbutilsFs,
    LocalDbutilsSecrets,
    LocalDbutilsWidgets,
)


def test_dbutils_filesystem_operations_share_dbfs_aliases(tmp_path: Path) -> None:
    fs = LocalDbutilsFs(tmp_path / "files" / "dbfs")

    assert fs.mkdirs("dbfs:/incoming/nested")
    assert fs.put("/dbfs/incoming/nested/input.txt", "hello brickhouse")
    assert fs.head("dbfs:/incoming/nested/input.txt", 5) == "hello"
    entries = fs.ls("dbfs:/incoming/nested")
    assert [(entry.name, entry.size, entry.isFile) for entry in entries] == [
        ("input.txt", 16, True)
    ]
    assert fs.cp("dbfs:/incoming/nested/input.txt", "dbfs:/incoming/copy.txt")
    assert fs.head("dbfs:/incoming/copy.txt") == "hello brickhouse"
    assert fs.mv("dbfs:/incoming/copy.txt", "dbfs:/incoming/moved.txt")
    assert not fs.rm("dbfs:/incoming/copy.txt")
    assert fs.rm("dbfs:/incoming/moved.txt")
    assert fs.rm("dbfs:/incoming", recurse=True)
    assert fs.ls("dbfs:/") == []


@pytest.mark.parametrize(
    "path",
    [
        "../outside",
        "dbfs:/../outside",
        "/dbfs/../outside",
        "dbfs:/a/./b",
        "dbfs:/a//b",
        "dbfs://host/path",
        "file:/etc/passwd",
        "/tmp/outside",
        "dbfs:\\outside",
    ],
)
def test_dbutils_filesystem_rejects_unmapped_and_traversing_paths(
    tmp_path: Path, path: str
) -> None:
    fs = LocalDbutilsFs(tmp_path / "dbfs")
    with pytest.raises(DbutilsPathError):
        fs.head(path)


def test_dbutils_filesystem_rejects_symlink_traversal(tmp_path: Path) -> None:
    root = tmp_path / "dbfs"
    fs = LocalDbutilsFs(root)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("outside", encoding="utf-8")
    try:
        (root / "escape").symlink_to(outside, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"test filesystem cannot create symlinks: {error}")

    with pytest.raises(DbutilsPathError, match="symbolic link"):
        fs.head("dbfs:/escape/secret.txt")
    with pytest.raises(DbutilsPathError, match="symbolic link"):
        fs.ls("dbfs:/")


def test_dbutils_filesystem_bounds_recursive_work_and_file_preview(tmp_path: Path) -> None:
    fs = LocalDbutilsFs(tmp_path / "dbfs")
    fs.mkdirs("dbfs:/data")
    fs.put("dbfs:/data/file.txt", "abcdef")

    with pytest.raises(ValueError, match="max_bytes"):
        fs.head("dbfs:/data/file.txt", 1024 * 1024 + 1)
    with pytest.raises(IsADirectoryError):
        fs.cp("dbfs:/data", "dbfs:/backup")
    with pytest.raises(FileExistsError):
        fs.put("dbfs:/data/file.txt", "changed")
    assert fs.put("dbfs:/data/file.txt", "changed", overwrite=True)
    assert fs.head("dbfs:/data/file.txt") == "changed"


def test_widgets_apply_parameters_after_defaults_and_are_attempt_local() -> None:
    first = LocalDbutilsWidgets({"mode": "fast", "colors": "red,blue"})
    first.text("mode", "safe")
    first.dropdown("region", "east", ["east", "west"])
    first.multiselect("colors", "red", ["red", "blue", "green"])

    assert first.get("mode") == "fast"
    assert first.get("region") == "east"
    assert first.get("colors") == "red,blue"
    assert first.getAll() == {"mode": "fast", "region": "east", "colors": "red,blue"}
    with pytest.raises(KeyError, match="not been declared"):
        first.get("missing")

    second = LocalDbutilsWidgets()
    second.text("mode", "safe")
    assert second.getAll() == {"mode": "safe"}
    first.remove("mode")
    assert second.get("mode") == "safe"
    first.removeAll()
    assert first.getAll() == {}


def test_widgets_reject_invalid_choice_values_and_unknown_dbutils_methods() -> None:
    widgets = LocalDbutilsWidgets({"region": "north"})
    with pytest.raises(ValueError, match="not one of choices"):
        widgets.dropdown("region", "east", ["east", "west"])
    with pytest.raises(DbutilsOperationNotSupportedError, match="dbutils.notebook"):
        _ = LocalDbutils(str(Path.cwd()), {}).notebook


def test_dbutils_secrets_expose_only_authorized_scope_metadata() -> None:
    secrets = LocalDbutilsSecrets({"approved": {"api-key": b"value"}})

    assert [scope.name for scope in secrets.listScopes()] == ["approved"]
    assert [item.key for item in secrets.list("approved")] == ["api-key"]
    assert secrets.get("approved", "api-key") == "value"
    assert secrets.getBytes("approved", "api-key") == b"value"
    with pytest.raises(KeyError, match="does not exist"):
        secrets.get("approved", "missing")
    with pytest.raises(PermissionError, match="not authorized"):
        secrets.get("unrelated", "api-key")


def test_dbutils_filesystem_methods_do_not_follow_directory_symlinks_on_remove(
    tmp_path: Path,
) -> None:
    fs = LocalDbutilsFs(tmp_path / "dbfs")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "kept.txt").write_text("keep", encoding="utf-8")
    try:
        (fs._root / "link").symlink_to(outside, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"test filesystem cannot create symlinks: {error}")

    with pytest.raises(DbutilsPathError, match="symbolic link"):
        fs.rm("dbfs:/link", recurse=True)
    assert (outside / "kept.txt").read_text(encoding="utf-8") == "keep"


@pytest.mark.skipif(os.name == "nt", reason="symlink permission behavior differs on Windows")
def test_local_dbutils_root_must_not_be_a_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"test filesystem cannot create symlinks: {error}")
    with pytest.raises(DbutilsPathError, match="root cannot be a symbolic link"):
        LocalDbutilsFs(link)
