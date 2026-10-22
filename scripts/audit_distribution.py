"""Audit built BrickHouse wheel/sdist metadata and retained third-party notices."""

from __future__ import annotations

import argparse
import hashlib
import json
import tomllib
from email.parser import Parser
from pathlib import Path, PurePosixPath
from tarfile import open as open_tar
from typing import Any
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[1]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def credential_path(name: str) -> bool:
    filename = PurePosixPath(name).name
    return filename == ".env" or (filename.startswith(".env.") and filename != ".env.example")


def audit_wheel(path: Path, project: dict[str, Any], expected_notices: set[str]) -> dict[str, Any]:
    with ZipFile(path) as archive:
        names = set(archive.namelist())
        sensitive = sorted(name for name in names if credential_path(name))
        require(not sensitive, f"Wheel contains credential-file paths: {sensitive}")
        metadata_paths = sorted(name for name in names if name.endswith(".dist-info/METADATA"))
        require(len(metadata_paths) == 1, f"Expected one wheel METADATA file: {path}")
        metadata = Parser().parsestr(archive.read(metadata_paths[0]).decode("utf-8"))

        require(metadata.get("Name") == project["name"], "Wheel name differs from pyproject.toml")
        require(
            metadata.get("Version") == project["version"],
            "Wheel version differs from pyproject.toml",
        )
        require(
            metadata.get("Requires-Python") == project["requires-python"],
            "Wheel Python requirement differs from pyproject.toml",
        )
        require(
            metadata.get("License-Expression") == project["license"],
            "Wheel license expression differs from pyproject.toml",
        )
        require(
            metadata.get("Description-Content-Type") == "text/markdown",
            "Wheel is missing its Markdown project description",
        )

        requires_dist = set(metadata.get_all("Requires-Dist", []))
        for dependency in project["dependencies"]:
            require(dependency in requires_dist, f"Wheel is missing base dependency {dependency}")
        optional = project["optional-dependencies"].get("notebooks", [])
        for dependency in optional:
            require(
                f"{dependency}; extra == 'notebooks'" in requires_dist,
                f"Wheel is missing notebook-extra dependency {dependency}",
            )

        package_files = [name for name in names if name.startswith("brickhouse/")]
        require(bool(package_files), "Wheel contains no brickhouse package modules")
        license_prefix = metadata_paths[0].removesuffix("METADATA") + "licenses/"
        retained = {
            name.removeprefix(license_prefix) for name in names if name.startswith(license_prefix)
        }
        require(
            expected_notices <= retained,
            f"Wheel is missing retained notice files: {sorted(expected_notices - retained)}",
        )
        for notice in expected_notices:
            require(
                archive.read(license_prefix + notice) == (ROOT / notice).read_bytes(),
                f"Wheel retained notice differs from source: {notice}",
            )
        return {
            "path": str(path),
            "sha256": sha256(path),
            "name": metadata.get("Name"),
            "version": metadata.get("Version"),
            "requires_python": metadata.get("Requires-Python"),
            "license_expression": metadata.get("License-Expression"),
            "package_file_count": len(package_files),
            "retained_notice_files": sorted(expected_notices),
        }


def audit_sdist(path: Path, project: dict[str, Any], expected_notices: set[str]) -> dict[str, Any]:
    root_name = f"{project['name']}-{project['version']}"
    with open_tar(path, "r:gz") as archive:
        names = set(archive.getnames())
        for notice in expected_notices:
            member = archive.extractfile(f"{root_name}/{notice}")
            require(member is not None, f"Sdist is missing retained notice: {notice}")
            assert member is not None
            require(
                member.read() == (ROOT / notice).read_bytes(),
                f"Sdist retained notice differs from source: {notice}",
            )
        pkg_info = archive.extractfile(f"{root_name}/PKG-INFO")
        require(pkg_info is not None, "Sdist is missing PKG-INFO")
        assert pkg_info is not None
        metadata = Parser().parsestr(pkg_info.read().decode("utf-8"))
        require(
            metadata.get("Name") == project["name"]
            and metadata.get("Version") == project["version"]
            and metadata.get("License-Expression") == project["license"],
            "Sdist name/version/license metadata differs from pyproject.toml",
        )
    sensitive = sorted(name for name in names if credential_path(name))
    require(not sensitive, f"Sdist contains credential-file paths: {sensitive}")
    release_bundle = f"{root_name}/artifacts/releases/"
    require(
        not any(name.startswith(release_bundle) for name in names),
        "Sdist must not recursively include retained release candidates",
    )
    require(f"{root_name}/pyproject.toml" in names, "Sdist is missing pyproject.toml")
    retained = {
        name.removeprefix(f"{root_name}/") for name in names if name.startswith(f"{root_name}/")
    }
    require(
        expected_notices <= retained,
        f"Sdist is missing retained notice files: {sorted(expected_notices - retained)}",
    )
    return {
        "path": str(path),
        "sha256": sha256(path),
        "name": project["name"],
        "version": project["version"],
        "retained_notice_files": sorted(expected_notices),
    }


def discover(artifact_dir: Path, suffix: str) -> Path:
    artifacts = sorted(artifact_dir.glob(f"*{suffix}"))
    require(len(artifacts) == 1, f"Expected one *{suffix} artifact under {artifact_dir}")
    return artifacts[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, default=Path("dist"))
    args = parser.parse_args()
    artifact_dir = args.artifact_dir.resolve()
    wheel = discover(artifact_dir, ".whl")
    sdist = discover(artifact_dir, ".tar.gz")

    project_config = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    require(project_config.get("license") == "Apache-2.0", "Expected Apache-2.0 project license")
    require((ROOT / "LICENSE").is_file(), "Project LICENSE is missing")
    expected_notices = {"LICENSE", "THIRD_PARTY_NOTICES.md"}
    expected_notices.update(
        f"THIRD_PARTY_LICENSES/{path.name}"
        for path in sorted((ROOT / "THIRD_PARTY_LICENSES").glob("*.txt"))
    )
    require(len(expected_notices) > 2, "No retained third-party license files found")

    result = {
        "result": "passed",
        "project_license_selected": bool(
            project_config.get("license") or (ROOT / "LICENSE").is_file()
        ),
        "wheel": audit_wheel(wheel, project_config, expected_notices),
        "sdist": audit_sdist(sdist, project_config, expected_notices),
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
