"""Explicit public-only artifact verification; never installs or executes candidate code."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import tarfile
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]
MAX_ARTIFACT_BYTES = 80 * 1024 * 1024
ALLOWED_HOSTS = {
    "files.pythonhosted.org",
    "crates.io",
    "static.crates.io",
    "raw.githubusercontent.com",
}


def download(url: str) -> bytes:
    if urlsplit(url).scheme != "https" or urlsplit(url).hostname not in ALLOWED_HOSTS:
        raise ValueError("Research verifier only permits the recorded public artifact hosts")
    request = urllib.request.Request(
        url, headers={"User-Agent": "BrickHouse-B063-public-artifact-research/1.0"}
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        if urlsplit(response.url).hostname not in ALLOWED_HOSTS:
            raise ValueError("Unexpected artifact redirect host")
        data: bytes = response.read(MAX_ARTIFACT_BYTES + 1)
    if len(data) > MAX_ARTIFACT_BYTES:
        raise ValueError("Artifact exceeds the research download limit")
    return data


def verify_hash(data: bytes, expected: str) -> None:
    if hashlib.sha256(data).hexdigest() != expected:
        raise ValueError("Artifact or inspected source hash differs from the recorded snapshot")


def verify_archive(record: dict[str, Any]) -> dict[str, Any]:
    data = download(record["artifact_url"])
    verify_hash(data, record["artifact_sha256"])
    inspected = (
        record.get("inspected_license_and_metadata_files", [])
        + record.get("inspected_source_files", [])
        + record.get("inspected_files", [])
    )
    if record["artifact_url"].endswith(".whl"):
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            for member in inspected:
                verify_hash(archive.read(member["path"]), member["sha256"])
    else:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as archive:
            for member in inspected:
                stream = archive.extractfile(member["path"])
                if stream is None:
                    raise ValueError("Recorded archive member is not a regular readable file")
                verify_hash(stream.read(), member["sha256"])
    return {
        "artifact": record.get("artifact_filename", record.get("crate", "native source")),
        "sha256": record["artifact_sha256"],
        "bytes": len(data),
        "inspected_members_verified": len(inspected),
        "result": "hashes_verified_static_only",
    }


def verify_source(record: dict[str, Any]) -> dict[str, Any]:
    for member in record["inspected_files"]:
        verify_hash(download(member["url"]), member["sha256"])
    return {
        "repository": record["repository"],
        "commit": record["commit"],
        "inspected_members_verified": len(record["inspected_files"]),
        "result": "hashes_verified_static_only",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify-public-artifacts", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not args.verify_public_artifacts:
        parser.error("Public downloads require explicit --verify-public-artifacts")
    inventory = json.loads((ROOT / "planning/oss-reuse.json").read_text(encoding="utf-8"))
    archives = [item["inspection"] for item in inventory["package_candidates"]]
    archives.extend(
        item["additional_source_inspection"]
        for item in inventory["package_candidates"]
        if "additional_source_inspection" in item
    )
    archives.extend(item for item in inventory["native_source_candidates"] if "crate" in item)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(verify_archive, archives))
    results.extend(
        verify_source(item) for item in inventory["native_source_candidates"] if "commit" in item
    )
    output = {
        "research_task": "B063",
        "result": "passed",
        "qualified_for_runtime": False,
        "artifact_or_source_checks": results,
        "limitations": "Static hash/source checks only; no install, import, build or runtime test",
    }
    encoded = json.dumps(output, indent=2) + "\n"
    if args.output:
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
