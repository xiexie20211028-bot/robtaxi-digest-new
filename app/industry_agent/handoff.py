"""只接受同次生产运行、同日期、同提交且校验完整的 Agent 工件。"""
from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path

from app.common import read_json, write_json


def create_manifest(root: Path, date: str, commit: str, run_id: str) -> dict:
    directory = root / date
    files = {}
    for path in sorted(directory.glob("agent_*")):
        if path.is_file() and not path.is_symlink():
            files[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {"version": 1, "date": date, "commit": commit, "workflow_run_id": run_id, "files": files}
    write_json(directory / "handoff_manifest.json", manifest)
    return manifest


def validate_manifest(root: Path, date: str, commit: str, run_id: str) -> tuple[bool, str]:
    directory = root / date
    try:
        manifest = read_json(directory / "handoff_manifest.json")
        if not isinstance(manifest, dict):
            return False, "handoff_invalid_manifest"
        if not commit or not run_id or manifest.get("version") != 1 or (manifest.get("date"), manifest.get("commit"), manifest.get("workflow_run_id")) != (date, commit, run_id):
            return False, "handoff_identity_mismatch"
        files = manifest.get("files", {})
        if not {"agent_run_report.json", "agent_events.jsonl"}.issubset(files):
            return False, "handoff_required_files_missing"
        for name, expected in files.items():
            if Path(name).name != name:
                return False, "handoff_invalid_filename"
            path = directory / name
            if path.is_symlink() or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                return False, "handoff_checksum_mismatch"
        report = read_json(directory / "agent_run_report.json")
        if not isinstance(report, dict):
            return False, "handoff_invalid_report"
        if report.get("run_date") != date:
            return False, "handoff_report_date_mismatch"
        return True, "verified"
    except (OSError, ValueError, TypeError):
        return False, "handoff_unreadable"


def create_legacy_manifest(root: Path, date: str, commit: str, run_id: str) -> dict:
    names = [f"raw/{date}/raw_items.jsonl", f"reports/{date}/run_report.json"]
    files = {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in names if (root / name).is_file()}
    manifest = {"version": 1, "date": date, "commit": commit, "workflow_run_id": run_id, "files": files}
    write_json(root / date / "legacy_handoff_manifest.json", manifest)
    return manifest


def validate_legacy_manifest(root: Path, date: str, commit: str, run_id: str) -> bool:
    try:
        value = read_json(root / date / "legacy_handoff_manifest.json")
        if not isinstance(value, dict):
            return False
        names = {f"raw/{date}/raw_items.jsonl", f"reports/{date}/run_report.json"}
        if not commit or not run_id or value.get("version") != 1 or (value.get("date"), value.get("commit"), value.get("workflow_run_id")) != (date, commit, run_id) or set(value.get("files", {})) != names:
            return False
        return all(not (root / name).is_symlink() and hashlib.sha256((root / name).read_bytes()).hexdigest() == value["files"][name] for name in names)
    except (OSError, ValueError, TypeError):
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="artifacts-agent")
    parser.add_argument("--date", required=True)
    parser.add_argument("--commit", default=os.environ.get("GITHUB_SHA", ""))
    parser.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID", ""))
    parser.add_argument("--route", choices=["agent", "legacy"], default="agent")
    args = parser.parse_args()
    create = create_legacy_manifest if args.route == "legacy" else create_manifest
    create(Path(args.root), args.date, args.commit, args.run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
