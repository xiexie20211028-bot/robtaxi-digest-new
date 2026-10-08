"""合并双路当日结果；路线异常不阻止另一条有效路线正常发布。"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from app.common import read_json, read_jsonl, write_jsonl
from app.report import patch_report, report_path
from .handoff import validate_manifest, validate_legacy_manifest
from .import_events import import_events


def prepare(date: str, root: Path, raw: Path, reports: Path, commit: str, run_id: str,
            legacy_status: str, profile: str = "hybrid_domestic") -> dict:
    valid, reason = validate_manifest(root, date, commit, run_id)
    raw_file = raw / date / "raw_items.jsonl"
    raw_exists = raw_file.is_file()
    if not raw_exists:
        write_jsonl(raw_file, [])
    report_file = report_path(reports, date)
    # 采集任务失败但留下半份文件，不能当作完整 legacy 输入。
    try:
        legacy_report = read_json(report_file) if report_file.is_file() else {}
        if not isinstance(legacy_report, dict):
            raise ValueError("invalid_legacy_report")
    except (OSError, ValueError):
        legacy_report = {}
        # 损坏的采集报告也不能影响 Agent 单路当日供稿。
        report_file.unlink(missing_ok=True)
    legacy_usable = legacy_status == "success" and raw_exists and legacy_report.get("collection_date") == date and validate_legacy_manifest(raw.parent, date, commit, run_id)
    if legacy_usable:
        legacy_usable = any(int(row.get("request_success_count", 0)) > 0 for row in legacy_report.get("source_stats", []))
    if not legacy_usable:
        write_jsonl(raw_file, [])
    else:
        rows = read_jsonl(raw_file)
        for row in rows:
            row["run_id"] = run_id
            row.setdefault("payload", {})["discovery_routes"] = ["legacy"]
        write_jsonl(raw_file, rows)
    agent_report = read_json(root / date / "agent_run_report.json") if valid else {}
    if not isinstance(agent_report.get("status", ""), str):
        agent_report["status"] = "failed"
    agent_usable = valid and agent_report.get("status") in {"success", "success_empty", "degraded", "partial_budget"}
    # degraded 只有已核验事件才可使用；空且覆盖完整才是合法零产出。
    try:
        agent_rows = read_jsonl(root / date / "agent_events.jsonl") if agent_usable else []
    except (OSError, ValueError):
        agent_rows = []
        agent_usable = False
    agent_usable = agent_usable and (bool(agent_rows) or agent_report.get("status") == "success_empty")
    if agent_report.get("status") == "success_empty" and agent_rows:
        agent_usable = False
    if agent_rows:
        agent_usable = agent_usable and all(isinstance(row, dict) and row.get("verification_status") in {"verified_primary", "verified_two_media"} and row.get("agent_run_id") == agent_report.get("agent_run_id") for row in agent_rows)
    if profile == "legacy":
        agent_usable = False
    notices = []
    if agent_usable:
        imported = import_events(date, profile, root, raw, reports, handoff_commit=commit, handoff_run_id=run_id)
        if agent_report.get("status") in {"degraded", "partial_budget"}:
            notices.append("本期 Agent 部分完成，仅采用已完成证据核验的事件。")
    else:
        imported = {"status": "missing" if not valid else "agent_failed", "imported": 0}
        if profile != "legacy":
            notices.append("本期 Agent 未完成，使用 Legacy 采集结果。")
    if not legacy_usable:
        notices = ["本期 Legacy 采集未完成，仅采用可用 Agent 研究结果，海外覆盖可能缺失。"] if agent_usable else ["本期两条发现路线均未完成，日报更新失败。"]
    available = legacy_usable or agent_usable
    patch_report(report_file, active_profile=profile, agent_import_status=imported["status"], agent_imported_count=imported["imported"], domestic_agent_notice="".join(notices), route_status={"agent": agent_report.get("status", reason), "legacy": legacy_status}, publication_status="ready" if available else "failed")
    return {"available": available, "agent_usable": agent_usable, "legacy_usable": legacy_usable, "notice": "".join(notices)}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--date", required=True)
    p.add_argument("--legacy-status", required=True)
    p.add_argument("--profile", default="hybrid_domestic")
    p.add_argument("--root", default="artifacts-agent")
    p.add_argument("--raw", default="artifacts/raw")
    p.add_argument("--reports", default="artifacts/reports")
    p.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT", ""))
    args = p.parse_args()
    result = prepare(args.date, Path(args.root), Path(args.raw), Path(args.reports), os.environ.get("GITHUB_SHA", ""), os.environ.get("GITHUB_RUN_ID", ""), args.legacy_status, args.profile)
    if args.github_output:
        with open(args.github_output, "a") as f:
            f.write(f"available={str(result['available']).lower()}\n")
    print(result["notice"] or "双路正式供稿已准备")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
