from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from app.common import now_beijing, read_json, read_jsonl, sha1_text, write_jsonl
from app.report import patch_report, report_path
from app.source_config import PROFILE_NAMES, load_source_config


PRIMARY_TYPES = {"regulator", "dataset", "filing", "company_newsroom"}
ACCEPTED_STATUSES = {"success", "success_empty", "partial_budget", "degraded"}


def event_to_raw(event: dict[str, Any]) -> dict[str, Any] | None:
    canonical = str(event.get("canonical_url", "")).strip()
    title = str(event.get("title", "")).strip()
    published = str(event.get("published_at_utc", "")).strip()
    if not canonical or not title or not published:
        return None
    evidence = [dict(value) for value in event.get("evidence", []) if isinstance(value, dict)]
    primary = next((value for value in evidence if str(value.get("evidence_type", "")) in PRIMARY_TYPES), None)
    representative = primary or (evidence[0] if evidence else {})
    evidence_type = str(representative.get("evidence_type", "industry_media"))
    source_role = "primary" if evidence_type in PRIMARY_TYPES else "secondary"
    publisher = str(representative.get("publisher", "")).strip() or (urlparse(canonical).netloc or "行业 Agent")
    host = (urlparse(canonical).netloc or "agent").lower().removeprefix("www.")
    companies = [str(value).strip() for value in event.get("companies", []) if str(value).strip()]
    return {
        "source_id": f"agent_{sha1_text(host)[:10]}",
        "source_name": publisher,
        "source_type": "agent_event",
        "region": "domestic",
        "company_hint": companies[0] if companies else "",
        "fetched_at": str(event.get("first_seen_at_utc", "")),
        "url": canonical,
        "source_role": source_role,
        "evidence_type": evidence_type,
        "criticality": "important",
        "coverage_domains": [str(value) for value in event.get("coverage_domains", []) if str(value)],
        "official_accounts": {},
        "payload": {
            "title": title,
            "content": str(event.get("factual_summary", "")),
            "summary": str(event.get("factual_summary", "")),
            "link": canonical,
            "canonical_url": canonical,
            "published": published,
            "source_name": publisher,
            "discovery_method": "agent_search",
            "evidence": evidence,
            "agent_run_id": str(event.get("agent_run_id", "")),
            "verification_status": str(event.get("verification_status", "")),
            "importance_score": int(event.get("importance_score", 0) or 0),
            "score_breakdown": dict(event.get("score_breakdown", {})),
            "outbound_urls": [str(value.get("url", "")) for value in evidence if str(value.get("url", ""))],
        },
    }


def import_events(
    date_text: str,
    profile: str,
    handoff_root: Path,
    raw_root: Path,
    report_root: Path,
    runtime_state_file: Path | None = None,
) -> dict[str, Any]:
    agent_dir = handoff_root / date_text
    agent_report_file = agent_dir / "agent_run_report.json"
    agent_events_file = agent_dir / "agent_events.jsonl"
    raw_file = raw_root / date_text / "raw_items.jsonl"
    report_file = report_path(report_root, date_text)
    imported = 0
    status = "not_required"
    notice = ""

    if profile != "agent_domestic":
        runtime_state = {}
        if runtime_state_file and runtime_state_file.exists():
            try:
                runtime_state = read_json(runtime_state_file)
            except Exception:
                runtime_state = {}
        if bool(runtime_state.get("fallback_active", False)):
            status = "legacy_rollback"
            notice = "国内行业 Agent 连续两日失败，已在回滚窗口内临时恢复旧国内信源流程。"
        patch_report(
            report_file,
            agent_import_status=status,
            agent_imported_count=0,
            domestic_agent_notice=notice,
        )
        return {"status": status, "imported": 0}

    if not agent_report_file.exists():
        status = "missing"
        notice = "国内行业 Agent 产物缺失，本期国内部分仅含监管骨干信息。"
    else:
        agent_report = read_json(agent_report_file)
        agent_status = str(agent_report.get("status", "failed"))
        if agent_status not in ACCEPTED_STATUSES:
            status = "agent_failed"
            notice = "国内行业 Agent 运行异常，本期国内部分仅含监管骨干信息。"
        elif agent_status == "success_empty":
            status = "success_empty"
        else:
            existing = read_jsonl(raw_file)
            converted = [event_to_raw(row) for row in read_jsonl(agent_events_file)]
            agent_rows = [row for row in converted if isinstance(row, dict)]
            existing_keys = {
                str(row.get("payload", {}).get("canonical_url", row.get("url", "")))
                for row in existing
                if isinstance(row, dict)
            }
            new_rows = [
                row
                for row in agent_rows
                if str(row.get("payload", {}).get("canonical_url", row.get("url", ""))) not in existing_keys
            ]
            write_jsonl(raw_file, existing + new_rows)
            imported = len(new_rows)
            status = agent_status
            if agent_status in {"partial_budget", "degraded"}:
                notice = "国内行业 Agent 本次为降级运行，仅合并已完成证据核验的事件。"

    total_raw = len(read_jsonl(raw_file))
    current_report = read_json(report_file) if report_file.exists() else {}
    stage_funnel = current_report.get("stage_funnel", {}) if isinstance(current_report.get("stage_funnel", {}), dict) else {}
    stage_funnel["agent_event"] = {
        "fetched": imported,
        "candidate": int(stage_funnel.get("agent_event", {}).get("candidate", 0)),
        "filtered": int(stage_funnel.get("agent_event", {}).get("filtered", 0)),
        "kept": int(stage_funnel.get("agent_event", {}).get("kept", 0)),
    }
    patch_report(
        report_file,
        agent_import_status=status,
        agent_imported_count=imported,
        total_items_raw=total_raw,
        stage_funnel=stage_funnel,
        domestic_agent_notice=notice,
    )
    return {"status": status, "imported": imported, "notice": notice}


def main() -> int:
    parser = argparse.ArgumentParser(description="将行业 Agent 事件在末端导入现有原始候选池")
    parser.add_argument("--date", default="")
    parser.add_argument("--profile", choices=sorted(PROFILE_NAMES), default="")
    parser.add_argument("--sources", default="./sources.json")
    parser.add_argument("--in", dest="handoff_root", default="./.agent-handoff")
    parser.add_argument("--raw", default="./artifacts/raw")
    parser.add_argument("--report", default="./artifacts/reports")
    parser.add_argument("--runtime-state", default="./.state/agent_runtime.json")
    args = parser.parse_args()
    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    _cfg, profile = load_source_config(Path(args.sources).expanduser().resolve(), args.profile)
    result = import_events(
        date_text,
        profile,
        Path(args.handoff_root).expanduser().resolve(),
        Path(args.raw).expanduser().resolve(),
        Path(args.report).expanduser().resolve(),
        Path(args.runtime_state).expanduser().resolve(),
    )
    print(f"[agent_import] profile={profile} status={result['status']} imported={result['imported']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
