from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .common import read_json, write_json


HEALTHY_STATUSES = {"ok", "healthy"}
DEGRADED_STATUSES = {"partial", "degraded"}
FAILED_STATUSES = {"fail", "failed", "silent_dead"}
SEVERITY_ORDER = {"healthy": 0, "degraded": 1, "failed": 2, "silent_dead": 3}


def normalize_health_status(value: str) -> str:
    status = str(value).strip().lower()
    if status in HEALTHY_STATUSES:
        return "healthy"
    if status in DEGRADED_STATUSES:
        return "degraded"
    if status == "silent_dead":
        return "silent_dead"
    return "failed"


def update_source_health_history(
    stats: list[dict[str, Any]],
    history_path: Path,
    date_text: str,
    retention_days: int = 35,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """持久化信源健康历史，并识别连续空列表形成的静默失效。"""
    history: dict[str, Any] = {"version": 1, "days": []}
    if history_path.exists():
        try:
            loaded = read_json(history_path)
            if isinstance(loaded, dict):
                history = loaded
        except Exception:
            pass

    days = history.get("days", [])
    if not isinstance(days, list):
        days = []
    days = [day for day in days if isinstance(day, dict) and str(day.get("date", "")) != date_text]
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).date().isoformat()
    days = [day for day in days if str(day.get("date", "")) >= cutoff]

    day_stats: list[dict[str, Any]] = []
    for stat in stats:
        day_stats.append(
            {
                "source_id": str(stat.get("source_id", "")),
                "status": normalize_health_status(str(stat.get("status", ""))),
                "listed_items": int(stat.get("listed_items", stat.get("fetched_items", 0))),
                "valid_items": int(stat.get("valid_items", stat.get("fetched_items", 0))),
                "date_parse_rate": float(stat.get("date_parse_rate", 0.0)),
                "body_parse_rate": float(stat.get("body_parse_rate", 0.0)),
                "whitelist_reject_rate": float(stat.get("whitelist_reject_rate", 0.0)),
                "criticality": str(stat.get("criticality", "important")),
                "source_role": str(stat.get("source_role", "secondary")),
                "request_count": int(stat.get("request_count", 0)),
                "request_success_count": int(stat.get("request_success_count", 0)),
            }
        )
    days.append({"date": date_text, "sources": day_stats})
    days.sort(key=lambda value: str(value.get("date", "")))
    history["days"] = days[-retention_days:]

    records_by_source: dict[str, list[dict[str, Any]]] = {}
    for day in history["days"]:
        for record in day.get("sources", []):
            sid = str(record.get("source_id", ""))
            if sid:
                records_by_source.setdefault(sid, []).append(record)

    output_stats: list[dict[str, Any]] = []
    rolling: dict[str, Any] = {}
    for original in stats:
        stat = dict(original)
        sid = str(stat.get("source_id", ""))
        records = records_by_source.get(sid, [])
        policy = stat.get("health_policy", {}) if isinstance(stat.get("health_policy", {}), dict) else {}
        empty_limit = max(2, int(policy.get("empty_listing_limit", 3)))
        recent = records[-empty_limit:]
        consecutive_empty = len(recent) >= empty_limit and all(
            normalize_health_status(str(record.get("status", ""))) == "healthy"
            and int(record.get("listed_items", 0)) == 0
            for record in recent
        )
        status = normalize_health_status(str(stat.get("status", "")))
        if status == "healthy" and consecutive_empty and str(stat.get("criticality", "")) == "required":
            status = "silent_dead"
            stat["error_reason_code"] = "consecutive_empty_listing"
            stat["error_reason_zh"] = f"连续 {empty_limit} 次空列表，疑似静默失效"
            stat["error"] = stat["error_reason_zh"]
        stat["status"] = status
        consecutive_failures = 0
        for value in reversed(records):
            if normalize_health_status(str(value.get("status", ""))) == "healthy":
                break
            consecutive_failures += 1
        stat["consecutive_failures"] = consecutive_failures

        last7 = records[-7:]
        last30 = records[-30:]
        rolling[sid] = {
            "days_7": len(last7),
            "days_30": len(last30),
            "success_rate_7d": _success_rate(last7),
            "success_rate_30d": _success_rate(last30),
            "empty_days_7d": sum(1 for value in last7 if int(value.get("listed_items", 0)) == 0),
            "empty_days_30d": sum(1 for value in last30 if int(value.get("listed_items", 0)) == 0),
            "consecutive_failures": consecutive_failures,
        }
        stat["rolling_7d"] = rolling[sid]["success_rate_7d"]
        stat["rolling_30d"] = rolling[sid]["success_rate_30d"]
        output_stats.append(stat)

    write_json(history_path, history)
    return output_stats, {"history_days": len(history["days"]), "sources": rolling}


def _success_rate(records: list[dict[str, Any]]) -> float:
    if not records:
        return 0.0
    successes = sum(1 for record in records if normalize_health_status(str(record.get("status", ""))) == "healthy")
    return round(successes / len(records), 4)
