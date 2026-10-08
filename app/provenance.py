"""发现路线由程序记录；媒体出处和摘要生成方式不参与路线判断。"""
from __future__ import annotations

from collections import Counter
from typing import Any

LABELS = {"agent": "Agent 研究", "legacy": "Legacy 采集", "both": "共同发现", "unknown": "来源待核实"}


def routes(item: dict[str, Any]) -> list[str]:
    values = item.get("discovery_routes", [])
    return sorted({v for v in values if v in {"agent", "legacy"}}) if isinstance(values, list) else []


def route_key(item: dict[str, Any]) -> str:
    value = routes(item)
    return "both" if len(value) == 2 else value[0] if value else "unknown"


def label(item: dict[str, Any]) -> str:
    return LABELS[route_key(item)]


def counts_text(items: list[dict[str, Any]]) -> str:
    counts = Counter(route_key(item) for item in items)
    text = "｜".join(f"{LABELS[k]} {counts[k]} 条" for k in ("agent", "legacy", "both"))
    return text + (f"｜来源待核实 {counts['unknown']} 条" if counts["unknown"] else "")


def merge(target: dict[str, Any], incoming: dict[str, Any]) -> None:
    target["discovery_routes"] = sorted(set(routes(target)) | set(routes(incoming)))
    for field in ("route_records", "evidence"):
        values = [dict(v) for v in target.get(field, []) if isinstance(v, dict)]
        for value in incoming.get(field, []):
            if isinstance(value, dict) and value not in values:
                values.append(dict(value))
        target[field] = values
    if "agent" in routes(incoming):
        for field in ("agent_run_id", "agent_verification_status"):
            if incoming.get(field):
                target[field] = incoming[field]
        target["agent_importance_score"] = max(int(target.get("agent_importance_score", 0)), int(incoming.get("agent_importance_score", 0)))
        for field in ("first_disclosed_at_utc", "filing_disclosed_at_utc"):
            if incoming.get(field):
                target[field] = min(str(target.get(field) or incoming[field]), str(incoming[field]))


def merge_objects(target: Any, incoming: Any) -> None:
    values = vars(target).copy()
    merge(values, vars(incoming))
    for field in ("discovery_routes", "route_records", "evidence", "agent_run_id", "agent_verification_status", "agent_importance_score", "first_disclosed_at_utc", "filing_disclosed_at_utc"):
        if hasattr(target, field) and field in values:
            setattr(target, field, values[field])
