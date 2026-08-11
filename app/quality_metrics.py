from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .common import read_json, write_json
from .source_health import normalize_health_status


def current_quality_metrics(items: list[dict[str, Any]], source_stats: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(items)
    coverage: Counter[str] = Counter()
    regions: Counter[str] = Counter()
    sources: Counter[str] = Counter()
    primary = 0
    discovery = 0
    for item in items:
        coverage.update(str(value) for value in item.get("coverage_domains", []) if str(value))
        regions[str(item.get("region", "foreign"))] += 1
        sources[str(item.get("source_id", "unknown"))] += 1
        role = str(item.get("source_role", "secondary"))
        if role == "primary":
            primary += 1
        if role in {"search_discovery", "social_discovery"}:
            discovery += 1
    silent_dead = sum(1 for stat in source_stats if normalize_health_status(str(stat.get("status", ""))) == "silent_dead")
    return {
        "items": total,
        "coverage_distribution": dict(coverage),
        "region_distribution": dict(regions),
        "primary_source_share": round(primary / total, 4) if total else 0.0,
        "discovery_dependency_share": round(discovery / total, 4) if total else 0.0,
        "max_single_source_share": round(max(sources.values(), default=0) / total, 4) if total else 0.0,
        "silent_dead_sources": silent_dead,
    }


def update_quality_metrics_history(
    history_path: Path,
    date_text: str,
    current: dict[str, Any],
    retention_days: int = 35,
) -> dict[str, Any]:
    history: dict[str, Any] = {"version": 1, "days": []}
    if history_path.exists():
        try:
            loaded = read_json(history_path)
            if isinstance(loaded, dict):
                history = loaded
        except Exception:
            pass
    days = history.get("days", []) if isinstance(history.get("days", []), list) else []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).date().isoformat()
    days = [day for day in days if isinstance(day, dict) and str(day.get("date", "")) >= cutoff and str(day.get("date", "")) != date_text]
    days.append({"date": date_text, "metrics": current})
    days.sort(key=lambda day: str(day.get("date", "")))
    history["days"] = days[-retention_days:]
    write_json(history_path, history)
    return {
        "current": current,
        "rolling_7d": _aggregate(history["days"][-7:]),
        "rolling_30d": _aggregate(history["days"][-30:]),
        "history_days": len(history["days"]),
    }


def _aggregate(days: list[dict[str, Any]]) -> dict[str, Any]:
    if not days:
        return {}
    total_items = 0
    coverage: Counter[str] = Counter()
    regions: Counter[str] = Counter()
    weighted_primary = 0.0
    weighted_discovery = 0.0
    max_source_share = 0.0
    silent_dead = 0
    for day in days:
        metrics = day.get("metrics", {}) if isinstance(day.get("metrics", {}), dict) else {}
        items = int(metrics.get("items", 0))
        total_items += items
        coverage.update({str(k): int(v) for k, v in metrics.get("coverage_distribution", {}).items()})
        regions.update({str(k): int(v) for k, v in metrics.get("region_distribution", {}).items()})
        weighted_primary += float(metrics.get("primary_source_share", 0.0)) * items
        weighted_discovery += float(metrics.get("discovery_dependency_share", 0.0)) * items
        max_source_share = max(max_source_share, float(metrics.get("max_single_source_share", 0.0)))
        silent_dead = max(silent_dead, int(metrics.get("silent_dead_sources", 0)))
    return {
        "days": len(days),
        "items": total_items,
        "coverage_distribution": dict(coverage),
        "region_distribution": dict(regions),
        "primary_source_share": round(weighted_primary / total_items, 4) if total_items else 0.0,
        "discovery_dependency_share": round(weighted_discovery / total_items, 4) if total_items else 0.0,
        "max_single_source_share": round(max_source_share, 4),
        "silent_dead_sources": silent_dead,
    }
