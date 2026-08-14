from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .common import read_json, write_json
from .taxonomy import classify_industry_item


def evaluate_rollout_gate(
    source_history: dict[str, Any],
    quality_history: dict[str, Any],
    golden: dict[str, Any],
    min_days: int = 14,
) -> dict[str, Any]:
    source_days = source_history.get("days", []) if isinstance(source_history.get("days", []), list) else []
    quality_days = quality_history.get("days", []) if isinstance(quality_history.get("days", []), list) else []
    source_days = source_days[-min_days:]
    quality_days = quality_days[-min_days:]

    request_count = request_success = 0
    body_rates: list[float] = []
    date_rates: list[float] = []
    severe_events: list[dict[str, str]] = []
    for day in source_days:
        for source in day.get("sources", []):
            if str(source.get("criticality", "")) != "required":
                continue
            request_count += int(source.get("request_count", 0))
            request_success += int(source.get("request_success_count", 0))
            if int(source.get("valid_items", 0)) > 0:
                body_rates.append(float(source.get("body_parse_rate", 0.0)))
                date_rates.append(float(source.get("date_parse_rate", 0.0)))
            if str(source.get("status", "")) in {"failed", "silent_dead"}:
                severe_events.append({"date": str(day.get("date", "")), "source_id": str(source.get("source_id", ""))})

    total_selected = primary_weighted = discovery_weighted = 0.0
    for day in quality_days:
        metrics = day.get("metrics", {}) if isinstance(day.get("metrics", {}), dict) else {}
        items = int(metrics.get("items", 0))
        total_selected += items
        primary_weighted += float(metrics.get("primary_source_share", 0.0)) * items
        discovery_weighted += float(metrics.get("discovery_dependency_share", 0.0)) * items

    positive = [str(value) for value in golden.get("positive", [])]
    negative = [str(value) for value in golden.get("negative", [])]
    broad_source = {
        "coverage_domains": ["robotaxi", "passenger_l3", "passenger_l4", "core_supply_chain", "regulation_safety"],
        "evidence_type": "industry_media",
    }
    true_positive = sum(classify_industry_item({"title": title}, broad_source)["in_scope"] for title in positive)
    false_positive = sum(classify_industry_item({"title": title}, broad_source)["in_scope"] for title in negative)
    recall = true_positive / len(positive) if positive else 0.0
    precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else 0.0

    metrics = {
        "shadow_days": min(len(source_days), len(quality_days)),
        "p0_request_success_rate": round(request_success / request_count, 4) if request_count else 0.0,
        "body_parse_rate": round(sum(body_rates) / len(body_rates), 4) if body_rates else 0.0,
        "date_parse_rate": round(sum(date_rates) / len(date_rates), 4) if date_rates else 0.0,
        "golden_recall": round(recall, 4),
        "golden_precision": round(precision, 4),
        "primary_source_share": round(primary_weighted / total_selected, 4) if total_selected else 0.0,
        "discovery_dependency_share": round(discovery_weighted / total_selected, 4) if total_selected else 0.0,
        "severe_health_events": severe_events,
    }
    checks = {
        "minimum_shadow_days": metrics["shadow_days"] >= min_days,
        "p0_request_success_rate": metrics["p0_request_success_rate"] >= 0.98,
        "body_parse_rate": metrics["body_parse_rate"] >= 0.95,
        "date_parse_rate": metrics["date_parse_rate"] >= 0.95,
        "golden_recall": metrics["golden_recall"] >= 0.85,
        "golden_precision": metrics["golden_precision"] >= 0.85,
        "primary_source_share": metrics["primary_source_share"] >= 0.60,
        "discovery_dependency_share": metrics["discovery_dependency_share"] <= 0.25,
        "no_severe_health_events": not severe_events,
    }
    return {
        "schema_version": "optimized-rollout-gate-v1",
        "minimum_days": min_days,
        "passed": all(checks.values()),
        "checks": checks,
        "metrics": metrics,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="检查 optimized profile 是否达到切换生产门槛")
    parser.add_argument("--source-health-history", default="./.state-shadow/source_health_history.json")
    parser.add_argument("--quality-history", default="./.state-shadow/digest_metrics_history.json")
    parser.add_argument("--golden", default="./tests/fixtures/golden_scope.json")
    parser.add_argument("--output", default="./artifacts-shadow/rollout_gate.json")
    parser.add_argument("--min-days", type=int, default=14)
    args = parser.parse_args()
    source_path = Path(args.source_health_history).expanduser().resolve()
    quality_path = Path(args.quality_history).expanduser().resolve()
    golden_path = Path(args.golden).expanduser().resolve()
    source_history = read_json(source_path) if source_path.exists() else {"days": []}
    quality_history = read_json(quality_path) if quality_path.exists() else {"days": []}
    golden = json.loads(golden_path.read_text(encoding="utf-8"))
    result = evaluate_rollout_gate(source_history, quality_history, golden, args.min_days)
    write_json(Path(args.output).expanduser().resolve(), result)
    print(f"[rollout_gate] passed={result['passed']} metrics={json.dumps(result['metrics'], ensure_ascii=False)}")
    return 0 if result["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
