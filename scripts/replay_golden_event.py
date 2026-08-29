#!/usr/bin/env python3
"""回放一个版本化黄金事件，并输出三路线可比较的范围判定基线。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.common import write_json  # noqa: E402
from app.taxonomy import classify_industry_item  # noqa: E402


REQUIRED_KEYS = {
    "schema_version", "event_id", "published_at", "topic", "importance", "official_url", "media_url",
    "title", "content", "expected", "route_inputs", "negative_controls",
}
ROUTES = ("legacy", "optimized", "agent_first")


def load_event(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not REQUIRED_KEYS.issubset(payload):
        missing = sorted(REQUIRED_KEYS.difference(payload if isinstance(payload, dict) else {}))
        raise ValueError(f"黄金事件缺少字段：{', '.join(missing)}")
    if payload.get("schema_version") != "robtaxi-golden-event-v1":
        raise ValueError("不支持的黄金事件版本")
    if set(payload["route_inputs"]) != set(ROUTES):
        raise ValueError("route_inputs 必须且只能包含 legacy、optimized、agent_first")
    return payload


def replay_event(event: dict[str, Any]) -> dict[str, Any]:
    """以每条路线各自的候选载荷回放共同的范围门槛。"""
    results: dict[str, dict[str, Any]] = {}
    for route in ROUTES:
        route_input = event["route_inputs"][route]
        source = dict(route_input["source"])
        row = {
            "title": event["title"],
            "content": event["content"],
            "canonical_url": route_input["discovery_url"],
        }
        classification = classify_industry_item(row, source)
        results[route] = {
            "candidate_id": route_input["candidate_id"],
            "discovery_url": route_input["discovery_url"],
            "in_scope": bool(classification["in_scope"]),
            "scope_reason": classification["scope_reason"],
            "coverage_domains": classification["coverage_domains"],
            "automation_level": classification["automation_level"],
        }
    independent_urls = {result["discovery_url"] for result in results.values()}
    discovered = sum(int(result["in_scope"]) for result in results.values())
    expected = event["expected"]
    return {
        "schema_version": "robtaxi-golden-event-replay-v1",
        "event_id": event["event_id"],
        "published_at": event["published_at"],
        "results": results,
        "independent_candidate_payloads": len(independent_urls),
        "discoveries": discovered,
        "minimum_independent_discoveries": int(expected["minimum_independent_discoveries"]),
        "acceptance_met": discovered >= int(expected["minimum_independent_discoveries"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="离线回放 Robotaxi Digest 黄金事件")
    parser.add_argument("--event", required=True, help="黄金事件 JSON 文件")
    parser.add_argument("--output", default="", help="可选报告输出路径")
    args = parser.parse_args()
    report = replay_event(load_event(Path(args.event).expanduser().resolve()))
    if args.output:
        write_json(Path(args.output).expanduser().resolve(), report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report["acceptance_met"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
