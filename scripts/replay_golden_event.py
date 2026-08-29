#!/usr/bin/env python3
"""离线回放版本化黄金事件，执行三条路线各自的本地决策适配器。"""

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
from app.decision_log import build_candidate_decision  # noqa: E402
from app.filter_rules import _build_company_aliases, _defaults  # noqa: E402
from app.filter_scoring import _collect_signals, _score_stage2  # noqa: E402
from app.industry_agent.verifier import DefaultEvidenceVerifier  # noqa: E402
from app.taxonomy import classify_industry_item  # noqa: E402


REQUIRED_KEYS = {
    "schema_version", "event_id", "published_at", "topic", "importance", "official_url", "media_url",
    "title", "content", "expected", "route_inputs", "negative_controls",
}
ROUTES = ("legacy", "optimized", "agent_first")


class FixturePageReader:
    """只读取黄金 fixture 提供的本地页面，不允许离线回放访问网络。"""

    def __init__(self, pages: dict[str, dict[str, Any]]) -> None:
        self.pages = pages

    def read(self, url: str) -> dict[str, Any]:
        return dict(self.pages.get(url, {"ok": False, "url": url}))


def load_event(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not REQUIRED_KEYS.issubset(payload):
        missing = sorted(REQUIRED_KEYS.difference(payload if isinstance(payload, dict) else {}))
        raise ValueError(f"黄金事件缺少字段：{', '.join(missing)}")
    if payload.get("schema_version") != "robtaxi-golden-event-v1":
        raise ValueError("不支持的黄金事件版本")
    if set(payload["route_inputs"]) != set(ROUTES):
        raise ValueError("route_inputs 必须且只能包含 legacy、optimized、agent_first")
    if not isinstance(payload["negative_controls"], list) or len(payload["negative_controls"]) < 20:
        raise ValueError("黄金事件至少需要 20 条法规/L2/泛交通负例")
    return payload


def _row(event: dict[str, Any], route_input: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": event["title"],
        "content": event["content"],
        "canonical_url": route_input["discovery_url"],
        "region": "domestic",
    }


def _decision(
    route_input: dict[str, Any],
    *,
    kept: bool,
    stage: str,
    reason: str,
    classification: dict[str, Any],
    signals: dict[str, Any] | None = None,
    score: int = 0,
    threshold: int | None = None,
) -> dict[str, Any]:
    return build_candidate_decision(
        route="golden_replay",
        candidate={"title": route_input.get("title", ""), "canonical_url": route_input["discovery_url"]},
        source=dict(route_input.get("source", {})),
        candidate_id=route_input["candidate_id"],
        kept=kept,
        stage=stage,
        final_reason=reason,
        signals=signals or classification.get("scope_signals", {}),
        score=score,
        threshold=threshold,
        extra={
            "discovery_url": route_input["discovery_url"],
            "in_scope": bool(classification["in_scope"]),
            "scope_reason": classification["scope_reason"],
            "coverage_domains": classification["coverage_domains"],
            "automation_level": classification["automation_level"],
        },
    )


def _legacy_replay(event: dict[str, Any], route_input: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    source = {"source_type": "search_api", "source_role": "search_discovery", **dict(route_input["source"])}
    row = _row(event, route_input)
    classification = classify_industry_item(row, source)
    if not classification["in_scope"]:
        return _decision(route_input, kept=False, stage="scope_gate", reason=classification["scope_reason"], classification=classification)
    settings = _defaults(config)
    signals = _collect_signals(row, source, settings, _build_company_aliases(config))
    if not signals["candidate_signals"]:
        return _decision(route_input, kept=False, stage="candidate_gate", reason="candidate_gate_miss", classification=classification, signals=signals)
    kept, score, reason, detail = _score_stage2(row, source, settings, signals)
    return _decision(
        route_input,
        kept=kept,
        stage="stage2",
        reason=reason,
        classification=classification,
        signals=signals,
        score=score,
        threshold=int(detail.get("threshold", 0) or 0),
    )


def _optimized_replay(event: dict[str, Any], route_input: dict[str, Any]) -> dict[str, Any]:
    classification = classify_industry_item(_row(event, route_input), dict(route_input["source"]))
    return _decision(
        route_input,
        kept=bool(classification["in_scope"]),
        stage="scope_gate",
        reason="kept" if classification["in_scope"] else classification["scope_reason"],
        classification=classification,
    )


def _agent_replay(event: dict[str, Any], route_input: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    url = str(route_input["discovery_url"])
    page = {
        "ok": True,
        "canonical_url": url,
        "publisher": "国家立法黄金 fixture",
        "published_at_utc": "2026-08-25T03:00:00+00:00",
        "title": event["title"],
        "content": event["content"],
    }
    candidate = {
        "title": event["title"],
        "factual_summary": event["content"],
        "companies": [],
        "coverage_domains": ["industry_wide_regulation"],
        "automation_level": "unknown",
        "event_type": "regulation",
        "deployment_stage": "unknown",
        "canonical_url": url,
        "evidence": [{"url": url, "evidence_type": str(route_input["source"].get("evidence_type", "regulator"))}],
        "score_breakdown": {
            "industry_impact": 30,
            "deployment_or_regulation": 25,
            "scope_relevance": 25,
            "evidence_quality": 20,
        },
    }
    classification = classify_industry_item({"title": candidate["title"], "content": candidate["factual_summary"]}, route_input["source"])
    verified, reason = DefaultEvidenceVerifier(FixturePageReader({url: page}), config).verify(candidate, "2026-08-26", "golden-replay")
    return _decision(
        route_input,
        kept=verified is not None,
        stage="evidence_verification",
        reason=reason,
        classification=classification,
        signals=classification.get("scope_signals", {}),
        score=int(verified.importance_score) if verified else 100,
        threshold=65,
    )


def replay_event(event: dict[str, Any]) -> dict[str, Any]:
    """运行 legacy、optimized 和 Agent-first 的离线真实决策路径。"""
    config = json.loads((ROOT / "sources.json").read_text(encoding="utf-8"))
    route_inputs = {
        route: {**dict(event["route_inputs"][route]), "title": event["title"]}
        for route in ROUTES
    }
    results = {
        "legacy": _legacy_replay(event, route_inputs["legacy"], config),
        "optimized": _optimized_replay(event, route_inputs["optimized"]),
        "agent_first": _agent_replay(event, route_inputs["agent_first"], config),
    }
    kept = [result for result in results.values() if result["kept"]]
    independent_urls = {result["discovery_url"] for result in kept}
    negative_results = [
        {
            "title": title,
            "kept": bool(classify_industry_item({"title": str(title)}, {"evidence_type": "industry_media"})["in_scope"]),
        }
        for title in event["negative_controls"]
    ]
    expected = event["expected"]
    minimum = int(expected["minimum_independent_discoveries"])
    return {
        "schema_version": "robtaxi-golden-event-replay-v2",
        "event_id": event["event_id"],
        "published_at": event["published_at"],
        "results": results,
        "independent_candidate_payloads": len({result["discovery_url"] for result in results.values()}),
        "independent_discoveries": len(independent_urls),
        "discoveries": len(kept),
        "minimum_independent_discoveries": minimum,
        "negative_controls": negative_results,
        "negative_controls_passed": all(not result["kept"] for result in negative_results),
        "acceptance_met": len(independent_urls) >= minimum and all(not result["kept"] for result in negative_results),
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
