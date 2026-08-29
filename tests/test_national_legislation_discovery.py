from __future__ import annotations

import json
from pathlib import Path

from app.industry_agent.runner import _scan_prompt


ROOT = Path(__file__).resolve().parents[1]


def _queries(config: dict, name: str) -> list[str]:
    return [str(row["q"]) for row in config["query_sets"][name]]


def test_national_legislation_queries_reach_legacy_and_optimized_discovery() -> None:
    config = json.loads((ROOT / "sources.json").read_text(encoding="utf-8"))

    assert "national_legislation" in config["defaults"]["discovery_query_groups"]
    for query_set in ("domestic_robtaxi", "domestic_robtaxi_discovery", "domestic_robtaxi_search_result"):
        queries = _queries(config, query_set)
        assert any("道路交通安全法" in query or "道交法" in query for query in queries)
        assert any("自动驾驶汽车" in query and ("责任" in query or "保险" in query or "上路" in query) for query in queries)

    discovery_queries = _queries(config, "domestic_robtaxi_discovery")
    for domain in ("npc.gov.cn", "news.cn", "gov.cn", "moj.gov.cn"):
        assert any(f"site:{domain}" in query for query in discovery_queries)


def test_agent_scan_prompt_treats_national_legislation_as_non_brand_domain() -> None:
    config = json.loads((ROOT / "sources.json").read_text(encoding="utf-8"))

    prompt = _scan_prompt("2026-08-26", config)

    assert "国家级立法是独立必查领域" in prompt
    assert "不得要求企业、Robotaxi、L3 或 L4 字面命中" in prompt
    assert "site:npc.gov.cn" in prompt
