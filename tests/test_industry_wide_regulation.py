import json
from pathlib import Path

from app.filter_rules import _build_company_aliases, _defaults
from app.filter_scoring import _collect_signals, _score_stage2
from app.industry_agent.verifier import DefaultEvidenceVerifier
from app.taxonomy import classify_industry_item


ROOT = Path(__file__).resolve().parents[1]
LAW_TITLE = "道路交通安全法修订草案新增自动驾驶汽车特别规定专章"
LAW_CONTENT = "全国人大常委会审议修订草案，涉及自动驾驶汽车上路条件、违法处理责任和保险制度。"


class FakePageReader:
    def __init__(self, page: dict) -> None:
        self.page = page

    def read(self, url: str) -> dict:
        return {**self.page, "canonical_url": url}


def test_national_autonomous_vehicle_law_enters_narrow_industry_wide_regulation_domain() -> None:
    result = classify_industry_item({"title": LAW_TITLE, "content": LAW_CONTENT})

    assert result["in_scope"] is True
    assert result["automation_level"] == "unknown"
    assert result["coverage_domains"] == ["industry_wide_regulation"]
    assert result["scope_signals"]["national_legislation_hits"]
    assert result["scope_signals"]["regulation_impact_hits"]


def test_national_regulation_scoring_does_not_require_company_or_robotaxi_literal() -> None:
    config = json.loads((ROOT / "sources.json").read_text(encoding="utf-8"))
    settings = _defaults(config)
    row = {"title": LAW_TITLE, "content": LAW_CONTENT, "region": "domestic"}
    source = {"source_type": "search_api", "source_role": "search_discovery", "evidence_type": "general_media"}

    signals = _collect_signals(row, source, settings, _build_company_aliases(config))
    kept, score, reason, detail = _score_stage2(row, source, settings, signals)

    assert signals["company_hits"] == []
    assert signals["national_regulation_hits"]
    assert kept is True
    assert reason == "kept"
    assert score >= detail["threshold"]
    assert detail["score_breakdown"]["national_legislation"] == 76


def test_agent_verifier_accepts_official_national_law_without_company_or_level() -> None:
    url = "https://www.npc.gov.cn/golden/road-traffic-safety-law"
    config = json.loads((ROOT / "sources.json").read_text(encoding="utf-8"))
    reader = FakePageReader(
        {
            "ok": True,
            "publisher": "全国人大",
            "published_at_utc": "2026-08-25T03:00:00+00:00",
            "title": LAW_TITLE,
            "content": LAW_CONTENT,
        }
    )
    candidate = {
        "title": LAW_TITLE,
        "factual_summary": LAW_CONTENT,
        "companies": [],
        "coverage_domains": ["industry_wide_regulation"],
        "automation_level": "unknown",
        "event_type": "regulation",
        "deployment_stage": "unknown",
        "canonical_url": url,
        "evidence": [{"url": url, "evidence_type": "regulator"}],
        "score_breakdown": {
            "industry_impact": 30,
            "deployment_or_regulation": 25,
            "scope_relevance": 25,
            "evidence_quality": 20,
        },
    }

    event, reason = DefaultEvidenceVerifier(reader, config).verify(candidate, "2026-08-26", "law-test")

    assert reason == "verified"
    assert event is not None
    assert event.coverage_domains == ["industry_wide_regulation"]
