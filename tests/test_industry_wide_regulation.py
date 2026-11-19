import json
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from app.fetch_rss import _parse_rss_feed
from app.fetch_structured import _extract_article_css
from app.filter_rules import _build_company_aliases, _defaults
from app.filter_scoring import _collect_signals, _score_stage2
from app.industry_agent.page_reader import GenericPageReader
from app.industry_agent.verifier import DefaultEvidenceVerifier
from app.parse import canonicalize_row
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


def test_roundup_cannot_combine_regulation_signals_across_news_items() -> None:
    config = json.loads((ROOT / "sources.json").read_text(encoding="utf-8"))
    row = {
        "title": "科技早报：食品召回；企业动态；辅助驾驶",
        "content": "食品召回涉及赔偿。美国国务院依据知识产权法实施制裁。"
        "受监督的自动驾驶功能公布搭载率，仍须驾驶员持续监督。",
        "region": "domestic",
    }
    signals = _collect_signals(row, {}, _defaults(config), [])

    assert signals["national_legislation_hits"]
    assert signals["regulation_impact_hits"]
    assert signals["national_regulation_hits"] == []
    assert classify_industry_item(row)["in_scope"] is False


def test_roundup_can_include_a_self_contained_national_automated_driving_rule() -> None:
    row = {
        "title": "多条行业消息",
        "content": "市场发布月度数据。全国人大审议自动驾驶汽车上路条件和责任的新草案；"
        "另一则消息关注普通车机系统。",
    }

    assert classify_industry_item(row)["coverage_domains"] == ["industry_wide_regulation"]


@pytest.mark.parametrize(
    ("title", "content"),
    [
        ("道路交通安全法修订草案提请审议", "全国人大常委会审议草案。草案新增自动驾驶车辆的道路通行、事故责任和保险规定。"),
        ("道路交通安全法修订草案提请审议", "全国人大常委会审议了自动驾驶相关修订草案。草案明确事故责任与保险安排。"),
        ("Federal transport update", "The legislative draft in the U.S. would set insurance and liability rules for self-driving vehicles."),
        ("Federal transport update", "The State Council published a legislative draft on self-driving vehicles. The draft clarifies liability and insurance."),
        ("道路交通安全法将规范自动驾驶车辆上路和保险", "全国人大常委会审议道路交通安全法修订草案。"),
    ],
)
def test_single_regulatory_item_keeps_explicit_continuation_and_abbreviation(title, content) -> None:
    config = json.loads((ROOT / "sources.json").read_text(encoding="utf-8"))
    row = {"title": title, "content": content, "region": "domestic"}
    assert classify_industry_item(row)["coverage_domains"] == ["industry_wide_regulation"]
    assert _collect_signals(row, {}, _defaults(config), [])["national_regulation_hits"]


def test_regulation_headline_cannot_borrow_details_from_unrelated_lead() -> None:
    row = {
        "title": "道路交通安全法将规范自动驾驶车辆上路和保险",
        "content": "全国人大常委会审议《食品安全法修订草案》。",
    }
    assert classify_industry_item(row)["in_scope"] is False


def test_article_routes_keep_headline_only_roundup_boundaries() -> None:
    url = "https://example.org/roundup"
    title = "科技快讯"
    body = "<article><ul><li>美国国务院宣布新制裁</li><li>食品召回赔偿问题</li><li>受监督的自动驾驶功能更新</li></ul></article>"
    feed = f"<rss><channel><item><title>{title}</title><link>{url}</link><description><![CDATA[{body}]]></description></item></channel></rss>"
    rss = _parse_rss_feed(feed.encode(), "综合媒体")[0]
    structured = _extract_article_css(url, f"<h1>{title}</h1>{body}", {"content": "article"}, "综合媒体")
    page = GenericPageReader._body(BeautifulSoup(body, "html.parser"))
    for source_type, payload in (("rss", rss), ("structured_web", structured),
                                 ("rss", {"title": title, "content": page, "link": url}),
                                 ("rss", {"title": title, "content": "美国国务院宣布新制裁\n食品召回赔偿问题\n受监督的自动驾驶功能更新", "link": url})):
        item = canonicalize_row({"source_type": source_type, "payload": payload})
        assert item is not None and len(item.content.splitlines()) == 3
        assert classify_industry_item({"title": item.title, "content": item.content})["in_scope"] is False


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
