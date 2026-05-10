from app.filter_rules import _build_company_aliases, _defaults
from app.filter_scoring import (
    _collect_signals,
    _compute_safety_milestone_bonus,
    _compute_strategic_shift_bonus,
    _score_stage2,
)


CFG = {
    "defaults": {
        "core_keywords_foreign": ["robotaxi", "waymo", "driverless taxi"],
        "core_keywords_domestic": ["robotaxi", "小马智行", "萝卜快跑"],
        "context_keywords_foreign": ["deployment", "permit", "safety", "regulation"],
        "context_keywords_domestic": ["部署", "监管", "许可", "安全"],
        "brand_keywords_foreign": ["waymo", "tesla", "pony.ai", "weride", "momenta"],
        "brand_keywords_domestic": ["小马智行", "文远知行", "momenta"],
        "exclude_keywords_foreign": [],
        "exclude_keywords_domestic": [],
        "fast_pass_enabled": True,
    },
    "companies": [{"name": "Waymo", "aliases": ["waymo"]}, {"name": "Momenta", "aliases": ["momenta"]}],
}


def test_general_media_requires_core_or_company_signal() -> None:
    settings = _defaults(CFG)
    aliases = _build_company_aliases(CFG)
    row = {"title": "AI hiring plans", "content": "Macro industry commentary", "source_name": "Example", "region": "foreign"}
    source = {"source_type": "rss", "category": "media", "source_profile": "general_media"}
    signals = _collect_signals(row, source, settings, aliases)
    is_keep, score, reason, _ = _score_stage2(row, source, settings, signals)
    assert not is_keep
    assert reason == "general_no_core_or_company"
    assert score >= 0


def test_strategic_shift_bonus_hits_xpeng_robotaxi_route_change() -> None:
    row = {
        "title": "XPeng robotaxi L4 strategy rollout",
        "content": "XPeng plans to deploy robotaxi services and commercialize its L4 roadmap.",
    }
    signals = {"company_hits": ["xpeng"], "brand_hits": [], "context_hits": [], "semantic_hits": [], "negative_hits": [], "context_terms_hit": [], "level_hits": [], "truck_hits": []}
    bonus, detail = _compute_strategic_shift_bonus(row, signals)
    assert bonus == 22
    assert "xpeng" in detail["company_hits"]


def test_safety_milestone_bonus_hits_momenta_asil_d() -> None:
    row = {
        "title": "Momenta achieves ASIL D safety certification",
        "content": "Momenta robotaxi autonomous driving stack reached a new functional safety milestone.",
    }
    signals = {"company_hits": ["momenta"], "brand_hits": [], "context_hits": [], "semantic_hits": [], "negative_hits": [], "context_terms_hit": [], "level_hits": [], "truck_hits": []}
    bonus, detail = _compute_safety_milestone_bonus(row, signals)
    assert bonus == 16
    assert "momenta" in detail["entity_hits"]
