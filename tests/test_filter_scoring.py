from app.filter_rules import _build_company_aliases, _defaults
from app.filter_scoring import (
    _collect_signals,
    _compute_safety_milestone_bonus,
    _compute_strategic_shift_bonus,
    _score_stage2,
)


CFG = {
    "defaults": {
        "core_keywords_foreign": ["robotaxi", "waymo", "driverless taxi", "automated driving systems", "global technical regulation", "automated passenger services", "self-driving passenger services", "no user-in-charge"],
        "core_keywords_domestic": ["robotaxi", "小马智行", "萝卜快跑"],
        "context_keywords_foreign": ["deployment", "permit", "safety", "regulation", "type approval", "permitting scheme", "automated vehicles act"],
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



def test_unece_regulatory_milestone_scores_as_regulator() -> None:
    settings = _defaults(CFG)
    aliases = _build_company_aliases(CFG)
    row = {
        "title": "UNECE adopts global technical regulation for automated driving systems",
        "content": "WP.29 and GRVA rules cover fully autonomous vehicles, type approval and operational design domain requirements.",
        "source_name": "UNECE Vehicle Regulations",
        "region": "foreign",
    }
    source = {
        "source_type": "structured_web",
        "category": "regulator",
        "source_profile": "regulator",
        "include_keywords": ["UNECE", "WP.29", "automated driving systems", "global technical regulation", "type approval"],
    }
    signals = _collect_signals(row, source, settings, aliases)
    assert signals["candidate_signals"]
    is_keep, score, reason, detail = _score_stage2(row, source, settings, signals)
    assert is_keep
    assert reason == "kept"
    assert score >= detail["threshold"]


def test_govuk_aps_milestone_scores_as_general_discovery() -> None:
    settings = _defaults(CFG)
    aliases = _build_company_aliases(CFG)
    row = {
        "title": "GOV.UK automated passenger services permitting scheme for self-driving passenger services",
        "content": "The Automated Vehicles Act framework covers no user-in-charge services and permits for automated vehicles.",
        "source_name": "GOV.UK",
        "region": "foreign",
    }
    source = {"source_type": "search_result", "category": "media", "source_profile": "general_media"}
    signals = _collect_signals(row, source, settings, aliases)
    assert signals["candidate_signals"]
    is_keep, score, reason, detail = _score_stage2(row, source, settings, signals)
    assert is_keep
    assert reason == "kept"
    assert score >= detail["threshold"]
