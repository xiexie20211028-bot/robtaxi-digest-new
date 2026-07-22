from __future__ import annotations

from typing import Any

from .filter_rules import _normalize_keywords, _source_profile


SEMANTIC_SIGNAL_TERMS = [
    "robtaxi",
    "robotaxi",
    "driverless taxi",
    "self-driving taxi",
    "autonomous taxi",
    "autonomous vehicle",
    "autonomous truck",
    "driverless truck",
    "driverless car",
    "self-driving car",
    "autonomous car",
    "无人驾驶",
    "自动驾驶",
    "无人驾驶货车",
    "自动驾驶货车",
    "智能网联汽车",
    "无人驾驶汽车",
    "网约车",
    "车队",
    "示范运营",
    "示范应用",
    "许可",
    "准入",
    "监管",
    "道路测试",
    "上路许可",
    "牌照管理",
    "征求意见稿",
    "通行管理",
    "l3",
    "l4",
    "level 3",
    "level 4",
    "icv",
    "deployment permit",
    "testing permit",
    "driverless testing",
    "rulemaking",
    "public comment",
    "disengagement report",
    "operational design domain",
    "commercial motor vehicle",
    "automated driving systems",
    "fully autonomous vehicles",
    "global technical regulation",
    "type approval",
    "automated passenger services",
    "self-driving passenger services",
    "automated vehicles act",
    "no user-in-charge",
]

STRATEGIC_COMPANY_KEYWORDS = [
    "小鹏",
    "小鹏汽车",
    "xpeng",
    "waymo",
    "tesla",
    "cybercab",
    "pony.ai",
    "pony ai",
    "小马智行",
    "weride",
    "文远知行",
    "apollo go",
    "萝卜快跑",
    "百度apollo",
    "百度 apollo",
]

STRATEGIC_AUTONOMOUS_KEYWORDS = [
    "robotaxi",
    "无人驾驶出租车",
    "自动驾驶出租车",
    "l4",
    "l3",
    "无人驾驶汽车",
    "自动驾驶",
]

STRATEGY_SIGNAL_KEYWORDS = [
    "直奔",
    "跳过",
    "落地",
    "部署",
    "推进",
    "量产",
    "商业化",
    "战略",
    "路线",
    "规划",
    "试运营",
    "示范运营",
    "deploy",
    "deployment",
    "commercialize",
    "commercialization",
    "strategy",
    "roadmap",
    "rollout",
    "launch",
    "scale",
    "l4 strategy",
    "robotaxi push",
]

SAFETY_ENTITY_KEYWORDS = [
    "momenta",
    "waymo",
    "tesla",
    "pony.ai",
    "pony ai",
    "weride",
    "文远知行",
    "百度apollo",
    "百度 apollo",
    "apollo go",
    "mobileye",
    "zoox",
    "cruise",
]

SAFETY_AUTONOMOUS_KEYWORDS = ["自动驾驶", "智能驾驶", "无人驾驶", "robotaxi"]

SAFETY_MILESTONE_KEYWORDS = [
    "功能安全",
    "安全认证",
    "最高等级",
    "asil d",
    "安全机制",
    "安全中间件",
    "functional safety",
    "safety certification",
    "safety case",
    "safety middleware",
]

AUTONOMOUS_CONTEXT_TERMS = [
    "无人驾驶",
    "自动驾驶",
    "robotaxi",
    "robtaxi",
    "autonomous",
    "self-driving",
    "driverless",
    "智能网联汽车",
    "无人驾驶汽车",
    "icv",
    "intelligent connected vehicle",
    "av",
    "apollo go",
]

LEVEL_TERMS = ["l3", "l4", "level 3", "level 4"]
TRUCK_TERMS = ["无人驾驶货车", "自动驾驶货车", "无人货运", "autonomous truck", "driverless truck", "freight", "truck"]



def _keyword_hits(text: str, words: list[str]) -> list[str]:
    hits = []
    for word in words:
        if word and word in text:
            hits.append(word)
    return sorted(set(hits))



def _collect_signals(
    row: dict[str, Any],
    source: dict[str, Any],
    cfg_defaults: dict[str, Any],
    company_aliases: list[str],
) -> dict[str, Any]:
    title = str(row.get("title", "")).strip()
    content = str(row.get("content", "")).strip()
    source_name = str(row.get("source_name", "")).strip()
    region = str(row.get("region", "foreign")).strip().lower()

    text_title = title.lower()
    text_all = f"{title} {content} {source_name}".lower()

    core_words = cfg_defaults["core_domestic"] if region == "domestic" else cfg_defaults["core_foreign"]
    context_words = cfg_defaults["context_domestic"] if region == "domestic" else cfg_defaults["context_foreign"]
    brand_words = cfg_defaults["brand_domestic"] if region == "domestic" else cfg_defaults["brand_foreign"]
    exclude_words = cfg_defaults["exclude_domestic"] if region == "domestic" else cfg_defaults["exclude_foreign"]

    include_words = _normalize_keywords(source.get("include_keywords", []))
    exclude_words = sorted(set(exclude_words + _normalize_keywords(source.get("exclude_keywords", []))))

    core_bucket = sorted(set(core_words + include_words))

    core_hits = _keyword_hits(text_all, core_bucket)
    core_title_hits = _keyword_hits(text_title, core_bucket)
    context_hits = _keyword_hits(text_all, context_words)
    brand_hits = _keyword_hits(text_all, brand_words)
    company_hits = _keyword_hits(text_all, company_aliases)
    semantic_hits = _keyword_hits(text_all, SEMANTIC_SIGNAL_TERMS)
    negative_hits = _keyword_hits(text_all, exclude_words)
    context_terms_hit = _keyword_hits(text_all, AUTONOMOUS_CONTEXT_TERMS)
    level_hits = _keyword_hits(text_all, LEVEL_TERMS)
    truck_hits = _keyword_hits(text_all, TRUCK_TERMS)
    fast_pass_title_hits = _keyword_hits(text_title, cfg_defaults["fast_pass_title_keywords"])

    candidate_signals = sorted(set(company_hits + brand_hits + context_hits + semantic_hits))

    return {
        "core_hits": core_hits,
        "core_title_hits": core_title_hits,
        "context_hits": context_hits,
        "brand_hits": brand_hits,
        "company_hits": company_hits,
        "semantic_hits": semantic_hits,
        "negative_hits": negative_hits,
        "context_terms_hit": context_terms_hit,
        "level_hits": level_hits,
        "truck_hits": truck_hits,
        "fast_pass_title_hits": fast_pass_title_hits,
        "candidate_signals": candidate_signals,
    }



def _is_fast_pass(row: dict[str, Any], signals: dict[str, Any], cfg_defaults: dict[str, Any]) -> bool:
    _ = row
    if not cfg_defaults["fast_pass_enabled"]:
        return False
    if not signals["fast_pass_title_hits"]:
        return False

    if cfg_defaults["fast_pass_require_company_or_context"]:
        has_company_signal = bool(signals["company_hits"] or signals["brand_hits"])
        has_context_signal = bool(signals["context_hits"])
        if not (has_company_signal or has_context_signal):
            return False
    return True



def _compute_strategic_shift_bonus(row: dict[str, Any], signals: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    title = str(row.get("title", "")).strip().lower()
    content = str(row.get("content", "")).strip().lower()
    text_all = f"{title} {content}"

    company_hits = sorted(set(signals["company_hits"] + signals["brand_hits"] + _keyword_hits(text_all, STRATEGIC_COMPANY_KEYWORDS)))
    autonomous_hits = _keyword_hits(text_all, STRATEGIC_AUTONOMOUS_KEYWORDS)
    strategy_hits = _keyword_hits(text_all, STRATEGY_SIGNAL_KEYWORDS)
    title_company_hits = _keyword_hits(title, STRATEGIC_COMPANY_KEYWORDS)
    title_autonomous_hits = _keyword_hits(title, ["robotaxi", "l4", "l3", "无人驾驶出租车", "自动驾驶出租车"])

    bonus = 0
    if company_hits and autonomous_hits and strategy_hits:
        bonus += 18
    if title_company_hits and title_autonomous_hits:
        bonus += 8
    bonus = min(22, bonus)
    return bonus, {
        "company_hits": company_hits,
        "autonomous_hits": autonomous_hits,
        "strategy_hits": strategy_hits,
        "title_company_hits": title_company_hits,
        "title_autonomous_hits": title_autonomous_hits,
    }



def _compute_safety_milestone_bonus(row: dict[str, Any], signals: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    title = str(row.get("title", "")).strip().lower()
    content = str(row.get("content", "")).strip().lower()
    text_all = f"{title} {content}"

    entity_hits = sorted(set(signals["company_hits"] + signals["brand_hits"] + _keyword_hits(text_all, SAFETY_ENTITY_KEYWORDS)))
    autonomous_hits = _keyword_hits(text_all, SAFETY_AUTONOMOUS_KEYWORDS)
    safety_hits = _keyword_hits(text_all, SAFETY_MILESTONE_KEYWORDS)
    title_safety_hits = _keyword_hits(title, ["asil d", "安全认证"])

    bonus = 0
    if entity_hits and autonomous_hits and safety_hits:
        bonus += 12
    if title_safety_hits:
        bonus += 6
    bonus = min(16, bonus)
    return bonus, {
        "entity_hits": entity_hits,
        "autonomous_hits": autonomous_hits,
        "safety_hits": safety_hits,
        "title_safety_hits": title_safety_hits,
    }



def _score_stage2(
    row: dict[str, Any],
    source: dict[str, Any],
    cfg_defaults: dict[str, Any],
    signals: dict[str, Any],
) -> tuple[bool, int, str, dict[str, Any]]:
    source_type = str(source.get("source_type", "rss")).strip().lower() or "rss"
    profile = _source_profile(source)

    score_breakdown = {
        "core": 0,
        "title": 0,
        "context": 0,
        "brand": 0,
        "company": 0,
        "semantic": 0,
        "profile": 0,
        "search_api": 0,
        "negative": 0,
        "pair_penalty": 0,
        "strategic_shift_bonus": 0,
        "safety_milestone_bonus": 0,
    }

    if signals["core_hits"]:
        score_breakdown["core"] = 20 + min(25, len(signals["core_hits"]) * 8)
    if signals["core_title_hits"]:
        score_breakdown["title"] = 10 + min(15, len(signals["core_title_hits"]) * 6)
    if signals["context_hits"]:
        score_breakdown["context"] = min(12, len(signals["context_hits"]) * 3)
    if signals["brand_hits"]:
        score_breakdown["brand"] = min(16, len(signals["brand_hits"]) * 4)
    if signals["company_hits"]:
        score_breakdown["company"] = 8 + min(18, len(signals["company_hits"]) * 5)
    if signals["semantic_hits"]:
        score_breakdown["semantic"] = min(12, len(signals["semantic_hits"]) * 4)

    score_breakdown["profile"] = {
        "general_media": 0,
        "industry_media": 6,
        "newsroom": 10,
        "regulator": 10,
        "research": 8,
    }.get(profile, 0)

    if source_type == "search_api":
        score_breakdown["search_api"] = 4

    if signals["negative_hits"]:
        score_breakdown["negative"] = -min(36, len(signals["negative_hits"]) * 12)

    pair_issues: list[str] = []
    if cfg_defaults["pair_require_level_context"] and signals["level_hits"] and not signals["context_terms_hit"]:
        pair_issues.append("level_without_context")
        score_breakdown["pair_penalty"] -= 14
    if cfg_defaults["pair_require_truck_context"] and signals["truck_hits"] and not signals["context_terms_hit"]:
        pair_issues.append("truck_without_context")
        score_breakdown["pair_penalty"] -= 18

    strategic_shift_bonus, strategic_shift_detail = _compute_strategic_shift_bonus(row, signals)
    safety_milestone_bonus, safety_milestone_detail = _compute_safety_milestone_bonus(row, signals)
    score_breakdown["strategic_shift_bonus"] = strategic_shift_bonus
    score_breakdown["safety_milestone_bonus"] = safety_milestone_bonus

    score = sum(score_breakdown.values())
    score = max(0, min(100, score))

    detail = {
        "profile": profile,
        "core_hits": signals["core_hits"],
        "context_hits": signals["context_hits"],
        "brand_hits": signals["brand_hits"],
        "company_hits": signals["company_hits"],
        "semantic_hits": signals["semantic_hits"],
        "negative_hits": signals["negative_hits"],
        "pair_issues": pair_issues,
        "score_breakdown": score_breakdown,
        "strategic_shift_detail": strategic_shift_detail,
        "safety_milestone_detail": safety_milestone_detail,
    }

    if pair_issues and not (signals["core_hits"] or signals["company_hits"] or signals["context_terms_hit"]):
        return False, score, "pair_rule_mismatch", detail

    if profile == "general_media" and cfg_defaults["require_company_signal_for_general_media"]:
        if not signals["core_hits"] and not signals["company_hits"]:
            return False, score, "general_no_core_or_company", detail

    threshold_key = "search_api" if source_type == "search_api" else profile
    threshold = cfg_defaults["thresholds"].get(threshold_key, 65)
    detail["threshold"] = threshold

    if score < threshold:
        return False, score, "score_below_threshold", detail

    return True, score, "kept", detail
