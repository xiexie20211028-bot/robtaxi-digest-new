from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

from .common import normalize_url, now_beijing, parse_datetime, read_json, read_jsonl, write_jsonl
from .report import mark_stage, patch_report, report_path


ALLOWED_SOURCE_PROFILES = {"general_media", "industry_media", "newsroom", "regulator", "research"}

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
    "许可",
    "监管",
    "l3",
    "l4",
    "level 3",
    "level 4",
    "icv",
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

FAST_PASS_TITLE_KEYWORDS_ZH_DEFAULT = [
    "robotaxi",
    "无人驾驶出租车",
    "自动驾驶出租车",
    "l4",
    "l3",
    "智能网联汽车",
    "无人驾驶汽车",
]

FAST_PASS_TITLE_KEYWORDS_EN_DEFAULT = [
    "robotaxi",
    "driverless taxi",
    "autonomous taxi",
    "self-driving taxi",
    "level 4",
    "level 3",
    "intelligent connected vehicle",
    "icv",
    "driverless car",
    "autonomous car",
    "self-driving car",
]

DROP_REASON_ZH = {
    "general_no_core_or_company": "通用媒体缺少核心词或公司信号",
    "score_below_threshold": "相关性评分低于阈值",
    "time_window": "超出时间窗口",
    "url_invalid": "链接无效",
    "url_homepage": "首页链接非文章",
    "url_not_in_allow_patterns": "链接不在允许路径",
    "url_blocked_pattern": "命中屏蔽路径",
    "general_source_cap": "通用媒体单源条数超限",
    "pair_rule_mismatch": "关键词配对规则不满足",
    "published_missing": "发布时间缺失",
    "not_today": "非当日新闻",
    "source_max_age": "超出来源时效窗口",
    "candidate_gate_miss": "未命中候选信号",
    "fast_pass": "直通保留",
    "kept": "保留",
}


def reason_zh(reason: str) -> str:
    return DROP_REASON_ZH.get(reason, reason)


def _normalize_keywords(words: list[Any]) -> list[str]:
    out = []
    for word in words:
        text = str(word).strip().lower()
        if text:
            out.append(text)
    return sorted(set(out))


def _source_profile(source: dict[str, Any]) -> str:
    raw = str(source.get("source_profile", "")).strip().lower()
    if raw in ALLOWED_SOURCE_PROFILES:
        return raw
    category = str(source.get("category", "")).strip().lower()
    if category == "media":
        return "general_media"
    if category in {"newsroom", "regulator", "research"}:
        return category
    return "industry_media"


def _build_company_aliases(cfg: dict[str, Any]) -> list[str]:
    aliases: set[str] = set()
    for company in cfg.get("companies", []):
        if not isinstance(company, dict):
            continue
        name = str(company.get("name", "")).strip().lower()
        if len(name) >= 2:
            aliases.add(name)
        for alias in company.get("aliases", []):
            text = str(alias).strip().lower()
            if len(text) >= 2:
                aliases.add(text)
    return sorted(aliases)


def _parse_int(raw: Any, default: int) -> int:
    try:
        return int(raw)
    except Exception:
        return default


def _resolve_timezone(name: str) -> timezone:
    if ZoneInfo is None:
        return timezone.utc
    try:
        return ZoneInfo(name)
    except Exception:
        return timezone.utc


def _defaults(cfg: dict[str, Any]) -> dict[str, Any]:
    defaults = cfg.get("defaults", {})
    if not isinstance(defaults, dict):
        defaults = {}

    mode = str(defaults.get("relevance_mode", "high_precision")).strip().lower()
    if mode not in {"high_precision", "balanced", "high_recall"}:
        mode = "high_precision"

    thresholds = defaults.get("relevance_thresholds", {})
    if not isinstance(thresholds, dict):
        thresholds = {}
    score_defaults = {
        "high_precision": {"general_media": 75, "industry_media": 65, "newsroom": 55, "regulator": 55, "research": 55, "search_api": 65},
        "balanced": {"general_media": 68, "industry_media": 58, "newsroom": 50, "regulator": 50, "research": 50, "search_api": 58},
        "high_recall": {"general_media": 60, "industry_media": 50, "newsroom": 45, "regulator": 45, "research": 45, "search_api": 52},
    }
    base_thresholds = score_defaults[mode]
    final_thresholds: dict[str, int] = {}
    for key, val in base_thresholds.items():
        final_thresholds[key] = _parse_int(thresholds.get(key, val), int(val))

    core_domestic = _normalize_keywords(defaults.get("core_keywords_domestic", defaults.get("domestic_keywords", [])))
    core_foreign = _normalize_keywords(defaults.get("core_keywords_foreign", defaults.get("foreign_keywords", [])))
    context_domestic = _normalize_keywords(defaults.get("context_keywords_domestic", []))
    context_foreign = _normalize_keywords(defaults.get("context_keywords_foreign", []))
    brand_domestic = _normalize_keywords(defaults.get("brand_keywords_domestic", []))
    brand_foreign = _normalize_keywords(defaults.get("brand_keywords_foreign", []))
    exclude_domestic = _normalize_keywords(defaults.get("exclude_keywords_domestic", []))
    exclude_foreign = _normalize_keywords(defaults.get("exclude_keywords_foreign", []))

    pair_rules = defaults.get("keyword_pair_rules", {})
    if not isinstance(pair_rules, dict):
        pair_rules = {}

    allow_missing_published_profiles = defaults.get("allow_missing_published_profiles", ["regulator"])
    if not isinstance(allow_missing_published_profiles, list):
        allow_missing_published_profiles = ["regulator"]
    allow_missing_published_profiles = [
        str(x).strip().lower() for x in allow_missing_published_profiles if str(x).strip().lower() in ALLOWED_SOURCE_PROFILES
    ]
    if not allow_missing_published_profiles:
        allow_missing_published_profiles = ["regulator"]

    fast_pass_title_keywords_zh = _normalize_keywords(
        defaults.get("fast_pass_title_keywords_zh", FAST_PASS_TITLE_KEYWORDS_ZH_DEFAULT)
    )
    fast_pass_title_keywords_en = _normalize_keywords(
        defaults.get("fast_pass_title_keywords_en", FAST_PASS_TITLE_KEYWORDS_EN_DEFAULT)
    )
    fast_pass_title_keywords = sorted(set(fast_pass_title_keywords_zh + fast_pass_title_keywords_en))

    return {
        "relevance_mode": mode,
        "window_days": _parse_int(defaults.get("window_days", 10), 10),
        "thresholds": final_thresholds,
        "core_domestic": core_domestic,
        "core_foreign": core_foreign,
        "context_domestic": context_domestic,
        "context_foreign": context_foreign,
        "brand_domestic": brand_domestic,
        "brand_foreign": brand_foreign,
        "exclude_domestic": exclude_domestic,
        "exclude_foreign": exclude_foreign,
        "require_company_signal_for_general_media": bool(defaults.get("require_company_signal_for_general_media", True)),
        "max_general_media_items_per_source": _parse_int(defaults.get("max_general_media_items_per_source", 2), 2),
        "enable_general_media_source_cap": bool(defaults.get("enable_general_media_source_cap", False)),
        "pair_require_level_context": bool(pair_rules.get("require_level_with_autonomous_context", True)),
        "pair_require_truck_context": bool(pair_rules.get("require_truck_with_autonomous_context", True)),
        "allow_missing_published_profiles": allow_missing_published_profiles,
        "strict_today_mode": bool(defaults.get("strict_today_mode", False)),
        "strict_today_timezone": str(defaults.get("strict_today_timezone", "Asia/Shanghai")).strip() or "Asia/Shanghai",
        "fast_pass_enabled": bool(defaults.get("fast_pass_enabled", True)),
        "fast_pass_window_hours": _parse_int(defaults.get("fast_pass_window_hours", 48), 48),
        "fast_pass_title_keywords": fast_pass_title_keywords,
        "fast_pass_require_company_or_context": bool(defaults.get("fast_pass_require_company_or_context", True)),
    }


def _is_recent(ts: str, window_days: int) -> bool:
    if not str(ts).strip():
        return False
    dt = parse_datetime(ts)
    cutoff = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=window_days)
    return dt >= cutoff


def _is_recent_hours(ts: str, window_hours: int) -> bool:
    if not str(ts).strip():
        return False
    dt = parse_datetime(ts)
    cutoff = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(hours=window_hours)
    return dt >= cutoff


def _is_same_day_in_tz(ts: str, run_date: str, tz_name: str) -> bool:
    if not str(ts).strip() or not str(run_date).strip():
        return False
    dt_utc = parse_datetime(ts)
    tz = _resolve_timezone(tz_name)
    try:
        return dt_utc.astimezone(tz).date().isoformat() == run_date
    except Exception:
        return False


def _keyword_hits(text: str, words: list[str]) -> list[str]:
    hits = []
    for word in words:
        if word and word in text:
            hits.append(word)
    return sorted(set(hits))


def _check_hard_constraints(
    row: dict[str, Any],
    source: dict[str, Any],
    cfg_defaults: dict[str, Any],
    run_date: str,
) -> tuple[bool, str, dict[str, Any]]:
    link = str(row.get("link", "")).strip()
    profile = _source_profile(source)

    norm_url = normalize_url(link)
    if not norm_url:
        return False, "url_invalid", {"profile": profile}

    path = (urlparse(norm_url).path or "").lower()
    if not path or path == "/":
        return False, "url_homepage", {"profile": profile}

    allow_patterns = [str(x).lower() for x in source.get("url_allow_patterns", []) if str(x).strip()]
    block_patterns = [str(x).lower() for x in source.get("url_block_patterns", []) if str(x).strip()]
    if block_patterns and any(p in path for p in block_patterns):
        return False, "url_blocked_pattern", {"profile": profile}
    if allow_patterns and not any(p in path for p in allow_patterns):
        return False, "url_not_in_allow_patterns", {"profile": profile}

    published = str(row.get("published_at_utc", "")).strip()
    published_missing = bool(row.get("published_missing", False)) or not published
    strict_today_mode = bool(cfg_defaults.get("strict_today_mode", False))
    if published_missing and (strict_today_mode or profile not in cfg_defaults["allow_missing_published_profiles"]):
        return False, "published_missing", {"profile": profile}
    if not published_missing:
        if strict_today_mode:
            if not _is_same_day_in_tz(published, run_date, cfg_defaults["strict_today_timezone"]):
                return False, "not_today", {"profile": profile}
        elif not _is_recent(published, cfg_defaults["window_days"]):
            return False, "time_window", {"profile": profile}

        source_max_age_hours = _parse_int(source.get("max_age_hours", 0), 0)
        if source_max_age_hours > 0 and not _is_recent_hours(published, source_max_age_hours):
            return False, "source_max_age", {"profile": profile}

    return True, "", {"profile": profile, "normalized_url": norm_url}


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


def _is_fast_pass(
    row: dict[str, Any],
    signals: dict[str, Any],
    cfg_defaults: dict[str, Any],
) -> bool:
    if not cfg_defaults["fast_pass_enabled"]:
        return False
    if not signals["fast_pass_title_hits"]:
        return False

    published = str(row.get("published_at_utc", "")).strip()
    if not _is_recent_hours(published, cfg_defaults["fast_pass_window_hours"]):
        return False

    if cfg_defaults["fast_pass_require_company_or_context"]:
        has_company_signal = bool(signals["company_hits"] or signals["brand_hits"])
        has_context_signal = bool(signals["context_hits"])
        if not (has_company_signal or has_context_signal):
            return False
    return True


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Filter canonical items for Robtaxi relevance")
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD; default Beijing date")
    parser.add_argument("--in", dest="in_root", default="./artifacts/canonical", help="Canonical input root")
    parser.add_argument("--out", default="./artifacts/filtered", help="Filtered output root")
    parser.add_argument("--sources", default="./sources.json", help="Path to sources config")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root")
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "canonical_items.jsonl"
    out_root = Path(args.out).expanduser().resolve() / date_text
    keep_file = out_root / "filtered_items.jsonl"
    drop_file = out_root / "dropped_items.jsonl"
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)

    cfg = read_json(Path(args.sources).expanduser().resolve())
    source_map = {}
    for src in cfg.get("sources", []):
        if isinstance(src, dict):
            source_map[str(src.get("id", "")).strip()] = src

    settings = _defaults(cfg)
    company_aliases = _build_company_aliases(cfg)
    rows = read_jsonl(in_file)
    rows = sorted(rows, key=lambda x: str(x.get("published_at_utc", "")), reverse=True)

    kept: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    drop_reasons: Counter[str] = Counter()
    kept_by_source: defaultdict[str, int] = defaultdict(int)
    general_media_kept: defaultdict[str, int] = defaultdict(int)

    fast_pass_kept_count = 0
    fast_pass_drop_count = 0
    stage2_scored_count = 0
    stage2_kept_count = 0

    for row in rows:
        sid = str(row.get("source_id", "")).strip()
        source = source_map.get(sid, {"source_type": "rss", "category": "media"})

        hard_ok, hard_reason, hard_detail = _check_hard_constraints(row, source, settings, date_text)
        profile = str(hard_detail.get("profile", "general_media"))

        signals = {
            "core_hits": [],
            "core_title_hits": [],
            "context_hits": [],
            "brand_hits": [],
            "company_hits": [],
            "semantic_hits": [],
            "negative_hits": [],
            "context_terms_hit": [],
            "level_hits": [],
            "truck_hits": [],
            "fast_pass_title_hits": [],
            "candidate_signals": [],
        }

        if hard_ok:
            signals = _collect_signals(row, source, settings, company_aliases)

        is_keep = False
        score = 0
        reason = hard_reason
        stage = "hard_drop"
        detail: dict[str, Any] = dict(hard_detail)

        if hard_ok:
            stage = "stage2"

            if settings["fast_pass_enabled"] and signals["fast_pass_title_hits"]:
                if _is_fast_pass(row, signals, settings):
                    is_keep = True
                    score = 100
                    reason = "fast_pass"
                    stage = "fast_pass"
                    fast_pass_kept_count += 1
                else:
                    fast_pass_drop_count += 1

            if not is_keep:
                if not signals["candidate_signals"]:
                    reason = "candidate_gate_miss"
                else:
                    stage2_scored_count += 1
                    is_keep, score, reason, detail = _score_stage2(row, source, settings, signals)
                    stage = "stage2"
                    if is_keep:
                        stage2_kept_count += 1

            if is_keep and settings["enable_general_media_source_cap"] and profile == "general_media":
                cap = settings["max_general_media_items_per_source"]
                if general_media_kept[sid] >= cap:
                    is_keep = False
                    reason = "general_source_cap"
                else:
                    general_media_kept[sid] += 1

        target = dict(row)
        target["relevance_stage"] = stage
        target["relevance_score"] = score
        target["relevance_profile"] = profile
        target["relevance_reason"] = reason
        target["relevance_reason_zh"] = reason_zh(reason)
        target["matched_core_keywords"] = signals["core_hits"]
        target["matched_context_keywords"] = signals["context_hits"]
        target["matched_brand_keywords"] = signals["brand_hits"]
        target["matched_company_aliases"] = signals["company_hits"]
        target["matched_fast_pass_title_keywords"] = signals["fast_pass_title_hits"]
        target["relevance_score_breakdown"] = detail.get("score_breakdown", {})
        target["drop_reason"] = reason if not is_keep else ""
        target["drop_reason_zh"] = reason_zh(reason) if not is_keep else ""
        target["relevance_detail"] = detail

        if is_keep:
            kept.append(target)
            kept_by_source[sid] += 1
        else:
            dropped.append(target)
            drop_reasons[reason] += 1

    write_jsonl(keep_file, kept)
    write_jsonl(drop_file, dropped)

    total_in = len(rows)
    total_kept = len(kept)
    total_dropped = len(dropped)
    pass_rate = round((total_kept / total_in) * 100.0, 2) if total_in else 0.0
    stage_status = "success" if total_kept > 0 else "partial"

    drop_reasons_zh: dict[str, int] = {}
    for reason_code, count in drop_reasons.items():
        label = reason_zh(reason_code)
        drop_reasons_zh[label] = drop_reasons_zh.get(label, 0) + count

    mark_stage(report_file, "filter", stage_status)
    patch_report(
        report_file,
        relevance_total_in=total_in,
        relevance_kept=total_kept,
        relevance_dropped=total_dropped,
        relevance_drop_by_reason=dict(drop_reasons),
        relevance_drop_by_reason_zh=drop_reasons_zh,
        published_missing_drop_count=int(drop_reasons.get("published_missing", 0)),
        not_today_drop_count=int(drop_reasons.get("not_today", 0)),
        source_max_age_drop_count=int(drop_reasons.get("source_max_age", 0)),
        candidate_gate_drop_count=int(drop_reasons.get("candidate_gate_miss", 0)),
        fast_pass_kept_count=fast_pass_kept_count,
        fast_pass_drop_count=fast_pass_drop_count,
        stage2_scored_count=stage2_scored_count,
        stage2_kept_count=stage2_kept_count,
        relevance_kept_by_source=dict(kept_by_source),
        relevance_precision_mode=settings["relevance_mode"],
        relevance_pass_rate=pass_rate,
        filtered_output=str(keep_file),
        dropped_output=str(drop_file),
    )

    print(
        f"[filter] date={date_text} in={total_in} kept={total_kept} dropped={total_dropped} "
        f"pass_rate={pass_rate}% mode={settings['relevance_mode']} strict_today={settings['strict_today_mode']} "
        f"fast_pass_kept={fast_pass_kept_count}"
    )
    print(f"[filter] output={keep_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
