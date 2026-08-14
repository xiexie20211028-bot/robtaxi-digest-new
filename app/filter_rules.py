from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

from .common import normalize_url, now_beijing, parse_datetime


ALLOWED_SOURCE_PROFILES = {"general_media", "industry_media", "newsroom", "regulator", "research"}
KNOWN_COMPANY_ALIASES = [
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
    "momenta",
]

DROP_REASON_ZH = {
    "blocked_publisher": "命中屏蔽发布源",
    "query_rss_unresolved_url": "查询发现源真实链接未解析",
    "query_rss_unverified_published": "查询发现源发布时间未验证",
    "search_result_unverified_published": "搜索发现源发布时间未验证",
    "general_no_core_or_company": "通用媒体缺少核心词或公司信号",
    "score_below_threshold": "相关性评分低于阈值",
    "time_window": "超出时间窗口",
    "url_invalid": "链接无效",
    "url_homepage": "首页链接非文章",
    "url_not_in_allow_patterns": "链接不在允许路径",
    "url_blocked_pattern": "命中屏蔽路径",
    "url_external_domain_not_allowed": "外链域名不在白名单",
    "general_source_cap": "通用媒体单源条数超限",
    "pair_rule_mismatch": "关键词配对规则不满足",
    "published_missing": "发布时间缺失",
    "published_unparseable": "发布时间无法解析",
    "published_missing_or_unparseable": "发布时间缺失或无法解析",
    "not_today": "非当日新闻",
    "source_max_age": "超出来源时效窗口",
    "outside_window": "非统计窗口新闻",
    "late_arrival_cap": "补录条数已达上限",
    "late_arrival_low_quality": "补录候选证据或评分不足",
    "non_passenger_scope": "属于货运、配送、矿区、港口或其他非乘用车场景",
    "l2_marketing_only": "仅为 L2/L2+ 营销表述",
    "scope_gate_miss": "未满足 Robotaxi 或 L3/L4 乘用车范围门槛",
    "supply_chain_without_l3_l4_binding": "供应链事件未绑定明确 L3/L4 或 Robotaxi 项目",
    "social_official_account_unverified": "社交账号官方身份无法验证",
    "social_published_unverified": "社交内容发布时间无法验证",
    "social_permalink_missing": "社交内容缺少永久链接",
    "social_low_value_post": "社交内容为回复、转发、招聘或活动预热",
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
    aliases.update(KNOWN_COMPANY_ALIASES)
    return sorted(aliases)



def _parse_int(raw: Any, default: int) -> int:
    try:
        return int(raw)
    except Exception:
        return default



def _resolve_timezone(name: str) -> timezone:
    if ZoneInfo is None:
        if name == "Asia/Shanghai":
            return timezone(timedelta(hours=8))
        return timezone.utc
    try:
        return ZoneInfo(name)
    except Exception:
        if name == "Asia/Shanghai":
            return timezone(timedelta(hours=8))
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
    blocked_publishers_domestic = _normalize_keywords(defaults.get("blocked_publishers_domestic", []))
    blocked_publishers_foreign = _normalize_keywords(defaults.get("blocked_publishers_foreign", []))

    pair_rules = defaults.get("keyword_pair_rules", {})
    if not isinstance(pair_rules, dict):
        pair_rules = {}

    fast_pass_title_keywords_zh = _normalize_keywords(defaults.get("fast_pass_title_keywords_zh", [
        "robotaxi", "无人驾驶出租车", "自动驾驶出租车", "l4", "l3", "智能网联汽车", "无人驾驶汽车",
    ]))
    fast_pass_title_keywords_en = _normalize_keywords(defaults.get("fast_pass_title_keywords_en", [
        "robotaxi", "driverless taxi", "autonomous taxi", "self-driving taxi", "level 4", "level 3",
        "intelligent connected vehicle", "icv", "driverless car", "autonomous car", "self-driving car",
    ]))
    fast_pass_title_keywords = sorted(set(fast_pass_title_keywords_zh + fast_pass_title_keywords_en))

    return {
        "relevance_mode": mode,
        "window_days": _parse_int(defaults.get("window_days", 10), 10),
        "window_mode": str(defaults.get("window_mode", "prev_natural_day")).strip().lower() or "prev_natural_day",
        "window_timezone": str(defaults.get("window_timezone", "Asia/Shanghai")).strip() or "Asia/Shanghai",
        "thresholds": final_thresholds,
        "core_domestic": core_domestic,
        "core_foreign": core_foreign,
        "context_domestic": context_domestic,
        "context_foreign": context_foreign,
        "brand_domestic": brand_domestic,
        "brand_foreign": brand_foreign,
        "exclude_domestic": exclude_domestic,
        "exclude_foreign": exclude_foreign,
        "blocked_publishers_domestic": blocked_publishers_domestic,
        "blocked_publishers_foreign": blocked_publishers_foreign,
        "require_company_signal_for_general_media": bool(defaults.get("require_company_signal_for_general_media", True)),
        "max_general_media_items_per_source": _parse_int(defaults.get("max_general_media_items_per_source", 2), 2),
        "enable_general_media_source_cap": bool(defaults.get("enable_general_media_source_cap", False)),
        "pair_require_level_context": bool(pair_rules.get("require_level_with_autonomous_context", True)),
        "pair_require_truck_context": bool(pair_rules.get("require_truck_with_autonomous_context", True)),
        "drop_if_published_missing": bool(defaults.get("drop_if_published_missing", True)),
        "drop_if_published_unparseable": bool(defaults.get("drop_if_published_unparseable", True)),
        "fast_pass_enabled": bool(defaults.get("fast_pass_enabled", True)),
        "fast_pass_window_hours": _parse_int(defaults.get("fast_pass_window_hours", 48), 48),
        "fast_pass_title_keywords": fast_pass_title_keywords,
        "fast_pass_require_company_or_context": bool(defaults.get("fast_pass_require_company_or_context", True)),
        "scope_mode": str(defaults.get("scope_mode", "legacy")).strip().lower() or "legacy",
        "late_arrival_enabled": bool(defaults.get("late_arrival_enabled", False)),
        "late_arrival_hours": _parse_int(defaults.get("late_arrival_hours", 72), 72),
        "late_arrival_max_items": _parse_int(defaults.get("late_arrival_max_items", 2), 2),
        "late_arrival_min_score": _parse_int(defaults.get("late_arrival_min_score", 80), 80),
        "late_arrival_allowed_roles": [
            str(value).strip().lower()
            for value in defaults.get("late_arrival_allowed_roles", ["primary", "secondary"])
            if str(value).strip()
        ],
    }



def _is_recent(ts: str, window_days: int) -> bool:
    if not str(ts).strip():
        return False
    dt = parse_datetime(ts)
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    return dt >= cutoff



def _is_recent_hours(ts: str, window_hours: int) -> bool:
    if not str(ts).strip():
        return False
    dt = parse_datetime(ts)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)
    return dt >= cutoff



def _resolve_prev_natural_day_window(run_date: str, tz_name: str) -> tuple[datetime, datetime]:
    tz = _resolve_timezone(tz_name)
    if str(run_date).strip():
        try:
            run_day_local = datetime.fromisoformat(run_date).replace(tzinfo=tz)
        except Exception:
            run_day_local = now_beijing().astimezone(tz)
    else:
        run_day_local = now_beijing().astimezone(tz)

    end_local = datetime(run_day_local.year, run_day_local.month, run_day_local.day, 0, 0, 0, tzinfo=tz)
    start_local = end_local - timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)



def _in_time_window(ts: str, start_utc: datetime, end_utc: datetime) -> bool:
    if not str(ts).strip():
        return False
    dt_utc = parse_datetime(ts)
    return start_utc <= dt_utc < end_utc



def _check_hard_constraints(
    row: dict[str, Any],
    source: dict[str, Any],
    cfg_defaults: dict[str, Any],
    window_start_utc: datetime,
    window_end_utc: datetime,
) -> tuple[bool, str, dict[str, Any]]:
    link = str(row.get("link", "")).strip()
    profile = _source_profile(source)

    norm_url = normalize_url(link)
    if not norm_url:
        return False, "url_invalid", {"profile": profile}

    parsed_url = urlparse(norm_url)
    path = (parsed_url.path or "").lower()
    host = (parsed_url.netloc or "").lower()
    if not path or path == "/":
        return False, "url_homepage", {"profile": profile}

    entry_domains = []
    for entry in source.get("entry_urls", []):
        text = str(entry).strip()
        if not text:
            continue
        dom = (urlparse(text).netloc or "").lower()
        if dom:
            entry_domains.append(dom)

    external_allow_domains = [str(x).strip().lower() for x in source.get("external_link_allow_domains", []) if str(x).strip()]

    is_external = bool(entry_domains) and host and not any(host == d or host.endswith(f".{d}") for d in entry_domains)
    external_allowed = False
    if is_external:
        external_allowed = any(host == d or host.endswith(f".{d}") for d in external_allow_domains)
        if not external_allowed:
            return False, "url_external_domain_not_allowed", {"profile": profile}

    allow_patterns = [str(x).lower() for x in source.get("url_allow_patterns", []) if str(x).strip()]
    block_patterns = [str(x).lower() for x in source.get("url_block_patterns", []) if str(x).strip()]
    if not external_allowed:
        if block_patterns and any(p in path for p in block_patterns):
            return False, "url_blocked_pattern", {"profile": profile}
        if allow_patterns and not any(p in path for p in allow_patterns):
            return False, "url_not_in_allow_patterns", {"profile": profile}

    published = str(row.get("published_at_utc", "")).strip()
    published_missing = bool(row.get("published_missing", False)) or not published
    published_parse_status = str(row.get("published_parse_status", "")).strip().lower()
    source_type = str(source.get("source_type", "rss")).strip().lower()
    if source_type == "query_rss":
        resolved_ok = row.get("resolved_ok", True)
        if isinstance(resolved_ok, str):
            resolved_ok = resolved_ok.lower() == "true"
        if not resolved_ok:
            return False, "query_rss_unresolved_url", {"profile": profile}
    if source_type == "query_rss" and published_parse_status == "query_rss_unverified":
        return False, "query_rss_unverified_published", {"profile": profile}
    if source_type == "search_result" and published_parse_status == "search_result_unverified":
        return False, "search_result_unverified_published", {"profile": profile}
    if published_missing:
        return False, "published_missing_or_unparseable", {"profile": profile}
    if cfg_defaults["drop_if_published_unparseable"] and published_parse_status.startswith("unparseable"):
        return False, "published_missing_or_unparseable", {"profile": profile}
    if cfg_defaults["drop_if_published_missing"] and published_parse_status == "missing":
        return False, "published_missing_or_unparseable", {"profile": profile}

    late_arrival = False
    if not _in_time_window(published, window_start_utc, window_end_utc):
        published_dt = parse_datetime(published)
        first_seen = str(row.get("first_seen_at_utc", "")).strip()
        first_seen_dt = parse_datetime(first_seen) if first_seen else datetime(1970, 1, 1, tzinfo=timezone.utc)
        late_start = window_end_utc - timedelta(hours=max(1, cfg_defaults["late_arrival_hours"]))
        late_arrival = bool(
            cfg_defaults["late_arrival_enabled"]
            and late_start <= published_dt < window_start_utc
            and first_seen_dt >= window_end_utc
        )
        if not late_arrival:
            return False, "outside_window", {"profile": profile}

    if source_type == "query_rss":
        source_name = str(row.get("source_name", "")).strip().lower()
        blocked_publishers = (
            cfg_defaults["blocked_publishers_domestic"]
            if str(row.get("region", "foreign")).strip().lower() == "domestic"
            else cfg_defaults["blocked_publishers_foreign"]
        )
        if source_name and any(source_name == publisher or source_name.endswith(f".{publisher}") for publisher in blocked_publishers):
            return False, "blocked_publisher", {"profile": profile, "blocked_publisher": source_name}

    return True, "", {"profile": profile, "normalized_url": norm_url, "late_arrival": late_arrival}
