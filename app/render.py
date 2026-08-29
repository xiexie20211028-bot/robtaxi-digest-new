from __future__ import annotations

import argparse
import html
import math
from collections import Counter
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .common import now_beijing, parse_datetime, read_jsonl, tokenize
from .report import METHOD_LABELS, METHOD_ORDER, empty_method_breakdown, empty_stage_funnel, load_or_init, mark_stage, patch_report, report_path
from .quality_metrics import current_quality_metrics, update_quality_metrics_history
from .source_config import PROFILE_NAMES, load_source_config
from .source_health import SEVERITY_ORDER, normalize_health_status


TOPIC_CATEGORIES = [
    ("Robotaxi", ["robotaxi"]),
    ("L3乘用车", ["passenger_l3"]),
    ("L4乘用车", ["passenger_l4"]),
    ("核心供应链", ["core_supply_chain"]),
    ("监管安全", ["regulation_safety"]),
    ("国家立法", ["industry_wide_regulation"]),
]

EVIDENCE_RANK = {
    "regulator": 7,
    "dataset": 7,
    "filing": 6,
    "company_newsroom": 5,
    "industry_media": 4,
    "social_post": 3,
    "general_media": 2,
}


FOREIGN_LOCATION_KEYWORDS = [
    "阿联酋", "迪拜", "多哈", "卡塔尔", "沙特",
    "韩国", "首尔", "日本", "东京",
    "美国", "欧洲", "英国", "德国",
    "奥斯汀", "旧金山", "洛杉矶", "凤凰城", "休斯顿", "纽约",
    "硅谷", "加州", "亚利桑那", "得克萨斯",
    "新加坡", "以色列",
]


def _build_company_lookup(companies: list[dict[str, Any]]) -> tuple[dict[str, str], set[str], list[str]]:
    alias_to_id: dict[str, str] = {}
    valid_ids: set[str] = set()
    for c in companies:
        if not isinstance(c, dict):
            continue
        cid = str(c.get("id", ""))
        if not cid:
            continue
        valid_ids.add(cid)
        alias_to_id[cid.lower()] = cid
        name = str(c.get("name", "")).strip().lower()
        if name:
            alias_to_id[name] = cid
        for alias in c.get("aliases", []):
            a = str(alias).strip().lower()
            if a:
                alias_to_id[a] = cid
    sorted_aliases = sorted((a for a in alias_to_id if len(a) >= 2), key=len, reverse=True)
    return alias_to_id, valid_ids, sorted_aliases


def _infer_company_id(item: dict[str, Any], alias_to_id: dict[str, str], valid_ids: set[str], sorted_aliases: list[str]) -> str:
    current = str(item.get("company_id", "")).strip()
    if current in valid_ids:
        return current
    if current and current != "other":
        cl = current.lower().strip()
        normalized = alias_to_id.get(cl)
        if normalized:
            return normalized
        for alias in sorted_aliases:
            if alias in cl or cl in alias:
                return alias_to_id[alias]
    title = str(item.get("title_zh", "")).lower()
    for alias in sorted_aliases:
        if alias in title:
            return alias_to_id[alias]
    return "other"


def _infer_event_region(item: dict[str, Any]) -> str:
    region = str(item.get("region", "foreign")).lower()
    if region != "domestic":
        return region
    title = str(item.get("title_zh", ""))
    for kw in FOREIGN_LOCATION_KEYWORDS:
        if kw in title:
            return "foreign"
    return region


def _dedupe_by_title(items: list[dict[str, Any]], threshold: float = 0.45) -> list[dict[str, Any]]:
    if len(items) <= 1:
        return items
    work = sorted(items, key=lambda x: str(x.get("published_at_utc", "")), reverse=True)
    work = sorted(
        work,
        key=lambda x: (
            -int(x.get("importance", 3)),
            -EVIDENCE_RANK.get(str(x.get("evidence_type", "general_media")), 0),
        ),
    )
    kept: list[dict[str, Any]] = []
    kept_token_sets: list[set[str]] = []
    for item in work:
        title = str(item.get("title_zh", ""))
        tokens = set(tokenize(title))
        if not tokens:
            kept.append(item)
            kept_token_sets.append(tokens)
            continue
        is_dup = False
        for prev_tokens in kept_token_sets:
            if not prev_tokens:
                continue
            intersection = len(tokens & prev_tokens)
            union = len(tokens | prev_tokens)
            if union > 0 and intersection / union >= threshold:
                is_dup = True
                break
        if not is_dup:
            kept.append(item)
            kept_token_sets.append(tokens)
    return kept


def _classify_topic(item: dict[str, Any]) -> str:
    domains = {str(value).strip() for value in item.get("coverage_domains", []) if str(value).strip()}
    # 监管与安全事件优先独立展示，其次是绑定项目的供应链动态。
    priority = ["industry_wide_regulation", "regulation_safety", "core_supply_chain", "robotaxi", "passenger_l3", "passenger_l4"]
    first_tag = next((value for value in priority if value in domains), "")
    for category_name, tag_keywords in TOPIC_CATEGORIES:
        if first_tag in tag_keywords:
            return category_name
    return "Robotaxi"


def select_digest_items(items: list[dict[str, Any]], defaults: dict[str, Any]) -> list[dict[str, Any]]:
    """按证据、去重、公司/信源限额和地区软均衡选出最终简报。"""
    top_n = max(1, int(defaults.get("top_n", 12)))
    company_cap = max(1, int(defaults.get("per_company_cap", 2)))
    source_cap = max(1, int(defaults.get("per_source_cap", 2)))
    discovery_cap = max(0, math.floor(top_n * float(defaults.get("discovery_direct_share_cap", 0.25))))
    region_cap = max(1, math.floor(top_n * float(defaults.get("region_soft_max_share", 0.60))))

    candidates = _dedupe_by_title(items)
    candidates = sorted(candidates, key=lambda row: str(row.get("published_at_utc", "")), reverse=True)
    candidates = sorted(
        candidates,
        key=lambda row: (
            int(row.get("importance", 3)),
            EVIDENCE_RANK.get(str(row.get("evidence_type", "general_media")), 0),
            int(row.get("relevance_score", 0)),
        ),
        reverse=True,
    )

    selected: list[dict[str, Any]] = []
    company_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    region_counts: Counter[str] = Counter()
    discovery_count = 0

    def can_add(item: dict[str, Any], enforce_region: bool) -> bool:
        nonlocal discovery_count
        company = str(item.get("company_id", "other"))
        source_id = str(item.get("source_id", ""))
        role = str(item.get("source_role", "secondary"))
        region = str(item.get("region", "foreign"))
        if company not in {"", "other"} and company_counts[company] >= company_cap:
            return False
        if source_id and source_counts[source_id] >= source_cap:
            return False
        if role in {"search_discovery", "social_discovery"} and discovery_count >= discovery_cap:
            return False
        if enforce_region and region_counts[region] >= region_cap:
            other_region = "domestic" if region == "foreign" else "foreign"
            if any(str(value.get("region", "foreign")) == other_region and value not in selected for value in candidates):
                return False
        return True

    def add(item: dict[str, Any]) -> None:
        nonlocal discovery_count
        selected.append(item)
        company = str(item.get("company_id", "other"))
        if company not in {"", "other"}:
            company_counts[company] += 1
        source_id = str(item.get("source_id", ""))
        if source_id:
            source_counts[source_id] += 1
        region_counts[str(item.get("region", "foreign"))] += 1
        if str(item.get("source_role", "secondary")) in {"search_discovery", "social_discovery"}:
            discovery_count += 1

    # 先保障已有高质量候选中的主题覆盖；无候选时不凑数。
    for domain in ("industry_wide_regulation", "robotaxi", "passenger_l3", "passenger_l4", "core_supply_chain", "regulation_safety"):
        match = next(
            (
                item
                for item in candidates
                if item not in selected and domain in item.get("coverage_domains", []) and can_add(item, True)
            ),
            None,
        )
        if match is not None and len(selected) < top_n:
            add(match)

    for enforce_region in (True, False):
        for item in candidates:
            if len(selected) >= top_n:
                break
            if item in selected or not can_add(item, enforce_region):
                continue
            add(item)
    return selected


def render_item_card(item: dict[str, Any]) -> str:
    published = parse_datetime(str(item.get("published_at_utc", ""))).astimezone(ZoneInfo("Asia/Shanghai")).strftime("%m-%d %H:%M")
    title = html.escape(str(item.get("title_zh", "")))
    summary_what = str(item.get("summary_what", "")).strip()
    summary_why = str(item.get("summary_why", "")).strip()
    summary_so_what = str(item.get("summary_so_what", "")).strip()
    legacy_summary = html.escape(str(item.get("summary_zh", "")))
    source_name = html.escape(str(item.get("source_name", "")))
    link = html.escape(str(item.get("link", "")))
    company_id = html.escape(str(item.get("company_id", "other")))
    region = str(item.get("region", "foreign")).lower()
    importance = int(item.get("importance", 3))

    if summary_what and summary_why and summary_so_what:
        merged = f"{html.escape(summary_what)} {html.escape(summary_why)} {html.escape(summary_so_what)}"
        summary_html = f"<p class='news-summary'>{merged}</p>"
    else:
        summary_html = f"<p class='news-summary'>{legacy_summary}</p>"

    impact_targets = [html.escape(str(t)) for t in item.get("impact_targets", []) if str(t).strip()]
    impact_html = "".join(f"<span class='chip chip-impact'>{t}</span>" for t in impact_targets)
    impact_line = (
        f"<div class='impact-row'><span class='impact-label'>影响对象：</span>{impact_html}</div>"
        if impact_html else ""
    )

    badge_cls = "badge-domestic" if region == "domestic" else "badge-foreign"
    badge_label = "国内" if region == "domestic" else "国外"
    badge_html = f"<span class='{badge_cls}'>[{badge_label}]</span> "

    importance_attr = " data-importance='high'" if importance >= 4 else ""
    late_badge = "<span class='badge-late'>[补录]</span> " if bool(item.get("late_arrival", False)) else ""

    return (
        f"<article class='news-card' data-company='{company_id}'{importance_attr}>"
        f"<a class='news-title' href=\"{link}\" target=\"_blank\" rel=\"noopener noreferrer\">{badge_html}{late_badge}{title}</a>"
        f"{summary_html}"
        f"<div class='news-meta'><span>来源：{source_name}</span><span>时间：{published}</span></div>"
        f"{impact_line}"
        "</article>"
    )


def render_topic_section(name: str, items: list[dict[str, Any]]) -> str:
    if not items:
        return ""

    # Sort by importance desc, then by published time desc (stable two-pass sort)
    sorted_items = sorted(items, key=lambda x: str(x.get("published_at_utc", "")), reverse=True)
    sorted_items = sorted(sorted_items, key=lambda x: -int(x.get("importance", 3)))

    cards = "\n".join(render_item_card(item) for item in sorted_items)
    return (
        f"<section class='topic-section' data-topic='{html.escape(name)}'>"
        f"<h2>{html.escape(name)}</h2>"
        f"<div class='card-grid'>{cards}</div>"
        "</section>"
    )


def summarize_failed_sources(source_stats: list[dict[str, Any]]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    failed = [s for s in source_stats if normalize_health_status(str(s.get("status", ""))) != "healthy"]
    compact_rows: list[dict[str, str]] = []
    detail_rows: list[dict[str, str]] = []
    for row in failed:
        name = str(row.get("source_name", "")).strip()
        sid = str(row.get("source_id", "")).strip()
        reason = str(row.get("error_reason_zh", "")).strip() or str(row.get("error", "")).strip() or "抓取异常"
        raw = str(row.get("error_raw", "")).strip()
        compact_rows.append({"name": name or sid, "reason": reason})
        if raw:
            detail_rows.append({"name": name or sid, "detail": raw[:500]})
    return compact_rows, detail_rows


def reason_top3_zh(report: dict[str, Any]) -> tuple[list[tuple[str, int, float]], int]:
    raw = report.get("relevance_drop_by_reason_zh", {})
    if not isinstance(raw, dict) or not raw:
        raw2 = report.get("relevance_drop_by_reason", {})
        if isinstance(raw2, dict):
            raw = {str(k): int(v) for k, v in raw2.items()}
        else:
            raw = {}

    total = sum(int(v) for v in raw.values())
    top = sorted(((str(k), int(v)) for k, v in raw.items()), key=lambda x: x[1], reverse=True)[:3]
    with_ratio = [(name, count, (count / total * 100.0) if total else 0.0) for name, count in top]
    return with_ratio, total


def _normalize_stage_funnel(report: dict[str, Any]) -> dict[str, dict[str, int]]:
    raw = report.get("stage_funnel", {})
    funnel = empty_stage_funnel()
    if isinstance(raw, dict):
        for method in funnel:
            current = raw.get(method, {})
            if isinstance(current, dict):
                funnel[method] = {
                    "fetched": int(current.get("fetched", 0)),
                    "candidate": int(current.get("candidate", 0)),
                    "filtered": int(current.get("filtered", 0)),
                    "kept": int(current.get("kept", 0)),
                }
    if not any(any(int(v) for v in counts.values()) for counts in funnel.values()):
        source_stats = report.get("source_stats", [])
        if isinstance(source_stats, list):
            for stat in source_stats:
                if not isinstance(stat, dict):
                    continue
                method = str(stat.get("source_type", "")).strip().lower()
                if method in funnel:
                    funnel[method]["fetched"] += int(stat.get("fetched_items", 0))
    return funnel


def _normalize_breakdown(report: dict[str, Any], field: str) -> dict[str, dict[str, int]]:
    raw = report.get(field, {})
    breakdown = empty_method_breakdown()
    if isinstance(raw, dict):
        for method in breakdown:
            current = raw.get(method, {})
            if isinstance(current, dict):
                breakdown[method] = {str(k): int(v) for k, v in current.items()}
    return breakdown


def _active_methods(
    funnel: dict[str, dict[str, int]],
    pre_candidate_breakdown: dict[str, dict[str, int]],
    candidate_filter_breakdown: dict[str, dict[str, int]],
) -> list[str]:
    active: list[str] = []
    for method in METHOD_ORDER:
        counts = funnel.get(method, {})
        if any(int(counts.get(key, 0)) > 0 for key in ("fetched", "candidate", "filtered", "kept")):
            active.append(method)
            continue
        if sum(int(v) for v in pre_candidate_breakdown.get(method, {}).values()) > 0:
            active.append(method)
            continue
        if sum(int(v) for v in candidate_filter_breakdown.get(method, {}).values()) > 0:
            active.append(method)
    return active or [method for method in METHOD_ORDER if method != "search_api"]


def _render_funnel_table(funnel: dict[str, dict[str, int]], methods: list[str]) -> str:
    rows = []
    for method in methods:
        counts = funnel.get(method, {})
        rows.append(
            "<tr>"
            f"<td>{html.escape(METHOD_LABELS.get(method, method))}</td>"
            f"<td>{int(counts.get('fetched', 0))}</td>"
            f"<td>{int(counts.get('candidate', 0))}</td>"
            f"<td>{int(counts.get('filtered', 0))}</td>"
            f"<td>{int(counts.get('kept', 0))}</td>"
            "</tr>"
        )
    return "".join(rows) or "<tr><td colspan='5'>暂无数据</td></tr>"


def _render_breakdown_table(
    breakdown: dict[str, dict[str, int]],
    methods: list[str],
    empty_text: str,
) -> str:
    reason_totals: dict[str, int] = {}
    for method in methods:
        for reason, count in breakdown.get(method, {}).items():
            reason_totals[reason] = reason_totals.get(reason, 0) + int(count)
    if not reason_totals:
        colspan = 2 + len(methods)
        return f"<tr><td colspan='{colspan}'>{html.escape(empty_text)}</td></tr>"

    rows = []
    for reason, total in sorted(reason_totals.items(), key=lambda x: x[1], reverse=True):
        cols = [f"<td>{html.escape(reason)}</td>", f"<td>{total}</td>"]
        for method in methods:
            cols.append(f"<td>{int(breakdown.get(method, {}).get(reason, 0))}</td>")
        rows.append("<tr>" + "".join(cols) + "</tr>")
    return "".join(rows)


def _render_quality_summary(report: dict[str, Any]) -> str:
    quality = report.get("quality_metrics", {}) if isinstance(report.get("quality_metrics", {}), dict) else {}
    rows: list[str] = []
    for key, label in (("rolling_7d", "滚动 7 天"), ("rolling_30d", "滚动 30 天")):
        metrics = quality.get(key, {}) if isinstance(quality.get(key, {}), dict) else {}
        if not metrics:
            continue
        coverage = metrics.get("coverage_distribution", {}) if isinstance(metrics.get("coverage_distribution", {}), dict) else {}
        coverage_text = " / ".join(f"{name}:{int(count)}" for name, count in sorted(coverage.items())) or "无"
        rows.append(
            "<tr>"
            f"<td>{label}</td>"
            f"<td>{float(metrics.get('primary_source_share', 0.0)) * 100:.1f}%</td>"
            f"<td>{float(metrics.get('discovery_dependency_share', 0.0)) * 100:.1f}%</td>"
            f"<td>{float(metrics.get('max_single_source_share', 0.0)) * 100:.1f}%</td>"
            f"<td>{int(metrics.get('silent_dead_sources', 0))}</td>"
            f"<td>{html.escape(coverage_text)}</td>"
            "</tr>"
        )
    return "".join(rows) or "<tr><td colspan='6'>历史尚不足，待影子运行积累</td></tr>"


_TEMPLATE_PATH = Path(__file__).parent / "digest_template.html"


def _load_template() -> str:
    return _TEMPLATE_PATH.read_text(encoding="utf-8")


def build_html(date_text: str, items: list[dict[str, Any]], report: dict[str, Any], cfg: dict[str, Any] | None = None, source_health_top_n: int = 20) -> str:
    generated = now_beijing().strftime("%Y-%m-%d %H:%M:%S")
    window_mode = str(report.get("window_mode", "prev_natural_day"))
    window_start_bj = str(report.get("window_start_bj", "")).strip()
    window_end_bj = str(report.get("window_end_bj", "")).strip()
    stat_date = window_start_bj.split(" ")[0] if window_start_bj else date_text
    source_stats = report.get("source_stats", [])
    source_stats = source_stats if isinstance(source_stats, list) else []

    ok_sources = [s for s in source_stats if normalize_health_status(str(s.get("status", ""))) == "healthy"]
    compact_failed, detail_failed = summarize_failed_sources(source_stats)

    stage_status = report.get("stage_status", {})
    summarize_fail = int(report.get("summarize_fail_count", 0))
    dedupe_drop = int(report.get("dedupe_drop_count", 0))
    relevance_total = int(report.get("relevance_total_in", 0))
    relevance_kept = int(report.get("relevance_kept", 0))
    relevance_dropped = int(report.get("relevance_dropped", 0))
    relevance_pass_rate = float(report.get("relevance_pass_rate", 0.0))
    top_drop_reasons, total_drop_reason = reason_top3_zh(report)
    stage_funnel = _normalize_stage_funnel(report)
    pre_candidate_breakdown = _normalize_breakdown(report, "pre_candidate_drop_breakdown")
    candidate_filter_breakdown = _normalize_breakdown(report, "candidate_filter_breakdown")
    active_methods = _active_methods(stage_funnel, pre_candidate_breakdown, candidate_filter_breakdown)
    funnel_table_html = _render_funnel_table(stage_funnel, active_methods)
    method_header_html = "".join(f"<th>{html.escape(METHOD_LABELS.get(method, method))}</th>" for method in active_methods)
    pre_candidate_table_html = _render_breakdown_table(
        pre_candidate_breakdown,
        active_methods,
        "暂无未进入候选池明细",
    )
    candidate_filter_table_html = _render_breakdown_table(
        candidate_filter_breakdown,
        active_methods,
        "暂无候选池过滤明细",
    )
    pre_candidate_total = int(report.get("pre_candidate_drop_total", 0))
    candidate_filter_total = sum(sum(int(v) for v in candidate_filter_breakdown.get(method, {}).values()) for method in active_methods)

    compact_failed_html = "".join(
        f"<li><span>{html.escape(row['name'])}</span><span>{html.escape(row['reason'])}</span></li>" for row in compact_failed
    ) or "<li><span>无</span><span>-</span></li>"

    detail_failed_html = "".join(
        f"<li><strong>{html.escape(row['name'])}</strong><div>{html.escape(row['detail'])}</div></li>" for row in detail_failed
    ) or "<li>无详细错误</li>"

    top_drop_html = "".join(
        f"<li><span>{html.escape(name)}</span><span>{count} 条（{ratio:.1f}%）</span></li>" for name, count, ratio in top_drop_reasons
    ) or "<li><span>暂无剔除原因</span><span>0 条（0.0%）</span></li>"

    required_stats = [s for s in source_stats if str(s.get("criticality", "")) == "required"]
    other_stats = [s for s in source_stats if str(s.get("criticality", "")) != "required"]
    health_display = required_stats + sorted(other_stats, key=lambda x: int(x.get("fetched_items", 0)), reverse=True)[:source_health_top_n]
    health_display = sorted(
        health_display,
        key=lambda x: (
            -SEVERITY_ORDER.get(normalize_health_status(str(x.get("status", ""))), 2),
            0 if str(x.get("criticality", "")) == "required" else 1,
            str(x.get("source_name", "")),
        ),
    )
    status_labels = {"healthy": "正常", "degraded": "退化", "failed": "失败", "silent_dead": "静默失效"}
    source_health_rows = "".join(
        (
            "<tr>"
            f"<td>{html.escape(str(s.get('source_name', '')))}</td>"
            f"<td>{html.escape(str(s.get('source_role', '')))}</td>"
            f"<td>{int(s.get('request_success_count', 0))}/{int(s.get('request_count', 0))}</td>"
            f"<td>{int(s.get('listed_items', s.get('fetched_items', 0)))}</td>"
            f"<td>{int(s.get('valid_items', 0))}</td>"
            f"<td>{float(s.get('date_parse_rate', 0.0)) * 100:.1f}%</td>"
            f"<td>{int(s.get('fresh_items', 0))}</td>"
            f"<td>{html.escape(str(s.get('newest_published_at', ''))[:16])}</td>"
            f"<td>{status_labels.get(normalize_health_status(str(s.get('status', ''))), '失败')}</td>"
            "</tr>"
        )
        for s in health_display
    ) or "<tr><td colspan='9'>暂无数据</td></tr>"
    quality_summary_rows = _render_quality_summary(report)
    agent_notice = str(report.get("domestic_agent_notice", "")).strip()
    agent_notice_html = (
        f"<section class='agent-notice'>{html.escape(agent_notice)}</section>" if agent_notice else ""
    )

    stage_status_text = (
        f"阶段状态：fetch={html.escape(str(stage_status.get('fetch', '')))} ｜"
        f" parse={html.escape(str(stage_status.get('parse', '')))} ｜"
        f" filter={html.escape(str(stage_status.get('filter', '')))} ｜"
        f" summarize={html.escape(str(stage_status.get('summarize', '')))} ｜"
        f" render={html.escape(str(stage_status.get('render', '')))} ｜"
        f" notify={html.escape(str(stage_status.get('notify', '')))}"
    )

    # Reader-friendly KPIs
    high_importance = [x for x in items if int(x.get("importance", 3)) >= 4]
    company_ids = set(str(x.get("company_id", "other")) for x in items)
    company_ids.discard("other")
    company_ids.discard("")
    domestic_items = [x for x in items if str(x.get("region", "")).lower() == "domestic"]
    foreign_items = [x for x in items if str(x.get("region", "")).lower() == "foreign"]

    # Group items by topic
    topic_groups: dict[str, list[dict[str, Any]]] = {}
    for cat_name, _ in TOPIC_CATEGORIES:
        topic_groups[cat_name] = []
    for item in items:
        topic = _classify_topic(item)
        topic_groups.setdefault(topic, []).append(item)

    topics_html_parts: list[str] = []
    for cat_name, _ in TOPIC_CATEGORIES:
        section_html = render_topic_section(cat_name, topic_groups.get(cat_name, []))
        if section_html:
            topics_html_parts.append(section_html)

    topics_html = "\n".join(topics_html_parts) if topics_html_parts else "<div class='empty'>今日无符合规则的公开新闻</div>"

    # Company filter buttons
    company_map: dict[str, str] = {}
    if cfg:
        for c in cfg.get("companies", []):
            if isinstance(c, dict):
                company_map[str(c.get("id", ""))] = str(c.get("name", ""))

    filter_chips: list[str] = ["<button class='filter-chip active' data-filter='all'>全部</button>"]
    for cid in sorted(company_ids):
        display_name = html.escape(company_map.get(cid, cid))
        filter_chips.append(f"<button class='filter-chip' data-filter='{html.escape(cid)}'>{display_name}</button>")
    company_filters_html = "".join(filter_chips)

    slots = {
        "__TITLE_DATE__": html.escape(date_text),
        "__STAT_DATE__": html.escape(stat_date),
        "__WINDOW_START__": html.escape(window_start_bj or "-"),
        "__WINDOW_END__": html.escape(window_end_bj or "-"),
        "__GENERATED_AT__": html.escape(generated),
        "__STAGE_STATUS__": stage_status_text,
        "__AGENT_NOTICE__": agent_notice_html,
        # Reader KPIs
        "__KPI_HEADLINE__": str(len(high_importance)),
        "__KPI_COMPANIES__": str(len(company_ids)),
        "__KPI_DOMESTIC__": str(len(domestic_items)),
        "__KPI_FOREIGN__": str(len(foreign_items)),
        # Topic sections
        "__SECTION_TOPICS__": topics_html,
        # Company filters
        "__COMPANY_FILTERS__": company_filters_html,
        # Ops details (folded)
        "__KPI_TOTAL__": str(relevance_total),
        "__KPI_KEPT__": str(relevance_kept),
        "__KPI_DROPPED__": str(relevance_dropped),
        "__KPI_PASS_RATE__": f"{relevance_pass_rate:.2f}%",
        "__KPI_DEDUPE__": str(dedupe_drop),
        "__KPI_FAIL__": str(summarize_fail),
        "__WINDOW_MODE__": html.escape(window_mode),
        "__METHOD_HEADER__": method_header_html,
        "__METHOD_FUNNEL_ROWS__": funnel_table_html,
        "__PRE_CANDIDATE_ROWS__": pre_candidate_table_html,
        "__PRE_CANDIDATE_TOTAL__": str(pre_candidate_total),
        "__CANDIDATE_FILTER_ROWS__": candidate_filter_table_html,
        "__CANDIDATE_FILTER_TOTAL__": str(candidate_filter_total),
        "__TOP_DROP_REASONS__": top_drop_html,
        "__TOTAL_DROP_REASON__": str(total_drop_reason),
        "__FAILED_SOURCES__": compact_failed_html,
        "__FAILED_SOURCES_DETAIL__": detail_failed_html,
        "__SOURCE_HEALTH_COUNTS__": f"{len(ok_sources)} / {len(source_stats)}",
        "__SOURCE_HEALTH_ROWS__": source_health_rows,
        "__QUALITY_SUMMARY_ROWS__": quality_summary_rows,
    }

    result = _load_template()
    for slot, value in slots.items():
        result = result.replace(slot, value)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Render HTML digest from brief jsonl")
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD; default Beijing date")
    parser.add_argument("--in", dest="in_root", default="./artifacts/brief", help="Brief input root")
    parser.add_argument("--out", default="./site/index.html", help="Output html path")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root")
    parser.add_argument("--sources", default="./sources.json", help="Sources config for defaults.top_n")
    parser.add_argument("--profile", choices=sorted(PROFILE_NAMES), default="", help="渲染 profile；默认读取 active_profile")
    parser.add_argument("--metrics-history", default="./.state/digest_metrics_history.json", help="7/30 天质量指标历史")
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "brief_items.jsonl"
    out_file = Path(args.out).expanduser().resolve()
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)

    brief_items = read_jsonl(in_file)
    cfg, active_profile = load_source_config(Path(args.sources).expanduser().resolve(), args.profile)
    defaults = cfg.get("defaults", {}) if isinstance(cfg, dict) else {}
    top_n = int(defaults.get("top_n", 12))
    source_health_top_n = int(defaults.get("source_health_top_n", 20))

    brief_items = sorted(brief_items, key=lambda x: str(x.get("published_at_utc", "")), reverse=True)
    pool = brief_items

    companies = cfg.get("companies", []) if isinstance(cfg, dict) else []
    alias_to_id, valid_ids, sorted_aliases = _build_company_lookup(companies)
    for item in pool:
        item["company_id"] = _infer_company_id(item, alias_to_id, valid_ids, sorted_aliases)
        item["region"] = _infer_event_region(item)

    all_items = select_digest_items(pool, defaults)

    domestic_count = len([x for x in all_items if str(x.get("region", "")).lower() == "domestic"])
    foreign_count = len([x for x in all_items if str(x.get("region", "")).lower() == "foreign"])

    report = load_or_init(report_file)
    source_stats = report.get("source_stats", []) if isinstance(report.get("source_stats", []), list) else []
    quality_metrics = update_quality_metrics_history(
        Path(args.metrics_history).expanduser().resolve(),
        date_text,
        current_quality_metrics(all_items, source_stats),
    )
    report["quality_metrics"] = quality_metrics
    html_text = build_html(date_text, all_items, report, cfg=cfg, source_health_top_n=source_health_top_n)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(html_text, encoding="utf-8")

    mark_stage(report_file, "render", "success")
    patch_report(
        report_file,
        html_output=str(out_file),
        domestic_count=domestic_count,
        foreign_count=foreign_count,
        active_profile=active_profile,
        selected_count=len(all_items),
        discovery_selected_count=len(
            [item for item in all_items if str(item.get("source_role", "")) in {"search_discovery", "social_discovery"}]
        ),
        quality_metrics=quality_metrics,
    )

    print(f"[render] date={date_text} total={len(all_items)} domestic={domestic_count} foreign={foreign_count}")
    print(f"[render] output={out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
