from __future__ import annotations

import argparse
import html
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .common import now_beijing, parse_datetime, read_json, read_jsonl, tokenize
from .report import load_or_init, mark_stage, patch_report, report_path


TOPIC_CATEGORIES = [
    ("商业运营", ["运营", "扩张"]),
    ("监管政策", ["监管"]),
    ("安全事件", ["安全"]),
    ("融资与资本", ["融资", "合作"]),
    ("技术与产品", ["产品"]),
]


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
    work = sorted(work, key=lambda x: -int(x.get("importance", 3)))
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
    tags = item.get("tags", [])
    first_tag = str(tags[0]).strip() if tags else ""
    for category_name, tag_keywords in TOPIC_CATEGORIES:
        if first_tag in tag_keywords:
            return category_name
    return "商业运营"


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

    return (
        f"<article class='news-card' data-company='{company_id}'{importance_attr}>"
        f"<a class='news-title' href=\"{link}\" target=\"_blank\" rel=\"noopener noreferrer\">{badge_html}{title}</a>"
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
    failed = [s for s in source_stats if str(s.get("status", "")) != "ok"]
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

    ok_sources = [s for s in source_stats if str(s.get("status", "")) == "ok" and int(s.get("fetched_items", 0)) > 0]
    compact_failed, detail_failed = summarize_failed_sources(source_stats)

    stage_status = report.get("stage_status", {})
    summarize_fail = int(report.get("summarize_fail_count", 0))
    dedupe_drop = int(report.get("dedupe_drop_count", 0))
    relevance_total = int(report.get("relevance_total_in", 0))
    relevance_kept = int(report.get("relevance_kept", 0))
    relevance_dropped = int(report.get("relevance_dropped", 0))
    relevance_pass_rate = float(report.get("relevance_pass_rate", 0.0))
    top_drop_reasons, total_drop_reason = reason_top3_zh(report)

    compact_failed_html = "".join(
        f"<li><span>{html.escape(row['name'])}</span><span>{html.escape(row['reason'])}</span></li>" for row in compact_failed
    ) or "<li><span>无</span><span>-</span></li>"

    detail_failed_html = "".join(
        f"<li><strong>{html.escape(row['name'])}</strong><div>{html.escape(row['detail'])}</div></li>" for row in detail_failed
    ) or "<li>无详细错误</li>"

    top_drop_html = "".join(
        f"<li><span>{html.escape(name)}</span><span>{count} 条（{ratio:.1f}%）</span></li>" for name, count, ratio in top_drop_reasons
    ) or "<li><span>暂无剔除原因</span><span>0 条（0.0%）</span></li>"

    source_health_rows = "".join(
        (
            "<tr>"
            f"<td>{html.escape(str(s.get('source_name', '')))}</td>"
            f"<td>{html.escape(str(s.get('source_type', '')))}</td>"
            f"<td>{int(s.get('fetched_items', 0))}</td>"
            f"<td>{'正常' if str(s.get('status', '')) == 'ok' else '失败'}</td>"
            "</tr>"
        )
        for s in sorted(source_stats, key=lambda x: int(x.get("fetched_items", 0)), reverse=True)[:source_health_top_n]
    ) or "<tr><td colspan='4'>暂无数据</td></tr>"

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
        "__TOP_DROP_REASONS__": top_drop_html,
        "__TOTAL_DROP_REASON__": str(total_drop_reason),
        "__FAILED_SOURCES__": compact_failed_html,
        "__FAILED_SOURCES_DETAIL__": detail_failed_html,
        "__SOURCE_HEALTH_COUNTS__": f"{len(ok_sources)} / {len(source_stats)}",
        "__SOURCE_HEALTH_ROWS__": source_health_rows,
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
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "brief_items.jsonl"
    out_file = Path(args.out).expanduser().resolve()
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)

    brief_items = read_jsonl(in_file)
    cfg = read_json(Path(args.sources).expanduser().resolve())
    defaults = cfg.get("defaults", {}) if isinstance(cfg, dict) else {}
    top_n = int(defaults.get("top_n", 12))
    source_health_top_n = int(defaults.get("source_health_top_n", 20))

    brief_items = sorted(brief_items, key=lambda x: str(x.get("published_at_utc", "")), reverse=True)
    pool = brief_items[: top_n * 3]

    companies = cfg.get("companies", []) if isinstance(cfg, dict) else []
    alias_to_id, valid_ids, sorted_aliases = _build_company_lookup(companies)
    for item in pool:
        item["company_id"] = _infer_company_id(item, alias_to_id, valid_ids, sorted_aliases)
        item["region"] = _infer_event_region(item)

    all_items = _dedupe_by_title(pool)[: top_n * 2]

    domestic_count = len([x for x in all_items if str(x.get("region", "")).lower() == "domestic"])
    foreign_count = len([x for x in all_items if str(x.get("region", "")).lower() == "foreign"])

    report = load_or_init(report_file)
    html_text = build_html(date_text, all_items, report, cfg=cfg, source_health_top_n=source_health_top_n)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(html_text, encoding="utf-8")

    mark_stage(report_file, "render", "success")
    patch_report(
        report_file,
        html_output=str(out_file),
        domestic_count=domestic_count,
        foreign_count=foreign_count,
    )

    print(f"[render] date={date_text} total={len(all_items)} domestic={domestic_count} foreign={foreign_count}")
    print(f"[render] output={out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
