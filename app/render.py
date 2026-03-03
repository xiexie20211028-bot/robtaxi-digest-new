from __future__ import annotations

import argparse
import html
from datetime import timezone
from pathlib import Path
from typing import Any

from .common import now_beijing, parse_datetime, read_json, read_jsonl
from .report import load_or_init, mark_stage, patch_report, report_path


def render_item_card(item: dict[str, Any]) -> str:
    published = parse_datetime(str(item.get("published_at_utc", ""))).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    title = html.escape(str(item.get("title_zh", "")))
    summary_what = str(item.get("summary_what", "")).strip()
    summary_why = str(item.get("summary_why", "")).strip()
    summary_so_what = str(item.get("summary_so_what", "")).strip()
    legacy_summary = html.escape(str(item.get("summary_zh", "")))
    source_name = html.escape(str(item.get("source_name", "")))
    link = html.escape(str(item.get("link", "")))
    tags = [html.escape(str(tag)) for tag in item.get("tags", []) if str(tag).strip()]
    impact_targets = [html.escape(str(t)) for t in item.get("impact_targets", []) if str(t).strip()]
    tags_html = "".join(f"<span class='chip'>{tag}</span>" for tag in tags)
    impact_html = "".join(f"<span class='chip chip-impact'>{t}</span>" for t in impact_targets)
    impact_line_html = impact_html if impact_html else "<span class='impact-text'>未标注</span>"

    if summary_what and summary_why and summary_so_what:
        summary_html = (
            "<div class='summary-structured'>"
            f"<p><strong>What：</strong>{html.escape(summary_what)}</p>"
            f"<p><strong>Why：</strong>{html.escape(summary_why)}</p>"
            f"<p><strong>So what：</strong>{html.escape(summary_so_what)}</p>"
            "</div>"
        )
    else:
        summary_html = f"<p class='news-summary'>{legacy_summary}</p>"

    return (
        "<article class='news-card'>"
        f"<a class='news-title' href=\"{link}\" target=\"_blank\" rel=\"noopener noreferrer\">{title}</a>"
        f"{summary_html}"
        f"<div class='news-meta'><span>来源：{source_name}</span><span>时间：{published}</span></div>"
        f"<div class='impact-row'><span class='impact-label'>影响对象：</span>{impact_line_html}</div>"
        f"<div class='chip-row'>{tags_html}</div>"
        "</article>"
    )


def render_news_section(title: str, items: list[dict[str, Any]]) -> str:
    if not items:
        return (
            "<section class='section'>"
            f"<h2>{html.escape(title)}</h2>"
            "<div class='empty'>该窗口无符合规则的公开新闻</div>"
            "</section>"
        )

    cards = "\n".join(render_item_card(item) for item in items)
    return (
        "<section class='section'>"
        f"<h2>{html.escape(title)}</h2>"
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


def build_html(date_text: str, domestic: list[dict[str, Any]], foreign: list[dict[str, Any]], report: dict[str, Any], source_health_top_n: int = 20) -> str:
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

    slots = {
        "__TITLE_DATE__": html.escape(date_text),
        "__STAT_DATE__": html.escape(stat_date),
        "__WINDOW_START__": html.escape(window_start_bj or "-"),
        "__WINDOW_END__": html.escape(window_end_bj or "-"),
        "__WINDOW_MODE__": html.escape(window_mode),
        "__GENERATED_AT__": html.escape(generated),
        "__STAGE_STATUS__": stage_status_text,
        "__KPI_TOTAL__": str(relevance_total),
        "__KPI_KEPT__": str(relevance_kept),
        "__KPI_DROPPED__": str(relevance_dropped),
        "__KPI_PASS_RATE__": f"{relevance_pass_rate:.2f}%",
        "__KPI_DEDUPE__": str(dedupe_drop),
        "__KPI_FAIL__": str(summarize_fail),
        "__SECTION_DOMESTIC__": render_news_section("国内动态", domestic),
        "__SECTION_FOREIGN__": render_news_section("国外动态", foreign),
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
    domestic = [x for x in brief_items if str(x.get("region", "")).lower() == "domestic"][:top_n]
    foreign = [x for x in brief_items if str(x.get("region", "")).lower() == "foreign"][:top_n]

    report = load_or_init(report_file)
    html_text = build_html(date_text, domestic, foreign, report, source_health_top_n=source_health_top_n)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(html_text, encoding="utf-8")

    mark_stage(report_file, "render", "success")
    patch_report(
        report_file,
        html_output=str(out_file),
        domestic_count=len(domestic),
        foreign_count=len(foreign),
    )

    print(f"[render] date={date_text} domestic={len(domestic)} foreign={len(foreign)}")
    print(f"[render] output={out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
