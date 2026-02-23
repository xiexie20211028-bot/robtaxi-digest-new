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
    summary = html.escape(str(item.get("summary_zh", "")))
    source_name = html.escape(str(item.get("source_name", "")))
    link = html.escape(str(item.get("link", "")))
    tags = [html.escape(str(tag)) for tag in item.get("tags", []) if str(tag).strip()]
    tags_html = "".join(f"<span class='chip'>{tag}</span>" for tag in tags)

    return (
        "<article class='news-card'>"
        f"<a class='news-title' href=\"{link}\" target=\"_blank\" rel=\"noopener noreferrer\">{title}</a>"
        f"<p class='news-summary'>{summary}</p>"
        f"<div class='news-meta'><span>来源：{source_name}</span><span>时间：{published}</span></div>"
        f"<div class='chip-row'>{tags_html}</div>"
        "</article>"
    )


def render_news_section(title: str, items: list[dict[str, Any]]) -> str:
    if not items:
        return (
            "<section class='section'>"
            f"<h2>{html.escape(title)}</h2>"
            "<div class='empty'>今日暂无可用新闻</div>"
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


def build_html(date_text: str, domestic: list[dict[str, Any]], foreign: list[dict[str, Any]], report: dict[str, Any]) -> str:
    generated = now_beijing().strftime("%Y-%m-%d %H:%M:%S")
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
        for s in sorted(source_stats, key=lambda x: int(x.get("fetched_items", 0)), reverse=True)[:12]
    ) or "<tr><td colspan='4'>暂无数据</td></tr>"

    return f"""<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
  <title>Robtaxi 行业简报 {date_text}</title>
  <style>
    :root {{
      --bg: #eef4f8;
      --panel: #ffffff;
      --ink: #12212f;
      --muted: #4a6073;
      --line: #d8e4ec;
      --primary: #136f63;
      --primary-soft: #d8f3ef;
      --secondary: #0f4c81;
      --chip: #ebf6ff;
      --danger-soft: #ffe9e9;
      --shadow: 0 10px 30px rgba(12, 34, 51, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: radial-gradient(circle at 5% -20%, #d5eef4 0, var(--bg) 42%), var(--bg); color: var(--ink); font-family: "Source Han Sans SC", "PingFang SC", "Microsoft YaHei", sans-serif; }}
    .wrap {{ max-width: 1260px; margin: 0 auto; padding: 24px 18px 40px; }}
    .hero {{ background: linear-gradient(125deg, #e4f6f2, #dceaf7); border: 1px solid var(--line); border-radius: 18px; padding: 20px 22px; box-shadow: var(--shadow); }}
    .hero h1 {{ margin: 0; font-size: 34px; letter-spacing: 0.3px; }}
    .meta {{ margin-top: 8px; color: var(--muted); font-size: 14px; }}

    .kpi-grid {{ display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 12px; margin-top: 16px; }}
    .kpi {{ background: var(--panel); border: 1px solid var(--line); border-radius: 14px; padding: 14px; box-shadow: var(--shadow); }}
    .kpi .label {{ color: var(--muted); font-size: 12px; }}
    .kpi .value {{ margin-top: 6px; font-size: 24px; font-weight: 700; color: var(--secondary); }}

    .layout {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 16px; }}
    .section {{ background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 14px; box-shadow: var(--shadow); }}
    h2 {{ margin: 0 0 12px 0; font-size: 20px; }}
    .card-grid {{ display: grid; gap: 10px; }}
    .news-card {{ border: 1px solid var(--line); border-radius: 12px; padding: 12px; background: #fbfeff; }}
    .news-title {{ color: #07355f; font-size: 16px; font-weight: 700; text-decoration: none; line-height: 1.4; }}
    .news-title:hover {{ text-decoration: underline; }}
    .news-summary {{ margin: 10px 0; color: #203242; line-height: 1.6; font-size: 14px; }}
    .news-meta {{ display: flex; justify-content: space-between; gap: 10px; color: var(--muted); font-size: 12px; }}
    .chip-row {{ margin-top: 8px; display: flex; flex-wrap: wrap; gap: 6px; }}
    .chip {{ background: var(--chip); color: #125b9c; font-size: 12px; border-radius: 999px; padding: 3px 8px; border: 1px solid #c9e4ff; }}
    .empty {{ color: var(--muted); padding: 12px 4px; }}

    .insight-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 16px; }}
    .mini-list {{ list-style: none; margin: 0; padding: 0; }}
    .mini-list li {{ display: grid; grid-template-columns: 1fr auto; gap: 12px; border-bottom: 1px dashed var(--line); padding: 9px 0; font-size: 14px; }}
    .mini-list li:last-child {{ border-bottom: 0; }}

    .danger-panel {{ background: #fff; border: 1px solid #f4d0d0; }}
    .danger-panel .mini-list li {{ background: linear-gradient(90deg, var(--danger-soft), #fff); border-radius: 8px; padding: 10px; margin-bottom: 6px; border-bottom: 0; }}

    details {{ margin-top: 10px; }}
    details summary {{ cursor: pointer; color: var(--muted); font-size: 13px; }}
    .detail-list {{ margin: 8px 0 0 0; padding-left: 18px; color: var(--muted); font-size: 12px; line-height: 1.5; }}

    .health-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    .health-table th, .health-table td {{ border-bottom: 1px solid var(--line); padding: 8px 6px; text-align: left; }}
    .health-table th {{ color: var(--muted); font-weight: 600; }}

    .footer {{ margin-top: 18px; color: var(--muted); font-size: 13px; }}

    @media (max-width: 1080px) {{
      .kpi-grid {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
      .layout, .insight-grid {{ grid-template-columns: 1fr; }}
    }}

    @media (max-width: 640px) {{
      .wrap {{ padding: 14px 12px 28px; }}
      .hero h1 {{ font-size: 26px; }}
      .kpi-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .news-meta {{ display: block; }}
    }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <section class=\"hero\">
      <h1>Robtaxi 行业简报</h1>
      <div class=\"meta\">日期：{date_text} ｜ 更新时间（北京时间）：{generated}</div>
      <div class=\"meta\">阶段状态：fetch={html.escape(str(stage_status.get('fetch', '')))} ｜ parse={html.escape(str(stage_status.get('parse', '')))} ｜ filter={html.escape(str(stage_status.get('filter', '')))} ｜ summarize={html.escape(str(stage_status.get('summarize', '')))} ｜ render={html.escape(str(stage_status.get('render', '')))} ｜ notify={html.escape(str(stage_status.get('notify', '')))}</div>
    </section>

    <section class=\"kpi-grid\">
      <div class=\"kpi\"><div class=\"label\">候选池总数</div><div class=\"value\">{relevance_total}</div></div>
      <div class=\"kpi\"><div class=\"label\">相关入选</div><div class=\"value\">{relevance_kept}</div></div>
      <div class=\"kpi\"><div class=\"label\">过滤剔除</div><div class=\"value\">{relevance_dropped}</div></div>
      <div class=\"kpi\"><div class=\"label\">候选池通过率</div><div class=\"value\">{relevance_pass_rate:.2f}%</div></div>
      <div class=\"kpi\"><div class=\"label\">去重丢弃</div><div class=\"value\">{dedupe_drop}</div></div>
      <div class=\"kpi\"><div class=\"label\">摘要降级次数</div><div class=\"value\">{summarize_fail}</div></div>
    </section>

    <section class=\"layout\">
      {render_news_section('国内动态', domestic)}
      {render_news_section('国外动态', foreign)}
    </section>

    <section class=\"insight-grid\">
      <section class=\"section\">
        <h2>过滤洞察</h2>
        <ul class=\"mini-list\">{top_drop_html}</ul>
        <div class=\"footer\">剔除原因样本总量：{total_drop_reason}</div>
      </section>

      <section class=\"section danger-panel\">
        <h2>失败源摘要</h2>
        <ul class=\"mini-list\">{compact_failed_html}</ul>
        <details>
          <summary>查看详情（技术排障）</summary>
          <ul class=\"detail-list\">{detail_failed_html}</ul>
        </details>
      </section>
    </section>

    <section class=\"section\" style=\"margin-top:16px;\">
      <h2>源健康概览</h2>
      <div class=\"meta\">今日有效源：{len(ok_sources)} / {len(source_stats)}</div>
      <table class=\"health-table\">
        <thead><tr><th>来源</th><th>类型</th><th>抓取条数</th><th>状态</th></tr></thead>
        <tbody>{source_health_rows}</tbody>
      </table>
    </section>
  </div>
</body>
</html>
"""


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

    brief_items = sorted(brief_items, key=lambda x: str(x.get("published_at_utc", "")), reverse=True)
    domestic = [x for x in brief_items if str(x.get("region", "")).lower() == "domestic"][:top_n]
    foreign = [x for x in brief_items if str(x.get("region", "")).lower() == "foreign"][:top_n]

    report = load_or_init(report_file)
    html_text = build_html(date_text, domestic, foreign, report)

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
