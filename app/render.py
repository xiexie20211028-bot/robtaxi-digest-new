from __future__ import annotations

import argparse
import html
from datetime import timezone
from pathlib import Path
from typing import Any

from .common import now_beijing, parse_datetime, read_json, read_jsonl
from .report import load_or_init, mark_stage, patch_report, report_path


def render_item(item: dict[str, Any]) -> str:
    published = parse_datetime(str(item.get("published_at_utc", ""))).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    tags = ", ".join(str(t) for t in item.get("tags", []))
    title = html.escape(str(item.get("title_zh", "")))
    summary = html.escape(str(item.get("summary_zh", "")))
    source_name = html.escape(str(item.get("source_name", "")))
    link = html.escape(str(item.get("link", "")))
    tag_html = f"<span class='tags'>{html.escape(tags)}</span>" if tags else ""

    return (
        "<li>"
        f"<a href=\"{link}\" target=\"_blank\" rel=\"noopener noreferrer\">{title}</a>"
        f"<p class='summary'>{summary}</p>"
        f"<small>来源: {source_name} | 时间: {published} {tag_html}</small>"
        "</li>"
    )


def render_section(title: str, items: list[dict[str, Any]]) -> str:
    if not items:
        return f"<section><h2>{title}</h2><p>今日暂无可用新闻。</p></section>"
    rows = "\n".join(render_item(item) for item in items)
    return f"<section><h2>{title}</h2><ol>{rows}</ol></section>"


def build_html(date_text: str, domestic: list[dict[str, Any]], foreign: list[dict[str, Any]], report: dict[str, Any]) -> str:
    generated = now_beijing().strftime("%Y-%m-%d %H:%M:%S")
    source_stats = report.get("source_stats", [])
    source_stats = source_stats if isinstance(source_stats, list) else []

    ok_sources = [s for s in source_stats if str(s.get("status", "")) == "ok" and int(s.get("fetched_items", 0)) > 0]
    failed_sources = [s for s in source_stats if str(s.get("status", "")) != "ok"]
    failed_items = "".join(
        f"<li>{html.escape(str(s.get('source_name', '')))} ({html.escape(str(s.get('source_id', '')))}): {html.escape(str(s.get('error', '')))}</li>"
        for s in failed_sources
    ) or "<li>无</li>"

    stage_status = report.get("stage_status", {})
    summarize_fail = int(report.get("summarize_fail_count", 0))
    dedupe_drop = int(report.get("dedupe_drop_count", 0))

    return f"""<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
  <title>Robtaxi 行业简报 {date_text}</title>
  <style>
    :root {{
      --bg-a: #f5f7fb;
      --bg-b: #ffffff;
      --ink: #1f2937;
      --sub: #4b5563;
      --accent: #0f766e;
      --line: #dbe4ea;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: "PingFang SC", "Noto Sans SC", sans-serif; color: var(--ink); background: linear-gradient(180deg, var(--bg-a), var(--bg-b)); }}
    .wrap {{ max-width: 980px; margin: 0 auto; padding: 28px 16px 36px; }}
    h1 {{ margin: 0; font-size: 30px; letter-spacing: .3px; }}
    h2 {{ margin-top: 28px; font-size: 22px; border-left: 4px solid var(--accent); padding-left: 10px; }}
    .meta {{ color: var(--sub); margin: 10px 0 18px; }}
    .summary {{ margin: 8px 0; line-height: 1.6; color: #111827; }}
    li {{ margin: 0 0 18px 0; padding-bottom: 8px; border-bottom: 1px dashed var(--line); }}
    a {{ color: #0c4a6e; text-decoration: none; font-weight: 600; }}
    a:hover {{ text-decoration: underline; }}
    small {{ color: var(--sub); }}
    .tags {{ color: #0369a1; margin-left: 6px; }}
    .footer {{ margin-top: 24px; border-top: 1px solid var(--line); padding-top: 14px; color: var(--sub); }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <h1>Robtaxi 行业简报</h1>
    <div class=\"meta\">日期: {date_text} | 生成时间（北京时间）: {generated}</div>
    <div class=\"meta\">国内 {len(domestic)} 条 | 国外 {len(foreign)} 条</div>

    {render_section('【国内 Robtaxi 最新动态】', domestic)}
    {render_section('【国外 Robtaxi 最新动态】', foreign)}

    <div class=\"footer\">
      <p><strong>今日有效源数量:</strong> {len(ok_sources)} / {len(source_stats)}</p>
      <p><strong>去重丢弃条数:</strong> {dedupe_drop} | <strong>摘要降级次数:</strong> {summarize_fail}</p>
      <p><strong>阶段状态:</strong> fetch={html.escape(str(stage_status.get('fetch', '')))}, parse={html.escape(str(stage_status.get('parse', '')))}, summarize={html.escape(str(stage_status.get('summarize', '')))}, render={html.escape(str(stage_status.get('render', '')))}, notify={html.escape(str(stage_status.get('notify', '')))}</p>
      <p><strong>抓取失败源列表:</strong></p>
      <ul>{failed_items}</ul>
    </div>
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
