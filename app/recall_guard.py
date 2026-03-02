from __future__ import annotations

import argparse
import xml.etree.ElementTree as ET
from datetime import timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from .common import clean_text, http_get_bytes, normalize_title, normalize_url, now_beijing, parse_datetime, read_json, read_jsonl
from .report import mark_stage, patch_report, report_path


def _extract_query_row(row: Any) -> tuple[str, dict[str, Any]]:
    if isinstance(row, str):
        return row.strip(), {}
    if isinstance(row, dict):
        q = str(row.get("q", "")).strip()
        extra = {k: v for k, v in row.items() if k != "q"}
        return q, extra
    return "", {}


def _inject_recency_token(query: str, recency_token: str) -> str:
    q = (query or "").strip()
    if not q:
        return q
    token = (recency_token or "").strip()
    if not token:
        return q
    if token.lower() in q.lower():
        return q
    return f"{q} {token}".strip()


def _parse_rss_items(xml_data: bytes) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    root = ET.fromstring(xml_data)
    for node in root.findall("./channel/item"):
        title = clean_text((node.findtext("title") or "").strip())
        link = (node.findtext("link") or "").strip()
        pub = (node.findtext("pubDate") or "").strip()
        source = clean_text((node.findtext("source") or "").strip())
        if title and link:
            out.append({"title": title, "link": link, "published": pub, "source": source})
    return out


def _is_same_bj_day(published: str, date_text: str) -> bool:
    if not published:
        return False
    dt = parse_datetime(published)
    bj_tz = now_beijing().tzinfo or timezone.utc
    return dt.astimezone(bj_tz).date().isoformat() == date_text


def _load_baseline(cfg: dict[str, Any], date_text: str, top_n: int) -> list[dict[str, str]]:
    defaults = cfg.get("defaults", {}) if isinstance(cfg, dict) else {}
    if not isinstance(defaults, dict):
        defaults = {}
    recency_token = str(defaults.get("discovery_query_recency", "when:1d")).strip()

    query_sets = cfg.get("query_sets", {}) if isinstance(cfg, dict) else {}
    if not isinstance(query_sets, dict):
        return []

    rows: list[dict[str, str]] = []
    for set_name in ("domestic_robtaxi_discovery", "foreign_robtaxi_discovery"):
        query_rows = query_sets.get(set_name, [])
        if not isinstance(query_rows, list):
            continue

        for row in query_rows:
            query, extra = _extract_query_row(row)
            if not query:
                continue
            query = _inject_recency_token(query, recency_token)
            params = {
                "q": query,
                "hl": str(extra.get("hl", "en")),
                "gl": str(extra.get("gl", "us")),
                "ceid": str(extra.get("ceid", "US:en")),
            }
            url = f"https://news.google.com/rss/search?{urlencode(params)}"
            try:
                data = http_get_bytes(url, timeout=20, retries=3)
                rss_rows = _parse_rss_items(data)
                for item in rss_rows:
                    if _is_same_bj_day(str(item.get("published", "")), date_text):
                        rows.append(item)
            except Exception:
                continue

    dedup: dict[str, dict[str, str]] = {}
    for row in rows:
        link = normalize_url(str(row.get("link", "")))
        title = normalize_title(str(row.get("title", "")))
        key = link or title
        if not key:
            continue
        prev = dedup.get(key)
        if prev is None:
            dedup[key] = row
            continue
        if parse_datetime(str(row.get("published", ""))) > parse_datetime(str(prev.get("published", ""))):
            dedup[key] = row

    sorted_rows = sorted(dedup.values(), key=lambda x: parse_datetime(str(x.get("published", ""))), reverse=True)
    return sorted_rows[:top_n]


def _is_matched(base: dict[str, str], pool_by_url: set[str], pool_titles: list[str]) -> bool:
    base_url = normalize_url(str(base.get("link", "")))
    if base_url and base_url in pool_by_url:
        return True

    title = normalize_title(str(base.get("title", "")))
    if not title:
        return False

    for t in pool_titles:
        if not t:
            continue
        if t == title:
            return True
        if len(title) >= 12 and (title in t or t in title):
            return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare daily pool recall against search baseline")
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD; default Beijing date")
    parser.add_argument("--in", dest="in_root", default="./artifacts/daily_pool", help="Daily pool root")
    parser.add_argument("--sources", default="./sources.json", help="Path to sources.json")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root")
    parser.add_argument("--top-n", type=int, default=20, help="Baseline top N")
    parser.add_argument("--min-recall", type=float, default=0.7, help="Alert threshold")
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "filtered_items.jsonl"
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)
    cfg = read_json(Path(args.sources).expanduser().resolve())

    pool_rows = read_jsonl(in_file)
    pool_urls = {normalize_url(str(r.get("link", ""))) for r in pool_rows if normalize_url(str(r.get("link", "")))}
    pool_titles = [normalize_title(str(r.get("title", "")) or str(r.get("title_zh", ""))) for r in pool_rows]

    baseline = _load_baseline(cfg, date_text, max(1, int(args.top_n)))

    matched = 0
    unmatched_samples: list[dict[str, str]] = []
    for row in baseline:
        if _is_matched(row, pool_urls, pool_titles):
            matched += 1
        elif len(unmatched_samples) < 5:
            unmatched_samples.append(
                {
                    "title": str(row.get("title", "")).strip(),
                    "link": str(row.get("link", "")).strip(),
                    "published": str(row.get("published", "")).strip(),
                }
            )

    baseline_count = len(baseline)
    unmatched_count = max(0, baseline_count - matched)
    recall_at_n = (matched / baseline_count) if baseline_count else 1.0

    pool_kept_today = int(len(pool_rows))
    alert = bool((baseline_count > 0 and pool_kept_today == 0) or (recall_at_n < float(args.min_recall)))
    if alert:
        message = (
            f"覆盖率告警：date={date_text} recall_at_{int(args.top_n)}={recall_at_n:.2f}, "
            f"baseline={baseline_count}, pool={pool_kept_today}"
        )
        status = "partial"
    else:
        message = ""
        status = "success"

    mark_stage(report_file, "recall_guard", status)
    patch_report(
        report_file,
        baseline_count=baseline_count,
        baseline_matched_count=matched,
        baseline_unmatched_count=unmatched_count,
        recall_at_20=round(recall_at_n, 4),
        baseline_unmatched_samples=unmatched_samples,
        recall_guard_alert=alert,
        recall_guard_message=message,
    )

    print(
        f"[recall_guard] date={date_text} baseline={baseline_count} matched={matched} "
        f"recall_at_{int(args.top_n)}={recall_at_n:.2f} alert={alert}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
