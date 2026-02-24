from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path

from .common import (
    CanonicalItem,
    clean_text,
    detect_language,
    normalize_title,
    normalize_url,
    now_beijing,
    parse_datetime,
    read_jsonl,
    sha1_text,
    to_dict_list,
    utc_iso,
    write_jsonl,
)
from .report import load_or_init, mark_stage, patch_report, report_path


def canonicalize_row(row: dict) -> CanonicalItem | None:
    payload = row.get("payload", {}) if isinstance(row.get("payload", {}), dict) else {}

    title = clean_text(str(payload.get("title", "")))
    content = clean_text(str(payload.get("content", ""))) or clean_text(str(payload.get("summary", "")))
    link = normalize_url(str(payload.get("link", "")) or str(row.get("url", "")))
    if not title or not link:
        return None

    raw_published = str(payload.get("published", "")).strip()
    published_missing = not bool(raw_published)
    published = "" if published_missing else utc_iso(parse_datetime(raw_published))
    source_id = str(row.get("source_id", "")).strip()
    source_name = str(payload.get("source_name", "") or row.get("source_name", "")).strip()
    region = str(row.get("region", "foreign")).strip().lower()
    company_hint = str(row.get("company_hint", "")).strip()

    uid_base = f"{link}|{published}|{title}"
    cid = sha1_text(uid_base)

    fingerprint = sha1_text(normalize_title(title) or title.lower())

    lang = detect_language(f"{title} {content}")

    return CanonicalItem(
        id=cid,
        source_id=source_id,
        source_name=source_name,
        region=region,
        company_hint=company_hint,
        title=title,
        content=content[:8000],
        link=link,
        published_at_utc=published,
        published_missing=published_missing,
        language=lang,
        fingerprint=fingerprint,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse raw items into canonical schema with L1/L2 dedupe")
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD; default today in Beijing")
    parser.add_argument("--in", dest="in_root", default="./artifacts/raw", help="Raw input root")
    parser.add_argument("--out", default="./artifacts/canonical", help="Canonical output root")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root")
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "raw_items.jsonl"
    out_file = Path(args.out).expanduser().resolve() / date_text / "canonical_items.jsonl"
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)

    rows = read_jsonl(in_file)
    discovery_source_ids = {
        str(row.get("source_id", "")).strip()
        for row in rows
        if str(row.get("source_type", "")).strip().lower() == "query_rss"
    }
    canonical_all: list[CanonicalItem] = []
    for row in rows:
        item = canonicalize_row(row)
        if item is not None:
            canonical_all.append(item)

    dropped_l1 = 0
    dropped_l2 = 0

    by_url: list[CanonicalItem] = []
    seen_urls = set()
    for item in sorted(canonical_all, key=lambda x: x.published_at_utc, reverse=True):
        if item.link in seen_urls:
            dropped_l1 += 1
            continue
        seen_urls.add(item.link)
        by_url.append(item)

    by_title: list[CanonicalItem] = []
    seen_titles = set()
    for item in by_url:
        tk = normalize_title(item.title)
        if tk and tk in seen_titles:
            dropped_l2 += 1
            continue
        if tk:
            seen_titles.add(tk)
        by_title.append(item)

    write_jsonl(out_file, to_dict_list(by_title))

    source_dist = defaultdict(int)
    for item in by_title:
        source_dist[item.source_id] += 1
    discovery_items_canonical_count = sum(1 for item in by_title if item.source_id in discovery_source_ids)

    report = load_or_init(report_file)
    report_dedupe = int(report.get("dedupe_drop_count", 0)) + dropped_l1 + dropped_l2

    mark_stage(report_file, "parse", "success")
    patch_report(
        report_file,
        total_items_canonical=len(by_title),
        dedupe_drop_count=report_dedupe,
        parse_dedupe_l1=dropped_l1,
        parse_dedupe_l2=dropped_l2,
        canonical_output=str(out_file),
        canonical_by_source=dict(source_dist),
        discovery_items_canonical_count=discovery_items_canonical_count,
    )

    print(
        f"[parse] date={date_text} raw={len(rows)} canonical={len(by_title)} "
        f"drop_l1={dropped_l1} drop_l2={dropped_l2}"
    )
    print(f"[parse] output={out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
