from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from .common import normalize_url, now_beijing, parse_datetime, read_jsonl, write_jsonl
from .report import mark_stage, patch_report, report_path


def _item_key(row: dict[str, Any]) -> str:
    link = normalize_url(str(row.get("link", "")))
    fp = str(row.get("fingerprint", "")).strip()
    title = str(row.get("title", "")).strip().lower()
    return f"{link}|{fp}|{title}"


def _published_sort_key(row: dict[str, Any]) -> str:
    published = str(row.get("published_at_utc", "")).strip()
    if not published:
        return "1970-01-01T00:00:00+00:00"
    return parse_datetime(published).isoformat()


def merge_pool(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    for row in existing + incoming:
        if not isinstance(row, dict):
            continue
        key = _item_key(row)
        if not key.strip("|"):
            continue

        prev = merged.get(key)
        if prev is None:
            merged[key] = row
            continue

        if _published_sort_key(row) >= _published_sort_key(prev):
            merged[key] = row

    return sorted(merged.values(), key=_published_sort_key, reverse=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge current filtered items into daily pool")
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD; default Beijing date")
    parser.add_argument("--in", dest="in_root", default="./artifacts/filtered", help="Filtered input root")
    parser.add_argument("--out", default="./artifacts/daily_pool", help="Daily pool output root")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root")
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "filtered_items.jsonl"
    out_root = Path(args.out).expanduser().resolve() / date_text
    pool_file = out_root / "pool_items.jsonl"
    out_filtered_file = out_root / "filtered_items.jsonl"
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)

    incoming = read_jsonl(in_file)
    existing = read_jsonl(pool_file)

    merged = merge_pool(existing, incoming)
    write_jsonl(pool_file, merged)
    # 与 summarize 现有输入契约兼容，直接输出同名文件。
    write_jsonl(out_filtered_file, merged)

    mark_stage(report_file, "pool", "success")
    patch_report(
        report_file,
        daily_pool_size=len(merged),
        daily_pool_incoming_count=len(incoming),
        daily_pool_existing_count=len(existing),
        daily_pool_output=str(pool_file),
    )

    print(
        f"[daily_pool] date={date_text} incoming={len(incoming)} existing={len(existing)} merged={len(merged)}"
    )
    print(f"[daily_pool] output={pool_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
