from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from .common import (
    clean_text,
    http_get_bytes,
    now_beijing,
    read_jsonl,
    write_jsonl,
)
from .report import mark_stage, patch_report, report_path


# CSS selector cascade for article body extraction.
_CONTENT_SELECTORS = [
    "article p",
    ".article-body p",
    ".entry-content p",
    ".post-content p",
    "main p",
]

MIN_CONTENT_LEN = 500


def _extract_jsonld_body(html: str) -> str:
    """Try to extract articleBody from JSON-LD structured data."""
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = (script.string or script.get_text() or "").strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except Exception:
            continue

        stack: list[Any] = [payload]
        while stack:
            cur = stack.pop()
            if isinstance(cur, list):
                stack.extend(cur)
                continue
            if not isinstance(cur, dict):
                continue
            typ = str(cur.get("@type", "")).lower()
            if "article" in typ:
                body = clean_text(str(cur.get("articleBody", "")))
                if body and len(body) >= MIN_CONTENT_LEN:
                    return body[:4000]
            stack.extend(cur.values())
    return ""


def _extract_css_body(html: str) -> str:
    """Try common CSS selectors to extract article body text."""
    soup = BeautifulSoup(html, "html.parser")
    for selector in _CONTENT_SELECTORS:
        nodes = soup.select(selector)
        if not nodes:
            continue
        body = clean_text(" ".join(n.get_text(" ", strip=True) for n in nodes))
        if body and len(body) >= MIN_CONTENT_LEN:
            return body[:4000]
    return ""


def enrich_item(item: dict[str, Any]) -> dict[str, Any]:
    """Fetch full article text for an item with short content."""
    content = str(item.get("content", ""))
    if len(content) >= MIN_CONTENT_LEN:
        item["enriched"] = False
        return item

    link = str(item.get("link", "")).strip()
    if not link:
        item["enriched"] = False
        return item

    try:
        data = http_get_bytes(link, timeout=15, retries=2)
        html = data.decode("utf-8", errors="ignore")
    except Exception:
        item["enriched"] = False
        return item

    # Try JSON-LD first, then CSS selectors.
    body = _extract_jsonld_body(html) or _extract_css_body(html)
    if body and len(body) > len(content):
        item["content"] = body
        item["enriched"] = True
    else:
        item["enriched"] = False

    return item


def main() -> int:
    parser = argparse.ArgumentParser(description="Enrich filtered items with full article text")
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD; default Beijing date")
    parser.add_argument("--in", dest="in_root", default="./artifacts/filtered", help="Filtered input root")
    parser.add_argument("--out", default="./artifacts/enriched", help="Enriched output root")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root")
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "filtered_items.jsonl"
    out_file = Path(args.out).expanduser().resolve() / date_text / "enriched_items.jsonl"
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)

    items = read_jsonl(in_file)
    if not items:
        write_jsonl(out_file, [])
        mark_stage(report_file, "enrich", "success")
        patch_report(report_file, enrich_total=0, enrich_attempted=0, enrich_success=0)
        print(f"[enrich] date={date_text} total=0 enriched=0")
        return 0

    enriched_items: list[dict[str, Any]] = []
    attempted = 0
    success = 0
    errors: list[str] = []

    for item in items:
        content = str(item.get("content", ""))
        needs_enrich = len(content) < MIN_CONTENT_LEN and str(item.get("link", "")).strip()
        if needs_enrich:
            attempted += 1

        try:
            enriched = enrich_item(item)
        except Exception as exc:
            errors.append(f"[{item.get('link', '?')}] {exc}")
            enriched = item
            enriched["enriched"] = False

        if enriched.get("enriched"):
            success += 1
        enriched_items.append(enriched)

    write_jsonl(out_file, enriched_items)

    stage = "success" if not errors else "partial"
    mark_stage(report_file, "enrich", stage)
    patch_report(
        report_file,
        enrich_total=len(items),
        enrich_attempted=attempted,
        enrich_success=success,
        enrich_errors="; ".join(errors)[:500] if errors else "",
    )

    print(f"[enrich] date={date_text} total={len(items)} attempted={attempted} enriched={success}")
    print(f"[enrich] output={out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
