from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

from .common import (
    USER_AGENT,
    CanonicalItem,
    clean_text,
    detect_language,
    http_get_bytes,
    normalize_title,
    normalize_url,
    now_beijing,
    parse_datetime,
    parse_datetime_with_status,
    read_jsonl,
    sha1_text,
    to_dict_list,
    utc_iso,
    write_jsonl,
)
from .fetch import _extract_article_jsonld
from .report import load_or_init, mark_stage, patch_report, report_path


def _extract_date_from_html(html: str, link: str, source_name: str) -> tuple[str, str]:
    """Extract publish date from HTML using tiered priority.

    Returns (raw_date_string, published_source).
    published_source is one of: jsonld, meta_article_published_time,
    meta_pubdate, time_datetime, text_date, unresolved.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Priority 1: JSON-LD datePublished / dateCreated
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        content = (script.string or script.get_text() or "").strip()
        if not content:
            continue
        try:
            payload = json.loads(content)
        except Exception:
            continue
        stack = [payload]
        while stack:
            cur = stack.pop()
            if isinstance(cur, list):
                stack.extend(cur)
                continue
            if not isinstance(cur, dict):
                continue
            typ = str(cur.get("@type", "")).lower()
            if "article" in typ or "newsarticle" in typ or "blogposting" in typ:
                for key in ("datePublished", "dateCreated"):
                    val = str(cur.get(key, "")).strip()
                    if val:
                        return val, "jsonld"
            stack.extend(v for v in cur.values() if isinstance(v, (dict, list)))

    # Priority 2: <meta property="article:published_time">
    meta = soup.select_one('meta[property="article:published_time"]')
    if meta:
        val = (meta.get("content") or "").strip()
        if val:
            return val, "meta_article_published_time"

    # Priority 3: <meta name="pubdate"> / <meta name="date">
    for sel in ('meta[name="pubdate"]', 'meta[name="PubDate"]', 'meta[name="date"]',
                'meta[name="publish_date"]', 'meta[itemprop="datePublished"]'):
        node = soup.select_one(sel)
        if node:
            val = (node.get("content") or "").strip()
            if val:
                return val, "meta_pubdate"

    # Priority 4: <time datetime="...">
    time_tag = soup.find("time", attrs={"datetime": True})
    if time_tag:
        val = (time_tag.get("datetime") or "").strip()
        if val:
            return val, "time_datetime"

    # Priority 5: explicit date text in body
    text = soup.get_text(" ", strip=True)
    date_patterns = [
        r"\b\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:?\d{2})?\b",
        r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}\b",
        r"\b\d{4}-\d{2}-\d{2}\b",
        r"\b\d{4}年\d{1,2}月\d{1,2}日\b",
    ]
    for pattern in date_patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            return m.group(0).strip(), "text_date"

    return "", "unresolved"


_TZ_PATTERN = re.compile(r"(?:Z|[+-]\d{2}:?\d{2})\s*$")
_BJ_OFFSET = timedelta(hours=8)


def _has_explicit_timezone(raw_date: str) -> bool:
    """Check if a raw date string contains explicit timezone info."""
    return bool(_TZ_PATTERN.search(raw_date.strip()))


def _parse_with_region_tz(raw_date: str, region: str) -> tuple[datetime, str]:
    """Parse a date string, treating timezone-naive dates as Beijing time for domestic region."""
    parsed_dt, parse_status = parse_datetime_with_status(raw_date)
    if parse_status != "ok":
        return parsed_dt, parse_status

    # If the raw string has no explicit timezone and region is domestic,
    # the site likely output Beijing time — adjust from assumed-UTC to real UTC.
    if region == "domestic" and not _has_explicit_timezone(raw_date):
        parsed_dt = parsed_dt.replace(tzinfo=None)
        parsed_dt = parsed_dt.replace(tzinfo=timezone(_BJ_OFFSET)).astimezone(timezone.utc)

    return parsed_dt, "ok"


def _resolve_query_rss_published(
    link: str, source_name: str, fetched_at: str, resolved_ok: bool, region: str = "foreign",
) -> tuple[str, bool, str, str]:
    """Resolve the real publish date for a query_rss item.

    Returns (published_utc_iso, published_missing, parse_status, published_source).
    """
    if not resolved_ok:
        return "", True, "query_rss_unverified", "unresolved"

    host = (urlparse(link).netloc or "").lower()
    if host.endswith("news.google.com"):
        # URL was not resolved — should not happen if resolved_ok is True
        return "", True, "query_rss_unverified", "unresolved"

    try:
        # Fetch article page, get headers for Last-Modified
        req = Request(link, headers={"User-Agent": USER_AGENT})
        with urlopen(req, timeout=10) as resp:
            last_modified_header = (resp.headers.get("Last-Modified") or "").strip()
            html = resp.read().decode("utf-8", errors="ignore")
    except Exception:
        return "", True, "query_rss_unverified", "unresolved"

    # Try tiered extraction from HTML (priorities 1-5)
    raw_date, pub_source = _extract_date_from_html(html, link, source_name)
    if raw_date:
        parsed_dt, parse_status = _parse_with_region_tz(raw_date, region)
        if parse_status == "ok":
            return utc_iso(parsed_dt), False, "ok", pub_source

    # Priority 6: HTTP Last-Modified (weak signal, 48h constraint)
    if last_modified_header:
        parsed_dt, parse_status = parse_datetime_with_status(last_modified_header)
        if parse_status == "ok":
            # Only accept if within 48h of fetched_at
            fetched_dt, _ = parse_datetime_with_status(fetched_at)
            diff = abs((fetched_dt - parsed_dt).total_seconds())
            if diff <= 48 * 3600:
                return utc_iso(parsed_dt), False, "ok", "last_modified"

    return "", True, "query_rss_unverified", "unresolved"


def canonicalize_row(row: dict) -> CanonicalItem | None:
    payload = row.get("payload", {}) if isinstance(row.get("payload", {}), dict) else {}

    title = clean_text(str(payload.get("title", "")))
    content = clean_text(str(payload.get("content", ""))) or clean_text(str(payload.get("summary", "")))
    link = normalize_url(str(payload.get("link", "")) or str(row.get("url", "")))
    if not title or not link:
        return None

    source_id = str(row.get("source_id", "")).strip()
    source_type = str(row.get("source_type", "")).strip().lower()
    source_name = str(payload.get("source_name", "") or row.get("source_name", "")).strip()
    region = str(row.get("region", "foreign")).strip().lower()
    company_hint = str(row.get("company_hint", "")).strip()
    discovery_query_group = str(payload.get("discovery_query_group", "")).strip().lower()

    raw_published = str(payload.get("published", "")).strip()
    parsed_dt, parse_status = parse_datetime_with_status(raw_published)
    if parse_status == "ok":
        published = utc_iso(parsed_dt)
        published_missing = False
    else:
        published = ""
        published_missing = True

    published_source = "feed"
    item_resolved_ok = True
    item_resolved_url = ""

    if source_type == "query_rss":
        item_resolved_ok = str(payload.get("resolved_ok", "")).lower() == "true"
        item_resolved_url = str(payload.get("resolved_url", "")).strip()
        fetched_at = str(row.get("fetched_at", "")).strip()
        verified_published, verified_missing, verified_status, pub_source = (
            _resolve_query_rss_published(link, source_name, fetched_at, item_resolved_ok, region)
        )
        published = verified_published
        published_missing = verified_missing
        parse_status = verified_status
        published_source = pub_source

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
        published_parse_status=parse_status,
        discovery_query_group=discovery_query_group,
        language=lang,
        fingerprint=fingerprint,
        published_source=published_source,
        resolved_ok=item_resolved_ok,
        resolved_url=item_resolved_url,
    )


# ---------------------------------------------------------------------------
#  Historical dedup (seen_urls)
# ---------------------------------------------------------------------------

_SEEN_STATE_PATH = Path(".state/seen_urls.jsonl")
_SEEN_RETENTION_DAYS = 14


def _load_seen_db(state_path: Path | None = None) -> tuple[set[str], set[str], list[dict]]:
    """Load historical seen URLs and fingerprints.

    Returns (seen_urls, seen_fingerprints, raw_records).
    """
    path = state_path or _SEEN_STATE_PATH
    seen_urls: set[str] = set()
    seen_fps: set[str] = set()
    records: list[dict] = []
    if not path.exists():
        return seen_urls, seen_fps, records
    try:
        cutoff = (datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=_SEEN_RETENTION_DAYS)).isoformat()
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            # Auto-clean records older than retention period
            if str(rec.get("last_seen_date", "")) < cutoff[:10]:
                continue
            records.append(rec)
            url = str(rec.get("resolved_url", "")).strip()
            fp = str(rec.get("fingerprint", "")).strip()
            if url:
                seen_urls.add(url)
            if fp:
                seen_fps.add(fp)
    except Exception:
        pass
    return seen_urls, seen_fps, records


def update_seen_db(
    items: list[dict],
    date_text: str,
    state_path: Path | None = None,
) -> int:
    """Write successfully processed query_rss items to the seen_urls history.

    Called after brief output to persist processed articles.
    Returns number of new entries added.
    """
    path = state_path or _SEEN_STATE_PATH
    # Load existing records (with auto-clean)
    _, _, existing = _load_seen_db(path)

    # Build lookup from existing records
    url_map: dict[str, dict] = {}
    fp_map: dict[str, dict] = {}
    for rec in existing:
        url = str(rec.get("resolved_url", "")).strip()
        fp = str(rec.get("fingerprint", "")).strip()
        if url:
            url_map[url] = rec
        if fp:
            fp_map[fp] = rec

    new_count = 0
    for item in items:
        source_type = str(item.get("source_type", "")).strip().lower()
        if source_type != "query_rss":
            # Also check source_id pattern — brief items may not carry source_type
            pass

        url = str(item.get("resolved_url", "") or item.get("link", "")).strip()
        fp = str(item.get("fingerprint", "")).strip()
        if not url and not fp:
            continue

        # Update existing or create new
        if url and url in url_map:
            url_map[url]["last_seen_date"] = date_text
        elif fp and fp in fp_map:
            fp_map[fp]["last_seen_date"] = date_text
        else:
            rec = {
                "resolved_url": url,
                "fingerprint": fp,
                "first_seen_date": date_text,
                "last_seen_date": date_text,
            }
            if url:
                url_map[url] = rec
            if fp:
                fp_map[fp] = rec
            existing.append(rec)
            new_count += 1

    # Deduplicate by resolved_url (prefer records with both url and fp)
    final: list[dict] = []
    seen_out: set[str] = set()
    for rec in existing:
        key = str(rec.get("resolved_url", "")).strip() or str(rec.get("fingerprint", "")).strip()
        if key and key in seen_out:
            continue
        if key:
            seen_out.add(key)
        final.append(rec)

    # Write back
    from .common import ensure_dir, write_jsonl as _write_jsonl
    ensure_dir(path.parent)
    _write_jsonl(path, final)

    return new_count


def _is_same_bj_day(ts_iso: str, date_text: str) -> bool:
    if not ts_iso or not date_text:
        return False
    dt = parse_datetime(ts_iso)
    return dt.astimezone(now_beijing().tzinfo or timezone.utc).date().isoformat() == date_text


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
    query_rss_seen_skip_count = 0

    by_url: list[CanonicalItem] = []
    seen_urls = set()
    for item in sorted(canonical_all, key=lambda x: x.published_at_utc, reverse=True):
        if item.link in seen_urls:
            dropped_l1 += 1
            continue
        seen_urls.add(item.link)
        by_url.append(item)

    # Historical dedup: skip query_rss items already seen in previous runs
    hist_urls, hist_fps, _hist_records = _load_seen_db()
    after_hist: list[CanonicalItem] = []
    for item in by_url:
        if item.source_id in discovery_source_ids:
            if item.link in hist_urls or item.fingerprint in hist_fps:
                query_rss_seen_skip_count += 1
                continue
        after_hist.append(item)

    by_title: list[CanonicalItem] = []
    seen_titles = set()
    for item in after_hist:
        tk = normalize_title(item.title) or item.title.lower().strip()
        if tk and tk in seen_titles:
            dropped_l2 += 1
            continue
        if tk:
            seen_titles.add(tk)
        by_title.append(item)

    write_jsonl(out_file, to_dict_list(by_title))

    source_dist = defaultdict(int)
    parse_status_dist = defaultdict(int)
    for item in by_title:
        source_dist[item.source_id] += 1
        parse_status_dist[item.published_parse_status] += 1
    discovery_items_canonical_count = sum(1 for item in by_title if item.source_id in discovery_source_ids)
    discovery_today_canonical_count = sum(
        1 for item in by_title if item.source_id in discovery_source_ids and _is_same_bj_day(item.published_at_utc, date_text)
    )
    published_unparseable_count = int(parse_status_dist.get("unparseable_relative", 0)) + int(
        parse_status_dist.get("unparseable_other", 0)
    )

    # Resolver stats for query_rss items
    query_rss_resolved_count = 0
    query_rss_resolve_fail_count = 0
    for item in canonical_all:
        if item.source_id in discovery_source_ids:
            if item.resolved_ok:
                query_rss_resolved_count += 1
            else:
                query_rss_resolve_fail_count += 1

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
        discovery_today_canonical_count=discovery_today_canonical_count,
        published_parse_status_dist=dict(parse_status_dist),
        published_unparseable_count=published_unparseable_count,
        query_rss_resolved_count=query_rss_resolved_count,
        query_rss_resolve_fail_count=query_rss_resolve_fail_count,
        query_rss_seen_skip_count=query_rss_seen_skip_count,
    )

    print(
        f"[parse] date={date_text} raw={len(rows)} canonical={len(by_title)} "
        f"drop_l1={dropped_l1} drop_l2={dropped_l2} seen_skip={query_rss_seen_skip_count}"
    )
    print(f"[parse] output={out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
