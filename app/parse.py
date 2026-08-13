from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from .common import (
    USER_AGENT,
    CanonicalItem,
    clean_text,
    detect_language,
    http_get_bytes,
    http_get_last_modified,
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
    read_json,
    write_json,
)
from .fetch import summarize_fetch_error
from .report import (
    empty_method_breakdown,
    empty_stage_funnel,
    load_or_init,
    mark_stage,
    normalize_method,
    patch_report,
    report_path,
)
from .site_rules import extract_site_specific_published


_DATE_PATTERNS = [
    r"\b\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:?\d{2})?\b",
    r"\b\d{4}/\d{2}/\d{2}[ T]\d{2}:\d{2}(?::\d{2})?\b",
    r"\b\d{4}/\d{2}/\d{2}\b",
    r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}(?:,\s*\d{1,2}:\d{2}(?::\d{2})?\s*(?:am|pm))?\b",
    r"\b\d{4}-\d{2}-\d{2}\b",
    r"\b\d{4}年\d{1,2}月\d{1,2}日(?:\s*\d{1,2}:\d{2}(?::\d{2})?)?\b",
]


def _pick_date_from_text(text: str) -> str:
    for pattern in _DATE_PATTERNS:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            return m.group(0).strip()
    return ""


def _extract_date_from_html(html: str, link: str, source_name: str) -> tuple[str, str]:
    """Extract publish date from HTML using tiered priority.

    Returns (raw_date_string, published_source).
    published_source is one of: jsonld, meta_article_published_time,
    meta_pubdate, time_datetime, text_date, unresolved.
    """
    soup = BeautifulSoup(html, "html.parser")

    host = (urlparse(link).netloc or "").lower()
    if host.endswith("gov.uk") or host.endswith("unece.org"):
        candidate, candidate_source = extract_site_specific_published("", html, link)
        if candidate:
            return candidate, candidate_source

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

    # Priority 4.5: 站点特化日期信号。
    candidate, candidate_source = extract_site_specific_published("", html, link)
    if candidate:
        return candidate, candidate_source

    # Priority 5/6: explicit date text with region priority.
    region_texts: list[str] = []
    if soup.head:
        head_parts: list[str] = [soup.head.get_text(" ", strip=True)]
        for node in soup.head.find_all("meta"):
            head_parts.extend(
                [
                    str(node.get("content", "")).strip(),
                    str(node.get("value", "")).strip(),
                ]
            )
        region_texts.append(" ".join(part for part in head_parts if part))

    body_root = soup.select_one("article") or soup.select_one("main") or soup.body
    if body_root:
        body_text = body_root.get_text(" ", strip=True)
        if body_text:
            region_texts.append(body_text[:2000])
            region_texts.append(body_text)

    region_texts.append(soup.get_text(" ", strip=True))

    for text in region_texts:
        date_text = _pick_date_from_text(text)
        if date_text:
            return date_text, "text_date"

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


def _resolve_discovery_published(
    link: str,
    source_name: str,
    fetched_at: str,
    resolved_ok: bool,
    region: str = "foreign",
    source_type: str = "query_rss",
) -> tuple[str, bool, str, str, str, str]:
    """Resolve the real publish date for a discovery item.

    Returns (published_utc_iso, published_missing, parse_status, published_source,
    verify_error_code, verify_error_zh).
    """
    source_type = (source_type or "query_rss").strip().lower()
    unresolved_reason = "查询发现源真实链接未解析" if source_type == "query_rss" else "搜索发现源真实链接未解析"
    unverified_reason = "查询发现源发布时间未验证" if source_type == "query_rss" else "搜索发现源发布时间未验证"
    unverified_status = "query_rss_unverified" if source_type == "query_rss" else "search_result_unverified"
    if not resolved_ok:
        return "", True, unverified_status, "unresolved", "resolver_failed", unresolved_reason

    host = (urlparse(link).netloc or "").lower()
    if host.endswith("news.google.com"):
        # URL was not resolved — should not happen if resolved_ok is True
        return "", True, unverified_status, "unresolved", "resolver_failed", unresolved_reason

    try:
        html = http_get_bytes(link, headers={"User-Agent": USER_AGENT}, timeout=15, retries=3).decode(
            "utf-8", errors="ignore"
        )
    except Exception as exc:
        err_code, err_zh = summarize_fetch_error(str(exc))
        code_map = {
            "access_forbidden": "fetch_forbidden",
            "timeout": "fetch_timeout",
            "ssl_error": "fetch_ssl_error",
        }
        return "", True, unverified_status, "unresolved", code_map.get(err_code, "fetch_other"), err_zh or "抓取异常"

    # Try tiered extraction from HTML (priorities 1-5)
    raw_date, pub_source = _extract_date_from_html(html, link, source_name)
    if raw_date:
        parsed_dt, parse_status = _parse_with_region_tz(raw_date, region)
        if parse_status == "ok":
            return utc_iso(parsed_dt), False, "ok", pub_source, "", ""

    # Priority 6: HTTP Last-Modified (weak signal, 48h constraint)
    last_modified_header = http_get_last_modified(link, headers={"User-Agent": USER_AGENT}, timeout=10)
    if last_modified_header:
        parsed_dt, parse_status = parse_datetime_with_status(last_modified_header)
        if parse_status == "ok":
            # Only accept if within 48h of fetched_at
            fetched_dt, _ = parse_datetime_with_status(fetched_at)
            diff = abs((fetched_dt - parsed_dt).total_seconds())
            if diff <= 48 * 3600:
                return utc_iso(parsed_dt), False, "ok", "last_modified", "", ""

    return "", True, unverified_status, "unresolved", "published_not_found", unverified_reason


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
    source_role = str(row.get("source_role", "secondary")).strip().lower() or "secondary"
    evidence_type = str(row.get("evidence_type", "general_media")).strip().lower() or "general_media"
    criticality = str(row.get("criticality", "important")).strip().lower() or "important"
    configured_domains = [str(value).strip().lower() for value in row.get("coverage_domains", []) if str(value).strip()]
    first_seen_at_utc = str(row.get("fetched_at", "")).strip()

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
    query_rss_verify_error_code = ""
    query_rss_verify_error_zh = ""

    if source_type in {"query_rss", "search_result"}:
        item_resolved_ok = str(payload.get("resolved_ok", "")).lower() == "true"
        if source_type == "search_result":
            item_resolved_ok = bool(link)
        item_resolved_url = str(payload.get("resolved_url", "")).strip()
        if source_type == "search_result":
            item_resolved_url = link

        # search_result 类型：若搜索结果页自带的时间已成功解析，信任该时间，跳过原文页验证
        if source_type == "search_result" and parse_status == "ok" and not published_missing:
            published_source = "search_result_display_time"
        else:
            fetched_at = str(row.get("fetched_at", "")).strip()
            verified_published, verified_missing, verified_status, pub_source, verify_err_code, verify_err_zh = (
                _resolve_discovery_published(link, source_name, fetched_at, item_resolved_ok, region, source_type)
            )
            published = verified_published
            published_missing = verified_missing
            parse_status = verified_status
            published_source = pub_source
            query_rss_verify_error_code = verify_err_code
            query_rss_verify_error_zh = verify_err_zh

    canonical_url = normalize_url(str(payload.get("canonical_url", ""))) or link
    outbound_urls = [
        normalize_url(str(value))
        for value in payload.get("outbound_urls", [])
        if normalize_url(str(value))
    ]
    if source_role == "social_discovery":
        official_outbound = next(
            (
                value
                for value in outbound_urls
                if (urlparse(value).netloc or "").lower()
                not in {"x.com", "www.x.com", "twitter.com", "www.twitter.com", "mp.weixin.qq.com"}
            ),
            "",
        )
        if official_outbound:
            canonical_url = official_outbound
    uid_base = f"{canonical_url}|{published}|{title}"
    cid = sha1_text(uid_base)

    fingerprint = sha1_text(normalize_title(title) or title.lower())

    lang = detect_language(f"{title} {content}")
    official_verified_raw = payload.get("official_account_verified", False)
    if isinstance(official_verified_raw, str):
        official_verified_raw = official_verified_raw.strip().lower() == "true"
    social_platform = str(payload.get("social_platform", "")).strip().lower()
    discovery_method = str(payload.get("discovery_method", row.get("discovery_method", ""))).strip().lower()
    if not discovery_method:
        if source_type in {"query_rss", "search_result", "search_api"}:
            discovery_method = "legacy_search"
        elif source_type == "social_provider":
            discovery_method = "social_seed"
        else:
            discovery_method = "direct_source"
    evidence = [dict(value) for value in payload.get("evidence", []) if isinstance(value, dict)]
    if source_role == "social_discovery":
        official_accounts = row.get("official_accounts", {}) if isinstance(row.get("official_accounts", {}), dict) else {}
        parsed_social_url = urlparse(link)
        host = (parsed_social_url.netloc or "").lower()
        path_parts = [value for value in (parsed_social_url.path or "").split("/") if value]
        if host in {"x.com", "www.x.com", "twitter.com", "www.twitter.com"} and len(path_parts) >= 3:
            configured_handles = {
                str(value).strip().lstrip("@").lower()
                for value in official_accounts.get("x_handles", [])
                if str(value).strip()
            }
            is_permalink = path_parts[1].lower() == "status" and bool(path_parts[2])
            official_verified_raw = is_permalink and path_parts[0].lower() in configured_handles
            social_platform = "x"

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
        query_rss_verify_error_code=query_rss_verify_error_code,
        query_rss_verify_error_zh=query_rss_verify_error_zh,
        coverage_domains=configured_domains,
        source_role=source_role,
        evidence_type=evidence_type,
        criticality=criticality,
        canonical_url=canonical_url,
        first_seen_at_utc=first_seen_at_utc,
        social_platform=social_platform,
        official_account_verified=bool(official_verified_raw),
        outbound_urls=outbound_urls,
        discovery_method=discovery_method,
        evidence=evidence,
        agent_run_id=str(payload.get("agent_run_id", "")),
        agent_verification_status=str(payload.get("verification_status", "")),
        agent_importance_score=int(payload.get("importance_score", 0) or 0),
        source_type=source_type,
    )


# ---------------------------------------------------------------------------
#  Historical dedup (seen_urls)
# ---------------------------------------------------------------------------

_SEEN_STATE_PATH = Path(".state/seen_urls.jsonl")
_SEEN_RETENTION_DAYS = 35
_FIRST_SEEN_STATE_PATH = Path(".state/first_seen.json")


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
        if source_type not in {"query_rss", "search_result"}:
            # Also check source_id pattern — brief items may not carry source_type
            pass

        url = str(item.get("canonical_url", "") or item.get("resolved_url", "") or item.get("link", "")).strip()
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


def update_first_seen_state(
    items: list[CanonicalItem],
    state_path: Path | None = None,
) -> int:
    """为所有发现候选固定首次发现时间，避免旧内容在后续运行中伪装成首次补录。"""
    path = state_path or _FIRST_SEEN_STATE_PATH
    records: dict[str, str] = {}
    if path.exists():
        try:
            payload = read_json(path)
            raw_records = payload.get("records", {}) if isinstance(payload, dict) else {}
            if isinstance(raw_records, dict):
                records = {str(key): str(value) for key, value in raw_records.items() if str(key) and str(value)}
        except Exception:
            records = {}

    cutoff = datetime.now(timezone.utc) - timedelta(days=_SEEN_RETENTION_DAYS)
    records = {
        key: value
        for key, value in records.items()
        if parse_datetime(value) >= cutoff
    }
    new_count = 0
    for item in items:
        key = item.canonical_url or item.link
        if not key:
            continue
        if key in records:
            item.first_seen_at_utc = records[key]
            continue
        first_seen = item.first_seen_at_utc or datetime.now(timezone.utc).isoformat()
        records[key] = first_seen
        item.first_seen_at_utc = first_seen
        new_count += 1
    write_json(path, {"version": 1, "records": records})
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
    parser.add_argument("--seen-state", default="./.state/seen_urls.jsonl", help="跨日已见内容状态文件")
    parser.add_argument("--first-seen-state", default="./.state/first_seen.json", help="候选首次发现时间状态文件")
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "raw_items.jsonl"
    out_file = Path(args.out).expanduser().resolve() / date_text / "canonical_items.jsonl"
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)

    rows = read_jsonl(in_file)
    pre_candidate_drop_breakdown = empty_method_breakdown()
    pre_candidate_drop_total = 0
    source_type_by_source_id = {
        str(row.get("source_id", "")).strip(): str(row.get("source_type", "")).strip().lower()
        for row in rows
        if str(row.get("source_id", "")).strip()
    }
    discovery_source_ids = {
        str(row.get("source_id", "")).strip()
        for row in rows
        if str(row.get("source_type", "")).strip().lower() in {"query_rss", "search_result"}
    }
    canonical_all: list[CanonicalItem] = []
    for row in rows:
        method = normalize_method(str(row.get("source_type", "")))
        item = canonicalize_row(row)
        if item is not None:
            canonical_all.append(item)
            continue
        if method:
            pre_candidate_drop_breakdown[method]["缺少标题或链接"] = (
                int(pre_candidate_drop_breakdown[method].get("缺少标题或链接", 0)) + 1
            )
            pre_candidate_drop_total += 1

    first_seen_new_count = update_first_seen_state(
        canonical_all,
        Path(args.first_seen_state).expanduser().resolve(),
    )

    dropped_l1 = 0
    dropped_l2 = 0
    query_rss_seen_skip_count = 0
    search_result_seen_skip_count = 0

    by_url: list[CanonicalItem] = []
    seen_urls = set()
    for item in sorted(canonical_all, key=lambda x: x.published_at_utc, reverse=True):
        dedupe_url = item.canonical_url or item.link
        if dedupe_url in seen_urls:
            dropped_l1 += 1
            method = normalize_method(source_type_by_source_id.get(item.source_id, ""))
            if method:
                pre_candidate_drop_breakdown[method]["一级去重（链接）"] = (
                    int(pre_candidate_drop_breakdown[method].get("一级去重（链接）", 0)) + 1
                )
                pre_candidate_drop_total += 1
            continue
        seen_urls.add(dedupe_url)
        by_url.append(item)

    # 历史去重覆盖全部入选过的信源，防止补录和跨日重复。
    hist_urls, hist_fps, _hist_records = _load_seen_db(Path(args.seen_state).expanduser().resolve())
    after_hist: list[CanonicalItem] = []
    for item in by_url:
        dedupe_url = item.canonical_url or item.link
        if dedupe_url in hist_urls or item.fingerprint in hist_fps:
            method = normalize_method(source_type_by_source_id.get(item.source_id, ""))
            if method:
                pre_candidate_drop_breakdown[method]["历史去重（已见文章）"] = (
                    int(pre_candidate_drop_breakdown[method].get("历史去重（已见文章）", 0)) + 1
                )
                pre_candidate_drop_total += 1
            if item.source_id in discovery_source_ids:
                if str(item.source_id).startswith(("domestic_discovery_search_result", "foreign_discovery_search_result")):
                    search_result_seen_skip_count += 1
                else:
                    query_rss_seen_skip_count += 1
            continue
        after_hist.append(item)

    by_title: list[CanonicalItem] = []
    seen_titles = set()
    for item in after_hist:
        tk = normalize_title(item.title) or item.title.lower().strip()
        if tk and tk in seen_titles:
            dropped_l2 += 1
            method = normalize_method(source_type_by_source_id.get(item.source_id, ""))
            if method:
                pre_candidate_drop_breakdown[method]["二级去重（标题）"] = (
                    int(pre_candidate_drop_breakdown[method].get("二级去重（标题）", 0)) + 1
                )
                pre_candidate_drop_total += 1
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
    query_rss_fetch_forbidden_count = 0
    query_rss_fetch_timeout_count = 0
    query_rss_fetch_ssl_error_count = 0
    query_rss_fetch_other_count = 0
    query_rss_published_not_found_count = 0
    search_result_fetch_success_count = 0
    search_result_fetch_fail_count = 0
    search_result_verified_count = 0
    for item in canonical_all:
        if item.source_id in discovery_source_ids:
            is_search_result = str(item.source_id).startswith(("domestic_discovery_search_result", "foreign_discovery_search_result"))
            if is_search_result:
                if item.published_parse_status == "ok":
                    search_result_fetch_success_count += 1
                    search_result_verified_count += 1
                else:
                    search_result_fetch_fail_count += 1
            elif item.resolved_ok:
                query_rss_resolved_count += 1
            else:
                query_rss_resolve_fail_count += 1
            if item.query_rss_verify_error_code == "fetch_forbidden":
                query_rss_fetch_forbidden_count += 1
            elif item.query_rss_verify_error_code == "fetch_timeout":
                query_rss_fetch_timeout_count += 1
            elif item.query_rss_verify_error_code == "fetch_ssl_error":
                query_rss_fetch_ssl_error_count += 1
            elif item.query_rss_verify_error_code == "fetch_other":
                query_rss_fetch_other_count += 1
            elif item.query_rss_verify_error_code == "published_not_found":
                query_rss_published_not_found_count += 1

    report = load_or_init(report_file)
    report_dedupe = int(report.get("dedupe_drop_count", 0)) + dropped_l1 + dropped_l2
    stage_funnel = report.get("stage_funnel", {})
    if not isinstance(stage_funnel, dict):
        stage_funnel = empty_stage_funnel()
    for method, counts in empty_stage_funnel().items():
        current = stage_funnel.get(method, {}) if isinstance(stage_funnel.get(method, {}), dict) else {}
        stage_funnel[method] = {
            "fetched": int(current.get("fetched", 0)),
            "candidate": int(current.get("candidate", 0)),
            "filtered": int(current.get("filtered", 0)),
            "kept": int(current.get("kept", 0)),
        }

    mark_stage(report_file, "parse", "success")
    patch_report(
        report_file,
        total_items_canonical=len(by_title),
        stage_funnel=stage_funnel,
        pre_candidate_drop_total=pre_candidate_drop_total,
        pre_candidate_drop_breakdown=pre_candidate_drop_breakdown,
        dedupe_drop_count=report_dedupe,
        parse_dedupe_l1=dropped_l1,
        parse_dedupe_l2=dropped_l2,
        canonical_output=str(out_file),
        canonical_by_source=dict(source_dist),
        first_seen_new_count=first_seen_new_count,
        discovery_items_canonical_count=discovery_items_canonical_count,
        discovery_today_canonical_count=discovery_today_canonical_count,
        published_parse_status_dist=dict(parse_status_dist),
        published_unparseable_count=published_unparseable_count,
        query_rss_resolved_count=query_rss_resolved_count,
        query_rss_resolve_fail_count=query_rss_resolve_fail_count,
        query_rss_seen_skip_count=query_rss_seen_skip_count,
        search_result_seen_skip_count=search_result_seen_skip_count,
        query_rss_fetch_forbidden_count=query_rss_fetch_forbidden_count,
        query_rss_fetch_timeout_count=query_rss_fetch_timeout_count,
        query_rss_fetch_ssl_error_count=query_rss_fetch_ssl_error_count,
        query_rss_fetch_other_count=query_rss_fetch_other_count,
        query_rss_published_not_found_count=query_rss_published_not_found_count,
        search_result_fetch_success_count=search_result_fetch_success_count,
        search_result_fetch_fail_count=search_result_fetch_fail_count,
        search_result_verified_count=search_result_verified_count,
    )

    print(
        f"[parse] date={date_text} raw={len(rows)} canonical={len(by_title)} "
        f"drop_l1={dropped_l1} drop_l2={dropped_l2} seen_skip={query_rss_seen_skip_count}"
    )
    print(f"[parse] output={out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
