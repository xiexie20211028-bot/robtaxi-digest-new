from __future__ import annotations

import argparse
import base64
import concurrent.futures
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, quote_plus, unquote, urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

from .common import (
    USER_AGENT,
    RawItem,
    SourceStat,
    clean_text,
    detect_xml_encoding,
    http_get_bytes,
    http_get_json,
    now_beijing,
    parse_datetime,
    parse_datetime_with_status,
    read_json,
    to_dict_list,
    write_jsonl,
)
from .report import mark_stage, patch_report, report_path


def summarize_fetch_error(error_text: str) -> tuple[str, str]:
    text = (error_text or "").lower()
    if not text:
        return "", ""

    if "search_api_missing_key" in text:
        return "search_api_missing_key", "缺少 Search API 密钥"
    if "401" in text or "unauthorized" in text:
        return "auth_unauthorized", "鉴权失败（密钥无效或未授权）"
    if "403" in text or "forbidden" in text:
        return "access_forbidden", "目标站点拒绝访问"
    if "521" in text:
        return "origin_unreachable", "目标站点源站不可达"
    if "404" in text or "not found" in text:
        return "not_found", "页面不存在或路径失效"
    if "name or service not known" in text or "nodename nor servname provided" in text:
        return "dns_error", "域名解析失败"
    if "timed out" in text or "timeout" in text:
        return "timeout", "请求超时"
    if "remote end closed connection without response" in text:
        return "remote_closed", "目标站点连接被远端中断"
    if "http/2 stream" in text and "not closed cleanly" in text:
        return "upstream_h2_reset", "目标站点连接不稳定（HTTP/2 中断）"
    if "curl: (92)" in text:
        return "upstream_h2_reset", "目标站点连接不稳定（HTTP/2 中断）"
    if "ssl" in text or "handshake" in text or "certificate" in text:
        return "ssl_error", "SSL 握手或证书异常"
    if "connection reset" in text or "connection refused" in text:
        return "connection_error", "网络连接失败"
    if "invalid search provider" in text:
        return "invalid_provider", "搜索服务配置无效"
    if "invalid query_rss provider" in text:
        return "invalid_query_rss_provider", "查询 RSS 提供方配置无效"
    if "invalid search_result provider" in text:
        return "invalid_search_result_provider", "搜索结果提供方配置无效"
    if "invalid official_api provider" in text:
        return "invalid_official_api_provider", "官方 API 提供方配置无效"
    if "invalid query set" in text:
        return "invalid_query_set", "搜索查询配置无效"
    if "structured_web source missing entry_urls" in text:
        return "missing_entry_urls", "结构化源缺少入口配置"
    if "mismatched tag" in text:
        return "non_rss_or_challenge_page", "目标页不是有效 RSS（可能触发反爬挑战）"
    if "unsupported source_type" in text:
        return "unsupported_source_type", "不支持的数据源类型"
    if "incompleteread" in text:
        return "incomplete_read", "响应数据不完整（服务端提前关闭连接）"
    return "unknown_error", "抓取异常"


def _safe_text(node: ET.Element | None, path: str, default: str = "") -> str:
    if node is None:
        return default
    child = node.find(path)
    if child is None or child.text is None:
        return default
    return child.text.strip()


def _is_valid_xml_char(ch: str) -> bool:
    cp = ord(ch)
    if ch in ("\t", "\n", "\r"):
        return True
    if 0x20 <= cp <= 0xD7FF:
        return True
    if 0xE000 <= cp <= 0xFFFD:
        return True
    return 0x10000 <= cp <= 0x10FFFF


def _sanitize_xml_for_parse(xml_data: bytes) -> bytes:
    encoding = detect_xml_encoding(xml_data)
    text = xml_data.decode(encoding, errors="ignore")
    cleaned = "".join(ch for ch in text if _is_valid_xml_char(ch))
    return cleaned.encode("utf-8")


def _parse_rss_feed(xml_data: bytes, source_name: str) -> list[dict[str, str]]:
    encoding = detect_xml_encoding(xml_data)
    feed_bytes = xml_data
    if encoding != "utf-8":
        # Re-encode to UTF-8 so ET.fromstring can parse non-UTF-8 feeds (e.g. GBK).
        text = xml_data.decode(encoding, errors="ignore")
        # Strip the original encoding declaration so the parser defaults to UTF-8.
        text = re.sub(r'(<\?xml\b[^?]*)\bencoding=["\'][^"\']*["\']', r'\1', text, count=1)
        feed_bytes = text.encode("utf-8")
    try:
        root = ET.fromstring(feed_bytes)
    except ET.ParseError as exc:
        # 某些源会注入非法控制字符（如 \x05），先清洗后再尝试解析。
        if "invalid token" not in str(exc).lower():
            raise
        root = ET.fromstring(_sanitize_xml_for_parse(xml_data))
    out: list[dict[str, str]] = []

    rss_items = root.findall("./channel/item")
    for node in rss_items:
        title = _safe_text(node, "title")
        summary = _safe_text(node, "description") or _safe_text(node, "{*}encoded")
        link = _safe_text(node, "link")
        published = _safe_text(node, "pubDate") or _safe_text(node, "{*}date")
        src = _safe_text(node, "source", source_name) or source_name
        if title and link:
            out.append(
                {
                    "title": clean_text(title),
                    "summary": clean_text(summary),
                    "content": clean_text(summary),
                    "link": link.strip(),
                    "published": published,
                    "source_name": clean_text(src),
                }
            )

    atom_entries = root.findall("./{*}entry")
    for entry in atom_entries:
        title = _safe_text(entry, "{*}title")
        summary = _safe_text(entry, "{*}summary") or _safe_text(entry, "{*}content")
        link = ""
        for lk in entry.findall("{*}link"):
            rel = (lk.attrib.get("rel") or "alternate").lower()
            href = (lk.attrib.get("href") or "").strip()
            if rel in {"", "alternate"} and href:
                link = href
                break
        published = _safe_text(entry, "{*}published") or _safe_text(entry, "{*}updated")
        if title and link:
            out.append(
                {
                    "title": clean_text(title),
                    "summary": clean_text(summary),
                    "content": clean_text(summary),
                    "link": link,
                    "published": published,
                    "source_name": source_name,
                }
            )

    return out


def fetch_rss_source(source: dict[str, Any]) -> tuple[list[dict[str, str]], str]:
    rows: list[dict[str, str]] = []
    errors: list[str] = []
    custom_headers = source.get("headers") if isinstance(source.get("headers"), dict) else None
    max_items = source.get("max_items")
    for url in source.get("rss_urls", []):
        try:
            data = http_get_bytes(str(url), headers=custom_headers, timeout=20, retries=3)
            rows.extend(_parse_rss_feed(data, str(source.get("name", ""))))
        except Exception as exc:
            errors.append(f"[{url}] {exc}")
            continue
    if max_items is not None and isinstance(max_items, int) and max_items > 0:
        rows = rows[:max_items]
    return rows, "; ".join(errors)


def _parse_serpapi(payload: dict[str, Any], source_name: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for item in payload.get("news_results", []):
        if not isinstance(item, dict):
            continue
        title = clean_text(str(item.get("title", "")))
        link = str(item.get("link", "")).strip()
        if not title or not link:
            continue
        out.append(
            {
                "title": title,
                "summary": clean_text(str(item.get("snippet", ""))),
                "content": clean_text(str(item.get("snippet", ""))),
                "link": link,
                "published": str(item.get("date", "")),
                "source_name": clean_text(str(item.get("source", ""))) or source_name,
            }
        )
    return out


def fetch_search_api_source(source: dict[str, Any], cfg: dict[str, Any]) -> tuple[list[dict[str, str]], str]:
    provider_name = str(source.get("provider", "")).strip()
    providers = cfg.get("search_providers", {})
    provider = providers.get(provider_name, {}) if isinstance(providers, dict) else {}
    if not isinstance(provider, dict):
        return [], "invalid search provider"

    if not bool(provider.get("enabled", True)):
        return [], ""

    api_key_env = str(provider.get("api_key_env", "SERPAPI_API_KEY")).strip()
    api_key = __import__("os").environ.get(api_key_env, "").strip() if api_key_env else ""
    if not api_key or api_key.lower().startswith("serpapi key"):
        return [], "search_api_missing_key"

    endpoint = str(provider.get("endpoint", "https://serpapi.com/search.json")).strip()
    engine = str(provider.get("engine", "google_news")).strip()
    query_set_name = str(source.get("query_set", "")).strip()
    query_sets = cfg.get("query_sets", {})
    query_rows = query_sets.get(query_set_name, []) if isinstance(query_sets, dict) else []
    if not isinstance(query_rows, list):
        return [], "invalid query set"

    max_results = int(source.get("max_results_per_query", provider.get("num", 10)))
    all_rows: list[dict[str, str]] = []
    errors: list[str] = []

    for row in query_rows:
        query, extra = _extract_query_row(row)

        if not query:
            continue

        params = {
            "engine": engine,
            "q": query,
            "api_key": api_key,
            "num": max_results,
        }
        for key in ("hl", "gl", "ceid", "location"):
            val = extra.get(key)
            if val:
                params[key] = str(val)
        q = "&".join(f"{k}={quote_plus(str(v))}" for k, v in params.items())

        try:
            payload = http_get_json(f"{endpoint}?{q}", timeout=25, retries=3)
            all_rows.extend(_parse_serpapi(payload, str(source.get("name", ""))))
        except Exception as exc:
            errors.append(f"[query={query}] {exc}")
            continue

    return all_rows, "; ".join(errors)


def _extract_query_row(row: Any) -> tuple[str, dict[str, Any]]:
    query = ""
    extra: dict[str, Any] = {}
    if isinstance(row, str):
        query = row.strip()
    elif isinstance(row, dict):
        query = str(row.get("q", "")).strip()
        extra = {k: v for k, v in row.items() if k != "q"}
    return query, extra


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


def _decode_toutiao_jump_url(url: str) -> str:
    """递归展开头条搜索的 jump 链接，尽量拿到真实文章地址。"""
    current = (url or "").strip()
    seen: set[str] = set()
    while current and "sou.toutiao.com/search/jump" in current and current not in seen:
        seen.add(current)
        parsed = urlparse(current)
        nested = parse_qs(parsed.query).get("url", [""])[0].strip()
        if not nested:
            break
        current = unquote(nested).strip()
    return current


def _extract_result_time_text(text: str) -> str:
    compact = clean_text(text)
    if not compact:
        return ""
    patterns = [
        r"\b\d+\s*(?:m|min|h|d)\b",
        r"\b\d+\s*(?:minutes?|hours?|days?)\s+ago\b",
        r"\b\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2})?\b",
        r"\b\d{4}/\d{1,2}/\d{1,2}(?:\s+\d{1,2}:\d{2})?\b",
        r"\b\d{1,2}月\d{1,2}日(?:\s+\d{1,2}:\d{2})?\b",
        r"\b\d+\s*(?:分钟前|小时前|天前)\b",
        r"\b昨天(?:\s+\d{1,2}:\d{2})?\b",
    ]
    for pattern in patterns:
        m = re.search(pattern, compact, flags=re.IGNORECASE)
        if m:
            return m.group(0).strip()
    return ""


def _parse_bing_news_results(html_text: str, source_name: str, query: str, max_results: int) -> list[dict[str, str]]:
    soup = BeautifulSoup(html_text, "html.parser")
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for rank, card in enumerate(soup.select(".news-card.newsitem.cardcommon"), start=1):
        link = (card.get("data-url") or "").strip()
        title = clean_text(card.get("data-title") or "")
        if not link or not title or link in seen:
            continue
        seen.add(link)
        snippet = clean_text(card.select_one(".snippet").get_text(" ", strip=True) if card.select_one(".snippet") else "")
        source_block = card.select_one(".source")
        source_name_text = clean_text(card.get("data-author") or "") or source_name
        display_time = ""
        if source_block is not None:
            raw_parts = []
            for s in source_block.select("span"):
                raw_parts.append(clean_text(s.get("aria-label") or s.get_text(" ", strip=True)))
            raw_parts = [part for part in raw_parts if part]
            if raw_parts:
                display_time = _extract_result_time_text(" ".join(raw_parts))
                source_candidates = [part for part in raw_parts if part != display_time]
                if source_candidates:
                    source_name_text = source_candidates[0]
        rows.append(
            {
                "title": title,
                "summary": snippet,
                "content": snippet,
                "link": link,
                "published": display_time,
                "source_name": source_name_text,
                "search_provider": "bing_news",
                "search_query": query,
                "search_display_time": display_time,
                "search_rank": str(rank),
            }
        )
        if len(rows) >= max_results:
            break
    return rows


def _parse_toutiao_news_results(html_text: str, source_name: str, query: str, max_results: int) -> list[dict[str, str]]:
    soup = BeautifulSoup(html_text, "html.parser")
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for rank, card in enumerate(soup.select('div[data-test-card-id="undefined-self_article"]'), start=1):
        link_node = card.select_one("div.cs-header a[href]")
        if link_node is None:
            continue
        raw_link = (link_node.get("href") or "").strip()
        link = _decode_toutiao_jump_url(raw_link)
        title = clean_text(link_node.get_text(" ", strip=True))
        if not link or not title or link in seen:
            continue
        seen.add(link)
        summary_node = card.select_one("div.text-default.text-m.text-regular span")
        snippet = clean_text(summary_node.get_text(" ", strip=True) if summary_node else "")
        source_name_text = source_name
        display_time = ""
        source_wrapper = card.select_one(".cs-source-wrapper")
        if source_wrapper is not None:
            raw_parts = [clean_text(s.get_text(" ", strip=True)) for s in source_wrapper.select("span")]
            raw_parts = [part for part in raw_parts if part]
            if raw_parts:
                display_time = _extract_result_time_text(" ".join(raw_parts))
                source_candidates = [part for part in raw_parts if part != display_time]
                if source_candidates:
                    source_name_text = source_candidates[0]
        rows.append(
            {
                "title": title,
                "summary": snippet,
                "content": snippet,
                "link": link,
                "published": display_time,
                "source_name": source_name_text,
                "search_provider": "toutiao_news",
                "search_query": query,
                "search_display_time": display_time,
                "search_rank": str(rank),
            }
        )
        if len(rows) >= max_results:
            break
    return rows


# ---------------------------------------------------------------------------
#  Google News URL Resolver
# ---------------------------------------------------------------------------

def _extract_gnews_token(url: str) -> str | None:
    """Extract the base64 article token from a Google News URL."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host.endswith("news.google.com"):
        return None
    parts = parsed.path.strip("/").split("/")
    # /rss/articles/TOKEN  or  /articles/TOKEN
    if len(parts) >= 2 and parts[-2] in ("articles", "read"):
        return parts[-1]
    return None


def _token_decode(token: str) -> str | None:
    """Decode old-style Google News protobuf token to extract embedded URL."""
    padded = token + "==="
    try:
        raw = base64.urlsafe_b64decode(padded)
    except Exception:
        return None

    prefix = b"\x08\x13\x22"
    if not raw.startswith(prefix):
        return None

    data = raw[len(prefix):]
    if len(data) < 2:
        return None

    length = data[0]
    offset = 1
    if length >= 0x80:
        if len(data) < 2:
            return None
        length = (data[0] & 0x7F) | (data[1] << 7)
        offset = 2

    if len(data) < offset + length:
        return None

    url = data[offset : offset + length].decode("utf-8", errors="ignore")
    if url.startswith(("http://", "https://")):
        return url
    return None


_GOOGLE_DOMAINS = {"google.com", "gstatic.com", "googleapis.com", "googleusercontent.com", "googlesyndication.com", "google-analytics.com", "googletagmanager.com", "doubleclick.net"}


def _is_google_domain(href: str) -> bool:
    """Check if a URL belongs to any Google-owned domain."""
    try:
        host = urlparse(href).hostname or ""
        return any(host == d or host.endswith(f".{d}") for d in _GOOGLE_DOMAINS)
    except Exception:
        return False


def _html_extract(token: str) -> str | None:
    """Fetch Google News wrapper page and extract real article URL.

    Tries batchexecute API first (for new-style tokens), then falls back
    to scraping <a> tags and meta redirects from the wrapper page.
    """
    # --- Try batchexecute approach for new-style tokens ---
    resolved = _batchexecute_resolve(token)
    if resolved and not _is_google_domain(resolved):
        return resolved

    # --- Fallback: scrape the wrapper page ---
    for path in (f"/rss/articles/{token}", f"/articles/{token}"):
        url = f"https://news.google.com{path}"
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=15) as resp:
                html = resp.read().decode("utf-8", errors="replace")

            soup = BeautifulSoup(html, "html.parser")

            # Look for <a> tags pointing outside Google domains
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if href.startswith(("http://", "https://")):
                    return href

            # <meta http-equiv="refresh" content="0;url=...">
            meta = soup.find("meta", attrs={"http-equiv": "refresh"})
            if meta and meta.get("content"):
                m = re.search(r"url=(.+)", str(meta["content"]), re.IGNORECASE)
                if m:
                    target = m.group(1).strip()
                    return target

            # data-href on any element
            for tag in soup.find_all(attrs={"data-href": True}):
                href = tag["data-href"]
                if href.startswith(("http://", "https://")):
                    return href

        except Exception:
            continue

    return None


def _batchexecute_resolve(token: str) -> str | None:
    """Use Google's batchexecute API to resolve new-style tokens."""
    # Step 1: fetch wrapper page to get signature and timestamp
    try:
        req = Request(
            f"https://news.google.com/articles/{token}",
            headers={"User-Agent": USER_AGENT},
        )
        with urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception:
        return None

    soup = BeautifulSoup(html, "html.parser")
    div = soup.select_one("c-wiz > div[jscontroller]") or soup.select_one("c-wiz > div")
    if div is None:
        return None

    signature = div.get("data-n-a-sg")
    timestamp = div.get("data-n-a-ts")
    if not signature or not timestamp:
        return None

    # Step 2: POST to batchexecute
    inner_payload = (
        f'["garturlreq",'
        f'[["X","X",["X","X"],null,null,1,1,"US:en",'
        f'null,1,null,null,null,null,null,0,1],'
        f'"X","X",1,[1,1,1],1,1,null,0,0,null,0],'
        f'"{token}",{timestamp},"{signature}"]'
    )
    payload = [["Fbv4je", inner_payload]]
    body = f"f.req={quote(json.dumps([payload]))}"

    try:
        req = Request(
            "https://news.google.com/_/DotsSplashUi/data/batchexecute",
            data=body.encode("utf-8"),
            headers={
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                "User-Agent": USER_AGENT,
            },
            method="POST",
        )
        with urlopen(req, timeout=15) as resp:
            response_text = resp.read().decode("utf-8", errors="replace")

        parts = response_text.split("\n\n", 1)
        if len(parts) < 2:
            return None
        parsed = json.loads(parts[1])
        inner = json.loads(parsed[0][2])
        url = inner[1] if isinstance(inner, list) and len(inner) > 1 else None
        if url and isinstance(url, str) and url.startswith(("http://", "https://")):
            return url
    except Exception:
        pass

    return None


def _is_valid_resolved_url(url: str) -> bool:
    """Validate that a resolved URL looks like a legitimate article URL."""
    if not url:
        return False
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https") or not p.netloc or "." not in p.netloc:
            return False
        # Reject Google infrastructure domains
        if _is_google_domain(url):
            return False
        return True
    except Exception:
        return False


def resolve_google_news_url(source_url: str) -> tuple[str, bool, str, bool]:
    """Resolve a Google News encoded URL to the real article URL.

    Returns (resolved_url, resolved_ok, resolver_method, token_decode_ok).
    resolver_method is one of:
    token_decode, html_extract, not_google_news,
    failed_html_extract, failed_google_link_left, failed.
    """
    token = _extract_gnews_token(source_url)
    if token is None:
        # Not a Google News URL — treat the URL as-is
        if _is_valid_resolved_url(source_url):
            return source_url, True, "not_google_news", False
        return "", False, "failed", False

    # Attempt 1: direct protobuf decode (old-style tokens, no network)
    url = _token_decode(token)
    if url and _is_valid_resolved_url(url):
        return url, True, "token_decode", True

    # Attempt 2: HTML extract (batchexecute + page scraping)
    url = _html_extract(token)
    if url and _is_valid_resolved_url(url):
        return url, True, "html_extract", False
    if url and _is_google_domain(url):
        return "", False, "failed_google_link_left", False

    return "", False, "failed_html_extract", False


def fetch_query_rss_source(source: dict[str, Any], cfg: dict[str, Any]) -> tuple[list[dict[str, str]], str]:
    provider_name = str(source.get("provider", "google_news")).strip().lower()
    if provider_name != "google_news":
        return [], "invalid query_rss provider"

    query_set_name = str(source.get("query_set", "")).strip()
    query_sets = cfg.get("query_sets", {})
    query_rows = query_sets.get(query_set_name, []) if isinstance(query_sets, dict) else []
    if not isinstance(query_rows, list):
        return [], "invalid query set"

    defaults = cfg.get("defaults", {}) if isinstance(cfg, dict) else {}
    if not isinstance(defaults, dict):
        defaults = {}
    default_max = int(defaults.get("discovery_max_results_per_query", 30))
    recency_token = str(defaults.get("discovery_query_recency", "when:1d")).strip()
    max_results = int(source.get("max_results_per_query", default_max))
    all_rows: list[dict[str, str]] = []
    errors: list[str] = []

    for row in query_rows:
        query, extra = _extract_query_row(row)
        if not query:
            continue
        query_group = str(extra.get("group", "")).strip().lower()
        query = _inject_recency_token(query, recency_token)

        params = {
            "q": query,
            "hl": str(extra.get("hl", source.get("hl", "en"))),
            "gl": str(extra.get("gl", source.get("gl", "us"))),
            "ceid": str(extra.get("ceid", source.get("ceid", "US:en"))),
        }
        url = f"https://news.google.com/rss/search?{urlencode(params)}"

        try:
            data = http_get_bytes(url, timeout=25, retries=3)
            rows = _parse_rss_feed(data, str(source.get("name", "")))
            for item in rows[:max_results]:
                item["feed_published"] = str(item.get("published", "")).strip()
                item["discovery_query"] = query
                item["discovery_query_group"] = query_group

                # --- Google News URL resolver ---
                original_link = str(item.get("link", "")).strip()
                resolved_url, resolved_ok, resolver_method, token_decode_ok = resolve_google_news_url(original_link)
                if resolved_ok and resolved_url:
                    item["google_news_link"] = original_link
                    item["link"] = resolved_url
                else:
                    item["google_news_link"] = original_link
                item["resolved_url"] = resolved_url
                item["resolved_ok"] = str(resolved_ok)
                item["resolver_method"] = resolver_method
                item["resolver_token_decode_ok"] = str(token_decode_ok)

                all_rows.append(item)
        except Exception as exc:
            errors.append(f"[query={query}] {exc}")
            continue

    return all_rows, "; ".join(errors)


def fetch_search_result_source(source: dict[str, Any], cfg: dict[str, Any]) -> tuple[list[dict[str, str]], str]:
    provider_name = str(source.get("provider", "")).strip().lower()
    if provider_name not in {"bing_news", "toutiao_news"}:
        return [], "invalid search_result provider"

    query_set_name = str(source.get("query_set", "")).strip()
    query_sets = cfg.get("query_sets", {})
    query_rows = query_sets.get(query_set_name, []) if isinstance(query_sets, dict) else []
    if not isinstance(query_rows, list):
        return [], "invalid query set"

    max_results = int(source.get("max_results_per_query", 20))
    headers = {"User-Agent": USER_AGENT}
    all_rows: list[dict[str, str]] = []
    errors: list[str] = []

    for row in query_rows:
        query, extra = _extract_query_row(row)
        if not query:
            continue

        if provider_name == "bing_news":
            params = {"q": query}
            setlang = str(extra.get("setlang", source.get("setlang", "en"))).strip()
            mkt = str(extra.get("mkt", source.get("mkt", "en-US"))).strip()
            if setlang:
                params["setlang"] = setlang
            if mkt:
                params["mkt"] = mkt
            url = f"https://www.bing.com/news/search?{urlencode(params)}"
        else:
            params = {
                "keyword": query,
                "page_num": str(extra.get("page_num", 0)),
                "source": str(extra.get("source", "pagination")),
                "action_type": str(extra.get("action_type", "search_subtab_switch")),
                "pd": str(extra.get("pd", source.get("pd", "news"))),
                "dvpf": str(extra.get("dvpf", source.get("dvpf", "pc"))),
            }
            url = f"https://so.toutiao.com/search?{urlencode(params)}"

        try:
            html = http_get_bytes(url, headers=headers, timeout=20, retries=3).decode("utf-8", errors="ignore")
            if provider_name == "bing_news":
                rows = _parse_bing_news_results(html, str(source.get("name", "")), query, max_results)
            else:
                rows = _parse_toutiao_news_results(html, str(source.get("name", "")), query, max_results)
            all_rows.extend(rows)
        except Exception as exc:
            errors.append(f"[query={query}] {exc}")
            continue

    return all_rows, "; ".join(errors)


def _parse_federalregister(payload: dict[str, Any], source_name: str) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for item in payload.get("results", []):
        if not isinstance(item, dict):
            continue
        title = clean_text(str(item.get("title", "")))
        link = str(item.get("html_url", "")).strip()
        if not title or not link:
            continue
        content = clean_text(
            str(item.get("abstract", ""))
            or str(item.get("excerpt", ""))
            or str(item.get("summary", ""))
        )
        published = str(item.get("publication_date", "")).strip()
        attachment_link = str(item.get("pdf_url", "")).strip()
        out.append(
            {
                "title": title,
                "summary": content[:320],
                "content": content[:4000],
                "link": link,
                "published": published,
                "attachment_link": attachment_link,
                "source_name": source_name,
            }
        )
    return out


def fetch_official_api_source(source: dict[str, Any]) -> tuple[list[dict[str, str]], str]:
    provider_name = str(source.get("provider", "")).strip().lower()
    if provider_name != "federalregister":
        return [], "invalid official_api provider"

    endpoint = str(source.get("endpoint", "https://www.federalregister.gov/api/v1/documents.json")).strip()
    agency = str(source.get("agency_slug", "")).strip()
    term = str(source.get("query", "")).strip()
    per_page = int(source.get("max_results_per_query", 10))
    params: dict[str, Any] = {
        "order": "newest",
        "per_page": per_page,
    }
    if agency:
        params["conditions[agencies][]"] = agency
    if term:
        params["conditions[term]"] = term

    q = urlencode(params)
    try:
        payload = http_get_json(f"{endpoint}?{q}", timeout=25, retries=3)
    except Exception as exc:
        return [], str(exc)
    return _parse_federalregister(payload, str(source.get("name", ""))), ""


def _extract_links_css(list_url: str, html_text: str, selectors: dict[str, Any]) -> list[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    link_selector = str(selectors.get("article_link", "a"))
    links: list[str] = []
    seen = set()
    for node in soup.select(link_selector):
        href = (node.get("href") or "").strip()
        if not href:
            continue
        if href.lower().startswith(("javascript:", "mailto:")):
            continue
        abs_url = urljoin(list_url, href)
        abs_url = abs_url.replace(" ", "%20")
        if abs_url in seen:
            continue
        seen.add(abs_url)
        links.append(abs_url)
    return links


def _normalize_published_text(raw: str) -> str:
    text = clean_text(raw)
    if not text:
        return ""

    patterns = [
        r"\b\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:?\d{2})?\b",
        r"\b\d{4}-\d{2}-\d{2}\b",
        r"\b\d{4}/\d{1,2}/\d{1,2}\b",
        r"\b\d{4}\.\d{1,2}\.\d{1,2}\b",
        r"\b\d{4}年\d{1,2}月\d{1,2}日(?:\d{1,2}时\d{1,2}分?)?\b",
        r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(0).strip()
    return text.strip()


def _extract_attachment_link(article_url: str, soup: BeautifulSoup, selectors: dict[str, Any]) -> str:
    attachment_selector = str(selectors.get("attachment_link", "")).strip()
    nodes = soup.select(attachment_selector) if attachment_selector else soup.select('a[href$=".pdf"], a[href*=".pdf?"], a[href*="/download/"]')
    for node in nodes:
        href = (node.get("href") or "").strip()
        if not href:
            continue
        return urljoin(article_url, href)
    return ""


def _extract_published_from_jsonld(soup: BeautifulSoup) -> str:
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    for script in scripts:
        content = (script.string or script.get_text() or "").strip()
        if not content:
            continue
        try:
            payload = json.loads(content)
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
            if "article" in typ or "newsarticle" in typ or "blogposting" in typ:
                for key in ("datePublished", "dateCreated"):
                    value = _normalize_published_text(str(cur.get(key, "")))
                    if value:
                        return value
            stack.extend(cur.values())
    return ""


def _extract_head_published_text(soup: BeautifulSoup) -> str:
    title_node = soup.select_one("h1, .article-title, .entry-title, .wp-block-post-title, .press-artical-title")
    if title_node is None:
        return ""

    snippets: list[str] = []
    parent = title_node.parent
    if parent is not None:
        snippets.append(parent.get_text(" ", strip=True))
    snippets.append(title_node.get_text(" ", strip=True))

    text = clean_text(" ".join(snippets))
    if not text:
        return ""

    return _guess_published_from_text(text[:1200])


def _extract_article_css(article_url: str, html_text: str, selectors: dict[str, Any], source_name: str) -> dict[str, str]:
    soup = BeautifulSoup(html_text, "html.parser")

    title_selector = str(selectors.get("title", "h1"))
    content_selector = str(selectors.get("content", "article p"))
    date_selector = str(selectors.get("published", "time"))

    title_node = soup.select_one(title_selector)
    title = clean_text(title_node.get_text(" ", strip=True) if title_node else "")
    if not title:
        meta_title = soup.select_one('meta[name="ArticleTitle"], meta[property="og:title"], meta[name="title"]')
        if meta_title is not None:
            title = clean_text(meta_title.get("content") or "")
    if not title and soup.title is not None:
        title = clean_text(soup.title.get_text(" ", strip=True))

    content_nodes = soup.select(content_selector)
    content = clean_text(" ".join(n.get_text(" ", strip=True) for n in content_nodes))
    if not content:
        content = clean_text(soup.get_text(" ", strip=True))

    date_node = soup.select_one(date_selector)
    published = ""
    if date_node is not None:
        published = _normalize_published_text(
            date_node.get("datetime")
            or date_node.get("content")
            or date_node.get_text(" ", strip=True)
            or ""
        )
        if published:
            _, status = parse_datetime_with_status(published)
            if status != "ok":
                published = ""

    if not published:
        for sel in (
            'meta[property="article:published_time"]',
            'meta[name="publish_date"]',
            'meta[name="pubdate"]',
            'meta[name="PubDate"]',
            'meta[itemprop="datePublished"]',
            'meta[name="date"]',
        ):
            node = soup.select_one(sel)
            if node is None:
                continue
            candidate = _normalize_published_text(node.get("content") or "")
            if not candidate:
                continue
            _, status = parse_datetime_with_status(candidate)
            if status == "ok":
                published = candidate
                break

    if not published:
        candidate = _extract_published_from_jsonld(soup)
        if candidate:
            _, status = parse_datetime_with_status(candidate)
            if status == "ok":
                published = candidate

    if not published:
        candidate = _extract_head_published_text(soup)
        if candidate:
            _, status = parse_datetime_with_status(candidate)
            if status == "ok":
                published = candidate

    if not published:
        for sel in (
            ".article-meta .date",
            "article .date",
            ".field--name-field-nir-news-date",
            ".detail__meta",
            ".content__meta",
            ".pages-date",
            ".article-info",
            ".pubtime",
            ".info",
            ".date",
            ".time",
        ):
            node = soup.select_one(sel)
            if node is None:
                continue
            candidate = _normalize_published_text(node.get_text(" ", strip=True))
            if not candidate:
                continue
            _, status = parse_datetime_with_status(candidate)
            if status == "ok":
                published = candidate
                break

    if not published:
        text = soup.get_text(" ", strip=True)
        for candidate in (_guess_published_from_text(text[:1200]), _guess_published_from_text(text)):
            if not candidate:
                continue
            _, status = parse_datetime_with_status(candidate)
            if status == "ok":
                published = candidate
                break

    attachment_link = _extract_attachment_link(article_url, soup, selectors)

    return {
        "title": title,
        "summary": content[:320],
        "content": content[:4000],
        "link": article_url,
        "published": published,
        "attachment_link": attachment_link,
        "source_name": source_name,
    }


def _guess_published_from_text(text: str) -> str:
    compact = clean_text(text)
    if not compact:
        return ""
    patterns = [
        r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}\b",
        r"\b\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:?\d{2})?)?\b",
        r"\b\d{4}/\d{1,2}/\d{1,2}\b",
        r"\b\d{4}年\d{1,2}月\d{1,2}日\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, compact, flags=re.IGNORECASE)
        if match:
            return match.group(0).strip()
    return ""


def _extract_article_jsonld(article_url: str, html_text: str, source_name: str) -> dict[str, str]:
    soup = BeautifulSoup(html_text, "html.parser")
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    for script in scripts:
        content = (script.string or script.get_text() or "").strip()
        if not content:
            continue
        try:
            payload = json.loads(content)
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
                headline = clean_text(str(cur.get("headline", "")))
                body = clean_text(str(cur.get("articleBody", "")))
                published = str(cur.get("datePublished", ""))
                url = str(cur.get("url", "")).strip() or article_url
                if headline and url:
                    attachment_link = _extract_attachment_link(article_url, soup, {})
                    return {
                        "title": headline,
                        "summary": body[:320],
                        "content": body[:4000],
                        "link": url,
                        "published": published,
                        "attachment_link": attachment_link,
                        "source_name": source_name,
                    }
            stack.extend(cur.values())

    return _extract_article_css(article_url, html_text, {}, source_name)


def _extract_links_sitemap(xml_data: bytes) -> list[str]:
    links: list[str] = []
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError:
        return links

    for loc in root.findall(".//{*}loc"):
        if loc.text and loc.text.strip():
            links.append(loc.text.strip())
    return links


def fetch_structured_source(source: dict[str, Any]) -> tuple[list[dict[str, str]], str]:
    entry_urls = [str(u).strip() for u in source.get("entry_urls", []) if str(u).strip()]
    if not entry_urls:
        return [], "structured_web source missing entry_urls"

    extractor = str(source.get("extractor", "css_selector")).strip().lower()
    selectors = source.get("selectors", {})
    if not isinstance(selectors, dict):
        selectors = {}

    max_items = int(source.get("max_items_per_run", 8))
    article_links: list[str] = []
    last_err = ""

    for list_url in entry_urls:
        try:
            data = http_get_bytes(list_url, timeout=20, retries=3)
            if extractor == "sitemap":
                article_links.extend(_extract_links_sitemap(data))
            else:
                html_text = data.decode("utf-8", errors="ignore")
                article_links.extend(_extract_links_css(list_url, html_text, selectors))
        except Exception as exc:
            last_err = str(exc)
            continue

    clean_links: list[str] = []
    seen = set()
    for link in article_links:
        if link in seen:
            continue
        seen.add(link)
        clean_links.append(link)
    clean_links = clean_links[:max_items]

    rows: list[dict[str, str]] = []
    for article_url in clean_links:
        try:
            page = http_get_bytes(article_url, timeout=20, retries=3).decode("utf-8", errors="ignore")
            if extractor == "json_ld":
                record = _extract_article_jsonld(article_url, page, str(source.get("name", "")))
            else:
                record = _extract_article_css(article_url, page, selectors, str(source.get("name", "")))
            if record.get("title") and record.get("link"):
                rows.append(record)
        except Exception as exc:
            last_err = str(exc)
            continue

    return rows, last_err


def process_source(source: dict[str, Any], cfg: dict[str, Any], fetch_time: str) -> tuple[list[RawItem], SourceStat]:
    source_id = str(source.get("id", "")).strip()
    source_name = str(source.get("name", "")).strip()
    source_type = str(source.get("source_type", "rss")).strip().lower() or "rss"
    region = str(source.get("region", "foreign")).strip().lower()
    company_hint = str(source.get("source_company_id", "")).strip()

    rows: list[dict[str, str]] = []
    err = ""
    try:
        if source_type == "rss":
            rows, err = fetch_rss_source(source)
        elif source_type == "search_api":
            rows, err = fetch_search_api_source(source, cfg)
        elif source_type == "query_rss":
            rows, err = fetch_query_rss_source(source, cfg)
        elif source_type == "search_result":
            rows, err = fetch_search_result_source(source, cfg)
        elif source_type == "official_api":
            rows, err = fetch_official_api_source(source)
        elif source_type == "structured_web":
            rows, err = fetch_structured_source(source)
        else:
            err = f"unsupported source_type={source_type}"
    except Exception as exc:
        err = str(exc)

    raw_items = [
        RawItem(
            source_id=source_id,
            source_name=source_name,
            source_type=source_type,
            region=region,
            company_hint=company_hint,
            fetched_at=fetch_time,
            url=row.get("link", ""),
            payload=row,
        )
        for row in rows
        if row.get("title") and row.get("link")
    ]

    status = "ok"
    if err and not raw_items:
        status = "fail"
    elif err and raw_items:
        status = "partial"
    err_raw = err[:500]
    err_code, err_zh = summarize_fetch_error(err_raw)

    stat = SourceStat(
        source_id=source_id,
        source_name=source_name,
        source_type=source_type,
        status=status,
        fetched_items=len(raw_items),
        error=err_zh if err_zh else err_raw[:120],
        error_reason_code=err_code,
        error_reason_zh=err_zh,
        error_raw=err_raw,
    )
    return raw_items, stat


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fetch raw Robtaxi items from RSS/Search API/Search Result/Structured Web/Official API")
    p.add_argument("--date", default="", help="Date in YYYY-MM-DD; default uses Beijing date")
    p.add_argument("--sources", default="./sources.json", help="Path to sources config JSON")
    p.add_argument("--out", default="./artifacts/raw", help="Output root for raw jsonl")
    p.add_argument("--report", default="./artifacts/reports", help="Report root directory")
    return p


def main() -> int:
    args = build_arg_parser().parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    sources_path = Path(args.sources).expanduser().resolve()
    raw_root = Path(args.out).expanduser().resolve()
    report_root = Path(args.report).expanduser().resolve()
    report_file = report_path(report_root, date_text)

    try:
        cfg = read_json(sources_path)
    except Exception as exc:
        mark_stage(report_file, "fetch", "failed")
        patch_report(report_file, source_stats=[], fetch_error=str(exc)[:300])
        raise SystemExit(f"[fetch] invalid config: {exc}")

    sources = cfg.get("sources", []) if isinstance(cfg, dict) else []
    enabled_sources = [s for s in sources if isinstance(s, dict) and bool(s.get("enabled", True))]

    fetch_time = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    all_raw: list[RawItem] = []
    all_stats: list[SourceStat] = []

    # Parallel fetch — each process_source call is independent (no shared mutable state).
    results: list[tuple[list[RawItem], SourceStat] | None] = [None] * len(enabled_sources)

    def _fetch_one(idx: int, source: dict[str, Any]) -> tuple[int, list[RawItem], SourceStat]:
        raw_rows, stat = process_source(source, cfg, fetch_time)
        return idx, raw_rows, stat

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(_fetch_one, i, src): i
            for i, src in enumerate(enabled_sources)
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                idx, raw_rows, stat = future.result()
                results[idx] = (raw_rows, stat)
            except Exception as exc:
                idx = futures[future]
                src = enabled_sources[idx]
                stat = SourceStat(
                    source_id=str(src.get("id", "")),
                    source_name=str(src.get("name", "")),
                    source_type=str(src.get("source_type", "rss")),
                    status="fail",
                    fetched_items=0,
                    error=str(exc)[:120],
                    error_raw=str(exc)[:500],
                )
                results[idx] = ([], stat)

    # Preserve deterministic output order.
    for result in results:
        if result is not None:
            raw_rows, stat = result
            all_raw.extend(raw_rows)
            all_stats.append(stat)

    out_file = raw_root / date_text / "raw_items.jsonl"
    write_jsonl(out_file, to_dict_list(all_raw))

    fail_count = len([s for s in all_stats if s.status != "ok"])
    search_api_missing_key_count = len([s for s in all_stats if s.error_reason_code == "search_api_missing_key"])
    non_search_fail_count = len(
        [s for s in all_stats if s.status != "ok" and s.error_reason_code != "search_api_missing_key"]
    )
    discovery_items_raw_count = len([r for r in all_raw if r.source_type in {"query_rss", "search_result"}])
    search_result_raw_count = len([r for r in all_raw if r.source_type == "search_result"])
    query_rss_resolved_count = 0
    query_rss_resolve_fail_count = 0
    query_rss_resolve_failed_token_decode_count = 0
    query_rss_resolve_failed_html_extract_count = 0
    query_rss_resolve_failed_google_link_left_count = 0
    date_bj = date_text
    discovery_today_raw_count = 0
    for r in all_raw:
        if r.source_type == "search_result":
            raw_display_time = str((r.payload or {}).get("search_display_time", "")).strip()
            if raw_display_time:
                try:
                    dt = parse_datetime(raw_display_time)
                    if dt.astimezone(now_beijing().tzinfo or timezone.utc).date().isoformat() == date_bj:
                        discovery_today_raw_count += 1
                except Exception:
                    pass
            continue
        if r.source_type != "query_rss":
            continue
        resolver_method = str((r.payload or {}).get("resolver_method", "")).strip()
        token_decode_ok = str((r.payload or {}).get("resolver_token_decode_ok", "")).lower() == "true"
        resolved_ok = str((r.payload or {}).get("resolved_ok", "")).lower() == "true"
        if resolved_ok:
            query_rss_resolved_count += 1
        else:
            query_rss_resolve_fail_count += 1
        if not token_decode_ok and resolver_method != "not_google_news":
            query_rss_resolve_failed_token_decode_count += 1
        if resolver_method == "failed_html_extract":
            query_rss_resolve_failed_html_extract_count += 1
        if resolver_method == "failed_google_link_left":
            query_rss_resolve_failed_google_link_left_count += 1
        raw_published = str((r.payload or {}).get("published", "")).strip()
        if not raw_published:
            continue
        try:
            dt = parse_datetime(raw_published)
            if dt.astimezone(now_beijing().tzinfo or timezone.utc).date().isoformat() == date_bj:
                discovery_today_raw_count += 1
        except Exception:
            continue
    stage = "success" if fail_count == 0 else "partial"
    mark_stage(report_file, "fetch", stage)
    patch_report(
        report_file,
        source_stats=to_dict_list(all_stats),
        total_items_raw=len(all_raw),
        discovery_items_raw_count=discovery_items_raw_count,
        search_result_raw_count=search_result_raw_count,
        discovery_today_raw_count=discovery_today_raw_count,
        query_rss_resolved_count=query_rss_resolved_count,
        query_rss_resolve_fail_count=query_rss_resolve_fail_count,
        query_rss_resolve_failed_token_decode_count=query_rss_resolve_failed_token_decode_count,
        query_rss_resolve_failed_html_extract_count=query_rss_resolve_failed_html_extract_count,
        query_rss_resolve_failed_google_link_left_count=query_rss_resolve_failed_google_link_left_count,
        non_search_fail_count=non_search_fail_count,
        search_api_missing_key_count=search_api_missing_key_count,
        raw_output=str(out_file),
    )

    print(f"[fetch] date={date_text} sources={len(enabled_sources)} raw_items={len(all_raw)} failures={fail_count}")
    print(f"[fetch] output={out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
