from __future__ import annotations

import base64
import json
import re
from typing import Any
from urllib.parse import parse_qs, quote, quote_plus, unquote, urlencode, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

from .common import USER_AGENT, clean_text, http_get_bytes, http_get_json

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

