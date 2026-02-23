from __future__ import annotations

import argparse
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup

from .common import (
    RawItem,
    SourceStat,
    clean_text,
    http_get_bytes,
    http_get_json,
    now_beijing,
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
    if "404" in text or "not found" in text:
        return "not_found", "页面不存在或路径失效"
    if "name or service not known" in text or "nodename nor servname provided" in text:
        return "dns_error", "域名解析失败"
    if "timed out" in text or "timeout" in text:
        return "timeout", "请求超时"
    if "ssl" in text or "handshake" in text or "certificate" in text:
        return "ssl_error", "SSL 握手或证书异常"
    if "connection reset" in text or "connection refused" in text:
        return "connection_error", "网络连接失败"
    if "invalid search provider" in text:
        return "invalid_provider", "搜索服务配置无效"
    if "invalid query set" in text:
        return "invalid_query_set", "搜索查询配置无效"
    if "structured_web source missing entry_urls" in text:
        return "missing_entry_urls", "结构化源缺少入口配置"
    if "unsupported source_type" in text:
        return "unsupported_source_type", "不支持的数据源类型"
    return "unknown_error", "抓取异常"


def _safe_text(node: ET.Element | None, path: str, default: str = "") -> str:
    if node is None:
        return default
    child = node.find(path)
    if child is None or child.text is None:
        return default
    return child.text.strip()


def _parse_rss_feed(xml_data: bytes, source_name: str) -> list[dict[str, str]]:
    root = ET.fromstring(xml_data)
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
        published = _safe_text(entry, "{*}updated") or _safe_text(entry, "{*}published")
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
    last_err = ""
    for url in source.get("rss_urls", []):
        try:
            data = http_get_bytes(str(url), timeout=20, retries=3)
            rows.extend(_parse_rss_feed(data, str(source.get("name", ""))))
        except Exception as exc:
            last_err = str(exc)
            continue
    return rows, last_err


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
    last_err = ""

    for row in query_rows:
        query = ""
        extra: dict[str, Any] = {}
        if isinstance(row, str):
            query = row.strip()
        elif isinstance(row, dict):
            query = str(row.get("q", "")).strip()
            extra = {k: v for k, v in row.items() if k != "q"}

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
            last_err = str(exc)
            continue

    return all_rows, last_err


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


def _extract_article_css(article_url: str, html_text: str, selectors: dict[str, Any], source_name: str) -> dict[str, str]:
    soup = BeautifulSoup(html_text, "html.parser")

    title_selector = str(selectors.get("title", "h1"))
    content_selector = str(selectors.get("content", "article p"))
    date_selector = str(selectors.get("published", "time"))

    title_node = soup.select_one(title_selector)
    if title_node is None and soup.title is not None:
        title = clean_text(soup.title.get_text(" ", strip=True))
    else:
        title = clean_text(title_node.get_text(" ", strip=True) if title_node else "")

    content_nodes = soup.select(content_selector)
    content = clean_text(" ".join(n.get_text(" ", strip=True) for n in content_nodes))
    if not content:
        content = clean_text(soup.get_text(" ", strip=True))

    date_node = soup.select_one(date_selector)
    published = ""
    if date_node is not None:
        published = (date_node.get("datetime") or date_node.get_text(" ", strip=True) or "").strip()

    return {
        "title": title,
        "summary": content[:320],
        "content": content[:4000],
        "link": article_url,
        "published": published,
        "source_name": source_name,
    }


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
                    return {
                        "title": headline,
                        "summary": body[:320],
                        "content": body[:4000],
                        "link": url,
                        "published": published,
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
    p = argparse.ArgumentParser(description="Fetch raw Robtaxi items from RSS/Search API/Structured Web")
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

    for source in enabled_sources:
        raw_rows, stat = process_source(source, cfg, fetch_time)
        all_raw.extend(raw_rows)
        all_stats.append(stat)

    out_file = raw_root / date_text / "raw_items.jsonl"
    write_jsonl(out_file, to_dict_list(all_raw))

    fail_count = len([s for s in all_stats if s.status != "ok"])
    stage = "success" if fail_count == 0 else "partial"
    mark_stage(report_file, "fetch", stage)
    patch_report(
        report_file,
        source_stats=to_dict_list(all_stats),
        total_items_raw=len(all_raw),
        raw_output=str(out_file),
    )

    print(f"[fetch] date={date_text} sources={len(enabled_sources)} raw_items={len(all_raw)} failures={fail_count}")
    print(f"[fetch] output={out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
