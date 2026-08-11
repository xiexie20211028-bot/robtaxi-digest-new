from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .common import clean_text, http_get_bytes, http_get_last_modified, parse_datetime_with_status
from .site_rules import (
    extract_site_specific_published,
    is_invalid_structured_record,
    normalize_site_specific_record,
    prefilter_structured_links,
)

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


def _extract_article_css(
    article_url: str,
    html_text: str,
    selectors: dict[str, Any],
    source_name: str,
    source_id: str = "",
) -> dict[str, str]:
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
    if source_id in {"unece_vehicle_regulations_structured", "govuk_automated_passenger_services_structured"}:
        candidate, _ = extract_site_specific_published(source_id, html_text, article_url)
        if candidate:
            _, status = parse_datetime_with_status(candidate)
            if status == "ok":
                published = candidate

    if not published and date_node is not None:
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
        candidate, _ = extract_site_specific_published(source_id, html_text, article_url)
        if candidate:
            _, status = parse_datetime_with_status(candidate)
            if status == "ok":
                published = candidate

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
    canonical_node = soup.select_one('link[rel="canonical"], meta[property="og:url"]')
    canonical_url = ""
    if canonical_node is not None:
        canonical_url = str(canonical_node.get("href") or canonical_node.get("content") or "").strip()
        canonical_url = urljoin(article_url, canonical_url) if canonical_url else ""
    outbound_urls: list[str] = []
    for node in soup.select("article a[href], main a[href]"):
        href = str(node.get("href", "")).strip()
        if not href:
            continue
        outbound = urljoin(article_url, href)
        if outbound.startswith(("http://", "https://")) and outbound not in outbound_urls:
            outbound_urls.append(outbound)

    return {
        "title": title,
        "summary": content[:320],
        "content": content[:4000],
        "link": article_url,
        "published": published,
        "attachment_link": attachment_link,
        "canonical_url": canonical_url or article_url,
        "outbound_urls": outbound_urls[:20],
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


def _extract_article_jsonld(article_url: str, html_text: str, source_name: str, source_id: str = "") -> dict[str, str]:
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
                        "canonical_url": url,
                        "outbound_urls": [],
                    }
            stack.extend(cur.values())

    return _extract_article_css(article_url, html_text, {}, source_name, source_id=source_id)


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


def fetch_structured_source(
    source: dict[str, Any],
    include_stats: bool = False,
) -> tuple[list[dict[str, str]], str] | tuple[list[dict[str, str]], str, dict[str, int]]:
    entry_urls = [str(u).strip() for u in source.get("entry_urls", []) if str(u).strip()]
    if not entry_urls:
        empty = ([], "structured_web source missing entry_urls", {"request_count": 0, "request_success_count": 0, "listed_items": 0})
        return empty if include_stats else empty[:2]

    extractor = str(source.get("extractor", "css_selector")).strip().lower()
    transport = source.get("transport", {}) if isinstance(source.get("transport", {}), dict) else {}
    index_transport = str(transport.get("index", "")).strip().lower()
    article_transport = str(transport.get("article", "")).strip().lower()
    if not index_transport:
        index_transport = "sitemap" if extractor == "sitemap" else "css"
    if not article_transport:
        article_transport = "jsonld" if extractor == "json_ld" else "css"
    selectors = source.get("selectors", {})
    if not isinstance(selectors, dict):
        selectors = {}

    max_items = int(source.get("max_items_per_run", 8))
    article_links: list[str] = []
    direct_article_urls = [str(u).strip() for u in source.get("article_urls", []) if str(u).strip()]
    last_err = ""
    request_count = 0
    request_success_count = 0
    cache_dir_text = str(source.get("_http_cache_dir", "")).strip()
    cache_dir = Path(cache_dir_text) if cache_dir_text else None
    domain_interval = float(source.get("domain_rate_limit_seconds", 0.25))

    for list_url in entry_urls:
        request_count += 1
        try:
            data = http_get_bytes(
                list_url, headers=source.get("request_headers"), timeout=20, retries=3,
                min_domain_interval=domain_interval, cache_dir=cache_dir
            )
            if index_transport == "sitemap":
                article_links.extend(_extract_links_sitemap(data))
            else:
                html_text = data.decode("utf-8", errors="ignore")
                article_links.extend(_extract_links_css(list_url, html_text, selectors))
            request_success_count += 1
        except Exception as exc:
            last_err = str(exc)
            continue

    source_id = str(source.get("id", "")).strip()
    article_links = prefilter_structured_links(source_id, article_links)
    article_links = direct_article_urls + article_links

    clean_links: list[str] = []
    seen = set()
    for link in article_links:
        if link in seen:
            continue
        seen.add(link)
        clean_links.append(link)
    clean_links = clean_links[:max_items]
    listed_items = len(clean_links)

    rows: list[dict[str, str]] = []
    for article_url in clean_links:
        request_count += 1
        try:
            if article_transport == "provider":
                published = _guess_published_from_text(article_url) or http_get_last_modified(article_url, timeout=10)
                filename = article_url.rstrip("/").split("/")[-1].split("?")[0]
                rows.append(
                    {
                        "title": f"{source.get('name', 'Official dataset')}: {filename}",
                        "summary": "官方结构化数据集更新",
                        "content": "官方结构化数据集更新；附件链接作为主证据。",
                        "link": article_url,
                        "published": published,
                        "attachment_link": article_url,
                        "canonical_url": article_url,
                        "outbound_urls": [],
                        "source_name": str(source.get("name", "")),
                    }
                )
                request_success_count += 1
                continue
            page = http_get_bytes(
                article_url, headers=source.get("request_headers"), timeout=20, retries=3,
                min_domain_interval=domain_interval, cache_dir=cache_dir
            ).decode("utf-8", errors="ignore")
            if article_transport == "jsonld":
                record = _extract_article_jsonld(article_url, page, str(source.get("name", "")), source_id=source_id)
            else:
                record = _extract_article_css(article_url, page, selectors, str(source.get("name", "")), source_id=source_id)
            record = dict(normalize_site_specific_record(source_id, record))
            request_success_count += 1
            if record.get("title") and record.get("link") and not is_invalid_structured_record(source_id, record):
                rows.append(record)
        except Exception as exc:
            last_err = str(exc)
            continue

    fetch_stats = {
        "request_count": request_count,
        "request_success_count": request_success_count,
        "listed_items": listed_items,
    }
    return (rows, last_err, fetch_stats) if include_stats else (rows, last_err)
