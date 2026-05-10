from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urlparse

from .common import clean_text, now_beijing


_AASTOCKS_INLINE_PATTERN = re.compile(
    r"ConvertToLocalTime\s*\(\s*\{\s*dt\s*:\s*'([^']+)'\s*\}\s*\)",
    flags=re.IGNORECASE | re.DOTALL,
)
_AASTOCKS_URL_PATTERN = re.compile(r"/aat(\d{2})(\d{2})(\d{2})", flags=re.IGNORECASE)
_SINGAPORE_LTA_RECENT_PATTERN = re.compile(
    r"/en/newsroom/(\d{4})/(\d{1,2})/(?:news-release|news-releases|media-replies)/",
    flags=re.IGNORECASE,
)


def prefilter_structured_links(source_id: str, links: list[str]) -> list[str]:
    if source_id != "singapore_lta_news_structured":
        return links

    current_year = now_beijing().year
    min_year = current_year - 1
    kept: list[tuple[tuple[int, int, int], str]] = []
    for link in links:
        match = _SINGAPORE_LTA_RECENT_PATTERN.search(link)
        if not match:
            continue
        year = int(match.group(1))
        month = int(match.group(2))
        if year < min_year:
            continue
        kept.append(((year, month, 0), link))

    kept.sort(key=lambda item: item[0], reverse=True)
    return [link for _, link in kept]



def is_invalid_structured_record(source_id: str, record: dict[str, str]) -> bool:
    title = clean_text(str(record.get("title", "")))
    link = str(record.get("link", "")).strip().lower()

    if source_id == "apollo_go_baidu_structured":
        if link.endswith("/news/apollo-self-driving") or "/news/apollo-self-driving" in link:
            return True

    if source_id == "california_dmv_news_structured":
        if link.rstrip("/") == "https://www.dmv.ca.gov/portal/news-and-media/news-releases":
            return True
        if link.endswith("/portal/news-and-media/news-releases/") or "/portal/news-and-media/news-releases/" in link:
            return True

    if source_id == "waymo_blog_structured":
        if title.lower() == "latest news":
            return True
        if "/blog/search" in link or "?t=" in link:
            return True

    if source_id == "singapore_lta_news_structured":
        if title.upper() == "LTA.GOV.SG":
            return True
        if "/2020/" in link or "/2019/" in link or "/2018/" in link:
            return True

    return False



def extract_site_specific_published(source_id: str, html: str, url: str) -> tuple[str, str]:
    _ = source_id
    host = (urlparse(url).netloc or "").lower()
    if "aastocks.com" not in host:
        return "", ""

    inline_match = _AASTOCKS_INLINE_PATTERN.search(html)
    if inline_match and inline_match.group(1).strip():
        return inline_match.group(1).strip(), "site_specific_date"

    url_match = _AASTOCKS_URL_PATTERN.search(url)
    if url_match:
        yy, mm, dd = url_match.groups()
        return f"20{yy}/{mm}/{dd}", "url_date"

    return "", ""



def normalize_site_specific_record(source_id: str, record: dict[str, Any]) -> dict[str, Any]:
    _ = source_id
    return record



def extract_jsonld_date(source_id: str, html: str) -> tuple[str, str]:
    if source_id != "california_dmv_news_structured":
        return "", ""

    jsonld_pattern = re.compile(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in jsonld_pattern.finditer(html):
        content = clean_text(match.group(1))
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
            for key in ("datePublished", "dateCreated"):
                value = str(cur.get(key, "")).strip()
                if value:
                    return value, "jsonld"
            stack.extend(cur.values())
    return "", ""
