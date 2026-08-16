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
_ENGLISH_DMY_PATTERN = re.compile(
    r"\b(\d{1,2})\s+"
    r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
    r"\s+(\d{4})\b",
    flags=re.IGNORECASE,
)
_ISO_DATE_PATTERN = re.compile(r"\b(\d{4}-\d{2}-\d{2})(?:[T\s]\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:?\d{2})?)?\b")
_UNECE_AUTONOMOUS_TERMS = (
    "automated",
    "autonomous",
    "driverless",
    "self-driving",
    "ads",
    "wp.29",
    "grva",
)
_GOVUK_APS_TERMS = (
    "automated passenger services",
    "self-driving",
    "automated vehicle",
    "automated vehicles",
    "driverless",
    "no user-in-charge",
)
_MONTHS = {
    "jan": "01",
    "january": "01",
    "feb": "02",
    "february": "02",
    "mar": "03",
    "march": "03",
    "apr": "04",
    "april": "04",
    "may": "05",
    "jun": "06",
    "june": "06",
    "jul": "07",
    "july": "07",
    "aug": "08",
    "august": "08",
    "sep": "09",
    "sept": "09",
    "september": "09",
    "oct": "10",
    "october": "10",
    "nov": "11",
    "november": "11",
    "dec": "12",
    "december": "12",
}


def _html_to_text(html: str) -> str:
    text = re.sub(r"<script\b.*?</script>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style\b.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    return clean_text(text)


def _record_text(record: dict[str, Any]) -> str:
    return clean_text(" ".join(str(record.get(key, "")) for key in ("title", "summary", "content", "link"))).lower()


def _contains_any(text: str, terms: tuple[str, ...]) -> bool:
    lowered = text.lower()
    return any(term in lowered for term in terms)


def _english_dmy_to_iso(text: str) -> str:
    match = _ENGLISH_DMY_PATTERN.search(text)
    if not match:
        return ""
    day, month_text, year = match.groups()
    month = _MONTHS.get(month_text.lower(), "")
    if not month:
        return ""
    return f"{year}-{month}-{int(day):02d}"


def _first_iso_date(text: str) -> str:
    match = _ISO_DATE_PATTERN.search(text)
    return match.group(1) if match else ""


def _extract_unece_published(html: str) -> tuple[str, str]:
    text = _html_to_text(html)
    for pattern in (
        r"(?:published|date)\s*:?\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4})",
        r"(\d{1,2}\s+[A-Za-z]+\s+\d{4})",
    ):
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            value = _english_dmy_to_iso(match.group(1))
            if value:
                return value, "site_specific_date"
    value = _first_iso_date(html)
    if value:
        return value, "site_specific_date"
    return "", ""


_WERIDE_PUBLISHED_AT_PATTERN = re.compile(
    r'"publishedAt"\s*:\s*"([^"]+)"',
    flags=re.IGNORECASE,
)


def _extract_weride_published(html: str) -> tuple[str, str]:
    match = _WERIDE_PUBLISHED_AT_PATTERN.search(html)
    if match and match.group(1).strip():
        return match.group(1).strip(), "site_specific_date"
    return "", ""


def _extract_govuk_published(html: str) -> tuple[str, str]:
    text = _html_to_text(html)
    for pattern in (
        r"last\s+updated\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4})",
        r"updated\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4})",
        r"published\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4})",
    ):
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            value = _english_dmy_to_iso(match.group(1))
            if value:
                return value, "site_specific_date"
    value = _first_iso_date(html)
    if value:
        return value, "site_specific_date"
    return "", ""


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

    if source_id == "unece_vehicle_regulations_structured":
        return not _contains_any(_record_text(record), _UNECE_AUTONOMOUS_TERMS)

    if source_id == "govuk_automated_passenger_services_structured":
        return not _contains_any(_record_text(record), _GOVUK_APS_TERMS)

    return False



def extract_site_specific_published(source_id: str, html: str, url: str) -> tuple[str, str]:
    host = (urlparse(url).netloc or "").lower()

    if source_id == "unece_vehicle_regulations_structured" or host.endswith("unece.org"):
        return _extract_unece_published(html)

    if source_id == "govuk_automated_passenger_services_structured" or host.endswith("gov.uk"):
        return _extract_govuk_published(html)

    if source_id == "weride_news_structured" or host.endswith("weride.ai"):
        return _extract_weride_published(html)

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
