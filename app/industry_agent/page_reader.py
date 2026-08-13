from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.common import USER_AGENT, clean_text, http_get_bytes, normalize_url, parse_datetime_with_status, utc_iso
from app.parse import _extract_date_from_html, _parse_with_region_tz


class GenericPageReader:
    """只维护通用网页读取能力，不增加站点级 CSS 适配器。"""

    def __init__(self, timeout: int = 20) -> None:
        self.timeout = timeout

    @staticmethod
    def _prefer_specific_url(original: str, extracted: str) -> str:
        """页面给出通用 viewer canonical 时保留包含文章标识的原链接。"""
        original_url = normalize_url(original)
        extracted_url = normalize_url(extracted)
        if not extracted_url:
            return original_url
        original_parts = urlparse(original_url)
        extracted_parts = urlparse(extracted_url)
        if original_parts.netloc.lower() != extracted_parts.netloc.lower():
            return extracted_url
        identity_tokens = re.findall(r"[a-z0-9]{10,}", original_parts.path.lower())
        extracted_path = extracted_parts.path.lower()
        generic_path = (
            extracted_path in {"", "/", "/index.html", "/index.htm"}
            or any(term in extracted_path for term in ("mobile-viewer", "article-viewer", "content-viewer"))
        )
        if identity_tokens and generic_path and not any(token in extracted_path for token in identity_tokens):
            return original_url
        return extracted_url

    @classmethod
    def _canonical(cls, soup: BeautifulSoup, url: str) -> str:
        node = soup.select_one('link[rel="canonical"]')
        if node and str(node.get("href", "")).strip():
            extracted = urljoin(url, str(node.get("href", "")).strip())
            return cls._prefer_specific_url(url, extracted)
        meta = soup.select_one('meta[property="og:url"]')
        if meta and str(meta.get("content", "")).strip():
            extracted = urljoin(url, str(meta.get("content", "")).strip())
            return cls._prefer_specific_url(url, extracted)
        return normalize_url(url)

    @staticmethod
    def _body(soup: BeautifulSoup) -> str:
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            try:
                payload = json.loads((script.string or script.get_text() or "").strip())
            except Exception:
                continue
            stack: list[Any] = [payload]
            while stack:
                current = stack.pop()
                if isinstance(current, list):
                    stack.extend(current)
                elif isinstance(current, dict):
                    body = clean_text(str(current.get("articleBody", "")))
                    if len(body) >= 120:
                        return body[:8000]
                    stack.extend(value for value in current.values() if isinstance(value, (dict, list)))
        root = soup.select_one("article") or soup.select_one("main") or soup.body
        if not root:
            return ""
        paragraphs = root.select("p")
        text = " ".join(node.get_text(" ", strip=True) for node in paragraphs) if paragraphs else root.get_text(" ", strip=True)
        return clean_text(text)[:8000]

    def read(self, url: str) -> dict[str, Any]:
        normalized = normalize_url(url)
        if not normalized:
            return {"ok": False, "url": url, "error": "invalid_url"}
        try:
            data = http_get_bytes(
                normalized,
                headers={"User-Agent": USER_AGENT},
                timeout=self.timeout,
                retries=2,
            )
        except Exception as exc:
            return {"ok": False, "url": normalized, "error": str(exc)[:200]}
        html = data.decode("utf-8", errors="ignore")
        soup = BeautifulSoup(html, "html.parser")
        title_node = soup.select_one('meta[property="og:title"]')
        title = str(title_node.get("content", "")).strip() if title_node else ""
        if not title and soup.title:
            title = soup.title.get_text(" ", strip=True)
        publisher_node = soup.select_one('meta[property="og:site_name"]')
        publisher = str(publisher_node.get("content", "")).strip() if publisher_node else ""
        publisher = publisher or (urlparse(normalized).netloc or "").lower()
        raw_date, date_source = _extract_date_from_html(html, normalized, publisher)
        published = ""
        if raw_date:
            dt, status = _parse_with_region_tz(raw_date, "domestic")
            if status == "ok":
                published = utc_iso(dt)
        return {
            "ok": True,
            "url": normalized,
            "canonical_url": self._canonical(soup, normalized),
            "title": clean_text(title),
            "publisher": clean_text(publisher),
            "published_at_utc": published,
            "published_source": date_source,
            "content": self._body(soup),
        }
