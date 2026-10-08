from __future__ import annotations

import json
import io
import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.common import USER_AGENT, clean_text, http_get_bytes, normalize_url, parse_datetime_with_status, utc_iso
from app.parse import _extract_date_from_html, _parse_with_region_tz


class GenericPageReader:
    """只维护通用网页读取能力，不增加站点级 CSS 适配器。"""

    def __init__(self, timeout: int = 20) -> None:
        self.timeout = timeout
        self.cache: dict[str, dict[str, Any]] = {}
        self.deadline: float | None = None

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
        # 财报关键数字常在表格中，不能只读取段落而静默丢掉财务表。
        paragraphs = root.select("p, table")
        text = " ".join(node.get_text(" ", strip=True) for node in paragraphs) if paragraphs else root.get_text(" ", strip=True)
        return clean_text(text)[:8000]

    def read(self, url: str) -> dict[str, Any]:
        normalized = normalize_url(url)
        if not normalized:
            return {"ok": False, "url": url, "error": "invalid_url"}
        if normalized in self.cache:
            return dict(self.cache[normalized])
        if self.deadline is not None and time.monotonic() >= self.deadline:
            return {"ok": False, "url": normalized, "error": "runtime_limit"}
        try:
            data = http_get_bytes(
                normalized,
                headers={"User-Agent": USER_AGENT},
                timeout=min(self.timeout, max(1, int(self.deadline - time.monotonic()))) if self.deadline is not None else self.timeout,
                retries=1 if self.deadline is not None else 2,
            )
        except Exception as exc:
            return {"ok": False, "url": normalized, "error": str(exc)[:200]}
        if data.startswith(b"%PDF"):
            try:
                from pypdf import PdfReader
                reader = PdfReader(io.BytesIO(data))
                text = " ".join((page.extract_text() or "") for page in reader.pages[:60])
                if len(text.strip()) < 100:
                    return {"ok": False, "url": normalized, "error": "pdf_text_unavailable"}
                raw_date = re.search(r"/(20\d{6})\d*(?:[^\d]|$)", normalized)
                date_text = raw_date.group(1) if raw_date else ""
                if date_text:
                    date_text = f"{date_text[:4]}-{date_text[4:6]}-{date_text[6:8]}"
                else:
                    match = re.search(r"20\d{2}[-年/]\d{1,2}[-月/]\d{1,2}", text)
                    date_text = match.group(0).replace("年", "-").replace("月", "-") if match else ""
                dt, status = _parse_with_region_tz(date_text, "domestic")
                page = {"ok": True, "url": normalized, "canonical_url": normalized, "title": str((reader.metadata or {}).get("/Title", "")) or text[:160], "publisher": urlparse(normalized).netloc, "published_at_utc": utc_iso(dt) if status == "ok" else "", "published_source": "filing_url" if raw_date else "pdf_text", "content": clean_text(text)[:30000], "source_origin": urlparse(normalized).netloc}
                self.cache[normalized] = page
                return dict(page)
            except Exception as exc:
                return {"ok": False, "url": normalized, "error": "pdf_read_failed:" + str(exc)[:120]}
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
        content = self._body(soup)
        origin_match = re.search(r"(?:文章来源|来源)[:：]\s*([^\s|，。]{2,40})", content)
        page = {
            "ok": True,
            "url": normalized,
            "canonical_url": self._canonical(soup, normalized),
            "title": clean_text(title),
            "publisher": clean_text(publisher),
            "published_at_utc": published,
            "published_source": date_source,
            "content": content,
            "source_origin": origin_match.group(1) if origin_match else "",
        }
        self.cache[normalized] = page
        return dict(page)
