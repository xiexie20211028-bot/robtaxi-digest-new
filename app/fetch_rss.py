from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from typing import Any

from .common import clean_text, detect_xml_encoding, http_get_bytes


_BARE_AMPERSAND_RE = re.compile(
    r"&(?!#(?:\d+|x[0-9a-fA-F]+);|[A-Za-z_][A-Za-z0-9_.:-]*;)"
)
_XML_PROTECTED_BLOCK_RE = re.compile(
    r"(<!\[CDATA\[.*?\]\]>|<!--.*?-->|<\?.*?\?>)",
    flags=re.DOTALL | re.IGNORECASE,
)



def summarize_fetch_error(error_text: str) -> tuple[str, str]:
    text = (error_text or "").lower()
    if not text:
        return "", ""

    if "search_api_missing_key" in text:
        return "search_api_missing_key", "缺少 Search API 密钥"
    if "401" in text or "unauthorized" in text:
        return "auth_unauthorized", "鉴权失败（密钥无效或未授权）"
    if "406" in text or "not acceptable" in text:
        return "http_not_acceptable", "目标页面不接受当前请求（HTTP 406）"
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
    if "non_rss_or_challenge_page" in text:
        return "non_rss_or_challenge_page", "目标页不是有效 RSS（可能触发反爬挑战）"
    if "invalid_xml" in text or "not well-formed" in text or "invalid token" in text or "mismatched tag" in text:
        return "invalid_xml", "RSS 或 Atom XML 格式无效"
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



def _decode_xml_as_utf8(xml_data: bytes) -> str:
    encoding = detect_xml_encoding(xml_data)
    text = xml_data.decode(encoding, errors="ignore")
    return re.sub(
        r'(<\?xml\b[^?]*)\bencoding=["\'][^"\']*["\']',
        r"\1",
        text,
        count=1,
        flags=re.IGNORECASE,
    )



def _looks_like_html_response(xml_data: bytes) -> bool:
    prefix = xml_data[:4096].decode("utf-8", errors="ignore").lstrip("\ufeff \t\r\n").lower()
    return prefix.startswith("<!doctype html") or bool(re.match(r"<html(?:\s|>)", prefix))



def _looks_like_feed_xml(text: str) -> bool:
    prefix = text[:4096]
    return bool(re.search(r"<(?:rss|feed|rdf:RDF)\b", prefix, flags=re.IGNORECASE))



def _escape_bare_ampersands(text: str) -> str:
    parts = _XML_PROTECTED_BLOCK_RE.split(text)
    for index in range(0, len(parts), 2):
        parts[index] = _BARE_AMPERSAND_RE.sub("&amp;", parts[index])
    return "".join(parts)



def _sanitize_xml_for_parse(xml_data: bytes) -> bytes:
    text = _decode_xml_as_utf8(xml_data)
    cleaned = "".join(ch for ch in text if _is_valid_xml_char(ch))
    cleaned = _escape_bare_ampersands(cleaned)
    return cleaned.encode("utf-8")



def _parse_rss_feed(xml_data: bytes, source_name: str) -> list[dict[str, str]]:
    if _looks_like_html_response(xml_data):
        raise ValueError("non_rss_or_challenge_page: received HTML response")

    feed_text = _decode_xml_as_utf8(xml_data)
    feed_bytes = feed_text.encode("utf-8")
    try:
        root = ET.fromstring(feed_bytes)
    except ET.ParseError:
        if not _looks_like_feed_xml(feed_text):
            raise ValueError("non_rss_or_challenge_page: response is not RSS or Atom")
        try:
            root = ET.fromstring(_sanitize_xml_for_parse(xml_data))
        except ET.ParseError as exc:
            raise ValueError(f"invalid_xml: {exc}") from exc
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
