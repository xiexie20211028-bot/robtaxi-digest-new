#!/usr/bin/env python3
import argparse
import concurrent.futures
import html
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from http.client import IncompleteRead
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, quote_plus, unquote, urlparse, urljoin
from urllib.error import HTTPError
from urllib.request import Request, urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


USER_AGENT = "Mozilla/5.0 (RobtaxiDigest/1.0)"
TRANSLATE_CACHE: dict[str, str] = {}


@dataclass
class SourceDef:
    id: str
    name: str
    region: str
    tier: str
    category: str
    enabled: bool
    source_company_id: str
    source_type: str
    rss_urls: list[str]
    provider: str
    query_set: str
    max_results_per_query: int


@dataclass
class SourceStatus:
    id: str
    name: str
    ok: bool
    error: str
    fetched_items: int
    kept_items: int
    query_runs: int
    api_hits: int


@dataclass
class NewsItem:
    title: str
    summary: str
    link: str
    source_name: str
    source_id: str
    region: str
    published: datetime
    company_id: str


def now_beijing() -> datetime:
    if ZoneInfo is None:
        return datetime.utcnow().replace(tzinfo=timezone.utc)
    return datetime.now(ZoneInfo("Asia/Shanghai"))


def load_sources_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"sources config not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def parse_datetime(value: str) -> datetime:
    if not value:
        return datetime.utcnow().replace(tzinfo=timezone.utc)
    value = value.strip()
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass

    try:
        iso = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return datetime.utcnow().replace(tzinfo=timezone.utc)


def safe_text(node: Optional[ET.Element], path: str, default: str = "") -> str:
    if node is None:
        return default
    found = node.find(path)
    if found is None or found.text is None:
        return default
    return found.text.strip()


def extract_atom_link(entry: ET.Element) -> str:
    for lk in entry.findall("{*}link"):
        rel = (lk.attrib.get("rel") or "alternate").lower()
        href = (lk.attrib.get("href") or "").strip()
        if rel in {"alternate", ""} and href:
            return href
    return ""


def clean_title(title: str) -> str:
    title = html.unescape(title)
    title = re.sub(r"\s+", " ", title)
    return title.strip()


def clean_summary(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\\s+", " ", text)
    return text.strip()


def fetch_xml(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    last_err = None
    current_url = url
    last_partial = b""
    for i in range(5):
        try:
            req = Request(current_url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=20) as resp:
                return resp.read()
        except IncompleteRead as e:
            if e.partial:
                last_partial = e.partial
            last_err = e
            time.sleep(1 + i)
        except HTTPError as e:
            if e.code == 308:
                loc = (e.headers.get("Location") or "").strip()
                if loc:
                    current_url = urljoin(current_url, loc)
                    continue
            last_err = e
            time.sleep(1 + i)
        except Exception as e:
            last_err = e
            time.sleep(1 + i)
    if last_partial:
        return last_partial
    raise RuntimeError(f"fetch failed for {url}: {last_err}")


def fetch_json(url: str) -> dict:
    req = Request(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json", "Accept-Encoding": "identity"},
    )
    last_err = None
    last_partial = b""
    for i in range(4):
        try:
            with urlopen(req, timeout=25) as resp:
                data = resp.read().decode("utf-8", errors="ignore")
            return json.loads(data)
        except IncompleteRead as e:
            if e.partial:
                last_partial = e.partial
                try:
                    return json.loads(e.partial.decode("utf-8", errors="ignore"))
                except Exception:
                    pass
            last_err = e
            time.sleep(1 + i)
        except Exception as e:
            last_err = e
            time.sleep(1 + i)
    if last_partial:
        try:
            return json.loads(last_partial.decode("utf-8", errors="ignore"))
        except Exception:
            pass
    raise RuntimeError(f"fetch json failed for {url}: {last_err}")


def parse_relative_datetime(text: str, now_utc: datetime) -> Optional[datetime]:
    t = (text or "").strip().lower()
    if not t:
        return None
    m = re.search(r"(\\d+)\\s+(minute|hour|day|week|month|year)s?\\s+ago", t)
    if not m:
        return None
    num = int(m.group(1))
    unit = m.group(2)
    if unit == "minute":
        return now_utc - timedelta(minutes=num)
    if unit == "hour":
        return now_utc - timedelta(hours=num)
    if unit == "day":
        return now_utc - timedelta(days=num)
    if unit == "week":
        return now_utc - timedelta(weeks=num)
    if unit == "month":
        return now_utc - timedelta(days=num * 30)
    if unit == "year":
        return now_utc - timedelta(days=num * 365)
    return None


def unwrap_google_link(link: str) -> str:
    try:
        p = urlparse(link)
        host = (p.netloc or "").lower()
        if "google." not in host:
            return link
        qs = parse_qs(p.query)
        for key in ("url", "u", "q"):
            vals = qs.get(key) or []
            if vals:
                candidate = unquote(vals[0]).strip()
                cp = urlparse(candidate)
                if cp.scheme in {"http", "https"} and cp.netloc:
                    return candidate
    except Exception:
        return link
    return link


def parse_serpapi_news_results(payload: dict, source: SourceDef, now_utc: datetime) -> tuple[list[NewsItem], int]:
    hits = payload.get("news_results", [])
    if not isinstance(hits, list):
        return [], 0

    out: list[NewsItem] = []
    for row in hits:
        if not isinstance(row, dict):
            continue
        title = clean_title(str(row.get("title", "")))
        summary = clean_summary(str(row.get("snippet", "")))
        link = unwrap_google_link(str(row.get("link", "")).strip())
        source_name = clean_title(str(row.get("source", ""))) or source.name
        date_text = str(row.get("date", "")).strip()
        published = parse_relative_datetime(date_text, now_utc) or parse_datetime(date_text)
        if title and link:
            out.append(
                NewsItem(
                    title=title,
                    summary=summary,
                    link=link,
                    source_name=source_name,
                    source_id=source.id,
                    region=source.region,
                    published=published,
                    company_id=source.source_company_id,
                )
            )
    return out, len(hits)


def fetch_search_api_items(source: SourceDef, cfg: dict, now_utc: datetime) -> tuple[list[NewsItem], int, int]:
    providers = cfg.get("search_providers", {})
    query_sets = cfg.get("query_sets", {})
    provider = providers.get(source.provider, {})
    if not isinstance(provider, dict):
        return [], 0, 0
    if not bool(provider.get("enabled", True)):
        return [], 0, 0
    endpoint = str(provider.get("endpoint", "https://serpapi.com/search.json")).strip()
    engine = str(provider.get("engine", "google_news")).strip()
    api_key_env = str(provider.get("api_key_env", "SERPAPI_API_KEY")).strip()
    api_key = str((os.environ.get(api_key_env, "") if api_key_env else "")).strip()
    if not api_key:
        return [], 0, 0

    query_rows = query_sets.get(source.query_set, [])
    if not isinstance(query_rows, list):
        return [], 0, 0

    default_num = int(provider.get("num", 10))
    max_per_query = source.max_results_per_query if source.max_results_per_query > 0 else default_num
    all_items: list[NewsItem] = []
    query_runs = 0
    api_hits = 0
    last_err = None

    for row in query_rows:
        if isinstance(row, str):
            q = row.strip()
            extra = {}
        elif isinstance(row, dict):
            q = str(row.get("q", "")).strip()
            extra = {k: v for k, v in row.items() if k != "q"}
        else:
            continue
        if not q:
            continue
        params = {
            "engine": engine,
            "q": q,
            "api_key": api_key,
            "num": max_per_query,
        }
        for k in ("hl", "gl", "ceid", "location"):
            v = extra.get(k)
            if v:
                params[k] = str(v)
        query = "&".join(f"{k}={quote_plus(str(v))}" for k, v in params.items())
        try:
            payload = fetch_json(f"{endpoint}?{query}")
            items, hits = parse_serpapi_news_results(payload, source, now_utc)
            all_items.extend(items)
            api_hits += hits
            query_runs += 1
        except Exception as e:
            last_err = e
            continue

    if query_runs == 0 and last_err is not None:
        raise RuntimeError(str(last_err))

    return all_items, query_runs, api_hits


def recover_rss_items_from_partial(data: bytes, source: SourceDef) -> list[NewsItem]:
    text = data.decode("utf-8", errors="ignore")
    blocks = re.findall(r"<item\\b.*?</item>", text, flags=re.S | re.I)
    items: list[NewsItem] = []
    for blk in blocks:
        title_m = re.search(r"<title>(.*?)</title>", blk, flags=re.S | re.I)
        link_m = re.search(r"<link>(.*?)</link>", blk, flags=re.S | re.I)
        pub_m = re.search(r"<pubDate>(.*?)</pubDate>", blk, flags=re.S | re.I)
        desc_m = re.search(r"<description>(.*?)</description>", blk, flags=re.S | re.I)
        src_m = re.search(r"<source[^>]*>(.*?)</source>", blk, flags=re.S | re.I)
        title = clean_title(html.unescape(title_m.group(1))) if title_m else ""
        summary = clean_summary(desc_m.group(1)) if desc_m else ""
        link = html.unescape(link_m.group(1)).strip() if link_m else ""
        pub = html.unescape(pub_m.group(1)).strip() if pub_m else ""
        source_name = clean_title(html.unescape(src_m.group(1))) if src_m else source.name
        if title and link:
            items.append(
                NewsItem(
                    title=title,
                    summary=summary,
                    link=link,
                    source_name=source_name,
                    source_id=source.id,
                    region=source.region,
                    published=parse_datetime(pub),
                    company_id=source.source_company_id,
                )
            )
    # Fallback for partial Atom feeds.
    if not items:
        entries = re.findall(r"<entry\\b.*?</entry>", text, flags=re.S | re.I)
        for ent in entries:
            title_m = re.search(r"<title[^>]*>(.*?)</title>", ent, flags=re.S | re.I)
            link_m = re.search(r"<link[^>]*href=[\"'](.*?)[\"']", ent, flags=re.S | re.I)
            pub_m = re.search(r"<updated>(.*?)</updated>", ent, flags=re.S | re.I) or re.search(
                r"<published>(.*?)</published>", ent, flags=re.S | re.I
            )
            title = clean_title(html.unescape(title_m.group(1))) if title_m else ""
            summary_m = re.search(r"<summary[^>]*>(.*?)</summary>", ent, flags=re.S | re.I)
            summary = clean_summary(summary_m.group(1)) if summary_m else ""
            link = html.unescape(link_m.group(1)).strip() if link_m else ""
            pub = html.unescape(pub_m.group(1)).strip() if pub_m else ""
            if title and link:
                items.append(
                    NewsItem(
                        title=title,
                        summary=summary,
                        link=link,
                        source_name=source.name,
                        source_id=source.id,
                        region=source.region,
                        published=parse_datetime(pub),
                        company_id=source.source_company_id,
                    )
                )
    return items


def parse_feed(url: str, source: SourceDef) -> list[NewsItem]:
    data = fetch_xml(url)
    try:
        root = ET.fromstring(data)
    except ET.ParseError:
        recovered = recover_rss_items_from_partial(data, source)
        if recovered:
            return recovered
        raise

    items: list[NewsItem] = []
    rss_items = root.findall("./channel/item")
    if rss_items:
        for node in rss_items:
            raw_title = safe_text(node, "title")
            raw_desc = safe_text(node, "description") or safe_text(node, "{*}encoded")
            link = safe_text(node, "link")
            pub = safe_text(node, "pubDate") or safe_text(node, "{*}date")
            source_name = safe_text(node, "source", source.name) or source.name
            title = clean_title(raw_title)
            summary = clean_summary(raw_desc)
            if title and link:
                items.append(
                    NewsItem(
                        title=title,
                        summary=summary,
                        link=link,
                        source_name=source_name,
                        source_id=source.id,
                        region=source.region,
                        published=parse_datetime(pub),
                        company_id=source.source_company_id,
                    )
                )
        return items

    atom_entries = root.findall("./{*}entry")
    for entry in atom_entries:
        raw_title = safe_text(entry, "{*}title")
        raw_summary = safe_text(entry, "{*}summary") or safe_text(entry, "{*}content")
        link = extract_atom_link(entry)
        pub = safe_text(entry, "{*}updated") or safe_text(entry, "{*}published")
        source_name = source.name
        title = clean_title(raw_title)
        summary = clean_summary(raw_summary)
        if title and link:
            items.append(
                NewsItem(
                    title=title,
                    summary=summary,
                    link=link,
                    source_name=source_name,
                    source_id=source.id,
                    region=source.region,
                    published=parse_datetime(pub),
                    company_id=source.source_company_id,
                )
            )
    return items


def normalize_title_key(title: str) -> str:
    key = title.lower().strip()
    key = re.sub(r"\(.*?\)", "", key)
    key = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", key)
    key = re.sub(r"\s+", " ", key).strip()
    return key


def dedupe_by_link(items: list[NewsItem]) -> list[NewsItem]:
    seen = set()
    out = []
    for item in items:
        k = item.link.strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(item)
    return out


def dedupe_by_title(items: list[NewsItem]) -> list[NewsItem]:
    seen = set()
    out = []
    for item in items:
        k = normalize_title_key(item.title)
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(item)
    return out


def is_direct_link(link: str) -> bool:
    try:
        p = urlparse(link)
    except Exception:
        return False
    if p.scheme not in {"http", "https"}:
        return False
    host = (p.netloc or "").lower()
    if host.endswith("news.google.com"):
        return False
    return True


def keyword_match_text(text: str, keywords: list[str]) -> bool:
    t = (text or "").lower()
    return any(k.lower() in t for k in keywords if k and k.strip())


def item_matches_keywords(item: NewsItem, keywords: list[str]) -> bool:
    return keyword_match_text(item.title, keywords) or keyword_match_text(item.summary, keywords)


def filter_window(items: list[NewsItem], now_bj: datetime, window_days: int) -> list[NewsItem]:
    cutoff = now_bj.astimezone(timezone.utc) - timedelta(days=window_days)
    return [x for x in items if x.published.astimezone(timezone.utc) >= cutoff]


def is_foreign_market_item(item: NewsItem) -> bool:
    text = f"{item.title} {item.source_name}".lower()
    if re.search(r"[\u4e00-\u9fff]", text):
        return False
    blocked = [" china", " chinese", "apollo go", "pony.ai", "weride"]
    return not any(b in text for b in blocked)


def source_defs_from_config(cfg: dict) -> list[SourceDef]:
    companies = {c.get("id"): c for c in cfg.get("companies", [])}
    result: list[SourceDef] = []
    for raw in cfg.get("sources", []):
        enabled = bool(raw.get("enabled", True))
        source_type = str(raw.get("source_type", "rss")).strip().lower() or "rss"
        source_company_id = str(raw.get("source_company_id", "")).strip()
        if source_company_id:
            comp = companies.get(source_company_id, {})
            if not bool(comp.get("newsroom", True)):
                enabled = False
        urls = [u.strip() for u in raw.get("rss_urls", []) if str(u).strip()]
        provider = str(raw.get("provider", "")).strip()
        query_set = str(raw.get("query_set", "")).strip()
        max_results_per_query = int(raw.get("max_results_per_query", 10))
        if source_type == "rss" and not urls:
            continue
        if source_type == "search_api" and (not provider or not query_set):
            continue
        result.append(
            SourceDef(
                id=str(raw.get("id", "")).strip(),
                name=str(raw.get("name", "")).strip(),
                region=str(raw.get("region", "foreign")).strip().lower(),
                tier=str(raw.get("tier", "B")).strip().upper(),
                category=str(raw.get("category", "media")).strip().lower(),
                enabled=enabled,
                source_company_id=source_company_id,
                source_type=source_type,
                rss_urls=urls,
                provider=provider,
                query_set=query_set,
                max_results_per_query=max_results_per_query,
            )
        )
    return [
        s
        for s in result
        if s.id and s.name and s.region in {"domestic", "foreign"} and s.source_type in {"rss", "search_api"} and s.enabled
    ]


def classify_foreign_company(item: NewsItem, company_patterns: dict[str, list[str]]) -> str:
    if item.company_id:
        return item.company_id
    text = f"{item.title} {item.source_name}".lower()
    for cid, patterns in company_patterns.items():
        for p in patterns:
            if p and p in text:
                return cid
    return "other"


def cap_items_by_company(items: list[NewsItem], company_patterns: dict[str, list[str]], cap: int, total: int) -> list[NewsItem]:
    out: list[NewsItem] = []
    counts: dict[str, int] = {}
    for item in items:
        cid = classify_foreign_company(item, company_patterns)
        c = counts.get(cid, 0)
        if c >= cap:
            continue
        out.append(item)
        counts[cid] = c + 1
        if len(out) >= total:
            break
    return out


def translate_to_zh(text: str) -> str:
    t = text.strip()
    if not t:
        return t
    if t in TRANSLATE_CACHE:
        return TRANSLATE_CACHE[t]
    api = (
        "https://translate.googleapis.com/translate_a/single"
        f"?client=gtx&sl=auto&tl=zh-CN&dt=t&q={quote_plus(t)}"
    )
    req = Request(api, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "identity"})
    last_err = None
    for i in range(3):
        try:
            with urlopen(req, timeout=15) as resp:
                data = resp.read().decode("utf-8", errors="ignore")
            payload = json.loads(data)
            zh = "".join(seg[0] for seg in payload[0] if seg and seg[0]).strip()
            if zh:
                TRANSLATE_CACHE[t] = zh
                return zh
        except Exception as e:
            last_err = e
            time.sleep(1 + i)
    print(f"[WARN] Translate failed for title: {t} ({last_err})", file=sys.stderr)
    TRANSLATE_CACHE[t] = t
    return t


def render_section(title: str, items: list[NewsItem]) -> str:
    if not items:
        return f"<h2>{title}</h2><p>今日暂无可用新闻。</p>"
    rows = [f"<h2>{title}</h2>", "<ol>"]
    for item in items:
        pub = item.published.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        display_title = item.title
        if "国外" in title:
            zh = translate_to_zh(item.title)
            if zh and zh != item.title:
                display_title = f"{zh} ({item.title})"
        rows.append(
            "<li>"
            f"<a href=\"{html.escape(item.link)}\" target=\"_blank\" rel=\"noopener noreferrer\">{html.escape(display_title)}</a>"
            f"<br><small>来源: {html.escape(item.source_name)} | 时间: {pub}</small>"
            "</li>"
        )
    rows.append("</ol>")
    return "\n".join(rows)


def render_footer(statuses: list[SourceStatus], domestic_fallback_triggered: bool) -> str:
    valid = [s for s in statuses if s.ok and s.kept_items > 0]
    failed = [s for s in statuses if not s.ok]
    total_query_runs = sum(s.query_runs for s in statuses)
    total_api_hits = sum(s.api_hits for s in statuses)
    failed_lines = "<li>无</li>"
    if failed:
        failed_lines = "\n".join(
            f"<li>{html.escape(s.name)} ({html.escape(s.id)}): {html.escape(s.error[:120])}</li>" for s in failed
        )
    return (
        "<hr>"
        f"<p><strong>今日有效源数量:</strong> {len(valid)} / {len(statuses)}</p>"
        f"<p><strong>Search API 查询次数:</strong> {total_query_runs} | <strong>命中条数:</strong> {total_api_hits}</p>"
        f"<p><strong>国内兜底触发:</strong> {'是' if domestic_fallback_triggered else '否'}</p>"
        "<p><strong>抓取失败源列表:</strong></p>"
        f"<ul>{failed_lines}</ul>"
    )


def build_html(
    domestic: list[NewsItem], foreign: list[NewsItem], statuses: list[SourceStatus], now_bj: datetime, domestic_fallback_triggered: bool
) -> str:
    generated = now_bj.strftime("%Y-%m-%d %H:%M:%S")
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>Robtaxi 行业日报 {now_bj.strftime('%Y-%m-%d')}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 24px auto; max-width: 960px; padding: 0 16px; color: #1f2937; }}
    h1 {{ margin-bottom: 8px; }}
    h2 {{ margin-top: 28px; }}
    .meta {{ color: #6b7280; margin-bottom: 16px; }}
    .count {{ color: #374151; font-size: 14px; margin-bottom: 8px; }}
    li {{ margin: 0 0 14px 0; }}
    a {{ color: #0369a1; text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <h1>Robtaxi 行业日报</h1>
  <div class="meta">更新时间（北京时间）: {generated}</div>
  <div class="count">国内 {len(domestic)} 条 | 国外 {len(foreign)} 条</div>
  {render_section('【国内 Robtaxi 最新动态】', domestic)}
  {render_section('【国外 Robtaxi 最新动态】', foreign)}
  {render_footer(statuses, domestic_fallback_triggered)}
</body>
</html>
"""


def process_sources(sources: list[SourceDef], cfg: dict, now_utc: datetime) -> tuple[list[NewsItem], list[SourceStatus]]:
    all_items: list[NewsItem] = []
    statuses: dict[str, SourceStatus] = {
        s.id: SourceStatus(
            id=s.id, name=s.name, ok=True, error="", fetched_items=0, kept_items=0, query_runs=0, api_hits=0
        )
        for s in sources
    }

    tasks: list[tuple[SourceDef, str, str]] = []
    for s in sources:
        if s.source_type == "rss":
            for u in s.rss_urls:
                tasks.append((s, "rss", u))
        elif s.source_type == "search_api":
            tasks.append((s, "search_api", ""))

    def worker(source: SourceDef, task_type: str, payload: str) -> tuple[str, list[NewsItem], str, int, int]:
        try:
            if task_type == "rss":
                return source.id, parse_feed(payload, source), "", 0, 0
            if task_type == "search_api":
                items, query_runs, api_hits = fetch_search_api_items(source, cfg, now_utc)
                return source.id, items, "", query_runs, api_hits
            return source.id, [], f"unsupported source_type: {task_type}", 0, 0
        except Exception as e:
            return source.id, [], str(e), 0, 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(worker, s, task_type, payload) for s, task_type, payload in tasks]
        for fut in concurrent.futures.as_completed(futures):
            sid, items, err, query_runs, api_hits = fut.result()
            st = statuses[sid]
            st.query_runs += query_runs
            st.api_hits += api_hits
            if err:
                st.ok = False
                if not st.error:
                    st.error = err
            else:
                st.fetched_items += len(items)
                all_items.extend(items)

    return all_items, list(statuses.values())


def compile_company_patterns(cfg: dict) -> dict[str, list[str]]:
    patterns: dict[str, list[str]] = {}
    for c in cfg.get("companies", []):
        cid = str(c.get("id", "")).strip()
        if not cid:
            continue
        aliases = [str(x).lower().strip() for x in c.get("aliases", []) if str(x).strip()]
        name = str(c.get("name", "")).lower().strip()
        if name:
            aliases.append(name)
        patterns[cid] = sorted(set(aliases))
    return patterns


def company_aliases(patterns: dict[str, list[str]]) -> list[str]:
    out = set()
    for vals in patterns.values():
        for v in vals:
            if len(v.strip()) >= 3:
                out.add(v.strip().lower())
    return sorted(out)


def aliases_by_region(cfg: dict, region: str) -> list[str]:
    out = set()
    for c in cfg.get("companies", []):
        if str(c.get("region", "")).lower().strip() != region:
            continue
        for a in c.get("aliases", []):
            s = str(a).strip().lower()
            if len(s) >= 2:
                out.add(s)
        name = str(c.get("name", "")).strip().lower()
        if name:
            out.add(name)
    return sorted(out)


def dedupe_sorted(items: list[NewsItem]) -> list[NewsItem]:
    return dedupe_by_title(dedupe_by_link(sorted(items, key=lambda x: x.published, reverse=True)))


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Robtaxi digest HTML from RSS + Search API sources")
    parser.add_argument("--top", type=int, default=0, help="Top N per section; 0 means use config default")
    parser.add_argument("--output", default="robtaxi_digest_latest.html", help="Output HTML path")
    parser.add_argument("--sources", default="sources.yaml", help="Path to sources config")
    parser.add_argument("--dry-run", action="store_true", help="Print summary only")
    parser.add_argument("--health-report", action="store_true", help="Print source health report and exit")
    args = parser.parse_args()

    sources_path = Path(args.sources).expanduser()
    if not sources_path.is_absolute():
        sources_path = Path.cwd() / sources_path
    cfg = load_sources_config(sources_path)

    now_bj = now_beijing()
    defaults = cfg.get("defaults", {})
    top_n = int(args.top or defaults.get("top_n", 12))
    window_days = int(defaults.get("window_days", 10))
    per_company_cap = int(defaults.get("per_company_cap_foreign", 3))

    domestic_keywords = [str(x) for x in defaults.get("domestic_keywords", ["robotaxi", "无人驾驶", "自动驾驶出租车"])]
    foreign_keywords = [
        str(x)
        for x in defaults.get(
            "foreign_keywords",
            ["robotaxi", "autonomous taxi", "self-driving taxi", "driverless taxi", "autonomous vehicle", "ride-hailing"],
        )
    ]

    all_sources = source_defs_from_config(cfg)
    if not all_sources:
        raise ValueError("No enabled sources in sources.yaml")

    if args.health_report:
        all_items, statuses = process_sources(all_sources, cfg, now_bj.astimezone(timezone.utc))
        kept_by_source = {s.id: 0 for s in statuses}
        for item in all_items:
            kept_by_source[item.source_id] = kept_by_source.get(item.source_id, 0) + 1
        for st in statuses:
            st.kept_items = kept_by_source.get(st.id, 0)
        print("id\\tstatus\\tfetched\\tkept\\tquery_runs\\tapi_hits\\terror")
        for st in sorted(statuses, key=lambda x: x.id):
            state = "ok" if st.ok else "fail"
            print(
                f"{st.id}\\t{state}\\t{st.fetched_items}\\t{st.kept_items}\\t"
                f"{st.query_runs}\\t{st.api_hits}\\t{st.error[:120]}"
            )
        return 0

    domestic_media_sources = [s for s in all_sources if s.region == "domestic" and s.category == "media" and s.tier == "A"]
    domestic_media_b_sources = [s for s in all_sources if s.region == "domestic" and s.category == "media" and s.tier != "A"]
    domestic_newsroom_sources = [s for s in all_sources if s.region == "domestic" and s.category == "newsroom"]
    foreign_sources = [s for s in all_sources if s.region == "foreign"]

    if not domestic_media_sources:
        domestic_media_sources = domestic_media_b_sources or [s for s in all_sources if s.region == "domestic"]
    if not foreign_sources:
        raise ValueError("No enabled foreign sources in sources.yaml")

    now_utc = now_bj.astimezone(timezone.utc)
    domestic_items_main, domestic_status_main = process_sources(domestic_media_sources, cfg, now_utc)
    foreign_items, foreign_status = process_sources(foreign_sources, cfg, now_utc)

    company_patterns = compile_company_patterns(cfg)
    domestic_aliases = aliases_by_region(cfg, "cn")
    foreign_aliases = aliases_by_region(cfg, "global")
    domestic_recent_main = filter_window(domestic_items_main, now_bj, window_days)
    domestic_recent_main = [x for x in domestic_recent_main if is_direct_link(x.link)]
    domestic = [x for x in domestic_recent_main if item_matches_keywords(x, domestic_keywords + domestic_aliases)]
    domestic = dedupe_sorted(domestic)[:top_n]

    domestic_fallback_triggered = False
    domestic_fallback_status: list[SourceStatus] = []
    if len(domestic) == 0 and domestic_newsroom_sources:
        domestic_fallback_triggered = True
        domestic_items_fb, domestic_fallback_status = process_sources(domestic_newsroom_sources, cfg, now_utc)
        domestic_recent_fb = filter_window(domestic_items_fb, now_bj, window_days)
        domestic_recent_fb = [x for x in domestic_recent_fb if is_direct_link(x.link)]
        domestic_fb = [x for x in domestic_recent_fb if item_matches_keywords(x, domestic_keywords + domestic_aliases)]
        domestic = dedupe_sorted(domestic_fb)[:top_n]

    foreign_recent = filter_window(foreign_items, now_bj, window_days)
    foreign_recent = [x for x in foreign_recent if is_direct_link(x.link)]
    foreign = [
        x
        for x in foreign_recent
        if (item_matches_keywords(x, foreign_keywords) or item_matches_keywords(x, foreign_aliases)) and is_foreign_market_item(x)
    ]

    foreign = dedupe_sorted(foreign)
    foreign = cap_items_by_company(foreign, company_patterns, cap=per_company_cap, total=top_n)

    statuses = domestic_status_main + foreign_status + domestic_fallback_status
    kept_by_source = {s.id: 0 for s in statuses}
    for item in domestic + foreign:
        kept_by_source[item.source_id] = kept_by_source.get(item.source_id, 0) + 1
    for st in statuses:
        st.kept_items = kept_by_source.get(st.id, 0)

    if args.dry_run:
        print(f"北京时间: {now_bj.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"国内: {len(domestic)} 条")
        print(f"国外: {len(foreign)} 条")
        print(f"国内兜底触发: {'是' if domestic_fallback_triggered else '否'}")
        print(f"Search API 查询次数: {sum(s.query_runs for s in statuses)}")
        print(f"Search API 命中条数: {sum(s.api_hits for s in statuses)}")
        print(f"有效源: {len([s for s in statuses if s.ok and s.kept_items > 0])}/{len(statuses)}")
        failed = [s.id for s in statuses if not s.ok]
        print(f"失败源: {', '.join(failed) if failed else '无'}")
        return 0

    html_text = build_html(domestic, foreign, statuses, now_bj, domestic_fallback_triggered)
    output_path = Path(args.output).expanduser()
    if not output_path.is_absolute():
        output_path = Path.cwd() / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html_text, encoding="utf-8")
    print(f"Digest HTML generated: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
