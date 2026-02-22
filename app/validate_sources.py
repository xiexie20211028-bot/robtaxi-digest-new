from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import urlparse

from .common import read_json


def is_http_url(url: str) -> bool:
    p = urlparse(url)
    return p.scheme in {"http", "https"} and bool(p.netloc)


def fail(msg: str) -> None:
    raise SystemExit(f"[ERROR] {msg}")


def validate_sources(cfg: dict) -> tuple[int, int]:
    if not isinstance(cfg.get("sources"), list):
        fail("sources must be a list")
    if not isinstance(cfg.get("companies"), list):
        fail("companies must be a list")

    company_ids = {str(c.get("id", "")).strip() for c in cfg["companies"] if isinstance(c, dict)}

    providers = cfg.get("search_providers", {})
    if not isinstance(providers, dict):
        fail("search_providers must be an object")
    query_sets = cfg.get("query_sets", {})
    if not isinstance(query_sets, dict):
        fail("query_sets must be an object")

    ids = set()
    for i, src in enumerate(cfg["sources"]):
        if not isinstance(src, dict):
            fail(f"sources[{i}] must be object")

        sid = str(src.get("id", "")).strip()
        if not sid:
            fail(f"sources[{i}] id is empty")
        if sid in ids:
            fail(f"duplicate source id: {sid}")
        ids.add(sid)

        if str(src.get("region", "")).strip().lower() not in {"domestic", "foreign"}:
            fail(f"sources[{i}] invalid region")

        stype = str(src.get("source_type", "rss")).strip().lower() or "rss"
        if stype not in {"rss", "search_api", "structured_web"}:
            fail(f"sources[{i}] invalid source_type: {stype}")

        company = str(src.get("source_company_id", "")).strip()
        if company and company not in company_ids:
            fail(f"sources[{i}] source_company_id not found in companies: {company}")

        if stype == "rss":
            urls = src.get("rss_urls", [])
            if not isinstance(urls, list) or not urls:
                fail(f"sources[{i}] rss_urls must be non-empty list")
            for u in urls:
                if not is_http_url(str(u)):
                    fail(f"sources[{i}] invalid rss url: {u}")

        elif stype == "search_api":
            provider = str(src.get("provider", "")).strip()
            qset = str(src.get("query_set", "")).strip()
            if provider not in providers:
                fail(f"sources[{i}] provider not found: {provider}")
            if qset not in query_sets:
                fail(f"sources[{i}] query_set not found: {qset}")

        elif stype == "structured_web":
            entry_urls = src.get("entry_urls", [])
            if not isinstance(entry_urls, list) or not entry_urls:
                fail(f"sources[{i}] entry_urls must be non-empty list")
            for u in entry_urls:
                if not is_http_url(str(u)):
                    fail(f"sources[{i}] invalid entry url: {u}")
            extractor = str(src.get("extractor", "css_selector")).strip().lower()
            if extractor not in {"css_selector", "json_ld", "sitemap"}:
                fail(f"sources[{i}] invalid extractor: {extractor}")
            selectors = src.get("selectors", {})
            if extractor in {"css_selector", "json_ld"} and not isinstance(selectors, dict):
                fail(f"sources[{i}] selectors must be object")

    return len(cfg["companies"]), len(cfg["sources"])


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate sources.json schema")
    parser.add_argument("config", nargs="?", default="./sources.json", help="Path to sources.json")
    args = parser.parse_args()

    cfg_path = Path(args.config).expanduser().resolve()
    if not cfg_path.exists():
        fail(f"config not found: {cfg_path}")

    cfg = read_json(cfg_path)
    companies, sources = validate_sources(cfg)
    print(f"[OK] config valid: {cfg_path}")
    print(f"companies={companies} sources={sources}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
