from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import urlparse

from .common import read_json

ALLOWED_SOURCE_PROFILES = {"general_media", "industry_media", "newsroom", "regulator", "research"}
ALLOWED_RELEVANCE_MODES = {"high_precision", "balanced", "high_recall"}
ALLOWED_QUERY_RSS_PROVIDERS = {"google_news"}


def is_http_url(url: str) -> bool:
    p = urlparse(url)
    return p.scheme in {"http", "https"} and bool(p.netloc)


def fail(msg: str) -> None:
    raise SystemExit(f"[ERROR] {msg}")


def ensure_string_list(name: str, value: object) -> None:
    if not isinstance(value, list):
        fail(f"{name} must be a list")
    for idx, item in enumerate(value):
        if not isinstance(item, str):
            fail(f"{name}[{idx}] must be string")


def validate_defaults(cfg: dict) -> None:
    defaults = cfg.get("defaults", {})
    if not isinstance(defaults, dict):
        fail("defaults must be an object")

    mode = str(defaults.get("relevance_mode", "high_precision")).strip().lower()
    if mode not in ALLOWED_RELEVANCE_MODES:
        fail(f"defaults.relevance_mode invalid: {mode}")

    for key in (
        "domestic_keywords",
        "foreign_keywords",
        "core_keywords_domestic",
        "core_keywords_foreign",
        "context_keywords_domestic",
        "context_keywords_foreign",
        "brand_keywords_domestic",
        "brand_keywords_foreign",
        "exclude_keywords_domestic",
        "exclude_keywords_foreign",
        "allow_missing_published_profiles",
        "fast_pass_title_keywords_zh",
        "fast_pass_title_keywords_en",
    ):
        if key in defaults:
            ensure_string_list(f"defaults.{key}", defaults[key])

    for key in ("fast_pass_enabled", "fast_pass_require_company_or_context", "enable_general_media_source_cap"):
        if key in defaults and not isinstance(defaults[key], bool):
            fail(f"defaults.{key} must be bool")

    if "keyword_pair_rules" in defaults:
        pair_rules = defaults["keyword_pair_rules"]
        if not isinstance(pair_rules, dict):
            fail("defaults.keyword_pair_rules must be an object")
        for key in ("require_level_with_autonomous_context", "require_truck_with_autonomous_context"):
            if key in pair_rules and not isinstance(pair_rules[key], bool):
                fail(f"defaults.keyword_pair_rules.{key} must be bool")

    if "relevance_thresholds" in defaults:
        thresholds = defaults["relevance_thresholds"]
        if not isinstance(thresholds, dict):
            fail("defaults.relevance_thresholds must be an object")
        for key in ("general_media", "industry_media", "newsroom", "regulator", "research", "search_api"):
            if key in thresholds:
                try:
                    int(thresholds[key])
                except Exception:
                    fail(f"defaults.relevance_thresholds.{key} must be int")

    for int_key in ("window_days", "top_n", "max_general_media_items_per_source", "fast_pass_window_hours"):
        if int_key in defaults:
            try:
                int(defaults[int_key])
            except Exception:
                fail(f"defaults.{int_key} must be int")


def validate_sources(cfg: dict) -> tuple[int, int]:
    if not isinstance(cfg.get("sources"), list):
        fail("sources must be a list")
    if not isinstance(cfg.get("companies"), list):
        fail("companies must be a list")
    validate_defaults(cfg)

    company_ids = {str(c.get("id", "")).strip() for c in cfg["companies"] if isinstance(c, dict)}

    providers = cfg.get("search_providers", {})
    if not isinstance(providers, dict):
        fail("search_providers must be an object")
    query_sets = cfg.get("query_sets", {})
    if not isinstance(query_sets, dict):
        fail("query_sets must be an object")
    for set_name, rows in query_sets.items():
        if not isinstance(rows, list):
            fail(f"query_sets.{set_name} must be list")
        for idx, row in enumerate(rows):
            if isinstance(row, str):
                if not row.strip():
                    fail(f"query_sets.{set_name}[{idx}] must not be empty")
                continue
            if not isinstance(row, dict):
                fail(f"query_sets.{set_name}[{idx}] must be string or object")
            q = str(row.get("q", "")).strip()
            if not q:
                fail(f"query_sets.{set_name}[{idx}].q is required")

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
        if stype not in {"rss", "search_api", "structured_web", "query_rss"}:
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

        elif stype == "query_rss":
            provider = str(src.get("provider", "")).strip().lower()
            qset = str(src.get("query_set", "")).strip()
            if provider not in ALLOWED_QUERY_RSS_PROVIDERS:
                fail(f"sources[{i}] query_rss provider not supported: {provider}")
            if qset not in query_sets:
                fail(f"sources[{i}] query_set not found: {qset}")
            if "max_results_per_query" in src:
                try:
                    int(src["max_results_per_query"])
                except Exception:
                    fail(f"sources[{i}].max_results_per_query must be int")

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

        source_profile = str(src.get("source_profile", "")).strip().lower()
        if source_profile and source_profile not in ALLOWED_SOURCE_PROFILES:
            fail(f"sources[{i}] invalid source_profile: {source_profile}")

        for key in ("include_keywords", "exclude_keywords", "url_allow_patterns", "url_block_patterns"):
            if key in src:
                ensure_string_list(f"sources[{i}].{key}", src[key])

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
