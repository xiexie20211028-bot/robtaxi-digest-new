from __future__ import annotations

import argparse
from pathlib import Path
from urllib.parse import urlparse

from .common import read_json
from .source_config import COVERAGE_DOMAINS, CRITICALITIES, EVIDENCE_TYPES, PROFILE_NAMES, SOURCE_ROLES

ALLOWED_SOURCE_PROFILES = {"general_media", "industry_media", "newsroom", "regulator", "research"}
ALLOWED_RELEVANCE_MODES = {"high_precision", "balanced", "high_recall"}
ALLOWED_QUERY_RSS_PROVIDERS = {"google_news"}
ALLOWED_SEARCH_RESULT_PROVIDERS = {"bing_news", "toutiao_news"}
ALLOWED_OFFICIAL_API_PROVIDERS = {"federalregister"}
ALLOWED_INDEX_TRANSPORTS = {"api", "rss", "sitemap", "css", "search"}
ALLOWED_ARTICLE_TRANSPORTS = {"jsonld", "css", "provider"}


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
        "discovery_query_groups",
        "impact_target_taxonomy",
        "summary_ban_phrases",
    ):
        if key in defaults:
            ensure_string_list(f"defaults.{key}", defaults[key])

    for key in (
        "fast_pass_enabled",
        "fast_pass_require_company_or_context",
        "enable_general_media_source_cap",
        "strict_today_mode",
        "summary_require_so_what",
    ):
        if key in defaults and not isinstance(defaults[key], bool):
            fail(f"defaults.{key} must be bool")

    if "summary_style" in defaults and not isinstance(defaults["summary_style"], str):
        fail("defaults.summary_style must be string")

    if "strict_today_timezone" in defaults and not isinstance(defaults["strict_today_timezone"], str):
        fail("defaults.strict_today_timezone must be string")

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

    if "self_check" in defaults:
        self_check = defaults["self_check"]
        if not isinstance(self_check, dict):
            fail("defaults.self_check must be an object")
        for key in (
            "source_failure_error_rate",
            "source_failure_critical_rate",
            "summary_fallback_error_rate",
        ):
            if key not in self_check:
                continue
            try:
                value = float(self_check[key])
            except Exception:
                fail(f"defaults.self_check.{key} must be a number")
            if not 0 <= value <= 1:
                fail(f"defaults.self_check.{key} must be between 0 and 1")
        source_error = float(self_check.get("source_failure_error_rate", 0.30))
        source_critical = float(self_check.get("source_failure_critical_rate", 0.60))
        if source_error >= source_critical:
            fail("defaults.self_check source error rate must be lower than critical rate")

    for int_key in (
        "window_days",
        "top_n",
        "max_general_media_items_per_source",
        "fast_pass_window_hours",
        "summary_sentence_min",
        "summary_sentence_max",
    ):
        if int_key in defaults:
            try:
                int(defaults[int_key])
            except Exception:
                fail(f"defaults.{int_key} must be int")

    if "window_mode" in defaults:
        mode = str(defaults["window_mode"]).strip().lower()
        if mode not in {"prev_natural_day"}:
            fail("defaults.window_mode must be prev_natural_day")
    if "window_timezone" in defaults and not isinstance(defaults["window_timezone"], str):
        fail("defaults.window_timezone must be string")
    for bool_key in ("drop_if_published_missing", "drop_if_published_unparseable"):
        if bool_key in defaults and not isinstance(defaults[bool_key], bool):
            fail(f"defaults.{bool_key} must be bool")

    if "discovery_query_recency" in defaults and not isinstance(defaults["discovery_query_recency"], str):
        fail("defaults.discovery_query_recency must be string")
    if "discovery_max_results_per_query" in defaults:
        try:
            int(defaults["discovery_max_results_per_query"])
        except Exception:
            fail("defaults.discovery_max_results_per_query must be int")


def validate_sources(cfg: dict) -> tuple[int, int]:
    if int(cfg.get("version", 0) or 0) != 3:
        fail("sources.json version must be 3")
    active_profile = str(cfg.get("active_profile", "")).strip().lower()
    if active_profile not in PROFILE_NAMES:
        fail(f"active_profile must be one of {sorted(PROFILE_NAMES)}")
    profiles = cfg.get("profiles", {})
    if not isinstance(profiles, dict) or not PROFILE_NAMES.issubset(set(profiles)):
        fail(f"profiles must define {sorted(PROFILE_NAMES)}")
    if not isinstance(cfg.get("sources"), list):
        fail("sources must be a list")
    if not isinstance(cfg.get("companies"), list):
        fail("companies must be a list")
    validate_defaults(cfg)
    agent_cfg = cfg.get("industry_agent", {})
    if not isinstance(agent_cfg, dict):
        fail("industry_agent must be an object")
    if str(agent_cfg.get("model_provider", "")) != "deepseek":
        fail("industry_agent.model_provider must be deepseek in v1")
    if str(agent_cfg.get("search_provider", "")) != "deepseek_web":
        fail("industry_agent.search_provider must be deepseek_web in v1")
    if int(agent_cfg.get("max_web_searches", 0) or 0) < 1:
        fail("industry_agent.max_web_searches must be positive")
    if float(agent_cfg.get("daily_budget_cny", 0.0) or 0.0) <= 0:
        fail("industry_agent.daily_budget_cny must be positive")
    pricing = agent_cfg.get("pricing", {})
    if not isinstance(pricing, dict):
        fail("industry_agent.pricing must be an object")
    for key in (
        "input_cache_hit_cny_per_million",
        "input_cache_miss_cny_per_million",
        "output_cny_per_million",
    ):
        if float(pricing.get(key, -1) or 0) < 0:
            fail(f"industry_agent.pricing.{key} must be non-negative")

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
        if stype not in {"rss", "search_api", "structured_web", "query_rss", "official_api", "search_result", "social_provider"}:
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
            if "max_age_hours" in src:
                try:
                    int(src["max_age_hours"])
                except Exception:
                    fail(f"sources[{i}].max_age_hours must be int")

        elif stype == "search_result":
            provider = str(src.get("provider", "")).strip().lower()
            qset = str(src.get("query_set", "")).strip()
            if provider not in ALLOWED_SEARCH_RESULT_PROVIDERS:
                fail(f"sources[{i}] search_result provider not supported: {provider}")
            if qset not in query_sets:
                fail(f"sources[{i}] query_set not found: {qset}")
            if "max_results_per_query" in src:
                try:
                    int(src["max_results_per_query"])
                except Exception:
                    fail(f"sources[{i}].max_results_per_query must be int")

        elif stype == "official_api":
            provider = str(src.get("provider", "")).strip().lower()
            if provider not in ALLOWED_OFFICIAL_API_PROVIDERS:
                fail(f"sources[{i}] official_api provider not supported: {provider}")
            endpoint = str(src.get("endpoint", "")).strip()
            if endpoint and not is_http_url(endpoint):
                fail(f"sources[{i}] invalid official_api endpoint: {endpoint}")
            if provider == "federalregister":
                agency_slug = str(src.get("agency_slug", "")).strip()
                if not agency_slug:
                    fail(f"sources[{i}].agency_slug is required for federalregister")
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

        elif stype == "social_provider":
            if str(src.get("provider", "")).strip().lower() != "manual_seed":
                fail(f"sources[{i}] social_provider only supports manual_seed")
            if not str(src.get("seed_file", "")).strip():
                fail(f"sources[{i}].seed_file is required")

        source_profile = str(src.get("source_profile", "")).strip().lower()
        if source_profile and source_profile not in ALLOWED_SOURCE_PROFILES:
            fail(f"sources[{i}] invalid source_profile: {source_profile}")

        source_role = str(src.get("source_role", "")).strip().lower()
        evidence_type = str(src.get("evidence_type", "")).strip().lower()
        criticality = str(src.get("criticality", "")).strip().lower()
        if source_role not in SOURCE_ROLES:
            fail(f"sources[{i}] invalid source_role: {source_role}")
        if evidence_type not in EVIDENCE_TYPES:
            fail(f"sources[{i}] invalid evidence_type: {evidence_type}")
        if criticality not in CRITICALITIES:
            fail(f"sources[{i}] invalid criticality: {criticality}")
        coverage_domains = src.get("coverage_domains", [])
        ensure_string_list(f"sources[{i}].coverage_domains", coverage_domains)
        if not coverage_domains or any(value not in COVERAGE_DOMAINS for value in coverage_domains):
            fail(f"sources[{i}] coverage_domains contains unsupported value")

        enabled_profiles = src.get("enabled_profiles", {})
        if not isinstance(enabled_profiles, dict):
            fail(f"sources[{i}].enabled_profiles must be object")
        # agent_domestic 通过 profile.source_policy 收缩国内源，不要求给 90 个源重复加开关。
        for profile_name in {"legacy", "optimized"}:
            if profile_name not in enabled_profiles or not isinstance(enabled_profiles[profile_name], bool):
                fail(f"sources[{i}].enabled_profiles.{profile_name} must be bool")

        transport = src.get("transport", {})
        if not isinstance(transport, dict):
            fail(f"sources[{i}].transport must be object")
        if str(transport.get("index", "")) not in ALLOWED_INDEX_TRANSPORTS:
            fail(f"sources[{i}].transport.index invalid")
        if str(transport.get("article", "")) not in ALLOWED_ARTICLE_TRANSPORTS:
            fail(f"sources[{i}].transport.article invalid")

        health_policy = src.get("health_policy", {})
        if not isinstance(health_policy, dict):
            fail(f"sources[{i}].health_policy must be object")
        for bool_key in ("alert_on_single_failure", "new_content_required"):
            if bool_key in health_policy and not isinstance(health_policy[bool_key], bool):
                fail(f"sources[{i}].health_policy.{bool_key} must be bool")
        for numeric_key in ("empty_listing_limit", "date_parse_rate_min", "whitelist_reject_rate_max"):
            if numeric_key in health_policy:
                try:
                    float(health_policy[numeric_key])
                except Exception:
                    fail(f"sources[{i}].health_policy.{numeric_key} must be numeric")
        fixture_path = str(health_policy.get("fixture_path", "")).strip()
        optimized_enabled = bool(src.get("enabled_profiles", {}).get("optimized", False))
        if stype == "structured_web" and criticality == "required" and optimized_enabled:
            if not fixture_path:
                fail(f"sources[{i}] required structured source must declare health_policy.fixture_path")
            if not Path(fixture_path).exists():
                fail(f"sources[{i}] fixture does not exist: {fixture_path}")

        official_accounts = src.get("official_accounts", {})
        if not isinstance(official_accounts, dict):
            fail(f"sources[{i}].official_accounts must be object")
        for account_key in ("wechat_names", "x_handles", "domains"):
            if account_key in official_accounts:
                ensure_string_list(f"sources[{i}].official_accounts.{account_key}", official_accounts[account_key])

        for key in ("include_keywords", "exclude_keywords", "url_allow_patterns", "url_block_patterns"):
            if key in src:
                ensure_string_list(f"sources[{i}].{key}", src[key])
        if "external_link_allow_domains" in src:
            ensure_string_list(f"sources[{i}].external_link_allow_domains", src["external_link_allow_domains"])

    agent_profile = profiles.get("agent_domestic", {}) if isinstance(profiles, dict) else {}
    if not isinstance(agent_profile, dict) or str(agent_profile.get("base_profile", "")) != "legacy":
        fail("profiles.agent_domestic.base_profile must be legacy")
    policy = agent_profile.get("source_policy", {}) if isinstance(agent_profile.get("source_policy", {}), dict) else {}
    retained_ids = policy.get("domestic_enabled_source_ids", [])
    ensure_string_list("profiles.agent_domestic.source_policy.domestic_enabled_source_ids", retained_ids)
    if len(set(retained_ids)) != 10:
        fail("agent_domestic must retain exactly 10 domestic regulator sources")
    by_id = {str(source.get("id", "")): source for source in cfg["sources"] if isinstance(source, dict)}
    for source_id in retained_ids:
        source = by_id.get(str(source_id))
        if not source:
            fail(f"agent_domestic retained source not found: {source_id}")
        if str(source.get("region", "")) != "domestic" or str(source.get("evidence_type", "")) != "regulator":
            fail(f"agent_domestic retained source must be a domestic regulator: {source_id}")

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
