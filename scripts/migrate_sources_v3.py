#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


ROBOTAXI_COMPANIES = {
    "waymo", "zoox", "motional", "may_mobility", "tesla", "baidu_apollo", "pony_ai", "weride",
    "didi_autonomous", "autox", "qingzhou",
}
SUPPLIER_COMPANIES = {"mobileye", "momenta", "nvidia_auto", "horizon", "hesai", "robosense", "black_sesame"}
PASSENGER_COMPANIES = {"mercedes", "bmw", "honda", "xpeng", "nio", "li_auto", "zeekr"}

DISABLE_OPTIMIZED = {
    "the_verge", "ars_technica", "tmtpost", "ifanr", "leikeji", "yahoo_finance_news", "mashable",
    "venturebeat_ai", "wired", "cnbc_tech", "freightwaves", "fmcsa_news_structured", "bnef_structured",
    "woodmac_structured", "domestic_search_api", "foreign_search_api", "nuro_news_structured", "lyft_ir_rss",
}
IMPORTANT_MEDIA = {"gasgoo", "leiphone", "qbitai", "cnevpost", "robot_report", "techcrunch", "electrek", "ieee_spectrum"}

OFFICIAL_ACCOUNTS: dict[str, dict[str, Any]] = {
    "waymo": {"x_handles": ["@Waymo"]},
    "zoox": {"x_handles": ["@zoox"]},
    "motional": {"x_handles": ["@motionaldrive"]},
    "may_mobility": {"x_handles": ["@May_Mobility"]},
    "tesla": {"x_handles": ["@Tesla", "@Tesla_AI"]},
    "baidu_apollo": {"wechat_names": ["Apollo智能驾驶"]},
    "pony_ai": {"wechat_names": ["小马智行"], "x_handles": ["@PonyAI_tech"]},
    "weride": {"wechat_names": ["文远知行WeRide"], "x_handles": ["@WeRide_ai"]},
    "didi_autonomous": {"wechat_names": ["滴滴自动驾驶"]},
    "autox": {"wechat_names": ["AutoX无人驾驶"], "x_handles": ["@AutoX_AI"]},
    "qingzhou": {"wechat_names": ["轻舟智航"]},
    "mobileye": {"x_handles": ["@Mobileye"]},
    "momenta": {"wechat_names": ["Momenta"]},
    "nvidia_auto": {"x_handles": ["@NVIDIADRIVE"]},
    "horizon": {"wechat_names": ["地平线HorizonRobotics"]},
    "hesai": {"wechat_names": ["禾赛科技"]},
    "robosense": {"wechat_names": ["RoboSense速腾聚创"]},
    "black_sesame": {"wechat_names": ["黑芝麻智能科技"]},
    "xpeng": {"wechat_names": ["小鹏汽车"], "x_handles": ["@XPengMotors"]},
    "nio": {"wechat_names": ["蔚来"]},
    "li_auto": {"wechat_names": ["理想汽车"]},
    "zeekr": {"wechat_names": ["极氪ZEEKR"], "x_handles": ["@ZEEKRGlobal"]},
    "mercedes": {"x_handles": ["@MercedesBenz"]},
    "bmw": {"x_handles": ["@BMWGroup"]},
    "honda": {"x_handles": ["@Honda"]},
}


NEW_COMPANIES = [
    ("mercedes", "Mercedes-Benz", ["mercedes-benz", "梅赛德斯-奔驰", "奔驰"]),
    ("bmw", "BMW", ["bmw group", "宝马"]),
    ("honda", "Honda", ["本田"]),
    ("xpeng", "小鹏汽车", ["xpeng", "小鹏"]),
    ("nio", "蔚来", ["nio"]),
    ("li_auto", "理想汽车", ["li auto", "理想"]),
    ("zeekr", "极氪", ["zeekr"]),
    ("nvidia_auto", "NVIDIA Automotive", ["nvidia drive", "英伟达汽车"]),
    ("horizon", "地平线", ["horizon robotics"]),
    ("hesai", "禾赛科技", ["hesai"]),
    ("robosense", "速腾聚创", ["robosense"]),
    ("black_sesame", "黑芝麻智能", ["black sesame technologies"]),
]


NEW_SOURCES: list[dict[str, Any]] = [
    {
        "id": "manual_official_social_seeds", "name": "Manual Official WeChat/X Seeds", "region": "domestic",
        "source_type": "social_provider", "source_profile": "general_media", "provider": "manual_seed",
        "seed_file": ".state/manual_social_seeds.json", "seed_file_env": "SOCIAL_SEED_FILE", "source_role": "social_discovery",
        "evidence_type": "social_post", "criticality": "optional",
        "coverage_domains": ["robotaxi", "passenger_l3", "passenger_l4", "core_supply_chain", "regulation_safety"],
        "official_accounts": {
            "wechat_names": ["Apollo智能驾驶", "小马智行", "文远知行WeRide", "滴滴自动驾驶", "AutoX无人驾驶", "轻舟智航", "Momenta", "地平线HorizonRobotics", "禾赛科技", "RoboSense速腾聚创", "黑芝麻智能科技", "小鹏汽车", "蔚来", "理想汽车", "极氪ZEEKR"],
            "x_handles": ["@Waymo", "@zoox", "@motionaldrive", "@May_Mobility", "@Tesla", "@PonyAI_tech", "@WeRide_ai", "@Mobileye", "@NVIDIADRIVE"]
        },
    },
    {
        "id": "official_x_social_discovery", "name": "Official X Accounts Discovery", "region": "foreign",
        "source_type": "search_result", "source_profile": "general_media", "provider": "bing_news",
        "query_set": "official_x_discovery", "max_results_per_query": 2,
        "source_role": "social_discovery", "evidence_type": "social_post", "criticality": "optional",
        "coverage_domains": ["robotaxi", "passenger_l3", "passenger_l4", "core_supply_chain", "regulation_safety"],
        "official_accounts": {
            "x_handles": ["@Waymo", "@zoox", "@motionaldrive", "@May_Mobility", "@Tesla", "@Tesla_AI", "@PonyAI_tech", "@WeRide_ai", "@AutoX_AI", "@Mobileye", "@NVIDIADRIVE", "@XPengMotors", "@ZEEKRGlobal", "@MercedesBenz", "@BMWGroup", "@Honda"]
        },
    },
    {
        "id": "nhtsa_sgo_dataset_structured", "name": "NHTSA ADS Standing General Order Data", "region": "foreign",
        "source_type": "structured_web", "source_profile": "regulator",
        "entry_urls": ["https://www.nhtsa.gov/laws-regulations/standing-general-order-crash-reporting"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href$='.csv'], a[href*='standing-general-order']", "title": "h1", "content": "main p", "published": "time, meta[name='date']", "attachment_link": "a[href$='.csv']"},
        "max_items_per_run": 8, "url_allow_patterns": ["standing-general-order", ".csv"],
        "coverage_domains": ["robotaxi", "passenger_l3", "passenger_l4", "regulation_safety"], "evidence_type": "dataset",
    },
    {
        "id": "mercedes_press_structured", "name": "Mercedes-Benz Media Autonomous Driving", "region": "foreign", "source_company_id": "mercedes",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://media.mercedes-benz.com/"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/article/'], a[href*='/news/']", "title": "h1", "content": "article p, main p", "published": "time, meta[property='article:published_time']"}, "max_items_per_run": 8,
    },
    {
        "id": "bmw_pressclub_structured", "name": "BMW PressClub Autonomous Driving", "region": "foreign", "source_company_id": "bmw",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://www.press.bmwgroup.com/global"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/article/detail/']", "title": "h1", "content": "article p, main p", "published": "time, .date, meta[property='article:published_time']"}, "max_items_per_run": 8, "url_allow_patterns": ["/article/detail/"],
    },
    {
        "id": "honda_global_news_structured", "name": "Honda Global News", "region": "foreign", "source_company_id": "honda",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://global.honda/en/newsroom/"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/newsroom/news/']", "title": "h1", "content": "article p, main p", "published": "time, .date"}, "max_items_per_run": 8, "url_allow_patterns": ["/newsroom/news/"],
    },
    {
        "id": "xpeng_ir_structured", "name": "XPeng IR News Releases", "region": "domestic", "source_company_id": "xpeng",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://ir.xiaopeng.com/news-events/news-releases"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/news-releases/news-release-details/']", "title": "h1", "content": "article p, main p", "published": "time, .date"}, "max_items_per_run": 8, "url_allow_patterns": ["/news-releases/news-release-details/"],
    },
    {
        "id": "nio_news_structured", "name": "NIO Official News", "region": "domestic", "source_company_id": "nio",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://www.nio.com/news"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/news/']", "title": "h1", "content": "article p, main p", "published": "time, .date"}, "max_items_per_run": 8, "url_allow_patterns": ["/news/"],
    },
    {
        "id": "li_auto_ir_structured", "name": "Li Auto IR News Releases", "region": "domestic", "source_company_id": "li_auto",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://ir.lixiang.com/news-events/news-releases"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/news-releases/news-release-details/']", "title": "h1", "content": "article p, main p", "published": "time, .date"}, "max_items_per_run": 8, "url_allow_patterns": ["/news-releases/news-release-details/"],
    },
    {
        "id": "zeekr_ir_structured", "name": "ZEEKR IR News Releases", "region": "domestic", "source_company_id": "zeekr",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://ir.zeekrgroup.com/news-events/news-releases"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/news-releases/news-release-details/']", "title": "h1", "content": "article p, main p", "published": "time, .date"}, "max_items_per_run": 8, "url_allow_patterns": ["/news-releases/news-release-details/"],
    },
    {
        "id": "didi_autonomous_news_structured", "name": "DiDi Autonomous Driving News", "region": "domestic", "source_company_id": "didi_autonomous",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://www.didiglobal.com/news"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/news/']", "title": "h1", "content": "article p, main p", "published": "time, .date"}, "max_items_per_run": 8, "url_allow_patterns": ["/news/"],
    },
    {
        "id": "autox_news_structured", "name": "AutoX Official News", "region": "domestic", "source_company_id": "autox",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://www.autox.ai/news"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/news/']", "title": "h1", "content": "article p, main p", "published": "time, .date"}, "max_items_per_run": 8, "url_allow_patterns": ["/news/"],
    },
    {
        "id": "qingzhou_news_structured", "name": "QCraft Official News", "region": "domestic", "source_company_id": "qingzhou",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://www.qcraft.ai/news"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/news/']", "title": "h1", "content": "article p, main p", "published": "time, .date"}, "max_items_per_run": 8, "url_allow_patterns": ["/news/"],
    },
    {
        "id": "nvidia_automotive_news_structured", "name": "NVIDIA Automotive News", "region": "foreign", "source_company_id": "nvidia_auto",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://blogs.nvidia.com/blog/category/auto/"],
        "extractor": "css_selector", "selectors": {"article_link": "article a[href*='/blog/']", "title": "h1", "content": "article p", "published": "time, meta[property='article:published_time']"}, "max_items_per_run": 8, "url_allow_patterns": ["/blog/"],
    },
    {
        "id": "hesai_news_structured", "name": "Hesai News", "region": "domestic", "source_company_id": "hesai",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://www.hesaitech.com/news/"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/news/']", "title": "h1", "content": "article p, main p", "published": "time, .date"}, "max_items_per_run": 8, "url_allow_patterns": ["/news/"],
    },
    {
        "id": "horizon_news_structured", "name": "Horizon Robotics News", "region": "domestic", "source_company_id": "horizon",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://en.horizon.auto/news/"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/news/']", "title": "h1", "content": "article p, main p", "published": "time, .date"}, "max_items_per_run": 8, "url_allow_patterns": ["/news/"],
    },
    {
        "id": "robosense_news_structured", "name": "RoboSense News", "region": "domestic", "source_company_id": "robosense",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://www.robosense.ai/en/news-show"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/news-show-']", "title": "h1", "content": "article p, main p", "published": "time, .date"}, "max_items_per_run": 8,
    },
    {
        "id": "black_sesame_news_structured", "name": "Black Sesame Technologies News", "region": "domestic", "source_company_id": "black_sesame",
        "source_type": "structured_web", "source_profile": "newsroom", "entry_urls": ["https://www.blacksesame.com.cn/news"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/news/']", "title": "h1", "content": "article p, main p", "published": "time, .date"}, "max_items_per_run": 8, "url_allow_patterns": ["/news/"],
    },
    {
        "id": "samr_recall_structured", "name": "SAMR Vehicle Recall Notices", "region": "domestic",
        "source_type": "structured_web", "source_profile": "regulator", "entry_urls": ["https://www.samr.gov.cn/zw/zfxxgk/fdzdgknr/zlfzj/"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/zw/'][href$='.html']", "title": "h1", "content": "article p, main p", "published": "time, .date"}, "max_items_per_run": 8, "url_allow_patterns": ["/zw/"],
    },
    {
        "id": "kba_press_structured", "name": "Germany KBA Press Releases", "region": "foreign",
        "source_type": "structured_web", "source_profile": "regulator", "entry_urls": ["https://www.kba.de/EN/Presse/Pressemitteilungen/pressemitteilungen_node.html"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/SharedDocs/Pressemitteilungen/EN/']", "title": "h1", "content": "article p, main p", "published": "time, .date"}, "max_items_per_run": 8, "url_allow_patterns": ["/SharedDocs/Pressemitteilungen/EN/"],
    },
    {
        "id": "nhtsa_investigations_structured", "name": "NHTSA Recalls and Investigations", "region": "foreign",
        "source_type": "structured_web", "source_profile": "regulator", "entry_urls": ["https://www.nhtsa.gov/vehicle-safety/automated-vehicles-safety"],
        "extractor": "css_selector", "selectors": {"article_link": "a[href*='/press-releases/'], a[href*='/vehicle-safety/']", "title": "h1", "content": "article p, main p", "published": "time, meta[property='article:published_time']"}, "max_items_per_run": 8, "url_allow_patterns": ["/press-releases/", "/vehicle-safety/"],
    },
]


def _domains(source: dict[str, Any]) -> list[str]:
    company = str(source.get("source_company_id", ""))
    profile = str(source.get("source_profile", ""))
    if company in SUPPLIER_COMPANIES:
        return ["core_supply_chain", "passenger_l3", "passenger_l4", "robotaxi"]
    if company in PASSENGER_COMPANIES:
        return ["passenger_l3", "passenger_l4", "regulation_safety"]
    if company in ROBOTAXI_COMPANIES:
        return ["robotaxi", "passenger_l4", "regulation_safety"]
    if profile == "regulator":
        return ["robotaxi", "passenger_l3", "passenger_l4", "regulation_safety"]
    return ["robotaxi", "passenger_l3", "passenger_l4", "core_supply_chain", "regulation_safety"]


def _domains_from_urls(source: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    for key in ("entry_urls", "rss_urls"):
        urls.extend(str(value) for value in source.get(key, []) if str(value))
    endpoint = str(source.get("endpoint", ""))
    if endpoint:
        urls.append(endpoint)
    return sorted({urlparse(url).netloc.lower() for url in urls if urlparse(url).netloc})


def _migrate_source(source: dict[str, Any]) -> dict[str, Any]:
    profile = str(source.get("source_profile", "general_media"))
    source_type = str(source.get("source_type", "rss"))
    company = str(source.get("source_company_id", ""))
    legacy_enabled = bool(source.get("enabled", True))

    configured_role = str(source.get("source_role", "")).strip().lower()
    if configured_role == "social_discovery":
        role, evidence, criticality = "social_discovery", "social_post", "optional"
    elif source_type in {"search_api", "query_rss", "search_result"}:
        role, evidence, criticality = "search_discovery", "general_media", "optional"
    elif profile in {"regulator", "newsroom"} or source_type == "official_api":
        role = "primary"
        evidence = "regulator" if profile == "regulator" or source_type == "official_api" else "company_newsroom"
        if profile == "regulator" or source_type == "official_api":
            criticality = "required"
        elif company in ROBOTAXI_COMPANIES | SUPPLIER_COMPANIES | PASSENGER_COMPANIES:
            criticality = "required"
        else:
            criticality = "important"
    elif profile == "industry_media" or str(source.get("id", "")) in IMPORTANT_MEDIA:
        role, evidence, criticality = "secondary", "industry_media", "important"
    else:
        role, evidence, criticality = "secondary", "general_media", "optional"

    if str(source.get("evidence_type", "")) == "dataset":
        evidence = "dataset"
    source["source_role"] = role
    source["evidence_type"] = evidence
    source["coverage_domains"] = source.get("coverage_domains") or _domains(source)
    source["criticality"] = criticality

    if source_type == "rss":
        index_transport, article_transport = "rss", "jsonld"
    elif source_type == "structured_web":
        index_transport = "sitemap" if source.get("extractor") == "sitemap" else "css"
        if evidence == "dataset":
            article_transport = "provider"
        else:
            article_transport = "jsonld" if source.get("extractor") == "json_ld" else "css"
    elif source_type == "official_api":
        index_transport, article_transport = "api", "provider"
    elif source_type == "social_provider":
        index_transport, article_transport = "search", "provider"
    else:
        index_transport, article_transport = "search", "provider"
    source["transport"] = {"index": index_transport, "article": article_transport}
    source["health_policy"] = {
        "alert_on_single_failure": criticality == "required",
        "empty_listing_limit": 3,
        "date_parse_rate_min": 0.90,
        "whitelist_reject_rate_max": 0.50,
        "new_content_required": False,
    }
    if criticality == "required" and source_type == "structured_web":
        source["health_policy"]["fixture_path"] = "tests/fixtures/structured_p0_universal.html"
    accounts = dict(source.get("official_accounts", {})) if isinstance(source.get("official_accounts", {}), dict) else {}
    for key, value in OFFICIAL_ACCOUNTS.get(company, {}).items():
        if key not in accounts:
            accounts[key] = value
    accounts["domains"] = _domains_from_urls(source)
    source["official_accounts"] = accounts
    optimized_enabled = legacy_enabled and str(source.get("id", "")) not in DISABLE_OPTIMIZED
    if str(source.get("id", "")) in {"unece_vehicle_regulations_structured", "nhtsa_press"}:
        optimized_enabled = True
    source["enabled_profiles"] = {"legacy": legacy_enabled, "optimized": optimized_enabled}
    return source


def migrate(cfg: dict[str, Any]) -> dict[str, Any]:
    cfg["version"] = 3
    cfg["active_profile"] = "legacy"
    defaults = cfg.setdefault("defaults", {})
    defaults.update(
        {
            "top_n": 12,
            "minimum_target_items": 8,
            "per_company_cap": 2,
            "per_source_cap": 2,
            "discovery_direct_share_cap": 0.25,
            "region_soft_max_share": 0.60,
            "state_retention_days": 35,
        }
    )
    excluded_scope_terms = (
        "truck", "freight", "货车", "货运", "干线物流", "commercial motor vehicle", "robovan", "delivery",
    )
    for key, value in list(defaults.items()):
        if isinstance(value, list) and ("keyword" in key or key in {"domestic_keywords", "foreign_keywords"}):
            defaults[key] = [
                item
                for item in value
                if not any(term in str(item).lower() for term in excluded_scope_terms)
            ]
    cfg["profiles"] = {
        "legacy": {"description": "当前生产逻辑，保留 30 天用于回滚", "defaults": {"scope_mode": "legacy", "late_arrival_enabled": False}},
        "optimized": {
            "description": "Robotaxi 与 L3/L4 乘用车信源优化影子 profile",
            "defaults": {
                "scope_mode": "passenger_l3_l4", "late_arrival_enabled": True, "late_arrival_hours": 72,
                "late_arrival_max_items": 2, "late_arrival_min_score": 80,
                "late_arrival_allowed_roles": ["primary", "secondary"],
            },
        },
    }
    # P2 搜索只做发现；SerpAPI 默认关闭且无密钥不告警。
    for provider in cfg.get("search_providers", {}).values():
        if isinstance(provider, dict):
            provider["enabled"] = False
            provider["missing_key_alert"] = False

    cfg["query_sets"]["domestic_robtaxi"] = [
        {"q": "Robotaxi OR 无人驾驶出租车 OR 自动驾驶出租车"},
        {"q": "L3 乘用车 准入 OR 量产 OR 上路"},
        {"q": "L4 乘用车 示范运营 OR 许可 OR 准入"},
        {"q": "自动驾驶 召回 OR 事故 OR 监管 L3 OR L4"},
    ]
    cfg["query_sets"]["foreign_robtaxi"] = [
        {"q": "robotaxi OR driverless taxi"},
        {"q": "level 3 passenger car approval OR production"},
        {"q": "level 4 passenger vehicle permit OR deployment"},
        {"q": "automated driving recall OR investigation level 3 OR level 4"},
    ]
    cfg["query_sets"]["official_x_discovery"] = [
        {"group": "official_x", "q": "site:x.com/Waymo/status robotaxi OR autonomous driving"},
        {"group": "official_x", "q": "site:x.com/zoox/status robotaxi OR autonomous vehicle"},
        {"group": "official_x", "q": "site:x.com/motionaldrive/status robotaxi"},
        {"group": "official_x", "q": "site:x.com/May_Mobility/status robotaxi"},
        {"group": "official_x", "q": "site:x.com/Tesla/status robotaxi OR Cybercab"},
        {"group": "official_x", "q": "site:x.com/PonyAI_tech/status robotaxi"},
        {"group": "official_x", "q": "site:x.com/WeRide_ai/status robotaxi"},
        {"group": "official_x", "q": "site:x.com/Mobileye/status level 3 OR level 4"},
        {"group": "official_x", "q": "site:x.com/NVIDIADRIVE/status level 3 OR robotaxi"},
        {"group": "official_x", "q": "site:x.com/XPengMotors/status level 3 autonomous"},
        {"group": "official_x", "q": "site:x.com/MercedesBenz/status level 3 DRIVE PILOT"},
        {"group": "official_x", "q": "site:x.com/BMWGroup/status level 3 autonomous"},
    ]
    for set_name, rows in list(cfg.get("query_sets", {}).items()):
        if not isinstance(rows, list):
            continue
        cleaned_rows = []
        for row in rows:
            query_text = str(row.get("q", "") if isinstance(row, dict) else row).lower()
            if any(term in query_text for term in excluded_scope_terms):
                continue
            cleaned_rows.append(row)
        cfg["query_sets"][set_name] = cleaned_rows

    domestic_search = cfg["query_sets"].setdefault("domestic_robtaxi_search_result", [])
    foreign_search = cfg["query_sets"].setdefault("foreign_robtaxi_search_result", [])
    for row in (
        {"group": "topic", "q": "L3 乘用车 准入 量产"},
        {"group": "topic", "q": "L4 乘用车 许可 示范运营"},
        {"group": "context", "q": "L3 L4 乘用车 召回 安全 调查"},
    ):
        if row["q"] not in {str(value.get("q", "")) for value in domestic_search if isinstance(value, dict)}:
            domestic_search.append(row)
    for row in (
        {"group": "topic", "q": "level 3 passenger car approval production"},
        {"group": "topic", "q": "level 4 passenger vehicle permit deployment"},
        {"group": "context", "q": "level 3 level 4 passenger car recall investigation"},
    ):
        if row["q"] not in {str(value.get("q", "")) for value in foreign_search if isinstance(value, dict)}:
            foreign_search.append(row)

    existing_companies = {str(company.get("id", "")) for company in cfg.get("companies", []) if isinstance(company, dict)}
    for company_id, name, aliases in NEW_COMPANIES:
        if company_id not in existing_companies:
            cfg["companies"].append({"id": company_id, "name": name, "aliases": aliases})
    for company in cfg.get("companies", []):
        if not isinstance(company, dict):
            continue
        company["official_accounts"] = OFFICIAL_ACCOUNTS.get(str(company.get("id", "")), {})

    source_map = {str(source.get("id", "")): source for source in cfg.get("sources", []) if isinstance(source, dict)}
    for new_source in NEW_SOURCES:
        if str(new_source["id"]) not in source_map:
            new_source["tier"] = "A"
            new_source["category"] = "regulator" if new_source.get("source_profile") == "regulator" else "newsroom"
            new_source["enabled"] = False
            cfg["sources"].append(new_source)

    for source in cfg["sources"]:
        sid = str(source.get("id", ""))
        if sid == "miit_news_structured":
            source["url_allow_patterns"] = ["/xwdt/", "/zwgk/", "/jgsj/"]
        elif sid == "pony_news_structured":
            source["url_allow_patterns"] = ["/press", "/news-releases/", "/releases/", "/news/"]
        elif sid == "uber_news_structured":
            source["request_headers"] = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.8",
            }
        elif sid == "unece_wp29_structured":
            source["enabled"] = False
            source["enabled_profiles"] = {"legacy": True, "optimized": False}
            source["name"] = "European Commission CCAM (not UNECE)"
            source["criticality"] = "optional"
        elif sid == "unece_vehicle_regulations_structured":
            source["entry_urls"] = [
                "https://unece.org/transport/vehicle-regulations/working-party-automatedautonomous-and-connected-vehicles-introduction",
                "https://unece.org/transport/vehicle-regulations/wp29-working-parties-and-informal-working-groups",
            ]
        elif sid == "nhtsa_press":
            # 旧 RSS 已失效，避免把它错误提升为 P0；由 SGO 数据页和站内发现承担。
            source["enabled"] = False
            source["enabled_profiles"] = {"legacy": False, "optimized": False}

        for keyword_key in ("include_keywords", "exclude_keywords"):
            if isinstance(source.get(keyword_key), list):
                source[keyword_key] = [
                    item
                    for item in source[keyword_key]
                    if not any(term in str(item).lower() for term in excluded_scope_terms)
                ]

        _migrate_source(source)
        if sid in {str(value["id"]) for value in NEW_SOURCES} | {"unece_vehicle_regulations_structured"}:
            source["enabled_profiles"]["optimized"] = True

    return cfg


def main() -> int:
    parser = argparse.ArgumentParser(description="将 Robotaxi 信源配置机械迁移到 v3")
    parser.add_argument("input", nargs="?", default="./sources.json")
    parser.add_argument("--output", default="./sources.json")
    args = parser.parse_args()
    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    cfg = json.loads(input_path.read_text(encoding="utf-8"))
    migrated = migrate(cfg)
    output_path.write_text(json.dumps(migrated, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[migrate] version=3 sources={len(migrated['sources'])} output={output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
