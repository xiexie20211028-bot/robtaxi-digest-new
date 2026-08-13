from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any

from .common import read_json


SOURCE_ROLES = {"primary", "secondary", "social_discovery", "search_discovery"}
EVIDENCE_TYPES = {
    "regulator",
    "dataset",
    "filing",
    "company_newsroom",
    "industry_media",
    "social_post",
    "general_media",
}
CRITICALITIES = {"required", "important", "optional"}
COVERAGE_DOMAINS = {
    "robotaxi",
    "passenger_l3",
    "passenger_l4",
    "core_supply_chain",
    "regulation_safety",
}
PROFILE_NAMES = {"legacy", "optimized", "agent_domestic"}


def resolve_profile(cfg: dict[str, Any], requested: str = "") -> str:
    # GitHub Actions 在审批后通过仓库变量切换，不需要自动修改仓库文件。
    runtime_profile = os.environ.get("ROBTAXI_PROFILE", "").strip().lower()
    profile = str(requested or runtime_profile or cfg.get("active_profile", "legacy")).strip().lower()
    if profile not in PROFILE_NAMES:
        raise ValueError(f"unsupported profile: {profile}")
    return profile


def source_metadata(source: dict[str, Any]) -> dict[str, Any]:
    """返回 v3 信源元数据；旧配置也能得到可预测的兼容默认值。"""
    source_type = str(source.get("source_type", "rss")).strip().lower()
    source_profile = str(source.get("source_profile", "")).strip().lower()

    role = str(source.get("source_role", "")).strip().lower()
    if role not in SOURCE_ROLES:
        if source_type == "social_provider":
            role = "social_discovery"
        elif source_type in {"search_api", "query_rss", "search_result"}:
            role = "search_discovery"
        elif source_profile in {"regulator", "newsroom"} or source_type == "official_api":
            role = "primary"
        else:
            role = "secondary"

    evidence_type = str(source.get("evidence_type", "")).strip().lower()
    if evidence_type not in EVIDENCE_TYPES:
        if source_profile == "regulator" or source_type == "official_api":
            evidence_type = "regulator"
        elif source_profile == "newsroom":
            evidence_type = "company_newsroom"
        elif source_profile == "industry_media":
            evidence_type = "industry_media"
        else:
            evidence_type = "general_media"

    criticality = str(source.get("criticality", "")).strip().lower()
    if criticality not in CRITICALITIES:
        if role == "primary":
            criticality = "required"
        elif role == "secondary":
            criticality = "important"
        else:
            criticality = "optional"

    coverage_domains = [
        str(item).strip().lower()
        for item in source.get("coverage_domains", [])
        if str(item).strip().lower() in COVERAGE_DOMAINS
    ]
    if not coverage_domains:
        coverage_domains = ["robotaxi"]

    return {
        "source_role": role,
        "evidence_type": evidence_type,
        "criticality": criticality,
        "coverage_domains": sorted(set(coverage_domains)),
        "transport": source.get("transport", {}) if isinstance(source.get("transport", {}), dict) else {},
        "health_policy": source.get("health_policy", {}) if isinstance(source.get("health_policy", {}), dict) else {},
        "official_accounts": source.get("official_accounts", {}) if isinstance(source.get("official_accounts", {}), dict) else {},
    }


def is_source_enabled(source: dict[str, Any], profile: str) -> bool:
    enabled_profiles = source.get("enabled_profiles")
    if isinstance(enabled_profiles, dict) and profile in enabled_profiles:
        return bool(enabled_profiles[profile])
    if isinstance(enabled_profiles, list):
        return profile in {str(value).strip().lower() for value in enabled_profiles}
    return bool(source.get("enabled", True))


def apply_profile(cfg: dict[str, Any], requested: str = "") -> tuple[dict[str, Any], str]:
    """应用 profile 的默认值和单信源覆盖，同时保留原配置不变。"""
    profile = resolve_profile(cfg, requested)
    resolved = copy.deepcopy(cfg)
    profiles = resolved.get("profiles", {})
    profile_cfg = profiles.get(profile, {}) if isinstance(profiles, dict) else {}
    if not isinstance(profile_cfg, dict):
        profile_cfg = {}

    defaults = resolved.get("defaults", {})
    defaults = defaults if isinstance(defaults, dict) else {}
    defaults.update(profile_cfg.get("defaults", {}) if isinstance(profile_cfg.get("defaults", {}), dict) else {})
    resolved["defaults"] = defaults

    base_profile = str(profile_cfg.get("base_profile", profile)).strip().lower() or profile
    source_policy = profile_cfg.get("source_policy", {}) if isinstance(profile_cfg.get("source_policy", {}), dict) else {}
    domestic_enabled_ids = {
        str(value).strip()
        for value in source_policy.get("domestic_enabled_source_ids", [])
        if str(value).strip()
    }
    source_overrides = profile_cfg.get("source_overrides", {})
    if not isinstance(source_overrides, dict):
        source_overrides = {}
    for source in resolved.get("sources", []):
        if not isinstance(source, dict):
            continue
        source["enabled"] = is_source_enabled(source, base_profile)
        if profile == "agent_domestic" and str(source.get("region", "")).strip().lower() == "domestic":
            source["enabled"] = str(source.get("id", "")).strip() in domestic_enabled_ids
        override = source_overrides.get(str(source.get("id", "")), {})
        if isinstance(override, dict):
            source.update(override)

    resolved["resolved_profile"] = profile
    return resolved, profile


def load_source_config(path: Path, requested: str = "") -> tuple[dict[str, Any], str]:
    cfg = read_json(path)
    if not isinstance(cfg, dict):
        raise ValueError("sources config must be an object")
    return apply_profile(cfg, requested)
