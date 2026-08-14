from __future__ import annotations

import re
from typing import Any


ROBOTAXI_TERMS = {
    "robotaxi", "robo-taxi", "driverless taxi", "autonomous taxi", "self-driving taxi",
    "无人驾驶出租车", "自动驾驶出租车", "无人出租车", "萝卜快跑", "apollo go", "cybercab",
}
L3_TERMS = {"l3", "level 3", "level-3", "三级自动驾驶", "有条件自动驾驶", "drive pilot"}
L4_TERMS = {"l4", "level 4", "level-4", "四级自动驾驶", "高度自动驾驶"}
AUTONOMOUS_TERMS = {
    "自动驾驶", "无人驾驶", "autonomous driving", "automated driving", "self-driving", "driverless",
    "ads", "intelligent connected vehicle", "智能网联汽车",
}
PASSENGER_TERMS = {
    "乘用车", "轿车", "suv", "sedan", "passenger car", "passenger vehicle", "量产车型", "车型",
}
NON_PASSENGER_TERMS = {
    "robotruck", "robo-truck", "autonomous truck", "driverless truck", "self-driving truck", "freight",
    "heavy truck", "commercial motor vehicle", "无人驾驶货车", "自动驾驶货车", "无人货运", "干线物流",
    "robovan", "robo-van", "delivery van", "无人配送", "末端配送", "矿区", "港口", "码头", "矿卡",
    "yard truck", "公交车", "autonomous bus", "shuttle bus", "接驳车",
}
L2_MARKETING_TERMS = {
    "l2+", "l2++", "level 2+", "level 2++", "准l3", "准 l3", "类l3", "类 l3",
    "未来支持l3", "未来支持 l3", "future-ready for level 3", "l3-ready", "高阶智驾", "城市领航", "高速领航",
}
FORMAL_L3_TERMS = {
    "有条件自动驾驶", "责任转移", "动态驾驶任务", "脱手脱眼", "获批", "准入", "许可", "认证",
    "type approval", "level 3 approval", "approved", "permit", "certification", "conditional automated driving",
    "drive pilot",
}
UNCONFIRMED_LEVEL_TERMS = {"尚未", "未获批", "未申请", "计划申请", "未来支持", "may support", "future support"}
REGULATION_TERMS = {
    "准入", "许可", "牌照", "监管", "法规", "政策", "召回", "调查", "事故报告", "安全报告",
    "道路测试", "示范应用", "示范运营", "type approval", "approval", "permit", "regulation",
    "rulemaking", "recall", "investigation", "crash report", "safety", "wp.29", "grva", "unece",
}
MILESTONE_TERMS = {
    "量产", "定点", "交付", "上市", "商业化", "部署", "运营", "获批", "准入", "认证", "许可",
    "责任转移", "脱手脱眼", "上路", "道路测试", "示范应用", "车型", "客户", "项目",
    "production", "sop", "nomination", "award", "launch", "deploy", "commercial", "approval",
    "certification", "permit", "type approval", "customer", "vehicle program", "fleet",
}
SUPPLY_CHAIN_TERMS = {
    "芯片", "激光雷达", "域控制器", "传感器", "计算平台", "自动驾驶方案", "算法", "功能安全",
    "chip", "lidar", "sensor", "compute platform", "driving system", "software stack", "functional safety",
    "mobileye", "momenta", "nvidia", "地平线", "禾赛", "速腾聚创", "黑芝麻智能",
}
SAFETY_EVENT_TERMS = {"碰撞", "事故", "伤亡", "暂停运营", "调查", "召回", "crash", "collision", "fatal", "recall", "probe"}


def _normalize(text: str) -> str:
    compact = re.sub(r"\s+", " ", str(text or "")).strip().lower()
    return compact.replace("ｌ", "l")


def _hits(text: str, terms: set[str]) -> list[str]:
    return sorted(term for term in terms if term in text)


def classify_industry_item(row: dict[str, Any], source: dict[str, Any] | None = None) -> dict[str, Any]:
    """执行范围硬门槛，并给出可落库的结构化分类。"""
    source = source or {}
    text = _normalize(
        " ".join(
            [
                str(row.get("title", row.get("title_zh", ""))),
                str(row.get("content", row.get("summary_zh", ""))),
                str(row.get("company_hint", row.get("company_id", ""))),
                str(row.get("source_name", "")),
            ]
        )
    )
    robotaxi_hits = _hits(text, ROBOTAXI_TERMS)
    l3_hits = _hits(text, L3_TERMS)
    l4_hits = _hits(text, L4_TERMS)
    autonomous_hits = _hits(text, AUTONOMOUS_TERMS)
    passenger_hits = _hits(text, PASSENGER_TERMS)
    excluded_hits = _hits(text, NON_PASSENGER_TERMS)
    marketing_hits = _hits(text, L2_MARKETING_TERMS)
    regulation_hits = _hits(text, REGULATION_TERMS)
    milestone_hits = _hits(text, MILESTONE_TERMS)
    supply_hits = _hits(text, SUPPLY_CHAIN_TERMS)
    safety_hits = _hits(text, SAFETY_EVENT_TERMS)

    # Robotaxi 本身属于乘用出行，不因文中同时出现“车队”等泛词被误杀。
    if excluded_hits and not robotaxi_hits:
        return _classification(False, "non_passenger_scope", [], "unknown", text, locals())

    explicit_level = bool(l3_hits or l4_hits)
    formal_l3_hits = _hits(text, FORMAL_L3_TERMS)
    unconfirmed_level_hits = _hits(text, UNCONFIRMED_LEVEL_TERMS)
    if marketing_hits and not robotaxi_hits and (not formal_l3_hits or unconfirmed_level_hits):
        return _classification(False, "l2_marketing_only", [], "unknown", text, locals())

    domains: list[str] = []
    automation_level = "unknown"
    if robotaxi_hits:
        domains.append("robotaxi")
        automation_level = "L4"
    if l3_hits and (autonomous_hits or passenger_hits or milestone_hits):
        domains.append("passenger_l3")
        automation_level = "L3"
    if l4_hits and (autonomous_hits or passenger_hits or milestone_hits):
        domains.append("passenger_l4")
        automation_level = "L4"

    core_context = bool(robotaxi_hits or l3_hits or l4_hits)
    if supply_hits and milestone_hits and core_context:
        domains.append("core_supply_chain")
    if (regulation_hits or safety_hits) and core_context:
        domains.append("regulation_safety")

    configured_domains = {
        str(value).strip().lower()
        for value in source.get("coverage_domains", [])
        if str(value).strip()
    }
    regulator_evidence = str(source.get("evidence_type", "")) in {"regulator", "dataset"}
    if regulator_evidence and regulation_hits and (explicit_level or robotaxi_hits):
        domains.append("regulation_safety")

    if not domains:
        return _classification(False, "scope_gate_miss", [], automation_level, text, locals())

    # 供应链文章必须绑定明确项目、客户或量产/准入里程碑。
    if configured_domains == {"core_supply_chain"} and not (core_context and milestone_hits):
        return _classification(False, "supply_chain_without_l3_l4_binding", [], automation_level, text, locals())

    event_type = _infer_event_type(text)
    deployment_stage = _infer_deployment_stage(text)
    result = _classification(True, "in_scope", sorted(set(domains)), automation_level, text, locals())
    result["event_type"] = event_type
    result["deployment_stage"] = deployment_stage
    return result


def _classification(
    in_scope: bool,
    reason: str,
    domains: list[str],
    automation_level: str,
    text: str,
    namespace: dict[str, Any],
) -> dict[str, Any]:
    _ = text
    keys = (
        "robotaxi_hits", "l3_hits", "l4_hits", "autonomous_hits", "passenger_hits", "excluded_hits",
        "marketing_hits", "regulation_hits", "milestone_hits", "supply_hits", "safety_hits",
        "formal_l3_hits",
        "unconfirmed_level_hits",
    )
    return {
        "in_scope": in_scope,
        "scope_reason": reason,
        "coverage_domains": domains,
        "automation_level": automation_level,
        "event_type": "other",
        "deployment_stage": "unknown",
        "scope_signals": {key: list(namespace.get(key, [])) for key in keys},
    }


def _infer_event_type(text: str) -> str:
    candidates = (
        ("safety_incident", SAFETY_EVENT_TERMS),
        ("regulation", REGULATION_TERMS),
        ("commercial_deployment", {"商业化", "运营", "上线", "launch", "deploy", "fleet"}),
        ("approval", {"获批", "准入", "许可", "认证", "approval", "permit", "certification"}),
        ("production", {"量产", "定点", "交付", "sop", "production", "nomination"}),
        ("partnership", {"合作", "签约", "partner", "partnership"}),
    )
    for label, terms in candidates:
        if any(term in text for term in terms):
            return label
    return "product_update"


def _infer_deployment_stage(text: str) -> str:
    candidates = (
        ("commercial", {"商业化", "收费运营", "paid service", "commercial operation"}),
        ("production", {"量产", "交付", "sop", "mass production"}),
        ("pilot", {"试点", "示范运营", "试运营", "pilot", "testing permit"}),
        ("approved", {"获批", "准入", "许可", "approval", "type approval"}),
        ("development", {"研发", "测试", "road test", "development"}),
    )
    for label, terms in candidates:
        if any(term in text for term in terms):
            return label
    return "unknown"


def validate_social_candidate(row: dict[str, Any], source: dict[str, Any]) -> tuple[bool, str]:
    """P1 社交候选的最小证据门槛；无永久链接或时间即拒绝。"""
    if str(source.get("source_role", "")) != "social_discovery":
        return True, "not_social"
    if not bool(row.get("official_account_verified", False)):
        return False, "social_official_account_unverified"
    if not str(row.get("published_at_utc", "")).strip() or bool(row.get("published_missing", False)):
        return False, "social_published_unverified"
    if not str(row.get("canonical_url", row.get("link", ""))).strip():
        return False, "social_permalink_missing"
    text = _normalize(f"{row.get('title', '')} {row.get('content', '')}")
    if any(term in text for term in {"reposted", "retweet", "转发", "回复", "招聘", "hiring", "预热", "敬请期待"}):
        return False, "social_low_value_post"
    return True, "social_verified"
