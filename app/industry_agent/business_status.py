"""区分 Agent 技术执行、业务产出和黄金事件覆盖告警。"""

from __future__ import annotations

from typing import Any


def derive_agent_status(run_status: str, verified_event_count: int, coverage_audit_status: str) -> dict[str, str]:
    """保留兼容 status，同时给出不能被 success_empty 掩盖的业务状态。"""
    run_status = str(run_status or "failed")
    if run_status in {"success", "success_empty"}:
        technical_status = "success"
    elif run_status in {"partial_budget", "degraded"}:
        technical_status = "degraded"
    else:
        technical_status = "failed"

    if int(verified_event_count) > 0:
        business_status = "success"
    elif coverage_audit_status == "completed":
        business_status = "empty_audited"
    else:
        business_status = "empty_uncovered"
    return {
        "technical_status": technical_status,
        "business_status": business_status,
        "coverage_audit_status": str(coverage_audit_status),
    }


def evaluate_common_miss_alert(event_id: str, route_results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """用独立黄金事件/Sentinel 判断单路 warning 与多路共同漏报 critical。"""
    missing_routes = sorted(route for route, result in route_results.items() if not bool(result.get("kept")))
    if len(missing_routes) >= 2:
        severity = "critical"
    elif len(missing_routes) == 1:
        severity = "warning"
    else:
        severity = "none"
    return {
        "event_id": str(event_id),
        "severity": severity,
        "missing_routes": missing_routes,
        "missing_route_count": len(missing_routes),
        "coverage_audit_status": "completed",
    }
