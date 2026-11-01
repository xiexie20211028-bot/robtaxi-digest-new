"""历史业务交付的固定任务验收器；不执行模型、网络或任意请求命令。"""
from __future__ import annotations

import json
from pathlib import Path

from .development_policy import digest, require

NAMES = {46: "law_discovery_v1", 73: "law_filter_v1", 83: "decision_trace_v1", 84: "business_status_v1"}


def binding_for(issue: int) -> dict:
    require(issue in NAMES, "任务没有可信离线业务验收器")
    return {"kind": "task", "mode": "offline_replay", "verifier": NAMES[issue], "verifier_version": 1}


def evaluate_offline(issue: int) -> dict:
    """运行原业务行为，保存逐项结论；未知条件不能被测试通过覆盖。"""
    from scripts.replay_golden_event import load_event, replay_event, ROUTES
    from .decision_log import build_candidate_decision
    from .industry_agent.business_status import derive_agent_status, evaluate_common_miss_alert

    binding = binding_for(issue)
    root = Path(__file__).resolve().parent.parent
    fixture = root / "tests/fixtures/golden_events/road_traffic_safety_law_2026-08-25.json"
    event = load_event(fixture)
    report = replay_event(event)
    rows = report["results"]
    checks = {}
    if issue in {46, 73}:
        minimum = 2 if issue == 46 else 3
        kept = [r for r in rows.values() if r.get("kept") is True]
        checks["independent_routes"] = len(kept) >= minimum and len({r.get("discovery_url") for r in kept}) >= minimum
        if issue == 73:
            negatives = report.get("negative_controls", [])
            checks["negative_controls"] = len(negatives) >= 20 and all(r.get("kept") is False for r in negatives)
            # 原标准为shadow噪声增长；黄金负例准确率不是同一个指标。
            checks["shadow_noise_growth_at_most_10_percent"] = None
    elif issue == 83:
        fields = {"route", "candidate_id", "title", "source", "stage", "signals", "score", "threshold", "kept", "final_reason"}
        checks["three_route_records"] = set(rows) == set(ROUTES) and all(fields.issubset(r) for r in rows.values())
        checks["route_identity"] = all(row.get("route") == route for route, row in rows.items())
        checks["stage_and_reason"] = all(r.get("stage") and r.get("final_reason") and r.get("candidate_id") for r in rows.values())
        # 使用专门标记的敏感输入验证投影，不把敏感原文存进正式证据。
        marker = "PRIVATE-ACCEPTANCE-CONTENT-DO-NOT-LOG"
        record = build_candidate_decision(route="legacy", candidate={"id": "privacy", "title": "安全样本",
            "content": marker, "secret": marker, "user_data": marker}, source={}, stage="test", kept=False, final_reason="test")
        checks["privacy_projection"] = marker not in json.dumps(record) and all(event["content"] not in json.dumps(r, ensure_ascii=False) for r in rows.values())
    else:
        checks["technical_vs_business"] = derive_agent_status("success_empty", 0, "skipped") == {
            "technical_status": "success", "business_status": "empty_uncovered", "coverage_audit_status": "skipped"}
        checks["audited_empty"] = derive_agent_status("success_empty", 0, "completed")["business_status"] == "empty_audited"
        checks["technical_failure"] = derive_agent_status("failed", 0, "failed")["technical_status"] == "failed"
        checks["single_and_shared_misses"] = True
        for missing in range(4):
            route_rows = {route: {"kept": i >= missing} for i, route in enumerate(ROUTES)}
            alert = evaluate_common_miss_alert(event["event_id"], route_rows)
            checks["single_and_shared_misses"] &= alert["severity"] == ("none" if missing == 0 else "warning" if missing == 1 else "critical") and alert["missing_route_count"] == missing
        # 黄金事件在路线结果之前独立存在；三条路线同时漏掉仍必须告警。
        empty = {r: {"kept": False} for r in ROUTES}
        checks["independent_sentinel"] = evaluate_common_miss_alert(event["event_id"], empty)["severity"] == "critical"
    return {"binding": binding, "checks": checks,
            "acceptance_passed": False if any(v is False for v in checks.values()) else None if any(v is None for v in checks.values()) else True,
            "fixture_sha256": digest(event), "result_digest": digest(report),
            "mode": "offline_replay"}
