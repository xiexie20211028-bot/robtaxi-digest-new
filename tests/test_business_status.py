from app.industry_agent.business_status import derive_agent_status, evaluate_common_miss_alert


def test_success_empty_requires_coverage_audit_before_business_can_be_healthy() -> None:
    status = derive_agent_status("success_empty", 0, "skipped")

    assert status == {
        "technical_status": "success",
        "business_status": "empty_uncovered",
        "coverage_audit_status": "skipped",
    }


def test_sentinel_escalates_single_route_to_warning_and_shared_miss_to_critical() -> None:
    warning = evaluate_common_miss_alert(
        "law-event",
        {"legacy": {"kept": True}, "optimized": {"kept": False}, "agent_first": {"kept": True}},
    )
    critical = evaluate_common_miss_alert(
        "law-event",
        {"legacy": {"kept": False}, "optimized": {"kept": False}, "agent_first": {"kept": True}},
    )

    assert warning["severity"] == "warning"
    assert warning["missing_routes"] == ["optimized"]
    assert critical["severity"] == "critical"
    assert critical["missing_route_count"] == 2
