from __future__ import annotations

from app.health_issue import build_incidents
from app.health_loop import evaluate_health_loop, incident_key, render_daily_report


def _health(
    *,
    run_id: str = "100",
    severity: str = "warning",
    source_id: str = "miit_news_structured",
    reason_code: str = "low_date_parse_rate",
    event_name: str = "schedule",
) -> dict:
    return {
        "date_bj": "2026-09-04",
        "overall_status": severity,
        "source_report": {"available": True},
        "run": {"github_run_id": run_id, "event_name": event_name},
        "findings": [
            {
                "check_id": "required_source_failure_rate",
                "severity": severity,
                "summary": "必需信源异常",
                "evidence": {"failed_sources": [{"source_id": source_id, "reason_code": reason_code}]},
            }
        ],
    }


def _state(key: str, **overrides: object) -> dict:
    record = {
        "source_id": "miit_news_structured",
        "reason_code": "low_date_parse_rate",
        "occurrence_count": 1,
        "last_run_id": "99",
        "engineering_issue": 49,
        "merged_commit_reachable": False,
        "recovery_count": 0,
    }
    record.update(overrides)
    return {"incidents": {key: record}}


def test_source_incident_identity_ignores_check_id() -> None:
    warning = build_incidents(_health(severity="warning"))[0]
    critical_health = _health(severity="critical")
    critical_health["findings"][0]["check_id"] = "source_health_escalation"
    critical = build_incidents(critical_health)[0]

    assert warning["incident_id"] == critical["incident_id"]
    assert incident_key(source_id="miit", reason_code="low_date_parse_rate", check_id="a") == incident_key(
        source_id="miit", reason_code="low_date_parse_rate", check_id="b"
    )


def test_first_warning_is_observed_then_second_creates_task() -> None:
    first = evaluate_health_loop(_health(run_id="100"))
    assert first["actions"][0]["action"] == "observe"
    key = first["actions"][0]["incident_key"]

    second = evaluate_health_loop(_health(run_id="101"), previous_state=first)
    assert second["actions"][0]["action"] == "create_task"
    assert second["incidents"][key]["occurrence_count"] == 2


def test_critical_creates_task_immediately() -> None:
    state = evaluate_health_loop(_health(severity="critical"))
    assert state["actions"][0]["action"] == "create_task"
    assert state["actions"][0]["risk"] == "Medium"


def test_external_failure_never_requests_code_change() -> None:
    state = evaluate_health_loop(_health(severity="critical", reason_code="ssl_error"))
    assert state["actions"][0]["action"] == "no_code_change"


def test_recovery_requires_schedule_source_evidence_and_two_runs() -> None:
    key = incident_key(
        source_id="miit_news_structured", reason_code="low_date_parse_rate", check_id="required_source_failure_rate"
    )
    previous = _state(key, merged_commit_reachable=True)
    healthy = {
        "date_bj": "2026-09-05",
        "overall_status": "healthy",
        "source_report": {"available": True},
        "run": {"github_run_id": "101", "event_name": "schedule"},
        "findings": [],
    }
    run_report = {"source_stats": [{"source_id": "miit_news_structured", "status": "ok"}]}

    first = evaluate_health_loop(healthy, previous_state=previous, run_report=run_report)
    assert first["actions"][0]["action"] == "verify"
    second_health = {**healthy, "run": {"github_run_id": "102", "event_name": "schedule"}}
    second = evaluate_health_loop(second_health, previous_state=first, run_report=run_report)
    assert second["actions"][0]["action"] == "close"


def test_manual_or_missing_source_run_cannot_close() -> None:
    key = incident_key(
        source_id="miit_news_structured", reason_code="low_date_parse_rate", check_id="required_source_failure_rate"
    )
    previous = _state(key, merged_commit_reachable=True)
    health = {
        "date_bj": "2026-09-05",
        "overall_status": "healthy",
        "source_report": {"available": True},
        "run": {"github_run_id": "101", "event_name": "workflow_dispatch"},
        "findings": [],
    }
    assert evaluate_health_loop(health, previous_state=previous, run_report={})["actions"] == []


def test_active_engineering_task_is_not_recreated_but_observation_failure_reopens_it() -> None:
    key = incident_key(
        source_id="miit_news_structured", reason_code="low_date_parse_rate", check_id="required_source_failure_rate"
    )
    open_task = evaluate_health_loop(_health(severity="critical"), previous_state=_state(key))
    assert open_task["actions"][0]["action"] == "observe"

    observing = _state(key, lifecycle="observing", merged_commit_reachable=True)
    reopened = evaluate_health_loop(_health(severity="critical"), previous_state=observing)
    assert reopened["actions"][0]["action"] == "create_task"
    assert reopened["actions"][0]["reopen"] is True


def test_review_business_gap_requires_approval() -> None:
    review = {
        "daily": {"business_status": "empty_uncovered"},
        "gate": {"metrics": {"truth_important": 0}, "automatic_checks": {}},
    }
    state = evaluate_health_loop(_health(), review=review)
    assert any(row["action"] == "needs_approval" for row in state["actions"])


def test_report_is_change_focused() -> None:
    state = evaluate_health_loop(_health(severity="critical"))
    report = render_daily_report(state)
    assert "## 新发现" in report
    assert "需要你批准" not in report


def test_unchanged_incident_is_saved_but_not_reported_again() -> None:
    first = evaluate_health_loop(_health(severity="critical"))
    second = evaluate_health_loop(_health(run_id="101", severity="critical"), previous_state=first)
    assert second["actions"]
    assert second["changed"] is False
    assert second["changed_actions"] == []
