"""运行结果、停滞和人工交接必须由证据决定。"""
from copy import deepcopy
from datetime import datetime, timedelta, timezone

from app.development_cycle import load_policy
from app.development_outcomes import (build_outcome, decision_state, production_deadline, publish_outcome, render_outcome, resolve_decision)
from app.development_policy import digest

NOW = datetime(2026, 10, 27, 4, 0, tzinfo=timezone.utc)
WORKFLOW = "schedule:\n - cron: '0 1 * * *'\n"


def build(*, now=NOW, events=None, state=None, decision=None, receipt=None, policy=None, pilot=None, trigger="schedule"):
    decision = decision if decision is not None else {"run_id": "100", "date_bj": now.astimezone(timezone(timedelta(hours=8))).date().isoformat(),
        "run_evidence": {"event_name": "schedule", "health_report_available": True}, "incidents": {}, "actions": []}
    receipt = receipt if receipt is not None else {"complete": True, "mode": "apply", "development_run_id": "current-run", "official_state": {"complete": True}, "decision_digest": digest(decision)}
    return build_outcome(policy=policy or load_policy(), state=state or {"tasks": [], "next_action": None}, decision=decision,
        receipt=receipt, events=events or [], run_started={"run_id": "current-run", "at": (now-timedelta(minutes=30)).isoformat()},
        now=now, workflow=WORKFLOW, trigger=trigger, pilot_task=pilot)


def test_critical_and_approval_survive_zero_operations():
    decision = {"run_id": "100", "date_bj": "2026-10-27", "run_evidence": {"event_name": "schedule", "health_report_available": True},
        "incidents": {"review:gap": {"incident_key": "review:gap", "severity": "critical"}},
        "actions": [{"action": "needs_approval", "incident_key": "review:gap", "summary": "重要事件覆盖未达标", "severity": "critical"}]}
    result = build(decision=decision)
    assert result["inspection"] == "complete"
    assert result["conclusion"] == "needs_approval" and result["health_severity"] == "critical"
    assert "严重异常" in render_outcome(result) and "重要事件覆盖" in render_outcome(result)


def test_completed_pilot_is_pending_activation_not_empty_queue():
    result = build(pilot={"state": "CLOSED"})
    assert result["conclusion"] == "needs_approval"
    assert result["pending_decisions"][0]["decision_id"] == "pilot_complete_awaiting_activation"
    assert "试点已完成" in render_outcome(result)


def test_heartbeat_and_old_receipt_do_not_mean_inspection_complete():
    result = build(receipt={"complete": True, "mode": "apply", "development_run_id": "yesterday-run", "official_state": {"complete": True}})
    assert result["conclusion"] == "inspection_incomplete"
    assert result["health_severity"] == "unknown"
    assert build(decision={}, receipt={})["conclusion"] == "inspection_incomplete"


def test_two_distinct_scheduled_runs_without_progress_escalate_but_same_day_does_not():
    state = {"tasks": [{"number": 57}], "next_action": {"kind": "plan", "issue": 57}}
    first = build(now=NOW-timedelta(days=1), state=state)
    assert first["stalled_count"] == 1
    events = [{"event": "run_outcome", "result": first}]
    second = build(events=events, state=state)
    assert second["conclusion"] == "stalled" and second["stalled_count"] == 2
    same_day = build(now=NOW-timedelta(days=1), events=events, state=state)
    assert same_day["stalled_count"] == 1
    events[0]["result"]["trigger"] = "manual"
    assert build(events=events, state=state)["stalled_count"] == 1


def test_comments_and_unrelated_main_change_are_not_progress_but_bound_head_is():
    state = {"main_sha": "a", "tasks": [{"number": 57}], "next_action": {"kind": "plan", "issue": 57}}
    previous = build(now=NOW-timedelta(days=1), state=state)
    changed = deepcopy(state);changed["main_sha"] = "b"
    changed["tasks"][0]["events"] = [{"event": "heartbeat", "run_id": "current-run"}]
    events = [{"event": "run_outcome", "result": previous}]
    assert build(state=changed, events=events)["conclusion"] == "stalled"
    changed["tasks"][0]["open_pr"] = {"headRefOid": "c" * 40}
    assert build(state=changed, events=events)["conclusion"] == "progress"


def test_production_freshness_does_not_hide_stall_of_an_executable_task():
    decision = {"date_bj": "2026-10-26", "run_evidence": {"event_name": "schedule", "health_report_available": True}}
    state = {"tasks": [{"number": 57}], "next_action": {"kind": "plan", "issue": 57}}
    before = build(now=NOW.replace(hour=2, minute=30), decision=decision, state=state)
    assert before["evidence_status"] == "waiting" and before["conclusion"] == "waiting_production"
    after = build(decision=decision, state=state)
    assert after["conclusion"] == "evidence_overdue" and after["stalled_count"] == 1
    waiting_state = {"tasks": [{"number": 57, "awaiting_production": {"pr": 10}}], "next_action": None}
    assert build(decision=decision, state=waiting_state)["stalled_count"] == 0
    assert production_deadline(WORKFLOW, NOW).astimezone(timezone(timedelta(hours=8))).hour == 11
    assert production_deadline(WORKFLOW.replace("0 1", "0 2"), NOW).astimezone(timezone.utc).hour == 4


def test_missing_source_report_cannot_be_healthy_even_with_complete_receipt():
    result = build(decision={"date_bj": "2026-10-27", "run_evidence": {"event_name": "schedule", "health_report_available": False}})
    assert result["health_severity"] == "unknown" and result["inspection"] == "incomplete"


def test_acknowledgement_does_not_resolve_and_reminder_boundaries_are_24_72_hours():
    first = {"event": "decision_required", "decision_id": "approval:x", "at": (NOW-timedelta(hours=24)).isoformat(), "reason": "需要批准"}
    events = [first, {"event": "decision_acknowledged", "decision_id": "approval:x"}]
    assert decision_state(events, [], NOW)[0]["reminder_due"]
    events.append({"event": "decision_reminder", "decision_id": "approval:x", "reminder_index": 1})
    assert not decision_state(events, [], NOW+timedelta(hours=71))[0]["reminder_due"]
    assert decision_state(events, [], NOW+timedelta(hours=72))[0]["reminder_due"]
    events.append({"event": "decision_resolved", "decision_id": "approval:x"})
    assert decision_state(events, [], NOW)  # 旧记录缺处理结论，仍为未知，不能当批准。
    events.append({"event": "decision_resolved", "decision_id": "approval:x", "resolution": "approved"})
    assert decision_state(events, [], NOW) == []


def test_publish_retry_and_same_day_report_are_idempotent():
    rows = []
    class Client:
        def events(self):return list(rows)
        def append(self, row):rows.append(row)
    def notification(event_id, text):
        return {"event": "notification_requested", "key": digest(event_id), "text": text}
    first = build(pilot={"state": "CLOSED"})
    publish_outcome(Client(), first, notification)
    size = len(rows)
    publish_outcome(Client(), first, notification)
    assert len(rows) == size
    later = {**first, "run_id": "another-run"}
    publish_outcome(Client(), later, notification)
    assert len([r for r in rows if r["event"] == "notification_requested"]) == 1


def test_long_pending_reasons_still_fit_existing_channels():
    result = build()
    result["pending_decisions"] = [{"reason": "长文本"*1000} for _ in range(10)]
    assert len(render_outcome(result).encode()) <= 1500


def test_last_known_critical_is_preserved_when_todays_production_is_late():
    decision = {"run_id": "100", "date_bj": "2026-10-26", "run_evidence": {"event_name": "schedule", "health_report_available": True},
                "incidents": {"source:x:empty": {"incident_key": "source:x:empty", "severity": "critical", "lifecycle": "open"}}, "actions": []}
    result = build(decision=decision)
    assert result["evidence_status"] == "overdue" and result["health_severity"] == "critical"
    assert result["health_evidence_status"] == "last_known" and result["requires_attention"]
    assert "最近已知异常" in render_outcome(result)


def test_progress_cannot_hide_critical_or_pending_decision():
    decision = {"run_id": "100", "date_bj": "2026-10-27", "run_evidence": {"event_name": "schedule", "health_report_available": True},
                "incidents": {"source:x:empty": {"incident_key": "source:x:empty", "severity": "critical"}}, "actions": []}
    state = {"tasks": [{"number": 57, "contract_digest": "a" * 64,
                        "events": [{"event": "contract", "run_id": "current-run", "key": "new-contract"}]}], "next_action": None}
    events = [{"event": "decision_required", "decision_id": "keyword", "at": NOW.isoformat(), "reason": "核实关键词"}]
    result = build(decision=decision, state=state, events=events)
    assert result["progress_issues"] == [57] and result["health_severity"] == "critical" and result["pending_decisions"]
    assert result["requires_attention"] and "仍有异常" in render_outcome(result)


def test_reposting_same_checkpoint_does_not_create_progress():
    state = {"tasks": [{"number": 57, "events": [{"event": "checkpoint", "stage": "tests_passed", "head_sha": "a" * 40}]}],
             "next_action": {"kind": "execute", "issue": 57}}
    old = build(now=NOW-timedelta(days=1), state=state)
    changed = deepcopy(state)
    changed["tasks"][0]["events"].append({"event": "checkpoint", "stage": "tests_passed", "head_sha": "a" * 40, "run_id": "current-run"})
    result = build(state=changed, events=[{"event": "run_outcome", "result": old}])
    assert result["progress_issues"] == [] and result["stalled_count"] == 2


def test_closed_task_delivery_and_verification_appear_from_control_evidence():
    state = {"tasks": [{"number": 101, "state": "CLOSED", "events": []}], "next_action": None, "budget_seconds_remaining": 600}
    events = [{"event": "delivery", "issue": 101, "key": "delivery-one", "run_id": "current-run"},
              {"event": "production_verified", "issue": 101, "key": "verified-one", "run_id": "current-run"}]
    result = build(state=state, events=events)
    assert result["merged_issues"] == result["verified_issues"] == [101]
    assert result["budget_seconds_remaining"] == 600 and "10分钟" in render_outcome(result)


def test_manual_assistance_never_consumes_scheduled_daily_notification_slot():
    rows = []
    class Client:
        def events(self): return rows
        def append(self, row): rows.append(row)
    def notification(event_id, text): return {"event": "notification_requested", "key": digest(event_id), "text": text}
    result = build(trigger="manual", pilot={"state": "CLOSED"})
    publish_outcome(Client(), result, notification)
    assert any(e["event"] == "run_outcome" for e in rows)
    assert not any(e["event"] in {"notification_requested", "decision_reminder"} for e in rows)


def test_failed_reminder_enqueue_does_not_consume_reminder_index():
    import pytest
    rows = []
    class Client:
        def events(self): return rows
        def append(self, row):
            if row["event"] == "notification_requested": raise OSError("记录未确认")
            rows.append(row)
    old = {"event": "decision_required", "decision_id": "approval:x", "at": (NOW-timedelta(hours=24)).isoformat(), "reason": "待处理"}
    result = build(events=[old]); rows.append(old)
    with pytest.raises(OSError):
        publish_outcome(Client(), result, lambda event_id, text: {"event": "notification_requested", "key": digest(event_id), "text": text})
    assert not any(e["event"] == "decision_reminder" for e in rows)


def test_explicit_resolution_is_idempotent_and_does_not_grant_policy_authority():
    import pytest
    from app.development_policy import DevelopmentError
    rows = [{"event": "decision_required", "decision_id": "activation", "at": NOW.isoformat()}]
    class Client:
        def events(self): return rows
        def append(self, row): rows.append(row)
    with pytest.raises(DevelopmentError): resolve_decision(Client(), "activation", "received", "已收到")
    event = resolve_decision(Client(), "activation", "approved", "用户明确处理依据")
    assert event["authorization_changed"] is False
    assert resolve_decision(Client(), "activation", "approved", "同一依据") == event
    assert len(rows) == 2 and load_policy()["mode"] == "pilot"
