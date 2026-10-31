"""弹性吞吐不放宽并发、预算、恢复与相关交付验收边界。"""
from datetime import datetime, timedelta, timezone
import copy

import pytest

from app.development_budget import daily_seconds, remaining_seconds, impact_from_diff, conflicts, require_independent, pending_deliveries, verifier_for_task
from app.development_cycle import active_claim, claim, release_task, load_policy, initialize_run, checkpoint
from app.development_runtime import GitHub
from app.development_policy import DevelopmentError, reserve, select_tasks, validate_policy, digest


def policy():
    return {**load_policy(), "schema_version": "robtaxi-autonomy-policy-v3", "throughput": "budgeted",
            "daily_tasks": None, "daily_merges": None, "daily_runtime_seconds": 5400,
            "queue_order": "critical_then_priority_then_stage",
            "stop_conditions": load_policy()["stop_conditions"] + ["daily_budget_exhausted"]}


def run_event(run, at, minutes):
    return {"event": "run_started", "key": f"run:{run}", "run_id": run, "at": at.isoformat(),
            "deadline": (at + timedelta(minutes=minutes)).isoformat()}


def test_daily_budget_rebuilds_completed_and_interrupted_reservations():
    now = datetime(2026, 10, 29, 4, tzinfo=timezone.utc)
    start = now - timedelta(hours=2)
    rows = [run_event("finished", start, 90),
            {"event": "run_finished", "run_id": "finished", "at": (start + timedelta(minutes=20)).isoformat()},
            run_event("interrupted", now - timedelta(minutes=40), 30)]
    assert daily_seconds(rows, now) == 3000
    assert remaining_seconds(rows, policy(), now) == 2400
    rows += [run_event("current", now - timedelta(minutes=5), 40)]
    assert daily_seconds(rows, now) == 5400
    assert remaining_seconds(rows, policy(), now, active_run_id="current") == 2100


def test_midnight_does_not_refresh_existing_deadline():
    now = datetime(2026, 10, 29, 16, 10, tzinfo=timezone.utc)
    start = now - timedelta(minutes=30)
    rows = [run_event("cross-day", start, 90)]
    assert daily_seconds(rows, now, active_run_id="cross-day") == 600
    assert daily_seconds(rows, now) == 4200
    assert rows[0]["deadline"] == (start + timedelta(minutes=90)).isoformat()


def test_v3_removes_count_gate_but_requires_real_run_and_budget():
    p = policy(); validate_policy(p)
    now = datetime.now(timezone.utc); rows = [run_event("real-run", now - timedelta(minutes=1), 30)]
    first = reserve(rows, p, now, "merge", "merge-one"); rows.append(first)
    assert reserve(rows, p, now, "merge", "merge-two")["kind"] == "merge"
    with pytest.raises(DevelopmentError): reserve(rows, p, now, "merge", "merge-one")
    with pytest.raises(DevelopmentError): reserve([], p, now, "task", "orphan")
    p["daily_tasks"] = 3
    with pytest.raises(DevelopmentError): validate_policy(p)


def task(n, **kwargs):
    return {"number": n, "state": "OPEN", "status": "待办", "priority": "P1", "type": "Bug",
            "target": "本周", "labels": [], "blockers": [], "assignees": 1, "contract": None,
            "risk": "Medium", "events": [], **kwargs}


def test_actionable_critical_precedes_p0_and_blocked_critical_does_not_starve_queue():
    p = {**policy(), "mode": "active"}
    rows = [task(1, priority="P0"), task(2, health_actionable=True, health_severity="critical"),
            task(3, health_actionable=True, health_severity="critical", blocked_reason="限流未解除")]
    result = select_tasks(rows, p)
    assert result["next_action"] == {"kind": "plan", "issue": 2}
    assert {"issue": 3, "reason": "限流未解除"} in result["excluded"]
    rows[1]["repair_attempts"] = 2
    assert select_tasks(rows, p)["next_action"]["issue"] == 1


def test_only_actual_single_source_diff_proves_independence():
    before = {"defaults": {"limit": 4}, "sources": [{"id": "x", "url": "old"}, {"id": "y", "url": "same"}]}
    after = copy.deepcopy(before); after["sources"][0]["url"] = "new"
    assert impact_from_diff(["sources.json", "tests/test_x.py"], before=before, after=after) == ["source:x"]
    after["defaults"]["limit"] = 3
    assert impact_from_diff(["sources.json"], before=before, after=after) == ["shared:production"]
    assert impact_from_diff(["app/filter.py"]) == ["shared:production"]
    assert impact_from_diff(["docs/guide.md", "tests/test_x.py"]) == []
    assert conflicts(None, []) and conflicts(["shared:production"], ["source:x"])
    assert not conflicts(["source:x"], ["source:y"])


def test_pending_deliveries_block_only_related_scope_and_retire_verified_regression_chain():
    delivery = {"event": "delivery", "issue": 1, "merge_sha": "old", "impact_keys": ["source:x"], "production": {"kind": "source"}}
    rows = [delivery]
    require_independent(["source:y"], rows, 2)
    with pytest.raises(DevelopmentError): require_independent(["source:x"], rows, 2)
    with pytest.raises(DevelopmentError): require_independent(["source:x"], rows, 1)
    rows += [{"event": "production_requeue", "merge_sha": "old"}]
    require_independent(["source:x"], rows, 1)
    rows += [{**delivery, "merge_sha": "new", "supersedes": ["old"]}, {"event": "production_verified", "issue": 1, "merge_sha": "new"}]
    assert pending_deliveries(rows) == []


def test_historical_unknown_impact_blocks_until_verified_or_bound_regression():
    rows = [{"event": "historical_delivery_adopted", "issue": 46, "key": "adoption", "facts": {
        "merge_sha": "old", "production": {"kind": "task"}}}]
    with pytest.raises(DevelopmentError): require_independent(["source:x"], rows, 57)
    with pytest.raises(DevelopmentError): require_independent(["shared:production"], rows, 46)
    rows.append({"event": "historical_production_requeue", "merge_sha": "old", "adoption_key": "wrong"})
    with pytest.raises(DevelopmentError): require_independent(["shared:production"], rows, 46)
    rows.append({"event": "historical_production_requeue", "merge_sha": "old", "adoption_key": "adoption"})
    require_independent(["shared:production"], rows, 46)
    rows.append({"event": "historical_production_verified", "issue": 46, "merge_sha": "old", "adoption_key": "adoption"})
    require_independent(["source:x"], rows, 57)


class Client:
    def __init__(self):
        self.policy = policy(); self.run_id = "budgeted-run"; self.control = []
        self.history = {1: [], 2: []}; now = datetime.now(timezone.utc)
        self.run_started = run_event(self.run_id, now, 30); self.control.append(self.run_started)
        self.control.append({"event": "health_queue_updated", "run_id": self.run_id, "global_state_valid": True})
        self.budget = type("Budget", (), {"check": lambda self: None, "remaining": lambda self: 1800})()
        self.budget.deadline = now + timedelta(minutes=30)
    def events(self, issue=None): return self.control if issue is None else self.history[issue]
    def append(self, event, issue=None): self.events(issue).append(event)
    def set_project_status(self, issue, status): pass


def test_release_then_claim_another_task_same_day_and_reject_stale_evidence(monkeypatch):
    client = Client(); p = {**client.policy, "mode": "active"}; client.policy = p
    monkeypatch.setattr("app.development_cycle.assert_trusted_checkout", lambda _: "a" * 40)
    rows = [task(1), task(2)]
    def state(n): return {"main_sha": "a" * 40, "tasks": rows, "events": client.control, "conflicting_running": [], "next_action": {"kind": "plan", "issue": n}}
    first = claim(client, p, state(1), issue=1)["claim"]
    with pytest.raises(DevelopmentError): claim(client, p, state(2), issue=2)
    with pytest.raises(DevelopmentError): release_task(client, p, state(1), 1, "planned")
    client.append({"event": "contract", "key": "saved-plan", "claim_key": first["key"]}, 1)
    release_task(client, p, state(1), 1, "planned")
    release_task(client, p, state(1), 1, "planned")
    assert active_claim(client.control, 1, datetime.now(timezone.utc)) is None
    second = claim(client, p, state(2), issue=2)["claim"]
    assert first["key"] != second["key"] and second["issue"] == 2
    assert len([e for e in client.control if e.get("event") == "task_released"]) == 1
    # 即使任务侧评论落后，控制记录中的释放也必须禁止旧任务继续写。
    rows[0]["events"] = [first]
    with pytest.raises(DevelopmentError, match="有效"):
        checkpoint(client, p, state(1), 1, {"event": "checkpoint", "stage": "blocked", "summary": "旧上下文"})


def test_v3_claim_cannot_reuse_old_run_health_proof(monkeypatch):
    client = Client(); p = {**client.policy, "mode": "active"}; client.policy = p
    monkeypatch.setattr("app.development_cycle.assert_trusted_checkout", lambda _: "a" * 40)
    for row in client.control:
        if row.get("event") == "health_queue_updated": row["run_id"] = "yesterday"
    state = {"main_sha": "a" * 40, "tasks": [task(1)], "events": client.control,
             "conflicting_running": [], "next_action": {"kind": "plan", "issue": 1}}
    with pytest.raises(DevelopmentError, match="本轮尚无可信"):
        claim(client, p, state, issue=1)


def test_insufficient_remaining_budget_cannot_start_another_issue(monkeypatch):
    client = Client(); p = {**client.policy, "mode": "active"}
    client.budget.remaining = lambda: 899
    monkeypatch.setattr("app.development_cycle.assert_trusted_checkout", lambda _: "a" * 40)
    state = {"main_sha": "a" * 40, "tasks": [task(1)], "events": client.control,
             "conflicting_running": [], "next_action": {"kind": "plan", "issue": 1}}
    with pytest.raises(DevelopmentError, match="15分钟"):
        claim(client, p, state, issue=1)
    assert not any(e.get("event") == "task_claimed" for e in client.control)


def test_new_run_only_receives_reconstructed_remaining_daily_time(monkeypatch, tmp_path):
    now = datetime.now(timezone.utc).replace(hour=4, minute=0, second=0, microsecond=0)
    class FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None): return now.astimezone(tz or timezone.utc)
    monkeypatch.setattr("app.development_cycle.datetime", FrozenDatetime)
    monkeypatch.setattr("app.development_recovery.datetime", FrozenDatetime)
    earlier = now - timedelta(minutes=30)
    rows = [run_event("earlier-run", earlier, 90),
            {"event": "run_finished", "run_id": "earlier-run", "at": (earlier + timedelta(minutes=20)).isoformat()}]
    client = GitHub(policy())
    monkeypatch.setattr(client, "events", lambda: rows)
    monkeypatch.setattr(client, "append", rows.append)
    monkeypatch.setattr("app.development_cycle.assert_trusted_checkout", lambda _: "a" * 40)
    monkeypatch.setattr("app.development_cycle.ROOT", tmp_path)
    event = initialize_run(client, client.policy, "new-budgeted-run", start=True)
    duration = (datetime.fromisoformat(event["deadline"]) - datetime.fromisoformat(event["at"])).total_seconds()
    assert 4199 <= duration <= 4200


def test_health_queries_use_recoverable_transport_but_mutations_do_not():
    from app.health_loop_sync import GhMetadataClient
    calls = []
    class Transport:
        def gh_read(self, *args): calls.append("read"); return "{}"
        def gh(self, *args): calls.append("write"); return "{}"
    client = GhMetadataClient(); client.transport = Transport()
    client._run(["api", "graphql", "-f", "query=query { viewer { login } }"])
    client._run(["api", "graphql", "-f", "query=mutation { change }"])
    client._run(["issue", "comment", "1", "--body", "正式事件"])
    assert calls == ["read", "write", "write"]


def test_old_policy_context_cannot_be_upgraded_to_get_new_budget(monkeypatch, tmp_path):
    now = datetime.now(timezone.utc); rows = [run_event("old-v2-run", now - timedelta(minutes=5), 90)]
    client = GitHub(policy()); monkeypatch.setattr(client, "events", lambda: rows)
    monkeypatch.setattr("app.development_cycle.ROOT", tmp_path)
    with pytest.raises(DevelopmentError, match="旧策略"):
        initialize_run(client, client.policy, "old-v2-run", start=True)
    assert len(rows) == 1


def test_source_verifier_requires_completed_formal_health_link_not_contract_claim():
    p = {**policy(), "automatic_source_verifiers": True}
    binding = {"issue": 150, "source_id": "src", "incident_key": "source:src:empty", "kind": "source", "name": "source_health_v1", "required_runs": 3}
    event = {"event": "source_verifier_bound", "binding": binding, "key": f"source-verifier:{digest(binding)}", "health_queue_key": "health-proof", "production_run_id": "100"}
    proof = {"event": "health_queue_updated", "key": "health-proof", "complete": True, "production_run_id": "100",
             "entries": [{"issue": 150, "source_id": "src", "incident_key": "source:src:empty", "actionable": True}]}
    assert verifier_for_task(p, 150, [event]) == {}
    assert verifier_for_task(p, 150, [proof, event])["required_runs"] == 3
    assert verifier_for_task(p, 151, [proof, event]) == {}
    proof["complete"] = False
    assert verifier_for_task(p, 150, [proof, event]) == {}


def test_v3_new_health_task_has_all_assessment_fields():
    from app.health_loop_sync import GhMetadataClient
    client = GhMetadataClient(); client.transport = type("Transport", (), {"policy": policy()})()
    fields = client._task_fields({"source_id": "src", "risk": "Medium"})
    assert {"Status", "Priority", "Task Type", "Change Risk", "Area", "Impact", "Urgency", "Reach", "Recurrence",
            "Effort", "Priority Score", "Target", "Remedy", "Route"} <= fields.keys()
