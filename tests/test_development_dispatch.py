"""用 11 月 17 日的生产与队列身份回放派工；正式读写替换为内存记录。"""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app import development_cycle as cycle, development_dispatch as worker, development_outcomes
from app.development_policy import DevelopmentError, digest

ARCHIVE = json.loads((Path(__file__).parent / "fixtures/dispatch_nov17.json").read_text())


class Client:
    run_id = ARCHIVE["development_run_id"]

    def __init__(self):
        self.history = []
        self.by_issue = {}
        self.seconds = 3600
        deadline = datetime.now(timezone.utc) + timedelta(hours=1)
        self.budget = SimpleNamespace(remaining=lambda: self.seconds, check=lambda: None, deadline=deadline)
        self.run_started = {"event": "run_started", "key": "run:" + self.run_id, "run_id": self.run_id,
                            "at": datetime.now(timezone.utc).isoformat(), "deadline": deadline.isoformat()}
        self.history.append(self.run_started)

    def append(self, e, issue=None):
        (self.history if issue is None else self.by_issue.setdefault(issue, [])).append(e)


@pytest.fixture
def replay(monkeypatch, tmp_path):
    client = Client()
    policy = cycle.load_policy()
    task = {"number": ARCHIVE["next_action"]["issue"], "status": "待办", "assignees": 1, "blockers": []}
    decision = {"run_id": ARCHIVE["production_run_id"]}
    proof = {"event": "health_queue_updated", "run_id": client.run_id, "production_run_id": decision["run_id"],
             "attempt": 1, "key": "queue-test", "decision_digest": digest(decision), "global_state_valid": True}
    client.history.append(proof)
    trace = []
    state = {"events": client.history, "tasks": [task], "next_action": ARCHIVE["next_action"],
             "main_sha": ARCHIVE["main_sha"], "conflicting_running": []}
    def sync(c, p, target, *, apply, phase):
        assert apply
        trace.append(phase)
        if phase == "maintenance":
            return {"stopped": "review_index_in_progress", "reports": []}
        target.mkdir(exist_ok=True)
        receipt = {"development_run_id": c.run_id, "decision_digest": digest(decision), "global_state_valid": True}
        (target / "decision.json").write_text(json.dumps(decision))
        (target / "receipt.json").write_text(json.dumps(receipt))
        return {"reports": [{"production_run_id": decision["run_id"], "attempt": 1,
            "decision": str(target / "decision.json"), "receipt": str(target / "receipt.json")}]}
    monkeypatch.setattr(worker, "sync_health", sync)
    monkeypatch.setattr(cycle, "set_status", lambda *args: None)
    monkeypatch.setattr(cycle, "assert_trusted_checkout", lambda *args: ARCHIVE["main_sha"])
    def snapshot(*args):
        task["events"] = client.by_issue.get(task["number"], [])
        trace.append("queue")
        return state
    monkeypatch.setattr(cycle, "snapshot", snapshot)
    monkeypatch.setattr(development_outcomes, "finalize_run", lambda c, outcome, **kw: {"outcome": outcome, **kw})
    return SimpleNamespace(client=client, trace=trace, state=state, proof=proof,
                           run=lambda: worker.dispatch(client, policy, tmp_path))


def test_current_source_precedes_historical_maintenance_and_claim_survives_restart(replay):
    assert ARCHIVE["reports_listed"] == 282 and len(ARCHIVE["invalid_review_productions"]) == 21
    assert ARCHIVE["remaining_at_outcome"] < 120
    first = replay.run()
    assert first["next_action"] == ARCHIVE["next_action"] and first["stage"] == "work"
    second = replay.run()
    assert second["handoff"]["resumed"] and second["handoff"]["claim"]["key"] == first["handoff"]["claim"]["key"]
    assert [e["event"] for e in replay.client.history].count("task_claimed") == 1
    assert replay.trace == ["current", "queue", "current", "queue"]


def test_checkout_requires_actual_run_specific_current_queue(replay):
    replay.proof["run_id"] = "another-run"
    with pytest.raises(DevelopmentError, match="正式队列回执"):
        replay.run()
    assert not any(e["event"] == "task_claimed" for e in replay.client.history)


def test_no_work_receives_maintenance_slice_and_finishes(replay):
    replay.state["next_action"] = None
    first = replay.run()
    assert first["stage"] == "finished" and first["completion"]["outcome"] == "no_eligible_task"
    replay.run()
    assert replay.trace.count("maintenance") == 1
    assert sum(e["event"] == "dispatch_maintenance_checked" for e in replay.client.history) == 1


def test_insufficient_budget_cannot_start_new_task(replay):
    replay.client.seconds = 899
    result = replay.run()
    assert result["stage"] == "finished" and "预算不足" in result["reason"]
    assert not any(e["event"] == "task_claimed" for e in replay.client.history)


def test_bound_pending_acceptance_checked_before_work(replay, monkeypatch):
    replay.state["tasks"].append({"number": 73, "status": "观察中"})
    def verify(client, policy, issue, *, apply):
        replay.trace.append("verify")
        assert issue == 73 and apply
        return {"results": [{"issue": 73, "status": "waiting"}]}
    monkeypatch.setattr(worker, "reconcile_production", verify)
    assert replay.run()["stage"] == "work"
    assert replay.run()["handoff"]["resumed"]
    assert replay.trace.count("verify") == 1


def test_owned_claim_precedes_new_pending_verifier(replay, monkeypatch):
    first = replay.run()
    replay.state["tasks"].append({"number": 83, "status": "观察中"})
    def unexpected(*args, **kwargs):
        pytest.fail("有效认领恢复时不能启动其他任务的历史验收")
    monkeypatch.setattr(worker, "reconcile_production", unexpected)
    result = replay.run()
    assert result["handoff"]["resumed"] and result["handoff"]["claim"]["key"] == first["handoff"]["claim"]["key"]


def test_background_read_phase_deadline_yields_without_ending_run(replay, monkeypatch, tmp_path):
    started = datetime.now(timezone.utc)
    clock = [started]
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0]
    class SlowClient(Client):
        def gh_read(self, *args):
            assert self.budget.deadline == started + timedelta(seconds=300)
            clock[0] += timedelta(seconds=295)
            raise DevelopmentError("剩余运行预算不足以重试")
    client = SlowClient()
    monkeypatch.setattr(worker, "datetime", Clock)
    def sync(bounded, policy, directory, *, apply, phase):
        bounded.gh_read("run", "list")
    monkeypatch.setattr(worker, "sync_health", sync)
    result = worker.maintenance_slice(client, {}, tmp_path)
    assert result["stopped"] == "maintenance_yield"
    assert client.budget.deadline > clock[0] + timedelta(minutes=15)


def test_missing_current_report_finishes_with_specific_cause(replay, monkeypatch):
    monkeypatch.setattr(worker, "sync_health", lambda *a, **kw: {"reports": [
        {"production_run_id": "7", "attempt": 1, "source_status": "missing_artifacts"}]})
    result = replay.run()
    assert result["stage"] == "finished" and result["completion"]["outcome"] == "failed"
    assert "missing_artifacts" in result["completion"]["stop_reason"]
    assert not any(e["event"] == "task_claimed" for e in replay.client.history)
