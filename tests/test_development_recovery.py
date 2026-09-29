"""真实入口的故障注入；不使用网络或生产写入。"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from app.development_cycle import (ROOT, active_claim, checkpoint_review, claim, initialize_run,
                                   load_policy, snapshot)
from app.development_policy import DevelopmentError, digest
from app.development_recovery import RecoveryBudget, RequestFailure, diagnostic, latest_checks
from app.development_runtime import GitHub


def error(text="Post https://api.github.com/graphql: EOF"):
    return RequestFailure(["gh", "api", "graphql", "--input", "-"], text, 1, 5)


@pytest.mark.parametrize("text,category", [("HTTP 401 Bad credentials", "authentication"),
    ("HTTP 403 permission denied", "permission"), ("HTTP 403 API rate limit exceeded", "rate_limit"),
    ("HTTP 503", "service"), ("EOF", "connection"), ("timeout", "timeout"), ("HTTP 422", "request_rejected")])
def test_error_category(text, category):
    assert error(text).category == category


def test_diagnostics_no_private_payload_and_retention(tmp_path):
    secret = "Authorization: Bearer abc123 https://user:password@host/a?sig=SECRET comment=PRIVATE EOF"
    old = tmp_path / "requests-2020-01-01.jsonl"
    old.write_text("old")
    unrelated = tmp_path / "keep-me.json"
    unrelated.write_text("keep")
    diagnostic(tmp_path, error(secret), 3, "checkpoint")
    contents = next(tmp_path.glob("requests-*.jsonl")).read_text()
    assert all(s not in contents for s in ("abc123", "password", "SECRET", "PRIVATE", "user:"))
    assert not old.exists() and unrelated.exists()
    assert json.loads(contents)["attempt"] == 3


def budget(tmp_path, **changes):
    policy = {**load_policy(), **changes}
    return RecoveryBudget(tmp_path, "test-run-123", (datetime.now(timezone.utc) + timedelta(minutes=20)).isoformat(), policy)


def test_long_recovery_bounded_and_persistent(tmp_path, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr("app.development_recovery.time.monotonic", lambda: clock[0])
    monkeypatch.setattr("app.development_recovery.time.sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    ctx = budget(tmp_path)
    for index in range(3):
        assert ctx.wait(error())
        ctx = budget(tmp_path)
        assert ctx.state["rounds"] == index + 1
    assert not ctx.wait(error())
    assert ctx.state["spent"] == 300
    assert not ctx.wait(error("HTTP 401"))


def test_rate_limit_obeys_server_and_deadline(tmp_path):
    ctx = budget(tmp_path)
    assert not ctx.wait(error("HTTP 429 Retry-After: 900"))
    assert not ctx.wait(error("HTTP 429"))
    ctx.deadline = datetime.now(timezone.utc) - timedelta(seconds=1)
    with pytest.raises(DevelopmentError, match="预算"):
        ctx.check()


def test_read_recovery_then_write_response_lost_does_not_repeat(tmp_path, monkeypatch):
    client = GitHub(load_policy())
    client.budget = budget(tmp_path)
    monkeypatch.setattr(client.budget, "wait", lambda failure: True)
    calls = []
    def gh(*args, stdin=None):
        calls.append(args)
        if len(calls) <= 3:
            raise error()
        return '{"sha":"ok"}'
    monkeypatch.setattr(client, "gh", gh)
    monkeypatch.setattr("app.development_runtime.time.sleep", lambda _: None)
    assert client.main_sha() == "ok"
    assert len(calls) == 4
    event = {"event": "review", "key": "review:bound", "verdict": "approve"}
    writes = []
    def lost(endpoint, **kwargs):
        writes.append(event)
        raise error()
    monkeypatch.setattr(client, "api", lost)
    monkeypatch.setattr(client, "events", lambda issue: writes)
    assert client.append(event, 69) == event
    assert len(writes) == 1


def test_unknown_write_absence_does_not_resend(monkeypatch):
    client = GitHub(load_policy())
    calls = []
    def lost(*args, **kwargs):
        calls.append(args)
        raise error()
    monkeypatch.setattr(client, "api", lost)
    monkeypatch.setattr(client, "events", lambda issue: [])
    with pytest.raises(RequestFailure):
        client.append({"event": "review", "key": "one"}, 69)
    assert len(calls) == 1


def check(number, status="completed", conclusion="success", **extra):
    return {"id": number, "name": "validate", "app": {"slug": "github-actions"},
            "started_at": f"2026-09-23T03:00:{number:02}Z", "status": status, "conclusion": conclusion, **extra}


def test_latest_ci_not_all_historical_runs():
    assert latest_checks([check(1, conclusion="failure"), check(2)], ["validate"]) == "success"
    assert latest_checks([check(1), check(2, "in_progress", None)], ["validate"]) == "pending"
    assert latest_checks([check(1), check(2, conclusion="failure")], ["validate"]) == "failed"
    assert latest_checks([check(1, app={"slug": "untrusted"})], ["validate"]) == "pending"


def test_target_snapshot_does_not_read_whole_project(monkeypatch):
    client = GitHub(load_policy())
    monkeypatch.setattr(client, "tasks", lambda: pytest.fail("不应读取整盘"))
    monkeypatch.setattr(client, "pulls", lambda: pytest.fail("不应读取全部PR"))
    monkeypatch.setattr(client, "merged_pulls", lambda: pytest.fail("不应读取全部合并历史"))
    monkeypatch.setattr(client, "task", lambda n: {"number": n, "state": "OPEN", "type": "Bug", "status": "待办", "labels": [], "blockers": []})
    monkeypatch.setattr(client, "issue_pulls", lambda n: [])
    monkeypatch.setattr(client, "events", lambda n=None: [])
    monkeypatch.setattr(client, "main_sha", lambda: "a" * 40)
    assert snapshot(client, load_policy(), 69)["next_action"] == {"kind": "plan", "issue": 69}


def test_plan_claim_and_resume_preserves_deadline(monkeypatch):
    now = datetime.now(timezone.utc)
    policy = load_policy()
    row = {"number": 69, "assignees": 1, "blockers": [], "events": []}
    state = {"main_sha": "a" * 40, "events": [], "tasks": [row], "conflicting_running": [], "next_action": {"kind": "plan", "issue": 69}}
    class Fake:
        run_id = "run-123456"
        run_started = {"deadline": (now + timedelta(minutes=20)).isoformat()}
        def append(self, e, issue=None):
            (state["events"] if issue is None else row["events"]).append(e)
    client = Fake()
    monkeypatch.setattr("app.development_cycle.assert_trusted_checkout", lambda c: "a" * 40)
    monkeypatch.setattr("app.development_cycle.set_status", lambda *args: None)
    first = claim(client, policy, state)
    second = claim(client, policy, state)
    assert first["claim"] == second["claim"] and second["resumed"]
    assert first["claim"]["lease_until"] == client.run_started["deadline"]
    assert len(state["events"]) == 1
    client.run_id = "other-run"
    with pytest.raises(DevelopmentError, match="另一运行"):
        claim(client, policy, state)


def test_review_cache_rejects_changed_version():
    task = {"number": 69, "review_needed": True, "open_pr": {"headRefOid": "b" * 40}, "contract_digest": "digest"}
    with pytest.raises(DevelopmentError, match="证据已过期"):
        checkpoint_review(object(), {"main_sha": "a" * 40}, task, {}, {"verdict": "approve", "evidence": ["old"], "head_sha": "c" * 40})
