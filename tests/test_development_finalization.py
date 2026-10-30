"""失败收尾必须留证据、报送或明确离线，并保持释放顺序。"""
from datetime import datetime, timezone
from copy import deepcopy
import json
from pathlib import Path
from contextlib import contextmanager, nullcontext
from zoneinfo import ZoneInfo

import pytest

from app import development_cycle as cycle
from app.development_outcomes import finalize_run, build_outcome, render_outcome
from app.development_policy import DevelopmentError, digest


class Client:
    def __init__(self):
        self.run_id = "finalize-test-run"; self.rows = []; self.calls = []
        self.policy = {**cycle.load_policy(), "mode": "active"}
        self.run_started = {"run_id": self.run_id, "at": datetime.now(timezone.utc).isoformat(), "trigger": "schedule"}
        self.budget = type("Budget", (), {"check": lambda s: None, "remaining": lambda s: 1800})()
    def events(self): return deepcopy(self.rows)
    def append(self, row): self.rows.append(row)
    def gh(self, *args): self.calls.append(args)


def setup(tmp_path, monkeypatch, c):
    monkeypatch.setattr(cycle, "ROOT", tmp_path)
    monkeypatch.setattr(cycle, "assert_trusted_checkout", lambda client: "a" * 40)
    monkeypatch.setattr(cycle, "snapshot", lambda *a: {"tasks": [], "next_action": None})
    def finish(client, outcome):
        event = {"event": "run_finished", "run_id": client.run_id, "outcome": outcome}
        client.append(event);return event
    monkeypatch.setattr(cycle, "finish_run", finish)
    workflows = tmp_path / ".github/workflows"; workflows.mkdir(parents=True)
    (workflows / "robtaxi-digest-pages.yml").write_text("schedule:\n - cron: '0 1 * * *'\n")
    return tmp_path / ".local/robtaxi-development/runs" / c.run_id


def test_failed_health_sync_generates_formal_daily_before_finish(tmp_path, monkeypatch):
    c = Client(); root = setup(tmp_path, monkeypatch, c)
    result = finalize_run(c, "failed", error="旧事故记录失配")
    assert result["finished"] and result["reporting_complete"]
    assert c.rows[-1]["event"] == "run_finished" and c.rows[-1]["outcome"] == "failed"
    report = next(r["result"] for r in c.rows if r["event"] == "run_outcome")
    assert report["inspection"] == "incomplete" and report["requires_attention"]
    assert any(r["event"] == "notification_requested" for r in c.rows)
    assert all("notification_only=true" in args for args in c.calls)
    assert (root / "completion.json").exists() and (root / "daily.md").exists()


def test_outbox_failure_is_visible_and_does_not_prevent_finish(tmp_path, monkeypatch):
    c = Client(); root = setup(tmp_path, monkeypatch, c); append = c.append
    def failing(row):
        if row["event"] == "notification_requested": raise DevelopmentError("写入失败")
        append(row)
    c.append = failing
    result = finalize_run(c, "failed")
    assert result["finished"] and not result["reporting_complete"] and result["reporting_error"]
    assert c.rows[-1]["event"] == "run_finished" and c.calls == []
    assert json.loads((root / "completion.json").read_text())["reporting_error"]


def test_expired_lease_saves_local_context_without_formal_writes(tmp_path, monkeypatch):
    c = Client(); root = setup(tmp_path, monkeypatch, c)
    def expired(): raise DevelopmentError("租约到期")
    c.budget.check = expired
    result = finalize_run(c, "failed")
    assert result["local_only"] and not result["finished"] and c.rows == []
    assert json.loads((root / "completion.json").read_text())["finalization_error"]


def test_dispatch_unknown_keeps_outbox_and_does_not_retry(tmp_path, monkeypatch):
    c = Client(); setup(tmp_path, monkeypatch, c)
    def dispatch(*a):
        c.calls.append(a);raise DevelopmentError("响应丢失")
    c.gh = dispatch
    result = finalize_run(c, "failed")
    assert result["finished"] and result["reporting_complete"] and result["reporting_error"]
    assert len(c.calls) == 1
    assert any(r["event"] == "notification_requested" for r in c.rows)


def test_manual_finish_does_not_consume_daily_slot(tmp_path, monkeypatch):
    c = Client(); setup(tmp_path, monkeypatch, c)
    result = finalize_run(c, "no_eligible_task", trigger="manual")
    assert result["finished"] and not c.calls
    assert not any(r["event"] == "notification_requested" for r in c.rows)


def test_partial_inspection_and_block_reason_are_visible():
    now = datetime.now(timezone.utc); c = Client()
    decision = {"run_id": "100", "date_bj": now.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat(),
                "run_evidence": {"event_name": "schedule", "health_report_available": True}, "incidents": {}, "actions": []}
    blocked = {"incident_key": "source:miit:date", "source_id": "miit", "status": "blocked", "reason": "旧记录待核对"}
    receipt = {"complete": False, "mode": "apply", "development_run_id": c.run_id,
               "global_state_valid": True, "decision_digest": digest(decision),
               "item_results": [{"status": "synced"}, blocked], "blocked_incidents": [blocked]}
    result = build_outcome(policy=c.policy, state={"tasks": [], "next_action": None}, decision=decision,
        receipt=receipt, events=[], run_started=c.run_started, now=now, workflow="cron: '0 1 * * *'", trigger="schedule")
    assert result["inspection"] == "partial" and result["requires_attention"]
    assert "部分完成" in render_outcome(result) and "旧记录待核对" in render_outcome(result)


def test_health_sync_error_automatically_enters_failed_finalization(tmp_path, monkeypatch):
    from app import health_loop_sync as sync, development_outcomes as outcomes, development_runtime as runtime
    import sys
    c = Client(); called = []
    @contextmanager
    def scope(*args): yield c
    def failing(*args): raise sync.SyncError("正式同步失败")
    monkeypatch.setattr(sync, "sync_run_scope", scope)
    monkeypatch.setattr(sync, "sync_main", failing)
    monkeypatch.setattr(runtime, "repository_lock", lambda *a: nullcontext())
    monkeypatch.setattr(outcomes, "finalize_run", lambda *a, **kw: called.append((a, kw)))
    monkeypatch.setattr(sys, "argv", ["health-sync", "--decision", str(tmp_path / "decision.json"),
        "--mode", "apply", "--out", str(tmp_path / "sync.json"), "--run-id", c.run_id])
    assert sync.main() == 1
    assert len(called) == 1 and called[0][0] == (c, "failed")
    assert called[0][1]["error"] == "正式同步失败"
