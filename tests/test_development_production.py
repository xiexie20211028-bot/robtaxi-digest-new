"""生产适配器使用固定Actions证据测试，不创建或触发线上运行。"""
from __future__ import annotations

import copy
from datetime import datetime, timezone

import pytest

from app.development_cycle import load_policy
from app.development_policy import digest, select_tasks
from app.development_production import (LEGACY_FIELDS, collect_evidence, evaluate_report, reconcile_production)


def contract():
    return {"schema_version": "robtaxi-execution-v1", "issue": 69, "version": 1, "base_sha": "a" * 40,
            "goal": "移除兼容空字段", "acceptance": ["不再生成且旧输入可读"], "allowed_paths": ["app/report.py"],
            "relevant_paths": ["app/report.py"], "risk": "High", "risk_reason": "共享格式", "dependencies": [],
            "tests": ["pytest"], "production": {"kind": "task", "checks": ["正常生产"]},
            "rollback_conditions": ["报告解析失败"], "reserved_decisions": []}


def report():
    return {"html_output": "site/index.html", "stage_status": {"render": "success", "notify": "success"},
            "feishu_push_status": {"final_status": "success"}, "wecom_push_status": {"final_status": "already_sent"}}


def jobs():
    return [{"id": n, "name": name, "status": "completed", "conclusion": "success"}
            for n, name in enumerate(("build", "deploy", "notify"), 1)]


def test_report_acceptance_ignores_unrelated_health_failure():
    assert evaluate_report("report_compat_v1", contract(), report(), jobs() + [
        {"id": 9, "name": "self_check", "conclusion": "failure", "status": "completed"}]) is True
    for field in LEGACY_FIELDS:
        assert evaluate_report("report_compat_v1", contract(), {**report(), field: None}, jobs()) is False
    assert evaluate_report("report_compat_v1", contract(), report(), []) is None
    broken = report()
    broken["wecom_push_status"]["final_status"] = "failed"
    assert evaluate_report("report_compat_v1", contract(), broken, jobs()) is False
    assert evaluate_report("unknown", contract(), report(), jobs()) is None


def test_source_must_participate_and_have_health_report():
    c = contract()
    c["production"] = {"kind": "source", "source_id": "src", "checks": ["两次"]}
    health = {"findings": []}
    assert evaluate_report("source_health_v1", c, report(), jobs(), health) is None
    r = {**report(), "source_stats": [{"source_id": "src", "status": "healthy"}]}
    assert evaluate_report("source_health_v1", c, r, jobs(), None) is None
    assert evaluate_report("source_health_v1", c, r, jobs(), health) is True


def test_collector_filters_non_adopted_and_manual_runs(monkeypatch):
    policy = load_policy()
    base = {"id": 1, "run_attempt": 1, "workflow_id": 22, "event": "schedule", "head_branch": "main",
            "created_at": "2026-09-24T03:00:00Z", "status": "completed", "head_sha": "c" * 40}
    runs = [base, {**base, "id": 2, "event": "workflow_dispatch"},
            {**base, "id": 3, "head_sha": "d" * 40}, {**base, "id": 4, "workflow_id": 33}]
    class Fake:
        def api(self, path):
            if "/compare/" in path:
                return {"status": "identical" if path.endswith("c" * 40) else "behind"}
            return {"id": 22, "path": ".github/workflows/robtaxi-digest-pages.yml"}
    def page(client, path, field):
        return {"workflow_runs": runs, "jobs": jobs(), "artifacts": []}[field]
    monkeypatch.setattr("app.development_production.pages", page)
    monkeypatch.setattr("app.development_production.artifact_json", lambda *args: (report(), "report-sha"))
    pr = {"merged_at": "2026-09-23T03:00:00Z", "merge_commit_sha": "c" * 40}
    evidence = collect_evidence(Fake(), policy, pr, contract(), "report_compat_v1")
    assert [e["id"] for e in evidence] == [1]
    assert evidence[0]["acceptance_passed"] is True
    monkeypatch.setattr("app.development_production.artifact_json", lambda *args: (None, None))
    evidence = collect_evidence(Fake(), policy, pr, contract(), "report_compat_v1")
    assert not evidence[0]["artifacts_complete"]


class ProductionClient:
    def __init__(self):
        c = contract()
        self.row = {"number": 69, "type": "优化", "status": "观察中", "state": "OPEN", "labels": []}
        self.history = [{"event": "contract", "producer": "codex-scheduled", "contract": c},
                        {"event": "delivery", "pr": 116, "merge_sha": "b" * 40, "head_sha": "c" * 40,
                         "contract_digest": digest(c)}]
        self.writes = []
        self.budget = type("Budget", (), {"check": lambda self: None})()
    def tasks(self):
        return [self.row]
    def task(self, n):
        return self.row
    def events(self, n):
        return self.history
    def api(self, path):
        return {"merged": True, "merge_commit_sha": "b" * 40, "merged_at": "2026-09-23T03:00:00Z",
                "head": {"sha": "c" * 40, "repo": {"full_name": load_policy()["repository"]}}, "base": {"ref": "main"}}
    def append(self, event, issue=None):
        self.history.append(event)
        self.writes.append(event["event"])
    def gh(self, *args):
        self.writes.append(args[:2])
        self.row["state"] = "CLOSED" if args[1] == "close" else "OPEN"
    def label(self, n, label):
        self.writes.append(label)


def evidence(n=1, **changes):
    return {"id": n, "created_at": f"2026-09-{23+n}T03:00:00Z", "event": "schedule", "contains_merge": True,
            "artifacts_complete": True, "acceptance_passed": True, "sources_executed": [], **changes}


def test_close_after_verified_and_resume_partial_status(monkeypatch):
    client = ProductionClient()
    monkeypatch.setattr("app.development_cycle.assert_trusted_checkout", lambda c: "b" * 40)
    monkeypatch.setattr("app.development_production.collect_evidence", lambda *args: [evidence()])
    calls = []
    def status(policy, number, value):
        calls.append(value)
        if len(calls) == 1:
            raise OSError("状态写入中断")
        client.row["status"] = value
    monkeypatch.setattr("app.development_cycle.set_status", status)
    with pytest.raises(OSError):
        reconcile_production(client, load_policy(), apply=True)
    assert client.writes == ["production_verified", ("issue", "close")]
    reconcile_production(client, load_policy(), apply=True)
    assert client.writes == ["production_verified", ("issue", "close")]
    assert client.row["status"] == "已完成"


@pytest.mark.parametrize("rows", [[], [evidence(artifacts_complete=False)], [evidence(contains_merge=False)]])
def test_incomplete_evidence_never_closes(monkeypatch, rows):
    client = ProductionClient()
    monkeypatch.setattr("app.development_cycle.assert_trusted_checkout", lambda c: "b" * 40)
    monkeypatch.setattr("app.development_production.collect_evidence", lambda *args: rows)
    result = reconcile_production(client, load_policy(), apply=True)
    assert result["results"][0]["status"] == "observing"
    assert client.writes == []


def test_unknown_verifier_and_dry_run_never_write(monkeypatch):
    client = ProductionClient()
    policy = load_policy()
    policy["production_verifiers"] = {}
    assert reconcile_production(client, policy)["results"][0]["status"] == "no_trusted_verifier"
    monkeypatch.setattr("app.development_production.collect_evidence", lambda *args: [evidence()])
    assert reconcile_production(client, load_policy())["results"][0]["status"] == "complete"
    assert client.writes == []


def test_failure_requeues_not_closes(monkeypatch):
    client = ProductionClient()
    monkeypatch.setattr("app.development_cycle.assert_trusted_checkout", lambda c: "b" * 40)
    monkeypatch.setattr("app.development_cycle.set_status", lambda p, n, s: client.row.update(status=s))
    monkeypatch.setattr("app.development_production.collect_evidence", lambda *args: [evidence(acceptance_passed=False)])
    result = reconcile_production(client, load_policy(), apply=True)
    assert result["results"][0]["status"] == "requeue"
    assert client.writes == ["production_requeue", "planning"]
    assert client.row["status"] == "待办" and client.row["state"] == "OPEN"


def test_no_observation_resumes_close_without_artifacts(monkeypatch):
    client = ProductionClient()
    client.history[0]["contract"]["production"] = {"kind": "none", "checks": ["文档验证"]}
    client.history[1]["contract_digest"] = digest(client.history[0]["contract"])
    monkeypatch.setattr("app.development_cycle.assert_trusted_checkout", lambda c: "b" * 40)
    monkeypatch.setattr("app.development_cycle.set_status", lambda p, n, s: client.row.update(status=s))
    monkeypatch.setattr("app.development_production.collect_evidence", lambda *args: pytest.fail("文档无需生产产物"))
    assert reconcile_production(client, load_policy(), apply=True)["results"][0]["status"] == "complete"
    assert client.row["state"] == "CLOSED" and client.row["status"] == "已完成"


def task(n, **changes):
    return {"number": n, "state": "OPEN", "type": "Bug", "priority": "P2", "target": "本周", "status": "待办",
            "contract": {"valid": True}, "labels": [], "blockers": [], "assignees": 1, **changes}


def test_p0_cross_stage_and_blocker_exclusion():
    policy = {**load_policy(), "mode": "active"}
    tasks = [task(1, priority="P3", review_needed=True), task(2, priority="P0", contract=None),
             task(3, priority="P0", blockers=[9], open_pr={"number": 10}), task(4, priority="P0", acceptance_clear=False)]
    assert select_tasks(tasks, policy)["next_action"] == {"kind": "plan", "issue": 2}
    tasks[1]["labels"] = ["automation:paused"]
    assert select_tasks(tasks, policy)["next_action"] == {"kind": "review", "issue": 1}


def test_requested_changes_enters_execute():
    policy = load_policy()
    row = task(69, open_pr={"number": 116}, changes_requested=True)
    assert select_tasks([row], policy)["next_action"] == {"kind": "execute", "issue": 69}
    row.update(changes_requested=False, ci_failed=True)
    assert select_tasks([row], policy)["next_action"] == {"kind": "execute", "issue": 69}
