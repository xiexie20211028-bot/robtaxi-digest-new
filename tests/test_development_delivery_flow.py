"""跨阶段交付故障注入：不调用GitHub、不修改真实PR。"""
import copy
import json
from datetime import datetime, timedelta, timezone

import pytest

from app.development_cycle import load_policy
from app.development_policy import DevelopmentError
from app.development_recovery import RequestFailure
from scripts.development_delivery import merge, wait_for_ci


class Server:
    def __init__(self, lose_response=False):
        self.policy = load_policy()
        now = datetime.now(timezone.utc)
        self.pr = {"number": 116, "state": "open", "draft": True, "merged": False, "head": {
            "sha": "b" * 40, "ref": "codex/development-69", "repo": {"full_name": self.policy["repository"]}},
            "base": {"ref": "main"}, "body": "Primary task: Refs #69"}
        self.control = [{"event": "task_claimed", "issue": 69, "key": "task:today:69", "action": "review",
                         "lease_until": (now + timedelta(minutes=20)).isoformat()}]
        self.history = [{"event": "checkpoint", "stage": "tests_passed", "head_sha": "b" * 40}]
        self.commands = []
        self.lose_response = lose_response
    def api(self, path):
        if "/pulls/" in path:
            return copy.deepcopy(self.pr)
        if "/check-runs" in path:
            rows = [{"id": n, "name": name, "head_sha": "b" * 40, "app": {"slug": "github-actions"},
                     "started_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                     "status": "completed", "conclusion": "success"}
                    for n, name in enumerate(self.policy["required_checks"], 1)]
            rows.append({**rows[0], "id": 0, "started_at": "2020-01-01T00:00:00Z", "conclusion": "failure"})
            return {"total_count": len(rows), "check_runs": rows}
        raise AssertionError(path)
    def main_sha(self):
        return "a" * 40
    def events(self, issue=None):
        return self.control if issue is None else self.history
    def append(self, event, issue=None):
        (self.control if issue is None else self.history).append(event)
    def gh_read(self, *args):
        assert args[:2] == ("pr", "view")
        return json.dumps({"mergeStateStatus": "CLEAN", "headRefOid": "b" * 40})
    def gh(self, *args):
        self.commands.append(args[:2])
        if args[:2] == ("pr", "ready"):
            self.pr["draft"] = False
        elif args[:2] == ("pr", "merge"):
            self.pr.update(merged=True, merge_commit_sha="c" * 40,
                           merged_at=datetime.now(timezone.utc).isoformat())
            if self.lose_response:
                raise RequestFailure(["gh", "pr", "merge"], "EOF", 1, 5)
        else:
            raise AssertionError(args)


@pytest.mark.parametrize("lose_response", [False, True])
def test_review_to_ready_ci_merge_and_receipt(monkeypatch, lose_response):
    client = Server(lose_response)
    gate_calls = []
    def gate(*args):
        gate_calls.append(args)
        return {"issue": 69, "production": {"kind": "task"}, "contract_digest": "contract"}
    monkeypatch.setattr("scripts.development_delivery.assert_trusted_checkout", lambda c: "a" * 40)
    monkeypatch.setattr("scripts.development_delivery.run", lambda *a, **kw: "")
    monkeypatch.setattr("scripts.development_delivery.github_gate", gate)
    statuses = []
    monkeypatch.setattr("scripts.development_delivery.set_status", lambda p, n, status: statuses.append(status))
    result = merge(client, client.policy, 116)
    assert result["event"] == "delivery"
    assert client.commands == [("pr", "ready"), ("pr", "merge")]
    assert len(gate_calls) == 2  # 等待后必须重新核对授权。
    assert statuses == ["观察中"]
    assert sum(e.get("event") == "delivery" for e in client.history) == 1
    assert sum(e.get("event") == "reserve" for e in client.control) == 1


def test_gate_denial_does_not_ready(monkeypatch):
    client = Server()
    monkeypatch.setattr("scripts.development_delivery.assert_trusted_checkout", lambda c: "a" * 40)
    monkeypatch.setattr("scripts.development_delivery.run", lambda *a, **kw: "")
    def deny(*args):
        raise DevelopmentError("缺少独立复核")
    monkeypatch.setattr("scripts.development_delivery.github_gate", deny)
    with pytest.raises(DevelopmentError, match="独立复核"):
        merge(client, client.policy, 116)
    assert client.commands == []


def test_pending_ci_checkpoints_and_does_not_merge(monkeypatch):
    client = Server()
    original = client.api
    def api(path):
        if "/check-runs" in path:
            return {"total_count": 0, "check_runs": []}
        return original(path)
    client.api = api
    policy = {**client.policy, "ci_wait_seconds": 1}
    assert not wait_for_ci(client, policy, client.pr, 69, client.control[0])
    assert client.commands == []
    assert client.history[-1]["stage"] == "waiting_ci"


def test_merge_unknown_never_retried(monkeypatch):
    client = Server()
    client.pr["draft"] = False
    client.control.append({"event": "merge_attempt", "pr": 116, "head_sha": "b" * 40})
    monkeypatch.setattr("scripts.development_delivery.assert_trusted_checkout", lambda c: "a" * 40)
    monkeypatch.setattr("scripts.development_delivery.run", lambda *a, **kw: "")
    monkeypatch.setattr("scripts.development_delivery.github_gate", lambda *a: {"issue": 69, "production": {"kind": "task"}, "contract_digest": "contract"})
    with pytest.raises(DevelopmentError, match="禁止重复合并"):
        merge(client, client.policy, 116)
    assert client.commands == []


def test_failed_ci_records_repair_without_merge():
    client = Server()
    original = client.api
    def api(path):
        data = original(path)
        if "/check-runs" in path:
            for row in data["check_runs"]:
                row["conclusion"] = "failure"
        return data
    client.api = api
    with pytest.raises(DevelopmentError, match="最新运行失败"):
        wait_for_ci(client, client.policy, client.pr, 69, client.control[0])
    assert client.history[-1]["reason"] == "ci_failed"
    assert client.history[-1]["changed"] is False
    assert client.commands == []
