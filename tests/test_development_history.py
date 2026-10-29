"""历史交付从真实关联恢复，不倒填授权，不重复开发。"""
import copy
from datetime import datetime, timezone

import pytest

from app.development_cycle import load_policy, snapshot
from app.development_history import adoption, inspect_delivery, record
from app.development_policy import DevelopmentError, production_status


class HistoryClient:
    def __init__(self):
        self.policy = {**load_policy(), "mode": "active"}
        self.repo = self.policy["repository"]
        self.rows = []; self.files = ["app/source.py"]
        self.pr = {"number": 85, "merged": True, "base": {"ref": "main"},
                   "head": {"sha": "a" * 40, "repo": {"full_name": self.repo}},
                   "merge_commit_sha": "b" * 40, "merged_at": "2026-08-29T17:00:00Z",
                   "html_url": "https://github.com/example/repo/pull/85", "body": "Primary task: Fixes #46"}
    def api(self, path):
        if "/compare/" in path: return {"status": "ahead"}
        if "/check-runs" in path:
            return {"total_count": 2, "check_runs": [{"id": i, "name": name, "conclusion": "success"}
                    for i, name in enumerate(self.policy["required_checks"])]}
        if "/files?" in path: return [{"filename": p} for p in self.files]
        return self.pr
    def main_sha(self): return "c" * 40
    def events(self, issue=None): return list(self.rows)
    def append(self, event, issue=None): self.rows.append(event)
    def task(self, issue): return {"state": "OPEN"}


def request():
    return {"issue": 46, "pr": 85, "merge_sha": "b" * 40, "delivered_scope": ["立法发现查询"],
            "remaining_scope": ["正常生产原验收"], "acceptance_evidence": ["原PR的范围与测试记录"],
            "production": {"kind": "task", "checks": ["正常生产原验收"]}}


def test_history_records_today_without_inventing_old_contract_or_approval():
    client = HistoryClient(); event = record(client, request())
    assert event["at"] != client.pr["merged_at"]
    assert event["approval_status"] == event["contract_status"] == "not_reconstructed"
    assert "contract" not in event and event["facts"]["tests_verified"]
    record(client, request())
    assert len(client.rows) == 1


@pytest.mark.parametrize("change", ["foreign", "primary", "unmerged", "sha"])
def test_history_rejects_unverified_identity(change):
    client = HistoryClient(); payload = request()
    if change == "foreign": client.pr["head"]["repo"]["full_name"] = "other/repo"
    if change == "primary": client.pr["body"] = "Primary task: Fixes #73\nRefs #46"
    if change == "unmerged": client.pr["merged"] = False
    if change == "sha": payload["merge_sha"] = "d" * 40
    with pytest.raises(DevelopmentError): adoption(client, payload)


def test_business_delivery_cannot_claim_no_observation():
    client = HistoryClient(); payload = request(); payload["production"]["kind"] = "none"
    payload["remaining_scope"] = []
    with pytest.raises(DevelopmentError): adoption(client, payload)
    client.files = ["docs/manual.md", "tests/test_contract.py"]
    assert adoption(client, payload)["facts"]["production"]["kind"] == "none"


def test_old_merged_branch_is_not_selected_for_fresh_development():
    class Client(HistoryClient):
        def tasks(self):
            return [{"number": 46, "state": "OPEN", "type": "Bug", "priority": "P0", "status": "开发中",
                     "labels": [], "blockers": [], "assignees": 1, "body": "## 验收标准\n原验收"}]
        def pulls(self): return []
        def merged_pulls(self):
            return [{"number": 85, "body": "Primary task: Fixes #46", "headRefName": "codex/law-01",
                     "headRefOid": "a" * 40, "mergeCommit": {"oid": "b" * 40}, "mergedAt": self.pr["merged_at"]}]
    client = Client(); result = snapshot(client, client.policy)
    assert result["next_action"] is None
    assert result["tasks"][0]["delivery_adoption_required"]
    assert result["tasks"][0]["awaiting_production"]["number"] == 85


@pytest.mark.parametrize("issue", [49, 52, 57, 58])
def test_original_three_scheduled_recoveries_are_preserved(issue):
    contract = {"issue": issue, "production": {"kind": "source", "source_id": "src"}}
    rows = [{"id": i, "created_at": f"2026-10-{20+i}T01:00:00Z", "event": "schedule",
             "contains_merge": True, "artifacts_complete": True, "sources_executed": ["src"],
             "acceptance_passed": True} for i in range(1, 4)]
    assert production_status(contract, "b" * 40, rows[:2]) == "observing"
    assert production_status(contract, "b" * 40, rows) == "complete"


def test_original_acceptance_text_can_raise_but_never_lower_recovery_standard():
    from app.health_history import required_recovery_runs
    assert required_recovery_runs(57, "## 验收标准\n连续 2 次健康") == 3
    assert required_recovery_runs(150, "## 验收标准\n连续 5 次健康\n## 其他\n连续 99 次") == 5
