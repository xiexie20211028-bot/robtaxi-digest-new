"""失配只阻塞相关事故；传输与公共状态失败绝不能被降级。"""
from copy import deepcopy
from argparse import Namespace
import json
from pathlib import Path

import pytest

from app import health_loop_sync as sync
from app.development_cycle import load_policy
from app.development_budget import verifier_for_task, require_health_independent, queue_entry_verified
from app.development_policy import DevelopmentError
from app.health_loop import evaluate_health_loop
from app.health_history import parse_observations

FIXTURE = json.loads((Path(__file__).parent / "fixtures/health_loop_legacy_oct29.json").read_text())


class Client(sync.GhMetadataClient):
    def __init__(self, missing=True):
        super().__init__(); self.comments = {}; self.applied = []
        self._issues = [deepcopy(i) for i in FIXTURE["issues"] if not missing or i["number"] not in (31, 36)]
        self.official = {"schema_version": sync.STATE_SCHEMA, "complete": True,
                         "incidents": deepcopy(FIXTURE["decision"]["incidents"])}
    def preflight(self, **kw): return deepcopy(self.official)
    def validate_write_access(self): pass
    def _comments(self, n): return deepcopy(self.comments.get(n, []))
    def _run(self, args):
        if self.run_guard: self.run_guard()
        assert args[:2] == ["issue", "comment"]
        self.comments.setdefault(int(args[2]), []).append({"user": {"login": self.owner}, "body": args[args.index("--body") + 1]})
        return sync.CommandResult("")
    def apply(self, ops):
        self.applied.extend(ops)
        for i, op in enumerate(ops, 501):
            self.official["incidents"].setdefault(op["incident_key"], {})["engineering_issue"] = op.get("issue_number") or i
        return [{"status": "applied"} for _ in ops]


class Control:
    def __init__(self):
        self.rows = []; self.policy = {**load_policy(), "schema_version": "robtaxi-autonomy-policy-v3", "automatic_source_verifiers": True}
        self.budget = type("Budget", (), {"check": lambda s: None})()
    def gh_read(self, *a): raise AssertionError("不得访问网络")
    def events(self): return deepcopy(self.rows)
    def append(self, row): self.rows.append(row)


def test_one_missing_legacy_record_does_not_discard_six_independent_observations():
    c = Client(); c.prepare_observations(FIXTURE["decision"], apply=False)
    assert c.comments == {}
    c.prepare_observations(FIXTURE["decision"], apply=True)
    rows = [r for cs in c.comments.values() for r in parse_observations(cs, c.owner)]
    assert len(rows) == 6
    assert {i["source_id"] for i in c.item_results if i["status"] == "blocked"} == {"miit_news_structured"}
    assert sum(r["severity"] == "critical" and bool(r["source_id"]) for r in rows) == 4
    before = deepcopy(c.comments)
    c.prepare_observations(FIXTURE["decision"], apply=True)
    assert c.comments == before


def test_global_control_history_corruption_stops_before_any_write():
    c = Client(); read = c._comments
    c._comments = lambda n: ([{"user": {"login": c.owner}, "body": '<!-- robtaxi-health-observation:broken -->'}] if n == 100 else read(n))
    with pytest.raises(sync.SyncError, match="历史损坏"):
        c.prepare_observations(FIXTURE["decision"], apply=True)
    assert c.comments == {}


def test_permission_or_network_failure_is_never_an_item_level_failure():
    c = Client()
    def unavailable(n): raise sync.SyncError("认证/网络失败")
    c._comments = unavailable
    with pytest.raises(sync.SyncError, match="认证/网络"):
        c.prepare_observations(FIXTURE["decision"], apply=True)
    assert c.comments == {}


def execute(tmp_path, monkeypatch, c, decision=None):
    path = tmp_path / "decision.json"; path.write_text(json.dumps(decision or FIXTURE["decision"]))
    monkeypatch.setattr(sync, "GhMetadataClient", lambda: c)
    control = Control()
    args = Namespace(decision=str(path), mode="apply", out=str(tmp_path / "result.json"), state_out="", run_id="test-run")
    sync.sync_main(args, control)
    return json.loads((tmp_path / "result.json").read_text()), control


def test_partial_formal_receipt_unlocks_only_verified_independent_source(tmp_path, monkeypatch):
    result, control = execute(tmp_path, monkeypatch, Client())
    assert result["complete"] is False and result["global_state_valid"] is True
    assert len(result["blocked_incidents"]) == 1
    queue = next(r for r in control.rows if r["event"] == "health_queue_updated")
    lta = next(e for e in queue["entries"] if e["issue"] == 57)
    assert lta["actionable"] and queue_entry_verified(queue, lta)
    assert verifier_for_task(control.policy, 57, control.rows)["source_id"] == "singapore_lta_news_structured"
    assert verifier_for_task(control.policy, 49, control.rows) == {}
    require_health_independent(["source:singapore_lta_news_structured"], control.rows)
    require_health_independent([], control.rows)
    with pytest.raises(DevelopmentError): require_health_independent(["shared:production"], control.rows)
    with pytest.raises(DevelopmentError): require_health_independent(["source:miit_news_structured"], control.rows)
    assert not queue_entry_verified({**queue, "global_state_valid": False}, lta)


def test_historical_backfill_does_not_apply_old_task_actions_or_replace_queue(tmp_path, monkeypatch):
    c = Client(missing=False); c.official["latest_observed_run"] = int(FIXTURE["decision"]["run_id"]) + 1
    result, control = execute(tmp_path, monkeypatch, c)
    assert result["historical_only"] and not result["complete"]
    assert c.applied == [] and control.rows == []
    assert sum(len(v) for v in c.comments.values()) == 7


def test_invalid_whole_report_blocks_apply(tmp_path, monkeypatch):
    c = Client(); decision = deepcopy(FIXTURE["decision"])
    decision["run_evidence"]["health_report_valid"] = False
    with pytest.raises(sync.SyncError, match="整份健康报告不可信"):
        execute(tmp_path, monkeypatch, c, decision)
    assert c.comments == {} and c.applied == []


def test_missing_business_review_preserves_known_business_incident_and_source_work():
    health = {"run": {"github_run_id": "100", "event_name": "schedule"}, "source_report": {"available": True},
              "findings": [{"check_id": "source_failure", "severity": "critical", "evidence": {"source_id": "src", "reason_code": "empty"}}]}
    old = {"incidents": {"review:business": {"incident_key": "review:business", "severity": "critical", "source_id": ""}}}
    d = evaluate_health_loop(health, previous_state=old, review={})
    assert d["run_evidence"]["review_available"] is False
    assert any(a["source_id"] == "src" and a["action"] == "create_task" for a in d["actions"])
    assert d["incidents"]["review:business"]["severity"] == "critical"
