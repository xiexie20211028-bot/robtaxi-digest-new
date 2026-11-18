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
    run_id = "test-run"
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


@pytest.mark.parametrize("newer_run,newer_attempt", [(1, 1), (0, 2)])
def test_historical_backfill_does_not_apply_old_task_actions_or_replace_queue(tmp_path, monkeypatch, newer_run, newer_attempt):
    c = Client(missing=False)
    c.official["latest_observed_run"] = int(FIXTURE["decision"]["run_id"]) + newer_run
    c.official["latest_observed_attempt"] = newer_attempt
    result, control = execute(tmp_path, monkeypatch, c)
    assert result["historical_only"] and not result["complete"]
    assert c.applied == []
    assert all(r["event"] == "health_write_intent" for r in control.rows)
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


def request_failed(message):
    raise sync.SyncError("GitHub 请求异常") from sync.RequestFailure(["gh", "issue", "comment"], message, 1, 0.1)


@pytest.mark.parametrize("landed", [True, False])
def test_observation_response_loss_reconciles_target_and_isolates_unknown_after_restart(landed):
    client = Client(missing=False)
    control = Control()
    client.transport = control
    write = client._run
    submitted = []

    def lose_response(args):
        number = int(args[2])
        submitted.append(number)
        if len(submitted) == 2:
            if landed:
                write(args)
            request_failed("EOF")
        return write(args)

    client._run = lose_response
    client.prepare_observations(FIXTURE["decision"], apply=True)
    assert len(submitted) == 7
    missing = submitted[1]
    blocked = {i["issue_number"] for i in client.item_results if i["status"] == "blocked"}
    assert blocked == (set() if landed else {missing})
    assert all(i.get("write_status") == "unknown" for i in client.item_results if i["status"] == "blocked")

    restored = Client(missing=False)
    restored.comments = deepcopy(client.comments)
    restored.transport = control
    restored._run = lambda args: pytest.fail(f"重复写入 {args[:3]}")
    restored.prepare_observations(FIXTURE["decision"], apply=True)
    assert {i["issue_number"] for i in restored.item_results if i["status"] == "blocked"} == blocked
    assert all(len(comments) == 1 for comments in restored.comments.values())

    if not landed:
        saved = next(r for r in FIXTURE["decision"]["observations"]
                     if r["incident_key"] == next(i["incident_key"] for i in restored.item_results if i["status"] == "blocked"))
        restored.comments[missing] = [{"user": {"login": restored.owner}, "body": sync.observation_marker([saved])}]
        restored.prepare_observations(FIXTURE["decision"], apply=True)
        assert all(i["status"] == "synced" for i in restored.item_results)
    assert len(control.rows) == 7


def test_authorization_error_stops_all_writes_and_is_never_treated_as_item_success():
    client = Client(missing=False)
    client.transport = Control()
    client._run = lambda args: request_failed("HTTP 403 permission denied")
    with pytest.raises(sync.SyncError, match="请求异常"):
        client.prepare_observations(FIXTURE["decision"], apply=True)
    assert len(client.transport.rows) == 1
    assert not client.comments


def test_restored_plan_rechecks_manual_changes_and_preserves_original_permission():
    client = Client(missing=False)
    client.transport = Control()
    decision = FIXTURE["decision"]
    original = next(r for r in decision["changed_actions"] if r["action"] == "create_task" and not r.get("reopen"))
    evidence = {**decision, "changed_actions": [original]}
    official = deepcopy(client.official)
    key = original["incident_key"]
    official["incidents"][key] = {}
    ops, _ = sync.isolated_sync_batches(evidence, official, [])
    assert len(ops) == 1
    client.resume_decision(evidence, authorize=ops)

    restored = Client(missing=False)
    restored.transport = client.transport
    current = restored.resume_decision({**evidence, "changed_actions": [],
                                       "business_review": {"status": "valid", "run_id": "222", "reason": "复盘已到达"}})
    official["incidents"][key] = {"engineering_issue": 777, "issue_state": "OPEN", "project_status": "开发中"}
    ops, _ = sync.isolated_sync_batches(current, official, [])
    assert len(ops) == 1 and ops[0]["issue_number"] == 777
    official["incidents"][key]["issue_state"] = "CLOSED"
    items = []
    assert sync.isolated_sync_batches(current, official, items) == ([], [])
    assert items[0]["status"] == "blocked" and "已关闭" in items[0]["reason"]


def test_partial_engineering_writes_resume_from_remote_state_without_clobbering_assignee_or_progress():
    control = Control()
    owner = sync.PROJECT_OWNER
    url = f"https://github.com/{sync.REPOSITORY}/issues/801"
    remote = {"issues": [], "items": [], "comments": {}}
    attempts = []
    action = {"incident_key": "source:abc:parse", "source_id": "abc", "reason_code": "parse",
              "operation": "ensure_task", "action_fingerprint": "s", "run_id": "123",
              "recovery_count": 0, "merged_commit": ""}
    injected = set()

    def lost_once(step):
        if step not in injected:
            injected.add(step)
            request_failed("EOF")

    def session():
        c = sync.GhMetadataClient()
        c.transport = control
        c._issues = deepcopy(remote["issues"])
        c._items = deepcopy(remote["items"])
        c._comments = lambda number: deepcopy(remote["comments"].get(number, []))
        c._task_fields = lambda op: {"Status": "待办", "Priority": "P1", "Target": "本周"}

        def read(args):
            if args[:2] == ["issue", "list"]:
                return deepcopy(remote["issues"])
            if args[:2] == ["project", "item-list"]:
                return {"items": deepcopy(remote["items"])}
            if args[:2] == ["issue", "view"]:
                return deepcopy(remote["issues"][0])
            pytest.fail(f"未知的远端读取 {args[:2]}")

        def write(args):
            step = args[:2]
            attempts.append(tuple(step))
            if step == ["issue", "create"]:
                body = args[args.index("--body") + 1]
                remote["issues"].append({"number": 801, "url": url, "body": body, "author": {"login": owner},
                                         "state": "OPEN", "assignees": [{"login": "human-owner"}]})
                lost_once("create")
            elif step == ["issue", "comment"]:
                remote["comments"].setdefault(801, []).append({"body": args[args.index("--body")+1], "user": {"login":owner}})
                lost_once("comment")
            elif step == ["issue", "close"]:
                remote["issues"][0]["state"] = "CLOSED"
                lost_once("close")
            else:
                pytest.fail(f"未知的远端写入 {args[:2]}")
            return sync.CommandResult("")

        def add(number, issue_url):
            assert issue_url == url
            attempts.append(("project", "add"))
            remote["items"].append({"content": {"number":number,"url":url}, "id":"I","status":"开发中"})
            lost_once("add")

        def field(item, name, value):
            attempts.append(("field", name))
            if name == "Target" and "target" not in injected:
                injected.add("target")
                request_failed("EOF")
            remote["items"][0][name[0].lower() + name[1:]] = value
            lost_once("field:" + name)
        c._run, c._json, c._set_field, c._ensure_project_item = write, read, field, add
        return c

    with pytest.raises(sync.ItemWriteUnknown):
        session()._apply_task(action)
    assert remote["items"][0]["status"] == "开发中"
    assert remote["items"][0]["priority"] == "P1"
    with pytest.raises(sync.ItemWriteUnknown):
        session()._apply_task(action)
    with pytest.raises(sync.ItemWriteUnknown, match="旧 field:Target"):
        session()._apply_task({**action, "run_id": "124", "action_fingerprint": "later"})
    assert attempts.count(("field", "Target")) == 1
    # 迟到的权威读取确认 Target 已经写入，下一轮只补回执。
    remote["items"][0]["target"] = "本周"
    assert session()._apply_task(action) == 801
    assert len(remote["comments"][801]) == 1
    closing = {**action, "operation": "close", "issue_number": 801, "action_fingerprint": "c"}
    assert session()._apply_task(closing) == 801
    assert session()._apply_task(closing) == 801
    assert attempts.count(("issue", "create")) == attempts.count(("project", "add")) == 1
    assert attempts.count(("issue", "close")) == 1 and len(remote["comments"][801]) == 2
    assert remote["issues"][0]["assignees"] == [{"login": "human-owner"}]
    assert all(len([r for r in control.rows if r["key"] == row["key"]]) == 1 for row in control.rows)


def test_duplicate_engineering_issues_block_only_the_affected_operation():
    client = sync.GhMetadataClient()
    client.transport = Control()
    key = "source:abc:parse"
    for number in (701, 702):
        client._issues.append({"number": number, "body": sync._incident_marker(key),
                               "state": "OPEN", "author": {"login": client.owner}})
    with pytest.raises(sync.ItemWriteBlocked, match="多张工程任务"):
        client._apply_task({"incident_key": key, "issue_number": 701, "operation": "ensure_task"})
    assert client.transport.rows == []


def test_item_lookup_uses_repository_and_issue_identity():
    client = sync.GhMetadataClient()
    client._items = [
        {"id": "wrong", "content": {"number": 701, "url": "https://github.com/other/repo/issues/701"}},
        {"id": "right", "content": {"number": 701, "url": f"https://github.com/{client.repository}/issues/701"}},
    ]
    assert client._project_item(701)["id"] == "right"
