"""真实旧事故记录回放，验证生产与研发共用身份解析。"""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from app.health_issue import (_stable_incident_id, incident_marker, resolve_issue_for_incident,
                              GitHubClient, sync_health_issue)
from app.health_history import parse_observations, counts
from app.health_loop_sync import GhMetadataClient, CommandResult

FIXTURE = json.loads((Path(__file__).parent / "fixtures/health_loop_legacy_oct29.json").read_text())


def history_client(comments, issues):
    client = GhMetadataClient(); client._issues = deepcopy(issues)
    client._comments = lambda n: deepcopy(comments.get(n, []))
    def execute(args):
        assert args[:2] == ["issue", "comment"]
        comments.setdefault(int(args[2]), []).append({"user": {"login": client.owner}, "body": args[args.index("--body") + 1]})
        return CommandResult("")
    client._run = execute
    return client


def test_real_all_seven_observations_survive_restart_without_reopening_old_issues():
    issues = deepcopy(FIXTURE["issues"]); before = deepcopy(issues); comments = {}
    client = history_client(comments, issues)
    client.persist_observations(FIXTURE["decision"])
    rows = [r for cs in comments.values() for r in parse_observations(cs, client.owner)]
    assert len(rows) == 7
    assert sum(r["severity"] == "critical" and bool(r["source_id"]) for r in rows) == 4
    assert parse_observations(comments[31], client.owner)[0]["result"] == "healthy"
    # 原报告只给出未知恢复证据，兼容编号不得顺便把它改成健康。
    assert parse_observations(comments[29], client.owner)[0]["result"] == "unknown"
    assert counts(parse_observations(comments[29], client.owner))["recovery_count"] == 0
    assert 36 not in comments and 34 not in comments
    assert "#31, #36" in comments[31][0]["body"]
    assert counts(parse_observations(comments[31], client.owner))["recovery_count"] == 1
    old = deepcopy(comments)
    history_client(comments, issues).persist_observations(FIXTURE["decision"])
    assert comments == old and issues == before


def test_new_identity_wins_and_duplicate_new_markers_stop_resolution():
    issues = deepcopy(FIXTURE["issues"])
    identity = _stable_incident_id("", "miit_news_structured", "low_date_parse_rate")
    resolve = lambda: resolve_issue_for_incident(issues, identity, source_id="miit_news_structured", reason_code="low_date_parse_rate")
    assert resolve()["issue"]["number"] == 31
    issues += [{"number": 200, "body": incident_marker(identity)}]
    assert resolve()["issue"]["number"] == 200
    issues += [{"number": 201, "body": incident_marker(identity)}]
    with pytest.raises(ValueError, match="多个新标记"): resolve()


def test_wrong_reason_or_title_only_never_aliases_a_different_root_cause():
    issues = [{"number": 1, "body": "legacy", "title": "pony_news_structured low_date_parse_rate"}]
    assert resolve_issue_for_incident(issues, "new", source_id="pony_news_structured", reason_code="low_date_parse_rate")["issue"] is None
    assert resolve_issue_for_incident(FIXTURE["issues"], "new", source_id="pony_news_structured", reason_code="different")["issue"] is None


def test_production_reuses_same_legacy_main_and_preserves_original_snapshot():
    class Client(GitHubClient):
        def __init__(self):
            self.repository = "owner/repo"; self.issues = deepcopy(FIXTURE["issues"]); self.comments = []; self.updated = []
        def ensure_labels(self): pass
        def list_health_issues(self): return self.issues
        def request(self, method, path, payload=None, **kwargs):
            assert method == "GET" and "/comments?" in path
            return self.comments
        def add_comment(self, number, body): self.comments.append({"body": body}); return {}
        def update_issue(self, number, **data):
            self.updated.append(number)
            return {"number": number, "html_url": "url", **data}
        def create_issue(self, *args): raise AssertionError("不得重复建单")
    c = Client(); old_body = next(i["body"] for i in c.issues if i["number"] == 31)
    health = {"source_report": {"available": False}, "run": {"github_run_id": "999"},
              "findings": [{"check_id": "required_source_miit", "severity": "critical", "evidence": {
                  "source_id": "miit_news_structured", "reason_code": "low_date_parse_rate"}}]}
    result = sync_health_issue(client=c, health=health, repair=None, artifact_name="test")
    assert result["issue_numbers"] == [31]
    assert c.updated == [31] and old_body in c.comments[0]["body"]
    before = len(c.comments)
    sync_health_issue(client=c, health=health, repair=None, artifact_name="test")
    assert len(c.comments) == before


def test_mismatching_new_marker_and_body_is_not_trusted():
    issue = deepcopy(FIXTURE["issues"][0])
    wrong = _stable_incident_id("", "other_source", "other_reason")
    issue["body"] = incident_marker(wrong) + "\n" + issue["body"]
    with pytest.raises(ValueError, match="不一致"):
        resolve_issue_for_incident([issue], wrong, source_id="other_source", reason_code="other_reason")


def test_engineering_issue_quoting_marker_is_not_an_incident_record():
    health = {**deepcopy(FIXTURE["issues"][0]), "labels": ["robtaxi-health"]}
    identity = _stable_incident_id("", "miit_news_structured", "low_date_parse_rate")
    engineering = {"number": 200, "labels": ["engineering"], "body": incident_marker(identity) + "\n" + health["body"]}
    result = resolve_issue_for_incident([engineering, health], identity,
        source_id="miit_news_structured", reason_code="low_date_parse_rate")
    assert result["issue"]["number"] == 31
