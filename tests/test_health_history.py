"""真实报告结构＋正式评论往返；每轮重建客户端，禁止依赖内存计数。"""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from app.development_policy import digest
from app.health_history import canonical_history, counts, make_observation, observation_marker
from app.health_issue import _stable_incident_id, build_incidents, incident_marker
from app.health_loop import evaluate_health_loop
from app.health_loop_sync import CommandResult, GhMetadataClient, SyncError, build_sync_plan


REPORTS = json.loads((Path(__file__).parent / "fixtures/health_loop_oct2026.json").read_text())


def client_for(comments, issues):
    client = GhMetadataClient()
    client._issues = issues
    client._comments = lambda n: deepcopy(comments.get(n, []))
    def execute(args):
        assert args[:2] == ["issue", "comment"]
        number = int(args[2]);body = args[args.index("--body") + 1]
        comments.setdefault(number, []).append({"user": {"login": client.owner}, "body": body})
        return CommandResult("")
    client._run = execute
    return client


def issues_for_reports(reports):
    sources = {}
    for health in reports:
        for row in build_incidents(health):
            if row["source_id"]:
                sources[row["incident_id"]] = {"number": 200 + len(sources), "body": incident_marker(row["incident_id"]), "author": {"login": "github-actions[bot]"}}
    # 编号稳定且唯一，正文可随时被生产覆盖，只保留事故标记。
    for n, issue in enumerate(sources.values(), 200):
        issue["number"] = n
    return list(sources.values())


def test_real_oct25_report_retains_all_four_critical_and_all_evidence():
    rows = build_incidents(REPORTS[-1])
    assert len(rows) == 4
    assert all(row["severity"] == "critical" for row in rows)
    assert all(len(row["evidence_items"]) == 2 for row in rows)


def test_real_five_day_reports_round_trip_through_comments_without_cache():
    comments, issues = {}, issues_for_reports(REPORTS)
    for index, health in enumerate(REPORTS, 1):
        client = client_for(comments, issues)
        evidence = {"run_id": health["run"]["github_run_id"], "commit_sha": health["run"]["commit_sha"]}
        official = client.official_state(run_evidence=evidence)
        decision = evaluate_health_loop(health, previous_state=official, state_origin="github_reconstructed")
        assert len(decision["observations"]) == len(build_incidents(health))
        client.persist_observations(decision)
        before = sum(map(len, comments.values()))
        client.persist_observations(decision)
        assert sum(map(len, comments.values())) == before
        restored = client_for(comments, issues).official_state(run_evidence=evidence)
        persistent = [r for r in restored["incidents"].values() if r.get("source_id") != "wuhan_transport_structured"]
        assert len(persistent) == 4
        assert all(r["occurrence_count"] == index for r in persistent)
        if index >= 4:
            assert restored["incidents"]["source:wuhan_transport_structured:connection_error"]["occurrence_count"] == 1
    # 反向回放旧日只读取当时及之前证据，不从未来累计值倒填。
    restored = client_for(comments, issues).official_state(run_evidence={"run_id": REPORTS[0]["run"]["github_run_id"]})
    assert all(r["occurrence_count"] == 1 for r in restored["incidents"].values())


def test_first_warning_survives_restart_and_second_warning_escalates():
    health = deepcopy(REPORTS[0])
    health["findings"] = [health["findings"][0]]
    comments, issues = {}, issues_for_reports([health])
    client = client_for(comments, issues)
    first = evaluate_health_loop(health, previous_state=client.official_state(), state_origin="github_reconstructed")
    assert all(a["action"] == "observe" for a in first["actions"])
    client.persist_observations(first)
    health["run"]["github_run_id"] = str(int(health["run"]["github_run_id"]) + 1)
    restored = client_for(comments, issues).official_state(run_evidence={"run_id": health["run"]["github_run_id"]})
    second = evaluate_health_loop(health, previous_state=restored, state_origin="github_reconstructed")
    assert all(a["action"] == "create_task" for a in second["actions"])
    assert all(a["occurrence_count"] == 2 for a in second["actions"])


def test_attempt_correction_and_out_of_order_records_do_not_inflate_counts():
    observation = make_observation(REPORTS[0], key="source:zoox:empty", source="zoox", reason="empty", result="failed", severity="critical", participated=True)
    corrected = {**observation, "attempt": 2, "result": "healthy", "severity": "healthy"}
    later = {**observation, "run_id": str(int(observation["run_id"]) + 1)}
    history = canonical_history([later, corrected, observation, corrected])
    assert len(history) == 2
    assert counts(history)["occurrence_count"] == 1
    assert counts(history)["consecutive_failures"] == 1
    with pytest.raises(ValueError, match="相互冲突"):
        canonical_history([corrected, {**corrected, "result": "failed"}])


def test_unbound_business_evidence_is_durable_without_counting_as_valid():
    health = {**REPORTS[0], "findings": [], "overall_status": "healthy"}
    review = {"daily": {"business_status": "empty_uncovered"}}
    decision = evaluate_health_loop(health, review=review, state_origin="github_reconstructed")
    comments = {};client = client_for(comments, [{"number": 100, "body": "control"}])
    official = client.official_state()
    assert build_sync_plan(decision, official) == []
    client.persist_observations(decision)
    restored = client_for(comments, client._issues).official_state()
    assert restored["incidents"]["review:business_empty_uncovered"]["occurrence_count"] == 0
    assert restored["incidents"]["review:business_empty_uncovered"]["observations"]


def test_missing_report_or_nonparticipating_source_cannot_count_recovery():
    health = deepcopy(REPORTS[0]);health["source_report"]["available"] = False
    row = make_observation(health, key="source:zoox:empty", source="zoox", reason="empty", result="healthy", severity="healthy", participated=True)
    assert counts([row])["recovery_count"] == 0
    health["source_report"]["available"] = True
    row = make_observation(health, key="source:zoox:empty", source="zoox", reason="empty", result="healthy", severity="healthy", participated=False)
    assert counts([row])["recovery_count"] == 0


def test_pony_date_failure_does_not_bind_whitelist_issue():
    health = deepcopy(REPORTS[0]);health["findings"] = [row for row in health["findings"] if row.get("evidence", {}).get("source_id") == "pony_news_structured"]
    decision = evaluate_health_loop(health, state_origin="github_reconstructed")
    client = client_for({}, [])
    operation = build_sync_plan(decision, client.official_state())[0]
    assert operation["issue_number"] is None


def test_bad_input_is_not_silently_healthy():
    with pytest.raises(ValueError):
        build_incidents({"findings": [{"evidence": {"failed_sources": "bad"}}]})
    with pytest.raises(ValueError):
        observation_marker([{"schema_version": "unknown"}])


def test_missing_source_report_cannot_create_or_reopen_engineering_task():
    health = deepcopy(REPORTS[-1]);health["source_report"]["available"] = False
    decision = evaluate_health_loop(health, state_origin="github_reconstructed")
    assert all(row["action"] == "needs_approval" for row in decision["actions"])
    assert build_sync_plan(decision, client_for({}, []).official_state()) == []


def test_already_closed_task_does_not_generate_another_close_operation():
    key = "source:miit_news_structured:low_date_parse_rate"
    old = {"incident_key": key, "source_id": "miit_news_structured", "reason_code": "low_date_parse_rate",
           "engineering_issue": 49, "lifecycle": "closed", "recovery_count": 3, "last_run_id": "102",
           "risk": "Medium", "merged_commit": "a"*40, "merged_commit_reachable": True}
    health = {"date_bj": "2026-10-28", "overall_status": "healthy", "source_report": {"available": True},
              "run": {"github_run_id": "103", "event_name": "schedule"}, "findings": []}
    decision = evaluate_health_loop(health, previous_state={"incidents": {key: old}},
             run_report={"source_stats": [{"source_id": "miit_news_structured", "status": "ok"}]}, state_origin="github_reconstructed")
    assert decision["actions"] == []
    assert decision["incidents"][key]["lifecycle"] == "closed"
    assert decision["incidents"][key]["recovery_count"] == 3


def test_official_reconstruction_to_recovery_operation_preserves_source_category():
    client = client_for({}, [{"number": 49, "body": "engineering", "state": "OPEN", "author": {"login": "xiexie20211028-bot"}}])
    client._items = [{"content": {"number": 49}, "status": "观察中"}]
    client._merged_pr_evidence = lambda n: {"merged_commit": "a"*40}
    client._commit_reachable = lambda *args: True
    official = client.official_state(run_evidence={"run_id": "103", "commit_sha": "b"*40})
    health = {"date_bj": "2026-10-28", "overall_status": "healthy", "source_report": {"available": True},
              "run": {"github_run_id": "103", "event_name": "schedule", "commit_sha": "b"*40}, "findings": []}
    decision = evaluate_health_loop(health, previous_state=official,
             run_report={"source_stats": [{"source_id": "miit_news_structured", "status": "ok"}]}, state_origin="github_reconstructed")
    operations = build_sync_plan(decision, official)
    assert len(operations) == 1 and operations[0]["operation"] == "verify"


def test_self_check_internal_failure_cannot_count_source_recovery():
    health = deepcopy(REPORTS[-1])
    health["findings"] = [{"check_id": "self_check_internal_failure", "severity": "critical", "evidence": {}}]
    row = make_observation(health, key="source:zoox:empty", source="zoox", reason="empty", result="healthy", severity="healthy", participated=True)
    assert row["valid"] is False and counts([row])["recovery_count"] == 0


def test_combined_sync_entry_preserves_run_guard_observations_and_deferred_work(tmp_path, monkeypatch):
    from argparse import Namespace
    from app import health_loop_sync as sync
    decision = evaluate_health_loop(REPORTS[-1], state_origin="github_reconstructed")
    path = tmp_path / "decision.json";path.write_text(json.dumps(decision))
    calls = []
    class Client:
        def preflight(self, **kwargs):
            calls.append("preflight")
            return {"schema_version": sync.STATE_SCHEMA, "complete": True, "incidents": {}}
        def validate_write_access(self):calls.append("access")
        def persist_observations(self, value):
            assert len(value["observations"]) == 4
            self.run_guard();calls.append("observations")
            return [{"status": "recorded"}]
        def apply(self, operations):
            assert len(operations) == 3
            self.run_guard();calls.append("apply")
            return [{"status": "applied"}]
    monkeypatch.setattr(sync, "GhMetadataClient", Client)
    args = Namespace(decision=str(path), mode="apply", out=str(tmp_path / "sync.json"), state_out="", run_id="combined-run")
    assert sync.sync_main(args, lambda: calls.append("guard")) == 0
    receipt = json.loads((tmp_path / "sync.json").read_text())
    assert receipt["development_run_id"] == "combined-run"
    assert receipt["observations_recorded"]
    assert receipt["complete"] is False and len(receipt["deferred_operations"]) == 1
    assert calls.count("guard") == 2


BUSINESS = json.loads((Path(__file__).parent / "fixtures/business_review_oct12_observations.json").read_text())
BUSINESS_KEY = "review:business_empty_uncovered"


def business_replay():
    # 从真实正式记录取生产身份，只还原本测试涉及的业务评审，省略无关的来源 findings。
    original = BUSINESS["bound"]["observations"][0]
    health = {"date_bj": original["run_at"], "source_report": {"available": True}, "findings": [],
              "run": {"github_run_id": original["run_id"], "github_run_attempt": original["attempt"],
                      "commit_sha": original["commit_sha"], "event_name": original["event_name"]}}
    review = deepcopy(BUSINESS["review"])
    evidence = {"status": "invalid", "date": original["run_at"],
                "production_run_id": original["run_id"], "production_attempt": original["attempt"],
                "production_sha": original["commit_sha"], "review_run_id": original["review_run_id"],
                "attempt": original["review_attempt"], "review_sha": BUSINESS["review_commit"],
                "sha256": original["review_sha256"]}
    assert evidence["sha256"] == digest(review)
    return health, review, evidence


def test_real_invalid_review_restart_correction_and_duplicate_write():
    health, review, evidence = business_replay()
    owner = "xiexie20211028-bot"
    comments = {100: [{"user": {"login": owner}, "body": observation_marker(BUSINESS["bound"]["observations"])}]}
    issues = [{"number": 100, "body": "control"}]

    def replay(review, evidence):
        client = client_for(comments, issues)
        previous = client.official_state()
        decision = evaluate_health_loop(health, review=review, review_binding=evidence,
                                        previous_state=previous, state_origin="github_reconstructed")
        client.persist_observations(decision)
        return client_for(comments, issues).official_state()["incidents"][BUSINESS_KEY]

    invalid = replay(review, evidence)
    assert invalid["occurrence_count"] == invalid["recovery_count"] == 0
    assert len(invalid["observations"]) == len(comments[100]) == 1
    assert replay(review, evidence) == invalid and len(comments[100]) == 1
    # 隔离修订样本：相同复盘 run 的新 attempt 给出有效统计日。
    updated = deepcopy(review)
    updated["daily"]["valid_statistical_day"] = True
    correction = {**evidence, "status": "matched", "attempt": 2, "sha256": digest(updated)}
    verified = replay(updated, correction)
    assert verified["occurrence_count"] == len(verified["observations"]) == 1
    assert verified["observations"][0]["review_attempt"] == 2
    assert replay(review, evidence) == verified and len(comments[100]) == 2
    assert replay(updated, correction) == verified and len(comments[100]) == 2


def test_real_unbound_observation_and_bound_correction_in_both_orders():
    legacy = BUSINESS["legacy"]["observations"][0]
    assert legacy["valid"] is True and "review_run_id" not in legacy
    assert counts([legacy])["occurrence_count"] == 0
    # 使用同生产、同事故的旧格式变体覆盖归并顺序。
    bound = BUSINESS["bound"]["observations"][0]
    unbound = {k: v for k, v in bound.items() if not k.startswith("review_")}
    unbound["valid"] = True
    for rows in ([unbound, bound], [bound, unbound]):
        assert canonical_history(rows)[0]["review_run_id"] == bound["review_run_id"]
        assert counts(rows)["occurrence_count"] == 0


@pytest.mark.parametrize("change", [{"review_sha256": "a" * 64}, {"valid": True}, {"review_run_id": "999999"}])
def test_conflicting_review_observations_fail_closed_in_both_orders(change):
    bound = BUSINESS["bound"]["observations"][0]
    disputed = {**bound, **change}
    for rows in ([bound, disputed], [disputed, bound]):
        with pytest.raises(ValueError):
            canonical_history(rows)


def test_invalid_business_review_leaves_check_and_source_evidence_unchanged():
    health, review, binding = business_replay()
    # 压力场景：真实控制评论里没发现 check:* 被污染，不把构造的检查项当成线上事故事实。
    health["findings"] = [{"check_id": "delivery_missing", "severity": "critical", "evidence": {}},
                          {"check_id": "empty_listing", "severity": "critical",
                           "evidence": {"source_id": "zoox_news_structured", "reason_code": "empty"}}]
    original = evaluate_health_loop(health)["observations"]
    with_review = evaluate_health_loop(health, review=review, review_binding=binding)["observations"]
    assert {r["incident_key"].split(":")[0] for r in original} == {"check", "source"}
    assert [r for r in with_review if not r["incident_key"].startswith("review:")] == original
    assert all(r["valid"] for r in original)
    assert all(not r["valid"] for r in with_review if r["incident_key"].startswith("review:"))


def test_review_tagged_check_history_uses_verified_production_row_only():
    health, _, binding = business_replay()
    health["findings"] = [{"check_id": "delivery_missing", "severity": "critical", "evidence": {}}]
    check, = evaluate_health_loop(health)["observations"]
    tagged = {**check, "valid": False, "review_run_id": binding["review_run_id"],
              "review_attempt": 1, "review_sha256": binding["sha256"]}
    assert counts([tagged])["occurrence_count"] == 0
    for rows in ([tagged, check], [check, tagged]):
        assert canonical_history(rows) == [check]
        assert counts(rows)["occurrence_count"] == 1
    with pytest.raises(ValueError):
        canonical_history([check, {**tagged, "severity": "warning"}])


def test_later_review_withdrawal_supersedes_failure_without_recovery():
    health, review, evidence = business_replay()
    # 构造一次有效故障与后继撤回，不修改真实快照。
    reported = deepcopy(review)
    reported["daily"]["valid_statistical_day"] = True
    first = {**evidence, "status": "matched", "attempt": 2, "sha256": digest(reported)}
    withdrawn = deepcopy(reported)
    withdrawn["daily"]["business_status"] = "healthy"
    later = {**first, "attempt": 3, "sha256": digest(withdrawn)}
    comments = {}
    issues = [{"number": 100, "body": "control"}]

    def persist(value, binding):
        client = client_for(comments, issues)
        decision = evaluate_health_loop(health, previous_state=client.official_state(),
                                        review=value, review_binding=binding, state_origin="github_reconstructed")
        client.persist_observations(decision)
        return client_for(comments, issues).official_state()["incidents"][BUSINESS_KEY]

    assert persist(reported, first)["occurrence_count"] == 1
    corrected = persist(withdrawn, later)
    assert corrected["occurrence_count"] == corrected["recovery_count"] == 0
    observation, = corrected["observations"]
    assert observation["result"] == "unknown" and observation["review_attempt"] == 3
    assert persist(reported, first) == corrected and len(comments[100]) == 2
