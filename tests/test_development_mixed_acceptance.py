"""同一任务同时有普通和历史交付时，从正常入口验证整体状态。"""
from copy import deepcopy

import pytest

from app.development_acceptance import binding_for
from app.development_budget import delivery_token, pending_deliveries
from app.development_cycle import load_policy
from app.development_policy import digest
from app.development_production import reconcile_production
from scripts.development_delivery import record_delivery
from test_development_production import ProductionClient, contract


@pytest.fixture
def mixed(monkeypatch):
    client = ProductionClient()
    policy = {**load_policy(), "mode": "active", "schema_version": "robtaxi-autonomy-policy-v3"}
    number = 84
    client.row.update(number=number, blockers=[83])
    binding = {**binding_for(number), "checks": ["原离线业务验收"]}
    original = {"event": "historical_delivery_adopted", "key": "original", "issue": number,
                "facts": {"pr": 88, "merge_sha": "old", "head_sha": "old-head",
                          "production": binding, "remaining_scope": []}}
    revised = {**original, "key": "revised"}
    current_contract = {**contract(), "issue": number, "production": {"kind": "none", "checks": ["仅测试，合并检查通过即可验收"]}}
    ordinary = {**client.history[-1], "issue": number, "key": "ordinary", "production": current_contract["production"],
                "contract_digest": digest(current_contract),
                "supersedes_tokens": [delivery_token(pending_deliveries([original])[0])]}
    client.history = [{"event": "contract", "producer": "codex-scheduled", "contract": current_contract},
                      original, ordinary, revised]
    client.control = deepcopy(client.history[1:])
    monkeypatch.setattr("app.development_cycle.assert_trusted_checkout", lambda c: "b" * 40)
    monkeypatch.setattr("app.development_cycle.set_status", lambda _p, _n, value: client.row.update(status=value))
    monkeypatch.setattr("app.development_history.inspect_delivery", lambda *a: {
        "merge_sha": "old", "head_sha": "old-head", "tests_verified": True, "main_sha": "b" * 40})
    return client, policy


def test_revised_adoption_blocks_whole_issue_then_normal_entry_completes_both(mixed):
    client, policy = mixed
    for apply in (False, True, True):
        row = reconcile_production(client, policy, 84, apply=apply)["results"][0]
        assert row["status"] == "remaining_deliveries" and row["delivery_status"] == "complete"
        assert row["historical_results"][0]["status"] == "remaining_scope_or_dependency"
        assert [d["adoption_key"] for d in row["pending_deliveries"]] == ["revised"]
        assert client.row["state"] == "OPEN" and client.row["status"] == "观察中"
    assert sum(e["event"] == "production_verified" for e in client.control) == 1

    client.row["blockers"] = []
    row = reconcile_production(client, policy, 84, apply=True)["results"][0]
    assert row["status"] == "complete"
    assert client.row["state"] == "CLOSED" and client.row["status"] == "已完成"
    for book in (client.history, client.control):
        assert [e["adoption_key"] for e in book if e["event"] == "historical_production_verified"] == ["revised"]
        assert pending_deliveries(book, include_no_observation=True) == []


@pytest.mark.parametrize("missing_on", ["issue", "control"])
def test_mixed_delivery_missing_current_registration_never_closes(mixed, missing_on):
    client, policy = mixed
    (client.history if missing_on == "issue" else client.control).pop()
    client.row["blockers"] = []
    row = reconcile_production(client, policy, 84, apply=True)["results"][0]
    assert row["status"] == "delivery_records_incomplete"
    assert client.row["state"] == "OPEN" and client.row["status"] == "观察中"


def test_kind_none_records_its_own_success_then_defers_issue_close_for_history(mixed, monkeypatch):
    client, policy = mixed
    original = client.control[0]
    client.control = [original, {"event": "reserve", "key": "merge", "impact_keys": [], "supersedes_tokens": []}]
    client.history = [original]
    monkeypatch.setattr("scripts.development_delivery.set_status", lambda p, n, v: client.row.update(status=v))
    pr = {"number": 100, "head": {"sha": "h"}, "merge_commit_sha": "m", "merged_at": "2026-11-18T03:00:00Z"}
    record_delivery(client, policy, pr, 84, {"kind": "none"}, "digest", "merge", client.control, client.history)
    assert all(any(r["event"] == "production_verified" and r["contract_digest"] == "digest" for r in book)
               for book in (client.control, client.history))
    assert client.row["state"] == "OPEN" and client.row["status"] == "观察中"


@pytest.mark.parametrize("related", [False, True])
def test_separate_ordinary_deliveries_require_exact_chain_or_specific_reconciliation(mixed, related):
    client, policy = mixed
    client.row["blockers"] = []
    ordinary = deepcopy(client.history[2])
    earlier = {**ordinary, "key": "earlier", "pr": 99, "contract_digest": "old-contract",
               "merge_sha": "old-ordinary", "supersedes_tokens": []}
    ordinary["supersedes_tokens"] = [delivery_token(earlier)] if related else []
    client.history = [client.history[0], earlier, ordinary]
    client.control = deepcopy(client.history[1:])
    row = reconcile_production(client, policy, 84, apply=True)["results"][0]
    if related:
        assert row["status"] == "complete" and client.row["state"] == "CLOSED"
    else:
        assert row["status"] == "remaining_deliveries" and client.row["state"] == "OPEN"
        assert "原合同对账" in row["reason"]
        assert row["pending_deliveries"] == [{"merge_sha": "old-ordinary", "contract_digest": "old-contract",
                                              "adoption_key": None}]


@pytest.mark.parametrize("apply", [False, True])
def test_control_only_ordinary_delivery_cannot_close_historical_task(mixed, apply):
    client, policy = mixed
    client.row["blockers"] = []
    client.history.pop(2)
    rows = reconcile_production(client, policy, 84, apply=apply)["results"]
    assert rows[0]["status"] == "delivery_records_incomplete"
    assert client.row["state"] == "OPEN"


def test_one_sided_verified_evidence_is_not_enough_to_close_mixed_history(mixed):
    client, policy = mixed
    client.row["blockers"] = [83]
    key = "historical-verified:revised"
    first = {"event": "historical_production_verified", "key": key, "issue": 84,
             "adoption_key": "revised", "merge_sha": "old", "evidence": [1]}
    client.history.append(first)
    client.control.append({**first, "evidence": [2]})
    result = reconcile_production(client, policy, 84, apply=True)["results"][0]
    assert result["status"] == "delivery_records_incomplete" and client.row["state"] == "OPEN"
    assert "冲突" in result["reason"]


def test_single_keyed_ordinary_receipt_requires_registration_on_both_sides(mixed):
    client, policy = mixed
    client.row["blockers"] = []
    ordinary = deepcopy(client.history[2])
    ordinary["supersedes_tokens"] = []
    client.history = [client.history[0], ordinary]
    client.control = []
    result = reconcile_production(client, policy, 84, apply=True)["results"][0]
    assert result["status"] == "delivery_records_incomplete" and client.row["state"] == "OPEN"
    client.control.append(deepcopy(ordinary))
    result = reconcile_production(client, policy, 84, apply=True)["results"][0]
    assert result["status"] == "complete" and client.row["state"] == "CLOSED"
