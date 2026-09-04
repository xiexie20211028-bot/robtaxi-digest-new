from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.health_loop import action_fingerprint
from app.health_loop_sync import (
    DECISION_SCHEMA,
    STATE_SCHEMA,
    GhMetadataClient,
    SyncError,
    build_sync_plan,
    main,
    validate_decision,
)


def _action(action: str = "create_task", **overrides: object) -> dict:
    row = {
        "action": action,
        "incident_key": "source:miit_news_structured:low_date_parse_rate",
        "category": "source",
        "source_id": "miit_news_structured",
        "reason_code": "low_date_parse_rate",
        "severity": "critical",
        "risk": "Medium",
        "occurrence_count": 2,
        "last_run_id": "102",
        "engineering_issue": None,
        "recovery_count": 0,
        "merged_commit": "",
        "merged_commit_reachable": False,
        "source_run_evidence": {},
    }
    row.update(overrides)
    row["action_fingerprint"] = action_fingerprint(row)
    return row


def _decision(row: dict, *, origin: str = "github_reconstructed") -> dict:
    return {
        "schema_version": DECISION_SCHEMA,
        "run_id": "102",
        "state_origin": origin,
        "run_evidence": {
            "run_id": "102",
            "event_name": "schedule",
            "commit_sha": "b" * 40,
            "health_report_available": True,
        },
        "changed_actions": [row],
    }


def _official(record: dict | None = None) -> dict:
    return {
        "schema_version": STATE_SCHEMA,
        "complete": True,
        "incidents": {} if record is None else {record["incident_key"]: record},
    }


def test_known_source_reuses_engineering_issue_and_ignores_local_issue_number() -> None:
    row = _action(engineering_issue=999)
    operations = build_sync_plan(_decision(row), _official())

    assert operations[0]["operation"] == "ensure_task"
    assert operations[0]["issue_number"] == 49


def test_duplicate_fingerprint_is_idempotently_skipped() -> None:
    row = _action()
    current = {
        "incident_key": row["incident_key"],
        "engineering_issue": 49,
        "issue_state": "OPEN",
        "applied_fingerprints": [row["action_fingerprint"]],
    }

    assert build_sync_plan(_decision(row), _official(current)) == []


def test_invalid_fingerprint_duplicate_key_and_batch_limit_fail_closed() -> None:
    bad = _action()
    bad["severity"] = "warning"
    with pytest.raises(SyncError, match="fingerprint"):
        validate_decision(_decision(bad))

    duplicate = _action()
    decision = _decision(duplicate)
    decision["changed_actions"] = [duplicate, dict(duplicate)]
    with pytest.raises(SyncError, match="重复"):
        validate_decision(decision)

    decision["changed_actions"] = [_action(incident_key=f"source:s{i}:reason") for i in range(4)]
    with pytest.raises(SyncError, match="超过 3"):
        validate_decision(decision)


def _recovery_action(action: str, *, recovery_count: int, run_id: str = "102", risk: str = "Medium") -> dict:
    evidence = {
        "run_id": run_id,
        "event_name": "schedule",
        "commit_sha": "b" * 40,
        "health_report_available": True,
        "source_participated": True,
    }
    return _action(
        action,
        last_run_id=run_id,
        recovery_count=recovery_count,
        risk=risk,
        merged_commit="a" * 40,
        merged_commit_reachable=True,
        source_run_evidence=evidence,
    )


def test_close_requires_two_distinct_runs_and_github_merge_evidence() -> None:
    row = _recovery_action("close", recovery_count=2)
    current = {
        "incident_key": row["incident_key"],
        "engineering_issue": 49,
        "issue_state": "OPEN",
        "merged_commit": "a" * 40,
        "merged_commit_reachable": True,
        "latest_sync": {"action": "verify", "recovery_count": 1, "run_id": "101"},
        "applied_fingerprints": [],
    }

    operations = build_sync_plan(_decision(row), _official(current))
    assert operations[0]["operation"] == "close"
    assert operations[0]["source_run_evidence"]["source_participated"] is True

    current["latest_sync"]["run_id"] = "102"
    with pytest.raises(SyncError, match="同一个运行"):
        build_sync_plan(_decision(row), _official(current))

    current["latest_sync"]["run_id"] = "101"
    current["merged_commit_reachable"] = False
    with pytest.raises(SyncError, match="生产已包含"):
        build_sync_plan(_decision(row), _official(current))


def test_recovery_cannot_use_local_cache_manual_run_or_high_risk() -> None:
    row = _recovery_action("verify", recovery_count=1)
    with pytest.raises(SyncError, match="GitHub 正式状态"):
        validate_decision(_decision(row, origin="local_cache"))

    row = _recovery_action("verify", recovery_count=1)
    row["source_run_evidence"]["event_name"] = "workflow_dispatch"
    row["action_fingerprint"] = action_fingerprint(row)
    with pytest.raises(SyncError, match="手动运行"):
        validate_decision(_decision(row))

    row = _recovery_action("verify", recovery_count=1, risk="High")
    with pytest.raises(SyncError, match="非 High"):
        validate_decision(_decision(row))


def test_github_commands_are_argument_arrays_not_shell(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[object] = []

    class Result:
        returncode = 0
        stdout = "{}"

    def fake_run(command: object, **kwargs: object) -> Result:
        seen.append((command, kwargs.get("shell")))
        return Result()

    monkeypatch.setattr("app.health_loop_sync.subprocess.run", fake_run)
    GhMetadataClient()._run(["issue", "view", "$(touch /tmp/never)"])

    assert seen == [(["gh", "issue", "view", "$(touch /tmp/never)"], None)]


def test_official_state_verifies_linked_merge_against_current_production_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = "source:miit_news_structured:low_date_parse_rate"
    client = GhMetadataClient()
    client._issues = [
        {
            "number": 49,
            "state": "OPEN",
            "body": "",
            "url": "https://example.test/issues/49",
            "author": {"login": "xiexie20211028-bot"},
        }
    ]
    client._items = [{"content": {"number": 49}, "status": "观察中", "id": "item-49"}]
    state_line = json.dumps(
        {
            "incident_key": key,
            "action": "verify",
            "run_id": "101",
            "recovery_count": 1,
            "severity": "warning",
            "risk": "Medium",
        },
        separators=(",", ":"),
    )
    monkeypatch.setattr(
        client,
        "_comments",
        lambda _number: [
            {
                "user": {"login": "xiexie20211028-bot"},
                "body": f"<!-- robtaxi-health-loop-state:{state_line} -->",
            }
        ],
    )
    monkeypatch.setattr(
        client,
        "_merged_pr_evidence",
        lambda _number: {"pr_number": 90, "merged_commit": "a" * 40},
    )
    calls: list[tuple[str, str]] = []

    def reachable(base: str, head: str) -> bool:
        calls.append((base, head))
        return True

    monkeypatch.setattr(client, "_commit_reachable", reachable)
    official = client.official_state(run_evidence={"commit_sha": "b" * 40})

    assert official["incidents"][key]["merge_evidence"]["pr_number"] == 90
    assert official["incidents"][key]["merged_commit_reachable"] is True
    assert calls == [("a" * 40, "b" * 40)]


def test_machine_state_markers_from_other_commenters_are_ignored() -> None:
    client = GhMetadataClient()
    trusted = '<!-- robtaxi-health-loop-state:{"incident_key":"source:trusted:reason"} -->'
    untrusted = '<!-- robtaxi-health-loop-state:{"incident_key":"source:attacker:reason"} -->'
    latest, fingerprints = client._latest_sync(
        [
            {"user": {"login": "xiexie20211028-bot"}, "body": trusted},
            {
                "user": {"login": "someone-else"},
                "body": "<!-- robtaxi-health-loop-sync:aaaaaaaaaaaaaaaaaaaa -->\n" + untrusted,
            },
        ]
    )

    assert latest["incident_key"] == "source:trusted:reason"
    assert fingerprints == []


def test_apply_validates_all_project_options_before_first_write(monkeypatch: pytest.MonkeyPatch) -> None:
    client = GhMetadataClient()
    client._issues = [{"number": 49, "state": "OPEN", "url": "https://example.test/issues/49"}]
    client._fields = {
        name: {"id": name, "options": [{"id": "ok", "name": option}]}
        for name, option in (
            ("Status", "待办"),
            ("Priority", "P1"),
            ("Task Type", "Bug"),
            ("Change Risk", "Medium"),
            ("Target", "本周"),
            # 故意缺少 Route，必须在任何写入前失败。
        )
    }
    writes: list[list[str]] = []
    monkeypatch.setattr(client, "_run", lambda args: writes.append(args))
    operation = build_sync_plan(_decision(_action()), _official())[0]

    with pytest.raises(SyncError, match="Route"):
        client.apply([operation])
    assert writes == []


def test_apply_write_permission_check_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    client = GhMetadataClient()
    monkeypatch.setattr(
        client,
        "_json",
        lambda _args: {
            "data": {
                "repository": {"viewerPermission": "WRITE"},
                "user": {"projectV2": {"viewerCanUpdate": False}},
            }
        },
    )

    with pytest.raises(SyncError, match="没有仓库和 Project 写权限"):
        client.validate_write_access()


def test_invalid_decision_still_writes_reconstructed_state_before_stopping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    row = _recovery_action("close", recovery_count=2)
    decision = _decision(row, origin="local_cache")
    decision_path = tmp_path / "decision.json"
    out_path = tmp_path / "sync.json"
    state_path = tmp_path / "official.json"
    decision_path.write_text(json.dumps(decision), encoding="utf-8")
    official = _official()

    class FakeClient:
        def preflight(self, **_kwargs: object) -> dict:
            return official

    monkeypatch.setattr("app.health_loop_sync.GhMetadataClient", FakeClient)
    monkeypatch.setattr(
        "sys.argv",
        [
            "health_loop_sync",
            "--decision",
            str(decision_path),
            "--mode",
            "shadow",
            "--out",
            str(out_path),
            "--state-out",
            str(state_path),
        ],
    )

    assert main() == 1
    assert json.loads(state_path.read_text(encoding="utf-8"))["schema_version"] == STATE_SCHEMA
    assert not out_path.exists()
