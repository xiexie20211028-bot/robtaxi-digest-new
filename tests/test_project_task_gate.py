from __future__ import annotations

import importlib.util
import json
from http.client import IncompleteRead
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("project_task_gate", ROOT / "scripts" / "validate_project_task.py")
assert SPEC and SPEC.loader
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def _config() -> dict:
    return json.loads((ROOT / ".github" / "robtaxi-project-governance.json").read_text(encoding="utf-8"))


def _body() -> str:
    return "\n".join(
        [
            "## 问题或目标\n目标。",
            "## 证据与日志\n证据。",
            "## 影响范围\n平台。",
            "## 根因\n不适用。",
            "## 临时措施\n措施。",
            "## 永久方案\n方案。",
            "## 验收标准\n- [ ] 通过校验。",
            "## 影响路线\n平台。",
            "## 依赖与阻塞\n无。",
            "## 关联 PR / Commit\n待创建。",
        ]
    )


def _item(
    *,
    status: str = "开发中",
    blockers: list[dict] | None = None,
    task_type: str = "技术债",
    labels: list[str] | None = None,
    priority: str = "P1",
) -> dict:
    fields: dict[str, object] = {
        "Status": status,
        "Priority": priority,
        "Task Type": task_type,
        "Area": "CI/GitHub",
        "Impact": 4,
        "Urgency": 4,
        "Reach": 5,
        "Recurrence": 4,
        "Effort": "M",
        "Priority Score": 31,
        "Target": "本周",
        "Remedy": "预防",
        "Route": "平台",
    }
    values = []
    for name, value in fields.items():
        entry: dict[str, object] = {"field": {"name": name}}
        entry["number" if isinstance(value, int) else "name"] = value
        values.append(entry)
    return {
        "fieldValues": {"nodes": values},
        "content": {
            "number": 76,
            "state": "OPEN",
            "body": _body(),
            "repository": {"nameWithOwner": "xiexie20211028-bot/robtaxi-digest-new"},
            "assignees": {"totalCount": 1},
            "labels": {"nodes": [{"name": label} for label in (labels or [])]},
            "blockedBy": {"nodes": blockers or []},
        },
    }


def test_valid_preflight_passes() -> None:
    result = gate.validate_item(_item(), _config(), "preflight", expected_issue_number=76)
    assert result == {"issue": 76, "status": "开发中", "priority_score": 31.0, "phase": "preflight"}


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"status": "Inbox"}, "当前 Status"),
        ({"blockers": [{"number": 1, "state": "OPEN"}]}, "Blocked by"),
        ({"task_type": "Epic"}, "Epic"),
        ({"labels": ["robtaxi-health"]}, "Health Issue"),
        ({"priority": "P9"}, "Priority 不合法"),
    ],
)
def test_invalid_execution_context_fails(kwargs: dict, message: str) -> None:
    with pytest.raises(gate.GovernanceError, match=message):
        gate.validate_item(_item(**kwargs), _config(), "preflight")


def test_score_mismatch_fails() -> None:
    item = _item()
    item["fieldValues"]["nodes"][-4]["number"] = 30
    with pytest.raises(gate.GovernanceError, match="Priority Score"):
        gate.validate_item(item, _config(), "preflight")


def test_ready_pr_requires_verification_status() -> None:
    with pytest.raises(gate.GovernanceError, match="PR 当前状态"):
        gate.validate_item(_item(), _config(), "pr", is_draft=False)
    assert gate.validate_item(_item(status="待验证"), _config(), "pr", is_draft=False)["status"] == "待验证"


def test_draft_pr_allows_development_status() -> None:
    assert gate.validate_item(_item(), _config(), "pr", is_draft=True)["status"] == "开发中"


def test_primary_task_parser_rejects_unstructured_text() -> None:
    assert gate.primary_issue_from_pr_body("Primary task: Fixes #76") == 76
    with pytest.raises(gate.GovernanceError, match="Primary task"):
        gate.primary_issue_from_pr_body("Fixes #76")


def test_issue_reference_rejects_ambiguous_text() -> None:
    assert gate.issue_number_from_ref("#76") == 76
    assert gate.issue_number_from_ref("https://github.com/xiexie20211028-bot/robtaxi-digest-new/issues/76") == 76
    with pytest.raises(gate.GovernanceError, match="Issue 编号"):
        gate.issue_number_from_ref("task-76")


def test_postflight_allows_completed_only_with_acceptance_evidence() -> None:
    completed = _item(status="已完成")
    with pytest.raises(gate.GovernanceError, match="验收完成"):
        gate.validate_item(completed, _config(), "postflight")
    completed["content"]["body"] += "\n\n验收完成：用户已确认。"
    assert gate.validate_item(completed, _config(), "postflight")["status"] == "已完成"


def test_fetch_project_item_paginates(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [
        {"data": {"user": {"projectV2": {"items": {"nodes": [], "pageInfo": {"hasNextPage": True, "endCursor": "next"}}}}}},
        {"data": {"user": {"projectV2": {"items": {"nodes": [_item()], "pageInfo": {"hasNextPage": False, "endCursor": None}}}}}},
    ]

    def fake_request(_token: str, variables: dict) -> dict:
        assert variables["owner"] == "xiexie20211028-bot"
        return responses.pop(0)

    monkeypatch.setattr(gate, "request_graphql", fake_request)
    assert gate.fetch_issue_item(_config(), 76, "test-token")["content"]["number"] == 76


def test_project_query_uses_bounded_page_size() -> None:
    assert f"items(first: {gate.PROJECT_PAGE_SIZE}, after: $after)" in gate._graphql_query()


def test_request_graphql_uses_gh_fallback_for_incomplete_response(monkeypatch: pytest.MonkeyPatch) -> None:
    class BrokenResponse:
        def __enter__(self) -> "BrokenResponse":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            raise IncompleteRead(b'{"data":', 10)

    def fake_urlopen(*_args: object, **_kwargs: object) -> BrokenResponse:
        return BrokenResponse()

    captured: dict[str, object] = {}

    def fake_run(*args: object, **kwargs: object) -> SimpleNamespace:
        captured["args"] = args
        captured["env"] = kwargs["env"]
        return SimpleNamespace(returncode=0, stdout='{"data": {"viewer": {"login": "bot"}}}')

    monkeypatch.setattr(gate, "urlopen", fake_urlopen)
    monkeypatch.setattr(gate, "run", fake_run)
    assert gate.request_graphql("test-token", {"owner": "xiexie20211028-bot", "number": 3, "after": None}) == {
        "data": {"viewer": {"login": "bot"}}
    }
    assert captured["args"] == (["gh", "api", "graphql", "--input", "-"],)
    assert captured["env"] == {"PATH": gate.os.environ.get("PATH", ""), "GH_TOKEN": "test-token", "NO_COLOR": "1"}


def test_gh_fallback_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gate, "run", lambda *_args, **_kwargs: SimpleNamespace(returncode=1, stdout=""))
    with pytest.raises(gate.GovernanceError, match="GitHub Project 查询失败"):
        gate.request_graphql_via_gh("test-token", b"{}")
