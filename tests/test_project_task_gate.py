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


def _body(risk: str = "Low") -> str:
    sections = ["## 问题或目标\n目标。", "## 验收标准\n- [ ] 通过校验。"]
    if risk == "Medium":
        sections += ["## 证据\nfixture。", "## 验证与回退\n运行定向测试；失败时回退。"]
    if risk == "High":
        sections += ["## 根因与风险\n共享逻辑风险。", "## 实施方案\n离线回放后 Draft PR。", "## 上线与监控\n获批后合并并观察。"]
    return "\n".join(sections)


def _item(*, status: str = "开发中", risk: str = "Low", blockers: list[dict] | None = None, task_type: str = "技术债", labels: list[str] | None = None) -> dict:
    fields = {"Status": status, "Priority": "P1", "Task Type": task_type, "Change Risk": risk, "Target": "本周", "Route": "平台"}
    return {
        "fieldValues": {"nodes": [{"field": {"name": name}, "name": value} for name, value in fields.items()]},
        "content": {
            "number": 76,
            "state": "OPEN",
            "body": _body(risk),
            "repository": {"nameWithOwner": "xiexie20211028-bot/robtaxi-digest-new"},
            "assignees": {"totalCount": 1},
            "labels": {"nodes": [{"name": label} for label in (labels or [])]},
            "blockedBy": {"nodes": blockers or []},
        },
    }


def test_low_risk_preflight_passes() -> None:
    assert gate.validate_item(_item(), _config(), "preflight", expected_issue_number=76)["risk"] == "Low"


def test_medium_requires_evidence_and_rollback() -> None:
    item = _item(risk="Medium")
    item["content"]["body"] = _body("Low")
    with pytest.raises(gate.GovernanceError, match="证据"):
        gate.validate_item(item, _config(), "preflight")


def test_high_ready_pr_requires_explicit_approval_label() -> None:
    with pytest.raises(gate.GovernanceError, match="high-risk-approved"):
        gate.validate_item(_item(risk="High"), _config(), "pr")
    assert gate.validate_item(_item(risk="High"), _config(), "pr", pr_labels={"high-risk-approved"})["status"] == "开发中"


def test_draft_and_ready_pr_allow_development_status() -> None:
    assert gate.validate_item(_item(risk="Medium"), _config(), "pr", is_draft=True)["status"] == "开发中"
    assert gate.validate_item(_item(risk="Medium"), _config(), "pr", is_draft=False)["status"] == "开发中"


@pytest.mark.parametrize(("kwargs", "message"), [({"status": "Inbox"}, "当前 Status"), ({"blockers": [{"number": 1, "state": "OPEN"}]}, "Blocked by"), ({"task_type": "Epic"}, "Epic"), ({"labels": ["robtaxi-health"]}, "Health Issue")])
def test_invalid_execution_context_fails(kwargs: dict, message: str) -> None:
    with pytest.raises(gate.GovernanceError, match=message):
        gate.validate_item(_item(**kwargs), _config(), "preflight")


def test_primary_task_supports_fix_and_ref() -> None:
    assert gate.primary_task_reference_from_pr_body("Primary task: Fixes #76") == (76, "Fixes")
    assert gate.primary_task_reference_from_pr_body("Primary task: Refs #76") == (76, "Refs")
    with pytest.raises(gate.GovernanceError, match="Primary task"):
        gate.primary_issue_from_pr_body("Refs #76")


def test_postflight_allows_observation_and_completed_evidence() -> None:
    assert gate.validate_item(_item(status="观察中"), _config(), "postflight")["status"] == "观察中"
    completed = _item(status="已完成")
    with pytest.raises(gate.GovernanceError, match="验收完成"):
        gate.validate_item(completed, _config(), "postflight")


def test_fetch_project_item_paginates(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [{"data": {"user": {"projectV2": {"items": {"nodes": [], "pageInfo": {"hasNextPage": True, "endCursor": "next"}}}}}}, {"data": {"user": {"projectV2": {"items": {"nodes": [_item()], "pageInfo": {"hasNextPage": False, "endCursor": None}}}}}}]
    monkeypatch.setattr(gate, "request_graphql", lambda *_args: responses.pop(0))
    assert gate.fetch_issue_item(_config(), 76, "test-token")["content"]["number"] == 76


def test_request_graphql_uses_gh_fallback_for_incomplete_response(monkeypatch: pytest.MonkeyPatch) -> None:
    class BrokenResponse:
        def __enter__(self) -> "BrokenResponse": return self
        def __exit__(self, *_args: object) -> None: return None
        def read(self) -> bytes: raise IncompleteRead(b'{"data":', 10)
    monkeypatch.setattr(gate, "urlopen", lambda *_args, **_kwargs: BrokenResponse())
    monkeypatch.setattr(gate, "run", lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout='{"data": {"viewer": {"login": "bot"}}}'))
    assert gate.request_graphql("test-token", {"owner": "xiexie20211028-bot", "number": 3, "after": None})["data"]["viewer"]["login"] == "bot"
