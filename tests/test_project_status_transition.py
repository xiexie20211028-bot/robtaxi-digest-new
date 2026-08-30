from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location("project_status_transition", ROOT / "scripts" / "transition_project_task.py")
assert SPEC and SPEC.loader
transitioner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(transitioner)


def _config() -> dict:
    return json.loads((ROOT / ".github" / "robtaxi-project-governance.json").read_text(encoding="utf-8"))


def _response(status: str) -> dict:
    return {"data": {"user": {"projectV2": {"id": "project", "fields": {"nodes": [{"id": "status-field", "name": "Status", "options": [{"id": "dev", "name": "开发中"}, {"id": "ready", "name": "待验证"}]}]}, "items": {"nodes": [{"id": "item", "content": {"number": 92}, "fieldValues": {"nodes": [{"name": status, "field": {"name": "Status"}}]}}], "pageInfo": {"hasNextPage": False, "endCursor": None}}}}}}


def test_transition_writes_and_reads_back_target_status(monkeypatch: pytest.MonkeyPatch) -> None:
    state = {"status": "开发中"}

    def fake_graphql(_token: str, query: str, variables: dict) -> dict:
        if query == transitioner.MUTATION:
            assert variables["optionId"] == "ready"
            state["status"] = "待验证"
            return {"data": {"updateProjectV2ItemFieldValue": {"projectV2Item": {"id": "item"}}}}
        return _response(state["status"])

    monkeypatch.setattr(transitioner, "graphql", fake_graphql)
    assert transitioner.transition(_config(), 92, "待验证", "token", retries=2, delay=0) == {
        "issue": 92, "status": "待验证", "attempts": 1, "item_id": "item"
    }


def test_transition_fails_closed_when_readback_never_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(transitioner, "graphql", lambda *_args, **_kwargs: _response("开发中"))
    with pytest.raises(transitioner.GovernanceError, match="未读回 待验证"):
        transitioner.transition(_config(), 92, "待验证", "token", retries=2, delay=0)
