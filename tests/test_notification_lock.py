from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.notification_lock import artifact_name, fetch_lock, has_active_artifact


def test_artifact_name_rejects_invalid_date_and_channel() -> None:
    assert artifact_name("2026-08-14", "feishu") == "robtaxi-notify-lock-2026-08-14-feishu"
    with pytest.raises(ValueError, match="invalid date"):
        artifact_name("2026/08/14", "feishu")
    with pytest.raises(ValueError, match="unsupported channel"):
        artifact_name("2026-08-14", "email")


def test_has_active_artifact_ignores_expired_and_other_names() -> None:
    name = artifact_name("2026-08-14", "wecom")
    assert has_active_artifact({"artifacts": [{"name": name, "expired": False}]}, name) is True
    assert has_active_artifact({"artifacts": [{"name": name, "expired": True}]}, name) is False
    assert has_active_artifact({"artifacts": [{"name": "other", "expired": False}]}, name) is False


def test_fetch_lock_uses_authenticated_github_artifact_api(monkeypatch) -> None:
    captured: dict[str, object] = {}
    expected = artifact_name("2026-08-14", "feishu")

    def fake_get(url, **kwargs):
        captured["url"] = url
        captured["headers"] = kwargs["headers"]
        return json.dumps({"artifacts": [{"name": expected, "expired": False}]}).encode()

    monkeypatch.setattr("app.notification_lock.http_get_bytes", fake_get)
    assert fetch_lock("owner/repo", "secret-token", "2026-08-14", "feishu") is True
    assert f"name={expected}" in str(captured["url"])
    headers = captured["headers"]
    assert isinstance(headers, dict)
    assert headers["Authorization"] == "Bearer secret-token"
    assert headers["X-GitHub-Api-Version"] == "2026-03-10"


def test_production_workflow_uses_artifacts_instead_of_cache_for_notification_locks() -> None:
    workflow = (Path(__file__).parents[1] / ".github/workflows/robtaxi-digest-pages.yml").read_text(encoding="utf-8")
    notify_block = workflow.split("  notify:\n", 1)[1].split("  self_check:\n", 1)[0]
    assert "python -m app.notification_lock" in notify_block
    assert "robtaxi-notify-lock-${{ steps.run_date.outputs.date_bj }}-feishu" in notify_block
    assert "robtaxi-notify-lock-${{ steps.run_date.outputs.date_bj }}-wecom" in notify_block
    assert "retention-days: 35" in notify_block
    assert notify_block.count("include-hidden-files: true") == 2
    assert "actions/cache/restore" not in notify_block
    assert "actions/cache/save" not in notify_block
    assert "steps.push_feishu.outputs.sent == 'true'" in notify_block
    assert "steps.push_wecom.outputs.sent == 'true'" in notify_block
