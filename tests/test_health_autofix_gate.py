from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "health_autofix_gate", ROOT / "scripts" / "validate_health_autofix.py"
)
assert SPEC and SPEC.loader
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def _config() -> dict:
    return {
        "schema_version": "robtaxi-health-autofix-v1",
        "enabled": True,
        "shadow_mode": False,
        "max_auto_merges_per_day": 1,
        "max_changed_files": 3,
        "max_changed_lines": 80,
        "sources": {
            "miit_news_structured": {
                "reason_codes": ["low_date_parse_rate"],
                "allowed_fields": ["url", "date_format", "article_selector"],
                "fixture_path": "tests/fixtures/sources/miit.html",
                "test_files": ["tests/test_fetch.py"],
            }
        },
    }


def _sources() -> dict:
    return {
        "defaults": {"timeout": 15},
        "sources": [
            {"id": "miit_news_structured", "url": "https://a.example", "date_format": "old"},
            {"id": "pony_news_structured", "url": "https://b.example", "date_format": "old"},
        ],
    }


def _event() -> dict:
    return {
        "source_id": "miit_news_structured",
        "reason_code": "low_date_parse_rate",
        "branch": "workbuddy/health-source-miit-low-date",
    }


def test_single_source_allowed_semantic_change_passes() -> None:
    base = _sources()
    head = _sources()
    head["sources"][0]["date_format"] = "new"

    result = gate.validate_patch(
        config=_config(),
        event=_event(),
        base_sha="a" * 40,
        head_sha="b" * 40,
        changed_files=["sources.json", "tests/fixtures/sources/miit.html", "tests/test_fetch.py"],
        base_sources=base,
        head_sources=head,
        changed_lines=25,
    )

    assert result["changed_fields"] == ["date_format"]


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("other_source", "只能修改目标"),
        ("top_level", "顶层共享配置"),
        ("new_source", "增加或删除来源"),
        ("forbidden_field", "字段超出白名单"),
    ],
)
def test_semantic_diff_rejects_shared_or_other_source_changes(mutation: str, message: str) -> None:
    base = _sources()
    head = _sources()
    if mutation == "other_source":
        head["sources"][1]["date_format"] = "new"
    elif mutation == "top_level":
        head["defaults"]["timeout"] = 30
        head["sources"][0]["date_format"] = "new"
    elif mutation == "new_source":
        head["sources"].append({"id": "new_source", "url": "https://new.example"})
    else:
        head["sources"][0]["shared_parser"] = "changed"

    with pytest.raises(gate.AutofixPolicyError, match=message):
        gate.validate_sources_semantic_diff(
            base,
            head,
            source_id="miit_news_structured",
            allowed_fields={"date_format"},
        )


@pytest.mark.parametrize(
    "changed_files",
    [
        ["sources.json", "app/fetch.py"],
        ["sources.json", ".github/robtaxi-health-autofix.json"],
        ["sources.json", "scripts/validate_health_autofix.py"],
        ["sources.json", ".github/workflows/robtaxi-project-governance.yml"],
    ],
)
def test_patch_rejects_shared_code_and_self_modification(changed_files: list[str]) -> None:
    base = _sources()
    head = _sources()
    head["sources"][0]["date_format"] = "new"
    with pytest.raises(gate.AutofixPolicyError, match="白名单外或自修改"):
        gate.validate_patch(
            config=_config(),
            event=_event(),
            base_sha="a",
            head_sha="b",
            changed_files=changed_files,
            base_sources=base,
            head_sources=head,
            changed_lines=10,
        )


def test_disabled_or_missing_policy_fails_closed(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    with pytest.raises(gate.AutofixPolicyError, match="不存在"):
        gate.load_config(missing)

    disabled = tmp_path / "disabled.json"
    disabled.write_text(
        json.dumps({"schema_version": "robtaxi-health-autofix-v1", "enabled": False}), encoding="utf-8"
    )
    with pytest.raises(gate.AutofixPolicyError, match="未启用"):
        gate.load_config(disabled)


def test_pr_event_requires_same_repo_branch_label_and_fixed_markers(tmp_path: Path) -> None:
    body = """Primary task: Refs #49
Autofix incident: source:miit_news_structured:low_date_parse_rate
Autofix source: miit_news_structured
Autofix evidence: fixture; `$(touch /tmp/never)` is data only
Autofix rollback: revert the one source entry
"""
    payload = {
        "pull_request": {
            "body": body,
            "head": {
                "ref": "workbuddy/health-source-miit-low-date",
                "repo": {"full_name": "owner/repo"},
            },
            "base": {"repo": {"full_name": "owner/repo"}},
            "labels": [{"name": "workbuddy-autofix"}],
        }
    }
    event = tmp_path / "event.json"
    event.write_text(json.dumps(payload), encoding="utf-8")
    assert gate.parse_event(event)["source_id"] == "miit_news_structured"

    payload["pull_request"]["head"]["repo"]["full_name"] = "fork/repo"
    event.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(gate.AutofixPolicyError, match="fork"):
        gate.parse_event(event)
