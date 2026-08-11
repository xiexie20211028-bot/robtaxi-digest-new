from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.common import write_json, write_jsonl
from app.finalize_notify import _channel_payload, aggregate_notify_status, normalize_channel_status
from app.health_issue import (
    build_incidents,
    build_issue_body,
    find_issue_for_incident,
    find_issue_for_run,
    run_marker,
    stale_pending_issues,
    sync_health_issue,
)
from app.report import default_report, empty_stage_funnel
from app.self_check import (
    build_health_report,
    classify_source_failure_rate,
    classify_summary_fallback_rate,
    write_internal_failure_report,
)


def _write_complete_fixture(
    root: Path,
    date_text: str = "2026-07-24",
    *,
    raw_rows: list[dict] | None = None,
) -> dict:
    report = default_report()
    report["stage_status"] = {
        "fetch": "success",
        "parse": "success",
        "filter": "success_empty",
        "enrich": "success",
        "summarize": "success",
        "editorial_digest": "success",
        "render": "success",
        "notify": "success",
    }
    report.update(
        {
            "window_start_bj": "2026-07-23 00:00:00",
            "window_end_bj": "2026-07-24 00:00:00",
            "source_stats": [
                {
                    "source_id": "required",
                    "source_type": "rss",
                    "status": "ok",
                    "fetched_items": 0,
                    "error_reason_code": "",
                },
                {
                    "source_id": "optional_search",
                    "source_type": "search_api",
                    "status": "fail",
                    "fetched_items": 0,
                    "error_reason_code": "search_api_missing_key",
                },
            ],
            "stage_funnel": empty_stage_funnel(),
            "total_items_raw": len(raw_rows or []),
            "total_items_canonical": 0,
            "relevance_total_in": 0,
            "relevance_kept": 0,
            "relevance_dropped": 0,
            "enrich_total": 0,
            "brief_count": 0,
            "summary_structured_count": 0,
            "summarize_fail_count": 0,
            "editorial_digest_fallback_used": False,
        }
    )
    write_json(root / "artifacts" / "reports" / date_text / "run_report.json", report)
    write_jsonl(root / "artifacts" / "raw" / date_text / "raw_items.jsonl", raw_rows or [])
    write_jsonl(root / "artifacts" / "canonical" / date_text / "canonical_items.jsonl", [])
    write_jsonl(root / "artifacts" / "filtered" / date_text / "filtered_items.jsonl", [])
    write_jsonl(root / "artifacts" / "filtered" / date_text / "dropped_items.jsonl", [])
    write_jsonl(root / "artifacts" / "enriched" / date_text / "enriched_items.jsonl", [])
    write_jsonl(root / "artifacts" / "brief" / date_text / "brief_items.jsonl", [])
    write_json(root / "artifacts" / "digest" / date_text / "daily_digest.json", {"headline": "无新增"})
    site = root / "site" / "index.html"
    site.parent.mkdir(parents=True, exist_ok=True)
    site.write_text("<html>ok</html>", encoding="utf-8")
    return report


def _build(root: Path, config: dict | None = None, **overrides: object) -> tuple[dict, dict | None]:
    args = {
        "date_text": "2026-07-24",
        "workspace": root,
        "config": config or {"defaults": {}},
        "build_status": "success",
        "test_status": "success",
        "deploy_status": "success",
        "notify_job_status": "success",
        "notify_expected": True,
        "feishu_status": "success",
        "wecom_status": "already_sent",
        "github_run_id": "123",
        "github_run_attempt": "1",
        "commit_sha": "a" * 40,
        "repository": "owner/repo",
        "run_url": "https://github.com/owner/repo/actions/runs/123",
        "event_name": "schedule",
    }
    args.update(overrides)
    return build_health_report(**args)


@pytest.mark.parametrize(
    ("rate", "expected"),
    [
        (0.0, "healthy"),
        (0.01, "warning"),
        (0.2999, "warning"),
        (0.30, "error"),
        (0.5999, "error"),
        (0.60, "critical"),
        (1.0, "critical"),
    ],
)
def test_source_failure_boundaries(rate: float, expected: str) -> None:
    assert classify_source_failure_rate(rate) == expected


@pytest.mark.parametrize(
    ("rate", "expected"),
    [(0.0, "healthy"), (0.01, "warning"), (0.1999, "warning"), (0.20, "error"), (1.0, "error")],
)
def test_summary_fallback_boundaries(rate: float, expected: str) -> None:
    assert classify_summary_fallback_rate(rate) == expected


@pytest.mark.parametrize(
    ("feishu", "wecom", "expected_flag", "aggregate"),
    [
        ("success", "success", True, "success"),
        ("already_sent", "success", True, "success"),
        ("failed", "success", True, "partial"),
        ("skipped", "failed", True, "failed"),
        ("skipped", "skipped", False, "skipped"),
    ],
)
def test_notify_aggregate(feishu: str, wecom: str, expected_flag: bool, aggregate: str) -> None:
    assert aggregate_notify_status(feishu, wecom, expected_flag) == aggregate


def test_notify_reported_status_normalization() -> None:
    assert normalize_channel_status("reported", {"status": "sent_webhook"}, True) == "success"
    assert normalize_channel_status("reported", {"status": "skipped"}, True) == "failed"
    assert _channel_payload("failed", {"status": "pending", "error": ""})["status"] == "failed"
    assert _channel_payload("success", {"status": "sent_webhook", "error": ""})["final_status"] == "success"


def test_healthy_empty_day_and_optional_search_key(tmp_path: Path) -> None:
    _write_complete_fixture(tmp_path)
    health, repair = _build(tmp_path)
    assert health["overall_status"] == "healthy"
    assert health["codex_required"] is False
    assert health["metrics"]["optional_search_api_missing_count"] == 1
    assert repair is None


def test_no_items_editorial_fallback_is_healthy_on_empty_day(tmp_path: Path) -> None:
    report = _write_complete_fixture(tmp_path)
    report["editorial_digest_fallback_used"] = True
    report["editorial_digest_fallback_reason"] = "no_items"
    write_json(tmp_path / "artifacts" / "reports" / "2026-07-24" / "run_report.json", report)

    health, repair = _build(tmp_path)

    assert health["overall_status"] == "healthy"
    assert repair is None
    assert not any(item["check_id"] == "editorial_digest_fallback" for item in health["findings"])


def test_no_items_does_not_hide_empty_brief_after_kept_items(tmp_path: Path) -> None:
    report = _write_complete_fixture(tmp_path)
    report["relevance_total_in"] = 1
    report["relevance_kept"] = 1
    report["relevance_dropped"] = 0
    report["editorial_digest_fallback_used"] = True
    report["editorial_digest_fallback_reason"] = "no_items"
    write_json(tmp_path / "artifacts" / "reports" / "2026-07-24" / "run_report.json", report)

    health, _repair = _build(tmp_path)

    assert health["overall_status"] == "error"
    assert any(item["check_id"] == "brief_empty_after_kept" for item in health["findings"])


def test_required_source_warning(tmp_path: Path) -> None:
    report = _write_complete_fixture(tmp_path)
    report["source_stats"] = [
        {"source_id": f"s{i}", "source_type": "rss", "status": "fail" if i == 0 else "ok"}
        for i in range(4)
    ]
    write_json(tmp_path / "artifacts" / "reports" / "2026-07-24" / "run_report.json", report)
    health, repair = _build(tmp_path)
    assert health["overall_status"] == "warning"
    assert health["codex_required"] is True
    assert repair is not None


def test_required_source_evidence_is_actionable_and_redacted(tmp_path: Path) -> None:
    report = _write_complete_fixture(tmp_path)
    report["source_stats"] = [
        {
            "source_id": "uber_news_structured",
            "source_type": "structured_web",
            "status": "partial",
            "fetched_items": 7,
            "error_reason_code": "http_not_acceptable",
            "error_reason_zh": "目标页面不接受当前请求（HTTP 406）",
            "error_raw": "HTTP 406 api_key=top-secret",
        }
    ]
    write_json(tmp_path / "artifacts" / "reports" / "2026-07-24" / "run_report.json", report)

    health, repair = _build(tmp_path)

    assert repair is not None
    failed = repair["findings"][0]["evidence"]["failed_sources"][0]
    assert failed["fetched_items"] == 7
    assert failed["reason_code"] == "http_not_acceptable"
    assert "top-secret" not in failed["error_detail"]
    assert "[REDACTED]" in failed["error_detail"]


def test_required_source_evidence_keeps_every_failed_source(tmp_path: Path) -> None:
    report = _write_complete_fixture(tmp_path)
    report["source_stats"] = [
        {
            "source_id": f"source_{index}",
            "source_type": "rss",
            "status": "fail",
            "error_reason_code": "invalid_xml",
            "error_reason_zh": "RSS XML 无法解析",
        }
        for index in range(21)
    ]
    write_json(tmp_path / "artifacts" / "reports" / "2026-07-24" / "run_report.json", report)

    health, _repair = _build(tmp_path)

    finding = next(item for item in health["findings"] if item["check_id"] == "required_source_failure_rate")
    assert len(finding["evidence"]["failed_sources"]) == 21


def test_count_mismatch_is_error(tmp_path: Path) -> None:
    _write_complete_fixture(tmp_path, raw_rows=[{"id": "1"}])
    report_path = tmp_path / "artifacts" / "reports" / "2026-07-24" / "run_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["total_items_raw"] = 0
    write_json(report_path, report)
    health, _repair = _build(tmp_path)
    assert health["overall_status"] == "error"
    assert any(item["check_id"] == "artifact_raw_items_count" for item in health["findings"])


def test_missing_report_is_critical(tmp_path: Path) -> None:
    health, repair = _build(tmp_path)
    assert health["overall_status"] == "critical"
    assert repair is not None
    assert health["findings"][0]["check_id"] == "run_report"


def test_wrong_window_and_broken_funnel_are_reported(tmp_path: Path) -> None:
    report = _write_complete_fixture(tmp_path)
    report["window_start_bj"] = "2026-07-22 00:00:00"
    report["stage_funnel"]["rss"] = {"fetched": 2, "candidate": 2, "filtered": 0, "kept": 1}
    write_json(tmp_path / "artifacts" / "reports" / "2026-07-24" / "run_report.json", report)
    health, _repair = _build(tmp_path)
    assert health["overall_status"] == "critical"
    check_ids = {item["check_id"] for item in health["findings"]}
    assert {"beijing_window", "funnel_rss"} <= check_ids


def test_notification_severity(tmp_path: Path) -> None:
    _write_complete_fixture(tmp_path)
    partial, _ = _build(tmp_path, feishu_status="failed", wecom_status="success")
    assert partial["overall_status"] == "error"
    failed, _ = _build(tmp_path, feishu_status="failed", wecom_status="failed")
    assert failed["overall_status"] == "critical"


def test_notify_job_failure_is_visible_even_when_channels_report_success(tmp_path: Path) -> None:
    _write_complete_fixture(tmp_path)
    health, _ = _build(tmp_path, notify_job_status="failure")
    assert health["overall_status"] == "error"
    assert any(item["check_id"] == "github_notify_job" for item in health["findings"])


def test_sensitive_evidence_is_redacted(tmp_path: Path) -> None:
    report = _write_complete_fixture(tmp_path)
    report["editorial_digest_fallback_used"] = True
    report["editorial_digest_fallback_reason"] = "api_key=top-secret"
    write_json(tmp_path / "artifacts" / "reports" / "2026-07-24" / "run_report.json", report)
    health, _repair = _build(tmp_path)
    payload = json.dumps(health, ensure_ascii=False)
    assert "top-secret" not in payload
    assert "[REDACTED]" in payload


def test_issue_dedup_marker_and_body() -> None:
    health = {
        "request_id": "rh_123_2",
        "date_bj": "2026-07-24",
        "overall_status": "warning",
        "run": {
            "github_run_id": "123",
            "github_run_attempt": "2",
            "commit_sha": "abc",
            "run_url": "https://example.test/run",
        },
        "findings": [],
    }
    body = build_issue_body(health, {"request_id": "rh_123_2"}, "health-artifact")
    issues = [{"number": 7, "body": body}, {"number": 8, "body": "other"}]
    assert run_marker("123") in body
    assert find_issue_for_run(issues, "123")["number"] == 7


def test_stale_pending_issue_detection() -> None:
    now = datetime(2026, 7, 24, 4, 0, tzinfo=timezone.utc)
    issues = [
        {
            "number": 1,
            "state": "open",
            "created_at": (now - timedelta(hours=25)).isoformat(),
            "labels": [{"name": "proposal-pending"}],
        },
        {
            "number": 2,
            "state": "open",
            "created_at": (now - timedelta(hours=2)).isoformat(),
            "labels": [{"name": "proposal-pending"}],
        },
    ]
    assert [item["number"] for item in stale_pending_issues(issues, now_utc=now)] == [1]


class _FakeGitHubClient:
    def __init__(self, issues: list[dict] | None = None) -> None:
        self.issues = issues or []
        self.created: list[dict] = []
        self.updated: list[tuple[int, dict]] = []
        self.comments: list[tuple[int, str]] = []

    def ensure_labels(self) -> None:
        return None

    def list_health_issues(self) -> list[dict]:
        return self.issues

    def create_issue(self, title: str, body: str, labels: list[str]) -> dict:
        number = 99 + len(self.created)
        issue = {
            "number": number,
            "title": title,
            "body": body,
            "labels": [{"name": item} for item in labels],
            "state": "open",
            "html_url": f"https://example.test/issues/{number}",
        }
        self.created.append(issue)
        self.issues.append(issue)
        return issue

    def update_issue(self, number: int, **payload: object) -> dict:
        self.updated.append((number, payload))
        issue = next(item for item in self.issues if item["number"] == number)
        issue.update(payload)
        if isinstance(payload.get("labels"), list):
            issue["labels"] = [{"name": item} for item in payload["labels"]]
        return issue

    def add_comment(self, number: int, body: str) -> dict:
        self.comments.append((number, body))
        return {"id": len(self.comments)}


def _source_health(date_text: str, request_id: str, run_id: str, sources: list[dict]) -> dict:
    return {
        "request_id": request_id,
        "date_bj": date_text,
        "overall_status": "warning",
        "run": {
            "github_run_id": run_id,
            "commit_sha": "abc",
            "run_url": "https://example.test/run",
        },
        "source_report": {"available": True},
        "findings": [
            {
                "check_id": "required_source_failure_rate",
                "severity": "warning",
                "summary": "必需数据源存在抓取失败",
                "evidence": {"failed_sources": sources},
            }
        ],
    }


def test_incident_id_is_stable_for_same_source_and_reason() -> None:
    source = {"source_id": "ifanr", "reason_code": "invalid_xml", "status": "fail"}
    first = build_incidents(_source_health("2026-07-24", "rh_1_1", "1", [source]))[0]
    second = build_incidents(_source_health("2026-07-25", "rh_2_1", "2", [source]))[0]
    assert first["incident_id"] == second["incident_id"]
    assert first["incident_id"].startswith("ri_")


def test_issue_same_incident_and_run_is_updated_without_increment() -> None:
    source = {"source_id": "ifanr", "reason_code": "invalid_xml", "status": "fail"}
    health = _source_health("2026-07-24", "rh_123_2", "123", [source])
    incident = build_incidents(health)[0]
    body = build_issue_body(
        health,
        {"request_id": "rh_123_2"},
        "health-artifact",
        incident=incident,
        first_seen="2026-07-24",
        occurrence_count=1,
    )
    existing = {
        "number": 7,
        "body": body,
        "state": "open",
        "labels": [{"name": "proposal-ready"}],
        "html_url": "https://example.test/issues/7",
    }
    client = _FakeGitHubClient([existing])
    result = sync_health_issue(
        client=client,  # type: ignore[arg-type]
        health=health,
        repair={"request_id": "rh_123_2"},
        artifact_name="health-artifact",
    )
    assert client.created == []
    assert client.updated[0][0] == 7
    assert "<!-- robtaxi-health-occurrence-count:1 -->" in client.updated[0][1]["body"]
    assert client.updated[0][1]["labels"] == [
        "robtaxi-health",
        "health-warning",
        "proposal-pending",
    ]
    assert result["issue_number"] == 7
    assert result["issue_numbers"] == [7]


def test_same_run_new_attempt_does_not_increment_occurrence_count() -> None:
    source = {"source_id": "ifanr", "reason_code": "invalid_xml", "status": "fail"}
    first_health = _source_health("2026-07-24", "rh_123_1", "123", [source])
    incident = build_incidents(first_health)[0]
    existing = {
        "number": 7,
        "body": build_issue_body(
            first_health,
            {"request_id": "rh_123_1"},
            "health-artifact",
            incident=incident,
            occurrence_count=1,
        ),
        "state": "open",
        "labels": [{"name": "proposal-pending"}],
        "html_url": "https://example.test/issues/7",
    }
    client = _FakeGitHubClient([existing])
    retry_health = _source_health("2026-07-24", "rh_123_2", "123", [source])

    sync_health_issue(
        client=client,  # type: ignore[arg-type]
        health=retry_health,
        repair={"request_id": "rh_123_2"},
        artifact_name="health-artifact",
    )

    assert "<!-- robtaxi-health-occurrence-count:1 -->" in existing["body"]


def test_same_incident_across_days_reuses_issue_and_increments_count() -> None:
    source = {"source_id": "freightwaves", "reason_code": "invalid_xml", "status": "fail"}
    first_health = _source_health("2026-07-24", "rh_123_1", "123", [source])
    incident = build_incidents(first_health)[0]
    existing = {
        "number": 7,
        "body": build_issue_body(
            first_health,
            {"request_id": "rh_123_1"},
            "health-artifact",
            incident=incident,
            first_seen="2026-07-24",
            occurrence_count=1,
        ),
        "state": "open",
        "labels": [{"name": "proposal-pending"}],
        "html_url": "https://example.test/issues/7",
    }
    client = _FakeGitHubClient([existing])
    second_health = _source_health("2026-07-25", "rh_124_1", "124", [source])

    result = sync_health_issue(
        client=client,  # type: ignore[arg-type]
        health=second_health,
        repair={"request_id": "rh_124_1"},
        artifact_name="health-artifact",
    )

    assert client.created == []
    assert result["issue_numbers"] == [7]
    assert "<!-- robtaxi-health-occurrence-count:2 -->" in existing["body"]
    assert "<!-- robtaxi-health-first-seen:2026-07-24 -->" in existing["body"]


def test_different_sources_create_separate_incident_issues() -> None:
    sources = [
        {"source_id": "ifanr", "reason_code": "invalid_xml", "status": "fail"},
        {"source_id": "uber_news_structured", "reason_code": "http_not_acceptable", "status": "partial"},
    ]
    client = _FakeGitHubClient()

    result = sync_health_issue(
        client=client,  # type: ignore[arg-type]
        health=_source_health("2026-07-24", "rh_123_1", "123", sources),
        repair={"request_id": "rh_123_1"},
        artifact_name="health-artifact",
    )

    assert len(client.created) == 2
    assert result["issue_numbers"] == [99, 100]
    built = build_incidents(_source_health("2026-07-24", "rh_123_1", "123", sources))
    incident_ids = {item["incident_id"] for item in built}
    assert all(find_issue_for_incident(client.issues, item) is not None for item in incident_ids)


def test_reason_change_creates_new_incident_and_closes_old_one() -> None:
    previous_health = _source_health(
        "2026-07-23",
        "rh_122_1",
        "122",
        [{"source_id": "ifanr", "reason_code": "invalid_xml", "status": "fail"}],
    )
    old_incident = build_incidents(previous_health)[0]
    existing = {
        "number": 7,
        "body": build_issue_body(previous_health, None, "health-artifact", incident=old_incident),
        "state": "open",
        "labels": [{"name": "proposal-pending"}],
        "html_url": "https://example.test/issues/7",
    }
    current_health = _source_health(
        "2026-07-24",
        "rh_123_1",
        "123",
        [{"source_id": "ifanr", "reason_code": "access_forbidden", "status": "fail"}],
    )
    client = _FakeGitHubClient([existing])

    result = sync_health_issue(
        client=client,
        health=current_health,
        repair={"request_id": "rh_123_1"},
        artifact_name="health-artifact",
    )  # type: ignore[arg-type]

    assert result["issue_numbers"] == [99]
    assert existing["state"] == "closed"
    assert client.comments[0][0] == 7


def test_healthy_run_closes_existing_incident_as_recovered() -> None:
    previous_health = _source_health(
        "2026-07-23",
        "rh_122_1",
        "122",
        [{"source_id": "ifanr", "reason_code": "invalid_xml", "status": "fail"}],
    )
    incident = build_incidents(previous_health)[0]
    existing = {
        "number": 7,
        "body": build_issue_body(
            previous_health,
            {"request_id": "rh_122_1"},
            "health-artifact",
            incident=incident,
        ),
        "state": "open",
        "labels": [{"name": "proposal-pending"}],
        "html_url": "https://example.test/issues/7",
    }
    client = _FakeGitHubClient([existing])
    health = {
        "request_id": "rh_123_2",
        "date_bj": "2026-07-24",
        "overall_status": "healthy",
        "run": {"github_run_id": "123"},
        "source_report": {"available": True},
        "findings": [],
    }
    result = sync_health_issue(
        client=client,  # type: ignore[arg-type]
        health=health,
        repair=None,
        artifact_name="health-artifact",
    )
    assert client.comments == [(7, "最新有效自检未再发现该事件，按来源级健康规则标记恢复：`rh_123_2`。")]
    assert client.updated[0] == (
        7,
        {"state": "closed", "labels": ["robtaxi-health", "health-recovered"]},
    )
    assert result["issue_number"] == 0
    assert result["issue_numbers"] == []


def test_internal_failure_does_not_close_existing_source_incident() -> None:
    previous_health = _source_health(
        "2026-07-23",
        "rh_122_1",
        "122",
        [{"source_id": "ifanr", "reason_code": "invalid_xml", "status": "fail"}],
    )
    incident = build_incidents(previous_health)[0]
    existing = {
        "number": 7,
        "body": build_issue_body(previous_health, None, "health-artifact", incident=incident),
        "state": "open",
        "labels": [{"name": "proposal-pending"}],
        "html_url": "https://example.test/issues/7",
    }
    health = {
        "request_id": "rh_123_1",
        "date_bj": "2026-07-24",
        "overall_status": "critical",
        "run": {"github_run_id": "123"},
        "source_report": {"available": False},
        "findings": [
            {
                "check_id": "self_check_internal_failure",
                "severity": "critical",
                "summary": "自检程序自身失败",
                "evidence": {},
            }
        ],
    }
    client = _FakeGitHubClient([existing])

    sync_health_issue(client=client, health=health, repair=None, artifact_name="health-artifact")  # type: ignore[arg-type]

    assert existing["state"] == "open"
    assert not any(number == 7 and payload.get("state") == "closed" for number, payload in client.updated)


def test_internal_self_check_failure_still_writes_critical_artifacts(tmp_path: Path) -> None:
    args = type(
        "Args",
        (),
        {
            "github_run_attempt": "2",
            "github_run_id": "456",
            "date": "2026-07-24",
            "commit_sha": "abc",
            "repository": "owner/repo",
            "run_url": "https://example.test/run",
            "event_name": "schedule",
            "build_status": "success",
            "test_status": "success",
            "deploy_status": "success",
            "notify_job_status": "success",
            "notify_expected": True,
            "feishu_status": "success",
            "wecom_status": "success",
            "out": str(tmp_path / "health"),
            "github_output": str(tmp_path / "github_output.txt"),
        },
    )()
    out_dir = write_internal_failure_report(args, RuntimeError("token=secret-value"))
    health = json.loads((out_dir / "health_report.json").read_text(encoding="utf-8"))
    repair = json.loads((out_dir / "repair_request.json").read_text(encoding="utf-8"))
    assert health["overall_status"] == "critical"
    assert health["request_id"] == "rh_456_2"
    assert repair["request_id"] == "rh_456_2"
    assert "secret-value" not in json.dumps(health)
    assert "overall_status=critical" in (tmp_path / "github_output.txt").read_text(encoding="utf-8")
