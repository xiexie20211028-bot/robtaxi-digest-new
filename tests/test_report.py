from __future__ import annotations

import json
from pathlib import Path

from app.report import default_report, load_or_init, mark_stage, patch_report


DEPRECATED_EMPTY_FIELDS = {
    "daily_pool_size",
    "baseline_count",
    "baseline_matched_count",
    "baseline_unmatched_count",
    "baseline_unmatched_samples",
    "recall_at_20",
    "recall_guard_alert",
    "recall_guard_message",
}
FIXTURE = Path(__file__).parent / "fixtures" / "run_report_legacy_compat.json"


def _write_report(path: Path, report: dict) -> None:
    path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")


def test_new_report_stops_producing_deprecated_empty_fields() -> None:
    report = default_report()

    assert DEPRECATED_EMPTY_FIELDS.isdisjoint(report)
    assert report["today_kept_count"] == 0
    assert report["feishu_push_status"]["status"] == "pending"


def test_legacy_report_with_empty_fields_remains_safe_to_update(tmp_path: Path) -> None:
    legacy = json.loads(FIXTURE.read_text(encoding="utf-8"))
    report_file = tmp_path / "run_report.json"
    _write_report(report_file, legacy)

    assert load_or_init(report_file) == legacy
    mark_stage(report_file, "render", "success")
    updated = patch_report(report_file, delivery_marker="unchanged-business-output")

    assert updated["stage_status"]["render"] == "success"
    assert updated["brief_count"] == legacy["brief_count"]
    assert updated["today_kept_count"] == legacy["today_kept_count"]
    assert {field: updated[field] for field in DEPRECATED_EMPTY_FIELDS} == {
        field: legacy[field] for field in DEPRECATED_EMPTY_FIELDS
    }


def test_report_missing_deprecated_fields_is_not_backfilled(tmp_path: Path) -> None:
    legacy = json.loads(FIXTURE.read_text(encoding="utf-8"))
    without_deprecated = {key: value for key, value in legacy.items() if key not in DEPRECATED_EMPTY_FIELDS}
    report_file = tmp_path / "run_report.json"
    _write_report(report_file, without_deprecated)

    mark_stage(report_file, "notify", "success")
    updated = patch_report(report_file, delivery_marker="unchanged-business-output")

    assert DEPRECATED_EMPTY_FIELDS.isdisjoint(updated)
    assert updated["stage_status"]["notify"] == "success"
    assert updated["brief_count"] == without_deprecated["brief_count"]
    assert updated["today_kept_count"] == without_deprecated["today_kept_count"]
