from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .common import ensure_dir, read_json, write_json


def default_report() -> dict[str, Any]:
    return {
        "run_id": str(uuid.uuid4()),
        "generated_at_utc": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        "stage_status": {
            "fetch": "pending",
            "parse": "pending",
            "filter": "pending",
            "summarize": "pending",
            "render": "pending",
            "notify": "pending",
        },
        "source_stats": [],
        "window_mode": "prev_natural_day",
        "window_start_bj": "",
        "window_end_bj": "",
        "dedupe_drop_count": 0,
        "summarize_fail_count": 0,
        "non_search_fail_count": 0,
        "search_api_missing_key_count": 0,
        "discovery_items_raw_count": 0,
        "discovery_today_raw_count": 0,
        "discovery_items_canonical_count": 0,
        "discovery_today_canonical_count": 0,
        "published_unparseable_count": 0,
        "published_missing_drop_count": 0,
        "not_today_drop_count": 0,
        "source_max_age_drop_count": 0,
        "fast_pass_kept_count": 0,
        "fast_pass_drop_count": 0,
        "stage2_scored_count": 0,
        "stage2_kept_count": 0,
        "candidate_gate_drop_count": 0,
        "brief_count": 0,
        "summary_structured_count": 0,
        "summary_structured_valid_count": 0,
        "summary_structured_invalid_count": 0,
        "summary_retry_count": 0,
        "impact_target_distribution": {},
        "today_kept_count": 0,
        # 兼容字段（本版不再生产使用，保留一个版本便于回溯）。
        "daily_pool_size": 0,
        "baseline_count": 0,
        "baseline_matched_count": 0,
        "baseline_unmatched_count": 0,
        "recall_at_20": 0.0,
        "recall_guard_alert": False,
        "recall_guard_message": "",
        "baseline_unmatched_samples": [],
        "feishu_push_status": {"status": "pending", "error": ""},
        "wecom_push_status": {"status": "pending", "error": ""},
    }


def report_path(report_root: Path, date_text: str) -> Path:
    return report_root / date_text / "run_report.json"


def load_or_init(report_file: Path) -> dict[str, Any]:
    if report_file.exists():
        return read_json(report_file)
    report = default_report()
    save(report_file, report)
    return report


def save(report_file: Path, report: dict[str, Any]) -> None:
    ensure_dir(report_file.parent)
    write_json(report_file, report)


def mark_stage(report_file: Path, stage: str, status: str, **extra: Any) -> dict[str, Any]:
    report = load_or_init(report_file)
    report.setdefault("stage_status", {})[stage] = status
    for key, value in extra.items():
        report[key] = value
    save(report_file, report)
    return report


def patch_report(report_file: Path, **extra: Any) -> dict[str, Any]:
    report = load_or_init(report_file)
    for key, value in extra.items():
        report[key] = value
    save(report_file, report)
    return report
