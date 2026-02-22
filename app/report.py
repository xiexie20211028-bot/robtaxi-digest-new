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
        "dedupe_drop_count": 0,
        "summarize_fail_count": 0,
        "feishu_push_status": {"status": "pending", "error": ""},
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
