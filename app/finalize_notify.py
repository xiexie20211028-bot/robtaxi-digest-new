from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from .report import mark_stage, patch_report, report_path

SUCCESS_STATUSES = {"success", "already_sent"}
REPORTED_SUCCESS_STATUSES = {"sent", "sent_webhook", "success", "already_sent"}


def aggregate_notify_status(feishu_status: str, wecom_status: str, expected: bool) -> str:
    if not expected:
        return "skipped"
    success_count = sum(status in SUCCESS_STATUSES for status in (feishu_status, wecom_status))
    if success_count == 2:
        return "success"
    if success_count == 1:
        return "partial"
    return "failed"


def normalize_channel_status(status: str, current: Any, expected: bool) -> str:
    if status != "reported":
        return status
    current_status = str(current.get("status", "")) if isinstance(current, dict) else ""
    if current_status in REPORTED_SUCCESS_STATUSES:
        return "success" if current_status != "already_sent" else "already_sent"
    if current_status in {"notify_failed", "failed"}:
        return "failed"
    if current_status == "skipped":
        return "failed" if expected else "skipped"
    return "failed" if expected else "skipped"


def _channel_payload(status: str, current: Any) -> dict[str, Any]:
    payload = dict(current) if isinstance(current, dict) else {}
    payload["final_status"] = status
    if status == "already_sent":
        payload.update({"status": "already_sent", "error": ""})
    elif status == "skipped" and str(payload.get("status", "")) in {"", "pending"}:
        payload.update({"status": "skipped", "error": ""})
    elif status == "failed" and str(payload.get("status", "")) in {"", "pending"}:
        payload["status"] = "failed"
    return payload


def _write_github_output(path: str, values: dict[str, str]) -> None:
    if not path:
        return
    with Path(path).open("a", encoding="utf-8") as handle:
        for key, value in values.items():
            handle.write(f"{key}={value}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Aggregate Feishu and WeCom notification results")
    parser.add_argument("--date", required=True)
    parser.add_argument("--report", default="./artifacts/reports")
    parser.add_argument("--feishu-status", required=True)
    parser.add_argument("--wecom-status", required=True)
    parser.add_argument("--expected", action="store_true")
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT", ""))
    args = parser.parse_args()

    report_file = report_path(Path(args.report).expanduser().resolve(), args.date)
    report = mark_stage(report_file, "notify", "pending")
    feishu_status = normalize_channel_status(
        args.feishu_status,
        report.get("feishu_push_status"),
        bool(args.expected),
    )
    wecom_status = normalize_channel_status(
        args.wecom_status,
        report.get("wecom_push_status"),
        bool(args.expected),
    )
    aggregate = aggregate_notify_status(feishu_status, wecom_status, bool(args.expected))
    report = mark_stage(report_file, "notify", aggregate)
    patch_report(
        report_file,
        feishu_push_status=_channel_payload(feishu_status, report.get("feishu_push_status")),
        wecom_push_status=_channel_payload(wecom_status, report.get("wecom_push_status")),
    )
    _write_github_output(
        args.github_output,
        {
            "notify_status": aggregate,
            "feishu_status": feishu_status,
            "wecom_status": wecom_status,
        },
    )
    print(
        f"[finalize_notify] aggregate={aggregate} "
        f"feishu={feishu_status} wecom={wecom_status}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
