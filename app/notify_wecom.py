from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
from typing import Any

from .common import http_post_json, now_beijing, read_json
from .report import mark_stage, patch_report, report_path


def _extract_wecom_code(resp: dict[str, Any]) -> int:
    """兼容企业微信常见返回结构。"""
    for key in ("errcode", "code"):
        if key in resp:
            try:
                return int(resp.get(key, -1))
            except (TypeError, ValueError):
                return -1
    return -1


def send_webhook(webhook_url: str, text: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "msgtype": "text",
        "text": {
            "content": text,
        },
    }
    resp = http_post_json(webhook_url, payload, timeout=20, retries=1)
    code = _extract_wecom_code(resp)
    if code != 0:
        errmsg = str(resp.get("errmsg", "")).strip() or str(resp.get("msg", "")).strip()
        raise RuntimeError(f"send wecom webhook failed: errcode={code}, errmsg={errmsg}")
    return resp


def _recall_alert_line(report: dict[str, Any]) -> str:
    if not bool(report.get("recall_guard_alert", False)):
        return ""
    recall = float(report.get("recall_at_20", 0.0))
    baseline = int(report.get("baseline_count", 0))
    matched = int(report.get("baseline_matched_count", 0))
    return f"覆盖率告警：recall@20={recall:.2f}，对账基线={baseline}，已命中={matched}"


def build_message(date_text: str, html_url: str, report: dict[str, Any]) -> str:
    lines = [f"Robtaxi 行业简报 {date_text}"]
    alert_line = _recall_alert_line(report)
    if alert_line:
        lines.extend(["", alert_line])
    if html_url.strip():
        lines.extend(["", f"完整网页：{html_url.strip()}"])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Send digest to WeCom bot (webhook)")
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD; default Beijing date")
    parser.add_argument("--html-url", default="", help="Published HTML URL")
    parser.add_argument("--in", dest="in_root", default="./artifacts/brief", help="Brief input root (reserved)")
    parser.add_argument("--text", default="", help="Send plain text instead of digest")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root")
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)
    webhook_url = os.environ.get("WECOM_WEBHOOK_URL", "").strip()

    if args.text.strip():
        text = args.text.strip()
    else:
        report = read_json(report_file) if report_file.exists() else {}
        text = build_message(date_text, args.html_url.strip(), report)

    run_id = os.environ.get("GITHUB_RUN_ID", "").strip()
    run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "").strip() or "1"
    if run_id:
        uuid_seed = f"{date_text}|{run_id}|{run_attempt}|{args.html_url.strip()}|{bool(args.text.strip())}"
    else:
        uuid_seed = f"{date_text}|{text}|{args.html_url.strip()}"
    message_uuid = hashlib.sha1(uuid_seed.encode("utf-8", errors="ignore")).hexdigest()

    if not webhook_url:
        mark_stage(report_file, "notify", "failed")
        patch_report(
            report_file,
            wecom_push_status={
                "status": "notify_failed",
                "error": "missing WECOM_WEBHOOK_URL",
                "message_uuid": message_uuid,
            },
        )
        print("[notify_wecom] failed: missing WECOM_WEBHOOK_URL")
        return 1

    try:
        resp = send_webhook(webhook_url, text)
        mark_stage(report_file, "notify", "success")
        patch_report(
            report_file,
            wecom_push_status={
                "status": "sent",
                "error": "",
                "message_uuid": message_uuid,
                "resp": resp,
            },
        )
        print(f"[notify_wecom] sent uuid={message_uuid}")
        return 0
    except Exception as exc:
        mark_stage(report_file, "notify", "failed")
        patch_report(
            report_file,
            wecom_push_status={
                "status": "notify_failed",
                "error": str(exc)[:500],
                "message_uuid": message_uuid,
            },
        )
        print(f"[notify_wecom] failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
