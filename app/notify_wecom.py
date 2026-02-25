from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
from typing import Any

from .common import http_post_json, now_beijing
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


def build_message(date_text: str, html_url: str) -> str:
    lines = [f"Robtaxi 行业简报 {date_text}"]
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
        text = build_message(date_text, args.html_url.strip())

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
