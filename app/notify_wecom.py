from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
from typing import Any

from .common import http_post_json, now_beijing, read_json, read_jsonl
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


def build_message(date_text: str, html_url: str, report: dict[str, Any], items: list[dict[str, Any]]) -> str:
    window_start_bj = str(report.get("window_start_bj", "")).strip()
    window_end_bj = str(report.get("window_end_bj", "")).strip()
    stat_date = window_start_bj.split(" ")[0] if window_start_bj else date_text
    lines = [f"Robtaxi 行业简报（统计日）{stat_date}"]
    if window_start_bj and window_end_bj:
        lines.extend(["", f"统计窗口（北京时间）：{window_start_bj} ~ {window_end_bj}"])
    for idx, item in enumerate(items[:5], 1):
        title = str(item.get("title_zh", "")).strip()
        link = str(item.get("link", "")).strip()
        so_what = str(item.get("summary_so_what", "")).strip()
        if not so_what:
            legacy_summary = str(item.get("summary_zh", "")).strip()
            so_what = legacy_summary.split("。")[0].strip()
            if so_what:
                so_what += "。"
        impact_targets = [str(x).strip() for x in item.get("impact_targets", []) if str(x).strip()]
        impact_text = " / ".join(impact_targets) if impact_targets else "未标注"
        lines.extend(["", f"{idx}. {title}"])
        if so_what:
            lines.append(f"So what：{so_what}")
        lines.append(f"影响对象：{impact_text}")
        if link:
            lines.append(link)
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
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "brief_items.jsonl"
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)
    webhook_url = os.environ.get("WECOM_WEBHOOK_URL", "").strip()

    if args.text.strip():
        text = args.text.strip()
    else:
        report = read_json(report_file) if report_file.exists() else {}
        items = read_jsonl(in_file)
        text = build_message(date_text, args.html_url.strip(), report, items)

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
