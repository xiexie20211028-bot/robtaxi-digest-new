from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from .common import http_post_json, now_beijing, read_jsonl
from .report import mark_stage, patch_report, report_path


def _extract_feishu_code(resp: dict[str, Any]) -> int:
    """兼容飞书应用与自定义机器人两种响应字段。"""
    for key in ("code", "StatusCode"):
        if key in resp:
            try:
                return int(resp.get(key, -1))
            except (TypeError, ValueError):
                return -1
    return -1


def fetch_tenant_token(app_id: str, app_secret: str) -> str:
    endpoint = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    body = {"app_id": app_id, "app_secret": app_secret}
    data = http_post_json(endpoint, body, timeout=20, retries=3)
    code = int(data.get("code", -1))
    if code != 0:
        raise RuntimeError(f"fetch tenant token failed: code={code}, msg={data.get('msg', '')}")
    token = str(data.get("tenant_access_token", "")).strip()
    if not token:
        raise RuntimeError("empty tenant_access_token")
    return token


def send_message(token: str, receive_open_id: str, text: str, message_uuid: str = "") -> dict[str, Any]:
    query = urlencode({"receive_id_type": "open_id"})
    endpoint = f"https://open.feishu.cn/open-apis/im/v1/messages?{query}"
    body = {
        "receive_id": receive_open_id,
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),
    }
    if message_uuid:
        body["uuid"] = message_uuid
    data = http_post_json(
        endpoint,
        body,
        headers={"Authorization": f"Bearer {token}"},
        timeout=20,
        retries=1,
    )
    code = _extract_feishu_code(data)
    if code != 0:
        raise RuntimeError(f"send message failed: code={code}, msg={data.get('msg', '')}")
    return data


def _feishu_webhook_sign(secret: str, timestamp: str) -> str:
    # Feishu custom bot signature:
    # sign = base64(hmac_sha256(secret, f"{timestamp}\n{secret}"))
    msg = f"{timestamp}\n{secret}".encode("utf-8")
    mac = hmac.new(secret.encode("utf-8"), msg, digestmod=hashlib.sha256).digest()
    return base64.b64encode(mac).decode("utf-8")


def send_webhook(webhook_url: str, webhook_secret: str, text: str, message_uuid: str = "") -> dict[str, Any]:
    payload: dict[str, Any] = {"msg_type": "text", "content": {"text": text}}
    if message_uuid:
        payload["uuid"] = message_uuid
    if webhook_secret:
        ts = str(int(time.time()))
        payload["timestamp"] = ts
        payload["sign"] = _feishu_webhook_sign(webhook_secret, ts)
    resp = http_post_json(webhook_url, payload, timeout=20, retries=1)
    code = _extract_feishu_code(resp)
    if code != 0:
        err_msg = resp.get("msg") or resp.get("StatusMessage") or ""
        raise RuntimeError(f"send webhook failed: code={code}, msg={err_msg}")
    return resp


def build_message(date_text: str, html_url: str, items: list[dict[str, Any]]) -> str:
    top = items[:8]
    lines = [f"Robtaxi 行业简报 {date_text}", ""]
    for idx, item in enumerate(top, 1):
        title = str(item.get("title_zh", "")).strip()
        link = str(item.get("link", "")).strip()
        lines.append(f"{idx}. {title}")
        lines.append(link)
    if html_url:
        lines.extend(["", f"完整网页：{html_url}"])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Send digest to Feishu app bot (open_id)")
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD; default Beijing date")
    parser.add_argument("--html-url", default="", help="Published HTML URL")
    parser.add_argument("--in", dest="in_root", default="./artifacts/brief", help="Brief input root")
    parser.add_argument("--text", default="", help="Send plain text instead of digest")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root")
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "brief_items.jsonl"
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)

    webhook_url = os.environ.get("FEISHU_WEBHOOK_URL", "").strip()
    webhook_secret = os.environ.get("FEISHU_WEBHOOK_SECRET", "").strip()

    app_id = os.environ.get("FEISHU_APP_ID", "").strip()
    app_secret = os.environ.get("FEISHU_APP_SECRET", "").strip()
    receive_id = os.environ.get("FEISHU_RECEIVE_OPEN_ID", "").strip()

    if args.text.strip():
        text = args.text.strip()
        items: list[dict[str, Any]] = []
    else:
        items = read_jsonl(in_file)
        text = build_message(date_text, args.html_url.strip(), items)

    run_id = os.environ.get("GITHUB_RUN_ID", "").strip()
    run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "").strip() or "1"
    if run_id:
        uuid_seed = f"{date_text}|{run_id}|{run_attempt}|{args.html_url.strip()}|{bool(args.text.strip())}"
    else:
        uuid_seed = f"{date_text}|{text}|{args.html_url.strip()}"
    message_uuid = hashlib.sha1(uuid_seed.encode("utf-8", errors="ignore")).hexdigest()

    if webhook_url:
        try:
            resp = send_webhook(webhook_url, webhook_secret, text, message_uuid=message_uuid)
            mark_stage(report_file, "notify", "success")
            patch_report(
                report_file,
                feishu_push_status={"status": "sent_webhook", "error": "", "message_uuid": message_uuid, "resp": resp},
            )
            print(f"[notify] sent via webhook uuid={message_uuid}")
            return 0
        except Exception as exc:
            mark_stage(report_file, "notify", "failed")
            patch_report(
                report_file,
                feishu_push_status={"status": "notify_failed", "message_uuid": message_uuid, "error": str(exc)[:500]},
            )
            print(f"[notify] webhook failed: {exc}")
            return 1

    if not (app_id and app_secret and receive_id):
        mark_stage(report_file, "notify", "skipped")
        patch_report(
            report_file,
            feishu_push_status={
                "status": "skipped",
                "error": "missing FEISHU_WEBHOOK_URL (recommended) or FEISHU_APP_ID/FEISHU_APP_SECRET/FEISHU_RECEIVE_OPEN_ID",
            },
        )
        print("[notify] skipped: missing feishu env vars")
        return 0

    try:
        token = fetch_tenant_token(app_id, app_secret)
        data = send_message(token, receive_id, text, message_uuid=message_uuid)
        message_id = str(data.get("data", {}).get("message_id", ""))
        mark_stage(report_file, "notify", "success")
        patch_report(
            report_file,
            feishu_push_status={
                "status": "sent",
                "error": "",
                "message_id": message_id,
                "message_uuid": message_uuid,
            },
        )
        print(f"[notify] sent message_id={message_id} uuid={message_uuid}")
        return 0
    except Exception as exc:
        mark_stage(report_file, "notify", "failed")
        patch_report(
            report_file,
            feishu_push_status={"status": "notify_failed", "message_uuid": message_uuid, "error": str(exc)[:500]},
        )
        print(f"[notify] failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
