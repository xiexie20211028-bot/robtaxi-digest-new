"""研发通知待发送记录与逐渠道回执；只有云端既有串行工作流执行发送。"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import json
import os
from pathlib import Path
import re
import socket
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .development_policy import DevelopmentError, digest, require, timestamp
from .editorial_digest import digest_title
from .finalize_notify import aggregate_notify_status

CHANNELS = ("feishu", "wecom")
RECEIPT_STATES = {"sending", "success", "failed", "unknown"}
HEADER = "Robotaxi 与 L3/L4 每日重点"
LEGACY_HEADER = "Robotaxi 与 L3/L4 乘用车产业简报｜研发闭环"


def notification(event_id: str, text: str, *, at: str | None = None) -> dict:
    require(bool(event_id) and len(event_id) <= 200, "通知必须有稳定事件ID")
    # 企微文本字节上限更短；超长内容由调用方改成摘要和证据链接。
    at = at or datetime.now(timezone.utc).isoformat()
    stat_date = timestamp(at).astimezone(timezone(timedelta(hours=8))).date().isoformat()
    message = f"{digest_title(stat_date)}\n【研发闭环状态通知】\n{text}"
    require(len(message.encode("utf-8")) <= 1800, "研发通知过长，请提供摘要和证据链接")
    return {"event": "notification_requested", "key": f"notification:{digest(event_id)}",
            "event_id": event_id, "text": message, "at": at}


def outbound_text(request: dict) -> str:
    """旧请求只更新发送信封，保留正式请求ID、时间、正文及成功回执。"""
    text = request["text"]
    if text.startswith(LEGACY_HEADER + "\n"):
        body = text.split("\n", 1)[1]
        return notification(request["event_id"], body, at=request["at"])["text"]
    return text


def validate_receipt(event: dict) -> None:
    from .development_policy import timestamp
    require(event.get("event") == "notification_receipt"
            and event.get("channel") in CHANNELS and event.get("status") in RECEIPT_STATES,
            "云端通知回执格式错误")
    require(isinstance(event.get("notification_key"), str) and event["notification_key"].startswith("notification:"), "通知回执缺少事件绑定")
    require(type(event.get("attempt")) is int and event["attempt"] > 0, "通知回执缺少尝试序号")
    require(event.get("key") == f"{event['notification_key']}:{event['channel']}:{event['attempt']}:{event['status']}", "通知回执键不匹配")
    require(event.get("error_category", "") in {"", "configuration", "rate_limit", "connection", "rejected", "unknown", "interrupted"}, "通知错误类别无效")
    require("provider_code" not in event or type(event["provider_code"]) is int, "通知服务错误码必须为整数")
    timestamp(event["at"])


class DeliveryFailure(Exception):
    def __init__(self, status: str, category: str, retry_after: float | None = None, *, provider_code: int | None = None):
        self.status, self.category, self.retry_after = status, category, retry_after
        self.provider_code = provider_code
        super().__init__(category)  # 不保存Webhook URL、签名或原始错误正文。


def post_once(url: str, body: dict, *, timeout: int, retries: int) -> dict:
    # 与已实际送达的生产发送器保持相同UTF-8正文，不把中文关键词转义成ASCII。
    request = Request(url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"), headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read())
    except HTTPError as exc:
        if exc.code == 429:
            raw = exc.headers.get("Retry-After", "")
            try:
                delay = float(raw)
            except ValueError:
                try:
                    delay = max(0, (parsedate_to_datetime(raw) - datetime.now(timezone.utc)).total_seconds())
                except (TypeError, ValueError):
                    delay = None
            raise DeliveryFailure("failed", "rate_limit", delay) from None
        if 400 <= exc.code < 500:
            raise DeliveryFailure("failed", "rejected") from None
        raise DeliveryFailure("unknown", "unknown") from None
    except URLError as exc:
        if isinstance(exc.reason, (socket.gaierror, ConnectionRefusedError)):
            raise DeliveryFailure("failed", "connection") from None
        raise DeliveryFailure("unknown", "unknown") from None
    except (TimeoutError, OSError, ValueError):
        raise DeliveryFailure("unknown", "unknown") from None


def send_channel(channel: str, text: str, key: str) -> None:
    from .notify_feishu import send_webhook as feishu
    from .notify_wecom import send_webhook as wecom
    url = os.environ.get(f"{channel.upper()}_WEBHOOK_URL", "").strip()
    if not url:
        raise DeliveryFailure("failed", "configuration")
    try:
        if channel == "feishu":
            feishu(url, os.environ.get("FEISHU_WEBHOOK_SECRET", "").strip(), text, digest(key), post=post_once)
        else:
            wecom(url, text, post=post_once)
    except DeliveryFailure:
        raise
    except RuntimeError as exc:
        code = re.search(r"(?:errcode|code)=(-?\d+)", str(exc))
        if code:
            category = "configuration" if code[1] == "19024" else "rejected"
            raise DeliveryFailure("failed", category, provider_code=int(code[1])) from None
        raise DeliveryFailure("unknown", "unknown") from None


def requested_notifications(events: list[dict]) -> list[dict]:
    requests = {}
    for event in events:
        if event.get("event") == "notification_requested":
            expected = notification(event.get("event_id", ""), "", at=event.get("at"))
            require(event.get("key") == expected["key"] and isinstance(event.get("text"), str), "通知请求格式错误")
            require(event["text"].startswith((HEADER, LEGACY_HEADER)) and len(event["text"].encode()) <= 1800, "通知正文无效")
            previous = requests.get(event["key"])
            require(not previous or previous["text"] == event["text"], "同一通知ID存在冲突正文")
            requests[event["key"]] = event
        elif event.get("event") == "heartbeat_status":
            # 从既有正式状态恢复通知，写状态后崩溃也不会永久漏发。
            message = "研发巡检已恢复；研发任务与生产验收结果另行报告。" if event.get("status") == "healthy" else "研发巡检超过36小时未成功；请检查本机网络和Codex运行状态。"
            item = notification(f"heartbeat:{event.get('at')}:{event.get('status')}", message, at=event.get("at"))
            requests[item["key"]] = item
    # 历史心跳只补最新一次，避免上线时补发过时的离线/恢复消息。
    latest_beat = next((e for e in reversed(list(requests.values())) if e["event_id"].startswith("heartbeat:")), None)
    return [r for r in requests.values() if not r["event_id"].startswith("heartbeat:") or r is latest_beat]


def drain(client, *, sender=send_channel, sleep=time.sleep, monotonic=time.monotonic, budget_seconds=300) -> dict:
    deadline = monotonic() + budget_seconds
    events = client.events()
    results = []
    for request in requested_notifications(events):
        statuses = {}
        for channel in CHANNELS:
            history = [e for e in events if e.get("event") == "notification_receipt"
                       and e.get("notification_key") == request["key"] and e.get("channel") == channel]
            for event in history:
                validate_receipt(event)
            latest = history[-1] if history else {}
            if latest.get("status") in {"success", "unknown"}:
                statuses[channel] = latest["status"]
                continue
            attempt = int(latest.get("attempt", 0))
            def record(status, category="", provider_code=None):
                event = {"event": "notification_receipt", "notification_key": request["key"],
                         "channel": channel, "status": status, "attempt": attempt,
                         "error_category": category, "at": datetime.now(timezone.utc).isoformat(),
                         "key": f"{request['key']}:{channel}:{attempt}:{status}"}
                if provider_code is not None:
                    event["provider_code"] = provider_code
                client.append(event)
                events.append(event)
                statuses[channel] = status
            if latest.get("status") == "sending":
                record("unknown", "interrupted")
                continue
            for local_attempt in range(3):
                if monotonic() + 25 > deadline:
                    statuses[channel] = "failed"
                    break
                attempt += 1
                record("sending")  # 必须先持久化；失败则绝不开始发送。
                try:
                    text = outbound_text(request)
                    if channel == "wecom" and statuses.get("feishu") == "unknown":
                        text += "\n飞书送达结果未知，请核验；本渠道独立报送。"
                    sender(channel, text, f"{request['key']}:{channel}")
                except DeliveryFailure as exc:
                    record(exc.status, exc.category, exc.provider_code)
                    if exc.status == "unknown" or exc.category not in {"connection", "rate_limit"} or local_attempt == 2:
                        break
                    delay = (30, 90)[local_attempt]
                    if exc.category == "rate_limit":
                        if exc.retry_after is None:
                            break
                        delay = max(delay, exc.retry_after)
                    if monotonic() + delay + 25 > deadline:
                        break
                    sleep(delay)
                except Exception:
                    record("unknown", "unknown")
                    break
                else:
                    record("success")
                    break
        aggregate = aggregate_notify_status(statuses.get("feishu", "failed"), statuses.get("wecom", "failed"), True)
        results.append({"key": request["key"], "channels": statuses, "status": aggregate})
    return {"schema_version": "robtaxi-development-notifications-v1", "results": results,
            "complete": all(r["status"] == "success" for r in results)}


def enqueue(client, event_id: str, text: str) -> dict:
    key = f"notification:{digest(event_id)}"
    previous = next((e for e in client.events() if e.get("key") == key), None)
    event = notification(event_id, text, at=previous.get("at") if previous else None)
    if previous:
        require(previous.get("event") == event["event"] and outbound_text(previous) == event["text"], "同一通知ID存在冲突正文")
        return previous
    client.append(event)
    return event


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", nargs="?", choices=["drain", "enqueue"], default="drain")
    parser.add_argument("--out", required=True)
    parser.add_argument("--request", help="包含event_id和text的结构化摘要")
    parser.add_argument("--run-id", default=os.environ.get("CODEX_THREAD_ID", ""))
    args = parser.parse_args()
    from .development_cycle import ROOT, assert_trusted_checkout, initialize_run, load_policy
    from .development_runtime import GitHub, repository_lock
    try:
        policy = load_policy()
        client = GitHub(policy)
        if args.action == "enqueue":
            with repository_lock(policy["repository"]):
                initialize_run(client, policy, args.run_id)
                assert_trusted_checkout(client)
                require(Path(args.out).resolve().is_relative_to((ROOT / ".local/robtaxi-development/runs" / args.run_id).resolve()), "通知输出须在本轮独立目录")
                require(bool(args.request), "缺少通知摘要")
                payload = json.loads(Path(args.request).read_text())
                event = enqueue(client, payload["event_id"], payload["text"])
                # 写入成功才触发已有通知入口；dispatch失败不删除待发送证据。
                client.gh("workflow", "run", "robtaxi-agent-review.yml", "--repo", policy["repository"], "--ref", "main", "-f", "notification_only=true")
                result = {"complete": True, "queued": event["key"], "delivery": "pending"}
        else:
            require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_REF") == "refs/heads/main", "发送仅限主分支云端串行工作流")
            result = drain(client)
    except (DevelopmentError, OSError, ValueError, KeyError) as exc:
        result = {"complete": False, "error_type": type(exc).__name__}
    output = Path(args.out)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
