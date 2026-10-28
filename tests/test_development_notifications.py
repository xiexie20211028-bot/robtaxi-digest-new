"""通知故障注入；不连接外部消息渠道。"""
import json
from pathlib import Path
from urllib.error import HTTPError
from email.message import Message

import pytest

from app.development_notifications import (DeliveryFailure, HEADER, LEGACY_HEADER, drain, enqueue, notification,
                                         post_once, requested_notifications, send_channel, validate_receipt)
from app.development_policy import DevelopmentError
from app.development_runtime import GitHub, MARKER
from app.development_cycle import load_policy


class Ledger:
    def __init__(self):
        self.rows = [notification("daily:2026-10-27", "今日研发：等待生产验收。")]
    def events(self, _issue=None):
        return [dict(e) for e in self.rows]
    def append(self, event):
        self.rows.append(dict(event))


def test_enqueue_resumes_same_request_and_rejects_changed_body():
    client = Ledger()
    initial = len(client.rows)
    enqueue(client, "daily:2026-10-28", "日报")
    enqueue(client, "daily:2026-10-28", "日报")
    assert len(client.rows) == initial + 1
    with pytest.raises(DevelopmentError, match="冲突"):
        enqueue(client, "daily:2026-10-28", "另一份报告")


def test_real_production_title_is_shared_without_changing_digest_text():
    from app.editorial_digest import digest_title, render_digest_text
    title = digest_title("2026-10-28")
    assert title == "Robotaxi 与 L3/L4 每日重点｜统计日 2026-10-28"
    message = notification("test", "研发验证", at="2026-10-27T17:00:00Z")
    assert message["text"].splitlines()[0] == title
    assert render_digest_text({"stat_date": "2026-10-28", "headline": "判断"}).splitlines()[0] == title


def test_legacy_failed_feishu_uses_production_title_without_resending_successful_wecom():
    client = Ledger()
    request = notification("legacy:2026-10-27", "旧状态正文", at="2026-10-27T08:03:00+00:00")
    request["text"] = LEGACY_HEADER + "\n旧状态正文"
    client.rows = [request, {"event": "notification_receipt", "notification_key": request["key"], "channel": "wecom", "status": "success",
        "attempt": 1, "error_category": "", "at": "2026-10-28T03:11:54+00:00", "key": f"{request['key']}:wecom:1:success"}]
    calls = []
    result = drain(client, sender=lambda channel, text, key: calls.append((channel, text, key)))
    assert result["complete"] and len(calls) == 1 and calls[0][0] == "feishu"
    assert calls[0][1].startswith("Robotaxi 与 L3/L4 每日重点｜统计日 2026-10-27")
    assert calls[0][1].endswith("旧状态正文")
    assert enqueue(client, "legacy:2026-10-27", "旧状态正文") == request


def test_cross_day_enqueue_keeps_original_title_date_and_identity():
    client = Ledger()
    original = notification("old-day", "固定正文", at="2026-10-20T02:00:00Z")
    client.rows.append(original)
    assert enqueue(client, "old-day", "固定正文") == original


def test_feishu_rejection_does_not_block_wecom_and_success_not_repeated():
    client, calls = Ledger(), []
    def send(channel, text, key):
        calls.append(channel)
        assert text.startswith(HEADER)
        if channel == "feishu":
            raise DeliveryFailure("failed", "configuration")
    first = drain(client, sender=send)
    assert calls == ["feishu", "wecom"]
    assert first["results"][0]["status"] == "partial" and not first["complete"]
    calls.clear()
    assert drain(client, sender=lambda *args: calls.append(args[0]))["complete"]
    assert calls == ["feishu"]
    drain(client, sender=lambda *_: pytest.fail("成功渠道不能重发"))


def test_unknown_result_not_retried_and_other_channel_explains():
    client, calls = Ledger(), []
    def send(channel, text, key):
        calls.append((channel, text))
        if channel == "feishu":
            raise DeliveryFailure("unknown", "unknown")
    result = drain(client, sender=send)
    assert result["results"][0]["channels"] == {"feishu": "unknown", "wecom": "success"}
    assert "结果未知" in calls[-1][1]
    drain(client, sender=lambda *_: pytest.fail("未知结果不能盲重发"))


def test_process_crash_after_send_intent_is_unknown_on_restart():
    client = Ledger()
    def crash(*_):
        raise KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        drain(client, sender=crash)
    assert client.rows[-1]["status"] == "sending"
    calls = []
    result = drain(client, sender=lambda channel, *_: calls.append(channel))
    assert calls == ["wecom"]
    assert result["results"][0]["channels"]["feishu"] == "unknown"


def test_lost_success_receipt_cannot_cause_duplicate_send():
    client = Ledger()
    append = client.append
    def fail_receipt(event):
        if event.get("status") == "success":
            raise DevelopmentError("正式状态失联")
        append(event)
    client.append = fail_receipt
    with pytest.raises(DevelopmentError):
        drain(client, sender=lambda *_: None)
    client.append = append
    calls = []
    result = drain(client, sender=lambda channel, *_: calls.append(channel))
    assert calls == ["wecom"]
    assert not result["complete"]


def test_failed_intent_prevents_external_send():
    client = Ledger()
    def unavailable(_):
        raise DevelopmentError("写入失败")
    client.append = unavailable
    with pytest.raises(DevelopmentError):
        drain(client, sender=lambda *_: pytest.fail("持久记录失败不能发送"))


def test_transient_retry_delays_and_total_attempt_limit():
    client, calls, waits = Ledger(), [], []
    def send(channel, *_):
        calls.append(channel)
        raise DeliveryFailure("failed", "connection")
    result = drain(client, sender=send, sleep=waits.append)
    assert calls == ["feishu"] * 3 + ["wecom"] * 3
    assert waits == [30, 90, 30, 90]
    assert not result["complete"]


@pytest.mark.parametrize("retry_after", [None, 900])
def test_rate_limit_without_safe_delay_or_outside_budget_is_not_retried(retry_after):
    calls = []
    def send(channel, *_):
        calls.append(channel)
        raise DeliveryFailure("failed", "rate_limit", retry_after)
    drain(Ledger(), sender=send, sleep=lambda _: pytest.fail("不应等待"))
    assert calls == ["feishu", "wecom"]


def test_remote_retry_after_is_respected():
    calls, waits = [], []
    def send(channel, *_):
        calls.append(channel)
        if len(calls) == 1:
            raise DeliveryFailure("failed", "rate_limit", 60)
    assert drain(Ledger(), sender=send, sleep=waits.append)["complete"]
    assert waits == [60]


def test_missing_channel_credentials_fail_not_skip(monkeypatch):
    for name in ("FEISHU_WEBHOOK_URL", "WECOM_WEBHOOK_URL"):
        monkeypatch.delenv(name, raising=False)
    result = drain(Ledger())
    assert result["results"][0]["status"] == "failed"


def test_keyword_rejection_is_sanitized_and_known_prefix_retained(monkeypatch):
    monkeypatch.setenv("FEISHU_WEBHOOK_URL", "https://example.invalid/SECRET")
    def rejected(*a, **k):
        raise RuntimeError("send webhook failed: code=19024, msg=Key Words Not Found SECRET")
    monkeypatch.setattr("app.notify_feishu.send_webhook", rejected)
    with pytest.raises(DeliveryFailure) as failure:
        send_channel("feishu", HEADER, "id")
    assert failure.value.category == "configuration" and "SECRET" not in str(failure.value)


def test_http_rate_limit_preserves_retry_after_without_leaking_url(monkeypatch):
    headers = Message(); headers["Retry-After"] = "75"
    def limited(*a, **k):
        raise HTTPError("https://example.invalid/SECRET", 429, "limited", headers, None)
    monkeypatch.setattr("app.development_notifications.urlopen", limited)
    with pytest.raises(DeliveryFailure) as failure:
        post_once("https://example.invalid/SECRET", {}, timeout=20, retries=1)
    assert failure.value.retry_after == 75 and "SECRET" not in str(failure.value)


def test_only_latest_legacy_heartbeat_is_recovered():
    events = [{"event": "heartbeat_status", "status": status, "at": date}
              for status, date in [("stale", "2026-10-01T00:00:00Z"), ("healthy", "2026-10-02T00:00:00Z")]]
    requests = requested_notifications(events)
    assert len(requests) == 1 and "已恢复" in requests[0]["text"]
    assert requests == requested_notifications(events)


def test_bot_receipt_is_allowed_but_contract_is_still_rejected(monkeypatch):
    client = GitHub(load_policy())
    ledger = Ledger();drain(ledger, sender=lambda *_: None)
    receipt = next(e for e in ledger.rows if e.get("status") == "success")
    validate_receipt(receipt)
    def comments(event):
        return [{"body": f"<!-- {MARKER}\n{json.dumps(event)}\n-->", "user": {"login": "github-actions[bot]"}, "created_at": "2026-10-27T00:00:00Z"}]
    monkeypatch.setattr(client, "paginate", lambda _: comments(receipt))
    assert client.events()[0]["status"] == "success"
    monkeypatch.setattr(client, "paginate", lambda _: comments({"event": "contract"}))
    with pytest.raises(DevelopmentError, match="越权"):
        client.events()
    receipt["channel"] = "anything"
    with pytest.raises(DevelopmentError):
        validate_receipt(receipt)


def test_notification_only_workflow_cannot_run_review_model():
    text = (Path(__file__).resolve().parents[1] / ".github/workflows/robtaxi-agent-review.yml").read_text()
    # 不引入运行时YAML依赖；核对实际工作流两条隔离路径和末尾失败门禁。
    notification_job, review_job = text.split("  review:\n", 1)
    assert "inputs.notification_only != true" in review_job
    assert "DEEPSEEK_API_KEY" not in notification_job
    assert "steps.delivery.outcome != 'success'" in notification_job
    assert "cancel-in-progress: false" in text
