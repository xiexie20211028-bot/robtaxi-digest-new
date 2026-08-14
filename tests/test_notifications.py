from __future__ import annotations

import sys

from app.common import read_json
from app.finalize_notify import aggregate_notify_status, normalize_channel_status
from app.notify_feishu import build_message as build_feishu_message
from app.notify_feishu import main as feishu_main
from app.notify_wecom import build_message as build_wecom_message
from app.notify_wecom import main as wecom_main


def _item() -> dict:
    return {
        "title_zh": "Robotaxi 获得运营许可",
        "summary_so_what": "这会扩大商业化运营范围。",
        "impact_targets": ["运营方", "监管"],
        "importance": 5,
        "link": "https://example.com/event",
    }


def test_notification_fallback_messages_include_business_fields() -> None:
    report = {"window_start_bj": "2026-08-13 00:00:00", "window_end_bj": "2026-08-14 00:00:00"}
    feishu = build_feishu_message("2026-08-14", "https://example.com/full", [_item()], report)
    wecom = build_wecom_message("2026-08-14", "https://example.com/full", report, [_item()])
    for text in (feishu, wecom):
        assert "Robotaxi 获得运营许可" in text
        assert "So what：这会扩大商业化运营范围。" in text
        assert "影响对象：运营方 / 监管" in text
        assert "完整网页：https://example.com/full" in text


def test_feishu_main_uses_editorial_digest_and_reports_sent_output(tmp_path, monkeypatch) -> None:
    date_text = "2026-08-14"
    output_file = tmp_path / "github-output"
    captured: dict[str, str] = {}
    monkeypatch.setenv("FEISHU_WEBHOOK_URL", "https://example.com/feishu")
    monkeypatch.delenv("FEISHU_APP_ID", raising=False)
    monkeypatch.setattr("app.notify_feishu.load_digest_text", lambda *_args: "主编摘要")

    def fake_send(_url, _secret, text, message_uuid=""):
        captured.update(text=text, uuid=message_uuid)
        return {"code": 0}

    monkeypatch.setattr("app.notify_feishu.send_webhook", fake_send)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "notify_feishu",
            "--date",
            date_text,
            "--report",
            str(tmp_path / "reports"),
            "--github-output",
            str(output_file),
        ],
    )

    assert feishu_main() == 0
    assert captured["text"] == "主编摘要"
    assert captured["uuid"]
    assert "sent=true" in output_file.read_text(encoding="utf-8")
    report = read_json(tmp_path / "reports" / date_text / "run_report.json")
    assert report["feishu_push_status"]["status"] == "sent_webhook"


def test_feishu_missing_credentials_never_reports_sent(tmp_path, monkeypatch) -> None:
    for name in (
        "FEISHU_WEBHOOK_URL",
        "FEISHU_WEBHOOK_SECRET",
        "FEISHU_APP_ID",
        "FEISHU_APP_SECRET",
        "FEISHU_RECEIVE_OPEN_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    output_file = tmp_path / "github-output"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "notify_feishu",
            "--date",
            "2026-08-14",
            "--text",
            "test",
            "--report",
            str(tmp_path / "reports"),
            "--github-output",
            str(output_file),
        ],
    )

    assert feishu_main() == 0
    assert "sent=false" in output_file.read_text(encoding="utf-8")
    assert "channel_status=skipped" in output_file.read_text(encoding="utf-8")


def test_wecom_main_reports_success_and_failure_outputs(tmp_path, monkeypatch) -> None:
    date_text = "2026-08-14"
    success_output = tmp_path / "success-output"
    monkeypatch.setenv("WECOM_WEBHOOK_URL", "https://example.com/wecom")
    monkeypatch.setattr("app.notify_wecom.send_webhook", lambda _url, _text: {"errcode": 0})
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "notify_wecom",
            "--date",
            date_text,
            "--text",
            "test",
            "--report",
            str(tmp_path / "reports"),
            "--github-output",
            str(success_output),
        ],
    )
    assert wecom_main() == 0
    assert "sent=true" in success_output.read_text(encoding="utf-8")

    failure_output = tmp_path / "failure-output"
    monkeypatch.delenv("WECOM_WEBHOOK_URL", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "notify_wecom",
            "--date",
            date_text,
            "--text",
            "test",
            "--report",
            str(tmp_path / "reports-2"),
            "--github-output",
            str(failure_output),
        ],
    )
    assert wecom_main() == 1
    assert "sent=false" in failure_output.read_text(encoding="utf-8")


def test_finalize_notification_status_handles_locks_and_partial_failures() -> None:
    assert aggregate_notify_status("already_sent", "success", expected=True) == "success"
    assert aggregate_notify_status("success", "failed", expected=True) == "partial"
    assert aggregate_notify_status("failed", "failed", expected=True) == "failed"
    assert aggregate_notify_status("failed", "failed", expected=False) == "skipped"
    assert normalize_channel_status("reported", {"status": "sent_webhook"}, expected=True) == "success"
    assert normalize_channel_status("reported", {"status": "skipped"}, expected=True) == "failed"
