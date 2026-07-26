from __future__ import annotations

import io
import json
from urllib.error import HTTPError

import pytest

from app import common
from app.editorial_digest import build_model_digest
from app.summarize import deepseek_summary_structured


def _summary_config() -> dict:
    return {
        "impact_target_taxonomy": ["运营方", "监管"],
        "ban_phrases": ["详见原文"],
    }


def test_summary_uses_v4_flash_non_thinking_json_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}

    def fake_post(_url: str, body: dict, **_kwargs: object) -> dict:
        captured.update(body)
        content = {
            "title_zh": "Waymo 扩大服务",
            "what": "Waymo 扩大了服务范围。",
            "why": "当地商业化运营进入新阶段。",
            "so_what": "这会影响运营规模和竞争节奏。",
            "impact_targets": ["运营方"],
            "tags": ["扩张"],
            "confidence": 0.9,
            "importance": 4,
        }
        return {"choices": [{"message": {"content": json.dumps(content, ensure_ascii=False)}}]}

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.delenv("DEEPSEEK_MODEL", raising=False)
    monkeypatch.setattr("app.summarize.http_post_json", fake_post)

    result = deepseek_summary_structured("Waymo expands", "Waymo expanded its service.", _summary_config())

    assert result["title_zh"] == "Waymo 扩大服务"
    assert captured["model"] == "deepseek-v4-flash"
    assert captured["thinking"] == {"type": "disabled"}
    assert captured["response_format"] == {"type": "json_object"}


def test_editorial_digest_uses_v4_flash_non_thinking_json_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}

    def fake_post(_url: str, body: dict, **_kwargs: object) -> dict:
        captured.update(body)
        content = {
            "headline": "Waymo 扩大 Robotaxi 服务。",
            "key_points": ["运营规模继续扩大。"],
            "top_items": [
                {
                    "title": "Waymo 扩大服务",
                    "why_it_matters": "这会影响当地 Robotaxi 的竞争格局。",
                    "impact_targets": ["运营方"],
                    "link": "https://example.com/waymo",
                }
            ],
            "other_items": [],
        }
        return {"choices": [{"message": {"content": json.dumps(content, ensure_ascii=False)}}]}

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.delenv("DEEPSEEK_MODEL", raising=False)
    monkeypatch.setattr("app.editorial_digest.http_post_json", fake_post)

    digest = build_model_digest(
        "2026-07-26",
        [
            {
                "title_zh": "Waymo 扩大服务",
                "summary_so_what": "这会影响当地 Robotaxi 的竞争格局。",
                "impact_targets": ["运营方"],
                "link": "https://example.com/waymo",
                "importance": 4,
            }
        ],
        {"window_start_bj": "2026-07-25 00:00:00"},
    )

    assert digest["fallback_used"] is False
    assert captured["model"] == "deepseek-v4-flash"
    assert captured["thinking"] == {"type": "disabled"}
    assert captured["response_format"] == {"type": "json_object"}


def test_http_post_json_keeps_sanitized_api_error_details(monkeypatch: pytest.MonkeyPatch) -> None:
    error_body = json.dumps(
        {
            "error": {
                "type": "invalid_request_error",
                "code": "model_not_found",
                "message": "model deepseek-chat not found; Bearer secret-token; sk-example123456",
            }
        }
    ).encode("utf-8")

    def fake_urlopen(*_args: object, **_kwargs: object) -> object:
        raise HTTPError(
            "https://api.deepseek.com/chat/completions",
            400,
            "Bad Request",
            {},
            io.BytesIO(error_body),
        )

    monkeypatch.setattr(common, "urlopen", fake_urlopen)
    monkeypatch.setattr(common.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError) as exc_info:
        common.http_post_json(
            "https://api.deepseek.com/chat/completions",
            {"model": "deepseek-chat"},
            retries=1,
        )

    message = str(exc_info.value)
    assert "HTTP 400 Bad Request" in message
    assert "code=model_not_found" in message
    assert "Bearer [REDACTED]" in message
    assert "[REDACTED]" in message
    assert "secret-token" not in message
    assert "sk-example123456" not in message
