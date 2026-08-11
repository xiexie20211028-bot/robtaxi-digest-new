from __future__ import annotations

import subprocess
import io
from urllib.error import HTTPError

import pytest

from app.common import _curl_http_get, http_get_bytes


def test_curl_bad_ecpoint_retries_with_tls12_p256(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    def fake_run(cmd: list[str], **_: object) -> subprocess.CompletedProcess[bytes]:
        commands.append(cmd)
        if len(commands) == 1:
            return subprocess.CompletedProcess(cmd, 35, b"", b"OpenSSL: bad ecpoint")
        return subprocess.CompletedProcess(cmd, 0, b"ok", b"")

    monkeypatch.setattr("app.common.shutil.which", lambda _: "/usr/bin/curl")
    monkeypatch.setattr("app.common.subprocess.run", fake_run)

    payload = _curl_http_get("https://example.com", {"User-Agent": "test"}, 20, 3)

    assert payload == b"ok"
    assert len(commands) == 2
    assert "--tlsv1.2" not in commands[0]
    assert commands[1][commands[1].index("--tls-max") + 1] == "1.2"
    assert commands[1][commands[1].index("--curves") + 1] == "P-256"
    assert "--insecure" not in commands[1]
    assert "-k" not in commands[1]


def test_curl_other_error_does_not_use_tls_compat_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    def fake_run(cmd: list[str], **_: object) -> subprocess.CompletedProcess[bytes]:
        commands.append(cmd)
        return subprocess.CompletedProcess(cmd, 28, b"", b"Operation timed out")

    monkeypatch.setattr("app.common.shutil.which", lambda _: "/usr/bin/curl")
    monkeypatch.setattr("app.common.subprocess.run", fake_run)

    with pytest.raises(RuntimeError, match="Operation timed out"):
        _curl_http_get("https://example.com", {"User-Agent": "test"}, 20, 3)

    assert len(commands) == 1


def test_curl_tls_compat_retry_failure_keeps_both_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    def fake_run(cmd: list[str], **_: object) -> subprocess.CompletedProcess[bytes]:
        commands.append(cmd)
        if len(commands) == 1:
            return subprocess.CompletedProcess(cmd, 35, b"", b"OpenSSL: bad ecpoint")
        return subprocess.CompletedProcess(cmd, 35, b"", b"TLS compatibility retry failed")

    monkeypatch.setattr("app.common.shutil.which", lambda _: "/usr/bin/curl")
    monkeypatch.setattr("app.common.subprocess.run", fake_run)

    with pytest.raises(RuntimeError, match="tls12_p256_retry_failed") as exc_info:
        _curl_http_get("https://example.com", {"User-Agent": "test"}, 20, 3)

    assert "bad ecpoint" in str(exc_info.value)
    assert "TLS compatibility retry failed" in str(exc_info.value)
    assert len(commands) == 2


def test_http_cache_reuses_body_on_etag_304(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    calls = []

    class Response:
        status = 200
        headers = {"ETag": '"v1"', "Last-Modified": "Mon, 10 Aug 2026 08:00:00 GMT"}

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return b"cached-body"

    def fake_urlopen(request, timeout=20):
        _ = timeout
        calls.append(request)
        if len(calls) == 1:
            return Response()
        assert request.headers.get("If-none-match") == '"v1"'
        raise HTTPError(request.full_url, 304, "Not Modified", {}, io.BytesIO())

    monkeypatch.setattr("app.common.urlopen", fake_urlopen)
    first = http_get_bytes("https://example.com/feed", retries=1, cache_dir=tmp_path)
    second = http_get_bytes("https://example.com/feed", retries=1, cache_dir=tmp_path)
    assert first == b"cached-body"
    assert second == b"cached-body"
