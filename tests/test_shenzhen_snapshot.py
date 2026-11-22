import base64
import json
import zlib
from datetime import datetime, timezone

import pytest

from app.fetch import process_source
from app.shenzhen_snapshot import SOURCE, VARIABLE, read_snapshot


NOW = datetime(2026, 11, 22, 8, tzinfo=timezone.utc)
SOURCE_CONFIG = {"id": SOURCE, "source_type": "structured_web", "entry_urls": ["https://jtys.sz.gov.cn/"]}
ARTICLE = "https://jtys.sz.gov.cn/ydmh/jtzx/tzgg_1508/content/post_123.html"


def snapshot(monkeypatch, **overrides):
    data = {"source_id": SOURCE, "entry_urls": SOURCE_CONFIG["entry_urls"], "collected_at": NOW.isoformat(),
            "rows": [{"link": ARTICLE, "title": "公告", "content": "公告原文", "published": "2026-11-21"}],
            "stats": {"listed_items": 1, "request_count": 2, "request_success_count": 2}, **overrides}
    monkeypatch.setenv(VARIABLE, base64.b64encode(zlib.compress(json.dumps(data).encode())).decode())


@pytest.mark.parametrize("change", [{"source_id": "other"}, {"collected_at": "2026-11-21T10:00:00+00:00"},
    {"collected_at": "2026-11-23T08:00:00+00:00"}, {"rows": []},
    {"stats": {"listed_items": 1, "request_count": 2, "request_success_count": 1}}])
def test_reject_invalid_snapshot(monkeypatch, change):
    snapshot(monkeypatch, **change)
    assert read_snapshot(SOURCE_CONFIG, NOW) is None


def test_source_report_preserves_collection_time(monkeypatch):
    snapshot(monkeypatch)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    monkeypatch.setattr("app.fetch.datetime", Clock)
    rows, stat = process_source(SOURCE_CONFIG, {}, "later")
    assert rows[0].fetched_at == stat.last_success_at == NOW.isoformat()
    assert rows[0].payload["collection_origin"] == "shenzhen_local"
    assert stat.request_success_count == 2
    assert read_snapshot({**SOURCE_CONFIG, "id": "another_source"}, NOW) is None
