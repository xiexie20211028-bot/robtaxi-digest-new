from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from app.fetch_rss import _parse_rss_feed
from app.fetch_structured import _extract_article_css, _extract_links_css


ROOT = Path(__file__).resolve().parents[1]


def _source(source_id: str) -> dict[str, Any]:
    config = json.loads((ROOT / "sources.json").read_text(encoding="utf-8"))
    return next(source for source in config["sources"] if source["id"] == source_id)


def test_rss_parser_recovers_bare_ampersands_without_changing_cdata() -> None:
    payload = b"""<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"><channel><item>
      <title>Robotaxi &amp; mobility</title>
      <description><![CDATA[AT&T keeps a literal & marker]]></description>
      <link>https://example.test/news?a=1&b=2</link>
      <media:content xmlns:media="urn:media" url="https://example.test/a.jpg?w=300&h=300" />
    </item></channel></rss>"""

    rows = _parse_rss_feed(payload, "测试源")

    assert rows[0]["title"] == "Robotaxi & mobility"
    assert rows[0]["summary"] == "AT&T keeps a literal & marker"
    assert rows[0]["link"] == "https://example.test/news?a=1&b=2"


def test_rss_parser_removes_invalid_control_char_before_retry() -> None:
    payload = (
        b'<rss version="2.0"><channel><item><title>Robotaxi\x08 update</title>'
        b"<link>https://example.test/news</link></item></channel></rss>"
    )

    rows = _parse_rss_feed(payload, "测试源")

    assert rows[0]["title"] == "Robotaxi update"


def test_rss_parser_rejects_html_challenge_page() -> None:
    with pytest.raises(ValueError, match="non_rss_or_challenge_page"):
        _parse_rss_feed(b"<!DOCTYPE html><html><body>challenge</body></html>", "36Kr")


def test_rss_parser_reports_xml_that_cannot_be_recovered() -> None:
    with pytest.raises(ValueError, match="invalid_xml"):
        _parse_rss_feed(b"<rss><channel><item></channel></rss>", "测试源")


def test_unavailable_sources_are_disabled() -> None:
    for source_id in ("huxiu", "thepaper", "mps_traffic_bureau_structured"):
        assert _source(source_id)["enabled"] is False


def test_partial_failure_entry_urls_are_removed() -> None:
    assert _source("weride_news_structured")["entry_urls"] == [
        "https://www.weride.ai/press"
    ]
    assert _source("california_dmv_av_reports_structured")["entry_urls"] == [
        "https://www.dmv.ca.gov/portal/vehicle-industry-services/autonomous-vehicles/autonomous-vehicle-testing-permit-holders/"
    ]


def test_uber_newsroom_selectors_keep_us_articles_only() -> None:
    source = _source("uber_news_structured")
    list_url = source["entry_urls"][0]
    html = """
    <main>
      <a href="/us/en/newsroom/">Newsroom</a>
      <a href="/us/en/newsroom/page/2/">More news</a>
      <a href="/gb/en/newsroom/uk-update/">UK update</a>
      <a href="/us/en/newsroom/robotaxi-update/">Robotaxi update</a>
    </main>
    """
    assert _extract_links_css(list_url, html, source["selectors"]) == [
        "https://www.uber.com/us/en/newsroom/robotaxi-update/"
    ]

    article = """
    <main>
      <div data-block-id="ArticleHeader">
        <h1>Robotaxi update</h1>
        <div data-baseweb="typo-labellarge">July 2, 2026</div>
      </div>
      <p>Uber announced a new autonomous mobility partnership.</p>
    </main>
    """
    record = _extract_article_css(
        "https://www.uber.com/us/en/newsroom/robotaxi-update/",
        article,
        source["selectors"],
        source["name"],
        source_id=source["id"],
    )
    assert record["title"] == "Robotaxi update"
    assert record["published"] == "July 2, 2026"
    assert "autonomous mobility partnership" in record["content"]


def test_shenzhen_mobile_notice_selectors_extract_article() -> None:
    source = _source("shenzhen_transport_structured")
    list_url = source["entry_urls"][0]
    html = """
    <main>
      <a href="/ydmh/jtzx/tzgg_1508/">通知公告</a>
      <a href="/ydmh/jtzx/tzgg_1508/content/post_12345678.html">测试公告</a>
    </main>
    """
    assert _extract_links_css(list_url, html, source["selectors"]) == [
        "https://jtys.sz.gov.cn/ydmh/jtzx/tzgg_1508/content/post_12345678.html"
    ]

    article = """
    <html>
      <head><meta name="PubDate" content="2026-03-09 09:27"></head>
      <body>
        <h2 class="tit">智能网联汽车测试公告</h2>
        <div class="xl-con"><p>深圳市交通运输局发布测试道路相关通知。</p></div>
      </body>
    </html>
    """
    record = _extract_article_css(
        "https://jtys.sz.gov.cn/ydmh/jtzx/tzgg_1508/content/post_12345678.html",
        article,
        source["selectors"],
        source["name"],
        source_id=source["id"],
    )
    assert record["title"] == "智能网联汽车测试公告"
    assert record["published"] == "2026-03-09 09:27"
    assert "测试道路相关通知" in record["content"]
