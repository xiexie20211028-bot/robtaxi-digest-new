from pathlib import Path

from app.fetch_structured import _extract_published_from_jsonld
from app.site_rules import (
    extract_jsonld_date,
    extract_site_specific_published,
    is_invalid_structured_record,
    normalize_site_specific_record,
    prefilter_structured_links,
)


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_prefilter_singapore_lta_recent_links() -> None:
    links = [
        "https://www.lta.gov.sg/content/ltagov/en/newsroom/2026/3/news-releases/test-a.html",
        "https://www.lta.gov.sg/content/ltagov/en/newsroom/2025/12/news-releases/test-b.html",
        "https://www.lta.gov.sg/content/ltagov/en/newsroom/2020/1/news-releases/test-c.html",
    ]
    filtered = prefilter_structured_links("singapore_lta_news_structured", links)
    assert filtered == [
        "https://www.lta.gov.sg/content/ltagov/en/newsroom/2026/3/news-releases/test-a.html",
        "https://www.lta.gov.sg/content/ltagov/en/newsroom/2025/12/news-releases/test-b.html",
    ]


def test_invalid_structured_records_are_blocked() -> None:
    assert is_invalid_structured_record(
        "waymo_blog_structured",
        {"title": "Latest news", "link": "https://waymo.com/blog/search/?t=Safety"},
    )
    assert is_invalid_structured_record(
        "apollo_go_baidu_structured",
        {"title": "Apollo", "link": "https://apollo.auto/news/apollo-self-driving"},
    )
    assert is_invalid_structured_record(
        "california_dmv_news_structured",
        {"title": "News Releases", "link": "https://www.dmv.ca.gov/portal/news-and-media/news-releases/"},
    )


def test_extract_aastocks_site_specific_published_from_inline_text() -> None:
    html = "<script>ConvertToLocalTime({ dt : '2026/03/09 12:30' })</script>"
    published, source = extract_site_specific_published("", html, "https://www.aastocks.com/news/aat240101.htm")
    assert published == "2026/03/09 12:30"
    assert source == "site_specific_date"


def test_extract_aastocks_site_specific_published_from_url() -> None:
    published, source = extract_site_specific_published("", "", "https://www.aastocks.com/news/aat260309.htm")
    assert published == "2026/03/09"
    assert source == "url_date"


def test_extract_california_dmv_jsonld_date() -> None:
    html = (FIXTURES / "dmv_webpage.html").read_text(encoding="utf-8")
    published, source = extract_jsonld_date("california_dmv_news_structured", html)
    assert published == "2026-02-21T00:19:53+00:00"
    assert source == "jsonld"


def test_extract_nuro_jsonld_published() -> None:
    html = (FIXTURES / "nuro_article.html").read_text(encoding="utf-8")
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    published = _extract_published_from_jsonld(soup)
    assert published == "2025-07-15T18:08:31"


def test_normalize_site_specific_record_is_stable() -> None:
    record = {"title": "demo", "link": "https://example.com/a"}
    assert normalize_site_specific_record("pony_news_structured", record) == record
