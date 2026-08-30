from pathlib import Path

from app.fetch_structured import _extract_article_css, _extract_links_css, _extract_published_from_jsonld, fetch_structured_source
from app.parse import _extract_date_from_html
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


def test_prefilter_miit_keeps_only_official_article_pages_and_sorts_newest_first() -> None:
    links = [
        "https://www.miit.gov.cn/xwdt/index.html",
        "https://www.miit.gov.cn/xwdt/tpxwa22/index.html",
        "https://www.miit.gov.cn/xwdt/gxdt/ldhd/art/2024/art_old.html",
        "https://www.miit.gov.cn/xwdt/gxdt/ldhd/art/2026/art_current.html",
        "https://example.com/xwdt/gxdt/ldhd/art/2027/art_spoofed.html",
    ]
    assert prefilter_structured_links("miit_news_structured", links) == [
        "https://www.miit.gov.cn/xwdt/gxdt/ldhd/art/2026/art_current.html",
        "https://www.miit.gov.cn/xwdt/gxdt/ldhd/art/2024/art_old.html",
    ]


def test_miit_invalid_structured_record_rejects_listing_and_legacy_column() -> None:
    assert is_invalid_structured_record(
        "miit_news_structured",
        {"title": "工业和信息化部新闻", "link": "https://www.miit.gov.cn/xwdt/index.html"},
    )
    assert is_invalid_structured_record(
        "miit_news_structured",
        {"title": "旧栏目", "link": "https://www.miit.gov.cn/xwdt/tpxwa22/index.html"},
    )


def test_miit_article_css_extracts_con_time_when_meta_date_is_unavailable() -> None:
    html = """
    <main>
      <h1>工业和信息化部新闻</h1>
      <div id="con_time">发布时间：2026-08-10 08:30</div>
      <p>页面正文。</p>
    </main>
    """
    record = _extract_article_css(
        "https://www.miit.gov.cn/xwdt/gxdt/ldhd/art/2026/art_example.html",
        html,
        {"title": "h1", "content": "p", "published": "#con_time"},
        "MIIT Public Notices",
        source_id="miit_news_structured",
    )
    assert record["published"] == "2026-08-10 08:30"


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



def test_extract_unece_regulatory_press_links() -> None:
    html = """
    <main>
      <a href="/sustainable-development/press/unece-adopts-first-ever-global-rules-allowing-fully-autonomous">UNECE adopts first ever global rules allowing fully autonomous vehicles</a>
    </main>
    """
    links = _extract_links_css(
        "https://unece.org/sustainable-development/press/vehicle-regulations",
        html,
        {"article_link": 'a[href*="/sustainable-development/press/"]'},
    )
    assert links == [
        "https://unece.org/sustainable-development/press/unece-adopts-first-ever-global-rules-allowing-fully-autonomous"
    ]


def test_extract_govuk_aps_links() -> None:
    html = """
    <main>
      <a href="/government/consultations/automated-passenger-services-permitting-scheme">Automated passenger services: permitting scheme</a>
      <a href="/government/speeches/automated-passenger-services-permitting-scheme-government-response">Government response</a>
    </main>
    """
    links = _extract_links_css(
        "https://www.gov.uk/search/all?keywords=automated%20passenger%20services",
        html,
        {"article_link": 'a[href*="/government/"][href*="automated-passenger-services"]'},
    )
    assert links == [
        "https://www.gov.uk/government/consultations/automated-passenger-services-permitting-scheme",
        "https://www.gov.uk/government/speeches/automated-passenger-services-permitting-scheme-government-response",
    ]


def test_extract_unece_site_specific_published() -> None:
    html = "<main><h1>UNECE adopts first ever global rules</h1><p>Published: 24 June 2026</p></main>"
    published, source = extract_site_specific_published(
        "unece_vehicle_regulations_structured",
        html,
        "https://unece.org/sustainable-development/press/unece-adopts-first-ever-global-rules-allowing-fully-autonomous",
    )
    assert published == "2026-06-24"
    assert source == "site_specific_date"


def test_extract_govuk_site_specific_published_prefers_last_updated() -> None:
    html = """
    <main>
      <h1>Automated passenger services: permitting scheme</h1>
      <p>Published 21 July 2025</p>
      <p>Last updated 23 April 2026</p>
    </main>
    """
    published, source = extract_site_specific_published(
        "govuk_automated_passenger_services_structured",
        html,
        "https://www.gov.uk/government/consultations/automated-passenger-services-permitting-scheme",
    )
    assert published == "2026-04-23"
    assert source == "site_specific_date"


def test_regulatory_site_rules_keep_only_autonomous_records() -> None:
    assert not is_invalid_structured_record(
        "unece_vehicle_regulations_structured",
        {
            "title": "UNECE adopts global technical regulation for automated driving systems",
            "content": "The rules cover fully autonomous vehicles.",
            "link": "https://unece.org/sustainable-development/press/unece-adopts-first-ever-global-rules-allowing-fully-autonomous",
        },
    )
    assert is_invalid_structured_record(
        "unece_vehicle_regulations_structured",
        {
            "title": "UNECE updates tyre regulation",
            "content": "The rule covers vehicle tyres and emissions.",
            "link": "https://unece.org/sustainable-development/press/tyre-rule",
        },
    )
    assert not is_invalid_structured_record(
        "govuk_automated_passenger_services_structured",
        {
            "title": "Automated passenger services permitting scheme",
            "content": "A route for self-driving passenger services with no user-in-charge.",
            "link": "https://www.gov.uk/government/consultations/automated-passenger-services-permitting-scheme",
        },
    )
    assert is_invalid_structured_record(
        "govuk_automated_passenger_services_structured",
        {
            "title": "Rail timetable policy update",
            "content": "A public transport policy note.",
            "link": "https://www.gov.uk/government/publications/rail-timetable-policy",
        },
    )


def test_govuk_article_css_prefers_site_specific_last_updated_date() -> None:
    html = """
    <main>
      <h1>Automated passenger services: permitting scheme</h1>
      <div class="gem-c-published-dates">
        <p>Published 21 July 2025</p>
        <p>Last updated 23 April 2026</p>
      </div>
      <div class="gem-c-govspeak"><p>Self-driving passenger services may operate without a safety driver.</p></div>
    </main>
    """
    record = _extract_article_css(
        "https://www.gov.uk/government/consultations/automated-passenger-services-permitting-scheme",
        html,
        {
            "title": "h1",
            "content": ".gem-c-govspeak p",
            "published": ".gem-c-published-dates time[datetime], time[datetime]",
        },
        "GOV.UK Automated Passenger Services",
        source_id="govuk_automated_passenger_services_structured",
    )
    assert record["published"] == "2026-04-23"



def test_discovery_date_extraction_prefers_govuk_last_updated() -> None:
    html = """
    <html>
      <head><meta property="article:published_time" content="2025-07-21"></head>
      <main>
        <h1>Automated passenger services: permitting scheme</h1>
        <p>Published 21 July 2025</p>
        <p>Last updated 23 April 2026</p>
      </main>
    </html>
    """
    published, source = _extract_date_from_html(
        html,
        "https://www.gov.uk/government/consultations/automated-passenger-services-permitting-scheme",
        "GOV.UK",
    )
    assert published == "2026-04-23"
    assert source == "site_specific_date"



def test_fetch_structured_source_reads_direct_article_urls(monkeypatch) -> None:
    list_url = "https://www.gov.uk/search/all?keywords=automated%20passenger%20services"
    article_url = "https://www.gov.uk/government/consultations/automated-passenger-services-permitting-scheme"
    pages = {
        list_url: b"<main></main>",
        article_url: b"""
        <main>
          <h1>Automated passenger services: permitting scheme</h1>
          <p>Last updated 23 April 2026</p>
          <div class="gem-c-govspeak"><p>Self-driving passenger services may operate without a safety driver.</p></div>
        </main>
        """,
    }

    def fake_http_get_bytes(url, *args, **kwargs):
        return pages[url]

    monkeypatch.setattr("app.fetch_structured.http_get_bytes", fake_http_get_bytes)

    rows, err = fetch_structured_source(
        {
            "id": "govuk_automated_passenger_services_structured",
            "name": "GOV.UK Automated Passenger Services",
            "source_type": "structured_web",
            "entry_urls": [list_url],
            "article_urls": [article_url],
            "selectors": {
                "article_link": 'a[href*="/government/"]',
                "title": "h1",
                "content": ".gem-c-govspeak p",
                "published": "time",
            },
            "max_items_per_run": 4,
        }
    )

    assert err == ""
    assert len(rows) == 1
    assert rows[0]["link"] == article_url
    assert rows[0]["published"] == "2026-04-23"
