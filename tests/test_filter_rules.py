from datetime import datetime, timedelta, timezone

from app.filter_rules import _check_hard_constraints, _defaults, _resolve_prev_natural_day_window


def _source(**kwargs):
    base = {
        "source_type": "structured_web",
        "source_profile": "newsroom",
        "entry_urls": ["https://pony.ai/news/"],
        "url_allow_patterns": ["/news/"],
        "url_block_patterns": [],
        "external_link_allow_domains": ["pony-ai-blog.ghost.io", "prnewswire.com"],
    }
    base.update(kwargs)
    return base


def test_external_domain_not_allowed() -> None:
    settings = _defaults({"defaults": {}})
    start, end = _resolve_prev_natural_day_window("2026-03-09", "Asia/Shanghai")
    ok, reason, detail = _check_hard_constraints(
        {
            "link": "https://www.linkedin.com/feed/update/123",
            "published_at_utc": "2026-03-08T12:00:00+00:00",
            "published_missing": False,
            "published_parse_status": "ok",
            "region": "foreign",
        },
        _source(),
        settings,
        start,
        end,
    )
    assert not ok
    assert reason == "url_external_domain_not_allowed"
    assert detail["profile"] == "newsroom"


def test_allow_pattern_blocks_non_article_path() -> None:
    settings = _defaults({"defaults": {}})
    start, end = _resolve_prev_natural_day_window("2026-03-09", "Asia/Shanghai")
    ok, reason, _ = _check_hard_constraints(
        {
            "link": "https://pony.ai/about/",
            "published_at_utc": "2026-03-08T12:00:00+00:00",
            "published_missing": False,
            "published_parse_status": "ok",
            "region": "foreign",
        },
        _source(),
        settings,
        start,
        end,
    )
    assert not ok
    assert reason == "url_not_in_allow_patterns"


def test_registered_pony_blog_subdomain_uses_its_own_article_root() -> None:
    settings = _defaults({"defaults": {}})
    start, end = _resolve_prev_natural_day_window("2026-03-09", "Asia/Shanghai")
    ok, reason, detail = _check_hard_constraints(
        {
            "link": "https://blog.pony.ai/pony-ai-announces-new-robotaxi-service",
            "published_at_utc": "2026-03-08T12:00:00+00:00",
            "published_missing": False,
            "published_parse_status": "ok",
            "region": "foreign",
        },
        _source(
            entry_urls=["https://pony.ai/press?lang=en"],
            url_allow_patterns=["/press"],
            external_link_allow_domains=["blog.pony.ai"],
        ),
        settings,
        start,
        end,
    )
    assert ok
    assert reason == ""
    assert detail["normalized_url"] == "https://blog.pony.ai/pony-ai-announces-new-robotaxi-service"


def test_unregistered_pony_subdomain_is_not_allowed() -> None:
    settings = _defaults({"defaults": {}})
    start, end = _resolve_prev_natural_day_window("2026-03-09", "Asia/Shanghai")
    ok, reason, _ = _check_hard_constraints(
        {
            "link": "https://news.pony.ai/robotaxi-service",
            "published_at_utc": "2026-03-08T12:00:00+00:00",
            "published_missing": False,
            "published_parse_status": "ok",
            "region": "foreign",
        },
        _source(
            entry_urls=["https://pony.ai/press?lang=en"],
            url_allow_patterns=["/press"],
            external_link_allow_domains=["blog.pony.ai"],
        ),
        settings,
        start,
        end,
    )
    assert not ok
    assert reason == "url_not_in_allow_patterns"
