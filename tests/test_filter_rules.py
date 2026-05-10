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
