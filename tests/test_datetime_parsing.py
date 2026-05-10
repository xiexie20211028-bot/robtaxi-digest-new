from app.common import parse_datetime_with_status


def test_parse_36kr_rss_pubdate_with_timezone() -> None:
    dt, status = parse_datetime_with_status("2026-03-08 16:43:17  +0800")
    assert status == "ok"
    assert dt.isoformat() == "2026-03-08T08:43:17+00:00"
