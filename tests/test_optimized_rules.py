from datetime import datetime, timedelta, timezone

from app.filter_rules import _check_hard_constraints, _defaults, _resolve_prev_natural_day_window
from app.render import select_digest_items
from app.parse import canonicalize_row, update_first_seen_state


def test_72_hour_first_discovery_can_enter_late_pool() -> None:
    cfg = {
        "defaults": {
            "late_arrival_enabled": True,
            "late_arrival_hours": 72,
            "drop_if_published_missing": True,
            "drop_if_published_unparseable": True,
        }
    }
    settings = _defaults(cfg)
    start, end = _resolve_prev_natural_day_window("2026-08-11", "Asia/Shanghai")
    row = {
        "link": "https://example.com/news/l4-approval",
        "published_at_utc": "2026-08-09T10:00:00+00:00",
        "published_missing": False,
        "published_parse_status": "ok",
        "first_seen_at_utc": "2026-08-10T18:00:00+00:00",
    }
    ok, reason, detail = _check_hard_constraints(row, {"entry_urls": ["https://example.com/news"]}, settings, start, end)
    assert ok is True
    assert reason == ""
    assert detail["late_arrival"] is True


def _item(index: int, *, role: str = "secondary", region: str = "domestic") -> dict:
    domain = ("robotaxi", "passenger_l3", "passenger_l4", "core_supply_chain", "regulation_safety")[index % 5]
    return {
        "title_zh": f"company{index} unique{index} milestone{index}",
        "published_at_utc": datetime(2026, 8, 10, index % 20, tzinfo=timezone.utc).isoformat(),
        "importance": 5 - index % 3,
        "relevance_score": 90 - index,
        "evidence_type": "regulator" if index < 2 else "industry_media",
        "source_role": role,
        "source_id": f"source-{index // 2}",
        "company_id": f"company-{index // 2}",
        "region": region,
        "coverage_domains": [domain],
    }


def test_final_selection_enforces_caps_and_soft_balance() -> None:
    rows = [
        _item(index, role="search_discovery" if index < 6 else "secondary", region="domestic" if index % 2 == 0 else "foreign")
        for index in range(24)
    ]
    selected = select_digest_items(
        rows,
        {
            "top_n": 12,
            "per_company_cap": 2,
            "per_source_cap": 2,
            "discovery_direct_share_cap": 0.25,
            "region_soft_max_share": 0.60,
        },
    )
    assert len(selected) == 12
    assert sum(item["source_role"] == "search_discovery" for item in selected) <= 3
    assert max(sum(item["source_id"] == source for item in selected) for source in {item["source_id"] for item in selected}) <= 2
    assert max(sum(item["company_id"] == company for item in selected) for company in {item["company_id"] for item in selected}) <= 2
    assert sum(item["region"] == "domestic" for item in selected) <= 7
    assert sum(item["region"] == "foreign" for item in selected) <= 7


def test_first_seen_time_is_stable_across_runs(tmp_path) -> None:
    def raw(fetched_at: str) -> dict:
        return {
            "source_id": "official",
            "source_name": "Official",
            "source_type": "rss",
            "region": "foreign",
            "company_hint": "waymo",
            "fetched_at": fetched_at,
            "url": "https://example.com/news/robotaxi",
            "payload": {
                "title": "Waymo Robotaxi expands service",
                "link": "https://example.com/news/robotaxi",
                "published": "2026-08-09T10:00:00+00:00",
            },
        }

    first_seen = datetime.now(timezone.utc).replace(microsecond=0)
    first = canonicalize_row(raw(first_seen.isoformat()))
    second = canonicalize_row(raw((first_seen + timedelta(days=1)).isoformat()))
    assert first is not None and second is not None
    state = tmp_path / "first-seen.json"
    update_first_seen_state([first], state)
    update_first_seen_state([second], state)
    assert second.first_seen_at_utc == first_seen.isoformat()
