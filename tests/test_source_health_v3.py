from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.source_health import update_source_health_history


def _stat() -> dict:
    return {
        "source_id": "required-source",
        "status": "healthy",
        "criticality": "required",
        "listed_items": 0,
        "valid_items": 0,
        "date_parse_rate": 1.0,
        "whitelist_reject_rate": 0.0,
        "health_policy": {"empty_listing_limit": 3},
    }


def _recent_dates() -> list[str]:
    today = datetime.now(timezone.utc).date()
    return [(today - timedelta(days=offset)).isoformat() for offset in (2, 1, 0)]


def test_empty_list_is_healthy_until_it_becomes_silent_dead(tmp_path: Path) -> None:
    history = tmp_path / "source-health.json"
    first, _ = update_source_health_history([_stat()], history, _recent_dates()[0])
    second, _ = update_source_health_history([_stat()], history, _recent_dates()[1])
    third, rolling = update_source_health_history([_stat()], history, _recent_dates()[2])
    assert first[0]["status"] == "healthy"
    assert second[0]["status"] == "healthy"
    assert third[0]["status"] == "silent_dead"
    assert rolling["history_days"] == 3


def test_failure_escalation_counter_is_persisted(tmp_path: Path) -> None:
    history = tmp_path / "source-health.json"
    failed = {**_stat(), "status": "failed"}
    update_source_health_history([failed], history, _recent_dates()[0])
    second, _ = update_source_health_history([failed], history, _recent_dates()[1])
    third, _ = update_source_health_history([failed], history, _recent_dates()[2])
    assert second[0]["consecutive_failures"] == 2
    assert third[0]["consecutive_failures"] == 3
