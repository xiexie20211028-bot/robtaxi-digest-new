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


def test_empty_list_is_healthy_until_it_becomes_silent_dead(tmp_path: Path) -> None:
    history = tmp_path / "source-health.json"
    first, _ = update_source_health_history([_stat()], history, "2026-08-09")
    second, _ = update_source_health_history([_stat()], history, "2026-08-10")
    third, rolling = update_source_health_history([_stat()], history, "2026-08-11")
    assert first[0]["status"] == "healthy"
    assert second[0]["status"] == "healthy"
    assert third[0]["status"] == "silent_dead"
    assert rolling["history_days"] == 3


def test_failure_escalation_counter_is_persisted(tmp_path: Path) -> None:
    history = tmp_path / "source-health.json"
    failed = {**_stat(), "status": "failed"}
    update_source_health_history([failed], history, "2026-08-09")
    second, _ = update_source_health_history([failed], history, "2026-08-10")
    third, _ = update_source_health_history([failed], history, "2026-08-11")
    assert second[0]["consecutive_failures"] == 2
    assert third[0]["consecutive_failures"] == 3
