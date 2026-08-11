from pathlib import Path

from app.source_config import load_source_config, source_metadata


ROOT = Path(__file__).resolve().parents[1]


def test_profiles_are_resolved_without_losing_rollback() -> None:
    legacy, legacy_name = load_source_config(ROOT / "sources.json", "legacy")
    optimized, optimized_name = load_source_config(ROOT / "sources.json", "optimized")
    assert legacy_name == "legacy"
    assert optimized_name == "optimized"
    assert legacy["defaults"]["scope_mode"] == "legacy"
    assert optimized["defaults"]["scope_mode"] == "passenger_l3_l4"
    assert optimized["defaults"]["late_arrival_max_items"] == 2
    assert len([source for source in optimized["sources"] if source["enabled"]]) > 0


def test_every_source_has_structured_v3_metadata() -> None:
    cfg, _ = load_source_config(ROOT / "sources.json", "optimized")
    for source in cfg["sources"]:
        metadata = source_metadata(source)
        assert metadata["source_role"]
        assert metadata["evidence_type"]
        assert metadata["criticality"]
        assert metadata["coverage_domains"]
        assert source["transport"]["index"]
        assert source["transport"]["article"]
        assert isinstance(source["official_accounts"], dict)


def test_serpapi_is_optional_and_disabled_by_default() -> None:
    cfg, _ = load_source_config(ROOT / "sources.json", "optimized")
    assert cfg["search_providers"]["serpapi"]["enabled"] is False
    assert cfg["search_providers"]["serpapi"]["missing_key_alert"] is False

