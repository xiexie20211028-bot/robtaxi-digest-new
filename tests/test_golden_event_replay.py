from pathlib import Path

from scripts.replay_golden_event import load_event, replay_event


FIXTURE = Path(__file__).parent / "fixtures" / "golden_events" / "road_traffic_safety_law_2026-08-25.json"


def test_road_traffic_safety_law_fixture_has_independent_route_payloads() -> None:
    event = load_event(FIXTURE)
    report = replay_event(event)

    assert report["independent_candidate_payloads"] == 3
    assert set(report["results"]) == {"legacy", "optimized", "agent_first"}
    assert event["expected"]["must_not_require_literal_terms"] == ["Robotaxi", "L3", "L4"]


def test_road_traffic_safety_law_replay_preserves_current_common_miss_baseline() -> None:
    report = replay_event(load_event(FIXTURE))

    assert report["discoveries"] == 0
    assert report["acceptance_met"] is False
    assert {value["scope_reason"] for value in report["results"].values()} == {"scope_gate_miss"}
