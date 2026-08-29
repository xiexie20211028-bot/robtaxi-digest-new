from app.decision_log import DECISION_LOG_SCHEMA_VERSION, build_candidate_decision


def test_candidate_decision_has_cross_route_required_fields() -> None:
    record = build_candidate_decision(
        route="legacy",
        candidate={"id": "candidate-1", "title": "道路交通安全法修订草案", "canonical_url": "https://npc.gov.cn/law"},
        source={"source_name": "全国人大", "source_id": "npc"},
        stage="stage2",
        kept=False,
        final_reason="score_below_threshold",
        signals={"national_legislation_hits": ["修订草案"]},
        score=60,
        threshold=65,
    )

    assert record == {
        "schema_version": DECISION_LOG_SCHEMA_VERSION,
        "route": "legacy",
        "candidate_id": "candidate-1",
        "title": "道路交通安全法修订草案",
        "source": "全国人大",
        "source_id": "npc",
        "canonical_url": "https://npc.gov.cn/law",
        "stage": "stage2",
        "signals": {"national_legislation_hits": ["修订草案"]},
        "score": 60.0,
        "threshold": 65.0,
        "kept": False,
        "final_reason": "score_below_threshold",
    }
