import json
from pathlib import Path

from app.rollout_gate import evaluate_rollout_gate


def test_rollout_gate_passes_only_with_14_days_and_all_thresholds() -> None:
    golden = json.loads((Path(__file__).parent / "fixtures" / "golden_scope.json").read_text(encoding="utf-8"))
    source_days = []
    quality_days = []
    for day in range(1, 15):
        date = f"2026-08-{day:02d}"
        source_days.append(
            {
                "date": date,
                "sources": [
                    {
                        "source_id": "p0",
                        "criticality": "required",
                        "status": "healthy",
                        "request_count": 1,
                        "request_success_count": 1,
                        "valid_items": 5,
                        "body_parse_rate": 1.0,
                        "date_parse_rate": 1.0,
                    }
                ],
            }
        )
        quality_days.append(
            {
                "date": date,
                "metrics": {
                    "items": 10,
                    "primary_source_share": 0.7,
                    "discovery_dependency_share": 0.2,
                },
            }
        )
    result = evaluate_rollout_gate({"days": source_days}, {"days": quality_days}, golden, min_days=14)
    assert result["passed"] is True
    assert all(result["checks"].values())


def test_rollout_gate_does_not_switch_early() -> None:
    result = evaluate_rollout_gate({"days": []}, {"days": []}, {"positive": [], "negative": []}, min_days=14)
    assert result["passed"] is False
    assert result["checks"]["minimum_shadow_days"] is False

