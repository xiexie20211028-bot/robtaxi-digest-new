from app.report import METHOD_ORDER, default_report, empty_stage_funnel


def test_stage_funnel_methods_are_complete() -> None:
    funnel = empty_stage_funnel()
    assert list(funnel.keys()) == METHOD_ORDER
    for method in METHOD_ORDER:
        assert funnel[method] == {"fetched": 0, "candidate": 0, "filtered": 0, "kept": 0}


def test_dashboard_funnel_conservation_example() -> None:
    report = default_report()
    report["stage_funnel"] = {
        "rss": {"fetched": 10, "candidate": 8, "filtered": 8, "kept": 0},
        "structured_web": {"fetched": 6, "candidate": 5, "filtered": 4, "kept": 1},
        "search_result": {"fetched": 3, "candidate": 3, "filtered": 2, "kept": 1},
        "official_api": {"fetched": 1, "candidate": 1, "filtered": 1, "kept": 0},
        "search_api": {"fetched": 0, "candidate": 0, "filtered": 0, "kept": 0},
    }
    report["pre_candidate_drop_total"] = 3
    fetched = sum(item["fetched"] for item in report["stage_funnel"].values())
    candidate = sum(item["candidate"] for item in report["stage_funnel"].values())
    filtered = sum(item["filtered"] for item in report["stage_funnel"].values())
    kept = sum(item["kept"] for item in report["stage_funnel"].values())
    assert fetched - report["pre_candidate_drop_total"] == candidate
    assert candidate - filtered == kept
