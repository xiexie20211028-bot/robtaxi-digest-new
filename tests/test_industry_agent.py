from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from app.common import read_jsonl
from app.industry_agent.approval import validate_approval
from app.industry_agent.contracts import AgentEvent, Evidence, ProviderUsage, SearchResearchResult
from app.industry_agent.import_events import event_to_raw, import_events
from app.industry_agent.providers import (
    DeepSeekWebSearchProvider,
    _bounded_output_tokens,
    _validated_deepseek_api_key,
    extract_json_object,
)
from app.industry_agent.review import _find, evaluate_agent_rollout, run_review
from app.industry_agent.runtime_profile import resolve_runtime_profile
from app.industry_agent.runner import run_agent
from app.industry_agent.verifier import DefaultEvidenceVerifier
from app.parse import canonicalize_row
from app.source_config import load_source_config
from app.summarize import dedupe_l3


ROOT = Path(__file__).resolve().parents[1]


class FakePageReader:
    def __init__(self, rows: dict[str, dict]) -> None:
        self.rows = rows

    def read(self, url: str) -> dict:
        return dict(self.rows.get(url, {"ok": False, "url": url}))


class FakeSearchProvider:
    name = "fake_search"

    def __init__(self, cost: float = 0.1) -> None:
        self.cost = cost
        self.calls = 0

    def probe(self, max_cost_cny: float | None = None):
        _ = max_cost_cny
        return True, ProviderUsage(web_searches=1, estimated_cost_cny=0.01), []

    def research(
        self,
        system_prompt: str,
        user_prompt: str,
        max_searches: int,
        max_cost_cny: float | None = None,
    ) -> SearchResearchResult:
        _ = (system_prompt, user_prompt, max_searches, max_cost_cny)
        self.calls += 1
        payload = {"events": [_candidate("https://www.miit.gov.cn/news/l3.html")]} if self.calls == 1 else {"events": []}
        return SearchResearchResult(
            text=json.dumps(payload, ensure_ascii=False),
            usage=ProviderUsage(web_searches=1, estimated_cost_cny=self.cost),
            capability_confirmed=True,
        )


class FakeModelProvider:
    name = "fake_model"

    def complete_json(self, system_prompt: str, user_prompt: str, max_cost_cny: float | None = None):
        _ = (system_prompt, user_prompt, max_cost_cny)
        return {"events": []}, ProviderUsage(estimated_cost_cny=0.01)


class FakeVerifier:
    def verify(self, candidate: dict, run_date: str, agent_run_id: str):
        url = str(candidate["canonical_url"])
        return (
            AgentEvent(
                event_id="event-1",
                title=str(candidate["title"]),
                factual_summary=str(candidate["factual_summary"]),
                companies=["xpeng"],
                coverage_domains=["passenger_l3"],
                automation_level="L3",
                event_type="approval",
                deployment_stage="approved",
                published_at_utc="2026-08-12T03:00:00+00:00",
                first_seen_at_utc="2026-08-13T00:00:00+00:00",
                late_arrival=False,
                importance_score=88,
                score_breakdown=dict(candidate["score_breakdown"]),
                canonical_url=url,
                evidence=[Evidence(url=url, publisher="工信部", evidence_type="regulator", accessible=True, date_verified=True)],
                verification_status="verified_primary",
                agent_run_id=agent_run_id,
                model_provider="fake_model",
                search_provider="fake_search",
            ),
            "verified",
        )


def _candidate(url: str) -> dict:
    return {
        "title": "工信部批准 L3 乘用车开展上路试点",
        "factual_summary": "工信部批准一款 L3 乘用车进入有条件自动驾驶上路试点。",
        "companies": ["小鹏汽车"],
        "coverage_domains": ["passenger_l3", "regulation_safety"],
        "automation_level": "L3",
        "event_type": "approval",
        "deployment_stage": "approved",
        "canonical_url": url,
        "evidence": [{"url": url, "evidence_type": "regulator"}],
        "score_breakdown": {
            "industry_impact": 24,
            "deployment_or_regulation": 22,
            "scope_relevance": 22,
            "evidence_quality": 20,
        },
    }


def test_extract_json_object_and_web_search_blocks() -> None:
    assert extract_json_object("```json\n{\"events\": []}\n```") == {"events": []}
    response = DeepSeekWebSearchProvider._parse_response(
        {
            "content": [
                {"type": "server_tool_use", "name": "web_search", "input": {"query": "L3 准入"}},
                {
                    "type": "web_search_tool_result",
                    "content": [{"type": "web_search_result", "url": "https://example.com/a"}],
                },
                {"type": "text", "text": '{"events":[]}'},
            ]
        }
    )
    assert response.capability_confirmed is True
    assert response.usage.web_searches == 1
    assert response.text == '{"events":[]}'
    assert response.trace[1]["urls"] == ["https://example.com/a"]


def test_deepseek_api_key_rejects_non_ascii_secret_before_network() -> None:
    assert _validated_deepseek_api_key("test-key") == "test-key"
    with pytest.raises(RuntimeError, match="printable ASCII"):
        _validated_deepseek_api_key("已授权并复制")


def test_review_workflow_downloads_failed_runs_with_preserved_artifacts() -> None:
    workflow = (ROOT / ".github/workflows/robtaxi-agent-review.yml").read_text(encoding="utf-8")
    assert "--status completed" in workflow
    assert "select(.conclusion == \"success\")" not in workflow


def test_review_prefers_newer_github_run_id_over_download_mtime(tmp_path: Path) -> None:
    old = tmp_path / "100" / "artifact" / "2026-08-13" / "agent_run_report.json"
    new = tmp_path / "200" / "artifact" / "2026-08-13" / "agent_run_report.json"
    old.parent.mkdir(parents=True)
    new.parent.mkdir(parents=True)
    old.write_text("{}", encoding="utf-8")
    new.write_text("{}", encoding="utf-8")
    # 模拟“新 run 先下载、旧 run 后下载”，故意让旧 run 的 mtime 更新。
    old.touch()
    assert old.stat().st_mtime >= new.stat().st_mtime
    assert _find(tmp_path, "agent_run_report.json", "2026-08-13") == new


def test_provider_rejects_request_before_call_when_remaining_budget_is_too_low() -> None:
    prices = {
        "input_cache_miss_cny_per_million": 1.0,
        "output_cny_per_million": 2.0,
    }
    with pytest.raises(RuntimeError, match="budget_preflight_rejected"):
        _bounded_output_tokens(prices, 12000, max_cost_cny=0.10)
    assert _bounded_output_tokens(prices, 12000, max_cost_cny=0.20) == 12000


def test_runner_fails_closed_if_provider_exceeds_hard_budget(tmp_path: Path) -> None:
    config = json.loads((ROOT / "sources.json").read_text(encoding="utf-8"))
    config["industry_agent"]["daily_budget_cny"] = 2.0
    report = run_agent(
        "2026-08-13",
        config,
        tmp_path / "out",
        tmp_path / "state",
        model_provider=FakeModelProvider(),
        search_provider=FakeSearchProvider(cost=2.1),
        verifier=FakeVerifier(),
    )
    assert report["status"] == "failed"
    assert report["verified_event_count"] == 0
    assert report["usage"]["web_searches"] == 2
    assert any("hard_budget_exceeded" in value for value in report["errors"])
    assert read_jsonl(tmp_path / "out" / "2026-08-13" / "agent_events.jsonl") == []


def test_runner_marks_non_json_search_output_failed(tmp_path: Path) -> None:
    class InvalidJsonSearchProvider(FakeSearchProvider):
        def research(
            self,
            system_prompt: str,
            user_prompt: str,
            max_searches: int,
            max_cost_cny: float | None = None,
        ) -> SearchResearchResult:
            _ = (system_prompt, user_prompt, max_searches, max_cost_cny)
            return SearchResearchResult(
                text="not-json-at-all",
                usage=ProviderUsage(web_searches=1, estimated_cost_cny=0.05),
                capability_confirmed=True,
            )

    config = json.loads((ROOT / "sources.json").read_text(encoding="utf-8"))
    report = run_agent(
        "2026-08-13",
        config,
        tmp_path / "out",
        tmp_path / "state",
        model_provider=FakeModelProvider(),
        search_provider=InvalidJsonSearchProvider(),
        verifier=FakeVerifier(),
    )
    assert report["status"] == "failed"
    assert report["candidate_count"] == 0
    assert any("scan_invalid_output" in value for value in report["errors"])


def test_verifier_accepts_primary_evidence_and_rejects_single_media() -> None:
    official_url = "https://www.miit.gov.cn/news/l3.html"
    media_url = "https://media.example.com/l3.html"
    reader = FakePageReader(
        {
            official_url: {
                "ok": True,
                "canonical_url": official_url,
                "publisher": "工信部",
                "published_at_utc": "2026-08-12T03:00:00+00:00",
                "title": "工信部批准小鹏汽车 L3 乘用车准入试点",
                "content": "小鹏汽车 L3 有条件自动驾驶乘用车准入试点正式获批。",
            },
            media_url: {
                "ok": True,
                "canonical_url": media_url,
                "publisher": "单一媒体",
                "published_at_utc": "2026-08-12T03:00:00+00:00",
                "title": "小鹏汽车 L3 乘用车获批",
                "content": "小鹏汽车 L3 有条件自动驾驶乘用车获批。",
            },
        }
    )
    config = {
        "sources": [
            {
                "name": "工信部",
                "evidence_type": "regulator",
                "entry_urls": ["https://www.miit.gov.cn/zwgk/"],
                "official_accounts": {"domains": ["miit.gov.cn"]},
            }
        ],
        "companies": [{"id": "xpeng", "name": "小鹏汽车", "aliases": ["xpeng", "小鹏"]}],
    }
    verifier = DefaultEvidenceVerifier(reader, config)
    event, reason = verifier.verify(_candidate(official_url), "2026-08-13", "run-1")
    assert reason == "verified"
    assert event is not None
    assert event.verification_status == "verified_primary"
    assert event.importance_score == 88

    media_candidate = _candidate(media_url)
    media_candidate["evidence"][0]["evidence_type"] = "industry_media"
    event, reason = verifier.verify(media_candidate, "2026-08-13", "run-1")
    assert event is None
    assert reason == "insufficient_independent_evidence"


def test_verifier_rejects_official_page_that_does_not_support_claim() -> None:
    url = "https://www.miit.gov.cn/unrelated-spectrum.html"
    reader = FakePageReader(
        {
            url: {
                "ok": True,
                "canonical_url": url,
                "publisher": "工信部",
                "published_at_utc": "2026-08-12T03:00:00+00:00",
                "title": "无线电频谱管理通知",
                "content": "本通知只涉及通信频谱分配，与汽车、自动驾驶和准入无关。",
            }
        }
    )
    config = {
        "companies": [{"id": "xpeng", "name": "小鹏汽车", "aliases": ["xpeng", "小鹏"]}],
        "sources": [
            {
                "name": "工信部",
                "evidence_type": "regulator",
                "entry_urls": ["https://www.miit.gov.cn/zwgk/"],
                "official_accounts": {"domains": ["miit.gov.cn"]},
            }
        ],
    }
    event, reason = DefaultEvidenceVerifier(reader, config).verify(_candidate(url), "2026-08-13", "run-1")
    assert event is None
    assert reason == "evidence_content_mismatch"


def test_agent_event_import_preserves_discovery_and_evidence(tmp_path: Path) -> None:
    event = {
        "agent_run_id": "run-1",
        "title": "工信部批准 L3 乘用车开展上路试点",
        "factual_summary": "L3 乘用车获批上路。",
        "companies": ["xpeng"],
        "coverage_domains": ["passenger_l3", "regulation_safety"],
        "published_at_utc": "2026-08-12T03:00:00+00:00",
        "first_seen_at_utc": "2026-08-13T00:10:00+00:00",
        "canonical_url": "https://www.miit.gov.cn/news/l3.html",
        "importance_score": 88,
        "score_breakdown": {},
        "verification_status": "verified_primary",
        "evidence": [
            {
                "url": "https://www.miit.gov.cn/news/l3.html",
                "publisher": "工信部",
                "evidence_type": "regulator",
                "accessible": True,
                "date_verified": True,
            }
        ],
    }
    raw = event_to_raw(event)
    assert raw is not None
    canonical = canonicalize_row(raw)
    assert canonical is not None
    assert canonical.discovery_method == "agent_search"
    assert canonical.evidence_type == "regulator"
    assert canonical.agent_importance_score == 88
    assert canonical.evidence[0]["publisher"] == "工信部"

    handoff = tmp_path / "handoff" / "2026-08-13"
    raw_root = tmp_path / "raw"
    report_root = tmp_path / "reports"
    handoff.mkdir(parents=True)
    (handoff / "agent_events.jsonl").write_text(json.dumps(event, ensure_ascii=False) + "\n", encoding="utf-8")
    (handoff / "agent_run_report.json").write_text(json.dumps({"status": "success"}), encoding="utf-8")
    result = import_events("2026-08-13", "agent_domestic", tmp_path / "handoff", raw_root, report_root)
    assert result["imported"] == 1
    assert read_jsonl(raw_root / "2026-08-13" / "raw_items.jsonl")[0]["source_type"] == "agent_event"


def test_agent_cluster_uses_one_digest_slot_and_prefers_direct_primary() -> None:
    common = {
        "title": "工信部批准 L3 乘用车开展上路试点",
        "content": "工信部批准一款 L3 乘用车进入有条件自动驾驶上路试点。",
        "canonical_url": "https://www.miit.gov.cn/news/l3.html",
        "company_hint": "xpeng",
        "event_type": "approval",
        "evidence_type": "regulator",
        "relevance_score": 90,
        "published_at_utc": "2026-08-12T03:00:00+00:00",
    }
    agent = {**common, "source_id": "agent_miit", "discovery_method": "agent_search"}
    direct = {**common, "source_id": "miit_news_structured", "discovery_method": "direct_source"}
    selected, dropped = dedupe_l3([agent, direct])
    assert dropped == 1
    assert len(selected) == 1
    assert selected[0]["source_id"] == "miit_news_structured"


def test_runtime_profile_rolls_back_only_after_two_failures(tmp_path: Path) -> None:
    state = tmp_path / "runtime.json"
    report = tmp_path / "agent.json"
    report.write_text(json.dumps({"status": "failed"}), encoding="utf-8")
    first = resolve_runtime_profile("2026-08-13", "agent_domestic", report, state)
    assert first["effective_profile"] == "agent_domestic"
    second = resolve_runtime_profile("2026-08-14", "agent_domestic", report, state)
    assert second["effective_profile"] == "legacy"
    assert second["fallback_active"] is True


def test_runtime_profile_same_day_rerun_does_not_increment_failure_day(tmp_path: Path) -> None:
    state = tmp_path / "runtime.json"
    missing_report = tmp_path / "missing.json"
    first = resolve_runtime_profile("2026-08-13", "agent_domestic", missing_report, state)
    rerun = resolve_runtime_profile("2026-08-13", "agent_domestic", missing_report, state)
    assert first["consecutive_failures"] == 1
    assert rerun["consecutive_failures"] == 1
    assert rerun["effective_profile"] == "agent_domestic"


def test_agent_domestic_profile_keeps_only_verified_domestic_regulators() -> None:
    legacy, _ = load_source_config(ROOT / "sources.json", "legacy")
    agent, _ = load_source_config(ROOT / "sources.json", "agent_domestic")
    legacy_foreign = {row["id"] for row in legacy["sources"] if row["enabled"] and row["region"] == "foreign"}
    agent_foreign = {row["id"] for row in agent["sources"] if row["enabled"] and row["region"] == "foreign"}
    agent_domestic = [row for row in agent["sources"] if row["enabled"] and row["region"] == "domestic"]
    assert agent_foreign == legacy_foreign
    assert len(agent_domestic) == 9
    assert all(row["evidence_type"] == "regulator" for row in agent_domestic)
    assert "mps_traffic_bureau_structured" not in {row["id"] for row in agent_domestic}


def test_rollout_gate_and_manual_approval() -> None:
    days = []
    for index in range(14):
        days.append(
            {
                "date": f"2026-08-{index + 1:02d}",
                "agent_status": "success",
                "truth_important": 10,
                "agent_true_positive": 9,
                "agent_selected": 10,
                "legacy_important": 8,
                "legacy_reproduced": 8,
                "agent_url_date_verified": 10,
                "agent_strong_evidence": 10,
                "agent_cost_cny": 0.8,
                "valid_statistical_day": True,
            }
        )
    gate = evaluate_agent_rollout({"days": days}, {})
    assert gate["ready_for_manual_approval"] is True
    sample_ids = [f"ev_{index}" for index in range(20)]
    marker = '<!-- agent-review ' + json.dumps(
        {
            "review_id": "ar_1",
            "ready_for_manual_approval": True,
            "manual_sample_count": 20,
            "manual_sample_event_ids": sample_ids,
        },
        separators=(",", ":"),
    ) + ' -->'
    approved = validate_approval("/agent-review approve ar_1 reject=ev_1", marker)
    assert approved["approved"] is True
    rejected = validate_approval("/agent-review approve ar_1 reject=ev_1,ev_2,ev_3", marker)
    assert rejected["approved"] is False
    assert rejected["reason"] == "manual_overturn_rate_too_high"


def test_rollout_gate_does_not_count_degraded_days_as_success() -> None:
    days = []
    for index in range(14):
        days.append(
            {
                "date": f"2026-08-{index + 1:02d}",
                "agent_status": "degraded" if index < 2 else "success",
                "truth_important": 1,
                "agent_true_positive": 1,
                "agent_selected": 1,
                "legacy_important": 1,
                "legacy_reproduced": 1,
                "agent_url_date_verified": 1,
                "agent_strong_evidence": 1,
                "agent_cost_cny": 0.5,
                "valid_statistical_day": True,
            }
        )
    gate = evaluate_agent_rollout({"days": days}, {})
    assert gate["metrics"]["success_days"] == 12
    assert gate["automatic_checks"]["success_days"] is False
    assert gate["automatic_checks"]["no_consecutive_two_day_failure"] is True


def test_rollout_gate_and_approval_reject_zero_evidence() -> None:
    days = [
        {
            "date": f"2026-08-{index + 1:02d}",
            "valid_statistical_day": True,
            "agent_status": "success_empty",
            "truth_important": 0,
            "agent_true_positive": 0,
            "agent_selected": 0,
            "legacy_important": 0,
            "legacy_reproduced": 0,
            "agent_url_date_verified": 0,
            "agent_strong_evidence": 0,
            "agent_cost_cny": 0.0,
        }
        for index in range(14)
    ]
    gate = evaluate_agent_rollout({"days": days}, {})
    assert gate["ready_for_manual_approval"] is False
    assert gate["automatic_checks"]["evaluation_volume"] is False
    assert gate["metrics"]["important_recall"] == 0.0

    marker = '<!-- agent-review {"review_id":"ar_empty","ready_for_manual_approval":true,"manual_sample_count":0,"manual_sample_event_ids":[]} -->'
    approval = validate_approval("/agent-review approve ar_empty reject=made_up", marker)
    assert approval == {"approved": False, "reason": "manual_samples_missing"}


class CompleteReviewModel:
    name = "complete_review"

    def complete_json(self, system_prompt: str, user_prompt: str, max_cost_cny: float | None = None):
        _ = (system_prompt, max_cost_cny)
        event_ids = sorted(set(re.findall(r'"blind_id":\s*"(ev_[a-f0-9]+)"', user_prompt)))
        return (
            {
                "events": [
                    {"blind_id": event_id, "in_scope": True, "important": True, "reason": "verified"}
                    for event_id in event_ids
                ]
            },
            ProviderUsage(input_tokens=100, output_tokens=20),
        )


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _review_event(title: str, url: str, first_seen: str) -> dict:
    return {
        "title": title,
        "factual_summary": f"{title}。",
        "canonical_url": url,
        "published_at_utc": "2026-08-12T03:00:00+00:00",
        "first_seen_at_utc": first_seen,
        "coverage_domains": ["passenger_l3"],
        "importance_score": 90,
        "evidence": [
            {
                "url": url,
                "publisher": "工信部",
                "evidence_type": "regulator",
                "accessible": True,
                "date_verified": True,
            }
        ],
    }


def test_review_requires_complete_artifacts_and_merges_next_day_lookback(tmp_path: Path) -> None:
    agent_root = tmp_path / "agent"
    legacy_root = tmp_path / "legacy"
    optimized_root = tmp_path / "optimized"
    base_date = "2026-08-13"
    lookback_date = "2026-08-14"
    common = _review_event("小鹏 L3 准入试点获批", "https://www.miit.gov.cn/a", "2026-08-13T00:10:00+00:00")
    late = _review_event("另一款 L3 乘用车次日补录", "https://www.miit.gov.cn/b", "2026-08-14T00:10:00+00:00")

    _write_jsonl(agent_root / base_date / "agent_events.jsonl", [common])
    _write_jsonl(agent_root / lookback_date / "agent_events.jsonl", [late])
    _write_jsonl(legacy_root / base_date / "brief_items.jsonl", [{"title": common["title"], **common}])
    _write_jsonl(legacy_root / lookback_date / "brief_items.jsonl", [])
    _write_jsonl(optimized_root / base_date / "brief_items.jsonl", [{"title": common["title"], **common}])
    _write_jsonl(optimized_root / lookback_date / "brief_items.jsonl", [])
    for root, run_date in ((agent_root, base_date), (agent_root, lookback_date)):
        (root / run_date / "agent_run_report.json").write_text(
            json.dumps({"status": "success", "usage": {"estimated_cost_cny": 0.5}}), encoding="utf-8"
        )
    for run_date in (base_date, lookback_date):
        (legacy_root / run_date / "run_report.json").write_text(json.dumps({"source_stats": []}), encoding="utf-8")

    config = json.loads((ROOT / "sources.json").read_text(encoding="utf-8"))
    output = run_review(
        base_date,
        config,
        agent_root,
        legacy_root,
        optimized_root,
        tmp_path / "history.json",
        tmp_path / "out",
        CompleteReviewModel(),
        lookback_date=lookback_date,
    )
    assert output["daily"]["valid_statistical_day"] is True
    assert output["daily"]["lookback_event_count"] == 1
    assert output["daily"]["truth_important"] == 2
    assert output["history_days"] == 1

    missing = run_review(
        base_date,
        config,
        agent_root,
        legacy_root,
        tmp_path / "missing-optimized",
        tmp_path / "missing-history.json",
        tmp_path / "missing-out",
        CompleteReviewModel(),
        lookback_date=lookback_date,
    )
    assert missing["daily"]["valid_statistical_day"] is False
    assert missing["history_days"] == 0

    fallback = run_review(
        base_date,
        config,
        agent_root,
        legacy_root,
        optimized_root,
        tmp_path / "fallback-history.json",
        tmp_path / "fallback-out",
        None,
        lookback_date=lookback_date,
    )
    assert fallback["judge"]["fallback"] is True
    assert fallback["daily"]["valid_statistical_day"] is False
    assert fallback["history_days"] == 0
