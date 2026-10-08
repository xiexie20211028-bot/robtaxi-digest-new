"""双路生产的故障与来源回放，不调用外部供应商。"""
from __future__ import annotations

import copy
import io
import json
import re
from pathlib import Path

import pytest

from app.common import read_json, read_jsonl, write_json, write_jsonl
from app.editorial_digest import _compact_item, _normalize_model_digest, build_fallback_digest, render_digest_text
from app.industry_agent.contracts import ProviderUsage, SearchResearchResult
from app.industry_agent.handoff import create_manifest, validate_manifest, create_legacy_manifest
from app.industry_agent.page_reader import GenericPageReader
from app.industry_agent.pipeline import run_research
from app.industry_agent.production import prepare
from app.industry_agent.providers import DeepSeekWebSearchProvider
from app.industry_agent.runner import _dedupe_candidates
from app.industry_agent.verifier import DefaultEvidenceVerifier
from app.notify_feishu import build_message as feishu_message
from app.notify_wecom import build_message as wecom_message
from app.parse import canonicalize_row, main as parse_main
from app.provenance import counts_text, merge_objects
from app.render import _dedupe_by_title, build_html
from app.source_config import load_source_config
from app.summarize import dedupe_l3
from test_industry_agent import FakePageReader, _candidate

ROOT = Path(__file__).parents[1]
DATE = "2026-09-25"
URL = "https://media.example.com/pony-20260924.html"
SECOND = "https://other.example.com/pony-20260924.html"
OFFICIAL = "https://ir.pony.ai/news-releases/august-results"
SCORE = dict(industry_impact=26, deployment_or_regulation=22, scope_relevance=22, evidence_quality=18)


def config():
    return json.loads((ROOT / "sources.json").read_text())


def pony(financial=False):
    return dict(title="小马智行" + ("中报披露：" if financial else "运营更新：") + "Robotaxi 商业运营车队达到1975辆",
                factual_summary="小马智行 Robotaxi 商业运营车队达到1975辆。",
                companies=["小马智行"], coverage_domains=["robotaxi"], automation_level="L4",
                event_type="commercial_deployment", deployment_stage="commercial",
                canonical_url=URL, evidence=[{"url": URL}], score_breakdown=SCORE.copy())


def page(url, text="", date="2026-09-24T03:00:00+00:00", origin=""):
    return dict(ok=True, url=url, canonical_url=url, title="小马智行 Robotaxi 商业运营进展",
                content=text or "小马智行 Robotaxi 商业运营车队达到1975辆。",
                published_at_utc=date, publisher=url.split("/")[2], source_origin=origin)


class Search:
    name = "search_fixture"
    def __init__(self, candidate=None, overrun=0, baseline=True):
        self.candidate = candidate or pony()
        self.calls = []
        self.overrun = overrun
        self.baseline = baseline

    def probe(self, **kwargs):
        return True, ProviderUsage(web_searches=1, estimated_cost_cny=.01), []

    def research(self, system, prompt, max_searches, **kwargs):
        self.calls.append((prompt, max_searches))
        if "同一报告期" in prompt:
            rows = [{"canonical_url": OFFICIAL}] if self.baseline else []
        elif len(self.calls) == 1:
            rows = [copy.deepcopy(self.candidate)]
        else:
            rows = []
        return SearchResearchResult(json.dumps({"events": rows}, ensure_ascii=False),
            ProviderUsage(web_searches=max_searches + self.overrun, estimated_cost_cny=.01),
            trace=[{"type": "web_search", "query": prompt[:100]}], capability_confirmed=True,
            results=[{"url": SECOND, "title": "小马智行运营车队新闻", "snippet": "小马智行 Robotaxi 1975辆", "query": "小马智行车队"}])


class Model:
    name = "model_fixture"
    def __init__(self, novelty="repeated", supported=True, score=SCORE):
        self.novelty = novelty
        self.supported = supported
        self.score = score
        self.stages = []

    def complete_json(self, system, prompt, **kwargs):
        self.stages.append(system)
        if "评分员" in system:
            rows = json.loads(prompt.split("事件：", 1)[1])
            value = {"events": [{"event_key": r["event_key"], "score_breakdown": self.score,
                                "factual_summary": "禁止评分器改写成未经核验事实"} for r in rows]}
        elif "事实核验员" in system:
            payload = json.loads(prompt)
            claim = payload["candidate"]["factual_summary"]
            number = int(re.search(r"(\d+)辆", claim).group(1))
            value = {"supported": self.supported, "conflicts": [], "supported_summary": claim, "report_period": "2026H1", "supporting_urls": [r["url"] for r in payload["evidence"]],
                     "facts": [{"metric": "fleet", "value": number, "claim": claim, "source_url": payload["evidence"][0]["url"]}]}
        elif "此前官方公告提取" in system:
            payload = json.loads(prompt)
            value = {"report_period": "unknown" if self.novelty == "unknown" else "2026H1",
                     "facts": [{"metric": "fleet", "value": 1975, "claim": "此前车队达到1975辆", "source_url": payload["previous_official_pages"][0]["url"]}]}
        else:
            payload = json.loads(prompt)
            value = {"status": self.novelty, "new_fact_claims": [f["claim"] for f in payload["current"]] if self.novelty == "new" else [],
                     "report_period": "2026H1", "reason": "fixture"}
        return value, ProviderUsage(estimated_cost_cny=.01)


def run(tmp_path, search=None, model=None, cfg=None, pages=None):
    cfg = cfg or config()
    pages = pages or {URL: page(URL), SECOND: page(SECOND, "另一独立报道：小马智行 Robotaxi 商业运营车队达到1975辆。"),
                      OFFICIAL: page(OFFICIAL, date="2026-08-18T03:00:00+00:00")}
    return run_research(DATE, cfg, tmp_path / "out", tmp_path / "state",
                        model_provider=model or Model(), search_provider=search or Search(),
                        verifier=DefaultEvidenceVerifier(FakePageReader(pages), cfg))


def test_same_article_rewording_keeps_candidate_id_and_evidence():
    first = pony()
    second = {**pony(), "title": "小马 Robotaxi 车队突破千辆", "evidence": [{"url": SECOND}]}
    before = _dedupe_candidates([first])[0]["event_key"]
    result = _dedupe_candidates([first, second])
    assert len(result) == 1 and result[0]["event_key"] == before
    assert {e["url"] for e in result[0]["evidence"]} == {URL, SECOND}


def test_provider_associates_each_result_with_its_actual_query():
    parsed = DeepSeekWebSearchProvider._parse_response({"model": "actual-model", "content": [
        {"type": "server_tool_use", "id": "a", "name": "web_search", "input": {"query": "小马中报"}},
        {"type": "server_tool_use", "id": "b", "name": "web_search", "input": {"query": "L3许可"}},
        {"type": "web_search_tool_result", "tool_use_id": "b", "content": [{"url": SECOND, "title": "许可"}]},
        {"type": "web_search_tool_result", "tool_use_id": "a", "content": [{"url": URL, "title": "中报"}]}]})
    assert [(r["url"], r["query"]) for r in parsed.results] == [(SECOND, "L3许可"), (URL, "小马中报")]
    assert parsed.response_model == "actual-model"


def test_unselected_second_source_is_used_before_any_repair_and_scoring(tmp_path):
    search, model = Search(), Model()
    report = run(tmp_path, search, model)
    assert report["verified_event_count"] == 1
    assert not any("为候选补证" in prompt for prompt, _ in search.calls)
    assert "事实核验员" in model.stages[0] and "评分员" in model.stages[-1]
    rows = read_jsonl(tmp_path / "out" / DATE / "agent_events.jsonl")
    assert rows[0]["factual_summary"] == pony()["factual_summary"]
    assert len(rows[0]["evidence"]) == 2
    pool = read_json(tmp_path / "out" / DATE / "agent_evidence_pool.json")
    assert pool[SECOND]["state"] == "adopted"


def test_same_day_reuses_verified_result_without_supplier_calls(tmp_path):
    run(tmp_path)
    class Never:
        def probe(self, **kwargs):
            raise AssertionError("同日完成结果不应再次收费")
    result = run(tmp_path, search=Never())
    assert result["same_day_reused"] and result["verified_event_count"] == 1


def test_fact_conflict_repairs_at_most_twice_and_never_scores(tmp_path):
    search, model = Search(), Model(supported=False)
    report = run(tmp_path, search, model)
    assert report["verified_event_count"] == 0
    assert sum("为候选补证" in p for p, _ in search.calls) == 2
    assert not any("评分员" in s for s in model.stages)
    assert report["drop_reasons"]["facts_unsupported_or_conflicting"] == 1


@pytest.mark.parametrize("verdict,expected", [("repeated", 0), ("unknown", 0), ("new", 1)])
def test_pony_case_checks_prior_original_before_deciding_increment(tmp_path, verdict, expected):
    candidate = pony(financial=True)
    pages = None
    if verdict == "new":
        candidate["factual_summary"] = "小马智行 Robotaxi 商业运营车队达到2000辆。"
        pages = {URL: page(URL, candidate["factual_summary"]), SECOND: page(SECOND, "独立报道：" + candidate["factual_summary"]), OFFICIAL: page(OFFICIAL, date="2026-08-18T03:00:00+00:00")}
    search = Search(candidate)
    result = run(tmp_path, search, Model(novelty=verdict), pages=pages)
    assert result["verified_event_count"] == expected
    assert any("同一报告期" in p for p, _ in search.calls)
    if not expected:
        pending = read_jsonl(tmp_path / "out" / DATE / "agent_pending_review.jsonl")
        assert any(p.get("stage") == "novelty" for p in pending)


def test_disclosure_missing_history_and_original_is_pending(tmp_path):
    result = run(tmp_path, Search(pony(True), baseline=False))
    assert result["drop_reasons"]["disclosure_baseline_unknown"] == 1


def test_syndicated_media_is_not_independent():
    cfg = config()
    verifier = DefaultEvidenceVerifier(FakePageReader({
        URL: page(URL, origin="同一通讯社"),
        SECOND: page(SECOND, text="报道措辞有不同，小马智行 Robotaxi 商业运营车队1975辆。", origin="同一通讯社")}), cfg)
    row = pony()
    row["evidence"].append({"url": SECOND})
    assert verifier.inspect(row)["reason"] == "insufficient_independent_evidence"


@pytest.mark.parametrize("changes,reason", [
    ({"companies": ["文远知行"]}, "company_mismatch"),
    ({"automation_level": "L3"}, "automation_level_mismatch"),
    ({"event_type": "approval"}, "event_action_mismatch"),
    ({"deployment_stage": "production"}, "deployment_stage_mismatch")])
def test_precise_content_failure(changes, reason):
    row = {**pony(), **changes}
    assert DefaultEvidenceVerifier(FakePageReader({}), config()).content_failure(row, page(URL), "industry_media") == reason


def test_search_overrun_reserve_and_zero_cost_runtime_preserve_diagnostics(tmp_path):
    search = Search(overrun=4)
    cfg = config()
    result = run(tmp_path, search, cfg=cfg)
    assert result["usage"]["web_searches"] <= 48
    assert result["status"] == "partial_budget"
    assert all(limit <= 48 - 4 for _, limit in search.calls)
    assert result["verified_event_count"] == 1
    cfg["industry_agent"]["runtime_seconds"] = 0
    result = run(tmp_path / "runtime", cfg=cfg)
    assert result["status"] == "partial_budget" and result["verified_event_count"] == 0
    assert any(v["status"] == "not_run" for v in read_json(tmp_path / "runtime/out" / DATE / "agent_coverage.json").values())


def test_pdf_text_and_hkex_long_document_id_date(monkeypatch):
    from pypdf import PdfWriter
    from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject
    writer = PdfWriter()
    pdf_page = writer.add_blank_page(600, 800)
    font = DictionaryObject({NameObject("/Type"): NameObject("/Font"), NameObject("/Subtype"): NameObject("/Type1"), NameObject("/BaseFont"): NameObject("/Helvetica")})
    pdf_page[NameObject("/Resources")] = DictionaryObject({NameObject("/Font"): DictionaryObject({NameObject("/F1"): writer._add_object(font)})})
    stream = DecodedStreamObject()
    stream.set_data(b"BT /F1 12 Tf 10 700 Td (" + b"Pony AI Robotaxi commercial operations grew in the interim report. " * 4 + b") Tj ET")
    pdf_page[NameObject("/Contents")] = writer._add_object(stream)
    data = io.BytesIO()
    writer.write(data)
    monkeypatch.setattr("app.industry_agent.page_reader.http_get_bytes", lambda *a, **k: data.getvalue())
    result = GenericPageReader().read("https://www.hkexnews.hk/listedco/listconews/sehk/2026/0923/2026092300695_c.pdf")
    assert result["ok"] and "Robotaxi" in result["content"]
    assert result["published_at_utc"].startswith("2026-09-22T16:00")


def brief(ident, route, title):
    return dict(id=ident, discovery_routes=route, route_records=[{"route": r, "candidate_id": ident, "run_id": "run", "source_url": f"https://example.com/{ident}"} for r in route],
                title_zh=title, title=title, content=title, link=f"https://example.com/{ident}", canonical_url=f"https://example.com/{ident}",
                importance=4, region="domestic", company_id="other", evidence_type="industry_media", source_name="原始媒体",
                source_id=ident, summary_what=title, summary_so_what="影响商业化节奏。", summary_why="新进展。", impact_targets=["运营方"],
                published_at_utc="2026-09-24T03:00:00+00:00", tags=["robotaxi"], relevance_score=90)


def test_three_surfaces_all_items_and_fallback_have_labels(tmp_path):
    items = [brief("a", ["agent"], "小马智行商业运营扩张"), brief("b", ["legacy"], "工信部发布新许可"),
             brief("c", ["agent", "legacy"], "供应商获得量产定点")]
    report = {"active_profile": "hybrid_domestic", "domestic_agent_notice": "本期 Agent 部分完成"}
    digest = build_fallback_digest(DATE, items, report, top_n=1, source_top_n=3)
    text = render_digest_text(digest)
    assert "其他入选" in text
    for rendered in (text, build_html(DATE, items, report, config()), feishu_message(DATE, "", items, report, 3), wecom_message(DATE, "", report, items, 3)):
        for label in ("Agent 研究", "Legacy 采集", "共同发现"):
            assert label in rendered
        assert "Agent 研究 1 条｜Legacy 采集 1 条｜共同发现 1 条" in rendered
    assert "原始媒体" in build_html(DATE, items, report, config())
    (tmp_path / "preview.html").write_text(build_html(DATE, items, report, config()))


@pytest.mark.parametrize("reverse", [False, True])
def test_route_union_survives_url_title_and_event_clustering(reverse):
    a, b = brief("a", ["agent"], "小马智行 Robotaxi 商业运营扩张"), brief("b", ["legacy"], "小马智行 Robotaxi 商业运营扩张")
    b["canonical_url"] = a["canonical_url"]
    inputs = [a, b][:: -1 if reverse else 1]
    clustered, _ = dedupe_l3(copy.deepcopy(inputs))
    assert len(clustered) == 1 and clustered[0]["discovery_routes"] == ["agent", "legacy"]
    rendered = _dedupe_by_title(copy.deepcopy(inputs))
    assert len(rendered) == 1 and rendered[0]["discovery_routes"] == ["agent", "legacy"]
    raw = {"source_id": "news", "source_name": "媒体", "source_type": "rss", "region": "domestic",
           "url": URL, "payload": {"title": pony()["title"], "published": "2026-09-24T03:00:00+00:00", "link": URL}}
    x, y = canonicalize_row(raw), canonicalize_row({**raw, "source_type": "agent_event"})
    merge_objects(x, y)
    assert x.discovery_routes == ["agent", "legacy"]
    assert len(x.route_records) == 2


def test_model_cannot_change_route_or_insert_unknown_item():
    items = [_compact_item(brief("a", ["legacy"], "来自 Legacy 的新闻"))]
    raw = {"headline": "今日判断", "key_points": [], "top_items": [{"id": "a", "discovery_routes": ["agent"], "title": "伪造标题", "link": "https://bad.invalid"}]}
    result = _normalize_model_digest(raw, DATE, {}, items)
    assert result["top_items"][0]["discovery_routes"] == ["legacy"]
    assert result["top_items"][0]["title"] == items[0]["title"]
    raw["top_items"][0]["id"] = "unknown"
    with pytest.raises(ValueError, match="unknown"):
        _normalize_model_digest(raw, DATE, {}, items)
    assert counts_text([{}]).endswith("来源待核实 1 条")


def artifacts(tmp_path, agent_status="success", legacy=True):
    root, raw, reports = tmp_path / "agent", tmp_path / "raw", tmp_path / "reports"
    row = {**pony(), "event_id": "a", "published_at_utc": "2026-09-24T03:00:00+00:00", "agent_run_id": "research-a",
           "verification_status": "verified_two_media", "importance_score": 88}
    write_json(root / DATE / "agent_run_report.json", {"run_date": DATE, "status": agent_status, "agent_run_id": "research-a"})
    write_jsonl(root / DATE / "agent_events.jsonl", [row])
    create_manifest(root, DATE, "commit", "workflow")
    if legacy:
        write_jsonl(raw / DATE / "raw_items.jsonl", [{"source_type": "rss", "url": URL, "payload": {"title": "legacy"}}])
        write_json(reports / DATE / "run_report.json", {"collection_date": DATE, "source_stats": [{"request_success_count": 1}]})
        create_legacy_manifest(tmp_path, DATE, "commit", "workflow")
    return root, raw, reports


@pytest.mark.parametrize("agent_status,legacy_status,available,agent_ok,legacy_ok", [
    ("success", "success", True, True, True), ("failed", "success", True, False, True),
    ("partial_budget", "success", True, True, True), ("success", "failure", True, True, False),
    ("failed", "failure", False, False, False)])
def test_same_run_route_failure_matrix(tmp_path, agent_status, legacy_status, available, agent_ok, legacy_ok):
    paths = artifacts(tmp_path, agent_status)
    result = prepare(DATE, *paths, "commit", "workflow", legacy_status)
    assert (result["available"], result["agent_usable"], result["legacy_usable"]) == (available, agent_ok, legacy_ok)
    if legacy_ok and not agent_ok:
        assert "Agent 未完成" in result["notice"]
    if agent_ok and not legacy_ok:
        assert "海外覆盖可能缺失" in result["notice"]


@pytest.mark.parametrize("date,commit,run_id", [("2026-09-26", "commit", "workflow"), (DATE, "wrong", "workflow"), (DATE, "commit", "wrong")])
def test_artifact_identity_mismatch_rejected(tmp_path, date, commit, run_id):
    root, _, _ = artifacts(tmp_path)
    assert not validate_manifest(root, date, commit, run_id)[0]


def test_artifact_corruption_and_missing_legacy_file_are_not_usable(tmp_path):
    root, raw, reports = artifacts(tmp_path, legacy=False)
    assert not prepare(DATE, root, raw, reports, "commit", "workflow", "success")["legacy_usable"]
    (root / DATE / "agent_events.jsonl").write_text("{}\n")
    assert validate_manifest(root, DATE, "commit", "workflow")[1] == "handoff_checksum_mismatch"


def test_hybrid_keeps_complete_legacy_sources_and_manual_only_agent_schedule():
    legacy, _ = load_source_config(ROOT / "sources.json", "legacy")
    hybrid, _ = load_source_config(ROOT / "sources.json", "hybrid_domestic")
    assert {s["id"] for s in legacy["sources"] if s["enabled"]} == {s["id"] for s in hybrid["sources"] if s["enabled"]}
    manual = (ROOT / ".github/workflows/robtaxi-industry-agent.yml").read_text()
    assert "schedule:" not in manual
    workflow = (ROOT / ".github/workflows/robtaxi-digest-pages.yml").read_text()
    assert "robtaxi-agent-handoff-" not in workflow
    assert "Check durable daily notification locks" in workflow

@pytest.mark.parametrize("published,score,accepted", [
    ("2026-09-23T16:00:00+00:00", 65, True),
    ("2026-09-24T16:00:00+00:00", 88, False),
    ("2026-09-21T16:00:00+00:00", 80, True),
    ("2026-09-21T16:00:00+00:00", 79, False),
    ("2026-09-21T15:59:59+00:00", 88, False)])
def test_date_window_boundaries_and_late_threshold(published, score, accepted):
    url = "https://policy.gov.cn/pony.html"
    row = {**pony(), "canonical_url": url, "evidence": [{"url": url}],
           "score_breakdown": dict(industry_impact=min(30, score), deployment_or_regulation=min(25, score-30), scope_relevance=min(25, max(0, score-55)), evidence_quality=max(0, score-80))}
    verifier = DefaultEvidenceVerifier(FakePageReader({url: page(url, date=published)}), config())
    event, reason = verifier.verify(row, DATE, "run")
    assert bool(event) == accepted, reason


def test_exhausted_daily_budget_is_not_reset_by_failed_retry(tmp_path):
    cfg = config()
    cfg["industry_agent"]["daily_budget_cny"] = .02
    first = run(tmp_path, cfg=cfg)
    class Never:
        name = "never"
        def probe(self, **kwargs):
            raise AssertionError("已耗尽的当日预算不能再次请求")
    second = run(tmp_path, search=Never(), cfg=cfg)
    assert first["budget_exhausted"] and second["budget_exhausted"]
    assert second["usage"]["estimated_cost_cny"] == first["usage"]["estimated_cost_cny"]


def test_homepage_does_not_merge_unrelated_candidate_titles():
    a, b = pony(), pony()
    a["canonical_url"] = b["canonical_url"] = "https://ir.pony.ai/"
    b["title"] = "小马智行获得另外城市的 Robotaxi 许可"
    assert len(_dedupe_candidates([a, b])) == 2


def test_actual_parse_url_dedupe_retains_both_routes_on_same_day_rerun(tmp_path, monkeypatch):
    date = "2026-10-08"
    url = "https://www.miit.gov.cn/approval/new-l3.html"
    common = dict(source_id="miit", source_name="工信部", region="domestic", company_hint="小鹏汽车", url=url,
                  coverage_domains=["passenger_l3"], source_role="primary", evidence_type="regulator",
                  payload={"title": "工信部批准小鹏汽车 L3 乘用车上路试点", "content": "工信部批准小鹏汽车 L3 有条件自动驾驶乘用车在中国开展上路试点。",
                           "link": url, "published": "2026-10-07T03:00:00+00:00"})
    raw = tmp_path / "raw" / date / "raw_items.jsonl"
    write_jsonl(raw, [{**common, "source_type": "rss"}, {**common, "source_type": "agent_event"}])
    seen = tmp_path / "seen.jsonl"
    write_jsonl(seen, [{"resolved_url": url, "fingerprint": "", "last_seen_date": date, "first_seen_date": date}])
    monkeypatch.setattr("sys.argv", ["parse", "--date", date, "--in", str(tmp_path / "raw"), "--out", str(tmp_path / "canonical"),
        "--report", str(tmp_path / "reports"), "--seen-state", str(seen), "--first-seen-state", str(tmp_path / "first.json")])
    assert parse_main() == 0
    rows = read_jsonl(tmp_path / "canonical" / date / "canonical_items.jsonl")
    assert len(rows) == 1 and rows[0]["discovery_routes"] == ["agent", "legacy"]


def test_unverified_or_wrong_research_run_never_enters_hybrid(tmp_path):
    root, raw, reports = artifacts(tmp_path)
    rows = read_jsonl(root / DATE / "agent_events.jsonl")
    rows[0]["verification_status"] = "pending"
    write_jsonl(root / DATE / "agent_events.jsonl", rows)
    create_manifest(root, DATE, "commit", "workflow")
    result = prepare(DATE, root, raw, reports, "commit", "workflow", "success")
    assert result["legacy_usable"] and not result["agent_usable"]


def test_one_switch_rollback_does_not_import_agent(tmp_path):
    root, raw, reports = artifacts(tmp_path)
    result = prepare(DATE, root, raw, reports, "commit", "workflow", "success", "legacy")
    assert result["available"] and not result["agent_usable"]
    assert len(read_jsonl(raw / DATE / "raw_items.jsonl")) == 1


def test_complete_failure_keeps_previous_date_and_content(tmp_path, monkeypatch):
    from app.industry_agent.publication import preserve_previous
    page_file = tmp_path / "site/index.html"
    monkeypatch.setattr("app.industry_agent.publication.http_get_bytes", lambda *a, **k: b'<html><body class="old"><h1>2026-09-24</h1><p>previous news</p></body></html>')
    preserve_previous(page_file, "https://example.com/")
    result = page_file.read_text()
    assert "2026-09-24" in result and "previous news" in result and "更新失败" in result
    page_file.unlink()
    monkeypatch.setattr("app.industry_agent.publication.http_get_bytes", lambda *a, **k: b"unreadable")
    with pytest.raises(RuntimeError, match="停止本次覆盖"):
        preserve_previous(page_file, "https://example.com/")
    assert not page_file.exists()


def test_production_workflow_has_valid_yaml_and_bound_route_artifacts():
    import yaml
    class StrictLoader(yaml.BaseLoader):
        pass
    def mapping(loader, node):
        result = {}
        for key, value in node.value:
            key = loader.construct_object(key)
            assert key not in result, f"重复 YAML 字段：{key}"
            result[key] = loader.construct_object(value)
        return result
    StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)
    workflows = {p.name: yaml.load(p.read_text(), Loader=StrictLoader) for p in (ROOT / ".github/workflows").glob("*.yml")}
    jobs = workflows["robtaxi-digest-pages.yml"]["jobs"]
    assert set(jobs["build"]["needs"]) == {"context", "agent", "legacy"}
    assert jobs["agent"]["timeout-minutes"] == "45"
    assert "always()" in jobs["build"]["if"]
    assert "available == 'true'" in jobs["notify"]["if"]
    assert "schedule" not in workflows["robtaxi-industry-agent.yml"]["on"]
    activation = next(s for s in workflows["robtaxi-agent-approval.yml"]["jobs"]["approve"]["steps"] if s.get("name") == "Activate agent_domestic phase1")
    assert "hybrid_domestic" in activation["if"]
