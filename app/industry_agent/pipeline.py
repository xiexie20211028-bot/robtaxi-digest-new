"""正式研究闭环：领域发现、证据检查、补证、增量审核、最终评分。"""
from __future__ import annotations

import json
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.common import normalize_url, read_json, read_jsonl, sha1_text, write_json, write_jsonl
from app.decision_log import build_candidate_decision
from app.taxonomy import classify_industry_item
from .contracts import ProviderUsage
from .providers import build_model_provider, build_search_provider
from .page_reader import GenericPageReader
from .verifier import DefaultEvidenceVerifier, ALLOWED_DOMAINS, PRIMARY_EVIDENCE
from .domestic_scope import has_domestic_relevance

QUERIES = {
    "robotaxi": ("中国 Robotaxi 无人出租车 运营 车队 牌照 小马智行 文远知行 萝卜快跑", "中国 无人驾驶出租车 商业运营 部署 收费 事故 公司公告"),
    "passenger_l3": ("中国 L3 乘用车 准入 责任转移 交付 工信部", "有条件自动驾驶 三级 自动驾驶汽车 许可 量产"),
    "passenger_l4": ("中国 L4 乘用车 四级 高度自动驾驶 测试 准入 量产", "中国 L4 私家车 公开道路部署 测试牌照"),
    "core_supply_chain": ("中国 Robotaxi L3 L4 激光雷达 芯片 域控制器 线控 定点 量产", "自动驾驶 乘用车 供应商 合作项目 认证 交付"),
    "regulation_safety": ("中国 自动驾驶 Robotaxi 事故 召回 安全调查 准入 政策", "site:gov.cn 智能网联汽车 上路 监管 安全"),
    "industry_wide_regulation": ("全国人大 道路交通安全法 自动驾驶 特别规定 修订草案", "site:npc.gov.cn 自动驾驶汽车 责任 保险 法律 审议"),
}


class Exhausted(RuntimeError):
    pass


def company_hints(config: dict) -> str:
    """企业与证券代码只能来自已审计配置。"""
    rows = []
    for c in config.get("companies", []):
        rows.append({k: c[k] for k in ("id", "name", "aliases", "stock_codes") if k in c})
    return json.dumps(rows, ensure_ascii=False)


def add_pool_links(candidate: dict, pool: dict[str, dict], config: dict | None = None) -> None:
    companies = [str(v).lower() for v in candidate.get("companies", []) if str(v)]
    for company in (config or {}).get("companies", []):
        aliases = [str(company.get("id", "")).lower(), str(company.get("name", "")).lower(), *[str(v).lower() for v in company.get("aliases", [])], *[str(v).lower() for v in company.get("stock_codes", [])]]
        if any(v in aliases for v in companies):
            companies.extend(aliases + [str(v) for v in company.get("stock_codes", [])])
    existing = {normalize_url(str(v.get("url", ""))) for v in candidate.get("evidence", []) if isinstance(v, dict)}
    # 标题/摘要中的主体匹配优先于宽泛查询，避免前 12 个读取名额被无关结果占满。
    tokens = [token for name in companies for token in re.findall(r"[a-z][a-z.]{2,}|[\u4e00-\u9fff]{2,}|\d{4,}", name)]
    ordered = sorted(pool.items(), key=lambda entry: not any(token in (str(entry[1].get("title", "")) + " " + str(entry[1].get("snippet", ""))).lower() for token in tokens))
    for url, row in ordered:
        text = (str(row.get("title", "")) + " " + str(row.get("snippet", "")) + " " + str(row.get("query", ""))).lower()
        if url in existing:
            if candidate["event_key"] not in row.setdefault("candidate_ids", []):
                row["candidate_ids"].append(candidate["event_key"])
            continue
        if not companies:
            tokens = [v for v in ("全国人大", "自动驾驶", "智能网联", "道路交通安全法") if v in str(candidate.get("title", ""))]
        if tokens and any(token in text for token in tokens):
            candidate.setdefault("evidence", []).append({"url": url})
            existing.add(url)
            if candidate["event_key"] not in row.setdefault("candidate_ids", []):
                row["candidate_ids"].append(candidate["event_key"])


def run_research(run_date: str, config: dict, out_root: Path, state_root: Path,
                 model_provider: Any = None, search_provider: Any = None, verifier: Any = None) -> dict:
    from .runner import (SYSTEM_PROMPT, _dedupe_candidates, _events_from_text, _normalize_discovery_output,
                         _score_candidates, _event_identity_keys, _load_seen, _save_seen)

    settings = config.get("industry_agent", {})
    model = model_provider or build_model_provider(settings)
    search = search_provider or build_search_provider(settings)
    verifier = verifier or DefaultEvidenceVerifier(GenericPageReader(), config)
    directory = out_root / run_date
    directory.mkdir(parents=True, exist_ok=True)
    state_root.mkdir(parents=True, exist_ok=True)
    signature = sha1_text(json.dumps({"settings": settings, "companies": config.get("companies", []), "commit": os.environ.get("GITHUB_SHA", "")}, sort_keys=True, ensure_ascii=False))
    daily_path = state_root / "daily_runs" / f"{run_date}.json"
    previous_run = read_json(daily_path) if daily_path.exists() else {}
    if previous_run.get("signature") == signature and previous_run.get("reusable"):
        for name, payload in previous_run["artifacts"].items():
            if name.endswith(".jsonl"):
                write_jsonl(directory / name, payload)
            else:
                write_json(directory / name, payload)
        report = read_json(directory / "agent_run_report.json")
        report["same_day_reused"] = True
        write_json(directory / "agent_run_report.json", report)
        return report
    run_id = f"agent_{run_date}_{uuid.uuid4().hex[:12]}"
    usage = ProviderUsage(**previous_run.get("usage", {}))
    budget = float(settings.get("daily_budget_cny", 5))
    search_cap = int(settings.get("max_web_searches", 48))
    reserve = int(settings.get("search_overrun_reserve", 4))
    deadline = time.monotonic() + float(settings.get("runtime_seconds", 2400))
    if hasattr(verifier.page_reader, "deadline"):
        verifier.page_reader.deadline = deadline
    trace, decisions, diagnostics, pending, errors = [], [], [], [], []
    pool: dict[str, dict] = {}
    snapshots = []
    last_queries = []
    coverage = {domain: {"status": "not_run", "attempts": []} for domain in QUERIES}
    candidates, events = [], []
    status = "success"
    exhausted = False
    history_path = state_root / "fact_history.json"
    history = read_json(history_path) if history_path.exists() else {"version": 1, "facts": []}
    cutoff = (datetime.fromisoformat(run_date) - timedelta(days=180)).date().isoformat()
    history["facts"] = [r for r in history.get("facts", []) if r.get("recorded_date", "") >= cutoff]

    def remaining() -> float:
        if time.monotonic() >= deadline:
            raise Exhausted("runtime_limit")
        if usage.estimated_cost_cny >= budget:
            raise Exhausted("cost_limit")
        return budget - usage.estimated_cost_cny

    def save_usage() -> None:
        # 每次真实调用后持久化，失败重试也不能重置当天费用。
        write_json(daily_path, {"signature": signature, "usage": usage.to_dict(), "reusable": False})

    def complete(system: str, prompt: str) -> dict:
        try:
            value, cost = model.complete_json(system, prompt, max_cost_cny=remaining())
        except Exception as exc:
            if isinstance(getattr(exc, "usage", None), ProviderUsage):
                usage.add(exc.usage)
                save_usage()
            raise
        usage.add(cost)
        save_usage()
        return value

    def research(stage: str, prompt: str, limit: int = 1) -> list[dict]:
        nonlocal last_queries
        available = search_cap - usage.web_searches - reserve
        if available < 1:
            raise Exhausted("search_limit")
        try:
            result = search.research(SYSTEM_PROMPT, prompt, min(limit, available), max_cost_cny=remaining())
        except Exception as exc:
            if isinstance(getattr(exc, "usage", None), ProviderUsage):
                usage.add(exc.usage)
                save_usage()
            raise
        usage.add(result.usage)
        save_usage()
        snapshots.append({"stage": stage, "type": "search_response", "text": result.text[:40000]})
        trace.extend({"stage": stage, **r} for r in result.trace)
        trace.append({"stage": stage, "type": "response_model", "model": getattr(result, "response_model", "")})
        for row in getattr(result, "results", []):
            url = normalize_url(str(row.get("url", "")))
            if url:
                pool.setdefault(url, {**row, "url": url, "state": "pending_verification", "candidate_ids": []})
                observations = pool[url].setdefault("observations", [])
                observations.append({"query": row.get("query", ""), "tool_use_id": row.get("tool_use_id", ""), "state": "duplicate" if observations else "pending_verification"})
        # 兼容旧 Provider 的 URL 结果；不虚构查询关联。
        for row in result.trace:
            for url in row.get("urls", []):
                if normalize_url(url):
                    pool.setdefault(normalize_url(url), {"url": normalize_url(url), "query": row.get("query", ""), "state": "pending_verification", "candidate_ids": []})
        if usage.web_searches > search_cap:
            errors.append("provider_search_overrun")
        if result.usage.web_searches < 1:
            raise RuntimeError("search_not_executed")
        last_queries = [str(row.get("query", "")).strip() for row in result.trace if row.get("type") == "web_search" and str(row.get("query", "")).strip()]
        if not last_queries:
            raise RuntimeError("search_query_unconfirmed")
        if any(row.get("type") == "web_search_error" for row in result.trace):
            raise RuntimeError("search_result_error")
        try:
            return _events_from_text(result.text)
        except ValueError:
            rows, cost = _normalize_discovery_output(model, result.text, result.trace, max_cost_cny=remaining())
            usage.add(cost)
            save_usage()
            return rows

    def inspect(candidate: dict) -> dict:
        result = verifier.inspect(candidate)
        for row in result["diagnostics"]:
            diagnostics.append({"candidate_id": candidate["event_key"], **row})
            if row["url"] in pool:
                pool[row["url"]]["state"] = "adopted" if row["reason"] == "supported" else "access_failed" if row["reason"] == "access_failed" else "unrelated"
                pool[row["url"]]["diagnostic_reason"] = row["reason"]
        candidate["verified_support"] = result["support"]
        return result

    def reject(candidate: dict, reason: str, stage: str) -> None:
        pending.append({"candidate": candidate, "reason": reason, "stage": stage})
        decisions.append(build_candidate_decision(route="agent_first", candidate=candidate, source={}, stage=stage, kept=False, final_reason=reason, candidate_id=candidate["event_key"], threshold=65))

    def check_facts(candidate: dict, supported: list[dict]) -> tuple[bool, str]:
        # 全部事实审核均有回源片段；无法确认不能由提高分数绕过。
        result = complete("你是事实核验员，只根据原文证据，输出 JSON，不得搜索或增加事实。", json.dumps({
            "task": "核对候选每个实质事实。摘要中的数字、主体、阶段须有原文支持；冲突不能视为支持。返回 supported(bool)、conflicts(list)、supported_summary(str)、supporting_urls(list，仅包含正文支持全部实质事实的 URL)、report_period、facts(list，每项含 metric、value、claim、source_url)。只支持部分旧事实的公告不能列入 supporting_urls。不支持返回 false。",
            "candidate": {k: candidate.get(k) for k in ("title", "factual_summary", "companies")}, "evidence": supported,
        }, ensure_ascii=False))
        if result.get("supported") is not True or result.get("conflicts"):
            return False, "facts_unsupported_or_conflicting"
        facts = result.get("facts", [])
        urls = {r["url"] for r in supported}
        if not isinstance(facts, list) or not facts or any(not isinstance(f, dict) or f.get("source_url") not in urls or not f.get("claim") for f in facts):
            return False, "fact_support_unresolved"
        supporting_urls = result.get("supporting_urls", [])
        if not isinstance(supporting_urls, list) or not supporting_urls or any(u not in urls for u in supporting_urls):
            return False, "fact_support_urls_unresolved"
        factual_candidate = {**candidate, "factual_summary": str(result.get("supported_summary", "")) or candidate.get("factual_summary", ""), "evidence": [{"url": url} for url in supporting_urls]}
        if normalize_url(factual_candidate.get("canonical_url", "")) not in supporting_urls:
            factual_candidate["canonical_url"] = supporting_urls[0]
        fact_evidence = verifier.inspect(factual_candidate)
        if fact_evidence["reason"] != "verified":
            return False, "facts:" + fact_evidence["reason"]
        candidate["facts"] = facts
        candidate["fact_support_urls"] = supporting_urls
        candidate["report_period"] = str(result.get("report_period", ""))
        # 内容修正必须有证据且保留原摘要。
        candidate["original_summary"] = candidate.get("factual_summary", "")
        candidate["factual_summary"] = str(result.get("supported_summary", "")) or candidate["original_summary"]
        return True, "verified"

    def valid_for_domain(row: dict, domain: str) -> bool:
        text = str(row.get("title", "")) + " " + str(row.get("factual_summary", ""))
        scope = classify_industry_item({"title": row.get("title", ""), "content": row.get("factual_summary", "")}, {"coverage_domains": list(ALLOWED_DOMAINS), "evidence_type": "industry_media"})
        return bool(scope.get("in_scope")) and domain in scope.get("coverage_domains", []) and has_domestic_relevance(text, config, row.get("companies", []))

    def novelty(candidate: dict, supported: list[dict]) -> tuple[bool, str]:
        text = candidate.get("title", "") + " " + candidate.get("factual_summary", "")
        if not re.search(r"财报|中报|中期|半年报|年报|业绩.{0,5}(?:报告|公告)|季度.{0,5}业绩|financial results|interim report|annual report", text, re.I):
            company = str((candidate.get("companies") or [""])[0])
            old_claims = {sha1_text(re.sub(r"\s+", "", str(f.get("claim", "")))) for r in history["facts"] if r.get("company") == company for f in r.get("facts", [])}
            new_facts = [f for f in candidate["facts"] if sha1_text(re.sub(r"\s+", "", f["claim"])) not in old_claims]
            if not new_facts:
                return False, "repeated_facts"
            candidate["novelty_status"] = "not_repeated_disclosure"
            return True, "verified"
        company = str((candidate.get("companies") or [""])[0])
        previous = [r for r in history["facts"] if r.get("company") == company]
        canonical = normalize_url(candidate.get("canonical_url", ""))
        current_date = next((r["published_at_utc"] for r in supported if normalize_url(r["url"]) == canonical), max(r["published_at_utc"] for r in supported))
        # 索引缺失也回查此前官方公告；只读页面，不使用搜索摘要判定增量。
        results = research("disclosure_baseline", f"寻找 {company} 同一报告期此前发布的官方业绩公告或 IR，早于 {current_date}。候选：{text[:1200]}。返回实际已公开公告 URL，不将当前媒体转载当原公告。", 2)
        prior_pages = []
        for row in results:
            links = [row.get("canonical_url", ""), *[v.get("url", "") for v in row.get("evidence", []) if isinstance(v, dict)]]
            for url in links:
                if not url:
                    continue
                page = verifier.page_reader.read(url)
                _, kind = verifier._domain_meta(url, "")
                date = str(page.get("published_at_utc", ""))
                if page.get("ok") and kind in PRIMARY_EVIDENCE and date and date < current_date:
                    prior_pages.append({"url": url, "date": date, "text": str(page.get("content", ""))[:12000]})
                    diagnostics.append({"candidate_id": candidate["event_key"], "stage": "disclosure_baseline", "url": url, "reason": "baseline_verified", "published_at_utc": date, "content_hash": sha1_text(str(page.get("content", ""))), "support_excerpt": str(page.get("content", ""))[:1200]})
                    if normalize_url(url) in pool:
                        pool[normalize_url(url)]["state"] = "adopted"
                        pool[normalize_url(url)]["purpose"] = "disclosure_baseline"
        if not previous and not prior_pages:
            return False, "disclosure_baseline_unknown"
        period = candidate.get("report_period", "")
        def fact_key(fact: dict) -> tuple[str, str]:
            value = re.sub(r"[,，%％\s]", "", str(fact.get("value", ""))).lower()
            return str(fact.get("metric", "")).lower(), value
        known = {fact_key(f) for row in previous if period and row.get("report_period") == period for f in row.get("facts", [])}
        if prior_pages:
            baseline = complete("仅从此前官方公告提取同一报告期事实，输出 JSON。", json.dumps({
                "task": "对照当前 facts 的 metric、value、口径和单位，提取此前已披露的匹配事实。返回 report_period、facts(list，metric/value/claim/source_url)。只能引用给定原文 URL；缺失或不同报告期不得猜测。",
                "current": candidate["facts"], "report_period": period, "previous_official_pages": prior_pages,
            }, ensure_ascii=False))
            prior_urls = {row["url"] for row in prior_pages}
            baseline_facts = [f for f in baseline.get("facts", []) if isinstance(f, dict) and f.get("source_url") in prior_urls]
            if period and baseline.get("report_period") == period:
                known.update(fact_key(f) for f in baseline_facts)
        known_claims = {f["claim"] for f in candidate["facts"] if fact_key(f) in known}
        if known_claims and len(known_claims) == len(candidate["facts"]):
            candidate["novelty_audit"] = {"status": "repeated", "reason": "same_period_same_metrics", "report_period": period}
            return False, "repeated_disclosure"
        verdict = complete("比较同一报告期的原始披露，仅重要新增事实构成新闻，输出 JSON。", json.dumps({
            "task": "返回 status(new/repeated/unknown)、new_fact_claims(list，仅从当前 facts.claim 中选择重要新增事实)、report_period、first_disclosed_at_utc、reason。新文件发布本身及旧数据换日期不算新增。无法确认返回 unknown。",
            "current": candidate["facts"], "current_evidence": supported, "previous_facts": previous, "previous_official_pages": prior_pages,
        }, ensure_ascii=False))
        candidate["novelty_audit"] = verdict
        claims = {f["claim"] for f in candidate["facts"]}
        new_claims = verdict.get("new_fact_claims", [])
        if verdict.get("status") != "new" or not isinstance(new_claims, list) or not new_claims or not set(new_claims).issubset(claims):
            return False, "repeated_disclosure" if verdict.get("status") == "repeated" else "disclosure_novelty_unknown"
        if set(new_claims) & known_claims:
            return False, "repeated_disclosure"
        candidate["factual_summary"] = "；".join(new_claims)
        candidate["title"] = f"{company}新增披露：{new_claims[0][:90]}"
        candidate["novelty_status"] = "important_new_facts"
        return True, "verified"

    try:
        if search_cap - usage.web_searches - reserve < 1:
            raise Exhausted("search_limit")
        ok, cost, probe = search.probe(max_cost_cny=remaining())
        usage.add(cost)
        trace.extend({"stage": "capability_probe", **r} for r in probe)
        save_usage()
        if not ok:
            raise RuntimeError("search_capability_unavailable")
        empty_domains = []
        # 第一轮先逐个检查全部领域，零结果改写放在第二轮，避免前几域挤占后几域。
        for domain, queries in QUERIES.items():
            try:
                rows = research("scan", f"研究运行日 {run_date} 前一北京时间自然日，重要迟到可回看72小时。仅检索领域 {domain}，必须使用查询：{queries[0]}。企业和股票代码配置：{company_hints(config)}。最多6个具有事实增量的事件。", 1)
                valid_rows = [r for r in rows if valid_for_domain(r, domain)]
                coverage[domain]["attempts"].append({"query": queries[0], "result_count": len(valid_rows), "actual_queries": list(last_queries)})
                coverage[domain]["status"] = "candidate_found" if valid_rows else "no_valid_results"
                candidates = _dedupe_candidates(candidates + rows)
                if not valid_rows:
                    empty_domains.append(domain)
            except Exhausted:
                raise
            except Exception as exc:
                coverage[domain]["status"] = "failed"
                errors.append(f"scan:{domain}:{str(exc)[:120]}")
                empty_domains.append(domain)
        for domain in empty_domains:
            rows = research("coverage_audit", f"对 {domain} 换角度查询：{QUERIES[domain][1]}。运行日 {run_date}，前一自然日事件或72小时重要迟到，返回有原文证据的事件。", 1)
            valid_rows = [r for r in rows if valid_for_domain(r, domain)]
            coverage[domain]["attempts"].append({"query": QUERIES[domain][1], "result_count": len(valid_rows), "actual_queries": list(last_queries)})
            coverage[domain]["status"] = "candidate_found" if valid_rows else "searched_empty"
            candidates = _dedupe_candidates(candidates + rows)
    except Exhausted as exc:
        exhausted = True
        errors.append(str(exc))
    except Exception as exc:
        errors.append(str(exc)[:250])
        status = "degraded" if candidates else "failed"

    seen = _load_seen(state_root)
    accepted: set[str] = set()
    late_count = 0
    for candidate in candidates[:30]:
        try:
            remaining()
            snapshots.append({"stage": "merged_candidate", "candidate_id": candidate["event_key"], "candidate": json.loads(json.dumps(candidate, ensure_ascii=False))})
            scope = classify_industry_item({"title": candidate.get("title", ""), "content": candidate.get("factual_summary", "")}, {"coverage_domains": list(ALLOWED_DOMAINS), "evidence_type": "industry_media"})
            if not scope.get("in_scope"):
                reject(candidate, "out_of_scope", "scope")
                continue
            candidate["coverage_domains"] = scope.get("coverage_domains", [])
            add_pool_links(candidate, pool, config)
            checked = inspect(candidate)
            supported, fact_reason = check_facts(candidate, checked["support"]) if checked["reason"] == "verified" else (False, checked["reason"])
            for round_no in range(min(2, int(settings.get("max_repair_rounds", 2)))):
                if checked["reason"] == "verified" and supported:
                    break
                rows = research("evidence_repair", f"为候选补证，保留 event_key={candidate['event_key']}。优先官方原文，或非同源第二媒体。事实缺口：{fact_reason}。失败诊断：{json.dumps(checked['diagnostics'],ensure_ascii=False)}；候选：{json.dumps(candidate,ensure_ascii=False)[:15000]}。配置主体：{company_hints(config)}。不得增加事件或事实。", 2)
                for row in rows:
                    if normalize_url(row.get("canonical_url", "")) == normalize_url(candidate.get("canonical_url", "")) or row.get("event_key") == candidate["event_key"]:
                        row["event_key"] = candidate["event_key"]
                        candidate = _dedupe_candidates([candidate, row])[0]
                add_pool_links(candidate, pool, config)
                trace.append({"stage": "repair", "candidate_id": candidate["event_key"], "round": round_no + 1})
                checked = inspect(candidate)
                supported, fact_reason = check_facts(candidate, checked["support"]) if checked["reason"] == "verified" else (False, checked["reason"])
            if checked["reason"] != "verified":
                reject(candidate, checked["reason"], "evidence")
                continue
            if not supported:
                reject(candidate, fact_reason, "facts")
                continue
            candidate["web_published_at_utc"] = next((row["published_at_utc"] for row in checked["support"] if normalize_url(row["url"]) == normalize_url(candidate.get("canonical_url", ""))), "")
            # 旧公告仅能充当增量比较基线，不能把新事实的日期拖回旧公告。
            fact_urls = set(candidate["fact_support_urls"])
            fact_support = [r for r in checked["support"] if r["url"] in fact_urls]
            is_new, reason = novelty(candidate, checked["support"])
            history["facts"].append({"company": str((candidate.get("companies") or [""])[0]), "recorded_date": run_date, "report_period": candidate.get("novelty_audit", {}).get("report_period", candidate.get("report_period", "")), "facts": candidate["facts"], "source_urls": [r["url"] for r in checked["support"]]})
            if not is_new:
                reject(candidate, reason, "novelty")
                continue
            candidate["verified_support"] = fact_support
            candidate["score_breakdown"] = {}
            scored, cost = _score_candidates(model, [candidate], max_cost_cny=remaining())
            usage.add(cost)
            save_usage()
            remaining()
            candidate = scored[0]
            candidate["evidence"] = [r.to_dict() for r in checked["evidence"] if r.url in fact_urls]
            if normalize_url(candidate.get("canonical_url", "")) not in fact_urls:
                candidate["canonical_url"] = next((r.url for r in checked["evidence"] if r.url in fact_urls and r.is_primary), fact_support[0]["url"])
            final_inspection = inspect(candidate)
            if final_inspection["reason"] != "verified":
                reject(candidate, "facts:" + final_inspection["reason"], "final_verification")
                continue
            candidate["evidence_read_limit"] = 12
            event, reason = verifier.verify(candidate, run_date, run_id)
            if not event:
                reject(candidate, reason, "final_verification")
                continue
            identity = _event_identity_keys(event)
            if identity & (seen | accepted):
                reject(candidate, "seen_within_35_days", "dedupe")
                continue
            if event.late_arrival and late_count >= int(settings.get("late_arrival_max_items", 2)):
                reject(candidate, "late_arrival_cap", "time")
                continue
            late_count += int(event.late_arrival)
            accepted.update(identity)
            event.model_provider, event.search_provider = model.name, search.name
            row = event.to_dict()
            row.update(discovery_routes=["agent"], workflow_run_id=os.environ.get("GITHUB_RUN_ID", ""), candidate_id=candidate["event_key"], novelty_status=candidate.get("novelty_status", ""), facts=candidate["facts"], first_disclosed_at_utc=event.published_at_utc, web_published_at_utc=candidate.get("web_published_at_utc", ""), filing_disclosed_at_utc=next((r["published_at_utc"] for r in checked["support"] if verifier._domain_meta(r["url"], "")[1] == "filing"), ""))
            events.append(row)
            snapshots.append({"stage": "final_scoring", "candidate_id": candidate["event_key"], "score_breakdown": candidate["score_breakdown"], "facts": candidate["facts"], "novelty_audit": candidate.get("novelty_audit", {})})
            decisions.append(build_candidate_decision(route="agent_first", candidate=candidate, source={}, stage="final_verification", kept=True, final_reason="verified", candidate_id=candidate["event_key"], score=event.importance_score, threshold=65, extra={"event_id": event.event_id}))
        except (Exhausted, RuntimeError) as exc:
            exhausted = exhausted or isinstance(exc, Exhausted) or "budget" in str(exc)
            errors.append(str(exc)[:200])
            reject(candidate, "incomplete:" + str(exc)[:120], "budget_or_provider")
        except Exception as exc:
            errors.append(str(exc)[:200])
            reject(candidate, "candidate_processing_failed", "processing")

    # 未执行领域不能成为成功的零新闻；保留已经完成核验的事件。
    coverage_complete = all(v["status"] in {"candidate_found", "searched_empty"} for v in coverage.values())
    if exhausted:
        status = "partial_budget"
    elif errors or not coverage_complete:
        status = "degraded" if candidates or events else "failed"
    elif not events:
        status = "success_empty"
    drops: dict[str, int] = {}
    for decision in decisions:
        if not decision.get("kept"):
            key = decision["final_reason"]
            drops[key] = drops.get(key, 0) + 1
    for row in pool.values():
        if not row.get("candidate_ids") and row["state"] == "pending_verification":
            row["state"] = "pending_verification"
            pending.append({"url": row["url"], "query": row.get("query", ""), "reason": "search_link_not_selected", "stage": "discovery"})
    # 支持全文仅在内存中核验；工件保留片段和哈希，不保存完整正文。
    for row in pending:
        if "candidate" in row:
            row["candidate"].pop("verified_support", None)
    write_jsonl(directory / "agent_events.jsonl", events)
    write_jsonl(directory / "agent_trace.jsonl", trace)
    write_jsonl(directory / "agent_candidate_decisions.jsonl", decisions)
    write_jsonl(directory / "agent_evidence_diagnostics.jsonl", diagnostics)
    write_jsonl(directory / "agent_pending_review.jsonl", pending)
    write_jsonl(directory / "agent_candidate_snapshots.jsonl", snapshots)
    write_json(directory / "agent_evidence_pool.json", pool)
    write_json(directory / "agent_coverage.json", coverage)
    write_json(history_path, history)
    # 历史去重写入沿用原契约。
    from .contracts import AgentEvent, Evidence
    accepted_events = [AgentEvent(**{k:v for k,v in row.items() if k in AgentEvent.__dataclass_fields__ and k != "evidence"}, evidence=[Evidence(**e) for e in row["evidence"]]) for row in events]
    _save_seen(state_root, accepted_events, run_date)
    report = {"schema_version": "industry-agent-run-v2", "agent_run_id": run_id, "run_date": run_date, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "status": status, "technical_status": status, "business_status": "success" if events else "empty_covered" if coverage_complete and status == "success_empty" else "empty_uncovered", "coverage_audit_status": "completed" if coverage_complete else "incomplete", "model_provider": model.name, "search_provider": search.name, "model": settings.get("model", ""), "candidate_count": len(candidates), "verified_event_count": len(events), "candidate_decision_count": len(decisions), "drop_reasons": drops, "usage": usage.to_dict(), "budget_cny": budget, "budget_exhausted": exhausted, "errors": errors, "events_output": str(directory / "agent_events.jsonl")}
    report.update(mode=settings.get("mode", "production"), workflow_run_id=os.environ.get("GITHUB_RUN_ID", ""), commit=os.environ.get("GITHUB_SHA", ""), pending_review_count=len(pending))
    write_json(directory / "agent_run_report.json", report)
    artifacts = {p.name: read_jsonl(p) if p.suffix == ".jsonl" else read_json(p) for p in directory.glob("agent_*") if p.suffix in {".json", ".jsonl"}}
    write_json(daily_path, {"signature": signature, "usage": usage.to_dict(), "reusable": bool(events) or status == "success_empty", "artifacts": artifacts})
    expiry = (datetime.fromisoformat(run_date) - timedelta(days=35)).date().isoformat()
    for path in daily_path.parent.glob("*.json"):
        if path.stem < expiry:
            path.unlink()
    return report
