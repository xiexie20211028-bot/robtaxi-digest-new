from __future__ import annotations

import argparse
import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from app.common import normalize_title, normalize_url, now_beijing, read_json, sha1_text, write_json, write_jsonl

from .contracts import AgentEvent, ProviderUsage
from .page_reader import GenericPageReader
from .providers import build_model_provider, build_search_provider, extract_json_object
from .verifier import DefaultEvidenceVerifier


COVERAGE_LABELS = {
    "robotaxi": "Robotaxi 商业运营、无人化部署、车队、订单、牌照与事故",
    "passenger_l3": "L3 乘用车准入、责任转移、量产、交付和上路",
    "passenger_l4": "L4 乘用车测试、准入、量产或公开道路部署",
    "core_supply_chain": "明确绑定上述项目、车型、定点、量产、认证的核心供应链",
    "regulation_safety": "监管政策、准入、召回、OTA、安全调查和事故",
}


SYSTEM_PROMPT = """你是国内 Robotaxi 与 L3/L4 乘用车行业研究员。
你的工作是发现指定北京时间窗口内发生的行业“事件”，不是罗列文章。
范围只包括 Robotaxi、L3 乘用车、L4 乘用车、直接绑定这些项目的核心供应链、监管与安全。
只研究中国市场或中国企业直接参与的事件；国内媒体报道的纯海外公司、纯海外市场事件不属于本任务。
排除 Robotruck、Robovan、矿区/港口无人车、普通 L2/L2+、准 L3、未来支持和营销预热。
搜索摘要只能用于发现线索；每个候选必须给出可访问的原始文章 URL 和明确发布时间。
优先寻找监管、公司公告或 IR；没有一手原文时至少寻找两家相互独立的可靠媒体。
不要输出思维过程。严格输出一个 JSON 对象，根字段为 events，每个事件包含 title、factual_summary、companies、coverage_domains、automation_level、event_type、deployment_stage、canonical_url、evidence、score_breakdown。evidence 每项包含 url、publisher、evidence_type、published_at_utc。score_breakdown 必须包含 industry_impact(0-30)、deployment_or_regulation(0-25)、scope_relevance(0-25)、evidence_quality(0-20)。"""


def _target_window(run_date: str) -> tuple[str, str]:
    tz = ZoneInfo("Asia/Shanghai")
    end = datetime.fromisoformat(run_date).replace(tzinfo=tz)
    start = end - timedelta(days=1)
    return start.isoformat(), end.isoformat()


def _company_hints(config: dict[str, Any]) -> list[str]:
    hints: list[str] = []
    for company in config.get("companies", []):
        if not isinstance(company, dict):
            continue
        region = str(company.get("region", ""))
        name = str(company.get("name", "")).strip()
        aliases = [str(value).strip() for value in company.get("aliases", []) if str(value).strip()]
        is_cn = region == "cn" or any("\u4e00" <= char <= "\u9fff" for char in f"{name}{''.join(aliases)}")
        if is_cn and name:
            hints.append(name)
    return hints[:24]


def _events_from_text(text: str) -> list[dict[str, Any]]:
    payload = extract_json_object(text)
    if not payload or "events" not in payload or not isinstance(payload.get("events"), list):
        raise ValueError("search_provider_invalid_events_json")
    rows = payload["events"]
    return [dict(row) for row in rows if isinstance(row, dict)]


def _candidate_key(row: dict[str, Any]) -> str:
    url = str(row.get("canonical_url", "")).strip()
    title = normalize_title(str(row.get("title", "")))
    return sha1_text(f"{url}|{title}")


def _dedupe_candidates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    title_keys: dict[str, str] = {}
    for raw in rows:
        row = dict(raw)
        title_key = normalize_title(str(row.get("title", "")))
        hinted_key = str(row.get("event_key", "")).strip()
        key = hinted_key if hinted_key in merged else _candidate_key(row)
        if title_key and title_key in title_keys:
            key = title_keys[title_key]
        if key not in merged:
            row["event_key"] = key
            merged[key] = row
            if title_key:
                title_keys[title_key] = key
            continue
        existing = merged[key]
        evidence = existing.get("evidence", []) if isinstance(existing.get("evidence", []), list) else []
        existing_primary = any(
            isinstance(value, dict)
            and str(value.get("evidence_type", "")) in {"regulator", "dataset", "filing", "company_newsroom"}
            for value in evidence
        )
        seen_urls = {str(value.get("url", "")) for value in evidence if isinstance(value, dict)}
        for value in row.get("evidence", []):
            if isinstance(value, dict) and str(value.get("url", "")) not in seen_urls:
                evidence.append(value)
                seen_urls.add(str(value.get("url", "")))
        existing["evidence"] = evidence
        incoming_primary = any(
            isinstance(value, dict)
            and str(value.get("evidence_type", "")) in {"regulator", "dataset", "filing", "company_newsroom"}
            for value in row.get("evidence", [])
        )
        if incoming_primary and not existing_primary and str(row.get("canonical_url", "")).strip():
            existing["canonical_url"] = str(row["canonical_url"]).strip()
        if len(str(row.get("factual_summary", ""))) > len(str(existing.get("factual_summary", ""))):
            existing["factual_summary"] = row.get("factual_summary")
    return list(merged.values())


def _event_identity_keys(event: AgentEvent) -> set[str]:
    """返回跨日去重使用的稳定标识，兼容链接或标题发生轻微变化。"""
    keys = {f"id:{event.event_id}"}
    canonical_url = event.canonical_url.rstrip("/")
    if canonical_url:
        keys.add(f"url:{canonical_url}")
    title_fingerprint = sha1_text(normalize_title(event.title))
    if title_fingerprint:
        keys.add(f"fingerprint:{title_fingerprint}")
    return keys


def _load_seen(state_root: Path) -> set[str]:
    path = state_root / "event_history.json"
    if not path.exists():
        return set()
    try:
        payload = read_json(path)
        records = payload.get("events", {}) if isinstance(payload.get("events", {}), dict) else {}
        seen: set[str] = set()
        for event_id, value in records.items():
            if not isinstance(value, dict):
                continue
            seen.add(f"id:{event_id}")
            canonical_url = str(value.get("canonical_url") or "").rstrip("/")
            fingerprint = str(value.get("fingerprint") or "")
            if canonical_url:
                seen.add(f"url:{canonical_url}")
            if fingerprint:
                seen.add(f"fingerprint:{fingerprint}")
        return seen
    except Exception:
        return set()


def _save_seen(state_root: Path, events: list[AgentEvent], run_date: str) -> None:
    path = state_root / "event_history.json"
    payload: dict[str, Any] = {"version": 1, "events": {}}
    if path.exists():
        try:
            payload = read_json(path)
        except Exception:
            pass
    records = payload.get("events", {}) if isinstance(payload.get("events", {}), dict) else {}
    cutoff = (datetime.fromisoformat(run_date) - timedelta(days=35)).date().isoformat()
    records = {
        str(key): value
        for key, value in records.items()
        if isinstance(value, dict) and str(value.get("last_seen_date", "")) >= cutoff
    }
    for event in events:
        records[event.event_id] = {
            "canonical_url": event.canonical_url,
            "fingerprint": sha1_text(normalize_title(event.title)),
            "last_seen_date": run_date,
        }
    write_json(path, {"version": 1, "events": records})


def _scan_prompt(run_date: str, config: dict[str, Any]) -> str:
    start, end = _target_window(run_date)
    topics = "\n".join(f"- {key}: {value}" for key, value in COVERAGE_LABELS.items())
    companies = "、".join(_company_hints(config))
    return f"""阶段一：行业扫描。
统计窗口为 [{start}, {end})，仅限事件实际发布时间落在该窗口；重要迟到事件可回看至窗口结束前 72 小时并明确标记。
请自主制定并改写搜索词，逐项检查：
{topics}
企业名称仅作线索提示而非白名单：{companies}。
最多返回 20 个具有明确事实增量的候选事件。"""


def _audit_prompt(run_date: str, candidates: list[dict[str, Any]]) -> str:
    covered = {str(value) for row in candidates for value in row.get("coverage_domains", []) if str(value)}
    missing = [key for key in COVERAGE_LABELS if key not in covered]
    titles = [str(row.get("title", ""))[:100] for row in candidates[:20]]
    return f"""阶段二：盲区审计。
运行日：{run_date}。第一轮已覆盖：{sorted(covered)}；尚未覆盖：{missing}。
已发现标题仅用于避免重复：{json.dumps(titles, ensure_ascii=False)}。
对每个未覆盖或证据薄弱领域至少换两个角度搜索。不要为了凑齐领域而收录无新闻或普通 L2 营销；只返回新增事件。"""


def _evidence_prompt(run_date: str, candidates: list[dict[str, Any]]) -> str:
    compact = [
        {
            "event_key": row.get("event_key", _candidate_key(row)),
            "title": row.get("title", ""),
            "canonical_url": row.get("canonical_url", ""),
            "evidence": row.get("evidence", []),
        }
        for row in candidates[:24]
    ]
    return f"""阶段三：证据整理。
运行日：{run_date}。请针对下面的候选寻找原始监管/企业证据；没有一手证据的，寻找第二家独立媒体。
对每个公司事件至少换一次“公司名+事件+官网/IR”或 site:官方域名搜索；不要把搜索摘要、聚合页或只转述其他媒体的页面当作原始证据。
合并重复事件并补全 URL 和发布时间，canonical_url 优先指向监管、公司新闻室或 IR 原文。只返回有证据增量的事件。
候选：{json.dumps(compact, ensure_ascii=False)}"""


def _score_candidates(
    model_provider: Any,
    candidates: list[dict[str, Any]],
    max_cost_cny: float | None = None,
) -> tuple[list[dict[str, Any]], ProviderUsage]:
    compact = []
    for row in candidates[:30]:
        evidence = []
        for value in row.get("evidence", [])[:4]:
            if isinstance(value, dict):
                evidence.append({key: value.get(key) for key in ("url", "publisher", "evidence_type", "published_at_utc")})
        compact.append(
            {
                "event_key": row.get("event_key", _candidate_key(row)),
                "title": row.get("title", ""),
                "factual_summary": row.get("factual_summary", row.get("summary", "")),
                "coverage_domains": row.get("coverage_domains", []),
                "evidence": evidence,
            }
        )
    prompt = f"""对以下事件按固定 100 分规则评分并纠正 factual_summary。不得增加新事件或新事实。
行业影响 0-30；落地或监管阶段 0-25；范围直接相关性 0-25；证据质量 0-20。
输出 JSON：{{"events":[{{"event_key":"...","factual_summary":"...","score_breakdown":{{"industry_impact":0,"deployment_or_regulation":0,"scope_relevance":0,"evidence_quality":0}}}}]}}。
事件：{json.dumps(compact, ensure_ascii=False)}"""
    payload, usage = model_provider.complete_json(
        "你是独立行业事件评分员，只依据给定事实和证据评分。",
        prompt,
        max_cost_cny=max_cost_cny,
    )
    scores = {
        str(row.get("event_key", "")): row
        for row in payload.get("events", [])
        if isinstance(row, dict) and str(row.get("event_key", ""))
    }
    for row in candidates:
        scored = scores.get(str(row.get("event_key", "")))
        if not scored:
            continue
        row["score_breakdown"] = scored.get("score_breakdown", row.get("score_breakdown", {}))
        if str(scored.get("factual_summary", "")).strip():
            row["factual_summary"] = str(scored.get("factual_summary", "")).strip()
    return candidates, usage


def _normalize_evidence_output(
    model_provider: Any,
    raw_text: str,
    search_trace: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    max_cost_cny: float | None = None,
) -> tuple[list[dict[str, Any]], ProviderUsage]:
    """将 Web Search 的非 JSON 最终文本归一化，搜索本身仍是唯一事实来源。"""
    compact_candidates = [
        {
            "event_key": row.get("event_key", _candidate_key(row)),
            "title": row.get("title", ""),
            "factual_summary": row.get("factual_summary", row.get("summary", "")),
            "canonical_url": row.get("canonical_url", ""),
            "evidence": row.get("evidence", [])[:4],
        }
        for row in candidates[:24]
    ]
    prompt = f"""Web Search 已完成证据搜索，但最终文本不是可解析 JSON。
仅根据下面的候选、搜索文本和搜索结果 URL 归一化结构；不得搜索、不得新增未出现的事实或 URL。
保留对应候选的 event_key。输出 JSON：{{"events":[{{"event_key":"...","title":"...","factual_summary":"...","companies":[],"coverage_domains":[],"automation_level":"L3|L4|unknown","event_type":"...","deployment_stage":"...","canonical_url":"...","evidence":[{{"url":"...","publisher":"...","evidence_type":"...","published_at_utc":"..."}}],"score_breakdown":{{}}}}]}}。
候选：{json.dumps(compact_candidates, ensure_ascii=False)}
搜索文本：{str(raw_text)[:50000]}
搜索结果：{json.dumps(search_trace, ensure_ascii=False)[:50000]}"""
    payload, usage = model_provider.complete_json(
        "你是搜索结果结构化器，不是新的发现 Agent。只输出 JSON。",
        prompt,
        max_cost_cny=max_cost_cny,
    )
    rows = payload.get("events", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        raise RuntimeError("evidence_normalizer_invalid_events_json")
    candidate_map = {
        str(row.get("event_key", _candidate_key(row))): dict(row)
        for row in candidates
    }
    allowed_urls = {
        normalize_url(match.rstrip(".,);]}>\"'"))
        for match in re.findall(r"https?://[^\s<]+", str(raw_text))
        if normalize_url(match.rstrip(".,);]}>\"'"))
    }
    for trace_row in search_trace:
        if not isinstance(trace_row, dict):
            continue
        allowed_urls.update(
            normalize_url(str(value))
            for value in trace_row.get("urls", [])
            if normalize_url(str(value))
        )
    for candidate in candidates:
        allowed_urls.add(normalize_url(str(candidate.get("canonical_url", ""))))
        allowed_urls.update(
            normalize_url(str(value.get("url", "")))
            for value in candidate.get("evidence", [])
            if isinstance(value, dict) and normalize_url(str(value.get("url", "")))
        )
    allowed_urls.discard("")

    normalized: list[dict[str, Any]] = []
    for raw_row in rows:
        if not isinstance(raw_row, dict):
            continue
        event_key = str(raw_row.get("event_key", ""))
        if event_key not in candidate_map:
            continue
        row = dict(candidate_map[event_key])
        evidence = [
            dict(value)
            for value in raw_row.get("evidence", [])
            if isinstance(value, dict) and normalize_url(str(value.get("url", ""))) in allowed_urls
        ]
        if evidence:
            row["evidence"] = evidence
        canonical = normalize_url(str(raw_row.get("canonical_url", "")))
        if canonical in allowed_urls:
            row["canonical_url"] = canonical
        row["event_key"] = event_key
        normalized.append(row)
    return normalized, usage


def run_agent(
    run_date: str,
    config: dict[str, Any],
    out_root: Path,
    state_root: Path,
    model_provider: Any | None = None,
    search_provider: Any | None = None,
    verifier: Any | None = None,
) -> dict[str, Any]:
    settings = config.get("industry_agent", {}) if isinstance(config.get("industry_agent", {}), dict) else {}
    run_id = f"agent_{run_date}_{uuid.uuid4().hex[:12]}"
    out_dir = out_root / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    state_root.mkdir(parents=True, exist_ok=True)
    events_file = out_dir / "agent_events.jsonl"
    trace_file = out_dir / "agent_trace.jsonl"
    report_file = out_dir / "agent_run_report.json"

    usage = ProviderUsage()
    traces: list[dict[str, Any]] = []
    errors: list[str] = []
    dropped: dict[str, int] = {}
    candidates: list[dict[str, Any]] = []
    status = "failed"
    budget = float(settings.get("daily_budget_cny", 2.0))
    max_searches = int(settings.get("max_web_searches", 20))
    search_overrun_reserve = max(0, int(settings.get("search_overrun_reserve", 4)))
    model_provider = model_provider or build_model_provider(settings)
    search_provider = search_provider or build_search_provider(settings)
    verifier = verifier or DefaultEvidenceVerifier(GenericPageReader(), config)

    try:
        ok, probe_usage, probe_trace = search_provider.probe(max_cost_cny=max(0.0, budget - usage.estimated_cost_cny))
        usage.add(probe_usage)
        if usage.estimated_cost_cny > budget:
            raise RuntimeError("hard_budget_exceeded_after_capability_probe")
        traces.extend({"stage": "capability_probe", **row} for row in probe_trace)
        if not ok:
            raise RuntimeError("DeepSeek Web Search capability unavailable")

        stages = [
            # 前两轮刻意收紧，为最后的原文证据搜索保留额度。
            ("scan", lambda rows: _scan_prompt(run_date, config), 5),
            ("coverage_audit", lambda rows: _audit_prompt(run_date, rows), 2),
            ("evidence", lambda rows: _evidence_prompt(run_date, rows), 6),
        ]
        completed_stages: set[str] = set()
        for stage, prompt_builder, stage_limit in stages:
            available = max_searches - usage.web_searches
            if stage == "coverage_audit":
                # 服务端最多观测到比 max_uses 多 4 次。当剩余额度不足以
                # 同时覆盖盲区审计和证据整理时，优先保证后者。
                current_worst = min(stage_limit, max(0, available - search_overrun_reserve)) + search_overrun_reserve
                evidence_worst = 2 + search_overrun_reserve
                if available < current_worst + evidence_worst:
                    traces.append({"stage": stage, "type": "stage_skipped", "reason": "reserved_for_evidence"})
                    continue
            # DeepSeek 服务端工具曾在 max_uses=5 时实际调用 7 次。为当前
            # 请求预留溢出量，避免总搜索数突破全局上限。
            remaining = max_searches - usage.web_searches - search_overrun_reserve
            if remaining <= 0 or usage.estimated_cost_cny >= budget:
                break
            try:
                result = search_provider.research(
                    SYSTEM_PROMPT,
                    prompt_builder(candidates),
                    min(stage_limit, remaining),
                    max_cost_cny=max(0.0, budget - usage.estimated_cost_cny),
                )
            except Exception as exc:
                if "hard_budget_exceeded" in str(exc):
                    raise
                if "budget_preflight_rejected" in str(exc):
                    status = "partial_budget"
                    break
                errors.append(f"{stage}_degraded:{str(exc)[:180]}")
                if not candidates:
                    raise
                status = "degraded"
                break
            usage.add(result.usage)
            traces.extend({"stage": stage, **row} for row in result.trace)
            if usage.web_searches > max_searches:
                raise RuntimeError(f"hard_search_limit_exceeded:{usage.web_searches}>{max_searches}")
            if usage.estimated_cost_cny > budget:
                raise RuntimeError(f"hard_budget_exceeded_after_{stage}")
            try:
                stage_candidates = _events_from_text(result.text)
            except ValueError as exc:
                if stage == "evidence" and candidates and str(result.text).strip():
                    try:
                        stage_candidates, normalize_usage = _normalize_evidence_output(
                            model_provider,
                            result.text,
                            result.trace,
                            candidates,
                            max_cost_cny=max(0.0, budget - usage.estimated_cost_cny),
                        )
                        usage.add(normalize_usage)
                        if usage.estimated_cost_cny > budget:
                            raise RuntimeError("hard_budget_exceeded_after_evidence_normalization")
                        traces.append(
                            {
                                "stage": stage,
                                "type": "output_normalized",
                                "raw_text_sha1": sha1_text(result.text),
                            }
                        )
                    except Exception as normalize_exc:
                        failed_usage = getattr(normalize_exc, "usage", None)
                        if isinstance(failed_usage, ProviderUsage):
                            usage.add(failed_usage)
                        errors.append(f"{stage}_invalid_output:{exc}")
                        errors.append(f"{stage}_normalizer_failed:{str(normalize_exc)[:180]}")
                        status = "degraded"
                        break
                else:
                    errors.append(f"{stage}_invalid_output:{exc}")
                    if not candidates:
                        raise RuntimeError(f"{stage}_invalid_output") from exc
                    status = "degraded"
                    break
            candidates = _dedupe_candidates(candidates + stage_candidates)
            completed_stages.add(stage)
            if usage.estimated_cost_cny >= budget:
                status = "partial_budget"
                break

        if "evidence" not in completed_stages:
            errors.append("evidence_stage_not_completed")
            status = "degraded"

        if candidates and usage.estimated_cost_cny < budget:
            try:
                candidates, score_usage = _score_candidates(
                    model_provider,
                    candidates,
                    max_cost_cny=max(0.0, budget - usage.estimated_cost_cny),
                )
                usage.add(score_usage)
                if usage.estimated_cost_cny > budget:
                    raise RuntimeError("hard_budget_exceeded_after_scoring")
                if usage.estimated_cost_cny >= budget:
                    status = "partial_budget"
            except Exception as exc:
                failed_usage = getattr(exc, "usage", None)
                if isinstance(failed_usage, ProviderUsage):
                    usage.add(failed_usage)
                if "budget_preflight_rejected" in str(exc):
                    status = "partial_budget"
                    errors.append("score_skipped_budget_preflight")
                else:
                    errors.append(f"score_degraded:{str(exc)[:180]}")
                    status = "degraded"

        seen = _load_seen(state_root)
        accepted_identities: set[str] = set()
        verified_events: list[AgentEvent] = []
        late_count = 0
        for candidate in candidates:
            event, reason = verifier.verify(candidate, run_date, run_id)
            traces.append(
                {
                    "stage": "candidate_verification",
                    "event_key": str(candidate.get("event_key", _candidate_key(candidate))),
                    "title": str(candidate.get("title", ""))[:200],
                    "canonical_url": str(candidate.get("canonical_url", ""))[:1000],
                    "evidence_urls": [
                        str(value.get("url", ""))[:1000]
                        for value in candidate.get("evidence", [])[:4]
                        if isinstance(value, dict)
                    ],
                    "result": reason,
                }
            )
            if event is None:
                dropped[reason] = dropped.get(reason, 0) + 1
                continue
            # 事件记录实际使用的实现，后续切换 Provider 时不沿用默认名称。
            event.model_provider = getattr(model_provider, "name", "unknown")
            event.search_provider = getattr(search_provider, "name", "unknown")
            identity_keys = _event_identity_keys(event)
            if identity_keys & seen or identity_keys & accepted_identities:
                dropped["seen_within_35_days"] = dropped.get("seen_within_35_days", 0) + 1
                continue
            if event.late_arrival:
                if late_count >= int(settings.get("late_arrival_max_items", 2)):
                    dropped["late_arrival_cap"] = dropped.get("late_arrival_cap", 0) + 1
                    continue
                late_count += 1
            verified_events.append(event)
            accepted_identities.update(identity_keys)

        # 同 canonical URL 仅保留得分最高事件。
        best: dict[str, AgentEvent] = {}
        for event in verified_events:
            key = event.canonical_url or normalize_title(event.title)
            if key not in best or event.importance_score > best[key].importance_score:
                best[key] = event
        verified_events = sorted(best.values(), key=lambda row: (row.importance_score, row.published_at_utc), reverse=True)
        write_jsonl(events_file, [row.to_dict() for row in verified_events])
        _save_seen(state_root, verified_events, run_date)

        infrastructure_drops = {
            "no_accessible_date_verified_evidence",
            "missing_evidence_url",
        }
        if not verified_events and candidates and any(reason in dropped for reason in infrastructure_drops):
            status = "degraded"
        elif status not in {"partial_budget", "degraded"}:
            status = "success" if verified_events else "success_empty"
    except Exception as exc:
        errors.append(str(exc)[:300])
        write_jsonl(events_file, [])
        status = "failed"

    write_jsonl(trace_file, traces)
    report = {
        "schema_version": "industry-agent-run-v1",
        "agent_run_id": run_id,
        "run_date": run_date,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "model_provider": getattr(model_provider, "name", "unknown"),
        "search_provider": getattr(search_provider, "name", "unknown"),
        "model": str(settings.get("model", "deepseek-v4-flash")),
        "candidate_count": len(candidates),
        "verified_event_count": sum(1 for _ in events_file.read_text(encoding="utf-8").splitlines() if _.strip()),
        "drop_reasons": dropped,
        "usage": usage.to_dict(),
        "budget_cny": budget,
        "budget_exhausted": status == "partial_budget",
        "errors": errors,
        "events_output": str(events_file),
        "trace_output": str(trace_file),
    }
    write_json(report_file, report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="运行国内 Robotaxi 与 L3/L4 行业研究 Agent")
    parser.add_argument("--date", default="", help="运行日 YYYY-MM-DD；研究窗口为前一北京时间自然日")
    parser.add_argument("--config", default="./sources.json")
    parser.add_argument("--out", default="./.agent-handoff")
    parser.add_argument("--state", default="./.state-agent")
    args = parser.parse_args()

    run_date = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    config = read_json(Path(args.config).expanduser().resolve())
    report = run_agent(
        run_date,
        config,
        Path(args.out).expanduser().resolve(),
        Path(args.state).expanduser().resolve(),
    )
    print(
        f"[industry_agent] date={run_date} status={report['status']} "
        f"events={report['verified_event_count']} searches={report['usage']['web_searches']} "
        f"cost_cny={report['usage']['estimated_cost_cny']:.4f}"
    )
    # 业务失败也要保留可供生产降级和复盘的产物。
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
