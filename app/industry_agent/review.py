from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from app.common import normalize_title, now_beijing, parse_datetime, read_json, read_jsonl, sha1_text, tokenize, write_json
from app.taxonomy import classify_industry_item

from .providers import build_model_provider


STRICT_SUCCESS_STATUSES = {"success", "success_empty"}
FAILURE_STATUSES = {"failed", "missing"}


def _find(root: Path, name: str, date_text: str) -> Path | None:
    if not root.exists():
        return None
    candidates = [path for path in root.rglob(name) if date_text in path.parts or date_text in str(path)]
    if not candidates:
        return None

    def recency(path: Path) -> tuple[int, float]:
        # Actions 下载目录的首层是单调递增的 run ID。不能依赖下载后的
        # mtime，因为较旧的运行后下载，反而会获得更新的本地时间。
        try:
            first = path.relative_to(root).parts[0]
            run_id = int(first) if first.isdigit() else -1
        except (ValueError, IndexError):
            run_id = -1
        return run_id, path.stat().st_mtime

    return max(candidates, key=recency)


def _normalize_agent(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": str(row.get("title", "")),
        "summary": str(row.get("factual_summary", "")),
        "canonical_url": str(row.get("canonical_url", "")),
        "published_at_utc": str(row.get("published_at_utc", "")),
        "first_seen_at_utc": str(row.get("first_seen_at_utc", "")),
        "late_arrival": bool(row.get("late_arrival", False)),
        "coverage_domains": list(row.get("coverage_domains", [])),
        "importance": int(row.get("importance_score", 0) or 0),
        "evidence": list(row.get("evidence", [])),
        "region": "domestic",
        "origins": {"agent"},
    }


def _normalize_brief(row: dict[str, Any], origin: str) -> dict[str, Any]:
    return {
        "title": str(row.get("title_zh", row.get("title", ""))),
        "summary": str(row.get("summary_zh", row.get("content", ""))),
        "canonical_url": str(row.get("canonical_url", row.get("link", ""))),
        "published_at_utc": str(row.get("published_at_utc", "")),
        "first_seen_at_utc": str(row.get("first_seen_at_utc", "")),
        "late_arrival": bool(row.get("late_arrival", False)),
        "coverage_domains": list(row.get("coverage_domains", [])),
        "importance": int(row.get("importance", 3) or 3) * 20,
        "evidence": list(row.get("evidence", [])),
        "region": str(row.get("region", "")),
        "origins": {origin},
    }


def _title_similarity(left: str, right: str) -> float:
    a = set(tokenize(normalize_title(left)))
    b = set(tokenize(normalize_title(right)))
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def cluster_events(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    clusters: list[dict[str, Any]] = []
    for row in rows:
        url = str(row.get("canonical_url", "")).strip()
        matched = None
        for cluster in clusters:
            same_url = bool(url and url == str(cluster.get("canonical_url", "")))
            same_title = _title_similarity(str(row.get("title", "")), str(cluster.get("title", ""))) >= 0.58
            if same_url or same_title:
                matched = cluster
                break
        if matched is None:
            clone = dict(row)
            clone["origins"] = set(row.get("origins", set()))
            clusters.append(clone)
            continue
        matched["origins"].update(row.get("origins", set()))
        if int(row.get("importance", 0)) > int(matched.get("importance", 0)):
            preserved_origins = set(matched["origins"])
            matched.update(row)
            matched["origins"] = preserved_origins
        evidence = matched.get("evidence", []) if isinstance(matched.get("evidence", []), list) else []
        urls = {str(value.get("url", "")) for value in evidence if isinstance(value, dict)}
        for value in row.get("evidence", []):
            if isinstance(value, dict) and str(value.get("url", "")) not in urls:
                evidence.append(value)
        matched["evidence"] = evidence
    for cluster in clusters:
        identity = f"{cluster.get('canonical_url', '')}|{normalize_title(str(cluster.get('title', '')))}"
        cluster["blind_id"] = f"ev_{sha1_text(identity)[:12]}"
    return clusters


def _fallback_judgement(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    judged: dict[str, dict[str, Any]] = {}
    broad_source = {
        "coverage_domains": ["robotaxi", "passenger_l3", "passenger_l4", "core_supply_chain", "regulation_safety"],
        "evidence_type": "industry_media",
    }
    for event in events:
        scope = classify_industry_item(
            {"title": event.get("title", ""), "content": event.get("summary", "")},
            broad_source,
        )
        judged[str(event["blind_id"])] = {
            "in_scope": bool(scope.get("in_scope")),
            "important": int(event.get("importance", 0)) >= 65,
            "reason": "deterministic_fallback",
        }
    return judged


def blind_judge(events: list[dict[str, Any]], model_provider: Any | None) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    # 不向评审模型暴露候选来自 Agent、legacy 还是 optimized。
    blinded = [
        {
            "blind_id": event["blind_id"],
            "title": event.get("title", ""),
            "summary": event.get("summary", ""),
            "published_at_utc": event.get("published_at_utc", ""),
            "coverage_domains": event.get("coverage_domains", []),
            "evidence_urls": [
                str(value.get("url", ""))
                for value in event.get("evidence", [])
                if isinstance(value, dict) and str(value.get("url", ""))
            ],
        }
        for event in sorted(events, key=lambda value: str(value["blind_id"]))
    ]
    if not blinded:
        return {}, {"provider": "none", "fallback": False, "complete": True, "empty": True}
    if model_provider is None:
        return _fallback_judgement(events), {"provider": "deterministic", "fallback": True, "complete": False}
    prompt = f"""盲评下面的行业事件，不要猜测发现渠道。判断是否属于国内 Robotaxi、L3/L4 乘用车、直接相关供应链或监管安全；排除 Robotruck、Robovan 和普通 L2 营销。再判断它是否属于值得进入每日产业简报的重要事实增量。
输出 JSON：{{"events":[{{"blind_id":"...","in_scope":true,"important":true,"reason":"简短原因"}}]}}。
候选：{json.dumps(blinded, ensure_ascii=False)}"""
    try:
        payload, usage = model_provider.complete_json("你是独立行业事件评审员。只依据候选事实和证据，保持来源盲态。", prompt)
        judged = {
            str(row.get("blind_id", "")): {
                "in_scope": bool(row.get("in_scope", False)),
                "important": bool(row.get("important", False)),
                "reason": str(row.get("reason", ""))[:160],
            }
            for row in payload.get("events", [])
            if isinstance(row, dict) and str(row.get("blind_id", ""))
        }
        expected_ids = {str(event["blind_id"]) for event in events}
        missing_ids = sorted(expected_ids - set(judged))
        if missing_ids:
            fallback = _fallback_judgement(events)
            for event_id in missing_ids:
                judged[event_id] = fallback[event_id]
        return judged, {
            "provider": getattr(model_provider, "name", "unknown"),
            "fallback": bool(missing_ids),
            "complete": not missing_ids,
            "missing_event_ids": missing_ids,
            "usage": usage.to_dict(),
        }
    except Exception as exc:
        return _fallback_judgement(events), {
            "provider": "deterministic",
            "fallback": True,
            "complete": False,
            "error": str(exc)[:200],
        }


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return float(ordered[max(0, math.ceil(len(ordered) * 0.95) - 1)])


def evaluate_agent_rollout(history: dict[str, Any], settings: dict[str, Any]) -> dict[str, Any]:
    days = history.get("days", []) if isinstance(history.get("days", []), list) else []
    minimum = int(settings.get("minimum_days", 14))
    valid_days = [day for day in days if isinstance(day, dict) and bool(day.get("valid_statistical_day", False))]
    sample = valid_days[-minimum:]
    success_days = sum(str(day.get("agent_status", "")) in STRICT_SUCCESS_STATUSES for day in sample)
    consecutive_failures = 0
    max_consecutive_failures = 0
    for day in sample:
        if str(day.get("agent_status", "")) in FAILURE_STATUSES:
            consecutive_failures += 1
            max_consecutive_failures = max(max_consecutive_failures, consecutive_failures)
        else:
            consecutive_failures = 0

    truth = sum(int(day.get("truth_important", 0)) for day in sample)
    agent_tp = sum(int(day.get("agent_true_positive", 0)) for day in sample)
    agent_selected = sum(int(day.get("agent_selected", 0)) for day in sample)
    legacy_important = sum(int(day.get("legacy_important", 0)) for day in sample)
    legacy_reproduced = sum(int(day.get("legacy_reproduced", 0)) for day in sample)
    verified = sum(int(day.get("agent_url_date_verified", 0)) for day in sample)
    strong = sum(int(day.get("agent_strong_evidence", 0)) for day in sample)
    costs = [float(day.get("agent_cost_cny", 0.0)) for day in sample]
    has_evaluation_volume = truth > 0 and agent_selected > 0 and legacy_important > 0
    metrics = {
        "history_days": len(sample),
        "total_history_days": len(days),
        "success_days": success_days,
        "max_consecutive_failures": max_consecutive_failures,
        "truth_important": truth,
        "agent_selected": agent_selected,
        "legacy_important": legacy_important,
        "important_recall": round(agent_tp / truth, 4) if truth else 0.0,
        "precision": round(agent_tp / agent_selected, 4) if agent_selected else 0.0,
        "legacy_reproduction": round(legacy_reproduced / legacy_important, 4) if legacy_important else 0.0,
        "url_date_verification": round(verified / agent_selected, 4) if agent_selected else 0.0,
        "strong_evidence_share": round(strong / agent_selected, 4) if agent_selected else 0.0,
        "daily_cost_p95_cny": round(_p95(costs), 4),
    }
    automatic_checks = {
        "minimum_days": metrics["history_days"] >= minimum,
        "evaluation_volume": has_evaluation_volume,
        "success_days": metrics["success_days"] >= max(1, minimum - 1),
        "no_consecutive_two_day_failure": metrics["max_consecutive_failures"] < 2,
        "important_recall": metrics["important_recall"] >= float(settings.get("important_recall_min", 0.90)),
        "precision": metrics["precision"] >= float(settings.get("precision_min", 0.85)),
        "legacy_reproduction": metrics["legacy_reproduction"] >= float(settings.get("legacy_reproduction_min", 0.90)),
        "url_date_verification": metrics["url_date_verification"] >= float(settings.get("url_date_verification_min", 0.95)),
        "strong_evidence_share": metrics["strong_evidence_share"] >= float(settings.get("strong_evidence_share_min", 0.90)),
        "daily_cost_p95": metrics["daily_cost_p95_cny"] <= float(settings.get("daily_budget_p95_cny", 2.0)),
    }
    return {
        "schema_version": "industry-agent-rollout-gate-v1",
        "ready_for_manual_approval": all(automatic_checks.values()),
        "passed": False,
        "automatic_checks": automatic_checks,
        "manual_check": {"approved": False, "overturn_rate": None},
        "metrics": metrics,
    }


def _belongs_to_run_window(row: dict[str, Any], run_date: str) -> bool:
    published = str(row.get("published_at_utc", "")).strip()
    if not published:
        return False
    tz = ZoneInfo("Asia/Shanghai")
    end = datetime.fromisoformat(run_date).replace(tzinfo=tz)
    start = end - timedelta(days=1)
    try:
        value = parse_datetime(published).astimezone(tz)
    except Exception:
        return False
    return start <= value < end


def _is_next_day(base_date: str, lookback_date: str) -> bool:
    try:
        return (date.fromisoformat(lookback_date) - date.fromisoformat(base_date)).days == 1
    except ValueError:
        return False


def _difference_label(event: dict[str, Any]) -> str:
    origins = set(event.get("origins", set()))
    baseline = bool(origins.intersection({"legacy", "optimized"}))
    if "agent" in origins and not baseline:
        return "agent_only"
    if "agent" not in origins and baseline:
        return "baseline_only"
    return "matched"


def _manual_candidates(
    truth_events: list[dict[str, Any]],
    judgements: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "event_id": str(event["blind_id"]),
            "title": str(event.get("title", "")),
            "canonical_url": str(event.get("canonical_url", "")),
            "difference": _difference_label(event),
            "importance": int(event.get("importance", 0)),
            "judge": judgements.get(str(event["blind_id"]), {}),
        }
        for event in truth_events
    ]


def _aggregate_manual_samples(history: dict[str, Any], minimum_days: int, limit: int) -> list[dict[str, Any]]:
    valid_days = [
        day
        for day in history.get("days", [])
        if isinstance(day, dict) and bool(day.get("valid_statistical_day", False))
    ][-minimum_days:]
    candidates: dict[str, dict[str, Any]] = {}
    for day in valid_days:
        for row in day.get("manual_candidates", []):
            if not isinstance(row, dict) or not str(row.get("event_id", "")):
                continue
            event_id = str(row["event_id"])
            existing = candidates.get(event_id)
            if existing is None or int(row.get("importance", 0)) > int(existing.get("importance", 0)):
                candidates[event_id] = dict(row)
    return sorted(
        candidates.values(),
        key=lambda row: (str(row.get("difference", "")) != "matched", int(row.get("importance", 0))),
        reverse=True,
    )[:limit]


def run_review(
    date_text: str,
    config: dict[str, Any],
    agent_root: Path,
    legacy_root: Path,
    optimized_root: Path,
    state_file: Path,
    out_root: Path,
    model_provider: Any | None,
    lookback_date: str = "",
) -> dict[str, Any]:
    agent_events_file = _find(agent_root, "agent_events.jsonl", date_text)
    agent_report_file = _find(agent_root, "agent_run_report.json", date_text)
    legacy_file = _find(legacy_root, "brief_items.jsonl", date_text)
    optimized_file = _find(optimized_root, "brief_items.jsonl", date_text)
    legacy_report_file = _find(legacy_root, "run_report.json", date_text)

    lookback_agent_file = _find(agent_root, "agent_events.jsonl", lookback_date) if lookback_date else None
    lookback_agent_report = _find(agent_root, "agent_run_report.json", lookback_date) if lookback_date else None
    lookback_legacy_file = _find(legacy_root, "brief_items.jsonl", lookback_date) if lookback_date else None
    lookback_legacy_report = _find(legacy_root, "run_report.json", lookback_date) if lookback_date else None
    lookback_optimized_file = _find(optimized_root, "brief_items.jsonl", lookback_date) if lookback_date else None

    base_artifacts = {
        "agent_events": bool(agent_events_file),
        "agent_report": bool(agent_report_file),
        "legacy_brief": bool(legacy_file),
        "legacy_report": bool(legacy_report_file),
        "optimized_brief": bool(optimized_file),
    }
    lookback_artifacts = {
        "agent_events": bool(lookback_agent_file),
        "agent_report": bool(lookback_agent_report),
        "legacy_brief": bool(lookback_legacy_file),
        "legacy_report": bool(lookback_legacy_report),
        "optimized_brief": bool(lookback_optimized_file),
    }
    agent_rows = [_normalize_agent(row) for row in read_jsonl(agent_events_file)] if agent_events_file else []
    legacy_rows = [_normalize_brief(row, "legacy") for row in read_jsonl(legacy_file)] if legacy_file else []
    optimized_rows = [_normalize_brief(row, "optimized") for row in read_jsonl(optimized_file)] if optimized_file else []
    # 用次日三条链路产物回看同一发布窗口，补入搜索索引延迟事件。
    lookback_rows: list[dict[str, Any]] = []
    if lookback_date:
        lookback_rows.extend(
            row
            for row in (
                [_normalize_agent(value) for value in read_jsonl(lookback_agent_file)]
                if lookback_agent_file
                else []
            )
            if _belongs_to_run_window(row, date_text)
        )
        lookback_rows.extend(
            row
            for row in (
                [_normalize_brief(value, "legacy") for value in read_jsonl(lookback_legacy_file)]
                if lookback_legacy_file
                else []
            )
            if _belongs_to_run_window(row, date_text)
        )
        lookback_rows.extend(
            row
            for row in (
                [_normalize_brief(value, "optimized") for value in read_jsonl(lookback_optimized_file)]
                if lookback_optimized_file
                else []
            )
            if _belongs_to_run_window(row, date_text)
        )

    # 第一版 Agent 只改造国内发现链，不能用海外 legacy 新闻惩罚它的召回率。
    domestic_rows = [
        row
        for row in agent_rows + legacy_rows + optimized_rows + lookback_rows
        if str(row.get("region", "")) == "domestic"
    ]
    events = cluster_events(domestic_rows)
    judgements, judge_meta = blind_judge(events, model_provider)
    truth_events = [
        event
        for event in events
        if judgements.get(str(event["blind_id"]), {}).get("in_scope")
        and judgements.get(str(event["blind_id"]), {}).get("important")
    ]
    agent_selected = [event for event in events if "agent" in event["origins"]]
    agent_tp = [event for event in truth_events if "agent" in event["origins"]]
    legacy_important = [event for event in truth_events if "legacy" in event["origins"]]
    legacy_reproduced = [event for event in legacy_important if "agent" in event["origins"]]
    agent_report = read_json(agent_report_file) if agent_report_file else {"status": "missing", "usage": {}}
    legacy_report = read_json(legacy_report_file) if legacy_report_file else {}
    url_date_verified = sum(
        bool(event.get("canonical_url"))
        and any(
            isinstance(value, dict) and value.get("accessible") and value.get("date_verified")
            for value in event.get("evidence", [])
        )
        for event in agent_selected
    )
    strong = sum(
        any(
            isinstance(value, dict) and str(value.get("evidence_type", "")) in {"regulator", "dataset", "filing", "company_newsroom"}
            for value in event.get("evidence", [])
        )
        or len(
            {
                str(value.get("publisher", "")).lower()
                for value in event.get("evidence", [])
                if isinstance(value, dict) and str(value.get("publisher", "")).strip()
            }
        ) >= 2
        for event in agent_selected
    )
    delays: list[float] = []
    delayed_48h = 0
    for event in agent_selected:
        published = str(event.get("published_at_utc", ""))
        first_seen = str(event.get("first_seen_at_utc", ""))
        if not published or not first_seen:
            continue
        try:
            delay = max(0.0, (parse_datetime(first_seen) - parse_datetime(published)).total_seconds() / 3600)
        except Exception:
            continue
        delays.append(delay)
        if delay > 48:
            delayed_48h += 1
    agent_usage = agent_report.get("usage", {}) if isinstance(agent_report.get("usage", {}), dict) else {}
    source_stats = legacy_report.get("source_stats", []) if isinstance(legacy_report.get("source_stats", []), list) else []
    old_source_incidents = sum(
        1
        for value in source_stats
        if isinstance(value, dict) and str(value.get("status", "")) in {"degraded", "failed", "silent_dead"}
    )
    agent_drop_reasons = agent_report.get("drop_reasons", {}) if isinstance(agent_report.get("drop_reasons", {}), dict) else {}
    invalid_evidence = int(agent_drop_reasons.get("missing_evidence_url", 0)) + int(
        agent_drop_reasons.get("no_accessible_date_verified_evidence", 0)
    ) + int(agent_drop_reasons.get("evidence_content_mismatch", 0))
    valid_statistical_day = (
        all(base_artifacts.values())
        and bool(lookback_date)
        and _is_next_day(date_text, lookback_date)
        and all(lookback_artifacts.values())
        and bool(judge_meta.get("complete", False))
        and not bool(judge_meta.get("fallback", False))
    )
    day = {
        "date": date_text,
        "lookback_date": lookback_date,
        "valid_statistical_day": valid_statistical_day,
        "base_artifacts": base_artifacts,
        "lookback_artifacts": lookback_artifacts,
        "judge_complete": bool(judge_meta.get("complete", False)),
        "judge_fallback": bool(judge_meta.get("fallback", False)),
        "agent_status": str(agent_report.get("status", "missing")),
        "truth_important": len(truth_events),
        "agent_true_positive": len(agent_tp),
        "agent_selected": len(agent_selected),
        "legacy_important": len(legacy_important),
        "legacy_reproduced": len(legacy_reproduced),
        "agent_url_date_verified": url_date_verified,
        "agent_strong_evidence": strong,
        "agent_cost_cny": float(agent_report.get("usage", {}).get("estimated_cost_cny", 0.0)),
        "agent_web_searches": int(agent_usage.get("web_searches", 0)),
        "agent_input_tokens": int(agent_usage.get("input_tokens", 0)),
        "agent_output_tokens": int(agent_usage.get("output_tokens", 0)),
        "average_discovery_delay_hours": round(sum(delays) / len(delays), 2) if delays else 0.0,
        "delayed_over_48h": delayed_48h,
        "lookback_event_count": len(lookback_rows),
        "invalid_evidence_count": invalid_evidence,
        "old_source_incidents": old_source_incidents,
        "coverage_distribution": {
            domain: sum(domain in event.get("coverage_domains", []) for event in agent_selected)
            for domain in ("robotaxi", "passenger_l3", "passenger_l4", "core_supply_chain", "regulation_safety")
        },
        "manual_candidates": _manual_candidates(truth_events, judgements),
    }
    history: dict[str, Any] = {"version": 1, "days": []}
    if state_file.exists():
        try:
            history = read_json(state_file)
        except Exception:
            pass
    days = history.get("days", []) if isinstance(history.get("days", []), list) else []
    days = [value for value in days if isinstance(value, dict) and str(value.get("date", "")) != date_text]
    days.append(day)
    days.sort(key=lambda value: str(value.get("date", "")))
    history["days"] = days[-35:]
    write_json(state_file, history)

    review_settings = config.get("industry_agent", {}).get("review", {})
    gate = evaluate_agent_rollout(history, review_settings)
    samples = _aggregate_manual_samples(
        history,
        int(review_settings.get("minimum_days", 14)),
        int(review_settings.get("max_manual_samples", 20)),
    )
    review_id = f"ar_{date_text.replace('-', '')}_{sha1_text('|'.join(str(value['event_id']) for value in samples))[:8]}"
    output = {
        "schema_version": "industry-agent-review-v1",
        "review_id": review_id,
        "date": date_text,
        "history_days": int(gate["metrics"]["history_days"]),
        "total_history_days": len(history["days"]),
        "daily": day,
        "gate": gate,
        "judge": judge_meta,
        "manual_samples": samples,
    }
    out_dir = out_root / date_text
    out_dir.mkdir(parents=True, exist_ok=True)
    write_json(out_dir / "daily_review.json", output)
    write_json(out_dir / "rollout_gate.json", {"review_id": review_id, **gate})
    marker = json.dumps(
        {
            "review_id": review_id,
            "ready_for_manual_approval": gate["ready_for_manual_approval"],
            "manual_sample_count": len(samples),
            "manual_sample_event_ids": [str(value["event_id"]) for value in samples],
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    lines = [
        f"# 国内行业 Agent 自动复盘（{date_text}）",
        "",
        f"<!-- agent-review {marker} -->",
        "",
        f"- review_id：`{review_id}`",
        f"- 已积累有效统计日：{gate['metrics']['history_days']} 天（总记录 {len(history['days'])} 天）",
        f"- 当日统计有效：{'是' if day['valid_statistical_day'] else '否'}；次日回看：{lookback_date or '缺失'}",
        f"- 当日 Agent 状态：{day['agent_status']}",
        f"- 重要事件召回率：{gate['metrics']['important_recall']:.1%}",
        f"- 精度：{gate['metrics']['precision']:.1%}",
        f"- 旧流程重要事件复现率：{gate['metrics']['legacy_reproduction']:.1%}",
        f"- 日成本 P95：{gate['metrics']['daily_cost_p95_cny']:.4f} 元",
        f"- 当日搜索/Token：{day['agent_web_searches']} 次 / 输入 {day['agent_input_tokens']} / 输出 {day['agent_output_tokens']}",
        f"- 平均发现延迟：{day['average_discovery_delay_hours']:.2f} 小时；超过 48 小时：{day['delayed_over_48h']} 条",
        f"- 旧信源异常：{day['old_source_incidents']} 个；Agent 无效证据：{day['invalid_evidence_count']} 条",
        f"- 自动门槛：{'通过，等待人工抽检' if gate['ready_for_manual_approval'] else '未通过'}",
        "",
        "## 人工抽检样本",
        "",
    ]
    for sample in output["manual_samples"]:
        lines.append(f"- `{sample['event_id']}` [{sample['title']}]({sample['canonical_url']}) — {sample['difference']}")
    if not output["manual_samples"]:
        lines.append("- 无高影响分歧事件。")
    lines.extend(
        [
            "",
            "自动门槛通过并完成抽检后，使用：",
            "",
            f"`/agent-review approve {review_id}`",
            "",
            "如有误判，在命令后添加：`reject=事件ID1,事件ID2`。",
        ]
    )
    (out_dir / "daily_review.md").write_text("\n".join(lines), encoding="utf-8")
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description="生成国内行业 Agent 每日事件级复盘和 14 天上线门槛")
    parser.add_argument("--date", default="")
    parser.add_argument("--config", default="./sources.json")
    parser.add_argument("--agent-root", default="./downloads/agent")
    parser.add_argument("--legacy-root", default="./downloads/legacy")
    parser.add_argument("--optimized-root", default="./downloads/optimized")
    parser.add_argument("--state", default="./.state-agent-review/history.json")
    parser.add_argument("--out", default="./artifacts-agent-review")
    parser.add_argument("--lookback-date", default="", help="用该运行日的三链路产物回看 --date 的发布窗口")
    parser.add_argument("--no-model-judge", action="store_true")
    args = parser.parse_args()
    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    config = read_json(Path(args.config).expanduser().resolve())
    model = None
    if not args.no_model_judge:
        try:
            model = build_model_provider(config.get("industry_agent", {}))
        except Exception:
            model = None
    output = run_review(
        date_text,
        config,
        Path(args.agent_root).expanduser().resolve(),
        Path(args.legacy_root).expanduser().resolve(),
        Path(args.optimized_root).expanduser().resolve(),
        Path(args.state).expanduser().resolve(),
        Path(args.out).expanduser().resolve(),
        model,
        lookback_date=args.lookback_date.strip(),
    )
    print(
        f"[agent_review] date={date_text} days={output['history_days']} "
        f"ready={output['gate']['ready_for_manual_approval']} review_id={output['review_id']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
