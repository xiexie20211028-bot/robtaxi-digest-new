from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .common import now_beijing, read_json, read_jsonl, write_jsonl
from .filter_rules import (
    _build_company_aliases,
    _check_hard_constraints,
    _defaults,
    _resolve_prev_natural_day_window,
    _resolve_timezone,
    reason_zh,
)
from .filter_scoring import _collect_signals, _is_fast_pass, _score_stage2
from .report import empty_method_breakdown, empty_stage_funnel, mark_stage, normalize_method, patch_report, report_path
from .source_config import load_source_config, source_metadata
from .source_health import update_source_health_history
from .taxonomy import classify_industry_item, validate_social_candidate



def main() -> int:
    parser = argparse.ArgumentParser(description="Filter canonical items for Robtaxi relevance")
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD; default Beijing date")
    parser.add_argument("--in", dest="in_root", default="./artifacts/canonical", help="Canonical input root")
    parser.add_argument("--out", default="./artifacts/filtered", help="Filtered output root")
    parser.add_argument("--sources", default="./sources.json", help="Path to sources config")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root")
    parser.add_argument("--profile", choices=("legacy", "optimized"), default="", help="筛选 profile；默认读取 active_profile")
    parser.add_argument("--health-history", default="./.state/source_health_history.json", help="信源健康历史文件")
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "canonical_items.jsonl"
    out_root = Path(args.out).expanduser().resolve() / date_text
    keep_file = out_root / "filtered_items.jsonl"
    drop_file = out_root / "dropped_items.jsonl"
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)

    cfg, active_profile = load_source_config(Path(args.sources).expanduser().resolve(), args.profile)
    source_map = {}
    for src in cfg.get("sources", []):
        if isinstance(src, dict):
            source_map[str(src.get("id", "")).strip()] = src

    settings = _defaults(cfg)
    window_start_utc, window_end_utc = _resolve_prev_natural_day_window(date_text, settings["window_timezone"])
    window_start_bj = window_start_utc.astimezone(_resolve_timezone(settings["window_timezone"]))
    window_end_bj = window_end_utc.astimezone(_resolve_timezone(settings["window_timezone"]))
    company_aliases = _build_company_aliases(cfg)
    rows = read_jsonl(in_file)
    rows = sorted(rows, key=lambda item: str(item.get("published_at_utc", "")), reverse=True)

    kept: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    drop_reasons: Counter[str] = Counter()
    kept_by_source: defaultdict[str, int] = defaultdict(int)
    candidate_by_source: defaultdict[str, int] = defaultdict(int)
    date_parsed_by_source: defaultdict[str, int] = defaultdict(int)
    whitelist_rejected_by_source: defaultdict[str, int] = defaultdict(int)
    general_media_kept: defaultdict[str, int] = defaultdict(int)
    candidate_method_totals: dict[str, int] = {method: 0 for method in empty_stage_funnel()}
    filtered_method_totals: dict[str, int] = {method: 0 for method in empty_stage_funnel()}
    kept_method_totals: dict[str, int] = {method: 0 for method in empty_stage_funnel()}
    candidate_filter_breakdown = empty_method_breakdown()

    fast_pass_kept_count = 0
    fast_pass_drop_count = 0
    stage2_scored_count = 0
    stage2_kept_count = 0
    strategic_shift_bonus_hit_count = 0
    safety_milestone_bonus_hit_count = 0
    late_arrival_kept_count = 0
    scope_drop_count = 0

    for row in rows:
        sid = str(row.get("source_id", "")).strip()
        candidate_by_source[sid] += 1
        if str(row.get("published_parse_status", "")).strip().lower() == "ok":
            date_parsed_by_source[sid] += 1
        source = dict(source_map.get(sid, {"source_type": "rss", "category": "media"}))
        source.update(source_metadata(source))
        method = normalize_method(str(source.get("source_type", "")))
        if method:
            candidate_method_totals[method] += 1

        hard_ok, hard_reason, hard_detail = _check_hard_constraints(
            row,
            source,
            settings,
            window_start_utc,
            window_end_utc,
        )
        profile = str(hard_detail.get("profile", "general_media"))

        signals = {
            "core_hits": [],
            "core_title_hits": [],
            "context_hits": [],
            "brand_hits": [],
            "company_hits": [],
            "semantic_hits": [],
            "negative_hits": [],
            "context_terms_hit": [],
            "level_hits": [],
            "truck_hits": [],
            "fast_pass_title_hits": [],
            "candidate_signals": [],
        }

        scope_detail: dict[str, Any] = {
            "in_scope": True,
            "scope_reason": "legacy_profile",
            "coverage_domains": row.get("coverage_domains", []),
            "automation_level": row.get("automation_level", "unknown"),
            "event_type": row.get("event_type", "other"),
            "deployment_stage": row.get("deployment_stage", "unknown"),
        }
        if hard_ok:
            classified_scope = classify_industry_item(row, source)
            if classified_scope["in_scope"]:
                scope_detail = classified_scope
            elif settings["scope_mode"] == "passenger_l3_l4":
                hard_ok = False
                hard_reason = str(classified_scope["scope_reason"])
                scope_detail = classified_scope
                scope_drop_count += 1
            else:
                scope_detail["coverage_domains"] = ["robotaxi"]

        if hard_ok:
            social_ok, social_reason = validate_social_candidate(row, source)
            if not social_ok:
                hard_ok = False
                hard_reason = social_reason

        if hard_ok:
            signals = _collect_signals(row, source, settings, company_aliases)

        is_keep = False
        score = 0
        reason = hard_reason
        stage = "hard_drop"
        detail: dict[str, Any] = dict(hard_detail)

        if hard_ok:
            stage = "stage2"

            if settings["fast_pass_enabled"] and signals["fast_pass_title_hits"]:
                if _is_fast_pass(row, signals, settings):
                    is_keep = True
                    score = 100
                    reason = "fast_pass"
                    stage = "fast_pass"
                    fast_pass_kept_count += 1
                else:
                    fast_pass_drop_count += 1

            if not is_keep:
                if not signals["candidate_signals"]:
                    reason = "candidate_gate_miss"
                else:
                    stage2_scored_count += 1
                    is_keep, score, reason, detail = _score_stage2(row, source, settings, signals)
                    stage = "stage2"
                    if detail.get("score_breakdown", {}).get("strategic_shift_bonus", 0) > 0:
                        strategic_shift_bonus_hit_count += 1
                    if detail.get("score_breakdown", {}).get("safety_milestone_bonus", 0) > 0:
                        safety_milestone_bonus_hit_count += 1
                    if is_keep:
                        stage2_kept_count += 1

            if is_keep and settings["enable_general_media_source_cap"] and profile == "general_media":
                cap = settings["max_general_media_items_per_source"]
                if general_media_kept[sid] >= cap:
                    is_keep = False
                    reason = "general_source_cap"
                else:
                    general_media_kept[sid] += 1

            if is_keep and bool(hard_detail.get("late_arrival", False)):
                evidence_allowed = str(source.get("source_role", "")) in settings["late_arrival_allowed_roles"]
                if score < settings["late_arrival_min_score"] or not evidence_allowed:
                    is_keep = False
                    reason = "late_arrival_low_quality"
                elif late_arrival_kept_count >= settings["late_arrival_max_items"]:
                    is_keep = False
                    reason = "late_arrival_cap"
                else:
                    late_arrival_kept_count += 1

        target = dict(row)
        target["relevance_stage"] = stage
        target["relevance_score"] = score
        target["relevance_profile"] = profile
        target["relevance_reason"] = reason
        target["relevance_reason_zh"] = reason_zh(reason)
        target["matched_core_keywords"] = signals["core_hits"]
        target["matched_context_keywords"] = signals["context_hits"]
        target["matched_brand_keywords"] = signals["brand_hits"]
        target["matched_company_aliases"] = signals["company_hits"]
        target["matched_fast_pass_title_keywords"] = signals["fast_pass_title_hits"]
        target["relevance_score_breakdown"] = detail.get("score_breakdown", {})
        target["strategic_shift_bonus"] = detail.get("score_breakdown", {}).get("strategic_shift_bonus", 0)
        target["safety_milestone_bonus"] = detail.get("score_breakdown", {}).get("safety_milestone_bonus", 0)
        target["drop_reason"] = reason if not is_keep else ""
        target["drop_reason_zh"] = reason_zh(reason) if not is_keep else ""
        target["relevance_detail"] = detail
        target["coverage_domains"] = list(scope_detail.get("coverage_domains", []))
        target["automation_level"] = str(scope_detail.get("automation_level", "unknown"))
        target["event_type"] = str(scope_detail.get("event_type", "other"))
        target["deployment_stage"] = str(scope_detail.get("deployment_stage", "unknown"))
        target["source_role"] = str(source.get("source_role", "secondary"))
        target["evidence_type"] = str(source.get("evidence_type", "general_media"))
        target["late_arrival"] = bool(hard_detail.get("late_arrival", False)) and is_keep

        if is_keep:
            kept.append(target)
            kept_by_source[sid] += 1
            if method:
                kept_method_totals[method] += 1
        else:
            dropped.append(target)
            drop_reasons[reason] += 1
            if reason in {"url_not_in_allow_patterns", "url_blocked_pattern", "url_external_domain_not_allowed"}:
                whitelist_rejected_by_source[sid] += 1
            if method:
                filtered_method_totals[method] += 1
                label = reason_zh(reason)
                candidate_filter_breakdown[method][label] = int(candidate_filter_breakdown[method].get(label, 0)) + 1

    write_jsonl(keep_file, kept)
    write_jsonl(drop_file, dropped)

    total_in = len(rows)
    total_kept = len(kept)
    total_dropped = len(dropped)
    pass_rate = round((total_kept / total_in) * 100.0, 2) if total_in else 0.0
    stage_status = "success_empty" if total_kept == 0 else "success"

    drop_reasons_zh: dict[str, int] = {}
    for reason_code, count in drop_reasons.items():
        label = reason_zh(reason_code)
        drop_reasons_zh[label] = drop_reasons_zh.get(label, 0) + count

    report_existing = read_json(report_file) if report_file.exists() else {}
    source_stats = report_existing.get("source_stats", []) if isinstance(report_existing, dict) else []
    if isinstance(source_stats, list):
        for stat in source_stats:
            if not isinstance(stat, dict):
                continue
            sid = str(stat.get("source_id", ""))
            candidates = int(candidate_by_source.get(sid, 0))
            parsed = int(date_parsed_by_source.get(sid, 0))
            whitelist_rejected = int(whitelist_rejected_by_source.get(sid, 0))
            stat["valid_items"] = candidates
            stat["date_parsed_items"] = parsed
            stat["fresh_items"] = int(kept_by_source.get(sid, 0))
            stat["whitelist_rejected_items"] = whitelist_rejected
            stat["date_parse_rate"] = round(parsed / candidates, 4) if candidates else float(stat.get("date_parse_rate", 1.0))
            stat["whitelist_reject_rate"] = round(whitelist_rejected / candidates, 4) if candidates else 0.0
            policy = stat.get("health_policy", {}) if isinstance(stat.get("health_policy", {}), dict) else {}
            date_floor = float(policy.get("date_parse_rate_min", 0.90))
            whitelist_ceiling = float(policy.get("whitelist_reject_rate_max", 0.50))
            if str(stat.get("status", "")) == "healthy" and candidates:
                if stat["date_parse_rate"] < date_floor or stat["whitelist_reject_rate"] > whitelist_ceiling:
                    stat["status"] = "degraded"
                    if stat["date_parse_rate"] < date_floor:
                        stat["error_reason_code"] = "low_date_parse_rate"
                        stat["error_reason_zh"] = "日期解析率低于健康阈值"
                    else:
                        stat["error_reason_code"] = "high_whitelist_reject_rate"
                        stat["error_reason_zh"] = "白名单拒绝率高于健康阈值"
        source_stats, rolling_health = update_source_health_history(
            source_stats,
            Path(args.health_history).expanduser().resolve(),
            date_text,
        )
    else:
        rolling_health = {}
    existing_funnel = report_existing.get("stage_funnel", {}) if isinstance(report_existing, dict) else {}
    stage_funnel = empty_stage_funnel()
    for method in stage_funnel:
        current = existing_funnel.get(method, {}) if isinstance(existing_funnel, dict) and isinstance(existing_funnel.get(method, {}), dict) else {}
        stage_funnel[method] = {
            "fetched": int(current.get("fetched", 0)),
            "candidate": int(candidate_method_totals.get(method, 0)),
            "filtered": int(filtered_method_totals.get(method, 0)),
            "kept": int(kept_method_totals.get(method, 0)),
        }

    mark_stage(report_file, "filter", stage_status)
    patch_report(
        report_file,
        stage_funnel=stage_funnel,
        source_stats=source_stats,
        source_health_rolling=rolling_health,
        candidate_filter_breakdown=candidate_filter_breakdown,
        window_mode=settings["window_mode"],
        window_start_bj=window_start_bj.strftime("%Y-%m-%d %H:%M:%S"),
        window_end_bj=window_end_bj.strftime("%Y-%m-%d %H:%M:%S"),
        relevance_total_in=total_in,
        relevance_kept=total_kept,
        today_kept_count=total_kept,
        relevance_dropped=total_dropped,
        relevance_drop_by_reason=dict(drop_reasons),
        relevance_drop_by_reason_zh=drop_reasons_zh,
        published_missing_drop_count=int(drop_reasons.get("published_missing_or_unparseable", 0)),
        query_rss_unresolved_drop_count=int(drop_reasons.get("query_rss_unresolved_url", 0)),
        query_rss_unverified_drop_count=int(drop_reasons.get("query_rss_unverified_published", 0)),
        search_result_unverified_drop_count=int(drop_reasons.get("search_result_unverified_published", 0)),
        published_unparseable_count=int(drop_reasons.get("published_missing_or_unparseable", 0)),
        not_today_drop_count=int(drop_reasons.get("outside_window", 0)),
        source_max_age_drop_count=0,
        candidate_gate_drop_count=int(drop_reasons.get("candidate_gate_miss", 0)),
        fast_pass_kept_count=fast_pass_kept_count,
        fast_pass_drop_count=fast_pass_drop_count,
        stage2_scored_count=stage2_scored_count,
        stage2_kept_count=stage2_kept_count,
        strategic_shift_bonus_hit_count=strategic_shift_bonus_hit_count,
        safety_milestone_bonus_hit_count=safety_milestone_bonus_hit_count,
        relevance_kept_by_source=dict(kept_by_source),
        relevance_precision_mode=settings["relevance_mode"],
        active_profile=active_profile,
        scope_mode=settings["scope_mode"],
        scope_drop_count=scope_drop_count,
        late_arrival_kept_count=late_arrival_kept_count,
        relevance_pass_rate=pass_rate,
        filtered_output=str(keep_file),
        dropped_output=str(drop_file),
    )

    print(
        f"[filter] date={date_text} in={total_in} kept={total_kept} dropped={total_dropped} "
        f"pass_rate={pass_rate}% mode={settings['relevance_mode']} window={settings['window_mode']} "
        f"fast_pass_kept={fast_pass_kept_count}"
    )
    print(f"[filter] output={keep_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
