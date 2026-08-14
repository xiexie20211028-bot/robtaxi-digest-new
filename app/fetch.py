from __future__ import annotations

import argparse
import concurrent.futures
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .common import RawItem, SourceStat, now_beijing, parse_datetime, parse_datetime_with_status, to_dict_list, write_jsonl
from .fetch_discovery import (
    fetch_official_api_source,
    fetch_query_rss_source,
    fetch_search_api_source,
    fetch_search_result_source,
)
from .fetch_rss import fetch_rss_source, summarize_fetch_error
from .fetch_structured import fetch_structured_source
from .report import empty_stage_funnel, load_or_init, mark_stage, normalize_method, patch_report, report_path
from .source_config import PROFILE_NAMES, load_source_config, source_metadata
from .source_health import update_source_health_history
from .social_provider import fetch_social_seed_source



def process_source(source: dict[str, Any], cfg: dict[str, Any], fetch_time: str) -> tuple[list[RawItem], SourceStat]:
    source = dict(source)
    source["_http_cache_dir"] = str(cfg.get("_http_cache_dir", ""))
    source_id = str(source.get("id", "")).strip()
    source_name = str(source.get("name", "")).strip()
    source_type = str(source.get("source_type", "rss")).strip().lower() or "rss"
    region = str(source.get("region", "foreign")).strip().lower()
    company_hint = str(source.get("source_company_id", "")).strip()
    metadata = source_metadata(source)

    rows: list[dict[str, str]] = []
    err = ""
    transport_stats = {"request_count": 1, "request_success_count": 0, "listed_items": 0}
    try:
        if source_type == "rss":
            rows, err = fetch_rss_source(source)
        elif source_type == "search_api":
            rows, err = fetch_search_api_source(source, cfg)
        elif source_type == "query_rss":
            rows, err = fetch_query_rss_source(source, cfg)
        elif source_type == "search_result":
            rows, err = fetch_search_result_source(source, cfg)
        elif source_type == "official_api":
            rows, err = fetch_official_api_source(source)
        elif source_type == "structured_web":
            rows, err, transport_stats = fetch_structured_source(source, include_stats=True)
        elif source_type == "social_provider":
            rows, err = fetch_social_seed_source(source, str(cfg.get("_social_since_utc", "")))
        else:
            err = f"unsupported source_type={source_type}"
    except Exception as exc:
        err = str(exc)

    raw_items = [
        RawItem(
            source_id=source_id,
            source_name=source_name,
            source_type=source_type,
            region=region,
            company_hint=company_hint,
            fetched_at=fetch_time,
            url=row.get("link", ""),
            payload=row,
            source_role=metadata["source_role"],
            evidence_type=metadata["evidence_type"],
            criticality=metadata["criticality"],
            coverage_domains=metadata["coverage_domains"],
            official_accounts=metadata["official_accounts"],
        )
        for row in rows
        if row.get("title") and row.get("link")
    ]

    status = "healthy"
    if err and not raw_items:
        status = "failed"
    elif err and raw_items:
        status = "degraded"
    err_raw = err[:500]
    err_code, err_zh = summarize_fetch_error(err_raw)
    if source_type != "structured_web":
        transport_stats = {
            "request_count": 1,
            "request_success_count": 0 if status == "failed" else 1,
            "listed_items": len(rows),
        }

    published_values = [str(row.get("published", "")).strip() for row in rows if str(row.get("published", "")).strip()]
    parsed_values = [value for value in published_values if parse_datetime_with_status(value)[1] == "ok"]
    body_parsed_items = sum(
        1
        for row in rows
        if str(row.get("content", "")).strip() or str(row.get("summary", "")).strip()
    )
    newest_published = ""
    if parsed_values:
        newest_published = max(parse_datetime(value).isoformat() for value in parsed_values)

    stat = SourceStat(
        source_id=source_id,
        source_name=source_name,
        source_type=source_type,
        status=status,
        fetched_items=len(raw_items),
        error=err_zh if err_zh else err_raw[:120],
        error_reason_code=err_code,
        error_reason_zh=err_zh,
        error_raw=err_raw,
        source_role=metadata["source_role"],
        evidence_type=metadata["evidence_type"],
        criticality=metadata["criticality"],
        coverage_domains=metadata["coverage_domains"],
        health_policy=metadata["health_policy"],
        request_count=int(transport_stats.get("request_count", 1)),
        request_success_count=int(transport_stats.get("request_success_count", 0)),
        listed_items=int(transport_stats.get("listed_items", len(rows))),
        valid_items=len(raw_items),
        date_parsed_items=len(parsed_values),
        body_parsed_items=body_parsed_items,
        date_parse_rate=round(len(parsed_values) / len(raw_items), 4) if raw_items else 1.0,
        body_parse_rate=round(body_parsed_items / len(raw_items), 4) if raw_items else 1.0,
        newest_published_at=newest_published,
        last_success_at=fetch_time if status != "failed" else "",
    )
    return raw_items, stat



def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch raw Robtaxi items from configured sources")
    parser.add_argument("--date", default="", help="Date in YYYY-MM-DD; default uses Beijing date")
    parser.add_argument("--sources", default="./sources.json", help="Path to sources config JSON")
    parser.add_argument("--out", default="./artifacts/raw", help="Output root for raw jsonl")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root directory")
    parser.add_argument("--profile", choices=sorted(PROFILE_NAMES), default="", help="采集 profile；默认读取 active_profile")
    parser.add_argument("--health-history", default="./.state/source_health_history.json", help="信源健康历史文件")
    return parser



def main() -> int:
    args = build_arg_parser().parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    sources_path = Path(args.sources).expanduser().resolve()
    raw_root = Path(args.out).expanduser().resolve()
    report_root = Path(args.report).expanduser().resolve()
    report_file = report_path(report_root, date_text)

    try:
        cfg, active_profile = load_source_config(sources_path, args.profile)
        cfg["_http_cache_dir"] = str(Path(args.health_history).expanduser().resolve().parent / "http_cache")
        cfg["_social_since_utc"] = (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat()
    except Exception as exc:
        mark_stage(report_file, "fetch", "failed")
        patch_report(report_file, source_stats=[], fetch_error=str(exc)[:300])
        raise SystemExit(f"[fetch] invalid config: {exc}")

    sources = cfg.get("sources", []) if isinstance(cfg, dict) else []
    enabled_sources = [s for s in sources if isinstance(s, dict) and bool(s.get("enabled", True))]

    fetch_time = datetime.now(timezone.utc).isoformat()
    all_raw: list[RawItem] = []
    all_stats: list[SourceStat] = []
    results: list[tuple[list[RawItem], SourceStat] | None] = [None] * len(enabled_sources)

    def _fetch_one(idx: int, source: dict[str, Any]) -> tuple[int, list[RawItem], SourceStat]:
        raw_rows, stat = process_source(source, cfg, fetch_time)
        return idx, raw_rows, stat

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(_fetch_one, i, src): i for i, src in enumerate(enabled_sources)}
        for future in concurrent.futures.as_completed(futures):
            try:
                idx, raw_rows, stat = future.result()
                results[idx] = (raw_rows, stat)
            except Exception as exc:
                idx = futures[future]
                src = enabled_sources[idx]
                metadata = source_metadata(src)
                stat = SourceStat(
                    source_id=str(src.get("id", "")),
                    source_name=str(src.get("name", "")),
                    source_type=str(src.get("source_type", "rss")),
                    status="failed",
                    fetched_items=0,
                    error=str(exc)[:120],
                    error_raw=str(exc)[:500],
                    source_role=metadata["source_role"],
                    evidence_type=metadata["evidence_type"],
                    criticality=metadata["criticality"],
                    coverage_domains=metadata["coverage_domains"],
                    health_policy=metadata["health_policy"],
                    request_count=1,
                )
                results[idx] = ([], stat)

    for result in results:
        if result is None:
            continue
        raw_rows, stat = result
        all_raw.extend(raw_rows)
        all_stats.append(stat)

    out_file = raw_root / date_text / "raw_items.jsonl"
    write_jsonl(out_file, to_dict_list(all_raw))

    stats_dicts, rolling_health = update_source_health_history(
        to_dict_list(all_stats),
        Path(args.health_history).expanduser().resolve(),
        date_text,
    )
    fail_count = len([s for s in stats_dicts if str(s.get("status")) != "healthy"])
    blocking_fail_count = len(
        [
            s
            for s in stats_dicts
            if str(s.get("status")) in {"failed", "silent_dead"} and str(s.get("criticality")) == "required"
        ]
    )
    search_api_missing_key_count = len([s for s in all_stats if s.error_reason_code == "search_api_missing_key"])
    non_search_fail_count = len(
        [s for s in stats_dicts if str(s.get("status")) != "healthy" and s.get("error_reason_code") != "search_api_missing_key"]
    )
    discovery_items_raw_count = len([r for r in all_raw if r.source_type in {"query_rss", "search_result"}])
    search_result_raw_count = len([r for r in all_raw if r.source_type == "search_result"])

    method_fetch_totals: dict[str, int] = {method: 0 for method in empty_stage_funnel()}
    for stat in all_stats:
        method = normalize_method(stat.source_type)
        if method:
            method_fetch_totals[method] += int(stat.fetched_items)

    query_rss_resolved_count = 0
    query_rss_resolve_fail_count = 0
    query_rss_resolve_failed_token_decode_count = 0
    query_rss_resolve_failed_html_extract_count = 0
    query_rss_resolve_failed_google_link_left_count = 0
    date_bj = date_text
    discovery_today_raw_count = 0

    for item in all_raw:
        if item.source_type == "search_result":
            raw_display_time = str((item.payload or {}).get("search_display_time", "")).strip()
            if raw_display_time:
                try:
                    dt = parse_datetime(raw_display_time)
                    if dt.astimezone(now_beijing().tzinfo or timezone.utc).date().isoformat() == date_bj:
                        discovery_today_raw_count += 1
                except Exception:
                    pass
            continue

        if item.source_type != "query_rss":
            continue

        resolver_method = str((item.payload or {}).get("resolver_method", "")).strip()
        token_decode_ok = str((item.payload or {}).get("resolver_token_decode_ok", "")).lower() == "true"
        resolved_ok = str((item.payload or {}).get("resolved_ok", "")).lower() == "true"
        if resolved_ok:
            query_rss_resolved_count += 1
        else:
            query_rss_resolve_fail_count += 1
        if not token_decode_ok and resolver_method != "not_google_news":
            query_rss_resolve_failed_token_decode_count += 1
        if resolver_method == "failed_html_extract":
            query_rss_resolve_failed_html_extract_count += 1
        if resolver_method == "failed_google_link_left":
            query_rss_resolve_failed_google_link_left_count += 1

        raw_published = str((item.payload or {}).get("published", "")).strip()
        if not raw_published:
            continue
        try:
            dt = parse_datetime(raw_published)
            if dt.astimezone(now_beijing().tzinfo or timezone.utc).date().isoformat() == date_bj:
                discovery_today_raw_count += 1
        except Exception:
            continue

    stage = "success" if blocking_fail_count == 0 else "partial"
    mark_stage(report_file, "fetch", stage)
    report = load_or_init(report_file)
    stage_funnel = report.get("stage_funnel", {}) if isinstance(report, dict) else {}
    if not isinstance(stage_funnel, dict):
        stage_funnel = empty_stage_funnel()

    for method, counts in empty_stage_funnel().items():
        current = stage_funnel.get(method, {}) if isinstance(stage_funnel.get(method, {}), dict) else {}
        stage_funnel[method] = {
            "fetched": int(method_fetch_totals.get(method, 0)),
            "candidate": int(current.get("candidate", 0)),
            "filtered": int(current.get("filtered", 0)),
            "kept": int(current.get("kept", 0)),
        }

    patch_report(
        report_file,
        source_stats=stats_dicts,
        source_health_rolling=rolling_health,
        active_profile=active_profile,
        stage_funnel=stage_funnel,
        total_items_raw=len(all_raw),
        discovery_items_raw_count=discovery_items_raw_count,
        search_result_raw_count=search_result_raw_count,
        discovery_today_raw_count=discovery_today_raw_count,
        query_rss_resolved_count=query_rss_resolved_count,
        query_rss_resolve_fail_count=query_rss_resolve_fail_count,
        query_rss_resolve_failed_token_decode_count=query_rss_resolve_failed_token_decode_count,
        query_rss_resolve_failed_html_extract_count=query_rss_resolve_failed_html_extract_count,
        query_rss_resolve_failed_google_link_left_count=query_rss_resolve_failed_google_link_left_count,
        non_search_fail_count=non_search_fail_count,
        search_api_missing_key_count=search_api_missing_key_count,
        raw_output=str(out_file),
    )

    print(
        f"[fetch] date={date_text} profile={active_profile} sources={len(enabled_sources)} "
        f"raw_items={len(all_raw)} failures={fail_count} blocking_failures={blocking_fail_count}"
    )
    print(f"[fetch] output={out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
