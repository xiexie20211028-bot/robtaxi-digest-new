from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .common import normalize_url, now_beijing, parse_datetime, read_json, read_jsonl, write_jsonl
from .report import mark_stage, patch_report, report_path


ALLOWED_SOURCE_PROFILES = {"general_media", "industry_media", "newsroom", "regulator", "research"}

SEMANTIC_SIGNAL_TERMS = [
    "robtaxi",
    "robotaxi",
    "driverless taxi",
    "self-driving taxi",
    "autonomous taxi",
    "autonomous vehicle",
    "无人驾驶",
    "自动驾驶",
    "网约车",
    "车队",
    "示范运营",
    "许可",
    "监管",
]


def _normalize_keywords(words: list[Any]) -> list[str]:
    out = []
    for word in words:
        text = str(word).strip().lower()
        if text:
            out.append(text)
    return sorted(set(out))


def _source_profile(source: dict[str, Any]) -> str:
    raw = str(source.get("source_profile", "")).strip().lower()
    if raw in ALLOWED_SOURCE_PROFILES:
        return raw
    category = str(source.get("category", "")).strip().lower()
    if category == "media":
        return "general_media"
    if category in {"newsroom", "regulator", "research"}:
        return category
    return "industry_media"


def _build_company_aliases(cfg: dict[str, Any]) -> list[str]:
    aliases: set[str] = set()
    for company in cfg.get("companies", []):
        if not isinstance(company, dict):
            continue
        name = str(company.get("name", "")).strip().lower()
        if len(name) >= 2:
            aliases.add(name)
        for alias in company.get("aliases", []):
            text = str(alias).strip().lower()
            if len(text) >= 2:
                aliases.add(text)
    return sorted(aliases)


def _defaults(cfg: dict[str, Any]) -> dict[str, Any]:
    defaults = cfg.get("defaults", {})
    if not isinstance(defaults, dict):
        defaults = {}

    mode = str(defaults.get("relevance_mode", "high_precision")).strip().lower()
    if mode not in {"high_precision", "balanced", "high_recall"}:
        mode = "high_precision"

    thresholds = defaults.get("relevance_thresholds", {})
    if not isinstance(thresholds, dict):
        thresholds = {}
    score_defaults = {
        "high_precision": {"general_media": 75, "industry_media": 65, "newsroom": 55, "regulator": 55, "research": 55, "search_api": 65},
        "balanced": {"general_media": 68, "industry_media": 58, "newsroom": 50, "regulator": 50, "research": 50, "search_api": 58},
        "high_recall": {"general_media": 60, "industry_media": 50, "newsroom": 45, "regulator": 45, "research": 45, "search_api": 52},
    }
    base_thresholds = score_defaults[mode]
    final_thresholds: dict[str, int] = {}
    for key, val in base_thresholds.items():
        raw = thresholds.get(key, val)
        try:
            final_thresholds[key] = int(raw)
        except Exception:
            final_thresholds[key] = int(val)

    core_domestic = _normalize_keywords(defaults.get("core_keywords_domestic", defaults.get("domestic_keywords", [])))
    core_foreign = _normalize_keywords(defaults.get("core_keywords_foreign", defaults.get("foreign_keywords", [])))
    exclude_domestic = _normalize_keywords(defaults.get("exclude_keywords_domestic", []))
    exclude_foreign = _normalize_keywords(defaults.get("exclude_keywords_foreign", []))

    return {
        "relevance_mode": mode,
        "window_days": int(defaults.get("window_days", 10)),
        "thresholds": final_thresholds,
        "core_domestic": core_domestic,
        "core_foreign": core_foreign,
        "exclude_domestic": exclude_domestic,
        "exclude_foreign": exclude_foreign,
        "require_company_signal_for_general_media": bool(defaults.get("require_company_signal_for_general_media", True)),
        "max_general_media_items_per_source": int(defaults.get("max_general_media_items_per_source", 2)),
    }


def _is_recent(ts: str, window_days: int) -> bool:
    dt = parse_datetime(ts)
    cutoff = datetime.utcnow().replace(tzinfo=timezone.utc) - __import__("datetime").timedelta(days=window_days)
    return dt >= cutoff


def _keyword_hits(text: str, words: list[str]) -> list[str]:
    hits = []
    for word in words:
        if word and word in text:
            hits.append(word)
    return sorted(set(hits))


def _score_item(
    row: dict[str, Any],
    source: dict[str, Any],
    cfg_defaults: dict[str, Any],
    company_aliases: list[str],
) -> tuple[bool, int, str, dict[str, Any]]:
    title = str(row.get("title", "")).strip()
    content = str(row.get("content", "")).strip()
    source_name = str(row.get("source_name", "")).strip()
    link = str(row.get("link", "")).strip()
    region = str(row.get("region", "foreign")).strip().lower()
    source_type = str(source.get("source_type", "rss")).strip().lower() or "rss"
    profile = _source_profile(source)

    norm_url = normalize_url(link)
    if not norm_url:
        return False, 0, "url_invalid", {"profile": profile}

    path = (urlparse(norm_url).path or "").lower()
    if not path or path == "/":
        return False, 0, "url_homepage", {"profile": profile}

    allow_patterns = [str(x).lower() for x in source.get("url_allow_patterns", []) if str(x).strip()]
    block_patterns = [str(x).lower() for x in source.get("url_block_patterns", []) if str(x).strip()]
    if block_patterns and any(p in path for p in block_patterns):
        return False, 0, "url_blocked_pattern", {"profile": profile}
    if allow_patterns and not any(p in path for p in allow_patterns):
        return False, 0, "url_not_in_allow_patterns", {"profile": profile}

    published = str(row.get("published_at_utc", ""))
    if not _is_recent(published, cfg_defaults["window_days"]):
        return False, 0, "time_window", {"profile": profile}

    text_title = title.lower()
    text_all = f"{title} {content} {source_name}".lower()

    core_words = cfg_defaults["core_domestic"] if region == "domestic" else cfg_defaults["core_foreign"]
    exclude_words = cfg_defaults["exclude_domestic"] if region == "domestic" else cfg_defaults["exclude_foreign"]
    include_words = _normalize_keywords(source.get("include_keywords", []))
    exclude_words = sorted(set(exclude_words + _normalize_keywords(source.get("exclude_keywords", []))))

    core_hits = _keyword_hits(text_all, sorted(set(core_words + include_words)))
    core_title_hits = _keyword_hits(text_title, sorted(set(core_words + include_words)))
    company_hits = _keyword_hits(text_all, company_aliases)
    semantic_hits = _keyword_hits(text_all, SEMANTIC_SIGNAL_TERMS)
    negative_hits = _keyword_hits(text_all, exclude_words)

    score = 0
    if core_hits:
        score += 20 + min(25, len(core_hits) * 8)
    if core_title_hits:
        score += 10 + min(15, len(core_title_hits) * 6)
    if company_hits:
        score += 8 + min(18, len(company_hits) * 5)
    if semantic_hits:
        score += min(12, len(semantic_hits) * 4)

    profile_boost = {
        "general_media": 0,
        "industry_media": 6,
        "newsroom": 10,
        "regulator": 10,
        "research": 8,
    }.get(profile, 0)
    score += profile_boost
    if source_type == "search_api":
        score += 4

    if negative_hits:
        score -= min(36, len(negative_hits) * 12)

    score = max(0, min(100, score))

    if profile == "general_media" and cfg_defaults["require_company_signal_for_general_media"]:
        if not core_hits and not company_hits:
            return False, score, "general_no_core_or_company", {
                "profile": profile,
                "core_hits": core_hits,
                "company_hits": company_hits,
                "semantic_hits": semantic_hits,
                "negative_hits": negative_hits,
            }

    threshold_key = "search_api" if source_type == "search_api" else profile
    threshold = cfg_defaults["thresholds"].get(threshold_key, 65)
    if score < threshold:
        return False, score, "score_below_threshold", {
            "profile": profile,
            "threshold": threshold,
            "core_hits": core_hits,
            "company_hits": company_hits,
            "semantic_hits": semantic_hits,
            "negative_hits": negative_hits,
        }

    return True, score, "kept", {
        "profile": profile,
        "threshold": threshold,
        "core_hits": core_hits,
        "company_hits": company_hits,
        "semantic_hits": semantic_hits,
        "negative_hits": negative_hits,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Filter canonical items for Robtaxi relevance")
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD; default Beijing date")
    parser.add_argument("--in", dest="in_root", default="./artifacts/canonical", help="Canonical input root")
    parser.add_argument("--out", default="./artifacts/filtered", help="Filtered output root")
    parser.add_argument("--sources", default="./sources.json", help="Path to sources config")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root")
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "canonical_items.jsonl"
    out_root = Path(args.out).expanduser().resolve() / date_text
    keep_file = out_root / "filtered_items.jsonl"
    drop_file = out_root / "dropped_items.jsonl"
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)

    cfg = read_json(Path(args.sources).expanduser().resolve())
    source_map = {}
    for src in cfg.get("sources", []):
        if isinstance(src, dict):
            source_map[str(src.get("id", "")).strip()] = src

    settings = _defaults(cfg)
    company_aliases = _build_company_aliases(cfg)
    rows = read_jsonl(in_file)
    rows = sorted(rows, key=lambda x: str(x.get("published_at_utc", "")), reverse=True)

    kept: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    drop_reasons: Counter[str] = Counter()
    kept_by_source: defaultdict[str, int] = defaultdict(int)
    general_media_kept: defaultdict[str, int] = defaultdict(int)

    for row in rows:
        sid = str(row.get("source_id", "")).strip()
        source = source_map.get(sid, {"source_type": "rss", "category": "media"})

        is_keep, score, reason, detail = _score_item(row, source, settings, company_aliases)
        profile = str(detail.get("profile", "general_media"))

        if is_keep and profile == "general_media":
            cap = settings["max_general_media_items_per_source"]
            if general_media_kept[sid] >= cap:
                is_keep = False
                reason = "general_source_cap"
            else:
                general_media_kept[sid] += 1

        target = dict(row)
        target["relevance_score"] = score
        target["relevance_profile"] = profile
        target["relevance_reason"] = reason
        target["relevance_detail"] = detail

        if is_keep:
            kept.append(target)
            kept_by_source[sid] += 1
        else:
            dropped.append(target)
            drop_reasons[reason] += 1

    write_jsonl(keep_file, kept)
    write_jsonl(drop_file, dropped)

    total_in = len(rows)
    total_kept = len(kept)
    total_dropped = len(dropped)
    pass_rate = round((total_kept / total_in) * 100.0, 2) if total_in else 0.0
    stage_status = "success" if total_kept > 0 else "partial"

    mark_stage(report_file, "filter", stage_status)
    patch_report(
        report_file,
        relevance_total_in=total_in,
        relevance_kept=total_kept,
        relevance_dropped=total_dropped,
        relevance_drop_by_reason=dict(drop_reasons),
        relevance_kept_by_source=dict(kept_by_source),
        relevance_precision_mode=settings["relevance_mode"],
        relevance_pass_rate=pass_rate,
        filtered_output=str(keep_file),
        dropped_output=str(drop_file),
    )

    print(
        f"[filter] date={date_text} in={total_in} kept={total_kept} dropped={total_dropped} "
        f"pass_rate={pass_rate}% mode={settings['relevance_mode']}"
    )
    print(f"[filter] output={keep_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
