from __future__ import annotations

import argparse
import json
import math
import os
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .common import (
    BriefItem,
    cosine_similarity,
    detect_language,
    http_post_json,
    now_beijing,
    read_json,
    read_jsonl,
    tokenize,
    to_dict_list,
    write_json,
    write_jsonl,
)
from .report import load_or_init, mark_stage, patch_report, report_path


ALLOWED_TAGS = ["监管", "融资", "扩张", "合作", "安全", "产品", "运营"]
SUMMARY_FORMAT_VERSION = "w-w-sw-v1"
DEFAULT_IMPACT_TARGETS = ["运营方", "车企", "供应链", "监管", "资本市场"]
DEFAULT_BAN_PHRASES = ["详见原文", "建议查看原文"]


def build_tfidf_vectors(texts: list[str]) -> list[dict[str, float]]:
    tokenized = [tokenize(t) for t in texts]
    df: Counter[str] = Counter()
    for toks in tokenized:
        for tk in set(toks):
            df[tk] += 1

    n_docs = max(1, len(texts))
    vectors: list[dict[str, float]] = []
    for toks in tokenized:
        tf = Counter(toks)
        total = max(1, sum(tf.values()))
        vec: dict[str, float] = {}
        for tk, cnt in tf.items():
            idf = math.log((1 + n_docs) / (1 + df[tk])) + 1
            vec[tk] = (cnt / total) * idf
        vectors.append(vec)
    return vectors


def dedupe_l3(items: list[dict[str, Any]], threshold: float = 0.75) -> tuple[list[dict[str, Any]], int]:
    if not items:
        return [], 0
    texts = [f"{x.get('title', '')} {x.get('content', '')[:500]}" for x in items]
    vectors = build_tfidf_vectors(texts)

    selected_idx: list[int] = []
    dropped = 0
    for i, _item in enumerate(items):
        is_dup = False
        for j in selected_idx:
            sim = cosine_similarity(vectors[i], vectors[j])
            if sim >= threshold:
                is_dup = True
                break
        if is_dup:
            dropped += 1
            continue
        selected_idx.append(i)

    return [items[i] for i in selected_idx], dropped


def infer_tags(text: str) -> list[str]:
    low = text.lower()
    tags: list[str] = []
    mapping = {
        "监管": ["cpuc", "监管", "工信部", "permit", "regulation", "政策", "nhtsa"],
        "融资": ["融资", "funding", "ipo", "投资", "raise", "估值", "并购", "股价"],
        "扩张": ["扩张", "launch", "new city", "新增", "部署", "扩大", "上线"],
        "合作": ["合作", "partnership", "joint", "alliance", "签约", "联合"],
        "安全": ["事故", "safety", "collision", "召回", "安全", "停运"],
        "产品": ["产品", "发布", "platform", "feature", "版本", "车型", "量产"],
        "运营": ["运营", "ride", "订单", "fleet", "商业化", "车队", "网约车"],
    }
    for tag, pats in mapping.items():
        if any(p in low for p in pats):
            tags.append(tag)
    if not tags:
        tags = ["运营"]
    return tags[:3]


def _summary_defaults(cfg: dict[str, Any]) -> dict[str, Any]:
    defaults = cfg.get("defaults", {}) if isinstance(cfg, dict) else {}
    if not isinstance(defaults, dict):
        defaults = {}

    style = str(defaults.get("summary_style", "what_why_so_what")).strip().lower() or "what_why_so_what"
    sentence_min = int(defaults.get("summary_sentence_min", 2) or 2)
    sentence_max = int(defaults.get("summary_sentence_max", 3) or 3)
    if sentence_min < 1:
        sentence_min = 1
    if sentence_max < sentence_min:
        sentence_max = sentence_min

    taxonomy_raw = defaults.get("impact_target_taxonomy", DEFAULT_IMPACT_TARGETS)
    taxonomy: list[str] = []
    if isinstance(taxonomy_raw, list):
        for x in taxonomy_raw:
            t = str(x).strip()
            if t:
                taxonomy.append(t)
    if not taxonomy:
        taxonomy = list(DEFAULT_IMPACT_TARGETS)

    ban_raw = defaults.get("summary_ban_phrases", DEFAULT_BAN_PHRASES)
    ban_phrases: list[str] = []
    if isinstance(ban_raw, list):
        for x in ban_raw:
            t = str(x).strip()
            if t:
                ban_phrases.append(t)
    if not ban_phrases:
        ban_phrases = list(DEFAULT_BAN_PHRASES)

    return {
        "style": style,
        "sentence_min": sentence_min,
        "sentence_max": sentence_max,
        "impact_target_taxonomy": taxonomy,
        "require_so_what": bool(defaults.get("summary_require_so_what", True)),
        "ban_phrases": ban_phrases,
    }


def _clean_clause(text: str) -> str:
    t = re.sub(r"\s+", " ", str(text or "").strip())
    t = re.sub(r"^(what|why|so\s*what)\s*[:：]\s*", "", t, flags=re.I)
    t = t.strip(" \t\n\r,，;；")
    return t


def _ensure_sentence(text: str) -> str:
    t = _clean_clause(text)
    if not t:
        return ""
    if t[-1] not in "。！？!?":
        t += "。"
    return t


def _split_sentences(text: str) -> list[str]:
    chunks = re.split(r"[。！？!?]+", str(text or ""))
    return [c.strip() for c in chunks if c.strip()]


def _contains_ban_phrase(text: str, ban_phrases: list[str]) -> bool:
    low = str(text or "").lower()
    return any(str(p).strip().lower() in low for p in ban_phrases if str(p).strip())


def _normalize_impact_targets(targets: Any, taxonomy: list[str]) -> list[str]:
    if not isinstance(targets, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    allow = set(taxonomy)
    for item in targets:
        t = str(item).strip()
        if not t or t not in allow or t in seen:
            continue
        out.append(t)
        seen.add(t)
    return out


def infer_impact_targets(text: str, taxonomy: list[str]) -> list[str]:
    low = str(text or "").lower()
    mapping = {
        "运营方": ["运营", "网约车", "fleet", "dispatch", "运力", "ride-hailing", "商业化运营", "车队", "订单"],
        "车企": ["车企", "整车", "主机厂", "automaker", "oem", "车型", "量产", "tesla", "toyota", "byd"],
        "供应链": ["供应链", "芯片", "激光雷达", "传感器", "地图", "算力", "零部件", "tier1", "platform"],
        "监管": ["监管", "许可", "牌照", "政策", "法规", "cpuc", "工信部", "nhtsa", "compliance"],
        "资本市场": ["融资", "ipo", "估值", "并购", "投资", "股价", "earnings", "funding", "财报"],
    }
    out: list[str] = []
    for target in taxonomy:
        pats = mapping.get(target, [])
        if any(p in low for p in pats):
            out.append(target)
    if not out:
        out = [taxonomy[0]]
    return out[:3]


def compose_summary_zh(what: str, why: str, so_what: str) -> str:
    parts: list[str] = []
    if what:
        parts.append(f"What：{what}")
    if why:
        parts.append(f"Why：{why}")
    if so_what:
        parts.append(f"So what：{so_what}")
    return " ".join(parts).strip()


def validate_structured_summary(payload: dict[str, Any], cfg: dict[str, Any]) -> tuple[bool, str]:
    what = _clean_clause(payload.get("what", ""))
    why = _clean_clause(payload.get("why", ""))
    so_what = _clean_clause(payload.get("so_what", ""))

    if not what:
        return False, "missing_what"
    if not why:
        return False, "missing_why"
    if cfg["require_so_what"] and not so_what:
        return False, "missing_so_what"

    combined = " ".join([_ensure_sentence(what), _ensure_sentence(why), _ensure_sentence(so_what)]).strip()
    sentence_count = len(_split_sentences(combined))
    if sentence_count < cfg["sentence_min"] or sentence_count > cfg["sentence_max"]:
        return False, "sentence_count_out_of_range"

    if _contains_ban_phrase(combined, cfg["ban_phrases"]):
        return False, "contains_ban_phrase"

    impacts = _normalize_impact_targets(payload.get("impact_targets", []), cfg["impact_target_taxonomy"])
    if not impacts:
        return False, "invalid_impact_targets"

    return True, "ok"


def parse_json_object(text: str) -> dict[str, Any]:
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        return json.loads(text[start : end + 1])
    raise ValueError("no json object found")


def _normalize_model_output(raw: dict[str, Any], title: str, content: str, cfg: dict[str, Any]) -> dict[str, Any]:
    title_zh = str(raw.get("title_zh", "")).strip() or title
    what = _ensure_sentence(raw.get("what", ""))
    why = _ensure_sentence(raw.get("why", ""))
    so_what = _ensure_sentence(raw.get("so_what", ""))
    impact_targets = _normalize_impact_targets(raw.get("impact_targets", []), cfg["impact_target_taxonomy"])

    tags_raw = raw.get("tags", [])
    tags: list[str] = []
    if isinstance(tags_raw, list):
        for t in tags_raw:
            ts = str(t).strip()
            if ts in ALLOWED_TAGS and ts not in tags:
                tags.append(ts)
    if not tags:
        tags = infer_tags(f"{title} {content} {what} {why} {so_what}")

    try:
        confidence = float(raw.get("confidence", 0.72))
    except Exception:
        confidence = 0.72
    confidence = max(0.0, min(1.0, confidence))

    try:
        importance = int(raw.get("importance", 3))
    except Exception:
        importance = 3
    importance = max(1, min(5, importance))

    return {
        "title_zh": title_zh,
        "what": what,
        "why": why,
        "so_what": so_what,
        "impact_targets": impact_targets,
        "tags": tags[:3],
        "confidence": confidence,
        "importance": importance,
    }


def deepseek_summary_structured(title: str, content: str, cfg: dict[str, Any], temperature: float = 0.2) -> dict[str, Any]:
    api_key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY is empty")

    endpoint = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/") + "/chat/completions"
    model = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")
    impacts = ",".join(cfg["impact_target_taxonomy"])

    prompt = (
        "请将以下Robotaxi新闻输出为结构化中文简报。"
        "必须返回JSON对象，字段严格为"
        '{"title_zh":"...","what":"...","why":"...","so_what":"...","impact_targets":["运营方"],"tags":["运营"],"confidence":0.0,"importance":3}。'
        "要求：what/why/so_what都必须是1句中文；整体2-3句；"
        f"impact_targets必须从[{impacts}]中选择1-3个；"
        "tags仅可从[监管,融资,扩张,合作,安全,产品,运营]中选择1-3个；"
        "importance为1-5整数，5=行业重大事件，4=重要动态，3=一般新闻，2=次要，1=边缘；"
        f"禁止出现短语：{','.join(cfg['ban_phrases'])}。"
        f"\n\n标题: {title}\n内容: {content[:1400]}"
    )

    payload = {
        "model": model,
        "temperature": temperature,
        "max_tokens": 400,
        "messages": [
            {"role": "system", "content": "你是Robotaxi行业分析师。只输出JSON对象，不要额外文本。"},
            {"role": "user", "content": prompt},
        ],
    }

    data = http_post_json(
        endpoint,
        payload,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30,
        retries=3,
    )

    choices = data.get("choices", [])
    if not choices:
        raise RuntimeError(f"empty DeepSeek response: {data}")
    content_text = str(choices[0].get("message", {}).get("content", "")).strip()
    obj = parse_json_object(content_text)
    return _normalize_model_output(obj, title, content, cfg)


def fallback_summary_structured(title: str, content: str, cfg: dict[str, Any]) -> dict[str, Any]:
    body = re.sub(r"\s+", " ", str(content or "")).strip()
    lang = detect_language(f"{title} {body}")
    tags = infer_tags(f"{title} {body}")

    if lang == "en":
        what = _ensure_sentence(f"{title} 相关进展已披露")
        why = _ensure_sentence("背景是企业在商业化落地、合作推进或监管变化中持续调整")
        so_what = _ensure_sentence("这将影响Robotaxi行业的运营效率、竞争格局与监管节奏")
    else:
        first_sentence = _split_sentences(body)
        why_hint = first_sentence[0] if first_sentence else "事件背景与企业阶段性策略调整相关"
        what = _ensure_sentence(f"{title}")
        why = _ensure_sentence(why_hint[:80])
        so_what = _ensure_sentence("这对Robotaxi商业化推进、行业竞争和监管预期具有参考价值")

    impact_targets = infer_impact_targets(f"{title} {body}", cfg["impact_target_taxonomy"])

    return {
        "title_zh": title,
        "what": what,
        "why": why,
        "so_what": so_what,
        "impact_targets": impact_targets,
        "tags": tags,
        "confidence": 0.55,
        "importance": 3,
    }


def load_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return read_json(path)


def prune_cache(cache: dict[str, Any], now_utc: datetime) -> int:
    stale_keys = [k for k, v in cache.items() if isinstance(v, dict) and not cache_valid(v, now_utc)]
    for k in stale_keys:
        del cache[k]
    return len(stale_keys)


def cache_valid(entry: dict[str, Any], now_utc: datetime) -> bool:
    updated = str(entry.get("updated_at", "")).strip()
    if not updated:
        return False
    try:
        dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return False
    return dt >= (now_utc - timedelta(days=7))


def _structured_from_cache(entry: dict[str, Any], title: str, content: str, cfg: dict[str, Any]) -> dict[str, Any]:
    raw = {
        "title_zh": str(entry.get("title_zh", "")).strip() or title,
        "what": str(entry.get("summary_what", "")).strip(),
        "why": str(entry.get("summary_why", "")).strip(),
        "so_what": str(entry.get("summary_so_what", "")).strip(),
        "impact_targets": entry.get("impact_targets", []),
        "tags": entry.get("tags", []),
        "confidence": entry.get("confidence", 0.8),
        "importance": entry.get("importance", 3),
    }

    # 兼容旧缓存：只有 summary_zh 时自动拆分。
    if (not raw["what"] or not raw["why"] or not raw["so_what"]) and str(entry.get("summary_zh", "")).strip():
        legacy = str(entry.get("summary_zh", "")).strip()
        parts = _split_sentences(legacy)
        if parts:
            raw["what"] = raw["what"] or _ensure_sentence(parts[0])
        if len(parts) > 1:
            raw["why"] = raw["why"] or _ensure_sentence(parts[1])
        if len(parts) > 2:
            raw["so_what"] = raw["so_what"] or _ensure_sentence(parts[2])
        if not raw["why"]:
            raw["why"] = _ensure_sentence("背景是企业在商业化推进与监管环境中持续调整")
        if not raw["so_what"]:
            raw["so_what"] = _ensure_sentence("这将影响Robotaxi行业的竞争格局与商业化节奏")

    normalized = _normalize_model_output(raw, title, content, cfg)
    if not normalized["impact_targets"]:
        normalized["impact_targets"] = infer_impact_targets(f"{title} {content}", cfg["impact_target_taxonomy"])
    return normalized


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize filtered items with structured What/Why/So what format")
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD; default Beijing date")
    parser.add_argument("--in", dest="in_root", default="./artifacts/filtered", help="Filtered input root")
    parser.add_argument("--out", default="./artifacts/brief", help="Brief output root")
    parser.add_argument("--provider", default="deepseek", help="Summary provider: deepseek or fallback")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root")
    parser.add_argument("--cache", default="./.state/summary_cache.json", help="Summary cache json")
    parser.add_argument("--sources", default="./sources.json", help="Path to sources.json")
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "filtered_items.jsonl"
    out_file = Path(args.out).expanduser().resolve() / date_text / "brief_items.jsonl"
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)
    cache_path = Path(args.cache).expanduser().resolve()

    cfg = read_json(Path(args.sources).expanduser().resolve())
    summary_cfg = _summary_defaults(cfg)

    filtered_rows = read_jsonl(in_file)
    sorted_rows = sorted(filtered_rows, key=lambda x: str(x.get("published_at_utc", "")), reverse=True)
    dedupe_threshold = float(cfg.get("defaults", {}).get("semantic_dedupe_threshold", 0.75))
    deduped_rows, dropped_l3 = dedupe_l3(sorted_rows, threshold=dedupe_threshold)

    cache = load_cache(cache_path)
    now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)

    brief_items: list[BriefItem] = []
    summarize_fail_count = 0
    structured_count = 0
    structured_valid_count = 0
    structured_invalid_count = 0
    summary_retry_count = 0
    impact_counter: Counter[str] = Counter()

    for row in deduped_rows:
        structured_count += 1
        title = str(row.get("title", ""))
        content = str(row.get("content", ""))
        fingerprint = str(row.get("fingerprint", "")).strip()
        cache_row = cache.get(fingerprint, {}) if isinstance(cache, dict) else {}

        normalized: dict[str, Any] | None = None

        if isinstance(cache_row, dict) and cache_valid(cache_row, now_utc):
            maybe = _structured_from_cache(cache_row, title, content, summary_cfg)
            ok, _reason = validate_structured_summary(maybe, summary_cfg)
            if ok:
                normalized = maybe
                structured_valid_count += 1
            else:
                structured_invalid_count += 1

        if normalized is None:
            used_fallback = False

            if args.provider == "deepseek" and summary_cfg["style"] == "what_why_so_what":
                try:
                    maybe = deepseek_summary_structured(title, content, summary_cfg, temperature=0.2)
                    ok, _reason = validate_structured_summary(maybe, summary_cfg)
                    if not ok:
                        structured_invalid_count += 1
                        summary_retry_count += 1
                        maybe = deepseek_summary_structured(title, content, summary_cfg, temperature=0.0)
                        ok, _reason = validate_structured_summary(maybe, summary_cfg)
                    if ok:
                        normalized = maybe
                        structured_valid_count += 1
                    else:
                        structured_invalid_count += 1
                except Exception as exc:
                    summary_retry_count += 1
                    print(f"[summarize] DeepSeek API error for '{title[:60]}': {exc}")

            if normalized is None:
                used_fallback = True
                normalized = fallback_summary_structured(title, content, summary_cfg)
                ok, _reason = validate_structured_summary(normalized, summary_cfg)
                if not ok:
                    structured_invalid_count += 1
                    # 再兜底一次，确保结构字段完整可展示。
                    normalized = {
                        "title_zh": title,
                        "what": _ensure_sentence(title or "该资讯已发布"),
                        "why": _ensure_sentence("背景是行业商业化与监管环境持续变化"),
                        "so_what": _ensure_sentence("这将影响Robotaxi运营策略、竞争格局与资本预期"),
                        "impact_targets": infer_impact_targets(f"{title} {content}", summary_cfg["impact_target_taxonomy"]),
                        "tags": infer_tags(f"{title} {content}"),
                        "confidence": 0.5,
                        "importance": 2,
                    }
                structured_valid_count += 1

            if used_fallback:
                summarize_fail_count += 1

        title_zh = str(normalized.get("title_zh", "")).strip() or title
        what = _ensure_sentence(normalized.get("what", ""))
        why = _ensure_sentence(normalized.get("why", ""))
        so_what = _ensure_sentence(normalized.get("so_what", ""))
        impact_targets = _normalize_impact_targets(normalized.get("impact_targets", []), summary_cfg["impact_target_taxonomy"])
        if not impact_targets:
            impact_targets = infer_impact_targets(f"{title} {content}", summary_cfg["impact_target_taxonomy"])
        for target in impact_targets:
            impact_counter[target] += 1

        tags = normalized.get("tags", [])
        if not isinstance(tags, list):
            tags = []
        tags = [str(t).strip() for t in tags if str(t).strip() in ALLOWED_TAGS]
        if not tags:
            tags = infer_tags(f"{title} {content} {what} {why} {so_what}")

        try:
            confidence = float(normalized.get("confidence", 0.72))
        except Exception:
            confidence = 0.72
        confidence = max(0.0, min(1.0, confidence))

        try:
            importance = int(normalized.get("importance", 3))
        except Exception:
            importance = 3
        importance = max(1, min(5, importance))

        summary_zh = compose_summary_zh(what, why, so_what)

        brief_items.append(
            BriefItem(
                id=str(row.get("id", "")),
                source_id=str(row.get("source_id", "")),
                source_name=str(row.get("source_name", "")),
                region=str(row.get("region", "foreign")),
                company_id=str(row.get("company_hint", "")) or "other",
                title_zh=title_zh,
                summary_zh=summary_zh,
                summary_what=what,
                summary_why=why,
                summary_so_what=so_what,
                impact_targets=impact_targets,
                summary_format_version=SUMMARY_FORMAT_VERSION,
                link=str(row.get("link", "")),
                published_at_utc=str(row.get("published_at_utc", "")),
                tags=tags,
                confidence=confidence,
                importance=importance,
            )
        )

        if isinstance(cache, dict) and fingerprint:
            cache[fingerprint] = {
                "title_zh": title_zh,
                "summary_zh": summary_zh,
                "summary_what": what,
                "summary_why": why,
                "summary_so_what": so_what,
                "impact_targets": impact_targets,
                "summary_format_version": SUMMARY_FORMAT_VERSION,
                "tags": tags,
                "confidence": confidence,
                "importance": importance,
                "updated_at": now_utc.isoformat(),
            }

    write_jsonl(out_file, to_dict_list(brief_items))
    if isinstance(cache, dict):
        pruned = prune_cache(cache, now_utc)
        if pruned:
            print(f"[summarize] cache pruned {pruned} stale entries")
        write_json(cache_path, cache)

    report = load_or_init(report_file)
    parse_drops = int(report.get("dedupe_drop_count", 0))
    total_drops = parse_drops + dropped_l3

    stage_status = "success" if summarize_fail_count == 0 else "partial"
    mark_stage(report_file, "summarize", stage_status)
    patch_report(
        report_file,
        dedupe_drop_count=total_drops,
        summarize_fail_count=summarize_fail_count,
        summarize_dedupe_l3=dropped_l3,
        total_items_brief=len(brief_items),
        brief_count=len(brief_items),
        brief_output=str(out_file),
        summary_structured_count=structured_count,
        summary_structured_valid_count=structured_valid_count,
        summary_structured_invalid_count=structured_invalid_count,
        summary_retry_count=summary_retry_count,
        impact_target_distribution=dict(impact_counter),
    )

    print(
        f"[summarize] date={date_text} filtered={len(filtered_rows)} brief={len(brief_items)} "
        f"drop_l3={dropped_l3} summarize_fail={summarize_fail_count} "
        f"structured_valid={structured_valid_count}/{structured_count}"
    )
    print(f"[summarize] output={out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
