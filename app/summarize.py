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


def dedupe_l3(items: list[dict[str, Any]], threshold: float = 0.85) -> tuple[list[dict[str, Any]], int]:
    if not items:
        return [], 0
    texts = [f"{x.get('title', '')} {x.get('content', '')[:500]}" for x in items]
    vectors = build_tfidf_vectors(texts)

    selected_idx: list[int] = []
    dropped = 0
    for i, item in enumerate(items):
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


def _clamp_summary(summary: str) -> str:
    s = re.sub(r"\s+", " ", (summary or "").strip())
    if len(s) > 120:
        s = s[:120].rstrip("，。,. ") + "。"
    if len(s) < 40:
        if not s:
            s = "该条资讯与Robotaxi业务推进相关，建议查看原文链接获取完整信息。"
        while len(s) < 40:
            s += "详见原文。"
    return s


def infer_tags(text: str) -> list[str]:
    low = text.lower()
    tags: list[str] = []
    mapping = {
        "监管": ["cpuc", "监管", "工信部", "permit", "regulation", "政策"],
        "融资": ["融资", "funding", "ipo", "投资", "raise"],
        "扩张": ["扩张", "launch", "new city", "新增", "部署", "扩大"],
        "合作": ["合作", "partnership", "joint", "alliance", "签约"],
        "安全": ["事故", "safety", "collision", "召回", "安全"],
        "产品": ["产品", "发布", "platform", "feature", "版本"],
        "运营": ["运营", "ride", "订单", "fleet", "商业化"],
    }
    for tag, pats in mapping.items():
        if any(p in low for p in pats):
            tags.append(tag)
    if not tags:
        tags = ["运营"]
    return tags[:3]


def fallback_summary(title: str, content: str) -> tuple[str, str, list[str], float]:
    title_zh = title
    body = re.sub(r"\s+", " ", (content or "").strip())
    if detect_language(f"{title} {body}") == "en":
        summary = f"该资讯聚焦于{title}，涉及Robotaxi业务进展、运营策略或监管动向，建议结合原文判断其行业影响。"
    else:
        snippet = body[:110].rstrip("，。,. ")
        summary = f"{snippet}。" if snippet else f"该资讯聚焦于{title}，包含Robotaxi行业的重要进展。"
    summary = _clamp_summary(summary)
    tags = infer_tags(f"{title} {content}")
    return title_zh, summary, tags, 0.55


def parse_json_object(text: str) -> dict[str, Any]:
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        return json.loads(text[start : end + 1])
    raise ValueError("no json object found")


def deepseek_summary(title: str, content: str) -> tuple[str, str, list[str], float]:
    api_key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY is empty")

    endpoint = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/") + "/chat/completions"
    model = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")

    prompt = (
        "请将以下Robotaxi新闻整理成中文简报。"
        "严格返回JSON对象，格式为"
        '{"title_zh":"...","summary_zh":"...","tags":["监管"],"confidence":0.0}。'
        "summary_zh限制为2句、40到120字，tags仅可从[监管,融资,扩张,合作,安全,产品,运营]中选择1-3个。"
        f"\n\n标题: {title}\n内容: {content[:1200]}"
    )

    payload = {
        "model": model,
        "temperature": 0.2,
        "max_tokens": 260,
        "messages": [
            {"role": "system", "content": "你是Robotaxi行业简报助手。输出必须是JSON。"},
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

    title_zh = str(obj.get("title_zh", "")).strip() or title
    summary_zh = _clamp_summary(str(obj.get("summary_zh", "")).strip())

    tags_raw = obj.get("tags", [])
    tags: list[str] = []
    if isinstance(tags_raw, list):
        for t in tags_raw:
            ts = str(t).strip()
            if ts in ALLOWED_TAGS:
                tags.append(ts)
    if not tags:
        tags = infer_tags(f"{title} {content}")

    try:
        confidence = float(obj.get("confidence", 0.72))
    except Exception:
        confidence = 0.72
    confidence = max(0.0, min(1.0, confidence))
    return title_zh, summary_zh, tags[:3], confidence


def load_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return read_json(path)


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize canonical items with DeepSeek + L3 semantic dedupe")
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD; default Beijing date")
    parser.add_argument("--in", dest="in_root", default="./artifacts/filtered", help="Filtered input root")
    parser.add_argument("--out", default="./artifacts/brief", help="Brief output root")
    parser.add_argument("--provider", default="deepseek", help="Summary provider: deepseek or fallback")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root")
    parser.add_argument("--cache", default="./.state/summary_cache.json", help="Summary cache json")
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "filtered_items.jsonl"
    out_file = Path(args.out).expanduser().resolve() / date_text / "brief_items.jsonl"
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)
    cache_path = Path(args.cache).expanduser().resolve()

    filtered_rows = read_jsonl(in_file)
    sorted_rows = sorted(filtered_rows, key=lambda x: str(x.get("published_at_utc", "")), reverse=True)

    deduped_rows, dropped_l3 = dedupe_l3(sorted_rows, threshold=0.85)

    cache = load_cache(cache_path)
    now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)

    brief_items: list[BriefItem] = []
    summarize_fail_count = 0

    for row in deduped_rows:
        fingerprint = str(row.get("fingerprint", "")).strip()
        cache_row = cache.get(fingerprint, {}) if isinstance(cache, dict) else {}

        if isinstance(cache_row, dict) and cache_valid(cache_row, now_utc):
            title_zh = str(cache_row.get("title_zh", "")).strip() or str(row.get("title", ""))
            summary_zh = _clamp_summary(str(cache_row.get("summary_zh", "")))
            tags = [str(t) for t in cache_row.get("tags", []) if str(t) in ALLOWED_TAGS]
            if not tags:
                tags = infer_tags(f"{row.get('title', '')} {row.get('content', '')}")
            confidence = float(cache_row.get("confidence", 0.8))
        else:
            try:
                if args.provider == "deepseek":
                    title_zh, summary_zh, tags, confidence = deepseek_summary(
                        str(row.get("title", "")), str(row.get("content", ""))
                    )
                else:
                    raise RuntimeError("provider fallback")
            except Exception:
                summarize_fail_count += 1
                title_zh, summary_zh, tags, confidence = fallback_summary(
                    str(row.get("title", "")), str(row.get("content", ""))
                )

            if isinstance(cache, dict) and fingerprint:
                cache[fingerprint] = {
                    "title_zh": title_zh,
                    "summary_zh": summary_zh,
                    "tags": tags,
                    "confidence": confidence,
                    "updated_at": now_utc.isoformat(),
                }

        brief_items.append(
            BriefItem(
                id=str(row.get("id", "")),
                source_id=str(row.get("source_id", "")),
                source_name=str(row.get("source_name", "")),
                region=str(row.get("region", "foreign")),
                company_id=str(row.get("company_hint", "")) or "other",
                title_zh=title_zh,
                summary_zh=summary_zh,
                link=str(row.get("link", "")),
                published_at_utc=str(row.get("published_at_utc", "")),
                tags=tags,
                confidence=confidence,
            )
        )

    write_jsonl(out_file, to_dict_list(brief_items))
    if isinstance(cache, dict):
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
    )

    print(
        f"[summarize] date={date_text} filtered={len(filtered_rows)} brief={len(brief_items)} "
        f"drop_l3={dropped_l3} summarize_fail={summarize_fail_count}"
    )
    print(f"[summarize] output={out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
