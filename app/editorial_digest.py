from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from .common import http_post_json, now_beijing, read_json, read_jsonl, write_json
from .report import mark_stage, patch_report, report_path


FORMAT_VERSION = "editorial-digest-v1"
DEFAULT_TOP_N = 3
DEFAULT_SOURCE_TOP_N = 8


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _stat_date(date_text: str, report: dict[str, Any]) -> str:
    window_start = str(report.get("window_start_bj", "")).strip()
    if window_start:
        return window_start.split(" ")[0]
    return date_text


def _settings(cfg: dict[str, Any]) -> dict[str, int | str]:
    defaults = cfg.get("defaults", {}) if isinstance(cfg, dict) else {}
    if not isinstance(defaults, dict):
        defaults = {}
    return {
        "mode": str(defaults.get("notify_digest_mode", "editorial_text")).strip() or "editorial_text",
        "top_n": max(1, _safe_int(defaults.get("notify_digest_top_n", DEFAULT_TOP_N), DEFAULT_TOP_N)),
        "source_top_n": max(1, _safe_int(defaults.get("notify_digest_source_top_n", DEFAULT_SOURCE_TOP_N), DEFAULT_SOURCE_TOP_N)),
    }


def _sort_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda x: (_safe_int(x.get("importance", 3), 3), str(x.get("published_at_utc", ""))),
        reverse=True,
    )


def _clean_sentence(text: Any) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    if value[-1] not in "。！？!?":
        value += "。"
    return value


def _compact_item(item: dict[str, Any]) -> dict[str, Any]:
    title = str(item.get("title_zh", "") or item.get("title", "")).strip()
    so_what = str(item.get("summary_so_what", "")).strip()
    why = str(item.get("summary_why", "")).strip()
    what = str(item.get("summary_what", "")).strip()
    impact_targets = [str(x).strip() for x in item.get("impact_targets", []) if str(x).strip()]
    return {
        "title": title,
        "what": what,
        "why": why,
        "so_what": so_what,
        "impact_targets": impact_targets,
        "link": str(item.get("link", "")).strip(),
        "importance": _safe_int(item.get("importance", 3), 3),
        "tags": [str(x).strip() for x in item.get("tags", []) if str(x).strip()],
    }


def build_fallback_digest(
    date_text: str,
    items: list[dict[str, Any]],
    report: dict[str, Any],
    *,
    top_n: int = DEFAULT_TOP_N,
    source_top_n: int = DEFAULT_SOURCE_TOP_N,
    reason: str = "",
) -> dict[str, Any]:
    stat_date = _stat_date(date_text, report)
    sorted_items = [_compact_item(x) for x in _sort_items(items)[:source_top_n]]
    top_items = sorted_items[:top_n]

    if not sorted_items:
        headline = "今日无符合规则的重点新闻。"
        key_points = ["系统未筛出满足时间窗口和相关性规则的重点新闻。"]
    else:
        first = top_items[0]
        first_reason = first.get("so_what") or first.get("why") or first.get("what") or "该事件值得继续关注。"
        headline = _clean_sentence(f"今日最值得关注的是：{first['title']}")
        key_points = []
        for item in top_items:
            point = item.get("so_what") or item.get("why") or item.get("what")
            if point:
                key_points.append(_clean_sentence(point))
        if not key_points:
            key_points = [_clean_sentence(first_reason)]

    digest_top = []
    for item in top_items:
        why_it_matters = item.get("so_what") or item.get("why") or item.get("what") or "该事件会影响 Robotaxi 行业后续节奏。"
        digest_top.append(
            {
                "title": item["title"],
                "why_it_matters": _clean_sentence(why_it_matters),
                "impact_targets": item["impact_targets"] or ["未标注"],
                "link": item["link"],
            }
        )

    return {
        "format_version": FORMAT_VERSION,
        "date": date_text,
        "stat_date": stat_date,
        "headline": headline,
        "key_points": key_points[:3],
        "top_items": digest_top,
        "other_items": [item["title"] for item in sorted_items[top_n:] if item.get("title")],
        "fallback_used": True,
        "fallback_reason": reason or ("no_items" if not sorted_items else "local_fallback"),
    }


def _parse_json_object(text: str) -> dict[str, Any]:
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        return json.loads(text[start : end + 1])
    raise ValueError("no json object found")


def _normalize_model_digest(raw: dict[str, Any], date_text: str, report: dict[str, Any], source_items: list[dict[str, Any]]) -> dict[str, Any]:
    stat_date = _stat_date(date_text, report)
    link_by_title = {str(x.get("title", "")).strip(): str(x.get("link", "")).strip() for x in source_items}
    digest: dict[str, Any] = {
        "format_version": FORMAT_VERSION,
        "date": date_text,
        "stat_date": stat_date,
        "headline": _clean_sentence(raw.get("headline", "")),
        "key_points": [],
        "top_items": [],
        "other_items": [],
        "fallback_used": False,
        "fallback_reason": "",
    }

    if isinstance(raw.get("key_points"), list):
        digest["key_points"] = [_clean_sentence(x) for x in raw["key_points"] if str(x).strip()][:3]

    if isinstance(raw.get("top_items"), list):
        for item in raw["top_items"]:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title", "")).strip()
            if not title:
                continue
            impacts = [str(x).strip() for x in item.get("impact_targets", []) if str(x).strip()]
            link = str(item.get("link", "")).strip() or link_by_title.get(title, "")
            digest["top_items"].append(
                {
                    "title": title,
                    "why_it_matters": _clean_sentence(item.get("why_it_matters", "")),
                    "impact_targets": impacts or ["未标注"],
                    "link": link,
                }
            )

    if isinstance(raw.get("other_items"), list):
        digest["other_items"] = [str(x).strip() for x in raw["other_items"] if str(x).strip()][:8]

    return digest


def validate_digest(digest: dict[str, Any]) -> tuple[bool, str]:
    if digest.get("format_version") != FORMAT_VERSION:
        return False, "invalid_format_version"
    if not str(digest.get("headline", "")).strip():
        return False, "missing_headline"
    if not isinstance(digest.get("key_points"), list):
        return False, "invalid_key_points"
    if not isinstance(digest.get("top_items"), list):
        return False, "invalid_top_items"
    for item in digest.get("top_items", []):
        if not isinstance(item, dict) or not str(item.get("title", "")).strip():
            return False, "invalid_top_item"
        if not str(item.get("why_it_matters", "")).strip():
            return False, "missing_why_it_matters"
    return True, "ok"


def build_model_digest(
    date_text: str,
    items: list[dict[str, Any]],
    report: dict[str, Any],
    *,
    top_n: int = DEFAULT_TOP_N,
    source_top_n: int = DEFAULT_SOURCE_TOP_N,
) -> dict[str, Any]:
    api_key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY is empty")
    source_items = [_compact_item(x) for x in _sort_items(items)[:source_top_n]]
    if not source_items:
        raise RuntimeError("no items to summarize")

    endpoint = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/") + "/chat/completions"
    model = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")
    stat_date = _stat_date(date_text, report)
    prompt = (
        "请基于以下 Robotaxi 入选新闻，生成一条可以直接推送到聊天工具的每日主编摘要。"
        "必须只返回 JSON 对象，字段严格为："
        '{"headline":"...","key_points":["..."],"top_items":[{"title":"...","why_it_matters":"...","impact_targets":["运营方"],"link":"..."}],"other_items":["..."]}。'
        f"统计日为 {stat_date}；headline 只能 1 句；key_points 为 2-3 条；top_items 最多 {top_n} 条；"
        "why_it_matters 必须说明行业影响，不要写“详见原文”。"
        f"\n\n新闻：{json.dumps(source_items, ensure_ascii=False)}"
    )
    payload = {
        "model": model,
        "temperature": 0.2,
        "max_tokens": 900,
        "messages": [
            {"role": "system", "content": "你是 Robotaxi 行业日报主编。只输出 JSON，不要额外解释。"},
            {"role": "user", "content": prompt},
        ],
    }
    data = http_post_json(endpoint, payload, headers={"Authorization": f"Bearer {api_key}"}, timeout=30, retries=3)
    choices = data.get("choices", [])
    if not choices:
        raise RuntimeError(f"empty DeepSeek response: {data}")
    content = str(choices[0].get("message", {}).get("content", "")).strip()
    return _normalize_model_digest(_parse_json_object(content), date_text, report, source_items)


def render_digest_text(digest: dict[str, Any], html_url: str = "") -> str:
    stat_date = str(digest.get("stat_date", "") or digest.get("date", "")).strip()
    lines = [f"Robtaxi 每日重点｜统计日 {stat_date}", "", "今日判断：", str(digest.get("headline", "")).strip()]

    key_points = [str(x).strip() for x in digest.get("key_points", []) if str(x).strip()]
    if key_points:
        lines.extend(["", "行业变化："])
        lines.extend(f"- {point}" for point in key_points[:3])

    top_items = digest.get("top_items", [])
    lines.extend(["", "重点新闻："])
    if top_items:
        for idx, item in enumerate(top_items, 1):
            title = str(item.get("title", "")).strip()
            why = str(item.get("why_it_matters", "")).strip()
            impacts = [str(x).strip() for x in item.get("impact_targets", []) if str(x).strip()]
            link = str(item.get("link", "")).strip()
            lines.append(f"{idx}. {title}")
            if why:
                lines.append(f"   重要性：{why}")
            lines.append(f"   影响对象：{' / '.join(impacts) if impacts else '未标注'}")
            if link:
                lines.append(f"   原文：{link}")
    else:
        lines.append("无符合规则的重点新闻。")

    other_items = [str(x).strip() for x in digest.get("other_items", []) if str(x).strip()]
    if other_items:
        lines.extend(["", "其他入选："])
        lines.extend(f"- {title}" for title in other_items[:8])

    if html_url.strip():
        lines.extend(["", f"完整网页：{html_url.strip()}"])
    return "\n".join(lines)


def load_digest_text(date_text: str, digest_root: str | Path, html_url: str = "") -> str:
    digest_file = Path(digest_root).expanduser().resolve() / date_text / "daily_digest.json"
    if not digest_file.exists():
        return ""
    try:
        digest = read_json(digest_file)
        ok, _reason = validate_digest(digest)
        if not ok:
            return ""
        return render_digest_text(digest, html_url)
    except Exception:
        return ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Build direct-readable daily Robtaxi digest text")
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD; default Beijing date")
    parser.add_argument("--in", dest="in_root", default="./artifacts/brief", help="Brief input root")
    parser.add_argument("--out", default="./artifacts/digest", help="Digest output root")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root")
    parser.add_argument("--sources", default="./sources.json", help="Path to sources config")
    parser.add_argument("--provider", default="deepseek", help="Digest provider: deepseek or fallback")
    args = parser.parse_args()

    date_text = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    in_file = Path(args.in_root).expanduser().resolve() / date_text / "brief_items.jsonl"
    out_dir = Path(args.out).expanduser().resolve() / date_text
    report_file = report_path(Path(args.report).expanduser().resolve(), date_text)
    cfg = read_json(Path(args.sources).expanduser().resolve())
    report = read_json(report_file) if report_file.exists() else {}
    items = read_jsonl(in_file)
    settings = _settings(cfg)

    fallback_reason = ""
    digest: dict[str, Any] | None = None
    if args.provider == "deepseek" and items:
        try:
            digest = build_model_digest(
                date_text,
                items,
                report,
                top_n=int(settings["top_n"]),
                source_top_n=int(settings["source_top_n"]),
            )
            ok, reason = validate_digest(digest)
            if not ok:
                fallback_reason = reason
                digest = None
        except Exception as exc:
            fallback_reason = str(exc)[:200]

    if digest is None:
        digest = build_fallback_digest(
            date_text,
            items,
            report,
            top_n=int(settings["top_n"]),
            source_top_n=int(settings["source_top_n"]),
            reason=fallback_reason,
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    json_file = out_dir / "daily_digest.json"
    text_file = out_dir / "daily_digest.txt"
    write_json(json_file, digest)
    text_file.write_text(render_digest_text(digest), encoding="utf-8")

    mark_stage(report_file, "editorial_digest", "success")
    patch_report(
        report_file,
        editorial_digest_status="success",
        editorial_digest_output=str(json_file),
        editorial_digest_text_output=str(text_file),
        editorial_digest_fallback_used=bool(digest.get("fallback_used", False)),
        editorial_digest_fallback_reason=str(digest.get("fallback_reason", "")),
    )
    print(
        f"[editorial_digest] date={date_text} items={len(items)} "
        f"fallback={bool(digest.get('fallback_used', False))} output={json_file}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
