"""三路线共用的候选决策记录契约。"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse


DECISION_LOG_SCHEMA_VERSION = "robtaxi-candidate-decision-v1"


def _source_label(source: dict[str, Any], candidate: dict[str, Any]) -> str:
    for key in ("source_name", "publisher", "source_id"):
        value = str(source.get(key, candidate.get(key, ""))).strip()
        if value:
            return value
    url = str(candidate.get("canonical_url", candidate.get("link", ""))).strip()
    return (urlparse(url).netloc or url).removeprefix("www.")


def build_candidate_decision(
    *,
    route: str,
    candidate: dict[str, Any],
    source: dict[str, Any] | None,
    stage: str,
    kept: bool,
    final_reason: str,
    signals: dict[str, Any] | None = None,
    score: int | float = 0,
    threshold: int | float | None = None,
    candidate_id: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """返回可查询、可跨路线比较且不包含页面正文的候选决策记录。"""
    source = source or {}
    candidate_id = candidate_id or str(candidate.get("id", candidate.get("event_key", ""))).strip()
    record: dict[str, Any] = {
        "schema_version": DECISION_LOG_SCHEMA_VERSION,
        "route": str(route),
        "candidate_id": candidate_id,
        "title": str(candidate.get("title", candidate.get("title_zh", "")))[:500],
        "source": _source_label(source, candidate),
        "source_id": str(source.get("source_id", candidate.get("source_id", ""))),
        "canonical_url": str(candidate.get("canonical_url", candidate.get("link", "")))[:2000],
        "stage": str(stage),
        "signals": signals or {},
        "score": round(float(score), 2),
        "threshold": round(float(threshold), 2) if threshold is not None else None,
        "kept": bool(kept),
        "final_reason": str(final_reason),
    }
    if extra:
        record.update(extra)
    return record
