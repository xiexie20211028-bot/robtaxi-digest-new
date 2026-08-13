from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from app.common import normalize_url, parse_datetime, sha1_text
from app.taxonomy import classify_industry_item

from .contracts import AgentEvent, Evidence


PRIMARY_EVIDENCE = {"regulator", "dataset", "filing", "company_newsroom"}
ALLOWED_DOMAINS = {"robotaxi", "passenger_l3", "passenger_l4", "core_supply_chain", "regulation_safety"}


def build_domain_registry(config: dict[str, Any]) -> dict[str, dict[str, str]]:
    registry: dict[str, dict[str, str]] = {}
    for source in config.get("sources", []):
        if not isinstance(source, dict):
            continue
        domains: set[str] = set()
        accounts = source.get("official_accounts", {}) if isinstance(source.get("official_accounts", {}), dict) else {}
        domains.update(str(value).strip().lower() for value in accounts.get("domains", []) if str(value).strip())
        for key in ("entry_urls", "rss_urls"):
            for value in source.get(key, []):
                host = (urlparse(str(value)).netloc or "").lower().removeprefix("www.")
                if host:
                    domains.add(host)
        for domain in domains:
            registry[domain] = {
                "publisher": str(source.get("name", domain)),
                "evidence_type": str(source.get("evidence_type", "general_media")),
            }
    return registry


class DefaultEvidenceVerifier:
    def __init__(self, page_reader: Any, config: dict[str, Any]) -> None:
        self.page_reader = page_reader
        self.registry = build_domain_registry(config)
        self.allowed_automation = {"L3", "L4", "unknown"}

    def _domain_meta(self, url: str, hinted_type: str) -> tuple[str, str]:
        host = (urlparse(url).netloc or "").lower().removeprefix("www.")
        matched = next((meta for domain, meta in self.registry.items() if host == domain or host.endswith(f".{domain}")), None)
        if matched:
            return str(matched.get("publisher", host)), str(matched.get("evidence_type", "general_media"))
        safe_hint = hinted_type if hinted_type in {"industry_media", "general_media", "social_post"} else "general_media"
        return host, safe_hint

    @staticmethod
    def _window(run_date: str) -> tuple[datetime, datetime, datetime]:
        tz = ZoneInfo("Asia/Shanghai")
        end_bj = datetime.fromisoformat(run_date).replace(tzinfo=tz)
        start_bj = end_bj - timedelta(days=1)
        late_start_bj = end_bj - timedelta(hours=72)
        return start_bj.astimezone(timezone.utc), end_bj.astimezone(timezone.utc), late_start_bj.astimezone(timezone.utc)

    @staticmethod
    def _normalize_score(candidate: dict[str, Any]) -> tuple[int, dict[str, int]]:
        raw = candidate.get("score_breakdown", {}) if isinstance(candidate.get("score_breakdown", {}), dict) else {}
        limits = {
            "industry_impact": 30,
            "deployment_or_regulation": 25,
            "scope_relevance": 25,
            "evidence_quality": 20,
        }
        normalized: dict[str, int] = {}
        for key, limit in limits.items():
            try:
                value = int(raw.get(key, 0))
            except Exception:
                value = 0
            normalized[key] = max(0, min(limit, value))
        total = sum(normalized.values())
        return total, normalized

    def verify(self, candidate: dict[str, Any], run_date: str, agent_run_id: str) -> tuple[AgentEvent | None, str]:
        title = str(candidate.get("title", "")).strip()
        summary = str(candidate.get("factual_summary", candidate.get("summary", ""))).strip()
        if not title or not summary:
            return None, "missing_title_or_summary"

        score, score_breakdown = self._normalize_score(candidate)
        if score < 65:
            return None, "importance_below_65"

        candidate_evidence = candidate.get("evidence", []) if isinstance(candidate.get("evidence", []), list) else []
        canonical_hint = normalize_url(str(candidate.get("canonical_url", "")))
        urls: list[tuple[str, dict[str, Any]]] = []
        if canonical_hint:
            urls.append((canonical_hint, {}))
        for row in candidate_evidence:
            if not isinstance(row, dict):
                continue
            url = normalize_url(str(row.get("url", "")))
            if url and all(url != existing[0] for existing in urls):
                urls.append((url, row))
        if not urls:
            return None, "missing_evidence_url"

        verified: list[Evidence] = []
        for url, hint in urls[:4]:
            page = self.page_reader.read(url)
            if not bool(page.get("ok")):
                continue
            published = str(page.get("published_at_utc", ""))
            if not published:
                continue
            publisher, evidence_type = self._domain_meta(url, str(hint.get("evidence_type", "general_media")))
            canonical = normalize_url(str(page.get("canonical_url", ""))) or url
            verified.append(
                Evidence(
                    url=url,
                    publisher=str(page.get("publisher", "")) or publisher,
                    evidence_type=evidence_type,
                    published_at_utc=published,
                    is_primary=evidence_type in PRIMARY_EVIDENCE,
                    independent=True,
                    accessible=True,
                    date_verified=True,
                    canonical_url=canonical,
                    excerpt=str(page.get("content", ""))[:800],
                )
            )
        if not verified:
            return None, "no_accessible_date_verified_evidence"

        hosts = {(urlparse(row.canonical_url or row.url).netloc or "").lower().removeprefix("www.") for row in verified}
        has_primary = any(row.is_primary for row in verified)
        independent_media = {
            host
            for row, host in zip(verified, [(urlparse(value.canonical_url or value.url).netloc or "").lower().removeprefix("www.") for value in verified])
            if row.evidence_type in {"industry_media", "general_media"}
        }
        if not has_primary and len(independent_media) < 2:
            return None, "insufficient_independent_evidence"

        published_values = [parse_datetime(row.published_at_utc) for row in verified]
        primary_values = [parse_datetime(row.published_at_utc) for row in verified if row.is_primary]
        published_dt = min(primary_values or published_values)
        start_utc, end_utc, late_start_utc = self._window(run_date)
        late_arrival = False
        if not (start_utc <= published_dt < end_utc):
            if late_start_utc <= published_dt < start_utc and score >= 80:
                late_arrival = True
            else:
                return None, "outside_time_window"

        source = {
            "coverage_domains": list(ALLOWED_DOMAINS),
            "evidence_type": "industry_media",
        }
        scope = classify_industry_item({"title": title, "content": summary}, source)
        if not bool(scope.get("in_scope")):
            return None, str(scope.get("scope_reason", "out_of_scope"))

        domains = [str(value) for value in candidate.get("coverage_domains", []) if str(value) in ALLOWED_DOMAINS]
        if not domains:
            domains = [str(value) for value in scope.get("coverage_domains", []) if str(value) in ALLOWED_DOMAINS]
        automation = str(candidate.get("automation_level", scope.get("automation_level", "unknown")))
        if automation not in self.allowed_automation:
            automation = "unknown"
        canonical = next((row.canonical_url for row in verified if row.is_primary), "") or verified[0].canonical_url
        now = datetime.now(timezone.utc).isoformat()
        event_id = sha1_text(f"{canonical}|{title}|{published_dt.isoformat()}")
        return (
            AgentEvent(
                event_id=event_id,
                title=title,
                factual_summary=summary,
                companies=[str(value).strip() for value in candidate.get("companies", []) if str(value).strip()],
                coverage_domains=domains,
                automation_level=automation,
                event_type=str(candidate.get("event_type", scope.get("event_type", "other"))),
                deployment_stage=str(candidate.get("deployment_stage", scope.get("deployment_stage", "unknown"))),
                published_at_utc=published_dt.astimezone(timezone.utc).isoformat(),
                first_seen_at_utc=now,
                late_arrival=late_arrival,
                importance_score=score,
                score_breakdown=score_breakdown,
                canonical_url=canonical,
                evidence=verified,
                verification_status="verified_primary" if has_primary else "verified_two_media",
                agent_run_id=agent_run_id,
                model_provider="deepseek",
                search_provider="deepseek_web",
            ),
            "verified",
        )
