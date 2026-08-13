from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Protocol


@dataclass
class ProviderUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    web_searches: int = 0
    estimated_cost_cny: float = 0.0

    def add(self, other: "ProviderUsage") -> None:
        self.input_tokens += int(other.input_tokens)
        self.output_tokens += int(other.output_tokens)
        self.cache_read_tokens += int(other.cache_read_tokens)
        self.web_searches += int(other.web_searches)
        self.estimated_cost_cny = round(self.estimated_cost_cny + float(other.estimated_cost_cny), 6)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Evidence:
    url: str
    publisher: str = ""
    evidence_type: str = "general_media"
    published_at_utc: str = ""
    is_primary: bool = False
    independent: bool = True
    accessible: bool = False
    date_verified: bool = False
    canonical_url: str = ""
    excerpt: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AgentEvent:
    event_id: str
    title: str
    factual_summary: str
    companies: list[str]
    coverage_domains: list[str]
    automation_level: str
    event_type: str
    deployment_stage: str
    published_at_utc: str
    first_seen_at_utc: str
    late_arrival: bool
    importance_score: int
    score_breakdown: dict[str, int]
    canonical_url: str
    evidence: list[Evidence]
    verification_status: str
    agent_run_id: str
    model_provider: str
    search_provider: str
    discovery_method: str = "agent_search"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["evidence"] = [row.to_dict() for row in self.evidence]
        return payload


@dataclass
class SearchResearchResult:
    text: str
    usage: ProviderUsage
    trace: list[dict[str, Any]] = field(default_factory=list)
    capability_confirmed: bool = False


class ModelProvider(Protocol):
    name: str

    def complete_json(self, system_prompt: str, user_prompt: str) -> tuple[dict[str, Any], ProviderUsage]: ...


class SearchProvider(Protocol):
    name: str

    def probe(self) -> tuple[bool, ProviderUsage, list[dict[str, Any]]]: ...

    def research(self, system_prompt: str, user_prompt: str, max_searches: int) -> SearchResearchResult: ...


class PageReader(Protocol):
    def read(self, url: str) -> dict[str, Any]: ...


class EvidenceVerifier(Protocol):
    def verify(self, candidate: dict[str, Any], run_date: str, agent_run_id: str) -> tuple[AgentEvent | None, str]: ...
