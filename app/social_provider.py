from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


@dataclass(frozen=True)
class SocialCandidate:
    platform: str
    account: str
    permalink: str
    published_at_utc: str
    text: str
    outbound_urls: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)


class SocialProvider(ABC):
    """P1 社交补漏统一接口；第一版不绑定 X API、微信登录态或手机 RPA。"""

    @abstractmethod
    def fetch_since(self, official_accounts: dict[str, Any], since_utc: str) -> list[SocialCandidate]:
        raise NotImplementedError


class DisabledSocialProvider(SocialProvider):
    def fetch_since(self, official_accounts: dict[str, Any], since_utc: str) -> list[SocialCandidate]:
        _ = official_accounts, since_utc
        return []


class ManualSeedSocialProvider(SocialProvider):
    """读取人工确认的永久链接种子；不登录微信，也不调用 X API。"""

    def __init__(self, seed_file: Path):
        self.seed_file = seed_file

    def fetch_since(self, official_accounts: dict[str, Any], since_utc: str) -> list[SocialCandidate]:
        if not self.seed_file.exists():
            return []
        try:
            payload = json.loads(self.seed_file.read_text(encoding="utf-8"))
        except Exception:
            return []
        rows = payload.get("items", []) if isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            return []
        candidates: list[SocialCandidate] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            platform = str(row.get("platform", "")).strip().lower()
            account = str(row.get("account", "")).strip()
            permalink = str(row.get("permalink", "")).strip()
            published = str(row.get("published_at_utc", "")).strip()
            text = str(row.get("text", "")).strip()
            if published < since_utc or not text or not _verified_account_permalink(platform, account, permalink, official_accounts):
                continue
            candidates.append(
                SocialCandidate(
                    platform=platform,
                    account=account,
                    permalink=permalink,
                    published_at_utc=published,
                    text=text,
                    outbound_urls=[str(value) for value in row.get("outbound_urls", []) if str(value)],
                    raw=row,
                )
            )
        return candidates


def _verified_account_permalink(
    platform: str,
    account: str,
    permalink: str,
    official_accounts: dict[str, Any],
) -> bool:
    parsed = urlparse(permalink)
    host = (parsed.netloc or "").lower()
    parts = [value for value in (parsed.path or "").split("/") if value]
    if platform == "x":
        handles = {str(value).strip().lstrip("@").lower() for value in official_accounts.get("x_handles", [])}
        return (
            host in {"x.com", "www.x.com", "twitter.com", "www.twitter.com"}
            and len(parts) >= 3
            and parts[0].lower() == account.lstrip("@").lower()
            and parts[0].lower() in handles
            and parts[1].lower() == "status"
        )
    if platform == "wechat":
        names = {str(value).strip() for value in official_accounts.get("wechat_names", [])}
        return host == "mp.weixin.qq.com" and account in names and bool(parts)
    return False


def build_social_provider(config: dict[str, Any] | None = None) -> SocialProvider:
    """返回当前生产 provider；未来接入官方 API 时只需新增实现并在此路由。"""
    config = config or {}
    if str(config.get("provider", "")).strip().lower() == "manual_seed":
        env_name = str(config.get("seed_file_env", "SOCIAL_SEED_FILE")).strip()
        seed_path = os.environ.get(env_name, "").strip() if env_name else ""
        seed_path = seed_path or str(config.get("seed_file", ".state/manual_social_seeds.json"))
        return ManualSeedSocialProvider(Path(seed_path).expanduser())
    return DisabledSocialProvider()


def fetch_social_seed_source(source: dict[str, Any], since_utc: str) -> tuple[list[dict[str, Any]], str]:
    provider = build_social_provider(source)
    candidates = provider.fetch_since(source.get("official_accounts", {}), since_utc)
    rows = [
        {
            "title": candidate.text[:240],
            "summary": candidate.text,
            "content": candidate.text,
            "link": candidate.permalink,
            "published": candidate.published_at_utc,
            "social_platform": candidate.platform,
            "official_account_verified": True,
            "outbound_urls": candidate.outbound_urls,
            "source_name": str(source.get("name", "Manual social seeds")),
        }
        for candidate in candidates
    ]
    return rows, ""
