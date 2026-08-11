import json
from pathlib import Path

from app.taxonomy import classify_industry_item, validate_social_candidate
from app.parse import canonicalize_row
from app.social_provider import ManualSeedSocialProvider


FIXTURE = Path(__file__).parent / "fixtures" / "golden_scope.json"
SOURCE = {
    "coverage_domains": ["robotaxi", "passenger_l3", "passenger_l4", "core_supply_chain", "regulation_safety"],
    "evidence_type": "industry_media",
}


def test_golden_scope_has_required_size_and_labels() -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert len(payload["positive"]) == 60
    assert len(payload["negative"]) == 30
    false_negatives = [title for title in payload["positive"] if not classify_industry_item({"title": title}, SOURCE)["in_scope"]]
    false_positives = [title for title in payload["negative"] if classify_industry_item({"title": title}, SOURCE)["in_scope"]]
    assert false_negatives == []
    assert false_positives == []


def test_supplier_requires_explicit_program_binding() -> None:
    source = {"coverage_domains": ["core_supply_chain"], "evidence_type": "company_newsroom"}
    result = classify_industry_item({"title": "禾赛发布新一代激光雷达产品"}, source)
    assert result["in_scope"] is False
    assert result["scope_reason"] == "scope_gate_miss"


def test_social_candidate_must_be_verifiable_and_official() -> None:
    source = {"source_role": "social_discovery"}
    base = {
        "title": "Waymo Robotaxi expands service",
        "canonical_url": "https://x.com/Waymo/status/123",
        "published_at_utc": "2026-08-10T10:00:00+00:00",
        "published_missing": False,
    }
    ok, reason = validate_social_candidate({**base, "official_account_verified": False}, source)
    assert (ok, reason) == (False, "social_official_account_unverified")
    ok, reason = validate_social_candidate({**base, "official_account_verified": True}, source)
    assert (ok, reason) == (True, "social_verified")


def test_x_permalink_requires_exact_configured_handle() -> None:
    base = {
        "source_id": "official_x_social_discovery",
        "source_name": "Official X",
        "source_type": "search_result",
        "source_role": "social_discovery",
        "evidence_type": "social_post",
        "region": "foreign",
        "company_hint": "",
        "fetched_at": "2026-08-11T01:00:00+00:00",
        "url": "https://x.com/Waymo/status/123456",
        "coverage_domains": ["robotaxi"],
        "official_accounts": {"x_handles": ["@Waymo"]},
        "payload": {
            "title": "Waymo Robotaxi expands service",
            "link": "https://x.com/Waymo/status/123456",
            "published": "2026-08-10T10:00:00+00:00",
            "outbound_urls": ["https://waymo.com/blog/official-expansion"],
        },
    }
    official = canonicalize_row(base)
    impostor = canonicalize_row(
        {
            **base,
            "url": "https://x.com/WaymoNews/status/123456",
            "payload": {**base["payload"], "link": "https://x.com/WaymoNews/status/123456"},
        }
    )
    assert official is not None and official.official_account_verified is True
    assert official.social_platform == "x"
    assert official.canonical_url == "https://waymo.com/blog/official-expansion"
    assert impostor is not None and impostor.official_account_verified is False


def test_manual_wechat_seed_requires_exact_official_name_and_permalink(tmp_path) -> None:
    seed = tmp_path / "social-seeds.json"
    seed.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "platform": "wechat",
                        "account": "小马智行",
                        "permalink": "https://mp.weixin.qq.com/s/official123",
                        "published_at_utc": "2026-08-10T10:00:00+00:00",
                        "text": "小马智行 Robotaxi 获批扩大运营",
                    },
                    {
                        "platform": "wechat",
                        "account": "小马智行资讯",
                        "permalink": "https://mp.weixin.qq.com/s/impostor123",
                        "published_at_utc": "2026-08-10T10:00:00+00:00",
                        "text": "同名非官方账号内容",
                    },
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    provider = ManualSeedSocialProvider(seed)
    rows = provider.fetch_since({"wechat_names": ["小马智行"]}, "2026-08-09T00:00:00+00:00")
    assert len(rows) == 1
    assert rows[0].account == "小马智行"
