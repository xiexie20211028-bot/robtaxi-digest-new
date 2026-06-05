import json

from app.common import write_json
from app.editorial_digest import (
    FORMAT_VERSION,
    build_fallback_digest,
    load_digest_text,
    main as editorial_main,
    render_digest_text,
    validate_digest,
)


def _item(title: str, importance: int = 3) -> dict:
    return {
        "title_zh": title,
        "summary_what": f"{title} 已发布。",
        "summary_why": "背景是 Robotaxi 商业化持续推进。",
        "summary_so_what": f"{title} 会影响运营节奏。",
        "impact_targets": ["运营方", "监管"],
        "link": f"https://example.com/{importance}",
        "published_at_utc": f"2026-03-0{importance}T00:00:00+00:00",
        "importance": importance,
    }


def test_empty_digest_is_directly_readable() -> None:
    digest = build_fallback_digest("2026-03-09", [], {"window_start_bj": "2026-03-08 00:00:00"})
    ok, reason = validate_digest(digest)
    assert ok, reason
    text = render_digest_text(digest, "https://example.com/page")
    assert "今日无符合规则的重点新闻" in text
    assert "完整网页：https://example.com/page" in text


def test_single_item_digest_contains_key_fields() -> None:
    digest = build_fallback_digest("2026-03-09", [_item("Waymo 扩大 Robotaxi 服务", 5)], {})
    text = render_digest_text(digest)
    assert "今日判断" in text
    assert "重点新闻" in text
    assert "Waymo 扩大 Robotaxi 服务" in text
    assert "影响对象：运营方 / 监管" in text


def test_many_items_digest_limits_top_items_and_lists_others() -> None:
    items = [_item("新闻 A", 5), _item("新闻 B", 4), _item("新闻 C", 3), _item("新闻 D", 2)]
    digest = build_fallback_digest("2026-03-09", items, {}, top_n=2, source_top_n=4)
    assert len(digest["top_items"]) == 2
    assert digest["other_items"] == ["新闻 C", "新闻 D"]


def test_load_digest_text_prefers_valid_daily_digest(tmp_path) -> None:
    digest = build_fallback_digest("2026-03-09", [_item("小马智行运营进展", 4)], {})
    out_dir = tmp_path / "digest" / "2026-03-09"
    write_json(out_dir / "daily_digest.json", digest)
    text = load_digest_text("2026-03-09", tmp_path / "digest", "https://example.com/full")
    assert "小马智行运营进展" in text
    assert "完整网页：https://example.com/full" in text


def test_editorial_main_falls_back_when_model_key_missing(tmp_path, monkeypatch) -> None:
    date_text = "2026-03-09"
    brief_dir = tmp_path / "brief" / date_text
    report_dir = tmp_path / "reports" / date_text
    brief_dir.mkdir(parents=True)
    report_dir.mkdir(parents=True)
    (brief_dir / "brief_items.jsonl").write_text(json.dumps(_item("Robotaxi 安全事件", 5), ensure_ascii=False) + "\n", encoding="utf-8")
    write_json(report_dir / "run_report.json", {"window_start_bj": "2026-03-08 00:00:00"})
    write_json(tmp_path / "sources.json", {"defaults": {"notify_digest_top_n": 3, "notify_digest_source_top_n": 8}})
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.setattr(
        "sys.argv",
        [
            "editorial_digest",
            "--date",
            date_text,
            "--in",
            str(tmp_path / "brief"),
            "--out",
            str(tmp_path / "digest"),
            "--report",
            str(tmp_path / "reports"),
            "--sources",
            str(tmp_path / "sources.json"),
            "--provider",
            "deepseek",
        ],
    )
    assert editorial_main() == 0
    digest = json.loads((tmp_path / "digest" / date_text / "daily_digest.json").read_text(encoding="utf-8"))
    assert digest["format_version"] == FORMAT_VERSION
    assert digest["fallback_used"] is True
    assert "Robotaxi 安全事件" in (tmp_path / "digest" / date_text / "daily_digest.txt").read_text(encoding="utf-8")
