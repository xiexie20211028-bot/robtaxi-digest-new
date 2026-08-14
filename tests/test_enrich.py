from __future__ import annotations

import json
import sys

from app.common import read_json, read_jsonl, write_jsonl
from app.enrich import MIN_CONTENT_LEN, enrich_item, main as enrich_main


def test_enrich_item_prefers_jsonld_article_body(monkeypatch) -> None:
    body = "无人驾正文" * 100
    html = f'<script type="application/ld+json">{json.dumps({"@type": "NewsArticle", "articleBody": body}, ensure_ascii=False)}</script>'
    monkeypatch.setattr("app.enrich.http_get_bytes", lambda *_args, **_kwargs: html.encode())

    enriched = enrich_item({"link": "https://example.com/news", "content": "短摘要"})

    assert enriched["enriched"] is True
    assert enriched["content"] == body


def test_enrich_item_uses_css_body_when_jsonld_is_too_short(monkeypatch) -> None:
    body = "产业进展" * 130
    html = f'<script type="application/ld+json">{{"@type":"NewsArticle","articleBody":"short"}}</script><article><p>{body}</p></article>'
    monkeypatch.setattr("app.enrich.http_get_bytes", lambda *_args, **_kwargs: html.encode())

    enriched = enrich_item({"link": "https://example.com/news", "content": "短摘要"})

    assert enriched["enriched"] is True
    assert enriched["content"] == body


def test_enrich_item_does_not_fetch_long_content_or_missing_link(monkeypatch) -> None:
    def unexpected_fetch(*_args, **_kwargs):
        raise AssertionError("不应发起网络请求")

    monkeypatch.setattr("app.enrich.http_get_bytes", unexpected_fetch)
    long_item = enrich_item({"link": "https://example.com/news", "content": "x" * MIN_CONTENT_LEN})
    no_link_item = enrich_item({"link": "", "content": "short"})

    assert long_item["enriched"] is False
    assert no_link_item["enriched"] is False


def test_enrich_item_keeps_original_content_on_fetch_failure(monkeypatch) -> None:
    monkeypatch.setattr("app.enrich.http_get_bytes", lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError("timeout")))
    item = {"link": "https://example.com/news", "content": "原摘要"}

    enriched = enrich_item(item)

    assert enriched["enriched"] is False
    assert enriched["content"] == "原摘要"


def test_enrich_main_writes_partial_report_when_one_item_raises(tmp_path, monkeypatch) -> None:
    date_text = "2026-08-14"
    input_root = tmp_path / "filtered"
    output_root = tmp_path / "enriched"
    report_root = tmp_path / "reports"
    items = [
        {"link": "https://example.com/ok", "content": "short"},
        {"link": "https://example.com/error", "content": "short"},
    ]
    write_jsonl(input_root / date_text / "filtered_items.jsonl", items)

    def fake_enrich(item):
        if item["link"].endswith("error"):
            raise RuntimeError("fixture failure")
        return {**item, "content": "x" * MIN_CONTENT_LEN, "enriched": True}

    monkeypatch.setattr("app.enrich.enrich_item", fake_enrich)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "enrich",
            "--date",
            date_text,
            "--in",
            str(input_root),
            "--out",
            str(output_root),
            "--report",
            str(report_root),
        ],
    )

    assert enrich_main() == 0
    output = read_jsonl(output_root / date_text / "enriched_items.jsonl")
    report = read_json(report_root / date_text / "run_report.json")
    assert [item["enriched"] for item in output] == [True, False]
    assert report["stage_status"]["enrich"] == "partial"
    assert report["enrich_attempted"] == 2
    assert report["enrich_success"] == 1
    assert "fixture failure" in report["enrich_errors"]
