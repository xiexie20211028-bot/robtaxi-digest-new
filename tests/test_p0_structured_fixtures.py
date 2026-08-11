from pathlib import Path

from app.common import read_json
from app.fetch_structured import _extract_article_css, _extract_links_css
from app.source_config import apply_profile


ROOT = Path(__file__).resolve().parents[1]


def test_every_optimized_p0_css_adapter_passes_fixed_fixture() -> None:
    cfg, _ = apply_profile(read_json(ROOT / "sources.json"), "optimized")
    sources = [
        source
        for source in cfg["sources"]
        if source["enabled"] and source["criticality"] == "required" and source["source_type"] == "structured_web"
    ]
    assert len(sources) >= 25
    for source in sources:
        fixture_path = ROOT / source["health_policy"]["fixture_path"]
        html = fixture_path.read_text(encoding="utf-8")
        links = _extract_links_css(source["entry_urls"][0], html, source["selectors"])
        assert links, source["id"]
        article = _extract_article_css(links[0], html, source["selectors"], source["name"], source["id"])
        assert article["title"], source["id"]
        assert article["content"], source["id"]
        assert article["published"], source["id"]
        assert article["canonical_url"], source["id"]
        assert article["attachment_link"], source["id"]

