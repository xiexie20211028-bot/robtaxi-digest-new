from pathlib import Path
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from app.common import read_json
from app.fetch_structured import _extract_article_css, _extract_links_css
from app.source_config import apply_profile


ROOT = Path(__file__).resolve().parents[1]


def test_every_structured_fixture_is_bound_to_exact_source() -> None:
    cfg = read_json(ROOT / "sources.json")
    sources = [
        source
        for source in cfg["sources"]
        if str(source.get("health_policy", {}).get("fixture_path", ""))
    ]
    fixture_paths = [source["health_policy"]["fixture_path"] for source in sources]
    assert len(fixture_paths) == len(set(fixture_paths))
    for source in sources:
        fixture_path = ROOT / source["health_policy"]["fixture_path"]
        soup = BeautifulSoup(fixture_path.read_text(encoding="utf-8"), "html.parser")
        marker = soup.select_one('meta[name="fixture-source-id"]')
        assert marker is not None and marker.get("content") == source["id"]


def test_every_optimized_p0_css_adapter_passes_fixed_fixture() -> None:
    cfg, _ = apply_profile(read_json(ROOT / "sources.json"), "optimized")
    sources = [
        source
        for source in cfg["sources"]
        if source["enabled"] and source["criticality"] == "required" and source["source_type"] == "structured_web"
    ]
    assert len(sources) >= 25
    fixture_paths = [source["health_policy"]["fixture_path"] for source in sources]
    assert len(fixture_paths) == len(set(fixture_paths))
    for source in sources:
        fixture_path = ROOT / source["health_policy"]["fixture_path"]
        html = fixture_path.read_text(encoding="utf-8")
        soup = BeautifulSoup(html, "html.parser")
        marker = soup.select_one('meta[name="fixture-source-id"]')
        assert marker is not None and marker.get("content") == source["id"]
        links = _extract_links_css(source["entry_urls"][0], html, source["selectors"])
        assert links, source["id"]
        assert urlparse(links[0]).hostname == urlparse(source["entry_urls"][0]).hostname
        article = _extract_article_css(links[0], html, source["selectors"], source["name"], source["id"])
        assert article["title"], source["id"]
        assert article["content"], source["id"]
        assert article["published"], source["id"]
        assert article["canonical_url"], source["id"]
        assert article["attachment_link"], source["id"]
