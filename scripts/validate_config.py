#!/usr/bin/env python3
import json
import sys
from pathlib import Path
from urllib.parse import urlparse


REQUIRED_SOURCE_FIELDS = {"id", "name", "region", "tier", "category", "enabled"}
REQUIRED_COMPANY_FIELDS = {"id", "name", "region", "newsroom", "social"}


def fail(msg: str) -> None:
    print(f"[ERROR] {msg}")
    raise SystemExit(1)


def is_http_url(url: str) -> bool:
    p = urlparse(url)
    return p.scheme in {"http", "https"} and bool(p.netloc)


def main() -> int:
    cfg_path = Path(sys.argv[1] if len(sys.argv) > 1 else "sources.yaml").expanduser()
    if not cfg_path.is_absolute():
        cfg_path = Path.cwd() / cfg_path
    if not cfg_path.exists():
        fail(f"config file not found: {cfg_path}")

    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception as e:
        fail(f"invalid JSON-compatible YAML in {cfg_path}: {e}")

    if "sources" not in cfg or not isinstance(cfg["sources"], list):
        fail("sources must be a list")
    if "companies" not in cfg or not isinstance(cfg["companies"], list):
        fail("companies must be a list")
    if "search_providers" in cfg and not isinstance(cfg["search_providers"], dict):
        fail("search_providers must be an object")
    if "query_sets" in cfg and not isinstance(cfg["query_sets"], dict):
        fail("query_sets must be an object")

    company_ids = set()
    for i, c in enumerate(cfg["companies"]):
        missing = REQUIRED_COMPANY_FIELDS - set(c.keys())
        if missing:
            fail(f"companies[{i}] missing fields: {sorted(missing)}")
        cid = str(c.get("id", "")).strip()
        if not cid:
            fail(f"companies[{i}] id is empty")
        if cid in company_ids:
            fail(f"duplicate company id: {cid}")
        company_ids.add(cid)

    providers = cfg.get("search_providers", {})
    query_sets = cfg.get("query_sets", {})
    if not isinstance(providers, dict):
        providers = {}
    if not isinstance(query_sets, dict):
        query_sets = {}
    for pid, pobj in providers.items():
        if not isinstance(pobj, dict):
            fail(f"search_providers.{pid} must be an object")
        endpoint = str(pobj.get("endpoint", "")).strip()
        if endpoint and not is_http_url(endpoint):
            fail(f"search_providers.{pid}.endpoint invalid: {endpoint}")
        key_env = str(pobj.get("api_key_env", "")).strip()
        if key_env and not key_env.replace("_", "").isalnum():
            fail(f"search_providers.{pid}.api_key_env invalid: {key_env}")
    for qid, qset in query_sets.items():
        if not isinstance(qset, list) or not qset:
            fail(f"query_sets.{qid} must be a non-empty list")
        for idx, row in enumerate(qset):
            if isinstance(row, str):
                if not row.strip():
                    fail(f"query_sets.{qid}[{idx}] empty query string")
            elif isinstance(row, dict):
                q = str(row.get("q", "")).strip()
                if not q:
                    fail(f"query_sets.{qid}[{idx}] missing q")
            else:
                fail(f"query_sets.{qid}[{idx}] must be string or object")

    source_ids = set()
    for i, s in enumerate(cfg["sources"]):
        missing = REQUIRED_SOURCE_FIELDS - set(s.keys())
        if missing:
            fail(f"sources[{i}] missing fields: {sorted(missing)}")

        sid = str(s.get("id", "")).strip()
        if not sid:
            fail(f"sources[{i}] id is empty")
        if sid in source_ids:
            fail(f"duplicate source id: {sid}")
        source_ids.add(sid)

        region = str(s.get("region", "")).lower().strip()
        if region not in {"domestic", "foreign"}:
            fail(f"sources[{i}] invalid region: {region}")

        source_type = str(s.get("source_type", "rss")).lower().strip() or "rss"
        if source_type not in {"rss", "search_api"}:
            fail(f"sources[{i}] invalid source_type: {source_type}")
        if source_type == "rss":
            urls = s.get("rss_urls", [])
            if not isinstance(urls, list) or not urls:
                fail(f"sources[{i}] rss_urls must be non-empty list for source_type=rss")
            for u in urls:
                if not is_http_url(str(u)):
                    fail(f"sources[{i}] invalid rss url: {u}")
        else:
            provider = str(s.get("provider", "")).strip()
            query_set = str(s.get("query_set", "")).strip()
            if not provider:
                fail(f"sources[{i}] provider required for source_type=search_api")
            if provider not in providers:
                fail(f"sources[{i}] provider not found in search_providers: {provider}")
            if not query_set:
                fail(f"sources[{i}] query_set required for source_type=search_api")
            if query_set not in query_sets:
                fail(f"sources[{i}] query_set not found in query_sets: {query_set}")
            max_results = s.get("max_results_per_query", 10)
            try:
                if int(max_results) <= 0:
                    fail(f"sources[{i}] max_results_per_query must be > 0")
            except Exception:
                fail(f"sources[{i}] max_results_per_query must be int")

        company_id = str(s.get("source_company_id", "")).strip()
        if company_id and company_id not in company_ids:
            fail(f"sources[{i}] source_company_id not found in companies: {company_id}")

    print(f"[OK] config valid: {cfg_path}")
    print(f"companies={len(cfg['companies'])} sources={len(cfg['sources'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
