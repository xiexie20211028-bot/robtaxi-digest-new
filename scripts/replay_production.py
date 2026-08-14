#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.common import read_jsonl, write_json  # noqa: E402
from app.source_config import load_source_config, source_metadata  # noqa: E402
from app.taxonomy import classify_industry_item  # noqa: E402


def replay(input_root: Path, cfg: dict[str, Any]) -> dict[str, Any]:
    source_map = {
        str(source.get("id", "")): {**source, **source_metadata(source)}
        for source in cfg.get("sources", [])
        if isinstance(source, dict)
    }
    files = sorted(input_root.rglob("canonical_items.jsonl"))
    totals: Counter[str] = Counter()
    domains: Counter[str] = Counter()
    drop_reasons: Counter[str] = Counter()
    by_day: list[dict[str, Any]] = []
    for path in files:
        rows = read_jsonl(path)
        kept = 0
        for row in rows:
            source = source_map.get(str(row.get("source_id", "")), {})
            result = classify_industry_item(row, source)
            if result["in_scope"]:
                kept += 1
                domains.update(result["coverage_domains"])
                totals["primary"] += int(str(source.get("source_role", "")) == "primary")
                totals["discovery"] += int(str(source.get("source_role", "")) in {"search_discovery", "social_discovery"})
            else:
                drop_reasons[str(result["scope_reason"])] += 1
        totals["input"] += len(rows)
        totals["kept"] += kept
        by_day.append({"date": path.parent.name, "input": len(rows), "kept": kept, "dropped": len(rows) - kept})
    kept_total = totals["kept"]
    return {
        "schema_version": "optimized-offline-replay-v1",
        "days": len(files),
        "files": [str(path) for path in files],
        "total_input": totals["input"],
        "total_kept": kept_total,
        "total_dropped": totals["input"] - kept_total,
        "primary_source_share": round(totals["primary"] / kept_total, 4) if kept_total else 0.0,
        "discovery_dependency_share": round(totals["discovery"] / kept_total, 4) if kept_total else 0.0,
        "coverage_distribution": dict(domains),
        "drop_reason_distribution": dict(drop_reasons),
        "by_day": by_day,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="离线回放历史 canonical 产物并审计 optimized 范围门槛")
    parser.add_argument("--input", required=True, help="包含按日 canonical_items.jsonl 的历史产物根目录")
    parser.add_argument("--sources", default="./sources.json")
    parser.add_argument("--output", default="./artifacts/replay/optimized_replay_report.json")
    parser.add_argument("--min-days", type=int, default=8)
    args = parser.parse_args()
    cfg, _ = load_source_config(Path(args.sources).expanduser().resolve(), "optimized")
    report = replay(Path(args.input).expanduser().resolve(), cfg)
    report["minimum_days"] = args.min_days
    report["minimum_days_met"] = int(report["days"]) >= args.min_days
    write_json(Path(args.output).expanduser().resolve(), report)
    print(
        f"[replay] days={report['days']} input={report['total_input']} kept={report['total_kept']} "
        f"minimum_days_met={report['minimum_days_met']}"
    )
    return 0 if report["minimum_days_met"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
