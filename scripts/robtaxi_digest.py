#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def run(cmd: list[str]) -> None:
    print("[run]", " ".join(cmd))
    proc = subprocess.run(cmd)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def main() -> int:
    parser = argparse.ArgumentParser(description="Robtaxi digest v2 wrapper")
    parser.add_argument("--date", default="", help="Date YYYY-MM-DD")
    parser.add_argument("--sources", default="./sources.json", help="Path to sources config")
    parser.add_argument("--output", default="./site/index.html", help="Output html path")
    parser.add_argument("--dry-run", action="store_true", help="Run fetch/parse/summarize without rendering")
    parser.add_argument("--health-report", action="store_true", help="Run fetch stage and print source stats")
    parser.add_argument("--report", default="./artifacts/reports", help="Report root")
    parser.add_argument("--raw", default="./artifacts/raw", help="Raw output root")
    parser.add_argument("--canonical", default="./artifacts/canonical", help="Canonical output root")
    parser.add_argument("--filtered", default="./artifacts/filtered", help="Filtered output root")
    parser.add_argument("--brief", default="./artifacts/brief", help="Brief output root")
    args = parser.parse_args()

    base = [sys.executable, "-m"]
    shared = []
    if args.date.strip():
        shared.extend(["--date", args.date.strip()])

    run(base + ["app.fetch"] + shared + ["--sources", args.sources, "--out", args.raw, "--report", args.report])

    if args.health_report:
        return 0

    run(base + ["app.parse"] + shared + ["--in", args.raw, "--out", args.canonical, "--report", args.report])
    run(
        base
        + ["app.filter_relevance"]
        + shared
        + ["--in", args.canonical, "--out", args.filtered, "--sources", args.sources, "--report", args.report]
    )
    run(
        base
        + ["app.summarize"]
        + shared
        + ["--in", args.filtered, "--out", args.brief, "--provider", "deepseek", "--report", args.report]
    )

    if not args.dry_run:
        run(
            base
            + ["app.render"]
            + shared
            + ["--in", args.brief, "--out", args.output, "--report", args.report, "--sources", args.sources]
        )

    print(f"[ok] done output={Path(args.output).expanduser()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
