#!/usr/bin/env zsh
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
STATE_DIR="$PROJECT_ROOT/.state"
DATE_BJ="$(TZ=Asia/Shanghai date +%Y-%m-%d)"
export DATE_BJ

mkdir -p "$STATE_DIR"

cd "$PROJECT_ROOT"
python3 -m app.fetch --date "$DATE_BJ" --sources ./sources.json --out ./artifacts/raw --report ./artifacts/reports

python3 - <<'PY'
import json
from pathlib import Path

root = Path("./artifacts/reports")
date = __import__("os").environ.get("DATE_BJ")
report_file = root / date / "run_report.json"
out_file = Path("./.state/source_health_latest.tsv")

report = json.loads(report_file.read_text(encoding="utf-8"))
stats = report.get("source_stats", [])

with out_file.open("w", encoding="utf-8") as f:
    f.write("id\tstatus\tfetched\terror\n")
    for row in stats:
        f.write(
            f"{row.get('source_id','')}\t{row.get('status','')}\t{row.get('fetched_items',0)}\t{str(row.get('error',''))[:120]}\n"
        )

print(f"Health report saved: {out_file}")
PY
