#!/usr/bin/env zsh
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
SOURCES_FILE="$PROJECT_ROOT/sources.yaml"

"$SCRIPT_DIR/validate_config.py" "$SOURCES_FILE"

cd "$PROJECT_ROOT"
"$SCRIPT_DIR/robtaxi_digest.py" --sources "$SOURCES_FILE" --health-report | tee "$PROJECT_ROOT/.state/source_health_latest.tsv"

echo
echo "Health report saved: $PROJECT_ROOT/.state/source_health_latest.tsv"
