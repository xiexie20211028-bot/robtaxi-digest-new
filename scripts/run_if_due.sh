#!/usr/bin/env zsh
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
STATE_DIR="$PROJECT_ROOT/.state"
LOG_DIR="$PROJECT_ROOT/logs"
STATE_FILE="$STATE_DIR/last_generated_date_bj.txt"
LOG_FILE="$LOG_DIR/robtaxi_digest.log"
OUTPUT_HTML="$PROJECT_ROOT/robtaxi_digest_latest.html"
SOURCES_FILE="$PROJECT_ROOT/sources.yaml"

mkdir -p "$STATE_DIR" "$LOG_DIR"

DATE_BJ="$(TZ=Asia/Shanghai date +%Y-%m-%d)"

if [[ -f "$STATE_FILE" ]] && [[ "$(cat "$STATE_FILE")" == "$DATE_BJ" ]]; then
  exit 0
fi

{
  echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] generating digest for BJ date $DATE_BJ"
  "$SCRIPT_DIR/robtaxi_digest.py" --sources "$SOURCES_FILE" --output "$OUTPUT_HTML"
  echo "$DATE_BJ" > "$STATE_FILE"
  echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] done"
} >> "$LOG_FILE" 2>&1
