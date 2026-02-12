#!/usr/bin/env zsh
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" && pwd)"
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
APP_DIR="$HOME/.robtaxi-digest"
APP_SCRIPTS_DIR="$APP_DIR/scripts"
APP_LOG_DIR="$APP_DIR/logs"
PLIST_DIR="$HOME/Library/LaunchAgents"
PLIST_PATH="$PLIST_DIR/com.robtaxi.digest.plist"
LOG_DIR="$APP_LOG_DIR"
WORKSPACE_LINK="$PROJECT_ROOT/robtaxi_digest_latest.html"
RUNTIME_HTML="$APP_DIR/robtaxi_digest_latest.html"

if [[ -z "${SERPAPI_API_KEY:-}" ]]; then
  echo "ERROR: SERPAPI_API_KEY is empty. Please export it before install."
  echo 'Example: export SERPAPI_API_KEY="your_key"'
  exit 1
fi

# XML-escape env value before embedding into plist.
SERPAPI_API_KEY_XML="${SERPAPI_API_KEY//&/&amp;}"
SERPAPI_API_KEY_XML="${SERPAPI_API_KEY_XML//</&lt;}"
SERPAPI_API_KEY_XML="${SERPAPI_API_KEY_XML//>/&gt;}"

mkdir -p "$PLIST_DIR" "$APP_SCRIPTS_DIR" "$LOG_DIR"

cp "$PROJECT_ROOT/scripts/robtaxi_digest.py" "$APP_SCRIPTS_DIR/robtaxi_digest.py"
cp "$PROJECT_ROOT/scripts/run_if_due.sh" "$APP_SCRIPTS_DIR/run_if_due.sh"
cp "$PROJECT_ROOT/scripts/validate_config.py" "$APP_SCRIPTS_DIR/validate_config.py"
cp "$PROJECT_ROOT/scripts/test_sources_health.sh" "$APP_SCRIPTS_DIR/test_sources_health.sh"
chmod +x "$APP_SCRIPTS_DIR/robtaxi_digest.py" "$APP_SCRIPTS_DIR/run_if_due.sh" "$APP_SCRIPTS_DIR/validate_config.py" "$APP_SCRIPTS_DIR/test_sources_health.sh"
cp "$PROJECT_ROOT/sources.yaml" "$APP_DIR/sources.yaml"

# Put a clickable shortcut in the workspace.
ln -sf "$RUNTIME_HTML" "$WORKSPACE_LINK"

cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.robtaxi.digest</string>

  <key>ProgramArguments</key>
  <array>
    <string>/bin/zsh</string>
    <string>-c</string>
    <string>./scripts/run_if_due.sh</string>
  </array>

  <key>WorkingDirectory</key>
  <string>$APP_DIR</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>SERPAPI_API_KEY</key>
    <string>$SERPAPI_API_KEY_XML</string>
  </dict>

  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key>
    <integer>9</integer>
    <key>Minute</key>
    <integer>0</integer>
  </dict>

  <key>StandardOutPath</key>
  <string>$LOG_DIR/launchd.out.log</string>

  <key>StandardErrorPath</key>
  <string>$LOG_DIR/launchd.err.log</string>
</dict>
</plist>
EOF

launchctl unload "$PLIST_PATH" 2>/dev/null || true
launchctl load "$PLIST_PATH"

echo "Installed and loaded: $PLIST_PATH"
echo "Runtime dir: $APP_DIR"
echo "Digest HTML: $RUNTIME_HTML"
echo "Workspace link: $WORKSPACE_LINK"
echo "Sources config: $APP_DIR/sources.yaml"
echo "Logs: $LOG_DIR"
