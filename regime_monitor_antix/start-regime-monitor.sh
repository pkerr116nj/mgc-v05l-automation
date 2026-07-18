#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOST="${REGIME_MONITOR_HOST:-127.0.0.1}"
PORT="${REGIME_MONITOR_PORT:-8765}"
URL="http://${HOST}:${PORT}/"

# Keep kiosk displays awake when xset is available.
if command -v xset >/dev/null 2>&1; then
  xset s off || true
  xset -dpms || true
  xset s noblank || true
fi

# The Python app hides the pointer itself. If unclutter is installed, it helps
# with window managers that override Tk's cursor setting.
if command -v unclutter >/dev/null 2>&1; then
  unclutter -idle 0.1 -root >/dev/null 2>&1 &
fi

python3 "$APP_DIR/regime_monitor.py" --host "$HOST" --port "$PORT" &
SERVER_PID="$!"

sleep 2

if command -v chromium >/dev/null 2>&1; then
  chromium --kiosk --noerrdialogs --disable-infobars "$URL" &
elif command -v chromium-browser >/dev/null 2>&1; then
  chromium-browser --kiosk --noerrdialogs --disable-infobars "$URL" &
elif command -v firefox >/dev/null 2>&1; then
  firefox --kiosk "$URL" &
elif command -v x-www-browser >/dev/null 2>&1; then
  x-www-browser "$URL" &
else
  echo "Regime monitor is serving at $URL; install Chromium or Firefox for kiosk fullscreen."
fi

wait "$SERVER_PID"
