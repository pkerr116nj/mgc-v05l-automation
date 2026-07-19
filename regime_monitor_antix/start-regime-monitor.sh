#!/usr/bin/env bash
set -euo pipefail

DASHBOARD_URL="${REGIME_MONITOR_DASHBOARD_URL:-http://192.168.1.80:5000/}"

# Keep kiosk displays awake when xset is available.
if command -v xset >/dev/null 2>&1; then
  xset s off || true
  xset -dpms || true
  xset s noblank || true
fi

if command -v unclutter >/dev/null 2>&1; then
  unclutter -idle 0.1 -root >/dev/null 2>&1 &
fi

if command -v chromium >/dev/null 2>&1; then
  exec chromium --kiosk --noerrdialogs --disable-infobars "$DASHBOARD_URL"
elif command -v chromium-browser >/dev/null 2>&1; then
  exec chromium-browser --kiosk --noerrdialogs --disable-infobars "$DASHBOARD_URL"
elif command -v firefox >/dev/null 2>&1; then
  exec firefox --kiosk "$DASHBOARD_URL"
elif command -v x-www-browser >/dev/null 2>&1; then
  exec x-www-browser "$DASHBOARD_URL"
else
  echo "Install Chromium or Firefox to display $DASHBOARD_URL in kiosk mode." >&2
  exit 1
fi
