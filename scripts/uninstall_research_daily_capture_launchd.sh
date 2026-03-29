#!/usr/bin/env bash
set -euo pipefail

LABEL="com.mgc_v05l.research_daily_capture.daily"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"
RUNTIME_ROOT="${HOME}/Library/Application Support/mgc_v05l/research_daily_capture_runtime"
GUI_DOMAIN="gui/$(id -u)"

launchctl bootout "${GUI_DOMAIN}" "${PLIST_PATH}" >/dev/null 2>&1 || true
rm -f "${PLIST_PATH}"
rm -rf "${RUNTIME_ROOT}"

echo "Uninstalled ${LABEL}"
echo "Removed: ${PLIST_PATH}"
echo "Removed: ${RUNTIME_ROOT}"
