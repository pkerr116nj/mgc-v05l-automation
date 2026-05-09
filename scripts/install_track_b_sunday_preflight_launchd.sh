#!/usr/bin/env bash
set -euo pipefail

EXPECTED_REPO_ROOT="/Users/patrick/Dev/MGC-v05l-automation"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
SCRIPT_PATH="${EXPECTED_REPO_ROOT}/scripts/track_b_paper_preflight.sh"
LABEL="com.mgc_v05l.track_b_sunday_preflight"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"
OUT_DIR="${EXPECTED_REPO_ROOT}/outputs/reports/track_b_paper_preflight"
STDOUT_LOG="${OUT_DIR}/sunday_static_preflight.stdout.log"
STDERR_LOG="${OUT_DIR}/sunday_static_preflight.stderr.log"
COMMAND_STRING="/bin/bash ${SCRIPT_PATH} --mode weekend-static"

refuse() {
  echo "REFUSE: $*" >&2
  exit 1
}

[[ "${REPO_ROOT}" == "${EXPECTED_REPO_ROOT}" ]] || refuse "repo root is ${REPO_ROOT}, expected ${EXPECTED_REPO_ROOT}"
[[ -f "${SCRIPT_PATH}" ]] || refuse "target script missing: ${SCRIPT_PATH}"
[[ -x "${SCRIPT_PATH}" ]] || refuse "target script is not executable: ${SCRIPT_PATH}"

case "${COMMAND_STRING}" in
  *"/Users/patrick/Documents"*|*"Mobile Documents"*|*"iCloud"*)
    refuse "LaunchAgent command references deprecated Documents/iCloud root"
    ;;
esac

case "${COMMAND_STRING}" in
  *"--mode monday-live"*)
    refuse "LaunchAgent must not schedule monday-live mode"
    ;;
esac

case "${COMMAND_STRING}" in
  *submit*|*cancel*|*placeOrder*|*paper_proof*)
    refuse "LaunchAgent command contains a broker-mutating or paper_proof term"
    ;;
esac

mkdir -p "${OUT_DIR}" "${HOME}/Library/LaunchAgents"

TMP_PLIST="$(mktemp "${OUT_DIR}/.${LABEL}.XXXXXX.plist")"
trap 'rm -f "${TMP_PLIST}"' EXIT

cat > "${TMP_PLIST}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${SCRIPT_PATH}</string>
    <string>--mode</string>
    <string>weekend-static</string>
  </array>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Weekday</key>
    <integer>0</integer>
    <key>Hour</key>
    <integer>16</integer>
    <key>Minute</key>
    <integer>0</integer>
  </dict>
  <key>WorkingDirectory</key>
  <string>${EXPECTED_REPO_ROOT}</string>
  <key>StandardOutPath</key>
  <string>${STDOUT_LOG}</string>
  <key>StandardErrorPath</key>
  <string>${STDERR_LOG}</string>
</dict>
</plist>
PLIST

grep -q "/Users/patrick/Documents" "${TMP_PLIST}" && refuse "plist references deprecated Documents root"
grep -q "Mobile Documents" "${TMP_PLIST}" && refuse "plist references Mobile Documents"
grep -q "iCloud" "${TMP_PLIST}" && refuse "plist references iCloud"
grep -q -- "--mode monday-live" "${TMP_PLIST}" && refuse "plist would run monday-live"
grep -Eq "submit|cancel|placeOrder|paper_proof" "${TMP_PLIST}" && refuse "plist contains broker-mutating or paper_proof term"

plutil -lint "${TMP_PLIST}" >/dev/null
cp "${TMP_PLIST}" "${PLIST_PATH}"
plutil -lint "${PLIST_PATH}"

if launchctl list "${LABEL}" >/dev/null 2>&1; then
  launchctl unload "${PLIST_PATH}" >/dev/null 2>&1 || true
fi
launchctl load "${PLIST_PATH}"

echo "Installed ${LABEL}"
echo "Schedule: Sunday 4:00pm local Mac time"
echo "Command: ${COMMAND_STRING}"
echo "Plist: ${PLIST_PATH}"
echo "Stdout: ${STDOUT_LOG}"
echo "Stderr: ${STDERR_LOG}"
