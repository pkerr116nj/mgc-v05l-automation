#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

APP_BUNDLE="${REPO_ROOT}/desktop/release/local/MGC Operator.app"
APP_EXECUTABLE="${APP_BUNDLE}/Contents/MacOS/MGC Operator"
HOST_LOG="${REPO_ROOT}/outputs/operator_dashboard/runtime/supervised_paper_host_autostart.log"
STATUS_FILE="${REPO_ROOT}/outputs/operator_dashboard/runtime/headless_supervised_paper_status.json"
SINGLETON_LOCK_FILE="${HOME}/Library/Application Support/mgc-operator-desktop/SingletonLock"
DIRECT_EXEC_DIAGNOSTIC="${MGC_DESKTOP_DIRECT_EXEC_DIAGNOSTIC:-0}"

ensure_local_bundle() {
  if [[ -x "${APP_EXECUTABLE}" ]]; then
    return 0
  fi
  node "${REPO_ROOT}/desktop/scripts/package_local_app.js"
}

host_usable() {
  local refreshed_status
  refreshed_status="$(mktemp)"
  if bash "${SCRIPT_DIR}/show_headless_supervised_paper_status.sh" --output "${STATUS_FILE}" > "${refreshed_status}" 2>/dev/null; then
    if "${PYTHON_BIN}" - <<'PY' "${refreshed_status}"
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    raise SystemExit(1)
raise SystemExit(0 if payload.get("app_usable_for_supervised_paper") is True else 1)
PY
    then
      rm -f "${refreshed_status}"
      return 0
    fi
  fi
  rm -f "${refreshed_status}"
  "${PYTHON_BIN}" - <<'PY' "${STATUS_FILE}"
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    raise SystemExit(1)
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    raise SystemExit(1)
raise SystemExit(0 if payload.get("app_usable_for_supervised_paper") is True else 1)
PY
}

ensure_host_usable() {
  if host_usable; then
    return 0
  fi
  if ! bash "${SCRIPT_DIR}/run_headless_supervised_paper_service.sh" --wait-timeout-seconds 120 > "${HOST_LOG}" 2>&1; then
    cat "${HOST_LOG}" >&2 || true
    echo "Supervised paper host did not reach a usable state." >&2
    echo "Check: ${STATUS_FILE}" >&2
    exit 1
  fi
}

wait_for_desktop_quiet() {
  local timeout_seconds="${1:-15}"
  local started_at
  started_at="$(date +%s)"
  while true; do
    local app_pid=""
    app_pid="$(
      ps -axo pid=,command= | while read -r pid command; do
        if [[ "${command}" == "${APP_EXECUTABLE}"* ]]; then
          printf '%s\n' "${pid}"
        fi
      done
    )"
    if [[ -z "${app_pid}" ]] && [[ ! -e "${SINGLETON_LOCK_FILE}" ]]; then
      return 0
    fi
    if (( $(date +%s) - started_at >= timeout_seconds )); then
      echo "Desktop singleton state did not clear within ${timeout_seconds}s." >&2
      echo "Executable: ${APP_EXECUTABLE}" >&2
      exit 1
    fi
    sleep 1
  done
}

ensure_local_bundle
ensure_host_usable
wait_for_desktop_quiet 15

unset CODEX_SANDBOX
unset CODEX_SHELL

if [[ "${DIRECT_EXEC_DIAGNOSTIC}" == "1" ]]; then
  exec "${APP_EXECUTABLE}" --mgc-repo-root="${REPO_ROOT}" "$@"
fi

exec open -n "${APP_BUNDLE}" --args --mgc-repo-root="${REPO_ROOT}" "$@"
