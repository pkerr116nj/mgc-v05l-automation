#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

require_schwab_auth_env

DEFAULT_RUNTIME_DIR="${REPO_ROOT}/outputs/operator_dashboard/runtime"
DEFAULT_STATUS_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_status.json"
DEFAULT_MARKDOWN_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_status.md"
DEFAULT_STARTUP_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_service_startup.json"
DEFAULT_MANAGER_PID_FILE="${DEFAULT_RUNTIME_DIR}/operator_dashboard_manager.pid"
DEFAULT_MANAGER_LOG_FILE="${DEFAULT_RUNTIME_DIR}/operator_dashboard_manager.log"
DEFAULT_DASHBOARD_PID_FILE="${DEFAULT_RUNTIME_DIR}/operator_dashboard.pid"
DEFAULT_PAPER_PID_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid"
DEFAULT_DASHBOARD_URL="${MGC_OPERATOR_DASHBOARD_URL:-http://127.0.0.1:8790/}"
SERVICE_HOST_AUTOSTART_BRIDGE_SUPERVISOR="${MGC_SERVICE_HOST_AUTOSTART_RESEARCH_RUNTIME_BRIDGE_SUPERVISOR:-1}"

WAIT_TIMEOUT_SECONDS=120
POLL_INTERVAL_SECONDS=3
STATUS_FILE="${DEFAULT_STATUS_FILE}"
MARKDOWN_FILE="${DEFAULT_MARKDOWN_FILE}"
STARTUP_FILE="${DEFAULT_STARTUP_FILE}"
MANAGER_PID_FILE="${DEFAULT_MANAGER_PID_FILE}"
MANAGER_LOG_FILE="${DEFAULT_MANAGER_LOG_FILE}"
DASHBOARD_PID_FILE="${DEFAULT_DASHBOARD_PID_FILE}"
PAPER_PID_FILE="${DEFAULT_PAPER_PID_FILE}"
DASHBOARD_URL="${DEFAULT_DASHBOARD_URL}"
START_PAPER=1
START_DASHBOARD=1

while (($# > 0)); do
  case "$1" in
    --wait-timeout-seconds)
      WAIT_TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    --wait-timeout-seconds=*)
      WAIT_TIMEOUT_SECONDS="${1#*=}"
      shift
      ;;
    --poll-interval-seconds)
      POLL_INTERVAL_SECONDS="$2"
      shift 2
      ;;
    --poll-interval-seconds=*)
      POLL_INTERVAL_SECONDS="${1#*=}"
      shift
      ;;
    --status-output)
      STATUS_FILE="$2"
      shift 2
      ;;
    --status-output=*)
      STATUS_FILE="${1#*=}"
      shift
      ;;
    --markdown-output)
      MARKDOWN_FILE="$2"
      shift 2
      ;;
    --markdown-output=*)
      MARKDOWN_FILE="${1#*=}"
      shift
      ;;
    --startup-output)
      STARTUP_FILE="$2"
      shift 2
      ;;
    --startup-output=*)
      STARTUP_FILE="${1#*=}"
      shift
      ;;
    --dashboard-url)
      DASHBOARD_URL="$2"
      shift 2
      ;;
    --dashboard-url=*)
      DASHBOARD_URL="${1#*=}"
      shift
      ;;
    --dashboard-pid-file)
      DASHBOARD_PID_FILE="$2"
      shift 2
      ;;
    --dashboard-pid-file=*)
      DASHBOARD_PID_FILE="${1#*=}"
      shift
      ;;
    --no-start-paper)
      START_PAPER=0
      shift
      ;;
    --no-start-dashboard)
      START_DASHBOARD=0
      shift
      ;;
    *)
      echo "Unsupported argument: $1" >&2
      exit 1
      ;;
  esac
done

ensure_dir "$(dirname "${STATUS_FILE}")"
ensure_dir "$(dirname "${MARKDOWN_FILE}")"
ensure_dir "$(dirname "${STARTUP_FILE}")"
ensure_dir "$(dirname "${MANAGER_PID_FILE}")"
ensure_dir "$(dirname "${MANAGER_LOG_FILE}")"
ensure_dir "$(dirname "${DASHBOARD_PID_FILE}")"

write_startup_summary() {
  local startup_state="$1"
  local reason="$2"
  local app_usable="$3"
  "${PYTHON_BIN}" - <<'PY' "${STARTUP_FILE}" "${startup_state}" "${reason}" "${app_usable}" "${STATUS_FILE}" "${MARKDOWN_FILE}" "${DASHBOARD_URL}"
import json
import sys
from datetime import datetime, timezone

path, state, reason, usable, status_path, markdown_path, dashboard_url = sys.argv[1:]
payload = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "mode": "HEADLESS_SUPERVISED_PAPER_SERVICE",
    "startup_state": state,
    "reason": reason,
    "app_usable_for_supervised_paper": usable.lower() == "true",
    "status_artifact": status_path,
    "markdown_artifact": markdown_path,
    "dashboard_url": dashboard_url,
}
with open(path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

start_paper_runtime() {
  if [[ "${START_PAPER}" -ne 1 ]]; then
    return 0
  fi
  if [[ -f "${PAPER_PID_FILE}" ]]; then
    local existing_pid
    existing_pid="$(cat "${PAPER_PID_FILE}" 2>/dev/null || true)"
    if [[ -n "${existing_pid}" ]] && kill -0 "${existing_pid}" 2>/dev/null; then
      return 0
    fi
  fi
  local output
  output="$(bash "${SCRIPT_DIR}/run_probationary_paper_soak.sh" --background 2>&1)" && return 0
  if [[ "${output}" == *"already running"* ]]; then
    return 0
  fi
  echo "${output}" >&2
  return 1
}

start_dashboard_manager() {
  if [[ "${START_DASHBOARD}" -ne 1 ]]; then
    return 0
  fi
  local attempt=1
  while (( attempt <= 5 )); do
    if curl -fsS "${DASHBOARD_URL%/}/health" >/dev/null 2>&1; then
      return 0
    fi
    attempt=$((attempt + 1))
    if (( attempt <= 5 )); then
      sleep 1
    fi
  done
  if [[ -f "${DASHBOARD_PID_FILE}" ]]; then
    local dashboard_pid
    dashboard_pid="$(cat "${DASHBOARD_PID_FILE}" 2>/dev/null || true)"
    if [[ -n "${dashboard_pid}" ]] && kill -0 "${dashboard_pid}" 2>/dev/null; then
      return 0
    fi
  fi
  if [[ -f "${MANAGER_PID_FILE}" ]]; then
    local existing_pid
    existing_pid="$(cat "${MANAGER_PID_FILE}" 2>/dev/null || true)"
    if [[ -n "${existing_pid}" ]] && kill -0 "${existing_pid}" 2>/dev/null; then
      return 0
    fi
  fi
  if curl -fsS "${DASHBOARD_URL%/}/health" >/dev/null 2>&1; then
    return 0
  fi
  MGC_SERVICE_HOST_AUTOSTART_RESEARCH_RUNTIME_BRIDGE_SUPERVISOR="${SERVICE_HOST_AUTOSTART_BRIDGE_SUPERVISOR}" \
    nohup bash "${SCRIPT_DIR}/run_operator_dashboard.sh" --no-open-browser >> "${MANAGER_LOG_FILE}" 2>&1 &
  local manager_pid=$!
  echo "${manager_pid}" > "${MANAGER_PID_FILE}"
}

read_contract_field() {
  local field="$1"
  "${PYTHON_BIN}" - <<'PY' "${STATUS_FILE}" "${field}"
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
value = payload
for part in sys.argv[2].split("."):
    if isinstance(value, dict):
        value = value.get(part)
    else:
        value = None
        break
if isinstance(value, bool):
    print("true" if value else "false")
elif value is None:
    print("")
else:
    print(value)
PY
}

if ! start_paper_runtime; then
  write_startup_summary "BLOCKED" "Failed to start the supervised paper runtime." "false"
  exit 1
fi
if ! start_dashboard_manager; then
  write_startup_summary "BLOCKED" "Failed to start the operator dashboard manager." "false"
  exit 1
fi

deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
last_reason="Headless supervised paper host is still warming."

while (( SECONDS < deadline )); do
  bash "${SCRIPT_DIR}/show_headless_supervised_paper_status.sh" \
    --dashboard-url "${DASHBOARD_URL}" \
    --output "${STATUS_FILE}" \
    --markdown-output "${MARKDOWN_FILE}" >/dev/null
  if [[ "$(read_contract_field "app_usable_for_supervised_paper")" == "true" ]]; then
    write_startup_summary "READY" "Headless supervised paper host is usable." "true"
    cat "${STATUS_FILE}"
    exit 0
  fi
  last_reason="$(read_contract_field "unusable_reason")"
  sleep "${POLL_INTERVAL_SECONDS}"
done

write_startup_summary "BLOCKED" "${last_reason}" "false"
cat "${STATUS_FILE}"
exit 1
