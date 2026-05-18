#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

DEFAULT_RUNTIME_DIR="${REPO_ROOT}/outputs/operator_dashboard/runtime"
DEFAULT_STATUS_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_status.json"
DEFAULT_MARKDOWN_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_status.md"
DEFAULT_CANONICAL_READINESS_FILE="${DEFAULT_RUNTIME_DIR}/latest_canonical_readiness.json"
DEFAULT_CANONICAL_READINESS_SUMMARY_FILE="${DEFAULT_RUNTIME_DIR}/latest_canonical_readiness_summary.json"
DEFAULT_STARTUP_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_service_startup.json"
DEFAULT_MANAGER_PID_FILE="${DEFAULT_RUNTIME_DIR}/operator_dashboard_manager.pid"
DEFAULT_MANAGER_LOG_FILE="${DEFAULT_RUNTIME_DIR}/operator_dashboard_manager.log"
DEFAULT_DASHBOARD_PID_FILE="${DEFAULT_RUNTIME_DIR}/operator_dashboard.pid"
DEFAULT_PAPER_PID_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid"
DEFAULT_PAPER_LOG_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.log"
DEFAULT_DASHBOARD_URL="${MGC_OPERATOR_DASHBOARD_URL:-http://127.0.0.1:8790/}"
SERVICE_HOST_AUTOSTART_BRIDGE_SUPERVISOR="${MGC_SERVICE_HOST_AUTOSTART_RESEARCH_RUNTIME_BRIDGE_SUPERVISOR:-1}"
DEFAULT_HEADLESS_PAPER_CONFIG_PATHS=(
  "${REPO_ROOT}/config/base.yaml"
  "${REPO_ROOT}/config/live.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine.yaml"
  "${REPO_ROOT}/config/headless_supervised_paper_runtime.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper.yaml"
)

WAIT_TIMEOUT_SECONDS=120
POLL_INTERVAL_SECONDS=3
STATUS_FILE="${DEFAULT_STATUS_FILE}"
MARKDOWN_FILE="${DEFAULT_MARKDOWN_FILE}"
CANONICAL_READINESS_FILE="${DEFAULT_CANONICAL_READINESS_FILE}"
CANONICAL_READINESS_SUMMARY_FILE="${DEFAULT_CANONICAL_READINESS_SUMMARY_FILE}"
STARTUP_FILE="${DEFAULT_STARTUP_FILE}"
MANAGER_PID_FILE="${DEFAULT_MANAGER_PID_FILE}"
MANAGER_LOG_FILE="${DEFAULT_MANAGER_LOG_FILE}"
DASHBOARD_PID_FILE="${DEFAULT_DASHBOARD_PID_FILE}"
PAPER_PID_FILE="${DEFAULT_PAPER_PID_FILE}"
PAPER_LOG_FILE="${DEFAULT_PAPER_LOG_FILE}"
DASHBOARD_URL="${DEFAULT_DASHBOARD_URL}"
START_PAPER=1
START_DASHBOARD=1

HEADLESS_PAPER_CONFIG_PATHS="${MGC_HEADLESS_SUPERVISED_PAPER_CONFIG_PATHS:-${MGC_PROBATIONARY_PAPER_CONFIG_PATHS:-}}"
if [[ -z "${HEADLESS_PAPER_CONFIG_PATHS}" ]]; then
  HEADLESS_PAPER_CONFIG_PATHS="$(IFS=:; printf '%s' "${DEFAULT_HEADLESS_PAPER_CONFIG_PATHS[*]}")"
fi

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
    --canonical-readiness-output)
      CANONICAL_READINESS_FILE="$2"
      shift 2
      ;;
    --canonical-readiness-output=*)
      CANONICAL_READINESS_FILE="${1#*=}"
      shift
      ;;
    --canonical-readiness-summary-output)
      CANONICAL_READINESS_SUMMARY_FILE="$2"
      shift 2
      ;;
    --canonical-readiness-summary-output=*)
      CANONICAL_READINESS_SUMMARY_FILE="${1#*=}"
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
ensure_dir "$(dirname "${CANONICAL_READINESS_FILE}")"
ensure_dir "$(dirname "${CANONICAL_READINESS_SUMMARY_FILE}")"
ensure_dir "$(dirname "${STARTUP_FILE}")"
ensure_dir "$(dirname "${MANAGER_PID_FILE}")"
ensure_dir "$(dirname "${MANAGER_LOG_FILE}")"
ensure_dir "$(dirname "${DASHBOARD_PID_FILE}")"
ensure_dir "$(dirname "${PAPER_LOG_FILE}")"

read_pid_file() {
  local pid_file="$1"
  if [[ ! -f "${pid_file}" ]]; then
    return 1
  fi
  tr -d '[:space:]' < "${pid_file}" 2>/dev/null
}

pid_file_alive() {
  local pid_file="$1"
  local pid
  pid="$(read_pid_file "${pid_file}" || true)"
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

launchctl_submit_available() {
  command -v launchctl >/dev/null 2>&1
}

screen_available() {
  command -v screen >/dev/null 2>&1
}

screen_session_name() {
  local suffix="$1"
  printf 'mgc_%s_%s_%s' "${suffix}" "$(date +%Y%m%d%H%M%S)" "$$"
}

launch_screen_paper_runtime() {
  local session_name
  session_name="$(screen_session_name "paper_runtime")"
  printf '%s\n' "${session_name}" > "${PAPER_PID_FILE}.screen_session"
  MGC_HEADLESS_PAPER_PID_FILE="${PAPER_PID_FILE}" \
    MGC_HEADLESS_PAPER_LOG_FILE="${PAPER_LOG_FILE}" \
    MGC_HEADLESS_SCRIPT_DIR="${SCRIPT_DIR}" \
    MGC_PROBATIONARY_PAPER_CONFIG_PATHS="${HEADLESS_PAPER_CONFIG_PATHS}" \
    screen -dmS "${session_name}" /bin/zsh -lc \
    'echo "$$" > "${MGC_HEADLESS_PAPER_PID_FILE}"; exec bash "${MGC_HEADLESS_SCRIPT_DIR}/run_probationary_paper_soak.sh" >> "${MGC_HEADLESS_PAPER_LOG_FILE}" 2>&1'
}

launch_screen_dashboard_manager() {
  local session_name
  session_name="$(screen_session_name "dashboard")"
  printf '%s\n' "${session_name}" > "${MANAGER_PID_FILE}.screen_session"
  MGC_HEADLESS_MANAGER_PID_FILE="${MANAGER_PID_FILE}" \
    MGC_HEADLESS_MANAGER_LOG_FILE="${MANAGER_LOG_FILE}" \
    MGC_HEADLESS_SCRIPT_DIR="${SCRIPT_DIR}" \
    screen -dmS "${session_name}" /bin/zsh -lc \
    'echo "$$" > "${MGC_HEADLESS_MANAGER_PID_FILE}"; exec bash "${MGC_HEADLESS_SCRIPT_DIR}/run_operator_dashboard.sh" --no-open-browser >> "${MGC_HEADLESS_MANAGER_LOG_FILE}" 2>&1'
}

launch_detached_paper_runtime() {
  local label="com.mgc-v05l.headless-supervised-paper.runtime.$(date +%Y%m%d%H%M%S).$$"
  printf '%s\n' "${label}" > "${PAPER_PID_FILE}.launchctl_label"
  launchctl submit -l "${label}" -- /usr/bin/env \
    MGC_HEADLESS_PAPER_PID_FILE="${PAPER_PID_FILE}" \
    MGC_HEADLESS_PAPER_LOG_FILE="${PAPER_LOG_FILE}" \
    MGC_HEADLESS_SCRIPT_DIR="${SCRIPT_DIR}" \
    MGC_PROBATIONARY_PAPER_CONFIG_PATHS="${HEADLESS_PAPER_CONFIG_PATHS}" \
    /bin/zsh -lc 'echo "$$" > "${MGC_HEADLESS_PAPER_PID_FILE}"; exec bash "${MGC_HEADLESS_SCRIPT_DIR}/run_probationary_paper_soak.sh" >> "${MGC_HEADLESS_PAPER_LOG_FILE}" 2>&1'
}

launch_detached_dashboard_manager() {
  local label="com.mgc-v05l.headless-supervised-paper.dashboard.$(date +%Y%m%d%H%M%S).$$"
  printf '%s\n' "${label}" > "${MANAGER_PID_FILE}.launchctl_label"
  launchctl submit -l "${label}" -- /usr/bin/env \
    MGC_HEADLESS_MANAGER_PID_FILE="${MANAGER_PID_FILE}" \
    MGC_HEADLESS_MANAGER_LOG_FILE="${MANAGER_LOG_FILE}" \
    MGC_HEADLESS_SCRIPT_DIR="${SCRIPT_DIR}" \
    MGC_SERVICE_HOST_AUTOSTART_RESEARCH_RUNTIME_BRIDGE_SUPERVISOR="${SERVICE_HOST_AUTOSTART_BRIDGE_SUPERVISOR}" \
    /bin/zsh -lc 'echo "$$" > "${MGC_HEADLESS_MANAGER_PID_FILE}"; exec bash "${MGC_HEADLESS_SCRIPT_DIR}/run_operator_dashboard.sh" --no-open-browser >> "${MGC_HEADLESS_MANAGER_LOG_FILE}" 2>&1'
}

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
  if pid_file_alive "${PAPER_PID_FILE}"; then
    return 0
  fi
  if screen_available; then
    launch_screen_paper_runtime
    return 0
  fi
  if launchctl_submit_available; then
    launch_detached_paper_runtime
    return 0
  fi
  local output
  output="$(MGC_PROBATIONARY_PAPER_CONFIG_PATHS="${HEADLESS_PAPER_CONFIG_PATHS}" bash "${SCRIPT_DIR}/run_probationary_paper_soak.sh" --background 2>&1)" && return 0
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
  if pid_file_alive "${DASHBOARD_PID_FILE}"; then
    return 0
  fi
  if pid_file_alive "${MANAGER_PID_FILE}"; then
    return 0
  fi
  if curl -fsS "${DASHBOARD_URL%/}/health" >/dev/null 2>&1; then
    return 0
  fi
  if screen_available; then
    launch_screen_dashboard_manager
    return 0
  fi
  if launchctl_submit_available; then
    launch_detached_dashboard_manager
    return 0
  fi
  MGC_SERVICE_HOST_AUTOSTART_RESEARCH_RUNTIME_BRIDGE_SUPERVISOR="${SERVICE_HOST_AUTOSTART_BRIDGE_SUPERVISOR}" \
    nohup bash "${SCRIPT_DIR}/run_operator_dashboard.sh" --no-open-browser >> "${MANAGER_LOG_FILE}" 2>&1 &
  local manager_pid=$!
  echo "${manager_pid}" > "${MANAGER_PID_FILE}"
}


ready_processes_and_endpoints_alive() {
  if [[ "${START_PAPER}" -eq 1 ]] && ! pid_file_alive "${PAPER_PID_FILE}"; then
    return 1
  fi
  if [[ "${START_DASHBOARD}" -eq 1 ]] && ! pid_file_alive "${DASHBOARD_PID_FILE}"; then
    return 1
  fi
  if [[ "${START_DASHBOARD}" -eq 1 ]] && ! pid_file_alive "${MANAGER_PID_FILE}"; then
    return 1
  fi
  curl -fsS "${DASHBOARD_URL%/}/health" >/dev/null 2>&1 || return 1
  curl -fsS "${DASHBOARD_URL%/}/api/dashboard" >/dev/null 2>&1 || return 1
  return 0
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

canonical_readiness_classification() {
  "${PYTHON_BIN}" - <<'PY' "${CANONICAL_READINESS_SUMMARY_FILE}"
import json
import sys
from pathlib import Path

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
print(payload.get("classification") or "NOT_READY_CONFIG")
PY
}

print_canonical_readiness_summary() {
  local phase="$1"
  "${PYTHON_BIN}" - <<'PY' "${CANONICAL_READINESS_SUMMARY_FILE}" "${phase}" >&2
import json
import sys
from pathlib import Path

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {"classification": "NOT_READY_CONFIG", "blockers": ["canonical_readiness_summary_missing"]}

print(f"canonical_readiness_summary[{sys.argv[2]}]:")
for key in (
    "classification",
    "blockers",
    "warnings",
    "root_match",
    "broker_truth_fresh",
    "reconciliation_state",
    "eligible_lane_count",
    "quarantine_count",
):
    value = payload.get(key)
    if isinstance(value, list):
        value = ",".join(str(item) for item in value) if value else "none"
    print(f"  {key}={value}")
PY
}

refresh_canonical_readiness_for_launch() {
  local phase="$1"
  local tmp_summary
  tmp_summary="${CANONICAL_READINESS_SUMMARY_FILE}.tmp"
  rm -f "${tmp_summary}"
  set +e
  "${PYTHON_BIN}" -m mgc_v05l.app.track_b_canonical_readiness \
    --repo-root "${REPO_ROOT}" \
    --expected-root "${REPO_ROOT}" \
    --output-path "${CANONICAL_READINESS_FILE}" \
    --json > "${tmp_summary}"
  local exit_code=$?
  set -e
  if [[ -s "${tmp_summary}" ]]; then
    mv "${tmp_summary}" "${CANONICAL_READINESS_SUMMARY_FILE}"
  else
    rm -f "${tmp_summary}"
  fi
  print_canonical_readiness_summary "${phase}"
  return "${exit_code}"
}

fail_fast_if_hard_canonical_blocker() {
  local phase="$1"
  local classification
  classification="$(canonical_readiness_classification)"
  case "${classification}" in
    NOT_READY_WRONG_ROOT|NOT_READY_CONFIG|NOT_READY_RECONCILIATION)
      write_startup_summary "BLOCKED" "Canonical readiness ${classification} during ${phase}." "false"
      cat "${STARTUP_FILE}"
      exit 2
      ;;
  esac
}

set +e
refresh_canonical_readiness_for_launch "pre-launch"
set -e
fail_fast_if_hard_canonical_blocker "pre-launch"

if ! start_paper_runtime; then
  write_startup_summary "BLOCKED" "Failed to start the supervised paper runtime." "false"
  exit 1
fi
if ! start_dashboard_manager; then
  write_startup_summary "BLOCKED" "Failed to start the operator dashboard manager." "false"
  exit 1
fi

set +e
refresh_canonical_readiness_for_launch "post-launch"
set -e
fail_fast_if_hard_canonical_blocker "post-launch"

deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
last_reason="Headless supervised paper host is still warming."

while (( SECONDS < deadline )); do
  set +e
  bash "${SCRIPT_DIR}/show_headless_supervised_paper_status.sh" \
    --dashboard-url "${DASHBOARD_URL}" \
    --output "${STATUS_FILE}" \
    --markdown-output "${MARKDOWN_FILE}" \
    --canonical-readiness-output "${CANONICAL_READINESS_FILE}" \
    --canonical-readiness-summary-output "${CANONICAL_READINESS_SUMMARY_FILE}" >/dev/null
  status_exit_code=$?
  set -e
  fail_fast_if_hard_canonical_blocker "status-refresh"
  if [[ "$(read_contract_field "app_usable_for_supervised_paper")" == "true" ]]; then
    if ready_processes_and_endpoints_alive; then
      canonical_state="$(canonical_readiness_classification)"
      if [[ "${canonical_state}" != "READY_SUBMIT_CAPABLE" ]]; then
        write_startup_summary "READY" "Headless supervised paper host is usable for ${canonical_state}." "true"
        cat "${STATUS_FILE}"
        exit 0
      fi
      write_startup_summary "READY" "Headless supervised paper host is READY_SUBMIT_CAPABLE." "true"
      cat "${STATUS_FILE}"
      exit 0
    fi
    last_reason="Headless supervised paper host reported usable before required processes/endpoints were stable."
  fi
  if [[ "${status_exit_code}" -eq 1 ]]; then
    last_reason="Canonical readiness is DEGRADED_NO_SUBMIT."
  elif [[ "${status_exit_code}" -eq 2 ]]; then
    last_reason="Canonical readiness is $(canonical_readiness_classification)."
  fi
  contract_reason="$(read_contract_field "unusable_reason")"
  if [[ -n "${contract_reason}" ]]; then
    last_reason="${contract_reason}"
  fi
  sleep "${POLL_INTERVAL_SECONDS}"
done

write_startup_summary "BLOCKED" "${last_reason}" "false"
cat "${STATUS_FILE}"
exit 1
