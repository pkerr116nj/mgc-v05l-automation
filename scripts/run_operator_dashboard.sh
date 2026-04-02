#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

DEFAULT_HOST="${OPERATOR_DASHBOARD_HOST:-127.0.0.1}"
DEFAULT_PORT="${OPERATOR_DASHBOARD_PORT:-8790}"
DEFAULT_RUNTIME_DIR="${REPO_ROOT}/outputs/operator_dashboard/runtime"
DEFAULT_LOG_FILE="${OPERATOR_DASHBOARD_LOG_FILE:-${DEFAULT_RUNTIME_DIR}/operator_dashboard.log}"
DEFAULT_INFO_FILE="${OPERATOR_DASHBOARD_INFO_FILE:-${DEFAULT_RUNTIME_DIR}/operator_dashboard.json}"
DEFAULT_PID_FILE="${OPERATOR_DASHBOARD_PID_FILE:-${DEFAULT_RUNTIME_DIR}/operator_dashboard.pid}"
INTERNAL_INFO_FILE="${TMPDIR:-/tmp}/mgc_v05l_operator_dashboard_launch.json"

HOST="${DEFAULT_HOST}"
PORT="${DEFAULT_PORT}"
LOG_FILE="${DEFAULT_LOG_FILE}"
INFO_FILE="${DEFAULT_INFO_FILE}"
PID_FILE="${DEFAULT_PID_FILE}"
PRINT_INFO_FILE=1
OPEN_BROWSER=0
VERIFY_DASHBOARD_API=0
ALLOW_PORT_FALLBACK=0
PASSTHROUGH_ARGS=()
DASHBOARD_PID=""

while (($# > 0)); do
  case "$1" in
    --host)
      HOST="$2"
      shift 2
      ;;
    --host=*)
      HOST="${1#*=}"
      shift
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --port=*)
      PORT="${1#*=}"
      shift
      ;;
    --log-file)
      LOG_FILE="$2"
      shift 2
      ;;
    --log-file=*)
      LOG_FILE="${1#*=}"
      shift
      ;;
    --pid-file)
      PID_FILE="$2"
      shift 2
      ;;
    --pid-file=*)
      PID_FILE="${1#*=}"
      shift
      ;;
    --info-file)
      INFO_FILE="$2"
      PRINT_INFO_FILE=1
      shift 2
      ;;
    --info-file=*)
      INFO_FILE="${1#*=}"
      PRINT_INFO_FILE=1
      shift
      ;;
    --no-info-file)
      INFO_FILE="${INTERNAL_INFO_FILE}"
      PRINT_INFO_FILE=0
      shift
      ;;
    --open-browser)
      OPEN_BROWSER=1
      shift
      ;;
    --no-open-browser)
      OPEN_BROWSER=0
      shift
      ;;
    --verify-dashboard-api)
      VERIFY_DASHBOARD_API=1
      shift
      ;;
    --no-verify-dashboard-api)
      VERIFY_DASHBOARD_API=0
      shift
      ;;
    --allow-port-fallback)
      ALLOW_PORT_FALLBACK=1
      shift
      ;;
    --no-allow-port-fallback)
      ALLOW_PORT_FALLBACK=0
      shift
      ;;
    *)
      PASSTHROUGH_ARGS+=("$1")
      shift
      ;;
  esac
done

ensure_dir "${DEFAULT_RUNTIME_DIR}"
ensure_dir "$(dirname "${LOG_FILE}")"
ensure_dir "$(dirname "${INFO_FILE}")"
ensure_dir "$(dirname "${PID_FILE}")"

emit_bootstrap_warning() {
  local label="$1"
  local message="$2"
  echo "DASHBOARD_BOOTSTRAP_WARNING ${label}: ${message}" >&2
}

if [[ "${MGC_BOOTSTRAP_REPLAY_DB_STATUS:-ready}" == "missing" ]]; then
  emit_bootstrap_warning \
    "replay_db_missing" \
    "Replay DB is missing at ${MGC_BOOTSTRAP_REPLAY_DB_PATH:-${DB_PATH}}. Dashboard will start in reduced mode; replay/research-backed panels will be unavailable. ${MGC_BOOTSTRAP_REPLAY_DB_NEXT_ACTION:-Run bash scripts/backfill_schwab_1m_history.sh.}"
fi

if [[ "${MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_STATUS:-ready}" == "missing" ]]; then
  emit_bootstrap_warning \
    "schwab_auth_env_missing" \
    "Schwab auth env is missing (${MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_MISSING_NAMES:-SCHWAB_APP_KEY SCHWAB_APP_SECRET SCHWAB_CALLBACK_URL}). Dashboard will start in reduced mode; Schwab-backed bootstrap paths will be unavailable. ${MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_NEXT_ACTION:-Export SCHWAB_APP_KEY, SCHWAB_APP_SECRET, and SCHWAB_CALLBACK_URL.}"
fi

cleanup() {
  if [[ -n "${DASHBOARD_PID}" ]] && ps -p "${DASHBOARD_PID}" >/dev/null 2>&1; then
    kill -TERM "${DASHBOARD_PID}" 2>/dev/null || true
  fi
}

trap cleanup INT TERM

emit_startup_marker() {
  local key="$1"
  local value="$2"
  echo "STARTUP_${key}=${value}" >&2
}

emit_startup_failure() {
  local kind="$1"
  local message="$2"
  local next_action="$3"
  emit_startup_marker "FAILURE_KIND" "${kind}"
  emit_startup_marker "NEXT_ACTION" "${next_action}"
  echo "${message}" >&2
}

health_check() {
  local health_url="$1"
  "${PYTHON_BIN}" -c 'import sys, urllib.request; urllib.request.urlopen(sys.argv[1], timeout=1).read()' "${health_url}" >/dev/null 2>&1
}

dashboard_api_check() {
  local dashboard_api_url="$1"
  "${PYTHON_BIN}" -c 'import json, sys, urllib.request; payload=json.loads(urllib.request.urlopen(sys.argv[1], timeout=15).read().decode("utf-8")); assert isinstance(payload, dict) and "operator_surface" in payload and "dashboard_meta" in payload' "${dashboard_api_url}" >/dev/null 2>&1
}

read_health_field() {
  local health_url="$1"
  local field="$2"
  "${PYTHON_BIN}" -c 'import json, sys, urllib.request; print(json.loads(urllib.request.urlopen(sys.argv[1], timeout=2).read().decode("utf-8"))[sys.argv[2]])' "${health_url}" "${field}"
}

read_local_build_stamp() {
  "${PYTHON_BIN}" - <<'PY'
from mgc_v05l.app.operator_dashboard import dashboard_build_stamp
print(dashboard_build_stamp())
PY
}

open_browser() {
  local dashboard_url="$1"
  open "${dashboard_url}" >/dev/null 2>&1
}

read_info_field() {
  local info_file="$1"
  local field="$2"
  "${PYTHON_BIN}" -c 'import json, sys; print(json.load(open(sys.argv[1], "r", encoding="utf-8"))[sys.argv[2]])' "${info_file}" "${field}"
}

listener_pid_for_port() {
  local port="$1"
  lsof -nP -iTCP:"${port}" -sTCP:LISTEN -t 2>/dev/null | head -n 1
}

read_process_command() {
  local pid="$1"
  ps -p "${pid}" -o command= 2>/dev/null || true
}

is_recoverable_local_dashboard_listener() {
  local command="$1"
  local normalized
  normalized="$(printf '%s' "${command}" | tr '[:upper:]' '[:lower:]')"
  if [[ -z "${normalized}" ]]; then
    return 1
  fi
  [[ "${normalized}" == *"mgc_v05l.app.main"* && "${normalized}" == *"operator-dashboard"* ]] || \
    [[ "${normalized}" == *"python"* && "${normalized}" == *"operator_dashboard"* ]]
}

dashboard_ready_check() {
  local dashboard_url="$1"
  local health_url="${dashboard_url%/}/health"
  local dashboard_api_url="${dashboard_url%/}/api/dashboard"
  if ! health_check "${health_url}"; then
    return 1
  fi
  dashboard_api_check "${dashboard_api_url}"
}

report_success() {
  local dashboard_url="$1"
  local bound_host="$2"
  local bound_port="$3"
  local pid="$4"
  local build_stamp="$5"
  local started_at="$6"
  local reused="${7:-0}"
  local health_status="${8:-unknown}"
  local health_ready="${9:-false}"
  if [[ "${health_ready}" == "True" ]] || [[ "${health_ready}" == "true" ]]; then
    echo "Operator dashboard ready."
  else
    echo "Operator dashboard service is live."
  fi
  if [[ "${reused}" -eq 1 ]]; then
    echo "Reusing existing dashboard instance."
  fi
  echo "Chosen URL: ${dashboard_url}"
  echo "Bound host/port: ${bound_host}:${bound_port}"
  if [[ "${ALLOW_PORT_FALLBACK}" -eq 1 ]]; then
    echo "Port policy: explicit fallback enabled"
  else
    echo "Port policy: fixed preferred port"
  fi
  echo "PID: ${pid}"
  echo "Build stamp: ${build_stamp}"
  echo "Server started: ${started_at}"
  echo "Health status: ${health_status}"
  echo "Health ready: ${health_ready}"
  echo "Health endpoint: ${dashboard_url%/}/health"
  echo "Dashboard API: ${dashboard_url%/}/api/dashboard"
  echo "Log file: ${LOG_FILE}"
  echo "PID file: ${PID_FILE}"
  if [[ ${PRINT_INFO_FILE} -eq 1 ]]; then
    echo "Info file: ${INFO_FILE}"
  fi
}

stop_dashboard_process() {
  local pid="$1"
  if [[ -z "${pid}" ]]; then
    return 1
  fi
  if ! ps -p "${pid}" >/dev/null 2>&1; then
    return 0
  fi
  kill -TERM "${pid}" 2>/dev/null || return 1
  for _ in $(seq 1 30); do
    if ! ps -p "${pid}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

LOCAL_BUILD_STAMP="$(read_local_build_stamp)"

if [[ -f "${INFO_FILE}" ]]; then
  DASHBOARD_URL_EXISTING="$(read_info_field "${INFO_FILE}" url 2>/dev/null || true)"
  BOUND_HOST_EXISTING="$(read_info_field "${INFO_FILE}" host 2>/dev/null || true)"
  BOUND_PORT_EXISTING="$(read_info_field "${INFO_FILE}" port 2>/dev/null || true)"
  PID_EXISTING=""
  if [[ -f "${PID_FILE}" ]]; then
    PID_EXISTING="$(cat "${PID_FILE}" 2>/dev/null || true)"
  fi
  if [[ -n "${DASHBOARD_URL_EXISTING}" ]] && health_check "${DASHBOARD_URL_EXISTING%/}/health"; then
    EXISTING_BUILD="$(read_health_field "${DASHBOARD_URL_EXISTING%/}/health" build_stamp 2>/dev/null || true)"
    EXISTING_STARTED="$(read_health_field "${DASHBOARD_URL_EXISTING%/}/health" started_at 2>/dev/null || true)"
    EXISTING_HEALTH_STATUS="$(read_health_field "${DASHBOARD_URL_EXISTING%/}/health" status 2>/dev/null || true)"
    EXISTING_HEALTH_READY="$(read_health_field "${DASHBOARD_URL_EXISTING%/}/health" ready 2>/dev/null || true)"
    if [[ -n "${EXISTING_BUILD}" ]] && [[ "${EXISTING_BUILD}" != "${LOCAL_BUILD_STAMP}" ]]; then
      emit_startup_marker "HEALTH_REACHABLE" "1"
      emit_startup_marker "BUILD_MISMATCH" "1"
      echo "Existing dashboard build mismatch: running ${EXISTING_BUILD}, local ${LOCAL_BUILD_STAMP}. Restarting." >&2
      if [[ -n "${PID_EXISTING}" ]] && stop_dashboard_process "${PID_EXISTING}"; then
        rm -f "${PID_FILE}" "${INFO_FILE}"
      else
        emit_startup_failure \
          "build_mismatch" \
          "Could not stop the old dashboard instance cleanly. PID: ${PID_EXISTING:-unknown}" \
          "Stop the old local dashboard instance, then retry Start/Restart Dashboard/API."
        exit 1
      fi
    elif [[ ${VERIFY_DASHBOARD_API} -eq 1 ]] && ! dashboard_api_check "${DASHBOARD_URL_EXISTING%/}/api/dashboard"; then
      emit_startup_marker "HEALTH_REACHABLE" "1"
      emit_startup_marker "DASHBOARD_API_TIMED_OUT" "1"
      echo "Existing dashboard is live but /api/dashboard is not ready yet. Restarting." >&2
      if [[ -n "${PID_EXISTING}" ]] && stop_dashboard_process "${PID_EXISTING}"; then
        rm -f "${PID_FILE}" "${INFO_FILE}"
      else
        emit_startup_failure \
          "stale_dashboard_instance" \
          "Could not stop the old dashboard instance cleanly. PID: ${PID_EXISTING:-unknown}" \
          "Stop the stale local dashboard instance, then retry Start/Restart Dashboard/API."
        exit 1
      fi
    else
      report_success "${DASHBOARD_URL_EXISTING}" "${BOUND_HOST_EXISTING:-${HOST}}" "${BOUND_PORT_EXISTING:-${PORT}}" "${PID_EXISTING:-unknown}" "${EXISTING_BUILD:-${LOCAL_BUILD_STAMP}}" "${EXISTING_STARTED:-unknown}" 1 "${EXISTING_HEALTH_STATUS:-unknown}" "${EXISTING_HEALTH_READY:-false}"
      if [[ ${OPEN_BROWSER} -eq 1 ]]; then
        if ! open_browser "${DASHBOARD_URL_EXISTING}"; then
          echo "Dashboard is healthy, but browser auto-open failed. Open this URL manually: ${DASHBOARD_URL_EXISTING}" >&2
        fi
      fi
      exit 0
    fi
  fi
  if [[ -n "${PID_EXISTING}" ]] && ps -p "${PID_EXISTING}" >/dev/null 2>&1; then
    emit_startup_marker "STALE_LISTENER_DETECTED" "1"
    echo "Existing dashboard process is not ready. Restarting PID ${PID_EXISTING}." >&2
    if stop_dashboard_process "${PID_EXISTING}"; then
      rm -f "${PID_FILE}" "${INFO_FILE}"
    else
      emit_startup_failure \
        "stale_dashboard_instance" \
        "Could not stop the stale dashboard process cleanly. PID: ${PID_EXISTING}" \
        "Stop the stale local dashboard process, then retry Start/Restart Dashboard/API."
      exit 1
    fi
  fi
fi

rm -f "${INFO_FILE}"

LISTENER_PID="$(listener_pid_for_port "${PORT}" || true)"
if [[ -n "${LISTENER_PID}" ]]; then
  LISTENER_COMMAND="$(read_process_command "${LISTENER_PID}")"
  emit_startup_marker "STALE_LISTENER_DETECTED" "1"
  emit_startup_marker "PORT_CONFLICT_DETECTED" "1"
  if [[ -n "${LISTENER_COMMAND}" ]] && is_recoverable_local_dashboard_listener "${LISTENER_COMMAND}"; then
    echo "Detected recoverable local dashboard listener on ${HOST}:${PORT}; stopping PID ${LISTENER_PID} automatically." >&2
    if stop_dashboard_process "${LISTENER_PID}"; then
      sleep 0.5
      LISTENER_PID="$(listener_pid_for_port "${PORT}" || true)"
    else
      emit_startup_failure \
        "stale_listener_conflict" \
        "Callback listener could not be cleared on ${HOST}:${PORT}. Listener PID ${LISTENER_PID} is still bound." \
        "Stop the stale local dashboard listener, then retry Start/Restart Dashboard/API."
      if [[ -n "${LISTENER_COMMAND}" ]]; then
        echo "Listener command: ${LISTENER_COMMAND}" >&2
      fi
      exit 1
    fi
  fi
  if [[ -n "${LISTENER_PID}" ]]; then
    echo "Dashboard port conflict on ${HOST}:${PORT}." >&2
    echo "Listener PID: ${LISTENER_PID}" >&2
    if [[ -n "${LISTENER_COMMAND}" ]]; then
      echo "Listener command: ${LISTENER_COMMAND}" >&2
    fi
    emit_startup_failure \
      "stale_listener_conflict" \
      "A listener is already bound on ${HOST}:${PORT} and could not be auto-cleared safely." \
      "Stop the conflicting listener or choose a different configured port before retrying Dashboard/API start."
    exit 1
  fi
fi

echo "Launching operator dashboard."
echo "Preferred host/port: ${HOST}:${PORT}"
echo "Local build stamp: ${LOCAL_BUILD_STAMP}"
echo "Dashboard log file: ${LOG_FILE}"
echo "Dashboard PID file: ${PID_FILE}"
if [[ ${PRINT_INFO_FILE} -eq 1 ]]; then
  echo "Dashboard info file: ${INFO_FILE}"
fi

DASHBOARD_CMD=(
  "${PYTHON_BIN}"
  -m
  mgc_v05l.app.main
  operator-dashboard
  --host "${HOST}"
  --port "${PORT}"
  --info-file "${INFO_FILE}"
)

if [[ "${ALLOW_PORT_FALLBACK}" -eq 1 ]]; then
  DASHBOARD_CMD+=(--allow-port-fallback)
fi

if ((${#PASSTHROUGH_ARGS[@]} > 0)); then
  DASHBOARD_CMD+=("${PASSTHROUGH_ARGS[@]}")
fi

"${DASHBOARD_CMD[@]}" > "${LOG_FILE}" 2>&1 &
DASHBOARD_PID=$!
echo "${DASHBOARD_PID}" > "${PID_FILE}"

for _ in $(seq 1 150); do
  if ! ps -p "${DASHBOARD_PID}" >/dev/null 2>&1; then
    emit_startup_failure \
      "early_process_exit" \
      "Dashboard failed to start: server process exited early." \
      "Review the backend log tail, fix the startup error, then retry Start/Restart Dashboard/API."
    rm -f "${PID_FILE}"
    echo "Log tail:" >&2
    tail -n 40 "${LOG_FILE}" >&2 || true
    exit 1
  fi

  if [[ -f "${INFO_FILE}" ]]; then
    DASHBOARD_URL="$(read_info_field "${INFO_FILE}" url)"
    BOUND_HOST="$(read_info_field "${INFO_FILE}" host)"
    BOUND_PORT="$(read_info_field "${INFO_FILE}" port)"
    if health_check "${DASHBOARD_URL%/}/health"; then
      emit_startup_marker "HEALTH_REACHABLE" "1"
      RUNNING_BUILD="$(read_health_field "${DASHBOARD_URL%/}/health" build_stamp 2>/dev/null || true)"
      RUNNING_STARTED="$(read_health_field "${DASHBOARD_URL%/}/health" started_at 2>/dev/null || true)"
      RUNNING_HEALTH_STATUS="$(read_health_field "${DASHBOARD_URL%/}/health" status 2>/dev/null || true)"
      RUNNING_HEALTH_READY="$(read_health_field "${DASHBOARD_URL%/}/health" ready 2>/dev/null || true)"
      if [[ -n "${RUNNING_BUILD}" ]] && [[ "${RUNNING_BUILD}" != "${LOCAL_BUILD_STAMP}" ]]; then
        emit_startup_marker "BUILD_MISMATCH" "1"
        emit_startup_failure \
          "build_mismatch" \
          "Dashboard health check returned a build mismatch: running ${RUNNING_BUILD}, local ${LOCAL_BUILD_STAMP}." \
          "Stop the stale local dashboard instance, then retry Start/Restart Dashboard/API."
        kill -TERM "${DASHBOARD_PID}" 2>/dev/null || true
        rm -f "${PID_FILE}"
        exit 1
      fi
      if [[ ${VERIFY_DASHBOARD_API} -eq 1 ]] && ! dashboard_api_check "${DASHBOARD_URL%/}/api/dashboard"; then
        emit_startup_marker "DASHBOARD_API_TIMED_OUT" "1"
        sleep 0.2
        continue
      fi
      report_success "${DASHBOARD_URL}" "${BOUND_HOST}" "${BOUND_PORT}" "${DASHBOARD_PID}" "${RUNNING_BUILD:-${LOCAL_BUILD_STAMP}}" "${RUNNING_STARTED:-unknown}" 0 "${RUNNING_HEALTH_STATUS:-unknown}" "${RUNNING_HEALTH_READY:-false}"
      if [[ ${OPEN_BROWSER} -eq 1 ]]; then
        if ! open_browser "${DASHBOARD_URL}"; then
          echo "Dashboard is healthy, but browser auto-open failed. Open this URL manually: ${DASHBOARD_URL}" >&2
        fi
      fi
      wait "${DASHBOARD_PID}"
      exit $?
    fi
  fi
  sleep 0.2
done

if [[ -f "${INFO_FILE}" ]]; then
  DASHBOARD_URL="$(read_info_field "${INFO_FILE}" url 2>/dev/null || true)"
  if [[ -n "${DASHBOARD_URL}" ]] && health_check "${DASHBOARD_URL%/}/health"; then
    emit_startup_marker "HEALTH_REACHABLE" "1"
    emit_startup_marker "DASHBOARD_API_TIMED_OUT" "1"
    emit_startup_failure \
      "dashboard_api_not_ready" \
      "Dashboard health is up, but /api/dashboard never became ready before timeout." \
      "Review the backend log and fix the blocking backend condition before retrying Dashboard/API start."
  else
    emit_startup_failure \
      "permission_or_bind_failure" \
      "Dashboard failed to become healthy before timeout." \
      "Review the backend log for bind, permission, or environment errors, then retry Dashboard/API start."
  fi
else
  emit_startup_failure \
    "permission_or_bind_failure" \
    "Dashboard failed to become healthy before timeout." \
    "Review the backend log for bind, permission, or environment errors, then retry Dashboard/API start."
fi
if ps -p "${DASHBOARD_PID}" >/dev/null 2>&1; then
  kill -TERM "${DASHBOARD_PID}" 2>/dev/null || true
fi
rm -f "${PID_FILE}"
echo "Log tail:" >&2
tail -n 40 "${LOG_FILE}" >&2 || true
exit 1
