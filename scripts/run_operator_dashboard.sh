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
DEFAULT_READINESS_FILE="${OPERATOR_DASHBOARD_READINESS_FILE:-${DEFAULT_RUNTIME_DIR}/operator_dashboard_readiness.json}"
DEFAULT_PID_FILE="${OPERATOR_DASHBOARD_PID_FILE:-${DEFAULT_RUNTIME_DIR}/operator_dashboard.pid}"
INTERNAL_INFO_FILE="${TMPDIR:-/tmp}/mgc_v05l_operator_dashboard_launch.json"

HOST="${DEFAULT_HOST}"
PORT="${DEFAULT_PORT}"
LOG_FILE="${DEFAULT_LOG_FILE}"
INFO_FILE="${DEFAULT_INFO_FILE}"
READINESS_FILE="${DEFAULT_READINESS_FILE}"
PID_FILE="${DEFAULT_PID_FILE}"
PRINT_INFO_FILE=1
OPEN_BROWSER=0
VERIFY_DASHBOARD_API=0
ALLOW_PORT_FALLBACK=0
PASSTHROUGH_ARGS=()
DASHBOARD_PID=""
HEARTBEAT_PID=""
MANAGER_INSTANCE_ID="$("${PYTHON_BIN}" - <<'PY'
import uuid
print(uuid.uuid4().hex)
PY
)"
resolve_node_bin() {
  local candidate=""
  candidate="$(command -v node || true)"
  if [[ -n "${candidate}" ]]; then
    echo "${candidate}"
    return 0
  fi
  for fallback in "/opt/homebrew/bin/node" "/usr/local/bin/node"; do
    if [[ -x "${fallback}" ]]; then
      echo "${fallback}"
      return 0
    fi
  done
  return 1
}

NODE_BIN="$(resolve_node_bin || true)"

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
ensure_dir "$(dirname "${READINESS_FILE}")"
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
  if [[ -n "${HEARTBEAT_PID}" ]] && ps -p "${HEARTBEAT_PID}" >/dev/null 2>&1; then
    kill -TERM "${HEARTBEAT_PID}" 2>/dev/null || true
  fi
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

read_json_path() {
  local json_file="$1"
  local path_expr="$2"
  "${PYTHON_BIN}" - <<'PY' "${json_file}" "${path_expr}"
import json
import sys
from pathlib import Path

payload_path = Path(sys.argv[1])
path_expr = sys.argv[2]
payload = json.loads(payload_path.read_text(encoding="utf-8"))
value = payload
for part in path_expr.split("."):
    if isinstance(value, dict):
        value = value.get(part)
    else:
        value = None
        break
if value is None:
    raise SystemExit(1)
if isinstance(value, bool):
    print("true" if value else "false")
elif isinstance(value, (dict, list)):
    print(json.dumps(value))
else:
    print(value)
PY
}

publish_dashboard_readiness_contract() {
  local manager_mode="$1"
  local wait_timeout_ms="$2"
  if [[ -z "${NODE_BIN}" ]]; then
    echo "Dashboard readiness contract helper could not find node on PATH, /opt/homebrew/bin/node, or /usr/local/bin/node." >&2
    return 1
  fi
  "${NODE_BIN}" "${REPO_ROOT}/desktop/scripts/publish_dashboard_readiness_contract.js" \
    --mode startup_wait \
    --output "${READINESS_FILE}" \
    --info-file "${INFO_FILE}" \
    --configured-url "http://${HOST}:${PORT}/" \
    --wait-timeout-ms "${wait_timeout_ms}" \
    --sample-interval-ms 500 \
    --sample-history-limit 8 \
    --min-stable-samples 3 \
    --stability-window-ms 1500 \
    --lease-ttl-ms 5000 \
    --manager-pid "$$" \
    --server-pid "${DASHBOARD_PID:-0}" \
    --manager-mode "${manager_mode}" \
    --manager-instance-id "${MANAGER_INSTANCE_ID}"
}

start_dashboard_readiness_heartbeat() {
  local manager_mode="$1"
  local owner_pid="${2:-${DASHBOARD_PID:-0}}"
  local server_pid="${3:-${DASHBOARD_PID:-0}}"
  nohup env \
    REPO_ROOT="${REPO_ROOT}" \
    READINESS_FILE="${READINESS_FILE}" \
    INFO_FILE="${INFO_FILE}" \
    HOST="${HOST}" \
    PORT="${PORT}" \
    OWNER_PID="${owner_pid}" \
    SERVER_PID="${server_pid}" \
    MANAGER_MODE="${manager_mode}" \
    MANAGER_INSTANCE_ID="${MANAGER_INSTANCE_ID}" \
    NODE_BIN="${NODE_BIN}" \
    /bin/zsh -lc '
      while true; do
        if [[ "${OWNER_PID}" != "0" ]] && ! ps -p "${OWNER_PID}" >/dev/null 2>&1; then
          exit 0
        fi
        if [[ "${SERVER_PID}" != "0" ]] && ! ps -p "${SERVER_PID}" >/dev/null 2>&1; then
          exit 0
        fi
        "${NODE_BIN}" "${REPO_ROOT}/desktop/scripts/publish_dashboard_readiness_contract.js" \
          --mode continuous \
          --output "${READINESS_FILE}" \
          --info-file "${INFO_FILE}" \
          --configured-url "http://${HOST}:${PORT}/" \
          --wait-timeout-ms 0 \
          --sample-interval-ms 1000 \
          --sample-history-limit 8 \
          --min-stable-samples 3 \
          --stability-window-ms 1500 \
          --lease-ttl-ms 5000 \
          --manager-pid "${OWNER_PID}" \
          --server-pid "${SERVER_PID}" \
          --manager-mode "${MANAGER_MODE}" \
          --manager-instance-id "${MANAGER_INSTANCE_ID}"
        if [[ "${OWNER_PID}" != "0" ]] && ! ps -p "${OWNER_PID}" >/dev/null 2>&1; then
          exit 0
        fi
        if [[ "${SERVER_PID}" != "0" ]] && ! ps -p "${SERVER_PID}" >/dev/null 2>&1; then
          exit 0
        fi
        sleep 1
      done
    ' >/dev/null 2>&1 &
  HEARTBEAT_PID=$!
}

await_launch_ready_contract() {
  local timeout_ms="$1"
  publish_dashboard_readiness_contract "startup_wait" "${timeout_ms}"
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
    [[ "${normalized}" == *"python"* && "${normalized}" == *"operator_dashboard"* ]] || \
    [[ "${normalized}" == *"run_operator_dashboard.sh"* ]] || \
    [[ "${normalized}" == *"desktop/dist/main/main.js"* && "${normalized}" == *"electron"* ]]
}

dashboard_health_identifies_local_service() {
  local health_url="$1"
  "${PYTHON_BIN}" - <<'PY' "${health_url}"
import json
import sys
import urllib.request

url = sys.argv[1]
try:
    payload = json.loads(urllib.request.urlopen(url, timeout=2).read().decode("utf-8"))
except Exception:
    raise SystemExit(1)
endpoints = payload.get("endpoints") or {}
build_stamp = str(payload.get("build_stamp") or "").strip()
dashboard_endpoint = str(endpoints.get("dashboard") or "").strip()
service_ready = bool(payload.get("status"))
if build_stamp and dashboard_endpoint == "/api/dashboard" and service_ready:
    raise SystemExit(0)
raise SystemExit(1)
PY
}

listener_matches_local_dashboard_service() {
  local host="$1"
  local port="$2"
  dashboard_health_identifies_local_service "http://${host}:${port}/health"
}

recover_local_dashboard_listener() {
  local pid="$1"
  if [[ -z "${pid}" ]]; then
    return 1
  fi
  if stop_dashboard_process "${pid}"; then
    sleep 0.5
    return 0
  fi
  return 1
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
  echo "Readiness contract: ${READINESS_FILE}"
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
      RECOVERY_PID="${PID_EXISTING}"
      if [[ -z "${RECOVERY_PID}" ]]; then
        RECOVERY_PID="$(listener_pid_for_port "${BOUND_PORT_EXISTING:-${PORT}}" || true)"
      fi
      if [[ -n "${RECOVERY_PID}" ]] && recover_local_dashboard_listener "${RECOVERY_PID}"; then
        rm -f "${PID_FILE}" "${INFO_FILE}"
      else
        emit_startup_failure \
          "stale_dashboard_instance" \
          "Could not stop the old dashboard instance cleanly. PID: ${RECOVERY_PID:-${PID_EXISTING:-unknown}}" \
          "Stop the stale local dashboard instance, then retry Start/Restart Dashboard/API."
        exit 1
      fi
    else
      if ! publish_dashboard_readiness_contract "reuse_existing" 4000; then
        READINESS_DETAIL="$(read_json_path "${READINESS_FILE}" reason_detail 2>/dev/null || true)"
        emit_startup_failure \
          "dashboard_api_not_ready" \
          "Existing dashboard health was reachable, but the manager could not verify a stable launch-ready readiness contract. ${READINESS_DETAIL:-Dashboard readiness remained ambiguous.}" \
          "Wait for the dashboard readiness contract to stabilize, then retry Start/Restart Dashboard/API."
        exit 1
      fi
      if [[ -n "${PID_EXISTING}" ]]; then
        start_dashboard_readiness_heartbeat "reuse_existing" "${PID_EXISTING}" "${PID_EXISTING}"
      fi
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

rm -f "${INFO_FILE}" "${READINESS_FILE}"

LISTENER_PID="$(listener_pid_for_port "${PORT}" || true)"
if [[ -n "${LISTENER_PID}" ]]; then
  LISTENER_COMMAND="$(read_process_command "${LISTENER_PID}")"
  emit_startup_marker "STALE_LISTENER_DETECTED" "1"
  emit_startup_marker "PORT_CONFLICT_DETECTED" "1"
  if {
    [[ -n "${LISTENER_COMMAND}" ]] && is_recoverable_local_dashboard_listener "${LISTENER_COMMAND}";
  } || listener_matches_local_dashboard_service "${HOST}" "${PORT}"; then
    echo "Detected recoverable local dashboard listener on ${HOST}:${PORT}; stopping PID ${LISTENER_PID} automatically." >&2
    if recover_local_dashboard_listener "${LISTENER_PID}"; then
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

if await_launch_ready_contract 30000; then
  start_dashboard_readiness_heartbeat "steady_state" "${DASHBOARD_PID}" "${DASHBOARD_PID}"
  DASHBOARD_URL="$(read_info_field "${INFO_FILE}" url 2>/dev/null || true)"
  BOUND_HOST="$(read_info_field "${INFO_FILE}" host 2>/dev/null || true)"
  BOUND_PORT="$(read_info_field "${INFO_FILE}" port 2>/dev/null || true)"
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
  report_success "${DASHBOARD_URL}" "${BOUND_HOST}" "${BOUND_PORT}" "${DASHBOARD_PID}" "${RUNNING_BUILD:-${LOCAL_BUILD_STAMP}}" "${RUNNING_STARTED:-unknown}" 0 "${RUNNING_HEALTH_STATUS:-unknown}" "${RUNNING_HEALTH_READY:-false}"
  if [[ ${OPEN_BROWSER} -eq 1 ]]; then
    if ! open_browser "${DASHBOARD_URL}"; then
      echo "Dashboard is healthy, but browser auto-open failed. Open this URL manually: ${DASHBOARD_URL}" >&2
    fi
  fi
  wait "${DASHBOARD_PID}"
  exit $?
fi

READINESS_REASON="$(read_json_path "${READINESS_FILE}" reason_code 2>/dev/null || true)"
READINESS_DETAIL="$(read_json_path "${READINESS_FILE}" reason_detail 2>/dev/null || true)"
if ! ps -p "${DASHBOARD_PID}" >/dev/null 2>&1; then
  emit_startup_failure \
    "early_process_exit" \
    "Dashboard failed to start: server process exited early. ${READINESS_DETAIL:-}" \
    "Review the backend log tail, fix the startup error, then retry Start/Restart Dashboard/API."
elif [[ "${READINESS_REASON}" == "stability_window_not_met" ]] || [[ "${READINESS_REASON}" == "payload_not_ready" ]] || [[ "${READINESS_REASON}" == "service_warming" ]]; then
  emit_startup_marker "DASHBOARD_API_TIMED_OUT" "1"
  echo "Dashboard manager did not yet observe a stable launch-ready readiness contract. Continuing managed warmup with lease renewal." >&2
  echo "Current readiness reason: ${READINESS_DETAIL:-Dashboard readiness remained unstable.}" >&2
  start_dashboard_readiness_heartbeat "warming"
  wait "${DASHBOARD_PID}"
  exit $?
else
  emit_startup_failure \
    "permission_or_bind_failure" \
    "Dashboard failed to publish a trustworthy readiness contract before timeout. ${READINESS_DETAIL:-Dashboard ownership or listener state remained ambiguous.}" \
    "Review the backend log and readiness contract for ownership or bind failures, then retry Dashboard/API start."
fi
if ps -p "${DASHBOARD_PID}" >/dev/null 2>&1; then
  kill -TERM "${DASHBOARD_PID}" 2>/dev/null || true
fi
rm -f "${PID_FILE}"
echo "Log tail:" >&2
tail -n 40 "${LOG_FILE}" >&2 || true
exit 1
