#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

require_schwab_auth_env

DEFAULT_PAPER_PID_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid"
PAPER_PID_FILE="${DEFAULT_PAPER_PID_FILE}"
PASSTHROUGH_ARGS=()

while (($# > 0)); do
  case "$1" in
    --paper-pid-file)
      PAPER_PID_FILE="$2"
      shift 2
      ;;
    --paper-pid-file=*)
      PAPER_PID_FILE="${1#*=}"
      shift
      ;;
    *)
      PASSTHROUGH_ARGS+=("$1")
      shift
      ;;
  esac
done

start_paper_runtime_if_needed() {
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

if ! start_paper_runtime_if_needed; then
  echo "Failed to start the supervised paper runtime." >&2
  exit 1
fi

export MGC_SERVICE_HOST_AUTOSTART_RESEARCH_RUNTIME_BRIDGE_SUPERVISOR="${MGC_SERVICE_HOST_AUTOSTART_RESEARCH_RUNTIME_BRIDGE_SUPERVISOR:-1}"

echo "Starting supervised paper host (paper-only, backend-first)." >&2
echo "Packaged desktop UI is optional and not required for uptime." >&2

if ((${#PASSTHROUGH_ARGS[@]} > 0)); then
  exec bash "${SCRIPT_DIR}/run_operator_dashboard.sh" --no-open-browser "${PASSTHROUGH_ARGS[@]}"
fi

exec bash "${SCRIPT_DIR}/run_operator_dashboard.sh" --no-open-browser
