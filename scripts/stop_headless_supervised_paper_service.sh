#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

MANAGER_PID_FILE="${REPO_ROOT}/outputs/operator_dashboard/runtime/operator_dashboard_manager.pid"

if [[ -f "${MANAGER_PID_FILE}" ]]; then
  manager_pid="$(cat "${MANAGER_PID_FILE}" 2>/dev/null || true)"
  if [[ -n "${manager_pid}" ]] && kill -0 "${manager_pid}" 2>/dev/null; then
    kill -TERM "${manager_pid}" 2>/dev/null || true
  fi
  rm -f "${MANAGER_PID_FILE}"
fi

bash "${SCRIPT_DIR}/stop_operator_dashboard.sh"
bash "${SCRIPT_DIR}/stop_probationary_paper_soak.sh"
