#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

PID_FILE="${OPERATOR_DASHBOARD_PID_FILE:-${REPO_ROOT}/outputs/operator_dashboard/runtime/operator_dashboard.pid}"
INFO_FILE="${OPERATOR_DASHBOARD_INFO_FILE:-${REPO_ROOT}/outputs/operator_dashboard/runtime/operator_dashboard.json}"

if [[ ! -f "${PID_FILE}" ]]; then
  echo "No operator dashboard PID file found at ${PID_FILE}."
  exit 1
fi

PID="$(cat "${PID_FILE}")"
if [[ -z "${PID}" ]]; then
  echo "PID file is empty: ${PID_FILE}" >&2
  exit 1
fi

if ! ps -p "${PID}" >/dev/null 2>&1; then
  echo "No running operator dashboard process found for PID ${PID}; removing stale files."
  rm -f "${PID_FILE}" "${INFO_FILE}"
  exit 0
fi

if ! kill -TERM "${PID}" 2>/dev/null; then
  echo "Failed to send TERM to operator dashboard PID ${PID}." >&2
  exit 1
fi
for _ in $(seq 1 20); do
  if ! ps -p "${PID}" >/dev/null 2>&1; then
    rm -f "${PID_FILE}" "${INFO_FILE}"
    echo "Operator dashboard stopped cleanly."
    exit 0
  fi
  sleep 1
done

echo "Operator dashboard process ${PID} did not exit after TERM; PID file left in place at ${PID_FILE}." >&2
exit 1
