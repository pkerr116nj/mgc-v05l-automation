#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

PID_FILE="${OPERATOR_DASHBOARD_PID_FILE:-${REPO_ROOT}/outputs/operator_dashboard/runtime/operator_dashboard.pid}"
INFO_FILE="${OPERATOR_DASHBOARD_INFO_FILE:-${REPO_ROOT}/outputs/operator_dashboard/runtime/operator_dashboard.json}"

if [[ ! -f "${PID_FILE}" ]] && [[ ! -f "${INFO_FILE}" ]]; then
  echo "No operator dashboard PID or info file found at ${PID_FILE} / ${INFO_FILE}."
  exit 1
fi

PID=""
if [[ -f "${PID_FILE}" ]]; then
  PID="$(cat "${PID_FILE}")"
fi
if [[ -z "${PID}" ]] && [[ -f "${INFO_FILE}" ]]; then
  DASHBOARD_URL="$("${PYTHON_BIN}" -c 'import json, sys; print(json.load(open(sys.argv[1], "r", encoding="utf-8")).get("url") or "")' "${INFO_FILE}" 2>/dev/null || true)"
  if [[ -n "${DASHBOARD_URL}" ]]; then
    PID="$("${PYTHON_BIN}" -c 'import json, sys, urllib.request; print(json.loads(urllib.request.urlopen(sys.argv[1].rstrip("/") + "/health", timeout=2).read().decode("utf-8")).get("pid") or "")' "${DASHBOARD_URL}" 2>/dev/null || true)"
  fi
fi
if [[ -z "${PID}" ]]; then
  echo "No operator dashboard PID could be resolved from ${PID_FILE} or ${INFO_FILE}." >&2
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
