#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

PID_FILE="${PROBATIONARY_PAPER_PID_FILE:-${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid}"

if [[ ! -f "${PID_FILE}" ]]; then
  echo "No probationary paper PID file found at ${PID_FILE}; paper runtime is already stopped."
  exit 0
fi

PID="$(cat "${PID_FILE}")"
if [[ -z "${PID}" ]]; then
  echo "PID file is empty: ${PID_FILE}" >&2
  exit 1
fi

if ! kill -0 "${PID}" 2>/dev/null; then
  echo "No running process found for PID ${PID}; removing stale PID file."
  rm -f "${PID_FILE}"
  exit 0
fi

kill -TERM "${PID}"
for _ in $(seq 1 20); do
  if ! kill -0 "${PID}" 2>/dev/null; then
    rm -f "${PID_FILE}"
    echo "Probationary paper soak stopped cleanly."
    exit 0
  fi
  sleep 1
done

echo "Process ${PID} did not exit after TERM; PID file left in place at ${PID_FILE}." >&2
exit 1
