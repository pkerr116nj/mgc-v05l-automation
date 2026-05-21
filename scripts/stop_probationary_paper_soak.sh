#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

PID_FILE="${PROBATIONARY_PAPER_PID_FILE:-${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid}"
LAUNCHCTL_LABEL_FILE="${PID_FILE}.launchctl_label"
RUNTIME_LAUNCHCTL_LABEL_PREFIX="com.mgc-v05l.headless-supervised-paper.runtime."

remove_runtime_launchctl_labels() {
  local label
  if [[ -f "${LAUNCHCTL_LABEL_FILE}" ]]; then
    label="$(tr -d '[:space:]' < "${LAUNCHCTL_LABEL_FILE}" 2>/dev/null || true)"
    if [[ -n "${label}" ]]; then
      launchctl remove "${label}" >/dev/null 2>&1 || true
    fi
  fi
  if command -v launchctl >/dev/null 2>&1; then
    while IFS= read -r label; do
      [[ -n "${label}" ]] || continue
      launchctl remove "${label}" >/dev/null 2>&1 || true
    done < <(
      launchctl list 2>/dev/null \
        | awk -v prefix="${RUNTIME_LAUNCHCTL_LABEL_PREFIX}" 'NR > 1 && index($3, prefix) == 1 {print $3}'
    )
  fi
}

remove_runtime_launchctl_labels

if [[ ! -f "${PID_FILE}" ]]; then
  echo "No probationary paper PID file found at ${PID_FILE}."
  exit 1
fi

PID="$(cat "${PID_FILE}")"
if [[ -z "${PID}" ]]; then
  echo "PID file is empty: ${PID_FILE}" >&2
  exit 1
fi

if ! kill -0 "${PID}" 2>/dev/null; then
  echo "No running process found for PID ${PID}; removing stale PID file."
  rm -f "${PID_FILE}" "${LAUNCHCTL_LABEL_FILE}"
  exit 0
fi

kill -TERM "${PID}"
for _ in $(seq 1 20); do
  if ! kill -0 "${PID}" 2>/dev/null; then
    rm -f "${PID_FILE}" "${LAUNCHCTL_LABEL_FILE}"
    echo "Probationary paper soak stopped cleanly."
    exit 0
  fi
  sleep 1
done

echo "Process ${PID} did not exit after TERM; PID file left in place at ${PID_FILE}." >&2
exit 1
