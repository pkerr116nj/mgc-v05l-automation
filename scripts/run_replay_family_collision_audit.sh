#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

if [[ $# -ne 2 ]]; then
  echo "Usage: bash scripts/run_replay_family_collision_audit.sh /absolute/path/to/control.summary.json /absolute/path/to/treatment.summary.json" >&2
  exit 1
fi

CONTROL_SUMMARY_PATH="$1"
TREATMENT_SUMMARY_PATH="$2"

if [[ ! -f "${CONTROL_SUMMARY_PATH}" ]]; then
  echo "Control summary file not found: ${CONTROL_SUMMARY_PATH}" >&2
  exit 1
fi

if [[ ! -f "${TREATMENT_SUMMARY_PATH}" ]]; then
  echo "Treatment summary file not found: ${TREATMENT_SUMMARY_PATH}" >&2
  exit 1
fi

"${PYTHON_BIN}" -m mgc_v05l.app.replay_family_collision_audit \
  "${CONTROL_SUMMARY_PATH}" \
  "${TREATMENT_SUMMARY_PATH}"
