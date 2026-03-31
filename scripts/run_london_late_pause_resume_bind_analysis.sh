#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

if [[ $# -ne 2 ]]; then
  echo "Usage: bash scripts/run_london_late_pause_resume_bind_analysis.sh /absolute/path/to/missed_entry_discovery_detail.csv /absolute/path/to/treatment.summary.json" >&2
  exit 1
fi

DETAIL_PATH="$1"
SUMMARY_PATH="$2"

if [[ ! -f "${DETAIL_PATH}" ]]; then
  echo "Missed-entry detail file not found: ${DETAIL_PATH}" >&2
  exit 1
fi

if [[ ! -f "${SUMMARY_PATH}" ]]; then
  echo "Treatment summary file not found: ${SUMMARY_PATH}" >&2
  exit 1
fi

"${PYTHON_BIN}" -m mgc_v05l.app.london_late_pause_resume_bind_analysis \
  "${DETAIL_PATH}" \
  "${SUMMARY_PATH}" \
  "${CONFIG_BASE}" \
  "${CONFIG_REPLAY}" \
  "${REPO_ROOT}/config/replay.research_control.yaml" \
  "${REPO_ROOT}/config/replay.london_late_pause_resume_short_family.yaml"
