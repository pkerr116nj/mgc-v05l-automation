#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/run_pattern_engine_v1_scan.sh /absolute/path/to/replay.summary.json" >&2
  exit 1
fi

SUMMARY_PATH="$1"

if [[ ! -f "${SUMMARY_PATH}" ]]; then
  echo "Replay summary file not found: ${SUMMARY_PATH}" >&2
  exit 1
fi

"${PYTHON_BIN}" -m mgc_v05l.app.pattern_engine_v1_scan "${SUMMARY_PATH}" --ticker MGC --timeframe 5m
