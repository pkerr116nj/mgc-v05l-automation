#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/run_pattern_engine_v1_failed_move_reversal_london_open_short_separator.sh /absolute/path/to/pattern_engine_v1_detail.csv" >&2
  exit 1
fi

PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"
"${PYTHON_BIN}" -m mgc_v05l.app.pattern_engine_v1_failed_move_reversal_london_open_short_separator "$1"
