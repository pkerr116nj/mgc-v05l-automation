#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "Usage: bash scripts/run_us_derivative_bear_regime_analysis.sh <anchor.summary.json> <widened.summary.json> [<longer_time_plus.summary.json>]" >&2
  exit 1
fi

ANCHOR_SUMMARY_PATH="$1"
WIDENED_SUMMARY_PATH="$2"
LONGER_TIME_PLUS_SUMMARY_PATH="${3:-}"

for path in "${ANCHOR_SUMMARY_PATH}" "${WIDENED_SUMMARY_PATH}"; do
  if [[ ! -f "${path}" ]]; then
    echo "Replay summary not found: ${path}" >&2
    exit 1
  fi
done
if [[ -n "${LONGER_TIME_PLUS_SUMMARY_PATH}" && ! -f "${LONGER_TIME_PLUS_SUMMARY_PATH}" ]]; then
  echo "Replay summary not found: ${LONGER_TIME_PLUS_SUMMARY_PATH}" >&2
  exit 1
fi

export ANCHOR_SUMMARY_PATH
export WIDENED_SUMMARY_PATH
export LONGER_TIME_PLUS_SUMMARY_PATH

"${PYTHON_BIN}" - <<'PY'
from __future__ import annotations

import json
import os
from pathlib import Path

from mgc_v05l.app.us_derivative_bear_regime_analysis import build_and_write_us_derivative_bear_regime_analysis

paths = build_and_write_us_derivative_bear_regime_analysis(
    anchor_summary_path=Path(os.environ["ANCHOR_SUMMARY_PATH"]),
    widened_summary_path=Path(os.environ["WIDENED_SUMMARY_PATH"]),
    longer_time_plus_summary_path=Path(os.environ["LONGER_TIME_PLUS_SUMMARY_PATH"]) if os.environ.get("LONGER_TIME_PLUS_SUMMARY_PATH") else None,
)
print(json.dumps(paths, sort_keys=True))
PY

PREFIX="${WIDENED_SUMMARY_PATH%.summary.json}"
for path in \
  "${PREFIX}.regime_branch_trade_comparison.csv" \
  "${PREFIX}.regime_slice_summary.csv" \
  "${PREFIX}.regime_hour_breakdown.csv" \
  "${PREFIX}.added_trade_detail.csv" \
  "${PREFIX}.regime_differences.json"; do
  if [[ ! -f "${path}" ]]; then
    echo "Expected regime-analysis artifact was not created: ${path}" >&2
    exit 1
  fi
done
