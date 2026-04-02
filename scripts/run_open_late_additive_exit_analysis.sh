#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

if [[ $# -ne 3 ]]; then
  echo "Usage: bash scripts/run_open_late_additive_exit_analysis.sh <additive-summary.json> <anchor-summary-metrics.json> <open-full-summary-metrics.json>" >&2
  exit 1
fi

SUMMARY_PATH="$1"
ANCHOR_SUMMARY_METRICS_PATH="$2"
OPEN_FULL_SUMMARY_METRICS_PATH="$3"

for path in "${SUMMARY_PATH}" "${ANCHOR_SUMMARY_METRICS_PATH}" "${OPEN_FULL_SUMMARY_METRICS_PATH}"; do
  if [[ ! -f "${path}" ]]; then
    echo "Required input not found: ${path}" >&2
    exit 1
  fi
done

export SUMMARY_PATH
export ANCHOR_SUMMARY_METRICS_PATH
export OPEN_FULL_SUMMARY_METRICS_PATH

"${PYTHON_BIN}" - <<'PY'
from __future__ import annotations

import json
import os
from pathlib import Path

from mgc_v05l.app.open_late_additive_exit_analysis import (
    build_and_write_open_late_additive_exit_analysis,
)

paths = build_and_write_open_late_additive_exit_analysis(
    summary_path=Path(os.environ["SUMMARY_PATH"]),
    anchor_summary_metrics_path=Path(os.environ["ANCHOR_SUMMARY_METRICS_PATH"]),
    open_full_summary_metrics_path=Path(os.environ["OPEN_FULL_SUMMARY_METRICS_PATH"]),
)
print(json.dumps(paths, sort_keys=True))
PY

PREFIX="${SUMMARY_PATH%.summary.json}"
for path in \
  "${PREFIX}.open_late_additive_exit_trade_diagnostics.csv" \
  "${PREFIX}.open_late_additive_exit_summary.json"; do
  if [[ ! -f "${path}" ]]; then
    echo "Expected open-late additive exit artifact was not created: ${path}" >&2
    exit 1
  fi
done
