#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/run_open_late_additive_sequence_shape_analysis.sh <summary.json>" >&2
  exit 1
fi

SUMMARY_PATH="$1"

if [[ ! -f "${SUMMARY_PATH}" ]]; then
  echo "Replay summary not found: ${SUMMARY_PATH}" >&2
  exit 1
fi

export SUMMARY_PATH

"${PYTHON_BIN}" - <<'PY'
from __future__ import annotations

import json
import os
from pathlib import Path

from mgc_v05l.app.open_late_additive_sequence_shape_analysis import (
    build_and_write_open_late_additive_sequence_shape_analysis,
)

paths = build_and_write_open_late_additive_sequence_shape_analysis(
    summary_path=Path(os.environ["SUMMARY_PATH"]),
)
print(json.dumps(paths, sort_keys=True))
PY

PREFIX="${SUMMARY_PATH%.summary.json}"
for path in \
  "${PREFIX}.open_late_additive_sequence_shape_detail.csv" \
  "${PREFIX}.open_late_additive_sequence_shape_comparison.csv" \
  "${PREFIX}.open_late_additive_sequence_shape_summary.json"; do
  if [[ ! -f "${path}" ]]; then
    echo "Expected open-late additive sequence-shape artifact was not created: ${path}" >&2
    exit 1
  fi
done
