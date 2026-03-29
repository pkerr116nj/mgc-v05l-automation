#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/run_replay_turn_research.sh /absolute/path/to/replay.summary.json" >&2
  exit 1
fi

SUMMARY_PATH_INPUT="$1"
if [[ ! -f "${SUMMARY_PATH_INPUT}" ]]; then
  echo "Replay summary file not found: ${SUMMARY_PATH_INPUT}" >&2
  exit 1
fi

export REPLAY_SUMMARY_PATH="${SUMMARY_PATH_INPUT}"

"${PYTHON_BIN}" - <<'PY'
from __future__ import annotations

import json
import os
from pathlib import Path

from mgc_v05l.app.replay_turn_research import build_and_write_replay_turn_research

summary_path = Path(os.environ["REPLAY_SUMMARY_PATH"])
paths = build_and_write_replay_turn_research(summary_path)
print(json.dumps(paths, sort_keys=True))
PY

PREFIX="${SUMMARY_PATH_INPUT%.summary.json}"
for path in \
  "${PREFIX}.turn_dataset.csv" \
  "${PREFIX}.turn_summary.json" \
  "${PREFIX}.derivative_bins.csv" \
  "${PREFIX}.missed_turns_by_derivative_bucket.csv" \
  "${PREFIX}.entry_quality_by_derivative_bucket.csv"; do
  if [[ ! -f "${path}" ]]; then
    echo "Expected replay turn-research artifact was not created: ${path}" >&2
    exit 1
  fi
done
