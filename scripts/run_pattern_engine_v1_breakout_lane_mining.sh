#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/run_pattern_engine_v1_breakout_lane_mining.sh /absolute/path/to/pattern_engine_v1_detail.csv" >&2
  exit 1
fi

DETAIL_PATH="$1"
if [[ "${DETAIL_PATH}" != /* ]]; then
  echo "detail path must be absolute" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-${ROOT_DIR}/.venv/bin/python}"

cd "${ROOT_DIR}"
PYTHONPATH=src "${PYTHON_BIN}" -m mgc_v05l.app.pattern_engine_v1_breakout_lane_mining "${DETAIL_PATH}"
