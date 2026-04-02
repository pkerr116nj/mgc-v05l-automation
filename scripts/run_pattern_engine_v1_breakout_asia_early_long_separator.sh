#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/common_env.sh"

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/run_pattern_engine_v1_breakout_asia_early_long_separator.sh /absolute/path/to/pattern_engine_v1_detail.csv" >&2
  exit 1
fi

"${PYTHON_BIN}" -m mgc_v05l.app.pattern_engine_v1_breakout_asia_early_long_separator "$1"
