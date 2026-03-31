#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/common_env.sh"

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/run_us_late_pause_resume_long_carryover_robustness.sh /absolute/path/to/family_trades.csv" >&2
  exit 1
fi

"${PYTHON_BIN}" -m mgc_v05l.app.us_late_pause_resume_long_carryover_robustness "$1"
