#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/run_pattern_engine_v1_pause_family_mining.sh /absolute/path/to/pattern_engine_v1_detail.csv" >&2
  exit 1
fi

DETAIL_PATH="$1"

if [[ ! -f "${DETAIL_PATH}" ]]; then
  echo "Pattern Engine v1 detail file not found: ${DETAIL_PATH}" >&2
  exit 1
fi

"${PYTHON_BIN}" -m mgc_v05l.app.pattern_engine_v1_pause_family_mining "${DETAIL_PATH}"
