#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <widened-summary-path>" >&2
  exit 1
fi

WIDENED_SUMMARY_PATH="$1"

PYTHONPATH="${ROOT_DIR}/src:${PYTHONPATH:-}" "${PYTHON_BIN}" - <<'PY' "${WIDENED_SUMMARY_PATH}"
from __future__ import annotations

import json
import sys
from pathlib import Path

from mgc_v05l.app.derivative_state_separator_research import (
    build_and_write_derivative_state_separator_research,
)

widened_summary_path = Path(sys.argv[1]).resolve()
artifacts = build_and_write_derivative_state_separator_research(
    widened_summary_path=widened_summary_path,
)
print(json.dumps(artifacts, indent=2, sort_keys=True))
PY
