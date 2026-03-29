#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

TIMEFRAME="${TIMEFRAME:-5m}"

"${PYTHON_BIN}" -m mgc_v05l.app.approved_branch_research_audit \
  futures-quantitative-deployment \
  --timeframe "${TIMEFRAME}"
