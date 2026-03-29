#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

require_schwab_auth_env

INTRADAY_START="${INTRADAY_START:-2025-06-30T00:00:00+00:00}"
INTRADAY_END="${INTRADAY_END:-2026-03-18T00:00:00+00:00}"
DAILY_START="${DAILY_START:-2010-01-01T00:00:00+00:00}"

"${PYTHON_BIN}" -m mgc_v05l.app.approved_branch_research_audit \
  etf-backfill \
  --symbols TQQQ SQQQ \
  --intraday-start "${INTRADAY_START}" \
  --intraday-end "${INTRADAY_END}" \
  --daily-start "${DAILY_START}"
