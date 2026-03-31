#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

export STATUS_JSON="${STATUS_JSON:-${REPORT_DIR}/market_data_status.json}"
export STATUS_CSV="${STATUS_CSV:-${REPORT_DIR}/market_data_status.csv}"

"${PYTHON_BIN}" -m mgc_v05l.app.market_data_status_report \
  --db-path "${DB_PATH}" \
  --symbol-config "${SCHWAB_CONFIG}" \
  --output-json "${STATUS_JSON}" \
  --output-csv "${STATUS_CSV}"
