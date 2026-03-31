#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

export INTERNAL_SYMBOLS="${INTERNAL_SYMBOLS:-$(active_schwab_symbols)}"
export INCLUDE_3M="${INCLUDE_3M:-1}"
export INCLUDE_5M_FROM_1M="${INCLUDE_5M_FROM_1M:-0}"
export HOUR_LADDER_SOURCE_TIMEFRAME="${HOUR_LADDER_SOURCE_TIMEFRAME:-30m}"
export HOUR_LADDER_TARGETS="${HOUR_LADDER_TARGETS:-60m,120m,240m,360m,720m}"

IFS=',' read -r -a symbols <<< "${INTERNAL_SYMBOLS}"
IFS=',' read -r -a hour_targets <<< "${HOUR_LADDER_TARGETS}"

for symbol in "${symbols[@]}"; do
  symbol="$(echo "${symbol}" | xargs)"
  [[ -n "${symbol}" ]] || continue

  if [[ "${INCLUDE_3M}" == "1" ]]; then
    "${PYTHON_BIN}" -m mgc_v05l.app.main research-resample-bars \
      --config "${CONFIG_BASE}" \
      --config "${CONFIG_REPLAY}" \
      --ticker "${symbol}" \
      --source-timeframe "1m" \
      --target-timeframe "3m" \
      --persist \
      --target-data-source "resampled_1m_to_3m" \
      --output-csv "${REPORT_DIR}/${symbol}_3m_from_1m.csv"
  fi

  if [[ "${INCLUDE_5M_FROM_1M}" == "1" ]]; then
    "${PYTHON_BIN}" -m mgc_v05l.app.main research-resample-bars \
      --config "${CONFIG_BASE}" \
      --config "${CONFIG_REPLAY}" \
      --ticker "${symbol}" \
      --source-timeframe "1m" \
      --target-timeframe "5m" \
      --persist \
      --target-data-source "resampled_1m_to_5m" \
      --output-csv "${REPORT_DIR}/${symbol}_5m_from_1m.csv"
  fi

  for timeframe in "${hour_targets[@]}"; do
    timeframe="$(echo "${timeframe}" | xargs)"
    [[ -n "${timeframe}" ]] || continue
    canonical="$(PYTHONPATH="${REPO_ROOT}/src" "${PYTHON_BIN}" - <<'PY' "$timeframe"
from mgc_v05l.market_data.timeframes import normalize_timeframe_label
import sys
print(normalize_timeframe_label(sys.argv[1]))
PY
)"
    "${PYTHON_BIN}" -m mgc_v05l.app.main research-resample-bars \
      --config "${CONFIG_BASE}" \
      --config "${CONFIG_REPLAY}" \
      --ticker "${symbol}" \
      --source-timeframe "${HOUR_LADDER_SOURCE_TIMEFRAME}" \
      --target-timeframe "${canonical}" \
      --persist \
      --target-data-source "resampled_${HOUR_LADDER_SOURCE_TIMEFRAME}_to_${canonical}" \
      --output-csv "${REPORT_DIR}/${symbol}_${canonical}_from_${HOUR_LADDER_SOURCE_TIMEFRAME}.csv"
  done
done

"${PYTHON_BIN}" -m mgc_v05l.app.market_data_status_report \
  --db-path "${DB_PATH}" \
  --symbol-config "${SCHWAB_CONFIG}" \
  --output-json "${REPORT_DIR}/market_data_status_after_timeframe_resample.json" \
  --output-csv "${REPORT_DIR}/market_data_status_after_timeframe_resample.csv"
