#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

export INTERNAL_SYMBOLS="${INTERNAL_SYMBOLS:-MGC}"
export SOURCE_TIMEFRAME="${SOURCE_TIMEFRAME:-1m}"
export TARGET_TIMEFRAMES="${TARGET_TIMEFRAMES:-3m}"

IFS=',' read -r -a symbols <<< "${INTERNAL_SYMBOLS}"
IFS=',' read -r -a timeframes <<< "${TARGET_TIMEFRAMES}"

for symbol in "${symbols[@]}"; do
  symbol="$(echo "${symbol}" | xargs)"
  [[ -n "${symbol}" ]] || continue
  for timeframe in "${timeframes[@]}"; do
    timeframe="$(echo "${timeframe}" | xargs)"
    [[ -n "${timeframe}" ]] || continue
    target_data_source="resampled_1m_to_${timeframe}"
    output_csv="${REPORT_DIR}/${symbol}_${timeframe}_from_${SOURCE_TIMEFRAME}.csv"
    "${PYTHON_BIN}" -m mgc_v05l.app.main research-resample-bars \
      --config "${CONFIG_BASE}" \
      --config "${CONFIG_REPLAY}" \
      --ticker "${symbol}" \
      --source-timeframe "${SOURCE_TIMEFRAME}" \
      --target-timeframe "${timeframe}" \
      --persist \
      --target-data-source "${target_data_source}" \
      --output-csv "${output_csv}"
  done
done

"${PYTHON_BIN}" -m mgc_v05l.app.market_data_status_report \
  --db-path "${DB_PATH}" \
  --output-json "${REPORT_DIR}/market_data_status_after_resample.json" \
  --output-csv "${REPORT_DIR}/market_data_status_after_resample.csv"
