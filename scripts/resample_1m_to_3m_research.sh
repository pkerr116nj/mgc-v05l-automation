#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

export INTERNAL_SYMBOL="${INTERNAL_SYMBOL:-MGC}"
export SOURCE_TIMEFRAME="${SOURCE_TIMEFRAME:-1m}"
export TARGET_TIMEFRAME="${TARGET_TIMEFRAME:-3m}"
export TARGET_DATA_SOURCE="${TARGET_DATA_SOURCE:-resampled_1m_to_3m}"
export OUTPUT_CSV="${OUTPUT_CSV:-${OUTPUT_ROOT}/reports/${INTERNAL_SYMBOL}_${TARGET_TIMEFRAME}_from_${SOURCE_TIMEFRAME}.csv}"

"${PYTHON_BIN}" -m mgc_v05l.app.main research-resample-bars \
  --config "${CONFIG_BASE}" \
  --config "${CONFIG_REPLAY}" \
  --ticker "${INTERNAL_SYMBOL}" \
  --source-timeframe "${SOURCE_TIMEFRAME}" \
  --target-timeframe "${TARGET_TIMEFRAME}" \
  --persist \
  --target-data-source "${TARGET_DATA_SOURCE}" \
  --output-csv "${OUTPUT_CSV}"
