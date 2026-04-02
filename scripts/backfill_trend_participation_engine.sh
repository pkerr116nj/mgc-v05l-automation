#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

export TPE_SOURCE_SQLITE="${TPE_SOURCE_SQLITE:-${DB_PATH}}"
export TPE_OUTPUT_DIR="${TPE_OUTPUT_DIR:-${REPORT_DIR}/trend_participation_engine}"
export TPE_INSTRUMENTS="${TPE_INSTRUMENTS:-MES MNQ}"
export TPE_START_TIMESTAMP="${TPE_START_TIMESTAMP:-}"
export TPE_END_TIMESTAMP="${TPE_END_TIMESTAMP:-}"
export TPE_PRIORITY_SIGNALS_JSON="${TPE_PRIORITY_SIGNALS_JSON:-}"
export TPE_SKIP_STORAGE_MATERIALIZATION="${TPE_SKIP_STORAGE_MATERIALIZATION:-0}"

cmd=(
  "${PYTHON_BIN}" -m mgc_v05l.app.main research-trend-participation
  --source-sqlite "${TPE_SOURCE_SQLITE}"
  --output-dir "${TPE_OUTPUT_DIR}"
  --mode backfill
  --instruments ${TPE_INSTRUMENTS}
)

if [[ -n "${TPE_START_TIMESTAMP}" ]]; then
  cmd+=(--start-timestamp "${TPE_START_TIMESTAMP}")
fi
if [[ -n "${TPE_END_TIMESTAMP}" ]]; then
  cmd+=(--end-timestamp "${TPE_END_TIMESTAMP}")
fi
if [[ -n "${TPE_PRIORITY_SIGNALS_JSON}" ]]; then
  cmd+=(--priority-signals-json "${TPE_PRIORITY_SIGNALS_JSON}")
fi
if [[ "${TPE_SKIP_STORAGE_MATERIALIZATION}" == "1" ]]; then
  cmd+=(--skip-storage-materialization)
fi

"${cmd[@]}"
