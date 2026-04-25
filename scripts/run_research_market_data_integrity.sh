#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

LOCAL_DOTENV=".env"
PYTHON_BIN="${RESEARCH_MARKET_DATA_INTEGRITY_PYTHON_BIN:-./.venv/bin/python}"

if [[ -f "${LOCAL_DOTENV}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${LOCAL_DOTENV}"
  set +a
fi

mode="audit"
args=("$@")
for ((i=0; i<${#args[@]}; i++)); do
  if [[ "${args[$i]}" == "--mode" && $((i + 1)) -lt ${#args[@]} ]]; then
    mode="${args[$((i + 1))]}"
    break
  fi
done

if [[ "${mode}" == "backfill" && -z "${DATABENTO_API_KEY:-}" ]]; then
  echo "research_market_data_integrity backfill requires DATABENTO_API_KEY. Add it to the project .env or the active shell environment, then rerun." >&2
  exit 1
fi

PYTHONPATH=src "${PYTHON_BIN}" -m mgc_v05l.app.research_market_data_integrity "$@"
