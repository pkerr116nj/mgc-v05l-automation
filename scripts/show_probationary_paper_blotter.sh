#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

DAILY_DIR="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/daily"

latest_blotter="$(ls -1 "${DAILY_DIR}"/*.blotter.csv 2>/dev/null | sort | tail -n 1 || true)"
if [[ -z "${latest_blotter}" ]]; then
  echo "No paper blotter CSV found under ${DAILY_DIR}. Generate one with bash scripts/run_probationary_paper_summary.sh" >&2
  exit 1
fi

echo "Latest paper blotter: ${latest_blotter}"
exec sed -n '1,80p' "${latest_blotter}"
