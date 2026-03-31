#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

PID_FILE="${ATP_COMPANION_V1_PAPER_PID_FILE:-${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/atp_companion_v1_paper.pid}"
LOG_FILE="${ATP_COMPANION_V1_PAPER_LOG_FILE:-${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/atp_companion_v1_paper.log}"

exec bash "${SCRIPT_DIR}/run_probationary_paper_soak.sh" \
  --include-atp-companion-v1-paper \
  --pid-file "${PID_FILE}" \
  --log-file "${LOG_FILE}" \
  "$@"
