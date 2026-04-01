#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

require_schwab_auth_env

DEFAULT_CONFIGS=(
  "${REPO_ROOT}/config/base.yaml"
  "${REPO_ROOT}/config/live.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper.yaml"
)

FINAL_ARGS=()
if [[ $# -gt 0 ]]; then
  FINAL_ARGS=("$@")
else
  echo "Usage: bash scripts/run_probationary_operator_control.sh --action <halt_entries|resume_entries|clear_fault|clear_risk_halts|flatten_and_halt|stop_after_cycle> [--shared-strategy-identity <IDENTITY>]" >&2
  exit 1
fi

CONFIG_ARGS=()
for config_path in "${DEFAULT_CONFIGS[@]}"; do
  CONFIG_ARGS+=(--config "${config_path}")
done

exec "${PYTHON_BIN}" -m mgc_v05l.app.main probationary-operator-control "${CONFIG_ARGS[@]}" "${FINAL_ARGS[@]}"
