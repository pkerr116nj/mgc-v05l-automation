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
)

FINAL_ARGS=()
if [[ $# -eq 0 ]]; then
  for config_path in "${DEFAULT_CONFIGS[@]}"; do
    FINAL_ARGS+=(--config "${config_path}")
  done
else
  FINAL_ARGS=("$@")
fi

exec "${PYTHON_BIN}" -m mgc_v05l.app.main probationary-inspect "${FINAL_ARGS[@]}"
