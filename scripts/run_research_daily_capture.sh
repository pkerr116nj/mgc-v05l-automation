#!/usr/bin/env bash

set -euo pipefail

if [[ -n "${REPO_ROOT:-}" && -f "${REPO_ROOT}/scripts/common_env.sh" ]]; then
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/scripts/common_env.sh"
else
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}/common_env.sh"
fi

require_schwab_auth_env

DEFAULT_SCHWAB_CONFIG="${SCHWAB_CONFIG:-${REPO_ROOT}/config/schwab.local.json}"

FINAL_ARGS=(
  --config "${REPO_ROOT}/config/base.yaml"
  --config "${REPO_ROOT}/config/replay.yaml"
  --schwab-config "${DEFAULT_SCHWAB_CONFIG}"
)

if [[ -n "${SCHWAB_TOKEN_FILE:-}" ]]; then
  FINAL_ARGS+=(--token-file "${SCHWAB_TOKEN_FILE}")
fi

if [[ "$#" -gt 0 ]]; then
  FINAL_ARGS+=("$@")
fi

exec "${PYTHON_BIN}" -m mgc_v05l.app.main research-daily-capture "${FINAL_ARGS[@]}"
