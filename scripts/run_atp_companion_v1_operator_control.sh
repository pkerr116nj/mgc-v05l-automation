#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

require_schwab_auth_env

CONFIG_ARGS=(
  --config "${REPO_ROOT}/config/base.yaml"
  --config "${REPO_ROOT}/config/live.yaml"
  --config "${REPO_ROOT}/config/probationary_pattern_engine.yaml"
  --config "${REPO_ROOT}/config/probationary_pattern_engine_paper.yaml"
  --config "${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_v1_asia_us.yaml"
)

exec "${PYTHON_BIN}" -m mgc_v05l.app.main probationary-operator-control "${CONFIG_ARGS[@]}" "$@"
