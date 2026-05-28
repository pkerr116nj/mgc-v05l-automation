#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

"${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_runtime_operability_contract \
  --repo-root "${REPO_ROOT}" \
  --json
