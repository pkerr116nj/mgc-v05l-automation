#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load the same runtime environment as the approved PAPER startup path.
# This sources local credential variables such as DATABENTO_API_KEY without
# duplicating or printing secret material.
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

if [[ $# -eq 0 ]]; then
  set -- recover
fi

exec "${REPO_ROOT}/.venv/bin/python" -m mgc_v05l.app.track_b_fast_paper_runtime_start --repo-root "${REPO_ROOT}" "$@"
