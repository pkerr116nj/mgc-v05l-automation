#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"
PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
  "${REPO_ROOT}/.venv/bin/python" -m mgc_v05l.execution_core.track_b_service_ownership \
  --repo-root "${REPO_ROOT}" \
  --write \
  --emit-plists \
  "$@"
