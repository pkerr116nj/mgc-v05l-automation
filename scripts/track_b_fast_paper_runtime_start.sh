#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"
export PYTHONPATH

exec "${REPO_ROOT}/.venv/bin/python" -m mgc_v05l.app.track_b_fast_paper_runtime_start --repo-root "${REPO_ROOT}" "$@"
