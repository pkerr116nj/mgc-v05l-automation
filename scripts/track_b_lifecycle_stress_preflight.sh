#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

PYTHON_BIN="${PYTHON_BIN:-./.venv/bin/python}"
exec "${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_lifecycle_stress_preflight "$@"
