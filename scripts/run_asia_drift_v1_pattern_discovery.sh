#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export PYTHONPATH="${PYTHONPATH:-src}"
exec ./.venv/bin/python -m mgc_v05l.app.asia_drift_v1_pattern_discovery "$@"
