#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

PYTHONPATH=src ./.venv/bin/python -m mgc_v05l.app.asia_drift_v1_phase1 "$@"
