#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
PYTHONPATH=src .venv/bin/python -m mgc_v05l.app.mgc_impulse_quant_rescue_pass "$@"
