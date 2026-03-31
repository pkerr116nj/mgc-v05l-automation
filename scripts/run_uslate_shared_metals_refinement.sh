#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
PYTHONPATH=src .venv/bin/python -m mgc_v05l.app.uslate_pause_resume_long_shared_metals_refinement
