#!/usr/bin/env bash
set -euo pipefail

cd /Users/patrick/Documents/MGC-v05l-automation
PYTHONPATH=src .venv/bin/python - <<'PY'
from mgc_v05l.app.index_futures_candidate_triage import build_and_write_index_futures_candidate_triage

paths = build_and_write_index_futures_candidate_triage()
for key, value in paths.items():
    print(f"{key}: {value}")
PY

