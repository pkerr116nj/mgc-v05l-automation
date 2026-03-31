#!/usr/bin/env bash
set -euo pipefail

cd /Users/patrick/Documents/MGC-v05l-automation
PYTHONPATH=src .venv/bin/python - <<'PY'
from mgc_v05l.app.us_hours_candidate_review import build_and_write_us_hours_candidate_review

paths = build_and_write_us_hours_candidate_review()
for key, value in paths.items():
    print(f"{key}: {value}")
PY
