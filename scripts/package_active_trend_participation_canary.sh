#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m mgc_v05l.app.main \
  research-trend-participation-canary-package \
  --source-sqlite mgc_v05l.replay.sqlite3 \
  --output-dir outputs/probationary_quant_canaries/active_trend_participation_engine \
  --instruments MES MNQ
