#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

require_schwab_auth_env

export BULK_START="${BULK_START:-2025-03-15}"
export BULK_END="${BULK_END:-2026-03-15}"
export CHUNK_DAYS="${CHUNK_DAYS:-14}"

"${PYTHON_BIN}" - <<'PY'
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os
import subprocess

repo = Path(os.environ["REPO_ROOT"])
python = Path(os.environ["PYTHON_BIN"])
start_date = datetime.fromisoformat(os.environ["BULK_START"]).replace(tzinfo=timezone.utc)
end_date = datetime.fromisoformat(os.environ["BULK_END"]).replace(tzinfo=timezone.utc)
chunk_days = int(os.environ["CHUNK_DAYS"])

current = start_date
while current < end_date:
    chunk_end = min(current + timedelta(days=chunk_days), end_date)
    start_ms = int(current.timestamp() * 1000)
    end_ms = int(chunk_end.timestamp() * 1000)
    cmd = [
        str(python), "-m", "mgc_v05l.app.main", "schwab-fetch-history",
        "--config", str(repo / "config/base.yaml"),
        "--config", str(repo / "config/replay.yaml"),
        "--schwab-config", str(repo / "config/schwab.local.json"),
        "--internal-symbol", "MGC",
        "--period-type", "day",
        "--frequency-type", "minute",
        "--frequency", "5",
        "--start-date-ms", str(start_ms),
        "--end-date-ms", str(end_ms),
        "--persist",
    ]
    print(f"Fetching {current.isoformat()} -> {chunk_end.isoformat()}", flush=True)
    subprocess.run(cmd, check=True, cwd=repo, env={**os.environ, "PYTHONPATH": str(repo / "src")})
    current = chunk_end
PY

sqlite3 "${DB_PATH}" "
select
  count(*) as bars_count,
  min(timestamp) as first_bar_ts,
  max(timestamp) as last_bar_ts
from bars
where ticker = '${MGC_V05L_SETTINGS_SYMBOL}' and timeframe = '${MGC_V05L_SETTINGS_TIMEFRAME}';
"
