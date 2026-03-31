#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

require_schwab_auth_env

export INTERNAL_SYMBOLS="${INTERNAL_SYMBOLS:-$(active_schwab_symbols)}"
export CHUNK_DAYS="${CHUNK_DAYS:-5}"
export LOOKBACK_DAYS_IF_EMPTY="${LOOKBACK_DAYS_IF_EMPTY:-7}"
export OVERLAP_MINUTES="${OVERLAP_MINUTES:-180}"
export DRY_RUN="${DRY_RUN:-0}"

"${PYTHON_BIN}" - <<'PY'
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import os
import sqlite3
import subprocess

repo = Path(os.environ["REPO_ROOT"])
python = Path(os.environ["PYTHON_BIN"])
db_path = Path(os.environ["DB_PATH"])
symbols = [symbol.strip() for symbol in os.environ["INTERNAL_SYMBOLS"].split(",") if symbol.strip()]
chunk_days = int(os.environ["CHUNK_DAYS"])
lookback_days_if_empty = int(os.environ["LOOKBACK_DAYS_IF_EMPTY"])
overlap_minutes = int(os.environ["OVERLAP_MINUTES"])
dry_run = os.environ["DRY_RUN"] == "1"
now_utc = datetime.now(timezone.utc)

def latest_bar(symbol: str) -> str | None:
    connection = sqlite3.connect(db_path)
    try:
        row = connection.execute(
            """
            select max(end_ts)
            from bars
            where ticker = ? and timeframe = '1m' and data_source = 'schwab_history'
            """,
            (symbol,),
        ).fetchone()
    finally:
        connection.close()
    return row[0]

def coverage(symbol: str) -> dict[str, str | int | None]:
    connection = sqlite3.connect(db_path)
    try:
        row = connection.execute(
            """
            select count(*), min(end_ts), max(end_ts)
            from bars
            where ticker = ? and timeframe = '1m' and data_source = 'schwab_history'
            """,
            (symbol,),
        ).fetchone()
    finally:
        connection.close()
    return {
        "bar_count": int(row[0] or 0),
        "first_bar_ts": row[1],
        "last_bar_ts": row[2],
    }

coverage_before = {symbol: coverage(symbol) for symbol in symbols}

for symbol in symbols:
    last_bar_ts = latest_bar(symbol)
    if last_bar_ts is None:
        current = now_utc - timedelta(days=lookback_days_if_empty)
    else:
        current = datetime.fromisoformat(last_bar_ts).astimezone(timezone.utc) - timedelta(minutes=overlap_minutes)
    current = max(current, now_utc - timedelta(days=3650))
    while current < now_utc:
        chunk_end = min(current + timedelta(days=chunk_days), now_utc)
        cmd = [
            str(python), "-m", "mgc_v05l.app.main", "schwab-fetch-history",
            "--config", str(repo / "config/base.yaml"),
            "--config", str(repo / "config/replay.yaml"),
            "--schwab-config", str(repo / "config/schwab.local.json"),
            "--internal-symbol", symbol,
            "--internal-timeframe", "1m",
            "--period-type", "day",
            "--frequency-type", "minute",
            "--frequency", "1",
            "--start-date-ms", str(int(current.timestamp() * 1000)),
            "--end-date-ms", str(int(chunk_end.timestamp() * 1000)),
            "--persist",
        ]
        print(f"Daily sync {symbol} 1m {current.isoformat()} -> {chunk_end.isoformat()}", flush=True)
        if dry_run:
            print(json.dumps({"dry_run": True, "cmd": cmd}, default=str))
        else:
            subprocess.run(cmd, check=True, cwd=repo, env={**os.environ, "PYTHONPATH": str(repo / "src")})
        current = chunk_end

coverage_after = {symbol: coverage(symbol) for symbol in symbols}
print(
    json.dumps(
        {
            "mode": "daily_sync_1m",
            "dry_run": dry_run,
            "symbols": symbols,
            "chunk_days": chunk_days,
            "lookback_days_if_empty": lookback_days_if_empty,
            "overlap_minutes": overlap_minutes,
            "sync_end_utc": now_utc.isoformat(),
            "coverage_before": coverage_before,
            "coverage_after": coverage_after,
        },
        indent=2,
        sort_keys=True,
    )
)
PY

"${PYTHON_BIN}" -m mgc_v05l.app.market_data_status_report \
  --db-path "${DB_PATH}" \
  --symbol-config "${SCHWAB_CONFIG}" \
  --output-json "${REPORT_DIR}/market_data_status_after_daily_sync.json" \
  --output-csv "${REPORT_DIR}/market_data_status_after_daily_sync.csv"
