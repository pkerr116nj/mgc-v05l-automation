#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

require_schwab_auth_env

export INTERNAL_SYMBOLS="${INTERNAL_SYMBOLS:-$(active_schwab_symbols)}"
export NATIVE_TIMEFRAMES="${NATIVE_TIMEFRAMES:-1m,5m,10m,15m,30m,1440m}"
export BULK_START="${BULK_START:-2010-01-01}"
export BULK_END="${BULK_END:-}"
export DRY_RUN="${DRY_RUN:-0}"

"${PYTHON_BIN}" - <<'PY'
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import os
import sqlite3
import subprocess

from mgc_v05l.market_data.timeframes import normalize_timeframe_label, timeframe_minutes

repo = Path(os.environ["REPO_ROOT"])
python = Path(os.environ["PYTHON_BIN"])
db_path = Path(os.environ["DB_PATH"])
symbols = [symbol.strip() for symbol in os.environ["INTERNAL_SYMBOLS"].split(",") if symbol.strip()]
timeframes = sorted(
    {normalize_timeframe_label(value) for value in os.environ["NATIVE_TIMEFRAMES"].split(",") if value.strip()},
    key=timeframe_minutes,
)
start_date = datetime.fromisoformat(os.environ["BULK_START"]).replace(tzinfo=timezone.utc)
end_env = os.environ["BULK_END"].strip()
end_date = datetime.fromisoformat(end_env).replace(tzinfo=timezone.utc) if end_env else datetime.now(timezone.utc)
dry_run = os.environ["DRY_RUN"] == "1"


def coverage(symbol: str, timeframe: str) -> dict[str, str | int | None]:
    connection = sqlite3.connect(db_path)
    try:
        row = connection.execute(
            """
            select count(*), min(end_ts), max(end_ts)
            from bars
            where ticker = ? and timeframe = ? and data_source = 'schwab_history'
            """,
            (symbol, timeframe),
        ).fetchone()
    finally:
        connection.close()
    return {
        "bar_count": int(row[0] or 0),
        "first_bar_ts": row[1],
        "last_bar_ts": row[2],
    }


coverage_before = {
    symbol: {timeframe: coverage(symbol, timeframe) for timeframe in timeframes}
    for symbol in symbols
}

results: list[dict[str, object]] = []
for symbol in symbols:
    for timeframe in timeframes:
        period_type = "year" if timeframe == "1440m" else "day"
        cmd = [
            str(python), "-m", "mgc_v05l.app.main", "schwab-fetch-history",
            "--config", str(repo / "config/base.yaml"),
            "--config", str(repo / "config/replay.yaml"),
            "--schwab-config", str(repo / "config/schwab.local.json"),
            "--internal-symbol", symbol,
            "--internal-timeframe", timeframe,
            "--period-type", period_type,
            "--start-date-ms", str(int(start_date.timestamp() * 1000)),
            "--end-date-ms", str(int(end_date.timestamp() * 1000)),
            "--persist",
        ]
        print(f"Backfill native {symbol} {timeframe} {start_date.isoformat()} -> {end_date.isoformat()}", flush=True)
        if dry_run:
            print(json.dumps({"dry_run": True, "cmd": cmd}, default=str))
            results.append({"symbol": symbol, "timeframe": timeframe, "dry_run": True})
            continue
        completed = subprocess.run(
            cmd,
            check=True,
            cwd=repo,
            env={**os.environ, "PYTHONPATH": str(repo / "src")},
            capture_output=True,
            text=True,
        )
        payload = json.loads(completed.stdout.strip())
        results.append({"symbol": symbol, "timeframe": timeframe, **payload})

coverage_after = {
    symbol: {timeframe: coverage(symbol, timeframe) for timeframe in timeframes}
    for symbol in symbols
}

print(
    json.dumps(
        {
            "mode": "native_timeframe_backfill",
            "dry_run": dry_run,
            "symbols": symbols,
            "timeframes": timeframes,
            "requested_start": start_date.isoformat(),
            "requested_end": end_date.isoformat(),
            "results": results,
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
  --output-json "${REPORT_DIR}/market_data_status_after_native_backfill.json" \
  --output-csv "${REPORT_DIR}/market_data_status_after_native_backfill.csv"
