#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

require_schwab_auth_env

PYTHONPATH="${REPO_ROOT}/src" "${PYTHON_BIN}" -m mgc_v05l.app.replay_base_preservation --write-report --fail-on-regression

export BULK_START="${BULK_START:-2025-03-15}"
export BULK_END="${BULK_END:-2026-03-16}"
export CHUNK_DAYS="${CHUNK_DAYS:-14}"
export INTERNAL_SYMBOLS="${INTERNAL_SYMBOLS:-$(active_schwab_symbols)}"
export DRY_RUN="${DRY_RUN:-0}"

"${PYTHON_BIN}" - <<'PY'
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import os
import subprocess
import sqlite3

repo = Path(os.environ["REPO_ROOT"])
python = Path(os.environ["PYTHON_BIN"])
start_date = datetime.fromisoformat(os.environ["BULK_START"]).replace(tzinfo=timezone.utc)
end_date = datetime.fromisoformat(os.environ["BULK_END"]).replace(tzinfo=timezone.utc)
chunk_days = int(os.environ["CHUNK_DAYS"])
symbols = [symbol.strip() for symbol in os.environ["INTERNAL_SYMBOLS"].split(",") if symbol.strip()]
dry_run = os.environ["DRY_RUN"] == "1"
db_path = Path(os.environ["DB_PATH"])

def load_coverage(symbol: str) -> dict[str, str | int | None]:
    if not db_path.exists():
        return {
            "bar_count": 0,
            "first_bar_ts": None,
            "last_bar_ts": None,
        }
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
    except sqlite3.OperationalError:
        row = (0, None, None)
    finally:
        connection.close()
    return {
        "bar_count": int(row[0] or 0),
        "first_bar_ts": row[1],
        "last_bar_ts": row[2],
    }

coverage_before = {symbol: load_coverage(symbol) for symbol in symbols}

for symbol in symbols:
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days), end_date)
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
        print(f"Fetching {symbol} 1m {current.isoformat()} -> {chunk_end.isoformat()}", flush=True)
        if dry_run:
            print(json.dumps({"dry_run": True, "cmd": cmd}, default=str))
        else:
            completed = subprocess.run(
                cmd,
                check=True,
                cwd=repo,
                env={**os.environ, "PYTHONPATH": str(repo / "src")},
                capture_output=True,
                text=True,
            )
            payload = json.loads(completed.stdout)
            print(
                json.dumps(
                    {
                        "symbol": symbol,
                        "chunk_start": current.isoformat(),
                        "chunk_end": chunk_end.isoformat(),
                        "bar_count": int(payload.get("bar_count") or 0),
                        "persisted": bool(payload.get("persisted")),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        current = chunk_end

coverage_after = {symbol: load_coverage(symbol) for symbol in symbols}
print(
    json.dumps(
        {
            "mode": "backfill_1m",
            "dry_run": dry_run,
            "symbols": symbols,
            "requested_start": start_date.isoformat(),
            "requested_end": end_date.isoformat(),
            "chunk_days": chunk_days,
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
  --output-json "${REPORT_DIR}/market_data_status_after_backfill.json" \
  --output-csv "${REPORT_DIR}/market_data_status_after_backfill.csv"

PYTHONPATH="${REPO_ROOT}/src" "${PYTHON_BIN}" -m mgc_v05l.app.replay_base_preservation --write-report --fail-on-regression
