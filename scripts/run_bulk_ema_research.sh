#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

BAR_COUNT="$(sqlite3 "${DB_PATH}" "select count(*) from bars where ticker = '${MGC_V05L_SETTINGS_SYMBOL}' and timeframe = '${MGC_V05L_SETTINGS_TIMEFRAME}';")"
if [[ "${BAR_COUNT}" -le 0 ]]; then
  echo "No persisted ${MGC_V05L_SETTINGS_SYMBOL} ${MGC_V05L_SETTINGS_TIMEFRAME} bars found in ${DB_PATH}." >&2
  exit 1
fi

echo "bars_count=${BAR_COUNT}"

ensure_signal_evaluations_structure_columns

export RUN_ID="$(
  "${PYTHON_BIN}" - <<'PY'
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.persistence.db import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.persistence.research_models import ExperimentRunRecord

settings = load_settings_from_files([
    Path("/Users/patrick/Documents/MGC-v05l-automation/config/base.yaml"),
    Path("/Users/patrick/Documents/MGC-v05l-automation/config/replay.yaml"),
])
repositories = RepositorySet(build_engine(settings.database_url))
run = repositories.experiment_runs.create(
    ExperimentRunRecord(
        name="real-data-ema-run-bulk",
        description="EMA research run after bulk Schwab backfill",
        started_at=datetime.now(ZoneInfo("UTC")),
        market_universe=settings.symbol,
        timeframe=settings.timeframe,
        feature_version="ema-v1",
        signal_version="ema-eval-v1",
        sizing_version="none",
    )
)
print(run.experiment_run_id)
PY
)"

echo "run_id=${RUN_ID}"

"${PYTHON_BIN}" - <<'PY'
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import os

from sqlalchemy import select

from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.domain.models import Bar
from mgc_v05l.persistence.db import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.persistence.tables import bars_table
from mgc_v05l.research import EMAMomentumResearchEvaluator, EMAMomentumResearchService, EMAStructureResearchLabeler

run_id = int(os.environ["RUN_ID"])
settings = load_settings_from_files([
    Path("/Users/patrick/Documents/MGC-v05l-automation/config/base.yaml"),
    Path("/Users/patrick/Documents/MGC-v05l-automation/config/replay.yaml"),
])
repositories = RepositorySet(build_engine(settings.database_url))

with repositories.engine.begin() as connection:
    rows = connection.execute(
        select(bars_table).where(
            bars_table.c.ticker == settings.symbol,
            bars_table.c.timeframe == settings.timeframe,
        ).order_by(bars_table.c.timestamp.asc())
    ).mappings().all()

bars = [
    Bar(
        bar_id=row["bar_id"],
        symbol=row["symbol"],
        timeframe=row["timeframe"],
        start_ts=datetime.fromisoformat(row["start_ts"]),
        end_ts=datetime.fromisoformat(row["end_ts"]),
        open=Decimal(str(row["open"])),
        high=Decimal(str(row["high"])),
        low=Decimal(str(row["low"])),
        close=Decimal(str(row["close"])),
        volume=int(row["volume"]),
        is_final=bool(row["is_final"]),
        session_asia=bool(row["session_asia"]),
        session_london=bool(row["session_london"]),
        session_us=bool(row["session_us"]),
        session_allowed=bool(row["session_allowed"]),
    )
    for row in rows
]

EMAMomentumResearchService(repositories=repositories, settings=settings).compute_and_persist(
    bars=bars,
    experiment_run_id=run_id,
)
EMAMomentumResearchEvaluator(repositories=repositories, volume_window=settings.vol_len).evaluate_and_persist(
    bars=bars,
    experiment_run_id=run_id,
)
EMAStructureResearchLabeler(repositories=repositories).label_and_persist(
    bars=bars,
    experiment_run_id=run_id,
)

print({"run_id": run_id, "bars_loaded": len(bars), "stage": "bulk_ema_research_complete"})
PY

sqlite3 "${DB_PATH}" "
select
  (select count(*) from bars where ticker = '${MGC_V05L_SETTINGS_SYMBOL}' and timeframe = '${MGC_V05L_SETTINGS_TIMEFRAME}') as bars_count,
  (select count(*) from experiment_runs) as experiment_runs_count,
  (select count(*) from derived_features where experiment_run_id = ${RUN_ID}) as derived_features_count,
  (select count(*) from signal_evaluations where experiment_run_id = ${RUN_ID}) as signal_evaluations_count,
  (select min(timestamp) from bars where ticker = '${MGC_V05L_SETTINGS_SYMBOL}' and timeframe = '${MGC_V05L_SETTINGS_TIMEFRAME}') as first_bar_ts,
  (select max(timestamp) from bars where ticker = '${MGC_V05L_SETTINGS_SYMBOL}' and timeframe = '${MGC_V05L_SETTINGS_TIMEFRAME}') as last_bar_ts;
"

REPORT_PATH="${REPORT_DIR}/ema_eval_report_run_${RUN_ID}.csv"
VIZ_PATH="${VIZ_DIR}/mgc_ema_viz_run_${RUN_ID}.html"

"${PYTHON_BIN}" -m mgc_v05l.app.main research-ema-eval-report \
  --config "${CONFIG_BASE}" \
  --config "${CONFIG_REPLAY}" \
  --experiment-run-id "${RUN_ID}" \
  --output "${REPORT_PATH}"

"${PYTHON_BIN}" -m mgc_v05l.app.main research-ema-viz \
  --config "${CONFIG_BASE}" \
  --config "${CONFIG_REPLAY}" \
  --experiment-run-id "${RUN_ID}" \
  --ticker "${MGC_V05L_SETTINGS_SYMBOL}" \
  --timeframe "${MGC_V05L_SETTINGS_TIMEFRAME}" \
  --output "${VIZ_PATH}"

echo "report_path=${REPORT_PATH}"
echo "viz_path=${VIZ_PATH}"
