#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

BAR_COUNT="$(sqlite3 "${DB_PATH}" "select count(*) from bars where ticker = '${MGC_V05L_SETTINGS_SYMBOL}' and timeframe = '${MGC_V05L_SETTINGS_TIMEFRAME}';")"
if [[ "${BAR_COUNT}" -le 0 ]]; then
  echo "No persisted ${MGC_V05L_SETTINGS_SYMBOL} ${MGC_V05L_SETTINGS_TIMEFRAME} bars found in ${DB_PATH}." >&2
  exit 1
fi

RUN_STAMP="${RUN_STAMP:-$(date +%Y%m%d_%H%M%S)}"
REPLAY_DB_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.sqlite3"
SUMMARY_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.summary.json"
TRADE_LEDGER_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.trade_ledger.csv"
SUMMARY_METRICS_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.summary_metrics.json"
EQUITY_CURVE_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.equity_curve.csv"
PNL_BY_SIGNAL_FAMILY_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.pnl_by_signal_family.csv"
PNL_BY_SESSION_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.pnl_by_session.csv"
PNL_BY_DIRECTION_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.pnl_by_direction.csv"
DRAWDOWN_CURVE_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.drawdown_curve.csv"
ROLLING_PERFORMANCE_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.rolling_performance.csv"
MAE_MFE_SUMMARY_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.mae_mfe_summary.json"
HOLD_TIME_SUMMARY_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.hold_time_summary.json"
EXIT_REASON_BREAKDOWN_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.exit_reason_breakdown.csv"
TRADE_EFFICIENCY_BY_SIGNAL_FAMILY_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.trade_efficiency_by_signal_family.csv"
TRADE_EFFICIENCY_BY_SESSION_PATH="${REPLAY_DIR}/persisted_bar_replay_${RUN_STAMP}.trade_efficiency_by_session.csv"

export REPLAY_DB_PATH
export SUMMARY_PATH
export TRADE_LEDGER_PATH
export SUMMARY_METRICS_PATH
export EQUITY_CURVE_PATH
export PNL_BY_SIGNAL_FAMILY_PATH
export PNL_BY_SESSION_PATH
export PNL_BY_DIRECTION_PATH
export DRAWDOWN_CURVE_PATH
export ROLLING_PERFORMANCE_PATH
export MAE_MFE_SUMMARY_PATH
export HOLD_TIME_SUMMARY_PATH
export EXIT_REASON_BREAKDOWN_PATH
export TRADE_EFFICIENCY_BY_SIGNAL_FAMILY_PATH
export TRADE_EFFICIENCY_BY_SESSION_PATH

"${PYTHON_BIN}" - <<'PY'
from __future__ import annotations

import json
import os
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from time import perf_counter

from sqlalchemy import select

from mgc_v05l.app.replay_reporting import (
    build_breakdown_rows,
    build_drawdown_curve_rows,
    build_equity_curve_rows,
    build_exit_reason_breakdown_rows,
    build_hold_time_summary,
    build_mae_mfe_summary,
    build_rolling_performance_rows,
    build_session_lookup,
    build_summary_metrics,
    build_trade_efficiency_rows,
    ReplayFeatureContext,
    build_trade_ledger,
    write_breakdown_csv,
    write_dict_rows_csv,
    write_drawdown_curve_csv,
    write_equity_curve_csv,
    write_hold_time_summary_json,
    write_mae_mfe_summary_json,
    write_rolling_performance_csv,
    write_summary_metrics_json,
    write_trade_ledger_csv,
)
from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.domain.enums import OrderIntentType
from mgc_v05l.domain.events import FillReceivedEvent, OrderIntentCreatedEvent
from mgc_v05l.domain.models import Bar
from mgc_v05l.persistence.db import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.persistence.tables import bars_table, features_table
from mgc_v05l.strategy.strategy_engine import StrategyEngine

config_base = Path("/Users/patrick/Documents/MGC-v05l-automation/config/base.yaml")
config_replay = Path("/Users/patrick/Documents/MGC-v05l-automation/config/replay.yaml")
config_research_control = Path("/Users/patrick/Documents/MGC-v05l-automation/config/replay.research_control.yaml")
config_override_raw = os.environ.get("CONFIG_OVERRIDE")
config_paths = [config_base, config_replay, config_research_control]
if config_override_raw:
    config_paths.append(Path(config_override_raw))
source_db_path = Path(os.environ["DB_PATH"])
replay_db_path = Path(os.environ["REPLAY_DB_PATH"])
summary_path = Path(os.environ["SUMMARY_PATH"])
trade_ledger_path = Path(os.environ["TRADE_LEDGER_PATH"])
summary_metrics_path = Path(os.environ["SUMMARY_METRICS_PATH"])
equity_curve_path = Path(os.environ["EQUITY_CURVE_PATH"])
pnl_by_signal_family_path = Path(os.environ["PNL_BY_SIGNAL_FAMILY_PATH"])
pnl_by_session_path = Path(os.environ["PNL_BY_SESSION_PATH"])
pnl_by_direction_path = Path(os.environ["PNL_BY_DIRECTION_PATH"])
drawdown_curve_path = Path(os.environ["DRAWDOWN_CURVE_PATH"])
rolling_performance_path = Path(os.environ["ROLLING_PERFORMANCE_PATH"])
mae_mfe_summary_path = Path(os.environ["MAE_MFE_SUMMARY_PATH"])
hold_time_summary_path = Path(os.environ["HOLD_TIME_SUMMARY_PATH"])
exit_reason_breakdown_path = Path(os.environ["EXIT_REASON_BREAKDOWN_PATH"])
trade_efficiency_by_signal_family_path = Path(os.environ["TRADE_EFFICIENCY_BY_SIGNAL_FAMILY_PATH"])
trade_efficiency_by_session_path = Path(os.environ["TRADE_EFFICIENCY_BY_SESSION_PATH"])
point_value = Decimal(os.environ["REPLAY_POINT_VALUE"])
fee_per_fill = Decimal(os.environ["REPLAY_FEE_PER_FILL"])
slippage_per_fill = Decimal(os.environ["REPLAY_SLIPPAGE_PER_FILL"])
slice_start_raw = os.environ.get("REPLAY_SLICE_START_TS")
slice_end_raw = os.environ.get("REPLAY_SLICE_END_TS")

settings = load_settings_from_files(config_paths)
source_engine = build_engine(f"sqlite:///{source_db_path}")

with source_engine.begin() as connection:
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
if slice_start_raw:
    slice_start = datetime.fromisoformat(slice_start_raw)
    bars = [bar for bar in bars if bar.end_ts >= slice_start]
else:
    slice_start = None
if slice_end_raw:
    slice_end = datetime.fromisoformat(slice_end_raw)
    bars = [bar for bar in bars if bar.end_ts <= slice_end]
else:
    slice_end = None

replay_settings = settings.model_copy(update={"database_url": f"sqlite:///{replay_db_path}"})
repositories = RepositorySet(build_engine(replay_settings.database_url))
strategy_engine = StrategyEngine(settings=replay_settings, repositories=repositories)

event_counts: Counter[str] = Counter()
started = perf_counter()
for bar in bars:
    for event in strategy_engine.process_bar(bar):
        if isinstance(event, OrderIntentCreatedEvent):
            event_counts["order_intents"] += 1
            if event.intent_type == OrderIntentType.BUY_TO_OPEN:
                event_counts["long_entries"] += 1
            elif event.intent_type == OrderIntentType.SELL_TO_OPEN:
                event_counts["short_entries"] += 1
            else:
                event_counts["exits"] += 1
        elif isinstance(event, FillReceivedEvent):
            event_counts["fills"] += 1

final_state = strategy_engine.state
runtime_seconds = perf_counter() - started
session_by_start_ts = build_session_lookup(bars)
def _deserialize_value(value):
    if not isinstance(value, dict) or "__type__" not in value:
        return value
    if value["__type__"] == "decimal":
        return Decimal(value["value"])
    if value["__type__"] == "datetime":
        return datetime.fromisoformat(value["value"])
    if value["__type__"] == "enum":
        return value["value"]
    return value

feature_context_by_bar_id = {}
with repositories.engine.begin() as connection:
    feature_rows = connection.execute(select(features_table)).mappings().all()
for row in feature_rows:
    payload_raw = json.loads(row["payload_json"])
    payload = {key: _deserialize_value(value) for key, value in payload_raw.items()}
    feature_context_by_bar_id[row["bar_id"]] = ReplayFeatureContext(
        atr=payload["atr"],
        turn_ema_fast=payload["turn_ema_fast"],
        turn_ema_slow=payload["turn_ema_slow"],
        vwap=payload["vwap"],
    )
trade_ledger = build_trade_ledger(
    repositories.order_intents.list_all(),
    repositories.fills.list_all(),
    session_by_start_ts,
    point_value=point_value,
    fee_per_fill=fee_per_fill,
    slippage_per_fill=slippage_per_fill,
    bars=bars,
    feature_context_by_bar_id=feature_context_by_bar_id,
)
summary_metrics = build_summary_metrics(trade_ledger)
mae_mfe_summary = build_mae_mfe_summary(trade_ledger)
hold_time_summary = build_hold_time_summary(trade_ledger)
equity_curve_rows = build_equity_curve_rows(trade_ledger)
drawdown_curve_rows = build_drawdown_curve_rows(trade_ledger)
rolling_performance_rows = build_rolling_performance_rows(trade_ledger, window_size=20)
signal_family_rows = build_breakdown_rows(trade_ledger, key_name="setup_family")
session_rows = build_breakdown_rows(trade_ledger, key_name="entry_session")
direction_rows = build_breakdown_rows(trade_ledger, key_name="direction")
exit_reason_rows = build_exit_reason_breakdown_rows(trade_ledger)
efficiency_by_signal_family_rows = build_trade_efficiency_rows(trade_ledger, key_name="setup_family")
efficiency_by_session_rows = build_trade_efficiency_rows(trade_ledger, key_name="entry_session")
write_trade_ledger_csv(trade_ledger, trade_ledger_path)
write_summary_metrics_json(
    summary_metrics,
    summary_metrics_path,
    point_value=point_value,
    fee_per_fill=fee_per_fill,
    slippage_per_fill=slippage_per_fill,
)
write_mae_mfe_summary_json(mae_mfe_summary, mae_mfe_summary_path)
write_hold_time_summary_json(hold_time_summary, hold_time_summary_path)
write_equity_curve_csv(equity_curve_rows, equity_curve_path)
write_breakdown_csv(signal_family_rows, pnl_by_signal_family_path)
write_breakdown_csv(session_rows, pnl_by_session_path)
write_breakdown_csv(direction_rows, pnl_by_direction_path)
write_breakdown_csv(exit_reason_rows, exit_reason_breakdown_path)
write_dict_rows_csv(efficiency_by_signal_family_rows, trade_efficiency_by_signal_family_path)
write_dict_rows_csv(efficiency_by_session_rows, trade_efficiency_by_session_path)
write_drawdown_curve_csv(drawdown_curve_rows, drawdown_curve_path)
write_rolling_performance_csv(rolling_performance_rows, rolling_performance_path)
summary = {
    "source_db_path": str(source_db_path),
    "config_paths": [str(path) for path in config_paths],
    "replay_db_path": str(replay_db_path),
    "summary_path": str(summary_path),
    "trade_ledger_path": str(trade_ledger_path),
    "summary_metrics_path": str(summary_metrics_path),
    "equity_curve_path": str(equity_curve_path),
    "pnl_by_signal_family_path": str(pnl_by_signal_family_path),
    "pnl_by_session_path": str(pnl_by_session_path),
    "pnl_by_direction_path": str(pnl_by_direction_path),
    "drawdown_curve_path": str(drawdown_curve_path),
    "rolling_performance_path": str(rolling_performance_path),
    "mae_mfe_summary_path": str(mae_mfe_summary_path),
    "hold_time_summary_path": str(hold_time_summary_path),
    "exit_reason_breakdown_path": str(exit_reason_breakdown_path),
    "trade_efficiency_by_signal_family_path": str(trade_efficiency_by_signal_family_path),
    "trade_efficiency_by_session_path": str(trade_efficiency_by_session_path),
    "runtime_seconds": runtime_seconds,
    "slice_start_ts": slice_start.isoformat() if slice_start is not None else None,
    "slice_end_ts": slice_end.isoformat() if slice_end is not None else None,
    "source_bar_count": len(bars),
    "source_first_bar_ts": bars[0].end_ts.isoformat() if bars else None,
    "source_last_bar_ts": bars[-1].end_ts.isoformat() if bars else None,
    "processed_bars": repositories.processed_bars.count(),
    "order_intents": event_counts["order_intents"],
    "fills": event_counts["fills"],
    "long_entries": event_counts["long_entries"],
    "short_entries": event_counts["short_entries"],
    "exits": event_counts["exits"],
    "closed_trades": len(trade_ledger),
    "final_position_side": final_state.position_side.value,
    "final_strategy_status": final_state.strategy_status.value,
}

summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps(summary, sort_keys=True))
PY

if [[ ! -f "${REPLAY_DB_PATH}" ]]; then
  echo "Expected replay DB artifact was not created: ${REPLAY_DB_PATH}" >&2
  exit 1
fi

if [[ ! -f "${SUMMARY_PATH}" ]]; then
  echo "Expected replay summary artifact was not created: ${SUMMARY_PATH}" >&2
  exit 1
fi

if [[ ! -f "${TRADE_LEDGER_PATH}" ]]; then
  echo "Expected trade ledger artifact was not created: ${TRADE_LEDGER_PATH}" >&2
  exit 1
fi

if [[ ! -f "${SUMMARY_METRICS_PATH}" ]]; then
  echo "Expected summary metrics artifact was not created: ${SUMMARY_METRICS_PATH}" >&2
  exit 1
fi

if [[ ! -f "${EQUITY_CURVE_PATH}" ]]; then
  echo "Expected equity curve artifact was not created: ${EQUITY_CURVE_PATH}" >&2
  exit 1
fi

if [[ ! -f "${PNL_BY_SIGNAL_FAMILY_PATH}" ]]; then
  echo "Expected signal-family breakdown artifact was not created: ${PNL_BY_SIGNAL_FAMILY_PATH}" >&2
  exit 1
fi

if [[ ! -f "${PNL_BY_SESSION_PATH}" ]]; then
  echo "Expected session breakdown artifact was not created: ${PNL_BY_SESSION_PATH}" >&2
  exit 1
fi

if [[ ! -f "${PNL_BY_DIRECTION_PATH}" ]]; then
  echo "Expected direction breakdown artifact was not created: ${PNL_BY_DIRECTION_PATH}" >&2
  exit 1
fi

if [[ ! -f "${DRAWDOWN_CURVE_PATH}" ]]; then
  echo "Expected drawdown curve artifact was not created: ${DRAWDOWN_CURVE_PATH}" >&2
  exit 1
fi

if [[ ! -f "${ROLLING_PERFORMANCE_PATH}" ]]; then
  echo "Expected rolling performance artifact was not created: ${ROLLING_PERFORMANCE_PATH}" >&2
  exit 1
fi

if [[ ! -f "${MAE_MFE_SUMMARY_PATH}" ]]; then
  echo "Expected MAE/MFE summary artifact was not created: ${MAE_MFE_SUMMARY_PATH}" >&2
  exit 1
fi

if [[ ! -f "${HOLD_TIME_SUMMARY_PATH}" ]]; then
  echo "Expected hold-time summary artifact was not created: ${HOLD_TIME_SUMMARY_PATH}" >&2
  exit 1
fi

if [[ ! -f "${EXIT_REASON_BREAKDOWN_PATH}" ]]; then
  echo "Expected exit-reason breakdown artifact was not created: ${EXIT_REASON_BREAKDOWN_PATH}" >&2
  exit 1
fi

if [[ ! -f "${TRADE_EFFICIENCY_BY_SIGNAL_FAMILY_PATH}" ]]; then
  echo "Expected signal-family efficiency artifact was not created: ${TRADE_EFFICIENCY_BY_SIGNAL_FAMILY_PATH}" >&2
  exit 1
fi

if [[ ! -f "${TRADE_EFFICIENCY_BY_SESSION_PATH}" ]]; then
  echo "Expected session efficiency artifact was not created: ${TRADE_EFFICIENCY_BY_SESSION_PATH}" >&2
  exit 1
fi

echo "replay_db_path=${REPLAY_DB_PATH}"
echo "summary_path=${SUMMARY_PATH}"
echo "trade_ledger_path=${TRADE_LEDGER_PATH}"
echo "summary_metrics_path=${SUMMARY_METRICS_PATH}"
echo "equity_curve_path=${EQUITY_CURVE_PATH}"
echo "pnl_by_signal_family_path=${PNL_BY_SIGNAL_FAMILY_PATH}"
echo "pnl_by_session_path=${PNL_BY_SESSION_PATH}"
echo "pnl_by_direction_path=${PNL_BY_DIRECTION_PATH}"
echo "drawdown_curve_path=${DRAWDOWN_CURVE_PATH}"
echo "rolling_performance_path=${ROLLING_PERFORMANCE_PATH}"
echo "mae_mfe_summary_path=${MAE_MFE_SUMMARY_PATH}"
echo "hold_time_summary_path=${HOLD_TIME_SUMMARY_PATH}"
echo "exit_reason_breakdown_path=${EXIT_REASON_BREAKDOWN_PATH}"
echo "trade_efficiency_by_signal_family_path=${TRADE_EFFICIENCY_BY_SIGNAL_FAMILY_PATH}"
echo "trade_efficiency_by_session_path=${TRADE_EFFICIENCY_BY_SESSION_PATH}"
