#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHONPATH=src .venv/bin/python -m mgc_v05l.app.three_minute_derivative_separator_analysis \
  --db-path "$ROOT_DIR/mgc_v05l.replay.sqlite3" \
  --ticker MGC \
  --source-timeframe 1m \
  --strict-trade-ledger "$ROOT_DIR/outputs/replays/persisted_bar_replay_midday_widen_strict_full_20260316.trade_ledger.csv" \
  --medium-1-trade-ledger "$ROOT_DIR/outputs/replays/persisted_bar_replay_midday_widen_medium_1_full_20260316.trade_ledger.csv" \
  --medium-2-trade-ledger "$ROOT_DIR/outputs/replays/persisted_bar_replay_midday_widen_medium_2_full_20260316.trade_ledger.csv" \
  --london-detail-csv "$ROOT_DIR/outputs/replays/persisted_bar_replay_additive_lane_open_late_only_flat_neg_plus_neg_neg_full_20260316.missed_entry_discovery_detail.csv" \
  --reference-trade-ledger "$ROOT_DIR/outputs/replays/persisted_bar_replay_london_family_control_full_20260316.trade_ledger.csv" \
  --output-prefix "$ROOT_DIR/outputs/replays/mgc_3m_derivative_separator_20260316" \
  --config config/base.yaml \
  --config config/replay.yaml
