#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

if [[ $# -ne 3 ]]; then
  echo "Usage: bash scripts/run_replay_ablation_compare.sh <control.summary.json> <treatment.summary.json> <output.comparison.json>" >&2
  exit 1
fi

export CONTROL_SUMMARY_PATH="$1"
export TREATMENT_SUMMARY_PATH="$2"
export COMPARISON_OUTPUT_PATH="$3"

if [[ ! -f "${CONTROL_SUMMARY_PATH}" ]]; then
  echo "Control summary not found: ${CONTROL_SUMMARY_PATH}" >&2
  exit 1
fi
if [[ ! -f "${TREATMENT_SUMMARY_PATH}" ]]; then
  echo "Treatment summary not found: ${TREATMENT_SUMMARY_PATH}" >&2
  exit 1
fi

"${PYTHON_BIN}" - <<'PY'
from __future__ import annotations

import csv
import json
import os
from collections import Counter
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_trade_counts(path: Path) -> tuple[dict[str, int], dict[str, int]]:
    by_direction: Counter[str] = Counter()
    by_signal_family: Counter[str] = Counter()
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            by_direction[row["direction"]] += 1
            by_signal_family[row["setup_family"]] += 1
    return dict(by_direction), dict(by_signal_family)


def load_breakdown(path: Path) -> dict[str, float]:
    values: dict[str, float] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            values[row["bucket"]] = float(row["total_net_pnl"])
    return values


def load_us_short_turn_participation(path: Path) -> dict[str, int]:
    counts: Counter[str] = Counter()
    if not path.exists():
        return {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["material_turn"] != "True":
                continue
            if row["direction_of_turn"] != "SHORT":
                continue
            if row["session"] != "US":
                continue
            counts[row["participation_classification"]] += 1
    return dict(counts)


def value_delta(treatment: float, control: float) -> float:
    return treatment - control


control_summary_path = Path(os.environ["CONTROL_SUMMARY_PATH"])
treatment_summary_path = Path(os.environ["TREATMENT_SUMMARY_PATH"])
output_path = Path(os.environ["COMPARISON_OUTPUT_PATH"])

control_summary = load_json(control_summary_path)
treatment_summary = load_json(treatment_summary_path)
control_metrics = load_json(Path(control_summary["summary_metrics_path"]))
treatment_metrics = load_json(Path(treatment_summary["summary_metrics_path"]))
control_trade_counts_by_direction, control_trade_counts_by_signal = load_trade_counts(Path(control_summary["trade_ledger_path"]))
treatment_trade_counts_by_direction, treatment_trade_counts_by_signal = load_trade_counts(Path(treatment_summary["trade_ledger_path"]))
control_pnl_by_direction = load_breakdown(Path(control_summary["pnl_by_direction_path"]))
treatment_pnl_by_direction = load_breakdown(Path(treatment_summary["pnl_by_direction_path"]))
control_pnl_by_session = load_breakdown(Path(control_summary["pnl_by_session_path"]))
treatment_pnl_by_session = load_breakdown(Path(treatment_summary["pnl_by_session_path"]))
control_turn_counts = load_us_short_turn_participation(control_summary_path.with_suffix("").with_suffix(".turn_dataset.csv"))
treatment_turn_counts = load_us_short_turn_participation(treatment_summary_path.with_suffix("").with_suffix(".turn_dataset.csv"))

comparison = {
    "control_summary_path": str(control_summary_path),
    "treatment_summary_path": str(treatment_summary_path),
    "control": {
        "summary_metrics": control_metrics,
        "pnl_by_direction": control_pnl_by_direction,
        "pnl_by_session": control_pnl_by_session,
        "trade_counts_by_direction": control_trade_counts_by_direction,
        "trade_counts_by_signal_family": control_trade_counts_by_signal,
        "us_short_material_turn_participation": control_turn_counts,
    },
    "treatment": {
        "summary_metrics": treatment_metrics,
        "pnl_by_direction": treatment_pnl_by_direction,
        "pnl_by_session": treatment_pnl_by_session,
        "trade_counts_by_direction": treatment_trade_counts_by_direction,
        "trade_counts_by_signal_family": treatment_trade_counts_by_signal,
        "us_short_material_turn_participation": treatment_turn_counts,
    },
    "deltas": {
        "pnl_delta": value_delta(treatment_metrics["total_net_pnl"], control_metrics["total_net_pnl"]),
        "drawdown_delta": value_delta(treatment_metrics["max_drawdown"], control_metrics["max_drawdown"]),
        "short_side_pnl_delta": value_delta(
            treatment_pnl_by_direction.get("SHORT", 0.0),
            control_pnl_by_direction.get("SHORT", 0.0),
        ),
        "us_session_pnl_delta": value_delta(
            treatment_pnl_by_session.get("US", 0.0),
            control_pnl_by_session.get("US", 0.0),
        ),
        "trade_count_delta": value_delta(treatment_metrics["number_of_trades"], control_metrics["number_of_trades"]),
        "first_bear_snap_turn_trade_count_delta": value_delta(
            treatment_trade_counts_by_signal.get("firstBearSnapTurn", 0),
            control_trade_counts_by_signal.get("firstBearSnapTurn", 0),
        ),
        "derivative_bear_turn_trade_count_delta": value_delta(
            treatment_trade_counts_by_signal.get("usDerivativeBearTurn", 0),
            control_trade_counts_by_signal.get("usDerivativeBearTurn", 0),
        ),
        "derivative_branch_pnl_delta": value_delta(
            treatment_metrics["pnl_by_signal_family"].get("usDerivativeBearTurn", 0.0),
            control_metrics["pnl_by_signal_family"].get("usDerivativeBearTurn", 0.0),
        ),
        "missed_bearish_turn_participation_delta": value_delta(
            treatment_turn_counts.get("good_entry", 0) + treatment_turn_counts.get("late_entry", 0) + treatment_turn_counts.get("poor_entry", 0),
            control_turn_counts.get("good_entry", 0) + control_turn_counts.get("late_entry", 0) + control_turn_counts.get("poor_entry", 0),
        ),
    },
}

output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(json.dumps(comparison, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps(comparison, sort_keys=True))
PY

if [[ ! -f "${COMPARISON_OUTPUT_PATH}" ]]; then
  echo "Expected comparison artifact was not created: ${COMPARISON_OUTPUT_PATH}" >&2
  exit 1
fi
