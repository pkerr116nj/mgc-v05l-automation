#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

if [[ $# -lt 3 ]]; then
  echo "Usage: bash scripts/run_replay_derivative_ladder_summary.sh <output.csv> <output.json> <summary1> [<summary2> ...]" >&2
  exit 1
fi

export LADDER_OUTPUT_CSV="$1"
export LADDER_OUTPUT_JSON="$2"
shift 2

for summary_path in "$@"; do
  if [[ ! -f "${summary_path}" ]]; then
    echo "Replay summary not found: ${summary_path}" >&2
    exit 1
  fi
done

export LADDER_SUMMARY_PATHS="$(printf '%s\n' "$@")"

"${PYTHON_BIN}" - <<'PY'
from __future__ import annotations

import csv
import json
import os
from collections import Counter
from pathlib import Path


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_trade_counts(path: Path):
    by_direction: Counter[str] = Counter()
    by_signal_family: Counter[str] = Counter()
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            by_direction[row["direction"]] += 1
            by_signal_family[row["setup_family"]] += 1
    return dict(by_direction), dict(by_signal_family)


def load_breakdown(path: Path):
    values = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            values[row["bucket"]] = float(row["total_net_pnl"])
    return values


def load_us_short_participation(path: Path):
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


def variant_name_from_summary(path: Path) -> str:
    name = path.name.removesuffix(".summary.json")
    prefix = "persisted_bar_replay_"
    return name[len(prefix):] if name.startswith(prefix) else name


summary_paths = [Path(item) for item in os.environ["LADDER_SUMMARY_PATHS"].splitlines() if item.strip()]
output_csv = Path(os.environ["LADDER_OUTPUT_CSV"])
output_json = Path(os.environ["LADDER_OUTPUT_JSON"])

rows = []
json_rows = []

control_summary = load_json(summary_paths[0])
control_metrics = load_json(Path(control_summary["summary_metrics_path"]))
control_trade_counts_by_direction, control_trade_counts_by_signal = load_trade_counts(Path(control_summary["trade_ledger_path"]))
control_pnl_by_direction = load_breakdown(Path(control_summary["pnl_by_direction_path"]))
control_pnl_by_session = load_breakdown(Path(control_summary["pnl_by_session_path"]))
control_turn_counts = load_us_short_participation(summary_paths[0].with_suffix("").with_suffix(".turn_dataset.csv"))
control_derivative_trades = control_trade_counts_by_signal.get("usDerivativeBearTurn", 0)

for summary_path in summary_paths:
    summary = load_json(summary_path)
    metrics = load_json(Path(summary["summary_metrics_path"]))
    trade_counts_by_direction, trade_counts_by_signal = load_trade_counts(Path(summary["trade_ledger_path"]))
    pnl_by_direction = load_breakdown(Path(summary["pnl_by_direction_path"]))
    pnl_by_session = load_breakdown(Path(summary["pnl_by_session_path"]))
    turn_counts = load_us_short_participation(summary_path.with_suffix("").with_suffix(".turn_dataset.csv"))

    derivative_trades = trade_counts_by_signal.get("usDerivativeBearTurn", 0)
    participated = turn_counts.get("good_entry", 0) + turn_counts.get("late_entry", 0) + turn_counts.get("poor_entry", 0)
    control_participated = control_turn_counts.get("good_entry", 0) + control_turn_counts.get("late_entry", 0) + control_turn_counts.get("poor_entry", 0)

    row = {
        "variant": variant_name_from_summary(summary_path),
        "total_net_pnl": metrics["total_net_pnl"],
        "number_of_trades": metrics["number_of_trades"],
        "win_rate": metrics["win_rate"],
        "avg_winner": metrics["avg_winner"],
        "avg_loser": metrics["avg_loser"],
        "expectancy": metrics["expectancy"],
        "max_drawdown": metrics["max_drawdown"],
        "short_side_pnl": pnl_by_direction.get("SHORT", 0.0),
        "us_session_pnl": pnl_by_session.get("US", 0.0),
        "derivative_branch_trade_count": derivative_trades,
        "derivative_branch_pnl": metrics["pnl_by_signal_family"].get("usDerivativeBearTurn", 0.0),
        "trade_count_added_vs_control": metrics["number_of_trades"] - control_metrics["number_of_trades"],
        "short_side_pnl_delta_vs_control": pnl_by_direction.get("SHORT", 0.0) - control_pnl_by_direction.get("SHORT", 0.0),
        "us_session_pnl_delta_vs_control": pnl_by_session.get("US", 0.0) - control_pnl_by_session.get("US", 0.0),
        "drawdown_delta_vs_control": metrics["max_drawdown"] - control_metrics["max_drawdown"],
        "missed_bearish_turn_participation_delta_vs_control": participated - control_participated,
        "derivative_trade_delta_vs_control": derivative_trades - control_derivative_trades,
    }
    rows.append(row)
    json_rows.append(
        {
            "variant": row["variant"],
            "summary_path": str(summary_path),
            "summary_metrics_path": summary["summary_metrics_path"],
            "trade_ledger_path": summary["trade_ledger_path"],
            "pnl_by_signal_family_path": summary["pnl_by_signal_family_path"],
            "pnl_by_session_path": summary["pnl_by_session_path"],
            "pnl_by_direction_path": summary["pnl_by_direction_path"],
            "drawdown_curve_path": summary["drawdown_curve_path"],
            "equity_curve_path": summary["equity_curve_path"],
            "metrics": row,
        }
    )

output_csv.parent.mkdir(parents=True, exist_ok=True)
with output_csv.open("w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

output_json.parent.mkdir(parents=True, exist_ok=True)
output_json.write_text(json.dumps(json_rows, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps({"csv": str(output_csv), "json": str(output_json)}, sort_keys=True))
PY

if [[ ! -f "${LADDER_OUTPUT_CSV}" ]]; then
  echo "Expected ladder CSV was not created: ${LADDER_OUTPUT_CSV}" >&2
  exit 1
fi
if [[ ! -f "${LADDER_OUTPUT_JSON}" ]]; then
  echo "Expected ladder JSON was not created: ${LADDER_OUTPUT_JSON}" >&2
  exit 1
fi
