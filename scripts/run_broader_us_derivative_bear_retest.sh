#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

RUN_TAG="${RUN_TAG:-$(date +%Y%m%d_%H%M%S)}"
REFERENCE_RUN_TAG="${REFERENCE_RUN_TAG:-derivative_exit_pass_20260316}"
DRAW_TOLERANCE="${PROMOTION_DRAWDOWN_TOLERANCE:-50}"
SLICE_FILE="$(mktemp)"
RESULTS_FILE="$(mktemp)"

cleanup() {
  rm -f "${SLICE_FILE}" "${RESULTS_FILE}"
}
trap cleanup EXIT

PROMOTED_CONTROL_SUMMARY="${REPLAY_DIR}/persisted_bar_replay_derivative_exit_${REFERENCE_RUN_TAG}_control_full.summary.json"
LONGER_TIME_FULL_SUMMARY="${REPLAY_DIR}/persisted_bar_replay_derivative_exit_${REFERENCE_RUN_TAG}_longer_time_anchor_full.summary.json"
LONGER_TIME_PLUS_FULL_SUMMARY="${REPLAY_DIR}/persisted_bar_replay_derivative_exit_${REFERENCE_RUN_TAG}_longer_time_plus_derivative_short_full.summary.json"

for path in \
  "${PROMOTED_CONTROL_SUMMARY}" \
  "${LONGER_TIME_FULL_SUMMARY}" \
  "${REPLAY_DIR}/persisted_bar_replay_derivative_exit_${REFERENCE_RUN_TAG}_longer_time_anchor_early.summary.json" \
  "${REPLAY_DIR}/persisted_bar_replay_derivative_exit_${REFERENCE_RUN_TAG}_longer_time_anchor_middle.summary.json" \
  "${REPLAY_DIR}/persisted_bar_replay_derivative_exit_${REFERENCE_RUN_TAG}_longer_time_anchor_recent.summary.json"; do
  if [[ ! -f "${path}" ]]; then
    echo "Reference replay summary not found: ${path}" >&2
    exit 1
  fi
done

python3 - <<'PY' > "${SLICE_FILE}"
import os
import sqlite3

db_path = os.environ["DB_PATH"]
symbol = os.environ["MGC_V05L_SETTINGS_SYMBOL"]
timeframe = os.environ["MGC_V05L_SETTINGS_TIMEFRAME"]

with sqlite3.connect(db_path) as connection:
    rows = connection.execute(
        """
        select timestamp
        from bars
        where ticker = ? and timeframe = ?
        order by timestamp asc
        """,
        (symbol, timeframe),
    ).fetchall()

timestamps = [row[0] for row in rows]
count = len(timestamps)
if count < 9:
    raise SystemExit("Need at least 9 bars for early/middle/recent slices.")

first_end = timestamps[(count // 3) - 1]
second_start = timestamps[count // 3]
second_end = timestamps[((2 * count) // 3) - 1]
third_start = timestamps[(2 * count) // 3]

print("slice_name,start_ts,end_ts")
print(f"early,,{first_end}")
print(f"middle,{second_start},{second_end}")
print(f"recent,{third_start},")
PY

declare -a VARIANTS=(
  "widen_1:${REPO_ROOT}/config/replay.retest_us_derivative_bear_widen_1.yaml"
  "widen_2:${REPO_ROOT}/config/replay.retest_us_derivative_bear_widen_2.yaml"
  "widen_3:${REPO_ROOT}/config/replay.retest_us_derivative_bear_widen_3.yaml"
)

echo "variant,slice_name,summary_path" > "${RESULTS_FILE}"

run_variant() {
  local variant_name="$1"
  local config_override="$2"
  local slice_name="$3"
  local slice_start="$4"
  local slice_end="$5"
  local stamp="us_derivative_bear_retest_${RUN_TAG}_${variant_name}_${slice_name}"
  local summary_path="${REPLAY_DIR}/persisted_bar_replay_${stamp}.summary.json"

  CONFIG_OVERRIDE="${config_override}" \
  REPLAY_SLICE_START_TS="${slice_start}" \
  REPLAY_SLICE_END_TS="${slice_end}" \
  RUN_STAMP="${stamp}" \
  bash "${REPO_ROOT}/scripts/run_persisted_bar_replay.sh"

  echo "${variant_name},${slice_name},${summary_path}" >> "${RESULTS_FILE}"
}

for variant in "${VARIANTS[@]}"; do
  variant_name="${variant%%:*}"
  config_override="${variant#*:}"
  run_variant "${variant_name}" "${config_override}" "full" "" ""
  bash "${REPO_ROOT}/scripts/run_replay_turn_research.sh" \
    "${REPLAY_DIR}/persisted_bar_replay_us_derivative_bear_retest_${RUN_TAG}_${variant_name}_full.summary.json"
  tail -n +2 "${SLICE_FILE}" | while IFS=, read -r slice_name slice_start slice_end; do
    run_variant "${variant_name}" "${config_override}" "${slice_name}" "${slice_start}" "${slice_end}"
  done
done

for reference_summary in \
  "${PROMOTED_CONTROL_SUMMARY}" \
  "${LONGER_TIME_FULL_SUMMARY}" \
  "${LONGER_TIME_PLUS_FULL_SUMMARY}"; do
  if [[ -f "${reference_summary}" ]]; then
    bash "${REPO_ROOT}/scripts/run_replay_turn_research.sh" "${reference_summary}"
  fi
done

for variant in widen_1 widen_2 widen_3; do
  bash "${REPO_ROOT}/scripts/run_replay_ablation_compare.sh" \
    "${PROMOTED_CONTROL_SUMMARY}" \
    "${REPLAY_DIR}/persisted_bar_replay_us_derivative_bear_retest_${RUN_TAG}_${variant}_full.summary.json" \
    "${REPLAY_DIR}/persisted_bar_replay_us_derivative_bear_retest_${RUN_TAG}_${variant}_vs_promoted_control.comparison.json"

  bash "${REPO_ROOT}/scripts/run_replay_ablation_compare.sh" \
    "${LONGER_TIME_FULL_SUMMARY}" \
    "${REPLAY_DIR}/persisted_bar_replay_us_derivative_bear_retest_${RUN_TAG}_${variant}_full.summary.json" \
    "${REPLAY_DIR}/persisted_bar_replay_us_derivative_bear_retest_${RUN_TAG}_${variant}_vs_longer_time_anchor.comparison.json"

  if [[ -f "${LONGER_TIME_PLUS_FULL_SUMMARY}" ]]; then
    bash "${REPO_ROOT}/scripts/run_replay_ablation_compare.sh" \
      "${LONGER_TIME_PLUS_FULL_SUMMARY}" \
      "${REPLAY_DIR}/persisted_bar_replay_us_derivative_bear_retest_${RUN_TAG}_${variant}_full.summary.json" \
      "${REPLAY_DIR}/persisted_bar_replay_us_derivative_bear_retest_${RUN_TAG}_${variant}_vs_longer_time_plus_derivative_short.comparison.json"
  fi
done

export RUN_TAG
export RESULTS_FILE
export DRAW_TOLERANCE
export PROMOTED_CONTROL_SUMMARY
export LONGER_TIME_FULL_SUMMARY
export LONGER_TIME_PLUS_FULL_SUMMARY
export REFERENCE_RUN_TAG

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
    by_session: Counter[str] = Counter()
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            by_direction[row["direction"]] += 1
            by_signal_family[row["setup_family"]] += 1
            by_session[row["entry_session"]] += 1
    return dict(by_direction), dict(by_signal_family), dict(by_session)


def load_breakdown(path: Path):
    values = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            values[row["bucket"]] = float(row["total_net_pnl"])
    return values


def load_us_short_participation(summary_path: Path):
    counts: Counter[str] = Counter()
    turn_dataset_path = summary_path.with_suffix("").with_suffix(".turn_dataset.csv")
    if not turn_dataset_path.exists():
        return {}
    with turn_dataset_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["material_turn"] != "True":
                continue
            if row["direction_of_turn"] != "SHORT":
                continue
            if row["session"] != "US":
                continue
            counts[row["participation_classification"]] += 1
    return dict(counts)


def material_participation_count(counts: dict[str, int]) -> int:
    return counts.get("good_entry", 0) + counts.get("late_entry", 0) + counts.get("poor_entry", 0)


run_tag = os.environ["RUN_TAG"]
draw_tolerance = float(os.environ["DRAW_TOLERANCE"])
results_file = Path(os.environ["RESULTS_FILE"])
promoted_control_summary_path = Path(os.environ["PROMOTED_CONTROL_SUMMARY"])
longer_time_summary_path = Path(os.environ["LONGER_TIME_FULL_SUMMARY"])
longer_time_plus_summary_path = Path(os.environ["LONGER_TIME_PLUS_FULL_SUMMARY"])
reference_run_tag = os.environ["REFERENCE_RUN_TAG"]
replay_dir = Path("/Users/patrick/Documents/MGC-v05l-automation/outputs/replays")
output_csv = replay_dir / f"us_derivative_bear_retest_{run_tag}.ranked.csv"
output_json = replay_dir / f"us_derivative_bear_retest_{run_tag}.ranked.json"

promoted_control_summary = load_json(promoted_control_summary_path)
promoted_control_metrics = load_json(Path(promoted_control_summary["summary_metrics_path"]))
promoted_control_pnl_by_direction = load_breakdown(Path(promoted_control_summary["pnl_by_direction_path"]))
promoted_control_pnl_by_session = load_breakdown(Path(promoted_control_summary["pnl_by_session_path"]))
promoted_control_trade_counts = load_trade_counts(Path(promoted_control_summary["trade_ledger_path"]))
promoted_control_turn_counts = load_us_short_participation(promoted_control_summary_path)

longer_time_summary = load_json(longer_time_summary_path)
longer_time_metrics = load_json(Path(longer_time_summary["summary_metrics_path"]))
longer_time_pnl_by_direction = load_breakdown(Path(longer_time_summary["pnl_by_direction_path"]))
longer_time_pnl_by_session = load_breakdown(Path(longer_time_summary["pnl_by_session_path"]))
longer_time_trade_counts = load_trade_counts(Path(longer_time_summary["trade_ledger_path"]))
longer_time_turn_counts = load_us_short_participation(longer_time_summary_path)
longer_time_mae = load_json(Path(longer_time_summary["mae_mfe_summary_path"]))
longer_time_hold = load_json(Path(longer_time_summary["hold_time_summary_path"]))

longer_time_plus = None
if longer_time_plus_summary_path.exists():
    longer_time_plus_summary = load_json(longer_time_plus_summary_path)
    longer_time_plus = {
        "summary_path": str(longer_time_plus_summary_path),
        "metrics": load_json(Path(longer_time_plus_summary["summary_metrics_path"])),
        "pnl_by_direction": load_breakdown(Path(longer_time_plus_summary["pnl_by_direction_path"])),
        "pnl_by_session": load_breakdown(Path(longer_time_plus_summary["pnl_by_session_path"])),
        "trade_counts": load_trade_counts(Path(longer_time_plus_summary["trade_ledger_path"])),
    }

rows = list(csv.DictReader(results_file.open(encoding="utf-8", newline="")))
by_variant: dict[str, dict[str, dict]] = {}
for row in rows:
    summary = load_json(Path(row["summary_path"]))
    by_variant.setdefault(row["variant"], {})[row["slice_name"]] = {
        "summary": summary,
        "metrics": load_json(Path(summary["summary_metrics_path"])),
        "mae": load_json(Path(summary["mae_mfe_summary_path"])),
        "hold": load_json(Path(summary["hold_time_summary_path"])),
        "pnl_by_direction": load_breakdown(Path(summary["pnl_by_direction_path"])),
        "pnl_by_session": load_breakdown(Path(summary["pnl_by_session_path"])),
        "trade_counts": load_trade_counts(Path(summary["trade_ledger_path"])),
        "turn_counts": load_us_short_participation(Path(row["summary_path"])) if row["slice_name"] == "full" else {},
    }

reference_slice_metrics = {
    "early": load_json(Path(replay_dir / f"persisted_bar_replay_derivative_exit_{reference_run_tag}_longer_time_anchor_early.summary_metrics.json")),
    "middle": load_json(Path(replay_dir / f"persisted_bar_replay_derivative_exit_{reference_run_tag}_longer_time_anchor_middle.summary_metrics.json")),
    "recent": load_json(Path(replay_dir / f"persisted_bar_replay_derivative_exit_{reference_run_tag}_longer_time_anchor_recent.summary_metrics.json")),
}

ranked_rows = []
for variant_name, variant_rows in by_variant.items():
    full = variant_rows["full"]
    metrics = full["metrics"]
    pnl_by_direction = full["pnl_by_direction"]
    pnl_by_session = full["pnl_by_session"]
    trade_counts_by_direction, trade_counts_by_signal, trade_counts_by_session = full["trade_counts"]
    derivative_trade_count = trade_counts_by_signal.get("usDerivativeBearTurn", 0)
    derivative_branch_pnl = metrics["pnl_by_signal_family"].get("usDerivativeBearTurn", 0.0)
    slice_improvement_count = 0
    slice_rows = {}
    for slice_name in ("early", "middle", "recent"):
        variant_slice = variant_rows[slice_name]["metrics"]
        anchor_slice = reference_slice_metrics[slice_name]
        improved = variant_slice["total_net_pnl"] > anchor_slice["total_net_pnl"]
        if improved:
            slice_improvement_count += 1
        slice_rows[slice_name] = {
            "variant_total_net_pnl": variant_slice["total_net_pnl"],
            "longer_time_anchor_total_net_pnl": anchor_slice["total_net_pnl"],
            "improved_vs_anchor": improved,
        }

    keep = (
        metrics["total_net_pnl"] > longer_time_metrics["total_net_pnl"]
        and metrics["expectancy"] >= longer_time_metrics["expectancy"]
        and metrics["max_drawdown"] <= longer_time_metrics["max_drawdown"] + draw_tolerance
        and slice_improvement_count >= 2
    )

    row = {
        "variant": variant_name,
        "verdict": "KEEP" if keep else "REJECT",
        "slice_improvement_count": slice_improvement_count,
        "total_net_pnl": metrics["total_net_pnl"],
        "number_of_trades": metrics["number_of_trades"],
        "win_rate": metrics["win_rate"],
        "avg_winner": metrics["avg_winner"],
        "avg_loser": metrics["avg_loser"],
        "expectancy": metrics["expectancy"],
        "max_drawdown": metrics["max_drawdown"],
        "control_pnl_delta": metrics["total_net_pnl"] - promoted_control_metrics["total_net_pnl"],
        "anchor_pnl_delta": metrics["total_net_pnl"] - longer_time_metrics["total_net_pnl"],
        "anchor_expectancy_delta": metrics["expectancy"] - longer_time_metrics["expectancy"],
        "anchor_drawdown_delta": metrics["max_drawdown"] - longer_time_metrics["max_drawdown"],
        "short_side_pnl_delta_vs_anchor": pnl_by_direction.get("SHORT", 0.0) - longer_time_pnl_by_direction.get("SHORT", 0.0),
        "us_session_pnl_delta_vs_anchor": pnl_by_session.get("US", 0.0) - longer_time_pnl_by_session.get("US", 0.0),
        "derivative_trade_count": derivative_trade_count,
        "derivative_trade_delta_vs_anchor": derivative_trade_count - longer_time_trade_counts[1].get("usDerivativeBearTurn", 0),
        "derivative_branch_pnl": derivative_branch_pnl,
        "derivative_branch_pnl_delta_vs_anchor": derivative_branch_pnl - longer_time_metrics["pnl_by_signal_family"].get("usDerivativeBearTurn", 0.0),
        "first_bear_snap_pnl_delta_vs_anchor": metrics["pnl_by_signal_family"].get("firstBearSnapTurn", 0.0) - longer_time_metrics["pnl_by_signal_family"].get("firstBearSnapTurn", 0.0),
        "first_bear_snap_trade_delta_vs_anchor": trade_counts_by_signal.get("firstBearSnapTurn", 0) - longer_time_trade_counts[1].get("firstBearSnapTurn", 0),
        "long_side_pnl_delta_vs_anchor": pnl_by_direction.get("LONG", 0.0) - longer_time_pnl_by_direction.get("LONG", 0.0),
        "missed_bearish_turn_participation_delta_vs_anchor": material_participation_count(full["turn_counts"]) - material_participation_count(longer_time_turn_counts),
        "trade_count_by_signal_family": trade_counts_by_signal,
        "trade_count_by_session": trade_counts_by_session,
        "pnl_by_signal_family": metrics["pnl_by_signal_family"],
        "pnl_by_session": metrics["pnl_by_session"],
        "pnl_by_direction": pnl_by_direction,
        "mae_mfe_summary": full["mae"],
        "hold_time_summary": full["hold"],
        "slice_rows": slice_rows,
    }
    if longer_time_plus is not None:
        row["longer_time_plus_pnl_delta"] = metrics["total_net_pnl"] - longer_time_plus["metrics"]["total_net_pnl"]
        row["longer_time_plus_expectancy_delta"] = metrics["expectancy"] - longer_time_plus["metrics"]["expectancy"]
        row["longer_time_plus_drawdown_delta"] = metrics["max_drawdown"] - longer_time_plus["metrics"]["max_drawdown"]
    ranked_rows.append(row)

ranked_rows.sort(key=lambda item: (item["verdict"] != "KEEP", -item["total_net_pnl"]))

with output_csv.open("w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=[
            "variant",
            "verdict",
            "slice_improvement_count",
            "total_net_pnl",
            "number_of_trades",
            "win_rate",
            "avg_winner",
            "avg_loser",
            "expectancy",
            "max_drawdown",
            "control_pnl_delta",
            "anchor_pnl_delta",
            "anchor_expectancy_delta",
            "anchor_drawdown_delta",
            "short_side_pnl_delta_vs_anchor",
            "us_session_pnl_delta_vs_anchor",
            "derivative_trade_count",
            "derivative_trade_delta_vs_anchor",
            "derivative_branch_pnl",
            "derivative_branch_pnl_delta_vs_anchor",
            "first_bear_snap_pnl_delta_vs_anchor",
            "first_bear_snap_trade_delta_vs_anchor",
            "long_side_pnl_delta_vs_anchor",
            "missed_bearish_turn_participation_delta_vs_anchor",
        ],
    )
    writer.writeheader()
    for row in ranked_rows:
        writer.writerow({key: row.get(key) for key in writer.fieldnames})

output_json.write_text(json.dumps(ranked_rows, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps({"ranked_csv_path": str(output_csv), "ranked_json_path": str(output_json)}, sort_keys=True))
PY
