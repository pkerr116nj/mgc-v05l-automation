#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

RUN_TAG="${RUN_TAG:-$(date +%Y%m%d_%H%M%S)}"
DRAW_TOLERANCE="${PROMOTION_DRAWDOWN_TOLERANCE:-50}"
SLICE_FILE="$(mktemp)"
RESULTS_FILE="$(mktemp)"

cleanup() {
  rm -f "${SLICE_FILE}" "${RESULTS_FILE}"
}
trap cleanup EXIT

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
  "control:"
  "longer_time_anchor:${REPO_ROOT}/config/replay.exit_longer_time_stop.yaml"
  "derivative_short:${REPO_ROOT}/config/replay.exit_derivative_maturity_short.yaml"
  "derivative_both:${REPO_ROOT}/config/replay.exit_derivative_maturity_both.yaml"
  "longer_time_plus_derivative_short:${REPO_ROOT}/config/replay.exit_longer_time_plus_derivative_short.yaml"
)

echo "variant,slice_name,summary_path" > "${RESULTS_FILE}"

run_variant() {
  local variant_name="$1"
  local config_override="$2"
  local slice_name="$3"
  local slice_start="$4"
  local slice_end="$5"
  local stamp="derivative_exit_${RUN_TAG}_${variant_name}_${slice_name}"
  local summary_path="${REPLAY_DIR}/persisted_bar_replay_${stamp}.summary.json"

  if [[ -n "${config_override}" ]]; then
    CONFIG_OVERRIDE="${config_override}" \
    REPLAY_SLICE_START_TS="${slice_start}" \
    REPLAY_SLICE_END_TS="${slice_end}" \
    RUN_STAMP="${stamp}" \
    bash "${REPO_ROOT}/scripts/run_persisted_bar_replay.sh"
  else
    REPLAY_SLICE_START_TS="${slice_start}" \
    REPLAY_SLICE_END_TS="${slice_end}" \
    RUN_STAMP="${stamp}" \
    bash "${REPO_ROOT}/scripts/run_persisted_bar_replay.sh"
  fi

  echo "${variant_name},${slice_name},${summary_path}" >> "${RESULTS_FILE}"
}

for variant in "${VARIANTS[@]}"; do
  variant_name="${variant%%:*}"
  config_override="${variant#*:}"
  run_variant "${variant_name}" "${config_override}" "full" "" ""
  tail -n +2 "${SLICE_FILE}" | while IFS=, read -r slice_name slice_start slice_end; do
    run_variant "${variant_name}" "${config_override}" "${slice_name}" "${slice_start}" "${slice_end}"
  done
done

for variant in derivative_short derivative_both longer_time_plus_derivative_short; do
  bash "${REPO_ROOT}/scripts/run_replay_ablation_compare.sh" \
    "${REPLAY_DIR}/persisted_bar_replay_derivative_exit_${RUN_TAG}_control_full.summary.json" \
    "${REPLAY_DIR}/persisted_bar_replay_derivative_exit_${RUN_TAG}_${variant}_full.summary.json" \
    "${REPLAY_DIR}/persisted_bar_replay_derivative_exit_${RUN_TAG}_${variant}_vs_control.comparison.json"

  bash "${REPO_ROOT}/scripts/run_replay_ablation_compare.sh" \
    "${REPLAY_DIR}/persisted_bar_replay_derivative_exit_${RUN_TAG}_longer_time_anchor_full.summary.json" \
    "${REPLAY_DIR}/persisted_bar_replay_derivative_exit_${RUN_TAG}_${variant}_full.summary.json" \
    "${REPLAY_DIR}/persisted_bar_replay_derivative_exit_${RUN_TAG}_${variant}_vs_longer_time.comparison.json"
done

export RUN_TAG
export DRAW_TOLERANCE
export RESULTS_FILE

python3 - <<'PY'
from __future__ import annotations

import csv
import json
import os
from pathlib import Path

run_tag = os.environ["RUN_TAG"]
draw_tolerance = float(os.environ["DRAW_TOLERANCE"])
results_file = Path(os.environ["RESULTS_FILE"])
replay_dir = Path("/Users/patrick/Documents/MGC-v05l-automation/outputs/replays")
ranked_csv_path = replay_dir / f"derivative_exit_pass_{run_tag}.ranked.csv"
ranked_json_path = replay_dir / f"derivative_exit_pass_{run_tag}.ranked.json"
retest_shortlist_path = replay_dir / f"rejected_entry_retest_shortlist_{run_tag}.json"

rows = list(csv.DictReader(results_file.open(encoding="utf-8", newline="")))
by_variant: dict[str, dict[str, dict]] = {}
for row in rows:
    summary = json.loads(Path(row["summary_path"]).read_text(encoding="utf-8"))
    metrics = json.loads(Path(summary["summary_metrics_path"]).read_text(encoding="utf-8"))
    mae = json.loads(Path(summary["mae_mfe_summary_path"]).read_text(encoding="utf-8"))
    hold = json.loads(Path(summary["hold_time_summary_path"]).read_text(encoding="utf-8"))
    by_variant.setdefault(row["variant"], {})[row["slice_name"]] = {
        "summary": summary,
        "metrics": metrics,
        "mae": mae,
        "hold": hold,
    }

control_full = by_variant["control"]["full"]
longer_full = by_variant["longer_time_anchor"]["full"]
control_slices = {k: v for k, v in by_variant["control"].items() if k != "full"}

ranked_rows = []
for variant in ("derivative_short", "derivative_both", "longer_time_plus_derivative_short"):
    full = by_variant[variant]["full"]
    control_delta = {
        "pnl_delta": full["metrics"]["total_net_pnl"] - control_full["metrics"]["total_net_pnl"],
        "expectancy_delta": full["metrics"]["expectancy"] - control_full["metrics"]["expectancy"],
        "drawdown_delta": full["metrics"]["max_drawdown"] - control_full["metrics"]["max_drawdown"],
        "avg_winner_delta": full["metrics"]["avg_winner"] - control_full["metrics"]["avg_winner"],
        "avg_loser_delta": full["metrics"]["avg_loser"] - control_full["metrics"]["avg_loser"],
        "avg_mfe_delta": full["mae"]["average_mfe"] - control_full["mae"]["average_mfe"],
        "avg_mae_delta": full["mae"]["average_mae"] - control_full["mae"]["average_mae"],
        "avg_bars_held_delta": full["hold"]["average_bars_held"] - control_full["hold"]["average_bars_held"],
    }
    longer_delta = {
        "pnl_delta": full["metrics"]["total_net_pnl"] - longer_full["metrics"]["total_net_pnl"],
        "expectancy_delta": full["metrics"]["expectancy"] - longer_full["metrics"]["expectancy"],
        "drawdown_delta": full["metrics"]["max_drawdown"] - longer_full["metrics"]["max_drawdown"],
    }
    slice_improvement_count = 0
    slice_rows = {}
    for slice_name, control_slice in control_slices.items():
        variant_slice = by_variant[variant][slice_name]
        improved = variant_slice["metrics"]["total_net_pnl"] > control_slice["metrics"]["total_net_pnl"]
        if improved:
            slice_improvement_count += 1
        slice_rows[slice_name] = {
            "variant_total_net_pnl": variant_slice["metrics"]["total_net_pnl"],
            "control_total_net_pnl": control_slice["metrics"]["total_net_pnl"],
            "improved": improved,
        }

    promotable = (
        full["metrics"]["total_net_pnl"] > control_full["metrics"]["total_net_pnl"]
        and full["metrics"]["expectancy"] >= control_full["metrics"]["expectancy"]
        and full["metrics"]["max_drawdown"] <= control_full["metrics"]["max_drawdown"] + draw_tolerance
        and slice_improvement_count >= 2
    )
    ranked_rows.append(
        {
            "variant": variant,
            "verdict": "KEEP" if promotable else "REJECT",
            "slice_improvement_count": slice_improvement_count,
            "total_net_pnl": full["metrics"]["total_net_pnl"],
            "expectancy": full["metrics"]["expectancy"],
            "max_drawdown": full["metrics"]["max_drawdown"],
            "avg_winner": full["metrics"]["avg_winner"],
            "avg_loser": full["metrics"]["avg_loser"],
            "win_rate": full["metrics"]["win_rate"],
            "number_of_trades": full["metrics"]["number_of_trades"],
            "control_delta": control_delta,
            "longer_time_anchor_delta": longer_delta,
            "mae_mfe_summary": full["mae"],
            "hold_time_summary": full["hold"],
            "slice_rows": slice_rows,
        }
    )

ranked_rows.sort(key=lambda row: (row["verdict"] != "KEEP", -row["total_net_pnl"]))

with ranked_csv_path.open("w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=[
            "variant",
            "verdict",
            "slice_improvement_count",
            "total_net_pnl",
            "expectancy",
            "max_drawdown",
            "avg_winner",
            "avg_loser",
            "win_rate",
            "number_of_trades",
            "control_pnl_delta",
            "control_expectancy_delta",
            "control_drawdown_delta",
            "longer_time_pnl_delta",
            "longer_time_expectancy_delta",
            "longer_time_drawdown_delta",
        ],
    )
    writer.writeheader()
    for row in ranked_rows:
        writer.writerow(
            {
                "variant": row["variant"],
                "verdict": row["verdict"],
                "slice_improvement_count": row["slice_improvement_count"],
                "total_net_pnl": row["total_net_pnl"],
                "expectancy": row["expectancy"],
                "max_drawdown": row["max_drawdown"],
                "avg_winner": row["avg_winner"],
                "avg_loser": row["avg_loser"],
                "win_rate": row["win_rate"],
                "number_of_trades": row["number_of_trades"],
                "control_pnl_delta": row["control_delta"]["pnl_delta"],
                "control_expectancy_delta": row["control_delta"]["expectancy_delta"],
                "control_drawdown_delta": row["control_delta"]["drawdown_delta"],
                "longer_time_pnl_delta": row["longer_time_anchor_delta"]["pnl_delta"],
                "longer_time_expectancy_delta": row["longer_time_anchor_delta"]["expectancy_delta"],
                "longer_time_drawdown_delta": row["longer_time_anchor_delta"]["drawdown_delta"],
            }
        )

ranked_json_path.write_text(json.dumps(ranked_rows, indent=2, sort_keys=True), encoding="utf-8")

retest_shortlist = [
    {
        "candidate": "broader_us_derivative_bear_branch",
        "priority": "high",
        "why_rejected_before": "Loose US derivative-bear trigger massively overfired and produced roughly 1536 added trades with large negative P/L.",
        "why_reconsider": "The derivative-short concept itself was validated by the promoted medium_1 branch; the failure mode was selectivity, not directionality.",
        "likely_issue": "entry_quality + regime_filter + anti_churn",
    },
    {
        "candidate": "one_r_take_profit_proxy_on_entry_sensitive_branches",
        "priority": "medium",
        "why_rejected_before": "The one-R proxy improved capture percentage but reduced total P/L and expectancy on the promoted control.",
        "why_reconsider": "It suggests some branches reach usable MFE but monetize poorly; a true partial-capital implementation could be more faithful than the one-lot proxy.",
        "likely_issue": "exit_logic",
    },
    {
        "candidate": "asiaVWAPLongSignal",
        "priority": "medium",
        "why_rejected_before": "Disabling the branch improved the baseline materially, indicating weak standalone monetization under the old exit baseline.",
        "why_reconsider": "It may contain directional information that was not monetized well enough under earlier exits; revisit only after exit baseline improves further.",
        "likely_issue": "entry_quality + exit_logic",
    },
    {
        "candidate": "us_london_firstBullSnapTurn_longs",
        "priority": "low",
        "why_rejected_before": "Outside Asia, Bull Snap longs were a concentrated drag and blocking them improved both P/L and drawdown.",
        "why_reconsider": "Only if future regime-specific filters become much stronger; current evidence points to a session/regime issue rather than a monetization issue.",
        "likely_issue": "regime_filter",
    },
    {
        "candidate": "slow_ema_gated_derivative_bear_variant",
        "priority": "low",
        "why_rejected_before": "The slow-EMA gate produced no change versus the promoted control.",
        "why_reconsider": "Low-priority sanity check only if future derivative branches widen again.",
        "likely_issue": "already_redundant_filter",
    },
]
retest_shortlist_path.write_text(json.dumps(retest_shortlist, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps({"ranked_csv_path": str(ranked_csv_path), "ranked_json_path": str(ranked_json_path), "retest_shortlist_path": str(retest_shortlist_path)}, sort_keys=True))
PY
