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
if len(timestamps) < 9:
    raise SystemExit("Need at least 9 bars to form early/middle/recent slices.")

count = len(timestamps)
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
  "shorter_time_stop:${REPO_ROOT}/config/replay.exit_shorter_time_stop.yaml"
  "longer_time_stop:${REPO_ROOT}/config/replay.exit_longer_time_stop.yaml"
  "ema_based_exit:${REPO_ROOT}/config/replay.exit_ema_based.yaml"
  "structure_break_exit:${REPO_ROOT}/config/replay.exit_structure_break.yaml"
  "one_r_take_profit_proxy:${REPO_ROOT}/config/replay.exit_one_r_take_profit_proxy.yaml"
  "trail_only:${REPO_ROOT}/config/replay.exit_trail_only.yaml"
)

echo "variant,slice_name,summary_path" > "${RESULTS_FILE}"

run_variant() {
  local variant_name="$1"
  local config_override="$2"
  local slice_name="$3"
  local slice_start="$4"
  local slice_end="$5"
  local stamp="exit_${RUN_TAG}_${variant_name}_${slice_name}"
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

for variant in "${VARIANTS[@]}"; do
  variant_name="${variant%%:*}"
  if [[ "${variant_name}" == "control" ]]; then
    continue
  fi
  bash "${REPO_ROOT}/scripts/run_replay_ablation_compare.sh" \
    "${REPLAY_DIR}/persisted_bar_replay_exit_${RUN_TAG}_control_full.summary.json" \
    "${REPLAY_DIR}/persisted_bar_replay_exit_${RUN_TAG}_${variant_name}_full.summary.json" \
    "${REPLAY_DIR}/persisted_bar_replay_exit_${RUN_TAG}_${variant_name}_vs_control.comparison.json"
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
metrics_by_slice_path = replay_dir / f"exit_experiments_{run_tag}.metrics_by_slice.csv"
promotion_stability_path = replay_dir / f"exit_experiments_{run_tag}.promotion_stability.json"
control_manifest_path = replay_dir / f"exit_experiments_{run_tag}.control_manifest.json"

rows = list(csv.DictReader(results_file.open(encoding="utf-8", newline="")))
by_variant: dict[str, dict[str, dict]] = {}
for row in rows:
    summary = json.loads(Path(row["summary_path"]).read_text(encoding="utf-8"))
    metrics = json.loads(Path(summary["summary_metrics_path"]).read_text(encoding="utf-8"))
    by_variant.setdefault(row["variant"], {})[row["slice_name"]] = {
        "summary": summary,
        "metrics": metrics,
    }

fieldnames = [
    "variant",
    "slice_name",
    "total_net_pnl",
    "number_of_trades",
    "win_rate",
    "expectancy",
    "max_drawdown",
    "runtime_seconds",
]
with metrics_by_slice_path.open("w", encoding="utf-8", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    for variant_name, slices in sorted(by_variant.items()):
        for slice_name, payload in sorted(slices.items()):
            metrics = payload["metrics"]
            summary = payload["summary"]
            writer.writerow(
                {
                    "variant": variant_name,
                    "slice_name": slice_name,
                    "total_net_pnl": metrics["total_net_pnl"],
                    "number_of_trades": metrics["number_of_trades"],
                    "win_rate": metrics["win_rate"],
                    "expectancy": metrics["expectancy"],
                    "max_drawdown": metrics["max_drawdown"],
                    "runtime_seconds": summary["runtime_seconds"],
                }
            )

control_full = by_variant["control"]["full"]["metrics"]
control_slices = {
    key: value["metrics"]
    for key, value in by_variant["control"].items()
    if key != "full"
}

variants_summary: dict[str, dict] = {}
for variant_name, slices in sorted(by_variant.items()):
    if variant_name == "control":
        continue
    full_metrics = slices["full"]["metrics"]
    slice_improvement_count = 0
    slice_details = {}
    for slice_name, control_slice_metrics in control_slices.items():
        metrics = slices[slice_name]["metrics"]
        improved = metrics["total_net_pnl"] > control_slice_metrics["total_net_pnl"]
        if improved:
            slice_improvement_count += 1
        slice_details[slice_name] = {
            "variant_total_net_pnl": metrics["total_net_pnl"],
            "control_total_net_pnl": control_slice_metrics["total_net_pnl"],
            "improved": improved,
        }
    promotable = (
        full_metrics["total_net_pnl"] > control_full["total_net_pnl"]
        and full_metrics["expectancy"] >= control_full["expectancy"]
        and full_metrics["max_drawdown"] <= control_full["max_drawdown"] + draw_tolerance
        and slice_improvement_count >= 2
    )
    variants_summary[variant_name] = {
        "verdict": "KEEP" if promotable else "REJECT",
        "slice_improvement_count": slice_improvement_count,
        "full_total_net_pnl": full_metrics["total_net_pnl"],
        "control_total_net_pnl": control_full["total_net_pnl"],
        "full_expectancy": full_metrics["expectancy"],
        "control_expectancy": control_full["expectancy"],
        "full_max_drawdown": full_metrics["max_drawdown"],
        "control_max_drawdown": control_full["max_drawdown"],
        "slice_details": slice_details,
    }

promotion_payload = {
    "control_variant": "replay.research_control.yaml",
    "drawdown_tolerance": draw_tolerance,
    "keep_reject_logic": {
        "requires_total_net_pnl_improvement": True,
        "requires_expectancy_not_worse": True,
        "max_drawdown_must_not_exceed_control_plus": draw_tolerance,
        "requires_improvement_in_at_least_two_of_three_slices": True,
    },
    "variants": variants_summary,
}
promotion_stability_path.write_text(json.dumps(promotion_payload, indent=2, sort_keys=True), encoding="utf-8")

control_manifest = {
    "control_config_path": "/Users/patrick/Documents/MGC-v05l-automation/config/replay.research_control.yaml",
    "control_description": [
        "Asia-only firstBullSnapTurn ON",
        "asiaVWAPLongSignal OFF",
        "London blocked",
        "US enabled",
        "Promoted medium_1 derivative-short branch ON",
    ],
    "results_path": str(metrics_by_slice_path),
}
control_manifest_path.write_text(json.dumps(control_manifest, indent=2, sort_keys=True), encoding="utf-8")

print(json.dumps({"metrics_by_slice_path": str(metrics_by_slice_path), "promotion_stability_path": str(promotion_stability_path), "control_manifest_path": str(control_manifest_path)}, sort_keys=True))
PY
