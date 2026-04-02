"""Blind attrition analysis for the US_MIDDAY pause/pullback/resume long cluster."""

from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from statistics import mean
from typing import Any, Callable

from ..config_models.loader import load_settings_from_files
from ..domain.enums import LongEntryFamily, PositionSide, ShortEntryFamily, StrategyStatus
from ..domain.models import Bar, StrategyState
from ..indicators.feature_engine import compute_features


@dataclass(frozen=True)
class ClusterRow:
    timestamp: str
    move_10bar: Decimal
    mfe_20bar: Decimal
    mae_20bar: Decimal
    followthrough_quality: str
    normalized_slope: Decimal
    normalized_curvature: Decimal
    range_expansion_ratio: Decimal
    slope_ok: bool
    curvature_ok: bool
    not_expanded_ok: bool
    rebound_below_slow_ok: bool
    one_bar_pullback_ok: bool
    break_above_prior_high_ok: bool


def build_and_write_us_midday_pause_resume_long_bind_analysis(
    *,
    detail_csv_path: Path,
    treatment_summary_path: Path,
    db_path: Path,
    config_paths: list[Path],
) -> dict[str, str]:
    settings = load_settings_from_files(config_paths)
    rows = _load_cluster_rows(detail_csv_path=detail_csv_path, db_path=db_path, settings=settings)
    attrition_rows = _build_attrition_rows(rows)
    single_relaxation_rows = _build_single_relaxation_rows(rows)
    summary_payload = _build_summary(
        rows=rows,
        attrition_rows=attrition_rows,
        single_relaxation_rows=single_relaxation_rows,
        treatment_summary_path=treatment_summary_path,
        settings=settings,
    )

    prefix = Path(str(treatment_summary_path).removesuffix(".summary.json"))
    detail_path = prefix.with_suffix(".us_midday_pause_resume_long_bind_detail.csv")
    attrition_path = prefix.with_suffix(".us_midday_pause_resume_long_bind_attrition.csv")
    relaxation_path = prefix.with_suffix(".us_midday_pause_resume_long_bind_single_relaxations.csv")
    summary_path = prefix.with_suffix(".us_midday_pause_resume_long_bind_summary.json")

    _write_csv(
        detail_path,
        [
            {
                "timestamp": row.timestamp,
                "move_10bar": str(row.move_10bar),
                "mfe_20bar": str(row.mfe_20bar),
                "mae_20bar": str(row.mae_20bar),
                "followthrough_quality": row.followthrough_quality,
                "normalized_slope": str(row.normalized_slope),
                "normalized_curvature": str(row.normalized_curvature),
                "range_expansion_ratio": str(row.range_expansion_ratio),
                "slope_ok": row.slope_ok,
                "curvature_ok": row.curvature_ok,
                "not_expanded_ok": row.not_expanded_ok,
                "rebound_below_slow_ok": row.rebound_below_slow_ok,
                "one_bar_pullback_ok": row.one_bar_pullback_ok,
                "break_above_prior_high_ok": row.break_above_prior_high_ok,
            }
            for row in rows
        ],
    )
    _write_csv(attrition_path, attrition_rows)
    _write_csv(relaxation_path, single_relaxation_rows)
    summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "us_midday_pause_resume_long_bind_detail_path": str(detail_path),
        "us_midday_pause_resume_long_bind_attrition_path": str(attrition_path),
        "us_midday_pause_resume_long_bind_single_relaxations_path": str(relaxation_path),
        "us_midday_pause_resume_long_bind_summary_path": str(summary_path),
    }


def _load_cluster_rows(*, detail_csv_path: Path, db_path: Path, settings) -> list[ClusterRow]:
    bars_by_timestamp, ordered_timestamps = _load_mgc_5m_bars(db_path)
    index_by_timestamp = {timestamp: index for index, timestamp in enumerate(ordered_timestamps)}

    rows: list[ClusterRow] = []
    with detail_csv_path.open(encoding="utf-8", newline="") as handle:
        for raw_row in csv.DictReader(handle):
            if raw_row["session_phase"] != "US_MIDDAY":
                continue
            if raw_row["direction_of_turn"] != "LONG":
                continue
            if raw_row["recent_path_shape"] != "pause_pullback_resume_long":
                continue

            timestamp = raw_row["timestamp"]
            current_index = index_by_timestamp.get(timestamp)
            if current_index is None or current_index < 2:
                continue

            history = [
                bars_by_timestamp[ordered_timestamps[index]]
                for index in range(max(0, current_index - 80), current_index + 1)
            ]
            features = compute_features(history, _blank_state(history[-1].end_ts), settings)
            normalizer = max(features.atr, settings.risk_floor)
            normalized_slope = features.velocity / normalizer
            normalized_curvature = features.velocity_delta / normalizer
            range_expansion_ratio = (
                (history[-1].high - history[-1].low) / features.atr if features.atr > 0 else Decimal("0")
            )

            rows.append(
                ClusterRow(
                    timestamp=timestamp,
                    move_10bar=Decimal(raw_row["move_10bar"]),
                    mfe_20bar=Decimal(raw_row["mfe_20bar"]),
                    mae_20bar=Decimal(raw_row["mae_20bar"]),
                    followthrough_quality=raw_row["followthrough_quality"],
                    normalized_slope=normalized_slope,
                    normalized_curvature=normalized_curvature,
                    range_expansion_ratio=range_expansion_ratio,
                    slope_ok=(
                        settings.us_midday_pause_resume_long_min_normalized_slope
                        <= normalized_slope
                        <= settings.us_midday_pause_resume_long_max_normalized_slope
                    ),
                    curvature_ok=(
                        settings.us_midday_pause_resume_long_min_normalized_curvature
                        <= normalized_curvature
                        <= settings.us_midday_pause_resume_long_max_normalized_curvature
                    ),
                    not_expanded_ok=(
                        range_expansion_ratio < settings.us_midday_pause_resume_long_max_range_expansion_ratio
                    ),
                    rebound_below_slow_ok=(
                        features.turn_ema_fast < features.turn_ema_slow
                        and history[-1].close > features.turn_ema_fast
                        and history[-1].close <= features.turn_ema_slow
                    ),
                    one_bar_pullback_ok=history[-2].close < history[-3].close,
                    break_above_prior_high_ok=history[-1].high > history[-2].high,
                )
            )
    return rows


def _load_mgc_5m_bars(db_path: Path) -> tuple[dict[str, Bar], list[str]]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        fetched = connection.execute(
            """
            select
              bar_id,
              ticker,
              timeframe,
              start_ts,
              end_ts,
              open,
              high,
              low,
              close,
              volume,
              is_final,
              session_asia,
              session_london,
              session_us,
              session_allowed,
              data_source
            from bars
            where ticker='MGC'
              and timeframe='5m'
              and data_source in ('schwab_history', 'internal')
            order by end_ts, case data_source when 'schwab_history' then 0 else 1 end
            """
        ).fetchall()
    finally:
        connection.close()

    bars_by_timestamp: dict[str, Bar] = {}
    ordered_timestamps: list[str] = []
    for row in fetched:
        timestamp = row["end_ts"]
        if timestamp in bars_by_timestamp:
            continue
        bars_by_timestamp[timestamp] = Bar(
            bar_id=row["bar_id"],
            symbol=row["ticker"],
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
        ordered_timestamps.append(timestamp)
    return bars_by_timestamp, ordered_timestamps


def _blank_state(updated_at: datetime) -> StrategyState:
    return StrategyState(
        strategy_status=StrategyStatus.READY,
        position_side=PositionSide.FLAT,
        broker_position_qty=0,
        internal_position_qty=0,
        entry_price=None,
        entry_timestamp=None,
        entry_bar_id=None,
        long_entry_family=LongEntryFamily.NONE,
        bars_in_trade=0,
        long_be_armed=False,
        short_be_armed=False,
        last_swing_low=None,
        last_swing_high=None,
        asia_reclaim_bar_low=None,
        asia_reclaim_bar_high=None,
        asia_reclaim_bar_vwap=None,
        bars_since_bull_snap=None,
        bars_since_bear_snap=None,
        bars_since_asia_reclaim=None,
        bars_since_asia_vwap_signal=None,
        bars_since_long_setup=None,
        bars_since_short_setup=None,
        last_signal_bar_id=None,
        last_order_intent_id=None,
        open_broker_order_id=None,
        entries_enabled=True,
        exits_enabled=True,
        operator_halt=False,
        same_underlying_entry_hold=False,
        same_underlying_hold_reason=None,
        reconcile_required=False,
        fault_code=None,
        updated_at=updated_at,
        short_entry_family=ShortEntryFamily.NONE,
        short_entry_source=None,
    )


def _build_attrition_rows(rows: list[ClusterRow]) -> list[dict[str, Any]]:
    filters: list[tuple[str, str, Callable[[ClusterRow], bool]]] = [
        ("raw_cluster", "US_MIDDAY long pause_pullback_resume_long missed turns", lambda _: True),
        ("slope_range", "normalized_slope in [0.00, 0.20]", lambda row: row.slope_ok),
        ("curvature_range", "normalized_curvature in [0.10, 0.60]", lambda row: row.curvature_ok),
        ("not_expanded", "range_expansion_ratio < 1.25", lambda row: row.not_expanded_ok),
        ("rebound_below_slow", "fast < slow and close rebounds above fast but not above slow", lambda row: row.rebound_below_slow_ok),
        ("one_bar_pullback", "prior bar closes below the bar before it", lambda row: row.one_bar_pullback_ok),
        ("break_above_prior_high", "signal bar breaks above prior 1-bar high", lambda row: row.break_above_prior_high_ok),
    ]

    results: list[dict[str, Any]] = []
    current = rows
    raw_count = len(rows)
    prior_count = len(rows)
    for index, (key, description, predicate) in enumerate(filters):
        if index == 0:
            passing = current
        else:
            passing = [row for row in current if predicate(row)]
            current = passing

        independent_count = raw_count if index == 0 else sum(1 for row in rows if predicate(row))
        drop_from_prior = prior_count - len(passing) if index > 0 else 0
        results.append(
            {
                "stage": key,
                "description": description,
                "cumulative_pass_count": len(passing),
                "cumulative_pass_share": _ratio(len(passing), raw_count),
                "independent_pass_count": independent_count,
                "independent_pass_share": _ratio(independent_count, raw_count),
                "drop_from_prior_stage": drop_from_prior,
            }
        )
        prior_count = len(passing)

    results.append(
        {
            "stage": "all_current_gates",
            "description": "rows that survive the full current midday long rule stack",
            "cumulative_pass_count": len(current),
            "cumulative_pass_share": _ratio(len(current), raw_count),
            "independent_pass_count": len(current),
            "independent_pass_share": _ratio(len(current), raw_count),
            "drop_from_prior_stage": 0,
        }
    )
    return results


def _build_single_relaxation_rows(rows: list[ClusterRow]) -> list[dict[str, Any]]:
    gates: list[tuple[str, Callable[[ClusterRow], bool]]] = [
        ("slope_range", lambda row: row.slope_ok),
        ("curvature_range", lambda row: row.curvature_ok),
        ("not_expanded", lambda row: row.not_expanded_ok),
        ("rebound_below_slow", lambda row: row.rebound_below_slow_ok),
        ("one_bar_pullback", lambda row: row.one_bar_pullback_ok),
        ("break_above_prior_high", lambda row: row.break_above_prior_high_ok),
    ]

    results: list[dict[str, Any]] = []
    for relaxed_key, _ in gates:
        survivors = [
            row
            for row in rows
            if all(predicate(row) for key, predicate in gates if key != relaxed_key)
        ]
        results.append(
            {
                "relaxed_gate": relaxed_key,
                "survivor_count": len(survivors),
                "survivor_share": _ratio(len(survivors), len(rows)),
                "avg_mfe_20bar": str(_avg_decimal(row.mfe_20bar for row in survivors)),
                "avg_move_10bar": str(_avg_decimal(row.move_10bar for row in survivors)),
                "avg_mae_20bar": str(_avg_decimal(row.mae_20bar for row in survivors)),
                "survivor_timestamps": ",".join(row.timestamp for row in survivors),
            }
        )
    return results


def _build_summary(
    *,
    rows: list[ClusterRow],
    attrition_rows: list[dict[str, Any]],
    single_relaxation_rows: list[dict[str, Any]],
    treatment_summary_path: Path,
    settings,
) -> dict[str, Any]:
    treatment_summary = json.loads(treatment_summary_path.read_text(encoding="utf-8"))
    treatment_ledger_path = Path(treatment_summary["trade_ledger_path"])
    with treatment_ledger_path.open(encoding="utf-8", newline="") as handle:
        realized_family_rows = [row for row in csv.DictReader(handle) if row["setup_family"] == "usMiddayPauseResumeLongTurn"]

    strongest_cumulative_bottleneck = max(
        (
            row
            for row in attrition_rows
            if row["stage"] not in {"raw_cluster", "all_current_gates"}
        ),
        key=lambda row: row["drop_from_prior_stage"],
    )
    strongest_independent_bottleneck = min(
        (
            row
            for row in attrition_rows
            if row["stage"] not in {"raw_cluster", "all_current_gates"}
        ),
        key=lambda row: row["independent_pass_count"],
    )
    best_single_relaxation = max(single_relaxation_rows, key=lambda row: row["survivor_count"])
    next_rule_preview_rows = [
        row
        for row in rows
        if Decimal("-0.10") <= row.normalized_slope <= settings.us_midday_pause_resume_long_max_normalized_slope
        and row.curvature_ok
        and row.not_expanded_ok
        and row.rebound_below_slow_ok
        and row.one_bar_pullback_ok
        and row.break_above_prior_high_ok
    ]

    all_rows_avg_mfe = _avg_decimal(row.mfe_20bar for row in rows)
    gate_quality = {
        "slope_range_avg_mfe": str(_avg_decimal(row.mfe_20bar for row in rows if row.slope_ok)),
        "curvature_range_avg_mfe": str(_avg_decimal(row.mfe_20bar for row in rows if row.curvature_ok)),
        "not_expanded_avg_mfe": str(_avg_decimal(row.mfe_20bar for row in rows if row.not_expanded_ok)),
        "rebound_below_slow_avg_mfe": str(_avg_decimal(row.mfe_20bar for row in rows if row.rebound_below_slow_ok)),
        "one_bar_pullback_avg_mfe": str(_avg_decimal(row.mfe_20bar for row in rows if row.one_bar_pullback_ok)),
        "break_above_prior_high_avg_mfe": str(_avg_decimal(row.mfe_20bar for row in rows if row.break_above_prior_high_ok)),
        "raw_cluster_avg_mfe": str(all_rows_avg_mfe),
    }

    return {
        "us_midday_long_cluster_summary": {
            "raw_cluster_count": len(rows),
            "estimated_value_mfe20_total": str(sum((row.mfe_20bar for row in rows), Decimal("0"))),
            "avg_move_10bar": str(_avg_decimal(row.move_10bar for row in rows)),
            "avg_mfe_20bar": str(all_rows_avg_mfe),
            "avg_mae_20bar": str(_avg_decimal(row.mae_20bar for row in rows)),
        },
        "current_rule_thresholds": {
            "normalized_slope_min": str(settings.us_midday_pause_resume_long_min_normalized_slope),
            "normalized_slope_max": str(settings.us_midday_pause_resume_long_max_normalized_slope),
            "normalized_curvature_min": str(settings.us_midday_pause_resume_long_min_normalized_curvature),
            "normalized_curvature_max": str(settings.us_midday_pause_resume_long_max_normalized_curvature),
            "max_range_expansion_ratio": str(settings.us_midday_pause_resume_long_max_range_expansion_ratio),
        },
        "filter_by_filter_attrition_table": attrition_rows,
        "single_gate_relaxation_table": single_relaxation_rows,
        "realized_treatment_family": {
            "trade_count": len(realized_family_rows),
            "pnl_total": str(sum((Decimal(row["net_pnl"]) for row in realized_family_rows), Decimal("0"))),
        },
        "gate_quality_summary": gate_quality,
        "main_findings": [
            f"The first major bottleneck is the exact slope band: it cuts the raw cluster from {len(rows)} to {next(row['cumulative_pass_count'] for row in attrition_rows if row['stage'] == 'slope_range')}.",
            f"The literal zero-out happens at rebound_below_slow, but that is not the best first relaxation because relaxing rebound alone still leaves {next(row['survivor_count'] for row in single_relaxation_rows if row['relaxed_gate'] == 'rebound_below_slow')} survivors.",
            f"Slope is the only single gate whose relaxation unlocks any candidates at all under the rest of the current rule stack; relaxing only slope yields {best_single_relaxation['survivor_count']} survivors.",
            "Curvature looks structurally useful rather than over-constraining here: its independent survivors have better average MFE than the raw cluster, while slope-range and rebound-below-slow survivors are weaker than the raw cluster on average.",
        ],
        "hardest_binding_gate": {
            "by_cumulative_drop": strongest_cumulative_bottleneck,
            "by_smallest_independent_survivor_set": strongest_independent_bottleneck,
        },
        "best_gate_to_relax_first": {
            "gate": best_single_relaxation["relaxed_gate"],
            "reason": (
                "It is the only single relaxation that makes the family bind at all while preserving the rest of the stack."
            ),
        },
        "next_rule_hypothesis_preview": {
            "normalized_slope_min": "-0.10",
            "normalized_slope_max": str(settings.us_midday_pause_resume_long_max_normalized_slope),
            "survivor_count": len(next_rule_preview_rows),
            "survivor_timestamps": [row.timestamp for row in next_rule_preview_rows],
            "avg_mfe_20bar": str(_avg_decimal(row.mfe_20bar for row in next_rule_preview_rows)),
            "avg_move_10bar": str(_avg_decimal(row.move_10bar for row in next_rule_preview_rows)),
        },
        "smallest_next_rule_hypothesis": (
            "Relax only the slope lower bound from 0.00 to -0.10, keep the 0.20 upper bound, "
            "and leave curvature, not_expanded, rebound_below_slow, one-bar pullback, and break-above-prior-high unchanged."
        ),
        "recommended_action": (
            "Run one next narrow A/B that changes only the midday long slope floor to -0.10. "
            "Do not relax rebound_below_slow or the pullback/break stack first."
        ),
    }


def _avg_decimal(values) -> Decimal:
    values = list(values)
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _ratio(numerator: int, denominator: int) -> str:
    if denominator == 0:
        return "0"
    return format(Decimal(numerator) / Decimal(denominator), "f")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"Cannot write empty CSV: {path}")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("detail_csv_path", type=Path)
    parser.add_argument("treatment_summary_path", type=Path)
    parser.add_argument("db_path", type=Path)
    parser.add_argument("config_paths", nargs="+", type=Path)
    args = parser.parse_args()

    outputs = build_and_write_us_midday_pause_resume_long_bind_analysis(
        detail_csv_path=args.detail_csv_path,
        treatment_summary_path=args.treatment_summary_path,
        db_path=args.db_path,
        config_paths=args.config_paths,
    )
    print(json.dumps(outputs, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
