"""Research-only separator analysis for widened derivative-bear trades."""

from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from .session_phase_labels import label_session_phase
from .us_derivative_bear_regime_analysis import assign_slice_name


DERIVATIVE_FAMILY = "usDerivativeBearTurn"


@dataclass(frozen=True)
class SeparatorTradeRow:
    cohort: str
    source_variant: str
    trade_id: int
    entry_ts: datetime
    entry_minute_bucket: str
    slice_name: str
    session_phase: str
    net_pnl: Decimal
    win: bool
    exit_reason: str
    bars_held: int
    entry_distance_vwap_atr: Decimal | None
    entry_distance_fast_ema_atr: Decimal | None
    entry_distance_slow_ema_atr: Decimal | None
    ema_spread_atr: Decimal | None
    entry_efficiency_5: Decimal | None
    mfe: Decimal | None
    mae: Decimal | None
    mfe_capture_pct: Decimal | None
    initial_favorable_3bar: Decimal | None
    initial_adverse_3bar: Decimal | None
    body_atr: Decimal | None
    close_location: Decimal | None
    normalized_slope: Decimal | None
    normalized_curvature: Decimal | None
    upside_stretch_atr: Decimal | None
    vol_ratio: Decimal | None
    extension_context: str


def build_and_write_widened_derivative_bear_separator_analysis(
    *,
    anchor_summary_path: Path,
    widened_summary_path: Path,
    reference_bad_summary_path: Path,
) -> dict[str, str]:
    anchor_run = _load_run(anchor_summary_path, "anchor")
    widened_run = _load_run(widened_summary_path, "widened")
    reference_bad_run = _load_run(reference_bad_summary_path, "reference_bad")

    slice_boundaries = _load_slice_boundaries(Path(anchor_run["run"]["source_db_path"]))

    anchor_keys = {_trade_key(trade) for trade in anchor_run["trades"]}
    widened_keys = {_trade_key(trade) for trade in widened_run["trades"]}

    current_added = [trade for trade in widened_run["trades"] if _trade_key(trade) not in anchor_keys]
    current_removed = [trade for trade in anchor_run["trades"] if _trade_key(trade) not in widened_keys]
    reference_bad_added_losers = [
        trade
        for trade in reference_bad_run["trades"]
        if _trade_key(trade) not in anchor_keys and trade["net_pnl"] < 0
    ]

    flattened_rows: list[SeparatorTradeRow] = []
    for trade in current_added:
        flattened_rows.extend(
            _build_separator_rows(
                trades=[trade],
                variant_label="widened",
                cohort_label="current_added_winner" if trade["net_pnl"] > 0 else "current_added_loser",
                slice_boundaries=slice_boundaries,
            )
        )
    for trade in current_removed:
        flattened_rows.extend(
            _build_separator_rows(
                trades=[trade],
                variant_label="anchor",
                cohort_label="removed_anchor_middle"
                if assign_slice_name(trade["entry_ts"], slice_boundaries) == "middle"
                else "removed_anchor_other",
                slice_boundaries=slice_boundaries,
            )
        )
    for trade in reference_bad_added_losers:
        flattened_rows.extend(
            _build_separator_rows(
                trades=[trade],
                variant_label="reference_bad",
                cohort_label="reference_bad_added_loser",
                slice_boundaries=slice_boundaries,
            )
        )
    current_added_winners = [row for row in flattened_rows if row.cohort == "current_added_winner"]
    current_added_losers = [row for row in flattened_rows if row.cohort == "current_added_loser"]
    current_open_late_winners = [
        row
        for row in current_added_winners
        if row.session_phase == "US_OPEN_LATE"
    ]
    removed_anchor_middle = [row for row in flattened_rows if row.cohort == "removed_anchor_middle"]
    reference_bad_losers = [row for row in flattened_rows if row.cohort == "reference_bad_added_loser"]

    prefix = Path(str(widened_summary_path).removesuffix(".summary.json"))
    detail_path = prefix.with_suffix(".separator_trade_detail.csv")
    comparison_path = prefix.with_suffix(".separator_cohort_comparison.csv")
    summary_path = prefix.with_suffix(".separator_summary.json")

    _write_csv(detail_path, [asdict(row) for row in flattened_rows])
    _write_csv(comparison_path, _build_cohort_comparison_rows(flattened_rows))

    summary = _build_separator_summary(
        current_added_winners=current_added_winners,
        current_added_losers=current_added_losers,
        current_open_late_winners=current_open_late_winners,
        removed_anchor_middle=removed_anchor_middle,
        reference_bad_losers=reference_bad_losers,
    )
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "separator_trade_detail_path": str(detail_path),
        "separator_cohort_comparison_path": str(comparison_path),
        "separator_summary_path": str(summary_path),
    }


def best_next_gate_hypothesis(summary: dict[str, Any]) -> str:
    return str(summary["best_next_gate_hypothesis"])


def _build_separator_rows(
    *,
    trades: list[dict[str, Any]],
    variant_label: str,
    cohort_label: str,
    slice_boundaries: dict[str, datetime],
) -> list[SeparatorTradeRow]:
    rows: list[SeparatorTradeRow] = []
    for trade in trades:
        entry_ts = trade["entry_ts"]
        entry_fast = trade["entry_distance_fast_ema_atr"]
        entry_slow = trade["entry_distance_slow_ema_atr"]
        ema_spread_atr = None
        if entry_fast is not None and entry_slow is not None:
            ema_spread_atr = abs(entry_slow - entry_fast)
        rows.append(
            SeparatorTradeRow(
                cohort=cohort_label,
                source_variant=variant_label,
                trade_id=trade["trade_id"],
                entry_ts=entry_ts,
                entry_minute_bucket=entry_ts.strftime("%H:%M"),
                slice_name=assign_slice_name(entry_ts, slice_boundaries),
                session_phase=trade["session_phase"],
                net_pnl=trade["net_pnl"],
                win=trade["net_pnl"] > 0,
                exit_reason=trade["exit_reason"],
                bars_held=trade["bars_held"],
                entry_distance_vwap_atr=trade["entry_distance_vwap_atr"],
                entry_distance_fast_ema_atr=entry_fast,
                entry_distance_slow_ema_atr=entry_slow,
                ema_spread_atr=ema_spread_atr,
                entry_efficiency_5=trade["entry_efficiency_5"],
                mfe=trade["mfe"],
                mae=trade["mae"],
                mfe_capture_pct=trade["mfe_capture_pct"],
                initial_favorable_3bar=trade["initial_favorable_3bar"],
                initial_adverse_3bar=trade["initial_adverse_3bar"],
                body_atr=trade["body_atr"],
                close_location=trade["close_location"],
                normalized_slope=trade["normalized_slope"],
                normalized_curvature=trade["normalized_curvature"],
                upside_stretch_atr=trade["upside_stretch_atr"],
                vol_ratio=trade["vol_ratio"],
                extension_context=_extension_context(trade),
            )
        )
    return rows


def _build_cohort_comparison_rows(rows: list[SeparatorTradeRow]) -> list[dict[str, Any]]:
    grouped: dict[str, list[SeparatorTradeRow]] = defaultdict(list)
    for row in rows:
        grouped[row.cohort].append(row)
    output_rows: list[dict[str, Any]] = []
    for cohort, cohort_rows in sorted(grouped.items()):
        output_rows.append(
            {
                "cohort": cohort,
                "trade_count": len(cohort_rows),
                "total_net_pnl": _sum_decimal_str([row.net_pnl for row in cohort_rows]),
                "avg_net_pnl": _avg_decimal_str([row.net_pnl for row in cohort_rows]),
                "avg_entry_distance_vwap_atr": _avg_decimal_str([row.entry_distance_vwap_atr for row in cohort_rows]),
                "avg_entry_distance_fast_ema_atr": _avg_decimal_str([row.entry_distance_fast_ema_atr for row in cohort_rows]),
                "avg_entry_distance_slow_ema_atr": _avg_decimal_str([row.entry_distance_slow_ema_atr for row in cohort_rows]),
                "avg_ema_spread_atr": _avg_decimal_str([row.ema_spread_atr for row in cohort_rows]),
                "avg_body_atr": _avg_decimal_str([row.body_atr for row in cohort_rows]),
                "avg_close_location": _avg_decimal_str([row.close_location for row in cohort_rows]),
                "avg_entry_efficiency_5": _avg_decimal_str([row.entry_efficiency_5 for row in cohort_rows]),
                "avg_mfe": _avg_decimal_str([row.mfe for row in cohort_rows]),
                "avg_mae": _avg_decimal_str([row.mae for row in cohort_rows]),
                "avg_initial_favorable_3bar": _avg_decimal_str([row.initial_favorable_3bar for row in cohort_rows]),
                "avg_initial_adverse_3bar": _avg_decimal_str([row.initial_adverse_3bar for row in cohort_rows]),
                "dominant_session_phase": _dominant_value([row.session_phase for row in cohort_rows]),
                "dominant_entry_minute_bucket": _dominant_value([row.entry_minute_bucket for row in cohort_rows]),
                "dominant_exit_reason": _dominant_value([row.exit_reason for row in cohort_rows]),
                "dominant_extension_context": _dominant_value([row.extension_context for row in cohort_rows]),
            }
        )
    return output_rows


def _build_separator_summary(
    *,
    current_added_winners: list[SeparatorTradeRow],
    current_added_losers: list[SeparatorTradeRow],
    current_open_late_winners: list[SeparatorTradeRow],
    removed_anchor_middle: list[SeparatorTradeRow],
    reference_bad_losers: list[SeparatorTradeRow],
) -> dict[str, Any]:
    bad_comparison_group = removed_anchor_middle + reference_bad_losers
    ranked_features = _rank_numeric_separators(
        current_open_late_winners or current_added_winners,
        bad_comparison_group,
    )
    added_winner_vs_bad_loser = _rank_numeric_separators(current_added_winners, reference_bad_losers)
    summary = {
        "current_added_winner_count": len(current_added_winners),
        "current_added_loser_count": len(current_added_losers),
        "current_open_late_winner_count": len(current_open_late_winners),
        "removed_anchor_middle_count": len(removed_anchor_middle),
        "reference_bad_loser_count": len(reference_bad_losers),
        "current_added_winner_minutes": sorted({row.entry_minute_bucket for row in current_added_winners}),
        "current_added_winner_phases": dict(Counter(row.session_phase for row in current_added_winners)),
        "removed_anchor_middle_minutes": sorted({row.entry_minute_bucket for row in removed_anchor_middle}),
        "removed_anchor_middle_phases": dict(Counter(row.session_phase for row in removed_anchor_middle)),
        "reference_bad_loser_minutes": sorted({row.entry_minute_bucket for row in reference_bad_losers}),
        "reference_bad_loser_phases": dict(Counter(row.session_phase for row in reference_bad_losers)),
        "good_us_open_late_vs_bad_middle_ranked_features": ranked_features,
        "added_winners_vs_reference_bad_losers_ranked_features": added_winner_vs_bad_loser,
        "cohort_notes": {
            "current_added_losers_absent": len(current_added_losers) == 0,
            "middle_slice_damage_is_displacement": len(current_added_losers) == 0 and len(removed_anchor_middle) > 0,
        },
        "best_next_gate_hypothesis": (
            "Test a VWAP-extension band only for US_OPEN_LATE widened trades: keep the existing <= 1.8 ATR cap, "
            "and add a minimum downside extension floor around 1.2 ATR below VWAP, while leaving US_CASH_OPEN_IMPULSE unchanged."
        ),
    }
    return summary


def _rank_numeric_separators(
    good_rows: list[SeparatorTradeRow],
    bad_rows: list[SeparatorTradeRow],
) -> list[dict[str, Any]]:
    feature_specs = [
        ("entry_distance_vwap_atr", "VWAP extension at entry"),
        ("entry_distance_fast_ema_atr", "fast EMA distance at entry"),
        ("entry_distance_slow_ema_atr", "slow EMA distance at entry"),
        ("ema_spread_atr", "fast-vs-slow EMA spread magnitude"),
        ("body_atr", "ATR-scaled bar body"),
        ("close_location", "close location in bar"),
        ("entry_efficiency_5", "entry_efficiency_5"),
        ("mfe", "MFE"),
        ("mae", "MAE"),
        ("mfe_capture_pct", "MFE capture pct"),
        ("initial_favorable_3bar", "initial favorable 3-bar"),
        ("initial_adverse_3bar", "initial adverse 3-bar"),
        ("normalized_slope", "normalized slope"),
        ("normalized_curvature", "normalized curvature"),
        ("upside_stretch_atr", "upside stretch"),
        ("vol_ratio", "vol ratio"),
    ]
    ranked: list[dict[str, Any]] = []
    for attr, label in feature_specs:
        good_values = [getattr(row, attr) for row in good_rows if getattr(row, attr) is not None]
        bad_values = [getattr(row, attr) for row in bad_rows if getattr(row, attr) is not None]
        if not good_values or not bad_values:
            continue
        good_mean = sum(good_values, Decimal("0")) / Decimal(len(good_values))
        bad_mean = sum(bad_values, Decimal("0")) / Decimal(len(bad_values))
        scale = max(abs(good_mean), abs(bad_mean), Decimal("1"))
        score = abs(good_mean - bad_mean) / scale
        ranked.append(
            {
                "feature": label,
                "good_mean": str(good_mean),
                "bad_mean": str(bad_mean),
                "absolute_gap": str(abs(good_mean - bad_mean)),
                "relative_gap": str(score),
            }
        )
    ranked.sort(key=lambda row: Decimal(row["relative_gap"]), reverse=True)
    return ranked[:8]


def _load_run(summary_path: Path, variant_label: str) -> dict[str, Any]:
    run = json.loads(summary_path.read_text(encoding="utf-8"))
    with Path(run["trade_ledger_path"]).open(encoding="utf-8", newline="") as handle:
        trade_rows = list(csv.DictReader(handle))

    signal_context = _load_signal_context_by_entry_ts(Path(run["replay_db_path"]))
    trades = []
    for row in trade_rows:
        family = row.get("setup_family") or row.get("signal_family")
        if family != DERIVATIVE_FAMILY:
            continue
        entry_ts = datetime.fromisoformat(row["entry_ts"])
        context = signal_context.get(entry_ts, {})
        entry_fast = _to_decimal(row.get("entry_distance_fast_ema_atr"))
        entry_slow = _to_decimal(row.get("entry_distance_slow_ema_atr"))
        trades.append(
            {
                "variant": variant_label,
                "trade_id": int(row["trade_id"]),
                "entry_ts": entry_ts,
                "entry_px": Decimal(row["entry_px"]),
                "net_pnl": Decimal(row["net_pnl"]),
                "exit_reason": row["exit_reason"],
                "bars_held": int(row["bars_held"]) if row["bars_held"] else 0,
                "entry_distance_vwap_atr": _to_decimal(row.get("entry_distance_vwap_atr")),
                "entry_distance_fast_ema_atr": entry_fast,
                "entry_distance_slow_ema_atr": entry_slow,
                "entry_efficiency_5": _to_decimal(row.get("entry_efficiency_5")),
                "mfe": _to_decimal(row.get("mfe")),
                "mae": _to_decimal(row.get("mae")),
                "mfe_capture_pct": _to_decimal(row.get("mfe_capture_pct")),
                "initial_favorable_3bar": _to_decimal(row.get("initial_favorable_3bar")),
                "initial_adverse_3bar": _to_decimal(row.get("initial_adverse_3bar")),
                "session_phase": row.get("entry_session_phase") or label_session_phase(entry_ts),
                "body_atr": context.get("body_atr"),
                "close_location": context.get("close_location"),
                "normalized_slope": context.get("normalized_slope"),
                "normalized_curvature": context.get("normalized_curvature"),
                "upside_stretch_atr": context.get("upside_stretch_atr"),
                "vol_ratio": context.get("vol_ratio"),
            }
        )
    return {"run": run, "trades": trades}


def _load_signal_context_by_entry_ts(replay_db_path: Path) -> dict[datetime, dict[str, Decimal | None]]:
    connection = sqlite3.connect(replay_db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            select
              f.fill_timestamp,
              feat.payload_json,
              b.close,
              b.low
            from order_intents oi
            join fills f on f.order_intent_id = oi.order_intent_id
            join bars b on b.bar_id = oi.bar_id
            left join features feat on feat.bar_id = oi.bar_id
            where oi.intent_type = 'SELL_TO_OPEN' and oi.reason_code = ?
            order by f.fill_timestamp asc
            """,
            (DERIVATIVE_FAMILY,),
        ).fetchall()
    finally:
        connection.close()

    by_entry_ts: dict[datetime, dict[str, Decimal | None]] = {}
    for row in rows:
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        atr = _payload_decimal(payload.get("atr"))
        if atr is None or atr == 0:
            continue
        bar_range = _payload_decimal(payload.get("bar_range")) or Decimal("0")
        body_size = _payload_decimal(payload.get("body_size")) or Decimal("0")
        close = Decimal(str(row["close"]))
        low = Decimal(str(row["low"]))
        close_location = None if bar_range == 0 else (close - low) / bar_range
        by_entry_ts[datetime.fromisoformat(row["fill_timestamp"])] = {
            "normalized_slope": (_payload_decimal(payload.get("velocity")) or Decimal("0")) / atr,
            "normalized_curvature": (_payload_decimal(payload.get("velocity_delta")) or Decimal("0")) / atr,
            "body_atr": body_size / atr,
            "close_location": close_location,
            "upside_stretch_atr": (_payload_decimal(payload.get("upside_stretch")) or Decimal("0")) / atr,
            "vol_ratio": _payload_decimal(payload.get("vol_ratio")),
        }
    return by_entry_ts


def _load_slice_boundaries(source_db_path: Path) -> dict[str, datetime]:
    connection = sqlite3.connect(source_db_path)
    try:
        timestamps = [
            datetime.fromisoformat(row[0])
            for row in connection.execute(
                """
                select timestamp
                from bars
                where ticker = 'MGC' and timeframe = '5m'
                order by timestamp asc
                """
            ).fetchall()
        ]
    finally:
        connection.close()
    count = len(timestamps)
    return {
        "middle_start": timestamps[count // 3],
        "recent_start": timestamps[(2 * count) // 3],
    }


def _extension_context(trade: dict[str, Any]) -> str:
    vwap_dist = trade.get("entry_distance_vwap_atr")
    body_atr = trade.get("body_atr")
    close_location = trade.get("close_location")
    if vwap_dist is None:
        return "unknown"
    if vwap_dist <= Decimal("-1.8") and (body_atr is None or body_atr < Decimal("0.45")):
        return "prior_downside_extension"
    if (
        body_atr is not None
        and close_location is not None
        and body_atr >= Decimal("0.45")
        and close_location <= Decimal("0.25")
        and vwap_dist > Decimal("-1.8")
    ):
        return "fresh_downside_acceleration"
    return "mixed"


def _trade_key(trade: dict[str, Any]) -> tuple[str, str]:
    return (trade["entry_ts"].isoformat(), str(trade["entry_px"]))


def _payload_decimal(value: Any) -> Decimal | None:
    if isinstance(value, dict) and value.get("__type__") == "decimal":
        return Decimal(value["value"])
    return None


def _to_decimal(value: str | None) -> Decimal | None:
    if value in ("", None):
        return None
    return Decimal(value)


def _dominant_value(values: list[str]) -> str:
    return Counter(values).most_common(1)[0][0] if values else ""


def _avg_decimal_str(values: list[Decimal | None]) -> str:
    usable = [value for value in values if value is not None]
    if not usable:
        return ""
    return str(sum(usable, Decimal("0")) / Decimal(len(usable)))


def _sum_decimal_str(values: list[Decimal]) -> str:
    return str(sum(values, Decimal("0")))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        rows = [{}]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    key: (
                        value.isoformat()
                        if isinstance(value, datetime)
                        else str(value)
                        if isinstance(value, Decimal)
                        else value
                    )
                    for key, value in row.items()
                }
            )
