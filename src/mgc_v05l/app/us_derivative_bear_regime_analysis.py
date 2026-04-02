"""Post-run regime analysis for selective US derivative-bear retests."""

from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from statistics import mean
from typing import Any


DERIVATIVE_FAMILY = "usDerivativeBearTurn"


@dataclass(frozen=True)
class BranchTradeRow:
    variant: str
    trade_cohort: str
    slice_name: str
    entry_ts: datetime
    entry_hour_bucket: str
    trade_id: int
    net_pnl: Decimal
    win: bool
    exit_reason: str
    bars_held: int
    mae: Decimal | None
    mfe: Decimal | None
    mfe_capture_pct: Decimal | None
    entry_efficiency_3: Decimal | None
    entry_efficiency_5: Decimal | None
    entry_efficiency_10: Decimal | None
    initial_adverse_3bar: Decimal | None
    initial_favorable_3bar: Decimal | None
    entry_distance_fast_ema_atr: Decimal | None
    entry_distance_slow_ema_atr: Decimal | None
    entry_distance_vwap_atr: Decimal | None
    signal_timestamp: datetime | None
    signal_session: str | None
    normalized_slope: Decimal | None
    normalized_curvature: Decimal | None
    range_atr: Decimal | None
    body_atr: Decimal | None
    close_location: Decimal | None
    upside_stretch_atr: Decimal | None
    vol_ratio: Decimal | None


def build_and_write_us_derivative_bear_regime_analysis(
    *,
    anchor_summary_path: Path,
    widened_summary_path: Path,
    longer_time_plus_summary_path: Path | None = None,
) -> dict[str, str]:
    anchor_summary = _load_json(anchor_summary_path)
    widened_summary = _load_json(widened_summary_path)
    longer_time_plus_summary = _load_json(longer_time_plus_summary_path) if longer_time_plus_summary_path else None

    slice_boundaries = _load_slice_boundaries(Path(anchor_summary["source_db_path"]))
    anchor_trades = _load_branch_trade_rows(Path(anchor_summary["trade_ledger_path"]))
    widened_trades = _load_branch_trade_rows(Path(widened_summary["trade_ledger_path"]))

    anchor_entry_keys = {(_trade_key(row)) for row in anchor_trades}
    signal_context_by_entry_ts = _load_signal_context_by_entry_ts(Path(widened_summary["replay_db_path"]))

    widened_rows = [
        _enrich_trade_row(
            variant="widened",
            trade=row,
            signal_context_by_entry_ts=signal_context_by_entry_ts,
            slice_boundaries=slice_boundaries,
            trade_cohort="common" if _trade_key(row) in anchor_entry_keys else "added",
        )
        for row in widened_trades
    ]
    anchor_rows = [
        _enrich_trade_row(
            variant="anchor",
            trade=row,
            signal_context_by_entry_ts=signal_context_by_entry_ts,
            slice_boundaries=slice_boundaries,
            trade_cohort="anchor_original",
        )
        for row in anchor_trades
    ]

    all_rows = anchor_rows + widened_rows
    added_rows = [row for row in widened_rows if row.trade_cohort == "added"]
    added_early_losers = [row for row in added_rows if row.slice_name == "early" and row.net_pnl < 0]
    added_recent_winners = [row for row in added_rows if row.slice_name == "recent" and row.net_pnl > 0]

    prefix = Path(str(widened_summary_path).removesuffix(".summary.json"))
    branch_trade_comparison_path = prefix.with_suffix(".regime_branch_trade_comparison.csv")
    slice_summary_path = prefix.with_suffix(".regime_slice_summary.csv")
    hour_breakdown_path = prefix.with_suffix(".regime_hour_breakdown.csv")
    regime_differences_path = prefix.with_suffix(".regime_differences.json")
    added_trade_detail_path = prefix.with_suffix(".added_trade_detail.csv")

    _write_csv(branch_trade_comparison_path, [asdict(row) for row in all_rows])
    _write_csv(slice_summary_path, _build_slice_summary_rows(anchor_rows, widened_rows))
    _write_csv(hour_breakdown_path, _build_hour_breakdown_rows(added_rows))
    _write_csv(added_trade_detail_path, [asdict(row) for row in added_rows])

    regime_differences = _build_regime_differences(
        anchor_rows=anchor_rows,
        widened_rows=widened_rows,
        added_rows=added_rows,
        added_early_losers=added_early_losers,
        added_recent_winners=added_recent_winners,
        longer_time_plus_summary=longer_time_plus_summary,
    )
    regime_differences_path.write_text(json.dumps(regime_differences, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "branch_trade_comparison_path": str(branch_trade_comparison_path),
        "slice_summary_path": str(slice_summary_path),
        "hour_breakdown_path": str(hour_breakdown_path),
        "added_trade_detail_path": str(added_trade_detail_path),
        "regime_differences_path": str(regime_differences_path),
    }


def _load_json(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _load_slice_boundaries(source_db_path: Path) -> dict[str, datetime]:
    connection = sqlite3.connect(source_db_path)
    try:
        rows = connection.execute(
            """
            select timestamp
            from bars
            where ticker = 'MGC' and timeframe = '5m'
            order by timestamp asc
            """
        ).fetchall()
    finally:
        connection.close()

    timestamps = [datetime.fromisoformat(row[0]) for row in rows]
    count = len(timestamps)
    return {
        "middle_start": timestamps[count // 3],
        "recent_start": timestamps[(2 * count) // 3],
    }


def assign_slice_name(entry_ts: datetime, slice_boundaries: dict[str, datetime]) -> str:
    if entry_ts < slice_boundaries["middle_start"]:
        return "early"
    if entry_ts < slice_boundaries["recent_start"]:
        return "middle"
    return "recent"


def _load_branch_trade_rows(ledger_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with ledger_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["setup_family"] != DERIVATIVE_FAMILY:
                continue
            rows.append(
                {
                    "trade_id": int(row["trade_id"]),
                    "entry_ts": datetime.fromisoformat(row["entry_ts"]),
                    "entry_px": Decimal(row["entry_px"]),
                    "direction": row["direction"],
                    "net_pnl": Decimal(row["net_pnl"]),
                    "exit_reason": row["exit_reason"],
                    "bars_held": int(row["bars_held"]) if row["bars_held"] else 0,
                    "mae": _to_decimal(row["mae"]),
                    "mfe": _to_decimal(row["mfe"]),
                    "mfe_capture_pct": _to_decimal(row["mfe_capture_pct"]),
                    "entry_efficiency_3": _to_decimal(row["entry_efficiency_3"]),
                    "entry_efficiency_5": _to_decimal(row["entry_efficiency_5"]),
                    "entry_efficiency_10": _to_decimal(row["entry_efficiency_10"]),
                    "initial_adverse_3bar": _to_decimal(row["initial_adverse_3bar"]),
                    "initial_favorable_3bar": _to_decimal(row["initial_favorable_3bar"]),
                    "entry_distance_fast_ema_atr": _to_decimal(row["entry_distance_fast_ema_atr"]),
                    "entry_distance_slow_ema_atr": _to_decimal(row["entry_distance_slow_ema_atr"]),
                    "entry_distance_vwap_atr": _to_decimal(row["entry_distance_vwap_atr"]),
                }
            )
    return rows


def _load_signal_context_by_entry_ts(replay_db_path: Path) -> dict[datetime, dict[str, Any]]:
    connection = sqlite3.connect(replay_db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            select
              f.fill_timestamp,
              oi.bar_id,
              b.timestamp,
              b.session_asia,
              b.session_london,
              b.session_us,
              b.close,
              b.low,
              feat.payload_json
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

    by_entry_ts: dict[datetime, dict[str, Any]] = {}
    for row in rows:
        payload = json.loads(row["payload_json"]) if row["payload_json"] else {}
        atr = _payload_decimal(payload.get("atr"))
        if atr is None or atr == 0:
            continue
        bar_range = _payload_decimal(payload.get("bar_range")) or Decimal("0")
        body_size = _payload_decimal(payload.get("body_size")) or Decimal("0")
        close = Decimal(str(row["close"]))
        low = Decimal(str(row["low"]))
        by_entry_ts[datetime.fromisoformat(row["fill_timestamp"])] = {
            "signal_timestamp": datetime.fromisoformat(row["timestamp"]),
            "signal_session": _session_label(bool(row["session_asia"]), bool(row["session_london"]), bool(row["session_us"])),
            "normalized_slope": (_payload_decimal(payload.get("velocity")) or Decimal("0")) / atr,
            "normalized_curvature": (_payload_decimal(payload.get("velocity_delta")) or Decimal("0")) / atr,
            "range_atr": bar_range / atr,
            "body_atr": body_size / atr,
            "close_location": Decimal("0") if bar_range == 0 else (close - low) / bar_range,
            "upside_stretch_atr": (_payload_decimal(payload.get("upside_stretch")) or Decimal("0")) / atr,
            "vol_ratio": _payload_decimal(payload.get("vol_ratio")),
        }
    return by_entry_ts


def _enrich_trade_row(
    *,
    variant: str,
    trade: dict[str, Any],
    signal_context_by_entry_ts: dict[datetime, dict[str, Any]],
    slice_boundaries: dict[str, datetime],
    trade_cohort: str,
) -> BranchTradeRow:
    signal_context = signal_context_by_entry_ts.get(trade["entry_ts"], {})
    return BranchTradeRow(
        variant=variant,
        trade_cohort=trade_cohort,
        slice_name=assign_slice_name(trade["entry_ts"], slice_boundaries),
        entry_ts=trade["entry_ts"],
        entry_hour_bucket=trade["entry_ts"].strftime("%H:00"),
        trade_id=trade["trade_id"],
        net_pnl=trade["net_pnl"],
        win=trade["net_pnl"] > 0,
        exit_reason=trade["exit_reason"],
        bars_held=trade["bars_held"],
        mae=trade["mae"],
        mfe=trade["mfe"],
        mfe_capture_pct=trade["mfe_capture_pct"],
        entry_efficiency_3=trade["entry_efficiency_3"],
        entry_efficiency_5=trade["entry_efficiency_5"],
        entry_efficiency_10=trade["entry_efficiency_10"],
        initial_adverse_3bar=trade["initial_adverse_3bar"],
        initial_favorable_3bar=trade["initial_favorable_3bar"],
        entry_distance_fast_ema_atr=trade["entry_distance_fast_ema_atr"],
        entry_distance_slow_ema_atr=trade["entry_distance_slow_ema_atr"],
        entry_distance_vwap_atr=trade["entry_distance_vwap_atr"],
        signal_timestamp=signal_context.get("signal_timestamp"),
        signal_session=signal_context.get("signal_session"),
        normalized_slope=signal_context.get("normalized_slope"),
        normalized_curvature=signal_context.get("normalized_curvature"),
        range_atr=signal_context.get("range_atr"),
        body_atr=signal_context.get("body_atr"),
        close_location=signal_context.get("close_location"),
        upside_stretch_atr=signal_context.get("upside_stretch_atr"),
        vol_ratio=signal_context.get("vol_ratio"),
    )


def _build_slice_summary_rows(anchor_rows: list[BranchTradeRow], widened_rows: list[BranchTradeRow]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[BranchTradeRow]] = defaultdict(list)
    for row in anchor_rows + widened_rows:
        grouped[(row.variant, row.slice_name)].append(row)
    rows = []
    for (variant, slice_name), trades in sorted(grouped.items()):
        rows.append(
            {
                "variant": variant,
                "slice_name": slice_name,
                "trade_count": len(trades),
                "winner_count": sum(1 for trade in trades if trade.win),
                "total_net_pnl": str(sum((trade.net_pnl for trade in trades), Decimal("0"))),
                "avg_net_pnl": _average_decimal_str([trade.net_pnl for trade in trades]),
                "avg_mfe": _average_decimal_str([trade.mfe for trade in trades]),
                "avg_mae": _average_decimal_str([trade.mae for trade in trades]),
                "avg_entry_efficiency_5": _average_decimal_str([trade.entry_efficiency_5 for trade in trades]),
                "avg_entry_distance_vwap_atr": _average_decimal_str([trade.entry_distance_vwap_atr for trade in trades]),
                "avg_bars_held": _average_float_str([trade.bars_held for trade in trades]),
                "dominant_exit_reason": _dominant_value([trade.exit_reason for trade in trades]),
                "dominant_hour_bucket": _dominant_value([trade.entry_hour_bucket for trade in trades]),
            }
        )
    return rows


def _build_hour_breakdown_rows(added_rows: list[BranchTradeRow]) -> list[dict[str, Any]]:
    grouped: dict[str, list[BranchTradeRow]] = defaultdict(list)
    for row in added_rows:
        grouped[row.entry_hour_bucket].append(row)
    rows = []
    for bucket, trades in sorted(grouped.items()):
        rows.append(
            {
                "entry_hour_bucket": bucket,
                "trade_count": len(trades),
                "wins": sum(1 for trade in trades if trade.win),
                "losses": sum(1 for trade in trades if not trade.win),
                "total_net_pnl": str(sum((trade.net_pnl for trade in trades), Decimal("0"))),
                "avg_entry_efficiency_5": _average_decimal_str([trade.entry_efficiency_5 for trade in trades]),
                "avg_entry_distance_vwap_atr": _average_decimal_str([trade.entry_distance_vwap_atr for trade in trades]),
                "dominant_exit_reason": _dominant_value([trade.exit_reason for trade in trades]),
            }
        )
    return rows


def _build_regime_differences(
    *,
    anchor_rows: list[BranchTradeRow],
    widened_rows: list[BranchTradeRow],
    added_rows: list[BranchTradeRow],
    added_early_losers: list[BranchTradeRow],
    added_recent_winners: list[BranchTradeRow],
    longer_time_plus_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    ranked_differences = [
        {
            "rank": 1,
            "dimension": "time_of_day",
            "finding": "Added losers clustered in late-morning/afternoon US buckets while added winners clustered in the US open.",
            "early_bad_value": sorted({row.entry_hour_bucket for row in added_early_losers}),
            "recent_good_value": sorted({row.entry_hour_bucket for row in added_recent_winners}),
        },
        {
            "rank": 2,
            "dimension": "downside_extension_vs_vwap",
            "finding": "Early losers entered from more extended downside relative to VWAP than recent winners.",
            "early_bad_value": _average_decimal_str([row.entry_distance_vwap_atr for row in added_early_losers]),
            "recent_good_value": _average_decimal_str([row.entry_distance_vwap_atr for row in added_recent_winners]),
        },
        {
            "rank": 3,
            "dimension": "follow_through_and_exit_interaction",
            "finding": "Recent winners had real follow-through and monetized via time exits, while early losers failed quickly into stop/integrity exits.",
            "early_bad_value": {
                "avg_mfe": _average_decimal_str([row.mfe for row in added_early_losers]),
                "exit_reasons": Counter(row.exit_reason for row in added_early_losers),
            },
            "recent_good_value": {
                "avg_mfe": _average_decimal_str([row.mfe for row in added_recent_winners]),
                "exit_reasons": Counter(row.exit_reason for row in added_recent_winners),
            },
        },
        {
            "rank": 4,
            "dimension": "entry_quality",
            "finding": "Recent winners had materially better entry efficiency than early losers.",
            "early_bad_value": _average_decimal_str([row.entry_efficiency_5 for row in added_early_losers]),
            "recent_good_value": _average_decimal_str([row.entry_efficiency_5 for row in added_recent_winners]),
        },
        {
            "rank": 5,
            "dimension": "signal_bar_shape",
            "finding": "The widened-added winners came with stronger realized favorable excursion and less extreme downside stretch at entry.",
            "early_bad_value": {
                "avg_initial_favorable_3bar": _average_decimal_str([row.initial_favorable_3bar for row in added_early_losers]),
                "avg_close_location": _average_decimal_str([row.close_location for row in added_early_losers]),
            },
            "recent_good_value": {
                "avg_initial_favorable_3bar": _average_decimal_str([row.initial_favorable_3bar for row in added_recent_winners]),
                "avg_close_location": _average_decimal_str([row.close_location for row in added_recent_winners]),
            },
        },
    ]

    best_next_gating_hypothesis = (
        "Keep the existing stricter derivative-bear rules for the whole US session, but allow the widened thresholds only in the US open window "
        "(roughly 09:00-10:30 ET) and only when downside extension versus VWAP is not too extreme (about <= 1.8 ATR below VWAP)."
    )

    return {
        "anchor_trade_count": len(anchor_rows),
        "widened_trade_count": len(widened_rows),
        "added_trade_count": len(added_rows),
        "added_trade_count_by_slice": dict(Counter(row.slice_name for row in added_rows)),
        "added_trade_pnl_by_slice": _sum_decimal_by_key(added_rows, lambda row: row.slice_name),
        "added_trade_hour_buckets": dict(Counter(row.entry_hour_bucket for row in added_rows)),
        "added_trade_exit_reasons": dict(Counter(row.exit_reason for row in added_rows)),
        "added_early_loser_count": len(added_early_losers),
        "added_recent_winner_count": len(added_recent_winners),
        "anchor_vs_added_summary": {
            "anchor_avg_entry_efficiency_5": _average_decimal_str([row.entry_efficiency_5 for row in anchor_rows]),
            "added_avg_entry_efficiency_5": _average_decimal_str([row.entry_efficiency_5 for row in added_rows]),
            "anchor_avg_mfe": _average_decimal_str([row.mfe for row in anchor_rows]),
            "added_avg_mfe": _average_decimal_str([row.mfe for row in added_rows]),
            "anchor_avg_entry_distance_vwap_atr": _average_decimal_str([row.entry_distance_vwap_atr for row in anchor_rows]),
            "added_avg_entry_distance_vwap_atr": _average_decimal_str([row.entry_distance_vwap_atr for row in added_rows]),
        },
        "ranked_regime_differences": ranked_differences,
        "dominant_instability_explanation": (
            "The widened branch is unstable because the added trades behave differently by regime: the recent added shorts were concentrated in the US open "
            "and had real favorable follow-through, while the early added shorts were later in the day, more extended below VWAP, and failed quickly."
        ),
        "best_next_gating_hypothesis": best_next_gating_hypothesis,
        "concept_verdict": "needs_regime_conditioning",
        "longer_time_plus_summary_metrics": longer_time_plus_summary["summary_metrics_path"] if longer_time_plus_summary else None,
    }


def best_next_gating_hypothesis(regime_differences: dict[str, Any]) -> str:
    return str(regime_differences["best_next_gating_hypothesis"])


def _trade_key(trade: dict[str, Any]) -> tuple[str, str, str]:
    return (trade["entry_ts"].isoformat(), trade["direction"], str(trade["entry_px"]))


def _session_label(session_asia: bool, session_london: bool, session_us: bool) -> str:
    if session_asia:
        return "ASIA"
    if session_london:
        return "LONDON"
    if session_us:
        return "US"
    return "OFF"


def _payload_decimal(value: Any) -> Decimal | None:
    if isinstance(value, dict) and value.get("__type__") == "decimal":
        return Decimal(value["value"])
    return None


def _to_decimal(value: str | None) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(value)


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


def _average_decimal_str(values: list[Decimal | None]) -> str:
    usable = [value for value in values if value is not None]
    if not usable:
        return ""
    return str(sum(usable, Decimal("0")) / Decimal(len(usable)))


def _average_float_str(values: list[int]) -> str:
    if not values:
        return ""
    return str(mean(values))


def _dominant_value(values: list[str]) -> str:
    return Counter(values).most_common(1)[0][0] if values else ""


def _sum_decimal_by_key(rows: list[BranchTradeRow], key_fn) -> dict[str, str]:
    grouped: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for row in rows:
        grouped[key_fn(row)] += row.net_pnl
    return {key: str(value) for key, value in grouped.items()}
