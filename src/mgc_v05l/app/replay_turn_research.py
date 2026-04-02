"""Post-run EMA turn research built on persisted replay artifacts."""

from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from ..research.causal_momentum import compute_causal_momentum_features
from .session_phase_labels import label_session_phase


PIVOT_RADIUS = 2
TURN_HORIZON_BARS = 20
MATERIAL_MOVE_ATR_MULTIPLIER = Decimal("1.0")
LATE_ENTRY_BARS = 2
POOR_ENTRY_EFFICIENCY_PCT = Decimal("40")
SLOPE_BUCKET_EDGES = (
    (Decimal("-0.50"), "SLOPE_STRONG_NEG"),
    (Decimal("-0.10"), "SLOPE_NEG"),
    (Decimal("0.10"), "SLOPE_FLAT"),
    (Decimal("0.50"), "SLOPE_POS"),
)
CURVATURE_BUCKET_EDGES = (
    (Decimal("-0.50"), "CURVATURE_STRONG_NEG"),
    (Decimal("-0.10"), "CURVATURE_NEG"),
    (Decimal("0.10"), "CURVATURE_FLAT"),
    (Decimal("0.50"), "CURVATURE_POS"),
)
MIN_BUCKET_ROWS = 20
MATERIAL_SPREAD_IMPROVEMENT = 0.05


@dataclass(frozen=True)
class TurnResearchBar:
    bar_id: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    session: str
    atr: Decimal
    vwap: Optional[Decimal]
    turn_ema_fast: Optional[Decimal]
    turn_ema_slow: Optional[Decimal]
    velocity: Optional[Decimal]
    velocity_delta: Optional[Decimal]
    bar_range: Optional[Decimal]
    vol_ratio: Optional[Decimal]


@dataclass(frozen=True)
class TurnLedgerRow:
    trade_id: int
    direction: str
    entry_ts: datetime
    entry_px: Decimal
    exit_ts: datetime
    exit_px: Decimal
    qty: int
    gross_pnl: Decimal
    fees: Decimal
    slippage: Decimal
    net_pnl: Decimal
    exit_reason: str
    setup_family: str
    entry_session: str
    exit_session: str
    entry_session_phase: str = ""
    exit_session_phase: str = ""


@dataclass(frozen=True)
class TurnDatasetRow:
    turn_id: int
    timestamp: datetime
    session: str
    session_phase: str
    time_bucket: str
    direction_of_turn: str
    local_turn_type: str
    price: Decimal
    vwap: Optional[Decimal]
    vwap_distance: Optional[Decimal]
    atr: Decimal
    turn_ema_fast: Optional[Decimal]
    turn_ema_slow: Optional[Decimal]
    first_derivative: Decimal
    second_derivative: Decimal
    normalized_slope: Decimal
    normalized_curvature: Decimal
    slope_bucket: str
    curvature_bucket: str
    derivative_bucket: str
    strategy_participated: bool
    participation_classification: str
    trade_id_if_any: Optional[int]
    signal_family_if_any: Optional[str]
    entry_delay_bars: Optional[int]
    entry_efficiency_pct: Optional[Decimal]
    move_5bar: Decimal
    move_10bar: Decimal
    move_20bar: Decimal
    mfe_20bar: Decimal
    mae_20bar: Decimal
    material_turn: bool
    range_expansion_ratio: Optional[Decimal]
    volatility_regime: str


def load_replay_summary(summary_path: Path) -> dict[str, Any]:
    return json.loads(summary_path.read_text(encoding="utf-8"))


def load_replay_bars(replay_db_path: Path, *, ticker: str = "MGC", timeframe: str | None = None) -> list[TurnResearchBar]:
    selected_timeframe = str(timeframe or "5m")
    connection = sqlite3.connect(replay_db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            select
              b.bar_id,
              b.timestamp,
              b.open,
              b.high,
              b.low,
              b.close,
              b.session_asia,
              b.session_london,
              b.session_us,
              f.payload_json
            from bars b
            left join features f on f.bar_id = b.bar_id
            where b.ticker = ? and b.timeframe = ?
            order by b.timestamp asc
            """,
            (ticker, selected_timeframe),
        ).fetchall()
    finally:
        connection.close()

    bars: list[TurnResearchBar] = []
    for row in rows:
        payload = _decode_payload_json(row["payload_json"]) if row["payload_json"] else {}
        bars.append(
            TurnResearchBar(
                bar_id=row["bar_id"],
                timestamp=datetime.fromisoformat(row["timestamp"]),
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                session=_session_from_flags(bool(row["session_asia"]), bool(row["session_london"]), bool(row["session_us"])),
                atr=_to_decimal(payload.get("atr")) or Decimal("0"),
                vwap=_to_decimal(payload.get("vwap")),
                turn_ema_fast=_to_decimal(payload.get("turn_ema_fast")),
                turn_ema_slow=_to_decimal(payload.get("turn_ema_slow")),
                velocity=_to_decimal(payload.get("velocity")),
                velocity_delta=_to_decimal(payload.get("velocity_delta")),
                bar_range=_to_decimal(payload.get("bar_range")),
                vol_ratio=_to_decimal(payload.get("vol_ratio")),
            )
        )
    return bars


def load_trade_ledger(ledger_path: Path) -> list[TurnLedgerRow]:
    rows: list[TurnLedgerRow] = []
    with ledger_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append(
                TurnLedgerRow(
                    trade_id=int(row["trade_id"]),
                    direction=row["direction"],
                    entry_ts=datetime.fromisoformat(row["entry_ts"]),
                    entry_px=Decimal(row["entry_px"]),
                    exit_ts=datetime.fromisoformat(row["exit_ts"]),
                    exit_px=Decimal(row["exit_px"]),
                    qty=int(row["qty"]),
                    gross_pnl=Decimal(row["gross_pnl"]),
                    fees=Decimal(row["fees"]),
                    slippage=Decimal(row["slippage"]),
                    net_pnl=Decimal(row["net_pnl"]),
                    exit_reason=row["exit_reason"],
                    setup_family=row["setup_family"],
                    entry_session=row["entry_session"],
                    entry_session_phase=row.get("entry_session_phase") or label_session_phase(datetime.fromisoformat(row["entry_ts"])),
                    exit_session=row["exit_session"],
                    exit_session_phase=row.get("exit_session_phase") or label_session_phase(datetime.fromisoformat(row["exit_ts"])),
                )
            )
    return rows


def build_turn_dataset_rows(bars: list[TurnResearchBar], ledger: list[TurnLedgerRow]) -> list[TurnDatasetRow]:
    if not bars:
        return []

    momentum_features = compute_causal_momentum_features(
        prices=[bar.close for bar in bars],
        volatility_scale=[max(bar.atr, Decimal("0.01")) for bar in bars],
        smoothing_length=3,
        normalization_floor=Decimal("0.01"),
    )
    bars_by_ts = {bar.timestamp: index for index, bar in enumerate(bars)}
    rows: list[TurnDatasetRow] = []
    turn_id = 0

    for index in range(PIVOT_RADIUS, max(PIVOT_RADIUS, len(bars) - TURN_HORIZON_BARS)):
        bar = bars[index]
        direction, local_turn_type = _classify_turn(bars, index)
        if direction is None or local_turn_type is None:
            continue

        future_window = bars[index + 1 : index + TURN_HORIZON_BARS + 1]
        if not future_window:
            continue

        move_5 = _directional_move(bar.close, future_window[:5], direction)
        move_10 = _directional_move(bar.close, future_window[:10], direction)
        move_20 = _directional_move(bar.close, future_window[:20], direction)
        mfe = _favorable_excursion_from_turn(bar.close, future_window[:20], direction)
        mae = _adverse_excursion_from_turn(bar.close, future_window[:20], direction)
        threshold = max(bar.atr, Decimal("5")) * MATERIAL_MOVE_ATR_MULTIPLIER
        material_turn = mfe >= threshold

        taken_trade = _find_same_direction_trade(ledger, bar.timestamp, direction)
        strategy_participated = taken_trade is not None
        trade_id_if_any: Optional[int] = None
        signal_family_if_any: Optional[str] = None
        entry_delay_bars: Optional[int] = None
        entry_efficiency_pct: Optional[Decimal] = None
        participation_classification = "no_trade"

        if taken_trade is not None:
            trade_id_if_any = taken_trade.trade_id
            signal_family_if_any = taken_trade.setup_family
            entry_index = bars_by_ts.get(taken_trade.entry_ts)
            if entry_index is not None:
                entry_delay_bars = entry_index - index
            entry_efficiency_pct = _turn_entry_efficiency_pct(bar.close, taken_trade.entry_px, future_window[:20], direction)
            if (entry_delay_bars or 0) > LATE_ENTRY_BARS:
                participation_classification = "late_entry"
            elif (entry_efficiency_pct or Decimal("0")) < POOR_ENTRY_EFFICIENCY_PCT:
                participation_classification = "poor_entry"
            else:
                participation_classification = "good_entry"

        momentum = momentum_features[index]
        vwap_distance = (bar.close - bar.vwap) if bar.vwap is not None else None
        slope_bucket = _bucketize(momentum.normalized_slope, SLOPE_BUCKET_EDGES, "SLOPE_STRONG_POS")
        curvature_bucket = _bucketize(momentum.normalized_curvature, CURVATURE_BUCKET_EDGES, "CURVATURE_STRONG_POS")

        turn_id += 1
        rows.append(
            TurnDatasetRow(
                turn_id=turn_id,
                timestamp=bar.timestamp,
                session=bar.session,
                session_phase=label_session_phase(bar.timestamp),
                time_bucket=_time_bucket(bar.timestamp),
                direction_of_turn=direction,
                local_turn_type=local_turn_type,
                price=bar.close,
                vwap=bar.vwap,
                vwap_distance=vwap_distance,
                atr=bar.atr,
                turn_ema_fast=bar.turn_ema_fast,
                turn_ema_slow=bar.turn_ema_slow,
                first_derivative=momentum.first_derivative,
                second_derivative=momentum.second_derivative,
                normalized_slope=momentum.normalized_slope,
                normalized_curvature=momentum.normalized_curvature,
                slope_bucket=slope_bucket,
                curvature_bucket=curvature_bucket,
                derivative_bucket=f"{slope_bucket}|{curvature_bucket}",
                strategy_participated=strategy_participated,
                participation_classification=participation_classification,
                trade_id_if_any=trade_id_if_any,
                signal_family_if_any=signal_family_if_any,
                entry_delay_bars=entry_delay_bars,
                entry_efficiency_pct=entry_efficiency_pct,
                move_5bar=move_5,
                move_10bar=move_10,
                move_20bar=move_20,
                mfe_20bar=mfe,
                mae_20bar=mae,
                material_turn=material_turn,
                range_expansion_ratio=_range_expansion_ratio(bar),
                volatility_regime=_volatility_regime(bar),
            )
        )

    return rows


def build_turn_summary(rows: list[TurnDatasetRow]) -> dict[str, Any]:
    material_rows = [row for row in rows if row.material_turn]
    missed_rows = [row for row in material_rows if row.participation_classification != "good_entry"]

    directional_findings = {
        "bearish": _top_bucket_rows(material_rows, direction="SHORT"),
        "bullish": _top_bucket_rows(material_rows, direction="LONG"),
    }
    first_only_spread = _bucket_spread(material_rows, lambda row: row.slope_bucket)
    combined_spread = _bucket_spread(material_rows, lambda row: row.derivative_bucket)

    return {
        "candidate_turn_count": len(rows),
        "material_turn_count": len(material_rows),
        "participation_count_by_classification": dict(Counter(row.participation_classification for row in material_rows)),
        "material_turn_count_by_session": dict(Counter(row.session for row in material_rows)),
        "material_turn_count_by_direction": dict(Counter(row.direction_of_turn for row in material_rows)),
        "missed_turn_count": len(missed_rows),
        "missed_turn_count_by_session": dict(Counter(row.session for row in missed_rows)),
        "missed_turn_count_by_direction": dict(Counter(row.direction_of_turn for row in missed_rows)),
        "missed_turn_count_by_time_bucket": dict(Counter(row.time_bucket for row in missed_rows)),
        "average_move_after_material_turn_5bar": _average_decimal([row.move_5bar for row in material_rows]),
        "average_move_after_material_turn_10bar": _average_decimal([row.move_10bar for row in material_rows]),
        "average_move_after_material_turn_20bar": _average_decimal([row.move_20bar for row in material_rows]),
        "top_derivative_regimes_for_missed_bearish_turns": directional_findings["bearish"],
        "top_derivative_regimes_for_missed_bullish_turns": directional_findings["bullish"],
        "first_derivative_only_bucket_spread": first_only_spread,
        "first_plus_second_derivative_bucket_spread": combined_spread,
        "first_derivative_alone_enough": first_only_spread >= combined_spread - MATERIAL_SPREAD_IMPROVEMENT,
        "first_plus_second_derivative_materially_better": combined_spread >= first_only_spread + MATERIAL_SPREAD_IMPROVEMENT,
        "missed_turns_concentrated_by_session": _top_counter_items(Counter(row.session for row in missed_rows), limit=4),
    }


def build_derivative_bin_rows(rows: list[TurnDatasetRow]) -> list[dict[str, Any]]:
    material_rows = [row for row in rows if row.material_turn]
    results: list[dict[str, Any]] = []
    for dimension, key_fn in (
        ("slope_bucket", lambda row: row.slope_bucket),
        ("derivative_bucket", lambda row: row.derivative_bucket),
    ):
        grouped: dict[tuple[str, str], list[TurnDatasetRow]] = defaultdict(list)
        for row in material_rows:
            grouped[(row.direction_of_turn, key_fn(row))].append(row)
        for (direction, bucket), bucket_rows in sorted(grouped.items()):
            missed = [row for row in bucket_rows if row.participation_classification != "good_entry"]
            results.append(
                {
                    "dimension": dimension,
                    "direction": direction,
                    "bucket": bucket,
                    "material_turn_count": len(bucket_rows),
                    "missed_turn_count": len(missed),
                    "missed_turn_rate": _ratio(len(missed), len(bucket_rows)),
                    "good_entry_count": len([row for row in bucket_rows if row.participation_classification == "good_entry"]),
                    "average_move_10bar": _average_decimal([row.move_10bar for row in bucket_rows]),
                    "average_mfe_20bar": _average_decimal([row.mfe_20bar for row in bucket_rows]),
                }
            )
    return results


def build_missed_turns_by_derivative_bucket_rows(rows: list[TurnDatasetRow]) -> list[dict[str, Any]]:
    missed_rows = [row for row in rows if row.material_turn and row.participation_classification != "good_entry"]
    grouped: dict[tuple[str, str, str], list[TurnDatasetRow]] = defaultdict(list)
    for row in missed_rows:
        grouped[(row.direction_of_turn, row.session, row.derivative_bucket)].append(row)

    return [
        {
            "direction": direction,
            "session": session,
            "derivative_bucket": bucket,
            "missed_turn_count": len(bucket_rows),
            "average_move_10bar": _average_decimal([row.move_10bar for row in bucket_rows]),
            "average_mfe_20bar": _average_decimal([row.mfe_20bar for row in bucket_rows]),
        }
        for (direction, session, bucket), bucket_rows in sorted(grouped.items())
    ]


def build_entry_quality_by_derivative_bucket_rows(rows: list[TurnDatasetRow]) -> list[dict[str, Any]]:
    participated = [row for row in rows if row.strategy_participated and row.entry_efficiency_pct is not None]
    grouped: dict[tuple[str, str], list[TurnDatasetRow]] = defaultdict(list)
    for row in participated:
        grouped[(row.direction_of_turn, row.derivative_bucket)].append(row)

    return [
        {
            "direction": direction,
            "derivative_bucket": bucket,
            "participated_turn_count": len(bucket_rows),
            "good_entry_count": len([row for row in bucket_rows if row.participation_classification == "good_entry"]),
            "late_entry_count": len([row for row in bucket_rows if row.participation_classification == "late_entry"]),
            "poor_entry_count": len([row for row in bucket_rows if row.participation_classification == "poor_entry"]),
            "average_entry_efficiency_pct": _average_decimal([row.entry_efficiency_pct for row in bucket_rows if row.entry_efficiency_pct is not None]),
        }
        for (direction, bucket), bucket_rows in sorted(grouped.items())
    ]


def write_turn_dataset_csv(rows: list[TurnDatasetRow], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(asdict(rows[0]).keys()) if rows else [
        "turn_id",
        "timestamp",
        "session",
        "session_phase",
        "time_bucket",
        "direction_of_turn",
        "local_turn_type",
        "price",
        "vwap",
        "vwap_distance",
        "atr",
        "turn_ema_fast",
        "turn_ema_slow",
        "first_derivative",
        "second_derivative",
        "normalized_slope",
        "normalized_curvature",
        "slope_bucket",
        "curvature_bucket",
        "derivative_bucket",
        "strategy_participated",
        "participation_classification",
        "trade_id_if_any",
        "signal_family_if_any",
        "entry_delay_bars",
        "entry_efficiency_pct",
        "move_5bar",
        "move_10bar",
        "move_20bar",
        "mfe_20bar",
        "mae_20bar",
        "material_turn",
        "range_expansion_ratio",
        "volatility_regime",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = asdict(row)
            payload["timestamp"] = row.timestamp.isoformat()
            for key, value in payload.items():
                if isinstance(value, Decimal):
                    payload[key] = str(value)
            writer.writerow(payload)
    return output_path


def write_rows_csv(rows: list[dict[str, Any]], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return output_path
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _stringify_for_csv(value) for key, value in row.items()})
    return output_path


def write_summary_json(summary: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def build_and_write_replay_turn_research(summary_path: Path) -> dict[str, str]:
    summary = load_replay_summary(summary_path)
    replay_db_path = Path(summary["replay_db_path"])
    ledger_path = Path(summary["trade_ledger_path"])
    selected_timeframe = str(summary.get("artifact_timeframe") or summary.get("target_timeframe") or "5m")

    bars = load_replay_bars(replay_db_path, timeframe=selected_timeframe)
    ledger = load_trade_ledger(ledger_path)
    turn_rows = build_turn_dataset_rows(bars, ledger)
    turn_summary = build_turn_summary(turn_rows)
    turn_summary["study_mode"] = str(summary.get("environment_mode") or "baseline_parity_mode")
    turn_summary["timeframe_truth"] = {
        "structural_signal_timeframe": summary.get("structural_signal_timeframe") or selected_timeframe,
        "execution_timeframe": summary.get("execution_timeframe") or selected_timeframe,
        "artifact_timeframe": summary.get("artifact_timeframe") or selected_timeframe,
    }
    turn_summary["source_replay_timeframe"] = selected_timeframe
    derivative_bin_rows = build_derivative_bin_rows(turn_rows)
    missed_by_bucket_rows = build_missed_turns_by_derivative_bucket_rows(turn_rows)
    entry_quality_rows = build_entry_quality_by_derivative_bucket_rows(turn_rows)

    prefix = summary_path.with_suffix("")
    output_paths = {
        "turn_dataset_path": str(write_turn_dataset_csv(turn_rows, prefix.with_suffix(".turn_dataset.csv"))),
        "turn_summary_path": str(write_summary_json(turn_summary, prefix.with_suffix(".turn_summary.json"))),
        "derivative_bins_path": str(write_rows_csv(derivative_bin_rows, prefix.with_suffix(".derivative_bins.csv"))),
        "missed_turns_by_derivative_bucket_path": str(
            write_rows_csv(missed_by_bucket_rows, prefix.with_suffix(".missed_turns_by_derivative_bucket.csv"))
        ),
        "entry_quality_by_derivative_bucket_path": str(
            write_rows_csv(entry_quality_rows, prefix.with_suffix(".entry_quality_by_derivative_bucket.csv"))
        ),
    }
    return output_paths


def _decode_payload_json(payload_json: str) -> dict[str, Any]:
    raw = json.loads(payload_json)
    return {key: _deserialize_payload_value(value) for key, value in raw.items()}


def _deserialize_payload_value(value: Any) -> Any:
    if not isinstance(value, dict) or "__type__" not in value:
        return value
    value_type = value["__type__"]
    if value_type == "decimal":
        return Decimal(value["value"])
    if value_type == "datetime":
        return datetime.fromisoformat(value["value"])
    if value_type == "enum":
        return value["value"]
    return value


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _session_from_flags(session_asia: bool, session_london: bool, session_us: bool) -> str:
    if session_asia:
        return "ASIA"
    if session_london:
        return "LONDON"
    if session_us:
        return "US"
    return "OFF"


def _classify_turn(bars: list[TurnResearchBar], index: int) -> tuple[Optional[str], Optional[str]]:
    window = bars[index - PIVOT_RADIUS : index + PIVOT_RADIUS + 1]
    current = bars[index]
    previous = bars[index - 1]
    if current.low == min(bar.low for bar in window) and current.close > current.open and current.close > previous.close:
        return "LONG", "pivot_low_reversal"
    if current.high == max(bar.high for bar in window) and current.close < current.open and current.close < previous.close:
        return "SHORT", "pivot_high_reversal"
    return None, None


def _directional_move(turn_price: Decimal, future_bars: list[TurnResearchBar], direction: str) -> Decimal:
    if not future_bars:
        return Decimal("0")
    later_close = future_bars[-1].close
    return later_close - turn_price if direction == "LONG" else turn_price - later_close


def _favorable_excursion_from_turn(turn_price: Decimal, future_bars: list[TurnResearchBar], direction: str) -> Decimal:
    if not future_bars:
        return Decimal("0")
    if direction == "LONG":
        return max(bar.high for bar in future_bars) - turn_price
    return turn_price - min(bar.low for bar in future_bars)


def _adverse_excursion_from_turn(turn_price: Decimal, future_bars: list[TurnResearchBar], direction: str) -> Decimal:
    if not future_bars:
        return Decimal("0")
    if direction == "LONG":
        return turn_price - min(bar.low for bar in future_bars)
    return max(bar.high for bar in future_bars) - turn_price


def _find_same_direction_trade(ledger: list[TurnLedgerRow], turn_ts: datetime, direction: str) -> Optional[TurnLedgerRow]:
    horizon_end = turn_ts.timestamp() + TURN_HORIZON_BARS * 5 * 60
    for trade in ledger:
        if trade.direction != direction:
            continue
        if trade.entry_ts.timestamp() < turn_ts.timestamp():
            continue
        if trade.entry_ts.timestamp() > horizon_end:
            continue
        return trade
    return None


def _turn_entry_efficiency_pct(
    turn_price: Decimal,
    entry_price: Decimal,
    future_bars: list[TurnResearchBar],
    direction: str,
) -> Decimal:
    if not future_bars:
        return Decimal("0")
    if direction == "LONG":
        best_future = max(bar.high for bar in future_bars)
        denominator = best_future - turn_price
        numerator = best_future - entry_price
    else:
        best_future = min(bar.low for bar in future_bars)
        denominator = turn_price - best_future
        numerator = entry_price - best_future
    if denominator <= 0:
        return Decimal("0")
    ratio = Decimal("100") * numerator / denominator
    return max(Decimal("0"), min(Decimal("100"), ratio))


def _range_expansion_ratio(bar: TurnResearchBar) -> Optional[Decimal]:
    if bar.bar_range is None or bar.atr <= 0:
        return None
    return bar.bar_range / bar.atr


def _volatility_regime(bar: TurnResearchBar) -> str:
    range_ratio = _range_expansion_ratio(bar)
    if range_ratio is not None and range_ratio >= Decimal("1.25"):
        return "HIGH"
    if bar.vol_ratio is not None and bar.vol_ratio >= Decimal("1.20"):
        return "HIGH"
    return "NORMAL"


def _time_bucket(timestamp: datetime) -> str:
    return f"{timestamp.hour:02d}:00"


def _bucketize(value: Decimal, edges: tuple[tuple[Decimal, str], ...], default: str) -> str:
    for threshold, label in edges:
        if value <= threshold:
            return label
    return default


def _bucket_spread(rows: list[TurnDatasetRow], key_fn) -> float:
    grouped: dict[str, list[TurnDatasetRow]] = defaultdict(list)
    for row in rows:
        grouped[key_fn(row)].append(row)
    rates = [
        _ratio(
            len([row for row in bucket_rows if row.participation_classification != "good_entry"]),
            len(bucket_rows),
        )
        for bucket_rows in grouped.values()
        if len(bucket_rows) >= MIN_BUCKET_ROWS
    ]
    if len(rates) < 2:
        return 0.0
    return max(rates) - min(rates)


def _top_bucket_rows(rows: list[TurnDatasetRow], *, direction: str) -> list[dict[str, Any]]:
    directional_rows = [row for row in rows if row.direction_of_turn == direction]
    grouped: dict[str, list[TurnDatasetRow]] = defaultdict(list)
    for row in directional_rows:
        grouped[row.derivative_bucket].append(row)

    ranked = []
    for bucket, bucket_rows in grouped.items():
        if len(bucket_rows) < MIN_BUCKET_ROWS:
            continue
        missed_rows = [row for row in bucket_rows if row.participation_classification != "good_entry"]
        ranked.append(
            {
                "derivative_bucket": bucket,
                "turn_count": len(bucket_rows),
                "missed_turn_count": len(missed_rows),
                "missed_turn_rate": _ratio(len(missed_rows), len(bucket_rows)),
                "average_mfe_20bar": _average_decimal([row.mfe_20bar for row in bucket_rows]),
            }
        )
    return sorted(ranked, key=lambda row: (row["missed_turn_rate"], row["average_mfe_20bar"]), reverse=True)[:5]


def _top_counter_items(counter: Counter[str], *, limit: int) -> list[dict[str, Any]]:
    return [{"key": key, "count": value} for key, value in counter.most_common(limit)]


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _average_decimal(values: list[Decimal]) -> float:
    if not values:
        return 0.0
    return float(sum(values, Decimal("0")) / Decimal(len(values)))


def _stringify_for_csv(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, float):
        return f"{value:.10f}"
    return value
