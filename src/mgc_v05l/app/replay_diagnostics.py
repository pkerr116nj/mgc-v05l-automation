"""Post-run replay diagnostics for missed turns and weak entries."""

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


PIVOT_RADIUS = 2
TURN_HORIZON_BARS = 20
MATERIAL_MOVE_ATR_MULTIPLIER = Decimal("1.0")
LATE_ENTRY_BARS = 2
POOR_ENTRY_EFFICIENCY_PCT = Decimal("40")
BAD_ENTRY_EFFICIENCY_PCT = Decimal("35")


@dataclass(frozen=True)
class DiagnosticBar:
    bar_id: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    session: str
    atr: Optional[Decimal]
    bar_range: Optional[Decimal]
    vol_ratio: Optional[Decimal]


@dataclass(frozen=True)
class LedgerRow:
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


@dataclass(frozen=True)
class MissedTurnRow:
    turn_id: int
    timestamp: datetime
    session: str
    direction: str
    local_turn_type: str
    price_at_turn: Decimal
    price_5_bars_later: Optional[Decimal]
    price_10_bars_later: Optional[Decimal]
    price_20_bars_later: Optional[Decimal]
    move_5bar: Decimal
    move_10bar: Decimal
    move_20bar: Decimal
    mfe_if_entered_at_turn: Decimal
    was_trade_taken: bool
    trade_id_if_any: Optional[int]
    entry_delay_bars: Optional[int]
    entry_efficiency_pct: Optional[Decimal]
    signal_family_if_any: Optional[str]
    range_expansion_ratio: Optional[Decimal]
    vol_ratio: Optional[Decimal]
    volatility_regime: str
    classifier: str


@dataclass(frozen=True)
class BadEntryRow:
    trade_id: int
    entry_ts: datetime
    session: str
    direction: str
    signal_family: str
    entry_px: Decimal
    best_px_next_5: Decimal
    best_px_next_10: Decimal
    worst_px_next_5: Decimal
    worst_px_next_10: Decimal
    entry_efficiency_pct: Decimal
    immediate_adverse_excursion: Decimal
    immediate_favorable_excursion: Decimal
    net_pnl: Decimal
    exit_reason: str
    range_expansion_ratio: Optional[Decimal]
    vol_ratio: Optional[Decimal]
    volatility_regime: str


def load_replay_summary(summary_path: Path) -> dict[str, Any]:
    return json.loads(summary_path.read_text(encoding="utf-8"))


def load_source_bars(source_db_path: Path, *, ticker: str = "MGC", timeframe: str = "5m") -> list[DiagnosticBar]:
    connection = sqlite3.connect(source_db_path)
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
            (ticker, timeframe),
        ).fetchall()
    finally:
        connection.close()

    bars: list[DiagnosticBar] = []
    for row in rows:
        feature_payload = _decode_payload_json(row["payload_json"]) if row["payload_json"] else {}
        atr = _to_decimal(feature_payload.get("atr"))
        bar_range = _to_decimal(feature_payload.get("bar_range"))
        vol_ratio = _to_decimal(feature_payload.get("vol_ratio"))
        bars.append(
            DiagnosticBar(
                bar_id=row["bar_id"],
                timestamp=datetime.fromisoformat(row["timestamp"]),
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                session=_session_from_flags(bool(row["session_asia"]), bool(row["session_london"]), bool(row["session_us"])),
                atr=atr,
                bar_range=bar_range,
                vol_ratio=vol_ratio,
            )
        )
    return bars


def load_trade_ledger(ledger_path: Path) -> list[LedgerRow]:
    rows: list[LedgerRow] = []
    with ledger_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append(
                LedgerRow(
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
                    exit_session=row["exit_session"],
                )
            )
    return rows


def build_missed_turn_rows(bars: list[DiagnosticBar], ledger: list[LedgerRow]) -> list[MissedTurnRow]:
    bars_by_ts = {bar.timestamp: index for index, bar in enumerate(bars)}
    rows: list[MissedTurnRow] = []
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
        threshold = max(bar.atr or Decimal("0"), Decimal("5")) * MATERIAL_MOVE_ATR_MULTIPLIER
        if mfe < threshold:
            continue

        taken_trade = _find_same_direction_trade(ledger, bar.timestamp, direction)
        was_trade_taken = taken_trade is not None
        entry_delay_bars: Optional[int] = None
        entry_efficiency_pct: Optional[Decimal] = None
        classifier = "no_trade"
        signal_family_if_any: Optional[str] = None
        trade_id_if_any: Optional[int] = None
        if taken_trade is not None:
            signal_family_if_any = taken_trade.setup_family
            trade_id_if_any = taken_trade.trade_id
            entry_index = bars_by_ts.get(taken_trade.entry_ts)
            if entry_index is not None:
                entry_delay_bars = entry_index - index
            entry_efficiency_pct = _turn_entry_efficiency_pct(bar.close, taken_trade.entry_px, future_window[:20], direction)
            if (entry_delay_bars or 0) > LATE_ENTRY_BARS:
                classifier = "late_entry"
            elif (entry_efficiency_pct or Decimal("0")) < POOR_ENTRY_EFFICIENCY_PCT:
                classifier = "poor_entry"
            else:
                continue

        turn_id += 1
        rows.append(
            MissedTurnRow(
                turn_id=turn_id,
                timestamp=bar.timestamp,
                session=bar.session,
                direction=direction,
                local_turn_type=local_turn_type,
                price_at_turn=bar.close,
                price_5_bars_later=_close_at_horizon(future_window, 5),
                price_10_bars_later=_close_at_horizon(future_window, 10),
                price_20_bars_later=_close_at_horizon(future_window, 20),
                move_5bar=move_5,
                move_10bar=move_10,
                move_20bar=move_20,
                mfe_if_entered_at_turn=mfe,
                was_trade_taken=was_trade_taken,
                trade_id_if_any=trade_id_if_any,
                entry_delay_bars=entry_delay_bars,
                entry_efficiency_pct=entry_efficiency_pct,
                signal_family_if_any=signal_family_if_any,
                range_expansion_ratio=_range_expansion_ratio(bar),
                vol_ratio=bar.vol_ratio,
                volatility_regime=_volatility_regime(bar),
                classifier=classifier,
            )
        )

    return rows


def build_bad_entry_rows(bars: list[DiagnosticBar], ledger: list[LedgerRow]) -> list[BadEntryRow]:
    bars_by_ts = {bar.timestamp: index for index, bar in enumerate(bars)}
    rows: list[BadEntryRow] = []

    for trade in ledger:
        entry_index = bars_by_ts.get(trade.entry_ts)
        if entry_index is None:
            continue
        future_5 = bars[entry_index + 1 : entry_index + 6]
        future_10 = bars[entry_index + 1 : entry_index + 11]
        if not future_10:
            continue

        best_5 = _best_price(future_5, trade.direction)
        best_10 = _best_price(future_10, trade.direction)
        worst_5 = _worst_price(future_5, trade.direction)
        worst_10 = _worst_price(future_10, trade.direction)
        favorable = _favorable_from_entry(trade.entry_px, future_10, trade.direction)
        adverse = _adverse_from_entry(trade.entry_px, future_10, trade.direction)
        efficiency = _entry_efficiency_pct(favorable, adverse)
        if efficiency >= BAD_ENTRY_EFFICIENCY_PCT:
            continue

        bar = bars[entry_index]
        rows.append(
            BadEntryRow(
                trade_id=trade.trade_id,
                entry_ts=trade.entry_ts,
                session=trade.entry_session,
                direction=trade.direction,
                signal_family=trade.setup_family,
                entry_px=trade.entry_px,
                best_px_next_5=best_5,
                best_px_next_10=best_10,
                worst_px_next_5=worst_5,
                worst_px_next_10=worst_10,
                entry_efficiency_pct=efficiency,
                immediate_adverse_excursion=adverse,
                immediate_favorable_excursion=favorable,
                net_pnl=trade.net_pnl,
                exit_reason=trade.exit_reason,
                range_expansion_ratio=_range_expansion_ratio(bar),
                vol_ratio=bar.vol_ratio,
                volatility_regime=_volatility_regime(bar),
            )
        )

    return rows


def build_missed_turn_summary(rows: list[MissedTurnRow]) -> dict[str, Any]:
    by_session = Counter(row.session for row in rows)
    by_direction = Counter(row.direction for row in rows)
    by_time_bucket = Counter(_time_bucket(row.timestamp) for row in rows)
    top_windows = [
        {
            "turn_id": row.turn_id,
            "timestamp": row.timestamp.isoformat(),
            "direction": row.direction,
            "session": row.session,
            "classifier": row.classifier,
            "mfe_if_entered_at_turn": float(row.mfe_if_entered_at_turn),
        }
        for row in sorted(rows, key=lambda item: item.mfe_if_entered_at_turn, reverse=True)[:10]
    ]
    return {
        "missed_turn_count": len(rows),
        "missed_turn_count_by_session": dict(by_session),
        "missed_turn_count_by_direction": dict(by_direction),
        "missed_turn_count_by_time_bucket": dict(by_time_bucket),
        "top_missed_turn_windows": top_windows,
        "average_move_after_missed_turn_5bar": _average_decimal([row.move_5bar for row in rows]),
        "average_move_after_missed_turn_10bar": _average_decimal([row.move_10bar for row in rows]),
        "average_move_after_missed_turn_20bar": _average_decimal([row.move_20bar for row in rows]),
    }


def build_bad_entry_summary(rows: list[BadEntryRow]) -> dict[str, Any]:
    by_signal_family = Counter(row.signal_family for row in rows)
    by_session = Counter(row.session for row in rows)
    average_entry_efficiency = _average_decimal([row.entry_efficiency_pct for row in rows])
    signal_efficiency = _group_average(rows, key=lambda row: row.signal_family, value=lambda row: row.entry_efficiency_pct)
    session_efficiency = _group_average(rows, key=lambda row: row.session, value=lambda row: row.entry_efficiency_pct)
    worst_signal_families = sorted(signal_efficiency.items(), key=lambda item: item[1])[:5]
    worst_sessions = sorted(session_efficiency.items(), key=lambda item: item[1])[:5]
    return {
        "bad_entry_count": len(rows),
        "bad_entry_count_by_signal_family": dict(by_signal_family),
        "bad_entry_count_by_session": dict(by_session),
        "average_entry_efficiency_pct": average_entry_efficiency,
        "worst_signal_families_by_entry_efficiency": [(key, float(value)) for key, value in worst_signal_families],
        "worst_sessions_by_entry_efficiency": [(key, float(value)) for key, value in worst_sessions],
    }


def write_missed_turns_csv(rows: list[MissedTurnRow], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "turn_id",
        "timestamp",
        "session",
        "direction",
        "local_turn_type",
        "price_at_turn",
        "price_5_bars_later",
        "price_10_bars_later",
        "price_20_bars_later",
        "move_5bar",
        "move_10bar",
        "move_20bar",
        "mfe_if_entered_at_turn",
        "was_trade_taken",
        "trade_id_if_any",
        "entry_delay_bars",
        "entry_efficiency_pct",
        "signal_family_if_any",
        "range_expansion_ratio",
        "vol_ratio",
        "volatility_regime",
        "classifier",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = asdict(row)
            payload["timestamp"] = row.timestamp.isoformat()
            for key in (
                "price_at_turn",
                "price_5_bars_later",
                "price_10_bars_later",
                "price_20_bars_later",
                "move_5bar",
                "move_10bar",
                "move_20bar",
                "mfe_if_entered_at_turn",
                "entry_efficiency_pct",
                "range_expansion_ratio",
                "vol_ratio",
            ):
                payload[key] = _to_string(payload[key])
            writer.writerow(payload)
    return output_path


def write_bad_entries_csv(rows: list[BadEntryRow], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "trade_id",
        "entry_ts",
        "session",
        "direction",
        "signal_family",
        "entry_px",
        "best_px_next_5",
        "best_px_next_10",
        "worst_px_next_5",
        "worst_px_next_10",
        "entry_efficiency_pct",
        "immediate_adverse_excursion",
        "immediate_favorable_excursion",
        "net_pnl",
        "exit_reason",
        "range_expansion_ratio",
        "vol_ratio",
        "volatility_regime",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = asdict(row)
            payload["entry_ts"] = row.entry_ts.isoformat()
            for key in (
                "entry_px",
                "best_px_next_5",
                "best_px_next_10",
                "worst_px_next_5",
                "worst_px_next_10",
                "entry_efficiency_pct",
                "immediate_adverse_excursion",
                "immediate_favorable_excursion",
                "net_pnl",
                "range_expansion_ratio",
                "vol_ratio",
            ):
                payload[key] = _to_string(payload[key])
            writer.writerow(payload)
    return output_path


def write_summary_json(summary: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def write_counter_csv(counter: Counter[str], output_path: Path, *, key_header: str, value_header: str) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[key_header, value_header])
        writer.writeheader()
        for key, value in sorted(counter.items()):
            writer.writerow({key_header: key, value_header: value})
    return output_path


def build_and_write_replay_diagnostics(summary_path: Path) -> dict[str, str]:
    summary = load_replay_summary(summary_path)
    bars = load_source_bars(Path(summary["source_db_path"]))
    ledger = load_trade_ledger(Path(summary["trade_ledger_path"]))
    missed_turns = build_missed_turn_rows(bars, ledger)
    bad_entries = build_bad_entry_rows(bars, ledger)
    missed_turn_summary = build_missed_turn_summary(missed_turns)
    bad_entry_summary = build_bad_entry_summary(bad_entries)

    prefix = summary_path.with_suffix("")
    output_paths = {
        "missed_turns_path": str(write_missed_turns_csv(missed_turns, prefix.with_suffix(".missed_turns.csv"))),
        "bad_entries_path": str(write_bad_entries_csv(bad_entries, prefix.with_suffix(".bad_entries.csv"))),
        "missed_turn_summary_path": str(
            write_summary_json(missed_turn_summary, prefix.with_suffix(".missed_turn_summary.json"))
        ),
        "bad_entry_summary_path": str(
            write_summary_json(bad_entry_summary, prefix.with_suffix(".bad_entry_summary.json"))
        ),
        "missed_turns_by_session_path": str(
            write_counter_csv(
                Counter(row.session for row in missed_turns),
                prefix.with_suffix(".missed_turns_by_session.csv"),
                key_header="session",
                value_header="missed_turn_count",
            )
        ),
        "missed_turns_by_hour_path": str(
            write_counter_csv(
                Counter(_time_bucket(row.timestamp) for row in missed_turns),
                prefix.with_suffix(".missed_turns_by_hour.csv"),
                key_header="time_bucket",
                value_header="missed_turn_count",
            )
        ),
        "bad_entries_by_signal_family_path": str(
            write_counter_csv(
                Counter(row.signal_family for row in bad_entries),
                prefix.with_suffix(".bad_entries_by_signal_family.csv"),
                key_header="signal_family",
                value_header="bad_entry_count",
            )
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


def _to_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _session_from_flags(session_asia: bool, session_london: bool, session_us: bool) -> str:
    if session_asia:
        return "ASIA"
    if session_london:
        return "LONDON"
    if session_us:
        return "US"
    return "OFF"


def _classify_turn(bars: list[DiagnosticBar], index: int) -> tuple[Optional[str], Optional[str]]:
    window = bars[index - PIVOT_RADIUS : index + PIVOT_RADIUS + 1]
    current = bars[index]
    previous = bars[index - 1]
    if current.low == min(bar.low for bar in window) and current.close > current.open and current.close > previous.close:
        return "LONG", "pivot_low_reversal"
    if current.high == max(bar.high for bar in window) and current.close < current.open and current.close < previous.close:
        return "SHORT", "pivot_high_reversal"
    return None, None


def _directional_move(turn_price: Decimal, future_bars: list[DiagnosticBar], direction: str) -> Decimal:
    if not future_bars:
        return Decimal("0")
    later_close = future_bars[-1].close
    return later_close - turn_price if direction == "LONG" else turn_price - later_close


def _favorable_excursion_from_turn(turn_price: Decimal, future_bars: list[DiagnosticBar], direction: str) -> Decimal:
    if not future_bars:
        return Decimal("0")
    if direction == "LONG":
        return max(bar.high for bar in future_bars) - turn_price
    return turn_price - min(bar.low for bar in future_bars)


def _close_at_horizon(future_bars: list[DiagnosticBar], horizon: int) -> Optional[Decimal]:
    if len(future_bars) < horizon:
        return None
    return future_bars[horizon - 1].close


def _find_same_direction_trade(ledger: list[LedgerRow], turn_ts: datetime, direction: str) -> Optional[LedgerRow]:
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
    future_bars: list[DiagnosticBar],
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


def _best_price(future_bars: list[DiagnosticBar], direction: str) -> Decimal:
    if not future_bars:
        return Decimal("0")
    return max(bar.high for bar in future_bars) if direction == "LONG" else min(bar.low for bar in future_bars)


def _worst_price(future_bars: list[DiagnosticBar], direction: str) -> Decimal:
    if not future_bars:
        return Decimal("0")
    return min(bar.low for bar in future_bars) if direction == "LONG" else max(bar.high for bar in future_bars)


def _favorable_from_entry(entry_price: Decimal, future_bars: list[DiagnosticBar], direction: str) -> Decimal:
    if not future_bars:
        return Decimal("0")
    if direction == "LONG":
        return max(bar.high for bar in future_bars) - entry_price
    return entry_price - min(bar.low for bar in future_bars)


def _adverse_from_entry(entry_price: Decimal, future_bars: list[DiagnosticBar], direction: str) -> Decimal:
    if not future_bars:
        return Decimal("0")
    if direction == "LONG":
        return entry_price - min(bar.low for bar in future_bars)
    return max(bar.high for bar in future_bars) - entry_price


def _entry_efficiency_pct(favorable: Decimal, adverse: Decimal) -> Decimal:
    total = favorable + adverse
    if total <= 0:
        return Decimal("50")
    ratio = Decimal("100") * favorable / total
    return max(Decimal("0"), min(Decimal("100"), ratio))


def _range_expansion_ratio(bar: DiagnosticBar) -> Optional[Decimal]:
    if bar.atr is None or bar.bar_range is None or bar.atr <= 0:
        return None
    return bar.bar_range / bar.atr


def _volatility_regime(bar: DiagnosticBar) -> str:
    range_ratio = _range_expansion_ratio(bar)
    vol_ratio = bar.vol_ratio
    if range_ratio is not None and range_ratio >= Decimal("1.25"):
        return "HIGH"
    if vol_ratio is not None and vol_ratio >= Decimal("1.20"):
        return "HIGH"
    return "NORMAL"


def _time_bucket(timestamp: datetime) -> str:
    return f"{timestamp.hour:02d}:00"


def _average_decimal(values: list[Decimal]) -> float:
    if not values:
        return 0.0
    return float(sum(values, Decimal("0")) / Decimal(len(values)))


def _group_average(rows: list[Any], *, key, value) -> dict[str, Decimal]:
    grouped: dict[str, list[Decimal]] = defaultdict(list)
    for row in rows:
        grouped[key(row)].append(value(row))
    return {
        group: (sum(values, Decimal("0")) / Decimal(len(values)) if values else Decimal("0"))
        for group, values in grouped.items()
    }
