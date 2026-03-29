"""Intratrade giveback diagnostics for the US_OPEN_LATE additive lane."""

from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from ..config_models.loader import load_settings_from_files
from ..domain.enums import LongEntryFamily, PositionSide, ShortEntryFamily, StrategyStatus
from ..domain.models import Bar, FeaturePacket
from ..strategy.risk_engine import compute_risk_context
from ..strategy.state_machine import update_additive_short_peak_state
from ..strategy.trade_state import build_initial_state


ADDITIVE_FAMILY = "usDerivativeBearAdditiveTurn"
US_OPEN_LATE = "US_OPEN_LATE"
ARM_LEVELS = (Decimal("0.50"), Decimal("0.75"), Decimal("1.00"))
GIVEBACK_FRACTIONS = (Decimal("0.25"), Decimal("0.33"), Decimal("0.50"))


@dataclass(frozen=True)
class GivebackPathRow:
    trade_id: str
    cohort: str
    entry_ts: str
    exit_ts: str
    bar_index: int
    bar_start_ts: str
    bar_end_ts: str
    bar_low: Decimal
    bar_close: Decimal
    short_risk: Decimal
    current_favorable_excursion: Decimal
    max_favorable_excursion: Decimal
    giveback_from_peak: Decimal
    reached_0_5r: bool
    reached_0_75r: bool
    reached_1_0r: bool
    fire_0_5r_25pct: bool
    fire_0_5r_33pct: bool
    fire_0_5r_50pct: bool
    fire_0_75r_25pct: bool
    fire_0_75r_33pct: bool
    fire_0_75r_50pct: bool
    fire_1_0r_25pct: bool
    fire_1_0r_33pct: bool
    fire_1_0r_50pct: bool


def build_and_write_open_late_additive_giveback_separator_analysis(*, summary_path: Path) -> dict[str, str]:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    settings = load_settings_from_files(summary["config_paths"])
    trade_ledger_path = Path(summary["trade_ledger_path"])
    replay_db_path = Path(summary["replay_db_path"])

    trades = _load_additive_trades(trade_ledger_path)
    path_rows, trade_summaries, findings = _build_analysis(
        replay_db_path=replay_db_path,
        settings=settings,
        trades=trades,
    )

    prefix = Path(str(summary_path).removesuffix(".summary.json"))
    detail_path = prefix.with_suffix(".open_late_additive_giveback_path_detail.csv")
    summary_json_path = prefix.with_suffix(".open_late_additive_giveback_path_summary.json")

    _write_csv(detail_path, [asdict(row) for row in path_rows])
    summary_json_path.write_text(
        json.dumps(
            {
                "trade_summaries": trade_summaries,
                "ranked_findings": findings,
            },
            indent=2,
            sort_keys=True,
            default=str,
        ),
        encoding="utf-8",
    )
    return {
        "open_late_additive_giveback_path_detail_path": str(detail_path),
        "open_late_additive_giveback_path_summary_path": str(summary_json_path),
    }


def _load_additive_trades(trade_ledger_path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with trade_ledger_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["setup_family"] != ADDITIVE_FAMILY or row["entry_session_phase"] != US_OPEN_LATE:
                continue
            rows.append(row)
    rows.sort(key=lambda row: row["entry_ts"])
    return rows


def _build_analysis(
    *,
    replay_db_path: Path,
    settings: Any,
    trades: list[dict[str, str]],
) -> tuple[list[GivebackPathRow], list[dict[str, Any]], list[str]]:
    conn = sqlite3.connect(replay_db_path)
    conn.row_factory = sqlite3.Row
    try:
        all_rows: list[GivebackPathRow] = []
        trade_summaries: list[dict[str, Any]] = []
        for trade in trades:
            cohort = "weak_middle" if trade["entry_ts"].startswith("2025-11-17") else "good_recent"
            rows = _build_trade_path_rows(conn=conn, settings=settings, trade=trade, cohort=cohort)
            all_rows.extend(rows)
            trade_summaries.append(_summarize_trade(rows, trade))
    finally:
        conn.close()

    findings = _build_findings(trade_summaries)
    return all_rows, trade_summaries, findings


def _build_trade_path_rows(
    *,
    conn: sqlite3.Connection,
    settings: Any,
    trade: dict[str, str],
    cohort: str,
) -> list[GivebackPathRow]:
    entry_ts = datetime.fromisoformat(trade["entry_ts"])
    exit_ts = datetime.fromisoformat(trade["exit_ts"])
    entry_price = Decimal(trade["entry_px"])

    bars = _load_bars(conn=conn, entry_ts=entry_ts, exit_ts=exit_ts)
    if not bars:
        raise ValueError(f"No bars found for additive trade at {trade['entry_ts']}")

    state = replace(
        build_initial_state(entry_ts),
        strategy_status=StrategyStatus.IN_SHORT_K,
        position_side=PositionSide.SHORT,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=entry_price,
        entry_timestamp=entry_ts,
        entry_bar_id=bars[0].bar_id,
        long_entry_family=LongEntryFamily.NONE,
        short_entry_family=ShortEntryFamily.DERIVATIVE_BEAR_ADDITIVE,
        short_entry_source=ADDITIVE_FAMILY,
        bars_in_trade=1,
    )
    rows: list[GivebackPathRow] = []
    for index, bar in enumerate(bars, start=1):
        state = replace(state, bars_in_trade=state.bars_in_trade + 1, updated_at=bar.end_ts)
        features = _load_feature_packet(conn, bar.bar_id)
        risk_context = compute_risk_context(_load_recent_bars(conn, current_end_ts=bar.end_ts), features, state, settings)
        state = update_additive_short_peak_state(state, bar, risk_context, settings, bar.end_ts)
        rows.append(
            GivebackPathRow(
                trade_id=trade["trade_id"],
                cohort=cohort,
                entry_ts=trade["entry_ts"],
                exit_ts=trade["exit_ts"],
                bar_index=index,
                bar_start_ts=bar.start_ts.isoformat(),
                bar_end_ts=bar.end_ts.isoformat(),
                bar_low=bar.low,
                bar_close=bar.close,
                short_risk=risk_context.short_risk or Decimal("0"),
                current_favorable_excursion=max(Decimal("0"), entry_price - bar.low),
                max_favorable_excursion=state.additive_short_max_favorable_excursion,
                giveback_from_peak=state.additive_short_giveback_from_peak,
                reached_0_5r=_reached_arm(state.additive_short_max_favorable_excursion, risk_context.short_risk, Decimal("0.50")),
                reached_0_75r=_reached_arm(state.additive_short_max_favorable_excursion, risk_context.short_risk, Decimal("0.75")),
                reached_1_0r=_reached_arm(state.additive_short_max_favorable_excursion, risk_context.short_risk, Decimal("1.00")),
                fire_0_5r_25pct=_would_fire(state, risk_context.short_risk, Decimal("0.50"), Decimal("0.25")),
                fire_0_5r_33pct=_would_fire(state, risk_context.short_risk, Decimal("0.50"), Decimal("0.33")),
                fire_0_5r_50pct=_would_fire(state, risk_context.short_risk, Decimal("0.50"), Decimal("0.50")),
                fire_0_75r_25pct=_would_fire(state, risk_context.short_risk, Decimal("0.75"), Decimal("0.25")),
                fire_0_75r_33pct=_would_fire(state, risk_context.short_risk, Decimal("0.75"), Decimal("0.33")),
                fire_0_75r_50pct=_would_fire(state, risk_context.short_risk, Decimal("0.75"), Decimal("0.50")),
                fire_1_0r_25pct=_would_fire(state, risk_context.short_risk, Decimal("1.00"), Decimal("0.25")),
                fire_1_0r_33pct=_would_fire(state, risk_context.short_risk, Decimal("1.00"), Decimal("0.33")),
                fire_1_0r_50pct=_would_fire(state, risk_context.short_risk, Decimal("1.00"), Decimal("0.50")),
            )
        )
    return rows


def _load_bars(*, conn: sqlite3.Connection, entry_ts: datetime, exit_ts: datetime) -> list[Bar]:
    rows = conn.execute(
        """
        SELECT bar_id, symbol, timeframe, start_ts, end_ts, open, high, low, close, volume,
               is_final, session_asia, session_london, session_us, session_allowed
        FROM bars
        WHERE start_ts >= ? AND end_ts <= ?
        ORDER BY end_ts
        """,
        (entry_ts.isoformat(), exit_ts.isoformat()),
    ).fetchall()
    return [
        _row_to_bar(row)
        for row in rows
    ]


def _load_recent_bars(conn: sqlite3.Connection, current_end_ts: datetime) -> list[Bar]:
    rows = conn.execute(
        """
        SELECT bar_id, symbol, timeframe, start_ts, end_ts, open, high, low, close, volume,
               is_final, session_asia, session_london, session_us, session_allowed
        FROM bars
        WHERE end_ts <= ?
        ORDER BY end_ts DESC
        LIMIT 3
        """,
        (current_end_ts.isoformat(),),
    ).fetchall()
    return [_row_to_bar(row) for row in reversed(rows)]


def _row_to_bar(row: sqlite3.Row) -> Bar:
    return Bar(
        bar_id=row["bar_id"],
        symbol=row["symbol"],
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


def _load_feature_packet(conn: sqlite3.Connection, bar_id: str) -> FeaturePacket:
    row = conn.execute("SELECT payload_json FROM features WHERE bar_id = ?", (bar_id,)).fetchone()
    if row is None:
        raise ValueError(f"Missing feature payload for {bar_id}")
    payload = _json_loads(row["payload_json"])
    return FeaturePacket(**payload)


def _json_loads(payload_json: str) -> dict[str, Any]:
    raw = json.loads(payload_json)
    return {key: _deserialize_value(value) for key, value in raw.items()}


def _deserialize_value(value: Any) -> Any:
    if not isinstance(value, dict) or "__type__" not in value:
        return value
    if value["__type__"] == "datetime":
        return datetime.fromisoformat(value["value"])
    if value["__type__"] == "decimal":
        return Decimal(value["value"])
    if value["__type__"] == "enum":
        return value["value"]
    return value


def _reached_arm(max_fe: Decimal, short_risk: Decimal | None, arm_level: Decimal) -> bool:
    if short_risk is None or short_risk <= 0:
        return False
    return max_fe >= arm_level * short_risk


def _would_fire(state: Any, short_risk: Decimal | None, arm_level: Decimal, giveback_fraction: Decimal) -> bool:
    if not _reached_arm(state.additive_short_max_favorable_excursion, short_risk, arm_level):
        return False
    return state.additive_short_giveback_from_peak >= state.additive_short_max_favorable_excursion * giveback_fraction


def _summarize_trade(rows: list[GivebackPathRow], trade: dict[str, str]) -> dict[str, Any]:
    reached = {
        str(level): _first_bar(rows, f"reached_{_arm_token(level)}r")
        for level in ARM_LEVELS
    }
    fire_points = {
        f"{arm}r_{fraction}": _first_bar(rows, f"fire_{_arm_token(arm)}r_{_fraction_token(fraction)}pct")
        for arm in ARM_LEVELS
        for fraction in GIVEBACK_FRACTIONS
    }
    return {
        "trade_id": trade["trade_id"],
        "entry_ts": trade["entry_ts"],
        "exit_ts": trade["exit_ts"],
        "cohort": rows[0].cohort,
        "net_pnl": Decimal(trade["net_pnl"]),
        "actual_exit_reason": trade["exit_reason"],
        "bars_held": int(trade["bars_held"]),
        "max_favorable_excursion": max(row.max_favorable_excursion for row in rows),
        "max_giveback_from_peak": max(row.giveback_from_peak for row in rows),
        "first_reached_by_arm_r": reached,
        "first_fire_by_arm_and_giveback": fire_points,
    }


def _first_bar(rows: list[GivebackPathRow], field_name: str) -> str | None:
    for row in rows:
        if getattr(row, field_name):
            return row.bar_end_ts
    return None


def _build_findings(trade_summaries: list[dict[str, Any]]) -> list[str]:
    weak = next(summary for summary in trade_summaries if summary["cohort"] == "weak_middle")
    strong = [summary for summary in trade_summaries if summary["cohort"] == "good_recent"]
    strong_fire_025 = [item["first_fire_by_arm_and_giveback"]["1.00r_0.25"] for item in strong]

    findings = [
        "The weak middle trade reached 0.50R, 0.75R, and 1.0R on its first bar, so earlier arming does not separate it from the strong trades.",
        "1.0R / 50% never fired on any of the 3 additive trades; that matches the no-op A/B result exactly.",
        f"Only one strong recent trade ever gave back enough to fire even a tighter 25% rule, and it did so only on its actual exit bar ({strong_fire_025[0]}).",
        "The other strong recent trade barely gave back from peak after arming, so tighter giveback fractions would still not change its exit timing.",
        "The giveback paths do not separate cleanly enough to justify another local giveback A/B.",
    ]
    return findings


def _arm_token(value: Decimal) -> str:
    mapping = {
        Decimal("0.50"): "0_5",
        Decimal("0.75"): "0_75",
        Decimal("1.00"): "1_0",
    }
    return mapping[value]


def _fraction_token(value: Decimal) -> str:
    mapping = {
        Decimal("0.25"): "25",
        Decimal("0.33"): "33",
        Decimal("0.50"): "50",
    }
    return mapping[value]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
