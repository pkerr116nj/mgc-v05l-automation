"""Operator-facing reporting helpers for persisted replay artifacts."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..domain.enums import OrderIntentType
from ..domain.models import Bar
from ..execution.order_models import FillEvent, OrderIntent
from ..persistence.repositories import decode_fill, decode_order_intent
from .session_phase_labels import label_session_phase


@dataclass(frozen=True)
class ReplayTradeLedgerRow:
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
    entry_session_phase: str
    exit_session: str
    exit_session_phase: str
    mae: Decimal | None = None
    mfe: Decimal | None = None
    bars_held: int | None = None
    time_to_mfe: int | None = None
    time_to_mae: int | None = None
    mfe_capture_pct: Decimal | None = None
    entry_efficiency_3: Decimal | None = None
    entry_efficiency_5: Decimal | None = None
    entry_efficiency_10: Decimal | None = None
    initial_adverse_3bar: Decimal | None = None
    initial_favorable_3bar: Decimal | None = None
    entry_distance_fast_ema_atr: Decimal | None = None
    entry_distance_slow_ema_atr: Decimal | None = None
    entry_distance_vwap_atr: Decimal | None = None


@dataclass(frozen=True)
class ReplayFeatureContext:
    atr: Decimal
    turn_ema_fast: Decimal
    turn_ema_slow: Decimal
    vwap: Decimal


@dataclass(frozen=True)
class ReplaySummaryMetrics:
    total_net_pnl: Decimal
    win_rate: Decimal
    avg_winner: Decimal
    avg_loser: Decimal
    expectancy: Decimal
    max_drawdown: Decimal
    number_of_trades: int
    pnl_by_signal_family: dict[str, Decimal]
    pnl_by_session: dict[str, Decimal]


@dataclass(frozen=True)
class ReplayBreakdownRow:
    bucket: str
    trade_count: int
    wins: int
    losses: int
    win_rate: Decimal
    total_net_pnl: Decimal
    avg_pnl: Decimal
    avg_winner: Decimal
    avg_loser: Decimal


@dataclass(frozen=True)
class ReplayHoldTimeSummary:
    average_bars_held: Decimal
    median_bars_held: Decimal
    average_time_to_mfe: Decimal
    average_time_to_mae: Decimal


@dataclass(frozen=True)
class ReplayMaeMfeSummary:
    average_mae: Decimal
    average_mfe: Decimal
    average_mfe_capture_pct: Decimal
    average_initial_adverse_3bar: Decimal
    average_initial_favorable_3bar: Decimal


def build_session_lookup(bars: Sequence[Bar]) -> dict[str, str]:
    """Build a start-timestamp to session label lookup."""
    return {bar.start_ts.isoformat(): _session_label(bar) for bar in bars}


def build_trade_ledger(
    order_intent_rows: Sequence[dict[str, Any]],
    fill_rows: Sequence[dict[str, Any]],
    session_by_start_ts: Mapping[str, str],
    point_value: Decimal,
    fee_per_fill: Decimal = Decimal("0"),
    slippage_per_fill: Decimal = Decimal("0"),
    bars: Sequence[Bar] | None = None,
    feature_context_by_bar_id: Mapping[str, ReplayFeatureContext] | None = None,
) -> list[ReplayTradeLedgerRow]:
    """Pair persisted entry/exit fills into closed trades."""
    fills_by_intent = {row["order_intent_id"]: decode_fill(dict(row)) for row in fill_rows}
    joined: list[tuple[OrderIntent, FillEvent]] = []
    for row in order_intent_rows:
        intent = decode_order_intent(dict(row))
        fill = fills_by_intent.get(intent.order_intent_id)
        if fill is None or fill.fill_price is None:
            continue
        joined.append((intent, fill))
    joined.sort(key=lambda pair: pair[1].fill_timestamp)

    bars = bars or []
    feature_context_by_bar_id = feature_context_by_bar_id or {}
    bars_by_start_ts = {bar.start_ts.isoformat(): index for index, bar in enumerate(bars)}

    ledger: list[ReplayTradeLedgerRow] = []
    open_trade: dict[str, Any] | None = None
    trade_id = 0

    for intent, fill in joined:
        if intent.intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN):
            open_trade = {
                "direction": "LONG" if intent.intent_type == OrderIntentType.BUY_TO_OPEN else "SHORT",
                "entry_ts": fill.fill_timestamp,
                "entry_px": fill.fill_price,
                "qty": intent.quantity,
                "setup_family": intent.reason_code,
                "entry_session": session_by_start_ts.get(fill.fill_timestamp.isoformat(), "UNKNOWN"),
                "signal_bar_id": intent.bar_id,
            }
            continue

        if open_trade is None:
            continue

        trade_id += 1
        exit_reason = intent.reason_code
        qty = int(open_trade["qty"])
        price_diff = (
            fill.fill_price - open_trade["entry_px"]
            if open_trade["direction"] == "LONG"
            else open_trade["entry_px"] - fill.fill_price
        )
        gross_pnl = price_diff * Decimal(qty) * point_value
        fees = fee_per_fill * Decimal(qty) * Decimal("2")
        slippage = slippage_per_fill * Decimal(qty) * Decimal("2")
        net_pnl = gross_pnl - fees - slippage
        trade_diagnostics = _build_trade_diagnostics(
            direction=open_trade["direction"],
            entry_price=open_trade["entry_px"],
            exit_price=fill.fill_price,
            entry_ts=open_trade["entry_ts"],
            exit_ts=fill.fill_timestamp,
            signal_bar_id=open_trade["signal_bar_id"],
            bars=bars,
            bars_by_start_ts=bars_by_start_ts,
            feature_context_by_bar_id=feature_context_by_bar_id,
        )
        ledger.append(
            ReplayTradeLedgerRow(
                trade_id=trade_id,
                direction=open_trade["direction"],
                entry_ts=open_trade["entry_ts"],
                entry_px=open_trade["entry_px"],
                exit_ts=fill.fill_timestamp,
                exit_px=fill.fill_price,
                qty=qty,
                gross_pnl=gross_pnl,
                fees=fees,
                slippage=slippage,
                net_pnl=net_pnl,
                exit_reason=exit_reason,
                setup_family=str(open_trade["setup_family"]),
                entry_session=str(open_trade["entry_session"]),
                entry_session_phase=label_session_phase(open_trade["entry_ts"]),
                exit_session=session_by_start_ts.get(fill.fill_timestamp.isoformat(), "UNKNOWN"),
                exit_session_phase=label_session_phase(fill.fill_timestamp),
                mae=trade_diagnostics["mae"],
                mfe=trade_diagnostics["mfe"],
                bars_held=trade_diagnostics["bars_held"],
                time_to_mfe=trade_diagnostics["time_to_mfe"],
                time_to_mae=trade_diagnostics["time_to_mae"],
                mfe_capture_pct=trade_diagnostics["mfe_capture_pct"],
                entry_efficiency_3=trade_diagnostics["entry_efficiency_3"],
                entry_efficiency_5=trade_diagnostics["entry_efficiency_5"],
                entry_efficiency_10=trade_diagnostics["entry_efficiency_10"],
                initial_adverse_3bar=trade_diagnostics["initial_adverse_3bar"],
                initial_favorable_3bar=trade_diagnostics["initial_favorable_3bar"],
                entry_distance_fast_ema_atr=trade_diagnostics["entry_distance_fast_ema_atr"],
                entry_distance_slow_ema_atr=trade_diagnostics["entry_distance_slow_ema_atr"],
                entry_distance_vwap_atr=trade_diagnostics["entry_distance_vwap_atr"],
            )
        )
        open_trade = None

    return ledger


def build_summary_metrics(ledger: Sequence[ReplayTradeLedgerRow]) -> ReplaySummaryMetrics:
    """Summarize a closed-trade ledger into operator-facing metrics."""
    winners = [row.net_pnl for row in ledger if row.net_pnl > 0]
    losers = [row.net_pnl for row in ledger if row.net_pnl < 0]
    pnl_by_signal_family: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    pnl_by_session: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))

    cumulative = Decimal("0")
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    for row in ledger:
        pnl_by_signal_family[row.setup_family] += row.net_pnl
        pnl_by_session[row.entry_session] += row.net_pnl
        cumulative += row.net_pnl
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    trade_count = len(ledger)
    total_net_pnl = sum((row.net_pnl for row in ledger), Decimal("0"))
    win_rate = Decimal("0") if trade_count == 0 else Decimal(len(winners)) / Decimal(trade_count)
    avg_winner = Decimal("0") if not winners else sum(winners, Decimal("0")) / Decimal(len(winners))
    avg_loser = Decimal("0") if not losers else sum(losers, Decimal("0")) / Decimal(len(losers))
    expectancy = Decimal("0") if trade_count == 0 else total_net_pnl / Decimal(trade_count)

    return ReplaySummaryMetrics(
        total_net_pnl=total_net_pnl,
        win_rate=win_rate,
        avg_winner=avg_winner,
        avg_loser=avg_loser,
        expectancy=expectancy,
        max_drawdown=max_drawdown,
        number_of_trades=trade_count,
        pnl_by_signal_family=dict(pnl_by_signal_family),
        pnl_by_session=dict(pnl_by_session),
    )


def build_equity_curve_rows(ledger: Sequence[ReplayTradeLedgerRow]) -> list[dict[str, Any]]:
    """Build a simple closed-trade equity curve."""
    rows: list[dict[str, Any]] = []
    cumulative = Decimal("0")
    peak = Decimal("0")

    for trade in ledger:
        cumulative += trade.net_pnl
        if cumulative > peak:
            peak = cumulative
        rows.append(
            {
                "trade_id": trade.trade_id,
                "exit_ts": trade.exit_ts.isoformat(),
                "net_pnl": str(trade.net_pnl),
                "cumulative_net_pnl": str(cumulative),
                "drawdown": str(peak - cumulative),
            }
        )
    return rows


def build_drawdown_curve_rows(ledger: Sequence[ReplayTradeLedgerRow]) -> list[dict[str, Any]]:
    """Build a drawdown curve from the closed-trade ledger."""
    rows: list[dict[str, Any]] = []
    cumulative = Decimal("0")
    peak = Decimal("0")

    for trade in ledger:
        cumulative += trade.net_pnl
        if cumulative > peak:
            peak = cumulative
        rows.append(
            {
                "trade_id": trade.trade_id,
                "exit_ts": trade.exit_ts.isoformat(),
                "cumulative_net_pnl": str(cumulative),
                "peak_equity": str(peak),
                "drawdown": str(peak - cumulative),
            }
        )
    return rows


def build_rolling_performance_rows(
    ledger: Sequence[ReplayTradeLedgerRow],
    *,
    window_size: int = 20,
) -> list[dict[str, Any]]:
    """Build rolling win-rate and expectancy rows from the closed-trade ledger."""
    rows: list[dict[str, Any]] = []
    if window_size <= 0:
        raise ValueError("window_size must be > 0")

    for index in range(len(ledger)):
        window = ledger[max(0, index - window_size + 1) : index + 1]
        wins = sum(1 for trade in window if trade.net_pnl > 0)
        total_pnl = sum((trade.net_pnl for trade in window), Decimal("0"))
        rows.append(
            {
                "trade_id": ledger[index].trade_id,
                "exit_ts": ledger[index].exit_ts.isoformat(),
                "window_size": len(window),
                "rolling_win_rate": str(Decimal(wins) / Decimal(len(window))),
                "rolling_expectancy": str(total_pnl / Decimal(len(window))),
            }
        )
    return rows


def build_breakdown_rows(
    ledger: Sequence[ReplayTradeLedgerRow],
    *,
    key_name: str,
) -> list[ReplayBreakdownRow]:
    """Build grouped P/L breakdown rows from the closed-trade ledger."""
    buckets: dict[str, list[ReplayTradeLedgerRow]] = defaultdict(list)
    for trade in ledger:
        if key_name == "setup_family":
            key = trade.setup_family
        elif key_name == "entry_session":
            key = trade.entry_session
        elif key_name == "direction":
            key = trade.direction
        else:
            raise ValueError(f"Unsupported breakdown key: {key_name}")
        buckets[key].append(trade)
    return _build_grouped_breakdown_rows(buckets)


def build_mae_mfe_summary(ledger: Sequence[ReplayTradeLedgerRow]) -> ReplayMaeMfeSummary:
    return ReplayMaeMfeSummary(
        average_mae=_average_decimal([row.mae for row in ledger if row.mae is not None]),
        average_mfe=_average_decimal([row.mfe for row in ledger if row.mfe is not None]),
        average_mfe_capture_pct=_average_decimal([row.mfe_capture_pct for row in ledger if row.mfe_capture_pct is not None]),
        average_initial_adverse_3bar=_average_decimal(
            [row.initial_adverse_3bar for row in ledger if row.initial_adverse_3bar is not None]
        ),
        average_initial_favorable_3bar=_average_decimal(
            [row.initial_favorable_3bar for row in ledger if row.initial_favorable_3bar is not None]
        ),
    )


def build_hold_time_summary(ledger: Sequence[ReplayTradeLedgerRow]) -> ReplayHoldTimeSummary:
    bars_held = sorted(row.bars_held for row in ledger if row.bars_held is not None)
    median_bars = Decimal("0")
    if bars_held:
        median_index = len(bars_held) // 2
        if len(bars_held) % 2:
            median_bars = Decimal(bars_held[median_index])
        else:
            median_bars = Decimal(bars_held[median_index - 1] + bars_held[median_index]) / Decimal("2")
    return ReplayHoldTimeSummary(
        average_bars_held=_average_decimal([Decimal(row.bars_held) for row in ledger if row.bars_held is not None]),
        median_bars_held=median_bars,
        average_time_to_mfe=_average_decimal([Decimal(row.time_to_mfe) for row in ledger if row.time_to_mfe is not None]),
        average_time_to_mae=_average_decimal([Decimal(row.time_to_mae) for row in ledger if row.time_to_mae is not None]),
    )


def build_exit_reason_breakdown_rows(ledger: Sequence[ReplayTradeLedgerRow]) -> list[ReplayBreakdownRow]:
    buckets: dict[str, list[ReplayTradeLedgerRow]] = defaultdict(list)
    for trade in ledger:
        buckets[trade.exit_reason].append(trade)
    return _build_grouped_breakdown_rows(buckets)


def build_trade_efficiency_rows(
    ledger: Sequence[ReplayTradeLedgerRow],
    *,
    key_name: str,
) -> list[dict[str, Any]]:
    buckets: dict[str, list[ReplayTradeLedgerRow]] = defaultdict(list)
    for trade in ledger:
        if key_name == "setup_family":
            buckets[trade.setup_family].append(trade)
        elif key_name == "entry_session":
            buckets[trade.entry_session].append(trade)
        else:
            raise ValueError(f"Unsupported efficiency key: {key_name}")

    rows: list[dict[str, Any]] = []
    for bucket, trades in sorted(buckets.items()):
        rows.append(
            {
                "bucket": bucket,
                "trade_count": len(trades),
                "avg_entry_efficiency_3": str(_average_decimal([trade.entry_efficiency_3 for trade in trades if trade.entry_efficiency_3 is not None])),
                "avg_entry_efficiency_5": str(_average_decimal([trade.entry_efficiency_5 for trade in trades if trade.entry_efficiency_5 is not None])),
                "avg_entry_efficiency_10": str(_average_decimal([trade.entry_efficiency_10 for trade in trades if trade.entry_efficiency_10 is not None])),
                "avg_mfe_capture_pct": str(_average_decimal([trade.mfe_capture_pct for trade in trades if trade.mfe_capture_pct is not None])),
                "avg_bars_held": str(_average_decimal([Decimal(trade.bars_held) for trade in trades if trade.bars_held is not None])),
                "avg_net_pnl": str(_average_decimal([trade.net_pnl for trade in trades])),
            }
        )
    return rows


def write_trade_ledger_csv(ledger: Sequence[ReplayTradeLedgerRow], output_path: Path) -> Path:
    """Write the closed-trade ledger to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "trade_id",
        "direction",
        "entry_ts",
        "entry_px",
        "exit_ts",
        "exit_px",
        "qty",
        "gross_pnl",
        "fees",
        "slippage",
        "net_pnl",
        "exit_reason",
        "setup_family",
        "entry_session",
        "entry_session_phase",
        "exit_session",
        "exit_session_phase",
        "mae",
        "mfe",
        "bars_held",
        "time_to_mfe",
        "time_to_mae",
        "mfe_capture_pct",
        "entry_efficiency_3",
        "entry_efficiency_5",
        "entry_efficiency_10",
        "initial_adverse_3bar",
        "initial_favorable_3bar",
        "entry_distance_fast_ema_atr",
        "entry_distance_slow_ema_atr",
        "entry_distance_vwap_atr",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in ledger:
            payload = asdict(row)
            payload["entry_ts"] = row.entry_ts.isoformat()
            payload["exit_ts"] = row.exit_ts.isoformat()
            for key in (
                "entry_px",
                "exit_px",
                "gross_pnl",
                "fees",
                "slippage",
                "net_pnl",
                "mae",
                "mfe",
                "mfe_capture_pct",
                "entry_efficiency_3",
                "entry_efficiency_5",
                "entry_efficiency_10",
                "initial_adverse_3bar",
                "initial_favorable_3bar",
                "entry_distance_fast_ema_atr",
                "entry_distance_slow_ema_atr",
                "entry_distance_vwap_atr",
            ):
                if payload[key] is not None:
                    payload[key] = str(payload[key])
            writer.writerow(payload)
    return output_path


def write_equity_curve_csv(rows: Sequence[dict[str, Any]], output_path: Path) -> Path:
    """Write the equity curve rows to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["trade_id", "exit_ts", "net_pnl", "cumulative_net_pnl", "drawdown"]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return output_path


def write_drawdown_curve_csv(rows: Sequence[dict[str, Any]], output_path: Path) -> Path:
    """Write the drawdown curve rows to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["trade_id", "exit_ts", "cumulative_net_pnl", "peak_equity", "drawdown"]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return output_path


def write_breakdown_csv(rows: Sequence[ReplayBreakdownRow], output_path: Path) -> Path:
    """Write grouped P/L breakdown rows to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "bucket",
        "trade_count",
        "wins",
        "losses",
        "win_rate",
        "total_net_pnl",
        "avg_pnl",
        "avg_winner",
        "avg_loser",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = asdict(row)
            for key in ("win_rate", "total_net_pnl", "avg_pnl", "avg_winner", "avg_loser"):
                payload[key] = str(payload[key])
            writer.writerow(payload)
    return output_path


def write_rolling_performance_csv(rows: Sequence[dict[str, Any]], output_path: Path) -> Path:
    """Write rolling performance rows to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["trade_id", "exit_ts", "window_size", "rolling_win_rate", "rolling_expectancy"]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return output_path


def write_summary_metrics_json(
    metrics: ReplaySummaryMetrics,
    output_path: Path,
    *,
    point_value: Decimal,
    fee_per_fill: Decimal,
    slippage_per_fill: Decimal,
) -> Path:
    """Write summary metrics plus modeling assumptions to JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "total_net_pnl": float(metrics.total_net_pnl),
        "win_rate": float(metrics.win_rate),
        "avg_winner": float(metrics.avg_winner),
        "avg_loser": float(metrics.avg_loser),
        "expectancy": float(metrics.expectancy),
        "max_drawdown": float(metrics.max_drawdown),
        "number_of_trades": metrics.number_of_trades,
        "pnl_by_signal_family": {key: float(value) for key, value in metrics.pnl_by_signal_family.items()},
        "pnl_by_session": {key: float(value) for key, value in metrics.pnl_by_session.items()},
        "assumptions": {
            "point_value": float(point_value),
            "fee_per_fill": float(fee_per_fill),
            "slippage_per_fill": float(slippage_per_fill),
        },
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def write_mae_mfe_summary_json(summary: ReplayMaeMfeSummary, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "average_mae": float(summary.average_mae),
        "average_mfe": float(summary.average_mfe),
        "average_mfe_capture_pct": float(summary.average_mfe_capture_pct),
        "average_initial_adverse_3bar": float(summary.average_initial_adverse_3bar),
        "average_initial_favorable_3bar": float(summary.average_initial_favorable_3bar),
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def write_hold_time_summary_json(summary: ReplayHoldTimeSummary, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "average_bars_held": float(summary.average_bars_held),
        "median_bars_held": float(summary.median_bars_held),
        "average_time_to_mfe": float(summary.average_time_to_mfe),
        "average_time_to_mae": float(summary.average_time_to_mae),
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def write_dict_rows_csv(rows: Sequence[dict[str, Any]], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return output_path
    fieldnames = list(rows[0].keys())
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return output_path


def _session_label(bar: Bar) -> str:
    if bar.session_asia:
        return "ASIA"
    if bar.session_london:
        return "LONDON"
    if bar.session_us:
        return "US"
    return "OFF"


def _build_grouped_breakdown_rows(
    buckets: Mapping[str, Sequence[ReplayTradeLedgerRow]],
) -> list[ReplayBreakdownRow]:
    rows: list[ReplayBreakdownRow] = []
    for bucket, trades in sorted(buckets.items()):
        wins = [trade.net_pnl for trade in trades if trade.net_pnl > 0]
        losses = [trade.net_pnl for trade in trades if trade.net_pnl < 0]
        total_net_pnl = sum((trade.net_pnl for trade in trades), Decimal("0"))
        rows.append(
            ReplayBreakdownRow(
                bucket=bucket,
                trade_count=len(trades),
                wins=len(wins),
                losses=len(losses),
                win_rate=Decimal(len(wins)) / Decimal(len(trades)),
                total_net_pnl=total_net_pnl,
                avg_pnl=total_net_pnl / Decimal(len(trades)),
                avg_winner=Decimal("0") if not wins else sum(wins, Decimal("0")) / Decimal(len(wins)),
                avg_loser=Decimal("0") if not losses else sum(losses, Decimal("0")) / Decimal(len(losses)),
            )
        )
    return rows


def _build_trade_diagnostics(
    *,
    direction: str,
    entry_price: Decimal,
    exit_price: Decimal,
    entry_ts: datetime,
    exit_ts: datetime,
    signal_bar_id: str,
    bars: Sequence[Bar],
    bars_by_start_ts: Mapping[str, int],
    feature_context_by_bar_id: Mapping[str, ReplayFeatureContext],
) -> dict[str, Any]:
    entry_index = bars_by_start_ts.get(entry_ts.isoformat())
    exit_index = bars_by_start_ts.get(exit_ts.isoformat())
    if entry_index is None or exit_index is None or exit_index <= entry_index:
        return {
            "mae": None,
            "mfe": None,
            "bars_held": None,
            "time_to_mfe": None,
            "time_to_mae": None,
            "mfe_capture_pct": None,
            "entry_efficiency_3": None,
            "entry_efficiency_5": None,
            "entry_efficiency_10": None,
            "initial_adverse_3bar": None,
            "initial_favorable_3bar": None,
            "entry_distance_fast_ema_atr": None,
            "entry_distance_slow_ema_atr": None,
            "entry_distance_vwap_atr": None,
        }

    in_trade_bars = list(bars[entry_index:exit_index])
    forward_3 = in_trade_bars[:3]
    forward_5 = in_trade_bars[:5]
    forward_10 = in_trade_bars[:10]
    favorable_series = [_favorable_from_entry(entry_price, [bar], direction) for bar in in_trade_bars]
    adverse_series = [_adverse_from_entry(entry_price, [bar], direction) for bar in in_trade_bars]
    mfe = max(favorable_series, default=Decimal("0"))
    mae = max(adverse_series, default=Decimal("0"))
    time_to_mfe = favorable_series.index(mfe) + 1 if favorable_series else None
    time_to_mae = adverse_series.index(mae) + 1 if adverse_series else None
    realized = _realized_move(entry_price, exit_price, direction)
    mfe_capture_pct = None
    if mfe > 0:
        mfe_capture_pct = max(Decimal("0"), min(Decimal("100"), Decimal("100") * realized / mfe))

    feature_context = feature_context_by_bar_id.get(signal_bar_id)
    atr = feature_context.atr if feature_context is not None else None
    return {
        "mae": mae,
        "mfe": mfe,
        "bars_held": len(in_trade_bars),
        "time_to_mfe": time_to_mfe,
        "time_to_mae": time_to_mae,
        "mfe_capture_pct": mfe_capture_pct,
        "entry_efficiency_3": _entry_efficiency_pct(
            _favorable_from_entry(entry_price, forward_3, direction),
            _adverse_from_entry(entry_price, forward_3, direction),
        ),
        "entry_efficiency_5": _entry_efficiency_pct(
            _favorable_from_entry(entry_price, forward_5, direction),
            _adverse_from_entry(entry_price, forward_5, direction),
        ),
        "entry_efficiency_10": _entry_efficiency_pct(
            _favorable_from_entry(entry_price, forward_10, direction),
            _adverse_from_entry(entry_price, forward_10, direction),
        ),
        "initial_adverse_3bar": _adverse_from_entry(entry_price, forward_3, direction),
        "initial_favorable_3bar": _favorable_from_entry(entry_price, forward_3, direction),
        "entry_distance_fast_ema_atr": _normalized_distance(
            entry_price,
            feature_context.turn_ema_fast if feature_context is not None else None,
            atr,
        ),
        "entry_distance_slow_ema_atr": _normalized_distance(
            entry_price,
            feature_context.turn_ema_slow if feature_context is not None else None,
            atr,
        ),
        "entry_distance_vwap_atr": _normalized_distance(
            entry_price,
            feature_context.vwap if feature_context is not None else None,
            atr,
        ),
    }


def _favorable_from_entry(entry_price: Decimal, future_bars: Sequence[Bar], direction: str) -> Decimal:
    if not future_bars:
        return Decimal("0")
    if direction == "LONG":
        return max(bar.high for bar in future_bars) - entry_price
    return entry_price - min(bar.low for bar in future_bars)


def _adverse_from_entry(entry_price: Decimal, future_bars: Sequence[Bar], direction: str) -> Decimal:
    if not future_bars:
        return Decimal("0")
    if direction == "LONG":
        return entry_price - min(bar.low for bar in future_bars)
    return max(bar.high for bar in future_bars) - entry_price


def _entry_efficiency_pct(favorable: Decimal, adverse: Decimal) -> Decimal:
    total = favorable + adverse
    if total <= 0:
        return Decimal("50")
    return max(Decimal("0"), min(Decimal("100"), Decimal("100") * favorable / total))


def _normalized_distance(
    price: Decimal,
    reference: Decimal | None,
    atr: Decimal | None,
) -> Decimal | None:
    if reference is None or atr is None or atr <= 0:
        return None
    return (price - reference) / atr


def _realized_move(entry_price: Decimal, exit_price: Decimal, direction: str) -> Decimal:
    if direction == "LONG":
        return exit_price - entry_price
    return entry_price - exit_price


def _average_decimal(values: Sequence[Decimal]) -> Decimal:
    decimal_values = [value for value in values if value is not None]
    if not decimal_values:
        return Decimal("0")
    return sum(decimal_values, Decimal("0")) / Decimal(len(decimal_values))
