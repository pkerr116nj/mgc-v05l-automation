"""ES/MES overnight regime-entry research with support/resistance limit orders."""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ..config_models import load_settings_from_files
from ..research.trend_participation.models import ResearchBar
from ..research.trend_participation.state_layers import (
    EMA_FAST_SPAN,
    EMA_SLOW_SPAN,
    LONG_BIAS,
    SHORT_BIAS,
    classify_bias,
    rolling_atr,
    rolling_ema,
)
from ..research.trend_participation.storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m
from .asia_london_participation_research import (
    DEFAULT_CONFIG_PATHS,
    HOLD_SEGMENTS,
    NEW_YORK,
    POINT_VALUES,
    ROUND_TURN_COMMISSION_DOLLARS,
    TICK_SIZES,
    _bars_vwap,
    _build_overnight_session_contexts,
    _execution_cost_points,
    _load_end_timestamp,
    _load_start_timestamp,
    _parse_date,
    _resolve_sqlite_database_path,
)
from .gc_mgc_london_late_long_research import _ema as _hold_ema
from .gc_mgc_london_late_long_research import _find_exit as _find_long_exit
from .gc_mgc_ny_early_short_research import NyEarlyShortSpec, _find_exit as _find_short_exit
from .gc_mgc_segment_forced_session_long_research import ForcedSegmentLongSpec


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "es_mes_asia_london_regime_entry_research"
DEFAULT_SYMBOLS: tuple[str, ...] = ("ES", "MES")
ENTRY_SEGMENT = "ASIA_EARLY"


@dataclass(frozen=True)
class RegimeEntryTrade:
    symbol: str
    trade_date: str
    side: str
    regime_state: str
    entered: bool
    entry_reason: str
    signal_end_ts: str | None
    entry_end_ts: str | None
    limit_price: float | None
    entry_price: float | None
    stop_price: float | None
    exit_end_ts: str | None
    exit_price: float | None
    exit_reason: str | None
    pnl_points: float | None
    net_pnl_points: float | None
    max_drawdown_points: float | None
    notes: tuple[str, ...] = ()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="es-mes-asia-london-regime-entry-research")
    parser.add_argument("--symbol", action="append", default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--config", action="append", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_es_mes_asia_london_regime_entry_research(
        symbols=args.symbol or list(DEFAULT_SYMBOLS),
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        config_paths=args.config,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_es_mes_asia_london_regime_entry_research(
    *,
    symbols: list[str] | tuple[str, ...] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    output_dir: str | Path | None = None,
    config_paths: list[str] | list[Path] | tuple[str | Path, ...] | None = None,
) -> dict[str, Any]:
    resolved_symbols = tuple(sorted({str(symbol).strip().upper() for symbol in (symbols or DEFAULT_SYMBOLS) if str(symbol).strip()}))
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    settings = load_settings_from_files([Path(path) for path in (config_paths or DEFAULT_CONFIG_PATHS)])
    sqlite_path = _resolve_sqlite_database_path(settings.database_url)
    start_day = _parse_date(start_date)
    end_day = _parse_date(end_date)

    symbol_reports: dict[str, Any] = {}
    for symbol in resolved_symbols:
        one_minute = load_sqlite_bars(
            sqlite_path=sqlite_path,
            instrument=symbol,
            timeframe="1m",
            data_source="historical_1m_canonical",
            start_ts=_load_start_timestamp(start_day),
            end_ts=_load_end_timestamp(end_day),
        )
        normalized_1m, quality_issues = normalize_and_check_bars(bars=one_minute, timeframe="1m")
        bars_3m = resample_bars_from_1m(bars_1m=normalized_1m, target_timeframe="3m")
        bars_5m = resample_bars_from_1m(bars_1m=normalized_1m, target_timeframe="5m")
        contexts = _build_overnight_session_contexts(
            decision_bars=bars_3m,
            symbol=symbol,
            start_day=start_day,
            end_day=end_day,
        )
        grouped_1m = _group_bars_by_trade_day(bars=normalized_1m, symbol=symbol)
        grouped_5m = _group_bars_by_trade_day(bars=bars_5m, symbol=symbol)

        long_rows = [
            _evaluate_regime_limit_trade(
                symbol=symbol,
                side="LONG",
                context=context,
                bars_1m=grouped_1m.get(context.trade_date, []),
                bars_5m=grouped_5m.get(context.trade_date, []),
            )
            for context in contexts
        ]
        short_rows = [
            _evaluate_regime_limit_trade(
                symbol=symbol,
                side="SHORT",
                context=context,
                bars_1m=grouped_1m.get(context.trade_date, []),
                bars_5m=grouped_5m.get(context.trade_date, []),
            )
            for context in contexts
        ]
        symbol_reports[symbol] = {
            "bar_count_1m": len(normalized_1m),
            "bar_count_3m": len(bars_3m),
            "bar_count_5m": len(bars_5m),
            "context_count": len(contexts),
            "data_quality_issues": [asdict(issue) for issue in quality_issues],
            "variants": {
                "LONG__regime_limit_support_entry": {
                    "trade_summary": _summarize_regime_trades(long_rows),
                    "sessions": [asdict(row) for row in long_rows],
                },
                "SHORT__regime_limit_resistance_entry": {
                    "trade_summary": _summarize_regime_trades(short_rows),
                    "sessions": [asdict(row) for row in short_rows],
                },
            },
        }

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "es_mes_asia_london_regime_entry_research",
        "definition": {
            "symbols": list(resolved_symbols),
            "entry_segment": ENTRY_SEGMENT,
            "hold_segments": list(HOLD_SEGMENTS),
            "description": "Regime-gated overnight ES/MES entries with support/resistance limit placement from trailing 1m/3m/5m candles and London-late forced exits.",
        },
        "database_path": str(sqlite_path),
        "symbol_reports": symbol_reports,
        "pair_rankings": _pair_rankings(symbol_reports=symbol_reports),
    }
    json_path = resolved_output_dir / "es_mes_asia_london_regime_entry_research.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return {
        "mode": "es_mes_asia_london_regime_entry_research",
        "artifact_paths": {"json": str(json_path)},
        "pair_rankings": payload["pair_rankings"],
    }


def _evaluate_regime_limit_trade(
    *,
    symbol: str,
    side: str,
    context: Any,
    bars_1m: list[ResearchBar],
    bars_5m: list[ResearchBar],
) -> RegimeEntryTrade:
    segment_bars = list(context.entry_segment_bars)
    hold_bars = list(context.hold_bars)
    if len(segment_bars) < 5 or len(bars_1m) < 10:
        return _empty_trade(symbol=symbol, trade_day=context.trade_date, side=side, note="insufficient_bars")

    closes = [float(bar.close) for bar in segment_bars]
    fast_ema_values = rolling_ema(closes, span=EMA_FAST_SPAN)
    slow_ema_values = rolling_ema(closes, span=EMA_SLOW_SPAN)
    atr_values = rolling_atr(segment_bars, window=5)

    selected_signal_index = None
    selected_bias = None
    selected_limit = None
    selected_stop = None
    signal_bar = None
    for index in range(4, min(len(segment_bars), 8)):
        fast_ema = fast_ema_values[index]
        slow_ema = slow_ema_values[index]
        atr = atr_values[index]
        prev_slow = slow_ema_values[index - 1] if index > 0 else slow_ema
        bias = classify_bias(
            bars=segment_bars,
            index=index,
            fast_ema=float(fast_ema),
            slow_ema=float(slow_ema),
            prev_slow_ema=float(prev_slow) if prev_slow is not None else None,
            session_vwap=_bars_vwap(segment_bars[: index + 1]),
            atr=float(atr),
        )
        if side == "LONG" and bias.state != LONG_BIAS:
            continue
        if side == "SHORT" and bias.state != SHORT_BIAS:
            continue
        signal_bar = segment_bars[index]
        signal_ts = signal_bar.end_ts
        window_1m = [bar for bar in bars_1m if bar.end_ts <= signal_ts][-5:]
        window_3m = segment_bars[max(0, index - 2) : index + 1]
        window_5m = [bar for bar in bars_5m if bar.end_ts <= signal_ts][-2:]
        if len(window_1m) < 3 or len(window_3m) < 2 or not window_5m:
            continue
        if side == "LONG":
            support_levels = [
                min(float(bar.low) for bar in window_1m),
                min(float(bar.low) for bar in window_3m),
                min(float(bar.low) for bar in window_5m),
            ]
            limit_price = round(max(support_levels), 4)
            stop_price = round(min(support_levels) - TICK_SIZES[symbol], 4)
            if limit_price >= float(signal_bar.close):
                continue
        else:
            resistance_levels = [
                max(float(bar.high) for bar in window_1m),
                max(float(bar.high) for bar in window_3m),
                max(float(bar.high) for bar in window_5m),
            ]
            limit_price = round(min(resistance_levels), 4)
            stop_price = round(max(resistance_levels) + TICK_SIZES[symbol], 4)
            if limit_price <= float(signal_bar.close):
                continue
        selected_signal_index = index
        selected_bias = bias
        selected_limit = limit_price
        selected_stop = stop_price
        break

    if signal_bar is None or selected_signal_index is None or selected_limit is None or selected_stop is None or selected_bias is None:
        return _empty_trade(symbol=symbol, trade_day=context.trade_date, side=side, note="no_regime_limit_setup")

    entry_fill = _find_limit_fill(
        side=side,
        entry_bars_1m=[bar for bar in bars_1m if bar.end_ts >= signal_bar.end_ts],
        limit_price=selected_limit,
        segment_end_ts=segment_bars[-1].end_ts,
    )
    if entry_fill is None:
        return RegimeEntryTrade(
            symbol=symbol,
            trade_date=context.trade_date.isoformat(),
            side=side,
            regime_state=selected_bias.state,
            entered=False,
            entry_reason="limit_not_filled",
            signal_end_ts=signal_bar.end_ts.isoformat(),
            entry_end_ts=None,
            limit_price=selected_limit,
            entry_price=None,
            stop_price=selected_stop,
            exit_end_ts=None,
            exit_price=None,
            exit_reason=None,
            pnl_points=None,
            net_pnl_points=None,
            max_drawdown_points=None,
            notes=tuple(selected_bias.reasons),
        )

    hold_entry_index = next((index for index, bar in enumerate(hold_bars) if bar.end_ts >= entry_fill.end_ts), None)
    if hold_entry_index is None:
        return _empty_trade(symbol=symbol, trade_day=context.trade_date, side=side, note="no_hold_bar_after_fill")

    closes_hold = [float(bar.close) for bar in hold_bars]
    ema_values = _hold_ema(closes_hold, length=5)
    if side == "LONG":
        spec = ForcedSegmentLongSpec(
            variant_id="es_mes_regime_limit_long",
            description="ES/MES regime limit long",
            tick_size=TICK_SIZES[symbol],
            fallback_entry_bar=8,
            exit_ema_length=5,
        )
        exit_index, exit_price, exit_reason = _find_long_exit(
            segment_bars=hold_bars,
            entry_index=hold_entry_index,
            stop_price=selected_stop,
            ema_values=ema_values,
            spec=spec,
        )
        pnl_points = round(float(exit_price) - float(selected_limit), 4)
        trade_window = hold_bars[hold_entry_index : exit_index + 1]
        max_drawdown = round(max(0.0, float(selected_limit) - min(float(bar.low) for bar in trade_window)), 4)
    else:
        spec = NyEarlyShortSpec(
            variant_id="es_mes_regime_limit_short",
            description="ES/MES regime limit short",
            setup_family="es_mes_regime_limit",
            tick_size=TICK_SIZES[symbol],
            exit_ema_length=5,
        )
        exit_index, exit_price, exit_reason = _find_short_exit(
            segment_bars=hold_bars,
            entry_index=hold_entry_index,
            stop_price=selected_stop,
            ema_values=ema_values,
            spec=spec,
        )
        pnl_points = round(float(selected_limit) - float(exit_price), 4)
        trade_window = hold_bars[hold_entry_index : exit_index + 1]
        max_drawdown = round(max(0.0, max(float(bar.high) for bar in trade_window) - float(selected_limit)), 4)

    net_pnl_points = round(
        pnl_points
        - _execution_cost_points(symbol=symbol, tick_size=TICK_SIZES[symbol], slippage_ticks_per_side=1.0),
        4,
    )
    exit_bar = hold_bars[exit_index]
    return RegimeEntryTrade(
        symbol=symbol,
        trade_date=context.trade_date.isoformat(),
        side=side,
        regime_state=selected_bias.state,
        entered=True,
        entry_reason="regime_limit_fill",
        signal_end_ts=signal_bar.end_ts.isoformat(),
        entry_end_ts=entry_fill.end_ts.isoformat(),
        limit_price=selected_limit,
        entry_price=selected_limit,
        stop_price=selected_stop,
        exit_end_ts=exit_bar.end_ts.isoformat(),
        exit_price=round(float(exit_price), 4),
        exit_reason=exit_reason,
        pnl_points=pnl_points,
        net_pnl_points=net_pnl_points,
        max_drawdown_points=max_drawdown,
        notes=tuple(selected_bias.reasons),
    )


def _find_limit_fill(
    *,
    side: str,
    entry_bars_1m: list[ResearchBar],
    limit_price: float,
    segment_end_ts: datetime,
) -> ResearchBar | None:
    for bar in entry_bars_1m:
        if bar.end_ts > segment_end_ts:
            return None
        if side == "LONG" and float(bar.low) <= limit_price:
            return bar
        if side == "SHORT" and float(bar.high) >= limit_price:
            return bar
    return None


def _group_bars_by_trade_day(*, bars: list[ResearchBar], symbol: str) -> dict[date, list[ResearchBar]]:
    grouped: dict[date, list[ResearchBar]] = defaultdict(list)
    for bar in bars:
        from .asia_london_participation_research import _trade_day_for_symbol

        grouped[_trade_day_for_symbol(symbol, bar.end_ts)].append(bar)
    return grouped


def _summarize_regime_trades(rows: list[RegimeEntryTrade]) -> dict[str, Any]:
    entered = [row for row in rows if row.entered and row.net_pnl_points is not None]
    winners = [row for row in entered if (row.net_pnl_points or 0.0) > 0.0]
    losers = [row for row in entered if (row.net_pnl_points or 0.0) <= 0.0]
    gross_profit = sum((row.net_pnl_points or 0.0) for row in winners)
    gross_loss = abs(sum((row.net_pnl_points or 0.0) for row in losers))
    net_curve = []
    running = 0.0
    for row in entered:
        running += row.net_pnl_points or 0.0
        net_curve.append(running)
    drawdowns = [max(net_curve[: index + 1]) - value for index, value in enumerate(net_curve)] if net_curve else [0.0]
    return {
        "entered_trade_count": len(entered),
        "average_net_pnl_points": round(statistics.fmean(row.net_pnl_points or 0.0 for row in entered), 4) if entered else None,
        "median_net_pnl_points": round(statistics.median(row.net_pnl_points or 0.0 for row in entered), 4) if entered else None,
        "net_profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
        "net_win_rate": round(len(winners) / len(entered), 4) if entered else None,
        "max_drawdown_points": round(max(drawdowns), 4) if drawdowns else 0.0,
    }


def _pair_rankings(*, symbol_reports: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    if not {"ES", "MES"} <= set(symbol_reports):
        return {}
    rows = []
    for variant_key in sorted(set(symbol_reports["ES"]["variants"]) & set(symbol_reports["MES"]["variants"])):
        es_summary = symbol_reports["ES"]["variants"][variant_key]["trade_summary"]
        mes_summary = symbol_reports["MES"]["variants"][variant_key]["trade_summary"]
        rows.append(
            {
                "variant_key": variant_key,
                "min_average_net_pnl_points": min(es_summary["average_net_pnl_points"] or 0.0, mes_summary["average_net_pnl_points"] or 0.0),
                "min_net_profit_factor": min(es_summary["net_profit_factor"] or 0.0, mes_summary["net_profit_factor"] or 0.0),
                "min_entered_trade_count": min(es_summary["entered_trade_count"], mes_summary["entered_trade_count"]),
                "max_pair_drawdown_points": max(es_summary["max_drawdown_points"] or 0.0, mes_summary["max_drawdown_points"] or 0.0),
            }
        )
    rows.sort(
        key=lambda row: (
            float(row["min_average_net_pnl_points"]),
            float(row["min_net_profit_factor"]),
            int(row["min_entered_trade_count"]),
        ),
        reverse=True,
    )
    return {"ES_MES": rows}


def _empty_trade(*, symbol: str, trade_day: date, side: str, note: str) -> RegimeEntryTrade:
    return RegimeEntryTrade(
        symbol=symbol,
        trade_date=trade_day.isoformat(),
        side=side,
        regime_state="NEUTRAL",
        entered=False,
        entry_reason=note,
        signal_end_ts=None,
        entry_end_ts=None,
        limit_price=None,
        entry_price=None,
        stop_price=None,
        exit_end_ts=None,
        exit_price=None,
        exit_reason=None,
        pnl_points=None,
        net_pnl_points=None,
        max_drawdown_points=None,
        notes=(note,),
    )


if __name__ == "__main__":
    raise SystemExit(main())
