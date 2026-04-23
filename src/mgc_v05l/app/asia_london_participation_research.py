"""Asia-to-London participation hold research across gold and stock-index futures.

The strategy shape is intentionally simple:
- establish directional bias during ASIA_EARLY using the validated forced-session entry variants
- hold through ASIA_LATE and LONDON_EARLY
- flatten during LONDON_LATE using the existing structure/stop exit logic, with a hard flat by segment close
"""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..config_models import load_settings_from_files
from ..research.trend_participation.models import ResearchBar
from ..research.trend_participation.storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m
from .gc_mgc_london_late_long_research import _ema as _long_ema
from .gc_mgc_london_late_long_research import _find_exit as _find_long_exit
from .gc_mgc_segment_forced_session_long_research import (
    ForcedSegmentLongSpec,
    build_variant_specs as build_long_variant_specs,
)
from .gc_mgc_segment_forced_session_long_research import _select_entry_index as _select_long_entry_index
from .gc_mgc_ny_early_short_research import NyEarlyShortSpec
from .gc_mgc_ny_early_short_research import _find_exit as _find_short_exit
from .gc_mgc_segment_forced_session_short_research import (
    ForcedSegmentShortSpec,
    build_variant_specs as build_short_variant_specs,
)
from .gc_mgc_segment_forced_session_short_research import _select_entry_index as _select_short_entry_index
from .gc_mgc_segment_regime_research import (
    DEFAULT_CONFIG_PATHS,
    _parse_date,
    _resolve_sqlite_database_path,
    label_gold_segment,
    trade_date_for_timestamp,
)
from .index_futures_forced_session_research import (
    label_stock_index_segment,
    stock_index_trade_date_for_timestamp,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "asia_london_participation_research"
DEFAULT_SYMBOLS: tuple[str, ...] = ("GC", "MGC", "ES", "MES", "NQ", "MNQ")
NEW_YORK = ZoneInfo("America/New_York")
VALID_SIDES: tuple[str, ...] = ("LONG", "SHORT")
ENTRY_SEGMENT = "ASIA_EARLY"
HOLD_SEGMENTS: tuple[str, ...] = ("ASIA_EARLY", "ASIA_LATE", "LONDON_EARLY", "LONDON_LATE")

DEFAULT_LONG_VARIANT_IDS: tuple[str, ...] = (
    "segment_forced_long_v4_breakout_or_bar7",
    "segment_forced_long_v5_dip_reclaim_or_bar8",
    "segment_forced_long_v6_contextual_fallback",
)
DEFAULT_SHORT_VARIANT_IDS: tuple[str, ...] = (
    "segment_forced_short_v2_reclaim_fail_or_bar7",
    "segment_forced_short_v4_breakdown_or_bar7",
    "segment_forced_short_v5_contextual_breakdown",
)
PAIR_GROUPS: tuple[tuple[str, ...], ...] = (("GC", "MGC"), ("ES", "MES"), ("NQ", "MNQ"))
POINT_VALUES: dict[str, float] = {
    "GC": 100.0,
    "MGC": 10.0,
    "ES": 50.0,
    "MES": 5.0,
    "NQ": 20.0,
    "MNQ": 2.0,
}
ROUND_TURN_COMMISSION_DOLLARS: dict[str, float] = {
    "GC": 4.5,
    "MGC": 1.5,
    "ES": 4.5,
    "MES": 1.5,
    "NQ": 4.5,
    "MNQ": 1.5,
}
TICK_SIZES: dict[str, float] = {
    "GC": 0.1,
    "MGC": 0.1,
    "ES": 0.25,
    "MES": 0.25,
    "NQ": 0.25,
    "MNQ": 0.25,
}


@dataclass(frozen=True)
class OvernightSessionContext:
    trade_date: date
    entry_segment_bars: list[ResearchBar]
    hold_bars: list[ResearchBar]


@dataclass(frozen=True)
class OvernightParticipationTrade:
    symbol: str
    trade_date: str
    side: str
    variant_id: str
    entered: bool
    entry_reason: str
    entry_bar_number: int | None
    entry_end_ts: str | None
    entry_price: float | None
    stop_price: float | None
    exit_bar_number: int | None
    exit_end_ts: str | None
    exit_price: float | None
    exit_reason: str | None
    exit_session_phase: str | None
    pnl_points: float | None
    net_pnl_points: float | None
    gross_r_multiple: float | None
    net_r_multiple: float | None
    mae_points: float | None
    mfe_points: float | None
    setup_return_points: float | None
    setup_range_points: float | None
    setup_close_location: float | None
    setup_vwap_displacement: float | None
    notes: tuple[str, ...] = ()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-london-participation-research")
    parser.add_argument("--symbol", action="append", default=None, help="Symbol to evaluate. Defaults to GC/MGC/ES/MES/NQ/MNQ.")
    parser.add_argument("--side", action="append", default=None, help="Optional side filter: LONG and/or SHORT.")
    parser.add_argument("--long-variant", action="append", default=None, help="Optional long variant id filter.")
    parser.add_argument("--short-variant", action="append", default=None, help="Optional short variant id filter.")
    parser.add_argument("--start-date", default=None, help="Optional inclusive trade-date filter.")
    parser.add_argument("--end-date", default=None, help="Optional inclusive trade-date filter.")
    parser.add_argument("--output-dir", default=None, help="Optional output directory override.")
    parser.add_argument("--config", action="append", default=None, help="Config path, may be supplied multiple times.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_asia_london_participation_research(
        symbols=args.symbol or list(DEFAULT_SYMBOLS),
        sides=args.side,
        long_variant_ids=args.long_variant,
        short_variant_ids=args.short_variant,
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        config_paths=args.config,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


def run_asia_london_participation_research(
    *,
    symbols: list[str] | tuple[str, ...] | None = None,
    sides: list[str] | tuple[str, ...] | None = None,
    long_variant_ids: list[str] | tuple[str, ...] | None = None,
    short_variant_ids: list[str] | tuple[str, ...] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    output_dir: str | Path | None = None,
    config_paths: list[str] | list[Path] | tuple[str | Path, ...] | None = None,
) -> dict[str, Any]:
    resolved_symbols = tuple(sorted({str(symbol).strip().upper() for symbol in (symbols or DEFAULT_SYMBOLS) if str(symbol).strip()}))
    requested_sides = {str(item).strip().upper() for item in (sides or VALID_SIDES) if str(item).strip()}
    resolved_sides = tuple(side for side in VALID_SIDES if side in requested_sides)
    if not resolved_sides:
        raise ValueError("No valid sides selected.")

    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    settings = load_settings_from_files([Path(path) for path in (config_paths or DEFAULT_CONFIG_PATHS)])
    sqlite_path = _resolve_sqlite_database_path(settings.database_url)
    start_day = _parse_date(start_date)
    end_day = _parse_date(end_date)

    long_specs = tuple(
        replace(spec, tick_size=TICK_SIZES["GC"])
        for spec in build_long_variant_specs(selected_ids=list(long_variant_ids or DEFAULT_LONG_VARIANT_IDS))
        if spec.decision_timeframe == "3m"
    )
    short_specs = tuple(
        replace(spec, tick_size=TICK_SIZES["GC"])
        for spec in build_short_variant_specs(selected_ids=list(short_variant_ids or DEFAULT_SHORT_VARIANT_IDS))
        if spec.decision_timeframe == "3m"
    )

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
        contexts = _build_overnight_session_contexts(
            decision_bars=bars_3m,
            symbol=symbol,
            start_day=start_day,
            end_day=end_day,
        )
        variants: dict[str, Any] = {}
        if "LONG" in resolved_sides:
            for base_spec in long_specs:
                spec = replace(base_spec, tick_size=TICK_SIZES[str(symbol).upper()])
                sessions = [
                    _evaluate_long_participation(symbol=symbol, context=context, spec=spec)
                    for context in contexts
                ]
                variant_key = f"LONG__{spec.variant_id}"
                variants[variant_key] = {
                    "side": "LONG",
                    "variant_id": spec.variant_id,
                    "description": spec.description,
                    "trade_summary": _summarize_participation_trades(sessions),
                    "sessions": [asdict(row) for row in sessions],
                }
        if "SHORT" in resolved_sides:
            for base_spec in short_specs:
                spec = replace(base_spec, tick_size=TICK_SIZES[str(symbol).upper()])
                sessions = [
                    _evaluate_short_participation(symbol=symbol, context=context, spec=spec)
                    for context in contexts
                ]
                variant_key = f"SHORT__{spec.variant_id}"
                variants[variant_key] = {
                    "side": "SHORT",
                    "variant_id": spec.variant_id,
                    "description": spec.description,
                    "trade_summary": _summarize_participation_trades(sessions),
                    "sessions": [asdict(row) for row in sessions],
                }
        symbol_reports[symbol] = {
            "bar_count_1m": len(normalized_1m),
            "bar_count_3m": len(bars_3m),
            "context_count": len(contexts),
            "data_quality_issues": [asdict(issue) for issue in quality_issues],
            "variants": variants,
        }

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "asia_london_participation_research",
        "definition": {
            "symbols": list(resolved_symbols),
            "sides": list(resolved_sides),
            "entry_segment": ENTRY_SEGMENT,
            "hold_segments": list(HOLD_SEGMENTS),
            "session_windows_et": {
                "SESSION_OPEN": "18:00-19:00",
                "ASIA_EARLY": "19:00-20:30",
                "ASIA_LATE": "20:30-23:00",
                "LONDON_EARLY": "03:00-05:30",
                "LONDON_LATE": "05:30-08:20",
                "US_EARLY": "08:20-11:00",
                "US_MIDDAY": "11:00-13:30",
                "US_LATE": "13:30-16:00",
            },
            "description": "Enter during ASIA_EARLY using validated forced-session entry logic, then hold through Asia/Europe and flatten during LONDON_LATE.",
            "long_variants": [asdict(spec) for spec in long_specs],
            "short_variants": [asdict(spec) for spec in short_specs],
            "start_date": start_day.isoformat() if start_day else None,
            "end_date": end_day.isoformat() if end_day else None,
        },
        "database_path": str(sqlite_path),
        "symbol_reports": symbol_reports,
        "pair_rankings": _pair_rankings(symbol_reports=symbol_reports),
        "all_symbol_ranking": _all_symbol_ranking(symbol_reports=symbol_reports),
    }
    json_path = resolved_output_dir / "asia_london_participation_research.json"
    markdown_path = resolved_output_dir / "asia_london_participation_research.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "asia_london_participation_research",
        "artifact_paths": {"json": str(json_path), "markdown": str(markdown_path)},
        "pair_rankings": payload["pair_rankings"],
        "all_symbol_ranking": payload["all_symbol_ranking"][:12],
    }


def _label_symbol_segment(symbol: str, timestamp: datetime) -> str | None:
    if str(symbol).upper() in {"GC", "MGC"}:
        return label_gold_segment(timestamp.astimezone(NEW_YORK))
    return label_stock_index_segment(timestamp)


def _trade_day_for_symbol(symbol: str, timestamp: datetime) -> date:
    if str(symbol).upper() in {"GC", "MGC"}:
        return trade_date_for_timestamp(timestamp.astimezone(NEW_YORK))
    return stock_index_trade_date_for_timestamp(timestamp)


def _build_overnight_session_contexts(
    *,
    decision_bars: list[ResearchBar],
    symbol: str,
    start_day: date | None,
    end_day: date | None,
) -> list[OvernightSessionContext]:
    grouped_hold: dict[date, list[ResearchBar]] = defaultdict(list)
    grouped_entry: dict[date, list[ResearchBar]] = defaultdict(list)
    for bar in decision_bars:
        segment = _label_symbol_segment(symbol, bar.end_ts)
        if segment not in HOLD_SEGMENTS:
            continue
        trade_day = _trade_day_for_symbol(symbol, bar.end_ts)
        if start_day is not None and trade_day < start_day:
            continue
        if end_day is not None and trade_day > end_day:
            continue
        grouped_hold[trade_day].append(bar)
        if segment == ENTRY_SEGMENT:
            grouped_entry[trade_day].append(bar)
    contexts: list[OvernightSessionContext] = []
    for trade_day in sorted(grouped_hold):
        hold_bars = grouped_hold[trade_day]
        entry_bars = grouped_entry.get(trade_day, [])
        if hold_bars:
            contexts.append(
                OvernightSessionContext(
                    trade_date=trade_day,
                    entry_segment_bars=entry_bars,
                    hold_bars=hold_bars,
                )
            )
    return contexts


def _evaluate_long_participation(
    *,
    symbol: str,
    context: OvernightSessionContext,
    spec: ForcedSegmentLongSpec,
) -> OvernightParticipationTrade:
    segment_bars = context.entry_segment_bars
    hold_bars = context.hold_bars
    if len(segment_bars) <= spec.fallback_entry_bar:
        return _empty_trade(symbol=symbol, trade_day=context.trade_date, side="LONG", variant_id=spec.variant_id, note="insufficient_segment_bars")

    setup_bars = segment_bars[: spec.setup_bar_count]
    setup_high = max(bar.high for bar in setup_bars)
    setup_low = min(bar.low for bar in setup_bars)
    setup_range = max(setup_high - setup_low, 1e-9)
    setup_return = setup_bars[-1].close - setup_bars[0].open
    setup_close_location = (setup_bars[-1].close - setup_low) / setup_range
    setup_vwap = _bars_vwap(setup_bars)
    setup_vwap_displacement = (setup_bars[-1].close - setup_vwap) / setup_range

    entry_index, entry_reason = _select_long_entry_index(
        segment_bars=segment_bars,
        segment_id=ENTRY_SEGMENT,
        spec=spec,
        setup_low=setup_low,
        setup_high=setup_high,
        setup_close_location=setup_close_location,
        setup_vwap_displacement=setup_vwap_displacement,
        setup_return=setup_return,
        setup_range=setup_range,
    )
    entry_bar = segment_bars[entry_index]
    entry_price = round(float(entry_bar.open), 4)
    stop_price = round(float(setup_low - spec.tick_size), 4)
    risk_points = round(max(entry_price - stop_price, spec.tick_size), 4)
    ema_values = _long_ema([bar.close for bar in hold_bars], length=spec.exit_ema_length)
    exit_index, exit_price, exit_reason = _find_long_exit(
        segment_bars=hold_bars,
        entry_index=entry_index,
        stop_price=stop_price,
        ema_values=ema_values,
        spec=spec,
    )
    pnl_points = round(exit_price - entry_price, 4)
    execution_cost_points = round(_execution_cost_points(symbol=symbol, tick_size=spec.tick_size, slippage_ticks_per_side=spec.slippage_ticks_per_side), 4)
    net_pnl_points = round(pnl_points - execution_cost_points, 4)
    trade_window = hold_bars[entry_index : exit_index + 1]
    mfe_points = round(max(0.0, max(bar.high for bar in trade_window) - entry_price), 4)
    mae_points = round(max(0.0, entry_price - min(bar.low for bar in trade_window)), 4)
    gross_r_multiple = round(pnl_points / risk_points, 4) if risk_points > 0 else None
    net_r_multiple = round(net_pnl_points / risk_points, 4) if risk_points > 0 else None
    exit_bar = hold_bars[exit_index]
    return OvernightParticipationTrade(
        symbol=symbol,
        trade_date=context.trade_date.isoformat(),
        side="LONG",
        variant_id=spec.variant_id,
        entered=True,
        entry_reason=entry_reason,
        entry_bar_number=entry_index + 1,
        entry_end_ts=entry_bar.end_ts.isoformat(),
        entry_price=entry_price,
        stop_price=stop_price,
        exit_bar_number=exit_index + 1,
        exit_end_ts=exit_bar.end_ts.isoformat(),
        exit_price=exit_price,
        exit_reason=exit_reason,
        exit_session_phase=_label_symbol_segment(symbol, exit_bar.end_ts),
        pnl_points=pnl_points,
        net_pnl_points=net_pnl_points,
        gross_r_multiple=gross_r_multiple,
        net_r_multiple=net_r_multiple,
        mae_points=mae_points,
        mfe_points=mfe_points,
        setup_return_points=round(float(setup_return), 4),
        setup_range_points=round(float(setup_range), 4),
        setup_close_location=round(float(setup_close_location), 4),
        setup_vwap_displacement=round(float(setup_vwap_displacement), 4),
        notes=(),
    )


def _evaluate_short_participation(
    *,
    symbol: str,
    context: OvernightSessionContext,
    spec: ForcedSegmentShortSpec,
) -> OvernightParticipationTrade:
    segment_bars = context.entry_segment_bars
    hold_bars = context.hold_bars
    if len(segment_bars) <= spec.fallback_entry_bar:
        return _empty_trade(symbol=symbol, trade_day=context.trade_date, side="SHORT", variant_id=spec.variant_id, note="insufficient_segment_bars")

    setup_bars = segment_bars[: spec.setup_bar_count]
    setup_high = max(bar.high for bar in setup_bars)
    setup_low = min(bar.low for bar in setup_bars)
    setup_range = max(setup_high - setup_low, 1e-9)
    setup_return = setup_bars[-1].close - setup_bars[0].open
    setup_close_location = (setup_bars[-1].close - setup_low) / setup_range
    setup_vwap = _bars_vwap(setup_bars)
    setup_vwap_displacement = (setup_bars[-1].close - setup_vwap) / setup_range

    entry_index, entry_reason = _select_short_entry_index(
        segment_bars=segment_bars,
        segment_id=ENTRY_SEGMENT,
        spec=spec,
        setup_low=setup_low,
        setup_high=setup_high,
        setup_close_location=setup_close_location,
        setup_vwap_displacement=setup_vwap_displacement,
        setup_return=setup_return,
        setup_range=setup_range,
    )
    entry_bar = segment_bars[entry_index]
    entry_price = round(float(entry_bar.open), 4)
    stop_price = round(float(setup_high + spec.tick_size), 4)
    risk_points = round(max(stop_price - entry_price, spec.tick_size), 4)
    ema_values = _long_ema([bar.close for bar in hold_bars], length=spec.exit_ema_length)
    base_spec = NyEarlyShortSpec(
        variant_id=spec.variant_id,
        description=spec.description,
        setup_family="asia_london_participation",
        decision_timeframe=spec.decision_timeframe,
        exit_mode=spec.exit_mode,
        exit_ema_length=spec.exit_ema_length,
        tick_size=spec.tick_size,
        slippage_ticks_per_side=spec.slippage_ticks_per_side,
        gc_round_turn_commission_dollars=spec.gc_round_turn_commission_dollars,
        mgc_round_turn_commission_dollars=spec.mgc_round_turn_commission_dollars,
    )
    exit_index, exit_price, exit_reason = _find_short_exit(
        segment_bars=hold_bars,
        entry_index=entry_index,
        stop_price=stop_price,
        ema_values=ema_values,
        spec=base_spec,
    )
    pnl_points = round(entry_price - exit_price, 4)
    execution_cost_points = round(_execution_cost_points(symbol=symbol, tick_size=spec.tick_size, slippage_ticks_per_side=spec.slippage_ticks_per_side), 4)
    net_pnl_points = round(pnl_points - execution_cost_points, 4)
    trade_window = hold_bars[entry_index : exit_index + 1]
    mfe_points = round(max(0.0, entry_price - min(bar.low for bar in trade_window)), 4)
    mae_points = round(max(0.0, max(bar.high for bar in trade_window) - entry_price), 4)
    gross_r_multiple = round(pnl_points / risk_points, 4) if risk_points > 0 else None
    net_r_multiple = round(net_pnl_points / risk_points, 4) if risk_points > 0 else None
    exit_bar = hold_bars[exit_index]
    return OvernightParticipationTrade(
        symbol=symbol,
        trade_date=context.trade_date.isoformat(),
        side="SHORT",
        variant_id=spec.variant_id,
        entered=True,
        entry_reason=entry_reason,
        entry_bar_number=entry_index + 1,
        entry_end_ts=entry_bar.end_ts.isoformat(),
        entry_price=entry_price,
        stop_price=stop_price,
        exit_bar_number=exit_index + 1,
        exit_end_ts=exit_bar.end_ts.isoformat(),
        exit_price=exit_price,
        exit_reason=exit_reason,
        exit_session_phase=_label_symbol_segment(symbol, exit_bar.end_ts),
        pnl_points=pnl_points,
        net_pnl_points=net_pnl_points,
        gross_r_multiple=gross_r_multiple,
        net_r_multiple=net_r_multiple,
        mae_points=mae_points,
        mfe_points=mfe_points,
        setup_return_points=round(float(setup_return), 4),
        setup_range_points=round(float(setup_range), 4),
        setup_close_location=round(float(setup_close_location), 4),
        setup_vwap_displacement=round(float(setup_vwap_displacement), 4),
        notes=(),
    )


def _empty_trade(*, symbol: str, trade_day: date, side: str, variant_id: str, note: str) -> OvernightParticipationTrade:
    return OvernightParticipationTrade(
        symbol=symbol,
        trade_date=trade_day.isoformat(),
        side=side,
        variant_id=variant_id,
        entered=False,
        entry_reason=note,
        entry_bar_number=None,
        entry_end_ts=None,
        entry_price=None,
        stop_price=None,
        exit_bar_number=None,
        exit_end_ts=None,
        exit_price=None,
        exit_reason=None,
        exit_session_phase=None,
        pnl_points=None,
        net_pnl_points=None,
        gross_r_multiple=None,
        net_r_multiple=None,
        mae_points=None,
        mfe_points=None,
        setup_return_points=None,
        setup_range_points=None,
        setup_close_location=None,
        setup_vwap_displacement=None,
        notes=(note,),
    )


def _bars_vwap(bars: list[ResearchBar]) -> float:
    total_volume = sum(max(float(bar.volume), 1.0) for bar in bars)
    total_pv = sum(float(bar.close) * max(float(bar.volume), 1.0) for bar in bars)
    return total_pv / max(total_volume, 1.0)


def _execution_cost_points(*, symbol: str, tick_size: float, slippage_ticks_per_side: float) -> float:
    normalized = str(symbol).upper()
    point_value = POINT_VALUES[normalized]
    commission_dollars = ROUND_TURN_COMMISSION_DOLLARS[normalized]
    slippage_points = 2.0 * slippage_ticks_per_side * tick_size
    commission_points = commission_dollars / point_value
    return slippage_points + commission_points


def _summarize_participation_trades(rows: list[OvernightParticipationTrade]) -> dict[str, Any]:
    entered = [row for row in rows if row.entered and row.net_pnl_points is not None]
    winners = [row for row in entered if (row.net_pnl_points or 0.0) > 0.0]
    losers = [row for row in entered if (row.net_pnl_points or 0.0) <= 0.0]
    gross_profit = sum((row.net_pnl_points or 0.0) for row in winners)
    gross_loss = abs(sum((row.net_pnl_points or 0.0) for row in losers))
    return {
        "entered_trade_count": len(entered),
        "average_net_pnl_points": round(statistics.fmean(row.net_pnl_points or 0.0 for row in entered), 4) if entered else None,
        "median_net_pnl_points": round(statistics.median(row.net_pnl_points or 0.0 for row in entered), 4) if entered else None,
        "net_profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
        "net_win_rate": round(len(winners) / len(entered), 4) if entered else None,
        "entered_trade_days": [row.trade_date for row in entered],
    }


def _pair_rankings(*, symbol_reports: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    rankings: dict[str, list[dict[str, Any]]] = {}
    for pair in PAIR_GROUPS:
        if any(symbol not in symbol_reports for symbol in pair):
            continue
        shared_keys = set(symbol_reports[pair[0]]["variants"].keys())
        for symbol in pair[1:]:
            shared_keys &= set(symbol_reports[symbol]["variants"].keys())
        pair_rows: list[dict[str, Any]] = []
        for key in sorted(shared_keys):
            summaries = [symbol_reports[symbol]["variants"][key]["trade_summary"] for symbol in pair]
            pair_rows.append(
                {
                    "variant_key": key,
                    "side": symbol_reports[pair[0]]["variants"][key]["side"],
                    "variant_id": symbol_reports[pair[0]]["variants"][key]["variant_id"],
                    "description": symbol_reports[pair[0]]["variants"][key]["description"],
                    "symbols": list(pair),
                    "min_entered_trade_count": min(summary["entered_trade_count"] for summary in summaries),
                    "min_average_net_pnl_points": min(summary["average_net_pnl_points"] or 0.0 for summary in summaries),
                    "min_net_profit_factor": min(summary["net_profit_factor"] or 0.0 for summary in summaries),
                }
            )
        pair_rows.sort(
            key=lambda row: (
                float(row["min_average_net_pnl_points"]),
                float(row["min_net_profit_factor"]),
                int(row["min_entered_trade_count"]),
            ),
            reverse=True,
        )
        rankings["_".join(pair)] = pair_rows
    return rankings


def _all_symbol_ranking(*, symbol_reports: dict[str, Any]) -> list[dict[str, Any]]:
    if not symbol_reports:
        return []
    all_keys = set.intersection(*(set(report["variants"].keys()) for report in symbol_reports.values() if report["variants"]))
    ranked: list[dict[str, Any]] = []
    for key in sorted(all_keys):
        summaries = [report["variants"][key]["trade_summary"] for report in symbol_reports.values()]
        first_variant = next(report["variants"][key] for report in symbol_reports.values())
        ranked.append(
            {
                "variant_key": key,
                "side": first_variant["side"],
                "variant_id": first_variant["variant_id"],
                "description": first_variant["description"],
                "symbols": sorted(symbol_reports.keys()),
                "min_entered_trade_count": min(summary["entered_trade_count"] for summary in summaries),
                "min_average_net_pnl_points": min(summary["average_net_pnl_points"] or 0.0 for summary in summaries),
                "min_net_profit_factor": min(summary["net_profit_factor"] or 0.0 for summary in summaries),
            }
        )
    ranked.sort(
        key=lambda row: (
            float(row["min_average_net_pnl_points"]),
            float(row["min_net_profit_factor"]),
            int(row["min_entered_trade_count"]),
        ),
        reverse=True,
    )
    return ranked


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Asia-to-London Participation Research",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Database: `{report['database_path']}`",
        f"- Entry segment: `{report['definition']['entry_segment']}`",
        f"- Hold segments: `{', '.join(report['definition']['hold_segments'])}`",
        "",
        "## Cross-Pair Ranking",
        "",
    ]
    for pair_key, rows in report["pair_rankings"].items():
        lines.append(f"### {pair_key}")
        lines.append("")
        for row in rows[:8]:
            lines.append(
                f"- `{row['variant_key']}`: min avg net `{row['min_average_net_pnl_points']}`, min PF `{row['min_net_profit_factor']}`, min trades `{row['min_entered_trade_count']}`"
            )
        lines.append("")
    lines.extend(["## All-Symbol Ranking", ""])
    for row in report["all_symbol_ranking"][:12]:
        lines.append(
            f"- `{row['variant_key']}`: min avg net `{row['min_average_net_pnl_points']}`, min PF `{row['min_net_profit_factor']}`, min trades `{row['min_entered_trade_count']}`"
        )
    return "\n".join(lines)


def _load_start_timestamp(start_day: date | None) -> datetime | None:
    if start_day is None:
        return None
    return datetime.combine(start_day - timedelta(days=1), time(18, 0), tzinfo=NEW_YORK)


def _load_end_timestamp(end_day: date | None) -> datetime | None:
    if end_day is None:
        return None
    return datetime.combine(end_day, time(8, 30), tzinfo=NEW_YORK)


if __name__ == "__main__":
    raise SystemExit(main())
