"""Research study for an ES/MES opening-drive continuation long model."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..config_models import load_settings_from_files
from ..research.trend_participation.models import ResearchBar
from ..research.trend_participation.storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "es_mes_opening_drive_continuation"
DEFAULT_CONFIG_PATHS = (
    REPO_ROOT / "config" / "base.yaml",
    REPO_ROOT / "config" / "replay.yaml",
)
DEFAULT_SYMBOLS = ("ES", "MES")
NEW_YORK = ZoneInfo("America/New_York")
PREOPEN_START = time(9, 0)
PREOPEN_END = time(9, 30)
CASH_FIRST_BAR_END = time(9, 35)


@dataclass(frozen=True)
class OpeningDriveSpec:
    variant_id: str = "opening_drive_continuation_v1"
    signal_symbol_preference: str = "ES"
    source_timeframe: str = "1m"
    decision_timeframe: str = "5m"
    setup_bar_count: int = 5
    entry_lookahead_bars: int = 3
    min_green_bars_in_setup: int = 3
    min_setup_strength_multiple: float = 1.35
    max_pullback_drawdown_ratio: float = 1.50
    min_setup_close_location: float = 0.55
    require_setup_bar_breakout: bool = True
    entry_trigger_mode: str = "setup_bar_high"
    require_last_setup_higher_low: bool = False
    require_last_setup_close_above_first_close: bool = False
    require_entry_vwap_reclaim: bool = False
    exit_ema_length: int = 5
    exit_structure_lookback: int = 3
    exit_cutoff_time: str = "12:00"
    exit_on_confirmation_next_open: bool = False
    require_preopen_breakout: bool = True
    require_vwap_support: bool = True
    preopen_base_lookback_minutes: int = 5
    tick_size: float = 0.25
    slippage_ticks_per_side: float = 1.0
    es_round_turn_commission_dollars: float = 4.50
    mes_round_turn_commission_dollars: float = 1.50


@dataclass(frozen=True)
class ContractEconomics:
    symbol: str
    point_value_dollars: float
    round_turn_commission_dollars: float


@dataclass(frozen=True)
class SessionTrade:
    symbol: str
    session_date: str
    qualified: bool
    entered: bool
    trigger_price: float | None
    initial_stop_price: float | None
    entry_bar_number: int | None
    entry_end_ts: str | None
    entry_price: float | None
    exit_bar_number: int | None
    exit_end_ts: str | None
    exit_price: float | None
    exit_reason: str | None
    hold_bars: int | None
    pnl_points: float | None
    gross_pnl_dollars: float | None
    net_pnl_points: float | None
    net_pnl_dollars: float | None
    execution_cost_points: float | None
    execution_cost_dollars: float | None
    risk_points: float | None
    risk_dollars: float | None
    gross_r_multiple: float | None
    net_r_multiple: float | None
    mae_points: float | None
    mfe_points: float | None
    mae_r_multiple: float | None
    mfe_r_multiple: float | None
    opening_move_points: float | None
    setup_strength_multiple: float | None
    drawdown_ratio: float | None
    green_bar_count: int
    setup_close_location: float | None
    preopen_high: float | None
    preopen_low: float | None
    preopen_breakout: bool
    vwap_supported: bool
    setup_bar_high: float | None
    setup_bar_close: float | None
    notes: tuple[str, ...] = ()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="es-mes-opening-drive-continuation-research")
    parser.add_argument("--symbol", action="append", default=None, help="Symbol to evaluate. Defaults to ES and MES.")
    parser.add_argument("--output-dir", default=None, help="Optional output directory override.")
    parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Config file path. May be supplied multiple times; later files override earlier ones.",
    )
    parser.add_argument("--start-date", default=None, help="Optional inclusive local session date in YYYY-MM-DD form.")
    parser.add_argument("--end-date", default=None, help="Optional inclusive local session date in YYYY-MM-DD form.")
    parser.add_argument(
        "--inspect-date",
        default="2026-04-17",
        help="Specific session date to highlight in the report. Defaults to 2026-04-17.",
    )
    parser.add_argument(
        "--variant",
        choices=["v1", "v2", "v3"],
        default="v1",
        help="Research variant to run. v1 is the original confirmation entry; v2 is the broader early-entry version; v3 is the stricter A+ trend-day capture version.",
    )
    parser.add_argument(
        "--slippage-ticks-per-side",
        type=float,
        default=OpeningDriveSpec.slippage_ticks_per_side,
        help="Execution slippage assumption in ticks per side. Defaults to 1.0.",
    )
    parser.add_argument(
        "--es-round-turn-commission",
        type=float,
        default=OpeningDriveSpec.es_round_turn_commission_dollars,
        help="Round-turn commission assumption in dollars for ES. Defaults to 4.50.",
    )
    parser.add_argument(
        "--mes-round-turn-commission",
        type=float,
        default=OpeningDriveSpec.mes_round_turn_commission_dollars,
        help="Round-turn commission assumption in dollars for MES. Defaults to 1.50.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_es_mes_opening_drive_continuation_research(
        symbols=args.symbol or list(DEFAULT_SYMBOLS),
        output_dir=args.output_dir,
        config_paths=args.config,
        start_date=args.start_date,
        end_date=args.end_date,
        inspect_date=args.inspect_date,
        spec=build_variant_spec(
            variant=str(args.variant),
            slippage_ticks_per_side=float(args.slippage_ticks_per_side),
            es_round_turn_commission_dollars=float(args.es_round_turn_commission),
            mes_round_turn_commission_dollars=float(args.mes_round_turn_commission),
        ),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_es_mes_opening_drive_continuation_research(
    *,
    symbols: list[str] | tuple[str, ...] | None = None,
    output_dir: str | Path | None = None,
    config_paths: list[str] | list[Path] | tuple[str | Path, ...] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    inspect_date: str | date = date(2026, 4, 17),
    spec: OpeningDriveSpec | None = None,
) -> dict[str, Any]:
    resolved_spec = spec or OpeningDriveSpec()
    resolved_symbols = tuple(
        sorted({str(symbol).strip().upper() for symbol in (symbols or DEFAULT_SYMBOLS) if str(symbol).strip()})
    )
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    settings = load_settings_from_files([Path(path) for path in (config_paths or DEFAULT_CONFIG_PATHS)])
    sqlite_path = _resolve_sqlite_database_path(settings.database_url)
    start_day = _parse_date(start_date)
    end_day = _parse_date(end_date)
    highlighted_day = _parse_date(inspect_date)
    start_ts = _day_start(start_day) if start_day is not None else None
    end_ts = _day_end(end_day) if end_day is not None else None

    symbol_reports: dict[str, Any] = {}
    highlighted_sessions: dict[str, Any] = {}
    for symbol in resolved_symbols:
        one_minute_count = 0
        five_minute_count = 0
        quality_issues: list[dict[str, Any]] = []
        session_rows: list[SessionTrade] = []
        for window_start_day, window_end_day in _iter_processing_windows(start_day=start_day, end_day=end_day):
            window_start_ts = _day_start(window_start_day) if window_start_day is not None else start_ts
            window_end_ts = _day_end(window_end_day) if window_end_day is not None else end_ts
            one_minute = load_sqlite_bars(
                sqlite_path=sqlite_path,
                instrument=symbol,
                timeframe=resolved_spec.source_timeframe,
                data_source="historical_1m_canonical",
                start_ts=window_start_ts,
                end_ts=window_end_ts,
            )
            normalized_1m, batch_issues = normalize_and_check_bars(
                bars=one_minute,
                timeframe=resolved_spec.source_timeframe,
            )
            bars_5m = resample_bars_from_1m(bars_1m=normalized_1m, target_timeframe=resolved_spec.decision_timeframe)
            session_rows.extend(
                evaluate_symbol_sessions(
                    symbol=symbol,
                    one_minute_bars=normalized_1m,
                    five_minute_bars=bars_5m,
                    spec=resolved_spec,
                    start_day=window_start_day,
                    end_day=window_end_day,
                )
            )
            one_minute_count += len(normalized_1m)
            five_minute_count += len(bars_5m)
            quality_issues.extend(asdict(issue) for issue in batch_issues)
        symbol_reports[symbol] = {
            "bar_count_1m": one_minute_count,
            "bar_count_5m": five_minute_count,
            "data_quality_issues": quality_issues,
            "session_count": len(session_rows),
            "trade_summary": _summarize_trades(session_rows),
            "sessions": [asdict(row) for row in session_rows],
        }
        highlighted = next((row for row in session_rows if row.session_date == highlighted_day.isoformat()), None)
        highlighted_sessions[symbol] = asdict(highlighted) if highlighted is not None else None

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "es_mes_opening_drive_continuation",
        "study_status": "research_first_pass",
        "definition": {
            "description": (
                "Opening-drive continuation long model: require a pre-open/base breakout, a strong first five-bar "
                "cash-open drive, a shallow pullback profile, then buy the first pause-high breakout and exit on "
                "the first confirmed EMA-plus-structure failure."
            ),
            "spec": asdict(resolved_spec),
            "notes": [
                "Signal discovery should be done on ES; MES is the execution-equivalent cross-check.",
                "The broader chart context is used as an entry-shape clue: avoid pure first-bar chase and prefer a base/pause breakout.",
                "Execution-cost assumptions are placeholders for research and should be tuned to your actual brokerage/size profile.",
                "This artifact is durable research code, not a production trading lane.",
            ],
        },
        "database_path": str(sqlite_path),
        "symbols": list(resolved_symbols),
        "date_filter": {
            "start_date": start_day.isoformat() if start_day is not None else None,
            "end_date": end_day.isoformat() if end_day is not None else None,
        },
        "highlighted_session_date": highlighted_day.isoformat(),
        "highlighted_sessions": highlighted_sessions,
        "symbol_reports": symbol_reports,
        "cross_symbol_summary": _cross_symbol_summary(symbol_reports),
    }

    json_path = resolved_output_dir / "es_mes_opening_drive_continuation_research.json"
    markdown_path = resolved_output_dir / "es_mes_opening_drive_continuation_research.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=_json_ready) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report).strip() + "\n", encoding="utf-8")
    return {
        "mode": "es_mes_opening_drive_continuation_research",
        "artifact_paths": {"json": str(json_path), "markdown": str(markdown_path)},
        "highlighted_session_date": highlighted_day.isoformat(),
        "highlighted_sessions": highlighted_sessions,
        "cross_symbol_summary": report["cross_symbol_summary"],
    }


def evaluate_symbol_sessions(
    *,
    symbol: str,
    one_minute_bars: list[ResearchBar],
    five_minute_bars: list[ResearchBar],
    spec: OpeningDriveSpec,
    start_day: date | None = None,
    end_day: date | None = None,
) -> list[SessionTrade]:
    session_dates = sorted(
        {
            _local_timestamp(bar.end_ts).date()
            for bar in five_minute_bars
            if _local_timestamp(bar.end_ts).time() >= CASH_FIRST_BAR_END
        }
    )
    if start_day is not None:
        session_dates = [item for item in session_dates if item >= start_day]
    if end_day is not None:
        session_dates = [item for item in session_dates if item <= end_day]

    rows: list[SessionTrade] = []
    for session_day in session_dates:
        preopen = [
            bar
            for bar in one_minute_bars
            if _local_timestamp(bar.end_ts).date() == session_day and PREOPEN_START <= _local_timestamp(bar.end_ts).time() < PREOPEN_END
        ]
        cash = [
            bar
            for bar in five_minute_bars
            if _local_timestamp(bar.end_ts).date() == session_day and CASH_FIRST_BAR_END <= _local_timestamp(bar.end_ts).time() <= _parse_clock(spec.exit_cutoff_time)
        ]
        if cash:
            rows.append(_evaluate_session(symbol=symbol, session_day=session_day, preopen_bars=preopen, cash_bars=cash, spec=spec))
    return rows


def _evaluate_session(
    *,
    symbol: str,
    session_day: date,
    preopen_bars: list[ResearchBar],
    cash_bars: list[ResearchBar],
    spec: OpeningDriveSpec,
) -> SessionTrade:
    notes: list[str] = []
    if len(cash_bars) < spec.setup_bar_count + 1:
        notes.append("insufficient_cash_bars")
        return _blank_session_trade(symbol=symbol, session_day=session_day, notes=tuple(notes))

    setup_bars = cash_bars[: spec.setup_bar_count]
    setup_bar = setup_bars[-1]
    preopen_base_bars = _select_preopen_base(preopen_bars, session_day=session_day, spec=spec)
    preopen_high = max((bar.high for bar in preopen_base_bars), default=None)
    preopen_low = min((bar.low for bar in preopen_base_bars), default=None)
    green_bar_count = sum(1 for bar in setup_bars if bar.close > bar.open)
    average_setup_range = statistics.fmean(bar.range_points for bar in setup_bars)
    opening_move_points = setup_bar.close - setup_bars[0].open
    setup_strength = opening_move_points / max(average_setup_range, 1e-9)
    setup_close_location = (setup_bar.close - setup_bar.low) / max(setup_bar.range_points, 1e-9)
    drawdown_ratio = _drawdown_ratio(setup_bars)
    vwap_values = _session_vwap(cash_bars)
    vwap_supported = (
        len(vwap_values) >= spec.setup_bar_count
        and setup_bars[-2].close > vwap_values[spec.setup_bar_count - 2]
        and setup_bar.close > vwap_values[spec.setup_bar_count - 1]
    )
    prior_high = max(bar.high for bar in setup_bars[:-1])
    pause_breakout = setup_bar.high > prior_high and setup_bar.close >= max(bar.close for bar in setup_bars[:-1])
    preopen_breakout = preopen_high is not None and setup_bar.close > preopen_high
    last_setup_higher_low = (
        len(setup_bars) < 3 or setup_bars[-1].low > min(bar.low for bar in setup_bars[1:-1])
    )
    last_setup_close_above_first_close = setup_bars[-1].close > setup_bars[0].close

    checks = {
        "green_share_ok": green_bar_count >= spec.min_green_bars_in_setup,
        "setup_strength_ok": setup_strength >= spec.min_setup_strength_multiple,
        "setup_close_ok": setup_close_location >= spec.min_setup_close_location,
        "pause_breakout_ok": (pause_breakout if spec.require_setup_bar_breakout else True),
        "preopen_breakout_ok": (preopen_breakout if spec.require_preopen_breakout else True),
        "vwap_supported_ok": (vwap_supported if spec.require_vwap_support else True),
        "last_setup_higher_low_ok": (last_setup_higher_low if spec.require_last_setup_higher_low else True),
        "last_setup_close_above_first_close_ok": (
            last_setup_close_above_first_close if spec.require_last_setup_close_above_first_close else True
        ),
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        notes.extend(failed)
        return SessionTrade(
            symbol=symbol,
            session_date=session_day.isoformat(),
            qualified=False,
            entered=False,
            trigger_price=None,
            initial_stop_price=None,
            entry_bar_number=None,
            entry_end_ts=None,
            entry_price=None,
            exit_bar_number=None,
            exit_end_ts=None,
            exit_price=None,
            exit_reason=None,
            hold_bars=None,
            pnl_points=None,
            gross_pnl_dollars=None,
            net_pnl_points=None,
            net_pnl_dollars=None,
            execution_cost_points=None,
            execution_cost_dollars=None,
            risk_points=None,
            risk_dollars=None,
            gross_r_multiple=None,
            net_r_multiple=None,
            mae_points=None,
            mfe_points=None,
            mae_r_multiple=None,
            mfe_r_multiple=None,
            opening_move_points=round(opening_move_points, 4),
            setup_strength_multiple=round(setup_strength, 4),
            drawdown_ratio=round(drawdown_ratio, 4),
            green_bar_count=green_bar_count,
            setup_close_location=round(setup_close_location, 4),
            preopen_high=preopen_high,
            preopen_low=preopen_low,
            preopen_breakout=preopen_breakout,
            vwap_supported=vwap_supported,
            setup_bar_high=setup_bar.high,
            setup_bar_close=setup_bar.close,
            notes=tuple(notes),
        )

    trigger_price = _round_price(_entry_trigger_price(setup_bars=setup_bars, spec=spec))
    initial_stop = _round_price(min(bar.low for bar in setup_bars[1:]) - spec.tick_size)
    entry_index = _find_entry_index(cash_bars, trigger_price=trigger_price, spec=spec)
    if (
        entry_index is not None
        and spec.require_entry_vwap_reclaim
        and not (cash_bars[entry_index].close > vwap_values[entry_index])
    ):
        entry_index = None
        notes.append("entry_vwap_reclaim_ok")
    if entry_index is None:
        notes.append("trigger_not_hit_in_entry_window")
        return SessionTrade(
            symbol=symbol,
            session_date=session_day.isoformat(),
            qualified=True,
            entered=False,
            trigger_price=trigger_price,
            initial_stop_price=initial_stop,
            entry_bar_number=None,
            entry_end_ts=None,
            entry_price=None,
            exit_bar_number=None,
            exit_end_ts=None,
            exit_price=None,
            exit_reason=None,
            hold_bars=None,
            pnl_points=None,
            gross_pnl_dollars=None,
            net_pnl_points=None,
            net_pnl_dollars=None,
            execution_cost_points=None,
            execution_cost_dollars=None,
            risk_points=None,
            risk_dollars=None,
            gross_r_multiple=None,
            net_r_multiple=None,
            mae_points=None,
            mfe_points=None,
            mae_r_multiple=None,
            mfe_r_multiple=None,
            opening_move_points=round(opening_move_points, 4),
            setup_strength_multiple=round(setup_strength, 4),
            drawdown_ratio=round(drawdown_ratio, 4),
            green_bar_count=green_bar_count,
            setup_close_location=round(setup_close_location, 4),
            preopen_high=preopen_high,
            preopen_low=preopen_low,
            preopen_breakout=preopen_breakout,
            vwap_supported=vwap_supported,
            setup_bar_high=setup_bar.high,
            setup_bar_close=setup_bar.close,
            notes=tuple(notes),
        )

    ema_values = _ema([bar.close for bar in cash_bars], length=spec.exit_ema_length)
    exit_index, exit_price, exit_reason = _find_exit(
        cash_bars=cash_bars,
        ema_values=ema_values,
        entry_index=entry_index,
        initial_stop=initial_stop,
        spec=spec,
    )
    pnl_points = _round_price(exit_price - trigger_price)
    hold_bars = exit_index - entry_index + 1
    economics = _contract_economics(symbol, spec=spec)
    execution_cost_points = _round_price(_execution_cost_points(spec=spec, economics=economics))
    execution_cost_dollars = _round_price(execution_cost_points * economics.point_value_dollars)
    gross_pnl_dollars = _round_price(pnl_points * economics.point_value_dollars)
    net_pnl_points = _round_price(pnl_points - execution_cost_points)
    net_pnl_dollars = _round_price(gross_pnl_dollars - execution_cost_dollars)
    risk_points = _round_price(trigger_price - initial_stop)
    risk_dollars = _round_price(risk_points * economics.point_value_dollars)
    gross_r_multiple = _round_price(pnl_points / risk_points) if risk_points > 0 else None
    net_r_multiple = _round_price(net_pnl_points / risk_points) if risk_points > 0 else None
    trade_window = cash_bars[entry_index : exit_index + 1]
    mae_points = _round_price(max(0.0, trigger_price - min(bar.low for bar in trade_window)))
    mfe_points = _round_price(max(0.0, max(bar.high for bar in trade_window) - trigger_price))
    mae_r_multiple = _round_price(mae_points / risk_points) if risk_points > 0 else None
    mfe_r_multiple = _round_price(mfe_points / risk_points) if risk_points > 0 else None

    return SessionTrade(
        symbol=symbol,
        session_date=session_day.isoformat(),
        qualified=True,
        entered=True,
        trigger_price=trigger_price,
        initial_stop_price=initial_stop,
        entry_bar_number=entry_index + 1,
        entry_end_ts=cash_bars[entry_index].end_ts.isoformat(),
        entry_price=trigger_price,
        exit_bar_number=exit_index + 1,
        exit_end_ts=cash_bars[exit_index].end_ts.isoformat(),
        exit_price=exit_price,
        exit_reason=exit_reason,
        hold_bars=hold_bars,
        pnl_points=pnl_points,
        gross_pnl_dollars=gross_pnl_dollars,
        net_pnl_points=net_pnl_points,
        net_pnl_dollars=net_pnl_dollars,
        execution_cost_points=execution_cost_points,
        execution_cost_dollars=execution_cost_dollars,
        risk_points=risk_points,
        risk_dollars=risk_dollars,
        gross_r_multiple=gross_r_multiple,
        net_r_multiple=net_r_multiple,
        mae_points=mae_points,
        mfe_points=mfe_points,
        mae_r_multiple=mae_r_multiple,
        mfe_r_multiple=mfe_r_multiple,
        opening_move_points=round(opening_move_points, 4),
        setup_strength_multiple=round(setup_strength, 4),
        drawdown_ratio=round(drawdown_ratio, 4),
        green_bar_count=green_bar_count,
        setup_close_location=round(setup_close_location, 4),
        preopen_high=preopen_high,
        preopen_low=preopen_low,
        preopen_breakout=preopen_breakout,
        vwap_supported=vwap_supported,
        setup_bar_high=setup_bar.high,
        setup_bar_close=setup_bar.close,
        notes=tuple(notes),
    )


def _blank_session_trade(*, symbol: str, session_day: date, notes: tuple[str, ...]) -> SessionTrade:
    return SessionTrade(
        symbol=symbol,
        session_date=session_day.isoformat(),
        qualified=False,
        entered=False,
        trigger_price=None,
        initial_stop_price=None,
        entry_bar_number=None,
        entry_end_ts=None,
        entry_price=None,
        exit_bar_number=None,
        exit_end_ts=None,
        exit_price=None,
        exit_reason=None,
        hold_bars=None,
        pnl_points=None,
        gross_pnl_dollars=None,
        net_pnl_points=None,
        net_pnl_dollars=None,
        execution_cost_points=None,
        execution_cost_dollars=None,
        risk_points=None,
        risk_dollars=None,
        gross_r_multiple=None,
        net_r_multiple=None,
        mae_points=None,
        mfe_points=None,
        mae_r_multiple=None,
        mfe_r_multiple=None,
        opening_move_points=None,
        setup_strength_multiple=None,
        drawdown_ratio=None,
        green_bar_count=0,
        setup_close_location=None,
        preopen_high=None,
        preopen_low=None,
        preopen_breakout=False,
        vwap_supported=False,
        setup_bar_high=None,
        setup_bar_close=None,
        notes=notes,
    )


def _find_entry_index(cash_bars: list[ResearchBar], *, trigger_price: float, spec: OpeningDriveSpec) -> int | None:
    start_index = spec.setup_bar_count
    stop_index = min(len(cash_bars), spec.setup_bar_count + spec.entry_lookahead_bars)
    for index in range(start_index, stop_index):
        if cash_bars[index].high >= trigger_price:
            return index
    return None


def _find_exit(
    *,
    cash_bars: list[ResearchBar],
    ema_values: list[float | None],
    entry_index: int,
    initial_stop: float,
    spec: OpeningDriveSpec,
) -> tuple[int, float, str]:
    cutoff_time = _parse_clock(spec.exit_cutoff_time)
    last_index_at_cutoff = entry_index
    for index in range(entry_index + 1, len(cash_bars)):
        local_ts = _local_timestamp(cash_bars[index].end_ts)
        if local_ts.time() > cutoff_time:
            break
        last_index_at_cutoff = index
        if cash_bars[index].low <= initial_stop:
            return index, initial_stop, "initial_stop"
        ema_value = ema_values[index]
        structure_break = (
            cash_bars[index].close < cash_bars[index - 1].low
            or (
                index >= spec.exit_structure_lookback
                and cash_bars[index].close
                < min(bar.low for bar in cash_bars[index - spec.exit_structure_lookback : index])
            )
        )
        if ema_value is not None and cash_bars[index].close < ema_value and structure_break:
            if spec.exit_on_confirmation_next_open and index + 1 < len(cash_bars):
                return index + 1, _round_price(cash_bars[index + 1].open), "ema_and_structure_break_next_open"
            return index, _round_price(cash_bars[index].close), "ema_and_structure_break"
    return last_index_at_cutoff, _round_price(cash_bars[last_index_at_cutoff].close), f"time_stop_{cutoff_time.strftime('%H%M')}"


def _session_vwap(bars: list[ResearchBar]) -> list[float]:
    values: list[float] = []
    cumulative_pv = 0.0
    cumulative_volume = 0.0
    for bar in bars:
        volume = max(float(bar.volume), 1.0)
        cumulative_pv += float(bar.close) * volume
        cumulative_volume += volume
        values.append(cumulative_pv / max(cumulative_volume, 1.0))
    return values


def _drawdown_ratio(bars: list[ResearchBar]) -> float:
    if len(bars) < 2:
        return 0.0
    average_setup_range = statistics.fmean(bar.range_points for bar in bars)
    if len(bars) >= 5:
        focus_bars = bars[2:]
        retracement_pool = bars[3:]
    elif len(bars) >= 3:
        focus_bars = bars[1:]
        retracement_pool = bars[2:]
    else:
        focus_bars = bars
        retracement_pool = bars[1:]
    setup_high = max(bar.high for bar in focus_bars)
    retracement_low = min(bar.low for bar in retracement_pool)
    return max(0.0, (setup_high - retracement_low) / max(average_setup_range, 1e-9))


def _select_preopen_base(preopen_bars: list[ResearchBar], *, session_day: date, spec: OpeningDriveSpec) -> list[ResearchBar]:
    if not preopen_bars:
        return []
    threshold = datetime.combine(session_day, PREOPEN_END, tzinfo=NEW_YORK) - timedelta(
        minutes=spec.preopen_base_lookback_minutes
    )
    narrowed = [bar for bar in preopen_bars if _local_timestamp(bar.end_ts) >= threshold]
    return narrowed or preopen_bars


def _ema(values: list[float], *, length: int) -> list[float | None]:
    if length <= 1:
        return [float(value) for value in values]
    alpha = 2.0 / (length + 1.0)
    output: list[float | None] = [None] * len(values)
    ema_value: float | None = None
    for index, value in enumerate(values):
        if ema_value is None:
            ema_value = float(value)
        else:
            ema_value = alpha * float(value) + (1.0 - alpha) * ema_value
        output[index] = ema_value
    return output


def _summarize_trades(rows: list[SessionTrade]) -> dict[str, Any]:
    qualified = [row for row in rows if row.qualified]
    entered = [row for row in qualified if row.entered and row.pnl_points is not None]
    gross_winners = [row for row in entered if (row.pnl_points or 0.0) > 0]
    gross_losers = [row for row in entered if (row.pnl_points or 0.0) <= 0]
    net_winners = [row for row in entered if (row.net_pnl_points or 0.0) > 0]
    net_losers = [row for row in entered if (row.net_pnl_points or 0.0) <= 0]
    gross_profit = sum((row.pnl_points or 0.0) for row in gross_winners)
    gross_loss = abs(sum((row.pnl_points or 0.0) for row in gross_losers))
    net_profit = sum((row.net_pnl_points or 0.0) for row in net_winners)
    net_loss = abs(sum((row.net_pnl_points or 0.0) for row in net_losers))
    return {
        "qualified_session_count": len(qualified),
        "entered_trade_count": len(entered),
        "gross_win_rate": round(len(gross_winners) / len(entered), 4) if entered else None,
        "net_win_rate": round(len(net_winners) / len(entered), 4) if entered else None,
        "average_pnl_points": round(statistics.fmean(row.pnl_points or 0.0 for row in entered), 4) if entered else None,
        "average_net_pnl_points": round(statistics.fmean(row.net_pnl_points or 0.0 for row in entered), 4) if entered else None,
        "average_gross_pnl_dollars": round(statistics.fmean(row.gross_pnl_dollars or 0.0 for row in entered), 4)
        if entered
        else None,
        "average_net_pnl_dollars": round(statistics.fmean(row.net_pnl_dollars or 0.0 for row in entered), 4)
        if entered
        else None,
        "median_pnl_points": round(statistics.median(row.pnl_points or 0.0 for row in entered), 4) if entered else None,
        "median_net_pnl_points": round(statistics.median(row.net_pnl_points or 0.0 for row in entered), 4)
        if entered
        else None,
        "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
        "net_profit_factor": round(net_profit / net_loss, 4) if net_loss > 0 else None,
        "average_gross_r_multiple": round(statistics.fmean(row.gross_r_multiple or 0.0 for row in entered), 4)
        if entered
        else None,
        "average_net_r_multiple": round(statistics.fmean(row.net_r_multiple or 0.0 for row in entered), 4)
        if entered
        else None,
        "average_mae_points": round(statistics.fmean(row.mae_points or 0.0 for row in entered), 4) if entered else None,
        "average_mfe_points": round(statistics.fmean(row.mfe_points or 0.0 for row in entered), 4) if entered else None,
        "average_mae_r_multiple": round(statistics.fmean(row.mae_r_multiple or 0.0 for row in entered), 4)
        if entered
        else None,
        "average_mfe_r_multiple": round(statistics.fmean(row.mfe_r_multiple or 0.0 for row in entered), 4)
        if entered
        else None,
        "average_execution_cost_points": round(
            statistics.fmean(row.execution_cost_points or 0.0 for row in entered),
            4,
        )
        if entered
        else None,
        "average_execution_cost_dollars": round(
            statistics.fmean(row.execution_cost_dollars or 0.0 for row in entered),
            4,
        )
        if entered
        else None,
        "best_trade_points": max((row.pnl_points or 0.0) for row in entered) if entered else None,
        "worst_trade_points": min((row.pnl_points or 0.0) for row in entered) if entered else None,
        "best_net_trade_points": max((row.net_pnl_points or 0.0) for row in entered) if entered else None,
        "worst_net_trade_points": min((row.net_pnl_points or 0.0) for row in entered) if entered else None,
    }


def _cross_symbol_summary(symbol_reports: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for symbol, payload in symbol_reports.items():
        summary[symbol] = payload["trade_summary"]
    if {"ES", "MES"}.issubset(symbol_reports):
        es_highlight = next(
            (row for row in symbol_reports["ES"]["sessions"] if row["session_date"] == "2026-04-17"),
            None,
        )
        mes_highlight = next(
            (row for row in symbol_reports["MES"]["sessions"] if row["session_date"] == "2026-04-17"),
            None,
        )
        summary["es_mes_alignment_2026_04_17"] = {
            "es_entered": es_highlight["entered"] if es_highlight else None,
            "mes_entered": mes_highlight["entered"] if mes_highlight else None,
            "es_exit_bar": es_highlight["exit_bar_number"] if es_highlight else None,
            "mes_exit_bar": mes_highlight["exit_bar_number"] if mes_highlight else None,
        }
    return summary


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# ES/MES Opening Drive Continuation Research",
        "",
        "## Rule Frame",
        "",
        "- Use canonical `1m` as truth and derive `5m` decisions from it.",
        "- Require a strong first five-bar cash-open drive that closes above the pre-open base.",
        "- Enter on the first pause-high breakout in bars 6-8.",
        "- Exit on the first `EMA(5)` plus structure failure, otherwise flatten by noon.",
        "",
        "## Highlighted Session",
        "",
        f"- Highlight date: `{report['highlighted_session_date']}`",
    ]
    for symbol, row in report["highlighted_sessions"].items():
        if row is None:
            lines.append(f"- {symbol}: no session row found")
            continue
        lines.append(
            f"- {symbol}: qualified={row['qualified']}, entered={row['entered']}, "
            f"entry_bar={row['entry_bar_number']}, exit_bar={row['exit_bar_number']}, "
            f"gross_points={row['pnl_points']}, net_points={row['net_pnl_points']}, exit_reason={row['exit_reason']}"
        )
    lines.extend(["", "## Symbol Summary", ""])
    for symbol, payload in report["symbol_reports"].items():
        summary = payload["trade_summary"]
        lines.append(
            f"- {symbol}: sessions={payload['session_count']}, qualified={summary['qualified_session_count']}, "
            f"trades={summary['entered_trade_count']}, gross_win_rate={summary['gross_win_rate']}, "
            f"net_win_rate={summary['net_win_rate']}, avg_net_pnl={summary['average_net_pnl_points']}"
        )
    return "\n".join(lines)


def _resolve_sqlite_database_path(database_url: str) -> Path:
    if not database_url.startswith("sqlite:///"):
        raise ValueError(f"Expected a sqlite database URL, received: {database_url}")
    return Path(database_url.removeprefix("sqlite:///")).resolve(strict=False)


def _parse_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _day_start(value: date) -> datetime:
    return datetime.combine(value, time.min, tzinfo=NEW_YORK)


def _day_end(value: date) -> datetime:
    return datetime.combine(value, time.max, tzinfo=NEW_YORK)


def _parse_clock(value: str) -> time:
    return time.fromisoformat(value)


def _local_timestamp(value: datetime) -> datetime:
    return value.astimezone(NEW_YORK)


def _round_price(value: float) -> float:
    if not math.isfinite(value):
        return value
    return round(value, 4)


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def build_variant_spec(
    *,
    variant: str,
    slippage_ticks_per_side: float,
    es_round_turn_commission_dollars: float,
    mes_round_turn_commission_dollars: float,
) -> OpeningDriveSpec:
    common = {
        "slippage_ticks_per_side": slippage_ticks_per_side,
        "es_round_turn_commission_dollars": es_round_turn_commission_dollars,
        "mes_round_turn_commission_dollars": mes_round_turn_commission_dollars,
    }
    if variant == "v2":
        return OpeningDriveSpec(
            variant_id="opening_drive_trend_capture_v2",
            setup_bar_count=3,
            entry_lookahead_bars=2,
            min_green_bars_in_setup=2,
            min_setup_strength_multiple=0.9,
            min_setup_close_location=0.45,
            require_setup_bar_breakout=False,
            entry_trigger_mode="setup_range_high",
            exit_on_confirmation_next_open=True,
            require_preopen_breakout=False,
            require_vwap_support=False,
            **common,
        )
    if variant == "v3":
        return OpeningDriveSpec(
            variant_id="opening_drive_trend_capture_v3_a_plus",
            setup_bar_count=4,
            entry_lookahead_bars=2,
            min_green_bars_in_setup=2,
            min_setup_strength_multiple=1.1,
            min_setup_close_location=0.5,
            require_setup_bar_breakout=False,
            entry_trigger_mode="setup_range_high",
            require_last_setup_higher_low=True,
            require_last_setup_close_above_first_close=False,
            require_entry_vwap_reclaim=True,
            exit_on_confirmation_next_open=True,
            require_preopen_breakout=False,
            require_vwap_support=False,
            **common,
        )
    return OpeningDriveSpec(**common)


def _entry_trigger_price(*, setup_bars: list[ResearchBar], spec: OpeningDriveSpec) -> float:
    if spec.entry_trigger_mode == "setup_range_high":
        return max(bar.high for bar in setup_bars) + spec.tick_size
    return setup_bars[-1].high + spec.tick_size


def _contract_economics(symbol: str, *, spec: OpeningDriveSpec) -> ContractEconomics:
    normalized = str(symbol).strip().upper()
    if normalized == "MES":
        return ContractEconomics(
            symbol=normalized,
            point_value_dollars=5.0,
            round_turn_commission_dollars=spec.mes_round_turn_commission_dollars,
        )
    return ContractEconomics(
        symbol=normalized,
        point_value_dollars=50.0,
        round_turn_commission_dollars=spec.es_round_turn_commission_dollars,
    )


def _execution_cost_points(*, spec: OpeningDriveSpec, economics: ContractEconomics) -> float:
    slippage_points = 2.0 * spec.slippage_ticks_per_side * spec.tick_size
    commission_points = economics.round_turn_commission_dollars / economics.point_value_dollars
    return slippage_points + commission_points


def _iter_processing_windows(*, start_day: date | None, end_day: date | None) -> list[tuple[date | None, date | None]]:
    if start_day is None or end_day is None:
        return [(start_day, end_day)]
    windows: list[tuple[date, date]] = []
    cursor = date(start_day.year, start_day.month, 1)
    if cursor < start_day:
        cursor = start_day
    while cursor <= end_day:
        month_end = _month_end(cursor)
        window_start = max(cursor, start_day)
        window_end = min(month_end, end_day)
        windows.append((window_start, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def _month_end(value: date) -> date:
    if value.month == 12:
        return date(value.year, 12, 31)
    return date(value.year, value.month + 1, 1) - timedelta(days=1)
