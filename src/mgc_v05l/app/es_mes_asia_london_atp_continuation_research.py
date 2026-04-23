"""ES/MES overnight ATP-continuation research across Asia to London.

This branch reuses the repo's ATP source-of-truth bias/pullback/reassertion stack
for entry readiness, then holds the resulting overnight position through the
existing Asia->London participation frame and exits during London late.
"""

from __future__ import annotations

import argparse
import json
import statistics
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ..config_models import load_settings_from_files
from ..research.trend_participation.features import build_feature_states
from ..research.trend_participation.models import ResearchBar
from ..research.trend_participation.phase2_continuation import classify_entry_states
from ..research.trend_participation.storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m
from .asia_london_participation_research import (
    DEFAULT_CONFIG_PATHS,
    HOLD_SEGMENTS,
    POINT_VALUES,
    ROUND_TURN_COMMISSION_DOLLARS,
    TICK_SIZES,
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
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "es_mes_asia_london_atp_continuation_research"
DEFAULT_SYMBOLS: tuple[str, ...] = ("ES", "MES")
ENTRY_SEGMENT = "ASIA_EARLY"


@dataclass(frozen=True)
class AtpContinuationTrade:
    symbol: str
    trade_date: str
    side: str
    variant_key: str
    entered: bool
    entry_reason: str
    decision_end_ts: str | None
    entry_end_ts: str | None
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
    parser = argparse.ArgumentParser(prog="es-mes-asia-london-atp-continuation-research")
    parser.add_argument("--symbol", action="append", default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--config", action="append", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_es_mes_asia_london_atp_continuation_research(
        symbols=args.symbol or list(DEFAULT_SYMBOLS),
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        config_paths=args.config,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_es_mes_asia_london_atp_continuation_research(
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
        bars_5m = resample_bars_from_1m(bars_1m=normalized_1m, target_timeframe="5m")
        feature_rows = build_feature_states(bars_5m=bars_5m, bars_1m=normalized_1m)
        entry_states_long = classify_entry_states(feature_rows=feature_rows, allowed_sessions=frozenset({ENTRY_SEGMENT}), side="LONG")
        entry_states_short = classify_entry_states(feature_rows=feature_rows, allowed_sessions=frozenset({ENTRY_SEGMENT}), side="SHORT")
        contexts = _build_overnight_session_contexts(
            decision_bars=bars_5m,
            symbol=symbol,
            start_day=start_day,
            end_day=end_day,
        )
        long_rows = [
            _evaluate_atp_continuation_trade(
                symbol=symbol,
                side="LONG",
                context=context,
                entry_states=entry_states_long,
            )
            for context in contexts
        ]
        short_rows = [
            _evaluate_atp_continuation_trade(
                symbol=symbol,
                side="SHORT",
                context=context,
                entry_states=entry_states_short,
            )
            for context in contexts
        ]
        symbol_reports[symbol] = {
            "bar_count_1m": len(normalized_1m),
            "bar_count_5m": len(bars_5m),
            "context_count": len(contexts),
            "data_quality_issues": [asdict(issue) for issue in quality_issues],
            "variants": {
                "LONG__atp_continuation_overnight": {
                    "trades": [asdict(row) for row in long_rows],
                    "trade_summary": _trade_summary(long_rows),
                },
                "SHORT__atp_continuation_overnight": {
                    "trades": [asdict(row) for row in short_rows],
                    "trade_summary": _trade_summary(short_rows),
                },
            },
        }

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "mode": "es_mes_asia_london_atp_continuation_research",
        "symbols": list(resolved_symbols),
        "window": {
            "start_date": start_day.isoformat() if start_day else None,
            "end_date": end_day.isoformat() if end_day else None,
            "entry_segment": ENTRY_SEGMENT,
            "hold_segments": list(HOLD_SEGMENTS),
        },
        "database_path": str(sqlite_path),
        "symbol_reports": symbol_reports,
        "pair_rankings": _pair_rankings(symbol_reports=symbol_reports),
    }
    json_path = resolved_output_dir / "es_mes_asia_london_atp_continuation_research.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return {
        "mode": payload["mode"],
        "artifact_paths": {"json": str(json_path)},
        "pair_rankings": payload["pair_rankings"],
    }


def _evaluate_atp_continuation_trade(
    *,
    symbol: str,
    side: str,
    context: Any,
    entry_states: list[Any],
) -> AtpContinuationTrade:
    state = next(
        (
            candidate
            for candidate in entry_states
            if candidate.session_date == context.trade_date
            and candidate.entry_eligible
        ),
        None,
    )
    if state is None:
        return _empty_trade(symbol=symbol, trade_day=context.trade_date, side=side, variant_key=f"{side}__atp_continuation_overnight", note="no_eligible_atp_continuation")

    hold_bars = list(context.hold_bars)
    decision_index = next((index for index, bar in enumerate(hold_bars) if bar.end_ts == state.decision_ts), None)
    if decision_index is None or decision_index + 1 >= len(hold_bars):
        return _empty_trade(symbol=symbol, trade_day=context.trade_date, side=side, variant_key=f"{side}__atp_continuation_overnight", note="decision_bar_missing")

    entry_index = decision_index + 1
    entry_bar = hold_bars[entry_index]
    entry_price = round(float(entry_bar.open), 4)
    prior_window = hold_bars[max(0, entry_index - 2) : entry_index + 1]
    if side == "LONG":
        stop_price = round(min(float(bar.low) for bar in prior_window) - TICK_SIZES[symbol], 4)
        ema_values = _hold_ema([float(bar.close) for bar in hold_bars], length=5)
        spec = ForcedSegmentLongSpec(
            variant_id="es_mes_atp_continuation_overnight_long",
            description="ES/MES ATP continuation overnight long",
            tick_size=TICK_SIZES[symbol],
            fallback_entry_bar=8,
            exit_ema_length=5,
        )
        exit_index, exit_price, exit_reason = _find_long_exit(
            segment_bars=hold_bars,
            entry_index=entry_index,
            stop_price=stop_price,
            ema_values=ema_values,
            spec=spec,
        )
        pnl_points = round(float(exit_price) - entry_price, 4)
        trade_window = hold_bars[entry_index : exit_index + 1]
        max_drawdown = round(max(0.0, entry_price - min(float(bar.low) for bar in trade_window)), 4)
    else:
        stop_price = round(max(float(bar.high) for bar in prior_window) + TICK_SIZES[symbol], 4)
        ema_values = _hold_ema([float(bar.close) for bar in hold_bars], length=5)
        spec = NyEarlyShortSpec(
            variant_id="es_mes_atp_continuation_overnight_short",
            description="ES/MES ATP continuation overnight short",
            setup_family="es_mes_atp_continuation",
            tick_size=TICK_SIZES[symbol],
            exit_ema_length=5,
        )
        exit_index, exit_price, exit_reason = _find_short_exit(
            segment_bars=hold_bars,
            entry_index=entry_index,
            stop_price=stop_price,
            ema_values=ema_values,
            spec=spec,
        )
        pnl_points = round(entry_price - float(exit_price), 4)
        trade_window = hold_bars[entry_index : exit_index + 1]
        max_drawdown = round(max(0.0, max(float(bar.high) for bar in trade_window) - entry_price), 4)

    net_pnl_points = round(
        pnl_points - _execution_cost_points(symbol=symbol, tick_size=TICK_SIZES[symbol], slippage_ticks_per_side=1.0),
        4,
    )
    exit_bar = hold_bars[exit_index]
    return AtpContinuationTrade(
        symbol=symbol,
        trade_date=context.trade_date.isoformat(),
        side=side,
        variant_key=f"{side}__atp_continuation_overnight",
        entered=True,
        entry_reason="atp_phase2_continuation_confirmed",
        decision_end_ts=state.decision_ts.isoformat(),
        entry_end_ts=entry_bar.end_ts.isoformat(),
        entry_price=entry_price,
        stop_price=stop_price,
        exit_end_ts=exit_bar.end_ts.isoformat(),
        exit_price=round(float(exit_price), 4),
        exit_reason=str(exit_reason),
        pnl_points=pnl_points,
        net_pnl_points=net_pnl_points,
        max_drawdown_points=max_drawdown,
        notes=tuple(getattr(state, "feature_snapshot", {}).get("bias_reasons", [])) if isinstance(getattr(state, "feature_snapshot", {}), dict) else (),
    )


def _empty_trade(*, symbol: str, trade_day: date, side: str, variant_key: str, note: str) -> AtpContinuationTrade:
    return AtpContinuationTrade(
        symbol=symbol,
        trade_date=trade_day.isoformat(),
        side=side,
        variant_key=variant_key,
        entered=False,
        entry_reason=note,
        decision_end_ts=None,
        entry_end_ts=None,
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


def _trade_summary(rows: list[AtpContinuationTrade]) -> dict[str, Any]:
    entered = [row for row in rows if row.entered and row.net_pnl_points is not None]
    if not entered:
        return {
            "average_net_pnl_points": 0.0,
            "entered_trade_count": 0,
            "max_drawdown_points": 0.0,
            "median_net_pnl_points": 0.0,
            "net_profit_factor": 0.0,
            "net_win_rate": 0.0,
        }
    net_points = [float(row.net_pnl_points) for row in entered if row.net_pnl_points is not None]
    positive = [value for value in net_points if value > 0]
    negative = [value for value in net_points if value < 0]
    gross_profit = sum(positive)
    gross_loss = abs(sum(negative))
    return {
        "average_net_pnl_points": round(statistics.fmean(net_points), 4),
        "entered_trade_count": len(entered),
        "max_drawdown_points": round(max(float(row.max_drawdown_points or 0.0) for row in entered), 4),
        "median_net_pnl_points": round(statistics.median(net_points), 4),
        "net_profit_factor": round((gross_profit / gross_loss) if gross_loss > 0 else gross_profit, 4),
        "net_win_rate": round(sum(1 for value in net_points if value > 0) / len(net_points), 4),
    }


def _pair_rankings(*, symbol_reports: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    pair_rows: list[dict[str, Any]] = []
    for variant_key in ("LONG__atp_continuation_overnight", "SHORT__atp_continuation_overnight"):
        summaries = []
        for symbol in ("ES", "MES"):
            report = symbol_reports.get(symbol)
            if not report:
                continue
            summaries.append(report["variants"][variant_key]["trade_summary"])
        if len(summaries) != 2:
            continue
        pair_rows.append(
            {
                "variant_key": variant_key,
                "min_average_net_pnl_points": round(min(float(item["average_net_pnl_points"]) for item in summaries), 4),
                "min_net_profit_factor": round(min(float(item["net_profit_factor"]) for item in summaries), 4),
                "min_entered_trade_count": min(int(item["entered_trade_count"]) for item in summaries),
                "max_pair_drawdown_points": round(max(float(item["max_drawdown_points"]) for item in summaries), 4),
            }
        )
    pair_rows.sort(key=lambda row: (row["min_average_net_pnl_points"], row["min_net_profit_factor"]), reverse=True)
    return {"ES_MES": pair_rows}


if __name__ == "__main__":
    raise SystemExit(main())
