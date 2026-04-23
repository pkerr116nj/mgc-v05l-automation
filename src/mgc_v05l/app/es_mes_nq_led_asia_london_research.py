"""ES/MES overnight follower research led by NQ/MNQ Asia-to-London winners.

This study tests the highest-confidence rescue path for ES/MES overnight coverage:
- use the already-positive NQ/MNQ overnight participation variants as the leader
- trigger ES from NQ and MES from MNQ on the same trade date and entry timestamp
- manage risk and exits using the follower instrument's own bars
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
from ..research.trend_participation.models import ResearchBar
from ..research.trend_participation.storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m
from .asia_london_participation_research import (
    DEFAULT_CONFIG_PATHS,
    HOLD_SEGMENTS,
    PAIR_GROUPS,
    TICK_SIZES,
    _build_overnight_session_contexts,
    _evaluate_long_participation,
    _evaluate_short_participation,
    _execution_cost_points,
    _load_end_timestamp,
    _load_start_timestamp,
    _parse_date,
    _resolve_sqlite_database_path,
)
from .gc_mgc_london_late_long_research import _ema as _hold_ema
from .gc_mgc_london_late_long_research import _find_exit as _find_long_exit
from .gc_mgc_ny_early_short_research import NyEarlyShortSpec, _find_exit as _find_short_exit
from .gc_mgc_segment_forced_session_long_research import (
    ForcedSegmentLongSpec,
    build_variant_specs as build_long_variant_specs,
)
from .gc_mgc_segment_forced_session_short_research import (
    ForcedSegmentShortSpec,
    build_variant_specs as build_short_variant_specs,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "es_mes_nq_led_asia_london_research_v1"
DEFAULT_SYMBOLS: tuple[str, ...] = ("ES", "MES")
LEADER_BY_FOLLOWER = {"ES": "NQ", "MES": "MNQ"}


@dataclass(frozen=True)
class FollowerTrade:
    symbol: str
    leader_symbol: str
    trade_date: str
    side: str
    leader_variant_id: str
    entered: bool
    entry_reason: str
    leader_entry_reason: str | None
    leader_entry_end_ts: str | None
    follower_entry_end_ts: str | None
    follower_entry_price: float | None
    stop_price: float | None
    exit_end_ts: str | None
    exit_price: float | None
    exit_reason: str | None
    pnl_points: float | None
    net_pnl_points: float | None
    gross_r_multiple: float | None
    max_drawdown_points: float | None
    notes: tuple[str, ...] = ()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="es-mes-nq-led-asia-london-research")
    parser.add_argument("--symbol", action="append", default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--config", action="append", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_es_mes_nq_led_asia_london_research(
        symbols=args.symbol or list(DEFAULT_SYMBOLS),
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        config_paths=args.config,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_es_mes_nq_led_asia_london_research(
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

    long_specs_by_symbol = _leader_long_specs()
    short_specs_by_symbol = _leader_short_specs()
    leader_contexts_by_symbol = _load_contexts(
        sqlite_path=sqlite_path,
        symbols=tuple(sorted(set(LEADER_BY_FOLLOWER[symbol] for symbol in resolved_symbols))),
        start_day=start_day,
        end_day=end_day,
    )
    follower_contexts_by_symbol = _load_contexts(
        sqlite_path=sqlite_path,
        symbols=resolved_symbols,
        start_day=start_day,
        end_day=end_day,
    )

    symbol_reports: dict[str, Any] = {}
    for symbol in resolved_symbols:
        leader_symbol = LEADER_BY_FOLLOWER[symbol]
        leader_contexts = leader_contexts_by_symbol[leader_symbol]
        follower_contexts = follower_contexts_by_symbol[symbol]
        leader_context_by_day = {context.trade_date: context for context in leader_contexts["contexts"]}
        follower_context_by_day = {context.trade_date: context for context in follower_contexts["contexts"]}
        trade_days = sorted(set(leader_context_by_day) & set(follower_context_by_day))

        variants: dict[str, Any] = {}
        for spec in long_specs_by_symbol[leader_symbol]:
            rows = [
                _evaluate_follower_trade(
                    follower_symbol=symbol,
                    leader_symbol=leader_symbol,
                    side="LONG",
                    leader_context=leader_context_by_day[trade_day],
                    follower_context=follower_context_by_day[trade_day],
                    leader_spec=spec,
                )
                for trade_day in trade_days
            ]
            variants[f"LONG__{spec.variant_id}"] = {
                "leader_symbol": leader_symbol,
                "leader_variant_id": spec.variant_id,
                "trade_summary": _summarize_follower_trades(rows),
                "sessions": [asdict(row) for row in rows],
            }
        for spec in short_specs_by_symbol[leader_symbol]:
            rows = [
                _evaluate_follower_trade(
                    follower_symbol=symbol,
                    leader_symbol=leader_symbol,
                    side="SHORT",
                    leader_context=leader_context_by_day[trade_day],
                    follower_context=follower_context_by_day[trade_day],
                    leader_spec=spec,
                )
                for trade_day in trade_days
            ]
            variants[f"SHORT__{spec.variant_id}"] = {
                "leader_symbol": leader_symbol,
                "leader_variant_id": spec.variant_id,
                "trade_summary": _summarize_follower_trades(rows),
                "sessions": [asdict(row) for row in rows],
            }
        symbol_reports[symbol] = {
            "leader_symbol": leader_symbol,
            "context_count": len(trade_days),
            "follower_bar_count_1m": follower_contexts["bar_count_1m"],
            "follower_bar_count_3m": follower_contexts["bar_count_3m"],
            "leader_bar_count_1m": leader_contexts["bar_count_1m"],
            "leader_bar_count_3m": leader_contexts["bar_count_3m"],
            "follower_data_quality_issues": [asdict(issue) for issue in follower_contexts["quality_issues"]],
            "leader_data_quality_issues": [asdict(issue) for issue in leader_contexts["quality_issues"]],
            "variants": variants,
        }

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "es_mes_nq_led_asia_london_research",
        "definition": {
            "symbols": list(resolved_symbols),
            "leader_map": LEADER_BY_FOLLOWER,
            "hold_segments": list(HOLD_SEGMENTS),
            "description": "Trade ES from NQ and MES from MNQ when the overnight Asia-to-London leader variants fire, then manage exits on the follower instrument's own bars.",
            "start_date": start_day.isoformat() if start_day else None,
            "end_date": end_day.isoformat() if end_day else None,
        },
        "database_path": str(sqlite_path),
        "symbol_reports": symbol_reports,
        "pair_rankings": _pair_rankings(symbol_reports=symbol_reports),
    }
    json_path = resolved_output_dir / "es_mes_nq_led_asia_london_research.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return {
        "mode": payload["study_id"],
        "artifact_paths": {"json": str(json_path)},
        "pair_rankings": payload["pair_rankings"],
    }


def _load_contexts(
    *,
    sqlite_path: Path,
    symbols: tuple[str, ...],
    start_day: date | None,
    end_day: date | None,
) -> dict[str, dict[str, Any]]:
    reports: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
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
        reports[symbol] = {
            "bar_count_1m": len(normalized_1m),
            "bar_count_3m": len(bars_3m),
            "quality_issues": quality_issues,
            "contexts": contexts,
        }
    return reports


def _leader_long_specs() -> dict[str, tuple[ForcedSegmentLongSpec, ...]]:
    leader_ids = ("segment_forced_long_v6_contextual_fallback", "segment_forced_long_v5_dip_reclaim_or_bar8")
    return {
        "NQ": tuple(
            spec for spec in build_long_variant_specs(selected_ids=list(leader_ids)) if spec.decision_timeframe == "3m"
        ),
        "MNQ": tuple(
            spec for spec in build_long_variant_specs(selected_ids=list(leader_ids)) if spec.decision_timeframe == "3m"
        ),
    }


def _leader_short_specs() -> dict[str, tuple[ForcedSegmentShortSpec, ...]]:
    leader_ids = ("segment_forced_short_v2_reclaim_fail_or_bar7",)
    return {
        "NQ": tuple(
            spec for spec in build_short_variant_specs(selected_ids=list(leader_ids)) if spec.decision_timeframe == "3m"
        ),
        "MNQ": tuple(
            spec for spec in build_short_variant_specs(selected_ids=list(leader_ids)) if spec.decision_timeframe == "3m"
        ),
    }


def _evaluate_follower_trade(
    *,
    follower_symbol: str,
    leader_symbol: str,
    side: str,
    leader_context: Any,
    follower_context: Any,
    leader_spec: ForcedSegmentLongSpec | ForcedSegmentShortSpec,
) -> FollowerTrade:
    if side == "LONG":
        leader_trade = _evaluate_long_participation(symbol=leader_symbol, context=leader_context, spec=leader_spec)
    else:
        leader_trade = _evaluate_short_participation(symbol=leader_symbol, context=leader_context, spec=leader_spec)
    if not leader_trade.entered:
        return _empty_follower_trade(
            follower_symbol=follower_symbol,
            leader_symbol=leader_symbol,
            trade_day=follower_context.trade_date,
            side=side,
            leader_variant_id=leader_spec.variant_id,
            reason="leader_no_entry",
            leader_entry_reason=leader_trade.entry_reason,
            leader_entry_end_ts=leader_trade.entry_end_ts,
        )

    entry_ts = datetime.fromisoformat(str(leader_trade.entry_end_ts))
    follower_hold_bars = list(follower_context.hold_bars)
    follower_entry_bars = list(follower_context.entry_segment_bars)
    entry_index = next((index for index, bar in enumerate(follower_hold_bars) if bar.end_ts == entry_ts), None)
    if entry_index is None:
        entry_index = next((index for index, bar in enumerate(follower_hold_bars) if bar.end_ts >= entry_ts), None)
    if entry_index is None or entry_index >= len(follower_hold_bars):
        return _empty_follower_trade(
            follower_symbol=follower_symbol,
            leader_symbol=leader_symbol,
            trade_day=follower_context.trade_date,
            side=side,
            leader_variant_id=leader_spec.variant_id,
            reason="follower_entry_bar_missing",
            leader_entry_reason=leader_trade.entry_reason,
            leader_entry_end_ts=leader_trade.entry_end_ts,
        )
    entry_bar = follower_hold_bars[entry_index]
    if entry_bar not in follower_entry_bars:
        return _empty_follower_trade(
            follower_symbol=follower_symbol,
            leader_symbol=leader_symbol,
            trade_day=follower_context.trade_date,
            side=side,
            leader_variant_id=leader_spec.variant_id,
            reason="follower_outside_entry_segment",
            leader_entry_reason=leader_trade.entry_reason,
            leader_entry_end_ts=leader_trade.entry_end_ts,
        )

    setup_count = int(getattr(leader_spec, "setup_bar_count", 4))
    setup_bars = follower_entry_bars[:setup_count]
    if len(setup_bars) < setup_count:
        return _empty_follower_trade(
            follower_symbol=follower_symbol,
            leader_symbol=leader_symbol,
            trade_day=follower_context.trade_date,
            side=side,
            leader_variant_id=leader_spec.variant_id,
            reason="insufficient_follower_setup_bars",
            leader_entry_reason=leader_trade.entry_reason,
            leader_entry_end_ts=leader_trade.entry_end_ts,
        )

    entry_price = round(float(entry_bar.open), 4)
    tick_size = TICK_SIZES[follower_symbol]
    if side == "LONG":
        stop_price = round(min(float(bar.low) for bar in setup_bars) - tick_size, 4)
        risk_points = round(max(entry_price - stop_price, tick_size), 4)
        exit_spec = ForcedSegmentLongSpec(
            variant_id=f"{leader_spec.variant_id}__follower_exit",
            description=f"{follower_symbol} follower long exit",
            decision_timeframe=getattr(leader_spec, "decision_timeframe", "3m"),
            setup_bar_count=setup_count,
            fallback_entry_bar=getattr(leader_spec, "fallback_entry_bar", 8),
            tick_size=tick_size,
            slippage_ticks_per_side=getattr(leader_spec, "slippage_ticks_per_side", 1.0),
            gc_round_turn_commission_dollars=getattr(leader_spec, "gc_round_turn_commission_dollars", 4.5),
            mgc_round_turn_commission_dollars=getattr(leader_spec, "mgc_round_turn_commission_dollars", 1.5),
            exit_mode=getattr(leader_spec, "exit_mode", "ema_structure"),
            exit_ema_length=getattr(leader_spec, "exit_ema_length", 5),
        )
        ema_values = _hold_ema([bar.close for bar in follower_hold_bars], length=exit_spec.exit_ema_length)
        exit_index, exit_price, exit_reason = _find_long_exit(
            segment_bars=follower_hold_bars,
            entry_index=entry_index,
            stop_price=stop_price,
            ema_values=ema_values,
            spec=exit_spec,
        )
        pnl_points = round(float(exit_price) - entry_price, 4)
        trade_window = follower_hold_bars[entry_index : exit_index + 1]
        max_drawdown = round(max(0.0, entry_price - min(float(bar.low) for bar in trade_window)), 4)
    else:
        stop_price = round(max(float(bar.high) for bar in setup_bars) + tick_size, 4)
        risk_points = round(max(stop_price - entry_price, tick_size), 4)
        exit_spec = NyEarlyShortSpec(
            variant_id=f"{leader_spec.variant_id}__follower_exit",
            description=f"{follower_symbol} follower short exit",
            setup_family="nq_led_es_mes_follower",
            decision_timeframe=getattr(leader_spec, "decision_timeframe", "3m"),
            exit_mode=getattr(leader_spec, "exit_mode", "ema_reclaim"),
            exit_ema_length=getattr(leader_spec, "exit_ema_length", 5),
            tick_size=tick_size,
            slippage_ticks_per_side=getattr(leader_spec, "slippage_ticks_per_side", 1.0),
            gc_round_turn_commission_dollars=getattr(leader_spec, "gc_round_turn_commission_dollars", 4.5),
            mgc_round_turn_commission_dollars=getattr(leader_spec, "mgc_round_turn_commission_dollars", 1.5),
        )
        ema_values = _hold_ema([bar.close for bar in follower_hold_bars], length=exit_spec.exit_ema_length)
        exit_index, exit_price, exit_reason = _find_short_exit(
            segment_bars=follower_hold_bars,
            entry_index=entry_index,
            stop_price=stop_price,
            ema_values=ema_values,
            spec=exit_spec,
        )
        pnl_points = round(entry_price - float(exit_price), 4)
        trade_window = follower_hold_bars[entry_index : exit_index + 1]
        max_drawdown = round(max(0.0, max(float(bar.high) for bar in trade_window) - entry_price), 4)

    net_pnl_points = round(
        pnl_points - _execution_cost_points(symbol=follower_symbol, tick_size=tick_size, slippage_ticks_per_side=getattr(leader_spec, "slippage_ticks_per_side", 1.0)),
        4,
    )
    gross_r = round(pnl_points / risk_points, 4) if risk_points > 0 else None
    exit_bar = follower_hold_bars[exit_index]
    return FollowerTrade(
        symbol=follower_symbol,
        leader_symbol=leader_symbol,
        trade_date=follower_context.trade_date.isoformat(),
        side=side,
        leader_variant_id=leader_spec.variant_id,
        entered=True,
        entry_reason="followed_leader_entry",
        leader_entry_reason=leader_trade.entry_reason,
        leader_entry_end_ts=leader_trade.entry_end_ts,
        follower_entry_end_ts=entry_bar.end_ts.isoformat(),
        follower_entry_price=entry_price,
        stop_price=stop_price,
        exit_end_ts=exit_bar.end_ts.isoformat(),
        exit_price=round(float(exit_price), 4),
        exit_reason=exit_reason,
        pnl_points=pnl_points,
        net_pnl_points=net_pnl_points,
        gross_r_multiple=gross_r,
        max_drawdown_points=max_drawdown,
        notes=(),
    )


def _empty_follower_trade(
    *,
    follower_symbol: str,
    leader_symbol: str,
    trade_day: date,
    side: str,
    leader_variant_id: str,
    reason: str,
    leader_entry_reason: str | None,
    leader_entry_end_ts: str | None,
) -> FollowerTrade:
    return FollowerTrade(
        symbol=follower_symbol,
        leader_symbol=leader_symbol,
        trade_date=trade_day.isoformat(),
        side=side,
        leader_variant_id=leader_variant_id,
        entered=False,
        entry_reason=reason,
        leader_entry_reason=leader_entry_reason,
        leader_entry_end_ts=leader_entry_end_ts,
        follower_entry_end_ts=None,
        follower_entry_price=None,
        stop_price=None,
        exit_end_ts=None,
        exit_price=None,
        exit_reason=None,
        pnl_points=None,
        net_pnl_points=None,
        gross_r_multiple=None,
        max_drawdown_points=None,
        notes=(reason,),
    )


def _summarize_follower_trades(rows: list[FollowerTrade]) -> dict[str, Any]:
    entered = [row for row in rows if row.entered and row.net_pnl_points is not None]
    if not entered:
        return {
            "trade_count": 0,
            "entered_trade_count": 0,
            "win_rate": None,
            "avg_net_pnl_points": None,
            "gross_net_pnl_points": 0.0,
            "profit_factor": None,
            "max_drawdown_points": None,
        }
    winners = [row for row in entered if float(row.net_pnl_points or 0.0) > 0.0]
    losers = [row for row in entered if float(row.net_pnl_points or 0.0) <= 0.0]
    gross_profit = sum(float(row.net_pnl_points or 0.0) for row in winners)
    gross_loss = abs(sum(float(row.net_pnl_points or 0.0) for row in losers))
    return {
        "trade_count": len(rows),
        "entered_trade_count": len(entered),
        "win_rate": round(len(winners) / len(entered), 4) if entered else None,
        "avg_net_pnl_points": round(statistics.mean(float(row.net_pnl_points or 0.0) for row in entered), 4),
        "gross_net_pnl_points": round(sum(float(row.net_pnl_points or 0.0) for row in entered), 4),
        "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
        "max_drawdown_points": round(max(float(row.max_drawdown_points or 0.0) for row in entered), 4) if entered else None,
    }


def _pair_rankings(*, symbol_reports: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    rankings: dict[str, list[dict[str, Any]]] = {}
    for pair in PAIR_GROUPS:
        if pair != ("ES", "MES"):
            continue
        if not all(symbol in symbol_reports for symbol in pair):
            continue
        shared_keys = sorted(set(symbol_reports["ES"]["variants"]) & set(symbol_reports["MES"]["variants"]))
        rows: list[dict[str, Any]] = []
        for variant_key in shared_keys:
            es_summary = symbol_reports["ES"]["variants"][variant_key]["trade_summary"]
            mes_summary = symbol_reports["MES"]["variants"][variant_key]["trade_summary"]
            es_avg = es_summary["avg_net_pnl_points"]
            mes_avg = mes_summary["avg_net_pnl_points"]
            if es_avg is None or mes_avg is None:
                min_avg = None
            else:
                min_avg = round(min(float(es_avg), float(mes_avg)), 4)
            es_pf = es_summary["profit_factor"]
            mes_pf = mes_summary["profit_factor"]
            if es_pf is None or mes_pf is None:
                min_pf = None
            else:
                min_pf = round(min(float(es_pf), float(mes_pf)), 4)
            rows.append(
                {
                    "variant_key": variant_key,
                    "min_avg_net_pnl_points": min_avg,
                    "min_profit_factor": min_pf,
                    "es_trade_summary": es_summary,
                    "mes_trade_summary": mes_summary,
                }
            )
        rows.sort(
            key=lambda row: (
                row["min_avg_net_pnl_points"] is None,
                -(row["min_avg_net_pnl_points"] or float("-inf")),
                -(row["min_profit_factor"] or float("-inf")),
            )
        )
        rankings["ES_MES"] = rows
    return rankings


if __name__ == "__main__":
    raise SystemExit(main())
