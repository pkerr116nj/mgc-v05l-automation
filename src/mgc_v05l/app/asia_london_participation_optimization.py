"""Optimization pass for Asia-to-London participation strategies.

Keeps the base overnight participation engine fixed and applies an ATP-style
bias gate at the setup bar so results stay apples-to-apples with the original
entry/exit logic.
"""

from __future__ import annotations

import argparse
import json
import statistics
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ..config_models import load_settings_from_files
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
    DEFAULT_OUTPUT_DIR,
    HOLD_SEGMENTS,
    TICK_SIZES,
    DEFAULT_SYMBOLS,
    OvernightParticipationTrade,
    _build_overnight_session_contexts,
    _evaluate_long_participation,
    _evaluate_short_participation,
    _parse_date,
    _resolve_sqlite_database_path,
    _load_start_timestamp,
    _load_end_timestamp,
)
from .gc_mgc_segment_forced_session_long_research import build_variant_specs as build_long_variant_specs
from .gc_mgc_segment_forced_session_short_research import build_variant_specs as build_short_variant_specs


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OPTIMIZATION_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "asia_london_participation_optimization"
DEFAULT_LONG_VARIANT_IDS: tuple[str, ...] = (
    "segment_forced_long_v5_dip_reclaim_or_bar8",
    "segment_forced_long_v6_contextual_fallback",
)
DEFAULT_SHORT_VARIANT_IDS: tuple[str, ...] = (
    "segment_forced_short_v2_reclaim_fail_or_bar7",
)


@dataclass(frozen=True)
class OptimizationVariant:
    side: str
    variant_id: str
    description: str
    gate_mode: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-london-participation-optimization")
    parser.add_argument("--symbol", action="append", default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--config", action="append", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_asia_london_participation_optimization(
        symbols=args.symbol or list(DEFAULT_SYMBOLS),
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        config_paths=args.config,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


def run_asia_london_participation_optimization(
    *,
    symbols: list[str] | tuple[str, ...] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    output_dir: str | Path | None = None,
    config_paths: list[str] | list[Path] | tuple[str | Path, ...] | None = None,
) -> dict[str, Any]:
    resolved_symbols = tuple(sorted({str(symbol).strip().upper() for symbol in (symbols or DEFAULT_SYMBOLS) if str(symbol).strip()}))
    resolved_output_dir = Path(output_dir or DEFAULT_OPTIMIZATION_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    settings = load_settings_from_files([Path(path) for path in (config_paths or DEFAULT_CONFIG_PATHS)])
    sqlite_path = _resolve_sqlite_database_path(settings.database_url)
    start_day = _parse_date(start_date)
    end_day = _parse_date(end_date)

    long_specs = {
        spec.variant_id: replace(spec, tick_size=TICK_SIZES["GC"])
        for spec in build_long_variant_specs(selected_ids=list(DEFAULT_LONG_VARIANT_IDS))
        if spec.decision_timeframe == "3m"
    }
    short_specs = {
        spec.variant_id: replace(spec, tick_size=TICK_SIZES["GC"])
        for spec in build_short_variant_specs(selected_ids=list(DEFAULT_SHORT_VARIANT_IDS))
        if spec.decision_timeframe == "3m"
    }

    optimization_variants: list[OptimizationVariant] = []
    for variant_id, spec in long_specs.items():
        optimization_variants.append(
            OptimizationVariant(side="LONG", variant_id=variant_id, description=spec.description, gate_mode="base")
        )
        optimization_variants.append(
            OptimizationVariant(
                side="LONG",
                variant_id=variant_id,
                description=f"{spec.description} + ATP bias gate",
                gate_mode="atp_bias_gate",
            )
        )
    for variant_id, spec in short_specs.items():
        optimization_variants.append(
            OptimizationVariant(side="SHORT", variant_id=variant_id, description=spec.description, gate_mode="base")
        )
        optimization_variants.append(
            OptimizationVariant(
                side="SHORT",
                variant_id=variant_id,
                description=f"{spec.description} + ATP bias gate",
                gate_mode="atp_bias_gate",
            )
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
        for variant in optimization_variants:
            if variant.side == "LONG":
                base_spec = replace(long_specs[variant.variant_id], tick_size=TICK_SIZES[symbol])
                sessions = [
                    _apply_gate_to_trade(
                        trade=_evaluate_long_participation(symbol=symbol, context=context, spec=base_spec),
                        context=context,
                        side="LONG",
                        gate_mode=variant.gate_mode,
                    )
                    for context in contexts
                ]
            else:
                base_spec = replace(short_specs[variant.variant_id], tick_size=TICK_SIZES[symbol])
                sessions = [
                    _apply_gate_to_trade(
                        trade=_evaluate_short_participation(symbol=symbol, context=context, spec=base_spec),
                        context=context,
                        side="SHORT",
                        gate_mode=variant.gate_mode,
                    )
                    for context in contexts
                ]
            variant_key = f"{variant.side}__{variant.variant_id}__{variant.gate_mode}"
            variants[variant_key] = {
                "side": variant.side,
                "variant_id": variant.variant_id,
                "gate_mode": variant.gate_mode,
                "description": variant.description,
                "trade_summary": _summarize_trades_with_drawdown(sessions),
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
        "study_id": "asia_london_participation_optimization",
        "definition": {
            "symbols": list(resolved_symbols),
            "hold_segments": list(HOLD_SEGMENTS),
            "optimization_modes": ["base", "atp_bias_gate"],
            "start_date": start_day.isoformat() if start_day else None,
            "end_date": end_day.isoformat() if end_day else None,
        },
        "database_path": str(sqlite_path),
        "symbol_reports": symbol_reports,
        "pair_rankings": _pair_rankings(symbol_reports=symbol_reports),
    }
    json_path = resolved_output_dir / "asia_london_participation_optimization.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return {
        "mode": "asia_london_participation_optimization",
        "artifact_paths": {"json": str(json_path)},
        "pair_rankings": payload["pair_rankings"],
    }


def _apply_gate_to_trade(
    *,
    trade: OvernightParticipationTrade,
    context: Any,
    side: str,
    gate_mode: str,
) -> OvernightParticipationTrade:
    if gate_mode == "base" or not trade.entered:
        return trade
    gate = _bias_gate_pass(context=context, side=side)
    if gate["passed"]:
        return trade
    return replace(
        trade,
        entered=False,
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
        notes=tuple(list(trade.notes) + [f"bias_gate_failed:{gate['bias_state']}", *gate["reasons"]]),
    )


def _bias_gate_pass(*, context: Any, side: str) -> dict[str, Any]:
    bars = list(context.entry_segment_bars)
    if len(bars) < 4:
        return {"passed": False, "bias_state": "INSUFFICIENT_BARS", "reasons": ("insufficient_segment_bars",)}
    closes = [float(bar.close) for bar in bars]
    fast_ema_values = rolling_ema(closes, span=EMA_FAST_SPAN)
    slow_ema_values = rolling_ema(closes, span=EMA_SLOW_SPAN)
    atr_values = rolling_atr(bars, window=8)
    vwap_values = _running_vwap(bars)
    index = min(3, len(bars) - 1)
    bias = classify_bias(
        bars=bars,
        index=index,
        fast_ema=fast_ema_values[index],
        slow_ema=slow_ema_values[index],
        prev_slow_ema=slow_ema_values[index - 1] if index > 0 else None,
        session_vwap=vwap_values[index],
        atr=atr_values[index],
    )
    desired_state = LONG_BIAS if side == "LONG" else SHORT_BIAS
    return {
        "passed": bias.state == desired_state,
        "bias_state": bias.state,
        "reasons": bias.reasons,
        "long_score": bias.long_score,
        "short_score": bias.short_score,
    }


def _running_vwap(bars: list[Any]) -> list[float]:
    values: list[float] = []
    cumulative_pv = 0.0
    cumulative_volume = 0.0
    for bar in bars:
        volume = max(float(bar.volume), 1.0)
        cumulative_pv += float(bar.close) * volume
        cumulative_volume += volume
        values.append(cumulative_pv / max(cumulative_volume, 1.0))
    return values


def _summarize_trades_with_drawdown(rows: list[OvernightParticipationTrade]) -> dict[str, Any]:
    entered = sorted(
        [row for row in rows if row.entered and row.net_pnl_points is not None],
        key=lambda row: (row.trade_date, row.entry_end_ts or ""),
    )
    winners = [row for row in entered if (row.net_pnl_points or 0.0) > 0.0]
    losers = [row for row in entered if (row.net_pnl_points or 0.0) <= 0.0]
    gross_profit = sum((row.net_pnl_points or 0.0) for row in winners)
    gross_loss = abs(sum((row.net_pnl_points or 0.0) for row in losers))
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for row in entered:
        cumulative += float(row.net_pnl_points or 0.0)
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return {
        "entered_trade_count": len(entered),
        "average_net_pnl_points": round(statistics.fmean(row.net_pnl_points or 0.0 for row in entered), 4) if entered else None,
        "median_net_pnl_points": round(statistics.median(row.net_pnl_points or 0.0 for row in entered), 4) if entered else None,
        "net_profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0 else None,
        "net_win_rate": round(len(winners) / len(entered), 4) if entered else None,
        "total_net_pnl_points": round(sum(float(row.net_pnl_points or 0.0) for row in entered), 4),
        "max_drawdown_points": round(max_drawdown, 4),
        "entered_trade_days": [row.trade_date for row in entered],
    }


def _pair_rankings(*, symbol_reports: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    pair_groups = (("GC", "MGC"), ("ES", "MES"), ("NQ", "MNQ"))
    rankings: dict[str, list[dict[str, Any]]] = {}
    for pair in pair_groups:
        if any(symbol not in symbol_reports for symbol in pair):
            continue
        shared_keys = set(symbol_reports[pair[0]]["variants"].keys())
        for symbol in pair[1:]:
            shared_keys &= set(symbol_reports[symbol]["variants"].keys())
        rows: list[dict[str, Any]] = []
        for key in sorted(shared_keys):
            summaries = [symbol_reports[symbol]["variants"][key]["trade_summary"] for symbol in pair]
            first = symbol_reports[pair[0]]["variants"][key]
            rows.append(
                {
                    "variant_key": key,
                    "side": first["side"],
                    "variant_id": first["variant_id"],
                    "gate_mode": first["gate_mode"],
                    "description": first["description"],
                    "symbols": list(pair),
                    "min_entered_trade_count": min(summary["entered_trade_count"] for summary in summaries),
                    "min_average_net_pnl_points": min(summary["average_net_pnl_points"] or 0.0 for summary in summaries),
                    "min_net_profit_factor": min(summary["net_profit_factor"] or 0.0 for summary in summaries),
                    "max_pair_drawdown_points": max(summary["max_drawdown_points"] or 0.0 for summary in summaries),
                }
            )
        rows.sort(
            key=lambda row: (
                float(row["min_average_net_pnl_points"]),
                float(row["min_net_profit_factor"]),
                -float(row["max_pair_drawdown_points"]),
                int(row["min_entered_trade_count"]),
            ),
            reverse=True,
        )
        rankings["_".join(pair)] = rows
    return rankings


if __name__ == "__main__":
    raise SystemExit(main())
