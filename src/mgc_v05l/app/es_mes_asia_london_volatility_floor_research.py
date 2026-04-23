"""ES/MES overnight one-sided research with Asia setup volatility floors.

This rescue path keeps the native overnight participation entry/exit logic fixed,
but only allows entries on nights where the ASIA_EARLY setup range is large
enough relative to the session ATR to plausibly pay for friction.
"""

from __future__ import annotations

import argparse
import json
import statistics
from dataclasses import asdict, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ..config_models import load_settings_from_files
from ..research.trend_participation.state_layers import rolling_atr
from ..research.trend_participation.storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m
from .asia_london_participation_research import (
    DEFAULT_CONFIG_PATHS,
    HOLD_SEGMENTS,
    OvernightParticipationTrade,
    TICK_SIZES,
    _build_overnight_session_contexts,
    _evaluate_long_participation,
    _evaluate_short_participation,
    _load_end_timestamp,
    _load_start_timestamp,
    _parse_date,
    _resolve_sqlite_database_path,
)
from .gc_mgc_segment_forced_session_long_research import build_variant_specs as build_long_variant_specs
from .gc_mgc_segment_forced_session_short_research import build_variant_specs as build_short_variant_specs


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "es_mes_asia_london_volatility_floor_research_v1"
DEFAULT_SYMBOLS: tuple[str, ...] = ("ES", "MES")
DEFAULT_LONG_VARIANT_IDS: tuple[str, ...] = (
    "segment_forced_long_v5_dip_reclaim_or_bar8",
    "segment_forced_long_v6_contextual_fallback",
)
DEFAULT_SHORT_VARIANT_IDS: tuple[str, ...] = ("segment_forced_short_v2_reclaim_fail_or_bar7",)
VOLATILITY_FLOORS: tuple[float, ...] = (0.0, 0.75, 1.0, 1.25)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="es-mes-asia-london-volatility-floor-research")
    parser.add_argument("--symbol", action="append", default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--config", action="append", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_es_mes_asia_london_volatility_floor_research(
        symbols=args.symbol or list(DEFAULT_SYMBOLS),
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        config_paths=args.config,
    )
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 0


def run_es_mes_asia_london_volatility_floor_research(
    *,
    symbols: list[str] | tuple[str, ...] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    output_dir: str | Path | None = None,
    config_paths: list[str] | list[Path] | tuple[str | Path, ...] | None = None,
    volatility_floors: list[float] | tuple[float, ...] | None = None,
    long_variant_ids: list[str] | tuple[str, ...] | None = None,
    short_variant_ids: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    resolved_symbols = tuple(sorted({str(symbol).strip().upper() for symbol in (symbols or DEFAULT_SYMBOLS) if str(symbol).strip()}))
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    settings = load_settings_from_files([Path(path) for path in (config_paths or DEFAULT_CONFIG_PATHS)])
    sqlite_path = _resolve_sqlite_database_path(settings.database_url)
    start_day = _parse_date(start_date)
    end_day = _parse_date(end_date)

    resolved_floors = tuple(float(value) for value in (volatility_floors or VOLATILITY_FLOORS))
    long_specs = {
        spec.variant_id: replace(spec, tick_size=TICK_SIZES["ES"])
        for spec in build_long_variant_specs(selected_ids=list(long_variant_ids or DEFAULT_LONG_VARIANT_IDS))
        if spec.decision_timeframe == "3m"
    }
    short_specs = {
        spec.variant_id: replace(spec, tick_size=TICK_SIZES["ES"])
        for spec in build_short_variant_specs(selected_ids=list(short_variant_ids or DEFAULT_SHORT_VARIANT_IDS))
        if spec.decision_timeframe == "3m"
    }

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
        for floor in resolved_floors:
            for variant_id, base_spec in long_specs.items():
                spec = replace(base_spec, tick_size=TICK_SIZES[symbol])
                rows = [
                    _apply_volatility_floor(
                        trade=_evaluate_long_participation(symbol=symbol, context=context, spec=spec),
                        context=context,
                        setup_bar_count=spec.setup_bar_count,
                        floor_ratio=floor,
                    )
                    for context in contexts
                ]
                variant_key = f"LONG__{variant_id}__vol_floor_{_format_floor(floor)}"
                variants[variant_key] = {
                    "side": "LONG",
                    "variant_id": variant_id,
                    "gate_mode": f"vol_floor_{_format_floor(floor)}",
                    "volatility_floor_ratio": floor,
                    "trade_summary": _summarize_trades_with_drawdown(rows),
                    "sessions": [asdict(row) for row in rows],
                }
            for variant_id, base_spec in short_specs.items():
                spec = replace(base_spec, tick_size=TICK_SIZES[symbol])
                rows = [
                    _apply_volatility_floor(
                        trade=_evaluate_short_participation(symbol=symbol, context=context, spec=spec),
                        context=context,
                        setup_bar_count=spec.setup_bar_count,
                        floor_ratio=floor,
                    )
                    for context in contexts
                ]
                variant_key = f"SHORT__{variant_id}__vol_floor_{_format_floor(floor)}"
                variants[variant_key] = {
                    "side": "SHORT",
                    "variant_id": variant_id,
                    "gate_mode": f"vol_floor_{_format_floor(floor)}",
                    "volatility_floor_ratio": floor,
                    "trade_summary": _summarize_trades_with_drawdown(rows),
                    "sessions": [asdict(row) for row in rows],
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
        "study_id": "es_mes_asia_london_volatility_floor_research",
        "definition": {
            "symbols": list(resolved_symbols),
            "hold_segments": list(HOLD_SEGMENTS),
            "volatility_floors": list(resolved_floors),
            "description": "Native ES/MES overnight participation variants filtered by minimum ASIA_EARLY setup-range-to-ATR ratio.",
            "start_date": start_day.isoformat() if start_day else None,
            "end_date": end_day.isoformat() if end_day else None,
        },
        "database_path": str(sqlite_path),
        "symbol_reports": symbol_reports,
        "pair_rankings": _pair_rankings(symbol_reports=symbol_reports),
        "single_symbol_best": _single_symbol_best(symbol_reports=symbol_reports),
    }
    json_path = resolved_output_dir / "es_mes_asia_london_volatility_floor_research.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return {
        "mode": payload["study_id"],
        "artifact_paths": {"json": str(json_path)},
        "pair_rankings": payload["pair_rankings"],
        "single_symbol_best": payload["single_symbol_best"],
    }


def _apply_volatility_floor(
    *,
    trade: OvernightParticipationTrade,
    context: Any,
    setup_bar_count: int,
    floor_ratio: float,
) -> OvernightParticipationTrade:
    if not trade.entered or floor_ratio <= 0.0:
        return trade
    decision = _setup_volatility_snapshot(context=context, setup_bar_count=setup_bar_count)
    ratio = decision["setup_range_atr_ratio"]
    if ratio is not None and float(ratio) >= float(floor_ratio):
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
        notes=tuple(
            list(trade.notes)
            + [f"volatility_floor_failed:{decision['setup_range_atr_ratio']}", f"required_floor:{floor_ratio}", *decision["reasons"]]
        ),
    )


def _setup_volatility_snapshot(*, context: Any, setup_bar_count: int) -> dict[str, Any]:
    bars = list(context.entry_segment_bars)
    if len(bars) < setup_bar_count:
        return {
            "setup_range_atr_ratio": None,
            "reasons": ("insufficient_segment_bars",),
        }
    atr_values = rolling_atr(bars, window=8)
    index = min(setup_bar_count - 1, len(bars) - 1)
    atr_value = max(float(atr_values[index]), 1e-9)
    setup_bars = bars[:setup_bar_count]
    setup_range = max(float(max(bar.high for bar in setup_bars) - min(bar.low for bar in setup_bars)), 0.0)
    ratio = round(setup_range / atr_value, 4)
    return {
        "setup_range_atr_ratio": ratio,
        "reasons": (),
    }


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
    if not {"ES", "MES"} <= set(symbol_reports):
        return {}
    shared_keys = sorted(set(symbol_reports["ES"]["variants"]) & set(symbol_reports["MES"]["variants"]))
    rows: list[dict[str, Any]] = []
    for key in shared_keys:
        es_summary = symbol_reports["ES"]["variants"][key]["trade_summary"]
        mes_summary = symbol_reports["MES"]["variants"][key]["trade_summary"]
        rows.append(
            {
                "variant_key": key,
                "side": symbol_reports["ES"]["variants"][key]["side"],
                "variant_id": symbol_reports["ES"]["variants"][key]["variant_id"],
                "gate_mode": symbol_reports["ES"]["variants"][key]["gate_mode"],
                "volatility_floor_ratio": symbol_reports["ES"]["variants"][key]["volatility_floor_ratio"],
                "min_entered_trade_count": min(es_summary["entered_trade_count"], mes_summary["entered_trade_count"]),
                "min_average_net_pnl_points": min(es_summary["average_net_pnl_points"] or 0.0, mes_summary["average_net_pnl_points"] or 0.0),
                "min_net_profit_factor": min(es_summary["net_profit_factor"] or 0.0, mes_summary["net_profit_factor"] or 0.0),
                "max_pair_drawdown_points": max(es_summary["max_drawdown_points"] or 0.0, mes_summary["max_drawdown_points"] or 0.0),
                "es_trade_summary": es_summary,
                "mes_trade_summary": mes_summary,
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
    return {"ES_MES": rows}


def _single_symbol_best(*, symbol_reports: dict[str, Any]) -> dict[str, dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for symbol, report in symbol_reports.items():
        ranked = sorted(
            [
                {
                    "variant_key": key,
                    "side": value["side"],
                    "variant_id": value["variant_id"],
                    "gate_mode": value["gate_mode"],
                    "volatility_floor_ratio": value["volatility_floor_ratio"],
                    "trade_summary": value["trade_summary"],
                }
                for key, value in report["variants"].items()
            ],
            key=lambda row: (
                float(row["trade_summary"]["average_net_pnl_points"] or 0.0),
                float(row["trade_summary"]["net_profit_factor"] or 0.0),
                -float(row["trade_summary"]["max_drawdown_points"] or 0.0),
                int(row["trade_summary"]["entered_trade_count"]),
            ),
            reverse=True,
        )
        if ranked:
            best[symbol] = ranked[0]
    return best


def _format_floor(value: float) -> str:
    return str(value).replace(".", "p")


if __name__ == "__main__":
    raise SystemExit(main())
