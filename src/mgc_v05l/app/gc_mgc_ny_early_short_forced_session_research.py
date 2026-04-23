"""Forced one-trade-per-session research for GC/MGC NY-early short playbooks."""

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
from .gc_mgc_ny_early_short_research import (
    DEFAULT_CONFIG_PATHS,
    DEFAULT_SYMBOLS,
    NyEarlySessionContext,
    NyEarlyShortSpec,
    _build_ny_early_session_contexts,
    _contract_economics,
    _ema,
    _execution_cost_points,
    _find_exit,
    _load_end_timestamp,
    _load_start_timestamp,
    _parse_date,
    _resolve_sqlite_database_path,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_ny_early_short_forced_session"


@dataclass(frozen=True)
class ForcedNyEarlyShortSpec:
    variant_id: str
    description: str
    decision_timeframe: str = "3m"
    setup_bar_count: int = 4
    fallback_entry_bar: int = 6
    tick_size: float = 0.1
    slippage_ticks_per_side: float = 1.0
    gc_round_turn_commission_dollars: float = 4.5
    mgc_round_turn_commission_dollars: float = 1.5
    exit_mode: str = "ema_structure"
    exit_ema_length: int = 5


@dataclass(frozen=True)
class ForcedSessionTrade:
    symbol: str
    trade_date: str
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
    parser = argparse.ArgumentParser(prog="gc-mgc-ny-early-short-forced-session-research")
    parser.add_argument("--symbol", action="append", default=None, help="Symbol to evaluate. Defaults to GC and MGC.")
    parser.add_argument("--variant", action="append", default=None, help="Optional variant id filter.")
    parser.add_argument("--output-dir", default=None, help="Optional output directory override.")
    parser.add_argument("--config", action="append", default=None, help="Config path, may be supplied multiple times.")
    parser.add_argument("--start-date", default=None, help="Optional inclusive futures trade-date filter.")
    parser.add_argument("--end-date", default=None, help="Optional inclusive futures trade-date filter.")
    parser.add_argument("--inspect-date", default=None, help="Optional futures trade date to highlight.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_gc_mgc_ny_early_short_forced_session_research(
        symbols=args.symbol or list(DEFAULT_SYMBOLS),
        variant_ids=args.variant,
        output_dir=args.output_dir,
        config_paths=args.config,
        start_date=args.start_date,
        end_date=args.end_date,
        inspect_date=args.inspect_date,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_gc_mgc_ny_early_short_forced_session_research(
    *,
    symbols: list[str] | tuple[str, ...] | None = None,
    variant_ids: list[str] | tuple[str, ...] | None = None,
    output_dir: str | Path | None = None,
    config_paths: list[str] | list[Path] | tuple[str | Path, ...] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    inspect_date: str | date | None = None,
) -> dict[str, Any]:
    resolved_symbols = tuple(sorted({str(s).strip().upper() for s in (symbols or DEFAULT_SYMBOLS) if str(s).strip()}))
    variants = build_variant_specs(selected_ids=variant_ids)
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    settings = load_settings_from_files([Path(path) for path in (config_paths or DEFAULT_CONFIG_PATHS)])
    sqlite_path = _resolve_sqlite_database_path(settings.database_url)
    start_day = _parse_date(start_date)
    end_day = _parse_date(end_date)
    highlighted_day = _parse_date(inspect_date)

    symbol_reports: dict[str, Any] = {}
    highlighted_sessions: dict[str, Any] = {}
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
        timeframe_cache = {
            timeframe: resample_bars_from_1m(bars_1m=normalized_1m, target_timeframe=timeframe)
            for timeframe in sorted({spec.decision_timeframe for spec in variants})
        }
        session_contexts_by_timeframe = {
            timeframe: _build_ny_early_session_contexts(
                one_minute_bars=normalized_1m,
                decision_bars=bars,
                start_day=start_day,
                end_day=end_day,
                max_pre_context_minutes=30,
            )
            for timeframe, bars in timeframe_cache.items()
        }
        variant_sessions: dict[str, list[ForcedSessionTrade]] = {}
        for spec in variants:
            sessions = [
                _evaluate_forced_session(symbol=symbol, trade_day=context.trade_date, segment_bars=context.segment_bars, spec=spec)
                for context in session_contexts_by_timeframe[spec.decision_timeframe]
            ]
            variant_sessions[spec.variant_id] = sessions
        symbol_reports[symbol] = {
            "bar_count_1m": len(normalized_1m),
            "bar_counts_by_timeframe": {timeframe: len(bars) for timeframe, bars in timeframe_cache.items()},
            "data_quality_issues": [asdict(issue) for issue in quality_issues],
            "variants": {
                variant_id: {
                    "trade_summary": _summarize_trades(rows),
                    "sessions": [asdict(row) for row in rows],
                }
                for variant_id, rows in variant_sessions.items()
            },
        }
        highlighted_sessions[symbol] = {
            variant_id: (asdict(next((row for row in rows if row.trade_date == highlighted_day.isoformat()), None)) if highlighted_day else None)
            for variant_id, rows in variant_sessions.items()
        }

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "gc_mgc_ny_early_short_forced_session",
        "study_status": "frequency_first_pass",
        "definition": {
            "description": "Forced-session NY-early short playbooks: exactly one short attempt per session with rule-specific preferred trigger and timed fallback.",
            "variants": [asdict(spec) for spec in variants],
        },
        "database_path": str(sqlite_path),
        "symbols": list(resolved_symbols),
        "highlighted_trade_date": highlighted_day.isoformat() if highlighted_day else None,
        "highlighted_sessions": highlighted_sessions,
        "symbol_reports": symbol_reports,
        "cross_symbol_ranking": _cross_symbol_ranking(symbol_reports, variants),
    }
    json_path = resolved_output_dir / "gc_mgc_ny_early_short_forced_session_research.json"
    markdown_path = resolved_output_dir / "gc_mgc_ny_early_short_forced_session_research.md"
    json_path.write_text(json.dumps(_json_ready(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report).strip() + "\n", encoding="utf-8")
    return {
        "mode": "gc_mgc_ny_early_short_forced_session_research",
        "artifact_paths": {"json": str(json_path), "markdown": str(markdown_path)},
        "cross_symbol_ranking": report["cross_symbol_ranking"],
    }


def build_variant_specs(*, selected_ids: list[str] | tuple[str, ...] | None = None) -> tuple[ForcedNyEarlyShortSpec, ...]:
    variants = (
        ForcedNyEarlyShortSpec(
            variant_id="ny_forced_short_v1_breakdown_or_bar6",
            description="Prefer break below first-four-bar low; otherwise short bar 6 open.",
            fallback_entry_bar=6,
        ),
        ForcedNyEarlyShortSpec(
            variant_id="ny_forced_short_v2_reclaim_fail_or_bar7",
            description="Prefer a failed reclaim after an early pop; otherwise short bar 7 open.",
            fallback_entry_bar=7,
        ),
        ForcedNyEarlyShortSpec(
            variant_id="ny_forced_short_v3_bar5_always",
            description="Always short bar 5 open using the first four bars for risk framing.",
            fallback_entry_bar=5,
        ),
    )
    if not selected_ids:
        return variants
    selected = {str(item).strip() for item in selected_ids if str(item).strip()}
    return tuple(spec for spec in variants if spec.variant_id in selected)


def _evaluate_forced_session(*, symbol: str, trade_day: date, segment_bars: list[ResearchBar], spec: ForcedNyEarlyShortSpec) -> ForcedSessionTrade:
    if len(segment_bars) <= spec.fallback_entry_bar:
        return ForcedSessionTrade(
            symbol=symbol,
            trade_date=trade_day.isoformat(),
            variant_id=spec.variant_id,
            entered=False,
            entry_reason="insufficient_segment_bars",
            entry_bar_number=None,
            entry_end_ts=None,
            entry_price=None,
            stop_price=None,
            exit_bar_number=None,
            exit_end_ts=None,
            exit_price=None,
            exit_reason=None,
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
            notes=("insufficient_segment_bars",),
        )

    setup_bars = segment_bars[: spec.setup_bar_count]
    setup_high = max(bar.high for bar in setup_bars)
    setup_low = min(bar.low for bar in setup_bars)
    setup_range = max(setup_high - setup_low, 1e-9)
    setup_return = setup_bars[-1].close - setup_bars[0].open
    setup_close_location = (setup_bars[-1].close - setup_low) / setup_range
    setup_vwap = _bars_vwap(setup_bars)
    setup_vwap_displacement = (setup_bars[-1].close - setup_vwap) / setup_range

    entry_index, entry_reason = _select_entry_index(segment_bars=segment_bars, spec=spec, setup_low=setup_low, setup_high=setup_high)
    entry_bar = segment_bars[entry_index]
    entry_price = round(float(entry_bar.open), 4)
    stop_price = round(float(setup_high + spec.tick_size), 4)
    risk_points = round(max(stop_price - entry_price, spec.tick_size), 4)
    base_spec = NyEarlyShortSpec(
        variant_id=spec.variant_id,
        description=spec.description,
        setup_family="forced_session",
        decision_timeframe=spec.decision_timeframe,
        exit_mode=spec.exit_mode,
        exit_ema_length=spec.exit_ema_length,
        tick_size=spec.tick_size,
        slippage_ticks_per_side=spec.slippage_ticks_per_side,
        gc_round_turn_commission_dollars=spec.gc_round_turn_commission_dollars,
        mgc_round_turn_commission_dollars=spec.mgc_round_turn_commission_dollars,
    )
    ema_values = _ema([bar.close for bar in segment_bars], length=spec.exit_ema_length)
    exit_index, exit_price, exit_reason = _find_exit(
        segment_bars=segment_bars,
        entry_index=entry_index,
        stop_price=stop_price,
        ema_values=ema_values,
        spec=base_spec,
    )
    pnl_points = round(entry_price - exit_price, 4)
    economics = _contract_economics(symbol, spec=base_spec)
    execution_cost_points = round(_execution_cost_points(spec=base_spec, economics=economics), 4)
    net_pnl_points = round(pnl_points - execution_cost_points, 4)
    trade_window = segment_bars[entry_index : exit_index + 1]
    mfe_points = round(max(0.0, entry_price - min(bar.low for bar in trade_window)), 4)
    mae_points = round(max(0.0, max(bar.high for bar in trade_window) - entry_price), 4)
    gross_r_multiple = round(pnl_points / risk_points, 4) if risk_points > 0 else None
    net_r_multiple = round(net_pnl_points / risk_points, 4) if risk_points > 0 else None
    return ForcedSessionTrade(
        symbol=symbol,
        trade_date=trade_day.isoformat(),
        variant_id=spec.variant_id,
        entered=True,
        entry_reason=entry_reason,
        entry_bar_number=entry_index + 1,
        entry_end_ts=entry_bar.end_ts.isoformat(),
        entry_price=entry_price,
        stop_price=stop_price,
        exit_bar_number=exit_index + 1,
        exit_end_ts=segment_bars[exit_index].end_ts.isoformat(),
        exit_price=exit_price,
        exit_reason=exit_reason,
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


def _select_entry_index(*, segment_bars: list[ResearchBar], spec: ForcedNyEarlyShortSpec, setup_low: float, setup_high: float) -> tuple[int, str]:
    if spec.variant_id == "ny_forced_short_v1_breakdown_or_bar6":
        trigger = setup_low - spec.tick_size
        for index in range(spec.setup_bar_count, min(len(segment_bars), spec.fallback_entry_bar + 1)):
            if segment_bars[index].close <= trigger:
                return index, "preferred_breakdown"
        return min(spec.fallback_entry_bar - 1, len(segment_bars) - 1), "fallback_bar6"
    if spec.variant_id == "ny_forced_short_v2_reclaim_fail_or_bar7":
        midpoint = setup_low + ((setup_high - setup_low) * 0.5)
        for index in range(spec.setup_bar_count, min(len(segment_bars), spec.fallback_entry_bar + 1)):
            prev = segment_bars[index - 1]
            cur = segment_bars[index]
            if prev.high >= midpoint and cur.close < prev.low:
                return index, "preferred_reclaim_fail"
        return min(spec.fallback_entry_bar - 1, len(segment_bars) - 1), "fallback_bar7"
    return min(spec.fallback_entry_bar - 1, len(segment_bars) - 1), "timed_bar5"


def _bars_vwap(bars: list[ResearchBar]) -> float:
    total_volume = sum(max(float(bar.volume), 1.0) for bar in bars)
    total_pv = sum(float(bar.close) * max(float(bar.volume), 1.0) for bar in bars)
    return total_pv / max(total_volume, 1.0)


def _summarize_trades(rows: list[ForcedSessionTrade]) -> dict[str, Any]:
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
    }


def _cross_symbol_ranking(symbol_reports: dict[str, Any], variants: tuple[ForcedNyEarlyShortSpec, ...]) -> list[dict[str, Any]]:
    ranked = []
    for spec in variants:
        gc = symbol_reports.get("GC", {}).get("variants", {}).get(spec.variant_id, {}).get("trade_summary")
        mgc = symbol_reports.get("MGC", {}).get("variants", {}).get(spec.variant_id, {}).get("trade_summary")
        if not gc or not mgc:
            continue
        ranked.append(
            {
                "variant_id": spec.variant_id,
                "description": spec.description,
                "gc_entered_trade_count": gc["entered_trade_count"],
                "mgc_entered_trade_count": mgc["entered_trade_count"],
                "gc_average_net_pnl_points": gc["average_net_pnl_points"],
                "mgc_average_net_pnl_points": mgc["average_net_pnl_points"],
                "gc_net_profit_factor": gc["net_profit_factor"],
                "mgc_net_profit_factor": mgc["net_profit_factor"],
                "min_average_net_pnl_points": min(gc["average_net_pnl_points"] or 0.0, mgc["average_net_pnl_points"] or 0.0),
                "min_net_profit_factor": min(gc["net_profit_factor"] or 0.0, mgc["net_profit_factor"] or 0.0),
                "min_entered_trade_count": min(gc["entered_trade_count"], mgc["entered_trade_count"]),
            }
        )
    ranked.sort(key=lambda row: (float(row["min_average_net_pnl_points"]), float(row["min_net_profit_factor"])), reverse=True)
    return ranked


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# GC/MGC NY Early Short Forced Session Research",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Database: `{report['database_path']}`",
        "",
        "## Cross-Symbol Ranking",
        "",
    ]
    for row in report["cross_symbol_ranking"]:
        lines.append(
            f"- `{row['variant_id']}`: min avg net `{row['min_average_net_pnl_points']}`, min PF `{row['min_net_profit_factor']}`, min trades `{row['min_entered_trade_count']}`"
        )
    lines.extend(["", "## Symbol Summary", ""])
    for symbol, payload in report["symbol_reports"].items():
        lines.append(f"### {symbol}")
        lines.append("")
        for variant_id, variant_payload in payload["variants"].items():
            summary = variant_payload["trade_summary"]
            lines.append(
                f"- `{variant_id}`: trades `{summary['entered_trade_count']}`, avg net `{summary['average_net_pnl_points']}`, PF `{summary['net_profit_factor']}`"
            )
        lines.append("")
    return "\n".join(lines)


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value
