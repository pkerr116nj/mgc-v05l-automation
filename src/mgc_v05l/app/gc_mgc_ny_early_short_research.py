"""Research study for GC/MGC NY-early short models."""

from __future__ import annotations

import argparse
import bisect
import json
import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..config_models import load_settings_from_files
from ..research.trend_participation.models import ResearchBar
from ..research.trend_participation.storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m
from .gc_mgc_segment_regime_research import label_gold_segment, trade_date_for_timestamp


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_ny_early_short"
DEFAULT_CONFIG_PATHS = (
    REPO_ROOT / "config" / "base.yaml",
    REPO_ROOT / "config" / "replay.yaml",
)
DEFAULT_SYMBOLS = ("GC", "MGC")
NEW_YORK = ZoneInfo("America/New_York")
NY_EARLY_START = time(8, 20)
NY_EARLY_END = time(11, 0)
DEFAULT_TICK_SIZE = 0.1


@dataclass(frozen=True)
class NyEarlyShortSpec:
    variant_id: str
    description: str
    setup_family: str
    decision_timeframe: str = "5m"
    setup_bar_count: int = 3
    entry_lookahead_bars: int = 4
    pre_context_minutes: int = 30
    max_pre_context_to_setup_range_ratio: float = 1.5
    min_setup_abs_efficiency: float = 0.35
    max_setup_close_location: float = 0.40
    min_setup_close_location: float = 0.60
    min_setup_vwap_displacement: float = 0.08
    max_setup_vwap_displacement: float = -0.08
    min_setup_green_share: float = 0.67
    min_setup_red_share: float = 0.67
    min_setup_volume_ratio: float = 0.90
    gate_mode: str = "strict"
    min_setup_score: int = 0
    meta_filter_mode: str = "none"
    max_meta_pre_context_ratio: float | None = None
    min_meta_abs_extension: float | None = None
    min_meta_volume_ratio: float | None = None
    min_meta_score: int = 0
    exit_mode: str = "ema_structure"
    exit_ema_length: int = 5
    tick_size: float = DEFAULT_TICK_SIZE
    slippage_ticks_per_side: float = 1.0
    gc_round_turn_commission_dollars: float = 4.5
    mgc_round_turn_commission_dollars: float = 1.5


@dataclass(frozen=True)
class ContractEconomics:
    symbol: str
    point_value_dollars: float
    round_turn_commission_dollars: float


@dataclass(frozen=True)
class NyEarlySessionContext:
    trade_date: date
    pre_context: list[ResearchBar]
    segment_bars: list[ResearchBar]


@dataclass(frozen=True)
class NyEarlyShortTrade:
    symbol: str
    trade_date: str
    variant_id: str
    setup_family: str
    qualified: bool
    entered: bool
    trigger_price: float | None
    stop_price: float | None
    entry_bar_number: int | None
    entry_end_ts: str | None
    entry_price: float | None
    exit_bar_number: int | None
    exit_end_ts: str | None
    exit_price: float | None
    exit_reason: str | None
    pnl_points: float | None
    net_pnl_points: float | None
    gross_pnl_dollars: float | None
    net_pnl_dollars: float | None
    execution_cost_points: float | None
    risk_points: float | None
    gross_r_multiple: float | None
    net_r_multiple: float | None
    mae_points: float | None
    mfe_points: float | None
    pre_context_range_points: float | None
    pre_context_to_setup_range_ratio: float | None
    setup_return_points: float | None
    setup_range_points: float | None
    setup_abs_efficiency: float | None
    setup_close_location: float | None
    setup_vwap_displacement: float | None
    setup_green_share: float | None
    setup_red_share: float | None
    setup_volume_ratio: float | None
    setup_score: int | None
    setup_score_max: int | None
    meta_score: int | None
    meta_score_max: int | None
    notes: tuple[str, ...] = ()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc-mgc-ny-early-short-research")
    parser.add_argument("--symbol", action="append", default=None, help="Symbol to evaluate. Defaults to GC and MGC.")
    parser.add_argument("--variant", action="append", default=None, help="Optional variant id filter.")
    parser.add_argument("--output-dir", default=None, help="Optional output directory override.")
    parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Config file path. May be supplied multiple times; later files override earlier ones.",
    )
    parser.add_argument("--start-date", default=None, help="Optional inclusive futures trade-date filter in YYYY-MM-DD form.")
    parser.add_argument("--end-date", default=None, help="Optional inclusive futures trade-date filter in YYYY-MM-DD form.")
    parser.add_argument("--inspect-date", default=None, help="Optional futures trade date to highlight.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_gc_mgc_ny_early_short_research(
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


def run_gc_mgc_ny_early_short_research(
    *,
    symbols: list[str] | tuple[str, ...] | None = None,
    variant_ids: list[str] | tuple[str, ...] | None = None,
    output_dir: str | Path | None = None,
    config_paths: list[str] | list[Path] | tuple[str | Path, ...] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    inspect_date: str | date | None = None,
) -> dict[str, Any]:
    resolved_symbols = tuple(
        sorted({str(symbol).strip().upper() for symbol in (symbols or DEFAULT_SYMBOLS) if str(symbol).strip()})
    )
    variants = build_variant_specs(selected_ids=variant_ids)
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    settings = load_settings_from_files([Path(path) for path in (config_paths or DEFAULT_CONFIG_PATHS)])
    sqlite_path = _resolve_sqlite_database_path(settings.database_url)
    start_day = _parse_date(start_date)
    end_day = _parse_date(end_date)
    highlighted_day = _parse_date(inspect_date)
    load_start_ts = _load_start_timestamp(start_day)
    load_end_ts = _load_end_timestamp(end_day)

    symbol_reports: dict[str, Any] = {}
    highlighted_sessions: dict[str, Any] = {}
    for symbol in resolved_symbols:
        one_minute = load_sqlite_bars(
            sqlite_path=sqlite_path,
            instrument=symbol,
            timeframe="1m",
            data_source="historical_1m_canonical",
            start_ts=load_start_ts,
            end_ts=load_end_ts,
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
                max_pre_context_minutes=max(spec.pre_context_minutes for spec in variants),
            )
            for timeframe, bars in timeframe_cache.items()
        }
        variant_sessions: dict[str, list[NyEarlyShortTrade]] = {}
        for spec in variants:
            sessions = evaluate_symbol_sessions(
                symbol=symbol,
                spec=spec,
                session_contexts=session_contexts_by_timeframe[spec.decision_timeframe],
            )
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
        "study_id": "gc_mgc_ny_early_short",
        "study_status": "research_first_pass",
        "definition": {
            "description": (
                "NY-early short study for gold futures. The first-pass hypotheses are a failed early upside pop "
                "that reverses lower and a downside resumption after weak early structure."
            ),
            "segment_window": {
                "segment_id": "US_EARLY",
                "start_time": NY_EARLY_START.isoformat(timespec="minutes"),
                "end_time": NY_EARLY_END.isoformat(timespec="minutes"),
            },
            "variants": [asdict(spec) for spec in variants],
            "notes": [
                "This is a fresh NY-early short extraction path based on the promoted gold regime cell.",
                "All decisions are made from canonical 1m bars with resampled decision bars per variant.",
                "Execution assumptions are placeholders for research and should be tuned before any live use.",
            ],
        },
        "database_path": str(sqlite_path),
        "symbols": list(resolved_symbols),
        "date_filter": {
            "start_date": start_day.isoformat() if start_day is not None else None,
            "end_date": end_day.isoformat() if end_day is not None else None,
        },
        "highlighted_trade_date": highlighted_day.isoformat() if highlighted_day is not None else None,
        "highlighted_sessions": highlighted_sessions,
        "symbol_reports": symbol_reports,
        "cross_symbol_ranking": _cross_symbol_ranking(symbol_reports, variants=variants),
    }

    json_path = resolved_output_dir / "gc_mgc_ny_early_short_research.json"
    markdown_path = resolved_output_dir / "gc_mgc_ny_early_short_research.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=_json_ready) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report).strip() + "\n", encoding="utf-8")
    return {
        "mode": "gc_mgc_ny_early_short_research",
        "artifact_paths": {"json": str(json_path), "markdown": str(markdown_path)},
        "highlighted_trade_date": highlighted_day.isoformat() if highlighted_day is not None else None,
        "cross_symbol_ranking": report["cross_symbol_ranking"],
    }


def build_variant_specs(*, selected_ids: list[str] | tuple[str, ...] | None = None) -> tuple[NyEarlyShortSpec, ...]:
    variants = (
        NyEarlyShortSpec(
            variant_id="ny_early_short_v1_failed_pop_5m",
            description="5m failed-pop reversal: early upthrust, then break below setup low.",
            setup_family="failed_pop_reversal",
            decision_timeframe="5m",
            setup_bar_count=3,
            entry_lookahead_bars=4,
            max_pre_context_to_setup_range_ratio=1.4,
            min_setup_abs_efficiency=0.40,
            min_setup_close_location=0.68,
            min_setup_vwap_displacement=0.08,
            min_setup_green_share=0.67,
            min_setup_volume_ratio=0.95,
            exit_mode="ema_structure",
        ),
        NyEarlyShortSpec(
            variant_id="ny_early_short_v2_failed_pop_3m",
            description="3m score-based failed-pop reversal for finer NY-early timing.",
            setup_family="failed_pop_reversal",
            decision_timeframe="3m",
            setup_bar_count=4,
            entry_lookahead_bars=6,
            max_pre_context_to_setup_range_ratio=1.8,
            min_setup_abs_efficiency=0.28,
            min_setup_close_location=0.58,
            min_setup_vwap_displacement=0.02,
            min_setup_green_share=0.50,
            min_setup_volume_ratio=0.80,
            gate_mode="score",
            min_setup_score=5,
            exit_mode="ema_structure",
        ),
        NyEarlyShortSpec(
            variant_id="ny_early_short_v3_downside_resume_5m",
            description="5m downside resumption after a weak NY-early opening structure.",
            setup_family="downside_resumption",
            decision_timeframe="5m",
            setup_bar_count=3,
            entry_lookahead_bars=4,
            max_pre_context_to_setup_range_ratio=1.6,
            min_setup_abs_efficiency=0.38,
            max_setup_close_location=0.35,
            max_setup_vwap_displacement=-0.06,
            min_setup_red_share=0.67,
            min_setup_volume_ratio=0.90,
            exit_mode="ema_structure",
        ),
        NyEarlyShortSpec(
            variant_id="ny_early_short_v4_failed_pop_3m_meta",
            description="3m failed-pop pool plus a tighter meta-filter for extension and volume.",
            setup_family="failed_pop_reversal",
            decision_timeframe="3m",
            setup_bar_count=4,
            entry_lookahead_bars=6,
            max_pre_context_to_setup_range_ratio=1.8,
            min_setup_abs_efficiency=0.28,
            min_setup_close_location=0.58,
            min_setup_vwap_displacement=0.02,
            min_setup_green_share=0.50,
            min_setup_volume_ratio=0.80,
            gate_mode="score",
            min_setup_score=5,
            meta_filter_mode="score",
            max_meta_pre_context_ratio=1.35,
            min_meta_abs_extension=0.30,
            min_meta_volume_ratio=1.05,
            min_meta_score=2,
            exit_mode="ema_structure",
        ),
    )
    if not selected_ids:
        return variants
    selected = {variant_id.strip() for variant_id in selected_ids if str(variant_id).strip()}
    return tuple(spec for spec in variants if spec.variant_id in selected)


def evaluate_symbol_sessions(
    *,
    symbol: str,
    spec: NyEarlyShortSpec,
    session_contexts: list[NyEarlySessionContext],
) -> list[NyEarlyShortTrade]:
    rows: list[NyEarlyShortTrade] = []
    for context in session_contexts:
        if spec.pre_context_minutes < len(context.pre_context):
            pre_context = context.pre_context[-spec.pre_context_minutes :]
        else:
            pre_context = context.pre_context
        rows.append(
            _evaluate_ny_early_session(
                symbol=symbol,
                trade_day=context.trade_date,
                pre_context=pre_context,
                segment_bars=context.segment_bars,
                spec=spec,
            )
        )
    return rows


def _build_ny_early_session_contexts(
    *,
    one_minute_bars: list[ResearchBar],
    decision_bars: list[ResearchBar],
    start_day: date | None,
    end_day: date | None,
    max_pre_context_minutes: int,
) -> list[NyEarlySessionContext]:
    grouped: dict[date, list[ResearchBar]] = defaultdict(list)
    for bar in decision_bars:
        if label_gold_segment(bar.end_ts) != "US_EARLY":
            continue
        trade_day = trade_date_for_timestamp(bar.end_ts)
        if start_day is not None and trade_day < start_day:
            continue
        if end_day is not None and trade_day > end_day:
            continue
        grouped[trade_day].append(bar)

    local_1m = sorted((_local_timestamp(bar.end_ts), bar) for bar in one_minute_bars)
    local_times = [local_dt for local_dt, _ in local_1m]
    contexts: list[NyEarlySessionContext] = []
    for trade_day in sorted(grouped):
        start_dt = datetime.combine(trade_day, NY_EARLY_START, tzinfo=NEW_YORK)
        left = bisect.bisect_left(local_times, start_dt - timedelta(minutes=max_pre_context_minutes))
        right = bisect.bisect_left(local_times, start_dt)
        contexts.append(
            NyEarlySessionContext(
                trade_date=trade_day,
                pre_context=[bar for _, bar in local_1m[left:right]],
                segment_bars=grouped[trade_day],
            )
        )
    return contexts


def _evaluate_ny_early_session(
    *,
    symbol: str,
    trade_day: date,
    pre_context: list[ResearchBar],
    segment_bars: list[ResearchBar],
    spec: NyEarlyShortSpec,
) -> NyEarlyShortTrade:
    if len(segment_bars) < spec.setup_bar_count + 2:
        return _blank_trade(
            symbol=symbol,
            trade_day=trade_day,
            variant_id=spec.variant_id,
            setup_family=spec.setup_family,
            notes=("insufficient_segment_bars",),
        )
    if len(pre_context) < max(5, spec.pre_context_minutes // 2):
        return _blank_trade(
            symbol=symbol,
            trade_day=trade_day,
            variant_id=spec.variant_id,
            setup_family=spec.setup_family,
            notes=("insufficient_pre_context",),
        )

    setup_bars = segment_bars[: spec.setup_bar_count]
    setup_high = max(bar.high for bar in setup_bars)
    setup_low = min(bar.low for bar in setup_bars)
    setup_range = max(setup_high - setup_low, 1e-9)
    setup_return = setup_bars[-1].close - setup_bars[0].open
    setup_abs_efficiency = abs(setup_return) / setup_range
    setup_close_location = (setup_bars[-1].close - setup_low) / setup_range
    setup_vwap = _bars_vwap(setup_bars)
    setup_vwap_displacement = (setup_bars[-1].close - setup_vwap) / setup_range
    setup_green_share = sum(1 for bar in setup_bars if bar.close > bar.open) / len(setup_bars)
    setup_red_share = sum(1 for bar in setup_bars if bar.close < bar.open) / len(setup_bars)
    pre_context_range = _bars_range(pre_context)
    pre_context_to_setup_range_ratio = (pre_context_range / setup_range) if pre_context_range is not None else None
    setup_volume_ratio = _volume_ratio(setup_bars, pre_context)

    setup_score = 0
    setup_score_max = 0
    core_failed: list[str] = []
    quality_failed: list[str] = []
    notes: list[str] = []

    if spec.setup_family == "failed_pop_reversal":
        core_checks = {
            "positive_setup_return": setup_return > 0.0,
        }
        quality_checks = {
            "pre_context_ratio_ok": (
                pre_context_to_setup_range_ratio is not None
                and pre_context_to_setup_range_ratio <= spec.max_pre_context_to_setup_range_ratio
            ),
            "setup_efficiency_ok": setup_abs_efficiency >= spec.min_setup_abs_efficiency,
            "setup_close_location_ok": setup_close_location >= spec.min_setup_close_location,
            "setup_vwap_displacement_ok": setup_vwap_displacement >= spec.min_setup_vwap_displacement,
            "setup_green_share_ok": setup_green_share >= spec.min_setup_green_share,
            "setup_volume_ratio_ok": setup_volume_ratio is not None and setup_volume_ratio >= spec.min_setup_volume_ratio,
        }
    else:
        core_checks = {
            "negative_setup_return": setup_return < 0.0,
        }
        quality_checks = {
            "pre_context_ratio_ok": (
                pre_context_to_setup_range_ratio is not None
                and pre_context_to_setup_range_ratio <= spec.max_pre_context_to_setup_range_ratio
            ),
            "setup_efficiency_ok": setup_abs_efficiency >= spec.min_setup_abs_efficiency,
            "setup_close_location_ok": setup_close_location <= spec.max_setup_close_location,
            "setup_vwap_displacement_ok": setup_vwap_displacement <= spec.max_setup_vwap_displacement,
            "setup_red_share_ok": setup_red_share >= spec.min_setup_red_share,
            "setup_volume_ratio_ok": setup_volume_ratio is not None and setup_volume_ratio >= spec.min_setup_volume_ratio,
        }
    setup_score = sum(1 for passed in quality_checks.values() if passed)
    setup_score_max = len(quality_checks)
    core_failed = [name for name, passed in core_checks.items() if not passed]
    quality_failed = [name for name, passed in quality_checks.items() if not passed]

    qualified = False
    if core_failed:
        notes.extend(core_failed)
    elif spec.gate_mode == "score":
        qualified = setup_score >= spec.min_setup_score
        if not qualified:
            notes.append(f"setup_score_below_threshold_{setup_score}_of_{setup_score_max}")
            notes.extend(quality_failed)
    else:
        qualified = not quality_failed
        if not qualified:
            notes.extend(quality_failed)

    meta_score = None
    meta_score_max = None
    if qualified and spec.meta_filter_mode == "score":
        meta_abs_extension = abs(setup_return) / max(abs(setup_bars[0].open), 1e-9)
        meta_checks = {
            "meta_pre_context_ratio_ok": (
                pre_context_to_setup_range_ratio is not None
                and spec.max_meta_pre_context_ratio is not None
                and pre_context_to_setup_range_ratio <= spec.max_meta_pre_context_ratio
            ),
            "meta_abs_extension_ok": (
                spec.min_meta_abs_extension is not None and meta_abs_extension >= spec.min_meta_abs_extension / 100.0
            ),
            "meta_volume_ratio_ok": spec.min_meta_volume_ratio is not None
            and setup_volume_ratio is not None
            and setup_volume_ratio >= spec.min_meta_volume_ratio,
        }
        meta_score = sum(1 for passed in meta_checks.values() if passed)
        meta_score_max = len(meta_checks)
        if meta_score < spec.min_meta_score:
            qualified = False
            notes.append(f"meta_score_below_threshold_{meta_score}_of_{meta_score_max}")
            notes.extend(name for name, passed in meta_checks.items() if not passed)

    if not qualified:
        return NyEarlyShortTrade(
            symbol=symbol,
            trade_date=trade_day.isoformat(),
            variant_id=spec.variant_id,
            setup_family=spec.setup_family,
            qualified=False,
            entered=False,
            trigger_price=None,
            stop_price=None,
            entry_bar_number=None,
            entry_end_ts=None,
            entry_price=None,
            exit_bar_number=None,
            exit_end_ts=None,
            exit_price=None,
            exit_reason=None,
            pnl_points=None,
            net_pnl_points=None,
            gross_pnl_dollars=None,
            net_pnl_dollars=None,
            execution_cost_points=None,
            risk_points=None,
            gross_r_multiple=None,
            net_r_multiple=None,
            mae_points=None,
            mfe_points=None,
            pre_context_range_points=_round_nullable(pre_context_range),
            pre_context_to_setup_range_ratio=_round_nullable(pre_context_to_setup_range_ratio),
            setup_return_points=_round_price(setup_return),
            setup_range_points=_round_price(setup_range),
            setup_abs_efficiency=_round_price(setup_abs_efficiency),
            setup_close_location=_round_price(setup_close_location),
            setup_vwap_displacement=_round_price(setup_vwap_displacement),
            setup_green_share=_round_price(setup_green_share),
            setup_red_share=_round_price(setup_red_share),
            setup_volume_ratio=_round_nullable(setup_volume_ratio),
            setup_score=setup_score,
            setup_score_max=setup_score_max,
            meta_score=meta_score,
            meta_score_max=meta_score_max,
            notes=tuple(notes),
        )

    trigger_price = _round_price(setup_low - spec.tick_size)
    stop_price = _round_price(setup_high + spec.tick_size)
    breakdown_index = _find_breakdown_close_index(segment_bars=segment_bars, trigger_price=trigger_price, spec=spec)
    if breakdown_index is None or breakdown_index + 1 >= len(segment_bars):
        return NyEarlyShortTrade(
            symbol=symbol,
            trade_date=trade_day.isoformat(),
            variant_id=spec.variant_id,
            setup_family=spec.setup_family,
            qualified=True,
            entered=False,
            trigger_price=trigger_price,
            stop_price=stop_price,
            entry_bar_number=None,
            entry_end_ts=None,
            entry_price=None,
            exit_bar_number=None,
            exit_end_ts=None,
            exit_price=None,
            exit_reason=None,
            pnl_points=None,
            net_pnl_points=None,
            gross_pnl_dollars=None,
            net_pnl_dollars=None,
            execution_cost_points=None,
            risk_points=_round_price(stop_price - trigger_price),
            gross_r_multiple=None,
            net_r_multiple=None,
            mae_points=None,
            mfe_points=None,
            pre_context_range_points=_round_nullable(pre_context_range),
            pre_context_to_setup_range_ratio=_round_nullable(pre_context_to_setup_range_ratio),
            setup_return_points=_round_price(setup_return),
            setup_range_points=_round_price(setup_range),
            setup_abs_efficiency=_round_price(setup_abs_efficiency),
            setup_close_location=_round_price(setup_close_location),
            setup_vwap_displacement=_round_price(setup_vwap_displacement),
            setup_green_share=_round_price(setup_green_share),
            setup_red_share=_round_price(setup_red_share),
            setup_volume_ratio=_round_nullable(setup_volume_ratio),
            setup_score=setup_score,
            setup_score_max=setup_score_max,
            meta_score=meta_score,
            meta_score_max=meta_score_max,
            notes=("breakdown_not_confirmed",),
        )

    entry_index = breakdown_index + 1
    entry_bar = segment_bars[entry_index]
    entry_price = _round_price(entry_bar.open)
    risk_points = _round_price(max(stop_price - entry_price, spec.tick_size))
    ema_values = _ema([bar.close for bar in segment_bars], length=spec.exit_ema_length)
    exit_index, exit_price, exit_reason = _find_exit(
        segment_bars=segment_bars,
        entry_index=entry_index,
        stop_price=stop_price,
        ema_values=ema_values,
        spec=spec,
    )
    pnl_points = _round_price(entry_price - exit_price)
    economics = _contract_economics(symbol, spec=spec)
    execution_cost_points = _round_price(_execution_cost_points(spec=spec, economics=economics))
    gross_pnl_dollars = _round_price(pnl_points * economics.point_value_dollars)
    net_pnl_points = _round_price(pnl_points - execution_cost_points)
    net_pnl_dollars = _round_price(gross_pnl_dollars - (execution_cost_points * economics.point_value_dollars))
    trade_window = segment_bars[entry_index : exit_index + 1]
    mfe_points = _round_price(max(0.0, entry_price - min(bar.low for bar in trade_window)))
    mae_points = _round_price(max(0.0, max(bar.high for bar in trade_window) - entry_price))

    gross_r_multiple = _round_price(pnl_points / risk_points) if risk_points > 0 else None
    net_r_multiple = _round_price(net_pnl_points / risk_points) if risk_points > 0 else None
    return NyEarlyShortTrade(
        symbol=symbol,
        trade_date=trade_day.isoformat(),
        variant_id=spec.variant_id,
        setup_family=spec.setup_family,
        qualified=True,
        entered=True,
        trigger_price=trigger_price,
        stop_price=stop_price,
        entry_bar_number=entry_index + 1,
        entry_end_ts=entry_bar.end_ts.isoformat(),
        entry_price=entry_price,
        exit_bar_number=exit_index + 1,
        exit_end_ts=segment_bars[exit_index].end_ts.isoformat(),
        exit_price=exit_price,
        exit_reason=exit_reason,
        pnl_points=pnl_points,
        net_pnl_points=net_pnl_points,
        gross_pnl_dollars=gross_pnl_dollars,
        net_pnl_dollars=net_pnl_dollars,
        execution_cost_points=execution_cost_points,
        risk_points=risk_points,
        gross_r_multiple=gross_r_multiple,
        net_r_multiple=net_r_multiple,
        mae_points=mae_points,
        mfe_points=mfe_points,
        pre_context_range_points=_round_nullable(pre_context_range),
        pre_context_to_setup_range_ratio=_round_nullable(pre_context_to_setup_range_ratio),
        setup_return_points=_round_price(setup_return),
        setup_range_points=_round_price(setup_range),
        setup_abs_efficiency=_round_price(setup_abs_efficiency),
        setup_close_location=_round_price(setup_close_location),
        setup_vwap_displacement=_round_price(setup_vwap_displacement),
        setup_green_share=_round_price(setup_green_share),
        setup_red_share=_round_price(setup_red_share),
        setup_volume_ratio=_round_nullable(setup_volume_ratio),
        setup_score=setup_score,
        setup_score_max=setup_score_max,
        meta_score=meta_score,
        meta_score_max=meta_score_max,
        notes=tuple(notes),
    )


def _blank_trade(*, symbol: str, trade_day: date, variant_id: str, setup_family: str, notes: tuple[str, ...]) -> NyEarlyShortTrade:
    return NyEarlyShortTrade(
        symbol=symbol,
        trade_date=trade_day.isoformat(),
        variant_id=variant_id,
        setup_family=setup_family,
        qualified=False,
        entered=False,
        trigger_price=None,
        stop_price=None,
        entry_bar_number=None,
        entry_end_ts=None,
        entry_price=None,
        exit_bar_number=None,
        exit_end_ts=None,
        exit_price=None,
        exit_reason=None,
        pnl_points=None,
        net_pnl_points=None,
        gross_pnl_dollars=None,
        net_pnl_dollars=None,
        execution_cost_points=None,
        risk_points=None,
        gross_r_multiple=None,
        net_r_multiple=None,
        mae_points=None,
        mfe_points=None,
        pre_context_range_points=None,
        pre_context_to_setup_range_ratio=None,
        setup_return_points=None,
        setup_range_points=None,
        setup_abs_efficiency=None,
        setup_close_location=None,
        setup_vwap_displacement=None,
        setup_green_share=None,
        setup_red_share=None,
        setup_volume_ratio=None,
        setup_score=None,
        setup_score_max=None,
        meta_score=None,
        meta_score_max=None,
        notes=notes,
    )


def _find_breakdown_close_index(*, segment_bars: list[ResearchBar], trigger_price: float, spec: NyEarlyShortSpec) -> int | None:
    start_index = spec.setup_bar_count
    stop_index = min(len(segment_bars), spec.setup_bar_count + spec.entry_lookahead_bars)
    for index in range(start_index, stop_index):
        if segment_bars[index].close <= trigger_price:
            return index
    return None


def _find_exit(
    *,
    segment_bars: list[ResearchBar],
    entry_index: int,
    stop_price: float,
    ema_values: list[float | None],
    spec: NyEarlyShortSpec,
) -> tuple[int, float, str]:
    for index in range(entry_index, len(segment_bars)):
        bar = segment_bars[index]
        if bar.high >= stop_price:
            return index, stop_price, "initial_stop"
        if spec.exit_mode == "ema_structure" and index > entry_index:
            ema_value = ema_values[index]
            if ema_value is not None and bar.close > ema_value and bar.close > segment_bars[index - 1].high:
                return index, _round_price(bar.close), "ema_structure_break"
    return len(segment_bars) - 1, _round_price(segment_bars[-1].close), "segment_close"


def _summarize_trades(rows: list[NyEarlyShortTrade]) -> dict[str, Any]:
    qualified = [row for row in rows if row.qualified]
    entered = [row for row in qualified if row.entered and row.pnl_points is not None]
    net_winners = [row for row in entered if (row.net_pnl_points or 0.0) > 0.0]
    net_losers = [row for row in entered if (row.net_pnl_points or 0.0) <= 0.0]
    net_profit = sum((row.net_pnl_points or 0.0) for row in net_winners)
    net_loss = abs(sum((row.net_pnl_points or 0.0) for row in net_losers))
    return {
        "qualified_session_count": len(qualified),
        "entered_trade_count": len(entered),
        "net_win_rate": _round_nullable(len(net_winners) / len(entered) if entered else None),
        "average_pnl_points": _round_nullable(statistics.fmean(row.pnl_points or 0.0 for row in entered) if entered else None),
        "average_net_pnl_points": _round_nullable(
            statistics.fmean(row.net_pnl_points or 0.0 for row in entered) if entered else None
        ),
        "median_net_pnl_points": _round_nullable(
            statistics.median(row.net_pnl_points or 0.0 for row in entered) if entered else None
        ),
        "net_profit_factor": _round_nullable(net_profit / net_loss if net_loss > 0 else None),
        "average_gross_r_multiple": _round_nullable(
            statistics.fmean(row.gross_r_multiple or 0.0 for row in entered) if entered else None
        ),
        "average_net_r_multiple": _round_nullable(
            statistics.fmean(row.net_r_multiple or 0.0 for row in entered) if entered else None
        ),
        "average_mae_points": _round_nullable(statistics.fmean(row.mae_points or 0.0 for row in entered) if entered else None),
        "average_mfe_points": _round_nullable(statistics.fmean(row.mfe_points or 0.0 for row in entered) if entered else None),
        "best_net_trade_points": max((row.net_pnl_points or 0.0) for row in entered) if entered else None,
        "worst_net_trade_points": min((row.net_pnl_points or 0.0) for row in entered) if entered else None,
    }


def _cross_symbol_ranking(symbol_reports: dict[str, Any], *, variants: tuple[NyEarlyShortSpec, ...]) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for spec in variants:
        gc_summary = symbol_reports.get("GC", {}).get("variants", {}).get(spec.variant_id, {}).get("trade_summary")
        mgc_summary = symbol_reports.get("MGC", {}).get("variants", {}).get(spec.variant_id, {}).get("trade_summary")
        if gc_summary is None or mgc_summary is None:
            continue
        ranked.append(
            {
                "variant_id": spec.variant_id,
                "description": spec.description,
                "setup_family": spec.setup_family,
                "gc_entered_trade_count": gc_summary["entered_trade_count"],
                "mgc_entered_trade_count": mgc_summary["entered_trade_count"],
                "gc_average_net_pnl_points": gc_summary["average_net_pnl_points"],
                "mgc_average_net_pnl_points": mgc_summary["average_net_pnl_points"],
                "gc_net_profit_factor": gc_summary["net_profit_factor"],
                "mgc_net_profit_factor": mgc_summary["net_profit_factor"],
                "min_entered_trade_count": min(gc_summary["entered_trade_count"], mgc_summary["entered_trade_count"]),
                "min_average_net_pnl_points": min(
                    gc_summary["average_net_pnl_points"] or 0.0,
                    mgc_summary["average_net_pnl_points"] or 0.0,
                ),
                "min_net_profit_factor": min(
                    gc_summary["net_profit_factor"] or 0.0,
                    mgc_summary["net_profit_factor"] or 0.0,
                ),
            }
        )
    ranked.sort(
        key=lambda item: (
            float(item["min_average_net_pnl_points"]),
            float(item["min_net_profit_factor"]),
            int(item["min_entered_trade_count"]),
        ),
        reverse=True,
    )
    return ranked


def _bars_range(bars: list[ResearchBar]) -> float | None:
    if not bars:
        return None
    return max(bar.high for bar in bars) - min(bar.low for bar in bars)


def _bars_vwap(bars: list[ResearchBar]) -> float:
    total_volume = sum(max(float(bar.volume), 1.0) for bar in bars)
    total_pv = sum(float(bar.close) * max(float(bar.volume), 1.0) for bar in bars)
    return total_pv / max(total_volume, 1.0)


def _volume_ratio(setup_bars: list[ResearchBar], pre_context: list[ResearchBar]) -> float | None:
    if not pre_context:
        return None
    setup_mean = statistics.fmean(max(float(bar.volume), 1.0) for bar in setup_bars)
    pre_mean = statistics.fmean(max(float(bar.volume), 1.0) for bar in pre_context)
    return setup_mean / max(pre_mean, 1e-9)


def _ema(values: list[float], *, length: int) -> list[float | None]:
    alpha = 2.0 / (length + 1.0)
    ema_value: float | None = None
    output: list[float | None] = [None] * len(values)
    for index, value in enumerate(values):
        if ema_value is None:
            ema_value = float(value)
        else:
            ema_value = alpha * float(value) + (1.0 - alpha) * ema_value
        output[index] = ema_value
    return output


def _contract_economics(symbol: str, *, spec: NyEarlyShortSpec) -> ContractEconomics:
    normalized = symbol.upper()
    if normalized == "GC":
        return ContractEconomics(
            symbol=normalized,
            point_value_dollars=100.0,
            round_turn_commission_dollars=spec.gc_round_turn_commission_dollars,
        )
    return ContractEconomics(
        symbol=normalized,
        point_value_dollars=10.0,
        round_turn_commission_dollars=spec.mgc_round_turn_commission_dollars,
    )


def _execution_cost_points(*, spec: NyEarlyShortSpec, economics: ContractEconomics) -> float:
    slippage_points = 2.0 * spec.slippage_ticks_per_side * spec.tick_size
    commission_points = economics.round_turn_commission_dollars / economics.point_value_dollars
    return slippage_points + commission_points


def _resolve_sqlite_database_path(database_url: str) -> Path:
    if not database_url.startswith("sqlite:///"):
        raise ValueError(f"Unsupported database URL for research load: {database_url}")
    return Path(database_url.removeprefix("sqlite:///")).resolve()


def _load_start_timestamp(start_day: date | None) -> datetime | None:
    if start_day is None:
        return None
    return datetime.combine(start_day, time(7, 45), tzinfo=NEW_YORK)


def _load_end_timestamp(end_day: date | None) -> datetime | None:
    if end_day is None:
        return None
    return datetime.combine(end_day, time(11, 10), tzinfo=NEW_YORK)


def _parse_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _local_timestamp(timestamp: datetime) -> datetime:
    return timestamp.astimezone(NEW_YORK) if timestamp.tzinfo is not None else timestamp.replace(tzinfo=NEW_YORK)


def _round_nullable(value: float | None) -> float | None:
    return None if value is None else _round_price(value)


def _round_price(value: float) -> float:
    return round(float(value), 4)


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


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# GC/MGC NY Early Short Research",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Database: `{report['database_path']}`",
        f"- Symbols: `{', '.join(report['symbols'])}`",
        "",
        "## Cross-Symbol Ranking",
        "",
    ]
    for row in report["cross_symbol_ranking"]:
        lines.append(
            f"- `{row['variant_id']}` ({row['setup_family']}): min avg net pnl `{row['min_average_net_pnl_points']}`, min PF `{row['min_net_profit_factor']}`, min trades `{row['min_entered_trade_count']}`"
        )
    lines.extend(["", "## Symbol Summary", ""])
    for symbol, payload in report["symbol_reports"].items():
        lines.append(f"### {symbol}")
        lines.append("")
        lines.append(f"- 1m bars loaded: `{payload['bar_count_1m']}`")
        lines.append(
            "- Decision bars derived: "
            + ", ".join(f"`{timeframe}`=`{count}`" for timeframe, count in payload["bar_counts_by_timeframe"].items())
        )
        for variant_id, variant_payload in payload["variants"].items():
            summary = variant_payload["trade_summary"]
            lines.append(
                f"- `{variant_id}`: trades `{summary['entered_trade_count']}`, avg net pnl `{summary['average_net_pnl_points']}`, net PF `{summary['net_profit_factor']}`"
            )
        lines.append("")
    return "\n".join(lines)
