"""Gold-native GC/MGC session-segment regime dataset and empirical score research."""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..config_models import load_settings_from_files
from ..research.trend_participation.models import ResearchBar
from ..research.trend_participation.storage import load_sqlite_bars, normalize_and_check_bars


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_segment_regime"
DEFAULT_CONFIG_PATHS = (
    REPO_ROOT / "config" / "base.yaml",
    REPO_ROOT / "config" / "replay.yaml",
)
DEFAULT_SYMBOLS = ("GC", "MGC")
NEW_YORK = ZoneInfo("America/New_York")
SESSION_RESET_TIME = time(18, 0)
DEFAULT_TICK_SIZE = 0.1
DEFAULT_SETUP_MINUTES = 15
DEFAULT_PRE_CONTEXT_MINUTES = 30
DEFAULT_BASELINE_CONTEXT_MINUTES = 30
SCORE_FEATURES = (
    "pre_context_range_points",
    "pre_context_drift_points",
    "pre_context_compression_ratio",
    "pre_context_mean_abs_delta",
    "setup_range_points",
    "setup_return_points",
    "setup_abs_efficiency",
    "setup_close_location",
    "setup_vwap_displacement",
    "setup_volume_ratio",
    "setup_green_share",
    "setup_pressure",
)


@dataclass(frozen=True)
class GoldSegmentDefinition:
    segment_id: str
    start_time: time
    end_time: time
    description: str


@dataclass
class GoldSegmentRegimeRow:
    symbol: str
    trade_date: str
    segment_id: str
    segment_start_ts: str
    segment_end_ts: str
    segment_bar_count: int
    setup_bar_count: int
    continuation_bar_count: int
    pre_context_bar_count: int
    baseline_context_bar_count: int
    pre_context_range_points: float | None
    pre_context_drift_points: float | None
    pre_context_compression_ratio: float | None
    pre_context_mean_abs_delta: float | None
    setup_range_points: float
    setup_return_points: float
    setup_abs_efficiency: float
    setup_close_location: float
    setup_vwap_displacement: float
    setup_volume_ratio: float | None
    setup_green_share: float
    setup_red_share: float
    setup_pressure: float
    segment_close_price: float
    long_trigger_price: float
    long_stop_price: float
    long_risk_points: float
    long_triggered: bool
    long_entry_ts: str | None
    long_close_pnl_points: float | None
    long_mfe_points: float | None
    long_mae_points: float | None
    long_one_r_reached: bool
    long_positive_label: bool
    short_trigger_price: float
    short_stop_price: float
    short_risk_points: float
    short_triggered: bool
    short_entry_ts: str | None
    short_close_pnl_points: float | None
    short_mfe_points: float | None
    short_mae_points: float | None
    short_one_r_reached: bool
    short_positive_label: bool
    long_regime_score: float | None = None
    short_regime_score: float | None = None


@dataclass(frozen=True)
class EmpiricalBucket:
    lower_bound: float
    upper_bound: float
    sample_count: int
    positive_count: int
    positive_rate: float


SEGMENTS: tuple[GoldSegmentDefinition, ...] = (
    GoldSegmentDefinition(
        segment_id="SESSION_OPEN",
        start_time=time(18, 0),
        end_time=time(19, 0),
        description="Globex reopen reset / initial discovery window.",
    ),
    GoldSegmentDefinition(
        segment_id="ASIA_EARLY",
        start_time=time(19, 0),
        end_time=time(20, 30),
        description="Post-open Asia impulse / early continuation window.",
    ),
    GoldSegmentDefinition(
        segment_id="ASIA_LATE",
        start_time=time(20, 30),
        end_time=time(23, 0),
        description="Late Asia continuation / digestion window.",
    ),
    GoldSegmentDefinition(
        segment_id="LONDON_EARLY",
        start_time=time(3, 0),
        end_time=time(5, 30),
        description="London open discovery window.",
    ),
    GoldSegmentDefinition(
        segment_id="LONDON_LATE",
        start_time=time(5, 30),
        end_time=time(8, 20),
        description="Late London transition into COMEX open.",
    ),
    GoldSegmentDefinition(
        segment_id="US_EARLY",
        start_time=time(8, 20),
        end_time=time(11, 0),
        description="COMEX/NY early response and continuation window.",
    ),
    GoldSegmentDefinition(
        segment_id="US_MIDDAY",
        start_time=time(11, 0),
        end_time=time(13, 30),
        description="Late NY continuation / fade window.",
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gc-mgc-segment-regime-research")
    parser.add_argument(
        "--symbol",
        action="append",
        default=None,
        help="Symbol to evaluate. Defaults to GC and MGC.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory override.",
    )
    parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Config file path. May be supplied multiple times; later files override earlier ones.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional inclusive futures trade-date filter in YYYY-MM-DD form.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional inclusive futures trade-date filter in YYYY-MM-DD form.",
    )
    parser.add_argument(
        "--inspect-date",
        default=None,
        help="Optional trade date to highlight in the output.",
    )
    parser.add_argument(
        "--tick-size",
        type=float,
        default=DEFAULT_TICK_SIZE,
        help="Trigger/stop tick size assumption. Defaults to 0.1 for GC/MGC.",
    )
    parser.add_argument(
        "--setup-minutes",
        type=int,
        default=DEFAULT_SETUP_MINUTES,
        help="Number of opening minutes inside each segment used to define the setup. Defaults to 15.",
    )
    parser.add_argument(
        "--pre-context-minutes",
        type=int,
        default=DEFAULT_PRE_CONTEXT_MINUTES,
        help="Minutes immediately preceding the segment used for local context. Defaults to 30.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_gc_mgc_segment_regime_research(
        symbols=args.symbol or list(DEFAULT_SYMBOLS),
        output_dir=args.output_dir,
        config_paths=args.config,
        start_date=args.start_date,
        end_date=args.end_date,
        inspect_date=args.inspect_date,
        tick_size=float(args.tick_size),
        setup_minutes=int(args.setup_minutes),
        pre_context_minutes=int(args.pre_context_minutes),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def run_gc_mgc_segment_regime_research(
    *,
    symbols: list[str] | tuple[str, ...] | None = None,
    output_dir: str | Path | None = None,
    config_paths: list[str] | list[Path] | tuple[str | Path, ...] | None = None,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    inspect_date: str | date | None = None,
    tick_size: float = DEFAULT_TICK_SIZE,
    setup_minutes: int = DEFAULT_SETUP_MINUTES,
    pre_context_minutes: int = DEFAULT_PRE_CONTEXT_MINUTES,
) -> dict[str, Any]:
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

    all_rows: list[GoldSegmentRegimeRow] = []
    symbol_reports: dict[str, Any] = {}
    highlighted_rows: dict[str, list[dict[str, Any]]] = {}

    load_start_ts = _load_start_timestamp(start_day)
    load_end_ts = _load_end_timestamp(end_day)
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
        scored_rows = build_segment_regime_rows(
            symbol=symbol,
            bars=normalized_1m,
            start_day=start_day,
            end_day=end_day,
            tick_size=tick_size,
            setup_minutes=setup_minutes,
            pre_context_minutes=pre_context_minutes,
        )
        all_rows.extend(scored_rows)
        highlighted_rows[symbol] = [
            _json_ready(asdict(row))
            for row in scored_rows
            if highlighted_day is not None and row.trade_date == highlighted_day.isoformat()
        ]
        symbol_reports[symbol] = {
            "bar_count_1m": len(normalized_1m),
            "data_quality_issues": [_json_ready(asdict(issue)) for issue in quality_issues],
            "row_count": len(scored_rows),
            "trade_date_count": len({row.trade_date for row in scored_rows}),
            "segment_summary": _summarize_segment_rows(scored_rows),
            "top_regime_cells": _top_regime_cells(scored_rows),
        }

    rows_csv_path = resolved_output_dir / "gc_mgc_segment_regime_dataset.csv"
    _write_rows_csv(rows_csv_path, all_rows)
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "study_id": "gc_mgc_segment_regime",
        "study_status": "research_dataset_and_empirical_score",
        "definition": {
            "description": (
                "Gold-native session-segment dataset and empirical regime scorecard for GC/MGC. "
                "Each row represents one trade-date segment, with long and short continuation labels "
                "built from the first setup window inside that segment."
            ),
            "segment_windows": [
                {
                    "segment_id": segment.segment_id,
                    "start_time": segment.start_time.isoformat(timespec="minutes"),
                    "end_time": segment.end_time.isoformat(timespec="minutes"),
                    "description": segment.description,
                }
                for segment in SEGMENTS
            ],
            "trade_date_roll": "Bars at or after 18:00 ET are assigned to the next futures trade date.",
            "assumptions": [
                "The scorecard is gold-native and does not use equity-index context features.",
                "Long and short labels are evaluated independently from the same segment setup window.",
                "Positive labels require both trigger confirmation and at least 1R favorable excursion before segment end.",
                "The current score is empirical and interpretable; it is a staging layer for later walk-forward models.",
            ],
            "parameters": {
                "tick_size": tick_size,
                "setup_minutes": setup_minutes,
                "pre_context_minutes": pre_context_minutes,
                "baseline_context_minutes": DEFAULT_BASELINE_CONTEXT_MINUTES,
                "score_features": list(SCORE_FEATURES),
            },
        },
        "database_path": str(sqlite_path),
        "symbols": list(resolved_symbols),
        "date_filter": {
            "start_date": start_day.isoformat() if start_day is not None else None,
            "end_date": end_day.isoformat() if end_day is not None else None,
        },
        "highlighted_trade_date": highlighted_day.isoformat() if highlighted_day is not None else None,
        "highlighted_rows": highlighted_rows,
        "dataset_path": str(rows_csv_path),
        "row_count": len(all_rows),
        "symbol_reports": symbol_reports,
    }

    json_path = resolved_output_dir / "gc_mgc_segment_regime_research.json"
    markdown_path = resolved_output_dir / "gc_mgc_segment_regime_research.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True, default=_json_ready) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report).strip() + "\n", encoding="utf-8")
    return {
        "mode": "gc_mgc_segment_regime_research",
        "artifact_paths": {
            "json": str(json_path),
            "markdown": str(markdown_path),
            "dataset_csv": str(rows_csv_path),
        },
        "row_count": len(all_rows),
        "highlighted_trade_date": highlighted_day.isoformat() if highlighted_day is not None else None,
        "symbols": list(resolved_symbols),
    }


def build_segment_regime_rows(
    *,
    symbol: str,
    bars: list[ResearchBar],
    start_day: date | None,
    end_day: date | None,
    tick_size: float,
    setup_minutes: int,
    pre_context_minutes: int,
) -> list[GoldSegmentRegimeRow]:
    indexed = _build_bar_index(bars)
    rows: list[GoldSegmentRegimeRow] = []
    candidate_trade_dates = sorted(indexed["segment_keys"])
    for trade_day, segment_id in candidate_trade_dates:
        if start_day is not None and trade_day < start_day:
            continue
        if end_day is not None and trade_day > end_day:
            continue
        segment = _segment_by_id(segment_id)
        if segment is None:
            continue
        segment_bars = indexed["segment_bars"].get((trade_day, segment_id), [])
        if len(segment_bars) <= setup_minutes:
            continue
        row = _build_segment_row(
            symbol=symbol,
            trade_day=trade_day,
            segment=segment,
            segment_bars=segment_bars,
            local_bars=indexed["local_bars"],
            local_times=indexed["local_times"],
            tick_size=tick_size,
            setup_minutes=setup_minutes,
            pre_context_minutes=pre_context_minutes,
        )
        if row is not None:
            rows.append(row)
    _apply_empirical_regime_scores(rows)
    return rows


def _build_bar_index(bars: list[ResearchBar]) -> dict[str, Any]:
    local_bars: list[tuple[datetime, ResearchBar]] = []
    segment_bars: dict[tuple[date, str], list[ResearchBar]] = defaultdict(list)
    segment_keys: set[tuple[date, str]] = set()
    for bar in sorted(bars, key=lambda item: item.end_ts):
        local_dt = _local_timestamp(bar.end_ts)
        local_bars.append((local_dt, bar))
        segment_id = label_gold_segment(local_dt)
        if segment_id is None:
            continue
        trade_day = trade_date_for_timestamp(local_dt)
        segment_bars[(trade_day, segment_id)].append(bar)
        segment_keys.add((trade_day, segment_id))
    return {
        "local_bars": local_bars,
        "local_times": [local_dt for local_dt, _ in local_bars],
        "segment_bars": segment_bars,
        "segment_keys": segment_keys,
    }


def _build_segment_row(
    *,
    symbol: str,
    trade_day: date,
    segment: GoldSegmentDefinition,
    segment_bars: list[ResearchBar],
    local_bars: list[tuple[datetime, ResearchBar]],
    local_times: list[datetime],
    tick_size: float,
    setup_minutes: int,
    pre_context_minutes: int,
) -> GoldSegmentRegimeRow | None:
    segment_start_dt, segment_end_dt = _segment_window_for_trade_date(trade_day, segment)
    setup_bars = segment_bars[:setup_minutes]
    continuation_bars = segment_bars[setup_minutes:]
    if not continuation_bars:
        return None

    pre_context = _slice_local_bars(
        local_bars=local_bars,
        local_times=local_times,
        start_dt=segment_start_dt - timedelta(minutes=pre_context_minutes),
        end_dt=segment_start_dt,
    )
    baseline_context = _slice_local_bars(
        local_bars=local_bars,
        local_times=local_times,
        start_dt=segment_start_dt - timedelta(minutes=pre_context_minutes + DEFAULT_BASELINE_CONTEXT_MINUTES),
        end_dt=segment_start_dt - timedelta(minutes=pre_context_minutes),
    )

    setup_high = max(bar.high for bar in setup_bars)
    setup_low = min(bar.low for bar in setup_bars)
    setup_range = max(setup_high - setup_low, 1e-9)
    setup_return = setup_bars[-1].close - setup_bars[0].open
    setup_green_share = sum(1 for bar in setup_bars if bar.close > bar.open) / len(setup_bars)
    setup_red_share = sum(1 for bar in setup_bars if bar.close < bar.open) / len(setup_bars)
    setup_pressure = sum(math.copysign(1.0, bar.close - bar.open) if bar.close != bar.open else 0.0 for bar in setup_bars) / len(setup_bars)
    pre_range = _bars_range(pre_context)
    baseline_range = _bars_range(baseline_context)
    pre_drift = _bars_drift(pre_context)
    pre_mean_abs_delta = _bars_mean_abs_delta(pre_context)
    setup_vwap = _bars_vwap(setup_bars)
    setup_volume_ratio = _volume_ratio(setup_bars, pre_context)
    segment_close_price = continuation_bars[-1].close

    long_trigger = _round_price(setup_high + tick_size)
    long_stop = _round_price(setup_low - tick_size)
    long_risk = _round_price(max(long_trigger - long_stop, tick_size))
    long_metrics = _direction_metrics(
        future_bars=continuation_bars,
        trigger_price=long_trigger,
        risk_points=long_risk,
        side="LONG",
    )

    short_trigger = _round_price(setup_low - tick_size)
    short_stop = _round_price(setup_high + tick_size)
    short_risk = _round_price(max(short_stop - short_trigger, tick_size))
    short_metrics = _direction_metrics(
        future_bars=continuation_bars,
        trigger_price=short_trigger,
        risk_points=short_risk,
        side="SHORT",
    )

    return GoldSegmentRegimeRow(
        symbol=symbol,
        trade_date=trade_day.isoformat(),
        segment_id=segment.segment_id,
        segment_start_ts=segment_start_dt.isoformat(),
        segment_end_ts=segment_end_dt.isoformat(),
        segment_bar_count=len(segment_bars),
        setup_bar_count=len(setup_bars),
        continuation_bar_count=len(continuation_bars),
        pre_context_bar_count=len(pre_context),
        baseline_context_bar_count=len(baseline_context),
        pre_context_range_points=_round_nullable(pre_range),
        pre_context_drift_points=_round_nullable(pre_drift),
        pre_context_compression_ratio=_round_nullable((pre_range / baseline_range) if pre_range is not None and baseline_range not in {None, 0.0} else None),
        pre_context_mean_abs_delta=_round_nullable(pre_mean_abs_delta),
        setup_range_points=_round_price(setup_range),
        setup_return_points=_round_price(setup_return),
        setup_abs_efficiency=_round_price(abs(setup_return) / setup_range),
        setup_close_location=_round_price((setup_bars[-1].close - setup_low) / setup_range),
        setup_vwap_displacement=_round_price((setup_bars[-1].close - setup_vwap) / setup_range),
        setup_volume_ratio=_round_nullable(setup_volume_ratio),
        setup_green_share=_round_price(setup_green_share),
        setup_red_share=_round_price(setup_red_share),
        setup_pressure=_round_price(setup_pressure),
        segment_close_price=_round_price(segment_close_price),
        long_trigger_price=long_trigger,
        long_stop_price=long_stop,
        long_risk_points=long_risk,
        long_triggered=long_metrics["triggered"],
        long_entry_ts=long_metrics["entry_ts"],
        long_close_pnl_points=long_metrics["close_pnl_points"],
        long_mfe_points=long_metrics["mfe_points"],
        long_mae_points=long_metrics["mae_points"],
        long_one_r_reached=long_metrics["one_r_reached"],
        long_positive_label=long_metrics["positive_label"],
        short_trigger_price=short_trigger,
        short_stop_price=short_stop,
        short_risk_points=short_risk,
        short_triggered=short_metrics["triggered"],
        short_entry_ts=short_metrics["entry_ts"],
        short_close_pnl_points=short_metrics["close_pnl_points"],
        short_mfe_points=short_metrics["mfe_points"],
        short_mae_points=short_metrics["mae_points"],
        short_one_r_reached=short_metrics["one_r_reached"],
        short_positive_label=short_metrics["positive_label"],
    )


def _direction_metrics(
    *,
    future_bars: list[ResearchBar],
    trigger_price: float,
    risk_points: float,
    side: str,
) -> dict[str, Any]:
    trigger_index: int | None = None
    for index, bar in enumerate(future_bars):
        if side == "LONG" and bar.high >= trigger_price:
            trigger_index = index
            break
        if side == "SHORT" and bar.low <= trigger_price:
            trigger_index = index
            break
    if trigger_index is None:
        return {
            "triggered": False,
            "entry_ts": None,
            "close_pnl_points": None,
            "mfe_points": None,
            "mae_points": None,
            "one_r_reached": False,
            "positive_label": False,
        }

    trade_window = future_bars[trigger_index:]
    close_price = trade_window[-1].close
    if side == "LONG":
        close_pnl = close_price - trigger_price
        mfe_points = max(bar.high for bar in trade_window) - trigger_price
        mae_points = max(0.0, trigger_price - min(bar.low for bar in trade_window))
    else:
        close_pnl = trigger_price - close_price
        mfe_points = trigger_price - min(bar.low for bar in trade_window)
        mae_points = max(0.0, max(bar.high for bar in trade_window) - trigger_price)
    one_r_reached = mfe_points >= risk_points
    return {
        "triggered": True,
        "entry_ts": trade_window[0].end_ts.isoformat(),
        "close_pnl_points": _round_price(close_pnl),
        "mfe_points": _round_price(max(0.0, mfe_points)),
        "mae_points": _round_price(max(0.0, mae_points)),
        "one_r_reached": one_r_reached,
        "positive_label": bool(one_r_reached and close_pnl > 0.0),
    }


def _apply_empirical_regime_scores(rows: list[GoldSegmentRegimeRow]) -> None:
    grouped: dict[tuple[str, str], dict[str, list[GoldSegmentRegimeRow]]] = defaultdict(lambda: {"LONG": [], "SHORT": []})
    for row in rows:
        grouped[(row.symbol, row.segment_id)]["LONG"].append(row)
        grouped[(row.symbol, row.segment_id)]["SHORT"].append(row)

    for (_, _), side_map in grouped.items():
        for side, group_rows in side_map.items():
            if not group_rows:
                continue
            outcome_attr = "long_positive_label" if side == "LONG" else "short_positive_label"
            base_rate = _smoothed_rate(sum(1 for row in group_rows if getattr(row, outcome_attr)), len(group_rows))
            contributions_by_row = [[] for _ in group_rows]
            for feature_name in SCORE_FEATURES:
                values = [getattr(row, feature_name) for row in group_rows]
                usable_values = [float(value) for value in values if value is not None]
                if len(usable_values) < 8 or min(usable_values) == max(usable_values):
                    continue
                buckets = _build_empirical_buckets(group_rows, feature_name=feature_name, outcome_attr=outcome_attr)
                if not buckets:
                    continue
                for index, row in enumerate(group_rows):
                    value = getattr(row, feature_name)
                    if value is None:
                        contributions_by_row[index].append(base_rate)
                        continue
                    contributions_by_row[index].append(_bucket_rate_for_value(buckets, float(value), fallback=base_rate))
            for index, row in enumerate(group_rows):
                score = statistics.fmean(contributions_by_row[index]) if contributions_by_row[index] else base_rate
                if side == "LONG":
                    row.long_regime_score = _round_price(score)
                else:
                    row.short_regime_score = _round_price(score)


def _build_empirical_buckets(
    rows: list[GoldSegmentRegimeRow],
    *,
    feature_name: str,
    outcome_attr: str,
    bucket_count: int = 4,
) -> list[EmpiricalBucket]:
    values = sorted({float(getattr(row, feature_name)) for row in rows if getattr(row, feature_name) is not None})
    if len(values) < 2:
        return []
    boundaries: list[float] = []
    for bucket_index in range(1, bucket_count):
        position = round((len(values) - 1) * bucket_index / bucket_count)
        boundaries.append(values[position])
    edges = [values[0], *sorted(set(boundaries)), values[-1]]
    buckets: list[EmpiricalBucket] = []
    for lower, upper in zip(edges, edges[1:], strict=False):
        bucket_rows = []
        for row in rows:
            value = getattr(row, feature_name)
            if value is None:
                continue
            numeric = float(value)
            is_last = upper == edges[-1]
            if lower <= numeric <= upper if is_last else lower <= numeric < upper:
                bucket_rows.append(row)
        if not bucket_rows:
            continue
        positive_count = sum(1 for row in bucket_rows if getattr(row, outcome_attr))
        buckets.append(
            EmpiricalBucket(
                lower_bound=lower,
                upper_bound=upper,
                sample_count=len(bucket_rows),
                positive_count=positive_count,
                positive_rate=_smoothed_rate(positive_count, len(bucket_rows)),
            )
        )
    return buckets


def _bucket_rate_for_value(buckets: list[EmpiricalBucket], value: float, *, fallback: float) -> float:
    for bucket in buckets[:-1]:
        if bucket.lower_bound <= value < bucket.upper_bound:
            return bucket.positive_rate
    last = buckets[-1] if buckets else None
    if last is not None and last.lower_bound <= value <= last.upper_bound:
        return last.positive_rate
    return fallback


def trade_date_for_timestamp(timestamp: datetime) -> date:
    local_dt = _local_timestamp(timestamp)
    if local_dt.timetz().replace(tzinfo=None) >= SESSION_RESET_TIME:
        return local_dt.date() + timedelta(days=1)
    return local_dt.date()


def label_gold_segment(timestamp: datetime) -> str | None:
    local_dt = _local_timestamp(timestamp)
    local_time = local_dt.timetz().replace(tzinfo=None)
    if time(18, 0) < local_time < time(19, 0):
        return "SESSION_OPEN"
    if time(19, 0) <= local_time < time(20, 30):
        return "ASIA_EARLY"
    if time(20, 30) <= local_time < time(23, 0):
        return "ASIA_LATE"
    if time(3, 0) <= local_time < time(5, 30):
        return "LONDON_EARLY"
    if time(5, 30) <= local_time < time(8, 20):
        return "LONDON_LATE"
    if time(8, 20) <= local_time < time(11, 0):
        return "US_EARLY"
    if time(11, 0) <= local_time < time(13, 30):
        return "US_MIDDAY"
    if time(13, 30) <= local_time < time(16, 0):
        return "US_LATE"
    return None


def _segment_window_for_trade_date(trade_day: date, segment: GoldSegmentDefinition) -> tuple[datetime, datetime]:
    base_day = trade_day - timedelta(days=1) if segment.start_time >= SESSION_RESET_TIME else trade_day
    start_dt = datetime.combine(base_day, segment.start_time, tzinfo=NEW_YORK)
    end_dt = datetime.combine(base_day, segment.end_time, tzinfo=NEW_YORK)
    if segment.end_time <= segment.start_time:
        end_dt += timedelta(days=1)
    return start_dt, end_dt


def _segment_by_id(segment_id: str) -> GoldSegmentDefinition | None:
    return next((segment for segment in SEGMENTS if segment.segment_id == segment_id), None)


def _slice_local_bars(
    *,
    local_bars: list[tuple[datetime, ResearchBar]],
    local_times: list[datetime],
    start_dt: datetime,
    end_dt: datetime,
) -> list[ResearchBar]:
    left = bisect.bisect_left(local_times, start_dt)
    right = bisect.bisect_left(local_times, end_dt)
    return [bar for _, bar in local_bars[left:right]]


def _bars_range(bars: list[ResearchBar]) -> float | None:
    if not bars:
        return None
    return max(bar.high for bar in bars) - min(bar.low for bar in bars)


def _bars_drift(bars: list[ResearchBar]) -> float | None:
    if not bars:
        return None
    return bars[-1].close - bars[0].open


def _bars_mean_abs_delta(bars: list[ResearchBar]) -> float | None:
    if len(bars) < 2:
        return None
    deltas = [abs(current.close - previous.close) for previous, current in zip(bars, bars[1:], strict=False)]
    return statistics.fmean(deltas) if deltas else None


def _bars_vwap(bars: list[ResearchBar]) -> float:
    weighted_close = sum(bar.close * max(bar.volume, 1) for bar in bars)
    total_volume = sum(max(bar.volume, 1) for bar in bars)
    return weighted_close / max(total_volume, 1)


def _volume_ratio(segment_setup: list[ResearchBar], pre_context: list[ResearchBar]) -> float | None:
    if not pre_context:
        return None
    setup_mean = statistics.fmean(max(bar.volume, 1) for bar in segment_setup)
    pre_mean = statistics.fmean(max(bar.volume, 1) for bar in pre_context)
    return setup_mean / max(pre_mean, 1e-9)


def _summarize_segment_rows(rows: list[GoldSegmentRegimeRow]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for segment in SEGMENTS:
        segment_rows = [row for row in rows if row.segment_id == segment.segment_id]
        if not segment_rows:
            continue
        summary[segment.segment_id] = {
            "description": segment.description,
            "sample_count": len(segment_rows),
            "long": _side_summary(segment_rows, side="LONG"),
            "short": _side_summary(segment_rows, side="SHORT"),
        }
    return summary


def _side_summary(rows: list[GoldSegmentRegimeRow], *, side: str) -> dict[str, Any]:
    triggered_attr = "long_triggered" if side == "LONG" else "short_triggered"
    positive_attr = "long_positive_label" if side == "LONG" else "short_positive_label"
    pnl_attr = "long_close_pnl_points" if side == "LONG" else "short_close_pnl_points"
    mfe_attr = "long_mfe_points" if side == "LONG" else "short_mfe_points"
    mae_attr = "long_mae_points" if side == "LONG" else "short_mae_points"
    score_attr = "long_regime_score" if side == "LONG" else "short_regime_score"
    triggered_rows = [row for row in rows if getattr(row, triggered_attr)]
    pnl_values = [getattr(row, pnl_attr) for row in triggered_rows if getattr(row, pnl_attr) is not None]
    mfe_values = [getattr(row, mfe_attr) for row in triggered_rows if getattr(row, mfe_attr) is not None]
    mae_values = [getattr(row, mae_attr) for row in triggered_rows if getattr(row, mae_attr) is not None]
    score_values = [getattr(row, score_attr) for row in rows if getattr(row, score_attr) is not None]
    positive_count = sum(1 for row in rows if getattr(row, positive_attr))
    return {
        "sample_count": len(rows),
        "trigger_count": len(triggered_rows),
        "trigger_rate": _round_price(len(triggered_rows) / len(rows)),
        "positive_count": positive_count,
        "positive_rate": _round_price(_smoothed_rate(positive_count, len(rows))),
        "avg_close_pnl_points": _round_nullable(statistics.fmean(pnl_values) if pnl_values else None),
        "avg_mfe_points": _round_nullable(statistics.fmean(mfe_values) if mfe_values else None),
        "avg_mae_points": _round_nullable(statistics.fmean(mae_values) if mae_values else None),
        "avg_regime_score": _round_nullable(statistics.fmean(score_values) if score_values else None),
    }


def _top_regime_cells(rows: list[GoldSegmentRegimeRow], *, min_samples: int = 8) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for segment in SEGMENTS:
        segment_rows = [row for row in rows if row.segment_id == segment.segment_id]
        if len(segment_rows) < min_samples:
            continue
        for side in ("LONG", "SHORT"):
            side_summary = _side_summary(segment_rows, side=side)
            if side_summary["sample_count"] < min_samples:
                continue
            ranked.append(
                {
                    "segment_id": segment.segment_id,
                    "side": side,
                    **side_summary,
                }
            )
    ranked.sort(
        key=lambda item: (
            float(item["positive_rate"] or 0.0),
            float(item["avg_close_pnl_points"] or -9999.0),
            float(item["avg_regime_score"] or 0.0),
        ),
        reverse=True,
    )
    return ranked[:6]


def _write_rows_csv(path: Path, rows: list[GoldSegmentRegimeRow]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return path
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(asdict(rows[0]).keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(_json_ready(asdict(row)))
    return path


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# GC/MGC Segment Regime Research",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Database: `{report['database_path']}`",
        f"- Symbols: `{', '.join(report['symbols'])}`",
        f"- Dataset rows: `{report['row_count']}`",
        "",
        "## Segment Windows",
        "",
    ]
    for segment in report["definition"]["segment_windows"]:
        lines.append(
            f"- `{segment['segment_id']}`: `{segment['start_time']}` to `{segment['end_time']}` ET. {segment['description']}"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
        ]
    )
    for note in report["definition"]["assumptions"]:
        lines.append(f"- {note}")
    lines.extend(["", "## Symbol Summary", ""])
    for symbol, payload in report["symbol_reports"].items():
        lines.append(f"### {symbol}")
        lines.append("")
        lines.append(f"- 1m bars loaded: `{payload['bar_count_1m']}`")
        lines.append(f"- Segment rows: `{payload['row_count']}`")
        lines.append(f"- Trade dates covered: `{payload['trade_date_count']}`")
        top_cells = payload.get("top_regime_cells", [])
        if top_cells:
            lines.append("- Top regime cells:")
            for cell in top_cells:
                lines.append(
                    f"  - `{cell['segment_id']} {cell['side']}`: positive rate `{cell['positive_rate']}`, avg close pnl `{cell['avg_close_pnl_points']}`"
                )
        lines.append("")
    return "\n".join(lines)


def _resolve_sqlite_database_path(database_url: str) -> Path:
    if not database_url.startswith("sqlite:///"):
        raise ValueError(f"Unsupported database URL for research load: {database_url}")
    return Path(database_url.removeprefix("sqlite:///")).resolve()


def _load_start_timestamp(start_day: date | None) -> datetime | None:
    if start_day is None:
        return None
    return datetime.combine(start_day - timedelta(days=1), SESSION_RESET_TIME, tzinfo=NEW_YORK)


def _load_end_timestamp(end_day: date | None) -> datetime | None:
    if end_day is None:
        return None
    return datetime.combine(end_day, time(14, 0), tzinfo=NEW_YORK)


def _parse_date(value: str | date | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _local_timestamp(timestamp: datetime) -> datetime:
    return timestamp.astimezone(NEW_YORK) if timestamp.tzinfo is not None else timestamp.replace(tzinfo=NEW_YORK)


def _smoothed_rate(positive_count: int, sample_count: int) -> float:
    return (positive_count + 1.0) / (sample_count + 2.0)


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
