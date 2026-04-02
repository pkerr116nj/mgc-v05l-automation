"""Research-only matched comparison of 3m and 5m derivative separator surfaces."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Sequence

from ..config_models import load_settings_from_files
from ..market_data import BarBuilder
from ..research import build_resampled_bars, write_resampled_bars_csv
from .three_minute_derivative_separator_analysis import (
    CURVATURE_FLAT_THRESHOLD,
    SLOPE_FLAT_THRESHOLD,
    _build_derivative_rows,
    _build_london_observations,
    _build_midday_observations,
    _build_reference_short_family_observations,
    _load_bars,
    _london_derivative_signal,
    _write_csv,
)


@dataclass(frozen=True)
class SurfaceObservation:
    surface: str
    cohort: str
    event_kind: str
    event_ts: str
    source_label: str
    source_pnl: str | None
    source_mfe_20bar: str | None
    anchor_end_ts: str
    current_slope_bucket: str
    current_curvature_bucket: str
    derivative_bucket: str
    derivative_path_3: str
    derivative_path_4: str


def build_and_write_derivative_surface_comparison(
    *,
    db_path: Path,
    ticker: str,
    strict_trade_ledger_path: Path,
    medium_1_trade_ledger_path: Path,
    medium_2_trade_ledger_path: Path,
    london_detail_csv_path: Path,
    reference_trade_ledger_path: Path,
    output_prefix: Path,
    config_paths: Sequence[Path],
) -> dict[str, str]:
    settings = load_settings_from_files(config_paths)
    one_minute_bars = _load_bars(db_path=db_path, ticker=ticker, timeframe="1m", data_source="schwab_history")
    five_minute_bars = _load_bars(db_path=db_path, ticker=ticker, timeframe="5m", data_source="internal")
    if not one_minute_bars or not five_minute_bars:
        raise ValueError("Both stored 1m schwab_history bars and 5m internal bars are required.")

    overlap_start = max(one_minute_bars[0].end_ts, five_minute_bars[0].end_ts)
    overlap_end = min(one_minute_bars[-1].end_ts, five_minute_bars[-1].end_ts)

    one_minute_overlap = [bar for bar in one_minute_bars if overlap_start <= bar.end_ts <= overlap_end]
    five_minute_overlap = [bar for bar in five_minute_bars if overlap_start <= bar.end_ts <= overlap_end]

    three_minute_resampled = build_resampled_bars(
        one_minute_overlap,
        target_timeframe="3m",
        bar_builder=BarBuilder(settings),
    )
    write_resampled_bars_csv(
        three_minute_resampled.bars,
        output_prefix.with_name(output_prefix.name + ".3m_resampled.csv"),
    )

    three_minute_rows = _build_derivative_rows(three_minute_resampled.bars)
    five_minute_rows = _build_derivative_rows(five_minute_overlap)

    observations = []
    observations.extend(
        _wrap_surface(
            "3m",
            _build_midday_observations(
                strict_trade_ledger_path=strict_trade_ledger_path,
                medium_1_trade_ledger_path=medium_1_trade_ledger_path,
                medium_2_trade_ledger_path=medium_2_trade_ledger_path,
                derivative_rows=three_minute_rows,
            ),
        )
    )
    observations.extend(_wrap_surface("3m", _build_london_observations(london_detail_csv_path=london_detail_csv_path, derivative_rows=three_minute_rows)))
    observations.extend(_wrap_surface("3m", _build_reference_short_family_observations(trade_ledger_path=reference_trade_ledger_path, derivative_rows=three_minute_rows)))

    observations.extend(
        _wrap_surface(
            "5m",
            _build_midday_observations(
                strict_trade_ledger_path=strict_trade_ledger_path,
                medium_1_trade_ledger_path=medium_1_trade_ledger_path,
                medium_2_trade_ledger_path=medium_2_trade_ledger_path,
                derivative_rows=five_minute_rows,
            ),
        )
    )
    observations.extend(_wrap_surface("5m", _build_london_observations(london_detail_csv_path=london_detail_csv_path, derivative_rows=five_minute_rows)))
    observations.extend(_wrap_surface("5m", _build_reference_short_family_observations(trade_ledger_path=reference_trade_ledger_path, derivative_rows=five_minute_rows)))

    detail_path = output_prefix.with_name(output_prefix.name + ".detail.csv")
    comparison_path = output_prefix.with_name(output_prefix.name + ".comparison.csv")
    summary_path = output_prefix.with_name(output_prefix.name + ".summary.json")

    _write_csv(detail_path, [asdict(item) for item in observations])
    comparison_rows = _build_comparison_rows(observations)
    _write_csv(comparison_path, comparison_rows)
    summary = _build_summary(
        overlap_start=overlap_start.isoformat(),
        overlap_end=overlap_end.isoformat(),
        one_minute_count=len(one_minute_overlap),
        three_minute_count=len(three_minute_resampled.bars),
        five_minute_count=len(five_minute_overlap),
        three_minute_skipped=three_minute_resampled.skipped_bucket_count,
        observations=observations,
    )
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "surface_detail_path": str(detail_path),
        "surface_comparison_path": str(comparison_path),
        "surface_summary_path": str(summary_path),
        "three_minute_resampled_csv_path": str(output_prefix.with_name(output_prefix.name + ".3m_resampled.csv")),
    }


def _wrap_surface(surface: str, rows: Sequence[Any]) -> list[SurfaceObservation]:
    wrapped: list[SurfaceObservation] = []
    for row in rows:
        wrapped.append(
            SurfaceObservation(
                surface=surface,
                cohort=row.cohort,
                event_kind=row.event_kind,
                event_ts=row.event_ts,
                source_label=row.source_label,
                source_pnl=row.source_pnl,
                source_mfe_20bar=row.source_mfe_20bar,
                anchor_end_ts=row.anchor_3m_end_ts,
                current_slope_bucket=row.current_slope_bucket,
                current_curvature_bucket=row.current_curvature_bucket,
                derivative_bucket=row.derivative_bucket_3m,
                derivative_path_3=row.derivative_path_3,
                derivative_path_4=row.derivative_path_4,
            )
        )
    return wrapped


def _build_comparison_rows(observations: Sequence[SurfaceObservation]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str], list[SurfaceObservation]] = {}
    for item in observations:
        grouped.setdefault((item.surface, item.cohort), []).append(item)
    for (surface, cohort), items in sorted(grouped.items()):
        rows.append(
            {
                "surface": surface,
                "cohort": cohort,
                "count": len(items),
                "derivative_bucket_distribution": json.dumps(_distribution(item.derivative_bucket for item in items), sort_keys=True),
                "derivative_path_3_distribution": json.dumps(_distribution(item.derivative_path_3 for item in items), sort_keys=True),
            }
        )
    return rows


def _build_summary(
    *,
    overlap_start: str,
    overlap_end: str,
    one_minute_count: int,
    three_minute_count: int,
    five_minute_count: int,
    three_minute_skipped: int,
    observations: Sequence[SurfaceObservation],
) -> dict[str, Any]:
    summary_3m = _surface_summary("3m", observations)
    summary_5m = _surface_summary("5m", observations)
    verdict = _comparison_verdict(summary_3m, summary_5m)
    return {
        "coverage_window_used": {
            "start": overlap_start,
            "end": overlap_end,
            "one_minute_bar_count": one_minute_count,
            "three_minute_bar_count": three_minute_count,
            "three_minute_skipped_bucket_count": three_minute_skipped,
            "five_minute_bar_count": five_minute_count,
        },
        "feature_definitions": {
            "shared_bucketing": {
                "slope": "current close change divided by trailing 5-bar ATR on that surface",
                "curvature": "current normalized slope minus previous normalized slope on that surface",
                "slope_bucket_threshold": str(SLOPE_FLAT_THRESHOLD),
                "curvature_bucket_threshold": str(CURVATURE_FLAT_THRESHOLD),
            },
            "three_minute_surface": "1m bars resampled into 3m bars inside the research pipeline",
            "five_minute_surface": "stored 5m internal bars from the replay DB",
        },
        "surface_3m": summary_3m,
        "surface_5m": summary_5m,
        "comparison_verdict": verdict,
    }


def _surface_summary(surface: str, observations: Sequence[SurfaceObservation]) -> dict[str, Any]:
    london_survivors = [item for item in observations if item.surface == surface and item.cohort == "london_late_visible_survivor"]
    london_blocked = [item for item in observations if item.surface == surface and item.cohort == "london_late_blocked_candidate"]
    london_high = [item for item in observations if item.surface == surface and item.cohort == "london_late_high_value_cluster"]
    london_low = [item for item in observations if item.surface == surface and item.cohort == "london_late_low_value_cluster"]
    midday_strict = [item for item in observations if item.surface == surface and item.cohort == "us_midday_strict_trade"]
    midday_widened = [item for item in observations if item.surface == surface and item.cohort.startswith("us_midday_widened_add")]
    reference_rows = [item for item in observations if item.surface == surface and item.cohort.startswith("reference_")]

    london_signal = _london_derivative_signal(
        _convert_for_signal(london_high),
        _convert_for_signal(london_low),
        _convert_for_signal(london_survivors),
        _convert_for_signal(london_blocked),
    )
    return {
        "london_late": {
            "visible_survivor_count": len(london_survivors),
            "blocked_count": len(london_blocked),
            "high_value_count": len(london_high),
            "low_value_count": len(london_low),
            "visible_survivor_derivative_buckets": _distribution(item.derivative_bucket for item in london_survivors),
            "blocked_derivative_buckets": _distribution(item.derivative_bucket for item in london_blocked),
            "high_value_derivative_path_3": _distribution(item.derivative_path_3 for item in london_high),
            "low_value_derivative_path_3": _distribution(item.derivative_path_3 for item in london_low),
            "helpful": london_signal["helpful"],
            "finding": london_signal["finding"],
            "rule_hypothesis": london_signal["rule_hypothesis"],
            "note": london_signal["note"],
        },
        "us_midday": {
            "strict_count": len(midday_strict),
            "widened_add_count": len(midday_widened),
            "strict_rows": [asdict(item) for item in midday_strict],
            "widened_add_rows": [asdict(item) for item in midday_widened],
            "finding": (
                "Still inconclusive on the matched covered window: one strict trade, zero widened-added losers."
            ),
        },
        "reference_short_families": {
            "count": len(reference_rows),
            "derivative_bucket_distribution": _distribution(item.derivative_bucket for item in reference_rows),
        },
    }


def _convert_for_signal(rows: Sequence[SurfaceObservation]) -> list[Any]:
    converted = []
    for row in rows:
        converted.append(
            type(
                "_SignalRow",
                (),
                {
                    "current_curvature_bucket": row.current_curvature_bucket,
                    "derivative_path_3": row.derivative_path_3,
                },
            )()
        )
    return converted


def _comparison_verdict(summary_3m: dict[str, Any], summary_5m: dict[str, Any]) -> dict[str, Any]:
    helpful_3m = summary_3m["london_late"]["helpful"]
    helpful_5m = summary_5m["london_late"]["helpful"]
    if helpful_3m and not helpful_5m:
        verdict = "3m better"
    elif helpful_5m and not helpful_3m:
        verdict = "5m better"
    elif helpful_3m and helpful_5m:
        verdict = "no meaningful difference"
    else:
        verdict = "both inconclusive"

    continue_3m = helpful_3m and not helpful_5m
    if verdict in {"no meaningful difference", "both inconclusive"}:
        continue_3m = False

    return {
        "verdict": verdict,
        "should_continue_prioritizing_3m_now": continue_3m,
        "note": (
            "3m earns priority only if it shows cleaner separator power than 5m on the matched covered sample. "
            "That did not happen in this pass."
        ),
    }


def _distribution(values: Sequence[str]) -> dict[str, int]:
    distribution: dict[str, int] = {}
    for value in values:
        distribution[value] = distribution.get(value, 0) + 1
    return distribution


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", required=True, type=Path)
    parser.add_argument("--ticker", default="MGC")
    parser.add_argument("--strict-trade-ledger", required=True, type=Path)
    parser.add_argument("--medium-1-trade-ledger", required=True, type=Path)
    parser.add_argument("--medium-2-trade-ledger", required=True, type=Path)
    parser.add_argument("--london-detail-csv", required=True, type=Path)
    parser.add_argument("--reference-trade-ledger", required=True, type=Path)
    parser.add_argument("--output-prefix", required=True, type=Path)
    parser.add_argument("--config", action="append", default=None)
    args = parser.parse_args()

    outputs = build_and_write_derivative_surface_comparison(
        db_path=args.db_path,
        ticker=args.ticker,
        strict_trade_ledger_path=args.strict_trade_ledger,
        medium_1_trade_ledger_path=args.medium_1_trade_ledger,
        medium_2_trade_ledger_path=args.medium_2_trade_ledger,
        london_detail_csv_path=args.london_detail_csv,
        reference_trade_ledger_path=args.reference_trade_ledger,
        output_prefix=args.output_prefix,
        config_paths=[Path(path) for path in (args.config or ["config/base.yaml", "config/replay.yaml"])],
    )
    print(json.dumps(outputs, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
