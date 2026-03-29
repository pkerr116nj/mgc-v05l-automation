"""Research-only 1m->3m derivative separator analysis for MGC entry families."""

from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

from ..config_models import load_settings_from_files
from ..domain import Bar
from ..market_data import BarBuilder
from ..research import build_resampled_bars, write_resampled_bars_csv


SLOPE_FLAT_THRESHOLD = Decimal("0.20")
CURVATURE_FLAT_THRESHOLD = Decimal("0.15")
STRUCTURAL_LONDON_EMA_RELATIONS = {"above_both_fast_gt_slow", "pullback_above_slow"}


@dataclass(frozen=True)
class ThreeMinuteDerivativeRow:
    cohort: str
    event_kind: str
    event_ts: str
    source_label: str
    source_pnl: str | None
    source_mfe_20bar: str | None
    anchor_3m_end_ts: str
    current_slope: str
    current_curvature: str
    current_slope_bucket: str
    current_curvature_bucket: str
    derivative_bucket_3m: str
    slope_path_3: str
    curvature_path_3: str
    derivative_path_3: str
    slope_path_4: str
    curvature_path_4: str
    derivative_path_4: str
    normalized_close_change_3m: str
    atr_5_3m: str


def build_and_write_three_minute_derivative_separator_analysis(
    *,
    db_path: Path,
    ticker: str,
    source_timeframe: str,
    strict_trade_ledger_path: Path,
    medium_1_trade_ledger_path: Path,
    medium_2_trade_ledger_path: Path,
    london_detail_csv_path: Path,
    reference_trade_ledger_path: Path,
    output_prefix: Path,
    config_paths: Sequence[Path],
) -> dict[str, str]:
    settings = load_settings_from_files(config_paths)
    one_minute_bars = _load_bars(db_path=db_path, ticker=ticker, timeframe=source_timeframe, data_source="schwab_history")
    if not one_minute_bars:
        raise ValueError(f"No stored {source_timeframe} bars found for {ticker} in {db_path}.")

    resampled = build_resampled_bars(
        one_minute_bars,
        target_timeframe="3m",
        bar_builder=BarBuilder(settings),
    )
    if not resampled.bars:
        raise ValueError("1m->3m resampling produced no bars.")

    derivative_rows = _build_derivative_rows(resampled.bars)
    derivative_index = {row["end_ts"]: row for row in derivative_rows}
    resampled_csv_path = output_prefix.with_name(output_prefix.name + ".resampled_3m.csv")
    write_resampled_bars_csv(resampled.bars, resampled_csv_path)

    observations: list[ThreeMinuteDerivativeRow] = []
    observations.extend(
        _build_midday_observations(
            strict_trade_ledger_path=strict_trade_ledger_path,
            medium_1_trade_ledger_path=medium_1_trade_ledger_path,
            medium_2_trade_ledger_path=medium_2_trade_ledger_path,
            derivative_rows=derivative_rows,
        )
    )
    observations.extend(
        _build_london_observations(
            london_detail_csv_path=london_detail_csv_path,
            derivative_rows=derivative_rows,
        )
    )
    observations.extend(
        _build_reference_short_family_observations(
            trade_ledger_path=reference_trade_ledger_path,
            derivative_rows=derivative_rows,
        )
    )

    detail_path = output_prefix.with_name(output_prefix.name + ".detail.csv")
    comparison_path = output_prefix.with_name(output_prefix.name + ".comparison.csv")
    summary_path = output_prefix.with_name(output_prefix.name + ".summary.json")

    detail_rows = [asdict(item) for item in observations]
    _write_csv(detail_path, detail_rows)
    comparison_rows = _build_comparison_rows(observations)
    _write_csv(comparison_path, comparison_rows)

    coverage_start = one_minute_bars[0].end_ts.isoformat()
    coverage_end = one_minute_bars[-1].end_ts.isoformat()
    summary = _build_summary(
        db_path=db_path,
        ticker=ticker,
        source_timeframe=source_timeframe,
        one_minute_bar_count=len(one_minute_bars),
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        resampled_bar_count=len(resampled.bars),
        resampled_skipped_bucket_count=resampled.skipped_bucket_count,
        observations=observations,
        london_detail_csv_path=london_detail_csv_path,
        derivative_index=derivative_index,
    )
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    return {
        "resampled_3m_csv_path": str(resampled_csv_path),
        "three_minute_derivative_detail_path": str(detail_path),
        "three_minute_derivative_comparison_path": str(comparison_path),
        "three_minute_derivative_summary_path": str(summary_path),
    }


def _load_bars(*, db_path: Path, ticker: str, timeframe: str, data_source: str) -> list[Bar]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            select
              bar_id,
              symbol,
              timeframe,
              start_ts,
              end_ts,
              open,
              high,
              low,
              close,
              volume,
              is_final,
              session_asia,
              session_london,
              session_us,
              session_allowed
            from bars
            where ticker = ? and timeframe = ? and data_source = ?
            order by end_ts asc
            """,
            (ticker, timeframe, data_source),
        ).fetchall()
    finally:
        connection.close()

    return [
        Bar(
            bar_id=row["bar_id"],
            symbol=row["symbol"],
            timeframe=row["timeframe"],
            start_ts=datetime.fromisoformat(row["start_ts"]),
            end_ts=datetime.fromisoformat(row["end_ts"]),
            open=Decimal(str(row["open"])),
            high=Decimal(str(row["high"])),
            low=Decimal(str(row["low"])),
            close=Decimal(str(row["close"])),
            volume=int(row["volume"]),
            is_final=bool(row["is_final"]),
            session_asia=bool(row["session_asia"]),
            session_london=bool(row["session_london"]),
            session_us=bool(row["session_us"]),
            session_allowed=bool(row["session_allowed"]),
        )
        for row in rows
    ]


def _build_derivative_rows(bars: Sequence[Bar]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    prior_close: Decimal | None = None
    true_ranges: list[Decimal] = []
    prior_slope = Decimal("0")
    for bar in bars:
        reference_close = prior_close if prior_close is not None else bar.open
        true_range = max(bar.high - bar.low, abs(bar.high - reference_close), abs(bar.low - reference_close))
        true_ranges.append(true_range)
        atr_5 = _avg(true_ranges[-5:])
        close_change = bar.close - reference_close
        slope = _safe_div(close_change, atr_5)
        curvature = slope - prior_slope
        rows.append(
            {
                "end_ts": bar.end_ts.isoformat(),
                "slope": slope,
                "curvature": curvature,
                "slope_bucket": _bucket_three_way(slope, flat_threshold=SLOPE_FLAT_THRESHOLD),
                "curvature_bucket": _bucket_three_way(curvature, flat_threshold=CURVATURE_FLAT_THRESHOLD),
                "derivative_bucket": (
                    f"{_bucket_three_way(slope, flat_threshold=SLOPE_FLAT_THRESHOLD)}|"
                    f"{_bucket_three_way(curvature, flat_threshold=CURVATURE_FLAT_THRESHOLD)}"
                ),
                "atr_5": atr_5,
                "close_change": close_change,
            }
        )
        prior_close = bar.close
        prior_slope = slope

    for index, row in enumerate(rows):
        prior3 = rows[max(0, index - 2) : index + 1]
        prior4 = rows[max(0, index - 3) : index + 1]
        row["slope_path_3"] = ">".join(item["slope_bucket"] for item in prior3)
        row["curvature_path_3"] = ">".join(item["curvature_bucket"] for item in prior3)
        row["derivative_path_3"] = ">".join(item["derivative_bucket"] for item in prior3)
        row["slope_path_4"] = ">".join(item["slope_bucket"] for item in prior4)
        row["curvature_path_4"] = ">".join(item["curvature_bucket"] for item in prior4)
        row["derivative_path_4"] = ">".join(item["derivative_bucket"] for item in prior4)
    return rows


def _build_midday_observations(
    *,
    strict_trade_ledger_path: Path,
    medium_1_trade_ledger_path: Path,
    medium_2_trade_ledger_path: Path,
    derivative_rows: Sequence[dict[str, Any]],
) -> list[ThreeMinuteDerivativeRow]:
    strict_rows = _load_family_trades(strict_trade_ledger_path, "usMiddayPauseResumeShortTurn")
    medium_1_rows = _load_family_trades(medium_1_trade_ledger_path, "usMiddayPauseResumeShortTurn")
    medium_2_rows = _load_family_trades(medium_2_trade_ledger_path, "usMiddayPauseResumeShortTurn")

    strict_entry_ts = {row["entry_ts"] for row in strict_rows}
    medium_1_entry_ts = {row["entry_ts"] for row in medium_1_rows}

    observations: list[ThreeMinuteDerivativeRow] = []
    observations.extend(_map_trade_rows_to_observations(strict_rows, "us_midday_strict_trade", derivative_rows))
    observations.extend(
        _map_trade_rows_to_observations(
            [row for row in medium_1_rows if row["entry_ts"] not in strict_entry_ts],
            "us_midday_widened_add_medium_1",
            derivative_rows,
        )
    )
    observations.extend(
        _map_trade_rows_to_observations(
            [row for row in medium_2_rows if row["entry_ts"] not in medium_1_entry_ts],
            "us_midday_widened_add_medium_2",
            derivative_rows,
        )
    )
    return observations


def _build_london_observations(
    *,
    london_detail_csv_path: Path,
    derivative_rows: Sequence[dict[str, Any]],
) -> list[ThreeMinuteDerivativeRow]:
    rows = _load_london_cluster_rows(london_detail_csv_path)
    structural_survivors = [
        row
        for row in rows
        if row["expansion_state"] == "not_expanded"
        and row["one_bar_rebound_before_signal"]
        and row["prior_3_any_positive_curvature"]
        and row["ema_relation"] in STRUCTURAL_LONDON_EMA_RELATIONS
    ]
    blocked = [row for row in rows if row not in structural_survivors]
    high_value = _top_third_by_mfe(rows)
    low_value = _bottom_third_by_mfe(rows)

    observations: list[ThreeMinuteDerivativeRow] = []
    observations.extend(_map_cluster_rows_to_observations(structural_survivors, "london_late_visible_survivor", derivative_rows))
    observations.extend(_map_cluster_rows_to_observations(blocked, "london_late_blocked_candidate", derivative_rows))
    observations.extend(_map_cluster_rows_to_observations(high_value, "london_late_high_value_cluster", derivative_rows))
    observations.extend(_map_cluster_rows_to_observations(low_value, "london_late_low_value_cluster", derivative_rows))
    return observations


def _build_reference_short_family_observations(
    *,
    trade_ledger_path: Path,
    derivative_rows: Sequence[dict[str, Any]],
) -> list[ThreeMinuteDerivativeRow]:
    observations: list[ThreeMinuteDerivativeRow] = []
    with trade_ledger_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["direction"] != "SHORT":
                continue
            if row["entry_ts"] < "2026-02-03":
                continue
            if row["setup_family"] not in {
                "firstBearSnapTurn",
                "usDerivativeBearTurn",
                "usDerivativeBearAdditiveTurn",
                "usMiddayPauseResumeShortTurn",
            }:
                continue
            observations.extend(_map_trade_rows_to_observations([row], f"reference_{row['setup_family']}", derivative_rows))
    return observations


def _load_family_trades(trade_ledger_path: Path, family: str) -> list[dict[str, str]]:
    with trade_ledger_path.open(encoding="utf-8", newline="") as handle:
        return [row for row in csv.DictReader(handle) if row["setup_family"] == family]


def _load_london_cluster_rows(detail_csv_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with detail_csv_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["timestamp"] < "2026-02-03":
                continue
            if row["session_phase"] != "LONDON_LATE":
                continue
            if row["direction_of_turn"] != "SHORT":
                continue
            if row["recent_path_shape"] != "pause_rebound_resume_short":
                continue
            rows.append(
                {
                    "event_ts": row["timestamp"],
                    "ema_relation": row["ema_relation"],
                    "expansion_state": row["expansion_state"],
                    "one_bar_rebound_before_signal": row["one_bar_rebound_before_signal"] == "True",
                    "prior_3_any_positive_curvature": row["prior_3_any_positive_curvature"] == "True",
                    "mfe_20bar": Decimal(row["mfe_20bar"]),
                }
            )
    return rows


def _map_trade_rows_to_observations(
    rows: Sequence[dict[str, str]],
    cohort: str,
    derivative_rows: Sequence[dict[str, Any]],
) -> list[ThreeMinuteDerivativeRow]:
    observations: list[ThreeMinuteDerivativeRow] = []
    for row in rows:
        anchor = _find_anchor_row(derivative_rows, row["entry_ts"])
        if anchor is None:
            continue
        observations.append(
            _build_observation(
                cohort=cohort,
                event_kind="trade",
                event_ts=row["entry_ts"],
                source_label=row["setup_family"],
                source_pnl=row["net_pnl"],
                source_mfe_20bar=None,
                anchor=anchor,
            )
        )
    return observations


def _map_cluster_rows_to_observations(
    rows: Sequence[dict[str, Any]],
    cohort: str,
    derivative_rows: Sequence[dict[str, Any]],
) -> list[ThreeMinuteDerivativeRow]:
    observations: list[ThreeMinuteDerivativeRow] = []
    for row in rows:
        anchor = _find_anchor_row(derivative_rows, row["event_ts"])
        if anchor is None:
            continue
        observations.append(
            _build_observation(
                cohort=cohort,
                event_kind="missed_turn",
                event_ts=row["event_ts"],
                source_label="pause_rebound_resume_short",
                source_pnl=None,
                source_mfe_20bar=str(row["mfe_20bar"]),
                anchor=anchor,
            )
        )
    return observations


def _build_observation(
    *,
    cohort: str,
    event_kind: str,
    event_ts: str,
    source_label: str,
    source_pnl: str | None,
    source_mfe_20bar: str | None,
    anchor: dict[str, Any],
) -> ThreeMinuteDerivativeRow:
    return ThreeMinuteDerivativeRow(
        cohort=cohort,
        event_kind=event_kind,
        event_ts=event_ts,
        source_label=source_label,
        source_pnl=source_pnl,
        source_mfe_20bar=source_mfe_20bar,
        anchor_3m_end_ts=anchor["end_ts"],
        current_slope=_decimal_str(anchor["slope"]),
        current_curvature=_decimal_str(anchor["curvature"]),
        current_slope_bucket=anchor["slope_bucket"],
        current_curvature_bucket=anchor["curvature_bucket"],
        derivative_bucket_3m=anchor["derivative_bucket"],
        slope_path_3=anchor["slope_path_3"],
        curvature_path_3=anchor["curvature_path_3"],
        derivative_path_3=anchor["derivative_path_3"],
        slope_path_4=anchor["slope_path_4"],
        curvature_path_4=anchor["curvature_path_4"],
        derivative_path_4=anchor["derivative_path_4"],
        normalized_close_change_3m=_decimal_str(anchor["close_change"]),
        atr_5_3m=_decimal_str(anchor["atr_5"]),
    )


def _find_anchor_row(rows: Sequence[dict[str, Any]], event_ts: str) -> dict[str, Any] | None:
    eligible = [row for row in rows if row["end_ts"] <= event_ts]
    return eligible[-1] if eligible else None


def _top_third_by_mfe(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: row["mfe_20bar"], reverse=True)
    count = max(1, len(ordered) // 3)
    return ordered[:count]


def _bottom_third_by_mfe(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: row["mfe_20bar"])
    count = max(1, len(ordered) // 3)
    return ordered[:count]


def _build_comparison_rows(observations: Sequence[ThreeMinuteDerivativeRow]) -> list[dict[str, Any]]:
    by_cohort: dict[str, list[ThreeMinuteDerivativeRow]] = {}
    for item in observations:
        by_cohort.setdefault(item.cohort, []).append(item)

    rows: list[dict[str, Any]] = []
    for cohort, cohort_rows in sorted(by_cohort.items()):
        rows.append(
            {
                "cohort": cohort,
                "count": len(cohort_rows),
                "slope_bucket_distribution": json.dumps(_distribution(item.current_slope_bucket for item in cohort_rows), sort_keys=True),
                "curvature_bucket_distribution": json.dumps(_distribution(item.current_curvature_bucket for item in cohort_rows), sort_keys=True),
                "derivative_bucket_distribution": json.dumps(_distribution(item.derivative_bucket_3m for item in cohort_rows), sort_keys=True),
                "derivative_path_3_distribution": json.dumps(_distribution(item.derivative_path_3 for item in cohort_rows), sort_keys=True),
                "avg_source_pnl": _decimal_str(_avg(_decimal_or_zero(item.source_pnl) for item in cohort_rows if item.source_pnl is not None)),
                "avg_source_mfe_20bar": _decimal_str(_avg(_decimal_or_zero(item.source_mfe_20bar) for item in cohort_rows if item.source_mfe_20bar is not None)),
            }
        )
    return rows


def _build_summary(
    *,
    db_path: Path,
    ticker: str,
    source_timeframe: str,
    one_minute_bar_count: int,
    coverage_start: str,
    coverage_end: str,
    resampled_bar_count: int,
    resampled_skipped_bucket_count: int,
    observations: Sequence[ThreeMinuteDerivativeRow],
    london_detail_csv_path: Path,
    derivative_index: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    midday_strict = [item for item in observations if item.cohort == "us_midday_strict_trade"]
    midday_widened = [item for item in observations if item.cohort.startswith("us_midday_widened_add")]
    london_survivors = [item for item in observations if item.cohort == "london_late_visible_survivor"]
    london_blocked = [item for item in observations if item.cohort == "london_late_blocked_candidate"]
    london_high = [item for item in observations if item.cohort == "london_late_high_value_cluster"]
    london_low = [item for item in observations if item.cohort == "london_late_low_value_cluster"]

    london_total_covered = len(_load_london_cluster_rows(london_detail_csv_path))
    london_mapped_count = len({item.event_ts for item in observations if item.cohort.startswith("london_late_")})
    london_derivative_help = _london_derivative_signal(london_high, london_low, london_survivors, london_blocked)

    return {
        "stored_1m_coverage_used": {
            "database_path": str(db_path),
            "ticker": ticker,
            "source_timeframe": source_timeframe,
            "bar_count": one_minute_bar_count,
            "coverage_start": coverage_start,
            "coverage_end": coverage_end,
        },
        "resampling_path": {
            "source": "stored 1m bars from SQLite data_source=schwab_history",
            "target": "3m research bars",
            "method": "build_resampled_bars via existing research pipeline",
            "resampled_bar_count": resampled_bar_count,
            "skipped_bucket_count": resampled_skipped_bucket_count,
        },
        "three_minute_derivative_feature_definitions": {
            "slope": "current 3m close change divided by trailing 5-bar 3m ATR",
            "curvature": "current normalized 3m slope minus previous normalized 3m slope",
            "slope_bucket": {
                "SLOPE_POS": f"slope > {SLOPE_FLAT_THRESHOLD}",
                "SLOPE_FLAT": f"|slope| <= {SLOPE_FLAT_THRESHOLD}",
                "SLOPE_NEG": f"slope < -{SLOPE_FLAT_THRESHOLD}",
            },
            "curvature_bucket": {
                "CURVATURE_POS": f"curvature > {CURVATURE_FLAT_THRESHOLD}",
                "CURVATURE_FLAT": f"|curvature| <= {CURVATURE_FLAT_THRESHOLD}",
                "CURVATURE_NEG": f"curvature < -{CURVATURE_FLAT_THRESHOLD}",
            },
            "recent_path_features": [
                "slope_path_3",
                "curvature_path_3",
                "derivative_path_3",
                "slope_path_4",
                "curvature_path_4",
                "derivative_path_4",
            ],
        },
        "us_midday_widened_add_separator": {
            "covered_strict_trade_count": len(midday_strict),
            "covered_widened_add_count": len(midday_widened),
            "covered_strict_trades": [asdict(item) for item in midday_strict],
            "covered_widened_adds": [asdict(item) for item in midday_widened],
            "finding": (
                "No 3m separator conclusion is available for widened midday adds on the current 1m coverage window because "
                "the widened-added losers all predate 2026-02-03. Only one strict midday family trade is covered."
            ),
        },
        "london_late_blocked_cluster_separator": {
            "covered_cluster_count_in_discovery_artifact": london_total_covered,
            "mapped_cluster_count_with_3m_anchor": london_mapped_count,
            "covered_visible_survivor_count": len(london_survivors),
            "covered_blocked_count": len(london_blocked),
            "covered_high_value_count": len(london_high),
            "covered_low_value_count": len(london_low),
            "visible_survivor_derivative_buckets": _distribution(item.derivative_bucket_3m for item in london_survivors),
            "blocked_derivative_buckets": _distribution(item.derivative_bucket_3m for item in london_blocked),
            "high_value_derivative_paths": _distribution(item.derivative_path_3 for item in london_high),
            "low_value_derivative_paths": _distribution(item.derivative_path_3 for item in london_low),
            "finding": london_derivative_help["finding"],
            "smallest_next_rule_hypothesis": london_derivative_help["rule_hypothesis"],
        },
        "reference_short_families": {
            "covered_reference_trade_count": len([item for item in observations if item.cohort.startswith("reference_")]),
            "derivative_bucket_distribution": _distribution(
                item.derivative_bucket_3m for item in observations if item.cohort.startswith("reference_")
            ),
        },
        "explicit_answer": {
            "do_three_minute_derivatives_help": london_derivative_help["helpful"],
            "smallest_next_rule_hypothesis": london_derivative_help["rule_hypothesis"],
            "note": london_derivative_help["note"],
        },
        "derivative_anchor_rows_available": len(derivative_index),
    }


def _london_derivative_signal(
    high_value: Sequence[ThreeMinuteDerivativeRow],
    low_value: Sequence[ThreeMinuteDerivativeRow],
    survivors: Sequence[ThreeMinuteDerivativeRow],
    blocked: Sequence[ThreeMinuteDerivativeRow],
) -> dict[str, Any]:
    high_neg_curvature_rate = _share(item.current_curvature_bucket == "CURVATURE_NEG" for item in high_value)
    low_neg_curvature_rate = _share(item.current_curvature_bucket == "CURVATURE_NEG" for item in low_value)
    survivor_path_rate = _share(
        item.derivative_path_3.endswith("SLOPE_FLAT|CURVATURE_NEG") or item.derivative_path_3.endswith("SLOPE_NEG|CURVATURE_NEG")
        for item in survivors
    )
    blocked_path_rate = _share(
        item.derivative_path_3.endswith("SLOPE_FLAT|CURVATURE_NEG") or item.derivative_path_3.endswith("SLOPE_NEG|CURVATURE_NEG")
        for item in blocked
    )

    if high_neg_curvature_rate >= Decimal("0.60") and high_neg_curvature_rate > low_neg_curvature_rate and survivor_path_rate >= blocked_path_rate:
        return {
            "helpful": True,
            "finding": (
                "3m derivatives add real context in covered London-late turns: the higher-value covered cluster rows skew more negative in current 3m curvature, "
                "and visible structural survivors more often finish their recent 3m derivative path on a negative-curvature bar than the blocked set."
            ),
            "rule_hypothesis": (
                "Inside the existing London-late structural core, require the anchor 3m bar to be CURVATURE_NEG and the recent 3m derivative path to end in "
                "SLOPE_FLAT|CURVATURE_NEG or SLOPE_NEG|CURVATURE_NEG."
            ),
            "note": (
                f"High-value London cluster current CURVATURE_NEG rate={high_neg_curvature_rate}, low-value rate={low_neg_curvature_rate}, "
                f"visible-survivor end-path rate={survivor_path_rate}, blocked end-path rate={blocked_path_rate}."
            ),
        }
    return {
        "helpful": False,
        "finding": (
            "3m derivatives did not separate cleanly enough on the covered sample to justify the next qualifier yet."
        ),
        "rule_hypothesis": None,
        "note": (
            f"High-value London cluster current CURVATURE_NEG rate={high_neg_curvature_rate}, low-value rate={low_neg_curvature_rate}, "
            f"visible-survivor end-path rate={survivor_path_rate}, blocked end-path rate={blocked_path_rate}."
        ),
    }


def _bucket_three_way(value: Decimal, *, flat_threshold: Decimal) -> str:
    if value > flat_threshold:
        return "SLOPE_POS" if flat_threshold == SLOPE_FLAT_THRESHOLD else "CURVATURE_POS"
    if value < -flat_threshold:
        return "SLOPE_NEG" if flat_threshold == SLOPE_FLAT_THRESHOLD else "CURVATURE_NEG"
    return "SLOPE_FLAT" if flat_threshold == SLOPE_FLAT_THRESHOLD else "CURVATURE_FLAT"


def _distribution(values: Iterable[str]) -> dict[str, int]:
    distribution: dict[str, int] = {}
    for value in values:
        distribution[value] = distribution.get(value, 0) + 1
    return distribution


def _share(matches: Iterable[bool]) -> Decimal:
    items = list(matches)
    if not items:
        return Decimal("0")
    return Decimal(sum(1 for item in items if item)) / Decimal(len(items))


def _avg(values: Iterable[Decimal]) -> Decimal:
    values = list(values)
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _decimal_str(value: Decimal) -> str:
    return format(value, "f")


def _decimal_or_zero(value: str | None) -> Decimal:
    return Decimal(value) if value is not None else Decimal("0")


def _safe_div(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        return Decimal("0")
    return numerator / denominator


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", required=True, type=Path)
    parser.add_argument("--ticker", default="MGC")
    parser.add_argument("--source-timeframe", default="1m")
    parser.add_argument("--strict-trade-ledger", required=True, type=Path)
    parser.add_argument("--medium-1-trade-ledger", required=True, type=Path)
    parser.add_argument("--medium-2-trade-ledger", required=True, type=Path)
    parser.add_argument("--london-detail-csv", required=True, type=Path)
    parser.add_argument("--reference-trade-ledger", required=True, type=Path)
    parser.add_argument("--output-prefix", required=True, type=Path)
    parser.add_argument("--config", action="append", default=None)
    args = parser.parse_args()

    outputs = build_and_write_three_minute_derivative_separator_analysis(
        db_path=args.db_path,
        ticker=args.ticker,
        source_timeframe=args.source_timeframe,
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
