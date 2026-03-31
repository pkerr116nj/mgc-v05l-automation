"""Research-only reporting/export for persisted EMA momentum evaluation results."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from sqlalchemy import and_, select

from ..persistence.repositories import RepositorySet
from ..persistence.tables import bars_table, derived_features_table, signal_evaluations_table


@dataclass(frozen=True)
class EMAMomentumEvaluationSummary:
    experiment_run_id: int
    total_bars_analyzed: int
    baseline_long_context_count: int
    baseline_short_context_count: int
    filter_pass_long_count: int
    filter_pass_short_count: int
    trigger_long_math_count: int
    trigger_short_math_count: int
    overlap_baseline_long_trigger_long_math: int
    overlap_baseline_short_trigger_short_math: int
    overlap_baseline_long_filter_pass_long: int
    overlap_baseline_short_filter_pass_short: int
    math_trigger_without_baseline_context_count: int
    baseline_context_blocked_by_filter_count: int
    compression_long_count: int
    compression_short_count: int
    reclaim_long_count: int
    failure_short_count: int
    separation_long_count: int
    separation_short_count: int
    structure_long_candidate_count: int
    structure_short_candidate_count: int
    compression_long_and_trigger_long_math_count: int
    compression_short_and_trigger_short_math_count: int
    structure_long_candidate_and_trigger_long_math_count: int
    structure_short_candidate_and_trigger_short_math_count: int
    structure_long_candidate_without_baseline_context_count: int
    structure_short_candidate_without_baseline_context_count: int
    baseline_long_context_and_structure_long_candidate_count: int
    baseline_short_context_and_structure_short_candidate_count: int


@dataclass(frozen=True)
class EMAMomentumEvaluationReportRow:
    bar_id: str
    ticker: str
    timeframe: str
    timestamp: str
    bull_snap_raw: bool
    bear_snap_raw: bool
    asia_vwap_reclaim_raw: bool
    baseline_long_context_present: bool
    baseline_short_context_present: bool
    momentum_compressing_up: bool
    momentum_turning_positive: bool
    momentum_compressing_down: bool
    momentum_turning_negative: bool
    filter_pass_long: bool
    filter_pass_short: bool
    trigger_long_math: bool
    trigger_short_math: bool
    compression_long: bool
    reclaim_long: bool
    separation_long: bool
    structure_long_candidate: bool
    compression_short: bool
    failure_short: bool
    separation_short: bool
    structure_short_candidate: bool
    quality_score_long: Decimal | None
    quality_score_short: Decimal | None
    size_recommendation_long: Decimal | None
    size_recommendation_short: Decimal | None
    warmup_complete: bool
    smoothed_close: Decimal | None
    momentum_raw: Decimal | None
    momentum_norm: Decimal | None
    momentum_delta: Decimal | None
    momentum_acceleration: Decimal | None
    volume_ratio: Decimal | None
    signed_impulse: Decimal | None
    smoothed_signed_impulse: Decimal | None
    impulse_delta: Decimal | None


def build_ema_momentum_evaluation_report(
    repositories: RepositorySet,
    experiment_run_id: int,
) -> tuple[EMAMomentumEvaluationSummary, list[EMAMomentumEvaluationReportRow]]:
    """Build a read-only report from persisted EMA momentum evaluation rows."""
    rows = _load_rows(repositories, experiment_run_id)
    summary = EMAMomentumEvaluationSummary(
        experiment_run_id=experiment_run_id,
        total_bars_analyzed=len(rows),
        baseline_long_context_count=sum(1 for row in rows if row.baseline_long_context_present),
        baseline_short_context_count=sum(1 for row in rows if row.baseline_short_context_present),
        filter_pass_long_count=sum(1 for row in rows if row.filter_pass_long),
        filter_pass_short_count=sum(1 for row in rows if row.filter_pass_short),
        trigger_long_math_count=sum(1 for row in rows if row.trigger_long_math),
        trigger_short_math_count=sum(1 for row in rows if row.trigger_short_math),
        overlap_baseline_long_trigger_long_math=sum(
            1 for row in rows if row.baseline_long_context_present and row.trigger_long_math
        ),
        overlap_baseline_short_trigger_short_math=sum(
            1 for row in rows if row.baseline_short_context_present and row.trigger_short_math
        ),
        overlap_baseline_long_filter_pass_long=sum(
            1 for row in rows if row.baseline_long_context_present and row.filter_pass_long
        ),
        overlap_baseline_short_filter_pass_short=sum(
            1 for row in rows if row.baseline_short_context_present and row.filter_pass_short
        ),
        math_trigger_without_baseline_context_count=sum(
            1
            for row in rows
            if (row.trigger_long_math and not row.baseline_long_context_present)
            or (row.trigger_short_math and not row.baseline_short_context_present)
        ),
        baseline_context_blocked_by_filter_count=sum(
            1
            for row in rows
            if (row.baseline_long_context_present and not row.filter_pass_long)
            or (row.baseline_short_context_present and not row.filter_pass_short)
        ),
        compression_long_count=sum(1 for row in rows if row.compression_long),
        compression_short_count=sum(1 for row in rows if row.compression_short),
        reclaim_long_count=sum(1 for row in rows if row.reclaim_long),
        failure_short_count=sum(1 for row in rows if row.failure_short),
        separation_long_count=sum(1 for row in rows if row.separation_long),
        separation_short_count=sum(1 for row in rows if row.separation_short),
        structure_long_candidate_count=sum(1 for row in rows if row.structure_long_candidate),
        structure_short_candidate_count=sum(1 for row in rows if row.structure_short_candidate),
        compression_long_and_trigger_long_math_count=sum(
            1 for row in rows if row.compression_long and row.trigger_long_math
        ),
        compression_short_and_trigger_short_math_count=sum(
            1 for row in rows if row.compression_short and row.trigger_short_math
        ),
        structure_long_candidate_and_trigger_long_math_count=sum(
            1 for row in rows if row.structure_long_candidate and row.trigger_long_math
        ),
        structure_short_candidate_and_trigger_short_math_count=sum(
            1 for row in rows if row.structure_short_candidate and row.trigger_short_math
        ),
        structure_long_candidate_without_baseline_context_count=sum(
            1 for row in rows if row.structure_long_candidate and not row.baseline_long_context_present
        ),
        structure_short_candidate_without_baseline_context_count=sum(
            1 for row in rows if row.structure_short_candidate and not row.baseline_short_context_present
        ),
        baseline_long_context_and_structure_long_candidate_count=sum(
            1 for row in rows if row.baseline_long_context_present and row.structure_long_candidate
        ),
        baseline_short_context_and_structure_short_candidate_count=sum(
            1 for row in rows if row.baseline_short_context_present and row.structure_short_candidate
        ),
    )
    return summary, rows


def write_ema_momentum_evaluation_report_csv(
    rows: Sequence[EMAMomentumEvaluationReportRow],
    output_path: str | Path,
) -> Path:
    """Write a per-bar EMA momentum evaluation export for offline analysis."""
    path = Path(output_path)
    fieldnames = [
        "bar_id",
        "ticker",
        "timeframe",
        "timestamp",
        "bull_snap_raw",
        "bear_snap_raw",
        "asia_vwap_reclaim_raw",
        "baseline_long_context_present",
        "baseline_short_context_present",
        "momentum_compressing_up",
        "momentum_turning_positive",
        "momentum_compressing_down",
        "momentum_turning_negative",
        "filter_pass_long",
        "filter_pass_short",
        "trigger_long_math",
        "trigger_short_math",
        "compression_long",
        "reclaim_long",
        "separation_long",
        "structure_long_candidate",
        "compression_short",
        "failure_short",
        "separation_short",
        "structure_short_candidate",
        "quality_score_long",
        "quality_score_short",
        "size_recommendation_long",
        "size_recommendation_short",
        "warmup_complete",
        "smoothed_close",
        "momentum_raw",
        "momentum_norm",
        "momentum_delta",
        "momentum_acceleration",
        "volume_ratio",
        "signed_impulse",
        "smoothed_signed_impulse",
        "impulse_delta",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "bar_id": row.bar_id,
                    "ticker": row.ticker,
                    "timeframe": row.timeframe,
                    "timestamp": row.timestamp,
                    "bull_snap_raw": str(row.bull_snap_raw),
                    "bear_snap_raw": str(row.bear_snap_raw),
                    "asia_vwap_reclaim_raw": str(row.asia_vwap_reclaim_raw),
                    "baseline_long_context_present": str(row.baseline_long_context_present),
                    "baseline_short_context_present": str(row.baseline_short_context_present),
                    "momentum_compressing_up": str(row.momentum_compressing_up),
                    "momentum_turning_positive": str(row.momentum_turning_positive),
                    "momentum_compressing_down": str(row.momentum_compressing_down),
                    "momentum_turning_negative": str(row.momentum_turning_negative),
                    "filter_pass_long": str(row.filter_pass_long),
                    "filter_pass_short": str(row.filter_pass_short),
                    "trigger_long_math": str(row.trigger_long_math),
                    "trigger_short_math": str(row.trigger_short_math),
                    "compression_long": str(row.compression_long),
                    "reclaim_long": str(row.reclaim_long),
                    "separation_long": str(row.separation_long),
                    "structure_long_candidate": str(row.structure_long_candidate),
                    "compression_short": str(row.compression_short),
                    "failure_short": str(row.failure_short),
                    "separation_short": str(row.separation_short),
                    "structure_short_candidate": str(row.structure_short_candidate),
                    "quality_score_long": _stringify_decimal(row.quality_score_long),
                    "quality_score_short": _stringify_decimal(row.quality_score_short),
                    "size_recommendation_long": _stringify_decimal(row.size_recommendation_long),
                    "size_recommendation_short": _stringify_decimal(row.size_recommendation_short),
                    "warmup_complete": str(row.warmup_complete),
                    "smoothed_close": _stringify_decimal(row.smoothed_close),
                    "momentum_raw": _stringify_decimal(row.momentum_raw),
                    "momentum_norm": _stringify_decimal(row.momentum_norm),
                    "momentum_delta": _stringify_decimal(row.momentum_delta),
                    "momentum_acceleration": _stringify_decimal(row.momentum_acceleration),
                    "volume_ratio": _stringify_decimal(row.volume_ratio),
                    "signed_impulse": _stringify_decimal(row.signed_impulse),
                    "smoothed_signed_impulse": _stringify_decimal(row.smoothed_signed_impulse),
                    "impulse_delta": _stringify_decimal(row.impulse_delta),
                }
            )
    return path


def _load_rows(
    repositories: RepositorySet,
    experiment_run_id: int,
) -> list[EMAMomentumEvaluationReportRow]:
    statement = (
        select(
            bars_table.c.bar_id,
            bars_table.c.ticker,
            bars_table.c.timeframe,
            bars_table.c.timestamp,
            signal_evaluations_table.c.bull_snap_raw,
            signal_evaluations_table.c.bear_snap_raw,
            signal_evaluations_table.c.asia_vwap_reclaim_raw,
            signal_evaluations_table.c.momentum_compressing_up,
            signal_evaluations_table.c.momentum_turning_positive,
            signal_evaluations_table.c.momentum_compressing_down,
            signal_evaluations_table.c.momentum_turning_negative,
            signal_evaluations_table.c.filter_pass_long,
            signal_evaluations_table.c.filter_pass_short,
            signal_evaluations_table.c.trigger_long_math,
            signal_evaluations_table.c.trigger_short_math,
            signal_evaluations_table.c.compression_long,
            signal_evaluations_table.c.reclaim_long,
            signal_evaluations_table.c.separation_long,
            signal_evaluations_table.c.structure_long_candidate,
            signal_evaluations_table.c.compression_short,
            signal_evaluations_table.c.failure_short,
            signal_evaluations_table.c.separation_short,
            signal_evaluations_table.c.structure_short_candidate,
            signal_evaluations_table.c.quality_score_long,
            signal_evaluations_table.c.quality_score_short,
            signal_evaluations_table.c.size_recommendation_long,
            signal_evaluations_table.c.size_recommendation_short,
            signal_evaluations_table.c.warmup_complete,
            derived_features_table.c.smoothed_close,
            derived_features_table.c.momentum_raw,
            derived_features_table.c.momentum_norm,
            derived_features_table.c.momentum_delta,
            derived_features_table.c.momentum_acceleration,
            derived_features_table.c.volume_ratio,
            derived_features_table.c.signed_impulse,
            derived_features_table.c.smoothed_signed_impulse,
            derived_features_table.c.impulse_delta,
        )
        .select_from(
            bars_table.join(
                signal_evaluations_table,
                bars_table.c.bar_id == signal_evaluations_table.c.bar_id,
            ).join(
                derived_features_table,
                and_(
                    bars_table.c.bar_id == derived_features_table.c.bar_id,
                    signal_evaluations_table.c.experiment_run_id == derived_features_table.c.experiment_run_id,
                ),
            )
        )
        .where(signal_evaluations_table.c.experiment_run_id == experiment_run_id)
        .order_by(bars_table.c.timestamp.asc(), bars_table.c.bar_id.asc())
    )
    with repositories.engine.begin() as connection:
        raw_rows = connection.execute(statement).mappings().all()
    return [_decode_report_row(dict(row)) for row in raw_rows]


def _decode_report_row(row: dict[str, Any]) -> EMAMomentumEvaluationReportRow:
    baseline_long_context_present = bool(row["bull_snap_raw"] or row["asia_vwap_reclaim_raw"])
    baseline_short_context_present = bool(row["bear_snap_raw"])
    return EMAMomentumEvaluationReportRow(
        bar_id=row["bar_id"],
        ticker=row["ticker"],
        timeframe=row["timeframe"],
        timestamp=row["timestamp"],
        bull_snap_raw=bool(row["bull_snap_raw"]),
        bear_snap_raw=bool(row["bear_snap_raw"]),
        asia_vwap_reclaim_raw=bool(row["asia_vwap_reclaim_raw"]),
        baseline_long_context_present=baseline_long_context_present,
        baseline_short_context_present=baseline_short_context_present,
        momentum_compressing_up=bool(row["momentum_compressing_up"]),
        momentum_turning_positive=bool(row["momentum_turning_positive"]),
        momentum_compressing_down=bool(row["momentum_compressing_down"]),
        momentum_turning_negative=bool(row["momentum_turning_negative"]),
        filter_pass_long=bool(row["filter_pass_long"]),
        filter_pass_short=bool(row["filter_pass_short"]),
        trigger_long_math=bool(row["trigger_long_math"]),
        trigger_short_math=bool(row["trigger_short_math"]),
        compression_long=bool(row["compression_long"]),
        reclaim_long=bool(row["reclaim_long"]),
        separation_long=bool(row["separation_long"]),
        structure_long_candidate=bool(row["structure_long_candidate"]),
        compression_short=bool(row["compression_short"]),
        failure_short=bool(row["failure_short"]),
        separation_short=bool(row["separation_short"]),
        structure_short_candidate=bool(row["structure_short_candidate"]),
        quality_score_long=_to_decimal(row["quality_score_long"]),
        quality_score_short=_to_decimal(row["quality_score_short"]),
        size_recommendation_long=_to_decimal(row["size_recommendation_long"]),
        size_recommendation_short=_to_decimal(row["size_recommendation_short"]),
        warmup_complete=bool(row["warmup_complete"]),
        smoothed_close=_to_decimal(row["smoothed_close"]),
        momentum_raw=_to_decimal(row["momentum_raw"]),
        momentum_norm=_to_decimal(row["momentum_norm"]),
        momentum_delta=_to_decimal(row["momentum_delta"]),
        momentum_acceleration=_to_decimal(row["momentum_acceleration"]),
        volume_ratio=_to_decimal(row["volume_ratio"]),
        signed_impulse=_to_decimal(row["signed_impulse"]),
        smoothed_signed_impulse=_to_decimal(row["smoothed_signed_impulse"]),
        impulse_delta=_to_decimal(row["impulse_delta"]),
    )


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _stringify_decimal(value: Decimal | None) -> str:
    return "" if value is None else str(value)
