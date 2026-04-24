"""Research-only compression-then-continuation detector branch for Asia Drift."""

from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Sequence

from ..trend_participation.models import ResearchBar
from ..trend_participation.storage import build_layout, write_storage_manifest
from .backtest import AsiaDriftPhase2Run, run_asia_drift_phase2, run_asia_drift_phase2_from_bars
from .broader_replay import AsiaDriftReplayBarWindow, AsiaDriftReplayWindow, DEFAULT_WIDER_REPLAY_WINDOWS
from .features import ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT, RECOVERY_CONFIRMED
from .taxonomy import (
    OPP_COMPRESSION_THEN_CONTINUATION,
    classify_session_taxonomy_for_run,
)


NO_BRANCH = "NO_BRANCH"
DRIFT_CONTEXT = "DRIFT_CONTEXT"
COMPRESSION_FORMING = "COMPRESSION_FORMING"
COMPRESSION_READY = "COMPRESSION_READY"
CONTINUATION_CONFIRMED = "CONTINUATION_CONFIRMED"
FALSE_BREAK_OR_CHOP = "FALSE_BREAK_OR_CHOP"
SESSION_TIMEOUT = "SESSION_TIMEOUT"
REJECTION_CONFIRMED = "REJECTION_CONFIRMED"
UNRESOLVED_PENDING = "UNRESOLVED_PENDING"
SESSION_TIMEOUT_UNRESOLVED = "SESSION_TIMEOUT_UNRESOLVED"

METALS_COMPRESSION_CONTINUATION = "metals_compression_continuation"
ES_MES_COMPRESSION_CONTINUATION = "es_mes_compression_continuation"
BRANCH_PROFILE_STRICT_CURRENT = "strict_current"
BRANCH_PROFILE_MATURATION_CONFIRMED = "maturation_confirmed"
BRANCH_PROFILE_FALSE_BREAK_CONFIRMED = "false_break_confirmed"
BRANCH_PROFILE_DIAGNOSTIC_LOOSE_CEILING = "diagnostic_loose_ceiling"
BRANCH_PROFILE_CANDIDATE_DISCOVERY = "candidate_discovery"

PRIMARY_INSTRUMENTS = ("MGC", "GC", "MES", "ES")
DIAGNOSTIC_INSTRUMENTS = ("MNQ", "NQ")
DEFAULT_BRANCH_PROFILE_COMPARISON = (
    BRANCH_PROFILE_CANDIDATE_DISCOVERY,
    BRANCH_PROFILE_STRICT_CURRENT,
    BRANCH_PROFILE_MATURATION_CONFIRMED,
    BRANCH_PROFILE_FALSE_BREAK_CONFIRMED,
    BRANCH_PROFILE_DIAGNOSTIC_LOOSE_CEILING,
)

INSTRUMENT_FAMILY = {
    "MGC": "metals",
    "GC": "metals",
    "MES": "indices",
    "ES": "indices",
    "MNQ": "indices_diagnostic",
    "NQ": "indices_diagnostic",
}


@dataclass(frozen=True)
class CompressionContinuationProfile:
    name: str
    family: str
    drift_score_min: float
    regime_persistence_min: float
    chop_compat_min_drift_score: float
    chop_compat_min_regime_persistence: float
    chop_compat_min_overlap_ratio: float
    chop_compat_max_reversal_frequency: float
    chop_compat_max_realized_vol_ratio: float
    chop_compat_max_pullback_depth_atr: float
    max_post_spike_fraction: float
    max_chop_fraction: float
    compression_forming_score_min: float
    compression_ready_score_min: float
    min_forming_bars: int
    max_range_ratio: float
    min_overlap_ratio: float
    max_vwap_distance_atr: float
    max_ema_distance_atr: float
    max_pullback_depth_atr: float
    max_pullback_expansion_ratio: float
    continuation_score_min: float
    breakout_buffer_atr: float
    expansion_ratio_min: float
    min_signed_vwap_displacement: float
    min_slope_combo: float
    min_efficiency_improvement: float
    false_break_adverse_atr: float


@dataclass(frozen=True)
class CompressionBranchControlProfile:
    name: str
    role: str
    description: str
    forming_maturation_bars: int
    ready_maturation_bars: int
    false_break_confirmation_bars: int
    true_chop_reversal_threshold: float
    true_chop_realized_vol_threshold: float
    continuation_score_relaxation: float
    continuation_boundary_buffer_scale: float
    allow_confirmation_after_failed_countertrend: bool
    continuation_requires_mature_ready: bool
    detector_forming_score_scale: float
    detector_ready_score_scale: float
    detector_possible_score_scale: float
    detector_range_ratio_relief: float
    detector_overlap_ratio_relief: float
    detector_vwap_distance_scale: float
    detector_ema_distance_scale: float
    detector_pullback_depth_scale: float
    detector_pullback_expansion_scale: float


PROFILE_BY_FAMILY = {
    "metals": CompressionContinuationProfile(
        name=METALS_COMPRESSION_CONTINUATION,
        family="metals",
        drift_score_min=2.25,
        regime_persistence_min=0.48,
        chop_compat_min_drift_score=0.70,
        chop_compat_min_regime_persistence=0.22,
        chop_compat_min_overlap_ratio=0.66,
        chop_compat_max_reversal_frequency=0.35,
        chop_compat_max_realized_vol_ratio=1.12,
        chop_compat_max_pullback_depth_atr=0.95,
        max_post_spike_fraction=0.20,
        max_chop_fraction=0.42,
        compression_forming_score_min=0.34,
        compression_ready_score_min=0.41,
        min_forming_bars=2,
        max_range_ratio=0.92,
        min_overlap_ratio=0.56,
        max_vwap_distance_atr=0.52,
        max_ema_distance_atr=0.55,
        max_pullback_depth_atr=1.10,
        max_pullback_expansion_ratio=1.18,
        continuation_score_min=0.62,
        breakout_buffer_atr=0.08,
        expansion_ratio_min=1.18,
        min_signed_vwap_displacement=0.10,
        min_slope_combo=0.15,
        min_efficiency_improvement=0.03,
        false_break_adverse_atr=0.95,
    ),
    "indices": CompressionContinuationProfile(
        name=ES_MES_COMPRESSION_CONTINUATION,
        family="indices",
        drift_score_min=2.40,
        regime_persistence_min=0.50,
        chop_compat_min_drift_score=0.90,
        chop_compat_min_regime_persistence=0.28,
        chop_compat_min_overlap_ratio=0.68,
        chop_compat_max_reversal_frequency=0.38,
        chop_compat_max_realized_vol_ratio=1.15,
        chop_compat_max_pullback_depth_atr=0.90,
        max_post_spike_fraction=0.24,
        max_chop_fraction=0.45,
        compression_forming_score_min=0.38,
        compression_ready_score_min=0.45,
        min_forming_bars=2,
        max_range_ratio=0.88,
        min_overlap_ratio=0.58,
        max_vwap_distance_atr=0.62,
        max_ema_distance_atr=0.62,
        max_pullback_depth_atr=1.00,
        max_pullback_expansion_ratio=1.12,
        continuation_score_min=0.66,
        breakout_buffer_atr=0.10,
        expansion_ratio_min=1.24,
        min_signed_vwap_displacement=0.14,
        min_slope_combo=0.18,
        min_efficiency_improvement=0.04,
        false_break_adverse_atr=0.90,
    ),
}

BRANCH_CONTROL_PROFILES = {
    BRANCH_PROFILE_CANDIDATE_DISCOVERY: CompressionBranchControlProfile(
        name=BRANCH_PROFILE_CANDIDATE_DISCOVERY,
        role="candidate_discovery",
        description="Broad detector-stage discovery profile for compression candidates; surfaces candidate phenomena without implying trade quality.",
        forming_maturation_bars=2,
        ready_maturation_bars=2,
        false_break_confirmation_bars=2,
        true_chop_reversal_threshold=0.56,
        true_chop_realized_vol_threshold=1.55,
        continuation_score_relaxation=0.08,
        continuation_boundary_buffer_scale=0.82,
        allow_confirmation_after_failed_countertrend=True,
        continuation_requires_mature_ready=False,
        detector_forming_score_scale=0.72,
        detector_ready_score_scale=0.80,
        detector_possible_score_scale=0.58,
        detector_range_ratio_relief=0.16,
        detector_overlap_ratio_relief=0.08,
        detector_vwap_distance_scale=1.35,
        detector_ema_distance_scale=1.35,
        detector_pullback_depth_scale=1.18,
        detector_pullback_expansion_scale=1.12,
    ),
    BRANCH_PROFILE_STRICT_CURRENT: CompressionBranchControlProfile(
        name=BRANCH_PROFILE_STRICT_CURRENT,
        role="trade_quality",
        description="Immediate branch resolution after a single false-break signal; closest to the original strict branch behavior.",
        forming_maturation_bars=1,
        ready_maturation_bars=1,
        false_break_confirmation_bars=1,
        true_chop_reversal_threshold=0.42,
        true_chop_realized_vol_threshold=1.25,
        continuation_score_relaxation=0.0,
        continuation_boundary_buffer_scale=1.0,
        allow_confirmation_after_failed_countertrend=False,
        continuation_requires_mature_ready=False,
        detector_forming_score_scale=1.0,
        detector_ready_score_scale=1.0,
        detector_possible_score_scale=1.0,
        detector_range_ratio_relief=0.0,
        detector_overlap_ratio_relief=0.0,
        detector_vwap_distance_scale=1.0,
        detector_ema_distance_scale=1.0,
        detector_pullback_depth_scale=1.0,
        detector_pullback_expansion_scale=1.0,
    ),
    BRANCH_PROFILE_MATURATION_CONFIRMED: CompressionBranchControlProfile(
        name=BRANCH_PROFILE_MATURATION_CONFIRMED,
        role="trade_quality",
        description="Requires compression to mature before overlap-heavy early bars can resolve to branch failure.",
        forming_maturation_bars=2,
        ready_maturation_bars=2,
        false_break_confirmation_bars=2,
        true_chop_reversal_threshold=0.48,
        true_chop_realized_vol_threshold=1.35,
        continuation_score_relaxation=0.02,
        continuation_boundary_buffer_scale=0.95,
        allow_confirmation_after_failed_countertrend=False,
        continuation_requires_mature_ready=False,
        detector_forming_score_scale=1.0,
        detector_ready_score_scale=1.0,
        detector_possible_score_scale=0.92,
        detector_range_ratio_relief=0.02,
        detector_overlap_ratio_relief=0.02,
        detector_vwap_distance_scale=1.04,
        detector_ema_distance_scale=1.04,
        detector_pullback_depth_scale=1.02,
        detector_pullback_expansion_scale=1.02,
    ),
    BRANCH_PROFILE_FALSE_BREAK_CONFIRMED: CompressionBranchControlProfile(
        name=BRANCH_PROFILE_FALSE_BREAK_CONFIRMED,
        role="trade_quality",
        description="Requires follow-through before labeling a mature compression window as false break or chop.",
        forming_maturation_bars=2,
        ready_maturation_bars=2,
        false_break_confirmation_bars=3,
        true_chop_reversal_threshold=0.50,
        true_chop_realized_vol_threshold=1.40,
        continuation_score_relaxation=0.04,
        continuation_boundary_buffer_scale=0.90,
        allow_confirmation_after_failed_countertrend=True,
        continuation_requires_mature_ready=True,
        detector_forming_score_scale=1.0,
        detector_ready_score_scale=1.0,
        detector_possible_score_scale=0.90,
        detector_range_ratio_relief=0.04,
        detector_overlap_ratio_relief=0.02,
        detector_vwap_distance_scale=1.06,
        detector_ema_distance_scale=1.06,
        detector_pullback_depth_scale=1.03,
        detector_pullback_expansion_scale=1.03,
    ),
    BRANCH_PROFILE_DIAGNOSTIC_LOOSE_CEILING: CompressionBranchControlProfile(
        name=BRANCH_PROFILE_DIAGNOSTIC_LOOSE_CEILING,
        role="trade_quality_diagnostic",
        description="Diagnostic ceiling to test whether branch scarcity is mainly a confirmation-timing issue.",
        forming_maturation_bars=3,
        ready_maturation_bars=2,
        false_break_confirmation_bars=3,
        true_chop_reversal_threshold=0.54,
        true_chop_realized_vol_threshold=1.48,
        continuation_score_relaxation=0.07,
        continuation_boundary_buffer_scale=0.82,
        allow_confirmation_after_failed_countertrend=True,
        continuation_requires_mature_ready=False,
        detector_forming_score_scale=0.92,
        detector_ready_score_scale=0.95,
        detector_possible_score_scale=0.82,
        detector_range_ratio_relief=0.08,
        detector_overlap_ratio_relief=0.04,
        detector_vwap_distance_scale=1.12,
        detector_ema_distance_scale=1.12,
        detector_pullback_depth_scale=1.08,
        detector_pullback_expansion_scale=1.08,
    ),
}


def get_compression_profile(instrument: str) -> CompressionContinuationProfile:
    family = instrument_family(instrument)
    if family == "metals":
        return PROFILE_BY_FAMILY["metals"]
    return PROFILE_BY_FAMILY["indices"]


def get_branch_control_profile(name: str) -> CompressionBranchControlProfile:
    return BRANCH_CONTROL_PROFILES[name]


def instrument_family(instrument: str) -> str:
    return INSTRUMENT_FAMILY.get(instrument.upper(), "other")


def run_compression_continuation_detector(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    instruments: Sequence[str] = PRIMARY_INSTRUMENTS,
    diagnostic_instruments: Sequence[str] = (),
    windows: Sequence[AsiaDriftReplayWindow] = DEFAULT_WIDER_REPLAY_WINDOWS,
    branch_profile_names: Sequence[str] = DEFAULT_BRANCH_PROFILE_COMPARISON,
) -> dict[str, Any]:
    database_coverage = _load_database_coverage(
        source_sqlite_path=source_sqlite_path,
        instruments=(*tuple(instruments), *tuple(diagnostic_instruments)),
        study_window_start=min((window.start_ts for window in windows), default=None),
    )
    instrument_runs = _load_phase2_runs(
        source_sqlite_path=source_sqlite_path,
        output_dir=output_dir,
        instruments=(*tuple(instruments), *tuple(diagnostic_instruments)),
        windows=windows,
    )
    payload = _build_comparison_payload(
        instrument_runs=instrument_runs,
        source_label=str(source_sqlite_path.resolve()),
        primary_instruments=tuple(instruments),
        diagnostic_instruments=tuple(diagnostic_instruments),
        windows=windows,
        branch_profile_names=branch_profile_names,
        database_coverage=database_coverage,
    )
    artifacts = _write_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def run_compression_continuation_detector_from_bars(
    *,
    output_dir: Path,
    primary_instrument: str,
    windows: Sequence[AsiaDriftReplayBarWindow],
    branch_profile_names: Sequence[str] = (BRANCH_PROFILE_FALSE_BREAK_CONFIRMED,),
) -> dict[str, Any]:
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]]] = {primary_instrument: []}
    for window in windows:
        run = run_asia_drift_phase2_from_bars(
            output_dir=output_dir / primary_instrument.lower() / window.label,
            bars_5m=window.bars_5m,
            bars_1m=window.bars_1m,
            source_label=window.label,
            calibration_profile_name=RECOVERY_CONFIRMED,
        )
        instrument_runs[primary_instrument].append(
            (
                AsiaDriftReplayWindow(
                    label=window.label,
                    start_ts=window.bars_5m[0].end_ts if window.bars_5m else datetime.fromisoformat("1970-01-01T00:00:00+00:00"),
                    end_ts=window.bars_5m[-1].end_ts if window.bars_5m else datetime.fromisoformat("1970-01-01T00:00:00+00:00"),
                ),
                run,
            )
        )
    payload = _build_comparison_payload(
        instrument_runs=instrument_runs,
        source_label="synthetic",
        primary_instruments=(primary_instrument,),
        diagnostic_instruments=(),
        windows=tuple(
            AsiaDriftReplayWindow(
                label=window.label,
                start_ts=window.bars_5m[0].end_ts if window.bars_5m else datetime.fromisoformat("1970-01-01T00:00:00+00:00"),
                end_ts=window.bars_5m[-1].end_ts if window.bars_5m else datetime.fromisoformat("1970-01-01T00:00:00+00:00"),
            )
            for window in windows
        ),
        branch_profile_names=branch_profile_names,
        database_coverage={"source_kind": "synthetic", "coverage_rows": [], "study_window_start": None, "study_window_end": None},
    )
    artifacts = _write_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def _load_phase2_runs(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    instruments: Sequence[str],
    windows: Sequence[AsiaDriftReplayWindow],
) -> dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]]]:
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]]] = {}
    for instrument in instruments:
        runs: list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]] = []
        for window in windows:
            run = run_asia_drift_phase2(
                source_sqlite_path=source_sqlite_path,
                output_dir=output_dir / instrument.lower() / window.label,
                instruments=(instrument,),
                start_ts=window.start_ts,
                end_ts=window.end_ts,
                calibration_profile_name=RECOVERY_CONFIRMED,
            )
            runs.append((window, run))
        instrument_runs[instrument] = runs
    return instrument_runs


def _load_database_coverage(
    *,
    source_sqlite_path: Path,
    instruments: Sequence[str],
    study_window_start: datetime | None = None,
) -> dict[str, Any]:
    coverage_rows: list[dict[str, Any]] = []
    with sqlite3.connect(source_sqlite_path) as connection:
        placeholders = ",".join("?" for _ in instruments)
        rows = connection.execute(
            f"""
            SELECT symbol, MIN(end_ts), MAX(end_ts), COUNT(*), COUNT(DISTINCT timeframe)
            FROM bars
            WHERE symbol IN ({placeholders})
            GROUP BY symbol
            ORDER BY symbol
            """,
            tuple(instruments),
        ).fetchall()
        missing_rows = connection.execute(
            f"""
            SELECT symbol, COUNT(*)
            FROM bars
            WHERE symbol IN ({placeholders})
              AND (end_ts IS NULL OR open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL)
            GROUP BY symbol
            ORDER BY symbol
            """,
            tuple(instruments),
        ).fetchall()
    missing_by_symbol = {symbol: count for symbol, count in missing_rows}
    for symbol, min_end_ts, max_end_ts, row_count, timeframe_count in rows:
        coverage_rows.append(
            {
                "instrument": symbol,
                "db_min_end_ts": min_end_ts,
                "db_max_end_ts": max_end_ts,
                "bar_count": row_count,
                "timeframe_count": timeframe_count,
                "missing_bar_count": missing_by_symbol.get(symbol, 0),
            }
        )
    earliest_db_start = min((row["db_min_end_ts"] for row in coverage_rows), default=None)
    return {
        "source_kind": "sqlite",
        "coverage_rows": coverage_rows,
        "earliest_db_start": earliest_db_start,
        "study_window_start_matches_db_start": (
            study_window_start.isoformat() == earliest_db_start if study_window_start is not None and earliest_db_start is not None else None
        ),
        "coverage_note": (
            "Database coverage begins in early January 2024 for the included instruments, but the current wider replay "
            "study window starts later and does not use the full history."
        ),
    }


def _build_comparison_payload(
    *,
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]]],
    source_label: str,
    primary_instruments: Sequence[str],
    diagnostic_instruments: Sequence[str],
    windows: Sequence[AsiaDriftReplayWindow],
    branch_profile_names: Sequence[str],
    database_coverage: dict[str, Any],
) -> dict[str, Any]:
    profile_results: dict[str, dict[str, Any]] = {}
    for branch_profile_name in branch_profile_names:
        profile_results[branch_profile_name] = _build_profile_payload(
            instrument_runs=instrument_runs,
            windows=windows,
            branch_profile_name=branch_profile_name,
        )
    comparison_rows = _profile_comparison_rows(profile_results=profile_results)
    recommendation = _comparison_recommendation(profile_results=profile_results, comparison_rows=comparison_rows)
    selected_profile_name = recommendation["recommended_detector_profile"] or recommendation["recommended_trade_quality_profile"]
    selected = profile_results[selected_profile_name]
    return {
        "module": "Asia Drift Compression Then Continuation Detector",
        "objective": (
            "Research-only detector branch for Asia/overnight compression-then-continuation using shared logic with "
            "family-specific thresholds for metals and ES/MES-style index overnight drift."
        ),
        "source_label": source_label,
        "windows": [window.label for window in windows],
        "study_window_coverage": {
            "study_window_start": min((window.start_ts for window in windows), default=None).isoformat() if windows else None,
            "study_window_end": max((window.end_ts for window in windows), default=None).isoformat() if windows else None,
            "window_count": len(windows),
        },
        "database_coverage": database_coverage,
        "primary_instruments": list(primary_instruments),
        "diagnostic_instruments": list(diagnostic_instruments),
        "family_profiles": {
            METALS_COMPRESSION_CONTINUATION: PROFILE_BY_FAMILY["metals"].__dict__,
            ES_MES_COMPRESSION_CONTINUATION: PROFILE_BY_FAMILY["indices"].__dict__,
        },
        "branch_profiles": {name: BRANCH_CONTROL_PROFILES[name].__dict__ for name in branch_profile_names},
        "profile_results": profile_results,
        "profile_comparison_rows": comparison_rows,
        "recommendation": recommendation,
        "selected_profile_name": selected_profile_name,
        "per_session_summary": selected["per_session_summary"],
        "instrument_summary": selected["instrument_summary"],
        "family_summary": selected["family_summary"],
        "evaluation": selected["evaluation"],
        "funnel_audit": selected["funnel_audit"],
        "candidate_maturation_summary": selected["candidate_maturation_summary"],
        "artifacts_for_review": selected["artifacts_for_review"],
        "counts": selected["counts"],
        "_profile_artifacts": {
            name: {
                "per_bar_rows": result["_per_bar_rows"],
                "compression_windows": result["_compression_windows"],
                "continuation_confirmations": result["_continuation_confirmations"],
                "false_breaks": result["_false_breaks"],
                "missed_target_audit": result["_missed_target_audit"],
                "session_stage_rows": result["_session_stage_rows"],
                "candidate_lifecycle_rows": result["_candidate_lifecycle_rows"],
            }
            for name, result in profile_results.items()
        },
        "_per_bar_rows": selected["_per_bar_rows"],
        "_compression_windows": selected["_compression_windows"],
        "_continuation_confirmations": selected["_continuation_confirmations"],
        "_false_breaks": selected["_false_breaks"],
        "_session_stage_rows": selected["_session_stage_rows"],
        "_candidate_lifecycle_rows": selected["_candidate_lifecycle_rows"],
    }


def _build_profile_payload(
    *,
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]]],
    windows: Sequence[AsiaDriftReplayWindow],
    branch_profile_name: str,
) -> dict[str, Any]:
    per_bar_rows: list[dict[str, Any]] = []
    compression_windows: list[dict[str, Any]] = []
    continuation_confirmations: list[dict[str, Any]] = []
    false_breaks: list[dict[str, Any]] = []
    session_rows: list[dict[str, Any]] = []
    missed_target_audit: list[dict[str, Any]] = []
    session_stage_rows: list[dict[str, Any]] = []
    candidate_lifecycle_rows: list[dict[str, Any]] = []

    for instrument, window_runs in sorted(instrument_runs.items()):
        for window, run in window_runs:
            taxonomy_rows = {
                row["asia_drift_session_id"]: row
                for row in classify_session_taxonomy_for_run(instrument=instrument, window=window, run=run)
            }
            for session_payload in _evaluate_run_sessions(
                instrument=instrument,
                window=window,
                run=run,
                taxonomy_rows=taxonomy_rows,
                branch_profile_name=branch_profile_name,
            ):
                per_bar_rows.extend(session_payload["per_bar_rows"])
                compression_windows.extend(session_payload["compression_windows"])
                continuation_confirmations.extend(session_payload["continuation_confirmations"])
                false_breaks.extend(session_payload["false_breaks"])
                session_rows.append(session_payload["session_summary"])
                session_stage_rows.append(session_payload["session_stage_row"])
                candidate_lifecycle_rows.append(session_payload["candidate_lifecycle_row"])
                if session_payload["missed_target_audit"] is not None:
                    missed_target_audit.append(session_payload["missed_target_audit"])

    instrument_summary = {
        instrument: _aggregate_branch_summary(
            session_rows=[row for row in session_rows if row["instrument"] == instrument],
            continuation_rows=[row for row in continuation_confirmations if row["instrument"] == instrument],
            false_break_rows=[row for row in false_breaks if row["instrument"] == instrument],
        )
        for instrument in sorted(instrument_runs)
    }
    family_summary = {
        family: _aggregate_branch_summary(
            session_rows=[row for row in session_rows if row["instrument_family"] == family],
            continuation_rows=[row for row in continuation_confirmations if row["instrument_family"] == family],
            false_break_rows=[row for row in false_breaks if row["instrument_family"] == family],
        )
        for family in sorted({row["instrument_family"] for row in session_rows})
    }
    evaluation = _build_evaluation(session_rows=session_rows, family_summary=family_summary, instrument_summary=instrument_summary)
    funnel_audit = _build_funnel_audit(
        per_bar_rows=per_bar_rows,
        session_stage_rows=session_stage_rows,
        missed_target_audit=missed_target_audit,
    )
    candidate_maturation_summary = _build_candidate_maturation_summary(
        session_rows=session_rows,
        candidate_lifecycle_rows=candidate_lifecycle_rows,
        branch_profile_name=branch_profile_name,
    )
    return {
        "branch_profile_name": branch_profile_name,
        "per_session_summary": session_rows,
        "session_stage_rows": session_stage_rows,
        "candidate_lifecycle_rows": candidate_lifecycle_rows,
        "instrument_summary": instrument_summary,
        "family_summary": family_summary,
        "evaluation": evaluation,
        "funnel_audit": funnel_audit,
        "candidate_maturation_summary": candidate_maturation_summary,
        "artifacts_for_review": {
            "compression_windows": compression_windows[:50],
            "continuation_confirmations": continuation_confirmations[:50],
            "false_breaks": false_breaks[:50],
            "missed_target_audit": missed_target_audit[:50],
            "candidate_lifecycle_rows": candidate_lifecycle_rows[:50],
            "pending_unresolved_manifest": [row for row in candidate_lifecycle_rows if row["lifecycle_outcome"] in {UNRESOLVED_PENDING, SESSION_TIMEOUT_UNRESOLVED}][:50],
            "confirmed_rejection_manifest": [row for row in candidate_lifecycle_rows if row["lifecycle_outcome"] == REJECTION_CONFIRMED][:50],
        },
        "counts": {
            "per_bar_row_count": len(per_bar_rows),
            "session_count": len(session_rows),
            "compression_window_count": len(compression_windows),
            "continuation_confirmation_count": len(continuation_confirmations),
            "false_break_count": len(false_breaks),
            "missed_target_count": len(missed_target_audit),
            "candidate_lifecycle_count": len(candidate_lifecycle_rows),
        },
        "_per_bar_rows": per_bar_rows,
        "_compression_windows": compression_windows,
        "_continuation_confirmations": continuation_confirmations,
        "_false_breaks": false_breaks,
        "_missed_target_audit": missed_target_audit,
        "_session_stage_rows": session_stage_rows,
        "_candidate_lifecycle_rows": candidate_lifecycle_rows,
    }


def _build_candidate_maturation_summary(
    *,
    session_rows: Sequence[dict[str, Any]],
    candidate_lifecycle_rows: Sequence[dict[str, Any]],
    branch_profile_name: str,
) -> dict[str, Any]:
    pending_rows = [row for row in candidate_lifecycle_rows if row["lifecycle_outcome"] in {UNRESOLVED_PENDING, SESSION_TIMEOUT_UNRESOLVED}]
    rejection_rows = [row for row in candidate_lifecycle_rows if row["lifecycle_outcome"] == REJECTION_CONFIRMED]
    clean_unresolved_rows = [row for row in pending_rows if row["candidate_cleanliness"] == "unresolved_clean_candidate"]
    contaminated_rows = [
        row
        for row in candidate_lifecycle_rows
        if row["candidate_cleanliness"] in {"post_spike_contaminated_candidate", "deep_damage_contaminated_candidate"}
    ]
    taxonomy_pending = [row for row in pending_rows if row["taxonomy_is_compression_target"]]
    family_rows: dict[str, dict[str, Any]] = {}
    for family in sorted({row["instrument_family"] for row in candidate_lifecycle_rows}):
        rows = [row for row in candidate_lifecycle_rows if row["instrument_family"] == family and row["candidate_seen"]]
        family_rows[family] = {
            "candidate_count": len(rows),
            "continuation_confirmed_count": sum(1 for row in rows if row["lifecycle_outcome"] == CONTINUATION_CONFIRMED),
            "confirmed_rejection_count": sum(1 for row in rows if row["lifecycle_outcome"] == REJECTION_CONFIRMED),
            "clean_unresolved_count": sum(1 for row in rows if row["candidate_cleanliness"] == "unresolved_clean_candidate"),
            "contaminated_candidate_count": sum(
                1
                for row in rows
                if row["candidate_cleanliness"] in {"post_spike_contaminated_candidate", "deep_damage_contaminated_candidate"}
            ),
            "dominant_maturation_failures": dict(
                Counter(
                    row["maturation_failure_feature"]
                    for row in rows
                    if row["maturation_failure_feature"] is not None
                ).most_common(6)
            ),
        }
    metals_rows = [row for row in candidate_lifecycle_rows if row["instrument_family"] == "metals" and row["candidate_seen"]]
    indices_rows = [row for row in candidate_lifecycle_rows if row["instrument_family"] == "indices" and row["candidate_seen"]]
    return {
        "branch_profile": branch_profile_name,
        "candidate_count": sum(1 for row in candidate_lifecycle_rows if row["candidate_seen"]),
        "clean_unresolved_count": len(clean_unresolved_rows),
        "confirmed_rejection_count": len(rejection_rows),
        "contaminated_candidate_count": len(contaminated_rows),
        "session_timeout_unresolved_count": sum(1 for row in pending_rows if row["lifecycle_outcome"] == SESSION_TIMEOUT_UNRESOLVED),
        "pending_unresolved_count": len(pending_rows),
        "taxonomy_target_clean_unresolved_count": len(taxonomy_pending),
        "taxonomy_target_pending_failure_features": dict(
            Counter(row["maturation_failure_feature"] for row in taxonomy_pending if row["maturation_failure_feature"] is not None).most_common(8)
        ),
        "family_comparison": family_rows,
        "metals_vs_es_mes": {
            "metals_candidate_count": len(metals_rows),
            "metals_confirmed_count": sum(1 for row in metals_rows if row["lifecycle_outcome"] == CONTINUATION_CONFIRMED),
            "metals_clean_unresolved_count": sum(1 for row in metals_rows if row["candidate_cleanliness"] == "unresolved_clean_candidate"),
            "metals_pending_failure_features": dict(
                Counter(row["maturation_failure_feature"] for row in metals_rows if row["maturation_failure_feature"] is not None).most_common(6)
            ),
            "indices_candidate_count": len(indices_rows),
            "indices_confirmed_count": sum(1 for row in indices_rows if row["lifecycle_outcome"] == CONTINUATION_CONFIRMED),
            "indices_clean_unresolved_count": sum(1 for row in indices_rows if row["candidate_cleanliness"] == "unresolved_clean_candidate"),
            "indices_pending_failure_features": dict(
                Counter(row["maturation_failure_feature"] for row in indices_rows if row["maturation_failure_feature"] is not None).most_common(6)
            ),
        },
        "selected_profile_session_count": len(session_rows),
    }


def _profile_comparison_rows(*, profile_results: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, result in profile_results.items():
        metals = result["family_summary"].get("metals", {})
        indices = result["family_summary"].get("indices", {})
        diag = result["family_summary"].get("indices_diagnostic", {})
        stage_rows = result["session_stage_rows"]
        taxonomy_targets = [row for row in stage_rows if row["taxonomy_is_compression_target"]]
        candidate_detected_targets = [row for row in taxonomy_targets if row["compression_forming"] or row["compression_ready"]]
        ready_targets = [row for row in taxonomy_targets if row["compression_ready"]]
        contaminated_candidates = [
            row
            for row in result["candidate_lifecycle_rows"]
            if row["candidate_cleanliness"] in {"post_spike_contaminated_candidate", "deep_damage_contaminated_candidate"}
        ]
        clean_unresolved = [
            row for row in result["candidate_lifecycle_rows"] if row["candidate_cleanliness"] == "unresolved_clean_candidate"
        ]
        rejection_confirmed = [
            row for row in result["candidate_lifecycle_rows"] if row["lifecycle_outcome"] == REJECTION_CONFIRMED
        ]
        chop_like_candidates = [
            row
            for row in stage_rows
            if (row["compression_forming"] or row["compression_ready"])
            and row["taxonomy_opportunity_type"] == "NO_DRIFT_CHOP"
        ]
        rows.append(
            {
                "branch_profile": name,
                "branch_profile_role": BRANCH_CONTROL_PROFILES[name].role,
                "metals_recall_against_taxonomy": metals.get("recall_against_taxonomy", 0.0),
                "indices_recall_against_taxonomy": indices.get("recall_against_taxonomy", 0.0),
                "metals_precision_against_taxonomy": metals.get("precision_against_taxonomy", 0.0),
                "indices_precision_against_taxonomy": indices.get("precision_against_taxonomy", 0.0),
                "candidate_detection_recall_against_taxonomy": _rate(len(candidate_detected_targets), len(taxonomy_targets)),
                "ready_recall_against_taxonomy": _rate(len(ready_targets), len(taxonomy_targets)),
                "compression_ready_count": sum(summary.get("compression_ready_count", 0) for summary in result["instrument_summary"].values()),
                "compression_forming_count": sum(1 for row in stage_rows if row["compression_forming"]),
                "continuation_confirmed_count": sum(summary.get("continuation_confirmed_count", 0) for summary in result["instrument_summary"].values()),
                "false_break_count": sum(summary.get("false_break_count", 0) for summary in result["instrument_summary"].values()),
                "false_break_or_chop_count": sum(summary.get("false_break_or_chop_count", 0) for summary in result["instrument_summary"].values()),
                "diagnostic_nq_continuation_count": diag.get("continuation_confirmed_count", 0),
                "diagnostic_nq_false_positive_count": diag.get("false_positive_count", 0),
                "contaminated_candidate_count": len(contaminated_candidates),
                "clean_unresolved_count": len(clean_unresolved),
                "confirmed_rejection_count": len(rejection_confirmed),
                "likely_simple_chop_candidate_count": len(chop_like_candidates),
                "missed_target_count": result["counts"]["missed_target_count"],
            }
        )
    return sorted(rows, key=lambda row: row["branch_profile"])


def _comparison_recommendation(
    *,
    profile_results: dict[str, dict[str, Any]],
    comparison_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    def score(row: dict[str, Any]) -> float:
        return (
            3.5 * float(row["candidate_detection_recall_against_taxonomy"])
            + 2.5 * float(row["ready_recall_against_taxonomy"])
            + 2.0 * float(row["metals_recall_against_taxonomy"])
            + 1.5 * float(row["indices_recall_against_taxonomy"])
            + 2.0 * float(row["metals_precision_against_taxonomy"])
            + 1.5 * row["continuation_confirmed_count"]
            + 0.25 * row["compression_ready_count"]
            - 0.75 * row["false_break_or_chop_count"]
            - 2.0 * row["diagnostic_nq_continuation_count"]
            - 0.40 * row["contaminated_candidate_count"]
            - 0.20 * row["likely_simple_chop_candidate_count"]
            - 0.05 * row["missed_target_count"]
        )

    ranked = sorted(comparison_rows, key=score, reverse=True)
    detector_rows = [row for row in ranked if row["branch_profile_role"] == "candidate_discovery"]
    trade_rows = [row for row in ranked if row["branch_profile_role"] != "candidate_discovery"]
    detector_best = detector_rows[0] if detector_rows else None
    trade_best = trade_rows[0] if trade_rows else None
    detector_result = profile_results[detector_best["branch_profile"]] if detector_best is not None else None
    trade_result = profile_results[trade_best["branch_profile"]] if trade_best is not None else None
    if (
        trade_best is not None
        and float(trade_best.get("candidate_detection_recall_against_taxonomy") or 0.0) >= 0.50
        and float(trade_best.get("ready_recall_against_taxonomy") or 0.0) >= 0.25
        and sum(summary.get("continuation_confirmed_count", 0) for summary in trade_result["instrument_summary"].values()) >= 2
    ):
        verdict = "begin_phase2_entry_exit_research"
        reason = "Compression maturation and confirmed-false-break logic are now surfacing branch continuations across metals and ES/MES."
    else:
        verdict = "continue_branch_diagnostics_only"
        reason = (
            "Candidate discovery can now surface a broader compression phenomenon set, but trade-quality continuation "
            "recall remains too low for entry/exit research."
        )
    return {
        "verdict": verdict,
        "recommended_detector_profile": detector_best["branch_profile"] if detector_best is not None else None,
        "recommended_trade_quality_profile": trade_best["branch_profile"] if trade_best is not None else None,
        "next_primary_branch": OPP_COMPRESSION_THEN_CONTINUATION,
        "shared_architecture_recommendation": "shared_cross_asset_detector_with_family_specific_thresholds",
        "reason": reason,
        "comparison_rows_ranked": ranked,
        "deprioritized_nq_contamination": (
            detector_result["evaluation"].get("deprioritized_nq_contamination")
            if detector_result is not None
            else trade_result["evaluation"].get("deprioritized_nq_contamination")
        ),
    }


def _evaluate_run_sessions(
    *,
    instrument: str,
    window: AsiaDriftReplayWindow,
    run: AsiaDriftPhase2Run,
    taxonomy_rows: dict[str, dict[str, Any]],
    branch_profile_name: str,
) -> list[dict[str, Any]]:
    features_by_session = _group_by(run.phase1_run.feature_rows, key=lambda row: row.asia_drift_session_id)
    summaries_by_session = {row.asia_drift_session_id: row for row in run.phase1_run.session_summaries}
    payloads: list[dict[str, Any]] = []
    for session_id, summary in sorted(summaries_by_session.items()):
        feature_rows = sorted(features_by_session.get(session_id, []), key=lambda row: row.decision_ts)
        payloads.append(
            _evaluate_session(
                instrument=instrument,
                window=window,
                summary=summary,
                feature_rows=feature_rows,
                taxonomy_row=taxonomy_rows.get(session_id),
                branch_profile_name=branch_profile_name,
            )
        )
    return payloads


def _evaluate_session(
    *,
    instrument: str,
    window: AsiaDriftReplayWindow,
    summary: Any,
    feature_rows: Sequence[Any],
    taxonomy_row: dict[str, Any] | None,
    branch_profile_name: str,
) -> dict[str, Any]:
    rows = [row for row in feature_rows if row.in_scope]
    family = instrument_family(instrument)
    diagnostic_only = family == "indices_diagnostic"
    profile = get_compression_profile(instrument)
    branch_profile = get_branch_control_profile(branch_profile_name)
    direction = summary.candidate_direction if summary.candidate_direction in {"LONG", "SHORT"} else (rows[0].dominant_direction if rows else "LONG")

    per_bar_rows: list[dict[str, Any]] = []
    compression_windows: list[dict[str, Any]] = []
    continuation_confirmations: list[dict[str, Any]] = []
    false_breaks: list[dict[str, Any]] = []

    previous_state = NO_BRANCH
    forming_start_index: int | None = None
    ready_start_index: int | None = None
    compression_high: float | None = None
    compression_low: float | None = None
    compression_scores: list[float] = []
    compression_range_norms: list[float] = []
    ready_count = 0
    continuation_detected = False
    false_break_detected = False
    compression_ready_detected = False
    state_counts: Counter[str] = Counter()
    compression_bar_count = 0
    false_break_pending_bars = 0
    false_break_pending_reasons: tuple[str, ...] = ()
    false_break_pending_stage: str | None = None
    max_false_break_pending_bars = 0
    max_compression_bar_count = 0

    for index, row in enumerate(rows):
        compression_metrics = _compression_metrics(
            rows=rows,
            index=index,
            direction=direction,
            profile=profile,
            branch_profile=branch_profile,
        )
        overlap_label = _overlap_classification(row=row, profile=profile, branch_profile=branch_profile)
        continuation_metrics = _continuation_metrics(
            rows=rows,
            index=index,
            direction=direction,
            profile=profile,
            branch_profile=branch_profile,
            compression_high=compression_high,
            compression_low=compression_low,
            compression_range_norms=compression_range_norms,
            mature_ready=ready_count >= branch_profile.ready_maturation_bars,
            recent_false_break_warning=false_break_pending_bars > 0,
        )
        drift_context = _drift_context(row=row, direction=direction, profile=profile, family=family)
        false_break = _false_break(
            row=row,
            direction=direction,
            profile=profile,
            branch_profile=branch_profile,
            compression_low=compression_low,
            compression_high=compression_high,
            family=family,
            overlap_label=overlap_label,
        )
        warmup_persistence = _compression_warmup_persistence(
            row=row,
            previous_state=previous_state,
            compression_score=compression_metrics["score"],
            profile=profile,
        )
        mature_compression = compression_bar_count >= branch_profile.forming_maturation_bars or ready_count >= branch_profile.ready_maturation_bars
        if warmup_persistence["active"]:
            drift_context = {
                "active": True,
                "reasons": [*drift_context["reasons"], *warmup_persistence["reasons"]],
            }
            if false_break["active"]:
                preserved_reasons = [reason for reason in false_break["reasons"] if reason not in {"chop_veto_active", "true_chop_overlap"}]
                false_break = {**false_break, "active": bool(preserved_reasons), "reasons": preserved_reasons}
        if previous_state in {COMPRESSION_FORMING, COMPRESSION_READY} and not drift_context["active"] and not false_break["hard"]:
            keep_alive = (
                overlap_label == "COMPRESSION_COMPATIBLE_OVERLAP"
                or compression_metrics["score"] >= profile.compression_forming_score_min * 0.85
            )
            if keep_alive:
                drift_context = {
                    "active": True,
                    "reasons": [*drift_context["reasons"], "compression_window_maintained"],
                }

        if previous_state in {COMPRESSION_FORMING, COMPRESSION_READY} and false_break["active"] and not false_break["hard"]:
            false_break_pending_bars += 1
            false_break_pending_reasons = tuple(false_break["reasons"])
            false_break_pending_stage = previous_state
        elif false_break["hard"]:
            false_break_pending_bars = branch_profile.false_break_confirmation_bars
            false_break_pending_reasons = tuple(false_break["reasons"])
            false_break_pending_stage = previous_state if previous_state in {COMPRESSION_FORMING, COMPRESSION_READY} else None
        else:
            false_break_pending_bars = 0
            false_break_pending_reasons = ()
            false_break_pending_stage = None
        max_false_break_pending_bars = max(max_false_break_pending_bars, false_break_pending_bars)
        false_break_confirmed = (
            previous_state in {COMPRESSION_FORMING, COMPRESSION_READY}
            and false_break["active"]
            and (
                false_break["hard"]
                or (
                    mature_compression
                    and false_break_pending_bars >= branch_profile.false_break_confirmation_bars
                )
            )
        )

        if row.session_timeout:
            if previous_state in {COMPRESSION_FORMING, COMPRESSION_READY} and forming_start_index is not None:
                compression_windows.append(
                    _compression_window_row(
                        instrument=instrument,
                        family=family,
                        window_label=window.label,
                        session_id=summary.asia_drift_session_id,
                        rows=rows,
                        start_index=forming_start_index,
                        end_index=max(index - 1, forming_start_index),
                        scores=compression_scores,
                        state_resolved_to=previous_state,
                        continuation_detected=continuation_detected,
                    )
                )
            state = SESSION_TIMEOUT
            reason_tags = ("session_timeout",)
            false_break_pending_bars = 0
            false_break_pending_reasons = ()
            false_break_pending_stage = None
        elif not drift_context["active"]:
            state = FALSE_BREAK_OR_CHOP if previous_state in {COMPRESSION_FORMING, COMPRESSION_READY} and (row.chop_veto or row.post_spike_instability) else NO_BRANCH
            reason_tags = tuple(drift_context["reasons"])
            if previous_state in {COMPRESSION_FORMING, COMPRESSION_READY} and forming_start_index is not None:
                compression_windows.append(
                    _compression_window_row(
                        instrument=instrument,
                        family=family,
                        window_label=window.label,
                        session_id=summary.asia_drift_session_id,
                        rows=rows,
                        start_index=forming_start_index,
                        end_index=max(index - 1, forming_start_index),
                        scores=compression_scores,
                        state_resolved_to=state,
                        continuation_detected=continuation_detected,
                    )
                )
            if state == FALSE_BREAK_OR_CHOP:
                false_break_detected = True
                false_breaks.append(
                    {
                        "instrument": instrument,
                        "instrument_family": family,
                        "window_label": window.label,
                        "asia_drift_session_id": summary.asia_drift_session_id,
                        "decision_ts": row.decision_ts.isoformat(),
                        "reason_tags": [tag for tag in drift_context["reasons"] if tag in {"chop_veto_present", "post_spike_present"}],
                        "compression_low": compression_low,
                        "compression_high": compression_high,
                    }
                )
            forming_start_index = None
            ready_start_index = None
            compression_high = None
            compression_low = None
            compression_scores = []
            compression_range_norms = []
            ready_count = 0
            compression_bar_count = 0
        elif false_break_confirmed:
            if forming_start_index is not None:
                compression_windows.append(
                    _compression_window_row(
                        instrument=instrument,
                        family=family,
                        window_label=window.label,
                        session_id=summary.asia_drift_session_id,
                        rows=rows,
                        start_index=forming_start_index,
                        end_index=max(index - 1, forming_start_index),
                        scores=compression_scores,
                        state_resolved_to=FALSE_BREAK_OR_CHOP,
                        continuation_detected=continuation_detected,
                    )
                )
            state = FALSE_BREAK_OR_CHOP
            reason_tags = tuple((*false_break_pending_reasons, "false_break_confirmed"))
            false_break_detected = True
            false_breaks.append(
                {
                    "instrument": instrument,
                    "instrument_family": family,
                    "window_label": window.label,
                    "asia_drift_session_id": summary.asia_drift_session_id,
                    "decision_ts": row.decision_ts.isoformat(),
                    "reason_tags": list((*false_break_pending_reasons, "false_break_confirmed")),
                    "compression_low": compression_low,
                    "compression_high": compression_high,
                }
            )
            forming_start_index = None
            ready_start_index = None
            compression_high = None
            compression_low = None
            compression_scores = []
            compression_range_norms = []
            ready_count = 0
            compression_bar_count = 0
            false_break_pending_bars = 0
            false_break_pending_reasons = ()
            false_break_pending_stage = None
        elif continuation_metrics["confirmed"] and previous_state in {COMPRESSION_FORMING, COMPRESSION_READY}:
            state = CONTINUATION_CONFIRMED
            reason_tags = tuple(continuation_metrics["reasons"])
            continuation_detected = True
            continuation_confirmations.append(
                {
                    "instrument": instrument,
                    "instrument_family": family,
                    "window_label": window.label,
                    "asia_drift_session_id": summary.asia_drift_session_id,
                    "decision_ts": row.decision_ts.isoformat(),
                    "continuation_score": continuation_metrics["score"],
                    "reason_tags": list(continuation_metrics["reasons"]),
                    "compression_low": compression_low,
                    "compression_high": compression_high,
                }
            )
            false_break_pending_bars = 0
            false_break_pending_reasons = ()
            false_break_pending_stage = None
        elif compression_metrics["ready"]:
            state = COMPRESSION_READY
            extra_reasons = []
            if false_break_pending_bars > 0:
                extra_reasons.append("false_break_warning_pending")
            if overlap_label == "COMPRESSION_COMPATIBLE_OVERLAP":
                extra_reasons.append("compression_compatible_overlap")
            reason_tags = tuple((*compression_metrics["reasons"], *extra_reasons))
            compression_ready_detected = True
            if forming_start_index is None:
                forming_start_index = index
            if ready_start_index is None:
                ready_start_index = index
            ready_count += 1
            compression_bar_count += 1
            max_compression_bar_count = max(max_compression_bar_count, compression_bar_count)
            compression_high = max(compression_high if compression_high is not None else row.high, row.high)
            compression_low = min(compression_low if compression_low is not None else row.low, row.low)
            compression_scores.append(compression_metrics["score"])
            compression_range_norms.append(compression_metrics["range_norm"])
        elif compression_metrics["forming"]:
            state = COMPRESSION_FORMING
            extra_reasons = []
            if false_break_pending_bars > 0:
                extra_reasons.append("false_break_warning_pending")
            if overlap_label == "COMPRESSION_COMPATIBLE_OVERLAP":
                extra_reasons.append("compression_compatible_overlap")
            reason_tags = tuple((*compression_metrics["reasons"], *extra_reasons))
            if forming_start_index is None:
                forming_start_index = index
            ready_start_index = None
            ready_count = 0
            compression_bar_count += 1
            max_compression_bar_count = max(max_compression_bar_count, compression_bar_count)
            compression_high = max(compression_high if compression_high is not None else row.high, row.high)
            compression_low = min(compression_low if compression_low is not None else row.low, row.low)
            compression_scores.append(compression_metrics["score"])
            compression_range_norms.append(compression_metrics["range_norm"])
        else:
            state = DRIFT_CONTEXT
            extra_reasons = []
            if false_break_pending_bars > 0:
                extra_reasons.append("false_break_warning_pending")
            if overlap_label == "COMPRESSION_COMPATIBLE_OVERLAP":
                extra_reasons.append("compression_compatible_overlap")
            reason_tags = tuple((*drift_context["reasons"], *extra_reasons))
            if previous_state in {COMPRESSION_FORMING, COMPRESSION_READY} and forming_start_index is not None:
                compression_windows.append(
                    _compression_window_row(
                        instrument=instrument,
                        family=family,
                        window_label=window.label,
                        session_id=summary.asia_drift_session_id,
                        rows=rows,
                        start_index=forming_start_index,
                        end_index=index - 1,
                        scores=compression_scores,
                        state_resolved_to=previous_state,
                        continuation_detected=continuation_detected,
                    )
                )
            forming_start_index = None
            ready_start_index = None
            compression_high = None
            compression_low = None
            compression_scores = []
            compression_range_norms = []
            ready_count = 0
            compression_bar_count = 0
            false_break_pending_bars = 0
            false_break_pending_reasons = ()
            false_break_pending_stage = None

        state_counts[state] += 1
        per_bar_rows.append(
            {
                "instrument": instrument,
                "instrument_family": family,
                "window_label": window.label,
                "asia_drift_session_id": summary.asia_drift_session_id,
                "decision_ts": row.decision_ts.isoformat(),
                "state": state,
                "previous_state": previous_state,
                "direction": direction,
                "compression_profile": profile.name,
                "branch_profile": branch_profile.name,
                "compression_score": compression_metrics["score"],
                "possible_compression_behavior": compression_metrics["possible"],
                "range_contraction_score": compression_metrics["range_contraction"],
                "overlap_score": compression_metrics["overlap_score"],
                "vwap_cluster_score": compression_metrics["vwap_cluster"],
                "ema_cluster_score": compression_metrics["ema_cluster"],
                "adverse_control_score": compression_metrics["adverse_control"],
                "expansion_control_score": compression_metrics["expansion_control"],
                "insufficient_range_contraction": compression_metrics["insufficient_range_contraction"],
                "boundary_instability": compression_metrics["boundary_instability"],
                "vwap_ema_logic_block": compression_metrics["vwap_ema_logic_block"],
                "deep_damage_active": row.pullback_depth_atr > profile.false_break_adverse_atr or row.pullback_state == "DISQUALIFYING_PULLBACK",
                "post_spike_active": row.post_spike_instability,
                "true_chop_overlap": overlap_label == "TRUE_CHOP",
                "continuation_score": continuation_metrics["score"],
                "continuation_boundary_break": continuation_metrics["boundary_break"],
                "continuation_expansion_score": continuation_metrics["expansion_score"],
                "continuation_close_score": continuation_metrics["close_score"],
                "continuation_vwap_score": continuation_metrics["vwap_score"],
                "continuation_slope_score": continuation_metrics["slope_score"],
                "continuation_efficiency_score": continuation_metrics["efficiency_score"],
                "continuation_supportive_resume": continuation_metrics["supportive_resume"],
                "continuation_score_threshold": continuation_metrics["score_threshold"],
                "compression_boundary_high": compression_high,
                "compression_boundary_low": compression_low,
                "overlap_label": overlap_label,
                "compression_bar_count": compression_bar_count,
                "mature_compression": mature_compression,
                "false_break_pending_bars": false_break_pending_bars,
                "false_break_pending_stage": false_break_pending_stage,
                "reason_tags": "|".join(reason_tags),
                "taxonomy_opportunity_type": taxonomy_row.get("opportunity_type") if taxonomy_row else None,
                "diagnostic_only": diagnostic_only,
            }
        )
        previous_state = state

    if previous_state in {COMPRESSION_FORMING, COMPRESSION_READY} and forming_start_index is not None:
        compression_windows.append(
            _compression_window_row(
                instrument=instrument,
                family=family,
                window_label=window.label,
                session_id=summary.asia_drift_session_id,
                rows=rows,
                start_index=forming_start_index,
                end_index=len(rows) - 1,
                scores=compression_scores,
                state_resolved_to=previous_state,
                continuation_detected=continuation_detected,
            )
        )

    taxonomy_type = taxonomy_row.get("opportunity_type") if taxonomy_row else None
    taxonomy_is_target = taxonomy_type == OPP_COMPRESSION_THEN_CONTINUATION
    session_stage_row = _session_stage_row(
        instrument=instrument,
        family=family,
        window_label=window.label,
        session_id=summary.asia_drift_session_id,
        branch_profile_name=branch_profile.name,
        branch_profile_role=branch_profile.role,
        taxonomy_type=taxonomy_type,
        per_bar_rows=per_bar_rows,
    )
    candidate_lifecycle_row = _candidate_lifecycle_row(
        instrument=instrument,
        family=family,
        window_label=window.label,
        session_id=summary.asia_drift_session_id,
        branch_profile_name=branch_profile.name,
        branch_profile_role=branch_profile.role,
        taxonomy_type=taxonomy_type,
        per_bar_rows=per_bar_rows,
        session_stage_row=session_stage_row,
    )
    missed_target_audit = _missed_target_audit(
        instrument=instrument,
        family=family,
        window_label=window.label,
        session_id=summary.asia_drift_session_id,
        taxonomy_is_target=taxonomy_is_target,
        continuation_detected=continuation_detected,
        compression_ready_detected=compression_ready_detected,
        state_counts=state_counts,
        per_bar_rows=per_bar_rows,
        candidate_lifecycle_row=candidate_lifecycle_row,
    )
    session_summary = {
        "instrument": instrument,
        "instrument_family": family,
        "diagnostic_only": diagnostic_only,
        "window_label": window.label,
        "asia_drift_session_id": summary.asia_drift_session_id,
        "compression_profile": profile.name,
        "branch_profile": branch_profile.name,
        "taxonomy_opportunity_type": taxonomy_type,
        "taxonomy_is_compression_target": taxonomy_is_target,
        "branch_detected_drift_context": state_counts[DRIFT_CONTEXT] > 0 or state_counts[COMPRESSION_FORMING] > 0 or state_counts[COMPRESSION_READY] > 0 or state_counts[CONTINUATION_CONFIRMED] > 0,
        "branch_detected_compression_ready": compression_ready_detected,
        "branch_detected_continuation_confirmed": continuation_detected,
        "branch_false_break_or_chop": false_break_detected,
        "candidate_peak_state": candidate_lifecycle_row["candidate_peak_state"],
        "candidate_lifecycle_outcome": candidate_lifecycle_row["lifecycle_outcome"],
        "candidate_cleanliness": candidate_lifecycle_row["candidate_cleanliness"],
        "pending_bar_count": candidate_lifecycle_row["pending_bar_count"],
        "missed_taxonomy_compression_session": taxonomy_is_target and not continuation_detected,
        "max_compression_bar_count": max_compression_bar_count,
        "max_false_break_pending_bars": max_false_break_pending_bars,
        "missed_target_stage": missed_target_audit["failed_stage"] if missed_target_audit is not None else None,
        "state_counts": dict(state_counts),
        "reason_tags": _session_reason_tags(
            continuation_detected=continuation_detected,
            false_break_detected=false_break_detected,
            taxonomy_is_target=taxonomy_is_target,
            taxonomy_row=taxonomy_row,
        ),
    }
    return {
        "per_bar_rows": per_bar_rows,
        "compression_windows": compression_windows,
        "continuation_confirmations": continuation_confirmations,
        "false_breaks": false_breaks,
        "session_summary": session_summary,
        "session_stage_row": session_stage_row,
        "candidate_lifecycle_row": candidate_lifecycle_row,
        "missed_target_audit": missed_target_audit,
    }


def _drift_context(*, row: Any, direction: str, profile: CompressionContinuationProfile, family: str) -> dict[str, Any]:
    drift_score = row.long_drift_score if direction == "LONG" else row.short_drift_score
    compatible_chop = _compression_compatible_chop(row=row, direction=direction, profile=profile)
    active = (
        row.regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}
        and drift_score >= profile.drift_score_min
        and row.regime_persistence_score >= profile.regime_persistence_min
        and not (row.post_spike_instability and family != "indices")
        and not row.chop_veto
    ) or compatible_chop["active"]
    reasons = []
    if row.regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}:
        reasons.append("drift_regime_present")
    if drift_score >= profile.drift_score_min:
        reasons.append("drift_score_supportive")
    if row.regime_persistence_score >= profile.regime_persistence_min:
        reasons.append("regime_persistence_supportive")
    if row.chop_veto:
        reasons.append("chop_veto_present")
    if row.post_spike_instability:
        reasons.append("post_spike_present")
    reasons.extend(compatible_chop["reasons"])
    return {"active": active, "reasons": reasons}


def _compression_compatible_chop(
    *,
    row: Any,
    direction: str,
    profile: CompressionContinuationProfile,
) -> dict[str, Any]:
    drift_score = row.long_drift_score if direction == "LONG" else row.short_drift_score
    directional_alignment = row.dominant_direction == direction
    directional_evidence = (
        row.countertrend_extension_failed
        or row.compression_followed_by_drift_expansion
        or row.bars_since_last_drift_impulse <= 1
        or row.renewed_signed_vwap_displacement >= profile.min_signed_vwap_displacement * 0.35
    )
    active = (
        row.chop_veto
        and not row.post_spike_instability
        and directional_alignment
        and drift_score >= profile.chop_compat_min_drift_score
        and row.regime_persistence_score >= profile.chop_compat_min_regime_persistence
        and row.bar_overlap_ratio_8 >= profile.chop_compat_min_overlap_ratio
        and row.reversal_frequency_12 <= profile.chop_compat_max_reversal_frequency
        and row.realized_volatility_ratio <= profile.chop_compat_max_realized_vol_ratio
        and row.pullback_depth_atr <= profile.chop_compat_max_pullback_depth_atr
        and row.pullback_expansion_ratio <= profile.max_pullback_expansion_ratio
        and directional_evidence
    )
    reasons: list[str] = []
    if active:
        reasons.extend(
            (
                "compression_compatible_chop",
                "coil_overlap_supportive",
                "reversal_frequency_controlled",
                "volatility_compressed",
            )
        )
        if row.countertrend_extension_failed:
            reasons.append("countertrend_failed")
        if row.compression_followed_by_drift_expansion:
            reasons.append("compression_resume_feature")
        if row.renewed_signed_vwap_displacement >= profile.min_signed_vwap_displacement * 0.35:
            reasons.append("proto_vwap_displacement_supportive")
        if row.bars_since_last_drift_impulse <= 1:
            reasons.append("recent_drift_impulse")
    return {"active": active, "reasons": reasons}


def _compression_warmup_persistence(
    *,
    row: Any,
    previous_state: str,
    compression_score: float,
    profile: CompressionContinuationProfile,
) -> dict[str, Any]:
    active = (
        previous_state in {DRIFT_CONTEXT, COMPRESSION_FORMING, COMPRESSION_READY}
        and row.chop_veto
        and not row.post_spike_instability
        and compression_score >= profile.compression_forming_score_min
        and row.bar_overlap_ratio_8 >= profile.min_overlap_ratio
        and row.pullback_depth_atr <= profile.max_pullback_depth_atr
        and row.pullback_expansion_ratio <= profile.max_pullback_expansion_ratio
        and row.realized_volatility_ratio <= profile.chop_compat_max_realized_vol_ratio
        and row.bars_since_last_drift_impulse <= 1
    )
    if not active:
        return {"active": False, "reasons": []}
    return {"active": True, "reasons": ["compression_warmup_persistence", "coil_overlap_supportive"]}


def _overlap_classification(
    *,
    row: Any,
    profile: CompressionContinuationProfile,
    branch_profile: CompressionBranchControlProfile,
) -> str:
    if not row.chop_veto:
        return "NO_CHOP_VETO"
    if row.post_spike_instability or row.pullback_state == "DISQUALIFYING_PULLBACK":
        return "TRUE_CHOP"
    if (
        row.bar_overlap_ratio_8 >= profile.chop_compat_min_overlap_ratio
        and row.reversal_frequency_12 <= branch_profile.true_chop_reversal_threshold
        and row.realized_volatility_ratio <= branch_profile.true_chop_realized_vol_threshold
        and row.pullback_depth_atr <= profile.max_pullback_depth_atr
        and row.pullback_expansion_ratio <= profile.max_pullback_expansion_ratio
    ):
        return "COMPRESSION_COMPATIBLE_OVERLAP"
    return "TRUE_CHOP"


def _compression_metrics(
    *,
    rows: Sequence[Any],
    index: int,
    direction: str,
    profile: CompressionContinuationProfile,
    branch_profile: CompressionBranchControlProfile,
) -> dict[str, Any]:
    row = rows[index]
    prior = rows[max(0, index - 6) : index]
    baseline_ranges = [prev.range_points / max(prev.atr, 1e-9) for prev in prior] or [row.range_points / max(row.atr, 1e-9)]
    baseline_range = float(median(baseline_ranges))
    range_norm = row.range_points / max(row.atr, 1e-9)
    range_ratio = range_norm / max(baseline_range, 1e-9)
    effective_max_range_ratio = profile.max_range_ratio + branch_profile.detector_range_ratio_relief
    effective_min_overlap_ratio = max(profile.min_overlap_ratio - branch_profile.detector_overlap_ratio_relief, 0.0)
    effective_max_vwap_distance = profile.max_vwap_distance_atr * branch_profile.detector_vwap_distance_scale
    effective_max_ema_distance = profile.max_ema_distance_atr * branch_profile.detector_ema_distance_scale
    effective_max_pullback_depth = profile.max_pullback_depth_atr * branch_profile.detector_pullback_depth_scale
    effective_max_pullback_expansion = profile.max_pullback_expansion_ratio * branch_profile.detector_pullback_expansion_scale
    range_contraction = _clip((effective_max_range_ratio - range_ratio) / max(effective_max_range_ratio, 1e-9), 0.0, 1.0)
    overlap_score = _clip((row.bar_overlap_ratio_8 - effective_min_overlap_ratio) / 0.35, 0.0, 1.0)
    vwap_distance = abs(row.close - row.session_vwap) / max(row.atr, 1e-9)
    ema_distance = abs(row.close - row.slow_ema) / max(row.atr, 1e-9)
    vwap_cluster = _clip((effective_max_vwap_distance - vwap_distance) / max(effective_max_vwap_distance, 1e-9), 0.0, 1.0)
    ema_cluster = _clip((effective_max_ema_distance - ema_distance) / max(effective_max_ema_distance, 1e-9), 0.0, 1.0)
    adverse_control = _clip((effective_max_pullback_depth - row.pullback_depth_atr) / max(effective_max_pullback_depth, 1e-9), 0.0, 1.0)
    expansion_control = _clip((effective_max_pullback_expansion - row.pullback_expansion_ratio) / max(effective_max_pullback_expansion, 1e-9), 0.0, 1.0)
    vol_contraction = _clip((1.25 - row.realized_volatility_ratio) / 0.60, 0.0, 1.0)
    score = (
        0.22 * range_contraction
        + 0.16 * overlap_score
        + 0.14 * vwap_cluster
        + 0.12 * ema_cluster
        + 0.14 * adverse_control
        + 0.10 * expansion_control
        + 0.12 * vol_contraction
    )
    reasons = []
    if range_contraction >= 0.45:
        reasons.append("range_contracted")
    if overlap_score >= 0.40:
        reasons.append("coil_overlap_supportive")
    if vwap_cluster >= 0.40:
        reasons.append("near_vwap")
    if ema_cluster >= 0.40:
        reasons.append("near_slow_ema")
    if adverse_control >= 0.40:
        reasons.append("adverse_extension_controlled")
    if expansion_control >= 0.35:
        reasons.append("countertrend_expansion_muted")
    forming_threshold = profile.compression_forming_score_min * branch_profile.detector_forming_score_scale
    ready_threshold = profile.compression_ready_score_min * branch_profile.detector_ready_score_scale
    possible_threshold = profile.compression_forming_score_min * branch_profile.detector_possible_score_scale
    possible = (
        score >= possible_threshold
        or (
            range_contraction >= 0.20
            and overlap_score >= 0.25
            and adverse_control >= 0.20
            and vol_contraction >= 0.10
        )
    )
    insufficient_range_contraction = range_contraction < 0.20
    boundary_instability = adverse_control < 0.18 or expansion_control < 0.18
    vwap_ema_logic_block = vwap_cluster < 0.15 and ema_cluster < 0.15
    forming = score >= forming_threshold and len(prior) >= 2
    ready = score >= ready_threshold and len(prior) >= profile.min_forming_bars
    return {
        "score": score,
        "forming_threshold": forming_threshold,
        "ready_threshold": ready_threshold,
        "possible_threshold": possible_threshold,
        "possible": possible,
        "forming": forming,
        "ready": ready,
        "reasons": reasons,
        "range_norm": range_norm,
        "range_ratio": range_ratio,
        "range_contraction": range_contraction,
        "overlap_score": overlap_score,
        "vwap_cluster": vwap_cluster,
        "ema_cluster": ema_cluster,
        "adverse_control": adverse_control,
        "expansion_control": expansion_control,
        "vol_contraction": vol_contraction,
        "insufficient_range_contraction": insufficient_range_contraction,
        "boundary_instability": boundary_instability,
        "vwap_ema_logic_block": vwap_ema_logic_block,
    }


def _continuation_metrics(
    *,
    rows: Sequence[Any],
    index: int,
    direction: str,
    profile: CompressionContinuationProfile,
    branch_profile: CompressionBranchControlProfile,
    compression_high: float | None,
    compression_low: float | None,
    compression_range_norms: Sequence[float],
    mature_ready: bool,
    recent_false_break_warning: bool,
) -> dict[str, Any]:
    row = rows[index]
    if compression_high is None or compression_low is None or not compression_range_norms:
        return {
            "score": 0.0,
            "confirmed": False,
            "reasons": [],
            "boundary_break": False,
            "expansion_score": 0.0,
            "close_score": 0.0,
            "vwap_score": 0.0,
            "slope_score": 0.0,
            "efficiency_score": 0.0,
            "supportive_resume": False,
            "score_threshold": profile.continuation_score_min - branch_profile.continuation_score_relaxation,
        }
    range_norm = row.range_points / max(row.atr, 1e-9)
    baseline_range = float(median(compression_range_norms))
    expansion_ratio = range_norm / max(baseline_range, 1e-9)
    if direction == "LONG":
        boundary_break = row.close >= compression_high + (profile.breakout_buffer_atr * branch_profile.continuation_boundary_buffer_scale) * row.atr
        close_location = row.close_location
        slope_combo = row.slope_combo_long
    else:
        boundary_break = row.close <= compression_low - (profile.breakout_buffer_atr * branch_profile.continuation_boundary_buffer_scale) * row.atr
        close_location = 1.0 - row.close_location
        slope_combo = row.slope_combo_short
    close_score = _clip((close_location - 0.52) / 0.35, 0.0, 1.0)
    signed_vwap = row.renewed_signed_vwap_displacement
    vwap_score = _clip((signed_vwap - profile.min_signed_vwap_displacement) / 0.35, 0.0, 1.0)
    slope_score = _clip((slope_combo - profile.min_slope_combo) / 0.24, 0.0, 1.0)
    efficiency_improvement = row.efficiency_ratio_12 - 0.30
    efficiency_score = _clip((efficiency_improvement - profile.min_efficiency_improvement) / 0.25, 0.0, 1.0)
    expansion_score = _clip((expansion_ratio - profile.expansion_ratio_min) / 0.60, 0.0, 1.0)
    countertrend_score = 1.0 if row.countertrend_extension_failed else 0.0
    compression_resume_score = 1.0 if row.compression_followed_by_drift_expansion else 0.0
    score = (
        0.20 * expansion_score
        + 0.18 * close_score
        + 0.18 * vwap_score
        + 0.16 * slope_score
        + 0.12 * efficiency_score
        + 0.08 * countertrend_score
        + 0.08 * compression_resume_score
    )
    reasons = []
    if boundary_break:
        reasons.append("compression_boundary_break")
    if expansion_score >= 0.40:
        reasons.append("range_expansion_confirmed")
    if close_score >= 0.40:
        reasons.append("close_location_supportive")
    if vwap_score >= 0.35:
        reasons.append("renewed_vwap_displacement")
    if slope_score >= 0.35:
        reasons.append("slope_reacceleration")
    if efficiency_score >= 0.35:
        reasons.append("directional_efficiency_improved")
    if row.countertrend_extension_failed:
        reasons.append("countertrend_failed")
    if row.compression_followed_by_drift_expansion:
        reasons.append("compression_resume_feature")
    score_threshold = profile.continuation_score_min - branch_profile.continuation_score_relaxation
    supportive_resume = (
        row.compression_followed_by_drift_expansion
        or (
            branch_profile.allow_confirmation_after_failed_countertrend
            and row.countertrend_extension_failed
            and recent_false_break_warning
        )
    )
    confirmed = (
        boundary_break
        and score >= score_threshold
        and not row.post_spike_instability
        and (
            not branch_profile.continuation_requires_mature_ready
            or mature_ready
            or supportive_resume
        )
        and (not row.chop_veto or supportive_resume)
    )
    return {
        "score": score,
        "confirmed": confirmed,
        "reasons": reasons,
        "boundary_break": boundary_break,
        "expansion_score": expansion_score,
        "close_score": close_score,
        "vwap_score": vwap_score,
        "slope_score": slope_score,
        "efficiency_score": efficiency_score,
        "supportive_resume": supportive_resume,
        "score_threshold": score_threshold,
    }


def _false_break(
    *,
    row: Any,
    direction: str,
    profile: CompressionContinuationProfile,
    branch_profile: CompressionBranchControlProfile,
    compression_low: float | None,
    compression_high: float | None,
    family: str,
    overlap_label: str,
) -> dict[str, Any]:
    reasons: list[str] = []
    active = False
    hard = False
    if overlap_label == "TRUE_CHOP":
        reasons.append("true_chop_overlap")
        active = True
    if row.post_spike_instability and family != "indices":
        reasons.append("post_spike_instability")
        active = True
        hard = True
    if row.pullback_depth_atr > profile.false_break_adverse_atr or row.pullback_state == "DISQUALIFYING_PULLBACK":
        reasons.append("adverse_break_exceeded")
        active = True
        hard = True
    if direction == "LONG" and compression_low is not None and row.close <= compression_low - 0.10 * row.atr:
        reasons.append("compression_broke_against_long")
        active = True
        if row.close <= compression_low - 0.24 * row.atr:
            hard = True
    if direction == "SHORT" and compression_high is not None and row.close >= compression_high + 0.10 * row.atr:
        reasons.append("compression_broke_against_short")
        active = True
        if row.close >= compression_high + 0.24 * row.atr:
            hard = True
    if direction == "LONG":
        if row.close < row.session_vwap - 0.03 * row.atr and row.renewed_signed_vwap_displacement < profile.min_signed_vwap_displacement * 0.35:
            reasons.append("vwap_acceptance_against_thesis")
            active = True
        if row.close < row.slow_ema - 0.03 * row.atr and row.slope_combo_long < 0.0:
            reasons.append("ema_acceptance_against_thesis")
            active = True
    else:
        if row.close > row.session_vwap + 0.03 * row.atr and row.renewed_signed_vwap_displacement < profile.min_signed_vwap_displacement * 0.35:
            reasons.append("vwap_acceptance_against_thesis")
            active = True
        if row.close > row.slow_ema + 0.03 * row.atr and row.slope_combo_short < 0.0:
            reasons.append("ema_acceptance_against_thesis")
            active = True
    if row.regime_persistence_score < profile.regime_persistence_min * 0.55 and row.bars_since_last_drift_impulse >= branch_profile.false_break_confirmation_bars:
        reasons.append("drift_context_decay_persistent")
        active = True
    return {"active": active, "hard": hard, "reasons": reasons}


def _compression_window_row(
    *,
    instrument: str,
    family: str,
    window_label: str,
    session_id: str,
    rows: Sequence[Any],
    start_index: int,
    end_index: int,
    scores: Sequence[float],
    state_resolved_to: str,
    continuation_detected: bool,
) -> dict[str, Any]:
    start_row = rows[start_index]
    end_row = rows[end_index]
    return {
        "instrument": instrument,
        "instrument_family": family,
        "window_label": window_label,
        "asia_drift_session_id": session_id,
        "start_ts": start_row.decision_ts.isoformat(),
        "end_ts": end_row.decision_ts.isoformat(),
        "bar_count": max(end_index - start_index + 1, 0),
        "max_compression_score": max(scores) if scores else None,
        "median_compression_score": _median(scores),
        "resolved_to_state": state_resolved_to,
        "continuation_detected": continuation_detected,
    }


def _session_reason_tags(
    *,
    continuation_detected: bool,
    false_break_detected: bool,
    taxonomy_is_target: bool,
    taxonomy_row: dict[str, Any] | None,
) -> list[str]:
    tags: list[str] = []
    if continuation_detected:
        tags.append("branch_continuation_confirmed")
    if false_break_detected:
        tags.append("branch_false_break_detected")
    if taxonomy_is_target:
        tags.append("taxonomy_compression_target")
    if taxonomy_row is not None and taxonomy_row.get("missed_taxonomy_compression_session"):
        tags.append("taxonomy_missed")
    return tags


def _split_reason_tags(value: Any) -> list[str]:
    return [tag for tag in str(value or "").split("|") if tag]


def _candidate_peak_state(per_bar_rows: Sequence[dict[str, Any]]) -> str:
    states = {row["state"] for row in per_bar_rows}
    if CONTINUATION_CONFIRMED in states:
        return CONTINUATION_CONFIRMED
    if COMPRESSION_READY in states:
        return COMPRESSION_READY
    if COMPRESSION_FORMING in states:
        return COMPRESSION_FORMING
    if DRIFT_CONTEXT in states:
        return DRIFT_CONTEXT
    return NO_BRANCH


def _candidate_lifecycle_outcome(
    *,
    per_bar_rows: Sequence[dict[str, Any]],
    compression_forming: bool,
    compression_ready: bool,
    continuation_confirmed: bool,
    false_break_or_chop: bool,
    killed_by_post_spike: bool,
    killed_by_deep_damage: bool,
) -> str:
    if continuation_confirmed:
        return CONTINUATION_CONFIRMED
    candidate_seen = compression_forming or compression_ready
    if candidate_seen and (false_break_or_chop or killed_by_post_spike or killed_by_deep_damage):
        return REJECTION_CONFIRMED
    if candidate_seen and per_bar_rows and per_bar_rows[-1]["state"] == SESSION_TIMEOUT:
        return SESSION_TIMEOUT_UNRESOLVED
    if candidate_seen:
        return UNRESOLVED_PENDING
    return NO_BRANCH


def _candidate_cleanliness(
    *,
    lifecycle_outcome: str,
    killed_by_post_spike: bool,
    killed_by_deep_damage: bool,
    candidate_seen: bool,
) -> str:
    if not candidate_seen:
        return "no_candidate"
    if killed_by_post_spike:
        return "post_spike_contaminated_candidate"
    if killed_by_deep_damage:
        return "deep_damage_contaminated_candidate"
    if lifecycle_outcome in {UNRESOLVED_PENDING, SESSION_TIMEOUT_UNRESOLVED}:
        return "unresolved_clean_candidate"
    return "clean_candidate"


def _maturation_feature_failure(
    *,
    per_bar_rows: Sequence[dict[str, Any]],
    session_stage_row: dict[str, Any],
    lifecycle_outcome: str,
) -> str | None:
    if not per_bar_rows:
        return None
    if any(bool(row.get("post_spike_active")) for row in per_bar_rows):
        return "post_spike_disorder"
    if any(bool(row.get("deep_damage_active")) for row in per_bar_rows):
        return "deep_damage"
    if not session_stage_row["compression_forming"]:
        if session_stage_row["killed_by_insufficient_range_contraction"]:
            return "range_contraction"
        if session_stage_row["killed_by_boundary_instability"]:
            return "boundary_stability"
        if session_stage_row["killed_by_vwap_ema_logic"]:
            return "drift_context"
        return "compression_recognition"

    candidate_rows = [
        row
        for row in per_bar_rows
        if row["state"] in {COMPRESSION_FORMING, COMPRESSION_READY, DRIFT_CONTEXT, SESSION_TIMEOUT}
    ]
    if not candidate_rows:
        candidate_rows = list(per_bar_rows)
    max_expansion = max((float(row.get("continuation_expansion_score") or 0.0) for row in candidate_rows), default=0.0)
    max_close = max((float(row.get("continuation_close_score") or 0.0) for row in candidate_rows), default=0.0)
    max_vwap = max((float(row.get("continuation_vwap_score") or 0.0) for row in candidate_rows), default=0.0)
    max_slope = max((float(row.get("continuation_slope_score") or 0.0) for row in candidate_rows), default=0.0)
    max_boundary_break = max((1.0 if row.get("continuation_boundary_break") else 0.0 for row in candidate_rows), default=0.0)
    max_continuation_score = max((float(row.get("continuation_score") or 0.0) for row in candidate_rows), default=0.0)
    max_threshold = max((float(row.get("continuation_score_threshold") or 0.0) for row in candidate_rows), default=0.0)
    if lifecycle_outcome == REJECTION_CONFIRMED:
        reason_tags = Counter(tag for row in candidate_rows for tag in _split_reason_tags(row.get("reason_tags")))
        if reason_tags.get("compression_broke_against_long", 0) or reason_tags.get("compression_broke_against_short", 0):
            return "boundary_break_against_thesis"
        if reason_tags.get("vwap_acceptance_against_thesis", 0):
            return "vwap_displacement"
        if reason_tags.get("ema_acceptance_against_thesis", 0):
            return "slope_recovery"
        if reason_tags.get("drift_context_decay_persistent", 0):
            return "drift_context"
        if reason_tags.get("true_chop_overlap", 0) or any(bool(row.get("true_chop_overlap")) for row in candidate_rows):
            return "true_chop_deterioration"
        return "true_chop_deterioration"
    if max_boundary_break < 1.0 and max_expansion < 0.35:
        return "expansion"
    if max_close < 0.35:
        return "close_location"
    if max_vwap < 0.35:
        return "vwap_displacement"
    if max_slope < 0.35:
        return "slope_recovery"
    if max_threshold > 0.0 and max_continuation_score < max_threshold:
        return "continuation_score"
    return "pending_maturation"


def _candidate_lifecycle_row(
    *,
    instrument: str,
    family: str,
    window_label: str,
    session_id: str,
    branch_profile_name: str,
    branch_profile_role: str,
    taxonomy_type: str | None,
    per_bar_rows: Sequence[dict[str, Any]],
    session_stage_row: dict[str, Any],
) -> dict[str, Any]:
    compression_forming = bool(session_stage_row["compression_forming"])
    compression_ready = bool(session_stage_row["compression_ready"])
    continuation_confirmed = bool(session_stage_row["continuation_confirmed"])
    false_break_or_chop = bool(session_stage_row["false_break_or_chop"])
    killed_by_post_spike = bool(session_stage_row["killed_by_post_spike"])
    killed_by_deep_damage = bool(session_stage_row["killed_by_deep_damage"])
    candidate_seen = compression_forming or compression_ready or continuation_confirmed
    lifecycle_outcome = _candidate_lifecycle_outcome(
        per_bar_rows=per_bar_rows,
        compression_forming=compression_forming,
        compression_ready=compression_ready,
        continuation_confirmed=continuation_confirmed,
        false_break_or_chop=false_break_or_chop,
        killed_by_post_spike=killed_by_post_spike,
        killed_by_deep_damage=killed_by_deep_damage,
    )
    first_forming_index = next((index for index, row in enumerate(per_bar_rows) if row["state"] == COMPRESSION_FORMING), None)
    first_ready_index = next((index for index, row in enumerate(per_bar_rows) if row["state"] == COMPRESSION_READY), None)
    first_confirmation_index = next((index for index, row in enumerate(per_bar_rows) if row["state"] == CONTINUATION_CONFIRMED), None)
    first_rejection_index = next((index for index, row in enumerate(per_bar_rows) if row["state"] == FALSE_BREAK_OR_CHOP), None)
    last_candidate_index = max(
        (index for index, row in enumerate(per_bar_rows) if row["state"] in {COMPRESSION_FORMING, COMPRESSION_READY}),
        default=None,
    )
    outcome_index = (
        first_confirmation_index
        if first_confirmation_index is not None
        else first_rejection_index
        if first_rejection_index is not None
        else len(per_bar_rows) - 1
        if per_bar_rows
        else None
    )
    candidate_start_index = first_forming_index if first_forming_index is not None else first_ready_index
    candidate_end_index = outcome_index if outcome_index is not None else last_candidate_index
    candidate_phase_rows = (
        per_bar_rows[candidate_start_index : candidate_end_index + 1]
        if candidate_start_index is not None and candidate_end_index is not None and candidate_end_index >= candidate_start_index
        else []
    )
    candidate_phase_post_spike = any(bool(row.get("post_spike_active")) for row in candidate_phase_rows)
    candidate_phase_deep_damage = any(bool(row.get("deep_damage_active")) for row in candidate_phase_rows)
    cleanliness = _candidate_cleanliness(
        lifecycle_outcome=lifecycle_outcome,
        killed_by_post_spike=candidate_phase_post_spike,
        killed_by_deep_damage=candidate_phase_deep_damage,
        candidate_seen=candidate_seen,
    )
    pending_bar_count = sum(1 for row in per_bar_rows if row["state"] in {COMPRESSION_FORMING, COMPRESSION_READY})
    pending_duration_bars = (
        max((outcome_index or 0) - (first_forming_index or 0), 0) + 1
        if first_forming_index is not None and outcome_index is not None
        else 0
    )
    ready_pending_duration_bars = (
        max((outcome_index or 0) - (first_ready_index or 0), 0) + 1
        if first_ready_index is not None and outcome_index is not None
        else 0
    )
    continuation_after_timeout_proxy = (
        lifecycle_outcome == SESSION_TIMEOUT_UNRESOLVED
        and any(
            float(row.get("continuation_score") or 0.0) >= float(row.get("continuation_score_threshold") or 0.0) * 0.85
            for row in per_bar_rows[-3:]
        )
    )
    adverse_reason_tags = {
        "adverse_break_exceeded",
        "compression_broke_against_long",
        "compression_broke_against_short",
        "vwap_acceptance_against_thesis",
        "ema_acceptance_against_thesis",
        "drift_context_decay_persistent",
        "post_spike_instability",
        "true_chop_overlap",
        "false_break_confirmed",
    }
    rejection_reason_counts = Counter(
        tag
        for row in candidate_phase_rows
        if row["state"] == FALSE_BREAK_OR_CHOP
        for tag in _split_reason_tags(row.get("reason_tags"))
        if tag in adverse_reason_tags
    )
    maturation_feature_failure = _maturation_feature_failure(
        per_bar_rows=candidate_phase_rows or per_bar_rows,
        session_stage_row=session_stage_row,
        lifecycle_outcome=lifecycle_outcome,
    )
    return {
        "instrument": instrument,
        "instrument_family": family,
        "window_label": window_label,
        "asia_drift_session_id": session_id,
        "branch_profile": branch_profile_name,
        "branch_profile_role": branch_profile_role,
        "taxonomy_opportunity_type": taxonomy_type,
        "taxonomy_is_compression_target": taxonomy_type == OPP_COMPRESSION_THEN_CONTINUATION,
        "candidate_peak_state": _candidate_peak_state(per_bar_rows),
        "lifecycle_outcome": lifecycle_outcome,
        "candidate_cleanliness": cleanliness,
        "candidate_seen": candidate_seen,
        "compression_forming_seen": compression_forming,
        "compression_ready_seen": compression_ready,
        "continuation_confirmed": continuation_confirmed,
        "rejection_confirmed": lifecycle_outcome == REJECTION_CONFIRMED,
        "unresolved_pending": lifecycle_outcome == UNRESOLVED_PENDING,
        "session_timeout_unresolved": lifecycle_outcome == SESSION_TIMEOUT_UNRESOLVED,
        "pending_bar_count": pending_bar_count,
        "pending_duration_bars": pending_duration_bars,
        "ready_pending_duration_bars": ready_pending_duration_bars,
        "bars_from_first_forming_to_outcome": pending_duration_bars,
        "bars_from_first_ready_to_outcome": ready_pending_duration_bars,
        "first_forming_ts": per_bar_rows[first_forming_index]["decision_ts"] if first_forming_index is not None else None,
        "first_ready_ts": per_bar_rows[first_ready_index]["decision_ts"] if first_ready_index is not None else None,
        "outcome_ts": per_bar_rows[outcome_index]["decision_ts"] if outcome_index is not None and per_bar_rows else None,
        "last_candidate_ts": per_bar_rows[last_candidate_index]["decision_ts"] if last_candidate_index is not None else None,
        "late_continuation_proxy_after_timeout": continuation_after_timeout_proxy,
        "post_spike_contaminated": candidate_phase_post_spike,
        "deep_damage_contaminated": candidate_phase_deep_damage,
        "adverse_rejection_reason_tags": [tag for tag, _ in rejection_reason_counts.most_common(6)],
        "maturation_failure_feature": maturation_feature_failure,
    }


def _missed_target_audit(
    *,
    instrument: str,
    family: str,
    window_label: str,
    session_id: str,
    taxonomy_is_target: bool,
    continuation_detected: bool,
    compression_ready_detected: bool,
    state_counts: Counter[str],
    per_bar_rows: Sequence[dict[str, Any]],
    candidate_lifecycle_row: dict[str, Any],
) -> dict[str, Any] | None:
    if not taxonomy_is_target or continuation_detected:
        return None
    any_drift = state_counts[DRIFT_CONTEXT] > 0 or state_counts[COMPRESSION_FORMING] > 0 or state_counts[COMPRESSION_READY] > 0
    any_forming = state_counts[COMPRESSION_FORMING] > 0
    blocked_tags = Counter(
        tag
        for row in per_bar_rows
        for tag in _split_reason_tags(row.get("reason_tags"))
        if tag
    )
    lifecycle_outcome = candidate_lifecycle_row["lifecycle_outcome"]
    if not any_drift:
        failed_stage = "drift_context_missing"
    elif not any_forming and not compression_ready_detected:
        failed_stage = "compression_not_recognized"
    elif lifecycle_outcome == REJECTION_CONFIRMED:
        failed_stage = "rejection_confirmed"
    elif compression_ready_detected:
        failed_stage = "continuation_not_confirmed"
    elif lifecycle_outcome == SESSION_TIMEOUT_UNRESOLVED:
        failed_stage = "session_timeout_unresolved"
    elif lifecycle_outcome == UNRESOLVED_PENDING:
        failed_stage = "unresolved_pending"
    else:
        failed_stage = "other_unconfirmed"
    return {
        "instrument": instrument,
        "instrument_family": family,
        "window_label": window_label,
        "asia_drift_session_id": session_id,
        "failed_stage": failed_stage,
        "lifecycle_outcome": lifecycle_outcome,
        "drift_context_missing": not any_drift,
        "compression_not_recognized": not any_forming and not compression_ready_detected,
        "rejection_confirmed": lifecycle_outcome == REJECTION_CONFIRMED,
        "continuation_not_confirmed": lifecycle_outcome in {UNRESOLVED_PENDING, SESSION_TIMEOUT_UNRESOLVED} and compression_ready_detected,
        "session_timeout_unresolved": lifecycle_outcome == SESSION_TIMEOUT_UNRESOLVED,
        "unresolved_pending": lifecycle_outcome == UNRESOLVED_PENDING,
        "post_spike_blocked": blocked_tags.get("post_spike_present", 0) > 0 or blocked_tags.get("post_spike_instability", 0) > 0,
        "deep_damage_blocked": blocked_tags.get("adverse_break_exceeded", 0) > 0,
        "chop_blocked": blocked_tags.get("true_chop_overlap", 0) > 0 or blocked_tags.get("chop_veto_present", 0) > 0,
        "false_break_warning_seen": blocked_tags.get("false_break_warning_pending", 0) > 0,
        "compression_compatible_overlap_seen": blocked_tags.get("compression_compatible_overlap", 0) > 0,
        "candidate_cleanliness": candidate_lifecycle_row["candidate_cleanliness"],
        "maturation_failure_feature": candidate_lifecycle_row["maturation_failure_feature"],
        "dominant_reason_tags": [tag for tag, _ in blocked_tags.most_common(6)],
    }


def _session_stage_row(
    *,
    instrument: str,
    family: str,
    window_label: str,
    session_id: str,
    branch_profile_name: str,
    branch_profile_role: str,
    taxonomy_type: str | None,
    per_bar_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    total_bars = len(per_bar_rows)
    sufficient_data = total_bars >= 12
    drift_context = any(row["state"] != NO_BRANCH for row in per_bar_rows)
    possible_compression = any(bool(row["possible_compression_behavior"]) for row in per_bar_rows)
    compression_forming = any(row["state"] == COMPRESSION_FORMING for row in per_bar_rows)
    compression_ready = any(row["state"] == COMPRESSION_READY for row in per_bar_rows)
    continuation_confirmed = any(row["state"] == CONTINUATION_CONFIRMED for row in per_bar_rows)
    false_break_or_chop = any(row["state"] == FALSE_BREAK_OR_CHOP for row in per_bar_rows)
    killed_by_chop = any(bool(row["true_chop_overlap"]) for row in per_bar_rows)
    killed_by_post_spike = any(bool(row["post_spike_active"]) for row in per_bar_rows)
    killed_by_deep_damage = any(bool(row["deep_damage_active"]) for row in per_bar_rows)
    killed_by_range = not possible_compression and any(bool(row["insufficient_range_contraction"]) for row in per_bar_rows)
    killed_by_boundary = not compression_forming and any(bool(row["boundary_instability"]) for row in per_bar_rows)
    killed_by_vwap_ema = not compression_forming and any(bool(row["vwap_ema_logic_block"]) for row in per_bar_rows)
    if not sufficient_data:
        failed_stage = "insufficient_data"
    elif not drift_context:
        failed_stage = "drift_context_missing"
    elif not possible_compression:
        failed_stage = "possible_compression_not_detected"
    elif not compression_forming:
        if killed_by_post_spike:
            failed_stage = "killed_by_post_spike"
        elif killed_by_deep_damage:
            failed_stage = "killed_by_deep_damage"
        elif killed_by_chop:
            failed_stage = "killed_by_chop"
        elif killed_by_range:
            failed_stage = "killed_by_insufficient_range_contraction"
        elif killed_by_boundary:
            failed_stage = "killed_by_boundary_instability"
        elif killed_by_vwap_ema:
            failed_stage = "killed_by_vwap_ema_logic"
        else:
            failed_stage = "compression_not_recognized"
    elif not compression_ready:
        failed_stage = "compression_forming_only"
    elif not continuation_confirmed:
        failed_stage = "continuation_not_confirmed"
    else:
        failed_stage = "continuation_confirmed"
    lifecycle_outcome = _candidate_lifecycle_outcome(
        per_bar_rows=per_bar_rows,
        compression_forming=compression_forming,
        compression_ready=compression_ready,
        continuation_confirmed=continuation_confirmed,
        false_break_or_chop=false_break_or_chop,
        killed_by_post_spike=killed_by_post_spike,
        killed_by_deep_damage=killed_by_deep_damage,
    )
    candidate_seen = compression_forming or compression_ready or continuation_confirmed
    if lifecycle_outcome == REJECTION_CONFIRMED:
        failed_stage = "rejection_confirmed"
    elif lifecycle_outcome == SESSION_TIMEOUT_UNRESOLVED:
        failed_stage = "session_timeout_unresolved"
    elif lifecycle_outcome == UNRESOLVED_PENDING and failed_stage == "continuation_not_confirmed":
        failed_stage = "unresolved_pending"
    return {
        "instrument": instrument,
        "instrument_family": family,
        "window_label": window_label,
        "asia_drift_session_id": session_id,
        "branch_profile": branch_profile_name,
        "branch_profile_role": branch_profile_role,
        "taxonomy_opportunity_type": taxonomy_type,
        "taxonomy_is_compression_target": taxonomy_type == OPP_COMPRESSION_THEN_CONTINUATION,
        "total_bars_loaded": total_bars,
        "sufficient_data": sufficient_data,
        "directional_drift_context": drift_context,
        "possible_compression_behavior": possible_compression,
        "killed_by_chop": killed_by_chop,
        "killed_by_post_spike": killed_by_post_spike,
        "killed_by_deep_damage": killed_by_deep_damage,
        "killed_by_insufficient_range_contraction": killed_by_range,
        "killed_by_boundary_instability": killed_by_boundary,
        "killed_by_vwap_ema_logic": killed_by_vwap_ema,
        "compression_forming": compression_forming,
        "compression_ready": compression_ready,
        "continuation_confirmed": continuation_confirmed,
        "false_break_or_chop": false_break_or_chop,
        "candidate_seen": candidate_seen,
        "candidate_peak_state": _candidate_peak_state(per_bar_rows),
        "lifecycle_outcome": lifecycle_outcome,
        "rejection_confirmed": lifecycle_outcome == REJECTION_CONFIRMED,
        "unresolved_pending": lifecycle_outcome == UNRESOLVED_PENDING,
        "session_timeout_unresolved": lifecycle_outcome == SESSION_TIMEOUT_UNRESOLVED,
        "failed_stage": failed_stage,
    }


def _build_funnel_audit(
    *,
    per_bar_rows: Sequence[dict[str, Any]],
    session_stage_rows: Sequence[dict[str, Any]],
    missed_target_audit: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    taxonomy_rows = [row for row in session_stage_rows if row["taxonomy_is_compression_target"]]
    return {
        "total_bars_loaded": len(per_bar_rows),
        "total_asia_sessions_scanned": len(session_stage_rows),
        "sessions_with_sufficient_data": sum(1 for row in session_stage_rows if row["sufficient_data"]),
        "sessions_with_directional_drift_context": sum(1 for row in session_stage_rows if row["directional_drift_context"]),
        "sessions_with_possible_compression_behavior": sum(1 for row in session_stage_rows if row["possible_compression_behavior"]),
        "sessions_killed_by_chop": sum(1 for row in session_stage_rows if row["killed_by_chop"]),
        "sessions_killed_by_post_spike": sum(1 for row in session_stage_rows if row["killed_by_post_spike"]),
        "sessions_killed_by_deep_damage": sum(1 for row in session_stage_rows if row["killed_by_deep_damage"]),
        "sessions_killed_by_insufficient_range_contraction": sum(1 for row in session_stage_rows if row["killed_by_insufficient_range_contraction"]),
        "sessions_killed_by_boundary_instability": sum(1 for row in session_stage_rows if row["killed_by_boundary_instability"]),
        "sessions_killed_by_vwap_ema_logic": sum(1 for row in session_stage_rows if row["killed_by_vwap_ema_logic"]),
        "sessions_classified_compression_forming": sum(1 for row in session_stage_rows if row["compression_forming"]),
        "sessions_classified_compression_ready": sum(1 for row in session_stage_rows if row["compression_ready"]),
        "sessions_reaching_continuation_confirmation": sum(1 for row in session_stage_rows if row["continuation_confirmed"]),
        "sessions_rejection_confirmed": sum(1 for row in session_stage_rows if row["rejection_confirmed"]),
        "sessions_unresolved_pending": sum(1 for row in session_stage_rows if row["unresolved_pending"]),
        "sessions_timeout_unresolved": sum(1 for row in session_stage_rows if row["session_timeout_unresolved"]),
        "taxonomy_target_stage_failures": dict(Counter(row["failed_stage"] for row in taxonomy_rows)),
        "taxonomy_target_lifecycle_outcomes": dict(Counter(row["lifecycle_outcome"] for row in taxonomy_rows)),
        "missed_target_audit_rows": len(missed_target_audit),
    }


def _aggregate_branch_summary(
    *,
    session_rows: Sequence[dict[str, Any]],
    continuation_rows: Sequence[dict[str, Any]],
    false_break_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    target_rows = [row for row in session_rows if row["taxonomy_is_compression_target"]]
    detected_rows = [row for row in session_rows if row["branch_detected_continuation_confirmed"]]
    true_positives = [row for row in target_rows if row["branch_detected_continuation_confirmed"]]
    false_negatives = [row for row in target_rows if not row["branch_detected_continuation_confirmed"]]
    false_positives = [row for row in detected_rows if not row["taxonomy_is_compression_target"]]
    return {
        "session_count": len(session_rows),
        "branch_drift_context_count": sum(1 for row in session_rows if row["branch_detected_drift_context"]),
        "compression_ready_count": sum(1 for row in session_rows if row["branch_detected_compression_ready"]),
        "continuation_confirmed_count": len(detected_rows),
        "false_break_or_chop_count": sum(1 for row in session_rows if row["branch_false_break_or_chop"]),
        "clean_unresolved_count": sum(1 for row in session_rows if row.get("candidate_cleanliness") == "unresolved_clean_candidate"),
        "confirmed_rejection_count": sum(1 for row in session_rows if row.get("candidate_lifecycle_outcome") == REJECTION_CONFIRMED),
        "contaminated_candidate_count": sum(
            1
            for row in session_rows
            if row.get("candidate_cleanliness") in {"post_spike_contaminated_candidate", "deep_damage_contaminated_candidate"}
        ),
        "missed_taxonomy_compression_count": sum(1 for row in session_rows if row["missed_taxonomy_compression_session"]),
        "taxonomy_target_count": len(target_rows),
        "precision_against_taxonomy": _rate(len(true_positives), len(detected_rows)),
        "recall_against_taxonomy": _rate(len(true_positives), len(target_rows)),
        "true_positive_count": len(true_positives),
        "false_negative_count": len(false_negatives),
        "false_positive_count": len(false_positives),
        "state_reason_counts": dict(Counter(tag for row in session_rows for tag in row["reason_tags"])),
        "continuation_confirmation_count": len(continuation_rows),
        "false_break_count": len(false_break_rows),
    }


def _build_evaluation(
    *,
    session_rows: Sequence[dict[str, Any]],
    family_summary: dict[str, Any],
    instrument_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "family_comparison": {
            family: {
                "precision_against_taxonomy": summary.get("precision_against_taxonomy"),
                "recall_against_taxonomy": summary.get("recall_against_taxonomy"),
                "taxonomy_target_count": summary.get("taxonomy_target_count"),
                "continuation_confirmed_count": summary.get("continuation_confirmed_count"),
                "false_break_or_chop_count": summary.get("false_break_or_chop_count"),
            }
            for family, summary in sorted(family_summary.items())
        },
        "instrument_comparison": {
            instrument: {
                "precision_against_taxonomy": summary.get("precision_against_taxonomy"),
                "recall_against_taxonomy": summary.get("recall_against_taxonomy"),
                "taxonomy_target_count": summary.get("taxonomy_target_count"),
                "continuation_confirmed_count": summary.get("continuation_confirmed_count"),
            }
            for instrument, summary in sorted(instrument_summary.items())
        },
        "deprioritized_nq_contamination": {
            "session_count": sum(1 for row in session_rows if row["instrument"] in {"NQ", "MNQ"}),
            "continuation_confirmed_count": sum(1 for row in session_rows if row["instrument"] in {"NQ", "MNQ"} and row["branch_detected_continuation_confirmed"]),
            "taxonomy_target_count": sum(1 for row in session_rows if row["instrument"] in {"NQ", "MNQ"} and row["taxonomy_is_compression_target"]),
        },
    }


def _recommendation(*, family_summary: dict[str, Any], evaluation: dict[str, Any]) -> dict[str, Any]:
    metals = family_summary.get("metals", {})
    indices = family_summary.get("indices", {})
    if (
        float(metals.get("recall_against_taxonomy") or 0.0) >= 0.50
        and float(indices.get("recall_against_taxonomy") or 0.0) >= 0.40
        and float(metals.get("precision_against_taxonomy") or 0.0) >= 0.50
    ):
        verdict = "begin_phase2_entry_exit_research"
        reason = "The compression branch is capturing a meaningful share of taxonomy-labelled sessions with explicit false-break accounting."
    else:
        verdict = "continue_branch_diagnostics_only"
        reason = "The compression branch is promising, but taxonomy recall/precision are not yet strong enough for immediate Phase 2 expansion."
    return {
        "verdict": verdict,
        "next_primary_branch": OPP_COMPRESSION_THEN_CONTINUATION,
        "shared_architecture_recommendation": "shared_cross_asset_detector_with_family_specific_thresholds",
        "reason": reason,
        "deprioritized_nq_contamination": evaluation.get("deprioritized_nq_contamination"),
    }


def _write_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    layout = build_layout(output_dir)
    summary_json_path = layout["reports"] / "asia_drift_compression_branch_summary.json"
    summary_markdown_path = layout["reports"] / "asia_drift_compression_branch_summary.md"
    comparison_json_path = layout["reports"] / "asia_drift_compression_branch_profile_comparison.json"
    comparison_markdown_path = layout["reports"] / "asia_drift_compression_branch_profile_comparison.md"
    funnel_json_path = layout["reports"] / "asia_drift_compression_funnel_audit.json"
    funnel_markdown_path = layout["reports"] / "asia_drift_compression_funnel_audit.md"
    maturation_json_path = layout["reports"] / "asia_drift_compression_candidate_maturation_summary.json"
    maturation_markdown_path = layout["reports"] / "asia_drift_compression_candidate_maturation_summary.md"
    per_bar_rows_path = layout["signals"] / "asia_drift_compression_branch_state_rows.csv"
    compression_windows_path = layout["reports"] / "asia_drift_compression_windows.csv"
    continuation_confirmations_path = layout["signals"] / "asia_drift_compression_continuations.csv"
    false_breaks_path = layout["signals"] / "asia_drift_compression_false_breaks.csv"
    missed_target_audit_path = layout["signals"] / "asia_drift_compression_missed_target_audit.csv"
    session_stage_rows_path = layout["signals"] / "asia_drift_compression_session_stage_failures.csv"
    candidate_lifecycle_rows_path = layout["signals"] / "asia_drift_compression_candidate_lifecycle_rows.csv"
    candidate_discovery_manifest_path = layout["reports"] / "asia_drift_compression_candidate_discovery_sessions.json"
    pending_unresolved_manifest_path = layout["reports"] / "asia_drift_compression_pending_unresolved_sessions.json"
    confirmed_rejection_manifest_path = layout["reports"] / "asia_drift_compression_confirmed_rejections.json"
    contamination_breakdown_path = layout["reports"] / "asia_drift_compression_contamination_breakdown.json"
    session_summary_path = layout["reports"] / "asia_drift_compression_branch_sessions.json"

    summary_payload = {key: value for key, value in payload.items() if not key.startswith("_")}
    summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown_path.write_text(_render_markdown(summary_payload), encoding="utf-8")
    comparison_json_path.write_text(json.dumps(payload.get("profile_comparison_rows") or [], indent=2, sort_keys=True), encoding="utf-8")
    comparison_markdown_path.write_text(_render_profile_comparison_markdown(summary_payload), encoding="utf-8")
    funnel_json_path.write_text(json.dumps(payload.get("funnel_audit") or {}, indent=2, sort_keys=True), encoding="utf-8")
    funnel_markdown_path.write_text(_render_funnel_markdown(summary_payload), encoding="utf-8")
    maturation_json_path.write_text(json.dumps(payload.get("candidate_maturation_summary") or {}, indent=2, sort_keys=True), encoding="utf-8")
    maturation_markdown_path.write_text(_render_candidate_maturation_markdown(summary_payload), encoding="utf-8")
    _write_csv(per_bar_rows_path, payload.get("_per_bar_rows") or [])
    _write_csv(compression_windows_path, payload.get("_compression_windows") or [])
    _write_csv(continuation_confirmations_path, payload.get("_continuation_confirmations") or [])
    _write_csv(false_breaks_path, payload.get("_false_breaks") or [])
    _write_csv(missed_target_audit_path, payload.get("_profile_artifacts", {}).get(payload.get("selected_profile_name"), {}).get("missed_target_audit") or [])
    _write_csv(session_stage_rows_path, payload.get("_session_stage_rows") or [])
    _write_csv(candidate_lifecycle_rows_path, payload.get("_candidate_lifecycle_rows") or [])
    candidate_discovery_payload = payload.get("profile_results", {}).get(BRANCH_PROFILE_CANDIDATE_DISCOVERY, {}).get("per_session_summary") or []
    candidate_discovery_manifest_path.write_text(json.dumps(candidate_discovery_payload, indent=2, sort_keys=True), encoding="utf-8")
    pending_unresolved_manifest = [
        row for row in (payload.get("_candidate_lifecycle_rows") or []) if row.get("lifecycle_outcome") in {UNRESOLVED_PENDING, SESSION_TIMEOUT_UNRESOLVED}
    ]
    confirmed_rejection_manifest = [
        row for row in (payload.get("_candidate_lifecycle_rows") or []) if row.get("lifecycle_outcome") == REJECTION_CONFIRMED
    ]
    contamination_breakdown = dict(
        Counter(
            row.get("candidate_cleanliness")
            for row in (payload.get("_candidate_lifecycle_rows") or [])
            if row.get("candidate_cleanliness") and row.get("candidate_cleanliness") != "no_candidate"
        )
    )
    pending_unresolved_manifest_path.write_text(json.dumps(pending_unresolved_manifest, indent=2, sort_keys=True), encoding="utf-8")
    confirmed_rejection_manifest_path.write_text(json.dumps(confirmed_rejection_manifest, indent=2, sort_keys=True), encoding="utf-8")
    contamination_breakdown_path.write_text(json.dumps(contamination_breakdown, indent=2, sort_keys=True), encoding="utf-8")
    session_summary_path.write_text(json.dumps(payload.get("per_session_summary") or [], indent=2, sort_keys=True), encoding="utf-8")

    profile_artifact_paths: dict[str, dict[str, str]] = {}
    for profile_name, artifact_rows in sorted((payload.get("_profile_artifacts") or {}).items()):
        per_profile_rows_path = layout["signals"] / f"asia_drift_compression_branch_state_rows_{profile_name}.csv"
        per_profile_windows_path = layout["reports"] / f"asia_drift_compression_windows_{profile_name}.csv"
        per_profile_continuations_path = layout["signals"] / f"asia_drift_compression_continuations_{profile_name}.csv"
        per_profile_false_breaks_path = layout["signals"] / f"asia_drift_compression_false_breaks_{profile_name}.csv"
        per_profile_missed_audit_path = layout["signals"] / f"asia_drift_compression_missed_target_audit_{profile_name}.csv"
        per_profile_stage_rows_path = layout["signals"] / f"asia_drift_compression_session_stage_failures_{profile_name}.csv"
        per_profile_lifecycle_rows_path = layout["signals"] / f"asia_drift_compression_candidate_lifecycle_rows_{profile_name}.csv"
        per_profile_sessions_path = layout["reports"] / f"asia_drift_compression_branch_sessions_{profile_name}.json"
        profile_payload = payload["profile_results"][profile_name]
        _write_csv(per_profile_rows_path, artifact_rows.get("per_bar_rows") or [])
        _write_csv(per_profile_windows_path, artifact_rows.get("compression_windows") or [])
        _write_csv(per_profile_continuations_path, artifact_rows.get("continuation_confirmations") or [])
        _write_csv(per_profile_false_breaks_path, artifact_rows.get("false_breaks") or [])
        _write_csv(per_profile_missed_audit_path, artifact_rows.get("missed_target_audit") or [])
        _write_csv(per_profile_stage_rows_path, artifact_rows.get("session_stage_rows") or [])
        _write_csv(per_profile_lifecycle_rows_path, artifact_rows.get("candidate_lifecycle_rows") or [])
        per_profile_sessions_path.write_text(json.dumps(profile_payload.get("per_session_summary") or [], indent=2, sort_keys=True), encoding="utf-8")
        profile_artifact_paths[profile_name] = {
            "per_bar_rows_path": str(per_profile_rows_path),
            "compression_windows_path": str(per_profile_windows_path),
            "continuation_confirmations_path": str(per_profile_continuations_path),
            "false_breaks_path": str(per_profile_false_breaks_path),
            "missed_target_audit_path": str(per_profile_missed_audit_path),
            "session_stage_rows_path": str(per_profile_stage_rows_path),
            "candidate_lifecycle_rows_path": str(per_profile_lifecycle_rows_path),
            "session_summary_path": str(per_profile_sessions_path),
        }

    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_compression_then_continuation_branch",
            "layout": {key: str(value) for key, value in layout.items()},
            "artifact_paths": {
                "summary_json_path": str(summary_json_path),
                "summary_markdown_path": str(summary_markdown_path),
                "comparison_json_path": str(comparison_json_path),
                "comparison_markdown_path": str(comparison_markdown_path),
                "funnel_json_path": str(funnel_json_path),
                "funnel_markdown_path": str(funnel_markdown_path),
                "maturation_json_path": str(maturation_json_path),
                "maturation_markdown_path": str(maturation_markdown_path),
                "per_bar_rows_path": str(per_bar_rows_path),
                "compression_windows_path": str(compression_windows_path),
                "continuation_confirmations_path": str(continuation_confirmations_path),
                "false_breaks_path": str(false_breaks_path),
                "missed_target_audit_path": str(missed_target_audit_path),
                "session_stage_rows_path": str(session_stage_rows_path),
                "candidate_lifecycle_rows_path": str(candidate_lifecycle_rows_path),
                "candidate_discovery_manifest_path": str(candidate_discovery_manifest_path),
                "pending_unresolved_manifest_path": str(pending_unresolved_manifest_path),
                "confirmed_rejection_manifest_path": str(confirmed_rejection_manifest_path),
                "contamination_breakdown_path": str(contamination_breakdown_path),
                "session_summary_path": str(session_summary_path),
                "profile_artifact_paths": profile_artifact_paths,
            },
        },
    )
    return {
        "summary_json_path": str(summary_json_path),
        "summary_markdown_path": str(summary_markdown_path),
        "comparison_json_path": str(comparison_json_path),
        "comparison_markdown_path": str(comparison_markdown_path),
        "funnel_json_path": str(funnel_json_path),
        "funnel_markdown_path": str(funnel_markdown_path),
        "maturation_json_path": str(maturation_json_path),
        "maturation_markdown_path": str(maturation_markdown_path),
        "per_bar_rows_path": str(per_bar_rows_path),
        "compression_windows_path": str(compression_windows_path),
        "continuation_confirmations_path": str(continuation_confirmations_path),
        "false_breaks_path": str(false_breaks_path),
        "missed_target_audit_path": str(missed_target_audit_path),
        "session_stage_rows_path": str(session_stage_rows_path),
        "candidate_lifecycle_rows_path": str(candidate_lifecycle_rows_path),
        "candidate_discovery_manifest_path": str(candidate_discovery_manifest_path),
        "pending_unresolved_manifest_path": str(pending_unresolved_manifest_path),
        "confirmed_rejection_manifest_path": str(confirmed_rejection_manifest_path),
        "contamination_breakdown_path": str(contamination_breakdown_path),
        "session_summary_path": str(session_summary_path),
        "storage_manifest_path": str(layout["storage_manifest"]),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia Drift Compression Then Continuation Detector",
        "",
        str(payload.get("objective") or ""),
        "",
        "## Coverage",
    ]
    coverage = payload.get("study_window_coverage") or {}
    lines.append(
        f"- study_window_start={coverage.get('study_window_start')} study_window_end={coverage.get('study_window_end')} window_count={coverage.get('window_count')}"
    )
    db_coverage = payload.get("database_coverage") or {}
    if db_coverage.get("coverage_rows"):
        for row in db_coverage["coverage_rows"]:
            lines.append(
                f"- db {row.get('instrument')}: min={row.get('db_min_end_ts')} max={row.get('db_max_end_ts')} bars={row.get('bar_count')} missing={row.get('missing_bar_count')}"
            )
    if db_coverage.get("coverage_note"):
        lines.append(f"- {db_coverage.get('coverage_note')}")
    lines.extend([
        "",
        "## Family Profiles",
    ])
    for name, profile in sorted((payload.get("family_profiles") or {}).items()):
        lines.append(f"- {name}: family={profile.get('family')} forming_score={profile.get('compression_forming_score_min')} ready_score={profile.get('compression_ready_score_min')} continuation_score={profile.get('continuation_score_min')}")
    lines.extend(["", "## Branch Profiles"])
    for name, profile in sorted((payload.get("branch_profiles") or {}).items()):
        lines.append(
            f"- {name}: maturation={profile.get('forming_maturation_bars')}/{profile.get('ready_maturation_bars')} "
            f"false_break_confirm={profile.get('false_break_confirmation_bars')} "
            f"continuation_relax={profile.get('continuation_score_relaxation')}"
        )
    lines.extend(["", "## Profile Comparison"])
    for row in payload.get("profile_comparison_rows") or []:
        lines.append(
            f"- {row.get('branch_profile')} ({row.get('branch_profile_role')}): candidate_recall={row.get('candidate_detection_recall_against_taxonomy')} "
            f"ready_recall={row.get('ready_recall_against_taxonomy')} "
            f"metals_recall={row.get('metals_recall_against_taxonomy')} indices_recall={row.get('indices_recall_against_taxonomy')} "
            f"continuations={row.get('continuation_confirmed_count')} "
            f"compression_ready={row.get('compression_ready_count')} "
            f"false_breaks={row.get('false_break_count')} contaminated_candidates={row.get('contaminated_candidate_count')}"
        )
    lines.extend(["", "## Instrument Summary"])
    for instrument, summary in sorted((payload.get("instrument_summary") or {}).items()):
        lines.append(
            f"- {instrument}: taxonomy_target_count={summary.get('taxonomy_target_count')} "
            f"continuation_confirmed_count={summary.get('continuation_confirmed_count')} "
            f"precision={summary.get('precision_against_taxonomy')} recall={summary.get('recall_against_taxonomy')} "
            f"false_break_or_chop_count={summary.get('false_break_or_chop_count')}"
        )
    lines.extend(["", "## Family Summary"])
    for family, summary in sorted((payload.get("family_summary") or {}).items()):
        lines.append(
            f"- {family}: taxonomy_target_count={summary.get('taxonomy_target_count')} "
            f"continuation_confirmed_count={summary.get('continuation_confirmed_count')} "
            f"precision={summary.get('precision_against_taxonomy')} recall={summary.get('recall_against_taxonomy')} "
            f"false_break_or_chop_count={summary.get('false_break_or_chop_count')}"
        )
    lines.extend(["", "## Evaluation"])
    for family, summary in sorted(((payload.get("evaluation") or {}).get("family_comparison") or {}).items()):
        lines.append(f"- {family}: {summary}")
    lines.extend(["", "## Candidate Maturation"])
    maturation = payload.get("candidate_maturation_summary") or {}
    lines.append(
        f"- clean_unresolved_count={maturation.get('clean_unresolved_count')} "
        f"confirmed_rejection_count={maturation.get('confirmed_rejection_count')} "
        f"contaminated_candidate_count={maturation.get('contaminated_candidate_count')} "
        f"session_timeout_unresolved_count={maturation.get('session_timeout_unresolved_count')}"
    )
    metals_vs_indices = maturation.get("metals_vs_es_mes") or {}
    if metals_vs_indices:
        lines.append(f"- metals_vs_es_mes: {metals_vs_indices}")
    lines.extend(["", "## Recommendation"])
    recommendation = payload.get("recommendation") or {}
    lines.append(f"- recommended_detector_profile: {recommendation.get('recommended_detector_profile')}")
    lines.append(f"- recommended_trade_quality_profile: {recommendation.get('recommended_trade_quality_profile')}")
    lines.append(f"- verdict: {recommendation.get('verdict')}")
    lines.append(f"- next_primary_branch: {recommendation.get('next_primary_branch')}")
    lines.append(f"- reason: {recommendation.get('reason')}")
    lines.append(f"- deprioritized_nq_contamination: {recommendation.get('deprioritized_nq_contamination')}")
    return "\n".join(lines) + "\n"


def _render_profile_comparison_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia Drift Compression Branch Profile Comparison",
        "",
        "## Rows",
    ]
    for row in payload.get("profile_comparison_rows") or []:
        lines.append(f"- {row}")
    lines.extend(["", "## Recommendation"])
    recommendation = payload.get("recommendation") or {}
    lines.append(f"- recommended_detector_profile: {recommendation.get('recommended_detector_profile')}")
    lines.append(f"- recommended_trade_quality_profile: {recommendation.get('recommended_trade_quality_profile')}")
    lines.append(f"- verdict: {recommendation.get('verdict')}")
    lines.append(f"- reason: {recommendation.get('reason')}")
    return "\n".join(lines) + "\n"


def _render_funnel_markdown(payload: dict[str, Any]) -> str:
    funnel = payload.get("funnel_audit") or {}
    lines = [
        "# Asia Drift Compression Funnel Audit",
        "",
        "Detector-stage funnel only. Candidate detections here are not validated trade setups.",
        "",
        "## Funnel",
    ]
    for key, value in funnel.items():
        if key == "taxonomy_target_stage_failures":
            continue
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Taxonomy Target Failures"])
    for key, value in sorted((funnel.get("taxonomy_target_stage_failures") or {}).items()):
        lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def _render_candidate_maturation_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("candidate_maturation_summary") or {}
    lines = [
        "# Asia Drift Compression Candidate Maturation Summary",
        "",
        "Detector-stage candidate lifecycle only. These are detected phenomena, not validated trade setups.",
        "",
        "## Counts",
        f"- candidate_count: {summary.get('candidate_count')}",
        f"- clean_unresolved_count: {summary.get('clean_unresolved_count')}",
        f"- confirmed_rejection_count: {summary.get('confirmed_rejection_count')}",
        f"- contaminated_candidate_count: {summary.get('contaminated_candidate_count')}",
        f"- session_timeout_unresolved_count: {summary.get('session_timeout_unresolved_count')}",
        "",
        "## Family Comparison",
    ]
    for family, row in sorted((summary.get("family_comparison") or {}).items()):
        lines.append(
            f"- {family}: candidate_count={row.get('candidate_count')} "
            f"confirmations={row.get('continuation_confirmed_count')} "
            f"clean_unresolved={row.get('clean_unresolved_count')} "
            f"contaminated={row.get('contaminated_candidate_count')} "
            f"confirmed_rejection={row.get('confirmed_rejection_count')} "
            f"dominant_failures={row.get('dominant_maturation_failures')}"
        )
    lines.extend(["", "## Metals vs ES/MES"])
    for key, value in sorted((summary.get("metals_vs_es_mes") or {}).items()):
        lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def _group_by(rows: Sequence[Any], *, key) -> dict[Any, list[Any]]:
    grouped: dict[Any, list[Any]] = {}
    for row in rows:
        grouped.setdefault(key(row), []).append(row)
    return grouped


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _median(values: Sequence[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return float(median(clean))


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))
