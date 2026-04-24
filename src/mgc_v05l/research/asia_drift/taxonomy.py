"""Asia Drift opportunity taxonomy over broader replay samples."""

from __future__ import annotations

import csv
import json
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
from .entries import ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED, PREFILL_PROFILE_RECOVERY_CONFIRMED
from .features import (
    ASIA_DRIFT_LONG,
    ASIA_DRIFT_SHORT,
    DISQUALIFYING_PULLBACK,
    FAST_DEEP_DISQUALIFYING,
    FAST_SHALLOW_VALID,
    FAST_STRETCHED_WARNING,
    NO_PULLBACK,
    NORMAL_PULLBACK,
    RECOVERY_CONFIRMED,
    STRETCHED_BUT_VALID,
)
from .session_scope import ASIA_DRIFT_BUILD, ASIA_DRIFT_MATURE, ASIA_DRIFT_PRE_HANDOFF


OPP_NO_DRIFT_CHOP = "NO_DRIFT_CHOP"
OPP_CLEAN_DIRECTIONAL_DRIFT = "CLEAN_DIRECTIONAL_DRIFT"
OPP_IMPULSE_THEN_PAUSE = "IMPULSE_THEN_PAUSE"
OPP_COMPRESSION_THEN_CONTINUATION = "COMPRESSION_THEN_CONTINUATION"
OPP_VWAP_REJECTION_CONTINUATION = "VWAP_REJECTION_RECLAIM_CONTINUATION"
OPP_FAILED_MEAN_REVERSION = "FAILED_MEAN_REVERSION"
OPP_POST_SPIKE_INSTABILITY = "POST_SPIKE_INSTABILITY"
OPP_LATE_SESSION_DRIFT = "LATE_SESSION_DRIFT_INTO_LONDON"
OPP_OTHER_DIRECTIONAL = "OTHER_DIRECTIONAL_PATTERN"


BEST_POINT_DRIFT_EXTREME = "DRIFT_EXTREME_RESET"
BEST_POINT_VWAP = "VWAP_REJECTION"
BEST_POINT_EMA = "EMA_RESET"
BEST_POINT_COMPRESSION = "COMPRESSION_BREAK"
BEST_POINT_TIME = "TIME_BASED_CONTINUATION"
BEST_POINT_MEAN_REV_FAIL = "FAILED_MEAN_REVERSION_FLIP"
BEST_POINT_NONE = "NO_CLEAR_POINT"

BRANCH_FAST_SHALLOW_REFERENCE = "FAST_SHALLOW_REFERENCE"
BRANCH_COMPRESSION = OPP_COMPRESSION_THEN_CONTINUATION
BRANCH_VWAP = OPP_VWAP_REJECTION_CONTINUATION

INSTRUMENT_FAMILY = {
    "GC": "metals",
    "MGC": "metals",
    "ES": "indices",
    "MES": "indices",
    "NQ": "indices",
    "MNQ": "indices",
}


@dataclass(frozen=True)
class _SessionTaxonomy:
    row: dict[str, Any]
    missed_pattern_row: dict[str, Any] | None


def classify_session_taxonomy_for_run(
    *,
    instrument: str,
    window: AsiaDriftReplayWindow,
    run: AsiaDriftPhase2Run,
) -> list[dict[str, Any]]:
    return [classified.row for classified in _classify_sessions(instrument=instrument, window=window, run=run)]


def run_asia_drift_opportunity_taxonomy(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    primary_instrument: str = "MGC",
    windows: Sequence[AsiaDriftReplayWindow] = DEFAULT_WIDER_REPLAY_WINDOWS,
    reference_instruments: Sequence[str] = (),
) -> dict[str, Any]:
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]]] = {}
    for instrument in (primary_instrument, *tuple(reference_instruments)):
        window_runs: list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]] = []
        for window in windows:
            run = run_asia_drift_phase2(
                source_sqlite_path=source_sqlite_path,
                output_dir=output_dir / instrument.lower() / window.label,
                instruments=(instrument,),
                start_ts=window.start_ts,
                end_ts=window.end_ts,
                calibration_profile_name=RECOVERY_CONFIRMED,
                refined_prefill_profile_name=PREFILL_PROFILE_RECOVERY_CONFIRMED,
            )
            window_runs.append((window, run))
        instrument_runs[instrument] = window_runs
    payload = _build_taxonomy_payload(
        instrument_runs=instrument_runs,
        source_label=str(source_sqlite_path.resolve()),
        primary_instrument=primary_instrument,
        window_labels=[window.label for window in windows],
    )
    artifacts = _write_taxonomy_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def run_asia_drift_opportunity_taxonomy_from_bars(
    *,
    output_dir: Path,
    primary_instrument: str,
    windows: Sequence[AsiaDriftReplayBarWindow],
) -> dict[str, Any]:
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]]] = {primary_instrument: []}
    for window in windows:
        run = run_asia_drift_phase2_from_bars(
            output_dir=output_dir / primary_instrument.lower() / window.label,
            bars_5m=window.bars_5m,
            bars_1m=window.bars_1m,
            source_label=window.label,
            calibration_profile_name=RECOVERY_CONFIRMED,
            refined_prefill_profile_name=PREFILL_PROFILE_RECOVERY_CONFIRMED,
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
    payload = _build_taxonomy_payload(
        instrument_runs=instrument_runs,
        source_label="synthetic",
        primary_instrument=primary_instrument,
        window_labels=[window.label for window in windows],
    )
    artifacts = _write_taxonomy_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def _build_taxonomy_payload(
    *,
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]]],
    source_label: str,
    primary_instrument: str,
    window_labels: Sequence[str],
) -> dict[str, Any]:
    session_rows: list[dict[str, Any]] = []
    missed_pattern_rows: list[dict[str, Any]] = []
    window_rows: list[dict[str, Any]] = []

    for instrument, window_runs in sorted(instrument_runs.items()):
        for window, run in window_runs:
            window_session_rows: list[dict[str, Any]] = []
            window_missed_rows: list[dict[str, Any]] = []
            for classified in _classify_sessions(instrument=instrument, window=window, run=run):
                session_rows.append(classified.row)
                window_session_rows.append(classified.row)
                if classified.missed_pattern_row is not None:
                    missed_pattern_rows.append(classified.missed_pattern_row)
                    window_missed_rows.append(classified.missed_pattern_row)
            window_rows.append(_window_taxonomy_summary(instrument=instrument, window=window, session_rows=window_session_rows, missed_rows=window_missed_rows))

    aggregate_summary = {
        instrument: _aggregate_taxonomy_rows(
            session_rows=[row for row in session_rows if row["instrument"] == instrument],
            missed_rows=[row for row in missed_pattern_rows if row["instrument"] == instrument],
        )
        for instrument in sorted(instrument_runs)
    }
    family_summary = {
        family: _aggregate_taxonomy_rows(
            session_rows=[row for row in session_rows if row["instrument_family"] == family],
            missed_rows=[row for row in missed_pattern_rows if row["instrument_family"] == family],
        )
        for family in sorted({row["instrument_family"] for row in session_rows})
    }
    branch_transfer_summary = _branch_transfer_summary(
        session_rows=session_rows,
        aggregate_summary=aggregate_summary,
        family_summary=family_summary,
    )
    detector_transfer_audit = _detector_transfer_audit(
        session_rows=session_rows,
        family_summary=family_summary,
    )
    index_behavior_summary = _index_behavior_summary(session_rows=session_rows)
    recommendation = _recommendation(
        primary_summary=aggregate_summary.get(primary_instrument, {}),
        primary_session_rows=[row for row in session_rows if row["instrument"] == primary_instrument],
        family_summary=family_summary,
        branch_transfer_summary=branch_transfer_summary,
    )
    return {
        "module": "Asia Drift v1 Opportunity Taxonomy",
        "objective": (
            "Session-level opportunity taxonomy over a wider Asia Drift replay sample to determine whether Asia "
            "drift itself is rare or whether the current fast-shallow participation implementation is too narrow."
        ),
        "primary_instrument": primary_instrument,
        "primary_instrument_family": _instrument_family(primary_instrument),
        "calibration_profile": RECOVERY_CONFIRMED,
        "primary_entry_model": ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
        "source_label": source_label,
        "window_labels": list(window_labels),
        "window_summaries": window_rows,
        "session_taxonomy_manifest": session_rows,
        "missed_pattern_audit": missed_pattern_rows,
        "aggregate_summary": aggregate_summary,
        "family_summary": family_summary,
        "branch_transfer_summary": branch_transfer_summary,
        "detector_transfer_audit": detector_transfer_audit,
        "index_behavior_summary": index_behavior_summary,
        "recommendation": recommendation,
    }


def _classify_sessions(
    *,
    instrument: str,
    window: AsiaDriftReplayWindow,
    run: AsiaDriftPhase2Run,
) -> list[_SessionTaxonomy]:
    features_by_session = _group_by(run.phase1_run.feature_rows, key=lambda row: row.asia_drift_session_id)
    states_by_session = _group_by(run.phase1_run.state_rows, key=lambda row: row.asia_drift_session_id)
    evals_by_session = _group_by(run.entry_evaluations, key=lambda row: row.asia_drift_session_id)
    trades_by_session = _group_by(run.trade_records, key=lambda row: row.asia_drift_session_id)
    summaries_by_session = {row.asia_drift_session_id: row for row in run.phase1_run.session_summaries}

    classified_rows: list[_SessionTaxonomy] = []
    for session_id, summary in sorted(summaries_by_session.items()):
        feature_rows = sorted(features_by_session.get(session_id, []), key=lambda row: row.decision_ts)
        state_rows = sorted(states_by_session.get(session_id, []), key=lambda row: row.decision_ts)
        entry_rows = sorted(evals_by_session.get(session_id, []), key=lambda row: (row.armed_ts, row.entry_model))
        trade_rows = sorted(trades_by_session.get(session_id, []), key=lambda row: row.entry_ts)
        classified_rows.append(
            _classify_single_session(
                instrument=instrument,
                window=window,
                summary=summary,
                feature_rows=feature_rows,
                state_rows=state_rows,
                entry_rows=entry_rows,
                trade_rows=trade_rows,
            )
        )
    return classified_rows


def _classify_single_session(
    *,
    instrument: str,
    window: AsiaDriftReplayWindow,
    summary: Any,
    feature_rows: Sequence[Any],
    state_rows: Sequence[Any],
    entry_rows: Sequence[Any],
    trade_rows: Sequence[Any],
) -> _SessionTaxonomy:
    in_scope_rows = [row for row in feature_rows if row.in_scope]
    rows = in_scope_rows or list(feature_rows)
    first = rows[0]
    direction = summary.candidate_direction if summary.candidate_direction in {"LONG", "SHORT"} else first.dominant_direction
    candidate_index = next((index for index, row in enumerate(rows) if row.regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}), None)
    candidate_row = rows[candidate_index] if candidate_index is not None else None
    base_price = candidate_row.close if candidate_row is not None else first.close
    atr_base = max(candidate_row.atr if candidate_row is not None else first.atr, 1e-9)

    continuation_begin_index: int | None = None
    max_favorable_atr = 0.0
    max_adverse_atr = 0.0
    for index, row in enumerate(rows[candidate_index or 0 :], start=candidate_index or 0):
        favorable = ((row.high - base_price) / atr_base) if direction == "LONG" else ((base_price - row.low) / atr_base)
        adverse = ((base_price - row.low) / atr_base) if direction == "LONG" else ((row.high - base_price) / atr_base)
        if favorable > max_favorable_atr:
            max_favorable_atr = favorable
        if adverse > max_adverse_atr:
            max_adverse_atr = adverse
        if continuation_begin_index is None and favorable >= 0.75:
            continuation_begin_index = index
    continuation_present = candidate_row is not None and max_favorable_atr >= 0.90
    continuation_row = rows[continuation_begin_index] if continuation_begin_index is not None else None

    pre_continuation_rows = rows[(candidate_index or 0) : (continuation_begin_index + 1 if continuation_begin_index is not None else len(rows))]
    meaningful_pullback_rows = [row for row in pre_continuation_rows if row.pullback_state != NO_PULLBACK or row.fast_pullback_class != "SLOW_OR_STANDARD"]
    fast_shallow_valid = any(row.fast_pullback_class == FAST_SHALLOW_VALID for row in pre_continuation_rows)
    fast_stretched_warning = any(row.fast_pullback_class == FAST_STRETCHED_WARNING for row in pre_continuation_rows)
    fast_deep_disqualifying = any(row.fast_pullback_class == FAST_DEEP_DISQUALIFYING for row in pre_continuation_rows)
    vwap_interaction = any(row.pullback_vwap_interaction in {"VWAP_TAG", "SINGLE_CLOSE_THROUGH_VWAP"} for row in pre_continuation_rows)
    compression_then_expansion = any(row.compression_followed_by_drift_expansion for row in pre_continuation_rows)
    countertrend_failed = any(row.countertrend_extension_failed for row in pre_continuation_rows)
    post_spike_fraction = _ratio(sum(1 for row in rows if row.post_spike_instability), len(rows))
    chop_fraction = _ratio(sum(1 for row in rows if row.chop_veto), len(rows))
    directional_bar_fraction = _ratio(sum(1 for row in rows if row.regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}), len(rows))
    median_efficiency = _median([row.efficiency_ratio_12 for row in rows])
    median_rv_ratio = _median([row.realized_volatility_ratio for row in rows])
    median_reversal_frequency = _median([row.reversal_frequency_12 for row in rows])
    median_directional_persistence = _median([row.directional_persistence_8 for row in rows])
    peak_signed_vwap_displacement = _peak_signed_vwap_displacement(rows=rows, direction=direction)
    peak_slope_persistence = _peak_slope_persistence(rows=rows, direction=direction)
    peak_close_location_persistence = _peak_close_location_persistence(rows=rows, direction=direction)

    if not meaningful_pullback_rows:
        pullback_descriptor = "ABSENT_OR_NOT_REQUIRED"
    elif fast_shallow_valid:
        pullback_descriptor = "FAST_SHALLOW"
    elif fast_stretched_warning:
        pullback_descriptor = "FAST_STRETCHED"
    elif fast_deep_disqualifying or any(row.pullback_state == DISQUALIFYING_PULLBACK for row in pre_continuation_rows):
        pullback_descriptor = "DEEP_OR_DISQUALIFYING"
    elif any(row.pullback_state == STRETCHED_BUT_VALID for row in pre_continuation_rows):
        pullback_descriptor = "STRETCHED"
    elif any(row.pullback_state == NORMAL_PULLBACK for row in pre_continuation_rows):
        pullback_descriptor = "NORMAL_PULLBACK"
    else:
        pullback_descriptor = "SLOW_OR_STANDARD"
    fast_shallow_reference_session = bool(fast_shallow_valid)
    slow_grind_without_pullback = bool(
        continuation_present
        and pullback_descriptor == "ABSENT_OR_NOT_REQUIRED"
        and (median_directional_persistence or 0.0) >= 0.45
        and chop_fraction < 0.25
    )
    late_continuation_session = bool(continuation_present and continuation_row is not None and continuation_row.subphase == ASIA_DRIFT_PRE_HANDOFF)

    if continuation_present and continuation_row is not None and continuation_row.subphase == ASIA_DRIFT_PRE_HANDOFF:
        opportunity_type = OPP_LATE_SESSION_DRIFT
    elif post_spike_fraction >= 0.15 and (not continuation_present or max_adverse_atr >= max_favorable_atr * 0.75):
        opportunity_type = OPP_POST_SPIKE_INSTABILITY
    elif not continuation_present and (chop_fraction >= 0.25 or directional_bar_fraction < 0.30):
        opportunity_type = OPP_NO_DRIFT_CHOP
    elif continuation_present and vwap_interaction:
        opportunity_type = OPP_VWAP_REJECTION_CONTINUATION
    elif continuation_present and compression_then_expansion:
        opportunity_type = OPP_COMPRESSION_THEN_CONTINUATION
    elif continuation_present and countertrend_failed:
        opportunity_type = OPP_FAILED_MEAN_REVERSION
    elif continuation_present and _impulse_then_pause(rows=pre_continuation_rows, candidate_index=candidate_index or 0, continuation_begin_index=continuation_begin_index):
        opportunity_type = OPP_IMPULSE_THEN_PAUSE
    elif continuation_present and chop_fraction < 0.20 and post_spike_fraction < 0.10 and pullback_descriptor in {"ABSENT_OR_NOT_REQUIRED", "FAST_SHALLOW", "NORMAL_PULLBACK"}:
        opportunity_type = OPP_CLEAN_DIRECTIONAL_DRIFT
    elif continuation_present:
        opportunity_type = OPP_OTHER_DIRECTIONAL
    else:
        opportunity_type = OPP_NO_DRIFT_CHOP

    if continuation_present and compression_then_expansion:
        best_point = BEST_POINT_COMPRESSION
    elif continuation_present and vwap_interaction:
        best_point = BEST_POINT_VWAP
    elif continuation_present and countertrend_failed:
        best_point = BEST_POINT_MEAN_REV_FAIL
    elif continuation_present and continuation_row is not None and abs(continuation_row.close - continuation_row.slow_ema) / max(continuation_row.atr, 1e-9) <= 0.20:
        best_point = BEST_POINT_EMA
    elif continuation_present and pullback_descriptor in {"FAST_SHALLOW", "NORMAL_PULLBACK", "STRETCHED"}:
        best_point = BEST_POINT_DRIFT_EXTREME
    elif continuation_present and pullback_descriptor == "ABSENT_OR_NOT_REQUIRED":
        best_point = BEST_POINT_TIME
    else:
        best_point = BEST_POINT_NONE

    continuation_quality = (
        "STRONG" if max_favorable_atr >= 1.50 else "MEDIUM" if max_favorable_atr >= 1.00 else "WEAK" if max_favorable_atr >= 0.90 else "NONE"
    )
    current_v1_detected = bool(entry_rows)
    current_v1_accepted = any(row.accepted for row in entry_rows)
    blocked_by, blocked_detail, excluded_pre_recognition = _block_diagnosis(summary=summary, entry_rows=entry_rows)
    row = {
        "instrument": instrument,
        "instrument_family": _instrument_family(instrument),
        "window_label": window.label,
        "asia_drift_session_id": summary.asia_drift_session_id,
        "local_session_date": summary.local_session_date.isoformat(),
        "opportunity_type": opportunity_type,
        "candidate_direction": direction,
        "directional_continuation": continuation_present,
        "continuation_quality": continuation_quality,
        "max_favorable_excursion_atr": max_favorable_atr,
        "max_adverse_excursion_atr": max_adverse_atr,
        "continuation_begin_ts": continuation_row.decision_ts.isoformat() if continuation_row is not None else None,
        "continuation_begin_subphase": continuation_row.subphase if continuation_row is not None else None,
        "continuation_begin_time_label": continuation_row.session_time_label if continuation_row is not None else None,
        "continuation_requires_pullback": bool(meaningful_pullback_rows),
        "pullback_descriptor": pullback_descriptor,
        "fast_shallow_reference_session": fast_shallow_reference_session,
        "vwap_involved": vwap_interaction,
        "best_participation_point_type": best_point,
        "compression_then_expansion_session": compression_then_expansion,
        "failed_mean_reversion_session": countertrend_failed,
        "slow_grind_without_pullback_session": slow_grind_without_pullback,
        "late_continuation_session": late_continuation_session,
        "current_v1_detected": current_v1_detected,
        "current_v1_accepted_entry": current_v1_accepted,
        "current_v1_excluded_pre_recognition": excluded_pre_recognition,
        "blocked_by_current_v1": blocked_by,
        "blocking_detail": blocked_detail,
        "alternative_opportunity_type": opportunity_type,
        "realized_volatility_bucket": _realized_volatility_bucket(median_rv_ratio),
        "directional_efficiency_bucket": _directional_efficiency_bucket(median_efficiency),
        "range_expansion_bucket": _range_expansion_bucket(_median([row.range_points / max(row.atr, 1e-9) for row in rows])),
        "session_texture_tag": _market_texture_tag(
            chop_fraction=chop_fraction,
            directional_bar_fraction=directional_bar_fraction,
            median_efficiency=median_efficiency,
        ),
        "post_spike_context_tag": "post_spike_present" if post_spike_fraction >= 0.10 else "clean",
        "time_of_night_bucket": _time_of_night_bucket(continuation_row.subphase if continuation_row is not None else first.subphase),
        "peak_signed_vwap_displacement": peak_signed_vwap_displacement,
        "median_directional_efficiency": median_efficiency,
        "peak_slope_persistence": peak_slope_persistence,
        "peak_close_location_persistence": peak_close_location_persistence,
        "median_reversal_frequency": median_reversal_frequency,
        "median_directional_persistence": median_directional_persistence,
        "entry_evaluation_count": len(entry_rows),
        "accepted_entry_count": sum(1 for row in entry_rows if row.accepted),
        "trade_count": len(trade_rows),
    }

    missed_pattern_row = None
    if continuation_present and not current_v1_accepted:
        dominant_reason = _dominant_cancellation_reason(entry_rows)
        missed_pattern_row = {
            "instrument": instrument,
            "instrument_family": row["instrument_family"],
            "window_label": window.label,
            "asia_drift_session_id": summary.asia_drift_session_id,
            "opportunity_type": opportunity_type,
            "candidate_direction": direction,
            "continuation_quality": continuation_quality,
            "max_favorable_excursion_atr": max_favorable_atr,
            "pullback_descriptor": pullback_descriptor,
            "best_participation_point_type": best_point,
            "fast_shallow_reference_session": fast_shallow_reference_session,
            "blocked_by_current_v1": blocked_by,
            "blocking_detail": blocked_detail,
            "dominant_cancellation_reason": dominant_reason,
            "current_filters_excluded_before_recognition": excluded_pre_recognition,
            "post_spike_context_tag": row["post_spike_context_tag"],
            "session_texture_tag": row["session_texture_tag"],
            "alternative_opportunity_type": opportunity_type,
        }
    return _SessionTaxonomy(row=row, missed_pattern_row=missed_pattern_row)


def _window_taxonomy_summary(
    *,
    instrument: str,
    window: AsiaDriftReplayWindow,
    session_rows: Sequence[dict[str, Any]],
    missed_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    continuation_rows = [row for row in session_rows if row["directional_continuation"]]
    accepted_rows = [row for row in session_rows if row["current_v1_accepted_entry"]]
    return {
        "instrument": instrument,
        "instrument_family": _instrument_family(instrument),
        "window_label": window.label,
        "start_ts": window.start_ts.isoformat(),
        "end_ts": window.end_ts.isoformat(),
        "asia_session_count": len(session_rows),
        "continuation_session_count": len(continuation_rows),
        "accepted_entry_session_count": len(accepted_rows),
        "taxonomy_counts": dict(Counter(row["opportunity_type"] for row in session_rows)),
        "branch_reference_counts": {
            BRANCH_COMPRESSION: sum(1 for row in continuation_rows if row["opportunity_type"] == OPP_COMPRESSION_THEN_CONTINUATION),
            BRANCH_VWAP: sum(1 for row in continuation_rows if row["opportunity_type"] == OPP_VWAP_REJECTION_CONTINUATION),
            BRANCH_FAST_SHALLOW_REFERENCE: sum(1 for row in continuation_rows if row["fast_shallow_reference_session"]),
        },
        "missed_pattern_count": len(missed_rows),
        "top_participation_point_types": dict(Counter(row["best_participation_point_type"] for row in continuation_rows).most_common(3)),
    }


def _aggregate_taxonomy_rows(
    *,
    session_rows: Sequence[dict[str, Any]],
    missed_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    continuation_rows = [row for row in session_rows if row["directional_continuation"]]
    counts_by_type: dict[str, dict[str, Any]] = {}
    for opportunity_type in sorted({row["opportunity_type"] for row in session_rows}):
        typed_rows = [row for row in session_rows if row["opportunity_type"] == opportunity_type]
        continuation_typed = [row for row in typed_rows if row["directional_continuation"]]
        counts_by_type[opportunity_type] = {
            "session_count": len(typed_rows),
            "continuation_session_count": len(continuation_typed),
            "current_v1_detected_count": sum(1 for row in typed_rows if row["current_v1_detected"]),
            "current_v1_accepted_count": sum(1 for row in typed_rows if row["current_v1_accepted_entry"]),
            "current_v1_missed_count": sum(1 for row in continuation_typed if not row["current_v1_accepted_entry"]),
            "median_continuation_atr": _median([row["max_favorable_excursion_atr"] for row in continuation_typed]),
            "top_participation_point_types": dict(Counter(row["best_participation_point_type"] for row in continuation_typed).most_common(3)),
        }

    candidate_types = [
        {
            "opportunity_type": opportunity_type,
            "continuation_session_count": stats["continuation_session_count"],
            "current_v1_accepted_count": stats["current_v1_accepted_count"],
            "current_v1_missed_count": stats["current_v1_missed_count"],
            "median_continuation_atr": stats["median_continuation_atr"],
        }
        for opportunity_type, stats in counts_by_type.items()
        if stats["continuation_session_count"] > 0 and stats["current_v1_missed_count"] >= stats["current_v1_accepted_count"]
    ]
    candidate_types.sort(
        key=lambda row: (
            -int(row["continuation_session_count"]),
            -int(row["current_v1_missed_count"]),
            -(float(row["median_continuation_atr"]) if row["median_continuation_atr"] is not None else 0.0),
        )
    )
    deprioritized_types = [
        {
            "opportunity_type": opportunity_type,
            "reason": "little_or_no_continuation"
            if stats["continuation_session_count"] == 0
            else "continuation_present_but_post_spike_or_unstable",
        }
        for opportunity_type, stats in counts_by_type.items()
        if stats["continuation_session_count"] == 0 or opportunity_type == OPP_POST_SPIKE_INSTABILITY
    ]
    branch_counts = {
        BRANCH_COMPRESSION: sum(1 for row in continuation_rows if row["opportunity_type"] == OPP_COMPRESSION_THEN_CONTINUATION),
        BRANCH_VWAP: sum(1 for row in continuation_rows if row["opportunity_type"] == OPP_VWAP_REJECTION_CONTINUATION),
        BRANCH_FAST_SHALLOW_REFERENCE: sum(1 for row in continuation_rows if row["fast_shallow_reference_session"]),
    }
    branch_quality = {
        BRANCH_COMPRESSION: _branch_quality(continuation_rows, predicate=lambda row: row["opportunity_type"] == OPP_COMPRESSION_THEN_CONTINUATION),
        BRANCH_VWAP: _branch_quality(continuation_rows, predicate=lambda row: row["opportunity_type"] == OPP_VWAP_REJECTION_CONTINUATION),
        BRANCH_FAST_SHALLOW_REFERENCE: _branch_quality(continuation_rows, predicate=lambda row: row["fast_shallow_reference_session"]),
    }
    return {
        "session_count": len(session_rows),
        "continuation_session_count": len(continuation_rows),
        "accepted_entry_session_count": sum(1 for row in session_rows if row["current_v1_accepted_entry"]),
        "taxonomy_counts": {key: stats["session_count"] for key, stats in counts_by_type.items()},
        "continuation_counts_by_type": {key: stats["continuation_session_count"] for key, stats in counts_by_type.items()},
        "current_v1_detection_by_type": {key: stats["current_v1_detected_count"] for key, stats in counts_by_type.items()},
        "current_v1_acceptance_by_type": {key: stats["current_v1_accepted_count"] for key, stats in counts_by_type.items()},
        "current_v1_miss_by_type": {key: stats["current_v1_missed_count"] for key, stats in counts_by_type.items()},
        "continuation_quality_by_type": {key: stats["median_continuation_atr"] for key, stats in counts_by_type.items()},
        "counts_by_type_detail": counts_by_type,
        "missed_pattern_count": len(missed_rows),
        "missed_pattern_block_counts": dict(Counter(row["blocked_by_current_v1"] for row in missed_rows)),
        "missed_pattern_detail_counts": dict(Counter(row["blocking_detail"] for row in missed_rows)),
        "candidate_opportunity_types_worth_research": candidate_types[:5],
        "types_to_reject_or_deprioritize": deprioritized_types,
        "branch_reference_counts": branch_counts,
        "branch_quality": branch_quality,
        "context_breakdown": {
            "post_spike_context": _context_breakdown(session_rows, key="post_spike_context_tag"),
            "realized_volatility_bucket": _context_breakdown(session_rows, key="realized_volatility_bucket"),
            "directional_efficiency_bucket": _context_breakdown(session_rows, key="directional_efficiency_bucket"),
            "range_expansion_bucket": _context_breakdown(session_rows, key="range_expansion_bucket"),
            "time_of_night_bucket": _context_breakdown(session_rows, key="time_of_night_bucket"),
        },
    }


def _recommendation(
    *,
    primary_summary: dict[str, Any],
    primary_session_rows: Sequence[dict[str, Any]],
    family_summary: dict[str, Any],
    branch_transfer_summary: dict[str, Any],
) -> dict[str, Any]:
    continuation_sessions = int(primary_summary.get("continuation_session_count", 0) or 0)
    accepted_sessions = int(primary_summary.get("accepted_entry_session_count", 0) or 0)
    candidate_types = primary_summary.get("candidate_opportunity_types_worth_research") or []
    top_candidate = candidate_types[0] if candidate_types else None
    fast_shallow_sessions = sum(
        1
        for row in primary_session_rows
        if row.get("directional_continuation") and row.get("best_participation_point_type") == BEST_POINT_DRIFT_EXTREME
    )
    fast_shallow_accepted = sum(
        1
        for row in primary_session_rows
        if row.get("current_v1_accepted_entry") and row.get("best_participation_point_type") == BEST_POINT_DRIFT_EXTREME
    )
    metals_summary = family_summary.get("metals", {})
    indices_summary = family_summary.get("indices", {})
    metals_candidates = metals_summary.get("candidate_opportunity_types_worth_research") or []
    indices_candidates = indices_summary.get("candidate_opportunity_types_worth_research") or []
    metals_top = metals_candidates[0]["opportunity_type"] if metals_candidates else None
    indices_top = indices_candidates[0]["opportunity_type"] if indices_candidates else None
    shared_transfer = (
        branch_transfer_summary.get("branches", {})
        .get(BRANCH_COMPRESSION, {})
        .get("transfer_status")
        in {"shared_across_families", "shared_but_family_weighted"}
        or branch_transfer_summary.get("branches", {})
        .get(BRANCH_VWAP, {})
        .get("transfer_status")
        in {"shared_across_families", "shared_but_family_weighted"}
    )
    indices_continuations = int(indices_summary.get("continuation_session_count", 0) or 0)
    if continuation_sessions == 0:
        return {
            "recommendation": "pause_asia_drift_entirely",
            "reason": "The wider taxonomy sample did not show repeated directional continuation sessions worth pursuing.",
            "next_best_research_hypothesis": None,
        }
    if indices_continuations < 3:
        return {
            "recommendation": "pause_index_expansion_due_to_insufficient_evidence",
            "reason": "The metals reference has continuation sessions, but the index family sample is still too thin to support expansion.",
            "next_best_research_hypothesis": metals_top or (top_candidate["opportunity_type"] if top_candidate else None),
        }
    if shared_transfer:
        return {
            "recommendation": "shared_cross_asset_detector_with_family_specific_thresholds",
            "reason": "Compression/VWAP continuation appears in both metals and indices, but family-aware thresholds are still warranted.",
            "next_best_research_hypothesis": metals_top or indices_top,
        }
    if metals_top and indices_top and metals_top != indices_top:
        return {
            "recommendation": "separate_index_overnight_drift_branch",
            "reason": f"Metals and indices show different primary continuation presentations ({metals_top} vs {indices_top}).",
            "next_best_research_hypothesis": metals_top,
        }
    if metals_top == OPP_COMPRESSION_THEN_CONTINUATION:
        return {
            "recommendation": "metals_only_compression_then_continuation",
            "reason": "Metals continue to center on compression-led continuation, while fast-shallow stays a minor branch.",
            "next_best_research_hypothesis": metals_top,
        }
    if metals_top == OPP_VWAP_REJECTION_CONTINUATION:
        return {
            "recommendation": "metals_only_vwap_rejection_reclaim_continuation",
            "reason": "Metals continuation is more often organized around VWAP rejection/reclaim than the fast-shallow lane.",
            "next_best_research_hypothesis": metals_top,
        }
    if top_candidate is None:
        return {
            "recommendation": "keep_fast_shallow_as_minor_branch_only",
            "reason": "Directional continuation exists, but no alternative repeating opportunity type stood out clearly.",
            "next_best_research_hypothesis": None,
        }
    if top_candidate["opportunity_type"] != OPP_CLEAN_DIRECTIONAL_DRIFT:
        return {
            "recommendation": "proceed_with_new_primary_hypothesis",
            "reason": (
                f"The most repeated missed continuation type was {top_candidate['opportunity_type']}, which suggests "
                "the current fast-shallow lane is not the main presentation."
            ),
            "next_best_research_hypothesis": top_candidate["opportunity_type"],
        }
    if fast_shallow_sessions > 0 and accepted_sessions <= fast_shallow_accepted:
        return {
            "recommendation": "keep_fast_shallow_as_minor_branch_only",
            "reason": "Fast-shallow participation exists, but it is too narrow to remain the primary Asia Drift hypothesis.",
            "next_best_research_hypothesis": top_candidate["opportunity_type"],
        }
    return {
        "recommendation": "pivot_to_different_asia_drift_presentation",
        "reason": "Asia drift appears in the sample, but the current fast-shallow implementation is too narrow or too context-dependent.",
        "next_best_research_hypothesis": top_candidate["opportunity_type"],
    }


def _branch_transfer_summary(
    *,
    session_rows: Sequence[dict[str, Any]],
    aggregate_summary: dict[str, Any],
    family_summary: dict[str, Any],
) -> dict[str, Any]:
    metals = family_summary.get("metals", {})
    indices = family_summary.get("indices", {})
    branches = {}
    for branch in (BRANCH_COMPRESSION, BRANCH_VWAP, BRANCH_FAST_SHALLOW_REFERENCE):
        metals_count = int((metals.get("branch_reference_counts") or {}).get(branch, 0) or 0)
        indices_count = int((indices.get("branch_reference_counts") or {}).get(branch, 0) or 0)
        if metals_count > 0 and indices_count > 0:
            transfer_status = "shared_across_families" if min(metals_count, indices_count) >= 2 else "shared_but_family_weighted"
        elif metals_count > 0:
            transfer_status = "metals_only"
        elif indices_count > 0:
            transfer_status = "indices_only"
        else:
            transfer_status = "not_observed"
        branches[branch] = {
            "transfer_status": transfer_status,
            "metals_count": metals_count,
            "indices_count": indices_count,
            "metals_quality": (metals.get("branch_quality") or {}).get(branch),
            "indices_quality": (indices.get("branch_quality") or {}).get(branch),
        }

    per_instrument = {
        instrument: {
            "family": _instrument_family(instrument),
            "branch_reference_counts": summary.get("branch_reference_counts"),
            "branch_quality": summary.get("branch_quality"),
        }
        for instrument, summary in sorted(aggregate_summary.items())
    }
    return {
        "branches": branches,
        "per_instrument": per_instrument,
    }


def _detector_transfer_audit(
    *,
    session_rows: Sequence[dict[str, Any]],
    family_summary: dict[str, Any],
) -> dict[str, Any]:
    continuation_rows = [row for row in session_rows if row["directional_continuation"]]
    families = sorted({row["instrument_family"] for row in session_rows})
    family_rows = {family: [row for row in continuation_rows if row["instrument_family"] == family] for family in families}
    return {
        "by_family": {
            family: {
                "signed_vwap_displacement_median": _median([row["peak_signed_vwap_displacement"] for row in rows]),
                "compression_then_continuation_rate": _ratio(sum(1 for row in rows if row["compression_then_expansion_session"]), len(rows)),
                "vwap_rejection_continuation_rate": _ratio(sum(1 for row in rows if row["vwap_involved"]), len(rows)),
                "directional_efficiency_median": _median([row["median_directional_efficiency"] for row in rows]),
                "slope_persistence_median": _median([row["peak_slope_persistence"] for row in rows]),
                "close_location_persistence_median": _median([row["peak_close_location_persistence"] for row in rows]),
                "reversal_frequency_median": _median([row["median_reversal_frequency"] for row in rows]),
                "failed_mean_reversion_rate": _ratio(sum(1 for row in rows if row["failed_mean_reversion_session"]), len(rows)),
                "late_asia_continuation_rate": _ratio(sum(1 for row in rows if row["late_continuation_session"]), len(rows)),
                "session_count": len(rows),
            }
            for family, rows in family_rows.items()
        },
        "transfer_view": {
            feature: {
                family: stats.get(feature)
                for family, stats in {
                    family: {
                        "signed_vwap_displacement_median": _median([row["peak_signed_vwap_displacement"] for row in rows]),
                        "compression_then_continuation_rate": _ratio(sum(1 for row in rows if row["compression_then_expansion_session"]), len(rows)),
                        "vwap_rejection_continuation_rate": _ratio(sum(1 for row in rows if row["vwap_involved"]), len(rows)),
                        "directional_efficiency_median": _median([row["median_directional_efficiency"] for row in rows]),
                        "slope_persistence_median": _median([row["peak_slope_persistence"] for row in rows]),
                        "close_location_persistence_median": _median([row["peak_close_location_persistence"] for row in rows]),
                        "reversal_frequency_median": _median([row["median_reversal_frequency"] for row in rows]),
                        "failed_mean_reversion_rate": _ratio(sum(1 for row in rows if row["failed_mean_reversion_session"]), len(rows)),
                        "late_asia_continuation_rate": _ratio(sum(1 for row in rows if row["late_continuation_session"]), len(rows)),
                    }
                    for family, rows in family_rows.items()
                }.items()
            }
            for feature in (
                "signed_vwap_displacement_median",
                "compression_then_continuation_rate",
                "vwap_rejection_continuation_rate",
                "directional_efficiency_median",
                "slope_persistence_median",
                "close_location_persistence_median",
                "reversal_frequency_median",
                "failed_mean_reversion_rate",
                "late_asia_continuation_rate",
            )
        },
        "family_session_counts": {family: int(summary.get("continuation_session_count", 0) or 0) for family, summary in family_summary.items()},
    }


def _index_behavior_summary(*, session_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    index_rows = [row for row in session_rows if row["instrument_family"] == "indices" and row["directional_continuation"]]
    by_date: dict[str, list[dict[str, Any]]] = {}
    for row in index_rows:
        by_date.setdefault(row["local_session_date"], []).append(row)
    synchronized_dates = 0
    synchronized_session_count = 0
    for rows in by_date.values():
        directions = {row["candidate_direction"] for row in rows if row["candidate_direction"] in {"LONG", "SHORT"}}
        if len(rows) >= 2 and len(directions) == 1:
            synchronized_dates += 1
            synchronized_session_count += len(rows)
    return {
        "continuation_session_count": len(index_rows),
        "compression_then_breakout_count": sum(1 for row in index_rows if row["opportunity_type"] == OPP_COMPRESSION_THEN_CONTINUATION),
        "vwap_reclaim_breakout_count": sum(1 for row in index_rows if row["opportunity_type"] == OPP_VWAP_REJECTION_CONTINUATION),
        "slow_grind_without_pullback_count": sum(1 for row in index_rows if row["slow_grind_without_pullback_session"]),
        "late_asia_early_london_continuation_count": sum(1 for row in index_rows if row["late_continuation_session"]),
        "failed_mean_reversion_count": sum(1 for row in index_rows if row["failed_mean_reversion_session"]),
        "risk_on_risk_off_synchronized_dates": synchronized_dates,
        "risk_on_risk_off_synchronized_session_count": synchronized_session_count,
    }


def _block_diagnosis(*, summary: Any, entry_rows: Sequence[Any]) -> tuple[str, str, bool]:
    if any(row.accepted for row in entry_rows):
        return "ACCEPTED", "accepted_entry_present", False
    if entry_rows:
        dominant_reason = _dominant_cancellation_reason(entry_rows)
        if dominant_reason in {"regime_lost_before_fill", "regime_loss_warning"}:
            return "REGIME_LOSS", dominant_reason, False
        if dominant_reason in {"chase_cap_exceeded", "reexpanded_without_fill"}:
            return "ENTRY_GEOMETRY", dominant_reason, False
        if dominant_reason in {"pullback_not_shallow_enough", "depth_exceeds_shallow_window", "depth_exceeds_limit", "fast_deep_disqualifying"}:
            return "PULLBACK_CLASSIFIER", dominant_reason, False
        if dominant_reason in {"latest_entry_time_passed", "session_timeout_boundary"}:
            return "TIMING_RULE", dominant_reason, False
        return "ENTRY_MODEL", dominant_reason, False
    if summary.post_spike_bar_count > 0:
        return "POST_SPIKE_VETO", "post_spike_instability", True
    if summary.chop_veto_bar_count > 0:
        return "CHOP_VETO", "chop_veto_active", True
    if summary.invalidated_bar_count > 0:
        return "REGIME_LOSS", "regime_lost_after_candidate", True
    if summary.entry_ready_bar_count <= 0 and summary.candidate_direction in {"LONG", "SHORT"}:
        return "DRIFT_FILTER", "candidate_never_became_entry_ready", True
    return "DRIFT_FILTER", "no_directional_candidate", True


def _dominant_cancellation_reason(entry_rows: Sequence[Any]) -> str | None:
    if not entry_rows:
        return None
    counts = Counter((row.cancellation_reason or "none") for row in entry_rows if not row.accepted)
    if not counts:
        return None
    return max(sorted(counts.items()), key=lambda item: item[1])[0]


def _impulse_then_pause(*, rows: Sequence[Any], candidate_index: int, continuation_begin_index: int | None) -> bool:
    if continuation_begin_index is None or continuation_begin_index <= candidate_index + 2:
        return False
    impulse_rows = list(rows[: min(3, len(rows))])
    if not impulse_rows:
        return False
    early_impulse = any(row.fresh_drift_impulse for row in impulse_rows)
    pause_rows = list(rows[min(3, len(rows)) : max(0, continuation_begin_index - candidate_index + 1)])
    pause_detected = any(row.bar_overlap_ratio_8 >= 0.65 or row.bars_since_last_drift_impulse >= 2 for row in pause_rows)
    return early_impulse and pause_detected


def _instrument_family(instrument: str) -> str:
    return INSTRUMENT_FAMILY.get(instrument.upper(), "other")


def _peak_signed_vwap_displacement(*, rows: Sequence[Any], direction: str) -> float | None:
    if direction == "SHORT":
        values = [row.signed_vwap_displacement_short for row in rows]
    else:
        values = [row.signed_vwap_displacement_long for row in rows]
    return _median(values) if values else None


def _peak_slope_persistence(*, rows: Sequence[Any], direction: str) -> float | None:
    if direction == "SHORT":
        values = [row.slope_combo_short for row in rows]
    else:
        values = [row.slope_combo_long for row in rows]
    return max(values) if values else None


def _peak_close_location_persistence(*, rows: Sequence[Any], direction: str) -> float | None:
    if direction == "SHORT":
        values = [row.close_location_persistence_short for row in rows]
    else:
        values = [row.close_location_persistence_long for row in rows]
    return max(values) if values else None


def _branch_quality(rows: Sequence[dict[str, Any]], *, predicate) -> dict[str, Any]:
    branch_rows = [row for row in rows if predicate(row)]
    return {
        "session_count": len(branch_rows),
        "median_continuation_atr": _median([row["max_favorable_excursion_atr"] for row in branch_rows]),
        "accepted_entry_session_count": sum(1 for row in branch_rows if row["current_v1_accepted_entry"]),
        "post_spike_present_count": sum(1 for row in branch_rows if row["post_spike_context_tag"] == "post_spike_present"),
    }


def _realized_volatility_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 0.85:
        return "quiet"
    if value < 1.25:
        return "normal"
    if value < 1.80:
        return "elevated"
    return "spiky"


def _directional_efficiency_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 0.35:
        return "low"
    if value < 0.50:
        return "medium"
    return "high"


def _range_expansion_bucket(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 0.8:
        return "compressed"
    if value < 1.25:
        return "normal"
    return "expanded"


def _market_texture_tag(*, chop_fraction: float, directional_bar_fraction: float, median_efficiency: float | None) -> str:
    if chop_fraction >= 0.35:
        return "chop_heavy"
    if directional_bar_fraction >= 0.45 and (median_efficiency or 0.0) >= 0.42:
        return "directional"
    return "mixed"


def _time_of_night_bucket(subphase: str | None) -> str:
    if subphase == ASIA_DRIFT_BUILD:
        return "asia_early"
    if subphase == ASIA_DRIFT_MATURE:
        return "asia_mature"
    if subphase == ASIA_DRIFT_PRE_HANDOFF:
        return "asia_late"
    return "unknown"


def _context_breakdown(rows: Sequence[dict[str, Any]], *, key: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(key) or "unknown"), []).append(row)
    return {
        group_key: {
            "session_count": len(group_rows),
            "continuation_session_count": sum(1 for row in group_rows if row.get("directional_continuation")),
            "accepted_entry_session_count": sum(1 for row in group_rows if row.get("current_v1_accepted_entry")),
            "taxonomy_counts": dict(Counter(row["opportunity_type"] for row in group_rows)),
        }
        for group_key, group_rows in sorted(grouped.items())
    }


def _write_taxonomy_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    layout = build_layout(output_dir)
    summary_json_path = layout["reports"] / "asia_drift_opportunity_taxonomy_summary.json"
    summary_markdown_path = layout["reports"] / "asia_drift_opportunity_taxonomy_summary.md"
    window_rows_path = layout["reports"] / "asia_drift_opportunity_taxonomy_window_rows.csv"
    session_manifest_path = layout["reports"] / "asia_drift_opportunity_taxonomy_sessions.json"
    session_rows_path = layout["signals"] / "asia_drift_opportunity_taxonomy_sessions.csv"
    missed_audit_path = layout["signals"] / "asia_drift_opportunity_taxonomy_missed_patterns.csv"

    summary_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown_path.write_text(_render_taxonomy_markdown(payload), encoding="utf-8")
    session_manifest_path.write_text(json.dumps(payload.get("session_taxonomy_manifest") or [], indent=2, sort_keys=True), encoding="utf-8")
    _write_csv(window_rows_path, payload.get("window_summaries") or [])
    _write_csv(session_rows_path, payload.get("session_taxonomy_manifest") or [])
    _write_csv(missed_audit_path, payload.get("missed_pattern_audit") or [])

    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_v1_opportunity_taxonomy",
            "layout": {key: str(value) for key, value in layout.items()},
            "artifact_paths": {
                "summary_json_path": str(summary_json_path),
                "summary_markdown_path": str(summary_markdown_path),
                "window_rows_path": str(window_rows_path),
                "session_manifest_path": str(session_manifest_path),
                "session_rows_path": str(session_rows_path),
                "missed_audit_path": str(missed_audit_path),
            },
        },
    )
    return {
        "summary_json_path": str(summary_json_path),
        "summary_markdown_path": str(summary_markdown_path),
        "window_rows_path": str(window_rows_path),
        "session_manifest_path": str(session_manifest_path),
        "session_rows_path": str(session_rows_path),
        "missed_audit_path": str(missed_audit_path),
        "storage_manifest_path": str(layout["storage_manifest"]),
    }


def _render_taxonomy_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia Drift v1 Opportunity Taxonomy",
        "",
        str(payload.get("objective") or ""),
        "",
        f"- primary_instrument: {payload.get('primary_instrument')}",
        f"- primary_instrument_family: {payload.get('primary_instrument_family')}",
        f"- calibration_profile: {payload.get('calibration_profile')}",
        f"- primary_entry_model: {payload.get('primary_entry_model')}",
        "",
        "## Window Summaries",
    ]
    for row in payload.get("window_summaries") or []:
        lines.append(
            f"- {row['instrument']} {row['window_label']}: sessions={row['asia_session_count']} "
            f"continuation_sessions={row['continuation_session_count']} accepted_entry_sessions={row['accepted_entry_session_count']} "
            f"missed_pattern_count={row['missed_pattern_count']} taxonomy_counts={row['taxonomy_counts']}"
        )
    lines.extend(["", "## Aggregate Summary"])
    for instrument, summary in sorted((payload.get("aggregate_summary") or {}).items()):
        lines.append(
            f"- {instrument}: sessions={summary.get('session_count')} continuation_sessions={summary.get('continuation_session_count')} "
            f"accepted_entry_sessions={summary.get('accepted_entry_session_count')} missed_pattern_count={summary.get('missed_pattern_count')}"
        )
        lines.append(f"  taxonomy_counts={summary.get('taxonomy_counts')}")
        lines.append(f"  candidate_opportunity_types={summary.get('candidate_opportunity_types_worth_research')}")
        lines.append(f"  branch_reference_counts={summary.get('branch_reference_counts')}")
    lines.extend(["", "## Family Summary"])
    for family, summary in sorted((payload.get("family_summary") or {}).items()):
        lines.append(
            f"- {family}: sessions={summary.get('session_count')} continuation_sessions={summary.get('continuation_session_count')} "
            f"accepted_entry_sessions={summary.get('accepted_entry_session_count')}"
        )
        lines.append(f"  taxonomy_counts={summary.get('taxonomy_counts')}")
        lines.append(f"  branch_reference_counts={summary.get('branch_reference_counts')}")
        lines.append(f"  candidate_opportunity_types={summary.get('candidate_opportunity_types_worth_research')}")
    lines.extend(["", "## Branch Transfer"])
    for branch, summary in sorted(((payload.get("branch_transfer_summary") or {}).get("branches") or {}).items()):
        lines.append(
            f"- {branch}: transfer_status={summary.get('transfer_status')} "
            f"metals_count={summary.get('metals_count')} indices_count={summary.get('indices_count')}"
        )
    lines.extend(["", "## Index Behavior"])
    index_behavior = payload.get("index_behavior_summary") or {}
    for key in (
        "continuation_session_count",
        "compression_then_breakout_count",
        "vwap_reclaim_breakout_count",
        "slow_grind_without_pullback_count",
        "late_asia_early_london_continuation_count",
        "failed_mean_reversion_count",
        "risk_on_risk_off_synchronized_dates",
        "risk_on_risk_off_synchronized_session_count",
    ):
        lines.append(f"- {key}: {index_behavior.get(key)}")
    lines.extend(["", "## Recommendation"])
    recommendation = payload.get("recommendation") or {}
    lines.append(f"- recommendation: {recommendation.get('recommendation')}")
    lines.append(f"- next_best_research_hypothesis: {recommendation.get('next_best_research_hypothesis')}")
    lines.append(f"- reason: {recommendation.get('reason')}")
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


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator
