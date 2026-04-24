"""Multi-year 5-minute structural discovery for Asia/overnight sessions."""

from __future__ import annotations

import csv
import json
import math
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Sequence

from ..trend_participation.models import ResearchBar
from ..trend_participation.storage import build_layout, write_storage_manifest
from .engine import AsiaDriftPhase1Run, run_asia_drift_phase1, run_asia_drift_phase1_from_bars
from .broader_replay import AsiaDriftReplayBarWindow, AsiaDriftReplayWindow
from .features import ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT, RECOVERY_CONFIRMED
from .open_pattern_clusters import (
    CLUSTER_INPUT_FEATURES,
    DEFAULT_OPEN_CLUSTER_COUNT,
    _augment_cross_asset_alignment,
    _cluster_feature_rows,
    _session_feature_row,
    _template_vs_cluster_comparison,
)
from .taxonomy import (
    OPP_CLEAN_DIRECTIONAL_DRIFT,
    OPP_COMPRESSION_THEN_CONTINUATION,
    OPP_FAILED_MEAN_REVERSION,
    OPP_IMPULSE_THEN_PAUSE,
    OPP_LATE_SESSION_DRIFT,
    OPP_NO_DRIFT_CHOP,
    OPP_OTHER_DIRECTIONAL,
    OPP_POST_SPIKE_INSTABILITY,
    OPP_VWAP_REJECTION_CONTINUATION,
    _directional_efficiency_bucket,
    _impulse_then_pause,
    _market_texture_tag,
    _peak_close_location_persistence,
    _peak_signed_vwap_displacement,
    _peak_slope_persistence,
    _range_expansion_bucket,
    _realized_volatility_bucket,
    _time_of_night_bucket,
)


DEFAULT_MULTI_YEAR_INSTRUMENTS = ("MGC", "GC", "MES", "ES", "MNQ", "NQ")
DEFAULT_DIAGNOSTIC_INSTRUMENTS = ("MNQ", "NQ")
EARLY_WINDOW_BARS = 12

EARLY_FEATURE_NAMES = (
    "early_signed_displacement_atr",
    "early_directional_efficiency_median",
    "early_close_location_persistence_peak",
    "early_signed_vwap_displacement_peak",
    "early_vwap_crossing_rate",
    "early_vwap_reclaim_rate",
    "early_compression_fraction",
    "early_expansion_readiness",
    "early_overlap_ratio_median",
    "early_reversal_frequency_median",
    "early_range_stability",
    "early_failed_countertrend_rate",
    "early_drift_context_fraction",
    "early_chop_fraction",
    "early_directional_skew",
    "early_realized_volatility_ratio_median",
    "early_post_spike_fraction",
    "early_deep_damage_fraction",
    "early_first_impulse_index_norm",
    "cross_asset_sync_count",
)


def run_asia_drift_multi_year_discovery(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    instruments: Sequence[str] = DEFAULT_MULTI_YEAR_INSTRUMENTS,
    cluster_count: int = DEFAULT_OPEN_CLUSTER_COUNT,
) -> dict[str, Any]:
    coverage_rows = _sqlite_instrument_coverage(sqlite_path=source_sqlite_path, instruments=instruments)
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase1Run]]] = {}
    for coverage in coverage_rows:
        if coverage["coverage_start_ts"] is None or coverage["coverage_end_ts"] is None:
            continue
        instrument = coverage["instrument"]
        window = AsiaDriftReplayWindow(
            label=f"{instrument.lower()}_{coverage['coverage_start_ts'][:10].replace('-', '')}_{coverage['coverage_end_ts'][:10].replace('-', '')}",
            start_ts=datetime.fromisoformat(coverage["coverage_start_ts"]),
            end_ts=datetime.fromisoformat(coverage["coverage_end_ts"]),
        )
        run = run_asia_drift_phase1(
            source_sqlite_path=source_sqlite_path,
            output_dir=output_dir / instrument.lower() / window.label,
            instruments=(instrument,),
            start_ts=window.start_ts,
            end_ts=window.end_ts,
            calibration_profile_name=RECOVERY_CONFIRMED,
        )
        instrument_runs[instrument] = [(window, run)]
    payload = _build_multi_year_payload(
        instrument_runs=instrument_runs,
        source_label=str(source_sqlite_path.resolve()),
        coverage_rows=coverage_rows,
        requested_cluster_count=cluster_count,
    )
    artifacts = _write_multi_year_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def run_asia_drift_multi_year_discovery_from_bars(
    *,
    output_dir: Path,
    instrument_windows: dict[str, Sequence[AsiaDriftReplayBarWindow]],
    cluster_count: int = DEFAULT_OPEN_CLUSTER_COUNT,
) -> dict[str, Any]:
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase1Run]]] = {}
    coverage_rows: list[dict[str, Any]] = []
    for instrument, windows in sorted(instrument_windows.items()):
        converted: list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase1Run]] = []
        all_bars = [bar for window in windows for bar in window.bars_5m]
        coverage_rows.append(
            {
                "instrument": instrument,
                "timeframe": "5m",
                "coverage_start_ts": all_bars[0].end_ts.isoformat() if all_bars else None,
                "coverage_end_ts": all_bars[-1].end_ts.isoformat() if all_bars else None,
                "bar_count": len(all_bars),
                "instrument_family": _instrument_family(instrument),
                "diagnostic_only": instrument in DEFAULT_DIAGNOSTIC_INSTRUMENTS,
            }
        )
        for window in windows:
            run = run_asia_drift_phase1_from_bars(
                output_dir=output_dir / instrument.lower() / window.label,
                bars_5m=window.bars_5m,
                bars_1m=window.bars_1m,
                source_label=window.label,
                calibration_profile_name=RECOVERY_CONFIRMED,
            )
            converted.append(
                (
                    AsiaDriftReplayWindow(
                        label=window.label,
                        start_ts=window.bars_5m[0].end_ts if window.bars_5m else datetime.fromisoformat("1970-01-01T00:00:00+00:00"),
                        end_ts=window.bars_5m[-1].end_ts if window.bars_5m else datetime.fromisoformat("1970-01-01T00:00:00+00:00"),
                    ),
                    run,
                )
            )
        instrument_runs[instrument] = converted
    payload = _build_multi_year_payload(
        instrument_runs=instrument_runs,
        source_label="synthetic",
        coverage_rows=coverage_rows,
        requested_cluster_count=cluster_count,
    )
    artifacts = _write_multi_year_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def _build_multi_year_payload(
    *,
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase1Run]]],
    source_label: str,
    coverage_rows: Sequence[dict[str, Any]],
    requested_cluster_count: int,
) -> dict[str, Any]:
    session_feature_rows: list[dict[str, Any]] = []
    early_feature_rows: list[dict[str, Any]] = []
    session_manifest_rows: list[dict[str, Any]] = []

    for instrument, window_runs in sorted(instrument_runs.items()):
        for window, run in window_runs:
            features_by_session = _group_by(run.feature_rows, key=lambda row: row.asia_drift_session_id)
            summaries_by_session = {row.asia_drift_session_id: row for row in run.session_summaries}
            for session_id, summary in sorted(summaries_by_session.items()):
                per_bar_rows = sorted(features_by_session.get(session_id, []), key=lambda row: row.decision_ts)
                taxonomy_row = _template_session_row(
                    instrument=instrument,
                    window=window,
                    summary=summary,
                    feature_rows=per_bar_rows,
                )
                full_row = _session_feature_row(
                    instrument=instrument,
                    window=window,
                    summary=summary,
                    taxonomy_row=taxonomy_row,
                    feature_rows=per_bar_rows,
                )
                early_row = _early_session_feature_row(
                    instrument=instrument,
                    window=window,
                    summary=summary,
                    taxonomy_row=taxonomy_row,
                    feature_rows=per_bar_rows,
                )
                if full_row is not None:
                    session_feature_rows.append(full_row)
                    session_manifest_rows.append(
                        {
                            "instrument": instrument,
                            "instrument_family": _instrument_family(instrument),
                            "window_label": window.label,
                            "asia_drift_session_id": summary.asia_drift_session_id,
                            "local_session_date": summary.local_session_date.isoformat(),
                            "template_opportunity_type": taxonomy_row.get("opportunity_type") if taxonomy_row else None,
                            "directional_continuation": bool(taxonomy_row.get("directional_continuation")) if taxonomy_row else False,
                            "continuation_quality": taxonomy_row.get("continuation_quality") if taxonomy_row else "NONE",
                            "diagnostic_only": instrument in DEFAULT_DIAGNOSTIC_INSTRUMENTS,
                        }
                    )
                if early_row is not None:
                    early_feature_rows.append(early_row)

    _augment_cross_asset_alignment(session_feature_rows)
    _augment_early_cross_asset_alignment(early_feature_rows)
    cluster_count = _resolved_cluster_count(session_count=len(session_feature_rows), requested_cluster_count=requested_cluster_count)
    assignments, centroids, scaling = _cluster_feature_rows(feature_rows=session_feature_rows, cluster_count=cluster_count)
    cluster_assignment_rows = _cluster_assignment_rows(session_feature_rows=session_feature_rows, assignments=assignments)
    cluster_summaries = _multi_year_cluster_summaries(
        cluster_assignment_rows=cluster_assignment_rows,
        centroids=centroids,
        scaling=scaling,
    )
    _attach_cluster_summary_fields(cluster_assignment_rows=cluster_assignment_rows, cluster_summaries=cluster_summaries)
    template_comparison = _template_vs_cluster_comparison(cluster_rows=cluster_assignment_rows)
    directional_analysis = _conditioned_directional_analysis(early_feature_rows=early_feature_rows)
    recommendation = _multi_year_recommendation(
        cluster_summaries=cluster_summaries,
        candidate_condition_sets=directional_analysis["candidate_condition_sets"],
    )

    return {
        "module": "Asia Drift Multi-Year 5-Minute Discovery",
        "objective": (
            "Research-only Layer 1 structural discovery over multi-year 5-minute overnight/Asia sessions. "
            "This pass identifies natural session structures first, then compares early-session positive vs "
            "matched control behavior to isolate candidate directional condition sets without building trade logic."
        ),
        "source_label": source_label,
        "instruments": sorted(instrument_runs),
        "coverage_report": {
            "coverage_rows": list(coverage_rows),
            "coverage_start_ts": _min_str([row["coverage_start_ts"] for row in coverage_rows]),
            "coverage_end_ts": _max_str([row["coverage_end_ts"] for row in coverage_rows]),
            "diagnostic_instruments": [instrument for instrument in sorted(instrument_runs) if instrument in DEFAULT_DIAGNOSTIC_INSTRUMENTS],
        },
        "cluster_input_features": list(CLUSTER_INPUT_FEATURES),
        "early_feature_names": list(EARLY_FEATURE_NAMES),
        "requested_cluster_count": requested_cluster_count,
        "actual_cluster_count": cluster_count,
        "feature_matrix_rows": session_feature_rows,
        "cluster_assignment_rows": cluster_assignment_rows,
        "cluster_summaries": cluster_summaries,
        "early_feature_matrix_rows": early_feature_rows,
        "positive_vs_control_comparison": directional_analysis["positive_vs_control_comparison"],
        "discriminator_analysis": directional_analysis["discriminator_analysis"],
        "candidate_condition_sets": directional_analysis["candidate_condition_sets"],
        "template_vs_cluster_comparison": template_comparison,
        "session_manifest": session_manifest_rows,
        "recommended_next_research_queue": recommendation["next_research_queue"],
        "recommendation": recommendation,
    }


def _sqlite_instrument_coverage(*, sqlite_path: Path, instruments: Sequence[str]) -> list[dict[str, Any]]:
    connection = sqlite3.connect(sqlite_path)
    try:
        placeholders = ",".join("?" for _ in instruments)
        rows = connection.execute(
            f"""
            select symbol, timeframe, min(end_ts), max(end_ts), count(*)
            from bars
            where timeframe = '1m' and symbol in ({placeholders})
            group by symbol, timeframe
            order by symbol
            """,
            tuple(instruments),
        ).fetchall()
    finally:
        connection.close()
    return [
        {
            "instrument": str(symbol).upper(),
            "timeframe": str(timeframe),
            "coverage_start_ts": str(start_ts) if start_ts is not None else None,
            "coverage_end_ts": str(end_ts) if end_ts is not None else None,
            "bar_count": int(count),
            "instrument_family": _instrument_family(str(symbol).upper()),
            "diagnostic_only": str(symbol).upper() in DEFAULT_DIAGNOSTIC_INSTRUMENTS,
        }
        for symbol, timeframe, start_ts, end_ts, count in rows
    ]


def _template_session_row(
    *,
    instrument: str,
    window: AsiaDriftReplayWindow,
    summary: Any,
    feature_rows: Sequence[Any],
) -> dict[str, Any] | None:
    rows = [row for row in feature_rows if row.in_scope] or list(feature_rows)
    if not rows:
        return None
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
    meaningful_pullback_rows = [row for row in pre_continuation_rows if row.pullback_state != "NO_PULLBACK" or row.fast_pullback_class != "SLOW_OR_STANDARD"]
    fast_shallow_valid = any(row.fast_pullback_class == "FAST_SHALLOW_VALID" for row in pre_continuation_rows)
    fast_stretched_warning = any(row.fast_pullback_class == "FAST_STRETCHED_WARNING" for row in pre_continuation_rows)
    fast_deep_disqualifying = any(row.fast_pullback_class == "FAST_DEEP_DISQUALIFYING" for row in pre_continuation_rows)
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
    elif fast_deep_disqualifying or any(row.pullback_state == "DISQUALIFYING_PULLBACK" for row in pre_continuation_rows):
        pullback_descriptor = "DEEP_OR_DISQUALIFYING"
    elif any(row.pullback_state == "STRETCHED_BUT_VALID" for row in pre_continuation_rows):
        pullback_descriptor = "STRETCHED"
    elif any(row.pullback_state == "NORMAL_PULLBACK" for row in pre_continuation_rows):
        pullback_descriptor = "NORMAL_PULLBACK"
    else:
        pullback_descriptor = "SLOW_OR_STANDARD"
    late_continuation_session = bool(continuation_present and continuation_row is not None and continuation_row.subphase == "ASIA_DRIFT_PRE_HANDOFF")

    if continuation_present and continuation_row is not None and continuation_row.subphase == "ASIA_DRIFT_PRE_HANDOFF":
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

    continuation_quality = (
        "STRONG" if max_favorable_atr >= 1.50 else "MEDIUM" if max_favorable_atr >= 1.00 else "WEAK" if max_favorable_atr >= 0.90 else "NONE"
    )
    return {
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
        "continuation_begin_subphase": continuation_row.subphase if continuation_row is not None else None,
        "pullback_descriptor": pullback_descriptor,
        "compression_then_expansion_session": compression_then_expansion,
        "failed_mean_reversion_session": countertrend_failed,
        "late_continuation_session": late_continuation_session,
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
    }


def _early_session_feature_row(
    *,
    instrument: str,
    window: AsiaDriftReplayWindow,
    summary: Any,
    taxonomy_row: dict[str, Any] | None,
    feature_rows: Sequence[Any],
) -> dict[str, Any] | None:
    rows = [row for row in feature_rows if row.in_scope] or list(feature_rows)
    if not rows:
        return None
    candidate_index = next((index for index, row in enumerate(rows) if row.regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}), 0)
    early_rows = rows[candidate_index : candidate_index + EARLY_WINDOW_BARS]
    if not early_rows:
        early_rows = rows[: min(EARLY_WINDOW_BARS, len(rows))]
    if not early_rows:
        return None
    direction = summary.candidate_direction if summary.candidate_direction in {"LONG", "SHORT"} else early_rows[0].dominant_direction
    atr_base = max(early_rows[0].atr, 1e-9)
    signed_displacement = (
        (early_rows[-1].close - early_rows[0].session_open) / atr_base
        if direction == "LONG"
        else (early_rows[0].session_open - early_rows[-1].close) / atr_base
    )
    range_norms = [row.range_points / max(row.atr, 1e-9) for row in early_rows]
    compression_flags = [
        (
            range_norm <= (median(range_norms) if range_norms else 0.0) * 0.95
            and row.bar_overlap_ratio_8 >= 0.60
            and row.realized_volatility_ratio <= 1.15
        )
        for row, range_norm in zip(early_rows, range_norms)
    ]
    expansion_readiness = max(range_norms) / max((median(range_norms) or 1e-9), 1e-9)
    vwap_cross_count, vwap_reclaim_count = _vwap_cross_metrics(early_rows, direction=direction)
    first_impulse_index = next((index for index, row in enumerate(early_rows) if row.fresh_drift_impulse), None)
    aligned_upside = [
        row.upside_extension_atr if direction == "LONG" else row.downside_extension_atr
        for row in early_rows
    ]
    aligned_adverse = [
        row.downside_extension_atr if direction == "LONG" else row.upside_extension_atr
        for row in early_rows
    ]
    return {
        "instrument": instrument,
        "instrument_family": _instrument_family(instrument),
        "window_label": window.label,
        "asia_drift_session_id": summary.asia_drift_session_id,
        "local_session_date": summary.local_session_date.isoformat(),
        "candidate_direction": direction,
        "template_opportunity_type": taxonomy_row.get("opportunity_type") if taxonomy_row else None,
        "directional_continuation": bool(taxonomy_row.get("directional_continuation")) if taxonomy_row else False,
        "continuation_quality": taxonomy_row.get("continuation_quality") if taxonomy_row else "NONE",
        "early_bar_count": len(early_rows),
        "early_signed_displacement_atr": signed_displacement,
        "early_directional_efficiency_median": _median([row.efficiency_ratio_12 for row in early_rows]) or 0.0,
        "early_close_location_persistence_peak": _peak_aligned(
            early_rows,
            direction=direction,
            long_attr="close_location_persistence_long",
            short_attr="close_location_persistence_short",
        ),
        "early_signed_vwap_displacement_peak": _peak_aligned(
            early_rows,
            direction=direction,
            long_attr="signed_vwap_displacement_long",
            short_attr="signed_vwap_displacement_short",
        ),
        "early_vwap_crossing_rate": vwap_cross_count / max(len(early_rows), 1),
        "early_vwap_reclaim_rate": vwap_reclaim_count / max(len(early_rows), 1),
        "early_compression_fraction": _ratio(sum(1 for flag in compression_flags if flag), len(early_rows)),
        "early_expansion_readiness": expansion_readiness,
        "early_overlap_ratio_median": _median([row.bar_overlap_ratio_8 for row in early_rows]) or 0.0,
        "early_reversal_frequency_median": _median([row.reversal_frequency_12 for row in early_rows]) or 0.0,
        "early_range_stability": _range_stability(range_norms),
        "early_failed_countertrend_rate": _ratio(sum(1 for row in early_rows if row.countertrend_extension_failed), len(early_rows)),
        "early_drift_context_fraction": _ratio(sum(1 for row in early_rows if row.regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}), len(early_rows)),
        "early_chop_fraction": _ratio(sum(1 for row in early_rows if row.chop_veto), len(early_rows)),
        "early_directional_skew": max(aligned_upside) - max(aligned_adverse),
        "early_realized_volatility_ratio_median": _median([row.realized_volatility_ratio for row in early_rows]) or 0.0,
        "early_post_spike_fraction": _ratio(sum(1 for row in early_rows if row.post_spike_instability), len(early_rows)),
        "early_deep_damage_fraction": _ratio(
            sum(
                1
                for row in early_rows
                if row.pullback_hard_invalidation_candidate or row.pullback_too_extended or row.pullback_structure_break
            ),
            len(early_rows),
        ),
        "early_first_impulse_index_norm": ((first_impulse_index or len(early_rows) - 1) / max(len(early_rows) - 1, 1)),
        "cross_asset_sync_count": 0,
        "cross_asset_family_alignment": "NONE",
        "contaminated": any(row.post_spike_instability for row in early_rows)
        or any(row.pullback_hard_invalidation_candidate or row.pullback_too_extended for row in early_rows),
        "diagnostic_only": instrument in DEFAULT_DIAGNOSTIC_INSTRUMENTS,
    }


def _conditioned_directional_analysis(*, early_feature_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    positives = [row for row in early_feature_rows if row["directional_continuation"]]
    controls = [row for row in early_feature_rows if not row["directional_continuation"]]
    matched_controls, match_rows = _match_positive_controls(positives=positives, controls=controls)
    comparison = {
        "positive_session_count": len(positives),
        "matched_control_session_count": len(matched_controls),
        "positive_instrument_breakdown": dict(Counter(row["instrument"] for row in positives)),
        "matched_control_instrument_breakdown": dict(Counter(row["instrument"] for row in matched_controls)),
        "positive_month_spread": len({row["local_session_date"][:7] for row in positives}),
        "matched_control_month_spread": len({row["local_session_date"][:7] for row in matched_controls}),
        "matching_rows": match_rows,
        "method": (
            "Nearest-neighbor matched controls using early-session features only. "
            f"Features are computed from the first {EARLY_WINDOW_BARS} in-scope 5-minute bars after initial regime context."
        ),
        "future_leakage_safe": True,
    }
    discriminator_rows = _discriminator_analysis(positives=positives, matched_controls=matched_controls)
    condition_sets = _candidate_condition_sets(early_feature_rows=early_feature_rows, discriminator_rows=discriminator_rows)
    return {
        "positive_vs_control_comparison": comparison,
        "discriminator_analysis": discriminator_rows,
        "candidate_condition_sets": condition_sets,
    }


def _match_positive_controls(
    *,
    positives: Sequence[dict[str, Any]],
    controls: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not positives or not controls:
        return [], []
    matrix = [[float(row[name]) for name in EARLY_FEATURE_NAMES] for row in [*positives, *controls]]
    scaling = _fit_standard_scaler(matrix, feature_names=EARLY_FEATURE_NAMES)
    scaled_positive = [_apply_standard_scaler([float(row[name]) for name in EARLY_FEATURE_NAMES], scaling, feature_names=EARLY_FEATURE_NAMES) for row in positives]
    scaled_controls = [_apply_standard_scaler([float(row[name]) for name in EARLY_FEATURE_NAMES], scaling, feature_names=EARLY_FEATURE_NAMES) for row in controls]
    matched_controls: list[dict[str, Any]] = []
    match_rows: list[dict[str, Any]] = []
    used_control_ids: set[str] = set()
    for positive_row, scaled_row in zip(positives, scaled_positive, strict=False):
        best_index = None
        best_distance = None
        for index, control_row in enumerate(controls):
            if control_row["asia_drift_session_id"] in used_control_ids:
                continue
            distance = _euclidean_sq(scaled_row, scaled_controls[index])
            if best_distance is None or distance < best_distance:
                best_distance = distance
                best_index = index
        if best_index is None:
            continue
        control_row = controls[best_index]
        used_control_ids.add(control_row["asia_drift_session_id"])
        matched_controls.append(control_row)
        match_rows.append(
            {
                "positive_session_id": positive_row["asia_drift_session_id"],
                "positive_instrument": positive_row["instrument"],
                "positive_local_session_date": positive_row["local_session_date"],
                "control_session_id": control_row["asia_drift_session_id"],
                "control_instrument": control_row["instrument"],
                "control_local_session_date": control_row["local_session_date"],
                "distance": math.sqrt(float(best_distance or 0.0)),
            }
        )
    return matched_controls, match_rows


def _discriminator_analysis(
    *,
    positives: Sequence[dict[str, Any]],
    matched_controls: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not positives or not matched_controls:
        return []
    rows: list[dict[str, Any]] = []
    for feature_name in EARLY_FEATURE_NAMES:
        pos_values = [float(row[feature_name]) for row in positives]
        ctl_values = [float(row[feature_name]) for row in matched_controls]
        overall_values = pos_values + ctl_values
        overall_mean = sum(overall_values) / len(overall_values)
        variance = sum((value - overall_mean) ** 2 for value in overall_values) / max(len(overall_values), 1)
        stdev = math.sqrt(variance) if variance > 1e-9 else 1.0
        positive_median = float(median(pos_values))
        control_median = float(median(ctl_values))
        positive_mean = sum(pos_values) / len(pos_values)
        control_mean = sum(ctl_values) / len(ctl_values)
        effect = (positive_mean - control_mean) / stdev
        rows.append(
            {
                "feature_name": feature_name,
                "positive_median": positive_median,
                "matched_control_median": control_median,
                "positive_mean": positive_mean,
                "matched_control_mean": control_mean,
                "median_delta": positive_median - control_median,
                "standardized_effect": effect,
                "discriminator_direction": "higher_in_positive" if effect > 0.10 else "lower_in_positive" if effect < -0.10 else "similar",
                "future_leakage_safe": True,
            }
        )
    rows.sort(key=lambda row: abs(float(row["standardized_effect"])), reverse=True)
    return rows


def _candidate_condition_sets(
    *,
    early_feature_rows: Sequence[dict[str, Any]],
    discriminator_rows: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    positives = [row for row in early_feature_rows if row["directional_continuation"]]
    controls = [row for row in early_feature_rows if not row["directional_continuation"]]
    if not early_feature_rows or not positives:
        return []

    def q(values: Sequence[float], level: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = min(max(int(round((len(ordered) - 1) * level)), 0), len(ordered) - 1)
        return float(ordered[index])

    pos = lambda name: [float(row[name]) for row in positives]
    allv = lambda name: [float(row[name]) for row in early_feature_rows]

    condition_specs = [
        {
            "condition_set_name": "COMPRESSION_WITH_DIRECTIONAL_SKEW",
            "plain_english": "Early overlap and contraction exist, but directional skew and VWAP pressure are already leaning one way.",
            "measurable_definition": (
                "early_compression_fraction >= positive_p60 and early_signed_vwap_displacement_peak >= positive_p60 "
                "with low early deep-damage contamination."
            ),
            "predicate": lambda row: (
                float(row["early_compression_fraction"]) >= q(pos("early_compression_fraction"), 0.60)
                and float(row["early_signed_vwap_displacement_peak"]) >= q(pos("early_signed_vwap_displacement_peak"), 0.60)
                and float(row["early_deep_damage_fraction"]) < 0.10
            ),
        },
        {
            "condition_set_name": "FAILED_MEAN_REVERSION_PRESSURE",
            "plain_english": "Countertrend attempts fail early while drift context persists.",
            "measurable_definition": (
                "early_failed_countertrend_rate >= positive_p60 and early_drift_context_fraction >= positive_p50 "
                "with early_reversal_frequency_median below overall_p60."
            ),
            "predicate": lambda row: (
                float(row["early_failed_countertrend_rate"]) >= q(pos("early_failed_countertrend_rate"), 0.60)
                and float(row["early_drift_context_fraction"]) >= q(pos("early_drift_context_fraction"), 0.50)
                and float(row["early_reversal_frequency_median"]) <= q(allv("early_reversal_frequency_median"), 0.60)
            ),
        },
        {
            "condition_set_name": "VWAP_PRESSURE_ACCUMULATION",
            "plain_english": "Directional pressure builds around VWAP without a fully noisy cross/reclaim sequence.",
            "measurable_definition": (
                "early_signed_vwap_displacement_peak >= positive_p70, early_close_location_persistence_peak >= positive_p60, "
                "and early_vwap_crossing_rate <= overall_p60."
            ),
            "predicate": lambda row: (
                float(row["early_signed_vwap_displacement_peak"]) >= q(pos("early_signed_vwap_displacement_peak"), 0.70)
                and float(row["early_close_location_persistence_peak"]) >= q(pos("early_close_location_persistence_peak"), 0.60)
                and float(row["early_vwap_crossing_rate"]) <= q(allv("early_vwap_crossing_rate"), 0.60)
            ),
        },
        {
            "condition_set_name": "ASYMMETRIC_CHOP_STRUCTURE",
            "plain_english": "The early session still looks choppy, but the chop is directionally asymmetric rather than dead.",
            "measurable_definition": (
                "early_chop_fraction >= overall_p50 with early_directional_skew >= positive_p60 "
                "and early_range_stability >= positive_p50."
            ),
            "predicate": lambda row: (
                float(row["early_chop_fraction"]) >= q(allv("early_chop_fraction"), 0.50)
                and float(row["early_directional_skew"]) >= q(pos("early_directional_skew"), 0.60)
                and float(row["early_range_stability"]) >= q(pos("early_range_stability"), 0.50)
            ),
        },
        {
            "condition_set_name": "VOLATILITY_CONTRACTION_BEFORE_EXPANSION",
            "plain_english": "Realized volatility contracts early, then the session later resolves directionally.",
            "measurable_definition": (
                "early_realized_volatility_ratio_median <= positive_p40 and early_compression_fraction >= positive_p50 "
                "with early_expansion_readiness >= positive_p50."
            ),
            "predicate": lambda row: (
                float(row["early_realized_volatility_ratio_median"]) <= q(pos("early_realized_volatility_ratio_median"), 0.40)
                and float(row["early_compression_fraction"]) >= q(pos("early_compression_fraction"), 0.50)
                and float(row["early_expansion_readiness"]) >= q(pos("early_expansion_readiness"), 0.50)
            ),
        },
        {
            "condition_set_name": "CROSS_ASSET_CONFIRMATION",
            "plain_english": "The same overnight date shows aligned directional pressure across multiple instruments.",
            "measurable_definition": (
                "cross_asset_sync_count >= 2 with early_signed_vwap_displacement_peak >= positive_p50 "
                "and low early post-spike contamination."
            ),
            "predicate": lambda row: (
                float(row["cross_asset_sync_count"]) >= 2.0
                and float(row["early_signed_vwap_displacement_peak"]) >= q(pos("early_signed_vwap_displacement_peak"), 0.50)
                and float(row["early_post_spike_fraction"]) < 0.10
            ),
        },
    ]

    results: list[dict[str, Any]] = []
    for spec in condition_specs:
        matched = [row for row in early_feature_rows if spec["predicate"](row)]
        if not matched:
            continue
        positive_count = sum(1 for row in matched if row["directional_continuation"])
        control_count = len(matched) - positive_count
        month_counts = Counter(row["local_session_date"][:7] for row in matched)
        instrument_counts = Counter(row["instrument"] for row in matched)
        contamination_count = sum(
            1 for row in matched if float(row["early_post_spike_fraction"]) >= 0.10 or float(row["early_deep_damage_fraction"]) >= 0.10
        )
        results.append(
            {
                "condition_set_name": spec["condition_set_name"],
                "plain_english_description": spec["plain_english"],
                "measurable_definition": spec["measurable_definition"],
                "sample_size": len(matched),
                "positive_hit_rate": _ratio(positive_count, len(matched)),
                "false_positive_rate": _ratio(control_count, len(matched)),
                "frequency": _ratio(len(matched), len(early_feature_rows)),
                "positive_count": positive_count,
                "control_count": control_count,
                "instrument_spread": len(instrument_counts),
                "instruments": sorted(instrument_counts),
                "instrument_family_concentration": dict(Counter(row["instrument_family"] for row in matched)),
                "date_month_spread": len(month_counts),
                "date_month_buckets": sorted(month_counts),
                "contamination_risk": _ratio(contamination_count, len(matched)),
                "clustering_risk": _date_clustering_risk(counts=month_counts),
                "overfit_warnings": _overfit_warnings(
                    sample_size=len(matched),
                    spread=len(month_counts),
                    instrument_spread=len(instrument_counts),
                    contamination_risk=_ratio(contamination_count, len(matched)),
                ),
                "future_leakage_safe": True,
            }
        )
    results.sort(
        key=lambda row: (
            -(
                1.8 * float(row["positive_hit_rate"])
                + 0.15 * int(row["sample_size"])
                + 0.10 * int(row["date_month_spread"])
                - 1.2 * float(row["contamination_risk"])
                - (0.75 if row["clustering_risk"] == "HIGH" else 0.35 if row["clustering_risk"] == "MEDIUM" else 0.0)
            ),
            row["condition_set_name"],
        )
    )
    return results


def _multi_year_cluster_summaries(
    *,
    cluster_assignment_rows: Sequence[dict[str, Any]],
    centroids: Sequence[Sequence[float]],
    scaling: dict[str, dict[str, float]],
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for cluster_name in sorted({row["cluster_id"] for row in cluster_assignment_rows}):
        rows = [row for row in cluster_assignment_rows if row["cluster_id"] == cluster_name]
        template_counts = Counter(row["template_opportunity_type"] for row in rows)
        instrument_counts = Counter(row["instrument"] for row in rows)
        family_counts = Counter(row["instrument_family"] for row in rows)
        month_counts = Counter(row["local_session_date"][:7] for row in rows)
        continuation_rate = _ratio(sum(1 for row in rows if row["directional_continuation"]), len(rows))
        contamination_risk = _ratio(
            sum(1 for row in rows if float(row["post_spike_fraction"]) >= 0.10 or float(row["failed_countertrend_rate"]) >= 0.35),
            len(rows),
        )
        cluster_index = int(cluster_name.split("_")[-1])
        raw_centroid = _invert_standard_scaler(list(centroids[cluster_index]), scaling, feature_names=CLUSTER_INPUT_FEATURES)
        interpretive_name = _interpret_cluster_name(rows=rows, centroid=raw_centroid)
        summaries.append(
            {
                "cluster_id": cluster_name,
                "interpretive_name": interpretive_name,
                "sample_size": len(rows),
                "date_month_spread": len(month_counts),
                "date_month_buckets": sorted(month_counts),
                "instrument_count": len(instrument_counts),
                "instruments": sorted(instrument_counts),
                "instrument_family_concentration": dict(family_counts),
                "template_distribution": dict(template_counts),
                "dominant_template": template_counts.most_common(1)[0][0] if template_counts else None,
                "dominant_template_share": _dominant_share(template_counts),
                "continuation_tendency": continuation_rate,
                "reversal_tendency": _ratio(
                    sum(
                        1
                        for row in rows
                        if float(row["max_adverse_excursion_atr"]) >= max(float(row["max_favorable_excursion_atr"]) * 0.75, 0.75)
                    ),
                    len(rows),
                ),
                "expansion_median_atr": _median([row["max_favorable_excursion_atr"] for row in rows if row["directional_continuation"]]),
                "contamination_risk": contamination_risk,
                "clustering_risk": _date_clustering_risk(counts=month_counts),
                "confidence_level": _confidence_level(
                    sample_size=len(rows),
                    spread=len(month_counts),
                    instrument_spread=len(instrument_counts),
                    contamination_risk=contamination_risk,
                ),
                "feature_centroid": raw_centroid,
                "overfit_warnings": _overfit_warnings(
                    sample_size=len(rows),
                    spread=len(month_counts),
                    instrument_spread=len(instrument_counts),
                    contamination_risk=contamination_risk,
                ),
            }
        )
    summaries.sort(
        key=lambda row: (
            -(1.6 * float(row["continuation_tendency"]) + 0.12 * int(row["sample_size"]) + 0.08 * int(row["date_month_spread"]) - 1.1 * float(row["contamination_risk"])),
            row["cluster_id"],
        )
    )
    return summaries


def _attach_cluster_summary_fields(
    *,
    cluster_assignment_rows: list[dict[str, Any]],
    cluster_summaries: Sequence[dict[str, Any]],
) -> None:
    by_cluster = {row["cluster_id"]: row for row in cluster_summaries}
    for row in cluster_assignment_rows:
        summary = by_cluster.get(row["cluster_id"])
        if summary is None:
            continue
        row["cluster_interpretive_name"] = summary["interpretive_name"]
        row["cluster_confidence_level"] = summary["confidence_level"]
        row["cluster_continuation_tendency"] = summary["continuation_tendency"]
        row["cluster_contamination_risk"] = summary["contamination_risk"]


def _cluster_assignment_rows(
    *,
    session_feature_rows: Sequence[dict[str, Any]],
    assignments: Sequence[int],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row, cluster_id in zip(session_feature_rows, assignments, strict=False):
        rows.append({**row, "cluster_id": f"cluster_{cluster_id}"})
    return rows


def _augment_early_cross_asset_alignment(early_feature_rows: Sequence[dict[str, Any]]) -> None:
    by_date: dict[str, list[dict[str, Any]]] = {}
    for row in early_feature_rows:
        by_date.setdefault(row["local_session_date"], []).append(row)
    for rows in by_date.values():
        directional = [row for row in rows if float(row["early_signed_vwap_displacement_peak"]) >= 0.50]
        direction_groups: dict[str, list[dict[str, Any]]] = {}
        for row in directional:
            direction_groups.setdefault(row["candidate_direction"], []).append(row)
        for aligned_rows in direction_groups.values():
            families = {row["instrument_family"] for row in aligned_rows}
            alignment = "CROSS_FAMILY_SYNC" if len(families) >= 2 else "SINGLE_FAMILY_SYNC"
            for row in aligned_rows:
                row["cross_asset_sync_count"] = max(int(row["cross_asset_sync_count"]), len(aligned_rows))
                row["cross_asset_family_alignment"] = alignment


def _fit_standard_scaler(matrix: Sequence[Sequence[float]], *, feature_names: Sequence[str]) -> dict[str, dict[str, float]]:
    columns = list(zip(*matrix))
    scaling: dict[str, dict[str, float]] = {}
    for feature, values in zip(feature_names, columns, strict=False):
        mean_value = sum(values) / len(values)
        variance = sum((value - mean_value) ** 2 for value in values) / max(len(values), 1)
        stdev = math.sqrt(variance)
        scaling[feature] = {"mean": mean_value, "stdev": stdev if stdev > 1e-9 else 1.0}
    return scaling


def _apply_standard_scaler(row: Sequence[float], scaling: dict[str, dict[str, float]], *, feature_names: Sequence[str]) -> list[float]:
    return [
        (float(value) - scaling[feature]["mean"]) / scaling[feature]["stdev"]
        for feature, value in zip(feature_names, row, strict=False)
    ]


def _invert_standard_scaler(values: Sequence[float], scaling: dict[str, dict[str, float]], *, feature_names: Sequence[str]) -> dict[str, float]:
    return {
        feature: values[index] * scaling[feature]["stdev"] + scaling[feature]["mean"]
        for index, feature in enumerate(feature_names)
    }


def _euclidean_sq(left: Sequence[float], right: Sequence[float]) -> float:
    return sum((l - r) ** 2 for l, r in zip(left, right, strict=False))


def _peak_aligned(rows: Sequence[Any], *, direction: str, long_attr: str, short_attr: str) -> float:
    if not rows:
        return 0.0
    if direction == "LONG":
        return max(float(getattr(row, long_attr)) for row in rows)
    return max(float(getattr(row, short_attr)) for row in rows)


def _vwap_cross_metrics(rows: Sequence[Any], *, direction: str) -> tuple[int, int]:
    previous_supportive: bool | None = None
    crossings = 0
    reclaims = 0
    for row in rows:
        aligned = (row.close - row.session_vwap) / max(row.atr, 1e-9) if direction == "LONG" else (row.session_vwap - row.close) / max(row.atr, 1e-9)
        supportive = aligned >= 0.0
        if previous_supportive is not None and supportive != previous_supportive:
            crossings += 1
            if supportive:
                reclaims += 1
        previous_supportive = supportive
    return crossings, reclaims


def _range_stability(range_norms: Sequence[float]) -> float:
    if not range_norms:
        return 0.0
    avg = sum(range_norms) / len(range_norms)
    if avg <= 1e-9:
        return 0.0
    variance = sum((value - avg) ** 2 for value in range_norms) / max(len(range_norms), 1)
    return 1.0 / (1.0 + math.sqrt(variance) / avg)


def _resolved_cluster_count(*, session_count: int, requested_cluster_count: int) -> int:
    if session_count <= 0:
        return 0
    if session_count == 1:
        return 1
    return min(max(2, requested_cluster_count), session_count)


def _date_clustering_risk(*, counts: Counter[str]) -> str:
    if not counts:
        return "HIGH"
    spread = len(counts)
    dominant = _dominant_share(counts)
    if spread <= 1 or dominant >= 0.75:
        return "HIGH"
    if spread <= 3 or dominant >= 0.55:
        return "MEDIUM"
    return "LOW"


def _confidence_level(*, sample_size: int, spread: int, instrument_spread: int, contamination_risk: float) -> str:
    if sample_size >= 16 and spread >= 6 and instrument_spread >= 3 and contamination_risk <= 0.35:
        return "high"
    if sample_size >= 6 and spread >= 3 and instrument_spread >= 2 and contamination_risk <= 0.55:
        return "medium"
    return "low"


def _overfit_warnings(*, sample_size: int, spread: int, instrument_spread: int, contamination_risk: float) -> list[str]:
    warnings: list[str] = []
    if sample_size < 6:
        warnings.append("small_sample")
    if spread <= 2:
        warnings.append("narrow_date_spread")
    if instrument_spread <= 1:
        warnings.append("one_instrument_concentrated")
    if contamination_risk >= 0.50:
        warnings.append("contamination_heavy")
    return warnings


def _interpret_cluster_name(*, rows: Sequence[dict[str, Any]], centroid: dict[str, float]) -> str:
    if float(centroid.get("post_spike_fraction", 0.0)) >= 0.14:
        return "DIRTY_POST_SPIKE_CHOP"
    if float(centroid.get("chop_fraction", 0.0)) >= 0.70 and float(centroid.get("drift_context_fraction", 0.0)) < 0.05:
        return "CHOP_HEAVY_MEAN_REVERTIVE"
    if float(centroid.get("compression_duration_fraction", 0.0)) >= 0.14 and float(centroid.get("expansion_after_compression_ratio", 0.0)) >= 1.20:
        return "COMPRESSION_EXPANSION"
    if float(centroid.get("vwap_reclaim_rate", 0.0)) >= 0.08 and float(centroid.get("signed_vwap_displacement_peak", 0.0)) >= 1.20:
        return "VWAP_RECLAIM_CONTINUATION"
    if float(centroid.get("failed_countertrend_rate", 0.0)) >= 0.18 and float(centroid.get("drift_context_fraction", 0.0)) >= 0.08:
        return "FAILED_MEAN_REVERSION_PRESSURE"
    if float(centroid.get("directional_efficiency_median", 0.0)) >= 0.28 and float(centroid.get("vwap_crossing_rate", 0.0)) < 0.10:
        return "SLOW_GRIND_DRIFT"
    return "EMERGENT_CLUSTER"


def _multi_year_recommendation(
    *,
    cluster_summaries: Sequence[dict[str, Any]],
    candidate_condition_sets: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    viable_conditions = [
        row
        for row in candidate_condition_sets
        if row["sample_size"] >= 8
        and row["positive_hit_rate"] >= 0.20
        and row["date_month_spread"] >= 3
        and row["contamination_risk"] <= 0.55
    ]
    if viable_conditions:
        top = viable_conditions[0]
        queue = [
            {
                "priority": index + 1,
                "branch_type": "condition_set",
                "name": row["condition_set_name"],
                "sample_size": row["sample_size"],
                "positive_hit_rate": row["positive_hit_rate"],
                "date_month_spread": row["date_month_spread"],
                "instrument_spread": row["instrument_spread"],
                "contamination_risk": row["contamination_risk"],
            }
            for index, row in enumerate(viable_conditions[:5])
        ]
        return {
            "recommendation": "proceed_with_conditioned_directional_branch_research",
            "reason": (
                f"{top['condition_set_name']} showed the strongest early-session separation between directional resolution "
                "and dead chop without relying on one narrow date cluster."
            ),
            "next_research_queue": queue,
            "primary_branch": top["condition_set_name"],
        }
    top_cluster = cluster_summaries[0] if cluster_summaries else None
    return {
        "recommendation": "continue_detector_side_condition_research",
        "reason": (
            f"No early condition set cleared the multi-year guardrails. The strongest natural structure was "
            f"{top_cluster['interpretive_name'] if top_cluster else 'unknown'}, which remains too chop-dominated "
            "to justify branch promotion."
        ),
        "next_research_queue": [],
        "primary_branch": None,
    }


def _group_by(rows: Sequence[Any], *, key) -> dict[Any, list[Any]]:
    grouped: dict[Any, list[Any]] = {}
    for row in rows:
        grouped.setdefault(key(row), []).append(row)
    return grouped


def _instrument_family(instrument: str) -> str:
    if instrument.upper() in {"GC", "MGC"}:
        return "metals"
    if instrument.upper() in {"ES", "MES", "NQ", "MNQ"}:
        return "indices"
    return "other"


def _dominant_share(counter: Counter[str]) -> float:
    if not counter:
        return 0.0
    total = sum(counter.values())
    return max(counter.values()) / max(total, 1)


def _median(values: Sequence[float | None]) -> float | None:
    usable = [float(value) for value in values if value is not None]
    if not usable:
        return None
    return float(median(usable))


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _min_str(values: Sequence[str | None]) -> str | None:
    usable = sorted(value for value in values if value is not None)
    return usable[0] if usable else None


def _max_str(values: Sequence[str | None]) -> str | None:
    usable = sorted(value for value in values if value is not None)
    return usable[-1] if usable else None


def _write_multi_year_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    layout = build_layout(output_dir)
    summary_json_path = layout["reports"] / "asia_drift_multi_year_discovery_summary.json"
    summary_markdown_path = layout["reports"] / "asia_drift_multi_year_discovery_summary.md"
    feature_matrix_path = layout["signals"] / "asia_drift_multi_year_feature_matrix.csv"
    cluster_assignments_path = layout["signals"] / "asia_drift_multi_year_cluster_assignments.csv"
    cluster_summary_json_path = layout["reports"] / "asia_drift_multi_year_cluster_summary.json"
    cluster_summary_markdown_path = layout["reports"] / "asia_drift_multi_year_cluster_summary.md"
    early_feature_matrix_path = layout["signals"] / "asia_drift_multi_year_early_feature_matrix.csv"
    positive_control_json_path = layout["reports"] / "asia_drift_multi_year_positive_vs_control.json"
    positive_control_markdown_path = layout["reports"] / "asia_drift_multi_year_positive_vs_control.md"
    discriminator_csv_path = layout["reports"] / "asia_drift_multi_year_discriminator_analysis.csv"
    candidate_condition_sets_json_path = layout["reports"] / "asia_drift_multi_year_candidate_condition_sets.json"
    candidate_condition_sets_csv_path = layout["reports"] / "asia_drift_multi_year_candidate_condition_sets.csv"
    template_comparison_json_path = layout["reports"] / "asia_drift_multi_year_template_vs_cluster.json"
    research_queue_path = layout["reports"] / "asia_drift_multi_year_research_queue.json"
    session_manifest_path = layout["reports"] / "asia_drift_multi_year_session_manifest.json"

    summary_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown_path.write_text(_render_multi_year_markdown(payload), encoding="utf-8")
    _write_csv(feature_matrix_path, payload.get("feature_matrix_rows") or [])
    _write_csv(cluster_assignments_path, payload.get("cluster_assignment_rows") or [])
    cluster_summary_json_path.write_text(json.dumps(payload.get("cluster_summaries") or [], indent=2, sort_keys=True), encoding="utf-8")
    cluster_summary_markdown_path.write_text(_render_cluster_summary_markdown(payload), encoding="utf-8")
    _write_csv(early_feature_matrix_path, payload.get("early_feature_matrix_rows") or [])
    positive_control_json_path.write_text(json.dumps(payload.get("positive_vs_control_comparison") or {}, indent=2, sort_keys=True), encoding="utf-8")
    positive_control_markdown_path.write_text(_render_positive_control_markdown(payload), encoding="utf-8")
    _write_csv(discriminator_csv_path, payload.get("discriminator_analysis") or [])
    candidate_condition_sets_json_path.write_text(json.dumps(payload.get("candidate_condition_sets") or [], indent=2, sort_keys=True), encoding="utf-8")
    _write_csv(candidate_condition_sets_csv_path, payload.get("candidate_condition_sets") or [])
    template_comparison_json_path.write_text(json.dumps(payload.get("template_vs_cluster_comparison") or {}, indent=2, sort_keys=True), encoding="utf-8")
    research_queue_path.write_text(json.dumps(payload.get("recommended_next_research_queue") or [], indent=2, sort_keys=True), encoding="utf-8")
    session_manifest_path.write_text(json.dumps(payload.get("session_manifest") or [], indent=2, sort_keys=True), encoding="utf-8")

    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_multi_year_discovery",
            "layout": {key: str(value) for key, value in layout.items()},
            "artifact_paths": {
                "summary_json_path": str(summary_json_path),
                "summary_markdown_path": str(summary_markdown_path),
                "feature_matrix_path": str(feature_matrix_path),
                "cluster_assignments_path": str(cluster_assignments_path),
                "cluster_summary_json_path": str(cluster_summary_json_path),
                "cluster_summary_markdown_path": str(cluster_summary_markdown_path),
                "early_feature_matrix_path": str(early_feature_matrix_path),
                "positive_control_json_path": str(positive_control_json_path),
                "positive_control_markdown_path": str(positive_control_markdown_path),
                "discriminator_csv_path": str(discriminator_csv_path),
                "candidate_condition_sets_json_path": str(candidate_condition_sets_json_path),
                "candidate_condition_sets_csv_path": str(candidate_condition_sets_csv_path),
                "template_comparison_json_path": str(template_comparison_json_path),
                "research_queue_path": str(research_queue_path),
                "session_manifest_path": str(session_manifest_path),
            },
        },
    )
    return {
        "summary_json_path": str(summary_json_path),
        "summary_markdown_path": str(summary_markdown_path),
        "feature_matrix_path": str(feature_matrix_path),
        "cluster_assignments_path": str(cluster_assignments_path),
        "cluster_summary_json_path": str(cluster_summary_json_path),
        "cluster_summary_markdown_path": str(cluster_summary_markdown_path),
        "early_feature_matrix_path": str(early_feature_matrix_path),
        "positive_control_json_path": str(positive_control_json_path),
        "positive_control_markdown_path": str(positive_control_markdown_path),
        "discriminator_csv_path": str(discriminator_csv_path),
        "candidate_condition_sets_json_path": str(candidate_condition_sets_json_path),
        "candidate_condition_sets_csv_path": str(candidate_condition_sets_csv_path),
        "template_comparison_json_path": str(template_comparison_json_path),
        "research_queue_path": str(research_queue_path),
        "session_manifest_path": str(session_manifest_path),
        "storage_manifest_path": str(layout["storage_manifest"]),
    }


def _render_multi_year_markdown(payload: dict[str, Any]) -> str:
    coverage = payload.get("coverage_report") or {}
    lines = [
        "# Asia Drift Multi-Year 5-Minute Discovery",
        "",
        str(payload.get("objective") or ""),
        "",
        "## Coverage",
        f"- instruments={payload.get('instruments')}",
        f"- coverage_start={coverage.get('coverage_start_ts')}",
        f"- coverage_end={coverage.get('coverage_end_ts')}",
        f"- coverage_rows={coverage.get('coverage_rows')}",
        "",
        "## Natural Clusters",
    ]
    for row in payload.get("cluster_summaries") or []:
        lines.append(
            f"- {row['cluster_id']} {row['interpretive_name']}: sample_size={row['sample_size']} "
            f"continuation_tendency={row['continuation_tendency']} contamination_risk={row['contamination_risk']} "
            f"date_month_spread={row['date_month_spread']} dominant_template={row['dominant_template']}"
        )
    lines.extend(["", "## Positive vs Control"])
    comparison = payload.get("positive_vs_control_comparison") or {}
    lines.append(
        f"- positives={comparison.get('positive_session_count')} matched_controls={comparison.get('matched_control_session_count')} "
        f"positive_month_spread={comparison.get('positive_month_spread')} matched_control_month_spread={comparison.get('matched_control_month_spread')}"
    )
    lines.append(f"- method={comparison.get('method')}")
    lines.extend(["", "## Top Discriminators"])
    for row in (payload.get("discriminator_analysis") or [])[:8]:
        lines.append(
            f"- {row['feature_name']}: effect={row['standardized_effect']} median_delta={row['median_delta']} "
            f"direction={row['discriminator_direction']}"
        )
    lines.extend(["", "## Candidate Condition Sets"])
    for row in (payload.get("candidate_condition_sets") or [])[:8]:
        lines.append(
            f"- {row['condition_set_name']}: sample_size={row['sample_size']} hit_rate={row['positive_hit_rate']} "
            f"false_positive_rate={row['false_positive_rate']} contamination_risk={row['contamination_risk']} "
            f"date_month_spread={row['date_month_spread']}"
        )
    lines.extend(["", "## Recommendation"])
    recommendation = payload.get("recommendation") or {}
    lines.append(f"- recommendation={recommendation.get('recommendation')}")
    lines.append(f"- reason={recommendation.get('reason')}")
    lines.append(f"- next_research_queue={recommendation.get('next_research_queue')}")
    return "\n".join(lines) + "\n"


def _render_cluster_summary_markdown(payload: dict[str, Any]) -> str:
    lines = ["# Cluster Summary", ""]
    for row in payload.get("cluster_summaries") or []:
        lines.append(
            f"- {row['cluster_id']} {row['interpretive_name']}: sample_size={row['sample_size']} "
            f"date_month_spread={row['date_month_spread']} instrument_count={row['instrument_count']} "
            f"continuation_tendency={row['continuation_tendency']} reversal_tendency={row['reversal_tendency']} "
            f"contamination_risk={row['contamination_risk']} confidence={row['confidence_level']}"
        )
    return "\n".join(lines) + "\n"


def _render_positive_control_markdown(payload: dict[str, Any]) -> str:
    comparison = payload.get("positive_vs_control_comparison") or {}
    lines = [
        "# Positive vs Control",
        "",
        f"- positives={comparison.get('positive_session_count')}",
        f"- matched_controls={comparison.get('matched_control_session_count')}",
        f"- method={comparison.get('method')}",
        "",
        "## Sample Matches",
    ]
    for row in (comparison.get("matching_rows") or [])[:10]:
        lines.append(
            f"- positive={row['positive_session_id']} ({row['positive_instrument']} {row['positive_local_session_date']}) "
            f"control={row['control_session_id']} ({row['control_instrument']} {row['control_local_session_date']}) "
            f"distance={row['distance']}"
        )
    return "\n".join(lines) + "\n"


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _coerce_csv_value(row.get(key)) for key in fieldnames})


def _coerce_csv_value(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return "|".join(str(item) for item in value)
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    return value
