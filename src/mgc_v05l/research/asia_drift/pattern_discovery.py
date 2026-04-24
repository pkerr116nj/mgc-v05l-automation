"""Broad Asia/overnight pattern discovery over the replay sample."""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Sequence

from ..trend_participation.models import ResearchBar
from ..trend_participation.storage import build_layout, write_storage_manifest
from .backtest import AsiaDriftPhase2Run, run_asia_drift_phase2, run_asia_drift_phase2_from_bars
from .broader_replay import AsiaDriftReplayBarWindow, AsiaDriftReplayWindow, DEFAULT_WIDER_REPLAY_WINDOWS
from .entries import PREFILL_PROFILE_RECOVERY_CONFIRMED
from .features import RECOVERY_CONFIRMED
from .taxonomy import (
    OPP_CLEAN_DIRECTIONAL_DRIFT,
    OPP_COMPRESSION_THEN_CONTINUATION,
    OPP_FAILED_MEAN_REVERSION,
    OPP_IMPULSE_THEN_PAUSE,
    OPP_LATE_SESSION_DRIFT,
    OPP_NO_DRIFT_CHOP,
    OPP_POST_SPIKE_INSTABILITY,
    OPP_VWAP_REJECTION_CONTINUATION,
    classify_session_taxonomy_for_run,
)


DEFAULT_PATTERN_DISCOVERY_INSTRUMENTS = ("MGC", "GC", "MES", "ES", "MNQ", "NQ")

PATTERN_DIRECTIONAL_SLOW_GRIND = "DIRECTIONAL_SLOW_GRIND"
PATTERN_FAILED_MEAN_REVERSION = "FAILED_MEAN_REVERSION"
PATTERN_VWAP_REJECTION_RECLAIM_CONTINUATION = "VWAP_REJECTION_RECLAIM_CONTINUATION"
PATTERN_COMPRESSION_THEN_EXPANSION = "COMPRESSION_THEN_EXPANSION"
PATTERN_IMPULSE_PAUSE_RESUME = "IMPULSE_PAUSE_RESUME"
PATTERN_LATE_ASIA_CONTINUATION = "LATE_ASIA_CONTINUATION"
PATTERN_POST_SPIKE_FAILURE_OR_REVERSAL = "POST_SPIKE_FAILURE_OR_REVERSAL"
PATTERN_RANGE_BOUND_MEAN_REVERSION = "RANGE_BOUND_MEAN_REVERSION"
PATTERN_VOL_CONTRACTION_DIRECTIONAL_EXPANSION = "VOL_CONTRACTION_BEFORE_DIRECTIONAL_EXPANSION"
PATTERN_CROSS_ASSET_SYNCHRONIZED_DRIFT = "CROSS_ASSET_SYNCHRONIZED_DRIFT"
PATTERN_METALS_INDICES_CONVERGENCE = "METALS_INDICES_CONVERGENCE"
PATTERN_METALS_INDICES_DIVERGENCE = "METALS_INDICES_DIVERGENCE"
PATTERN_OTHER_DIRECTIONAL_CONTINUATION = "OTHER_DIRECTIONAL_CONTINUATION"

PATTERN_DESCRIPTIONS = {
    PATTERN_DIRECTIONAL_SLOW_GRIND: {
        "plain_english": "A controlled overnight grind with directional persistence and little need for a meaningful pullback.",
        "measurable_definition": (
            "Directional continuation with no required pullback, persistent closes, low chop fraction, and moderate directional efficiency."
        ),
    },
    PATTERN_FAILED_MEAN_REVERSION: {
        "plain_english": "Countertrend attempts fail and the original overnight drift resumes.",
        "measurable_definition": (
            "Directional continuation where failed countertrend extension is present before resumed favorable excursion."
        ),
    },
    PATTERN_VWAP_REJECTION_RECLAIM_CONTINUATION: {
        "plain_english": "Overnight drift pauses near VWAP and continuation resumes after VWAP rejection or reclaim.",
        "measurable_definition": (
            "Directional continuation with supportive VWAP interaction, positive signed VWAP displacement, and resumed directional follow-through."
        ),
    },
    PATTERN_COMPRESSION_THEN_EXPANSION: {
        "plain_english": "Directional context compresses and later expands in the drift direction.",
        "measurable_definition": (
            "Directional continuation with measured contraction/overlap features followed by drift-direction expansion."
        ),
    },
    PATTERN_IMPULSE_PAUSE_RESUME: {
        "plain_english": "An early impulse pauses, overlaps, then resumes later in the session.",
        "measurable_definition": (
            "Early drift impulse followed by pause/overlap and later continuation with renewed favorable excursion."
        ),
    },
    PATTERN_LATE_ASIA_CONTINUATION: {
        "plain_english": "Continuation does not resolve until late Asia or the London handoff.",
        "measurable_definition": (
            "Directional continuation whose first meaningful favorable excursion begins in the late Asia / pre-handoff bucket."
        ),
    },
    PATTERN_POST_SPIKE_FAILURE_OR_REVERSAL: {
        "plain_english": "Post-spike overnight behavior dominates the session and continuation quality degrades or reverses.",
        "measurable_definition": (
            "Post-spike context with unstable continuation, elevated contamination risk, or adverse excursion dominating favorable excursion."
        ),
    },
    PATTERN_RANGE_BOUND_MEAN_REVERSION: {
        "plain_english": "The overnight session stays range-bound and mean-reverting instead of trending.",
        "measurable_definition": (
            "No directional continuation, chop-heavy texture, higher reversal frequency, and low signed VWAP displacement."
        ),
    },
    PATTERN_VOL_CONTRACTION_DIRECTIONAL_EXPANSION: {
        "plain_english": "Volatility contracts before the session resumes in one direction.",
        "measurable_definition": (
            "Quiet or normal realized-volatility bucket with later continuation and compression or expansion timing evidence."
        ),
    },
    PATTERN_CROSS_ASSET_SYNCHRONIZED_DRIFT: {
        "plain_english": "Multiple overnight instruments drift in the same direction on the same session date.",
        "measurable_definition": (
            "At least two instruments show directional continuation on the same local session date with aligned direction."
        ),
    },
    PATTERN_METALS_INDICES_CONVERGENCE: {
        "plain_english": "Metals and indices continue in the same direction on the same overnight date.",
        "measurable_definition": (
            "At least one metals session and one index session continue on the same date with aligned direction."
        ),
    },
    PATTERN_METALS_INDICES_DIVERGENCE: {
        "plain_english": "Metals and indices diverge overnight, suggesting cross-family disagreement.",
        "measurable_definition": (
            "At least one metals session and one index session continue on the same date with opposing directions."
        ),
    },
    PATTERN_OTHER_DIRECTIONAL_CONTINUATION: {
        "plain_english": "Directional continuation exists, but it does not fit the cleaner recurring families yet.",
        "measurable_definition": (
            "Directional continuation without strong compression, VWAP, slow-grind, impulse-pause, or failed-mean-reversion structure."
        ),
    },
}


@dataclass(frozen=True)
class _PatternSessionRow:
    row: dict[str, Any]
    feature_snapshot: dict[str, Any]


def run_asia_drift_pattern_discovery(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    instruments: Sequence[str] = DEFAULT_PATTERN_DISCOVERY_INSTRUMENTS,
    windows: Sequence[AsiaDriftReplayWindow] = DEFAULT_WIDER_REPLAY_WINDOWS,
) -> dict[str, Any]:
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]]] = {}
    for instrument in instruments:
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
    payload = _build_pattern_discovery_payload(
        instrument_runs=instrument_runs,
        source_label=str(source_sqlite_path.resolve()),
        window_labels=[window.label for window in windows],
    )
    artifacts = _write_pattern_discovery_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def run_asia_drift_pattern_discovery_from_bars(
    *,
    output_dir: Path,
    instrument_windows: dict[str, Sequence[AsiaDriftReplayBarWindow]],
) -> dict[str, Any]:
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]]] = {}
    for instrument, windows in sorted(instrument_windows.items()):
        converted: list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]] = []
        for window in windows:
            run = run_asia_drift_phase2_from_bars(
                output_dir=output_dir / instrument.lower() / window.label,
                bars_5m=window.bars_5m,
                bars_1m=window.bars_1m,
                source_label=window.label,
                calibration_profile_name=RECOVERY_CONFIRMED,
                refined_prefill_profile_name=PREFILL_PROFILE_RECOVERY_CONFIRMED,
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
    payload = _build_pattern_discovery_payload(
        instrument_runs=instrument_runs,
        source_label="synthetic",
        window_labels=sorted({window.label for windows in instrument_windows.values() for window in windows}),
    )
    artifacts = _write_pattern_discovery_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def _build_pattern_discovery_payload(
    *,
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]]],
    source_label: str,
    window_labels: Sequence[str],
) -> dict[str, Any]:
    session_rows: list[dict[str, Any]] = []
    feature_rows: list[dict[str, Any]] = []
    window_rows: list[dict[str, Any]] = []

    for instrument, window_runs in sorted(instrument_runs.items()):
        for window, run in window_runs:
            taxonomy_rows = classify_session_taxonomy_for_run(instrument=instrument, window=window, run=run)
            discovered_rows = [_discover_session_patterns(row=row) for row in taxonomy_rows]
            session_rows.extend(discovered.row for discovered in discovered_rows)
            feature_rows.extend(discovered.feature_snapshot for discovered in discovered_rows)
            window_rows.append(_window_pattern_summary(instrument=instrument, window=window, session_rows=[row.row for row in discovered_rows]))

    cross_asset_events = _cross_asset_pattern_events(session_rows=session_rows)
    feature_family_summary = _feature_family_summary(session_rows=session_rows)
    pattern_taxonomy = _aggregate_pattern_taxonomy(session_rows=session_rows, cross_asset_events=cross_asset_events)
    top_candidate_patterns = [row for row in pattern_taxonomy if row["status"] == "candidate"][:8]
    rejected_patterns = [row for row in pattern_taxonomy if row["status"] == "deprioritized"][:8]
    recommendation = _pattern_discovery_recommendation(pattern_taxonomy=pattern_taxonomy)
    return {
        "module": "Asia Drift Broad Pattern Discovery",
        "objective": (
            "Research-only broad pattern discovery over Asia/overnight sessions to identify recurring measurable "
            "signal families before committing to branch-specific entry geometry."
        ),
        "source_label": source_label,
        "window_labels": list(window_labels),
        "instruments": sorted(instrument_runs),
        "instrument_families": {instrument: _instrument_family(instrument) for instrument in sorted(instrument_runs)},
        "session_pattern_manifest": session_rows,
        "feature_snapshot_rows": feature_rows,
        "window_summaries": window_rows,
        "cross_asset_pattern_events": cross_asset_events,
        "feature_family_summary": feature_family_summary,
        "candidate_pattern_taxonomy": pattern_taxonomy,
        "top_candidate_patterns": top_candidate_patterns,
        "rejected_or_deprioritized_patterns": rejected_patterns,
        "recommended_next_research_queue": recommendation["next_research_queue"],
        "recommendation": recommendation,
    }


def _discover_session_patterns(*, row: dict[str, Any]) -> _PatternSessionRow:
    pattern_flags: list[str] = []
    if row["directional_continuation"] and row["slow_grind_without_pullback_session"]:
        pattern_flags.append(PATTERN_DIRECTIONAL_SLOW_GRIND)
    if row["directional_continuation"] and row["failed_mean_reversion_session"]:
        pattern_flags.append(PATTERN_FAILED_MEAN_REVERSION)
    if row["directional_continuation"] and row["vwap_involved"]:
        pattern_flags.append(PATTERN_VWAP_REJECTION_RECLAIM_CONTINUATION)
    if row["directional_continuation"] and row["compression_then_expansion_session"]:
        pattern_flags.append(PATTERN_COMPRESSION_THEN_EXPANSION)
    if row["opportunity_type"] == OPP_IMPULSE_THEN_PAUSE:
        pattern_flags.append(PATTERN_IMPULSE_PAUSE_RESUME)
    if row["directional_continuation"] and row["late_continuation_session"]:
        pattern_flags.append(PATTERN_LATE_ASIA_CONTINUATION)
    if row["opportunity_type"] == OPP_POST_SPIKE_INSTABILITY:
        pattern_flags.append(PATTERN_POST_SPIKE_FAILURE_OR_REVERSAL)
    if row["opportunity_type"] == OPP_NO_DRIFT_CHOP and row["session_texture_tag"] == "chop_heavy":
        pattern_flags.append(PATTERN_RANGE_BOUND_MEAN_REVERSION)
    if (
        row["directional_continuation"]
        and row["realized_volatility_bucket"] in {"quiet", "normal"}
        and (
            row["compression_then_expansion_session"]
            or row["best_participation_point_type"] == "COMPRESSION_BREAK"
        )
    ):
        pattern_flags.append(PATTERN_VOL_CONTRACTION_DIRECTIONAL_EXPANSION)
    if row["directional_continuation"] and not pattern_flags:
        pattern_flags.append(PATTERN_OTHER_DIRECTIONAL_CONTINUATION)

    priority = [
        PATTERN_POST_SPIKE_FAILURE_OR_REVERSAL,
        PATTERN_COMPRESSION_THEN_EXPANSION,
        PATTERN_VWAP_REJECTION_RECLAIM_CONTINUATION,
        PATTERN_IMPULSE_PAUSE_RESUME,
        PATTERN_FAILED_MEAN_REVERSION,
        PATTERN_DIRECTIONAL_SLOW_GRIND,
        PATTERN_LATE_ASIA_CONTINUATION,
        PATTERN_VOL_CONTRACTION_DIRECTIONAL_EXPANSION,
        PATTERN_RANGE_BOUND_MEAN_REVERSION,
        PATTERN_OTHER_DIRECTIONAL_CONTINUATION,
    ]
    primary_pattern = next((name for name in priority if name in pattern_flags), PATTERN_RANGE_BOUND_MEAN_REVERSION if not row["directional_continuation"] else PATTERN_OTHER_DIRECTIONAL_CONTINUATION)
    pattern_row = {
        **row,
        "primary_pattern": primary_pattern,
        "pattern_flags": pattern_flags,
        "pattern_flag_count": len(pattern_flags),
        "dirty_regime": row["post_spike_context_tag"] != "clean",
        "likely_signal_candidate": row["directional_continuation"] and primary_pattern not in {PATTERN_POST_SPIKE_FAILURE_OR_REVERSAL, PATTERN_RANGE_BOUND_MEAN_REVERSION},
    }
    feature_snapshot = {
        "instrument": row["instrument"],
        "instrument_family": row["instrument_family"],
        "window_label": row["window_label"],
        "asia_drift_session_id": row["asia_drift_session_id"],
        "local_session_date": row["local_session_date"],
        "candidate_direction": row["candidate_direction"],
        "primary_pattern": primary_pattern,
        "pattern_flags": "|".join(pattern_flags),
        "directional_continuation": row["directional_continuation"],
        "continuation_quality": row["continuation_quality"],
        "max_favorable_excursion_atr": row["max_favorable_excursion_atr"],
        "max_adverse_excursion_atr": row["max_adverse_excursion_atr"],
        "peak_signed_vwap_displacement": row["peak_signed_vwap_displacement"],
        "median_directional_efficiency": row["median_directional_efficiency"],
        "peak_slope_persistence": row["peak_slope_persistence"],
        "peak_close_location_persistence": row["peak_close_location_persistence"],
        "median_reversal_frequency": row["median_reversal_frequency"],
        "median_directional_persistence": row["median_directional_persistence"],
        "realized_volatility_bucket": row["realized_volatility_bucket"],
        "range_expansion_bucket": row["range_expansion_bucket"],
        "session_texture_tag": row["session_texture_tag"],
        "time_of_night_bucket": row["time_of_night_bucket"],
        "post_spike_context_tag": row["post_spike_context_tag"],
    }
    return _PatternSessionRow(row=pattern_row, feature_snapshot=feature_snapshot)


def _window_pattern_summary(
    *,
    instrument: str,
    window: AsiaDriftReplayWindow,
    session_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "instrument": instrument,
        "instrument_family": _instrument_family(instrument),
        "window_label": window.label,
        "start_ts": window.start_ts.isoformat(),
        "end_ts": window.end_ts.isoformat(),
        "session_count": len(session_rows),
        "continuation_session_count": sum(1 for row in session_rows if row["directional_continuation"]),
        "pattern_counts": dict(Counter(row["primary_pattern"] for row in session_rows)),
        "signal_candidate_count": sum(1 for row in session_rows if row["likely_signal_candidate"]),
        "dirty_regime_count": sum(1 for row in session_rows if row["dirty_regime"]),
    }


def _cross_asset_pattern_events(*, session_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in session_rows:
        by_date[row["local_session_date"]].append(row)
    events: list[dict[str, Any]] = []
    for local_date, rows in sorted(by_date.items()):
        continuation_rows = [row for row in rows if row["directional_continuation"] and row["candidate_direction"] in {"LONG", "SHORT"}]
        if len(continuation_rows) < 2:
            continue
        direction_counts = Counter(row["candidate_direction"] for row in continuation_rows)
        dominant_direction, dominant_count = direction_counts.most_common(1)[0]
        if dominant_count >= 2:
            aligned = [row for row in continuation_rows if row["candidate_direction"] == dominant_direction]
            families = {row["instrument_family"] for row in aligned}
            events.append(
                {
                    "event_pattern": PATTERN_CROSS_ASSET_SYNCHRONIZED_DRIFT,
                    "local_session_date": local_date,
                    "instrument_count": len(aligned),
                    "instruments": sorted(row["instrument"] for row in aligned),
                    "instrument_families": sorted(families),
                    "candidate_direction": dominant_direction,
                    "average_favorable_excursion_atr": _median([row["max_favorable_excursion_atr"] for row in aligned]),
                    "average_adverse_excursion_atr": _median([row["max_adverse_excursion_atr"] for row in aligned]),
                    "contamination_risk": _ratio(sum(1 for row in aligned if row["dirty_regime"]), len(aligned)),
                    "time_of_night_bucket": _dominant([row["time_of_night_bucket"] for row in aligned]),
                    "window_label": _dominant([row["window_label"] for row in aligned]),
                }
            )
            if "metals" in families and "indices" in families:
                events.append(
                    {
                        "event_pattern": PATTERN_METALS_INDICES_CONVERGENCE,
                        "local_session_date": local_date,
                        "instrument_count": len(aligned),
                        "instruments": sorted(row["instrument"] for row in aligned),
                        "instrument_families": sorted(families),
                        "candidate_direction": dominant_direction,
                        "average_favorable_excursion_atr": _median([row["max_favorable_excursion_atr"] for row in aligned]),
                        "average_adverse_excursion_atr": _median([row["max_adverse_excursion_atr"] for row in aligned]),
                        "contamination_risk": _ratio(sum(1 for row in aligned if row["dirty_regime"]), len(aligned)),
                        "time_of_night_bucket": _dominant([row["time_of_night_bucket"] for row in aligned]),
                        "window_label": _dominant([row["window_label"] for row in aligned]),
                    }
                )
        metals_dirs = {row["candidate_direction"] for row in continuation_rows if row["instrument_family"] == "metals"}
        index_dirs = {row["candidate_direction"] for row in continuation_rows if row["instrument_family"] == "indices"}
        if metals_dirs and index_dirs and metals_dirs.isdisjoint(index_dirs):
            divergent = [row for row in continuation_rows if row["instrument_family"] in {"metals", "indices"}]
            events.append(
                {
                    "event_pattern": PATTERN_METALS_INDICES_DIVERGENCE,
                    "local_session_date": local_date,
                    "instrument_count": len(divergent),
                    "instruments": sorted(row["instrument"] for row in divergent),
                    "instrument_families": sorted({row["instrument_family"] for row in divergent}),
                    "candidate_direction": "MIXED",
                    "average_favorable_excursion_atr": _median([row["max_favorable_excursion_atr"] for row in divergent]),
                    "average_adverse_excursion_atr": _median([row["max_adverse_excursion_atr"] for row in divergent]),
                    "contamination_risk": _ratio(sum(1 for row in divergent if row["dirty_regime"]), len(divergent)),
                    "time_of_night_bucket": _dominant([row["time_of_night_bucket"] for row in divergent]),
                    "window_label": _dominant([row["window_label"] for row in divergent]),
                }
            )
    return events


def _aggregate_pattern_taxonomy(
    *,
    session_rows: Sequence[dict[str, Any]],
    cross_asset_events: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    pattern_rows: list[dict[str, Any]] = []
    session_pattern_names = sorted({flag for row in session_rows for flag in row["pattern_flags"]})
    for pattern_name in session_pattern_names:
        matched = [row for row in session_rows if pattern_name in row["pattern_flags"]]
        pattern_rows.append(_session_pattern_summary(pattern_name=pattern_name, rows=matched))
    for event_pattern in sorted({row["event_pattern"] for row in cross_asset_events}):
        matched = [row for row in cross_asset_events if row["event_pattern"] == event_pattern]
        pattern_rows.append(_event_pattern_summary(pattern_name=event_pattern, rows=matched))
    pattern_rows.sort(key=lambda row: (-float(row["candidate_score"]), row["pattern_name"]))
    return pattern_rows


def _session_pattern_summary(*, pattern_name: str, rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    sample_size = len(rows)
    window_counts = Counter(row["window_label"] for row in rows)
    instrument_counts = Counter(row["instrument"] for row in rows)
    family_counts = Counter(row["instrument_family"] for row in rows)
    time_counts = Counter(row["time_of_night_bucket"] for row in rows)
    context_counts = Counter(
        (
            row["realized_volatility_bucket"],
            row["session_texture_tag"],
            row["post_spike_context_tag"],
        )
        for row in rows
    )
    continuation_rows = [row for row in rows if row["directional_continuation"]]
    continuation_tendency = _ratio(len(continuation_rows), sample_size)
    contamination_risk = _ratio(sum(1 for row in rows if row["dirty_regime"]), sample_size)
    failure_rate = _ratio(
        sum(
            1
            for row in rows
            if (not row["directional_continuation"]) or row["max_adverse_excursion_atr"] >= max(row["max_favorable_excursion_atr"] * 0.75, 0.75)
        ),
        sample_size,
    )
    window_spread = len(window_counts)
    instrument_spread = len(instrument_counts)
    cluster_share = _dominant_share(window_counts)
    clustering_risk = _clustering_risk(sample_size=sample_size, window_spread=window_spread, dominant_share=cluster_share)
    dirty_regime_dominant = contamination_risk >= 0.55
    confidence = _confidence_level(
        sample_size=sample_size,
        window_spread=window_spread,
        instrument_spread=instrument_spread,
        contamination_risk=contamination_risk,
        clustering_risk=clustering_risk,
    )
    candidate_score = _candidate_score(
        sample_size=sample_size,
        continuation_tendency=continuation_tendency,
        follow_through_median=_median([row["max_favorable_excursion_atr"] for row in continuation_rows]),
        contamination_risk=contamination_risk,
        clustering_risk=clustering_risk,
    )
    status = _pattern_status(
        sample_size=sample_size,
        continuation_tendency=continuation_tendency,
        contamination_risk=contamination_risk,
        clustering_risk=clustering_risk,
        dirty_regime_dominant=dirty_regime_dominant,
    )
    return {
        "pattern_name": pattern_name,
        "entity_type": "session",
        "plain_english_description": PATTERN_DESCRIPTIONS[pattern_name]["plain_english"],
        "measurable_definition": PATTERN_DESCRIPTIONS[pattern_name]["measurable_definition"],
        "sample_size": sample_size,
        "date_window_spread": window_spread,
        "window_labels": sorted(window_counts),
        "instrument_count": instrument_spread,
        "instruments": sorted(instrument_counts),
        "instrument_family_concentration": dict(family_counts),
        "time_of_session_concentration": dict(time_counts),
        "regime_context_dependency": {
            "dominant_contexts": [
                {
                    "realized_volatility_bucket": context[0],
                    "session_texture_tag": context[1],
                    "post_spike_context_tag": context[2],
                    "count": count,
                }
                for context, count in context_counts.most_common(4)
            ]
        },
        "continuation_tendency": continuation_tendency,
        "follow_through_median_atr": _median([row["max_favorable_excursion_atr"] for row in continuation_rows]),
        "failure_rate": failure_rate,
        "contamination_risk": contamination_risk,
        "clustering_risk": clustering_risk,
        "clustered_in_one_window_only": window_spread == 1,
        "mostly_dirty_regime_behavior": dirty_regime_dominant,
        "survives_across_instruments": instrument_spread >= 2,
        "survives_across_families": len(family_counts) >= 2,
        "confidence_level": confidence,
        "candidate_score": candidate_score,
        "status": status,
        "conditions_where_it_fails": _failure_conditions(rows=rows),
        "recommended_next_research_branch": _branch_recommendation(pattern_name=pattern_name, status=status),
    }


def _event_pattern_summary(*, pattern_name: str, rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    sample_size = len(rows)
    window_counts = Counter(row["window_label"] for row in rows)
    family_counts = Counter(family for row in rows for family in row["instrument_families"])
    instrument_counts = Counter(instrument for row in rows for instrument in row["instruments"])
    contamination_risk = _median([row["contamination_risk"] for row in rows])
    continuation_tendency = 1.0
    failure_rate = _ratio(
        sum(1 for row in rows if row["average_adverse_excursion_atr"] >= max(row["average_favorable_excursion_atr"] * 0.75, 0.75)),
        sample_size,
    )
    clustering_risk = _clustering_risk(sample_size=sample_size, window_spread=len(window_counts), dominant_share=_dominant_share(window_counts))
    confidence = _confidence_level(
        sample_size=sample_size,
        window_spread=len(window_counts),
        instrument_spread=len(instrument_counts),
        contamination_risk=contamination_risk,
        clustering_risk=clustering_risk,
    )
    candidate_score = _candidate_score(
        sample_size=sample_size,
        continuation_tendency=continuation_tendency,
        follow_through_median=_median([row["average_favorable_excursion_atr"] for row in rows]),
        contamination_risk=contamination_risk,
        clustering_risk=clustering_risk,
    )
    status = _pattern_status(
        sample_size=sample_size,
        continuation_tendency=continuation_tendency,
        contamination_risk=contamination_risk,
        clustering_risk=clustering_risk,
        dirty_regime_dominant=contamination_risk >= 0.55,
    )
    return {
        "pattern_name": pattern_name,
        "entity_type": "cross_asset_date",
        "plain_english_description": PATTERN_DESCRIPTIONS[pattern_name]["plain_english"],
        "measurable_definition": PATTERN_DESCRIPTIONS[pattern_name]["measurable_definition"],
        "sample_size": sample_size,
        "date_window_spread": len(window_counts),
        "window_labels": sorted(window_counts),
        "instrument_count": len(instrument_counts),
        "instruments": sorted(instrument_counts),
        "instrument_family_concentration": dict(family_counts),
        "time_of_session_concentration": dict(Counter(row["time_of_night_bucket"] for row in rows)),
        "regime_context_dependency": {
            "dominant_contexts": [
                {"window_label": label, "count": count}
                for label, count in window_counts.most_common(4)
            ]
        },
        "continuation_tendency": continuation_tendency,
        "follow_through_median_atr": _median([row["average_favorable_excursion_atr"] for row in rows]),
        "failure_rate": failure_rate,
        "contamination_risk": contamination_risk,
        "clustering_risk": clustering_risk,
        "clustered_in_one_window_only": len(window_counts) == 1,
        "mostly_dirty_regime_behavior": contamination_risk >= 0.55,
        "survives_across_instruments": len(instrument_counts) >= 2,
        "survives_across_families": len(family_counts) >= 2,
        "confidence_level": confidence,
        "candidate_score": candidate_score,
        "status": status,
        "conditions_where_it_fails": (
            "Often degrades when aligned instruments are contaminated by post-spike or adverse extension."
            if contamination_risk > 0.0
            else "Failure modes are mainly clustering and low sample count."
        ),
        "recommended_next_research_branch": _branch_recommendation(pattern_name=pattern_name, status=status),
    }


def _feature_family_summary(*, session_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    families = sorted({row["instrument_family"] for row in session_rows})
    return {
        family: {
            "session_count": len(rows),
            "directional_continuation_rate": _ratio(sum(1 for row in rows if row["directional_continuation"]), len(rows)),
            "peak_signed_vwap_displacement_median": _median([row["peak_signed_vwap_displacement"] for row in rows]),
            "directional_efficiency_median": _median([row["median_directional_efficiency"] for row in rows]),
            "slope_persistence_median": _median([row["peak_slope_persistence"] for row in rows]),
            "close_location_persistence_median": _median([row["peak_close_location_persistence"] for row in rows]),
            "reversal_frequency_median": _median([row["median_reversal_frequency"] for row in rows]),
            "time_of_night_breakdown": dict(Counter(row["time_of_night_bucket"] for row in rows)),
            "volatility_bucket_breakdown": dict(Counter(row["realized_volatility_bucket"] for row in rows)),
            "pattern_breakdown": dict(Counter(row["primary_pattern"] for row in rows)),
        }
        for family in families
        for rows in ([row for row in session_rows if row["instrument_family"] == family],)
    }


def _pattern_discovery_recommendation(*, pattern_taxonomy: Sequence[dict[str, Any]]) -> dict[str, Any]:
    candidates = [row for row in pattern_taxonomy if row["status"] == "candidate"]
    queue = [
        {
            "priority": index + 1,
            "pattern_name": row["pattern_name"],
            "entity_type": row["entity_type"],
            "confidence_level": row["confidence_level"],
            "sample_size": row["sample_size"],
            "date_window_spread": row["date_window_spread"],
            "instrument_count": row["instrument_count"],
            "contamination_risk": row["contamination_risk"],
            "clustering_risk": row["clustering_risk"],
            "recommended_next_research_branch": row["recommended_next_research_branch"],
        }
        for index, row in enumerate(candidates[:6])
    ]
    top = candidates[0] if candidates else None
    if top is None:
        return {
            "recommendation": "pause_and_rethink_mechanism",
            "reason": "No broad overnight pattern cleared the sample-size, spread, and contamination guardrails.",
            "next_research_queue": [],
        }
    if top["pattern_name"] == PATTERN_COMPRESSION_THEN_EXPANSION:
        recommendation = "proceed_with_compression_then_expansion_branch"
    elif top["pattern_name"] == PATTERN_VWAP_REJECTION_RECLAIM_CONTINUATION:
        recommendation = "proceed_with_vwap_rejection_reclaim_branch"
    elif top["pattern_name"] == PATTERN_CROSS_ASSET_SYNCHRONIZED_DRIFT:
        recommendation = "research_cross_asset_sync_drift_branch"
    else:
        recommendation = "proceed_with_broad_pattern_followup"
    return {
        "recommendation": recommendation,
        "reason": (
            f"{top['pattern_name']} had the strongest blend of sample size, measurable structure, and cross-window spread "
            "without relying entirely on one dirty regime."
        ),
        "next_research_queue": queue,
    }


def _pattern_status(
    *,
    sample_size: int,
    continuation_tendency: float,
    contamination_risk: float,
    clustering_risk: str,
    dirty_regime_dominant: bool,
) -> str:
    if sample_size < 2 or continuation_tendency < 0.20 or dirty_regime_dominant:
        return "deprioritized"
    if contamination_risk > 0.60 or clustering_risk == "HIGH" and sample_size < 4:
        return "deprioritized"
    return "candidate"


def _branch_recommendation(*, pattern_name: str, status: str) -> str | None:
    if status != "candidate":
        return None
    if pattern_name == PATTERN_COMPRESSION_THEN_EXPANSION:
        return "compression_then_continuation_detector_branch"
    if pattern_name == PATTERN_VWAP_REJECTION_RECLAIM_CONTINUATION:
        return "vwap_rejection_reclaim_detector_branch"
    if pattern_name == PATTERN_CROSS_ASSET_SYNCHRONIZED_DRIFT:
        return "cross_asset_overnight_sync_research"
    if pattern_name == PATTERN_DIRECTIONAL_SLOW_GRIND:
        return "slow_grind_context_branch"
    if pattern_name == PATTERN_FAILED_MEAN_REVERSION:
        return "failed_mean_reversion_branch"
    return "followup_detector_research"


def _failure_conditions(*, rows: Sequence[dict[str, Any]]) -> str:
    if not rows:
        return "Insufficient evidence."
    post_spike_rate = _ratio(sum(1 for row in rows if row["post_spike_context_tag"] != "clean"), len(rows))
    chop_rate = _ratio(sum(1 for row in rows if row["session_texture_tag"] == "chop_heavy"), len(rows))
    if post_spike_rate >= 0.50:
        return "Often degrades in post-spike or otherwise dirty overnight conditions."
    if chop_rate >= 0.50:
        return "Often dissolves into chop-heavy overnight sessions without clear continuation."
    return "Failures are mainly non-continuation or adverse excursion dominating favorable follow-through."


def _candidate_score(
    *,
    sample_size: int,
    continuation_tendency: float,
    follow_through_median: float | None,
    contamination_risk: float,
    clustering_risk: str,
) -> float:
    cluster_penalty = {"LOW": 0.0, "MEDIUM": 0.35, "HIGH": 0.75}[clustering_risk]
    return (
        1.5 * continuation_tendency
        + 0.18 * sample_size
        + 0.7 * float(follow_through_median or 0.0)
        - 1.1 * contamination_risk
        - cluster_penalty
    )


def _confidence_level(
    *,
    sample_size: int,
    window_spread: int,
    instrument_spread: int,
    contamination_risk: float,
    clustering_risk: str,
) -> str:
    if sample_size >= 8 and window_spread >= 3 and instrument_spread >= 3 and contamination_risk <= 0.35 and clustering_risk == "LOW":
        return "high"
    if sample_size >= 4 and window_spread >= 2 and instrument_spread >= 2 and contamination_risk <= 0.55:
        return "medium"
    return "low"


def _clustering_risk(*, sample_size: int, window_spread: int, dominant_share: float) -> str:
    if sample_size <= 1 or window_spread <= 1 or dominant_share >= 0.75:
        return "HIGH"
    if window_spread <= 2 or dominant_share >= 0.55:
        return "MEDIUM"
    return "LOW"


def _instrument_family(instrument: str) -> str:
    if instrument.upper() in {"GC", "MGC"}:
        return "metals"
    if instrument.upper() in {"ES", "MES", "NQ", "MNQ"}:
        return "indices"
    return "other"


def _dominant(values: Sequence[str]) -> str | None:
    if not values:
        return None
    return Counter(values).most_common(1)[0][0]


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


def _write_pattern_discovery_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    layout = build_layout(output_dir)
    summary_json_path = layout["reports"] / "asia_drift_pattern_discovery_summary.json"
    summary_markdown_path = layout["reports"] / "asia_drift_pattern_discovery_summary.md"
    session_manifest_path = layout["reports"] / "asia_drift_pattern_discovery_sessions.json"
    session_rows_path = layout["signals"] / "asia_drift_pattern_discovery_sessions.csv"
    feature_rows_path = layout["signals"] / "asia_drift_pattern_discovery_feature_snapshots.csv"
    top_patterns_path = layout["reports"] / "asia_drift_pattern_discovery_top_patterns.csv"
    rejected_patterns_path = layout["reports"] / "asia_drift_pattern_discovery_rejected_patterns.csv"
    research_queue_path = layout["reports"] / "asia_drift_pattern_discovery_research_queue.json"
    cross_asset_events_path = layout["reports"] / "asia_drift_pattern_discovery_cross_asset_events.csv"
    window_rows_path = layout["reports"] / "asia_drift_pattern_discovery_window_rows.csv"

    summary_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown_path.write_text(_render_pattern_discovery_markdown(payload), encoding="utf-8")
    session_manifest_path.write_text(json.dumps(payload.get("session_pattern_manifest") or [], indent=2, sort_keys=True), encoding="utf-8")
    _write_csv(session_rows_path, payload.get("session_pattern_manifest") or [])
    _write_csv(feature_rows_path, payload.get("feature_snapshot_rows") or [])
    _write_csv(top_patterns_path, payload.get("top_candidate_patterns") or [])
    _write_csv(rejected_patterns_path, payload.get("rejected_or_deprioritized_patterns") or [])
    research_queue_path.write_text(json.dumps(payload.get("recommended_next_research_queue") or [], indent=2, sort_keys=True), encoding="utf-8")
    _write_csv(cross_asset_events_path, payload.get("cross_asset_pattern_events") or [])
    _write_csv(window_rows_path, payload.get("window_summaries") or [])

    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_pattern_discovery",
            "layout": {key: str(value) for key, value in layout.items()},
            "artifact_paths": {
                "summary_json_path": str(summary_json_path),
                "summary_markdown_path": str(summary_markdown_path),
                "session_manifest_path": str(session_manifest_path),
                "session_rows_path": str(session_rows_path),
                "feature_rows_path": str(feature_rows_path),
                "top_patterns_path": str(top_patterns_path),
                "rejected_patterns_path": str(rejected_patterns_path),
                "research_queue_path": str(research_queue_path),
                "cross_asset_events_path": str(cross_asset_events_path),
                "window_rows_path": str(window_rows_path),
            },
        },
    )
    return {
        "summary_json_path": str(summary_json_path),
        "summary_markdown_path": str(summary_markdown_path),
        "session_manifest_path": str(session_manifest_path),
        "session_rows_path": str(session_rows_path),
        "feature_rows_path": str(feature_rows_path),
        "top_patterns_path": str(top_patterns_path),
        "rejected_patterns_path": str(rejected_patterns_path),
        "research_queue_path": str(research_queue_path),
        "cross_asset_events_path": str(cross_asset_events_path),
        "window_rows_path": str(window_rows_path),
        "storage_manifest_path": str(layout["storage_manifest"]),
    }


def _render_pattern_discovery_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia Drift Broad Pattern Discovery",
        "",
        str(payload.get("objective") or ""),
        "",
        "## Coverage",
        f"- windows={payload.get('window_labels')}",
        f"- instruments={payload.get('instruments')}",
        "",
        "## Top Candidate Patterns",
    ]
    for row in payload.get("top_candidate_patterns") or []:
        lines.append(
            f"- {row['pattern_name']}: sample_size={row['sample_size']} confidence={row['confidence_level']} "
            f"continuation_tendency={row['continuation_tendency']} contamination_risk={row['contamination_risk']} "
            f"clustering_risk={row['clustering_risk']} next_branch={row['recommended_next_research_branch']}"
        )
    lines.extend(["", "## Deprioritized Patterns"])
    for row in payload.get("rejected_or_deprioritized_patterns") or []:
        lines.append(
            f"- {row['pattern_name']}: sample_size={row['sample_size']} contamination_risk={row['contamination_risk']} "
            f"clustering_risk={row['clustering_risk']} failure_rate={row['failure_rate']}"
        )
    lines.extend(["", "## Family Feature Summary"])
    for family, row in sorted((payload.get("feature_family_summary") or {}).items()):
        lines.append(
            f"- {family}: session_count={row['session_count']} continuation_rate={row['directional_continuation_rate']} "
            f"vwap_disp={row['peak_signed_vwap_displacement_median']} efficiency={row['directional_efficiency_median']} "
            f"slope={row['slope_persistence_median']} reversal={row['reversal_frequency_median']}"
        )
    lines.extend(["", "## Recommendation"])
    recommendation = payload.get("recommendation") or {}
    lines.append(f"- recommendation: {recommendation.get('recommendation')}")
    lines.append(f"- reason: {recommendation.get('reason')}")
    lines.append(f"- next_research_queue: {recommendation.get('next_research_queue')}")
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
