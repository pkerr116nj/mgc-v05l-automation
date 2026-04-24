"""Open feature-cluster discovery for Asia/overnight sessions."""

from __future__ import annotations

import csv
import json
import math
import random
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from typing import Any, Sequence

from ..trend_participation.models import ResearchBar
from ..trend_participation.storage import build_layout, write_storage_manifest
from .backtest import AsiaDriftPhase2Run, run_asia_drift_phase2, run_asia_drift_phase2_from_bars
from .broader_replay import AsiaDriftReplayBarWindow, AsiaDriftReplayWindow, DEFAULT_WIDER_REPLAY_WINDOWS
from .entries import PREFILL_PROFILE_RECOVERY_CONFIRMED
from .features import ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT, RECOVERY_CONFIRMED
from .taxonomy import classify_session_taxonomy_for_run


DEFAULT_OPEN_CLUSTER_INSTRUMENTS = ("MGC", "GC", "MES", "ES", "MNQ", "NQ")
DEFAULT_OPEN_CLUSTER_COUNT = 5

CLUSTER_INPUT_FEATURES = (
    "abs_session_displacement_atr",
    "max_favorable_excursion_atr",
    "max_adverse_excursion_atr",
    "directional_efficiency_median",
    "close_location_persistence_peak",
    "signed_vwap_displacement_peak",
    "vwap_crossing_rate",
    "vwap_reclaim_rate",
    "compression_duration_fraction",
    "expansion_after_compression_ratio",
    "overlap_ratio_median",
    "reversal_frequency_median",
    "first_impulse_index_norm",
    "largest_expansion_index_norm",
    "realized_volatility_ratio_median",
    "post_spike_fraction",
    "failed_countertrend_rate",
    "chop_fraction",
    "drift_context_fraction",
)


def run_asia_drift_open_cluster_discovery(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    instruments: Sequence[str] = DEFAULT_OPEN_CLUSTER_INSTRUMENTS,
    windows: Sequence[AsiaDriftReplayWindow] = DEFAULT_WIDER_REPLAY_WINDOWS,
    cluster_count: int = DEFAULT_OPEN_CLUSTER_COUNT,
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
    payload = _build_open_cluster_payload(
        instrument_runs=instrument_runs,
        source_label=str(source_sqlite_path.resolve()),
        window_labels=[window.label for window in windows],
        requested_cluster_count=cluster_count,
    )
    artifacts = _write_open_cluster_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def run_asia_drift_open_cluster_discovery_from_bars(
    *,
    output_dir: Path,
    instrument_windows: dict[str, Sequence[AsiaDriftReplayBarWindow]],
    cluster_count: int = DEFAULT_OPEN_CLUSTER_COUNT,
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
    payload = _build_open_cluster_payload(
        instrument_runs=instrument_runs,
        source_label="synthetic",
        window_labels=sorted({window.label for windows in instrument_windows.values() for window in windows}),
        requested_cluster_count=cluster_count,
    )
    artifacts = _write_open_cluster_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def _build_open_cluster_payload(
    *,
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]]],
    source_label: str,
    window_labels: Sequence[str],
    requested_cluster_count: int,
) -> dict[str, Any]:
    feature_rows: list[dict[str, Any]] = []
    for instrument, window_runs in sorted(instrument_runs.items()):
        for window, run in window_runs:
            taxonomy_rows = {
                row["asia_drift_session_id"]: row
                for row in classify_session_taxonomy_for_run(instrument=instrument, window=window, run=run)
            }
            features_by_session = _group_by(run.phase1_run.feature_rows, key=lambda row: row.asia_drift_session_id)
            summaries_by_session = {row.asia_drift_session_id: row for row in run.phase1_run.session_summaries}
            for session_id, summary in sorted(summaries_by_session.items()):
                per_bar_rows = sorted(features_by_session.get(session_id, []), key=lambda row: row.decision_ts)
                feature_row = _session_feature_row(
                    instrument=instrument,
                    window=window,
                    summary=summary,
                    taxonomy_row=taxonomy_rows.get(session_id),
                    feature_rows=per_bar_rows,
                )
                if feature_row is not None:
                    feature_rows.append(feature_row)

    _augment_cross_asset_alignment(feature_rows)
    cluster_count = _resolved_cluster_count(session_count=len(feature_rows), requested_cluster_count=requested_cluster_count)
    assignments, centroids, scaling = _cluster_feature_rows(feature_rows=feature_rows, cluster_count=cluster_count)
    cluster_rows = _cluster_assignment_rows(feature_rows=feature_rows, assignments=assignments)
    cluster_summaries = _cluster_summaries(cluster_rows=cluster_rows, centroids=centroids, scaling=scaling)
    _attach_cluster_summary_fields(cluster_rows=cluster_rows, cluster_summaries=cluster_summaries)
    template_comparison = _template_vs_cluster_comparison(cluster_rows=cluster_rows)
    recommendation = _open_cluster_recommendation(cluster_summaries=cluster_summaries)
    return {
        "module": "Asia Drift Open Feature Cluster Discovery",
        "objective": (
            "Research-only open cluster discovery over Asia/overnight sessions using measured session features first "
            "and template interpretation only after natural groupings are formed."
        ),
        "source_label": source_label,
        "window_labels": list(window_labels),
        "instruments": sorted(instrument_runs),
        "requested_cluster_count": requested_cluster_count,
        "actual_cluster_count": cluster_count,
        "cluster_input_features": list(CLUSTER_INPUT_FEATURES),
        "sample_coverage": _sample_coverage(
            feature_rows=feature_rows,
            source_label=source_label,
            window_labels=window_labels,
        ),
        "feature_matrix_rows": feature_rows,
        "cluster_assignment_rows": cluster_rows,
        "cluster_summaries": cluster_summaries,
        "template_vs_cluster_comparison": template_comparison,
        "recommended_next_research_queue": recommendation["next_research_queue"],
        "recommendation": recommendation,
    }


def _session_feature_row(
    *,
    instrument: str,
    window: AsiaDriftReplayWindow,
    summary: Any,
    taxonomy_row: dict[str, Any] | None,
    feature_rows: Sequence[Any],
) -> dict[str, Any] | None:
    rows = [row for row in feature_rows if row.in_scope]
    if not rows:
        rows = list(feature_rows)
    if not rows:
        return None
    direction = summary.candidate_direction if summary.candidate_direction in {"LONG", "SHORT"} else rows[0].dominant_direction
    first = rows[0]
    last = rows[-1]
    session_open = first.session_open
    atr_base = max(first.atr, 1e-9)
    signed_displacement = ((last.close - session_open) / atr_base) if direction == "LONG" else ((session_open - last.close) / atr_base)
    max_favorable = float(taxonomy_row.get("max_favorable_excursion_atr") if taxonomy_row else 0.0)
    max_adverse = float(taxonomy_row.get("max_adverse_excursion_atr") if taxonomy_row else 0.0)
    range_norms = [row.range_points / max(row.atr, 1e-9) for row in rows]
    overlap_values = [row.bar_overlap_ratio_8 for row in rows]
    compression_run = _longest_compression_run(rows)
    expansion_after_compression = _expansion_after_compression(rows, compression_run)
    vwap_cross_count, vwap_reclaim_count = _vwap_cross_metrics(rows, direction=direction)
    first_impulse_index = next((index for index, row in enumerate(rows) if row.fresh_drift_impulse), None)
    largest_expansion_index = max(range(len(rows)), key=lambda index: range_norms[index]) if rows else None
    taxonomy_type = taxonomy_row.get("opportunity_type") if taxonomy_row else None
    return {
        "instrument": instrument,
        "instrument_family": _instrument_family(instrument),
        "window_label": window.label,
        "asia_drift_session_id": summary.asia_drift_session_id,
        "local_session_date": summary.local_session_date.isoformat(),
        "candidate_direction": direction,
        "template_opportunity_type": taxonomy_type,
        "directional_continuation": bool(taxonomy_row.get("directional_continuation")) if taxonomy_row else False,
        "continuation_quality": taxonomy_row.get("continuation_quality") if taxonomy_row else "NONE",
        "abs_session_displacement_atr": abs(signed_displacement),
        "max_favorable_excursion_atr": max_favorable,
        "max_adverse_excursion_atr": max_adverse,
        "directional_efficiency_median": _median([row.efficiency_ratio_12 for row in rows]) or 0.0,
        "close_location_persistence_peak": _peak_aligned(rows, direction=direction, long_attr="close_location_persistence_long", short_attr="close_location_persistence_short"),
        "signed_vwap_displacement_peak": _peak_aligned(rows, direction=direction, long_attr="signed_vwap_displacement_long", short_attr="signed_vwap_displacement_short"),
        "vwap_crossing_rate": vwap_cross_count / max(len(rows), 1),
        "vwap_reclaim_rate": vwap_reclaim_count / max(len(rows), 1),
        "compression_duration_fraction": compression_run["length"] / max(len(rows), 1),
        "expansion_after_compression_ratio": expansion_after_compression,
        "overlap_ratio_median": _median(overlap_values) or 0.0,
        "reversal_frequency_median": _median([row.reversal_frequency_12 for row in rows]) or 0.0,
        "first_impulse_index_norm": ((first_impulse_index or len(rows) - 1) / max(len(rows) - 1, 1)),
        "largest_expansion_index_norm": ((largest_expansion_index or len(rows) - 1) / max(len(rows) - 1, 1)),
        "realized_volatility_ratio_median": _median([row.realized_volatility_ratio for row in rows]) or 0.0,
        "post_spike_fraction": _ratio(sum(1 for row in rows if row.post_spike_instability), len(rows)),
        "failed_countertrend_rate": _ratio(sum(1 for row in rows if row.countertrend_extension_failed), len(rows)),
        "chop_fraction": _ratio(sum(1 for row in rows if row.chop_veto), len(rows)),
        "drift_context_fraction": _ratio(sum(1 for row in rows if row.regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}), len(rows)),
        "range_norm_median": _median(range_norms) or 0.0,
        "range_norm_peak": max(range_norms) if range_norms else 0.0,
        "time_of_first_impulse_subphase": rows[first_impulse_index].subphase if first_impulse_index is not None else last.subphase,
        "time_of_largest_expansion_subphase": rows[largest_expansion_index].subphase if largest_expansion_index is not None else last.subphase,
        "cross_asset_sync_count": 0,
        "cross_asset_family_alignment": "NONE",
    }


def _augment_cross_asset_alignment(feature_rows: list[dict[str, Any]]) -> None:
    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in feature_rows:
        by_date[row["local_session_date"]].append(row)
    for rows in by_date.values():
        continuation_rows = [row for row in rows if row["directional_continuation"] and row["candidate_direction"] in {"LONG", "SHORT"}]
        direction_groups = defaultdict(list)
        for row in continuation_rows:
            direction_groups[row["candidate_direction"]].append(row)
        for direction, aligned_rows in direction_groups.items():
            families = {row["instrument_family"] for row in aligned_rows}
            alignment = "CROSS_FAMILY_SYNC" if len(families) >= 2 else "SINGLE_FAMILY_SYNC"
            for row in aligned_rows:
                row["cross_asset_sync_count"] = max(row["cross_asset_sync_count"], len(aligned_rows))
                row["cross_asset_family_alignment"] = alignment
        metals_dirs = {row["candidate_direction"] for row in continuation_rows if row["instrument_family"] == "metals"}
        index_dirs = {row["candidate_direction"] for row in continuation_rows if row["instrument_family"] == "indices"}
        if metals_dirs and index_dirs and metals_dirs.isdisjoint(index_dirs):
            for row in continuation_rows:
                if row["instrument_family"] in {"metals", "indices"}:
                    row["cross_asset_family_alignment"] = "FAMILY_DIVERGENCE"


def _cluster_feature_rows(
    *,
    feature_rows: Sequence[dict[str, Any]],
    cluster_count: int,
) -> tuple[list[int], list[list[float]], dict[str, dict[str, float]]]:
    if not feature_rows or cluster_count <= 0:
        return [], [], {}
    matrix = [[float(row[name]) for name in CLUSTER_INPUT_FEATURES] for row in feature_rows]
    scaling = _fit_standard_scaler(matrix)
    scaled = [_apply_standard_scaler(row, scaling) for row in matrix]
    centroids = _kmeans_plus_plus_init(scaled, cluster_count=cluster_count, seed=0)
    assignments = [0 for _ in scaled]
    for _ in range(25):
        changed = False
        for index, row in enumerate(scaled):
            best_cluster = min(range(len(centroids)), key=lambda cluster: _euclidean_sq(row, centroids[cluster]))
            if assignments[index] != best_cluster:
                assignments[index] = best_cluster
                changed = True
        new_centroids: list[list[float]] = []
        for cluster in range(len(centroids)):
            members = [scaled[index] for index, assigned in enumerate(assignments) if assigned == cluster]
            if not members:
                new_centroids.append(list(centroids[cluster]))
            else:
                new_centroids.append([sum(values) / len(values) for values in zip(*members)])
        centroids = new_centroids
        if not changed:
            break
    return assignments, centroids, scaling


def _cluster_assignment_rows(
    *,
    feature_rows: Sequence[dict[str, Any]],
    assignments: Sequence[int],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row, cluster_id in zip(feature_rows, assignments):
        rows.append({**row, "cluster_id": f"cluster_{cluster_id}"})
    return rows


def _attach_cluster_summary_fields(
    *,
    cluster_rows: list[dict[str, Any]],
    cluster_summaries: Sequence[dict[str, Any]],
) -> None:
    by_cluster = {row["cluster_id"]: row for row in cluster_summaries}
    for row in cluster_rows:
        summary = by_cluster.get(row["cluster_id"])
        if summary is None:
            continue
        row["cluster_interpretive_name"] = summary["interpretive_name"]
        row["cluster_status"] = summary["status"]
        row["cluster_confidence_level"] = summary["confidence_level"]
        row["cluster_follow_through_tendency"] = summary["follow_through_tendency"]
        row["cluster_contamination_risk"] = summary["contamination_risk"]


def _cluster_summaries(
    *,
    cluster_rows: Sequence[dict[str, Any]],
    centroids: Sequence[Sequence[float]],
    scaling: dict[str, dict[str, float]],
) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for cluster_name in sorted({row["cluster_id"] for row in cluster_rows}):
        rows = [row for row in cluster_rows if row["cluster_id"] == cluster_name]
        template_counts = Counter(row["template_opportunity_type"] for row in rows)
        instrument_counts = Counter(row["instrument"] for row in rows)
        family_counts = Counter(row["instrument_family"] for row in rows)
        window_counts = Counter(row["window_label"] for row in rows)
        continuation_rate = _ratio(sum(1 for row in rows if row["directional_continuation"]), len(rows))
        contamination_risk = _ratio(sum(1 for row in rows if row["post_spike_fraction"] >= 0.10), len(rows))
        cluster_index = int(cluster_name.split("_")[-1])
        raw_centroid = _invert_standard_scaler(list(centroids[cluster_index]), scaling)
        interpretive_name = _interpret_cluster_name(rows=rows, centroid=raw_centroid)
        follow_through_median = _median([row["max_favorable_excursion_atr"] for row in rows if row["directional_continuation"]])
        reversal_rate = _ratio(
            sum(
                1
                for row in rows
                if row["max_adverse_excursion_atr"] >= max(row["max_favorable_excursion_atr"] * 0.75, 0.75)
            ),
            len(rows),
        )
        clustering_risk = _clustering_risk(
            sample_size=len(rows),
            window_spread=len(window_counts),
            dominant_share=_dominant_share(window_counts),
        )
        confidence = _confidence_level(
            sample_size=len(rows),
            window_spread=len(window_counts),
            instrument_spread=len(instrument_counts),
            contamination_risk=contamination_risk,
            clustering_risk=clustering_risk,
        )
        candidate_score = _candidate_score(
            sample_size=len(rows),
            continuation_tendency=continuation_rate,
            follow_through_median=follow_through_median,
            contamination_risk=contamination_risk,
            clustering_risk=clustering_risk,
        )
        status = _pattern_status(
            sample_size=len(rows),
            continuation_tendency=continuation_rate,
            contamination_risk=contamination_risk,
            clustering_risk=clustering_risk,
            dirty_regime_dominant=contamination_risk >= 0.55,
        )
        summaries.append(
            {
                "cluster_id": cluster_name,
                "interpretive_name": interpretive_name,
                "sample_size": len(rows),
                "date_window_spread": len(window_counts),
                "instrument_count": len(instrument_counts),
                "instrument_family_concentration": dict(family_counts),
                "window_labels": sorted(window_counts),
                "template_distribution": dict(template_counts),
                "dominant_template": template_counts.most_common(1)[0][0] if template_counts else None,
                "dominant_template_share": _dominant_share(template_counts),
                "follow_through_tendency": continuation_rate,
                "follow_through_median_atr": follow_through_median,
                "reversal_rate": reversal_rate,
                "contamination_risk": contamination_risk,
                "clustering_risk": clustering_risk,
                "confidence_level": confidence,
                "candidate_score": candidate_score,
                "status": status,
                "suitable_for_further_branch_research": status == "candidate",
                "rough_mfe_median_atr": _median([row["max_favorable_excursion_atr"] for row in rows]),
                "rough_mae_median_atr": _median([row["max_adverse_excursion_atr"] for row in rows]),
                "feature_centroid": raw_centroid,
                "overfit_warnings": _overfit_warnings(
                    sample_size=len(rows),
                    window_spread=len(window_counts),
                    instrument_spread=len(instrument_counts),
                    contamination_risk=contamination_risk,
                ),
            }
        )
    summaries.sort(key=lambda row: (-float(row["candidate_score"]), row["cluster_id"]))
    return summaries


def _template_vs_cluster_comparison(*, cluster_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    cluster_to_template: dict[str, dict[str, Any]] = {}
    template_to_clusters: dict[str, dict[str, Any]] = {}
    for cluster_name in sorted({row["cluster_id"] for row in cluster_rows}):
        rows = [row for row in cluster_rows if row["cluster_id"] == cluster_name]
        template_counts = Counter(row["template_opportunity_type"] for row in rows)
        cluster_to_template[cluster_name] = {
            "sample_size": len(rows),
            "template_distribution": dict(template_counts),
            "dominant_template": template_counts.most_common(1)[0][0] if template_counts else None,
            "dominant_template_share": _dominant_share(template_counts),
        }
    template_names = sorted({row["template_opportunity_type"] for row in cluster_rows})
    for template_name in template_names:
        rows = [row for row in cluster_rows if row["template_opportunity_type"] == template_name]
        cluster_counts = Counter(row["cluster_id"] for row in rows)
        template_to_clusters[template_name] = {
            "sample_size": len(rows),
            "cluster_distribution": dict(cluster_counts),
            "cluster_count": len(cluster_counts),
            "dominant_cluster": cluster_counts.most_common(1)[0][0] if cluster_counts else None,
            "dominant_cluster_share": _dominant_share(cluster_counts),
        }
    templates_corresponding = [
        {"template_name": template, **row}
        for template, row in template_to_clusters.items()
        if row["cluster_count"] == 1 or row["dominant_cluster_share"] >= 0.65
    ]
    templates_split = [
        {"template_name": template, **row}
        for template, row in template_to_clusters.items()
        if row["cluster_count"] >= 2 and row["dominant_cluster_share"] < 0.65
    ]
    emergent_clusters = [
        {"cluster_id": cluster, **row}
        for cluster, row in cluster_to_template.items()
        if row["dominant_template_share"] < 0.55
    ]
    broad_templates = [
        {"template_name": template, **row}
        for template, row in template_to_clusters.items()
        if row["cluster_count"] >= 3 or row["dominant_cluster_share"] < 0.50
    ]
    return {
        "cluster_to_template": cluster_to_template,
        "template_to_clusters": template_to_clusters,
        "templates_corresponding_to_natural_clusters": templates_corresponding,
        "templates_cut_across_multiple_clusters": templates_split,
        "natural_clusters_missed_or_only_loosely_captured_by_templates": emergent_clusters,
        "template_labels_appearing_artificial_or_too_broad": broad_templates,
    }


def _open_cluster_recommendation(*, cluster_summaries: Sequence[dict[str, Any]]) -> dict[str, Any]:
    candidates = [row for row in cluster_summaries if row["status"] == "candidate"]
    queue = [
        {
            "priority": index + 1,
            "cluster_id": row["cluster_id"],
            "interpretive_name": row["interpretive_name"],
            "sample_size": row["sample_size"],
            "date_window_spread": row["date_window_spread"],
            "instrument_count": row["instrument_count"],
            "follow_through_tendency": row["follow_through_tendency"],
            "contamination_risk": row["contamination_risk"],
            "clustering_risk": row["clustering_risk"],
            "dominant_template": row["dominant_template"],
        }
        for index, row in enumerate(candidates[:6])
    ]
    top = candidates[0] if candidates else None
    if top is None:
        return {
            "recommendation": "pause_and_rethink_mechanism",
            "reason": "No natural cluster cleared the repeatability and contamination guardrails.",
            "next_research_queue": [],
        }
    if "COMPRESSION" in top["interpretive_name"]:
        branch = "compression_then_continuation_detector_branch"
    elif "FAILED_MEAN_REVERSION" in top["interpretive_name"]:
        branch = "failed_mean_reversion_branch"
    elif "VWAP" in top["interpretive_name"]:
        branch = "vwap_rejection_reclaim_detector_branch"
    else:
        branch = "followup_detector_research"
    return {
        "recommendation": branch,
        "reason": (
            f"{top['cluster_id']} ({top['interpretive_name']}) showed the strongest repeatable structure without relying entirely on one window or one instrument."
        ),
        "next_research_queue": queue,
    }


def _resolved_cluster_count(*, session_count: int, requested_cluster_count: int) -> int:
    if session_count <= 0:
        return 0
    if session_count == 1:
        return 1
    return min(max(2, requested_cluster_count), session_count)


def _sample_coverage(
    *,
    feature_rows: Sequence[dict[str, Any]],
    source_label: str,
    window_labels: Sequence[str],
) -> dict[str, Any]:
    local_dates = sorted({row["local_session_date"] for row in feature_rows})
    instruments = sorted({row["instrument"] for row in feature_rows})
    families = Counter(row["instrument_family"] for row in feature_rows)
    return {
        "source_label": source_label,
        "window_labels": list(window_labels),
        "session_count": len(feature_rows),
        "instrument_count": len(instruments),
        "instruments": instruments,
        "instrument_family_breakdown": dict(families),
        "local_date_start": local_dates[0] if local_dates else None,
        "local_date_end": local_dates[-1] if local_dates else None,
    }


def _interpret_cluster_name(*, rows: Sequence[dict[str, Any]], centroid: dict[str, float]) -> str:
    if centroid["post_spike_fraction"] >= 0.14 and centroid["max_adverse_excursion_atr"] >= centroid["max_favorable_excursion_atr"] * 0.75:
        return "DIRTY_POST_SPIKE_CHOP"
    if centroid["drift_context_fraction"] < 0.05 and centroid["chop_fraction"] >= 0.70:
        return "CHOP_HEAVY_MEAN_REVERTIVE"
    if (
        centroid["compression_duration_fraction"] >= 0.16
        and centroid["expansion_after_compression_ratio"] >= 1.20
        and centroid["drift_context_fraction"] >= 0.08
    ):
        return "COMPRESSION_EXPANSION"
    if (
        centroid["failed_countertrend_rate"] >= 0.18
        and centroid["follow_through_proxy"] >= 0.50
        and centroid["drift_context_fraction"] >= 0.08
        and centroid["chop_fraction"] < 0.60
    ):
        return "FAILED_MEAN_REVERSION"
    if (
        centroid["vwap_reclaim_rate"] >= 0.08
        and centroid["signed_vwap_displacement_peak"] >= 1.20
        and centroid["drift_context_fraction"] >= 0.08
    ):
        return "VWAP_RECLAIM_CONTINUATION"
    if (
        centroid["directional_efficiency_median"] >= 0.28
        and centroid["max_adverse_excursion_atr"] < 1.0
        and centroid["vwap_crossing_rate"] < 0.10
        and centroid["drift_context_fraction"] >= 0.10
    ):
        return "SLOW_GRIND_DRIFT"
    if centroid["max_favorable_excursion_atr"] < 0.75 and centroid["reversal_frequency_median"] >= 0.45:
        return "RANGE_BOUND_MEAN_REVERSION"
    return "EMERGENT_CLUSTER"


def _longest_compression_run(rows: Sequence[Any]) -> dict[str, int]:
    if not rows:
        return {"start": -1, "end": -1, "length": 0}
    range_norms = [row.range_points / max(row.atr, 1e-9) for row in rows]
    median_range = _median(range_norms) or 0.0
    best_start = -1
    best_end = -1
    best_length = 0
    current_start = None
    for index, row in enumerate(rows):
        compressed = (
            range_norms[index] <= median_range * 0.92
            and row.bar_overlap_ratio_8 >= 0.60
            and row.realized_volatility_ratio <= 1.15
        )
        if compressed and current_start is None:
            current_start = index
        elif not compressed and current_start is not None:
            length = index - current_start
            if length > best_length:
                best_start, best_end, best_length = current_start, index - 1, length
            current_start = None
    if current_start is not None:
        length = len(rows) - current_start
        if length > best_length:
            best_start, best_end, best_length = current_start, len(rows) - 1, length
    return {"start": best_start, "end": best_end, "length": best_length}


def _expansion_after_compression(rows: Sequence[Any], compression_run: dict[str, int]) -> float:
    if compression_run["length"] <= 0:
        return 0.0
    start = compression_run["start"]
    end = compression_run["end"]
    compression_ranges = [rows[index].range_points / max(rows[index].atr, 1e-9) for index in range(start, end + 1)]
    later_ranges = [rows[index].range_points / max(rows[index].atr, 1e-9) for index in range(end + 1, len(rows))]
    if not compression_ranges or not later_ranges:
        return 0.0
    return max(later_ranges) / max(_median(compression_ranges) or 1e-9, 1e-9)


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


def _peak_aligned(rows: Sequence[Any], *, direction: str, long_attr: str, short_attr: str) -> float:
    if not rows:
        return 0.0
    if direction == "LONG":
        return max(float(getattr(row, long_attr)) for row in rows)
    return max(float(getattr(row, short_attr)) for row in rows)


def _fit_standard_scaler(matrix: Sequence[Sequence[float]]) -> dict[str, dict[str, float]]:
    columns = list(zip(*matrix))
    scaling: dict[str, dict[str, float]] = {}
    for feature, values in zip(CLUSTER_INPUT_FEATURES, columns):
        mean_value = sum(values) / len(values)
        variance = sum((value - mean_value) ** 2 for value in values) / max(len(values), 1)
        stdev = math.sqrt(variance)
        scaling[feature] = {"mean": mean_value, "stdev": stdev if stdev > 1e-9 else 1.0}
    return scaling


def _apply_standard_scaler(row: Sequence[float], scaling: dict[str, dict[str, float]]) -> list[float]:
    return [
        (float(value) - scaling[feature]["mean"]) / scaling[feature]["stdev"]
        for feature, value in zip(CLUSTER_INPUT_FEATURES, row)
    ]


def _invert_standard_scaler(values: Sequence[float], scaling: dict[str, dict[str, float]]) -> dict[str, float]:
    raw = {
        feature: values[index] * scaling[feature]["stdev"] + scaling[feature]["mean"]
        for index, feature in enumerate(CLUSTER_INPUT_FEATURES)
    }
    raw["follow_through_proxy"] = raw["max_favorable_excursion_atr"] / max(raw["max_favorable_excursion_atr"] + raw["max_adverse_excursion_atr"], 1e-9)
    return raw


def _kmeans_plus_plus_init(matrix: Sequence[Sequence[float]], *, cluster_count: int, seed: int) -> list[list[float]]:
    rng = random.Random(seed)
    centroids = [list(matrix[rng.randrange(len(matrix))])]
    while len(centroids) < cluster_count:
        distances = [min(_euclidean_sq(row, centroid) for centroid in centroids) for row in matrix]
        total = sum(distances)
        if total <= 1e-9:
            centroids.append(list(matrix[rng.randrange(len(matrix))]))
            continue
        threshold = rng.random() * total
        cumulative = 0.0
        chosen = matrix[-1]
        for row, distance in zip(matrix, distances):
            cumulative += distance
            if cumulative >= threshold:
                chosen = row
                break
        centroids.append(list(chosen))
    return centroids


def _euclidean_sq(a: Sequence[float], b: Sequence[float]) -> float:
    return sum((left - right) ** 2 for left, right in zip(a, b))


def _pattern_status(
    *,
    sample_size: int,
    continuation_tendency: float,
    contamination_risk: float,
    clustering_risk: str,
    dirty_regime_dominant: bool,
) -> str:
    if sample_size < 3 or dirty_regime_dominant:
        return "deprioritized"
    if contamination_risk > 0.60:
        return "deprioritized"
    if clustering_risk == "HIGH" and sample_size < 5:
        return "deprioritized"
    if continuation_tendency < 0.25:
        return "deprioritized"
    return "candidate"


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
        1.6 * continuation_tendency
        + 0.20 * sample_size
        + 0.7 * float(follow_through_median or 0.0)
        - 1.2 * contamination_risk
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
    if sample_size >= 10 and window_spread >= 3 and instrument_spread >= 3 and contamination_risk <= 0.35 and clustering_risk == "LOW":
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


def _overfit_warnings(
    *,
    sample_size: int,
    window_spread: int,
    instrument_spread: int,
    contamination_risk: float,
) -> list[str]:
    warnings: list[str] = []
    if sample_size < 4:
        warnings.append("small_sample_cluster")
    if window_spread <= 1:
        warnings.append("one_window_cluster")
    if instrument_spread <= 1:
        warnings.append("one_instrument_cluster")
    if contamination_risk >= 0.50:
        warnings.append("contamination_dominant_cluster")
    return warnings


def _dominant_share(counter: Counter[str]) -> float:
    if not counter:
        return 0.0
    total = sum(counter.values())
    return max(counter.values()) / max(total, 1)


def _instrument_family(instrument: str) -> str:
    if instrument.upper() in {"GC", "MGC"}:
        return "metals"
    if instrument.upper() in {"ES", "MES", "NQ", "MNQ"}:
        return "indices"
    return "other"


def _group_by(rows: Sequence[Any], *, key) -> dict[Any, list[Any]]:
    grouped: dict[Any, list[Any]] = {}
    for row in rows:
        grouped.setdefault(key(row), []).append(row)
    return grouped


def _median(values: Sequence[float | None]) -> float | None:
    usable = [float(value) for value in values if value is not None]
    if not usable:
        return None
    return float(median(usable))


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _write_open_cluster_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    layout = build_layout(output_dir)
    summary_json_path = layout["reports"] / "asia_drift_open_cluster_summary.json"
    summary_markdown_path = layout["reports"] / "asia_drift_open_cluster_summary.md"
    feature_matrix_path = layout["signals"] / "asia_drift_open_cluster_feature_matrix.csv"
    cluster_assignments_path = layout["signals"] / "asia_drift_open_cluster_assignments.csv"
    cluster_summary_csv_path = layout["reports"] / "asia_drift_open_cluster_summary_clusters.csv"
    cluster_summary_json_path = layout["reports"] / "asia_drift_open_cluster_summary_clusters.json"
    template_comparison_markdown_path = layout["reports"] / "asia_drift_open_cluster_template_comparison.md"
    template_comparison_json_path = layout["reports"] / "asia_drift_open_cluster_template_comparison.json"
    research_queue_path = layout["reports"] / "asia_drift_open_cluster_research_queue.json"

    summary_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown_path.write_text(_render_open_cluster_markdown(payload), encoding="utf-8")
    _write_csv(feature_matrix_path, payload.get("feature_matrix_rows") or [])
    _write_csv(cluster_assignments_path, payload.get("cluster_assignment_rows") or [])
    _write_csv(cluster_summary_csv_path, payload.get("cluster_summaries") or [])
    cluster_summary_json_path.write_text(json.dumps(payload.get("cluster_summaries") or [], indent=2, sort_keys=True), encoding="utf-8")
    template_comparison_markdown_path.write_text(_render_template_comparison_markdown(payload), encoding="utf-8")
    template_comparison_json_path.write_text(json.dumps(payload.get("template_vs_cluster_comparison") or {}, indent=2, sort_keys=True), encoding="utf-8")
    research_queue_path.write_text(json.dumps(payload.get("recommended_next_research_queue") or [], indent=2, sort_keys=True), encoding="utf-8")

    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_open_cluster_discovery",
            "layout": {key: str(value) for key, value in layout.items()},
            "artifact_paths": {
                "summary_json_path": str(summary_json_path),
                "summary_markdown_path": str(summary_markdown_path),
                "feature_matrix_path": str(feature_matrix_path),
                "cluster_assignments_path": str(cluster_assignments_path),
                "cluster_summary_csv_path": str(cluster_summary_csv_path),
                "cluster_summary_json_path": str(cluster_summary_json_path),
                "template_comparison_markdown_path": str(template_comparison_markdown_path),
                "template_comparison_json_path": str(template_comparison_json_path),
                "research_queue_path": str(research_queue_path),
            },
        },
    )
    return {
        "summary_json_path": str(summary_json_path),
        "summary_markdown_path": str(summary_markdown_path),
        "feature_matrix_path": str(feature_matrix_path),
        "cluster_assignments_path": str(cluster_assignments_path),
        "cluster_summary_csv_path": str(cluster_summary_csv_path),
        "cluster_summary_json_path": str(cluster_summary_json_path),
        "template_comparison_markdown_path": str(template_comparison_markdown_path),
        "template_comparison_json_path": str(template_comparison_json_path),
        "research_queue_path": str(research_queue_path),
        "storage_manifest_path": str(layout["storage_manifest"]),
    }


def _render_open_cluster_markdown(payload: dict[str, Any]) -> str:
    coverage = payload.get("sample_coverage") or {}
    lines = [
        "# Asia Drift Open Feature Cluster Discovery",
        "",
        str(payload.get("objective") or ""),
        "",
        "## Coverage",
        f"- windows={payload.get('window_labels')}",
        f"- instruments={payload.get('instruments')}",
        f"- cluster_count={payload.get('actual_cluster_count')}",
        f"- local_date_range={coverage.get('local_date_start')}..{coverage.get('local_date_end')}",
        f"- session_count={coverage.get('session_count')}",
        "",
        "## Cluster Summaries",
    ]
    for row in payload.get("cluster_summaries") or []:
        lines.append(
            f"- {row['cluster_id']} {row['interpretive_name']}: sample_size={row['sample_size']} "
            f"follow_through={row['follow_through_tendency']} contamination={row['contamination_risk']} "
            f"clustering_risk={row['clustering_risk']} dominant_template={row['dominant_template']}"
        )
    lines.extend(["", "## Template Comparison"])
    comparison = payload.get("template_vs_cluster_comparison") or {}
    lines.append(f"- templates_corresponding={comparison.get('templates_corresponding_to_natural_clusters')}")
    lines.append(f"- templates_cut_across_multiple_clusters={comparison.get('templates_cut_across_multiple_clusters')}")
    lines.append(f"- natural_clusters_missed={comparison.get('natural_clusters_missed_or_only_loosely_captured_by_templates')}")
    lines.extend(["", "## Recommendation"])
    recommendation = payload.get("recommendation") or {}
    lines.append(f"- recommendation: {recommendation.get('recommendation')}")
    lines.append(f"- reason: {recommendation.get('reason')}")
    lines.append(f"- next_research_queue: {recommendation.get('next_research_queue')}")
    return "\n".join(lines) + "\n"


def _render_template_comparison_markdown(payload: dict[str, Any]) -> str:
    comparison = payload.get("template_vs_cluster_comparison") or {}
    lines = [
        "# Asia Drift Open Cluster vs Template Comparison",
        "",
        "## Templates Corresponding To Natural Clusters",
    ]
    for row in comparison.get("templates_corresponding_to_natural_clusters") or []:
        lines.append(
            f"- {row['template_name']}: cluster_count={row['cluster_count']} dominant_cluster={row['dominant_cluster']} "
            f"dominant_cluster_share={row['dominant_cluster_share']}"
        )
    lines.extend(["", "## Templates Cut Across Multiple Clusters"])
    for row in comparison.get("templates_cut_across_multiple_clusters") or []:
        lines.append(
            f"- {row['template_name']}: cluster_count={row['cluster_count']} dominant_cluster_share={row['dominant_cluster_share']}"
        )
    lines.extend(["", "## Emergent Or Weakly Captured Natural Clusters"])
    for row in comparison.get("natural_clusters_missed_or_only_loosely_captured_by_templates") or []:
        lines.append(
            f"- {row['cluster_id']}: dominant_template={row['dominant_template']} dominant_template_share={row['dominant_template_share']}"
        )
    lines.extend(["", "## Broad Or Artificial Template Labels"])
    for row in comparison.get("template_labels_appearing_artificial_or_too_broad") or []:
        lines.append(
            f"- {row['template_name']}: cluster_count={row['cluster_count']} dominant_cluster_share={row['dominant_cluster_share']}"
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
