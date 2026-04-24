"""Cross-asset confirmation research for overnight metals vs ES/MES."""

from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from statistics import median
from typing import Any, Sequence

from ..trend_participation.storage import build_layout, write_storage_manifest
from .broader_replay import AsiaDriftReplayBarWindow
from .multi_year_discovery import (
    DEFAULT_MULTI_YEAR_INSTRUMENTS,
    run_asia_drift_multi_year_discovery,
    run_asia_drift_multi_year_discovery_from_bars,
)


PRIMARY_INSTRUMENTS = ("MGC", "GC")
CONFIRMATION_INSTRUMENTS = ("ES", "MES")
DIAGNOSTIC_INSTRUMENTS = ("MNQ", "NQ")

TIER_A = "TIER_A_STRONG_ALIGNMENT"
TIER_B = "TIER_B_MODERATE_ALIGNMENT"
TIER_C = "TIER_C_WEAK_ALIGNMENT"
TIER_NONE = "NO_CONFIRMATION"
TIER_DIVERGENT = "DIVERGENT"
TIER_NO_PARTNER = "NO_CONFIRMATION_PARTNER"

LEAD_METALS = "METALS_LEAD"
LEAD_INDEX = "ES_MES_LEAD"
LEAD_SIMULTANEOUS = "SIMULTANEOUS"
LEAD_UNKNOWN = "UNKNOWN"


def run_cross_asset_confirmation(
    *,
    output_dir: Path,
    multi_year_output_dir: Path | None = None,
    source_sqlite_path: Path | None = None,
    instruments: Sequence[str] = DEFAULT_MULTI_YEAR_INSTRUMENTS,
) -> dict[str, Any]:
    if multi_year_output_dir is not None and _multi_year_artifacts_exist(multi_year_output_dir):
        structural_payload = _load_multi_year_payload(multi_year_output_dir)
        structural_source = str(multi_year_output_dir.resolve())
    else:
        if source_sqlite_path is None:
            raise ValueError("source_sqlite_path is required when no multi-year structural artifact directory is provided")
        generated = run_asia_drift_multi_year_discovery(
            source_sqlite_path=source_sqlite_path,
            output_dir=output_dir / "_structural_source",
            instruments=instruments,
        )
        structural_payload = generated["payload"]
        structural_source = str(source_sqlite_path.resolve())
    payload = _build_cross_asset_payload(structural_payload=structural_payload, structural_source=structural_source)
    artifacts = _write_cross_asset_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def run_cross_asset_confirmation_from_bars(
    *,
    output_dir: Path,
    instrument_windows: dict[str, Sequence[AsiaDriftReplayBarWindow]],
) -> dict[str, Any]:
    structural = run_asia_drift_multi_year_discovery_from_bars(
        output_dir=output_dir / "_structural_source",
        instrument_windows=instrument_windows,
    )
    payload = _build_cross_asset_payload(
        structural_payload=structural["payload"],
        structural_source="synthetic",
    )
    artifacts = _write_cross_asset_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def _build_cross_asset_payload(
    *,
    structural_payload: dict[str, Any],
    structural_source: str,
) -> dict[str, Any]:
    full_rows = {row["asia_drift_session_id"]: row for row in structural_payload.get("feature_matrix_rows") or []}
    early_rows = structural_payload.get("early_feature_matrix_rows") or []
    by_date: dict[str, list[dict[str, Any]]] = {}
    for row in early_rows:
        by_date.setdefault(str(row["local_session_date"]), []).append(row)

    confirmation_rows: list[dict[str, Any]] = []
    for local_date, rows in sorted(by_date.items()):
        metal_rows = [row for row in rows if row["instrument"] in PRIMARY_INSTRUMENTS]
        confirm_rows = [row for row in rows if row["instrument"] in CONFIRMATION_INSTRUMENTS]
        diag_rows = [row for row in rows if row["instrument"] in DIAGNOSTIC_INSTRUMENTS]
        for metal_row in metal_rows:
            confirmation_rows.append(
                _build_confirmation_row(
                    metal_row=metal_row,
                    confirmation_candidates=confirm_rows,
                    diagnostic_candidates=diag_rows,
                    full_row=full_rows.get(metal_row["asia_drift_session_id"], {}),
                )
            )

    tier_summary = _tier_summary(confirmation_rows=confirmation_rows)
    baseline_comparison = _baseline_comparison(confirmation_rows=confirmation_rows)
    lead_lag_report = _lead_lag_report(confirmation_rows=confirmation_rows)
    recommendation = _recommendation(
        tier_summary=tier_summary,
        baseline_comparison=baseline_comparison,
    )
    return {
        "module": "Asia Drift Cross-Asset Confirmation Research",
        "objective": (
            "Research-only conditioned branch study for overnight metals confirmed by ES/MES. "
            "This pass measures early-session cross-asset alignment, tiers its strength, compares it against metals-only "
            "and no-confirmation baselines, and evaluates whether confirmation separates directional sessions from dead chop."
        ),
        "structural_source": structural_source,
        "primary_instruments": list(PRIMARY_INSTRUMENTS),
        "confirmation_instruments": list(CONFIRMATION_INSTRUMENTS),
        "diagnostic_instruments": list(DIAGNOSTIC_INSTRUMENTS),
        "coverage_report": structural_payload.get("coverage_report") or {},
        "session_confirmation_rows": confirmation_rows,
        "tier_summary": tier_summary,
        "baseline_comparison": baseline_comparison,
        "lead_lag_report": lead_lag_report,
        "recommendation": recommendation,
    }


def _build_confirmation_row(
    *,
    metal_row: dict[str, Any],
    confirmation_candidates: Sequence[dict[str, Any]],
    diagnostic_candidates: Sequence[dict[str, Any]],
    full_row: dict[str, Any],
) -> dict[str, Any]:
    best_partner = None
    best_metrics = None
    for partner in confirmation_candidates:
        metrics = _pair_metrics(primary=metal_row, partner=partner)
        if best_metrics is None or metrics["alignment_score"] > best_metrics["alignment_score"]:
            best_partner = partner
            best_metrics = metrics

    diagnostic_alignment = _diagnostic_alignment(primary=metal_row, diagnostic_candidates=diagnostic_candidates)
    metals_only_active = _primary_signal_active(metal_row)
    partner_only_active = bool(best_partner and _primary_signal_active(best_partner))

    if best_partner is None or best_metrics is None:
        tier = TIER_NO_PARTNER
    elif best_metrics["divergence_flag"]:
        tier = TIER_DIVERGENT
    elif best_metrics["contamination_flag"]:
        tier = TIER_NONE
    elif best_metrics["alignment_score"] >= 6:
        tier = TIER_A
    elif best_metrics["alignment_score"] >= 5 and best_metrics["direction_agreement"]:
        tier = TIER_B
    elif best_metrics["alignment_score"] >= 4 and best_metrics["direction_agreement"]:
        tier = TIER_C
    else:
        tier = TIER_NONE

    continuation = _as_bool(full_row.get("directional_continuation"), default=_as_bool(metal_row.get("directional_continuation")))
    result = {
        "local_session_date": metal_row["local_session_date"],
        "primary_session_id": metal_row["asia_drift_session_id"],
        "primary_instrument": metal_row["instrument"],
        "primary_instrument_family": metal_row["instrument_family"],
        "primary_direction": metal_row["candidate_direction"],
        "primary_template_type": metal_row.get("template_opportunity_type"),
        "primary_contaminated": _as_bool(metal_row.get("contaminated")),
        "confirmation_tier": tier,
        "confirmation_partner_instrument": best_partner["instrument"] if best_partner is not None else None,
        "confirmation_partner_session_id": best_partner["asia_drift_session_id"] if best_partner is not None else None,
        "confirmation_partner_direction": best_partner["candidate_direction"] if best_partner is not None else None,
        "diagnostic_nq_alignment": diagnostic_alignment["alignment"],
        "diagnostic_nq_count": diagnostic_alignment["count"],
        "metals_only_signal": metals_only_active,
        "es_mes_only_signal": partner_only_active,
        "no_confirmation_baseline": tier in {TIER_NONE, TIER_DIVERGENT, TIER_NO_PARTNER},
        "directional_resolution": continuation,
        "continuation_quality": full_row.get("continuation_quality") or metal_row.get("continuation_quality"),
        "continuation_magnitude_atr": _as_float(full_row.get("max_favorable_excursion_atr")),
        "adverse_magnitude_atr": _as_float(full_row.get("max_adverse_excursion_atr")),
        "resolution_subphase": full_row.get("time_of_largest_expansion_subphase"),
        "lead_lag_category": best_metrics["lead_lag_category"] if best_metrics else LEAD_UNKNOWN,
        "lead_lag_bars_estimate": best_metrics["lead_lag_bars_estimate"] if best_metrics else None,
        "alignment_score": best_metrics["alignment_score"] if best_metrics else 0,
        "direction_agreement": best_metrics["direction_agreement"] if best_metrics else False,
        "signed_vwap_alignment": best_metrics["signed_vwap_alignment"] if best_metrics else False,
        "directional_efficiency_alignment": best_metrics["directional_efficiency_alignment"] if best_metrics else False,
        "drift_context_agreement": best_metrics["drift_context_agreement"] if best_metrics else False,
        "impulse_timing_agreement": best_metrics["impulse_timing_agreement"] if best_metrics else False,
        "failed_mean_reversion_alignment": best_metrics["failed_mean_reversion_alignment"] if best_metrics else False,
        "vwap_noise_suppressed": best_metrics["vwap_noise_suppressed"] if best_metrics else False,
        "divergence_flag": best_metrics["divergence_flag"] if best_metrics else False,
        "contamination_flag": best_metrics["contamination_flag"] if best_metrics else _as_bool(metal_row.get("contaminated")),
        "lead_asset": best_metrics["lead_asset"] if best_metrics else None,
        "lead_partner_score": best_metrics["partner_signal_strength"] if best_metrics else None,
        "primary_signal_strength": _signal_strength(metal_row),
    }
    return result


def _pair_metrics(*, primary: dict[str, Any], partner: dict[str, Any]) -> dict[str, Any]:
    direction_agreement = primary["candidate_direction"] == partner["candidate_direction"]
    signed_vwap_alignment = (
        direction_agreement
        and _as_float(primary["early_signed_vwap_displacement_peak"]) >= 0.75
        and _as_float(partner["early_signed_vwap_displacement_peak"]) >= 0.75
    )
    directional_efficiency_alignment = (
        _as_float(primary["early_directional_efficiency_median"]) >= 0.30
        and _as_float(partner["early_directional_efficiency_median"]) >= 0.30
    )
    drift_context_agreement = (
        _as_float(primary["early_drift_context_fraction"]) >= 0.25
        and _as_float(partner["early_drift_context_fraction"]) >= 0.25
    )
    impulse_timing_agreement = abs(
        _as_float(primary["early_first_impulse_index_norm"]) - _as_float(partner["early_first_impulse_index_norm"])
    ) <= 0.20
    failed_mean_reversion_alignment = min(
        _as_float(primary["early_failed_countertrend_rate"]),
        _as_float(partner["early_failed_countertrend_rate"]),
    ) >= 0.10
    vwap_noise_suppressed = (
        max(_as_float(primary["early_vwap_crossing_rate"]), _as_float(partner["early_vwap_crossing_rate"])) <= 0.20
        and max(_as_float(primary["early_vwap_reclaim_rate"]), _as_float(partner["early_vwap_reclaim_rate"])) <= 0.10
    )
    contamination_flag = _as_bool(primary.get("contaminated")) or _as_bool(partner.get("contaminated"))
    divergence_flag = (
        not direction_agreement
        or (
            _as_float(primary["early_signed_vwap_displacement_peak"]) >= 0.75
            and _as_float(partner["early_signed_vwap_displacement_peak"]) < 0.35
        )
    )
    impulse_diff = _as_float(partner["early_first_impulse_index_norm"]) - _as_float(primary["early_first_impulse_index_norm"])
    lead_lag_bars_estimate = round(abs(impulse_diff) * 11.0, 2)
    if abs(impulse_diff) <= 0.15:
        lead_lag_category = LEAD_SIMULTANEOUS
        lead_asset = "SIMULTANEOUS"
    elif impulse_diff > 0:
        lead_lag_category = LEAD_METALS
        lead_asset = "METALS"
    else:
        lead_lag_category = LEAD_INDEX
        lead_asset = "ES_MES"
    alignment_score = sum(
        1
        for flag in (
            direction_agreement,
            signed_vwap_alignment,
            directional_efficiency_alignment,
            drift_context_agreement,
            impulse_timing_agreement,
            failed_mean_reversion_alignment,
            vwap_noise_suppressed,
        )
        if flag
    )
    return {
        "direction_agreement": direction_agreement,
        "signed_vwap_alignment": signed_vwap_alignment,
        "directional_efficiency_alignment": directional_efficiency_alignment,
        "drift_context_agreement": drift_context_agreement,
        "impulse_timing_agreement": impulse_timing_agreement,
        "failed_mean_reversion_alignment": failed_mean_reversion_alignment,
        "vwap_noise_suppressed": vwap_noise_suppressed,
        "contamination_flag": contamination_flag,
        "divergence_flag": divergence_flag,
        "alignment_score": alignment_score,
        "lead_lag_category": lead_lag_category,
        "lead_lag_bars_estimate": lead_lag_bars_estimate,
        "lead_asset": lead_asset,
        "partner_signal_strength": _signal_strength(partner),
    }


def _diagnostic_alignment(*, primary: dict[str, Any], diagnostic_candidates: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not diagnostic_candidates:
        return {"alignment": "NONE", "count": 0}
    aligned = sum(1 for row in diagnostic_candidates if row["candidate_direction"] == primary["candidate_direction"])
    if aligned == len(diagnostic_candidates):
        return {"alignment": "ALIGNED", "count": len(diagnostic_candidates)}
    if aligned == 0:
        return {"alignment": "DIVERGENT", "count": len(diagnostic_candidates)}
    return {"alignment": "MIXED", "count": len(diagnostic_candidates)}


def _primary_signal_active(row: dict[str, Any]) -> bool:
    return (
        _as_float(row["early_signed_vwap_displacement_peak"]) >= 0.75
        and _as_float(row["early_drift_context_fraction"]) >= 0.25
        and _as_float(row["early_post_spike_fraction"]) < 0.10
        and _as_float(row["early_deep_damage_fraction"]) < 0.10
    )


def _signal_strength(row: dict[str, Any]) -> float:
    return (
        0.8 * _as_float(row["early_signed_vwap_displacement_peak"])
        + 0.7 * _as_float(row["early_drift_context_fraction"])
        + 0.5 * _as_float(row["early_directional_efficiency_median"])
        + 0.3 * _as_float(row["early_failed_countertrend_rate"])
        - 0.6 * _as_float(row["early_post_spike_fraction"])
        - 0.6 * _as_float(row["early_deep_damage_fraction"])
    )


def _tier_summary(*, confirmation_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered_tiers = [TIER_A, TIER_B, TIER_C, TIER_NONE, TIER_DIVERGENT, TIER_NO_PARTNER]
    results: list[dict[str, Any]] = []
    for tier in ordered_tiers:
        rows = [row for row in confirmation_rows if row["confirmation_tier"] == tier]
        if not rows:
            continue
        results.append(
            {
                "tier": tier,
                "sample_size": len(rows),
                "directional_resolution_rate": _ratio(sum(1 for row in rows if row["directional_resolution"]), len(rows)),
                "continuation_magnitude_median_atr": _median([row["continuation_magnitude_atr"] for row in rows if row["directional_resolution"]]),
                "failure_reversal_rate": _ratio(
                    sum(1 for row in rows if _as_float(row["adverse_magnitude_atr"]) >= max(_as_float(row["continuation_magnitude_atr"]) * 0.75, 0.75)),
                    len(rows),
                ),
                "contamination_rate": _ratio(sum(1 for row in rows if row["contamination_flag"]), len(rows)),
                "month_distribution": dict(Counter(row["local_session_date"][:7] for row in rows)),
                "instrument_breakdown": dict(Counter(row["primary_instrument"] for row in rows)),
                "resolution_subphase_distribution": dict(Counter((row["resolution_subphase"] or "UNKNOWN") for row in rows)),
                "lead_lag_distribution": dict(Counter(row["lead_lag_category"] for row in rows)),
            }
        )
    return results


def _baseline_comparison(*, confirmation_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = {
        "cross_asset_tier_a_b": [row for row in confirmation_rows if row["confirmation_tier"] in {TIER_A, TIER_B}],
        "metals_only_signal": [row for row in confirmation_rows if row["metals_only_signal"]],
        "es_mes_only_signal": [row for row in confirmation_rows if row["es_mes_only_signal"]],
        "no_confirmation_baseline": [row for row in confirmation_rows if row["no_confirmation_baseline"]],
    }
    results: list[dict[str, Any]] = []
    for name, rows in groups.items():
        if not rows:
            continue
        results.append(
            {
                "group_name": name,
                "sample_size": len(rows),
                "directional_resolution_rate": _ratio(sum(1 for row in rows if row["directional_resolution"]), len(rows)),
                "continuation_magnitude_median_atr": _median([row["continuation_magnitude_atr"] for row in rows if row["directional_resolution"]]),
                "failure_reversal_rate": _ratio(
                    sum(1 for row in rows if _as_float(row["adverse_magnitude_atr"]) >= max(_as_float(row["continuation_magnitude_atr"]) * 0.75, 0.75)),
                    len(rows),
                ),
                "contamination_rate": _ratio(sum(1 for row in rows if row["contamination_flag"]), len(rows)),
                "month_spread": len({row["local_session_date"][:7] for row in rows}),
                "instrument_breakdown": dict(Counter(row["primary_instrument"] for row in rows)),
            }
        )
    return results


def _lead_lag_report(*, confirmation_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    active = [row for row in confirmation_rows if row["confirmation_tier"] in {TIER_A, TIER_B, TIER_C}]
    return {
        "overall_distribution": dict(Counter(row["lead_lag_category"] for row in active)),
        "resolution_rate_by_category": {
            category: _ratio(
                sum(1 for row in rows if row["directional_resolution"]),
                len(rows),
            )
            for category in sorted({row["lead_lag_category"] for row in active})
            for rows in ([row for row in active if row["lead_lag_category"] == category],)
        },
        "sample_events": active[:25],
    }


def _recommendation(
    *,
    tier_summary: Sequence[dict[str, Any]],
    baseline_comparison: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    tiers = {row["tier"]: row for row in tier_summary}
    baselines = {row["group_name"]: row for row in baseline_comparison}
    tier_ab = baselines.get("cross_asset_tier_a_b")
    no_confirmation = baselines.get("no_confirmation_baseline")
    metals_only = baselines.get("metals_only_signal")
    if tier_ab and no_confirmation:
        uplift = float(tier_ab["directional_resolution_rate"]) - float(no_confirmation["directional_resolution_rate"])
    else:
        uplift = 0.0
    viable = bool(
        tier_ab
        and float(tier_ab["directional_resolution_rate"]) >= 0.25
        and uplift >= 0.15
        and int(tier_ab["month_spread"]) >= 6
        and float(tier_ab["contamination_rate"]) <= 0.50
    )
    if viable:
        verdict = "cross_asset_confirmation_separates_directional_sessions"
        reason = (
            "Tier A/B cross-asset confirmation materially outperformed the no-confirmation baseline "
            "across multiple months with manageable contamination."
        )
    else:
        verdict = "cross_asset_confirmation_is_promising_but_not_yet_reliable"
        reason = (
            "Cross-asset confirmation improves select directional cases, but the effect remains conditional and "
            "still sits inside a mostly chop-heavy overnight universe."
        )
    return {
        "recommendation": verdict,
        "reason": reason,
        "tier_a_summary": tiers.get(TIER_A),
        "tier_b_summary": tiers.get(TIER_B),
        "tier_ab_vs_metals_only_resolution_uplift": (
            float(tier_ab["directional_resolution_rate"]) - float(metals_only["directional_resolution_rate"])
            if tier_ab and metals_only
            else None
        ),
        "tier_ab_vs_no_confirmation_resolution_uplift": uplift if tier_ab and no_confirmation else None,
    }


def _multi_year_artifacts_exist(path: Path) -> bool:
    return (
        (path / "signals" / "asia_drift_multi_year_early_feature_matrix.csv").exists()
        and (path / "signals" / "asia_drift_multi_year_feature_matrix.csv").exists()
        and (path / "reports" / "asia_drift_multi_year_discovery_summary.json").exists()
    )


def _load_multi_year_payload(path: Path) -> dict[str, Any]:
    summary = json.loads((path / "reports" / "asia_drift_multi_year_discovery_summary.json").read_text(encoding="utf-8"))
    early_rows = list(csv.DictReader((path / "signals" / "asia_drift_multi_year_early_feature_matrix.csv").open()))
    full_rows = list(csv.DictReader((path / "signals" / "asia_drift_multi_year_feature_matrix.csv").open()))
    summary["early_feature_matrix_rows"] = early_rows
    summary["feature_matrix_rows"] = full_rows
    return summary


def _write_cross_asset_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    layout = build_layout(output_dir)
    summary_json_path = layout["reports"] / "asia_drift_cross_asset_confirmation_summary.json"
    summary_markdown_path = layout["reports"] / "asia_drift_cross_asset_confirmation_summary.md"
    session_labels_path = layout["signals"] / "asia_drift_cross_asset_confirmation_sessions.csv"
    confirmation_feature_matrix_path = layout["signals"] / "asia_drift_cross_asset_confirmation_feature_matrix.csv"
    tier_summary_json_path = layout["reports"] / "asia_drift_cross_asset_confirmation_tiers.json"
    tier_summary_markdown_path = layout["reports"] / "asia_drift_cross_asset_confirmation_tiers.md"
    baseline_comparison_json_path = layout["reports"] / "asia_drift_cross_asset_confirmation_baselines.json"
    lead_lag_json_path = layout["reports"] / "asia_drift_cross_asset_confirmation_lead_lag.json"

    summary_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown_path.write_text(_render_markdown(payload), encoding="utf-8")
    _write_csv(session_labels_path, payload.get("session_confirmation_rows") or [])
    _write_csv(confirmation_feature_matrix_path, payload.get("session_confirmation_rows") or [])
    tier_summary_json_path.write_text(json.dumps(payload.get("tier_summary") or [], indent=2, sort_keys=True), encoding="utf-8")
    tier_summary_markdown_path.write_text(_render_tier_markdown(payload), encoding="utf-8")
    baseline_comparison_json_path.write_text(json.dumps(payload.get("baseline_comparison") or [], indent=2, sort_keys=True), encoding="utf-8")
    lead_lag_json_path.write_text(json.dumps(payload.get("lead_lag_report") or {}, indent=2, sort_keys=True), encoding="utf-8")

    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_cross_asset_confirmation",
            "layout": {key: str(value) for key, value in layout.items()},
            "artifact_paths": {
                "summary_json_path": str(summary_json_path),
                "summary_markdown_path": str(summary_markdown_path),
                "session_labels_path": str(session_labels_path),
                "confirmation_feature_matrix_path": str(confirmation_feature_matrix_path),
                "tier_summary_json_path": str(tier_summary_json_path),
                "tier_summary_markdown_path": str(tier_summary_markdown_path),
                "baseline_comparison_json_path": str(baseline_comparison_json_path),
                "lead_lag_json_path": str(lead_lag_json_path),
            },
        },
    )
    return {
        "summary_json_path": str(summary_json_path),
        "summary_markdown_path": str(summary_markdown_path),
        "session_labels_path": str(session_labels_path),
        "confirmation_feature_matrix_path": str(confirmation_feature_matrix_path),
        "tier_summary_json_path": str(tier_summary_json_path),
        "tier_summary_markdown_path": str(tier_summary_markdown_path),
        "baseline_comparison_json_path": str(baseline_comparison_json_path),
        "lead_lag_json_path": str(lead_lag_json_path),
        "storage_manifest_path": str(layout["storage_manifest"]),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia Drift Cross-Asset Confirmation",
        "",
        str(payload.get("objective") or ""),
        "",
        "## Coverage",
        f"- primary={payload.get('primary_instruments')}",
        f"- confirmation={payload.get('confirmation_instruments')}",
        f"- diagnostic={payload.get('diagnostic_instruments')}",
        f"- coverage={payload.get('coverage_report')}",
        "",
        "## Tier Summary",
    ]
    for row in payload.get("tier_summary") or []:
        lines.append(
            f"- {row['tier']}: sample_size={row['sample_size']} resolution_rate={row['directional_resolution_rate']} "
            f"magnitude={row['continuation_magnitude_median_atr']} contamination={row['contamination_rate']}"
        )
    lines.extend(["", "## Baselines"])
    for row in payload.get("baseline_comparison") or []:
        lines.append(
            f"- {row['group_name']}: sample_size={row['sample_size']} resolution_rate={row['directional_resolution_rate']} "
            f"magnitude={row['continuation_magnitude_median_atr']} contamination={row['contamination_rate']}"
        )
    lines.extend(["", "## Recommendation"])
    recommendation = payload.get("recommendation") or {}
    lines.append(f"- recommendation={recommendation.get('recommendation')}")
    lines.append(f"- reason={recommendation.get('reason')}")
    lines.append(f"- tier_ab_vs_no_confirmation_resolution_uplift={recommendation.get('tier_ab_vs_no_confirmation_resolution_uplift')}")
    return "\n".join(lines) + "\n"


def _render_tier_markdown(payload: dict[str, Any]) -> str:
    lines = ["# Cross-Asset Confirmation Tier Summary", ""]
    for row in payload.get("tier_summary") or []:
        lines.append(
            f"- {row['tier']}: sample_size={row['sample_size']} resolution_rate={row['directional_resolution_rate']} "
            f"failure_rate={row['failure_reversal_rate']} contamination={row['contamination_rate']} "
            f"months={len(row['month_distribution'])}"
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


def _as_float(value: Any) -> float:
    if value in {None, ""}:
        return 0.0
    return float(value)


def _as_bool(value: Any, default: bool = False) -> bool:
    if value in {None, ""}:
        return default
    if isinstance(value, bool):
        return value
    return str(value).lower() == "true"


def _median(values: Sequence[float | None]) -> float | None:
    usable = [float(value) for value in values if value is not None]
    if not usable:
        return None
    return float(median(usable))


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator
