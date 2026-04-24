"""Artifact generation for Asia Drift v1 Phase 1."""

from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Sequence

from ..trend_participation.storage import build_layout, write_storage_manifest
from .models import (
    AsiaDriftEntryEvaluation,
    AsiaDriftEntrySetup,
    AsiaDriftFeatureRow,
    AsiaDriftPhase1Artifacts,
    AsiaDriftPhase2Artifacts,
    AsiaDriftSessionSummary,
    AsiaDriftStateRow,
    AsiaDriftTradeRecord,
    row_to_flat_dict,
)
from .state_machine import (
    STATE_DRIFT_AT_RISK,
    STATE_ENTRY_ARMED,
    STATE_RECOVERED_DRIFT,
    STATE_REQUALIFIED_CANDIDATE,
    STATE_THESIS_INVALIDATED,
)


def write_phase1_artifacts(
    *,
    output_dir: Path,
    feature_rows: Sequence[AsiaDriftFeatureRow],
    state_rows: Sequence[AsiaDriftStateRow],
    session_summaries: Sequence[AsiaDriftSessionSummary],
    data_summary: dict[str, Any],
) -> AsiaDriftPhase1Artifacts:
    layout = build_layout(output_dir)
    feature_rows_path = layout["features"] / "asia_drift_feature_rows.csv"
    state_rows_path = layout["signals"] / "asia_drift_state_rows.csv"
    session_summaries_path = layout["reports"] / "asia_drift_session_summaries.json"
    candidate_manifest_path = layout["reports"] / "asia_drift_candidate_manifest.json"
    summary_json_path = layout["reports"] / "asia_drift_phase1_summary.json"
    summary_markdown_path = layout["reports"] / "asia_drift_phase1_summary.md"

    _write_csv(feature_rows_path, [row_to_flat_dict(row) for row in feature_rows])
    _write_csv(state_rows_path, [row_to_flat_dict(row) for row in state_rows])
    session_payload = [row_to_flat_dict(row) for row in session_summaries]
    session_summaries_path.write_text(json.dumps(session_payload, indent=2, sort_keys=True), encoding="utf-8")

    candidate_manifest = _build_candidate_manifest(session_summaries)
    candidate_manifest_path.write_text(json.dumps(candidate_manifest, indent=2, sort_keys=True), encoding="utf-8")

    summary_payload = _build_summary_payload(
        feature_rows=feature_rows,
        state_rows=state_rows,
        session_summaries=session_summaries,
        candidate_manifest=candidate_manifest,
        data_summary=data_summary,
        layout=layout,
    )
    summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown_path.write_text(_render_markdown(summary_payload), encoding="utf-8")

    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_v1_phase1",
            "layout": {key: str(value) for key, value in layout.items()},
            "bar_count": len(feature_rows),
            "session_count": len(session_summaries),
            "artifact_paths": {
                "summary_json_path": str(summary_json_path),
                "summary_markdown_path": str(summary_markdown_path),
                "feature_rows_path": str(feature_rows_path),
                "state_rows_path": str(state_rows_path),
                "session_summaries_path": str(session_summaries_path),
                "candidate_manifest_path": str(candidate_manifest_path),
            },
        },
    )
    return AsiaDriftPhase1Artifacts(
        root_dir=layout["root"],
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        feature_rows_path=feature_rows_path,
        state_rows_path=state_rows_path,
        session_summaries_path=session_summaries_path,
        candidate_manifest_path=candidate_manifest_path,
        storage_manifest_path=layout["storage_manifest"],
    )


def write_phase2_artifacts(
    *,
    output_dir: Path,
    phase1_artifacts: AsiaDriftPhase1Artifacts,
    entry_setups: Sequence[AsiaDriftEntrySetup],
    entry_evaluations: Sequence[AsiaDriftEntryEvaluation],
    trade_records: Sequence[AsiaDriftTradeRecord],
    diagnostics: dict[str, Any],
) -> AsiaDriftPhase2Artifacts:
    layout = build_layout(output_dir)
    entry_setups_path = layout["reports"] / "asia_drift_phase2_entry_setups.json"
    entry_evaluations_path = layout["signals"] / "asia_drift_phase2_entry_evaluations.csv"
    trade_records_path = layout["trades"] / "asia_drift_phase2_trade_records.csv"
    diagnostics_json_path = layout["reports"] / "asia_drift_phase2_diagnostics.json"
    summary_json_path = layout["reports"] / "asia_drift_phase2_summary.json"
    summary_markdown_path = layout["reports"] / "asia_drift_phase2_summary.md"

    entry_setups_path.write_text(
        json.dumps([row_to_flat_dict(row) for row in entry_setups], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    _write_csv(entry_evaluations_path, [row_to_flat_dict(row) for row in entry_evaluations])
    _write_csv(trade_records_path, [row_to_flat_dict(row) for row in trade_records])
    diagnostics_json_path.write_text(json.dumps(diagnostics, indent=2, sort_keys=True), encoding="utf-8")

    summary_payload = _build_phase2_summary_payload(
        diagnostics=diagnostics,
        phase1_artifacts=phase1_artifacts,
        entry_setups_path=entry_setups_path,
        entry_evaluations_path=entry_evaluations_path,
        trade_records_path=trade_records_path,
        diagnostics_json_path=diagnostics_json_path,
    )
    summary_json_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_markdown_path.write_text(_render_phase2_markdown(summary_payload), encoding="utf-8")

    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_v1_phase2",
            "layout": {key: str(value) for key, value in layout.items()},
            "entry_setup_count": len(entry_setups),
            "entry_evaluation_count": len(entry_evaluations),
            "trade_record_count": len(trade_records),
            "phase1_root_dir": str(phase1_artifacts.root_dir),
            "artifact_paths": {
                "entry_setups_path": str(entry_setups_path),
                "entry_evaluations_path": str(entry_evaluations_path),
                "trade_records_path": str(trade_records_path),
                "diagnostics_json_path": str(diagnostics_json_path),
                "summary_json_path": str(summary_json_path),
                "summary_markdown_path": str(summary_markdown_path),
            },
        },
    )
    return AsiaDriftPhase2Artifacts(
        root_dir=layout["root"],
        phase1_root_dir=phase1_artifacts.root_dir,
        entry_setups_path=entry_setups_path,
        entry_evaluations_path=entry_evaluations_path,
        trade_records_path=trade_records_path,
        diagnostics_json_path=diagnostics_json_path,
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        storage_manifest_path=layout["storage_manifest"],
    )


def _build_candidate_manifest(session_summaries: Sequence[AsiaDriftSessionSummary]) -> dict[str, Any]:
    promising = [row for row in session_summaries if row.promising_session]
    promising_sorted = sorted(
        promising,
        key=lambda row: (
            row.entry_ready_bar_count,
            row.max_long_drift_score if row.candidate_direction == "LONG" else row.max_short_drift_score,
        ),
        reverse=True,
    )
    return {
        "promising_session_count": len(promising_sorted),
        "promising_sessions": [row_to_flat_dict(row) for row in promising_sorted[:25]],
        "entry_ready_sessions": [
            row_to_flat_dict(row)
            for row in promising_sorted
            if row.entry_ready_bar_count > 0
        ][:25],
        "invalidated_candidate_sessions": [
            row_to_flat_dict(row)
            for row in promising_sorted
            if row.invalidated_bar_count > 0
        ][:25],
    }


def _build_summary_payload(
    *,
    feature_rows: Sequence[AsiaDriftFeatureRow],
    state_rows: Sequence[AsiaDriftStateRow],
    session_summaries: Sequence[AsiaDriftSessionSummary],
    candidate_manifest: dict[str, Any],
    data_summary: dict[str, Any],
    layout: dict[str, Path],
) -> dict[str, Any]:
    regime_counts = Counter(row.regime for row in feature_rows)
    pullback_counts = Counter(row.pullback_state for row in feature_rows if row.regime != "NO_TRADE")
    state_counts = Counter(row.state for row in state_rows)
    drift_strength_counts = Counter(
        row.long_drift_strength if row.regime == "ASIA_DRIFT_LONG" else row.short_drift_strength
        for row in feature_rows
        if row.regime in {"ASIA_DRIFT_LONG", "ASIA_DRIFT_SHORT"}
    )
    failure_mode_counts = Counter()
    for row in feature_rows:
        if not row.in_scope:
            continue
        if row.chop_veto:
            failure_mode_counts["chop_veto"] += 1
        if row.post_spike_instability:
            failure_mode_counts["post_spike_instability"] += 1
        if row.pullback_state == "DISQUALIFYING_PULLBACK":
            failure_mode_counts["disqualifying_pullback"] += 1
        if row.pullback_state == "TOO_EXTENDED":
            failure_mode_counts["too_extended_without_reset"] += 1
    return {
        "module": "Asia Drift v1 Phase 1",
        "calibration_profile": feature_rows[0].calibration_profile if feature_rows else None,
        "objective": (
            "Research-only observability-first pass to test whether Asia-session directional drift can be identified "
            "mathematically, whether pullback quality is coherent, and whether the state machine produces useful "
            "session snapshots worthy of Phase 2 entry/exit study."
        ),
        "project_structure": {
            "package_root": "src/mgc_v05l/research/asia_drift",
            "components": [
                "session_scope.py",
                "features.py",
                "state_machine.py",
                "report.py",
                "engine.py",
            ],
            "non_goals": [
                "live execution",
                "broker integration",
                "production routing",
                "Phase 2 entry/exit engine",
                "promotion gating",
            ],
        },
        "data_summary": data_summary,
        "artifact_overview": {
            "feature_rows_csv": str(layout["features"] / "asia_drift_feature_rows.csv"),
            "state_rows_csv": str(layout["signals"] / "asia_drift_state_rows.csv"),
            "session_summaries_json": str(layout["reports"] / "asia_drift_session_summaries.json"),
            "candidate_manifest_json": str(layout["reports"] / "asia_drift_candidate_manifest.json"),
        },
        "headline_counts": {
            "feature_row_count": len(feature_rows),
            "state_row_count": len(state_rows),
            "session_count": len(session_summaries),
            "promising_session_count": candidate_manifest.get("promising_session_count", 0),
            "entry_armed_bar_count": state_counts.get(STATE_ENTRY_ARMED, 0),
            "at_risk_bar_count": state_counts.get(STATE_DRIFT_AT_RISK, 0),
            "recovered_drift_bar_count": state_counts.get(STATE_RECOVERED_DRIFT, 0),
            "requalified_candidate_bar_count": state_counts.get(STATE_REQUALIFIED_CANDIDATE, 0),
            "thesis_invalidated_bar_count": state_counts.get(STATE_THESIS_INVALIDATED, 0),
        },
        "regime_counts": dict(regime_counts),
        "pullback_counts_on_tradable_bars": dict(pullback_counts),
        "state_counts": dict(state_counts),
        "drift_strength_counts_on_tradable_bars": dict(drift_strength_counts),
        "candidate_manifest": candidate_manifest,
        "failure_mode_counts": dict(failure_mode_counts),
        "warning_counts": {
            "pullback_warning_bars": sum(1 for row in feature_rows if row.pullback_warning_flag),
            "single_close_through_vwap": sum(1 for row in feature_rows if row.pullback_warning_reason == "single_close_through_vwap"),
            "drift_at_risk_state_bars": state_counts.get(STATE_DRIFT_AT_RISK, 0),
        },
    }


def _build_phase2_summary_payload(
    *,
    diagnostics: dict[str, Any],
    phase1_artifacts: AsiaDriftPhase1Artifacts,
    entry_setups_path: Path,
    entry_evaluations_path: Path,
    trade_records_path: Path,
    diagnostics_json_path: Path,
) -> dict[str, Any]:
    return {
        "module": diagnostics.get("module"),
        "objective": diagnostics.get("objective"),
        "calibration_profile": diagnostics.get("calibration_profile"),
        "source_summary": diagnostics.get("source_summary"),
        "headline_counts": diagnostics.get("headline_counts"),
        "protection_preservation": diagnostics.get("protection_preservation"),
        "refined_prefill_profile": diagnostics.get("refined_prefill_profile"),
        "prefill_regime_loss_audit": diagnostics.get("prefill_regime_loss_audit"),
        "entry_model_summary": diagnostics.get("entry_model_summary"),
        "entry_geometry_diagnostics": diagnostics.get("entry_geometry_diagnostics"),
        "trade_matrix_summary": diagnostics.get("trade_matrix_summary"),
        "exit_reason_distribution": diagnostics.get("exit_reason_distribution"),
        "invalidation_diagnostics": diagnostics.get("invalidation_diagnostics"),
        "premature_invalidation_audit": diagnostics.get("premature_invalidation_audit"),
        "warning_state_audit": diagnostics.get("warning_state_audit"),
        "recovery_requalification_audit": diagnostics.get("recovery_requalification_audit"),
        "pullback_veto_diagnostics": diagnostics.get("pullback_veto_diagnostics"),
        "handoff_diagnostics": diagnostics.get("handoff_diagnostics"),
        "interpretation": diagnostics.get("interpretation"),
        "artifact_overview": {
            "phase1_root_dir": str(phase1_artifacts.root_dir),
            "phase1_summary_markdown_path": str(phase1_artifacts.summary_markdown_path),
            "entry_setups_path": str(entry_setups_path),
            "entry_evaluations_path": str(entry_evaluations_path),
            "trade_records_path": str(trade_records_path),
            "diagnostics_json_path": str(diagnostics_json_path),
        },
        "review_samples": diagnostics.get("artifacts_for_review"),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia Drift v1 Phase 1",
        "",
        payload["objective"],
        "",
        f"- calibration_profile: {payload.get('calibration_profile')}",
        f"- refined_prefill_profile: {payload.get('refined_prefill_profile')}",
        "",
        "## Headline Counts",
    ]
    for key, value in sorted((payload.get("headline_counts") or {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Regime Counts"])
    for key, value in sorted((payload.get("regime_counts") or {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Pullback Counts On Tradable Bars"])
    for key, value in sorted((payload.get("pullback_counts_on_tradable_bars") or {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## State Counts"])
    for key, value in sorted((payload.get("state_counts") or {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Failure Mode Counts"])
    for key, value in sorted((payload.get("failure_mode_counts") or {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Warning Counts"])
    for key, value in sorted((payload.get("warning_counts") or {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Promising Sessions"])
    promising = (payload.get("candidate_manifest") or {}).get("promising_sessions") or []
    if not promising:
        lines.append("- none")
    else:
        for row in promising[:10]:
            lines.append(
                f"- {row['asia_drift_session_id']}: direction={row['candidate_direction']} "
                f"max_strength={row['max_drift_strength']} entry_ready_bars={row['entry_ready_bar_count']} "
                f"invalidated_bars={row['invalidated_bar_count']}"
            )
    return "\n".join(lines) + "\n"


def _render_phase2_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia Drift v1 Phase 2",
        "",
        str(payload.get("objective") or ""),
        "",
        f"- calibration_profile: {payload.get('calibration_profile')}",
        "",
        "## Headline Counts",
    ]
    for key, value in sorted((payload.get("headline_counts") or {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Protection Preservation"])
    for key, value in sorted((payload.get("protection_preservation") or {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Entry Model Summary"])
    for model, summary in sorted((payload.get("entry_model_summary") or {}).items()):
        lines.append(
            f"- {model}: accepted_rate={summary.get('accepted_rate')} accepted_count={summary.get('accepted_count')} "
            f"continuation_after_reject_rate={summary.get('continuation_after_reject_rate')}"
        )
    lines.extend(["", "## Entry Geometry Diagnostics"])
    geometry = payload.get("entry_geometry_diagnostics") or {}
    for key in [
        "false_cancellation_rate",
    ]:
        lines.append(f"- {key}: {geometry.get(key)}")
    for model, summary in sorted((geometry.get("geometry_summary") or {}).items()):
        lines.append(
            f"- {model}: accepted_entry_count={summary.get('accepted_entry_count')} "
            f"cancellation_count={summary.get('cancellation_count')} "
            f"median_closest_fill_distance_atr={summary.get('median_closest_fill_distance_atr')}"
        )
    lines.extend(["", "## Prefill Regime-Loss Audit"])
    prefill = payload.get("prefill_regime_loss_audit") or {}
    for key in [
        "event_count",
        "resumed_within_2_bars_count",
        "would_reach_participation_zone_count",
    ]:
        lines.append(f"- {key}: {prefill.get(key)}")
    lines.extend(["", "## Fast Pullback Classification"])
    fast_audit = geometry.get("fast_pullback_cancellation_audit") or {}
    for key in [
        "count",
        "continuation_rate",
        "less_strict_would_fill_count",
        "refined_shallow_would_fill_count",
    ]:
        lines.append(f"- {key}: {fast_audit.get(key)}")
    for key, value in sorted((fast_audit.get("classification_breakdown") or {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Pullback Class Outcomes"])
    for pullback_class, summary in sorted((geometry.get("pullback_class_outcomes") or {}).items()):
        lines.append(
            f"- {pullback_class}: evaluation_count={summary.get('evaluation_count')} "
            f"accepted_count={summary.get('accepted_count')} "
            f"continuation_after_reject_rate={summary.get('continuation_after_reject_rate')}"
        )
    lines.extend(["", "## Shallow Participation Trade Summary"])
    shallow_trade = geometry.get("shallow_participation_trade_summary") or {}
    for key in [
        "trade_count",
        "median_mfe_r",
        "median_mae_r",
    ]:
        lines.append(f"- {key}: {shallow_trade.get(key)}")
    lines.extend(["", "## Trade Matrix Summary"])
    for key, summary in sorted((payload.get("trade_matrix_summary") or {}).items()):
        lines.append(
            f"- {key}: trade_count={summary.get('trade_count')} median_gross_r={summary.get('median_gross_r')} "
            f"continuation_rate={summary.get('continuation_rate')}"
        )
    lines.extend(["", "## Invalidation Diagnostics"])
    invalidation = payload.get("invalidation_diagnostics") or {}
    for key in [
        "invalidated_before_fill_count",
        "continuation_reasserted_after_invalidation_count",
        "continuation_reasserted_after_invalidation_rate",
        "median_missed_favorable_excursion_r",
    ]:
        lines.append(f"- {key}: {invalidation.get(key)}")
    lines.extend(["", "## Premature Invalidation Audit"])
    prem = payload.get("premature_invalidation_audit") or {}
    for key in [
        "event_count",
        "resumed_after_invalidation_count",
        "resumed_after_invalidation_rate",
        "false_invalidation_rate",
        "median_bars_to_resumption",
        "median_post_invalidation_favorable_r",
    ]:
        lines.append(f"- {key}: {prem.get(key)}")
    lines.extend(["", "## Warning State Audit"])
    warn = payload.get("warning_state_audit") or {}
    for key in [
        "event_count",
        "recovered_count",
        "requalified_count",
        "invalidated_count",
        "timed_out_or_cleared_count",
        "recovered_rate",
        "continuation_after_recovery_rate",
        "median_bars_at_risk",
    ]:
        lines.append(f"- {key}: {warn.get(key)}")
    lines.extend(["", "## Recovery Requalification Audit"])
    recovery = payload.get("recovery_requalification_audit") or {}
    for key in [
        "resolution_count",
        "recovered_drift_count",
        "requalified_candidate_count",
        "continuation_after_recovery_count",
        "continuation_after_invalidation_count",
        "median_bars_at_risk_before_recovery",
    ]:
        lines.append(f"- {key}: {recovery.get(key)}")
    lines.extend(["", "## Pullback Veto Diagnostics"])
    pullback = payload.get("pullback_veto_diagnostics") or {}
    for key in [
        "accepted_pullback_count",
        "rejected_pullback_count",
        "continuation_after_accepted_pullback_rate",
        "continuation_after_rejected_pullback_rate",
        "false_veto_count",
    ]:
        lines.append(f"- {key}: {pullback.get(key)}")
    lines.extend(["", "## Handoff Diagnostics"])
    handoff = payload.get("handoff_diagnostics") or {}
    for key in [
        "session_timeout_trade_count",
        "timeout_appears_helpful_count",
        "timeout_appears_harmful_count",
        "timeout_appears_helpful_rate",
        "timeout_appears_harmful_rate",
    ]:
        lines.append(f"- {key}: {handoff.get(key)}")
    lines.extend(["", "## Interpretation"])
    for key, value in sorted((payload.get("interpretation") or {}).items()):
        lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
