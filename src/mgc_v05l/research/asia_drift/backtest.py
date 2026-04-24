"""Replay-only Asia Drift v1 Phase 2 orchestration."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Sequence

from ..trend_participation.models import ResearchBar
from .engine import AsiaDriftPhase1Run, run_asia_drift_phase1, run_asia_drift_phase1_from_bars
from .entries import (
    ENTRY_MODEL_CONFIRMATION_REACCEL,
    ENTRY_MODEL_LIMIT_LESS_PASSIVE,
    ENTRY_MODEL_LIMIT_PULLBACK,
    ENTRY_MODEL_SHALLOW_PARTICIPATION,
    ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
    PREFILL_PROFILE_RECOVERY_CONFIRMED,
    ENTRY_STATUS_CANCELLED,
    ENTRY_STATUS_ENTERED,
    assemble_entry_setups,
    estimate_stop_and_risk,
    evaluate_entry_models,
)
from .exits import default_exit_profiles, simulate_trades, summarize_exit_reasons
from .features import ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT, DISQUALIFYING_PULLBACK, RECOVERY_CONFIRMED
from .models import (
    AsiaDriftEntryEvaluation,
    AsiaDriftEntrySetup,
    AsiaDriftFeatureRow,
    AsiaDriftPhase2Artifacts,
    AsiaDriftSessionSummary,
    AsiaDriftTradeRecord,
)
from .report import write_phase2_artifacts
from .state_machine import STATE_CONFIRMED_INVALIDATION, STATE_DRIFT_AT_RISK, STATE_REQUALIFIED_CANDIDATE, STATE_RECOVERED_DRIFT


@dataclass(frozen=True)
class AsiaDriftPhase2Run:
    phase1_run: AsiaDriftPhase1Run
    entry_setups: list[AsiaDriftEntrySetup]
    entry_evaluations: list[AsiaDriftEntryEvaluation]
    trade_records: list[AsiaDriftTradeRecord]
    diagnostics: dict[str, Any]
    artifacts: AsiaDriftPhase2Artifacts


def run_asia_drift_phase2(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    instruments: tuple[str, ...] = ("MGC",),
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
    calibration_profile_name: str = RECOVERY_CONFIRMED,
    refined_prefill_profile_name: str = PREFILL_PROFILE_RECOVERY_CONFIRMED,
) -> AsiaDriftPhase2Run:
    phase1_run = run_asia_drift_phase1(
        source_sqlite_path=source_sqlite_path,
        output_dir=output_dir / "phase1",
        instruments=instruments,
        start_ts=start_ts,
        end_ts=end_ts,
        calibration_profile_name=calibration_profile_name,
    )
    return run_asia_drift_phase2_from_phase1(
        phase1_run=phase1_run,
        output_dir=output_dir,
        source_summary={
            "source_sqlite_path": str(source_sqlite_path.resolve()),
            "instruments": list(instruments),
            "start_ts": start_ts.isoformat() if start_ts is not None else None,
            "end_ts": end_ts.isoformat() if end_ts is not None else None,
            "phase": "phase2_replay_only_entry_exit_research",
            "calibration_profile": calibration_profile_name,
            "refined_prefill_profile": refined_prefill_profile_name,
        },
        refined_prefill_profile_name=refined_prefill_profile_name,
    )


def run_asia_drift_phase2_from_bars(
    *,
    output_dir: Path,
    bars_5m: Sequence[ResearchBar],
    bars_1m: Sequence[ResearchBar] | None = None,
    source_label: str = "synthetic",
    calibration_profile_name: str = RECOVERY_CONFIRMED,
    refined_prefill_profile_name: str = PREFILL_PROFILE_RECOVERY_CONFIRMED,
) -> AsiaDriftPhase2Run:
    phase1_run = run_asia_drift_phase1_from_bars(
        output_dir=output_dir / "phase1",
        bars_5m=bars_5m,
        bars_1m=bars_1m,
        source_label=source_label,
        calibration_profile_name=calibration_profile_name,
    )
    return run_asia_drift_phase2_from_phase1(
        phase1_run=phase1_run,
        output_dir=output_dir,
        source_summary={
            "source_label": source_label,
            "bar_count_1m": len(bars_1m or ()),
            "bar_count_5m": len(bars_5m),
            "phase": "phase2_replay_only_entry_exit_research",
            "calibration_profile": calibration_profile_name,
            "refined_prefill_profile": refined_prefill_profile_name,
        },
        refined_prefill_profile_name=refined_prefill_profile_name,
    )


def run_asia_drift_phase2_from_phase1(
    *,
    phase1_run: AsiaDriftPhase1Run,
    output_dir: Path,
    source_summary: dict[str, Any],
    refined_prefill_profile_name: str = PREFILL_PROFILE_RECOVERY_CONFIRMED,
) -> AsiaDriftPhase2Run:
    entry_setups = assemble_entry_setups(
        feature_rows=phase1_run.feature_rows,
        state_rows=phase1_run.state_rows,
    )
    entry_evaluations = evaluate_entry_models(
        feature_rows=phase1_run.feature_rows,
        state_rows=phase1_run.state_rows,
        setups=entry_setups,
        refined_prefill_profile_name=refined_prefill_profile_name,
    )
    trade_records = simulate_trades(
        feature_rows=phase1_run.feature_rows,
        setups=entry_setups,
        entry_evaluations=entry_evaluations,
        exit_profiles=default_exit_profiles(),
    )
    diagnostics = _build_phase2_diagnostics(
        feature_rows=phase1_run.feature_rows,
        session_summaries=phase1_run.session_summaries,
        entry_setups=entry_setups,
        entry_evaluations=entry_evaluations,
        trade_records=trade_records,
        source_summary=source_summary,
        phase1_run=phase1_run,
    )
    artifacts = write_phase2_artifacts(
        output_dir=output_dir,
        phase1_artifacts=phase1_run.artifacts,
        entry_setups=entry_setups,
        entry_evaluations=entry_evaluations,
        trade_records=trade_records,
        diagnostics=diagnostics,
    )
    return AsiaDriftPhase2Run(
        phase1_run=phase1_run,
        entry_setups=list(entry_setups),
        entry_evaluations=list(entry_evaluations),
        trade_records=list(trade_records),
        diagnostics=diagnostics,
        artifacts=artifacts,
    )


def _build_phase2_diagnostics(
    *,
    feature_rows: Sequence[AsiaDriftFeatureRow],
    session_summaries: Sequence[AsiaDriftSessionSummary],
    entry_setups: Sequence[AsiaDriftEntrySetup],
    entry_evaluations: Sequence[AsiaDriftEntryEvaluation],
    trade_records: Sequence[AsiaDriftTradeRecord],
    source_summary: dict[str, Any],
    phase1_run: AsiaDriftPhase1Run,
) -> dict[str, Any]:
    accepted_evals = [row for row in entry_evaluations if row.accepted]
    rejected_evals = [row for row in entry_evaluations if not row.accepted]
    invalidated_evals = [
        row
        for row in entry_evaluations
        if row.status == ENTRY_STATUS_CANCELLED
        and (row.cancellation_reason or "") not in {"latest_entry_time_passed", "session_timeout_boundary"}
    ]
    pullback_vetoes = _build_pullback_veto_events(feature_rows)
    cancellation_events = _build_entry_cancellation_events(
        feature_rows=feature_rows,
        state_rows=phase1_run.state_rows,
        entry_setups=entry_setups,
        entry_evaluations=entry_evaluations,
    )
    prefill_regime_loss_audit = _build_prefill_regime_loss_audit(
        feature_rows=feature_rows,
        entry_evaluations=entry_evaluations,
    )
    warning_state_events = _build_warning_state_events(feature_rows=feature_rows, state_rows=phase1_run.state_rows)
    recovered_events = [row for row in warning_state_events if row["resolution"] in {"RECOVERED", "REQUALIFIED"}]
    invalidation_events = _build_invalidation_events(feature_rows=feature_rows, state_rows=phase1_run.state_rows)
    setup_groups = _group_by(entry_evaluations, key=lambda row: row.setup_id)
    armed_never_entered = [
        {
            "setup_id": setup_id,
            "session_id": rows[0].asia_drift_session_id,
            "models": [row.entry_model for row in rows],
            "reasons": [row.cancellation_reason for row in rows if row.cancellation_reason],
            "missed_favorable_excursion_r": max(row.missed_favorable_excursion_r for row in rows),
            "continuation_reasserted_after_reject": any(row.continuation_reasserted_after_reject for row in rows),
        }
        for setup_id, rows in sorted(setup_groups.items())
        if not any(row.accepted for row in rows)
    ]
    entered_opportunities = [
        {
            "setup_id": row.setup_id,
            "entry_model": row.entry_model,
            "entry_ts": row.entry_ts.isoformat() if row.entry_ts is not None else None,
            "entry_price": row.entry_price,
            "reason_tags": list(row.reason_tags),
        }
        for row in accepted_evals
    ]
    invalidated_opportunities = [
        {
            "setup_id": row.setup_id,
            "entry_model": row.entry_model,
            "cancellation_reason": row.cancellation_reason,
            "continuation_reasserted_after_reject": row.continuation_reasserted_after_reject,
            "missed_favorable_excursion_r": row.missed_favorable_excursion_r,
        }
        for row in invalidated_evals
    ]
    false_vetoes = [row for row in pullback_vetoes if row["continuation_reasserted"]]
    accepted_setup_continuation = _accepted_setup_continuation(entry_setups=entry_setups, feature_rows=feature_rows)
    trade_model_summary = _summarize_trade_matrix(trade_records)
    handoff_trades = [row for row in trade_records if row.exit_reason == "session_timeout_handoff"]
    timeout_helped = sum(1 for row in handoff_trades if row.post_exit_deterioration_r >= 0.50)
    timeout_hurt = sum(1 for row in handoff_trades if row.post_exit_followthrough_r >= 0.50)

    diagnostics = {
        "module": "Asia Drift v1 Phase 2",
        "objective": (
            "Replay-only bounded entry/exit research layer built on Phase 1 state and feature artifacts to test "
            "whether the current strict drift engine is too conservative, whether pullback vetoes are justified, "
            "and which entry/exit families are structurally promising for later research."
        ),
        "calibration_profile": feature_rows[0].calibration_profile if feature_rows else None,
        "source_summary": source_summary,
        "phase1_artifact_paths": {
            "summary_json_path": str(phase1_run.artifacts.summary_json_path),
            "summary_markdown_path": str(phase1_run.artifacts.summary_markdown_path),
            "feature_rows_path": str(phase1_run.artifacts.feature_rows_path),
            "state_rows_path": str(phase1_run.artifacts.state_rows_path),
            "session_summaries_path": str(phase1_run.artifacts.session_summaries_path),
            "candidate_manifest_path": str(phase1_run.artifacts.candidate_manifest_path),
        },
        "headline_counts": {
            "candidate_sessions": len(session_summaries),
            "entry_setups": len(entry_setups),
            "entry_evaluations": len(entry_evaluations),
            "accepted_entries": len(accepted_evals),
            "armed_but_never_entered": len(armed_never_entered),
            "trade_records": len(trade_records),
            "pullback_veto_events": len(pullback_vetoes),
        },
        "protection_preservation": {
            "chop_veto_bar_count": sum(1 for row in feature_rows if row.chop_veto),
            "post_spike_bar_count": sum(1 for row in feature_rows if row.post_spike_instability),
        },
        "entry_model_summary": _summarize_entry_models(entry_evaluations),
        "refined_prefill_profile": source_summary.get("refined_prefill_profile"),
        "prefill_regime_loss_audit": prefill_regime_loss_audit,
        "entry_geometry_diagnostics": _build_entry_geometry_diagnostics(
            cancellation_events,
            entry_evaluations,
            feature_rows,
            trade_records,
        ),
        "trade_matrix_summary": trade_model_summary,
        "exit_reason_distribution": summarize_exit_reasons(trade_records),
        "invalidation_diagnostics": {
            "invalidated_before_fill_count": len(invalidated_evals),
            "continuation_reasserted_after_invalidation_count": sum(
                1 for row in invalidated_evals if row.continuation_reasserted_after_reject
            ),
            "continuation_reasserted_after_invalidation_rate": _rate(
                sum(1 for row in invalidated_evals if row.continuation_reasserted_after_reject),
                len(invalidated_evals),
            ),
            "median_missed_favorable_excursion_r": _median([row.missed_favorable_excursion_r for row in invalidated_evals]),
            "sample_invalidated_opportunities": invalidated_opportunities[:10],
        },
        "premature_invalidation_audit": {
            "event_count": len(invalidation_events),
            "resumed_after_invalidation_count": sum(1 for row in invalidation_events if row["drift_resumed"]),
            "resumed_after_invalidation_rate": _rate(
                sum(1 for row in invalidation_events if row["drift_resumed"]),
                len(invalidation_events),
            ),
            "false_invalidation_rate": _rate(
                sum(1 for row in invalidation_events if row["drift_resumed"]),
                len(invalidation_events),
            ),
            "invalidation_reason_counts": dict(Counter(row["invalidation_reason"] for row in invalidation_events)),
            "invalidation_category_counts": dict(Counter(row["invalidation_category"] for row in invalidation_events)),
            "median_bars_to_resumption": _median([row["bars_to_resumption"] for row in invalidation_events]),
            "median_post_invalidation_favorable_r": _median([row["max_favorable_continuation_r"] for row in invalidation_events]),
            "sample_events": invalidation_events[:10],
        },
        "warning_state_audit": {
            "event_count": len(warning_state_events),
            "recovered_count": sum(1 for row in warning_state_events if row["resolution"] == "RECOVERED"),
            "requalified_count": sum(1 for row in warning_state_events if row["resolution"] == "REQUALIFIED"),
            "invalidated_count": sum(1 for row in warning_state_events if row["resolution"] == "INVALIDATED"),
            "timed_out_or_cleared_count": sum(
                1 for row in warning_state_events if row["resolution"] in {"TIMEOUT", "SESSION_CLEARED"}
            ),
            "recovered_rate": _rate(
                sum(1 for row in warning_state_events if row["resolution"] in {"RECOVERED", "REQUALIFIED"}),
                len(warning_state_events),
            ),
            "median_bars_at_risk": _median([row["bars_at_risk"] for row in warning_state_events]),
            "continuation_after_recovery_rate": _rate(
                sum(1 for row in recovered_events if row["continuation_after_resolution"]),
                len(recovered_events),
            ),
            "sample_events": warning_state_events[:10],
        },
        "recovery_requalification_audit": {
            "resolution_count": len(warning_state_events),
            "recovered_drift_count": sum(1 for row in warning_state_events if row["resolution"] == "RECOVERED"),
            "requalified_candidate_count": sum(1 for row in warning_state_events if row["resolution"] == "REQUALIFIED"),
            "continuation_after_recovery_count": sum(
                1 for row in warning_state_events if row["resolution"] in {"RECOVERED", "REQUALIFIED"} and row["continuation_after_resolution"]
            ),
            "continuation_after_invalidation_count": sum(
                1 for row in warning_state_events if row["resolution"] == "INVALIDATED" and row["continuation_after_resolution"]
            ),
            "median_bars_at_risk_before_recovery": _median(
                [row["bars_at_risk"] for row in warning_state_events if row["resolution"] in {"RECOVERED", "REQUALIFIED"}]
            ),
            "sample_events": warning_state_events[:10],
        },
        "pullback_veto_diagnostics": {
            "accepted_pullback_count": len(accepted_setup_continuation),
            "rejected_pullback_count": len(pullback_vetoes),
            "continuation_after_accepted_pullback_rate": _rate(
                sum(1 for row in accepted_setup_continuation if row["continuation_reasserted"]),
                len(accepted_setup_continuation),
            ),
            "continuation_after_rejected_pullback_rate": _rate(
                sum(1 for row in pullback_vetoes if row["continuation_reasserted"]),
                len(pullback_vetoes),
            ),
            "false_veto_count": len(false_vetoes),
            "veto_reason_counts": dict(Counter(row["veto_reason"] for row in pullback_vetoes)),
            "veto_category_counts": dict(Counter(row["veto_category"] for row in pullback_vetoes)),
            "sample_false_vetoes": false_vetoes[:10],
        },
        "handoff_diagnostics": {
            "session_timeout_trade_count": len(handoff_trades),
            "timeout_appears_helpful_count": timeout_helped,
            "timeout_appears_harmful_count": timeout_hurt,
            "timeout_appears_helpful_rate": _rate(timeout_helped, len(handoff_trades)),
            "timeout_appears_harmful_rate": _rate(timeout_hurt, len(handoff_trades)),
        },
        "artifacts_for_review": {
            "armed_but_never_entered": armed_never_entered[:15],
            "entered_opportunities": entered_opportunities[:15],
            "pullback_vetoes": pullback_vetoes[:15],
            "false_vetoes": false_vetoes[:15],
            "entry_cancellations": cancellation_events[:15],
        },
        "interpretation": _interpret_phase2(
            entry_evaluations=entry_evaluations,
            trade_records=trade_records,
            pullback_vetoes=pullback_vetoes,
            invalidated_evals=invalidated_evals,
        ),
    }
    return diagnostics


def _summarize_entry_models(
    entry_evaluations: Sequence[AsiaDriftEntryEvaluation],
) -> dict[str, dict[str, Any]]:
    grouped = _group_by(entry_evaluations, key=lambda row: row.entry_model)
    summary: dict[str, dict[str, Any]] = {}
    for model, rows in sorted(grouped.items()):
        accepted = [row for row in rows if row.accepted]
        rejected = [row for row in rows if not row.accepted]
        cancellation_counts = Counter((row.cancellation_reason or "none") for row in rows)
        summary[model] = {
            "evaluation_count": len(rows),
            "accepted_count": len(accepted),
            "accepted_rate": _rate(len(accepted), len(rows)),
            "rejected_or_expired_count": len(rejected),
            "continuation_after_reject_rate": _rate(
                sum(1 for row in rejected if row.continuation_reasserted_after_reject),
                len(rejected),
            ),
            "median_missed_favorable_excursion_r": _median([row.missed_favorable_excursion_r for row in rejected]),
            "cancellation_reason_counts": dict(cancellation_counts),
        }
    return summary


def _build_entry_geometry_diagnostics(
    cancellation_events: Sequence[dict[str, Any]],
    entry_evaluations: Sequence[AsiaDriftEntryEvaluation],
    feature_rows: Sequence[AsiaDriftFeatureRow],
    trade_records: Sequence[AsiaDriftTradeRecord],
) -> dict[str, Any]:
    grouped = _group_by(cancellation_events, key=lambda row: row["entry_model"])
    geometry_summary: dict[str, Any] = {}
    for model, rows in sorted(grouped.items()):
        geometry_summary[model] = {
            "cancellation_count": len(rows),
            "false_cancellation_rate": _rate(sum(1 for row in rows if row["continuation_without_fill"]), len(rows)),
            "median_closest_fill_distance_atr": _median([row["closest_fill_distance_atr"] for row in rows]),
            "accepted_entry_count": sum(1 for row in entry_evaluations if row.entry_model == model and row.accepted),
            "cancellation_reason_counts": dict(Counter(row["cancellation_reason"] for row in rows)),
        }

    reexpanded = [row for row in cancellation_events if row["cancellation_reason"] == "reexpanded_without_fill"]
    fast_pullbacks = [row for row in cancellation_events if row["cancellation_reason"] == "pullback_too_fast"]
    depth_rows = [row for row in cancellation_events if row["cancellation_reason"] in {"depth_warning", "depth_exceeds_limit"}]
    pullback_class_outcomes = _pullback_class_outcomes(feature_rows, entry_evaluations)
    shallow_trade_records = [
        row
        for row in trade_records
        if row.entry_model in {ENTRY_MODEL_SHALLOW_PARTICIPATION, ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED}
    ]
    return {
        "geometry_summary": geometry_summary,
        "fill_proximity_distribution": _fill_proximity_distribution(cancellation_events),
        "cancellation_reason_breakdown": dict(Counter(row["cancellation_reason"] for row in cancellation_events)),
        "hypothetical_fill_counts_by_geometry": {
            model: sum(1 for row in entry_evaluations if row.entry_model == model and row.accepted)
            for model in sorted({row.entry_model for row in entry_evaluations})
        },
        "false_cancellation_rate": _rate(
            sum(1 for row in cancellation_events if row["continuation_without_fill"]),
            len(cancellation_events),
        ),
        "reexpanded_without_fill_audit": {
            "count": len(reexpanded),
            "median_closest_fill_distance_atr": _median([row["closest_fill_distance_atr"] for row in reexpanded]),
            "less_passive_would_fill_count": sum(1 for row in reexpanded if row["less_passive_limit_would_fill"]),
            "confirmation_would_fill_count": sum(1 for row in reexpanded if row["confirmation_would_fill"]),
            "shallow_participation_would_fill_count": sum(1 for row in reexpanded if row["shallow_participation_would_fill"]),
            "sample_events": reexpanded[:10],
        },
        "fast_pullback_cancellation_audit": {
            "count": len(fast_pullbacks),
            "continuation_rate": _rate(sum(1 for row in fast_pullbacks if row["continuation_without_fill"]), len(fast_pullbacks)),
            "less_strict_would_fill_count": sum(1 for row in fast_pullbacks if row["less_strict_entry_would_fill"]),
            "refined_shallow_would_fill_count": sum(1 for row in fast_pullbacks if row["refined_shallow_would_fill"]),
            "classification_breakdown": dict(Counter(row["fast_pullback_class"] for row in fast_pullbacks)),
            "sample_events": fast_pullbacks[:10],
        },
        "depth_cancellation_audit": {
            "count": len(depth_rows),
            "bucket_summary": _depth_bucket_summary(depth_rows),
            "continuation_by_fast_class": {
                pullback_class: {
                    "count": len(class_rows),
                    "continuation_rate": _rate(
                        sum(1 for row in class_rows if row["continuation_without_fill"]),
                        len(class_rows),
                    ),
                }
                for pullback_class, class_rows in sorted(_group_by(depth_rows, key=lambda row: row["fast_pullback_class"]).items())
            },
            "sample_events": depth_rows[:10],
        },
        "pullback_class_outcomes": pullback_class_outcomes,
        "shallow_participation_trade_summary": {
            "trade_count": len(shallow_trade_records),
            "median_mfe_r": _median([row.mfe_r for row in shallow_trade_records]),
            "median_mae_r": _median([row.mae_r for row in shallow_trade_records]),
            "exit_reason_distribution": dict(Counter(row.exit_reason for row in shallow_trade_records)),
            "sample_trades": [
                {
                    "trade_id": row.trade_id,
                    "entry_model": row.entry_model,
                    "gross_r": row.gross_r,
                    "mfe_r": row.mfe_r,
                    "mae_r": row.mae_r,
                    "exit_reason": row.exit_reason,
                }
                for row in shallow_trade_records[:10]
            ],
        },
        "remaining_cancellation_reasons": {
            model: rows["cancellation_reason_counts"]
            for model, rows in geometry_summary.items()
        },
        "sample_cancellations": list(cancellation_events[:15]),
    }


def _build_prefill_regime_loss_audit(
    *,
    feature_rows: Sequence[AsiaDriftFeatureRow],
    entry_evaluations: Sequence[AsiaDriftEntryEvaluation],
) -> dict[str, Any]:
    features_by_session = _group_by(
        sorted(feature_rows, key=lambda row: (row.asia_drift_session_id, row.decision_ts)),
        key=lambda row: row.asia_drift_session_id,
    )
    events: list[dict[str, Any]] = []
    for evaluation in entry_evaluations:
        if evaluation.entry_model != ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED:
            continue
        if evaluation.cancellation_reason != "regime_lost_before_fill":
            continue
        session_rows = features_by_session.get(evaluation.asia_drift_session_id, [])
        cancel_index = next(
            (index for index, row in enumerate(session_rows) if row.decision_ts == evaluation.evaluation_end_ts),
            None,
        )
        if cancel_index is None:
            continue
        cancel_row = session_rows[cancel_index]
        future_rows = [row for row in session_rows[cancel_index + 1 :] if row.in_scope]
        if evaluation.direction == "LONG":
            bars_to_resume = next(
                (idx for idx, row in enumerate(future_rows, start=1) if row.regime == ASIA_DRIFT_LONG),
                None,
            )
            would_reach_zone = any(row.low <= (evaluation.candidate_entry_price or cancel_row.close) for row in future_rows)
            max_favorable = max((row.high - (evaluation.candidate_entry_price or cancel_row.close) for row in future_rows), default=0.0)
        else:
            bars_to_resume = next(
                (idx for idx, row in enumerate(future_rows, start=1) if row.regime == ASIA_DRIFT_SHORT),
                None,
            )
            would_reach_zone = any(row.high >= (evaluation.candidate_entry_price or cancel_row.close) for row in future_rows)
            max_favorable = max(((evaluation.candidate_entry_price or cancel_row.close) - row.low for row in future_rows), default=0.0)
        warning_source = _reason_tag_value(evaluation.reason_tags, "prefill_warning_source:")
        warning_bars = _reason_tag_value(evaluation.reason_tags, "prefill_warning_bars:")
        events.append(
            {
                "setup_id": evaluation.setup_id,
                "asia_drift_session_id": evaluation.asia_drift_session_id,
                "bars_from_setup_to_cancellation": evaluation.bars_waited,
                "bars_to_regime_resumption": bars_to_resume,
                "drift_resumed_within_2_bars": bars_to_resume is not None and bars_to_resume <= 2,
                "would_reach_participation_zone_after_cancellation": would_reach_zone,
                "max_favorable_continuation_r_after_cancellation": evaluation.missed_favorable_excursion_r,
                "max_favorable_continuation_points_after_cancellation": max_favorable,
                "regime_loss_source_category": warning_source or _prefill_source_category_from_row(cancel_row),
                "setup_pullback_class": cancel_row.fast_pullback_class if cancel_row.fast_pullback_class != "SLOW_OR_STANDARD" else cancel_row.pullback_state,
                "warning_bars_before_cancellation": int(warning_bars) if warning_bars is not None else 0,
                "pullback_vwap_interaction": cancel_row.pullback_vwap_interaction,
                "regime_persistence_label": cancel_row.regime_persistence_label,
            }
        )
    return {
        "event_count": len(events),
        "resumed_within_2_bars_count": sum(1 for row in events if row["drift_resumed_within_2_bars"]),
        "would_reach_participation_zone_count": sum(
            1 for row in events if row["would_reach_participation_zone_after_cancellation"]
        ),
        "source_category_counts": dict(Counter(row["regime_loss_source_category"] for row in events)),
        "setup_pullback_class_counts": dict(Counter(row["setup_pullback_class"] for row in events)),
        "sample_events": events[:15],
    }


def _summarize_trade_matrix(
    trade_records: Sequence[AsiaDriftTradeRecord],
) -> dict[str, dict[str, Any]]:
    grouped = _group_by(trade_records, key=lambda row: f"{row.entry_model}__{row.exit_profile}")
    summary: dict[str, dict[str, Any]] = {}
    for key, rows in sorted(grouped.items()):
        summary[key] = {
            "trade_count": len(rows),
            "median_gross_r": _median([row.gross_r for row in rows]),
            "positive_gross_r_rate": _rate(sum(1 for row in rows if row.gross_r > 0.0), len(rows)),
            "continuation_rate": _rate(sum(1 for row in rows if row.continuation_achieved), len(rows)),
            "median_time_to_follow_through_bars": _median(
                [row.time_to_follow_through_bars for row in rows if row.time_to_follow_through_bars is not None]
            ),
            "median_time_to_failure_bars": _median(
                [row.time_to_failure_bars for row in rows if row.time_to_failure_bars is not None]
            ),
            "exit_reason_counts": dict(Counter(row.exit_reason for row in rows)),
        }
    return summary


def _build_pullback_veto_events(
    feature_rows: Sequence[AsiaDriftFeatureRow],
) -> list[dict[str, Any]]:
    grouped = _group_by(
        sorted(feature_rows, key=lambda row: (row.asia_drift_session_id, row.decision_ts)),
        key=lambda row: row.asia_drift_session_id,
    )
    events: list[dict[str, Any]] = []
    for session_id, rows in sorted(grouped.items()):
        previous_state = None
        for index, row in enumerate(rows):
            if row.regime not in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}:
                previous_state = row.pullback_state
                continue
            if row.pullback_state != DISQUALIFYING_PULLBACK or previous_state == DISQUALIFYING_PULLBACK:
                previous_state = row.pullback_state
                continue
            reference_price = row.close
            stop_price, risk_points = estimate_stop_and_risk(
                setup=AsiaDriftEntrySetup(
                    setup_id=f"{session_id}__veto_probe",
                    calibration_profile=row.calibration_profile,
                    instrument=row.instrument,
                    timeframe=row.timeframe,
                    asia_drift_session_id=row.asia_drift_session_id,
                    local_session_date=row.local_session_date,
                    direction="LONG" if row.regime == ASIA_DRIFT_LONG else "SHORT",
                    regime=row.regime,
                    drift_strength=row.long_drift_strength if row.regime == ASIA_DRIFT_LONG else row.short_drift_strength,
                    armed_ts=row.decision_ts,
                    armed_subphase=row.subphase,
                    armed_session_bar_index=row.session_bar_index,
                    armed_transition_reason="pullback_veto_probe",
                    limit_expiry_ts=None,
                    confirmation_expiry_ts=None,
                    session_timeout_ts=row.decision_ts,
                    latest_entry_ts=row.decision_ts,
                    entry_zone_low=row.envelope_low,
                    entry_zone_high=row.envelope_high,
                    entry_limit_price=row.close,
                    entry_zone_valid=True,
                    pullback_pivot_price=row.high if row.regime == ASIA_DRIFT_LONG else row.low,
                    protected_swing_price=row.protected_swing_price,
                    drift_leg_extreme=row.drift_leg_extreme,
                    atr=row.atr,
                    scope_provenance=row.scope_provenance,
                    feature_version=row.feature_version,
                ),
                entry_price=reference_price,
            )
            future_rows = [future for future in rows[index + 1 :] if future.in_scope]
            if row.regime == ASIA_DRIFT_LONG:
                continuation_index = next(
                    (future_index for future_index, future in enumerate(future_rows, start=1) if future.high >= row.drift_leg_extreme + 0.10 * row.atr),
                    None,
                )
                continuation = continuation_index is not None
                best_points = max((future.high - reference_price for future in future_rows), default=0.0)
            else:
                continuation_index = next(
                    (future_index for future_index, future in enumerate(future_rows, start=1) if future.low <= row.drift_leg_extreme - 0.10 * row.atr),
                    None,
                )
                continuation = continuation_index is not None
                best_points = max((reference_price - future.low for future in future_rows), default=0.0)
            events.append(
                {
                    "calibration_profile": row.calibration_profile,
                    "asia_drift_session_id": session_id,
                    "decision_ts": row.decision_ts.isoformat(),
                    "direction": "LONG" if row.regime == ASIA_DRIFT_LONG else "SHORT",
                    "veto_reason": row.pullback_reason,
                    "veto_category": row.pullback_veto_category,
                    "continuation_reasserted": continuation,
                    "bars_to_continuation": continuation_index,
                    "best_future_excursion_r": max(best_points, 0.0) / max(risk_points, 1e-9),
                    "best_future_excursion_points": max(best_points, 0.0),
                    "pullback_depth_fraction": row.pullback_depth_fraction,
                    "pullback_depth_atr": row.pullback_depth_atr,
                    "pullback_speed": row.pullback_speed,
                    "pullback_expansion_ratio": row.pullback_expansion_ratio,
                    "pullback_vwap_interaction": row.pullback_vwap_interaction,
                    "pullback_structure_break": row.pullback_structure_break,
                }
            )
            previous_state = row.pullback_state
    return events


def _build_entry_cancellation_events(
    *,
    feature_rows: Sequence[AsiaDriftFeatureRow],
    state_rows: Sequence[Any],
    entry_setups: Sequence[AsiaDriftEntrySetup],
    entry_evaluations: Sequence[AsiaDriftEntryEvaluation],
) -> list[dict[str, Any]]:
    features_by_session = _group_by(
        sorted(feature_rows, key=lambda row: (row.asia_drift_session_id, row.decision_ts)),
        key=lambda row: row.asia_drift_session_id,
    )
    state_by_key = {(row.asia_drift_session_id, row.decision_ts): row for row in state_rows}
    setups_by_id = {row.setup_id: row for row in entry_setups}
    evals_by_setup = _group_by(entry_evaluations, key=lambda row: row.setup_id)
    events: list[dict[str, Any]] = []

    for evaluation in entry_evaluations:
        if evaluation.accepted:
            continue
        setup = setups_by_id.get(evaluation.setup_id)
        if setup is None:
            continue
        session_rows = features_by_session.get(setup.asia_drift_session_id, [])
        armed_index = next((idx for idx, row in enumerate(session_rows) if row.decision_ts == setup.armed_ts), None)
        if armed_index is None:
            continue
        start_index, end_index = _evaluation_window_indices(
            model=evaluation.entry_model,
            armed_index=armed_index,
            session_rows=session_rows,
            evaluation=evaluation,
        )
        validity_rows = session_rows[start_index : end_index + 1] if start_index <= end_index else []
        candidate_price = evaluation.candidate_entry_price or setup.entry_limit_price or session_rows[armed_index].close
        cancellation_row = session_rows[end_index] if 0 <= end_index < len(session_rows) else session_rows[armed_index]
        less_passive_eval = _lookup_sibling_eval(evals_by_setup, evaluation.setup_id, ENTRY_MODEL_LIMIT_LESS_PASSIVE)
        confirmation_eval = _lookup_sibling_eval(evals_by_setup, evaluation.setup_id, ENTRY_MODEL_CONFIRMATION_REACCEL)
        shallow_eval = _lookup_sibling_eval(evals_by_setup, evaluation.setup_id, ENTRY_MODEL_SHALLOW_PARTICIPATION)
        refined_shallow_eval = _lookup_sibling_eval(
            evals_by_setup,
            evaluation.setup_id,
            ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
        )
        closest_distance_points, closest_distance_atr, best_fill_price = _fill_proximity(
            direction=setup.direction,
            candidate_price=candidate_price,
            validity_rows=validity_rows,
            atr=setup.atr,
        )
        bars_to_reexpansion = _bars_to_reexpansion(setup=setup, session_rows=session_rows, armed_index=armed_index)
        bars_to_regime_loss = _bars_to_regime_loss(
            setup=setup,
            session_rows=session_rows,
            state_by_key=state_by_key,
            armed_index=armed_index,
        )
        events.append(
            {
                "setup_id": evaluation.setup_id,
                "entry_model": evaluation.entry_model,
                "calibration_profile": evaluation.calibration_profile,
                "cancellation_reason": evaluation.cancellation_reason,
                "best_available_fill_price": best_fill_price,
                "candidate_entry_price": candidate_price,
                "window_low": min((row.low for row in validity_rows), default=None),
                "window_high": max((row.high for row in validity_rows), default=None),
                "closest_fill_distance_points": closest_distance_points,
                "closest_fill_distance_atr": closest_distance_atr,
                "continuation_without_fill": evaluation.continuation_reasserted_after_reject,
                "less_strict_entry_would_fill": any(
                    sibling is not None and sibling.accepted
                    for sibling in (less_passive_eval, shallow_eval, refined_shallow_eval)
                ),
                "less_passive_limit_would_fill": bool(less_passive_eval and less_passive_eval.accepted),
                "confirmation_would_fill": bool(confirmation_eval and confirmation_eval.accepted),
                "shallow_participation_would_fill": bool(shallow_eval and shallow_eval.accepted),
                "refined_shallow_would_fill": bool(refined_shallow_eval and refined_shallow_eval.accepted),
                "bars_to_reexpansion": bars_to_reexpansion,
                "bars_to_regime_loss": bars_to_regime_loss,
                "max_favorable_excursion_after_cancellation": evaluation.missed_favorable_excursion_points,
                "max_favorable_excursion_r_after_cancellation": evaluation.missed_favorable_excursion_r,
                "pullback_depth_atr": cancellation_row.pullback_depth_atr,
                "pullback_depth_fraction": cancellation_row.pullback_depth_fraction,
                "depth_bucket": _depth_bucket(cancellation_row),
                "pullback_speed": cancellation_row.pullback_speed,
                "fast_pullback_class": cancellation_row.fast_pullback_class,
                "recovery_score": cancellation_row.recovery_score,
                "reexpanded_without_fill": evaluation.cancellation_reason == "reexpanded_without_fill",
                "evaluation_start_ts": evaluation.evaluation_start_ts.isoformat() if evaluation.evaluation_start_ts else None,
                "evaluation_end_ts": evaluation.evaluation_end_ts.isoformat() if evaluation.evaluation_end_ts else None,
            }
        )
    return events


def _pullback_class_outcomes(
    feature_rows: Sequence[AsiaDriftFeatureRow],
    entry_evaluations: Sequence[AsiaDriftEntryEvaluation],
) -> dict[str, dict[str, Any]]:
    feature_by_key = {
        (row.asia_drift_session_id, row.decision_ts): row
        for row in feature_rows
    }
    grouped: dict[str, list[AsiaDriftEntryEvaluation]] = defaultdict(list)
    for evaluation in entry_evaluations:
        event_ts = evaluation.entry_ts or evaluation.evaluation_end_ts
        if event_ts is None:
            continue
        feature = feature_by_key.get((evaluation.asia_drift_session_id, event_ts))
        if feature is None:
            continue
        grouped[feature.fast_pullback_class].append(evaluation)
    return {
        pullback_class: {
            "evaluation_count": len(rows),
            "accepted_count": sum(1 for row in rows if row.accepted),
            "accepted_rate": _rate(sum(1 for row in rows if row.accepted), len(rows)),
            "rejected_count": sum(1 for row in rows if not row.accepted),
            "continuation_after_reject_rate": _rate(
                sum(1 for row in rows if not row.accepted and row.continuation_reasserted_after_reject),
                sum(1 for row in rows if not row.accepted),
            ),
            "entry_model_counts": dict(Counter(row.entry_model for row in rows)),
        }
        for pullback_class, rows in sorted(grouped.items())
    }


def _build_invalidation_events(
    *,
    feature_rows: Sequence[AsiaDriftFeatureRow],
    state_rows: Sequence[Any],
) -> list[dict[str, Any]]:
    features_by_session = _group_by(
        sorted(feature_rows, key=lambda row: (row.asia_drift_session_id, row.decision_ts)),
        key=lambda row: row.asia_drift_session_id,
    )
    state_by_key = {
        (row.asia_drift_session_id, row.decision_ts): row
        for row in state_rows
    }
    events: list[dict[str, Any]] = []
    for session_id, rows in sorted(features_by_session.items()):
        previous_invalidated = False
        for index, row in enumerate(rows):
            state = state_by_key.get((session_id, row.decision_ts))
            if state is None:
                continue
            if state.state != STATE_CONFIRMED_INVALIDATION:
                previous_invalidated = False
                continue
            if previous_invalidated:
                continue
            previous_invalidated = True
            future_rows = [future for future in rows[index + 1 :] if future.in_scope]
            if row.regime == ASIA_DRIFT_LONG or row.dominant_direction == "LONG":
                resumption_index = next(
                    (future_index for future_index, future in enumerate(future_rows, start=1) if future.high >= row.drift_leg_extreme + 0.10 * row.atr),
                    None,
                )
                best_points = max((future.high - row.close for future in future_rows), default=0.0)
            else:
                resumption_index = next(
                    (future_index for future_index, future in enumerate(future_rows, start=1) if future.low <= row.drift_leg_extreme - 0.10 * row.atr),
                    None,
                )
                best_points = max((row.close - future.low for future in future_rows), default=0.0)
            category = _invalidation_category(
                reason=state.transition_reason,
                row=row,
            )
            events.append(
                {
                    "calibration_profile": row.calibration_profile,
                    "asia_drift_session_id": session_id,
                    "decision_ts": row.decision_ts.isoformat(),
                    "invalidation_reason": state.transition_reason,
                    "invalidation_category": category,
                    "drift_resumed": resumption_index is not None,
                    "bars_to_resumption": resumption_index,
                    "bars_in_at_risk_before_invalidation": state.bars_in_at_risk_state,
                    "at_risk_reason": state.at_risk_reason,
                    "max_favorable_continuation_points": max(best_points, 0.0),
                    "max_favorable_continuation_r": max(best_points, 0.0) / max(max(row.atr, 1e-9), 1e-9),
                    "triggered_by_single_bar_noise": state.transition_reason in {"pullback_too_fast", "single_close_through_vwap", "regime_lost_after_candidate"},
                    "triggered_by_vwap_reclaim": category == "VWAP_RECLAIM",
                    "triggered_by_ema_failure": category == "EMA_FAILURE",
                    "triggered_by_drift_score_collapse": category == "DRIFT_COLLAPSE",
                    "pullback_vwap_interaction": row.pullback_vwap_interaction,
                    "pullback_warning_reason": row.pullback_warning_reason,
                }
            )
    return events


def _evaluation_window_indices(
    *,
    model: str,
    armed_index: int,
    session_rows: Sequence[AsiaDriftFeatureRow],
    evaluation: AsiaDriftEntryEvaluation,
) -> tuple[int, int]:
    if model == ENTRY_MODEL_CONFIRMATION_REACCEL:
        start_index = min(armed_index + 4, len(session_rows) - 1)
        default_end = min(armed_index + 5, len(session_rows) - 1)
    else:
        start_index = min(armed_index + 1, len(session_rows) - 1)
        default_end = min(armed_index + 3, len(session_rows) - 1)
    if evaluation.evaluation_end_ts is None:
        return start_index, default_end
    end_index = next((idx for idx, row in enumerate(session_rows) if row.decision_ts == evaluation.evaluation_end_ts), default_end)
    return start_index, max(start_index, end_index)


def _lookup_sibling_eval(
    grouped: dict[str, list[AsiaDriftEntryEvaluation]],
    setup_id: str,
    model: str,
) -> AsiaDriftEntryEvaluation | None:
    return next((row for row in grouped.get(setup_id, []) if row.entry_model == model), None)


def _fill_proximity(
    *,
    direction: str,
    candidate_price: float,
    validity_rows: Sequence[AsiaDriftFeatureRow],
    atr: float,
) -> tuple[float | None, float | None, float | None]:
    if not validity_rows:
        return None, None, None
    if direction == "LONG":
        best_fill_price = min(row.low for row in validity_rows)
        shortfall = max(best_fill_price - candidate_price, 0.0)
    else:
        best_fill_price = max(row.high for row in validity_rows)
        shortfall = max(candidate_price - best_fill_price, 0.0)
    return shortfall, shortfall / max(atr, 1e-9), best_fill_price


def _bars_to_reexpansion(
    *,
    setup: AsiaDriftEntrySetup,
    session_rows: Sequence[AsiaDriftFeatureRow],
    armed_index: int,
) -> int | None:
    future_rows = session_rows[armed_index + 1 :]
    if setup.direction == "LONG":
        for offset, row in enumerate(future_rows, start=1):
            if row.close > setup.drift_leg_extreme + 0.15 * setup.atr:
                return offset
        return None
    for offset, row in enumerate(future_rows, start=1):
        if row.close < setup.drift_leg_extreme - 0.15 * setup.atr:
            return offset
    return None


def _bars_to_regime_loss(
    *,
    setup: AsiaDriftEntrySetup,
    session_rows: Sequence[AsiaDriftFeatureRow],
    state_by_key: dict[tuple[str, datetime], Any],
    armed_index: int,
) -> int | None:
    for offset, row in enumerate(session_rows[armed_index + 1 :], start=1):
        state = state_by_key.get((setup.asia_drift_session_id, row.decision_ts))
        if row.regime != setup.regime or (state is not None and state.state == STATE_CONFIRMED_INVALIDATION):
            return offset
    return None


def _fill_proximity_distribution(cancellation_events: Sequence[dict[str, Any]]) -> dict[str, int]:
    buckets = Counter()
    for row in cancellation_events:
        value = row.get("closest_fill_distance_atr")
        if value is None:
            buckets["unknown"] += 1
        elif value <= 0.0:
            buckets["at_or_through_limit"] += 1
        elif value <= 0.10:
            buckets["within_0.10_atr"] += 1
        elif value <= 0.25:
            buckets["within_0.25_atr"] += 1
        else:
            buckets["beyond_0.25_atr"] += 1
    return dict(buckets)


def _depth_bucket(row: AsiaDriftFeatureRow) -> str:
    if row.pullback_depth_atr <= 0.75:
        return "shallow_to_moderate"
    if row.pullback_depth_atr <= 1.10:
        return "moderately_deep"
    if row.pullback_depth_atr <= 1.45:
        return "deep_but_researchable"
    return "severe_damage"


def _depth_bucket_summary(rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped = _group_by(rows, key=lambda row: row["depth_bucket"])
    return {
        bucket: {
            "count": len(bucket_rows),
            "continuation_rate": _rate(sum(1 for row in bucket_rows if row["continuation_without_fill"]), len(bucket_rows)),
            "median_closest_fill_distance_atr": _median([row["closest_fill_distance_atr"] for row in bucket_rows]),
        }
        for bucket, bucket_rows in sorted(grouped.items())
    }


def _reason_tag_value(reason_tags: Sequence[str], prefix: str) -> str | None:
    for tag in reason_tags:
        if tag.startswith(prefix):
            return tag[len(prefix) :]
    return None


def _prefill_source_category_from_row(row: AsiaDriftFeatureRow) -> str:
    vwap_damage = row.pullback_vwap_interaction in {"SINGLE_CLOSE_THROUGH_VWAP", "CONFIRMED_CLOSES_THROUGH_VWAP", "CLOSE_THROUGH_VWAP_AND_SLOW_EMA"}
    structure_damage = row.pullback_structure_break
    collapse = row.regime_persistence_label == "COLLAPSING" or row.recovery_score < 0.45
    active = sum(bool(flag) for flag in (vwap_damage, structure_damage, collapse))
    if active >= 2:
        return "combined_damage"
    if vwap_damage:
        return "vwap_failure"
    if structure_damage:
        return "ema_structure_failure"
    if collapse:
        return "drift_score_collapse"
    return "regime_flip_only"


def _build_warning_state_events(
    *,
    feature_rows: Sequence[AsiaDriftFeatureRow],
    state_rows: Sequence[Any],
) -> list[dict[str, Any]]:
    state_by_session = _group_by(
        sorted(state_rows, key=lambda row: (row.asia_drift_session_id, row.decision_ts)),
        key=lambda row: row.asia_drift_session_id,
    )
    feature_by_key = {
        (row.asia_drift_session_id, row.decision_ts): row
        for row in feature_rows
    }
    events: list[dict[str, Any]] = []
    for session_id, rows in sorted(state_by_session.items()):
        active_start = None
        active_reason = None
        active_bars = 0
        for row in rows:
            if row.state == STATE_DRIFT_AT_RISK:
                if active_start is None:
                    active_start = row.decision_ts
                    active_reason = row.transition_reason
                active_bars = row.bars_in_at_risk_state
                continue
            if active_start is None:
                continue
            feature = feature_by_key.get((session_id, row.decision_ts))
            resolution = "RECOVERED"
            if row.state == STATE_REQUALIFIED_CANDIDATE:
                resolution = "REQUALIFIED"
            elif row.state == STATE_CONFIRMED_INVALIDATION:
                resolution = "INVALIDATED"
            elif row.state == "SESSION_TIMEOUT":
                resolution = "TIMEOUT"
            elif row.state == "NO_TRADE":
                resolution = "SESSION_CLEARED"
            continuation_after_resolution = False
            if feature is not None:
                session_features = [
                    candidate
                    for candidate in feature_rows
                    if candidate.asia_drift_session_id == session_id and candidate.decision_ts > row.decision_ts and candidate.in_scope
                ]
                if feature.dominant_direction == "LONG":
                    continuation_after_resolution = any(
                        candidate.high >= feature.drift_leg_extreme + 0.10 * feature.atr for candidate in session_features
                    )
                else:
                    continuation_after_resolution = any(
                        candidate.low <= feature.drift_leg_extreme - 0.10 * feature.atr for candidate in session_features
                    )
            events.append(
                {
                    "asia_drift_session_id": session_id,
                    "warning_start_ts": active_start.isoformat(),
                    "resolution_ts": row.decision_ts.isoformat(),
                    "warning_reason": active_reason,
                    "resolution": resolution,
                    "resolution_reason": row.transition_reason,
                    "bars_at_risk": active_bars,
                    "continuation_after_resolution": continuation_after_resolution,
                    "recovery_score_on_resolution": feature.recovery_score if feature is not None else None,
                    "recovery_label_on_resolution": feature.recovery_label if feature is not None else None,
                    "regime_persistence_score_on_resolution": feature.regime_persistence_score if feature is not None else None,
                    "pullback_vwap_interaction_on_resolution": feature.pullback_vwap_interaction if feature is not None else None,
                }
            )
            active_start = None
            active_reason = None
            active_bars = 0
    return events


def _accepted_setup_continuation(
    *,
    entry_setups: Sequence[AsiaDriftEntrySetup],
    feature_rows: Sequence[AsiaDriftFeatureRow],
) -> list[dict[str, Any]]:
    features_by_session = _group_by(
        sorted(feature_rows, key=lambda row: (row.asia_drift_session_id, row.decision_ts)),
        key=lambda row: row.asia_drift_session_id,
    )
    rows: list[dict[str, Any]] = []
    for setup in entry_setups:
        session_rows = features_by_session.get(setup.asia_drift_session_id, [])
        setup_index = next((index for index, row in enumerate(session_rows) if row.decision_ts == setup.armed_ts), None)
        if setup_index is None:
            continue
        future_rows = [row for row in session_rows[setup_index + 1 :] if row.in_scope]
        if setup.direction == "LONG":
            continuation = any(row.high >= setup.drift_leg_extreme + 0.10 * setup.atr for row in future_rows)
        else:
            continuation = any(row.low <= setup.drift_leg_extreme - 0.10 * setup.atr for row in future_rows)
        rows.append(
            {
                "setup_id": setup.setup_id,
                "direction": setup.direction,
                "continuation_reasserted": continuation,
            }
        )
    return rows


def _interpret_phase2(
    *,
    entry_evaluations: Sequence[AsiaDriftEntryEvaluation],
    trade_records: Sequence[AsiaDriftTradeRecord],
    pullback_vetoes: Sequence[dict[str, Any]],
    invalidated_evals: Sequence[AsiaDriftEntryEvaluation],
) -> dict[str, Any]:
    limit_rows = [row for row in entry_evaluations if row.entry_model == ENTRY_MODEL_LIMIT_PULLBACK]
    confirm_rows = [row for row in entry_evaluations if row.entry_model == ENTRY_MODEL_CONFIRMATION_REACCEL]
    limit_accept_rate = _rate(sum(1 for row in limit_rows if row.accepted), len(limit_rows))
    confirm_accept_rate = _rate(sum(1 for row in confirm_rows if row.accepted), len(confirm_rows))
    false_veto_rate = _rate(sum(1 for row in pullback_vetoes if row["continuation_reasserted"]), len(pullback_vetoes))
    invalidation_reassert_rate = _rate(
        sum(1 for row in invalidated_evals if row.continuation_reasserted_after_reject),
        len(invalidated_evals),
    )
    avg_trade_r = _median([row.gross_r for row in trade_records])

    if false_veto_rate >= 0.45 or invalidation_reassert_rate >= 0.45:
        state_engine_assessment = "too_strict"
    elif false_veto_rate <= 0.20 and invalidation_reassert_rate <= 0.20:
        state_engine_assessment = "about_right"
    else:
        state_engine_assessment = "mixed_borderline"

    if limit_accept_rate > confirm_accept_rate:
        preferred_entry_family = ENTRY_MODEL_LIMIT_PULLBACK
    elif confirm_accept_rate > limit_accept_rate:
        preferred_entry_family = ENTRY_MODEL_CONFIRMATION_REACCEL
    else:
        preferred_entry_family = "TIE"

    return {
        "state_engine_assessment": state_engine_assessment,
        "preferred_entry_family_by_acceptance": preferred_entry_family,
        "median_trade_gross_r": avg_trade_r,
        "phase3_research_candidate": bool(
            trade_records
            and state_engine_assessment in {"about_right", "mixed_borderline"}
        ),
    }


def _invalidation_category(*, reason: str, row: AsiaDriftFeatureRow) -> str:
    mapping = {
        "protected_swing_break": "STRUCTURE_DAMAGE",
        "confirmed_vwap_reclaim": "VWAP_RECLAIM",
        "vwap_and_slow_ema_failure": "EMA_FAILURE",
        "depth_exceeds_limit": "DEPTH",
        "pullback_too_fast": "SINGLE_BAR_NOISE",
        "violent_countertrend_expansion": "EXPANSION",
        "regime_lost_after_candidate": "DRIFT_COLLAPSE",
    }
    if reason in mapping:
        return mapping[reason]
    if row.pullback_warning_reason == "single_close_through_vwap":
        return "VWAP_RECLAIM"
    return "OTHER"


def _group_by(rows: Sequence[Any], *, key: Any) -> dict[Any, list[Any]]:
    grouped: dict[Any, list[Any]] = defaultdict(list)
    for row in rows:
        grouped[key(row)].append(row)
    return grouped


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _median(values: Sequence[float | int | None]) -> float | None:
    cleaned = [float(value) for value in values if value is not None]
    if not cleaned:
        return None
    return float(median(cleaned))
