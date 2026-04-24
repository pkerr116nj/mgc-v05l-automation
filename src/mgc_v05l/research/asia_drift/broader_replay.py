"""Bounded broader replay research for Asia Drift refined shallow participation."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Sequence

from ..trend_participation.models import ResearchBar
from ..trend_participation.storage import build_layout, write_storage_manifest
from .backtest import AsiaDriftPhase2Run, run_asia_drift_phase2, run_asia_drift_phase2_from_bars
from .entries import (
    ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
    PREFILL_PROFILE_LOOSE_DIAGNOSTIC,
    PREFILL_PROFILE_PERSISTENCE_MEDIUM,
    PREFILL_PROFILE_PERSISTENCE_SHORT,
    PREFILL_PROFILE_RECOVERY_CONFIRMED,
)
from .features import ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT, FAST_SHALLOW_VALID, RECOVERY_CONFIRMED


@dataclass(frozen=True)
class AsiaDriftReplayWindow:
    label: str
    start_ts: datetime
    end_ts: datetime


@dataclass(frozen=True)
class AsiaDriftReplayBarWindow:
    label: str
    bars_5m: list[ResearchBar]
    bars_1m: list[ResearchBar] | None = None


DEFAULT_REFINED_REPLAY_WINDOWS = (
    AsiaDriftReplayWindow(
        label="mgc_20260301_20260305",
        start_ts=datetime.fromisoformat("2026-03-01T00:00:00+00:00"),
        end_ts=datetime.fromisoformat("2026-03-05T23:59:00+00:00"),
    ),
    AsiaDriftReplayWindow(
        label="mgc_20260306_20260310",
        start_ts=datetime.fromisoformat("2026-03-06T00:00:00+00:00"),
        end_ts=datetime.fromisoformat("2026-03-10T23:59:00+00:00"),
    ),
    AsiaDriftReplayWindow(
        label="mgc_20260311_20260315",
        start_ts=datetime.fromisoformat("2026-03-11T00:00:00+00:00"),
        end_ts=datetime.fromisoformat("2026-03-15T23:59:00+00:00"),
    ),
)
DEFAULT_WIDER_REPLAY_WINDOWS = (
    AsiaDriftReplayWindow(
        label="mgc_20250908_20250912",
        start_ts=datetime.fromisoformat("2025-09-08T00:00:00+00:00"),
        end_ts=datetime.fromisoformat("2025-09-12T23:59:00+00:00"),
    ),
    AsiaDriftReplayWindow(
        label="mgc_20251020_20251024",
        start_ts=datetime.fromisoformat("2025-10-20T00:00:00+00:00"),
        end_ts=datetime.fromisoformat("2025-10-24T23:59:00+00:00"),
    ),
    AsiaDriftReplayWindow(
        label="mgc_20251208_20251212",
        start_ts=datetime.fromisoformat("2025-12-08T00:00:00+00:00"),
        end_ts=datetime.fromisoformat("2025-12-12T23:59:00+00:00"),
    ),
    AsiaDriftReplayWindow(
        label="mgc_20260112_20260116",
        start_ts=datetime.fromisoformat("2026-01-12T00:00:00+00:00"),
        end_ts=datetime.fromisoformat("2026-01-16T23:59:00+00:00"),
    ),
    AsiaDriftReplayWindow(
        label="mgc_20260209_20260213",
        start_ts=datetime.fromisoformat("2026-02-09T00:00:00+00:00"),
        end_ts=datetime.fromisoformat("2026-02-13T23:59:00+00:00"),
    ),
    AsiaDriftReplayWindow(
        label="mgc_20260301_20260305",
        start_ts=datetime.fromisoformat("2026-03-01T00:00:00+00:00"),
        end_ts=datetime.fromisoformat("2026-03-05T23:59:00+00:00"),
    ),
    AsiaDriftReplayWindow(
        label="mgc_20260406_20260410",
        start_ts=datetime.fromisoformat("2026-04-06T00:00:00+00:00"),
        end_ts=datetime.fromisoformat("2026-04-10T23:59:00+00:00"),
    ),
)
DEFAULT_PREFILL_COMPARISON_PROFILES = (
    PREFILL_PROFILE_RECOVERY_CONFIRMED,
    PREFILL_PROFILE_PERSISTENCE_SHORT,
    PREFILL_PROFILE_PERSISTENCE_MEDIUM,
    PREFILL_PROFILE_LOOSE_DIAGNOSTIC,
)


def run_refined_shallow_research(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    primary_instrument: str = "MGC",
    windows: Sequence[AsiaDriftReplayWindow] = DEFAULT_REFINED_REPLAY_WINDOWS,
    reference_instruments: Sequence[str] = (),
    refined_prefill_profile_name: str = PREFILL_PROFILE_RECOVERY_CONFIRMED,
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
                refined_prefill_profile_name=refined_prefill_profile_name,
            )
            window_runs.append((window, run))
        instrument_runs[instrument] = window_runs
    payload = _build_payload(
        instrument_runs=instrument_runs,
        source_label=str(source_sqlite_path.resolve()),
        primary_instrument=primary_instrument,
        refined_prefill_profile_name=refined_prefill_profile_name,
        module_name="Asia Drift v1 Refined Shallow Broader Replay",
        objective=(
            "Broader but still bounded replay research focused on SHALLOW_PARTICIPATION_REFINED under the "
            "recovery_confirmed profile to test whether shallow/fast Asia drift participation generalizes "
            "across multiple windows without weakening depth/chop/post-spike protections."
        ),
    )
    artifacts = _write_artifacts(output_dir=output_dir, payload=payload)
    return {
        "payload": payload,
        "artifacts": artifacts,
    }


def run_refined_shallow_research_from_bars(
    *,
    output_dir: Path,
    primary_instrument: str,
    windows: Sequence[AsiaDriftReplayBarWindow],
    refined_prefill_profile_name: str = PREFILL_PROFILE_RECOVERY_CONFIRMED,
) -> dict[str, Any]:
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]]] = {primary_instrument: []}
    for window in windows:
        run = run_asia_drift_phase2_from_bars(
            output_dir=output_dir / primary_instrument.lower() / window.label,
            bars_5m=window.bars_5m,
            bars_1m=window.bars_1m,
            source_label=window.label,
            calibration_profile_name=RECOVERY_CONFIRMED,
            refined_prefill_profile_name=refined_prefill_profile_name,
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
    payload = _build_payload(
        instrument_runs=instrument_runs,
        source_label="synthetic",
        primary_instrument=primary_instrument,
        refined_prefill_profile_name=refined_prefill_profile_name,
        module_name="Asia Drift v1 Refined Shallow Broader Replay",
        objective=(
            "Broader but still bounded replay research focused on SHALLOW_PARTICIPATION_REFINED under the "
            "recovery_confirmed profile to test whether shallow/fast Asia drift participation generalizes "
            "across multiple windows without weakening depth/chop/post-spike protections."
        ),
    )
    artifacts = _write_artifacts(output_dir=output_dir, payload=payload)
    return {
        "payload": payload,
        "artifacts": artifacts,
    }


def run_refined_shallow_wider_research(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    primary_instrument: str = "MGC",
    windows: Sequence[AsiaDriftReplayWindow] = DEFAULT_WIDER_REPLAY_WINDOWS,
    reference_instruments: Sequence[str] = (),
    refined_prefill_profile_name: str = PREFILL_PROFILE_RECOVERY_CONFIRMED,
) -> dict[str, Any]:
    result = run_refined_shallow_research(
        source_sqlite_path=source_sqlite_path,
        output_dir=output_dir,
        primary_instrument=primary_instrument,
        windows=windows,
        reference_instruments=reference_instruments,
        refined_prefill_profile_name=refined_prefill_profile_name,
    )
    payload = dict(result["payload"])
    payload["module"] = "Asia Drift v1 Refined Shallow Wider-Date Replay"
    payload["objective"] = (
        "Structured wider-date replay of SHALLOW_PARTICIPATION_REFINED under the recovery_confirmed profile "
        "to test whether the shallow/fast Asia drift mechanism repeats across quieter, directional, and "
        "choppier windows without changing protections."
    )
    payload["study_type"] = "wider_date_replay"
    payload["default_window_set"] = [row.label for row in windows]
    artifacts = _write_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def run_refined_shallow_wider_research_from_bars(
    *,
    output_dir: Path,
    primary_instrument: str,
    windows: Sequence[AsiaDriftReplayBarWindow],
    refined_prefill_profile_name: str = PREFILL_PROFILE_RECOVERY_CONFIRMED,
) -> dict[str, Any]:
    result = run_refined_shallow_research_from_bars(
        output_dir=output_dir,
        primary_instrument=primary_instrument,
        windows=windows,
        refined_prefill_profile_name=refined_prefill_profile_name,
    )
    payload = dict(result["payload"])
    payload["module"] = "Asia Drift v1 Refined Shallow Wider-Date Replay"
    payload["objective"] = (
        "Structured wider-date replay of SHALLOW_PARTICIPATION_REFINED under the recovery_confirmed profile "
        "to test whether the shallow/fast Asia drift mechanism repeats across quieter, directional, and "
        "choppier windows without changing protections."
    )
    payload["study_type"] = "wider_date_replay"
    payload["default_window_set"] = [row.label for row in windows]
    artifacts = _write_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def _build_payload(
    *,
    instrument_runs: dict[str, list[tuple[AsiaDriftReplayWindow, AsiaDriftPhase2Run]]],
    source_label: str,
    primary_instrument: str,
    refined_prefill_profile_name: str,
    module_name: str,
    objective: str,
) -> dict[str, Any]:
    per_window_rows: list[dict[str, Any]] = []
    candidate_trade_manifest: list[dict[str, Any]] = []
    opportunity_manifest: list[dict[str, Any]] = []
    session_manifest: list[dict[str, Any]] = []

    for instrument, window_runs in sorted(instrument_runs.items()):
        for window, run in window_runs:
            row = _window_summary_row(instrument=instrument, window=window, run=run)
            per_window_rows.append(row)
            candidate_trade_manifest.extend(_candidate_trade_rows(instrument=instrument, window=window, run=run))
            opportunity_manifest.extend(_opportunity_rows(instrument=instrument, window=window, run=run))
            session_manifest.extend(_session_rows(instrument=instrument, window=window, run=run))

    aggregate_by_instrument = {
        instrument: _aggregate_instrument_rows(
            rows=[row for row in per_window_rows if row["instrument"] == instrument],
            candidate_trade_manifest=[row for row in candidate_trade_manifest if row["instrument"] == instrument],
        )
        for instrument in sorted(instrument_runs)
    }
    recommendation = _recommendation(
        primary_summary=aggregate_by_instrument.get(primary_instrument, {}),
        primary_rows=[row for row in per_window_rows if row["instrument"] == primary_instrument],
    )
    return {
        "module": module_name,
        "objective": objective,
        "primary_entry_model": ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
        "calibration_profile": RECOVERY_CONFIRMED,
        "refined_prefill_profile": refined_prefill_profile_name,
        "source_label": source_label,
        "primary_instrument": primary_instrument,
        "window_summaries": per_window_rows,
        "aggregate_summary": aggregate_by_instrument,
        "candidate_trade_manifest": candidate_trade_manifest,
        "opportunity_manifest": opportunity_manifest,
        "session_manifest": session_manifest,
        "recommendation": recommendation,
    }


def _window_summary_row(
    *,
    instrument: str,
    window: AsiaDriftReplayWindow,
    run: AsiaDriftPhase2Run,
) -> dict[str, Any]:
    diagnostics = run.diagnostics
    refined_summary = diagnostics["entry_model_summary"].get(ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED, {})
    prefill_audit = diagnostics.get("prefill_regime_loss_audit") or {}
    refined_trades = [row for row in run.trade_records if row.entry_model == ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED]
    refined_setups = {
        row.setup_id
        for row in run.entry_evaluations
        if row.entry_model == ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED
    }
    fast_shallow_opportunities = sum(
        1
        for row in run.phase1_run.feature_rows
        if row.in_scope and row.regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT} and row.fast_pullback_class == FAST_SHALLOW_VALID
    )
    drift_candidate_sessions = sum(
        1
        for row in run.phase1_run.session_summaries
        if row.candidate_direction in {"LONG", "SHORT"}
    )
    context_tags = _window_context_tags(run)
    protections = diagnostics.get("protection_preservation") or {}
    cancellation_counts = refined_summary.get("cancellation_reason_counts") or {}
    return {
        "instrument": instrument,
        "window_label": window.label,
        "start_ts": window.start_ts.isoformat(),
        "end_ts": window.end_ts.isoformat(),
        "asia_session_count": len(run.phase1_run.session_summaries),
        "drift_candidate_sessions": drift_candidate_sessions,
        "refined_setup_count": len(refined_setups),
        "fast_shallow_valid_opportunities": fast_shallow_opportunities,
        "accepted_entries": refined_summary.get("accepted_count", 0),
        "rejected_entries": refined_summary.get("rejected_or_expired_count", 0),
        "accepted_entry_present": refined_summary.get("accepted_count", 0) > 0,
        "regime_lost_before_fill_count": cancellation_counts.get("regime_lost_before_fill", 0),
        "prefill_warning_recovery_count": sum(
            1
            for row in run.entry_evaluations
            if row.entry_model == ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED
            and row.accepted
            and "prefill_recovered_after_warning" in row.reason_tags
        ),
        "false_persistence_count": sum(
            1
            for row in refined_trades
            if row.gross_r <= 0.0 or row.exit_reason in {"depth_exceeds_limit", "hard_stop_close_breach"}
        ),
        "deep_pullback_rejection_count": (
            cancellation_counts.get("fast_deep_disqualifying", 0)
            + cancellation_counts.get("depth_exceeds_shallow_window", 0)
            + cancellation_counts.get("depth_exceeds_limit", 0)
        ),
        "prefill_resumed_within_2_bars_count": prefill_audit.get("resumed_within_2_bars_count", 0),
        "prefill_would_reach_participation_zone_count": prefill_audit.get("would_reach_participation_zone_count", 0),
        "cancellation_reason_counts": cancellation_counts,
        "chop_veto_bar_count": protections.get("chop_veto_bar_count", 0),
        "post_spike_bar_count": protections.get("post_spike_bar_count", 0),
        "trade_count": len(refined_trades),
        "median_mfe_r": _median([row.mfe_r for row in refined_trades]),
        "median_mae_r": _median([row.mae_r for row in refined_trades]),
        "median_gross_r": _median([row.gross_r for row in refined_trades]),
        "gross_r_sum": sum(row.gross_r for row in refined_trades),
        "exit_reason_distribution": _counter_to_dict(row.exit_reason for row in refined_trades),
        "context_realized_volatility_bucket": context_tags["realized_volatility_bucket"],
        "context_directional_efficiency_bucket": context_tags["directional_efficiency_bucket"],
        "context_market_texture": context_tags["market_texture"],
        "context_post_spike_context": context_tags["post_spike_context"],
        "context_instrument": context_tags["instrument_context"],
        "context_median_realized_volatility_ratio": context_tags["median_realized_volatility_ratio"],
        "context_median_directional_efficiency": context_tags["median_directional_efficiency"],
        "context_chop_veto_fraction": context_tags["chop_veto_fraction"],
        "context_directional_bar_fraction": context_tags["directional_bar_fraction"],
        "context_post_spike_fraction": context_tags["post_spike_fraction"],
        "phase2_summary_markdown_path": str(run.artifacts.summary_markdown_path),
        "phase2_diagnostics_json_path": str(run.artifacts.diagnostics_json_path),
    }


def _candidate_trade_rows(
    *,
    instrument: str,
    window: AsiaDriftReplayWindow,
    run: AsiaDriftPhase2Run,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trade in run.trade_records:
        if trade.entry_model != ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED:
            continue
        rows.append(
            {
                "instrument": instrument,
                "window_label": window.label,
                "trade_id": trade.trade_id,
                "setup_id": trade.setup_id,
                "exit_profile": trade.exit_profile,
                "entry_ts": trade.entry_ts.isoformat(),
                "exit_ts": trade.exit_ts.isoformat(),
                "gross_r": trade.gross_r,
                "mfe_r": trade.mfe_r,
                "mae_r": trade.mae_r,
                "exit_reason": trade.exit_reason,
            }
        )
    return rows


def _opportunity_rows(
    *,
    instrument: str,
    window: AsiaDriftReplayWindow,
    run: AsiaDriftPhase2Run,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for evaluation in run.entry_evaluations:
        if evaluation.entry_model != ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED:
            continue
        rows.append(
            {
                "instrument": instrument,
                "window_label": window.label,
                "setup_id": evaluation.setup_id,
                "accepted": evaluation.accepted,
                "status": evaluation.status,
                "cancellation_reason": evaluation.cancellation_reason,
                "continuation_reasserted_after_reject": evaluation.continuation_reasserted_after_reject,
                "missed_favorable_excursion_r": evaluation.missed_favorable_excursion_r,
            }
        )
    return rows


def _session_rows(
    *,
    instrument: str,
    window: AsiaDriftReplayWindow,
    run: AsiaDriftPhase2Run,
) -> list[dict[str, Any]]:
    feature_rows_by_session: dict[str, list[Any]] = {}
    for feature_row in run.phase1_run.feature_rows:
        feature_rows_by_session.setdefault(feature_row.asia_drift_session_id, []).append(feature_row)
    rows: list[dict[str, Any]] = []
    for row in run.phase1_run.session_summaries:
        context_tags = _session_context_tags(feature_rows_by_session.get(row.asia_drift_session_id, ()))
        rows.append(
            {
                "instrument": instrument,
                "window_label": window.label,
                "asia_drift_session_id": row.asia_drift_session_id,
                "candidate_direction": row.candidate_direction,
                "entry_ready_bar_count": row.entry_ready_bar_count,
                "requalified_bar_count": row.requalified_bar_count,
                "invalidated_bar_count": row.invalidated_bar_count,
                "chop_veto_bar_count": row.chop_veto_bar_count,
                "post_spike_bar_count": row.post_spike_bar_count,
                "realized_volatility_bucket": context_tags["realized_volatility_bucket"],
                "directional_efficiency_bucket": context_tags["directional_efficiency_bucket"],
                "session_texture_tag": context_tags["market_texture"],
                "post_spike_context_tag": context_tags["post_spike_context"],
            }
        )
    return rows


def _aggregate_instrument_rows(
    *,
    rows: Sequence[dict[str, Any]],
    candidate_trade_manifest: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    cancellation_reason_counts: dict[str, int] = {}
    exit_reason_counts: dict[str, int] = {}
    for row in rows:
        for reason, count in (row.get("cancellation_reason_counts") or {}).items():
            cancellation_reason_counts[reason] = cancellation_reason_counts.get(reason, 0) + int(count)
        for reason, count in (row.get("exit_reason_distribution") or {}).items():
            exit_reason_counts[reason] = exit_reason_counts.get(reason, 0) + int(count)

    windows_with_accepts = sum(1 for row in rows if row.get("accepted_entries", 0) > 0)
    context_breakdown = {
        "realized_volatility_bucket": _context_breakdown(rows, key="context_realized_volatility_bucket"),
        "directional_efficiency_bucket": _context_breakdown(rows, key="context_directional_efficiency_bucket"),
        "market_texture": _context_breakdown(rows, key="context_market_texture"),
        "post_spike_context": _context_breakdown(rows, key="context_post_spike_context"),
    }
    return {
        "window_count": len(rows),
        "asia_session_count": sum(int(row["asia_session_count"]) for row in rows),
        "drift_candidate_sessions": sum(int(row["drift_candidate_sessions"]) for row in rows),
        "fast_shallow_valid_opportunities": sum(int(row["fast_shallow_valid_opportunities"]) for row in rows),
        "accepted_entries": sum(int(row["accepted_entries"]) for row in rows),
        "windows_with_accepted_entries": windows_with_accepts,
        "regime_lost_before_fill_count": sum(int(row["regime_lost_before_fill_count"]) for row in rows),
        "prefill_warning_recovery_count": sum(int(row["prefill_warning_recovery_count"]) for row in rows),
        "false_persistence_count": sum(int(row["false_persistence_count"]) for row in rows),
        "deep_pullback_rejection_count": sum(int(row["deep_pullback_rejection_count"]) for row in rows),
        "prefill_resumed_within_2_bars_count": sum(int(row["prefill_resumed_within_2_bars_count"]) for row in rows),
        "prefill_would_reach_participation_zone_count": sum(int(row["prefill_would_reach_participation_zone_count"]) for row in rows),
        "chop_veto_bar_count": sum(int(row["chop_veto_bar_count"]) for row in rows),
        "post_spike_bar_count": sum(int(row["post_spike_bar_count"]) for row in rows),
        "cancellation_reason_counts": cancellation_reason_counts,
        "exit_reason_distribution": exit_reason_counts,
        "median_trade_gross_r": _median([row["gross_r"] for row in candidate_trade_manifest]),
        "median_trade_mfe_r": _median([row["mfe_r"] for row in candidate_trade_manifest]),
        "median_trade_mae_r": _median([row["mae_r"] for row in candidate_trade_manifest]),
        "accepted_entry_windows": [row["window_label"] for row in rows if row.get("accepted_entries", 0) > 0],
        "accepted_entry_clustering": {
            "accepted_window_count": windows_with_accepts,
            "accepted_entry_window_share": (windows_with_accepts / len(rows)) if rows else 0.0,
            "entries_clustered_in_single_window": windows_with_accepts == 1 and sum(int(row["accepted_entries"]) for row in rows) > 0,
            "accepted_entry_windows": [row["window_label"] for row in rows if row.get("accepted_entries", 0) > 0],
            "windows_without_accepted_entries": [row["window_label"] for row in rows if row.get("accepted_entries", 0) <= 0],
        },
        "context_breakdown": context_breakdown,
    }


def _recommendation(
    *,
    primary_summary: dict[str, Any],
    primary_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    accepted_windows = int(primary_summary.get("windows_with_accepted_entries", 0) or 0)
    accepted_entries = int(primary_summary.get("accepted_entries", 0) or 0)
    window_count = int(primary_summary.get("window_count", 0) or 0)
    asia_session_count = int(primary_summary.get("asia_session_count", 0) or 0)
    cancellation_counts = primary_summary.get("cancellation_reason_counts") or {}
    dominant_blocker = None
    if cancellation_counts:
        dominant_blocker = max(sorted(cancellation_counts.items()), key=lambda item: item[1])[0]
    repeated_fast_shallow = sum(1 for row in primary_rows if row.get("fast_shallow_valid_opportunities", 0) > 0)
    classification_id = 4
    classification_label = "insufficient_evidence_due_to_data_limitations"
    if window_count == 0 or asia_session_count == 0:
        reason = "The wider replay did not produce enough Asia-session coverage to support a decision."
    elif accepted_windows >= 3 and accepted_entries >= 4:
        classification_id = 1
        classification_label = "broader_replay_supports_continued_formal_strategy_research"
        reason = "Accepted refined-shallow entries appeared across multiple windows instead of clustering in one period."
    elif accepted_entries > 0 and accepted_windows == 1:
        classification_id = 3
        classification_label = "accepted_entries_are_isolated_or_clustered_artifact"
        reason = "Accepted refined-shallow entries remained confined to a single window even though the pattern repeated."
    elif repeated_fast_shallow >= 3:
        classification_id = 2
        classification_label = "pattern_appears_but_accepted_entry_engine_remains_too_rare"
        reason = "Fast-shallow participation repeated across windows, but conversion into accepted entries remained sparse."
    else:
        classification_id = 5
        classification_label = "pause_this_approach_and_rethink_mechanism"
        reason = "The wider replay did not show enough repeated shallow-participation behavior to justify continuing unchanged."
    return {
        "classification_id": classification_id,
        "classification_label": classification_label,
        "verdict": classification_label,
        "dominant_blocker": dominant_blocker,
        "reason": reason,
        "accepted_window_count": accepted_windows,
        "repeated_fast_shallow_window_count": repeated_fast_shallow,
    }


def run_prefill_persistence_comparison(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    primary_instrument: str = "MGC",
    windows: Sequence[AsiaDriftReplayWindow] = DEFAULT_REFINED_REPLAY_WINDOWS,
    reference_instruments: Sequence[str] = (),
    profile_names: Sequence[str] = DEFAULT_PREFILL_COMPARISON_PROFILES,
) -> dict[str, Any]:
    profile_payloads: dict[str, dict[str, Any]] = {}
    for profile_name in profile_names:
        result = run_refined_shallow_research(
            source_sqlite_path=source_sqlite_path,
            output_dir=output_dir / profile_name,
            primary_instrument=primary_instrument,
            windows=windows,
            reference_instruments=reference_instruments,
            refined_prefill_profile_name=profile_name,
        )
        profile_payloads[profile_name] = result["payload"]
    payload = _build_prefill_comparison_payload(
        profile_payloads=profile_payloads,
        primary_instrument=primary_instrument,
    )
    artifacts = _write_prefill_comparison_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def run_prefill_persistence_comparison_from_bars(
    *,
    output_dir: Path,
    primary_instrument: str,
    windows: Sequence[AsiaDriftReplayBarWindow],
    profile_names: Sequence[str] = DEFAULT_PREFILL_COMPARISON_PROFILES,
) -> dict[str, Any]:
    profile_payloads: dict[str, dict[str, Any]] = {}
    for profile_name in profile_names:
        result = run_refined_shallow_research_from_bars(
            output_dir=output_dir / profile_name,
            primary_instrument=primary_instrument,
            windows=windows,
            refined_prefill_profile_name=profile_name,
        )
        profile_payloads[profile_name] = result["payload"]
    payload = _build_prefill_comparison_payload(
        profile_payloads=profile_payloads,
        primary_instrument=primary_instrument,
    )
    artifacts = _write_prefill_comparison_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def _build_prefill_comparison_payload(
    *,
    profile_payloads: dict[str, dict[str, Any]],
    primary_instrument: str,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for profile_name, payload in sorted(profile_payloads.items()):
        primary = (payload.get("aggregate_summary") or {}).get(primary_instrument, {})
        rows.append(
            {
                "prefill_profile": profile_name,
                "accepted_entries": primary.get("accepted_entries"),
                "windows_with_accepted_entries": primary.get("windows_with_accepted_entries"),
                "fast_shallow_valid_opportunities": primary.get("fast_shallow_valid_opportunities"),
                "regime_lost_before_fill_count": primary.get("regime_lost_before_fill_count"),
                "prefill_warning_recovery_count": primary.get("prefill_warning_recovery_count"),
                "false_persistence_count": primary.get("false_persistence_count"),
                "deep_pullback_rejection_count": primary.get("deep_pullback_rejection_count"),
                "chop_veto_bar_count": primary.get("chop_veto_bar_count"),
                "post_spike_bar_count": primary.get("post_spike_bar_count"),
                "accepted_entry_windows": primary.get("accepted_entry_windows"),
                "median_trade_gross_r": primary.get("median_trade_gross_r"),
            }
        )
    baseline = next((row for row in rows if row["prefill_profile"] == PREFILL_PROFILE_RECOVERY_CONFIRMED), None)
    non_diagnostic_rows = [row for row in rows if row["prefill_profile"] != PREFILL_PROFILE_LOOSE_DIAGNOSTIC] or rows
    recommended = min(
        non_diagnostic_rows,
        key=lambda row: (
            -int(row.get("windows_with_accepted_entries") or 0),
            -int(row.get("accepted_entries") or 0),
            int(row.get("false_persistence_count") or 0),
            int(row.get("regime_lost_before_fill_count") or 0),
            0 if row["prefill_profile"] == PREFILL_PROFILE_PERSISTENCE_SHORT else (1 if row["prefill_profile"] == PREFILL_PROFILE_PERSISTENCE_MEDIUM else 2),
        ),
    ) if rows else None
    if baseline is not None and recommended is not None:
        improved = (
            int(recommended.get("windows_with_accepted_entries") or 0) > int(baseline.get("windows_with_accepted_entries") or 0)
            or int(recommended.get("accepted_entries") or 0) > int(baseline.get("accepted_entries") or 0)
        )
        if not improved:
            recommended = baseline
            reason = (
                "No prefill persistence profile increased accepted-entry spread beyond the baseline; keep "
                "recovery_confirmed as the operative default and treat the persistence profiles as diagnostic only."
            )
        else:
            reason = (
                "Best profile improves accepted-entry spread first, then reduces regime_lost_before_fill without "
                "raising false persistence."
            )
    else:
        reason = (
            "Best profile improves accepted-entry spread first, then reduces regime_lost_before_fill without "
            "raising false persistence."
        )
    return {
        "module": "Asia Drift v1 Prefill Persistence Comparison",
        "objective": (
            "Comparison of bounded pre-fill persistence profiles for SHALLOW_PARTICIPATION_REFINED to determine "
            "whether regime_lost_before_fill is premature across the same bounded replay windows."
        ),
        "primary_instrument": primary_instrument,
        "profile_rows": rows,
        "profiles": profile_payloads,
        "recommendation": {
            "recommended_profile": recommended.get("prefill_profile") if recommended else None,
            "reason": reason,
        },
    }


def _write_prefill_comparison_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    layout = build_layout(output_dir)
    comparison_json_path = layout["reports"] / "asia_drift_prefill_persistence_comparison.json"
    comparison_markdown_path = layout["reports"] / "asia_drift_prefill_persistence_comparison.md"
    comparison_rows_path = layout["reports"] / "asia_drift_prefill_persistence_rows.csv"
    comparison_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    comparison_markdown_path.write_text(_render_prefill_comparison_markdown(payload), encoding="utf-8")
    _write_csv(comparison_rows_path, payload.get("profile_rows") or [])
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_v1_prefill_persistence_comparison",
            "layout": {key: str(value) for key, value in layout.items()},
            "artifact_paths": {
                "comparison_json_path": str(comparison_json_path),
                "comparison_markdown_path": str(comparison_markdown_path),
                "comparison_rows_path": str(comparison_rows_path),
            },
        },
    )
    return {
        "comparison_json_path": str(comparison_json_path),
        "comparison_markdown_path": str(comparison_markdown_path),
        "comparison_rows_path": str(comparison_rows_path),
        "storage_manifest_path": str(layout["storage_manifest"]),
    }


def _render_prefill_comparison_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia Drift v1 Prefill Persistence Comparison",
        "",
        str(payload.get("objective") or ""),
        "",
        "## Profile Rows",
    ]
    for row in payload.get("profile_rows") or []:
        lines.append(
            f"- {row['prefill_profile']}: accepted_entries={row['accepted_entries']} "
            f"windows_with_accepted_entries={row['windows_with_accepted_entries']} "
            f"regime_lost_before_fill_count={row['regime_lost_before_fill_count']} "
            f"prefill_warning_recovery_count={row['prefill_warning_recovery_count']} "
            f"false_persistence_count={row['false_persistence_count']}"
        )
    lines.extend(["", "## Recommendation"])
    recommendation = payload.get("recommendation") or {}
    lines.append(f"- recommended_profile: {recommendation.get('recommended_profile')}")
    lines.append(f"- reason: {recommendation.get('reason')}")
    return "\n".join(lines) + "\n"


def _write_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    layout = build_layout(output_dir)
    aggregate_json_path = layout["reports"] / "asia_drift_refined_shallow_replay_aggregate.json"
    aggregate_markdown_path = layout["reports"] / "asia_drift_refined_shallow_replay_aggregate.md"
    window_rows_path = layout["reports"] / "asia_drift_refined_shallow_window_rows.csv"
    candidate_trade_manifest_path = layout["trades"] / "asia_drift_refined_shallow_candidate_trades.csv"
    opportunity_manifest_path = layout["signals"] / "asia_drift_refined_shallow_opportunities.csv"
    session_manifest_path = layout["reports"] / "asia_drift_refined_shallow_session_manifest.json"

    aggregate_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    aggregate_markdown_path.write_text(_render_markdown(payload), encoding="utf-8")
    _write_csv(window_rows_path, payload.get("window_summaries") or [])
    _write_csv(candidate_trade_manifest_path, payload.get("candidate_trade_manifest") or [])
    _write_csv(opportunity_manifest_path, payload.get("opportunity_manifest") or [])
    session_manifest_path.write_text(
        json.dumps(payload.get("session_manifest") or [], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "asia_drift_v1_refined_shallow_broader_replay",
            "layout": {key: str(value) for key, value in layout.items()},
            "artifact_paths": {
                "aggregate_json_path": str(aggregate_json_path),
                "aggregate_markdown_path": str(aggregate_markdown_path),
                "window_rows_path": str(window_rows_path),
                "candidate_trade_manifest_path": str(candidate_trade_manifest_path),
                "opportunity_manifest_path": str(opportunity_manifest_path),
                "session_manifest_path": str(session_manifest_path),
            },
        },
    )
    return {
        "aggregate_json_path": str(aggregate_json_path),
        "aggregate_markdown_path": str(aggregate_markdown_path),
        "window_rows_path": str(window_rows_path),
        "candidate_trade_manifest_path": str(candidate_trade_manifest_path),
        "opportunity_manifest_path": str(opportunity_manifest_path),
        "session_manifest_path": str(session_manifest_path),
        "storage_manifest_path": str(layout["storage_manifest"]),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Asia Drift v1 Refined Shallow Broader Replay",
        "",
        str(payload.get("objective") or ""),
        "",
        f"- primary_instrument: {payload.get('primary_instrument')}",
        f"- calibration_profile: {payload.get('calibration_profile')}",
        f"- primary_entry_model: {payload.get('primary_entry_model')}",
        "",
        "## Window Summaries",
    ]
    for row in payload.get("window_summaries") or []:
        lines.append(
            f"- {row['instrument']} {row['window_label']}: sessions={row['asia_session_count']} "
            f"drift_candidate_sessions={row['drift_candidate_sessions']} fast_shallow_valid_opportunities={row['fast_shallow_valid_opportunities']} "
            f"accepted_entries={row['accepted_entries']} regime_lost_before_fill_count={row['regime_lost_before_fill_count']} "
            f"deep_pullback_rejection_count={row['deep_pullback_rejection_count']} "
            f"vol_bucket={row['context_realized_volatility_bucket']} "
            f"eff_bucket={row['context_directional_efficiency_bucket']} "
            f"texture={row['context_market_texture']} "
            f"post_spike={row['context_post_spike_context']}"
        )
    lines.extend(["", "## Aggregate Summary"])
    for instrument, summary in sorted((payload.get("aggregate_summary") or {}).items()):
        lines.append(
            f"- {instrument}: window_count={summary.get('window_count')} accepted_entries={summary.get('accepted_entries')} "
            f"windows_with_accepted_entries={summary.get('windows_with_accepted_entries')} "
            f"fast_shallow_valid_opportunities={summary.get('fast_shallow_valid_opportunities')} "
            f"regime_lost_before_fill_count={summary.get('regime_lost_before_fill_count')}"
        )
        clustering = summary.get("accepted_entry_clustering") or {}
        lines.append(
            f"  clustering: accepted_window_share={clustering.get('accepted_entry_window_share')} "
            f"entries_clustered_in_single_window={clustering.get('entries_clustered_in_single_window')}"
        )
    lines.extend(["", "## Recommendation"])
    recommendation = payload.get("recommendation") or {}
    lines.append(f"- classification_id: {recommendation.get('classification_id')}")
    lines.append(f"- classification_label: {recommendation.get('classification_label')}")
    lines.append(f"- verdict: {recommendation.get('verdict')}")
    lines.append(f"- dominant_blocker: {recommendation.get('dominant_blocker')}")
    lines.append(f"- reason: {recommendation.get('reason')}")
    return "\n".join(lines) + "\n"


def _window_context_tags(run: AsiaDriftPhase2Run) -> dict[str, Any]:
    in_scope_rows = [row for row in run.phase1_run.feature_rows if row.in_scope]
    directional_rows = [row for row in in_scope_rows if row.regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}]
    median_realized_volatility_ratio = _median([row.realized_volatility_ratio for row in in_scope_rows])
    median_directional_efficiency = _median([row.efficiency_ratio_12 for row in in_scope_rows])
    chop_veto_fraction = (sum(1 for row in in_scope_rows if row.chop_veto) / len(in_scope_rows)) if in_scope_rows else 0.0
    directional_bar_fraction = (len(directional_rows) / len(in_scope_rows)) if in_scope_rows else 0.0
    post_spike_fraction = (sum(1 for row in in_scope_rows if row.post_spike_instability) / len(in_scope_rows)) if in_scope_rows else 0.0
    return {
        "realized_volatility_bucket": _realized_volatility_bucket(median_realized_volatility_ratio),
        "directional_efficiency_bucket": _directional_efficiency_bucket(median_directional_efficiency),
        "market_texture": _market_texture_tag(
            chop_veto_fraction=chop_veto_fraction,
            directional_bar_fraction=directional_bar_fraction,
            median_directional_efficiency=median_directional_efficiency,
        ),
        "post_spike_context": "post_spike_present" if post_spike_fraction >= 0.10 else "clean",
        "instrument_context": in_scope_rows[0].instrument if in_scope_rows else "UNKNOWN",
        "median_realized_volatility_ratio": median_realized_volatility_ratio,
        "median_directional_efficiency": median_directional_efficiency,
        "chop_veto_fraction": chop_veto_fraction,
        "directional_bar_fraction": directional_bar_fraction,
        "post_spike_fraction": post_spike_fraction,
    }


def _session_context_tags(rows: Sequence[Any]) -> dict[str, Any]:
    in_scope_rows = [row for row in rows if getattr(row, "in_scope", False)]
    directional_rows = [row for row in in_scope_rows if row.regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}]
    median_realized_volatility_ratio = _median([row.realized_volatility_ratio for row in in_scope_rows])
    median_directional_efficiency = _median([row.efficiency_ratio_12 for row in in_scope_rows])
    chop_veto_fraction = (sum(1 for row in in_scope_rows if row.chop_veto) / len(in_scope_rows)) if in_scope_rows else 0.0
    directional_bar_fraction = (len(directional_rows) / len(in_scope_rows)) if in_scope_rows else 0.0
    post_spike_fraction = (sum(1 for row in in_scope_rows if row.post_spike_instability) / len(in_scope_rows)) if in_scope_rows else 0.0
    return {
        "realized_volatility_bucket": _realized_volatility_bucket(median_realized_volatility_ratio),
        "directional_efficiency_bucket": _directional_efficiency_bucket(median_directional_efficiency),
        "market_texture": _market_texture_tag(
            chop_veto_fraction=chop_veto_fraction,
            directional_bar_fraction=directional_bar_fraction,
            median_directional_efficiency=median_directional_efficiency,
        ),
        "post_spike_context": "post_spike_present" if post_spike_fraction >= 0.10 else "clean",
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


def _market_texture_tag(
    *,
    chop_veto_fraction: float,
    directional_bar_fraction: float,
    median_directional_efficiency: float | None,
) -> str:
    if chop_veto_fraction >= 0.35:
        return "chop_heavy"
    if directional_bar_fraction >= 0.45 and (median_directional_efficiency or 0.0) >= 0.42:
        return "directional"
    return "mixed"


def _context_breakdown(rows: Sequence[dict[str, Any]], *, key: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(key) or "unknown"), []).append(row)
    return {
        group_key: {
            "window_count": len(group_rows),
            "accepted_entries": sum(int(row.get("accepted_entries", 0) or 0) for row in group_rows),
            "accepted_window_count": sum(1 for row in group_rows if int(row.get("accepted_entries", 0) or 0) > 0),
            "fast_shallow_valid_opportunities": sum(int(row.get("fast_shallow_valid_opportunities", 0) or 0) for row in group_rows),
        }
        for group_key, group_rows in sorted(grouped.items())
    }


def _counter_to_dict(values: Sequence[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


def _median(values: Sequence[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return float(median(clean))


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
