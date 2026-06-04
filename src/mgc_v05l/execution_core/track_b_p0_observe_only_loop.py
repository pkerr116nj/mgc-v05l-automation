"""Bounded read-only Track B P0 observe-only loop.

This module repeats the existing P0 observe-only proof sequence against fresh
Phase-1 runtime market data. It is intentionally a manual, bounded CLI: it does
not install a daemon, start runtime, submit orders, call broker mutation APIs, or
grant lifecycle/order authority.
"""

from __future__ import annotations

import argparse
import json
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.phase1_runtime_data_readiness import (
    Phase1RuntimeDataReadinessConfig,
    build_phase1_runtime_data_readiness,
    write_phase1_runtime_data_readiness_artifacts,
)
from mgc_v05l.execution_core.track_b_asian_drift_watch_chain import (
    run_track_b_asian_drift_watch_chain,
)
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_control_plane_snapshot import (
    CONTROL_PLANE_SNAPSHOT_READY,
    TrackBControlPlaneSnapshotConfig,
    build_track_b_control_plane_snapshot,
    write_track_b_control_plane_snapshot,
)
from mgc_v05l.execution_core.track_b_multi_strategy_runtime_cycle import (
    TrackBMultiStrategyRuntimeCycleConfig,
    build_track_b_multi_strategy_cycle_authority,
    run_track_b_multi_strategy_runtime_cycle,
)
from mgc_v05l.execution_core.track_b_paper_proof_readiness import (
    TrackBPaperProofReadinessConfig,
    build_track_b_paper_proof_readiness,
    write_track_b_paper_proof_readiness,
)
from mgc_v05l.execution_core.track_b_research_shadow_drift import (
    TrackBResearchShadowDriftConfig,
    build_gap_drift_continuation_shadow,
    build_late_join_asian_drift_shadow,
    build_p0_near_miss_shadow,
    write_research_shadow_payloads,
)
from mgc_v05l.execution_core.track_b_session_strategy_envelope_producer import (
    produce_track_b_session_strategy_envelopes,
)
from mgc_v05l.execution_core.track_b_snap_turn_envelope_producer import (
    produce_track_b_snap_turn_envelopes,
)
from mgc_v05l.execution_core.track_b_strategy_registry import get_track_b_strategy_registry


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_P0_OBSERVE_ONLY_OUTPUT_ROOT = (
    Path("outputs") / "track_b_execution_core" / "p0_observe_only"
)
DEFAULT_P0_OBSERVE_ONLY_LATEST = (
    DEFAULT_P0_OBSERVE_ONLY_OUTPUT_ROOT / "latest_p0_observe_only_loop.json"
)
DEFAULT_P0_OBSERVE_ONLY_EVENTS = (
    DEFAULT_P0_OBSERVE_ONLY_OUTPUT_ROOT / "p0_observe_only_loop_events.jsonl"
)
DEFAULT_GUARDED_PAPER_ROSTER_CONFIG = Path("config") / "track_b_guarded_paper_roster.json"
DEFAULT_MGC_1M_CANDLES = (
    Path("outputs")
    / "track_b_execution_core"
    / "phase1_runtime_market_data"
    / "MGC"
    / "1m"
    / "latest_runtime_candles.json"
)
DEFAULT_MGC_5M_CANDLES = (
    Path("outputs")
    / "track_b_execution_core"
    / "phase1_runtime_market_data"
    / "MGC"
    / "5m"
    / "latest_runtime_candles.json"
)
DEFAULT_MNQ_5M_CANDLES = (
    Path("outputs")
    / "track_b_execution_core"
    / "phase1_runtime_market_data"
    / "MNQ"
    / "5m"
    / "latest_runtime_candles.json"
)
DEFAULT_MNQ_1M_CANDLES = (
    Path("outputs")
    / "track_b_execution_core"
    / "phase1_runtime_market_data"
    / "MNQ"
    / "1m"
    / "latest_runtime_candles.json"
)

P0_OBSERVE_LOOP_READY = "P0_OBSERVE_LOOP_READY"
P0_OBSERVE_LOOP_ITERATION_OK = "P0_OBSERVE_LOOP_ITERATION_OK"
P0_OBSERVE_LOOP_BLOCKED_CONTROL_PLANE = "P0_OBSERVE_LOOP_BLOCKED_CONTROL_PLANE"
P0_OBSERVE_LOOP_BLOCKED_SAFE_STATE = "P0_OBSERVE_LOOP_BLOCKED_SAFE_STATE"
P0_OBSERVE_LOOP_ENVELOPE_REFRESH_FAILED = "P0_OBSERVE_LOOP_ENVELOPE_REFRESH_FAILED"
P0_OBSERVE_LOOP_CYCLE_FAILED = "P0_OBSERVE_LOOP_CYCLE_FAILED"
P0_OBSERVE_LOOP_COMPLETED = "P0_OBSERVE_LOOP_COMPLETED"

P0_LOOP_MODE_OBSERVE_ONLY = "observe-only"
P0_LOOP_MODE_SUBMIT_DISABLED = "submit-disabled"
P0_LOOP_MODE_GUARDED_PAPER = "guarded-paper"
P0_SUBMIT_DISABLED_LOOP_READY = "P0_SUBMIT_DISABLED_LOOP_READY"
P0_SUBMIT_DISABLED_LOOP_ITERATION_OK = "P0_SUBMIT_DISABLED_LOOP_ITERATION_OK"
P0_SUBMIT_DISABLED_LOOP_CANDIDATE_PREVIEW_READY = "P0_SUBMIT_DISABLED_LOOP_CANDIDATE_PREVIEW_READY"
P0_SUBMIT_DISABLED_LOOP_NO_CANDIDATE = "P0_SUBMIT_DISABLED_LOOP_NO_CANDIDATE"
P0_SUBMIT_DISABLED_LOOP_BLOCKED_CONTROL_PLANE = "P0_SUBMIT_DISABLED_LOOP_BLOCKED_CONTROL_PLANE"
P0_SUBMIT_DISABLED_LOOP_SAFETY_BOUNDARY_FAILED = "P0_SUBMIT_DISABLED_LOOP_SAFETY_BOUNDARY_FAILED"
P0_GUARDED_PAPER_LOOP_READY = "P0_GUARDED_PAPER_LOOP_READY"
P0_GUARDED_PAPER_LOOP_NO_CANDIDATE = "P0_GUARDED_PAPER_LOOP_NO_CANDIDATE"
P0_GUARDED_PAPER_LOOP_CANDIDATE_HANDOFF = "P0_GUARDED_PAPER_LOOP_CANDIDATE_HANDOFF"
P0_GUARDED_PAPER_LOOP_BLOCKED_CONTROL_PLANE = "P0_GUARDED_PAPER_LOOP_BLOCKED_CONTROL_PLANE"
P0_GUARDED_PAPER_LOOP_BLOCKED_SAFE_STATE = "P0_GUARDED_PAPER_LOOP_BLOCKED_SAFE_STATE"
P0_GUARDED_PAPER_LOOP_BLOCKED_ROSTER = "P0_GUARDED_PAPER_LOOP_BLOCKED_ROSTER"
P0_GUARDED_PAPER_LOOP_COMPLETED = "P0_GUARDED_PAPER_LOOP_COMPLETED"
TRACK_B_GUARDED_PAPER_ROSTER_READY = "TRACK_B_GUARDED_PAPER_ROSTER_READY"
TRACK_B_GUARDED_PAPER_ROSTER_READY_WITH_REJECTIONS = "TRACK_B_GUARDED_PAPER_ROSTER_READY_WITH_REJECTIONS"
TRACK_B_GUARDED_PAPER_ROSTER_BLOCKED_EMPTY = "TRACK_B_GUARDED_PAPER_ROSTER_BLOCKED_EMPTY"
TRACK_B_GUARDED_PAPER_ROSTER_BLOCKED_INVALID_CONFIG = "TRACK_B_GUARDED_PAPER_ROSTER_BLOCKED_INVALID_CONFIG"
TRACK_B_GUARDED_PAPER_ROSTER_SCHEMA_VERSION = "track_b_guarded_paper_roster_v1"

P0_STRATEGY_IDS = (
    "asian_drift_v1",
    "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
    "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
    "MNQ_FIRST_BEAR_SNAP_TURN_V1",
    "MNQ_FIRST_BULL_SNAP_TURN_V1",
)
OLDER_APPROVED_PAPER_STRATEGY_IDS = (
    "FIRST_BULL_SNAP_TURN_V1",
    "FIRST_BEAR_SNAP_TURN_V1",
    "LONDON_LATE_PAUSE_RESUME_SHORT_V1",
    "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
    "US_DERIVATIVE_BEAR_TURN_V1",
    "MNQ_US_DERIVATIVE_BEAR_TURN_V1",
    "US_LATE_PAUSE_RESUME_LONG_V1",
)
APPROVED_PAPER_STRATEGY_IDS = P0_STRATEGY_IDS + OLDER_APPROVED_PAPER_STRATEGY_IDS
P0_ROSTER = "p0"
APPROVED_PAPER_ROSTER = "approved-paper"


@dataclass(frozen=True)
class TrackBP0ObserveOnlyLoopConfig:
    repo_root: Path = REPO_ROOT
    mode: str = P0_LOOP_MODE_OBSERVE_ONLY
    roster_name: str = P0_ROSTER
    enabled_strategy_ids: tuple[str, ...] = P0_STRATEGY_IDS
    roster_config_path: Path | None = DEFAULT_GUARDED_PAPER_ROSTER_CONFIG
    output_path: Path = DEFAULT_P0_OBSERVE_ONLY_LATEST
    event_log_path: Path = DEFAULT_P0_OBSERVE_ONLY_EVENTS
    mgc_1m_candles_path: Path = DEFAULT_MGC_1M_CANDLES
    mgc_5m_candles_path: Path = DEFAULT_MGC_5M_CANDLES
    mnq_1m_candles_path: Path = DEFAULT_MNQ_1M_CANDLES
    mnq_5m_candles_path: Path = DEFAULT_MNQ_5M_CANDLES
    inbox_dir: Path = Path("examples/track_b_shadow_listener/inbox")
    iterations: int = 12
    sleep_seconds: float = 60.0
    max_completed_5m_age_seconds: int = 900
    expected_account_id: str = "DUM882026"
    account_id: str = "DUM882026"
    update_operator_status: bool = True

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


@dataclass(frozen=True)
class TrackBP0ObserveOnlyLoopStages:
    phase1_readiness: Callable[[TrackBP0ObserveOnlyLoopConfig, datetime], Mapping[str, Any]]
    proof_readiness: Callable[[TrackBP0ObserveOnlyLoopConfig, datetime], Mapping[str, Any]]
    control_plane_snapshot: Callable[[TrackBP0ObserveOnlyLoopConfig, datetime], Mapping[str, Any]]
    asian_drift: Callable[[TrackBP0ObserveOnlyLoopConfig, datetime], Any]
    session_envelopes: Callable[[TrackBP0ObserveOnlyLoopConfig, datetime], Any]
    snap_turn_envelopes: Callable[[TrackBP0ObserveOnlyLoopConfig, datetime], Any]
    multi_strategy_cycle: Callable[[TrackBP0ObserveOnlyLoopConfig, datetime, Mapping[str, Any]], Any]
    sleep: Callable[[float], None] = time.sleep


def default_stages() -> TrackBP0ObserveOnlyLoopStages:
    return TrackBP0ObserveOnlyLoopStages(
        phase1_readiness=_default_phase1_readiness,
        proof_readiness=_default_proof_readiness,
        control_plane_snapshot=_default_control_plane_snapshot,
        asian_drift=_default_asian_drift,
        session_envelopes=_default_session_envelopes,
        snap_turn_envelopes=_default_snap_turn_envelopes,
        multi_strategy_cycle=_default_multi_strategy_cycle,
    )


def run_track_b_p0_observe_only_loop(
    *,
    config: TrackBP0ObserveOnlyLoopConfig,
    stages: TrackBP0ObserveOnlyLoopStages | None = None,
    now_factory: Callable[[], datetime] | None = None,
    loop_id: str | None = None,
) -> dict[str, Any]:
    actual_stages = stages or default_stages()
    loop_mode = _normalized_loop_mode(config.mode)
    actual_loop_id = loop_id or f"track-b-p0-observe-loop-{uuid.uuid4().hex}"
    iterations = max(int(config.iterations), 1)
    sleep_seconds = max(float(config.sleep_seconds), 0.0)
    iteration_reports: list[dict[str, Any]] = []
    latest_payload: dict[str, Any] = _base_payload(
        config=config,
        loop_id=actual_loop_id,
        classification=_ready_classification(loop_mode),
        iterations=iterations,
        sleep_seconds=sleep_seconds,
        iteration_reports=iteration_reports,
        generated_at=_now(now_factory).isoformat(),
    )
    _write_latest_and_event(config=config, payload=latest_payload, event=None)

    final_classification = _completed_classification(loop_mode, [])
    for index in range(1, iterations + 1):
        now = _now(now_factory)
        iteration = _run_iteration(
            config=config,
            stages=actual_stages,
            iteration=index,
            now=now,
        )
        iteration_reports.append(iteration)
        latest_payload = _base_payload(
            config=config,
            loop_id=actual_loop_id,
            classification=iteration["classification"],
            iterations=iterations,
            sleep_seconds=sleep_seconds,
            iteration_reports=iteration_reports,
            generated_at=iteration["generated_at"],
        )
        latest_payload["latest_iteration"] = iteration
        _write_latest_and_event(config=config, payload=latest_payload, event=iteration)

        if iteration["classification"] != P0_OBSERVE_LOOP_ITERATION_OK:
            if not _is_success_iteration(str(iteration["classification"]), loop_mode):
                final_classification = str(iteration["classification"])
                break
        if index < iterations and sleep_seconds > 0:
            actual_stages.sleep(sleep_seconds)
    else:
        final_classification = _completed_classification(loop_mode, iteration_reports)

    final_payload = _base_payload(
        config=config,
        loop_id=actual_loop_id,
        classification=final_classification,
        iterations=iterations,
        sleep_seconds=sleep_seconds,
        iteration_reports=iteration_reports,
        generated_at=_now(now_factory).isoformat(),
    )
    final_payload["latest_iteration"] = iteration_reports[-1] if iteration_reports else None
    write_json_atomic(config.resolve(config.output_path), final_payload)
    return final_payload


def _run_iteration(
    *,
    config: TrackBP0ObserveOnlyLoopConfig,
    stages: TrackBP0ObserveOnlyLoopStages,
    iteration: int,
    now: datetime,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "iteration": iteration,
        "generated_at": now.isoformat(),
        "mode": _normalized_loop_mode(config.mode),
        "broker_state_mutated": False,
        "broker_adapter_constructed": False,
        "broker_mutation_attempted": False,
        "lifecycle_mutation_attempted": False,
        "submit_attempted": False,
        "submit_limit_order_called": False,
        "submit_delegation_forced_off": _normalized_loop_mode(config.mode) != P0_LOOP_MODE_GUARDED_PAPER,
        "execution_enabled": _normalized_loop_mode(config.mode) == P0_LOOP_MODE_GUARDED_PAPER,
        "final_submit_allowed": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "read_only": _normalized_loop_mode(config.mode) != P0_LOOP_MODE_GUARDED_PAPER,
        "observe_only": _normalized_loop_mode(config.mode) != P0_LOOP_MODE_GUARDED_PAPER,
    }
    try:
        phase1 = dict(stages.phase1_readiness(config, now))
        proof = dict(stages.proof_readiness(config, now))
        snapshot = dict(stages.control_plane_snapshot(config, now))
        base.update(
            {
                "phase1_runtime_data_readiness": _phase1_summary(phase1),
                "proof_readiness_classification": proof.get("classification"),
                "control_plane_snapshot": _control_plane_summary(snapshot),
                "latest_mgc_5m_candle_timestamp": _latest_runtime_candle_timestamp(
                    config.resolve(config.mgc_5m_candles_path)
                ),
                "latest_mnq_5m_candle_timestamp": _latest_runtime_candle_timestamp(
                    config.resolve(config.mnq_5m_candles_path)
                ),
            }
        )
        block = _control_plane_blocker(snapshot)
        if block:
            return {
                **base,
                "classification": _blocked_control_plane_classification(config.mode),
                "primary_blocker": block,
                "required_next_action": "Refresh/repair Control Plane Snapshot before the next P0 observe-only iteration.",
            }
        safe_block = _safe_state_blocker(snapshot)
        if safe_block:
            return {
                **base,
                "classification": _blocked_safe_state_classification(config.mode),
                "primary_blocker": safe_block,
                "required_next_action": "Keep P0 observe-only loop stopped while Safe-State is not normal.",
            }
        roster = _resolve_guarded_paper_roster(config)
        if roster["enabled_strategy_count"] < 1:
            return {
                **base,
                "classification": _blocked_roster_classification(config.mode),
                "roster_validation": roster,
                "primary_blocker": roster.get("primary_blocker")
                or "No guarded PAPER strategies are enabled after roster validation.",
                "required_next_action": (
                    "Edit the guarded PAPER roster config with at least one registered paper-eligible strategy "
                    "that has a managed lifecycle path."
                ),
            }

        envelope_context = _refresh_p0_envelopes(config=config, stages=stages, now=now, snapshot=snapshot)
        envelope_context["enabled_strategy_ids"] = tuple(roster["enabled_strategy_ids"])
        envelope_context["roster_validation"] = roster
        cycle = stages.multi_strategy_cycle(config, now, envelope_context)
        cycle_report = dict(getattr(cycle, "report", {}) or {})
        cycle_verdict = str(cycle_report.get("multi_strategy_runtime_cycle_verdict") or "")
        if not cycle_verdict:
            return {
                **base,
                **envelope_context["iteration_fields"],
                "roster_validation": roster,
                "classification": P0_OBSERVE_LOOP_CYCLE_FAILED,
                "primary_blocker": "Multi-strategy cycle did not return a verdict.",
                "required_next_action": "Review P0 multi-strategy cycle stage diagnostics before continuing.",
            }
        safety_failure = _submit_disabled_safety_failure(config=config, cycle_report=cycle_report)
        near_miss_shadow = _p0_near_miss_shadow_diagnostics(
            config=config,
            now=now,
            cycle_report=cycle_report,
            cycle_report_path=getattr(cycle, "report_json", None),
            prior_shadow_diagnostics=envelope_context.get("research_shadow_diagnostics"),
        )
        if safety_failure:
            return {
                **base,
                **envelope_context["iteration_fields"],
                "roster_validation": roster,
                "p0_near_miss_shadow": _p0_near_miss_shadow_summary(near_miss_shadow),
                "classification": P0_SUBMIT_DISABLED_LOOP_SAFETY_BOUNDARY_FAILED,
                "cycle_verdict": cycle_verdict,
                "cycle_report_path": str(getattr(cycle, "report_json", "") or ""),
                "candidate_signal_count": len(cycle_report.get("candidate_signals") or []),
                "suppressed_signal_count": len(cycle_report.get("suppressed_signals") or []),
                "submit_authorization_preview": _empty_submit_disabled_preview(cycle_report),
                "primary_blocker": safety_failure,
                "required_next_action": "Stop submit-disabled loop; a broker-capable path was reached unexpectedly.",
            }
        submit_preview = _submit_disabled_preview(
            config=config,
            now=now,
            envelope_context=envelope_context,
            cycle_report=cycle_report,
        )
        iteration_classification = _iteration_success_classification(
            mode=config.mode,
            candidate_count=len(cycle_report.get("candidate_signals") or []),
            submit_preview=submit_preview,
        )
        return {
            **base,
            **envelope_context["iteration_fields"],
            "roster_validation": roster,
            "classification": iteration_classification,
            "cycle_verdict": cycle_verdict,
            "cycle_report_path": str(getattr(cycle, "report_json", "") or ""),
            "cycle_authority_classification": cycle_report.get("multi_strategy_cycle_authorization_classification"),
            "cycle_submit_allowed": cycle_report.get("cycle_submit_allowed") is True,
            "cycle_broker_mutation_allowed": cycle_report.get("cycle_broker_mutation_allowed") is True,
            "candidate_signal_count": len(cycle_report.get("candidate_signals") or []),
            "suppressed_signal_count": len(cycle_report.get("suppressed_signals") or []),
            "chosen_strategy_id": cycle_report.get("chosen_strategy_id"),
            "submit_authorization_preview": submit_preview,
            "would_submit_preview": submit_preview.get("would_submit_preview") is True,
            "p0_near_miss_shadow": _p0_near_miss_shadow_summary(near_miss_shadow),
            "final_submit_allowed": _normalized_loop_mode(config.mode) == P0_LOOP_MODE_GUARDED_PAPER
            and cycle_report.get("cycle_submit_allowed") is True,
            "broker_adapter_constructed": _normalized_loop_mode(config.mode) == P0_LOOP_MODE_GUARDED_PAPER
            and cycle_report.get("paper_runner_report_path") is not None,
            "submit_attempted": cycle_report.get("submit_attempted") is True,
            "submit_limit_order_called": cycle_report.get("submit_attempted") is True,
            "broker_mutation_attempted": cycle_report.get("submit_attempted") is True,
            "broker_state_mutated": cycle_report.get("broker_state_mutated") is True,
            "per_strategy": _per_strategy_summary(cycle_report),
            "continuation_aware_exit": _continuation_summary(snapshot),
            "primary_blocker": cycle_report.get("primary_blocker"),
            "required_next_action": cycle_report.get("required_next_action"),
        }
    except Exception as exc:  # noqa: BLE001 - loop failures must be artifacted.
        return {
            **base,
            "classification": P0_OBSERVE_LOOP_ENVELOPE_REFRESH_FAILED,
            "primary_blocker": f"P0 observe-only iteration failed: {exc}",
            "required_next_action": "Review P0 observe-only loop diagnostics before continuing.",
        }


def _refresh_p0_envelopes(
    *,
    config: TrackBP0ObserveOnlyLoopConfig,
    stages: TrackBP0ObserveOnlyLoopStages,
    now: datetime,
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    asian = stages.asian_drift(config, now)
    session = stages.session_envelopes(config, now)
    snap = stages.snap_turn_envelopes(config, now)
    asian_report = dict(getattr(asian, "report", {}) or {})
    session_report = dict(getattr(session, "report", {}) or {})
    snap_report = dict(getattr(snap, "report", {}) or {})
    asian_state_path = Path(
        str(
            asian_report.get("asian_drift_state_snapshot_path")
            or config.resolve(Path("outputs/track_b_execution_core/asian_drift_state/latest_asian_drift_5m_state_snapshot.json"))
        )
    )
    envelope_paths = {
        "asian_drift": str(asian_state_path),
        "asia_early_pause_resume_short": _path_str(getattr(session, "asia_early_pause_resume_short_event_json", None)),
        "asia_early_normal_breakout_retest_hold_long": _path_str(
            getattr(session, "asia_early_normal_breakout_retest_hold_long_event_json", None)
        ),
        "first_bull_snap_turn": _path_str(getattr(snap, "first_bull_snap_turn_event_json", None)),
        "first_bear_snap_turn": _path_str(getattr(snap, "first_bear_snap_turn_event_json", None)),
        "london_late_pause_resume_short": _path_str(getattr(session, "london_late_pause_resume_short_event_json", None)),
        "asia_late_flat_pullback_pause_resume_long": _path_str(
            getattr(session, "asia_late_flat_pullback_pause_resume_long_event_json", None)
        ),
        "us_derivative_bear_turn": _path_str(getattr(session, "us_derivative_bear_turn_event_json", None)),
        "mnq_us_derivative_bear_turn": _path_str(getattr(session, "mnq_us_derivative_bear_turn_event_json", None)),
        "mnq_first_bear_snap_turn": _path_str(getattr(snap, "mnq_first_bear_snap_turn_event_json", None)),
        "mnq_first_bull_snap_turn": _path_str(getattr(snap, "mnq_first_bull_snap_turn_event_json", None)),
        "us_late_pause_resume_long": _path_str(getattr(session, "us_late_pause_resume_long_event_json", None)),
    }
    shadow_diagnostics = _research_shadow_diagnostics(
        config=config,
        now=now,
        asian_report=asian_report,
        asian_report_path=getattr(asian, "report_json", None),
        safe_state=snapshot,
    )
    iteration_fields = {
        "envelope_refresh": {
            "asian_drift_watch_verdict": asian_report.get("asian_drift_watch_verdict"),
            "asian_drift_report_path": str(getattr(asian, "report_json", "") or ""),
            "session_strategy_envelope_producer_verdict": session_report.get(
                "session_strategy_envelope_producer_verdict"
            ),
            "session_strategy_report_path": str(getattr(session, "report_json", "") or ""),
            "snap_turn_envelope_producer_verdict": snap_report.get("snap_turn_envelope_producer_verdict"),
            "snap_turn_report_path": str(getattr(snap, "report_json", "") or ""),
            "envelope_paths": envelope_paths,
        },
        "late_join_asian_drift_diagnostic": _asian_late_join_summary(asian_report),
        "research_shadow_diagnostics": _research_shadow_summary(shadow_diagnostics),
    }
    return {
        "iteration_fields": iteration_fields,
        "asian_drift_event_json": asian_state_path,
        "pause_resume_short_event_json": getattr(session, "asia_early_pause_resume_short_event_json", None),
        "breakout_retest_hold_long_event_json": getattr(
            session,
            "asia_early_normal_breakout_retest_hold_long_event_json",
            None,
        ),
        "first_bull_snap_turn_event_json": getattr(snap, "first_bull_snap_turn_event_json", None),
        "first_bear_snap_turn_event_json": getattr(snap, "first_bear_snap_turn_event_json", None),
        "london_late_pause_resume_short_event_json": getattr(session, "london_late_pause_resume_short_event_json", None),
        "asia_late_flat_pullback_pause_resume_long_event_json": getattr(
            session,
            "asia_late_flat_pullback_pause_resume_long_event_json",
            None,
        ),
        "us_derivative_bear_turn_event_json": getattr(session, "us_derivative_bear_turn_event_json", None),
        "mnq_us_derivative_bear_turn_event_json": getattr(session, "mnq_us_derivative_bear_turn_event_json", None),
        "mnq_first_bear_snap_turn_event_json": getattr(snap, "mnq_first_bear_snap_turn_event_json", None),
        "mnq_first_bull_snap_turn_event_json": getattr(snap, "mnq_first_bull_snap_turn_event_json", None),
        "us_late_pause_resume_long_event_json": getattr(session, "us_late_pause_resume_long_event_json", None),
        "research_shadow_diagnostics": shadow_diagnostics,
    }


def _default_phase1_readiness(config: TrackBP0ObserveOnlyLoopConfig, now: datetime) -> Mapping[str, Any]:
    readiness_config = Phase1RuntimeDataReadinessConfig(repo_root=config.repo_root, now=now)
    artifacts = build_phase1_runtime_data_readiness(config=readiness_config)
    write_phase1_runtime_data_readiness_artifacts(config=readiness_config, artifacts=artifacts)
    return artifacts.report


def _default_proof_readiness(config: TrackBP0ObserveOnlyLoopConfig, now: datetime) -> Mapping[str, Any]:
    proof_config = TrackBPaperProofReadinessConfig(repo_root=config.repo_root, now=now)
    payload = build_track_b_paper_proof_readiness(config=proof_config)
    write_track_b_paper_proof_readiness(config=proof_config, payload=payload)
    return payload


def _default_control_plane_snapshot(config: TrackBP0ObserveOnlyLoopConfig, now: datetime) -> Mapping[str, Any]:
    snapshot_config = TrackBControlPlaneSnapshotConfig(repo_root=config.repo_root)
    payload = build_track_b_control_plane_snapshot(config=snapshot_config, now=now)
    write_track_b_control_plane_snapshot(config=snapshot_config, payload=payload)
    return payload


def _default_asian_drift(config: TrackBP0ObserveOnlyLoopConfig, now: datetime) -> Any:
    return run_track_b_asian_drift_watch_chain(
        candle_payload=_read_json(config.resolve(config.mgc_1m_candles_path)),
        source_payload_path=config.resolve(config.mgc_1m_candles_path),
        expected_account_id=config.expected_account_id,
        account_id=config.account_id,
        contract_key="MGC-202606",
        instrument_family="MGC",
        local_symbol="MGCM6",
        dataset="GLBX.MDP3",
        source_id="p0_observe_only_loop_asian_drift",
        strategy_id="asian_drift_v1",
        lane_id="mgc_example_long_lmt_day",
        max_source_bars=250,
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        inbox_dir=config.resolve(config.inbox_dir),
        output_root=config.resolve(Path("outputs/track_b_execution_core/asian_drift_state")),
        now=now,
    )


def _default_session_envelopes(config: TrackBP0ObserveOnlyLoopConfig, now: datetime) -> Any:
    mgc = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=_read_json(config.resolve(config.mgc_5m_candles_path)),
        runtime_5m_payload_path=config.resolve(config.mgc_5m_candles_path),
        expected_account_id=config.expected_account_id,
        source_id="p0_observe_only_loop_session_strategy",
        output_root=config.resolve(Path("outputs/track_b_execution_core/session_strategy_state")),
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        now=now,
    )
    mnq = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=_read_json(config.resolve(config.mnq_5m_candles_path)),
        runtime_5m_payload_path=config.resolve(config.mnq_5m_candles_path),
        expected_account_id=config.expected_account_id,
        source_id="p0_observe_only_loop_mnq_session_strategy",
        output_root=config.resolve(Path("outputs/track_b_execution_core/session_strategy_state")),
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        now=now,
    )
    return SimpleNamespace(
        report={
            "session_strategy_envelope_producer_verdict": (
                "TRACK_B_SESSION_STRATEGY_ENVELOPE_PRODUCER_WROTE_ENVELOPES"
            ),
            "mgc_report": dict(getattr(mgc, "report", {}) or {}),
            "mnq_report": dict(getattr(mnq, "report", {}) or {}),
        },
        report_json=getattr(mgc, "report_json", None),
        mgc_report_json=getattr(mgc, "report_json", None),
        mnq_report_json=getattr(mnq, "report_json", None),
        london_late_pause_resume_short_event_json=getattr(
            mgc, "london_late_pause_resume_short_event_json", None
        ),
        asia_late_flat_pullback_pause_resume_long_event_json=getattr(
            mgc, "asia_late_flat_pullback_pause_resume_long_event_json", None
        ),
        asia_early_pause_resume_short_event_json=getattr(
            mgc, "asia_early_pause_resume_short_event_json", None
        ),
        asia_early_normal_breakout_retest_hold_long_event_json=getattr(
            mgc, "asia_early_normal_breakout_retest_hold_long_event_json", None
        ),
        us_derivative_bear_turn_event_json=getattr(mgc, "us_derivative_bear_turn_event_json", None),
        us_late_pause_resume_long_event_json=getattr(mgc, "us_late_pause_resume_long_event_json", None),
        mnq_us_derivative_bear_turn_event_json=getattr(
            mnq, "mnq_us_derivative_bear_turn_event_json", None
        ),
    )


def _default_snap_turn_envelopes(config: TrackBP0ObserveOnlyLoopConfig, now: datetime) -> Any:
    mgc = produce_track_b_snap_turn_envelopes(
        runtime_5m_payload=_read_json(config.resolve(config.mgc_5m_candles_path)),
        runtime_5m_payload_path=config.resolve(config.mgc_5m_candles_path),
        expected_account_id=config.expected_account_id,
        source_id="p0_observe_only_loop_mgc_snap_turn",
        output_root=config.resolve(Path("outputs/track_b_execution_core/snap_turn_state")),
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        now=now,
    )
    mnq = produce_track_b_snap_turn_envelopes(
        runtime_5m_payload=_read_json(config.resolve(config.mnq_5m_candles_path)),
        runtime_5m_payload_path=config.resolve(config.mnq_5m_candles_path),
        expected_account_id=config.expected_account_id,
        source_id="p0_observe_only_loop_snap_turn",
        output_root=config.resolve(Path("outputs/track_b_execution_core/snap_turn_state")),
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        now=now,
    )
    return SimpleNamespace(
        report={
            "snap_turn_envelope_producer_verdict": "TRACK_B_SNAP_TURN_ENVELOPE_PRODUCER_WROTE_ENVELOPES",
            "mgc_report": dict(getattr(mgc, "report", {}) or {}),
            "mnq_report": dict(getattr(mnq, "report", {}) or {}),
        },
        report_json=getattr(mnq, "report_json", None),
        mgc_report_json=getattr(mgc, "report_json", None),
        mnq_report_json=getattr(mnq, "report_json", None),
        first_bull_snap_turn_event_json=getattr(mgc, "first_bull_snap_turn_event_json", None),
        first_bear_snap_turn_event_json=getattr(mgc, "first_bear_snap_turn_event_json", None),
        mnq_first_bull_snap_turn_event_json=getattr(mnq, "first_bull_snap_turn_event_json", None),
        mnq_first_bear_snap_turn_event_json=getattr(mnq, "first_bear_snap_turn_event_json", None),
    )


def _default_multi_strategy_cycle(
    config: TrackBP0ObserveOnlyLoopConfig,
    now: datetime,
    envelope_context: Mapping[str, Any],
) -> Any:
    return run_track_b_multi_strategy_runtime_cycle(
        config=_multi_strategy_cycle_config(config=config, envelope_context=envelope_context),
        now=now,
    )


def _research_shadow_diagnostics(
    *,
    config: TrackBP0ObserveOnlyLoopConfig,
    now: datetime,
    asian_report: Mapping[str, Any],
    asian_report_path: Any,
    safe_state: Mapping[str, Any],
) -> dict[str, Any]:
    shadow_config = TrackBResearchShadowDriftConfig(repo_root=config.repo_root)
    late_join = build_late_join_asian_drift_shadow(
        asian_drift_report=asian_report,
        p0_loop_events=(),
        source_report_path=asian_report_path,
        now=now,
    )
    gap_drift = build_gap_drift_continuation_shadow(
        candle_payloads={
            "MGC": _read_json(config.resolve(config.mgc_5m_candles_path)),
            "MNQ": _read_json(config.resolve(config.mnq_5m_candles_path)),
        },
        source_paths={
            "MGC": config.resolve(config.mgc_5m_candles_path),
            "MNQ": config.resolve(config.mnq_5m_candles_path),
        },
        safe_state=safe_state,
        now=now,
    )
    paths = write_research_shadow_payloads(
        config=shadow_config,
        late_join_shadow=late_join,
        gap_drift_shadow=gap_drift,
    )
    return {
        "late_join_asian_drift_shadow": late_join,
        "gap_drift_continuation_shadow": gap_drift,
        **paths,
    }


def _p0_near_miss_shadow_diagnostics(
    *,
    config: TrackBP0ObserveOnlyLoopConfig,
    now: datetime,
    cycle_report: Mapping[str, Any],
    cycle_report_path: Any,
    prior_shadow_diagnostics: Any,
) -> dict[str, Any]:
    shadow_config = TrackBResearchShadowDriftConfig(repo_root=config.repo_root)
    near_miss = build_p0_near_miss_shadow(
        evaluated_strategies=list(cycle_report.get("evaluated_strategies") or []),
        source_report_path=cycle_report_path,
        now=now,
    )
    prior = prior_shadow_diagnostics if isinstance(prior_shadow_diagnostics, Mapping) else {}
    paths = write_research_shadow_payloads(
        config=shadow_config,
        late_join_shadow=prior.get("late_join_asian_drift_shadow") or {},
        gap_drift_shadow=prior.get("gap_drift_continuation_shadow") or {},
        p0_near_miss_shadow=near_miss,
    )
    return {
        "p0_near_miss_shadow": near_miss,
        **paths,
    }


def _multi_strategy_cycle_config(
    *,
    config: TrackBP0ObserveOnlyLoopConfig,
    envelope_context: Mapping[str, Any],
) -> TrackBMultiStrategyRuntimeCycleConfig:
    guarded = _normalized_loop_mode(config.mode) == P0_LOOP_MODE_GUARDED_PAPER
    enabled_strategy_ids = tuple(envelope_context.get("enabled_strategy_ids") or config.enabled_strategy_ids)
    return TrackBMultiStrategyRuntimeCycleConfig(
        enabled_strategy_ids=enabled_strategy_ids,
        asian_drift_event_json=_optional_path(envelope_context.get("asian_drift_event_json")),
        pause_resume_short_event_json=_optional_path(envelope_context.get("pause_resume_short_event_json")),
        breakout_retest_hold_long_event_json=_optional_path(
            envelope_context.get("breakout_retest_hold_long_event_json")
        ),
        first_bull_snap_turn_event_json=_optional_path(
            envelope_context.get("first_bull_snap_turn_event_json")
        ),
        first_bear_snap_turn_event_json=_optional_path(
            envelope_context.get("first_bear_snap_turn_event_json")
        ),
        london_late_pause_resume_short_event_json=_optional_path(
            envelope_context.get("london_late_pause_resume_short_event_json")
        ),
        asia_late_flat_pullback_pause_resume_long_event_json=_optional_path(
            envelope_context.get("asia_late_flat_pullback_pause_resume_long_event_json")
        ),
        us_derivative_bear_turn_event_json=_optional_path(
            envelope_context.get("us_derivative_bear_turn_event_json")
        ),
        mnq_us_derivative_bear_turn_event_json=_optional_path(
            envelope_context.get("mnq_us_derivative_bear_turn_event_json")
        ),
        mnq_first_bear_snap_turn_event_json=_optional_path(
            envelope_context.get("mnq_first_bear_snap_turn_event_json")
        ),
        mnq_first_bull_snap_turn_event_json=_optional_path(
            envelope_context.get("mnq_first_bull_snap_turn_event_json")
        ),
        us_late_pause_resume_long_event_json=_optional_path(
            envelope_context.get("us_late_pause_resume_long_event_json")
        ),
        inbox_dir=config.resolve(config.inbox_dir),
        expected_account_id=config.expected_account_id,
        account_id=config.account_id,
        source_id=f"p0_{_normalized_loop_mode(config.mode).replace('-', '_')}_loop_multi_strategy_cycle",
        side="AUTO",
        quantity=1,
        submit_paper=guarded,
        confirm_paper_submit=guarded,
        paper_order_pricing_policy="LIMIT_AT_LAST",
        mgc_pricing_context_json=config.resolve(config.mgc_1m_candles_path),
        mnq_pricing_context_json=config.resolve(config.mnq_1m_candles_path),
        update_operator_status=config.update_operator_status,
        output_root=config.resolve(Path("outputs/track_b_execution_core/track_b_multi_strategy_runtime_cycle")),
        strategy_rule_output_root=config.resolve(Path("outputs/track_b_execution_core/track_b_strategy_rule_runner")),
        strategy_paper_runner_output_root=config.resolve(Path("outputs/track_b_execution_core/track_b_strategy_paper_runner")),
        operator_status_output_root=config.resolve(Path("outputs/track_b_execution_core/operator_status")),
        repo_root=config.repo_root,
    )


def _base_payload(
    *,
    config: TrackBP0ObserveOnlyLoopConfig,
    loop_id: str,
    classification: str,
    iterations: int,
    sleep_seconds: float,
    iteration_reports: Sequence[Mapping[str, Any]],
    generated_at: str,
) -> dict[str, Any]:
    guarded = _normalized_loop_mode(config.mode) == P0_LOOP_MODE_GUARDED_PAPER
    return {
        "schema_version": "track_b_p0_observe_only_loop_v1",
        "p0_observe_only_loop_id": loop_id,
        "generated_at": generated_at,
        "classification": classification,
        "loop_start_classification": _ready_classification(config.mode),
        "mode": "PAPER",
        "loop_mode": _normalized_loop_mode(config.mode),
        "read_only": not guarded,
        "observe_only": not guarded,
        "dry_run_only": not guarded,
        "submit_disabled": _normalized_loop_mode(config.mode) == P0_LOOP_MODE_SUBMIT_DISABLED,
        "guarded_paper_enabled": guarded,
        "submit_authority": guarded,
        "submit_delegation_forced_off": not guarded,
        "broker_mutation_allowed": guarded,
        "broker_state_mutated": False,
        "lifecycle_mutation_allowed": False,
        "runtime_restart_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "configured_iterations": iterations,
        "completed_iterations": len(iteration_reports),
        "sleep_seconds": sleep_seconds,
        "roster_name": config.roster_name,
        "enabled_strategy_ids": list(config.enabled_strategy_ids),
        "roster_config_path": str(config.resolve(config.roster_config_path))
        if config.roster_config_path is not None
        else None,
        "roster_hot_reload_enabled": config.roster_config_path is not None,
        "p0_strategy_ids": list(P0_STRATEGY_IDS),
        "older_approved_paper_strategy_ids": list(OLDER_APPROVED_PAPER_STRATEGY_IDS),
        "source_paths": {
            "mgc_1m_candles": str(config.resolve(config.mgc_1m_candles_path)),
            "mgc_5m_candles": str(config.resolve(config.mgc_5m_candles_path)),
            "mnq_1m_candles": str(config.resolve(config.mnq_1m_candles_path)),
            "mnq_5m_candles": str(config.resolve(config.mnq_5m_candles_path)),
        },
        "iterations": [dict(item) for item in iteration_reports],
        "latest_report_json_path": str(config.resolve(config.output_path)),
        "event_log_path": str(config.resolve(config.event_log_path)),
    }


def _control_plane_blocker(snapshot: Mapping[str, Any]) -> str | None:
    if snapshot.get("classification") != CONTROL_PLANE_SNAPSHOT_READY:
        return f"Control Plane Snapshot classification is {snapshot.get('classification')}; expected {CONTROL_PLANE_SNAPSHOT_READY}."
    if snapshot.get("shared_truth_coherence_status") != "COHERENT":
        return f"Shared truth coherence is {snapshot.get('shared_truth_coherence_status')}; expected COHERENT."
    if snapshot.get("live_money_eligible") is not False:
        return "Control Plane Snapshot does not prove live_money_eligible=false."
    if snapshot.get("paper_proof_invoked") is not False:
        return "Control Plane Snapshot does not prove paper_proof_invoked=false."
    return None


def _normalized_loop_mode(mode: str) -> str:
    value = str(mode or P0_LOOP_MODE_OBSERVE_ONLY).strip().lower().replace("_", "-")
    if value == P0_LOOP_MODE_GUARDED_PAPER:
        return P0_LOOP_MODE_GUARDED_PAPER
    if value == P0_LOOP_MODE_SUBMIT_DISABLED:
        return P0_LOOP_MODE_SUBMIT_DISABLED
    return P0_LOOP_MODE_OBSERVE_ONLY


def _normalized_roster_name(roster_name: str) -> str:
    value = str(roster_name or P0_ROSTER).strip().lower().replace("_", "-")
    if value == APPROVED_PAPER_ROSTER:
        return APPROVED_PAPER_ROSTER
    return P0_ROSTER


def _strategy_ids_for_roster(roster_name: str) -> tuple[str, ...]:
    if _normalized_roster_name(roster_name) == APPROVED_PAPER_ROSTER:
        return APPROVED_PAPER_STRATEGY_IDS
    return P0_STRATEGY_IDS


def _ready_classification(mode: str) -> str:
    if _normalized_loop_mode(mode) == P0_LOOP_MODE_GUARDED_PAPER:
        return P0_GUARDED_PAPER_LOOP_READY
    return (
        P0_SUBMIT_DISABLED_LOOP_READY
        if _normalized_loop_mode(mode) == P0_LOOP_MODE_SUBMIT_DISABLED
        else P0_OBSERVE_LOOP_READY
    )


def _blocked_control_plane_classification(mode: str) -> str:
    if _normalized_loop_mode(mode) == P0_LOOP_MODE_GUARDED_PAPER:
        return P0_GUARDED_PAPER_LOOP_BLOCKED_CONTROL_PLANE
    return (
        P0_SUBMIT_DISABLED_LOOP_BLOCKED_CONTROL_PLANE
        if _normalized_loop_mode(mode) == P0_LOOP_MODE_SUBMIT_DISABLED
        else P0_OBSERVE_LOOP_BLOCKED_CONTROL_PLANE
    )


def _blocked_safe_state_classification(mode: str) -> str:
    if _normalized_loop_mode(mode) == P0_LOOP_MODE_GUARDED_PAPER:
        return P0_GUARDED_PAPER_LOOP_BLOCKED_SAFE_STATE
    return (
        P0_SUBMIT_DISABLED_LOOP_SAFETY_BOUNDARY_FAILED
        if _normalized_loop_mode(mode) == P0_LOOP_MODE_SUBMIT_DISABLED
        else P0_OBSERVE_LOOP_BLOCKED_SAFE_STATE
    )


def _blocked_roster_classification(mode: str) -> str:
    if _normalized_loop_mode(mode) == P0_LOOP_MODE_GUARDED_PAPER:
        return P0_GUARDED_PAPER_LOOP_BLOCKED_ROSTER
    return P0_OBSERVE_LOOP_ENVELOPE_REFRESH_FAILED


def _resolve_guarded_paper_roster(config: TrackBP0ObserveOnlyLoopConfig) -> dict[str, Any]:
    source_path = config.resolve(config.roster_config_path) if config.roster_config_path is not None else None
    payload: Mapping[str, Any] = {}
    source_exists = False
    load_error: str | None = None
    if source_path is not None and source_path.exists():
        source_exists = True
        try:
            payload = _read_json(source_path)
        except Exception as exc:  # noqa: BLE001 - roster errors must be artifacted.
            load_error = str(exc)
    requested = _coerce_strategy_ids(payload.get("enabled_strategy_ids") if isinstance(payload, Mapping) else None)
    if not requested:
        requested = list(config.enabled_strategy_ids)
    disabled = set(_coerce_strategy_ids(payload.get("disabled_strategy_ids") if isinstance(payload, Mapping) else None))
    enabled: list[str] = []
    rejected: list[dict[str, Any]] = []
    seen: set[str] = set()
    registry_by_id = {entry.strategy_id: entry for entry in get_track_b_strategy_registry()}
    config_account = str(payload.get("paper_account_id") or config.account_id) if isinstance(payload, Mapping) else config.account_id
    config_live_money = payload.get("live_money_eligible") if isinstance(payload, Mapping) else None
    config_paper_proof = payload.get("paper_proof_invoked") if isinstance(payload, Mapping) else None

    if load_error:
        return {
            "classification": TRACK_B_GUARDED_PAPER_ROSTER_BLOCKED_INVALID_CONFIG,
            "source_path": str(source_path) if source_path is not None else None,
            "source_exists": source_exists,
            "enabled_strategy_ids": [],
            "enabled_strategy_count": 0,
            "rejected_strategy_ids": [],
            "rejections": [{"strategy_id": None, "reason": load_error}],
            "primary_blocker": f"Guarded PAPER roster config could not be read: {load_error}",
            "hot_reload_enabled": source_path is not None,
            "dashboard_projection_consumed": False,
        }
    required_error = _guarded_paper_roster_required_field_error(payload) if source_exists else None
    if required_error:
        return {
            "classification": TRACK_B_GUARDED_PAPER_ROSTER_BLOCKED_INVALID_CONFIG,
            "source_path": str(source_path) if source_path is not None else None,
            "source_exists": source_exists,
            "enabled_strategy_ids": [],
            "enabled_strategy_count": 0,
            "rejected_strategy_ids": list(requested),
            "rejections": [{"strategy_id": strategy_id, "reason": required_error} for strategy_id in requested],
            "primary_blocker": required_error,
            "hot_reload_enabled": source_path is not None,
            "dashboard_projection_consumed": False,
        }
    if config_account != config.expected_account_id:
        return {
            "classification": TRACK_B_GUARDED_PAPER_ROSTER_BLOCKED_INVALID_CONFIG,
            "source_path": str(source_path) if source_path is not None else None,
            "source_exists": source_exists,
            "enabled_strategy_ids": [],
            "enabled_strategy_count": 0,
            "rejected_strategy_ids": list(requested),
            "rejections": [
                {
                    "strategy_id": strategy_id,
                    "reason": f"roster paper_account_id={config_account}; expected {config.expected_account_id}",
                }
                for strategy_id in requested
            ],
            "primary_blocker": f"Guarded PAPER roster must target account {config.expected_account_id}.",
            "hot_reload_enabled": source_path is not None,
            "dashboard_projection_consumed": False,
        }
    if config_live_money is True or config_paper_proof is True:
        reason = "roster must keep live_money_eligible=false and paper_proof_invoked=false"
        return {
            "classification": TRACK_B_GUARDED_PAPER_ROSTER_BLOCKED_INVALID_CONFIG,
            "source_path": str(source_path) if source_path is not None else None,
            "source_exists": source_exists,
            "enabled_strategy_ids": [],
            "enabled_strategy_count": 0,
            "rejected_strategy_ids": list(requested),
            "rejections": [{"strategy_id": strategy_id, "reason": reason} for strategy_id in requested],
            "primary_blocker": reason,
            "hot_reload_enabled": source_path is not None,
            "dashboard_projection_consumed": False,
        }

    for strategy_id in requested:
        if strategy_id in disabled:
            rejected.append({"strategy_id": strategy_id, "reason": "disabled_by_roster"})
            continue
        if strategy_id in seen:
            rejected.append({"strategy_id": strategy_id, "reason": "duplicate_in_roster"})
            continue
        seen.add(strategy_id)
        entry = registry_by_id.get(strategy_id)
        reason = _guarded_paper_strategy_rejection_reason(entry)
        if reason:
            rejected.append({"strategy_id": strategy_id, "reason": reason})
            continue
        enabled.append(strategy_id)

    if not enabled:
        classification = TRACK_B_GUARDED_PAPER_ROSTER_BLOCKED_EMPTY
        primary_blocker = "No guarded PAPER strategies remain enabled after registry and lifecycle validation."
    else:
        classification = (
            TRACK_B_GUARDED_PAPER_ROSTER_READY_WITH_REJECTIONS
            if rejected
            else TRACK_B_GUARDED_PAPER_ROSTER_READY
        )
        primary_blocker = None
    return {
        "classification": classification,
        "source_path": str(source_path) if source_path is not None else None,
        "source_exists": source_exists,
        "hot_reload_enabled": source_path is not None,
        "paper_account_id": config_account,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "requested_strategy_ids": requested,
        "disabled_strategy_ids": sorted(disabled),
        "enabled_strategy_ids": enabled,
        "enabled_strategy_count": len(enabled),
        "rejected_strategy_ids": [str(item.get("strategy_id")) for item in rejected if item.get("strategy_id")],
        "rejections": rejected,
        "primary_blocker": primary_blocker,
        "dashboard_projection_consumed": False,
    }


def _coerce_strategy_ids(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _guarded_paper_roster_required_field_error(payload: Mapping[str, Any]) -> str | None:
    if not isinstance(payload, Mapping):
        return "Guarded PAPER roster must be a JSON object."
    schema_version = str(payload.get("schema_version") or "").strip()
    if schema_version != TRACK_B_GUARDED_PAPER_ROSTER_SCHEMA_VERSION:
        return (
            "Guarded PAPER roster schema_version must be "
            f"{TRACK_B_GUARDED_PAPER_ROSTER_SCHEMA_VERSION}."
        )
    required_fields = (
        "paper_account_id",
        "live_money_eligible",
        "paper_proof_invoked",
        "enabled_strategy_ids",
        "disabled_strategy_ids",
        "max_quantity_per_strategy",
    )
    missing = [field for field in required_fields if field not in payload]
    if missing:
        return f"Guarded PAPER roster missing required field(s): {', '.join(missing)}."
    if not isinstance(payload.get("enabled_strategy_ids"), list):
        return "Guarded PAPER roster enabled_strategy_ids must be a list."
    if not isinstance(payload.get("disabled_strategy_ids"), list):
        return "Guarded PAPER roster disabled_strategy_ids must be a list."
    try:
        max_qty = int(payload.get("max_quantity_per_strategy"))
    except (TypeError, ValueError):
        return "Guarded PAPER roster max_quantity_per_strategy must be integer 1."
    if max_qty != 1:
        return "Guarded PAPER roster max_quantity_per_strategy must be exactly 1."
    return None


def _guarded_paper_strategy_rejection_reason(entry: Any) -> str | None:
    if entry is None:
        return "strategy_not_registered"
    if getattr(entry, "paper_eligible", None) is not True:
        return "strategy_not_paper_eligible"
    if getattr(entry, "live_money_eligible", None) is not False:
        return "strategy_live_money_eligible_not_false"
    if getattr(entry, "exit_not_available", True) is True or not getattr(entry, "managed_exit_policy_id", None):
        return "missing_managed_exit_policy"
    if getattr(entry, "strategy_id", "") in {"track_b_demo_wiring_proof", "human_review_only"}:
        return "diagnostic_or_manual_review_strategy_not_guarded_paper"
    return None


def _iteration_success_classification(
    *,
    mode: str,
    candidate_count: int,
    submit_preview: Mapping[str, Any],
) -> str:
    if _normalized_loop_mode(mode) == P0_LOOP_MODE_GUARDED_PAPER:
        return P0_GUARDED_PAPER_LOOP_CANDIDATE_HANDOFF if candidate_count > 0 else P0_GUARDED_PAPER_LOOP_NO_CANDIDATE
    if _normalized_loop_mode(mode) != P0_LOOP_MODE_SUBMIT_DISABLED:
        return P0_OBSERVE_LOOP_ITERATION_OK
    if candidate_count > 0 or submit_preview.get("would_submit_preview") is True:
        return P0_SUBMIT_DISABLED_LOOP_CANDIDATE_PREVIEW_READY
    return P0_SUBMIT_DISABLED_LOOP_NO_CANDIDATE


def _is_success_iteration(classification: str, mode: str) -> bool:
    if _normalized_loop_mode(mode) == P0_LOOP_MODE_GUARDED_PAPER:
        return classification in {
            P0_GUARDED_PAPER_LOOP_NO_CANDIDATE,
            P0_GUARDED_PAPER_LOOP_CANDIDATE_HANDOFF,
        }
    if _normalized_loop_mode(mode) == P0_LOOP_MODE_SUBMIT_DISABLED:
        return classification in {
            P0_SUBMIT_DISABLED_LOOP_ITERATION_OK,
            P0_SUBMIT_DISABLED_LOOP_NO_CANDIDATE,
            P0_SUBMIT_DISABLED_LOOP_CANDIDATE_PREVIEW_READY,
        }
    return classification == P0_OBSERVE_LOOP_ITERATION_OK


def _completed_classification(mode: str, iteration_reports: Sequence[Mapping[str, Any]]) -> str:
    if _normalized_loop_mode(mode) == P0_LOOP_MODE_GUARDED_PAPER:
        if any(
            item.get("classification") == P0_GUARDED_PAPER_LOOP_CANDIDATE_HANDOFF
            for item in iteration_reports
        ):
            return P0_GUARDED_PAPER_LOOP_CANDIDATE_HANDOFF
        return P0_GUARDED_PAPER_LOOP_COMPLETED
    if _normalized_loop_mode(mode) != P0_LOOP_MODE_SUBMIT_DISABLED:
        return P0_OBSERVE_LOOP_COMPLETED
    if any(
        item.get("classification") == P0_SUBMIT_DISABLED_LOOP_CANDIDATE_PREVIEW_READY
        for item in iteration_reports
    ):
        return P0_SUBMIT_DISABLED_LOOP_CANDIDATE_PREVIEW_READY
    return P0_SUBMIT_DISABLED_LOOP_NO_CANDIDATE


def _submit_disabled_safety_failure(
    *,
    config: TrackBP0ObserveOnlyLoopConfig,
    cycle_report: Mapping[str, Any],
) -> str | None:
    if _normalized_loop_mode(config.mode) != P0_LOOP_MODE_SUBMIT_DISABLED:
        return None
    if cycle_report.get("paper_runner_report_path"):
        return "Submit-disabled mode reached the strategy paper runner report boundary."
    if cycle_report.get("managed_lifecycle_invoked") is True:
        return "Submit-disabled mode invoked strategy-managed lifecycle."
    if cycle_report.get("submit_attempted") is True:
        return "Submit-disabled mode attempted submit."
    if cycle_report.get("broker_state_mutated") is True:
        return "Submit-disabled mode mutated broker state."
    if cycle_report.get("paper_proof_invoked") is True:
        return "Submit-disabled mode invoked paper_proof."
    return None


def _empty_submit_disabled_preview(cycle_report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "mode": P0_LOOP_MODE_SUBMIT_DISABLED,
        "would_submit_preview": False,
        "candidate_count": len(cycle_report.get("candidate_signals") or []),
        "chosen_strategy_id": cycle_report.get("chosen_strategy_id"),
        "execution_enabled": False,
        "final_submit_allowed": False,
        "broker_mutation_allowed": False,
        "broker_adapter_constructed": False,
        "submit_limit_order_called": False,
        "dashboard_projection_consumed": False,
    }


def _submit_disabled_preview(
    *,
    config: TrackBP0ObserveOnlyLoopConfig,
    now: datetime,
    envelope_context: Mapping[str, Any],
    cycle_report: Mapping[str, Any],
) -> dict[str, Any]:
    if _normalized_loop_mode(config.mode) != P0_LOOP_MODE_SUBMIT_DISABLED:
        return {}
    preview = _empty_submit_disabled_preview(cycle_report)
    candidates = list(cycle_report.get("candidate_signals") or [])
    if not candidates:
        return {
            **preview,
            "classification": P0_SUBMIT_DISABLED_LOOP_NO_CANDIDATE,
            "reason": "No natural P0 candidate reached submit-disabled preview.",
        }
    cycle_config = _multi_strategy_cycle_config(config=config, envelope_context=envelope_context)
    authority = build_track_b_multi_strategy_cycle_authority(
        config=cycle_config,
        submit_requested=True,
        now=now,
    )
    return {
        **preview,
        "classification": P0_SUBMIT_DISABLED_LOOP_CANDIDATE_PREVIEW_READY,
        "would_submit_preview": True,
        "cycle_submit_authority_preview": authority,
        "submit_authorization_preview_built": True,
        "authorization_classification": authority.get("authorization_classification"),
        "control_plane_snapshot_id": authority.get("control_plane_snapshot_id"),
        "shared_truth_generation_id": authority.get("shared_truth_generation_id"),
        "runtime_generation_id": authority.get("runtime_generation_id"),
        "safe_state_classification": authority.get("safe_state_classification"),
        "runtime_supervisor_classification": authority.get("runtime_supervisor_classification"),
        "runtime_resume_action_policy": authority.get("runtime_resume_action_policy"),
        "final_submit_allowed": False,
        "broker_mutation_allowed": False,
        "execution_enabled": False,
        "broker_adapter_constructed": False,
        "submit_limit_order_called": False,
        "reason": "Candidate reached submit-disabled authority preview; final broker boundary remains disabled.",
    }


def _safe_state_blocker(snapshot: Mapping[str, Any]) -> str | None:
    safe_state = str(snapshot.get("safe_state_classification") or "")
    if safe_state != "SAFE_STATE_NORMAL":
        return f"Safe-State classification is {safe_state or 'MISSING'}; expected SAFE_STATE_NORMAL."
    if snapshot.get("safe_state_observe_only") is True:
        return "Safe-State is observe-only containment; P0 loop stops for explicit operator review of posture."
    return None


def _phase1_summary(phase1: Mapping[str, Any]) -> dict[str, Any]:
    rows = list(phase1.get("rows") or [])
    return {
        "generated_at": phase1.get("generated_at"),
        "ready_ticker_count": phase1.get("ready_ticker_count"),
        "row_count": phase1.get("row_count"),
        "required_p0_rows": [
            {
                "symbol": row.get("symbol"),
                "runtime_candles_ready": row.get("runtime_candles_ready"),
                "runtime_candles_block_reason": row.get("runtime_candles_block_reason"),
                "derived_features_ready": row.get("derived_features_ready"),
                "derived_features_block_reason": row.get("derived_features_block_reason"),
            }
            for row in rows
            if row.get("symbol") in {"MGC", "MNQ"}
        ],
    }


def _control_plane_summary(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "control_plane_snapshot_id": snapshot.get("control_plane_snapshot_id"),
        "classification": snapshot.get("classification"),
        "shared_truth_refresh_generation_id": snapshot.get("shared_truth_refresh_generation_id"),
        "shared_truth_coherence_status": snapshot.get("shared_truth_coherence_status"),
        "runtime_supervisor_classification": snapshot.get("runtime_supervisor_classification"),
        "supervisor_mode": snapshot.get("supervisor_mode"),
        "proof_window_status": snapshot.get("proof_window_status"),
        "safe_state_classification": snapshot.get("safe_state_classification"),
        "safe_state_observe_only": snapshot.get("safe_state_observe_only") is True,
        "safe_state_submit_allowed": snapshot.get("safe_state_submit_allowed") is True,
        "live_money_eligible": snapshot.get("live_money_eligible"),
        "paper_proof_invoked": snapshot.get("paper_proof_invoked"),
        "blockers": list(snapshot.get("blockers") or []),
        "warnings": list(snapshot.get("warnings") or []),
    }


def _per_strategy_summary(cycle_report: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in cycle_report.get("evaluated_strategies") or []:
        if not isinstance(row, Mapping):
            continue
        rows.append(
            {
                "strategy_id": row.get("strategy_id"),
                "strategy_runtime_verdict": row.get("strategy_runtime_verdict"),
                "decision": row.get("decision"),
                "decision_reason": row.get("decision_reason"),
                "signal_emitted": row.get("signal_emitted") is True,
                "signal_direction": row.get("signal_direction"),
                "input_event_path": row.get("input_event_path"),
                "failed_predicates": [
                    key for key, value in dict(row.get("rule_conditions") or {}).items() if value is False
                ],
                "rule_blockers": list(row.get("rule_blockers") or []),
                "late_join_diagnostic": row.get("late_join_diagnostic") is True,
                "late_join_classification": row.get("late_join_classification"),
                "asian_drift_diagnostic_classification": row.get("asian_drift_diagnostic_classification"),
                "operator_explanation": row.get("operator_explanation"),
            }
        )
    return rows


def _asian_late_join_summary(report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "asian_drift_diagnostic_classification": report.get("asian_drift_diagnostic_classification"),
        "late_join_classification": report.get("late_join_classification"),
        "late_join_diagnostic": report.get("late_join_diagnostic") is True,
        "anchor_required": report.get("anchor_required"),
        "anchor_observed": report.get("anchor_observed"),
        "anchor_window_start": report.get("anchor_window_start"),
        "anchor_window_end": report.get("anchor_window_end"),
        "runtime_context_start": report.get("runtime_context_start"),
        "missing_anchor_reason": report.get("missing_anchor_reason"),
        "drift_observed_after_anchor": report.get("drift_observed_after_anchor"),
        "late_join_policy": report.get("late_join_policy"),
        "hypothetical_late_join_score": report.get("hypothetical_late_join_score"),
        "operator_explanation": report.get("operator_explanation"),
        "submit_allowed": report.get("submit_allowed") is True,
        "no_mutation": report.get("no_mutation") is True,
    }


def _research_shadow_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    late_join = payload.get("late_join_asian_drift_shadow")
    gap_drift = payload.get("gap_drift_continuation_shadow")
    late_join_payload = late_join if isinstance(late_join, Mapping) else {}
    gap_drift_payload = gap_drift if isinstance(gap_drift, Mapping) else {}
    return {
        "late_join_asian_drift_shadow_classification": late_join_payload.get("shadow_classification"),
        "late_join_hypothetical_direction": late_join_payload.get("hypothetical_direction"),
        "late_join_hypothetical_score": late_join_payload.get("hypothetical_score"),
        "gap_drift_continuation_shadow_classification": gap_drift_payload.get("shadow_classification"),
        "gap_drift_symbol": gap_drift_payload.get("symbol"),
        "gap_drift_hypothetical_direction": gap_drift_payload.get("hypothetical_direction"),
        "gap_drift_hypothetical_score": gap_drift_payload.get("hypothetical_score"),
        "dry_run_only": True,
        "research_only": True,
        "submit_allowed": False,
        "broker_mutation_allowed": False,
        "not_order_authority": True,
        "not_lifecycle_authority": True,
        "latest_late_join_asian_drift_shadow_path": payload.get("latest_late_join_asian_drift_shadow_path"),
        "latest_gap_drift_continuation_shadow_path": payload.get("latest_gap_drift_continuation_shadow_path"),
    }


def _p0_near_miss_shadow_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    near_miss = payload.get("p0_near_miss_shadow")
    near_miss_payload = near_miss if isinstance(near_miss, Mapping) else {}
    return {
        "shadow_classification": near_miss_payload.get("shadow_classification"),
        "b_grade_candidate_count": near_miss_payload.get("b_grade_candidate_count"),
        "primary_shadow_strategy_id": near_miss_payload.get("primary_shadow_strategy_id"),
        "primary_entry_grade": near_miss_payload.get("primary_entry_grade"),
        "hypothetical_direction": near_miss_payload.get("hypothetical_direction"),
        "hypothetical_score": near_miss_payload.get("hypothetical_score"),
        "entry_grade_summary": near_miss_payload.get("entry_grade_summary"),
        "dry_run_only": True,
        "research_only": True,
        "submit_allowed": False,
        "broker_mutation_allowed": False,
        "not_order_authority": True,
        "not_lifecycle_authority": True,
        "latest_p0_near_miss_shadow_path": payload.get("latest_p0_near_miss_shadow_path"),
    }


def _continuation_summary(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "strategy_id": snapshot.get("continuation_aware_exit_strategy_id"),
        "symbol": snapshot.get("continuation_aware_exit_symbol"),
        "exit_policy_id": snapshot.get("continuation_aware_exit_policy_id"),
        "exit_profile_id": snapshot.get("continuation_aware_exit_profile_id"),
        "exit_state": snapshot.get("continuation_aware_exit_state"),
        "quality_state": snapshot.get("continuation_aware_exit_quality_state"),
        "should_request_close": snapshot.get("continuation_aware_exit_should_request_close") is True,
        "dry_run_only": snapshot.get("continuation_aware_exit_dry_run_only") is True,
        "not_order_authority": snapshot.get("continuation_aware_exit_not_order_authority") is not False,
        "not_lifecycle_authority": snapshot.get("continuation_aware_exit_not_lifecycle_authority") is not False,
        "history_classification": snapshot.get("continuation_aware_exit_history_classification"),
        "history_total_events": snapshot.get("continuation_aware_exit_history_total_events"),
    }


def _latest_runtime_candle_timestamp(path: Path) -> str | None:
    payload = _read_json(path)
    bars = payload.get("bars") or payload.get("candles") or payload.get("candle_history") or []
    if not isinstance(bars, list) or not bars:
        return None
    latest = bars[-1]
    if not isinstance(latest, Mapping):
        return None
    return str(
        latest.get("bar_end")
        or latest.get("candle_timestamp")
        or latest.get("timestamp")
        or latest.get("bar_start")
        or ""
    ) or None


def _write_latest_and_event(
    *,
    config: TrackBP0ObserveOnlyLoopConfig,
    payload: Mapping[str, Any],
    event: Mapping[str, Any] | None,
) -> None:
    write_json_atomic(config.resolve(config.output_path), payload)
    if event is None:
        return
    event_log_path = config.resolve(config.event_log_path)
    event_log_path.parent.mkdir(parents=True, exist_ok=True)
    with event_log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(event), sort_keys=True) + "\n")


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return payload


def _path_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _optional_path(value: Any) -> Path | None:
    if value is None:
        return None
    return Path(str(value))


def _now(now_factory: Callable[[], datetime] | None) -> datetime:
    value = now_factory() if now_factory is not None else datetime.now(UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a bounded read-only Track B P0 observe-only loop.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument(
        "--mode",
        choices=(P0_LOOP_MODE_OBSERVE_ONLY, P0_LOOP_MODE_SUBMIT_DISABLED, P0_LOOP_MODE_GUARDED_PAPER),
        default=P0_LOOP_MODE_OBSERVE_ONLY,
        help=(
            "submit-disabled allows candidate/authorization previews while keeping broker submit impossible; "
            "guarded-paper enables the hardened PAPER managed lifecycle boundary when all hard gates pass."
        ),
    )
    parser.add_argument("--iterations", type=int, default=12)
    parser.add_argument("--sleep-seconds", type=float, default=60.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--roster",
        choices=(P0_ROSTER, APPROVED_PAPER_ROSTER),
        default=P0_ROSTER,
        help=(
            "p0 keeps the original five-strategy proving roster; approved-paper expands to older approved "
            "paper-eligible strategies that have hardened managed lifecycle submit paths."
        ),
    )
    parser.add_argument("--max-completed-5m-age-seconds", type=int, default=900)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_P0_OBSERVE_ONLY_LATEST)
    parser.add_argument("--event-log-path", type=Path, default=DEFAULT_P0_OBSERVE_ONLY_EVENTS)
    parser.add_argument("--roster-config-json", type=Path, default=DEFAULT_GUARDED_PAPER_ROSTER_CONFIG)
    parser.add_argument("--mgc-1m-candles-json", type=Path, default=DEFAULT_MGC_1M_CANDLES)
    parser.add_argument("--mgc-5m-candles-json", type=Path, default=DEFAULT_MGC_5M_CANDLES)
    parser.add_argument("--mnq-1m-candles-json", type=Path, default=DEFAULT_MNQ_1M_CANDLES)
    parser.add_argument("--mnq-5m-candles-json", type=Path, default=DEFAULT_MNQ_5M_CANDLES)
    parser.add_argument("--inbox-dir", type=Path, default=Path("examples/track_b_shadow_listener/inbox"))
    parser.add_argument("--no-update-operator-status", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    roster_name = _normalized_roster_name(str(args.roster))
    config = TrackBP0ObserveOnlyLoopConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        mode=str(args.mode),
        roster_name=roster_name,
        enabled_strategy_ids=_strategy_ids_for_roster(roster_name),
        roster_config_path=Path(args.roster_config_json) if args.roster_config_json else None,
        output_path=Path(args.output_path),
        event_log_path=Path(args.event_log_path),
        mgc_1m_candles_path=Path(args.mgc_1m_candles_json),
        mgc_5m_candles_path=Path(args.mgc_5m_candles_json),
        mnq_1m_candles_path=Path(args.mnq_1m_candles_json),
        mnq_5m_candles_path=Path(args.mnq_5m_candles_json),
        inbox_dir=Path(args.inbox_dir),
        iterations=1 if bool(args.once) else int(args.iterations),
        sleep_seconds=float(args.sleep_seconds),
        max_completed_5m_age_seconds=int(args.max_completed_5m_age_seconds),
        update_operator_status=not bool(args.no_update_operator_status),
    )
    payload = run_track_b_p0_observe_only_loop(config=config)
    summary = {
        "classification": payload.get("classification"),
        "completed_iterations": payload.get("completed_iterations"),
        "configured_iterations": payload.get("configured_iterations"),
        "latest_iteration": payload.get("latest_iteration"),
        "latest_report_json_path": payload.get("latest_report_json_path"),
        "event_log_path": payload.get("event_log_path"),
        "loop_mode": payload.get("loop_mode"),
        "roster_name": payload.get("roster_name"),
        "enabled_strategy_ids": payload.get("enabled_strategy_ids"),
        "submit_disabled": payload.get("submit_disabled"),
        "read_only": payload.get("read_only"),
        "observe_only": payload.get("observe_only"),
        "guarded_paper_enabled": payload.get("guarded_paper_enabled"),
        "submit_attempted": bool((payload.get("latest_iteration") or {}).get("submit_attempted")),
        "broker_state_mutated": bool((payload.get("latest_iteration") or {}).get("broker_state_mutated")),
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }
    print(json.dumps(payload if bool(args.json) else summary, indent=2, sort_keys=True))
    ok_classifications = {
        P0_OBSERVE_LOOP_COMPLETED,
        P0_OBSERVE_LOOP_ITERATION_OK,
        P0_SUBMIT_DISABLED_LOOP_NO_CANDIDATE,
        P0_SUBMIT_DISABLED_LOOP_CANDIDATE_PREVIEW_READY,
        P0_SUBMIT_DISABLED_LOOP_ITERATION_OK,
        P0_GUARDED_PAPER_LOOP_COMPLETED,
        P0_GUARDED_PAPER_LOOP_NO_CANDIDATE,
        P0_GUARDED_PAPER_LOOP_CANDIDATE_HANDOFF,
    }
    return 0 if payload.get("classification") in ok_classifications else 2


if __name__ == "__main__":
    raise SystemExit(main())
