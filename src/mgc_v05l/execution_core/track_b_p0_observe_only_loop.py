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
    run_track_b_multi_strategy_runtime_cycle,
)
from mgc_v05l.execution_core.track_b_paper_proof_readiness import (
    TrackBPaperProofReadinessConfig,
    build_track_b_paper_proof_readiness,
    write_track_b_paper_proof_readiness,
)
from mgc_v05l.execution_core.track_b_session_strategy_envelope_producer import (
    produce_track_b_session_strategy_envelopes,
)
from mgc_v05l.execution_core.track_b_snap_turn_envelope_producer import (
    produce_track_b_snap_turn_envelopes,
)


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

P0_OBSERVE_LOOP_READY = "P0_OBSERVE_LOOP_READY"
P0_OBSERVE_LOOP_ITERATION_OK = "P0_OBSERVE_LOOP_ITERATION_OK"
P0_OBSERVE_LOOP_BLOCKED_CONTROL_PLANE = "P0_OBSERVE_LOOP_BLOCKED_CONTROL_PLANE"
P0_OBSERVE_LOOP_BLOCKED_SAFE_STATE = "P0_OBSERVE_LOOP_BLOCKED_SAFE_STATE"
P0_OBSERVE_LOOP_ENVELOPE_REFRESH_FAILED = "P0_OBSERVE_LOOP_ENVELOPE_REFRESH_FAILED"
P0_OBSERVE_LOOP_CYCLE_FAILED = "P0_OBSERVE_LOOP_CYCLE_FAILED"
P0_OBSERVE_LOOP_COMPLETED = "P0_OBSERVE_LOOP_COMPLETED"

P0_STRATEGY_IDS = (
    "asian_drift_v1",
    "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
    "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
    "MNQ_FIRST_BEAR_SNAP_TURN_V1",
    "MNQ_FIRST_BULL_SNAP_TURN_V1",
)


@dataclass(frozen=True)
class TrackBP0ObserveOnlyLoopConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_P0_OBSERVE_ONLY_LATEST
    event_log_path: Path = DEFAULT_P0_OBSERVE_ONLY_EVENTS
    mgc_1m_candles_path: Path = DEFAULT_MGC_1M_CANDLES
    mgc_5m_candles_path: Path = DEFAULT_MGC_5M_CANDLES
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
    actual_loop_id = loop_id or f"track-b-p0-observe-loop-{uuid.uuid4().hex}"
    iterations = max(int(config.iterations), 1)
    sleep_seconds = max(float(config.sleep_seconds), 0.0)
    iteration_reports: list[dict[str, Any]] = []
    latest_payload: dict[str, Any] = _base_payload(
        config=config,
        loop_id=actual_loop_id,
        classification=P0_OBSERVE_LOOP_READY,
        iterations=iterations,
        sleep_seconds=sleep_seconds,
        iteration_reports=iteration_reports,
        generated_at=_now(now_factory).isoformat(),
    )
    _write_latest_and_event(config=config, payload=latest_payload, event=None)

    final_classification = P0_OBSERVE_LOOP_COMPLETED
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
            final_classification = str(iteration["classification"])
            break
        if index < iterations and sleep_seconds > 0:
            actual_stages.sleep(sleep_seconds)
    else:
        final_classification = P0_OBSERVE_LOOP_COMPLETED

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
        "broker_state_mutated": False,
        "broker_mutation_attempted": False,
        "lifecycle_mutation_attempted": False,
        "submit_attempted": False,
        "submit_delegation_forced_off": True,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "read_only": True,
        "observe_only": True,
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
                "classification": P0_OBSERVE_LOOP_BLOCKED_CONTROL_PLANE,
                "primary_blocker": block,
                "required_next_action": "Refresh/repair Control Plane Snapshot before the next P0 observe-only iteration.",
            }
        safe_block = _safe_state_blocker(snapshot)
        if safe_block:
            return {
                **base,
                "classification": P0_OBSERVE_LOOP_BLOCKED_SAFE_STATE,
                "primary_blocker": safe_block,
                "required_next_action": "Keep P0 observe-only loop stopped while Safe-State is not normal.",
            }

        envelope_context = _refresh_p0_envelopes(config=config, stages=stages, now=now)
        cycle = stages.multi_strategy_cycle(config, now, envelope_context)
        cycle_report = dict(getattr(cycle, "report", {}) or {})
        cycle_verdict = str(cycle_report.get("multi_strategy_runtime_cycle_verdict") or "")
        if not cycle_verdict:
            return {
                **base,
                **envelope_context["iteration_fields"],
                "classification": P0_OBSERVE_LOOP_CYCLE_FAILED,
                "primary_blocker": "Multi-strategy cycle did not return a verdict.",
                "required_next_action": "Review P0 multi-strategy cycle stage diagnostics before continuing.",
            }
        return {
            **base,
            **envelope_context["iteration_fields"],
            "classification": P0_OBSERVE_LOOP_ITERATION_OK,
            "cycle_verdict": cycle_verdict,
            "cycle_report_path": str(getattr(cycle, "report_json", "") or ""),
            "cycle_authority_classification": cycle_report.get("multi_strategy_cycle_authorization_classification"),
            "cycle_submit_allowed": cycle_report.get("cycle_submit_allowed") is True,
            "cycle_broker_mutation_allowed": cycle_report.get("cycle_broker_mutation_allowed") is True,
            "candidate_signal_count": len(cycle_report.get("candidate_signals") or []),
            "suppressed_signal_count": len(cycle_report.get("suppressed_signals") or []),
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
        "mnq_first_bear_snap_turn": _path_str(getattr(snap, "first_bear_snap_turn_event_json", None)),
        "mnq_first_bull_snap_turn": _path_str(getattr(snap, "first_bull_snap_turn_event_json", None)),
    }
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
        "mnq_first_bear_snap_turn_event_json": getattr(snap, "first_bear_snap_turn_event_json", None),
        "mnq_first_bull_snap_turn_event_json": getattr(snap, "first_bull_snap_turn_event_json", None),
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
    return produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=_read_json(config.resolve(config.mgc_5m_candles_path)),
        runtime_5m_payload_path=config.resolve(config.mgc_5m_candles_path),
        expected_account_id=config.expected_account_id,
        source_id="p0_observe_only_loop_session_strategy",
        output_root=config.resolve(Path("outputs/track_b_execution_core/session_strategy_state")),
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        now=now,
    )


def _default_snap_turn_envelopes(config: TrackBP0ObserveOnlyLoopConfig, now: datetime) -> Any:
    return produce_track_b_snap_turn_envelopes(
        runtime_5m_payload=_read_json(config.resolve(config.mnq_5m_candles_path)),
        runtime_5m_payload_path=config.resolve(config.mnq_5m_candles_path),
        expected_account_id=config.expected_account_id,
        source_id="p0_observe_only_loop_snap_turn",
        output_root=config.resolve(Path("outputs/track_b_execution_core/snap_turn_state")),
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        now=now,
    )


def _default_multi_strategy_cycle(
    config: TrackBP0ObserveOnlyLoopConfig,
    now: datetime,
    envelope_context: Mapping[str, Any],
) -> Any:
    return run_track_b_multi_strategy_runtime_cycle(
        config=TrackBMultiStrategyRuntimeCycleConfig(
            enabled_strategy_ids=P0_STRATEGY_IDS,
            asian_drift_event_json=_optional_path(envelope_context.get("asian_drift_event_json")),
            pause_resume_short_event_json=_optional_path(envelope_context.get("pause_resume_short_event_json")),
            breakout_retest_hold_long_event_json=_optional_path(
                envelope_context.get("breakout_retest_hold_long_event_json")
            ),
            mnq_first_bear_snap_turn_event_json=_optional_path(
                envelope_context.get("mnq_first_bear_snap_turn_event_json")
            ),
            mnq_first_bull_snap_turn_event_json=_optional_path(
                envelope_context.get("mnq_first_bull_snap_turn_event_json")
            ),
            inbox_dir=config.resolve(config.inbox_dir),
            expected_account_id=config.expected_account_id,
            source_id="p0_observe_only_loop_multi_strategy_cycle",
            submit_paper=False,
            confirm_paper_submit=False,
            update_operator_status=config.update_operator_status,
            output_root=config.resolve(Path("outputs/track_b_execution_core/track_b_multi_strategy_runtime_cycle")),
            strategy_rule_output_root=config.resolve(Path("outputs/track_b_execution_core/track_b_strategy_rule_runner")),
            strategy_paper_runner_output_root=config.resolve(Path("outputs/track_b_execution_core/track_b_strategy_paper_runner")),
            operator_status_output_root=config.resolve(Path("outputs/track_b_execution_core/operator_status")),
            repo_root=config.repo_root,
        ),
        now=now,
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
    return {
        "schema_version": "track_b_p0_observe_only_loop_v1",
        "p0_observe_only_loop_id": loop_id,
        "generated_at": generated_at,
        "classification": classification,
        "loop_start_classification": P0_OBSERVE_LOOP_READY,
        "mode": "PAPER",
        "read_only": True,
        "observe_only": True,
        "dry_run_only": True,
        "submit_authority": False,
        "submit_delegation_forced_off": True,
        "broker_mutation_allowed": False,
        "broker_state_mutated": False,
        "lifecycle_mutation_allowed": False,
        "runtime_restart_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "configured_iterations": iterations,
        "completed_iterations": len(iteration_reports),
        "sleep_seconds": sleep_seconds,
        "p0_strategy_ids": list(P0_STRATEGY_IDS),
        "source_paths": {
            "mgc_1m_candles": str(config.resolve(config.mgc_1m_candles_path)),
            "mgc_5m_candles": str(config.resolve(config.mgc_5m_candles_path)),
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
    parser.add_argument("--iterations", type=int, default=12)
    parser.add_argument("--sleep-seconds", type=float, default=60.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--max-completed-5m-age-seconds", type=int, default=900)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_P0_OBSERVE_ONLY_LATEST)
    parser.add_argument("--event-log-path", type=Path, default=DEFAULT_P0_OBSERVE_ONLY_EVENTS)
    parser.add_argument("--mgc-1m-candles-json", type=Path, default=DEFAULT_MGC_1M_CANDLES)
    parser.add_argument("--mgc-5m-candles-json", type=Path, default=DEFAULT_MGC_5M_CANDLES)
    parser.add_argument("--mnq-5m-candles-json", type=Path, default=DEFAULT_MNQ_5M_CANDLES)
    parser.add_argument("--inbox-dir", type=Path, default=Path("examples/track_b_shadow_listener/inbox"))
    parser.add_argument("--no-update-operator-status", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBP0ObserveOnlyLoopConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        event_log_path=Path(args.event_log_path),
        mgc_1m_candles_path=Path(args.mgc_1m_candles_json),
        mgc_5m_candles_path=Path(args.mgc_5m_candles_json),
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
        "read_only": True,
        "observe_only": True,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }
    print(json.dumps(payload if bool(args.json) else summary, indent=2, sort_keys=True))
    return 0 if payload.get("classification") in {P0_OBSERVE_LOOP_COMPLETED, P0_OBSERVE_LOOP_ITERATION_OK} else 2


if __name__ == "__main__":
    raise SystemExit(main())
