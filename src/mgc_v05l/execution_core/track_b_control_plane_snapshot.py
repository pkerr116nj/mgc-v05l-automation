"""Read-only Track B PAPER control-plane snapshot.

This command is the explicit pre-supervisor orchestration boundary:
Shared Truth Refresh runs first, Runtime Supervisor Authority builds from that
refresh generation, and this module records the coherent decision snapshot.
It is advisory only and never starts, stops, submits, cancels, replaces, closes,
flattens, or mutates broker/lifecycle state.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_agent_health import (
    DEFAULT_AGENT_HEALTH_ARTIFACT,
    TrackBAgentHealthConfig,
    build_track_b_agent_health,
    write_track_b_agent_health,
)
from mgc_v05l.execution_core.track_b_artifact_archive_planner import (
    DEFAULT_ARTIFACT_ARCHIVE_PLAN_PATH,
    TrackBArtifactArchivePlannerConfig,
    build_track_b_artifact_archive_plan,
    write_track_b_artifact_archive_plan,
)
from mgc_v05l.execution_core.track_b_control_plane_top_line import (
    build_track_b_control_plane_top_line,
)
from mgc_v05l.execution_core.track_b_continuation_aware_exit_decision import (
    DEFAULT_CONTINUATION_AWARE_EXIT_PREVIEW_EVENT_LOG,
    DEFAULT_CONTINUATION_AWARE_EXIT_PREVIEW_PATH,
)
from mgc_v05l.execution_core.track_b_continuation_aware_exit_history import (
    DEFAULT_CONTINUATION_AWARE_EXIT_HISTORY_PATH,
    TrackBContinuationAwareExitHistoryConfig,
    build_continuation_aware_exit_history,
    write_continuation_aware_exit_history,
)
from mgc_v05l.execution_core.track_b_fresh_truth_contract import (
    RUNTIME_AUTHORITY_STALE_WITH_BROKER_EXPOSURE,
)
from mgc_v05l.execution_core.track_b_projection_metadata import build_projection_metadata
from mgc_v05l.execution_core.track_b_paper_proof_readiness import (
    DEFAULT_OUTPUT_PATH as DEFAULT_PROOF_READINESS_ARTIFACT,
    TrackBPaperProofReadinessConfig,
    build_track_b_paper_proof_readiness,
    write_track_b_paper_proof_readiness,
)
from mgc_v05l.execution_core.track_b_order_adjustment_planner import (
    TrackBOrderAdjustmentPlannerConfig,
    build_track_b_order_adjustment_plan,
    write_track_b_order_adjustment_plan,
)
from mgc_v05l.execution_core.track_b_recovery_attempt_history import (
    DEFAULT_RECOVERY_ATTEMPT_HISTORY_ARTIFACT,
    TrackBRecoveryAttemptHistoryConfig,
    build_track_b_recovery_attempt_history,
    write_track_b_recovery_attempt_history,
)
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    SHARED_TRUTH_COHERENT,
    TrackBRuntimeSupervisorAuthorityConfig,
    build_track_b_runtime_supervisor_authority,
    write_track_b_runtime_supervisor_authority,
)
from mgc_v05l.execution_core.track_b_runtime_safe_state_envelope import (
    DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT,
    TrackBRuntimeSafeStateEnvelopeConfig,
    build_track_b_runtime_safe_state_envelope,
    write_track_b_runtime_safe_state_envelope,
)
from mgc_v05l.execution_core.track_b_runtime_resume_semantics import (
    TrackBRuntimeResumeSemanticsConfig,
    build_track_b_runtime_resume_semantics,
    write_track_b_runtime_resume_semantics,
)
from mgc_v05l.execution_core.track_b_self_recover_rules import (
    TrackBSelfRecoverRulesConfig,
    build_track_b_self_recover_rules,
    write_track_b_self_recover_rules,
)
from mgc_v05l.execution_core.track_b_shared_truth_refresh_cli import (
    DEFAULT_SHARED_TRUTH_REFRESH_ARTIFACT,
    TrackBSharedTruthRefreshConfig,
    refresh_track_b_shared_truth,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "control_plane"
    / "latest_control_plane_snapshot.json"
)
DEFAULT_DASHBOARD_CONTROL_PLANE_SNAPSHOT_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_control_plane_snapshot.json"
)
DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_autonomous_recovery"
    / "latest_paper_autonomous_recovery_plan.json"
)

CONTROL_PLANE_SNAPSHOT_READY = "CONTROL_PLANE_SNAPSHOT_READY"
CONTROL_PLANE_SNAPSHOT_BLOCKED = "CONTROL_PLANE_SNAPSHOT_BLOCKED"
CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED = "CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED"


@dataclass(frozen=True)
class TrackBControlPlaneSnapshotConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_CONTROL_PLANE_SNAPSHOT_PROJECTION
    shared_truth_refresh_path: Path = DEFAULT_SHARED_TRUTH_REFRESH_ARTIFACT
    agent_health_path: Path = DEFAULT_AGENT_HEALTH_ARTIFACT
    proof_readiness_path: Path = DEFAULT_PROOF_READINESS_ARTIFACT
    paper_autonomous_recovery_plan_path: Path = DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT
    recovery_attempt_history_path: Path = DEFAULT_RECOVERY_ATTEMPT_HISTORY_ARTIFACT
    artifact_archive_plan_path: Path = DEFAULT_ARTIFACT_ARCHIVE_PLAN_PATH
    continuation_aware_exit_preview_path: Path = DEFAULT_CONTINUATION_AWARE_EXIT_PREVIEW_PATH
    continuation_aware_exit_history_path: Path = DEFAULT_CONTINUATION_AWARE_EXIT_HISTORY_PATH
    runtime_safe_state_envelope_path: Path = DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
    broker_lease_history_path: Path | None = None
    refresh_proof_readiness_before_snapshot: bool = True
    proof_required_symbols: tuple[str, ...] | None = None

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_control_plane_snapshot(
    *,
    config: TrackBControlPlaneSnapshotConfig,
    now: datetime | None = None,
    pid_running: Callable[[int], bool] | None = None,
    process_rows: Sequence[Mapping[str, Any]] | None = None,
    process_root_resolver: Callable[[int], Path | None] | None = None,
    source_commit_resolver: Callable[[Path], str | None] | None = None,
    post_shared_truth_refresh_hook: Callable[[Mapping[str, Any]], None] | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    if config.refresh_proof_readiness_before_snapshot:
        _refresh_proof_readiness_for_snapshot(config=config, now=actual_now)
    shared_truth = _refresh_shared_truth_for_snapshot(
        config=config,
        now=actual_now,
        pid_running=pid_running,
        process_root_resolver=process_root_resolver,
        source_commit_resolver=source_commit_resolver,
    )
    if post_shared_truth_refresh_hook is not None:
        post_shared_truth_refresh_hook(shared_truth)

    agent_health_config = TrackBAgentHealthConfig(
        repo_root=config.repo_root,
        output_path=config.agent_health_path,
        dashboard_projection_path=None,
    )
    agent_health = build_track_b_agent_health(
        config=agent_health_config,
        now=actual_now,
        process_rows=process_rows,
        pid_running=pid_running,
        source_commit_resolver=source_commit_resolver,
    )
    agent_health_path = write_track_b_agent_health(config=agent_health_config, payload=agent_health)
    runtime_resume_config = TrackBRuntimeResumeSemanticsConfig(
        repo_root=config.repo_root,
        dashboard_projection_path=None,
        allow_control_plane_build_context=True,
    )
    runtime_resume = build_track_b_runtime_resume_semantics(config=runtime_resume_config, now=actual_now)
    write_track_b_runtime_resume_semantics(config=runtime_resume_config, payload=runtime_resume)
    self_recover_config = TrackBSelfRecoverRulesConfig(
        repo_root=config.repo_root,
        dashboard_projection_path=None,
    )
    self_recover = build_track_b_self_recover_rules(config=self_recover_config, now=actual_now)
    write_track_b_self_recover_rules(config=self_recover_config, payload=self_recover)

    if post_shared_truth_refresh_hook is None:
        shared_truth = _converge_shared_truth_for_snapshot(
            config=config,
            now=actual_now,
            pid_running=pid_running,
            process_root_resolver=process_root_resolver,
            source_commit_resolver=source_commit_resolver,
        )

    supervisor_config = TrackBRuntimeSupervisorAuthorityConfig(
        repo_root=config.repo_root,
        dashboard_projection_path=None,
        shared_truth_path=config.shared_truth_refresh_path,
        runtime_safe_state_envelope_path=Path(
            "outputs/track_b_execution_core/safe_state/__control_plane_snapshot_pre_safe_state_not_authority.json"
        ),
    )
    runtime_supervisor = build_track_b_runtime_supervisor_authority(
        config=supervisor_config,
        now=actual_now,
    )
    runtime_supervisor_path = write_track_b_runtime_supervisor_authority(
        config=supervisor_config,
        payload=runtime_supervisor,
    )
    if (
        post_shared_truth_refresh_hook is None
        and runtime_supervisor.get("shared_truth_coherence_status") != SHARED_TRUTH_COHERENT
    ):
        shared_truth = _converge_shared_truth_for_snapshot(
            config=config,
            now=actual_now,
            pid_running=pid_running,
            process_root_resolver=process_root_resolver,
            source_commit_resolver=source_commit_resolver,
        )
        runtime_supervisor = build_track_b_runtime_supervisor_authority(
            config=supervisor_config,
            now=actual_now,
        )
        runtime_supervisor_path = write_track_b_runtime_supervisor_authority(
            config=supervisor_config,
            payload=runtime_supervisor,
        )
    autonomous_recovery_plan = _read_json(config.resolve(config.paper_autonomous_recovery_plan_path))
    continuation_aware_exit_preview = _read_json(config.resolve(config.continuation_aware_exit_preview_path))
    continuation_aware_exit_history_config = TrackBContinuationAwareExitHistoryConfig(
        repo_root=config.repo_root,
        output_path=config.continuation_aware_exit_history_path,
        event_log_path=DEFAULT_CONTINUATION_AWARE_EXIT_PREVIEW_EVENT_LOG,
        latest_preview_path=config.continuation_aware_exit_preview_path,
    )
    continuation_aware_exit_history = build_continuation_aware_exit_history(
        config=continuation_aware_exit_history_config,
        now=actual_now,
    )
    continuation_aware_exit_history_path = write_continuation_aware_exit_history(
        config=continuation_aware_exit_history_config,
        payload=continuation_aware_exit_history,
    )
    recovery_attempt_history_config = TrackBRecoveryAttemptHistoryConfig(
        repo_root=config.repo_root,
        output_path=config.recovery_attempt_history_path,
        control_plane_snapshot_path=config.output_path,
    )
    recovery_attempt_history = build_track_b_recovery_attempt_history(
        config=recovery_attempt_history_config,
        now=actual_now,
    )
    recovery_attempt_history_path = write_track_b_recovery_attempt_history(
        config=recovery_attempt_history_config,
        payload=recovery_attempt_history,
    )
    artifact_archive_plan_config = TrackBArtifactArchivePlannerConfig(
        repo_root=config.repo_root,
        output_path=config.artifact_archive_plan_path,
        control_plane_snapshot_path=config.output_path,
        agent_health_path=config.agent_health_path,
        recovery_attempt_history_path=config.recovery_attempt_history_path,
    )
    artifact_archive_plan = build_track_b_artifact_archive_plan(
        config=artifact_archive_plan_config,
        now=actual_now,
    )
    artifact_archive_plan_path = write_track_b_artifact_archive_plan(
        config=artifact_archive_plan_config,
        payload=artifact_archive_plan,
    )
    payload = _snapshot_payload(
        config=config,
        now=actual_now,
        shared_truth=shared_truth,
        agent_health=agent_health,
        agent_health_path=agent_health_path,
        runtime_supervisor=runtime_supervisor,
        runtime_supervisor_path=runtime_supervisor_path,
        autonomous_recovery_plan=autonomous_recovery_plan,
        continuation_aware_exit_preview=continuation_aware_exit_preview,
        continuation_aware_exit_history=continuation_aware_exit_history,
        continuation_aware_exit_history_path=continuation_aware_exit_history_path,
        recovery_attempt_history=recovery_attempt_history,
        recovery_attempt_history_path=recovery_attempt_history_path,
        artifact_archive_plan=artifact_archive_plan,
        artifact_archive_plan_path=artifact_archive_plan_path,
    )
    safe_state_config = TrackBRuntimeSafeStateEnvelopeConfig(
        repo_root=config.repo_root,
        output_path=config.runtime_safe_state_envelope_path,
        control_plane_snapshot_path=config.output_path,
    )
    safe_state = build_track_b_runtime_safe_state_envelope(
        config=safe_state_config,
        now=actual_now,
        input_overrides={
            "control_plane_snapshot": payload,
            "recovery_attempt_history": recovery_attempt_history,
        },
    )
    safe_state_path = write_track_b_runtime_safe_state_envelope(config=safe_state_config, payload=safe_state)
    _apply_safe_state_to_snapshot(payload=payload, safe_state=safe_state)
    payload["source_artifact_paths"]["runtime_safe_state_envelope"] = str(safe_state_path)
    payload.update(build_track_b_control_plane_top_line(payload))
    return payload


def _refresh_shared_truth_for_snapshot(
    *,
    config: TrackBControlPlaneSnapshotConfig,
    now: datetime,
    pid_running: Callable[[int], bool] | None,
    process_root_resolver: Callable[[int], Path | None] | None,
    source_commit_resolver: Callable[[Path], str | None] | None,
) -> dict[str, Any]:
    return refresh_track_b_shared_truth(
        config=TrackBSharedTruthRefreshConfig(
            repo_root=config.repo_root,
            shared_truth_refresh_path=config.shared_truth_refresh_path,
            broker_lease_history_path=config.broker_lease_history_path,
        ),
        now=now,
        pid_running=pid_running,
        process_root_resolver=process_root_resolver,
        source_commit_resolver=source_commit_resolver,
    )


def _converge_shared_truth_for_snapshot(
    *,
    config: TrackBControlPlaneSnapshotConfig,
    now: datetime,
    pid_running: Callable[[int], bool] | None,
    process_root_resolver: Callable[[int], Path | None] | None,
    source_commit_resolver: Callable[[Path], str | None] | None,
) -> dict[str, Any]:
    """Refresh the authority generation after local producers finish writing.

    Control Plane builds several read-only authority artifacts before Runtime
    Supervisor checks shared-truth coherence. Some of those producers rebuild
    lower-level current-scope artifacts. This bounded convergence pass makes the
    Supervisor compare against the final shared-truth generation, and it also
    refreshes the dry-run order-adjustment plan so a stale suspicious plan cannot
    outlive a broker-flat/no-open-order shared truth state.
    """

    shared_truth = _refresh_shared_truth_for_snapshot(
        config=config,
        now=now,
        pid_running=pid_running,
        process_root_resolver=process_root_resolver,
        source_commit_resolver=source_commit_resolver,
    )
    _refresh_order_adjustment_plan_for_snapshot(config=config, now=now, shared_truth=shared_truth)
    shared_truth = _refresh_shared_truth_for_snapshot(
        config=config,
        now=now,
        pid_running=pid_running,
        process_root_resolver=process_root_resolver,
        source_commit_resolver=source_commit_resolver,
    )
    _refresh_order_adjustment_plan_for_snapshot(config=config, now=now, shared_truth=shared_truth)
    return shared_truth


def _refresh_order_adjustment_plan_for_snapshot(
    *,
    config: TrackBControlPlaneSnapshotConfig,
    now: datetime,
    shared_truth: Mapping[str, Any],
) -> Mapping[str, Any]:
    planner_config = TrackBOrderAdjustmentPlannerConfig(repo_root=config.repo_root)
    payload = build_track_b_order_adjustment_plan(
        config=planner_config,
        now=now,
        shared_truth_refresh=shared_truth,
    )
    write_track_b_order_adjustment_plan(config=planner_config, payload=payload)
    return payload


def _refresh_proof_readiness_for_snapshot(
    *,
    config: TrackBControlPlaneSnapshotConfig,
    now: datetime,
) -> Mapping[str, Any]:
    proof_config = TrackBPaperProofReadinessConfig(
        repo_root=config.repo_root,
        output_path=config.proof_readiness_path,
        required_symbols=config.proof_required_symbols or TrackBPaperProofReadinessConfig.required_symbols,
        now=now,
        broker_lease_history_path=config.broker_lease_history_path,
    )
    payload = build_track_b_paper_proof_readiness(config=proof_config, now=now)
    write_track_b_paper_proof_readiness(config=proof_config, payload=payload)
    return payload


def write_track_b_control_plane_snapshot(
    *,
    config: TrackBControlPlaneSnapshotConfig,
    payload: Mapping[str, Any],
) -> Path:
    authority_path = config.resolve(config.output_path)
    _write_json_atomic(authority_path, dict(payload))
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_control_plane_snapshot_projection(
                authority_payload=payload,
                authority_path=authority_path,
            ),
        )
    return authority_path


def build_dashboard_control_plane_snapshot_projection(
    *,
    authority_payload: Mapping[str, Any],
    authority_path: Path,
) -> dict[str, Any]:
    return {
        **dict(authority_payload),
        "schema_version": "track_b_control_plane_snapshot_dashboard_projection_v1",
        **build_projection_metadata(
            source_authority_path=authority_path,
            generated_from_control_plane_snapshot_id=(
                str(authority_payload.get("control_plane_snapshot_id"))
                if authority_payload.get("control_plane_snapshot_id")
                else None
            ),
            control_plane_snapshot_required=True,
        ),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write a read-only Track B PAPER control-plane snapshot.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT)
    parser.add_argument(
        "--dashboard-projection-path",
        type=Path,
        default=DEFAULT_DASHBOARD_CONTROL_PLANE_SNAPSHOT_PROJECTION,
    )
    parser.add_argument("--no-dashboard-projection", action="store_true")
    parser.add_argument("--no-broker-lease-history", action="store_true")
    parser.add_argument(
        "--proof-required-symbols",
        default=os.environ.get("TRACK_B_PAPER_PROOF_REQUIRED_SYMBOLS"),
        help=(
            "Comma-separated Phase-1 symbols required by this launch scope. "
            "Defaults to the proof-readiness contract default unless "
            "TRACK_B_PAPER_PROOF_REQUIRED_SYMBOLS is set."
        ),
    )
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBControlPlaneSnapshotConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        dashboard_projection_path=None if bool(args.no_dashboard_projection) else Path(args.dashboard_projection_path),
        broker_lease_history_path=None if bool(args.no_broker_lease_history) else None,
        proof_required_symbols=_csv_tuple(args.proof_required_symbols),
    )
    payload = build_track_b_control_plane_snapshot(config=config)
    authority_path = write_track_b_control_plane_snapshot(config=config, payload=payload)
    summary = {
        "classification": payload.get("classification"),
        "shared_truth_refresh_generation_id": payload.get("shared_truth_refresh_generation_id"),
        "shared_truth_coherence_status": payload.get("shared_truth_coherence_status"),
        "runtime_supervisor_classification": payload.get("runtime_supervisor_classification"),
        "supervisor_mode": payload.get("supervisor_mode"),
        "proof_window_status": payload.get("proof_window_status"),
        "top_line_classification": payload.get("top_line_classification"),
        "top_line_status": payload.get("top_line_status"),
        "runtime_resume_action_policy": payload.get("runtime_resume_action_policy"),
        "runtime_resume_proposed_next_runtime_generation_id": payload.get(
            "runtime_resume_proposed_next_runtime_generation_id"
        ),
        "runtime_resume_attempts_remaining": payload.get("runtime_resume_attempts_remaining"),
        "runtime_resume_cooldown_until": payload.get("runtime_resume_cooldown_until"),
        "recommended_recovery_action": payload.get("recommended_recovery_action"),
        "paper_action_policy": payload.get("paper_action_policy"),
        "self_recover_autonomous_recovery_plan_classification": payload.get(
            "self_recover_autonomous_recovery_plan_classification"
        ),
        "recovery_budget_key": payload.get("recovery_budget_key"),
        "attempts_remaining": payload.get("attempts_remaining"),
        "cooldown_until": payload.get("cooldown_until"),
        "quarantine_required": payload.get("quarantine_required"),
        "latest_recovery_attempt_id": payload.get("latest_recovery_attempt_id"),
        "latest_recovery_attempt_action_type": payload.get("latest_recovery_attempt_action_type"),
        "latest_recovery_attempt_classification": payload.get("latest_recovery_attempt_classification"),
        "recovery_attempt_history_no_history": payload.get("recovery_attempt_history_no_history"),
        "recovery_attempt_last_success_at": payload.get("recovery_attempt_last_success_at"),
        "recovery_attempt_last_failure_at": payload.get("recovery_attempt_last_failure_at"),
        "artifact_archive_plan_classification": payload.get("artifact_archive_plan_classification"),
        "artifact_archive_cold_archive_candidate_count": payload.get(
            "artifact_archive_cold_archive_candidate_count"
        ),
        "artifact_archive_blocked_candidate_count": payload.get("artifact_archive_blocked_candidate_count"),
        "artifact_archive_estimated_bytes": payload.get("artifact_archive_estimated_bytes"),
        "artifact_archive_dry_run_only": payload.get("artifact_archive_dry_run_only"),
        "artifact_archive_execution_enabled": payload.get("artifact_archive_execution_enabled"),
        "continuation_aware_exit_strategy_id": payload.get("continuation_aware_exit_strategy_id"),
        "continuation_aware_exit_symbol": payload.get("continuation_aware_exit_symbol"),
        "continuation_aware_exit_policy_id": payload.get("continuation_aware_exit_policy_id"),
        "continuation_aware_exit_profile_id": payload.get("continuation_aware_exit_profile_id"),
        "continuation_aware_exit_state": payload.get("continuation_aware_exit_state"),
        "continuation_aware_exit_quality_state": payload.get("continuation_aware_exit_quality_state"),
        "continuation_aware_exit_should_request_close": payload.get("continuation_aware_exit_should_request_close"),
        "continuation_aware_exit_dry_run_only": payload.get("continuation_aware_exit_dry_run_only"),
        "continuation_aware_exit_not_order_authority": payload.get(
            "continuation_aware_exit_not_order_authority"
        ),
        "continuation_aware_exit_not_lifecycle_authority": payload.get(
            "continuation_aware_exit_not_lifecycle_authority"
        ),
        "continuation_aware_exit_missing_inputs": payload.get("continuation_aware_exit_missing_inputs"),
        "continuation_aware_exit_source_report_path": payload.get("continuation_aware_exit_source_report_path"),
        "continuation_aware_exit_no_preview": payload.get("continuation_aware_exit_no_preview"),
        "continuation_aware_exit_history_classification": payload.get(
            "continuation_aware_exit_history_classification"
        ),
        "continuation_aware_exit_history_total_events": payload.get("continuation_aware_exit_history_total_events"),
        "continuation_aware_exit_history_strategy_count": payload.get(
            "continuation_aware_exit_history_strategy_count"
        ),
        "continuation_aware_exit_history_latest_strategy_id": payload.get(
            "continuation_aware_exit_history_latest_strategy_id"
        ),
        "continuation_aware_exit_history_latest_exit_state": payload.get(
            "continuation_aware_exit_history_latest_exit_state"
        ),
        "safe_state_classification": payload.get("safe_state_classification"),
        "safe_state_observe_only": payload.get("safe_state_observe_only"),
        "safe_state_recovery_only": payload.get("safe_state_recovery_only"),
        "safe_state_runtime_start_allowed": payload.get("safe_state_runtime_start_allowed"),
        "safe_state_submit_allowed": payload.get("safe_state_submit_allowed"),
        "safe_state_broker_mutation_allowed": payload.get("safe_state_broker_mutation_allowed"),
        "primary_blocking_agent_id": payload.get("primary_blocking_agent_id"),
        "operator_explanation": payload.get("operator_explanation"),
        "recommended_observation_step": payload.get("recommended_observation_step"),
        "recommended_next_command": payload.get("recommended_next_command"),
        "authority_path": str(authority_path),
        "read_only": True,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }
    print(json.dumps(payload if bool(args.json) else summary, indent=2, sort_keys=True))
    return 0 if payload.get("classification") == CONTROL_PLANE_SNAPSHOT_READY else 2


def _snapshot_payload(
    *,
    config: TrackBControlPlaneSnapshotConfig,
    now: datetime,
    shared_truth: Mapping[str, Any],
    agent_health: Mapping[str, Any],
    agent_health_path: Path,
    runtime_supervisor: Mapping[str, Any],
    runtime_supervisor_path: Path,
    autonomous_recovery_plan: Mapping[str, Any],
    continuation_aware_exit_preview: Mapping[str, Any],
    continuation_aware_exit_history: Mapping[str, Any],
    continuation_aware_exit_history_path: Path,
    recovery_attempt_history: Mapping[str, Any],
    recovery_attempt_history_path: Path,
    artifact_archive_plan: Mapping[str, Any],
    artifact_archive_plan_path: Path,
) -> dict[str, Any]:
    coherence_status = str(runtime_supervisor.get("shared_truth_coherence_status") or "")
    generation_matches = (
        shared_truth.get("refresh_generation_id") == runtime_supervisor.get("shared_truth_refresh_generation_id")
    )
    evidence = _mapping(runtime_supervisor.get("evidence_summary"))
    agent_health_evidence = _agent_health_evidence(agent_health)
    runtime_authority_exposure = _runtime_authority_exposure_fields(
        runtime_supervisor=runtime_supervisor,
        agent_health_evidence=agent_health_evidence,
    )
    agent_health_runtime_submit_start_tolerated = _agent_health_runtime_submit_start_tolerated(
        runtime_supervisor=runtime_supervisor,
        agent_health_evidence=agent_health_evidence,
    )
    classification = _snapshot_classification(
        coherence_status=coherence_status,
        generation_matches=generation_matches,
        supervisor_classification=str(runtime_supervisor.get("classification") or ""),
        agent_health_evidence=agent_health_evidence,
        agent_health_runtime_submit_start_tolerated=agent_health_runtime_submit_start_tolerated,
    )
    blockers = _snapshot_blockers(
        shared_truth=shared_truth,
        runtime_supervisor=runtime_supervisor,
        agent_health_evidence=agent_health_evidence,
        coherence_status=coherence_status,
        generation_matches=generation_matches,
        agent_health_runtime_submit_start_tolerated=agent_health_runtime_submit_start_tolerated,
    )
    warnings = list(shared_truth.get("warnings") or []) + list(runtime_supervisor.get("warnings") or [])
    warnings.extend(_agent_health_warnings(agent_health_evidence))
    planner_explanation = _planner_explanation_fields(autonomous_recovery_plan)
    payload = {
        "schema_version": "track_b_control_plane_snapshot_v1",
        "control_plane_snapshot_id": _snapshot_id(now),
        "generated_at": now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "advisory_only": True,
        "submit_authority": False,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "runtime_restart_authority": False,
        "runtime_stop_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "classification": classification,
        "shared_truth_refresh_generation_id": shared_truth.get("refresh_generation_id"),
        "shared_truth_refresh_generated_at": shared_truth.get("generated_at"),
        "shared_truth_coherence_status": coherence_status,
        "shared_truth_generation_matches_supervisor": generation_matches,
        "stale_or_mixed_sources": list(runtime_supervisor.get("stale_or_mixed_sources") or []),
        "runtime_supervisor_decision_id": runtime_supervisor.get("supervisor_decision_id"),
        "runtime_supervisor_classification": runtime_supervisor.get("classification"),
        "supervisor_mode": runtime_supervisor.get("supervisor_mode"),
        "proof_window_status": runtime_supervisor.get("proof_window_status"),
        "recommended_next_command": runtime_supervisor.get("recommended_next_command"),
        **_runtime_resume_v2_fields(runtime_supervisor),
        **_self_recover_v2_fields(runtime_supervisor),
        **_recovery_attempt_history_fields(recovery_attempt_history),
        **_artifact_archive_plan_fields(artifact_archive_plan),
        **_continuation_aware_exit_preview_fields(continuation_aware_exit_preview),
        **_continuation_aware_exit_history_fields(continuation_aware_exit_history),
        "safe_to_start_runtime": runtime_supervisor.get("safe_to_start_runtime") is True
        and classification == CONTROL_PLANE_SNAPSHOT_READY
        and agent_health_evidence.get("agent_health_has_duplicate_writer") is not True
        and (
            agent_health_evidence.get("agent_health_blocks_runtime_submit") is not True
            or agent_health_runtime_submit_start_tolerated
        ),
        "paper_recovery_policy": evidence.get("paper_action_policy") or shared_truth.get("paper_recovery_policy"),
        **planner_explanation,
        **agent_health_evidence,
        **runtime_authority_exposure,
        "autonomous_recovery_plan_classification": runtime_supervisor.get(
            "autonomous_recovery_plan_classification"
        ),
        "autonomous_recovery_next_action": runtime_supervisor.get("autonomous_recovery_next_action"),
        "autonomous_recovery_execution_enabled": False,
        "broker_order_position_summary": _broker_order_position_summary(shared_truth, runtime_supervisor),
        "broker_position_guardian_classification": _mapping(shared_truth.get("classifications")).get(
            "Broker Position Guardian"
        ),
        "broker_position_guardian_blocks_submit": _mapping(shared_truth.get("classifications")).get(
            "Broker Position Guardian"
        )
        == "BROKER_POSITION_GUARDIAN_HARD_HOLD",
        "blockers": blockers,
        "warnings": warnings,
        "source_artifact_paths": {
            "shared_truth_refresh": str(config.resolve(config.shared_truth_refresh_path)),
            "agent_health": str(agent_health_path),
            "paper_autonomous_recovery_plan": str(config.resolve(config.paper_autonomous_recovery_plan_path)),
            "continuation_aware_exit_preview": str(config.resolve(config.continuation_aware_exit_preview_path)),
            "continuation_aware_exit_history": str(continuation_aware_exit_history_path),
            "recovery_attempt_history": str(recovery_attempt_history_path),
            "artifact_archive_plan": str(artifact_archive_plan_path),
            "runtime_supervisor_authority": str(runtime_supervisor_path),
            "control_plane_snapshot": str(config.resolve(config.output_path)),
            **_mapping(shared_truth.get("artifact_paths")),
        },
    }
    payload.update(build_track_b_control_plane_top_line(payload))
    return payload


def _snapshot_classification(
    *,
    coherence_status: str,
    generation_matches: bool,
    supervisor_classification: str,
    agent_health_evidence: Mapping[str, Any],
    agent_health_runtime_submit_start_tolerated: bool = False,
) -> str:
    if coherence_status != SHARED_TRUTH_COHERENT or not generation_matches:
        return CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED
    if (
        agent_health_evidence.get("agent_health_has_duplicate_writer") is True
        or (
            agent_health_evidence.get("agent_health_blocks_runtime_submit") is True
            and not agent_health_runtime_submit_start_tolerated
        )
        or agent_health_evidence.get("agent_health_blocks_recovery") is True
    ):
        return CONTROL_PLANE_SNAPSHOT_BLOCKED
    if supervisor_classification in {
        "SUPERVISOR_RUNTIME_START_ALLOWED",
        "SUPERVISOR_RUNTIME_ALREADY_HEALTHY",
        "SUPERVISOR_WAIT_MARKET_CLOSED",
    }:
        return CONTROL_PLANE_SNAPSHOT_READY
    return CONTROL_PLANE_SNAPSHOT_BLOCKED


def _runtime_resume_v2_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "runtime_resume_semantics_version": str(payload.get("runtime_resume_semantics_version") or ""),
        "runtime_resume_action_policy": str(payload.get("runtime_resume_action_policy") or ""),
        "runtime_resume_previous_runtime_generation_id": payload.get("runtime_resume_previous_runtime_generation_id"),
        "runtime_resume_proposed_next_runtime_generation_id": payload.get(
            "runtime_resume_proposed_next_runtime_generation_id"
        ),
        "runtime_resume_bounded_retry_budget_key": payload.get("runtime_resume_bounded_retry_budget_key"),
        "runtime_resume_attempts_remaining": payload.get("runtime_resume_attempts_remaining"),
        "runtime_resume_cooldown_until": payload.get("runtime_resume_cooldown_until"),
        "runtime_resume_generation_reuse_allowed": payload.get("runtime_resume_generation_reuse_allowed") is True,
        "runtime_resume_must_start_new_generation": payload.get("runtime_resume_must_start_new_generation") is True,
    }


def _self_recover_v2_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "self_recover_schema_version": str(payload.get("self_recover_schema_version") or ""),
        "self_recover_recovery_plan_id": payload.get("self_recover_recovery_plan_id"),
        "self_recover_control_plane_snapshot_id": payload.get("self_recover_control_plane_snapshot_id"),
        "self_recover_shared_truth_generation_id": payload.get("self_recover_shared_truth_generation_id"),
        "recommended_recovery_action": payload.get("self_recover_recommended_recovery_action"),
        "paper_action_policy": payload.get("self_recover_paper_action_policy"),
        "self_recover_autonomous_recovery_plan_classification": payload.get(
            "self_recover_autonomous_recovery_plan_classification"
        ),
        "recovery_budget_key": payload.get("self_recover_recovery_budget_key"),
        "attempts_remaining": payload.get("self_recover_attempts_remaining"),
        "cooldown_until": payload.get("self_recover_cooldown_until"),
        "quarantine_required": payload.get("self_recover_quarantine_required") is True,
        "self_recover_agent_health_top_blockers": list(payload.get("self_recover_agent_health_top_blockers") or []),
        "self_recover_operator_explanation": str(payload.get("self_recover_operator_explanation") or ""),
        "self_recover_execution_enabled": payload.get("self_recover_execution_enabled") is True,
    }


def _recovery_attempt_history_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    latest_attempt_id = str(payload.get("latest_recovery_attempt_id") or "")
    return {
        "latest_recovery_attempt_id": latest_attempt_id,
        "latest_recovery_attempt_action_type": payload.get("latest_action_type") or "",
        "latest_recovery_attempt_classification": payload.get("latest_classification") or "",
        "recovery_attempt_recommended_recovery_action": payload.get("recommended_recovery_action") or "",
        "recovery_attempt_recovery_budget_key": payload.get("recovery_budget_key") or "",
        "recovery_attempt_attempts_remaining": payload.get("attempts_remaining"),
        "recovery_attempt_quarantine_required": payload.get("quarantine_required") is True,
        "recovery_attempt_last_success_at": payload.get("last_success_at"),
        "recovery_attempt_last_failure_at": payload.get("last_failure_at"),
        "recovery_attempt_recent_attempts": list(payload.get("recent_attempts") or [])[:5],
        "recovery_attempt_active_budget_reservations": list(payload.get("active_budget_reservations") or [])[:5],
        "recovery_attempt_stale_or_incomplete_attempts": list(payload.get("stale_or_incomplete_attempts") or [])[:5],
        "recovery_attempt_operator_explanation": payload.get("operator_explanation") or "",
        "recovery_attempt_history_no_history": not bool(latest_attempt_id),
    }


def _artifact_archive_plan_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "artifact_archive_plan_classification": payload.get("classification") or "",
        "artifact_archive_hot_authority_protected_count": payload.get("hot_authority_protected_count"),
        "artifact_archive_active_lifecycle_protected_count": payload.get("active_lifecycle_protected_count"),
        "artifact_archive_warm_diagnostic_count": payload.get("warm_diagnostic_count"),
        "artifact_archive_cold_archive_candidate_count": payload.get("cold_archive_candidate_count"),
        "artifact_archive_blocked_candidate_count": payload.get("blocked_candidate_count"),
        "artifact_archive_estimated_bytes": payload.get("estimated_bytes"),
        "artifact_archive_dry_run_only": payload.get("dry_run_only") is True,
        "artifact_archive_execution_enabled": payload.get("execution_enabled") is True,
        "artifact_archive_diagnostic_only": True,
        "artifact_archive_not_routing_authority": True,
    }


def _continuation_aware_exit_preview_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    has_preview = bool(payload)
    source_report_path = (
        payload.get("source_strategy_report_path")
        or payload.get("source_lifecycle_report_path")
        or payload.get("source_report_path")
    )
    return {
        "continuation_aware_exit_strategy_id": payload.get("strategy_id") if has_preview else "",
        "continuation_aware_exit_symbol": payload.get("symbol") if has_preview else "",
        "continuation_aware_exit_policy_id": payload.get("exit_policy_id") if has_preview else "",
        "continuation_aware_exit_profile_id": payload.get("exit_profile_id") if has_preview else "",
        "continuation_aware_exit_state": payload.get("exit_state") if has_preview else "NO_CONTINUATION_AWARE_EXIT_PREVIEW",
        "continuation_aware_exit_quality_state": payload.get("continuation_quality_state") if has_preview else "",
        "continuation_aware_exit_should_request_close": payload.get("should_request_close") is True,
        "continuation_aware_exit_dry_run_only": payload.get("dry_run_only") is True,
        "continuation_aware_exit_not_order_authority": True,
        "continuation_aware_exit_not_lifecycle_authority": True,
        "continuation_aware_exit_missing_inputs": list(payload.get("missing_inputs") or []) if has_preview else [],
        "continuation_aware_exit_source_report_path": str(source_report_path or ""),
        "continuation_aware_exit_no_preview": not has_preview,
        "continuation_aware_exit_diagnostic_only": True,
        "continuation_aware_exit_not_routing_authority": True,
        "continuation_aware_exit_safe_state_authority": False,
        "continuation_aware_exit_order_authority": False,
        "continuation_aware_exit_lifecycle_authority": False,
    }


def _continuation_aware_exit_history_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    by_strategy = list(payload.get("by_strategy") or []) if payload else []
    top_rows = [
        {
            "strategy_id": row.get("strategy_id") or "",
            "symbol": row.get("symbol") or "",
            "latest_exit_profile_id": row.get("latest_exit_profile_id") or "",
            "latest_exit_state": row.get("latest_exit_state") or "",
            "latest_continuation_quality_state": row.get("latest_continuation_quality_state") or "",
            "latest_generated_at": row.get("latest_generated_at"),
            "hold_count": row.get("hold_count") or 0,
            "exit_preview_count": row.get("exit_preview_count") or 0,
            "insufficient_data_count": row.get("insufficient_data_count") or 0,
            "hard_override_count": row.get("hard_override_count") or 0,
            "latest_operator_summary": row.get("latest_operator_summary") or "",
        }
        for row in by_strategy[:3]
        if isinstance(row, Mapping)
    ]
    return {
        "continuation_aware_exit_history_classification": payload.get("history_classification")
        if payload
        else "CONTINUATION_EXIT_HISTORY_EMPTY",
        "continuation_aware_exit_history_total_events": payload.get("total_events") if payload else 0,
        "continuation_aware_exit_history_lookback_event_count": payload.get("lookback_event_count")
        if payload
        else 0,
        "continuation_aware_exit_history_strategy_count": payload.get("strategy_count") if payload else 0,
        "continuation_aware_exit_history_latest_strategy_id": payload.get("latest_strategy_id") if payload else "",
        "continuation_aware_exit_history_latest_symbol": payload.get("latest_symbol") if payload else "",
        "continuation_aware_exit_history_latest_exit_profile_id": payload.get("latest_exit_profile_id")
        if payload
        else "",
        "continuation_aware_exit_history_latest_exit_state": payload.get("latest_exit_state") if payload else "",
        "continuation_aware_exit_history_latest_quality_state": payload.get(
            "latest_continuation_quality_state"
        )
        if payload
        else "",
        "continuation_aware_exit_history_latest_generated_at": payload.get("latest_generated_at")
        if payload
        else None,
        "continuation_aware_exit_history_malformed_row_count": payload.get("malformed_row_count") if payload else 0,
        "continuation_aware_exit_history_top_strategies": top_rows,
        "continuation_aware_exit_history_dry_run_only": True,
        "continuation_aware_exit_history_diagnostic_only": True,
        "continuation_aware_exit_history_not_order_authority": True,
        "continuation_aware_exit_history_not_lifecycle_authority": True,
        "continuation_aware_exit_history_not_routing_authority": True,
    }


def _safe_state_envelope_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    classification = str(payload.get("safe_state_classification") or payload.get("classification") or "")
    return {
        "safe_state_classification": classification,
        "safe_state_broker_mutation_allowed": payload.get("broker_mutation_allowed") is True,
        "safe_state_entry_mutation_allowed": payload.get("entry_mutation_allowed") is True,
        "safe_state_managed_close_mutation_allowed": payload.get("managed_close_mutation_allowed") is True,
        "safe_state_close_authority_reason_codes": list(payload.get("close_authority_reason_codes") or []),
        "safe_state_close_authority": dict(_mapping(payload.get("close_authority"))),
        "risk_reducing_close_authority": dict(_mapping(payload.get("risk_reducing_close_authority"))),
        "risk_reducing_close_classification": str(payload.get("risk_reducing_close_classification") or ""),
        "risk_reducing_close_allowed_runtime_stale": payload.get("risk_reducing_close_allowed_runtime_stale") is True,
        "risk_reducing_close_candidate": dict(_mapping(payload.get("risk_reducing_close_candidate"))),
        "safe_state_runtime_start_allowed": payload.get("runtime_start_allowed") is True,
        "safe_state_submit_allowed": payload.get("submit_allowed") is True,
        "safe_state_observe_only": payload.get("observe_only") is True,
        "safe_state_recovery_only": payload.get("recovery_only") is True,
        "safe_state_tripped_limits": list(payload.get("tripped_limits") or []),
        "safe_state_limit_counters": dict(_mapping(payload.get("limit_counters"))),
        "safe_state_runtime_generation_id": payload.get("runtime_generation_id"),
        "safe_state_control_plane_snapshot_id": payload.get("control_plane_snapshot_id"),
        "safe_state_operator_explanation": str(payload.get("operator_explanation") or ""),
        "safe_state_recommended_next_step": str(payload.get("recommended_next_step") or ""),
    }


def _apply_safe_state_to_snapshot(*, payload: dict[str, Any], safe_state: Mapping[str, Any]) -> None:
    fields = _safe_state_envelope_fields(safe_state)
    payload.update(fields)
    classification = str(fields["safe_state_classification"] or "")
    if classification and classification != "SAFE_STATE_NORMAL":
        payload["classification"] = CONTROL_PLANE_SNAPSHOT_BLOCKED
        payload["safe_to_start_runtime"] = False
        payload.setdefault("blockers", []).append(
            {
                "code": "runtime_safe_state_envelope",
                "detail": f"Runtime Safe-State Envelope is {classification}.",
            }
        )
        if not payload.get("operator_explanation"):
            payload["operator_explanation"] = fields["safe_state_operator_explanation"]
        if not payload.get("recommended_observation_step"):
            payload["recommended_observation_step"] = fields["safe_state_recommended_next_step"]


def _snapshot_blockers(
    *,
    shared_truth: Mapping[str, Any],
    runtime_supervisor: Mapping[str, Any],
    agent_health_evidence: Mapping[str, Any],
    coherence_status: str,
    generation_matches: bool,
    agent_health_runtime_submit_start_tolerated: bool = False,
) -> list[dict[str, str]]:
    blockers: list[dict[str, str]] = []
    if coherence_status != SHARED_TRUTH_COHERENT:
        blockers.append(
            {
                "code": "shared_truth_coherence_not_confirmed",
                "detail": f"Runtime Supervisor reported shared_truth_coherence_status={coherence_status}.",
            }
        )
    if not generation_matches:
        blockers.append(
            {
                "code": "shared_truth_generation_mismatch",
                "detail": (
                    "Shared Truth Refresh generation does not match Runtime Supervisor Authority generation: "
                    f"{shared_truth.get('refresh_generation_id')} != "
                    f"{runtime_supervisor.get('shared_truth_refresh_generation_id')}."
                ),
            }
        )
    blockers.extend(_stringify_blockers(shared_truth.get("unsafe_blockers")))
    blockers.extend(_stringify_blockers(runtime_supervisor.get("blockers")))
    if agent_health_evidence.get("agent_health_has_duplicate_writer") is True:
        blockers.append(
            {
                "code": "agent_health_duplicate_writer",
                "detail": "Agent Health v2 reports duplicate Track B PAPER runtime writer evidence.",
            }
        )
    if (
        agent_health_evidence.get("agent_health_blocks_runtime_submit") is True
        and not agent_health_runtime_submit_start_tolerated
    ):
        blockers.append(
            {
                "code": "agent_health_blocks_runtime_submit",
                "detail": "Agent Health v2 reports required runtime-submit evidence is blocking.",
            }
        )
    if agent_health_evidence.get("agent_health_blocks_recovery") is True:
        blockers.append(
            {
                "code": "agent_health_blocks_recovery",
                "detail": "Agent Health v2 reports recovery-blocking process or artifact evidence.",
            }
        )
    return blockers


def _agent_health_runtime_submit_start_tolerated(
    *,
    runtime_supervisor: Mapping[str, Any],
    agent_health_evidence: Mapping[str, Any],
) -> bool:
    """Allow start-only recovery when the supervisor proves owned managed exposure.

    Agent Health reports a stopped runtime with broker exposure as submit-blocking.
    That remains true for new entries, but it must not veto a supervised runtime
    start when the pre-restart resolver has proven the exposure is exact,
    registry-backed, and restartable so managed-close maintenance can resume.
    """

    if runtime_supervisor.get("safe_to_start_runtime") is not True:
        return False
    if agent_health_evidence.get("agent_health_has_duplicate_writer") is True:
        return False
    if agent_health_evidence.get("agent_health_blocks_recovery") is True:
        return False
    if agent_health_evidence.get("agent_health_blocks_runtime_submit") is not True:
        return False
    evidence = _mapping(runtime_supervisor.get("evidence_summary"))
    if evidence.get("restart_with_owned_exposure_allowed") is not True:
        return False
    if str(evidence.get("pre_restart_exposure_resolution_classification") or "") != "MANAGED_EXPOSURE_RESOLVED":
        return False
    blockers = _list(agent_health_evidence.get("agent_health_top_blockers"))
    if not blockers:
        return False
    for blocker in blockers:
        row = _mapping(blocker)
        if row.get("blocking_for_runtime_submit") is not True:
            continue
        if str(row.get("reason") or "") != "RUNTIME_DOWN_WITH_BROKER_EXPOSURE":
            return False
    return True


def _runtime_authority_exposure_fields(
    *,
    runtime_supervisor: Mapping[str, Any],
    agent_health_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = _mapping(runtime_supervisor.get("evidence_summary"))
    runtime_classification = str(evidence.get("runtime_environment_truth_classification") or "")
    agent_reports_runtime_down_exposure = any(
        str(_mapping(blocker).get("reason") or "") == "RUNTIME_DOWN_WITH_BROKER_EXPOSURE"
        for blocker in _list(agent_health_evidence.get("agent_health_top_blockers"))
    )
    stale_runtime_with_exposure = (
        runtime_classification == "RUNTIME_DOWN_WITH_BROKER_EXPOSURE" or agent_reports_runtime_down_exposure
    )
    return {
        "runtime_authority_exposure_classification": (
            RUNTIME_AUTHORITY_STALE_WITH_BROKER_EXPOSURE if stale_runtime_with_exposure else ""
        ),
        "runtime_authority_stale_with_broker_exposure": stale_runtime_with_exposure,
        "fresh_broker_exposure_visible_when_runtime_stale": stale_runtime_with_exposure,
        "runtime_authority_stale_submit_blocked": stale_runtime_with_exposure,
        "runtime_authority_stale_maintenance_needed": (
            stale_runtime_with_exposure and evidence.get("restart_with_owned_exposure_allowed") is True
        ),
    }


def _broker_order_position_summary(
    shared_truth: Mapping[str, Any],
    runtime_supervisor: Mapping[str, Any],
) -> dict[str, Any]:
    classifications = _mapping(shared_truth.get("classifications"))
    evidence = _mapping(runtime_supervisor.get("evidence_summary"))
    return {
        "open_order_truth": classifications.get("Open Order Truth") or evidence.get("open_order_truth_classification"),
        "managed_order_registry": classifications.get("Managed Order Registry")
        or evidence.get("managed_order_registry_classification"),
        "position_truth": classifications.get("Position Truth") or evidence.get("position_truth_classification"),
        "runtime_environment_truth": classifications.get("Runtime Environment Truth")
        or evidence.get("runtime_environment_truth_classification"),
        "managed_position_registry": classifications.get("Managed Position Registry")
        or evidence.get("managed_position_registry_classification"),
        "reconciliation": classifications.get("Reconciliation") or evidence.get("reconciliation_classification"),
        "broker_truth_lease": classifications.get("Broker Truth Lease") or evidence.get("broker_lease_classification"),
        "broker_position_guardian": classifications.get("Broker Position Guardian"),
    }


def _agent_health_evidence(agent_health: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(agent_health.get("summary"))
    stale_pid_count = sum(
        1 for agent in _list(agent_health.get("agents")) if _mapping(agent).get("stale_pid_detected") is True
    )
    duplicate_process_count = _as_int(summary.get("duplicate_process_count"))
    blocking_for_proof_count = _as_int(summary.get("blocking_for_proof_count"))
    blocking_for_runtime_submit_count = _as_int(summary.get("blocking_for_runtime_submit_count"))
    blocking_for_recovery_count = _as_int(summary.get("blocking_for_recovery_count"))
    top_blockers = [
        {
            "agent_id": str(agent.get("agent_id") or ""),
            "display_name": str(agent.get("display_name") or agent.get("agent_id") or ""),
            "status": str(agent.get("status") or ""),
            "reason": str(agent.get("reason") or ""),
            "blocking_for_proof": agent.get("blocking_for_proof") is True,
            "blocking_for_runtime_submit": agent.get("blocking_for_runtime_submit") is True,
            "blocking_for_recovery": agent.get("blocking_for_recovery") is True,
            "diagnostic_only": agent.get("diagnostic_only") is True,
        }
        for agent in _list(agent_health.get("agents"))
        if isinstance(agent, Mapping)
        and (
            agent.get("blocking_for_proof") is True
            or agent.get("blocking_for_runtime_submit") is True
            or agent.get("blocking_for_recovery") is True
        )
    ][:5]
    return {
        "agent_health_schema_version": agent_health.get("schema_version"),
        "agent_health_classification": agent_health.get("classification"),
        "agent_health_summary": dict(summary),
        "agent_health_top_blockers": top_blockers,
        "blocking_for_proof_count": blocking_for_proof_count,
        "blocking_for_runtime_submit_count": blocking_for_runtime_submit_count,
        "blocking_for_recovery_count": blocking_for_recovery_count,
        "duplicate_process_count": duplicate_process_count,
        "missing_artifact_count": _as_int(summary.get("missing_artifact_count")),
        "stale_pid_count": stale_pid_count,
        "source_commit_mismatch_count": _as_int(summary.get("source_commit_mismatch_count")),
        "root_mismatch_count": _as_int(summary.get("root_mismatch_count")),
        "agent_health_blocks_proof": blocking_for_proof_count > 0,
        "agent_health_blocks_runtime_submit": blocking_for_runtime_submit_count > 0,
        "agent_health_blocks_recovery": blocking_for_recovery_count > 0,
        "agent_health_has_duplicate_writer": duplicate_process_count > 0,
    }


def _agent_health_warnings(evidence: Mapping[str, Any]) -> list[dict[str, str]]:
    warnings: list[dict[str, str]] = []
    if _as_int(evidence.get("stale_pid_count")):
        warnings.append(
            {
                "code": "agent_health_stale_pid_detected",
                "detail": f"Agent Health reports stale_pid_count={evidence.get('stale_pid_count')}.",
            }
        )
    for key in ("missing_artifact_count", "source_commit_mismatch_count", "root_mismatch_count"):
        count = _as_int(evidence.get(key))
        if count:
            warnings.append(
                {
                    "code": f"agent_health_{key}",
                    "detail": f"Agent Health reports {key}={count}.",
                }
            )
    return warnings


def _planner_explanation_fields(plan: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "primary_blocking_agent_id": str(plan.get("primary_blocking_agent_id") or ""),
        "primary_blocking_reason": str(plan.get("primary_blocking_reason") or ""),
        "operator_explanation": str(plan.get("operator_explanation") or ""),
        "recommended_observation_step": str(plan.get("recommended_observation_step") or ""),
        "prioritized_blockers": _planner_prioritized_blockers(plan.get("prioritized_blockers")),
    }


def _planner_prioritized_blockers(value: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _list(value):
        if not isinstance(item, Mapping):
            continue
        rows.append(
            {
                "agent_id": str(item.get("agent_id") or ""),
                "display_name": str(item.get("display_name") or item.get("agent_id") or ""),
                "status": str(item.get("status") or ""),
                "reason": str(item.get("reason") or ""),
                "blocking_for_proof": item.get("blocking_for_proof") is True,
                "blocking_for_runtime_submit": item.get("blocking_for_runtime_submit") is True,
                "blocking_for_recovery": item.get("blocking_for_recovery") is True,
                "diagnostic_only": item.get("diagnostic_only") is True,
                "source": str(item.get("source") or ""),
                "priority": _as_int(item.get("priority")),
            }
        )
    return rows[:8]


def _stringify_blockers(value: Any) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not isinstance(value, list):
        return rows
    for item in value:
        if not isinstance(item, Mapping):
            continue
        rows.append(
            {
                "code": str(item.get("code") or "blocker"),
                "detail": str(item.get("detail") or item.get("reason") or item),
            }
        )
    return rows


def _snapshot_id(value: datetime) -> str:
    return f"track-b-control-plane-{value.strftime('%Y%m%dT%H%M%S%fZ')}"


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(path, payload)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _csv_tuple(value: str | None) -> tuple[str, ...] | None:
    symbols = tuple(symbol.strip().upper() for symbol in str(value or "").split(",") if symbol.strip())
    return symbols or None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
