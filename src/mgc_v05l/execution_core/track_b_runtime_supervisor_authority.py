"""Read-only Track B PAPER runtime supervisor authority.

Runtime Supervisor Authority lives in execution_core. Dashboard artifacts are
projections and must not be used as runtime, readiness, restart, broker, or
routing authority. This v2 service recommends what should happen next with the
PAPER runtime only; it never starts, stops, restarts, submits, cancels,
replaces, closes, or flattens anything.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_agent_health import DEFAULT_AGENT_HEALTH_ARTIFACT
from mgc_v05l.execution_core.track_b_agent_registry import DEFAULT_AGENT_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_broker_truth_lease import DEFAULT_LEASE_ARTIFACT
from mgc_v05l.execution_core.track_b_crash_loop_protection import (
    DEFAULT_CRASH_LOOP_PROTECTION_ARTIFACT,
    OPERATOR_ACK_REQUIRED,
)
from mgc_v05l.execution_core.track_b_managed_order_registry import (
    ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING,
    DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT,
    NO_MANAGED_ORDERS,
)
from mgc_v05l.execution_core.track_b_managed_position_registry import (
    DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT,
    NO_MANAGED_POSITIONS,
)
from mgc_v05l.execution_core.track_b_order_adjustment_planner import DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT
from mgc_v05l.execution_core.track_b_open_order_truth import (
    BROKER_POSITION_WITHOUT_CLOSE_ORDER,
    DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT,
    NO_OPEN_ORDERS,
)
from mgc_v05l.execution_core.track_b_paper_proof_readiness import DEFAULT_OUTPUT_PATH as DEFAULT_PROOF_READINESS_ARTIFACT
from mgc_v05l.execution_core.track_b_paper_proof_readiness import READY_FOR_PROOF
from mgc_v05l.execution_core.track_b_position_truth_monitor import DEFAULT_POSITION_TRUTH_ARTIFACT
from mgc_v05l.execution_core.track_b_projection_metadata import build_projection_metadata
from mgc_v05l.execution_core.track_b_pre_restart_exposure_reconciliation import (
    PreRestartExposureResolverConfig,
    resolve_pre_restart_exposure_reconciliation,
)
from mgc_v05l.execution_core.track_b_runtime_environment_truth import (
    DEFAULT_CANONICAL_READINESS_ARTIFACT,
    DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT,
    RUNTIME_ACTIVE_OBSERVATION_ONLY,
    RUNTIME_ACTIVE_TRADE_CAPABLE,
    RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
)
from mgc_v05l.execution_core.track_b_runtime_resume_semantics import (
    DEFAULT_RUNTIME_RESUME_SEMANTICS_ARTIFACT,
    RESUME_ALLOWED_CLEAN,
    RESUME_BLOCKED_CRASH_LOOP,
    RESUME_BLOCKED_MARKET_CLOSED,
    RESUME_BLOCKED_OPERATOR_ACK_REQUIRED,
)
from mgc_v05l.execution_core.track_b_runtime_safe_state_envelope import (
    DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT,
    SAFE_STATE_BROKER_MUTATION_LIMIT_HIT,
    SAFE_STATE_DUPLICATE_INTENT_RISK,
    SAFE_STATE_HARD_HOLD,
    SAFE_STATE_LIFECYCLE_DISAGREEMENT_LIMIT_HIT,
    SAFE_STATE_POSITION_LIMIT_HIT,
    SAFE_STATE_RECOVERY_ONLY,
)
from mgc_v05l.execution_core.track_b_self_recover_rules import DEFAULT_SELF_RECOVER_RULES_ARTIFACT
from mgc_v05l.execution_core.track_b_shared_truth_refresh_cli import DEFAULT_RECONCILIATION_ARTIFACT
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


SUPERVISOR_NO_ACTION_NEEDED = "SUPERVISOR_NO_ACTION_NEEDED"
SUPERVISOR_WAIT_MARKET_CLOSED = "SUPERVISOR_WAIT_MARKET_CLOSED"
SUPERVISOR_RUNTIME_START_ALLOWED = "SUPERVISOR_RUNTIME_START_ALLOWED"
SUPERVISOR_RUNTIME_ALREADY_HEALTHY = "SUPERVISOR_RUNTIME_ALREADY_HEALTHY"
SUPERVISOR_RUNTIME_START_BLOCKED = "SUPERVISOR_RUNTIME_START_BLOCKED"
SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP = "SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP"
SUPERVISOR_RESTART_BLOCKED_OPERATOR_ACK = "SUPERVISOR_RESTART_BLOCKED_OPERATOR_ACK"
SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME = "SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME"
SUPERVISOR_MANUAL_REVIEW_REQUIRED = "SUPERVISOR_MANUAL_REVIEW_REQUIRED"
SUPERVISOR_SHARED_TRUTH_STALE = "SUPERVISOR_SHARED_TRUTH_STALE"
SUPERVISOR_UNKNOWN_REVIEW_REQUIRED = "SUPERVISOR_UNKNOWN_REVIEW_REQUIRED"
SUPERVISOR_PAPER_QUARANTINE_OBSERVE_ONLY = "SUPERVISOR_PAPER_QUARANTINE_OBSERVE_ONLY"
SUPERVISOR_HARD_UNSAFE_HOLD = "SUPERVISOR_HARD_UNSAFE_HOLD"

SHARED_TRUTH_COHERENT = "COHERENT"
SHARED_TRUTH_STALE_OR_MIXED = "STALE_OR_MIXED"
SHARED_TRUTH_MISSING = "MISSING"

MARKET_CLOSED_WAIT = "MARKET_CLOSED_WAIT"
READY_FOR_OPERATOR_START = "READY_FOR_OPERATOR_START"
RUNTIME_ACTIVE_MONITOR = "RUNTIME_ACTIVE_MONITOR"
CLEANUP_REQUIRED = "CLEANUP_REQUIRED"
MANUAL_REVIEW_REQUIRED = "MANUAL_REVIEW_REQUIRED"
CRASH_LOOP_HOLD = "CRASH_LOOP_HOLD"
STALE_EVIDENCE_HOLD = "STALE_EVIDENCE_HOLD"
PAPER_QUARANTINE_OBSERVE_ONLY = "PAPER_QUARANTINE_OBSERVE_ONLY"
HARD_UNSAFE_HOLD = "HARD_UNSAFE_HOLD"

PAPER_POLICY_AUTONOMOUS_RETRY_ELIGIBLE = "AUTONOMOUS_RETRY_ELIGIBLE"
PAPER_POLICY_QUARANTINE_OBSERVE_ONLY = "QUARANTINE_OBSERVE_ONLY"
PAPER_POLICY_HARD_UNSAFE_HOLD = "HARD_UNSAFE_HOLD"
PAPER_POLICY_OBSERVE = "OBSERVE"

PROOF_WINDOW_MARKET_CLOSED = "market_closed"
PROOF_WINDOW_DATA_STALE = "data_stale"
PROOF_WINDOW_READY = "ready"
PROOF_WINDOW_BLOCKED = "blocked"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "runtime_supervisor"
    / "latest_runtime_supervisor_authority.json"
)
DEFAULT_DASHBOARD_RUNTIME_SUPERVISOR_AUTHORITY_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_runtime_supervisor_authority.json"
)
DEFAULT_SHARED_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "shared_truth" / "latest_track_b_shared_truth_refresh.json"
)
DEFAULT_RUNTIME_STOP_PROVENANCE_ARTIFACT = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "latest_runtime_stop_provenance.json"
)
DEFAULT_PHASE1_READINESS_ARTIFACT = Path("outputs") / "reports" / "phase1_runtime_data_readiness" / "latest_phase1_runtime_data_readiness.json"
DEFAULT_PAPER_RECOVERY_POLICY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "paper_recovery_policy" / "latest_paper_recovery_policy.json"
)
DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_autonomous_recovery"
    / "latest_paper_autonomous_recovery_plan.json"
)


@dataclass(frozen=True)
class TrackBRuntimeSupervisorAuthorityConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_RUNTIME_SUPERVISOR_AUTHORITY_PROJECTION
    runtime_environment_truth_path: Path = DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT
    runtime_resume_semantics_path: Path = DEFAULT_RUNTIME_RESUME_SEMANTICS_ARTIFACT
    self_recover_rules_path: Path = DEFAULT_SELF_RECOVER_RULES_ARTIFACT
    crash_loop_protection_path: Path = DEFAULT_CRASH_LOOP_PROTECTION_ARTIFACT
    agent_health_path: Path = DEFAULT_AGENT_HEALTH_ARTIFACT
    agent_registry_path: Path = DEFAULT_AGENT_REGISTRY_ARTIFACT
    proof_readiness_path: Path = DEFAULT_PROOF_READINESS_ARTIFACT
    shared_truth_path: Path = DEFAULT_SHARED_TRUTH_ARTIFACT
    canonical_readiness_path: Path = DEFAULT_CANONICAL_READINESS_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    order_adjustment_plan_path: Path = DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    broker_lease_path: Path = DEFAULT_LEASE_ARTIFACT
    phase1_readiness_path: Path = DEFAULT_PHASE1_READINESS_ARTIFACT
    stop_provenance_path: Path = DEFAULT_RUNTIME_STOP_PROVENANCE_ARTIFACT
    paper_recovery_policy_path: Path = DEFAULT_PAPER_RECOVERY_POLICY_ARTIFACT
    paper_autonomous_recovery_plan_path: Path = DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT
    runtime_safe_state_envelope_path: Path = DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_runtime_supervisor_authority(
    *,
    config: TrackBRuntimeSupervisorAuthorityConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    inputs = {
        "runtime_environment_truth": _read_json(config.resolve(config.runtime_environment_truth_path)),
        "runtime_resume_semantics": _read_json(config.resolve(config.runtime_resume_semantics_path)),
        "self_recover_rules": _read_json(config.resolve(config.self_recover_rules_path)),
        "crash_loop_protection": _read_json(config.resolve(config.crash_loop_protection_path)),
        "agent_health": _read_json(config.resolve(config.agent_health_path)),
        "agent_registry": _read_json(config.resolve(config.agent_registry_path)),
        "proof_readiness": _read_json(config.resolve(config.proof_readiness_path)),
        "shared_truth": _read_json(config.resolve(config.shared_truth_path)),
        "canonical_readiness": _read_json(config.resolve(config.canonical_readiness_path)),
        "position_truth": _read_json(config.resolve(config.position_truth_path)),
        "open_order_truth": _read_json(config.resolve(config.open_order_truth_path)),
        "managed_order_registry": _read_json(config.resolve(config.managed_order_registry_path)),
        "managed_position_registry": _read_json(config.resolve(config.managed_position_registry_path)),
        "order_adjustment_plan": _read_json(config.resolve(config.order_adjustment_plan_path)),
        "reconciliation": _read_json(config.resolve(config.reconciliation_path)),
        "broker_lease": _read_json(config.resolve(config.broker_lease_path)),
        "phase1_readiness": _read_json(config.resolve(config.phase1_readiness_path)),
        "stop_provenance": _read_json(config.resolve(config.stop_provenance_path)),
        "paper_recovery_policy": _read_json(config.resolve(config.paper_recovery_policy_path)),
        "paper_autonomous_recovery_plan": _read_json(config.resolve(config.paper_autonomous_recovery_plan_path)),
        "runtime_safe_state_envelope": _read_json(config.resolve(config.runtime_safe_state_envelope_path)),
    }
    inputs["pre_restart_exposure_resolution"] = resolve_pre_restart_exposure_reconciliation(
        config=PreRestartExposureResolverConfig(repo_root=config.repo_root),
        broker_positions=_list(inputs["reconciliation"].get("track_b_broker_positions")),
        lifecycle_positions=_list(inputs["reconciliation"].get("track_b_lifecycle_positions")),
        broker_open_orders=_list(inputs["reconciliation"].get("track_b_broker_open_orders")),
    )
    inputs["shared_truth_coherence"] = _shared_truth_coherence(inputs)
    decision = _classify_supervisor(inputs=inputs)
    v2 = _v2_advisory(decision=decision, inputs=inputs)
    autonomous_recovery_plan = _autonomous_recovery_plan_fields(inputs["paper_autonomous_recovery_plan"])
    self_recover_plan = _self_recover_v2_fields(inputs["self_recover_rules"])
    shared_truth_coherence = _mapping(inputs["shared_truth_coherence"])
    return {
        "schema_version": "track_b_runtime_supervisor_authority_v2",
        "generated_at": actual_now.isoformat(),
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
        "supervisor_decision_id": _decision_id(actual_now),
        "classification": decision["classification"],
        "supervisor_mode": v2["supervisor_mode"],
        "recommended_action": decision["recommended_action"],
        "recommended_next_command": v2["recommended_next_command"],
        "action_allowed": decision["action_allowed"],
        "operator_ack_required": decision["operator_ack_required"],
        "operator_ack": v2["operator_ack"],
        "proof_window_status": v2["proof_window_status"],
        "decision_precedence": v2["decision_precedence"],
        **_runtime_resume_v2_fields(inputs["runtime_resume_semantics"]),
        **self_recover_plan,
        "shared_truth_refresh_generation_id": shared_truth_coherence.get("refresh_generation_id"),
        "shared_truth_refresh_generated_at": shared_truth_coherence.get("generated_at"),
        "shared_truth_coherence_status": shared_truth_coherence.get("status"),
        "shared_truth_generation_sources": list(shared_truth_coherence.get("source_services") or []),
        "stale_or_mixed_sources": list(shared_truth_coherence.get("stale_or_mixed_sources") or []),
        "autonomous_recovery_plan_classification": autonomous_recovery_plan["classification"],
        "autonomous_recovery_next_action": autonomous_recovery_plan["next_action"],
        "autonomous_recovery_execution_enabled": False,
        "autonomous_recovery_blockers": autonomous_recovery_plan["blockers"],
        "autonomous_recovery_budget_summary": autonomous_recovery_plan["budget_summary"],
        **_safe_state_fields(inputs["runtime_safe_state_envelope"]),
        "safe_to_start_runtime": decision["safe_to_start_runtime"],
        "safe_to_leave_runtime_running": decision["safe_to_leave_runtime_running"],
        "safe_to_stop_runtime": decision["safe_to_stop_runtime"],
        "reason": decision["reason"],
        "blockers": decision["blockers"],
        "warnings": decision["warnings"],
        "evidence_summary": _evidence(inputs),
        "input_artifacts": _input_artifacts(config),
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "dashboard_projection": None
            if config.dashboard_projection_path is None
            else str(config.resolve(config.dashboard_projection_path)),
        },
        "todo_v2": [
            "Wire self-healing executor to consult this advisory artifact before any runtime action.",
            "Consume PAPER Recovery Policy as the normal PAPER interpretation layer for bounded recovery posture.",
            "Consume PAPER Autonomous Recovery Planner as advisory evidence only until an executor boundary is approved.",
            "Converge manual remediation scripts on supervisor authority plus shared truth blockers.",
        ],
    }


def write_track_b_runtime_supervisor_authority(
    *,
    config: TrackBRuntimeSupervisorAuthorityConfig,
    payload: Mapping[str, Any],
) -> Path:
    authority_path = config.resolve(config.output_path)
    _write_json_atomic(authority_path, dict(payload))
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_runtime_supervisor_projection(
                authority_payload=payload,
                authority_path=authority_path,
            ),
        )
    return authority_path


def build_dashboard_runtime_supervisor_projection(
    *,
    authority_payload: Mapping[str, Any],
    authority_path: Path,
) -> dict[str, Any]:
    return {
        **dict(authority_payload),
        "schema_version": "track_b_runtime_supervisor_authority_dashboard_projection_v1",
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
    parser = argparse.ArgumentParser(description="Write read-only Track B PAPER runtime supervisor authority.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT)
    parser.add_argument("--dashboard-projection-path", type=Path, default=DEFAULT_DASHBOARD_RUNTIME_SUPERVISOR_AUTHORITY_PROJECTION)
    parser.add_argument("--no-dashboard-projection", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBRuntimeSupervisorAuthorityConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        dashboard_projection_path=None if bool(args.no_dashboard_projection) else Path(args.dashboard_projection_path),
    )
    payload = build_track_b_runtime_supervisor_authority(config=config)
    authority_path = write_track_b_runtime_supervisor_authority(config=config, payload=payload)
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            json.dumps(
                {
                    "classification": payload.get("classification"),
                    "supervisor_mode": payload.get("supervisor_mode"),
                    "recommended_action": payload.get("recommended_action"),
                    "recommended_next_command": payload.get("recommended_next_command"),
                    "action_allowed": payload.get("action_allowed"),
                    "operator_ack": payload.get("operator_ack"),
                    "proof_window_status": payload.get("proof_window_status"),
                    "runtime_resume_action_policy": payload.get("runtime_resume_action_policy"),
                    "runtime_resume_proposed_next_runtime_generation_id": payload.get(
                        "runtime_resume_proposed_next_runtime_generation_id"
                    ),
                    "runtime_resume_attempts_remaining": payload.get("runtime_resume_attempts_remaining"),
                    "runtime_resume_cooldown_until": payload.get("runtime_resume_cooldown_until"),
                    "self_recover_recommended_recovery_action": payload.get(
                        "self_recover_recommended_recovery_action"
                    ),
                    "self_recover_paper_action_policy": payload.get("self_recover_paper_action_policy"),
                    "self_recover_autonomous_recovery_plan_classification": payload.get(
                        "self_recover_autonomous_recovery_plan_classification"
                    ),
                    "self_recover_attempts_remaining": payload.get("self_recover_attempts_remaining"),
                    "self_recover_cooldown_until": payload.get("self_recover_cooldown_until"),
                    "self_recover_quarantine_required": payload.get("self_recover_quarantine_required"),
                    "autonomous_recovery_plan_classification": payload.get(
                        "autonomous_recovery_plan_classification"
                    ),
                    "autonomous_recovery_next_action": payload.get("autonomous_recovery_next_action"),
                    "autonomous_recovery_execution_enabled": payload.get("autonomous_recovery_execution_enabled"),
                    "safe_state_classification": payload.get("safe_state_classification"),
                    "safe_state_observe_only": payload.get("safe_state_observe_only"),
                    "safe_state_recovery_only": payload.get("safe_state_recovery_only"),
                    "safe_state_runtime_start_allowed": payload.get("safe_state_runtime_start_allowed"),
                    "safe_state_submit_allowed": payload.get("safe_state_submit_allowed"),
                    "shared_truth_refresh_generation_id": payload.get("shared_truth_refresh_generation_id"),
                    "shared_truth_coherence_status": payload.get("shared_truth_coherence_status"),
                    "reason": payload.get("reason"),
                    "authority_path": str(authority_path),
                    "read_only": True,
                    "paper_proof_invoked": False,
                    "live_money_eligible": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0 if payload.get("classification") in {SUPERVISOR_NO_ACTION_NEEDED, SUPERVISOR_WAIT_MARKET_CLOSED, SUPERVISOR_RUNTIME_START_ALLOWED, SUPERVISOR_RUNTIME_ALREADY_HEALTHY} else 2


def _classify_supervisor(*, inputs: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    evidence = _evidence(inputs)

    if evidence["safe_state_classification"] in {SAFE_STATE_HARD_HOLD, SAFE_STATE_POSITION_LIMIT_HIT}:
        return _decision(
            SUPERVISOR_HARD_UNSAFE_HOLD,
            "SAFE_STATE_HARD_HOLD",
            f"Runtime Safe-State Envelope is {evidence['safe_state_classification']}.",
            blockers=[_blocker("runtime_safe_state_envelope", evidence["safe_state_classification"])],
        )

    if evidence["safe_state_classification"] == SAFE_STATE_BROKER_MUTATION_LIMIT_HIT:
        return _decision(
            SUPERVISOR_PAPER_QUARANTINE_OBSERVE_ONLY,
            "SAFE_STATE_RECOVERY_ONLY",
            "Runtime Safe-State Envelope tripped broker mutation containment; PAPER shifts to recovery-only posture.",
            blockers=[_blocker("runtime_safe_state_envelope", evidence["safe_state_classification"])],
        )

    if evidence["safe_state_classification"] in {
        SAFE_STATE_DUPLICATE_INTENT_RISK,
        SAFE_STATE_LIFECYCLE_DISAGREEMENT_LIMIT_HIT,
        SAFE_STATE_RECOVERY_ONLY,
    }:
        return _decision(
            SUPERVISOR_PAPER_QUARANTINE_OBSERVE_ONLY,
            "SAFE_STATE_OBSERVE_OR_RECOVERY_ONLY",
            f"Runtime Safe-State Envelope is {evidence['safe_state_classification']}.",
            blockers=[_blocker("runtime_safe_state_envelope", evidence["safe_state_classification"])],
        )

    if evidence["live_money_eligible"] is True or evidence["paper_action_policy"] == PAPER_POLICY_HARD_UNSAFE_HOLD:
        return _decision(
            SUPERVISOR_HARD_UNSAFE_HOLD,
            "HARD_UNSAFE_HOLD",
            f"PAPER Recovery Policy is {evidence['paper_action_policy']} ({evidence['paper_policy_severity']}).",
            blockers=[_blocker("paper_recovery_policy", evidence["paper_action_policy"])],
        )

    if _duplicate_writer(evidence):
        return _decision(
            SUPERVISOR_HARD_UNSAFE_HOLD,
            "HARD_UNSAFE_HOLD",
            "Duplicate runtime writer evidence is present.",
            blockers=[_blocker("runtime_environment_truth", evidence["runtime_environment_truth_classification"])],
        )

    if evidence["shared_truth_coherence_status"] != SHARED_TRUTH_COHERENT:
        stale_sources = [
            str(_mapping(source).get("service") or _mapping(source).get("reason") or source)
            for source in evidence["shared_truth_stale_or_mixed_sources"]
        ]
        detail = ", ".join(stale_sources) if stale_sources else evidence["shared_truth_coherence_status"]
        return _decision(
            SUPERVISOR_SHARED_TRUTH_STALE,
            "REFRESH_SHARED_TRUTH",
            f"Shared Truth Refresh generation is {evidence['shared_truth_coherence_status']}: {detail}.",
            blockers=[
                _blocker("shared_truth_coherence", evidence["shared_truth_coherence_status"]),
                *[_blocker("stale_or_mixed_source", item) for item in stale_sources],
            ],
        )

    if evidence["runtime_environment_truth_classification"] in {
        RUNTIME_ACTIVE_TRADE_CAPABLE,
        RUNTIME_ACTIVE_OBSERVATION_ONLY,
    }:
        return _decision(
            SUPERVISOR_RUNTIME_ALREADY_HEALTHY,
            "LEAVE_RUNTIME_RUNNING",
            f"Runtime Environment Truth is {evidence['runtime_environment_truth_classification']}.",
            action_allowed=True,
            safe_to_leave_runtime_running=True,
        )

    suspicious_order = evidence["open_order_truth_classification"] in {
        "SUSPICIOUS_ORDER_STATE",
        "UNKNOWN_OPEN_ORDER",
        "DUPLICATE_CLOSE_ORDER",
        "CLOSE_ORDER_STALE",
        "CLOSE_ORDER_MARKETABLE_NOT_FILLED",
    } or evidence["managed_order_registry_classification"] in {
        "CLOSE_ORDER_SUSPICIOUS",
        "DUPLICATE_CLOSE_ORDER_BLOCKED",
        "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED",
        "BROKER_FLAT_WITH_WORKING_CLOSE",
    } or evidence["order_adjustment_plan_classification"] in {
        "REVIEW_REQUIRED_SUSPICIOUS_STATE",
        "DO_NOT_REPLACE_DUPLICATE_RISK",
    }
    if suspicious_order:
        return _decision(
            SUPERVISOR_MANUAL_REVIEW_REQUIRED,
            "MANUAL_REVIEW_REQUIRED",
            "Open Order Truth or Managed Order Registry indicates suspicious order state.",
            blockers=[
                _blocker("open_order_truth", evidence["open_order_truth_classification"]),
                _blocker("managed_order_registry", evidence["managed_order_registry_classification"]),
                _blocker("order_adjustment_plan", evidence["order_adjustment_plan_classification"]),
            ],
        )

    if _managed_active_hold_pending(evidence):
        return _decision(
            SUPERVISOR_RUNTIME_START_ALLOWED,
            "MANAGED_ACTIVE_HOLD",
            (
                "Managed PAPER exposure is reconciled, lifecycle-owned, and waiting inside its timed-exit "
                "profile; non-conflicting guarded lanes may proceed."
            ),
            action_allowed=True,
            safe_to_start_runtime=True,
        )

    if (
        evidence["runtime_environment_truth_classification"] == RUNTIME_DOWN_WITH_BROKER_EXPOSURE
        and _owned_managed_exposure_restart_allowed(evidence)
    ):
        return _decision(
            SUPERVISOR_RUNTIME_START_ALLOWED,
            "START_RUNTIME_WITH_OWNED_MANAGED_EXPOSURE",
            (
                "Exact registry-backed managed exposure is proven by the pre-restart resolver; "
                "runtime start is allowed so managed-exit maintenance can resume."
            ),
            action_allowed=True,
            safe_to_start_runtime=True,
            warnings=[
                _blocker(
                    "pre_restart_exposure_resolution",
                    evidence["pre_restart_exposure_resolution_classification"],
                )
            ],
        )

    if (
        evidence["runtime_environment_truth_classification"] == RUNTIME_DOWN_WITH_BROKER_EXPOSURE
        or evidence["position_truth_classification"] != "CLEAN_FLAT_READY"
    ):
        return _decision(
            SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME,
            "CLEANUP_REQUIRED_BEFORE_RUNTIME",
            f"Position Truth is {evidence['position_truth_classification']}.",
            blockers=[_blocker("position_truth", evidence["position_truth_classification"])],
        )

    if evidence["open_order_truth_classification"] != NO_OPEN_ORDERS:
        return _decision(
            SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME,
            "CLEANUP_REQUIRED_BEFORE_RUNTIME",
            f"Open Order Truth is {evidence['open_order_truth_classification']}.",
            blockers=[_blocker("open_order_truth", evidence["open_order_truth_classification"])],
        )

    if evidence["managed_order_registry_classification"] != NO_MANAGED_ORDERS:
        return _decision(
            SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME,
            "CLEANUP_REQUIRED_BEFORE_RUNTIME",
            f"Managed Order Registry is {evidence['managed_order_registry_classification']}.",
            blockers=[_blocker("managed_order_registry", evidence["managed_order_registry_classification"])],
        )

    if evidence["managed_position_registry_classification"] != NO_MANAGED_POSITIONS:
        return _decision(
            SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME,
            "CLEANUP_REQUIRED_BEFORE_RUNTIME",
            f"Managed Position Registry is {evidence['managed_position_registry_classification']}.",
            blockers=[_blocker("managed_position_registry", evidence["managed_position_registry_classification"])],
        )

    if evidence["runtime_resume_required_operator_ack"] is True or evidence["crash_loop_operator_ack_required"] is True:
        if _paper_policy_bounded_retry(evidence) and _clean_start_facts(evidence):
            return _decision(
                SUPERVISOR_RUNTIME_START_ALLOWED,
                "START_RUNTIME_ALLOWED",
                "PAPER Recovery Policy permits bounded retry/enhanced observation despite legacy operator-ack evidence.",
                action_allowed=True,
                safe_to_start_runtime=True,
                warnings=[
                    _blocker(
                        "paper_recovery_policy",
                        f"{evidence['paper_action_policy']} severity={evidence['paper_policy_severity']}",
                    )
                ],
            )
        if _paper_policy_quarantine(evidence):
            return _decision(
                SUPERVISOR_PAPER_QUARANTINE_OBSERVE_ONLY,
                "PAPER_QUARANTINE_OBSERVE_ONLY",
                "PAPER Recovery Policy converts operator-ack/crash-loop evidence into quarantine-observe posture.",
                blockers=[_blocker("paper_recovery_policy", evidence["paper_action_policy"])],
            )
        return _decision(
            SUPERVISOR_RESTART_BLOCKED_OPERATOR_ACK,
            "HOLD_DOWN_OPERATOR_ACK_REQUIRED",
            (
                "Legacy operator-ack evidence is present; PAPER treats this as advisory hold/quarantine "
                "unless PAPER Recovery Policy explicitly requires acknowledgement."
            ),
            operator_ack_required=bool(evidence.get("paper_requires_operator_ack")),
            blockers=[
                _blocker("runtime_resume", evidence["runtime_resume_classification"]),
                _blocker("crash_loop_protection", evidence["crash_loop_classification"]),
            ],
        )

    if evidence["crash_loop_restart_blocked"] is True or evidence["runtime_resume_classification"] == RESUME_BLOCKED_CRASH_LOOP:
        if _paper_policy_quarantine(evidence):
            return _decision(
                SUPERVISOR_PAPER_QUARANTINE_OBSERVE_ONLY,
                "PAPER_QUARANTINE_OBSERVE_ONLY",
                "PAPER Recovery Policy marks crash-loop budget exhausted as quarantine-observe, not operator ack.",
                blockers=[_blocker("paper_recovery_policy", evidence["paper_action_policy"])],
                warnings=[_blocker("crash_loop_protection", evidence["crash_loop_classification"])],
            )
        return _decision(
            SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP,
            "HOLD_DOWN_CRASH_LOOP",
            (
                f"Crash Loop Protection is {evidence['crash_loop_classification']}; "
                "PAPER holds/quarantines without making operator acknowledgement a routine dependency."
            ),
            blockers=[_blocker("crash_loop_protection", evidence["crash_loop_classification"])],
        )

    if _market_closed(inputs):
        if _clean_diagnostic_runtime_start_facts(evidence):
            return _decision(
                SUPERVISOR_RUNTIME_START_ALLOWED,
                "START_RUNTIME_DIAGNOSTIC_ONLY",
                MARKET_CLOSED_NO_FRESH_BARS,
                action_allowed=True,
                safe_to_start_runtime=True,
                warnings=[_blocker("market_session", MARKET_CLOSED_NO_FRESH_BARS)],
            )
        return _decision(
            SUPERVISOR_WAIT_MARKET_CLOSED,
            "WAIT_MARKET_CLOSED",
            MARKET_CLOSED_NO_FRESH_BARS,
            action_allowed=True,
            warnings=[_blocker("market_session", MARKET_CLOSED_NO_FRESH_BARS)],
        )

    missing = _missing_or_stale_evidence(evidence)
    if missing:
        return _decision(
            SUPERVISOR_SHARED_TRUTH_STALE,
            "REFRESH_SHARED_TRUTH",
            f"Stale or missing authority evidence: {', '.join(missing)}.",
            blockers=[_blocker("stale_or_missing_evidence", item) for item in missing],
        )

    if (
        evidence["proof_readiness_classification"] == READY_FOR_PROOF
        and evidence["runtime_resume_classification"] == RESUME_ALLOWED_CLEAN
        and evidence["crash_loop_restart_blocked"] is False
    ):
        return _decision(
            SUPERVISOR_RUNTIME_START_ALLOWED,
            "START_RUNTIME_ALLOWED",
            "Clean shared truth, proof readiness is READY_FOR_PROOF, and resume semantics allow a clean start.",
            action_allowed=True,
            safe_to_start_runtime=True,
        )

    if evidence["proof_readiness_classification"] and evidence["proof_readiness_classification"] != READY_FOR_PROOF:
        return _decision(
            SUPERVISOR_SHARED_TRUTH_STALE,
            "REFRESH_PROOF_READINESS",
            f"Proof Readiness is {evidence['proof_readiness_classification']}.",
            blockers=[_blocker("proof_readiness", evidence["proof_readiness_classification"])],
        )

    if evidence["runtime_resume_classification"] in {
        RESUME_BLOCKED_MARKET_CLOSED,
        RESUME_BLOCKED_CRASH_LOOP,
        RESUME_BLOCKED_OPERATOR_ACK_REQUIRED,
    }:
        return _decision(
            SUPERVISOR_RUNTIME_START_BLOCKED,
            "HOLD_DOWN_RUNTIME",
            f"Runtime Resume Semantics is {evidence['runtime_resume_classification']}.",
            blockers=[_blocker("runtime_resume", evidence["runtime_resume_classification"])],
        )

    if evidence["runtime_environment_truth_classification"]:
        return _decision(
            SUPERVISOR_NO_ACTION_NEEDED,
            "NO_ACTION_NEEDED",
            f"Runtime supervisor has no allowed action for {evidence['runtime_environment_truth_classification']}.",
            action_allowed=True,
        )

    return _decision(
        SUPERVISOR_UNKNOWN_REVIEW_REQUIRED,
        "OPERATOR_REVIEW_REQUIRED",
        "Runtime supervisor authority could not classify current state from available evidence.",
        blockers=[_blocker("unknown_state", "missing_runtime_environment_truth")],
    )


def _decision(
    classification: str,
    recommended_action: str,
    reason: str,
    *,
    action_allowed: bool = False,
    operator_ack_required: bool = False,
    safe_to_start_runtime: bool = False,
    safe_to_leave_runtime_running: bool = False,
    safe_to_stop_runtime: bool = False,
    blockers: list[dict[str, str]] | None = None,
    warnings: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    return {
        "classification": classification,
        "recommended_action": recommended_action,
        "action_allowed": action_allowed,
        "operator_ack_required": operator_ack_required,
        "safe_to_start_runtime": safe_to_start_runtime,
        "safe_to_leave_runtime_running": safe_to_leave_runtime_running,
        "safe_to_stop_runtime": safe_to_stop_runtime,
        "reason": reason,
        "blockers": blockers or [],
        "warnings": warnings or [],
    }


def _v2_advisory(*, decision: Mapping[str, Any], inputs: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    evidence = _evidence(inputs)
    classification = str(decision.get("classification") or "")
    supervisor_mode = _supervisor_mode(classification=classification)
    proof_window_status = _proof_window_status(inputs=inputs, evidence=evidence)
    return {
        "supervisor_mode": supervisor_mode,
        "recommended_next_command": _recommended_next_command(
            supervisor_mode=supervisor_mode,
            classification=classification,
            proof_window_status=proof_window_status,
        ),
        "operator_ack": _operator_ack_fields(
            classification=classification,
            decision=decision,
            evidence=evidence,
        ),
        "proof_window_status": proof_window_status,
        "decision_precedence": _decision_precedence(
            classification=classification,
            supervisor_mode=supervisor_mode,
            evidence=evidence,
        ),
    }


def _supervisor_mode(*, classification: str) -> str:
    if classification == SUPERVISOR_WAIT_MARKET_CLOSED:
        return MARKET_CLOSED_WAIT
    if classification == SUPERVISOR_RUNTIME_START_ALLOWED:
        return READY_FOR_OPERATOR_START
    if classification == SUPERVISOR_RUNTIME_ALREADY_HEALTHY:
        return RUNTIME_ACTIVE_MONITOR
    if classification == SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME:
        return CLEANUP_REQUIRED
    if classification in {SUPERVISOR_MANUAL_REVIEW_REQUIRED, SUPERVISOR_RESTART_BLOCKED_OPERATOR_ACK}:
        return MANUAL_REVIEW_REQUIRED
    if classification == SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP:
        return CRASH_LOOP_HOLD
    if classification == SUPERVISOR_SHARED_TRUTH_STALE:
        return STALE_EVIDENCE_HOLD
    if classification == SUPERVISOR_PAPER_QUARANTINE_OBSERVE_ONLY:
        return PAPER_QUARANTINE_OBSERVE_ONLY
    if classification == SUPERVISOR_HARD_UNSAFE_HOLD:
        return HARD_UNSAFE_HOLD
    return MANUAL_REVIEW_REQUIRED


def _recommended_next_command(*, supervisor_mode: str, classification: str, proof_window_status: str) -> str:
    if supervisor_mode == MARKET_CLOSED_WAIT:
        return "wait for market reopen; rerun proof readiness before any runtime start"
    if supervisor_mode == READY_FOR_OPERATOR_START:
        return "operator may start Track B PAPER runtime using the repaired direct supervisor launcher"
    if supervisor_mode == RUNTIME_ACTIVE_MONITOR:
        return "monitor active runtime through execution_core shared truth artifacts"
    if supervisor_mode == CLEANUP_REQUIRED:
        return "perform exact scoped cleanup only after operator authorization"
    if supervisor_mode == CRASH_LOOP_HOLD:
        return "preserve crash-loop evidence; wait for PAPER recovery budget reset or policy refresh"
    if supervisor_mode == STALE_EVIDENCE_HOLD:
        return "run shared truth refresh and proof readiness"
    if supervisor_mode == PAPER_QUARANTINE_OBSERVE_ONLY:
        return "quarantine runtime recovery; refresh evidence and preserve abnormal PAPER artifacts"
    if supervisor_mode == HARD_UNSAFE_HOLD:
        return "hold runtime recovery until hard unsafe invariant is resolved"
    if classification == SUPERVISOR_RESTART_BLOCKED_OPERATOR_ACK:
        return "refresh PAPER recovery policy; operator ack is not a routine PAPER recovery dependency"
    if proof_window_status == PROOF_WINDOW_DATA_STALE:
        return "restore Phase-1 market-data freshness before proof"
    return "PAPER advisory manual review: quarantine/observe, refresh evidence, and preserve artifacts before any mutation"


def _operator_ack_fields(
    *,
    classification: str,
    decision: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    required = bool(decision.get("operator_ack_required"))
    reason = ""
    ack_type = None
    if classification == SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP:
        required = bool(evidence.get("paper_requires_operator_ack"))
        reason = f"Crash Loop Protection is {evidence['crash_loop_classification']}."
        ack_type = "crash_loop_hold"
    elif classification == SUPERVISOR_RESTART_BLOCKED_OPERATOR_ACK:
        required = bool(evidence.get("paper_requires_operator_ack"))
        reason = "Legacy operator-ack evidence is present; PAPER treats acknowledgement as advisory unless policy says otherwise."
        ack_type = "unsafe_stop_or_operator_ack"
    elif classification == SUPERVISOR_MANUAL_REVIEW_REQUIRED:
        reason = "Manual review is advisory in PAPER unless a hard invariant or ambiguous mutation identity is present."
        ack_type = "manual_review"
    return {
        "required": required,
        "reason": reason,
        "ack_type": ack_type,
        "ack_id_expected": _ack_id_expected(ack_type=ack_type, evidence=evidence) if required else None,
    }


def _ack_id_expected(*, ack_type: str | None, evidence: Mapping[str, Any]) -> str | None:
    if not ack_type:
        return None
    stop = _mapping(evidence.get("stop_provenance"))
    runtime_instance_id = str(stop.get("runtime_instance_id") or "unknown-runtime")
    stop_reason = str(stop.get("stop_reason") or evidence.get("crash_loop_classification") or "unknown")
    return f"track_b_paper_{ack_type}_{runtime_instance_id}_{stop_reason}"


def _proof_window_status(*, inputs: Mapping[str, Mapping[str, Any]], evidence: Mapping[str, Any]) -> str:
    if _market_closed(inputs):
        return PROOF_WINDOW_MARKET_CLOSED
    proof_classification = evidence["proof_readiness_classification"]
    phase1_classification = evidence["phase1_readiness_classification"]
    phase1_session = evidence["phase1_session_reason"]
    if proof_classification == READY_FOR_PROOF:
        return PROOF_WINDOW_READY
    if phase1_classification and phase1_classification not in {READY_FOR_PROOF, MARKET_CLOSED_NO_FRESH_BARS}:
        return PROOF_WINDOW_DATA_STALE
    if phase1_session and phase1_session not in {READY_FOR_PROOF, MARKET_CLOSED_NO_FRESH_BARS}:
        return PROOF_WINDOW_DATA_STALE
    if proof_classification and "PHASE1" in proof_classification:
        return PROOF_WINDOW_DATA_STALE
    return PROOF_WINDOW_BLOCKED


def _decision_precedence(
    *,
    classification: str,
    supervisor_mode: str,
    evidence: Mapping[str, Any],
) -> list[dict[str, Any]]:
    decisive_service = {
        SUPERVISOR_RUNTIME_ALREADY_HEALTHY: "Runtime Environment Truth",
        SUPERVISOR_MANUAL_REVIEW_REQUIRED: "Open Order Truth / Managed Order Registry / Order Adjustment Planner",
        SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME: "Position Truth / Managed Position Registry",
        SUPERVISOR_RESTART_BLOCKED_OPERATOR_ACK: "Runtime Resume Semantics / Crash Loop Protection",
        SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP: "Crash Loop Protection",
        SUPERVISOR_PAPER_QUARANTINE_OBSERVE_ONLY: "PAPER Recovery Policy",
        SUPERVISOR_HARD_UNSAFE_HOLD: "PAPER Recovery Policy / Runtime Environment Truth",
        SUPERVISOR_WAIT_MARKET_CLOSED: "Proof Readiness / Phase-1 Readiness",
        SUPERVISOR_SHARED_TRUTH_STALE: "Shared Truth Evidence Freshness",
        SUPERVISOR_RUNTIME_START_ALLOWED: "Proof Readiness / Runtime Resume Semantics",
        SUPERVISOR_RUNTIME_START_BLOCKED: "Runtime Resume Semantics",
        SUPERVISOR_NO_ACTION_NEEDED: "Runtime Environment Truth",
    }.get(classification, "Runtime Supervisor Authority")
    return [
        {
            "rank": 1,
            "service": decisive_service,
            "classification": classification,
            "supervisor_mode": supervisor_mode,
            "decisive": True,
        },
        {
            "rank": 2,
            "service": "Proof Readiness",
            "classification": evidence["proof_readiness_classification"],
            "decisive": decisive_service == "Proof Readiness / Phase-1 Readiness",
        },
        {
            "rank": 3,
            "service": "Runtime Resume Semantics",
            "classification": evidence["runtime_resume_classification"],
            "decisive": decisive_service in {
                "Runtime Resume Semantics / Crash Loop Protection",
                "Runtime Resume Semantics",
            },
        },
        {
            "rank": 4,
            "service": "PAPER Recovery Policy",
            "classification": {
                "severity": evidence["paper_policy_severity"],
                "paper_action_policy": evidence["paper_action_policy"],
                "autonomous_recovery_allowed": evidence["paper_autonomous_recovery_allowed"],
                "requires_operator_ack_for_paper": evidence["paper_requires_operator_ack"],
            },
            "decisive": decisive_service == "PAPER Recovery Policy",
        },
        {
            "rank": 5,
            "service": "PAPER Autonomous Recovery Planner",
            "classification": {
                "classification": evidence["autonomous_recovery_plan_classification"],
                "next_action": evidence["autonomous_recovery_next_action"],
                "execution_enabled": evidence["autonomous_recovery_execution_enabled"],
            },
            "decisive": False,
        },
        {
            "rank": 6,
            "service": "Crash Loop Protection",
            "classification": evidence["crash_loop_classification"],
            "decisive": decisive_service == "Crash Loop Protection",
        },
        {
            "rank": 7,
            "service": "Shared Truth",
            "classification": {
                "position_truth": evidence["position_truth_classification"],
                "open_order_truth": evidence["open_order_truth_classification"],
                "managed_order_registry": evidence["managed_order_registry_classification"],
                "managed_position_registry": evidence["managed_position_registry_classification"],
                "reconciliation": evidence["reconciliation_classification"],
                "broker_lease": evidence["broker_lease_classification"],
            },
            "decisive": False,
        },
    ]


def _evidence(inputs: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "runtime_environment_truth_classification": _classification(inputs["runtime_environment_truth"]),
        "runtime_resume_classification": _classification(inputs["runtime_resume_semantics"]),
        **_runtime_resume_v2_fields(inputs["runtime_resume_semantics"]),
        "runtime_resume_allowed": inputs["runtime_resume_semantics"].get("allowed") is True,
        "runtime_resume_safe_to_start_runtime": inputs["runtime_resume_semantics"].get("safe_to_start_runtime") is True,
        "runtime_resume_required_operator_ack": inputs["runtime_resume_semantics"].get("required_operator_ack") is True,
        "self_recover_recommendation": str(inputs["self_recover_rules"].get("recommendation") or _classification(inputs["self_recover_rules"])),
        **_self_recover_v2_fields(inputs["self_recover_rules"]),
        "crash_loop_classification": _classification(inputs["crash_loop_protection"]),
        "crash_loop_restart_blocked": inputs["crash_loop_protection"].get("restart_blocked") is True,
        "crash_loop_operator_ack_required": inputs["crash_loop_protection"].get("operator_ack_required") is True
        or _classification(inputs["crash_loop_protection"]) == OPERATOR_ACK_REQUIRED,
        "agent_health_classification": _classification(inputs["agent_health"]),
        "agent_registry_classification": _classification(inputs["agent_registry"]),
        "proof_readiness_classification": _classification(inputs["proof_readiness"]),
        "shared_truth_classification": _classification(inputs["shared_truth"]),
        "pre_restart_exposure_resolution": dict(_mapping(inputs.get("pre_restart_exposure_resolution"))),
        "pre_restart_exposure_resolution_classification": str(
            _mapping(inputs.get("pre_restart_exposure_resolution")).get("classification") or ""
        ),
        "restart_with_owned_exposure_allowed": _mapping(inputs.get("pre_restart_exposure_resolution")).get(
            "restart_with_owned_exposure_allowed"
        )
        is True,
        "shared_truth_classifications": _mapping(inputs["shared_truth"].get("classifications")),
        "shared_truth_refresh_generation_id": str(
            _mapping(inputs["shared_truth_coherence"]).get("refresh_generation_id") or ""
        ),
        "shared_truth_refresh_generated_at": str(_mapping(inputs["shared_truth_coherence"]).get("generated_at") or ""),
        "shared_truth_coherence_status": str(_mapping(inputs["shared_truth_coherence"]).get("status") or SHARED_TRUTH_MISSING),
        "shared_truth_stale_or_mixed_sources": list(
            _mapping(inputs["shared_truth_coherence"]).get("stale_or_mixed_sources") or []
        ),
        "canonical_readiness": str(inputs["canonical_readiness"].get("canonical_readiness") or _classification(inputs["canonical_readiness"])),
        "position_truth_classification": _position_truth_classification(inputs["position_truth"]),
        "open_order_truth_classification": _classification(inputs["open_order_truth"]),
        "managed_order_registry_classification": _classification(inputs["managed_order_registry"]),
        "managed_position_registry_classification": _classification(inputs["managed_position_registry"]),
        "order_adjustment_plan_classification": _classification(inputs["order_adjustment_plan"]),
        "reconciliation_classification": _classification(inputs["reconciliation"]),
        "broker_lease_classification": _classification(inputs["broker_lease"])
        or str(inputs["broker_lease"].get("lease_state") or ""),
        "phase1_readiness_classification": _classification(inputs["phase1_readiness"]),
        "phase1_session_reason": _phase1_session_reason(inputs["phase1_readiness"], inputs["proof_readiness"]),
        "stop_provenance": dict(_stop_provenance(inputs["stop_provenance"])),
        "paper_recovery_policy": _paper_policy_evidence(inputs["paper_recovery_policy"]),
        "paper_autonomous_recovery_plan": _autonomous_recovery_plan_fields(inputs["paper_autonomous_recovery_plan"]),
        "runtime_safe_state_envelope": _safe_state_fields(inputs["runtime_safe_state_envelope"]),
        **_safe_state_evidence_fields(inputs["runtime_safe_state_envelope"]),
        "autonomous_recovery_plan_classification": _classification(inputs["paper_autonomous_recovery_plan"]),
        "autonomous_recovery_next_action": _autonomous_recovery_next_action(inputs["paper_autonomous_recovery_plan"]),
        "autonomous_recovery_execution_enabled": inputs["paper_autonomous_recovery_plan"].get("execution_enabled") is True,
        "autonomous_recovery_blockers": list(inputs["paper_autonomous_recovery_plan"].get("blockers") or []),
        "autonomous_recovery_budget_summary": _autonomous_recovery_budget_summary(
            inputs["paper_autonomous_recovery_plan"]
        ),
        "paper_policy_severity": str(inputs["paper_recovery_policy"].get("severity") or ""),
        "paper_action_policy": str(inputs["paper_recovery_policy"].get("paper_action_policy") or ""),
        "paper_live_action_policy": str(inputs["paper_recovery_policy"].get("live_action_policy") or ""),
        "paper_autonomous_recovery_allowed": inputs["paper_recovery_policy"].get("autonomous_recovery_allowed") is True,
        "paper_requires_operator_ack": inputs["paper_recovery_policy"].get("requires_operator_ack_for_paper") is True,
        "paper_bounded_recovery_budget": _mapping(inputs["paper_recovery_policy"].get("bounded_recovery_budget")),
        "live_money_eligible": _any_true(
            inputs["runtime_environment_truth"],
            inputs["runtime_resume_semantics"],
            inputs["self_recover_rules"],
            inputs["crash_loop_protection"],
            inputs["agent_health"],
            inputs["proof_readiness"],
            inputs["position_truth"],
            inputs["open_order_truth"],
            inputs["managed_order_registry"],
            inputs["managed_position_registry"],
            inputs["reconciliation"],
            inputs["paper_recovery_policy"],
            inputs["paper_autonomous_recovery_plan"],
            inputs["runtime_safe_state_envelope"],
            key="live_money_eligible",
        ),
        "paper_proof_invoked": _any_true(
            inputs["runtime_environment_truth"],
            inputs["runtime_resume_semantics"],
            inputs["self_recover_rules"],
            inputs["crash_loop_protection"],
            inputs["agent_health"],
            inputs["proof_readiness"],
            inputs["position_truth"],
            inputs["open_order_truth"],
            inputs["managed_order_registry"],
            inputs["managed_position_registry"],
            inputs["reconciliation"],
            inputs["paper_recovery_policy"],
            inputs["paper_autonomous_recovery_plan"],
            inputs["runtime_safe_state_envelope"],
            key="paper_proof_invoked",
        ),
        "runtime_writer_authority": str(inputs["runtime_environment_truth"].get("writer_authority") or ""),
        "duplicate_writer_count": int(inputs["runtime_environment_truth"].get("duplicate_writer_count") or 0),
    }


def _runtime_resume_v2_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "runtime_resume_semantics_version": str(payload.get("resume_semantics_version") or ""),
        "runtime_resume_action_policy": str(payload.get("resume_action_policy") or ""),
        "runtime_resume_previous_runtime_generation_id": payload.get("previous_runtime_generation_id"),
        "runtime_resume_proposed_next_runtime_generation_id": payload.get("proposed_next_runtime_generation_id"),
        "runtime_resume_bounded_retry_budget_key": payload.get("bounded_retry_budget_key"),
        "runtime_resume_attempts_remaining": payload.get("attempts_remaining"),
        "runtime_resume_cooldown_until": payload.get("cooldown_until"),
        "runtime_resume_generation_reuse_allowed": payload.get("generation_reuse_allowed") is True,
        "runtime_resume_must_start_new_generation": payload.get("must_start_new_generation") is True,
    }


def _self_recover_v2_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "self_recover_schema_version": str(payload.get("self_recover_schema_version") or ""),
        "self_recover_recovery_plan_id": payload.get("recovery_plan_id"),
        "self_recover_control_plane_snapshot_id": payload.get("control_plane_snapshot_id"),
        "self_recover_shared_truth_generation_id": payload.get("shared_truth_generation_id"),
        "self_recover_recommended_recovery_action": payload.get("recommended_recovery_action"),
        "self_recover_paper_action_policy": payload.get("paper_action_policy"),
        "self_recover_autonomous_recovery_plan_classification": payload.get(
            "autonomous_recovery_plan_classification"
        ),
        "self_recover_recovery_budget_key": payload.get("recovery_budget_key"),
        "self_recover_attempts_remaining": payload.get("attempts_remaining"),
        "self_recover_cooldown_until": payload.get("cooldown_until"),
        "self_recover_quarantine_required": payload.get("quarantine_required") is True,
        "self_recover_agent_health_top_blockers": list(payload.get("agent_health_top_blockers") or []),
        "self_recover_operator_explanation": str(payload.get("operator_explanation") or ""),
        "self_recover_execution_enabled": payload.get("execution_enabled") is True,
    }


def _safe_state_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "safe_state_classification": str(
            payload.get("safe_state_classification") or payload.get("classification") or ""
        ),
        "safe_state_broker_mutation_allowed": payload.get("broker_mutation_allowed") is True,
        "safe_state_runtime_start_allowed": payload.get("runtime_start_allowed") is True,
        "safe_state_submit_allowed": payload.get("submit_allowed") is True,
        "safe_state_observe_only": payload.get("observe_only") is True,
        "safe_state_recovery_only": payload.get("recovery_only") is True,
        "safe_state_tripped_limits": list(payload.get("tripped_limits") or []),
        "safe_state_limit_counters": dict(_mapping(payload.get("limit_counters"))),
        "safe_state_operator_explanation": str(payload.get("operator_explanation") or ""),
        "safe_state_recommended_next_step": str(payload.get("recommended_next_step") or ""),
    }


def _safe_state_evidence_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    return _safe_state_fields(payload)


def _missing_or_stale_evidence(evidence: Mapping[str, Any]) -> list[str]:
    missing: list[str] = []
    required = {
        "runtime_environment_truth": evidence["runtime_environment_truth_classification"],
        "runtime_resume_semantics": evidence["runtime_resume_classification"],
        "self_recover_rules": evidence["self_recover_recommendation"],
        "crash_loop_protection": evidence["crash_loop_classification"],
        "paper_recovery_policy": evidence["paper_action_policy"],
        "agent_health": evidence["agent_health_classification"],
        "agent_registry": evidence["agent_registry_classification"],
        "proof_readiness": evidence["proof_readiness_classification"],
        "position_truth": evidence["position_truth_classification"],
        "open_order_truth": evidence["open_order_truth_classification"],
        "managed_order_registry": evidence["managed_order_registry_classification"],
        "managed_position_registry": evidence["managed_position_registry_classification"],
        "reconciliation": evidence["reconciliation_classification"],
        "broker_lease": evidence["broker_lease_classification"],
    }
    for name, classification in required.items():
        if not classification:
            missing.append(name)
            continue
        text = str(classification)
        if text in {"ORDER_TRUTH_STALE", "STALE_MANAGED_POSITION_EVIDENCE", "AGENT_HEALTH_UNKNOWN"}:
            missing.append(f"{name}:{text}")
    return missing


def _shared_truth_coherence(inputs: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    shared_truth = inputs["shared_truth"]
    refresh_generation_id = str(shared_truth.get("refresh_generation_id") or "")
    generated_at = str(shared_truth.get("generated_at") or "")
    services = _list(shared_truth.get("services"))
    if not shared_truth or not refresh_generation_id or not generated_at or not services:
        return {
            "status": SHARED_TRUTH_MISSING,
            "refresh_generation_id": refresh_generation_id or None,
            "generated_at": generated_at or None,
            "stale_or_mixed_sources": [
                {
                    "service": "Shared Truth Refresh",
                    "reason": "missing_refresh_generation_contract",
                    "expected": "refresh_generation_id/generated_at/services",
                    "observed": "missing",
                }
            ],
            "source_services": [],
        }

    rows = {str(_mapping(row).get("service") or ""): _mapping(row) for row in services}
    stale_or_mixed: list[dict[str, Any]] = []
    for service, input_key, observed_classification, observed_generated_at in _coherence_sources(inputs):
        row = rows.get(service)
        observed_payload = _mapping(inputs.get(input_key))
        if not row:
            stale_or_mixed.append(
                {
                    "service": service,
                    "reason": "missing_from_refresh_generation",
                    "expected": "service row in Shared Truth Refresh",
                    "observed": "missing",
                }
            )
            continue
        expected_classification = str(row.get("classification") or "")
        if expected_classification != str(observed_classification or ""):
            stale_or_mixed.append(
                {
                    "service": service,
                    "source": input_key,
                    "reason": "classification_mismatch",
                    "expected": expected_classification,
                    "observed": observed_classification,
                }
            )
        expected_generation_id = str(row.get("authority_generation_id") or "")
        observed_generation_id = str(observed_payload.get("authority_generation_id") or "")
        if expected_generation_id and observed_generation_id:
            if expected_generation_id != observed_generation_id:
                stale_or_mixed.append(
                    {
                        "service": service,
                        "source": input_key,
                        "reason": "authority_generation_id_mismatch",
                        "expected": expected_generation_id,
                        "observed": observed_generation_id,
                    }
                )
            continue
        expected_generated_at = str(row.get("generated_at") or "")
        if expected_generated_at and observed_generated_at and expected_generated_at != str(observed_generated_at):
            if _newer_clean_reconciliation_supersedes_expected(
                service=service,
                expected_generated_at=expected_generated_at,
                observed_generated_at=str(observed_generated_at),
                observed_payload=observed_payload,
            ):
                continue
            stale_or_mixed.append(
                {
                    "service": service,
                    "source": input_key,
                    "reason": "generated_at_mismatch",
                    "expected": expected_generated_at,
                    "observed": observed_generated_at,
                }
            )
    return {
        "status": SHARED_TRUTH_STALE_OR_MIXED if stale_or_mixed else SHARED_TRUTH_COHERENT,
        "refresh_generation_id": refresh_generation_id,
        "generated_at": generated_at,
        "stale_or_mixed_sources": stale_or_mixed,
        "source_classifications": _mapping(shared_truth.get("classifications")),
        "source_services": [
            {
                "service": str(row.get("service") or ""),
                "classification": row.get("classification"),
                "generated_at": row.get("generated_at"),
                "authority_generation_id": row.get("authority_generation_id"),
                "authority_cycle_generated_at": row.get("authority_cycle_generated_at"),
                "source_generation_references": _mapping(row.get("source_generation_references")),
                "artifact_path": row.get("artifact_path"),
            }
            for row in rows.values()
        ],
    }


def _newer_clean_reconciliation_supersedes_expected(
    *,
    service: str,
    expected_generated_at: str,
    observed_generated_at: str,
    observed_payload: Mapping[str, Any],
) -> bool:
    if service != "Reconciliation":
        return False
    expected_at = _parse_datetime(expected_generated_at)
    observed_at = _parse_datetime(observed_generated_at)
    if expected_at is None or observed_at is None or observed_at <= expected_at:
        return False
    classification = _classification(observed_payload)
    if classification != "TRACK_B_PAPER_BROKER_RECONCILED":
        return False
    if observed_payload.get("broker_reconciled") is False:
        return False
    if observed_payload.get("lifecycle_broker_reconciled") is False:
        return False
    if _int_or_none(observed_payload.get("track_b_broker_position_count")) not in {None, 0}:
        return False
    if _int_or_none(observed_payload.get("track_b_broker_open_order_count")) not in {None, 0}:
        return False
    if _int_or_none(observed_payload.get("unknown_open_order_count")) not in {None, 0}:
        return False
    if _int_or_none(observed_payload.get("current_scope_lifecycle_open_position_count")) not in {None, 0}:
        return False
    if _int_or_none(observed_payload.get("lifecycle_open_order_count")) not in {None, 0}:
        return False
    if _int_or_none(observed_payload.get("review_required_count")) not in {None, 0}:
        return False
    if list(observed_payload.get("blockers") or []):
        return False
    return True


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coherence_sources(inputs: Mapping[str, Mapping[str, Any]]) -> list[tuple[str, str, str, str]]:
    return [
        (
            "Open Order Truth",
            "open_order_truth",
            _classification(inputs["open_order_truth"]),
            str(inputs["open_order_truth"].get("generated_at") or ""),
        ),
        (
            "Managed Order Registry",
            "managed_order_registry",
            _classification(inputs["managed_order_registry"]),
            str(inputs["managed_order_registry"].get("generated_at") or ""),
        ),
        (
            "Position Truth",
            "position_truth",
            _position_truth_classification(inputs["position_truth"]),
            str(inputs["position_truth"].get("generated_at") or ""),
        ),
        (
            "Runtime Environment Truth",
            "runtime_environment_truth",
            _classification(inputs["runtime_environment_truth"]),
            str(inputs["runtime_environment_truth"].get("generated_at") or ""),
        ),
        (
            "Managed Position Registry",
            "managed_position_registry",
            _classification(inputs["managed_position_registry"]),
            str(inputs["managed_position_registry"].get("generated_at") or ""),
        ),
        (
            "Reconciliation",
            "reconciliation",
            _classification(inputs["reconciliation"]),
            str(inputs["reconciliation"].get("generated_at") or ""),
        ),
        (
            "Broker Truth Lease",
            "broker_lease",
            _classification(inputs["broker_lease"]) or str(inputs["broker_lease"].get("lease_state") or ""),
            str(inputs["broker_lease"].get("generated_at") or ""),
        ),
        (
            "PAPER Recovery Policy",
            "paper_recovery_policy",
            str(inputs["paper_recovery_policy"].get("paper_action_policy") or _classification(inputs["paper_recovery_policy"])),
            str(inputs["paper_recovery_policy"].get("generated_at") or ""),
        ),
        (
            "PAPER Autonomous Recovery Planner",
            "paper_autonomous_recovery_plan",
            _classification(inputs["paper_autonomous_recovery_plan"]),
            str(inputs["paper_autonomous_recovery_plan"].get("generated_at") or ""),
        ),
    ]


def _market_closed(inputs: Mapping[str, Mapping[str, Any]]) -> bool:
    proof = inputs["proof_readiness"]
    if _proof_readiness_reports_open_session(proof):
        return False
    resume = inputs["runtime_resume_semantics"]
    self_recover = inputs["self_recover_rules"]
    if proof.get("classification") == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    if proof.get("phase1_session_reason") == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    phase1 = inputs["phase1_readiness"]
    if _phase1_session_reason(phase1, proof) == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    if resume.get("classification") == RESUME_BLOCKED_MARKET_CLOSED or resume.get("reason") == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    if self_recover.get("recommendation") == "WAIT_MARKET_CLOSED":
        return True
    return False


def _proof_readiness_reports_open_session(proof: Mapping[str, Any]) -> bool:
    session = _mapping(proof.get("phase1_market_session"))
    if session.get("market_closed") is False:
        return True
    reason = str(session.get("reason") or proof.get("phase1_session_reason") or "")
    return reason in {"GLOBEX_SESSION_OPEN", "MARKET_OPEN_EXPECT_FRESH_BARS"}


def _input_artifacts(config: TrackBRuntimeSupervisorAuthorityConfig) -> dict[str, str]:
    return {
        "runtime_environment_truth": str(config.resolve(config.runtime_environment_truth_path)),
        "runtime_resume_semantics": str(config.resolve(config.runtime_resume_semantics_path)),
        "self_recover_rules": str(config.resolve(config.self_recover_rules_path)),
        "crash_loop_protection": str(config.resolve(config.crash_loop_protection_path)),
        "agent_health": str(config.resolve(config.agent_health_path)),
        "agent_registry": str(config.resolve(config.agent_registry_path)),
        "proof_readiness": str(config.resolve(config.proof_readiness_path)),
        "shared_truth": str(config.resolve(config.shared_truth_path)),
        "canonical_readiness": str(config.resolve(config.canonical_readiness_path)),
        "position_truth": str(config.resolve(config.position_truth_path)),
        "open_order_truth": str(config.resolve(config.open_order_truth_path)),
        "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
        "managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
        "order_adjustment_plan": str(config.resolve(config.order_adjustment_plan_path)),
        "reconciliation": str(config.resolve(config.reconciliation_path)),
        "broker_lease": str(config.resolve(config.broker_lease_path)),
        "phase1_readiness": str(config.resolve(config.phase1_readiness_path)),
        "stop_provenance": str(config.resolve(config.stop_provenance_path)),
        "paper_recovery_policy": str(config.resolve(config.paper_recovery_policy_path)),
        "paper_autonomous_recovery_plan": str(config.resolve(config.paper_autonomous_recovery_plan_path)),
        "runtime_safe_state_envelope": str(config.resolve(config.runtime_safe_state_envelope_path)),
    }


def _paper_policy_bounded_retry(evidence: Mapping[str, Any]) -> bool:
    return (
        evidence["paper_action_policy"] == PAPER_POLICY_AUTONOMOUS_RETRY_ELIGIBLE
        and evidence["paper_autonomous_recovery_allowed"] is True
        and evidence["paper_requires_operator_ack"] is False
    )


def _paper_policy_quarantine(evidence: Mapping[str, Any]) -> bool:
    return evidence["paper_action_policy"] in {PAPER_POLICY_QUARANTINE_OBSERVE_ONLY, PAPER_POLICY_OBSERVE} and evidence[
        "paper_requires_operator_ack"
    ] is False


def _clean_start_facts(evidence: Mapping[str, Any]) -> bool:
    return (
        evidence["proof_readiness_classification"] == READY_FOR_PROOF
        and _clean_diagnostic_runtime_start_facts(evidence)
    )


def _clean_diagnostic_runtime_start_facts(evidence: Mapping[str, Any]) -> bool:
    return (
        evidence["position_truth_classification"] == "CLEAN_FLAT_READY"
        and evidence["open_order_truth_classification"] == NO_OPEN_ORDERS
        and evidence["managed_order_registry_classification"] == NO_MANAGED_ORDERS
        and evidence["managed_position_registry_classification"] == NO_MANAGED_POSITIONS
        and evidence["reconciliation_classification"] in {"TRACK_B_PAPER_BROKER_RECONCILED", ""}
    )


def _managed_active_hold_pending(evidence: Mapping[str, Any]) -> bool:
    return (
        evidence["open_order_truth_classification"] == BROKER_POSITION_WITHOUT_CLOSE_ORDER
        and evidence["managed_order_registry_classification"] == ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING
        and evidence["position_truth_classification"] == ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING
        and evidence["managed_position_registry_classification"] == "OPEN_MANAGED_MATCHED"
        and evidence["reconciliation_classification"] in {"TRACK_B_PAPER_BROKER_RECONCILED", ""}
        and evidence["live_money_eligible"] is False
        and evidence["paper_proof_invoked"] is False
    )


def _owned_managed_exposure_restart_allowed(evidence: Mapping[str, Any]) -> bool:
    return (
        evidence.get("restart_with_owned_exposure_allowed") is True
        and evidence["open_order_truth_classification"] in {NO_OPEN_ORDERS, BROKER_POSITION_WITHOUT_CLOSE_ORDER}
        and evidence["managed_order_registry_classification"] == "POSITION_WITHOUT_CLOSE_ORDER"
        and evidence["position_truth_classification"] == "ATTENTION_REQUIRED"
        and evidence["managed_position_registry_classification"] == "OPEN_MANAGED_EXIT_DUE"
        and evidence["reconciliation_classification"] in {"TRACK_B_PAPER_BROKER_RECONCILED", ""}
        and evidence["live_money_eligible"] is False
        and evidence["paper_proof_invoked"] is False
    )


def _duplicate_writer(evidence: Mapping[str, Any]) -> bool:
    if evidence["runtime_environment_truth_classification"] == "DUPLICATE_RUNTIME_WRITERS":
        return True
    if evidence["runtime_writer_authority"] == "DUPLICATE_WRITER":
        return True
    return evidence["duplicate_writer_count"] > 0


def _paper_policy_evidence(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "severity": payload.get("severity"),
        "paper_action_policy": payload.get("paper_action_policy"),
        "live_action_policy": payload.get("live_action_policy"),
        "autonomous_recovery_allowed": payload.get("autonomous_recovery_allowed"),
        "requires_operator_ack_for_paper": payload.get("requires_operator_ack_for_paper"),
        "bounded_recovery_budget": _mapping(payload.get("bounded_recovery_budget")),
    }


def _autonomous_recovery_plan_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "classification": _classification(payload),
        "next_action": _autonomous_recovery_next_action(payload),
        "execution_enabled": payload.get("execution_enabled") is True,
        "blockers": list(payload.get("blockers") or []),
        "budget_summary": _autonomous_recovery_budget_summary(payload),
    }


def _autonomous_recovery_next_action(payload: Mapping[str, Any]) -> str | None:
    for action in list(payload.get("proposed_actions") or []):
        if isinstance(action, Mapping):
            return str(action.get("action_type") or action.get("action_id") or "") or None
    for action in list(payload.get("blocked_actions") or []):
        if isinstance(action, Mapping):
            return str(action.get("action_type") or action.get("action_id") or "") or None
    return None


def _autonomous_recovery_budget_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    evidence = _mapping(payload.get("evidence_summary"))
    budget = _mapping(evidence.get("bounded_recovery_budget"))
    return {
        "budget_exhausted": budget.get("budget_exhausted"),
        "max_attempts_per_target": budget.get("max_attempts_per_target"),
        "max_attempts_per_window": budget.get("max_attempts_per_window"),
        "cooldown_seconds": budget.get("cooldown_seconds"),
    }


def _stop_provenance(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    nested = _mapping(payload.get("stop_provenance"))
    return nested or payload


def _position_truth_classification(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return str(payload.get("classification") or summary.get("overall_classification") or "")


def _phase1_session_reason(phase1_readiness: Mapping[str, Any], proof_readiness: Mapping[str, Any]) -> str:
    market_session = _mapping(phase1_readiness.get("market_session"))
    if market_session.get("classification"):
        return str(market_session.get("classification"))
    for row in _list(phase1_readiness.get("rows")):
        for check in _mapping(row.get("candle_checks")).values():
            if isinstance(check, Mapping) and check.get("reason"):
                return str(check.get("reason"))
    return str(proof_readiness.get("phase1_session_reason") or "")


def _blocker(code: str, detail: str) -> dict[str, str]:
    return {"code": code, "detail": detail}


def _decision_id(value: datetime) -> str:
    return f"track-b-paper-supervisor-{value.strftime('%Y%m%dT%H%M%SZ')}"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(path, payload)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _classification(payload: Mapping[str, Any]) -> str:
    return str(payload.get("classification") or "")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _any_true(*payloads: Mapping[str, Any], key: str) -> bool:
    return any(payload.get(key) is True for payload in payloads)


if __name__ == "__main__":
    raise SystemExit(main())
