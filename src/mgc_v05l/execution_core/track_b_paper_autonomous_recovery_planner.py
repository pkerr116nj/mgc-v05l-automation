"""Dry-run Track B PAPER autonomous recovery planner.

This planner is the design boundary between advisory recovery policy and any
future recovery executor. It consumes execution_core authority artifacts only
and writes a dry-run plan describing what a future PAPER executor would do
within bounded recovery budgets. It never starts, stops, restarts, submits,
cancels, replaces, modifies, closes, flattens, or mutates broker/lifecycle
state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_agent_health import DEFAULT_AGENT_HEALTH_ARTIFACT
from mgc_v05l.execution_core.track_b_broker_truth_lease import DEFAULT_LEASE_ARTIFACT
from mgc_v05l.execution_core.track_b_control_plane_snapshot import DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
from mgc_v05l.execution_core.track_b_crash_loop_protection import DEFAULT_CRASH_LOOP_PROTECTION_ARTIFACT
from mgc_v05l.execution_core.track_b_managed_order_registry import DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_managed_position_registry import DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_open_order_truth import DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT, NO_OPEN_ORDERS
from mgc_v05l.execution_core.track_b_order_adjustment_planner import DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT
from mgc_v05l.execution_core.track_b_paper_proof_readiness import DEFAULT_OUTPUT_PATH as DEFAULT_PROOF_READINESS_ARTIFACT
from mgc_v05l.execution_core.track_b_paper_proof_readiness import READY_FOR_PROOF
from mgc_v05l.execution_core.track_b_paper_recovery_policy import DEFAULT_PAPER_RECOVERY_POLICY_ARTIFACT
from mgc_v05l.execution_core.track_b_position_truth_monitor import DEFAULT_POSITION_TRUTH_ARTIFACT
from mgc_v05l.execution_core.track_b_runtime_resume_semantics import DEFAULT_RUNTIME_RESUME_SEMANTICS_ARTIFACT
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT,
    SUPERVISOR_RUNTIME_START_ALLOWED,
    SUPERVISOR_WAIT_MARKET_CLOSED,
)
from mgc_v05l.execution_core.track_b_self_recover_rules import DEFAULT_SELF_RECOVER_RULES_ARTIFACT
from mgc_v05l.execution_core.track_b_shared_truth_refresh_cli import DEFAULT_RECONCILIATION_ARTIFACT
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


NO_ACTION_NEEDED = "NO_ACTION_NEEDED"
WAIT_MARKET_CLOSED = "WAIT_MARKET_CLOSED"
PLAN_RUNTIME_RETRY = "PLAN_RUNTIME_RETRY"
PLAN_EVIDENCE_REFRESH = "PLAN_EVIDENCE_REFRESH"
PLAN_MARKET_DATA_RESTART = "PLAN_MARKET_DATA_RESTART"
PLAN_SCOPED_POSITION_CLEANUP = "PLAN_SCOPED_POSITION_CLEANUP"
PLAN_MANAGED_ORDER_MODIFY = "PLAN_MANAGED_ORDER_MODIFY"
PLAN_TARGETED_CANCEL_REPLACE = "PLAN_TARGETED_CANCEL_REPLACE"
PLAN_QUARANTINE_OBSERVE_ONLY = "PLAN_QUARANTINE_OBSERVE_ONLY"
PLAN_HARD_UNSAFE_HOLD = "PLAN_HARD_UNSAFE_HOLD"
PLAN_BLOCKED_BUDGET_EXHAUSTED = "PLAN_BLOCKED_BUDGET_EXHAUSTED"
PLAN_BLOCKED_STALE_EVIDENCE = "PLAN_BLOCKED_STALE_EVIDENCE"
PLAN_BLOCKED_IDENTITY_AMBIGUITY = "PLAN_BLOCKED_IDENTITY_AMBIGUITY"

PAPER_POLICY_OBSERVE = "OBSERVE"
PAPER_POLICY_REFRESH_EVIDENCE = "REFRESH_EVIDENCE"
PAPER_POLICY_AUTONOMOUS_RETRY_ELIGIBLE = "AUTONOMOUS_RETRY_ELIGIBLE"
PAPER_POLICY_SCOPED_RECOVERY_ELIGIBLE = "SCOPED_RECOVERY_ELIGIBLE"
PAPER_POLICY_QUARANTINE_OBSERVE_ONLY = "QUARANTINE_OBSERVE_ONLY"
PAPER_POLICY_HARD_UNSAFE_HOLD = "HARD_UNSAFE_HOLD"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_autonomous_recovery"
    / "latest_paper_autonomous_recovery_plan.json"
)
DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json"
)
DEFAULT_LIFECYCLE_STATE_MATRIX_DOC = Path("docs") / "track_b_lifecycle_state_matrix.md"


@dataclass(frozen=True)
class TrackBPaperAutonomousRecoveryPlannerConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT
    paper_recovery_policy_path: Path = DEFAULT_PAPER_RECOVERY_POLICY_ARTIFACT
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    control_plane_snapshot_max_age_seconds: int = 300
    runtime_supervisor_authority_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT
    runtime_resume_semantics_path: Path = DEFAULT_RUNTIME_RESUME_SEMANTICS_ARTIFACT
    self_recover_rules_path: Path = DEFAULT_SELF_RECOVER_RULES_ARTIFACT
    crash_loop_protection_path: Path = DEFAULT_CRASH_LOOP_PROTECTION_ARTIFACT
    agent_health_path: Path = DEFAULT_AGENT_HEALTH_ARTIFACT
    proof_readiness_path: Path = DEFAULT_PROOF_READINESS_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    order_adjustment_plan_path: Path = DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    broker_lease_path: Path = DEFAULT_LEASE_ARTIFACT
    runtime_environment_truth_path: Path = DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT
    lifecycle_state_matrix_path: Path = DEFAULT_LIFECYCLE_STATE_MATRIX_DOC

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_paper_autonomous_recovery_plan(
    *,
    config: TrackBPaperAutonomousRecoveryPlannerConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    inputs = {
        "paper_recovery_policy": _read_json(config.resolve(config.paper_recovery_policy_path)),
        "control_plane_snapshot": _read_json(config.resolve(config.control_plane_snapshot_path)),
        "runtime_supervisor_authority": _read_json(config.resolve(config.runtime_supervisor_authority_path)),
        "runtime_resume_semantics": _read_json(config.resolve(config.runtime_resume_semantics_path)),
        "self_recover_rules": _read_json(config.resolve(config.self_recover_rules_path)),
        "crash_loop_protection": _read_json(config.resolve(config.crash_loop_protection_path)),
        "agent_health": _read_json(config.resolve(config.agent_health_path)),
        "proof_readiness": _read_json(config.resolve(config.proof_readiness_path)),
        "open_order_truth": _read_json(config.resolve(config.open_order_truth_path)),
        "managed_order_registry": _read_json(config.resolve(config.managed_order_registry_path)),
        "order_adjustment_plan": _read_json(config.resolve(config.order_adjustment_plan_path)),
        "position_truth": _read_json(config.resolve(config.position_truth_path)),
        "managed_position_registry": _read_json(config.resolve(config.managed_position_registry_path)),
        "reconciliation": _read_json(config.resolve(config.reconciliation_path)),
        "broker_lease": _read_json(config.resolve(config.broker_lease_path)),
        "runtime_environment_truth": _read_json(config.resolve(config.runtime_environment_truth_path)),
    }
    evidence = _evidence(inputs=inputs, now=actual_now, config=config)
    decision = _classify_plan(inputs=inputs, evidence=evidence)
    return {
        "schema_version": "track_b_paper_autonomous_recovery_plan_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "dry_run": True,
        "planning_only": True,
        "read_only": True,
        "execution_enabled": False,
        "broker_mutation_allowed": False,
        "lifecycle_mutation_allowed": False,
        "runtime_restart_allowed": False,
        "submit_authority": False,
        "cancel_authority": False,
        "replace_authority": False,
        "modify_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "control_plane_snapshot_id": evidence["control_plane_snapshot_id"],
        "shared_truth_refresh_generation_id": evidence["shared_truth_refresh_generation_id"],
        "snapshot_coherence_status": evidence["snapshot_coherence_status"],
        "supervisor_decision_id": evidence["supervisor_decision_id"],
        "supervisor_classification": evidence["supervisor_classification"],
        "classification": decision["classification"],
        "reason": decision["reason"],
        "proposed_actions": decision["proposed_actions"],
        "blocked_actions": decision["blocked_actions"],
        "blockers": decision["blockers"],
        "warnings": decision["warnings"],
        "evidence_summary": evidence,
        "input_artifacts": _input_artifacts(config),
        "artifact_paths": {"authority": str(config.resolve(config.output_path))},
        "prohibited_actions": [
            "live_money_route",
            "broad_cancel",
            "broad_flatten",
            "hidden_recovery",
            "duplicate_runtime_writers",
            "dashboard_projection_authority",
            "paper_proof_bypass",
            "broker_mutation_without_exact_identity",
            "runtime_restart_without_budget",
        ],
        "todo_executor_v2": [
            "Keep this planner dry-run-only until a separate executor boundary is explicitly approved.",
            "Attach persisted per-target recovery budgets before enabling any autonomous action.",
            "Require a coherent Control Plane Snapshot captured immediately before every future executor action.",
            "Require exact identity revalidation immediately before every future mutation.",
        ],
    }


def write_track_b_paper_autonomous_recovery_plan(
    *,
    config: TrackBPaperAutonomousRecoveryPlannerConfig,
    payload: Mapping[str, Any],
) -> Path:
    output_path = config.resolve(config.output_path)
    _write_json_atomic(output_path, dict(payload))
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write dry-run Track B PAPER autonomous recovery plan.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBPaperAutonomousRecoveryPlannerConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
    )
    payload = build_track_b_paper_autonomous_recovery_plan(config=config)
    authority_path = write_track_b_paper_autonomous_recovery_plan(config=config, payload=payload)
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"classification={payload.get('classification')}")
        print(f"reason={payload.get('reason')}")
        print(f"artifact_path={authority_path}")
    return 2 if payload.get("classification") in {PLAN_HARD_UNSAFE_HOLD, PLAN_BLOCKED_IDENTITY_AMBIGUITY} else 0


def _classify_plan(
    *,
    inputs: Mapping[str, Mapping[str, Any]],
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    if evidence["live_money_eligible"] is True:
        return _decision(
            classification=PLAN_HARD_UNSAFE_HOLD,
            reason="Live-money eligibility is true; PAPER autonomous recovery is hard-held.",
            blockers=[_blocker("live_money_eligible", "true")],
            blocked_actions=[_action("hard_unsafe_hold", "HOLD", reason="live money route is prohibited")],
        )
    if _duplicate_writer(evidence):
        return _decision(
            classification=PLAN_HARD_UNSAFE_HOLD,
            reason="Duplicate runtime writer evidence is present.",
            blockers=[_blocker("runtime_environment_truth", evidence["runtime_environment_truth_classification"])],
            blocked_actions=[_action("hard_unsafe_hold", "HOLD", reason="single runtime writer invariant violated")],
        )
    if _market_closed(evidence):
        return _decision(
            classification=WAIT_MARKET_CLOSED,
            reason=MARKET_CLOSED_NO_FRESH_BARS,
            proposed_actions=[
                _action(
                    "wait_market_closed",
                    "WAIT_MARKET_CLOSED",
                    reason="Market/session is closed; no fresh Phase-1 bars are expected.",
                )
            ],
            warnings=[_blocker("market_session", MARKET_CLOSED_NO_FRESH_BARS)],
        )
    if _budget_exhausted(evidence):
        return _decision(
            classification=PLAN_BLOCKED_BUDGET_EXHAUSTED,
            reason="PAPER autonomous recovery budget is exhausted; quarantine and preserve evidence.",
            blockers=[_blocker("bounded_recovery_budget", "budget_exhausted")],
            blocked_actions=[_action("budget_exhausted", "RUNTIME_RETRY", reason="bounded recovery budget exhausted")],
        )
    if _stale_or_missing(evidence):
        return _decision(
            classification=PLAN_BLOCKED_STALE_EVIDENCE,
            reason="Authority evidence is stale or missing; refresh evidence before planning autonomous recovery.",
            blockers=[_blocker("stale_or_missing_evidence", item) for item in evidence["stale_or_missing_evidence"]],
            proposed_actions=[
                _action(
                    "refresh_evidence",
                    "REFRESH_EVIDENCE",
                    reason="Fresh execution_core authority artifacts are required before recovery planning.",
                )
            ],
        )
    if _market_data_restart_candidate(inputs=inputs, evidence=evidence):
        return _decision(
            classification=PLAN_MARKET_DATA_RESTART,
            reason="Phase-1 market-data producer appears unhealthy during an open-market proof window.",
            proposed_actions=[
                _action(
                    "market_data_restart",
                    "MARKET_DATA_RESTART",
                    reason="Future executor may restart only the Phase-1 market-data producer after rechecking evidence.",
                    target_identity=_phase1_target(inputs["agent_health"]),
                )
            ],
            warnings=[_blocker("agent_health", evidence["agent_health_classification"])],
        )
    if _suspicious_or_ambiguous_order(evidence):
        classification = (
            PLAN_BLOCKED_IDENTITY_AMBIGUITY
            if evidence["managed_order_registry_classification"] in {"ORDER_STATE_UNKNOWN_REVIEW_REQUIRED", "DUPLICATE_CLOSE_ORDER_BLOCKED"}
            else PLAN_QUARANTINE_OBSERVE_ONLY
        )
        return _decision(
            classification=classification,
            reason="Suspicious, duplicate, or identity-ambiguous order state is present; preserve evidence and observe only.",
            blockers=[
                _blocker("open_order_truth", evidence["open_order_truth_classification"]),
                _blocker("managed_order_registry", evidence["managed_order_registry_classification"]),
                _blocker("order_adjustment_plan", evidence["order_adjustment_plan_classification"]),
            ],
            proposed_actions=[
                _action(
                    "quarantine_observe_order_state",
                    "QUARANTINE_OBSERVE_ONLY",
                    reason="Do not mutate suspicious PAPER order state from this dry-run planner.",
                )
            ],
        )
    if evidence["order_adjustment_plan_classification"] == "MODIFY_IN_PLACE_ELIGIBLE":
        return _decision(
            classification=PLAN_MANAGED_ORDER_MODIFY,
            reason="A working managed close order is eligible for future modify-in-place planning.",
            proposed_actions=[
                _action(
                    "managed_order_modify",
                    "MANAGED_ORDER_MODIFY",
                    target_identity=_first_order_identity(inputs["order_adjustment_plan"], inputs["managed_order_registry"]),
                    reason="Future executor would require exact order identity and explicit operator authorization.",
                    would_mutate_broker=True,
                )
            ],
        )
    if evidence["order_adjustment_plan_classification"] == "TARGETED_CANCEL_REPLACE_REQUIRED":
        return _decision(
            classification=PLAN_TARGETED_CANCEL_REPLACE,
            reason="A terminal old close order with an open position is eligible for future targeted cancel/replace planning.",
            proposed_actions=[
                _action(
                    "targeted_cancel_replace",
                    "TARGETED_CANCEL_REPLACE",
                    target_identity=_first_order_identity(inputs["order_adjustment_plan"], inputs["managed_order_registry"]),
                    reason="Future executor must confirm old order terminal before any replacement close.",
                    would_mutate_broker=True,
                )
            ],
        )
    if _exact_broker_exposure(evidence=evidence, inputs=inputs):
        return _decision(
            classification=PLAN_SCOPED_POSITION_CLEANUP,
            reason="Exact broker exposure target is known; future executor could plan scoped cleanup within budget.",
            proposed_actions=[
                _action(
                    "scoped_position_cleanup",
                    "SCOPED_POSITION_CLEANUP",
                    target_identity=_first_position_identity(inputs["position_truth"], inputs["managed_position_registry"]),
                    reason="Future cleanup must remain exact-target scoped and revalidate identity immediately before mutation.",
                    would_mutate_broker=True,
                    would_mutate_lifecycle=True,
                )
            ],
        )
    if _clean_retry_candidate(evidence):
        return _decision(
            classification=PLAN_RUNTIME_RETRY,
            reason="Shared truth is clean and PAPER policy allows bounded autonomous runtime retry.",
            proposed_actions=[
                _action(
                    "runtime_retry",
                    "RUNTIME_RETRY",
                    reason="Future executor may start one PAPER runtime generation after final shared-truth refresh.",
                    would_restart_runtime=True,
                )
            ],
        )
    if evidence["paper_action_policy"] == PAPER_POLICY_REFRESH_EVIDENCE:
        return _decision(
            classification=PLAN_EVIDENCE_REFRESH,
            reason="PAPER policy requests evidence refresh before recovery.",
            proposed_actions=[
                _action(
                    "refresh_evidence",
                    "REFRESH_EVIDENCE",
                    reason="Refresh shared truth, proof readiness, and supervisor evidence.",
                )
            ],
        )
    if evidence["paper_action_policy"] == PAPER_POLICY_QUARANTINE_OBSERVE_ONLY:
        return _decision(
            classification=PLAN_QUARANTINE_OBSERVE_ONLY,
            reason="PAPER policy is quarantine/observe only.",
            proposed_actions=[
                _action(
                    "quarantine_observe",
                    "QUARANTINE_OBSERVE_ONLY",
                    reason="Preserve evidence and continue read-only observation.",
                )
            ],
        )
    return _decision(
        classification=NO_ACTION_NEEDED,
        reason="No autonomous recovery action is currently planned.",
        proposed_actions=[
            _action("no_action_needed", "NO_ACTION_NEEDED", reason="Current authority evidence does not require recovery.")
        ],
    )


def _decision(
    *,
    classification: str,
    reason: str,
    proposed_actions: list[dict[str, Any]] | None = None,
    blocked_actions: list[dict[str, Any]] | None = None,
    blockers: list[dict[str, str]] | None = None,
    warnings: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    return {
        "classification": classification,
        "reason": reason,
        "proposed_actions": proposed_actions or [],
        "blocked_actions": blocked_actions or [],
        "blockers": blockers or [],
        "warnings": warnings or [],
    }


def _action(
    action_id: str,
    action_type: str,
    *,
    reason: str,
    target_identity: Mapping[str, Any] | None = None,
    would_mutate_broker: bool = False,
    would_mutate_lifecycle: bool = False,
    would_restart_runtime: bool = False,
) -> dict[str, Any]:
    return {
        "action_id": action_id,
        "action_type": action_type,
        "target_identity": dict(target_identity or {}),
        "reason": reason,
        "required_preconditions": [
            "fresh_execution_core_authority_evidence",
            "coherent_control_plane_snapshot_captured_immediately_before_action",
            "paper_mode_only",
            "live_money_eligible_false",
            "single_runtime_writer_invariant",
            "exact_identity_revalidated_immediately_before_execution",
        ],
        "prohibited_actions": [
            "live_money_route",
            "broad_cancel",
            "broad_flatten",
            "dashboard_projection_authority",
            "hidden_recovery",
        ],
        "budget_key": action_type.lower(),
        "remaining_budget": None,
        "would_mutate_broker": would_mutate_broker,
        "would_mutate_lifecycle": would_mutate_lifecycle,
        "would_restart_runtime": would_restart_runtime,
        "execution_enabled": False,
    }


def _evidence(
    *,
    inputs: Mapping[str, Mapping[str, Any]],
    now: datetime,
    config: TrackBPaperAutonomousRecoveryPlannerConfig,
) -> dict[str, Any]:
    policy = inputs["paper_recovery_policy"]
    snapshot = inputs["control_plane_snapshot"]
    supervisor = inputs["runtime_supervisor_authority"]
    resume = inputs["runtime_resume_semantics"]
    self_recover = inputs["self_recover_rules"]
    crash_loop = inputs["crash_loop_protection"]
    proof = inputs["proof_readiness"]
    open_order = inputs["open_order_truth"]
    managed_order = inputs["managed_order_registry"]
    order_plan = inputs["order_adjustment_plan"]
    position = inputs["position_truth"]
    managed_position = inputs["managed_position_registry"]
    reconciliation = inputs["reconciliation"]
    broker_lease = inputs["broker_lease"]
    runtime_truth = inputs["runtime_environment_truth"]
    budget = _mapping(policy.get("bounded_recovery_budget"))
    snapshot_evidence = _control_plane_snapshot_evidence(
        snapshot=snapshot,
        now=now,
        max_age_seconds=config.control_plane_snapshot_max_age_seconds,
    )
    return {
        **snapshot_evidence,
        "paper_recovery_policy_classification": _classification(policy) or str(policy.get("paper_action_policy") or ""),
        "paper_action_policy": str(policy.get("paper_action_policy") or ""),
        "paper_policy_severity": str(policy.get("severity") or ""),
        "autonomous_recovery_allowed": policy.get("autonomous_recovery_allowed") is True,
        "requires_operator_ack_for_paper": policy.get("requires_operator_ack_for_paper") is True,
        "live_action_policy": str(policy.get("live_action_policy") or ""),
        "budget_exhausted": budget.get("budget_exhausted") is True,
        "bounded_recovery_budget": dict(budget),
        "runtime_supervisor_classification": _classification(supervisor),
        "runtime_supervisor_mode": str(supervisor.get("supervisor_mode") or ""),
        "runtime_supervisor_safe_to_start": supervisor.get("safe_to_start_runtime") is True,
        "runtime_resume_classification": _classification(resume),
        "self_recover_recommendation": str(self_recover.get("recommendation") or _classification(self_recover)),
        "crash_loop_classification": _classification(crash_loop),
        "agent_health_classification": _classification(inputs["agent_health"]),
        "proof_readiness_classification": _classification(proof),
        "phase1_session_reason": str(
            proof.get("phase1_session_reason") or _mapping(proof.get("market_session")).get("classification") or ""
        ),
        "open_order_truth_classification": _classification(open_order),
        "managed_order_registry_classification": _classification(managed_order),
        "order_adjustment_plan_classification": _classification(order_plan),
        "position_truth_classification": _position_truth_classification(position),
        "managed_position_registry_classification": _classification(managed_position),
        "reconciliation_classification": _classification(reconciliation),
        "broker_lease_classification": _classification(broker_lease) or str(broker_lease.get("lease_state") or ""),
        "runtime_environment_truth_classification": _classification(runtime_truth),
        "runtime_writer_authority": str(runtime_truth.get("writer_authority") or ""),
        "duplicate_writer_count": int(runtime_truth.get("duplicate_writer_count") or 0),
        "live_money_eligible": _any_true(
            policy,
            snapshot,
            supervisor,
            resume,
            self_recover,
            crash_loop,
            proof,
            open_order,
            managed_order,
            position,
            managed_position,
            reconciliation,
            runtime_truth,
            key="live_money_eligible",
        ),
        "stale_or_missing_evidence": _stale_or_missing_evidence(inputs, snapshot_evidence=snapshot_evidence),
        "lifecycle_state_matrix": {
            "authority": "mgc_v05l.execution_core.track_b_lifecycle_state_transition",
            "doc": str(DEFAULT_LIFECYCLE_STATE_MATRIX_DOC),
        },
    }


def _market_closed(evidence: Mapping[str, Any]) -> bool:
    return (
        evidence["paper_action_policy"] == PAPER_POLICY_OBSERVE
        and (
            evidence["phase1_session_reason"] == MARKET_CLOSED_NO_FRESH_BARS
            or evidence["proof_readiness_classification"] == MARKET_CLOSED_NO_FRESH_BARS
            or evidence["runtime_supervisor_classification"] == SUPERVISOR_WAIT_MARKET_CLOSED
        )
    )


def _budget_exhausted(evidence: Mapping[str, Any]) -> bool:
    return evidence["budget_exhausted"] is True or evidence["crash_loop_classification"] in {
        "RESTART_COOLDOWN_ACTIVE",
        "REPEATED_RUNTIME_FAILURE",
        "REPEATED_BROKER_LEASE_FAILURE",
    }


def _stale_or_missing(evidence: Mapping[str, Any]) -> bool:
    return bool(evidence["stale_or_missing_evidence"]) or evidence["paper_action_policy"] == PAPER_POLICY_REFRESH_EVIDENCE


def _market_data_restart_candidate(*, inputs: Mapping[str, Mapping[str, Any]], evidence: Mapping[str, Any]) -> bool:
    if not _clean_shared_truth(evidence):
        return False
    if evidence["phase1_session_reason"] == MARKET_CLOSED_NO_FRESH_BARS:
        return False
    for agent in _list(inputs["agent_health"].get("agents")):
        if str(agent.get("agent_id") or "") != "phase1_databento_live_candles":
            continue
        status = str(agent.get("status") or "")
        reason = str(agent.get("reason") or "")
        return status in {"MISSING", "STALE", "STOPPED_UNEXPECTED", "DEGRADED"} and reason != MARKET_CLOSED_NO_FRESH_BARS
    return False


def _suspicious_or_ambiguous_order(evidence: Mapping[str, Any]) -> bool:
    return evidence["open_order_truth_classification"] in {
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


def _exact_broker_exposure(*, evidence: Mapping[str, Any], inputs: Mapping[str, Mapping[str, Any]]) -> bool:
    if evidence["paper_action_policy"] != PAPER_POLICY_SCOPED_RECOVERY_ELIGIBLE:
        return False
    return bool(_first_position_identity(inputs["position_truth"], inputs["managed_position_registry"]))


def _clean_retry_candidate(evidence: Mapping[str, Any]) -> bool:
    return (
        evidence["paper_action_policy"] == PAPER_POLICY_AUTONOMOUS_RETRY_ELIGIBLE
        and evidence["autonomous_recovery_allowed"] is True
        and evidence["proof_readiness_classification"] == READY_FOR_PROOF
        and evidence["runtime_supervisor_classification"] == SUPERVISOR_RUNTIME_START_ALLOWED
        and evidence["runtime_supervisor_safe_to_start"] is True
        and _clean_shared_truth(evidence)
    )


def _clean_shared_truth(evidence: Mapping[str, Any]) -> bool:
    return (
        evidence["position_truth_classification"] == "CLEAN_FLAT_READY"
        and evidence["open_order_truth_classification"] == NO_OPEN_ORDERS
        and evidence["managed_order_registry_classification"] in {"", "NO_MANAGED_ORDERS"}
        and evidence["managed_position_registry_classification"] in {"", "NO_MANAGED_POSITIONS"}
        and evidence["reconciliation_classification"] in {"", "TRACK_B_PAPER_BROKER_RECONCILED"}
    )


def _duplicate_writer(evidence: Mapping[str, Any]) -> bool:
    return evidence["runtime_environment_truth_classification"] == "DUPLICATE_RUNTIME_WRITERS" or int(
        evidence["duplicate_writer_count"] or 0
    ) > 0


def _first_position_identity(position_truth: Mapping[str, Any], managed_position_registry: Mapping[str, Any]) -> dict[str, Any]:
    for payload in (position_truth, managed_position_registry):
        for row in _list(payload.get("positions")) + _list(payload.get("managed_positions")):
            identity = {
                "symbol": row.get("symbol") or row.get("track_b_root"),
                "contract": row.get("contract") or row.get("local_symbol"),
                "con_id": row.get("con_id"),
                "quantity": row.get("quantity") or row.get("qty") or row.get("position"),
                "side": row.get("side"),
                "lifecycle_id": row.get("lifecycle_id"),
                "ownership_id": row.get("ownership_id"),
                "manifest_id": row.get("manifest_id"),
                "lane": row.get("lane") or row.get("lane_id"),
            }
            if identity.get("symbol") and identity.get("contract") and identity.get("quantity") not in {None, ""}:
                return identity
    return {}


def _first_order_identity(order_adjustment_plan: Mapping[str, Any], managed_order_registry: Mapping[str, Any]) -> dict[str, Any]:
    for row in _list(order_adjustment_plan.get("plans")) + _list(managed_order_registry.get("managed_orders")):
        identity = _mapping(row.get("identity"))
        if not identity:
            identity = {
                "account_id": row.get("account_id"),
                "symbol": row.get("symbol"),
                "contract": row.get("contract") or row.get("local_symbol"),
                "con_id": row.get("con_id"),
                "broker_order_id": row.get("broker_order_id"),
                "perm_id": row.get("perm_id"),
                "action": row.get("action"),
                "quantity": row.get("quantity"),
            }
        if identity.get("contract") or identity.get("broker_order_id") or identity.get("perm_id"):
            return dict(identity)
    return {}


def _phase1_target(agent_health: Mapping[str, Any]) -> dict[str, Any]:
    for agent in _list(agent_health.get("agents")):
        if str(agent.get("agent_id") or "") == "phase1_databento_live_candles":
            return {
                "agent_id": agent.get("agent_id"),
                "status": agent.get("status"),
                "reason": agent.get("reason"),
                "heartbeat_path": agent.get("heartbeat_path"),
                "primary_artifact_path": agent.get("primary_artifact_path"),
            }
    return {"agent_id": "phase1_databento_live_candles"}


def _control_plane_snapshot_evidence(
    *,
    snapshot: Mapping[str, Any],
    now: datetime,
    max_age_seconds: int,
) -> dict[str, Any]:
    generated_at = _parse_datetime(snapshot.get("generated_at"))
    age_seconds = None
    if generated_at is not None:
        age_seconds = max(0.0, (now - generated_at).total_seconds())

    stale_or_missing: list[str] = []
    if not snapshot:
        stale_or_missing.append("control_plane_snapshot_missing")
    if snapshot and not snapshot.get("control_plane_snapshot_id"):
        stale_or_missing.append("control_plane_snapshot_id_missing")
    if snapshot and snapshot.get("shared_truth_coherence_status") != "COHERENT":
        stale_or_missing.append("control_plane_snapshot_not_coherent")
    if snapshot and not snapshot.get("shared_truth_refresh_generation_id"):
        stale_or_missing.append("control_plane_snapshot_generation_missing")
    if snapshot and not snapshot.get("runtime_supervisor_decision_id"):
        stale_or_missing.append("control_plane_snapshot_supervisor_decision_missing")
    if snapshot and generated_at is None:
        stale_or_missing.append("control_plane_snapshot_generated_at_missing")
    if age_seconds is not None and age_seconds > max_age_seconds:
        stale_or_missing.append("control_plane_snapshot_stale")

    return {
        "control_plane_snapshot_id": str(snapshot.get("control_plane_snapshot_id") or ""),
        "control_plane_snapshot_generated_at": str(snapshot.get("generated_at") or ""),
        "control_plane_snapshot_age_seconds": age_seconds,
        "control_plane_snapshot_max_age_seconds": max_age_seconds,
        "shared_truth_refresh_generation_id": str(snapshot.get("shared_truth_refresh_generation_id") or ""),
        "snapshot_coherence_status": str(snapshot.get("shared_truth_coherence_status") or ""),
        "supervisor_decision_id": str(snapshot.get("runtime_supervisor_decision_id") or ""),
        "supervisor_classification": str(snapshot.get("runtime_supervisor_classification") or ""),
        "control_plane_snapshot_stale_or_missing": stale_or_missing,
        "executor_pre_action_evidence_packet_required": True,
        "executor_pre_action_evidence_packet_source": "control_plane_snapshot",
    }


def _stale_or_missing_evidence(
    inputs: Mapping[str, Mapping[str, Any]],
    *,
    snapshot_evidence: Mapping[str, Any],
) -> list[str]:
    required = {
        "paper_recovery_policy": "paper_action_policy",
        "runtime_supervisor_authority": "classification",
        "runtime_resume_semantics": "classification",
        "self_recover_rules": "recommendation",
        "crash_loop_protection": "classification",
        "agent_health": "classification",
        "proof_readiness": "classification",
        "open_order_truth": "classification",
        "managed_order_registry": "classification",
        "order_adjustment_plan": "classification",
        "position_truth": "classification",
        "managed_position_registry": "classification",
        "reconciliation": "classification",
        "broker_lease": "classification",
        "runtime_environment_truth": "classification",
    }
    missing: list[str] = [str(item) for item in _list(snapshot_evidence.get("control_plane_snapshot_stale_or_missing"))]
    for name, field in required.items():
        payload = inputs[name]
        if not payload:
            missing.append(name)
            continue
        if name == "position_truth" and _position_truth_classification(payload):
            continue
        if name == "broker_lease" and (payload.get("classification") or payload.get("lease_state")):
            continue
        if not payload.get(field):
            missing.append(name)
    return missing


def _input_artifacts(config: TrackBPaperAutonomousRecoveryPlannerConfig) -> dict[str, str]:
    return {
        "paper_recovery_policy": str(config.resolve(config.paper_recovery_policy_path)),
        "control_plane_snapshot": str(config.resolve(config.control_plane_snapshot_path)),
        "runtime_supervisor_authority": str(config.resolve(config.runtime_supervisor_authority_path)),
        "runtime_resume_semantics": str(config.resolve(config.runtime_resume_semantics_path)),
        "self_recover_rules": str(config.resolve(config.self_recover_rules_path)),
        "crash_loop_protection": str(config.resolve(config.crash_loop_protection_path)),
        "agent_health": str(config.resolve(config.agent_health_path)),
        "proof_readiness": str(config.resolve(config.proof_readiness_path)),
        "open_order_truth": str(config.resolve(config.open_order_truth_path)),
        "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
        "order_adjustment_plan": str(config.resolve(config.order_adjustment_plan_path)),
        "position_truth": str(config.resolve(config.position_truth_path)),
        "managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
        "reconciliation": str(config.resolve(config.reconciliation_path)),
        "broker_lease": str(config.resolve(config.broker_lease_path)),
        "runtime_environment_truth": str(config.resolve(config.runtime_environment_truth_path)),
        "lifecycle_state_matrix": str(config.resolve(config.lifecycle_state_matrix_path)),
    }


def _blocker(code: str, detail: str) -> dict[str, str]:
    return {"code": str(code), "detail": str(detail)}


def _classification(payload: Mapping[str, Any]) -> str:
    return str(payload.get("classification") or payload.get("overall_classification") or "")


def _position_truth_classification(payload: Mapping[str, Any]) -> str:
    return str(payload.get("classification") or _mapping(payload.get("summary")).get("overall_classification") or "")


def _any_true(*payloads: Mapping[str, Any], key: str) -> bool:
    return any(payload.get(key) is True for payload in payloads)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


if __name__ == "__main__":
    raise SystemExit(main())
