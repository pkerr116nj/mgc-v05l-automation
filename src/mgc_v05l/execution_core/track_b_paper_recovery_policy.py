"""Read-only Track B PAPER recovery policy authority.

PAPER Recovery Policy lives in execution_core. Dashboard artifacts are
projections and must not be used as runtime, readiness, restart, broker, order,
or lifecycle authority. This v1 service is advisory/policy only: it never
starts, stops, restarts, submits, cancels, replaces, closes, flattens, or
mutates broker/lifecycle state.

The policy deliberately separates PAPER recovery posture from future LIVE
recovery posture. PAPER is a bounded autonomous failure-discovery environment;
LIVE/PRE-LIVE can add stronger human gates later.
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
from mgc_v05l.execution_core.track_b_broker_truth_lease import DEFAULT_LEASE_ARTIFACT
from mgc_v05l.execution_core.track_b_crash_loop_protection import DEFAULT_CRASH_LOOP_PROTECTION_ARTIFACT
from mgc_v05l.execution_core.track_b_lifecycle_state_transition import (
    BLOCKED_NO_BROKER_EFFECT,
    BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE,
    CLOSED_FLAT,
    CLOSE_UNKNOWN,
    MANUAL_OR_MALFORMED_CLEANUP,
    OPEN_MANAGED,
    OPEN_MANAGED_METADATA_INCOMPLETE,
    REVIEW_REQUIRED,
)
from mgc_v05l.execution_core.track_b_managed_order_registry import DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_managed_position_registry import DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_open_order_truth import DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT, NO_OPEN_ORDERS
from mgc_v05l.execution_core.track_b_order_adjustment_planner import DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT
from mgc_v05l.execution_core.track_b_paper_proof_readiness import DEFAULT_OUTPUT_PATH as DEFAULT_PROOF_READINESS_ARTIFACT
from mgc_v05l.execution_core.track_b_paper_proof_readiness import READY_FOR_PROOF
from mgc_v05l.execution_core.track_b_position_truth_monitor import DEFAULT_POSITION_TRUTH_ARTIFACT
from mgc_v05l.execution_core.track_b_projection_metadata import build_projection_metadata
from mgc_v05l.execution_core.track_b_recovery_budget_ledger import DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT
from mgc_v05l.execution_core.track_b_runtime_resume_semantics import DEFAULT_RUNTIME_RESUME_SEMANTICS_ARTIFACT
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    DEFAULT_RUNTIME_STOP_PROVENANCE_ARTIFACT,
    DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT,
    SUPERVISOR_RUNTIME_START_ALLOWED,
    SUPERVISOR_WAIT_MARKET_CLOSED,
)
from mgc_v05l.execution_core.track_b_self_recover_rules import DEFAULT_SELF_RECOVER_RULES_ARTIFACT
from mgc_v05l.execution_core.track_b_shared_truth_refresh_cli import DEFAULT_RECONCILIATION_ARTIFACT
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


INFO = "INFO"
WARNING = "WARNING"
ATTENTION = "ATTENTION"
UNSAFE = "UNSAFE"

OBSERVE = "OBSERVE"
REFRESH_EVIDENCE = "REFRESH_EVIDENCE"
AUTONOMOUS_RETRY_ELIGIBLE = "AUTONOMOUS_RETRY_ELIGIBLE"
SCOPED_RECOVERY_ELIGIBLE = "SCOPED_RECOVERY_ELIGIBLE"
QUARANTINE_OBSERVE_ONLY = "QUARANTINE_OBSERVE_ONLY"
HARD_UNSAFE_HOLD = "HARD_UNSAFE_HOLD"

LIVE_OBSERVE = "OBSERVE"
LIVE_REQUIRE_ACK = "REQUIRE_ACK"
LIVE_HOLD_DOWN = "HOLD_DOWN"

DEFAULT_MAX_ATTEMPTS_PER_TARGET = 1
DEFAULT_MAX_ATTEMPTS_PER_WINDOW = 2
DEFAULT_COOLDOWN_SECONDS = 300

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PAPER_RECOVERY_POLICY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "paper_recovery_policy" / "latest_paper_recovery_policy.json"
)
DEFAULT_DASHBOARD_PAPER_RECOVERY_POLICY_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_paper_recovery_policy.json"
)
DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json"
)
DEFAULT_LIFECYCLE_STATE_MATRIX_DOC = Path("docs") / "track_b_lifecycle_state_matrix.md"


@dataclass(frozen=True)
class TrackBPaperRecoveryPolicyConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_PAPER_RECOVERY_POLICY_ARTIFACT
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_PAPER_RECOVERY_POLICY_PROJECTION
    runtime_supervisor_authority_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT
    self_recover_rules_path: Path = DEFAULT_SELF_RECOVER_RULES_ARTIFACT
    crash_loop_protection_path: Path = DEFAULT_CRASH_LOOP_PROTECTION_ARTIFACT
    runtime_resume_semantics_path: Path = DEFAULT_RUNTIME_RESUME_SEMANTICS_ARTIFACT
    agent_health_path: Path = DEFAULT_AGENT_HEALTH_ARTIFACT
    proof_readiness_path: Path = DEFAULT_PROOF_READINESS_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    order_adjustment_plan_path: Path = DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    broker_lease_path: Path = DEFAULT_LEASE_ARTIFACT
    stop_provenance_path: Path = DEFAULT_RUNTIME_STOP_PROVENANCE_ARTIFACT
    runtime_environment_truth_path: Path = DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT
    lifecycle_state_matrix_path: Path = DEFAULT_LIFECYCLE_STATE_MATRIX_DOC
    recovery_budget_ledger_path: Path = DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT
    max_attempts_per_target: int = DEFAULT_MAX_ATTEMPTS_PER_TARGET
    max_attempts_per_window: int = DEFAULT_MAX_ATTEMPTS_PER_WINDOW
    cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_paper_recovery_policy(
    *,
    config: TrackBPaperRecoveryPolicyConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    inputs = {
        "runtime_supervisor_authority": _read_json(config.resolve(config.runtime_supervisor_authority_path)),
        "self_recover_rules": _read_json(config.resolve(config.self_recover_rules_path)),
        "crash_loop_protection": _read_json(config.resolve(config.crash_loop_protection_path)),
        "runtime_resume_semantics": _read_json(config.resolve(config.runtime_resume_semantics_path)),
        "agent_health": _read_json(config.resolve(config.agent_health_path)),
        "proof_readiness": _read_json(config.resolve(config.proof_readiness_path)),
        "open_order_truth": _read_json(config.resolve(config.open_order_truth_path)),
        "managed_order_registry": _read_json(config.resolve(config.managed_order_registry_path)),
        "order_adjustment_plan": _read_json(config.resolve(config.order_adjustment_plan_path)),
        "position_truth": _read_json(config.resolve(config.position_truth_path)),
        "managed_position_registry": _read_json(config.resolve(config.managed_position_registry_path)),
        "reconciliation": _read_json(config.resolve(config.reconciliation_path)),
        "broker_lease": _read_json(config.resolve(config.broker_lease_path)),
        "stop_provenance": _read_json(config.resolve(config.stop_provenance_path)),
        "runtime_environment_truth": _read_json(config.resolve(config.runtime_environment_truth_path)),
        "recovery_budget_ledger": _read_json(config.resolve(config.recovery_budget_ledger_path)),
    }
    evidence = _evidence(inputs)
    decision = _classify_policy(config=config, evidence=evidence, inputs=inputs)
    return {
        "schema_version": "track_b_paper_recovery_policy_v1",
        "generated_at": actual_now.isoformat(),
        "policy_mode": "PAPER",
        "recovery_policy_id": _policy_id(actual_now),
        "read_only": True,
        "advisory_only": True,
        "submit_authority": False,
        "broker_mutation_allowed": False,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "runtime_restart_allowed": False,
        "runtime_restart_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "severity": decision["severity"],
        "paper_action_policy": decision["paper_action_policy"],
        "live_action_policy": decision["live_action_policy"],
        "autonomous_recovery_allowed": decision["autonomous_recovery_allowed"],
        "requires_operator_ack_for_paper": decision["requires_operator_ack_for_paper"],
        "reason": decision["reason"],
        "evidence_summary": evidence,
        "bounded_recovery_budget": {
            "max_attempts_per_target": decision["max_attempts_per_target"],
            "max_attempts_per_window": config.max_attempts_per_window,
            "cooldown_seconds": config.cooldown_seconds,
            "budget_exhausted": decision["budget_exhausted"],
            "ledger_classification": evidence["recovery_budget_classification"],
            "ledger_budget_exhausted": evidence["recovery_budget_exhausted"],
            "quarantine_required": evidence["recovery_budget_quarantine_required"],
            "attempts_remaining": evidence["recovery_budget_attempts_remaining"],
            "source_authority_path": evidence["recovery_budget_authority_path"],
        },
        "prohibited_actions": [
            "live_money_route",
            "broad_cancel",
            "broad_flatten",
            "duplicate_runtime_writers",
            "hidden_recovery",
            "stale_evidence_as_mutation_permission",
            "dashboard_projection_authority",
            "paper_proof_bypass",
            "pretending_suspicious_state_is_clean",
        ],
        "allowed_next_steps": decision["allowed_next_steps"],
        "blockers": decision["blockers"],
        "warnings": decision["warnings"],
        "input_artifacts": _input_artifacts(config),
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "dashboard_projection": None
            if config.dashboard_projection_path is None
            else str(config.resolve(config.dashboard_projection_path)),
        },
        "todo_v2": [
            "Have Runtime Supervisor Authority consume this PAPER policy as advisory evidence.",
            "Convert core PAPER supervisor/resume/crash-loop holds into bounded retry, quarantine-observe, or scoped recovery budgets where structurally safe.",
            "Keep any manual/human gate as an isolated temporary policy adapter, not a core PAPER recovery dependency.",
            "Keep LIVE/PRE-LIVE recovery policy separate and more conservative.",
        ],
    }


def write_track_b_paper_recovery_policy(
    *,
    config: TrackBPaperRecoveryPolicyConfig,
    payload: Mapping[str, Any],
) -> Path:
    authority_path = config.resolve(config.output_path)
    _write_json_atomic(authority_path, dict(payload))
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_paper_recovery_policy_projection(
                authority_payload=payload,
                authority_path=authority_path,
            ),
        )
    return authority_path


def build_dashboard_paper_recovery_policy_projection(
    *,
    authority_payload: Mapping[str, Any],
    authority_path: Path,
) -> dict[str, Any]:
    return {
        **dict(authority_payload),
        "schema_version": "track_b_paper_recovery_policy_dashboard_projection_v1",
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
    parser = argparse.ArgumentParser(description="Write read-only Track B PAPER recovery policy.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_PAPER_RECOVERY_POLICY_ARTIFACT)
    parser.add_argument("--dashboard-projection-path", type=Path, default=DEFAULT_DASHBOARD_PAPER_RECOVERY_POLICY_PROJECTION)
    parser.add_argument("--no-dashboard-projection", action="store_true")
    parser.add_argument("--max-attempts-per-target", type=int, default=DEFAULT_MAX_ATTEMPTS_PER_TARGET)
    parser.add_argument("--max-attempts-per-window", type=int, default=DEFAULT_MAX_ATTEMPTS_PER_WINDOW)
    parser.add_argument("--cooldown-seconds", type=int, default=DEFAULT_COOLDOWN_SECONDS)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBPaperRecoveryPolicyConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        dashboard_projection_path=None if bool(args.no_dashboard_projection) else Path(args.dashboard_projection_path),
        max_attempts_per_target=int(args.max_attempts_per_target),
        max_attempts_per_window=int(args.max_attempts_per_window),
        cooldown_seconds=int(args.cooldown_seconds),
    )
    payload = build_track_b_paper_recovery_policy(config=config)
    authority_path = write_track_b_paper_recovery_policy(config=config, payload=payload)
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            json.dumps(
                {
                    "severity": payload.get("severity"),
                    "paper_action_policy": payload.get("paper_action_policy"),
                    "autonomous_recovery_allowed": payload.get("autonomous_recovery_allowed"),
                    "requires_operator_ack_for_paper": payload.get("requires_operator_ack_for_paper"),
                    "reason": payload.get("reason"),
                    "authority_path": str(authority_path),
                    "read_only": True,
                    "broker_mutation_allowed": False,
                    "runtime_restart_allowed": False,
                    "paper_proof_invoked": False,
                    "live_money_eligible": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 2 if payload.get("paper_action_policy") == HARD_UNSAFE_HOLD else 0


def _classify_policy(
    *,
    config: TrackBPaperRecoveryPolicyConfig,
    evidence: Mapping[str, Any],
    inputs: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    if evidence["live_money_eligible"] is True:
        return _decision(
            severity=UNSAFE,
            paper_action_policy=HARD_UNSAFE_HOLD,
            live_action_policy=LIVE_HOLD_DOWN,
            reason="Live-money eligibility is true; PAPER recovery policy must hold.",
            blockers=[_blocker("live_money_eligible", "true")],
        )
    if _duplicate_runtime_writer(evidence=evidence, inputs=inputs):
        return _decision(
            severity=UNSAFE,
            paper_action_policy=HARD_UNSAFE_HOLD,
            live_action_policy=LIVE_HOLD_DOWN,
            reason="Duplicate runtime writer evidence is present.",
            blockers=[_blocker("runtime_environment_truth", evidence["runtime_environment_truth_classification"])],
        )
    if _market_closed(evidence=evidence, inputs=inputs):
        return _decision(
            severity=INFO,
            paper_action_policy=OBSERVE,
            live_action_policy=LIVE_OBSERVE,
            reason=MARKET_CLOSED_NO_FRESH_BARS,
            allowed_next_steps=["preserve artifacts", "wait for market reopen", "refresh proof readiness after reopen"],
            warnings=[_blocker("market_session", MARKET_CLOSED_NO_FRESH_BARS)],
        )
    if _stale_or_missing_authority(evidence):
        return _decision(
            severity=WARNING,
            paper_action_policy=REFRESH_EVIDENCE,
            live_action_policy=LIVE_REQUIRE_ACK,
            reason="Authority evidence is stale or missing; refresh shared truth before recovery planning.",
            allowed_next_steps=["run shared truth refresh", "rerun proof readiness", "rewrite recovery policy artifact"],
            warnings=[_blocker("stale_or_missing_evidence", item) for item in evidence["stale_or_missing_evidence"]],
        )
    if _suspicious_order(evidence):
        return _decision(
            severity=ATTENTION,
            paper_action_policy=QUARANTINE_OBSERVE_ONLY,
            live_action_policy=LIVE_REQUIRE_ACK,
            reason="Suspicious or duplicate order evidence exists; v1 permits observation/evidence refresh only.",
            blockers=[
                _blocker("open_order_truth", evidence["open_order_truth_classification"]),
                _blocker("managed_order_registry", evidence["managed_order_registry_classification"]),
                _blocker("order_adjustment_plan", evidence["order_adjustment_plan_classification"]),
            ],
            allowed_next_steps=["refresh broker/open-order evidence", "preserve suspicious order artifacts", "continue observation only"],
        )
    if _broker_exposure(evidence):
        if _exact_recovery_target_known(inputs=inputs):
            return _decision(
                severity=ATTENTION,
                paper_action_policy=SCOPED_RECOVERY_ELIGIBLE,
                live_action_policy=LIVE_REQUIRE_ACK,
                reason="Broker exposure exists, but exact target identity is available for bounded scoped recovery planning.",
                autonomous_recovery_allowed=True,
                blockers=[_blocker("position_truth", evidence["position_truth_classification"])],
                allowed_next_steps=[
                    "build scoped recovery plan for exact target",
                    "preserve pre/post shared-truth snapshots",
                    "do not use broad cancel or broad flatten",
                ],
            )
        return _decision(
            severity=ATTENTION,
            paper_action_policy=QUARANTINE_OBSERVE_ONLY,
            live_action_policy=LIVE_REQUIRE_ACK,
            reason="Broker exposure exists without sufficient exact target identity for scoped autonomous recovery.",
            blockers=[_blocker("position_truth", evidence["position_truth_classification"])],
            allowed_next_steps=["refresh position/ownership/manifest evidence", "preserve artifacts", "observe only"],
        )
    if _recovery_budget_exhausted(evidence) or _crash_loop_budget_exhausted(evidence):
        return _decision(
            severity=ATTENTION,
            paper_action_policy=QUARANTINE_OBSERVE_ONLY,
            live_action_policy=LIVE_REQUIRE_ACK,
            reason=(
                "Crash-loop or repeated failure budget is exhausted; PAPER should quarantine, preserve evidence, "
                "and keep the catastrophic-behavior candidate visible without routine human gating."
            ),
            budget_exhausted=True,
            blockers=[_blocker("crash_loop_protection", evidence["crash_loop_classification"])],
            allowed_next_steps=["preserve crash-loop evidence", "refresh shared truth", "observe until cooldown/budget reset"],
        )
    if _prior_unsafe_stop(evidence) and _clean_current_truth(evidence):
        return _decision(
            severity=WARNING,
            paper_action_policy=AUTONOMOUS_RETRY_ELIGIBLE,
            live_action_policy=LIVE_REQUIRE_ACK,
            reason="Prior unsafe stop is recorded, but current shared truth is clean; PAPER allows bounded retry with enhanced observation.",
            autonomous_recovery_allowed=True,
            max_attempts_per_target=1,
            warnings=[_blocker("stop_provenance", "previous_broker_safe_at_stop=false")],
            allowed_next_steps=["retry within reduced budget", "preserve stop provenance", "monitor shared truth after launch"],
        )
    if _runtime_preflight_failure(evidence) and not _crash_loop_budget_exhausted(evidence):
        return _decision(
            severity=WARNING,
            paper_action_policy=AUTONOMOUS_RETRY_ELIGIBLE,
            live_action_policy=LIVE_REQUIRE_ACK,
            reason="Runtime failed before sustained convergence, but bounded PAPER retry budget remains.",
            autonomous_recovery_allowed=True,
            warnings=[_blocker("runtime_stop_or_launch", evidence["last_stop_or_launch_reason"])],
            allowed_next_steps=["retry runtime start within budget", "preserve launch status and stop provenance"],
        )
    if _clean_proof_ready(evidence):
        return _decision(
            severity=INFO,
            paper_action_policy=AUTONOMOUS_RETRY_ELIGIBLE,
            live_action_policy=LIVE_REQUIRE_ACK,
            reason="Shared truth is clean and proof readiness is ready; future PAPER executor may start runtime within budget.",
            autonomous_recovery_allowed=True,
            allowed_next_steps=["future executor may start runtime within bounded PAPER budget", "continue artifact-rich observation"],
        )
    return _decision(
        severity=WARNING,
        paper_action_policy=REFRESH_EVIDENCE,
        live_action_policy=LIVE_REQUIRE_ACK,
        reason="PAPER recovery policy could not identify a more specific action; refresh evidence and preserve artifacts.",
        warnings=[_blocker("unknown_policy_state", "refresh_evidence")],
        allowed_next_steps=["refresh shared truth", "rerun proof readiness", "preserve current artifacts"],
    )


def _decision(
    *,
    severity: str,
    paper_action_policy: str,
    live_action_policy: str,
    reason: str,
    autonomous_recovery_allowed: bool = False,
    budget_exhausted: bool = False,
    max_attempts_per_target: int = DEFAULT_MAX_ATTEMPTS_PER_TARGET,
    blockers: list[dict[str, str]] | None = None,
    warnings: list[dict[str, str]] | None = None,
    allowed_next_steps: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "severity": severity,
        "paper_action_policy": paper_action_policy,
        "live_action_policy": live_action_policy,
        "autonomous_recovery_allowed": autonomous_recovery_allowed,
        "requires_operator_ack_for_paper": False,
        "budget_exhausted": budget_exhausted,
        "max_attempts_per_target": max_attempts_per_target,
        "reason": reason,
        "blockers": blockers or [],
        "warnings": warnings or [],
        "allowed_next_steps": allowed_next_steps or ["preserve artifacts", "observe through shared truth"],
    }


def _evidence(inputs: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    runtime_supervisor = inputs["runtime_supervisor_authority"]
    crash_loop = inputs["crash_loop_protection"]
    runtime_resume = inputs["runtime_resume_semantics"]
    proof_readiness = inputs["proof_readiness"]
    position_truth = inputs["position_truth"]
    open_order_truth = inputs["open_order_truth"]
    managed_order_registry = inputs["managed_order_registry"]
    managed_position_registry = inputs["managed_position_registry"]
    order_adjustment_plan = inputs["order_adjustment_plan"]
    reconciliation = inputs["reconciliation"]
    broker_lease = inputs["broker_lease"]
    runtime_environment_truth = inputs["runtime_environment_truth"]
    stop_provenance = inputs["stop_provenance"]
    recovery_budget = inputs["recovery_budget_ledger"]
    recovery_budget_summary = _mapping(recovery_budget.get("summary"))
    recovery_budget_entry = _first_budget_entry(recovery_budget)
    return {
        "runtime_supervisor_classification": _classification(runtime_supervisor),
        "runtime_supervisor_mode": str(runtime_supervisor.get("supervisor_mode") or ""),
        "runtime_supervisor_recommended_next_command": str(runtime_supervisor.get("recommended_next_command") or ""),
        "self_recover_recommendation": str(inputs["self_recover_rules"].get("recommendation") or inputs["self_recover_rules"].get("classification") or ""),
        "crash_loop_classification": _classification(crash_loop),
        "crash_loop_restart_blocked": crash_loop.get("restart_blocked") is True,
        "crash_loop_operator_ack_required": crash_loop.get("operator_ack_required") is True,
        "recovery_budget_classification": _classification(recovery_budget),
        "recovery_budget_exhausted": recovery_budget.get("budget_exhausted") is True
        or recovery_budget_summary.get("budget_exhausted") is True,
        "recovery_budget_quarantine_required": recovery_budget.get("quarantine_required") is True
        or recovery_budget_summary.get("quarantine_required") is True,
        "recovery_budget_attempts_remaining": recovery_budget_entry.get("attempts_remaining"),
        "recovery_budget_authority_path": _mapping(recovery_budget.get("artifact_paths")).get("authority"),
        "runtime_resume_classification": _classification(runtime_resume),
        "runtime_resume_required_operator_ack": runtime_resume.get("required_operator_ack") is True
        or runtime_resume.get("operator_ack_required") is True,
        "agent_health_classification": _classification(inputs["agent_health"]),
        "proof_readiness_classification": _classification(proof_readiness),
        "phase1_session_reason": str(
            proof_readiness.get("phase1_session_reason")
            or _mapping(proof_readiness.get("market_session")).get("classification")
            or ""
        ),
        "open_order_truth_classification": _classification(open_order_truth),
        "managed_order_registry_classification": _classification(managed_order_registry),
        "order_adjustment_plan_classification": _classification(order_adjustment_plan),
        "position_truth_classification": _position_truth_classification(position_truth),
        "managed_position_registry_classification": _classification(managed_position_registry),
        "reconciliation_classification": _classification(reconciliation),
        "broker_lease_classification": _classification(broker_lease) or str(broker_lease.get("lease_state") or ""),
        "runtime_environment_truth_classification": _classification(runtime_environment_truth),
        "runtime_writer_authority": str(runtime_environment_truth.get("writer_authority") or ""),
        "duplicate_writer_count": int(runtime_environment_truth.get("duplicate_writer_count") or 0),
        "live_money_eligible": _any_true(
            runtime_supervisor,
            inputs["self_recover_rules"],
            crash_loop,
            runtime_resume,
            proof_readiness,
            open_order_truth,
            managed_order_registry,
            position_truth,
            managed_position_registry,
            reconciliation,
            runtime_environment_truth,
            key="live_money_eligible",
        ),
        "stop_provenance": dict(stop_provenance),
        "previous_broker_safe_at_stop": stop_provenance.get("broker_safe_at_stop")
        if "broker_safe_at_stop" in stop_provenance
        else runtime_resume.get("previous_broker_safe_at_stop"),
        "last_stop_or_launch_reason": str(
            stop_provenance.get("stop_reason")
            or runtime_resume.get("previous_stop_reason")
            or runtime_supervisor.get("reason")
            or ""
        ),
        "stale_or_missing_evidence": _stale_or_missing_evidence(inputs),
        "lifecycle_state_matrix": {
            "states": [
                OPEN_MANAGED,
                CLOSED_FLAT,
                REVIEW_REQUIRED,
                BLOCKED_NO_BROKER_EFFECT,
                BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE,
                OPEN_MANAGED_METADATA_INCOMPLETE,
                CLOSE_UNKNOWN,
                MANUAL_OR_MALFORMED_CLEANUP,
            ],
            "authority": "mgc_v05l.execution_core.track_b_lifecycle_state_transition",
        },
    }


def _clean_proof_ready(evidence: Mapping[str, Any]) -> bool:
    return (
        evidence["proof_readiness_classification"] == READY_FOR_PROOF
        and evidence["runtime_supervisor_classification"] == SUPERVISOR_RUNTIME_START_ALLOWED
        and _clean_current_truth(evidence)
    )


def _clean_current_truth(evidence: Mapping[str, Any]) -> bool:
    return (
        evidence["position_truth_classification"] == "CLEAN_FLAT_READY"
        and evidence["open_order_truth_classification"] == NO_OPEN_ORDERS
        and evidence["managed_order_registry_classification"] in {"NO_MANAGED_ORDERS", ""}
        and evidence["managed_position_registry_classification"] in {"NO_MANAGED_POSITIONS", ""}
        and evidence["reconciliation_classification"] in {"TRACK_B_PAPER_BROKER_RECONCILED", ""}
    )


def _market_closed(*, evidence: Mapping[str, Any], inputs: Mapping[str, Mapping[str, Any]]) -> bool:
    if evidence["runtime_supervisor_classification"] == SUPERVISOR_WAIT_MARKET_CLOSED:
        return True
    if evidence["proof_readiness_classification"] == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    if evidence["phase1_session_reason"] == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    for agent in _list(inputs["agent_health"].get("agents")):
        if agent.get("agent_id") == "phase1_databento_live_candles" and agent.get("reason") == MARKET_CLOSED_NO_FRESH_BARS:
            return True
    return False


def _stale_or_missing_authority(evidence: Mapping[str, Any]) -> bool:
    return bool(evidence["stale_or_missing_evidence"])


def _stale_or_missing_evidence(inputs: Mapping[str, Mapping[str, Any]]) -> list[str]:
    required = {
        "runtime_supervisor_authority": "classification",
        "self_recover_rules": "recommendation",
        "crash_loop_protection": "classification",
        "runtime_resume_semantics": "classification",
        "agent_health": "classification",
        "proof_readiness": "classification",
        "open_order_truth": "classification",
        "managed_order_registry": "classification",
        "position_truth": "classification",
        "managed_position_registry": "classification",
        "reconciliation": "classification",
        "broker_lease": "classification",
        "runtime_environment_truth": "classification",
        "recovery_budget_ledger": "classification",
    }
    missing: list[str] = []
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


def _suspicious_order(evidence: Mapping[str, Any]) -> bool:
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


def _broker_exposure(evidence: Mapping[str, Any]) -> bool:
    if evidence["runtime_environment_truth_classification"] == "RUNTIME_DOWN_WITH_BROKER_EXPOSURE":
        return True
    if evidence["position_truth_classification"] not in {"", "CLEAN_FLAT_READY"}:
        return True
    if evidence["managed_position_registry_classification"] not in {"", "NO_MANAGED_POSITIONS"}:
        return True
    return False


def _exact_recovery_target_known(*, inputs: Mapping[str, Mapping[str, Any]]) -> bool:
    for payload_name in ("position_truth", "managed_position_registry"):
        payload = inputs[payload_name]
        if payload.get("exact_target_identity_known") is True:
            return True
        for position in _positions(payload):
            if position.get("exact_target_identity_known") is True:
                return True
            if _position_identity_complete(position):
                return True
    return False


def _position_identity_complete(position: Mapping[str, Any]) -> bool:
    symbol = _text(position.get("symbol"), position.get("instrument"), position.get("root_symbol"))
    contract = _text(position.get("contract"), position.get("local_symbol"), position.get("contract_key"), position.get("conId"))
    qty = _text(position.get("quantity"), position.get("qty"), position.get("broker_quantity"))
    owner = _text(position.get("ownership_id"), position.get("owner_id"), position.get("submit_owner_id"))
    lifecycle = _text(position.get("lifecycle_id"), position.get("lifecycle_report_path"), position.get("manifest_id"), position.get("manifest_path"))
    return bool(symbol and contract and qty and (owner or lifecycle))


def _positions(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    for key in ("positions", "broker_positions", "managed_positions", "per_position"):
        value = payload.get(key)
        if isinstance(value, Mapping):
            rows.extend(_mapping(item) for item in value.values())
        else:
            rows.extend(_mapping(item) for item in _list(value))
    summary = _mapping(payload.get("summary"))
    for key in ("positions", "broker_positions", "managed_positions"):
        value = summary.get(key)
        if isinstance(value, Mapping):
            rows.extend(_mapping(item) for item in value.values())
        else:
            rows.extend(_mapping(item) for item in _list(value))
    return [row for row in rows if row]


def _crash_loop_budget_exhausted(evidence: Mapping[str, Any]) -> bool:
    return evidence["crash_loop_restart_blocked"] is True or evidence["crash_loop_classification"] in {
        "RESTART_COOLDOWN_ACTIVE",
        "REPEATED_RUNTIME_FAILURE",
        "REPEATED_MARKET_DATA_FAILURE",
        "REPEATED_BROKER_LEASE_FAILURE",
        "REPEATED_UNSAFE_STOP_QUARANTINE",
        "OPERATOR_ACK_REQUIRED",
    }


def _recovery_budget_exhausted(evidence: Mapping[str, Any]) -> bool:
    return evidence["recovery_budget_exhausted"] is True or evidence["recovery_budget_quarantine_required"] is True


def _prior_unsafe_stop(evidence: Mapping[str, Any]) -> bool:
    return evidence["previous_broker_safe_at_stop"] is False


def _runtime_preflight_failure(evidence: Mapping[str, Any]) -> bool:
    reason = str(evidence["last_stop_or_launch_reason"] or "")
    supervisor_reason = str(evidence["runtime_supervisor_classification"] or "")
    return reason in {
        "runtime_exited_after_preflight",
        "RUNTIME_EXITED_AFTER_PREFLIGHT",
        "RUNTIME_TRUTH_NOT_CONVERGED",
        "RUNTIME_EXITED_AFTER_INITIAL_TRUTH",
    } or supervisor_reason in {"RUNTIME_EXITED_AFTER_PREFLIGHT", "RUNTIME_TRUTH_NOT_CONVERGED"}


def _duplicate_runtime_writer(*, evidence: Mapping[str, Any], inputs: Mapping[str, Mapping[str, Any]]) -> bool:
    if evidence["runtime_environment_truth_classification"] == "DUPLICATE_RUNTIME_WRITERS":
        return True
    if evidence["runtime_writer_authority"] == "DUPLICATE_WRITER":
        return True
    if evidence["duplicate_writer_count"] > 0:
        return True
    runtime_supervisor = inputs["runtime_supervisor_authority"]
    return runtime_supervisor.get("duplicate_runtime_writers") is True


def _input_artifacts(config: TrackBPaperRecoveryPolicyConfig) -> dict[str, str]:
    return {
        "runtime_supervisor_authority": str(config.resolve(config.runtime_supervisor_authority_path)),
        "self_recover_rules": str(config.resolve(config.self_recover_rules_path)),
        "crash_loop_protection": str(config.resolve(config.crash_loop_protection_path)),
        "runtime_resume_semantics": str(config.resolve(config.runtime_resume_semantics_path)),
        "agent_health": str(config.resolve(config.agent_health_path)),
        "proof_readiness": str(config.resolve(config.proof_readiness_path)),
        "open_order_truth": str(config.resolve(config.open_order_truth_path)),
        "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
        "order_adjustment_plan": str(config.resolve(config.order_adjustment_plan_path)),
        "position_truth": str(config.resolve(config.position_truth_path)),
        "managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
        "reconciliation": str(config.resolve(config.reconciliation_path)),
        "broker_lease": str(config.resolve(config.broker_lease_path)),
        "stop_provenance": str(config.resolve(config.stop_provenance_path)),
        "runtime_environment_truth": str(config.resolve(config.runtime_environment_truth_path)),
        "lifecycle_state_matrix": str(config.resolve(config.lifecycle_state_matrix_path)),
        "recovery_budget_ledger": str(config.resolve(config.recovery_budget_ledger_path)),
    }


def _blocker(code: str, detail: Any) -> dict[str, str]:
    return {"code": code, "detail": str(detail or "")}


def _policy_id(now: datetime) -> str:
    return f"track-b-paper-recovery-policy-{now.strftime('%Y%m%dT%H%M%SZ')}"


def _classification(payload: Mapping[str, Any]) -> str:
    return str(payload.get("classification") or "")


def _position_truth_classification(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return str(payload.get("classification") or summary.get("overall_classification") or "")


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


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _first_budget_entry(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    for entry in _list(payload.get("entries")):
        return _mapping(entry)
    return {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _text(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _any_true(*payloads: Mapping[str, Any], key: str) -> bool:
    return any(payload.get(key) is True for payload in payloads)


if __name__ == "__main__":
    raise SystemExit(main())
