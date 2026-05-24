"""Read-only Track B PAPER runtime resume semantics authority.

Runtime Resume Semantics authority lives in execution_core. Dashboard artifacts
are projections and must not be used as runtime, readiness, restart, broker, or
routing authority. This v1 policy classifies resume eligibility only; it never
starts, stops, restarts, submits, cancels, replaces, closes, or flattens
anything.
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
from mgc_v05l.execution_core.track_b_control_plane_snapshot_status import (
    CONTROL_PLANE_READY,
    classify_control_plane_snapshot_status,
)
from mgc_v05l.execution_core.track_b_crash_loop_protection import (
    DEFAULT_CRASH_LOOP_PROTECTION_ARTIFACT,
    OPERATOR_ACK_REQUIRED,
    RESTART_COOLDOWN_ACTIVE,
)
from mgc_v05l.execution_core.track_b_managed_order_registry import (
    DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT,
    NO_MANAGED_ORDERS,
)
from mgc_v05l.execution_core.track_b_managed_position_registry import (
    DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT,
    NO_MANAGED_POSITIONS,
)
from mgc_v05l.execution_core.track_b_open_order_truth import DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT, NO_OPEN_ORDERS
from mgc_v05l.execution_core.track_b_paper_proof_readiness import DEFAULT_OUTPUT_PATH as DEFAULT_PROOF_READINESS_ARTIFACT
from mgc_v05l.execution_core.track_b_paper_proof_readiness import READY_FOR_PROOF
from mgc_v05l.execution_core.track_b_position_truth_monitor import DEFAULT_POSITION_TRUTH_ARTIFACT
from mgc_v05l.execution_core.track_b_projection_metadata import build_projection_metadata
from mgc_v05l.execution_core.track_b_recovery_budget_ledger import (
    DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT,
    budget_summary_for_action,
)
from mgc_v05l.execution_core.track_b_runtime_environment_truth import (
    DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT,
    RUNTIME_ACTIVE_OBSERVATION_ONLY,
    RUNTIME_ACTIVE_TRADE_CAPABLE,
    RUNTIME_DOWN_CLEAN,
    RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
)
from mgc_v05l.execution_core.track_b_self_recover_rules import DEFAULT_SELF_RECOVER_RULES_ARTIFACT
from mgc_v05l.execution_core.track_b_shared_truth_refresh_cli import DEFAULT_RECONCILIATION_ARTIFACT
from mgc_v05l.execution_core.track_b_broker_truth_lease import DEFAULT_LEASE_ARTIFACT
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


RESUME_ALLOWED_CLEAN = "RESUME_ALLOWED_CLEAN"
RESUME_BLOCKED_MARKET_CLOSED = "RESUME_BLOCKED_MARKET_CLOSED"
RESUME_BLOCKED_SHARED_TRUTH = "RESUME_BLOCKED_SHARED_TRUTH"
RESUME_BLOCKED_CRASH_LOOP = "RESUME_BLOCKED_CRASH_LOOP"
RESUME_BLOCKED_OPERATOR_ACK_REQUIRED = "RESUME_BLOCKED_OPERATOR_ACK_REQUIRED"
RESUME_BLOCKED_RUNTIME_ALREADY_ACTIVE = "RESUME_BLOCKED_RUNTIME_ALREADY_ACTIVE"
RESUME_BLOCKED_BROKER_EXPOSURE = "RESUME_BLOCKED_BROKER_EXPOSURE"
RESUME_BLOCKED_OPEN_ORDER = "RESUME_BLOCKED_OPEN_ORDER"
RESUME_BLOCKED_MANAGED_POSITION = "RESUME_BLOCKED_MANAGED_POSITION"
RESUME_BLOCKED_STALE_OR_MISSING_EVIDENCE = "RESUME_BLOCKED_STALE_OR_MISSING_EVIDENCE"
RESUME_REQUIRES_MANUAL_CLEANUP = "RESUME_REQUIRES_MANUAL_CLEANUP"
RESUME_UNKNOWN_REVIEW_REQUIRED = "RESUME_UNKNOWN_REVIEW_REQUIRED"
RESUME_ALLOWED_PAPER_BOUNDED_RETRY = "RESUME_ALLOWED_PAPER_BOUNDED_RETRY"
RESUME_BLOCKED_PAPER_QUARANTINE_OBSERVE_ONLY = "RESUME_BLOCKED_PAPER_QUARANTINE_OBSERVE_ONLY"
RESUME_BLOCKED_HARD_UNSAFE = "RESUME_BLOCKED_HARD_UNSAFE"

PAPER_POLICY_AUTONOMOUS_RETRY_ELIGIBLE = "AUTONOMOUS_RETRY_ELIGIBLE"
PAPER_POLICY_SCOPED_RECOVERY_ELIGIBLE = "SCOPED_RECOVERY_ELIGIBLE"
PAPER_POLICY_QUARANTINE_OBSERVE_ONLY = "QUARANTINE_OBSERVE_ONLY"
PAPER_POLICY_HARD_UNSAFE_HOLD = "HARD_UNSAFE_HOLD"
PAPER_POLICY_OBSERVE = "OBSERVE"

RESUME_POLICY_NEW_RUNTIME_GENERATION_ALLOWED = "NEW_RUNTIME_GENERATION_ALLOWED"
RESUME_POLICY_RESUME_EXISTING_RUNTIME = "RESUME_EXISTING_RUNTIME"
RESUME_POLICY_HOLD_MARKET_CLOSED = "HOLD_MARKET_CLOSED"
RESUME_POLICY_HOLD_STALE_EVIDENCE = "HOLD_STALE_EVIDENCE"
RESUME_POLICY_HOLD_DUPLICATE_WRITER = "HOLD_DUPLICATE_WRITER"
RESUME_POLICY_HOLD_LIVE_MONEY = "HOLD_LIVE_MONEY"
RESUME_POLICY_HOLD_BUDGET_EXHAUSTED = "HOLD_BUDGET_EXHAUSTED"
RESUME_POLICY_QUARANTINE_OBSERVE_ONLY = "QUARANTINE_OBSERVE_ONLY"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RUNTIME_RESUME_SEMANTICS_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "runtime_resume" / "latest_runtime_resume_semantics.json"
)
DEFAULT_DASHBOARD_RUNTIME_RESUME_SEMANTICS_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_runtime_resume_semantics.json"
)
DEFAULT_SHARED_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "shared_truth" / "latest_track_b_shared_truth_refresh.json"
)
DEFAULT_RUNTIME_STOP_PROVENANCE_ARTIFACT = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "latest_runtime_stop_provenance.json"
)
DEFAULT_PAPER_RECOVERY_POLICY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "paper_recovery_policy" / "latest_paper_recovery_policy.json"
)
DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "control_plane" / "latest_control_plane_snapshot.json"
)


@dataclass(frozen=True)
class TrackBRuntimeResumeSemanticsConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_RUNTIME_RESUME_SEMANTICS_ARTIFACT
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_RUNTIME_RESUME_SEMANTICS_PROJECTION
    proof_readiness_path: Path = DEFAULT_PROOF_READINESS_ARTIFACT
    shared_truth_path: Path = DEFAULT_SHARED_TRUTH_ARTIFACT
    runtime_environment_truth_path: Path = DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT
    agent_health_path: Path = DEFAULT_AGENT_HEALTH_ARTIFACT
    self_recover_rules_path: Path = DEFAULT_SELF_RECOVER_RULES_ARTIFACT
    crash_loop_protection_path: Path = DEFAULT_CRASH_LOOP_PROTECTION_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    broker_lease_path: Path = DEFAULT_LEASE_ARTIFACT
    stop_provenance_path: Path = DEFAULT_RUNTIME_STOP_PROVENANCE_ARTIFACT
    paper_recovery_policy_path: Path = DEFAULT_PAPER_RECOVERY_POLICY_ARTIFACT
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    allow_control_plane_build_context: bool = False
    recovery_budget_ledger_path: Path = DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_runtime_resume_semantics(
    *,
    config: TrackBRuntimeResumeSemanticsConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    inputs = {
        "proof_readiness": _read_json(config.resolve(config.proof_readiness_path)),
        "shared_truth": _read_json(config.resolve(config.shared_truth_path)),
        "runtime_environment_truth": _read_json(config.resolve(config.runtime_environment_truth_path)),
        "agent_health": _read_json(config.resolve(config.agent_health_path)),
        "self_recover_rules": _read_json(config.resolve(config.self_recover_rules_path)),
        "crash_loop_protection": _read_json(config.resolve(config.crash_loop_protection_path)),
        "position_truth": _read_json(config.resolve(config.position_truth_path)),
        "open_order_truth": _read_json(config.resolve(config.open_order_truth_path)),
        "managed_order_registry": _read_json(config.resolve(config.managed_order_registry_path)),
        "managed_position_registry": _read_json(config.resolve(config.managed_position_registry_path)),
        "reconciliation": _read_json(config.resolve(config.reconciliation_path)),
        "broker_lease": _read_json(config.resolve(config.broker_lease_path)),
        "stop_provenance": _read_json(config.resolve(config.stop_provenance_path)),
        "paper_recovery_policy": _read_json(config.resolve(config.paper_recovery_policy_path)),
        "control_plane_snapshot": _read_json(config.resolve(config.control_plane_snapshot_path)),
        "recovery_budget_ledger": _read_json(config.resolve(config.recovery_budget_ledger_path)),
    }
    decision = _classify_resume(
        inputs=inputs,
        now=actual_now,
        allow_control_plane_build_context=config.allow_control_plane_build_context,
    )
    stop = _stop_provenance(inputs["stop_provenance"])
    generation = _generation_evidence(inputs=inputs, now=actual_now)
    payload = {
        "schema_version": "track_b_runtime_resume_semantics_v2",
        "resume_semantics_version": "v2",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "recommendation_only": True,
        "submit_authority": False,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "runtime_restart_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "resume_id": _resume_id(actual_now),
        "classification": decision["classification"],
        "allowed": decision["allowed"],
        "reason": decision["reason"],
        "required_operator_ack": decision["required_operator_ack"],
        "safe_to_start_runtime": decision["safe_to_start_runtime"],
        "safe_to_reuse_previous_runtime_state": decision["safe_to_reuse_previous_runtime_state"],
        "must_start_new_runtime_generation": decision["must_start_new_runtime_generation"],
        "resume_action_policy": decision["resume_action_policy"],
        "generation_reuse_allowed": decision["generation_reuse_allowed"],
        "must_start_new_generation": decision["must_start_new_generation"],
        "bounded_retry_budget_key": decision["bounded_retry_budget_key"],
        "attempts_remaining": decision["attempts_remaining"],
        "cooldown_until": decision["cooldown_until"],
        "enhanced_observation_required": decision["enhanced_observation_required"],
        **generation,
        "previous_runtime_instance_id": stop.get("runtime_instance_id"),
        "previous_stop_source": stop.get("stop_source"),
        "previous_stop_reason": stop.get("stop_reason"),
        "previous_broker_safe_at_stop": stop.get("broker_safe_at_stop"),
        "resume_mode": decision["resume_mode"],
        "blockers": decision["blockers"],
        "warnings": decision["warnings"],
        "evidence": _evidence(inputs),
        "input_artifacts": {
            "proof_readiness": str(config.resolve(config.proof_readiness_path)),
            "shared_truth": str(config.resolve(config.shared_truth_path)),
            "runtime_environment_truth": str(config.resolve(config.runtime_environment_truth_path)),
            "agent_health": str(config.resolve(config.agent_health_path)),
            "self_recover_rules": str(config.resolve(config.self_recover_rules_path)),
            "crash_loop_protection": str(config.resolve(config.crash_loop_protection_path)),
            "position_truth": str(config.resolve(config.position_truth_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
            "managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "broker_lease": str(config.resolve(config.broker_lease_path)),
            "stop_provenance": str(config.resolve(config.stop_provenance_path)),
            "paper_recovery_policy": str(config.resolve(config.paper_recovery_policy_path)),
            "control_plane_snapshot": str(config.resolve(config.control_plane_snapshot_path)),
            "recovery_budget_ledger": str(config.resolve(config.recovery_budget_ledger_path)),
        },
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "dashboard_projection": None
            if config.dashboard_projection_path is None
            else str(config.resolve(config.dashboard_projection_path)),
        },
        "todo_v2": [
            "Keep operator acknowledgement as a future LIVE/PRE-LIVE or exceptional PAPER adapter, not a core PAPER recovery dependency.",
            "Attach resume attempt ids to launch status so new generation starts can be correlated back to this verdict.",
            "Teach self-healing executor to consume this advisory artifact before any restart action.",
        ],
    }
    return payload


def write_track_b_runtime_resume_semantics(
    *,
    config: TrackBRuntimeResumeSemanticsConfig,
    payload: Mapping[str, Any],
) -> Path:
    authority_path = config.resolve(config.output_path)
    _write_json_atomic(authority_path, dict(payload))
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_runtime_resume_projection(authority_payload=payload, authority_path=authority_path),
        )
    return authority_path


def build_dashboard_runtime_resume_projection(
    *,
    authority_payload: Mapping[str, Any],
    authority_path: Path,
) -> dict[str, Any]:
    return {
        **dict(authority_payload),
        "schema_version": "track_b_runtime_resume_semantics_dashboard_projection_v1",
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
    parser = argparse.ArgumentParser(description="Write read-only Track B PAPER runtime resume semantics.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_RUNTIME_RESUME_SEMANTICS_ARTIFACT)
    parser.add_argument("--dashboard-projection-path", type=Path, default=DEFAULT_DASHBOARD_RUNTIME_RESUME_SEMANTICS_PROJECTION)
    parser.add_argument("--no-dashboard-projection", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBRuntimeResumeSemanticsConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        dashboard_projection_path=None if bool(args.no_dashboard_projection) else Path(args.dashboard_projection_path),
    )
    payload = build_track_b_runtime_resume_semantics(config=config)
    authority_path = write_track_b_runtime_resume_semantics(config=config, payload=payload)
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            json.dumps(
                {
                    "classification": payload.get("classification"),
                    "allowed": payload.get("allowed"),
                    "reason": payload.get("reason"),
                    "resume_semantics_version": payload.get("resume_semantics_version"),
                    "resume_action_policy": payload.get("resume_action_policy"),
                    "safe_to_start_runtime": payload.get("safe_to_start_runtime"),
                    "proposed_next_runtime_generation_id": payload.get("proposed_next_runtime_generation_id"),
                    "attempts_remaining": payload.get("attempts_remaining"),
                    "authority_path": str(authority_path),
                    "read_only": True,
                    "paper_proof_invoked": False,
                    "live_money_eligible": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0 if payload.get("allowed") is True else 2


def _classify_resume(
    *,
    inputs: Mapping[str, Mapping[str, Any]],
    now: datetime,
    allow_control_plane_build_context: bool = False,
) -> dict[str, Any]:
    evidence = _evidence(inputs)
    stop = _stop_provenance(inputs["stop_provenance"])
    control_plane_status = classify_control_plane_snapshot_status(inputs["control_plane_snapshot"], now=now)
    budget = _runtime_retry_budget(inputs["recovery_budget_ledger"])
    blockers: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []

    if evidence["live_money_eligible"] is True:
        return _decision(
            RESUME_BLOCKED_HARD_UNSAFE,
            "live_money_eligible=true is a hard unsafe PAPER hold.",
            blockers=[_blocker("live_money_eligible", "true")],
            warnings=warnings,
            resume_mode="hard_unsafe_hold",
            resume_action_policy=RESUME_POLICY_HOLD_LIVE_MONEY,
            budget=budget,
        )

    if _duplicate_writer(evidence):
        return _decision(
            RESUME_BLOCKED_HARD_UNSAFE,
            "Duplicate runtime writer evidence is present.",
            blockers=[_blocker("runtime_environment_truth", evidence["runtime_environment_truth_classification"])],
            warnings=warnings,
            resume_mode="hard_unsafe_hold",
            resume_action_policy=RESUME_POLICY_HOLD_DUPLICATE_WRITER,
            budget=budget,
        )

    if _market_closed(inputs):
        return _decision(
            RESUME_BLOCKED_MARKET_CLOSED,
            MARKET_CLOSED_NO_FRESH_BARS,
            blockers=[_blocker("market_session", MARKET_CLOSED_NO_FRESH_BARS)],
            warnings=warnings,
            resume_mode="hold_down_market_closed",
            resume_action_policy=RESUME_POLICY_HOLD_MARKET_CLOSED,
            budget=budget,
        )

    if control_plane_status.get("classification") != CONTROL_PLANE_READY and not (
        allow_control_plane_build_context and _shared_truth_supports_new_generation(evidence)
    ):
        return _decision(
            RESUME_BLOCKED_STALE_OR_MISSING_EVIDENCE,
            str(control_plane_status.get("reason") or "Control Plane Snapshot is unavailable or not launch-ready."),
            blockers=[_blocker("control_plane_snapshot", str(control_plane_status.get("classification") or ""))],
            warnings=warnings,
            resume_mode="hold_down_refresh_required",
            resume_action_policy=RESUME_POLICY_HOLD_STALE_EVIDENCE,
            budget=budget,
        )
    if control_plane_status.get("classification") != CONTROL_PLANE_READY:
        warnings.append(
            _blocker(
                "control_plane_snapshot",
                str(control_plane_status.get("classification") or "CONTROL_PLANE_SNAPSHOT_REFRESH_IN_PROGRESS"),
            )
        )

    if budget.get("missing") is True or budget.get("attempts_remaining") is None:
        return _decision(
            RESUME_BLOCKED_STALE_OR_MISSING_EVIDENCE,
            "Recovery Budget Ledger is missing or does not contain RUNTIME_RETRY budget evidence.",
            blockers=[_blocker("recovery_budget_ledger", "missing_runtime_retry_budget")],
            warnings=warnings,
            resume_mode="hold_down_refresh_required",
            resume_action_policy=RESUME_POLICY_HOLD_STALE_EVIDENCE,
            budget=budget,
        )

    if evidence["paper_action_policy"] == PAPER_POLICY_HARD_UNSAFE_HOLD:
        return _decision(
            RESUME_BLOCKED_HARD_UNSAFE,
            f"PAPER Recovery Policy is {evidence['paper_action_policy']} ({evidence['paper_policy_severity']}).",
            blockers=[_blocker("paper_recovery_policy", evidence["paper_action_policy"])],
            warnings=warnings,
            resume_mode="hard_unsafe_hold",
            resume_action_policy=RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
            budget=budget,
        )

    runtime_class = evidence["runtime_environment_truth_classification"]
    if runtime_class in {RUNTIME_ACTIVE_TRADE_CAPABLE, RUNTIME_ACTIVE_OBSERVATION_ONLY}:
        return _decision(
            RESUME_BLOCKED_RUNTIME_ALREADY_ACTIVE,
            "Runtime Environment Truth already reports an active PAPER runtime.",
            blockers=[_blocker("runtime_already_active", runtime_class)],
            warnings=warnings,
            resume_mode="existing_runtime_present",
            safe_to_reuse_previous_runtime_state=True,
            must_start_new_runtime_generation=False,
            resume_action_policy=RESUME_POLICY_RESUME_EXISTING_RUNTIME,
            generation_reuse_allowed=True,
            must_start_new_generation=False,
            budget=budget,
        )

    if runtime_class == RUNTIME_DOWN_WITH_BROKER_EXPOSURE or evidence["position_truth_classification"] != "CLEAN_FLAT_READY":
        return _decision(
            RESUME_BLOCKED_BROKER_EXPOSURE,
            f"Position Truth is {evidence['position_truth_classification']}.",
            blockers=[_blocker("broker_exposure", evidence["position_truth_classification"])],
            warnings=warnings,
            resume_mode="manual_cleanup_required",
            resume_action_policy=RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
            budget=budget,
        )

    if evidence["open_order_truth_classification"] != NO_OPEN_ORDERS:
        return _decision(
            RESUME_BLOCKED_OPEN_ORDER,
            f"Open Order Truth is {evidence['open_order_truth_classification']}.",
            blockers=[_blocker("open_order_truth", evidence["open_order_truth_classification"])],
            warnings=warnings,
            resume_mode="manual_cleanup_required",
            resume_action_policy=RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
            budget=budget,
        )

    if evidence["managed_order_registry_classification"] != NO_MANAGED_ORDERS:
        return _decision(
            RESUME_BLOCKED_OPEN_ORDER,
            f"Managed Order Registry is {evidence['managed_order_registry_classification']}.",
            blockers=[_blocker("managed_order_registry", evidence["managed_order_registry_classification"])],
            warnings=warnings,
            resume_mode="manual_cleanup_required",
            resume_action_policy=RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
            budget=budget,
        )

    if evidence["managed_position_registry_classification"] != NO_MANAGED_POSITIONS:
        return _decision(
            RESUME_BLOCKED_MANAGED_POSITION,
            f"Managed Position Registry is {evidence['managed_position_registry_classification']}.",
            blockers=[_blocker("managed_position_registry", evidence["managed_position_registry_classification"])],
            warnings=warnings,
            resume_mode="manual_cleanup_required",
            resume_action_policy=RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
            budget=budget,
        )

    if evidence["reconciliation_classification"] not in {"TRACK_B_PAPER_BROKER_RECONCILED", ""}:
        return _decision(
            RESUME_REQUIRES_MANUAL_CLEANUP,
            f"Reconciliation is {evidence['reconciliation_classification']}.",
            blockers=[_blocker("reconciliation", evidence["reconciliation_classification"])],
            warnings=warnings,
            resume_mode="manual_cleanup_required",
            resume_action_policy=RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
            budget=budget,
        )

    if budget.get("budget_exhausted") is True or budget.get("quarantine_required") is True:
        return _decision(
            RESUME_BLOCKED_PAPER_QUARANTINE_OBSERVE_ONLY,
            "Recovery Budget Ledger is exhausted; PAPER should quarantine-observe rather than require routine operator ack.",
            blockers=[_blocker("recovery_budget_ledger", str(budget.get("budget_key") or ""))],
            warnings=warnings,
            resume_mode="paper_quarantine_observe_only",
            resume_action_policy=RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
            budget=budget,
        )

    if evidence["crash_loop_operator_ack_required"] is True or stop.get("broker_safe_at_stop") is False:
        if _paper_policy_bounded_retry(evidence):
            return _decision(
                RESUME_ALLOWED_PAPER_BOUNDED_RETRY,
                "PAPER Recovery Policy permits bounded retry/enhanced observation despite legacy operator-ack evidence.",
                blockers=[],
                warnings=[
                    _blocker(
                        "paper_recovery_policy",
                        f"{evidence['paper_action_policy']} severity={evidence['paper_policy_severity']}",
                    )
                ],
                resume_mode="paper_bounded_retry_enhanced_observation",
                allowed=True,
                safe_to_start_runtime=True,
                resume_action_policy=RESUME_POLICY_NEW_RUNTIME_GENERATION_ALLOWED,
                budget=budget,
                enhanced_observation_required=stop.get("broker_safe_at_stop") is False
                or evidence["crash_loop_operator_ack_required"] is True,
            )
        if _paper_policy_quarantine(evidence):
            return _decision(
                RESUME_BLOCKED_PAPER_QUARANTINE_OBSERVE_ONLY,
                "PAPER Recovery Policy converts crash-loop/operator-ack evidence into quarantine-observe posture.",
                blockers=[_blocker("paper_recovery_policy", evidence["paper_action_policy"])],
                warnings=warnings,
                resume_mode="paper_quarantine_observe_only",
                resume_action_policy=RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
                budget=budget,
            )
        return _decision(
            RESUME_BLOCKED_OPERATOR_ACK_REQUIRED,
            (
                "Legacy operator-ack evidence is present; PAPER treats this as a blocked/quarantine posture "
                "until PAPER Recovery Policy or fresh shared truth provides a bounded action."
            ),
            blockers=[_blocker("operator_ack_required", str(evidence["crash_loop_classification"] or stop.get("stop_reason") or ""))],
            warnings=warnings,
            resume_mode="hold_down_operator_ack_required",
            required_operator_ack=False,
            resume_action_policy=RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
            budget=budget,
        )

    if evidence["crash_loop_restart_blocked"] is True:
        if _paper_policy_quarantine(evidence):
            return _decision(
                RESUME_BLOCKED_PAPER_QUARANTINE_OBSERVE_ONLY,
                "PAPER Recovery Policy marks crash-loop budget exhausted as quarantine-observe, not operator ack.",
                blockers=[_blocker("paper_recovery_policy", evidence["paper_action_policy"])],
                warnings=[_blocker("crash_loop_protection", evidence["crash_loop_classification"])],
                resume_mode="paper_quarantine_observe_only",
                resume_action_policy=RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
                budget=budget,
            )
        return _decision(
            RESUME_BLOCKED_CRASH_LOOP,
            f"Crash Loop Protection is {evidence['crash_loop_classification']}.",
            blockers=[_blocker("crash_loop_protection", evidence["crash_loop_classification"])],
            warnings=warnings,
            resume_mode="hold_down_crash_loop",
            resume_action_policy=RESUME_POLICY_HOLD_BUDGET_EXHAUSTED
            if budget.get("budget_exhausted") is True
            else RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
            budget=budget,
        )

    missing = _missing_or_stale_evidence(evidence)
    if missing:
        blockers.extend(_blocker("stale_or_missing_evidence", item) for item in missing)
        return _decision(
            RESUME_BLOCKED_STALE_OR_MISSING_EVIDENCE,
            f"Stale or missing authority evidence: {', '.join(missing)}.",
            blockers=blockers,
            warnings=warnings,
            resume_mode="hold_down_refresh_required",
            resume_action_policy=RESUME_POLICY_HOLD_STALE_EVIDENCE,
            budget=budget,
        )

    proof_class = evidence["proof_readiness_classification"]
    if proof_class == READY_FOR_PROOF:
        return _decision(
            RESUME_ALLOWED_CLEAN,
            "Clean flat shared truth and proof readiness is READY_FOR_PROOF.",
            blockers=[],
            warnings=warnings,
            resume_mode="clean_new_start",
            allowed=True,
            safe_to_start_runtime=True,
            resume_action_policy=RESUME_POLICY_NEW_RUNTIME_GENERATION_ALLOWED,
            budget=budget,
        )

    return _decision(
        RESUME_BLOCKED_SHARED_TRUTH,
        f"Proof Readiness is {proof_class}.",
        blockers=[_blocker("proof_readiness", proof_class)],
        warnings=warnings,
        resume_mode="hold_down_shared_truth",
        resume_action_policy=RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
        budget=budget,
    )


def _decision(
    classification: str,
    reason: str,
    *,
    blockers: list[dict[str, str]],
    warnings: list[dict[str, str]],
    resume_mode: str,
    resume_action_policy: str,
    budget: Mapping[str, Any],
    allowed: bool = False,
    safe_to_start_runtime: bool = False,
    safe_to_reuse_previous_runtime_state: bool = False,
    must_start_new_runtime_generation: bool = True,
    generation_reuse_allowed: bool = False,
    must_start_new_generation: bool = True,
    required_operator_ack: bool = False,
    enhanced_observation_required: bool = False,
) -> dict[str, Any]:
    return {
        "classification": classification,
        "allowed": allowed,
        "reason": reason,
        "required_operator_ack": required_operator_ack,
        "safe_to_start_runtime": safe_to_start_runtime,
        "safe_to_reuse_previous_runtime_state": safe_to_reuse_previous_runtime_state,
        "must_start_new_runtime_generation": must_start_new_runtime_generation,
        "resume_action_policy": resume_action_policy,
        "generation_reuse_allowed": generation_reuse_allowed,
        "must_start_new_generation": must_start_new_generation,
        "bounded_retry_budget_key": budget.get("budget_key"),
        "attempts_remaining": budget.get("attempts_remaining"),
        "cooldown_until": budget.get("cooldown_until"),
        "enhanced_observation_required": enhanced_observation_required,
        "resume_mode": resume_mode,
        "blockers": blockers,
        "warnings": warnings,
    }


def _evidence(inputs: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    control_plane_status = classify_control_plane_snapshot_status(inputs["control_plane_snapshot"])
    budget = _runtime_retry_budget(inputs["recovery_budget_ledger"])
    return {
        "control_plane_snapshot_id": inputs["control_plane_snapshot"].get("control_plane_snapshot_id"),
        "control_plane_snapshot_classification": _classification(inputs["control_plane_snapshot"]),
        "control_plane_snapshot_status": control_plane_status.get("classification"),
        "control_plane_snapshot_safe_to_start_runtime": control_plane_status.get("safe_to_start_runtime") is True,
        "control_plane_shared_truth_generation_id": inputs["control_plane_snapshot"].get("shared_truth_refresh_generation_id"),
        "proof_readiness_classification": _classification(inputs["proof_readiness"]),
        "shared_truth_classification": _classification(inputs["shared_truth"]),
        "shared_truth_classifications": _mapping(inputs["shared_truth"].get("classifications")),
        "runtime_environment_truth_classification": _classification(inputs["runtime_environment_truth"]),
        "agent_health_classification": _classification(inputs["agent_health"]),
        "self_recover_recommendation": str(inputs["self_recover_rules"].get("recommendation") or _classification(inputs["self_recover_rules"])),
        "crash_loop_classification": _classification(inputs["crash_loop_protection"]),
        "crash_loop_restart_blocked": inputs["crash_loop_protection"].get("restart_blocked") is True,
        "crash_loop_operator_ack_required": inputs["crash_loop_protection"].get("operator_ack_required") is True
        or _classification(inputs["crash_loop_protection"]) == OPERATOR_ACK_REQUIRED,
        "position_truth_classification": _position_truth_classification(inputs["position_truth"]),
        "open_order_truth_classification": _classification(inputs["open_order_truth"]),
        "managed_order_registry_classification": _classification(inputs["managed_order_registry"]),
        "managed_position_registry_classification": _classification(inputs["managed_position_registry"]),
        "reconciliation_classification": _classification(inputs["reconciliation"]),
        "broker_lease_classification": _classification(inputs["broker_lease"])
        or str(inputs["broker_lease"].get("lease_state") or ""),
        "stop_provenance": dict(_stop_provenance(inputs["stop_provenance"])),
        "paper_recovery_policy": _paper_policy_evidence(inputs["paper_recovery_policy"]),
        "paper_policy_severity": str(inputs["paper_recovery_policy"].get("severity") or ""),
        "paper_action_policy": str(inputs["paper_recovery_policy"].get("paper_action_policy") or ""),
        "paper_autonomous_recovery_allowed": inputs["paper_recovery_policy"].get("autonomous_recovery_allowed") is True,
        "paper_requires_operator_ack": inputs["paper_recovery_policy"].get("requires_operator_ack_for_paper") is True,
        "paper_bounded_recovery_budget": _mapping(inputs["paper_recovery_policy"].get("bounded_recovery_budget")),
        "recovery_budget_classification": _classification(inputs["recovery_budget_ledger"]),
        "runtime_retry_budget": budget,
        "live_money_eligible": _any_true(
            inputs["proof_readiness"],
            inputs["runtime_environment_truth"],
            inputs["self_recover_rules"],
            inputs["crash_loop_protection"],
            inputs["position_truth"],
            inputs["open_order_truth"],
            inputs["managed_order_registry"],
            inputs["managed_position_registry"],
            inputs["reconciliation"],
            inputs["paper_recovery_policy"],
            key="live_money_eligible",
        ),
        "runtime_writer_authority": str(inputs["runtime_environment_truth"].get("writer_authority") or ""),
        "duplicate_writer_count": int(inputs["runtime_environment_truth"].get("duplicate_writer_count") or 0),
    }


def _runtime_retry_budget(ledger: Mapping[str, Any]) -> dict[str, Any]:
    if not ledger:
        return {
            "budget_key": "",
            "attempts_used": None,
            "attempts_remaining": None,
            "budget_exhausted": False,
            "cooldown_until": None,
            "quarantine_required": False,
            "missing": True,
        }
    budget = budget_summary_for_action(
        ledger,
        agent_id="track_b_paper_runtime",
        action_type="RUNTIME_RETRY",
        target_identity={},
        failure_classification="runtime_retry",
    )
    budget["missing"] = False
    return budget


def _shared_truth_supports_new_generation(evidence: Mapping[str, Any]) -> bool:
    classifications = _mapping(evidence.get("shared_truth_classifications"))
    return (
        evidence["proof_readiness_classification"] == READY_FOR_PROOF
        and evidence["runtime_environment_truth_classification"] == RUNTIME_DOWN_CLEAN
        and evidence["position_truth_classification"] == "CLEAN_FLAT_READY"
        and evidence["open_order_truth_classification"] == NO_OPEN_ORDERS
        and evidence["managed_order_registry_classification"] == NO_MANAGED_ORDERS
        and evidence["managed_position_registry_classification"] == NO_MANAGED_POSITIONS
        and evidence["reconciliation_classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
        and evidence["broker_lease_classification"] in {"ACTIVE", "ACTIVE_DEGRADED_REFRESH_FAILING"}
        and classifications.get("Open Order Truth") == NO_OPEN_ORDERS
        and classifications.get("Managed Order Registry") == NO_MANAGED_ORDERS
        and classifications.get("Position Truth") == "CLEAN_FLAT_READY"
        and classifications.get("Runtime Environment Truth") == RUNTIME_DOWN_CLEAN
        and classifications.get("Managed Position Registry") == NO_MANAGED_POSITIONS
        and classifications.get("Reconciliation") == "TRACK_B_PAPER_BROKER_RECONCILED"
        and classifications.get("Broker Truth Lease") in {"ACTIVE", "ACTIVE_DEGRADED_REFRESH_FAILING"}
    )


def _generation_evidence(*, inputs: Mapping[str, Mapping[str, Any]], now: datetime) -> dict[str, Any]:
    stop = _stop_provenance(inputs["stop_provenance"])
    runtime = inputs["runtime_environment_truth"]
    control_plane = inputs["control_plane_snapshot"]
    previous_runtime_generation_id = (
        stop.get("runtime_generation_id")
        or stop.get("restart_generation")
        or runtime.get("runtime_generation_id")
        or runtime.get("restart_generation")
    )
    return {
        "previous_runtime_generation_id": previous_runtime_generation_id,
        "proposed_next_runtime_generation_id": _proposed_runtime_generation_id(now),
        "previous_source_commit": stop.get("source_commit") or runtime.get("source_commit"),
        "previous_control_plane_snapshot_id": stop.get("control_plane_snapshot_id")
        or control_plane.get("control_plane_snapshot_id"),
        "control_plane_snapshot_id": control_plane.get("control_plane_snapshot_id"),
        "shared_truth_refresh_generation_id": control_plane.get("shared_truth_refresh_generation_id")
        or inputs["shared_truth"].get("refresh_generation_id"),
    }


def _proposed_runtime_generation_id(value: datetime) -> str:
    return f"track-b-paper-runtime-generation-{value.strftime('%Y%m%dT%H%M%SZ')}"


def _missing_or_stale_evidence(evidence: Mapping[str, Any]) -> list[str]:
    missing: list[str] = []
    required = {
        "proof_readiness": evidence["proof_readiness_classification"],
        "runtime_environment_truth": evidence["runtime_environment_truth_classification"],
        "agent_health": evidence["agent_health_classification"],
        "crash_loop_protection": evidence["crash_loop_classification"],
        "paper_recovery_policy": evidence["paper_action_policy"],
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


def _market_closed(inputs: Mapping[str, Mapping[str, Any]]) -> bool:
    proof = inputs["proof_readiness"]
    if proof.get("classification") == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    if proof.get("phase1_session_reason") == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    self_recover = inputs["self_recover_rules"]
    if self_recover.get("recommendation") == "WAIT_MARKET_CLOSED":
        return True
    for agent in _list(inputs["agent_health"].get("agents")):
        if agent.get("agent_id") == "phase1_databento_live_candles" and agent.get("reason") == MARKET_CLOSED_NO_FRESH_BARS:
            return True
    return False


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


def _stop_provenance(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    nested = _mapping(payload.get("stop_provenance"))
    return nested or payload


def _position_truth_classification(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return str(payload.get("classification") or summary.get("overall_classification") or "")


def _blocker(code: str, detail: str) -> dict[str, str]:
    return {"code": code, "detail": detail}


def _resume_id(value: datetime) -> str:
    return f"track-b-paper-resume-{value.strftime('%Y%m%dT%H%M%SZ')}"


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


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _any_true(*payloads: Mapping[str, Any], key: str) -> bool:
    return any(payload.get(key) is True for payload in payloads)


if __name__ == "__main__":
    raise SystemExit(main())
