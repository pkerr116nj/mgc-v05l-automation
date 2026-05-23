"""Read-only Track B PAPER runtime supervisor authority.

Runtime Supervisor Authority lives in execution_core. Dashboard artifacts are
projections and must not be used as runtime, readiness, restart, broker, or
routing authority. This v1 service recommends what should happen next with the
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

from mgc_v05l.execution_core.track_b_agent_health import DEFAULT_AGENT_HEALTH_ARTIFACT
from mgc_v05l.execution_core.track_b_agent_registry import DEFAULT_AGENT_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_broker_truth_lease import DEFAULT_LEASE_ARTIFACT
from mgc_v05l.execution_core.track_b_crash_loop_protection import (
    DEFAULT_CRASH_LOOP_PROTECTION_ARTIFACT,
    OPERATOR_ACK_REQUIRED,
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
from mgc_v05l.execution_core.track_b_runtime_environment_truth import (
    DEFAULT_CANONICAL_READINESS_ARTIFACT,
    DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT,
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
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    broker_lease_path: Path = DEFAULT_LEASE_ARTIFACT
    stop_provenance_path: Path = DEFAULT_RUNTIME_STOP_PROVENANCE_ARTIFACT

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
        "reconciliation": _read_json(config.resolve(config.reconciliation_path)),
        "broker_lease": _read_json(config.resolve(config.broker_lease_path)),
        "stop_provenance": _read_json(config.resolve(config.stop_provenance_path)),
    }
    decision = _classify_supervisor(inputs=inputs)
    return {
        "schema_version": "track_b_runtime_supervisor_authority_v1",
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
        "recommended_action": decision["recommended_action"],
        "action_allowed": decision["action_allowed"],
        "operator_ack_required": decision["operator_ack_required"],
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
            "Add operator acknowledgement artifact checks once Runtime Resume Semantics v2 defines them.",
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
        "projection_only": True,
        "not_routing_authority": True,
        "source_authority_path": str(authority_path),
        "authority_owner": "execution_core",
        "operator_dashboard_display_only": True,
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
                    "recommended_action": payload.get("recommended_action"),
                    "action_allowed": payload.get("action_allowed"),
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

    if evidence["runtime_environment_truth_classification"] == RUNTIME_ACTIVE_TRADE_CAPABLE:
        return _decision(
            SUPERVISOR_RUNTIME_ALREADY_HEALTHY,
            "LEAVE_RUNTIME_RUNNING",
            "Runtime Environment Truth is RUNTIME_ACTIVE_TRADE_CAPABLE.",
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
    }
    if suspicious_order:
        return _decision(
            SUPERVISOR_MANUAL_REVIEW_REQUIRED,
            "MANUAL_REVIEW_REQUIRED",
            "Open Order Truth or Managed Order Registry indicates suspicious order state.",
            blockers=[
                _blocker("open_order_truth", evidence["open_order_truth_classification"]),
                _blocker("managed_order_registry", evidence["managed_order_registry_classification"]),
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
        return _decision(
            SUPERVISOR_RESTART_BLOCKED_OPERATOR_ACK,
            "HOLD_DOWN_OPERATOR_ACK_REQUIRED",
            "Runtime Resume Semantics or Crash Loop Protection requires operator acknowledgement.",
            operator_ack_required=True,
            blockers=[
                _blocker("runtime_resume", evidence["runtime_resume_classification"]),
                _blocker("crash_loop_protection", evidence["crash_loop_classification"]),
            ],
        )

    if evidence["crash_loop_restart_blocked"] is True or evidence["runtime_resume_classification"] == RESUME_BLOCKED_CRASH_LOOP:
        return _decision(
            SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP,
            "HOLD_DOWN_CRASH_LOOP",
            f"Crash Loop Protection is {evidence['crash_loop_classification']}.",
            blockers=[_blocker("crash_loop_protection", evidence["crash_loop_classification"])],
        )

    if _market_closed(inputs):
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


def _evidence(inputs: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "runtime_environment_truth_classification": _classification(inputs["runtime_environment_truth"]),
        "runtime_resume_classification": _classification(inputs["runtime_resume_semantics"]),
        "runtime_resume_allowed": inputs["runtime_resume_semantics"].get("allowed") is True,
        "runtime_resume_safe_to_start_runtime": inputs["runtime_resume_semantics"].get("safe_to_start_runtime") is True,
        "runtime_resume_required_operator_ack": inputs["runtime_resume_semantics"].get("required_operator_ack") is True,
        "self_recover_recommendation": str(inputs["self_recover_rules"].get("recommendation") or _classification(inputs["self_recover_rules"])),
        "crash_loop_classification": _classification(inputs["crash_loop_protection"]),
        "crash_loop_restart_blocked": inputs["crash_loop_protection"].get("restart_blocked") is True,
        "crash_loop_operator_ack_required": inputs["crash_loop_protection"].get("operator_ack_required") is True
        or _classification(inputs["crash_loop_protection"]) == OPERATOR_ACK_REQUIRED,
        "agent_health_classification": _classification(inputs["agent_health"]),
        "agent_registry_classification": _classification(inputs["agent_registry"]),
        "proof_readiness_classification": _classification(inputs["proof_readiness"]),
        "shared_truth_classification": _classification(inputs["shared_truth"]),
        "shared_truth_classifications": _mapping(inputs["shared_truth"].get("classifications")),
        "canonical_readiness": str(inputs["canonical_readiness"].get("canonical_readiness") or _classification(inputs["canonical_readiness"])),
        "position_truth_classification": _position_truth_classification(inputs["position_truth"]),
        "open_order_truth_classification": _classification(inputs["open_order_truth"]),
        "managed_order_registry_classification": _classification(inputs["managed_order_registry"]),
        "managed_position_registry_classification": _classification(inputs["managed_position_registry"]),
        "reconciliation_classification": _classification(inputs["reconciliation"]),
        "broker_lease_classification": _classification(inputs["broker_lease"])
        or str(inputs["broker_lease"].get("lease_state") or ""),
        "stop_provenance": dict(_stop_provenance(inputs["stop_provenance"])),
    }


def _missing_or_stale_evidence(evidence: Mapping[str, Any]) -> list[str]:
    missing: list[str] = []
    required = {
        "runtime_environment_truth": evidence["runtime_environment_truth_classification"],
        "runtime_resume_semantics": evidence["runtime_resume_classification"],
        "self_recover_rules": evidence["self_recover_recommendation"],
        "crash_loop_protection": evidence["crash_loop_classification"],
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


def _market_closed(inputs: Mapping[str, Mapping[str, Any]]) -> bool:
    proof = inputs["proof_readiness"]
    resume = inputs["runtime_resume_semantics"]
    self_recover = inputs["self_recover_rules"]
    if proof.get("classification") == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    if proof.get("phase1_session_reason") == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    if resume.get("classification") == RESUME_BLOCKED_MARKET_CLOSED or resume.get("reason") == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    if self_recover.get("recommendation") == "WAIT_MARKET_CLOSED":
        return True
    return False


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
        "reconciliation": str(config.resolve(config.reconciliation_path)),
        "broker_lease": str(config.resolve(config.broker_lease_path)),
        "stop_provenance": str(config.resolve(config.stop_provenance_path)),
    }


def _stop_provenance(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    nested = _mapping(payload.get("stop_provenance"))
    return nested or payload


def _position_truth_classification(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return str(payload.get("classification") or summary.get("overall_classification") or "")


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
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _classification(payload: Mapping[str, Any]) -> str:
    return str(payload.get("classification") or "")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


if __name__ == "__main__":
    raise SystemExit(main())
