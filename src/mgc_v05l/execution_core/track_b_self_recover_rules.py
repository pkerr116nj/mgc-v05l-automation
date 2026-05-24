"""Read-only Track B PAPER self-recover recommendation authority.

Self-Recover Rules authority lives in execution_core. Dashboard artifacts are
projections and must not be used as runtime, readiness, restart, broker, or
routing authority. This v1 engine recommends only; it never starts, stops,
restarts, submits, cancels, replaces, closes, or flattens anything.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_agent_health import (
    DEFAULT_AGENT_HEALTH_ARTIFACT,
    HEALTHY,
    MISSING,
    STALE,
    STOPPED_UNEXPECTED,
)
from mgc_v05l.execution_core.track_b_agent_registry import DEFAULT_AGENT_REGISTRY_ARTIFACT
from mgc_v05l.execution_core.track_b_managed_order_registry import NO_MANAGED_ORDERS
from mgc_v05l.execution_core.track_b_managed_position_registry import NO_MANAGED_POSITIONS
from mgc_v05l.execution_core.track_b_open_order_truth import NO_OPEN_ORDERS
from mgc_v05l.execution_core.track_b_paper_proof_readiness import READY_FOR_PROOF
from mgc_v05l.execution_core.track_b_projection_metadata import build_projection_metadata
from mgc_v05l.execution_core.track_b_runtime_environment_truth import (
    RUNTIME_DOWN_CLEAN,
    RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
)
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


NO_ACTION_NEEDED = "NO_ACTION_NEEDED"
WAIT_MARKET_CLOSED = "WAIT_MARKET_CLOSED"
REFRESH_SHARED_TRUTH = "REFRESH_SHARED_TRUTH"
RESTART_RUNTIME_ALLOWED = "RESTART_RUNTIME_ALLOWED"
RESTART_RUNTIME_BLOCKED = "RESTART_RUNTIME_BLOCKED"
RESTART_MARKET_DATA_PRODUCER_ALLOWED = "RESTART_MARKET_DATA_PRODUCER_ALLOWED"
RESTART_MARKET_DATA_PRODUCER_BLOCKED = "RESTART_MARKET_DATA_PRODUCER_BLOCKED"
OPERATOR_REVIEW_REQUIRED = "OPERATOR_REVIEW_REQUIRED"
MANUAL_TWS_REVIEW_REQUIRED = "MANUAL_TWS_REVIEW_REQUIRED"
CLEANUP_REQUIRED_BEFORE_RESTART = "CLEANUP_REQUIRED_BEFORE_RESTART"
DO_NOT_RECOVER_UNSAFE_STATE = "DO_NOT_RECOVER_UNSAFE_STATE"
CLEAN_FLAT_READY = "CLEAN_FLAT_READY"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SELF_RECOVER_RULES_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "self_recover" / "latest_self_recover_rules.json"
)
DEFAULT_DASHBOARD_SELF_RECOVER_RULES_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_self_recover_rules.json"
)
DEFAULT_SHARED_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "shared_truth" / "latest_track_b_shared_truth_refresh.json"
)
DEFAULT_PROOF_READINESS_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "proof_readiness" / "latest_track_b_paper_proof_readiness.json"
)
DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json"
)
DEFAULT_POSITION_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
)
DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
)
DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json"
)
DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
)
DEFAULT_RECONCILIATION_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_BROKER_LEASE_ARTIFACT = Path("outputs") / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"
DEFAULT_STOP_PROVENANCE_ARTIFACT = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "latest_runtime_stop_provenance.json"
)


@dataclass(frozen=True)
class TrackBSelfRecoverRulesConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_SELF_RECOVER_RULES_ARTIFACT
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_SELF_RECOVER_RULES_PROJECTION
    agent_registry_path: Path = DEFAULT_AGENT_REGISTRY_ARTIFACT
    agent_health_path: Path = DEFAULT_AGENT_HEALTH_ARTIFACT
    shared_truth_path: Path = DEFAULT_SHARED_TRUTH_ARTIFACT
    proof_readiness_path: Path = DEFAULT_PROOF_READINESS_ARTIFACT
    runtime_environment_truth_path: Path = DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    managed_order_registry_path: Path = DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    broker_lease_path: Path = DEFAULT_BROKER_LEASE_ARTIFACT
    stop_provenance_path: Path = DEFAULT_STOP_PROVENANCE_ARTIFACT

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_self_recover_rules(
    *,
    config: TrackBSelfRecoverRulesConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    inputs = {
        "agent_registry": _read_json(config.resolve(config.agent_registry_path)),
        "agent_health": _read_json(config.resolve(config.agent_health_path)),
        "shared_truth": _read_json(config.resolve(config.shared_truth_path)),
        "proof_readiness": _read_json(config.resolve(config.proof_readiness_path)),
        "runtime_environment_truth": _read_json(config.resolve(config.runtime_environment_truth_path)),
        "position_truth": _read_json(config.resolve(config.position_truth_path)),
        "open_order_truth": _read_json(config.resolve(config.open_order_truth_path)),
        "managed_order_registry": _read_json(config.resolve(config.managed_order_registry_path)),
        "managed_position_registry": _read_json(config.resolve(config.managed_position_registry_path)),
        "reconciliation": _read_json(config.resolve(config.reconciliation_path)),
        "broker_lease": _read_json(config.resolve(config.broker_lease_path)),
        "stop_provenance": _read_json(config.resolve(config.stop_provenance_path)),
    }
    decision = _recommendation(inputs)
    return {
        "schema_version": "track_b_self_recover_rules_v1",
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
        "classification": decision["recommendation"],
        "recommendation": decision["recommendation"],
        "allowed": decision["allowed"],
        "blocked": not decision["allowed"],
        "reason": decision["reason"],
        "evidence": _evidence(inputs),
        "input_artifacts": {
            "agent_registry": str(config.resolve(config.agent_registry_path)),
            "agent_health": str(config.resolve(config.agent_health_path)),
            "shared_truth": str(config.resolve(config.shared_truth_path)),
            "proof_readiness": str(config.resolve(config.proof_readiness_path)),
            "runtime_environment_truth": str(config.resolve(config.runtime_environment_truth_path)),
            "position_truth": str(config.resolve(config.position_truth_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
            "managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "broker_lease": str(config.resolve(config.broker_lease_path)),
            "stop_provenance": str(config.resolve(config.stop_provenance_path)),
        },
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "dashboard_projection": None
            if config.dashboard_projection_path is None
            else str(config.resolve(config.dashboard_projection_path)),
        },
        "todo_v2": [
            "Add crash-loop and repeated-same-class failure budgets.",
            "Attach operator approval workflow ids to allowed recovery recommendations.",
            "Converge self-healing executor on this policy before any automatic recovery action.",
        ],
    }


def write_track_b_self_recover_rules(
    *,
    config: TrackBSelfRecoverRulesConfig,
    payload: Mapping[str, Any],
) -> Path:
    authority_path = config.resolve(config.output_path)
    _write_json_atomic(authority_path, dict(payload))
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_self_recover_projection(authority_payload=payload, authority_path=authority_path),
        )
    return authority_path


def build_dashboard_self_recover_projection(*, authority_payload: Mapping[str, Any], authority_path: Path) -> dict[str, Any]:
    return {
        **dict(authority_payload),
        "schema_version": "track_b_self_recover_rules_dashboard_projection_v1",
        **build_projection_metadata(source_authority_path=authority_path),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write read-only Track B PAPER self-recover recommendations.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_SELF_RECOVER_RULES_ARTIFACT)
    parser.add_argument("--dashboard-projection-path", type=Path, default=DEFAULT_DASHBOARD_SELF_RECOVER_RULES_PROJECTION)
    parser.add_argument("--no-dashboard-projection", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBSelfRecoverRulesConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        dashboard_projection_path=None if bool(args.no_dashboard_projection) else Path(args.dashboard_projection_path),
    )
    payload = build_track_b_self_recover_rules(config=config)
    authority_path = write_track_b_self_recover_rules(config=config, payload=payload)
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            json.dumps(
                {
                    "recommendation": payload.get("recommendation"),
                    "allowed": payload.get("allowed"),
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
    return 0 if payload.get("recommendation") in {NO_ACTION_NEEDED, WAIT_MARKET_CLOSED, REFRESH_SHARED_TRUTH, RESTART_RUNTIME_ALLOWED, RESTART_MARKET_DATA_PRODUCER_ALLOWED} else 2


def _recommendation(inputs: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    open_order_class = _classification(inputs["open_order_truth"])
    managed_order_class = _classification(inputs["managed_order_registry"])
    position_class = _position_truth_classification(inputs["position_truth"])
    managed_position_class = _classification(inputs["managed_position_registry"])
    runtime_class = _classification(inputs["runtime_environment_truth"])
    proof_class = _classification(inputs["proof_readiness"])
    reconciliation_class = _classification(inputs["reconciliation"])
    agent_health = inputs["agent_health"]
    phase1_agent = _agent(agent_health, "phase1_databento_live_candles")
    stale_truth = _stale_truth_agents(agent_health)

    if open_order_class in {"SUSPICIOUS_ORDER_STATE", "DUPLICATE_CLOSE_ORDER", "UNKNOWN_OPEN_ORDER"}:
        return _decision(MANUAL_TWS_REVIEW_REQUIRED, False, f"Open Order Truth is {open_order_class}.")
    if managed_order_class in {
        "CLOSE_ORDER_SUSPICIOUS",
        "DUPLICATE_CLOSE_ORDER_BLOCKED",
        "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED",
        "BROKER_FLAT_WITH_WORKING_CLOSE",
    }:
        return _decision(MANUAL_TWS_REVIEW_REQUIRED, False, f"Managed Order Registry is {managed_order_class}.")
    if runtime_class == RUNTIME_DOWN_WITH_BROKER_EXPOSURE or position_class != CLEAN_FLAT_READY:
        return _decision(CLEANUP_REQUIRED_BEFORE_RESTART, False, f"Position Truth is {position_class}.")
    if open_order_class != NO_OPEN_ORDERS:
        return _decision(DO_NOT_RECOVER_UNSAFE_STATE, False, f"Open Order Truth is {open_order_class}.")
    if managed_order_class != NO_MANAGED_ORDERS:
        return _decision(DO_NOT_RECOVER_UNSAFE_STATE, False, f"Managed Order Registry is {managed_order_class}.")
    if managed_position_class != NO_MANAGED_POSITIONS:
        return _decision(CLEANUP_REQUIRED_BEFORE_RESTART, False, f"Managed Position Registry is {managed_position_class}.")
    if reconciliation_class not in {"TRACK_B_PAPER_BROKER_RECONCILED", ""}:
        return _decision(DO_NOT_RECOVER_UNSAFE_STATE, False, f"Reconciliation is {reconciliation_class}.")
    if _market_closed(inputs["proof_readiness"], phase1_agent):
        return _decision(WAIT_MARKET_CLOSED, True, MARKET_CLOSED_NO_FRESH_BARS)
    if _phase1_restart_allowed(phase1_agent, proof_class):
        return _decision(RESTART_MARKET_DATA_PRODUCER_ALLOWED, True, str(phase1_agent.get("reason") or "phase1_unhealthy"))
    if stale_truth:
        return _decision(REFRESH_SHARED_TRUTH, True, f"Stale/missing truth agents: {', '.join(stale_truth)}.")
    if runtime_class == RUNTIME_DOWN_CLEAN and proof_class == READY_FOR_PROOF:
        return _decision(RESTART_RUNTIME_ALLOWED, True, "Runtime is down clean and proof readiness is READY_FOR_PROOF.")
    if runtime_class == RUNTIME_DOWN_CLEAN:
        return _decision(RESTART_RUNTIME_BLOCKED, False, f"Runtime is down clean but proof readiness is {proof_class}.")
    return _decision(NO_ACTION_NEEDED, True, f"Runtime/environment classification is {runtime_class}.")


def _decision(recommendation: str, allowed: bool, reason: str) -> dict[str, Any]:
    return {"recommendation": recommendation, "allowed": allowed, "reason": reason}


def _classification(payload: Mapping[str, Any]) -> str:
    return str(payload.get("classification") or "")


def _position_truth_classification(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return str(payload.get("classification") or summary.get("overall_classification") or "")


def _market_closed(proof_readiness: Mapping[str, Any], phase1_agent: Mapping[str, Any]) -> bool:
    if proof_readiness.get("classification") == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    if proof_readiness.get("phase1_session_reason") == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    return phase1_agent.get("reason") == MARKET_CLOSED_NO_FRESH_BARS


def _phase1_restart_allowed(phase1_agent: Mapping[str, Any], proof_class: str) -> bool:
    if phase1_agent.get("status") not in {MISSING, STALE, STOPPED_UNEXPECTED}:
        return False
    return proof_class != MARKET_CLOSED_NO_FRESH_BARS


def _stale_truth_agents(agent_health: Mapping[str, Any]) -> list[str]:
    stale: list[str] = []
    for agent in _list(agent_health.get("agents")):
        if agent.get("category") != "truth_service":
            continue
        if agent.get("agent_id") in {"shared_truth_refresh"}:
            continue
        if agent.get("status") in {MISSING, STALE}:
            stale.append(str(agent.get("agent_id") or "unknown"))
    return stale


def _agent(agent_health: Mapping[str, Any], agent_id: str) -> Mapping[str, Any]:
    for agent in _list(agent_health.get("agents")):
        if agent.get("agent_id") == agent_id:
            return _mapping(agent)
    return {}


def _evidence(inputs: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    agent_health = inputs["agent_health"]
    return {
        "agent_registry_classification": _classification(inputs["agent_registry"]),
        "agent_health_classification": _classification(agent_health),
        "proof_readiness_classification": _classification(inputs["proof_readiness"]),
        "runtime_environment_truth_classification": _classification(inputs["runtime_environment_truth"]),
        "position_truth_classification": _position_truth_classification(inputs["position_truth"]),
        "open_order_truth_classification": _classification(inputs["open_order_truth"]),
        "managed_order_registry_classification": _classification(inputs["managed_order_registry"]),
        "managed_position_registry_classification": _classification(inputs["managed_position_registry"]),
        "reconciliation_classification": _classification(inputs["reconciliation"]),
        "broker_lease_classification": _classification(inputs["broker_lease"])
        or str(inputs["broker_lease"].get("lease_state") or ""),
        "stop_provenance": inputs["stop_provenance"],
        "stale_truth_agents": _stale_truth_agents(agent_health),
        "phase1_agent": dict(_agent(agent_health, "phase1_databento_live_candles")),
    }


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


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


if __name__ == "__main__":
    raise SystemExit(main())
