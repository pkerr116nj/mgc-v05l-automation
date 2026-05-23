"""Read-only Track B PAPER agent registry authority.

Agent Registry authority lives in execution_core. Dashboard artifacts are
projections and must not be used as runtime, readiness, restart, broker, or
routing authority. This v1 registry is declarative only; it never starts,
stops, restarts, submits, cancels, replaces, closes, or flattens anything.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


AGENT_REGISTRY_READY = "AGENT_REGISTRY_READY"
AGENT_REGISTRY_INCOMPLETE = "AGENT_REGISTRY_INCOMPLETE"
AGENT_REGISTRY_STALE = "AGENT_REGISTRY_STALE"
AGENT_REGISTRY_ERROR = "AGENT_REGISTRY_ERROR"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_AGENT_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "agent_registry" / "latest_agent_registry.json"
)
DEFAULT_DASHBOARD_AGENT_REGISTRY_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_agent_registry.json"
)

REQUIRED_PROOF_AGENT_IDS = {
    "track_b_paper_runtime",
    "phase1_databento_live_candles",
    "open_order_truth",
    "managed_order_registry",
    "position_truth",
    "runtime_environment_truth",
    "managed_position_registry",
    "shared_truth_refresh",
    "proof_readiness",
    "canonical_readiness_refresher",
    "broker_truth_lease_refresher",
}

REQUIRED_RUNTIME_SUBMIT_AGENT_IDS = {
    "track_b_paper_runtime",
    "phase1_databento_live_candles",
    "open_order_truth",
    "managed_order_registry",
    "position_truth",
    "runtime_environment_truth",
    "managed_position_registry",
    "canonical_readiness_refresher",
    "broker_truth_lease_refresher",
}


@dataclass(frozen=True)
class TrackBAgentRegistryConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_AGENT_REGISTRY_ARTIFACT
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_AGENT_REGISTRY_PROJECTION
    expected_proof_agent_ids: frozenset[str] = frozenset(REQUIRED_PROOF_AGENT_IDS)
    expected_runtime_submit_agent_ids: frozenset[str] = frozenset(REQUIRED_RUNTIME_SUBMIT_AGENT_IDS)

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_agent_registry(
    *,
    config: TrackBAgentRegistryConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    agents = [_agent_payload(agent, repo_root=config.repo_root) for agent in _agent_definitions()]
    classification, blockers = _classify_registry(
        agents=agents,
        expected_proof_agent_ids=config.expected_proof_agent_ids,
        expected_runtime_submit_agent_ids=config.expected_runtime_submit_agent_ids,
    )
    return {
        "schema_version": "track_b_agent_registry_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "authority_owner": "execution_core",
        "projection_only": False,
        "classification": classification,
        "blockers": blockers,
        "agents": agents,
        "required_for_proof_agent_ids": sorted(config.expected_proof_agent_ids),
        "required_for_runtime_submit_agent_ids": sorted(config.expected_runtime_submit_agent_ids),
        "summary": {
            "classification": classification,
            "agent_count": len(agents),
            "required_for_proof_count": sum(1 for agent in agents if agent.get("required_for_proof") is True),
            "required_for_runtime_submit_count": sum(
                1 for agent in agents if agent.get("required_for_runtime_submit") is True
            ),
            "diagnostic_agent_count": sum(1 for agent in agents if agent.get("category") == "diagnostic"),
        },
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "dashboard_projection": None
            if config.dashboard_projection_path is None
            else str(config.resolve(config.dashboard_projection_path)),
        },
    }


def write_track_b_agent_registry(
    *,
    config: TrackBAgentRegistryConfig,
    payload: Mapping[str, Any],
) -> Path:
    authority_path = config.resolve(config.output_path)
    _write_json_atomic(authority_path, dict(payload))
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_agent_registry_projection(authority_payload=payload, authority_path=authority_path),
        )
    return authority_path


def build_dashboard_agent_registry_projection(
    *,
    authority_payload: Mapping[str, Any],
    authority_path: Path,
) -> dict[str, Any]:
    return {
        **dict(authority_payload),
        "schema_version": "track_b_agent_registry_dashboard_projection_v1",
        "projection_only": True,
        "not_routing_authority": True,
        "source_authority_path": str(authority_path),
        "authority_owner": "execution_core",
        "operator_dashboard_display_only": True,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write the read-only Track B PAPER agent registry authority artifact.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_AGENT_REGISTRY_ARTIFACT)
    parser.add_argument("--dashboard-projection-path", type=Path, default=DEFAULT_DASHBOARD_AGENT_REGISTRY_PROJECTION)
    parser.add_argument("--no-dashboard-projection", action="store_true")
    parser.add_argument("--json", action="store_true", help="Print the full registry artifact as JSON.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBAgentRegistryConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        dashboard_projection_path=None if bool(args.no_dashboard_projection) else Path(args.dashboard_projection_path),
    )
    payload = build_track_b_agent_registry(config=config)
    authority_path = write_track_b_agent_registry(config=config, payload=payload)
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            json.dumps(
                {
                    "classification": payload.get("classification"),
                    "agent_count": payload.get("summary", {}).get("agent_count"),
                    "authority_path": str(authority_path),
                    "read_only": True,
                    "paper_proof_invoked": False,
                    "live_money_eligible": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0 if payload.get("classification") == AGENT_REGISTRY_READY else 2


def _agent_definitions() -> list[dict[str, Any]]:
    return [
        {
            "agent_id": "track_b_paper_runtime",
            "display_name": "Track B PAPER runtime",
            "category": "runtime",
            "required_for_proof": True,
            "required_for_runtime_submit": True,
            "authority_scope": "PAPER runtime loop and strategy-managed submit path after gates pass",
            "command": "bash scripts/run_probationary_paper_soak.sh",
            "module": "mgc_v05l.app.main probationary-paper-soak",
            "expected_artifact_paths": [
                "outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_truth.json",
                "outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid.json",
                "outputs/probationary_pattern_engine/paper_session/operator_status.json",
            ],
            "heartbeat_artifact_path": "outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_truth.json",
            "owner_root_expectation": "Dev root",
            "restart_policy_id": "track_b_paper_runtime_restart_policy_v1",
            "health_contract_id": "TODO_track_b_paper_runtime_health_contract_v1",
            "dependencies": [
                "phase1_databento_live_candles",
                "shared_truth_refresh",
                "proof_readiness",
                "canonical_readiness_refresher",
                "broker_truth_lease_refresher",
            ],
        },
        {
            "agent_id": "phase1_databento_live_candles",
            "display_name": "Phase-1 Databento live supervisor/listener",
            "category": "market_data",
            "required_for_proof": True,
            "required_for_runtime_submit": True,
            "authority_scope": "runtime candle freshness evidence for MGC/MNQ 1m/5m",
            "command": "bash scripts/start-phase1-databento-live-candles",
            "module": "mgc_v05l.execution_core.phase1_runtime_data_readiness",
            "expected_artifact_paths": [
                "outputs/track_b_execution_core/phase1_runtime_market_data/MGC/1m/latest_runtime_candles.json",
                "outputs/track_b_execution_core/phase1_runtime_market_data/MGC/5m/latest_runtime_candles.json",
                "outputs/track_b_execution_core/phase1_runtime_market_data/MNQ/1m/latest_runtime_candles.json",
                "outputs/track_b_execution_core/phase1_runtime_market_data/MNQ/5m/latest_runtime_candles.json",
            ],
            "heartbeat_artifact_path": "outputs/reports/phase1_runtime_data_readiness/latest_phase1_runtime_data_readiness.json",
            "owner_root_expectation": "Dev root",
            "restart_policy_id": "phase1_candle_supervisor_restart_policy_v1",
            "health_contract_id": "TODO_phase1_market_data_health_contract_v1",
            "dependencies": [],
        },
        _truth_service(
            agent_id="open_order_truth",
            display_name="Open Order Truth",
            module="mgc_v05l.execution_core.track_b_open_order_truth",
            artifact="outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
            dependencies=["broker_truth_lease_refresher"],
            required_for_submit=True,
        ),
        _truth_service(
            agent_id="managed_order_registry",
            display_name="Managed Order Registry",
            module="mgc_v05l.execution_core.track_b_managed_order_registry",
            artifact="outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
            dependencies=["open_order_truth", "managed_position_registry"],
            required_for_submit=True,
        ),
        _truth_service(
            agent_id="position_truth",
            display_name="Position Truth monitor",
            module="mgc_v05l.execution_core.track_b_position_truth_monitor",
            artifact="outputs/track_b_execution_core/position_truth/latest_position_truth.json",
            dependencies=["open_order_truth", "managed_order_registry", "broker_truth_lease_refresher"],
            required_for_submit=True,
        ),
        _truth_service(
            agent_id="runtime_environment_truth",
            display_name="Runtime Environment Truth",
            module="mgc_v05l.execution_core.track_b_runtime_environment_truth",
            artifact="outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json",
            dependencies=["position_truth"],
            required_for_submit=True,
        ),
        _truth_service(
            agent_id="managed_position_registry",
            display_name="Managed Position Registry",
            module="mgc_v05l.execution_core.track_b_managed_position_registry",
            artifact="outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
            dependencies=["position_truth", "open_order_truth", "managed_order_registry"],
            required_for_submit=True,
        ),
        {
            "agent_id": "shared_truth_refresh",
            "display_name": "Shared Truth Refresh",
            "category": "truth_service",
            "required_for_proof": True,
            "required_for_runtime_submit": False,
            "authority_scope": "one-shot refresh orchestration for execution_core shared truth",
            "command": "./.venv/bin/python -m mgc_v05l.execution_core.track_b_shared_truth_refresh_cli",
            "module": "mgc_v05l.execution_core.track_b_shared_truth_refresh_cli",
            "expected_artifact_paths": [],
            "heartbeat_artifact_path": None,
            "owner_root_expectation": "Dev root",
            "restart_policy_id": None,
            "health_contract_id": "TODO_shared_truth_refresh_health_contract_v1",
            "dependencies": [
                "open_order_truth",
                "managed_order_registry",
                "position_truth",
                "runtime_environment_truth",
                "managed_position_registry",
                "broker_truth_lease_refresher",
            ],
            "projection_only": False,
        },
        {
            "agent_id": "proof_readiness",
            "display_name": "Proof Readiness",
            "category": "readiness",
            "required_for_proof": True,
            "required_for_runtime_submit": False,
            "authority_scope": "supervised PAPER proof preflight authority",
            "command": "./.venv/bin/python -m mgc_v05l.execution_core.track_b_paper_proof_readiness",
            "module": "mgc_v05l.execution_core.track_b_paper_proof_readiness",
            "expected_artifact_paths": [
                "outputs/track_b_execution_core/proof_readiness/latest_track_b_paper_proof_readiness.json"
            ],
            "heartbeat_artifact_path": None,
            "owner_root_expectation": "Dev root",
            "restart_policy_id": None,
            "health_contract_id": "TODO_proof_readiness_health_contract_v1",
            "dependencies": ["shared_truth_refresh", "phase1_databento_live_candles"],
            "projection_only": False,
        },
        {
            "agent_id": "position_truth_notifier",
            "display_name": "Position Truth notifier",
            "category": "notifier",
            "required_for_proof": False,
            "required_for_runtime_submit": False,
            "authority_scope": "operator notification sidecar consuming Position Truth events",
            "command": "./.venv/bin/python -m mgc_v05l.app.track_b_position_truth_notifier --service",
            "module": "mgc_v05l.app.track_b_position_truth_notifier",
            "expected_artifact_paths": [
                "outputs/operator_dashboard/runtime/track_b_notifications.jsonl",
                "outputs/operator_dashboard/runtime/track_b_notification_sidecar_state.json",
            ],
            "heartbeat_artifact_path": "outputs/operator_dashboard/runtime/track_b_notification_sidecar_state.json",
            "owner_root_expectation": "Dev root",
            "restart_policy_id": None,
            "health_contract_id": "TODO_position_truth_notifier_health_contract_v1",
            "dependencies": ["position_truth"],
            "projection_only": False,
        },
        {
            "agent_id": "self_healing_supervisor",
            "display_name": "Self-healing supervisor",
            "category": "supervisor",
            "required_for_proof": False,
            "required_for_runtime_submit": False,
            "authority_scope": "read-only restart eligibility diagnostics and approved sidecar recovery planning",
            "command": "bash scripts/status-track-b-self-healing-supervisor",
            "module": "mgc_v05l.app.track_b_self_healing_supervisor",
            "expected_artifact_paths": ["outputs/operator_dashboard/runtime/latest_track_b_self_healing_health.json"],
            "heartbeat_artifact_path": "outputs/operator_dashboard/runtime/latest_track_b_self_healing_health.json",
            "owner_root_expectation": "Dev root",
            "restart_policy_id": "track_b_self_healing_restart_policy_v1",
            "health_contract_id": "TODO_self_healing_supervisor_health_contract_v1",
            "dependencies": ["shared_truth_refresh", "runtime_environment_truth"],
            "projection_only": False,
        },
        {
            "agent_id": "canonical_readiness_refresher",
            "display_name": "Canonical readiness refresher",
            "category": "readiness",
            "required_for_proof": True,
            "required_for_runtime_submit": True,
            "authority_scope": "canonical PAPER submit readiness evidence",
            "command": "bash scripts/start-track-b-operator-readiness-refresh",
            "module": "mgc_v05l.app.track_b_canonical_readiness",
            "expected_artifact_paths": [
                "outputs/operator_dashboard/runtime/latest_canonical_readiness.json",
                "outputs/operator_dashboard/runtime/latest_canonical_readiness_summary.json",
            ],
            "heartbeat_artifact_path": "var/track_b_operator_readiness_refresh_heartbeat.json",
            "owner_root_expectation": "Dev root",
            "restart_policy_id": "operator_readiness_refresher_restart_policy_v1",
            "health_contract_id": "TODO_canonical_readiness_refresher_health_contract_v1",
            "dependencies": ["shared_truth_refresh", "proof_readiness"],
            "projection_only": False,
        },
        {
            "agent_id": "broker_truth_lease_refresher",
            "display_name": "Broker truth/lease refresher",
            "category": "readiness",
            "required_for_proof": True,
            "required_for_runtime_submit": True,
            "authority_scope": "read-only IBKR broker truth refresh and broker lease evidence",
            "command": "bash scripts/start-track-b-broker-truth-refresh",
            "module": "mgc_v05l.app.ibkr_broker_truth_refresher",
            "expected_artifact_paths": [
                "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json",
                "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json",
            ],
            "heartbeat_artifact_path": "var/track_b_broker_truth_refresh_service.pid",
            "owner_root_expectation": "Dev root",
            "restart_policy_id": "broker_truth_refresher_restart_policy_v1",
            "health_contract_id": "TODO_broker_truth_refresher_health_contract_v1",
            "dependencies": [],
            "projection_only": False,
        },
        {
            "agent_id": "legacy_phase1_gc_panel",
            "display_name": "Legacy GC/Phase-1 diagnostic panel",
            "category": "diagnostic",
            "required_for_proof": False,
            "required_for_runtime_submit": False,
            "authority_scope": "diagnostic display only; superseded by shared truth and proof readiness",
            "command": None,
            "module": "mgc_v05l.app.operator_dashboard",
            "expected_artifact_paths": [],
            "heartbeat_artifact_path": None,
            "owner_root_expectation": "Dev root",
            "restart_policy_id": None,
            "health_contract_id": "TODO_legacy_diagnostic_panel_health_contract_v1",
            "dependencies": ["proof_readiness", "shared_truth_refresh"],
            "diagnostic_only": True,
            "not_routing_authority": True,
            "projection_only": False,
        },
    ]


def _truth_service(
    *,
    agent_id: str,
    display_name: str,
    module: str,
    artifact: str,
    dependencies: list[str],
    required_for_submit: bool,
) -> dict[str, Any]:
    return {
        "agent_id": agent_id,
        "display_name": display_name,
        "category": "truth_service",
        "required_for_proof": True,
        "required_for_runtime_submit": required_for_submit,
        "authority_scope": f"{display_name} execution_core authority artifact",
        "command": f"./.venv/bin/python -m {module}",
        "module": module,
        "expected_artifact_paths": [artifact],
        "heartbeat_artifact_path": artifact,
        "owner_root_expectation": "Dev root",
        "restart_policy_id": None,
        "health_contract_id": f"TODO_{agent_id}_health_contract_v1",
        "dependencies": dependencies,
        "projection_only": False,
    }


def _agent_payload(agent: Mapping[str, Any], *, repo_root: Path) -> dict[str, Any]:
    expected_paths = [str(_resolve_display_path(repo_root, Path(str(path)))) for path in _list(agent.get("expected_artifact_paths"))]
    heartbeat = agent.get("heartbeat_artifact_path")
    return {
        **dict(agent),
        "expected_artifact_paths": expected_paths,
        "heartbeat_artifact_path": None
        if heartbeat in (None, "")
        else str(_resolve_display_path(repo_root, Path(str(heartbeat)))),
        "projection_only": bool(agent.get("projection_only", False)),
    }


def _classify_registry(
    *,
    agents: Sequence[Mapping[str, Any]],
    expected_proof_agent_ids: set[str] | frozenset[str],
    expected_runtime_submit_agent_ids: set[str] | frozenset[str],
) -> tuple[str, list[dict[str, Any]]]:
    ids = [str(agent.get("agent_id") or "") for agent in agents]
    id_set = set(ids)
    duplicate_ids = sorted({agent_id for agent_id in ids if ids.count(agent_id) > 1})
    missing_proof = sorted(set(expected_proof_agent_ids) - id_set)
    missing_runtime_submit = sorted(set(expected_runtime_submit_agent_ids) - id_set)
    blockers: list[dict[str, Any]] = []
    if duplicate_ids:
        blockers.append({"code": "duplicate_agent_ids", "agent_ids": duplicate_ids})
    if missing_proof:
        blockers.append({"code": "missing_required_proof_agents", "agent_ids": missing_proof})
    if missing_runtime_submit:
        blockers.append({"code": "missing_required_runtime_submit_agents", "agent_ids": missing_runtime_submit})
    classification = AGENT_REGISTRY_READY if not blockers else AGENT_REGISTRY_INCOMPLETE
    return classification, blockers


def _resolve_display_path(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


if __name__ == "__main__":
    raise SystemExit(main())
