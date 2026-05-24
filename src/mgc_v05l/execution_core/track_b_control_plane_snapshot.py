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
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_projection_metadata import build_projection_metadata
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    SHARED_TRUTH_COHERENT,
    TrackBRuntimeSupervisorAuthorityConfig,
    build_track_b_runtime_supervisor_authority,
    write_track_b_runtime_supervisor_authority,
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

CONTROL_PLANE_SNAPSHOT_READY = "CONTROL_PLANE_SNAPSHOT_READY"
CONTROL_PLANE_SNAPSHOT_BLOCKED = "CONTROL_PLANE_SNAPSHOT_BLOCKED"
CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED = "CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED"


@dataclass(frozen=True)
class TrackBControlPlaneSnapshotConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_CONTROL_PLANE_SNAPSHOT_PROJECTION
    shared_truth_refresh_path: Path = DEFAULT_SHARED_TRUTH_REFRESH_ARTIFACT
    broker_lease_history_path: Path | None = None

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_control_plane_snapshot(
    *,
    config: TrackBControlPlaneSnapshotConfig,
    now: datetime | None = None,
    pid_running: Callable[[int], bool] | None = None,
    process_root_resolver: Callable[[int], Path | None] | None = None,
    source_commit_resolver: Callable[[Path], str | None] | None = None,
    post_shared_truth_refresh_hook: Callable[[Mapping[str, Any]], None] | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    shared_truth = refresh_track_b_shared_truth(
        config=TrackBSharedTruthRefreshConfig(
            repo_root=config.repo_root,
            shared_truth_refresh_path=config.shared_truth_refresh_path,
            broker_lease_history_path=config.broker_lease_history_path,
        ),
        now=actual_now,
        pid_running=pid_running,
        process_root_resolver=process_root_resolver,
        source_commit_resolver=source_commit_resolver,
    )
    if post_shared_truth_refresh_hook is not None:
        post_shared_truth_refresh_hook(shared_truth)

    supervisor_config = TrackBRuntimeSupervisorAuthorityConfig(
        repo_root=config.repo_root,
        dashboard_projection_path=None,
        shared_truth_path=config.shared_truth_refresh_path,
    )
    runtime_supervisor = build_track_b_runtime_supervisor_authority(
        config=supervisor_config,
        now=actual_now,
    )
    runtime_supervisor_path = write_track_b_runtime_supervisor_authority(
        config=supervisor_config,
        payload=runtime_supervisor,
    )
    return _snapshot_payload(
        config=config,
        now=actual_now,
        shared_truth=shared_truth,
        runtime_supervisor=runtime_supervisor,
        runtime_supervisor_path=runtime_supervisor_path,
    )


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
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBControlPlaneSnapshotConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        dashboard_projection_path=None if bool(args.no_dashboard_projection) else Path(args.dashboard_projection_path),
        broker_lease_history_path=None if bool(args.no_broker_lease_history) else None,
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
    runtime_supervisor: Mapping[str, Any],
    runtime_supervisor_path: Path,
) -> dict[str, Any]:
    coherence_status = str(runtime_supervisor.get("shared_truth_coherence_status") or "")
    generation_matches = (
        shared_truth.get("refresh_generation_id") == runtime_supervisor.get("shared_truth_refresh_generation_id")
    )
    classification = _snapshot_classification(
        coherence_status=coherence_status,
        generation_matches=generation_matches,
        supervisor_classification=str(runtime_supervisor.get("classification") or ""),
    )
    blockers = _snapshot_blockers(
        shared_truth=shared_truth,
        runtime_supervisor=runtime_supervisor,
        coherence_status=coherence_status,
        generation_matches=generation_matches,
    )
    warnings = list(shared_truth.get("warnings") or []) + list(runtime_supervisor.get("warnings") or [])
    evidence = _mapping(runtime_supervisor.get("evidence_summary"))
    return {
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
        "safe_to_start_runtime": runtime_supervisor.get("safe_to_start_runtime") is True,
        "paper_recovery_policy": evidence.get("paper_action_policy") or shared_truth.get("paper_recovery_policy"),
        "autonomous_recovery_plan_classification": runtime_supervisor.get(
            "autonomous_recovery_plan_classification"
        ),
        "autonomous_recovery_next_action": runtime_supervisor.get("autonomous_recovery_next_action"),
        "autonomous_recovery_execution_enabled": False,
        "broker_order_position_summary": _broker_order_position_summary(shared_truth, runtime_supervisor),
        "blockers": blockers,
        "warnings": warnings,
        "source_artifact_paths": {
            "shared_truth_refresh": str(config.resolve(config.shared_truth_refresh_path)),
            "runtime_supervisor_authority": str(runtime_supervisor_path),
            "control_plane_snapshot": str(config.resolve(config.output_path)),
            **_mapping(shared_truth.get("artifact_paths")),
        },
    }


def _snapshot_classification(
    *,
    coherence_status: str,
    generation_matches: bool,
    supervisor_classification: str,
) -> str:
    if coherence_status != SHARED_TRUTH_COHERENT or not generation_matches:
        return CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED
    if supervisor_classification in {"SUPERVISOR_RUNTIME_START_ALLOWED", "SUPERVISOR_WAIT_MARKET_CLOSED"}:
        return CONTROL_PLANE_SNAPSHOT_READY
    return CONTROL_PLANE_SNAPSHOT_BLOCKED


def _snapshot_blockers(
    *,
    shared_truth: Mapping[str, Any],
    runtime_supervisor: Mapping[str, Any],
    coherence_status: str,
    generation_matches: bool,
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
    return blockers


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
    }


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


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
