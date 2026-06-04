"""Read-only Track B PAPER authority refresh heartbeat.

The heartbeat keeps submit-authority artifacts fresh while the PAPER runtime is
alive. It never submits, modifies, cancels, closes, flattens, restarts, or
mutates broker state.
"""

from __future__ import annotations

import argparse
import errno
import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution.ibkr_paper_strategy_bridge import IbkrPaperStrategyBridgeConfig
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_control_plane_snapshot import (
    DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT,
    TrackBControlPlaneSnapshotConfig,
    build_track_b_control_plane_snapshot,
    write_track_b_control_plane_snapshot,
)
from mgc_v05l.execution_core.track_b_order_adjustment_planner import (
    DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT,
    TrackBOrderAdjustmentPlannerConfig,
    build_track_b_order_adjustment_plan,
    write_track_b_order_adjustment_plan,
)
from mgc_v05l.execution_core.track_b_paper_autonomous_recovery_planner import (
    DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT,
    TrackBPaperAutonomousRecoveryPlannerConfig,
    build_track_b_paper_autonomous_recovery_plan,
    write_track_b_paper_autonomous_recovery_plan,
)
from mgc_v05l.execution_core.track_b_readiness_state import (
    DEFAULT_CANONICAL_READINESS_ARTIFACT,
    write_canonical_readiness_artifact,
)
from mgc_v05l.execution_core.track_b_runtime_safe_state_envelope import (
    DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT,
    TrackBRuntimeSupervisorAuthorityConfig,
    build_track_b_runtime_supervisor_authority,
    write_track_b_runtime_supervisor_authority,
)
from mgc_v05l.execution_core.track_b_shared_truth_refresh_cli import (
    DEFAULT_SHARED_TRUTH_REFRESH_ARTIFACT,
    TrackBSharedTruthRefreshConfig,
    refresh_track_b_shared_truth,
)


AUTHORITY_REFRESH_INTERVAL_SECONDS = 90.0
BRIDGE_PRE_ACTION_MAX_AGE_SECONDS = float(
    IbkrPaperStrategyBridgeConfig.__dataclass_fields__["pre_action_snapshot_max_age_seconds"].default
)
SCHEMA_VERSION = "track_b_authority_refresh_heartbeat_v1"

AUTHORITY_REFRESHED = "AUTHORITY_REFRESHED"
AUTHORITY_REFRESH_FAILED = "AUTHORITY_REFRESH_FAILED"
AUTHORITY_REFRESH_SKIPPED_NOT_DUE = "AUTHORITY_REFRESH_SKIPPED_NOT_DUE"
AUTHORITY_REFRESH_SKIPPED_RUNTIME_INACTIVE = "AUTHORITY_REFRESH_SKIPPED_RUNTIME_INACTIVE"

DEFAULT_AUTHORITY_REFRESH_HEARTBEAT_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "authority_refresh" / "latest_authority_refresh_heartbeat.json"
)
DEFAULT_AUTHORITY_REFRESH_EVENTS_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "authority_refresh" / "authority_refresh_events.jsonl"
)
DEFAULT_PAPER_RUNTIME_TRUTH_ARTIFACT = (
    Path("outputs")
    / "probationary_pattern_engine"
    / "paper_session"
    / "runtime"
    / "paper_runtime_truth.json"
)
DEFAULT_PAPER_OPERATOR_STATUS_ARTIFACT = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "operator_status.json"
)


@dataclass(frozen=True)
class TrackBAuthorityRefreshHeartbeatConfig:
    repo_root: Path
    output_path: Path = DEFAULT_AUTHORITY_REFRESH_HEARTBEAT_ARTIFACT
    events_path: Path = DEFAULT_AUTHORITY_REFRESH_EVENTS_ARTIFACT
    runtime_truth_path: Path = DEFAULT_PAPER_RUNTIME_TRUTH_ARTIFACT
    operator_status_path: Path = DEFAULT_PAPER_OPERATOR_STATUS_ARTIFACT
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    runtime_safe_state_envelope_path: Path = DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
    runtime_supervisor_authority_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT
    shared_truth_refresh_path: Path = DEFAULT_SHARED_TRUTH_REFRESH_ARTIFACT
    canonical_readiness_path: Path = DEFAULT_CANONICAL_READINESS_ARTIFACT
    order_adjustment_plan_path: Path = DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT
    paper_autonomous_recovery_plan_path: Path = DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT
    interval_seconds: float = AUTHORITY_REFRESH_INTERVAL_SECONDS
    bridge_max_age_seconds: float = BRIDGE_PRE_ACTION_MAX_AGE_SECONDS

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def refresh_track_b_paper_authority_if_due(
    *,
    config: TrackBAuthorityRefreshHeartbeatConfig,
    now: datetime | None = None,
    force: bool = False,
    runtime_active: bool | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    previous = _read_json(config.resolve(config.output_path))
    runtime_truth = _read_json(config.resolve(config.runtime_truth_path))
    runtime_is_active = _runtime_active(runtime_truth) if runtime_active is None else bool(runtime_active)
    latest_successful_refresh_at = previous.get("latest_successful_refresh_at")

    if not runtime_is_active:
        payload = _base_payload(
            config=config,
            now=actual_now,
            classification=AUTHORITY_REFRESH_SKIPPED_RUNTIME_INACTIVE,
            latest_successful_refresh_at=latest_successful_refresh_at,
            reason_codes=["RUNTIME_INACTIVE"],
            runtime_truth=runtime_truth,
        )
        return _write_heartbeat(config=config, payload=payload)

    control_plane = _read_json(config.resolve(config.control_plane_snapshot_path))
    control_plane_age = _artifact_age_seconds(control_plane, actual_now)
    canonical_readiness = _read_json(config.resolve(config.canonical_readiness_path))
    canonical_readiness_state = str(
        canonical_readiness.get("canonical_readiness") or canonical_readiness.get("state") or ""
    )
    last_success_age = _age_seconds(latest_successful_refresh_at, actual_now)
    due = (
        force
        or last_success_age is None
        or last_success_age >= float(config.interval_seconds)
        or control_plane_age is None
        or control_plane_age >= float(config.interval_seconds)
        or canonical_readiness_state
        in {"BLOCKED_STALE_TRUTH", "NOT_READY_DEPENDENCY", "NOT_READY_RECONCILIATION", "BLOCKED_INFRASTRUCTURE"}
    )
    if not due:
        payload = _base_payload(
            config=config,
            now=actual_now,
            classification=AUTHORITY_REFRESH_SKIPPED_NOT_DUE,
            latest_successful_refresh_at=latest_successful_refresh_at,
            reason_codes=[],
            runtime_truth=runtime_truth,
            control_plane_age_seconds=control_plane_age,
        )
        return _write_heartbeat(config=config, payload=payload)

    substage_durations: list[dict[str, Any]] = []

    def _timed_substage(
        name: str,
        fn,
        *,
        mandatory_each_refresh: bool = True,
        cache_or_throttle_candidate: bool = False,
        move_off_hot_path_candidate: bool = False,
        touches_broker_tws: bool = False,
        scans_historical_artifacts: bool = False,
        can_delay_submit_capable_readiness: bool = True,
    ):
        started = time.perf_counter()
        try:
            return fn()
        finally:
            substage_durations.append(
                {
                    "substage": name,
                    "duration_seconds": round(max(time.perf_counter() - started, 0.0), 6),
                    "mandatory_each_refresh": mandatory_each_refresh,
                    "cache_or_throttle_candidate": cache_or_throttle_candidate,
                    "move_off_hot_path_candidate": move_off_hot_path_candidate,
                    "touches_broker_tws": touches_broker_tws,
                    "scans_historical_artifacts": scans_historical_artifacts,
                    "can_delay_submit_capable_readiness": can_delay_submit_capable_readiness,
                }
            )

    refresh_stage = "shared_truth"
    try:
        shared_truth = _timed_substage(
            "shared_truth_refresh",
            lambda: refresh_track_b_shared_truth(
                config=TrackBSharedTruthRefreshConfig(
                    repo_root=config.repo_root,
                    shared_truth_refresh_path=config.shared_truth_refresh_path,
                    broker_lease_history_path=None,
                ),
                now=actual_now,
            ),
            cache_or_throttle_candidate=True,
            move_off_hot_path_candidate=True,
            scans_historical_artifacts=True,
        )
        refresh_stage = "order_adjustment_planner"
        planner_config = TrackBOrderAdjustmentPlannerConfig(
            repo_root=config.repo_root,
            output_path=config.order_adjustment_plan_path,
        )
        planner = _timed_substage(
            "order_adjustment_planner_build",
            lambda: build_track_b_order_adjustment_plan(
                config=planner_config,
                now=actual_now,
                shared_truth_refresh=shared_truth,
            ),
            cache_or_throttle_candidate=True,
            move_off_hot_path_candidate=True,
        )
        planner_path = _timed_substage(
            "order_adjustment_planner_write",
            lambda: write_track_b_order_adjustment_plan(config=planner_config, payload=planner),
        )

        refresh_stage = "control_plane_safe_state"
        control_plane_config = TrackBControlPlaneSnapshotConfig(
            repo_root=config.repo_root,
            output_path=config.control_plane_snapshot_path,
            shared_truth_refresh_path=config.shared_truth_refresh_path,
            runtime_safe_state_envelope_path=config.runtime_safe_state_envelope_path,
            dashboard_projection_path=None,
            broker_lease_history_path=None,
        )
        control_plane = _timed_substage(
            "control_plane_snapshot_build",
            lambda: build_track_b_control_plane_snapshot(config=control_plane_config, now=actual_now),
            cache_or_throttle_candidate=True,
            move_off_hot_path_candidate=True,
            scans_historical_artifacts=True,
        )
        control_plane_path = _timed_substage(
            "control_plane_snapshot_write",
            lambda: write_track_b_control_plane_snapshot(
                config=control_plane_config,
                payload=control_plane,
            ),
        )
        refresh_stage = "paper_autonomous_recovery_planner"
        recovery_plan_config = TrackBPaperAutonomousRecoveryPlannerConfig(
            repo_root=config.repo_root,
            output_path=config.paper_autonomous_recovery_plan_path,
            control_plane_snapshot_path=config.control_plane_snapshot_path,
        )
        recovery_plan = _timed_substage(
            "paper_autonomous_recovery_planner_build",
            lambda: build_track_b_paper_autonomous_recovery_plan(
                config=recovery_plan_config,
                now=actual_now,
            ),
            cache_or_throttle_candidate=True,
            move_off_hot_path_candidate=True,
        )
        recovery_plan_path = _timed_substage(
            "paper_autonomous_recovery_planner_write",
            lambda: write_track_b_paper_autonomous_recovery_plan(
                config=recovery_plan_config,
                payload=recovery_plan,
            ),
        )
        shared_truth = _timed_substage(
            "shared_truth_recovery_plan_alignment_write",
            lambda: _align_shared_truth_recovery_plan_row(
                config=config,
                shared_truth=shared_truth,
                recovery_plan=recovery_plan,
                recovery_plan_path=recovery_plan_path,
            ),
            cache_or_throttle_candidate=True,
            move_off_hot_path_candidate=True,
        )
        refresh_stage = "runtime_supervisor_authority"
        supervisor_config = TrackBRuntimeSupervisorAuthorityConfig(
            repo_root=config.repo_root,
            output_path=config.runtime_supervisor_authority_path,
            dashboard_projection_path=None,
            shared_truth_path=config.shared_truth_refresh_path,
            runtime_safe_state_envelope_path=config.runtime_safe_state_envelope_path,
        )
        runtime_supervisor = _timed_substage(
            "runtime_supervisor_authority_build",
            lambda: build_track_b_runtime_supervisor_authority(
                config=supervisor_config,
                now=actual_now,
            ),
            cache_or_throttle_candidate=True,
            move_off_hot_path_candidate=True,
        )
        runtime_supervisor_path = _timed_substage(
            "runtime_supervisor_authority_write",
            lambda: write_track_b_runtime_supervisor_authority(
                config=supervisor_config,
                payload=runtime_supervisor,
            ),
        )
        refresh_stage = "canonical_readiness"
        readiness = _timed_substage(
            "canonical_readiness_write",
            lambda: write_canonical_readiness_artifact(
                repo_root=config.repo_root,
                output_path=config.resolve(config.canonical_readiness_path),
                now=actual_now,
            ),
            cache_or_throttle_candidate=True,
            move_off_hot_path_candidate=True,
            scans_historical_artifacts=True,
        )
    except Exception as exc:  # pragma: no cover - exercised through tests with monkeypatch
        payload = _base_payload(
            config=config,
            now=actual_now,
            classification=AUTHORITY_REFRESH_FAILED,
            latest_successful_refresh_at=latest_successful_refresh_at,
            reason_codes=["AUTHORITY_REFRESH_FAILED", f"AUTHORITY_REFRESH_FAILED_{refresh_stage.upper()}", type(exc).__name__],
            runtime_truth=runtime_truth,
            control_plane_age_seconds=control_plane_age,
        )
        payload.update(
            {
                "last_failure_at": actual_now.isoformat(),
                "exception_type": type(exc).__name__,
                "exception_message": str(exc),
                "refresh_stage": refresh_stage,
                "substage_durations": substage_durations,
                "slowest_substage": _slowest_substage(substage_durations),
            }
        )
        return _write_heartbeat(config=config, payload=payload)

    refreshed_control_plane_age = _artifact_age_seconds(control_plane, actual_now)
    payload = _base_payload(
        config=config,
        now=actual_now,
        classification=AUTHORITY_REFRESHED,
        latest_successful_refresh_at=actual_now.isoformat(),
        reason_codes=[],
        runtime_truth=runtime_truth,
        control_plane_age_seconds=refreshed_control_plane_age,
    )
    payload.update(
        {
            "control_plane_snapshot_id": control_plane.get("control_plane_snapshot_id"),
            "control_plane_classification": control_plane.get("classification"),
            "safe_state_classification": control_plane.get("runtime_safe_state_classification"),
            "runtime_supervisor_classification": runtime_supervisor.get("classification"),
            "planner_classification": planner.get("classification"),
            "paper_autonomous_recovery_plan_classification": recovery_plan.get("classification"),
            "paper_autonomous_recovery_plan_control_plane_snapshot_id": recovery_plan.get(
                "control_plane_snapshot_id"
            ),
            "canonical_readiness": readiness.get("canonical_readiness") or readiness.get("state"),
            "ready_submit_capable": readiness.get("canonical_readiness") == "READY_SUBMIT_CAPABLE"
            or readiness.get("state") == "READY_SUBMIT_CAPABLE",
            "submit_allowed": readiness.get("submit_allowed") is True,
            "shared_truth_refresh_classification": shared_truth.get("classification"),
            "substage_durations": substage_durations,
            "slowest_substage": _slowest_substage(substage_durations),
            "artifact_paths": {
                "authority_refresh": str(config.resolve(config.output_path)),
                "control_plane_snapshot": str(control_plane_path),
                "safe_state_envelope": str(
                    config.resolve(config.runtime_safe_state_envelope_path)
                ),
                "runtime_supervisor_authority": str(runtime_supervisor_path),
                "canonical_readiness": str(config.resolve(config.canonical_readiness_path)),
                "order_adjustment_planner": str(planner_path),
                "paper_autonomous_recovery_planner": str(recovery_plan_path),
                "shared_truth_refresh": str(config.resolve(config.shared_truth_refresh_path)),
            },
        }
    )
    return _write_heartbeat(config=config, payload=payload)


def _slowest_substage(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    return dict(max(rows, key=lambda row: float(row.get("duration_seconds") or 0.0)))


def _align_shared_truth_recovery_plan_row(
    *,
    config: TrackBAuthorityRefreshHeartbeatConfig,
    shared_truth: Mapping[str, Any],
    recovery_plan: Mapping[str, Any],
    recovery_plan_path: Path,
) -> dict[str, Any]:
    payload = dict(shared_truth)
    services = []
    replaced = False
    for row in payload.get("services") or []:
        if not isinstance(row, Mapping):
            continue
        item = dict(row)
        if item.get("service") == "PAPER Autonomous Recovery Planner":
            item.update(
                {
                    "classification": recovery_plan.get("classification") or "MISSING",
                    "generated_at": recovery_plan.get("generated_at"),
                    "artifact_path": str(recovery_plan_path),
                }
            )
            replaced = True
        services.append(item)
    if not replaced:
        services.append(
            {
                "service": "PAPER Autonomous Recovery Planner",
                "classification": recovery_plan.get("classification") or "MISSING",
                "generated_at": recovery_plan.get("generated_at"),
                "artifact_path": str(recovery_plan_path),
            }
        )
    payload["services"] = services
    classifications = dict(payload.get("classifications") or {})
    classifications["PAPER Autonomous Recovery Planner"] = recovery_plan.get("classification") or "MISSING"
    payload["classifications"] = classifications
    artifact_paths = dict(payload.get("artifact_paths") or {})
    artifact_paths["PAPER Autonomous Recovery Planner"] = str(recovery_plan_path)
    payload["artifact_paths"] = artifact_paths
    payload["autonomous_recovery_plan_classification"] = recovery_plan.get("classification")
    payload["autonomous_recovery_execution_enabled"] = recovery_plan.get("execution_enabled") is True
    warnings = [
        dict(row)
        for row in payload.get("warnings") or []
        if (row if isinstance(row, Mapping) else {}).get("code") != "paper_autonomous_recovery_plan_advisory_stale"
        or recovery_plan.get("classification") in {"PLAN_BLOCKED_STALE_EVIDENCE", "MISSING"}
    ]
    payload["warnings"] = warnings
    write_json_atomic(config.resolve(config.shared_truth_refresh_path), payload)
    return payload


def record_track_b_authority_refresh_runtime_failure(
    *,
    config: TrackBAuthorityRefreshHeartbeatConfig,
    exception: BaseException,
    now: datetime | None = None,
    reason_codes: Sequence[str] = ("AUTHORITY_REFRESH_RUNTIME_CALL_FAILED",),
) -> dict[str, Any]:
    """Persist a visible read-only heartbeat failure from the active runtime loop."""

    actual_now = _ensure_utc(now or datetime.now(UTC))
    previous = _read_json(config.resolve(config.output_path))
    runtime_truth = _read_json(config.resolve(config.runtime_truth_path))
    control_plane = _read_json(config.resolve(config.control_plane_snapshot_path))
    payload = _base_payload(
        config=config,
        now=actual_now,
        classification=AUTHORITY_REFRESH_FAILED,
        latest_successful_refresh_at=previous.get("latest_successful_refresh_at"),
        reason_codes=[*reason_codes, type(exception).__name__],
        runtime_truth=runtime_truth,
        control_plane_age_seconds=_artifact_age_seconds(control_plane, actual_now),
    )
    payload.update(
        {
            "last_failure_at": actual_now.isoformat(),
            "exception_type": type(exception).__name__,
            "exception_message": str(exception),
        }
    )
    return _write_heartbeat(config=config, payload=payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh read-only Track B PAPER authority artifacts when due.")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[3]))
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBAuthorityRefreshHeartbeatConfig(repo_root=Path(args.repo_root).expanduser().resolve())
    payload = refresh_track_b_paper_authority_if_due(config=config, force=bool(args.force))
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"classification={payload.get('classification')}")
        print(f"latest_successful_refresh_at={payload.get('latest_successful_refresh_at')}")
        print(f"reason_codes={','.join(str(code) for code in payload.get('reason_codes') or []) or 'none'}")
    return 2 if payload.get("classification") == AUTHORITY_REFRESH_FAILED else 0


def _base_payload(
    *,
    config: TrackBAuthorityRefreshHeartbeatConfig,
    now: datetime,
    classification: str,
    latest_successful_refresh_at: object,
    reason_codes: Sequence[str],
    runtime_truth: Mapping[str, Any],
    control_plane_age_seconds: float | None = None,
) -> dict[str, Any]:
    latest_successful_refresh_age_seconds = _age_seconds(latest_successful_refresh_at, now)
    authority_fresh = (
        classification != AUTHORITY_REFRESH_FAILED
        and latest_successful_refresh_age_seconds is not None
        and latest_successful_refresh_age_seconds < float(config.bridge_max_age_seconds)
    )
    runtime_active = _runtime_active(runtime_truth)
    operator_status = _read_json(config.resolve(config.operator_status_path))
    out_of_window = _all_active_lanes_out_of_window(operator_status)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now.isoformat(),
        "classification": classification,
        "activity_classification": _activity_classification(
            runtime_active=runtime_active,
            authority_fresh=authority_fresh,
            out_of_window=out_of_window,
            classification=classification,
        ),
        "reason_codes": list(reason_codes),
        "latest_successful_refresh_at": latest_successful_refresh_at,
        "latest_successful_refresh_age_seconds": latest_successful_refresh_age_seconds,
        "authority_fresh": authority_fresh,
        "runtime_active": runtime_active,
        "runtime_pid": runtime_truth.get("producer_pid") or runtime_truth.get("pid"),
        "runtime_instance_id": runtime_truth.get("runtime_instance_id"),
        "current_detected_session": operator_status.get("current_detected_session"),
        "all_active_lanes_out_of_window": out_of_window,
        "control_plane_snapshot_age_seconds": control_plane_age_seconds,
        "refresh_interval_seconds": float(config.interval_seconds),
        "bridge_max_age_seconds": float(config.bridge_max_age_seconds),
        "refresh_interval_inside_bridge_window": float(config.interval_seconds) < float(config.bridge_max_age_seconds),
        "read_only": True,
        "submit_authority": False,
        "broker_mutation_allowed": False,
        "runtime_mutation_allowed": False,
        "paper_only": True,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "artifact_paths": {
            "authority_refresh": str(config.resolve(config.output_path)),
            "runtime_truth": str(config.resolve(config.runtime_truth_path)),
            "operator_status": str(config.resolve(config.operator_status_path)),
            "control_plane_snapshot": str(config.resolve(config.control_plane_snapshot_path)),
            "safe_state_envelope": str(config.resolve(config.runtime_safe_state_envelope_path)),
            "runtime_supervisor_authority": str(config.resolve(config.runtime_supervisor_authority_path)),
            "canonical_readiness": str(config.resolve(config.canonical_readiness_path)),
            "order_adjustment_planner": str(config.resolve(config.order_adjustment_plan_path)),
            "shared_truth_refresh": str(config.resolve(config.shared_truth_refresh_path)),
        },
    }


def _activity_classification(
    *,
    runtime_active: bool,
    authority_fresh: bool,
    out_of_window: bool,
    classification: str,
) -> str:
    if not runtime_active:
        return "RUNTIME_INACTIVE"
    if classification == AUTHORITY_REFRESH_FAILED:
        return "AUTHORITY_REFRESH_FAILED"
    if authority_fresh and out_of_window:
        return "OUT_OF_WINDOW_BUT_AUTHORITY_FRESH"
    if authority_fresh:
        return "AUTHORITY_FRESH_RUNTIME_ACTIVE"
    return "BLOCKED_STALE_TRUTH"


def _all_active_lanes_out_of_window(operator_status: Mapping[str, Any]) -> bool:
    current_session = _normalize_session(operator_status.get("current_detected_session"))
    lanes = operator_status.get("lanes")
    if not current_session or not isinstance(lanes, Sequence) or isinstance(lanes, (str, bytes)):
        return False
    active_lane_ids = {
        str(lane_id)
        for lane_id in (operator_status.get("active_lane_ids") or [])
        if str(lane_id)
    }
    considered = []
    for lane in lanes:
        if not isinstance(lane, Mapping):
            continue
        lane_id = str(lane.get("lane_id") or "")
        if active_lane_ids and lane_id not in active_lane_ids:
            continue
        allowed_sessions = {
            _normalize_session(session)
            for session in (lane.get("allowed_sessions") or [])
            if _normalize_session(session)
        }
        if not allowed_sessions:
            raw_restriction = str(lane.get("session_restriction") or "")
            allowed_sessions = {
                _normalize_session(part)
                for part in raw_restriction.replace(",", "/").split("/")
                if _normalize_session(part)
            }
        if not allowed_sessions or "ALL" in allowed_sessions:
            return False
        considered.append(lane_id)
        if current_session in allowed_sessions:
            return False
    return bool(considered)


def _normalize_session(value: object) -> str:
    return str(value or "").strip().upper()


def _write_heartbeat(
    *,
    config: TrackBAuthorityRefreshHeartbeatConfig,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    output_path = write_json_atomic(config.resolve(config.output_path), dict(payload))
    event_path = config.resolve(config.events_path)
    event_path.parent.mkdir(parents=True, exist_ok=True)
    with event_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(payload), sort_keys=True) + "\n")
    result = dict(payload)
    result["artifact_path"] = str(output_path)
    result["event_log_path"] = str(event_path)
    return result


def _runtime_active(runtime_truth: Mapping[str, Any]) -> bool:
    pid = _optional_int(runtime_truth.get("producer_pid") or runtime_truth.get("pid"))
    if pid is None or not _process_running(pid):
        return False
    heartbeat = str(runtime_truth.get("heartbeat_state") or "")
    return heartbeat in {"HEALTHY", "FRESH", ""}


def _process_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError as exc:
        if exc.errno == errno.EPERM:
            return True
        return False
    return True


def _artifact_age_seconds(payload: Mapping[str, Any], now: datetime) -> float | None:
    return _age_seconds(payload.get("generated_at") or payload.get("last_success_at"), now)


def _age_seconds(value: object, now: datetime) -> float | None:
    parsed = _parse_iso(value)
    if parsed is None:
        return None
    return max(0.0, (now - parsed).total_seconds())


def _parse_iso(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _optional_int(value: object) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


if __name__ == "__main__":
    raise SystemExit(main())
