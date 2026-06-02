"""Read-only Track B PAPER live runtime environment watchdog.

The watchdog answers whether a launchd-owned PAPER runtime is operational, not
just alive. It is diagnostic/read-only: it never starts, submits, cancels,
closes, flattens, or mutates broker state.
"""

from __future__ import annotations

import argparse
import errno
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution.ibkr_paper_strategy_bridge import IbkrPaperStrategyBridgeConfig
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_authority_refresh_heartbeat import (
    DEFAULT_AUTHORITY_REFRESH_HEARTBEAT_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_control_plane_snapshot import DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
from mgc_v05l.execution_core.track_b_runtime_safe_state_envelope import DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_registry_truth_diagnostics import DEFAULT_DIAGNOSTICS_REPORT_PATH
from mgc_v05l.execution_core.track_b_pre_restart_exposure_reconciliation import (
    PreRestartExposureResolverConfig,
    resolve_pre_restart_exposure_reconciliation,
)


SCHEMA_VERSION = "track_b_live_runtime_environment_watchdog_v1"

READY_SUBMIT_CAPABLE = "READY_SUBMIT_CAPABLE"
OUT_OF_WINDOW_BUT_HEALTHY = "OUT_OF_WINDOW_BUT_HEALTHY"
DEGRADED_AUTHORITY_STALE = "DEGRADED_AUTHORITY_STALE"
DEGRADED_DATA_STALE = "DEGRADED_DATA_STALE"
DEGRADED_LANES_NOT_EVALUATING = "DEGRADED_LANES_NOT_EVALUATING"
RECOVERY_REQUIRED = "RECOVERY_REQUIRED"
REVIEW_REQUIRED = "REVIEW_REQUIRED"

WATCHDOG_INTERVAL_SECONDS = 90.0
BRIDGE_PRE_ACTION_MAX_AGE_SECONDS = float(
    IbkrPaperStrategyBridgeConfig.__dataclass_fields__["pre_action_snapshot_max_age_seconds"].default
)

DEFAULT_LIVE_RUNTIME_ENVIRONMENT_WATCHDOG_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "runtime_environment"
    / "latest_live_runtime_environment_watchdog.json"
)
DEFAULT_LIVE_RUNTIME_ENVIRONMENT_WATCHDOG_EVENTS = (
    Path("outputs") / "track_b_execution_core" / "runtime_environment" / "live_runtime_environment_events.jsonl"
)
DEFAULT_PAPER_RUNTIME_TRUTH_ARTIFACT = (
    Path("outputs")
    / "probationary_pattern_engine"
    / "paper_session"
    / "runtime"
    / "paper_runtime_truth.json"
)
DEFAULT_OPERATOR_STATUS_ARTIFACT = Path("outputs") / "probationary_pattern_engine" / "paper_session" / "operator_status.json"
DEFAULT_CANONICAL_READINESS_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
)
DEFAULT_BROKER_RECONCILIATION_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_BROKER_TRUTH_REFRESH_STATUS_ARTIFACT = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json"
)
DEFAULT_PHASE1_LISTENER_STATUS_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "phase1_databento_live_runtime_candles"
    / "latest_phase1_databento_live_listener_status.json"
)
DEFAULT_RECOVERY_STATUS_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "runtime_recovery" / "latest_launchd_recovery_status.json"
)


@dataclass(frozen=True)
class TrackBLiveRuntimeEnvironmentWatchdogConfig:
    repo_root: Path
    output_path: Path = DEFAULT_LIVE_RUNTIME_ENVIRONMENT_WATCHDOG_ARTIFACT
    events_path: Path = DEFAULT_LIVE_RUNTIME_ENVIRONMENT_WATCHDOG_EVENTS
    runtime_truth_path: Path = DEFAULT_PAPER_RUNTIME_TRUTH_ARTIFACT
    operator_status_path: Path = DEFAULT_OPERATOR_STATUS_ARTIFACT
    canonical_readiness_path: Path = DEFAULT_CANONICAL_READINESS_ARTIFACT
    authority_refresh_path: Path = DEFAULT_AUTHORITY_REFRESH_HEARTBEAT_ARTIFACT
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    safe_state_envelope_path: Path = DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
    runtime_supervisor_authority_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT
    broker_reconciliation_path: Path = DEFAULT_BROKER_RECONCILIATION_ARTIFACT
    broker_truth_refresh_status_path: Path = DEFAULT_BROKER_TRUTH_REFRESH_STATUS_ARTIFACT
    registry_diagnostics_path: Path = DEFAULT_DIAGNOSTICS_REPORT_PATH
    phase1_listener_status_path: Path = DEFAULT_PHASE1_LISTENER_STATUS_ARTIFACT
    recovery_status_path: Path = DEFAULT_RECOVERY_STATUS_ARTIFACT
    watchdog_interval_seconds: float = WATCHDOG_INTERVAL_SECONDS
    bridge_max_age_seconds: float = BRIDGE_PRE_ACTION_MAX_AGE_SECONDS
    runtime_truth_max_age_seconds: float = 180.0
    market_data_max_age_seconds: float = 180.0
    reconciliation_max_age_seconds: float = 300.0
    lane_stall_tolerance_seconds: float = 300.0

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_live_runtime_environment_watchdog(
    *,
    config: TrackBLiveRuntimeEnvironmentWatchdogConfig,
    now: datetime | None = None,
    pid_running: Callable[[int], bool] | None = None,
    source_commit_resolver: Callable[[Path], str | None] | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    pid_running = pid_running or _pid_running
    source_commit_resolver = source_commit_resolver or _git_head

    runtime_truth = _read_json(config.resolve(config.runtime_truth_path))
    operator_status = _read_json(config.resolve(config.operator_status_path))
    canonical_readiness = _read_json(config.resolve(config.canonical_readiness_path))
    authority_refresh = _read_json(config.resolve(config.authority_refresh_path))
    control_plane = _read_json(config.resolve(config.control_plane_snapshot_path))
    safe_state = _read_json(config.resolve(config.safe_state_envelope_path))
    supervisor = _read_json(config.resolve(config.runtime_supervisor_authority_path))
    reconciliation = _read_json(config.resolve(config.broker_reconciliation_path))
    broker_truth = _read_json(config.resolve(config.broker_truth_refresh_status_path))
    registry = _read_json(config.resolve(config.registry_diagnostics_path))
    phase1 = _read_json(config.resolve(config.phase1_listener_status_path))
    recovery = _read_json(config.resolve(config.recovery_status_path))

    runtime_pid = _optional_int(runtime_truth.get("producer_pid") or runtime_truth.get("pid"))
    runtime_alive = bool(runtime_pid is not None and pid_running(runtime_pid))
    runtime_truth_age = _artifact_age_seconds(runtime_truth, actual_now)
    runtime_truth_fresh = runtime_truth_age is not None and runtime_truth_age <= float(config.runtime_truth_max_age_seconds)

    authority_age = _age_seconds(
        authority_refresh.get("latest_successful_refresh_at") or authority_refresh.get("generated_at"),
        actual_now,
    )
    authority_fresh = (
        authority_refresh.get("classification") in {"AUTHORITY_REFRESHED", "AUTHORITY_REFRESH_SKIPPED_NOT_DUE"}
        and authority_age is not None
        and authority_age <= float(config.bridge_max_age_seconds)
    )
    source_ages = {
        "control_plane": _artifact_age_seconds(control_plane, actual_now),
        "safe_state": _artifact_age_seconds(safe_state, actual_now),
        "runtime_supervisor": _artifact_age_seconds(supervisor, actual_now),
    }
    authority_source_fresh = {
        key: age is not None and age <= float(config.bridge_max_age_seconds) for key, age in source_ages.items()
    }
    authority_bundle_fresh = authority_fresh and all(authority_source_fresh.values())

    market_state = str(canonical_readiness.get("market_schedule_state") or "").strip().upper()
    scheduled_halt = bool(canonical_readiness.get("readiness_block_is_scheduled_halt")) or market_state in {
        "SCHEDULED_MARKET_HALT",
        "WEEKEND_GLOBEX_HALT_BEFORE_SUNDAY_REOPEN",
    }
    market_open = not scheduled_halt and market_state not in {"", "OUT_OF_WINDOW"}
    phase1_age = _age_seconds(phase1.get("latest_record_at") or phase1.get("generated_at"), actual_now)
    market_data_fresh = phase1_age is not None and phase1_age <= float(config.market_data_max_age_seconds)

    broker_lifecycle_clean = _broker_lifecycle_clean(reconciliation)
    registry_clean = _registry_clean(registry)
    duplicate_writer = _duplicate_writer_detected(runtime_truth)
    track_b_positions = _int_or_zero(
        reconciliation.get("track_b_broker_position_count")
        or registry.get("track_b_managed_futures_position_count")
    )
    broker_open_orders = _int_or_zero(reconciliation.get("track_b_broker_open_order_count") or registry.get("broker_open_order_count"))
    lifecycle_open_positions = _int_or_zero(
        reconciliation.get("lifecycle_open_position_count") or registry.get("lifecycle_open_position_count")
    )
    pre_restart_exposure_resolution = resolve_pre_restart_exposure_reconciliation(
        config=PreRestartExposureResolverConfig(repo_root=config.repo_root),
        broker_positions=_list(reconciliation.get("track_b_broker_positions")),
        lifecycle_positions=_list(reconciliation.get("track_b_lifecycle_positions")),
        broker_open_orders=_list(reconciliation.get("track_b_broker_open_orders")),
    )
    recovery_active = _recovery_active(recovery)
    current_head = source_commit_resolver(config.repo_root)
    source_commit = str(runtime_truth.get("source_commit") or "").strip() or None
    code_version_matches = bool(source_commit and current_head and source_commit == current_head)

    lane_health = _lane_health(operator_status, now=actual_now, tolerance_seconds=float(config.lane_stall_tolerance_seconds))
    reason_codes: list[str] = []
    if not runtime_alive:
        reason_codes.append("RUNTIME_PROCESS_NOT_ALIVE")
    if not runtime_truth_fresh:
        reason_codes.append("RUNTIME_TRUTH_STALE")
    if not authority_bundle_fresh:
        reason_codes.append("AUTHORITY_HEARTBEAT_STALE")
    for key, fresh in authority_source_fresh.items():
        if not fresh:
            reason_codes.append(f"{key.upper()}_STALE")
    if market_open and not market_data_fresh:
        reason_codes.append("MARKET_DATA_STALE")
    if duplicate_writer:
        reason_codes.append("DUPLICATE_RUNTIME_WRITER_DETECTED")
    if not broker_lifecycle_clean:
        reason_codes.append("BROKER_LIFECYCLE_NOT_RECONCILED")
    if not registry_clean:
        reason_codes.append("REGISTRY_RECONCILIATION_NOT_MATCHED")
    if source_commit and current_head and source_commit != current_head:
        reason_codes.append("RUNTIME_CODE_VERSION_MISMATCH")
    if not source_commit:
        reason_codes.append("RUNTIME_CODE_VERSION_UNCERTAIN")
    if lane_health["in_window_lane_count"] > 0 and lane_health["stalled_lanes"]:
        reason_codes.append("LANES_NOT_EVALUATING")

    classification = _classify(
        runtime_alive=runtime_alive,
        runtime_truth_fresh=runtime_truth_fresh,
        authority_bundle_fresh=authority_bundle_fresh,
        market_open=market_open,
        market_data_fresh=market_data_fresh,
        duplicate_writer=duplicate_writer,
        broker_lifecycle_clean=broker_lifecycle_clean,
        registry_clean=registry_clean,
        lane_stalled=bool(lane_health["stalled_lanes"]),
        out_of_window=lane_health["all_active_lanes_out_of_window"],
        ready_submit_capable=_ready_submit_capable(canonical_readiness),
    )
    restart_policy = _restart_policy(
        runtime_alive=runtime_alive,
        classification=classification,
        track_b_positions=track_b_positions,
        broker_open_orders=broker_open_orders,
        lifecycle_open_positions=lifecycle_open_positions,
        broker_lifecycle_clean=broker_lifecycle_clean,
        registry_clean=registry_clean,
        duplicate_writer=duplicate_writer,
        recovery_active=recovery_active,
        pre_restart_exposure_resolution=pre_restart_exposure_resolution,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": actual_now.isoformat(),
        "classification": classification,
        "reason_codes": sorted(set(reason_codes)),
        "read_only": True,
        "submit_authority": False,
        "broker_mutation_allowed": False,
        "runtime_mutation_allowed": False,
        "paper_only": True,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "liveness_contract": {
            "process_alive": runtime_alive,
            "runtime_truth_fresh": runtime_truth_fresh,
            "market_data_fresh_when_open": (not market_open) or market_data_fresh,
            "authority_heartbeat_fresh_within_bridge_max_age": authority_bundle_fresh,
            "control_plane_fresh": authority_source_fresh["control_plane"],
            "safe_state_fresh": authority_source_fresh["safe_state"],
            "runtime_supervisor_fresh": authority_source_fresh["runtime_supervisor"],
            "broker_truth_lease_fresh": broker_truth.get("fresh") is True
            or str(broker_truth.get("classification") or "").upper() in {"BROKER_TRUTH_REFRESH_ACTIVE", "BROKER_TRUTH_FRESH"},
            "broker_lifecycle_reconciliation_fresh": broker_lifecycle_clean,
            "registry_reconciliation_matched": registry_clean,
            "duplicate_writer_false": not duplicate_writer,
            "lane_evaluation_advancing_when_in_window": not lane_health["stalled_lanes"],
        },
        "runtime": {
            "pid": runtime_pid,
            "source_commit": source_commit,
            "current_head": current_head,
            "code_version_matches_head": code_version_matches,
            "lane_count": runtime_truth.get("lane_count"),
            "runtime_truth_generated_at": runtime_truth.get("generated_at"),
            "runtime_truth_age_seconds": runtime_truth_age,
        },
        "authority": {
            "classification": authority_refresh.get("classification"),
            "latest_successful_refresh_at": authority_refresh.get("latest_successful_refresh_at"),
            "age_seconds": authority_age,
            "bridge_max_age_seconds": float(config.bridge_max_age_seconds),
            "refresh_interval_seconds": authority_refresh.get("refresh_interval_seconds"),
            "source_ages_seconds": source_ages,
            "source_fresh": authority_source_fresh,
            "last_failure_at": authority_refresh.get("last_failure_at"),
        },
        "market_data": {
            "market_schedule_state": market_state or None,
            "scheduled_halt": scheduled_halt,
            "market_open_requires_fresh_data": market_open,
            "phase1_latest_record_at": phase1.get("latest_record_at"),
            "phase1_age_seconds": phase1_age,
            "fresh": market_data_fresh,
        },
        "lanes": lane_health,
        "broker_lifecycle": {
            "clean": broker_lifecycle_clean,
            "classification": reconciliation.get("classification") or reconciliation.get("reconciliation_state"),
            "track_b_position_count": track_b_positions,
            "broker_open_order_count": broker_open_orders,
            "lifecycle_open_position_count": lifecycle_open_positions,
            "generated_at": reconciliation.get("generated_at"),
        },
        "registry": {
            "clean_current_scope": registry_clean,
            "classification": registry.get("classification"),
            "current_scope_review_required_count": registry.get("current_scope_review_required_count")
            or len(registry.get("review_required_trade_ids") or []),
            "generated_at": registry.get("generated_at"),
        },
        "pre_restart_exposure_resolution": pre_restart_exposure_resolution,
        "recovery": {
            "active": recovery_active,
            "classification": recovery.get("classification"),
            "generated_at": recovery.get("generated_at"),
        },
        "restart_policy": restart_policy,
        "artifact_paths": {
            "watchdog": str(config.resolve(config.output_path)),
            "runtime_truth": str(config.resolve(config.runtime_truth_path)),
            "operator_status": str(config.resolve(config.operator_status_path)),
            "canonical_readiness": str(config.resolve(config.canonical_readiness_path)),
            "authority_refresh": str(config.resolve(config.authority_refresh_path)),
            "control_plane_snapshot": str(config.resolve(config.control_plane_snapshot_path)),
            "safe_state_envelope": str(config.resolve(config.safe_state_envelope_path)),
            "runtime_supervisor_authority": str(config.resolve(config.runtime_supervisor_authority_path)),
            "broker_reconciliation": str(config.resolve(config.broker_reconciliation_path)),
            "registry_diagnostics": str(config.resolve(config.registry_diagnostics_path)),
            "phase1_listener_status": str(config.resolve(config.phase1_listener_status_path)),
            "recovery_status": str(config.resolve(config.recovery_status_path)),
        },
    }


def write_track_b_live_runtime_environment_watchdog(
    *,
    config: TrackBLiveRuntimeEnvironmentWatchdogConfig,
    payload: Mapping[str, Any],
) -> Path:
    output_path = write_json_atomic(config.resolve(config.output_path), dict(payload))
    event_path = config.resolve(config.events_path)
    event_path.parent.mkdir(parents=True, exist_ok=True)
    with event_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(payload), sort_keys=True) + "\n")
    return output_path


def run_track_b_live_runtime_environment_watchdog_if_due(
    *,
    config: TrackBLiveRuntimeEnvironmentWatchdogConfig,
    now: datetime | None = None,
    force: bool = False,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    previous = _read_json(config.resolve(config.output_path))
    previous_age = _artifact_age_seconds(previous, actual_now)
    if not force and previous_age is not None and previous_age < float(config.watchdog_interval_seconds):
        return {
            **previous,
            "skipped_not_due": True,
            "watchdog_interval_seconds": float(config.watchdog_interval_seconds),
        }
    payload = build_track_b_live_runtime_environment_watchdog(config=config, now=actual_now)
    path = write_track_b_live_runtime_environment_watchdog(config=config, payload=payload)
    return {**payload, "artifact_path": str(path), "skipped_not_due": False}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Emit read-only Track B live runtime environment watchdog status.")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[3]))
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBLiveRuntimeEnvironmentWatchdogConfig(repo_root=Path(args.repo_root).expanduser().resolve())
    payload = run_track_b_live_runtime_environment_watchdog_if_due(config=config, force=bool(args.force))
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"classification={payload.get('classification')}")
        print(f"reason_codes={','.join(str(code) for code in payload.get('reason_codes') or []) or 'none'}")
    return 0


def _classify(
    *,
    runtime_alive: bool,
    runtime_truth_fresh: bool,
    authority_bundle_fresh: bool,
    market_open: bool,
    market_data_fresh: bool,
    duplicate_writer: bool,
    broker_lifecycle_clean: bool,
    registry_clean: bool,
    lane_stalled: bool,
    out_of_window: bool,
    ready_submit_capable: bool,
) -> str:
    if not runtime_alive:
        return RECOVERY_REQUIRED
    if duplicate_writer or not broker_lifecycle_clean or not registry_clean:
        return REVIEW_REQUIRED
    if not runtime_truth_fresh or not authority_bundle_fresh:
        return DEGRADED_AUTHORITY_STALE
    if market_open and not market_data_fresh:
        return DEGRADED_DATA_STALE
    if lane_stalled:
        return DEGRADED_LANES_NOT_EVALUATING
    if ready_submit_capable:
        return READY_SUBMIT_CAPABLE
    if out_of_window:
        return OUT_OF_WINDOW_BUT_HEALTHY
    return REVIEW_REQUIRED


def _restart_policy(
    *,
    runtime_alive: bool,
    classification: str,
    track_b_positions: int,
    broker_open_orders: int,
    lifecycle_open_positions: int,
    broker_lifecycle_clean: bool,
    registry_clean: bool,
    duplicate_writer: bool,
    recovery_active: bool,
    pre_restart_exposure_resolution: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    flat_clean = (
        track_b_positions == 0
        and broker_open_orders == 0
        and lifecycle_open_positions == 0
        and broker_lifecycle_clean
        and registry_clean
        and not duplicate_writer
        and recovery_active
    )
    open_exposure = track_b_positions > 0 or broker_open_orders > 0 or lifecycle_open_positions > 0
    exposure_resolution = dict(pre_restart_exposure_resolution or {})
    owned_exposure_proven = (
        track_b_positions > 0
        and broker_open_orders == 0
        and exposure_resolution.get("restart_with_owned_exposure_allowed") is True
        and int(exposure_resolution.get("review_required_exposure_count") or 0) == 0
        and not duplicate_writer
        and recovery_active
    )
    restart_allowed = flat_clean or owned_exposure_proven
    return {
        "process_died_restart_allowed": (not runtime_alive) and restart_allowed,
        "degraded_restart_allowed": runtime_alive
        and classification in {DEGRADED_AUTHORITY_STALE, DEGRADED_DATA_STALE, DEGRADED_LANES_NOT_EVALUATING}
        and restart_allowed,
        "requires_flat_clean_state": not owned_exposure_proven,
        "owned_exposure_restart_allowed": owned_exposure_proven,
        "open_exposure_or_orders_present": open_exposure,
        "recovery_active_required": True,
        "pre_restart_exposure_resolution_classification": exposure_resolution.get("classification"),
        "reason_codes": [] if restart_allowed else _restart_blockers(
            track_b_positions=track_b_positions,
            broker_open_orders=broker_open_orders,
            lifecycle_open_positions=lifecycle_open_positions,
            broker_lifecycle_clean=broker_lifecycle_clean,
            registry_clean=registry_clean,
            duplicate_writer=duplicate_writer,
            recovery_active=recovery_active,
            owned_exposure_proven=owned_exposure_proven,
        ),
    }


def _restart_blockers(
    *,
    track_b_positions: int,
    broker_open_orders: int,
    lifecycle_open_positions: int,
    broker_lifecycle_clean: bool,
    registry_clean: bool,
    duplicate_writer: bool,
    recovery_active: bool,
    owned_exposure_proven: bool = False,
) -> list[str]:
    blockers: list[str] = []
    if (track_b_positions or broker_open_orders or lifecycle_open_positions) and not owned_exposure_proven:
        blockers.append("NO_AUTOMATIC_RESTART_OPEN_EXPOSURE_WITHOUT_PROVEN_IDENTITY")
    if not broker_lifecycle_clean:
        blockers.append("BROKER_LIFECYCLE_NOT_RECONCILED")
    if not registry_clean:
        blockers.append("REGISTRY_RECONCILIATION_NOT_MATCHED")
    if duplicate_writer:
        blockers.append("DUPLICATE_RUNTIME_WRITER_DETECTED")
    if not recovery_active:
        blockers.append("RECOVERY_SERVICE_NOT_ACTIVE")
    return blockers


def _lane_health(operator_status: Mapping[str, Any], *, now: datetime, tolerance_seconds: float) -> dict[str, Any]:
    raw_lanes = operator_status.get("lanes")
    if isinstance(raw_lanes, Mapping):
        lanes = [lane for lane in raw_lanes.values() if isinstance(lane, Mapping)]
    elif isinstance(raw_lanes, Sequence) and not isinstance(raw_lanes, (str, bytes)):
        lanes = [lane for lane in raw_lanes if isinstance(lane, Mapping)]
    else:
        lanes = []
    active_lane_ids = {str(value) for value in operator_status.get("active_lane_ids") or [] if str(value)}
    in_window: list[str] = []
    out_of_window: list[str] = []
    stalled: list[dict[str, Any]] = []
    for lane in lanes:
        lane_id = str(lane.get("lane_id") or "")
        if active_lane_ids and lane_id not in active_lane_ids:
            continue
        classification = str(lane.get("current_session_window_classification") or lane.get("eligibility_reason") or "").upper()
        is_in_window = (
            lane.get("eligible_now") is True
            or lane.get("allowed_session_match") is True
            or classification == "IN_WINDOW"
        )
        if is_in_window:
            in_window.append(lane_id)
            processed_ts = lane.get("last_processed_bar_end_ts")
            execution_ts = lane.get("last_execution_bar_evaluated_at") or lane.get("last_completed_context_bars_at")
            processed = _parse_iso(processed_ts)
            execution = _parse_iso(execution_ts)
            if processed is not None and (execution is None or (processed - execution).total_seconds() > tolerance_seconds):
                stalled.append(
                    {
                        "lane_id": lane_id,
                        "last_processed_bar_end_ts": processed_ts,
                        "last_execution_bar_evaluated_at": execution_ts,
                    }
                )
        elif classification in {"OUT_OF_WINDOW", "WRONG_SESSION"} or lane.get("eligible_now") is False:
            out_of_window.append(lane_id)
    considered = in_window + out_of_window
    return {
        "active_lane_count": len(considered),
        "in_window_lane_count": len(in_window),
        "out_of_window_lane_count": len(out_of_window),
        "all_active_lanes_out_of_window": bool(considered) and not in_window,
        "stalled_lanes": stalled,
    }


def _ready_submit_capable(payload: Mapping[str, Any]) -> bool:
    state = str(payload.get("canonical_readiness") or payload.get("state") or "").strip().upper()
    return state == READY_SUBMIT_CAPABLE and payload.get("submit_allowed") is not False


def _broker_lifecycle_clean(payload: Mapping[str, Any]) -> bool:
    classification = str(payload.get("classification") or payload.get("reconciliation_state") or "").upper()
    if classification in {"TRACK_B_PAPER_BROKER_RECONCILED", "BROKER_LIFECYCLE_RECONCILED"}:
        return True
    return payload.get("broker_reconciled") is True and _int_or_zero(payload.get("review_required_count")) == 0


def _registry_clean(payload: Mapping[str, Any]) -> bool:
    classification = str(payload.get("classification") or "").upper()
    review_count = _int_or_zero(payload.get("current_scope_review_required_count") or len(payload.get("review_required_trade_ids") or []))
    return classification in {
        "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE",
        "TRACK_B_DIAGNOSTICS_CLEAN",
    } and review_count == 0


def _duplicate_writer_detected(runtime_truth: Mapping[str, Any]) -> bool:
    duplicate = runtime_truth.get("duplicate_writer_detection")
    return runtime_truth.get("duplicate_writer_detected") is True or (
        isinstance(duplicate, Mapping) and duplicate.get("duplicate_writer_detected") is True
    )


def _recovery_active(payload: Mapping[str, Any]) -> bool:
    classification = str(payload.get("classification") or "").upper()
    return classification in {"RECOVERY_ACTIVE", "SUPERVISOR_RUNNING"} or payload.get("standalone_recovery_launchd_loaded") is True


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _list(value: object) -> list[dict[str, Any]]:
    return [dict(item) for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


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


def _int_or_zero(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError as exc:
        if exc.errno == errno.EPERM:
            return True
        return False
    return True


def _git_head(repo_root: Path) -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


if __name__ == "__main__":
    raise SystemExit(main())
