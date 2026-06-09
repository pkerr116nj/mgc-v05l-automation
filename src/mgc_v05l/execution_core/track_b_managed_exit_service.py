"""Cadenced close-only service for Track B managed exits.

The service owns scheduling only. Broker mutation, when explicitly enabled, is
delegated to the guarded managed-exit actuator.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.models import require_aware_datetime, to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_managed_exit_actuator import (
    MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
    MANAGED_EXIT_ACTUATOR_DRY_RUN_READY,
    MANAGED_EXIT_ACTUATOR_PARTIAL,
    TrackBManagedExitActuatorConfig,
    run_track_b_managed_exit_actuator,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_STATUS_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "managed_exit_service"
    / "latest_managed_exit_service_status.json"
)
DEFAULT_HEARTBEAT_PATH = Path("var") / "track_b_managed_exit_service_heartbeat.json"
DEFAULT_CADENCE_SECONDS = 45.0

MANAGED_EXIT_SERVICE_NOOP = "TRACK_B_MANAGED_EXIT_SERVICE_NOOP"
MANAGED_EXIT_SERVICE_DRY_RUN_READY = "TRACK_B_MANAGED_EXIT_SERVICE_DRY_RUN_READY"
MANAGED_EXIT_SERVICE_BLOCKED = "TRACK_B_MANAGED_EXIT_SERVICE_BLOCKED"
MANAGED_EXIT_SERVICE_APPLIED_OR_PENDING = "TRACK_B_MANAGED_EXIT_SERVICE_APPLIED_OR_PENDING"
MANAGED_EXIT_SERVICE_PARTIAL = "TRACK_B_MANAGED_EXIT_SERVICE_PARTIAL"
MANAGED_EXIT_SERVICE_REFRESH_FAILED = "TRACK_B_MANAGED_EXIT_SERVICE_REFRESH_FAILED"
MANAGED_EXIT_SERVICE_RUNNING = "TRACK_B_MANAGED_EXIT_SERVICE_RUNNING"
MANAGED_EXIT_SERVICE_STOPPING = "TRACK_B_MANAGED_EXIT_SERVICE_STOPPING"


@dataclass(frozen=True)
class TrackBManagedExitServiceConfig:
    repo_root: Path = REPO_ROOT
    status_path: Path = DEFAULT_STATUS_PATH
    heartbeat_path: Path | None = DEFAULT_HEARTBEAT_PATH
    cadence_seconds: float = DEFAULT_CADENCE_SECONDS
    apply: bool = False
    operator_authorized_managed_exit: bool = False
    max_cycles_per_tick: int = 4
    authority_refresh_before_apply: bool = True
    authority_refresh_after_attempt: bool = True
    authority_refresh_timeout_seconds: float = 120.0

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


ActuatorRunner = Callable[[TrackBManagedExitActuatorConfig, datetime], Mapping[str, Any]]
AuthorityRefresher = Callable[[TrackBManagedExitServiceConfig, str], Mapping[str, Any]]
SleepFunc = Callable[[float], None]


def run_track_b_managed_exit_service_once(
    *,
    config: TrackBManagedExitServiceConfig,
    now: datetime | None = None,
    actuator_runner: ActuatorRunner | None = None,
    authority_refresher: AuthorityRefresher | None = None,
    write: bool = True,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actuator_runner = actuator_runner or _run_actuator
    authority_refresher = authority_refresher or _refresh_operator_authority
    authority_refreshes: list[dict[str, Any]] = []
    actuator_reports: list[dict[str, Any]] = []

    if config.authority_refresh_before_apply:
        pre_refresh = _run_authority_refresh(authority_refresher, config, "before_actuator")
        authority_refreshes.append(pre_refresh)
        if pre_refresh.get("succeeded") is not True:
            payload = _service_payload(
                config=config,
                now=actual_now,
                classification=MANAGED_EXIT_SERVICE_REFRESH_FAILED,
                authority_refreshes=authority_refreshes,
                actuator_reports=actuator_reports,
            )
            if write:
                write_track_b_managed_exit_service_status(config=config, payload=payload)
            return payload

    max_cycles = max(int(config.max_cycles_per_tick or 0), 1)
    for cycle_index in range(max_cycles):
        actuator_config = TrackBManagedExitActuatorConfig(
            repo_root=config.repo_root,
            apply=config.apply is True,
            operator_authorized_managed_exit=config.operator_authorized_managed_exit is True,
            max_closes_per_run=1,
        )
        actuator_report = dict(actuator_runner(actuator_config, actual_now))
        actuator_report["service_cycle_index"] = cycle_index
        actuator_reports.append(actuator_report)

        submitted = int(actuator_report.get("submitted_count") or 0)
        should_refresh_after = config.authority_refresh_after_attempt and (
            submitted > 0 or actuator_report.get("submit_attempted") is True
        )
        if should_refresh_after:
            authority_refreshes.append(_run_authority_refresh(authority_refresher, config, "after_actuator_attempt"))

        if _should_stop_after_actuator(actuator_report):
            break

    classification = _service_classification(actuator_reports)
    payload = _service_payload(
        config=config,
        now=actual_now,
        classification=classification,
        authority_refreshes=authority_refreshes,
        actuator_reports=actuator_reports,
    )
    if write:
        write_track_b_managed_exit_service_status(config=config, payload=payload)
    return payload


def run_track_b_managed_exit_service(
    *,
    config: TrackBManagedExitServiceConfig,
    actuator_runner: ActuatorRunner | None = None,
    authority_refresher: AuthorityRefresher | None = None,
    sleep_func: SleepFunc = time.sleep,
    max_iterations: int | None = None,
) -> int:
    stopping = False

    def _handle_stop(signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True
        payload = {
            "schema_version": "track_b_managed_exit_service_status_v1",
            "generated_at": datetime.now(UTC).isoformat(),
            "classification": MANAGED_EXIT_SERVICE_STOPPING,
            "reason": f"signal={signum}",
            "repo_root": str(config.repo_root),
            "cadence_seconds": config.cadence_seconds,
            "close_only": True,
            "entry_allowed": False,
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        }
        write_track_b_managed_exit_service_status(config=config, payload=payload)
        _write_heartbeat(config=config, payload=payload, service_running=False)

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    iteration = 0
    while not stopping:
        _write_heartbeat(
            config=config,
            payload={
                "classification": MANAGED_EXIT_SERVICE_RUNNING,
                "repo_root": str(config.repo_root),
                "cadence_seconds": config.cadence_seconds,
                "close_only": True,
                "entry_allowed": False,
            },
            service_running=True,
        )
        payload = run_track_b_managed_exit_service_once(
            config=config,
            actuator_runner=actuator_runner,
            authority_refresher=authority_refresher,
            write=True,
        )
        _write_heartbeat(config=config, payload=payload, service_running=True)
        iteration += 1
        if max_iterations is not None and iteration >= max_iterations:
            break
        sleep_func(max(float(config.cadence_seconds), 1.0))
    return 0


def read_track_b_managed_exit_service_status(
    *, repo_root: Path = REPO_ROOT, status_path: Path = DEFAULT_STATUS_PATH
) -> dict[str, Any]:
    resolved = status_path if status_path.is_absolute() else repo_root / status_path
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "schema_version": "track_b_managed_exit_service_status_v1",
            "generated_at": datetime.now(UTC).isoformat(),
            "classification": "TRACK_B_MANAGED_EXIT_SERVICE_STATUS_MISSING",
            "status_path": str(resolved),
            "fresh": False,
            "close_only": True,
            "entry_allowed": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        }
    return dict(payload) if isinstance(payload, Mapping) else {}


def write_track_b_managed_exit_service_status(
    *, config: TrackBManagedExitServiceConfig, payload: Mapping[str, Any]
) -> Path:
    return write_json_atomic(config.resolve(config.status_path), to_jsonable(dict(payload)))


def _service_payload(
    *,
    config: TrackBManagedExitServiceConfig,
    now: datetime,
    classification: str,
    authority_refreshes: Sequence[Mapping[str, Any]],
    actuator_reports: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    attempted = [row for report in actuator_reports for row in report.get("attempted_closes") or []]
    return {
        "schema_version": "track_b_managed_exit_service_status_v1",
        "generated_at": now.isoformat(),
        "classification": classification,
        "repo_root": str(config.repo_root),
        "cadence_seconds": config.cadence_seconds,
        "close_only": True,
        "entry_allowed": False,
        "apply_requested": config.apply is True,
        "operator_authorized_managed_exit": config.operator_authorized_managed_exit is True,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "authority_inputs": [
            "managed_positions",
            "managed_orders",
            "reconciliation",
            "open_order_truth",
            "broker_session_authority",
            "safe_state",
            "guardian",
        ],
        "authority_refresh_before_apply": config.authority_refresh_before_apply,
        "authority_refresh_after_attempt": config.authority_refresh_after_attempt,
        "authority_refreshes": list(authority_refreshes),
        "authority_refresh_failed": any(row.get("succeeded") is not True for row in authority_refreshes),
        "actuator_invocation_count": len(actuator_reports),
        "actuator_reports": list(actuator_reports),
        "latest_actuator_classification": actuator_reports[-1].get("classification") if actuator_reports else None,
        "exit_due_count": _max_int(actuator_reports, "exit_due_count"),
        "eligible_count": _max_int(actuator_reports, "eligible_count"),
        "attempted_count": len(attempted),
        "submitted_count": sum(int(report.get("submitted_count") or 0) for report in actuator_reports),
        "submit_attempted": any(report.get("submit_attempted") is True for report in actuator_reports),
        "broker_state_mutated": any(report.get("broker_state_mutated") is True for report in actuator_reports),
        "attempted_closes": attempted,
        "required_next_action": _next_action(classification),
        "status_path": str(config.resolve(config.status_path)),
    }


def _service_classification(actuator_reports: Sequence[Mapping[str, Any]]) -> str:
    if not actuator_reports:
        return MANAGED_EXIT_SERVICE_NOOP
    classifications = [str(report.get("classification") or "") for report in actuator_reports]
    submitted = sum(int(report.get("submitted_count") or 0) for report in actuator_reports)
    if submitted > 0 and any("BLOCKED" in item or item == MANAGED_EXIT_ACTUATOR_PARTIAL for item in classifications):
        return MANAGED_EXIT_SERVICE_PARTIAL
    if submitted > 0:
        return MANAGED_EXIT_SERVICE_APPLIED_OR_PENDING
    if classifications[-1] == MANAGED_EXIT_ACTUATOR_DRY_RUN_READY:
        return MANAGED_EXIT_SERVICE_DRY_RUN_READY
    if any("BLOCKED" in item for item in classifications):
        return MANAGED_EXIT_SERVICE_BLOCKED
    return MANAGED_EXIT_SERVICE_NOOP


def _should_stop_after_actuator(report: Mapping[str, Any]) -> bool:
    classification = str(report.get("classification") or "")
    submitted = int(report.get("submitted_count") or 0)
    if submitted > 0 and classification in {MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING, MANAGED_EXIT_ACTUATOR_PARTIAL}:
        return False
    return True


def _next_action(classification: str) -> str:
    if classification == MANAGED_EXIT_SERVICE_DRY_RUN_READY:
        return "ENABLE_APPLY_MODE_ONLY_IF_OPERATOR_INTENDS_AUTONOMOUS_CLOSE_SERVICE"
    if classification == MANAGED_EXIT_SERVICE_APPLIED_OR_PENDING:
        return "VERIFY_BROKER_AND_MANAGED_TRUTH_AFTER_GUARDED_CLOSE"
    if classification == MANAGED_EXIT_SERVICE_PARTIAL:
        return "REFRESH_AUTHORITY_AND_REVIEW_BLOCKED_CLOSE"
    if classification == MANAGED_EXIT_SERVICE_REFRESH_FAILED:
        return "REFRESH_AUTHORITY"
    if classification == MANAGED_EXIT_SERVICE_BLOCKED:
        return "OPERATOR_REVIEW_REQUIRED"
    return "NO_ACTION"


def _run_actuator(config: TrackBManagedExitActuatorConfig, now: datetime) -> Mapping[str, Any]:
    return run_track_b_managed_exit_actuator(config=config, now=now, write=True)


def _run_authority_refresh(
    authority_refresher: AuthorityRefresher,
    config: TrackBManagedExitServiceConfig,
    phase: str,
) -> dict[str, Any]:
    try:
        payload = dict(authority_refresher(config, phase))
    except Exception as exc:  # defensive: fail closed and surface the refresh fault.
        return {
            "phase": phase,
            "succeeded": False,
            "classification": "TRACK_B_MANAGED_EXIT_SERVICE_AUTHORITY_REFRESH_EXCEPTION",
            "error": str(exc),
        }
    payload.setdefault("phase", phase)
    payload.setdefault("succeeded", False)
    return payload


def _refresh_operator_authority(config: TrackBManagedExitServiceConfig, phase: str) -> Mapping[str, Any]:
    from mgc_v05l.app.track_b_operator_readiness_refresher import RefreshConfig, refresh_once

    payload = refresh_once(
        config=RefreshConfig(
            repo_root=config.repo_root,
            refresh_seconds=config.cadence_seconds,
            timeout_seconds=config.authority_refresh_timeout_seconds,
        )
    )
    return {
        "phase": phase,
        "succeeded": payload.get("last_success") is True,
        "classification": payload.get("classification"),
        "generated_at": payload.get("generated_at"),
        "dependency_refresh_failures": payload.get("dependency_refresh_failures") or [],
        "status_path": str(config.resolve(config.status_path)),
    }


def _write_heartbeat(
    *,
    config: TrackBManagedExitServiceConfig,
    payload: Mapping[str, Any],
    service_running: bool,
) -> Path | None:
    if config.heartbeat_path is None:
        return None
    heartbeat = {
        "schema_version": "track_b_managed_exit_service_heartbeat_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "service_running": service_running,
        "pid": os.getpid(),
        "classification": payload.get("classification"),
        "cadence_seconds": config.cadence_seconds,
        "status_path": str(config.resolve(config.status_path)),
        "close_only": True,
        "entry_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    return write_json_atomic(config.resolve(config.heartbeat_path), heartbeat)


def _max_int(rows: Sequence[Mapping[str, Any]], key: str) -> int:
    values: list[int] = []
    for row in rows:
        try:
            values.append(int(row.get(key) or 0))
        except (TypeError, ValueError):
            values.append(0)
    return max(values or [0])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--status-path", type=Path, default=DEFAULT_STATUS_PATH)
    parser.add_argument("--heartbeat-path", type=Path, default=DEFAULT_HEARTBEAT_PATH)
    parser.add_argument("--no-heartbeat", action="store_true")
    parser.add_argument("--cadence-seconds", type=float, default=DEFAULT_CADENCE_SECONDS)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--operator-authorized-managed-exit", action="store_true")
    parser.add_argument("--max-cycles-per-tick", type=int, default=4)
    parser.add_argument("--no-authority-refresh-before-apply", action="store_true")
    parser.add_argument("--no-authority-refresh-after-attempt", action="store_true")
    parser.add_argument("--authority-refresh-timeout-seconds", type=float, default=120.0)
    parser.add_argument("--service", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def _config_from_args(args: argparse.Namespace) -> TrackBManagedExitServiceConfig:
    return TrackBManagedExitServiceConfig(
        repo_root=args.repo_root.expanduser().resolve(),
        status_path=args.status_path,
        heartbeat_path=None if args.no_heartbeat else args.heartbeat_path,
        cadence_seconds=args.cadence_seconds,
        apply=bool(args.apply),
        operator_authorized_managed_exit=bool(args.operator_authorized_managed_exit),
        max_cycles_per_tick=args.max_cycles_per_tick,
        authority_refresh_before_apply=not bool(args.no_authority_refresh_before_apply),
        authority_refresh_after_attempt=not bool(args.no_authority_refresh_after_attempt),
        authority_refresh_timeout_seconds=args.authority_refresh_timeout_seconds,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = _config_from_args(args)
    if args.status:
        payload = read_track_b_managed_exit_service_status(repo_root=config.repo_root, status_path=config.status_path)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(f"classification={payload.get('classification')}")
            print(f"generated_at={payload.get('generated_at')}")
        return 0
    if args.service:
        return run_track_b_managed_exit_service(config=config)
    payload = run_track_b_managed_exit_service_once(config=config)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"classification={payload.get('classification')}")
        print(f"submitted_count={payload.get('submitted_count')}")
    return 0 if str(payload.get("classification") or "") != MANAGED_EXIT_SERVICE_REFRESH_FAILED else 2


if __name__ == "__main__":
    raise SystemExit(main())
