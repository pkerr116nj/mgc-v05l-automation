"""Close-only Track B PAPER managed-exit actuator.

This module is the runtime-independent caller for exact, owned, risk-reducing
managed exits. It never creates entries and delegates broker mutation to the
guarded managed-exit attach boundary.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_managed_exit_attach import (
    TrackBManagedExitAttachConfig,
    run_track_b_managed_exit_attach,
)
from mgc_v05l.execution_core.track_b_managed_exit_recovery import (
    EXIT_DUE_CLOSE_PARTIAL_READY,
    EXIT_DUE_CLOSE_READY,
    TrackBManagedExitRecoveryConfig,
    build_track_b_managed_exit_recovery_plan,
)
from mgc_v05l.execution_core.models import require_aware_datetime, to_jsonable


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_MANAGED_EXIT_ACTUATOR_REPORT = (
    Path("outputs") / "track_b_execution_core" / "managed_exit_actuator" / "latest_managed_exit_actuator.json"
)

MANAGED_EXIT_ACTUATOR_NOOP = "MANAGED_EXIT_ACTUATOR_NOOP"
MANAGED_EXIT_ACTUATOR_DRY_RUN_READY = "MANAGED_EXIT_ACTUATOR_DRY_RUN_READY"
MANAGED_EXIT_ACTUATOR_BLOCKED = "MANAGED_EXIT_ACTUATOR_BLOCKED"
MANAGED_EXIT_ACTUATOR_PARTIAL = "MANAGED_EXIT_ACTUATOR_PARTIAL"
MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING = "MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING"
MANAGED_EXIT_ACTUATOR_ATTACH_TIMEOUT = "MANAGED_EXIT_ACTUATOR_ATTACH_TIMEOUT"
MANAGED_EXIT_ACTUATOR_PHASE_RUNNING = "MANAGED_EXIT_ACTUATOR_PHASE_RUNNING"


@dataclass(frozen=True)
class TrackBManagedExitActuatorConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_MANAGED_EXIT_ACTUATOR_REPORT
    apply: bool = False
    operator_authorized_managed_exit: bool = False
    max_closes_per_run: int | None = None
    attach_timeout_seconds: float = 90.0
    recovery_config: TrackBManagedExitRecoveryConfig | None = None

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


AttachRunner = Callable[[TrackBManagedExitAttachConfig, datetime], Mapping[str, Any]]
RefreshHook = Callable[[dict[str, Any]], None]
CommandRunner = Callable[[Sequence[str], Path, float], subprocess.CompletedProcess[str]]


def build_track_b_managed_exit_actuator_report(
    *,
    config: TrackBManagedExitActuatorConfig,
    now: datetime | None = None,
    input_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    phase_timings: list[dict[str, Any]] = []
    _record_phase(phase_timings, "startup", actual_now)
    recovery = _recovery_plan(config=config, now=actual_now, input_overrides=input_overrides)
    _record_phase(phase_timings, "candidate_discovery", actual_now)
    eligible = _eligible_positions(recovery)
    if not eligible:
        classification = MANAGED_EXIT_ACTUATOR_NOOP if int(recovery.get("exit_due_count") or 0) == 0 else MANAGED_EXIT_ACTUATOR_BLOCKED
    elif config.apply is not True or config.operator_authorized_managed_exit is not True:
        classification = MANAGED_EXIT_ACTUATOR_DRY_RUN_READY
    else:
        classification = MANAGED_EXIT_ACTUATOR_DRY_RUN_READY
    return _base_report(
        config=config,
        now=actual_now,
        recovery=recovery,
        classification=classification,
        attempted=[],
        phase_timings=phase_timings,
    )


def run_track_b_managed_exit_actuator(
    *,
    config: TrackBManagedExitActuatorConfig,
    now: datetime | None = None,
    input_overrides: Mapping[str, Mapping[str, Any]] | None = None,
    attach_runner: AttachRunner | None = None,
    command_runner: CommandRunner | None = None,
    refresh_hook: RefreshHook | None = None,
    write: bool = True,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    attach_runner = attach_runner or (
        lambda attach_config, attach_now: _run_attach_child(
            attach_config,
            attach_now,
            timeout_seconds=config.attach_timeout_seconds,
            command_runner=command_runner,
        )
    )
    attempted: list[dict[str, Any]] = []
    phase_timings: list[dict[str, Any]] = []
    _record_phase(phase_timings, "startup", actual_now)
    if write:
        _write_phase_status(config=config, now=actual_now, phase="candidate_discovery", phase_timings=phase_timings)
    recovery = _recovery_plan(config=config, now=actual_now, input_overrides=input_overrides)
    _record_phase(phase_timings, "candidate_discovery", actual_now)
    initial_eligible = _eligible_positions(recovery)

    if not initial_eligible:
        classification = MANAGED_EXIT_ACTUATOR_NOOP if int(recovery.get("exit_due_count") or 0) == 0 else MANAGED_EXIT_ACTUATOR_BLOCKED
        report = _base_report(
            config=config,
            now=actual_now,
            recovery=recovery,
            classification=classification,
            attempted=attempted,
            phase_timings=phase_timings,
        )
        if write:
            write_track_b_managed_exit_actuator_report(config=config, payload=report)
        return report

    if config.apply is not True or config.operator_authorized_managed_exit is not True:
        report = _base_report(
            config=config,
            now=actual_now,
            recovery=recovery,
            classification=MANAGED_EXIT_ACTUATOR_DRY_RUN_READY,
            attempted=attempted,
            phase_timings=phase_timings,
        )
        if write:
            write_track_b_managed_exit_actuator_report(config=config, payload=report)
        return report

    limit = config.max_closes_per_run if config.max_closes_per_run is not None else len(initial_eligible)
    for planned in initial_eligible[: max(limit, 0)]:
        if write:
            _write_phase_status(config=config, now=actual_now, phase="pre_submit_recheck", phase_timings=phase_timings)
        latest_recovery = _recovery_plan(config=config, now=actual_now, input_overrides=input_overrides)
        _record_phase(phase_timings, "pre_submit_recheck", actual_now)
        latest = _matching_eligible_position(_eligible_positions(latest_recovery), planned)
        if latest is None:
            attempted.append(
                {
                    "classification": "MANAGED_EXIT_ACTUATOR_RECHECK_BLOCKED",
                    "identity": planned.get("identity"),
                    "close_candidate": planned.get("close_candidate"),
                    "submitted": False,
                    "submit_attempted": False,
                    "broker_state_mutated": False,
                    "blocker": "Candidate was no longer apply-eligible on immediate pre-submit recheck.",
                    "rechecked": True,
                }
            )
            recovery = latest_recovery
            break
        attach_config = _attach_config(config=config, position=latest)
        if write:
            _write_phase_status(config=config, now=actual_now, phase="guarded_attach", phase_timings=phase_timings)
        attach_result = dict(attach_runner(attach_config, actual_now))
        _record_phase(phase_timings, "guarded_attach", actual_now)
        attempted_row = {
            "classification": attach_result.get("classification"),
            "identity": latest.get("identity"),
            "close_candidate": latest.get("close_candidate"),
            "submitted": attach_result.get("submit_attempted") is True,
            "submit_attempted": attach_result.get("submit_attempted") is True,
            "broker_state_mutated": attach_result.get("broker_state_mutated") is True,
            "order_id": _nested(attach_result, "apply_result", "close_submit_attempt", "broker_order_id")
            or _nested(attach_result, "close_submit_attempt", "broker_order_id"),
            "perm_id": _nested(attach_result, "apply_result", "close_submit_attempt", "perm_id")
            or _nested(attach_result, "close_submit_attempt", "perm_id"),
            "fill": attach_result.get("close_fill") or _nested(attach_result, "apply_result", "close_fill"),
            "primary_blocker": attach_result.get("primary_blocker") or _nested(attach_result, "apply_result", "primary_blocker"),
            "attach_output_path": str(attach_config.resolve(attach_config.output_path)),
            "rechecked": True,
        }
        attempted.append(attempted_row)
        if refresh_hook is not None:
            refresh_hook(attempted_row)
        recovery = _recovery_plan(config=config, now=actual_now, input_overrides=input_overrides)
        _record_phase(phase_timings, "post_attempt_recovery_refresh", actual_now)
        if _unsafe_after_attempt(attempted_row):
            break

    submitted = [row for row in attempted if row.get("submit_attempted") is True]
    blocked_attempts = [row for row in attempted if row.get("submit_attempted") is not True]
    if submitted and not blocked_attempts:
        classification = MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING
    elif submitted and blocked_attempts:
        classification = MANAGED_EXIT_ACTUATOR_PARTIAL
    else:
        classification = MANAGED_EXIT_ACTUATOR_BLOCKED
    report = _base_report(
        config=config,
        now=actual_now,
        recovery=recovery,
        classification=classification,
        attempted=attempted,
        phase_timings=phase_timings,
    )
    if write:
        write_track_b_managed_exit_actuator_report(config=config, payload=report)
    return report


def write_track_b_managed_exit_actuator_report(
    *, config: TrackBManagedExitActuatorConfig, payload: Mapping[str, Any]
) -> Path:
    return write_json_atomic(config.resolve(config.output_path), to_jsonable(dict(payload)))


def _base_report(
    *,
    config: TrackBManagedExitActuatorConfig,
    now: datetime,
    recovery: Mapping[str, Any],
    classification: str,
    attempted: Sequence[Mapping[str, Any]],
    phase_timings: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    eligible = _eligible_positions(recovery)
    return {
        "schema_version": "track_b_managed_exit_actuator_v1",
        "generated_at": now.isoformat(),
        "classification": classification,
        "mode": "PAPER",
        "close_only": True,
        "entry_allowed": False,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "phase_timings": list(phase_timings or []),
        "latest_phase": (phase_timings or [{}])[-1].get("phase") if phase_timings else None,
        "attach_timeout_seconds": config.attach_timeout_seconds,
        "apply_requested": config.apply is True,
        "operator_authorized_managed_exit": config.operator_authorized_managed_exit is True,
        "broker_state_mutated": any(row.get("broker_state_mutated") is True for row in attempted),
        "submit_attempted": any(row.get("submit_attempted") is True for row in attempted),
        "eligible_count": len(eligible),
        "exit_due_count": int(recovery.get("exit_due_count") or 0),
        "blocked_count": int(recovery.get("blocked_count") or 0),
        "attempted_count": len(attempted),
        "submitted_count": sum(1 for row in attempted if row.get("submit_attempted") is True),
        "eligible_positions": eligible,
        "blocked_positions": recovery.get("blocked_positions") or [],
        "attempted_closes": list(attempted),
        "recovery_classification": recovery.get("classification"),
        "recovery_plan_path": str(_recovery_config(config).resolve(_recovery_config(config).output_path)),
        "output_path": str(config.resolve(config.output_path)),
        "required_next_action": _next_action(classification=classification, attempted=attempted),
    }


def _next_action(*, classification: str, attempted: Sequence[Mapping[str, Any]]) -> str:
    if classification == MANAGED_EXIT_ACTUATOR_DRY_RUN_READY:
        return "Rerun with apply authorization only if exact close authority remains current."
    if classification == MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING:
        return "Refresh broker truth, managed orders/positions, reconciliation, and verify close fill or protected working close."
    if classification == MANAGED_EXIT_ACTUATOR_PARTIAL:
        return "Refresh authority and review the blocked close before another actuator pass."
    if attempted:
        return "Review guarded attach blocker before retrying."
    return "No actuator close submitted."


def _recovery_plan(
    *,
    config: TrackBManagedExitActuatorConfig,
    now: datetime,
    input_overrides: Mapping[str, Mapping[str, Any]] | None,
) -> dict[str, Any]:
    return build_track_b_managed_exit_recovery_plan(
        config=_recovery_config(config),
        now=now,
        input_overrides=input_overrides,
    )


def _recovery_config(config: TrackBManagedExitActuatorConfig) -> TrackBManagedExitRecoveryConfig:
    return config.recovery_config or TrackBManagedExitRecoveryConfig(repo_root=config.repo_root)


def _eligible_positions(recovery: Mapping[str, Any]) -> list[dict[str, Any]]:
    if str(recovery.get("classification") or "") not in {EXIT_DUE_CLOSE_READY, EXIT_DUE_CLOSE_PARTIAL_READY}:
        return []
    return [dict(row) for row in recovery.get("eligible_positions") or [] if isinstance(row, Mapping)]


def _matching_eligible_position(positions: Sequence[Mapping[str, Any]], planned: Mapping[str, Any]) -> dict[str, Any] | None:
    planned_identity = _identity_key(planned)
    for row in positions:
        if _identity_key(row) == planned_identity:
            return dict(row)
    return None


def _identity_key(row: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    identity = row.get("identity") if isinstance(row.get("identity"), Mapping) else {}
    candidate = row.get("close_candidate") if isinstance(row.get("close_candidate"), Mapping) else {}
    return (
        str(identity.get("account_id") or candidate.get("account_id") or ""),
        str(identity.get("local_symbol") or candidate.get("local_symbol") or ""),
        str(identity.get("con_id") or candidate.get("con_id") or ""),
        str(identity.get("lifecycle_id") or candidate.get("lifecycle_id") or ""),
        str(identity.get("trade_id") or candidate.get("trade_id") or ""),
    )


def _attach_config(*, config: TrackBManagedExitActuatorConfig, position: Mapping[str, Any]) -> TrackBManagedExitAttachConfig:
    candidate = position.get("close_candidate") if isinstance(position.get("close_candidate"), Mapping) else {}
    broker_position = position.get("broker_position") if isinstance(position.get("broker_position"), Mapping) else {}
    lifecycle_position = broker_position.get("canonical_managed_position") if isinstance(broker_position.get("canonical_managed_position"), Mapping) else {}
    symbol = str(candidate.get("symbol") or broker_position.get("symbol") or broker_position.get("track_b_root") or "").upper()
    local_symbol = str(candidate.get("local_symbol") or broker_position.get("local_symbol") or "")
    return TrackBManagedExitAttachConfig(
        repo_root=config.repo_root,
        account_id=str(candidate.get("account_id") or broker_position.get("account_id") or "DUM882026"),
        expected_account_id=str(candidate.get("account_id") or broker_position.get("account_id") or "DUM882026"),
        strategy_id=str(candidate.get("strategy_id") or position.get("strategy_id") or lifecycle_position.get("strategy_id") or ""),
        lane_id=str(candidate.get("lane_id") or position.get("lane_id") or lifecycle_position.get("lane_id") or ""),
        lifecycle_id=str(candidate.get("lifecycle_id") or ""),
        instrument_family=symbol,
        contract_key=str(position.get("contract_key") or lifecycle_position.get("contract_key") or f"{symbol}-202606"),
        local_symbol=local_symbol,
        con_id=_int(candidate.get("con_id") or broker_position.get("con_id")),
        expiry=str(broker_position.get("expiry") or lifecycle_position.get("expiry") or ""),
        side=str(position.get("broker_position_side") or position.get("side") or lifecycle_position.get("side") or _side_from_candidate(candidate)),
        quantity=_int(candidate.get("quantity")) or 1,
        apply=True,
        operator_authorized_managed_exit=True,
        refresh_control_plane=False,
        auto_select_active_managed_position=True,
    )


def _side_from_candidate(candidate: Mapping[str, Any]) -> str:
    action = str(candidate.get("action") or "").upper()
    if action == "BUY":
        return "SHORT"
    if action == "SELL":
        return "LONG"
    return ""


def _run_attach(config: TrackBManagedExitAttachConfig, now: datetime) -> Mapping[str, Any]:
    return run_track_b_managed_exit_attach(config=config, now=now)


def _run_attach_child(
    config: TrackBManagedExitAttachConfig,
    now: datetime,
    *,
    timeout_seconds: float,
    command_runner: CommandRunner | None = None,
) -> Mapping[str, Any]:
    command_runner = command_runner or _run_command
    command = [
        sys.executable,
        "-m",
        "mgc_v05l.execution_core.track_b_managed_exit_attach",
        "--repo-root",
        str(config.repo_root),
        "--account",
        config.account_id,
        "--strategy-id",
        config.strategy_id,
        "--lane-id",
        config.lane_id,
        "--runtime-generation-id",
        config.runtime_generation_id,
        "--lifecycle-id",
        config.lifecycle_id,
        "--instrument-family",
        config.instrument_family,
        "--contract-key",
        config.contract_key,
        "--local-symbol",
        config.local_symbol,
        "--con-id",
        str(config.con_id),
        "--expiry",
        config.expiry,
        "--quantity",
        str(config.quantity),
        "--side",
        config.side,
        "--skip-control-plane-refresh",
        "--json",
    ]
    if config.close_limit_price:
        command.extend(["--close-limit-price", str(config.close_limit_price)])
    if config.apply:
        command.append("--apply")
    if config.operator_authorized_managed_exit:
        command.append("--operator-authorized-managed-exit")
    completed = command_runner(command, config.repo_root, timeout_seconds)
    if completed.returncode == 124:
        return {
            "classification": MANAGED_EXIT_ACTUATOR_ATTACH_TIMEOUT,
            "generated_at": now.isoformat(),
            "timeout_seconds": timeout_seconds,
            "command": command,
            "stdout_tail": _tail(completed.stdout),
            "stderr_tail": _tail(completed.stderr),
            "submit_attempted": False,
            "broker_state_mutated": False,
            "primary_blocker": "BROKER_HANDSHAKE_OR_ATTACH_TIMEOUT_BEFORE_SUBMIT",
        }
    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError:
        return {
            "classification": MANAGED_EXIT_ACTUATOR_BLOCKED,
            "generated_at": now.isoformat(),
            "returncode": completed.returncode,
            "command": command,
            "stdout_tail": _tail(completed.stdout),
            "stderr_tail": _tail(completed.stderr),
            "submit_attempted": False,
            "broker_state_mutated": False,
            "primary_blocker": "MANAGED_EXIT_ATTACH_INVALID_JSON",
        }
    if isinstance(payload, Mapping):
        result = dict(payload)
        result.setdefault("returncode", completed.returncode)
        result.setdefault("command", command)
        result.setdefault("stdout_tail", _tail(completed.stdout))
        result.setdefault("stderr_tail", _tail(completed.stderr))
        return result
    return {
        "classification": MANAGED_EXIT_ACTUATOR_BLOCKED,
        "generated_at": now.isoformat(),
        "returncode": completed.returncode,
        "command": command,
        "stdout_tail": _tail(completed.stdout),
        "stderr_tail": _tail(completed.stderr),
        "submit_attempted": False,
        "broker_state_mutated": False,
        "primary_blocker": "MANAGED_EXIT_ATTACH_NON_OBJECT_JSON",
    }


def _run_command(command: Sequence[str], repo_root: Path, timeout_seconds: float) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{repo_root / 'src'}{':' + existing_pythonpath if existing_pythonpath else ''}"
    process = subprocess.Popen(
        list(command),
        cwd=repo_root,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except OSError:
            process.kill()
        stdout, stderr = process.communicate(timeout=10)
        return subprocess.CompletedProcess(list(command), 124, stdout=stdout or "", stderr=stderr or "")
    return subprocess.CompletedProcess(list(command), process.returncode, stdout=stdout or "", stderr=stderr or "")


def _record_phase(phase_timings: list[dict[str, Any]], phase: str, started_at: datetime) -> None:
    phase_timings.append(
        {
            "phase": phase,
            "recorded_at": datetime.now(UTC).isoformat(),
            "elapsed_seconds": round(max((datetime.now(UTC) - started_at).total_seconds(), 0.0), 3),
        }
    )


def _write_phase_status(
    *,
    config: TrackBManagedExitActuatorConfig,
    now: datetime,
    phase: str,
    phase_timings: Sequence[Mapping[str, Any]],
) -> None:
    write_track_b_managed_exit_actuator_report(
        config=config,
        payload={
            "schema_version": "track_b_managed_exit_actuator_v1",
            "generated_at": now.isoformat(),
            "classification": MANAGED_EXIT_ACTUATOR_PHASE_RUNNING,
            "phase": phase,
            "phase_timings": list(phase_timings),
            "mode": "PAPER",
            "close_only": True,
            "entry_allowed": False,
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
            "apply_requested": config.apply is True,
            "operator_authorized_managed_exit": config.operator_authorized_managed_exit is True,
            "submit_attempted": False,
            "submitted_count": 0,
            "broker_state_mutated": False,
            "output_path": str(config.resolve(config.output_path)),
        },
    )


def _tail(value: str | None, limit: int = 4000) -> str:
    text = value or ""
    return text[-limit:]


def _unsafe_after_attempt(row: Mapping[str, Any]) -> bool:
    return row.get("submit_attempted") is not True


def _nested(payload: Mapping[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_MANAGED_EXIT_ACTUATOR_REPORT)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--operator-authorized-managed-exit", action="store_true")
    parser.add_argument("--max-closes-per-run", type=int)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBManagedExitActuatorConfig(
        repo_root=args.repo_root.expanduser().resolve(),
        output_path=args.output_path,
        apply=bool(args.apply),
        operator_authorized_managed_exit=bool(args.operator_authorized_managed_exit),
        max_closes_per_run=args.max_closes_per_run,
    )
    payload = run_track_b_managed_exit_actuator(config=config, write=not args.no_write)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"classification={payload.get('classification')}")
        print(f"eligible_count={payload.get('eligible_count')}")
        print(f"submitted_count={payload.get('submitted_count')}")
    return 0 if payload.get("classification") != MANAGED_EXIT_ACTUATOR_BLOCKED else 2


if __name__ == "__main__":
    raise SystemExit(main())
