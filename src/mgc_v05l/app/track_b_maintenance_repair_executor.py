"""Limited Track B maintenance repair executor for broker-truth sidecar only.

This CLI is intentionally narrow. It can dry-run or apply only broker-truth
sidecar maintenance that the shared maintenance supervisor already recommended.
It never submits/cancels/closes orders, mutates lifecycle, restarts runtime, or
changes strategy behavior.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.track_b_readiness_state import REPO_ROOT

ALLOWED_SUPERVISOR_ACTIONS = {"RESTART_SIDECAR", "REFRESH_BROKER_TRUTH", "RETRY", "ROTATE_CLIENT_ID"}
BLOCKING_SUPERVISOR_STATES = {"OPERATOR_REQUIRED", "BLOCKED"}
DEFAULT_RUNTIME_DIR = Path("outputs") / "operator_dashboard" / "runtime"
DEFAULT_DECISION_ARTIFACT = DEFAULT_RUNTIME_DIR / "latest_maintenance_supervisor_decision.json"
DEFAULT_RESULT_ARTIFACT = DEFAULT_RUNTIME_DIR / "latest_maintenance_repair_result.json"
DEFAULT_HISTORY_ARTIFACT = DEFAULT_RUNTIME_DIR / "maintenance_repair_history.jsonl"
DEFAULT_CANONICAL_READINESS_ARTIFACT = DEFAULT_RUNTIME_DIR / "latest_canonical_readiness.json"
DEFAULT_BROKER_TRUTH_STATUS = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json"
)
DEFAULT_PID_FILE = Path("var") / "track_b_broker_truth_refresh_service.pid"
DEFAULT_ACTION = "broker-truth-sidecar"


@dataclass(frozen=True)
class RepairExecutorConfig:
    repo_root: Path = REPO_ROOT
    action: str = DEFAULT_ACTION
    apply: bool = False
    cooldown_seconds: int = 300
    max_retries: int = 1
    decision_path: Path | None = None
    broker_truth_status_path: Path | None = None
    canonical_readiness_path: Path | None = None
    result_path: Path | None = None
    history_path: Path | None = None
    pid_file: Path | None = None
    python_bin: str | None = None


def run_repair_executor(
    *,
    config: RepairExecutorConfig,
    command_runner: Callable[..., Any] = subprocess.run,
    pid_checker: Callable[[int], bool] | None = None,
    now_fn: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    now_fn = now_fn or (lambda: datetime.now(timezone.utc))
    now = _ensure_utc(now_fn())
    repo_root = Path(config.repo_root).expanduser().resolve()
    paths = _resolve_paths(config, repo_root)
    decision = _read_json(paths["decision"])
    broker_truth_status = _read_json(paths["broker_truth_status"])
    canonical_readiness = _read_json(paths["canonical_readiness"])
    previous_result = _read_json(paths["result"])
    pid = _read_pid(paths["pid_file"])
    pid_checker = pid_checker or _pid_alive
    sidecar_alive = bool(pid is not None and pid_checker(int(pid)))

    base = _base_result(
        config=config,
        now=now,
        paths=paths,
        decision=decision,
        broker_truth_status=broker_truth_status,
        canonical_readiness=canonical_readiness,
        pid=pid,
        sidecar_alive=sidecar_alive,
    )
    blocked_reason = _apply_blocker(config=config, decision=decision, previous_result=previous_result, now=now)
    selected = _select_repair(
        decision=decision,
        broker_truth_status=broker_truth_status,
        sidecar_alive=sidecar_alive,
        retry_count=int(previous_result.get("retry_count") or 0),
        max_retries=int(config.max_retries),
    )
    result = {**base, **selected}
    if blocked_reason:
        result.update(
            {
                "classification": "TRACK_B_MAINTENANCE_REPAIR_APPLY_BLOCKED"
                if config.apply
                else "TRACK_B_MAINTENANCE_REPAIR_DRY_RUN_BLOCKED",
                "blocked_reason": blocked_reason,
                "would_execute": False,
                "executed": False,
            }
        )
        _write_result_and_history(paths=paths, result=result)
        return result

    commands = _commands_for_repair(selected["repair_action"], repo_root=repo_root, python_bin=_python_bin(config))
    result["commands"] = [_command_display(command) for command in commands]
    result["would_execute"] = bool(commands)
    if not config.apply:
        result.update(
            {
                "classification": "TRACK_B_MAINTENANCE_REPAIR_DRY_RUN_READY"
                if commands
                else "TRACK_B_MAINTENANCE_REPAIR_DRY_RUN_NO_ACTION",
                "executed": False,
            }
        )
        _write_result_and_history(paths=paths, result=result)
        return result

    executed_commands: list[dict[str, Any]] = []
    success = True
    for command in commands:
        completed = command_runner(
            command,
            cwd=str(repo_root),
            check=False,
            capture_output=True,
            text=True,
        )
        command_result = _completed_process_payload(command, completed)
        executed_commands.append(command_result)
        if int(command_result["returncode"]) != 0:
            success = False
            break
    canonical_rerun = None
    if success and commands:
        canonical_rerun = _rerun_canonical_readiness(
            repo_root=repo_root,
            output_path=paths["canonical_readiness"],
            python_bin=_python_bin(config),
            command_runner=command_runner,
        )
        if int(canonical_rerun["returncode"]) not in {0, 1, 2}:
            success = False
    result.update(
        {
            "classification": "TRACK_B_MAINTENANCE_REPAIR_APPLIED"
            if success
            else "TRACK_B_MAINTENANCE_REPAIR_APPLY_FAILED",
            "executed": bool(commands),
            "executed_commands": executed_commands,
            "canonical_readiness_rerun": canonical_rerun,
            "retry_count": int(previous_result.get("retry_count") or 0) + (1 if commands else 0),
            "cooldown_until": (now + timedelta(seconds=max(int(config.cooldown_seconds), 0))).isoformat()
            if commands
            else None,
        }
    )
    _write_result_and_history(paths=paths, result=result)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="track-b-maintenance-repair-executor")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--action", default=DEFAULT_ACTION, choices=[DEFAULT_ACTION])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Report what would be done. Default.")
    mode.add_argument("--apply", action="store_true", help="Execute the allowed broker-truth sidecar repair.")
    parser.add_argument("--cooldown-seconds", type=int, default=300)
    parser.add_argument("--max-retries", type=int, default=1)
    parser.add_argument("--decision-path", default=None)
    parser.add_argument("--broker-truth-status-path", default=None)
    parser.add_argument("--canonical-readiness-path", default=None)
    parser.add_argument("--result-path", default=None)
    parser.add_argument("--history-path", default=None)
    parser.add_argument("--pid-file", default=None)
    parser.add_argument("--python-bin", default=None)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root).expanduser().resolve()
    config = RepairExecutorConfig(
        repo_root=repo_root,
        action=str(args.action),
        apply=bool(args.apply),
        cooldown_seconds=int(args.cooldown_seconds),
        max_retries=int(args.max_retries),
        decision_path=_optional_path(args.decision_path),
        broker_truth_status_path=_optional_path(args.broker_truth_status_path),
        canonical_readiness_path=_optional_path(args.canonical_readiness_path),
        result_path=_optional_path(args.result_path),
        history_path=_optional_path(args.history_path),
        pid_file=_optional_path(args.pid_file),
        python_bin=args.python_bin,
    )
    result = run_repair_executor(config=config)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_summary(result)
    if str(result.get("classification", "")).endswith("BLOCKED"):
        return 2
    if str(result.get("classification", "")).endswith("FAILED"):
        return 1
    return 0


def _base_result(
    *,
    config: RepairExecutorConfig,
    now: datetime,
    paths: Mapping[str, Path],
    decision: Mapping[str, Any],
    broker_truth_status: Mapping[str, Any],
    canonical_readiness: Mapping[str, Any],
    pid: int | None,
    sidecar_alive: bool,
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_maintenance_repair_result_v1",
        "generated_at": now.isoformat(),
        "paper_only": True,
        "action": config.action,
        "mode": "apply" if config.apply else "dry_run",
        "decision_artifact": str(paths["decision"]),
        "broker_truth_status_artifact": str(paths["broker_truth_status"]),
        "canonical_readiness_artifact": str(paths["canonical_readiness"]),
        "broker_truth_pid_file": str(paths["pid_file"]),
        "broker_truth_pid": pid,
        "broker_truth_sidecar_alive": sidecar_alive,
        "supervisor_state": decision.get("supervisor_state"),
        "supervisor_recommended_actions": list(decision.get("recommended_actions") or []),
        "canonical_readiness": canonical_readiness.get("canonical_readiness") or canonical_readiness.get("state"),
        "broker_truth_classification": broker_truth_status.get("classification"),
        "submit_authority": False,
        "live_money_eligible": False,
        "authority": {
            "paper_only": True,
            "broker_mutation_allowed": False,
            "order_api_allowed": False,
            "runtime_restart_allowed": False,
            "sidecar_restart_scope": "broker_truth_only",
            "lifecycle_mutation_allowed": False,
            "submit_authority": False,
            "live_money_eligible": False,
        },
    }


def _select_repair(
    *,
    decision: Mapping[str, Any],
    broker_truth_status: Mapping[str, Any],
    sidecar_alive: bool,
    retry_count: int,
    max_retries: int,
) -> dict[str, Any]:
    actions = set(str(action) for action in (decision.get("recommended_actions") or []))
    relevant = actions & ALLOWED_SUPERVISOR_ACTIONS
    if not relevant:
        return {
            "repair_action": "NO_ACTION",
            "reason": "Supervisor did not recommend a broker-truth sidecar repair action.",
            "retry_count": retry_count,
        }
    if retry_count >= max_retries:
        return {
            "repair_action": "NO_ACTION",
            "reason": "Maximum repair retry budget is exhausted.",
            "retry_count": retry_count,
        }
    if not sidecar_alive:
        return {
            "repair_action": "START_BROKER_TRUTH_SIDECAR",
            "reason": "Broker-truth sidecar is not alive and supervisor recommended broker-truth repair.",
            "retry_count": retry_count,
        }
    stale_or_failing = (
        broker_truth_status.get("fresh") is not True
        or broker_truth_status.get("last_failure") is True
        or str(broker_truth_status.get("classification") or "").endswith("FAILED")
        or "REFRESH_BROKER_TRUTH" in relevant
        or "RETRY" in relevant
        or "ROTATE_CLIENT_ID" in relevant
    )
    if stale_or_failing:
        return {
            "repair_action": "REFRESH_BROKER_TRUTH_ONCE",
            "reason": "Broker-truth sidecar is alive but broker truth is stale/failing or supervisor requested refresh.",
            "retry_count": retry_count,
        }
    return {
        "repair_action": "NO_ACTION",
        "reason": "Broker-truth sidecar is alive and broker truth does not require immediate repair.",
        "retry_count": retry_count,
    }


def _apply_blocker(
    *,
    config: RepairExecutorConfig,
    decision: Mapping[str, Any],
    previous_result: Mapping[str, Any],
    now: datetime,
) -> str | None:
    if config.action != DEFAULT_ACTION:
        return "Only --action broker-truth-sidecar is supported."
    supervisor_state = str(decision.get("supervisor_state") or "")
    canonical = str(decision.get("canonical_readiness") or "")
    blocker_codes = {
        str(row.get("code"))
        for row in decision.get("blockers") or []
        if isinstance(row, Mapping) and row.get("code")
    }
    if supervisor_state in BLOCKING_SUPERVISOR_STATES:
        return f"Supervisor state {supervisor_state} blocks repair apply."
    if canonical == "NOT_READY_WRONG_ROOT" or "wrong_root_process" in blocker_codes:
        return "Wrong-root readiness blocks broker-truth repair apply."
    cooldown_until = _parse_iso(previous_result.get("cooldown_until"))
    if config.apply and cooldown_until is not None and now < cooldown_until:
        return f"Repair cooldown is active until {cooldown_until.isoformat()}."
    return None


def _commands_for_repair(repair_action: str, *, repo_root: Path, python_bin: str) -> list[list[str]]:
    if repair_action == "START_BROKER_TRUTH_SIDECAR":
        return [["bash", str(repo_root / "scripts" / "start-track-b-broker-truth-refresh")]]
    if repair_action == "REFRESH_BROKER_TRUTH_ONCE":
        return [
            [
                python_bin,
                "-m",
                "mgc_v05l.app.ibkr_broker_truth_refresher",
                "--once",
                "--mode",
                "PAPER",
                "--host",
                "127.0.0.1",
                "--port",
                "7497",
                "--client-id",
                "9087",
                "--account-id",
                "DUM882026",
                "--read-only",
                "--timeout-seconds",
                "8",
            ]
        ]
    return []


def _rerun_canonical_readiness(
    *,
    repo_root: Path,
    output_path: Path,
    python_bin: str,
    command_runner: Callable[..., Any],
) -> dict[str, Any]:
    command = [
        python_bin,
        "-m",
        "mgc_v05l.app.track_b_canonical_readiness",
        "--repo-root",
        str(repo_root),
        "--expected-root",
        str(repo_root),
        "--output-path",
        str(output_path),
        "--json",
    ]
    completed = command_runner(command, cwd=str(repo_root), check=False, capture_output=True, text=True)
    return _completed_process_payload(command, completed)


def _write_result_and_history(*, paths: Mapping[str, Path], result: Mapping[str, Any]) -> None:
    _write_json(paths["result"], result)
    history_path = paths["history"]
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(result, sort_keys=True) + "\n")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _resolve_paths(config: RepairExecutorConfig, repo_root: Path) -> dict[str, Path]:
    return {
        "decision": _resolve(repo_root, config.decision_path, DEFAULT_DECISION_ARTIFACT),
        "broker_truth_status": _resolve(repo_root, config.broker_truth_status_path, DEFAULT_BROKER_TRUTH_STATUS),
        "canonical_readiness": _resolve(repo_root, config.canonical_readiness_path, DEFAULT_CANONICAL_READINESS_ARTIFACT),
        "result": _resolve(repo_root, config.result_path, DEFAULT_RESULT_ARTIFACT),
        "history": _resolve(repo_root, config.history_path, DEFAULT_HISTORY_ARTIFACT),
        "pid_file": _resolve(repo_root, config.pid_file, DEFAULT_PID_FILE),
    }


def _resolve(repo_root: Path, explicit: Path | None, default_relative: Path) -> Path:
    path = explicit if explicit is not None else default_relative
    path = Path(path)
    return path if path.is_absolute() else repo_root / path


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_pid(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _python_bin(config: RepairExecutorConfig) -> str:
    return str(config.python_bin or os.environ.get("PYTHON_BIN") or "python")


def _completed_process_payload(command: Sequence[str], completed: Any) -> dict[str, Any]:
    return {
        "command": _command_display(command),
        "returncode": int(getattr(completed, "returncode", 1)),
        "stdout": str(getattr(completed, "stdout", "") or "")[-2000:],
        "stderr": str(getattr(completed, "stderr", "") or "")[-2000:],
    }


def _command_display(command: Sequence[str]) -> list[str]:
    return [str(part) for part in command]


def _optional_path(value: str | None) -> Path | None:
    return Path(value).expanduser() if value else None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _print_summary(result: Mapping[str, Any]) -> None:
    for key in (
        "classification",
        "mode",
        "repair_action",
        "reason",
        "blocked_reason",
        "broker_truth_sidecar_alive",
        "would_execute",
        "executed",
        "canonical_readiness",
        "live_money_eligible",
    ):
        print(f"{key}={result.get(key)}")


if __name__ == "__main__":
    raise SystemExit(main())
