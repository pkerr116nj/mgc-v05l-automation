"""Read-only Track B operator readiness artifact refresher."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_STATUS_PATH = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "track_b_operator_readiness_refresher"
    / "latest_track_b_operator_readiness_refresher_status.json"
)
DEFAULT_REFRESH_SECONDS = 60.0
DEFAULT_PREFLIGHT_MODE = "monday-live"


@dataclass(frozen=True)
class RefreshCommandResult:
    name: str
    command: list[str]
    returncode: int
    stdout_tail: str
    stderr_tail: str
    duration_seconds: float


@dataclass(frozen=True)
class RefreshConfig:
    repo_root: Path = REPO_ROOT
    status_path: Path = DEFAULT_STATUS_PATH
    refresh_seconds: float = DEFAULT_REFRESH_SECONDS
    preflight_mode: str = DEFAULT_PREFLIGHT_MODE
    timeout_seconds: float = 120.0


Runner = Callable[[Sequence[str], Path, float], subprocess.CompletedProcess[str]]


def refresh_once(*, config: RefreshConfig, runner: Runner | None = None) -> dict[str, Any]:
    repo_root = Path(config.repo_root)
    runner = runner or _run_command
    started = _utc_now()
    commands = _refresh_commands(repo_root=repo_root, preflight_mode=config.preflight_mode)
    results: list[RefreshCommandResult] = []
    for name, command in commands:
        command_started = time.monotonic()
        completed = runner(command, repo_root, config.timeout_seconds)
        results.append(
            RefreshCommandResult(
                name=name,
                command=list(command),
                returncode=completed.returncode,
                stdout_tail=_tail(completed.stdout),
                stderr_tail=_tail(completed.stderr),
                duration_seconds=round(time.monotonic() - command_started, 3),
            )
        )
    succeeded = all(result.returncode == 0 for result in results)
    payload = _status_payload(
        config=config,
        started_at=started,
        command_results=results,
        succeeded=succeeded,
    )
    _write_json_atomic(config.status_path, payload)
    return payload


def run_service(*, config: RefreshConfig) -> int:
    stopping = False

    def _handle_stop(signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True
        payload = {
            "schema_version": "track_b_operator_readiness_refresher_status_v1",
            "generated_at": _utc_now().isoformat(),
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_STOPPING",
            "reason": f"signal={signum}",
            "repo_root": str(config.repo_root),
            "refresh_seconds": config.refresh_seconds,
            "preflight_mode": config.preflight_mode,
            "submit_authority": False,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        }
        _write_json_atomic(config.status_path, payload)

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    while not stopping:
        refresh_once(config=config)
        deadline = time.monotonic() + max(config.refresh_seconds, 1.0)
        while not stopping and time.monotonic() < deadline:
            time.sleep(min(1.0, deadline - time.monotonic()))
    return 0


def read_status(*, status_path: Path = DEFAULT_STATUS_PATH) -> dict[str, Any]:
    try:
        return json.loads(Path(status_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {
            "schema_version": "track_b_operator_readiness_refresher_status_v1",
            "generated_at": _utc_now().isoformat(),
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_STATUS_MISSING",
            "path": str(status_path),
            "refresh_running": False,
            "submit_authority": False,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        }


def _refresh_commands(*, repo_root: Path, preflight_mode: str) -> list[tuple[str, list[str]]]:
    python_bin = str(repo_root / ".venv" / "bin" / "python")
    return [
        (
            "phase1_runtime_data_readiness",
            [
                python_bin,
                "-m",
                "mgc_v05l.execution_core.phase1_runtime_data_readiness",
                "--repo-root",
                str(repo_root),
            ],
        ),
        (
            "phase1_ticker_readiness_matrix",
            [
                python_bin,
                "-m",
                "mgc_v05l.app.phase1_ticker_readiness_matrix",
                "--repo-root",
                str(repo_root),
            ],
        ),
        (
            "track_b_paper_preflight",
            [
                "/bin/bash",
                str(repo_root / "scripts" / "track_b_paper_preflight.sh"),
                "--mode",
                preflight_mode,
            ],
        ),
    ]


def _run_command(command: Sequence[str], repo_root: Path, timeout_seconds: float) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{repo_root / 'src'}{':' + existing_pythonpath if existing_pythonpath else ''}"
    return subprocess.run(
        list(command),
        cwd=repo_root,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout_seconds,
        check=False,
    )


def _status_payload(
    *,
    config: RefreshConfig,
    started_at: datetime,
    command_results: Sequence[RefreshCommandResult],
    succeeded: bool,
) -> dict[str, Any]:
    now = _utc_now()
    return {
        "schema_version": "track_b_operator_readiness_refresher_status_v1",
        "generated_at": now.isoformat(),
        "last_refresh_started_at": started_at.isoformat(),
        "last_refresh_finished_at": now.isoformat(),
        "last_success": succeeded,
        "last_success_at": now.isoformat() if succeeded else None,
        "classification": (
            "TRACK_B_OPERATOR_READINESS_REFRESH_READY"
            if succeeded
            else "TRACK_B_OPERATOR_READINESS_REFRESH_FAILED"
        ),
        "repo_root": str(config.repo_root),
        "refresh_seconds": config.refresh_seconds,
        "preflight_mode": config.preflight_mode,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "refreshed_artifacts": {
            "phase1_runtime_data_readiness": str(
                config.repo_root
                / "outputs"
                / "reports"
                / "phase1_runtime_data_readiness"
                / "latest_phase1_runtime_data_readiness.json"
            ),
            "phase1_ticker_readiness_matrix": str(
                config.repo_root
                / "outputs"
                / "reports"
                / "phase1_ticker_readiness_matrix"
                / "latest_phase1_ticker_readiness_matrix.json"
            ),
            "track_b_paper_preflight": str(
                config.repo_root
                / "outputs"
                / "reports"
                / "track_b_paper_preflight"
                / "latest_track_b_paper_preflight.json"
            ),
        },
        "commands": [
            {
                "name": result.name,
                "command": result.command,
                "returncode": result.returncode,
                "duration_seconds": result.duration_seconds,
                "stdout_tail": result.stdout_tail,
                "stderr_tail": result.stderr_tail,
            }
            for result in command_results
        ],
    }


def _tail(value: str, *, max_chars: int = 2000) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh read-only Track B operator readiness artifacts.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--status-path", default=str(DEFAULT_STATUS_PATH))
    parser.add_argument("--refresh-seconds", type=float, default=float(os.environ.get("TRACK_B_OPERATOR_READINESS_REFRESH_SECONDS", DEFAULT_REFRESH_SECONDS)))
    parser.add_argument("--preflight-mode", choices=("monday-live", "weekend-static"), default=DEFAULT_PREFLIGHT_MODE)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Run one refresh pass and exit.")
    mode.add_argument("--service", action="store_true", help="Run refreshes until stopped.")
    mode.add_argument("--status", action="store_true", help="Print latest refresher status.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = RefreshConfig(
        repo_root=Path(args.repo_root),
        status_path=Path(args.status_path),
        refresh_seconds=args.refresh_seconds,
        preflight_mode=args.preflight_mode,
        timeout_seconds=args.timeout_seconds,
    )
    if args.status:
        print(json.dumps(read_status(status_path=config.status_path), indent=2, sort_keys=True))
        return 0
    if args.service:
        return run_service(config=config)
    payload = refresh_once(config=config)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload.get("last_success") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
