"""Bounded current-scope refresh after Track B PAPER broker mutations."""

from __future__ import annotations

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

from .phase1_runtime_ticker_registry import PHASE1_RUNTIME_TICKER_ORDER
from .track_b_atomic_io import write_json_atomic


POST_BROKER_MUTATION_REFRESH_SUCCEEDED = "POST_BROKER_MUTATION_REFRESH_SUCCEEDED"
POST_BROKER_MUTATION_REFRESH_DEGRADED = "POST_BROKER_MUTATION_REFRESH_DEGRADED"
POST_BROKER_MUTATION_REFRESH_SKIPPED = "POST_BROKER_MUTATION_REFRESH_SKIPPED"

DEFAULT_POST_BROKER_MUTATION_REFRESH_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "post_broker_mutation_refresh"
    / "latest_post_broker_mutation_refresh.json"
)

PAPER_ACCOUNT = "DUM882026"
DEFAULT_SYMBOLS = PHASE1_RUNTIME_TICKER_ORDER

CommandRunner = Callable[[Sequence[str], Path, float], subprocess.CompletedProcess[str]]


@dataclass(frozen=True)
class PostBrokerMutationRefreshConfig:
    repo_root: Path
    account_id: str = PAPER_ACCOUNT
    mode: str = "PAPER"
    host: str = "127.0.0.1"
    port: int = 7497
    read_only: bool = True
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS
    timeout_seconds: float = 30.0
    output_path: Path = DEFAULT_POST_BROKER_MUTATION_REFRESH_ARTIFACT
    python_executable: str = sys.executable

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def post_position_order_change_refresh(
    *,
    config: PostBrokerMutationRefreshConfig,
    trigger: str,
    mutation_report: Mapping[str, Any] | None = None,
    now: datetime | None = None,
    command_runner: CommandRunner | None = None,
    write: bool = True,
) -> dict[str, Any]:
    """Run a best-effort publication convergence cycle after PAPER mutation.

    The hook is intentionally not a broker-mutation authority gate. It runs after
    submit/modify/cancel/fill evidence has already been handled by the caller,
    records failures as diagnostics, and keeps ODS publication last.
    """

    actual_now = _ensure_utc(now or datetime.now(UTC))
    runner = command_runner or _run_command
    safety_blockers = _safety_blockers(config)
    if safety_blockers:
        payload = _base_payload(
            config=config,
            trigger=trigger,
            now=actual_now,
            mutation_report=mutation_report,
            classification=POST_BROKER_MUTATION_REFRESH_SKIPPED,
            steps=[],
            safety_blockers=safety_blockers,
        )
        if write:
            write_json_atomic(config.resolve(config.output_path), payload)
        return payload

    steps: list[dict[str, Any]] = []
    for step in _refresh_steps(config):
        started = time.monotonic()
        completed = _run_safely(runner, step["command"], config.repo_root, float(config.timeout_seconds))
        steps.append(
            {
                "name": step["name"],
                "artifact_group": step["artifact_group"],
                "diagnostic_only": bool(step.get("diagnostic_only")),
                "command": list(step["command"]),
                "returncode": int(completed.returncode),
                "succeeded": completed.returncode == 0,
                "duration_seconds": round(time.monotonic() - started, 3),
                "stdout_tail": _tail(completed.stdout),
                "stderr_tail": _tail(completed.stderr),
            }
        )

    required_failures = [row for row in steps if row.get("succeeded") is not True and row.get("diagnostic_only") is not True]
    payload = _base_payload(
        config=config,
        trigger=trigger,
        now=actual_now,
        mutation_report=mutation_report,
        classification=POST_BROKER_MUTATION_REFRESH_SUCCEEDED
        if not required_failures
        else POST_BROKER_MUTATION_REFRESH_DEGRADED,
        steps=steps,
        safety_blockers=[],
    )
    if write:
        write_json_atomic(config.resolve(config.output_path), payload)
    return payload


def _refresh_steps(config: PostBrokerMutationRefreshConfig) -> list[dict[str, Any]]:
    python = config.python_executable
    repo = str(config.repo_root)
    account = str(config.account_id)
    canonical_symbols = _normal_symbols(PHASE1_RUNTIME_TICKER_ORDER)
    symbols = ",".join(canonical_symbols)
    broker_timeout = str(max(1.0, min(float(config.timeout_seconds), 20.0)))
    return [
        {
            "name": "broker_truth_lease_and_bsa",
            "artifact_group": "broker_truth_broker_lease_bsa",
            "command": [
                python,
                "-m",
                "mgc_v05l.app.ibkr_broker_truth_refresher",
                "--once",
                "--mode",
                "PAPER",
                "--host",
                str(config.host),
                "--port",
                str(config.port),
                "--account-id",
                account,
                "--read-only",
                "--timeout-seconds",
                broker_timeout,
            ],
        },
        {
            "name": "broker_reconciliation",
            "artifact_group": "reconciliation",
            "command": [
                python,
                "-m",
                "mgc_v05l.execution_core.track_b_paper_broker_reconciliation",
                "--repo-root",
                repo,
                "--account",
                account,
                "--symbols",
                symbols,
            ],
        },
        {
            "name": "shared_truth",
            "artifact_group": "open_order_truth_managed_positions_managed_orders",
            "command": [
                python,
                "-m",
                "mgc_v05l.execution_core.track_b_shared_truth_refresh_cli",
                "--repo-root",
                repo,
                "--account",
                account,
                "--symbols",
                symbols,
                "--json",
            ],
        },
        {
            "name": "canonical_readiness",
            "artifact_group": "canonical_readiness",
            "command": [
                python,
                "-m",
                "mgc_v05l.app.track_b_canonical_readiness",
                "--repo-root",
                repo,
                "--json",
            ],
        },
        {
            "name": "current_scope_state",
            "artifact_group": "current_scope_state",
            "command": [
                python,
                "-m",
                "mgc_v05l.execution_core.track_b_current_scope_state",
                "--repo-root",
                repo,
                "--json",
            ],
        },
        {
            "name": "control_plane_snapshot_diagnostic",
            "artifact_group": "control_plane_snapshot_diagnostic",
            "diagnostic_only": True,
            "command": [
                python,
                "-m",
                "mgc_v05l.execution_core.track_b_control_plane_snapshot",
                "--repo-root",
                repo,
                "--no-dashboard-projection",
                "--no-broker-lease-history",
                "--json",
            ],
        },
        {
            "name": "operator_decision_surface",
            "artifact_group": "ods",
            "command": [
                python,
                "-m",
                "mgc_v05l.execution_core.track_b_operator_decision_surface",
                "--repo-root",
                repo,
                "--json",
            ],
        },
    ]


def _normal_symbols(symbols: Sequence[str]) -> tuple[str, ...]:
    return tuple(text for symbol in symbols if (text := str(symbol).strip().upper()))


def _base_payload(
    *,
    config: PostBrokerMutationRefreshConfig,
    trigger: str,
    now: datetime,
    mutation_report: Mapping[str, Any] | None,
    classification: str,
    steps: Sequence[Mapping[str, Any]],
    safety_blockers: Sequence[str],
) -> dict[str, Any]:
    first_failure = next((dict(row) for row in steps if row.get("succeeded") is not True), None)
    return {
        "schema_version": "track_b_post_broker_mutation_refresh_v1",
        "generated_at": now.isoformat(),
        "classification": classification,
        "trigger": trigger,
        "mode": config.mode,
        "paper_only": True,
        "account_id": config.account_id,
        "host": config.host,
        "port": int(config.port),
        "read_only": bool(config.read_only),
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "global_cancel_allowed": False,
        "broad_flatten_allowed": False,
        "safety_blockers": list(safety_blockers),
        "requested_symbols": list(_normal_symbols(config.symbols)),
        "canonical_symbols": list(_normal_symbols(PHASE1_RUNTIME_TICKER_ORDER)),
        "canonical_refresh_scope": "GLOBAL_COMPLETE",
        "scoped_symbols_diagnostic_only": tuple(_normal_symbols(config.symbols))
        != tuple(_normal_symbols(PHASE1_RUNTIME_TICKER_ORDER)),
        "step_count": len(steps),
        "required_failure_count": sum(
            1 for row in steps if row.get("succeeded") is not True and row.get("diagnostic_only") is not True
        ),
        "diagnostic_failure_count": sum(
            1 for row in steps if row.get("succeeded") is not True and row.get("diagnostic_only") is True
        ),
        "first_failing_step": first_failure,
        "steps": [dict(row) for row in steps],
        "refresh_order": [
            "broker_truth_positions_orders_executions",
            "broker_truth_lease",
            "broker_session_authority",
            "reconciliation",
            "open_order_truth",
            "managed_positions_orders",
            "canonical_readiness",
            "current_scope_state",
            "control_plane_snapshot_diagnostic",
            "operator_decision_surface",
        ],
        "mutation_report_summary": _mutation_summary(mutation_report or {}),
        "output_path": str(config.resolve(config.output_path)),
    }


def _mutation_summary(report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "classification": report.get("classification"),
        "broker_state_mutated": report.get("broker_state_mutated"),
        "broker_mutation_performed": report.get("broker_mutation_performed"),
        "submit_attempted": report.get("submit_attempted"),
        "submitted_count": report.get("submitted_count"),
        "order_id": report.get("order_id") or report.get("broker_order_id"),
        "perm_id": report.get("perm_id"),
    }


def _safety_blockers(config: PostBrokerMutationRefreshConfig) -> list[str]:
    blockers: list[str] = []
    if str(config.mode).upper() != "PAPER":
        blockers.append("non_paper_mode")
    if str(config.account_id) != PAPER_ACCOUNT:
        blockers.append("wrong_paper_account")
    if int(config.port) != 7497 or str(config.host) != "127.0.0.1":
        blockers.append("non_paper_endpoint")
    if config.read_only is not True:
        blockers.append("refresh_must_be_read_only")
    return blockers


def _run_safely(
    runner: CommandRunner,
    command: Sequence[str],
    repo_root: Path,
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    try:
        return runner(command, repo_root, timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else str(exc.stdout or "")
        stderr = exc.stderr if isinstance(exc.stderr, str) else str(exc.stderr or "")
        return subprocess.CompletedProcess(
            list(command),
            124,
            stdout=stdout,
            stderr=f"refresh command timed out after {timeout_seconds}s: {stderr}".strip(),
        )
    except Exception as exc:  # defensive: mutation callers should retain their successful broker result.
        return subprocess.CompletedProcess(list(command), 1, stdout="", stderr=f"refresh command exception: {exc}")


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
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else str(exc.stdout or "")
        stderr = exc.stderr if isinstance(exc.stderr, str) else str(exc.stderr or "")
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except OSError:
            try:
                process.kill()
            except OSError:
                pass
        try:
            cleanup_stdout, cleanup_stderr = process.communicate(timeout=2)
            stdout = cleanup_stdout if cleanup_stdout is not None else stdout
            stderr = cleanup_stderr if cleanup_stderr is not None else stderr
        except subprocess.TimeoutExpired:
            pass
        return subprocess.CompletedProcess(
            list(command),
            124,
            stdout=stdout or "",
            stderr=f"refresh command timed out after {timeout_seconds}s: {stderr or ''}".strip(),
        )
    return subprocess.CompletedProcess(list(command), process.returncode, stdout=stdout or "", stderr=stderr or "")


def _tail(value: str | bytes | None, *, limit: int = 1600) -> str:
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="replace")
    else:
        text = str(value or "")
    return text[-limit:]


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
