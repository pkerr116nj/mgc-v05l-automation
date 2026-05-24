"""Read-only Track B PAPER process-surface hygiene report."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic


PROCESS_SURFACE_CLEAR = "PROCESS_SURFACE_CLEAR"
PROCESS_SURFACE_ATTENTION = "PROCESS_SURFACE_ATTENTION"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PROCESS_HYGIENE_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "process_hygiene"
    / "latest_track_b_process_surface_hygiene.json"
)


@dataclass(frozen=True)
class TrackBProcessSurfaceHygieneConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_PROCESS_HYGIENE_ARTIFACT

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_process_surface_hygiene(
    *,
    config: TrackBProcessSurfaceHygieneConfig,
    now: datetime | None = None,
    process_rows: Sequence[Mapping[str, Any]] | None = None,
    pid_running: Callable[[int], bool] | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    rows = [
        row
        for row in [_normalize_process(row) for row in (process_rows if process_rows is not None else _ps_rows())]
        if not _is_audit_command(row)
    ]
    runtime_writers = [_process(row, "track_b_paper_runtime_writer", "proof_blocking") for row in rows if _is_runtime_writer(row)]
    paper_monitors = [_process(row, "paper_strategy_monitor", "diagnostic") for row in rows if _contains(row, "ibkr_paper_strategy_monitor")]
    paper_preflights = [_process(row, "paper_preflight_script", "diagnostic") for row in rows if _contains(row, "track_b_paper_preflight.sh")]
    pytest_processes = [_process(row, "pytest", "diagnostic") for row in rows if _contains(row, "pytest")]
    phase1 = [_process(row, "phase1_databento_supervisor_or_listener", "expected_support") for row in rows if _contains(row, "phase1_databento_live_runtime_candles")]
    broker_truth = [_process(row, "broker_truth_refresher", "expected_support") for row in rows if _contains(row, "ibkr_broker_truth_refresher")]
    dashboard = [
        _process(row, "operator_dashboard_or_readiness_refresher", "expected_support")
        for row in rows
        if _contains(row, "operator_dashboard")
        or _contains(row, "track_b_operator_readiness_refresher")
        or _contains(row, "publish_dashboard_readiness_contract")
    ]
    stale_pid_files = _stale_pid_files(config=config, pid_running=pid_running or _pid_running)
    proof_blocking = list(runtime_writers)
    classification = PROCESS_SURFACE_ATTENTION if proof_blocking else PROCESS_SURFACE_CLEAR
    return {
        "schema_version": "track_b_process_surface_hygiene_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "runtime_restart_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "classification": classification,
        "proof_blocking_processes": proof_blocking,
        "diagnostic_processes": paper_monitors + paper_preflights + pytest_processes,
        "expected_support_processes": phase1 + broker_truth + dashboard,
        "stale_pid_files": stale_pid_files,
        "summary": {
            "proof_blocking_process_count": len(proof_blocking),
            "diagnostic_process_count": len(paper_monitors + paper_preflights + pytest_processes),
            "expected_support_process_count": len(phase1 + broker_truth + dashboard),
            "stale_pid_file_count": len(stale_pid_files),
            "active_runtime_writer_count": len(runtime_writers),
            "paper_strategy_monitor_count": len(paper_monitors),
            "paper_preflight_script_count": len(paper_preflights),
            "pytest_process_count": len(pytest_processes),
            "phase1_databento_process_count": len(phase1),
            "broker_truth_refresher_count": len(broker_truth),
            "operator_dashboard_readiness_process_count": len(dashboard),
        },
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
        },
    }


def write_track_b_process_surface_hygiene(
    *,
    config: TrackBProcessSurfaceHygieneConfig,
    payload: Mapping[str, Any],
) -> Path:
    return write_json_atomic(config.resolve(config.output_path), payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write read-only Track B PAPER process-surface hygiene report.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--output-path", default=str(DEFAULT_PROCESS_HYGIENE_ARTIFACT))
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBProcessSurfaceHygieneConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
    )
    payload = build_track_b_process_surface_hygiene(config=config)
    write_track_b_process_surface_hygiene(config=config, payload=payload)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            json.dumps(
                {
                    "classification": payload["classification"],
                    **payload["summary"],
                    "authority_path": payload["artifact_paths"]["authority"],
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0 if not payload["proof_blocking_processes"] else 2


def _ps_rows() -> list[dict[str, Any]]:
    try:
        output = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
    except (OSError, subprocess.SubprocessError):
        return []
    rows: list[dict[str, Any]] = []
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        pid_text, _, command = stripped.partition(" ")
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        rows.append({"pid": pid, "command": command.strip()})
    return rows


def _stale_pid_files(
    *,
    config: TrackBProcessSurfaceHygieneConfig,
    pid_running: Callable[[int], bool],
) -> list[dict[str, Any]]:
    paths = [
        ("track_b_paper_runtime", Path("outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid")),
        ("track_b_paper_runtime_wrapper", Path("outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.wrapper.pid")),
        ("phase1_databento_service", Path("var/phase1_databento_live_candles_service.pid")),
        ("phase1_databento_child", Path("var/phase1_databento_live_candles_child.pid")),
        ("broker_truth_refresher", Path("var/track_b_broker_truth_refresh_service.pid")),
        ("operator_dashboard", Path("outputs/operator_dashboard/runtime/operator_dashboard.pid")),
        ("operator_dashboard_manager", Path("outputs/operator_dashboard/runtime/operator_dashboard_manager.pid")),
        ("operator_readiness_refresher_service", Path("var/track_b_operator_readiness_refresh_service.pid")),
        ("operator_readiness_refresher_child", Path("var/track_b_operator_readiness_refresh_child.pid")),
    ]
    stale: list[dict[str, Any]] = []
    for label, relative_path in paths:
        path = config.resolve(relative_path)
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="replace").strip()
        if not text.isdigit():
            stale.append({"label": label, "path": str(path), "pid": text, "reason": "pid_file_not_numeric"})
            continue
        pid = int(text)
        if not pid_running(pid):
            stale.append({"label": label, "path": str(path), "pid": pid, "reason": "pid_not_running"})
    return stale


def _normalize_process(row: Mapping[str, Any]) -> dict[str, Any]:
    return {"pid": int(row.get("pid") or 0), "command": str(row.get("command") or "")}


def _process(row: Mapping[str, Any], kind: str, classification: str) -> dict[str, Any]:
    return {
        "pid": int(row.get("pid") or 0),
        "kind": kind,
        "classification": classification,
        "command": str(row.get("command") or ""),
    }


def _contains(row: Mapping[str, Any], needle: str) -> bool:
    return needle in str(row.get("command") or "")


def _is_runtime_writer(row: Mapping[str, Any]) -> bool:
    command = str(row.get("command") or "")
    return (
        "probationary-paper-soak" in command
        and "mgc_v05l.app.main" in command
    ) or (_is_shell_invocation(command) and "run_probationary_paper_soak.sh" in command)


def _is_shell_invocation(command: str) -> bool:
    head = command.strip().split(" ", 1)[0]
    return head.endswith("bash") or head.endswith("zsh")


def _is_audit_command(row: Mapping[str, Any]) -> bool:
    command = str(row.get("command") or "")
    return (
        "git diff --check" in command
        or command.startswith("rg ")
        or " rg " in command
        or "track_b_process_surface_hygiene" in command
    )


def _pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
