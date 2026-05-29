"""Read-only Track B service ownership migration scaffolding.

This module defines the Phase 1 launchd/app ownership contract for persistent
Track B PAPER support services.  It intentionally does not install, start,
stop, unload, or mutate any live service.
"""

from __future__ import annotations

import argparse
import json
import os
import plistlib
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "service_ownership"
    / "latest_track_b_service_ownership.json"
)
DEFAULT_PLIST_OUTPUT_DIR = Path("var") / "launchd" / "track_b"

CANONICAL_PAPER_CONFIG_STACK = (
    "config/base.yaml",
    "config/live.yaml",
    "config/probationary_pattern_engine.yaml",
    "config/headless_supervised_paper_runtime.yaml",
    "config/probationary_pattern_engine_paper.yaml",
)

REVIEW_OVERLAY_CONFIG = "config/probationary_pattern_engine_paper_mnq_mgc_plus_mnq_us_intraday_review.yaml"

PAPER_RUNTIME_TRUTH_PATH = Path(
    "outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_truth.json"
)
PAPER_RUNTIME_PID_METADATA_PATH = Path(
    "outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid.json"
)
PAPER_RUNTIME_PID_PATH = Path(
    "outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid"
)


@dataclass(frozen=True)
class TrackBServiceSpec:
    service_id: str
    label: str
    owner: str
    command: tuple[str, ...]
    working_directory: Path
    stdout_log: Path
    stderr_log: Path
    pid_path: Path
    status_command: tuple[str, ...]
    install_command: tuple[str, ...]
    start_command: tuple[str, ...]
    stop_command: tuple[str, ...]
    restart_policy: str
    broker_mutating: bool = False
    app_owned: bool = False
    disabled_by_default: bool = False
    start_interval_seconds: int | None = None
    launchd_keep_alive: bool = False
    environment: Mapping[str, str] = field(default_factory=dict)
    notes: tuple[str, ...] = ()

    def to_dict(self, repo_root: Path) -> dict[str, Any]:
        return {
            "service_id": self.service_id,
            "label": self.label,
            "owner": self.owner,
            "broker_mutating": self.broker_mutating,
            "app_owned": self.app_owned,
            "disabled_by_default": self.disabled_by_default,
            "start_interval_seconds": self.start_interval_seconds,
            "working_directory": str(_resolve(repo_root, self.working_directory)),
            "command": list(self.command),
            "environment": dict(self.environment),
            "stdout_log": str(_resolve(repo_root, self.stdout_log)),
            "stderr_log": str(_resolve(repo_root, self.stderr_log)),
            "pid_path": str(_resolve(repo_root, self.pid_path)),
            "status_command": list(self.status_command),
            "install_command": list(self.install_command),
            "start_command": list(self.start_command),
            "stop_command": list(self.stop_command),
            "restart_policy": self.restart_policy,
            "launchd_keep_alive": self.launchd_keep_alive,
            "notes": list(self.notes),
            "plist": build_launchd_plist(self, repo_root=repo_root),
        }


@dataclass(frozen=True)
class TrackBServiceOwnershipConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_OUTPUT_PATH
    plist_output_dir: Path = DEFAULT_PLIST_OUTPUT_DIR

    def resolve(self, path: Path) -> Path:
        return _resolve(self.repo_root, path)


def service_specs(repo_root: Path = REPO_ROOT) -> tuple[TrackBServiceSpec, ...]:
    canonical_stack = ":".join(str(repo_root / item) for item in CANONICAL_PAPER_CONFIG_STACK)
    return (
        TrackBServiceSpec(
            service_id="track_b_hourly_paper_runtime_recovery",
            label="com.mgc.trackb.paper-runtime-recovery",
            owner="launchd_user_agent",
            command=("/bin/bash", "scripts/track_b_hourly_paper_runtime_recovery.sh", "tick"),
            environment={
                "PYTHONPATH": "src",
                "MGC_TRACK_B_SERVICE_OWNER": "launchd",
                "MGC_TRACK_B_SERVICE_ID": "track_b_hourly_paper_runtime_recovery",
            },
            working_directory=Path("."),
            stdout_log=Path("logs/launchd/track_b_paper_runtime_recovery.out.log"),
            stderr_log=Path("logs/launchd/track_b_paper_runtime_recovery.err.log"),
            pid_path=Path("outputs/track_b_execution_core/runtime_recovery/latest_hourly_runtime_recovery_audit.json"),
            status_command=("bash", "scripts/track_b_hourly_paper_runtime_recovery.sh", "status"),
            install_command=("bash", "scripts/generate_track_b_launchd_plists.sh", "--write"),
            start_command=("bash", "scripts/track_b_hourly_paper_runtime_recovery.sh", "enable"),
            stop_command=("bash", "scripts/track_b_hourly_paper_runtime_recovery.sh", "disable"),
            restart_policy="disabled_by_default_hourly_launchd; uses canonical paper-stack status/start only",
            disabled_by_default=True,
            start_interval_seconds=3600,
            launchd_keep_alive=False,
            notes=(
                "Standalone recovery owner; survives Codex exit only after explicit launchctl enable/bootstrap.",
                "Disabled by default and must report PAUSED until explicitly enabled.",
                "Uses scripts/track_b_status_paper_stack.sh before any start attempt.",
                "Uses scripts/track_b_start_paper_stack.sh as the only runtime start path.",
                "Must not start when a runtime PID is already running or canonical blockers are present.",
            ),
        ),
        TrackBServiceSpec(
            service_id="track_b_paper_runtime",
            label="com.mgc.trackb.paper-runtime",
            owner="launchd_user_agent",
            broker_mutating=True,
            command=(
                "/bin/bash",
                "scripts/run_headless_supervised_paper_service.sh",
                "--no-start-dashboard",
                "--wait-timeout-seconds",
                "120",
                "--post-start-pid-wait-timeout-seconds",
                "45",
            ),
            environment={
                "PYTHONPATH": "src",
                "MGC_HEADLESS_PAPER_LAUNCH_METHOD": "direct",
                "MGC_HEADLESS_SUPERVISED_PAPER_CONFIG_PATHS": canonical_stack,
                "MGC_HEADLESS_REQUIRED_PAPER_CONFIGS": canonical_stack,
                "MGC_TRACK_B_SERVICE_OWNER": "launchd",
                "MGC_TRACK_B_SERVICE_ID": "track_b_paper_runtime",
            },
            working_directory=Path("."),
            stdout_log=Path("logs/launchd/track_b_paper_runtime.out.log"),
            stderr_log=Path("logs/launchd/track_b_paper_runtime.err.log"),
            pid_path=PAPER_RUNTIME_PID_PATH,
            status_command=("bash", "scripts/track_b_runtime_operability_status.sh"),
            install_command=("bash", "scripts/generate_track_b_launchd_plists.sh", "--write"),
            start_command=("launchctl", "load", "~/Library/LaunchAgents/com.mgc.trackb.paper-runtime.plist"),
            stop_command=("launchctl", "unload", "~/Library/LaunchAgents/com.mgc.trackb.paper-runtime.plist"),
            restart_policy="launchd_run_at_load_with_single_writer_guard; cutover must stop manual runtime first",
            launchd_keep_alive=False,
            notes=(
                "Exactly one broker-mutating Track B PAPER runtime may exist.",
                "Canonical config stack excludes review-overlay and stress-test configs.",
                "Phase 1 is template-only; do not load while manual PID 72320 is active.",
            ),
        ),
        TrackBServiceSpec(
            service_id="phase1_databento_candle_supervisor",
            label="com.mgc.trackb.phase1-databento-candles",
            owner="launchd_user_agent",
            command=("/bin/bash", "scripts/start-phase1-databento-live-candles"),
            environment={
                "PYTHONPATH": "src",
                "MGC_TRACK_B_SERVICE_OWNER": "launchd",
                "MGC_TRACK_B_SERVICE_ID": "phase1_databento_candle_supervisor",
            },
            working_directory=Path("."),
            stdout_log=Path("logs/launchd/phase1_databento_candles.out.log"),
            stderr_log=Path("logs/launchd/phase1_databento_candles.err.log"),
            pid_path=Path("var/phase1_databento_live_candles_service.pid"),
            status_command=("bash", "scripts/status-phase1-databento-live-candles"),
            install_command=("bash", "scripts/generate_track_b_launchd_plists.sh", "--write"),
            start_command=("launchctl", "load", "~/Library/LaunchAgents/com.mgc.trackb.phase1-databento-candles.plist"),
            stop_command=("launchctl", "unload", "~/Library/LaunchAgents/com.mgc.trackb.phase1-databento-candles.plist"),
            restart_policy="launchd_run_at_load; read-only candle service",
            notes=("No broker mutation authority.",),
        ),
        TrackBServiceSpec(
            service_id="ibkr_paper_broker_truth_refresher",
            label="com.mgc.trackb.ibkr-paper-broker-truth",
            owner="launchd_user_agent",
            command=("/bin/bash", "scripts/start-track-b-broker-truth-refresh"),
            environment={
                "PYTHONPATH": "src",
                "MGC_TRACK_B_SERVICE_OWNER": "launchd",
                "MGC_TRACK_B_SERVICE_ID": "ibkr_paper_broker_truth_refresher",
            },
            working_directory=Path("."),
            stdout_log=Path("logs/launchd/ibkr_paper_broker_truth.out.log"),
            stderr_log=Path("logs/launchd/ibkr_paper_broker_truth.err.log"),
            pid_path=Path("var/track_b_broker_truth_refresh_service.pid"),
            status_command=("bash", "scripts/status-track-b-broker-truth-refresh"),
            install_command=("bash", "scripts/generate_track_b_launchd_plists.sh", "--write"),
            start_command=("launchctl", "load", "~/Library/LaunchAgents/com.mgc.trackb.ibkr-paper-broker-truth.plist"),
            stop_command=("launchctl", "unload", "~/Library/LaunchAgents/com.mgc.trackb.ibkr-paper-broker-truth.plist"),
            restart_policy="launchd_run_at_load; read-only broker truth refresh",
            notes=("Reads PAPER broker truth; does not submit/cancel/modify/flatten.",),
        ),
        TrackBServiceSpec(
            service_id="operator_dashboard_backend",
            label="com.mgc.trackb.operator-dashboard",
            owner="app_or_launchd_user_agent",
            app_owned=True,
            command=("/bin/bash", "scripts/run_operator_dashboard.sh", "--no-open-browser"),
            environment={
                "PYTHONPATH": "src",
                "MGC_TRACK_B_SERVICE_OWNER": "launchd_or_app",
                "MGC_TRACK_B_SERVICE_ID": "operator_dashboard_backend",
            },
            working_directory=Path("."),
            stdout_log=Path("logs/launchd/operator_dashboard.out.log"),
            stderr_log=Path("logs/launchd/operator_dashboard.err.log"),
            pid_path=Path("outputs/operator_dashboard/runtime/operator_dashboard.pid"),
            status_command=("bash", "scripts/diagnose_operator_dashboard.sh"),
            install_command=("bash", "scripts/generate_track_b_launchd_plists.sh", "--write"),
            start_command=("launchctl", "load", "~/Library/LaunchAgents/com.mgc.trackb.operator-dashboard.plist"),
            stop_command=("launchctl", "unload", "~/Library/LaunchAgents/com.mgc.trackb.operator-dashboard.plist"),
            restart_policy="app_owned_preferred; launchd acceptable for backend publisher",
            notes=("Dashboard/backend degradation must not imply runtime down unless canonical truth says so.",),
        ),
    )


def build_service_ownership_artifact(
    *,
    config: TrackBServiceOwnershipConfig,
    now: datetime | None = None,
    process_rows: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    repo_root = config.repo_root.expanduser().resolve()
    actual_now = _ensure_utc(now or datetime.now(UTC))
    specs = service_specs(repo_root)
    rows = _process_rows_with_runtime_artifacts(
        repo_root=repo_root,
        process_rows=process_rows,
    )
    active_runtime_processes = _active_runtime_processes(rows)
    broker_mutating_specs = [spec for spec in specs if spec.broker_mutating]
    duplicate_writer_status = validate_single_broker_mutating_runtime(
        specs=specs,
        process_rows=rows,
    )
    return {
        "schema_version": "track_b_service_ownership_v1",
        "generated_at": actual_now.isoformat(),
        "repo_root": str(repo_root),
        "phase": "PHASE_1_TEMPLATE_ONLY_NO_CUTOVER",
        "read_only": True,
        "broker_mutation_allowed": False,
        "submit_authority": False,
        "lifecycle_authority": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "install_performed": False,
        "launchd_load_performed": False,
        "runtime_cutover_performed": False,
        "canonical_paper_config_stack": [str(repo_root / item) for item in CANONICAL_PAPER_CONFIG_STACK],
        "excluded_review_overlay_config": str(repo_root / REVIEW_OVERLAY_CONFIG),
        "services": [spec.to_dict(repo_root) for spec in specs],
        "summary": {
            "service_count": len(specs),
            "broker_mutating_service_count": len(broker_mutating_specs),
            "active_runtime_process_count": len(active_runtime_processes),
            "duplicate_writer_guard": duplicate_writer_status["classification"],
        },
        "duplicate_writer_guard": duplicate_writer_status,
        "cutover_checklist": cutover_checklist(repo_root),
        "artifact_paths": {
            "ownership": str(config.resolve(config.output_path)),
            "plist_output_dir": str(config.resolve(config.plist_output_dir)),
            "runtime_truth": str(repo_root / PAPER_RUNTIME_TRUTH_PATH),
            "runtime_pid_metadata": str(repo_root / PAPER_RUNTIME_PID_METADATA_PATH),
        },
    }


def validate_single_broker_mutating_runtime(
    *,
    specs: Sequence[TrackBServiceSpec],
    process_rows: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    rows = [_normalize_process(row) for row in (process_rows if process_rows is not None else _ps_rows())]
    active_runtime_processes = _active_runtime_processes(rows)
    broker_mutating_specs = [spec for spec in specs if spec.broker_mutating]
    loaded_runtime_launchd_specs = [
        row for row in rows if "com.mgc.trackb.paper-runtime" in str(row.get("command") or "")
    ]
    active_count = len(active_runtime_processes) + len(loaded_runtime_launchd_specs)
    classification = "SINGLE_WRITER_READY"
    if len(broker_mutating_specs) != 1:
        classification = "BROKER_MUTATING_SERVICE_SPEC_INVALID"
    elif active_count > 1:
        classification = "DUPLICATE_BROKER_MUTATING_RUNTIME_BLOCKED"
    elif active_count == 1:
        classification = "MANUAL_RUNTIME_ACTIVE_CUTOVER_REQUIRED"
    return {
        "classification": classification,
        "max_broker_mutating_runtime_count": 1,
        "broker_mutating_service_ids": [spec.service_id for spec in broker_mutating_specs],
        "active_runtime_processes": active_runtime_processes,
        "loaded_runtime_launchd_specs": loaded_runtime_launchd_specs,
        "active_broker_mutating_runtime_count": active_count,
        "cutover_allowed_without_stop": active_count == 0 and len(broker_mutating_specs) == 1,
    }


def build_launchd_plist(spec: TrackBServiceSpec, *, repo_root: Path) -> dict[str, Any]:
    env = {key: _expand_repo_root(value, repo_root) for key, value in dict(spec.environment).items()}
    env.setdefault("PYTHONUNBUFFERED", "1")
    payload = {
        "Label": spec.label,
        "ProgramArguments": [
            _expand_repo_root(part, repo_root) if index == 0 and part.startswith("scripts/") else part
            for index, part in enumerate(spec.command)
        ],
        "WorkingDirectory": str(repo_root),
        "EnvironmentVariables": env,
        "StandardOutPath": str(_resolve(repo_root, spec.stdout_log)),
        "StandardErrorPath": str(_resolve(repo_root, spec.stderr_log)),
        "RunAtLoad": not spec.disabled_by_default,
        "KeepAlive": bool(spec.launchd_keep_alive),
    }
    if spec.disabled_by_default:
        payload["Disabled"] = True
    if spec.start_interval_seconds is not None:
        payload["StartInterval"] = int(spec.start_interval_seconds)
    return payload


def write_service_ownership_artifact(
    *,
    config: TrackBServiceOwnershipConfig,
    payload: Mapping[str, Any],
) -> Path:
    return write_json_atomic(config.resolve(config.output_path), payload)


def write_launchd_plist_templates(
    *,
    config: TrackBServiceOwnershipConfig,
    specs: Sequence[TrackBServiceSpec] | None = None,
) -> list[Path]:
    repo_root = config.repo_root.expanduser().resolve()
    output_dir = config.resolve(config.plist_output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for spec in specs or service_specs(repo_root):
        path = output_dir / f"{spec.label}.plist"
        with path.open("wb") as handle:
            plistlib.dump(build_launchd_plist(spec, repo_root=repo_root), handle, sort_keys=True)
        written.append(path)
    return written


def cutover_checklist(repo_root: Path) -> list[str]:
    return [
        "Confirm current manual Track B PAPER runtime is flat/clean or intentionally safe to stop.",
        "Confirm broker open orders = 0, managed positions/orders clean, Guardian/Control Plane/Safe-State clean.",
        "Run service ownership dry-run and verify duplicate_writer_guard is MANUAL_RUNTIME_ACTIVE_CUTOVER_REQUIRED.",
        "Stop the manual runtime through the approved guarded stop path.",
        "Install launchd templates into ~/Library/LaunchAgents only after manual writer is stopped.",
        "Load exactly com.mgc.trackb.paper-runtime; verify canonical config stack and PAPER-only invariants.",
        "Verify runtime truth reports one PID, one generation, lane_count expected, and READY_SUBMIT_CAPABLE.",
        "Leave review/stress-test overlays unloaded unless explicitly approved.",
        f"Use Dev root only: {repo_root}",
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Track B service ownership Phase 1 artifact.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--plist-output-dir", default=str(DEFAULT_PLIST_OUTPUT_DIR))
    parser.add_argument("--write", action="store_true", help="Write the ownership artifact.")
    parser.add_argument("--emit-plists", action="store_true", help="Write repo-local launchd plist templates.")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBServiceOwnershipConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        plist_output_dir=Path(args.plist_output_dir),
    )
    payload = build_service_ownership_artifact(config=config)
    written_plists: list[Path] = []
    if args.write:
        write_service_ownership_artifact(config=config, payload=payload)
    if args.emit_plists:
        written_plists = write_launchd_plist_templates(config=config)
        payload = {**payload, "written_plist_templates": [str(path) for path in written_plists]}
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            json.dumps(
                {
                    "phase": payload["phase"],
                    "service_count": payload["summary"]["service_count"],
                    "duplicate_writer_guard": payload["duplicate_writer_guard"]["classification"],
                    "ownership_artifact": payload["artifact_paths"]["ownership"],
                    "plist_output_dir": payload["artifact_paths"]["plist_output_dir"],
                    "written_plist_count": len(written_plists),
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0


def _active_runtime_processes(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_pid: dict[int, dict[str, Any]] = {}
    for row in rows:
        if not _is_runtime_writer(row):
            continue
        pid = int(row.get("pid") or 0)
        if pid <= 0 or pid in by_pid:
            continue
        by_pid[pid] = {"pid": pid, "command": str(row.get("command") or "")}
    return list(by_pid.values())


def _ps_rows() -> list[dict[str, Any]]:
    try:
        output = subprocess.check_output(["ps", "-wwaxo", "pid=,command="], text=True)
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


def _process_rows_with_runtime_artifacts(
    *,
    repo_root: Path,
    process_rows: Sequence[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    rows = [_normalize_process(row) for row in (process_rows if process_rows is not None else _ps_rows())]
    seen_pids = {int(row.get("pid") or 0) for row in rows}
    for row in _runtime_pid_artifact_rows(repo_root):
        if int(row.get("pid") or 0) not in seen_pids:
            rows.append(row)
    return rows


def _runtime_pid_artifact_rows(repo_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in (
        repo_root / PAPER_RUNTIME_PID_METADATA_PATH,
        repo_root / PAPER_RUNTIME_TRUTH_PATH,
    ):
        payload = _read_json(path)
        pid = _coerce_pid(payload.get("producer_pid") or payload.get("pid")) if payload else None
        if pid is None or not _pid_running(pid):
            continue
        runtime_instance_id = str(payload.get("runtime_instance_id") or "")
        rows.append(
            {
                "pid": pid,
                "command": (
                    "canonical-runtime-artifact "
                    "mgc_v05l.app.main probationary-paper-soak "
                    f"runtime_instance_id={runtime_instance_id} source={path}"
                ),
            }
        )
    pid_path = repo_root / PAPER_RUNTIME_PID_PATH
    try:
        pid = _coerce_pid(pid_path.read_text(encoding="utf-8").strip())
    except OSError:
        pid = None
    if pid is not None and _pid_running(pid):
        rows.append(
            {
                "pid": pid,
                "command": (
                    "canonical-runtime-pid-file "
                    "mgc_v05l.app.main probationary-paper-soak "
                    f"source={pid_path}"
                ),
            }
        )
    return rows


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _coerce_pid(value: Any) -> int | None:
    try:
        pid = int(value)
    except (TypeError, ValueError):
        return None
    return pid if pid > 0 else None


def _pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _is_runtime_writer(row: Mapping[str, Any]) -> bool:
    command = str(row.get("command") or "")
    return "mgc_v05l.app.main" in command and "probationary-paper-soak" in command


def _normalize_process(row: Mapping[str, Any]) -> dict[str, Any]:
    return {"pid": int(row.get("pid") or 0), "command": str(row.get("command") or "")}


def _expand_repo_root(value: str, repo_root: Path) -> str:
    return value.replace(str(REPO_ROOT), str(repo_root))


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
