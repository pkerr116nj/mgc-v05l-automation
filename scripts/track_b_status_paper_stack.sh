#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

JSON_ONLY=0
if [[ "${1:-}" == "--json" ]]; then
  JSON_ONLY=1
fi

RUNTIME_DIR="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime"
STATUS_DIR="${REPO_ROOT}/outputs/track_b_execution_core/paper_stack"
STATUS_ARTIFACT="${STATUS_DIR}/latest_paper_stack_status.json"
OPERABILITY_TMP="${STATUS_DIR}/.operability_status.$$.json"

mkdir -p "${STATUS_DIR}"

set +e
"${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_runtime_operability_contract \
  --repo-root "${REPO_ROOT}" \
  --json > "${OPERABILITY_TMP}"
OPERABILITY_RC=$?
set -e

"${PYTHON_BIN}" - "${REPO_ROOT}" "${RUNTIME_DIR}" "${OPERABILITY_TMP}" "${OPERABILITY_RC}" "${STATUS_ARTIFACT}" <<'PY'
import json
import os
import subprocess
import sys
import errno
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_hourly_runtime_recovery_audit import (
    SUPERVISOR_PAUSED,
    SUPERVISOR_RUNNING,
    classify_scheduler_evidence,
    collect_scheduler_evidence,
)
from mgc_v05l.execution_core.track_b_registry_truth_diagnostics import (
    TrackBDiagnosticsMode,
    TrackBRegistryTruthDiagnosticsConfig,
    build_track_b_registry_truth_diagnostics,
    write_track_b_registry_truth_diagnostics,
)

repo_root = Path(sys.argv[1]).resolve()
runtime_dir = Path(sys.argv[2])
operability_path = Path(sys.argv[3])
operability_rc = int(sys.argv[4])
status_artifact = Path(sys.argv[5])


def read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def read_lines(path: Path) -> list[str]:
    try:
        return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except OSError:
        return []


def process_running(pid: object) -> bool:
    try:
        value = int(pid)
    except (TypeError, ValueError):
        return False
    try:
        os.kill(value, 0)
    except OSError as exc:
        if exc.errno == errno.EPERM:
            return True
        return False
    try:
        stat = subprocess.check_output(["ps", "-p", str(value), "-o", "stat="], text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return True
    return not stat.startswith("Z")


def process_command(pid: object) -> str | None:
    try:
        value = int(pid)
    except (TypeError, ValueError):
        return None
    try:
        return subprocess.check_output(["ps", "-p", str(value), "-o", "command="], text=True).strip() or None
    except (OSError, subprocess.CalledProcessError):
        return None


def process_ppid(pid: object) -> int | None:
    try:
        value = int(pid)
    except (TypeError, ValueError):
        return None
    try:
        text = subprocess.check_output(["ps", "-p", str(value), "-o", "ppid="], text=True).strip()
        return int(text) if text else None
    except (OSError, subprocess.CalledProcessError, ValueError):
        return None


def screen_sessions() -> str:
    try:
        return subprocess.check_output(["screen", "-ls"], text=True, stderr=subprocess.STDOUT)
    except (OSError, subprocess.CalledProcessError) as exc:
        return getattr(exc, "output", "") or ""


def launchctl_has_label(label: str | None) -> bool:
    if not label:
        return False
    try:
        subprocess.check_output(
            ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        return True
    except (OSError, subprocess.CalledProcessError):
        pass
    try:
        output = subprocess.check_output(["launchctl", "list"], text=True, stderr=subprocess.DEVNULL)
    except (OSError, subprocess.CalledProcessError):
        return False
    return label in output


def parse_iso(value: object):
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


operability = read_json(operability_path)
pid_metadata = read_json(runtime_dir / "probationary_paper.pid.json")
runtime_truth = read_json(runtime_dir / "paper_runtime_truth.json")
launch_guard = read_json(runtime_dir / "probationary_paper.pid.json.launch_guard.json")
launch_status = read_json(runtime_dir / "probationary_paper_launch_status.json")
broker_reconciliation = read_json(
    repo_root / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
)
broker_truth_status = read_json(repo_root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json")
phase1_status = read_json(
    repo_root
    / "outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_listener_status.json"
)
# Canonical readiness is read only for extra status fields; operator_dashboard_readiness remains diagnostic only.
canonical_readiness = read_json(repo_root / "outputs/operator_dashboard/runtime/latest_canonical_readiness.json")
hourly_recovery_audit_path = (
    repo_root / "outputs/track_b_execution_core/runtime_recovery/latest_hourly_runtime_recovery_audit.json"
)
hourly_recovery_audit = read_json(hourly_recovery_audit_path)
standalone_recovery_status_path = (
    repo_root / "outputs/track_b_execution_core/runtime_recovery/latest_launchd_recovery_status.json"
)
standalone_recovery_tick_path = (
    repo_root / "outputs/track_b_execution_core/runtime_recovery/latest_launchd_recovery_tick.json"
)
standalone_recovery_disabled_marker_path = (
    repo_root / "outputs/track_b_execution_core/runtime_recovery/recovery_disabled_by_operator.json"
)
standalone_recovery_status = read_json(standalone_recovery_status_path)
standalone_recovery_tick = read_json(standalone_recovery_tick_path)
hourly_recovery_generated_at = parse_iso(hourly_recovery_audit.get("generated_at"))
hourly_recovery_age_seconds = (
    (datetime.now(timezone.utc) - hourly_recovery_generated_at.astimezone(timezone.utc)).total_seconds()
    if hourly_recovery_generated_at
    else None
)
live_scheduler_evidence = collect_scheduler_evidence(repo_root=repo_root)
live_scheduler_classification = classify_scheduler_evidence(live_scheduler_evidence)
standalone_recovery_label = "com.mgc.trackb.paper-runtime-recovery"
standalone_recovery_loaded = launchctl_has_label(standalone_recovery_label)
standalone_recovery_disabled = standalone_recovery_disabled_marker_path.exists()
if standalone_recovery_disabled:
    standalone_recovery_classification = "RECOVERY_DISABLED_BY_OPERATOR"
elif standalone_recovery_loaded:
    standalone_recovery_classification = "RECOVERY_ACTIVE"
else:
    standalone_recovery_classification = "SUPERVISOR_PAUSED"

pid_candidates = [
    runtime_truth.get("producer_pid"),
    runtime_truth.get("pid"),
    (operability.get("runtime_summary") or {}).get("pid"),
    pid_metadata.get("pid"),
    pid_metadata.get("producer_pid"),
]
pid = None
running = False
command = None
ppid = None
fallback_running: tuple[object, str | None, int | None] | None = None
for candidate in pid_candidates:
    try:
        candidate_pid = int(candidate)
    except (TypeError, ValueError):
        continue
    candidate_running = process_running(candidate_pid)
    candidate_command = process_command(candidate_pid) if candidate_running else None
    candidate_ppid = process_ppid(candidate_pid) if candidate_running else None
    if candidate_running and fallback_running is None:
        fallback_running = (candidate_pid, candidate_command, candidate_ppid)
    if candidate_running and "mgc_v05l.app.main probationary-paper-soak" in str(candidate_command or ""):
        pid = candidate_pid
        running = True
        command = candidate_command
        ppid = candidate_ppid
        break
if pid is None and fallback_running is not None:
    pid, command, ppid = fallback_running
    running = True
if pid is None:
    pid = next((candidate for candidate in pid_candidates if str(candidate or "").isdigit()), None)
screen_name = ""
try:
    screen_name = (runtime_dir / "probationary_paper.pid.screen_session").read_text(encoding="utf-8").strip()
except OSError:
    pass
launchctl_label = ""
try:
    launchctl_label = (runtime_dir / "probationary_paper.pid.launchctl_label").read_text(encoding="utf-8").strip()
except OSError:
    pass

screens = screen_sessions()
screen_active = bool(screen_name and screen_name in screens)
if running and launchctl_has_label(launchctl_label):
    owner = "launchd"
elif running and screen_active:
    owner = "screen"
elif running and ppid == 1:
    owner = "launchd_or_service"
elif running and command:
    owner = "manual_or_detached_process"
elif running:
    owner = "running_process_owner_unknown"
else:
    owner = "down"

config_stack = read_lines(runtime_dir / "paper_runtime_config_paths.txt")
review_overlay = "config/probationary_pattern_engine_paper_mnq_mgc_plus_mnq_us_intraday_review.yaml"
review_overlay_active = any(item.endswith(review_overlay) for item in config_stack)
config_summary = dict(operability.get("config_summary") or {})
lane_count = config_summary.get("lane_count") or runtime_truth.get("lane_count")
canonical_state = str(operability.get("canonical_state") or "UNKNOWN")
ready_submit_capable = bool(operability.get("ready_submit_capable")) and running
blockers = list(operability.get("blockers") or [])
warnings = list(operability.get("warnings") or [])
if not running:
    warnings.append({"code": "runtime_not_running", "detail": "No live Track B PAPER runtime process is present."})
elif command is None and ppid is None:
    warnings.append(
        {
            "code": "runtime_owner_metadata_unavailable",
            "detail": "Runtime PID is alive, but this environment did not allow process command/parent metadata lookup.",
            "source": "process_table",
        }
    )
if screen_name and not screen_active:
    warnings.append(
        {
            "code": "stale_screen_session_marker_ignored",
            "detail": "A screen-session marker exists, but screen does not report that session as active.",
            "source": "runtime_owner_marker",
        }
    )
if (
    hourly_recovery_age_seconds is not None
    and hourly_recovery_age_seconds > 3900
    and live_scheduler_classification == SUPERVISOR_PAUSED
):
    warnings.append(
        {
            "code": "stale_hourly_recovery_artifact_live_scheduler_paused",
            "detail": "The last hourly recovery audit artifact is stale; live scheduler probe reports PAUSED.",
            "source": "hourly_recovery_scheduler",
        }
    )
if (
    ((hourly_recovery_audit.get("hourly_supervisor") or {}).get("classification") or "") == SUPERVISOR_RUNNING
    and live_scheduler_classification == SUPERVISOR_PAUSED
    and not standalone_recovery_loaded
):
    warnings.append(
        {
            "code": "hourly_recovery_artifact_live_scheduler_conflict",
            "detail": "The last audit artifact claimed the hourly recovery supervisor was running, but live scheduler probe reports PAUSED.",
            "source": "hourly_recovery_scheduler",
        }
    )
if review_overlay_active:
    blockers.append(
        {
            "code": "review_overlay_active",
            "detail": "27-lane review overlay is present in the runtime config stack; default startup must not use it.",
        }
    )

registry_truth_diagnostics = {
    "mode": "CURRENT_HOT_PATH",
    "classification": "TRACK_B_DIAGNOSTICS_UNAVAILABLE",
    "diagnostic_only": True,
    "error": None,
}
try:
    registry_truth_config = TrackBRegistryTruthDiagnosticsConfig(
        repo_root=repo_root,
        mode=TrackBDiagnosticsMode.CURRENT_HOT_PATH,
    )
    registry_truth_report = build_track_b_registry_truth_diagnostics(config=registry_truth_config)
    registry_truth_path = write_track_b_registry_truth_diagnostics(
        config=registry_truth_config,
        report=registry_truth_report,
    )
    registry_truth_payload = registry_truth_report.to_dict()
    registry_truth_diagnostics = {
        "mode": registry_truth_payload["mode"],
        "classification": registry_truth_payload["classification"],
        "diagnostic_only": True,
        "report_path": str(registry_truth_path),
        "track_b_managed_futures_position_count": registry_truth_payload["track_b_managed_futures_position_count"],
        "track_b_managed_futures_positions": registry_truth_payload["broker_positions_by_scope"][
            "track_b_managed_futures_positions"
        ],
        "broker_open_order_count": registry_truth_payload["broker_open_order_count"],
        "lifecycle_open_position_count": registry_truth_payload["lifecycle_open_position_count"],
        "current_scope_review_required_count": len(registry_truth_payload["review_required_trade_ids"]),
        "current_scope_review_required_trade_ids": registry_truth_payload["review_required_trade_ids"],
        "historical_quarantined_count": max(
            0,
            len(registry_truth_payload["historical_review_required_trade_ids"])
            - len(registry_truth_payload["review_required_trade_ids"]),
        ),
        "latest_lifecycle_stress_preflight_hard_failure_count": registry_truth_payload[
            "latest_preflight_hard_failure_count"
        ],
        "stale_authority_reason_codes": [
            code
            for code in registry_truth_payload["reason_codes"]
            if "STALE" in str(code) or code == "TRUTH_AUTHORITY_STALE"
        ],
        "full_artifact_audit_mode": "separate_diagnostic_only",
        "full_artifact_audit_output_path": str(
            repo_root
            / "outputs/track_b_execution_core/diagnostics/latest_track_b_registry_truth_diagnostics_full_artifact_audit.json"
        ),
        "error": None,
    }
except Exception as exc:
    registry_truth_diagnostics = {
        "mode": "CURRENT_HOT_PATH",
        "classification": "TRACK_B_DIAGNOSTICS_UNAVAILABLE",
        "diagnostic_only": True,
        "error": f"{type(exc).__name__}: {exc}",
    }

next_action = "none"
if blockers:
    next_action = "resolve_blockers_before_start"
elif not running:
    next_action = "run scripts/track_b_start_paper_stack.sh"
elif not ready_submit_capable:
    next_action = "wait_or_refresh_status; runtime process exists but canonical submit-capable state is not confirmed"

payload = {
    "schema_version": "track_b_paper_stack_status_v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "repo_root": str(repo_root),
    "operability_rc": operability_rc,
    "startup_artifact": str(repo_root / "outputs/track_b_execution_core/paper_stack/latest_paper_stack_startup.json"),
    "status_artifact": str(status_artifact),
    "runtime": {
        "pid": int(pid) if str(pid or "").isdigit() else None,
        "running": running,
        "owner": owner,
        "command": command,
        "ppid": ppid,
        "runtime_instance_id": pid_metadata.get("runtime_instance_id") or runtime_truth.get("runtime_instance_id"),
        "restart_generation": pid_metadata.get("restart_generation") or runtime_truth.get("restart_generation"),
        "screen_session": screen_name if screen_active else None,
        "launchctl_label": launchctl_label or None,
    },
    "readiness": {
        "canonical_state": canonical_state,
        "ready_submit_capable": ready_submit_capable,
        "restart_allowed_if_runtime_down": operability.get("restart_allowed_if_runtime_down") is True,
        "market_schedule_state": canonical_readiness.get("market_schedule_state"),
        "stale_market_data_expected": canonical_readiness.get("stale_market_data_expected") is True,
        "next_expected_reopen_time": canonical_readiness.get("next_expected_reopen_time"),
        "market_data_grace_until": canonical_readiness.get("market_data_grace_until"),
        "readiness_block_is_scheduled_halt": canonical_readiness.get("readiness_block_is_scheduled_halt") is True,
        "blockers": blockers,
        "warnings": warnings,
    },
    "config": {
        "config_stack": config_stack,
        "lane_count": lane_count,
        "paper_only": config_summary.get("paper_only") is True,
        "live_money_eligible": config_summary.get("live_money_eligible") is True,
        "review_overlay_active": review_overlay_active,
        "forbidden_review_overlay": review_overlay,
    },
    "broker_lifecycle": {
        "reconciliation_classification": broker_reconciliation.get("classification")
        or broker_reconciliation.get("reconciliation_state"),
        "broker_truth_classification": broker_truth_status.get("classification"),
        "broker_truth_fresh": broker_truth_status.get("fresh"),
    },
    "registry_truth_diagnostics": registry_truth_diagnostics,
    "data": {
        "phase1_listener_classification": phase1_status.get("final_classification"),
        "phase1_latest_record_at": phase1_status.get("latest_record_at"),
        "phase1_provider_status": phase1_status.get("provider_status"),
    },
    "duplicate_writer": {
        "launch_guard_classification": launch_guard.get("classification"),
        "duplicate_writer_detected": launch_guard.get("duplicate_writer_detected") is True
        or ((runtime_truth.get("duplicate_writer_detection") or {}).get("duplicate_writer_detected") is True),
    },
    "recovery": {
        "classification": standalone_recovery_classification,
        "standalone_recovery_classification": standalone_recovery_classification,
        "standalone_recovery_status_artifact": str(standalone_recovery_status_path),
        "standalone_recovery_status_generated_at": standalone_recovery_status.get("generated_at"),
        "standalone_recovery_last_tick_artifact": str(standalone_recovery_tick_path),
        "standalone_recovery_last_tick": standalone_recovery_tick.get("generated_at"),
        "standalone_recovery_last_action": standalone_recovery_tick.get("last_action"),
        "standalone_recovery_last_blocker": standalone_recovery_tick.get("last_blocker"),
        "standalone_recovery_launchd_label": standalone_recovery_label,
        "standalone_recovery_launchd_loaded": standalone_recovery_loaded,
        "standalone_recovery_launchd_enabled": standalone_recovery_loaded and not standalone_recovery_disabled,
        "standalone_recovery_operator_disabled": standalone_recovery_disabled,
        "hourly_recovery_artifact": str(hourly_recovery_audit_path),
        "hourly_recovery_artifact_generated_at": hourly_recovery_audit.get("generated_at"),
        "hourly_recovery_artifact_age_seconds": hourly_recovery_age_seconds,
        "hourly_recovery_artifact_classification": hourly_recovery_audit.get("classification"),
        "hourly_recovery_artifact_supervisor_classification": (
            hourly_recovery_audit.get("hourly_supervisor") or {}
        ).get("classification"),
        "live_scheduler_classification": live_scheduler_classification,
        "hourly_recovery_active": standalone_recovery_loaded and not standalone_recovery_disabled,
        "hourly_recovery_paused": not standalone_recovery_loaded or standalone_recovery_disabled,
        "recovery_authoritative": standalone_recovery_loaded and not standalone_recovery_disabled,
        "authority_reason": (
            "standalone_launchd_recovery_service_disabled_by_operator"
            if standalone_recovery_disabled
            else (
                "standalone_launchd_recovery_service_loaded"
                if standalone_recovery_loaded
                else "standalone_launchd_recovery_service_not_loaded"
            )
        ),
        "latest_run_time": standalone_recovery_tick.get("generated_at") or hourly_recovery_audit.get("generated_at"),
        "scheduler_evidence": live_scheduler_evidence,
    },
    "safety": {
        "paper_only": config_summary.get("paper_only") is True,
        "live_money_eligible": operability.get("live_money_eligible") is True,
        "paper_proof_invoked": operability.get("paper_proof_invoked") is True,
        "broker_mutation_allowed": operability.get("broker_mutation_allowed") is True,
        "runtime_mutation_allowed": operability.get("runtime_mutation_allowed") is True,
    },
    "next_action": next_action,
}

status_artifact.parent.mkdir(parents=True, exist_ok=True)
tmp = status_artifact.with_name(f".{status_artifact.name}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(status_artifact)
print(json.dumps(payload, indent=2, sort_keys=True))
PY

rm -f "${OPERABILITY_TMP}"

if [[ "${JSON_ONLY}" -eq 0 ]]; then
  "${PYTHON_BIN}" - "${STATUS_ARTIFACT}" <<'PY'
import json
import sys
payload = json.load(open(sys.argv[1], encoding="utf-8"))
runtime = payload["runtime"]
readiness = payload["readiness"]
config = payload["config"]
recovery = payload["recovery"]
print(
    "Track B PAPER stack: "
    f"state={readiness['canonical_state']} "
    f"ready_submit_capable={readiness['ready_submit_capable']} "
    f"pid={runtime['pid']} running={runtime['running']} owner={runtime['owner']} "
    f"lane_count={config['lane_count']} "
    f"recovery={recovery['classification']} "
    f"next_action={payload['next_action']}"
)
print(f"status_artifact={payload['status_artifact']}")
PY
fi
