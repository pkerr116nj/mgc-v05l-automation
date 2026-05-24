#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

DEFAULT_RUNTIME_DIR="${REPO_ROOT}/outputs/operator_dashboard/runtime"
DEFAULT_HEALTH_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_health.json"
DEFAULT_STATUS_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_status.json"
DEFAULT_MARKDOWN_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_status.md"
DEFAULT_CANONICAL_READINESS_FILE="${DEFAULT_RUNTIME_DIR}/latest_canonical_readiness.json"
DEFAULT_CANONICAL_READINESS_SUMMARY_FILE="${DEFAULT_RUNTIME_DIR}/latest_canonical_readiness_summary.json"
DEFAULT_MAINTENANCE_SUPERVISOR_FILE="${DEFAULT_RUNTIME_DIR}/latest_maintenance_supervisor_decision.json"
DEFAULT_MAINTENANCE_SUPERVISOR_SUMMARY_FILE="${DEFAULT_RUNTIME_DIR}/latest_maintenance_supervisor_summary.json"
DEFAULT_PAPER_RUNTIME_TRUTH_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_truth.json"
DEFAULT_PAPER_PID_METADATA_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid.json"
DEFAULT_PAPER_CONFIG_IN_FORCE_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json"
DEFAULT_PAPER_OPERATOR_STATUS_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/operator_status.json"
DEFAULT_PAPER_RECONCILIATION_FILE="${REPO_ROOT}/outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
DEFAULT_PAPER_RUNTIME_LAUNCH_STATUS_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper_launch_status.json"
DEFAULT_AGENT_REGISTRY_FILE="${REPO_ROOT}/outputs/track_b_execution_core/agent_registry/latest_agent_registry.json"
DEFAULT_AGENT_HEALTH_FILE="${REPO_ROOT}/outputs/track_b_execution_core/agent_health/latest_agent_health.json"
DEFAULT_SELF_RECOVER_RULES_FILE="${REPO_ROOT}/outputs/track_b_execution_core/self_recover/latest_self_recover_rules.json"
DEFAULT_CRASH_LOOP_PROTECTION_FILE="${REPO_ROOT}/outputs/track_b_execution_core/crash_loop_protection/latest_crash_loop_protection.json"
DEFAULT_RUNTIME_RESUME_SEMANTICS_FILE="${REPO_ROOT}/outputs/track_b_execution_core/runtime_resume/latest_runtime_resume_semantics.json"
DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_FILE="${REPO_ROOT}/outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json"
DEFAULT_CONTROL_PLANE_SNAPSHOT_FILE="${REPO_ROOT}/outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"
DEFAULT_PAPER_RECOVERY_POLICY_FILE="${REPO_ROOT}/outputs/track_b_execution_core/paper_recovery_policy/latest_paper_recovery_policy.json"
DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_FILE="${REPO_ROOT}/outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json"
DEFAULT_ARTIFACT_ARCHIVE_PLAN_FILE="${REPO_ROOT}/outputs/track_b_execution_core/artifact_retention/latest_artifact_archive_plan.json"
DEFAULT_STARTUP_FILE="${REPO_ROOT}/outputs/operator_dashboard/startup_control_plane_snapshot.json"
DEFAULT_OPERABILITY_FILE="${REPO_ROOT}/outputs/operator_dashboard/supervised_paper_operability_snapshot.json"
DEFAULT_INFO_FILE="${DEFAULT_RUNTIME_DIR}/operator_dashboard.json"
DEFAULT_DASHBOARD_PAYLOAD_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_dashboard.json"
DEFAULT_LIVE_STARTUP_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_startup_control_plane.json"
DEFAULT_LIVE_OPERABILITY_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_operability.json"
DEFAULT_URL="${MGC_OPERATOR_DASHBOARD_URL:-http://127.0.0.1:8790/}"
DEFAULT_HEALTH_ATTEMPTS=5
DEFAULT_HEALTH_RETRY_DELAY_SECONDS=1

STATUS_FILE="${DEFAULT_STATUS_FILE}"
MARKDOWN_FILE="${DEFAULT_MARKDOWN_FILE}"
CANONICAL_READINESS_FILE="${DEFAULT_CANONICAL_READINESS_FILE}"
CANONICAL_READINESS_SUMMARY_FILE="${DEFAULT_CANONICAL_READINESS_SUMMARY_FILE}"
MAINTENANCE_SUPERVISOR_FILE="${DEFAULT_MAINTENANCE_SUPERVISOR_FILE}"
MAINTENANCE_SUPERVISOR_SUMMARY_FILE="${DEFAULT_MAINTENANCE_SUPERVISOR_SUMMARY_FILE}"
HEALTH_FILE="${DEFAULT_HEALTH_FILE}"
DASHBOARD_URL="${DEFAULT_URL}"
HEALTH_ATTEMPTS="${DEFAULT_HEALTH_ATTEMPTS}"
HEALTH_RETRY_DELAY_SECONDS="${DEFAULT_HEALTH_RETRY_DELAY_SECONDS}"
DASHBOARD_PAYLOAD_FILE="${DEFAULT_DASHBOARD_PAYLOAD_FILE}"
LIVE_STARTUP_FILE="${DEFAULT_LIVE_STARTUP_FILE}"
LIVE_OPERABILITY_FILE="${DEFAULT_LIVE_OPERABILITY_FILE}"

while (($# > 0)); do
  case "$1" in
    --output)
      STATUS_FILE="$2"
      shift 2
      ;;
    --output=*)
      STATUS_FILE="${1#*=}"
      shift
      ;;
    --markdown-output)
      MARKDOWN_FILE="$2"
      shift 2
      ;;
    --markdown-output=*)
      MARKDOWN_FILE="${1#*=}"
      shift
      ;;
    --health-file)
      HEALTH_FILE="$2"
      shift 2
      ;;
    --health-file=*)
      HEALTH_FILE="${1#*=}"
      shift
      ;;
    --canonical-readiness-output)
      CANONICAL_READINESS_FILE="$2"
      shift 2
      ;;
    --canonical-readiness-output=*)
      CANONICAL_READINESS_FILE="${1#*=}"
      shift
      ;;
    --canonical-readiness-summary-output)
      CANONICAL_READINESS_SUMMARY_FILE="$2"
      shift 2
      ;;
    --canonical-readiness-summary-output=*)
      CANONICAL_READINESS_SUMMARY_FILE="${1#*=}"
      shift
      ;;
    --maintenance-supervisor-output)
      MAINTENANCE_SUPERVISOR_FILE="$2"
      shift 2
      ;;
    --maintenance-supervisor-output=*)
      MAINTENANCE_SUPERVISOR_FILE="${1#*=}"
      shift
      ;;
    --maintenance-supervisor-summary-output)
      MAINTENANCE_SUPERVISOR_SUMMARY_FILE="$2"
      shift 2
      ;;
    --maintenance-supervisor-summary-output=*)
      MAINTENANCE_SUPERVISOR_SUMMARY_FILE="${1#*=}"
      shift
      ;;
    --dashboard-url)
      DASHBOARD_URL="$2"
      shift 2
      ;;
    --dashboard-url=*)
      DASHBOARD_URL="${1#*=}"
      shift
      ;;
    --health-attempts)
      HEALTH_ATTEMPTS="$2"
      shift 2
      ;;
    --health-attempts=*)
      HEALTH_ATTEMPTS="${1#*=}"
      shift
      ;;
    --health-retry-delay-seconds)
      HEALTH_RETRY_DELAY_SECONDS="$2"
      shift 2
      ;;
    --health-retry-delay-seconds=*)
      HEALTH_RETRY_DELAY_SECONDS="${1#*=}"
      shift
      ;;
    *)
      echo "Unsupported argument: $1" >&2
      exit 1
      ;;
  esac
done

ensure_dir "$(dirname "${STATUS_FILE}")"
ensure_dir "$(dirname "${MARKDOWN_FILE}")"
ensure_dir "$(dirname "${HEALTH_FILE}")"
ensure_dir "$(dirname "${DASHBOARD_PAYLOAD_FILE}")"
ensure_dir "$(dirname "${CANONICAL_READINESS_FILE}")"
ensure_dir "$(dirname "${CANONICAL_READINESS_SUMMARY_FILE}")"
ensure_dir "$(dirname "${MAINTENANCE_SUPERVISOR_FILE}")"
ensure_dir "$(dirname "${MAINTENANCE_SUPERVISOR_SUMMARY_FILE}")"

refresh_operator_readiness_artifacts() {
  "${PYTHON_BIN}" -m mgc_v05l.app.track_b_operator_readiness_refresher \
    --repo-root "${REPO_ROOT}" \
    --once >/dev/null
}

refresh_broker_truth_lease() {
  "${PYTHON_BIN}" -m mgc_v05l.app.track_b_broker_truth_lease \
    --repo-root "${REPO_ROOT}" \
    --no-history \
    --json >/dev/null
}

set +e
refresh_operator_readiness_artifacts
operator_readiness_refresh_exit_code=$?
refresh_broker_truth_lease
broker_truth_lease_exit_code=$?
set -e

refresh_canonical_readiness() {
  local tmp_summary
  tmp_summary="${CANONICAL_READINESS_SUMMARY_FILE}.tmp"
  rm -f "${tmp_summary}"
  if "${PYTHON_BIN}" -m mgc_v05l.app.track_b_canonical_readiness \
    --repo-root "${REPO_ROOT}" \
    --expected-root "${REPO_ROOT}" \
    --output-path "${CANONICAL_READINESS_FILE}" \
    --json > "${tmp_summary}"; then
    mv "${tmp_summary}" "${CANONICAL_READINESS_SUMMARY_FILE}"
    return 0
  fi
  local exit_code=$?
  if [[ -s "${tmp_summary}" ]]; then
    mv "${tmp_summary}" "${CANONICAL_READINESS_SUMMARY_FILE}"
  else
    rm -f "${tmp_summary}"
  fi
  return "${exit_code}"
}

set +e
refresh_canonical_readiness
canonical_readiness_exit_code=$?
set -e

canonical_readiness_classification() {
  "${PYTHON_BIN}" - <<'PY' "${CANONICAL_READINESS_SUMMARY_FILE}"
import json
import sys
from pathlib import Path

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
print(payload.get("classification") or "NOT_READY_CONFIG")
PY
}

print_canonical_readiness_summary() {
  "${PYTHON_BIN}" - <<'PY' "${CANONICAL_READINESS_SUMMARY_FILE}" >&2
import json
import sys
from pathlib import Path

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {"classification": "NOT_READY_CONFIG", "blockers": ["canonical_readiness_summary_missing"]}

print("canonical_readiness_summary:")
for key in (
    "classification",
    "blockers",
    "warnings",
    "root_match",
    "broker_truth_fresh",
    "broker_truth_lease_state",
    "broker_truth_lease_age_seconds",
    "broker_truth_lease_entry_seconds_remaining",
    "proof_readiness_classification",
    "shared_truth_open_order_truth",
    "shared_truth_managed_order_registry",
    "shared_truth_order_adjustment_planner",
    "shared_truth_position_truth",
    "shared_truth_runtime_environment_truth",
    "shared_truth_managed_position_registry",
    "shared_truth_broker_lease",
    "reconciliation_state",
    "eligible_lane_count",
    "quarantine_count",
):
    value = payload.get(key)
    if isinstance(value, list):
        value = ",".join(str(item) for item in value) if value else "none"
    print(f"  {key}={value}")
PY
}

merge_canonical_readiness_status() {
  "${PYTHON_BIN}" - <<'PY' "${STATUS_FILE}" "${CANONICAL_READINESS_FILE}" "${CANONICAL_READINESS_SUMMARY_FILE}" "${broker_truth_lease_exit_code}" "${operator_readiness_refresh_exit_code}"
import json
import sys
from pathlib import Path

from mgc_v05l.execution_core.track_b_control_plane_snapshot_status import classify_control_plane_snapshot_status
from mgc_v05l.execution_core.track_b_control_plane_top_line import build_track_b_control_plane_top_line

status_path = Path(sys.argv[1])
readiness_path = Path(sys.argv[2])
summary_path = Path(sys.argv[3])

try:
    status = json.loads(status_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    status = {}
try:
    readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    readiness = {}
try:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    summary = {}

classification = summary.get("classification") or readiness.get("canonical_readiness") or "NOT_READY_CONFIG"
status["canonical_readiness"] = classification
status["canonical_readiness_summary"] = summary
status["canonical_readiness_artifact"] = str(readiness_path)
status["readiness_blockers"] = readiness.get("readiness_blockers") or []
status["readiness_warnings"] = readiness.get("readiness_warnings") or []
status["root_guard_summary"] = readiness.get("root_guard_summary") or {}
broker_truth_lease = readiness.get("broker_truth_lease") or {}
status["broker_truth_fresh"] = (readiness.get("broker_truth") or {}).get("fresh") is True
status["broker_truth_lease_state"] = broker_truth_lease.get("lease_state")
status["broker_truth_lease_age_seconds"] = broker_truth_lease.get("age_seconds")
status["broker_truth_lease_entry_seconds_remaining"] = broker_truth_lease.get("entry_seconds_remaining")
status["broker_truth_lease_exit_code"] = int(sys.argv[4])
status["operator_readiness_refresh_exit_code"] = int(sys.argv[5])
shared_truth = readiness.get("execution_core_shared_truth") or {}
proof_readiness = shared_truth.get("proof_readiness") or {}
shared_truth_classifications = shared_truth.get("classifications") or {}
shared_truth_artifact_paths = shared_truth.get("artifact_paths") or {}
shared_truth_source_paths = [str(path) for path in shared_truth_artifact_paths.values() if path]
market_closed = proof_readiness.get("classification") == "MARKET_CLOSED_NO_FRESH_BARS"
status["shared_truth"] = {
    "source": "canonical_readiness_execution_core_shared_truth",
    "source_authority": "execution_core_authority",
    "projection_only": True,
    "dashboard_projection_authority": False,
    "not_routing_authority": True,
    "source_authority_path": None,
    "source_authority_paths": shared_truth_source_paths,
    "authority_owner": "execution_core",
    "operator_dashboard_display_only": True,
    "generated_from_control_plane_snapshot_id": None,
    "control_plane_snapshot_required": False,
    "projection_metadata_complete": bool(shared_truth_source_paths),
    "projection_degraded": not bool(shared_truth_source_paths),
    "diagnostic_only": not bool(shared_truth_source_paths),
    "degraded_reason": None if shared_truth_source_paths else "missing_source_authority_path",
    "proof_readiness": proof_readiness.get("classification"),
    "open_order_truth": shared_truth_classifications.get("Open Order Truth"),
    "managed_order_registry": shared_truth_classifications.get("Managed Order Registry"),
    "order_adjustment_planner": shared_truth_classifications.get("Order Adjustment Planner"),
    "position_truth": shared_truth_classifications.get("Position Truth"),
    "runtime_environment_truth": shared_truth_classifications.get("Runtime Environment Truth"),
    "managed_position_registry": shared_truth_classifications.get("Managed Position Registry"),
    "reconciliation": shared_truth_classifications.get("Reconciliation"),
    "broker_truth_lease": shared_truth_classifications.get("Broker Truth Lease"),
    "phase1_session_reason": proof_readiness.get("phase1_session_reason"),
    "market_closed_no_fresh_bars_expected": market_closed,
    "operator_message": "Market closed/no fresh bars expected" if market_closed else None,
    "artifact_paths": shared_truth_artifact_paths,
}
status["eligible_lane_count"] = int((readiness.get("runtime") or {}).get("eligible_lane_count") or 0)
status["quarantine_count"] = int((readiness.get("lane_quarantine") or {}).get("quarantine_count") or 0)
status["lane_quarantine"] = readiness.get("lane_quarantine") or {}
status["ready_submit_capable"] = classification == "READY_SUBMIT_CAPABLE"
status["ready_observation_only"] = classification == "READY_OBSERVATION_ONLY"
status["degraded_no_submit"] = classification == "DEGRADED_NO_SUBMIT"
status["paper_only"] = True
status["live_money_eligible"] = False

status_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

refresh_maintenance_supervisor() {
  local tmp_summary
  tmp_summary="${MAINTENANCE_SUPERVISOR_SUMMARY_FILE}.tmp"
  rm -f "${tmp_summary}"
  if "${PYTHON_BIN}" -m mgc_v05l.app.track_b_readiness_maintenance_supervisor \
    --repo-root "${REPO_ROOT}" \
    --output-path "${MAINTENANCE_SUPERVISOR_FILE}" \
    --json > "${tmp_summary}"; then
    mv "${tmp_summary}" "${MAINTENANCE_SUPERVISOR_SUMMARY_FILE}"
    return 0
  fi
  local exit_code=$?
  if [[ -s "${tmp_summary}" ]]; then
    mv "${tmp_summary}" "${MAINTENANCE_SUPERVISOR_SUMMARY_FILE}"
  else
    rm -f "${tmp_summary}"
  fi
  return "${exit_code}"
}

print_maintenance_supervisor_summary() {
  "${PYTHON_BIN}" - <<'PY' "${MAINTENANCE_SUPERVISOR_SUMMARY_FILE}" >&2
import json
import sys
from pathlib import Path

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {"supervisor_state": "BLOCKED", "recommended_actions": ["supervisor_summary_missing"]}

print("maintenance_supervisor_summary:")
for key in (
    "supervisor_state",
    "recommended_actions",
    "action_scope",
    "blockers",
    "warnings",
    "operator_action_required",
    "submit_block_required",
):
    value = payload.get(key)
    if isinstance(value, list):
        value = ",".join(str(item) for item in value) if value else "none"
    print(f"  {key}={value}")
PY
}

merge_maintenance_supervisor_status() {
  "${PYTHON_BIN}" - <<'PY' "${STATUS_FILE}" "${MAINTENANCE_SUPERVISOR_FILE}" "${MAINTENANCE_SUPERVISOR_SUMMARY_FILE}"
import json
import sys
from pathlib import Path

status_path = Path(sys.argv[1])
decision_path = Path(sys.argv[2])
summary_path = Path(sys.argv[3])

try:
    status = json.loads(status_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    status = {}
try:
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    decision = {}
try:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    summary = {}

status["maintenance_supervisor"] = {
    "supervisor_state": summary.get("supervisor_state") or decision.get("supervisor_state") or "BLOCKED",
    "recommended_actions": summary.get("recommended_actions") or decision.get("recommended_actions") or [],
    "action_scope": summary.get("action_scope") or decision.get("action_scope") or [],
    "operator_action_required": summary.get("operator_action_required") is True
    or decision.get("operator_action_required") is True,
    "submit_block_required": summary.get("submit_block_required") is True
    or decision.get("submit_block_required") is True,
    "blockers": summary.get("blockers") or [row.get("code") for row in decision.get("blockers") or [] if isinstance(row, dict)],
    "warnings": summary.get("warnings") or [row.get("code") for row in decision.get("warnings") or [] if isinstance(row, dict)],
    "artifact": str(decision_path),
    "summary_artifact": str(summary_path),
}
status["maintenance_supervisor_state"] = status["maintenance_supervisor"]["supervisor_state"]
status["maintenance_supervisor_recommended_actions"] = status["maintenance_supervisor"]["recommended_actions"]
status["maintenance_supervisor_operator_action_required"] = status["maintenance_supervisor"]["operator_action_required"]
status["maintenance_supervisor_submit_block_required"] = status["maintenance_supervisor"]["submit_block_required"]
status["paper_only"] = True
status["live_money_eligible"] = False

status_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

merge_paper_runtime_truth_status() {
  "${PYTHON_BIN}" - <<'PY' "${STATUS_FILE}" "${DEFAULT_PAPER_RUNTIME_TRUTH_FILE}"
import json
import sys
from pathlib import Path

status_path = Path(sys.argv[1])
truth_path = Path(sys.argv[2])

try:
    status = json.loads(status_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    status = {}
try:
    truth = json.loads(truth_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    truth = {}

status["paper_runtime_truth_artifact"] = str(truth_path)
status["paper_runtime_truth_evidence_only"] = True
status["paper_runtime_truth"] = truth
status["paper_runtime_truth_present"] = bool(truth)
status["paper_runtime_truth_freshness_state"] = truth.get("freshness_state")
status["paper_runtime_truth_heartbeat_state"] = truth.get("heartbeat_state")
status["paper_runtime_truth_writer_authority"] = truth.get("writer_authority")
status["paper_runtime_truth_runtime_instance_id"] = truth.get("runtime_instance_id")
status["paper_runtime_truth_lane_count"] = truth.get("lane_count")
status["paper_runtime_truth_b_plus_threshold"] = truth.get("b_plus_threshold")
status["paper_runtime_truth_test_mule_enabled"] = truth.get("test_mule_enabled")
status["paper_only"] = True
status["live_money_eligible"] = False

status_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

merge_paper_runtime_generation_status() {
  "${PYTHON_BIN}" - <<'PY' "${STATUS_FILE}" "${DEFAULT_PAPER_PID_METADATA_FILE}" "${DEFAULT_PAPER_RUNTIME_TRUTH_FILE}" "${DEFAULT_PAPER_CONFIG_IN_FORCE_FILE}" "${DEFAULT_PAPER_OPERATOR_STATUS_FILE}" "${DEFAULT_PAPER_RECONCILIATION_FILE}" "${DEFAULT_PAPER_RUNTIME_LAUNCH_STATUS_FILE}" "${REPO_ROOT}"
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_runtime_truth_contract import (
    classify_launch_status_convergence,
    classify_freshness,
    classify_pid_metadata,
    classify_runtime_launch_guard,
    runtime_generation_mismatches,
)

status_path = Path(sys.argv[1])
metadata_path = Path(sys.argv[2])
truth_path = Path(sys.argv[3])
config_path = Path(sys.argv[4])
operator_path = Path(sys.argv[5])
reconciliation_path = Path(sys.argv[6])
launch_status_path = Path(sys.argv[7])
expected_root = str(Path(sys.argv[8]).resolve())

def read_json(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}

def process_probe(pid: object) -> dict:
    try:
        pid_int = int(pid)
    except (TypeError, ValueError):
        return {"running": False, "zombie": False, "cwd": None, "command": ""}
    try:
        os.kill(pid_int, 0)
        running = True
    except PermissionError:
        running = True
    except OSError:
        running = False
    command = ""
    stat = ""
    cwd = None
    if running:
        try:
            proc = subprocess.run(["ps", "-p", str(pid_int), "-o", "stat=", "-o", "command="], check=False, capture_output=True, text=True, timeout=2)
            line = proc.stdout.strip()
            if line:
                parts = line.split(maxsplit=1)
                stat = parts[0]
                command = parts[1] if len(parts) > 1 else ""
        except (OSError, subprocess.SubprocessError):
            pass
        try:
            proc = subprocess.run(["lsof", "-a", "-p", str(pid_int), "-d", "cwd", "-Fn"], check=False, capture_output=True, text=True, timeout=2)
            rows = [row[1:] for row in proc.stdout.splitlines() if row.startswith("n")]
            cwd = str(Path(rows[-1]).resolve()) if rows else None
        except (OSError, subprocess.SubprocessError):
            cwd = None
    return {"running": running, "zombie": stat.startswith("Z"), "cwd": cwd, "command": command}

try:
    status = json.loads(status_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    status = {}

pid_metadata = read_json(metadata_path)
runtime_truth = read_json(truth_path)
config_in_force = read_json(config_path)
operator_status = read_json(operator_path)
reconciliation = read_json(reconciliation_path)
launch_status = read_json(launch_status_path)
probe = process_probe(pid_metadata.get("pid"))
metadata_state = classify_pid_metadata(
    pid_metadata,
    now=datetime.now(timezone.utc),
    freshness_ttl_seconds=float(pid_metadata.get("freshness_ttl_seconds") or 180.0),
    process_probe=probe,
    expected_root=expected_root,
)
mismatches = runtime_generation_mismatches(
    pid_metadata=pid_metadata,
    runtime_truth=runtime_truth,
    config_in_force=config_in_force,
    operator_status=operator_status,
)
duplicate_writer_detected = bool(
    (runtime_truth.get("duplicate_writer_detection") or {}).get("duplicate_writer_detected")
    or len({str(row.get("runtime_instance_id")) for row in (pid_metadata, runtime_truth, config_in_force, operator_status) if row.get("runtime_instance_id")}) > 1
)
broker_clean = str(reconciliation.get("classification") or "") == "TRACK_B_PAPER_BROKER_RECONCILED"
launch_guard = classify_runtime_launch_guard(
    pid_metadata_state=metadata_state,
    pid_metadata=pid_metadata,
    runtime_truth=runtime_truth,
    config_in_force=config_in_force,
    operator_status=operator_status,
    broker_clean=broker_clean,
    process_running=probe.get("running"),
    duplicate_writer_detected=duplicate_writer_detected,
)
launch_status_convergence = classify_launch_status_convergence(
    launch_status=launch_status,
    pid_metadata=pid_metadata,
    runtime_truth=runtime_truth,
    process_probe=probe,
    expected_root=expected_root,
    duplicate_writer_detected=duplicate_writer_detected,
)

def artifact_freshness(payload: dict, present: bool) -> dict:
    timestamp = payload.get("generated_at") or payload.get("updated_at") or payload.get("last_heartbeat_at")
    ttl = float(payload.get("freshness_ttl_seconds") or 180.0)
    return classify_freshness(
        generated_at=timestamp,
        freshness_ttl_seconds=ttl,
        now=datetime.now(timezone.utc),
        artifact_present=present,
    ).as_dict()

status["paper_runtime_generation_evidence_only"] = True
status["paper_runtime_pid_metadata_artifact"] = str(metadata_path)
status["paper_runtime_pid_metadata_present"] = bool(pid_metadata)
status["paper_runtime_pid_metadata"] = pid_metadata
status["paper_runtime_pid_metadata_state"] = metadata_state
status["paper_runtime_pid_metadata_process_probe"] = probe
status["paper_runtime_generation_mismatches"] = list(mismatches)
status["paper_runtime_generation_runtime_truth_freshness"] = artifact_freshness(runtime_truth, bool(runtime_truth))
status["paper_runtime_generation_config_in_force_freshness"] = artifact_freshness(config_in_force, bool(config_in_force))
status["paper_runtime_generation_operator_status_freshness"] = artifact_freshness(operator_status, bool(operator_status))
status["paper_runtime_generation_duplicate_writer_state"] = (
    "DUPLICATE_WRITER_DETECTED" if duplicate_writer_detected else "NO_DUPLICATE_WRITER_EVIDENCE"
)
status["paper_runtime_launch_guard"] = launch_guard
status["paper_runtime_launch_guard_classification"] = launch_guard.get("classification")
status["paper_runtime_launch_guard_cleanup_allowed"] = launch_guard.get("cleanup_allowed")
status["paper_runtime_launch_guard_launch_allowed"] = launch_guard.get("launch_allowed")
status["paper_runtime_launch_guard_blockers"] = launch_guard.get("blockers")
status["paper_runtime_launch_guard_broker_clean"] = launch_guard.get("broker_clean")
status["paper_runtime_launch_status_artifact"] = str(launch_status_path)
status["paper_runtime_launch_status"] = launch_status
status["paper_runtime_launch_status_original_classification"] = launch_status_convergence.get("original_classification")
status["paper_runtime_launch_status_effective_classification"] = launch_status_convergence.get("effective_classification")
status["paper_runtime_launch_status_runtime_converged"] = launch_status_convergence.get("runtime_converged")
status["paper_runtime_launch_status_stale_failure_superseded"] = launch_status_convergence.get("stale_failure_superseded")
status["paper_runtime_launch_status_blockers"] = launch_status_convergence.get("blockers")
status["paper_runtime_reconciliation_artifact"] = str(reconciliation_path)
status["paper_runtime_generation_runtime_instance_id"] = runtime_truth.get("runtime_instance_id") or pid_metadata.get("runtime_instance_id")
status["paper_runtime_generation_restart_generation"] = runtime_truth.get("restart_generation") or pid_metadata.get("restart_generation")
status["paper_only"] = True
status["live_money_eligible"] = False

status_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

merge_control_plane_services_status() {
  "${PYTHON_BIN}" - <<'PY' "${STATUS_FILE}" "${DEFAULT_AGENT_REGISTRY_FILE}" "${DEFAULT_AGENT_HEALTH_FILE}" "${DEFAULT_SELF_RECOVER_RULES_FILE}" "${DEFAULT_CRASH_LOOP_PROTECTION_FILE}" "${DEFAULT_RUNTIME_RESUME_SEMANTICS_FILE}" "${DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_FILE}" "${DEFAULT_PAPER_RECOVERY_POLICY_FILE}" "${DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_FILE}" "${DEFAULT_CONTROL_PLANE_SNAPSHOT_FILE}" "${DEFAULT_ARTIFACT_ARCHIVE_PLAN_FILE}"
import json
import sys
from pathlib import Path

from mgc_v05l.execution_core.track_b_control_plane_snapshot_status import classify_control_plane_snapshot_status
from mgc_v05l.execution_core.track_b_control_plane_top_line import build_track_b_control_plane_top_line

status_path = Path(sys.argv[1])
agent_registry_path = Path(sys.argv[2])
agent_health_path = Path(sys.argv[3])
self_recover_path = Path(sys.argv[4])
crash_loop_path = Path(sys.argv[5])
runtime_resume_path = Path(sys.argv[6])
runtime_supervisor_path = Path(sys.argv[7])
paper_recovery_policy_path = Path(sys.argv[8])
paper_autonomous_recovery_plan_path = Path(sys.argv[9])
control_plane_snapshot_path = Path(sys.argv[10])
artifact_archive_plan_path = Path(sys.argv[11])

def read_json(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}

try:
    status = json.loads(status_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    status = {}

agent_registry = read_json(agent_registry_path)
agent_health = read_json(agent_health_path)
self_recover = read_json(self_recover_path)
crash_loop = read_json(crash_loop_path)
runtime_resume = read_json(runtime_resume_path)
runtime_supervisor = read_json(runtime_supervisor_path)
paper_recovery_policy = read_json(paper_recovery_policy_path)
paper_autonomous_recovery_plan = read_json(paper_autonomous_recovery_plan_path)
control_plane_snapshot = read_json(control_plane_snapshot_path)
artifact_archive_plan = read_json(artifact_archive_plan_path)
control_plane_status = classify_control_plane_snapshot_status(control_plane_snapshot)
control_plane_top_line = build_track_b_control_plane_top_line(control_plane_snapshot)
operator_ack = runtime_supervisor.get("operator_ack") or {}
paper_action_policy = paper_recovery_policy.get("paper_action_policy")
paper_reason = str(paper_recovery_policy.get("reason") or "")
if paper_action_policy in {"OBSERVE", "REFRESH_EVIDENCE"} and "MARKET_CLOSED_NO_FRESH_BARS" in paper_reason:
    paper_recovery_diagnostic = "WAIT_MARKET_CLOSED"
elif paper_action_policy == "AUTONOMOUS_RETRY_ELIGIBLE":
    paper_recovery_diagnostic = "BOUNDED_AUTONOMOUS_RETRY"
elif paper_action_policy == "SCOPED_RECOVERY_ELIGIBLE":
    paper_recovery_diagnostic = "SCOPED_RECOVERY_ELIGIBLE"
elif paper_action_policy == "QUARANTINE_OBSERVE_ONLY":
    paper_recovery_diagnostic = "QUARANTINE_OBSERVE_ONLY"
elif paper_action_policy == "HARD_UNSAFE_HOLD":
    paper_recovery_diagnostic = "HARD_UNSAFE_HOLD"
else:
    paper_recovery_diagnostic = paper_action_policy
requires_operator_ack_for_paper = paper_recovery_policy.get("requires_operator_ack_for_paper") is True
def autonomous_next_action(payload):
    for action in payload.get("proposed_actions") or []:
        if isinstance(action, dict) and (action.get("action_type") or action.get("action_id")):
            return action.get("action_type") or action.get("action_id")
    for action in payload.get("blocked_actions") or []:
        if isinstance(action, dict) and (action.get("action_type") or action.get("action_id")):
            return action.get("action_type") or action.get("action_id")
    return None

def autonomous_budget_summary(payload):
    evidence = payload.get("evidence_summary") if isinstance(payload.get("evidence_summary"), dict) else {}
    budget = evidence.get("bounded_recovery_budget") if isinstance(evidence.get("bounded_recovery_budget"), dict) else {}
    return {
        "budget_exhausted": budget.get("budget_exhausted"),
        "max_attempts_per_target": budget.get("max_attempts_per_target"),
        "max_attempts_per_window": budget.get("max_attempts_per_window"),
        "cooldown_seconds": budget.get("cooldown_seconds"),
    }

market_closed = (
    self_recover.get("recommendation") == "WAIT_MARKET_CLOSED"
    or runtime_resume.get("classification") == "RESUME_BLOCKED_MARKET_CLOSED"
    or runtime_resume.get("reason") == "MARKET_CLOSED_NO_FRESH_BARS"
    or runtime_supervisor.get("supervisor_mode") == "MARKET_CLOSED_WAIT"
    or runtime_supervisor.get("proof_window_status") == "market_closed"
    or paper_recovery_diagnostic == "WAIT_MARKET_CLOSED"
)
authority_paths = {
    "agent_registry": str(agent_registry_path),
    "agent_health": str(agent_health_path),
    "self_recover": str(self_recover_path),
    "crash_loop_protection": str(crash_loop_path),
    "runtime_resume": str(runtime_resume_path),
    "runtime_supervisor": str(runtime_supervisor_path),
    "control_plane_snapshot": str(control_plane_snapshot_path),
    "paper_recovery_policy": str(paper_recovery_policy_path),
    "paper_autonomous_recovery_plan": str(paper_autonomous_recovery_plan_path),
    "artifact_archive_plan": str(artifact_archive_plan_path),
}
source_authority_paths = [path for path in authority_paths.values() if path]
control_plane_snapshot_id = control_plane_snapshot.get("control_plane_snapshot_id")
status["track_b_control_plane"] = {
    "source": "execution_core_control_plane_authority_projection",
    "source_authority": "execution_core_authority",
    "projection_only": True,
    "dashboard_projection_authority": False,
    "not_routing_authority": True,
    "source_authority_path": None,
    "source_authority_paths": source_authority_paths,
    "authority_owner": "execution_core",
    "operator_dashboard_display_only": True,
    "generated_from_control_plane_snapshot_id": control_plane_snapshot_id,
    "control_plane_snapshot_required": True,
    "projection_metadata_complete": bool(source_authority_paths),
    "projection_degraded": not bool(source_authority_paths),
    "diagnostic_only": control_plane_status["diagnostic_only"] or not bool(source_authority_paths),
    "degraded_reason": None if source_authority_paths else "missing_source_authority_path",
    "agent_registry": agent_registry.get("classification"),
    "agent_health": agent_health.get("classification"),
    "agent_health_schema_version": control_plane_snapshot.get("agent_health_schema_version") or agent_health.get("schema_version"),
    "agent_health_classification": control_plane_snapshot.get("agent_health_classification") or agent_health.get("classification"),
    "agent_health_summary": control_plane_snapshot.get("agent_health_summary") or agent_health.get("summary") or {},
    "agent_health_top_blockers": (control_plane_snapshot.get("agent_health_top_blockers") or [
        agent for agent in (agent_health.get("agents") or [])
        if isinstance(agent, dict) and (
            agent.get("blocking_for_proof") is True
            or agent.get("blocking_for_runtime_submit") is True
            or agent.get("blocking_for_recovery") is True
        )
    ])[:5],
    "agent_health_blocks_proof": control_plane_snapshot.get("agent_health_blocks_proof") is True,
    "agent_health_blocks_runtime_submit": control_plane_snapshot.get("agent_health_blocks_runtime_submit") is True,
    "agent_health_blocks_recovery": control_plane_snapshot.get("agent_health_blocks_recovery") is True,
    "agent_health_has_duplicate_writer": control_plane_snapshot.get("agent_health_has_duplicate_writer") is True,
    "agent_health_duplicate_process_count": control_plane_snapshot.get("duplicate_process_count"),
    "agent_health_missing_artifact_count": control_plane_snapshot.get("missing_artifact_count"),
    "agent_health_stale_pid_count": control_plane_snapshot.get("stale_pid_count"),
    "agent_health_source_commit_mismatch_count": control_plane_snapshot.get("source_commit_mismatch_count"),
    "agent_health_root_mismatch_count": control_plane_snapshot.get("root_mismatch_count"),
    "self_recover_recommendation": self_recover.get("recommendation") or self_recover.get("classification"),
    "self_recover_schema_version": control_plane_snapshot.get("self_recover_schema_version") or self_recover.get("self_recover_schema_version"),
    "recommended_recovery_action": control_plane_snapshot.get("recommended_recovery_action") or self_recover.get("recommended_recovery_action"),
    "self_recover_paper_action_policy": control_plane_snapshot.get("paper_action_policy") or self_recover.get("paper_action_policy"),
    "self_recover_autonomous_recovery_plan_classification": control_plane_snapshot.get("self_recover_autonomous_recovery_plan_classification") or self_recover.get("autonomous_recovery_plan_classification"),
    "self_recover_recovery_budget_key": control_plane_snapshot.get("recovery_budget_key") or self_recover.get("recovery_budget_key"),
    "self_recover_attempts_remaining": control_plane_snapshot.get("attempts_remaining") if control_plane_snapshot.get("attempts_remaining") is not None else self_recover.get("attempts_remaining"),
    "self_recover_cooldown_until": control_plane_snapshot.get("cooldown_until") or self_recover.get("cooldown_until"),
    "self_recover_quarantine_required": control_plane_snapshot.get("quarantine_required") is True or self_recover.get("quarantine_required") is True,
    "self_recover_execution_enabled": control_plane_snapshot.get("self_recover_execution_enabled") is True or self_recover.get("execution_enabled") is True,
    "latest_recovery_attempt_id": control_plane_snapshot.get("latest_recovery_attempt_id") or "",
    "latest_recovery_attempt_action_type": control_plane_snapshot.get("latest_recovery_attempt_action_type") or "",
    "latest_recovery_attempt_classification": control_plane_snapshot.get("latest_recovery_attempt_classification") or "",
    "recovery_attempt_recommended_recovery_action": control_plane_snapshot.get("recovery_attempt_recommended_recovery_action") or "",
    "recovery_attempt_recovery_budget_key": control_plane_snapshot.get("recovery_attempt_recovery_budget_key") or "",
    "recovery_attempt_attempts_remaining": control_plane_snapshot.get("recovery_attempt_attempts_remaining"),
    "recovery_attempt_quarantine_required": control_plane_snapshot.get("recovery_attempt_quarantine_required") is True,
    "recovery_attempt_last_success_at": control_plane_snapshot.get("recovery_attempt_last_success_at"),
    "recovery_attempt_last_failure_at": control_plane_snapshot.get("recovery_attempt_last_failure_at"),
    "recovery_attempt_history_no_history": control_plane_snapshot.get("recovery_attempt_history_no_history") is True,
    "recent_recovery_attempts": (control_plane_snapshot.get("recovery_attempt_recent_attempts") or [])[:5],
    "artifact_archive_plan_classification": control_plane_snapshot.get("artifact_archive_plan_classification") or artifact_archive_plan.get("classification") or "",
    "artifact_archive_hot_authority_protected_count": control_plane_snapshot.get("artifact_archive_hot_authority_protected_count") if control_plane_snapshot.get("artifact_archive_hot_authority_protected_count") is not None else artifact_archive_plan.get("hot_authority_protected_count"),
    "artifact_archive_active_lifecycle_protected_count": control_plane_snapshot.get("artifact_archive_active_lifecycle_protected_count") if control_plane_snapshot.get("artifact_archive_active_lifecycle_protected_count") is not None else artifact_archive_plan.get("active_lifecycle_protected_count"),
    "artifact_archive_warm_diagnostic_count": control_plane_snapshot.get("artifact_archive_warm_diagnostic_count") if control_plane_snapshot.get("artifact_archive_warm_diagnostic_count") is not None else artifact_archive_plan.get("warm_diagnostic_count"),
    "artifact_archive_cold_archive_candidate_count": control_plane_snapshot.get("artifact_archive_cold_archive_candidate_count") if control_plane_snapshot.get("artifact_archive_cold_archive_candidate_count") is not None else artifact_archive_plan.get("cold_archive_candidate_count"),
    "artifact_archive_blocked_candidate_count": control_plane_snapshot.get("artifact_archive_blocked_candidate_count") if control_plane_snapshot.get("artifact_archive_blocked_candidate_count") is not None else artifact_archive_plan.get("blocked_candidate_count"),
    "artifact_archive_estimated_bytes": control_plane_snapshot.get("artifact_archive_estimated_bytes") if control_plane_snapshot.get("artifact_archive_estimated_bytes") is not None else artifact_archive_plan.get("estimated_bytes"),
    "artifact_archive_dry_run_only": control_plane_snapshot.get("artifact_archive_dry_run_only") is True or artifact_archive_plan.get("dry_run_only") is True,
    "artifact_archive_execution_enabled": control_plane_snapshot.get("artifact_archive_execution_enabled") is True or artifact_archive_plan.get("execution_enabled") is True,
    "artifact_archive_diagnostic_only": True,
    "artifact_archive_not_routing_authority": True,
    "crash_loop_classification": crash_loop.get("classification"),
    "crash_loop_restart_blocked": crash_loop.get("restart_blocked") is True,
    "control_plane_status_classification": control_plane_status["classification"],
    "control_plane_status_reason": control_plane_status["reason"],
    "control_plane_diagnostic_only": control_plane_status["diagnostic_only"],
    "control_plane_not_routing_authority": control_plane_status["not_routing_authority"],
    "control_plane_snapshot_missing": control_plane_status["control_plane_snapshot_missing"],
    "control_plane_snapshot_stale": control_plane_status["control_plane_snapshot_stale"],
    "control_plane_snapshot_incoherent": control_plane_status["control_plane_snapshot_incoherent"],
    "control_plane_snapshot_missing_or_stale": control_plane_status["control_plane_snapshot_missing_or_stale"],
    "control_plane_snapshot_age_seconds": control_plane_status["control_plane_snapshot_age_seconds"],
    "control_plane_snapshot_safe_to_start_runtime": control_plane_status["safe_to_start_runtime"],
    "control_plane_snapshot_id": control_plane_snapshot.get("control_plane_snapshot_id"),
    "control_plane_snapshot_classification": control_plane_snapshot.get("classification"),
    "control_plane_snapshot_shared_truth_generation_id": control_plane_snapshot.get("shared_truth_refresh_generation_id"),
    "control_plane_snapshot_shared_truth_coherence_status": control_plane_snapshot.get("shared_truth_coherence_status"),
    "control_plane_snapshot_supervisor_classification": control_plane_snapshot.get("runtime_supervisor_classification"),
    "control_plane_snapshot_supervisor_mode": control_plane_snapshot.get("supervisor_mode"),
    "control_plane_snapshot_proof_window_status": control_plane_snapshot.get("proof_window_status"),
    **control_plane_top_line,
    "top_line_classification": control_plane_top_line.get("top_line_classification"),
    "top_line_status": control_plane_top_line.get("top_line_status"),
    "primary_blocking_agent_id": control_plane_top_line.get("primary_blocking_agent_id"),
    "primary_blocking_reason": control_plane_snapshot.get("primary_blocking_reason"),
    "operator_explanation": control_plane_top_line.get("operator_explanation"),
    "recommended_observation_step": control_plane_top_line.get("recommended_observation_step"),
    "prioritized_blockers": (control_plane_snapshot.get("prioritized_blockers") or paper_autonomous_recovery_plan.get("prioritized_blockers") or [])[:5],
    "paper_recovery_policy": paper_action_policy,
    "paper_recovery_severity": paper_recovery_policy.get("severity"),
    "paper_recovery_diagnostic": paper_recovery_diagnostic,
    "paper_action_policy": paper_action_policy,
    "autonomous_recovery_allowed": paper_recovery_policy.get("autonomous_recovery_allowed") is True,
    "requires_operator_ack_for_paper": requires_operator_ack_for_paper,
    "operator_ack_advisory_only_for_paper": operator_ack.get("required") is True and not requires_operator_ack_for_paper and bool(paper_action_policy),
    "bounded_recovery_budget": paper_recovery_policy.get("bounded_recovery_budget") or {},
    "live_action_policy": paper_recovery_policy.get("live_action_policy"),
    "runtime_resume_classification": runtime_resume.get("classification"),
    "runtime_resume_semantics_version": control_plane_snapshot.get("runtime_resume_semantics_version"),
    "runtime_resume_action_policy": control_plane_snapshot.get("runtime_resume_action_policy"),
    "runtime_resume_previous_runtime_generation_id": control_plane_snapshot.get("runtime_resume_previous_runtime_generation_id"),
    "runtime_resume_proposed_next_runtime_generation_id": control_plane_snapshot.get("runtime_resume_proposed_next_runtime_generation_id"),
    "runtime_resume_bounded_retry_budget_key": control_plane_snapshot.get("runtime_resume_bounded_retry_budget_key"),
    "runtime_resume_attempts_remaining": control_plane_snapshot.get("runtime_resume_attempts_remaining"),
    "runtime_resume_cooldown_until": control_plane_snapshot.get("runtime_resume_cooldown_until"),
    "runtime_resume_generation_reuse_allowed": control_plane_snapshot.get("runtime_resume_generation_reuse_allowed") is True,
    "runtime_resume_must_start_new_generation": control_plane_snapshot.get("runtime_resume_must_start_new_generation") is True,
    "runtime_resume_allowed": runtime_resume.get("allowed") is True,
    "runtime_resume_safe_to_start_runtime": (
        runtime_resume.get("safe_to_start_runtime") is True and control_plane_status["safe_to_start_runtime"] is True
    ),
    "runtime_resume_raw_safe_to_start_runtime": runtime_resume.get("safe_to_start_runtime") is True,
    "runtime_resume_diagnostic_only": control_plane_status["classification"] != "CONTROL_PLANE_READY",
    "runtime_resume_required_operator_ack": runtime_resume.get("required_operator_ack") is True,
    "runtime_resume_resume_mode": runtime_resume.get("resume_mode"),
    "runtime_resume_reason": runtime_resume.get("reason"),
    "runtime_resume_blockers": runtime_resume.get("blockers") or [],
    "runtime_resume_warnings": runtime_resume.get("warnings") or [],
    "runtime_supervisor_classification": runtime_supervisor.get("classification"),
    "runtime_supervisor_mode": runtime_supervisor.get("supervisor_mode"),
    "runtime_supervisor_proof_window_status": runtime_supervisor.get("proof_window_status"),
    "runtime_supervisor_recommended_next_command": runtime_supervisor.get("recommended_next_command"),
    "runtime_supervisor_shared_truth_refresh_generation_id": runtime_supervisor.get("shared_truth_refresh_generation_id"),
    "runtime_supervisor_shared_truth_coherence_status": runtime_supervisor.get("shared_truth_coherence_status"),
    "runtime_supervisor_stale_or_mixed_sources": (runtime_supervisor.get("stale_or_mixed_sources") or [])[:3],
    "runtime_supervisor_operator_ack_required": operator_ack.get("required") is True,
    "runtime_supervisor_operator_ack_required_for_paper": requires_operator_ack_for_paper,
    "runtime_supervisor_operator_ack": operator_ack,
    "runtime_supervisor_top_blockers": (runtime_supervisor.get("blockers") or [])[:3],
    "runtime_supervisor_top_warnings": (runtime_supervisor.get("warnings") or [])[:3],
    "runtime_supervisor_decision_precedence": (runtime_supervisor.get("decision_precedence") or [])[:3],
    "autonomous_recovery_plan_classification": runtime_supervisor.get("autonomous_recovery_plan_classification") or paper_autonomous_recovery_plan.get("classification"),
    "autonomous_recovery_next_action": runtime_supervisor.get("autonomous_recovery_next_action") or autonomous_next_action(paper_autonomous_recovery_plan),
    "autonomous_recovery_execution_enabled": False,
    "autonomous_recovery_blockers": (runtime_supervisor.get("autonomous_recovery_blockers") or paper_autonomous_recovery_plan.get("blockers") or [])[:3],
    "autonomous_recovery_budget_summary": runtime_supervisor.get("autonomous_recovery_budget_summary") or autonomous_budget_summary(paper_autonomous_recovery_plan),
    "market_closed_no_fresh_bars_expected": market_closed,
    "operator_message": "MARKET_CLOSED_WAIT: market closed/no fresh bars expected; wait and rerun proof readiness after reopen" if market_closed else None,
    "artifact_paths": authority_paths,
}
status["paper_only"] = True
status["live_money_eligible"] = False

status_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

canonical_readiness_exit_for_classification() {
  case "$1" in
    READY_SUBMIT_CAPABLE|READY_OBSERVATION_ONLY)
      return 0
      ;;
    DEGRADED_NO_SUBMIT)
      return 1
      ;;
    *)
      return 2
      ;;
  esac
}

print_canonical_readiness_summary

set +e
refresh_maintenance_supervisor
maintenance_supervisor_exit_code=$?
set -e

print_maintenance_supervisor_summary

fetch_health_snapshot() {
  local tmp_file
  tmp_file="${HEALTH_FILE}.tmp"
  rm -f "${tmp_file}"
  local attempt=1
  while (( attempt <= HEALTH_ATTEMPTS )); do
    if curl -fsS "${DASHBOARD_URL%/}/health" > "${tmp_file}"; then
      mv "${tmp_file}" "${HEALTH_FILE}"
      return 0
    fi
    attempt=$((attempt + 1))
    if (( attempt <= HEALTH_ATTEMPTS )); then
      sleep "${HEALTH_RETRY_DELAY_SECONDS}"
    fi
  done
  rm -f "${tmp_file}"
  return 1
}

if ! fetch_health_snapshot; then
  rm -f "${HEALTH_FILE}"
fi

fetch_dashboard_payload() {
  local tmp_file
  tmp_file="${DASHBOARD_PAYLOAD_FILE}.tmp"
  rm -f "${tmp_file}"
  local attempt=1
  while (( attempt <= HEALTH_ATTEMPTS )); do
    if curl -fsS "${DASHBOARD_URL%/}/api/dashboard" > "${tmp_file}"; then
      mv "${tmp_file}" "${DASHBOARD_PAYLOAD_FILE}"
      return 0
    fi
    attempt=$((attempt + 1))
    if (( attempt <= HEALTH_ATTEMPTS )); then
      sleep "${HEALTH_RETRY_DELAY_SECONDS}"
    fi
  done
  rm -f "${tmp_file}"
  return 1
}

STARTUP_FILE="${DEFAULT_STARTUP_FILE}"
OPERABILITY_FILE="${DEFAULT_OPERABILITY_FILE}"

if [[ -f "${HEALTH_FILE}" ]] && fetch_dashboard_payload; then
  "${PYTHON_BIN}" - <<'PY' "${DASHBOARD_PAYLOAD_FILE}" "${LIVE_STARTUP_FILE}" "${LIVE_OPERABILITY_FILE}"
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
startup = payload.get("startup_control_plane") or {}
operability = payload.get("supervised_paper_operability") or {}
Path(sys.argv[2]).write_text(json.dumps(startup, indent=2, sort_keys=True) + "\n", encoding="utf-8")
Path(sys.argv[3]).write_text(json.dumps(operability, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
  STARTUP_FILE="${LIVE_STARTUP_FILE}"
  OPERABILITY_FILE="${LIVE_OPERABILITY_FILE}"
fi

"${PYTHON_BIN}" -m mgc_v05l.app.main headless-supervised-paper-status \
  --health-file "${HEALTH_FILE}" \
  --startup-control-plane-file "${STARTUP_FILE}" \
  --supervised-operability-file "${OPERABILITY_FILE}" \
  --dashboard-info-file "${DEFAULT_INFO_FILE}" \
  --output "${STATUS_FILE}" \
  --markdown-output "${MARKDOWN_FILE}" >/dev/null

merge_canonical_readiness_status
merge_maintenance_supervisor_status
merge_paper_runtime_truth_status
merge_paper_runtime_generation_status
merge_control_plane_services_status
cat "${STATUS_FILE}"
canonical_state="$(canonical_readiness_classification)"
canonical_readiness_exit_for_classification "${canonical_state}"
