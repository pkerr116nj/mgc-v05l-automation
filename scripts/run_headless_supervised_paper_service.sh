#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

DEFAULT_RUNTIME_DIR="${REPO_ROOT}/outputs/operator_dashboard/runtime"
DEFAULT_STATUS_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_status.json"
DEFAULT_MARKDOWN_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_status.md"
DEFAULT_CANONICAL_READINESS_FILE="${DEFAULT_RUNTIME_DIR}/latest_canonical_readiness.json"
DEFAULT_CANONICAL_READINESS_SUMMARY_FILE="${DEFAULT_RUNTIME_DIR}/latest_canonical_readiness_summary.json"
DEFAULT_STARTUP_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_service_startup.json"
DEFAULT_REQUESTED_CONFIG_PATHS_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_requested_config_paths.txt"
DEFAULT_MANAGER_PID_FILE="${DEFAULT_RUNTIME_DIR}/operator_dashboard_manager.pid"
DEFAULT_MANAGER_LOG_FILE="${DEFAULT_RUNTIME_DIR}/operator_dashboard_manager.log"
DEFAULT_DASHBOARD_PID_FILE="${DEFAULT_RUNTIME_DIR}/operator_dashboard.pid"
DEFAULT_PAPER_PID_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid"
DEFAULT_PAPER_PID_METADATA_FILE="${DEFAULT_PAPER_PID_FILE}.json"
DEFAULT_PAPER_RUNTIME_TRUTH_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_truth.json"
DEFAULT_PAPER_CONFIG_IN_FORCE_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json"
DEFAULT_PAPER_OPERATOR_STATUS_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/operator_status.json"
DEFAULT_PAPER_RECONCILIATION_FILE="${REPO_ROOT}/outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
DEFAULT_PAPER_LAUNCH_GUARD_FILE="${DEFAULT_PAPER_PID_METADATA_FILE}.launch_guard.json"
DEFAULT_PAPER_WRAPPER_PID_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.wrapper.pid"
DEFAULT_PAPER_WRAPPER_STATUS_FILE="${DEFAULT_PAPER_PID_FILE}.wrapper_status.json"
DEFAULT_PAPER_LOG_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.log"
DEFAULT_PAPER_CONFIG_PATHS_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/paper_runtime_config_paths.txt"
DEFAULT_PAPER_RUNTIME_LAUNCH_STATUS_FILE="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper_launch_status.json"
DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_FILE="${REPO_ROOT}/outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json"
DEFAULT_CONTROL_PLANE_SNAPSHOT_FILE="${REPO_ROOT}/outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"
DEFAULT_PAPER_LAUNCHCTL_STDOUT_FILE="${DEFAULT_PAPER_PID_FILE}.launchctl_submit.stdout"
DEFAULT_PAPER_LAUNCHCTL_STDERR_FILE="${DEFAULT_PAPER_PID_FILE}.launchctl_submit.stderr"
DEFAULT_DASHBOARD_URL="${MGC_OPERATOR_DASHBOARD_URL:-http://127.0.0.1:8790/}"
SERVICE_HOST_AUTOSTART_BRIDGE_SUPERVISOR="${MGC_SERVICE_HOST_AUTOSTART_RESEARCH_RUNTIME_BRIDGE_SUPERVISOR:-1}"
DEFAULT_HEADLESS_PAPER_CONFIG_PATHS=(
  "${REPO_ROOT}/config/base.yaml"
  "${REPO_ROOT}/config/live.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine.yaml"
  "${REPO_ROOT}/config/headless_supervised_paper_runtime.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper.yaml"
)

WAIT_TIMEOUT_SECONDS=120
POLL_INTERVAL_SECONDS=3
POST_START_PID_WAIT_TIMEOUT_SECONDS="${MGC_HEADLESS_POST_START_PID_WAIT_TIMEOUT_SECONDS:-30}"
STATUS_FILE="${DEFAULT_STATUS_FILE}"
MARKDOWN_FILE="${DEFAULT_MARKDOWN_FILE}"
CANONICAL_READINESS_FILE="${DEFAULT_CANONICAL_READINESS_FILE}"
CANONICAL_READINESS_SUMMARY_FILE="${DEFAULT_CANONICAL_READINESS_SUMMARY_FILE}"
STARTUP_FILE="${DEFAULT_STARTUP_FILE}"
REQUESTED_CONFIG_PATHS_FILE="${DEFAULT_REQUESTED_CONFIG_PATHS_FILE}"
MANAGER_PID_FILE="${DEFAULT_MANAGER_PID_FILE}"
MANAGER_LOG_FILE="${DEFAULT_MANAGER_LOG_FILE}"
DASHBOARD_PID_FILE="${DEFAULT_DASHBOARD_PID_FILE}"
PAPER_PID_FILE="${DEFAULT_PAPER_PID_FILE}"
PAPER_PID_METADATA_FILE="${DEFAULT_PAPER_PID_METADATA_FILE}"
PAPER_RUNTIME_TRUTH_FILE="${DEFAULT_PAPER_RUNTIME_TRUTH_FILE}"
PAPER_CONFIG_IN_FORCE_FILE="${DEFAULT_PAPER_CONFIG_IN_FORCE_FILE}"
PAPER_OPERATOR_STATUS_FILE="${DEFAULT_PAPER_OPERATOR_STATUS_FILE}"
PAPER_RECONCILIATION_FILE="${DEFAULT_PAPER_RECONCILIATION_FILE}"
PAPER_LAUNCH_GUARD_FILE="${DEFAULT_PAPER_LAUNCH_GUARD_FILE}"
PAPER_WRAPPER_PID_FILE="${DEFAULT_PAPER_WRAPPER_PID_FILE}"
PAPER_WRAPPER_STATUS_FILE="${DEFAULT_PAPER_WRAPPER_STATUS_FILE}"
PAPER_LOG_FILE="${DEFAULT_PAPER_LOG_FILE}"
PAPER_CONFIG_PATHS_FILE="${DEFAULT_PAPER_CONFIG_PATHS_FILE}"
PAPER_RUNTIME_LAUNCH_STATUS_FILE="${DEFAULT_PAPER_RUNTIME_LAUNCH_STATUS_FILE}"
RUNTIME_SUPERVISOR_AUTHORITY_FILE="${DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_FILE}"
CONTROL_PLANE_SNAPSHOT_FILE="${DEFAULT_CONTROL_PLANE_SNAPSHOT_FILE}"
PAPER_LAUNCHCTL_STDOUT_FILE="${DEFAULT_PAPER_LAUNCHCTL_STDOUT_FILE}"
PAPER_LAUNCHCTL_STDERR_FILE="${DEFAULT_PAPER_LAUNCHCTL_STDERR_FILE}"
DASHBOARD_URL="${DEFAULT_DASHBOARD_URL}"
START_PAPER=1
START_DASHBOARD=1
BROKER_TRUTH_REFRESH_PROFILE="${MGC_HEADLESS_BROKER_TRUTH_REFRESH_PROFILE:-${MGC_OPERATIONAL_PROFILE:-headless_paper}}"
STRICT_BROKER_TRUTH_REFRESH="${MGC_HEADLESS_STRICT_BROKER_TRUTH_REFRESH:-0}"
START_BROKER_TRUTH_REFRESH="${MGC_HEADLESS_START_BROKER_TRUTH_REFRESH:-}"
PAPER_RUNTIME_LAUNCH_METHOD="${MGC_HEADLESS_PAPER_LAUNCH_METHOD:-direct}"
DEFAULT_START_BROKER_TRUTH_REFRESH=1
case "$(printf '%s' "${BROKER_TRUTH_REFRESH_PROFILE}" | tr '[:upper:]' '[:lower:]')" in
  dev|development|test|local)
    DEFAULT_START_BROKER_TRUTH_REFRESH=0
    ;;
esac
if [[ -z "${START_BROKER_TRUTH_REFRESH}" ]]; then
  START_BROKER_TRUTH_REFRESH="${DEFAULT_START_BROKER_TRUTH_REFRESH}"
fi

HEADLESS_PAPER_CONFIG_PATHS="${MGC_HEADLESS_SUPERVISED_PAPER_CONFIG_PATHS:-${MGC_PROBATIONARY_PAPER_CONFIG_PATHS:-}}"
if [[ -z "${HEADLESS_PAPER_CONFIG_PATHS}" ]]; then
  HEADLESS_PAPER_CONFIG_PATHS="$(IFS=:; printf '%s' "${DEFAULT_HEADLESS_PAPER_CONFIG_PATHS[*]}")"
fi
REQUIRED_PAPER_CONFIG_PATHS="${MGC_HEADLESS_REQUIRED_PAPER_CONFIGS:-${MGC_HEADLESS_REQUIRED_PAPER_CONFIG_PATHS:-}}"

while (($# > 0)); do
  case "$1" in
    --wait-timeout-seconds)
      WAIT_TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    --wait-timeout-seconds=*)
      WAIT_TIMEOUT_SECONDS="${1#*=}"
      shift
      ;;
    --poll-interval-seconds)
      POLL_INTERVAL_SECONDS="$2"
      shift 2
      ;;
    --poll-interval-seconds=*)
      POLL_INTERVAL_SECONDS="${1#*=}"
      shift
      ;;
    --post-start-pid-wait-timeout-seconds)
      POST_START_PID_WAIT_TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    --post-start-pid-wait-timeout-seconds=*)
      POST_START_PID_WAIT_TIMEOUT_SECONDS="${1#*=}"
      shift
      ;;
    --status-output)
      STATUS_FILE="$2"
      shift 2
      ;;
    --status-output=*)
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
    --startup-output)
      STARTUP_FILE="$2"
      shift 2
      ;;
    --startup-output=*)
      STARTUP_FILE="${1#*=}"
      shift
      ;;
    --requested-config-paths-output)
      REQUESTED_CONFIG_PATHS_FILE="$2"
      shift 2
      ;;
    --requested-config-paths-output=*)
      REQUESTED_CONFIG_PATHS_FILE="${1#*=}"
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
    --dashboard-pid-file)
      DASHBOARD_PID_FILE="$2"
      shift 2
      ;;
    --dashboard-pid-file=*)
      DASHBOARD_PID_FILE="${1#*=}"
      shift
      ;;
    --paper-config-paths-file)
      PAPER_CONFIG_PATHS_FILE="$2"
      shift 2
      ;;
    --paper-config-paths-file=*)
      PAPER_CONFIG_PATHS_FILE="${1#*=}"
      shift
      ;;
    --no-start-paper)
      START_PAPER=0
      shift
      ;;
    --no-start-dashboard)
      START_DASHBOARD=0
      shift
      ;;
    --start-broker-truth-refresh)
      START_BROKER_TRUTH_REFRESH=1
      shift
      ;;
    --no-start-broker-truth-refresh)
      START_BROKER_TRUTH_REFRESH=0
      shift
      ;;
    --strict-broker-truth-refresh)
      STRICT_BROKER_TRUTH_REFRESH=1
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
ensure_dir "$(dirname "${CANONICAL_READINESS_FILE}")"
ensure_dir "$(dirname "${CANONICAL_READINESS_SUMMARY_FILE}")"
ensure_dir "$(dirname "${STARTUP_FILE}")"
ensure_dir "$(dirname "${REQUESTED_CONFIG_PATHS_FILE}")"
ensure_dir "$(dirname "${MANAGER_PID_FILE}")"
ensure_dir "$(dirname "${MANAGER_LOG_FILE}")"
ensure_dir "$(dirname "${DASHBOARD_PID_FILE}")"
ensure_dir "$(dirname "${PAPER_LOG_FILE}")"
ensure_dir "$(dirname "${PAPER_PID_METADATA_FILE}")"
ensure_dir "$(dirname "${PAPER_LAUNCH_GUARD_FILE}")"
ensure_dir "$(dirname "${PAPER_WRAPPER_PID_FILE}")"
ensure_dir "$(dirname "${PAPER_CONFIG_PATHS_FILE}")"

read_pid_file() {
  local pid_file="$1"
  if [[ ! -f "${pid_file}" ]]; then
    return 1
  fi
  tr -d '[:space:]' < "${pid_file}" 2>/dev/null
}

truthy_flag() {
  case "$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|y|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

pid_file_alive() {
  local pid_file="$1"
  local pid
  pid="$(read_pid_file "${pid_file}" || true)"
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

split_config_paths() {
  local raw="$1"
  "${PYTHON_BIN}" - <<'PY' "${REPO_ROOT}" "${raw}"
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
raw = sys.argv[2]
for item in raw.replace(",", ":").split(":"):
    item = item.strip()
    if not item:
        continue
    path = Path(item)
    if not path.is_absolute():
        path = repo_root / path
    print(path.resolve())
PY
}

persist_requested_config_paths() {
  split_config_paths "${HEADLESS_PAPER_CONFIG_PATHS}" > "${REQUESTED_CONFIG_PATHS_FILE}"
  cp "${REQUESTED_CONFIG_PATHS_FILE}" "${PAPER_CONFIG_PATHS_FILE}"
}

requested_config_paths_arg() {
  "${PYTHON_BIN}" - <<'PY' "${REQUESTED_CONFIG_PATHS_FILE}"
import sys
from pathlib import Path

path = Path(sys.argv[1])
try:
    rows = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
except OSError:
    rows = []
print(":".join(rows))
PY
}

requested_config_fingerprint() {
  "${PYTHON_BIN}" - <<'PY' "${REQUESTED_CONFIG_PATHS_FILE}"
import hashlib
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
try:
    rows = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
except OSError:
    rows = []
digest = hashlib.sha256(json.dumps(rows, separators=(",", ":"), sort_keys=True).encode("utf-8")).hexdigest()
print(f"sha256:{digest}")
PY
}

resolved_config_paths_arg() {
  local raw="$1"
  "${PYTHON_BIN}" - <<'PY' "${REPO_ROOT}" "${raw}"
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
raw = sys.argv[2]
rows = []
for item in raw.replace(",", ":").split(":"):
    item = item.strip()
    if not item:
        continue
    path = Path(item)
    if not path.is_absolute():
        path = repo_root / path
    rows.append(str(path.resolve()))
print(":".join(rows))
PY
}

assert_required_config_paths_present() {
  local required_raw="$1"
  if [[ -z "${required_raw}" ]]; then
    return 0
  fi
  "${PYTHON_BIN}" - <<'PY' "${REPO_ROOT}" "${REQUESTED_CONFIG_PATHS_FILE}" "${required_raw}"
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
requested_file = Path(sys.argv[2])
required_raw = sys.argv[3]
requested = {Path(line.strip()).resolve() for line in requested_file.read_text(encoding="utf-8").splitlines() if line.strip()}
missing = []
for item in required_raw.replace(",", ":").split(":"):
    item = item.strip()
    if not item:
        continue
    path = Path(item)
    if not path.is_absolute():
        path = repo_root / path
    if path.resolve() not in requested:
        missing.append(str(path.resolve()))
if missing:
    print("Missing required paper config path(s): " + ", ".join(missing), file=sys.stderr)
    raise SystemExit(1)
PY
}

assert_requested_config_stack_safe() {
  "${PYTHON_BIN}" - <<'PY' "${REPO_ROOT}" "${REQUESTED_CONFIG_PATHS_FILE}"
import json
import sys
from pathlib import Path

from mgc_v05l.execution_core.track_b_runtime_truth_contract import classify_paper_config_stack_safety

repo_root = Path(sys.argv[1]).resolve()
requested_file = Path(sys.argv[2])
try:
    rows = [line.strip() for line in requested_file.read_text(encoding="utf-8").splitlines() if line.strip()]
except OSError:
    rows = []
classification = classify_paper_config_stack_safety(rows, expected_root=str(repo_root))
if not classification["launch_allowed"]:
    print("Unsafe PAPER runtime config stack: " + json.dumps(classification, sort_keys=True), file=sys.stderr)
    raise SystemExit(2)
PY
}

assert_runtime_config_paths_match_request() {
  local phase="$1"
  if [[ "${START_PAPER}" -ne 1 ]]; then
    return 0
  fi
  local pid
  local wrapper_pid
  pid="$(read_pid_file "${PAPER_PID_FILE}" || true)"
  if [[ -z "${pid}" ]] || ! kill -0 "${pid}" 2>/dev/null; then
    wrapper_pid="$(read_pid_file "${PAPER_WRAPPER_PID_FILE}" || true)"
    if [[ -n "${wrapper_pid}" ]] && ! kill -0 "${wrapper_pid}" 2>/dev/null; then
      echo "RUNTIME_EXITED_BEFORE_PID: Paper runtime wrapper exited before a valid Python runtime PID became available during ${phase}." >&2
      return 3
    fi
    echo "RUNTIME_PID_UNAVAILABLE: Paper runtime PID is unavailable during ${phase}." >&2
    return 1
  fi
  "${PYTHON_BIN}" - <<'PY' "${pid}" "${REQUESTED_CONFIG_PATHS_FILE}" "${phase}" "${REPO_ROOT}"
import subprocess
import sys
from pathlib import Path

pid = sys.argv[1]
requested_file = Path(sys.argv[2])
phase = sys.argv[3]
repo_root = Path(sys.argv[4]).resolve()
requested = [line.strip() for line in requested_file.read_text(encoding="utf-8").splitlines() if line.strip()]
try:
    command = subprocess.check_output(["ps", "-p", pid, "-o", "command="], text=True).strip()
except (OSError, subprocess.CalledProcessError) as exc:
    print(f"Unable to inspect paper runtime command during {phase}: {exc}", file=sys.stderr)
    raise SystemExit(2)
runtime_markers = ("mgc_v05l.app.main", "probationary-paper-soak")
if not all(marker in command for marker in runtime_markers):
    print(
        f"RUNTIME_PID_PENDING: PID {pid} is alive during {phase} but has not execed the Python paper runtime yet.",
        file=sys.stderr,
    )
    print(f"Active command: {command}", file=sys.stderr)
    raise SystemExit(1)
try:
    cwd_output = subprocess.check_output(["lsof", "-a", "-p", pid, "-d", "cwd", "-Fn"], text=True).strip()
except (OSError, subprocess.CalledProcessError) as exc:
    print(f"Unable to inspect paper runtime cwd during {phase}: {exc}", file=sys.stderr)
    raise SystemExit(2)
cwd_rows = [line[1:] for line in cwd_output.splitlines() if line.startswith("n")]
cwd = Path(cwd_rows[-1]).resolve() if cwd_rows else None
if cwd != repo_root:
    print(
        f"Paper runtime root mismatch during {phase}; expected {repo_root}, observed {cwd}",
        file=sys.stderr,
    )
    print(f"Active command: {command}", file=sys.stderr)
    raise SystemExit(2)
missing = [path for path in requested if path not in command]
if missing:
    print(
        f"Paper runtime config path mismatch during {phase}; missing from active command: "
        + ", ".join(missing),
        file=sys.stderr,
    )
    print(f"Active command: {command}", file=sys.stderr)
    raise SystemExit(2)
PY
}

wait_for_runtime_config_paths_match_request() {
  local phase="$1"
  local timeout_seconds="$2"
  local deadline=$((SECONDS + timeout_seconds))
  local rc=1
  if [[ "${START_PAPER}" -ne 1 ]]; then
    return 0
  fi
  while (( SECONDS <= deadline )); do
    set +e
    assert_runtime_config_paths_match_request "${phase}"
    rc=$?
    set -e
    case "${rc}" in
      0)
        return 0
        ;;
      2)
        return 2
        ;;
      3)
        return 3
        ;;
    esac
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep "${POLL_INTERVAL_SECONDS}"
  done
  echo "RUNTIME_PID_UNAVAILABLE: Paper runtime PID did not become available during ${phase} within ${timeout_seconds}s." >&2
  return 1
}

classify_paper_runtime_launch_guard() {
  "${PYTHON_BIN}" - <<'PY' \
    "${PAPER_LAUNCH_GUARD_FILE}" \
    "${PAPER_PID_METADATA_FILE}" \
    "${PAPER_RUNTIME_TRUTH_FILE}" \
    "${PAPER_CONFIG_IN_FORCE_FILE}" \
    "${PAPER_OPERATOR_STATUS_FILE}" \
    "${PAPER_RECONCILIATION_FILE}" \
    "${PAPER_PID_FILE}" \
    "$(requested_config_fingerprint)" \
    "${REPO_ROOT}"
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_runtime_truth_contract import (
    classify_launchctl_runtime_jobs,
    classify_pid_metadata,
    classify_runtime_launch_guard,
)

(
    guard_path,
    metadata_path,
    truth_path,
    config_path,
    operator_path,
    reconciliation_path,
    pid_path,
    expected_config_fingerprint,
    expected_root,
) = sys.argv[1:]
expected_root = str(Path(expected_root).resolve())

def read_json(raw_path: str) -> dict:
    path = Path(raw_path)
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
            proc = subprocess.run(
                ["ps", "-p", str(pid_int), "-o", "stat=", "-o", "command="],
                check=False,
                capture_output=True,
                text=True,
                timeout=2,
            )
            line = proc.stdout.strip()
            if line:
                parts = line.split(maxsplit=1)
                stat = parts[0]
                command = parts[1] if len(parts) > 1 else ""
        except (OSError, subprocess.SubprocessError):
            pass
        try:
            proc = subprocess.run(
                ["lsof", "-a", "-p", str(pid_int), "-d", "cwd", "-Fn"],
                check=False,
                capture_output=True,
                text=True,
                timeout=2,
            )
            rows = [row[1:] for row in proc.stdout.splitlines() if row.startswith("n")]
            cwd = str(Path(rows[-1]).resolve()) if rows else None
        except (OSError, subprocess.SubprocessError):
            cwd = None
    return {"running": running, "zombie": stat.startswith("Z"), "cwd": cwd, "command": command}

pid_metadata = read_json(metadata_path)
runtime_truth = read_json(truth_path)
config_in_force = read_json(config_path)
operator_status = read_json(operator_path)
reconciliation = read_json(reconciliation_path)
label_path = Path(f"{pid_path}.launchctl_label")
try:
    expected_launchctl_label = label_path.read_text(encoding="utf-8").strip()
except OSError:
    expected_launchctl_label = ""

def launchctl_runtime_jobs() -> list[dict]:
    try:
        proc = subprocess.run(
            ["launchctl", "list"],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    jobs = []
    for line in proc.stdout.splitlines():
        parts = line.split()
        if len(parts) < 3 or parts[0] == "PID":
            continue
        jobs.append({"pid": parts[0], "status": parts[1], "label": parts[2]})
    return jobs

launchctl_jobs = classify_launchctl_runtime_jobs(
    launchctl_runtime_jobs(),
    expected_label=expected_launchctl_label,
)
probe = process_probe(pid_metadata.get("pid"))
pid_metadata_state = classify_pid_metadata(
    pid_metadata,
    now=datetime.now(timezone.utc),
    freshness_ttl_seconds=float(pid_metadata.get("freshness_ttl_seconds") or 180.0),
    process_probe=probe,
    expected_root=expected_root,
)
broker_clean = str(reconciliation.get("classification") or "") == "TRACK_B_PAPER_BROKER_RECONCILED"
runtime_truth_duplicate = bool((runtime_truth.get("duplicate_writer_detection") or {}).get("duplicate_writer_detected"))
identity_rows = [pid_metadata, runtime_truth, config_in_force, operator_status]
runtime_instance_ids = {
    str(row.get("runtime_instance_id"))
    for row in identity_rows
    if row.get("runtime_instance_id")
}
duplicate_writer_detected = runtime_truth_duplicate or (probe.get("running") is True and len(runtime_instance_ids) > 1)
if launchctl_jobs.get("classification") == "LAUNCHCTL_STALE_RUNTIME_JOB_BLOCKED":
    duplicate_writer_detected = True
try:
    current_commit = subprocess.check_output(["git", "-C", expected_root, "rev-parse", "HEAD"], text=True, timeout=3).strip()
except (OSError, subprocess.SubprocessError):
    current_commit = ""
guard = classify_runtime_launch_guard(
    pid_metadata_state=pid_metadata_state,
    pid_metadata=pid_metadata,
    runtime_truth=runtime_truth,
    config_in_force=config_in_force,
    operator_status=operator_status,
    broker_clean=broker_clean,
    process_running=probe.get("running"),
    duplicate_writer_detected=duplicate_writer_detected,
)
stale_source_commit = bool(current_commit and pid_metadata.get("source_commit") and pid_metadata.get("source_commit") != current_commit)
stale_config_fingerprint = bool(
    expected_config_fingerprint
    and pid_metadata.get("config_fingerprint")
    and pid_metadata.get("config_fingerprint") != expected_config_fingerprint
)
if launchctl_jobs.get("classification") == "LAUNCHCTL_STALE_RUNTIME_JOB_BLOCKED":
    guard["classification"] = "LAUNCH_CONFLICTING_WRITER_BLOCKED"
    guard["launch_allowed"] = False
    guard["cleanup_allowed"] = False
    guard["active_runtime_accepted"] = False
    guard.setdefault("blockers", []).extend(launchctl_jobs.get("blockers") or [])
if probe.get("running") is True and (stale_source_commit or stale_config_fingerprint):
    guard["classification"] = "LAUNCH_CONFLICTING_WRITER_BLOCKED"
    guard["launch_allowed"] = False
    guard["cleanup_allowed"] = False
    guard["active_runtime_accepted"] = False
    if stale_source_commit:
        guard.setdefault("blockers", []).append("runtime_source_commit_mismatch")
    if stale_config_fingerprint:
        guard.setdefault("blockers", []).append("runtime_config_fingerprint_mismatch")
guard.update(
    {
        "schema_version": "track_b_paper_runtime_launch_guard_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "evidence_only_for_readiness": True,
        "restart_authority": False,
        "readiness_authority": False,
        "paper_only": True,
        "live_money_eligible": False,
        "pid_metadata_artifact": metadata_path,
        "runtime_truth_artifact": truth_path,
        "config_in_force_artifact": config_path,
        "operator_status_artifact": operator_path,
        "reconciliation_artifact": reconciliation_path,
        "process_probe": probe,
        "reconciliation_classification": reconciliation.get("classification"),
        "launchctl_runtime_jobs": launchctl_jobs,
        "stale_launchctl_runtime_job_count": len(launchctl_jobs.get("stale_runtime_labels") or []),
        "expected_source_commit": current_commit,
        "source_commit_mismatch": stale_source_commit,
        "expected_config_fingerprint": expected_config_fingerprint,
        "config_fingerprint_mismatch": stale_config_fingerprint,
    }
)
path = Path(guard_path)
path.parent.mkdir(parents=True, exist_ok=True)
tmp = path.with_name(f".{path.name}.tmp")
tmp.write_text(json.dumps(guard, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(path)
print(guard["classification"])
PY
}

launch_guard_field() {
  local field="$1"
  "${PYTHON_BIN}" - <<'PY' "${PAPER_LAUNCH_GUARD_FILE}" "${field}"
import json
import sys
from pathlib import Path

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
value = payload.get(sys.argv[2])
if isinstance(value, bool):
    print("true" if value else "false")
elif value is None:
    print("")
else:
    print(value)
PY
}

cleanup_stale_paper_pid_metadata_if_allowed() {
  if [[ "$(launch_guard_field "cleanup_allowed")" != "true" ]]; then
    return 1
  fi
  rm -f "${PAPER_PID_FILE}" "${PAPER_PID_METADATA_FILE}"
}

launchctl_submit_available() {
  command -v launchctl >/dev/null 2>&1
}

screen_available() {
  command -v screen >/dev/null 2>&1
}

screen_session_name() {
  local suffix="$1"
  printf 'mgc_%s_%s_%s' "${suffix}" "$(date +%Y%m%d%H%M%S)" "$$"
}

prepare_paper_runtime_generation() {
  PAPER_RUNTIME_LAUNCH_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  PAPER_RUNTIME_INSTANCE_ID="track-b-paper-runtime-$(date -u +%Y%m%dT%H%M%SZ)-$$"
  PAPER_RUNTIME_EXPECTED_SOURCE_COMMIT="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
  PAPER_RUNTIME_RESTART_GENERATION="$(
    "${PYTHON_BIN}" -c 'import json, sys; from pathlib import Path; path=Path(sys.argv[1]);
try:
    payload=json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload={}
try:
    generation=int(payload.get("restart_generation") or 0)+1
except (TypeError, ValueError):
    generation=1
print(max(generation, 1))' "${PAPER_PID_METADATA_FILE}"
  )"
  PAPER_RUNTIME_CONFIG_FINGERPRINT="$(requested_config_fingerprint)"
  "${PYTHON_BIN}" - <<'PY' \
    "${PAPER_PID_METADATA_FILE}" \
    "${PAPER_RUNTIME_INSTANCE_ID}" \
    "${PAPER_RUNTIME_RESTART_GENERATION}" \
    "${PAPER_RUNTIME_LAUNCH_STARTED_AT}" \
    "$$" \
    "${REPO_ROOT}" \
    "${PAPER_RUNTIME_CONFIG_FINGERPRINT}" \
    "${REQUESTED_CONFIG_PATHS_FILE}"
import json
import subprocess
import sys
from pathlib import Path

(
    metadata_path,
    runtime_instance_id,
    restart_generation,
    launch_started_at,
    launcher_pid,
    repo_root,
    config_fingerprint,
    requested_config_paths_file,
) = sys.argv[1:]
root = Path(repo_root).resolve()
try:
    source_commit = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
except (OSError, subprocess.CalledProcessError):
    source_commit = None
try:
    config_paths = [
        line.strip()
        for line in Path(requested_config_paths_file).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
except OSError:
    config_paths = []
payload = {
    "schema_version": "track_b_paper_runtime_pid_metadata_v1",
    "service_name": "track_b_paper_runtime",
    "pid": None,
    "runtime_instance_id": runtime_instance_id,
    "restart_generation": int(restart_generation),
    "launch_started_at": launch_started_at,
    "generated_at": launch_started_at,
    "launcher_pid": int(launcher_pid),
    "expected_project_root": str(root),
    "root": str(root),
    "source_commit": source_commit,
    "config_fingerprint": config_fingerprint,
    "launcher_config_fingerprint": config_fingerprint,
    "requested_config_paths": config_paths,
    "runtime_mode": "PAPER",
    "paper_only": True,
    "live_money_eligible": False,
    "submit_authority": False,
    "readiness_authority": False,
    "restart_authority": False,
}
path = Path(metadata_path)
path.parent.mkdir(parents=True, exist_ok=True)
tmp = path.with_name(f".{path.name}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(path)
PY
}

write_launchctl_runtime_status() {
  local classification="$1"
  local launchctl_exit_code="${2:-}"
  local pid_available="${3:-false}"
  local detail="${4:-}"
  local wrapper_path="${PAPER_PID_FILE}.runtime_wrapper.sh"
  local label=""
  if [[ -f "${PAPER_PID_FILE}.launchctl_label" ]]; then
    label="$(<"${PAPER_PID_FILE}.launchctl_label")"
  fi
  "${PYTHON_BIN}" - <<'PY' \
    "${PAPER_RUNTIME_LAUNCH_STATUS_FILE}" \
    "${classification}" \
    "${launchctl_exit_code}" \
    "${pid_available}" \
    "${detail}" \
    "${label}" \
    "${wrapper_path}" \
    "${PAPER_PID_FILE}" \
    "${PAPER_PID_METADATA_FILE}" \
    "${PAPER_WRAPPER_STATUS_FILE}" \
    "${PAPER_LAUNCHCTL_STDOUT_FILE}" \
    "${PAPER_LAUNCHCTL_STDERR_FILE}" \
    "${REPO_ROOT}" \
    "${PAPER_RUNTIME_INSTANCE_ID:-}" \
    "${PAPER_RUNTIME_RESTART_GENERATION:-}" \
    "${PAPER_RUNTIME_LAUNCH_STARTED_AT:-}" \
    "${PAPER_RUNTIME_EXPECTED_SOURCE_COMMIT:-}" \
    "${PAPER_RUNTIME_CONFIG_FINGERPRINT:-}" \
    "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" \
    "${CONTROL_PLANE_SNAPSHOT_FILE}"
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_runtime_truth_contract import classify_launchctl_start_attempt

(
    status_path,
    classification,
    launchctl_exit_code_raw,
    pid_available_raw,
    detail,
    label,
    wrapper_path,
    pid_file,
    pid_metadata_file,
    wrapper_status_file,
    stdout_file,
    stderr_file,
    repo_root,
    runtime_instance_id,
    restart_generation_raw,
    launch_started_at,
    expected_source_commit,
    config_fingerprint,
    runtime_supervisor_authority_path,
    control_plane_snapshot_path,
) = sys.argv[1:]

def read_text(path_raw: str) -> str:
    try:
        return Path(path_raw).read_text(encoding="utf-8")
    except OSError:
        return ""

def read_json(path_raw: str) -> dict:
    try:
        payload = json.loads(Path(path_raw).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}

try:
    launchctl_exit_code = int(launchctl_exit_code_raw) if launchctl_exit_code_raw != "" else None
except ValueError:
    launchctl_exit_code = None
pid_available = pid_available_raw.lower() == "true"
wrapper_status = read_json(wrapper_status_file)
attempt = classify_launchctl_start_attempt(
    launchctl_exit_code=launchctl_exit_code,
    pid_available=pid_available,
    wrapper_status=wrapper_status,
    expected_pid_file=pid_file,
)
if classification:
    attempt["classification"] = classification
try:
    restart_generation = int(restart_generation_raw) if restart_generation_raw else None
except ValueError:
    restart_generation = None
payload = {
    "schema_version": "track_b_paper_launchctl_runtime_status_v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "classification": attempt["classification"],
    "attempt_classification": attempt,
    "detail": detail or None,
    "runtime_instance_id": runtime_instance_id or None,
    "restart_generation": restart_generation,
    "launch_started_at": launch_started_at or None,
    "launchctl_label": label or None,
    "launchctl_command": [
        "launchctl",
        "submit",
        "-l",
        label,
        "-o",
        stdout_file,
        "-e",
        stderr_file,
        "--",
        "/bin/bash",
        wrapper_path,
    ]
    if label
    else None,
    "wrapper_path": wrapper_path,
    "pid_file": pid_file,
    "pid_metadata_file": pid_metadata_file,
    "wrapper_status_file": wrapper_status_file,
    "launchctl_stdout_file": stdout_file,
    "launchctl_stderr_file": stderr_file,
    "launchctl_exit_code": launchctl_exit_code,
    "launchctl_stdout": read_text(stdout_file).strip(),
    "launchctl_stderr": read_text(stderr_file).strip(),
    "wrapper_status": wrapper_status,
    "pid_metadata": read_json(pid_metadata_file),
    "repo_root": repo_root,
    "expected_source_commit": expected_source_commit or None,
    "config_fingerprint": config_fingerprint or None,
    "runtime_supervisor_authority": read_json(runtime_supervisor_authority_path),
    "runtime_supervisor_authority_path": runtime_supervisor_authority_path,
    "control_plane_snapshot": read_json(control_plane_snapshot_path),
    "control_plane_snapshot_path": control_plane_snapshot_path,
    "paper_only": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "broker_mutation": False,
    "submit_authority": False,
    "readiness_authority": False,
    "restart_authority": False,
}
path = Path(status_path)
path.parent.mkdir(parents=True, exist_ok=True)
tmp = path.with_name(f".{path.name}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(path)
PY
}

write_paper_runtime_wrapper() {
  local wrapper_path="${PAPER_PID_FILE}.runtime_wrapper.sh"
  local requested_stack
  local required_stack
  requested_stack="$(requested_config_paths_arg)"
  required_stack="$(resolved_config_paths_arg "${REQUIRED_PAPER_CONFIG_PATHS}")"
  "${PYTHON_BIN}" - <<'PY' \
    "${wrapper_path}" \
    "${REPO_ROOT}" \
    "${PYTHON_BIN}" \
    "${SCRIPT_DIR}" \
    "${PAPER_PID_FILE}" \
    "${PAPER_PID_METADATA_FILE}" \
    "${PAPER_WRAPPER_STATUS_FILE}" \
    "${PAPER_LOG_FILE}" \
    "${requested_stack}" \
    "${required_stack}" \
    "${PAPER_RUNTIME_INSTANCE_ID}" \
    "${PAPER_RUNTIME_RESTART_GENERATION}" \
    "${PAPER_RUNTIME_LAUNCH_STARTED_AT}" \
    "$$" \
    "${PAPER_RUNTIME_CONFIG_FINGERPRINT}" \
    "${PAPER_RUNTIME_EXPECTED_SOURCE_COMMIT}"
import shlex
import sys
from pathlib import Path

(
    wrapper_path,
    repo_root,
    python_bin,
    script_dir,
    pid_file,
    pid_metadata_file,
    wrapper_status_file,
    log_file,
    requested_stack,
    required_stack,
    runtime_instance_id,
    restart_generation,
    launch_started_at,
    launcher_pid,
    config_fingerprint,
    expected_source_commit,
) = sys.argv[1:]

q = shlex.quote
path = Path(wrapper_path)
path.parent.mkdir(parents=True, exist_ok=True)
payload = f"""#!/usr/bin/env bash
set -euo pipefail
cd {q(repo_root)}
export REPO_ROOT={q(repo_root)}
export PYTHON_BIN={q(python_bin)}
if [[ -n "${{PYTHONPATH:-}}" ]]; then
  export PYTHONPATH={q(str(Path(repo_root) / "src"))}:$PYTHONPATH
else
  export PYTHONPATH={q(str(Path(repo_root) / "src"))}
fi
export MGC_HEADLESS_PAPER_PID_FILE={q(pid_file)}
export MGC_TRACK_B_PAPER_PID_METADATA_FILE={q(pid_metadata_file)}
export MGC_TRACK_B_PAPER_WRAPPER_STATUS_FILE={q(wrapper_status_file)}
export MGC_HEADLESS_PAPER_LOG_FILE={q(log_file)}
export MGC_HEADLESS_SCRIPT_DIR={q(script_dir)}
export MGC_PROBATIONARY_PAPER_CONFIG_PATHS={q(requested_stack)}
export MGC_HEADLESS_SUPERVISED_PAPER_CONFIG_PATHS={q(requested_stack)}
export MGC_HEADLESS_REQUIRED_PAPER_CONFIGS={q(required_stack)}
export MGC_HEADLESS_REQUIRED_PAPER_CONFIG_PATHS={q(required_stack)}
export MGC_TRACK_B_RUNTIME_INSTANCE_ID={q(runtime_instance_id)}
export MGC_TRACK_B_PAPER_RUNTIME_RESTART_GENERATION={q(restart_generation)}
export MGC_TRACK_B_PAPER_LAUNCH_STARTED_AT={q(launch_started_at)}
export MGC_TRACK_B_PAPER_LAUNCHER_PID={q(launcher_pid)}
export MGC_TRACK_B_EXPECTED_PROJECT_ROOT={q(repo_root)}
export MGC_TRACK_B_PAPER_CONFIG_FINGERPRINT={q(config_fingerprint)}
export MGC_TRACK_B_EXPECTED_SOURCE_COMMIT={q(expected_source_commit)}
mkdir -p "$(dirname "$MGC_HEADLESS_PAPER_PID_FILE")" "$(dirname "$MGC_HEADLESS_PAPER_LOG_FILE")"
write_wrapper_status() {{
  local classification="$1"
  local reason="${{2:-}}"
  {q(python_bin)} - "$MGC_TRACK_B_PAPER_WRAPPER_STATUS_FILE" "$classification" "$reason" "$$" "$REPO_ROOT" "$MGC_TRACK_B_RUNTIME_INSTANCE_ID" "$MGC_TRACK_B_PAPER_RUNTIME_RESTART_GENERATION" "$MGC_HEADLESS_PAPER_PID_FILE" "$MGC_TRACK_B_EXPECTED_SOURCE_COMMIT" "${{current_commit:-}}" <<'WRAPPER_STATUS_PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    status_path,
    classification,
    reason,
    pid,
    repo_root,
    runtime_instance_id,
    restart_generation,
    pid_file,
    expected_source_commit,
    observed_source_commit,
) = sys.argv[1:]
payload = {{
    "schema_version": "track_b_paper_runtime_wrapper_status_v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "classification": classification,
    "reason": reason or None,
    "producer_pid": int(pid),
    "producer_root": repo_root,
    "runtime_instance_id": runtime_instance_id,
    "restart_generation": int(restart_generation),
    "pid_file": pid_file,
    "expected_source_commit": expected_source_commit,
    "observed_source_commit": observed_source_commit or None,
    "paper_only": True,
    "live_money_eligible": False,
    "submit_authority": False,
    "readiness_authority": False,
    "restart_authority": False,
}}
path = Path(status_path)
path.parent.mkdir(parents=True, exist_ok=True)
tmp = path.with_name(f".{{path.name}}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\\n", encoding="utf-8")
tmp.replace(path)
WRAPPER_STATUS_PY
}}
write_wrapper_status "WRAPPER_STARTED" ""
{{
  printf '%s\\n' "headless_runtime_wrapper_start generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf '%s\\n' "repo_root=$REPO_ROOT"
  printf '%s\\n' "python_bin=$PYTHON_BIN"
  printf '%s\\n' "config_stack=$MGC_PROBATIONARY_PAPER_CONFIG_PATHS"
}} >> "$MGC_HEADLESS_PAPER_LOG_FILE"
current_commit="$(git -C "$REPO_ROOT" rev-parse HEAD)"
if [[ "$current_commit" != "$MGC_TRACK_B_EXPECTED_SOURCE_COMMIT" ]]; then
  write_wrapper_status "WRAPPER_PRE_EXEC_FAILURE" "source_commit_mismatch"
  printf '%s\\n' "headless_runtime_wrapper_fence_blocked reason=source_commit_mismatch expected=$MGC_TRACK_B_EXPECTED_SOURCE_COMMIT observed=$current_commit generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$MGC_HEADLESS_PAPER_LOG_FILE"
  exit 23
fi
echo "$$" > "$MGC_HEADLESS_PAPER_PID_FILE"
{q(python_bin)} - <<'RUNTIME_PID_METADATA_PY' "$MGC_TRACK_B_PAPER_PID_METADATA_FILE" "$$" "$MGC_TRACK_B_RUNTIME_INSTANCE_ID" "$MGC_TRACK_B_PAPER_RUNTIME_RESTART_GENERATION" "$REPO_ROOT"
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

path = Path(sys.argv[1])
pid = int(sys.argv[2])
runtime_instance_id = sys.argv[3]
restart_generation = int(sys.argv[4])
repo_root = sys.argv[5]
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {{}}
payload.update({{
    "pid": pid,
    "producer_pid": pid,
    "runtime_instance_id": runtime_instance_id,
    "restart_generation": restart_generation,
    "root": repo_root,
    "producer_root": repo_root,
    "generated_at": datetime.now(timezone.utc).isoformat(),
}})
tmp = path.with_name(f".{{path.name}}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\\n", encoding="utf-8")
tmp.replace(path)
RUNTIME_PID_METADATA_PY
printf '%s\\n' "headless_runtime_wrapper_exec generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$MGC_HEADLESS_PAPER_LOG_FILE"
write_wrapper_status "WRAPPER_EXECING_RUNTIME" ""
exec bash "$MGC_HEADLESS_SCRIPT_DIR/run_probationary_paper_soak.sh" >> "$MGC_HEADLESS_PAPER_LOG_FILE" 2>&1
"""
path.write_text(payload, encoding="utf-8")
path.chmod(0o755)
print(path)
PY
}

launch_background_paper_runtime() {
  local launch_rc
  rm -f "${PAPER_WRAPPER_PID_FILE}"
  rm -f "${PAPER_PID_FILE}.screen_session"
  case "$(printf '%s' "${PAPER_RUNTIME_LAUNCH_METHOD}" | tr '[:upper:]' '[:lower:]')" in
    direct|direct-supervisor|probationary)
      launch_direct_paper_runtime
      return $?
      ;;
    launchctl|diagnostic-launchctl)
      ;;
    *)
      echo "CANONICAL_PAPER_LAUNCHER_UNAVAILABLE: unsupported PAPER launch method ${PAPER_RUNTIME_LAUNCH_METHOD}; use direct or diagnostic launchctl." >&2
      return 1
      ;;
  esac
  if launchctl_submit_available; then
    set +e
    launch_detached_paper_runtime
    launch_rc=$?
    set -e
    return "${launch_rc}"
  fi
  echo "CANONICAL_PAPER_LAUNCHER_UNAVAILABLE: launchctl diagnostic launch requested but launchctl is unavailable." >&2
  return 1
}

launch_direct_paper_runtime() {
  prepare_paper_runtime_generation
  MGC_PROBATIONARY_PAPER_CONFIG_PATHS="$(requested_config_paths_arg)" \
  MGC_HEADLESS_SUPERVISED_PAPER_CONFIG_PATHS="$(requested_config_paths_arg)" \
  MGC_HEADLESS_REQUIRED_PAPER_CONFIGS="$(resolved_config_paths_arg "${REQUIRED_PAPER_CONFIG_PATHS}")" \
  MGC_HEADLESS_REQUIRED_PAPER_CONFIG_PATHS="$(resolved_config_paths_arg "${REQUIRED_PAPER_CONFIG_PATHS}")" \
  MGC_TRACK_B_RUNTIME_INSTANCE_ID="${PAPER_RUNTIME_INSTANCE_ID}" \
  MGC_TRACK_B_PAPER_RUNTIME_RESTART_GENERATION="${PAPER_RUNTIME_RESTART_GENERATION}" \
  MGC_TRACK_B_PAPER_LAUNCH_STARTED_AT="${PAPER_RUNTIME_LAUNCH_STARTED_AT}" \
  MGC_TRACK_B_PAPER_LAUNCHER_PID="$$" \
  MGC_TRACK_B_EXPECTED_PROJECT_ROOT="${REPO_ROOT}" \
  MGC_TRACK_B_PAPER_CONFIG_FINGERPRINT="${PAPER_RUNTIME_CONFIG_FINGERPRINT}" \
  MGC_TRACK_B_EXPECTED_SOURCE_COMMIT="${PAPER_RUNTIME_EXPECTED_SOURCE_COMMIT}" \
  MGC_PROBATIONARY_PAPER_RUNTIME_TRUTH_FILE="${PAPER_RUNTIME_TRUTH_FILE}" \
  bash "${SCRIPT_DIR}/run_probationary_paper_soak.sh" \
    --background \
    --pid-file "${PAPER_PID_FILE}" \
    --log-file "${PAPER_LOG_FILE}" \
    --config-paths-file "${PAPER_CONFIG_PATHS_FILE}" \
    --launch-status-file "${PAPER_RUNTIME_LAUNCH_STATUS_FILE}" \
    --schwab-config "${REPO_ROOT}/config/schwab.local.json"
}

launch_screen_dashboard_manager() {
  local session_name
  session_name="$(screen_session_name "dashboard")"
  printf '%s\n' "${session_name}" > "${MANAGER_PID_FILE}.screen_session"
  MGC_HEADLESS_MANAGER_PID_FILE="${MANAGER_PID_FILE}" \
    MGC_HEADLESS_MANAGER_LOG_FILE="${MANAGER_LOG_FILE}" \
    MGC_HEADLESS_SCRIPT_DIR="${SCRIPT_DIR}" \
    screen -dmS "${session_name}" /bin/zsh -lc \
    'echo "$$" > "${MGC_HEADLESS_MANAGER_PID_FILE}"; exec bash "${MGC_HEADLESS_SCRIPT_DIR}/run_operator_dashboard.sh" --no-open-browser >> "${MGC_HEADLESS_MANAGER_LOG_FILE}" 2>&1'
}

launch_detached_paper_runtime() {
  local label="com.mgc-v05l.headless-supervised-paper.runtime.$(date +%Y%m%d%H%M%S).$$"
  local wrapper_path
  local launchctl_rc
  prepare_paper_runtime_generation
  printf '%s\n' "${label}" > "${PAPER_PID_FILE}.launchctl_label"
  rm -f "${PAPER_PID_FILE}.screen_session"
  rm -f "${PAPER_WRAPPER_STATUS_FILE}" "${PAPER_LAUNCHCTL_STDOUT_FILE}" "${PAPER_LAUNCHCTL_STDERR_FILE}"
  wrapper_path="$(write_paper_runtime_wrapper)"
  set +e
  launchctl submit \
    -l "${label}" \
    -o "${PAPER_LAUNCHCTL_STDOUT_FILE}" \
    -e "${PAPER_LAUNCHCTL_STDERR_FILE}" \
    -- /bin/bash "${wrapper_path}"
  launchctl_rc=$?
  set -e
  if [[ "${launchctl_rc}" -ne 0 ]]; then
    write_launchctl_runtime_status "LAUNCHCTL_SUBMIT_FAILED" "${launchctl_rc}" "false" "launchctl submit failed before runtime wrapper could be verified"
    launchctl remove "${label}" >/dev/null 2>&1 || true
    rm -f "${PAPER_PID_FILE}.launchctl_label"
    return "${launchctl_rc}"
  fi
  write_launchctl_runtime_status "LAUNCHCTL_SUBMIT_ACCEPTED" "${launchctl_rc}" "false" "launchctl submit returned successfully; waiting for runtime PID"
}

launch_detached_dashboard_manager() {
  local label="com.mgc-v05l.headless-supervised-paper.dashboard.$(date +%Y%m%d%H%M%S).$$"
  printf '%s\n' "${label}" > "${MANAGER_PID_FILE}.launchctl_label"
  launchctl submit -l "${label}" -- /usr/bin/env \
    MGC_HEADLESS_MANAGER_PID_FILE="${MANAGER_PID_FILE}" \
    MGC_HEADLESS_MANAGER_LOG_FILE="${MANAGER_LOG_FILE}" \
    MGC_HEADLESS_SCRIPT_DIR="${SCRIPT_DIR}" \
    MGC_SERVICE_HOST_AUTOSTART_RESEARCH_RUNTIME_BRIDGE_SUPERVISOR="${SERVICE_HOST_AUTOSTART_BRIDGE_SUPERVISOR}" \
    /bin/zsh -lc 'echo "$$" > "${MGC_HEADLESS_MANAGER_PID_FILE}"; exec bash "${MGC_HEADLESS_SCRIPT_DIR}/run_operator_dashboard.sh" --no-open-browser >> "${MGC_HEADLESS_MANAGER_LOG_FILE}" 2>&1'
}

write_startup_summary() {
  local startup_state="$1"
  local reason="$2"
  local app_usable="$3"
  "${PYTHON_BIN}" - <<'PY' "${STARTUP_FILE}" "${startup_state}" "${reason}" "${app_usable}" "${STATUS_FILE}" "${MARKDOWN_FILE}" "${DASHBOARD_URL}"
import json
import sys
from datetime import datetime, timezone

path, state, reason, usable, status_path, markdown_path, dashboard_url = sys.argv[1:]
payload = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "mode": "HEADLESS_SUPERVISED_PAPER_SERVICE",
    "startup_state": state,
    "reason": reason,
    "app_usable_for_supervised_paper": usable.lower() == "true",
    "status_artifact": status_path,
    "markdown_artifact": markdown_path,
    "dashboard_url": dashboard_url,
}
with open(path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

stop_paper_runtime_best_effort() {
  if [[ "${START_PAPER}" -ne 1 ]]; then
    return 0
  fi
  local wrapper_pid
  wrapper_pid="$(read_pid_file "${PAPER_WRAPPER_PID_FILE}" || true)"
  if [[ -n "${wrapper_pid}" ]] && kill -0 "${wrapper_pid}" 2>/dev/null; then
    kill "${wrapper_pid}" >/dev/null 2>&1 || true
  fi
  bash "${SCRIPT_DIR}/stop_probationary_paper_soak.sh" >/dev/null 2>&1 || true
}

start_paper_runtime() {
  if [[ "${START_PAPER}" -ne 1 ]]; then
    return 0
  fi
  local guard_classification
  guard_classification="$(classify_paper_runtime_launch_guard)"
  case "${guard_classification}" in
    LAUNCH_PID_ACCEPTED)
      assert_runtime_config_paths_match_request "pre-existing-runtime"
      return $?
      ;;
    LAUNCH_STALE_PID_CLEANUP_ALLOWED)
      cleanup_stale_paper_pid_metadata_if_allowed || true
      ;;
    LAUNCH_NO_PID_METADATA)
      ;;
    LAUNCH_CONFLICTING_WRITER_BLOCKED|LAUNCH_WRONG_ROOT_BLOCKED|LAUNCH_ZOMBIE_PID_REJECTED|LAUNCH_STALE_PID_CLEANUP_BLOCKED)
      echo "PAPER_RUNTIME_LAUNCH_GUARD_BLOCKED: ${guard_classification}; see ${PAPER_LAUNCH_GUARD_FILE}" >&2
      return 2
      ;;
    *)
      echo "PAPER_RUNTIME_LAUNCH_GUARD_BLOCKED: unexpected ${guard_classification}; see ${PAPER_LAUNCH_GUARD_FILE}" >&2
      return 2
      ;;
  esac
  if pid_file_alive "${PAPER_PID_FILE}"; then
    assert_runtime_config_paths_match_request "pre-existing-runtime"
    return $?
  fi
  launch_background_paper_runtime
  return $?
}

refresh_phase1_reconciliation_for_launch() {
  "${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_paper_broker_reconciliation \
    --repo-root "${REPO_ROOT}" >/dev/null
}

refresh_operator_readiness_for_launch() {
  "${PYTHON_BIN}" -m mgc_v05l.app.track_b_operator_readiness_refresher \
    --repo-root "${REPO_ROOT}" \
    --once >/dev/null
}

refresh_broker_truth_lease_for_launch() {
  "${PYTHON_BIN}" -m mgc_v05l.app.track_b_broker_truth_lease \
    --repo-root "${REPO_ROOT}" \
    --no-history \
    --json >/dev/null
}

start_dashboard_manager() {
  if [[ "${START_DASHBOARD}" -ne 1 ]]; then
    return 0
  fi
  local attempt=1
  while (( attempt <= 5 )); do
    if curl -fsS "${DASHBOARD_URL%/}/health" >/dev/null 2>&1; then
      return 0
    fi
    attempt=$((attempt + 1))
    if (( attempt <= 5 )); then
      sleep 1
    fi
  done
  if pid_file_alive "${DASHBOARD_PID_FILE}"; then
    return 0
  fi
  if pid_file_alive "${MANAGER_PID_FILE}"; then
    return 0
  fi
  if curl -fsS "${DASHBOARD_URL%/}/health" >/dev/null 2>&1; then
    return 0
  fi
  if screen_available; then
    launch_screen_dashboard_manager
    return 0
  fi
  if launchctl_submit_available; then
    launch_detached_dashboard_manager
    return 0
  fi
  MGC_SERVICE_HOST_AUTOSTART_RESEARCH_RUNTIME_BRIDGE_SUPERVISOR="${SERVICE_HOST_AUTOSTART_BRIDGE_SUPERVISOR}" \
    nohup bash "${SCRIPT_DIR}/run_operator_dashboard.sh" --no-open-browser >> "${MANAGER_LOG_FILE}" 2>&1 &
  local manager_pid=$!
  echo "${manager_pid}" > "${MANAGER_PID_FILE}"
}

start_broker_truth_refresher() {
  if ! truthy_flag "${START_BROKER_TRUTH_REFRESH}"; then
    return 0
  fi
  TRACK_B_BROKER_TRUTH_REFRESH_SECONDS="${TRACK_B_BROKER_TRUTH_REFRESH_SECONDS:-60}" \
    TRACK_B_BROKER_TRUTH_REFRESH_CLIENT_ID="${TRACK_B_BROKER_TRUTH_REFRESH_CLIENT_ID:-9077}" \
    bash "${SCRIPT_DIR}/start-track-b-broker-truth-refresh" >/dev/null
}

ready_processes_and_endpoints_alive() {
  if [[ "${START_PAPER}" -eq 1 ]] && ! pid_file_alive "${PAPER_PID_FILE}"; then
    return 1
  fi
  if [[ "${START_DASHBOARD}" -eq 1 ]] && ! pid_file_alive "${DASHBOARD_PID_FILE}"; then
    return 1
  fi
  if [[ "${START_DASHBOARD}" -eq 1 ]] && ! pid_file_alive "${MANAGER_PID_FILE}"; then
    return 1
  fi
  curl -fsS "${DASHBOARD_URL%/}/health" >/dev/null 2>&1 || return 1
  curl -fsS "${DASHBOARD_URL%/}/api/dashboard" >/dev/null 2>&1 || return 1
  return 0
}

read_contract_field() {
  local field="$1"
  "${PYTHON_BIN}" - <<'PY' "${STATUS_FILE}" "${field}"
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
value = payload
for part in sys.argv[2].split("."):
    if isinstance(value, dict):
        value = value.get(part)
    else:
        value = None
        break
if isinstance(value, bool):
    print("true" if value else "false")
elif value is None:
    print("")
else:
    print(value)
PY
}

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
  local phase="$1"
  "${PYTHON_BIN}" - <<'PY' "${CANONICAL_READINESS_SUMMARY_FILE}" "${phase}" >&2
import json
import sys
from pathlib import Path

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {"classification": "NOT_READY_CONFIG", "blockers": ["canonical_readiness_summary_missing"]}

print(f"canonical_readiness_summary[{sys.argv[2]}]:")
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

refresh_canonical_readiness_for_launch() {
  local phase="$1"
  refresh_operator_readiness_for_launch || true
  refresh_broker_truth_lease_for_launch || true
  local tmp_summary
  tmp_summary="${CANONICAL_READINESS_SUMMARY_FILE}.$$.${RANDOM}.tmp"
  rm -f "${tmp_summary}"
  set +e
  "${PYTHON_BIN}" -m mgc_v05l.app.track_b_canonical_readiness \
    --repo-root "${REPO_ROOT}" \
    --expected-root "${REPO_ROOT}" \
    --output-path "${CANONICAL_READINESS_FILE}" \
    --json > "${tmp_summary}"
  local exit_code=$?
  set -e
  if [[ -s "${tmp_summary}" ]]; then
    mv "${tmp_summary}" "${CANONICAL_READINESS_SUMMARY_FILE}"
  else
    rm -f "${tmp_summary}"
  fi
  print_canonical_readiness_summary "${phase}"
  return "${exit_code}"
}

fail_fast_if_hard_canonical_blocker() {
  local phase="$1"
  local classification
  classification="$(canonical_readiness_classification)"
  case "${classification}" in
    NOT_READY_WRONG_ROOT|NOT_READY_CONFIG|NOT_READY_RECONCILIATION)
      if [[ "${phase}" != "pre-launch" ]]; then
        stop_paper_runtime_best_effort
      fi
      write_startup_summary "BLOCKED" "Canonical readiness ${classification} during ${phase}." "false"
      cat "${STARTUP_FILE}"
      exit 2
      ;;
  esac
}

refresh_runtime_supervisor_for_launch() {
  # Legacy fallback only. The launch hot path uses refresh_control_plane_snapshot_for_launch.
  local tmp_summary
  tmp_summary="${RUNTIME_SUPERVISOR_AUTHORITY_FILE}.$$.${RANDOM}.tmp"
  rm -f "${tmp_summary}"
  set +e
  "${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_runtime_supervisor_authority \
    --repo-root "${REPO_ROOT}" \
    --output-path "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" \
    --no-dashboard-projection \
    --json > "${tmp_summary}"
  local exit_code=$?
  set -e
  if [[ -s "${tmp_summary}" ]]; then
    mv "${tmp_summary}" "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}"
    return 0
  else
    rm -f "${tmp_summary}"
  fi
  return "${exit_code}"
}

refresh_control_plane_snapshot_for_launch() {
  local tmp_summary
  tmp_summary="${CONTROL_PLANE_SNAPSHOT_FILE}.$$.${RANDOM}.tmp"
  rm -f "${tmp_summary}"
  set +e
  "${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_control_plane_snapshot \
    --repo-root "${REPO_ROOT}" \
    --output-path "${CONTROL_PLANE_SNAPSHOT_FILE}" \
    --no-dashboard-projection \
    --no-broker-lease-history \
    --json > "${tmp_summary}"
  local exit_code=$?
  set -e
  if [[ -s "${tmp_summary}" ]]; then
    mv "${tmp_summary}" "${CONTROL_PLANE_SNAPSHOT_FILE}"
    return 0
  else
    rm -f "${tmp_summary}"
  fi
  return "${exit_code}"
}

control_plane_snapshot_start_gate() {
  "${PYTHON_BIN}" - <<'PY' "${CONTROL_PLANE_SNAPSHOT_FILE}"
import json
import sys
from pathlib import Path
from mgc_v05l.execution_core.track_b_control_plane_snapshot_status import classify_control_plane_snapshot_status

path = Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    status = classify_control_plane_snapshot_status({}, required_for_launch=True)
    print(
        "CONTROL_PLANE_SNAPSHOT_START_BLOCKED: "
        f"control_plane_status_classification={status['classification']} "
        "diagnostic_only=true not_routing_authority=true "
        f"reason={status['reason']}",
        file=sys.stderr,
    )
    raise SystemExit(2)

status = classify_control_plane_snapshot_status(payload, required_for_launch=True)
allowed = (
    status.get("classification") == "CONTROL_PLANE_READY"
    and payload.get("runtime_supervisor_classification") == "SUPERVISOR_RUNTIME_START_ALLOWED"
    and payload.get("supervisor_mode") == "READY_FOR_OPERATOR_START"
    and status.get("safe_to_start_runtime") is True
    and not payload.get("blockers")
)
if not allowed:
    print(
        "CONTROL_PLANE_SNAPSHOT_START_BLOCKED: "
        f"control_plane_status_classification={status.get('classification')} "
        f"diagnostic_only={status.get('diagnostic_only')} "
        f"not_routing_authority={status.get('not_routing_authority')} "
        f"control_plane_snapshot_missing={status.get('control_plane_snapshot_missing')} "
        f"control_plane_snapshot_stale={status.get('control_plane_snapshot_stale')} "
        f"control_plane_snapshot_incoherent={status.get('control_plane_snapshot_incoherent')} "
        f"snapshot_id={payload.get('control_plane_snapshot_id')} "
        f"classification={payload.get('classification')} "
        f"shared_truth_refresh_generation_id={payload.get('shared_truth_refresh_generation_id')} "
        f"shared_truth_coherence_status={payload.get('shared_truth_coherence_status')} "
        f"runtime_supervisor_classification={payload.get('runtime_supervisor_classification')} "
        f"supervisor_mode={payload.get('supervisor_mode')} "
        f"proof_window_status={payload.get('proof_window_status')} "
        f"safe_to_start_runtime={status.get('safe_to_start_runtime')} "
        f"paper_recovery_policy={payload.get('paper_recovery_policy')} "
        f"autonomous_recovery_plan_classification={payload.get('autonomous_recovery_plan_classification')} "
        f"autonomous_recovery_next_action={payload.get('autonomous_recovery_next_action')} "
        f"autonomous_recovery_execution_enabled={payload.get('autonomous_recovery_execution_enabled')} "
        f"recommended_next_command={payload.get('recommended_next_command')}",
        file=sys.stderr,
    )
    raise SystemExit(2)
raise SystemExit(0)
PY
}

control_plane_snapshot_blocked_reason() {
  "${PYTHON_BIN}" - <<'PY' "${CONTROL_PLANE_SNAPSHOT_FILE}"
import json
import sys
from pathlib import Path
from mgc_v05l.execution_core.track_b_control_plane_snapshot_status import classify_control_plane_snapshot_status

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
status = classify_control_plane_snapshot_status(payload, required_for_launch=True)
print(
    "Control Plane Snapshot blocked start: "
    f"control_plane_status_classification={status.get('classification')} "
    f"diagnostic_only={status.get('diagnostic_only')} "
    f"not_routing_authority={status.get('not_routing_authority')} "
    f"control_plane_snapshot_missing={status.get('control_plane_snapshot_missing')} "
    f"control_plane_snapshot_stale={status.get('control_plane_snapshot_stale')} "
    f"control_plane_snapshot_incoherent={status.get('control_plane_snapshot_incoherent')} "
    f"snapshot_id={payload.get('control_plane_snapshot_id')} "
    f"classification={payload.get('classification')} "
    f"shared_truth_refresh_generation_id={payload.get('shared_truth_refresh_generation_id')} "
    f"shared_truth_coherence_status={payload.get('shared_truth_coherence_status')} "
    f"runtime_supervisor_classification={payload.get('runtime_supervisor_classification')} "
    f"supervisor_mode={payload.get('supervisor_mode')} "
    f"proof_window_status={payload.get('proof_window_status')} "
    f"safe_to_start_runtime={status.get('safe_to_start_runtime')} "
    f"paper_recovery_policy={payload.get('paper_recovery_policy')} "
    f"autonomous_recovery_plan_classification={payload.get('autonomous_recovery_plan_classification')} "
    f"autonomous_recovery_next_action={payload.get('autonomous_recovery_next_action')} "
    f"autonomous_recovery_execution_enabled={payload.get('autonomous_recovery_execution_enabled')} "
    f"recommended_next_command={payload.get('recommended_next_command')}"
)
PY
}

runtime_supervisor_start_gate() {
  "${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}"
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    print("RUNTIME_SUPERVISOR_START_BLOCKED: missing_or_invalid_supervisor_authority", file=sys.stderr)
    raise SystemExit(2)
operator_ack = payload.get("operator_ack") if isinstance(payload.get("operator_ack"), dict) else {}
evidence = payload.get("evidence_summary") if isinstance(payload.get("evidence_summary"), dict) else {}
classification = payload.get("classification")
mode = payload.get("supervisor_mode")
proof_window_status = payload.get("proof_window_status")
recommended = payload.get("recommended_next_command")
autonomous_plan_classification = payload.get("autonomous_recovery_plan_classification")
autonomous_next_action = payload.get("autonomous_recovery_next_action")
autonomous_execution_enabled = payload.get("autonomous_recovery_execution_enabled") is True
shared_truth_generation_id = payload.get("shared_truth_refresh_generation_id")
shared_truth_coherence_status = payload.get("shared_truth_coherence_status")
ack_required = payload.get("operator_ack_required") is True or operator_ack.get("required") is True
paper_action_policy = evidence.get("paper_action_policy")
autonomous_recovery_allowed = evidence.get("paper_autonomous_recovery_allowed") is True
requires_operator_ack_for_paper = evidence.get("paper_requires_operator_ack") is True
live_action_policy = evidence.get("paper_live_action_policy")
operator_ack_advisory_only_for_paper = bool(ack_required and not requires_operator_ack_for_paper and paper_action_policy)
allowed = (
    classification == "SUPERVISOR_RUNTIME_START_ALLOWED"
    and mode == "READY_FOR_OPERATOR_START"
    and payload.get("safe_to_start_runtime") is True
    and not ack_required
)
if not allowed:
    print(
        "RUNTIME_SUPERVISOR_START_BLOCKED: "
        f"classification={classification} supervisor_mode={mode} proof_window_status={proof_window_status} "
        f"operator_ack_required={ack_required} paper_action_policy={paper_action_policy} "
        f"autonomous_recovery_allowed={autonomous_recovery_allowed} "
        f"requires_operator_ack_for_paper={requires_operator_ack_for_paper} "
        f"operator_ack_advisory_only_for_paper={operator_ack_advisory_only_for_paper} "
        f"autonomous_recovery_plan_classification={autonomous_plan_classification} "
        f"autonomous_recovery_next_action={autonomous_next_action} "
        f"autonomous_recovery_execution_enabled={autonomous_execution_enabled} "
        f"shared_truth_refresh_generation_id={shared_truth_generation_id} "
        f"shared_truth_coherence_status={shared_truth_coherence_status} "
        f"live_action_policy={live_action_policy} recommended_next_command={recommended}",
        file=sys.stderr,
    )
    raise SystemExit(2)
raise SystemExit(0)
PY
}

runtime_supervisor_blocked_reason() {
  "${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}"
import json
import sys
from pathlib import Path

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
operator_ack = payload.get("operator_ack") if isinstance(payload.get("operator_ack"), dict) else {}
evidence = payload.get("evidence_summary") if isinstance(payload.get("evidence_summary"), dict) else {}
ack_required = payload.get("operator_ack_required") is True or operator_ack.get("required") is True
autonomous_plan_classification = payload.get("autonomous_recovery_plan_classification")
autonomous_next_action = payload.get("autonomous_recovery_next_action")
autonomous_execution_enabled = payload.get("autonomous_recovery_execution_enabled") is True
shared_truth_generation_id = payload.get("shared_truth_refresh_generation_id")
shared_truth_coherence_status = payload.get("shared_truth_coherence_status")
paper_action_policy = evidence.get("paper_action_policy")
autonomous_recovery_allowed = evidence.get("paper_autonomous_recovery_allowed") is True
requires_operator_ack_for_paper = evidence.get("paper_requires_operator_ack") is True
live_action_policy = evidence.get("paper_live_action_policy")
operator_ack_advisory_only_for_paper = bool(ack_required and not requires_operator_ack_for_paper and paper_action_policy)
print(
    "Runtime Supervisor Authority blocked start: "
    f"classification={payload.get('classification')} "
    f"supervisor_mode={payload.get('supervisor_mode')} "
    f"proof_window_status={payload.get('proof_window_status')} "
    f"operator_ack_required={ack_required}. "
    f"paper_action_policy={paper_action_policy} "
    f"autonomous_recovery_allowed={autonomous_recovery_allowed} "
    f"requires_operator_ack_for_paper={requires_operator_ack_for_paper} "
    f"operator_ack_advisory_only_for_paper={operator_ack_advisory_only_for_paper} "
    f"autonomous_recovery_plan_classification={autonomous_plan_classification} "
    f"autonomous_recovery_next_action={autonomous_next_action} "
    f"autonomous_recovery_execution_enabled={autonomous_execution_enabled} "
    f"shared_truth_refresh_generation_id={shared_truth_generation_id} "
    f"shared_truth_coherence_status={shared_truth_coherence_status} "
    f"live_action_policy={live_action_policy}. "
    f"recommended_next_command={payload.get('recommended_next_command')}"
)
PY
}

persist_requested_config_paths
if ! assert_required_config_paths_present "${REQUIRED_PAPER_CONFIG_PATHS}"; then
  write_startup_summary "BLOCKED" "Requested paper runtime config stack is missing required config paths." "false"
  cat "${STARTUP_FILE}"
  exit 2
fi
if ! assert_requested_config_stack_safe; then
  write_startup_summary "BLOCKED" "Requested paper runtime config stack is unsafe." "false"
  cat "${STARTUP_FILE}"
  exit 2
fi
set +e
refresh_canonical_readiness_for_launch "pre-launch"
set -e
fail_fast_if_hard_canonical_blocker "pre-launch"
if ! refresh_control_plane_snapshot_for_launch; then
  write_launchctl_runtime_status "CONTROL_PLANE_SNAPSHOT_START_BLOCKED" "0" "false" "Control Plane Snapshot refresh failed before launch."
  write_startup_summary "BLOCKED" "Control Plane Snapshot refresh failed before launch." "false"
  cat "${STARTUP_FILE}"
  exit 2
fi
if ! control_plane_snapshot_start_gate; then
  supervisor_reason="$(control_plane_snapshot_blocked_reason)"
  write_launchctl_runtime_status "CONTROL_PLANE_SNAPSHOT_START_BLOCKED" "0" "false" "${supervisor_reason}"
  write_startup_summary "BLOCKED" "${supervisor_reason}" "false"
  cat "${STARTUP_FILE}"
  exit 2
fi

if ! start_paper_runtime; then
  write_startup_summary "BLOCKED" "Failed to start the supervised paper runtime." "false"
  exit 1
fi
set +e
wait_for_runtime_config_paths_match_request "post-start" "${POST_START_PID_WAIT_TIMEOUT_SECONDS}"
wait_rc=$?
set -e
if [[ "${wait_rc}" -ne 0 ]]; then
  if [[ "${wait_rc}" -eq 1 ]]; then
    write_launchctl_runtime_status "RUNTIME_PID_UNAVAILABLE_AFTER_LAUNCHCTL_SUBMIT" "0" "false" "Paper runtime PID unavailable during post-start."
  elif [[ "${wait_rc}" -eq 3 ]]; then
    write_launchctl_runtime_status "WRAPPER_PRE_EXEC_FAILURE" "0" "false" "Paper runtime wrapper exited before a valid Python runtime PID became available during post-start."
  else
    write_launchctl_runtime_status "PID_WRITE_PATH_MISMATCH" "0" "false" "Active paper runtime config paths did not match requested launch config stack."
  fi
  stop_paper_runtime_best_effort
  if [[ "${wait_rc}" -eq 1 ]]; then
    write_startup_summary "BLOCKED" "Paper runtime PID unavailable during post-start." "false"
  elif [[ "${wait_rc}" -eq 3 ]]; then
    write_startup_summary "BLOCKED" "Paper runtime exited before a valid Python runtime PID became available during post-start." "false"
  else
    write_startup_summary "BLOCKED" "Active paper runtime config paths did not match requested launch config stack." "false"
  fi
  exit 2
fi
write_launchctl_runtime_status "RUNTIME_PID_AVAILABLE" "0" "true" "Paper runtime PID and requested config stack verified during post-start."
if ! start_dashboard_manager; then
  write_startup_summary "BLOCKED" "Failed to start the operator dashboard manager." "false"
  exit 1
fi
if ! start_broker_truth_refresher; then
  if truthy_flag "${STRICT_BROKER_TRUTH_REFRESH}"; then
    write_startup_summary "BLOCKED" "Failed to start the read-only broker-truth refresh sidecar in strict mode." "false"
    exit 1
  fi
  echo "Warning: failed to start read-only broker-truth refresh sidecar; canonical readiness remains authoritative and will fail closed if broker truth is stale." >&2
fi

set +e
refresh_canonical_readiness_for_launch "post-launch"
set -e
fail_fast_if_hard_canonical_blocker "post-launch"

deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
last_reason="Headless supervised paper host is still warming."

while (( SECONDS < deadline )); do
  set +e
  bash "${SCRIPT_DIR}/show_headless_supervised_paper_status.sh" \
    --dashboard-url "${DASHBOARD_URL}" \
    --output "${STATUS_FILE}" \
    --markdown-output "${MARKDOWN_FILE}" \
    --canonical-readiness-output "${CANONICAL_READINESS_FILE}" \
    --canonical-readiness-summary-output "${CANONICAL_READINESS_SUMMARY_FILE}" >/dev/null
  status_exit_code=$?
  set -e
  fail_fast_if_hard_canonical_blocker "status-refresh"
  if [[ "$(read_contract_field "app_usable_for_supervised_paper")" == "true" ]]; then
    if ready_processes_and_endpoints_alive; then
      if ! assert_runtime_config_paths_match_request "pre-success"; then
        stop_paper_runtime_best_effort
        write_startup_summary "BLOCKED" "Active paper runtime config paths did not match requested launch config stack before success." "false"
        exit 2
      fi
      if ! refresh_phase1_reconciliation_for_launch; then
        stop_paper_runtime_best_effort
        write_startup_summary "BLOCKED" "Failed to refresh Phase-1 reconciliation before claiming launch success." "false"
        exit 2
      fi
      set +e
      refresh_canonical_readiness_for_launch "pre-success"
      set -e
      fail_fast_if_hard_canonical_blocker "pre-success"
      canonical_state="$(canonical_readiness_classification)"
      if [[ "${canonical_state}" != "READY_SUBMIT_CAPABLE" ]]; then
        write_startup_summary "READY" "Headless supervised paper host is usable for ${canonical_state}." "true"
        cat "${STATUS_FILE}"
        exit 0
      fi
      write_startup_summary "READY" "Headless supervised paper host is READY_SUBMIT_CAPABLE." "true"
      cat "${STATUS_FILE}"
      exit 0
    fi
    last_reason="Headless supervised paper host reported usable before required processes/endpoints were stable."
  fi
  if [[ "${status_exit_code}" -eq 1 ]]; then
    last_reason="Canonical readiness is DEGRADED_NO_SUBMIT."
  elif [[ "${status_exit_code}" -eq 2 ]]; then
    last_reason="Canonical readiness is $(canonical_readiness_classification)."
  fi
  contract_reason="$(read_contract_field "unusable_reason")"
  if [[ -n "${contract_reason}" ]]; then
    last_reason="${contract_reason}"
  fi
  sleep "${POLL_INTERVAL_SECONDS}"
done

write_startup_summary "BLOCKED" "${last_reason}" "false"
cat "${STATUS_FILE}"
exit 1
