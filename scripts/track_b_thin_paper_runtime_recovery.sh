#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

EXPECTED_ROOT="/Users/patrick/Dev/MGC-v05l-automation"
if [[ "${REPO_ROOT}" != "${EXPECTED_ROOT}" ]]; then
  echo "BLOCKED_WRONG_REPO_ROOT: expected ${EXPECTED_ROOT}, got ${REPO_ROOT}" >&2
  exit 2
fi

PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
RUNTIME_DIR="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime"
PID_FILE="${RUNTIME_DIR}/probationary_paper.pid"
WRAPPER_PATH="${RUNTIME_DIR}/track_b_paper_stack_runtime_wrapper.sh"
STATE_DIR="${REPO_ROOT}/outputs/track_b_execution_core/runtime_recovery"
ARTIFACT_PATH="${STATE_DIR}/latest_thin_paper_runtime_recovery.json"
START_SCRIPT="${REPO_ROOT}/scripts/track_b_start_paper_stack.sh"
PROFILE="${TRACK_B_PAPER_STACK_PROFILE:-mnq_mes_full_session_active_evidence}"
EXPECTED_LANES="${TRACK_B_PAPER_EXPECTED_LANE_COUNT:-71}"
EXPECTED_EXECUTION_MODE="${TRACK_B_PAPER_EXPECTED_EXECUTION_MODE:-IBKR_PAPER_BRIDGE}"
MODE="${1:-start}"

write_artifact() {
  local classification="$1"
  local detail="${2:-}"
  local runtime_pid="${3:-}"
  mkdir -p "${STATE_DIR}"
  "${PYTHON_BIN}" - "${ARTIFACT_PATH}" "${classification}" "${detail}" "${runtime_pid}" "${REPO_ROOT}" "${PROFILE}" "${EXPECTED_LANES}" "${EXPECTED_EXECUTION_MODE}" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

path, classification, detail, runtime_pid, repo_root, profile, expected_lanes, expected_execution_mode = sys.argv[1:]
payload = {
    "schema_version": "track_b_thin_paper_runtime_recovery_v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "classification": classification,
    "detail": detail or None,
    "runtime_pid": int(runtime_pid) if runtime_pid.isdigit() else None,
    "repo_root": repo_root,
    "paper_only": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "broad_cancel_used": False,
    "profile": profile,
    "expected_lane_count": int(expected_lanes),
    "expected_execution_mode": expected_execution_mode,
    "restart_command": "TRACK_B_PAPER_STACK_PROFILE={profile} TRACK_B_PAPER_STACK_RESTART=1 TRACK_B_PAPER_MINIMAL_STARTUP_V1=1 TRACK_B_PAPER_STACK_DISABLE_RECOVERY_SERVICE=1 bash scripts/track_b_start_paper_stack.sh".format(profile=profile),
}
tmp = Path(path).with_name(f".{Path(path).name}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(path)
PY
}

runtime_pid_from_artifact() {
  if [[ -s "${PID_FILE}" ]]; then
    tr -dc '0-9' < "${PID_FILE}" || true
  fi
}

pid_alive() {
  local pid="$1"
  [[ -n "${pid}" ]] && ps -p "${pid}" >/dev/null 2>&1
}

require_clean_worktree() {
  if [[ -n "$(git -C "${REPO_ROOT}" status --porcelain)" ]]; then
    write_artifact "WORKTREE_NOT_CLEAN_RECOVERY_BLOCKED" "Thin PAPER recovery requires a clean worktree before restart." ""
    echo "WORKTREE_NOT_CLEAN_RECOVERY_BLOCKED" >&2
    exit 2
  fi
  git -C "${REPO_ROOT}" rev-parse HEAD >/dev/null
}

refresh_broker_truth_read_only() {
  "${PYTHON_BIN}" -m mgc_v05l.app.ibkr_broker_truth_refresher \
    --once \
    --mode PAPER \
    --host 127.0.0.1 \
    --port 7497 \
    --account-id DUM882026 \
    --read-only >/dev/null
}

require_clean_broker_truth() {
  "${PYTHON_BIN}" - "${REPO_ROOT}" "${ARTIFACT_PATH}" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_broker_startup_authority import (
    classify_fresh_complete_clean_broker_truth,
)

repo_root = Path(sys.argv[1])
artifact_path = Path(sys.argv[2])
positions_path = repo_root / "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json"
orders_path = repo_root / "outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json"
status_path = repo_root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json"
managed_positions_path = repo_root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"

def load(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

positions = load(positions_path)
orders = load(orders_path)
status = load(status_path)
managed_positions = load(managed_positions_path)
authority = classify_fresh_complete_clean_broker_truth(
    broker_truth_status=status,
    positions_snapshot=positions,
    open_orders_snapshot=orders,
    managed_positions=managed_positions,
    allow_known_managed_positions=True,
    expected_account_id="DUM882026",
)
blockers = list(authority.blockers)
classification = "BROKER_TRUTH_CLEAN" if authority.broker_truth_clean else "BROKER_TRUTH_NOT_CLEAN_RECOVERY_BLOCKED"
payload = {
    "schema_version": "track_b_thin_paper_runtime_recovery_broker_truth_check_v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "classification": classification,
    "broker_positions_path": str(positions_path),
    "broker_open_orders_path": str(orders_path),
    "broker_truth_status_path": str(status_path),
    "managed_positions_path": str(managed_positions_path),
    "broker_startup_authority": authority.to_dict(),
    "track_b_futures_positions": [dict(row) for row in authority.track_b_futures_positions],
    "broker_open_order_count": authority.broker_open_order_count,
    "unknown_order_count": authority.unknown_open_order_count,
    "known_managed_position_count": authority.known_managed_position_count,
    "unrelated_open_order_count": authority.unrelated_open_order_count,
    "blockers": blockers,
    "paper_only": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
}
tmp = artifact_path.with_name(f".{artifact_path.name}.broker.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(artifact_path)
if blockers:
    print(classification, file=sys.stderr)
    raise SystemExit(2)
print(classification)
PY
}

verify_runtime_shape() {
  local pid="$1"
  local expected_commit
  expected_commit="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
  "${PYTHON_BIN}" - "${REPO_ROOT}" "${RUNTIME_DIR}" "${pid}" "${expected_commit}" "${PROFILE}" "${EXPECTED_LANES}" "${EXPECTED_EXECUTION_MODE}" "${WRAPPER_PATH}" <<'PY'
import json
import subprocess
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
runtime_dir = Path(sys.argv[2])
pid = int(sys.argv[3])
expected_commit = sys.argv[4]
expected_profile = sys.argv[5]
expected_lanes = int(sys.argv[6])
expected_execution_mode = sys.argv[7]
wrapper_path = Path(sys.argv[8])
truth = json.loads((runtime_dir / "paper_runtime_truth.json").read_text(encoding="utf-8"))
config = json.loads((runtime_dir / "paper_config_in_force.json").read_text(encoding="utf-8"))
truth_pid = int(truth.get("producer_pid") or truth.get("pid") or 0)
if truth_pid != pid:
    raise SystemExit("runtime_pid_mismatch")
if truth.get("source_commit") != expected_commit:
    raise SystemExit("runtime_commit_mismatch")
if config.get("profile") != expected_profile:
    raise SystemExit("runtime_profile_mismatch")
lanes = [row for row in config.get("lanes") or [] if isinstance(row, dict)]
if len(lanes) != expected_lanes or int(truth.get("lane_count") or 0) != expected_lanes:
    raise SystemExit("runtime_lane_count_mismatch")
execution_modes = {str(row.get("execution_mode") or (row.get("runtime_overlay_params") or {}).get("execution_mode") or "") for row in lanes}
if execution_modes != {expected_execution_mode}:
    raise SystemExit("runtime_execution_mode_mismatch")
ps = subprocess.run(["ps", "-axo", "pid=,ppid=,command="], text=True, check=False, capture_output=True)
runtime_pids = []
wrapper_pids = []
runtime_parent_pid = None
for line in ps.stdout.splitlines():
    parts = line.strip().split(None, 2)
    if len(parts) != 3:
        continue
    row_pid, ppid, command = parts
    try:
        row_pid_int = int(row_pid)
        ppid_int = int(ppid)
    except ValueError:
        continue
    if str(wrapper_path) in command and "track_b_paper_stack_runtime_wrapper.sh" in command:
        wrapper_pids.append(row_pid_int)
    if str(repo_root) in command and "mgc_v05l.app.main" in command and "probationary-paper-soak" in command:
        runtime_pids.append(row_pid_int)
        runtime_parent_pid = ppid_int
if runtime_pids != [pid]:
    raise SystemExit("runtime_process_count_mismatch")
if len(wrapper_pids) != 1:
    raise SystemExit("runtime_parent_count_mismatch")
if runtime_parent_pid not in wrapper_pids:
    raise SystemExit("runtime_parent_child_mismatch")
PY
}

case "${MODE}" in
  start|restart)
    require_clean_worktree
    refresh_broker_truth_read_only
    require_clean_broker_truth >/dev/null
    TRACK_B_PAPER_STACK_PROFILE="${PROFILE}" \
    TRACK_B_PAPER_STACK_RESTART=1 \
    TRACK_B_PAPER_MINIMAL_STARTUP_V1=1 \
    TRACK_B_PAPER_STACK_DISABLE_RECOVERY_SERVICE=1 \
    bash "${START_SCRIPT}"
    pid="$(runtime_pid_from_artifact)"
    if ! pid_alive "${pid}"; then
      write_artifact "THIN_RECOVERY_START_FAILED" "Runtime PID is unavailable after thin restart." "${pid}"
      exit 1
    fi
    refresh_broker_truth_read_only
    require_clean_broker_truth >/dev/null
    verify_runtime_shape "${pid}"
    write_artifact "THIN_RECOVERY_READY" "Thin PAPER runtime recovery started and verified required runtime shape." "${pid}"
    ;;
  check|preflight)
    require_clean_worktree
    refresh_broker_truth_read_only
    require_clean_broker_truth >/dev/null
    pid="$(runtime_pid_from_artifact)"
    if pid_alive "${pid}"; then
      verify_runtime_shape "${pid}"
      write_artifact "THIN_RECOVERY_READY" "Existing PAPER runtime matches required runtime shape." "${pid}"
    else
      write_artifact "THIN_RECOVERY_READY_TO_START" "Broker truth is clean and no runtime is alive." ""
    fi
    ;;
  *)
    echo "usage: $0 {start|restart|check|preflight}" >&2
    exit 2
    ;;
esac
