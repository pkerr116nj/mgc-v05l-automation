#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

require_schwab_auth_env_if_required

DEFAULT_SCHWAB_CONFIG="${SCHWAB_CONFIG:-${REPO_ROOT}/config/schwab.local.json}"
DEFAULT_CONFIGS=(
  "${REPO_ROOT}/config/base.yaml"
  "${REPO_ROOT}/config/live.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_v1_asia_us.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_v1_asia_us_5m.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_5m.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_v1_pl_asia_us.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_v1_pl_asia_us_5m.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only_5m.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_promotion_1_075r_favorable_only.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_promotion_1_075r_favorable_only_5m.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_v1_gc_asia_us_production_track_5m.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_shared_runtime.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper_track_b_restored.yaml"
)
ATPE_CANARY_CONFIG="${REPO_ROOT}/config/probationary_pattern_engine_paper_atpe_canary.yaml"
GC_MGC_ACCEPTANCE_CONFIG="${REPO_ROOT}/config/probationary_pattern_engine_paper_gc_mgc_acceptance.yaml"
DEFAULT_RUNTIME_DIR="${MGC_PROBATIONARY_PAPER_RUNTIME_DIR:-${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime}"
DEFAULT_PID_FILE="${DEFAULT_RUNTIME_DIR}/probationary_paper.pid"
DEFAULT_LOG_FILE="${DEFAULT_RUNTIME_DIR}/probationary_paper.log"
DEFAULT_CONFIG_PATHS_FILE="${DEFAULT_RUNTIME_DIR}/paper_runtime_config_paths.txt"
DEFAULT_LAUNCH_STATUS_FILE="${DEFAULT_RUNTIME_DIR}/probationary_paper_launch_status.json"
DEFAULT_RUNTIME_TRUTH_FILE="${DEFAULT_RUNTIME_DIR}/paper_runtime_truth.json"
DEFAULT_SHARED_TRUTH_PREFLIGHT_FILE="${DEFAULT_RUNTIME_DIR}/shared_truth_runtime_start_preflight.json"
DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_FILE="${REPO_ROOT}/outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json"
DEFAULT_CONTROL_PLANE_SNAPSHOT_FILE="${REPO_ROOT}/outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"
CANARY_ENABLE_SENTINEL="${DEFAULT_RUNTIME_DIR}/enable_paper_route_canary.flag"
CONFIG_OVERRIDE_RAW="${MGC_PROBATIONARY_PAPER_CONFIG_PATHS:-}"
LAUNCH_PYTHON_BIN="${MGC_PROBATIONARY_PAPER_LAUNCH_PYTHON_BIN:-${PYTHON_BIN}}"
BACKGROUND_VERIFY_ATTEMPTS="${MGC_PROBATIONARY_PAPER_BACKGROUND_VERIFY_ATTEMPTS:-180}"
BACKGROUND_VERIFY_POLL_SECONDS="${MGC_PROBATIONARY_PAPER_BACKGROUND_VERIFY_POLL_SECONDS:-1}"
BACKGROUND_OBSERVATION_WINDOW_SECONDS="${MGC_PROBATIONARY_PAPER_BACKGROUND_OBSERVATION_WINDOW_SECONDS:-5}"
RUNTIME_TRUTH_FILE="${MGC_PROBATIONARY_PAPER_RUNTIME_TRUTH_FILE:-${DEFAULT_RUNTIME_TRUTH_FILE}}"
SHARED_TRUTH_PREFLIGHT_FILE="${MGC_TRACK_B_SHARED_TRUTH_PREFLIGHT_FILE:-${DEFAULT_SHARED_TRUTH_PREFLIGHT_FILE}}"
SHARED_TRUTH_PREFLIGHT_REPO_ROOT="${MGC_TRACK_B_SHARED_TRUTH_PREFLIGHT_REPO_ROOT:-${REPO_ROOT}}"
RUNTIME_SUPERVISOR_AUTHORITY_FILE="${MGC_TRACK_B_RUNTIME_SUPERVISOR_AUTHORITY_FILE:-${DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_FILE}}"
CONTROL_PLANE_SNAPSHOT_FILE="${MGC_TRACK_B_CONTROL_PLANE_SNAPSHOT_FILE:-${DEFAULT_CONTROL_PLANE_SNAPSHOT_FILE}}"
LAUNCH_FIRST_TRUTH_GENERATED_AT=""
LAUNCH_SECOND_TRUTH_GENERATED_AT=""
LAUNCH_SUSTAINED_CONVERGENCE_CONFIRMED="false"
LAUNCH_FINAL_PID_ALIVE="false"
LAUNCH_TERMINATED_BY_VERIFIER="false"
LAUNCH_TERMINATION_SIGNAL=""
LAUNCH_TERMINATION_REASON=""
LAUNCH_STOP_SOURCE=""
LAUNCH_STOP_REQUESTED_AT=""
LAUNCH_STOP_OBSERVED_AT=""
LAUNCH_STOP_EXPECTED_CLEANUP="false"
LAUNCH_STOP_BROKER_SAFE="false"
LAUNCH_SUPERVISOR_CLASSIFICATION=""
LAUNCH_SUPERVISOR_MODE=""
LAUNCH_SUPERVISOR_PROOF_WINDOW_STATUS=""
LAUNCH_SUPERVISOR_RECOMMENDED_NEXT_COMMAND=""
LAUNCH_SUPERVISOR_OPERATOR_ACK_REQUIRED="false"
LAUNCH_SUPERVISOR_PAPER_ACTION_POLICY=""
LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_ALLOWED="false"
LAUNCH_SUPERVISOR_REQUIRES_OPERATOR_ACK_FOR_PAPER="false"
LAUNCH_SUPERVISOR_OPERATOR_ACK_ADVISORY_ONLY_FOR_PAPER="false"
LAUNCH_SUPERVISOR_LIVE_ACTION_POLICY=""
LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_PLAN_CLASSIFICATION=""
LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_NEXT_ACTION=""
LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_EXECUTION_ENABLED="false"
LAUNCH_SUPERVISOR_SHARED_TRUTH_REFRESH_GENERATION_ID=""
LAUNCH_SUPERVISOR_SHARED_TRUTH_COHERENCE_STATUS=""
LAUNCH_CONTROL_PLANE_SNAPSHOT_ID=""
LAUNCH_CONTROL_PLANE_SNAPSHOT_CLASSIFICATION=""
LAUNCH_CONTROL_PLANE_SNAPSHOT_FILE="${CONTROL_PLANE_SNAPSHOT_FILE}"

ARGS=()
CONFIG_SET=0
SCHWAB_CONFIG_SET=0
BACKGROUND=0
NETWORK_PREFLIGHT_ONLY=0
INCLUDE_ATPE_CANARY=0
INCLUDE_GC_MGC_ACCEPTANCE=0
ENABLE_PAPER_ROUTE_CANARY_FLAG=0
PID_FILE="${DEFAULT_PID_FILE}"
LOG_FILE="${DEFAULT_LOG_FILE}"
CONFIG_PATHS_FILE="${DEFAULT_CONFIG_PATHS_FILE}"
LAUNCH_STATUS_FILE="${DEFAULT_LAUNCH_STATUS_FILE}"

while (($# > 0)); do
  case "$1" in
    --background)
      BACKGROUND=1
      shift
      ;;
    --network-preflight-only)
      NETWORK_PREFLIGHT_ONLY=1
      shift
      ;;
    --include-atpe-canary)
      INCLUDE_ATPE_CANARY=1
      shift
      ;;
    --include-gc-mgc-acceptance)
      INCLUDE_GC_MGC_ACCEPTANCE=1
      shift
      ;;
    --enable-paper-route-canary)
      ENABLE_PAPER_ROUTE_CANARY_FLAG=1
      shift
      ;;
    --pid-file)
      PID_FILE="$2"
      shift 2
      ;;
    --pid-file=*)
      PID_FILE="${1#*=}"
      shift
      ;;
    --log-file)
      LOG_FILE="$2"
      shift 2
      ;;
    --log-file=*)
      LOG_FILE="${1#*=}"
      shift
      ;;
    --config-paths-file)
      CONFIG_PATHS_FILE="$2"
      shift 2
      ;;
    --config-paths-file=*)
      CONFIG_PATHS_FILE="${1#*=}"
      shift
      ;;
    --launch-status-file)
      LAUNCH_STATUS_FILE="$2"
      shift 2
      ;;
    --launch-status-file=*)
      LAUNCH_STATUS_FILE="${1#*=}"
      shift
      ;;
    --config)
      CONFIG_SET=1
      ARGS+=("$1" "$2")
      shift 2
      ;;
    --config=*)
      CONFIG_SET=1
      ARGS+=("$1")
      shift
      ;;
    --schwab-config)
      SCHWAB_CONFIG_SET=1
      ARGS+=("$1" "$2")
      shift 2
      ;;
    --schwab-config=*)
      SCHWAB_CONFIG_SET=1
      ARGS+=("$1")
      shift
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

FINAL_ARGS=()
if [[ ${CONFIG_SET} -eq 0 ]]; then
  CONFIG_PATHS=()
  if [[ -n "${CONFIG_OVERRIDE_RAW}" ]]; then
    IFS=',:' read -r -a CONFIG_OVERRIDE_PARTS <<< "${CONFIG_OVERRIDE_RAW}"
    for config_path in "${CONFIG_OVERRIDE_PARTS[@]}"; do
      config_path="${config_path#"${config_path%%[![:space:]]*}"}"
      config_path="${config_path%"${config_path##*[![:space:]]}"}"
      if [[ -z "${config_path}" ]]; then
        continue
      fi
      if [[ "${config_path}" != /* ]]; then
        config_path="${REPO_ROOT}/${config_path}"
      fi
      CONFIG_PATHS+=("${config_path}")
    done
  fi
  if [[ ${#CONFIG_PATHS[@]} -eq 0 ]] && [[ -f "${CONFIG_PATHS_FILE}" ]]; then
    while IFS= read -r config_path; do
      config_path="${config_path#"${config_path%%[![:space:]]*}"}"
      config_path="${config_path%"${config_path##*[![:space:]]}"}"
      if [[ -z "${config_path}" ]]; then
        continue
      fi
      CONFIG_PATHS+=("${config_path}")
    done < "${CONFIG_PATHS_FILE}"
  fi
  if [[ ${#CONFIG_PATHS[@]} -eq 0 ]]; then
    CONFIG_PATHS=("${DEFAULT_CONFIGS[@]}")
  fi
  for config_path in "${CONFIG_PATHS[@]}"; do
    FINAL_ARGS+=(--config "${config_path}")
  done
  if [[ ${INCLUDE_ATPE_CANARY} -eq 1 ]]; then
    FINAL_ARGS+=(--config "${ATPE_CANARY_CONFIG}")
  fi
  if [[ ${INCLUDE_GC_MGC_ACCEPTANCE} -eq 1 ]]; then
    FINAL_ARGS+=(--config "${GC_MGC_ACCEPTANCE_CONFIG}")
  fi
fi
if [[ ${SCHWAB_CONFIG_SET} -eq 0 && -f "${DEFAULT_SCHWAB_CONFIG}" ]]; then
  FINAL_ARGS+=(--schwab-config "${DEFAULT_SCHWAB_CONFIG}")
fi
if [[ ${#ARGS[@]} -gt 0 ]]; then
  FINAL_ARGS+=("${ARGS[@]}")
fi

if [[ ${ENABLE_PAPER_ROUTE_CANARY_FLAG} -eq 1 ]]; then
  export ENABLE_PAPER_ROUTE_CANARY=true
fi

ensure_dir "${DEFAULT_RUNTIME_DIR}"
if [[ ${ENABLE_PAPER_ROUTE_CANARY_FLAG} -eq 1 ]]; then
  printf 'enabled=true\n' > "${CANARY_ENABLE_SENTINEL}"
else
  rm -f "${CANARY_ENABLE_SENTINEL}"
fi

write_launch_status() {
  local classification="$1"
  local pid="${2:-}"
  local detail="${3:-}"
  local child_exit_code="${4:-}"
  ensure_dir "$(dirname "${LAUNCH_STATUS_FILE}")"
  LAUNCH_STATUS_FILE="${LAUNCH_STATUS_FILE}" \
  LAUNCH_CLASSIFICATION="${classification}" \
  LAUNCH_PID="${pid}" \
  LAUNCH_DETAIL="${detail}" \
  LAUNCH_CHILD_EXIT_CODE="${child_exit_code}" \
  LAUNCH_PID_FILE="${PID_FILE}" \
  LAUNCH_LOG_FILE="${LOG_FILE}" \
  LAUNCH_CONFIG_PATHS_FILE="${CONFIG_PATHS_FILE}" \
  LAUNCH_RUNTIME_TRUTH_FILE="${RUNTIME_TRUTH_FILE}" \
  LAUNCH_REPO_ROOT="${REPO_ROOT}" \
  LAUNCH_CWD="$(pwd)" \
  LAUNCH_PYTHON_BIN="${LAUNCH_PYTHON_BIN}" \
  LAUNCH_BACKGROUND_VERIFY_ATTEMPTS="${BACKGROUND_VERIFY_ATTEMPTS}" \
  LAUNCH_BACKGROUND_VERIFY_POLL_SECONDS="${BACKGROUND_VERIFY_POLL_SECONDS}" \
  LAUNCH_OBSERVATION_WINDOW_SECONDS="${BACKGROUND_OBSERVATION_WINDOW_SECONDS}" \
  LAUNCH_FIRST_TRUTH_GENERATED_AT="${LAUNCH_FIRST_TRUTH_GENERATED_AT}" \
  LAUNCH_SECOND_TRUTH_GENERATED_AT="${LAUNCH_SECOND_TRUTH_GENERATED_AT}" \
  LAUNCH_SUSTAINED_CONVERGENCE_CONFIRMED="${LAUNCH_SUSTAINED_CONVERGENCE_CONFIRMED}" \
  LAUNCH_FINAL_PID_ALIVE="${LAUNCH_FINAL_PID_ALIVE}" \
  LAUNCH_TERMINATED_BY_VERIFIER="${LAUNCH_TERMINATED_BY_VERIFIER}" \
  LAUNCH_TERMINATION_SIGNAL="${LAUNCH_TERMINATION_SIGNAL}" \
  LAUNCH_TERMINATION_REASON="${LAUNCH_TERMINATION_REASON}" \
  LAUNCH_STOP_SOURCE="${LAUNCH_STOP_SOURCE}" \
  LAUNCH_STOP_REQUESTED_AT="${LAUNCH_STOP_REQUESTED_AT}" \
  LAUNCH_STOP_OBSERVED_AT="${LAUNCH_STOP_OBSERVED_AT}" \
  LAUNCH_STOP_EXPECTED_CLEANUP="${LAUNCH_STOP_EXPECTED_CLEANUP}" \
  LAUNCH_STOP_BROKER_SAFE="${LAUNCH_STOP_BROKER_SAFE}" \
  LAUNCH_RUNTIME_SUPERVISOR_AUTHORITY_FILE="${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" \
  LAUNCH_CONTROL_PLANE_SNAPSHOT_FILE="${CONTROL_PLANE_SNAPSHOT_FILE}" \
  LAUNCH_CONTROL_PLANE_SNAPSHOT_ID="${LAUNCH_CONTROL_PLANE_SNAPSHOT_ID}" \
  LAUNCH_CONTROL_PLANE_SNAPSHOT_CLASSIFICATION="${LAUNCH_CONTROL_PLANE_SNAPSHOT_CLASSIFICATION}" \
  LAUNCH_SUPERVISOR_CLASSIFICATION="${LAUNCH_SUPERVISOR_CLASSIFICATION}" \
  LAUNCH_SUPERVISOR_MODE="${LAUNCH_SUPERVISOR_MODE}" \
  LAUNCH_SUPERVISOR_PROOF_WINDOW_STATUS="${LAUNCH_SUPERVISOR_PROOF_WINDOW_STATUS}" \
  LAUNCH_SUPERVISOR_RECOMMENDED_NEXT_COMMAND="${LAUNCH_SUPERVISOR_RECOMMENDED_NEXT_COMMAND}" \
  LAUNCH_SUPERVISOR_OPERATOR_ACK_REQUIRED="${LAUNCH_SUPERVISOR_OPERATOR_ACK_REQUIRED}" \
  LAUNCH_SUPERVISOR_PAPER_ACTION_POLICY="${LAUNCH_SUPERVISOR_PAPER_ACTION_POLICY}" \
  LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_ALLOWED="${LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_ALLOWED}" \
  LAUNCH_SUPERVISOR_REQUIRES_OPERATOR_ACK_FOR_PAPER="${LAUNCH_SUPERVISOR_REQUIRES_OPERATOR_ACK_FOR_PAPER}" \
  LAUNCH_SUPERVISOR_OPERATOR_ACK_ADVISORY_ONLY_FOR_PAPER="${LAUNCH_SUPERVISOR_OPERATOR_ACK_ADVISORY_ONLY_FOR_PAPER}" \
  LAUNCH_SUPERVISOR_LIVE_ACTION_POLICY="${LAUNCH_SUPERVISOR_LIVE_ACTION_POLICY}" \
  LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_PLAN_CLASSIFICATION="${LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_PLAN_CLASSIFICATION}" \
  LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_NEXT_ACTION="${LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_NEXT_ACTION}" \
  LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_EXECUTION_ENABLED="${LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_EXECUTION_ENABLED}" \
  "${PYTHON_BIN}" -c '
import json
import os
from datetime import datetime, timezone
from pathlib import Path

def _git_head(repo_root):
    try:
        git_dir = Path(repo_root) / ".git"
        head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
        if head.startswith("ref: "):
            ref = git_dir / head.removeprefix("ref: ").strip()
            return ref.read_text(encoding="utf-8").strip() if ref.exists() else None
        return head or None
    except OSError:
        return None

config_paths = [item for item in os.environ.get("LAUNCH_FINAL_ARGS", "").split(os.pathsep) if item]
generated_at = datetime.now(timezone.utc).isoformat()
stop_reason = os.environ.get("LAUNCH_TERMINATION_REASON") or None
payload = {
    "generated_at": generated_at,
    "classification": os.environ["LAUNCH_CLASSIFICATION"],
    "pid": int(os.environ["LAUNCH_PID"]) if os.environ.get("LAUNCH_PID", "").isdigit() else None,
    "pid_file": os.environ["LAUNCH_PID_FILE"],
    "log_file": os.environ["LAUNCH_LOG_FILE"],
    "config_paths_file": os.environ["LAUNCH_CONFIG_PATHS_FILE"],
    "runtime_truth_file": os.environ["LAUNCH_RUNTIME_TRUTH_FILE"],
    "repo_root": os.environ["LAUNCH_REPO_ROOT"],
    "cwd": os.environ["LAUNCH_CWD"],
    "python_bin": os.environ["LAUNCH_PYTHON_BIN"],
    "detail": os.environ.get("LAUNCH_DETAIL") or None,
    "child_exit_code": int(os.environ["LAUNCH_CHILD_EXIT_CODE"]) if os.environ.get("LAUNCH_CHILD_EXIT_CODE", "").isdigit() else None,
    "background_verify_attempts": int(os.environ["LAUNCH_BACKGROUND_VERIFY_ATTEMPTS"]),
    "background_verify_poll_seconds": float(os.environ["LAUNCH_BACKGROUND_VERIFY_POLL_SECONDS"]),
    "observation_window_seconds": float(os.environ["LAUNCH_OBSERVATION_WINDOW_SECONDS"]),
    "first_truth_generated_at": os.environ.get("LAUNCH_FIRST_TRUTH_GENERATED_AT") or None,
    "second_truth_generated_at": os.environ.get("LAUNCH_SECOND_TRUTH_GENERATED_AT") or None,
    "sustained_convergence_confirmed": os.environ.get("LAUNCH_SUSTAINED_CONVERGENCE_CONFIRMED", "").lower() == "true",
    "final_pid_alive": os.environ.get("LAUNCH_FINAL_PID_ALIVE", "").lower() == "true",
    "terminated_by_launch_verifier": os.environ.get("LAUNCH_TERMINATED_BY_VERIFIER", "").lower() == "true",
    "termination_signal": os.environ.get("LAUNCH_TERMINATION_SIGNAL") or None,
    "termination_reason": stop_reason,
    "config_paths": config_paths,
    "paper_only": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "submit_authority": False,
    "runtime_supervisor_authority": {
        "artifact_path": os.environ["LAUNCH_RUNTIME_SUPERVISOR_AUTHORITY_FILE"],
        "classification": os.environ.get("LAUNCH_SUPERVISOR_CLASSIFICATION") or None,
        "supervisor_mode": os.environ.get("LAUNCH_SUPERVISOR_MODE") or None,
        "proof_window_status": os.environ.get("LAUNCH_SUPERVISOR_PROOF_WINDOW_STATUS") or None,
        "recommended_next_command": os.environ.get("LAUNCH_SUPERVISOR_RECOMMENDED_NEXT_COMMAND") or None,
        "operator_ack_required": os.environ.get("LAUNCH_SUPERVISOR_OPERATOR_ACK_REQUIRED", "").lower() == "true",
        "paper_action_policy": os.environ.get("LAUNCH_SUPERVISOR_PAPER_ACTION_POLICY") or None,
        "autonomous_recovery_allowed": os.environ.get("LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_ALLOWED", "").lower() == "true",
        "requires_operator_ack_for_paper": os.environ.get("LAUNCH_SUPERVISOR_REQUIRES_OPERATOR_ACK_FOR_PAPER", "").lower() == "true",
        "operator_ack_advisory_only_for_paper": os.environ.get("LAUNCH_SUPERVISOR_OPERATOR_ACK_ADVISORY_ONLY_FOR_PAPER", "").lower() == "true",
        "live_action_policy": os.environ.get("LAUNCH_SUPERVISOR_LIVE_ACTION_POLICY") or None,
        "autonomous_recovery_plan_classification": os.environ.get("LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_PLAN_CLASSIFICATION") or None,
        "autonomous_recovery_next_action": os.environ.get("LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_NEXT_ACTION") or None,
        "autonomous_recovery_execution_enabled": os.environ.get("LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_EXECUTION_ENABLED", "").lower() == "true",
        "shared_truth_refresh_generation_id": os.environ.get("LAUNCH_SUPERVISOR_SHARED_TRUTH_REFRESH_GENERATION_ID") or None,
        "shared_truth_coherence_status": os.environ.get("LAUNCH_SUPERVISOR_SHARED_TRUTH_COHERENCE_STATUS") or None,
    },
    "control_plane_snapshot": {
        "artifact_path": os.environ["LAUNCH_CONTROL_PLANE_SNAPSHOT_FILE"],
        "control_plane_snapshot_id": os.environ.get("LAUNCH_CONTROL_PLANE_SNAPSHOT_ID") or None,
        "classification": os.environ.get("LAUNCH_CONTROL_PLANE_SNAPSHOT_CLASSIFICATION") or None,
        "shared_truth_refresh_generation_id": os.environ.get("LAUNCH_SUPERVISOR_SHARED_TRUTH_REFRESH_GENERATION_ID") or None,
        "shared_truth_coherence_status": os.environ.get("LAUNCH_SUPERVISOR_SHARED_TRUTH_COHERENCE_STATUS") or None,
        "runtime_supervisor_classification": os.environ.get("LAUNCH_SUPERVISOR_CLASSIFICATION") or None,
        "supervisor_mode": os.environ.get("LAUNCH_SUPERVISOR_MODE") or None,
        "proof_window_status": os.environ.get("LAUNCH_SUPERVISOR_PROOF_WINDOW_STATUS") or None,
        "paper_recovery_policy": os.environ.get("LAUNCH_SUPERVISOR_PAPER_ACTION_POLICY") or None,
        "autonomous_recovery_plan_classification": os.environ.get("LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_PLAN_CLASSIFICATION") or None,
        "autonomous_recovery_next_action": os.environ.get("LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_NEXT_ACTION") or None,
        "autonomous_recovery_execution_enabled": os.environ.get("LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_EXECUTION_ENABLED", "").lower() == "true",
        "recommended_next_command": os.environ.get("LAUNCH_SUPERVISOR_RECOMMENDED_NEXT_COMMAND") or None,
    },
}
if os.environ.get("LAUNCH_STOP_SOURCE") or stop_reason:
    payload["stop_provenance"] = {
        "stop_source": os.environ.get("LAUNCH_STOP_SOURCE") or "unknown",
        "stop_reason": stop_reason,
        "runtime_instance_id": os.environ.get("MGC_TRACK_B_RUNTIME_INSTANCE_ID") or None,
        "restart_generation": int(os.environ.get("MGC_TRACK_B_PAPER_RUNTIME_RESTART_GENERATION") or os.environ.get("MGC_TRACK_B_RESTART_GENERATION") or 0),
        "source_commit": _git_head(os.environ["LAUNCH_REPO_ROOT"]),
        "control_action_id": None,
        "requested_at": os.environ.get("LAUNCH_STOP_REQUESTED_AT") or generated_at,
        "observed_at": os.environ.get("LAUNCH_STOP_OBSERVED_AT") or generated_at,
        "expected_cleanup": os.environ.get("LAUNCH_STOP_EXPECTED_CLEANUP", "").lower() == "true",
        "broker_safe_at_stop": os.environ.get("LAUNCH_STOP_BROKER_SAFE", "").lower() == "true",
    }
with open(os.environ["LAUNCH_STATUS_FILE"], "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2, sort_keys=True)
    fh.write("\n")
'
}

run_shared_truth_runtime_start_preflight() {
  # Legacy fallback only. The launch hot path uses run_control_plane_snapshot_start_preflight
  # so Shared Truth Refresh and Runtime Supervisor Authority share one coherent generation.
  local tmp_file="${SHARED_TRUTH_PREFLIGHT_FILE}.$$.${RANDOM}.tmp"
  local stderr_file="${SHARED_TRUTH_PREFLIGHT_FILE}.stderr.log"
  local status=0
  local summary=""
  ensure_dir "$(dirname "${SHARED_TRUTH_PREFLIGHT_FILE}")"
  set +e
  "${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_shared_truth_refresh_cli \
    --repo-root "${SHARED_TRUTH_PREFLIGHT_REPO_ROOT}" \
    --json \
    --runtime-start-preflight \
    > "${tmp_file}" \
    2> "${stderr_file}"
  status=$?
  set -e
  if [[ -s "${tmp_file}" ]]; then
    mv "${tmp_file}" "${SHARED_TRUTH_PREFLIGHT_FILE}"
  else
    rm -f "${tmp_file}"
  fi
  summary="$("${PYTHON_BIN}" - <<'PY' "${SHARED_TRUTH_PREFLIGHT_FILE}" "${stderr_file}" || true
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
stderr_path = Path(sys.argv[2])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    stderr = ""
    try:
        stderr = stderr_path.read_text(encoding="utf-8").strip()
    except OSError:
        pass
    print(f"Shared Truth runtime-start preflight did not produce valid JSON. stderr={stderr[:500]}")
    raise SystemExit(0)

preflight = payload.get("runtime_start_preflight") or {}
classification = preflight.get("classification") or "SHARED_TRUTH_PREFLIGHT_MISSING"
classifications = payload.get("classifications") or {}
parts = [f"{key}={value}" for key, value in classifications.items()]
blockers = preflight.get("blockers") or []
if blockers:
    blocker_text = "; ".join(str(item.get("detail") or item.get("code")) for item in blockers if isinstance(item, dict))
    print(f"{classification}: {', '.join(parts)}. blockers={blocker_text}")
else:
    print(f"{classification}: {', '.join(parts)}")
PY
)"
  echo "Track B shared-truth runtime-start preflight: ${summary}"
  if [[ ${status} -ne 0 ]]; then
    if [[ -s "${stderr_file}" ]]; then
      cat "${stderr_file}" >&2
    fi
    write_launch_status "SHARED_TRUTH_PREFLIGHT_BLOCKED" "" "${summary}" "${status}"
    return "${status}"
  fi
  rm -f "${stderr_file}"
  return 0
}

run_runtime_supervisor_start_preflight() {
  # Legacy fallback only. The launch hot path gates on Control Plane Snapshot.
  local tmp_file="${RUNTIME_SUPERVISOR_AUTHORITY_FILE}.runtime_start.$$.${RANDOM}.tmp"
  local stderr_file="${RUNTIME_SUPERVISOR_AUTHORITY_FILE}.runtime_start.stderr.log"
  local status=0
  local summary=""
  ensure_dir "$(dirname "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}")"
  set +e
  "${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_runtime_supervisor_authority \
    --repo-root "${REPO_ROOT}" \
    --output-path "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" \
    --no-dashboard-projection \
    --json \
    > "${tmp_file}" \
    2> "${stderr_file}"
  status=$?
  set -e
  if [[ -s "${tmp_file}" ]]; then
    mv "${tmp_file}" "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}"
  else
    rm -f "${tmp_file}"
  fi
  summary="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" "${stderr_file}" || true
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
stderr_path = Path(sys.argv[2])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    stderr = ""
    try:
        stderr = stderr_path.read_text(encoding="utf-8").strip()
    except OSError:
        pass
    print(f"Runtime Supervisor Authority v2 did not produce valid JSON. stderr={stderr[:500]}")
    raise SystemExit(0)

operator_ack = payload.get("operator_ack") if isinstance(payload.get("operator_ack"), dict) else {}
evidence = payload.get("evidence_summary") if isinstance(payload.get("evidence_summary"), dict) else {}
paper_action_policy = evidence.get("paper_action_policy")
autonomous_recovery_allowed = evidence.get("paper_autonomous_recovery_allowed") is True
requires_operator_ack_for_paper = evidence.get("paper_requires_operator_ack") is True
live_action_policy = evidence.get("paper_live_action_policy")
autonomous_plan_classification = payload.get("autonomous_recovery_plan_classification")
autonomous_next_action = payload.get("autonomous_recovery_next_action")
autonomous_execution_enabled = payload.get("autonomous_recovery_execution_enabled") is True
shared_truth_generation_id = payload.get("shared_truth_refresh_generation_id")
shared_truth_coherence_status = payload.get("shared_truth_coherence_status")
operator_ack_required = payload.get("operator_ack_required") is True or operator_ack.get("required") is True
operator_ack_advisory_only_for_paper = bool(operator_ack_required and not requires_operator_ack_for_paper and paper_action_policy)
print(
    "classification={classification} supervisor_mode={mode} proof_window_status={window} "
    "safe_to_start_runtime={safe} operator_ack_required={ack} "
    "paper_action_policy={paper_action_policy} autonomous_recovery_allowed={autonomous} "
    "requires_operator_ack_for_paper={paper_ack} operator_ack_advisory_only_for_paper={advisory} "
    "autonomous_recovery_plan_classification={plan_classification} "
    "autonomous_recovery_next_action={plan_action} "
    "autonomous_recovery_execution_enabled={plan_execution_enabled} "
    "shared_truth_refresh_generation_id={shared_truth_generation_id} "
    "shared_truth_coherence_status={shared_truth_coherence_status} "
    "live_action_policy={live_action_policy} recommended_next_command={command}".format(
        classification=payload.get("classification"),
        mode=payload.get("supervisor_mode"),
        window=payload.get("proof_window_status"),
        safe=payload.get("safe_to_start_runtime"),
        ack=operator_ack_required,
        paper_action_policy=paper_action_policy,
        autonomous=autonomous_recovery_allowed,
        paper_ack=requires_operator_ack_for_paper,
        advisory=operator_ack_advisory_only_for_paper,
        plan_classification=autonomous_plan_classification,
        plan_action=autonomous_next_action,
        plan_execution_enabled=autonomous_execution_enabled,
        shared_truth_generation_id=shared_truth_generation_id,
        shared_truth_coherence_status=shared_truth_coherence_status,
        live_action_policy=live_action_policy,
        command=payload.get("recommended_next_command"),
    )
)
PY
)"
  echo "Track B Runtime Supervisor Authority v2 start preflight: ${summary}"
  if [[ ${status} -ne 0 ]] && [[ -s "${stderr_file}" ]]; then
    cat "${stderr_file}" >&2
  fi
  rm -f "${stderr_file}"
  set +e
  "${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}"
import json
import os
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
ack_required = payload.get("operator_ack_required") is True or operator_ack.get("required") is True
paper_action_policy = evidence.get("paper_action_policy")
autonomous_recovery_allowed = evidence.get("paper_autonomous_recovery_allowed") is True
requires_operator_ack_for_paper = evidence.get("paper_requires_operator_ack") is True
live_action_policy = evidence.get("paper_live_action_policy")
autonomous_plan_classification = payload.get("autonomous_recovery_plan_classification")
autonomous_next_action = payload.get("autonomous_recovery_next_action")
autonomous_execution_enabled = payload.get("autonomous_recovery_execution_enabled") is True
shared_truth_generation_id = payload.get("shared_truth_refresh_generation_id")
shared_truth_coherence_status = payload.get("shared_truth_coherence_status")
operator_ack_advisory_only_for_paper = bool(ack_required and not requires_operator_ack_for_paper and paper_action_policy)
os.environ["MGC_RUNTIME_SUPERVISOR_CLASSIFICATION"] = str(classification or "")
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
  local gate_rc=$?
  set -e
  LAUNCH_SUPERVISOR_CLASSIFICATION="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" classification || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
print(payload.get(sys.argv[2]) or "")
PY
)"
  LAUNCH_SUPERVISOR_MODE="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" supervisor_mode || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
print(payload.get(sys.argv[2]) or "")
PY
)"
  LAUNCH_SUPERVISOR_PROOF_WINDOW_STATUS="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" proof_window_status || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
print(payload.get(sys.argv[2]) or "")
PY
)"
  LAUNCH_SUPERVISOR_RECOMMENDED_NEXT_COMMAND="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" recommended_next_command || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
print(payload.get(sys.argv[2]) or "")
PY
)"
  LAUNCH_SUPERVISOR_OPERATOR_ACK_REQUIRED="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
operator_ack = payload.get("operator_ack") if isinstance(payload.get("operator_ack"), dict) else {}
print("true" if payload.get("operator_ack_required") is True or operator_ack.get("required") is True else "false")
PY
)"
  LAUNCH_SUPERVISOR_PAPER_ACTION_POLICY="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" paper_action_policy || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
evidence = payload.get("evidence_summary") if isinstance(payload.get("evidence_summary"), dict) else {}
print(evidence.get(sys.argv[2]) or "")
PY
)"
  LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_ALLOWED="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
evidence = payload.get("evidence_summary") if isinstance(payload.get("evidence_summary"), dict) else {}
print("true" if evidence.get("paper_autonomous_recovery_allowed") is True else "false")
PY
)"
  LAUNCH_SUPERVISOR_REQUIRES_OPERATOR_ACK_FOR_PAPER="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
evidence = payload.get("evidence_summary") if isinstance(payload.get("evidence_summary"), dict) else {}
print("true" if evidence.get("paper_requires_operator_ack") is True else "false")
PY
)"
  LAUNCH_SUPERVISOR_OPERATOR_ACK_ADVISORY_ONLY_FOR_PAPER="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
operator_ack = payload.get("operator_ack") if isinstance(payload.get("operator_ack"), dict) else {}
evidence = payload.get("evidence_summary") if isinstance(payload.get("evidence_summary"), dict) else {}
ack_required = payload.get("operator_ack_required") is True or operator_ack.get("required") is True
paper_action_policy = evidence.get("paper_action_policy")
requires_operator_ack_for_paper = evidence.get("paper_requires_operator_ack") is True
print("true" if ack_required and not requires_operator_ack_for_paper and paper_action_policy else "false")
PY
)"
  LAUNCH_SUPERVISOR_LIVE_ACTION_POLICY="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" paper_live_action_policy || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
evidence = payload.get("evidence_summary") if isinstance(payload.get("evidence_summary"), dict) else {}
print(evidence.get(sys.argv[2]) or "")
PY
)"
  LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_PLAN_CLASSIFICATION="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" autonomous_recovery_plan_classification || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
print(payload.get(sys.argv[2]) or "")
PY
)"
  LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_NEXT_ACTION="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" autonomous_recovery_next_action || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
print(payload.get(sys.argv[2]) or "")
PY
)"
  LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_EXECUTION_ENABLED="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
print("true" if payload.get("autonomous_recovery_execution_enabled") is True else "false")
PY
)"
  LAUNCH_SUPERVISOR_SHARED_TRUTH_REFRESH_GENERATION_ID="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" shared_truth_refresh_generation_id || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
print(payload.get(sys.argv[2]) or "")
PY
)"
  LAUNCH_SUPERVISOR_SHARED_TRUTH_COHERENCE_STATUS="$("${PYTHON_BIN}" - <<'PY' "${RUNTIME_SUPERVISOR_AUTHORITY_FILE}" shared_truth_coherence_status || true
import json, sys
from pathlib import Path
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
print(payload.get(sys.argv[2]) or "")
PY
)"
  if [[ ${gate_rc} -ne 0 ]]; then
    write_launch_status "RUNTIME_SUPERVISOR_START_BLOCKED" "" "${summary}" "${gate_rc}"
    return "${gate_rc}"
  fi
  return 0
}

run_control_plane_snapshot_start_preflight() {
  local tmp_file="${CONTROL_PLANE_SNAPSHOT_FILE}.runtime_start.$$.${RANDOM}.tmp"
  local stderr_file="${CONTROL_PLANE_SNAPSHOT_FILE}.runtime_start.stderr.log"
  local status=0
  local summary=""
  ensure_dir "$(dirname "${CONTROL_PLANE_SNAPSHOT_FILE}")"
  set +e
  "${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_control_plane_snapshot \
    --repo-root "${REPO_ROOT}" \
    --output-path "${CONTROL_PLANE_SNAPSHOT_FILE}" \
    --no-dashboard-projection \
    --no-broker-lease-history \
    --json \
    > "${tmp_file}" \
    2> "${stderr_file}"
  status=$?
  set -e
  if [[ -s "${tmp_file}" ]]; then
    mv "${tmp_file}" "${CONTROL_PLANE_SNAPSHOT_FILE}"
  else
    rm -f "${tmp_file}"
  fi
  summary="$("${PYTHON_BIN}" - <<'PY' "${CONTROL_PLANE_SNAPSHOT_FILE}" "${stderr_file}" || true
import json
import sys
from pathlib import Path
from mgc_v05l.execution_core.track_b_control_plane_snapshot_status import classify_control_plane_snapshot_status

path = Path(sys.argv[1])
stderr_path = Path(sys.argv[2])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    stderr = ""
    try:
        stderr = stderr_path.read_text(encoding="utf-8").strip()
    except OSError:
        pass
    print(f"Control Plane Snapshot did not produce valid JSON. stderr={stderr[:500]}")
    raise SystemExit(0)

status = classify_control_plane_snapshot_status(payload, required_for_launch=True)
print(
    "control_plane_status_classification={status_classification} diagnostic_only={diagnostic_only} "
    "not_routing_authority={not_routing_authority} snapshot_id={snapshot_id} classification={classification} "
    "shared_truth_refresh_generation_id={generation_id} shared_truth_coherence_status={coherence} "
    "runtime_supervisor_classification={supervisor_classification} supervisor_mode={mode} "
    "proof_window_status={window} safe_to_start_runtime={safe} "
    "paper_recovery_policy={paper_policy} autonomous_recovery_plan_classification={plan_classification} "
    "autonomous_recovery_next_action={plan_action} autonomous_recovery_execution_enabled={plan_enabled} "
    "recommended_next_command={command}".format(
        status_classification=status.get("classification"),
        diagnostic_only=status.get("diagnostic_only"),
        not_routing_authority=status.get("not_routing_authority"),
        snapshot_id=payload.get("control_plane_snapshot_id"),
        classification=payload.get("classification"),
        generation_id=payload.get("shared_truth_refresh_generation_id"),
        coherence=payload.get("shared_truth_coherence_status"),
        supervisor_classification=payload.get("runtime_supervisor_classification"),
        mode=payload.get("supervisor_mode"),
        window=payload.get("proof_window_status"),
        safe=payload.get("safe_to_start_runtime"),
        paper_policy=payload.get("paper_recovery_policy"),
        plan_classification=payload.get("autonomous_recovery_plan_classification"),
        plan_action=payload.get("autonomous_recovery_next_action"),
        plan_enabled=payload.get("autonomous_recovery_execution_enabled"),
        command=payload.get("recommended_next_command"),
    )
)
PY
)"
  echo "Track B Control Plane Snapshot start preflight: ${summary}"
  if [[ ${status} -ne 0 ]] && [[ -s "${stderr_file}" ]]; then
    cat "${stderr_file}" >&2
  fi
  rm -f "${stderr_file}"
  set +e
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
  local gate_rc=$?
  set -e
  read_control_plane_snapshot_launch_fields
  if [[ ${gate_rc} -ne 0 ]]; then
    write_launch_status "CONTROL_PLANE_SNAPSHOT_START_BLOCKED" "" "${summary}" "${gate_rc}"
    return "${gate_rc}"
  fi
  return 0
}

read_control_plane_snapshot_launch_fields() {
  local values
  values="$("${PYTHON_BIN}" - <<'PY' "${CONTROL_PLANE_SNAPSHOT_FILE}" || true
import json
import shlex
import sys
from pathlib import Path

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}

fields = {
    "LAUNCH_CONTROL_PLANE_SNAPSHOT_ID": payload.get("control_plane_snapshot_id"),
    "LAUNCH_CONTROL_PLANE_SNAPSHOT_CLASSIFICATION": payload.get("classification"),
    "LAUNCH_SUPERVISOR_CLASSIFICATION": payload.get("runtime_supervisor_classification"),
    "LAUNCH_SUPERVISOR_MODE": payload.get("supervisor_mode"),
    "LAUNCH_SUPERVISOR_PROOF_WINDOW_STATUS": payload.get("proof_window_status"),
    "LAUNCH_SUPERVISOR_RECOMMENDED_NEXT_COMMAND": payload.get("recommended_next_command"),
    "LAUNCH_SUPERVISOR_PAPER_ACTION_POLICY": payload.get("paper_recovery_policy"),
    "LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_PLAN_CLASSIFICATION": payload.get("autonomous_recovery_plan_classification"),
    "LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_NEXT_ACTION": payload.get("autonomous_recovery_next_action"),
    "LAUNCH_SUPERVISOR_SHARED_TRUTH_REFRESH_GENERATION_ID": payload.get("shared_truth_refresh_generation_id"),
    "LAUNCH_SUPERVISOR_SHARED_TRUTH_COHERENCE_STATUS": payload.get("shared_truth_coherence_status"),
}
for key, value in fields.items():
    print(f"{key}={shlex.quote(str(value or ''))}")
print("LAUNCH_SUPERVISOR_OPERATOR_ACK_REQUIRED=false")
print("LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_ALLOWED=false")
print("LAUNCH_SUPERVISOR_REQUIRES_OPERATOR_ACK_FOR_PAPER=false")
print("LAUNCH_SUPERVISOR_OPERATOR_ACK_ADVISORY_ONLY_FOR_PAPER=false")
print("LAUNCH_SUPERVISOR_LIVE_ACTION_POLICY=")
print(f"LAUNCH_SUPERVISOR_AUTONOMOUS_RECOVERY_EXECUTION_ENABLED={'true' if payload.get('autonomous_recovery_execution_enabled') is True else 'false'}")
PY
)"
  eval "${values}"
}

background_child_stayed_alive() {
  local pid="$1"
  local launched_at_epoch="$2"
  local first_truth=""
  local second_truth=""
  local first_truth_seen_epoch=""
  local attempt=0
  while [[ ${attempt} -lt ${BACKGROUND_VERIFY_ATTEMPTS} ]]; do
    if ! process_alive_not_zombie "${pid}"; then
      LAUNCH_FINAL_PID_ALIVE="false"
      return 1
    fi
    if runtime_process_identity_matches "${pid}"; then
      truth_generated_at="$(runtime_truth_generated_at_for_pid "${pid}" "${launched_at_epoch}" || true)"
      if [[ -n "${truth_generated_at}" ]]; then
        if [[ -z "${first_truth}" ]]; then
          first_truth="${truth_generated_at}"
          first_truth_seen_epoch="$(date +%s)"
          LAUNCH_FIRST_TRUTH_GENERATED_AT="${first_truth}"
        elif [[ "${truth_generated_at}" != "${first_truth}" ]]; then
          second_truth="${truth_generated_at}"
          LAUNCH_SECOND_TRUTH_GENERATED_AT="${second_truth}"
          if (( $(date +%s) - first_truth_seen_epoch >= BACKGROUND_OBSERVATION_WINDOW_SECONDS )); then
            if process_alive_not_zombie "${pid}" && runtime_process_identity_matches "${pid}"; then
              LAUNCH_SUSTAINED_CONVERGENCE_CONFIRMED="true"
              LAUNCH_FINAL_PID_ALIVE="true"
              return 0
            fi
            LAUNCH_FINAL_PID_ALIVE="false"
            return 1
          fi
        fi
      fi
    fi
    sleep "${BACKGROUND_VERIFY_POLL_SECONDS}"
    attempt=$((attempt + 1))
  done
  if process_alive_not_zombie "${pid}"; then
    LAUNCH_FINAL_PID_ALIVE="true"
  else
    LAUNCH_FINAL_PID_ALIVE="false"
  fi
  return 1
}

runtime_truth_generated_at_for_pid() {
  local pid="$1"
  local launched_at_epoch="$2"
  if [[ ! -f "${RUNTIME_TRUTH_FILE}" ]]; then
    return 1
  fi
  "${PYTHON_BIN}" - <<'PY' "${RUNTIME_TRUTH_FILE}" "${pid}" "${launched_at_epoch}"
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

path = Path(sys.argv[1])
expected_pid = int(sys.argv[2])
launched_at_epoch = float(sys.argv[3])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    raise SystemExit(1)
try:
    producer_pid = int(payload.get("producer_pid"))
except (TypeError, ValueError):
    raise SystemExit(1)
if producer_pid != expected_pid:
    raise SystemExit(1)
generated_raw = str(payload.get("generated_at") or "")
try:
    generated = datetime.fromisoformat(generated_raw.replace("Z", "+00:00"))
except ValueError:
    raise SystemExit(1)
if generated.tzinfo is None:
    generated = generated.replace(tzinfo=timezone.utc)
if generated.timestamp() < launched_at_epoch:
    raise SystemExit(1)
if payload.get("heartbeat_state") != "HEALTHY":
    raise SystemExit(1)
if payload.get("freshness_state") != "FRESH":
    raise SystemExit(1)
if payload.get("writer_authority") != "SINGLE_WRITER":
    raise SystemExit(1)
duplicate_writer_detection = payload.get("duplicate_writer_detection")
if isinstance(duplicate_writer_detection, dict) and duplicate_writer_detection.get("duplicate_writer_detected"):
    raise SystemExit(1)
if payload.get("runtime_mode") != "PAPER":
    raise SystemExit(1)
if payload.get("live_money_eligible") is not False:
    raise SystemExit(1)
print(generated.astimezone(timezone.utc).isoformat())
raise SystemExit(0)
PY
}

runtime_process_identity_matches() {
  local pid="$1"
  "${PYTHON_BIN}" - <<'PY' "${pid}" "${REPO_ROOT}"
import subprocess
import sys
from pathlib import Path

pid = sys.argv[1]
repo_root = Path(sys.argv[2]).resolve()
try:
    command = subprocess.check_output(["ps", "-p", pid, "-o", "command="], text=True).strip()
except (OSError, subprocess.CalledProcessError):
    raise SystemExit(1)
if "mgc_v05l.app.main" not in command or "probationary-paper-soak" not in command:
    raise SystemExit(1)
try:
    cwd_output = subprocess.check_output(["lsof", "-a", "-p", pid, "-d", "cwd", "-Fn"], text=True).strip()
except (OSError, subprocess.CalledProcessError):
    raise SystemExit(1)
cwd_rows = [line[1:] for line in cwd_output.splitlines() if line.startswith("n")]
if not cwd_rows:
    raise SystemExit(1)
try:
    cwd = Path(cwd_rows[-1]).resolve()
except OSError:
    raise SystemExit(1)
if cwd != repo_root:
    raise SystemExit(1)
raise SystemExit(0)
PY
}

background_child_reached_preflight() {
  local pid="$1"
  [[ -f "${LOG_FILE}" ]] && grep -q "\"pid\": ${pid}" "${LOG_FILE}" && grep -q "Probationary paper runtime artifact preflight" "${LOG_FILE}"
}

process_alive_not_zombie() {
  local pid="$1"
  local stat
  if ! kill -0 "${pid}" 2>/dev/null; then
    return 1
  fi
  stat="$(ps -p "${pid}" -o stat= 2>/dev/null || true)"
  if [[ "${stat}" == Z* ]]; then
    return 1
  fi
  return 0
}

remove_pid_file_if_matches() {
  local pid="$1"
  if [[ -f "${PID_FILE}" ]] && [[ "$(cat "${PID_FILE}")" == "${pid}" ]]; then
    rm -f "${PID_FILE}"
  fi
}

persist_runtime_config_paths() {
  local path
  local persisted=()
  local index=0
  while [[ ${index} -lt ${#FINAL_ARGS[@]} ]]; do
    local arg="${FINAL_ARGS[${index}]}"
    if [[ "${arg}" == "--config" ]]; then
      index=$((index + 1))
      if [[ ${index} -lt ${#FINAL_ARGS[@]} ]]; then
        path="${FINAL_ARGS[${index}]}"
        [[ -n "${path}" ]] && persisted+=("${path}")
      fi
    elif [[ "${arg}" == --config=* ]]; then
      path="${arg#*=}"
      [[ -n "${path}" ]] && persisted+=("${path}")
    fi
    index=$((index + 1))
  done
  ensure_dir "$(dirname "${CONFIG_PATHS_FILE}")"
  : > "${CONFIG_PATHS_FILE}"
  for path in "${persisted[@]}"; do
    printf '%s\n' "${path}" >> "${CONFIG_PATHS_FILE}"
  done
}

echo "Launching probationary paper soak with repo bootstrap."
if [[ -f "${DEFAULT_SCHWAB_CONFIG}" ]]; then
  echo "Schwab config: ${DEFAULT_SCHWAB_CONFIG}"
else
  echo "Schwab config: optional fallback unavailable"
fi
echo "Paper configs:"
if [[ ${CONFIG_SET} -eq 0 ]]; then
  for config_path in "${CONFIG_PATHS[@]}"; do
    echo "  - ${config_path}"
  done
  if [[ ${INCLUDE_ATPE_CANARY} -eq 1 ]]; then
    echo "  - ${ATPE_CANARY_CONFIG}"
  fi
  if [[ ${INCLUDE_GC_MGC_ACCEPTANCE} -eq 1 ]]; then
    echo "  - ${GC_MGC_ACCEPTANCE_CONFIG}"
  fi
else
  echo "  - custom --config args supplied"
fi

if schwab_runtime_dependency_required && [[ -f "${DEFAULT_SCHWAB_CONFIG}" ]]; then
  runtime_network_resolution_preflight "${DEFAULT_SCHWAB_CONFIG}" "probationary-paper-soak-launch"
fi
persist_runtime_config_paths
run_control_plane_snapshot_start_preflight

LAUNCH_CONFIG_PATHS=()
index=0
while [[ ${index} -lt ${#FINAL_ARGS[@]} ]]; do
  arg="${FINAL_ARGS[${index}]}"
  if [[ "${arg}" == "--config" ]]; then
    index=$((index + 1))
    if [[ ${index} -lt ${#FINAL_ARGS[@]} ]]; then
      LAUNCH_CONFIG_PATHS+=("${FINAL_ARGS[${index}]}")
    fi
  elif [[ "${arg}" == --config=* ]]; then
    LAUNCH_CONFIG_PATHS+=("${arg#*=}")
  fi
  index=$((index + 1))
done
LAUNCH_FINAL_ARGS="$(IFS=":"; printf "%s" "${LAUNCH_CONFIG_PATHS[*]:-}")"
export LAUNCH_FINAL_ARGS

if [[ ${NETWORK_PREFLIGHT_ONLY} -eq 1 ]]; then
  echo "Runtime network preflight completed; skipping probationary paper soak launch."
  exit 0
fi

if [[ ${BACKGROUND} -eq 1 ]]; then
  ensure_dir "$(dirname "${PID_FILE}")"
  ensure_dir "$(dirname "${LOG_FILE}")"
  if [[ -f "${PID_FILE}" ]]; then
    existing_pid="$(cat "${PID_FILE}")"
    if [[ -n "${existing_pid}" ]] && kill -0 "${existing_pid}" 2>/dev/null; then
      echo "Probationary paper soak already running with PID ${existing_pid} (${PID_FILE})." >&2
      exit 1
    fi
  fi
  launch_started_epoch="$(date +%s)"
  nohup "${LAUNCH_PYTHON_BIN}" -m mgc_v05l.app.main probationary-paper-soak "${FINAL_ARGS[@]}" >> "${LOG_FILE}" 2>&1 &
  paper_pid=$!
  if background_child_stayed_alive "${paper_pid}" "${launch_started_epoch}"; then
    echo "${paper_pid}" > "${PID_FILE}"
    write_launch_status "PROBATIONARY_PAPER_BACKGROUND_STARTED" "${paper_pid}" "background child produced fresh runtime truth and remained alive through launch verification" ""
    echo "Probationary paper soak running in background."
    echo "PID: ${paper_pid}"
    echo "PID file: ${PID_FILE}"
    echo "Log file: ${LOG_FILE}"
    echo "Launch status: ${LAUNCH_STATUS_FILE}"
    exit 0
  fi
  child_exit_code=""
  if process_alive_not_zombie "${paper_pid}"; then
    LAUNCH_TERMINATED_BY_VERIFIER="true"
    LAUNCH_TERMINATION_SIGNAL="TERM"
    LAUNCH_TERMINATION_REASON="launch_verifier_timeout_before_sustained_runtime_truth"
    LAUNCH_STOP_SOURCE="launcher"
    LAUNCH_STOP_REQUESTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    LAUNCH_STOP_EXPECTED_CLEANUP="true"
    LAUNCH_STOP_BROKER_SAFE="false"
    kill "${paper_pid}" >/dev/null 2>&1 || true
    sleep 1
    if process_alive_not_zombie "${paper_pid}"; then
      kill -TERM "${paper_pid}" >/dev/null 2>&1 || true
    fi
    LAUNCH_STOP_OBSERVED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    set +e
    wait "${paper_pid}"
    child_exit_code=$?
    set -e
    remove_pid_file_if_matches "${paper_pid}"
    if [[ -n "${LAUNCH_FIRST_TRUTH_GENERATED_AT}" ]]; then
      write_launch_status "LAUNCH_VERIFIER_STOPPED_CHILD_AFTER_TIMEOUT" "${paper_pid}" "launch verifier stopped the background child after runtime truth failed to advance during the bounded observation window" "${child_exit_code}"
    else
      write_launch_status "LAUNCH_VERIFIER_STOPPED_CHILD_AFTER_TIMEOUT" "${paper_pid}" "launch verifier stopped the background child after it failed to produce fresh runtime truth/heartbeat before the bounded verification timeout" "${child_exit_code}"
    fi
    echo "Probationary paper soak failed to produce runtime truth in background." >&2
    echo "Launch status: ${LAUNCH_STATUS_FILE}" >&2
    echo "Log file: ${LOG_FILE}" >&2
    exit 1
  fi
  set +e
  wait "${paper_pid}"
  child_exit_code=$?
  set -e
  remove_pid_file_if_matches "${paper_pid}"
  LAUNCH_FINAL_PID_ALIVE="false"
  LAUNCH_STOP_OBSERVED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  LAUNCH_STOP_REQUESTED_AT="${LAUNCH_STOP_OBSERVED_AT}"
  LAUNCH_STOP_EXPECTED_CLEANUP="false"
  LAUNCH_STOP_BROKER_SAFE="false"
  if [[ -n "${LAUNCH_FIRST_TRUTH_GENERATED_AT}" ]]; then
    LAUNCH_STOP_SOURCE="runtime_internal"
    LAUNCH_TERMINATION_REASON="runtime_exited_after_initial_truth"
    write_launch_status "RUNTIME_EXITED_AFTER_INITIAL_TRUTH" "${paper_pid}" "background child emitted initial runtime truth but exited before sustained runtime convergence" "${child_exit_code}"
  elif background_child_reached_preflight "${paper_pid}"; then
    LAUNCH_STOP_SOURCE="runtime_internal"
    LAUNCH_TERMINATION_REASON="runtime_exited_after_preflight"
    write_launch_status "RUNTIME_EXITED_AFTER_PREFLIGHT" "${paper_pid}" "background child exited after Phase-1 preflight but before runtime truth/heartbeat convergence" "${child_exit_code}"
  else
    LAUNCH_STOP_SOURCE="unknown"
    LAUNCH_TERMINATION_REASON="background_start_failed_before_runtime_truth"
    write_launch_status "PROBATIONARY_PAPER_BACKGROUND_START_FAILED" "${paper_pid}" "background child exited before launch verification completed" "${child_exit_code}"
  fi
  echo "Probationary paper soak failed to remain running in background." >&2
  echo "Launch status: ${LAUNCH_STATUS_FILE}" >&2
  echo "Log file: ${LOG_FILE}" >&2
  exit 1
fi

exec "${PYTHON_BIN}" -m mgc_v05l.app.main probationary-paper-soak "${FINAL_ARGS[@]}"
