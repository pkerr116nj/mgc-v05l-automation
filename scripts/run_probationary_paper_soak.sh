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
CANARY_ENABLE_SENTINEL="${DEFAULT_RUNTIME_DIR}/enable_paper_route_canary.flag"
CONFIG_OVERRIDE_RAW="${MGC_PROBATIONARY_PAPER_CONFIG_PATHS:-}"
LAUNCH_PYTHON_BIN="${MGC_PROBATIONARY_PAPER_LAUNCH_PYTHON_BIN:-${PYTHON_BIN}}"
BACKGROUND_VERIFY_ATTEMPTS="${MGC_PROBATIONARY_PAPER_BACKGROUND_VERIFY_ATTEMPTS:-60}"
BACKGROUND_VERIFY_POLL_SECONDS="${MGC_PROBATIONARY_PAPER_BACKGROUND_VERIFY_POLL_SECONDS:-1}"
BACKGROUND_OBSERVATION_WINDOW_SECONDS="${MGC_PROBATIONARY_PAPER_BACKGROUND_OBSERVATION_WINDOW_SECONDS:-5}"
RUNTIME_TRUTH_FILE="${MGC_PROBATIONARY_PAPER_RUNTIME_TRUTH_FILE:-${DEFAULT_RUNTIME_TRUTH_FILE}}"
LAUNCH_FIRST_TRUTH_GENERATED_AT=""
LAUNCH_SECOND_TRUTH_GENERATED_AT=""
LAUNCH_SUSTAINED_CONVERGENCE_CONFIRMED="false"
LAUNCH_FINAL_PID_ALIVE="false"

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
  "${PYTHON_BIN}" -c '
import json
import os
from datetime import datetime, timezone

config_paths = [item for item in os.environ.get("LAUNCH_FINAL_ARGS", "").split(os.pathsep) if item]
payload = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
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
    "config_paths": config_paths,
    "paper_only": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "submit_authority": False,
}
with open(os.environ["LAUNCH_STATUS_FILE"], "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2, sort_keys=True)
    fh.write("\n")
'
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
    kill "${paper_pid}" >/dev/null 2>&1 || true
    sleep 1
    if process_alive_not_zombie "${paper_pid}"; then
      kill -TERM "${paper_pid}" >/dev/null 2>&1 || true
    fi
    set +e
    wait "${paper_pid}"
    child_exit_code=$?
    set -e
    remove_pid_file_if_matches "${paper_pid}"
    if [[ -n "${LAUNCH_FIRST_TRUTH_GENERATED_AT}" ]]; then
      write_launch_status "RUNTIME_TRUTH_NOT_ADVANCING" "${paper_pid}" "background child stayed alive but runtime truth did not advance during launch observation window" "${child_exit_code}"
    else
      write_launch_status "RUNTIME_TRUTH_NOT_CONVERGED" "${paper_pid}" "background child stayed alive but did not produce fresh runtime truth/heartbeat before launch verification timed out" "${child_exit_code}"
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
  if [[ -n "${LAUNCH_FIRST_TRUTH_GENERATED_AT}" ]]; then
    write_launch_status "RUNTIME_EXITED_AFTER_INITIAL_TRUTH" "${paper_pid}" "background child emitted initial runtime truth but exited before sustained runtime convergence" "${child_exit_code}"
  elif background_child_reached_preflight "${paper_pid}"; then
    write_launch_status "RUNTIME_EXITED_AFTER_PREFLIGHT" "${paper_pid}" "background child exited after Phase-1 preflight but before runtime truth/heartbeat convergence" "${child_exit_code}"
  else
    write_launch_status "PROBATIONARY_PAPER_BACKGROUND_START_FAILED" "${paper_pid}" "background child exited before launch verification completed" "${child_exit_code}"
  fi
  echo "Probationary paper soak failed to remain running in background." >&2
  echo "Launch status: ${LAUNCH_STATUS_FILE}" >&2
  echo "Log file: ${LOG_FILE}" >&2
  exit 1
fi

exec "${PYTHON_BIN}" -m mgc_v05l.app.main probationary-paper-soak "${FINAL_ARGS[@]}"
