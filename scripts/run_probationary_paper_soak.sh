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
)
ATPE_CANARY_CONFIG="${REPO_ROOT}/config/probationary_pattern_engine_paper_atpe_canary.yaml"
GC_MGC_ACCEPTANCE_CONFIG="${REPO_ROOT}/config/probationary_pattern_engine_paper_gc_mgc_acceptance.yaml"
DEFAULT_RUNTIME_DIR="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime"
DEFAULT_PID_FILE="${DEFAULT_RUNTIME_DIR}/probationary_paper.pid"
DEFAULT_LOG_FILE="${DEFAULT_RUNTIME_DIR}/probationary_paper.log"
DEFAULT_CONFIG_PATHS_FILE="${DEFAULT_RUNTIME_DIR}/paper_runtime_config_paths.txt"
CANARY_ENABLE_SENTINEL="${DEFAULT_RUNTIME_DIR}/enable_paper_route_canary.flag"
CONFIG_OVERRIDE_RAW="${MGC_PROBATIONARY_PAPER_CONFIG_PATHS:-}"

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
  nohup "${PYTHON_BIN}" -m mgc_v05l.app.main probationary-paper-soak "${FINAL_ARGS[@]}" >> "${LOG_FILE}" 2>&1 &
  paper_pid=$!
  echo "${paper_pid}" > "${PID_FILE}"
  echo "Probationary paper soak running in background."
  echo "PID: ${paper_pid}"
  echo "PID file: ${PID_FILE}"
  echo "Log file: ${LOG_FILE}"
  exit 0
fi

exec "${PYTHON_BIN}" -m mgc_v05l.app.main probationary-paper-soak "${FINAL_ARGS[@]}"
