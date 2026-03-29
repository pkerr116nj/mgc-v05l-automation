#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

require_schwab_auth_env

DEFAULT_SCHWAB_CONFIG="${SCHWAB_CONFIG:-${REPO_ROOT}/config/schwab.local.json}"
DEFAULT_CONFIGS=(
  "${REPO_ROOT}/config/base.yaml"
  "${REPO_ROOT}/config/live.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper.yaml"
)
ATPE_CANARY_CONFIG="${REPO_ROOT}/config/probationary_pattern_engine_paper_atpe_canary.yaml"
ATP_COMPANION_V1_CONFIG="${REPO_ROOT}/config/probationary_pattern_engine_paper_atp_companion_v1_asia_us.yaml"
GC_MGC_ACCEPTANCE_CONFIG="${REPO_ROOT}/config/probationary_pattern_engine_paper_gc_mgc_acceptance.yaml"
DEFAULT_RUNTIME_DIR="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime"
DEFAULT_PID_FILE="${DEFAULT_RUNTIME_DIR}/probationary_paper.pid"
DEFAULT_LOG_FILE="${DEFAULT_RUNTIME_DIR}/probationary_paper.log"

ARGS=()
CONFIG_SET=0
SCHWAB_CONFIG_SET=0
BACKGROUND=0
INCLUDE_ATPE_CANARY=0
INCLUDE_ATP_COMPANION_V1=0
INCLUDE_GC_MGC_ACCEPTANCE=0
PID_FILE="${DEFAULT_PID_FILE}"
LOG_FILE="${DEFAULT_LOG_FILE}"

while (($# > 0)); do
  case "$1" in
    --background)
      BACKGROUND=1
      shift
      ;;
    --include-atpe-canary)
      INCLUDE_ATPE_CANARY=1
      shift
      ;;
    --include-atp-companion-v1-paper)
      INCLUDE_ATP_COMPANION_V1=1
      shift
      ;;
    --include-gc-mgc-acceptance)
      INCLUDE_GC_MGC_ACCEPTANCE=1
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
  for config_path in "${DEFAULT_CONFIGS[@]}"; do
    FINAL_ARGS+=(--config "${config_path}")
  done
  if [[ ${INCLUDE_ATPE_CANARY} -eq 1 ]]; then
    FINAL_ARGS+=(--config "${ATPE_CANARY_CONFIG}")
  fi
  if [[ ${INCLUDE_ATP_COMPANION_V1} -eq 1 ]]; then
    FINAL_ARGS+=(--config "${ATP_COMPANION_V1_CONFIG}")
  fi
  if [[ ${INCLUDE_GC_MGC_ACCEPTANCE} -eq 1 ]]; then
    FINAL_ARGS+=(--config "${GC_MGC_ACCEPTANCE_CONFIG}")
  fi
fi
if [[ ${SCHWAB_CONFIG_SET} -eq 0 ]]; then
  FINAL_ARGS+=(--schwab-config "${DEFAULT_SCHWAB_CONFIG}")
fi
if [[ ${#ARGS[@]} -gt 0 ]]; then
  FINAL_ARGS+=("${ARGS[@]}")
fi

echo "Launching probationary paper soak with repo bootstrap."
echo "Schwab config: ${DEFAULT_SCHWAB_CONFIG}"
echo "Paper configs:"
if [[ ${CONFIG_SET} -eq 0 ]]; then
  for config_path in "${DEFAULT_CONFIGS[@]}"; do
    echo "  - ${config_path}"
  done
  if [[ ${INCLUDE_ATPE_CANARY} -eq 1 ]]; then
    echo "  - ${ATPE_CANARY_CONFIG}"
  fi
  if [[ ${INCLUDE_ATP_COMPANION_V1} -eq 1 ]]; then
    echo "  - ${ATP_COMPANION_V1_CONFIG}"
  fi
  if [[ ${INCLUDE_GC_MGC_ACCEPTANCE} -eq 1 ]]; then
    echo "  - ${GC_MGC_ACCEPTANCE_CONFIG}"
  fi
else
  echo "  - custom --config args supplied"
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
