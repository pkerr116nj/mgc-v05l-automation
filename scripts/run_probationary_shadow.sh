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
)
DEFAULT_RUNTIME_DIR="${REPO_ROOT}/outputs/probationary_pattern_engine/runtime"
DEFAULT_PID_FILE="${DEFAULT_RUNTIME_DIR}/probationary_shadow.pid"
DEFAULT_LOG_FILE="${DEFAULT_RUNTIME_DIR}/probationary_shadow.log"

ARGS=()
CONFIG_SET=0
SCHWAB_CONFIG_SET=0
BACKGROUND=0
PID_FILE="${DEFAULT_PID_FILE}"
LOG_FILE="${DEFAULT_LOG_FILE}"

while (($# > 0)); do
  case "$1" in
    --background)
      BACKGROUND=1
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
fi
if [[ ${SCHWAB_CONFIG_SET} -eq 0 ]]; then
  FINAL_ARGS+=(--schwab-config "${DEFAULT_SCHWAB_CONFIG}")
fi
if [[ ${#ARGS[@]} -gt 0 ]]; then
  FINAL_ARGS+=("${ARGS[@]}")
fi

echo "Launching probationary shadow with repo bootstrap."
echo "Schwab config: ${DEFAULT_SCHWAB_CONFIG}"
echo "Probationary configs:"
if [[ ${CONFIG_SET} -eq 0 ]]; then
  for config_path in "${DEFAULT_CONFIGS[@]}"; do
    echo "  - ${config_path}"
  done
else
  echo "  - custom --config args supplied"
fi
if [[ ${BACKGROUND} -eq 1 ]]; then
  ensure_dir "$(dirname "${PID_FILE}")"
  ensure_dir "$(dirname "${LOG_FILE}")"
  if [[ -f "${PID_FILE}" ]]; then
    existing_pid="$(cat "${PID_FILE}")"
    if [[ -n "${existing_pid}" ]] && kill -0 "${existing_pid}" 2>/dev/null; then
      echo "Probationary shadow already running with PID ${existing_pid} (${PID_FILE})." >&2
      exit 1
    fi
  fi
  nohup "${PYTHON_BIN}" -m mgc_v05l.app.main probationary-shadow "${FINAL_ARGS[@]}" >> "${LOG_FILE}" 2>&1 &
  shadow_pid=$!
  echo "${shadow_pid}" > "${PID_FILE}"
  echo "Probationary shadow running in background."
  echo "PID: ${shadow_pid}"
  echo "PID file: ${PID_FILE}"
  echo "Log file: ${LOG_FILE}"
  exit 0
fi

exec "${PYTHON_BIN}" -m mgc_v05l.app.main probationary-shadow "${FINAL_ARGS[@]}"
