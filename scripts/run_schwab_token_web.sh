#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOCAL_SCHWAB_ENV="${REPO_ROOT}/.local/schwab_env.sh"
LOCAL_DOTENV="${REPO_ROOT}/.env"
VENV_ACTIVATE="${REPO_ROOT}/.venv/bin/activate"
PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"

if [[ -f "${VENV_ACTIVATE}" ]]; then
  # shellcheck disable=SC1091
  source "${VENV_ACTIVATE}"
fi
if [[ -f "${LOCAL_SCHWAB_ENV}" ]]; then
  # shellcheck disable=SC1091
  source "${LOCAL_SCHWAB_ENV}"
fi
if [[ -f "${LOCAL_DOTENV}" ]]; then
  # shellcheck disable=SC1091
  source "${LOCAL_DOTENV}"
fi

for name in SCHWAB_APP_KEY SCHWAB_APP_SECRET SCHWAB_CALLBACK_URL; do
  if [[ -z "${!name:-}" ]]; then
    echo "Schwab auth bootstrap incomplete: missing ${name}. Checked shell env, ${LOCAL_SCHWAB_ENV}, and ${LOCAL_DOTENV}." >&2
    exit 1
  fi
done

export REPO_ROOT
export PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"

DEFAULT_HOST="${SCHWAB_TOKEN_WEB_HOST:-127.0.0.1}"
DEFAULT_PORT="${SCHWAB_TOKEN_WEB_PORT:-8765}"
DEFAULT_INFO_FILE="${SCHWAB_TOKEN_WEB_INFO_FILE:-${TMPDIR:-/tmp}/mgc_v05l_schwab_token_web.json}"
DEFAULT_SCHWAB_CONFIG="${SCHWAB_CONFIG:-${REPO_ROOT}/config/schwab.local.json}"
DEFAULT_PROBE_SYMBOL="${SCHWAB_TOKEN_WEB_PROBE_SYMBOL:-MGC}"

HOST_SET=0
PORT_SET=0
INFO_FILE_SET=0
SCHWAB_CONFIG_SET=0
PROBE_SYMBOL_SET=0
ARGS=()
FINAL_ARGS=()

while (($# > 0)); do
  case "$1" in
    --host)
      HOST_SET=1
      ARGS+=("$1" "$2")
      shift 2
      ;;
    --host=*)
      HOST_SET=1
      ARGS+=("$1")
      shift
      ;;
    --port)
      PORT_SET=1
      ARGS+=("$1" "$2")
      shift 2
      ;;
    --port=*)
      PORT_SET=1
      ARGS+=("$1")
      shift
      ;;
    --info-file)
      INFO_FILE_SET=1
      ARGS+=("$1" "$2")
      shift 2
      ;;
    --info-file=*)
      INFO_FILE_SET=1
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
    --probe-symbol)
      PROBE_SYMBOL_SET=1
      ARGS+=("$1" "$2")
      shift 2
      ;;
    --probe-symbol=*)
      PROBE_SYMBOL_SET=1
      ARGS+=("$1")
      shift
      ;;
    --no-info-file)
      INFO_FILE_SET=1
      shift
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ ${HOST_SET} -eq 0 ]]; then
  FINAL_ARGS+=(--host "${DEFAULT_HOST}")
fi
if [[ ${PORT_SET} -eq 0 ]]; then
  FINAL_ARGS+=(--port "${DEFAULT_PORT}")
fi
if [[ ${INFO_FILE_SET} -eq 0 && -n "${DEFAULT_INFO_FILE}" ]]; then
  FINAL_ARGS+=(--info-file "${DEFAULT_INFO_FILE}")
fi
if [[ ${SCHWAB_CONFIG_SET} -eq 0 ]]; then
  FINAL_ARGS+=(--schwab-config "${DEFAULT_SCHWAB_CONFIG}")
fi
if [[ ${PROBE_SYMBOL_SET} -eq 0 ]]; then
  FINAL_ARGS+=(--probe-symbol "${DEFAULT_PROBE_SYMBOL}")
fi
if [[ ${#ARGS[@]} -gt 0 ]]; then
  FINAL_ARGS+=("${ARGS[@]}")
fi

exec "${PYTHON_BIN}" -m mgc_v05l.app.main schwab-token-web "${FINAL_ARGS[@]}"
