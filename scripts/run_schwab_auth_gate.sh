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

DEFAULT_SCHWAB_CONFIG="${SCHWAB_CONFIG:-${REPO_ROOT}/config/schwab.local.json}"
DEFAULT_PROBE_SYMBOL="${SCHWAB_AUTH_GATE_SYMBOL:-MGC}"

ARGS=()
SCHWAB_CONFIG_SET=0
SYMBOL_SET=0

while (($# > 0)); do
  case "$1" in
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
    --internal-symbol)
      SYMBOL_SET=1
      ARGS+=("$1" "$2")
      shift 2
      ;;
    --internal-symbol=*)
      SYMBOL_SET=1
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
if [[ ${SCHWAB_CONFIG_SET} -eq 0 ]]; then
  FINAL_ARGS+=(--schwab-config "${DEFAULT_SCHWAB_CONFIG}")
fi
if [[ ${SYMBOL_SET} -eq 0 ]]; then
  FINAL_ARGS+=(--internal-symbol "${DEFAULT_PROBE_SYMBOL}")
fi
if [[ ${#ARGS[@]} -gt 0 ]]; then
  FINAL_ARGS+=("${ARGS[@]}")
fi

exec "${PYTHON_BIN}" -m mgc_v05l.app.main schwab-auth-gate "${FINAL_ARGS[@]}"
