#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -f "${REPO_ROOT}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.venv/bin/activate"
fi
if [[ -f "${REPO_ROOT}/.local/schwab_env.sh" ]]; then
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.local/schwab_env.sh"
fi
if [[ -n "${MGC_DATABENTO_ENV_FILE:-}" ]]; then
  if [[ ! -f "${MGC_DATABENTO_ENV_FILE}" ]]; then
    echo "MGC_DATABENTO_ENV_FILE does not exist: ${MGC_DATABENTO_ENV_FILE}" >&2
    exit 1
  fi
  set -a
  # shellcheck disable=SC1090
  source "${MGC_DATABENTO_ENV_FILE}"
  set +a
elif [[ -f "${REPO_ROOT}/.env.local" ]]; then
  # Local-only developer configuration; never print its contents.
  set -a
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.env.local"
  set +a
fi
export PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"

if [[ " ${*} " != *" --demo "* ]]; then
  missing=()
  for name in SCHWAB_APP_KEY SCHWAB_APP_SECRET SCHWAB_CALLBACK_URL; do
    if [[ -z "${!name:-}" ]]; then
      missing+=("${name}")
    fi
  done
  if (( ${#missing[@]} > 0 )); then
    echo "Schwab auth is incomplete: missing ${missing[*]}. Configure .local/schwab_env.sh; do not paste secrets into chat." >&2
    exit 1
  fi
fi

if [[ " ${*} " == *" --databento "* ]] && [[ -z "${DATABENTO_API_KEY:-}" ]]; then
  echo "Databento mode requires DATABENTO_API_KEY. Set MGC_DATABENTO_ENV_FILE to the existing Mars env file; do not paste the key into chat." >&2
  exit 1
fi

exec python -m mgc_v05l.ndxp_terminal "$@"
