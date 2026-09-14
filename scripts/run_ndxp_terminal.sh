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

exec python -m mgc_v05l.ndxp_terminal "$@"
