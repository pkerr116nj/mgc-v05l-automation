#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

require_schwab_auth_env

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
