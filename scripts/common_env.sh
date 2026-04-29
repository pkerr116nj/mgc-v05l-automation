#!/usr/bin/env bash

set -euo pipefail

if [[ -n "${REPO_ROOT:-}" && -d "${REPO_ROOT}" ]]; then
  SCRIPT_DIR="${REPO_ROOT}/scripts"
else
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi
LOCAL_DOTENV="${REPO_ROOT}/.env"
LOCAL_SCHWAB_ENV="${REPO_ROOT}/.local/schwab_env.sh"
APP_SUPPORT_DIR="${HOME}/Library/Application Support/mgc_v05l"
APP_SUPPORT_RESEARCH_RUNTIME_ROOT="${APP_SUPPORT_DIR}/research_daily_capture_runtime"
APP_SUPPORT_SCHWAB_ENV="${APP_SUPPORT_RESEARCH_RUNTIME_ROOT}/schwab_env.sh"
VENV_ACTIVATE="${REPO_ROOT}/.venv/bin/activate"

prepend_path_if_dir() {
  local candidate="$1"
  if [[ -d "${candidate}" ]]; then
    case ":${PATH:-}:" in
      *":${candidate}:"*) ;;
      *)
        PATH="${candidate}${PATH:+:${PATH}}"
        ;;
    esac
  fi
}

normalize_bool_env() {
  local raw="${1:-}"
  raw="$(printf '%s' "${raw}" | tr '[:upper:]' '[:lower:]')"
  case "${raw}" in
    1|true|yes|on)
      printf 'true'
      ;;
    *)
      printf 'false'
      ;;
  esac
}

normalize_provider_name() {
  printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]'
}

missing_schwab_auth_env_names() {
  local missing=()
  for name in SCHWAB_APP_KEY SCHWAB_APP_SECRET SCHWAB_CALLBACK_URL; do
    if [[ -z "${!name:-}" ]]; then
      missing+=("${name}")
    fi
  done
  if [[ "${#missing[@]}" -eq 0 ]]; then
    return 0
  fi
  printf '%s\n' "${missing[*]}"
}

schwab_env_source_kind_for_path() {
  local path="$1"
  if [[ -z "${path}" ]]; then
    printf 'none'
    return 0
  fi
  if [[ "${path}" == "${LOCAL_SCHWAB_ENV}" ]]; then
    printf 'repo_local'
    return 0
  fi
  if [[ "${path}" == "${APP_SUPPORT_SCHWAB_ENV}" ]]; then
    printf 'app_support_research_runtime'
    return 0
  fi
  printf 'custom'
}

resolve_schwab_env_source() {
  if [[ -n "${MGC_SCHWAB_ENV_FILE:-}" && -f "${MGC_SCHWAB_ENV_FILE}" ]]; then
    printf '%s\n' "${MGC_SCHWAB_ENV_FILE}"
    return 0
  fi
  if [[ -f "${APP_SUPPORT_SCHWAB_ENV}" ]]; then
    printf '%s\n' "${APP_SUPPORT_SCHWAB_ENV}"
    return 0
  fi
  if [[ -f "${LOCAL_SCHWAB_ENV}" ]]; then
    printf '%s\n' "${LOCAL_SCHWAB_ENV}"
    return 0
  fi
  return 1
}

schwab_auth_env_next_action() {
  local source_path="$1"
  if [[ -n "${source_path}" ]]; then
    printf 'Restore or source Schwab auth env from %s before running Schwab-backed bootstrap actions.' "${source_path}"
    return 0
  fi
  printf 'Restore Schwab auth env in %s or export SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_CALLBACK_URL, and SCHWAB_TOKEN_FILE before running Schwab-backed bootstrap actions.' "${APP_SUPPORT_SCHWAB_ENV}"
}

bootstrap_local_operator_env() {
  local missing=()
  local resolved_schwab_env=""
  local existing_auth_missing=""

  prepend_path_if_dir "/opt/homebrew/bin"
  prepend_path_if_dir "/usr/local/bin"

  if [[ -f "${VENV_ACTIVATE}" ]]; then
    # shellcheck disable=SC1091
    source "${VENV_ACTIVATE}"
  elif [[ ! -x "${REPO_ROOT}/.venv/bin/python" ]]; then
    missing+=(".venv/bin/activate")
  fi

  existing_auth_missing="$(missing_schwab_auth_env_names || true)"
  if [[ -n "${existing_auth_missing}" ]]; then
    resolved_schwab_env="$(resolve_schwab_env_source || true)"
  fi
  if [[ -n "${resolved_schwab_env}" ]]; then
    # shellcheck disable=SC1091
    source "${resolved_schwab_env}"
  fi

  if [[ -f "${LOCAL_DOTENV}" ]]; then
    # shellcheck disable=SC1091
    source "${LOCAL_DOTENV}"
  fi

  export PATH
  export PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"
  export MGC_BOOTSTRAP_SCHWAB_ENV_SOURCE_PATH="${resolved_schwab_env}"
  export MGC_BOOTSTRAP_SCHWAB_ENV_SOURCE_KIND="$(schwab_env_source_kind_for_path "${resolved_schwab_env}")"

  if [[ "${#missing[@]}" -ne 0 ]]; then
    echo "Operator bootstrap failed: missing ${missing[*]} under ${REPO_ROOT}. Run from the repo root with the local virtualenv present." >&2
    exit 1
  fi
}

bootstrap_local_operator_env

export REPO_ROOT
export CONFIG_BASE="${REPO_ROOT}/config/base.yaml"
export CONFIG_REPLAY="${REPO_ROOT}/config/replay.yaml"
export SCHWAB_CONFIG="${REPO_ROOT}/config/schwab.local.json"
export DB_PATH="${REPO_ROOT}/mgc_v05l.replay.sqlite3"
export PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
export OUTPUT_ROOT="${REPO_ROOT}/outputs"
export REPORT_DIR="${OUTPUT_ROOT}/reports"
export VIZ_DIR="${OUTPUT_ROOT}/visualizations"
export REPLAY_DIR="${OUTPUT_ROOT}/replays"
export REPLAY_POINT_VALUE="${REPLAY_POINT_VALUE:-10}"
export REPLAY_FEE_PER_FILL="${REPLAY_FEE_PER_FILL:-0}"
export REPLAY_SLIPPAGE_PER_FILL="${REPLAY_SLIPPAGE_PER_FILL:-0}"
export MARKET_DATA_PRIMARY="${MARKET_DATA_PRIMARY:-databento}"
export MARKET_DATA_FALLBACK="${MARKET_DATA_FALLBACK:-schwab}"
export BROKER_TRUTH_PROVIDER="${BROKER_TRUTH_PROVIDER:-ibkr}"
export EXECUTION_PROVIDER="${EXECUTION_PROVIDER:-ibkr}"
export ALLOW_SCHWAB_FALLBACK="${ALLOW_SCHWAB_FALLBACK:-true}"
export REQUIRE_SCHWAB_AUTH="${REQUIRE_SCHWAB_AUTH:-false}"

ensure_file() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "Required file not found: $path" >&2
    exit 1
  fi
}

ensure_dir() {
  local path="$1"
  mkdir -p "$path"
}

ensure_file "${CONFIG_BASE}"
ensure_file "${CONFIG_REPLAY}"
ensure_file "${PYTHON_BIN}"

ensure_dir "${REPORT_DIR}"
ensure_dir "${VIZ_DIR}"
ensure_dir "${REPLAY_DIR}"

export MGC_V05L_SETTINGS_SYMBOL="${MGC_V05L_SETTINGS_SYMBOL:-MGC}"
export MGC_V05L_SETTINGS_TIMEFRAME="${MGC_V05L_SETTINGS_TIMEFRAME:-5m}"

schwab_auth_env_loaded() {
  [[ -z "$(missing_schwab_auth_env_names)" ]]
}

schwab_config_available() {
  [[ -f "${SCHWAB_CONFIG}" ]]
}

schwab_runtime_dependency_required() {
  if [[ "$(normalize_bool_env "${REQUIRE_SCHWAB_AUTH:-false}")" == "true" ]]; then
    return 0
  fi
  if [[ "$(normalize_provider_name "${MARKET_DATA_PRIMARY:-databento}")" == "schwab" ]]; then
    return 0
  fi
  if [[ "$(normalize_provider_name "${BROKER_TRUTH_PROVIDER:-ibkr}")" == "schwab" ]]; then
    return 0
  fi
  if [[ "$(normalize_provider_name "${EXECUTION_PROVIDER:-ibkr}")" == "schwab" ]]; then
    return 0
  fi
  return 1
}

schwab_auth_status() {
  if schwab_auth_env_loaded && schwab_config_available; then
    printf 'ready'
    return 0
  fi
  if schwab_runtime_dependency_required; then
    printf 'missing'
    return 0
  fi
  printf 'fallback_unavailable'
}

schwab_auth_status_reason() {
  local status
  status="$(schwab_auth_status)"
  if [[ "${status}" == "ready" ]]; then
    printf 'Schwab fallback inputs are available.'
    return 0
  fi
  if [[ "${status}" == "missing" ]]; then
    printf 'Schwab is an active required provider path for this runtime and its auth/config inputs are missing.'
    return 0
  fi
  printf 'Schwab fallback inputs are unavailable, but the active IBKR/Databento runtime path does not require Schwab auth.'
}

replay_db_missing() {
  [[ ! -f "${DB_PATH}" ]]
}

replay_db_bootstrap_next_action() {
  if schwab_auth_env_loaded; then
    printf 'Run `bash scripts/backfill_schwab_1m_history.sh` to create and populate %s.' "${DB_PATH}"
  else
    printf 'Load Schwab auth env (`source .local/schwab_env.sh`) and then run `bash scripts/backfill_schwab_1m_history.sh` to create %s.' "${DB_PATH}"
  fi
}

export MGC_BOOTSTRAP_REPLAY_DB_STATUS="$([[ -f "${DB_PATH}" ]] && echo "ready" || echo "missing")"
export MGC_BOOTSTRAP_REPLAY_DB_PATH="${DB_PATH}"
export MGC_BOOTSTRAP_REPLAY_DB_NEXT_ACTION="$(replay_db_bootstrap_next_action)"
export MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_MISSING_NAMES="$(missing_schwab_auth_env_names)"
export MGC_BOOTSTRAP_SCHWAB_CONFIG_STATUS="$([[ -f "${SCHWAB_CONFIG}" ]] && echo "ready" || echo "missing")"
export MGC_BOOTSTRAP_SCHWAB_RUNTIME_REQUIRED="$(schwab_runtime_dependency_required && echo "true" || echo "false")"
export MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_STATUS="$(schwab_auth_status)"
export MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_NEXT_ACTION="$(schwab_auth_env_next_action "${MGC_BOOTSTRAP_SCHWAB_ENV_SOURCE_PATH:-${APP_SUPPORT_SCHWAB_ENV}}")"
export MGC_BOOTSTRAP_SCHWAB_STATUS_REASON="$(schwab_auth_status_reason)"
export MGC_OPERATOR_DASHBOARD_REDUCED_MODE="$([[ "${MGC_BOOTSTRAP_REPLAY_DB_STATUS}" == "missing" || "${MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_STATUS}" == "missing" ]] && echo "1" || echo "0")"

ensure_signal_evaluations_structure_columns() {
  require_replay_db_available
  "${PYTHON_BIN}" - <<'PY'
import os
import sqlite3
from pathlib import Path

db_path = Path(os.environ["DB_PATH"])
connection = sqlite3.connect(db_path)
try:
    existing = {
        row[1]
        for row in connection.execute("pragma table_info(signal_evaluations)")
    }
    required = {
        "compression_long": "alter table signal_evaluations add column compression_long BOOLEAN NOT NULL DEFAULT 0",
        "reclaim_long": "alter table signal_evaluations add column reclaim_long BOOLEAN NOT NULL DEFAULT 0",
        "separation_long": "alter table signal_evaluations add column separation_long BOOLEAN NOT NULL DEFAULT 0",
        "structure_long_candidate": "alter table signal_evaluations add column structure_long_candidate BOOLEAN NOT NULL DEFAULT 0",
        "compression_short": "alter table signal_evaluations add column compression_short BOOLEAN NOT NULL DEFAULT 0",
        "failure_short": "alter table signal_evaluations add column failure_short BOOLEAN NOT NULL DEFAULT 0",
        "separation_short": "alter table signal_evaluations add column separation_short BOOLEAN NOT NULL DEFAULT 0",
        "structure_short_candidate": "alter table signal_evaluations add column structure_short_candidate BOOLEAN NOT NULL DEFAULT 0",
    }
    for column_name, statement in required.items():
        if column_name not in existing:
            connection.execute(statement)
    connection.commit()
finally:
    connection.close()
PY
}

require_schwab_auth_env() {
  local missing
  missing="$(missing_schwab_auth_env_names)"
  if [[ -n "${missing}" ]]; then
    echo "Schwab auth bootstrap incomplete: missing ${missing}. Checked shell env, ${LOCAL_SCHWAB_ENV}, and ${LOCAL_DOTENV}." >&2
    exit 1
  fi
  if [[ ! -f "${SCHWAB_CONFIG}" ]]; then
    echo "Schwab config bootstrap incomplete: missing ${SCHWAB_CONFIG}." >&2
    exit 1
  fi
}

require_schwab_auth_env_if_required() {
  if schwab_runtime_dependency_required; then
    require_schwab_auth_env
  fi
}

require_replay_db_available() {
  if replay_db_missing; then
    echo "Replay DB bootstrap incomplete: missing ${DB_PATH}. $(replay_db_bootstrap_next_action)" >&2
    exit 1
  fi
}

active_schwab_symbols() {
  if [[ ! -f "${SCHWAB_CONFIG}" ]]; then
    return 0
  fi
  "${PYTHON_BIN}" - <<'PY'
import json
import os
from pathlib import Path

config_path = Path(os.environ["SCHWAB_CONFIG"])
payload = json.loads(config_path.read_text(encoding="utf-8"))
symbols = [str(symbol).strip() for symbol in payload.get("historical_symbol_map", {}).keys() if str(symbol).strip()]
print(",".join(symbols))
PY
}

runtime_network_resolution_preflight() {
  local schwab_config_path="$1"
  local label="${2:-runtime}"
  "${PYTHON_BIN}" - "${schwab_config_path}" "${label}" <<'PY'
import json
import os
import socket
import sys
from urllib.parse import urlparse

from mgc_v05l.market_data import load_schwab_market_data_config

config_path = sys.argv[1]
label = sys.argv[2]
config = load_schwab_market_data_config(config_path)
base_url = str(config.market_data_base_url or "").strip()
parsed = urlparse(base_url)
scheme = parsed.scheme or ""
hostname = parsed.hostname or ""
port = parsed.port or (443 if scheme == "https" else 80)
endpoint = f"{base_url.rstrip('/')}/pricehistory" if base_url else ""
env_names = (
    "CODEX_SANDBOX",
    "CODEX_SHELL",
    "PATH",
    "PYTHONPATH",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "RES_OPTIONS",
    "HOSTALIASES",
    "SSL_CERT_FILE",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
)
payload = {
    "label": label,
    "python_executable": sys.executable,
    "python_version": sys.version.split()[0],
    "market_data_base_url": base_url,
    "hostname": hostname,
    "port": port,
    "endpoint": endpoint,
    "env": {name: os.environ.get(name, "<unset>") for name in env_names},
}
print("Runtime network preflight:", json.dumps(payload, sort_keys=True))
if not hostname:
    print(
        "Runtime network preflight failed: "
        f"invalid market_data_base_url={base_url!r}",
        file=sys.stderr,
    )
    raise SystemExit(1)
try:
    resolved = socket.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)
except socket.gaierror as exc:
    sandbox_hint = ""
    if os.environ.get("CODEX_SANDBOX"):
        sandbox_hint = (
            " The current launcher is running inside a sandboxed parent process; "
            "repo code cannot remove that launch context."
        )
    print(
        "Runtime network preflight failed: "
        f"hostname={hostname!r} "
        f"base_url={base_url!r} "
        f"endpoint={endpoint!r} "
        f"python={sys.executable!r} "
        f"error={exc!r}."
        f"{sandbox_hint}",
        file=sys.stderr,
    )
    raise SystemExit(1)
resolved_addresses = sorted({entry[4][0] for entry in resolved if entry[4]})
print(
    "Runtime network preflight passed: "
    f"hostname={hostname!r} "
    f"resolved_addresses={resolved_addresses} "
    f"endpoint={endpoint!r}"
)
PY
}
