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
VENV_ACTIVATE="${REPO_ROOT}/.venv/bin/activate"

bootstrap_local_operator_env() {
  local missing=()

  if [[ -f "${VENV_ACTIVATE}" ]]; then
    # shellcheck disable=SC1091
    source "${VENV_ACTIVATE}"
  elif [[ ! -x "${REPO_ROOT}/.venv/bin/python" ]]; then
    missing+=(".venv/bin/activate")
  fi

  if [[ -f "${LOCAL_SCHWAB_ENV}" ]]; then
    # shellcheck disable=SC1091
    source "${LOCAL_SCHWAB_ENV}"
  fi

  if [[ -f "${LOCAL_DOTENV}" ]]; then
    # shellcheck disable=SC1091
    source "${LOCAL_DOTENV}"
  fi

  export PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"

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
ensure_file "${SCHWAB_CONFIG}"
ensure_file "${PYTHON_BIN}"

ensure_dir "${REPORT_DIR}"
ensure_dir "${VIZ_DIR}"
ensure_dir "${REPLAY_DIR}"

export MGC_V05L_SETTINGS_SYMBOL="${MGC_V05L_SETTINGS_SYMBOL:-MGC}"
export MGC_V05L_SETTINGS_TIMEFRAME="${MGC_V05L_SETTINGS_TIMEFRAME:-5m}"

missing_schwab_auth_env_names() {
  local missing=()
  for name in SCHWAB_APP_KEY SCHWAB_APP_SECRET SCHWAB_CALLBACK_URL; do
    if [[ -z "${!name:-}" ]]; then
      missing+=("${name}")
    fi
  done
  printf '%s\n' "${missing[*]}"
}

schwab_auth_env_loaded() {
  [[ -z "$(missing_schwab_auth_env_names)" ]]
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
export MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_STATUS="$([[ -z "${MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_MISSING_NAMES}" ]] && echo "ready" || echo "missing")"
export MGC_BOOTSTRAP_SCHWAB_AUTH_ENV_NEXT_ACTION="Export SCHWAB_APP_KEY, SCHWAB_APP_SECRET, and SCHWAB_CALLBACK_URL or source .local/schwab_env.sh before running Schwab-backed bootstrap actions."
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
}

require_replay_db_available() {
  if replay_db_missing; then
    echo "Replay DB bootstrap incomplete: missing ${DB_PATH}. $(replay_db_bootstrap_next_action)" >&2
    exit 1
  fi
}

active_schwab_symbols() {
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
