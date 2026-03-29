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
ensure_file "${DB_PATH}"

ensure_dir "${REPORT_DIR}"
ensure_dir "${VIZ_DIR}"
ensure_dir "${REPLAY_DIR}"

export MGC_V05L_SETTINGS_SYMBOL="${MGC_V05L_SETTINGS_SYMBOL:-MGC}"
export MGC_V05L_SETTINGS_TIMEFRAME="${MGC_V05L_SETTINGS_TIMEFRAME:-5m}"

ensure_signal_evaluations_structure_columns() {
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
  local missing=()
  for name in SCHWAB_APP_KEY SCHWAB_APP_SECRET SCHWAB_CALLBACK_URL; do
    if [[ -z "${!name:-}" ]]; then
      missing+=("${name}")
    fi
  done
  if [[ "${#missing[@]}" -ne 0 ]]; then
    echo "Schwab auth bootstrap incomplete: missing ${missing[*]}. Checked shell env, ${LOCAL_SCHWAB_ENV}, and ${LOCAL_DOTENV}." >&2
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
