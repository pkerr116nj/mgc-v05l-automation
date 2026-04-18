#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

DEFAULT_RUNTIME_DIR="${REPO_ROOT}/outputs/operator_dashboard/runtime"
DEFAULT_HEALTH_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_health.json"
DEFAULT_STATUS_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_status.json"
DEFAULT_MARKDOWN_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_status.md"
DEFAULT_STARTUP_FILE="${REPO_ROOT}/outputs/operator_dashboard/startup_control_plane_snapshot.json"
DEFAULT_OPERABILITY_FILE="${REPO_ROOT}/outputs/operator_dashboard/supervised_paper_operability_snapshot.json"
DEFAULT_INFO_FILE="${DEFAULT_RUNTIME_DIR}/operator_dashboard.json"
DEFAULT_DASHBOARD_PAYLOAD_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_dashboard.json"
DEFAULT_LIVE_STARTUP_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_startup_control_plane.json"
DEFAULT_LIVE_OPERABILITY_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_operability.json"
DEFAULT_URL="${MGC_OPERATOR_DASHBOARD_URL:-http://127.0.0.1:8790/}"
DEFAULT_HEALTH_ATTEMPTS=5
DEFAULT_HEALTH_RETRY_DELAY_SECONDS=1

STATUS_FILE="${DEFAULT_STATUS_FILE}"
MARKDOWN_FILE="${DEFAULT_MARKDOWN_FILE}"
HEALTH_FILE="${DEFAULT_HEALTH_FILE}"
DASHBOARD_URL="${DEFAULT_URL}"
HEALTH_ATTEMPTS="${DEFAULT_HEALTH_ATTEMPTS}"
HEALTH_RETRY_DELAY_SECONDS="${DEFAULT_HEALTH_RETRY_DELAY_SECONDS}"
DASHBOARD_PAYLOAD_FILE="${DEFAULT_DASHBOARD_PAYLOAD_FILE}"
LIVE_STARTUP_FILE="${DEFAULT_LIVE_STARTUP_FILE}"
LIVE_OPERABILITY_FILE="${DEFAULT_LIVE_OPERABILITY_FILE}"

while (($# > 0)); do
  case "$1" in
    --output)
      STATUS_FILE="$2"
      shift 2
      ;;
    --output=*)
      STATUS_FILE="${1#*=}"
      shift
      ;;
    --markdown-output)
      MARKDOWN_FILE="$2"
      shift 2
      ;;
    --markdown-output=*)
      MARKDOWN_FILE="${1#*=}"
      shift
      ;;
    --health-file)
      HEALTH_FILE="$2"
      shift 2
      ;;
    --health-file=*)
      HEALTH_FILE="${1#*=}"
      shift
      ;;
    --dashboard-url)
      DASHBOARD_URL="$2"
      shift 2
      ;;
    --dashboard-url=*)
      DASHBOARD_URL="${1#*=}"
      shift
      ;;
    --health-attempts)
      HEALTH_ATTEMPTS="$2"
      shift 2
      ;;
    --health-attempts=*)
      HEALTH_ATTEMPTS="${1#*=}"
      shift
      ;;
    --health-retry-delay-seconds)
      HEALTH_RETRY_DELAY_SECONDS="$2"
      shift 2
      ;;
    --health-retry-delay-seconds=*)
      HEALTH_RETRY_DELAY_SECONDS="${1#*=}"
      shift
      ;;
    *)
      echo "Unsupported argument: $1" >&2
      exit 1
      ;;
  esac
done

ensure_dir "$(dirname "${STATUS_FILE}")"
ensure_dir "$(dirname "${MARKDOWN_FILE}")"
ensure_dir "$(dirname "${HEALTH_FILE}")"
ensure_dir "$(dirname "${DASHBOARD_PAYLOAD_FILE}")"

fetch_health_snapshot() {
  local tmp_file
  tmp_file="${HEALTH_FILE}.tmp"
  rm -f "${tmp_file}"
  local attempt=1
  while (( attempt <= HEALTH_ATTEMPTS )); do
    if curl -fsS "${DASHBOARD_URL%/}/health" > "${tmp_file}"; then
      mv "${tmp_file}" "${HEALTH_FILE}"
      return 0
    fi
    attempt=$((attempt + 1))
    if (( attempt <= HEALTH_ATTEMPTS )); then
      sleep "${HEALTH_RETRY_DELAY_SECONDS}"
    fi
  done
  rm -f "${tmp_file}"
  return 1
}

if ! fetch_health_snapshot; then
  rm -f "${HEALTH_FILE}"
fi

fetch_dashboard_payload() {
  local tmp_file
  tmp_file="${DASHBOARD_PAYLOAD_FILE}.tmp"
  rm -f "${tmp_file}"
  local attempt=1
  while (( attempt <= HEALTH_ATTEMPTS )); do
    if curl -fsS "${DASHBOARD_URL%/}/api/dashboard" > "${tmp_file}"; then
      mv "${tmp_file}" "${DASHBOARD_PAYLOAD_FILE}"
      return 0
    fi
    attempt=$((attempt + 1))
    if (( attempt <= HEALTH_ATTEMPTS )); then
      sleep "${HEALTH_RETRY_DELAY_SECONDS}"
    fi
  done
  rm -f "${tmp_file}"
  return 1
}

STARTUP_FILE="${DEFAULT_STARTUP_FILE}"
OPERABILITY_FILE="${DEFAULT_OPERABILITY_FILE}"

if [[ -f "${HEALTH_FILE}" ]] && fetch_dashboard_payload; then
  "${PYTHON_BIN}" - <<'PY' "${DASHBOARD_PAYLOAD_FILE}" "${LIVE_STARTUP_FILE}" "${LIVE_OPERABILITY_FILE}"
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
startup = payload.get("startup_control_plane") or {}
operability = payload.get("supervised_paper_operability") or {}
Path(sys.argv[2]).write_text(json.dumps(startup, indent=2, sort_keys=True) + "\n", encoding="utf-8")
Path(sys.argv[3]).write_text(json.dumps(operability, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
  STARTUP_FILE="${LIVE_STARTUP_FILE}"
  OPERABILITY_FILE="${LIVE_OPERABILITY_FILE}"
fi

exec "${PYTHON_BIN}" -m mgc_v05l.app.main headless-supervised-paper-status \
  --health-file "${HEALTH_FILE}" \
  --startup-control-plane-file "${STARTUP_FILE}" \
  --supervised-operability-file "${OPERABILITY_FILE}" \
  --dashboard-info-file "${DEFAULT_INFO_FILE}" \
  --output "${STATUS_FILE}" \
  --markdown-output "${MARKDOWN_FILE}"
