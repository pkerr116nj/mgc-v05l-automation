#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

DEFAULT_RUNTIME_DIR="${REPO_ROOT}/outputs/operator_dashboard/runtime"
DEFAULT_HEALTH_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_health.json"
DEFAULT_STATUS_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_status.json"
DEFAULT_MARKDOWN_FILE="${DEFAULT_RUNTIME_DIR}/headless_supervised_paper_status.md"
DEFAULT_CANONICAL_READINESS_FILE="${DEFAULT_RUNTIME_DIR}/latest_canonical_readiness.json"
DEFAULT_CANONICAL_READINESS_SUMMARY_FILE="${DEFAULT_RUNTIME_DIR}/latest_canonical_readiness_summary.json"
DEFAULT_MAINTENANCE_SUPERVISOR_FILE="${DEFAULT_RUNTIME_DIR}/latest_maintenance_supervisor_decision.json"
DEFAULT_MAINTENANCE_SUPERVISOR_SUMMARY_FILE="${DEFAULT_RUNTIME_DIR}/latest_maintenance_supervisor_summary.json"
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
CANONICAL_READINESS_FILE="${DEFAULT_CANONICAL_READINESS_FILE}"
CANONICAL_READINESS_SUMMARY_FILE="${DEFAULT_CANONICAL_READINESS_SUMMARY_FILE}"
MAINTENANCE_SUPERVISOR_FILE="${DEFAULT_MAINTENANCE_SUPERVISOR_FILE}"
MAINTENANCE_SUPERVISOR_SUMMARY_FILE="${DEFAULT_MAINTENANCE_SUPERVISOR_SUMMARY_FILE}"
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
    --canonical-readiness-output)
      CANONICAL_READINESS_FILE="$2"
      shift 2
      ;;
    --canonical-readiness-output=*)
      CANONICAL_READINESS_FILE="${1#*=}"
      shift
      ;;
    --canonical-readiness-summary-output)
      CANONICAL_READINESS_SUMMARY_FILE="$2"
      shift 2
      ;;
    --canonical-readiness-summary-output=*)
      CANONICAL_READINESS_SUMMARY_FILE="${1#*=}"
      shift
      ;;
    --maintenance-supervisor-output)
      MAINTENANCE_SUPERVISOR_FILE="$2"
      shift 2
      ;;
    --maintenance-supervisor-output=*)
      MAINTENANCE_SUPERVISOR_FILE="${1#*=}"
      shift
      ;;
    --maintenance-supervisor-summary-output)
      MAINTENANCE_SUPERVISOR_SUMMARY_FILE="$2"
      shift 2
      ;;
    --maintenance-supervisor-summary-output=*)
      MAINTENANCE_SUPERVISOR_SUMMARY_FILE="${1#*=}"
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
ensure_dir "$(dirname "${CANONICAL_READINESS_FILE}")"
ensure_dir "$(dirname "${CANONICAL_READINESS_SUMMARY_FILE}")"
ensure_dir "$(dirname "${MAINTENANCE_SUPERVISOR_FILE}")"
ensure_dir "$(dirname "${MAINTENANCE_SUPERVISOR_SUMMARY_FILE}")"

refresh_broker_truth_lease() {
  "${PYTHON_BIN}" -m mgc_v05l.app.track_b_broker_truth_lease \
    --repo-root "${REPO_ROOT}" \
    --no-history \
    --json >/dev/null
}

set +e
refresh_broker_truth_lease
broker_truth_lease_exit_code=$?
set -e

refresh_canonical_readiness() {
  local tmp_summary
  tmp_summary="${CANONICAL_READINESS_SUMMARY_FILE}.tmp"
  rm -f "${tmp_summary}"
  if "${PYTHON_BIN}" -m mgc_v05l.app.track_b_canonical_readiness \
    --repo-root "${REPO_ROOT}" \
    --expected-root "${REPO_ROOT}" \
    --output-path "${CANONICAL_READINESS_FILE}" \
    --json > "${tmp_summary}"; then
    mv "${tmp_summary}" "${CANONICAL_READINESS_SUMMARY_FILE}"
    return 0
  fi
  local exit_code=$?
  if [[ -s "${tmp_summary}" ]]; then
    mv "${tmp_summary}" "${CANONICAL_READINESS_SUMMARY_FILE}"
  else
    rm -f "${tmp_summary}"
  fi
  return "${exit_code}"
}

set +e
refresh_canonical_readiness
canonical_readiness_exit_code=$?
set -e

canonical_readiness_classification() {
  "${PYTHON_BIN}" - <<'PY' "${CANONICAL_READINESS_SUMMARY_FILE}"
import json
import sys
from pathlib import Path

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
print(payload.get("classification") or "NOT_READY_CONFIG")
PY
}

print_canonical_readiness_summary() {
  "${PYTHON_BIN}" - <<'PY' "${CANONICAL_READINESS_SUMMARY_FILE}" >&2
import json
import sys
from pathlib import Path

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {"classification": "NOT_READY_CONFIG", "blockers": ["canonical_readiness_summary_missing"]}

print("canonical_readiness_summary:")
for key in (
    "classification",
    "blockers",
    "warnings",
    "root_match",
    "broker_truth_fresh",
    "broker_truth_lease_state",
    "broker_truth_lease_age_seconds",
    "broker_truth_lease_entry_seconds_remaining",
    "reconciliation_state",
    "eligible_lane_count",
    "quarantine_count",
):
    value = payload.get(key)
    if isinstance(value, list):
        value = ",".join(str(item) for item in value) if value else "none"
    print(f"  {key}={value}")
PY
}

merge_canonical_readiness_status() {
  "${PYTHON_BIN}" - <<'PY' "${STATUS_FILE}" "${CANONICAL_READINESS_FILE}" "${CANONICAL_READINESS_SUMMARY_FILE}" "${broker_truth_lease_exit_code}"
import json
import sys
from pathlib import Path

status_path = Path(sys.argv[1])
readiness_path = Path(sys.argv[2])
summary_path = Path(sys.argv[3])

try:
    status = json.loads(status_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    status = {}
try:
    readiness = json.loads(readiness_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    readiness = {}
try:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    summary = {}

classification = summary.get("classification") or readiness.get("canonical_readiness") or "NOT_READY_CONFIG"
status["canonical_readiness"] = classification
status["canonical_readiness_summary"] = summary
status["canonical_readiness_artifact"] = str(readiness_path)
status["readiness_blockers"] = readiness.get("readiness_blockers") or []
status["readiness_warnings"] = readiness.get("readiness_warnings") or []
status["root_guard_summary"] = readiness.get("root_guard_summary") or {}
broker_truth_lease = readiness.get("broker_truth_lease") or {}
status["broker_truth_fresh"] = (readiness.get("broker_truth") or {}).get("fresh") is True
status["broker_truth_lease_state"] = broker_truth_lease.get("lease_state")
status["broker_truth_lease_age_seconds"] = broker_truth_lease.get("age_seconds")
status["broker_truth_lease_entry_seconds_remaining"] = broker_truth_lease.get("entry_seconds_remaining")
status["broker_truth_lease_exit_code"] = int(sys.argv[4])
status["eligible_lane_count"] = int((readiness.get("runtime") or {}).get("eligible_lane_count") or 0)
status["quarantine_count"] = int((readiness.get("lane_quarantine") or {}).get("quarantine_count") or 0)
status["lane_quarantine"] = readiness.get("lane_quarantine") or {}
status["ready_submit_capable"] = classification == "READY_SUBMIT_CAPABLE"
status["ready_observation_only"] = classification == "READY_OBSERVATION_ONLY"
status["degraded_no_submit"] = classification == "DEGRADED_NO_SUBMIT"
status["paper_only"] = True
status["live_money_eligible"] = False

status_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

refresh_maintenance_supervisor() {
  local tmp_summary
  tmp_summary="${MAINTENANCE_SUPERVISOR_SUMMARY_FILE}.tmp"
  rm -f "${tmp_summary}"
  if "${PYTHON_BIN}" -m mgc_v05l.app.track_b_readiness_maintenance_supervisor \
    --repo-root "${REPO_ROOT}" \
    --output-path "${MAINTENANCE_SUPERVISOR_FILE}" \
    --json > "${tmp_summary}"; then
    mv "${tmp_summary}" "${MAINTENANCE_SUPERVISOR_SUMMARY_FILE}"
    return 0
  fi
  local exit_code=$?
  if [[ -s "${tmp_summary}" ]]; then
    mv "${tmp_summary}" "${MAINTENANCE_SUPERVISOR_SUMMARY_FILE}"
  else
    rm -f "${tmp_summary}"
  fi
  return "${exit_code}"
}

print_maintenance_supervisor_summary() {
  "${PYTHON_BIN}" - <<'PY' "${MAINTENANCE_SUPERVISOR_SUMMARY_FILE}" >&2
import json
import sys
from pathlib import Path

try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {"supervisor_state": "BLOCKED", "recommended_actions": ["supervisor_summary_missing"]}

print("maintenance_supervisor_summary:")
for key in (
    "supervisor_state",
    "recommended_actions",
    "action_scope",
    "blockers",
    "warnings",
    "operator_action_required",
    "submit_block_required",
):
    value = payload.get(key)
    if isinstance(value, list):
        value = ",".join(str(item) for item in value) if value else "none"
    print(f"  {key}={value}")
PY
}

merge_maintenance_supervisor_status() {
  "${PYTHON_BIN}" - <<'PY' "${STATUS_FILE}" "${MAINTENANCE_SUPERVISOR_FILE}" "${MAINTENANCE_SUPERVISOR_SUMMARY_FILE}"
import json
import sys
from pathlib import Path

status_path = Path(sys.argv[1])
decision_path = Path(sys.argv[2])
summary_path = Path(sys.argv[3])

try:
    status = json.loads(status_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    status = {}
try:
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    decision = {}
try:
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    summary = {}

status["maintenance_supervisor"] = {
    "supervisor_state": summary.get("supervisor_state") or decision.get("supervisor_state") or "BLOCKED",
    "recommended_actions": summary.get("recommended_actions") or decision.get("recommended_actions") or [],
    "action_scope": summary.get("action_scope") or decision.get("action_scope") or [],
    "operator_action_required": summary.get("operator_action_required") is True
    or decision.get("operator_action_required") is True,
    "submit_block_required": summary.get("submit_block_required") is True
    or decision.get("submit_block_required") is True,
    "blockers": summary.get("blockers") or [row.get("code") for row in decision.get("blockers") or [] if isinstance(row, dict)],
    "warnings": summary.get("warnings") or [row.get("code") for row in decision.get("warnings") or [] if isinstance(row, dict)],
    "artifact": str(decision_path),
    "summary_artifact": str(summary_path),
}
status["maintenance_supervisor_state"] = status["maintenance_supervisor"]["supervisor_state"]
status["maintenance_supervisor_recommended_actions"] = status["maintenance_supervisor"]["recommended_actions"]
status["maintenance_supervisor_operator_action_required"] = status["maintenance_supervisor"]["operator_action_required"]
status["maintenance_supervisor_submit_block_required"] = status["maintenance_supervisor"]["submit_block_required"]
status["paper_only"] = True
status["live_money_eligible"] = False

status_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

canonical_readiness_exit_for_classification() {
  case "$1" in
    READY_SUBMIT_CAPABLE|READY_OBSERVATION_ONLY)
      return 0
      ;;
    DEGRADED_NO_SUBMIT)
      return 1
      ;;
    *)
      return 2
      ;;
  esac
}

print_canonical_readiness_summary

set +e
refresh_maintenance_supervisor
maintenance_supervisor_exit_code=$?
set -e

print_maintenance_supervisor_summary

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

"${PYTHON_BIN}" -m mgc_v05l.app.main headless-supervised-paper-status \
  --health-file "${HEALTH_FILE}" \
  --startup-control-plane-file "${STARTUP_FILE}" \
  --supervised-operability-file "${OPERABILITY_FILE}" \
  --dashboard-info-file "${DEFAULT_INFO_FILE}" \
  --output "${STATUS_FILE}" \
  --markdown-output "${MARKDOWN_FILE}" >/dev/null

merge_canonical_readiness_status
merge_maintenance_supervisor_status
cat "${STATUS_FILE}"
canonical_state="$(canonical_readiness_classification)"
canonical_readiness_exit_for_classification "${canonical_state}"
