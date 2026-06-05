#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

EXPECTED_ROOT="/Users/patrick/Dev/MGC-v05l-automation"
if [[ "${REPO_ROOT}" != "${EXPECTED_ROOT}" ]]; then
  echo "BLOCKED_WRONG_REPO_ROOT: expected ${EXPECTED_ROOT}, got ${REPO_ROOT}" >&2
  exit 2
fi

RUNTIME_DIR="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime"
STACK_DIR="${REPO_ROOT}/outputs/track_b_execution_core/paper_stack"
RECOVERY_STATE_DIR="${REPO_ROOT}/outputs/track_b_execution_core/runtime_recovery"
CONTROL_PLANE_SNAPSHOT_FILE="${REPO_ROOT}/outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"
CANONICAL_READINESS_FILE="${REPO_ROOT}/outputs/operator_dashboard/runtime/latest_canonical_readiness.json"
STARTUP_ARTIFACT="${STACK_DIR}/latest_paper_stack_startup.json"
APPROVED_PROFILE_ARTIFACT="${RECOVERY_STATE_DIR}/approved_paper_stack_profile.json"
STATUS_SCRIPT="${SCRIPT_DIR}/track_b_status_paper_stack.sh"
RUNTIME_LOG="${RUNTIME_DIR}/probationary_paper.log"
PID_FILE="${RUNTIME_DIR}/probationary_paper.pid"
PID_METADATA_FILE="${RUNTIME_DIR}/probationary_paper.pid.json"
CONFIG_PATHS_FILE="${RUNTIME_DIR}/paper_runtime_config_paths.txt"
LAUNCH_STATUS_FILE="${RUNTIME_DIR}/probationary_paper_launch_status.json"
WRAPPER_PATH="${RUNTIME_DIR}/track_b_paper_stack_runtime_wrapper.sh"
LAUNCHCTL_LABEL_FILE="${PID_FILE}.launchctl_label"
LAUNCHCTL_STDOUT_FILE="${PID_FILE}.launchctl_submit.stdout"
LAUNCHCTL_STDERR_FILE="${PID_FILE}.launchctl_submit.stderr"
WAIT_SECONDS="${TRACK_B_PAPER_STACK_START_WAIT_SECONDS:-120}"
STABLE_SECONDS="${TRACK_B_PAPER_STACK_STABLE_SECONDS:-30}"
PREFERRED_CARRIER="${TRACK_B_PAPER_STACK_CARRIER:-auto}"
STACK_PROFILE="${TRACK_B_PAPER_STACK_PROFILE:-canonical}"
RESTART_REQUESTED="${TRACK_B_PAPER_STACK_RESTART:-0}"
SCOPED_CONFIG_PATH="${RUNTIME_DIR}/paper_stack_${STACK_PROFILE}.yaml"
SCOPED_ROSTER_PATH="${RUNTIME_DIR}/paper_stack_${STACK_PROFILE}_guarded_roster.json"
ROSTER_ENV_PATH=""
PROOF_REQUIRED_SYMBOLS=""
RECOVERY_SERVICE_OPT_OUT="${TRACK_B_PAPER_STACK_DISABLE_RECOVERY_SERVICE:-${MGC_TRACK_B_DISABLE_STANDALONE_RECOVERY:-0}}"
STARTUP_PREFLIGHT_REFRESH_ATTEMPTED="false"
STARTUP_PREFLIGHT_REFRESH_CLASSIFICATION="NOT_ATTEMPTED"
STARTUP_PREFLIGHT_REFRESHED_ARTIFACT_PATHS_JSON="[]"
STARTUP_PREFLIGHT_REMAINING_START_BLOCKERS_JSON="[]"

CANONICAL_CONFIGS=(
  "${REPO_ROOT}/config/base.yaml"
  "${REPO_ROOT}/config/live.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine.yaml"
  "${REPO_ROOT}/config/headless_supervised_paper_runtime.yaml"
  "${REPO_ROOT}/config/probationary_pattern_engine_paper.yaml"
)
FORBIDDEN_REVIEW_OVERLAY="${REPO_ROOT}/config/probationary_pattern_engine_paper_mnq_mgc_plus_mnq_us_intraday_review.yaml"

mkdir -p "${RUNTIME_DIR}" "${STACK_DIR}"

recovery_service_opted_out() {
  case "${RECOVERY_SERVICE_OPT_OUT}" in
    1|true|TRUE|yes|YES)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

ensure_recovery_service_enabled() {
  if recovery_service_opted_out; then
    return 0
  fi
  if [[ -x "${SCRIPT_DIR}/track_b_hourly_paper_runtime_recovery.sh" ]]; then
    if ! bash "${SCRIPT_DIR}/track_b_hourly_paper_runtime_recovery.sh" enable >/dev/null; then
      echo "WARNING_RECOVERY_SERVICE_ENABLE_FAILED: Track B standalone recovery could not be enabled; continuing canonical PAPER stack start." >&2
    fi
  fi
}

ensure_recovery_service_enabled

if [[ "${STACK_PROFILE}" == "mnq_mes_active_evidence" ]]; then
  cat > "${SCOPED_CONFIG_PATH}" <<'YAML'
probationary_paper_runtime_exclusive_config: true
probationary_paper_lanes_json: '[]'
YAML
  cat > "${SCOPED_ROSTER_PATH}" <<'JSON'
{
  "schema_version": "track_b_guarded_paper_roster_v1",
  "profile": "mnq_mes_active_evidence",
  "paper_account_id": "DUM882026",
  "live_money_eligible": false,
  "paper_proof_invoked": false,
  "enabled_strategy_ids": [
    "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_SHORT_V1"
  ],
  "disabled_strategy_ids": [],
  "max_quantity_per_strategy": 1
}
JSON
  CANONICAL_CONFIGS+=("${SCOPED_CONFIG_PATH}")
  ROSTER_ENV_PATH="${SCOPED_ROSTER_PATH}"
  PROOF_REQUIRED_SYMBOLS="MNQ,MES"
elif [[ "${STACK_PROFILE}" == "mnq_mes_globex_active_evidence" ]]; then
  cat > "${SCOPED_CONFIG_PATH}" <<'YAML'
probationary_paper_runtime_exclusive_config: true
probationary_paper_lanes_json: '[]'
YAML
  cat > "${SCOPED_ROSTER_PATH}" <<'JSON'
{
  "schema_version": "track_b_guarded_paper_roster_v1",
  "profile": "mnq_mes_globex_active_evidence",
  "paper_account_id": "DUM882026",
  "live_money_eligible": false,
  "paper_proof_invoked": false,
  "enabled_strategy_ids": [
    "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_SHORT_V1"
  ],
  "disabled_strategy_ids": [],
  "max_quantity_per_strategy": 1
}
JSON
  CANONICAL_CONFIGS+=("${SCOPED_CONFIG_PATH}")
  ROSTER_ENV_PATH="${SCOPED_ROSTER_PATH}"
  PROOF_REQUIRED_SYMBOLS="MNQ,MES"
elif [[ "${STACK_PROFILE}" == "mnq_mes_session_coverage_active_evidence" ]]; then
  cat > "${SCOPED_CONFIG_PATH}" <<'YAML'
probationary_paper_runtime_exclusive_config: true
probationary_paper_lanes_json: '[]'
YAML
  cat > "${SCOPED_ROSTER_PATH}" <<'JSON'
{
  "schema_version": "track_b_guarded_paper_roster_v1",
  "profile": "mnq_mes_session_coverage_active_evidence",
  "paper_account_id": "DUM882026",
  "live_money_eligible": false,
  "paper_proof_invoked": false,
  "enabled_strategy_ids": [
    "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_SHORT_V1"
  ],
  "disabled_strategy_ids": [],
  "max_quantity_per_strategy": 1
}
JSON
  CANONICAL_CONFIGS+=("${SCOPED_CONFIG_PATH}")
  ROSTER_ENV_PATH="${SCOPED_ROSTER_PATH}"
  PROOF_REQUIRED_SYMBOLS="MNQ,MES"
elif [[ "${STACK_PROFILE}" == "mnq_mes_london_open_active_evidence" ]]; then
  cat > "${SCOPED_CONFIG_PATH}" <<'YAML'
probationary_paper_runtime_exclusive_config: true
probationary_paper_lanes_json: '[]'
YAML
  cat > "${SCOPED_ROSTER_PATH}" <<'JSON'
{
  "schema_version": "track_b_guarded_paper_roster_v1",
  "profile": "mnq_mes_london_open_active_evidence",
  "extends_profile": "mnq_mes_session_coverage_active_evidence",
  "paper_account_id": "DUM882026",
  "live_money_eligible": false,
  "paper_proof_invoked": false,
  "enabled_strategy_ids": [
    "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1"
  ],
  "shadow_only_strategy_ids": [
    "PAPER_WATCH_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_LONG_SHADOW_V1",
    "PAPER_WATCH_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_SHORT_SHADOW_V1",
    "PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_LONG_SHADOW_V1",
    "PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_SHADOW_V1"
  ],
  "shadow_only_reason_codes": [
    "LONDON_LATE_CANONICAL_ANCHOR_NOT_YET_DEFINED_FOR_BROKER_AUTHORITY"
  ],
  "disabled_strategy_ids": [],
  "max_quantity_per_strategy": 1
}
JSON
  CANONICAL_CONFIGS+=("${SCOPED_CONFIG_PATH}")
  ROSTER_ENV_PATH="${SCOPED_ROSTER_PATH}"
  PROOF_REQUIRED_SYMBOLS="MNQ,MES"
elif [[ "${STACK_PROFILE}" == "mnq_mes_london_late_mnq_short_active_evidence" ]]; then
  cat > "${SCOPED_CONFIG_PATH}" <<'YAML'
probationary_paper_runtime_exclusive_config: true
probationary_paper_lanes_json: '[]'
YAML
  cat > "${SCOPED_ROSTER_PATH}" <<'JSON'
{
  "schema_version": "track_b_guarded_paper_roster_v1",
  "profile": "mnq_mes_london_late_mnq_short_active_evidence",
  "extends_profile": "mnq_mes_session_coverage_active_evidence",
  "paper_account_id": "DUM882026",
  "live_money_eligible": false,
  "paper_proof_invoked": false,
  "enabled_strategy_ids": [
    "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1"
  ],
  "shadow_only_strategy_ids": [
    "PAPER_WATCH_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_LONG_SHADOW_V1",
    "PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_LONG_SHADOW_V1",
    "PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_SHADOW_V1"
  ],
  "shadow_only_reason_codes": [
    "LONDON_LATE_MNQ_SHORT_ONLY_INITIAL_PAPER_ELEVATION",
    "COMBINED_MNQ_MES_LONDON_CONFLICT_GROUP_LIMITS_SESSION_TO_ONE_TRADE"
  ],
  "disabled_strategy_ids": [],
  "max_quantity_per_strategy": 1
}
JSON
  CANONICAL_CONFIGS+=("${SCOPED_CONFIG_PATH}")
  ROSTER_ENV_PATH="${SCOPED_ROSTER_PATH}"
  PROOF_REQUIRED_SYMBOLS="MNQ,MES"
elif [[ "${STACK_PROFILE}" == "mnq_mes_full_session_active_evidence" ]]; then
  cat > "${SCOPED_CONFIG_PATH}" <<'YAML'
probationary_paper_runtime_exclusive_config: true
probationary_paper_lanes_json: '[]'
YAML
  cat > "${SCOPED_ROSTER_PATH}" <<'JSON'
{
  "schema_version": "track_b_guarded_paper_roster_v1",
  "profile": "mnq_mes_full_session_active_evidence",
  "extends_profile": "mnq_mes_session_coverage_active_evidence",
  "paper_account_id": "DUM882026",
  "live_money_eligible": false,
  "paper_proof_invoked": false,
  "enabled_strategy_ids": [
    "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1"
  ],
  "shadow_only_strategy_ids": [
    "PAPER_WATCH_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_LONG_SHADOW_V1",
    "PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_LONG_SHADOW_V1",
    "PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_SHADOW_V1"
  ],
  "shadow_only_reason_codes": [
    "FULL_SESSION_PROFILE_INITIAL_LONDON_LATE_MNQ_SHORT_ONLY_ELEVATION",
    "COMBINED_MNQ_MES_LONDON_CONFLICT_GROUP_LIMITS_SESSION_TO_ONE_TRADE"
  ],
  "disabled_strategy_ids": [],
  "max_quantity_per_strategy": 1
}
JSON
  CANONICAL_CONFIGS+=("${SCOPED_CONFIG_PATH}")
  ROSTER_ENV_PATH="${SCOPED_ROSTER_PATH}"
  PROOF_REQUIRED_SYMBOLS="MNQ,MES"
elif [[ "${STACK_PROFILE}" != "canonical" ]]; then
  echo "BLOCKED_UNKNOWN_PROFILE: Unknown Track B PAPER stack profile: ${STACK_PROFILE}" >&2
  exit 2
fi

scoped_profile_start_allowed() {
  [[ "${STACK_PROFILE}" == "mnq_mes_active_evidence" || "${STACK_PROFILE}" == "mnq_mes_globex_active_evidence" || "${STACK_PROFILE}" == "mnq_mes_session_coverage_active_evidence" || "${STACK_PROFILE}" == "mnq_mes_london_open_active_evidence" || "${STACK_PROFILE}" == "mnq_mes_london_late_mnq_short_active_evidence" || "${STACK_PROFILE}" == "mnq_mes_full_session_active_evidence" ]] || return 1
  "${PYTHON_BIN}" - "${status_json}" "${REPO_ROOT}" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

status = json.loads(sys.argv[1])
repo_root = Path(sys.argv[2])
safety = status["safety"]
broker = status["broker_lifecycle"]
duplicate = status["duplicate_writer"]
config = status["config"]
if status["runtime"]["running"] is True:
    print("false")
    raise SystemExit
if safety["paper_only"] is not True or safety["live_money_eligible"] is not False:
    print("false")
    raise SystemExit
if safety["paper_proof_invoked"] is not False or safety["broker_mutation_allowed"] is not False:
    print("false")
    raise SystemExit
if "RECONCILED" not in str(broker.get("reconciliation_classification") or ""):
    print("false")
    raise SystemExit
if broker.get("broker_truth_fresh") is not True:
    print("false")
    raise SystemExit
if duplicate.get("duplicate_writer_detected") is True or config.get("review_overlay_active") is True:
    print("false")
    raise SystemExit

path = repo_root / "outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_listener_status.json"
try:
    listener = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    print("false")
    raise SystemExit
if str(listener.get("provider_status") or "").upper() != "RUNNING":
    print("false")
    raise SystemExit
if listener.get("listener_alive") is not True:
    print("false")
    raise SystemExit
if listener.get("source") != "DATABENTO_REALTIME_PHASE1":
    print("false")
    raise SystemExit
generated_at = listener.get("generated_at")
try:
    generated = datetime.fromisoformat(str(generated_at).replace("Z", "+00:00"))
except ValueError:
    print("false")
    raise SystemExit
if (datetime.now(timezone.utc) - generated).total_seconds() > 180:
    print("false")
    raise SystemExit
rows = {str(row.get("symbol") or "").upper(): row for row in listener.get("rows") or [] if isinstance(row, dict)}
required = {"MNQ", "MES"}
for symbol in required:
    row = rows.get(symbol) or {}
    latest = row.get("latest_completed_bar_ts")
    try:
        latest_ts = datetime.fromisoformat(str(latest).replace("Z", "+00:00"))
    except ValueError:
        print("false")
        raise SystemExit
    age = (datetime.now(timezone.utc) - latest_ts).total_seconds()
    bar_count = int(row.get("bar_count") or 0)
    min_bars = int(row.get("min_confirmed_bars") or 8)
    if age > float(row.get("freshness_threshold_seconds") or 180):
        print("false")
        raise SystemExit
    if bar_count < min_bars:
        print("false")
        raise SystemExit
    if row.get("realtime_feed_confirmed") is not True:
        print("false")
        raise SystemExit
print("true")
PY
}

write_startup_artifact() {
  local classification="$1"
  local detail="$2"
  local pid="${3:-}"
  MGC_STARTUP_PREFLIGHT_REFRESH_ATTEMPTED="${STARTUP_PREFLIGHT_REFRESH_ATTEMPTED}" \
  MGC_STARTUP_PREFLIGHT_REFRESH_CLASSIFICATION="${STARTUP_PREFLIGHT_REFRESH_CLASSIFICATION}" \
  MGC_STARTUP_PREFLIGHT_REFRESHED_ARTIFACT_PATHS_JSON="${STARTUP_PREFLIGHT_REFRESHED_ARTIFACT_PATHS_JSON}" \
  MGC_STARTUP_PREFLIGHT_REMAINING_START_BLOCKERS_JSON="${STARTUP_PREFLIGHT_REMAINING_START_BLOCKERS_JSON}" \
  "${PYTHON_BIN}" - "$STARTUP_ARTIFACT" "$classification" "$detail" "$pid" "$REPO_ROOT" "$CONFIG_PATHS_FILE" "$STACK_PROFILE" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

artifact, classification, detail, pid, repo_root, config_paths_file, stack_profile = sys.argv[1:]
paths = []
try:
    paths = [line.strip() for line in Path(config_paths_file).read_text(encoding="utf-8").splitlines() if line.strip()]
except OSError:
    pass

def env_json_list(name):
    try:
        value = json.loads(os.environ.get(name, "[]"))
    except json.JSONDecodeError:
        return []
    return value if isinstance(value, list) else []

payload = {
    "schema_version": "track_b_paper_stack_startup_v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "classification": classification,
    "detail": detail,
    "pid": int(pid) if pid.isdigit() else None,
    "repo_root": repo_root,
    "stack_profile": stack_profile,
    "config_stack": paths,
    "paper_only": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "broker_mutation": False,
    "dashboard_authority": False,
    "startup_preflight_refresh_attempted": os.environ.get("MGC_STARTUP_PREFLIGHT_REFRESH_ATTEMPTED") == "true",
    "startup_preflight_refresh_classification": os.environ.get("MGC_STARTUP_PREFLIGHT_REFRESH_CLASSIFICATION") or "NOT_ATTEMPTED",
    "refreshed_artifact_paths": env_json_list("MGC_STARTUP_PREFLIGHT_REFRESHED_ARTIFACT_PATHS_JSON"),
    "remaining_start_blockers": env_json_list("MGC_STARTUP_PREFLIGHT_REMAINING_START_BLOCKERS_JSON"),
}
path = Path(artifact)
path.parent.mkdir(parents=True, exist_ok=True)
tmp = path.with_name(f".{path.name}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(path)
print(json.dumps(payload, indent=2, sort_keys=True))
PY
}

write_approved_profile_artifact() {
  if [[ "${STACK_PROFILE}" == "canonical" ]]; then
    return 0
  fi
  mkdir -p "${RECOVERY_STATE_DIR}"
  "${PYTHON_BIN}" - "$APPROVED_PROFILE_ARTIFACT" "$STACK_PROFILE" "$REPO_ROOT" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

artifact, stack_profile, repo_root = sys.argv[1:]
payload = {
    "schema_version": "track_b_approved_paper_stack_profile_v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "approved_profile": stack_profile,
    "recovery_requested_profile": stack_profile,
    "recovery_profile_source": "track_b_start_paper_stack_operator_profile",
    "recovery_profile_approved": True,
    "repo_root": repo_root,
    "paper_only": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "broker_mutation": False,
}
path = Path(artifact)
tmp = path.with_name(f".{path.name}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(path)
PY
}

startup_phase_timeout_detail() {
  "${PYTHON_BIN}" - <<'PY'
import json
import sys

try:
    payload = json.loads(sys.stdin.read() or "{}")
except json.JSONDecodeError:
    payload = {}
startup = payload.get("startup_phase") if isinstance(payload.get("startup_phase"), dict) else {}
phase = payload.get("startup_phase_current_phase") or startup.get("phase") or "UNKNOWN"
classification = payload.get("startup_phase_classification") or startup.get("classification") or "UNKNOWN"
blocker = payload.get("startup_phase_current_blocker") or (startup.get("current_blockers") or [{}])[0]
if not isinstance(blocker, dict):
    blocker = {}
blocker_phase = blocker.get("phase") or startup.get("next_expected_phase") or payload.get("startup_phase_next_expected_phase") or "UNKNOWN"
blocker_detail = blocker.get("detail") or blocker.get("code") or "startup_phase_blocker_unavailable"
print(
    "startup_phase="
    f"{phase}; startup_classification={classification}; "
    f"startup_blocker_phase={blocker_phase}; startup_blocker={blocker_detail}"
)
PY
}

run_startup_preflight_evidence_refresh() {
  STARTUP_PREFLIGHT_REFRESH_ATTEMPTED="true"
  local readiness_stdout="${STACK_DIR}/.startup_preflight_canonical_readiness.$$.json"
  local readiness_stderr="${STACK_DIR}/.startup_preflight_canonical_readiness.$$.stderr"
  local control_stdout="${STACK_DIR}/.startup_preflight_control_plane.$$.json"
  local control_stderr="${STACK_DIR}/.startup_preflight_control_plane.$$.stderr"
  local status_stdout="${STACK_DIR}/.startup_preflight_status.$$.json"
  local status_stderr="${STACK_DIR}/.startup_preflight_status.$$.stderr"
  local result_json="${STACK_DIR}/.startup_preflight_refresh_result.$$.json"
  local readiness_rc=0
  local control_rc=0
  local status_rc=0

  set +e
  "${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_readiness_state \
    --repo-root "${REPO_ROOT}" \
    --output "${CANONICAL_READINESS_FILE}" \
    > "${readiness_stdout}" 2> "${readiness_stderr}"
  readiness_rc=$?
  "${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_control_plane_snapshot \
    --repo-root "${REPO_ROOT}" \
    --output-path "${CONTROL_PLANE_SNAPSHOT_FILE}" \
    --no-dashboard-projection \
    --no-broker-lease-history \
    --json \
    > "${control_stdout}" 2> "${control_stderr}"
  control_rc=$?
  "${STATUS_SCRIPT}" --json > "${status_stdout}" 2> "${status_stderr}"
  status_rc=$?
  set -e

  "${PYTHON_BIN}" - \
    "${REPO_ROOT}" \
    "${status_stdout}" \
    "${readiness_stdout}" \
    "${control_stdout}" \
    "${readiness_rc}" \
    "${control_rc}" \
    "${status_rc}" \
    "${CANONICAL_READINESS_FILE}" \
    "${CONTROL_PLANE_SNAPSHOT_FILE}" \
    > "${result_json}" <<'PY'
import json
import sys
from pathlib import Path

(
    repo_root,
    status_path,
    readiness_stdout,
    control_stdout,
    readiness_rc,
    control_rc,
    status_rc,
    readiness_artifact,
    control_artifact,
) = sys.argv[1:]
repo_root = Path(repo_root)

def load_json(path_value):
    path = Path(path_value)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

def first_present(payload, *keys, default=None):
    for key in keys:
        if key in payload:
            return payload.get(key)
    return default

def add_path(paths, value):
    if not value:
        return
    if isinstance(value, str):
        paths.add(value)
        return
    if isinstance(value, dict):
        for item in value.values():
            add_path(paths, item)
        return
    if isinstance(value, list):
        for item in value:
            add_path(paths, item)

def add_blocker(blockers, code, detail=None, source=None):
    row = {"code": code}
    if detail:
        row["detail"] = str(detail)
    if source:
        row["source"] = str(source)
    blockers.append(row)

status = load_json(status_path)
readiness = load_json(readiness_stdout) or load_json(readiness_artifact)
control = load_json(control_stdout) or load_json(control_artifact)
reconciliation = load_json(
    repo_root
    / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
)

paths = {str(Path(readiness_artifact)), str(Path(control_artifact))}
add_path(paths, control.get("source_artifact_paths"))
add_path(paths, control.get("refreshed_artifact_paths"))

blockers = []
if int(readiness_rc) != 0:
    add_blocker(blockers, "canonical_readiness_refresh_failed", source="canonical_readiness")
if int(control_rc) != 0:
    add_blocker(blockers, "control_plane_refresh_failed", source="control_plane")
if int(status_rc) != 0:
    add_blocker(blockers, "paper_stack_status_refresh_failed", source="paper_stack_status")

safety = status.get("safety") if isinstance(status.get("safety"), dict) else {}
if safety.get("paper_only") is not True:
    add_blocker(blockers, "paper_only_not_confirmed", source="safety")
if safety.get("live_money_eligible") is not False:
    add_blocker(blockers, "live_money_eligible_not_false", source="safety")
if safety.get("paper_proof_invoked") is not False:
    add_blocker(blockers, "paper_proof_invoked_not_false", source="safety")
if safety.get("broker_mutation_allowed") is not False:
    add_blocker(blockers, "broker_mutation_allowed_not_false", source="safety")

recon_class = str(
    first_present(
        reconciliation,
        "classification",
        "reconciliation_classification",
        default=(status.get("broker_lifecycle") or {}).get("reconciliation_classification"),
    )
    or ""
)
if "RECONCILED" not in recon_class:
    add_blocker(blockers, "broker_lifecycle_not_reconciled", detail=recon_class, source="broker_lifecycle")
if first_present(reconciliation, "broker_reconciled", default=True) is False:
    add_blocker(blockers, "broker_lifecycle_reconciled_flag_false", source="broker_lifecycle")

broker_position_count = int(first_present(reconciliation, "track_b_broker_position_count", "broker_position_count", default=0) or 0)
broker_order_count = int(first_present(reconciliation, "track_b_broker_open_order_count", "broker_open_order_count", default=0) or 0)
lifecycle_position_count = int(
    first_present(
        reconciliation,
        "current_scope_lifecycle_open_position_count",
        "lifecycle_open_position_count",
        default=0,
    )
    or 0
)
lifecycle_order_count = int(first_present(reconciliation, "lifecycle_open_order_count", default=0) or 0)
if broker_position_count != 0 or broker_order_count != 0:
    add_blocker(
        blockers,
        "broker_positions_or_orders_not_flat",
        detail=f"positions={broker_position_count} orders={broker_order_count}",
        source="broker_lifecycle",
    )
if lifecycle_position_count != 0 or lifecycle_order_count != 0:
    add_blocker(
        blockers,
        "lifecycle_positions_or_orders_not_flat",
        detail=f"positions={lifecycle_position_count} orders={lifecycle_order_count}",
        source="broker_lifecycle",
    )

if control:
    if control.get("safe_to_start_runtime") is not True:
        add_blocker(
            blockers,
            "control_plane_start_not_allowed",
            detail=control.get("top_line_status") or control.get("classification"),
            source="control_plane",
        )
    primary_agent = control.get("primary_blocking_agent_id")
    primary_reason = control.get("primary_blocking_reason")
    if primary_agent or primary_reason:
        add_blocker(
            blockers,
            "control_plane_primary_blocker",
            detail=f"{primary_agent or 'unknown'}:{primary_reason or 'unknown'}",
            source="control_plane",
        )
    for row in control.get("agent_health_blockers") or []:
        if isinstance(row, dict):
            add_blocker(
                blockers,
                "agent_health_blocker",
                detail=f"{row.get('agent_id') or row.get('id') or row.get('source') or 'unknown'}:{row.get('reason') or row.get('code') or row.get('detail') or 'unknown'}",
                source="agent_health",
            )
else:
    add_blocker(blockers, "control_plane_snapshot_unavailable", source="control_plane")

if int(readiness_rc) == 0 and int(control_rc) == 0 and int(status_rc) == 0 and not blockers:
    classification = "STARTUP_PREFLIGHT_REFRESH_CLEAN"
elif int(readiness_rc) != 0 or int(control_rc) != 0 or int(status_rc) != 0:
    classification = "STARTUP_PREFLIGHT_REFRESH_FAILED"
else:
    classification = "STARTUP_PREFLIGHT_REFRESH_BLOCKED"

payload = {
    "classification": classification,
    "startup_preflight_refresh_attempted": True,
    "refreshed_artifact_paths": sorted(paths),
    "remaining_start_blockers": blockers,
    "canonical_readiness_classification": readiness.get("classification")
    or readiness.get("canonical_state")
    or readiness.get("readiness_classification"),
    "control_plane_classification": control.get("classification"),
}
print(json.dumps(payload, indent=2, sort_keys=True))
PY

  STARTUP_PREFLIGHT_REFRESH_CLASSIFICATION="$("${PYTHON_BIN}" -c 'import json,sys; print(json.load(open(sys.argv[1])).get("classification") or "UNKNOWN")' "${result_json}")"
  STARTUP_PREFLIGHT_REFRESHED_ARTIFACT_PATHS_JSON="$("${PYTHON_BIN}" -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1])).get("refreshed_artifact_paths") or []))' "${result_json}")"
  STARTUP_PREFLIGHT_REMAINING_START_BLOCKERS_JSON="$("${PYTHON_BIN}" -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1])).get("remaining_start_blockers") or []))' "${result_json}")"

  if [[ -s "${status_stdout}" ]]; then
    status_json="$(cat "${status_stdout}")"
  fi

  if [[ "${STARTUP_PREFLIGHT_REFRESH_CLASSIFICATION}" != "STARTUP_PREFLIGHT_REFRESH_CLEAN" ]]; then
    write_startup_artifact "BLOCKED_START_PREFLIGHT_REFRESH" "Startup preflight evidence refresh did not produce clean start authority." ""
    return 1
  fi
  return 0
}

screen_available() {
  local smoke_name="track_b_screen_smoke_$(date -u +%Y%m%dT%H%M%SZ)_$$"
  screen -wipe >/dev/null 2>&1 || true
  screen -dmS "${smoke_name}" sleep 35 >/dev/null 2>&1 || return 1
  sleep 1
  if screen -ls 2>/dev/null | grep -q "${smoke_name}"; then
    screen -S "${smoke_name}" -X quit >/dev/null 2>&1 || true
    return 0
  fi
  return 1
}

launchctl_available() {
  command -v launchctl >/dev/null 2>&1
}

status_json="$("${STATUS_SCRIPT}" --json)"
already_running="$("${PYTHON_BIN}" -c 'import json,sys; print(str(json.loads(sys.stdin.read())["runtime"]["running"]).lower())' <<<"${status_json}")"
if [[ "${already_running}" == "true" ]]; then
  pid="$("${PYTHON_BIN}" -c 'import json,sys; print(json.loads(sys.stdin.read())["runtime"]["pid"] or "")' <<<"${status_json}")"
  if [[ "${RESTART_REQUESTED}" != "1" ]]; then
    write_startup_artifact "ALREADY_RUNNING" "Track B PAPER runtime is already running; no start action taken." "${pid}"
    exit 0
  fi
  if [[ "${STACK_PROFILE}" == "canonical" ]]; then
    write_startup_artifact "BLOCKED_RESTART_REQUIRES_PROFILE" "Set TRACK_B_PAPER_STACK_PROFILE for an explicit restart generation." "${pid}"
    exit 2
  fi
  restart_precheck="$("${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_paper_stack_restart_precheck <<<"${status_json}")"
  restart_precheck_allowed="$("${PYTHON_BIN}" -c 'import json,sys; print(str(json.loads(sys.stdin.read()).get("restart_allowed") is True).lower())' <<<"${restart_precheck}")"
  restart_precheck_classification="$("${PYTHON_BIN}" -c 'import json,sys; print(json.loads(sys.stdin.read()).get("classification") or "")' <<<"${restart_precheck}")"
  restart_precheck_detail="$("${PYTHON_BIN}" -c 'import json,sys; print(json.loads(sys.stdin.read()).get("detail") or "")' <<<"${restart_precheck}")"
  if [[ "${restart_precheck_allowed}" != "true" ]]; then
    write_startup_artifact "${restart_precheck_classification:-BLOCKED_RESTART_PRECHECK}" "${restart_precheck_detail:-Broker/lifecycle/safety state is not clean enough for a controlled restart.}" "${pid}"
    exit 2
  fi
  write_startup_artifact "${restart_precheck_classification}" "${restart_precheck_detail}" "${pid}" >/dev/null
  PROBATIONARY_PAPER_PID_FILE="${PID_FILE}" bash "${SCRIPT_DIR}/stop_probationary_paper_soak.sh"
  status_json="$("${STATUS_SCRIPT}" --json)"
fi

restart_allowed="$("${PYTHON_BIN}" -c 'import json,sys; print(str(json.loads(sys.stdin.read())["readiness"]["restart_allowed_if_runtime_down"]).lower())' <<<"${status_json}")"
runtime_start_allowed="$("${PYTHON_BIN}" -c 'import json,sys; print(str(json.loads(sys.stdin.read())["readiness"].get("runtime_start_allowed") is True).lower())' <<<"${status_json}")"
blocker_count="$("${PYTHON_BIN}" -c 'import json,sys; print(len(json.loads(sys.stdin.read())["readiness"]["blockers"]))' <<<"${status_json}")"
launch_guard_restart_allowed="$("${PYTHON_BIN}" -c '
import json, sys
payload = json.loads(sys.stdin.read())
blockers = payload["readiness"]["blockers"]
safety = payload["safety"]
broker = payload["broker_lifecycle"]
config = payload["config"]
duplicate = payload["duplicate_writer"]
stale_runtime_truth_only = bool(blockers) and all(
    row.get("code") == "authority_artifact_stale" and row.get("source") == "runtime_truth"
    for row in blockers
)
ok = (
    stale_runtime_truth_only
    and payload["runtime"]["running"] is False
    and duplicate.get("launch_guard_classification") == "LAUNCH_STALE_PID_CLEANUP_ALLOWED"
    and duplicate.get("duplicate_writer_detected") is False
    and safety["paper_only"] is True
    and safety["live_money_eligible"] is False
    and safety["paper_proof_invoked"] is False
    and safety["broker_mutation_allowed"] is False
    and "RECONCILED" in str(broker.get("reconciliation_classification") or "")
    and broker.get("broker_truth_fresh") is True
    and config.get("review_overlay_active") is False
)
print(str(ok).lower())
' <<<"${status_json}")"
restart_authority="$("${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_paper_stack_restart_precheck <<<"${status_json}")"
restart_authority_allowed="$("${PYTHON_BIN}" -c 'import json,sys; p=json.loads(sys.stdin.read()); print(str(p.get("restart_allowed") is True).lower())' <<<"${restart_authority}")"
scoped_profile_allowed="$(scoped_profile_start_allowed || true)"
if [[ ( "${runtime_start_allowed}" != "true" || "${blocker_count}" != "0" ) && ( "${restart_allowed}" != "true" || "${blocker_count}" != "0" ) && "${launch_guard_restart_allowed}" != "true" && "${restart_authority_allowed}" != "true" && "${scoped_profile_allowed}" != "true" ]]; then
  write_startup_artifact "BLOCKED_PRECHECK" "Canonical readiness does not allow a clean PAPER runtime start." ""
  exit 2
fi
if ! run_startup_preflight_evidence_refresh; then
  exit 2
fi
write_approved_profile_artifact

for config_path in "${CANONICAL_CONFIGS[@]}"; do
  if [[ ! -f "${config_path}" ]]; then
    write_startup_artifact "BLOCKED_MISSING_CONFIG" "Missing canonical config: ${config_path}" ""
    exit 2
  fi
done
if [[ " ${CANONICAL_CONFIGS[*]} " == *" ${FORBIDDEN_REVIEW_OVERLAY} "* ]]; then
  write_startup_artifact "BLOCKED_FORBIDDEN_REVIEW_OVERLAY" "Forbidden 27-lane review overlay is in canonical config stack." ""
  exit 2
fi

: > "${CONFIG_PATHS_FILE}"
for config_path in "${CANONICAL_CONFIGS[@]}"; do
  printf '%s\n' "${config_path}" >> "${CONFIG_PATHS_FILE}"
done

session_name="track_b_paper_stack_$(date -u +%Y%m%dT%H%M%SZ)_$$"
config_stack="$(IFS=":"; printf "%s" "${CANONICAL_CONFIGS[*]}")"
source_commit="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
runtime_instance_id="track-b-paper-stack-$(date -u +%Y%m%dT%H%M%SZ)-$$"

cat > "${WRAPPER_PATH}" <<WRAPPER
#!/usr/bin/env bash
set -euo pipefail
cd "${REPO_ROOT}"
export REPO_ROOT="${REPO_ROOT}"
export PYTHON_BIN="${PYTHON_BIN}"
export PYTHONPATH="${REPO_ROOT}/src:\${PYTHONPATH:-}"
export MGC_PROBATIONARY_PAPER_CONFIG_PATHS="${config_stack}"
export MGC_HEADLESS_SUPERVISED_PAPER_CONFIG_PATHS="${config_stack}"
export MGC_HEADLESS_REQUIRED_PAPER_CONFIGS=""
export MGC_HEADLESS_REQUIRED_PAPER_CONFIG_PATHS=""
export TRACK_B_GUARDED_PAPER_ROSTER_PATH="${ROSTER_ENV_PATH}"
export TRACK_B_PAPER_PROOF_REQUIRED_SYMBOLS="${PROOF_REQUIRED_SYMBOLS}"
export MGC_HEADLESS_PAPER_PID_FILE="${PID_FILE}"
export MGC_TRACK_B_PAPER_PID_METADATA_FILE="${PID_METADATA_FILE}"
export MGC_HEADLESS_PAPER_LOG_FILE="${RUNTIME_LOG}"
export MGC_TRACK_B_RUNTIME_INSTANCE_ID="${runtime_instance_id}"
export MGC_TRACK_B_PAPER_RUNTIME_RESTART_GENERATION="1"
export MGC_TRACK_B_EXPECTED_PROJECT_ROOT="${REPO_ROOT}"
export MGC_TRACK_B_EXPECTED_SOURCE_COMMIT="${source_commit}"
mkdir -p "${RUNTIME_DIR}"
if "${PYTHON_BIN}" - <<'PY' "${RUNTIME_DIR}/paper_runtime_truth.json" "\$\$"
import json
import os
import sys
from pathlib import Path

truth_path = Path(sys.argv[1])
current_pid = int(sys.argv[2])
try:
    truth = json.loads(truth_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    raise SystemExit(1)
pid = truth.get("producer_pid") or truth.get("pid")
try:
    pid = int(pid)
except (TypeError, ValueError):
    raise SystemExit(1)
if pid == current_pid:
    raise SystemExit(1)
try:
    os.kill(pid, 0)
except OSError:
    raise SystemExit(1)
heartbeat = str(truth.get("heartbeat_state") or "").upper()
freshness = str(truth.get("freshness_state") or "").upper()
if heartbeat == "HEALTHY" and freshness == "FRESH":
    raise SystemExit(0)
raise SystemExit(1)
PY
then
  printf '%s\n' "track_b_paper_stack_wrapper_existing_runtime_detected generated_at=\$(date -u +%Y-%m-%dT%H:%M:%SZ) repo_root=${REPO_ROOT}" >> "${RUNTIME_LOG}"
  exit 0
fi
echo "\$\$" > "${PID_FILE}"
"${PYTHON_BIN}" - <<'PY' "${PID_METADATA_FILE}" "\$\$" "${runtime_instance_id}" "${REPO_ROOT}" "${source_commit}"
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

path = Path(sys.argv[1])
payload = {
    "schema_version": "track_b_paper_runtime_pid_metadata_v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "pid": int(sys.argv[2]),
    "producer_pid": int(sys.argv[2]),
    "runtime_instance_id": sys.argv[3],
    "root": sys.argv[4],
    "producer_root": sys.argv[4],
    "source_commit": sys.argv[5],
    "runtime_mode": "PAPER",
    "service_name": "track_b_paper_runtime",
    "paper_only": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "submit_authority": False,
    "readiness_authority": False,
    "restart_authority": False,
}
tmp = path.with_name(f".{path.name}.{sys.argv[2]}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\\n", encoding="utf-8")
tmp.replace(path)
PY
printf '%s\n' "track_b_paper_stack_wrapper_start generated_at=\$(date -u +%Y-%m-%dT%H:%M:%SZ) repo_root=${REPO_ROOT} config_stack=${config_stack}" >> "${RUNTIME_LOG}"
exec bash "${SCRIPT_DIR}/run_probationary_paper_soak.sh" \
  --pid-file "${PID_FILE}" \
  --log-file "${RUNTIME_LOG}" \
  --config-paths-file "${CONFIG_PATHS_FILE}" \
  --launch-status-file "${LAUNCH_STATUS_FILE}" \
  --schwab-config "${REPO_ROOT}/config/schwab.local.json" >> "${RUNTIME_LOG}" 2>&1
WRAPPER
chmod +x "${WRAPPER_PATH}"

carrier="screen"
if [[ "${PREFERRED_CARRIER}" == "nohup" ]]; then
  write_startup_artifact "BLOCKED_UNSUPPORTED_CARRIER" "nohup is not a durable Track B PAPER runtime carrier in this environment; use auto, screen, or launchctl." ""
  exit 1
elif [[ "${PREFERRED_CARRIER}" == "screen" ]]; then
  if ! screen_available; then
    write_startup_artifact "BLOCKED_SCREEN_UNAVAILABLE" "screen carrier was explicitly requested but detached screen sessions are not usable." ""
    exit 1
  fi
elif [[ "${PREFERRED_CARRIER}" == "launchctl" ]]; then
  if ! launchctl_available; then
    write_startup_artifact "BLOCKED_LAUNCHCTL_UNAVAILABLE" "launchctl carrier was explicitly requested but launchctl is unavailable." ""
    exit 1
  fi
elif ! screen_available; then
  if launchctl_available; then
    carrier="launchctl"
  else
    write_startup_artifact "BLOCKED_NO_DURABLE_CARRIER" "Neither screen nor launchctl is available for durable Track B PAPER runtime ownership." ""
    exit 1
  fi
fi

if [[ "${carrier}" == "screen" ]]; then
  screen -wipe >/dev/null 2>&1 || true
  printf '%s\n' "${session_name}" > "${PID_FILE}.screen_session"
  rm -f "${LAUNCHCTL_LABEL_FILE}"
  screen -dmS "${session_name}" /bin/bash "${WRAPPER_PATH}"
elif [[ "${carrier}" == "launchctl" ]]; then
  label="com.mgc-v05l.track-b-paper-stack.$(date -u +%Y%m%d%H%M%S).$$"
  printf '%s\n' "${label}" > "${LAUNCHCTL_LABEL_FILE}"
  rm -f "${PID_FILE}.screen_session"
  if ! launchctl submit \
    -l "${label}" \
    -o "${LAUNCHCTL_STDOUT_FILE}" \
    -e "${LAUNCHCTL_STDERR_FILE}" \
    -- /bin/bash "${WRAPPER_PATH}"; then
    write_startup_artifact "BLOCKED_LAUNCHCTL_SUBMIT_FAILED" "launchctl submit failed; inspect ${LAUNCHCTL_STDERR_FILE}." ""
    rm -f "${LAUNCHCTL_LABEL_FILE}"
    exit 1
  fi
fi

deadline=$((SECONDS + WAIT_SECONDS))
last_status=""
ready_since=0
ready_pid=""
while [[ "${SECONDS}" -lt "${deadline}" ]]; do
  sleep 2
  last_status="$("${STATUS_SCRIPT}" --json || true)"
  running="$("${PYTHON_BIN}" -c 'import json,sys; print(str(json.loads(sys.stdin.read())["runtime"]["running"]).lower())' <<<"${last_status}" 2>/dev/null || echo false)"
  ready="$("${PYTHON_BIN}" -c 'import json,sys; print(str(json.loads(sys.stdin.read())["readiness"]["ready_submit_capable"]).lower())' <<<"${last_status}" 2>/dev/null || echo false)"
  start_allowed="$("${PYTHON_BIN}" -c 'import json,sys; print(str(json.loads(sys.stdin.read())["readiness"].get("runtime_start_allowed") is True).lower())' <<<"${last_status}" 2>/dev/null || echo false)"
  scheduled_halt="$("${PYTHON_BIN}" -c 'import json,sys; p=json.loads(sys.stdin.read())["readiness"]; print(str(p.get("readiness_block_is_scheduled_halt") is True and p.get("ready_submit_capable") is not True).lower())' <<<"${last_status}" 2>/dev/null || echo false)"
  pid="$("${PYTHON_BIN}" -c 'import json,sys; print(json.loads(sys.stdin.read())["runtime"]["pid"] or "")' <<<"${last_status}" 2>/dev/null || true)"
  if [[ "${running}" == "true" && "${ready}" == "true" ]]; then
    if [[ "${ready_pid}" != "${pid}" ]]; then
      ready_pid="${pid}"
      ready_since="${SECONDS}"
    fi
    if (( SECONDS - ready_since >= STABLE_SECONDS )); then
      write_startup_artifact "READY_SUBMIT_CAPABLE" "Track B PAPER runtime started through paper stack command and remained READY_SUBMIT_CAPABLE for ${STABLE_SECONDS}s." "${pid}"
      exit 0
    fi
    write_startup_artifact "RUNTIME_RUNNING_WAITING_FOR_SUSTAINED_READINESS" "Runtime is READY_SUBMIT_CAPABLE; waiting for ${STABLE_SECONDS}s sustained readiness." "${pid}" >/dev/null
    continue
  fi
  if [[ "${running}" == "true" && "${start_allowed}" == "true" && "${scheduled_halt}" == "true" ]]; then
    if [[ "${ready_pid}" != "${pid}" ]]; then
      ready_pid="${pid}"
      ready_since="${SECONDS}"
    fi
    if (( SECONDS - ready_since >= STABLE_SECONDS )); then
      write_startup_artifact "READY_TO_START_DIAGNOSTIC_ONLY" "Track B PAPER runtime started before reopen and remained diagnostic-start-ready for ${STABLE_SECONDS}s; submit remains disabled until market data is fresh." "${pid}"
      exit 0
    fi
    write_startup_artifact "RUNTIME_RUNNING_WAITING_FOR_DIAGNOSTIC_START_STABILITY" "Runtime is alive during scheduled halt; waiting for ${STABLE_SECONDS}s sustained diagnostic start readiness." "${pid}" >/dev/null
    continue
  fi
  if [[ -n "${ready_pid}" && "${running}" != "true" ]]; then
    write_startup_artifact "BLOCKED_RUNTIME_EXITED_DURING_STARTUP" "Runtime reached initial readiness, then exited before ${STABLE_SECONDS}s sustained readiness." "${ready_pid}"
    exit 1
  fi
  if [[ "${running}" == "true" ]]; then
    write_startup_artifact "RUNTIME_RUNNING_WAITING_FOR_READINESS" "Runtime process is alive; waiting for READY_SUBMIT_CAPABLE convergence." "${pid}" >/dev/null
  fi
done

timeout_startup_phase_detail="$(startup_phase_timeout_detail <<<"${last_status}")"
write_startup_artifact "BLOCKED_START_TIMEOUT" "Runtime did not remain READY_SUBMIT_CAPABLE or scheduled-halt diagnostic-start-ready for ${STABLE_SECONDS}s before timeout; ${timeout_startup_phase_detail}; inspect status artifact and runtime log." ""
exit 1
