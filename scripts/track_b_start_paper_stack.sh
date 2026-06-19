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
POST_TRUTH_PROGRESS_FILE="${RUNTIME_DIR}/paper_post_truth_startup_progress.json"
LAUNCH_STATUS_FILE="${RUNTIME_DIR}/probationary_paper_launch_status.json"
DETACHED_CHILD_STATUS_FILE="${RUNTIME_DIR}/probationary_paper_detached_child_status.json"
RUNTIME_SUBMIT_GRANT_FILE="${RUNTIME_DIR}/paper_runtime_submit_authority_grant.json"
WRAPPER_PATH="${RUNTIME_DIR}/track_b_paper_stack_runtime_wrapper.sh"
LAUNCHCTL_LABEL_FILE="${PID_FILE}.launchctl_label"
LAUNCHCTL_STDOUT_FILE="${PID_FILE}.launchctl_submit.stdout"
LAUNCHCTL_STDERR_FILE="${PID_FILE}.launchctl_submit.stderr"
WAIT_SECONDS="${TRACK_B_PAPER_STACK_START_WAIT_SECONDS:-120}"
STABLE_SECONDS="${TRACK_B_PAPER_STACK_STABLE_SECONDS:-30}"
PREFERRED_CARRIER="${TRACK_B_PAPER_STACK_CARRIER:-auto}"
STACK_PROFILE="${TRACK_B_PAPER_STACK_PROFILE:-canonical}"
START_LOCK_DIR="${RUNTIME_DIR}/paper_stack_${STACK_PROFILE}.runtime_scope.lock"
START_LOCK_METADATA_FILE="${START_LOCK_DIR}/owner.json"
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
STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_ATTEMPTED="false"
STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_STEPS_JSON="[]"
STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_FAILURES_JSON="[]"
STARTUP_MODE="STANDARD_START"
STARTUP_OWNED_MANAGED_EXPOSURE_RESTORE_JSON="{}"
PAPER_MINIMAL_STARTUP_V1="${TRACK_B_PAPER_MINIMAL_STARTUP_V1:-1}"

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

paper_minimal_startup_enabled() {
  case "${PAPER_MINIMAL_STARTUP_V1}" in
    1|true|TRUE|yes|YES)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

ensure_recovery_service_enabled() {
  if paper_minimal_startup_enabled; then
    return 0
  fi
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

materialize_scoped_lane_config_from_roster() {
  local roster_path="$1"
  local source_config_path="$2"
  local output_config_path="$3"
  local repo_root="$4"
  "${PYTHON_BIN}" - "${roster_path}" "${source_config_path}" "${output_config_path}" "${repo_root}" <<'PY'
import json
import sys
from pathlib import Path

roster_path = Path(sys.argv[1])
source_config_path = Path(sys.argv[2])
output_config_path = Path(sys.argv[3])
repo_root = Path(sys.argv[4])


def _load_source_lanes(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        for line in text.splitlines():
            if not line.startswith("probationary_paper_lanes_json:"):
                continue
            raw = line.split(":", 1)[1].strip()
            if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
                raw = raw[1:-1]
            parsed = json.loads(raw)
            return [dict(row) for row in parsed if isinstance(row, dict)]
        return []
    return [dict(row) for row in payload.get("lanes") or [] if isinstance(row, dict)]


def _normalize_lane(row: dict) -> dict:
    lane = dict(row)
    lane["execution_mode"] = "IBKR_PAPER_BRIDGE"
    lane["current_order_destination"] = "ibkr_paper_bridge_submit_capable"
    lane["bridge_adapter_required"] = True
    runtime_overlay = dict(lane.get("runtime_overlay_params") or {})
    runtime_overlay["execution_mode"] = "IBKR_PAPER_BRIDGE"
    runtime_overlay["current_order_destination"] = "ibkr_paper_bridge_submit_capable"
    lane["runtime_overlay_params"] = runtime_overlay
    return lane


roster = json.loads(roster_path.read_text(encoding="utf-8"))
enabled = [str(value) for value in roster.get("enabled_strategy_ids") or [] if str(value)]
enabled_set = set(enabled)
lanes = []
seen_sources: set[str] = set()
for row in _load_source_lanes(source_config_path):
    sources = {str(value) for value in [*list(row.get("long_sources") or []), *list(row.get("short_sources") or [])]}
    matched = sources & enabled_set
    if matched:
        lanes.append(_normalize_lane(row))
        seen_sources.update(matched)

missing = [source_id for source_id in enabled if source_id not in seen_sources]
if missing:
    sys.path.insert(0, str(repo_root / "src"))
    from mgc_v05l.execution_core.track_b_shadow_promotion_contract import promoted_probationary_paper_lane_rows

    for row in promoted_probationary_paper_lane_rows({"enabled_strategy_ids": missing}):
        sources = {str(value) for value in [*list(row.get("long_sources") or []), *list(row.get("short_sources") or [])]}
        matched = sources & set(missing)
        if matched:
            lanes.append(_normalize_lane(row))
            seen_sources.update(matched)

missing = [source_id for source_id in enabled if source_id not in seen_sources]
if missing:
    raise SystemExit(f"BLOCKED_PROFILE_LANE_MATERIALIZATION_MISSING_SPECS: {','.join(missing)}")
if len(lanes) != len(enabled):
    raise SystemExit(
        "BLOCKED_PROFILE_LANE_MATERIALIZATION_COUNT_MISMATCH: "
        f"enabled={len(enabled)} lanes={len(lanes)}"
    )

lanes_json = json.dumps(lanes, separators=(",", ":"), sort_keys=True)
output_config_path.write_text(
    "probationary_paper_runtime_exclusive_config: true\n"
    f"probationary_paper_lanes_json: {lanes_json}\n",
    encoding="utf-8",
)
PY
}

scoped_profile_lane_source_config() {
  local existing_scoped_config="$1"
  local fallback_config="$2"
  if [[ -f "${existing_scoped_config}" ]]; then
    printf '%s\n' "${existing_scoped_config}"
  else
    printf '%s\n' "${fallback_config}"
  fi
}

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
    "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MGC_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MGC_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MGC_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MGC_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MGC_LONDON_OPEN_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_MGC_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_MGC_LONDON_LATE_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_GC_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_GC_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_GC_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_GC_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_GC_LONDON_OPEN_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_GC_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_GC_LONDON_LATE_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_NQ_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_NQ_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_NQ_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_NQ_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_NQ_LONDON_OPEN_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_NQ_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_NQ_LONDON_LATE_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ES_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ES_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ES_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ES_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ES_LONDON_OPEN_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ES_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ES_LONDON_LATE_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZT_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ZT_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZT_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ZT_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZT_LONDON_OPEN_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ZT_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZT_LONDON_LATE_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZF_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ZF_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZF_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ZF_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZF_LONDON_OPEN_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ZF_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZF_LONDON_LATE_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZN_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ZN_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZN_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ZN_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZN_LONDON_OPEN_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ZN_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZN_LONDON_LATE_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZB_US_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ZB_US_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZB_GLOBEX_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ZB_GLOBEX_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZB_LONDON_OPEN_PARTICIPATION_LONG_V1",
    "PAPER_ACTIVE_EVIDENCE_ZB_LONDON_OPEN_PARTICIPATION_SHORT_V1",
    "PAPER_ACTIVE_EVIDENCE_ZB_LONDON_LATE_PARTICIPATION_SHORT_V1",
    "MNQ_US_DERIVATIVE_BEAR_TURN_V1"
  ],
  "shadow_only_strategy_ids": [
    "PAPER_WATCH_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_LONG_SHADOW_V1",
    "PAPER_WATCH_ACTIVE_EVIDENCE_MES_LONDON_LATE_LONG_SHADOW_V1"
  ],
  "shadow_only_reason_codes": [
    "FULL_SESSION_PROFILE_INITIAL_LONDON_LATE_SHORT_ONLY_ELEVATION",
    "COMBINED_MNQ_MES_LONDON_CONFLICT_GROUP_LIMITS_SESSION_TO_ONE_TRADE"
  ],
  "disabled_strategy_ids": [],
  "max_quantity_per_strategy": 1
}
JSON
  materialize_scoped_lane_config_from_roster "${SCOPED_ROSTER_PATH}" "$(scoped_profile_lane_source_config "${SCOPED_CONFIG_PATH}" "${RUNTIME_DIR}/paper_config_in_force.json")" "${SCOPED_CONFIG_PATH}" "${REPO_ROOT}"
  CANONICAL_CONFIGS+=("${SCOPED_CONFIG_PATH}")
  ROSTER_ENV_PATH="${SCOPED_ROSTER_PATH}"
  PROOF_REQUIRED_SYMBOLS="GC,MGC,NQ,ES,ZT,ZF,ZN,ZB,MNQ,MES"
elif [[ "${STACK_PROFILE}" != "canonical" ]]; then
  echo "BLOCKED_UNKNOWN_PROFILE: Unknown Track B PAPER stack profile: ${STACK_PROFILE}" >&2
  exit 2
fi

scoped_profile_start_allowed() {
  [[ "${STACK_PROFILE}" == "mnq_mes_active_evidence" || "${STACK_PROFILE}" == "mnq_mes_globex_active_evidence" || "${STACK_PROFILE}" == "mnq_mes_session_coverage_active_evidence" || "${STACK_PROFILE}" == "mnq_mes_london_open_active_evidence" || "${STACK_PROFILE}" == "mnq_mes_london_late_mnq_short_active_evidence" || "${STACK_PROFILE}" == "mnq_mes_full_session_active_evidence" ]] || return 1
  "${PYTHON_BIN}" - "${status_json}" "${REPO_ROOT}" <<'PY'
import json
import os
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
  MGC_STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_ATTEMPTED="${STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_ATTEMPTED}" \
  MGC_STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_STEPS_JSON="${STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_STEPS_JSON}" \
  MGC_STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_FAILURES_JSON="${STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_FAILURES_JSON}" \
  MGC_TRACK_B_PAPER_STACK_STARTUP_MODE="${STARTUP_MODE}" \
  MGC_TRACK_B_PAPER_STACK_OWNED_MANAGED_EXPOSURE_RESTORE_JSON="${STARTUP_OWNED_MANAGED_EXPOSURE_RESTORE_JSON}" \
  MGC_TRACK_B_DETACHED_CHILD_STATUS_FILE="${DETACHED_CHILD_STATUS_FILE}" \
  "${PYTHON_BIN}" - "$STARTUP_ARTIFACT" "$classification" "$detail" "$pid" "$REPO_ROOT" "$CONFIG_PATHS_FILE" "$STACK_PROFILE" "$LAUNCH_STATUS_FILE" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

artifact, classification, detail, pid, repo_root, config_paths_file, stack_profile, launch_status_file = sys.argv[1:]
paths = []
try:
    paths = [line.strip() for line in Path(config_paths_file).read_text(encoding="utf-8").splitlines() if line.strip()]
except OSError:
    pass
try:
    launch_status = json.loads(Path(launch_status_file).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    launch_status = {}

def _read_json(path):
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}

def env_json_list(name):
    try:
        value = json.loads(os.environ.get(name, "[]"))
    except json.JSONDecodeError:
        return []
    return value if isinstance(value, list) else []

detached_child_status = (
    _read_json(Path(os.environ.get("MGC_TRACK_B_DETACHED_CHILD_STATUS_FILE", "")))
    if os.environ.get("MGC_TRACK_B_DETACHED_CHILD_STATUS_FILE")
    else {}
)
exit_source = detached_child_status

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
    "runtime_launch_status": launch_status or None,
    "runtime_detached_child_status": detached_child_status or None,
    "runtime_exit_status": {
        "classification": exit_source.get("classification"),
        "child_exit_code": exit_source.get("child_exit_code"),
        "child_exit_signal": exit_source.get("child_exit_signal"),
        "child_final_status": exit_source.get("child_final_status"),
        "final_pid_alive": (
            exit_source.get("final_pid_alive")
            if "final_pid_alive" in exit_source
            else exit_source.get("process_alive")
        ),
        "last_runtime_cycle_marker": exit_source.get("last_runtime_cycle_marker"),
        "termination_reason": exit_source.get("termination_reason"),
    } if exit_source else None,
    "startup_mode": os.environ.get("MGC_TRACK_B_PAPER_STACK_STARTUP_MODE") or "STANDARD_START",
    "owned_managed_exposure_maintenance_restore": json.loads(
        os.environ.get("MGC_TRACK_B_PAPER_STACK_OWNED_MANAGED_EXPOSURE_RESTORE_JSON") or "{}"
    ),
    "startup_preflight_refresh_attempted": os.environ.get("MGC_STARTUP_PREFLIGHT_REFRESH_ATTEMPTED") == "true",
    "startup_preflight_refresh_classification": os.environ.get("MGC_STARTUP_PREFLIGHT_REFRESH_CLASSIFICATION") or "NOT_ATTEMPTED",
    "startup_preflight_dependency_refresh_attempted": os.environ.get(
        "MGC_STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_ATTEMPTED"
    ) == "true",
    "dependency_refresh_steps": env_json_list("MGC_STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_STEPS_JSON"),
    "dependency_refresh_failures": env_json_list("MGC_STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_FAILURES_JSON"),
    "refreshed_artifact_paths": env_json_list("MGC_STARTUP_PREFLIGHT_REFRESHED_ARTIFACT_PATHS_JSON"),
    "remaining_start_blockers": env_json_list("MGC_STARTUP_PREFLIGHT_REMAINING_START_BLOCKERS_JSON"),
}
path = Path(artifact)
path.parent.mkdir(parents=True, exist_ok=True)
tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
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
import os
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
tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
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
  STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_ATTEMPTED="true"
  local broker_truth_stdout="${STACK_DIR}/.startup_preflight_broker_truth.$$.json"
  local broker_truth_stderr="${STACK_DIR}/.startup_preflight_broker_truth.$$.stderr"
  local reconciliation_stdout="${STACK_DIR}/.startup_preflight_reconciliation.$$.json"
  local reconciliation_stderr="${STACK_DIR}/.startup_preflight_reconciliation.$$.stderr"
  local open_order_stdout="${STACK_DIR}/.startup_preflight_open_order_truth.$$.json"
  local open_order_stderr="${STACK_DIR}/.startup_preflight_open_order_truth.$$.stderr"
  local managed_position_stdout="${STACK_DIR}/.startup_preflight_managed_positions.$$.json"
  local managed_position_stderr="${STACK_DIR}/.startup_preflight_managed_positions.$$.stderr"
  local managed_order_stdout="${STACK_DIR}/.startup_preflight_managed_orders.$$.json"
  local managed_order_stderr="${STACK_DIR}/.startup_preflight_managed_orders.$$.stderr"
  local shared_truth_stdout="${STACK_DIR}/.startup_preflight_shared_truth.$$.json"
  local shared_truth_stderr="${STACK_DIR}/.startup_preflight_shared_truth.$$.stderr"
  local readiness_stdout="${STACK_DIR}/.startup_preflight_canonical_readiness.$$.json"
  local readiness_stderr="${STACK_DIR}/.startup_preflight_canonical_readiness.$$.stderr"
  local control_stdout="${STACK_DIR}/.startup_preflight_control_plane.$$.json"
  local control_stderr="${STACK_DIR}/.startup_preflight_control_plane.$$.stderr"
  local status_stdout="${STACK_DIR}/.startup_preflight_status.$$.json"
  local status_stderr="${STACK_DIR}/.startup_preflight_status.$$.stderr"
  local result_json="${STACK_DIR}/.startup_preflight_refresh_result.$$.json"
  local broker_truth_rc=0
  local reconciliation_rc=0
  local open_order_rc=0
  local managed_position_rc=0
  local managed_order_rc=0
  local shared_truth_rc=0
  local readiness_rc=0
  local control_rc=0
  local status_rc=0

  set +e
  "${PYTHON_BIN}" -m mgc_v05l.app.ibkr_broker_truth_refresher \
    --once \
    --read-only \
    --mode PAPER \
    --account-id DUM882026 \
    > "${broker_truth_stdout}" 2> "${broker_truth_stderr}"
  broker_truth_rc=$?
  "${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_paper_broker_reconciliation \
    --repo-root "${REPO_ROOT}" \
    --account DUM882026 \
    --symbols MNQ,MES \
    > "${reconciliation_stdout}" 2> "${reconciliation_stderr}"
  reconciliation_rc=$?
  "${PYTHON_BIN}" -m mgc_v05l.app.track_b_open_order_truth \
    --repo-root "${REPO_ROOT}" \
    --once \
    > "${open_order_stdout}" 2> "${open_order_stderr}"
  open_order_rc=$?
  "${PYTHON_BIN}" -m mgc_v05l.app.track_b_managed_position_registry \
    --repo-root "${REPO_ROOT}" \
    --once \
    > "${managed_position_stdout}" 2> "${managed_position_stderr}"
  managed_position_rc=$?
  "${PYTHON_BIN}" -m mgc_v05l.app.track_b_managed_order_registry \
    --repo-root "${REPO_ROOT}" \
    --once \
    > "${managed_order_stdout}" 2> "${managed_order_stderr}"
  managed_order_rc=$?
  "${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_shared_truth_refresh_cli \
    --repo-root "${REPO_ROOT}" \
    --account DUM882026 \
    --symbols MNQ,MES \
    --runtime-start-preflight \
    --no-broker-lease-history \
    --json \
    > "${shared_truth_stdout}" 2> "${shared_truth_stderr}"
  shared_truth_rc=$?
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
    "${broker_truth_stdout}" \
    "${reconciliation_stdout}" \
    "${open_order_stdout}" \
    "${managed_position_stdout}" \
    "${managed_order_stdout}" \
    "${shared_truth_stdout}" \
    "${status_stdout}" \
    "${readiness_stdout}" \
    "${control_stdout}" \
    "${broker_truth_rc}" \
    "${reconciliation_rc}" \
    "${open_order_rc}" \
    "${managed_position_rc}" \
    "${managed_order_rc}" \
    "${shared_truth_rc}" \
    "${readiness_rc}" \
    "${control_rc}" \
    "${status_rc}" \
    "${CANONICAL_READINESS_FILE}" \
    "${CONTROL_PLANE_SNAPSHOT_FILE}" \
    "${STACK_PROFILE}" \
    > "${result_json}" <<'PY'
import json
import sys
from pathlib import Path

from mgc_v05l.execution_core.track_b_broker_startup_authority import (
    classify_fresh_complete_clean_broker_truth,
)

(
    repo_root,
    broker_truth_stdout,
    reconciliation_stdout,
    open_order_stdout,
    managed_position_stdout,
    managed_order_stdout,
    shared_truth_stdout,
    status_path,
    readiness_stdout,
    control_stdout,
    broker_truth_rc,
    reconciliation_rc,
    open_order_rc,
    managed_position_rc,
    managed_order_rc,
    shared_truth_rc,
    readiness_rc,
    control_rc,
    status_rc,
    readiness_artifact,
    control_artifact,
    stack_profile,
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

def to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0

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

def add_failure(failures, step, code, detail=None):
    row = {"step": step, "code": code}
    if detail:
        row["detail"] = str(detail)
    failures.append(row)

def list_rows(value):
    return value if isinstance(value, list) else []

def as_mapping(value):
    return value if isinstance(value, dict) else {}

def int_value(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(str(value)))
        except (TypeError, ValueError):
            return 0

def normalized_text(value):
    return str(value or "").strip().upper()

def control_plane_start_safe(control):
    classification = str(control.get("classification") or "").strip().upper()
    top_line = str(control.get("top_line_classification") or "").strip().upper()
    supervisor_mode = str(control.get("supervisor_mode") or "").strip().upper()
    ready_classifications = {"CONTROL_PLANE_SNAPSHOT_READY"}
    ready_modes = {"READY_FOR_OPERATOR_START"}
    return bool(
        control.get("safe_to_start_runtime") is True
        and (classification in ready_classifications or top_line in ready_modes or supervisor_mode in ready_modes)
    )

def control_plane_has_explicit_unsafe_status(control):
    fields = [
        control.get("classification"),
        control.get("top_line_classification"),
        control.get("supervisor_mode"),
        control.get("paper_action_policy"),
        control.get("safe_state_classification"),
    ]
    unsafe_markers = ("HARD_HOLD", "HARD_UNSAFE", "UNSAFE", "QUARANTINE", "BLOCKED")
    return any(
        any(marker in str(value or "").strip().upper() for marker in unsafe_markers)
        for value in fields
    )

def control_plane_maintenance_restore_blockers(control):
    rows = [*list_rows(control.get("blockers")), *list_rows(control.get("prioritized_blockers"))]
    disallowed = []
    allowed_agent_reasons = {
        ("canonical_readiness_refresher", "artifact_stale"),
        ("track_b_paper_runtime", "RUNTIME_DOWN_WITH_BROKER_EXPOSURE"),
        ("track_b_paper_runtime", "STOPPED_UNEXPECTED"),
    }
    for row in rows:
        if not isinstance(row, dict):
            disallowed.append(row)
            continue
        code = str(row.get("code") or "").strip()
        agent_id = str(row.get("agent_id") or row.get("id") or "").strip()
        reason = str(row.get("reason") or row.get("detail") or row.get("status") or "").strip()
        if code == "agent_health_blocks_runtime_submit":
            continue
        if code == "shared_truth_coherence_not_confirmed" and "STALE_OR_MIXED" in reason:
            continue
        if code == "shared_truth_coherence" and reason == "STALE_OR_MIXED":
            continue
        if code == "stale_or_mixed_source" and reason in {"Position Truth", "Broker Truth Lease"}:
            continue
        if (agent_id, reason) in allowed_agent_reasons:
            continue
        disallowed.append(row)
    return disallowed

def hard_unsafe_for_maintenance_restore(control):
    values = [
        control.get("safe_state_classification"),
        control.get("paper_action_policy"),
        control.get("supervisor_mode"),
        control.get("top_line_classification"),
    ]
    markers = ("HARD_HOLD", "HARD_UNSAFE", "UNSAFE", "QUARANTINE")
    return any(any(marker in normalized_text(value) for marker in markers) for value in values)

status = load_json(status_path)
broker_truth = load_json(broker_truth_stdout)
reconciliation_stdout_payload = load_json(reconciliation_stdout)
open_order_truth = load_json(open_order_stdout)
managed_positions = load_json(managed_position_stdout)
managed_orders = load_json(managed_order_stdout)
shared_truth = load_json(shared_truth_stdout)
readiness = load_json(readiness_stdout) or load_json(readiness_artifact)
control = load_json(control_stdout) or load_json(control_artifact)
safe_state = load_json(repo_root / "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json")
guardian = load_json(repo_root / "outputs/track_b_execution_core/broker_position_guardian/latest_broker_position_guardian.json")
managed_positions_artifact = load_json(
    repo_root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
)
managed_orders_artifact = load_json(
    repo_root / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json"
)
broker_positions_snapshot = load_json(
    broker_truth.get("positions_snapshot_path")
    or repo_root / "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json"
)
broker_open_orders_snapshot = load_json(
    broker_truth.get("open_orders_snapshot_path")
    or repo_root / "outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json"
)
reconciliation = reconciliation_stdout_payload or load_json(
    repo_root
    / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
)

broker_startup_authority = classify_fresh_complete_clean_broker_truth(
    broker_truth_status=broker_truth,
    positions_snapshot=broker_positions_snapshot,
    open_orders_snapshot=broker_open_orders_snapshot,
    reconciliation=reconciliation,
    open_order_truth=open_order_truth,
    status=status,
    safety=as_mapping(status.get("safety")),
    managed_positions=managed_positions or managed_positions_artifact,
    allow_known_managed_positions=True,
    expected_account_id="DUM882026",
).to_dict()

paths = {str(Path(readiness_artifact)), str(Path(control_artifact))}
for payload in (
    broker_truth,
    reconciliation_stdout_payload,
    open_order_truth,
    managed_positions,
    managed_orders,
    shared_truth,
):
    add_path(paths, payload.get("output_path"))
    add_path(paths, payload.get("artifact_path"))
    add_path(paths, payload.get("source_artifact_path"))
    add_path(paths, payload.get("source_artifact_paths"))
    add_path(paths, payload.get("refreshed_artifact_paths"))
add_path(paths, control.get("source_artifact_paths"))
add_path(paths, control.get("refreshed_artifact_paths"))

blockers = []
dependency_refresh_failures = []
if broker_startup_authority.get("broker_truth_clean") is not True:
    for code in broker_startup_authority.get("blockers") or []:
        add_blocker(
            blockers,
            f"broker_startup_authority_{code}",
            source="broker_startup_authority",
        )

def nested_runtime_preflight_classification(payload):
    runtime_start = payload.get("runtime_start_preflight")
    if isinstance(runtime_start, dict):
        return runtime_start.get("classification")
    return None

dependency_steps = [
    {
        "step": "broker_truth_broker_truth_lease_bsa",
        "return_code": to_int(broker_truth_rc),
        "classification": first_present(
            broker_truth,
            "classification",
            "broker_session_authority_classification",
            "broker_truth_lease_classification",
        ),
        "artifact_path": first_present(
            broker_truth,
            "output_path",
            "artifact_path",
            "broker_truth_lease_path",
            "broker_session_authority_path",
        ),
    },
    {
        "step": "broker_lifecycle_reconciliation",
        "return_code": to_int(reconciliation_rc),
        "classification": first_present(
            reconciliation,
            "classification",
            "reconciliation_classification",
        ),
        "artifact_path": first_present(reconciliation, "output_path", "artifact_path"),
    },
    {
        "step": "open_order_truth",
        "return_code": to_int(open_order_rc),
        "classification": first_present(open_order_truth, "classification", "order_truth_classification"),
        "artifact_path": first_present(open_order_truth, "output_path", "artifact_path"),
    },
    {
        "step": "managed_position_registry",
        "return_code": to_int(managed_position_rc),
        "classification": first_present(managed_positions, "classification", "managed_position_classification"),
        "artifact_path": first_present(managed_positions, "output_path", "artifact_path"),
    },
    {
        "step": "managed_order_registry",
        "return_code": to_int(managed_order_rc),
        "classification": first_present(managed_orders, "classification", "managed_order_classification"),
        "artifact_path": first_present(managed_orders, "output_path", "artifact_path"),
    },
    {
        "step": "shared_truth",
        "return_code": to_int(shared_truth_rc),
        "classification": first_present(shared_truth, "classification")
        or nested_runtime_preflight_classification(shared_truth),
        "artifact_path": first_present(shared_truth, "output_path", "artifact_path"),
    },
    {
        "step": "canonical_readiness",
        "return_code": to_int(readiness_rc),
        "classification": readiness.get("classification")
        or readiness.get("canonical_state")
        or readiness.get("readiness_classification"),
        "artifact_path": str(Path(readiness_artifact)),
    },
    {
        "step": "control_plane_snapshot",
        "return_code": to_int(control_rc),
        "classification": control.get("classification"),
        "artifact_path": str(Path(control_artifact)),
    },
    {
        "step": "paper_stack_status",
        "return_code": to_int(status_rc),
        "classification": first_present(status, "classification", "status_classification"),
        "artifact_path": first_present(status, "output_path", "artifact_path"),
    },
]

for step in dependency_steps:
    if step["step"] == "control_plane_snapshot" and step["return_code"] == 2 and control:
        continue
    if step["return_code"] != 0:
        code = f"{step['step']}_refresh_failed"
        add_failure(dependency_refresh_failures, step["step"], code, detail=f"return_code={step['return_code']}")
        add_blocker(blockers, code, source=step["step"])

if to_int(readiness_rc) != 0:
    add_blocker(blockers, "canonical_readiness_refresh_failed", source="canonical_readiness")
if to_int(control_rc) != 0 and not (to_int(control_rc) == 2 and control):
    add_blocker(blockers, "control_plane_refresh_failed", source="control_plane")
if to_int(status_rc) != 0:
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
unknown_broker_order_count = int(
    first_present(
        reconciliation,
        "unknown_broker_open_order_count",
        default=first_present(open_order_truth, "unknown_open_order_count", default=0),
    )
    or 0
)
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

open_order_classification = str(
    first_present(open_order_truth, "classification", "order_truth_classification", default="")
    or ""
)
if open_order_classification and open_order_classification != "NO_OPEN_ORDERS":
    add_blocker(
        blockers,
        "open_order_truth_not_clean",
        detail=open_order_classification,
        source="open_order_truth",
    )
if unknown_broker_order_count != 0:
    add_blocker(
        blockers,
        "unknown_open_orders_present",
        detail=unknown_broker_order_count,
        source="open_order_truth",
    )

managed_position_classification = str(
    first_present(managed_positions, "classification", "managed_position_classification", default="")
    or ""
)
if managed_position_classification and managed_position_classification not in {
    "NO_MANAGED_POSITIONS",
    "TRACK_B_MANAGED_POSITIONS_CLEAN_FLAT",
}:
    add_blocker(
        blockers,
        "managed_position_registry_not_clean",
        detail=managed_position_classification,
        source="managed_position_registry",
    )

managed_order_classification = str(
    first_present(managed_orders, "classification", "managed_order_classification", default="")
    or ""
)
if managed_order_classification and managed_order_classification != "NO_MANAGED_ORDERS":
    add_blocker(
        blockers,
        "managed_order_registry_not_clean",
        detail=managed_order_classification,
        source="managed_order_registry",
    )

shared_runtime_start = shared_truth.get("runtime_start_preflight")
if isinstance(shared_runtime_start, dict):
    if shared_runtime_start.get("clean_for_runtime_start") is False:
        add_blocker(
            blockers,
            "shared_truth_runtime_start_not_clean",
            detail=shared_runtime_start.get("classification"),
            source="shared_truth",
        )
    for row in list_rows(shared_runtime_start.get("blockers")):
        if isinstance(row, dict):
            add_blocker(
                blockers,
                "shared_truth_runtime_start_blocker",
                detail=row.get("code") or row.get("detail") or row.get("reason") or row,
                source="shared_truth",
            )

if control:
    is_start_safe = control_plane_start_safe(control)
    if not is_start_safe:
        add_blocker(
            blockers,
            "control_plane_start_not_allowed",
            detail=control.get("top_line_status") or control.get("classification"),
            source="control_plane",
        )
    primary_agent = control.get("primary_blocking_agent_id")
    primary_reason = control.get("primary_blocking_reason")
    control_blocker_rows = [
        *list_rows(control.get("blockers")),
        *list_rows(control.get("prioritized_blockers")),
    ]
    if control_blocker_rows:
        add_blocker(
            blockers,
            "control_plane_reported_blockers",
            detail=json.dumps(control_blocker_rows, sort_keys=True),
            source="control_plane",
        )
    if str(primary_agent or "").strip():
        add_blocker(
            blockers,
            "control_plane_primary_blocker",
            detail=f"{primary_agent or 'unknown'}:{primary_reason or 'unknown'}",
            source="control_plane",
        )
    elif primary_reason and not is_start_safe:
        add_blocker(
            blockers,
            "control_plane_primary_blocker",
            detail=f"unknown:{primary_reason}",
            source="control_plane",
        )
    if control_plane_has_explicit_unsafe_status(control):
        add_blocker(
            blockers,
            "control_plane_explicit_unsafe_status",
            detail=control.get("classification") or control.get("top_line_classification"),
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

def exact_owned_managed_exposure_restore_evidence():
    evidence_blockers = []
    if stack_profile != "mnq_mes_full_session_active_evidence":
        evidence_blockers.append("profile_not_approved_for_maintenance_restore")
    runtime = as_mapping(status.get("runtime"))
    live_runtime = as_mapping(status.get("live_runtime_environment"))
    liveness = as_mapping(live_runtime.get("liveness_contract"))
    runtime_down = runtime.get("running") is False or liveness.get("process_alive") is False
    if runtime_down is not True:
        evidence_blockers.append("runtime_not_confirmed_down")
    if "RECONCILED" not in recon_class or first_present(reconciliation, "broker_reconciled", default=True) is False:
        evidence_blockers.append("broker_lifecycle_not_clean")
    if broker_position_count <= 0 or lifecycle_position_count <= 0:
        evidence_blockers.append("owned_restore_requires_current_exposure")
    if broker_startup_authority.get("broker_truth_clean") is True and broker_startup_authority.get("known_managed_position_count", 0) > 0:
        owned_exposure_count = int_value(broker_startup_authority.get("known_managed_position_count"))
    elif broker_position_count != lifecycle_position_count:
        evidence_blockers.append("broker_lifecycle_exposure_count_mismatch")
    if broker_order_count != 0 or lifecycle_order_count != 0 or unknown_broker_order_count != 0:
        evidence_blockers.append("owned_restore_requires_zero_open_orders")
    if open_order_classification != "NO_OPEN_ORDERS":
        evidence_blockers.append("open_order_truth_not_clean_for_owned_restore")

    owner_resolution = as_mapping(reconciliation.get("current_exposure_owner_resolution"))
    if owner_resolution.get("classification") != "OWNED_MANAGED_EXPOSURE":
        evidence_blockers.append("owner_resolution_not_owned_managed_exposure")
    owned_exposure_count = int_value(owner_resolution.get("owned_exposure_count"))
    if broker_startup_authority.get("broker_truth_clean") is True and int_value(broker_startup_authority.get("known_managed_position_count")) > 0:
        owned_exposure_count = int_value(broker_startup_authority.get("known_managed_position_count"))
    if owned_exposure_count <= 0:
        evidence_blockers.append("owned_exposure_count_missing")
    if (
        broker_startup_authority.get("broker_truth_clean") is not True
        and (owned_exposure_count != broker_position_count or owned_exposure_count != lifecycle_position_count)
    ):
        evidence_blockers.append("owned_exposure_count_not_current_scope_count")
    if list_rows(owner_resolution.get("ambiguous_exposures")):
        evidence_blockers.append("ambiguous_owner_exposure_present")

    def identity_key(row):
        row = as_mapping(row)
        lifecycle_id = row.get("lifecycle_id")
        trade_id = row.get("trade_id")
        if not lifecycle_id or not trade_id:
            return None
        return (str(lifecycle_id), str(trade_id))

    def add_managed_candidate(candidates, row):
        row = as_mapping(row)
        key = identity_key(row)
        if not key:
            return
        candidates.setdefault(key, row)

    managed_candidates = {}
    for source_payload in (managed_positions, managed_positions_artifact):
        for row in list_rows(source_payload.get("managed_positions")):
            add_managed_candidate(managed_candidates, row)
    for row in list_rows(owner_resolution.get("owned_exposures")):
        row = as_mapping(row)
        add_managed_candidate(managed_candidates, row.get("lifecycle_position"))
        add_managed_candidate(managed_candidates, row)
    for source_payload in (managed_orders, managed_orders_artifact):
        for row in list_rows(source_payload.get("managed_orders")):
            row = as_mapping(row)
            add_managed_candidate(managed_candidates, row.get("canonical_managed_position"))
            broker_position = as_mapping(row.get("broker_position"))
            add_managed_candidate(managed_candidates, broker_position.get("canonical_managed_position"))

    managed_position_rows = list(managed_candidates.values())
    active_managed_rows = [
        row for row in managed_position_rows
        if normalized_text(row.get("classification")) in {"OPEN_MANAGED", "OPEN_MANAGED_MATCHED", "OPEN_MANAGED_EXIT_DUE"}
        or row.get("exit_due") is True
        or row.get("managed_exit_policy_id")
    ]
    if len(active_managed_rows) != owned_exposure_count:
        evidence_blockers.append("managed_position_count_not_current_owned_count")
    if not active_managed_rows:
        evidence_blockers.append("managed_position_classification_not_restore_safe")
    for row in active_managed_rows:
        if not (row.get("lifecycle_id") and row.get("trade_id")):
            evidence_blockers.append("managed_position_identity_missing")
        if not row.get("managed_exit_policy_id"):
            evidence_blockers.append("managed_exit_policy_missing")

    current_states = list_rows(as_mapping(status.get("registry_truth_diagnostics")).get("current_scope_trade_states"))
    if len(current_states) != owned_exposure_count:
        evidence_blockers.append("registry_current_scope_count_not_current_owned_count")
    active_identity_keys = {identity_key(row) for row in active_managed_rows}
    active_identity_keys.discard(None)
    for raw_state in current_states:
        state = as_mapping(raw_state)
        if state.get("current_derived_state") != "OPEN_MANAGED":
            evidence_blockers.append("registry_current_state_not_open_managed")
        if state.get("registry_agrees_with_reconciliation") is not True:
            evidence_blockers.append("registry_not_reconciled_to_current_exposure")
        if identity_key(state) not in active_identity_keys:
            evidence_blockers.append("registry_identity_differs_from_managed_position")

    if safe_state.get("classification") != "SAFE_STATE_NORMAL" and broker_startup_authority.get("broker_truth_clean") is not True:
        evidence_blockers.append("safe_state_not_normal")
    close_authority = as_mapping(safe_state.get("close_authority"))
    if close_authority.get("broad_flatten_allowed") is True or close_authority.get("global_flatten_allowed") is True:
        evidence_blockers.append("safe_state_flatten_path_available")
    if safe_state.get("live_money_eligible") is True or safe_state.get("paper_proof_invoked") is True:
        evidence_blockers.append("safe_state_live_money_or_paper_proof")

    if guardian.get("classification") != "BROKER_POSITION_GUARDIAN_READY" and broker_startup_authority.get("broker_truth_clean") is not True:
        evidence_blockers.append("guardian_not_ready")
    guardian_close = as_mapping(guardian.get("managed_close_authority"))
    if guardian_close.get("broad_flatten_allowed") is True or guardian_close.get("global_flatten_allowed") is True:
        evidence_blockers.append("guardian_flatten_path_available")
    if hard_unsafe_for_maintenance_restore(control):
        evidence_blockers.append("control_plane_hard_unsafe_for_maintenance_restore")
    if control_plane_maintenance_restore_blockers(control):
        evidence_blockers.append("control_plane_has_non_maintenance_blockers")
    if safety.get("live_money_eligible") is not False or safety.get("paper_proof_invoked") is not False:
        evidence_blockers.append("status_live_money_or_paper_proof")
    if safety.get("broker_mutation_allowed") is not False:
        evidence_blockers.append("status_broker_mutation_allowed")

    return {
        "allowed": not evidence_blockers,
        "blockers": evidence_blockers,
        "managed_exposure_count": len(active_managed_rows),
        "lifecycle_ids": [row.get("lifecycle_id") for row in active_managed_rows],
        "trade_ids": [row.get("trade_id") for row in active_managed_rows],
        "lifecycle_id": active_managed_rows[0].get("lifecycle_id") if len(active_managed_rows) == 1 else None,
        "trade_id": active_managed_rows[0].get("trade_id") if len(active_managed_rows) == 1 else None,
        "managed_position_classification": managed_position_classification,
        "managed_order_classification": managed_order_classification,
        "owner_resolution_classification": owner_resolution.get("classification"),
    }

maintenance_restore = exact_owned_managed_exposure_restore_evidence()
maintenance_allowed_blockers = {
    "broker_positions_or_orders_not_flat",
    "lifecycle_positions_or_orders_not_flat",
    "managed_position_registry_not_clean",
    "managed_order_registry_not_clean",
    "control_plane_snapshot_refresh_failed",
    "control_plane_refresh_failed",
    "control_plane_start_not_allowed",
    "control_plane_reported_blockers",
    "control_plane_primary_blocker",
    "control_plane_explicit_unsafe_status",
}
broker_truth_authority_diagnostic_blockers = {
    "broker_lifecycle_reconciliation_refresh_failed",
    "broker_lifecycle_not_reconciled",
    "broker_lifecycle_reconciled_flag_false",
    "broker_positions_or_orders_not_flat",
    "open_order_truth_refresh_failed",
    "lifecycle_positions_or_orders_not_flat",
    "open_order_truth_not_clean",
    "managed_position_registry_refresh_failed",
    "managed_position_registry_not_clean",
    "managed_order_registry_refresh_failed",
    "managed_order_registry_not_clean",
    "shared_truth_refresh_failed",
    "shared_truth_runtime_start_not_clean",
    "shared_truth_runtime_start_blocker",
}
broker_truth_authority_diagnostic_steps = {
    "broker_lifecycle_reconciliation",
    "open_order_truth",
    "managed_position_registry",
    "managed_order_registry",
    "shared_truth",
}
startup_mode = "OWNED_MANAGED_EXPOSURE_MAINTENANCE_RESTORE" if maintenance_restore["allowed"] else "STANDARD_START"
effective_blockers = [
    row for row in blockers
    if not (maintenance_restore["allowed"] and row.get("code") in maintenance_allowed_blockers)
    and not (
        broker_startup_authority.get("broker_truth_clean") is True
        and row.get("code") in broker_truth_authority_diagnostic_blockers
    )
]
effective_dependency_refresh_failures = [
    row for row in dependency_refresh_failures
    if not (maintenance_restore["allowed"] and row.get("code") == "control_plane_snapshot_refresh_failed")
    and not (
        broker_startup_authority.get("broker_truth_clean") is True
        and row.get("step") in broker_truth_authority_diagnostic_steps
    )
]

dependency_return_codes = [
    to_int(broker_truth_rc),
    to_int(reconciliation_rc),
    to_int(open_order_rc),
    to_int(managed_position_rc),
    to_int(managed_order_rc),
    to_int(shared_truth_rc),
    to_int(readiness_rc),
    to_int(control_rc),
    to_int(status_rc),
]
effective_dependency_return_codes = list(dependency_return_codes)
if to_int(control_rc) == 2 and control:
    effective_dependency_return_codes[7] = 0
if maintenance_restore["allowed"]:
    effective_dependency_return_codes[7] = 0
if broker_startup_authority.get("broker_truth_clean") is True:
    for index in (1, 2, 3, 4, 5):
        effective_dependency_return_codes[index] = 0

if all(rc == 0 for rc in effective_dependency_return_codes) and not effective_blockers:
    classification = "STARTUP_PREFLIGHT_REFRESH_CLEAN"
elif any(rc != 0 for rc in effective_dependency_return_codes):
    classification = "STARTUP_PREFLIGHT_REFRESH_FAILED"
else:
    classification = "STARTUP_PREFLIGHT_REFRESH_BLOCKED"

payload = {
    "classification": classification,
    "startup_preflight_refresh_attempted": True,
    "startup_preflight_dependency_refresh_attempted": True,
    "startup_mode": startup_mode,
    "owned_managed_exposure_maintenance_restore": maintenance_restore,
    "broker_startup_authority": broker_startup_authority,
    "dependency_refresh_steps": dependency_steps,
    "dependency_refresh_failures": effective_dependency_refresh_failures,
    "refreshed_artifact_paths": sorted(paths),
    "remaining_start_blockers": effective_blockers,
    "canonical_readiness_classification": readiness.get("classification")
    or readiness.get("canonical_state")
    or readiness.get("readiness_classification"),
    "control_plane_classification": control.get("classification"),
}
print(json.dumps(payload, indent=2, sort_keys=True))
PY

  STARTUP_PREFLIGHT_REFRESH_CLASSIFICATION="$("${PYTHON_BIN}" -c 'import json,sys; print(json.load(open(sys.argv[1])).get("classification") or "UNKNOWN")' "${result_json}")"
  STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_STEPS_JSON="$("${PYTHON_BIN}" -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1])).get("dependency_refresh_steps") or []))' "${result_json}")"
  STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_FAILURES_JSON="$("${PYTHON_BIN}" -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1])).get("dependency_refresh_failures") or []))' "${result_json}")"
  STARTUP_PREFLIGHT_REFRESHED_ARTIFACT_PATHS_JSON="$("${PYTHON_BIN}" -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1])).get("refreshed_artifact_paths") or []))' "${result_json}")"
  STARTUP_PREFLIGHT_REMAINING_START_BLOCKERS_JSON="$("${PYTHON_BIN}" -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1])).get("remaining_start_blockers") or []))' "${result_json}")"
  STARTUP_MODE="$("${PYTHON_BIN}" -c 'import json,sys; print(json.load(open(sys.argv[1])).get("startup_mode") or "STANDARD_START")' "${result_json}")"
  STARTUP_OWNED_MANAGED_EXPOSURE_RESTORE_JSON="$("${PYTHON_BIN}" -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1])).get("owned_managed_exposure_maintenance_restore") or {}))' "${result_json}")"

  if [[ -s "${status_stdout}" ]]; then
    status_json="$(cat "${status_stdout}")"
  fi

  if [[ "${STARTUP_PREFLIGHT_REFRESH_CLASSIFICATION}" != "STARTUP_PREFLIGHT_REFRESH_CLEAN" ]]; then
    write_startup_artifact "BLOCKED_START_PREFLIGHT_REFRESH" "Startup preflight evidence refresh did not produce clean start authority." ""
    return 1
  fi
  return 0
}

run_minimal_startup_broker_truth_refresh() {
  STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_ATTEMPTED="true"
  local broker_truth_stdout="${STACK_DIR}/.minimal_startup_broker_truth.$$.json"
  local broker_truth_stderr="${STACK_DIR}/.minimal_startup_broker_truth.$$.stderr"
  local broker_truth_rc=0

  set +e
  "${PYTHON_BIN}" -m mgc_v05l.app.ibkr_broker_truth_refresher \
    --once \
    --read-only \
    --mode PAPER \
    --account-id DUM882026 \
    > "${broker_truth_stdout}" 2> "${broker_truth_stderr}"
  broker_truth_rc=$?
  set -e

  STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_STEPS_JSON="$("${PYTHON_BIN}" -c '
import json
import sys
from pathlib import Path

stdout_path = Path(sys.argv[1])
stderr_path = Path(sys.argv[2])
rc = int(sys.argv[3])
payload = {}
try:
    payload = json.loads(stdout_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
print(json.dumps([{
    "step": "minimal_startup_broker_truth_refresh",
    "return_code": rc,
    "classification": payload.get("classification") or "UNKNOWN",
    "artifact_path": str(stdout_path),
    "stderr_path": str(stderr_path),
}]))
' "${broker_truth_stdout}" "${broker_truth_stderr}" "${broker_truth_rc}")"
  STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_FAILURES_JSON="$("${PYTHON_BIN}" -c '
import json
import sys
from pathlib import Path

stdout_path = Path(sys.argv[1])
stderr_path = Path(sys.argv[2])
rc = int(sys.argv[3])
payload = {}
try:
    payload = json.loads(stdout_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
classification = str(payload.get("classification") or "")
if rc == 0 and classification == "BROKER_TRUTH_REFRESH_READY":
    print("[]")
else:
    print(json.dumps([{
        "step": "minimal_startup_broker_truth_refresh",
        "code": "minimal_startup_broker_truth_refresh_failed",
        "return_code": rc,
        "classification": classification or "UNKNOWN",
        "stderr_path": str(stderr_path),
    }]))
' "${broker_truth_stdout}" "${broker_truth_stderr}" "${broker_truth_rc}")"

  if [[ "${broker_truth_rc}" != "0" ]]; then
    STARTUP_PREFLIGHT_REFRESH_CLASSIFICATION="BROKER_TRUTH_REFRESH_FAILED"
    STARTUP_PREFLIGHT_REFRESHED_ARTIFACT_PATHS_JSON="$("${PYTHON_BIN}" -c 'import json,sys; print(json.dumps([sys.argv[1], sys.argv[2]]))' "${broker_truth_stdout}" "${broker_truth_stderr}")"
    STARTUP_PREFLIGHT_REMAINING_START_BLOCKERS_JSON='[{"code":"broker_truth_refresh_failed","source":"broker_truth","detail":"Minimal startup could not refresh fresh IBKR broker truth before evaluating startup authority."}]'
    return 1
  fi

  STARTUP_PREFLIGHT_REFRESHED_ARTIFACT_PATHS_JSON="$("${PYTHON_BIN}" -c '
import json
import sys
from pathlib import Path

paths = [sys.argv[1], sys.argv[2]]
try:
    payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    payload = {}
for key in ("positions_snapshot_path", "open_orders_snapshot_path", "latest_attempt_status_path"):
    value = payload.get(key)
    if value:
        paths.append(str(value))
print(json.dumps(paths))
' "${broker_truth_stdout}" "${broker_truth_stderr}")"
  return 0
}

run_paper_minimal_startup_preflight() {
  STARTUP_PREFLIGHT_REFRESH_ATTEMPTED="true"
  STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_ATTEMPTED="false"
  STARTUP_MODE="PAPER_MINIMAL_STARTUP_V1"
  local result_json="${STACK_DIR}/.paper_minimal_startup_v1.$$.json"
  if ! run_minimal_startup_broker_truth_refresh; then
    write_startup_artifact "BLOCKED_PAPER_MINIMAL_STARTUP_V1" "PAPER_MINIMAL_STARTUP_V1 could not refresh fresh broker truth before evaluating startup authority." ""
    return 1
  fi
  set +e
  "${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_paper_minimal_startup \
    --repo-root "${REPO_ROOT}" \
    --account-id DUM882026 \
    --json \
    > "${result_json}"
  local minimal_rc=$?
  set -e
  STARTUP_PREFLIGHT_REFRESH_CLASSIFICATION="$("${PYTHON_BIN}" -c 'import json,sys; print(json.load(open(sys.argv[1])).get("classification") or "UNKNOWN")' "${result_json}")"
  STARTUP_PREFLIGHT_REFRESHED_ARTIFACT_PATHS_JSON="$("${PYTHON_BIN}" -c 'import json,sys; print(json.dumps(list((json.load(open(sys.argv[1])).get("source_artifact_refs") or {}).values())))' "${result_json}")"
  STARTUP_PREFLIGHT_REMAINING_START_BLOCKERS_JSON="$("${PYTHON_BIN}" -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1])).get("blockers") or []))' "${result_json}")"
  STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_STEPS_JSON="$("${PYTHON_BIN}" -c 'import json,sys; p=json.load(open(sys.argv[1])); print(json.dumps([{"step":"paper_minimal_startup_v1","return_code":0 if p.get("allowed") is True else 2,"classification":p.get("classification"),"artifact_path":p.get("source_artifact_refs",{}).get("config_in_force")}]))' "${result_json}")"
  STARTUP_PREFLIGHT_DEPENDENCY_REFRESH_FAILURES_JSON="$("${PYTHON_BIN}" -c 'import json,sys; p=json.load(open(sys.argv[1])); print(json.dumps([] if p.get("allowed") is True else [{"step":"paper_minimal_startup_v1","code":"paper_minimal_startup_v1_blocked"}]))' "${result_json}")"
  if [[ "${minimal_rc}" != "0" || "${STARTUP_PREFLIGHT_REFRESH_CLASSIFICATION}" != "PAPER_MINIMAL_STARTUP_ALLOWED" ]]; then
    write_startup_artifact "BLOCKED_PAPER_MINIMAL_STARTUP_V1" "PAPER_MINIMAL_STARTUP_V1 did not satisfy the minimal PAPER submit-capable startup contract." ""
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

nohup_available() {
  command -v nohup >/dev/null 2>&1
}

cleanup_stale_runtime_carrier_artifacts() {
  "${PYTHON_BIN}" - \
    "${REPO_ROOT}" \
    "${WRAPPER_PATH}" \
    "${SCOPED_CONFIG_PATH}" \
    "${LAUNCHCTL_LABEL_FILE}" \
    "${PID_FILE}.screen_session" <<'PY'
import json
import subprocess
import sys
from pathlib import Path

repo_root = str(Path(sys.argv[1]))
wrapper_path = str(Path(sys.argv[2]))
scoped_config_path = str(Path(sys.argv[3]))
launchctl_label_file = Path(sys.argv[4])
screen_session_file = Path(sys.argv[5])


def _rows() -> list[dict[str, object]]:
    ps = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,command="],
        text=True,
        check=False,
        capture_output=True,
    )
    rows: list[dict[str, object]] = []
    for line in ps.stdout.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) != 3:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
        except ValueError:
            continue
        command = parts[2]
        if repo_root not in command:
            continue
        is_wrapper = wrapper_path in command and "track_b_paper_stack_runtime_wrapper.sh" in command
        is_runtime_child = (
            ("mgc_v05l.app.main" in command and "probationary-paper-soak" in command)
            or "run_probationary_paper_soak.sh" in command
        )
        same_scope_child = is_runtime_child and (
            scoped_config_path in command or "paper_runtime_config_paths.txt" in command
        )
        if is_wrapper or same_scope_child:
            rows.append(
                {
                    "pid": pid,
                    "ppid": ppid,
                    "role": "wrapper" if is_wrapper else "runtime_child",
                    "command": command,
                }
            )
    return rows


active = _rows()
if active:
    print(json.dumps({"classification": "ACTIVE_RUNTIME_CARRIER_PRESENT", "active_processes": active}))
    raise SystemExit(0)

if launchctl_label_file.exists():
    label = launchctl_label_file.read_text(encoding="utf-8").strip()
    if label:
        try:
            subprocess.run(["launchctl", "remove", label], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except FileNotFoundError:
            pass
    launchctl_label_file.unlink(missing_ok=True)
screen_session_file.unlink(missing_ok=True)
print(json.dumps({"classification": "STALE_RUNTIME_CARRIER_ARTIFACTS_CLEANED"}))
PY
}

acquire_authoritative_runtime_scope_lock() {
  "${PYTHON_BIN}" - \
    "${START_LOCK_DIR}" \
    "${START_LOCK_METADATA_FILE}" \
    "${REPO_ROOT}" \
    "${WRAPPER_PATH}" \
    "${SCOPED_CONFIG_PATH}" \
    "${STACK_PROFILE}" \
    "$$" <<'PY'
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

lock_dir = Path(sys.argv[1])
metadata_file = Path(sys.argv[2])
repo_root = str(Path(sys.argv[3]))
wrapper_path = str(Path(sys.argv[4]))
scoped_config_path = str(Path(sys.argv[5]))
stack_profile = sys.argv[6]
launcher_pid = int(sys.argv[7])


def process_rows() -> list[dict[str, object]]:
    ps = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,command="],
        text=True,
        check=False,
        capture_output=True,
    )
    rows: list[dict[str, object]] = []
    for line in ps.stdout.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) != 3:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
        except ValueError:
            continue
        command = parts[2]
        if pid == launcher_pid:
            continue
        if ppid == launcher_pid and lock_dir.name in command and "track_b_paper_stack_runtime_wrapper.sh" in command:
            continue
        if repo_root not in command:
            continue
        is_wrapper = wrapper_path in command and "track_b_paper_stack_runtime_wrapper.sh" in command
        is_runtime_child = (
            ("mgc_v05l.app.main" in command and "probationary-paper-soak" in command)
            or "run_probationary_paper_soak.sh" in command
        )
        same_scope_child = is_runtime_child and (
            scoped_config_path in command or "paper_runtime_config_paths.txt" in command
        )
        is_launcher = "scripts/track_b_start_paper_stack.sh" in command
        if is_wrapper or same_scope_child or is_launcher:
            rows.append(
                {
                    "pid": pid,
                    "ppid": ppid,
                    "role": "wrapper" if is_wrapper else "runtime_child" if same_scope_child else "launcher",
                    "command": command,
                }
            )
    return rows


active_rows = process_rows()
active_wrappers_or_children = [row for row in active_rows if row["role"] in {"wrapper", "runtime_child"}]
active_launchers = [row for row in active_rows if row["role"] == "launcher"]

if active_wrappers_or_children:
    payload = {
        "classification": "BLOCKED_DUPLICATE_RUNTIME_CARRIER",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stack_profile": stack_profile,
        "repo_root": repo_root,
        "active_processes": active_wrappers_or_children,
        "lock_dir": str(lock_dir),
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    raise SystemExit(2)

if active_launchers:
    payload = {
        "classification": "BLOCKED_OVERLAPPING_RUNTIME_START",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stack_profile": stack_profile,
        "repo_root": repo_root,
        "active_processes": active_launchers,
        "lock_dir": str(lock_dir),
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    raise SystemExit(2)

if lock_dir.exists():
    try:
        shutil.rmtree(lock_dir)
    except OSError as exc:
        payload = {
            "classification": "BLOCKED_RUNTIME_SCOPE_LOCK_STALE_CLEANUP_FAILED",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "stack_profile": stack_profile,
            "repo_root": repo_root,
            "lock_dir": str(lock_dir),
            "error": str(exc),
            "paper_only": True,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        raise SystemExit(2)

try:
    lock_dir.mkdir(parents=True, exist_ok=False)
except FileExistsError:
    payload = {
        "classification": "BLOCKED_RUNTIME_SCOPE_LOCK_HELD",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stack_profile": stack_profile,
        "repo_root": repo_root,
        "lock_dir": str(lock_dir),
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    raise SystemExit(2)

payload = {
    "classification": "RUNTIME_SCOPE_LOCK_ACQUIRED",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "launcher_pid": launcher_pid,
    "stack_profile": stack_profile,
    "repo_root": repo_root,
    "wrapper_path": wrapper_path,
    "scoped_config_path": scoped_config_path,
    "lock_dir": str(lock_dir),
    "paper_only": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
}
tmp = metadata_file.with_name(f".{metadata_file.name}.{os.getpid()}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(metadata_file)
print(json.dumps(payload, indent=2, sort_keys=True))
PY
}

stop_exact_runtime_pid_for_minimal_restart() {
  local pid="$1"
  if [[ -z "${pid}" ]]; then
    rm -f "${PID_FILE}" "${PID_METADATA_FILE}" "${PID_FILE}.screen_session" "${LAUNCHCTL_LABEL_FILE}"
    return 0
  fi
  if ! ps -p "${pid}" >/dev/null 2>&1; then
    rm -f "${PID_FILE}" "${PID_METADATA_FILE}" "${PID_FILE}.screen_session" "${LAUNCHCTL_LABEL_FILE}"
    return 0
  fi

  kill -TERM "${pid}" >/dev/null 2>&1 || true
  local deadline=$((SECONDS + 15))
  while [[ "${SECONDS}" -lt "${deadline}" ]]; do
    if ! ps -p "${pid}" >/dev/null 2>&1; then
      rm -f "${PID_FILE}" "${PID_METADATA_FILE}" "${PID_FILE}.screen_session" "${LAUNCHCTL_LABEL_FILE}"
      return 0
    fi
    sleep 1
  done

  kill -KILL "${pid}" >/dev/null 2>&1 || true
  deadline=$((SECONDS + 5))
  while [[ "${SECONDS}" -lt "${deadline}" ]]; do
    if ! ps -p "${pid}" >/dev/null 2>&1; then
      rm -f "${PID_FILE}" "${PID_METADATA_FILE}" "${PID_FILE}.screen_session" "${LAUNCHCTL_LABEL_FILE}"
      return 0
    fi
    sleep 1
  done

  write_startup_artifact "BLOCKED_EXACT_PID_STOP_FAILED" "Controlled PAPER restart could not stop exact runtime PID ${pid}; no broad process stop attempted." "${pid}"
  exit 1
}

verify_direct_paper_runtime_shape() {
  local pid="$1"
  local expected_commit="$2"
  "${PYTHON_BIN}" - "${REPO_ROOT}" "${RUNTIME_DIR}" "${pid}" "${expected_commit}" "${STACK_PROFILE}" "${WRAPPER_PATH}" <<'PY'
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

repo_root = Path(sys.argv[1])
runtime_dir = Path(sys.argv[2])
pid = int(sys.argv[3])
expected_commit = sys.argv[4]
expected_profile = sys.argv[5]
wrapper_path = Path(sys.argv[6])
truth_path = runtime_dir / "paper_runtime_truth.json"
config_path = runtime_dir / "paper_config_in_force.json"

try:
    truth = json.loads(truth_path.read_text(encoding="utf-8"))
    config = json.loads(config_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    raise SystemExit(1)

try:
    truth_pid = int(truth.get("producer_pid") or truth.get("pid"))
except (TypeError, ValueError):
    raise SystemExit(1)
if truth_pid != pid:
    raise SystemExit(1)
if truth.get("source_commit") != expected_commit:
    raise SystemExit(1)
if config.get("profile") != expected_profile:
    raise SystemExit(1)
generated_at = str(truth.get("generated_at") or "").strip()
try:
    generated = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
except ValueError:
    raise SystemExit(1)
if generated.tzinfo is None:
    generated = generated.replace(tzinfo=timezone.utc)

progress_fresh_for_post_truth_startup = False
try:
    progress = json.loads((runtime_dir / "paper_post_truth_startup_progress.json").read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    progress = {}
if progress:
    try:
        progress_pid = int(progress.get("producer_pid") or progress.get("pid"))
    except (TypeError, ValueError):
        progress_pid = None
    progress_runtime = str(progress.get("runtime_instance_id") or "").strip()
    truth_runtime = str(truth.get("runtime_instance_id") or "").strip()
    progress_state = str(progress.get("state") or "").strip().upper()
    progress_stage = str(progress.get("stage") or "").strip()
    progress_generated_at = str(progress.get("heartbeat_at") or progress.get("generated_at") or "").strip()
    try:
        progress_generated = datetime.fromisoformat(progress_generated_at.replace("Z", "+00:00"))
    except ValueError:
        progress_generated = None
    if progress_generated is not None:
        if progress_generated.tzinfo is None:
            progress_generated = progress_generated.replace(tzinfo=timezone.utc)
        progress_fresh_for_post_truth_startup = (
            progress_pid == pid
            and (not progress_runtime or not truth_runtime or progress_runtime == truth_runtime)
            and progress_state in {"STARTED", "IN_PROGRESS", "COMPLETED", "AWAITING_SUBMIT_AUTHORITY", "TRADING_LOOP_ENTERED"}
            and progress_stage in {"authority_refresh", "watchdog_liveness_refresh", "lane_restore", "runtime_cycle"}
            and (datetime.now(timezone.utc) - progress_generated).total_seconds() <= 15
        )

if (datetime.now(timezone.utc) - generated).total_seconds() > 90 and not progress_fresh_for_post_truth_startup:
    raise SystemExit(1)
if str(truth.get("freshness_state") or "").strip().upper() not in {"", "FRESH"}:
    raise SystemExit(1)
if str(truth.get("heartbeat_state") or "").strip().upper() not in {"", "HEALTHY"}:
    raise SystemExit(1)

lanes = [row for row in config.get("lanes") or [] if isinstance(row, dict)]
active_lane_ids = [str(value) for value in config.get("active_lane_ids") or [] if str(value)]
if expected_profile == "mnq_mes_full_session_active_evidence":
    expected_lane_count = 71
else:
    expected_lane_count = len(active_lane_ids) or len(lanes)
if len(lanes) != expected_lane_count:
    raise SystemExit(1)
if int(truth.get("lane_count") or 0) != expected_lane_count:
    raise SystemExit(1)
if active_lane_ids and len(active_lane_ids) != expected_lane_count:
    raise SystemExit(1)

execution_modes = {
    str(row.get("execution_mode") or (row.get("runtime_overlay_params") or {}).get("execution_mode") or "")
    for row in lanes
}
if execution_modes != {"IBKR_PAPER_BRIDGE"}:
    raise SystemExit(1)
if any("SIMULATION" in mode for mode in execution_modes):
    raise SystemExit(1)

ps = subprocess.run(
    ["ps", "-axo", "pid=,ppid=,command="],
    text=True,
    check=False,
    capture_output=True,
)
runtime_pids = []
wrapper_pids = []
runtime_parent_pid = None
for line in ps.stdout.splitlines():
    parts = line.strip().split(None, 2)
    if len(parts) != 3:
        continue
    row_pid, ppid, command = parts
    try:
        row_pid_int = int(row_pid)
        ppid_int = int(ppid)
    except ValueError:
        continue
    if str(wrapper_path) in command and "track_b_paper_stack_runtime_wrapper.sh" in command:
        wrapper_pids.append(row_pid_int)
    if (
        str(repo_root) in command
        and "mgc_v05l.app.main" in command
        and "probationary-paper-soak" in command
    ):
        runtime_pids.append(row_pid_int)
        runtime_parent_pid = ppid_int
if runtime_pids != [pid]:
    raise SystemExit(1)
if len(wrapper_pids) != 1:
    raise SystemExit(1)
if runtime_parent_pid not in wrapper_pids:
    raise SystemExit(1)
print(generated_at)
PY
}

post_truth_startup_progress_heartbeat() {
  local pid="$1"
  "${PYTHON_BIN}" - "${RUNTIME_DIR}/paper_runtime_truth.json" "${POST_TRUTH_PROGRESS_FILE}" "${pid}" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

truth_path = Path(sys.argv[1])
progress_path = Path(sys.argv[2])
pid = int(sys.argv[3])
try:
    truth = json.loads(truth_path.read_text(encoding="utf-8"))
    progress = json.loads(progress_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    raise SystemExit(1)
try:
    progress_pid = int(progress.get("producer_pid") or progress.get("pid"))
except (TypeError, ValueError):
    raise SystemExit(1)
if progress_pid != pid:
    raise SystemExit(1)
progress_runtime = str(progress.get("runtime_instance_id") or "").strip()
truth_runtime = str(truth.get("runtime_instance_id") or "").strip()
if progress_runtime and truth_runtime and progress_runtime != truth_runtime:
    raise SystemExit(1)
progress_state = str(progress.get("state") or "").strip().upper()
progress_stage = str(progress.get("stage") or "").strip()
if progress_state not in {
    "STARTED",
    "IN_PROGRESS",
    "COMPLETED",
    "AWAITING_SUBMIT_AUTHORITY",
    "TRADING_LOOP_ENTERED",
    "BLOCKED_SUBMIT_AUTHORITY_GRANT_MISSING",
    "BLOCKED_SUBMIT_AUTHORITY_GRANT_TIMEOUT",
}:
    raise SystemExit(1)
if progress_stage not in {"authority_refresh", "watchdog_liveness_refresh", "lane_restore", "runtime_cycle"}:
    raise SystemExit(1)
generated_at = str(progress.get("heartbeat_at") or progress.get("generated_at") or "").strip()
try:
    generated = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
except ValueError:
    raise SystemExit(1)
if generated.tzinfo is None:
    generated = generated.replace(tzinfo=timezone.utc)
if (datetime.now(timezone.utc) - generated).total_seconds() > 15:
    raise SystemExit(1)
print(generated_at)
PY
}

update_detached_child_monitor() {
  local pid="$1"
  "${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_detached_runtime_monitor \
    --event heartbeat \
    --status-path "${DETACHED_CHILD_STATUS_FILE}" \
    --pid "${pid}" \
    --repo-root "${REPO_ROOT}" \
    --log-file "${RUNTIME_LOG}" \
    --pid-file "${PID_FILE}" \
    --config-paths-file "${CONFIG_PATHS_FILE}" \
    --runtime-truth-file "${RUNTIME_DIR}/paper_runtime_truth.json" \
    --post-truth-progress-file "${POST_TRUTH_PROGRESS_FILE}" \
    --runtime-instance-id "${runtime_instance_id}" \
    --source-commit "${source_commit}" \
    --python-bin "${PYTHON_BIN}"
}

detached_child_ready_authority() {
  local pid="$1"
  "${PYTHON_BIN}" - "${DETACHED_CHILD_STATUS_FILE}" "${pid}" <<'PY'
from datetime import datetime
import json
import sys
from pathlib import Path

def parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None

path = Path(sys.argv[1])
expected_pid = int(sys.argv[2])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    print("false")
    raise SystemExit(0)
if int(payload.get("child_pid") or payload.get("pid") or 0) != expected_pid:
    print("false")
    raise SystemExit(0)
if payload.get("child_final_status") != "RUNNING" or payload.get("process_alive") is not True:
    print("false")
    raise SystemExit(0)
classification = payload.get("classification")
if classification not in {"RUNTIME_CHILD_RUNNING_CYCLE_OBSERVED", "RUNTIME_CHILD_RUNNING_INITIAL_TRUTH"}:
    print("false")
    raise SystemExit(0)
marker = payload.get("last_runtime_cycle_marker")
progress = payload.get("last_post_truth_marker")
if not isinstance(marker, dict) and not isinstance(progress, dict):
    print("false")
    raise SystemExit(0)
truth = payload.get("runtime_truth_marker")
if not isinstance(truth, dict):
    print("false")
    raise SystemExit(0)
if isinstance(marker, dict):
    if marker.get("stage") != "runtime_cycle" or marker.get("state") not in {
        "STARTED",
        "IN_PROGRESS",
        "COMPLETED",
        "AWAITING_SUBMIT_AUTHORITY",
        "TRADING_LOOP_ENTERED",
    }:
        print("false")
        raise SystemExit(0)
elif progress.get("state") != "AWAITING_SUBMIT_AUTHORITY":
    print("false")
    raise SystemExit(0)
if isinstance(marker, dict) and marker.get("state") == "COMPLETED":
    marker_at = parse_ts(marker.get("generated_at") or marker.get("heartbeat_at"))
    truth_at = parse_ts(truth.get("generated_at"))
    if marker_at is not None and truth_at is not None and truth_at <= marker_at:
        print("false")
        raise SystemExit(0)
print("true")
PY
}

detached_child_exit_classification() {
  local pid="$1"
  "${PYTHON_BIN}" - "${DETACHED_CHILD_STATUS_FILE}" "${pid}" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
expected_pid = int(sys.argv[2])
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    raise SystemExit(1)
if int(payload.get("child_pid") or payload.get("pid") or 0) != expected_pid:
    raise SystemExit(1)
if payload.get("child_final_status") == "RUNNING" and payload.get("process_alive") is True:
    raise SystemExit(1)
print(str(payload.get("classification") or "RUNTIME_EXITED_BEFORE_DURABLE_READY"))
PY
}

write_runtime_submit_authority_grant() {
  local pid="$1"
  "${PYTHON_BIN}" - "${RUNTIME_SUBMIT_GRANT_FILE}" "${pid}" "${runtime_instance_id}" "${source_commit}" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

path = Path(sys.argv[1])
payload = {
    "schema_version": "track_b_paper_runtime_submit_authority_grant_v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "runtime_pid": int(sys.argv[2]),
    "producer_pid": int(sys.argv[2]),
    "runtime_instance_id": sys.argv[3],
    "source_commit": sys.argv[4],
    "submit_authority": True,
    "paper_only": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "authority_source": "track_b_start_paper_stack_durable_liveness_window",
}
path.parent.mkdir(parents=True, exist_ok=True)
tmp = path.with_name(f".{path.name}.{sys.argv[2]}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(path)
PY
}

write_runtime_config_paths_file() {
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
}

write_runtime_config_paths_file

if paper_minimal_startup_enabled; then
  if ! run_paper_minimal_startup_preflight; then
    exit 2
  fi
  pid=""
  if [[ -s "${PID_FILE}" ]]; then
    pid="$(tr -dc '0-9' < "${PID_FILE}" || true)"
  fi
  if [[ -n "${pid}" ]] && ps -p "${pid}" >/dev/null 2>&1; then
    already_running="true"
  else
    already_running="false"
  fi
  status_json="{}"
else
  status_json="$("${STATUS_SCRIPT}" --json)"
  already_running="$("${PYTHON_BIN}" -c 'import json,sys; print(str(json.loads(sys.stdin.read())["runtime"]["running"]).lower())' <<<"${status_json}")"
  pid="$("${PYTHON_BIN}" -c 'import json,sys; print(json.loads(sys.stdin.read())["runtime"]["pid"] or "")' <<<"${status_json}")"
fi
if [[ "${already_running}" == "true" ]]; then
  if [[ "${RESTART_REQUESTED}" != "1" ]]; then
    write_startup_artifact "ALREADY_RUNNING" "Track B PAPER runtime is already running; no start action taken." "${pid}"
    exit 0
  fi
  if [[ "${STACK_PROFILE}" == "canonical" ]]; then
    write_startup_artifact "BLOCKED_RESTART_REQUIRES_PROFILE" "Set TRACK_B_PAPER_STACK_PROFILE for an explicit restart generation." "${pid}"
    exit 2
  fi
  if paper_minimal_startup_enabled; then
    restart_precheck_classification="RESTART_ALLOWED_PAPER_MINIMAL_STARTUP_V1"
    restart_precheck_detail="PAPER_MINIMAL_STARTUP_V1 allowed controlled PAPER restart; legacy restart precheck is diagnostic only."
  else
    restart_precheck="$("${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_paper_stack_restart_precheck <<<"${status_json}")"
    restart_precheck_allowed="$("${PYTHON_BIN}" -c 'import json,sys; print(str(json.loads(sys.stdin.read()).get("restart_allowed") is True).lower())' <<<"${restart_precheck}")"
    restart_precheck_classification="$("${PYTHON_BIN}" -c 'import json,sys; print(json.loads(sys.stdin.read()).get("classification") or "")' <<<"${restart_precheck}")"
    restart_precheck_detail="$("${PYTHON_BIN}" -c 'import json,sys; print(json.loads(sys.stdin.read()).get("detail") or "")' <<<"${restart_precheck}")"
    if [[ "${restart_precheck_allowed}" != "true" ]]; then
      write_startup_artifact "${restart_precheck_classification:-BLOCKED_RESTART_PRECHECK}" "${restart_precheck_detail:-Broker/lifecycle/safety state is not clean enough for a controlled restart.}" "${pid}"
      exit 2
    fi
  fi
  write_startup_artifact "${restart_precheck_classification}" "${restart_precheck_detail}" "${pid}" >/dev/null
  if paper_minimal_startup_enabled; then
    stop_exact_runtime_pid_for_minimal_restart "${pid}"
    status_json="{}"
  else
    PROBATIONARY_PAPER_PID_FILE="${PID_FILE}" bash "${SCRIPT_DIR}/stop_probationary_paper_soak.sh"
    status_json="$("${STATUS_SCRIPT}" --json)"
  fi
fi

if paper_minimal_startup_enabled; then
  restart_allowed="true"
  runtime_start_allowed="true"
  blocker_count="0"
  launch_guard_restart_allowed="false"
  restart_authority_allowed="true"
  scoped_profile_allowed="true"
else
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
fi
if [[ ( "${runtime_start_allowed}" != "true" || "${blocker_count}" != "0" ) && ( "${restart_allowed}" != "true" || "${blocker_count}" != "0" ) && "${launch_guard_restart_allowed}" != "true" && "${restart_authority_allowed}" != "true" && "${scoped_profile_allowed}" != "true" ]]; then
  write_startup_artifact "BLOCKED_PRECHECK" "Canonical readiness does not allow a clean PAPER runtime start." ""
  exit 2
fi
if ! paper_minimal_startup_enabled; then
  if ! run_startup_preflight_evidence_refresh; then
    exit 2
  fi
fi
write_approved_profile_artifact

write_runtime_config_paths_file

session_name="track_b_paper_stack_$(date -u +%Y%m%dT%H%M%SZ)_$$"
config_stack="$(IFS=":"; printf "%s" "${CANONICAL_CONFIGS[*]}")"
source_commit="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
runtime_instance_id="track-b-paper-stack-$(date -u +%Y%m%dT%H%M%SZ)-$$"
wrapper_tmp="${WRAPPER_PATH}.$$.$RANDOM.tmp"

cat > "${wrapper_tmp}" <<WRAPPER
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
export MGC_TRACK_B_PAPER_STACK_PROFILE="${STACK_PROFILE}"
export MGC_TRACK_B_EXPECTED_PROJECT_ROOT="${REPO_ROOT}"
export MGC_TRACK_B_EXPECTED_SOURCE_COMMIT="${source_commit}"
export MGC_TRACK_B_PAPER_STACK_STARTUP_MODE="${STARTUP_MODE}"
export MGC_TRACK_B_PAPER_STACK_OWNED_MANAGED_EXPOSURE_RESTORE_JSON='${STARTUP_OWNED_MANAGED_EXPOSURE_RESTORE_JSON}'
export MGC_TRACK_B_PAPER_MINIMAL_STARTUP_V1="${PAPER_MINIMAL_STARTUP_V1}"
export MGC_TRACK_B_PAPER_MINIMAL_STARTUP_CLASSIFICATION="${STARTUP_PREFLIGHT_REFRESH_CLASSIFICATION}"
export MGC_TRACK_B_PAPER_STACK_RUNTIME_SCOPE_LOCK_DIR="${START_LOCK_DIR}"
export MGC_TRACK_B_PAPER_RUNTIME_SUBMIT_GRANT_FILE="${RUNTIME_SUBMIT_GRANT_FILE}"
mkdir -p "${RUNTIME_DIR}"
runtime_pid=""
runtime_child_started_at=""
runtime_child_command=""
detached_child_final_status_written=0
write_detached_child_status() {
  local event="\$1"
  local exit_code="\${2:-}"
  if [[ -z "\${runtime_pid:-}" ]]; then
    return 0
  fi
  local args=(
    -m mgc_v05l.execution_core.track_b_detached_runtime_monitor
    --event "\${event}"
    --status-path "${DETACHED_CHILD_STATUS_FILE}"
    --pid "\${runtime_pid}"
    --started-at "\${runtime_child_started_at}"
    --repo-root "${REPO_ROOT}"
    --log-file "${RUNTIME_LOG}"
    --pid-file "${PID_FILE}"
    --config-paths-file "${CONFIG_PATHS_FILE}"
    --runtime-truth-file "${RUNTIME_DIR}/paper_runtime_truth.json"
    --post-truth-progress-file "${POST_TRUTH_PROGRESS_FILE}"
    --runtime-instance-id "${runtime_instance_id}"
    --source-commit "${source_commit}"
    --python-bin "${PYTHON_BIN}"
    --parent-pid "\$\$"
  )
  if [[ -n "\${runtime_child_command:-}" ]]; then
    args+=(--child-command "\${runtime_child_command}")
  fi
  if [[ -n "\${exit_code}" ]]; then
    args+=(--exit-code "\${exit_code}")
  fi
  "${PYTHON_BIN}" "\${args[@]}" >/dev/null 2>>"${RUNTIME_LOG}" || true
}
write_detached_child_final_status_on_wrapper_exit() {
  local wrapper_exit_code="\$1"
  if [[ "\${detached_child_final_status_written:-0}" == "1" || -z "\${runtime_pid:-}" ]]; then
    return 0
  fi
  detached_child_final_status_written=1
  local final_event="exited"
  if ps -p "\${runtime_pid}" >/dev/null 2>&1; then
    final_event="heartbeat"
  fi
  printf '%s\n' "track_b_paper_stack_wrapper_final_status generated_at=\$(date -u +%Y-%m-%dT%H:%M:%SZ) runtime_pid=\${runtime_pid} wrapper_exit_code=\${wrapper_exit_code} final_event=\${final_event}" >> "${RUNTIME_LOG}"
  if [[ "\${final_event}" == "exited" ]]; then
    write_detached_child_status "\${final_event}" "\${wrapper_exit_code}"
  else
    write_detached_child_status "\${final_event}"
  fi
}
release_runtime_scope_lock_on_wrapper_exit() {
  local lock_dir="\${MGC_TRACK_B_PAPER_STACK_RUNTIME_SCOPE_LOCK_DIR:-}"
  if [[ -n "\${lock_dir}" && -d "\${lock_dir}" ]]; then
    rm -rf "\${lock_dir}" >/dev/null 2>&1 || true
  fi
}
trap 'wrapper_exit_code=\$?; write_detached_child_final_status_on_wrapper_exit "\${wrapper_exit_code}"; release_runtime_scope_lock_on_wrapper_exit' EXIT
verify_single_runtime_carrier_on_wrapper_start() {
  "${PYTHON_BIN}" - <<'PY' "${REPO_ROOT}" "${WRAPPER_PATH}" "${SCOPED_CONFIG_PATH}" "\$\$" "${RUNTIME_LOG}" "${STACK_PROFILE}" "${DETACHED_CHILD_STATUS_FILE}" "${RUNTIME_DIR}/paper_runtime_truth.json" "${POST_TRUTH_PROGRESS_FILE}" "${runtime_instance_id}" "${source_commit}" "${PYTHON_BIN}" "${PID_FILE}" "${CONFIG_PATHS_FILE}"
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

repo_root = str(Path(sys.argv[1]))
wrapper_path = str(Path(sys.argv[2]))
scoped_config_path = str(Path(sys.argv[3]))
current_pid = int(sys.argv[4])
runtime_log = Path(sys.argv[5])
stack_profile = sys.argv[6]
detached_child_status_file = Path(sys.argv[7])
runtime_truth_file = Path(sys.argv[8])
post_truth_progress_file = Path(sys.argv[9])
runtime_instance_id = sys.argv[10]
source_commit = sys.argv[11]
python_bin = sys.argv[12]
pid_file = Path(sys.argv[13])
config_paths_file = Path(sys.argv[14])

ps = subprocess.run(
    ["ps", "-axo", "pid=,ppid=,command="],
    text=True,
    check=False,
    capture_output=True,
)
active = []
for line in ps.stdout.splitlines():
    parts = line.strip().split(None, 2)
    if len(parts) != 3:
        continue
    try:
        pid = int(parts[0])
        ppid = int(parts[1])
    except ValueError:
        continue
    if pid == current_pid:
        continue
    command = parts[2]
    if ppid == current_pid and "track_b_paper_stack_runtime_wrapper.sh" in command:
        continue
    if repo_root not in command:
        continue
    is_wrapper = wrapper_path in command and "track_b_paper_stack_runtime_wrapper.sh" in command
    is_runtime_child = (
        ("mgc_v05l.app.main" in command and "probationary-paper-soak" in command)
        or "run_probationary_paper_soak.sh" in command
    )
    same_scope_child = is_runtime_child and (
        scoped_config_path in command or "paper_runtime_config_paths.txt" in command
    )
    if is_wrapper or same_scope_child:
        active.append(
            {
                "pid": pid,
                "ppid": ppid,
                "role": "wrapper" if is_wrapper else "runtime_child",
                "command": command,
            }
        )

if active:
    active_child = next((row for row in active if row.get("role") == "runtime_child"), None)
    if active_child is not None:
        try:
            from mgc_v05l.execution_core.track_b_detached_runtime_monitor import (
                build_detached_runtime_child_status,
            )

            build_detached_runtime_child_status(
                event="heartbeat",
                status_path=detached_child_status_file,
                pid=int(active_child["pid"]),
                observed_at=datetime.now(timezone.utc),
                repo_root=Path(repo_root),
                log_file=runtime_log,
                pid_file=pid_file,
                config_paths_file=config_paths_file,
                runtime_truth_file=runtime_truth_file,
                post_truth_progress_file=post_truth_progress_file,
                runtime_instance_id=runtime_instance_id,
                source_commit=source_commit,
                python_bin=python_bin,
                child_command=active_child.get("command"),
                parent_pid=int(active_child.get("ppid") or 0),
            )
        except Exception as exc:
            runtime_log.parent.mkdir(parents=True, exist_ok=True)
            with runtime_log.open("a", encoding="utf-8") as handle:
                handle.write("track_b_paper_stack_wrapper_duplicate_carrier_monitor_refresh_failed ")
                handle.write(json.dumps({"error": str(exc)}, sort_keys=True))
                handle.write("\n")
    payload = {
        "classification": "BLOCKED_DUPLICATE_RUNTIME_CARRIER",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stack_profile": stack_profile,
        "current_wrapper_pid": current_pid,
        "active_processes": active,
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    runtime_log.parent.mkdir(parents=True, exist_ok=True)
    with runtime_log.open("a", encoding="utf-8") as handle:
        handle.write("track_b_paper_stack_wrapper_duplicate_carrier_blocked ")
        handle.write(json.dumps(payload, sort_keys=True))
        handle.write("\n")
    raise SystemExit(2)
PY
}
verify_single_runtime_carrier_on_wrapper_start
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
printf '%s\n' "track_b_paper_stack_wrapper_start generated_at=\$(date -u +%Y-%m-%dT%H:%M:%SZ) repo_root=${REPO_ROOT} config_stack=${config_stack}" >> "${RUNTIME_LOG}"
rm -f "${RUNTIME_SUBMIT_GRANT_FILE}" >/dev/null 2>&1 || true
runtime_child_started_at="\$(date -u +%Y-%m-%dT%H:%M:%SZ)"
runtime_child_command="bash ${SCRIPT_DIR}/run_probationary_paper_soak.sh --pid-file ${PID_FILE} --log-file ${RUNTIME_LOG} --config-paths-file ${CONFIG_PATHS_FILE} --launch-status-file ${LAUNCH_STATUS_FILE} --schwab-config ${REPO_ROOT}/config/schwab.local.json"
bash "${SCRIPT_DIR}/run_probationary_paper_soak.sh" \
  --pid-file "${PID_FILE}" \
  --log-file "${RUNTIME_LOG}" \
  --config-paths-file "${CONFIG_PATHS_FILE}" \
  --launch-status-file "${LAUNCH_STATUS_FILE}" \
  --schwab-config "${REPO_ROOT}/config/schwab.local.json" >> "${RUNTIME_LOG}" 2>&1 &
runtime_pid="\$!"
echo "\${runtime_pid}" > "${PID_FILE}"
write_detached_child_status "started"
"${PYTHON_BIN}" - <<'PY' "${PID_METADATA_FILE}" "\${runtime_pid}" "${runtime_instance_id}" "${REPO_ROOT}" "${source_commit}"
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
set +e
wait "\${runtime_pid}"
runtime_exit_code="\$?"
set -e
runtime_exit_observed_at="\$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf '%s\n' "track_b_paper_stack_wrapper_child_exit generated_at=\${runtime_exit_observed_at} runtime_pid=\${runtime_pid} exit_code=\${runtime_exit_code}" >> "${RUNTIME_LOG}"
write_detached_child_status "exited" "\${runtime_exit_code}"
detached_child_final_status_written=1
if ! "${PYTHON_BIN}" - <<'PY' "${LAUNCH_STATUS_FILE}" "${PID_FILE}" "${RUNTIME_LOG}" "${CONFIG_PATHS_FILE}" "${RUNTIME_DIR}/paper_runtime_truth.json" "\${runtime_pid}" "\${runtime_exit_code}" "${REPO_ROOT}" "${PYTHON_BIN}" "\${runtime_exit_observed_at}"
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

status_path = Path(sys.argv[1])
pid_file = sys.argv[2]
log_file = sys.argv[3]
config_paths_file = sys.argv[4]
truth_path = Path(sys.argv[5])
runtime_pid = int(sys.argv[6])
exit_code = int(sys.argv[7])
repo_root = sys.argv[8]
python_bin = sys.argv[9]
observed_at = sys.argv[10]
truth = {}
try:
    truth = json.loads(truth_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    truth = {}
truth_pid = truth.get("producer_pid") or truth.get("pid")
try:
    truth_pid = int(truth_pid)
except (TypeError, ValueError):
    truth_pid = None
first_truth = truth.get("generated_at") if truth_pid == runtime_pid else None
classification = "RUNTIME_EXITED_AFTER_INITIAL_TRUTH" if first_truth else "RUNTIME_EXITED_BEFORE_RUNTIME_TRUTH"
payload = {
    "generated_at": observed_at or datetime.now(timezone.utc).isoformat(),
    "classification": classification,
    "pid": runtime_pid,
    "pid_file": pid_file,
    "log_file": log_file,
    "config_paths_file": config_paths_file,
    "runtime_truth_file": str(truth_path),
    "repo_root": repo_root,
    "cwd": os.getcwd(),
    "python_bin": python_bin,
    "detail": "foreground child exited; wrapper captured exit status",
    "child_exit_code": exit_code,
    "first_truth_generated_at": first_truth,
    "second_truth_generated_at": None,
    "sustained_convergence_confirmed": False,
    "final_pid_alive": False,
    "terminated_by_launch_verifier": False,
    "termination_signal": None,
    "termination_reason": "runtime_exited_after_initial_truth" if first_truth else "runtime_exited_before_runtime_truth",
    "stop_source": "runtime_internal",
    "stop_observed_at": observed_at or datetime.now(timezone.utc).isoformat(),
    "paper_only": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "submit_authority": False,
}
status_path.parent.mkdir(parents=True, exist_ok=True)
tmp = status_path.with_name(f".{status_path.name}.{runtime_pid}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(status_path)
PY
then
  status_tmp="${LAUNCH_STATUS_FILE}.\$\$.tmp"
  printf '{\n' > "\${status_tmp}"
  printf '  "classification": "RUNTIME_EXIT_STATUS_CAPTURE_FAILED",\n' >> "\${status_tmp}"
  printf '  "child_exit_code": %s,\n' "\${runtime_exit_code}" >> "\${status_tmp}"
  printf '  "detail": "foreground child exited, but Python launch-status writer failed; shell fallback captured exit status",\n' >> "\${status_tmp}"
  printf '  "final_pid_alive": false,\n' >> "\${status_tmp}"
  printf '  "generated_at": "%s",\n' "\${runtime_exit_observed_at}" >> "\${status_tmp}"
  printf '  "live_money_eligible": false,\n' >> "\${status_tmp}"
  printf '  "paper_only": true,\n' >> "\${status_tmp}"
  printf '  "paper_proof_invoked": false,\n' >> "\${status_tmp}"
  printf '  "pid": %s,\n' "\${runtime_pid}" >> "\${status_tmp}"
  printf '  "stop_observed_at": "%s",\n' "\${runtime_exit_observed_at}" >> "\${status_tmp}"
  printf '  "stop_source": "runtime_internal",\n' >> "\${status_tmp}"
  printf '  "submit_authority": false,\n' >> "\${status_tmp}"
  printf '  "termination_reason": "runtime_exit_status_capture_failed"\n' >> "\${status_tmp}"
  printf '}\n' >> "\${status_tmp}"
  mv "\${status_tmp}" "${LAUNCH_STATUS_FILE}"
fi
exit "\${runtime_exit_code}"
WRAPPER
chmod +x "${wrapper_tmp}"
mv "${wrapper_tmp}" "${WRAPPER_PATH}"

carrier="screen"
if paper_minimal_startup_enabled; then
  if [[ "${PREFERRED_CARRIER}" == "launchctl" ]]; then
    write_startup_artifact "BLOCKED_LAUNCHCTL_DISABLED_FOR_PAPER_MINIMAL_STARTUP" "Controlled PAPER_MINIMAL_STARTUP_V1 restarts use the direct wrapper path; launchctl is not allowed for thin recovery." ""
    exit 1
  elif [[ "${PREFERRED_CARRIER}" == "nohup" ]]; then
    if ! nohup_available; then
      write_startup_artifact "BLOCKED_NOHUP_UNAVAILABLE" "nohup carrier was explicitly requested but nohup is unavailable." ""
      exit 1
    fi
    carrier="nohup"
  elif [[ "${PREFERRED_CARRIER}" == "screen" ]]; then
    write_startup_artifact "BLOCKED_SCREEN_DISABLED_FOR_PAPER_MINIMAL_STARTUP" "Controlled PAPER_MINIMAL_STARTUP_V1 restarts require the detached wrapper to be the sole runtime supervisor; screen is not authoritative enough for child-exit capture." ""
    exit 1
  elif nohup_available; then
    carrier="nohup"
  elif launchctl_available; then
    write_startup_artifact "BLOCKED_NO_DIRECT_CARRIER" "nohup is unavailable and launchctl is disabled for controlled PAPER_MINIMAL_STARTUP_V1 thin recovery." ""
    exit 1
  else
    write_startup_artifact "BLOCKED_NO_DIRECT_CARRIER" "Neither launchctl nor nohup is available for controlled PAPER runtime ownership by the detached wrapper supervisor." ""
    exit 1
  fi
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

lock_result="${STACK_DIR}/.runtime_scope_lock.$$.json"
cleanup_stale_runtime_carrier_artifacts >/dev/null || true
set +e
acquire_authoritative_runtime_scope_lock > "${lock_result}"
lock_rc=$?
set -e
if [[ "${lock_rc}" != "0" ]]; then
  lock_classification="$("${PYTHON_BIN}" -c 'import json,sys; print((json.load(open(sys.argv[1])).get("classification") or "BLOCKED_RUNTIME_SCOPE_LOCK"))' "${lock_result}" 2>/dev/null || echo "BLOCKED_RUNTIME_SCOPE_LOCK")"
  lock_detail="$("${PYTHON_BIN}" -c 'import json,sys; p=json.load(open(sys.argv[1])); print(p.get("detail") or p.get("error") or "Existing same-scope runtime carrier/child prevents a safe PAPER start.")' "${lock_result}" 2>/dev/null || echo "Existing same-scope runtime carrier/child prevents a safe PAPER start.")"
  write_startup_artifact "${lock_classification}" "${lock_detail}" ""
  exit 1
fi

rm -f "${LAUNCH_STATUS_FILE}"
rm -f "${DETACHED_CHILD_STATUS_FILE}"
if paper_minimal_startup_enabled; then
  rm -f "${PID_FILE}" "${PID_METADATA_FILE}"
fi
if [[ "${carrier}" == "screen" ]]; then
  screen -wipe >/dev/null 2>&1 || true
  printf '%s\n' "${session_name}" > "${PID_FILE}.screen_session"
  rm -f "${LAUNCHCTL_LABEL_FILE}"
  screen -dmS "${session_name}" /bin/bash "${WRAPPER_PATH}"
elif [[ "${carrier}" == "nohup" ]]; then
  rm -f "${PID_FILE}.screen_session" "${LAUNCHCTL_LABEL_FILE}"
  nohup /bin/bash "${WRAPPER_PATH}" >/dev/null 2>&1 &
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
    rm -rf "${START_LOCK_DIR}" >/dev/null 2>&1 || true
    rm -f "${LAUNCHCTL_LABEL_FILE}"
    exit 1
  fi
fi

if paper_minimal_startup_enabled; then
  deadline=$((SECONDS + WAIT_SECONDS))
  candidate_pid=""
  candidate_seen_since=0
  stable_since=0
  first_truth_generated_at=""
  last_truth_generated_at=""
  truth_advanced="false"
  observed_runtime_pid=""
  while [[ "${SECONDS}" -lt "${deadline}" ]]; do
    sleep 2
    pid=""
    if [[ -s "${PID_FILE}" ]]; then
      pid="$(tr -dc '0-9' < "${PID_FILE}" || true)"
    fi
    if [[ -n "${pid}" ]] && ps -p "${pid}" >/dev/null 2>&1; then
      observed_runtime_pid="${pid}"
      update_detached_child_monitor "${pid}" >/dev/null || true
      if [[ "${candidate_pid}" != "${pid}" ]]; then
        candidate_pid="${pid}"
        candidate_seen_since="${SECONDS}"
        stable_since=0
        first_truth_generated_at=""
        last_truth_generated_at=""
        truth_advanced="false"
      fi
      if run_paper_minimal_startup_preflight >/dev/null; then
        truth_generated_at="$(verify_direct_paper_runtime_shape "${pid}" "${source_commit}" 2>/dev/null || true)"
        if [[ -n "${truth_generated_at}" ]]; then
          if [[ "${stable_since}" -eq 0 ]]; then
            stable_since="${candidate_seen_since}"
            if [[ "${stable_since}" -le 0 ]]; then
              stable_since="${SECONDS}"
            fi
            first_truth_generated_at="${truth_generated_at}"
            last_truth_generated_at="${truth_generated_at}"
            write_startup_artifact "RUNTIME_RUNNING_WAITING_FOR_MINIMAL_STARTUP_STABILITY" "Runtime matched PAPER_MINIMAL_STARTUP_V1 shape; waiting for ${STABLE_SECONDS}s same-PID liveness and advancing runtime truth or post-truth startup progress." "${pid}" >/dev/null
          fi
          if [[ "${truth_generated_at}" != "${last_truth_generated_at}" ]]; then
            last_truth_generated_at="${truth_generated_at}"
            if [[ "${truth_generated_at}" != "${first_truth_generated_at}" ]]; then
              truth_advanced="true"
            fi
          fi
          progress_generated_at="$(post_truth_startup_progress_heartbeat "${pid}" 2>/dev/null || true)"
          if [[ -n "${progress_generated_at}" && "${truth_advanced}" != "true" ]]; then
            update_detached_child_monitor "${pid}" >/dev/null || true
            write_startup_artifact "RUNTIME_RUNNING_POST_TRUTH_AUTHORITY_REFRESH" "Runtime PID ${pid} is alive and publishing post-truth startup progress at ${progress_generated_at}; treating progress as diagnostic while same-PID runtime shape and detached-child authority are verified." "${pid}" >/dev/null
          fi
          detached_child_ready="$(detached_child_ready_authority "${pid}" 2>/dev/null || true)"
          if [[ "${detached_child_ready}" != "true" ]]; then
            update_detached_child_monitor "${pid}" >/dev/null || true
            write_startup_artifact "RUNTIME_RUNNING_WAITING_FOR_DETACHED_CHILD_AUTHORITY" "Runtime matched PAPER_MINIMAL_STARTUP_V1 shape; waiting for fresh detached-child monitor authority before durable readiness." "${pid}" >/dev/null
            continue
          fi
          if (( SECONDS - stable_since >= STABLE_SECONDS )); then
            update_detached_child_monitor "${pid}" >/dev/null || true
            detached_child_ready="$(detached_child_ready_authority "${pid}" 2>/dev/null || true)"
            if [[ "${detached_child_ready}" != "true" ]]; then
              write_startup_artifact "RUNTIME_EXITED_BEFORE_DURABLE_READY" "Runtime passed the stability window, but refreshed detached-child monitor authority did not prove a live durable runtime." "${pid}"
              exit 1
            fi
            write_runtime_submit_authority_grant "${pid}"
            write_startup_artifact "READY_SUBMIT_CAPABLE" "Track B PAPER runtime started through direct PAPER_MINIMAL_STARTUP_V1 process path, matched required runtime shape, survived ${STABLE_SECONDS}s, received explicit wrapper-owned submit authority, and retained authoritative wrapper/child ownership; runtime truth advancement is diagnostic after shape verification." "${pid}"
          exit 0
          fi
          write_startup_artifact "RUNTIME_RUNNING_WAITING_FOR_MINIMAL_STARTUP_STABILITY" "Runtime matched PAPER_MINIMAL_STARTUP_V1 shape; waiting for ${STABLE_SECONDS}s same-PID liveness and advancing runtime truth or post-truth startup progress." "${pid}" >/dev/null
        else
          stable_since=0
          first_truth_generated_at=""
          last_truth_generated_at=""
          truth_advanced="false"
          write_startup_artifact "RUNTIME_RUNNING_WAITING_FOR_MINIMAL_RUNTIME_SHAPE" "Runtime process is alive; waiting for commit/profile/lane-count/runtime-truth shape to match PAPER_MINIMAL_STARTUP_V1." "${pid}" >/dev/null
        fi
      fi
    elif [[ -n "${pid}" ]]; then
      child_exit_classification="$(detached_child_exit_classification "${pid}" 2>/dev/null || true)"
      if [[ -z "${child_exit_classification}" ]]; then
        update_detached_child_monitor "${pid}" >/dev/null || true
        child_exit_classification="$(detached_child_exit_classification "${pid}" 2>/dev/null || true)"
      fi
      if [[ "${child_exit_classification}" == "RUNTIME_CLEAN_EXIT_AFTER_CYCLE" || "${child_exit_classification}" == "RUNTIME_CLEAN_EXIT_AFTER_MAX_CYCLES" ]]; then
        write_startup_artifact "${child_exit_classification}" "Runtime child exited cleanly after a cycle; this is not a durable service-ready state." "${pid}"
        exit 1
      fi
      if [[ -n "${child_exit_classification}" ]]; then
        write_startup_artifact "RUNTIME_EXITED_BEFORE_DURABLE_READY" "Runtime child exited before durable service readiness; detached child status captured exit code and last marker." "${pid}"
        exit 1
      fi
      write_startup_artifact "RUNTIME_EXITED_BEFORE_DURABLE_READY" "Runtime wrote PID ${pid}, then exited before ${STABLE_SECONDS}s same-PID liveness and advancing runtime truth or post-truth startup progress were verified." "${pid}"
      exit 1
    elif [[ -n "${observed_runtime_pid}" ]]; then
      child_exit_classification="$(detached_child_exit_classification "${observed_runtime_pid}" 2>/dev/null || true)"
      if [[ -z "${child_exit_classification}" ]]; then
        update_detached_child_monitor "${observed_runtime_pid}" >/dev/null || true
        child_exit_classification="$(detached_child_exit_classification "${observed_runtime_pid}" 2>/dev/null || true)"
      fi
      if [[ "${child_exit_classification}" == "RUNTIME_CLEAN_EXIT_AFTER_CYCLE" || "${child_exit_classification}" == "RUNTIME_CLEAN_EXIT_AFTER_MAX_CYCLES" ]]; then
        write_startup_artifact "${child_exit_classification}" "Runtime child exited cleanly after a cycle; this is not a durable service-ready state." "${observed_runtime_pid}"
        exit 1
      fi
      if [[ -n "${child_exit_classification}" ]]; then
        write_startup_artifact "RUNTIME_EXITED_BEFORE_DURABLE_READY" "Runtime child exited before durable service readiness; detached child status captured exit code and last marker." "${observed_runtime_pid}"
        exit 1
      fi
      write_startup_artifact "RUNTIME_EXITED_BEFORE_DURABLE_READY" "Runtime reached initial minimal process ownership, then exited before ${STABLE_SECONDS}s same-PID liveness and advancing runtime truth or post-truth startup progress were verified." "${observed_runtime_pid}"
      exit 1
    fi
  done
  write_startup_artifact "BLOCKED_START_TIMEOUT" "Runtime did not reach the required PAPER_MINIMAL_STARTUP_V1 direct-process runtime shape, same-PID liveness, and advancing runtime truth or post-truth startup progress before timeout." ""
  exit 1
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
  maintenance_restore_ready="$("${PYTHON_BIN}" -c '
import json
import sys
payload = json.loads(sys.stdin.read())
startup = payload.get("startup_phase") if isinstance(payload.get("startup_phase"), dict) else {}
evidence = startup.get("evidence") if isinstance(startup.get("evidence"), dict) else {}
readiness = payload.get("readiness") if isinstance(payload.get("readiness"), dict) else {}
required = (
    "runtime_process_alive",
    "process_identified",
    "profile_loaded",
    "lanes_loaded",
    "market_data_feed_observed",
    "runtime_ingestion_advancing",
    "authority_refreshed",
    "readiness_evaluated",
)
ok = (
    startup.get("phase") == "READINESS_EVALUATED"
    and all(evidence.get(key) is True for key in required)
    and readiness.get("submit_allowed") is not True
    and readiness.get("ready_submit_capable") is not True
)
print(str(ok).lower())
' <<<"${last_status}" 2>/dev/null || echo false)"
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
  if [[ "${STARTUP_MODE}" == "OWNED_MANAGED_EXPOSURE_MAINTENANCE_RESTORE" && "${running}" == "true" && "${maintenance_restore_ready}" == "true" ]]; then
    if [[ "${ready_pid}" != "${pid}" ]]; then
      ready_pid="${pid}"
      ready_since="${SECONDS}"
    fi
    if (( SECONDS - ready_since >= STABLE_SECONDS )); then
      write_startup_artifact "OWNED_MANAGED_EXPOSURE_MAINTENANCE_RESTORE_ACTIVE" "Track B PAPER runtime restored managed-maintenance observability for owned managed exposure; new entries remain blocked by canonical readiness." "${pid}"
      exit 0
    fi
    write_startup_artifact "RUNTIME_RUNNING_WAITING_FOR_MAINTENANCE_RESTORE_STABILITY" "Runtime is alive in owned-managed-exposure maintenance-restore mode; waiting for ${STABLE_SECONDS}s sustained maintenance observability." "${pid}" >/dev/null
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
