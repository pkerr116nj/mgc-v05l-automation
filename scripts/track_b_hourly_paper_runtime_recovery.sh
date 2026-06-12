#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LABEL="com.mgc.trackb.paper-runtime-recovery"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"
TEMPLATE_PATH="${REPO_ROOT}/var/launchd/track_b/${LABEL}.plist"
STATUS_SCRIPT="${REPO_ROOT}/scripts/track_b_status_paper_stack.sh"
START_SCRIPT="${REPO_ROOT}/scripts/track_b_start_paper_stack.sh"
THIN_RECOVERY_SCRIPT="${REPO_ROOT}/scripts/track_b_thin_paper_runtime_recovery.sh"
AUDIT_MODULE="mgc_v05l.execution_core.track_b_hourly_runtime_recovery_audit"
PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
RUNTIME_DIR="${REPO_ROOT}/outputs/probationary_pattern_engine/paper_session/runtime"
STATE_DIR="${REPO_ROOT}/outputs/track_b_execution_core/runtime_recovery"
DISABLED_MARKER="${STATE_DIR}/recovery_disabled_by_operator.json"
STATUS_ARTIFACT="${STATE_DIR}/latest_launchd_recovery_status.json"
LAST_TICK_ARTIFACT="${STATE_DIR}/latest_launchd_recovery_tick.json"
APPROVED_PROFILE_ARTIFACT="${STATE_DIR}/approved_paper_stack_profile.json"
STARTUP_ARTIFACT="${REPO_ROOT}/outputs/track_b_execution_core/paper_stack/latest_paper_stack_startup.json"
RECOVERY_TICK_INTERVAL_SECONDS=120
PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}"
export PYTHONPATH

mode="${1:-status}"

json_value() {
  "${PYTHON_BIN}" - "$1" "$2" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
field = sys.argv[2].split(".")
try:
    payload = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    print("")
    raise SystemExit(0)
value = payload
for key in field:
    if not isinstance(value, dict):
        print("")
        raise SystemExit(0)
    value = value.get(key)
if isinstance(value, bool):
    print("true" if value else "false")
elif value is None:
    print("")
else:
    print(value)
PY
}

launchctl_loaded() {
  launchctl print "gui/$(id -u)/${LABEL}" >/dev/null 2>&1 || launchctl list "${LABEL}" >/dev/null 2>&1
}

write_disabled_marker() {
  mkdir -p "${STATE_DIR}"
  "${PYTHON_BIN}" - "$DISABLED_MARKER" "$REPO_ROOT" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

path = Path(sys.argv[1])
payload = {
    "schema_version": "track_b_recovery_operator_disabled_v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "classification": "RECOVERY_DISABLED_BY_OPERATOR",
    "repo_root": sys.argv[2],
}
tmp = path.with_name(f".{path.name}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(path)
PY
}

write_tick_artifact() {
  local action="$1"
  local blocker="${2:-}"
  local detail="${3:-}"
  local status_path="${4:-}"
  local profile_resolution_path="${5:-}"
  mkdir -p "${STATE_DIR}"
  "${PYTHON_BIN}" - "$LAST_TICK_ARTIFACT" "$action" "$blocker" "$detail" "$status_path" "$REPO_ROOT" "$profile_resolution_path" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

artifact, action, blocker, detail, status_path, repo_root, profile_resolution_path = sys.argv[1:]
status_payload = {}
if status_path:
    try:
        status_payload = json.loads(Path(status_path).read_text(encoding="utf-8"))
    except Exception:
        status_payload = {}
profile_resolution = {}
if profile_resolution_path:
    try:
        profile_resolution = json.loads(Path(profile_resolution_path).read_text(encoding="utf-8"))
    except Exception:
        profile_resolution = {}
payload = {
    "schema_version": "track_b_hourly_paper_runtime_recovery_tick_v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "last_action": action,
    "last_blocker": blocker or None,
    "detail": detail or None,
    "repo_root": repo_root,
    "paper_only": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "runtime_running": (((status_payload.get("runtime") or {}).get("running")) if isinstance(status_payload, dict) else None),
    "ready_submit_capable": (((status_payload.get("readiness") or {}).get("ready_submit_capable")) if isinstance(status_payload, dict) else None),
    "recovery_requested_profile": profile_resolution.get("recovery_requested_profile"),
    "recovery_profile_source": profile_resolution.get("recovery_profile_source"),
    "recovery_profile_approved": profile_resolution.get("recovery_profile_approved"),
    "recovery_profile_blocker": profile_resolution.get("recovery_profile_blocker"),
}
path = Path(artifact)
tmp = path.with_name(f".{path.name}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(path)
PY
}

run_managed_exit_actuator() {
  local output_path="$1"
  "${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_managed_exit_actuator \
    --repo-root "${REPO_ROOT}" \
    --apply \
    --operator-authorized-managed-exit \
    --json > "${output_path}"
}

emit_status() {
  local loaded="false"
  local state="SUPERVISOR_PAUSED"
  local disabled="false"
  if [[ -f "${DISABLED_MARKER}" ]]; then
    disabled="true"
    state="RECOVERY_DISABLED_BY_OPERATOR"
  fi
  if launchctl_loaded; then
    loaded="true"
    if [[ "${disabled}" != "true" ]]; then
      state="RECOVERY_ACTIVE"
    fi
  fi
  mkdir -p "${STATE_DIR}"
  "${PYTHON_BIN}" - "$state" "$loaded" "$disabled" "$PLIST_PATH" "$TEMPLATE_PATH" "$REPO_ROOT" "$STATUS_ARTIFACT" "$LAST_TICK_ARTIFACT" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

state, loaded, disabled, plist, template, repo, status_artifact, last_tick_artifact = sys.argv[1:]
last_tick = {}
try:
    last_tick = json.loads(Path(last_tick_artifact).read_text(encoding="utf-8"))
except Exception:
    last_tick = {}
payload = {
    "schema_version": "track_b_hourly_paper_runtime_recovery_launchd_status_v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "classification": state,
    "state": state,
    "launchd_loaded": loaded == "true",
    "launchd_enabled": loaded == "true" and disabled != "true",
    "operator_disabled": disabled == "true",
    "survives_codex_exit": loaded == "true" and disabled != "true",
    "launchd_owned": loaded == "true",
    "can_restart_runtime_without_codex": loaded == "true" and disabled != "true",
    "label": "com.mgc.trackb.paper-runtime-recovery",
    "run_interval_seconds": 120,
    "cadence_classification": "ACTIVE_SESSION_WATCHDOG_120S",
    "plist_path": plist,
    "repo_template_path": template,
    "repo_root": repo,
    "last_tick": last_tick.get("generated_at"),
    "last_action": last_tick.get("last_action"),
    "last_blocker": last_tick.get("last_blocker"),
    "recovery_requested_profile": last_tick.get("recovery_requested_profile"),
    "recovery_profile_source": last_tick.get("recovery_profile_source"),
    "recovery_profile_approved": last_tick.get("recovery_profile_approved"),
    "recovery_profile_blocker": last_tick.get("recovery_profile_blocker"),
    "enable_command": "bash scripts/track_b_hourly_paper_runtime_recovery.sh enable",
    "disable_command": "bash scripts/track_b_hourly_paper_runtime_recovery.sh disable",
    "status_command": "bash scripts/track_b_hourly_paper_runtime_recovery.sh status",
    "tick_command": "bash scripts/track_b_hourly_paper_runtime_recovery.sh tick",
    "runtime_start_path": "scripts/track_b_thin_paper_runtime_recovery.sh",
    "runtime_status_path": "runtime_pid_artifact_only",
    "restart_authority_source": "thin_broker_truth_recovery",
    "paper_only": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
}
path = Path(status_artifact)
tmp = path.with_name(f".{path.name}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(path)
print(json.dumps(payload, indent=2, sort_keys=True))
PY
}

run_audit() {
  "${PYTHON_BIN}" -m "${AUDIT_MODULE}" --repo-root "${REPO_ROOT}" --json >/dev/null
}

resolve_recovery_profile() {
  local status_path="${1:-}"
  "${PYTHON_BIN}" - "$status_path" "$APPROVED_PROFILE_ARTIFACT" "$STARTUP_ARTIFACT" "$RUNTIME_DIR/probationary_paper.pid.json" "$RUNTIME_DIR/paper_runtime_config_paths.txt" "${TRACK_B_PAPER_RECOVERY_APPROVED_PROFILE:-}" "${TRACK_B_PAPER_STACK_PROFILE:-}" "${TRACK_B_PAPER_RECOVERY_ALLOW_CANONICAL:-0}" <<'PY'
import json
import re
import sys
from pathlib import Path

(
    status_path,
    approved_profile_artifact,
    startup_artifact,
    pid_metadata_path,
    config_paths_file,
    explicit_recovery_profile,
    explicit_stack_profile,
    allow_canonical,
) = sys.argv[1:]

approved_profiles = {
    "mnq_mes_active_evidence",
    "mnq_mes_globex_active_evidence",
    "mnq_mes_session_coverage_active_evidence",
    "mnq_mes_london_open_active_evidence",
    "mnq_mes_london_late_mnq_short_active_evidence",
    "mnq_mes_full_session_active_evidence",
}


def load_json(path: str) -> dict:
    if not path:
        return {}
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def read_lines(path: str) -> list[str]:
    try:
        return [line.strip() for line in Path(path).read_text(encoding="utf-8").splitlines() if line.strip()]
    except Exception:
        return []


def profile_from_paths(paths: object) -> str:
    if not isinstance(paths, list):
        return ""
    for raw in paths:
        text = str(raw)
        match = re.search(r"paper_stack_([A-Za-z0-9_]+)\.yaml$", text)
        if match:
            return match.group(1)
    return ""


def candidates() -> list[tuple[str, str]]:
    if explicit_recovery_profile.strip():
        yield explicit_recovery_profile.strip(), "TRACK_B_PAPER_RECOVERY_APPROVED_PROFILE"
    if explicit_stack_profile.strip():
        yield explicit_stack_profile.strip(), "TRACK_B_PAPER_STACK_PROFILE"

    approved_payload = load_json(approved_profile_artifact)
    for key in ("recovery_requested_profile", "approved_profile", "stack_profile", "profile"):
        value = str(approved_payload.get(key) or "").strip()
        if value:
            yield value, f"approved_profile_artifact.{key}"

    startup_payload = load_json(startup_artifact)
    for key in ("recovery_requested_profile", "stack_profile", "profile", "runtime_profile", "profile_id"):
        value = str(startup_payload.get(key) or "").strip()
        if value:
            yield value, f"startup_artifact.{key}"
    inferred = profile_from_paths(startup_payload.get("config_stack"))
    if inferred:
        yield inferred, "startup_artifact.config_stack"

    status_payload = load_json(status_path)
    config = status_payload.get("config") if isinstance(status_payload.get("config"), dict) else {}
    inferred = profile_from_paths(config.get("config_stack") or config.get("config_paths"))
    if inferred:
        yield inferred, "status.config_stack"
    for key in ("stack_profile", "runtime_profile", "profile", "profile_id"):
        value = str(config.get(key) or "").strip()
        if value:
            yield value, f"status.config.{key}"

    pid_payload = load_json(pid_metadata_path)
    for key in ("stack_profile", "runtime_profile", "profile", "profile_id"):
        value = str(pid_payload.get(key) or "").strip()
        if value:
            yield value, f"pid_metadata.{key}"

    inferred = profile_from_paths(read_lines(config_paths_file))
    if inferred:
        yield inferred, "paper_runtime_config_paths"


requested_profile = ""
profile_source = ""
for profile, source in candidates():
    requested_profile = profile
    profile_source = source
    break

canonical_explicitly_allowed = str(allow_canonical).lower() in {"1", "true", "yes"} and profile_source in {
    "TRACK_B_PAPER_RECOVERY_APPROVED_PROFILE",
    "TRACK_B_PAPER_STACK_PROFILE",
}
approved = requested_profile in approved_profiles or (requested_profile == "canonical" and canonical_explicitly_allowed)
if not requested_profile:
    blocker = "RECOVERY_BLOCKED_PROFILE_NOT_APPROVED"
elif requested_profile == "canonical" and not canonical_explicitly_allowed:
    blocker = "RECOVERY_BLOCKED_CANONICAL_PROFILE_NOT_EXPLICITLY_APPROVED"
elif not approved:
    blocker = "RECOVERY_BLOCKED_PROFILE_NOT_APPROVED"
else:
    blocker = None

print(
    json.dumps(
        {
            "schema_version": "track_b_recovery_profile_resolution_v1",
            "recovery_requested_profile": requested_profile or None,
            "recovery_profile_source": profile_source or None,
            "recovery_profile_approved": bool(approved),
            "recovery_profile_blocker": blocker,
        },
        indent=2,
        sort_keys=True,
    )
)
PY
}

case "${mode}" in
  status)
    emit_status
    ;;
  tick)
    if [[ -f "${DISABLED_MARKER}" ]]; then
      write_tick_artifact "NO_ACTION_RECOVERY_DISABLED_BY_OPERATOR" "RECOVERY_DISABLED_BY_OPERATOR" "Standalone recovery is explicitly disabled by operator."
      echo "Track B recovery tick: disabled by operator; no action."
      exit 0
    fi
    pid=""
    if [[ -s "${RUNTIME_DIR}/probationary_paper.pid" ]]; then
      pid="$(tr -dc '0-9' < "${RUNTIME_DIR}/probationary_paper.pid" || true)"
    fi
    if [[ -n "${pid}" ]] && ps -p "${pid}" >/dev/null 2>&1; then
      if TRACK_B_PAPER_STACK_PROFILE="mnq_mes_full_session_active_evidence" bash "${THIN_RECOVERY_SCRIPT}" check >/dev/null; then
        write_tick_artifact "NO_ACTION_RUNTIME_RUNNING" "" "Exact runtime PID artifact is alive and matches thin PAPER runtime shape."
        echo "Track B recovery tick: runtime already running with expected shape; no action."
        exit 0
      fi
      thin_classification="$(json_value "${STATE_DIR}/latest_thin_paper_runtime_recovery.json" "classification")"
      if [[ "${thin_classification}" == "BROKER_TRUTH_NOT_CLEAN_RECOVERY_BLOCKED" ]]; then
        write_tick_artifact "NO_ACTION_BROKER_TRUTH_NOT_CLEAN" "BROKER_TRUTH_NOT_CLEAN_RECOVERY_BLOCKED" "Runtime shape check could not restart because broker truth is not clean."
        echo "Track B recovery tick: broker truth is not clean; no restart."
        exit 0
      fi
      write_tick_artifact "THIN_RECOVERY_RESTART_REQUIRED" "" "Exact runtime PID artifact is alive but failed thin runtime shape verification; invoking thin restart."
      TRACK_B_PAPER_STACK_PROFILE="mnq_mes_full_session_active_evidence" bash "${THIN_RECOVERY_SCRIPT}" restart
      exit 0
    fi
    write_tick_artifact "START_REQUESTED_THIN_PAPER_RECOVERY" "" "Runtime PID artifact is absent/dead; invoking thin broker-truth PAPER recovery path."
    TRACK_B_PAPER_STACK_PROFILE="mnq_mes_full_session_active_evidence" bash "${THIN_RECOVERY_SCRIPT}" start
    ;;
  enable)
    bash "${REPO_ROOT}/scripts/generate_track_b_launchd_plists.sh" --json >/dev/null
    mkdir -p "${HOME}/Library/LaunchAgents"
    rm -f "${DISABLED_MARKER}"
    cp "${TEMPLATE_PATH}" "${PLIST_PATH}"
    launchctl bootstrap "gui/$(id -u)" "${PLIST_PATH}" 2>/dev/null || true
    launchctl enable "gui/$(id -u)/${LABEL}" 2>/dev/null || true
    echo "Track B PAPER runtime recovery launchd service enabled: ${LABEL} interval=${RECOVERY_TICK_INTERVAL_SECONDS}s"
    ;;
  disable)
    write_disabled_marker
    launchctl bootout "gui/$(id -u)" "${PLIST_PATH}" 2>/dev/null || true
    launchctl disable "gui/$(id -u)/${LABEL}" 2>/dev/null || true
    echo "Track B hourly PAPER runtime recovery launchd service disabled: ${LABEL}"
    ;;
  *)
    echo "usage: $0 {status|tick|enable|disable}" >&2
    exit 2
    ;;
esac
