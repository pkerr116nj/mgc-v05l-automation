#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LABEL="com.mgc.trackb.paper-runtime-recovery"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"
TEMPLATE_PATH="${REPO_ROOT}/var/launchd/track_b/${LABEL}.plist"
STATUS_SCRIPT="${REPO_ROOT}/scripts/track_b_status_paper_stack.sh"
START_SCRIPT="${REPO_ROOT}/scripts/track_b_start_paper_stack.sh"
AUDIT_MODULE="mgc_v05l.execution_core.track_b_hourly_runtime_recovery_audit"
PYTHON_BIN="${REPO_ROOT}/.venv/bin/python"
STATE_DIR="${REPO_ROOT}/outputs/track_b_execution_core/runtime_recovery"
DISABLED_MARKER="${STATE_DIR}/recovery_disabled_by_operator.json"
STATUS_ARTIFACT="${STATE_DIR}/latest_launchd_recovery_status.json"
LAST_TICK_ARTIFACT="${STATE_DIR}/latest_launchd_recovery_tick.json"
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
  mkdir -p "${STATE_DIR}"
  "${PYTHON_BIN}" - "$LAST_TICK_ARTIFACT" "$action" "$blocker" "$detail" "$status_path" "$REPO_ROOT" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

artifact, action, blocker, detail, status_path, repo_root = sys.argv[1:]
status_payload = {}
if status_path:
    try:
        status_payload = json.loads(Path(status_path).read_text(encoding="utf-8"))
    except Exception:
        status_payload = {}
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
}
path = Path(artifact)
tmp = path.with_name(f".{path.name}.tmp")
tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
tmp.replace(path)
PY
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
    "enable_command": "bash scripts/track_b_hourly_paper_runtime_recovery.sh enable",
    "disable_command": "bash scripts/track_b_hourly_paper_runtime_recovery.sh disable",
    "status_command": "bash scripts/track_b_hourly_paper_runtime_recovery.sh status",
    "tick_command": "bash scripts/track_b_hourly_paper_runtime_recovery.sh tick",
    "runtime_start_path": "scripts/track_b_start_paper_stack.sh",
    "runtime_status_path": "scripts/track_b_status_paper_stack.sh",
    "restart_authority_source": "canonical_paper_stack_restart_precheck",
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
    run_audit
    status_tmp="$(mktemp "${TMPDIR:-/tmp}/track_b_paper_stack_status.XXXXXX.json")"
    trap 'rm -f "${status_tmp}"' EXIT
    bash "${STATUS_SCRIPT}" --json > "${status_tmp}"
    runtime_running="$(json_value "${status_tmp}" runtime.running)"
    live_runtime_classification="$(json_value "${status_tmp}" live_runtime_environment.classification)"
    live_runtime_pid_alive="$(json_value "${status_tmp}" live_runtime_environment.runtime.pid_alive)"
    ready_submit_capable="$(json_value "${status_tmp}" readiness.ready_submit_capable)"
    restart_allowed="$(json_value "${status_tmp}" readiness.restart_allowed_if_runtime_down)"
    next_action="$(json_value "${status_tmp}" next_action)"
    restart_authority="$("${PYTHON_BIN}" -m mgc_v05l.execution_core.track_b_paper_stack_restart_precheck < "${status_tmp}")"
    restart_authority_allowed="$(printf '%s' "${restart_authority}" | "${PYTHON_BIN}" -c 'import json,sys; print(str(json.loads(sys.stdin.read()).get("restart_allowed") is True).lower())')"
    restart_authority_classification="$(printf '%s' "${restart_authority}" | "${PYTHON_BIN}" -c 'import json,sys; print(json.loads(sys.stdin.read()).get("classification") or "")')"
    duplicate_writer_detected="$(json_value "${status_tmp}" duplicate_writer.duplicate_writer_detected)"
    if [[ "${duplicate_writer_detected}" == "true" ]]; then
      write_tick_artifact "NO_ACTION_DUPLICATE_WRITER" "duplicate_writer_detected" "Duplicate writer guard blocks recovery start." "${status_tmp}"
      echo "Track B recovery tick: duplicate writer detected; no action."
      exit 0
    fi
    if [[ "${runtime_running}" == "true" && "${live_runtime_classification}" == "RUNTIME_DOWN_WITH_BROKER_EXPOSURE" ]]; then
      runtime_running="false"
    fi
    if [[ "${runtime_running}" == "true" && "${live_runtime_pid_alive}" == "false" ]]; then
      runtime_running="false"
    fi
    if [[ "${runtime_running}" == "true" ]]; then
      write_tick_artifact "NO_ACTION_RUNTIME_RUNNING" "" "Runtime is healthy/running; recovery did not start anything." "${status_tmp}"
      echo "Track B recovery tick: runtime already running; no action."
      exit 0
    fi
    if [[ ( "${restart_allowed}" != "true" || "${next_action}" != "run scripts/track_b_start_paper_stack.sh" ) && "${restart_authority_allowed}" != "true" ]]; then
      write_tick_artifact "NO_ACTION_BLOCKED_GATES" "restart_not_allowed" "restart_allowed=${restart_allowed} restart_authority_allowed=${restart_authority_allowed} restart_authority=${restart_authority_classification} ready=${ready_submit_capable} next_action=${next_action}" "${status_tmp}"
      echo "Track B recovery tick: PAUSED by safety/status; restart_allowed=${restart_allowed} restart_authority_allowed=${restart_authority_allowed} restart_authority=${restart_authority_classification} ready=${ready_submit_capable} next_action=${next_action}."
      exit 0
    fi
    write_tick_artifact "START_REQUESTED_CANONICAL_PAPER_STACK" "" "Runtime down and canonical/precheck gates allow recovery start; restart_authority=${restart_authority_classification}." "${status_tmp}"
    bash "${START_SCRIPT}"
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
