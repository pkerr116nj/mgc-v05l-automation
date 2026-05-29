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
  launchctl list "${LABEL}" >/dev/null 2>&1
}

emit_status() {
  local loaded="false"
  local state="PAUSED"
  if launchctl_loaded; then
    loaded="true"
    state="ACTIVE"
  fi
  "${PYTHON_BIN}" - "$state" "$loaded" "$PLIST_PATH" "$TEMPLATE_PATH" "$REPO_ROOT" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

state, loaded, plist, template, repo = sys.argv[1:]
payload = {
    "schema_version": "track_b_hourly_paper_runtime_recovery_launchd_status_v1",
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "classification": f"LAUNCHD_RECOVERY_{state}",
    "state": state,
    "launchd_loaded": loaded == "true",
    "survives_codex_exit": loaded == "true",
    "launchd_owned": loaded == "true",
    "can_restart_runtime_without_codex": loaded == "true",
    "label": "com.mgc.trackb.paper-runtime-recovery",
    "plist_path": plist,
    "repo_template_path": template,
    "repo_root": repo,
    "enable_command": "bash scripts/track_b_hourly_paper_runtime_recovery.sh enable",
    "disable_command": "bash scripts/track_b_hourly_paper_runtime_recovery.sh disable",
    "status_command": "bash scripts/track_b_hourly_paper_runtime_recovery.sh status",
    "tick_command": "bash scripts/track_b_hourly_paper_runtime_recovery.sh tick",
    "runtime_start_path": "scripts/track_b_start_paper_stack.sh",
    "runtime_status_path": "scripts/track_b_status_paper_stack.sh",
    "paper_only": True,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
}
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
    run_audit
    status_tmp="$(mktemp "${TMPDIR:-/tmp}/track_b_paper_stack_status.XXXXXX.json")"
    trap 'rm -f "${status_tmp}"' EXIT
    bash "${STATUS_SCRIPT}" --json > "${status_tmp}"
    runtime_running="$(json_value "${status_tmp}" runtime.running)"
    ready_submit_capable="$(json_value "${status_tmp}" readiness.ready_submit_capable)"
    restart_allowed="$(json_value "${status_tmp}" readiness.restart_allowed_if_runtime_down)"
    next_action="$(json_value "${status_tmp}" next_action)"
    if [[ "${runtime_running}" == "true" ]]; then
      echo "Track B recovery tick: runtime already running; no action."
      exit 0
    fi
    if [[ "${restart_allowed}" != "true" || "${next_action}" != "run scripts/track_b_start_paper_stack.sh" ]]; then
      echo "Track B recovery tick: PAUSED by safety/status; restart_allowed=${restart_allowed} ready=${ready_submit_capable} next_action=${next_action}."
      exit 0
    fi
    bash "${START_SCRIPT}"
    ;;
  enable)
    if [[ ! -f "${TEMPLATE_PATH}" ]]; then
      bash "${REPO_ROOT}/scripts/generate_track_b_launchd_plists.sh" --json >/dev/null
    fi
    mkdir -p "${HOME}/Library/LaunchAgents"
    cp "${TEMPLATE_PATH}" "${PLIST_PATH}"
    launchctl bootstrap "gui/$(id -u)" "${PLIST_PATH}" 2>/dev/null || true
    launchctl enable "gui/$(id -u)/${LABEL}" 2>/dev/null || true
    echo "Track B hourly PAPER runtime recovery launchd service enabled: ${LABEL}"
    ;;
  disable)
    launchctl bootout "gui/$(id -u)" "${PLIST_PATH}" 2>/dev/null || true
    launchctl disable "gui/$(id -u)/${LABEL}" 2>/dev/null || true
    echo "Track B hourly PAPER runtime recovery launchd service disabled: ${LABEL}"
    ;;
  *)
    echo "usage: $0 {status|tick|enable|disable}" >&2
    exit 2
    ;;
esac
