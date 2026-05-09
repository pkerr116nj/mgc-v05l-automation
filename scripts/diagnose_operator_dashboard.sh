#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

HOST="${OPERATOR_DASHBOARD_HOST:-127.0.0.1}"
PORT="${OPERATOR_DASHBOARD_PORT:-8790}"
RUNTIME_DIR="${REPO_ROOT}/outputs/operator_dashboard/runtime"
INFO_FILE="${OPERATOR_DASHBOARD_INFO_FILE:-${RUNTIME_DIR}/operator_dashboard.json}"
READINESS_FILE="${OPERATOR_DASHBOARD_READINESS_FILE:-${RUNTIME_DIR}/operator_dashboard_readiness.json}"
URL="http://${HOST}:${PORT}/"

echo "Expected backend URL: ${URL}"
echo "Info file: ${INFO_FILE}"
echo "Readiness file: ${READINESS_FILE}"

LISTENER_PID="$(lsof -nP -iTCP:"${PORT}" -sTCP:LISTEN -t 2>/dev/null | head -n 1 || true)"
if [[ -n "${LISTENER_PID}" ]]; then
  echo "Port ${PORT} listening: yes"
  echo "Owning PID: ${LISTENER_PID}"
  echo "Owning command: $(ps -p "${LISTENER_PID}" -o command= 2>/dev/null || true)"
else
  echo "Port ${PORT} listening: no"
fi

"${PYTHON_BIN}" - <<'PY' "${URL}" "${INFO_FILE}" "${READINESS_FILE}"
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

url = sys.argv[1].rstrip("/") + "/"
info_file = Path(sys.argv[2])
readiness_file = Path(sys.argv[3])


def probe_json(endpoint: str, timeout: float) -> dict[str, object]:
    try:
        with urllib.request.urlopen(url + endpoint, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return {"ok": True, "payload": payload}
    except Exception as exc:  # noqa: BLE001 - diagnostic script
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


health = probe_json("health", 3)
dashboard = probe_json("api/dashboard", 10)
info = json.loads(info_file.read_text(encoding="utf-8")) if info_file.exists() else {}
readiness = json.loads(readiness_file.read_text(encoding="utf-8")) if readiness_file.exists() else {}

print(f"Health endpoint ok: {health['ok']}")
if health["ok"]:
    payload = health["payload"]
    print(f"Health status: {payload.get('status')}")
    print(f"Health ready: {payload.get('ready')}")
    print(f"Health pid: {payload.get('pid')}")
    print(f"Health build: {payload.get('build_stamp')}")
else:
    print(f"Health error: {health.get('error')}")

print(f"Dashboard API ok: {dashboard['ok']}")
if dashboard["ok"]:
    payload = dashboard["payload"]
    print(f"Dashboard API generated_at: {payload.get('generated_at')}")
    print(f"Dashboard API keys: {', '.join(sorted(payload.keys())[:12])}")
else:
    print(f"Dashboard API error: {dashboard.get('error')}")

print(f"Info file pid: {info.get('pid')}")
print(f"Info file url: {info.get('url')}")
print(f"Readiness state: {readiness.get('readiness_state')}")
print(f"Readiness reason: {readiness.get('reason_code')}")
print(f"Readiness detail: {readiness.get('reason_detail')}")
print(f"Readiness launch_allowed: {readiness.get('launch_allowed')}")
PY
