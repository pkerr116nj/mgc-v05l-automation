#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LABEL="com.mgc_v05l.research_daily_capture.daily"
TEMPLATE_PATH="${REPO_ROOT}/ops/launchd/${LABEL}.plist.template"
LAUNCH_AGENTS_DIR="${HOME}/Library/LaunchAgents"
PLIST_PATH="${LAUNCH_AGENTS_DIR}/${LABEL}.plist"
APP_SUPPORT_DIR="${HOME}/Library/Application Support/mgc_v05l"
RUNTIME_ROOT="${APP_SUPPORT_DIR}/research_daily_capture_runtime"
RUNTIME_CONFIG_DIR="${RUNTIME_ROOT}/config"
LAUNCHER_PATH="${RUNTIME_ROOT}/research_daily_capture_runner.sh"
RUNTIME_ENV_PATH="${RUNTIME_ROOT}/schwab_env.sh"
RUNTIME_TOKEN_PATH="${RUNTIME_ROOT}/tokens.json"
RUNTIME_POLICY_PATH="${RUNTIME_CONFIG_DIR}/data_storage_policy.json"
LOG_DIR="${RUNTIME_ROOT}/logs"
STDOUT_PATH="${LOG_DIR}/daily_capture.stdout.log"
STDERR_PATH="${LOG_DIR}/daily_capture.stderr.log"
GUI_DOMAIN="gui/$(id -u)"
RUNNER_PATH="${REPO_ROOT}/scripts/run_research_daily_capture.sh"
SOURCE_TOKEN_PATH=""

# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/common_env.sh"

SOURCE_TOKEN_PATH="${SCHWAB_TOKEN_FILE:-${REPO_ROOT}/.local/schwab/tokens.json}"

mkdir -p "${LAUNCH_AGENTS_DIR}" "${RUNTIME_CONFIG_DIR}" "${LOG_DIR}"
chmod +x "${RUNNER_PATH}"

if [[ ! -f "${TEMPLATE_PATH}" ]]; then
  echo "Missing launchd template: ${TEMPLATE_PATH}" >&2
  exit 1
fi

if [[ ! -f "${SOURCE_TOKEN_PATH}" ]]; then
  echo "Missing Schwab token file for launchd runtime copy: ${SOURCE_TOKEN_PATH}" >&2
  exit 1
fi

cp "${REPO_ROOT}/config/base.yaml" "${RUNTIME_CONFIG_DIR}/base.yaml"
cp "${REPO_ROOT}/config/replay.yaml" "${RUNTIME_CONFIG_DIR}/replay.yaml"
cp "${REPO_ROOT}/config/schwab.local.json" "${RUNTIME_CONFIG_DIR}/schwab.local.json"
cp "${REPO_ROOT}/config/data_storage_policy.json" "${RUNTIME_POLICY_PATH}"
cp "${SOURCE_TOKEN_PATH}" "${RUNTIME_TOKEN_PATH}"
chmod 600 "${RUNTIME_TOKEN_PATH}"

cat > "${RUNTIME_ENV_PATH}" <<EOF
#!/usr/bin/env bash
export SCHWAB_APP_KEY=$(printf '%q' "${SCHWAB_APP_KEY}")
export SCHWAB_APP_SECRET=$(printf '%q' "${SCHWAB_APP_SECRET}")
export SCHWAB_CALLBACK_URL=$(printf '%q' "${SCHWAB_CALLBACK_URL}")
export SCHWAB_TOKEN_FILE=$(printf '%q' "${RUNTIME_TOKEN_PATH}")
EOF

chmod 600 "${RUNTIME_ENV_PATH}"

cat > "${LAUNCHER_PATH}" <<EOF
#!/usr/bin/env bash
set -euo pipefail

export REPO_ROOT="${REPO_ROOT}"
RUNTIME_ROOT="${RUNTIME_ROOT}"
RUNTIME_ENV_PATH="${RUNTIME_ENV_PATH}"
PYTHON_BIN="\${REPO_ROOT}/.venv/bin/python"
CONFIG_BASE="\${RUNTIME_ROOT}/config/base.yaml"
CONFIG_REPLAY="\${RUNTIME_ROOT}/config/replay.yaml"
SCHWAB_CONFIG_PATH="\${RUNTIME_ROOT}/config/schwab.local.json"
TOKEN_PATH="${RUNTIME_TOKEN_PATH}"
POLICY_PATH="${RUNTIME_POLICY_PATH}"

if [[ -f "\${RUNTIME_ENV_PATH}" ]]; then
  # shellcheck disable=SC1091
  source "\${RUNTIME_ENV_PATH}"
fi

export PYTHONPATH="\${REPO_ROOT}/src\${PYTHONPATH:+:\${PYTHONPATH}}"
export SCHWAB_TOKEN_FILE="\${SCHWAB_TOKEN_FILE:-\${TOKEN_PATH}}"
export PYTHONUNBUFFERED=1

exec "\${PYTHON_BIN}" -m mgc_v05l.app.main research-daily-capture \\
  --config "\${CONFIG_BASE}" \\
  --config "\${CONFIG_REPLAY}" \\
  --policy-config "\${POLICY_PATH}" \\
  --token-file "\${SCHWAB_TOKEN_FILE}" \\
  --schwab-config "\${SCHWAB_CONFIG_PATH}"
EOF

chmod +x "${LAUNCHER_PATH}"

sed \
  -e "s|__LAUNCHER_PATH__|${LAUNCHER_PATH}|g" \
  -e "s|__REPO_ROOT__|${REPO_ROOT}|g" \
  -e "s|__STDOUT_PATH__|${STDOUT_PATH}|g" \
  -e "s|__STDERR_PATH__|${STDERR_PATH}|g" \
  "${TEMPLATE_PATH}" > "${PLIST_PATH}"

launchctl bootout "${GUI_DOMAIN}" "${PLIST_PATH}" >/dev/null 2>&1 || true
launchctl bootstrap "${GUI_DOMAIN}" "${PLIST_PATH}"
launchctl enable "${GUI_DOMAIN}/${LABEL}"

echo "Installed ${LABEL}"
echo "Plist: ${PLIST_PATH}"
echo "Launcher: ${LAUNCHER_PATH}"
echo "Runtime Root: ${RUNTIME_ROOT}"
echo "Runtime Env: ${RUNTIME_ENV_PATH}"
echo "Runtime Token: ${RUNTIME_TOKEN_PATH}"
echo "Runtime Policy: ${RUNTIME_POLICY_PATH}"
echo "Stdout: ${STDOUT_PATH}"
echo "Stderr: ${STDERR_PATH}"
echo "Verify: launchctl print ${GUI_DOMAIN}/${LABEL}"
