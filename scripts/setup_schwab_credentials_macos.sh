#!/usr/bin/env bash

set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]] || ! command -v security >/dev/null 2>&1; then
  echo "This helper requires macOS Keychain." >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ACCOUNT_NAME="${USER}"
KEY_SERVICE="mgc-v05l-schwab-app-key"
SECRET_SERVICE="mgc-v05l-schwab-app-secret"

read -r -p "Schwab application key: " APP_KEY
read -r -s -p "Schwab application secret: " APP_SECRET
echo
read -r -p "Callback URL [https://127.0.0.1:8182/callback]: " CALLBACK_URL
CALLBACK_URL="${CALLBACK_URL:-https://127.0.0.1:8182/callback}"

if [[ -z "${APP_KEY}" || -z "${APP_SECRET}" ]]; then
  echo "Application key and secret are required." >&2
  exit 1
fi

security add-generic-password -U -a "${ACCOUNT_NAME}" -s "${KEY_SERVICE}" -w "${APP_KEY}" >/dev/null
security add-generic-password -U -a "${ACCOUNT_NAME}" -s "${SECRET_SERVICE}" -w "${APP_SECRET}" >/dev/null

mkdir -p "${REPO_ROOT}/.local/schwab"
chmod 700 "${REPO_ROOT}/.local" "${REPO_ROOT}/.local/schwab"
cat > "${REPO_ROOT}/.local/schwab_env.sh" <<EOF
export SCHWAB_APP_KEY="\$(security find-generic-password -a "\${USER}" -s "${KEY_SERVICE}" -w)"
export SCHWAB_APP_SECRET="\$(security find-generic-password -a "\${USER}" -s "${SECRET_SERVICE}" -w)"
export SCHWAB_CALLBACK_URL="${CALLBACK_URL}"
export SCHWAB_TOKEN_FILE="${REPO_ROOT}/.local/schwab/tokens.json"
EOF
chmod 600 "${REPO_ROOT}/.local/schwab_env.sh"

echo "Schwab application credentials are stored in macOS Keychain. Local OAuth environment created at .local/schwab_env.sh."
echo "Next: bash scripts/run_schwab_token_web.sh"
