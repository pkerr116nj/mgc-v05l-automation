#!/bin/zsh
set -euo pipefail

SCRIPT_DIR=${0:A:h}
PORT=${OBSERVATORY_PORT:-8765}
URL="http://127.0.0.1:${PORT}/?scenario=ATLANTIC_BRIDGE_MIDDAY&venues=prepared"
LOG_FILE="/tmp/observatory-prototype-${PORT}.log"

if ! nc -z 127.0.0.1 "${PORT}" >/dev/null 2>&1; then
  nohup python3 -m http.server "${PORT}" --bind 127.0.0.1 --directory "${SCRIPT_DIR}" >"${LOG_FILE}" 2>&1 &!
  for _ in {1..40}; do
    if nc -z 127.0.0.1 "${PORT}" >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done
fi

if ! nc -z 127.0.0.1 "${PORT}" >/dev/null 2>&1; then
  print -u2 "Observatory server failed to start. See ${LOG_FILE}."
  exit 1
fi

if [[ "${1:-}" != "--no-open" ]]; then
  open "${URL}"
fi
print "Observatory: ${URL}"
