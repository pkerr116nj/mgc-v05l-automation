#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

SYMBOL="${SYMBOL:-MGC}"

"${PYTHON_BIN}" -m mgc_v05l.app.mgc_impulse_burst_subclass_diagnostics \
  --symbol "${SYMBOL}"
