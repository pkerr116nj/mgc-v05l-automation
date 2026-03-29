#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KILL_SWITCH="$ROOT_DIR/outputs/probationary_quant_canaries/active_trend_participation_engine/DISABLE_ACTIVE_TREND_PARTICIPATION_CANARY"

if [[ -f "$KILL_SWITCH" ]]; then
  rm -f "$KILL_SWITCH"
fi

echo "$KILL_SWITCH"
