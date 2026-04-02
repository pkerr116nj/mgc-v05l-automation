#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/common_env.sh"

ARTIFACT_DIR="${REPO_ROOT}/outputs/probationary_pattern_engine"
TAIL_LINES="${TAIL_LINES:-40}"

exec tail -n "${TAIL_LINES}" -F \
  "${ARTIFACT_DIR}/branch_sources.jsonl" \
  "${ARTIFACT_DIR}/rule_blocks.jsonl" \
  "${ARTIFACT_DIR}/alerts.jsonl"
