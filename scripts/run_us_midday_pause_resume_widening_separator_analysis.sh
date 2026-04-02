#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 STRICT_SUMMARY MEDIUM_1_SUMMARY MEDIUM_2_SUMMARY" >&2
  exit 1
fi

PYTHONPATH=src .venv/bin/python -m mgc_v05l.app.us_midday_pause_resume_widening_separator_analysis "$1" "$2" "$3"
