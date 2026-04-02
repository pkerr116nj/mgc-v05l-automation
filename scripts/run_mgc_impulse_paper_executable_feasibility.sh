#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common_env.sh"

"${PYTHON_BIN}" -m mgc_v05l.app.mgc_impulse_paper_executable_feasibility
