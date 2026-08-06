"""CLI wrapper for fixture-only prospective market context Phase 1."""

from __future__ import annotations

import sys
from typing import Sequence

from mgc_v05l.execution_core.track_b_prospective_market_context import main as _main


def main(argv: Sequence[str] | None = None) -> int:
    return _main(sys.argv[1:] if argv is None else argv)


if __name__ == "__main__":
    raise SystemExit(main())
