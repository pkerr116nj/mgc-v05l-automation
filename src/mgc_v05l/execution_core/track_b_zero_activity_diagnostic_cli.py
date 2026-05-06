"""CLI for the bounded Track B zero-activity diagnostic."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from .track_b_zero_activity_diagnostic import (
    DEFAULT_DECISION_BAR_AUDIT_WINDOW_MINUTES,
    DEFAULT_RECENT_CYCLE_LIMIT,
    DEFAULT_TRACK_B_ZERO_ACTIVITY_DIAGNOSTIC_OUTPUT_ROOT,
    build_track_b_zero_activity_diagnostic,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a bounded read-only diagnostic explaining why the Track B PAPER monitor has zero trade activity."
        )
    )
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_ZERO_ACTIVITY_DIAGNOSTIC_OUTPUT_ROOT)
    parser.add_argument("--recent-cycle-limit", type=int, default=DEFAULT_RECENT_CYCLE_LIMIT)
    parser.add_argument("--decision-bar-audit-window-minutes", type=int, default=DEFAULT_DECISION_BAR_AUDIT_WINDOW_MINUTES)
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_track_b_zero_activity_diagnostic(
        repo_root=args.repo_root,
        output_root=args.output_root,
        recent_cycle_limit=args.recent_cycle_limit,
        decision_bar_audit_window_minutes=args.decision_bar_audit_window_minutes,
        write=not args.no_write,
    )
    print(json.dumps(result.report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
