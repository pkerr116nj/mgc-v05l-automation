"""CLI for Track B missed-move forensic replay."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from .track_b_missed_move_forensic_replay import (
    DEFAULT_INSTRUMENTS,
    DEFAULT_MAX_RUNTIME_REPORTS,
    DEFAULT_TRACK_B_MISSED_MOVE_FORENSIC_OUTPUT_ROOT,
    build_track_b_missed_move_forensic_replay,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a bounded read-only Track B missed-move forensic replay diagnostic."
    )
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_MISSED_MOVE_FORENSIC_OUTPUT_ROOT)
    parser.add_argument("--instrument", action="append", choices=DEFAULT_INSTRUMENTS)
    parser.add_argument("--max-runtime-reports", type=int, default=DEFAULT_MAX_RUNTIME_REPORTS)
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_track_b_missed_move_forensic_replay(
        repo_root=args.repo_root,
        output_root=args.output_root,
        instruments=tuple(args.instrument or DEFAULT_INSTRUMENTS),
        max_runtime_reports=args.max_runtime_reports,
        write=not args.no_write,
    )
    print(json.dumps(result.report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
