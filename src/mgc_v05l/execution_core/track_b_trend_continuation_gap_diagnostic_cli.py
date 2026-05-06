"""CLI for the Track B trend-continuation gap research diagnostic."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from .track_b_trend_continuation_gap_diagnostic import (
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_POSTMORTEM_PATH,
    build_track_b_trend_continuation_gap_diagnostic,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a read-only Track B trend-continuation gap research diagnostic."
    )
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--postmortem-path", type=Path, default=DEFAULT_POSTMORTEM_PATH)
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_track_b_trend_continuation_gap_diagnostic(
        repo_root=args.repo_root,
        output_root=args.output_root,
        postmortem_path=args.postmortem_path,
        write=not args.no_write,
    )
    print(json.dumps(result.report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
