"""CLI for the Track 1 vs Track B same-day parity audit."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from .track1_trackb_same_day_parity_audit import (
    DEFAULT_FORENSIC_PATH,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_POSTMORTEM_PATH,
    build_track1_trackb_same_day_parity_audit,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a read-only Track 1 vs Track B same-day parity audit.")
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--forensic-path", type=Path, default=DEFAULT_FORENSIC_PATH)
    parser.add_argument("--postmortem-path", type=Path, default=DEFAULT_POSTMORTEM_PATH)
    parser.add_argument("--track1-reference-path", type=Path, action="append", default=[])
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_track1_trackb_same_day_parity_audit(
        repo_root=args.repo_root,
        output_root=args.output_root,
        forensic_path=args.forensic_path,
        postmortem_path=args.postmortem_path,
        track1_reference_paths=args.track1_reference_path,
        write=not args.no_write,
    )
    print(json.dumps(result.report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

