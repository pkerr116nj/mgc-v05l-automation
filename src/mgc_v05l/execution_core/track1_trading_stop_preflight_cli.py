"""CLI for Track 1 trading-stop preflight."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from .track1_trading_stop_preflight import DEFAULT_OUTPUT_ROOT, MAX_CANDIDATE_FILES, build_track1_trading_stop_preflight


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a bounded read-only preflight for legacy Track 1 trading-stop evidence."
    )
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--max-files", type=int, default=MAX_CANDIDATE_FILES)
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_track1_trading_stop_preflight(
        repo_root=args.repo_root,
        output_root=args.output_root,
        max_files=args.max_files,
        write=not args.no_write,
    )
    summary = {
        "classification": result.report.get("classification"),
        "report_json": str(result.report_json),
        "report_md": str(result.report_md),
        "track1_like_artifact_count": result.report.get("track1_like_artifact_count"),
        "trade_artifact_count": result.report.get("trade_artifact_count"),
        "signal_artifact_count": result.report.get("signal_artifact_count"),
        "handoff_artifact_count": result.report.get("handoff_artifact_count"),
        "breakpoint": result.report.get("breakpoint"),
        "recommended_next_action": result.report.get("recommended_next_action"),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
