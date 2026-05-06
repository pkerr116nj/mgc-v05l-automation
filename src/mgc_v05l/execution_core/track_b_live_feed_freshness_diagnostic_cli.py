"""CLI for bounded Track B Databento Live freshness diagnostics."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .track_b_live_feed_freshness_diagnostic import (
    DEFAULT_TRACK_B_LIVE_FEED_FRESHNESS_DIAGNOSTIC_OUTPUT_ROOT,
    build_track_b_live_feed_freshness_diagnostic,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a bounded Track B Live feed freshness diagnostic.")
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_LIVE_FEED_FRESHNESS_DIAGNOSTIC_OUTPUT_ROOT)
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_track_b_live_feed_freshness_diagnostic(
        repo_root=args.repo_root,
        output_root=args.output_root,
        write=not args.no_write,
    )
    print(
        json.dumps(
            {
                "diagnosis_classification": result.report.get("diagnosis_classification"),
                "primary_blocker": result.report.get("primary_blocker"),
                "report_json": str(result.report_json),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
