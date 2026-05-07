"""CLI for Track 1 signal-to-handoff breakpoint audit."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from .track1_signal_handoff_breakpoint_audit import (
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_PREFLIGHT,
    build_track1_signal_handoff_breakpoint_audit,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a bounded read-only Track 1 signal-to-handoff breakpoint audit."
    )
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--preflight-path", type=Path, default=DEFAULT_PREFLIGHT)
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = build_track1_signal_handoff_breakpoint_audit(
        repo_root=args.repo_root,
        output_root=args.output_root,
        preflight_path=args.preflight_path,
        write=not args.no_write,
    )
    summary = {
        "classification": result.report.get("classification"),
        "missing_link": result.report.get("missing_link"),
        "report_json": str(result.report_json),
        "report_md": str(result.report_md),
        "first_known_signal_without_trade": result.report.get("first_known_signal_without_trade"),
        "suspected_commit_or_config_breakpoint": result.report.get("suspected_commit_or_config_breakpoint"),
        "recommended_next_action": result.report.get("recommended_next_action"),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
