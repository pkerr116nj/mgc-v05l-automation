"""CLI helper for evaluating captured IBKR Milestone A snapshots."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .ibkr_milestone_a_acceptance import evaluate_ibkr_milestone_a_snapshot


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-milestone-a-check")
    parser.add_argument(
        "--snapshot",
        required=True,
        help="Path to a captured IBKR broker-truth snapshot JSON file.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional path to write the evaluated acceptance result as JSON.",
    )
    return parser


def main(argv: list[str] | None = None) -> dict[str, Any]:
    args = build_parser().parse_args(argv)
    snapshot_path = Path(args.snapshot)
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    result = evaluate_ibkr_milestone_a_snapshot(payload)
    if args.output:
        Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    return result

