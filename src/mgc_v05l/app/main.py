"""CLI entrypoint for replay-first usage."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from .bootstrap import bootstrap_service
from .runner import StrategyServiceRunner
from ..research import build_causal_momentum_report, write_causal_momentum_report_csv


def build_parser() -> argparse.ArgumentParser:
    """Build the replay-first CLI parser."""
    parser = argparse.ArgumentParser(prog="mgc-v05l")
    subparsers = parser.add_subparsers(dest="command", required=True)

    replay_parser = subparsers.add_parser("replay", help="Run a deterministic CSV replay.")
    replay_parser.add_argument("--csv", required=True, help="Path to replay CSV with locked columns.")
    replay_parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Config file path. May be supplied multiple times; later files override earlier ones.",
    )

    report_parser = subparsers.add_parser(
        "research-causal-report",
        help="Export experimental causal momentum-shape features from replay data.",
    )
    report_parser.add_argument("--csv", required=True, help="Path to replay CSV with locked columns.")
    report_parser.add_argument("--output", required=True, help="Output CSV path for the research report.")
    report_parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Config file path. May be supplied multiple times; later files override earlier ones.",
    )
    report_parser.add_argument(
        "--smoothing-length",
        type=int,
        default=3,
        help="Trailing exponential smoothing length for the experimental report.",
    )
    report_parser.add_argument(
        "--normalization-floor",
        default="0.01",
        help="Minimum normalization denominator for ATR-scaled derivatives.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and print a small JSON summary."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "replay":
        config_paths = args.config or ["config/base.yaml", "config/replay.yaml"]
        container, _ = bootstrap_service([Path(path) for path in config_paths])
        summary = StrategyServiceRunner(container).run_replay(args.csv)
        print(json.dumps(asdict(summary), sort_keys=True))
        return 0

    if args.command == "research-causal-report":
        config_paths = args.config or ["config/base.yaml", "config/replay.yaml"]
        container, _ = bootstrap_service([Path(path) for path in config_paths])
        bars = container.replay_feed.load_csv(args.csv)
        rows = build_causal_momentum_report(
            bars=bars,
            settings=container.settings,
            smoothing_length=args.smoothing_length,
            normalization_floor=Decimal(args.normalization_floor),
        )
        output_path = write_causal_momentum_report_csv(rows, args.output)
        print(
            json.dumps(
                {
                    "rows": len(rows),
                    "output": str(output_path),
                    "research_only": True,
                },
                sort_keys=True,
            )
        )
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2
