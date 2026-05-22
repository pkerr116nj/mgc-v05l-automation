"""CLI for the read-only Track B PAPER position truth monitor."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_position_truth_monitor import (
    DEFAULT_DASHBOARD_POSITION_TRUTH_PROJECTION,
    DEFAULT_POSITION_TRUTH_ARTIFACT,
    DEFAULT_TRADE_OUTCOME_EVENTS,
    TrackBPositionTruthMonitorConfig,
    build_track_b_position_truth,
    write_track_b_position_truth,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the read-only Track B PAPER position truth artifact.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_POSITION_TRUTH_ARTIFACT)
    parser.add_argument("--event-log-path", type=Path, default=DEFAULT_TRADE_OUTCOME_EVENTS)
    parser.add_argument("--dashboard-projection-path", type=Path, default=DEFAULT_DASHBOARD_POSITION_TRUTH_PROJECTION)
    parser.add_argument("--no-dashboard-projection", action="store_true")
    parser.add_argument("--once", action="store_true", help="Run one monitor pass and exit.")
    parser.add_argument("--service", action="store_true", help="Run continuously, writing the latest artifact each interval.")
    parser.add_argument("--interval-seconds", type=float, default=30.0)
    parser.add_argument("--no-write", action="store_true", help="Print the payload without writing artifacts.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.service:
        while True:
            _run_once(args)
            time.sleep(max(float(args.interval_seconds), 1.0))
    _run_once(args)
    return 0


def _run_once(args: argparse.Namespace) -> None:
    config = TrackBPositionTruthMonitorConfig(
        repo_root=Path(args.repo_root),
        output_path=Path(args.output_path),
        event_log_path=Path(args.event_log_path),
        dashboard_projection_path=None
        if args.no_dashboard_projection
        else Path(args.dashboard_projection_path),
    )
    payload = build_track_b_position_truth(config=config, now=datetime.now().astimezone())
    if args.no_write:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return
    output_path, events = write_track_b_position_truth(config=config, payload=payload)
    print(
        json.dumps(
            {
                "classification": payload.get("summary", {}).get("overall_classification"),
                "output_path": str(output_path),
                "event_log_path": str(config.resolve(config.event_log_path)),
                "dashboard_projection_path": None
                if config.dashboard_projection_path is None
                else str(config.resolve(config.dashboard_projection_path)),
                "events_appended": len(events),
                "read_only": True,
                "paper_proof_invoked": False,
                "live_money_eligible": False,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
