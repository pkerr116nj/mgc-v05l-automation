"""CLI for the read-only Track B Position Truth notification sidecar."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_position_truth_notifier import (
    DEFAULT_NOTIFICATION_EVENTS,
    DEFAULT_NOTIFICATION_STATE,
    TrackBPositionTruthNotificationConfig,
    process_position_truth_notifications,
)
from mgc_v05l.execution_core.track_b_position_truth_monitor import DEFAULT_TRADE_OUTCOME_EVENTS

REPO_ROOT = Path(__file__).resolve().parents[3]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Emit read-only notifications from Track B Position Truth events.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--event-log-path", type=Path, default=DEFAULT_TRADE_OUTCOME_EVENTS)
    parser.add_argument("--notification-log-path", type=Path, default=DEFAULT_NOTIFICATION_EVENTS)
    parser.add_argument("--state-path", type=Path, default=DEFAULT_NOTIFICATION_STATE)
    parser.add_argument("--rate-limit-seconds", type=float, default=300.0)
    parser.add_argument("--disable-macos-notifications", action="store_true")
    parser.add_argument("--once", action="store_true", help="Process current unread events and exit.")
    parser.add_argument("--service", action="store_true", help="Continuously tail Position Truth events.")
    parser.add_argument("--interval-seconds", type=float, default=5.0)
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
    config = TrackBPositionTruthNotificationConfig(
        repo_root=Path(args.repo_root),
        event_log_path=Path(args.event_log_path),
        notification_log_path=Path(args.notification_log_path),
        state_path=Path(args.state_path),
        rate_limit_seconds=float(args.rate_limit_seconds),
        enable_macos_notifications=not bool(args.disable_macos_notifications),
    )
    notifications = process_position_truth_notifications(config=config)
    print(
        json.dumps(
            {
                "notifications_emitted": len(notifications),
                "event_log_path": str(config.resolve(config.event_log_path)),
                "notification_log_path": str(config.resolve(config.notification_log_path)),
                "state_path": str(config.resolve(config.state_path)),
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
