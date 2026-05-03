"""CLI wrapper for the Track B no-submit shadow listener skeleton."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .shadow_listener import DEFAULT_SHADOW_LISTENER_OUTPUT_ROOT, ShadowListenerVerdict, ShadowListenerWatchVerdict, run_shadow_listener_cycle, run_shadow_listener_watch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Poll once for Track B shadow signal batch JSON files and run no-submit replay.")
    parser.add_argument("--listener-config-json", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--watch", action="store_true", help="Run bounded no-submit watch cycles instead of the default poll-once cycle.")
    parser.add_argument("--max-cycles", type=int, default=None, help="Maximum watch cycles when --watch is enabled.")
    parser.add_argument("--poll-seconds", type=float, default=None, help="Seconds to sleep between watch cycles when --watch is enabled.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config_payload = json.loads(args.listener_config_json.read_text(encoding="utf-8"))
    output_root = args.output_root or Path(config_payload.get("output_root") or DEFAULT_SHADOW_LISTENER_OUTPUT_ROOT)
    if args.watch:
        watch_payload = dict(config_payload)
        watch_payload["watch_enabled"] = True
        watch_payload["poll_once"] = False
        if args.max_cycles is not None:
            watch_payload["max_cycles"] = args.max_cycles
        if args.poll_seconds is not None:
            watch_payload["poll_seconds"] = args.poll_seconds
        result = run_shadow_listener_watch(config_payload=watch_payload, output_root=output_root)
        print(
            json.dumps(
                {
                    "watch_verdict": result.report["watch_verdict"],
                    "listener_mode": result.report["listener_mode"],
                    "current_cycle_number": result.report["current_cycle_number"],
                    "last_health_verdict": result.report["last_health_verdict"],
                    "processed_cycles": result.report["processed_cycles"],
                    "failed_cycles": result.report["failed_cycles"],
                    "no_file_cycles": result.report["no_file_cycles"],
                    "watch_exited_normally": result.report["watch_exited_normally"],
                    "submit_allowed": result.report["submit_allowed"],
                    "submit_attempted": result.report["submit_attempted"],
                    "live_money_readiness": result.report["live_money_readiness"],
                    "primary_blocker": result.report["primary_blocker"],
                    "required_next_action": result.report["required_next_action"],
                    "heartbeat_json": str(result.heartbeat_json),
                },
                sort_keys=True,
            )
        )
        return 0 if result.verdict == ShadowListenerWatchVerdict.COMPLETED else 2

    result = run_shadow_listener_cycle(config_payload=config_payload, output_root=output_root)
    print(
        json.dumps(
            {
                "listener_verdict": result.report["listener_verdict"],
                "files_discovered": result.report["files_discovered"],
                "files_succeeded": result.report["files_succeeded"],
                "files_failed": result.report["files_failed"],
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "live_money_readiness": result.report["live_money_readiness"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "health_report": result.report["health_report_path"],
                "latest_health_report": result.report["latest_health_report_path"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict in {ShadowListenerVerdict.COMPLETED, ShadowListenerVerdict.NO_FILES} else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
