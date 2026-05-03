"""CLI wrapper for the Track B no-submit shadow listener skeleton."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .shadow_listener import DEFAULT_SHADOW_LISTENER_OUTPUT_ROOT, ShadowListenerVerdict, run_shadow_listener_cycle


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Poll once for Track B shadow signal batch JSON files and run no-submit replay.")
    parser.add_argument("--listener-config-json", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config_payload = json.loads(args.listener_config_json.read_text(encoding="utf-8"))
    result = run_shadow_listener_cycle(
        config_payload=config_payload,
        output_root=args.output_root or Path(config_payload.get("output_root") or DEFAULT_SHADOW_LISTENER_OUTPUT_ROOT),
    )
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
