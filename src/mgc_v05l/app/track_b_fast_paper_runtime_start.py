"""CLI for the broker-centered Track B PAPER fast-start contract."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_fast_paper_runtime_start import (
    DEFAULT_MANIFEST_PATH,
    run_fast_paper_runtime_start,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Start or recover Track B PAPER runtime through the fast broker-centered contract.")
    parser.add_argument(
        "command",
        choices=["validate-manifest", "preflight", "start", "recover", "status", "simulate-validation"],
    )
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--skip-broker-refresh", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic tests.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_fast_paper_runtime_start(
        command=args.command,
        repo_root=args.repo_root,
        manifest_path=args.manifest_path,
        skip_broker_refresh=args.skip_broker_refresh,
        dry_run=args.dry_run,
        now=args.now,
    )
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("ok") is True else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
