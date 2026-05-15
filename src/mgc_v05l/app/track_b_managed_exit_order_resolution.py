"""CLI for resolving disappeared known managed Track B PAPER exit orders."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_managed_exit_order_resolution import (
    ManagedExitOrderResolutionConfig,
    resolve_known_managed_exit_order_disappearance,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Resolve a disappeared known managed Track B PAPER exit order.")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--lifecycle-id", required=True)
    parser.add_argument("--broker-order-id", required=True)
    parser.add_argument("--client-id", type=int)
    parser.add_argument("--perm-id", type=int)
    parser.add_argument("--symbol")
    parser.add_argument("--local-symbol")
    parser.add_argument("--con-id", type=int)
    parser.add_argument("--apply", action="store_true", help="Persist the lifecycle close/cleanup if exact evidence is present.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = ManagedExitOrderResolutionConfig(
        repo_root=args.repo_root,
        lifecycle_id=args.lifecycle_id,
        broker_order_id=args.broker_order_id,
        client_id=args.client_id,
        perm_id=args.perm_id,
        symbol=args.symbol,
        local_symbol=args.local_symbol,
        con_id=args.con_id,
        apply=args.apply,
    )
    report = resolve_known_managed_exit_order_disappearance(config=config)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
