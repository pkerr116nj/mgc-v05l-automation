"""CLI for guarded Track B PAPER managed-exit cancel/replace."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mgc_v05l.app.ibkr_broker_truth_refresher import BrokerTruthRefreshConfig, run_broker_truth_refresh_once
from mgc_v05l.execution_core.track_b_managed_exit_cancel_replace import (
    ManagedExitCancelReplaceConfig,
    run_guarded_managed_exit_cancel_replace,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Guarded Track B PAPER cancel/replace for known managed exit orders.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--broker-order-id", required=True)
    parser.add_argument("--client-id", type=int, default=None)
    parser.add_argument("--perm-id", type=int, default=None)
    parser.add_argument("--mode", default="PAPER")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7497)
    parser.add_argument("--tws-client-id", type=int, default=10941)
    parser.add_argument("--account-id", default="DUM882026")
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--apply", action="store_true", help="Execute the exact guarded cancel/replace. Omit for readiness only.")
    parser.add_argument("--skip-broker-truth-refresh", action="store_true", help="Use existing read-only broker truth artifacts.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root)
    config = ManagedExitCancelReplaceConfig(
        repo_root=repo_root,
        broker_order_id=str(args.broker_order_id),
        client_id=args.client_id,
        perm_id=args.perm_id,
        mode=str(args.mode or "").strip().upper(),
        host=str(args.host or "").strip(),
        port=int(args.port),
        tws_client_id=int(args.tws_client_id),
        account_id=str(args.account_id or "").strip(),
        output_dir=args.output_dir if args.output_dir is not None else ManagedExitCancelReplaceConfig(repo_root=repo_root, broker_order_id=str(args.broker_order_id)).output_dir,
        apply=bool(args.apply),
    )

    def _refresh() -> dict:
        return run_broker_truth_refresh_once(
            config=BrokerTruthRefreshConfig(
                repo_root=repo_root,
                output_dir=repo_root / "outputs" / "reports" / "ibkr_read_only_verification",
                status_path=repo_root / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json",
                var_status_path=repo_root / "var" / "ibkr_broker_truth_refresh_status.json",
                mode=config.mode,
                host=config.host,
                port=config.port,
                account_id=config.account_id,
            )
        )

    report = run_guarded_managed_exit_cancel_replace(
        config=config,
        broker_truth_refresh=None if args.skip_broker_truth_refresh else _refresh,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("classification") in {
        "GUARDED_CANCEL_REPLACE_READY",
        "GUARDED_CANCEL_REPLACE_REPLACEMENT_SUBMITTED",
        "GUARDED_CANCEL_REPLACE_REPLACEMENT_FILLED",
        "GUARDED_CANCEL_REPLACE_REPLACEMENT_WORKING",
    } else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
