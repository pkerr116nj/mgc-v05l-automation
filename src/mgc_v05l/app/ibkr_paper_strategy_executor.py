"""CLI for the supervised IBKR paper strategy executor."""

from __future__ import annotations

import argparse
import json
import signal
import threading
from pathlib import Path

from ..execution.ibkr_paper_strategy_executor import (
    IbkrPaperStrategyExecutorConfig,
    IbkrPaperStrategyExecutorLoopConfig,
    render_ibkr_paper_strategy_executor_markdown,
    render_ibkr_paper_strategy_executor_loop_markdown,
    run_ibkr_paper_strategy_executor,
    run_ibkr_paper_strategy_executor_loop,
    write_ibkr_paper_strategy_executor_artifacts,
    write_ibkr_paper_strategy_executor_loop_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_paper_strategy_executor"


class IbkrPaperStrategyExecutorCliError(RuntimeError):
    """Raised when CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-paper-strategy-executor")
    parser.add_argument("--mode", default="PAPER", help="Required environment mode lock. Must be PAPER.")
    parser.add_argument("--host", default="127.0.0.1", help="Required environment host lock. Must be 127.0.0.1.")
    parser.add_argument("--port", type=int, default=7497, help="Required environment port lock. Must be 7497.")
    parser.add_argument("--client-id", type=int, default=9301, help="Dedicated supervised paper strategy executor client id.")
    parser.add_argument("--account-id", default="DUM882026", help="Expected paper account id.")
    parser.add_argument("--strategy-id", default="ATP_COMPANION_V1_ASIA_US", help="First supervised strategy lane.")
    parser.add_argument("--symbol", default="MGC", help="Exact broker symbol. Must be MGC.")
    parser.add_argument("--contract-month", default="202606", help="Friendly contract month label.")
    parser.add_argument("--exact-expiry", default="20260626", help="Exact qualified contract expiry.")
    parser.add_argument("--con-id", type=int, default=712565978, help="Exact qualified contract conId.")
    parser.add_argument("--local-symbol", default="MGCM6", help="Exact qualified localSymbol.")
    parser.add_argument("--quantity", type=float, default=1.0, help="Maximum allowed supervised paper quantity.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--dashboard-freshness-seconds", type=float, default=120.0, help="Maximum allowed age for dashboard snapshot evidence.")
    parser.add_argument("--monitor-freshness-seconds", type=float, default=45.0, help="Maximum allowed age for live monitor runtime evidence.")
    parser.add_argument("--loop", action="store_true", help="Run the supervised executor repeatedly until stopped or until it exits the current long position.")
    parser.add_argument("--poll-interval-seconds", type=float, default=45.0, help="Polling interval for loop mode.")
    parser.add_argument("--max-cycles", type=int, default=0, help="Maximum loop cycles to run. Use 0 to run until stopped.")
    parser.add_argument("--stop-on-blocked", action="store_true", help="Stop loop mode immediately if a cycle returns a blocked classification.")
    parser.add_argument("--force-exit-long", action="store_true", help="Force a single supervised EXIT_LONG for the reconciled ATP-owned position.")
    parser.add_argument(
        "--allow-direct-reconciliation-close",
        action="store_true",
        help="Allow a one-off direct broker/ledger reconciliation fallback when the monitor runtime wrapper is stale or disconnected.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Allow writing into a non-empty output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    config = IbkrPaperStrategyExecutorConfig(
        repo_root=REPO_ROOT,
        mode=str(args.mode or "").strip().upper(),
        host=str(args.host or "").strip(),
        port=int(args.port),
        client_id=int(args.client_id),
        account_id=str(args.account_id or "").strip(),
        strategy_id=str(args.strategy_id or "").strip(),
        symbol=str(args.symbol or "").strip().upper(),
        contract_month=str(args.contract_month or "").strip(),
        exact_expiry=str(args.exact_expiry or "").strip(),
        con_id=int(args.con_id),
        local_symbol=str(args.local_symbol or "").strip().upper(),
        quantity=float(args.quantity),
        output_dir=output_dir,
        dashboard_freshness_seconds=float(args.dashboard_freshness_seconds),
        monitor_freshness_seconds=float(args.monitor_freshness_seconds),
        force_exit_long=bool(args.force_exit_long),
        allow_direct_reconciliation_close=bool(args.allow_direct_reconciliation_close),
    )
    if bool(args.loop):
        stop_event = threading.Event()

        def _request_stop(_signum: int, _frame: object) -> None:
            stop_event.set()

        previous_sigint = signal.getsignal(signal.SIGINT)
        previous_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGINT, _request_stop)
        signal.signal(signal.SIGTERM, _request_stop)
        artifacts = run_ibkr_paper_strategy_executor_loop(
            config=IbkrPaperStrategyExecutorLoopConfig(
                executor_config=config,
                poll_interval_seconds=float(args.poll_interval_seconds),
                max_cycles=int(args.max_cycles),
                stop_on_blocked=bool(args.stop_on_blocked),
            ),
            should_stop=stop_event.is_set,
        )
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)
        write_ibkr_paper_strategy_executor_loop_artifacts(
            config=IbkrPaperStrategyExecutorLoopConfig(
                executor_config=config,
                poll_interval_seconds=float(args.poll_interval_seconds),
                max_cycles=int(args.max_cycles),
                stop_on_blocked=bool(args.stop_on_blocked),
            ),
            artifacts=artifacts,
        )
        print(json.dumps(artifacts.runtime_status, indent=2, sort_keys=True))
        print()
        print(render_ibkr_paper_strategy_executor_loop_markdown(artifacts.report))
        return artifacts.exit_code
    artifacts = run_ibkr_paper_strategy_executor(config=config)
    write_ibkr_paper_strategy_executor_artifacts(config=config, artifacts=artifacts)
    print(json.dumps(artifacts.report, indent=2, sort_keys=True))
    print()
    print(render_ibkr_paper_strategy_executor_markdown(artifacts.report))
    return artifacts.exit_code


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrPaperStrategyExecutorCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
