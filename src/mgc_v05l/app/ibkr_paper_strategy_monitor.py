"""CLI for the persistent IBKR paper strategy position and P&L monitor."""

from __future__ import annotations

import argparse
import json
import signal
import threading
from pathlib import Path

from ..execution.ibkr_paper_strategy_monitor import (
    DEFAULT_PAPER_STRATEGY_MONITOR_STARTUP_GRACE_SECONDS,
    IbkrPaperStrategyMonitorDaemonConfig,
    IbkrPaperStrategyMonitorConfig,
    paper_strategy_monitor_startup_validation_permanent_failure,
    paper_strategy_monitor_startup_validation_ready,
    render_ibkr_paper_strategy_monitor_markdown,
    render_ibkr_paper_strategy_monitor_daemon_markdown,
    run_ibkr_paper_strategy_monitor_daemon,
    run_ibkr_paper_strategy_monitor,
    write_ibkr_paper_strategy_monitor_daemon_artifacts,
    write_ibkr_paper_strategy_monitor_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "paper_strategy_monitor"


class IbkrPaperStrategyMonitorCliError(RuntimeError):
    """Raised when CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-paper-strategy-monitor")
    parser.add_argument("--mode", default="PAPER", help="Required environment mode lock. Must be PAPER.")
    parser.add_argument("--host", default="127.0.0.1", help="Required environment host lock. Must be 127.0.0.1.")
    parser.add_argument("--port", type=int, default=7497, help="Required environment port lock. Must be 7497.")
    parser.add_argument("--client-id", type=int, default=9245, help="Dedicated read-only paper strategy monitor client id.")
    parser.add_argument("--account-id", default="DUM882026", help="Expected paper account id.")
    parser.add_argument("--strategy-id", default="ATP_COMPANION_V1_ASIA_US", help="Expected strategy owner for adoption.")
    parser.add_argument("--symbol", default="MGC", help="Expected broker symbol. Must be MGC.")
    parser.add_argument("--contract-month", default="202606", help="Friendly contract month label.")
    parser.add_argument("--exact-expiry", default="20260626", help="Exact qualified contract expiry.")
    parser.add_argument("--con-id", type=int, default=712565978, help="Exact qualified contract conId.")
    parser.add_argument("--local-symbol", default="MGCM6", help="Exact qualified localSymbol.")
    parser.add_argument("--dashboard-url", default="http://127.0.0.1:8790/api/dashboard", help="Live operator dashboard API URL.")
    parser.add_argument("--ledger-path", type=Path, default=REPO_ROOT / "var" / "paper_strategy_position_ledger.json", help="Persistent strategy-position ledger path.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--recent-fill-lookback-minutes", type=int, default=240, help="Execution lookback window used for ownership adoption.")
    parser.add_argument("--daemon", action="store_true", help="Run the polling monitor loop instead of a single snapshot pass.")
    parser.add_argument("--poll-interval-seconds", type=float, default=10.0, help="Polling interval for daemon mode.")
    parser.add_argument("--max-cycles", type=int, default=3, help="Number of polling cycles to run in daemon mode.")
    parser.add_argument("--freshness-window-seconds", type=float, default=45.0, help="How fresh runtime monitor output must remain for bridge submit gating.")
    parser.add_argument(
        "--startup-grace-seconds",
        type=float,
        default=DEFAULT_PAPER_STRATEGY_MONITOR_STARTUP_GRACE_SECONDS,
        help="Bounded startup validation grace window used by wrappers before declaring daemon startup failed.",
    )
    parser.add_argument("--overwrite", action="store_true", help="Allow writing into a non-empty output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    config = IbkrPaperStrategyMonitorConfig(
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
        dashboard_url=str(args.dashboard_url or "").strip(),
        ledger_path=Path(args.ledger_path),
        output_dir=output_dir,
        recent_fill_lookback_minutes=int(args.recent_fill_lookback_minutes),
    )
    if bool(args.daemon):
        stop_event = threading.Event()

        def _request_stop(_signum: int, _frame: object) -> None:
            stop_event.set()

        previous_sigint = signal.getsignal(signal.SIGINT)
        previous_sigterm = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGINT, _request_stop)
        signal.signal(signal.SIGTERM, _request_stop)
        daemon_artifacts = run_ibkr_paper_strategy_monitor_daemon(
            config=IbkrPaperStrategyMonitorDaemonConfig(
                monitor_config=config,
                poll_interval_seconds=float(args.poll_interval_seconds),
                max_cycles=int(args.max_cycles),
                freshness_window_seconds=float(args.freshness_window_seconds),
            ),
            should_stop=stop_event.is_set,
        )
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)
        write_ibkr_paper_strategy_monitor_daemon_artifacts(
            config=IbkrPaperStrategyMonitorDaemonConfig(
                monitor_config=config,
                poll_interval_seconds=float(args.poll_interval_seconds),
                max_cycles=int(args.max_cycles),
                freshness_window_seconds=float(args.freshness_window_seconds),
            ),
            artifacts=daemon_artifacts,
        )
        print(json.dumps(daemon_artifacts.runtime_status, indent=2, sort_keys=True))
        print()
        print(render_ibkr_paper_strategy_monitor_daemon_markdown(daemon_artifacts.daemon_report))
        return daemon_artifacts.exit_code

    artifacts = run_ibkr_paper_strategy_monitor(config=config)
    write_ibkr_paper_strategy_monitor_artifacts(config=config, artifacts=artifacts)
    print(json.dumps(artifacts.status, indent=2, sort_keys=True))
    print()
    print(
        render_ibkr_paper_strategy_monitor_markdown(
            status=artifacts.status,
            ledger=artifacts.ledger,
            pnl_snapshot=artifacts.pnl_snapshot,
        )
    )
    return artifacts.exit_code


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrPaperStrategyMonitorCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
