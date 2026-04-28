"""CLI for the IBKR paper execution-to-position reconciliation pass."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..execution.ibkr_position_reconciliation import (
    IbkrPositionReconciliationConfig,
    run_ibkr_position_reconciliation,
    write_ibkr_position_reconciliation_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_position_reconciliation"


class IbkrPositionReconciliationCliError(RuntimeError):
    """Raised when CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-position-reconciliation")
    parser.add_argument("--mode", default="PAPER", help="Required environment mode lock. Must be PAPER.")
    parser.add_argument("--host", default="127.0.0.1", help="Required environment host lock. Must be 127.0.0.1.")
    parser.add_argument("--port", type=int, default=7497, help="Required environment port lock. Must be 7497 for TWS paper.")
    parser.add_argument("--client-id", type=int, default=9161, help="Dedicated read-only reconciliation client id.")
    parser.add_argument("--account-id", default="DUM882026", help="Expected paper account id.")
    parser.add_argument("--read-only", action="store_true", help="Required for this pass. Fails closed if omitted.")
    parser.add_argument("--symbol", default="MGC", help="Contract symbol under reconciliation.")
    parser.add_argument("--contract-month", default="202606", help="Friendly contract month label used for prior fill context.")
    parser.add_argument("--exact-expiry", default="20260626", help="Exact IBKR futures expiry used for exact-contract reconciliation.")
    parser.add_argument("--con-id", type=int, default=712565978, help="Exact IBKR conId used for exact-contract reconciliation.")
    parser.add_argument("--local-symbol", default="MGCM6", help="Exact IBKR localSymbol used for exact-contract reconciliation.")
    parser.add_argument("--exchange", default="COMEX", help="Expected contract exchange.")
    parser.add_argument("--currency", default="USD", help="Expected contract currency.")
    parser.add_argument("--multiplier", default="10", help="Expected contract multiplier.")
    parser.add_argument("--timeout-seconds", type=float, default=15.0, help="Per-request callback timeout.")
    parser.add_argument("--observation-window-seconds", type=float, default=8.0, help="How long to keep resampling positions.")
    parser.add_argument("--sample-interval-seconds", type=float, default=1.0, help="Delay between position samples.")
    parser.add_argument("--recent-fill-lookback-minutes", type=int, default=120, help="Execution lookback window for the recent MGC fill.")
    parser.add_argument("--reference-fill-report-path", type=Path, default=None, help="Optional prior successful paper fill report used to explain the earlier 0.0 position snapshot.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--overwrite", action="store_true", help="Allow writing into a non-empty output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    config = IbkrPositionReconciliationConfig(
        repo_root=REPO_ROOT,
        mode=str(args.mode or "").strip().upper(),
        host=str(args.host or "").strip(),
        port=int(args.port),
        client_id=int(args.client_id),
        read_only=bool(args.read_only),
        account_id=str(args.account_id or "").strip() or None,
        symbol=str(args.symbol or "").strip().upper(),
        contract_month=str(args.contract_month or "").strip(),
        exact_expiry=str(args.exact_expiry or "").strip(),
        con_id=int(args.con_id),
        local_symbol=str(args.local_symbol or "").strip().upper(),
        exchange=str(args.exchange or "").strip().upper(),
        currency=str(args.currency or "").strip().upper(),
        multiplier=str(args.multiplier or "").strip(),
        timeout_seconds=float(args.timeout_seconds),
        observation_window_seconds=float(args.observation_window_seconds),
        sample_interval_seconds=float(args.sample_interval_seconds),
        recent_fill_lookback_minutes=int(args.recent_fill_lookback_minutes),
        reference_fill_report_path=Path(args.reference_fill_report_path) if args.reference_fill_report_path is not None else None,
    )
    artifacts = run_ibkr_position_reconciliation(config=config)
    write_ibkr_position_reconciliation_artifacts(output_dir=output_dir, artifacts=artifacts)
    print(json.dumps(artifacts.report, indent=2, sort_keys=True))
    return artifacts.exit_code


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrPositionReconciliationCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
