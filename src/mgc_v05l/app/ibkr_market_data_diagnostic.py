"""CLI for IBKR paper read-only market-data diagnostics."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..execution.ibkr_market_data_diagnostic import (
    IbkrMarketDataDiagnosticConfig,
    run_ibkr_market_data_diagnostic,
    write_ibkr_market_data_diagnostic_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_market_data_diagnostic"


class IbkrMarketDataDiagnosticCliError(RuntimeError):
    """Raised when diagnostic CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-market-data-diagnostic")
    parser.add_argument("--mode", default="PAPER", help="Required environment mode lock. Must be PAPER.")
    parser.add_argument("--host", default="127.0.0.1", help="Required environment host lock. Must be 127.0.0.1.")
    parser.add_argument("--port", type=int, default=7497, help="Required environment port lock. Must be 7497 for TWS paper.")
    parser.add_argument("--client-id", type=int, default=9072, help="Dedicated read-only diagnostic client id.")
    parser.add_argument("--account-id", default=None, help="Optional explicit account id if TWS exposes multiple managed accounts.")
    parser.add_argument("--read-only", action="store_true", help="Required for this diagnostic. Fails closed if omitted.")
    parser.add_argument("--timeout-seconds", type=float, default=12.0, help="Per-probe timeout in seconds.")
    parser.add_argument("--gc-expiry", default="202606", help="GC contract month in YYYYMM format.")
    parser.add_argument("--mgc-expiry", default="202606", help="MGC contract month in YYYYMM format.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--overwrite", action="store_true", help="Allow replacing a non-empty output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    config = IbkrMarketDataDiagnosticConfig(
        repo_root=REPO_ROOT,
        mode=str(args.mode or "").strip().upper(),
        host=str(args.host or "").strip(),
        port=int(args.port),
        client_id=int(args.client_id),
        read_only=bool(args.read_only),
        account_id=str(args.account_id or "").strip() or None,
        timeout_seconds=float(args.timeout_seconds),
        gc_expiry=str(args.gc_expiry or "").strip(),
        mgc_expiry=str(args.mgc_expiry or "").strip(),
    )
    artifacts = run_ibkr_market_data_diagnostic(config=config)
    write_ibkr_market_data_diagnostic_artifacts(output_dir=output_dir, artifacts=artifacts)
    print(json.dumps(artifacts.report, indent=2, sort_keys=True))
    return artifacts.exit_code


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrMarketDataDiagnosticCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
