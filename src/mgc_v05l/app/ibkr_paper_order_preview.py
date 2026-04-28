"""CLI for the manual-only IBKR paper-order preview harness."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..execution.ibkr_paper_order_preview import (
    IbkrPaperOrderPreviewArtifacts,
    IbkrPaperOrderPreviewConfig,
    run_ibkr_paper_order_preview,
    write_ibkr_paper_order_preview_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_paper_order_preview"


class IbkrPaperOrderPreviewCliError(RuntimeError):
    """Raised when preview CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-paper-order-preview")
    parser.add_argument("--mode", default="PAPER", help="Required environment mode lock. Must be PAPER.")
    parser.add_argument("--host", default="127.0.0.1", help="Required environment host lock. Must be 127.0.0.1.")
    parser.add_argument("--port", type=int, default=7497, help="Required environment port lock. Must be 7497 for TWS paper.")
    parser.add_argument("--client-id", type=int, default=9073, help="Dedicated preview-only client id.")
    parser.add_argument("--account-id", default=None, help="Optional expected account id. Preview fails closed if it does not match.")
    parser.add_argument("--symbol", default="MGC", help="Whitelisted symbol. Only GC or MGC are allowed.")
    parser.add_argument("--expiry", default="202606", help="Whitelisted expiry. Initial preview allows only 202606.")
    parser.add_argument("--action", default="BUY", help="Hypothetical order action. Only BUY or SELL are allowed.")
    parser.add_argument("--quantity", type=float, default=1.0, help="Hypothetical quantity. Must be <= 1.")
    parser.add_argument("--order-type", default="LMT", help="Hypothetical order type. Only LMT is allowed.")
    parser.add_argument("--limit-price", type=float, required=True, help="Hypothetical limit price. Preview blocks if omitted.")
    parser.add_argument("--time-in-force", default="DAY", help="Hypothetical time in force. Only DAY is allowed.")
    parser.add_argument("--timeout-seconds", type=float, default=12.0, help="Connection and quote timeout in seconds.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--overwrite", action="store_true", help="Allow replacing a non-empty output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    config = IbkrPaperOrderPreviewConfig(
        repo_root=REPO_ROOT,
        mode=str(args.mode or "").strip().upper(),
        host=str(args.host or "").strip(),
        port=int(args.port),
        client_id=int(args.client_id),
        symbol=str(args.symbol or "").strip().upper(),
        expiry=str(args.expiry or "").strip(),
        action=str(args.action or "").strip().upper(),
        quantity=float(args.quantity),
        order_type=str(args.order_type or "").strip().upper(),
        limit_price=float(args.limit_price),
        time_in_force=str(args.time_in_force or "").strip().upper(),
        account_id=str(args.account_id or "").strip() or None,
        timeout_seconds=float(args.timeout_seconds),
        caller_path="manual_cli",
    )
    artifacts = run_ibkr_paper_order_preview(config=config)
    write_ibkr_paper_order_preview_artifacts(output_dir=output_dir, artifacts=artifacts)
    print(json.dumps(artifacts.report, indent=2, sort_keys=True))
    return artifacts.exit_code


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrPaperOrderPreviewCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

