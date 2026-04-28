"""CLI for the unattended IBKR paper close/flatten test."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..execution.ibkr_unattended_paper_close import (
    IbkrUnattendedPaperCloseConfig,
    run_ibkr_unattended_paper_close,
    write_ibkr_unattended_paper_close_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_unattended_paper_close_test"


class IbkrUnattendedPaperCloseCliError(RuntimeError):
    """Raised when CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-unattended-paper-close")
    parser.add_argument("--mode", default="PAPER", help="Required environment mode lock. Must be PAPER.")
    parser.add_argument("--host", default="127.0.0.1", help="Required environment host lock. Must be 127.0.0.1.")
    parser.add_argument("--port", type=int, default=7497, help="Required environment port lock. Must be 7497 for TWS paper.")
    parser.add_argument("--client-id", type=int, default=9191, help="Dedicated unattended paper close client id.")
    parser.add_argument("--account-id", default="DUM882026", help="Expected paper account id.")
    parser.add_argument("--unattended-paper", action="store_true", help="Required explicit gate for unattended paper mutation testing.")
    parser.add_argument("--timeout-seconds", type=float, default=15.0, help="Per-request callback timeout.")
    parser.add_argument("--observation-seconds", type=float, default=8.0, help="How long to wait for broker-truth fill/cancel verification after submit.")
    parser.add_argument("--limit-offset-ticks", type=float, default=1.0, help="Tick offset below delayed bid/last for the marketable SELL limit.")
    parser.add_argument("--delayed-quote-max-age-seconds", type=float, default=30.0, help="Maximum age of the delayed quote snapshot.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--overwrite", action="store_true", help="Allow writing into a non-empty output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    config = IbkrUnattendedPaperCloseConfig(
        repo_root=REPO_ROOT,
        mode=str(args.mode or "").strip().upper(),
        host=str(args.host or "").strip(),
        port=int(args.port),
        client_id=int(args.client_id),
        unattended_paper=bool(args.unattended_paper),
        account_id=str(args.account_id or "").strip() or None,
        timeout_seconds=float(args.timeout_seconds),
        observation_seconds=float(args.observation_seconds),
        limit_offset_ticks=float(args.limit_offset_ticks),
        delayed_quote_max_age_seconds=float(args.delayed_quote_max_age_seconds),
    )
    artifacts = run_ibkr_unattended_paper_close(config=config)
    write_ibkr_unattended_paper_close_artifacts(output_dir=output_dir, artifacts=artifacts)
    print(json.dumps(artifacts.report, indent=2, sort_keys=True))
    return artifacts.exit_code


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrUnattendedPaperCloseCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
