"""CLI for paper-only IBKR strategy exposure attribution and gating."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..execution.ibkr_paper_strategy_exposure import (
    IbkrPaperStrategyExposureConfig,
    render_ibkr_paper_strategy_exposure_markdown,
    run_ibkr_paper_strategy_exposure,
    write_ibkr_paper_strategy_exposure_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "paper_strategy_exposure"


class IbkrPaperStrategyExposureCliError(RuntimeError):
    """Raised when CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-paper-strategy-exposure")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--strategy-id", type=str, default=None, help="Optional strategy lane to evaluate for an exposure gate decision.")
    parser.add_argument("--bridge-strategy-id", type=str, default=None, help="Optional bridge strategy identity for ownership matching.")
    parser.add_argument("--action", type=str, default=None, help="Optional action to evaluate: BUY / SELL / EXIT.")
    parser.add_argument("--quantity", type=float, default=1.0, help="Requested quantity for the optional strategy action.")
    parser.add_argument("--max-total-mgc-contracts", type=float, default=20.0, help="Aggregate executable-contract cap in MGC units.")
    parser.add_argument("--max-total-gc-equivalent", type=float, default=2.0, help="Aggregate gold exposure cap in GC-equivalent units.")
    parser.add_argument("--max-per-strategy-mgc-contracts", type=float, default=1.0, help="Per-strategy contract cap in MGC units.")
    parser.add_argument("--disallow-stacking", action="store_true", help="Disable stacking across strategies for evaluation.")
    parser.add_argument("--overwrite", action="store_true", help="Allow writing into a non-empty output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    config = IbkrPaperStrategyExposureConfig(
        repo_root=REPO_ROOT,
        output_dir=output_dir,
        strategy_id=args.strategy_id,
        bridge_strategy_id=args.bridge_strategy_id,
        action=args.action,
        quantity=float(args.quantity),
        allow_stacking=not bool(args.disallow_stacking),
        max_total_mgc_contracts=args.max_total_mgc_contracts,
        max_total_gc_equivalent=float(args.max_total_gc_equivalent),
        max_per_strategy_mgc_contracts=float(args.max_per_strategy_mgc_contracts),
    )
    artifacts = run_ibkr_paper_strategy_exposure(config=config)
    write_ibkr_paper_strategy_exposure_artifacts(config=config, artifacts=artifacts)
    print(json.dumps(artifacts.report, indent=2, sort_keys=True))
    print()
    print(render_ibkr_paper_strategy_exposure_markdown(artifacts.report))
    return 0


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrPaperStrategyExposureCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
