"""CLI for IBKR paper strategy governance and probation status refresh."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..execution.ibkr_paper_strategy_governance import (
    IbkrPaperStrategyGovernanceConfig,
    render_ibkr_paper_strategy_governance_markdown,
    run_ibkr_paper_strategy_governance,
    write_ibkr_paper_strategy_governance_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_strategy_governance"


class IbkrPaperStrategyGovernanceCliError(RuntimeError):
    """Raised when CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-paper-strategy-governance")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--freshness-window-seconds", type=float, default=120.0, help="Maximum age of governance status before bridge gating treats it as stale.")
    parser.add_argument("--daily-order-limit", type=int, default=2, help="Per-strategy daily order cap for future submit gating.")
    parser.add_argument("--weekly-order-limit", type=int, default=5, help="Per-strategy weekly order cap for future submit gating.")
    parser.add_argument("--drawdown-limit", type=float, default=2500.0, help="Per-strategy drawdown cap for governance gating.")
    parser.add_argument("--overwrite", action="store_true", help="Allow writing into a non-empty output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    config = IbkrPaperStrategyGovernanceConfig(
        repo_root=REPO_ROOT,
        output_dir=output_dir,
        freshness_window_seconds=float(args.freshness_window_seconds),
        daily_order_limit=int(args.daily_order_limit),
        weekly_order_limit=int(args.weekly_order_limit),
        drawdown_limit=float(args.drawdown_limit),
    )
    artifacts = run_ibkr_paper_strategy_governance(config=config)
    write_ibkr_paper_strategy_governance_artifacts(config=config, artifacts=artifacts)
    print(json.dumps(artifacts.status_payload, indent=2, sort_keys=True))
    print()
    print(render_ibkr_paper_strategy_governance_markdown(artifacts.report))
    return 0


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrPaperStrategyGovernanceCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
