"""CLI for live-paper strategy inventory and IBKR paper intent adapter porting."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..execution.ibkr_paper_strategy_porting import (
    IbkrPaperStrategyPortingConfig,
    render_ibkr_paper_strategy_porting_markdown,
    render_ibkr_strategy_intent_adapter_markdown,
    run_ibkr_paper_strategy_porting,
    write_ibkr_paper_strategy_porting_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_strategy_porting"


class IbkrPaperStrategyPortingCliError(RuntimeError):
    """Raised when CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-paper-strategy-porting")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--overwrite", action="store_true", help="Allow writing into a non-empty output directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    config = IbkrPaperStrategyPortingConfig(
        repo_root=REPO_ROOT,
        output_dir=output_dir,
    )
    artifacts = run_ibkr_paper_strategy_porting(config=config)
    write_ibkr_paper_strategy_porting_artifacts(config=config, artifacts=artifacts)
    print(json.dumps(artifacts.report, indent=2, sort_keys=True))
    print()
    print(render_ibkr_paper_strategy_porting_markdown(artifacts.report))
    print()
    print(render_ibkr_strategy_intent_adapter_markdown(artifacts.report))
    return 0


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrPaperStrategyPortingCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
