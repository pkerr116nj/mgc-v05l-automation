"""CLI for porting the first non-ATP GC/MGC lane into submit-capable IBKR paper routing."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ..execution.ibkr_lane_submit_port import (
    IbkrLaneSubmitPortConfig,
    render_ibkr_lane_submit_port_markdown,
    run_ibkr_lane_submit_port,
    write_ibkr_lane_submit_port_artifacts,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "ibkr_lane_submit_port"


class IbkrLaneSubmitPortCliError(RuntimeError):
    """Raised when CLI inputs are invalid."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ibkr-lane-submit-port")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Artifact output directory.")
    parser.add_argument("--overwrite", action="store_true", help="Allow writing into a non-empty output directory.")
    parser.add_argument("--no-submit", action="store_true", help="Disable live bridge submit even if the selected lane emits an actionable intent.")
    parser.add_argument("--strategy-id", type=str, default=None, help="Optional specific supported GC/MGC lane to port through the shared bridge.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    config = IbkrLaneSubmitPortConfig(
        repo_root=REPO_ROOT,
        output_dir=output_dir,
        submit=not bool(args.no_submit),
        strategy_id=args.strategy_id,
    )
    artifacts = run_ibkr_lane_submit_port(config=config)
    write_ibkr_lane_submit_port_artifacts(config=config, artifacts=artifacts)
    print(json.dumps(artifacts.report, indent=2, sort_keys=True))
    print()
    print(render_ibkr_lane_submit_port_markdown(artifacts.report))
    return 0


def _ensure_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise IbkrLaneSubmitPortCliError(
            f"Output directory {output_dir} already exists and is not empty. Re-run with --overwrite to replace artifacts."
        )
    output_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
