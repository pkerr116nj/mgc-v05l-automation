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
    parser.add_argument("--submit", action="store_true", help="Request lane bridge delegation. Still requires control-plane authorization and snapshot validation.")
    parser.add_argument("--no-submit", action="store_true", help="Compatibility flag; forces diagnostic-only mode.")
    parser.add_argument(
        "--control-plane-authorized-submit",
        action="store_true",
        help="Explicitly acknowledge that submit delegation must pass Control Plane Snapshot validation before bridge construction.",
    )
    parser.add_argument(
        "--pre-action-snapshot-max-age-seconds",
        type=int,
        default=300,
        help="Maximum age for the Control Plane Snapshot required before lane bridge delegation.",
    )
    parser.add_argument("--strategy-id", type=str, default=None, help="Optional specific supported GC/MGC lane to port through the shared bridge.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    _ensure_output_dir(output_dir, overwrite=bool(args.overwrite))
    config = IbkrLaneSubmitPortConfig(
        repo_root=REPO_ROOT,
        output_dir=output_dir,
        submit=bool(args.submit) and not bool(args.no_submit),
        control_plane_authorized_submit=bool(args.control_plane_authorized_submit),
        pre_action_snapshot_max_age_seconds=int(args.pre_action_snapshot_max_age_seconds),
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
