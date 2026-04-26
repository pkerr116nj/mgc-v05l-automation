"""Runner for the third controlled Asia Drift probabilistic evidence-stack pass."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ..research.asia_drift.probabilistic_pass3 import run_probabilistic_pass3


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PASS1_ROOT = REPO_ROOT / "outputs" / "reports" / "asia_drift_probabilistic_pass1" / "six_core_20200101_20260421_v6"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "asia_drift_probabilistic_pass3" / "six_core_20200101_20260421"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-probabilistic-pass3")
    parser.add_argument("--pass1-root", default=str(DEFAULT_PASS1_ROOT), help="Completed pass1 artifact root.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT), help="Output directory for pass3 artifacts.")
    return parser


def run_asia_drift_probabilistic_pass3(*, pass1_root: Path, output_dir: Path) -> dict[str, Any]:
    result = run_probabilistic_pass3(pass1_root=pass1_root.resolve(), output_dir=output_dir.resolve())
    return {
        "output_dir": str(output_dir.resolve()),
        "artifacts": result["artifacts"],
        "headline": result["summary"]["headline"],
        "row_counts": result["summary"]["row_counts"],
        "assumptions": result["summary"]["assumptions"],
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_asia_drift_probabilistic_pass3(
        pass1_root=Path(args.pass1_root),
        output_dir=Path(args.output_dir),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
