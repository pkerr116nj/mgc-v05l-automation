"""Runner for the U.S. open NDX signal extraction pass."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from ..research.us_open_follow_through.probabilistic_pass4 import run_probabilistic_pass4


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PASS1_ROOT = REPO_ROOT / "outputs" / "reports" / "us_open_probabilistic_pass1" / "index_universe_20200101_20260421_v2"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "us_open_probabilistic_pass4" / "index_universe_20200101_20260421_v1"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="us-open-probabilistic-pass4")
    parser.add_argument("--pass1-root", default=str(DEFAULT_PASS1_ROOT), help="Pass 1 artifact root.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT), help="Pass 4 artifact output directory.")
    return parser


def run_us_open_probabilistic_pass4(*, pass1_root: Path, output_dir: Path) -> dict[str, Any]:
    result = run_probabilistic_pass4(pass1_root=pass1_root.resolve(), output_dir=output_dir.resolve())
    return {
        "output_dir": str(output_dir.resolve()),
        "signal_extraction_csv": result["artifacts"]["signal_extraction_csv"],
        "baseline_comparison_csv": result["artifacts"]["baseline_comparison_csv"],
        "distribution_csv": result["artifacts"]["distribution_csv"],
        "dev_holdout_csv": result["artifacts"]["dev_holdout_csv"],
        "summary_json": result["artifacts"]["summary_json"],
        "summary_markdown": result["artifacts"]["summary_markdown"],
        "storage_manifest": result["artifacts"]["storage_manifest"],
        "blunt_conclusion": result["summary"]["blunt_conclusion"],
        "row_counts": result["summary"]["row_counts"],
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_us_open_probabilistic_pass4(pass1_root=Path(args.pass1_root), output_dir=Path(args.output_dir))
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
