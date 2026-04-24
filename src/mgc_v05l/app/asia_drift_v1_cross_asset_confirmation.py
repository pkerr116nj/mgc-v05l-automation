"""Runner for Asia Drift cross-asset confirmation research."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..research.asia_drift import run_cross_asset_confirmation


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_DB = REPO_ROOT / "mgc_v05l.replay.archive.sqlite3"
DEFAULT_STRUCTURAL_DIR = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_multi_year_discovery" / "full_20240101_20260421"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_cross_asset_confirmation"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-v1-cross-asset-confirmation")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="Replay SQLite database path.")
    parser.add_argument(
        "--multi-year-dir",
        default=str(DEFAULT_STRUCTURAL_DIR),
        help="Existing multi-year structural artifact directory to reuse when available.",
    )
    parser.add_argument("--output-dir", default=None)
    return parser


def run_asia_drift_v1_cross_asset_confirmation(
    *,
    source_db: Path,
    multi_year_dir: Path | None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_output_dir = (
        output_dir.resolve()
        if output_dir is not None
        else (DEFAULT_OUTPUT_ROOT / datetime.now(UTC).strftime("%Y%m%d_%H%M%S")).resolve()
    )
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    result = run_cross_asset_confirmation(
        output_dir=resolved_output_dir,
        multi_year_output_dir=multi_year_dir.resolve() if multi_year_dir is not None else None,
        source_sqlite_path=source_db.resolve(),
    )
    artifacts = result["artifacts"]
    return {
        "output_dir": str(resolved_output_dir),
        "summary_json_path": artifacts["summary_json_path"],
        "summary_markdown_path": artifacts["summary_markdown_path"],
        "session_labels_path": artifacts["session_labels_path"],
        "confirmation_feature_matrix_path": artifacts["confirmation_feature_matrix_path"],
        "tier_summary_json_path": artifacts["tier_summary_json_path"],
        "tier_summary_markdown_path": artifacts["tier_summary_markdown_path"],
        "baseline_comparison_json_path": artifacts["baseline_comparison_json_path"],
        "lead_lag_json_path": artifacts["lead_lag_json_path"],
        "storage_manifest_path": artifacts["storage_manifest_path"],
        "recommendation": result["payload"]["recommendation"],
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_asia_drift_v1_cross_asset_confirmation(
        source_db=Path(args.source_db),
        multi_year_dir=Path(args.multi_year_dir) if args.multi_year_dir else None,
        output_dir=Path(args.output_dir) if args.output_dir else None,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
