"""Runner for Asia Drift multi-year 5-minute structural discovery."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..research.asia_drift import (
    DEFAULT_MULTI_YEAR_INSTRUMENTS,
    run_asia_drift_multi_year_discovery,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SOURCE_DB = REPO_ROOT / "mgc_v05l.replay.archive.sqlite3"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "asia_drift_v1_multi_year_discovery"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="asia-drift-v1-multi-year-discovery")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="Replay SQLite database path.")
    parser.add_argument(
        "--instrument",
        action="append",
        dest="instruments",
        default=None,
        help="Instrument to include in the multi-year discovery pass. Defaults to MGC,GC,MES,ES,MNQ,NQ. May be supplied multiple times.",
    )
    parser.add_argument("--cluster-count", type=int, default=5, help="Requested small-K cluster count for structural discovery.")
    parser.add_argument("--output-dir", default=None)
    return parser


def run_asia_drift_v1_multi_year_discovery(
    *,
    source_db: Path,
    instruments: tuple[str, ...] = DEFAULT_MULTI_YEAR_INSTRUMENTS,
    cluster_count: int = 5,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_output_dir = (
        output_dir.resolve()
        if output_dir is not None
        else (DEFAULT_OUTPUT_ROOT / datetime.now(UTC).strftime("%Y%m%d_%H%M%S")).resolve()
    )
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    result = run_asia_drift_multi_year_discovery(
        source_sqlite_path=source_db.resolve(),
        output_dir=resolved_output_dir,
        instruments=instruments,
        cluster_count=cluster_count,
    )
    artifacts = result["artifacts"]
    return {
        "output_dir": str(resolved_output_dir),
        "summary_json_path": artifacts["summary_json_path"],
        "summary_markdown_path": artifacts["summary_markdown_path"],
        "feature_matrix_path": artifacts["feature_matrix_path"],
        "cluster_assignments_path": artifacts["cluster_assignments_path"],
        "cluster_summary_json_path": artifacts["cluster_summary_json_path"],
        "cluster_summary_markdown_path": artifacts["cluster_summary_markdown_path"],
        "early_feature_matrix_path": artifacts["early_feature_matrix_path"],
        "positive_control_json_path": artifacts["positive_control_json_path"],
        "positive_control_markdown_path": artifacts["positive_control_markdown_path"],
        "discriminator_csv_path": artifacts["discriminator_csv_path"],
        "candidate_condition_sets_json_path": artifacts["candidate_condition_sets_json_path"],
        "candidate_condition_sets_csv_path": artifacts["candidate_condition_sets_csv_path"],
        "template_comparison_json_path": artifacts["template_comparison_json_path"],
        "research_queue_path": artifacts["research_queue_path"],
        "session_manifest_path": artifacts["session_manifest_path"],
        "storage_manifest_path": artifacts["storage_manifest_path"],
        "recommendation": result["payload"]["recommendation"],
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run_asia_drift_v1_multi_year_discovery(
        source_db=Path(args.source_db),
        instruments=tuple(args.instruments or DEFAULT_MULTI_YEAR_INSTRUMENTS),
        cluster_count=args.cluster_count,
        output_dir=Path(args.output_dir) if args.output_dir else None,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
