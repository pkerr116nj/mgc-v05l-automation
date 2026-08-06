"""CLI for historical research eligibility classification."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_research_eligibility import (
    DEFAULT_ANOMALY_ROOT_CAUSE_PATH,
    DEFAULT_CRR_PATH,
    DEFAULT_CRR_VALIDATION_PATH,
    DEFAULT_EXTREME_CLASSIFICATION_PATH,
    DEFAULT_OUTPUT_DIR,
    run_research_eligibility,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build diagnostic-only research eligibility artifacts.")
    parser.add_argument("--crr-path", type=Path, default=DEFAULT_CRR_PATH)
    parser.add_argument("--crr-validation-path", type=Path, default=DEFAULT_CRR_VALIDATION_PATH)
    parser.add_argument("--anomaly-root-cause-path", type=Path, default=DEFAULT_ANOMALY_ROOT_CAUSE_PATH)
    parser.add_argument("--extreme-classification-path", type=Path, default=DEFAULT_EXTREME_CLASSIFICATION_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_research_eligibility(
        crr_path=args.crr_path,
        crr_validation_path=args.crr_validation_path,
        anomaly_root_cause_path=args.anomaly_root_cause_path,
        extreme_classification_path=args.extreme_classification_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    payload = {
        "schema_version": result.summary.get("schema_version"),
        "status": result.validation.get("status"),
        "eligibility_records": len(result.records),
        "classification_counts": result.summary.get("classification_counts"),
        "views": {
            view_id: {
                "included_count": view.get("included_count"),
                "excluded_count": view.get("excluded_count"),
                "total_realized_pnl_proxy": view.get("metrics", {}).get("total_realized_pnl_proxy"),
            }
            for view_id, view in result.summary.get("views", {}).items()
        },
        "records_path": str(result.records_path),
        "summary_path": str(result.summary_path),
        "validation_path": str(result.validation_path),
        "diagnostic_only": result.summary.get("diagnostic_only"),
        "production_recommendation": result.summary.get("production_recommendation"),
        "trading_gate": result.summary.get("trading_gate"),
    }
    print(json.dumps(payload, sort_keys=True))
    return 1 if result.validation.get("status") == "INVALID" else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
