"""CLI for the prospective NQ cohort monitor."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_nq_prospective_cohort_monitor import (
    DEFAULT_CRR_PATH,
    DEFAULT_CRR_VALIDATION_PATH,
    DEFAULT_ELIGIBILITY_RECORDS_PATH,
    DEFAULT_INV_005_PEER_ASSIGNMENTS_PATH,
    DEFAULT_INV_005_PEER_METRICS_PATH,
    DEFAULT_INV_006_PERIOD_COMPARISON_PATH,
    DEFAULT_OUTPUT_DIR,
    run_nq_prospective_cohort_monitor,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build offline prospective NQ cohort monitor artifacts.")
    parser.add_argument("--crr-path", type=Path, default=DEFAULT_CRR_PATH)
    parser.add_argument("--crr-validation-path", type=Path, default=DEFAULT_CRR_VALIDATION_PATH)
    parser.add_argument("--eligibility-records-path", type=Path, default=DEFAULT_ELIGIBILITY_RECORDS_PATH)
    parser.add_argument("--inv-005-peer-assignments-path", type=Path, default=DEFAULT_INV_005_PEER_ASSIGNMENTS_PATH)
    parser.add_argument("--inv-005-peer-metrics-path", type=Path, default=DEFAULT_INV_005_PEER_METRICS_PATH)
    parser.add_argument("--inv-006-period-comparison-path", type=Path, default=DEFAULT_INV_006_PERIOD_COMPARISON_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_nq_prospective_cohort_monitor(
        crr_path=args.crr_path,
        crr_validation_path=args.crr_validation_path,
        eligibility_records_path=args.eligibility_records_path,
        inv_005_peer_assignments_path=args.inv_005_peer_assignments_path,
        inv_005_peer_metrics_path=args.inv_005_peer_metrics_path,
        inv_006_period_comparison_path=args.inv_006_period_comparison_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    payload = {
        "schema_version": result.discovery_baselines.get("schema_version"),
        "baseline_count": result.discovery_baselines.get("baseline_count"),
        "prospective_trade_count": result.prospective_population.get("included_count"),
        "validation_status": result.validation_report.get("status"),
        "checkpoint_count": len(result.checkpoint_history),
        "output_dir": str(args.output_dir),
        "html_path": str(result.output_paths["html"]),
        "diagnostic_only": result.discovery_baselines.get("guardrails", {}).get("diagnostic_only"),
        "production_recommendation": result.discovery_baselines.get("guardrails", {}).get("production_recommendation"),
        "trading_gate": result.discovery_baselines.get("guardrails", {}).get("trading_gate"),
        "live_money_eligible": result.discovery_baselines.get("guardrails", {}).get("live_money_eligible"),
    }
    print(json.dumps(payload, sort_keys=True))
    return 1 if result.validation_report.get("status") == "INVALID" else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
