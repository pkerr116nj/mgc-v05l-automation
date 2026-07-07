"""CLI for RA4 exit policy counterfactual design."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_exit_policy_counterfactual_design import (
    DEFAULT_ATTRIBUTIONS_PATH,
    DEFAULT_ENRICHMENTS_PATH,
    DEFAULT_OUTCOMES_PATH,
    DEFAULT_OUTPUT_DIR,
    run_exit_policy_counterfactual_design,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build diagnostic exit-policy counterfactual design reports from CTOL/CTOE/RA3 artifacts.")
    parser.add_argument("--outcomes-path", type=Path, default=DEFAULT_OUTCOMES_PATH)
    parser.add_argument("--enrichments-path", type=Path, default=DEFAULT_ENRICHMENTS_PATH)
    parser.add_argument("--attributions-path", type=Path, default=DEFAULT_ATTRIBUTIONS_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_exit_policy_counterfactual_design(
        outcomes_path=args.outcomes_path,
        enrichments_path=args.enrichments_path,
        attributions_path=args.attributions_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    overall = result.summary.get("overall", {})
    counter = result.summary.get("timebox_counterfactuals", {})
    print(
        json.dumps(
            {
                "schema_version": result.summary.get("schema_version"),
                "completed_trades": overall.get("completed_trades"),
                "timebox_exit_trades": overall.get("timebox_exit_trades"),
                "profitable_at_timeout_count": overall.get("profitable_at_timeout_count"),
                "mfe_coverage": overall.get("mfe_coverage"),
                "mae_coverage": overall.get("mae_coverage"),
                "shorter_timebox_testable_now": counter.get("shorter_timebox_testable_now"),
                "longer_timebox_testable_now": counter.get("longer_timebox_testable_now"),
                "primary_blocker": counter.get("primary_blocker"),
                "summary_path": str(result.summary_json_path),
                "observations_path": str(result.observations_path),
                "diagnostic_only": result.summary.get("diagnostic_only"),
                "production_recommendation": result.summary.get("production_recommendation"),
                "trading_gate": result.summary.get("trading_gate"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
