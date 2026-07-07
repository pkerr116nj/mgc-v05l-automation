"""CLI for RA3 canonical trade decision attribution."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_trade_decision_attribution import (
    DEFAULT_CANONICAL_RECORDS_PATH,
    DEFAULT_ENRICHMENTS_PATH,
    DEFAULT_OUTCOMES_PATH,
    DEFAULT_OUTPUT_DIR,
    run_trade_decision_attribution,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build diagnostic trade decision attribution for completed CTOL/CTOE trades.")
    parser.add_argument("--outcomes-path", type=Path, default=DEFAULT_OUTCOMES_PATH)
    parser.add_argument("--enrichments-path", type=Path, default=DEFAULT_ENRICHMENTS_PATH)
    parser.add_argument("--canonical-records-path", type=Path, default=DEFAULT_CANONICAL_RECORDS_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_trade_decision_attribution(
        outcomes_path=args.outcomes_path,
        enrichments_path=args.enrichments_path,
        canonical_records_path=args.canonical_records_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    overall = result.summary.get("overall", {})
    print(
        json.dumps(
            {
                "schema_version": result.summary.get("schema_version"),
                "completed_trade_attributions": overall.get("completed_trade_attributions"),
                "entry_attribution_completeness": overall.get("entry_attribution_completeness"),
                "exit_attribution_completeness": overall.get("exit_attribution_completeness"),
                "managed_exit_involvement_count": overall.get("managed_exit_involvement_count"),
                "unknown_attribution_rate": overall.get("unknown_attribution_rate"),
                "summary_path": str(result.summary_json_path),
                "attribution_path": str(result.attribution_path),
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

