"""CLI for Track B trade outcome research enrichment."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_trade_outcome_enrichment import (
    DEFAULT_CRFD_ROWS,
    DEFAULT_GRE_REPORT,
    DEFAULT_MARKET_CONTEXT_ROWS,
    DEFAULT_OUTCOMES_PATH,
    DEFAULT_OUTPUT_DIR,
    run_trade_outcome_enrichment,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build diagnostic research enrichment rows for canonical Track B trade outcomes."
    )
    parser.add_argument("--outcomes-path", type=Path, default=DEFAULT_OUTCOMES_PATH)
    parser.add_argument("--crfd-rows-path", type=Path, default=DEFAULT_CRFD_ROWS)
    parser.add_argument("--gre-report-path", type=Path, default=DEFAULT_GRE_REPORT)
    parser.add_argument("--market-context-rows-path", type=Path, default=DEFAULT_MARKET_CONTEXT_ROWS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_trade_outcome_enrichment(
        outcomes_path=args.outcomes_path,
        crfd_rows_path=args.crfd_rows_path,
        gre_report_path=args.gre_report_path,
        market_context_rows_path=args.market_context_rows_path,
        output_dir=args.output_dir,
        now=args.now,
    )
    overall = result.summary.get("overall", {})
    print(
        json.dumps(
            {
                "schema_version": result.summary.get("schema_version"),
                "enrichment_count": overall.get("enrichment_count"),
                "gre_coverage": overall.get("gre_coverage"),
                "crfd_coverage": overall.get("crfd_coverage"),
                "vwap_coverage": overall.get("vwap_coverage"),
                "avwap_coverage": overall.get("avwap_coverage"),
                "vix_coverage": overall.get("vix_coverage"),
                "market_context_coverage": overall.get("market_context_coverage"),
                "enrichment_path": str(result.enrichment_path),
                "summary_path": str(result.summary_path),
                "diagnostic_only": result.summary.get("diagnostic_only"),
                "production_effect": result.summary.get("production_effect"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
