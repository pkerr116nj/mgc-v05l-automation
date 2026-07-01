"""CLI for diagnostic-only GRE historical backfill generation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT
from mgc_v05l.execution_core.track_b_gre_historical_backfill import run_gre_historical_backfill


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate diagnostic-only historical GRE observations from retained GC/MGC candles. "
            "No broker, runtime, strategy, or Managed Exit authority is invoked."
        )
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    parser.add_argument("--cadence-minutes", type=int, default=5)
    parser.add_argument("--max-observations", type=int, default=200)
    parser.add_argument("--provider", choices=("retained", "parquet"), default="retained")
    parser.add_argument("--research-store-root", type=Path, help="Historical research store root for parquet provider.")
    parser.add_argument("--max-source-candles", type=int, default=5000)
    parser.add_argument("--crfd-max-rows", type=int, help="Bounded CRFD row limit for provider-backed GRE context.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_gre_historical_backfill(
        output_root=args.output_root,
        now=args.now,
        cadence_minutes=args.cadence_minutes,
        max_observations=args.max_observations,
        provider=args.provider,
        research_store_root=args.research_store_root,
        max_source_candles=args.max_source_candles,
        crfd_max_rows=args.crfd_max_rows,
    )
    print(
        json.dumps(
            {
                "schema_version": result.summary.get("schema_version"),
                "provider_id": result.summary.get("provider_id"),
                "provider_kind": result.summary.get("provider_kind"),
                "observation_count": result.summary.get("observation_count"),
                "validated_observations": result.summary.get("validated_observations"),
                "coverage_assessment": result.summary.get("coverage_assessment"),
                "validation_percentages": result.summary.get("validation_percentages"),
                "scorecard_readiness": result.scorecard.get("readiness_assessment", {}).get("classification"),
                "analyzer_sample_status": result.analyzer.get("sample_assessment", {}).get("sample_status"),
                "rows_path": str(result.rows_path),
                "summary_path": str(result.summary_path),
                "historical_validation_summary_path": str(result.validation_summary_path),
                "scorecard_path": str(result.scorecard_path),
                "analyzer_path": str(result.analyzer_path),
                "comparison_path": str(result.comparison_path),
                "diagnostic_only": result.summary.get("diagnostic_only"),
                "broker_authority": result.summary.get("broker_authority"),
                "runtime_authority": result.summary.get("runtime_authority"),
                "strategy_authority": result.summary.get("strategy_authority"),
                "managed_exit_authority": result.summary.get("managed_exit_authority"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
