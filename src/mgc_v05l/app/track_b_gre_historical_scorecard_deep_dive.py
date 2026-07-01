"""CLI for diagnostic-only GRE historical scorecard deep-dive reports."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT
from mgc_v05l.execution_core.track_b_gre_historical_scorecard_deep_dive import run_gre_historical_scorecard_deep_dive


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate diagnostic-only GRE historical scorecard deep-dive reports. No trading authority is invoked."
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    parser.add_argument("--rows-path", type=Path, help="Optional GRE historical validation rows path.")
    parser.add_argument("--crfd-rows-path", type=Path, help="Optional CRFD rows path.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_gre_historical_scorecard_deep_dive(
        output_root=args.output_root,
        now=args.now,
        rows_path=args.rows_path,
        crfd_rows_path=args.crfd_rows_path,
    )
    print(
        json.dumps(
            {
                "schema_version": result.report.get("schema_version"),
                "observation_count": result.report.get("overall", {}).get("observation_count"),
                "validated_count": result.report.get("overall", {}).get("validated_count"),
                "crfd_joined_count": result.report.get("overall", {}).get("crfd_joined_count"),
                "ready_for_shadow_gate_simulation": result.report.get("gate_research_recommendations", {}).get(
                    "ready_for_shadow_gate_simulation"
                ),
                "next_feature_provider": result.report.get("gate_research_recommendations", {}).get("next_feature_provider"),
                "json_path": str(result.json_path),
                "markdown_path": str(result.markdown_path),
                "diagnostic_only": result.report.get("diagnostic_only"),
                "broker_authority": result.report.get("broker_authority"),
                "runtime_authority": result.report.get("runtime_authority"),
                "strategy_authority": result.report.get("strategy_authority"),
                "managed_exit_authority": result.report.get("managed_exit_authority"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
