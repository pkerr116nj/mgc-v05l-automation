"""CLI for diagnostic-only GRE research analyzer."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT
from mgc_v05l.execution_core.track_b_gre_research_analyzer import run_gre_research_analyzer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze GRE validation and scorecard outputs for research next steps. "
            "No broker, runtime, strategy, or Managed Exit authority is invoked."
        )
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_gre_research_analyzer(output_root=args.output_root, now=args.now)
    analyzer = result.analyzer
    print(
        json.dumps(
            {
                "schema_version": analyzer.get("schema_version"),
                "sample_status": analyzer.get("sample_assessment", {}).get("sample_status"),
                "readiness_level": analyzer.get("readiness_level"),
                "observation_count": analyzer.get("observation_count"),
                "validated_count": analyzer.get("validated_count"),
                "recommended_next_feature_provider": analyzer.get("recommended_next_feature_provider"),
                "json_path": str(result.json_path),
                "markdown_path": str(result.markdown_path),
                "interface_json_path": str(result.interface_json_path),
                "interface_markdown_path": str(result.interface_markdown_path),
                "diagnostic_only": analyzer.get("diagnostic_only"),
                "broker_authority": analyzer.get("broker_authority"),
                "runtime_authority": analyzer.get("runtime_authority"),
                "strategy_authority": analyzer.get("strategy_authority"),
                "managed_exit_authority": analyzer.get("managed_exit_authority"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
