"""CLI for offline diagnostic GRE shadow gate observations."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT
from mgc_v05l.execution_core.track_b_gre_shadow_observation import run_gre_shadow_observation_generator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate offline diagnostic GRE shadow gate observations. No runtime hook or trading authority is invoked."
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    parser.add_argument("--gre-path", type=Path, help="Optional latest GRE output path.")
    parser.add_argument("--crfd-rows-path", type=Path, help="Optional CRFD rows path.")
    parser.add_argument("--candidate-path", action="append", type=Path, dest="candidate_paths", help="Optional candidate artifact path.")
    parser.add_argument("--max-candidates", type=int, default=200)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_gre_shadow_observation_generator(
        output_root=args.output_root,
        now=args.now,
        gre_path=args.gre_path,
        crfd_rows_path=args.crfd_rows_path,
        candidate_paths=args.candidate_paths,
        max_candidates=args.max_candidates,
    )
    print(
        json.dumps(
            {
                "schema_version": result.report.get("schema_version"),
                "observation_count": result.report.get("observation_count"),
                "candidate_count": result.report.get("candidate_count"),
                "result_counts": result.report.get("result_counts"),
                "rows_path": str(result.rows_path),
                "summary_json_path": str(result.summary_json_path),
                "summary_markdown_path": str(result.summary_markdown_path),
                "adapter_validation_path": str(result.adapter_validation_path),
                "adapter_migration_path": str(result.adapter_migration_path),
                "candidate_adapter": result.report.get("candidate_adapter", {}).get("adapter_id"),
                "diagnostic_only": result.report.get("diagnostic_only"),
                "production_effect": result.report.get("production_effect"),
                "runtime_hook": result.report.get("safety_contract", {}).get("runtime_hook"),
                "trading_gate": result.report.get("safety_contract", {}).get("trading_gate"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
