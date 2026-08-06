"""CLI for Research Evidence Explorer v1."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_research_evidence_explorer import (
    DEFAULT_CRR_PATH,
    DEFAULT_CRR_VALIDATION_PATH,
    DEFAULT_INVESTIGATION_DOC_DIR,
    DEFAULT_INVESTIGATION_OUTPUT_DIR,
    DEFAULT_OUTPUT_DIR,
    run_research_investigations,
    run_research_evidence_explorer,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the read-only Research Evidence Explorer v1 artifacts.")
    parser.add_argument("--mode", choices=("explorer", "investigations", "all"), default="explorer")
    parser.add_argument("--crr-path", type=Path, default=DEFAULT_CRR_PATH)
    parser.add_argument("--crr-validation-path", type=Path, default=DEFAULT_CRR_VALIDATION_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--investigation-output-dir", type=Path, default=DEFAULT_INVESTIGATION_OUTPUT_DIR)
    parser.add_argument("--investigation-docs-dir", type=Path, default=DEFAULT_INVESTIGATION_DOC_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = {"mode": args.mode}
    exit_code = 0
    if args.mode in {"explorer", "all"}:
        result = run_research_evidence_explorer(
            crr_path=args.crr_path,
            crr_validation_path=args.crr_validation_path,
            output_dir=args.output_dir,
            now=args.now,
        )
        payload["explorer"] = {
            "schema_version": result.analysis.get("schema_version"),
            "validation_status": result.validation.get("status"),
            "population_count": result.population.get("included_count"),
            "excluded_count": result.population.get("excluded_count"),
            "top_decile_count": result.analysis.get("cohorts", {}).get("top_decile", {}).get("trade_count"),
            "bottom_decile_count": result.analysis.get("cohorts", {}).get("bottom_decile", {}).get("trade_count"),
            "ra8_exact_count": result.population.get("coverage", {}).get("ra8_exact_count"),
            "ra8_missing_count": result.population.get("coverage", {}).get("ra8_missing_count"),
            "analysis_path": str(result.analysis_path),
            "presentation_path": str(result.presentation_path),
            "diagnostic_only": result.analysis.get("diagnostic_only"),
            "production_recommendation": result.analysis.get("production_recommendation"),
            "trading_gate": result.analysis.get("trading_gate"),
        }
        if result.validation.get("status") == "INVALID":
            exit_code = 1
    if args.mode in {"investigations", "all"}:
        investigation_result = run_research_investigations(
            crr_path=args.crr_path,
            crr_validation_path=args.crr_validation_path,
            explorer_output_dir=args.output_dir,
            output_dir=args.investigation_output_dir,
            docs_dir=args.investigation_docs_dir,
            now=args.now,
        )
        payload["investigations"] = {
            "investigation_count": len(investigation_result.investigations),
            "index_path": str(investigation_result.index_json_path),
            "index_html_path": str(investigation_result.index_html_path),
            "investigation_ids": sorted(investigation_result.investigations),
            "diagnostic_only": True,
            "production_recommendation": False,
            "trading_gate": False,
        }
    print(json.dumps(payload, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
