"""CLI for Canonical Research Record v1."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_canonical_research_record import (
    DEFAULT_ATTRIBUTIONS_PATH,
    DEFAULT_CANONICAL_RECORDS_PATH,
    DEFAULT_ENRICHMENTS_PATH,
    DEFAULT_FINALIZED_CAPTURES_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_OUTCOMES_PATH,
    DEFAULT_TRADE_PATHS_PATH,
    evaluate_canonical_research_record,
    run_canonical_research_record,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Canonical Research Record v1 research artifacts.")
    parser.add_argument("--mode", choices=("generate", "validate", "readiness"), default="generate")
    parser.add_argument("--canonical-records-path", type=Path, default=DEFAULT_CANONICAL_RECORDS_PATH)
    parser.add_argument("--outcomes-path", type=Path, default=DEFAULT_OUTCOMES_PATH)
    parser.add_argument("--enrichments-path", type=Path, default=DEFAULT_ENRICHMENTS_PATH)
    parser.add_argument("--trade-paths-path", type=Path, default=DEFAULT_TRADE_PATHS_PATH)
    parser.add_argument("--attributions-path", type=Path, default=DEFAULT_ATTRIBUTIONS_PATH)
    parser.add_argument("--finalized-captures-path", type=Path, default=DEFAULT_FINALIZED_CAPTURES_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.mode == "generate":
        result = run_canonical_research_record(
            canonical_records_path=args.canonical_records_path,
            outcomes_path=args.outcomes_path,
            enrichments_path=args.enrichments_path,
            trade_paths_path=args.trade_paths_path,
            attributions_path=args.attributions_path,
            finalized_captures_path=args.finalized_captures_path,
            output_dir=args.output_dir,
            now=args.now,
        )
        summary = result.summary
        validation = result.validation
        records_path = str(result.records_path)
        validation_report = str(result.validation_json_path)
    else:
        evaluation = evaluate_canonical_research_record(
            canonical_records_path=args.canonical_records_path,
            outcomes_path=args.outcomes_path,
            enrichments_path=args.enrichments_path,
            trade_paths_path=args.trade_paths_path,
            attributions_path=args.attributions_path,
            finalized_captures_path=args.finalized_captures_path,
            now=args.now,
        )
        summary = evaluation.summary
        validation = evaluation.validation
        records_path = None
        validation_report = None
    overall = summary.get("overall", {})
    payload = {
        "mode": args.mode,
        "schema_version": summary.get("schema_version"),
        "canonical_research_record_count": overall.get("canonical_research_record_count"),
        "validation_status": overall.get("validation_status"),
        "exact_ctol_count": overall.get("exact_ctol_count"),
        "exact_ctoe_count": overall.get("exact_ctoe_count"),
        "exact_ra7_count": overall.get("exact_ra7_count"),
        "exact_ra3_count": overall.get("exact_ra3_count"),
        "exact_ra8_count": overall.get("exact_ra8_count"),
        "upstream_readiness": validation.get("upstream_readiness", []),
        "refresh_guidance": validation.get("refresh_guidance", []),
        "records_path": records_path,
        "validation_report": validation_report,
        "diagnostic_only": summary.get("diagnostic_only"),
        "production_recommendation": summary.get("production_recommendation"),
        "trading_gate": summary.get("trading_gate"),
    }
    print(json.dumps(payload, sort_keys=True))
    return 1 if validation.get("status") not in {"VALID", "VALID_WITH_WARNINGS"} else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
