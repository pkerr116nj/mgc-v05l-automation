"""CLI wrapper for Track B no-submit readiness summary."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .readiness_summary import DEFAULT_READINESS_SUMMARY_OUTPUT_ROOT, ReadinessSummaryConfig, run_readiness_summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize Track B readiness reports without submitting.")
    parser.add_argument("--recovery-report-json", required=True, type=Path)
    parser.add_argument("--preflight-report-json", required=True, type=Path)
    parser.add_argument("--quote-report-json", type=Path)
    parser.add_argument("--proof-timing-status", required=True)
    parser.add_argument("--proof-timing-source", default="operator_summary")
    parser.add_argument("--proof-timing-detail")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_READINESS_SUMMARY_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_readiness_summary(
        config=ReadinessSummaryConfig(
            recovery_report_json=args.recovery_report_json,
            preflight_report_json=args.preflight_report_json,
            quote_report_json=args.quote_report_json,
            proof_timing_status=args.proof_timing_status,
            proof_timing_source=args.proof_timing_source,
            proof_timing_detail=args.proof_timing_detail,
            output_root=args.output_root,
        )
    )
    print(
        json.dumps(
            {
                "final_readiness_verdict": result.report["final_readiness_verdict"],
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "primary_blocker": result.report["primary_blocker"],
                "secondary_blockers": result.report["secondary_blockers"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.report["submit_allowed"] else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
