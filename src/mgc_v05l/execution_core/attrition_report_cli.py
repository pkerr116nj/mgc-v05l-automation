"""CLI wrapper for Track B no-submit attrition reports."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .attrition_report import DEFAULT_ATTRITION_REPORT_OUTPUT_ROOT, AttritionReportVerdict, create_attrition_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create a Track B no-submit attrition report from summary artifacts.")
    parser.add_argument("--signal-batch-summary-json", type=Path)
    parser.add_argument("--shadow-run-summary-json", type=Path)
    parser.add_argument("--readiness-summary-json", type=Path)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_ATTRITION_REPORT_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = create_attrition_report(
        signal_batch_summary=_read_json(args.signal_batch_summary_json),
        shadow_run_summary=_read_json(args.shadow_run_summary_json),
        readiness_summary=_read_json(args.readiness_summary_json),
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "attrition_report_verdict": result.report["attrition_report_verdict"],
                "primary_attrition_stage": result.report.get("primary_attrition_stage"),
                "missing_stages": result.report.get("missing_stages"),
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "live_money_readiness": result.report["live_money_readiness"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict in {AttritionReportVerdict.CREATED_FOR_REVIEW, AttritionReportVerdict.CREATED_WITH_MISSING_STAGES} else 2


def _read_json(path: Path | None) -> dict[str, object] | None:
    if path is None:
        return None
    return json.loads(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
