"""CLI wrapper for Track B no-submit shadow evaluation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .shadow_evaluation import DEFAULT_SHADOW_EVALUATION_OUTPUT_ROOT, ShadowEvaluationConfig, ShadowEvaluationVerdict, run_shadow_evaluation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create a Track B no-submit shadow evaluation report.")
    parser.add_argument("--intent-json", required=True, type=Path)
    parser.add_argument("--expected-account-id")
    parser.add_argument("--readiness-summary-json", type=Path)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_SHADOW_EVALUATION_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = json.loads(args.intent_json.read_text(encoding="utf-8"))
    result = run_shadow_evaluation(
        payload=payload,
        config=ShadowEvaluationConfig(
            expected_account_id=args.expected_account_id,
            readiness_summary_json=args.readiness_summary_json,
            output_root=args.output_root,
        ),
    )
    print(
        json.dumps(
            {
                "shadow_evaluation_verdict": result.report["shadow_evaluation_verdict"],
                "intent_validation_verdict": result.report["intent_validation_verdict"],
                "order_plan_verdict": result.report["order_plan_verdict"],
                "readiness_verdict": result.report["readiness_verdict"],
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict == ShadowEvaluationVerdict.CREATED_FOR_REVIEW else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
