"""CLI wrapper for Track B no-submit order plan creation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .order_plan import DEFAULT_ORDER_PLAN_OUTPUT_ROOT, OrderPlanConfig, OrderPlanVerdict, create_order_plan_from_intent


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create a Track B order plan from a strategy intent without submitting.")
    parser.add_argument("--intent-json", required=True, type=Path)
    parser.add_argument("--expected-account-id")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_ORDER_PLAN_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = json.loads(args.intent_json.read_text(encoding="utf-8"))
    result = create_order_plan_from_intent(
        payload=payload,
        config=OrderPlanConfig(
            expected_account_id=args.expected_account_id,
            output_root=args.output_root,
        ),
    )
    print(
        json.dumps(
            {
                "order_plan_verdict": result.report["order_plan_verdict"],
                "order_plan_created": result.report["order_plan_created"],
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict == OrderPlanVerdict.CREATED_FOR_PAPER_REVIEW else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
