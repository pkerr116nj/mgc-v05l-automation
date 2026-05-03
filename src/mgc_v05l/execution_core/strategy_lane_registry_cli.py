"""CLI wrapper for Track B no-submit strategy lane registry validation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .strategy_lane_registry import DEFAULT_LANE_REGISTRY_OUTPUT_ROOT, LaneValidationVerdict, validate_strategy_lane


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate a Track B strategy intent against a lane registry without submitting.")
    parser.add_argument("--intent-json", required=True, type=Path)
    parser.add_argument("--registry-json", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_LANE_REGISTRY_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    intent_payload = json.loads(args.intent_json.read_text(encoding="utf-8"))
    registry_payload = json.loads(args.registry_json.read_text(encoding="utf-8"))
    result = validate_strategy_lane(
        intent_payload=intent_payload,
        registry_payload=registry_payload,
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "lane_validation_verdict": result.report["lane_validation_verdict"],
                "lane_authorized_for_review": result.report["lane_authorized_for_review"],
                "lane_status": result.report["lane_status"],
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict in {LaneValidationVerdict.AUTHORIZED_FOR_SHADOW_REVIEW, LaneValidationVerdict.AUTHORIZED_FOR_PAPER_REVIEW} else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
