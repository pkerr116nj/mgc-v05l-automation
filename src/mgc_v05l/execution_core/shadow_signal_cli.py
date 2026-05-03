"""CLI wrapper for Track B no-submit shadow signal validation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .shadow_signal import DEFAULT_SHADOW_SIGNAL_OUTPUT_ROOT, ShadowSignalValidationConfig, ShadowSignalValidationVerdict, validate_shadow_signal


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate a Track B no-submit shadow signal observation.")
    parser.add_argument("--signal-json", required=True, type=Path)
    parser.add_argument("--expected-account-id")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_SHADOW_SIGNAL_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = json.loads(args.signal_json.read_text(encoding="utf-8"))
    result = validate_shadow_signal(
        payload=payload,
        config=ShadowSignalValidationConfig(
            expected_account_id=args.expected_account_id,
            output_root=args.output_root,
        ),
    )
    print(
        json.dumps(
            {
                "shadow_signal_validation_verdict": result.report["shadow_signal_validation_verdict"],
                "signal_allowed_for_review": result.report["signal_allowed_for_review"],
                "decision_style": result.report["decision_style"],
                "scoring_present": result.report["scoring_present"],
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict == ShadowSignalValidationVerdict.VALID_FOR_REVIEW else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
