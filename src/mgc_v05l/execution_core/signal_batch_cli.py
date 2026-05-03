"""CLI wrapper for Track B no-submit signal batch processing."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .signal_batch import DEFAULT_SIGNAL_BATCH_OUTPUT_ROOT, SignalBatchVerdict, process_signal_batch


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Process a Track B no-submit batch of shadow signals into proposed intent artifacts.")
    parser.add_argument("--batch-json", required=True, type=Path)
    parser.add_argument("--policy-json", required=True, type=Path)
    parser.add_argument("--expected-account-id")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_SIGNAL_BATCH_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    batch_payload = json.loads(args.batch_json.read_text(encoding="utf-8"))
    policy_payload = json.loads(args.policy_json.read_text(encoding="utf-8"))
    result = process_signal_batch(
        batch_payload=batch_payload,
        policy_payload=policy_payload,
        expected_account_id=args.expected_account_id,
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "batch_validation_verdict": result.report["batch_validation_verdict"],
                "total_signals": result.report["total_signals"],
                "proposed_intents_created": result.report["proposed_intents_created"],
                "blocked_proposals": result.report["blocked_proposals"],
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict == SignalBatchVerdict.PROCESSED_FOR_REVIEW else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
