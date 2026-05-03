"""CLI wrapper for Track B no-submit signal-to-intent proposal policy."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .signal_intent_proposal import (
    DEFAULT_SIGNAL_INTENT_PROPOSAL_OUTPUT_ROOT,
    IntentProposalVerdict,
    SignalIntentProposalConfig,
    SignalIntentProposalPolicy,
    propose_intent_from_signal,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply a Track B no-submit signal-to-intent proposal policy.")
    parser.add_argument("--signal-json", required=True, type=Path)
    parser.add_argument("--policy-json", required=True, type=Path)
    parser.add_argument("--expected-account-id")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_SIGNAL_INTENT_PROPOSAL_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    signal_payload = json.loads(args.signal_json.read_text(encoding="utf-8"))
    policy_payload = json.loads(args.policy_json.read_text(encoding="utf-8"))
    result = propose_intent_from_signal(
        signal_payload=signal_payload,
        config=SignalIntentProposalConfig(
            expected_account_id=args.expected_account_id,
            policy=SignalIntentProposalPolicy.from_mapping(policy_payload),
            output_root=args.output_root,
        ),
    )
    print(
        json.dumps(
            {
                "intent_proposal_verdict": result.report["intent_proposal_verdict"],
                "intent_proposal_created": result.report["intent_proposal_created"],
                "signal_validation_verdict": result.report["signal_validation_verdict"],
                "decision_style": result.report["decision_style"],
                "scoring_passed_policy": result.report["scoring_passed_policy"],
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict == IntentProposalVerdict.CREATED_FOR_REVIEW else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
