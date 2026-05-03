"""CLI wrapper for Track B no-submit end-to-end shadow replay."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .shadow_replay_runner import DEFAULT_SHADOW_REPLAY_RUNNER_OUTPUT_ROOT, ShadowReplayRunnerVerdict, run_shadow_replay


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Track B no-submit shadow replay chain from signal batch to attrition report.")
    parser.add_argument("--signal-batch-json", required=True, type=Path)
    parser.add_argument("--proposal-policy-json", required=True, type=Path)
    parser.add_argument("--manifest-json", required=True, type=Path)
    parser.add_argument("--registry-json", required=True, type=Path)
    parser.add_argument("--readiness-summary-json", type=Path)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_SHADOW_REPLAY_RUNNER_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_shadow_replay(
        signal_batch_payload=_read_json(args.signal_batch_json),
        proposal_policy_payload=_read_json(args.proposal_policy_json),
        manifest_payload=_read_json(args.manifest_json),
        registry_payload=_read_json(args.registry_json),
        readiness_summary_payload=_read_json(args.readiness_summary_json),
        readiness_summary_json=args.readiness_summary_json,
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "runner_verdict": result.report["runner_verdict"],
                "total_signals": result.report.get("total_signals"),
                "proposed_intents_created": result.report.get("proposed_intents_created"),
                "shadow_run_verdict": result.report.get("shadow_run_verdict"),
                "attrition_report_verdict": result.report.get("attrition_report_verdict"),
                "submit_allowed": result.report["submit_allowed"],
                "submit_attempted": result.report["submit_attempted"],
                "live_money_readiness": result.report["live_money_readiness"],
                "primary_blocker": result.report["primary_blocker"],
                "required_next_action": result.report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if result.verdict == ShadowReplayRunnerVerdict.COMPLETED_FOR_REVIEW else 2


def _read_json(path: Path | None) -> dict[str, object] | None:
    if path is None:
        return None
    return json.loads(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
