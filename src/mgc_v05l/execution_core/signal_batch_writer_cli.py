"""CLI wrapper for Track B no-submit signal batch inbox writer."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .signal_batch_writer import DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT, SignalBatchWriterVerdict, write_signal_batch_to_inbox


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write validated Track B no-submit signal batch JSON files to a shadow listener inbox.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--batch-json", type=Path)
    source.add_argument("--signal-json", action="append", type=Path)
    parser.add_argument("--inbox-dir", required=True, type=Path)
    parser.add_argument("--source-id")
    parser.add_argument("--batch-id")
    parser.add_argument("--shadow-run-id")
    parser.add_argument("--expected-account-id")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    batch_payload = json.loads(args.batch_json.read_text(encoding="utf-8")) if args.batch_json else None
    signal_payloads = [json.loads(path.read_text(encoding="utf-8")) for path in args.signal_json or ()]
    result = write_signal_batch_to_inbox(
        inbox_dir=args.inbox_dir,
        batch_payload=batch_payload,
        signal_payloads=signal_payloads,
        batch_id=args.batch_id,
        shadow_run_id=args.shadow_run_id,
        source_id=args.source_id,
        expected_account_id=args.expected_account_id,
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "signal_batch_writer_verdict": result.report["signal_batch_writer_verdict"],
                "batch_file_written": result.report["batch_file_written"],
                "batch_id": result.report["batch_id"],
                "total_signals": result.report["total_signals"],
                "batch_json_path": result.report["batch_json_path"],
                "listener_invoked": result.report["listener_invoked"],
                "runner_invoked": result.report["runner_invoked"],
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
    return 0 if result.verdict == SignalBatchWriterVerdict.WROTE_BATCH else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
