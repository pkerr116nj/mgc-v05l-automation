"""CLI wrapper for Track B no-submit candle signal producer."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .candle_signal_producer import DEFAULT_CANDLE_SIGNAL_PRODUCER_OUTPUT_ROOT, CandleSignalProducerVerdict, produce_candle_signal_batch
from .signal_batch_writer import DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Translate explicit candle/event JSON into Track B no-submit signal batch inbox work.")
    parser.add_argument("--candle-event-json", required=True, type=Path)
    parser.add_argument("--inbox-dir", required=True, type=Path)
    parser.add_argument("--expected-account-id")
    parser.add_argument("--source-id")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_CANDLE_SIGNAL_PRODUCER_OUTPUT_ROOT)
    parser.add_argument("--writer-output-root", type=Path, default=DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = json.loads(args.candle_event_json.read_text(encoding="utf-8"))
    result = produce_candle_signal_batch(
        candle_event_payload=payload,
        inbox_dir=args.inbox_dir,
        expected_account_id=args.expected_account_id,
        source_id=args.source_id,
        output_root=args.output_root,
        writer_output_root=args.writer_output_root,
    )
    print(
        json.dumps(
            {
                "producer_verdict": result.report["producer_verdict"],
                "source_id": result.report["source_id"],
                "batch_id": result.report["batch_id"],
                "signal_count": result.report["signal_count"],
                "output_batch_path": result.report["output_batch_path"],
                "writer_report_path": result.report["writer_report_path"],
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
    return 0 if result.verdict == CandleSignalProducerVerdict.PRODUCED_SIGNAL_BATCH else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
