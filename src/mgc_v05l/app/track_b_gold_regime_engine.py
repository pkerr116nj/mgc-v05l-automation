"""CLI for the diagnostic-only Gold Regime Engine."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_gold_regime_engine import DEFAULT_OUTPUT_ROOT, run_gold_regime_engine


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the diagnostic-only Gold Regime Engine and publish bounded research artifacts. "
            "No broker, runtime, strategy, or Managed Exit authority is invoked."
        )
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    parser.add_argument("--max-snapshot-bytes", type=int, help="Optional bounded snapshot cap for tests/diagnostics.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = run_gold_regime_engine(
        output_root=args.output_root,
        now=args.now,
        max_snapshot_bytes=args.max_snapshot_bytes,
    )
    print(
        json.dumps(
            {
                "schema_version": report.get("schema_version"),
                "plugin_id": report.get("plugin_id"),
                "instrument": report.get("instrument"),
                "regime_label": report.get("regime_label"),
                "confidence": report.get("confidence"),
                "diagnostic_only": report.get("diagnostic_only"),
                "json_path": report.get("artifact_paths", {}).get("json"),
                "markdown_path": report.get("artifact_paths", {}).get("markdown"),
                "bounded_snapshot": report.get("bounded_snapshot"),
                "broker_state_mutated": report.get("broker_state_mutated"),
                "order_intent_created": report.get("order_intent_created"),
                "submit_allowed": report.get("submit_allowed"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
