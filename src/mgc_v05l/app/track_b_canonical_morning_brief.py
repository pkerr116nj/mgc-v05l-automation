"""CLI for the Canonical Morning Brief aggregator."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_canonical_morning_brief import (
    DEFAULT_OUTPUT_DIR,
    run_canonical_morning_brief,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the canonical read-only Track B Morning Brief.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_canonical_morning_brief(output_dir=args.output_dir, now=args.now)
    brief = result.brief
    change_explanation = json.loads(result.change_explanation_path.read_text(encoding="utf-8"))
    print(
        json.dumps(
            {
                "schema_version": brief.get("schema_version"),
                "classification": brief.get("platform", {}).get("certification_classification"),
                "insight_count": brief.get("analytics", {}).get("latest_insight_count"),
                "manual_review_count": brief.get("research", {}).get("manual_review_count"),
                "market_context_status": brief.get("market_context", {}).get("current_market_context_status"),
                "json_path": str(result.json_path),
                "markdown_path": str(result.markdown_path),
                "archive_path": str(result.archive_path),
                "archive_summary_path": str(result.archive_summary_path),
                "diff_path": str(result.diff_path),
                "change_explanation_path": str(result.change_explanation_path),
                "change_explanation_classification": change_explanation.get("classification"),
                "change_explanation_count": change_explanation.get("change_count"),
                "diagnostic_only": brief.get("diagnostic_only"),
                "production_recommendation": brief.get("production_recommendation"),
                "trading_gate": brief.get("trading_gate"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
