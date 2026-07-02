"""CLI for Canonical Market Context diagnostics."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from mgc_v05l.execution_core.track_b_canonical_market_context import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_WAREHOUSE_ROOT,
    run_canonical_market_context,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build read-only Canonical Market Context artifacts.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--warehouse-root", type=Path, default=DEFAULT_WAREHOUSE_ROOT)
    parser.add_argument("--vix-source-path", type=Path, default=None)
    parser.add_argument("--now", help="Optional ISO timestamp for deterministic report generation.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_canonical_market_context(
        output_dir=args.output_dir,
        warehouse_root=args.warehouse_root,
        vix_source_path=args.vix_source_path,
        now=args.now,
    )
    vix = result.summary.get("vix") or {}
    print(
        json.dumps(
            {
                "schema_version": result.summary.get("schema_version"),
                "provider_count": result.summary.get("provider_count"),
                "row_count": result.summary.get("row_count"),
                "vix_available": vix.get("available"),
                "vix_row_count": vix.get("row_count"),
                "vix_join_readiness": vix.get("join_readiness"),
                "context_path": str(result.context_path),
                "summary_path": str(result.summary_path),
                "diagnostic_only": result.summary.get("diagnostic_only"),
                "production_effect": result.summary.get("production_effect"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

