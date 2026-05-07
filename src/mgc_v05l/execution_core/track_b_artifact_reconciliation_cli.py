"""CLI for read-only Track B artifact reconciliation.

This command only reconciles compact artifact state against existing read-only
broker preflight/recovery reports. It must not query or mutate a broker.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from .track_b_paper_trade_ledger import (
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_JSONL,
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT,
    reconcile_manually_flattened_proof_lifecycle,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Archive a stale proof/canary Track B ledger lifecycle after read-only broker flat confirmation."
    )
    parser.add_argument("--lifecycle-id", required=True)
    parser.add_argument("--preflight-report-json", required=True, type=Path)
    parser.add_argument("--recovery-report-json", type=Path)
    parser.add_argument("--ledger-jsonl", type=Path, default=DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_JSONL)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT)
    parser.add_argument("--diagnostics-root", type=Path, default=Path("outputs/track_b_execution_core/diagnostics"))
    parser.add_argument("--expected-account-id")
    parser.add_argument("--expected-contract-key")
    parser.add_argument("--expected-local-symbol")
    parser.add_argument("--expected-con-id", type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = reconcile_manually_flattened_proof_lifecycle(
        lifecycle_id=args.lifecycle_id,
        preflight_report_json=args.preflight_report_json,
        recovery_report_json=args.recovery_report_json,
        ledger_jsonl=args.ledger_jsonl,
        output_root=args.output_root,
        diagnostics_root=args.diagnostics_root,
        expected_account_id=args.expected_account_id,
        expected_contract_key=args.expected_contract_key,
        expected_local_symbol=args.expected_local_symbol,
        expected_con_id=args.expected_con_id,
    )
    print(json.dumps(result.reconciliation_report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
