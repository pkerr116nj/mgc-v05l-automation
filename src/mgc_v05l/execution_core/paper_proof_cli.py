"""CLI wrapper for the Track B paper-proof submit harness."""

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Sequence

from .ibkr_readonly_transport import IbkrReadOnlyTransportConfig, IbkrReadOnlyTwsTransport
from .models import TerminalClassification
from .paper_proof import PaperProofConfig, PaperProofResult, ProofRunner, run_paper_proof
from .preflight import ReadOnlyPreflightConfig, run_read_only_preflight


PreflightRunnerFactory = Callable[[IbkrReadOnlyTransportConfig], object]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Track B paper-proof submit harness.")
    parser.add_argument("--mode", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--client-id", required=True, type=int)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--contract-key", required=True)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--side", default="BUY")
    parser.add_argument("--quantity", default=1, type=int)
    parser.add_argument("--order-type", default="LMT")
    parser.add_argument("--time-in-force", default="DAY")
    parser.add_argument("--submit-enabled", action="store_true")
    parser.add_argument("--confirm-paper-submit", action="store_true")
    parser.add_argument("--allow-delayed-data-paper-proof", action="store_true")
    parser.add_argument("--manual-limit-price")
    parser.add_argument("--request-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--quote-timeout-seconds", type=float, default=3.0)
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    transport_factory: PreflightRunnerFactory | None = None,
    proof_runner: ProofRunner | None = None,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _validate_operator_args(parser, args)

    config = PaperProofConfig(
        mode=args.mode,
        host=args.host,
        port=args.port,
        client_id=args.client_id,
        account_id=args.account_id,
        contract_key=args.contract_key,
        side=args.side,
        quantity=args.quantity,
        order_type=args.order_type,
        time_in_force=args.time_in_force,
        output_root=args.output_root,
        submit_enabled=args.submit_enabled,
        confirm_paper_submit=args.confirm_paper_submit,
        allow_delayed_data_for_paper_proof=args.allow_delayed_data_paper_proof,
        manual_limit_price=args.manual_limit_price,
    )
    transport_config = IbkrReadOnlyTransportConfig(
        request_timeout_seconds=args.request_timeout_seconds,
        quote_timeout_seconds=args.quote_timeout_seconds,
    )
    factory = transport_factory or (lambda cfg: IbkrReadOnlyTwsTransport(config=cfg))

    def preflight_runner(preflight_config: ReadOnlyPreflightConfig, run_id: str):
        return run_read_only_preflight(
            config=preflight_config,
            transport=factory(transport_config),  # type: ignore[arg-type]
            run_id=run_id,
        )

    result = run_paper_proof(config=config, preflight_runner=preflight_runner, proof_runner=proof_runner)
    _print_result(result)
    if result.classification == TerminalClassification.PASSED:
        return 0
    if result.classification == TerminalClassification.BLOCKED:
        return 2
    return 3


def _validate_operator_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if str(args.mode).upper() != "PAPER":
        parser.error("--mode must be PAPER")
    if str(args.host) != "127.0.0.1":
        parser.error("--host must be 127.0.0.1")
    if int(args.port) != 7497:
        parser.error("--port must be 7497 for TWS paper")
    if int(args.client_id) <= 0:
        parser.error("--client-id must be a positive integer")
    if not str(args.account_id or "").strip():
        parser.error("--account-id is required")
    if not str(args.contract_key or "").strip():
        parser.error("--contract-key is required")
    if not args.submit_enabled:
        parser.error("--submit-enabled is required")
    if not args.confirm_paper_submit:
        parser.error("--confirm-paper-submit is required")
    if int(args.quantity) != 1:
        parser.error("--quantity must be exactly 1")
    if str(args.order_type).upper() != "LMT":
        parser.error("--order-type must be LMT")
    if str(args.time_in_force).upper() != "DAY":
        parser.error("--time-in-force must be DAY")
    if args.manual_limit_price is not None:
        if not args.allow_delayed_data_paper_proof:
            parser.error("--manual-limit-price requires --allow-delayed-data-paper-proof")
        try:
            manual_price = Decimal(str(args.manual_limit_price))
        except (InvalidOperation, ValueError):
            parser.error("--manual-limit-price must be a positive decimal")
        if not manual_price.is_finite() or manual_price <= 0:
            parser.error("--manual-limit-price must be positive")
        allowlist_entry = ReadOnlyPreflightConfig().contract_allowlist.get(str(args.contract_key))
        if allowlist_entry is None:
            parser.error("--manual-limit-price requires exact allowlisted contract")
        try:
            tick_size = Decimal(str(allowlist_entry.get("tick_size")))
        except (InvalidOperation, ValueError):
            parser.error("--manual-limit-price requires valid contract tick_size")
        if not tick_size.is_finite() or tick_size <= 0:
            parser.error("--manual-limit-price requires positive contract tick_size")
        if manual_price % tick_size != 0:
            parser.error("--manual-limit-price must be valid for contract tick_size")


def _print_result(result: PaperProofResult) -> None:
    print(
        json.dumps(
            {
                "classification": result.classification.value,
                "report_json": str(result.report_json),
                "report_md": str(result.report_md),
                "proof_report_json": str(result.proof_result.proof_report_json) if result.proof_result else None,
                "proof_report_md": str(result.proof_result.proof_report_md) if result.proof_result else None,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
