"""CLI wrapper for Track B read-only TWS paper preflight."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Callable, Sequence

from .ibkr_readonly_transport import IbkrReadOnlyTransportConfig, IbkrReadOnlyTwsTransport
from .preflight import PreflightClassification, ReadOnlyPreflightConfig, run_read_only_preflight
from .pricing import MarketDataMode


TransportFactory = Callable[[IbkrReadOnlyTransportConfig], object]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Track B read-only TWS paper preflight.")
    parser.add_argument("--mode", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--client-id", required=True, type=int)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--contract-key", required=True)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--skip-quote", action="store_true")
    parser.add_argument("--request-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--quote-timeout-seconds", type=float, default=3.0)
    parser.add_argument(
        "--market-data-mode",
        choices=(MarketDataMode.REALTIME, MarketDataMode.DELAYED, MarketDataMode.DELAYED_FROZEN, MarketDataMode.UNKNOWN),
        default=MarketDataMode.DELAYED,
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    transport_factory: TransportFactory | None = None,
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _validate_explicit_paper_args(parser, args)

    config = ReadOnlyPreflightConfig(
        mode=args.mode,
        host=args.host,
        port=args.port,
        client_id=args.client_id,
        account_id=args.account_id,
        contract_key=args.contract_key,
        output_root=args.output_root,
        observe_quote=not args.skip_quote,
    )
    transport_config = IbkrReadOnlyTransportConfig(
        request_timeout_seconds=args.request_timeout_seconds,
        quote_timeout_seconds=args.quote_timeout_seconds,
        market_data_mode=args.market_data_mode,
    )
    factory = transport_factory or (lambda cfg: IbkrReadOnlyTwsTransport(config=cfg))
    result = run_read_only_preflight(config=config, transport=factory(transport_config))  # type: ignore[arg-type]
    print(
        json.dumps(
            {
                "classification": result.classification.value,
                "report_json": str(result.report_json),
                "report_md": str(result.report_md),
            },
            sort_keys=True,
        )
    )
    if result.classification == PreflightClassification.READY_READ_ONLY:
        return 0
    if result.classification == PreflightClassification.BLOCKED:
        return 2
    return 3


def _validate_explicit_paper_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
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


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
