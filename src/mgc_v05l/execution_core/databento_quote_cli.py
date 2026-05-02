"""Read-only CLI for Track B Databento quote snapshots."""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence

from .databento_quote_provider import DatabentoQuoteProvider, DatabentoQuoteProviderConfig
from .quote_provider import validate_quote_for_pricing


DEFAULT_OUTPUT_ROOT = Path("outputs/track_b_execution_core/quotes")
ProviderFactory = Callable[[DatabentoQuoteProviderConfig], DatabentoQuoteProvider]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch one Track B read-only Databento quote snapshot.")
    parser.add_argument("--contract-key", required=True)
    parser.add_argument("--databento-symbol", required=True)
    parser.add_argument("--tick-size", required=True)
    parser.add_argument("--exchange", required=True)
    parser.add_argument("--currency", required=True)
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--stype-in", default="raw_symbol")
    parser.add_argument("--base-url", default="https://hist.databento.com/v0")
    parser.add_argument("--lookback-seconds", type=int, default=300)
    parser.add_argument("--max-age-seconds", type=int, default=15)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None, *, provider_factory: ProviderFactory | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    api_key = str(os.environ.get("DATABENTO_API_KEY") or "").strip()
    if not api_key:
        parser.error("DATABENTO_API_KEY is required for read-only Databento quote retrieval")
    config = DatabentoQuoteProviderConfig(
        contract_key=args.contract_key,
        databento_symbol=args.databento_symbol,
        tick_size=args.tick_size,
        exchange=args.exchange,
        currency=args.currency,
        api_key=api_key,
        dataset=args.dataset,
        stype_in=args.stype_in,
        base_url=args.base_url,
        lookback_seconds=args.lookback_seconds,
        realtime_max_age_seconds=args.max_age_seconds,
    )
    factory = provider_factory or (lambda cfg: DatabentoQuoteProvider(config=cfg))
    provider = factory(config)
    quote = provider.get_quote(args.contract_key)
    readiness_error = None
    try:
        validate_quote_for_pricing(
            quote,
            now=datetime.now(timezone.utc),
            max_age_seconds=args.max_age_seconds,
            allow_delayed_for_paper=False,
            live_money=True,
        )
    except Exception as exc:  # noqa: BLE001 - quote diagnostics should report validation failure.
        readiness_error = str(exc)

    run_id = f"databento_quote_{uuid.uuid4().hex}"
    report_json = Path(args.output_root) / run_id / "quote_report.json"
    report = {
        "schema_version": "track_b_databento_quote_v1",
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "contract_key": args.contract_key,
        "databento_symbol": args.databento_symbol,
        "quote": quote.to_json_dict(),
        "live_money_quote_ready": readiness_error is None,
        "readiness_error": readiness_error,
        "api_key_present": True,
        "api_key_value": None,
        "submit_enabled": False,
        "place_order_called": False,
        "cancel_called": False,
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(
        json.dumps(
            {
                "provider": quote.provider,
                "contract_key": quote.contract_key,
                "databento_symbol": quote.provider_symbol,
                "mode": quote.mode,
                "bid": str(quote.bid) if quote.bid is not None else None,
                "ask": str(quote.ask) if quote.ask is not None else None,
                "last": str(quote.last) if quote.last is not None else None,
                "timestamp": quote.timestamp.isoformat(),
                "live_money_quote_ready": readiness_error is None,
                "readiness_error": readiness_error,
                "report_json": str(report_json),
            },
            sort_keys=True,
        )
    )
    return 0 if readiness_error is None else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
