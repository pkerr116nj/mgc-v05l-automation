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
    parser.add_argument("--databento-continuous-symbol")
    parser.add_argument("--databento-symbol")
    parser.add_argument("--allowlisted-local-symbol")
    parser.add_argument("--tick-size", required=True)
    parser.add_argument("--exchange", required=True)
    parser.add_argument("--currency", required=True)
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--stype-in", default="raw_symbol")
    parser.add_argument("--resolver-stype-in", default="continuous")
    parser.add_argument("--resolver-stype-out", default="instrument_id")
    parser.add_argument("--resolution-date")
    parser.add_argument("--resolution-start")
    parser.add_argument("--resolution-end")
    parser.add_argument("--allow-prior-session-resolution", action="store_true")
    parser.add_argument("--prior-session-resolution-lookback-days", type=int, default=3)
    parser.add_argument("--base-url", default="https://hist.databento.com/v0")
    parser.add_argument("--lookback-seconds", type=int, default=300)
    parser.add_argument("--max-age-seconds", type=int, default=15)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None, *, provider_factory: ProviderFactory | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    raw_symbol = str(args.databento_symbol or "").strip()
    continuous_symbol = str(args.databento_continuous_symbol or "").strip()
    if bool(raw_symbol) == bool(continuous_symbol):
        parser.error("configure exactly one of --databento-continuous-symbol or --databento-symbol")
    api_key = str(os.environ.get("DATABENTO_API_KEY") or "").strip()
    if not api_key:
        parser.error("DATABENTO_API_KEY is required for read-only Databento quote retrieval")
    config = DatabentoQuoteProviderConfig(
        contract_key=args.contract_key,
        tick_size=args.tick_size,
        exchange=args.exchange,
        currency=args.currency,
        api_key=api_key,
        databento_symbol=raw_symbol or None,
        databento_continuous_symbol=continuous_symbol or None,
        allowlisted_local_symbol=args.allowlisted_local_symbol,
        dataset=args.dataset,
        stype_in=args.stype_in,
        resolver_stype_in=args.resolver_stype_in,
        resolver_stype_out=args.resolver_stype_out,
        resolution_date=args.resolution_date,
        resolution_start=args.resolution_start,
        resolution_end=args.resolution_end,
        allow_prior_session_resolution=args.allow_prior_session_resolution,
        prior_session_resolution_lookback_days=args.prior_session_resolution_lookback_days,
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
        "databento_continuous_symbol": continuous_symbol or None,
        "databento_symbol": raw_symbol or None,
        "manual_provider_symbol_override": bool(raw_symbol),
        "allowlisted_local_symbol": args.allowlisted_local_symbol,
        "resolution": {
            "symbol_source": quote.raw.get("symbol_source"),
            "requested_continuous_symbol": quote.raw.get("requested_continuous_symbol"),
            "resolved_instrument_id": quote.raw.get("resolved_instrument_id"),
            "resolved_raw_symbol": quote.raw.get("resolved_raw_symbol"),
            "resolution_status": quote.raw.get("resolution_status"),
            "resolution_path": quote.raw.get("resolution_path"),
            "requested_resolution_date": quote.raw.get("requested_resolution_date"),
            "actual_resolution_date_used": quote.raw.get("actual_resolution_date_used"),
            "prior_session_fallback_used": quote.raw.get("prior_session_fallback_used"),
            "fallback_lookback_days": quote.raw.get("fallback_lookback_days"),
            "resolution_session_type": quote.raw.get("resolution_session_type"),
            "resolution_date": quote.raw.get("resolution_date"),
            "resolution_start": quote.raw.get("resolution_start"),
            "resolution_end": quote.raw.get("resolution_end"),
            "mapping_intervals": quote.raw.get("mapping_intervals"),
            "active_mapping": quote.raw.get("active_mapping"),
            "raw_symbol_lookup_path": quote.raw.get("raw_symbol_lookup_path"),
            "raw_symbol_resolution_status": quote.raw.get("raw_symbol_resolution_status"),
            "raw_symbol_match_status": quote.raw.get("raw_symbol_match_status"),
            "quote_request_symbol": quote.raw.get("quote_request_symbol"),
            "quote_request_stype_in": quote.raw.get("quote_request_stype_in"),
            "execution_contract_validation_status": quote.raw.get("execution_contract_validation_status"),
        },
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
                "databento_continuous_symbol": continuous_symbol or None,
                "manual_provider_symbol_override": bool(raw_symbol),
                "resolved_instrument_id": quote.raw.get("resolved_instrument_id"),
                "resolved_raw_symbol": quote.raw.get("resolved_raw_symbol"),
                "resolution_path": quote.raw.get("resolution_path"),
                "requested_resolution_date": quote.raw.get("requested_resolution_date"),
                "actual_resolution_date_used": quote.raw.get("actual_resolution_date_used"),
                "prior_session_fallback_used": quote.raw.get("prior_session_fallback_used"),
                "fallback_lookback_days": quote.raw.get("fallback_lookback_days"),
                "resolution_session_type": quote.raw.get("resolution_session_type"),
                "resolution_date": quote.raw.get("resolution_date"),
                "resolution_start": quote.raw.get("resolution_start"),
                "resolution_end": quote.raw.get("resolution_end"),
                "raw_symbol_lookup_path": quote.raw.get("raw_symbol_lookup_path"),
                "raw_symbol_match_status": quote.raw.get("raw_symbol_match_status"),
                "quote_request_symbol": quote.raw.get("quote_request_symbol"),
                "quote_request_stype_in": quote.raw.get("quote_request_stype_in"),
                "execution_contract_validation_status": quote.raw.get("execution_contract_validation_status"),
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
