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

from .databento_quote_provider import (
    DatabentoAvailableEndError,
    DatabentoQuoteProvider,
    DatabentoQuoteProviderConfig,
    DatabentoQuoteProviderError,
    DatabentoQuoteTransport,
    DatabentoRecordDiagnosticRequest,
    run_databento_record_diagnostic,
)
from .quote_provider import validate_quote_for_pricing


DEFAULT_OUTPUT_ROOT = Path("outputs/track_b_execution_core/quotes")
ProviderFactory = Callable[[DatabentoQuoteProviderConfig], DatabentoQuoteProvider]
DiagnosticTransportFactory = Callable[[], DatabentoQuoteTransport]


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
    parser.add_argument("--quote-lookback-seconds", type=int)
    parser.add_argument("--quote-end-timestamp")
    parser.add_argument("--allow-available-end-fallback", action="store_true")
    parser.add_argument("--max-age-seconds", type=int, default=15)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--diagnose-records", action="store_true")
    parser.add_argument("--native-databento", action="store_true", help="Use the native Databento client path. This is the Track B default.")
    parser.add_argument("--diagnostic-start")
    parser.add_argument("--diagnostic-end")
    parser.add_argument("--diagnostic-schema", action="append")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    provider_factory: ProviderFactory | None = None,
    diagnostic_transport_factory: DiagnosticTransportFactory | None = None,
) -> int:
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
        lookback_seconds=args.quote_lookback_seconds if args.quote_lookback_seconds is not None else args.lookback_seconds,
        quote_end_timestamp=args.quote_end_timestamp,
        allow_available_end_fallback=args.allow_available_end_fallback,
        realtime_max_age_seconds=args.max_age_seconds,
    )
    if args.diagnose_records:
        return _run_records_diagnostic(args=args, config=config, diagnostic_transport_factory=diagnostic_transport_factory)
    factory = provider_factory or (lambda cfg: DatabentoQuoteProvider(config=cfg))
    provider = factory(config)
    run_id = f"databento_quote_{uuid.uuid4().hex}"
    report_json = Path(args.output_root) / run_id / "quote_report.json"
    try:
        quote = provider.get_quote(args.contract_key)
    except DatabentoQuoteProviderError as exc:
        provider_available_end = getattr(exc, "provider_available_end", None)
        parser_diagnostics = getattr(exc, "diagnostics", {})
        corrective_message = (
            "requested quote window is after Databento available_end; rerun with --allow-available-end-fallback "
            "or earlier --quote-end-timestamp"
            if isinstance(exc, DatabentoAvailableEndError)
            else None
        )
        report = {
            "schema_version": "track_b_databento_quote_v1",
            "run_id": run_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "classification": "FAILED_BEFORE_QUOTE",
            "contract_key": args.contract_key,
            "databento_continuous_symbol": continuous_symbol or None,
            "databento_symbol": raw_symbol or None,
            "manual_provider_symbol_override": bool(raw_symbol),
            "quote_observed": False,
            "provider_error": str(exc),
            "provider_available_end": provider_available_end.isoformat() if provider_available_end is not None else None,
            "corrective_message": corrective_message,
            "resolved_instrument_id": parser_diagnostics.get("resolved_instrument_id"),
            "resolved_symbol_stype": parser_diagnostics.get("resolved_symbol_stype"),
            "quote_request_symbol": parser_diagnostics.get("quote_request_symbol"),
            "quote_request_stype_in": parser_diagnostics.get("quote_request_stype_in"),
            "dataset": parser_diagnostics.get("dataset"),
            "schema": parser_diagnostics.get("schema"),
            "start": parser_diagnostics.get("start"),
            "end": parser_diagnostics.get("end"),
            "encoding": parser_diagnostics.get("encoding"),
            "request_details": parser_diagnostics.get("request_details"),
            "raw_provider_error": parser_diagnostics.get("raw_provider_error"),
            "databento_schema": parser_diagnostics.get("databento_schema"),
            "records_returned": parser_diagnostics.get("records_returned"),
            "first_raw_record_keys_or_shape": parser_diagnostics.get("first_raw_record_keys_or_shape"),
            "parser_bid_field_source": parser_diagnostics.get("parser_bid_field_source"),
            "parser_ask_field_source": parser_diagnostics.get("parser_ask_field_source"),
            "parser_last_field_source": parser_diagnostics.get("parser_last_field_source"),
            "no_quote_records_reason": parser_diagnostics.get("no_quote_records_reason"),
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
                    "classification": "FAILED_BEFORE_QUOTE",
                    "contract_key": args.contract_key,
                    "databento_continuous_symbol": continuous_symbol or None,
                    "databento_symbol": raw_symbol or None,
                    "quote_observed": False,
                    "provider_error": str(exc),
                    "provider_available_end": report["provider_available_end"],
                    "corrective_message": corrective_message,
                    "resolved_instrument_id": report["resolved_instrument_id"],
                    "resolved_symbol_stype": report["resolved_symbol_stype"],
                    "quote_request_symbol": report["quote_request_symbol"],
                    "quote_request_stype_in": report["quote_request_stype_in"],
                    "dataset": report["dataset"],
                    "schema": report["schema"],
                    "start": report["start"],
                    "end": report["end"],
                    "encoding": report["encoding"],
                    "request_details": report["request_details"],
                    "raw_provider_error": report["raw_provider_error"],
                    "databento_schema": report["databento_schema"],
                    "records_returned": report["records_returned"],
                    "first_raw_record_keys_or_shape": report["first_raw_record_keys_or_shape"],
                    "parser_bid_field_source": report["parser_bid_field_source"],
                    "parser_ask_field_source": report["parser_ask_field_source"],
                    "parser_last_field_source": report["parser_last_field_source"],
                    "no_quote_records_reason": report["no_quote_records_reason"],
                    "report_json": str(report_json),
                },
                sort_keys=True,
            )
        )
        return 2
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

    report = {
        "schema_version": "track_b_databento_quote_v1",
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "classification": "QUOTE_OBSERVED",
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
        "quote_observed": True,
        "quote_window": {
            "requested_quote_start": quote.raw.get("requested_quote_start"),
            "requested_quote_end": quote.raw.get("requested_quote_end"),
            "actual_quote_start": quote.raw.get("actual_quote_start"),
            "actual_quote_end": quote.raw.get("actual_quote_end"),
            "provider_available_end": quote.raw.get("provider_available_end"),
            "available_end_fallback_used": quote.raw.get("available_end_fallback_used"),
            "quote_age_seconds": quote.raw.get("quote_age_seconds"),
            "usable_for_paper_pricing": quote.raw.get("usable_for_paper_pricing"),
            "usable_for_live_money_readiness": quote.raw.get("usable_for_live_money_readiness"),
        },
        "databento_schema": quote.raw.get("databento_schema"),
        "records_returned": quote.raw.get("records_returned"),
        "first_raw_record_keys_or_shape": quote.raw.get("first_raw_record_keys_or_shape"),
        "parser_bid_field_source": quote.raw.get("parser_bid_field_source"),
        "parser_ask_field_source": quote.raw.get("parser_ask_field_source"),
        "parser_last_field_source": quote.raw.get("parser_last_field_source"),
        "no_quote_records_reason": quote.raw.get("no_quote_records_reason"),
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
                "classification": "QUOTE_OBSERVED",
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
                "quote_observed": True,
                "requested_quote_start": quote.raw.get("requested_quote_start"),
                "requested_quote_end": quote.raw.get("requested_quote_end"),
                "actual_quote_start": quote.raw.get("actual_quote_start"),
                "actual_quote_end": quote.raw.get("actual_quote_end"),
                "provider_available_end": quote.raw.get("provider_available_end"),
                "available_end_fallback_used": quote.raw.get("available_end_fallback_used"),
                "quote_age_seconds": quote.raw.get("quote_age_seconds"),
                "databento_schema": quote.raw.get("databento_schema"),
                "records_returned": quote.raw.get("records_returned"),
                "first_raw_record_keys_or_shape": quote.raw.get("first_raw_record_keys_or_shape"),
                "parser_bid_field_source": quote.raw.get("parser_bid_field_source"),
                "parser_ask_field_source": quote.raw.get("parser_ask_field_source"),
                "parser_last_field_source": quote.raw.get("parser_last_field_source"),
                "no_quote_records_reason": quote.raw.get("no_quote_records_reason"),
                "usable_for_paper_pricing": quote.raw.get("usable_for_paper_pricing"),
                "usable_for_live_money_readiness": quote.raw.get("usable_for_live_money_readiness"),
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


def _run_records_diagnostic(
    *,
    args: argparse.Namespace,
    config: DatabentoQuoteProviderConfig,
    diagnostic_transport_factory: DiagnosticTransportFactory | None,
) -> int:
    if not config.databento_symbol:
        raise SystemExit("--diagnose-records requires --databento-symbol as the exact diagnostic symbol to send")
    if not args.diagnostic_start or not args.diagnostic_end:
        raise SystemExit("--diagnose-records requires --diagnostic-start and --diagnostic-end")
    schemas = tuple(args.diagnostic_schema or ("mbp-1", "trades"))
    start = _parse_cli_datetime(args.diagnostic_start, "--diagnostic-start")
    end = _parse_cli_datetime(args.diagnostic_end, "--diagnostic-end")
    transport = diagnostic_transport_factory() if diagnostic_transport_factory else None
    run_id = f"databento_records_diagnostic_{uuid.uuid4().hex}"
    report_json = Path(args.output_root) / run_id / "records_diagnostic_report.json"
    results = [
        run_databento_record_diagnostic(
            request=DatabentoRecordDiagnosticRequest(
                api_key=config.api_key,
                dataset=config.dataset,
                symbol=str(config.databento_symbol),
                stype_in=config.stype_in,
                schema=schema,
                start=start,
                end=end,
                base_url=config.base_url,
                record_limit=config.record_limit,
            ),
            transport=transport,
        ).to_json_dict()
        for schema in schemas
    ]
    classification = "RECORDS_FOUND" if any(result["records_returned"] for result in results) else "ZERO_RECORDS_OR_PROVIDER_ERROR"
    report = {
        "schema_version": "track_b_databento_records_diagnostic_v1",
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "classification": classification,
        "contract_key": args.contract_key,
        "symbol": config.databento_symbol,
        "stype_in": config.stype_in,
        "dataset": config.dataset,
        "diagnostic_start": start.isoformat(),
        "diagnostic_end": end.isoformat(),
        "results": results,
        "api_key_present": True,
        "api_key_value": None,
        "submit_enabled": False,
        "place_order_called": False,
        "cancel_called": False,
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({**report, "report_json": str(report_json)}, sort_keys=True))
    return 0 if classification == "RECORDS_FOUND" else 2


def _parse_cli_datetime(value: str, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise SystemExit(f"{label} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
