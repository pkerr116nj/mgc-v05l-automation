"""CLI wrapper for Track B MGC 1m data maintenance."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Mapping, Sequence

from .databento_quote_provider import DatabentoAvailableEndError
from .databento_quote_provider import NativeDatabentoQuoteTransport
from .track_b_data_maintenance import (
    DEFAULT_TRACK_B_DATA_MAINTENANCE_OUTPUT_ROOT,
    MGC_DATABENTO_CONTINUOUS_SYMBOL,
    MGC_DATASET,
    MGC_LOCAL_SYMBOL,
    TrackBDataMaintenanceVerdict,
    maintain_track_b_mgc_1m_history,
    write_data_maintenance_provider_error,
)
from .track_b_data_maintenance_registry import require_runtime_data_maintenance_instrument
from .track_b_mgc_candle_history_producer import fetch_databento_ohlcv_1m_records, provider_error_message


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Maintain Track B local rolling MGC 1m history. Market-data maintenance only; "
            "no broker, paper proof, listener, strategy, or submit calls."
        )
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--history-json", type=Path, help="Existing Databento-like MGC 1m OHLCV history JSON to merge into the rolling store.")
    source.add_argument("--fetch-databento-history", action="store_true", help="Fetch bounded Databento ohlcv-1m history for maintenance.")
    parser.add_argument("--instrument", help="Track B data-maintenance registry instrument key. MGC is the only runtime-enabled instrument in this slice.")
    parser.add_argument("--expected-account-id", default="DUM882026")
    parser.add_argument("--strategy-id", default="track_b_example_gold_shadow_v1")
    parser.add_argument("--lane-id", default="mgc_example_long_lmt_day")
    parser.add_argument("--contract-key", default="MGC-202606")
    parser.add_argument("--databento-continuous-symbol", default=MGC_DATABENTO_CONTINUOUS_SYMBOL)
    parser.add_argument("--databento-symbol", help="Optional raw Databento symbol override, e.g. MGCM6.")
    parser.add_argument("--stype-in", default="continuous")
    parser.add_argument("--dataset", default=MGC_DATASET)
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--allowlisted-local-symbol", default=MGC_LOCAL_SYMBOL)
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--lookback-minutes", type=int, default=240)
    parser.add_argument("--history-start")
    parser.add_argument("--history-end")
    parser.add_argument("--base-url", default="https://hist.databento.com/v0")
    parser.add_argument("--max-bars", type=int, default=5000)
    parser.add_argument("--export-bars", type=int, default=50)
    parser.add_argument("--min-bars", type=int, default=20)
    parser.add_argument("--max-history-age-seconds", type=int, default=900)
    parser.add_argument("--source-id")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_DATA_MAINTENANCE_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    registry_error = _apply_registry_defaults(args)
    if registry_error:
        result = write_data_maintenance_provider_error(
            primary_blocker=registry_error,
            required_next_action="Use --instrument MGC or omit --instrument and provide explicit MGC maintenance flags.",
            output_root=args.output_root,
            expected_account_id=args.expected_account_id,
            strategy_id=args.strategy_id,
            lane_id=args.lane_id,
            contract_key=args.contract_key,
            symbol=args.allowlisted_local_symbol,
            databento_continuous_symbol=args.databento_continuous_symbol,
            dataset=args.dataset,
            timeframe=args.timeframe,
            source_id=args.source_id,
        )
        _print_result(result)
        return 2
    source_payload_path = args.history_json
    if args.fetch_databento_history:
        raw_api_key = str(os.environ.get("DATABENTO_API_KEY") or "").strip()
        if not raw_api_key:
            result = write_data_maintenance_provider_error(
                primary_blocker="DATABENTO_API_KEY is required for Track B MGC 1m data maintenance fetch.",
                required_next_action="Set DATABENTO_API_KEY or provide --history-json with bounded MGC 1m bars.",
                output_root=args.output_root,
                expected_account_id=args.expected_account_id,
                strategy_id=args.strategy_id,
                lane_id=args.lane_id,
                contract_key=args.contract_key,
                symbol=args.allowlisted_local_symbol,
                databento_continuous_symbol=args.databento_continuous_symbol,
                dataset=args.dataset,
                timeframe=args.timeframe,
                source_id=args.source_id,
            )
            _print_result(result)
            return 2
        try:
            end = _parse_time(args.history_end) if args.history_end else datetime.now(UTC)
            start = _parse_time(args.history_start) if args.history_start else end - timedelta(minutes=max(int(args.lookback_minutes), 1))
            symbol = args.databento_symbol or args.databento_continuous_symbol
            stype_in = "raw_symbol" if args.databento_symbol else args.stype_in
            transport = NativeDatabentoQuoteTransport()
            requested_history_end = end
            history_end_used = end
            provider_available_end = None
            available_end_lag_seconds = None
            history_provider_mode = "DATABENTO_DATA_MAINTENANCE_FETCH"
            try:
                records = fetch_databento_ohlcv_1m_records(
                    transport=transport,
                    api_key=raw_api_key,
                    dataset=args.dataset,
                    symbol=symbol,
                    stype_in=stype_in,
                    schema=args.schema,
                    start=start,
                    end=end,
                    max_candles=args.export_bars,
                    base_url=args.base_url,
                )
            except DatabentoAvailableEndError as exc:
                if exc.provider_available_end is None:
                    raise
                provider_available_end = exc.provider_available_end.astimezone(UTC)
                if provider_available_end <= start:
                    raise
                history_end_used = provider_available_end
                available_end_lag_seconds = max(int((requested_history_end.astimezone(UTC) - provider_available_end).total_seconds()), 0)
                history_provider_mode = "HISTORICAL_AVAILABLE_END"
                fallback_start = history_end_used - timedelta(minutes=max(int(args.lookback_minutes), 1))
                records = fetch_databento_ohlcv_1m_records(
                    transport=transport,
                    api_key=raw_api_key,
                    dataset=args.dataset,
                    symbol=symbol,
                    stype_in=stype_in,
                    schema=args.schema,
                    start=fallback_start,
                    end=history_end_used,
                    max_candles=args.export_bars,
                    base_url=args.base_url,
                )
            history_payload: dict[str, object] = _history_payload_from_records(
                records=records,
                args=args,
                requested_history_end=requested_history_end,
                provider_available_end=provider_available_end,
                history_end_used=history_end_used,
                available_end_lag_seconds=available_end_lag_seconds,
                history_provider_mode=history_provider_mode,
            )
            source_payload_path = None
        except Exception as exc:  # noqa: BLE001 - provider failures become explicit maintenance artifacts.
            result = write_data_maintenance_provider_error(
                primary_blocker=provider_error_message(exc),
                required_next_action="Retry after Databento historical available_end advances, use a bounded earlier history window, or provide --history-json.",
                output_root=args.output_root,
                expected_account_id=args.expected_account_id,
                strategy_id=args.strategy_id,
                lane_id=args.lane_id,
                contract_key=args.contract_key,
                symbol=args.allowlisted_local_symbol,
                databento_continuous_symbol=args.databento_continuous_symbol,
                dataset=args.dataset,
                timeframe=args.timeframe,
                source_id=args.source_id,
                requested_history_end=end if "end" in locals() else None,
                provider_available_end=getattr(exc, "provider_available_end", None),
                history_end_used=None,
                available_end_lag_seconds=None,
                history_provider_mode="HISTORICAL_AVAILABLE_END"
                if isinstance(exc, DatabentoAvailableEndError)
                else "DATABENTO_DATA_MAINTENANCE_FETCH",
            )
            _print_result(result)
            return 2
    else:
        history_payload = json.loads(args.history_json.read_text(encoding="utf-8"))

    result = maintain_track_b_mgc_1m_history(
        incoming_history_payload=history_payload,
        output_root=args.output_root,
        source_payload_path=source_payload_path,
        expected_account_id=args.expected_account_id,
        strategy_id=args.strategy_id,
        lane_id=args.lane_id,
        contract_key=args.contract_key,
        symbol=args.allowlisted_local_symbol,
        databento_continuous_symbol=args.databento_continuous_symbol,
        dataset=args.dataset,
        timeframe=args.timeframe,
        max_bars=args.max_bars,
        export_bars=args.export_bars,
        min_bars=args.min_bars,
        max_history_age_seconds=args.max_history_age_seconds,
        source_id=args.source_id,
    )
    _print_result(result)
    return 0 if result.verdict == TrackBDataMaintenanceVerdict.UPDATED_HISTORY_READY else 2


def _history_payload_from_records(
    *,
    records: Sequence[Mapping[str, object]],
    args: argparse.Namespace,
    requested_history_end: datetime,
    provider_available_end: datetime | None,
    history_end_used: datetime,
    available_end_lag_seconds: int | None,
    history_provider_mode: str,
) -> dict[str, object]:
    return {
        "schema_version": "track_b_databento_ohlcv_history_raw_v1",
        "source_id": args.source_id or "track_b_data_maintenance_databento_fetch",
        "account_id": args.expected_account_id,
        "contract_key": args.contract_key,
        "instrument_family": "MGC",
        "strategy_id": args.strategy_id,
        "lane_id": args.lane_id,
        "dataset": args.dataset,
        "databento_continuous_symbol": args.databento_continuous_symbol,
        "symbol": args.allowlisted_local_symbol,
        "timeframe": args.timeframe,
        "history_provider_mode": history_provider_mode,
        "requested_history_end": requested_history_end.astimezone(UTC).isoformat(),
        "provider_available_end": None if provider_available_end is None else provider_available_end.astimezone(UTC).isoformat(),
        "history_end_used": history_end_used.astimezone(UTC).isoformat(),
        "available_end_lag_seconds": available_end_lag_seconds,
        "candles": list(records),
    }


def _apply_registry_defaults(args: argparse.Namespace) -> str | None:
    if not args.instrument:
        return None
    try:
        instrument = require_runtime_data_maintenance_instrument(args.instrument)
    except ValueError as exc:
        return str(exc)
    if instrument.contract_key is not None:
        args.contract_key = instrument.contract_key
    if instrument.provider_symbol is not None:
        args.databento_symbol = instrument.provider_symbol
    if instrument.continuous_symbol is not None:
        args.databento_continuous_symbol = instrument.continuous_symbol
    if instrument.dataset is not None:
        args.dataset = instrument.dataset
    args.schema = instrument.schema
    args.timeframe = instrument.timeframe
    return None


def _print_result(result) -> None:  # type: ignore[no-untyped-def]
    print(
        json.dumps(
            {
                "data_maintenance_verdict": result.report["data_maintenance_verdict"],
                "source_provider": result.report["source_provider"],
                "contract_key": result.report["contract_key"],
                "symbol": result.report["symbol"],
                "timeframe": result.report["timeframe"],
                "bars_available": result.report["bars_available"],
                "first_bar_timestamp": result.report["first_bar_timestamp"],
                "last_bar_timestamp": result.report["last_bar_timestamp"],
                "gap_count": result.report["gap_count"],
                "duplicate_count": result.report["duplicate_count"],
                "latest_good_history_path": result.report["latest_good_history_path"],
                "requested_history_end": result.report["requested_history_end"],
                "provider_available_end": result.report["provider_available_end"],
                "history_end_used": result.report["history_end_used"],
                "available_end_lag_seconds": result.report["available_end_lag_seconds"],
                "history_provider_mode": result.report["history_provider_mode"],
                "history_freshness_seconds": result.report["history_freshness_seconds"],
                "history_ready": result.report["history_ready"],
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


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
