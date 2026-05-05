"""CLI for Track B bounded runtime MGC candle capture."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Sequence

from .databento_quote_provider import DatabentoAvailableEndError, NativeDatabentoQuoteTransport
from .track_b_mgc_candle_history_producer import fetch_databento_ohlcv_1m_records, provider_error_message
from .track_b_runtime_candle_capture import (
    DEFAULT_TRACK_B_RUNTIME_CANDLE_CAPTURE_OUTPUT_ROOT,
    MGC_CONTINUOUS_SYMBOL,
    MGC_DATASET,
    MGC_LOCAL_SYMBOL,
    TrackBRuntimeCandleCaptureResult,
    TrackBRuntimeCandleCaptureVerdict,
    capture_track_b_runtime_mgc_1m_candles,
    write_runtime_candle_capture_provider_error,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Write a bounded Track B runtime MGC 1m candle context artifact. No broker or submit paths are invoked."
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--runtime-candle-json", type=Path, help="Supplied runtime MGC candle JSON payload.")
    source.add_argument("--fetch-databento-history", action="store_true", help="Fetch bounded recent Databento ohlcv-1m records.")
    parser.add_argument("--current-quote-report-json", type=Path)
    parser.add_argument("--expected-account-id", default="DUM882026")
    parser.add_argument("--account-id", default="DUM882026")
    parser.add_argument("--contract-key", default="MGC-202606")
    parser.add_argument("--local-symbol", default=MGC_LOCAL_SYMBOL)
    parser.add_argument("--databento-continuous-symbol", default=MGC_CONTINUOUS_SYMBOL)
    parser.add_argument("--databento-symbol", help="Optional raw Databento symbol override, e.g. MGCM6.")
    parser.add_argument("--stype-in", default="continuous")
    parser.add_argument("--dataset", default=MGC_DATASET)
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--lookback-minutes", type=int, default=60)
    parser.add_argument("--history-start")
    parser.add_argument("--history-end")
    parser.add_argument("--env-file", type=Path, help="Optional dotenv file containing DATABENTO_API_KEY. Defaults to repo .env.local for Databento fetch mode.")
    parser.add_argument("--base-url", default="https://hist.databento.com/v0")
    parser.add_argument("--max-bars", type=int, default=250)
    parser.add_argument("--min-bars", type=int, default=3)
    parser.add_argument("--candle-source-mode", default="SUPPLIED_RUNTIME_CANDLES")
    parser.add_argument("--max-latest-1m-age-seconds", type=int, default=900)
    parser.add_argument("--max-completed-5m-age-seconds", type=int, default=900)
    parser.add_argument("--source-id", default="track_b_runtime_candle_capture")
    parser.add_argument("--strategy-id", default="track_b_example_gold_shadow_v1")
    parser.add_argument("--lane-id", default="mgc_example_long_lmt_day")
    parser.add_argument("--retention-runs", type=int, default=5)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_RUNTIME_CANDLE_CAPTURE_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    requested_window_start = None
    requested_window_end = None
    provider_available_end = None
    history_end_used = None
    available_end_lag_seconds = None
    credential_status = "NOT_APPLICABLE"
    credential_source = None
    source_payload_path = args.runtime_candle_json
    candle_source_mode = args.candle_source_mode

    if args.fetch_databento_history:
        quote_payload = _read_quote_payload(args.current_quote_report_json)
        requested_window_end = _parse_time(args.history_end) if args.history_end else datetime.now(UTC)
        requested_window_start = (
            _parse_time(args.history_start)
            if args.history_start
            else requested_window_end - timedelta(minutes=max(int(args.lookback_minutes), 1))
        )
        raw_api_key, credential_status, credential_source = _load_databento_api_key(args.env_file)
        if not raw_api_key:
            result = _provider_error_result(
                args=args,
                primary_blocker="DATABENTO_API_KEY is missing for bounded Databento runtime candle fetch.",
                required_next_action="Set DATABENTO_API_KEY in the process environment or repo .env.local, or provide --runtime-candle-json with fresh bounded runtime candles.",
                requested_window_start=requested_window_start,
                requested_window_end=requested_window_end,
                provider_credential_status=credential_status,
                provider_credential_source=credential_source,
            )
            _print_result(result)
            return 2
        try:
            records, provider_available_end, history_end_used, available_end_lag_seconds, candle_source_mode = _fetch_records(
                args=args,
                api_key=raw_api_key,
                requested_window_start=requested_window_start,
                requested_window_end=requested_window_end,
            )
        except Exception as exc:  # noqa: BLE001 - provider failures become explicit artifacts.
            result = _provider_error_result(
                args=args,
                primary_blocker=provider_error_message(exc),
                required_next_action="Retry with a bounded runtime candle window, valid Databento entitlement, or provide --runtime-candle-json.",
                requested_window_start=requested_window_start,
                requested_window_end=requested_window_end,
                provider_available_end=getattr(exc, "provider_available_end", None),
                provider_credential_status=credential_status,
                provider_credential_source=credential_source,
            )
            _print_result(result)
            return 2
        payload = _runtime_payload_from_records(
            records=records,
            args=args,
            quote_payload=quote_payload,
            candle_source_mode=candle_source_mode,
            requested_window_start=requested_window_start,
            requested_window_end=requested_window_end,
            provider_available_end=provider_available_end,
            history_end_used=history_end_used,
            available_end_lag_seconds=available_end_lag_seconds,
        )
        source_payload_path = None
    else:
        payload = json.loads(args.runtime_candle_json.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise SystemExit("--runtime-candle-json must contain a JSON object.")

    result = capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=payload,
        source_payload_path=source_payload_path,
        expected_account_id=args.expected_account_id,
        account_id=args.account_id,
        contract_key=args.contract_key,
        local_symbol=args.local_symbol,
        databento_continuous_symbol=args.databento_continuous_symbol,
        dataset=args.dataset,
        timeframe=args.timeframe,
        max_bars=args.max_bars,
        min_bars=args.min_bars,
        candle_source_mode=candle_source_mode,
        requested_window_start=requested_window_start,
        requested_window_end=requested_window_end,
        provider_available_end=provider_available_end,
        history_end_used=history_end_used,
        available_end_lag_seconds=available_end_lag_seconds,
        max_latest_1m_age_seconds=args.max_latest_1m_age_seconds,
        max_completed_5m_age_seconds=args.max_completed_5m_age_seconds,
        provider_credential_status=credential_status,
        provider_credential_source=credential_source,
        source_id=args.source_id,
        strategy_id=args.strategy_id,
        lane_id=args.lane_id,
        retention_runs=args.retention_runs,
        output_root=args.output_root,
    )
    _print_result(result)
    return 0 if result.verdict == TrackBRuntimeCandleCaptureVerdict.WROTE_RUNTIME_CANDLES else 2


def _fetch_records(
    *,
    args: argparse.Namespace,
    api_key: str,
    requested_window_start: datetime,
    requested_window_end: datetime,
) -> tuple[Sequence[object], datetime | None, datetime, int | None, str]:
    symbol = args.databento_symbol or args.databento_continuous_symbol
    stype_in = "raw_symbol" if args.databento_symbol else args.stype_in
    try:
        records = fetch_databento_ohlcv_1m_records(
            transport=NativeDatabentoQuoteTransport(),
            api_key=api_key,
            dataset=args.dataset,
            symbol=symbol,
            stype_in=stype_in,
            schema=args.schema,
            start=requested_window_start,
            end=requested_window_end,
            max_candles=args.max_bars,
            base_url=args.base_url,
        )
        return records, None, requested_window_end, None, "DATABENTO_HISTORICAL_RECENT"
    except DatabentoAvailableEndError as exc:
        provider_available_end = None if exc.provider_available_end is None else exc.provider_available_end.astimezone(UTC)
        if provider_available_end is None or provider_available_end <= requested_window_start:
            raise
        fallback_start = provider_available_end - timedelta(minutes=max(int(args.lookback_minutes), 1))
        records = fetch_databento_ohlcv_1m_records(
            transport=NativeDatabentoQuoteTransport(),
            api_key=api_key,
            dataset=args.dataset,
            symbol=symbol,
            stype_in=stype_in,
            schema=args.schema,
            start=fallback_start,
            end=provider_available_end,
            max_candles=args.max_bars,
            base_url=args.base_url,
        )
        lag = max(int((requested_window_end - provider_available_end).total_seconds()), 0)
        return records, provider_available_end, provider_available_end, lag, "DATABENTO_HISTORICAL_AVAILABLE_END_RECENT"


def _provider_error_result(
    *,
    args: argparse.Namespace,
    primary_blocker: str,
    required_next_action: str,
    requested_window_start: datetime | None,
    requested_window_end: datetime | None,
    provider_available_end: datetime | None = None,
    provider_credential_status: str | None = None,
    provider_credential_source: str | None = None,
) -> TrackBRuntimeCandleCaptureResult:
    return write_runtime_candle_capture_provider_error(
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
        source_id=args.source_id,
        account_id=args.account_id,
        contract_key=args.contract_key,
        local_symbol=args.local_symbol,
        databento_continuous_symbol=args.databento_continuous_symbol,
        dataset=args.dataset,
        timeframe=args.timeframe,
        requested_window_start=requested_window_start,
        requested_window_end=requested_window_end,
        provider_available_end=provider_available_end,
        max_bars=args.max_bars,
        min_bars=args.min_bars,
        max_latest_1m_age_seconds=args.max_latest_1m_age_seconds,
        max_completed_5m_age_seconds=args.max_completed_5m_age_seconds,
        provider_credential_status=provider_credential_status,
        provider_credential_source=provider_credential_source,
        output_root=args.output_root,
    )


def _runtime_payload_from_records(
    *,
    records: Sequence[object],
    args: argparse.Namespace,
    quote_payload: dict[str, object],
    candle_source_mode: str,
    requested_window_start: datetime,
    requested_window_end: datetime,
    provider_available_end: datetime | None,
    history_end_used: datetime | None,
    available_end_lag_seconds: int | None,
) -> dict[str, object]:
    return {
        "source_id": args.source_id,
        "account_id": args.account_id,
        "expected_account_id": args.expected_account_id,
        "contract_key": args.contract_key,
        "instrument_family": "MGC",
        "strategy_id": args.strategy_id,
        "lane_id": args.lane_id,
        "symbol": args.local_symbol,
        "local_symbol": args.local_symbol,
        "databento_continuous_symbol": args.databento_continuous_symbol,
        "dataset": args.dataset,
        "timeframe": args.timeframe,
        "candle_source_mode": candle_source_mode,
        "requested_window_start": requested_window_start.isoformat(),
        "requested_window_end": requested_window_end.isoformat(),
        "provider_available_end": None if provider_available_end is None else provider_available_end.isoformat(),
        "history_end_used": None if history_end_used is None else history_end_used.isoformat(),
        "available_end_lag_seconds": available_end_lag_seconds,
        "quote_provider_mode": quote_payload.get("quote_provider_mode"),
        "realtime_quote_received": quote_payload.get("realtime_quote_received"),
        "current_quote_available": quote_payload.get("current_quote_available"),
        "quote_freshness_verdict": quote_payload.get("quote_freshness_verdict"),
        "quote_report_path": quote_payload.get("report_json_path"),
        "candles": list(records),
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def _read_quote_payload(path: Path | None) -> dict[str, object]:
    if path is None:
        raise SystemExit("--current-quote-report-json is required with --fetch-databento-history.")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit("--current-quote-report-json must contain a JSON object.")
    return payload


def _load_databento_api_key(env_file: Path | None) -> tuple[str, str, str | None]:
    process_value = str(os.environ.get("DATABENTO_API_KEY") or "").strip()
    if process_value:
        return process_value, "FOUND_IN_PROCESS_ENV", "process:DATABENTO_API_KEY"
    dotenv_path = env_file or _repo_root() / ".env.local"
    file_value = _read_dotenv_value(dotenv_path, "DATABENTO_API_KEY")
    if file_value:
        os.environ.setdefault("DATABENTO_API_KEY", file_value)
        return file_value, "FOUND_IN_ENV_FILE", str(dotenv_path)
    return "", "MISSING", str(dotenv_path)


def _read_dotenv_value(path: Path, key: str) -> str:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ""
    prefix = f"{key}="
    export_prefix = f"export {key}="
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith(export_prefix):
            raw_value = line[len(export_prefix) :]
        elif line.startswith(prefix):
            raw_value = line[len(prefix) :]
        else:
            continue
        value = raw_value.strip()
        if "#" in value and not (value.startswith('"') or value.startswith("'")):
            value = value.split("#", 1)[0].strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        return value.strip()
    return ""


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _print_result(result: TrackBRuntimeCandleCaptureResult) -> None:
    report = result.report
    print(
        json.dumps(
            {
                "runtime_candle_capture_verdict": report["runtime_candle_capture_verdict"],
                "runtime_candle_context_ready": report["runtime_candle_context_ready"],
                "bars_available": report["bars_available"],
                "max_bars": report["max_bars"],
                "gap_count": report["gap_count"],
                "candle_source_mode": report["candle_source_mode"],
                "requested_window_start": report.get("requested_window_start"),
                "requested_window_end": report.get("requested_window_end"),
                "provider_available_end": report.get("provider_available_end"),
                "history_end_used": report.get("history_end_used"),
                "available_end_lag_seconds": report.get("available_end_lag_seconds"),
                "provider_credential_status": report.get("provider_credential_status"),
                "provider_credential_source": report.get("provider_credential_source"),
                "latest_1m_candle_timestamp": report.get("latest_1m_candle_timestamp"),
                "latest_completed_5m_candle_timestamp": report.get("latest_completed_5m_candle_timestamp"),
                "latest_1m_candle_age_seconds": report.get("latest_1m_candle_age_seconds"),
                "latest_completed_5m_candle_age_seconds": report.get("latest_completed_5m_candle_age_seconds"),
                "runtime_candle_context_stale": report.get("runtime_candle_context_stale"),
                "latest_runtime_candles_path": report["latest_runtime_candles_path"],
                "submit_allowed": report["submit_allowed"],
                "submit_attempted": report["submit_attempted"],
                "live_money_readiness": report["live_money_readiness"],
                "primary_blocker": report["primary_blocker"],
                "required_next_action": report["required_next_action"],
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
