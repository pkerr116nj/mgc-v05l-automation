"""CLI entrypoint for replay, research, and Schwab developer utilities."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

from ..config_models import load_settings_from_files
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet
from ..research import build_causal_momentum_report, write_causal_momentum_report_csv
from ..market_data import (
    HistoricalBackfillService,
    QuoteService,
    SchwabHistoricalHttpClient,
    SchwabHistoricalRequest,
    SchwabOAuthClient,
    SchwabQuoteHttpClient,
    SchwabQuoteRequest,
    SchwabTokenStore,
    UrllibJsonTransport,
    load_schwab_auth_config_from_env,
    load_schwab_market_data_config,
)
from ..market_data.schwab_adapter import SchwabMarketDataAdapter
from ..app.bootstrap import bootstrap_service
from .runner import StrategyServiceRunner


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""
    parser = argparse.ArgumentParser(prog="mgc-v05l")
    subparsers = parser.add_subparsers(dest="command", required=True)

    replay_parser = subparsers.add_parser("replay", help="Run a deterministic CSV replay.")
    replay_parser.add_argument("--csv", required=True, help="Path to replay CSV with locked columns.")
    replay_parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Config file path. May be supplied multiple times; later files override earlier ones.",
    )

    report_parser = subparsers.add_parser(
        "research-causal-report",
        help="Export experimental causal momentum-shape features from replay data.",
    )
    report_parser.add_argument("--csv", required=True, help="Path to replay CSV with locked columns.")
    report_parser.add_argument("--output", required=True, help="Output CSV path for the research report.")
    report_parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Config file path. May be supplied multiple times; later files override earlier ones.",
    )
    report_parser.add_argument(
        "--smoothing-length",
        type=int,
        default=3,
        help="Trailing exponential smoothing length for the experimental report.",
    )
    report_parser.add_argument(
        "--normalization-floor",
        default="0.01",
        help="Minimum normalization denominator for ATR-scaled derivatives.",
    )

    auth_url_parser = subparsers.add_parser("schwab-auth-url", help="Build a Schwab OAuth authorization URL.")
    auth_url_parser.add_argument("--state", default="mgc-v05l-local", help="Opaque OAuth state value.")
    auth_url_parser.add_argument("--scope", default=None, help="Optional OAuth scope string.")
    auth_url_parser.add_argument("--token-file", default=None, help="Optional local token file override.")

    exchange_parser = subparsers.add_parser(
        "schwab-exchange-code",
        help="Exchange a Schwab auth code for local access and refresh tokens.",
    )
    exchange_parser.add_argument("--code", required=True, help="Authorization code returned by Schwab.")
    exchange_parser.add_argument("--token-file", default=None, help="Optional local token file override.")

    refresh_parser = subparsers.add_parser(
        "schwab-refresh-token",
        help="Refresh the local Schwab access token using the stored refresh token.",
    )
    refresh_parser.add_argument("--token-file", default=None, help="Optional local token file override.")

    history_parser = subparsers.add_parser(
        "schwab-fetch-history",
        help="Fetch Schwab /pricehistory candles and normalize them into internal bars.",
    )
    history_parser.add_argument("--internal-symbol", required=True, help="Internal strategy symbol, such as MGC.")
    history_parser.add_argument("--period-type", required=True, help="Schwab periodType value.")
    history_parser.add_argument("--period", type=int, default=None, help="Optional Schwab period value.")
    history_parser.add_argument("--frequency-type", default=None, help="Optional Schwab frequencyType value.")
    history_parser.add_argument("--frequency", type=int, default=None, help="Optional Schwab frequency value.")
    history_parser.add_argument("--start-date-ms", type=int, default=None, help="Optional epoch-ms start date.")
    history_parser.add_argument("--end-date-ms", type=int, default=None, help="Optional epoch-ms end date.")
    history_parser.add_argument(
        "--need-extended-hours-data",
        action="store_true",
        help="Pass needExtendedHoursData=true to Schwab /pricehistory.",
    )
    history_parser.add_argument(
        "--need-previous-close",
        action="store_true",
        help="Pass needPreviousClose=true to Schwab /pricehistory.",
    )
    history_parser.add_argument(
        "--historical-symbol",
        default=None,
        help="One-off Schwab historical symbol override for the internal symbol.",
    )
    history_parser.add_argument(
        "--schwab-config",
        default=None,
        help="Optional JSON config file for explicit Schwab symbol/timeframe mapping.",
    )
    history_parser.add_argument(
        "--token-file",
        default=None,
        help="Optional local token file override.",
    )
    history_parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Strategy config file path. Used for internal timezone and persistence settings.",
    )
    history_parser.add_argument(
        "--persist",
        action="store_true",
        help="Persist normalized bars into the configured SQLite database.",
    )

    quote_parser = subparsers.add_parser(
        "schwab-fetch-quote",
        help="Fetch Schwab /quotes data and normalize it into internal quote results.",
    )
    quote_parser.add_argument("--internal-symbol", required=True, help="Internal strategy symbol, such as MGC.")
    quote_parser.add_argument(
        "--quote-symbol",
        default=None,
        help="One-off Schwab quote symbol override for the internal symbol.",
    )
    quote_parser.add_argument(
        "--schwab-config",
        default=None,
        help="Optional JSON config file for explicit Schwab symbol/timeframe mapping.",
    )
    quote_parser.add_argument("--token-file", default=None, help="Optional local token file override.")
    quote_parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Strategy config file path. Used for internal timezone and internal symbol validation.",
    )

    dashboard_parser = subparsers.add_parser(
        "operator-dashboard",
        help="Run the local operator dashboard for shadow and paper environments.",
    )
    dashboard_parser.add_argument("--host", default="127.0.0.1", help="Dashboard bind host.")
    dashboard_parser.add_argument("--port", type=int, default=8790, help="Preferred dashboard port.")
    dashboard_parser.add_argument(
        "--info-file",
        default=None,
        help="Optional JSON file to write the final bound dashboard URL.",
    )
    dashboard_parser.add_argument(
        "--allow-port-fallback",
        action="store_true",
        help="If the preferred port is unavailable, search upward for the next open port.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and print a small JSON summary."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "replay":
        config_paths = args.config or ["config/base.yaml", "config/replay.yaml"]
        container, _ = bootstrap_service([Path(path) for path in config_paths])
        summary = StrategyServiceRunner(container).run_replay(args.csv)
        print(json.dumps(asdict(summary), sort_keys=True))
        return 0

    if args.command == "research-causal-report":
        config_paths = args.config or ["config/base.yaml", "config/replay.yaml"]
        container, _ = bootstrap_service([Path(path) for path in config_paths])
        bars = container.replay_feed.load_csv(args.csv)
        rows = build_causal_momentum_report(
            bars=bars,
            settings=container.settings,
            smoothing_length=args.smoothing_length,
            normalization_floor=Decimal(args.normalization_floor),
        )
        output_path = write_causal_momentum_report_csv(rows, args.output)
        print(
            json.dumps(
                {
                    "rows": len(rows),
                    "output": str(output_path),
                    "research_only": True,
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "schwab-auth-url":
        auth_config = load_schwab_auth_config_from_env(args.token_file)
        oauth_client = _build_oauth_client(auth_config)
        print(
            json.dumps(
                {
                    "authorize_url": oauth_client.build_authorize_url(args.state, scope=args.scope),
                    "token_file": str(auth_config.token_store_path),
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "schwab-exchange-code":
        auth_config = load_schwab_auth_config_from_env(args.token_file)
        oauth_client = _build_oauth_client(auth_config)
        token_set = oauth_client.exchange_code(args.code)
        print(json.dumps(_json_ready(token_set), sort_keys=True))
        return 0

    if args.command == "schwab-refresh-token":
        auth_config = load_schwab_auth_config_from_env(args.token_file)
        oauth_client = _build_oauth_client(auth_config)
        token_set = oauth_client.refresh_token()
        print(json.dumps(_json_ready(token_set), sort_keys=True))
        return 0

    if args.command == "schwab-fetch-history":
        settings = load_settings_from_files(args.config or ["config/base.yaml", "config/replay.yaml"])
        schwab_config = _load_cli_schwab_config(
            schwab_config_path=args.schwab_config,
            token_file=args.token_file,
            internal_symbol=args.internal_symbol,
            historical_symbol=args.historical_symbol,
            quote_symbol=None,
        )
        repositories = RepositorySet(build_engine(settings.database_url)) if args.persist else None
        adapter = SchwabMarketDataAdapter(settings, schwab_config)
        service = HistoricalBackfillService(
            adapter=adapter,
            client=SchwabHistoricalHttpClient(
                oauth_client=_build_oauth_client(schwab_config.auth),
                market_data_config=schwab_config,
                transport=UrllibJsonTransport(),
            ),
            repositories=repositories,
        )
        bars = service.fetch_bars(
            SchwabHistoricalRequest(
                internal_symbol=args.internal_symbol,
                period_type=args.period_type,
                period=args.period,
                frequency_type=args.frequency_type,
                frequency=args.frequency,
                start_date_ms=args.start_date_ms,
                end_date_ms=args.end_date_ms,
                need_extended_hours_data=args.need_extended_hours_data,
                need_previous_close=args.need_previous_close,
            ),
            internal_timeframe=settings.timeframe,
        )
        print(
            json.dumps(
                {
                    "bar_count": len(bars),
                    "bars": _json_ready(bars),
                    "persisted": bool(args.persist),
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "schwab-fetch-quote":
        settings = load_settings_from_files(args.config or ["config/base.yaml", "config/replay.yaml"])
        schwab_config = _load_cli_schwab_config(
            schwab_config_path=args.schwab_config,
            token_file=args.token_file,
            internal_symbol=args.internal_symbol,
            historical_symbol=None,
            quote_symbol=args.quote_symbol,
        )
        adapter = SchwabMarketDataAdapter(settings, schwab_config)
        service = QuoteService(
            adapter=adapter,
            client=SchwabQuoteHttpClient(
                oauth_client=_build_oauth_client(schwab_config.auth),
                market_data_config=schwab_config,
                transport=UrllibJsonTransport(),
            ),
        )
        quotes = service.fetch_quotes(SchwabQuoteRequest(internal_symbols=(args.internal_symbol,)))
        print(json.dumps({"quotes": _json_ready(quotes)}, sort_keys=True))
        return 0

    if args.command == "operator-dashboard":
        from .operator_dashboard import run_operator_dashboard_server

        run_operator_dashboard_server(
            host=args.host,
            port=args.port,
            info_file=args.info_file,
            allow_port_fallback=args.allow_port_fallback,
        )
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


def _build_oauth_client(auth_config):
    return SchwabOAuthClient(
        config=auth_config,
        transport=UrllibJsonTransport(),
        token_store=SchwabTokenStore(auth_config.token_store_path),
    )


def _load_cli_schwab_config(
    schwab_config_path: str | None,
    token_file: str | None,
    internal_symbol: str,
    historical_symbol: str | None,
    quote_symbol: str | None,
):
    schwab_config = load_schwab_market_data_config(schwab_config_path)
    auth_config = schwab_config.auth
    if token_file is not None:
        auth_config = load_schwab_auth_config_from_env(token_file)
    historical_symbol_map = dict(schwab_config.historical_symbol_map)
    quote_symbol_map = dict(schwab_config.quote_symbol_map)
    if historical_symbol is not None:
        historical_symbol_map[internal_symbol] = historical_symbol
    if quote_symbol is not None:
        quote_symbol_map[internal_symbol] = quote_symbol
    return type(schwab_config)(
        auth=auth_config,
        historical_symbol_map=historical_symbol_map,
        quote_symbol_map=quote_symbol_map,
        timeframe_map=schwab_config.timeframe_map,
        field_map=schwab_config.field_map,
        market_data_base_url=schwab_config.market_data_base_url,
        quotes_symbol_query_param=schwab_config.quotes_symbol_query_param,
    )


def _json_ready(value: Any) -> Any:
    if is_dataclass(value):
        return _json_ready(asdict(value))
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


if __name__ == "__main__":
    raise SystemExit(main())
