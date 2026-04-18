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
from ..market_data import (
    CanonicalMarketDataMaintenanceService,
    DatabentoMarketDataProvider,
    HistoricalBarsRequest,
    HistoricalMarketDataIngestionService,
    QuoteService,
    SchwabHistoricalHttpClient,
    SchwabHistoricalRequest,
    SchwabMarketDataProvider,
    SchwabOAuthClient,
    SchwabQuoteHttpClient,
    SchwabQuoteRequest,
    SchwabTokenStore,
    UrllibJsonTransport,
    load_schwab_auth_config_from_env,
    load_schwab_market_data_config,
)
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet
from ..research import build_causal_momentum_report, write_causal_momentum_report_csv
from ..market_data.schwab_adapter import SchwabMarketDataAdapter
from ..app.bootstrap import bootstrap_service
from .replay_base_preservation import preserve_replay_base
from .probationary_runtime import (
    ProbationaryRuntimeTransportFailure,
    REALIZED_LOSER_SESSION_OVERRIDE_ACTION,
    LIVE_STRATEGY_PILOT_REARM_ACTION,
    build_probationary_paper_runner,
    build_probationary_live_strategy_pilot_runner,
    run_probationary_market_data_transport_probe,
    submit_probationary_operator_control,
)
from .runner import StrategyServiceRunner
from .historical_playback import run_historical_playback
from .headless_supervised_paper import (
    build_headless_supervised_paper_contract,
    write_headless_supervised_paper_artifacts,
)
from .atp_loosened_history_publish import (
    DEFAULT_END_TIMESTAMP as ATP_LOOSENED_DEFAULT_END_TIMESTAMP,
    DEFAULT_REPORT_DIR as ATP_LOOSENED_DEFAULT_REPORT_DIR,
    DEFAULT_SOURCE_DB as ATP_LOOSENED_DEFAULT_SOURCE_DB,
    DEFAULT_START_TIMESTAMP as ATP_LOOSENED_DEFAULT_START_TIMESTAMP,
    run_atp_loosened_history_publish,
)
from .atp_continuous_backfill_publish import (
    DEFAULT_BACKFILL_END_TIMESTAMP as ATP_CONTINUOUS_BACKFILL_DEFAULT_END_TIMESTAMP,
    DEFAULT_BACKFILL_START_TIMESTAMP as ATP_CONTINUOUS_BACKFILL_DEFAULT_BACKFILL_START_TIMESTAMP,
    DEFAULT_HISTORICAL_PLAYBACK_DIR as ATP_CONTINUOUS_BACKFILL_DEFAULT_HISTORICAL_PLAYBACK_DIR,
    DEFAULT_REPORT_DIR as ATP_CONTINUOUS_BACKFILL_DEFAULT_REPORT_DIR,
    DEFAULT_SOURCE_DB as ATP_CONTINUOUS_BACKFILL_DEFAULT_SOURCE_DB,
    DEFAULT_START_TIMESTAMP as ATP_CONTINUOUS_BACKFILL_DEFAULT_START_TIMESTAMP,
    run_atp_continuous_backfill_publish,
)
from .ibkr_milestone_a_check import main as run_ibkr_milestone_a_check
from .strategy_risk_shape_lab import DEFAULT_REPORT_DIR as STRATEGY_RISK_SHAPE_DEFAULT_REPORT_DIR
from .strategy_risk_shape_lab import DEFAULT_HISTORICAL_PLAYBACK_DIR as STRATEGY_RISK_SHAPE_DEFAULT_HISTORICAL_PLAYBACK_DIR
from .strategy_risk_shape_lab import publish_strategy_risk_shaped_studies
from .strategy_risk_shape_lab import run_strategy_risk_shape_lab
from .atp_scope_replay_probe import run_atp_scope_replay_probe
from .atp_scope_replay_probe import publish_atp_scope_replay_probe_study
from .published_strategy_exit_probe import DEFAULT_REPORT_DIR as PUBLISHED_EXIT_PROBE_DEFAULT_REPORT_DIR
from .published_strategy_exit_probe import PublishedExitProbeSpec
from .published_strategy_exit_probe import publish_published_strategy_exit_probe_study
from .published_strategy_exit_probe import run_published_strategy_exit_probe
from .approved_exit_transplant_similarity import DEFAULT_REPORT_DIR as APPROVED_EXIT_TRANSPLANT_SIMILARITY_REPORT_DIR
from .approved_exit_transplant_similarity import run_approved_exit_transplant_similarity
from .paper_engine_silent_failure_audit import DEFAULT_OUTPUT_DIR as PAPER_ENGINE_SILENT_FAILURE_AUDIT_REPORT_DIR
from .paper_engine_silent_failure_audit import run_paper_engine_silent_failure_audit


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

    debug_exchange_parser = subparsers.add_parser(
        "schwab-debug-exchange-refresh",
        help="Run authorization-code exchange plus immediate refresh validation without the bootstrap UI.",
    )
    debug_exchange_parser.add_argument("--code", required=True, help="Authorization code returned by Schwab.")
    debug_exchange_parser.add_argument("--token-file", default=None, help="Optional local token file override.")
    debug_exchange_parser.add_argument(
        "--schwab-config",
        default="config/schwab.local.json",
        help="Optional JSON config file for shared readiness diagnostics.",
    )
    debug_exchange_parser.add_argument(
        "--probe-symbol",
        default="MGC",
        help="Internal symbol to carry through shared readiness diagnostics.",
    )

    local_authorize_proof_parser = subparsers.add_parser(
        "schwab-local-authorize-proof",
        help="Run loopback local authorize plus immediate refresh/probe proof without manual code copy.",
    )
    local_authorize_proof_parser.add_argument("--token-file", default=None, help="Optional local token file override.")
    local_authorize_proof_parser.add_argument(
        "--schwab-config",
        default="config/schwab.local.json",
        help="Optional JSON config file for shared readiness diagnostics.",
    )
    local_authorize_proof_parser.add_argument(
        "--probe-symbol",
        default="MGC",
        help="Internal symbol to carry through shared readiness diagnostics.",
    )
    local_authorize_proof_parser.add_argument("--state", default="mgc-v05l-local", help="Opaque OAuth state value.")
    local_authorize_proof_parser.add_argument("--scope", default=None, help="Optional OAuth scope string.")
    local_authorize_proof_parser.add_argument("--timeout-seconds", type=int, default=180, help="Loopback callback timeout.")

    auth_gate_parser = subparsers.add_parser(
        "schwab-auth-gate",
        help="Validate that the local Schwab token/bootstrap state is ready for paper runtime use.",
    )
    auth_gate_parser.add_argument("--token-file", default=None, help="Optional local token file override.")
    auth_gate_parser.add_argument(
        "--schwab-config",
        default="config/schwab.local.json",
        help="Optional JSON config file for the shared market-data readiness probe.",
    )
    auth_gate_parser.add_argument(
        "--internal-symbol",
        default="MGC",
        help="Internal symbol to use for the shared runtime readiness probe.",
    )

    token_web_parser = subparsers.add_parser(
        "schwab-token-web",
        help="Run the local Schwab token bootstrap web helper.",
    )
    token_web_parser.add_argument("--host", default="127.0.0.1", help="Preferred bind host.")
    token_web_parser.add_argument("--port", type=int, default=8765, help="Preferred bind port.")
    token_web_parser.add_argument("--token-file", default=None, help="Optional local token file override.")
    token_web_parser.add_argument(
        "--info-file",
        default=None,
        help="Optional JSON file to write the final bound bootstrap URL.",
    )
    token_web_parser.add_argument(
        "--port-search-limit",
        type=int,
        default=25,
        help="How many higher ports to probe if the preferred port is unavailable.",
    )
    token_web_parser.add_argument(
        "--schwab-config",
        default="config/schwab.local.json",
        help="Optional JSON config file for the shared market-data readiness probe.",
    )
    token_web_parser.add_argument(
        "--probe-symbol",
        default="MGC",
        help="Internal symbol to use for the shared runtime readiness probe.",
    )
    token_web_parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not auto-open the local bootstrap UI in a browser.",
    )

    history_parser = subparsers.add_parser(
        "schwab-fetch-history",
        help="Fetch Schwab /pricehistory candles and normalize them into internal bars.",
    )
    history_parser.add_argument("--internal-symbol", required=True, help="Internal strategy symbol, such as MGC.")
    history_parser.add_argument(
        "--internal-timeframe",
        default=None,
        help="Optional internal timeframe override, such as 1m. Defaults to the loaded settings timeframe.",
    )
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

    provider_backfill_parser = subparsers.add_parser(
        "market-data-backfill",
        help="Backfill canonical historical bars through a configured market-data provider.",
    )
    provider_backfill_parser.add_argument(
        "--provider",
        required=True,
        choices=["databento", "schwab_market_data"],
        help="Market-data provider to use for the historical backfill.",
    )
    provider_backfill_parser.add_argument(
        "--symbol",
        action="append",
        required=True,
        help="Internal symbol to backfill. May be supplied multiple times.",
    )
    provider_backfill_parser.add_argument(
        "--start",
        required=True,
        help="Inclusive start timestamp in ISO-8601 form.",
    )
    provider_backfill_parser.add_argument(
        "--end",
        default=None,
        help="Exclusive end timestamp in ISO-8601 form.",
    )
    provider_backfill_parser.add_argument(
        "--timeframe",
        default="1m",
        help="Internal timeframe to backfill. Defaults to 1m.",
    )
    provider_backfill_parser.add_argument(
        "--provider-config",
        default=None,
        help="Optional provider-routing JSON config override.",
    )
    provider_backfill_parser.add_argument(
        "--schwab-config",
        default=None,
        help="Optional Schwab market-data config path for the Schwab provider.",
    )
    provider_backfill_parser.add_argument(
        "--allow-canonical-overwrite",
        action="store_true",
        help="Allow incoming historical bars to overwrite existing canonical bars with the same bar_id.",
    )
    provider_backfill_parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Strategy config file path. Used for timezone and replay DB selection.",
    )
    provider_backfill_parser.add_argument(
        "--skip-replay-preservation",
        action="store_true",
        help="Skip the replay-base preservation copy/merge pass for targeted backfill repair runs.",
    )

    historical_playback_parser = subparsers.add_parser(
        "historical-playback",
        help="Run persisted historical playback and emit a manifest plus strategy-study artifacts.",
    )
    historical_playback_parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Config file path. Later files override earlier ones.",
    )
    historical_playback_parser.add_argument("--database", required=True, help="SQLite source database path.")
    historical_playback_parser.add_argument(
        "--symbol",
        action="append",
        required=True,
        help="Internal symbol to replay. May be supplied multiple times.",
    )
    historical_playback_parser.add_argument("--source-timeframe", required=True, help="Persisted source timeframe to load.")
    historical_playback_parser.add_argument("--target-timeframe", default="5m", help="Playback target timeframe.")
    historical_playback_parser.add_argument("--output-dir", required=True, help="Artifact output directory.")
    historical_playback_parser.add_argument("--run-stamp", default=None, help="Optional fixed run stamp.")
    historical_playback_parser.add_argument("--data-source", default=None, help="Optional explicit data source override.")
    historical_playback_parser.add_argument("--start", default=None, help="Optional inclusive ISO timestamp.")
    historical_playback_parser.add_argument("--end", default=None, help="Optional inclusive ISO timestamp.")
    historical_playback_parser.add_argument(
        "--ephemeral-replay-db",
        action="store_true",
        help="Run playback against an in-memory replay DB and emit summary/study artifacts without persisting replay SQLite files.",
    )

    atp_loosened_history_parser = subparsers.add_parser(
        "atp-loosened-history-publish",
        help="Publish loosened ATP historical studies beside the canonical playback catalog.",
    )
    atp_loosened_history_parser.add_argument(
        "--report-dir",
        default=str(ATP_LOOSENED_DEFAULT_REPORT_DIR),
        help="Output directory for the loosened ATP publish report.",
    )
    atp_loosened_history_parser.add_argument(
        "--historical-playback-dir",
        default="./outputs/historical_playback",
        help="Historical playback directory that will receive the merged manifest.",
    )
    atp_loosened_history_parser.add_argument(
        "--start",
        default=ATP_LOOSENED_DEFAULT_START_TIMESTAMP.isoformat(),
        help="Inclusive ISO timestamp for the replay window.",
    )
    atp_loosened_history_parser.add_argument(
        "--end",
        default=ATP_LOOSENED_DEFAULT_END_TIMESTAMP.isoformat(),
        help="Inclusive ISO timestamp for the replay window.",
    )
    atp_loosened_history_parser.add_argument(
        "--study-suffix",
        default="_loosened_v1",
        help="Suffix appended to the published parallel ATP standalone strategy IDs.",
    )
    atp_loosened_history_parser.add_argument(
        "--label-suffix",
        default=" [Loosened v1]",
        help="Suffix appended to published display labels.",
    )
    atp_loosened_history_parser.add_argument(
        "--target-config",
        action="append",
        default=[],
        help="Explicit ATP config path to publish. May be supplied multiple times.",
    )
    atp_loosened_history_parser.add_argument(
        "--source-db",
        action="append",
        default=[str(ATP_LOOSENED_DEFAULT_SOURCE_DB)],
        help="SQLite source DB path. May be supplied multiple times.",
    )
    atp_loosened_history_parser.add_argument(
        "--exit-policy",
        default="fixed_target_time_stop",
        help="ATP replay exit policy for the published studies.",
    )
    atp_loosened_history_parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Optional max worker override for ATP replay publishing. Defaults to a safer low-concurrency path.",
    )
    atp_loosened_history_parser.add_argument(
        "--unsafe-parallel",
        action="store_true",
        help="Allow more aggressive ATP replay parallelism. Safer low-concurrency mode is the default.",
    )

    atp_continuous_backfill_parser = subparsers.add_parser(
        "atp-continuous-backfill-publish",
        help="Publish stitched ATP continuity studies that preserve intact pre-gap history and append repaired gap-window playback.",
    )
    atp_continuous_backfill_parser.add_argument(
        "--report-dir",
        default=str(ATP_CONTINUOUS_BACKFILL_DEFAULT_REPORT_DIR),
        help="Output directory for the ATP continuity backfill report.",
    )
    atp_continuous_backfill_parser.add_argument(
        "--historical-playback-dir",
        default=str(ATP_CONTINUOUS_BACKFILL_DEFAULT_HISTORICAL_PLAYBACK_DIR),
        help="Historical playback directory that will receive the merged continuity studies.",
    )
    atp_continuous_backfill_parser.add_argument(
        "--start",
        default=ATP_CONTINUOUS_BACKFILL_DEFAULT_START_TIMESTAMP.isoformat(),
        help="Inclusive full-history ISO timestamp for rebuilt study bars.",
    )
    atp_continuous_backfill_parser.add_argument(
        "--backfill-start",
        default=ATP_CONTINUOUS_BACKFILL_DEFAULT_BACKFILL_START_TIMESTAMP.isoformat(),
        help="Inclusive ISO timestamp where repaired gap-window evaluation begins.",
    )
    atp_continuous_backfill_parser.add_argument(
        "--end",
        default=ATP_CONTINUOUS_BACKFILL_DEFAULT_END_TIMESTAMP.isoformat(),
        help="Inclusive ISO timestamp where repaired gap-window evaluation ends.",
    )
    atp_continuous_backfill_parser.add_argument(
        "--source-db",
        action="append",
        default=[str(ATP_CONTINUOUS_BACKFILL_DEFAULT_SOURCE_DB)],
        help="SQLite source DB path. May be supplied multiple times.",
    )

    strategy_risk_shape_parser = subparsers.add_parser(
        "strategy-risk-shape-lab",
        help="Replay published strategy-study closed trades under shared session-risk rules.",
    )
    strategy_risk_shape_parser.add_argument(
        "--study-json",
        action="append",
        required=True,
        help="Published strategy-study JSON path. May be supplied multiple times.",
    )
    strategy_risk_shape_parser.add_argument(
        "--report-dir",
        default=str(STRATEGY_RISK_SHAPE_DEFAULT_REPORT_DIR),
        help="Output directory for the reusable risk-shape report.",
    )
    strategy_risk_shape_parser.add_argument(
        "--publish-profile",
        default=None,
        help="Optional built-in risk-shape profile ID to publish as parallel historical playback studies.",
    )
    strategy_risk_shape_parser.add_argument(
        "--historical-playback-dir",
        default=str(STRATEGY_RISK_SHAPE_DEFAULT_HISTORICAL_PLAYBACK_DIR),
        help="Historical playback directory for published risk-shaped studies.",
    )

    canonical_maintenance_parser = subparsers.add_parser(
        "market-data-maintain-canonical",
        help="Audit canonical 1m coverage, repair gaps, and derive higher whole-minute canonical bars.",
    )
    canonical_maintenance_parser.add_argument(
        "--symbol",
        action="append",
        required=True,
        help="Internal symbol to audit/repair/derive. May be supplied multiple times.",
    )
    canonical_maintenance_parser.add_argument(
        "--derive-timeframe",
        action="append",
        default=["5m", "10m"],
        help="Derived timeframe to persist from canonical 1m. Defaults to 5m and 10m.",
    )
    canonical_maintenance_parser.add_argument(
        "--repair-gaps",
        action="store_true",
        help="Backfill detected unexpected gaps through the selected provider.",
    )
    canonical_maintenance_parser.add_argument(
        "--provider",
        default="databento",
        choices=["databento", "schwab_market_data"],
        help="Historical market-data provider to use when repairing gaps.",
    )
    canonical_maintenance_parser.add_argument(
        "--provider-config",
        default=None,
        help="Optional provider-routing JSON config override.",
    )
    canonical_maintenance_parser.add_argument(
        "--schwab-config",
        default=None,
        help="Optional Schwab market-data config path for the Schwab provider.",
    )
    canonical_maintenance_parser.add_argument(
        "--start",
        default=None,
        help="Optional inclusive start timestamp to limit derived timeframe coverage.",
    )
    canonical_maintenance_parser.add_argument(
        "--end",
        default=None,
        help="Optional inclusive end timestamp to limit derived timeframe coverage.",
    )
    canonical_maintenance_parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Strategy config file path. Used for timezone and replay DB selection.",
    )

    paper_soak_parser = subparsers.add_parser(
        "probationary-paper-soak",
        help="Run the probationary paper runtime with optional temporary-paper overlays.",
    )
    paper_soak_parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Config file path. Later files override earlier ones.",
    )
    paper_soak_parser.add_argument(
        "--schwab-config",
        default="config/schwab.local.json",
        help="Optional Schwab market-data config JSON.",
    )
    paper_soak_parser.add_argument(
        "--poll-once",
        action="store_true",
        help="Poll once, process completed bars, and exit.",
    )
    paper_soak_parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="Optional max polling cycles before exit.",
    )

    live_pilot_parser = subparsers.add_parser(
        "probationary-live-strategy-pilot",
        help="Run the tightly gated ATP live-entry pilot against live Schwab broker truth.",
    )
    live_pilot_parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Config file path. Later files override earlier ones.",
    )
    live_pilot_parser.add_argument(
        "--schwab-config",
        default="config/schwab.local.json",
        help="Optional Schwab market-data config JSON.",
    )
    live_pilot_parser.add_argument(
        "--poll-once",
        action="store_true",
        help="Poll once, process completed bars, and exit.",
    )
    live_pilot_parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="Optional max polling cycles before exit.",
    )

    market_data_probe_parser = subparsers.add_parser(
        "probationary-market-data-probe",
        help="Run the shared authenticated Schwab /pricehistory reachability probe used by paper runtime startup.",
    )
    market_data_probe_parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Config file path. Later files override earlier ones.",
    )
    market_data_probe_parser.add_argument(
        "--schwab-config",
        default="config/schwab.local.json",
        help="Optional Schwab market-data config JSON.",
    )

    operator_control_parser = subparsers.add_parser(
        "probationary-operator-control",
        help="Queue a shared operator control action for the probationary paper runtime.",
    )
    operator_control_parser.add_argument(
        "--config",
        action="append",
        default=None,
        help="Config file path. Later files override earlier ones.",
    )
    operator_control_parser.add_argument(
        "--action",
        required=True,
        choices=[
            "halt_entries",
            "resume_entries",
            "clear_fault",
            "clear_risk_halts",
            "flatten_and_halt",
            "stop_after_cycle",
            "force_reconcile",
            LIVE_STRATEGY_PILOT_REARM_ACTION,
            REALIZED_LOSER_SESSION_OVERRIDE_ACTION,
        ],
        help="Shared operator control action to queue.",
    )
    operator_control_parser.add_argument(
        "--lane-id",
        default=None,
        help="Optional lane_id to target a single probationary paper lane.",
    )
    operator_control_parser.add_argument(
        "--payload-json",
        default=None,
        help="Optional JSON object payload merged into the queued operator control request.",
    )
    operator_control_parser.add_argument(
        "--shared-strategy-identity",
        default=None,
        help="Optional shared strategy identity to target a single active paper lane through the shared operator-control path.",
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

    headless_status_parser = subparsers.add_parser(
        "headless-supervised-paper-status",
        help="Build a service-first supervised-paper operability contract without requiring Electron.",
    )
    headless_status_parser.add_argument(
        "--health-file",
        default="outputs/operator_dashboard/runtime/headless_supervised_paper_health.json",
        help="Path to a captured dashboard /health JSON payload.",
    )
    headless_status_parser.add_argument(
        "--startup-control-plane-file",
        default="outputs/operator_dashboard/startup_control_plane_snapshot.json",
        help="Path to the startup control-plane snapshot.",
    )
    headless_status_parser.add_argument(
        "--supervised-operability-file",
        default="outputs/operator_dashboard/supervised_paper_operability_snapshot.json",
        help="Path to the supervised-paper operability snapshot.",
    )
    headless_status_parser.add_argument(
        "--dashboard-info-file",
        default="outputs/operator_dashboard/runtime/operator_dashboard.json",
        help="Path to the dashboard info file.",
    )
    headless_status_parser.add_argument(
        "--output",
        default=None,
        help="Optional JSON path to write the headless supervised-paper contract.",
    )
    headless_status_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional Markdown path to write the headless supervised-paper summary.",
    )

    ibkr_milestone_parser = subparsers.add_parser(
        "ibkr-milestone-a-check",
        help="Evaluate a captured IBKR broker-truth snapshot against Milestone A acceptance checks.",
    )
    ibkr_milestone_parser.add_argument(
        "--snapshot",
        required=True,
        help="Path to a captured IBKR broker-truth snapshot JSON file.",
    )
    ibkr_milestone_parser.add_argument(
        "--output",
        default=None,
        help="Optional path to write the evaluated acceptance result as JSON.",
    )

    atp_scope_probe_parser = subparsers.add_parser(
        "atp-scope-replay-probe",
        help="Replay a cached ATP scope bundle under isolated variant overrides using reduced 1m bar windows.",
    )
    atp_scope_probe_parser.add_argument(
        "--scope-bundle-manifest",
        required=True,
        help="Path to an ATP scope-bundle manifest.json file.",
    )
    atp_scope_probe_parser.add_argument(
        "--exit-policy",
        default="target_checkpoint_no_traction_abort",
        help="Replay exit policy to apply.",
    )
    atp_scope_probe_parser.add_argument(
        "--variant-overrides-json",
        default=None,
        help="Optional JSON object of PatternVariant override fields.",
    )
    atp_scope_probe_parser.add_argument(
        "--pre-confirmation-stop-r-multiple",
        type=float,
        default=None,
        help="Optional tighter stop multiple to apply before confirmation release.",
    )
    atp_scope_probe_parser.add_argument(
        "--pre-confirmation-release-candidate-id",
        default=None,
        help="Optional ATP candidate id whose confirmation releases the tighter first-attempt stop.",
    )
    atp_scope_probe_parser.add_argument(
        "--probe-size-fraction",
        type=float,
        default=1.0,
        help="Optional initial probe size fraction to apply to each replay trade.",
    )
    atp_scope_probe_parser.add_argument(
        "--confirmation-add-candidate-id",
        default=None,
        help="Optional ATP earned-add candidate id to use for confirmation sizing.",
    )
    atp_scope_probe_parser.add_argument(
        "--confirmation-add-size-fraction",
        type=float,
        default=0.0,
        help="Optional add size fraction to earn once the confirmation candidate fires.",
    )
    atp_scope_probe_parser.add_argument(
        "--point-value",
        type=float,
        default=None,
        help="Optional point-value override. Defaults to the scope bundle's point_value.",
    )
    atp_scope_probe_parser.add_argument(
        "--output",
        default=None,
        help="Optional JSON output path.",
    )
    atp_scope_probe_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional Markdown output path.",
    )
    atp_scope_publish_parser = subparsers.add_parser(
        "atp-scope-replay-probe-publish",
        help="Publish a scope-replay probe result as a parallel historical playback study.",
    )
    atp_scope_publish_parser.add_argument(
        "--source-study-json",
        required=True,
        help="Path to the source strategy study JSON to inherit metadata from.",
    )
    atp_scope_publish_parser.add_argument(
        "--scope-bundle-manifest",
        required=True,
        help="Path to an ATP scope-bundle manifest.json file.",
    )
    atp_scope_publish_parser.add_argument(
        "--exit-policy",
        default="target_checkpoint_no_traction_abort",
        help="Replay exit policy to apply.",
    )
    atp_scope_publish_parser.add_argument(
        "--variant-overrides-json",
        default=None,
        help="Optional JSON object of PatternVariant override fields.",
    )
    atp_scope_publish_parser.add_argument(
        "--pre-confirmation-stop-r-multiple",
        type=float,
        default=None,
        help="Optional tighter stop multiple to apply before confirmation release.",
    )
    atp_scope_publish_parser.add_argument(
        "--pre-confirmation-release-candidate-id",
        default=None,
        help="Optional ATP candidate id whose confirmation releases the tighter first-attempt stop.",
    )
    atp_scope_publish_parser.add_argument(
        "--probe-size-fraction",
        type=float,
        default=1.0,
        help="Optional initial probe size fraction to apply to each replay trade.",
    )
    atp_scope_publish_parser.add_argument(
        "--confirmation-add-candidate-id",
        default=None,
        help="Optional ATP earned-add candidate id to use for confirmation sizing.",
    )
    atp_scope_publish_parser.add_argument(
        "--confirmation-add-size-fraction",
        type=float,
        default=0.0,
        help="Optional add size fraction to earn once the confirmation candidate fires.",
    )
    atp_scope_publish_parser.add_argument(
        "--point-value",
        type=float,
        default=None,
        help="Optional point-value override. Defaults to the scope bundle's point_value.",
    )
    atp_scope_publish_parser.add_argument(
        "--study-suffix",
        default="_probe_confirmation_v1",
        help="Suffix for the published strategy id.",
    )
    atp_scope_publish_parser.add_argument(
        "--label-suffix",
        default=" [Probe Confirmation v1]",
        help="Display-name suffix for the published strategy study.",
    )
    published_exit_probe_parser = subparsers.add_parser(
        "published-strategy-exit-probe",
        help="Replay alternate exits against already-published strategy studies using embedded bars and frozen entries.",
    )
    published_exit_probe_parser.add_argument(
        "--study-json",
        action="append",
        required=True,
        help="Published strategy-study JSON path. May be supplied multiple times.",
    )
    published_exit_probe_parser.add_argument(
        "--report-dir",
        default=str(PUBLISHED_EXIT_PROBE_DEFAULT_REPORT_DIR),
        help="Output directory for the exit-probe report.",
    )
    published_exit_probe_parser.add_argument("--checkpoint-arm-r", type=float, default=0.80, help="MFE threshold, in risk units, required to arm the checkpoint trail.")
    published_exit_probe_parser.add_argument("--checkpoint-lock-r", type=float, default=0.35, help="Locked-in profit stop, in risk units, after the checkpoint arms.")
    published_exit_probe_parser.add_argument("--checkpoint-trail-r", type=float, default=0.25, help="Structure trail distance, in risk units, after the checkpoint arms.")
    published_exit_probe_parser.add_argument("--no-traction-abort-bars", type=int, default=2, help="Abort a trade this many bars in if it still has not shown enough favorable excursion.")
    published_exit_probe_parser.add_argument("--no-traction-min-favorable-r", type=float, default=0.25, help="Minimum favorable excursion, in risk units, required to avoid the no-traction abort.")
    published_exit_probe_parser.add_argument("--risk-lookback-bars", type=int, default=5, help="Lookback bars used to estimate per-trade risk from the embedded bars.")
    published_exit_probe_parser.add_argument("--risk-range-floor", type=float, default=0.25, help="Minimum risk range in raw price units when the embedded bar range is tiny.")
    published_exit_publish_parser = subparsers.add_parser(
        "published-strategy-exit-probe-publish",
        help="Publish an exit-only probe result as a parallel historical playback study.",
    )
    published_exit_publish_parser.add_argument(
        "--source-study-json",
        required=True,
        help="Published strategy-study JSON path to inherit metadata and bars from.",
    )
    published_exit_publish_parser.add_argument(
        "--report-dir",
        default=str(PUBLISHED_EXIT_PROBE_DEFAULT_REPORT_DIR),
        help="Output directory for publish artifacts.",
    )
    published_exit_publish_parser.add_argument(
        "--study-suffix",
        default="_checkpoint_no_traction_v1",
        help="Suffix for the published strategy id.",
    )
    published_exit_publish_parser.add_argument(
        "--label-suffix",
        default=" [Checkpoint + No-Traction v1]",
        help="Display-name suffix for the published strategy study.",
    )
    published_exit_publish_parser.add_argument("--checkpoint-arm-r", type=float, default=0.80, help="MFE threshold, in risk units, required to arm the checkpoint trail.")
    published_exit_publish_parser.add_argument("--checkpoint-lock-r", type=float, default=0.35, help="Locked-in profit stop, in risk units, after the checkpoint arms.")
    published_exit_publish_parser.add_argument("--checkpoint-trail-r", type=float, default=0.25, help="Structure trail distance, in risk units, after the checkpoint arms.")
    published_exit_publish_parser.add_argument("--no-traction-abort-bars", type=int, default=2, help="Abort a trade this many bars in if it still has not shown enough favorable excursion.")
    published_exit_publish_parser.add_argument("--no-traction-min-favorable-r", type=float, default=0.25, help="Minimum favorable excursion, in risk units, required to avoid the no-traction abort.")
    published_exit_publish_parser.add_argument("--risk-lookback-bars", type=int, default=5, help="Lookback bars used to estimate per-trade risk from the embedded bars.")
    published_exit_publish_parser.add_argument("--risk-range-floor", type=float, default=0.25, help="Minimum risk range in raw price units when the embedded bar range is tiny.")
    approved_exit_similarity_parser = subparsers.add_parser(
        "approved-exit-transplant-similarity",
        help="Rank approved non-ATP lanes by structural similarity to a target lane for exit-only transplant review.",
    )
    approved_exit_similarity_parser.add_argument(
        "--target-branch",
        required=True,
        help="Exact approved branch label to use as the similarity anchor.",
    )
    approved_exit_similarity_parser.add_argument(
        "--report-dir",
        default=str(APPROVED_EXIT_TRANSPLANT_SIMILARITY_REPORT_DIR),
        help="Output directory for the similarity report.",
    )
    paper_engine_silent_failure_audit_parser = subparsers.add_parser(
        "paper-engine-silent-failure-audit",
        help="Audit live paper lanes from eligibility through persistence and dashboard rendering freshness.",
    )
    paper_engine_silent_failure_audit_parser.add_argument(
        "--report-dir",
        default=str(PAPER_ENGINE_SILENT_FAILURE_AUDIT_REPORT_DIR),
        help="Output directory for the paper-engine audit report.",
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

    if args.command == "schwab-debug-exchange-refresh":
        from .schwab_token_bootstrap_web import SchwabTokenBootstrapService

        payload = SchwabTokenBootstrapService(
            token_file=args.token_file,
            schwab_config_path=args.schwab_config,
            probe_symbol=args.probe_symbol,
        ).debug_exchange_refresh(args.code)
        print(json.dumps(_json_ready(payload), sort_keys=True))
        return 0

    if args.command == "schwab-local-authorize-proof":
        from .schwab_token_bootstrap_web import SchwabTokenBootstrapService

        payload = SchwabTokenBootstrapService(
            token_file=args.token_file,
            schwab_config_path=args.schwab_config,
            probe_symbol=args.probe_symbol,
        ).local_authorize_proof(
            state=args.state,
            scope=args.scope,
            timeout_seconds=args.timeout_seconds,
        )
        print(json.dumps(_json_ready(payload), sort_keys=True))
        return 0

    if args.command == "schwab-auth-gate":
        from .schwab_token_bootstrap_web import SchwabTokenBootstrapService

        try:
            payload = SchwabTokenBootstrapService(
                token_file=args.token_file,
                schwab_config_path=args.schwab_config,
                probe_symbol=args.internal_symbol,
            ).check_runtime_ready()
        except Exception as exc:  # pragma: no cover - CLI guardrail
            print(
                json.dumps(
                    {
                        "runtime_ready": False,
                        "error": str(exc),
                        "probe_symbol": args.internal_symbol,
                        "schwab_config_path": str(Path(args.schwab_config).resolve(strict=False)),
                        "token_file": args.token_file,
                    },
                    sort_keys=True,
                )
            )
            return 1
        print(json.dumps(_json_ready(payload), sort_keys=True))
        return 0

    if args.command == "schwab-token-web":
        from .schwab_token_bootstrap_web import run_schwab_token_bootstrap_server

        result = run_schwab_token_bootstrap_server(
            host=args.host,
            port=args.port,
            token_file=args.token_file,
            open_browser=not args.no_browser,
            info_file=args.info_file,
            port_search_limit=args.port_search_limit,
            schwab_config_path=args.schwab_config,
            probe_symbol=args.probe_symbol,
        )
        print(json.dumps(_json_ready(result), sort_keys=True))
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
            canonical_maintenance=CanonicalMarketDataMaintenanceService(database_url=settings.database_url) if args.persist else None,
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
            internal_timeframe=args.internal_timeframe or settings.timeframe,
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

    if args.command == "market-data-backfill":
        try:
            settings = load_settings_from_files(args.config or ["config/base.yaml", "config/replay.yaml"])
            preservation = None
            if not bool(args.skip_replay_preservation):
                preserve_replay_base()
            ingestion = HistoricalMarketDataIngestionService(
                database_url=settings.database_url,
                provider_config_path=args.provider_config,
            )
            provider = _build_market_data_provider(
                provider_name=args.provider,
                settings=settings,
                repo_root=Path.cwd(),
                provider_config_path=args.provider_config,
                schwab_config_path=args.schwab_config,
            )
            start = _parse_cli_datetime(args.start, settings)
            end = _parse_cli_datetime(args.end, settings) if args.end is not None else None
            audits = []
            for symbol in args.symbol:
                audits.append(
                    ingestion.ingest(
                        provider=provider,
                        request=HistoricalBarsRequest(
                            internal_symbol=symbol.strip().upper(),
                            timeframe=args.timeframe,
                            start=start,
                            end=end,
                        ),
                        allow_canonical_overwrite=args.allow_canonical_overwrite,
                    )
                )
            if not bool(args.skip_replay_preservation):
                preservation = preserve_replay_base()
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "provider": args.provider,
                        "symbols": [str(item).strip().upper() for item in args.symbol],
                        "timeframe": args.timeframe,
                        "start": args.start,
                        "end": args.end,
                        "error": str(exc),
                    },
                    sort_keys=True,
                )
            )
            return 1
        print(
            json.dumps(
                {
                    "provider": args.provider,
                    "symbol_count": len(audits),
                    "symbols": [audit.internal_symbol for audit in audits],
                    "audits": _json_ready(audits),
                    "skip_replay_preservation": bool(args.skip_replay_preservation),
                    "replay_base_preservation": _json_ready(preservation),
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "historical-playback":
        settings = load_settings_from_files(args.config or ["config/base.yaml", "config/replay.yaml"])
        result = run_historical_playback(
            config_paths=args.config or ["config/base.yaml", "config/replay.yaml"],
            source_db_path=args.database,
            symbols=[str(item).strip().upper() for item in args.symbol],
            source_timeframe=args.source_timeframe,
            target_timeframe=args.target_timeframe,
            start_timestamp=_parse_cli_datetime(args.start, settings) if args.start is not None else None,
            end_timestamp=_parse_cli_datetime(args.end, settings) if args.end is not None else None,
            output_dir=args.output_dir,
            data_source=args.data_source,
            run_stamp=args.run_stamp,
            persist_replay_db=not bool(args.ephemeral_replay_db),
        )
        print(json.dumps(_json_ready(result), sort_keys=True))
        return 0

    if args.command == "atp-loosened-history-publish":
        result = run_atp_loosened_history_publish(
            report_dir=Path(args.report_dir),
            historical_playback_dir=Path(args.historical_playback_dir),
            start_timestamp=datetime.fromisoformat(args.start),
            end_timestamp=datetime.fromisoformat(args.end),
            study_suffix=str(args.study_suffix),
            label_suffix=str(args.label_suffix),
            target_configs=[Path(path) for path in args.target_config] if args.target_config else None,
            source_database_paths=[Path(path) for path in args.source_db],
            exit_policy=str(args.exit_policy),
            safe_mode=not bool(args.unsafe_parallel),
            max_workers=args.max_workers,
        )
        print(json.dumps(_json_ready(result), sort_keys=True))
        return 0

    if args.command == "atp-continuous-backfill-publish":
        result = run_atp_continuous_backfill_publish(
            report_dir=Path(args.report_dir),
            historical_playback_dir=Path(args.historical_playback_dir),
            source_database_paths=[Path(path) for path in args.source_db],
            full_start_timestamp=datetime.fromisoformat(args.start),
            backfill_start_timestamp=datetime.fromisoformat(args.backfill_start),
            backfill_end_timestamp=datetime.fromisoformat(args.end),
        )
        print(json.dumps(_json_ready(result), sort_keys=True))
        return 0

    if args.command == "strategy-risk-shape-lab":
        if args.publish_profile:
            result = publish_strategy_risk_shaped_studies(
                study_json_paths=[Path(path) for path in args.study_json],
                profile_id=str(args.publish_profile),
                report_dir=Path(args.report_dir),
                historical_playback_dir=Path(args.historical_playback_dir),
            )
        else:
            result = run_strategy_risk_shape_lab(
                study_json_paths=[Path(path) for path in args.study_json],
                report_dir=Path(args.report_dir),
            )
        print(json.dumps(_json_ready(result), sort_keys=True))
        return 0

    if args.command == "market-data-maintain-canonical":
        try:
            settings = load_settings_from_files(args.config or ["config/base.yaml", "config/replay.yaml"])
            preserve_replay_base()
            maintenance = CanonicalMarketDataMaintenanceService(
                database_url=settings.database_url,
                provider_config_path=args.provider_config,
            )
            start = _parse_cli_datetime(args.start, settings) if args.start is not None else None
            end = _parse_cli_datetime(args.end, settings) if args.end is not None else None
            repair_provider = None
            if args.repair_gaps:
                repair_provider = _build_market_data_provider(
                    provider_name=args.provider,
                    settings=settings,
                    repo_root=Path.cwd(),
                    provider_config_path=args.provider_config,
                    schwab_config_path=args.schwab_config,
                )
            coverage_audits = []
            repair_results = []
            derivation_audits = []
            for symbol in args.symbol:
                normalized_symbol = symbol.strip().upper()
                coverage_audits.append(
                    maintenance.audit_coverage(symbol=normalized_symbol)
                )
                if repair_provider is not None:
                    repair_results.append(
                        maintenance.backfill_detected_gaps(
                            provider=repair_provider,
                            symbol=normalized_symbol,
                        )
                    )
                    coverage_audits[-1] = maintenance.audit_coverage(symbol=normalized_symbol)
                for timeframe in {str(item).strip().lower() for item in args.derive_timeframe if str(item).strip()}:
                    derivation_audits.append(
                        maintenance.derive_timeframe(
                            symbol=normalized_symbol,
                            target_timeframe=timeframe,
                            start=start,
                            end=end,
                        )
                    )
            preservation = preserve_replay_base()
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "symbols": [str(item).strip().upper() for item in args.symbol],
                        "repair_gaps": bool(args.repair_gaps),
                        "provider": args.provider,
                        "error": str(exc),
                    },
                    sort_keys=True,
                )
            )
            return 1
        print(
            json.dumps(
                {
                    "symbols": [str(item).strip().upper() for item in args.symbol],
                    "coverage_audits": _json_ready(coverage_audits),
                    "repair_results": _json_ready(repair_results),
                    "derivation_audits": _json_ready(derivation_audits),
                    "replay_base_preservation": _json_ready(preservation),
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "probationary-paper-soak":
        config_paths = args.config or [
            "config/base.yaml",
            "config/live.yaml",
            "config/probationary_pattern_engine.yaml",
            "config/probationary_pattern_engine_paper.yaml",
        ]
        try:
            runner = build_probationary_paper_runner(
                [Path(path) for path in config_paths],
                schwab_config_path=args.schwab_config,
            )
            summary = runner.run(poll_once=args.poll_once, max_cycles=args.max_cycles)
        except ProbationaryRuntimeTransportFailure as exc:
            print(json.dumps(_json_ready(exc.payload), sort_keys=True))
            return 1
        print(json.dumps(_json_ready(summary), sort_keys=True))
        return 0

    if args.command == "probationary-live-strategy-pilot":
        config_paths = args.config or [
            "config/base.yaml",
            "config/live.yaml",
            "config/probationary_pattern_engine.yaml",
            "config/probationary_pattern_engine_live_atp_companion_v1_gc_asia_us_pilot.yaml",
        ]
        try:
            runner = build_probationary_live_strategy_pilot_runner(
                [Path(path) for path in config_paths],
                schwab_config_path=args.schwab_config,
            )
            summary = runner.run(poll_once=args.poll_once, max_cycles=args.max_cycles)
        except ProbationaryRuntimeTransportFailure as exc:
            print(json.dumps(_json_ready(exc.payload), sort_keys=True))
            return 1
        print(json.dumps(_json_ready(summary), sort_keys=True))
        return 0

    if args.command == "probationary-market-data-probe":
        config_paths = args.config or [
            "config/base.yaml",
            "config/live.yaml",
            "config/probationary_pattern_engine.yaml",
            "config/probationary_pattern_engine_paper.yaml",
        ]
        try:
            payload = run_probationary_market_data_transport_probe(
                [Path(path) for path in config_paths],
                schwab_config_path=args.schwab_config,
            )
        except ProbationaryRuntimeTransportFailure as exc:
            print(json.dumps(_json_ready(exc.payload), sort_keys=True))
            return 1
        print(json.dumps(_json_ready(payload), sort_keys=True))
        return 0

    if args.command == "probationary-operator-control":
        config_paths = args.config or [
            "config/base.yaml",
            "config/live.yaml",
            "config/probationary_pattern_engine.yaml",
            "config/probationary_pattern_engine_paper.yaml",
        ]
        control_payload = json.loads(args.payload_json) if args.payload_json is not None else {}
        if args.lane_id:
            control_payload["lane_id"] = args.lane_id
        summary = submit_probationary_operator_control(
            [Path(path) for path in config_paths],
            args.action,
            payload=control_payload or None,
            shared_strategy_identity=args.shared_strategy_identity,
        )
        print(json.dumps(_json_ready(summary), sort_keys=True))
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

    if args.command == "headless-supervised-paper-status":
        def _read_optional_json(path_value: str | None) -> dict[str, Any]:
            if not path_value:
                return {}
            path = Path(path_value)
            if not path.exists():
                return {}
            return json.loads(path.read_text(encoding="utf-8"))

        contract = build_headless_supervised_paper_contract(
            health_payload=_read_optional_json(args.health_file),
            startup_control_plane=_read_optional_json(args.startup_control_plane_file),
            supervised_paper_operability=_read_optional_json(args.supervised_operability_file),
            dashboard_info=_read_optional_json(args.dashboard_info_file),
        )
        write_headless_supervised_paper_artifacts(
            contract=contract,
            output_path=args.output,
            markdown_path=args.markdown_output,
        )
        print(json.dumps(_json_ready(contract), sort_keys=True))
        return 0

    if args.command == "ibkr-milestone-a-check":
        result = run_ibkr_milestone_a_check(
            [
                "--snapshot",
                args.snapshot,
                *([] if not args.output else ["--output", args.output]),
            ]
        )
        print(json.dumps(_json_ready(result), sort_keys=True))
        return 0

    if args.command == "atp-scope-replay-probe":
        result = run_atp_scope_replay_probe(
            scope_bundle_manifest=Path(args.scope_bundle_manifest),
            exit_policy=str(args.exit_policy or "").strip() or "target_checkpoint_no_traction_abort",
            variant_overrides=(
                json.loads(args.variant_overrides_json)
                if args.variant_overrides_json
                else None
            ),
            pre_confirmation_stop_r_multiple=args.pre_confirmation_stop_r_multiple,
            pre_confirmation_release_candidate_id=(
                str(args.pre_confirmation_release_candidate_id).strip()
                if args.pre_confirmation_release_candidate_id
                else None
            ),
            probe_size_fraction=float(args.probe_size_fraction),
            confirmation_add_candidate_id=(
                str(args.confirmation_add_candidate_id).strip()
                if args.confirmation_add_candidate_id
                else None
            ),
            confirmation_add_size_fraction=float(args.confirmation_add_size_fraction),
            point_value_override=args.point_value,
            output_path=Path(args.output) if args.output else None,
            markdown_output_path=Path(args.markdown_output) if args.markdown_output else None,
        )
        print(json.dumps(_json_ready(result), sort_keys=True))
        return 0

    if args.command == "atp-scope-replay-probe-publish":
        result = publish_atp_scope_replay_probe_study(
            source_study_json_path=Path(args.source_study_json),
            scope_bundle_manifest=Path(args.scope_bundle_manifest),
            exit_policy=str(args.exit_policy or "").strip() or "target_checkpoint_no_traction_abort",
            variant_overrides=(
                json.loads(args.variant_overrides_json)
                if args.variant_overrides_json
                else None
            ),
            pre_confirmation_stop_r_multiple=args.pre_confirmation_stop_r_multiple,
            pre_confirmation_release_candidate_id=(
                str(args.pre_confirmation_release_candidate_id).strip()
                if args.pre_confirmation_release_candidate_id
                else None
            ),
            probe_size_fraction=float(args.probe_size_fraction),
            confirmation_add_candidate_id=(
                str(args.confirmation_add_candidate_id).strip()
                if args.confirmation_add_candidate_id
                else None
            ),
            confirmation_add_size_fraction=float(args.confirmation_add_size_fraction),
            point_value_override=args.point_value,
            study_suffix=str(args.study_suffix or "").strip() or "_probe_confirmation_v1",
            label_suffix=str(args.label_suffix or "").strip() or " [Probe Confirmation v1]",
        )
        print(json.dumps(_json_ready(result), sort_keys=True))
        return 0

    if args.command == "published-strategy-exit-probe":
        result = run_published_strategy_exit_probe(
            study_json_paths=[Path(path) for path in (args.study_json or [])],
            report_dir=Path(args.report_dir),
            spec=PublishedExitProbeSpec(
                checkpoint_arm_r=float(args.checkpoint_arm_r),
                checkpoint_lock_r=float(args.checkpoint_lock_r),
                checkpoint_trail_r=float(args.checkpoint_trail_r),
                no_traction_abort_bars=int(args.no_traction_abort_bars),
                no_traction_min_favorable_r=float(args.no_traction_min_favorable_r),
                risk_lookback_bars=int(args.risk_lookback_bars),
                risk_range_floor=float(args.risk_range_floor),
            ),
        )
        print(json.dumps(_json_ready(result), sort_keys=True))
        return 0

    if args.command == "published-strategy-exit-probe-publish":
        result = publish_published_strategy_exit_probe_study(
            source_study_json_path=Path(args.source_study_json),
            report_dir=Path(args.report_dir),
            study_suffix=str(args.study_suffix or "").strip() or "_checkpoint_no_traction_v1",
            label_suffix=str(args.label_suffix or "").strip() or " [Checkpoint + No-Traction v1]",
            spec=PublishedExitProbeSpec(
                checkpoint_arm_r=float(args.checkpoint_arm_r),
                checkpoint_lock_r=float(args.checkpoint_lock_r),
                checkpoint_trail_r=float(args.checkpoint_trail_r),
                no_traction_abort_bars=int(args.no_traction_abort_bars),
                no_traction_min_favorable_r=float(args.no_traction_min_favorable_r),
                risk_lookback_bars=int(args.risk_lookback_bars),
                risk_range_floor=float(args.risk_range_floor),
            ),
        )
        print(json.dumps(_json_ready(result), sort_keys=True))
        return 0

    if args.command == "approved-exit-transplant-similarity":
        result = run_approved_exit_transplant_similarity(
            target_branch=str(args.target_branch),
            report_dir=Path(args.report_dir),
        )
        print(json.dumps(_json_ready(result), sort_keys=True))
        return 0

    if args.command == "paper-engine-silent-failure-audit":
        result = run_paper_engine_silent_failure_audit(
            output_dir=Path(args.report_dir),
        )
        print(json.dumps(_json_ready(result), sort_keys=True))
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


def _build_market_data_provider(
    *,
    provider_name: str,
    settings,
    repo_root: Path,
    provider_config_path: str | None,
    schwab_config_path: str | None,
):
    if provider_name == "databento":
        return DatabentoMarketDataProvider(
            settings,
            repo_root=repo_root,
            config_path=provider_config_path,
        )
    if provider_name == "schwab_market_data":
        return SchwabMarketDataProvider(
            settings,
            repo_root=repo_root,
            provider_config_path=provider_config_path,
            schwab_config_path=schwab_config_path,
        )
    raise ValueError(f"Unsupported market-data provider: {provider_name}")


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


def _parse_cli_datetime(raw_value: str, settings) -> datetime:
    parsed = datetime.fromisoformat(raw_value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=settings.timezone_info)
    return parsed.astimezone(settings.timezone_info)


if __name__ == "__main__":
    raise SystemExit(main())
