"""CLI for the service-ready Track B SHADOW monitor."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .track_b_shadow_monitor import (
    DEFAULT_BACKEND_HEALTH_JSON,
    DEFAULT_CURRENT_QUOTE_REPORT_JSON,
    DEFAULT_LOCKFILE,
    DEFAULT_PIDFILE,
    DEFAULT_TRACK_B_SHADOW_MONITOR_OUTPUT_ROOT,
    TrackBRuntimeDataSource,
    TrackBShadowMonitorConfig,
    run_track_b_shadow_monitor,
)
from .track_b_databento_live_runtime_feed import DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the service-ready Track B monitor. SHADOW is no-submit; PAPER requires explicit "
            "--enable-paper-trading and --paper-on-signal and delegates only through the guarded Track B lifecycle."
        )
    )
    parser.add_argument("--mode", default="SHADOW", choices=["SHADOW", "PAPER"])
    parser.add_argument("--max-cycles", type=int, default=999)
    parser.add_argument("--once", action="store_true", help="Run exactly one monitor cycle.")
    parser.add_argument("--poll-seconds", type=float, default=15.0)
    parser.add_argument("--data-refresh-seconds", type=float, default=60.0)
    parser.add_argument("--max-backoff-seconds", type=float, default=300.0)
    parser.add_argument("--max-consecutive-failures", type=int)
    parser.add_argument("--expected-account-id", default="DUM882026")
    parser.add_argument("--account-id", default="DUM882026")
    parser.add_argument("--enable-paper-trading", action="store_true")
    parser.add_argument("--paper-on-signal", action="store_true")
    parser.add_argument("--max-paper-trades-per-run", type=int, default=1)
    parser.add_argument("--pause-after-paper-trade", choices=["true", "false"], default="true")
    parser.add_argument("--quantity", type=int)
    parser.add_argument(
        "--paper-order-pricing-policy",
        choices=["MARKETABLE_LIMIT_FROM_LIVE_CONTEXT", "LIMIT_AT_LAST", "LIMIT_AT_SIGNAL_PRICE", "MANUAL_LIMIT_PRICES"],
        default="MARKETABLE_LIMIT_FROM_LIVE_CONTEXT",
    )
    parser.add_argument("--paper-order-price-offset-ticks", type=int, default=2)
    parser.add_argument("--paper-exit-price-offset-ticks", type=int, default=2)
    parser.add_argument("--manual-open-limit-price")
    parser.add_argument("--manual-close-limit-price")
    parser.add_argument("--tick-size", default="0.1")
    parser.add_argument("--con-id", type=int, default=712565978)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7497)
    parser.add_argument("--client-id", type=int, default=17086)
    parser.add_argument("--contract-key", default="MGC-202606")
    parser.add_argument("--local-symbol", default="MGCM6")
    parser.add_argument("--databento-continuous-symbol", default="MGC.v.0")
    parser.add_argument("--dataset", default="GLBX.MDP3")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--lookback-minutes", type=int, default=60)
    parser.add_argument("--max-bars", type=int, default=90)
    parser.add_argument("--min-bars", type=int, default=8)
    parser.add_argument("--provider-timeout-seconds", "--provider-fetch-timeout-seconds", dest="provider_timeout_seconds", type=float, default=20.0)
    parser.add_argument("--provider-transport", choices=["native", "http"], default="http")
    parser.add_argument("--stype-out", default="instrument_id")
    parser.add_argument(
        "--runtime-data-source",
        "--runtime-decision-source",
        dest="runtime_data_source",
        choices=[item.value for item in TrackBRuntimeDataSource],
        default=TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT.value,
        help="Live artifact is the default execution-runtime source. HTTP is explicit backfill/recovery only.",
    )
    parser.add_argument("--live-runtime-feed-output-root", type=Path, default=DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT)
    parser.add_argument("--manage-live-feed", choices=["true", "false"], default="true")
    parser.add_argument("--live-feed-warmup-timeout-seconds", type=float, default=3600.0)
    parser.add_argument("--leave-live-feed-running", action="store_true")
    parser.add_argument("--force-stop-owned-feed", action="store_true")
    parser.add_argument("--live-feed-restart-backoff-seconds", type=float, default=60.0)
    parser.add_argument("--live-feed-max-records", type=int, default=1_000_000)
    parser.add_argument("--live-feed-max-seconds", type=float, default=86400.0)
    parser.add_argument("--live-feed-min-bars", type=int, default=40)
    parser.add_argument("--use-continuous-symbol-for-runtime-fetch", action="store_true")
    parser.add_argument("--disable-fresh-runtime-artifact-fallback", action="store_true")
    parser.add_argument("--max-latest-1m-age-seconds", type=int, default=900)
    parser.add_argument("--max-completed-5m-age-seconds", type=int, default=900)
    parser.add_argument("--current-quote-report-json", type=Path, default=DEFAULT_CURRENT_QUOTE_REPORT_JSON)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--base-url", default="https://hist.databento.com/v0")
    parser.add_argument("--stype-in", default="continuous")
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--source-id", default="track_b_shadow_monitor")
    parser.add_argument("--inbox-dir", type=Path, default=Path("examples/track_b_shadow_listener/inbox"))
    parser.add_argument("--output-root", type=Path, default=DEFAULT_TRACK_B_SHADOW_MONITOR_OUTPUT_ROOT)
    parser.add_argument(
        "--runtime-candle-capture-output-root",
        type=Path,
        default=Path("outputs/track_b_execution_core/track_b_runtime_candle_capture"),
    )
    parser.add_argument(
        "--asian-drift-output-root",
        type=Path,
        default=Path("outputs/track_b_execution_core/asian_drift_state"),
    )
    parser.add_argument(
        "--snap-turn-output-root",
        type=Path,
        default=Path("outputs/track_b_execution_core/snap_turn_state"),
    )
    parser.add_argument(
        "--session-strategy-output-root",
        type=Path,
        default=Path("outputs/track_b_execution_core/session_strategy_state"),
    )
    parser.add_argument(
        "--multi-strategy-output-root",
        type=Path,
        default=Path("outputs/track_b_execution_core/track_b_multi_strategy_runtime_cycle"),
    )
    parser.add_argument("--operator-status-output-root", type=Path, default=Path("outputs/track_b_execution_core/operator_status"))
    parser.add_argument("--backend-health-json", type=Path, default=DEFAULT_BACKEND_HEALTH_JSON)
    parser.add_argument("--lockfile", type=Path, default=DEFAULT_LOCKFILE)
    parser.add_argument("--pidfile", type=Path, default=DEFAULT_PIDFILE)
    parser.add_argument("--update-operator-status", action="store_true")
    parser.add_argument("--stop-on-error", action="store_true")
    parser.add_argument("--force-takeover", action="store_true")
    parser.add_argument("--retention-cycles", type=int, default=20)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    actual_argv = list(argv or [])
    args = build_parser().parse_args(actual_argv)
    result = run_track_b_shadow_monitor(
        config=TrackBShadowMonitorConfig(
            mode=args.mode,
            max_cycles=1 if args.once else args.max_cycles,
            poll_seconds=args.poll_seconds,
            data_refresh_seconds=args.data_refresh_seconds,
            max_backoff_seconds=args.max_backoff_seconds,
            max_consecutive_failures=args.max_consecutive_failures,
            expected_account_id=args.expected_account_id,
            account_id=args.account_id,
            enable_paper_trading=args.enable_paper_trading,
            paper_on_signal=args.paper_on_signal,
            max_paper_trades_per_run=args.max_paper_trades_per_run,
            pause_after_paper_trade=args.pause_after_paper_trade == "true",
            quantity=args.quantity,
            paper_order_pricing_policy=args.paper_order_pricing_policy,
            paper_order_price_offset_ticks=args.paper_order_price_offset_ticks,
            paper_exit_price_offset_ticks=args.paper_exit_price_offset_ticks,
            manual_open_limit_price=args.manual_open_limit_price,
            manual_close_limit_price=args.manual_close_limit_price,
            tick_size=args.tick_size,
            con_id=args.con_id,
            host=args.host,
            port=args.port,
            client_id=args.client_id,
            contract_key=args.contract_key,
            local_symbol=args.local_symbol,
            databento_continuous_symbol=args.databento_continuous_symbol,
            dataset=args.dataset,
            timeframe=args.timeframe,
            lookback_minutes=args.lookback_minutes,
            max_bars=args.max_bars,
            min_bars=args.min_bars,
            provider_timeout_seconds=args.provider_timeout_seconds,
            provider_transport=args.provider_transport,
            provider_stype_out=args.stype_out,
            runtime_data_source=args.runtime_data_source,
            live_runtime_feed_output_root=args.live_runtime_feed_output_root,
            manage_live_feed=args.manage_live_feed == "true",
            live_feed_warmup_timeout_seconds=args.live_feed_warmup_timeout_seconds,
            leave_live_feed_running=args.leave_live_feed_running,
            force_stop_owned_feed=args.force_stop_owned_feed,
            live_feed_restart_backoff_seconds=args.live_feed_restart_backoff_seconds,
            live_feed_max_records=args.live_feed_max_records,
            live_feed_max_seconds=args.live_feed_max_seconds,
            live_feed_min_bars=args.live_feed_min_bars,
            prefer_raw_local_symbol_for_runtime_fetch=not args.use_continuous_symbol_for_runtime_fetch,
            allow_fresh_runtime_artifact_fallback=not args.disable_fresh_runtime_artifact_fallback,
            max_latest_1m_age_seconds=args.max_latest_1m_age_seconds,
            max_completed_5m_age_seconds=args.max_completed_5m_age_seconds,
            current_quote_report_json=args.current_quote_report_json,
            env_file=args.env_file,
            base_url=args.base_url,
            stype_in=args.stype_in,
            schema=args.schema,
            source_id=args.source_id,
            inbox_dir=args.inbox_dir,
            output_root=args.output_root,
            runtime_candle_capture_output_root=args.runtime_candle_capture_output_root,
            asian_drift_output_root=args.asian_drift_output_root,
            snap_turn_output_root=args.snap_turn_output_root,
            session_strategy_output_root=args.session_strategy_output_root,
            multi_strategy_output_root=args.multi_strategy_output_root,
            operator_status_output_root=args.operator_status_output_root,
            backend_health_json=args.backend_health_json,
            lockfile=args.lockfile,
            pidfile=args.pidfile,
            update_operator_status=args.update_operator_status,
            stop_on_error=args.stop_on_error,
            force_takeover=args.force_takeover,
            retention_cycles=args.retention_cycles,
            command=(sys.executable, "-m", "mgc_v05l.execution_core.track_b_shadow_monitor_cli", *actual_argv),
            repo_root=Path.cwd(),
        )
    )
    print(
        json.dumps(
            {
                "monitor_verdict": result.report["monitor_verdict"],
                "monitor_mode": result.report.get("monitor_mode"),
                "cycle_id": result.report["cycle_id"],
                "instrument_families": result.report.get("instrument_families", []),
                "evaluated_strategy_count": result.report.get("evaluated_strategy_count"),
                "runtime_data_source": result.report.get("runtime_data_source"),
                "runtime_decision_source": result.report.get("runtime_decision_source"),
                "live_feed_managed": result.report.get("live_feed_managed"),
                "paper_trading_enabled": result.report.get("paper_trading_enabled"),
                "paper_on_signal": result.report.get("paper_on_signal"),
                "max_paper_trades_per_run": result.report.get("max_paper_trades_per_run"),
                "paper_trades_attempted_count": result.report.get("paper_trades_attempted_count"),
                "latest_signal_strategy_id": result.report.get("latest_signal_strategy_id"),
                "latest_signal_side": result.report.get("latest_signal_side"),
                "latest_paper_lifecycle_report_path": result.report.get("latest_paper_lifecycle_report_path"),
                "latest_broker_state_classification": result.report.get("latest_broker_state_classification"),
                "paper_order_pricing_policy": result.report.get("paper_order_pricing_policy"),
                "latest_paper_order_parameters": result.report.get("latest_paper_order_parameters"),
                "latest_paper_order_parameter_blocker": result.report.get("latest_paper_order_parameter_blocker"),
                "runtime_data_freshness_by_instrument": result.report.get("runtime_data_freshness_by_instrument"),
                "candidate_signals": result.report.get("candidate_signals", []),
                "suppressed_signals": result.report.get("suppressed_signals", []),
                "decision_journal_tier_counts": result.report.get("decision_journal_tier_counts", {}),
                "operator_status_path": result.report.get("operator_status_path"),
                "operator_status_verdict": result.report.get("operator_status_verdict"),
                "submit_allowed": result.report.get("submit_allowed"),
                "submit_attempted": result.report.get("submit_attempted"),
                "paper_proof_invoked": result.report.get("paper_proof_invoked"),
                "broker_state_mutated": result.report.get("broker_state_mutated"),
                "live_money_readiness": result.report.get("live_money_readiness"),
                "primary_blocker": result.report.get("primary_blocker"),
                "required_next_action": result.report.get("required_next_action"),
                "report_json": str(result.report_json),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
