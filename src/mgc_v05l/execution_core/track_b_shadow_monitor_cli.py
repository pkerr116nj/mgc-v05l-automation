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
        description="Run the service-ready Track B SHADOW monitor. This CLI has no submit/PAPER mode."
    )
    parser.add_argument("--mode", default="SHADOW", choices=["SHADOW"])
    parser.add_argument("--max-cycles", type=int, default=999)
    parser.add_argument("--once", action="store_true", help="Run exactly one monitor cycle.")
    parser.add_argument("--poll-seconds", type=float, default=15.0)
    parser.add_argument("--data-refresh-seconds", type=float, default=60.0)
    parser.add_argument("--max-backoff-seconds", type=float, default=300.0)
    parser.add_argument("--max-consecutive-failures", type=int)
    parser.add_argument("--expected-account-id", default="DUM882026")
    parser.add_argument("--account-id", default="DUM882026")
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
        choices=[item.value for item in TrackBRuntimeDataSource],
        default=TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT.value,
        help="Live artifact is the default execution-runtime source. HTTP is explicit backfill/recovery only.",
    )
    parser.add_argument("--live-runtime-feed-output-root", type=Path, default=DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT)
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
                "cycle_id": result.report["cycle_id"],
                "instrument_families": result.report.get("instrument_families", []),
                "evaluated_strategy_count": result.report.get("evaluated_strategy_count"),
                "runtime_data_source": result.report.get("runtime_data_source"),
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
