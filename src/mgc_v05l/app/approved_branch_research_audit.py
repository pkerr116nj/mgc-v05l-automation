"""Research-only audit tooling for approved promoted branches across futures and ETFs."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import statistics
import subprocess
import sys
import tempfile
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import select

from ..app.replay_reporting import (
    ReplayFeatureContext,
    build_session_lookup,
    build_summary_metrics,
    build_trade_ledger,
    write_summary_metrics_json,
    write_trade_ledger_csv,
)
from ..config_models import load_settings_from_files
from ..domain.enums import OrderIntentType
from ..domain.events import FillReceivedEvent, OrderIntentCreatedEvent
from ..domain.models import Bar
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet
from ..persistence.tables import bars_table, features_table
from ..strategy.strategy_engine import StrategyEngine
from .session_phase_labels import label_session_phase

REPO_ROOT = Path(__file__).resolve().parents[3]
REPLAY_DB_PATH = REPO_ROOT / "mgc_v05l.replay.sqlite3"
OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "approved_branch_research"
REPLAY_OUTPUT_DIR = REPO_ROOT / "outputs" / "replays"
APPROVED_BRANCHES: tuple[dict[str, str], ...] = (
    {
        "branch": "usLatePauseResumeLongTurn",
        "side": "LONG",
        "signal_flag": "long_entry",
        "source_field": "long_entry_source",
        "entry_intent_type": "BUY_TO_OPEN",
        "exit_intent_type": "SELL_TO_CLOSE",
    },
    {
        "branch": "asiaEarlyNormalBreakoutRetestHoldTurn",
        "side": "LONG",
        "signal_flag": "long_entry",
        "source_field": "long_entry_source",
        "entry_intent_type": "BUY_TO_OPEN",
        "exit_intent_type": "SELL_TO_CLOSE",
    },
    {
        "branch": "asiaEarlyPauseResumeShortTurn",
        "side": "SHORT",
        "signal_flag": "short_entry",
        "source_field": "short_entry_source",
        "entry_intent_type": "SELL_TO_OPEN",
        "exit_intent_type": "BUY_TO_CLOSE",
    },
)
APPROVED_BRANCH_BY_NAME: dict[str, dict[str, str]] = {item["branch"]: item for item in APPROVED_BRANCHES}
SECOND_PASS_SHORTLIST: dict[str, tuple[str, ...]] = {
    "usLatePauseResumeLongTurn": ("CL", "GC", "NG", "PL", "ZB"),
    "asiaEarlyNormalBreakoutRetestHoldTurn": ("6B", "6E", "GC", "NG"),
    "asiaEarlyPauseResumeShortTurn": ("CL", "MBT"),
}
SECOND_PASS_ADAPTATION_CANDIDATES: dict[tuple[str, str], tuple[dict[str, Any], ...]] = {
    (
        "usLatePauseResumeLongTurn",
        "CL",
    ): (
        {
            "variant": "tight_curvature",
            "rationale": "Tighten positive-turn separator only, keeping the family behavior unchanged.",
            "overrides": {
                "us_late_pause_resume_long_setup_curvature_min": 0.20,
                "us_late_pause_resume_long_min_resumption_curvature": 0.20,
            },
        },
        {
            "variant": "tight_curvature_and_expansion",
            "rationale": "Tighten curvature and slightly cap range expansion to reduce low-quality late resumptions.",
            "overrides": {
                "us_late_pause_resume_long_setup_curvature_min": 0.20,
                "us_late_pause_resume_long_min_resumption_curvature": 0.20,
                "us_late_pause_resume_long_max_range_expansion_ratio": 1.15,
            },
        },
    ),
    (
        "usLatePauseResumeLongTurn",
        "NG",
    ): (
        {
            "variant": "tight_curvature",
            "rationale": "Require stronger positive turn shape without changing the family concept.",
            "overrides": {
                "us_late_pause_resume_long_setup_curvature_min": 0.20,
                "us_late_pause_resume_long_min_resumption_curvature": 0.20,
            },
        },
        {
            "variant": "tight_curvature_and_expansion",
            "rationale": "Tighten curvature and range expansion together for a narrower late-pause entry subset.",
            "overrides": {
                "us_late_pause_resume_long_setup_curvature_min": 0.20,
                "us_late_pause_resume_long_min_resumption_curvature": 0.20,
                "us_late_pause_resume_long_max_range_expansion_ratio": 1.15,
            },
        },
    ),
    (
        "asiaEarlyNormalBreakoutRetestHoldTurn",
        "6B",
    ): (
        {
            "variant": "tight_breakout_slope",
            "rationale": "Tighten the breakout slope gate to keep only the cleaner early-Asia retest structures.",
            "overrides": {
                "asia_early_breakout_retest_hold_breakout_abs_slope_max": 0.15,
            },
        },
        {
            "variant": "tight_slope_and_range_band",
            "rationale": "Narrow the acceptable breakout/retest expansion band without changing the setup family.",
            "overrides": {
                "asia_early_breakout_retest_hold_breakout_abs_slope_max": 0.15,
                "asia_early_breakout_retest_hold_breakout_min_range_expansion_ratio": 0.95,
                "asia_early_breakout_retest_hold_breakout_max_range_expansion_ratio": 1.10,
            },
        },
    ),
    (
        "asiaEarlyNormalBreakoutRetestHoldTurn",
        "6E",
    ): (
        {
            "variant": "tight_breakout_slope",
            "rationale": "Tighten the breakout slope gate to filter softer early-Asia retests.",
            "overrides": {
                "asia_early_breakout_retest_hold_breakout_abs_slope_max": 0.15,
            },
        },
        {
            "variant": "tight_slope_and_range_band",
            "rationale": "Narrow the breakout range band to test whether the family improves with only separator tightening.",
            "overrides": {
                "asia_early_breakout_retest_hold_breakout_abs_slope_max": 0.15,
                "asia_early_breakout_retest_hold_breakout_min_range_expansion_ratio": 0.95,
                "asia_early_breakout_retest_hold_breakout_max_range_expansion_ratio": 1.10,
            },
        },
    ),
    (
        "asiaEarlyNormalBreakoutRetestHoldTurn",
        "NG",
    ): (
        {
            "variant": "tight_breakout_slope",
            "rationale": "Test whether a slightly tighter breakout slope keeps the same family but removes noisy gas retests.",
            "overrides": {
                "asia_early_breakout_retest_hold_breakout_abs_slope_max": 0.15,
            },
        },
        {
            "variant": "tight_slope_and_range_band",
            "rationale": "Narrow the allowed breakout band to see if near-flat expectancy can be improved without rewriting the family.",
            "overrides": {
                "asia_early_breakout_retest_hold_breakout_abs_slope_max": 0.15,
                "asia_early_breakout_retest_hold_breakout_min_range_expansion_ratio": 0.95,
                "asia_early_breakout_retest_hold_breakout_max_range_expansion_ratio": 1.10,
            },
        },
    ),
}
DEPLOYMENT_PROMOTE_SHORTLIST: tuple[tuple[str, str], ...] = (
    ("PL", "usLatePauseResumeLongTurn"),
    ("GC", "usLatePauseResumeLongTurn"),
    ("CL", "usLatePauseResumeLongTurn"),
    ("NG", "usLatePauseResumeLongTurn"),
    ("GC", "asiaEarlyNormalBreakoutRetestHoldTurn"),
)
DEPLOYMENT_LATER_REVIEW: tuple[tuple[str, str], ...] = (
    ("6B", "asiaEarlyNormalBreakoutRetestHoldTurn"),
    ("6E", "asiaEarlyNormalBreakoutRetestHoldTurn"),
    ("NG", "asiaEarlyNormalBreakoutRetestHoldTurn"),
    ("MBT", "asiaEarlyPauseResumeShortTurn"),
    ("CL", "asiaEarlyPauseResumeShortTurn"),
    ("ZB", "usLatePauseResumeLongTurn"),
)
QUANTITATIVE_DEPLOYMENT_SCOPE: tuple[tuple[str, str], ...] = DEPLOYMENT_PROMOTE_SHORTLIST + DEPLOYMENT_LATER_REVIEW
PROMOTION_BUCKETS = (
    "PROMOTE_TO_PROBATIONARY_RESEARCH_READY",
    "KEEP_LATER_REVIEW",
    "REJECT_FOR_NOW",
)
DISCOVERY_FAILURE_BUCKETS = (
    "TOO_FEW_RAW_SETUPS",
    "FILTER_ATTRITION_TOO_HIGH",
    "BLOCKED_MOSTLY",
    "ECONOMICS_WEAK",
    "CONCENTRATION_TOO_HIGH",
    "DRAWDOWN_TOO_HIGH",
    "SESSION_TOO_NARROW",
    "STRUCTURALLY_PRESENT_BUT_RIGID_REPLAY_FORM",
    "NO_STRUCTURAL_FIT",
)
DISCOVERY_TRANSFER_BUCKETS = (
    "DIRECT_TRANSFER_CREDIBLE",
    "NARROW_ADAPTATION_WORTH_TESTING",
    "LATER_REVIEW_ONLY",
    "REJECT_FOR_NOW",
)
DISCOVERY_FUNNEL_BUCKETS = (
    "ADVANCE_TO_ROBUSTNESS_TESTING",
    "NARROW_ADAPTATION_NEXT",
    "KEEP_LATER_REVIEW",
    "STOP_WORK_FOR_NOW",
)
APPROVED_ONLY_OVERRIDE = {
    "max_bars_long": 8,
    "max_bars_short": 6,
    "vwap_long_max_bars": 6,
    "enable_bull_snap_longs": False,
    "enable_first_bull_snap_us_london": False,
    "enable_us_midday_pause_resume_longs": False,
    "enable_us_late_pause_resume_longs": True,
    "enable_us_late_failed_move_reversal_longs": False,
    "enable_us_late_breakout_retest_hold_longs": False,
    "enable_asia_early_breakout_retest_hold_longs": False,
    "enable_asia_early_normal_breakout_retest_hold_longs": True,
    "enable_asia_late_pause_resume_longs": False,
    "enable_asia_late_flat_pullback_pause_resume_longs": False,
    "enable_asia_late_compressed_flat_pullback_pause_resume_longs": False,
    "enable_bear_snap_shorts": False,
    "enable_us_derivative_bear_shorts": False,
    "enable_us_derivative_bear_additive_shorts": False,
    "enable_us_midday_pause_resume_shorts": False,
    "enable_us_midday_expanded_pause_resume_shorts": False,
    "enable_us_midday_compressed_pause_resume_shorts": False,
    "enable_us_midday_compressed_failed_move_reversal_shorts": False,
    "enable_us_midday_compressed_rebound_failed_move_reversal_shorts": False,
    "enable_london_late_pause_resume_shorts": False,
    "enable_asia_early_pause_resume_shorts": True,
    "enable_asia_early_compressed_pause_resume_shorts": False,
    "enable_asia_early_expanded_breakout_retest_hold_shorts": False,
    "enable_asia_vwap_longs": False,
    "asia_early_pause_resume_short_max_normalized_curvature": -0.15,
    "asia_early_pause_resume_short_setup_curvature_flat_threshold": 0.15,
    "asia_early_pause_resume_short_max_range_expansion_ratio": 1.25,
    "asia_early_pause_resume_short_require_one_bar_rebound": True,
    "asia_early_pause_resume_short_require_break_below_prior_1_low": True,
    "asia_early_pause_resume_short_require_close_below_fast_ema": True,
    "us_late_pause_resume_long_setup_curvature_min": 0.15,
    "us_late_pause_resume_long_min_resumption_curvature": 0.15,
    "us_late_pause_resume_long_max_range_expansion_ratio": 1.25,
    "us_late_pause_resume_long_exclude_1755_carryover": True,
    "asia_early_breakout_retest_hold_breakout_abs_slope_max": 0.20,
    "asia_early_breakout_retest_hold_breakout_min_range_expansion_ratio": 0.85,
    "asia_early_breakout_retest_hold_breakout_max_range_expansion_ratio": 1.25,
    "probationary_enforce_approved_branches": True,
}
ETF_SESSION_OVERRIDE = {
    "allow_asia": False,
    "allow_london": False,
    "allow_us": True,
    "us_start": "09:30:00",
    "us_end": "16:00:00",
}


@dataclass(frozen=True)
class ReplayArtifactPaths:
    run_stamp: str
    replay_db_path: Path
    summary_path: Path
    summary_metrics_path: Path
    trade_ledger_path: Path


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.command == "futures-audit":
        payload = run_futures_audit(timeframe=args.timeframe)
    elif args.command == "futures-second-pass":
        payload = run_futures_second_pass(timeframe=args.timeframe)
    elif args.command == "futures-deployment-readiness":
        payload = run_futures_deployment_readiness(timeframe=args.timeframe)
    elif args.command == "futures-quantitative-deployment":
        payload = run_futures_quantitative_deployment(timeframe=args.timeframe)
    elif args.command == "futures-discovery-diagnostics":
        payload = run_futures_discovery_diagnostics(timeframe=args.timeframe)
    elif args.command == "etf-backfill":
        payload = run_etf_backfill(
            symbols=tuple(args.symbols),
            intraday_start=args.intraday_start,
            intraday_end=args.intraday_end,
            daily_start=args.daily_start,
        )
    elif args.command == "etf-audit":
        payload = run_etf_audit(symbols=tuple(args.symbols), timeframe=args.timeframe)
    else:
        parser.error(f"Unsupported command: {args.command}")
        return 2

    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="approved-branch-research-audit")
    subparsers = parser.add_subparsers(dest="command", required=True)

    futures = subparsers.add_parser("futures-audit", help="Replay approved promoted branches across the futures history universe.")
    futures.add_argument("--timeframe", default="5m", help="Timeframe to audit. Defaults to 5m.")

    futures_second_pass = subparsers.add_parser(
        "futures-second-pass",
        help="Run a second-pass promotion audit on the shortlisted futures instruments for the approved promoted branches.",
    )
    futures_second_pass.add_argument("--timeframe", default="5m", help="Timeframe to audit. Defaults to 5m.")

    futures_deployment = subparsers.add_parser(
        "futures-deployment-readiness",
        help="Convert the second-pass futures promotion results into a deployment-readiness / probationary-paper candidate slate.",
    )
    futures_deployment.add_argument("--timeframe", default="5m", help="Timeframe to audit. Defaults to 5m.")

    futures_quant = subparsers.add_parser(
        "futures-quantitative-deployment",
        help="Generate a quantitative deployment committee packet from the completed second-pass futures artifacts.",
    )
    futures_quant.add_argument("--timeframe", default="5m", help="Timeframe to audit. Defaults to 5m.")

    futures_discovery = subparsers.add_parser(
        "futures-discovery-diagnostics",
        help="Build discovery diagnostics, narrow-adaptation candidates, and robustness-prep outputs from existing futures research artifacts.",
    )
    futures_discovery.add_argument("--timeframe", default="5m", help="Timeframe to audit. Defaults to 5m.")

    etf_backfill = subparsers.add_parser("etf-backfill", help="Backfill research-only TQQQ/SQQQ history with ETF session assumptions.")
    etf_backfill.add_argument("--symbols", nargs="+", default=["TQQQ", "SQQQ"])
    etf_backfill.add_argument("--intraday-start", default="2025-06-30T00:00:00+00:00")
    etf_backfill.add_argument("--intraday-end", default="2026-03-18T00:00:00+00:00")
    etf_backfill.add_argument("--daily-start", default="2010-01-01T00:00:00+00:00")

    etf_audit = subparsers.add_parser("etf-audit", help="Run approved promoted branches in research-only mode on TQQQ/SQQQ.")
    etf_audit.add_argument("--symbols", nargs="+", default=["TQQQ", "SQQQ"])
    etf_audit.add_argument("--timeframe", default="5m", help="Timeframe to audit. Defaults to 5m.")
    return parser


def run_futures_audit(*, timeframe: str) -> dict[str, Any]:
    instruments = _available_futures_instruments(timeframe=timeframe)
    futures_results = []
    worker_count = min(4, max(1, (os.cpu_count() or 1)))
    try:
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(_run_symbol_audit_task, symbol=symbol, timeframe=timeframe, lane="futures"): symbol
                for symbol in instruments
            }
            for future in as_completed(future_map):
                futures_results.append(future.result())
    except PermissionError:
        for symbol in instruments:
            futures_results.append(_run_symbol_audit_task(symbol=symbol, timeframe=timeframe, lane="futures"))

    futures_results = _apply_portability_labels(futures_results)
    futures_results.sort(key=lambda row: row["symbol"])
    summary = _build_futures_summary(futures_results)
    written_paths = _write_json_and_markdown(
        stem="approved_branch_futures_portability_audit",
        payload={"timeframe": timeframe, "futures_instruments_tested": instruments, "results": futures_results, "summary": summary},
        markdown=_render_futures_markdown(futures_results, summary),
    )
    return {
        "mode": "futures_audit",
        "timeframe": timeframe,
        "futures_instruments_tested": instruments,
        "artifact_paths": written_paths,
        "summary": summary,
    }


def run_futures_second_pass(*, timeframe: str) -> dict[str, Any]:
    direct_results = _run_second_pass_direct_portability(timeframe=timeframe)
    adaptation_results = _run_second_pass_adaptations(timeframe=timeframe, direct_results=direct_results)
    recommendations = _build_second_pass_recommendations(
        timeframe=timeframe,
        direct_results=direct_results,
        adaptation_results=adaptation_results,
    )

    direct_paths = _write_json_and_markdown(
        stem="approved_branch_futures_direct_portability_second_pass",
        payload=direct_results,
        markdown=_render_second_pass_direct_markdown(direct_results),
    )
    adaptation_paths = _write_json_and_markdown(
        stem="approved_branch_futures_narrow_adaptation_second_pass",
        payload=adaptation_results,
        markdown=_render_second_pass_adaptation_markdown(adaptation_results),
    )
    recommendation_paths = _write_json_and_markdown(
        stem="approved_branch_futures_promotion_recommendations_second_pass",
        payload=recommendations,
        markdown=_render_second_pass_recommendations_markdown(recommendations),
    )

    return {
        "mode": "futures_second_pass",
        "timeframe": timeframe,
        "direct_artifact_paths": direct_paths,
        "adaptation_artifact_paths": adaptation_paths,
        "recommendation_artifact_paths": recommendation_paths,
        "next_paper_candidates": recommendations["ranked_next_paper_candidates"],
        "recommendation_summary": recommendations["summary"],
    }


def run_futures_deployment_readiness(*, timeframe: str) -> dict[str, Any]:
    direct_payload = json.loads((OUTPUT_DIR / "approved_branch_futures_direct_portability_second_pass.json").read_text(encoding="utf-8"))
    recommendations_payload = json.loads(
        (OUTPUT_DIR / "approved_branch_futures_promotion_recommendations_second_pass.json").read_text(encoding="utf-8")
    )
    direct_by_pair = {(row["symbol"], row["branch"]): row for row in direct_payload["direct_results"]}

    promote_rows = [_build_deployment_readiness_row(direct_by_pair[pair]) for pair in DEPLOYMENT_PROMOTE_SHORTLIST]
    later_rows = [_build_deployment_readiness_row(direct_by_pair[pair]) for pair in DEPLOYMENT_LATER_REVIEW]
    gc_overlap_review = _build_gc_overlap_review(
        gc_us_late=direct_by_pair[("GC", "usLatePauseResumeLongTurn")],
        gc_asia=direct_by_pair[("GC", "asiaEarlyNormalBreakoutRetestHoldTurn")],
    )
    readiness_payload = {
        "timeframe": timeframe,
        **_report_horizon_header(promote_rows + later_rows),
        "source_artifacts": {
            "direct_second_pass": str(OUTPUT_DIR / "approved_branch_futures_direct_portability_second_pass.json"),
            "recommendations_second_pass": str(OUTPUT_DIR / "approved_branch_futures_promotion_recommendations_second_pass.json"),
        },
        "promote_shortlist": promote_rows,
        "later_review": later_rows,
        "gc_overlap_review": gc_overlap_review,
    }
    ranking_payload = _build_deployment_ranking(
        promote_rows=promote_rows,
        later_rows=later_rows,
        recommendations_payload=recommendations_payload,
        gc_overlap_review=gc_overlap_review,
    )
    ranking_payload = {**_report_horizon_header(ranking_payload["tier_1"] + ranking_payload["tier_2"] + ranking_payload["tier_3"] + ranking_payload["tier_4"]), **ranking_payload}
    readiness_paths = _write_json_and_markdown(
        stem="approved_branch_futures_deployment_readiness",
        payload=readiness_payload,
        markdown=_render_deployment_readiness_markdown(readiness_payload),
    )
    ranking_paths = _write_json_and_markdown(
        stem="approved_branch_futures_deployment_ranking",
        payload=ranking_payload,
        markdown=_render_deployment_ranking_markdown(ranking_payload),
    )
    return {
        "mode": "futures_deployment_readiness",
        "timeframe": timeframe,
        "readiness_artifact_paths": readiness_paths,
        "ranking_artifact_paths": ranking_paths,
        "tier_1": [f"{row['symbol']} / {row['branch']}" for row in ranking_payload["tier_1"]],
        "tier_2": [f"{row['symbol']} / {row['branch']}" for row in ranking_payload["tier_2"]],
    }


def run_futures_quantitative_deployment(*, timeframe: str) -> dict[str, Any]:
    direct_payload = json.loads((OUTPUT_DIR / "approved_branch_futures_direct_portability_second_pass.json").read_text(encoding="utf-8"))
    ranking_payload = json.loads((OUTPUT_DIR / "approved_branch_futures_deployment_ranking.json").read_text(encoding="utf-8"))
    portability_payload = json.loads((OUTPUT_DIR / "approved_branch_futures_portability_audit.json").read_text(encoding="utf-8"))

    direct_by_pair = {(row["symbol"], row["branch"]): row for row in direct_payload["direct_results"]}
    baseline_by_branch = _load_baseline_home_cases(portability_payload)
    recommendation_by_pair, ranking_order = _build_quantitative_recommendation_map(ranking_payload)

    ranked_rows = []
    for pair in ranking_order:
        direct_row = direct_by_pair.get(pair)
        if direct_row is None:
            continue
        ranked_rows.append(
            _build_quantitative_deployment_row(
                direct_row=direct_row,
                baseline_case=baseline_by_branch.get(str(direct_row["branch"])),
                recommendation=recommendation_by_pair.get(pair),
            )
        )

    gc_comparison = _build_gc_quantitative_comparison(
        gc_us_late=direct_by_pair[("GC", "usLatePauseResumeLongTurn")],
        gc_asia=direct_by_pair[("GC", "asiaEarlyNormalBreakoutRetestHoldTurn")],
    )
    recommendation_section = _build_quantitative_recommendation_section(ranked_rows)
    payload = {
        "timeframe": timeframe,
        **_report_horizon_header(ranked_rows),
        "source_artifacts": {
            "direct_second_pass": str(OUTPUT_DIR / "approved_branch_futures_direct_portability_second_pass.json"),
            "deployment_ranking": str(OUTPUT_DIR / "approved_branch_futures_deployment_ranking.json"),
            "futures_portability_audit": str(OUTPUT_DIR / "approved_branch_futures_portability_audit.json"),
        },
        "metric_method_notes": {
            "max_drawdown": "Derived from the cumulative realized P/L path on the persisted replay trade ledger, ordered by realized exit timestamp.",
            "per_trade_sharpe_proxy": "Computed from the per-trade realized P/L series as mean/stdev*sqrt(n); this is a trade-series proxy, not a bar-return Sharpe.",
            "longest_no_trade_stretch": "Computed as the longest calendar-time gap between trade entries, including leading and trailing gaps inside the tested replay date range.",
            "top_5_trade_share": "Computed as the sum of the top five realized trades divided by total realized P/L; this can exceed 100% when the sample also contains meaningful losses.",
        },
        "ranked_pairs": ranked_rows,
        "gc_side_by_side": gc_comparison,
        "recommendations": recommendation_section,
    }
    written_paths = _write_json_and_markdown(
        stem="approved_branch_futures_quantitative_deployment_report",
        payload=payload,
        markdown=_render_quantitative_deployment_markdown(payload),
    )
    return {
        "mode": "futures_quantitative_deployment",
        "timeframe": timeframe,
        "artifact_paths": written_paths,
        "recommendation_counts": {bucket: len(rows) for bucket, rows in recommendation_section["buckets"].items()},
    }


def run_futures_discovery_diagnostics(*, timeframe: str) -> dict[str, Any]:
    portability_payload = json.loads((OUTPUT_DIR / "approved_branch_futures_portability_audit.json").read_text(encoding="utf-8"))
    direct_payload = json.loads((OUTPUT_DIR / "approved_branch_futures_direct_portability_second_pass.json").read_text(encoding="utf-8"))
    adaptation_payload = json.loads((OUTPUT_DIR / "approved_branch_futures_narrow_adaptation_second_pass.json").read_text(encoding="utf-8"))

    diagnostics_rows = _build_futures_discovery_rows(portability_payload)
    diagnostics_rows.sort(
        key=lambda row: (
            0 if row["symbol"] == "MGC" else 1,
            row["branch"],
            row["symbol"],
        )
    )
    diagnostics_summary = _build_discovery_diagnostics_summary(diagnostics_rows)
    diagnostics_report = {
        "timeframe": timeframe,
        "source_artifacts": {
            "futures_portability_audit": str(OUTPUT_DIR / "approved_branch_futures_portability_audit.json"),
        },
        **_report_horizon_header(diagnostics_rows),
        "failure_buckets": list(DISCOVERY_FAILURE_BUCKETS),
        "transfer_verdict_buckets": list(DISCOVERY_TRANSFER_BUCKETS),
        "rows": diagnostics_rows,
        "summary": diagnostics_summary,
    }
    discovery_paths = _write_json_and_markdown(
        stem="discovery_diagnostics_futures",
        payload=diagnostics_report,
        markdown=_render_discovery_diagnostics_markdown(diagnostics_report),
    )

    adaptation_report = _build_narrow_adaptation_candidates_report(
        diagnostics_rows=diagnostics_rows,
        direct_payload=direct_payload,
        adaptation_payload=adaptation_payload,
        timeframe=timeframe,
    )
    adaptation_paths = _write_json_and_markdown(
        stem="narrow_adaptation_candidates",
        payload=adaptation_report,
        markdown=_render_narrow_adaptation_markdown(adaptation_report),
    )

    robustness_report = _build_robustness_prep_shortlist(
        diagnostics_rows=diagnostics_rows,
        adaptation_report=adaptation_report,
        timeframe=timeframe,
    )
    robustness_paths = _write_json_and_markdown(
        stem="robustness_prep_shortlist",
        payload=robustness_report,
        markdown=_render_robustness_prep_markdown(robustness_report),
    )
    return {
        "mode": "futures_discovery_diagnostics",
        "timeframe": timeframe,
        "artifact_paths": {
            "discovery_diagnostics": discovery_paths,
            "narrow_adaptation_candidates": adaptation_paths,
            "robustness_prep_shortlist": robustness_paths,
        },
        "summary": {
            "discovery": diagnostics_summary,
            "bucket_counts": robustness_report["bucket_counts"],
            "adaptation_candidate_count": len(adaptation_report["rows"]),
        },
    }


def run_etf_backfill(
    *,
    symbols: tuple[str, ...],
    intraday_start: str,
    intraday_end: str,
    daily_start: str,
) -> dict[str, Any]:
    intraday_start_dt = datetime.fromisoformat(intraday_start)
    intraday_end_dt = datetime.fromisoformat(intraday_end)
    daily_start_dt = datetime.fromisoformat(daily_start)
    validation: dict[str, Any] = {
        "symbols_requested": list(symbols),
        "data_preexisting": _load_existing_symbol_coverage(symbols),
        "session_assumptions": {
            "timezone": "America/New_York",
            "regular_hours_only": True,
            "us_session_start": ETF_SESSION_OVERRIDE["us_start"],
            "us_session_end": ETF_SESSION_OVERRIDE["us_end"],
            "allow_asia": False,
            "allow_london": False,
            "allow_us": True,
            "extended_hours_requested": False,
        },
        "backfill_runs": [],
    }

    for symbol in symbols:
        validation["backfill_runs"].append(
            _backfill_symbol_stack(
                symbol=symbol,
                intraday_start=intraday_start_dt,
                intraday_end=intraday_end_dt,
                daily_start=daily_start_dt,
            )
        )

    coverage = _load_existing_symbol_coverage(symbols)
    readiness = _build_etf_data_readiness(coverage)
    payload = {
        "symbols_requested": list(symbols),
        "coverage": coverage,
        "validation": validation,
        "readiness": readiness,
    }
    written_paths = _write_json_and_markdown(
        stem="tqqq_sqqq_data_validation",
        payload=payload,
        markdown=_render_etf_validation_markdown(payload),
    )
    payload["artifact_paths"] = written_paths
    return payload


def run_etf_audit(*, symbols: tuple[str, ...], timeframe: str) -> dict[str, Any]:
    etf_results = []
    for symbol in symbols:
        etf_results.append(_run_symbol_audit_task(symbol=symbol, timeframe=timeframe, lane="etf"))
    etf_results = _apply_portability_labels(etf_results)
    summary = _build_etf_summary(etf_results)
    payload = {
        "timeframe": timeframe,
        "symbols": list(symbols),
        "results": etf_results,
        "summary": summary,
    }
    written_paths = _write_json_and_markdown(
        stem="tqqq_sqqq_research_audit",
        payload=payload,
        markdown=_render_etf_audit_markdown(etf_results, summary),
    )
    payload["artifact_paths"] = written_paths
    return payload


def _run_second_pass_direct_portability(*, timeframe: str) -> dict[str, Any]:
    baseline_by_branch = _load_mgc_reference_metrics()
    symbol_results: dict[str, dict[str, Any]] = {}
    for symbol in _second_pass_unique_symbols():
        symbol_results[symbol] = _run_symbol_audit_task(
            symbol=symbol,
            timeframe=timeframe,
            lane="futures",
            label="second_pass_direct",
        )

    pair_rows = []
    for branch, symbols in SECOND_PASS_SHORTLIST.items():
        for symbol in symbols:
            pair_rows.append(
                _build_second_pass_pair_row(
                    symbol_result=symbol_results[symbol],
                    branch=branch,
                    baseline_metrics=baseline_by_branch.get(branch),
                    variant="direct",
                    overrides=None,
                    rationale="Approved promoted family replayed unchanged on the shortlisted futures instrument.",
                )
            )
    pair_rows.sort(key=lambda row: (row["branch"], row["symbol"], row["variant"]))
    return {
        "timeframe": timeframe,
        "baseline_reference_source": str(OUTPUT_DIR / "approved_branch_futures_portability_audit.json"),
        "baseline_reference_symbol": "MGC",
        "shortlist": {branch: list(symbols) for branch, symbols in SECOND_PASS_SHORTLIST.items()},
        "direct_results": pair_rows,
    }


def _run_second_pass_adaptations(*, timeframe: str, direct_results: dict[str, Any]) -> dict[str, Any]:
    direct_by_pair = {
        (row["branch"], row["symbol"]): row
        for row in direct_results["direct_results"]
    }
    adaptation_rows = []
    for pair, plans in SECOND_PASS_ADAPTATION_CANDIDATES.items():
        branch, symbol = pair
        direct_row = direct_by_pair.get(pair)
        if direct_row is None or not _is_close_but_not_promotion_ready(direct_row):
            continue
        baseline_metrics = _load_mgc_reference_metrics().get(branch)
        for plan in plans:
            symbol_result = _run_symbol_audit_task(
                symbol=symbol,
                timeframe=timeframe,
                lane="futures",
                label=f"second_pass_{branch}_{symbol}_{plan['variant']}",
                extra_overrides=plan["overrides"],
            )
            adaptation_rows.append(
                _build_second_pass_pair_row(
                    symbol_result=symbol_result,
                    branch=branch,
                    baseline_metrics=baseline_metrics,
                    variant=plan["variant"],
                    overrides=plan["overrides"],
                    rationale=plan["rationale"],
                    direct_reference=direct_row,
                )
            )

    adaptation_rows.sort(key=lambda row: (row["branch"], row["symbol"], row["variant"]))
    return {
        "timeframe": timeframe,
        "adapted_results": adaptation_rows,
    }


def _build_second_pass_recommendations(
    *,
    timeframe: str,
    direct_results: dict[str, Any],
    adaptation_results: dict[str, Any],
) -> dict[str, Any]:
    direct_rows = direct_results["direct_results"]
    adaptations_by_pair: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in adaptation_results["adapted_results"]:
        adaptations_by_pair.setdefault((row["branch"], row["symbol"]), []).append(row)

    recommendations = []
    for direct_row in direct_rows:
        pair = (direct_row["branch"], direct_row["symbol"])
        best_row = _pick_best_result(direct_row, adaptations_by_pair.get(pair, []))
        bucket = _recommendation_bucket(best_row)
        reason = _recommendation_rationale(best_row, direct_row=direct_row)
        recommendations.append(
            {
                "branch": direct_row["branch"],
                "symbol": direct_row["symbol"],
                "bucket": bucket,
                "selected_variant": best_row["variant"],
                "used_adaptation": best_row["variant"] != "direct",
                "recommendation_score": _recommendation_score(best_row),
                "rationale": reason,
                "best_result": best_row,
                "direct_result": direct_row,
            }
        )

    recommendations.sort(key=lambda row: (-row["recommendation_score"], row["symbol"], row["branch"]))
    promote = [row for row in recommendations if row["bucket"] == "PROMOTE_TO_PROBATIONARY_RESEARCH_READY"]
    later = [row for row in recommendations if row["bucket"] == "KEEP_LATER_REVIEW"]
    reject = [row for row in recommendations if row["bucket"] == "REJECT_FOR_NOW"]
    family_transfer = _family_transfer_summary(recommendations)
    ranked = [
        {
            "rank": index + 1,
            "symbol": row["symbol"],
            "branch": row["branch"],
            "selected_variant": row["selected_variant"],
            "recommendation_score": row["recommendation_score"],
            "bucket": row["bucket"],
        }
        for index, row in enumerate(promote + later)
    ]
    return {
        "timeframe": timeframe,
        "promote_now": promote,
        "later_review": later,
        "reject_for_now": reject,
        "ranked_next_paper_candidates": ranked,
        "family_transfer_summary": family_transfer,
        "summary": {
            "next_paper_candidates": [f"{row['symbol']} / {row['branch']}" for row in promote],
            "credible_but_later_review": [f"{row['symbol']} / {row['branch']}" for row in later],
            "not_worth_pursuing_now": [f"{row['symbol']} / {row['branch']}" for row in reject],
        },
    }


def _available_futures_instruments(*, timeframe: str) -> list[str]:
    connection = sqlite3.connect(REPLAY_DB_PATH)
    try:
        rows = connection.execute(
            "select ticker from bars where timeframe = ? group by ticker order by ticker",
            (timeframe,),
        ).fetchall()
    finally:
        connection.close()
    excluded = {"TQQQ", "SQQQ"}
    return [str(row[0]) for row in rows if str(row[0]) not in excluded]


def _run_replay_for_symbol(
    *,
    symbol: str,
    timeframe: str,
    lane: str,
    label: str | None = None,
    extra_overrides: dict[str, Any] | None = None,
) -> ReplayArtifactPaths:
    prefix = label or lane
    run_stamp = f"{prefix}_approved_{symbol.lower()}_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
    artifacts = ReplayArtifactPaths(
        run_stamp=run_stamp,
        replay_db_path=REPLAY_OUTPUT_DIR / f"persisted_bar_replay_{run_stamp}.sqlite3",
        summary_path=REPLAY_OUTPUT_DIR / f"persisted_bar_replay_{run_stamp}.summary.json",
        summary_metrics_path=REPLAY_OUTPUT_DIR / f"persisted_bar_replay_{run_stamp}.summary_metrics.json",
        trade_ledger_path=REPLAY_OUTPUT_DIR / f"persisted_bar_replay_{run_stamp}.trade_ledger.csv",
    )
    override = _build_override_yaml(lane=lane, extra_overrides=extra_overrides)
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", prefix=f"{lane}_{symbol.lower()}_", delete=False) as handle:
        handle.write(override)
        override_path = Path(handle.name)
    try:
        settings = load_settings_from_files(
            [
                str(REPO_ROOT / "config" / "base.yaml"),
                str(REPO_ROOT / "config" / "replay.yaml"),
                str(REPO_ROOT / "config" / "replay.research_control.yaml"),
                str(override_path),
            ]
        )
        source_engine = build_engine(f"sqlite:///{REPLAY_DB_PATH}")
        with source_engine.begin() as connection:
            rows = connection.execute(
                select(bars_table)
                .where(
                    bars_table.c.ticker == symbol,
                    bars_table.c.timeframe == timeframe,
                )
                .order_by(bars_table.c.timestamp.asc())
            ).mappings().all()
        bars = [
            Bar(
                bar_id=row["bar_id"],
                symbol=row["symbol"],
                timeframe=row["timeframe"],
                start_ts=datetime.fromisoformat(row["start_ts"]),
                end_ts=datetime.fromisoformat(row["end_ts"]),
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                volume=int(row["volume"]),
                is_final=bool(row["is_final"]),
                session_asia=bool(row["session_asia"]),
                session_london=bool(row["session_london"]),
                session_us=bool(row["session_us"]),
                session_allowed=bool(row["session_allowed"]),
            )
            for row in rows
        ]
        if not bars:
            raise RuntimeError(f"No persisted {symbol} {timeframe} bars were found in {REPLAY_DB_PATH}.")

        replay_settings = settings.model_copy(
            update={
                "symbol": symbol,
                "timeframe": timeframe,
                "database_url": f"sqlite:///{artifacts.replay_db_path}",
            }
        )
        repositories = RepositorySet(build_engine(replay_settings.database_url))
        repositories.bars.save = lambda *args, **kwargs: None
        captured_signal_bars: dict[str, list[str]] = {item["branch"]: [] for item in APPROVED_BRANCHES}

        def capture_signal_packet(signals, created_at=None):
            for item in APPROVED_BRANCHES:
                branch = item["branch"]
                if getattr(signals, item["source_field"], None) == branch and getattr(signals, item["signal_flag"], False):
                    captured_signal_bars[branch].append(str(signals.bar_id))

        repositories.signals.save = capture_signal_packet
        strategy_engine = StrategyEngine(settings=replay_settings, repositories=repositories)
        if strategy_engine._state_repository is not None:
            strategy_engine._state_repository.save_snapshot = lambda *args, **kwargs: None

        event_counts: Counter[str] = Counter()
        started = datetime.now(UTC)
        for bar in bars:
            for event in strategy_engine.process_bar(bar):
                if isinstance(event, OrderIntentCreatedEvent):
                    event_counts["order_intents"] += 1
                    if event.intent_type == OrderIntentType.BUY_TO_OPEN:
                        event_counts["long_entries"] += 1
                    elif event.intent_type == OrderIntentType.SELL_TO_OPEN:
                        event_counts["short_entries"] += 1
                    else:
                        event_counts["exits"] += 1
                elif isinstance(event, FillReceivedEvent):
                    event_counts["fills"] += 1

        session_by_start_ts = build_session_lookup(bars)
        feature_context_by_bar_id = _load_feature_context(repositories)
        point_value = Decimal(os.environ.get("REPLAY_POINT_VALUE", "10"))
        fee_per_fill = Decimal(os.environ.get("REPLAY_FEE_PER_FILL", "0"))
        slippage_per_fill = Decimal(os.environ.get("REPLAY_SLIPPAGE_PER_FILL", "0"))
        trade_ledger = build_trade_ledger(
            repositories.order_intents.list_all(),
            repositories.fills.list_all(),
            session_by_start_ts,
            point_value=point_value,
            fee_per_fill=fee_per_fill,
            slippage_per_fill=slippage_per_fill,
            bars=bars,
            feature_context_by_bar_id=feature_context_by_bar_id,
        )
        summary_metrics = build_summary_metrics(trade_ledger)
        write_trade_ledger_csv(trade_ledger, artifacts.trade_ledger_path)
        write_summary_metrics_json(
            summary_metrics,
            artifacts.summary_metrics_path,
            point_value=point_value,
            fee_per_fill=fee_per_fill,
            slippage_per_fill=slippage_per_fill,
        )
        artifacts.summary_path.write_text(
            json.dumps(
                {
                    "source_db_path": str(REPLAY_DB_PATH),
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "replay_db_path": str(artifacts.replay_db_path),
                    "trade_ledger_path": str(artifacts.trade_ledger_path),
                    "summary_metrics_path": str(artifacts.summary_metrics_path),
                    "processed_bars": repositories.processed_bars.count(),
                    "source_bar_count": len(bars),
                    "source_first_bar_ts": bars[0].end_ts.isoformat(),
                    "source_last_bar_ts": bars[-1].end_ts.isoformat(),
                    "runtime_started_at": started.isoformat(),
                    "event_counts": dict(event_counts),
                    "approved_branch_signal_bars": captured_signal_bars,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
    finally:
        override_path.unlink(missing_ok=True)
    return artifacts


def _run_symbol_audit_task(
    *,
    symbol: str,
    timeframe: str,
    lane: str,
    label: str | None = None,
    extra_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    replay_artifacts = _run_replay_for_symbol(
        symbol=symbol,
        timeframe=timeframe,
        lane=lane,
        label=label,
        extra_overrides=extra_overrides,
    )
    return _build_symbol_audit(symbol=symbol, timeframe=timeframe, lane=lane, replay_artifacts=replay_artifacts)


def _build_override_yaml(*, lane: str, extra_overrides: dict[str, Any] | None = None) -> str:
    fields = dict(APPROVED_ONLY_OVERRIDE)
    if lane == "etf":
        fields.update(ETF_SESSION_OVERRIDE)
    if extra_overrides:
        fields.update(extra_overrides)
    lines = [f'{key}: {_yaml_scalar(value)}' for key, value in fields.items()]
    return "\n".join(lines) + "\n"


def _yaml_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(str(value))


def _build_symbol_audit(*, symbol: str, timeframe: str, lane: str, replay_artifacts: ReplayArtifactPaths) -> dict[str, Any]:
    summary_metrics = json.loads(replay_artifacts.summary_metrics_path.read_text(encoding="utf-8"))
    summary_payload = json.loads(replay_artifacts.summary_path.read_text(encoding="utf-8"))
    trade_rows = _load_trade_rows(replay_artifacts.trade_ledger_path)
    replay_counts = _load_replay_event_counts(
        replay_artifacts.replay_db_path,
        signal_bars_by_branch=summary_payload.get("approved_branch_signal_bars"),
    )
    coverage = _load_symbol_timeframe_coverage(symbol=symbol, timeframe=timeframe)
    branch_rows = []
    for branch_config in APPROVED_BRANCHES:
        branch = branch_config["branch"]
        trades = [row for row in trade_rows if row.get("setup_family") == branch]
        counts = replay_counts.get(branch, {})
        branch_rows.append(
            {
                "branch": branch,
                "side": branch_config["side"],
                "signals": counts.get("signals", 0),
                "blocked_count": counts.get("blocked_count", 0),
                "entry_intents": counts.get("entry_intents", 0),
                "entry_fills": counts.get("entry_fills", 0),
                "closed_trades": len(trades),
                "realized_pnl": round(sum(_float(row.get("net_pnl")) for row in trades), 4),
                "win_rate": _win_rate(trades),
                "average_realized_per_trade": _average_realized_per_trade(trades),
                "sample_warning": _sample_warning(signals=counts.get("signals", 0), closed_trades=len(trades)),
                "coverage_warning": _coverage_warning(coverage),
                "blocked_count_provenance": "Derived as approved-branch signal bars without a same-bar entry intent in replay artifacts because replay rule-block rows are not persisted separately.",
                "notes": _branch_activity_note(counts.get("signals", 0), counts.get("blocked_count", 0), len(trades)),
            }
        )

    return {
        "symbol": symbol,
        "lane": lane,
        "timeframe": timeframe,
        "coverage": coverage,
        "summary_metrics": {
            "total_net_pnl": summary_metrics.get("total_net_pnl"),
            "number_of_trades": summary_metrics.get("number_of_trades"),
            "win_rate": summary_metrics.get("win_rate"),
            "avg_winner": summary_metrics.get("avg_winner"),
            "avg_loser": summary_metrics.get("avg_loser"),
            "expectancy": summary_metrics.get("expectancy"),
        },
        "artifact_paths": {
            "replay_summary": str(replay_artifacts.summary_path),
            "summary_metrics": str(replay_artifacts.summary_metrics_path),
            "trade_ledger": str(replay_artifacts.trade_ledger_path),
            "replay_db": str(replay_artifacts.replay_db_path),
        },
        "session_window_note": (
            "Research-only regular-hours ETF session assumptions were applied."
            if lane == "etf"
            else "Futures replay used the approved promoted-branch replay override on the stored futures bars."
        ),
        "branch_rows": branch_rows,
        "overall_activity": {
            "latest_bar": summary_payload.get("source_last_bar_ts"),
            "processed_bars": summary_payload.get("processed_bars"),
        },
    }


def _second_pass_unique_symbols() -> list[str]:
    seen: set[str] = set()
    ordered = []
    for symbols in SECOND_PASS_SHORTLIST.values():
        for symbol in symbols:
            if symbol not in seen:
                seen.add(symbol)
                ordered.append(symbol)
    return ordered


def _load_mgc_reference_metrics() -> dict[str, dict[str, Any]]:
    path = OUTPUT_DIR / "approved_branch_futures_portability_audit.json"
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    baseline_rows = {}
    for result in payload.get("results", []):
        if result.get("symbol") != "MGC":
            continue
        for row in result.get("branch_rows", []):
            baseline_rows[str(row.get("branch"))] = row
    return baseline_rows


def _build_second_pass_pair_row(
    *,
    symbol_result: dict[str, Any],
    branch: str,
    baseline_metrics: dict[str, Any] | None,
    variant: str,
    overrides: dict[str, Any] | None,
    rationale: str,
    direct_reference: dict[str, Any] | None = None,
) -> dict[str, Any]:
    branch_row = next(row for row in symbol_result["branch_rows"] if row["branch"] == branch)
    trade_rows = _load_trade_rows(Path(symbol_result["artifact_paths"]["trade_ledger"]))
    branch_trades = [row for row in trade_rows if row.get("setup_family") == branch]
    entry_phase_counts = dict(Counter(row.get("entry_session_phase") or "UNKNOWN" for row in branch_trades))
    dominant_phase, dominant_share = _dominant_bucket(entry_phase_counts)
    largest_win = max((_float(row.get("net_pnl")) for row in branch_trades), default=None)
    largest_loss = min((_float(row.get("net_pnl")) for row in branch_trades), default=None)
    vs_mgc = _degradation_vs_mgc(branch_row, baseline_metrics)
    direct_delta = _direct_delta(branch_row, direct_reference)
    portability_assessment = branch_row.get("portability_assessment") or classify_branch_portability(
        branch_metrics=branch_row,
        baseline_metrics=baseline_metrics,
        instrument_symbol=str(symbol_result["symbol"]),
    )
    return {
        "symbol": symbol_result["symbol"],
        "branch": branch,
        "side": APPROVED_BRANCH_BY_NAME[branch]["side"],
        "variant": variant,
        "variant_overrides": overrides or {},
        "variant_rationale": rationale,
        "artifact_paths": symbol_result["artifact_paths"],
        "closed_trades": branch_row["closed_trades"],
        "win_rate": branch_row["win_rate"],
        "realized_pnl": branch_row["realized_pnl"],
        "average_realized_per_trade": branch_row["average_realized_per_trade"],
        "largest_win": round(largest_win, 4) if largest_win is not None else None,
        "largest_loss": round(largest_loss, 4) if largest_loss is not None else None,
        "blocked_decisions": branch_row["blocked_count"],
        "intents": branch_row["entry_intents"],
        "fills": branch_row["entry_fills"],
        "signals": branch_row["signals"],
        "sample_warning": branch_row["sample_warning"],
        "coverage_warning": branch_row["coverage_warning"],
        "portability_assessment": portability_assessment,
        "notes": branch_row["notes"],
        "dominant_entry_phase": dominant_phase,
        "dominant_entry_phase_share": dominant_share,
        "entry_phase_counts": entry_phase_counts,
        "degradation_vs_mgc": vs_mgc,
        "delta_vs_direct": direct_delta,
        "promotion_readiness": _direct_promotion_readiness(
            branch_row=branch_row,
            dominant_phase=dominant_phase,
            dominant_share=dominant_share,
        ),
    }


def _dominant_bucket(counts: dict[str, int]) -> tuple[str | None, float | None]:
    if not counts:
        return None, None
    bucket, count = max(counts.items(), key=lambda item: item[1])
    total = sum(counts.values())
    if total <= 0:
        return bucket, None
    return bucket, round(count / total, 4)


def _degradation_vs_mgc(branch_metrics: dict[str, Any], baseline_metrics: dict[str, Any] | None) -> dict[str, Any] | None:
    if not baseline_metrics:
        return None
    baseline_avg = _float(baseline_metrics.get("average_realized_per_trade"))
    current_avg = _float(branch_metrics.get("average_realized_per_trade"))
    baseline_win = baseline_metrics.get("win_rate")
    current_win = branch_metrics.get("win_rate")
    baseline_trades = int(baseline_metrics.get("closed_trades") or 0)
    current_trades = int(branch_metrics.get("closed_trades") or 0)
    return {
        "avg_realized_ratio": round(current_avg / baseline_avg, 4) if baseline_avg else None,
        "win_rate_delta": round(_float(current_win) - _float(baseline_win), 4) if baseline_win is not None and current_win is not None else None,
        "trade_count_ratio": round(current_trades / baseline_trades, 4) if baseline_trades else None,
        "baseline_symbol": "MGC",
    }


def _direct_delta(branch_metrics: dict[str, Any], direct_reference: dict[str, Any] | None) -> dict[str, Any] | None:
    if direct_reference is None:
        return None
    return {
        "realized_pnl_delta": round(_float(branch_metrics.get("realized_pnl")) - _float(direct_reference.get("realized_pnl")), 4),
        "avg_realized_per_trade_delta": round(
            _float(branch_metrics.get("average_realized_per_trade")) - _float(direct_reference.get("average_realized_per_trade")),
            4,
        ),
        "closed_trades_delta": int(branch_metrics.get("closed_trades") or 0) - int(direct_reference.get("closed_trades") or 0),
        "win_rate_delta": round(_float(branch_metrics.get("win_rate")) - _float(direct_reference.get("win_rate")), 4),
    }


def _expected_phase_for_branch(branch: str) -> str | None:
    if branch == "usLatePauseResumeLongTurn":
        return "US_LATE"
    if branch in {"asiaEarlyNormalBreakoutRetestHoldTurn", "asiaEarlyPauseResumeShortTurn"}:
        return "ASIA_EARLY"
    return None


def _direct_promotion_readiness(*, branch_row: dict[str, Any], dominant_phase: str | None, dominant_share: float | None) -> str:
    trades = int(branch_row.get("closed_trades") or 0)
    pnl = _float(branch_row.get("realized_pnl"))
    avg_trade = _float(branch_row.get("average_realized_per_trade"))
    expected_phase = _expected_phase_for_branch(str(branch_row.get("branch")))
    phase_match = expected_phase is None or (dominant_phase == expected_phase and (dominant_share or 0.0) >= 0.6)
    if trades >= 10 and pnl > 0 and avg_trade >= 0.05 and phase_match:
        return "DIRECT_PROMOTION_CANDIDATE"
    if trades >= 10 and pnl >= 0 and phase_match:
        return "CLOSE_BUT_NOT_READY"
    if trades < 10 and pnl > 0:
        return "THIN_POSITIVE_SAMPLE"
    if pnl <= 0 and trades >= 5:
        return "STRUCTURAL_MISMATCH_OR_DEGRADED"
    return "INSUFFICIENT_EVIDENCE"


def _is_close_but_not_promotion_ready(row: dict[str, Any]) -> bool:
    return row["promotion_readiness"] == "CLOSE_BUT_NOT_READY"


def _pick_best_result(direct_row: dict[str, Any], adapted_rows: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = [direct_row, *adapted_rows]
    return max(candidates, key=_recommendation_score)


def _recommendation_score(row: dict[str, Any]) -> float:
    trades = float(row.get("closed_trades") or 0)
    pnl = _float(row.get("realized_pnl"))
    avg_trade = _float(row.get("average_realized_per_trade"))
    win_rate = _float(row.get("win_rate"))
    dominant_share = float(row.get("dominant_entry_phase_share") or 0)
    direct_bonus = 10.0 if row.get("variant") == "direct" else 5.0
    return (trades * 0.5) + pnl + (avg_trade * 2.0) + (win_rate * 20.0) + (dominant_share * 10.0) + direct_bonus


def _recommendation_bucket(row: dict[str, Any]) -> str:
    trades = int(row.get("closed_trades") or 0)
    pnl = _float(row.get("realized_pnl"))
    avg_trade = _float(row.get("average_realized_per_trade"))
    largest_loss = abs(_float(row.get("largest_loss")))
    largest_win = abs(_float(row.get("largest_win")))
    win_rate = row.get("win_rate")
    readiness = row.get("promotion_readiness")
    direct_delta = row.get("delta_vs_direct") or {}
    tuned_narrowly = len(row.get("variant_overrides") or {}) <= 3
    if readiness == "DIRECT_PROMOTION_CANDIDATE" and trades >= 10 and pnl > 0 and avg_trade > 0 and (largest_win == 0 or largest_loss <= largest_win * 2.5):
        return "PROMOTE_TO_PROBATIONARY_RESEARCH_READY"
    if row.get("variant") != "direct" and tuned_narrowly and trades >= 10 and pnl > 0 and avg_trade > 0:
        if _float(direct_delta.get("realized_pnl_delta")) > 0 and _float(direct_delta.get("avg_realized_per_trade_delta")) >= 0:
            return "PROMOTE_TO_PROBATIONARY_RESEARCH_READY"
    if trades >= 5 and (pnl > 0 or readiness in {"CLOSE_BUT_NOT_READY", "THIN_POSITIVE_SAMPLE"}):
        return "KEEP_LATER_REVIEW"
    if trades >= 10 and win_rate is not None and _float(win_rate) >= 0.35 and pnl >= 0:
        return "KEEP_LATER_REVIEW"
    return "REJECT_FOR_NOW"


def _recommendation_rationale(best_row: dict[str, Any], *, direct_row: dict[str, Any]) -> str:
    symbol = best_row["symbol"]
    branch = best_row["branch"]
    trades = best_row["closed_trades"]
    pnl = best_row["realized_pnl"]
    avg_trade = best_row["average_realized_per_trade"]
    phase = best_row.get("dominant_entry_phase") or "UNKNOWN"
    share = best_row.get("dominant_entry_phase_share")
    share_text = f"{round(float(share) * 100, 1)}%" if share is not None else "n/a"
    if best_row["variant"] == "direct":
        return (
            f"{symbol} / {branch} stayed viable without changing the approved family: {trades} closed trades, "
            f"{pnl} realized P/L, {avg_trade} average realized per trade, with activity concentrated in {phase} ({share_text})."
        )
    delta = best_row.get("delta_vs_direct") or {}
    return (
        f"{symbol} / {branch} only became credible after a narrow separator-only variant ({best_row['variant']}), "
        f"improving direct realized P/L by {delta.get('realized_pnl_delta')} and average realized/trade by "
        f"{delta.get('avg_realized_per_trade_delta')} while keeping the same family behavior concentrated in {phase} ({share_text})."
    )


def _family_transfer_summary(recommendations: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {}
    for branch in SECOND_PASS_SHORTLIST:
        rows = [row for row in recommendations if row["branch"] == branch]
        summary[branch] = {
            "promote_now": [row["symbol"] for row in rows if row["bucket"] == "PROMOTE_TO_PROBATIONARY_RESEARCH_READY"],
            "later_review": [row["symbol"] for row in rows if row["bucket"] == "KEEP_LATER_REVIEW"],
            "reject_for_now": [row["symbol"] for row in rows if row["bucket"] == "REJECT_FOR_NOW"],
        }
    return summary


def _load_trade_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _load_replay_event_counts(path: Path, *, signal_bars_by_branch: dict[str, list[str]] | None = None) -> dict[str, dict[str, int]]:
    signal_rows_by_branch: dict[str, list[str]] = {
        item["branch"]: list((signal_bars_by_branch or {}).get(item["branch"], []))
        for item in APPROVED_BRANCHES
    }
    intent_bars_by_branch: dict[str, set[str]] = {item["branch"]: set() for item in APPROVED_BRANCHES}
    fill_counts_by_branch: dict[str, int] = {item["branch"]: 0 for item in APPROVED_BRANCHES}
    connection = sqlite3.connect(path)
    try:
        intent_rows = connection.execute(
            "select bar_id, reason_code, intent_type, order_intent_id from order_intents where reason_code in (?, ?, ?)",
            tuple(item["branch"] for item in APPROVED_BRANCHES),
        ).fetchall()
        intent_id_to_branch: dict[str, str] = {}
        for bar_id, reason_code, intent_type, order_intent_id in intent_rows:
            branch = str(reason_code)
            expected_type = next(item["entry_intent_type"] for item in APPROVED_BRANCHES if item["branch"] == branch)
            if intent_type == expected_type:
                intent_bars_by_branch[branch].add(str(bar_id))
                intent_id_to_branch[str(order_intent_id)] = branch
        fill_rows = connection.execute("select order_intent_id from fills").fetchall()
        for (order_intent_id,) in fill_rows:
            branch = intent_id_to_branch.get(str(order_intent_id))
            if branch:
                fill_counts_by_branch[branch] += 1
    finally:
        connection.close()

    counts: dict[str, dict[str, int]] = {}
    for item in APPROVED_BRANCHES:
        branch = item["branch"]
        signal_bars = signal_rows_by_branch[branch]
        blocked_count = sum(1 for bar_id in signal_bars if bar_id not in intent_bars_by_branch[branch])
        counts[branch] = {
            "signals": len(signal_bars),
            "blocked_count": blocked_count,
            "entry_intents": len(intent_bars_by_branch[branch]),
            "entry_fills": fill_counts_by_branch[branch],
        }
    return counts


def _load_symbol_timeframe_coverage(*, symbol: str, timeframe: str) -> dict[str, Any]:
    connection = sqlite3.connect(REPLAY_DB_PATH)
    try:
        row = connection.execute(
            "select count(*), min(timestamp), max(timestamp) from bars where ticker = ? and timeframe = ?",
            (symbol, timeframe),
        ).fetchone()
    finally:
        connection.close()
    return {
        "bar_count": int(row[0] or 0),
        "first_bar_ts": row[1],
        "last_bar_ts": row[2],
    }


def _coverage_warning(coverage: dict[str, Any]) -> str | None:
    bar_count = int(coverage.get("bar_count") or 0)
    if bar_count < 2000:
        return "Very thin stored bar coverage for this timeframe."
    if bar_count < 10000:
        return "Coverage is materially thinner than the main futures lane."
    return None


def _sample_warning(*, signals: int, closed_trades: int) -> str | None:
    warnings = []
    if signals < 5:
        warnings.append("Low signal count.")
    if closed_trades < 5:
        warnings.append("Low closed-trade sample.")
    return " ".join(warnings) if warnings else None


def _win_rate(trades: list[dict[str, str]]) -> float | None:
    if len(trades) < 5:
        return None
    wins = sum(1 for row in trades if _float(row.get("net_pnl")) > 0)
    return round(wins / len(trades), 4) if trades else None


def _average_realized_per_trade(trades: list[dict[str, str]]) -> float | None:
    if not trades:
        return None
    return round(sum(_float(row.get("net_pnl")) for row in trades) / len(trades), 4)


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _branch_activity_note(signals: int, blocked_count: int, closed_trades: int) -> str:
    if signals <= 0:
        return "No approved-branch signal activity observed in replay artifacts."
    if closed_trades <= 0 and blocked_count > 0:
        return "Signals were present, but they did not progress to closed trades on this instrument."
    if closed_trades <= 0:
        return "Signals were present, but no closed trade sample is available yet."
    return "Signal and closed-trade activity both appeared on this instrument."


def _apply_portability_labels(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    baseline_by_branch = {}
    for result in results:
        if result["symbol"] == "MGC":
            for row in result["branch_rows"]:
                baseline_by_branch[row["branch"]] = row

    for result in results:
        for row in result["branch_rows"]:
            row["portability_assessment"] = classify_branch_portability(
                branch_metrics=row,
                baseline_metrics=baseline_by_branch.get(row["branch"]),
                instrument_symbol=result["symbol"],
            )
    return results


def classify_branch_portability(
    *,
    branch_metrics: dict[str, Any],
    baseline_metrics: dict[str, Any] | None,
    instrument_symbol: str,
) -> str:
    if instrument_symbol == "MGC":
        return "REFERENCE_LANE"
    signals = int(branch_metrics.get("signals") or 0)
    trades = int(branch_metrics.get("closed_trades") or 0)
    pnl = _float(branch_metrics.get("realized_pnl"))
    avg_trade = _float(branch_metrics.get("average_realized_per_trade"))
    baseline_signals = int((baseline_metrics or {}).get("signals") or 0)
    if signals == 0 and trades == 0:
        return "NO_ACTIVITY"
    if signals < 5 or trades < 5:
        return "THIN_SAMPLE"
    if baseline_signals and signals < max(3, int(baseline_signals * 0.25)):
        return "INSTRUMENT_SPECIFIC_OR_WEAK"
    if pnl > 0 and avg_trade > 0:
        return "PORTABLE_CANDIDATE"
    if pnl <= 0 and trades >= 5:
        return "DEGRADED_OUTSIDE_ORIGINAL_LANE"
    return "MIXED_NEEDS_MORE_RESEARCH"


def _build_futures_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    branch_summary = {}
    promising_instruments = []
    for item in APPROVED_BRANCHES:
        branch = item["branch"]
        portable = [r["symbol"] for r in results if next((row for row in r["branch_rows"] if row["branch"] == branch and row["portability_assessment"] == "PORTABLE_CANDIDATE"), None)]
        degraded = [r["symbol"] for r in results if next((row for row in r["branch_rows"] if row["branch"] == branch and row["portability_assessment"] == "DEGRADED_OUTSIDE_ORIGINAL_LANE"), None)]
        thin = [r["symbol"] for r in results if next((row for row in r["branch_rows"] if row["branch"] == branch and row["portability_assessment"] in {"THIN_SAMPLE", "NO_ACTIVITY", "INSTRUMENT_SPECIFIC_OR_WEAK"}), None)]
        branch_summary[branch] = {
            "portable_candidates": portable,
            "degraded_outside_original_lane": degraded,
            "thin_or_instrument_specific": thin,
        }

    for result in results:
        if result["symbol"] == "MGC":
            continue
        portable_branches = [row["branch"] for row in result["branch_rows"] if row["portability_assessment"] == "PORTABLE_CANDIDATE"]
        if portable_branches:
            promising_instruments.append(
                {
                    "symbol": result["symbol"],
                    "portable_branches": portable_branches,
                    "note": "Research-only promising instrument. Not a paper-promotion recommendation.",
                }
            )

    return {
        "branch_summary": branch_summary,
        "promising_instruments_for_later_review": promising_instruments,
        "research_conclusion": (
            "ready_to_nominate_additional_futures_instruments_for_paper"
            if promising_instruments
            else "research_only_not_yet_paper_ready"
        ),
        "truthfulness_note": "Low-sample and thin-coverage instruments remain explicitly non-promotable in this audit.",
    }


def _build_etf_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    branch_activity = {}
    long_positive = 0
    short_positive = 0
    for item in APPROVED_BRANCHES:
        branch = item["branch"]
        branch_rows = [
            next(row for row in result["branch_rows"] if row["branch"] == branch)
            for result in results
        ]
        branch_activity[branch] = {
            "signals": sum(int(row.get("signals") or 0) for row in branch_rows),
            "closed_trades": sum(int(row.get("closed_trades") or 0) for row in branch_rows),
            "realized_pnl": round(sum(_float(row.get("realized_pnl")) for row in branch_rows), 4),
            "notes": "Research-only ETF pass. No paper/live promotion recommendation from this artifact.",
        }
        if item["side"] == "LONG" and branch_activity[branch]["realized_pnl"] > 0:
            long_positive += 1
        if item["side"] == "SHORT" and branch_activity[branch]["realized_pnl"] > 0:
            short_positive += 1

    if long_positive and not short_positive:
        asymmetry = "Long-side approved branches showed relatively stronger ETF response than the short-side branch."
    elif short_positive and not long_positive:
        asymmetry = "Short-side approved branch showed relatively stronger ETF response than the long-side branches."
    else:
        asymmetry = "No strong long/short asymmetry conclusion is justified from the first ETF pass."

    return {
        "branch_activity": branch_activity,
        "long_short_asymmetry_note": asymmetry,
        "structural_fit_verdict": "ETF lane needs separate modeling assumptions",
        "truthfulness_note": "Leveraged ETF behavior was evaluated in research-only mode with explicit regular-hours assumptions and no promotion recommendation.",
    }


def _load_existing_symbol_coverage(symbols: tuple[str, ...]) -> dict[str, Any]:
    connection = sqlite3.connect(REPLAY_DB_PATH)
    try:
        coverage: dict[str, Any] = {}
        for symbol in symbols:
            rows = connection.execute(
                "select timeframe, count(*), min(timestamp), max(timestamp) from bars where ticker = ? group by timeframe order by timeframe",
                (symbol,),
            ).fetchall()
            coverage[symbol] = [
                {
                    "timeframe": timeframe,
                    "bar_count": int(bar_count or 0),
                    "first_bar_ts": first_bar_ts,
                    "last_bar_ts": last_bar_ts,
                }
                for timeframe, bar_count, first_bar_ts, last_bar_ts in rows
            ]
        return coverage
    finally:
        connection.close()


def _backfill_symbol_stack(
    *,
    symbol: str,
    intraday_start: datetime,
    intraday_end: datetime,
    daily_start: datetime,
) -> dict[str, Any]:
    runs = []
    # 1m chunked lane.
    current = intraday_start
    while current < intraday_end:
        chunk_end = min(current + timedelta(days=14), intraday_end)
        payload = _run_history_fetch(
            symbol=symbol,
            timeframe="1m",
            start=current,
            end=chunk_end,
            period_type="day",
            frequency_type="minute",
            frequency=1,
        )
        runs.append(payload)
        current = chunk_end

    for timeframe, freq in (("5m", 5), ("10m", 10), ("15m", 15), ("30m", 30)):
        runs.append(
            _run_history_fetch(
                symbol=symbol,
                timeframe=timeframe,
                start=intraday_start,
                end=intraday_end,
                period_type="day",
                frequency_type="minute",
                frequency=freq,
            )
        )

    runs.append(
        _run_history_fetch(
            symbol=symbol,
            timeframe="1440m",
            start=daily_start,
            end=intraday_end,
            period_type="year",
            frequency_type="daily",
            frequency=1,
        )
    )
    return {"symbol": symbol, "fetch_runs": runs}


def _run_history_fetch(
    *,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    period_type: str,
    frequency_type: str,
    frequency: int,
) -> dict[str, Any]:
    override = _build_override_yaml(lane="etf")
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", prefix=f"etf_fetch_{symbol.lower()}_", delete=False) as handle:
        handle.write(override)
        override_path = Path(handle.name)
    cmd = [
        str(REPO_ROOT / ".venv" / "bin" / "python"),
        "-m",
        "mgc_v05l.app.main",
        "schwab-fetch-history",
        "--config",
        str(REPO_ROOT / "config" / "base.yaml"),
        "--config",
        str(REPO_ROOT / "config" / "replay.yaml"),
        "--config",
        str(override_path),
        "--schwab-config",
        str(REPO_ROOT / "config" / "schwab.local.json"),
        "--internal-symbol",
        symbol,
        "--historical-symbol",
        symbol,
        "--internal-timeframe",
        timeframe,
        "--period-type",
        period_type,
        "--frequency-type",
        frequency_type,
        "--frequency",
        str(frequency),
        "--start-date-ms",
        str(int(start.timestamp() * 1000)),
        "--end-date-ms",
        str(int(end.timestamp() * 1000)),
        "--persist",
    ]
    try:
        completed = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
            env={**os.environ, "PYTHONPATH": str(REPO_ROOT / "src")},
        )
        payload = json.loads(completed.stdout.strip() or "{}")
        return {"symbol": symbol, "timeframe": timeframe, "ok": True, "fetch_result": payload}
    finally:
        override_path.unlink(missing_ok=True)


def _build_etf_data_readiness(coverage: dict[str, Any]) -> dict[str, Any]:
    readiness = {}
    for symbol, rows in coverage.items():
        by_timeframe = {row["timeframe"]: row for row in rows}
        ready = bool(by_timeframe.get("5m", {}).get("bar_count")) and bool(by_timeframe.get("1440m", {}).get("bar_count"))
        readiness[symbol] = {
            "research_ready": ready,
            "bar_frequency_coverage": sorted(by_timeframe.keys()),
            "gap_notes": _build_gap_notes(symbol, by_timeframe),
            "session_assumptions_used": "Regular-hours ETF lane only (09:30-16:00 ET, no Asia/London, no extended hours).",
        }
    return readiness


def _build_gap_notes(symbol: str, by_timeframe: dict[str, dict[str, Any]]) -> list[str]:
    notes = []
    five_minute = by_timeframe.get("5m")
    if not five_minute:
        return ["5m ETF bars are missing."]
    connection = sqlite3.connect(REPLAY_DB_PATH)
    try:
        rows = connection.execute(
            "select timestamp from bars where ticker = ? and timeframe = '5m' order by timestamp",
            (symbol,),
        ).fetchall()
    finally:
        connection.close()
    same_day_gap_count = 0
    last_dt: datetime | None = None
    for (timestamp,) in rows:
        current_dt = datetime.fromisoformat(str(timestamp))
        if last_dt and current_dt.date() == last_dt.date():
            if (current_dt - last_dt) > timedelta(minutes=10):
                same_day_gap_count += 1
        last_dt = current_dt
    if same_day_gap_count:
        notes.append(f"Observed {same_day_gap_count} same-session 5m gaps greater than 10 minutes.")
    else:
        notes.append("No same-session 5m gaps greater than 10 minutes were observed.")
    return notes


def _write_json_and_markdown(*, stem: str, payload: dict[str, Any], markdown: str) -> dict[str, str]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / f"{stem}.json"
    md_path = OUTPUT_DIR / f"{stem}.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(markdown.strip() + "\n", encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}


def _render_second_pass_direct_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Approved Branch Futures Direct Portability Second Pass",
        "",
        f"Baseline reference: {payload['baseline_reference_symbol']}",
        "",
    ]
    grouped = _group_second_pass_rows(payload["direct_results"])
    for branch, rows in grouped.items():
        lines.append(f"## {branch}")
        for row in rows:
            lines.append(
                f"- {row['symbol']}: trades={row['closed_trades']}, win_rate={row['win_rate']}, realized_pnl={row['realized_pnl']}, "
                f"avg_trade={row['average_realized_per_trade']}, largest_win={row['largest_win']}, largest_loss={row['largest_loss']}, "
                f"blocked={row['blocked_decisions']}, intents={row['intents']}, fills={row['fills']}, "
                f"dominant_phase={row['dominant_entry_phase']} ({row['dominant_entry_phase_share']}), readiness={row['promotion_readiness']}"
            )
        lines.append("")
    return "\n".join(lines)


def _render_second_pass_adaptation_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Approved Branch Futures Narrow Adaptation Second Pass",
        "",
        "Only narrow, explicit separator/session-band tests are included here.",
        "",
    ]
    grouped = _group_second_pass_rows(payload["adapted_results"])
    for branch, rows in grouped.items():
        lines.append(f"## {branch}")
        for row in rows:
            delta = row.get("delta_vs_direct") or {}
            lines.append(
                f"- {row['symbol']} / {row['variant']}: realized_pnl={row['realized_pnl']} "
                f"(delta {delta.get('realized_pnl_delta')}), avg_trade={row['average_realized_per_trade']} "
                f"(delta {delta.get('avg_realized_per_trade_delta')}), trades={row['closed_trades']} "
                f"(delta {delta.get('closed_trades_delta')}), overrides={json.dumps(row['variant_overrides'], sort_keys=True)}"
            )
        lines.append("")
    return "\n".join(lines)


def _render_second_pass_recommendations_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Approved Branch Futures Promotion Recommendations Second Pass",
        "",
        "## Strongest next paper candidates",
    ]
    for row in payload["promote_now"]:
        lines.append(f"- {row['symbol']} / {row['branch']}: {row['rationale']}")
    if not payload["promote_now"]:
        lines.append("- None")
    lines.extend(["", "## Credible but later-review"])
    for row in payload["later_review"]:
        lines.append(f"- {row['symbol']} / {row['branch']}: {row['rationale']}")
    if not payload["later_review"]:
        lines.append("- None")
    lines.extend(["", "## Not worth pursuing now"])
    for row in payload["reject_for_now"]:
        lines.append(f"- {row['symbol']} / {row['branch']}: {row['rationale']}")
    if not payload["reject_for_now"]:
        lines.append("- None")
    return "\n".join(lines)


def _group_second_pass_rows(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["branch"]), []).append(row)
    for branch_rows in grouped.values():
        branch_rows.sort(key=lambda item: (item["symbol"], item["variant"]))
    return grouped


def _render_deployment_readiness_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Approved Branch Futures Deployment Readiness",
        "",
        f"Sample start: {payload.get('sample_start_date')}",
        f"Sample end: {payload.get('sample_end_date')}",
        f"Trading days/sessions: {payload.get('number_of_trading_days_or_sessions')}",
        f"Bars used: {payload.get('number_of_bars_used')}",
        f"History scope: {payload.get('history_window_type')}",
        f"MGC comparison periods identical for all rows: {payload.get('mgc_comparison_periods_exactly_identical')}",
        "",
        "## Promote-now shortlist",
    ]
    for row in payload["promote_shortlist"]:
        lines.extend(
            [
                f"### {row['symbol']} / {row['branch']}",
                f"- Sample: {row['sample_start_date']} to {row['sample_end_date']}",
                f"- Trading days/sessions: {row['number_of_trading_days_or_sessions']}",
                f"- Bars used: {row['number_of_bars_used']}",
                f"- History scope: {row['history_window_type']}",
                f"- Exact period match vs MGC home case: {row['baseline_comparison']['period_exact_match_vs_mgc_home_case']}",
                f"- Trades: {row['total_trades']}",
                f"- Win rate: {row['win_rate']}",
                f"- Realized P/L: {row['realized_pnl']}",
                f"- Avg realized/trade: {row['average_realized_per_trade']}",
                f"- Largest win/loss: {row['largest_win']} / {row['largest_loss']}",
                f"- Loss tail: {row['loss_shape']['tail_note']}",
                f"- Session concentration: {row['session_concentration']['dominant_phase']} ({row['session_concentration']['dominant_phase_share']})",
                f"- Intraday dependence: dominant {row['intraday_dependence']['dominant_bucket']} ({row['intraday_dependence']['dominant_bucket_share']})",
                f"- Broadness: {row['broadness_assessment']['label']} - {row['broadness_assessment']['note']}",
                f"- Trade frequency: {row['trade_frequency_assessment']['label']} - {row['trade_frequency_assessment']['note']}",
                f"- Structural similarity: {row['structural_similarity']['label']} - {row['structural_similarity']['note']}",
                f"- Guardrails: {', '.join(row['recommended_paper_guardrails']) or 'None beyond standard paper monitoring.'}",
                "",
            ]
        )
    lines.append("## Later-review bucket")
    for row in payload["later_review"]:
        lines.append(
            f"- {row['symbol']} / {row['branch']}: trades={row['total_trades']}, realized_pnl={row['realized_pnl']}, "
            f"broadness={row['broadness_assessment']['label']}, frequency={row['trade_frequency_assessment']['label']}"
        )
    lines.extend(
        [
            "",
            "## GC overlap review",
            f"- Conflict label: {payload['gc_overlap_review']['conflict_label']}",
            f"- Recommendation: {payload['gc_overlap_review']['recommendation']}",
            f"- If only one goes first: {payload['gc_overlap_review']['if_only_one_goes_first']}",
        ]
    )
    return "\n".join(lines)


def _render_deployment_ranking_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Approved Branch Futures Deployment Ranking",
        "",
        f"Sample start: {payload.get('sample_start_date')}",
        f"Sample end: {payload.get('sample_end_date')}",
        f"Trading days/sessions: {payload.get('number_of_trading_days_or_sessions')}",
        f"Bars used: {payload.get('number_of_bars_used')}",
        f"History scope: {payload.get('history_window_type')}",
        f"MGC comparison periods identical for all rows: {payload.get('mgc_comparison_periods_exactly_identical')}",
        "",
        "## Tier 1 = strongest immediate probationary paper candidates",
    ]
    for row in payload["tier_1"]:
        lines.append(
            f"- {row['symbol']} / {row['branch']}: {row['rationale']} "
            f"Sample={row['sample_start_date']} to {row['sample_end_date']}, "
            f"days={row['number_of_trading_days_or_sessions']}, bars={row['number_of_bars_used']}, "
            f"scope={row['history_window_type']}, mgc_period_match={row['period_exact_match_vs_mgc_home_case']}"
        )
    if not payload["tier_1"]:
        lines.append("- None")
    lines.extend(["", "## Tier 2 = needs one explicit guardrail first"])
    for row in payload["tier_2"]:
        lines.append(
            f"- {row['symbol']} / {row['branch']}: {row['rationale']} Guardrails: {', '.join(row['guardrails']) or 'None'} "
            f"Sample={row['sample_start_date']} to {row['sample_end_date']}, "
            f"days={row['number_of_trading_days_or_sessions']}, bars={row['number_of_bars_used']}, "
            f"scope={row['history_window_type']}, mgc_period_match={row['period_exact_match_vs_mgc_home_case']}"
        )
    if not payload["tier_2"]:
        lines.append("- None")
    lines.extend(["", "## Tier 3 = credible but later-review only"])
    for row in payload["tier_3"]:
        lines.append(
            f"- {row['symbol']} / {row['branch']}: {row['rationale']} "
            f"Sample={row['sample_start_date']} to {row['sample_end_date']}, "
            f"days={row['number_of_trading_days_or_sessions']}, bars={row['number_of_bars_used']}, "
            f"scope={row['history_window_type']}, mgc_period_match={row['period_exact_match_vs_mgc_home_case']}"
        )
    if not payload["tier_3"]:
        lines.append("- None")
    lines.extend(["", "## Tier 4 = not worth more work now"])
    for row in payload["tier_4"]:
        lines.append(
            f"- {row['symbol']} / {row['branch']}: {row['rationale']} "
            f"Sample={row['sample_start_date']} to {row['sample_end_date']}, "
            f"days={row['number_of_trading_days_or_sessions']}, bars={row['number_of_bars_used']}, "
            f"scope={row['history_window_type']}, mgc_period_match={row['period_exact_match_vs_mgc_home_case']}"
        )
    if not payload["tier_4"]:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Explicit answers",
            f"- Next probationary paper order: {', '.join(payload['answers']['next_probationary_paper_order']) or 'None'}",
            f"- GC first choice: {payload['answers']['gc_first_choice']}",
            f"- Later-review only: {', '.join(payload['answers']['later_review_only']) or 'None'}",
            f"- Too thin / unstable now: {', '.join(payload['answers']['too_thin_or_unstable']) or 'None'}",
            f"- Smallest defensible next expansion: {payload['answers']['smallest_defensible_next_expansion']}",
        ]
    )
    return "\n".join(lines)


def _render_quantitative_deployment_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Approved Branch Futures Quantitative Deployment Report",
        "",
        f"Sample start: {payload.get('sample_start_date')}",
        f"Sample end: {payload.get('sample_end_date')}",
        f"Trading days/sessions: {payload.get('number_of_trading_days_or_sessions')}",
        f"Bars used: {payload.get('number_of_bars_used')}",
        f"History scope: {payload.get('history_window_type')}",
        f"MGC comparison periods identical for all rows: {payload.get('mgc_comparison_periods_exactly_identical')}",
        "",
        "| Rank | Pair | Bucket | Sample Start | Sample End | Days/Sessions | Bars | Window | MGC Period Match | Trades | Win % | Realized | Avg/Trade | Median | Gross Wins | Gross Losses | PF | Max DD | DD Range | Trades/Active Month | Longest Gap (days) | Dominant Session % | Top 5 P/L % | No Top 1 | No Top 3 | Rel Trades vs MGC | Rel Avg vs MGC | Rel P/L vs MGC | Rel Max DD vs MGC | Sharpe* |",
        "| --- | --- | --- | --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for idx, row in enumerate(payload["ranked_pairs"], start=1):
        lines.append(
            "| {rank} | {pair} | {bucket} | {sample_start} | {sample_end} | {trading_days} | {bars_used} | {window} | {mgc_match} | {trades} | {win_pct} | {realized} | {avg_trade} | {median_trade} | {gross_wins} | {gross_losses} | {profit_factor} | {max_dd} | {dd_range} | {trades_per_month} | {longest_gap_days} | {dominant_pct} | {top5_pct} | {no_top1} | {no_top3} | {rel_trades} | {rel_avg} | {rel_pnl} | {rel_dd} | {sharpe} |".format(
                rank=idx,
                pair=f"{row['symbol']} / {row['branch']}",
                bucket=row["recommendation_bucket"],
                sample_start=row["sample_start_date"],
                sample_end=row["sample_end_date"],
                trading_days=row["number_of_trading_days_or_sessions"],
                bars_used=row["number_of_bars_used"],
                window=row["history_window_type"],
                mgc_match=row["baseline_comparison"]["period_exact_match_vs_mgc_home_case"],
                trades=row["number_of_trades"],
                win_pct=_fmt_num(row["win_rate_pct"], 1),
                realized=_fmt_num(row["total_realized_pnl"], 4),
                avg_trade=_fmt_num(row["average_realized_pnl_per_trade"], 4),
                median_trade=_fmt_num(row["median_realized_pnl_per_trade"], 4),
                gross_wins=_fmt_num(row["gross_wins"], 4),
                gross_losses=_fmt_num(row["gross_losses"], 4),
                profit_factor=_fmt_num(row["profit_factor"], 3),
                max_dd=_fmt_num(row["max_drawdown"], 4),
                dd_range=_fmt_dd_range(row["max_drawdown_range"]),
                trades_per_month=_fmt_num(row["average_trades_per_active_month"], 2),
                longest_gap_days=_fmt_num(row["longest_no_trade_stretch"]["calendar_days"], 1),
                dominant_pct=_fmt_num(row["dominant_session_trade_share_pct"], 1),
                top5_pct=_fmt_num(row["top_5_trade_pnl_share_pct"], 1),
                no_top1=_fmt_survival(row["robustness"]["without_top_1_trade"]),
                no_top3=_fmt_survival(row["robustness"]["without_top_3_trades"]),
                rel_trades=_fmt_num(row["baseline_comparison"]["relative_trade_count"], 3),
                rel_avg=_fmt_num(row["baseline_comparison"]["relative_avg_trade"], 3),
                rel_pnl=_fmt_num(row["baseline_comparison"]["relative_total_pnl"], 3),
                rel_dd=_fmt_num(row["baseline_comparison"]["relative_max_drawdown"], 3),
                sharpe=_fmt_num(row["per_trade_sharpe_proxy"], 3),
            )
        )
    lines.extend(
        [
            "",
            "* `Sharpe*` is a per-trade realized-series proxy, not a bar-return Sharpe.",
            "",
            "## GC Side-By-Side",
            "",
            "| Pair | Trades | Realized | Avg/Trade | Max DD | Dominant Session | Shared Calendar Dates | Shared Date Overlap % | Shared-Date Realized | Relationship |",
            "| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | --- |",
        ]
    )
    for row in payload["gc_side_by_side"]["pairs"]:
        lines.append(
            "| {pair} | {trades} | {realized} | {avg_trade} | {max_dd} | {session} | {shared_dates} | {shared_pct} | {shared_pnl} | {relationship} |".format(
                pair=f"{row['symbol']} / {row['branch']}",
                trades=row["number_of_trades"],
                realized=_fmt_num(row["total_realized_pnl"], 4),
                avg_trade=_fmt_num(row["average_realized_pnl_per_trade"], 4),
                max_dd=_fmt_num(row["max_drawdown"], 4),
                session=f"{row['dominant_session_pocket']} ({_fmt_num(row['dominant_session_trade_share_pct'], 1)}%)",
                shared_dates=payload["gc_side_by_side"]["shared_calendar_dates_count"],
                shared_pct=_fmt_num(payload["gc_side_by_side"]["shared_date_overlap_pct"][row["branch"]], 1),
                shared_pnl=_fmt_num(payload["gc_side_by_side"]["shared_date_realized_pnl"][row["branch"]], 4),
                relationship=payload["gc_side_by_side"]["overlap_assessment"],
            )
        )
    lines.extend(
        [
            "",
            f"- Shared date sample: {', '.join(payload['gc_side_by_side']['shared_calendar_dates_sample']) or 'None'}",
            "",
            "## Recommendation Buckets",
        ]
    )
    for bucket, rows in payload["recommendations"]["buckets"].items():
        lines.append(f"### {bucket}")
        lines.append("")
        lines.append("| Pair | Trades | Realized | Max DD | Top 5 P/L % | No Top 3 | Dominant Session % |")
        lines.append("| --- | ---: | ---: | ---: | ---: | --- | ---: |")
        if not rows:
            lines.append("| None | - | - | - | - | - | - |")
        for row in rows:
            lines.append(
                "| {pair} | {trades} | {realized} | {max_dd} | {top5_pct} | {no_top3} | {dominant_pct} |".format(
                    pair=f"{row['symbol']} / {row['branch']}",
                    trades=row["number_of_trades"],
                    realized=_fmt_num(row["total_realized_pnl"], 4),
                    max_dd=_fmt_num(row["max_drawdown"], 4),
                    top5_pct=_fmt_num(row["top_5_trade_pnl_share_pct"], 1),
                    no_top3=_fmt_survival(row["robustness"]["without_top_3_trades"]),
                    dominant_pct=_fmt_num(row["dominant_session_trade_share_pct"], 1),
                )
            )
        lines.append("")
    return "\n".join(lines)


def _build_deployment_readiness_row(direct_row: dict[str, Any]) -> dict[str, Any]:
    ledger_rows = _load_trade_rows(Path(direct_row["artifact_paths"]["trade_ledger"]))
    horizon = _sample_horizon_from_artifacts(direct_row["artifact_paths"])
    branch = direct_row["branch"]
    trades = [row for row in ledger_rows if row.get("setup_family") == branch]
    monthly = _monthly_trade_summary(trades)
    hourly = _intraday_trade_summary(trades)
    loss = _loss_shape_summary(trades)
    broadness = _broadness_assessment(trades, monthly=monthly, hourly=hourly)
    thick_enough = _trade_frequency_assessment(len(trades), len(monthly["months"]))
    structural_similarity = _structural_similarity_assessment(direct_row)
    return {
        "symbol": direct_row["symbol"],
        "branch": branch,
        "side": direct_row["side"],
        "sample_start_date": horizon["sample_start_date"],
        "sample_end_date": horizon["sample_end_date"],
        "number_of_trading_days_or_sessions": horizon["number_of_trading_days_or_sessions"],
        "number_of_bars_used": horizon["number_of_bars_used"],
        "history_window_type": horizon["history_window_type"],
        "period_exact_match_vs_mgc_home_case": _period_exact_match_against_mgc_home_case(horizon, branch),
        "direct_artifact_paths": direct_row["artifact_paths"],
        "total_trades": direct_row["closed_trades"],
        "win_rate": direct_row["win_rate"],
        "realized_pnl": direct_row["realized_pnl"],
        "average_realized_per_trade": direct_row["average_realized_per_trade"],
        "largest_win": direct_row["largest_win"],
        "largest_loss": direct_row["largest_loss"],
        "loss_shape": loss,
        "session_concentration": {
            "dominant_phase": direct_row["dominant_entry_phase"],
            "dominant_phase_share": direct_row["dominant_entry_phase_share"],
            "phase_counts": direct_row["entry_phase_counts"],
        },
        "intraday_dependence": hourly,
        "monthly_consistency": monthly,
        "broadness_assessment": broadness,
        "trade_frequency_assessment": thick_enough,
        "structural_similarity": structural_similarity,
        "promotion_readiness": direct_row["promotion_readiness"],
        "portability_assessment": direct_row["portability_assessment"],
        "recommended_paper_guardrails": _recommended_paper_guardrails(direct_row, broadness=broadness, frequency=thick_enough),
        "baseline_comparison": {
            "period_exact_match_vs_mgc_home_case": _period_exact_match_against_mgc_home_case(horizon, branch),
        },
    }


def _monthly_trade_summary(trades: list[dict[str, str]]) -> dict[str, Any]:
    month_rows: dict[str, dict[str, Any]] = {}
    for row in trades:
        month = str(row.get("entry_ts", ""))[:7]
        net = _float(row.get("net_pnl"))
        bucket = month_rows.setdefault(month, {"trades": 0, "realized_pnl": 0.0, "wins": 0, "losses": 0})
        bucket["trades"] += 1
        bucket["realized_pnl"] += net
        if net > 0:
            bucket["wins"] += 1
        elif net < 0:
            bucket["losses"] += 1
    months = [
        {
            "month": month,
            "trades": values["trades"],
            "realized_pnl": round(values["realized_pnl"], 4),
            "wins": values["wins"],
            "losses": values["losses"],
        }
        for month, values in sorted(month_rows.items())
    ]
    total_positive = sum(max(item["realized_pnl"], 0.0) for item in months)
    best_month = max(months, key=lambda item: item["realized_pnl"], default=None)
    worst_month = min(months, key=lambda item: item["realized_pnl"], default=None)
    profitable_months = sum(1 for item in months if item["realized_pnl"] > 0)
    negative_months = sum(1 for item in months if item["realized_pnl"] < 0)
    best_share = (
        round(max(best_month["realized_pnl"], 0.0) / total_positive, 4)
        if best_month is not None and total_positive > 0
        else None
    )
    return {
        "months": months,
        "active_months": len(months),
        "profitable_months": profitable_months,
        "negative_months": negative_months,
        "best_month": best_month,
        "worst_month": worst_month,
        "best_month_positive_pnl_share": best_share,
    }


def _intraday_trade_summary(trades: list[dict[str, str]]) -> dict[str, Any]:
    bucket_counts: Counter[str] = Counter()
    for row in trades:
        entry_ts = row.get("entry_ts")
        if not entry_ts:
            continue
        parsed = datetime.fromisoformat(entry_ts)
        minute_bucket = "00" if parsed.minute < 30 else "30"
        bucket_counts[f"{parsed.hour:02d}:{minute_bucket}"] += 1
    if not bucket_counts:
        return {"dominant_bucket": None, "dominant_bucket_share": None, "bucket_counts": {}}
    dominant_bucket, count = bucket_counts.most_common(1)[0]
    return {
        "dominant_bucket": dominant_bucket,
        "dominant_bucket_share": round(count / sum(bucket_counts.values()), 4),
        "bucket_counts": dict(sorted(bucket_counts.items())),
    }


def _loss_shape_summary(trades: list[dict[str, str]]) -> dict[str, Any]:
    pnls = [_float(row.get("net_pnl")) for row in trades]
    losses = sorted((abs(value) for value in pnls if value < 0), reverse=True)
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in pnls:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    total_losses = sum(losses)
    worst_loss_share = round((losses[0] / total_losses), 4) if losses and total_losses > 0 else None
    return {
        "max_drawdown_estimate": round(max_drawdown, 4),
        "loss_count": len(losses),
        "worst_loss_share_of_total_losses": worst_loss_share,
        "tail_note": _loss_tail_note(losses, worst_loss_share),
    }


def _loss_tail_note(losses: list[float], worst_loss_share: float | None) -> str:
    if not losses:
        return "No losing trades in the replay sample."
    if len(losses) < 5:
        return "Loss tail remains low-confidence because the losing-trade sample is still small."
    if worst_loss_share is not None and worst_loss_share >= 0.35:
        return "Losses are concentrated in a small tail of outsized losers."
    return "Losses are distributed across the sample rather than dominated by one outsized outlier."


def _broadness_assessment(trades: list[dict[str, str]], *, monthly: dict[str, Any], hourly: dict[str, Any]) -> dict[str, Any]:
    active_months = int(monthly["active_months"])
    best_share = monthly.get("best_month_positive_pnl_share")
    dominant_bucket_share = hourly.get("dominant_bucket_share")
    if active_months < 3:
        label = "NARROW_POCKET"
        note = "The sample spans too few active months to call broad."
    elif best_share is not None and best_share > 0.6:
        label = "POCKET_DEPENDENT"
        note = "A single month contributes more than 60% of the positive realized result."
    elif dominant_bucket_share is not None and dominant_bucket_share > 0.5:
        label = "TIME_BUCKET_DEPENDENT"
        note = "More than half of the trades concentrate in one 30-minute bucket."
    else:
        label = "BROAD_ENOUGH_FOR_PROBATIONARY_REVIEW"
        note = "Activity is spread across enough months and intraday buckets to avoid a one-pocket result."
    return {"label": label, "note": note}


def _trade_frequency_assessment(total_trades: int, active_months: int) -> dict[str, Any]:
    if total_trades >= 20 and active_months >= 4:
        return {"label": "THICK_ENOUGH", "note": "Trade frequency is thick enough for probationary paper review."}
    if total_trades >= 12 and active_months >= 4:
        return {"label": "MODERATE_BUT_SPARSE", "note": "Trade frequency is real but sparse enough that paper review should expect gaps."}
    return {"label": "THIN", "note": "Trade frequency is too thin for confident probationary paper review."}


def _structural_similarity_assessment(direct_row: dict[str, Any]) -> dict[str, Any]:
    expected_phase = _expected_phase_for_branch(direct_row["branch"])
    actual_phase = direct_row["dominant_entry_phase"]
    share = float(direct_row.get("dominant_entry_phase_share") or 0)
    aligned = expected_phase == actual_phase and share >= 0.8
    return {
        "label": "STRUCTURALLY_ALIGNED" if aligned else "STRUCTURALLY_DRIFTED",
        "note": (
            f"Trades remain concentrated in the expected {expected_phase} pocket."
            if aligned
            else f"Trades drifted away from the expected {expected_phase} pocket toward {actual_phase}."
        ),
    }


def _recommended_paper_guardrails(direct_row: dict[str, Any], *, broadness: dict[str, Any], frequency: dict[str, Any]) -> list[str]:
    guardrails = []
    phase = direct_row.get("dominant_entry_phase")
    if phase == "US_LATE":
        guardrails.append("Restrict paper review to the US_LATE session pocket.")
    elif phase == "ASIA_EARLY":
        guardrails.append("Restrict paper review to the ASIA_EARLY session pocket.")
    if frequency["label"] == "MODERATE_BUT_SPARSE":
        guardrails.append("Expect sparse trade cadence; evaluate over a longer probationary paper window before judging inactivity.")
    if broadness["label"] in {"POCKET_DEPENDENT", "TIME_BUCKET_DEPENDENT"}:
        guardrails.append("Review time-of-day concentration before widening paper expectations beyond the observed pocket.")
    return guardrails


def _build_gc_overlap_review(*, gc_us_late: dict[str, Any], gc_asia: dict[str, Any]) -> dict[str, Any]:
    us_trades = [row for row in _load_trade_rows(Path(gc_us_late["artifact_paths"]["trade_ledger"])) if row.get("setup_family") == gc_us_late["branch"]]
    asia_trades = [row for row in _load_trade_rows(Path(gc_asia["artifact_paths"]["trade_ledger"])) if row.get("setup_family") == gc_asia["branch"]]
    us_dates = {str(row.get("entry_ts"))[:10] for row in us_trades}
    asia_dates = {str(row.get("entry_ts"))[:10] for row in asia_trades}
    shared_dates = sorted(us_dates & asia_dates)
    clean_session_separation = gc_us_late["dominant_entry_phase"] != gc_asia["dominant_entry_phase"]
    conflict_label = "DIFFERENT_SESSION_POCKETS_CLEAN" if clean_session_separation else "POTENTIAL_SESSION_OVERLAP"
    first_choice = "GC / asiaEarlyNormalBreakoutRetestHoldTurn" if gc_asia["closed_trades"] > gc_us_late["closed_trades"] else "GC / usLatePauseResumeLongTurn"
    return {
        "pairs": [
            {"symbol": "GC", "branch": gc_us_late["branch"], "dominant_phase": gc_us_late["dominant_entry_phase"], "trades": gc_us_late["closed_trades"]},
            {"symbol": "GC", "branch": gc_asia["branch"], "dominant_phase": gc_asia["dominant_entry_phase"], "trades": gc_asia["closed_trades"]},
        ],
        "conflict_label": conflict_label,
        "shared_trade_dates_count": len(shared_dates),
        "shared_trade_dates_sample": shared_dates[:10],
        "recommendation": (
            "They operate in different session pockets cleanly and can be sequenced or co-enabled without obvious direct overlap."
            if clean_session_separation
            else "They appear to overlap materially and should be sequenced rather than co-enabled."
        ),
        "if_only_one_goes_first": first_choice,
        "first_choice_rationale": (
            "Choose the Asia breakout family first on GC because it carries the thicker GC sample while staying fully concentrated in ASIA_EARLY."
            if first_choice.endswith("asiaEarlyNormalBreakoutRetestHoldTurn")
            else "Choose the US late pause/resume family first on GC because it is the cleaner direct transfer with the simpler session pocket."
        ),
    }


def _build_deployment_ranking(
    *,
    promote_rows: list[dict[str, Any]],
    later_rows: list[dict[str, Any]],
    recommendations_payload: dict[str, Any],
    gc_overlap_review: dict[str, Any],
) -> dict[str, Any]:
    tier_1 = []
    tier_2 = []
    tier_3 = []
    tier_4 = []

    for row in promote_rows:
        if row["trade_frequency_assessment"]["label"] == "THICK_ENOUGH" and row["broadness_assessment"]["label"] == "BROAD_ENOUGH_FOR_PROBATIONARY_REVIEW":
            tier_1.append(_ranking_row(row))
        else:
            tier_2.append(_ranking_row(row))

    for row in later_rows:
        if row["trade_frequency_assessment"]["label"] == "THIN":
            tier_4.append(_ranking_row(row))
        else:
            tier_3.append(_ranking_row(row))

    tier_1.sort(key=lambda item: (-item["recommendation_score"], item["symbol"], item["branch"]))
    tier_2.sort(key=lambda item: (-item["recommendation_score"], item["symbol"], item["branch"]))
    tier_3.sort(key=lambda item: (-item["recommendation_score"], item["symbol"], item["branch"]))
    tier_4.sort(key=lambda item: (-item["recommendation_score"], item["symbol"], item["branch"]))

    return {
        "tier_1": tier_1,
        "tier_2": tier_2,
        "tier_3": tier_3,
        "tier_4": tier_4,
        "gc_overlap_review": gc_overlap_review,
        "answers": {
            "next_probationary_paper_order": [f"{row['symbol']} / {row['branch']}" for row in (tier_1 + tier_2)],
            "gc_first_choice": gc_overlap_review["if_only_one_goes_first"],
            "later_review_only": [f"{row['symbol']} / {row['branch']}" for row in tier_3],
            "too_thin_or_unstable": [f"{row['symbol']} / {row['branch']}" for row in tier_4],
            "smallest_defensible_next_expansion": (
                "After the Tier 1 slate, the smallest defensible next expansion is CL / usLatePauseResumeLongTurn, then NG / usLatePauseResumeLongTurn."
            ),
        },
    }


def _ranking_row(readiness_row: dict[str, Any]) -> dict[str, Any]:
    score = (
        float(readiness_row["total_trades"]) * 0.5
        + _float(readiness_row["realized_pnl"])
        + (_float(readiness_row["average_realized_per_trade"]) * 2.0)
        + (_float(readiness_row["win_rate"]) * 20.0)
    )
    return {
        "symbol": readiness_row["symbol"],
        "branch": readiness_row["branch"],
        "sample_start_date": readiness_row["sample_start_date"],
        "sample_end_date": readiness_row["sample_end_date"],
        "number_of_trading_days_or_sessions": readiness_row["number_of_trading_days_or_sessions"],
        "number_of_bars_used": readiness_row["number_of_bars_used"],
        "history_window_type": readiness_row["history_window_type"],
        "period_exact_match_vs_mgc_home_case": readiness_row["baseline_comparison"]["period_exact_match_vs_mgc_home_case"],
        "recommendation_score": round(score, 4),
        "trade_frequency_label": readiness_row["trade_frequency_assessment"]["label"],
        "broadness_label": readiness_row["broadness_assessment"]["label"],
        "guardrails": readiness_row["recommended_paper_guardrails"],
        "rationale": _deployment_rationale(readiness_row),
    }


def _deployment_rationale(readiness_row: dict[str, Any]) -> str:
    return (
        f"{readiness_row['symbol']} / {readiness_row['branch']} produced {readiness_row['total_trades']} trades, "
        f"{readiness_row['realized_pnl']} realized P/L, {readiness_row['average_realized_per_trade']} average realized/trade, "
        f"and stayed concentrated in {readiness_row['session_concentration']['dominant_phase']}."
    )


def _load_baseline_home_cases(portability_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    mgc_result = next((row for row in portability_payload.get("results", []) if row.get("symbol") == "MGC"), None)
    if mgc_result is None:
        return {}
    summary_payload = json.loads(Path(mgc_result["artifact_paths"]["replay_summary"]).read_text(encoding="utf-8"))
    trade_rows = _load_trade_rows(Path(mgc_result["artifact_paths"]["trade_ledger"]))
    baselines: dict[str, dict[str, Any]] = {}
    for branch_row in mgc_result.get("branch_rows", []):
        branch = str(branch_row["branch"])
        branch_trades = [row for row in trade_rows if row.get("setup_family") == branch]
        baselines[branch] = {
            "symbol": "MGC",
            "branch": branch,
            "artifact_paths": mgc_result["artifact_paths"],
            "summary": summary_payload,
            "horizon": _sample_horizon_from_artifacts(mgc_result["artifact_paths"]),
            "trade_rows": branch_trades,
            "branch_row": branch_row,
        }
    return baselines


def _build_quantitative_recommendation_map(ranking_payload: dict[str, Any]) -> tuple[dict[tuple[str, str], dict[str, Any]], list[tuple[str, str]]]:
    tier_mapping = {
        "tier_1": "PROMOTE_TO_PAPER_NOW",
        "tier_2": "PAPER_AFTER_GUARDRAIL",
        "tier_3": "LATER_REVIEW",
        "tier_4": "REJECT_FOR_NOW",
    }
    recommendation_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    ranking_order: list[tuple[str, str]] = []
    for tier_name in ("tier_1", "tier_2", "tier_3", "tier_4"):
        for row in ranking_payload.get(tier_name, []):
            pair = (str(row["symbol"]), str(row["branch"]))
            recommendation_by_pair[pair] = {
                "bucket": tier_mapping[tier_name],
                "ranking_tier": tier_name.upper(),
                "recommendation_score": row.get("recommendation_score"),
                "guardrails": row.get("guardrails") or [],
            }
            ranking_order.append(pair)
    return recommendation_by_pair, ranking_order


def _build_quantitative_deployment_row(
    *,
    direct_row: dict[str, Any],
    baseline_case: dict[str, Any] | None,
    recommendation: dict[str, Any] | None,
) -> dict[str, Any]:
    summary = json.loads(Path(direct_row["artifact_paths"]["replay_summary"]).read_text(encoding="utf-8"))
    horizon = _sample_horizon_from_artifacts(direct_row["artifact_paths"])
    trade_rows = _load_trade_rows(Path(direct_row["artifact_paths"]["trade_ledger"]))
    branch = str(direct_row["branch"])
    branch_trades = [row for row in trade_rows if row.get("setup_family") == branch]
    metrics = _compute_trade_metrics(
        trades=branch_trades,
        range_start=summary.get("source_first_bar_ts"),
        range_end=summary.get("source_last_bar_ts"),
        dominant_session=direct_row.get("dominant_entry_phase"),
    )
    baseline_metrics = _compute_trade_metrics(
        trades=list(baseline_case["trade_rows"]) if baseline_case else [],
        range_start=(baseline_case or {}).get("summary", {}).get("source_first_bar_ts") if baseline_case else None,
        range_end=(baseline_case or {}).get("summary", {}).get("source_last_bar_ts") if baseline_case else None,
        dominant_session=None,
    )
    return {
        "symbol": direct_row["symbol"],
        "branch": branch,
        "side": direct_row["side"],
        "sample_start_date": horizon["sample_start_date"],
        "sample_end_date": horizon["sample_end_date"],
        "number_of_trading_days_or_sessions": horizon["number_of_trading_days_or_sessions"],
        "number_of_bars_used": horizon["number_of_bars_used"],
        "history_window_type": horizon["history_window_type"],
        "period_exact_match_vs_mgc_home_case": _baseline_horizon_exact_match(horizon, (baseline_case or {}).get("horizon")),
        "recommendation_bucket": (recommendation or {}).get("bucket", "UNKNOWN"),
        "ranking_tier": (recommendation or {}).get("ranking_tier", "UNKNOWN"),
        "recommendation_score": (recommendation or {}).get("recommendation_score"),
        "guardrails": list((recommendation or {}).get("guardrails") or []),
        "date_range_tested": metrics["date_range_tested"],
        "number_of_trades": metrics["number_of_trades"],
        "win_rate_pct": metrics["win_rate_pct"],
        "total_realized_pnl": metrics["total_realized_pnl"],
        "average_realized_pnl_per_trade": metrics["average_realized_pnl_per_trade"],
        "median_realized_pnl_per_trade": metrics["median_realized_pnl_per_trade"],
        "gross_wins": metrics["gross_wins"],
        "gross_losses": metrics["gross_losses"],
        "profit_factor": metrics["profit_factor"],
        "max_drawdown": metrics["max_drawdown"],
        "max_drawdown_range": metrics["max_drawdown_range"],
        "average_trades_per_active_month": metrics["average_trades_per_active_month"],
        "active_months": metrics["active_months"],
        "longest_no_trade_stretch": metrics["longest_no_trade_stretch"],
        "dominant_session_pocket": metrics["dominant_session_pocket"],
        "dominant_session_trade_share_pct": metrics["dominant_session_trade_share_pct"],
        "top_5_trade_pnl_share_pct": metrics["top_5_trade_pnl_share_pct"],
        "robustness": metrics["robustness"],
        "per_trade_sharpe_proxy": metrics["per_trade_sharpe_proxy"],
        "signals": direct_row.get("signals"),
        "blocked_decisions": direct_row.get("blocked_decisions"),
        "intents": direct_row.get("intents"),
        "fills": direct_row.get("fills"),
        "baseline_comparison": _build_baseline_comparison(metrics, baseline_metrics, baseline_case, current_horizon=horizon),
        "artifact_paths": direct_row["artifact_paths"],
        "computation_notes": metrics["computation_notes"],
    }


def _compute_trade_metrics(
    *,
    trades: list[dict[str, str]],
    range_start: str | None,
    range_end: str | None,
    dominant_session: str | None,
) -> dict[str, Any]:
    ordered_by_exit = sorted(trades, key=lambda row: _trade_sort_ts(row, prefer="exit"))
    pnls = [_float(row.get("net_pnl")) for row in ordered_by_exit]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    gross_wins = round(sum(wins), 4)
    gross_losses = round(sum(losses), 4)
    trade_count = len(pnls)
    phase_counts = Counter(str(row.get("entry_session_phase") or "UNKNOWN") for row in trades)
    dominant_phase, dominant_share = _dominant_bucket(dict(phase_counts))
    if dominant_session is None:
        dominant_session = dominant_phase
    return {
        "date_range_tested": {"start": range_start, "end": range_end},
        "number_of_trades": trade_count,
        "win_rate_pct": round((len(wins) / trade_count) * 100, 2) if trade_count else None,
        "total_realized_pnl": round(sum(pnls), 4),
        "average_realized_pnl_per_trade": round(sum(pnls) / trade_count, 4) if trade_count else None,
        "median_realized_pnl_per_trade": round(statistics.median(pnls), 4) if pnls else None,
        "gross_wins": gross_wins,
        "gross_losses": gross_losses,
        "profit_factor": round(gross_wins / abs(gross_losses), 4) if gross_losses < 0 else None,
        "max_drawdown": _max_drawdown_window(ordered_by_exit, range_start=range_start)["max_drawdown"],
        "max_drawdown_range": _max_drawdown_window(ordered_by_exit, range_start=range_start)["range"],
        "average_trades_per_active_month": _average_trades_per_active_month(trades),
        "active_months": _active_month_count(trades),
        "longest_no_trade_stretch": _longest_no_trade_stretch(trades, range_start=range_start, range_end=range_end),
        "dominant_session_pocket": dominant_session,
        "dominant_session_trade_share_pct": round((dominant_share or 0.0) * 100, 2) if dominant_share is not None else None,
        "top_5_trade_pnl_share_pct": _top_trade_share_pct(pnls, top_n=5),
        "robustness": {
            "without_top_1_trade": _robustness_without_top_trades(pnls, top_n=1),
            "without_top_3_trades": _robustness_without_top_trades(pnls, top_n=3),
        },
        "per_trade_sharpe_proxy": _per_trade_sharpe_proxy(pnls),
        "computation_notes": _metric_notes_for_trade_set(trades),
    }


def _trade_sort_ts(row: dict[str, str], *, prefer: str) -> datetime:
    preferred = row.get("exit_ts") if prefer == "exit" else row.get("entry_ts")
    fallback = row.get("entry_ts") if prefer == "exit" else row.get("exit_ts")
    value = preferred or fallback
    if not value:
        return datetime.min.replace(tzinfo=UTC)
    return datetime.fromisoformat(str(value))


def _max_drawdown_window(trades: list[dict[str, str]], *, range_start: str | None) -> dict[str, Any]:
    peak = 0.0
    cumulative = 0.0
    peak_ts = range_start
    max_drawdown = 0.0
    dd_start = None
    dd_end = None
    for row in trades:
        cumulative += _float(row.get("net_pnl"))
        ts = row.get("exit_ts") or row.get("entry_ts")
        if cumulative > peak:
            peak = cumulative
            peak_ts = ts
        drawdown = peak - cumulative
        if drawdown > max_drawdown:
            max_drawdown = drawdown
            dd_start = peak_ts
            dd_end = ts
    return {
        "max_drawdown": round(max_drawdown, 4),
        "range": {
            "peak_ts": dd_start,
            "trough_ts": dd_end,
        }
        if dd_start and dd_end
        else None,
    }


def _average_trades_per_active_month(trades: list[dict[str, str]]) -> float | None:
    month_count = _active_month_count(trades)
    return round(len(trades) / month_count, 4) if month_count else None


def _active_month_count(trades: list[dict[str, str]]) -> int:
    return len({str(row.get("entry_ts", ""))[:7] for row in trades if row.get("entry_ts")})


def _longest_no_trade_stretch(trades: list[dict[str, str]], *, range_start: str | None, range_end: str | None) -> dict[str, Any]:
    if not range_start or not range_end:
        return {"calendar_days": None, "start_ts": None, "end_ts": None}
    start_dt = datetime.fromisoformat(range_start)
    end_dt = datetime.fromisoformat(range_end)
    entry_times = sorted(datetime.fromisoformat(str(row["entry_ts"])) for row in trades if row.get("entry_ts"))
    if not entry_times:
        return {
            "calendar_days": round((end_dt - start_dt).total_seconds() / 86400, 4),
            "start_ts": range_start,
            "end_ts": range_end,
        }
    best_gap = -1.0
    best_start = start_dt
    best_end = entry_times[0]
    prev = start_dt
    for current in entry_times:
        gap_days = (current - prev).total_seconds() / 86400
        if gap_days > best_gap:
            best_gap = gap_days
            best_start = prev
            best_end = current
        prev = current
    tail_gap = (end_dt - prev).total_seconds() / 86400
    if tail_gap > best_gap:
        best_gap = tail_gap
        best_start = prev
        best_end = end_dt
    return {
        "calendar_days": round(best_gap, 4),
        "start_ts": best_start.isoformat(),
        "end_ts": best_end.isoformat(),
    }


def _top_trade_share_pct(pnls: list[float], *, top_n: int) -> float | None:
    total = sum(pnls)
    if not pnls or total == 0:
        return None
    top_sum = sum(sorted(pnls, reverse=True)[:top_n])
    return round((top_sum / total) * 100, 2)


def _robustness_without_top_trades(pnls: list[float], *, top_n: int) -> dict[str, Any]:
    if not pnls:
        return {"survives": None, "revised_pnl": None}
    revised_pnl = round(sum(pnls) - sum(sorted(pnls, reverse=True)[:top_n]), 4)
    return {"survives": revised_pnl > 0, "revised_pnl": revised_pnl}


def _per_trade_sharpe_proxy(pnls: list[float]) -> float | None:
    if len(pnls) < 2:
        return None
    stdev = statistics.stdev(pnls)
    if stdev == 0:
        return None
    mean = statistics.fmean(pnls)
    return round((mean / stdev) * (len(pnls) ** 0.5), 4)


def _metric_notes_for_trade_set(trades: list[dict[str, str]]) -> list[str]:
    if not trades:
        return ["No closed trades were present for this pair in the replay ledger."]
    return []


def _build_baseline_comparison(
    current_metrics: dict[str, Any],
    baseline_metrics: dict[str, Any],
    baseline_case: dict[str, Any] | None,
    *,
    current_horizon: dict[str, Any],
) -> dict[str, Any]:
    return {
        "baseline_symbol": (baseline_case or {}).get("symbol", "MGC"),
        "baseline_branch": (baseline_case or {}).get("branch"),
        "baseline_date_range": baseline_metrics.get("date_range_tested"),
        "baseline_sample_start_date": ((baseline_case or {}).get("horizon") or {}).get("sample_start_date"),
        "baseline_sample_end_date": ((baseline_case or {}).get("horizon") or {}).get("sample_end_date"),
        "baseline_number_of_trading_days_or_sessions": ((baseline_case or {}).get("horizon") or {}).get("number_of_trading_days_or_sessions"),
        "baseline_number_of_bars_used": ((baseline_case or {}).get("horizon") or {}).get("number_of_bars_used"),
        "baseline_history_window_type": ((baseline_case or {}).get("horizon") or {}).get("history_window_type"),
        "baseline_number_of_trades": baseline_metrics.get("number_of_trades"),
        "baseline_average_realized_pnl_per_trade": baseline_metrics.get("average_realized_pnl_per_trade"),
        "baseline_total_realized_pnl": baseline_metrics.get("total_realized_pnl"),
        "baseline_max_drawdown": baseline_metrics.get("max_drawdown"),
        "period_exact_match_vs_mgc_home_case": _baseline_horizon_exact_match(current_horizon, (baseline_case or {}).get("horizon")),
        "relative_trade_count": _ratio_or_none(current_metrics.get("number_of_trades"), baseline_metrics.get("number_of_trades")),
        "relative_avg_trade": _ratio_or_none(
            current_metrics.get("average_realized_pnl_per_trade"), baseline_metrics.get("average_realized_pnl_per_trade")
        ),
        "relative_total_pnl": _ratio_or_none(current_metrics.get("total_realized_pnl"), baseline_metrics.get("total_realized_pnl")),
        "relative_max_drawdown": _ratio_or_none(current_metrics.get("max_drawdown"), baseline_metrics.get("max_drawdown")),
    }


def _ratio_or_none(numerator: float | int | None, denominator: float | int | None) -> float | None:
    num = _float(numerator)
    den = _float(denominator)
    if den == 0:
        return None
    return round(num / den, 4)


def _build_gc_quantitative_comparison(*, gc_us_late: dict[str, Any], gc_asia: dict[str, Any]) -> dict[str, Any]:
    us_row = _build_quantitative_deployment_row(direct_row=gc_us_late, baseline_case=None, recommendation=None)
    asia_row = _build_quantitative_deployment_row(direct_row=gc_asia, baseline_case=None, recommendation=None)
    us_trades = [row for row in _load_trade_rows(Path(gc_us_late["artifact_paths"]["trade_ledger"])) if row.get("setup_family") == gc_us_late["branch"]]
    asia_trades = [row for row in _load_trade_rows(Path(gc_asia["artifact_paths"]["trade_ledger"])) if row.get("setup_family") == gc_asia["branch"]]
    us_dates = {str(row.get("entry_ts"))[:10] for row in us_trades if row.get("entry_ts")}
    asia_dates = {str(row.get("entry_ts"))[:10] for row in asia_trades if row.get("entry_ts")}
    shared_dates = sorted(us_dates & asia_dates)
    shared_us_pnl = round(sum(_float(row.get("net_pnl")) for row in us_trades if str(row.get("entry_ts"))[:10] in shared_dates), 4)
    shared_asia_pnl = round(sum(_float(row.get("net_pnl")) for row in asia_trades if str(row.get("entry_ts"))[:10] in shared_dates), 4)
    overlap_pct = {
        gc_us_late["branch"]: round((len(shared_dates) / len(us_dates)) * 100, 2) if us_dates else None,
        gc_asia["branch"]: round((len(shared_dates) / len(asia_dates)) * 100, 2) if asia_dates else None,
    }
    phases_different = gc_us_late["dominant_entry_phase"] != gc_asia["dominant_entry_phase"]
    overlap_assessment = (
        "DISTINCT_SESSION_POCKETS_WITH_LIMITED_DAY_OVERLAP"
        if phases_different and max(value or 0 for value in overlap_pct.values()) < 35
        else "MATERIALLY_OVERLAPPING_CALENDAR_DAYS"
    )
    return {
        "pairs": [us_row, asia_row],
        "shared_calendar_dates_count": len(shared_dates),
        "shared_calendar_dates_sample": shared_dates[:10],
        "shared_date_overlap_pct": overlap_pct,
        "shared_date_realized_pnl": {
            gc_us_late["branch"]: shared_us_pnl,
            gc_asia["branch"]: shared_asia_pnl,
        },
        "overlap_assessment": overlap_assessment,
    }


def _build_quantitative_recommendation_section(rows: list[dict[str, Any]]) -> dict[str, Any]:
    bucket_order = ("PROMOTE_TO_PAPER_NOW", "PAPER_AFTER_GUARDRAIL", "LATER_REVIEW", "REJECT_FOR_NOW")
    buckets = {bucket: [] for bucket in bucket_order}
    for row in rows:
        buckets.setdefault(row["recommendation_bucket"], []).append(row)
    return {"bucket_order": list(bucket_order), "buckets": buckets}


def _fmt_num(value: Any, digits: int) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.{digits}f}"


def _fmt_survival(payload: dict[str, Any]) -> str:
    survives = payload.get("survives")
    revised = payload.get("revised_pnl")
    if survives is None:
        return "n/a"
    return f"{'Y' if survives else 'N'} ({_fmt_num(revised, 4)})"


def _fmt_dd_range(payload: dict[str, Any] | None) -> str:
    if not payload:
        return "n/a"
    peak = str(payload.get("peak_ts") or "")[:10]
    trough = str(payload.get("trough_ts") or "")[:10]
    if not peak or not trough:
        return "n/a"
    return f"{peak}->{trough}"


def _sample_horizon_from_artifacts(artifact_paths: dict[str, Any]) -> dict[str, Any]:
    summary = json.loads(Path(artifact_paths["replay_summary"]).read_text(encoding="utf-8"))
    replay_db_path = Path(artifact_paths["replay_db"])
    bar_count = int(summary.get("processed_bars") or summary.get("source_bar_count") or 0)
    trading_days = _count_processed_trading_days(replay_db_path)
    processed_bars = int(summary.get("processed_bars") or 0)
    source_bars = int(summary.get("source_bar_count") or 0)
    history_window_type = "FULL_HISTORY_FROM_AVAILABLE_REPLAY_SOURCE" if source_bars and processed_bars == source_bars else "FIXED_TRAILING_OR_PARTIAL_WINDOW"
    return {
        "sample_start_date": str(summary.get("source_first_bar_ts"))[:10] if summary.get("source_first_bar_ts") else None,
        "sample_end_date": str(summary.get("source_last_bar_ts"))[:10] if summary.get("source_last_bar_ts") else None,
        "number_of_trading_days_or_sessions": trading_days,
        "number_of_bars_used": bar_count,
        "history_window_type": history_window_type,
    }


def _count_processed_trading_days(replay_db_path: Path) -> int | None:
    connection = sqlite3.connect(replay_db_path)
    try:
        row = connection.execute("select count(distinct substr(end_ts, 1, 10)) from processed_bars").fetchone()
        return int(row[0]) if row and row[0] is not None else None
    finally:
        connection.close()


def _period_exact_match_against_mgc_home_case(horizon: dict[str, Any], branch: str) -> bool | None:
    baseline_rows = _load_mgc_reference_metrics()
    baseline_row = baseline_rows.get(branch)
    if baseline_row is None:
        return None
    portability_payload = json.loads((OUTPUT_DIR / "approved_branch_futures_portability_audit.json").read_text(encoding="utf-8"))
    baseline_case = _load_baseline_home_cases(portability_payload).get(branch)
    if baseline_case is None:
        return None
    return _horizons_exact_match(horizon, baseline_case["horizon"])


def _baseline_horizon_exact_match(current_horizon: dict[str, Any], baseline_horizon: dict[str, Any] | None) -> bool | None:
    if baseline_horizon is None:
        return None
    return _horizons_exact_match(current_horizon, baseline_horizon)


def _horizons_exact_match(left: dict[str, Any], right: dict[str, Any]) -> bool:
    keys = (
        "sample_start_date",
        "sample_end_date",
        "number_of_trading_days_or_sessions",
        "number_of_bars_used",
        "history_window_type",
    )
    return all(left.get(key) == right.get(key) for key in keys)


def _report_horizon_header(rows: list[dict[str, Any]]) -> dict[str, Any]:
    starts = [row.get("sample_start_date") for row in rows if row.get("sample_start_date")]
    ends = [row.get("sample_end_date") for row in rows if row.get("sample_end_date")]
    days = [row.get("number_of_trading_days_or_sessions") for row in rows if row.get("number_of_trading_days_or_sessions") is not None]
    bars = [row.get("number_of_bars_used") for row in rows if row.get("number_of_bars_used") is not None]
    scopes = sorted({row.get("history_window_type") for row in rows if row.get("history_window_type")})
    mgc_matches = []
    for row in rows:
        if "baseline_comparison" in row:
            mgc_matches.append(row.get("baseline_comparison", {}).get("period_exact_match_vs_mgc_home_case"))
        elif "period_exact_match_vs_mgc_home_case" in row:
            mgc_matches.append(row.get("period_exact_match_vs_mgc_home_case"))
    return {
        "sample_start_date": min(starts) if starts else None,
        "sample_end_date": max(ends) if ends else None,
        "number_of_trading_days_or_sessions": {"min": min(days), "max": max(days)} if days else None,
        "number_of_bars_used": {"min": min(bars), "max": max(bars)} if bars else None,
        "history_window_type": scopes[0] if len(scopes) == 1 else scopes,
        "mgc_comparison_periods_exactly_identical": all(match is True for match in mgc_matches) if mgc_matches else None,
    }


def _build_futures_discovery_rows(portability_payload: dict[str, Any]) -> list[dict[str, Any]]:
    baselines = _load_baseline_home_cases(portability_payload)
    rows: list[dict[str, Any]] = []
    for result in portability_payload.get("results", []):
        summary = json.loads(Path(result["artifact_paths"]["replay_summary"]).read_text(encoding="utf-8"))
        event_counts = _load_replay_event_counts(
            Path(result["artifact_paths"]["replay_db"]),
            signal_bars_by_branch=summary.get("approved_branch_signal_bars") or {},
        )
        trade_rows = _load_trade_rows(Path(result["artifact_paths"]["trade_ledger"]))
        for branch_row in result.get("branch_rows", []):
            rows.append(
                _build_discovery_diagnostic_row(
                    symbol=str(result["symbol"]),
                    branch_row=branch_row,
                    artifact_paths=result["artifact_paths"],
                    summary=summary,
                    event_counts=(event_counts.get(str(branch_row["branch"])) or {}),
                    trade_rows=trade_rows,
                    baseline_case=baselines.get(str(branch_row["branch"])),
                )
            )
    return rows


def _build_discovery_diagnostic_row(
    *,
    symbol: str,
    branch_row: dict[str, Any],
    artifact_paths: dict[str, Any],
    summary: dict[str, Any],
    event_counts: dict[str, int],
    trade_rows: list[dict[str, str]],
    baseline_case: dict[str, Any] | None,
) -> dict[str, Any]:
    branch = str(branch_row["branch"])
    branch_trades = [row for row in trade_rows if row.get("setup_family") == branch]
    signal_bars = list((summary.get("approved_branch_signal_bars") or {}).get(branch, []))
    signal_timestamps = _signal_bar_timestamps(signal_bars)
    session_distribution = _session_pocket_distribution(signal_timestamps)
    _, pocket_concentration = _dominant_bucket(session_distribution)
    metrics = _compute_trade_metrics(
        trades=branch_trades,
        range_start=summary.get("source_first_bar_ts"),
        range_end=summary.get("source_last_bar_ts"),
        dominant_session=None,
    )
    gross_metrics = _compute_trade_metric_snapshot([_float(row.get("gross_pnl")) for row in branch_trades])
    net_metrics = _compute_trade_metric_snapshot([_float(row.get("net_pnl")) for row in branch_trades])
    top_1_contribution = _top_trade_share_pct([_float(row.get("net_pnl")) for row in branch_trades], top_n=1)
    top_3_contribution = _top_trade_share_pct([_float(row.get("net_pnl")) for row in branch_trades], top_n=3)
    raw_setup_count = len(signal_bars)
    blocked_count = int(event_counts.get("blocked_count") or 0)
    intent_count = int(event_counts.get("entry_intents") or 0)
    fill_count = int(event_counts.get("entry_fills") or 0)
    post_filter_setup_count = intent_count
    decision_count = raw_setup_count
    row: dict[str, Any] = {
        "symbol": symbol,
        "branch": branch,
        "side": branch_row.get("side"),
        "is_reference_lane": symbol == "MGC",
        **_sample_horizon_from_artifacts(artifact_paths),
        "bars_scanned": int(summary.get("processed_bars") or summary.get("source_bar_count") or 0),
        "raw_setup_count": raw_setup_count,
        "post_filter_setup_count": post_filter_setup_count,
        "blocked_count": blocked_count,
        "decision_count": decision_count,
        "intent_count": intent_count,
        "fill_count": fill_count,
        "fill_rate_from_raw": _ratio_or_none(fill_count, raw_setup_count),
        "fill_rate_from_post_filter": _ratio_or_none(fill_count, post_filter_setup_count),
        "average_days_between_raw_setups": _average_days_between_timestamps(signal_timestamps),
        "average_days_between_fills": _average_days_between_trade_entries(branch_trades),
        "session_pocket_distribution": session_distribution,
        "pocket_concentration_ratio": round(pocket_concentration, 4) if pocket_concentration is not None else None,
        "realized_pnl": metrics["total_realized_pnl"],
        "avg_trade": metrics["average_realized_pnl_per_trade"],
        "median_trade": metrics["median_realized_pnl_per_trade"],
        "profit_factor": metrics["profit_factor"],
        "max_drawdown": metrics["max_drawdown"],
        "trade_sharpe_proxy": metrics["per_trade_sharpe_proxy"],
        "top_1_trade_contribution": top_1_contribution,
        "top_3_trade_contribution": top_3_contribution,
        "survives_without_top_1": metrics["robustness"]["without_top_1_trade"]["survives"],
        "survives_without_top_3": metrics["robustness"]["without_top_3_trades"]["survives"],
        "pre_cost_metrics": gross_metrics,
        "post_cost_metrics": net_metrics,
        "portability_assessment": branch_row.get("portability_assessment"),
        "sample_warning": branch_row.get("sample_warning"),
        "coverage_warning": branch_row.get("coverage_warning"),
        "artifact_paths": artifact_paths,
        "baseline_comparison": _build_discovery_baseline_comparison(
            current_row={
                "raw_setup_count": raw_setup_count,
                "avg_trade": metrics["average_realized_pnl_per_trade"],
                "realized_pnl": metrics["total_realized_pnl"],
                "max_drawdown": metrics["max_drawdown"],
            },
            baseline_case=baseline_case,
        ),
        "diagnostic_notes": [
            "post_filter_setup_count is intent-backed because the stored research replay artifacts do not persist a richer intermediate post-filter stage.",
        ],
    }
    failure_primary, failure_secondary = _classify_discovery_failure(row)
    row["failure_primary_cause"] = failure_primary
    row["failure_secondary_cause"] = failure_secondary
    transfer_verdict, transfer_reason = _classify_transfer_verdict(row)
    row["transfer_verdict"] = transfer_verdict
    row["transfer_reason"] = transfer_reason
    return row


def _signal_bar_timestamps(signal_bars: list[str]) -> list[str]:
    timestamps: list[str] = []
    for value in signal_bars:
        parts = str(value).split("|")
        if len(parts) >= 3:
            timestamps.append(parts[-1])
    return timestamps


def _session_pocket_distribution(signal_timestamps: list[str]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for value in signal_timestamps:
        try:
            current_dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            continue
        counts[label_session_phase(current_dt)] += 1
    return dict(sorted(counts.items()))


def _average_days_between_timestamps(timestamps: list[str]) -> float | None:
    ordered = []
    for value in timestamps:
        try:
            ordered.append(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
        except ValueError:
            continue
    ordered.sort()
    if len(ordered) < 2:
        return None
    gaps = [
        (current - previous).total_seconds() / 86400
        for previous, current in zip(ordered, ordered[1:], strict=False)
    ]
    return round(statistics.fmean(gaps), 4) if gaps else None


def _average_days_between_trade_entries(trades: list[dict[str, str]]) -> float | None:
    return _average_days_between_timestamps([str(row.get("entry_ts")) for row in trades if row.get("entry_ts")])


def _compute_trade_metric_snapshot(pnls: list[float]) -> dict[str, Any] | None:
    if not pnls:
        return None
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    gross_wins = sum(wins)
    gross_losses = sum(losses)
    return {
        "trade_count": len(pnls),
        "total_pnl": round(sum(pnls), 4),
        "average_trade": round(statistics.fmean(pnls), 4),
        "median_trade": round(statistics.median(pnls), 4),
        "profit_factor": round(gross_wins / abs(gross_losses), 4) if gross_losses < 0 else None,
    }


def _build_discovery_baseline_comparison(*, current_row: dict[str, Any], baseline_case: dict[str, Any] | None) -> dict[str, Any] | None:
    if baseline_case is None:
        return None
    baseline_branch_row = baseline_case.get("branch_row") or {}
    baseline_trade_rows = baseline_case.get("trade_rows") or []
    baseline_metrics = _compute_trade_metrics(
        trades=list(baseline_trade_rows),
        range_start=(baseline_case.get("summary") or {}).get("source_first_bar_ts"),
        range_end=(baseline_case.get("summary") or {}).get("source_last_bar_ts"),
        dominant_session=None,
    )
    return {
        "baseline_symbol": baseline_case.get("symbol", "MGC"),
        "relative_trade_count": _ratio_or_none(current_row.get("raw_setup_count"), baseline_branch_row.get("signals")),
        "relative_avg_trade": _ratio_or_none(current_row.get("avg_trade"), baseline_metrics.get("average_realized_pnl_per_trade")),
        "relative_total_pnl": _ratio_or_none(current_row.get("realized_pnl"), baseline_metrics.get("total_realized_pnl")),
        "relative_max_drawdown": _ratio_or_none(current_row.get("max_drawdown"), baseline_metrics.get("max_drawdown")),
    }


def _classify_discovery_failure(row: dict[str, Any]) -> tuple[str | None, str | None]:
    raw = int(row.get("raw_setup_count") or 0)
    blocked = int(row.get("blocked_count") or 0)
    intents = int(row.get("intent_count") or 0)
    fills = int(row.get("fill_count") or 0)
    realized = _float(row.get("realized_pnl"))
    avg_trade = _float(row.get("avg_trade"))
    profit_factor = row.get("profit_factor")
    max_drawdown = _float(row.get("max_drawdown"))
    pocket_concentration = row.get("pocket_concentration_ratio") or 0.0
    top_1 = row.get("top_1_trade_contribution")
    top_3 = row.get("top_3_trade_contribution")
    survives_top_1 = row.get("survives_without_top_1")
    survives_top_3 = row.get("survives_without_top_3")
    pair = (str(row.get("branch")), str(row.get("symbol")))
    adaptation_candidate = pair in SECOND_PASS_ADAPTATION_CANDIDATES

    reasons: list[str] = []
    if raw == 0 and intents == 0 and fills == 0:
        reasons.append("NO_STRUCTURAL_FIT")
    elif raw < 5:
        reasons.append("TOO_FEW_RAW_SETUPS")
    if raw and blocked / raw >= 0.7:
        reasons.append("BLOCKED_MOSTLY")
    if raw >= 8 and intents / raw <= 0.3:
        reasons.append("FILTER_ATTRITION_TOO_HIGH")
    if raw >= 8 and intents >= 3 and adaptation_candidate and (realized <= 0 or avg_trade <= 0):
        reasons.append("STRUCTURALLY_PRESENT_BUT_RIGID_REPLAY_FORM")
    if pocket_concentration >= 0.85 and raw < 12:
        reasons.append("SESSION_TOO_NARROW")
    if realized > 0 and ((top_1 is not None and top_1 >= 85.0) or (top_3 is not None and top_3 >= 100.0) or survives_top_3 is False):
        reasons.append("CONCENTRATION_TOO_HIGH")
    if realized > 0 and max_drawdown > realized:
        reasons.append("DRAWDOWN_TOO_HIGH")
    if raw > 0 and (realized <= 0 or avg_trade <= 0 or (profit_factor is not None and profit_factor < 1.05)):
        reasons.append("ECONOMICS_WEAK")
    deduped: list[str] = []
    for reason in reasons:
        if reason not in deduped:
            deduped.append(reason)
    if not deduped:
        return None, None
    return deduped[0], (deduped[1] if len(deduped) > 1 else None)


def _classify_transfer_verdict(row: dict[str, Any]) -> tuple[str, str]:
    symbol = str(row["symbol"])
    branch = str(row["branch"])
    raw = int(row.get("raw_setup_count") or 0)
    fills = int(row.get("fill_count") or 0)
    realized = _float(row.get("realized_pnl"))
    avg_trade = _float(row.get("avg_trade"))
    failure_primary = row.get("failure_primary_cause")
    portability = row.get("portability_assessment")
    pair = (branch, symbol)

    if symbol == "MGC":
        return "DIRECT_TRANSFER_CREDIBLE", "Home reference lane for this family; used as the structural baseline rather than a transfer test."
    if failure_primary == "NO_STRUCTURAL_FIT":
        return "REJECT_FOR_NOW", "No raw setup chain survived into persisted replay evidence on this instrument."
    if failure_primary == "TOO_FEW_RAW_SETUPS":
        return "REJECT_FOR_NOW", "The family barely triggers on this instrument, so there is not enough structural density to keep spending time."
    if pair in SECOND_PASS_ADAPTATION_CANDIDATES and raw >= 8 and fills >= 3 and failure_primary in {
        "ECONOMICS_WEAK",
        "CONCENTRATION_TOO_HIGH",
        "DRAWDOWN_TOO_HIGH",
        "SESSION_TOO_NARROW",
        "STRUCTURALLY_PRESENT_BUT_RIGID_REPLAY_FORM",
    }:
        return "NARROW_ADAPTATION_WORTH_TESTING", "Structural signal activity is present, but the current replay form looks too rigid or fragile for this instrument."
    if (
        portability == "PORTABLE_CANDIDATE"
        and realized > 0
        and avg_trade > 0
        and raw >= 8
        and fills >= 5
        and failure_primary not in {"SESSION_TOO_NARROW", "TOO_FEW_RAW_SETUPS"}
    ):
        return "DIRECT_TRANSFER_CREDIBLE", "The family shows enough structural activity and positive economics on the unchanged replay form."
    if portability in {"THIN_SAMPLE", "MIXED_NEEDS_MORE_RESEARCH", "DEGRADED_OUTSIDE_ORIGINAL_LANE"} or realized > 0:
        return "LATER_REVIEW_ONLY", "There is some structural presence, but the current evidence is either thin, mixed, or not yet strong enough for the next lane."
    return "REJECT_FOR_NOW", "The family does not show a credible unchanged transfer on this instrument."


def _build_discovery_diagnostics_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    non_reference = [row for row in rows if not row.get("is_reference_lane")]
    by_failure = Counter(row.get("failure_primary_cause") or "NONE" for row in non_reference)
    by_transfer = Counter(row.get("transfer_verdict") for row in non_reference)
    strongest = sorted(
        [
            row
            for row in non_reference
            if row.get("transfer_verdict") == "DIRECT_TRANSFER_CREDIBLE"
        ],
        key=lambda row: (
            _float(row.get("realized_pnl")),
            int(row.get("fill_count") or 0),
            int(row.get("raw_setup_count") or 0),
        ),
        reverse=True,
    )[:5]
    return {
        "tested_pair_count": len(rows),
        "non_reference_pair_count": len(non_reference),
        "failure_primary_cause_counts": dict(sorted(by_failure.items())),
        "transfer_verdict_counts": dict(sorted(by_transfer.items())),
        "strongest_direct_transfer_candidates": [f"{row['symbol']} / {row['branch']}" for row in strongest],
        "diagnostic_takeaway": "Most weak transfers are failing because structural activity exists but economics or concentration remain weak, not because the families never trigger at all."
        if by_failure.get("ECONOMICS_WEAK") or by_failure.get("CONCENTRATION_TOO_HIGH")
        else "Most rejected transfers are failing because the families barely trigger or do not fit structurally on the instrument.",
    }


def _build_narrow_adaptation_candidates_report(
    *,
    diagnostics_rows: list[dict[str, Any]],
    direct_payload: dict[str, Any],
    adaptation_payload: dict[str, Any],
    timeframe: str,
) -> dict[str, Any]:
    direct_by_pair = {(str(row["branch"]), str(row["symbol"])): row for row in direct_payload.get("direct_results", [])}
    adapted_by_pair: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in adaptation_payload.get("adapted_results", []):
        adapted_by_pair.setdefault((str(row["branch"]), str(row["symbol"])), []).append(row)
    rows = []
    for diagnostic in diagnostics_rows:
        if diagnostic.get("transfer_verdict") != "NARROW_ADAPTATION_WORTH_TESTING":
            continue
        pair = (str(diagnostic["branch"]), str(diagnostic["symbol"]))
        direct_row = direct_by_pair.get(pair)
        variants = adapted_by_pair.get(pair, [])
        if direct_row is not None and not variants and pair in SECOND_PASS_ADAPTATION_CANDIDATES:
            variants = _materialize_missing_narrow_adaptation_variants(
                branch=pair[0],
                symbol=pair[1],
                direct_row=direct_row,
                timeframe=timeframe,
            )
        if direct_row is None or not variants:
            continue
        rows.append(_build_narrow_adaptation_row(diagnostic=diagnostic, direct_row=direct_row, variants=variants))
    rows.sort(key=lambda row: (row["branch"], row["symbol"]))
    return {
        "timeframe": timeframe,
        "source_artifacts": {
            "direct_second_pass": str(OUTPUT_DIR / "approved_branch_futures_direct_portability_second_pass.json"),
            "adapted_second_pass": str(OUTPUT_DIR / "approved_branch_futures_narrow_adaptation_second_pass.json"),
        },
        "rows": rows,
        "summary": {
            "candidate_count": len(rows),
            "meaningful_clear_count": sum(1 for row in rows if row["adaptation_outcome"] == "MEANINGFUL_BAR_CLEARED"),
            "slight_noise_only_count": sum(1 for row in rows if row["adaptation_outcome"] == "SLIGHT_NOISE_IMPROVEMENT_ONLY"),
            "no_improvement_count": sum(1 for row in rows if row["adaptation_outcome"] == "NO_MATERIAL_IMPROVEMENT"),
        },
    }


def _materialize_missing_narrow_adaptation_variants(
    *,
    branch: str,
    symbol: str,
    direct_row: dict[str, Any],
    timeframe: str,
) -> list[dict[str, Any]]:
    baseline_metrics = _load_mgc_reference_metrics().get(branch)
    rows: list[dict[str, Any]] = []
    for plan in SECOND_PASS_ADAPTATION_CANDIDATES[(branch, symbol)]:
        symbol_result = _run_symbol_audit_task(
            symbol=symbol,
            timeframe=timeframe,
            lane="futures",
            label=f"discovery_diag_{branch}_{symbol}_{plan['variant']}",
            extra_overrides=plan["overrides"],
        )
        rows.append(
            _build_second_pass_pair_row(
                symbol_result=symbol_result,
                branch=branch,
                baseline_metrics=baseline_metrics,
                variant=plan["variant"],
                overrides=plan["overrides"],
                rationale=plan["rationale"],
                direct_reference=direct_row,
            )
        )
    return rows


def _build_narrow_adaptation_row(
    *,
    diagnostic: dict[str, Any],
    direct_row: dict[str, Any],
    variants: list[dict[str, Any]],
) -> dict[str, Any]:
    direct_quant = _build_quantitative_deployment_row(direct_row=direct_row, baseline_case=None, recommendation=None)
    direct_metrics = {
        "number_of_trades": direct_quant["number_of_trades"],
        "total_realized_pnl": direct_quant["total_realized_pnl"],
        "profit_factor": direct_quant["profit_factor"],
        "max_drawdown": direct_quant["max_drawdown"],
        "top_3_trade_pnl_share_pct": direct_quant["top_5_trade_pnl_share_pct"],
    }
    variant_rows = []
    for variant in variants:
        quant = _build_quantitative_deployment_row(direct_row=variant, baseline_case=None, recommendation=None)
        concentration = _top_trade_share_pct(
            [_float(row.get("net_pnl")) for row in _load_trade_rows(Path(variant["artifact_paths"]["trade_ledger"])) if row.get("setup_family") == variant["branch"]],
            top_n=3,
        )
        variant_rows.append(
            {
                "variant": variant["variant"],
                "variant_rationale": variant["variant_rationale"],
                "variant_overrides": variant.get("variant_overrides") or {},
                "result": {
                    "number_of_trades": quant["number_of_trades"],
                    "total_realized_pnl": quant["total_realized_pnl"],
                    "profit_factor": quant["profit_factor"],
                    "max_drawdown": quant["max_drawdown"],
                    "top_3_trade_pnl_share_pct": concentration,
                },
                "delta_vs_direct": {
                    "trade_count_delta": (quant["number_of_trades"] or 0) - (direct_quant["number_of_trades"] or 0),
                    "pnl_delta": round(_float(quant["total_realized_pnl"]) - _float(direct_quant["total_realized_pnl"]), 4),
                    "profit_factor_delta": _delta_or_none(quant["profit_factor"], direct_quant["profit_factor"]),
                    "drawdown_delta": _delta_or_none(quant["max_drawdown"], direct_quant["max_drawdown"]),
                    "concentration_delta": _delta_or_none(concentration, _top_trade_share_pct(
                        [_float(row.get("net_pnl")) for row in _load_trade_rows(Path(direct_row["artifact_paths"]["trade_ledger"])) if row.get("setup_family") == direct_row["branch"]],
                        top_n=3,
                    )),
                },
            }
        )
    best_variant = max(variant_rows, key=lambda row: (_float(row["result"]["total_realized_pnl"]), _float(row["result"]["profit_factor"])))
    meaningful = (
        _float(best_variant["result"]["total_realized_pnl"]) > _float(direct_quant["total_realized_pnl"])
        and (best_variant["result"]["profit_factor"] or 0) > ((direct_quant["profit_factor"] or 0) + 0.05)
        and (best_variant["result"]["number_of_trades"] or 0) >= max(5, int((direct_quant["number_of_trades"] or 0) * 0.7))
    )
    slight = _float(best_variant["result"]["total_realized_pnl"]) > _float(direct_quant["total_realized_pnl"])
    adaptation_outcome = (
        "MEANINGFUL_BAR_CLEARED"
        if meaningful
        else "SLIGHT_NOISE_IMPROVEMENT_ONLY"
        if slight
        else "NO_MATERIAL_IMPROVEMENT"
    )
    return {
        "symbol": diagnostic["symbol"],
        "branch": diagnostic["branch"],
        "failure_primary_cause": diagnostic["failure_primary_cause"],
        "transfer_verdict": diagnostic["transfer_verdict"],
        "direct_result": direct_metrics,
        "adapted_variants": variant_rows,
        "best_variant": best_variant["variant"],
        "best_variant_result": best_variant["result"],
        "adaptation_outcome": adaptation_outcome,
    }


def _delta_or_none(value: Any, baseline: Any) -> float | None:
    if value is None or baseline is None:
        return None
    return round(_float(value) - _float(baseline), 4)


def _build_robustness_prep_shortlist(
    *,
    diagnostics_rows: list[dict[str, Any]],
    adaptation_report: dict[str, Any],
    timeframe: str,
) -> dict[str, Any]:
    adaptation_pairs = {(str(row["branch"]), str(row["symbol"])) for row in adaptation_report.get("rows", [])}
    ranked_rows: list[dict[str, Any]] = []
    for row in diagnostics_rows:
        if row.get("is_reference_lane"):
            continue
        pair = (str(row["branch"]), str(row["symbol"]))
        bucket = _robustness_funnel_bucket(row=row, adaptation_pairs=adaptation_pairs)
        ranked_rows.append(
            {
                "symbol": row["symbol"],
                "branch": row["branch"],
                "bucket": bucket,
                "failure_primary_cause": row.get("failure_primary_cause"),
                "transfer_verdict": row.get("transfer_verdict"),
                "realized_pnl": row.get("realized_pnl"),
                "fill_count": row.get("fill_count"),
                "avg_trade": row.get("avg_trade"),
                "profit_factor": row.get("profit_factor"),
                "fragility_concern": _fragility_concern(row),
                "later_robustness_focus": _later_robustness_focus(row),
                "readiness_reason": _robustness_readiness_reason(row, bucket=bucket),
            }
        )
    bucket_order = list(DISCOVERY_FUNNEL_BUCKETS)
    ranked_rows.sort(
        key=lambda row: (
            bucket_order.index(row["bucket"]),
            -_float(row.get("realized_pnl")),
            -int(row.get("fill_count") or 0),
        )
    )
    grouped = {bucket: [row for row in ranked_rows if row["bucket"] == bucket] for bucket in bucket_order}
    return {
        "timeframe": timeframe,
        "bucket_order": bucket_order,
        "bucket_counts": {bucket: len(rows) for bucket, rows in grouped.items()},
        "rows": ranked_rows,
        "buckets": grouped,
    }


def _robustness_funnel_bucket(*, row: dict[str, Any], adaptation_pairs: set[tuple[str, str]]) -> str:
    pair = (str(row["branch"]), str(row["symbol"]))
    if row.get("transfer_verdict") == "REJECT_FOR_NOW":
        return "STOP_WORK_FOR_NOW"
    if row.get("transfer_verdict") == "NARROW_ADAPTATION_WORTH_TESTING":
        return "NARROW_ADAPTATION_NEXT"
    if (
        row.get("transfer_verdict") == "DIRECT_TRANSFER_CREDIBLE"
        and int(row.get("fill_count") or 0) >= 20
        and _float(row.get("realized_pnl")) > _float(row.get("max_drawdown"))
    ):
        return "ADVANCE_TO_ROBUSTNESS_TESTING"
    if row.get("transfer_verdict") == "LATER_REVIEW_ONLY":
        return "KEEP_LATER_REVIEW"
    if row.get("transfer_verdict") == "DIRECT_TRANSFER_CREDIBLE":
        return "KEEP_LATER_REVIEW"
    return "STOP_WORK_FOR_NOW"


def _fragility_concern(row: dict[str, Any]) -> str:
    if row.get("failure_primary_cause") == "CONCENTRATION_TOO_HIGH":
        return "Economics depend too heavily on a few outsized winners."
    if row.get("failure_primary_cause") == "DRAWDOWN_TOO_HIGH":
        return "Drawdown shape is too heavy relative to realized gains."
    if row.get("failure_primary_cause") == "ECONOMICS_WEAK":
        return "Structural activity exists, but trade economics remain weak."
    return "No dominant fragility concern exceeded the failure thresholds in this pass."


def _later_robustness_focus(row: dict[str, Any]) -> str:
    if row.get("failure_primary_cause") == "CONCENTRATION_TOO_HIGH":
        return "top-trade removal stress"
    if row.get("failure_primary_cause") == "DRAWDOWN_TOO_HIGH":
        return "drawdown distribution stress"
    if row.get("failure_secondary_cause") == "CONCENTRATION_TOO_HIGH":
        return "bootstrap resampling"
    if row.get("failure_primary_cause") == "BLOCKED_MOSTLY":
        return "clustered loss stress"
    return "trade order reshuffle"


def _robustness_readiness_reason(row: dict[str, Any], *, bucket: str) -> str:
    if bucket == "ADVANCE_TO_ROBUSTNESS_TESTING":
        return "This pair is already structurally credible enough that a later robustness lane would test fragility rather than search for basic fit."
    if bucket == "NARROW_ADAPTATION_NEXT":
        return "Structural presence is real, but the direct form is too rigid or too fragile to justify robustness work yet."
    if bucket == "KEEP_LATER_REVIEW":
        return "Some evidence exists, but it remains too thin or mixed for the next lane."
    return "Current evidence does not justify more research time right now."


def _render_discovery_diagnostics_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Futures Discovery Diagnostics",
        "",
        f"Takeaway: {payload['summary']['diagnostic_takeaway']}",
        "",
        f"- Tested pairs: {payload['summary']['tested_pair_count']}",
        f"- Non-reference pairs: {payload['summary']['non_reference_pair_count']}",
        "",
        "## Failure Cause Counts",
        "",
    ]
    for cause, count in sorted(payload["summary"]["failure_primary_cause_counts"].items()):
        lines.append(f"- {cause}: {count}")
    lines.extend(["", "## Transfer Verdict Counts", ""])
    for verdict, count in sorted(payload["summary"]["transfer_verdict_counts"].items()):
        lines.append(f"- {verdict}: {count}")
    lines.extend(["", "## Pair Diagnostics", ""])
    for row in payload["rows"]:
        lines.append(
            f"- {row['symbol']} / {row['branch']}: raw={row['raw_setup_count']}, blocked={row['blocked_count']}, intents={row['intent_count']}, "
            f"fills={row['fill_count']}, realized={row['realized_pnl']}, pf={row['profit_factor']}, "
            f"failure={row.get('failure_primary_cause') or 'n/a'}, verdict={row['transfer_verdict']}"
        )
    return "\n".join(lines)


def _render_narrow_adaptation_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Futures Narrow Adaptation Candidates",
        "",
        f"Candidates reviewed: {payload['summary']['candidate_count']}",
        "",
    ]
    for row in payload["rows"]:
        lines.append(f"## {row['symbol']} / {row['branch']}")
        lines.append(
            f"- Direct: trades={row['direct_result']['number_of_trades']}, pnl={row['direct_result']['total_realized_pnl']}, "
            f"pf={row['direct_result']['profit_factor']}, dd={row['direct_result']['max_drawdown']}"
        )
        lines.append(f"- Outcome: {row['adaptation_outcome']}")
        for variant in row["adapted_variants"]:
            delta = variant["delta_vs_direct"]
            lines.append(
                f"- {variant['variant']}: pnl={variant['result']['total_realized_pnl']} (delta {delta['pnl_delta']}), "
                f"trades={variant['result']['number_of_trades']} (delta {delta['trade_count_delta']}), "
                f"pf={variant['result']['profit_factor']} (delta {delta['profit_factor_delta']}), "
                f"dd={variant['result']['max_drawdown']} (delta {delta['drawdown_delta']})"
            )
        lines.append("")
    return "\n".join(lines)


def _render_robustness_prep_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Futures Robustness Prep Shortlist",
        "",
    ]
    for bucket in payload["bucket_order"]:
        lines.append(f"## {bucket}")
        for row in payload["buckets"][bucket]:
            lines.append(
                f"- {row['symbol']} / {row['branch']}: realized={row['realized_pnl']}, fills={row['fill_count']}, "
                f"failure={row.get('failure_primary_cause') or 'n/a'}, focus={row['later_robustness_focus']}"
            )
        lines.append("")
    return "\n".join(lines)


def _render_futures_markdown(results: list[dict[str, Any]], summary: dict[str, Any]) -> str:
    lines = [
        "# Approved Branch Futures Portability Audit",
        "",
        f"Conclusion: {summary['research_conclusion']}",
        "",
        "## Branch summary",
    ]
    for branch, branch_summary in summary["branch_summary"].items():
        lines.extend(
            [
                f"### {branch}",
                f"- Portable candidates: {', '.join(branch_summary['portable_candidates']) or 'None'}",
                f"- Degraded outside original lane: {', '.join(branch_summary['degraded_outside_original_lane']) or 'None'}",
                f"- Thin or instrument-specific: {', '.join(branch_summary['thin_or_instrument_specific']) or 'None'}",
                "",
            ]
        )
    lines.append("## Instrument highlights")
    for result in results:
        lines.append(f"### {result['symbol']}")
        for row in result["branch_rows"]:
            lines.append(
                f"- {row['branch']}: signals={row['signals']}, blocked={row['blocked_count']}, trades={row['closed_trades']}, "
                f"realized_pnl={row['realized_pnl']}, portability={row['portability_assessment']}"
            )
        lines.append("")
    return "\n".join(lines)


def _render_etf_validation_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# TQQQ / SQQQ Data Validation",
        "",
        "## Session assumptions",
        f"- {json.dumps(payload['validation']['session_assumptions'], sort_keys=True)}",
        "",
        "## Coverage",
    ]
    for symbol, rows in payload["coverage"].items():
        lines.append(f"### {symbol}")
        for row in rows:
            lines.append(
                f"- {row['timeframe']}: bars={row['bar_count']} range={row['first_bar_ts']} -> {row['last_bar_ts']}"
            )
        for note in payload["readiness"].get(symbol, {}).get("gap_notes", []):
            lines.append(f"- {note}")
        lines.append("")
    return "\n".join(lines)


def _render_etf_audit_markdown(results: list[dict[str, Any]], summary: dict[str, Any]) -> str:
    lines = [
        "# TQQQ / SQQQ Research Audit",
        "",
        f"Verdict: {summary['structural_fit_verdict']}",
        "",
        f"Asymmetry: {summary['long_short_asymmetry_note']}",
        "",
    ]
    for result in results:
        lines.append(f"## {result['symbol']}")
        for row in result["branch_rows"]:
            lines.append(
                f"- {row['branch']}: signals={row['signals']}, blocked={row['blocked_count']}, trades={row['closed_trades']}, "
                f"realized_pnl={row['realized_pnl']}, portability={row['portability_assessment']}"
            )
        lines.append("")
    return "\n".join(lines)


def _load_feature_context(repositories: RepositorySet) -> dict[str, ReplayFeatureContext]:
    def _deserialize_value(value: Any) -> Any:
        if not isinstance(value, dict) or "__type__" not in value:
            return value
        if value["__type__"] == "decimal":
            return Decimal(value["value"])
        if value["__type__"] == "datetime":
            return datetime.fromisoformat(value["value"])
        if value["__type__"] == "enum":
            return value["value"]
        return value

    feature_context_by_bar_id: dict[str, ReplayFeatureContext] = {}
    with repositories.engine.begin() as connection:
        feature_rows = connection.execute(select(features_table)).mappings().all()
    for row in feature_rows:
        payload_raw = json.loads(row["payload_json"])
        payload = {key: _deserialize_value(value) for key, value in payload_raw.items()}
        feature_context_by_bar_id[row["bar_id"]] = ReplayFeatureContext(
            atr=payload["atr"],
            turn_ema_fast=payload["turn_ema_fast"],
            turn_ema_slow=payload["turn_ema_slow"],
            vwap=payload["vwap"],
        )
    return feature_context_by_bar_id


if __name__ == "__main__":
    raise SystemExit(main())
