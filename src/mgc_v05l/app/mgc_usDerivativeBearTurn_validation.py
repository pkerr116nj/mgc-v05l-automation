"""Dedicated MGC-native validation pass for usDerivativeBearTurn."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from .mnq_usDerivativeBearTurn_validation import (
    FEE_PER_FILL,
    REPO_ROOT,
    REPLAY_DB_PATH,
    REPLAY_OUTPUT_DIR,
    REPORT_DIR,
    SLIPPAGE_PER_FILL,
    TARGET_FAMILY,
    TIMEFRAME,
    BASE_CONFIG_PATHS,
    MGC_US_LATE_REFERENCE,
    PairMetrics,
    ReplayArtifacts,
    _compute_metrics,
    _direct_answers as _mnq_direct_answers,
    _load_feature_context,
    _load_impulse_reference,
    _load_reference_metrics,
    _load_rows,
    _render_markdown as _render_mnq_markdown,
    _resolve_entry_phase,
    _session_windows_et,
)
from ..app.replay_reporting import build_session_lookup, build_summary_metrics, build_trade_ledger, write_summary_metrics_json, write_trade_ledger_csv
from ..config_models import load_settings_from_files
from ..domain.enums import OrderIntentType
from ..domain.events import FillReceivedEvent, OrderIntentCreatedEvent
from ..domain.models import Bar
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet
from ..persistence.tables import bars_table
from ..strategy.strategy_engine import StrategyEngine
from sqlalchemy import select


SYMBOL = "MGC"
POINT_VALUE = Decimal("10")

ADDITIVE_REFERENCE = {
    "summary": REPO_ROOT / "outputs/replays/persisted_bar_replay_additive_lane_open_late_only_downside_resumption_break_2_full_20260316.summary.json",
    "ledger": REPO_ROOT / "outputs/replays/persisted_bar_replay_additive_lane_open_late_only_downside_resumption_break_2_full_20260316.trade_ledger.csv",
}
MNQ_VALIDATION_REPORT = REPORT_DIR / "mnq_usDerivativeBearTurn_validation.json"
PRIOR_CACHED_HOME_REFERENCE = {
    "summary": REPO_ROOT / "outputs/replays/persisted_bar_replay_us_derivative_bear_retest_us_derivative_bear_retest_20260316_widen_1_full.summary.json",
    "ledger": REPO_ROOT / "outputs/replays/persisted_bar_replay_us_derivative_bear_retest_us_derivative_bear_retest_20260316_widen_1_full.trade_ledger.csv",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-us-derivative-bear-turn-validation")
    parser.add_argument("--run-stamp", default=None, help="Optional replay run stamp. Defaults to a UTC timestamp.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = build_and_write_mgc_us_derivative_bear_turn_validation(run_stamp=args.run_stamp)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_and_write_mgc_us_derivative_bear_turn_validation(*, run_stamp: str | None = None) -> dict[str, Any]:
    artifacts = _run_native_mgc_replay(run_stamp=run_stamp)
    summary = json.loads(artifacts.summary_path.read_text(encoding="utf-8"))
    ledger_rows = _load_rows(artifacts.trade_ledger_path)
    derivative_rows = [row for row in ledger_rows if row["setup_family"] == TARGET_FAMILY]

    target_metrics = _compute_metrics(
        summary=summary,
        rows=derivative_rows,
        sample_start=summary.get("source_first_bar_ts"),
        sample_end=summary.get("source_last_bar_ts"),
    )
    us_late_metrics = _load_reference_metrics(MGC_US_LATE_REFERENCE, "usLatePauseResumeLongTurn")
    additive_metrics = _load_reference_metrics(ADDITIVE_REFERENCE, "usDerivativeBearAdditiveTurn")
    impulse_reference = _load_impulse_reference()
    mnq_reference = _load_mnq_reference()
    prior_cached_reference = _load_reference_metrics(PRIOR_CACHED_HOME_REFERENCE, TARGET_FAMILY)
    session_counts = Counter(_resolve_entry_phase(row) for row in derivative_rows)

    payload = {
        "thread_scope": "thread_1_only",
        "research_scope": "research_only",
        "instrument": SYMBOL,
        "family": TARGET_FAMILY,
        "replay_analysis_path_used": {
            "source_db_path": str(REPLAY_DB_PATH),
            "symbol": SYMBOL,
            "timeframe": TIMEFRAME,
            "config_paths": [str(path) for path in BASE_CONFIG_PATHS],
            "native_family_form": "replay.retest_us_derivative_bear_widen_1.yaml",
            "point_value_used": float(POINT_VALUE),
            "fee_per_fill": float(FEE_PER_FILL),
            "slippage_per_fill": float(SLIPPAGE_PER_FILL),
            "generated_replay_summary": str(artifacts.summary_path),
            "generated_summary_metrics": str(artifacts.summary_metrics_path),
            "generated_trade_ledger": str(artifacts.trade_ledger_path),
            "generated_replay_db": str(artifacts.replay_db_path),
        },
        "structural_execution_fit": {
            "session_windows_et": _session_windows_et(session_counts),
            "entry_phase_counts": dict(session_counts),
            "directional_bias": "SHORT_BIASED",
            "naturally_causal_executable": True,
            "relies_on_future_confirmation": False,
            "depends_on_special_context": (
                "Requires a narrow US-morning derivative short-turn context with slope and curvature weakness, "
                "close weakness, upside stretch, and VWAP / EMA location discipline."
            ),
            "likely_observable_live_window_note": (
                "Activity is focused in US_PREOPEN_OPENING, US_CASH_OPEN_IMPULSE, US_OPEN_LATE, with occasional later follow-through. "
                "It is observable in live hours, but materially rarer than usLatePauseResumeLongTurn."
            ),
            "materially_cleaner_than_parked_impulse_branch": (
                target_metrics.median_trade is not None
                and target_metrics.median_trade > impulse_reference["median_trade"]
                and target_metrics.max_drawdown < impulse_reference["max_drawdown"]
                and target_metrics.realized_pnl > 0
            ),
            "evidence_thin": target_metrics.trades < 10,
        },
        "economic_replay_quality": asdict(target_metrics),
        "comparisons": {
            "vs_MGC_usLatePauseResumeLongTurn": {
                "reference_metrics": asdict(us_late_metrics),
                "comparison_note": (
                    "usLatePauseResumeLongTurn remains the mature live-hours benchmark. "
                    "DerivativeBearTurn is judged here as the cleaner non-admitted short branch, not as a replacement."
                ),
            },
            "vs_parked_impulse_executable_reference": {
                "reference_metrics": impulse_reference,
                "comparison_note": (
                    "The parked impulse executable reference had more activity but a negative median trade and concentration fragility. "
                    "DerivativeBearTurn is judged on whether it is materially cleaner and more executable."
                ),
            },
            "vs_prior_cached_MGC_reference": {
                "reference_metrics": asdict(prior_cached_reference),
                "comparison_note": (
                    "The refreshed native packet is compared against the older cached MGC derivative-bear artifact because Thread 1 history "
                    "previously relied on that stronger packet. If the fresh replay is materially thinner, the fresh result takes precedence."
                ),
            },
            "vs_MNQ_usDerivativeBearTurn_validation": {
                "reference_metrics": mnq_reference,
                "comparison_note": (
                    "MNQ remains the best later expansion lead, but its dedicated packet was thinner and more concentration-sensitive. "
                    "This MGC pass tests whether the home lane is robust enough to lead active Thread 1 work first."
                ),
            },
            "vs_usDerivativeBearAdditiveTurn": {
                "reference_metrics": asdict(additive_metrics),
                "comparison_note": (
                    "The additive branch is included only as a directly related sub-branch comparison. "
                    "If the parent derivative-bear family is not decisively stronger, the additive path should not lead."
                ),
            },
        },
        "operational_suitability": {
            "likely_observable_live_hours_behavior_soon": target_metrics.trades >= 6,
            "more_or_less_paper_suitable_than_usLatePauseResumeLongTurn": (
                "LESS_PAPER_SUITABLE"
                if target_metrics.trades < us_late_metrics.trades or not target_metrics.survives_without_top_3
                else "COMPARABLE_OR_STRONGER"
            ),
            "strong_enough_for_future_paper_admission_design_pass": (
                target_metrics.trades >= 7
                and (target_metrics.profit_factor or 0.0) >= 3.0
                and (target_metrics.median_trade or -999999.0) > 0
                and target_metrics.survives_without_top_3
            ),
        },
        "verdict_bucket": _verdict_bucket(target_metrics),
        "direct_answers": _direct_answers(
            target_metrics=target_metrics,
            us_late_metrics=us_late_metrics,
            prior_cached_reference=prior_cached_reference,
        ),
    }

    json_path = REPORT_DIR / "mgc_usDerivativeBearTurn_validation.json"
    md_path = REPORT_DIR / "mgc_usDerivativeBearTurn_validation.md"
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mgc_usDerivativeBearTurn_validation_json": str(json_path),
        "mgc_usDerivativeBearTurn_validation_md": str(md_path),
        "verdict_bucket": payload["verdict_bucket"],
        "replay_analysis_path_used": payload["replay_analysis_path_used"],
    }


def _run_native_mgc_replay(*, run_stamp: str | None) -> ReplayArtifacts:
    stamp = run_stamp or f"mgc_us_derivative_bear_validation_{datetime.now().astimezone().strftime('%Y%m%d_%H%M%S')}"
    artifacts = ReplayArtifacts(
        run_stamp=stamp,
        replay_db_path=REPLAY_OUTPUT_DIR / f"persisted_bar_replay_{stamp}.sqlite3",
        summary_path=REPLAY_OUTPUT_DIR / f"persisted_bar_replay_{stamp}.summary.json",
        summary_metrics_path=REPLAY_OUTPUT_DIR / f"persisted_bar_replay_{stamp}.summary_metrics.json",
        trade_ledger_path=REPLAY_OUTPUT_DIR / f"persisted_bar_replay_{stamp}.trade_ledger.csv",
    )

    settings = load_settings_from_files([str(path) for path in BASE_CONFIG_PATHS])
    source_engine = build_engine(f"sqlite:///{REPLAY_DB_PATH}")
    with source_engine.begin() as connection:
        rows = connection.execute(
            select(bars_table)
            .where(
                bars_table.c.ticker == SYMBOL,
                bars_table.c.timeframe == TIMEFRAME,
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
        raise RuntimeError(f"No persisted {SYMBOL} {TIMEFRAME} bars were found in {REPLAY_DB_PATH}.")

    replay_settings = settings.model_copy(
        update={
            "symbol": SYMBOL,
            "timeframe": TIMEFRAME,
            "database_url": f"sqlite:///{artifacts.replay_db_path}",
        }
    )
    repositories = RepositorySet(build_engine(replay_settings.database_url))
    strategy_engine = StrategyEngine(settings=replay_settings, repositories=repositories)

    event_counts: Counter[str] = Counter()
    started = datetime.now().astimezone()
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
    trade_ledger = build_trade_ledger(
        repositories.order_intents.list_all(),
        repositories.fills.list_all(),
        session_by_start_ts,
        point_value=POINT_VALUE,
        fee_per_fill=FEE_PER_FILL,
        slippage_per_fill=SLIPPAGE_PER_FILL,
        bars=bars,
        feature_context_by_bar_id=feature_context_by_bar_id,
    )
    summary_metrics = build_summary_metrics(trade_ledger)
    write_trade_ledger_csv(trade_ledger, artifacts.trade_ledger_path)
    write_summary_metrics_json(
        summary_metrics,
        artifacts.summary_metrics_path,
        point_value=POINT_VALUE,
        fee_per_fill=FEE_PER_FILL,
        slippage_per_fill=SLIPPAGE_PER_FILL,
    )
    artifacts.summary_path.write_text(
        json.dumps(
            {
                "source_db_path": str(REPLAY_DB_PATH),
                "symbol": SYMBOL,
                "timeframe": TIMEFRAME,
                "replay_db_path": str(artifacts.replay_db_path),
                "summary_path": str(artifacts.summary_path),
                "trade_ledger_path": str(artifacts.trade_ledger_path),
                "summary_metrics_path": str(artifacts.summary_metrics_path),
                "processed_bars": repositories.processed_bars.count(),
                "source_bar_count": len(bars),
                "source_first_bar_ts": bars[0].end_ts.isoformat(),
                "source_last_bar_ts": bars[-1].end_ts.isoformat(),
                "runtime_started_at": started.isoformat(),
                "event_counts": dict(event_counts),
                "config_paths": [str(path) for path in BASE_CONFIG_PATHS],
                "assumptions": {
                    "point_value": float(POINT_VALUE),
                    "fee_per_fill": float(FEE_PER_FILL),
                    "slippage_per_fill": float(SLIPPAGE_PER_FILL),
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return artifacts


def _load_mnq_reference() -> dict[str, Any]:
    payload = json.loads(MNQ_VALIDATION_REPORT.read_text(encoding="utf-8"))
    return payload["economic_replay_quality"]


def _verdict_bucket(metrics: PairMetrics) -> str:
    if metrics.trades == 0 or metrics.realized_pnl <= 0:
        return "DEPRIORITIZE"
    if (
        metrics.trades >= 7
        and (metrics.profit_factor or 0.0) >= 3.0
        and (metrics.median_trade or -999999.0) > 0
        and metrics.survives_without_top_3
    ):
        return "PRIORITIZE_NOW"
    if (
        metrics.trades >= 5
        and (metrics.profit_factor or 0.0) >= 1.5
        and metrics.realized_pnl > 0
    ):
        return "SERIOUS_NEXT_CANDIDATE"
    return "LATER_REVIEW"


def _direct_answers(*, target_metrics: PairMetrics, us_late_metrics: PairMetrics, prior_cached_reference: PairMetrics) -> dict[str, str]:
    return {
        "should_become_next_immediate_active_thread_1_focus": (
            "Yes. The home-lane replay is clean enough and causal enough to justify making this the next immediate Thread 1 research focus."
            if _verdict_bucket(target_metrics) == "PRIORITIZE_NOW"
            else "No. The refreshed home-lane packet is still positive, but it is too thin to displace the stronger existing Thread 1 priorities right now."
        ),
        "stronger_or_weaker_than_usLate_as_paper_candidate_path": (
            "Weaker. usLatePauseResumeLongTurn remains the more mature and paper-suitable path because it has broader activity and less paper-readiness uncertainty."
            if target_metrics.trades < us_late_metrics.trades or not target_metrics.survives_without_top_3
            else "Comparable or stronger."
        ),
        "right_bridge_into_later_MNQ_work": (
            "Only provisionally. It is still the logical bridge family because it is causal and live-hours-native, but the refreshed MGC packet needs one reproducibility check first because it came in materially thinner than the older cached reference."
        ),
        "single_biggest_remaining_blocker": (
            "Sample breadth is still limited, and the refreshed native packet is materially thinner than the older cached home-lane reference."
            if target_metrics.trades < 10 or target_metrics.trades < prior_cached_reference.trades
            else "Needs a narrow future paper-design pass after one more confirmation cycle."
        ),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    metrics = payload["economic_replay_quality"]
    lines = [
        "# MGC usDerivativeBearTurn Validation",
        "",
        f"- Verdict: `{payload['verdict_bucket']}`",
        f"- Native replay path: `{payload['replay_analysis_path_used']['generated_replay_summary']}`",
        f"- Sample: `{metrics['sample_start']}` -> `{metrics['sample_end']}`",
        (
            f"- Trades `{metrics['trades']}`, realized P/L `{metrics['realized_pnl']}`, avg trade `{metrics['avg_trade']}`, "
            f"median trade `{metrics['median_trade']}`, PF `{metrics['profit_factor']}`, max DD `{metrics['max_drawdown']}`, "
            f"win rate `{metrics['win_rate']}`"
        ),
        (
            f"- Losses: avg `{metrics['average_loser']}`, median `{metrics['median_loser']}`, "
            f"p95 `{metrics['p95_loser']}`, worst `{metrics['worst_loser']}`"
        ),
        (
            f"- Tail: avg winner `{metrics['average_winner']}`, avg win/loss `{metrics['avg_winner_over_avg_loser']}`, "
            f"top-1 `{metrics['top_1_contribution']}`, top-3 `{metrics['top_3_contribution']}`, "
            f"survive ex top-1 `{metrics['survives_without_top_1']}`, survive ex top-3 `{metrics['survives_without_top_3']}`"
        ),
        "",
        "## Structural Fit",
        f"- Naturally causal/executable: `{payload['structural_execution_fit']['naturally_causal_executable']}`",
        f"- Future confirmation required: `{payload['structural_execution_fit']['relies_on_future_confirmation']}`",
        f"- Sessions: {', '.join(payload['structural_execution_fit']['session_windows_et'])}",
        f"- Cleaner than parked impulse branch: `{payload['structural_execution_fit']['materially_cleaner_than_parked_impulse_branch']}`",
        "",
        "## Direct Answers",
        f"- Next immediate Thread 1 focus: {payload['direct_answers']['should_become_next_immediate_active_thread_1_focus']}",
        f"- Versus usLate as paper path: {payload['direct_answers']['stronger_or_weaker_than_usLate_as_paper_candidate_path']}",
        f"- Bridge into later MNQ work: {payload['direct_answers']['right_bridge_into_later_MNQ_work']}",
        f"- Biggest remaining blocker: {payload['direct_answers']['single_biggest_remaining_blocker']}",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
