"""Dedicated MNQ-native validation pass for usDerivativeBearTurn."""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
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
REPLAY_OUTPUT_DIR = REPO_ROOT / "outputs" / "replays"
REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "approved_branch_research"

SYMBOL = "MNQ"
TIMEFRAME = "5m"
POINT_VALUE = Decimal("2")
FEE_PER_FILL = Decimal("0")
SLIPPAGE_PER_FILL = Decimal("0")
TARGET_FAMILY = "usDerivativeBearTurn"
NATIVE_OVERRIDE_PATH = REPO_ROOT / "config" / "replay.retest_us_derivative_bear_widen_1.yaml"
BASE_CONFIG_PATHS = (
    REPO_ROOT / "config" / "base.yaml",
    REPO_ROOT / "config" / "replay.yaml",
    REPO_ROOT / "config" / "replay.research_control.yaml",
    NATIVE_OVERRIDE_PATH,
)

MGC_DERIVATIVE_REFERENCE = {
    "summary": REPO_ROOT / "outputs/replays/persisted_bar_replay_us_derivative_bear_retest_us_derivative_bear_retest_20260316_widen_1_full.summary.json",
    "ledger": REPO_ROOT / "outputs/replays/persisted_bar_replay_us_derivative_bear_retest_us_derivative_bear_retest_20260316_widen_1_full.trade_ledger.csv",
}
MGC_US_LATE_REFERENCE = {
    "summary": REPO_ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.summary.json",
    "ledger": REPO_ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.trade_ledger.csv",
}
IMPULSE_REFERENCE_PATH = REPO_ROOT / "outputs" / "reports" / "approved_branch_research" / "mgc_impulse_same_bar_causalization.json"


@dataclass(frozen=True)
class ReplayArtifacts:
    run_stamp: str
    replay_db_path: Path
    summary_path: Path
    summary_metrics_path: Path
    trade_ledger_path: Path


@dataclass(frozen=True)
class PairMetrics:
    sample_start: str | None
    sample_end: str | None
    trades: int
    realized_pnl: float
    avg_trade: float | None
    median_trade: float | None
    profit_factor: float | None
    max_drawdown: float
    win_rate: float | None
    average_loser: float | None
    median_loser: float | None
    p95_loser: float | None
    worst_loser: float | None
    average_winner: float | None
    avg_winner_over_avg_loser: float | None
    top_1_contribution: float | None
    top_3_contribution: float | None
    survives_without_top_1: bool
    survives_without_top_3: bool
    large_winner_count: int
    very_large_winner_count: int


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mnq-us-derivative-bear-turn-validation")
    parser.add_argument("--run-stamp", default=None, help="Optional replay run stamp. Defaults to a UTC timestamp.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = build_and_write_mnq_us_derivative_bear_turn_validation(run_stamp=args.run_stamp)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_and_write_mnq_us_derivative_bear_turn_validation(*, run_stamp: str | None = None) -> dict[str, Any]:
    artifacts = _run_native_mnq_replay(run_stamp=run_stamp)
    summary = json.loads(artifacts.summary_path.read_text(encoding="utf-8"))
    ledger_rows = _load_rows(artifacts.trade_ledger_path)
    derivative_rows = [row for row in ledger_rows if row["setup_family"] == TARGET_FAMILY]
    first_bear_rows = [row for row in ledger_rows if row["setup_family"] == "firstBearSnapTurn"]
    first_bull_rows = [row for row in ledger_rows if row["setup_family"] == "firstBullSnapTurn"]

    target_metrics = _compute_metrics(
        summary=summary,
        rows=derivative_rows,
        sample_start=summary.get("source_first_bar_ts"),
        sample_end=summary.get("source_last_bar_ts"),
    )
    first_bear_metrics = _compute_metrics(
        summary=summary,
        rows=first_bear_rows,
        sample_start=summary.get("source_first_bar_ts"),
        sample_end=summary.get("source_last_bar_ts"),
    )
    first_bull_metrics = _compute_metrics(
        summary=summary,
        rows=first_bull_rows,
        sample_start=summary.get("source_first_bar_ts"),
        sample_end=summary.get("source_last_bar_ts"),
    )

    mgc_derivative_metrics = _load_reference_metrics(MGC_DERIVATIVE_REFERENCE, TARGET_FAMILY)
    mgc_us_late_metrics = _load_reference_metrics(MGC_US_LATE_REFERENCE, "usLatePauseResumeLongTurn")
    impulse_reference = _load_impulse_reference()
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
                "Requires a US-morning derivative short-turn context with slope/curvature weakness, "
                "close weakness, upside stretch, and VWAP/EMA location discipline."
            ),
            "likely_observable_live_window_note": (
                "The family is concentrated in US_PREOPEN_OPENING, US_CASH_OPEN_IMPULSE, and US_OPEN_LATE, "
                "so it fits a live US-hours observation model better than the parked impulse branch."
            ),
            "mnq_shape_fit_note": (
                "The family shape remains naturally executable on MNQ because the trigger is defined on the completed 5m bar "
                "without future-bar confirmation or delayed-entry dependence."
            ),
            "evidence_thin": target_metrics.trades < 8,
        },
        "economic_replay_quality": asdict(target_metrics),
        "comparisons": {
            "vs_parked_mgc_impulse_best_executable_reference": {
                "reference_family": "MGC impulse_burst_continuation same-bar raw control",
                "reference_metrics": impulse_reference,
                "comparison_note": (
                    "The parked impulse branch had more activity, but its best executable reference still carried a negative median trade "
                    "and concentration fragility. This MNQ derivative-bear pass is judged against that failed executable baseline."
                ),
            },
            "vs_mnq_conceptual_alternatives_from_same_native_packet": {
                "firstBearSnapTurn": asdict(first_bear_metrics),
                "firstBullSnapTurn": asdict(first_bull_metrics),
                "comparison_note": (
                    "These are conceptual comparison families from the same native replay packet, not separate optimized MNQ packets."
                ),
            },
            "vs_current_live_hour_leaders_where_sensible": {
                "MGC_usDerivativeBearTurn_reference": asdict(mgc_derivative_metrics),
                "MGC_usLatePauseResumeLongTurn_reference": asdict(mgc_us_late_metrics),
                "comparison_note": (
                    "MGC usDerivativeBearTurn is the direct home-lane reference. MGC usLatePauseResumeLongTurn is the mature live-hours leader "
                    "used as a benchmark for paper-candidate cleanliness."
                ),
            },
        },
        "verdict_bucket": _verdict_bucket(target_metrics),
        "direct_answers": _direct_answers(target_metrics),
    }

    json_path = REPORT_DIR / "mnq_usDerivativeBearTurn_validation.json"
    md_path = REPORT_DIR / "mnq_usDerivativeBearTurn_validation.md"
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mnq_usDerivativeBearTurn_validation_json": str(json_path),
        "mnq_usDerivativeBearTurn_validation_md": str(md_path),
        "verdict_bucket": payload["verdict_bucket"],
        "replay_analysis_path_used": payload["replay_analysis_path_used"],
    }


def _run_native_mnq_replay(*, run_stamp: str | None) -> ReplayArtifacts:
    stamp = run_stamp or f"mnq_us_derivative_bear_validation_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
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


def _load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_reference_metrics(paths: dict[str, Path], family: str) -> PairMetrics:
    summary = json.loads(paths["summary"].read_text(encoding="utf-8"))
    rows = [row for row in _load_rows(paths["ledger"]) if row["setup_family"] == family]
    return _compute_metrics(
        summary=summary,
        rows=rows,
        sample_start=summary.get("slice_start_ts") or summary.get("source_first_bar_ts"),
        sample_end=summary.get("slice_end_ts") or summary.get("source_last_bar_ts"),
    )


def _compute_metrics(*, summary: dict[str, Any], rows: list[dict[str, str]], sample_start: str | None, sample_end: str | None) -> PairMetrics:
    pnls = [float(row["net_pnl"]) for row in rows]
    winners = [pnl for pnl in pnls if pnl > 0]
    losers = [-pnl for pnl in pnls if pnl < 0]
    total = sum(pnls)
    ordered_winners = sorted(winners, reverse=True)
    r_proxy = statistics.median(losers) if losers else 0.0
    large_threshold = 3.0 * r_proxy if r_proxy else math.inf
    very_large_threshold = 5.0 * r_proxy if r_proxy else math.inf
    return PairMetrics(
        sample_start=sample_start,
        sample_end=sample_end,
        trades=len(pnls),
        realized_pnl=round(total, 4),
        avg_trade=_mean_or_none(pnls),
        median_trade=_median_or_none(pnls),
        profit_factor=_profit_factor(pnls),
        max_drawdown=round(_max_drawdown(pnls), 4),
        win_rate=round(len(winners) / len(pnls), 4) if pnls else None,
        average_loser=_mean_or_none(losers),
        median_loser=_median_or_none(losers),
        p95_loser=_percentile_or_none(losers, 0.95),
        worst_loser=round(max(losers), 4) if losers else None,
        average_winner=_mean_or_none(winners),
        avg_winner_over_avg_loser=_safe_ratio(_mean_or_none(winners), _mean_or_none(losers)),
        top_1_contribution=_top_trade_share(pnls, top_n=1),
        top_3_contribution=_top_trade_share(pnls, top_n=3),
        survives_without_top_1=_survives_without_top(pnls, top_n=1),
        survives_without_top_3=_survives_without_top(pnls, top_n=3),
        large_winner_count=_count_above_threshold(winners, large_threshold),
        very_large_winner_count=_count_above_threshold(winners, very_large_threshold),
    )


def _load_impulse_reference() -> dict[str, Any]:
    payload = json.loads(IMPULSE_REFERENCE_PATH.read_text(encoding="utf-8"))
    reference = dict(payload["raw_control_metrics"])
    reference["decision_bucket"] = payload["causalization_conclusion"]["best_bucket"]
    reference["family"] = "MGC impulse_burst_continuation"
    return reference


def _direct_answers(metrics: PairMetrics) -> dict[str, Any]:
    next_focus = (
        "Not yet. The dedicated MNQ replay is positive enough to keep the branch active, but it is still too thin and too top-3-sensitive "
        "to become the immediate Thread 1 priority."
    )
    if (
        metrics.trades >= 8
        and (metrics.profit_factor or 0.0) >= 1.5
        and (metrics.median_trade or -999999.0) > 0
        and metrics.survives_without_top_3
    ):
        next_focus = "Yes. The branch is broad enough and clean enough to become the next active Thread 1 research focus."

    return {
        "is_next_active_thread_1_research_focus": next_focus,
        "is_strongest_current_index_futures_candidate_overall": (
            "Probably yes, but only as an evidence-thin lead. The listed MNQ/MES alternatives remain weaker in the current evidence set."
            if metrics.realized_pnl > 0 and (metrics.profit_factor or 0.0) >= 1.0
            else "No. The dedicated replay did not hold up strongly enough to keep the lead."
        ),
        "closer_to_eventual_paper_candidacy_than_parked_impulse_branch": (
            "Yes structurally. It is naturally causal, has a positive dedicated replay result, and looks more executable than the parked impulse branch, "
            "but it is still far from paper-candidate quality."
            if (metrics.profit_factor or 0.0) > 1.1918 and metrics.realized_pnl > 0
            else "No. The executable edge is not materially cleaner than the parked impulse branch yet."
        ),
        "single_biggest_remaining_blocker": (
            "Sample breadth is still thin on dedicated MNQ replay, and the current result still fails without its top 3 trades."
            if metrics.trades < 8 or not metrics.survives_without_top_3
            else "Needs one more disciplined MNQ-native pass before paper-candidate planning."
        ),
    }


def _verdict_bucket(metrics: PairMetrics) -> str:
    if metrics.trades == 0 or metrics.realized_pnl <= 0 or (metrics.profit_factor or 0.0) < 1.0:
        return "DEPRIORITIZE"
    if (
        metrics.trades >= 8
        and (metrics.profit_factor or 0.0) >= 1.5
        and (metrics.median_trade or -999999.0) > 0
        and metrics.survives_without_top_3
        and (metrics.top_3_contribution or 999999.0) <= 75.0
    ):
        return "PRIORITIZE_NOW"
    if (
        metrics.trades >= 5
        and (metrics.profit_factor or 0.0) >= 1.2
        and metrics.realized_pnl > 0
        and metrics.survives_without_top_1
    ):
        return "SERIOUS_NEXT_CANDIDATE"
    return "LATER_REVIEW"


def _resolve_entry_phase(row: dict[str, str]) -> str:
    if row.get("entry_session_phase"):
        return row["entry_session_phase"]
    if row.get("entry_ts"):
        return label_session_phase(datetime.fromisoformat(row["entry_ts"]))
    return "UNCLASSIFIED"


def _session_windows_et(session_counts: Counter[str]) -> list[str]:
    windows = {
        "US_PREOPEN_OPENING": "09:00-09:30 ET",
        "US_CASH_OPEN_IMPULSE": "09:30-10:00 ET",
        "US_OPEN_LATE": "10:00-10:30 ET",
        "US_MIDDAY": "10:30-14:00 ET",
        "US_LATE": "14:00-17:00 ET",
        "ASIA_EARLY": "18:00-20:30 ET",
        "ASIA_LATE": "20:30-23:00 ET",
        "LONDON_OPEN": "03:00-05:30 ET",
        "LONDON_LATE": "05:30-08:30 ET",
        "UNCLASSIFIED": "Outside labeled research pockets",
    }
    return [f"{phase}: {windows.get(phase, phase)}" for phase, _ in session_counts.most_common()]


def _render_markdown(payload: dict[str, Any]) -> str:
    metrics = payload["economic_replay_quality"]
    alt = payload["comparisons"]["vs_mnq_conceptual_alternatives_from_same_native_packet"]
    lines = [
        "# MNQ usDerivativeBearTurn Validation",
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
        f"- Session windows: {', '.join(payload['structural_execution_fit']['session_windows_et'])}",
        f"- Observation note: {payload['structural_execution_fit']['likely_observable_live_window_note']}",
        "",
        "## Conceptual MNQ Alternatives From Same Packet",
        f"- firstBearSnapTurn: `{alt['firstBearSnapTurn']['trades']}` trades, P/L `{alt['firstBearSnapTurn']['realized_pnl']}`, PF `{alt['firstBearSnapTurn']['profit_factor']}`",
        f"- firstBullSnapTurn: `{alt['firstBullSnapTurn']['trades']}` trades, P/L `{alt['firstBullSnapTurn']['realized_pnl']}`, PF `{alt['firstBullSnapTurn']['profit_factor']}`",
        "",
        "## Direct Answers",
        f"- Next active Thread 1 focus: {payload['direct_answers']['is_next_active_thread_1_research_focus']}",
        f"- Strongest current index-futures candidate overall: {payload['direct_answers']['is_strongest_current_index_futures_candidate_overall']}",
        f"- Closer to paper candidacy than parked impulse branch: {payload['direct_answers']['closer_to_eventual_paper_candidacy_than_parked_impulse_branch']}",
        f"- Biggest remaining blocker: {payload['direct_answers']['single_biggest_remaining_blocker']}",
    ]
    return "\n".join(lines)


def _mean_or_none(values: list[float]) -> float | None:
    return round(statistics.fmean(values), 4) if values else None


def _median_or_none(values: list[float]) -> float | None:
    return round(statistics.median(values), 4) if values else None


def _percentile_or_none(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 4)
    raw_index = (len(ordered) - 1) * pct
    lower = int(math.floor(raw_index))
    upper = int(math.ceil(raw_index))
    if lower == upper:
        return round(ordered[lower], 4)
    weight = raw_index - lower
    return round((ordered[lower] * (1.0 - weight)) + (ordered[upper] * weight), 4)


def _profit_factor(pnls: list[float]) -> float | None:
    gross_profit = sum(value for value in pnls if value > 0)
    gross_loss = -sum(value for value in pnls if value < 0)
    if gross_loss <= 0:
        return None
    return round(gross_profit / gross_loss, 4)


def _max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return round(numerator / denominator, 4)


def _top_trade_share(pnls: list[float], *, top_n: int) -> float | None:
    total = sum(pnls)
    if not pnls or total == 0:
        return None
    winners = sorted((value for value in pnls if value > 0), reverse=True)
    if not winners:
        return None
    return round((sum(winners[:top_n]) / total) * 100.0, 4)


def _survives_without_top(pnls: list[float], *, top_n: int) -> bool:
    winners = sorted((value for value in pnls if value > 0), reverse=True)
    return (sum(pnls) - sum(winners[:top_n])) > 0


def _count_above_threshold(values: list[float], threshold: float) -> int:
    if math.isinf(threshold):
        return 0
    return sum(1 for value in values if value >= threshold)


if __name__ == "__main__":
    raise SystemExit(main())
