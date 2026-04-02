"""ATP promotion/add research on top of the frozen ATP Companion baseline."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

from .broader_sampling import build_cross_run_summary
from .features import build_feature_states
from .models import HigherPrioritySignal, ResearchBar, TradeRecord
from .performance_validation import _trade_metrics
from .phase2_continuation import build_phase2_replay_package
from .phase3_timing import _bar_vwap, _minute_fast_ema_map, build_phase3_replay_package, classify_vwap_price_quality
from .phase4 import build_rolling_windows
from .storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m

BASELINE_BENCHMARK_LABEL = "ATP_COMPANION_V1_ASIA_US"
BASELINE_BENCHMARK_CONFIG = "config/atp_companion_baseline_v1_asia_us.yaml"
DEFAULT_OUTPUT_DIR = Path("outputs/reports/atp_promotion_add_review")
DEFAULT_CANDIDATE_LANE_OUTPUT_DIR = Path("outputs/reports/atp_candidate_lane_promotion_1_075r_favorable_only")
DEFAULT_CANDIDATE_LANE_CONFIG = "config/atp_companion_candidate_promotion_1_075r_favorable_only.yaml"
DEFAULT_CANDIDATE_BRANCH_REGISTRY_CONFIG = "config/atp_promotion_add_candidate_registry.yaml"
DEFAULT_POINT_VALUE = 5.0
DEFAULT_FEE_PER_ADD = 1.50
STRONGEST_PROMOTION_ADD_CANDIDATE_ID = "promotion_1_075r_favorable_only"


@dataclass(frozen=True)
class PromotionAddCandidate:
    candidate_id: str
    label: str
    progress_r_multiple: float
    allowed_price_quality_states: tuple[str, ...]
    require_positive_reacceleration: bool = True
    require_low_above_entry: bool = True
    max_adds_per_trade: int = 1
    hypothesis: str = ""
    allowed_when: tuple[str, ...] = ()
    does_not_change: tuple[str, ...] = (
        "Frozen ATP Companion Baseline v1 setup qualification",
        "Frozen ATP Companion Baseline v1 current-candle VWAP entry timing",
        "Frozen ATP Companion Baseline v1 exit behavior",
        "Asia + US executable session scope",
        "London diagnostic-only posture",
    )


def default_atp_promotion_add_candidates() -> tuple[PromotionAddCandidate, ...]:
    return (
        PromotionAddCandidate(
            candidate_id="promotion_1_050r_neutral_plus",
            label="Promotion 1 at +0.50R, VWAP favorable or neutral",
            progress_r_multiple=0.50,
            allowed_price_quality_states=("VWAP_FAVORABLE", "VWAP_NEUTRAL"),
            hypothesis=(
                "A faster first earned add may lift total participation if ATP can prove trend follow-through"
                " without chasing and without changing the frozen baseline exit stack."
            ),
            allowed_when=(
                "Trade has progressed at least +0.50R beyond the baseline entry",
                "Current minute price quality is VWAP_FAVORABLE or VWAP_NEUTRAL",
                "Minute close remains above the minute fast EMA",
                "Minute shows positive reacceleration versus the prior minute",
                "Minute low remains above the original entry as a stop-quality proxy",
            ),
        ),
        PromotionAddCandidate(
            candidate_id="promotion_1_075r_neutral_plus",
            label="Promotion 1 at +0.75R, VWAP favorable or neutral",
            progress_r_multiple=0.75,
            allowed_price_quality_states=("VWAP_FAVORABLE", "VWAP_NEUTRAL"),
            hypothesis=(
                "A later earned add may preserve more of ATP's existing trade quality while still increasing"
                " participation on trend days."
            ),
            allowed_when=(
                "Trade has progressed at least +0.75R beyond the baseline entry",
                "Current minute price quality is VWAP_FAVORABLE or VWAP_NEUTRAL",
                "Minute close remains above the minute fast EMA",
                "Minute shows positive reacceleration versus the prior minute",
                "Minute low remains above the original entry as a stop-quality proxy",
            ),
        ),
        PromotionAddCandidate(
            candidate_id="promotion_1_075r_favorable_only",
            label="Promotion 1 at +0.75R, VWAP favorable only",
            progress_r_multiple=0.75,
            allowed_price_quality_states=("VWAP_FAVORABLE",),
            hypothesis=(
                "A stricter earned add may produce smaller participation gains but higher-quality adds by only"
                " permitting clearly favorable VWAP locations."
            ),
            allowed_when=(
                "Trade has progressed at least +0.75R beyond the baseline entry",
                "Current minute price quality is VWAP_FAVORABLE",
                "Minute close remains above the minute fast EMA",
                "Minute shows positive reacceleration versus the prior minute",
                "Minute low remains above the original entry as a stop-quality proxy",
            ),
        ),
    )


def run_atp_promotion_add_review(
    *,
    source_sqlite_path: Path,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    instruments: tuple[str, ...] = ("MGC",),
    higher_priority_signals: Iterable[HigherPrioritySignal] = (),
    window_days: int = 1,
    max_windows: int | None = None,
    point_value: float = DEFAULT_POINT_VALUE,
    candidates: Sequence[PromotionAddCandidate] | None = None,
) -> dict[str, Path]:
    review_candidates = tuple(candidates or default_atp_promotion_add_candidates())
    output_root = output_dir.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    shared_start, shared_end = _shared_1m_coverage(sqlite_path=source_sqlite_path, instruments=instruments)
    windows = build_rolling_windows(shared_start=shared_start, shared_end=shared_end, window_days=window_days)
    if max_windows is not None:
        windows = windows[-max_windows:]

    sampled_runs: list[dict[str, Any]] = []
    baseline_rows: list[dict[str, Any]] = []
    candidate_rows: dict[str, list[dict[str, Any]]] = {candidate.candidate_id: [] for candidate in review_candidates}
    total_bars = 0

    for window in windows:
        run_payload = _run_window_candidate_sample(
            source_sqlite_path=source_sqlite_path,
            instruments=instruments,
            start_ts=window.start_ts,
            end_ts=window.end_ts,
            higher_priority_signals=higher_priority_signals,
            point_value=point_value,
            candidates=review_candidates,
        )
        if run_payload["bar_count"] <= 0:
            continue
        sampled_runs.append(run_payload["summary_row"])
        total_bars += run_payload["bar_count"]
        baseline_rows.extend(run_payload["baseline_rows"])
        for candidate in review_candidates:
            candidate_rows[candidate.candidate_id].extend(run_payload["candidate_rows"][candidate.candidate_id])

    baseline_summary = _trade_metrics(baseline_rows, bar_count=total_bars)
    candidate_payloads = [
        _candidate_summary(
            candidate=candidate,
            baseline_summary=baseline_summary,
            position_rows=candidate_rows[candidate.candidate_id],
            bar_count=total_bars,
        )
        for candidate in review_candidates
    ]

    payload = {
        "module": "ATP promotion/add research",
        "study": "atp_promotion_add_review",
        "baseline_reference": {
            "benchmark_label": BASELINE_BENCHMARK_LABEL,
            "benchmark_config_path": BASELINE_BENCHMARK_CONFIG,
            "semantics_changed": False,
            "promotion_add_logic_in_baseline": False,
        },
        "methodology": {
            "source_sqlite_path": str(source_sqlite_path.resolve()),
            "selection_policy": "systematic_full_coverage_rolling_windows",
            "window_days": window_days,
            "run_count": len(sampled_runs),
            "instruments": list(instruments),
            "window_labels": [row["label"] for row in sampled_runs],
            "point_value": point_value,
            "evidence_truth": "strong_replay_derived_from_phase3_trades_and_1m_bars",
            "mode_timeframe_truth": {
                "study_mode": "research_execution_mode",
                "context_resolution": "5m",
                "execution_resolution": "1m",
            },
            "guardrails": [
                "No change to ATP Companion Baseline v1 setup qualification",
                "No change to ATP Companion Baseline v1 entry timing",
                "No change to ATP Companion Baseline v1 exit behavior",
                "No London execution reopen",
                "No change to locked fill/state invariants",
            ],
        },
        "baseline_metrics": baseline_summary,
        "sampled_runs": sampled_runs,
        "candidate_results": candidate_payloads,
        "candidate_branch_registry": build_candidate_branch_registry(candidate_payloads),
        "recommendation": _recommend_candidate(candidate_payloads, baseline_summary=baseline_summary),
        "cross_run_context": build_cross_run_summary(
            run_rows=sampled_runs,
            combined_validation={
                "atp_phase3_performance": baseline_summary,
                "same_window_comparison": {
                    "atp_phase3": {
                        "trade_count": baseline_summary["total_trades"],
                        "net_pnl_cash": baseline_summary["net_pnl_cash"],
                        "profit_factor": baseline_summary["profit_factor"],
                        "max_drawdown": baseline_summary["max_drawdown"],
                        "average_trade_pnl_cash": baseline_summary["average_trade_pnl_cash"],
                        "win_rate": baseline_summary["win_rate"],
                        "entries_per_100_bars": baseline_summary["entries_per_100_bars"],
                    },
                    "legacy_replay_proxy": {},
                    "delta": {},
                },
                "segment_breakdowns": {
                    "by_bias_state": [],
                    "by_pullback_state": [],
                    "by_timing_state": [],
                    "by_vwap_price_quality_state": [],
                    "by_session_segment": [],
                    "by_entry_family": [],
                },
            },
            combined_enriched_trades=[
                {
                    "entry_ts": row["entry_ts"],
                    "decision_ts": row["decision_ts"],
                    "session_segment": row["session_segment"],
                    "pnl_cash": row["pnl_cash"],
                }
                for row in baseline_rows
            ],
            total_bars=total_bars,
        ),
    }

    json_path = output_root / "atp_promotion_add_review.json"
    markdown_path = output_root / "atp_promotion_add_review.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(render_atp_promotion_add_review_markdown(payload), encoding="utf-8")
    return {"json_path": json_path, "markdown_path": markdown_path}


def run_atp_candidate_lane_review(
    *,
    source_sqlite_path: Path,
    output_dir: Path = DEFAULT_CANDIDATE_LANE_OUTPUT_DIR,
    candidate_id: str = STRONGEST_PROMOTION_ADD_CANDIDATE_ID,
    instruments: tuple[str, ...] = ("MGC",),
    higher_priority_signals: Iterable[HigherPrioritySignal] = (),
    window_days: int = 1,
    max_windows: int | None = None,
    point_value: float = DEFAULT_POINT_VALUE,
) -> dict[str, Path]:
    review_output = run_atp_promotion_add_review(
        source_sqlite_path=source_sqlite_path,
        output_dir=DEFAULT_OUTPUT_DIR,
        instruments=instruments,
        higher_priority_signals=higher_priority_signals,
        window_days=window_days,
        max_windows=max_windows,
        point_value=point_value,
    )
    review_payload = json.loads(Path(review_output["json_path"]).read_text(encoding="utf-8"))
    baseline_metrics = dict(review_payload.get("baseline_metrics") or {})
    candidate_payload = next(
        candidate
        for candidate in review_payload.get("candidate_results") or []
        if candidate.get("candidate_id") == candidate_id
    )
    lane_identity = build_atp_candidate_lane_identity(candidate_payload=candidate_payload)
    session_acceptability = _candidate_session_acceptability(candidate_payload)
    readiness = _candidate_lane_readiness(
        candidate_payload=candidate_payload,
        session_acceptability=session_acceptability,
    )
    payload = {
        "lane_identity": lane_identity,
        "candidate_branch_registry": build_candidate_branch_registry(
            review_payload.get("candidate_results") or []
        ),
        "baseline_reference": review_payload.get("baseline_reference"),
        "methodology": {
            **dict(review_payload.get("methodology") or {}),
            "lane_status": "RESEARCH_CANDIDATE_ONLY",
            "paper_lane_enabled": False,
            "review_source_artifact": str(Path(review_output["json_path"]).resolve()),
        },
        "core_atp_contribution": baseline_metrics,
        "candidate_combined_contribution": candidate_payload["metrics"],
        "add_only_contribution": candidate_payload["add_only_metrics"],
        "add_only_drawdown_impact": {
            "candidate_vs_baseline_max_drawdown_delta": candidate_payload["baseline_delta"]["max_drawdown_delta"],
            "candidate_quality_verdict": candidate_payload["quality_verdict"],
        },
        "session_decomposition": candidate_payload["session_breakdown"],
        "session_acceptability": session_acceptability,
        "candidate_readiness": readiness,
        "baseline_safety": {
            "baseline_semantics_changed": False,
            "baseline_candidate_separation_explicit": True,
        },
    }
    output_root = output_dir.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "atp_candidate_lane_review.json"
    markdown_path = output_root / "atp_candidate_lane_review.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(render_atp_candidate_lane_review_markdown(payload), encoding="utf-8")
    return {"json_path": json_path, "markdown_path": markdown_path}


def render_atp_promotion_add_review_markdown(payload: dict[str, Any]) -> str:
    baseline = dict(payload.get("baseline_metrics") or {})
    recommendation = dict(payload.get("recommendation") or {})
    lines = [
        "# ATP Promotion/Add Review",
        "",
        f"- Baseline benchmark: `{payload['baseline_reference']['benchmark_label']}`",
        f"- Windows evaluated: `{payload['methodology']['run_count']}`",
        f"- Baseline trades: `{baseline.get('total_trades')}`",
        f"- Baseline net P/L: `{baseline.get('net_pnl_cash')}`",
        f"- Baseline avg trade: `{baseline.get('average_trade_pnl_cash')}`",
        f"- Baseline profit factor: `{baseline.get('profit_factor')}`",
        f"- Baseline max drawdown: `{baseline.get('max_drawdown')}`",
        "",
        "## Candidates",
    ]
    for candidate in payload.get("candidate_results") or []:
        metrics = candidate["metrics"]
        delta = candidate["baseline_delta"]
        lines.extend(
            [
                f"### {candidate['label']}",
                f"- Candidate id: `{candidate['candidate_id']}`",
                f"- Adds executed: `{candidate['add_count']}` (`{candidate['add_rate_percent']}%` of baseline trades)",
                f"- Net P/L / delta: `{metrics['net_pnl_cash']}` / `{delta['net_pnl_cash_delta']}`",
                f"- Avg trade / delta: `{metrics['average_trade_pnl_cash']}` / `{delta['average_trade_pnl_cash_delta']}`",
                f"- Profit factor / delta: `{metrics['profit_factor']}` / `{delta['profit_factor_delta']}`",
                f"- Max drawdown / delta: `{metrics['max_drawdown']}` / `{delta['max_drawdown_delta']}`",
                f"- Win rate / delta: `{metrics['win_rate']}` / `{delta['win_rate_delta']}`",
                f"- Add contribution: `{candidate['add_contribution_net_pnl_cash']}`",
                f"- Quality verdict: `{candidate['quality_verdict']}`",
                f"- Evidence note: `{candidate['evidence_note']}`",
            ]
        )
        session_rows = candidate.get("session_breakdown") or {}
        if session_rows:
            lines.append("- Session breakdown:")
            for session, row in session_rows.items():
                lines.append(
                    f"  - `{session}` trades=`{row['trade_count']}` adds=`{row['add_count']}` "
                    f"net=`{row['net_pnl_cash']}` add_contrib=`{row['add_contribution_net_pnl_cash']}`"
                )
    lines.extend(
        [
            "",
            "## Recommendation",
            f"- Recommended candidate: `{recommendation.get('recommended_candidate_id')}`",
            f"- Verdict: `{recommendation.get('verdict')}`",
            f"- Reason: `{recommendation.get('reason')}`",
            "",
            "## Baseline Safety",
            "- ATP Companion Baseline v1 semantics remained unchanged outside the named candidate evaluations.",
        ]
    )
    return "\n".join(lines) + "\n"


def render_atp_candidate_lane_review_markdown(payload: dict[str, Any]) -> str:
    lane = dict(payload.get("lane_identity") or {})
    core = dict(payload.get("core_atp_contribution") or {})
    combined = dict(payload.get("candidate_combined_contribution") or {})
    add_only = dict(payload.get("add_only_contribution") or {})
    readiness = dict(payload.get("candidate_readiness") or {})
    session_acceptability = dict(payload.get("session_acceptability") or {})
    lines = [
        "# ATP Candidate Lane Review",
        "",
        f"- Candidate lane: `{lane.get('candidate_id')}`",
        f"- Replay identity: `{lane.get('replay_strategy_id')}`",
        f"- Study identity: `{lane.get('study_candidate_id')}`",
        f"- Paper identity: `{lane.get('paper_strategy_id')}` (`enabled={lane.get('paper_lane_enabled')}`)",
        "",
        "## Core Vs Add",
        f"- Core ATP net P/L: `{core.get('net_pnl_cash')}`",
        f"- Candidate combined net P/L: `{combined.get('net_pnl_cash')}`",
        f"- Add-only net P/L: `{add_only.get('net_pnl_cash')}`",
        f"- Add-only trades: `{add_only.get('total_trades')}`",
        f"- Add-only avg trade: `{add_only.get('average_trade_pnl_cash')}`",
        f"- Add-only win rate: `{add_only.get('win_rate')}`",
        f"- Add-only profit factor: `{add_only.get('profit_factor')}`",
        "",
        "## Session Acceptability",
        f"- Overall: `{session_acceptability.get('overall_status')}`",
        f"- Reason: `{session_acceptability.get('overall_reason')}`",
    ]
    for session, row in (payload.get("session_decomposition") or {}).items():
        session_status = ((session_acceptability.get("sessions") or {}).get(session) or {}).get("status")
        lines.append(
            f"- `{session}` add_net=`{row['add_contribution_net_pnl_cash']}` adds=`{row['add_count']}` "
            f"combined_net=`{row['net_pnl_cash']}` status=`{session_status}`"
        )
    lines.extend(
        [
            "",
            "## Readiness",
            f"- Verdict: `{readiness.get('verdict')}`",
            f"- Reason: `{readiness.get('reason')}`",
            "",
            "## Baseline Safety",
            "- Frozen ATP Companion Baseline v1 semantics remained unchanged outside this explicit candidate lane.",
        ]
    )
    return "\n".join(lines) + "\n"


def evaluate_promotion_add_candidate(
    *,
    trade: TradeRecord,
    minute_bars: Sequence[ResearchBar],
    candidate: PromotionAddCandidate,
    point_value: float,
    fee_per_add: float = DEFAULT_FEE_PER_ADD,
) -> dict[str, Any]:
    if not minute_bars or candidate.max_adds_per_trade <= 0:
        return _no_add_result(trade=trade, candidate=candidate, reason="NO_TRADE_WINDOW")

    initial_risk = max(float(trade.entry_price) - float(trade.stop_price), 1e-9)
    trigger_price = float(trade.entry_price) + candidate.progress_r_multiple * initial_risk
    minute_ema = _minute_fast_ema_map(minute_bars)
    previous_bar: ResearchBar | None = None

    for bar in minute_bars:
        if bar.end_ts >= trade.exit_ts:
            break
        if float(bar.high) < trigger_price:
            previous_bar = bar
            continue
        add_entry_price = max(float(bar.open), trigger_price)
        price_quality = classify_vwap_price_quality(
            side="LONG",
            entry_price=add_entry_price,
            bar_vwap=_bar_vwap(bar),
            band_reference=max(bar.range_points, initial_risk, 1e-9),
        )
        if price_quality not in candidate.allowed_price_quality_states:
            previous_bar = bar
            continue
        if candidate.require_positive_reacceleration:
            positive_reacceleration = (
                previous_bar is not None
                and float(bar.close) > float(previous_bar.close)
                and float(bar.high) >= float(previous_bar.high)
            )
            if not positive_reacceleration:
                previous_bar = bar
                continue
        if float(bar.close) < minute_ema.get(bar.end_ts, float(bar.close)):
            previous_bar = bar
            continue
        if candidate.require_low_above_entry and not (float(bar.low) > float(trade.entry_price)):
            previous_bar = bar
            continue
        add_pnl_points = float(trade.exit_price) - add_entry_price
        add_pnl_cash = add_pnl_points * point_value - fee_per_add
        return {
            "added": True,
            "candidate_id": candidate.candidate_id,
            "candidate_label": candidate.label,
            "entry_ts": trade.entry_ts,
            "decision_ts": trade.decision_ts,
            "position_entry_price": float(trade.entry_price),
            "position_exit_price": float(trade.exit_price),
            "trade_pnl_cash": float(trade.pnl_cash),
            "pnl_cash": float(trade.pnl_cash) + add_pnl_cash,
            "add_pnl_cash": round(add_pnl_cash, 4),
            "add_pnl_points": round(add_pnl_points, 4),
            "add_entry_ts": bar.end_ts,
            "add_exit_ts": trade.exit_ts,
            "add_entry_price": round(add_entry_price, 4),
            "add_trigger_price": round(trigger_price, 4),
            "add_price_quality_state": price_quality,
            "add_reason": "PROMOTION_1_EARNED",
            "evidence_truth": "strong_replay_derived",
            "modeled_exit_dependency": "inherits_frozen_baseline_exit",
            "depends_on_weak_evidence": False,
            "hold_minutes": float(trade.hold_minutes),
            "bars_held_1m": int(trade.bars_held_1m),
            "add_hold_minutes": round((trade.exit_ts - bar.end_ts).total_seconds() / 60.0, 4),
            "side": trade.side,
            "session_segment": trade.session_segment,
            "mfe_points": float(trade.mfe_points),
            "mae_points": float(trade.mae_points),
            "family": trade.family,
            "exit_reason": trade.exit_reason,
        }
    return _no_add_result(trade=trade, candidate=candidate, reason="PROMOTION_NOT_EARNED")


def _run_window_candidate_sample(
    *,
    source_sqlite_path: Path,
    instruments: tuple[str, ...],
    start_ts: datetime,
    end_ts: datetime,
    higher_priority_signals: Iterable[HigherPrioritySignal],
    point_value: float,
    candidates: Sequence[PromotionAddCandidate],
) -> dict[str, Any]:
    baseline_rows: list[dict[str, Any]] = []
    candidate_rows: dict[str, list[dict[str, Any]]] = {candidate.candidate_id: [] for candidate in candidates}
    total_bars = 0

    for instrument in instruments:
        raw_1m = load_sqlite_bars(
            sqlite_path=source_sqlite_path,
            instrument=instrument,
            timeframe="1m",
            start_ts=start_ts,
            end_ts=end_ts,
        )
        normalized_1m, _ = normalize_and_check_bars(bars=raw_1m, timeframe="1m")
        raw_5m = load_sqlite_bars(
            sqlite_path=source_sqlite_path,
            instrument=instrument,
            timeframe="5m",
            start_ts=start_ts,
            end_ts=end_ts,
        )
        if not raw_5m and normalized_1m:
            raw_5m = resample_bars_from_1m(bars_1m=normalized_1m, target_timeframe="5m")
        normalized_5m, _ = normalize_and_check_bars(bars=raw_5m, timeframe="5m")
        if normalized_1m:
            first_1m_ts = normalized_1m[0].end_ts
            last_1m_ts = normalized_1m[-1].end_ts
            normalized_5m = [bar for bar in normalized_5m if first_1m_ts <= bar.end_ts <= last_1m_ts]
        if not normalized_1m or not normalized_5m:
            continue

        total_bars += len(normalized_5m)
        feature_rows = build_feature_states(bars_5m=normalized_5m, bars_1m=normalized_1m)
        phase2 = build_phase2_replay_package(
            feature_rows=feature_rows,
            bars_1m=normalized_1m,
            higher_priority_signals=higher_priority_signals,
            point_value=point_value,
        )
        phase3 = build_phase3_replay_package(
            entry_states=phase2["entry_states"],
            bars_1m=normalized_1m,
            point_value=point_value,
            old_proxy_trade_count=len(phase2["shadow_trades"]),
        )

        bars_by_end_ts = {bar.end_ts: index for index, bar in enumerate(normalized_1m)}
        for trade in phase3["shadow_trades"]:
            baseline_rows.append(_baseline_trade_row(trade))
            trade_bars = _trade_window_bars(trade=trade, bars_1m=normalized_1m, bars_by_end_ts=bars_by_end_ts)
            for candidate in candidates:
                candidate_rows[candidate.candidate_id].append(
                    evaluate_promotion_add_candidate(
                        trade=trade,
                        minute_bars=trade_bars,
                        candidate=candidate,
                        point_value=point_value,
                    )
                )

    summary_row = {
        "label": f"{start_ts.date().isoformat()}->{end_ts.date().isoformat()}",
        "bars_processed": total_bars,
        "atp_phase3_performance": _trade_metrics(baseline_rows, bar_count=total_bars),
        "tags": {
            "tape_direction_tag": "ATP_BASELINE_REPLAY",
            "regime_tag": "ATP_PROMOTION_RESEARCH",
            "dominant_session_tag": _dominant_session_tag(baseline_rows),
        },
    }
    return {
        "bar_count": total_bars,
        "baseline_rows": baseline_rows,
        "candidate_rows": candidate_rows,
        "summary_row": summary_row,
    }


def _candidate_summary(
    *,
    candidate: PromotionAddCandidate,
    baseline_summary: dict[str, Any],
    position_rows: Sequence[dict[str, Any]],
    bar_count: int,
) -> dict[str, Any]:
    metrics = _trade_metrics(position_rows, bar_count=bar_count)
    add_rows = [row for row in position_rows if row.get("added")]
    add_only_rows = [
        {
            "entry_ts": row.get("add_entry_ts"),
            "decision_ts": row.get("decision_ts"),
            "pnl_cash": row.get("add_pnl_cash"),
            "mfe_points": 0.0,
            "mae_points": 0.0,
            "hold_minutes": row.get("add_hold_minutes"),
            "bars_held_1m": row.get("bars_held_1m"),
            "side": row.get("side"),
            "session_segment": row.get("session_segment"),
        }
        for row in add_rows
    ]
    add_only_metrics = _trade_metrics(add_only_rows, bar_count=bar_count)
    session_breakdown = _candidate_session_breakdown(position_rows)
    quality_verdict = _candidate_quality_verdict(candidate_rows=position_rows, baseline_summary=baseline_summary, metrics=metrics)
    return {
        "candidate_id": candidate.candidate_id,
        "label": candidate.label,
        "hypothesis": candidate.hypothesis,
        "allowed_when": list(candidate.allowed_when),
        "does_not_change": list(candidate.does_not_change),
        "metrics": metrics,
        "trade_count": metrics["total_trades"],
        "add_count": len(add_rows),
        "add_rate_percent": round((len(add_rows) / metrics["total_trades"]) * 100.0, 4) if metrics["total_trades"] else 0.0,
        "add_contribution_net_pnl_cash": round(sum(float(row.get("add_pnl_cash") or 0.0) for row in add_rows), 4),
        "add_only_metrics": add_only_metrics,
        "session_breakdown": session_breakdown,
        "evidence_truth": "strong_replay_derived",
        "depends_on_weak_evidence": False,
        "evidence_note": "Add entries are replay-derived from 1m bars; add exits inherit the frozen baseline exit record.",
        "baseline_delta": {
            "trade_count_delta": metrics["total_trades"] - baseline_summary["total_trades"],
            "net_pnl_cash_delta": round(metrics["net_pnl_cash"] - baseline_summary["net_pnl_cash"], 4),
            "average_trade_pnl_cash_delta": round(
                metrics["average_trade_pnl_cash"] - baseline_summary["average_trade_pnl_cash"],
                4,
            ),
            "profit_factor_delta": round(metrics["profit_factor"] - baseline_summary["profit_factor"], 4),
            "max_drawdown_delta": round(metrics["max_drawdown"] - baseline_summary["max_drawdown"], 4),
            "win_rate_delta": round(metrics["win_rate"] - baseline_summary["win_rate"], 4),
        },
        "quality_verdict": quality_verdict["verdict"],
        "quality_note": quality_verdict["note"],
    }


def _candidate_quality_verdict(
    *,
    candidate_rows: Sequence[dict[str, Any]],
    baseline_summary: dict[str, Any],
    metrics: dict[str, Any],
) -> dict[str, str]:
    add_contrib = sum(float(row.get("add_pnl_cash") or 0.0) for row in candidate_rows)
    if add_contrib <= 0.0:
        return {
            "verdict": "FAILED",
            "note": "The add leg did not improve net P/L after costs.",
        }
    if (
        metrics["net_pnl_cash"] > baseline_summary["net_pnl_cash"]
        and metrics["average_trade_pnl_cash"] > baseline_summary["average_trade_pnl_cash"]
        and metrics["profit_factor"] >= baseline_summary["profit_factor"]
        and metrics["max_drawdown"] <= baseline_summary["max_drawdown"]
    ):
        return {
            "verdict": "QUALITY_IMPROVED",
            "note": "Net P/L, average trade, and profit factor improved without worsening baseline drawdown.",
        }
    return {
        "verdict": "QUANTITY_UP_QUALITY_MIXED",
        "note": "Participation increased and net P/L improved, but quality mixed versus the frozen baseline.",
    }


def _recommend_candidate(candidate_payloads: Sequence[dict[str, Any]], *, baseline_summary: dict[str, Any]) -> dict[str, Any]:
    viable = [candidate for candidate in candidate_payloads if candidate["quality_verdict"] == "QUALITY_IMPROVED"]
    if viable:
        best = max(
            viable,
            key=lambda candidate: (
                candidate["baseline_delta"]["net_pnl_cash_delta"],
                candidate["baseline_delta"]["profit_factor_delta"],
                -candidate["baseline_delta"]["max_drawdown_delta"],
            ),
        )
        return {
            "recommended_candidate_id": best["candidate_id"],
            "verdict": "PROMOTION_RESEARCH_CONTINUE",
            "reason": best["quality_note"],
        }
    best_mixed = max(
        candidate_payloads,
        key=lambda candidate: candidate["baseline_delta"]["net_pnl_cash_delta"],
        default=None,
    )
    if best_mixed and best_mixed["baseline_delta"]["net_pnl_cash_delta"] > 0.0:
        return {
            "recommended_candidate_id": None,
            "verdict": "NO_PROMOTION_YET",
            "reason": (
                "Some candidates increased net P/L, but none cleared a clean quality-improvement threshold"
                " versus the frozen ATP baseline."
            ),
        }
    return {
        "recommended_candidate_id": None,
        "verdict": "NO_PROMOTION_YET",
        "reason": "No tested candidate improved the frozen ATP Companion Baseline v1 after costs.",
    }


def build_atp_candidate_lane_identity(*, candidate_payload: dict[str, Any]) -> dict[str, Any]:
    candidate_id = str(candidate_payload.get("candidate_id") or STRONGEST_PROMOTION_ADD_CANDIDATE_ID)
    strategy_suffix = candidate_id.replace(".", "_")
    return {
        "candidate_id": candidate_id,
        "candidate_label": candidate_payload.get("label"),
        "candidate_status": "RESEARCH_CANDIDATE_ONLY",
        "candidate_config_path": DEFAULT_CANDIDATE_LANE_CONFIG,
        "benchmark_reference": BASELINE_BENCHMARK_LABEL,
        "replay_strategy_id": f"atp_companion_v1_asia_us__{strategy_suffix}",
        "study_candidate_id": f"ATP_COMPANION_V1_ASIA_US__{candidate_id}",
        "paper_strategy_id": f"atp_companion_v1_asia_us__paper__{strategy_suffix}",
        "paper_lane_enabled": False,
        "paper_lane_status": "IDENTITY_ONLY_NOT_ENABLED",
        "strategy_family": "active_trend_participation_engine",
        "execution_scope": "replay_study_identity_only_in_this_pass",
    }


def build_candidate_branch_registry(candidate_payloads: Sequence[dict[str, Any]]) -> dict[str, Any]:
    items = []
    for candidate in candidate_payloads:
        candidate_id = str(candidate.get("candidate_id") or "")
        advanced = candidate_id == STRONGEST_PROMOTION_ADD_CANDIDATE_ID
        items.append(
            {
                "candidate_id": candidate_id,
                "label": candidate.get("label"),
                "status": "ACTIVE_RESEARCH_CANDIDATE" if advanced else "RETAINED_RESEARCH_CANDIDATE",
                "advanced_now": advanced,
                "config_path": (
                    DEFAULT_CANDIDATE_LANE_CONFIG if advanced else DEFAULT_CANDIDATE_BRANCH_REGISTRY_CONFIG
                ),
                "quality_verdict": candidate.get("quality_verdict"),
                "net_pnl_cash_delta": ((candidate.get("baseline_delta") or {}).get("net_pnl_cash_delta")),
                "profit_factor_delta": ((candidate.get("baseline_delta") or {}).get("profit_factor_delta")),
                "session_limit_note": (
                    "US add contribution is negative"
                    if candidate_id == STRONGEST_PROMOTION_ADD_CANDIDATE_ID
                    else "Retained for later optimization; not advanced in this pass."
                ),
            }
        )
    return {
        "registry_config_path": DEFAULT_CANDIDATE_BRANCH_REGISTRY_CONFIG,
        "advanced_candidate_id": STRONGEST_PROMOTION_ADD_CANDIDATE_ID,
        "items": items,
    }


def _candidate_session_acceptability(candidate_payload: dict[str, Any]) -> dict[str, Any]:
    sessions: dict[str, Any] = {}
    session_rows = dict(candidate_payload.get("session_breakdown") or {})
    overall_status = "ACCEPTABLE"
    reasons: list[str] = []
    for session, row in session_rows.items():
        add_net = float(row.get("add_contribution_net_pnl_cash") or 0.0)
        add_count = int(row.get("add_count") or 0)
        if add_count <= 0:
            status = "NO_ADDS"
            reason = "No add observations for this session."
        elif add_net < 0.0:
            status = "NEGATIVE"
            reason = "Add contribution is negative in this session."
            overall_status = "LIMITED_BY_SESSION_WEAKNESS"
            reasons.append(f"{session} add contribution is negative")
        else:
            status = "POSITIVE"
            reason = "Add contribution is positive in this session."
        sessions[session] = {
            "status": status,
            "reason": reason,
            "add_count": add_count,
            "add_contribution_net_pnl_cash": round(add_net, 4),
        }
    if not reasons:
        overall_reason = "No session showed negative add contribution."
    else:
        overall_reason = "; ".join(reasons)
    return {
        "overall_status": overall_status,
        "overall_reason": overall_reason,
        "sessions": sessions,
    }


def _candidate_lane_readiness(*, candidate_payload: dict[str, Any], session_acceptability: dict[str, Any]) -> dict[str, Any]:
    if candidate_payload.get("quality_verdict") != "QUALITY_IMPROVED":
        return {
            "verdict": "RESEARCH_ONLY",
            "reason": "The candidate did not clear a clean quality-improvement threshold versus baseline.",
        }
    if session_acceptability.get("overall_status") != "ACCEPTABLE":
        return {
            "verdict": "RESEARCH_WORTHY_NOT_PROMOTION_READY",
            "reason": "The candidate is promising overall, but negative session contribution still limits promotion readiness.",
        }
    return {
        "verdict": "ADVANCE_TO_NEXT_RESEARCH_STAGE",
        "reason": "The candidate improved quality and did not show session-level weakness in the sampled evidence.",
    }


def _candidate_session_breakdown(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    buckets: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "trade_count": 0,
            "add_count": 0,
            "net_pnl_cash": 0.0,
            "add_contribution_net_pnl_cash": 0.0,
        }
    )
    for row in rows:
        session = str(row.get("session_segment") or "UNKNOWN")
        bucket = buckets[session]
        bucket["trade_count"] += 1
        bucket["net_pnl_cash"] = round(bucket["net_pnl_cash"] + float(row.get("pnl_cash") or 0.0), 4)
        if row.get("added"):
            bucket["add_count"] += 1
            bucket["add_contribution_net_pnl_cash"] = round(
                bucket["add_contribution_net_pnl_cash"] + float(row.get("add_pnl_cash") or 0.0),
                4,
            )
    return dict(sorted(buckets.items()))


def _baseline_trade_row(trade: TradeRecord) -> dict[str, Any]:
    return {
        "entry_ts": trade.entry_ts,
        "decision_ts": trade.decision_ts,
        "pnl_cash": float(trade.pnl_cash),
        "mfe_points": float(trade.mfe_points),
        "mae_points": float(trade.mae_points),
        "hold_minutes": float(trade.hold_minutes),
        "bars_held_1m": int(trade.bars_held_1m),
        "side": trade.side,
        "session_segment": trade.session_segment,
    }


def _trade_window_bars(
    *,
    trade: TradeRecord,
    bars_1m: Sequence[ResearchBar],
    bars_by_end_ts: dict[datetime, int],
) -> list[ResearchBar]:
    start_index = bars_by_end_ts.get(trade.entry_ts)
    end_index = bars_by_end_ts.get(trade.exit_ts)
    if start_index is None or end_index is None or end_index <= start_index:
        return []
    return list(bars_1m[start_index + 1 : end_index + 1])


def _no_add_result(*, trade: TradeRecord, candidate: PromotionAddCandidate, reason: str) -> dict[str, Any]:
    return {
        "added": False,
        "candidate_id": candidate.candidate_id,
        "candidate_label": candidate.label,
        "entry_ts": trade.entry_ts,
        "decision_ts": trade.decision_ts,
        "position_entry_price": float(trade.entry_price),
        "position_exit_price": float(trade.exit_price),
        "trade_pnl_cash": float(trade.pnl_cash),
        "pnl_cash": float(trade.pnl_cash),
        "add_pnl_cash": 0.0,
        "add_pnl_points": 0.0,
        "add_entry_ts": None,
        "add_exit_ts": None,
        "add_entry_price": None,
        "add_trigger_price": None,
        "add_price_quality_state": None,
        "add_reason": reason,
        "evidence_truth": "strong_replay_derived",
        "modeled_exit_dependency": "inherits_frozen_baseline_exit",
        "depends_on_weak_evidence": False,
        "hold_minutes": float(trade.hold_minutes),
        "bars_held_1m": int(trade.bars_held_1m),
        "add_hold_minutes": 0.0,
        "side": trade.side,
        "session_segment": trade.session_segment,
        "mfe_points": float(trade.mfe_points),
        "mae_points": float(trade.mae_points),
        "family": trade.family,
        "exit_reason": trade.exit_reason,
    }


def _dominant_session_tag(rows: Sequence[dict[str, Any]]) -> str:
    session_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        session_counts[str(row.get("session_segment") or "UNKNOWN")] += 1
    if not session_counts:
        return "NO_TRADES"
    return max(session_counts.items(), key=lambda item: item[1])[0]


def _shared_1m_coverage(*, sqlite_path: Path, instruments: tuple[str, ...]) -> tuple[datetime, datetime]:
    connection = sqlite3.connect(sqlite_path)
    try:
        rows = connection.execute(
            """
            select symbol, min(end_ts), max(end_ts)
            from bars
            where timeframe = '1m' and symbol in ({placeholders})
            group by symbol
            """.format(placeholders=",".join("?" for _ in instruments)),
            list(instruments),
        ).fetchall()
    finally:
        connection.close()
    starts = [datetime.fromisoformat(row[1]) for row in rows]
    ends = [datetime.fromisoformat(row[2]) for row in rows]
    return max(starts), min(ends)
