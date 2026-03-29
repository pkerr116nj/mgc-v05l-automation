"""Compact executable-candidate review for US-hours families."""

from __future__ import annotations

import csv
import json
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

from .session_phase_labels import label_session_phase


ROOT = Path("/Users/patrick/Documents/MGC-v05l-automation")
REPORT_DIR = ROOT / "outputs/reports/approved_branch_research"
PAPER_CONFIG_PATH = ROOT / "config/probationary_pattern_engine_paper.yaml"
IMPULSE_SAME_BAR_PATH = REPORT_DIR / "mgc_impulse_same_bar_causalization.json"
IMPULSE_DELAYED_PATH = REPORT_DIR / "mgc_impulse_delayed_confirmation_revalidation.json"

REVIEW_PATHS: dict[str, dict[str, Path]] = {
    "usLatePauseResumeLongTurn": {
        "summary": ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.summary.json",
        "ledger": ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.trade_ledger.csv",
    },
    "usDerivativeBearTurn": {
        "summary": ROOT / "outputs/replays/persisted_bar_replay_us_derivative_bear_retest_us_derivative_bear_retest_20260316_widen_1_full.summary.json",
        "ledger": ROOT / "outputs/replays/persisted_bar_replay_us_derivative_bear_retest_us_derivative_bear_retest_20260316_widen_1_full.trade_ledger.csv",
    },
    "usDerivativeBearAdditiveTurn": {
        "summary": ROOT / "outputs/replays/persisted_bar_replay_additive_lane_open_late_only_downside_resumption_break_2_full_20260316.summary.json",
        "ledger": ROOT / "outputs/replays/persisted_bar_replay_additive_lane_open_late_only_downside_resumption_break_2_full_20260316.trade_ledger.csv",
    },
    "firstBullSnapTurn": {
        "summary": ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.summary.json",
        "ledger": ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.trade_ledger.csv",
    },
    "firstBearSnapTurn": {
        "summary": ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.summary.json",
        "ledger": ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.trade_ledger.csv",
    },
}

STRUCTURAL_NOTES: dict[str, dict[str, Any]] = {
    "usLatePauseResumeLongTurn": {
        "bias": "LONG_BIASED",
        "future_confirmation": False,
        "session_behavior": "US_LATE only",
        "special_context": "Requires one-bar pullback and resumed break in the late-US turn context.",
        "candidate_type": "mature_pause_resume_long",
    },
    "usDerivativeBearTurn": {
        "bias": "SHORT_BIASED",
        "future_confirmation": False,
        "session_behavior": "US_PREOPEN_OPENING, US_CASH_OPEN_IMPULSE, and US_OPEN_LATE",
        "special_context": "Derivative short turn with VWAP / EMA / slope-curvature gating in a narrow US morning window.",
        "candidate_type": "us_open_short_derivative",
    },
    "usDerivativeBearAdditiveTurn": {
        "bias": "SHORT_BIASED",
        "future_confirmation": False,
        "session_behavior": "Mostly US_OPEN_LATE additive pocket",
        "special_context": "Additive-only short lane requiring a narrower recent-context pattern inside the derivative-bear stack.",
        "candidate_type": "us_open_additive_short",
    },
    "firstBullSnapTurn": {
        "bias": "LONG_BIASED",
        "future_confirmation": False,
        "session_behavior": "Mostly ASIA_EARLY and ASIA_LATE",
        "special_context": "Baseline reversal family; useful reference traffic, but not a focused US-hours operating model.",
        "candidate_type": "baseline_snap_reference",
    },
    "firstBearSnapTurn": {
        "bias": "SHORT_BIASED",
        "future_confirmation": False,
        "session_behavior": "Mixed ASIA and US traffic",
        "special_context": "Baseline short snap family spanning multiple pockets, not a clean dedicated US-hours research lane.",
        "candidate_type": "baseline_snap_reference",
    },
}


@dataclass(frozen=True)
class CandidateMetrics:
    sample_start: str
    sample_end: str
    trades: int
    realized_pnl: float
    avg_trade: float
    median_trade: float
    profit_factor: float | None
    max_drawdown: float
    win_rate: float
    top_1_contribution: float | None
    top_3_contribution: float | None
    survives_without_top_1: bool
    survives_without_top_3: bool


def build_and_write_us_hours_candidate_review() -> dict[str, str]:
    impulse_reference = _load_impulse_reference()
    admitted_sources = _load_admitted_sources(PAPER_CONFIG_PATH)
    candidates = [_build_candidate_row(family, admitted_sources, impulse_reference) for family in REVIEW_PATHS]
    ranked = _rank_recommendations(candidates)
    comparisons = _build_comparisons(candidates)
    europe_note = (
        "No Europe-hours family deserves immediate attention from this review set. "
        "The best naturally executable opportunities here remain US_LATE and US-open pockets."
    )

    payload = {
        "thread_scope": "thread_1_only",
        "research_scope": "research_only",
        "families_reviewed": list(REVIEW_PATHS),
        "impulse_branch_status_reference": impulse_reference,
        "candidate_reviews": candidates,
        "comparisons": comparisons,
        "ranked_recommendation": ranked,
        "europe_hours_note": europe_note,
    }

    json_path = REPORT_DIR / "us_hours_candidate_review.json"
    md_path = REPORT_DIR / "us_hours_candidate_review.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "us_hours_candidate_review_json": str(json_path),
        "us_hours_candidate_review_md": str(md_path),
    }


def _build_candidate_row(
    family: str,
    admitted_sources: set[str],
    impulse_reference: dict[str, Any],
) -> dict[str, Any]:
    summary = json.loads(REVIEW_PATHS[family]["summary"].read_text(encoding="utf-8"))
    ledger_rows = _load_family_rows(REVIEW_PATHS[family]["ledger"], family)
    metrics = _compute_metrics(summary, ledger_rows)
    session_counts = Counter(_resolve_entry_phase(row) for row in ledger_rows)
    note = STRUCTURAL_NOTES[family]
    already_admitted = family in admitted_sources
    cleaner_than_impulse = _cleaner_than_failed_impulse(metrics, family, impulse_reference)

    return {
        "family": family,
        "structural_execution_fit": {
            "naturally_causal_executable": True,
            "relies_on_future_confirmation": note["future_confirmation"],
            "fits_live_us_hours_model": family in {"usLatePauseResumeLongTurn", "usDerivativeBearTurn", "usDerivativeBearAdditiveTurn"},
            "likely_observable_activity_in_europe_or_us_windows": family != "firstBullSnapTurn",
            "cleaner_than_failed_direct_impulse_branch": cleaner_than_impulse,
        },
        "economic_replay_quality": asdict(metrics),
        "operational_suitability": {
            "session_windows_et": _session_windows_et(session_counts),
            "entry_phase_counts": dict(session_counts),
            "already_admitted_anywhere_in_paper": already_admitted,
            "bias": note["bias"],
            "depends_on_special_context": note["special_context"],
            "session_behavior": note["session_behavior"],
        },
        "verdict_bucket": _verdict_bucket(family, metrics, already_admitted),
        "artifact_sources": {
            "summary": str(REVIEW_PATHS[family]["summary"]),
            "trade_ledger": str(REVIEW_PATHS[family]["ledger"]),
        },
    }


def _load_family_rows(path: Path, family: str) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return [row for row in csv.DictReader(handle) if row["setup_family"] == family]


def _compute_metrics(summary: dict[str, Any], rows: list[dict[str, str]]) -> CandidateMetrics:
    pnls = [float(row["net_pnl"]) for row in rows]
    wins = [value for value in pnls if value > 0]
    losses = [-value for value in pnls if value < 0]
    gross_profit = sum(wins)
    gross_loss = sum(losses)
    profit_factor = None if gross_loss == 0 else gross_profit / gross_loss
    drawdown = _max_drawdown(pnls)
    ordered_winners = sorted(pnls, reverse=True)
    total = sum(pnls)
    top1 = ordered_winners[0] if ordered_winners else 0.0
    top3 = sum(ordered_winners[:3]) if ordered_winners else 0.0
    return CandidateMetrics(
        sample_start=summary.get("slice_start_ts") or summary.get("source_first_bar_ts"),
        sample_end=summary.get("slice_end_ts") or summary.get("source_last_bar_ts"),
        trades=len(pnls),
        realized_pnl=round(total, 4),
        avg_trade=round(total / len(pnls), 4) if pnls else 0.0,
        median_trade=round(float(median(pnls)), 4) if pnls else 0.0,
        profit_factor=round(profit_factor, 4) if profit_factor is not None else None,
        max_drawdown=round(drawdown, 4),
        win_rate=round(sum(1 for value in pnls if value > 0) / len(pnls), 4) if pnls else 0.0,
        top_1_contribution=round((top1 / total) * 100, 4) if total else None,
        top_3_contribution=round((top3 / total) * 100, 4) if total else None,
        survives_without_top_1=(total - top1) > 0,
        survives_without_top_3=(total - top3) > 0,
    )


def _max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return max_drawdown


def _resolve_entry_phase(row: dict[str, str]) -> str:
    if row.get("entry_session_phase"):
        return row["entry_session_phase"]
    if row.get("entry_ts"):
        return label_session_phase(datetime.fromisoformat(row["entry_ts"]))
    return "UNCLASSIFIED"


def _load_admitted_sources(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    marker = "probationary_paper_lanes_json: '"
    start = text.find(marker)
    if start == -1:
        return set()
    start += len(marker)
    end = text.find("'\n", start)
    lanes = json.loads(text[start:end])
    admitted: set[str] = set()
    for lane in lanes:
        admitted.update(lane.get("long_sources", []))
        admitted.update(lane.get("short_sources", []))
    return admitted


def _load_impulse_reference() -> dict[str, Any]:
    same_bar = json.loads(IMPULSE_SAME_BAR_PATH.read_text(encoding="utf-8"))
    delayed = json.loads(IMPULSE_DELAYED_PATH.read_text(encoding="utf-8"))
    return {
        "family": "impulse_burst_continuation",
        "failed_same_bar_bucket": same_bar["causalization_conclusion"]["best_bucket"],
        "failed_same_bar_metrics": same_bar["raw_control_metrics"],
        "failed_delayed_bucket": delayed["causal_revalidation_conclusion"]["decision_bucket"],
        "failed_delayed_best_variant": delayed["best_delayed_confirmation_variant"]["variant_name"],
    }


def _cleaner_than_failed_impulse(metrics: CandidateMetrics, family: str, impulse_reference: dict[str, Any]) -> bool:
    impulse_metrics = impulse_reference["failed_same_bar_metrics"]
    if family in {"firstBullSnapTurn", "firstBearSnapTurn"}:
        return bool(
            metrics.profit_factor
            and metrics.profit_factor > impulse_metrics["profit_factor"]
            and metrics.median_trade > impulse_metrics["median_trade"]
            and (metrics.top_3_contribution or 10_000) < impulse_metrics["top_3_contribution"]
        )
    return bool(
        metrics.profit_factor
        and metrics.profit_factor > impulse_metrics["profit_factor"]
        and metrics.max_drawdown < impulse_metrics["max_drawdown"]
    )


def _verdict_bucket(family: str, metrics: CandidateMetrics, already_admitted: bool) -> str:
    if already_admitted:
        return "PRIORITIZE_NOW"
    if family == "usDerivativeBearTurn":
        return "SERIOUS_NEXT_CANDIDATE"
    if family == "usDerivativeBearAdditiveTurn":
        return "LATER_REVIEW"
    if family == "firstBullSnapTurn":
        return "DEPRIORITIZE"
    if family == "firstBearSnapTurn":
        return "DEPRIORITIZE"
    return "LATER_REVIEW"


def _session_windows_et(session_counts: Counter[str] | dict[str, int]) -> list[str]:
    windows = {
        "US_PREOPEN_OPENING": "09:00-09:30 ET",
        "US_CASH_OPEN_IMPULSE": "09:30-10:00 ET",
        "US_OPEN_LATE": "10:00-10:30 ET",
        "US_MIDDAY": "10:30-14:00 ET",
        "US_LATE": "14:00-17:00 ET",
        "ASIA_EARLY": "18:00-20:30 ET",
        "ASIA_LATE": "20:30-23:00 ET",
        "LONDON_OPEN": "03:00-05:30 ET",
        "UNCLASSIFIED": "Outside labeled research pockets",
        "SESSION_RESET_1800": "18:00 ET reset bar",
    }
    counter = session_counts if isinstance(session_counts, Counter) else Counter(session_counts)
    return [f"{phase}: {windows.get(phase, phase)}" for phase, _ in counter.most_common()]


def _build_comparisons(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    by_family = {row["family"]: row for row in candidates}
    us_late = by_family["usLatePauseResumeLongTurn"]
    derivative = by_family["usDerivativeBearTurn"]
    additive = by_family["usDerivativeBearAdditiveTurn"]
    bull = by_family["firstBullSnapTurn"]
    bear = by_family["firstBearSnapTurn"]
    return {
        "usLatePauseResumeLongTurn_vs_usDerivativeBearTurn": {
            "winner_for_next_active_research": "usDerivativeBearTurn",
            "reason": (
                "usLatePauseResumeLongTurn is already paper-admitted and economically mature; "
                "usDerivativeBearTurn is the cleaner non-admitted US-hours short branch and the better next research frontier."
            ),
            "us_late_metrics": us_late["economic_replay_quality"],
            "derivative_metrics": derivative["economic_replay_quality"],
        },
        "usDerivativeBearTurn_vs_usDerivativeBearAdditiveTurn": {
            "winner": "usDerivativeBearTurn",
            "reason": (
                "The additive branch inherits the same causal structure but is materially thinner and more context-dependent. "
                "DerivativeBearTurn has the stronger evidence base and better chance of becoming the next paper candidate."
            ),
            "derivative_metrics": derivative["economic_replay_quality"],
            "additive_metrics": additive["economic_replay_quality"],
        },
        "snap_turn_operator_baseline_reference": {
            "firstBullSnapTurn": bull["economic_replay_quality"],
            "firstBearSnapTurn": bear["economic_replay_quality"],
            "reason": (
                "The snap families remain useful baseline traffic references, but their median trade and concentration profile "
                "make them weaker US-hours funnel candidates than the pause-resume and derivative-bear branches."
            ),
        },
    }


def _rank_recommendations(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    ranked_families = [
        "usDerivativeBearTurn",
        "usLatePauseResumeLongTurn",
        "usDerivativeBearAdditiveTurn",
        "firstBearSnapTurn",
        "firstBullSnapTurn",
    ]
    return {
        "ranked_families": ranked_families,
        "next_active_thread_1_research_focus": "usDerivativeBearTurn",
        "most_likely_to_produce_observable_live_hour_behavior_soon": "usLatePauseResumeLongTurn",
        "most_likely_to_become_next_paper_candidate": "usDerivativeBearTurn",
        "do_not_spend_time_now": "firstBullSnapTurn",
        "rationale": {
            "usDerivativeBearTurn": (
                "Best non-admitted executable US-hours short candidate: causal, focused in US morning pockets, "
                "strong PF and low drawdown, but still thin enough to need one more disciplined pass."
            ),
            "usLatePauseResumeLongTurn": (
                "Closest to paper quality overall and already admitted in MGC paper; not the best next research frontier because it is already mature."
            ),
            "usDerivativeBearAdditiveTurn": (
                "Interesting additive pocket, but too thin and too special-context to outrank the parent derivative-bear branch."
            ),
            "firstBullSnapTurn": (
                "Mostly Asia traffic with weak median trade and concentration fragility; poor fit for the current executable US-hours push."
            ),
        },
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# US-Hours Candidate Review",
        "",
        "## Ranked Recommendation",
        f"- Next active Thread 1 research focus: `{payload['ranked_recommendation']['next_active_thread_1_research_focus']}`",
        f"- Most likely to produce observable live-hour behavior soon: `{payload['ranked_recommendation']['most_likely_to_produce_observable_live_hour_behavior_soon']}`",
        f"- Most likely to become the next paper candidate: `{payload['ranked_recommendation']['most_likely_to_become_next_paper_candidate']}`",
        f"- Do not spend time on right now: `{payload['ranked_recommendation']['do_not_spend_time_now']}`",
        f"- Europe-hours note: {payload['europe_hours_note']}",
        "",
        "## Candidate Reviews",
    ]
    for row in payload["candidate_reviews"]:
        metrics = row["economic_replay_quality"]
        ops = row["operational_suitability"]
        fit = row["structural_execution_fit"]
        lines.extend(
            [
                f"### {row['family']}",
                f"- Verdict: `{row['verdict_bucket']}`",
                f"- Sample: `{metrics['sample_start']}` -> `{metrics['sample_end']}`",
                (
                    f"- Trades `{metrics['trades']}`, realized P/L `{metrics['realized_pnl']}`, avg trade `{metrics['avg_trade']}`, "
                    f"median trade `{metrics['median_trade']}`, PF `{metrics['profit_factor']}`, max DD `{metrics['max_drawdown']}`, "
                    f"win rate `{metrics['win_rate']}`"
                ),
                (
                    f"- Concentration: top-1 `{metrics['top_1_contribution']}`, top-3 `{metrics['top_3_contribution']}`, "
                    f"survives ex top-1 `{metrics['survives_without_top_1']}`, survives ex top-3 `{metrics['survives_without_top_3']}`"
                ),
                f"- Structural fit: causal `{fit['naturally_causal_executable']}`, future confirmation `{fit['relies_on_future_confirmation']}`, cleaner than failed impulse `{fit['cleaner_than_failed_direct_impulse_branch']}`",
                f"- Sessions: {', '.join(ops['session_windows_et'])}",
                f"- Paper admission today: `{ops['already_admitted_anywhere_in_paper']}`",
                f"- Bias: `{ops['bias']}`",
                f"- Context note: {ops['depends_on_special_context']}",
                "",
            ]
        )
    lines.extend(
        [
            "## Required Comparisons",
            f"- usLatePauseResumeLongTurn vs usDerivativeBearTurn: {payload['comparisons']['usLatePauseResumeLongTurn_vs_usDerivativeBearTurn']['reason']}",
            f"- usDerivativeBearTurn vs usDerivativeBearAdditiveTurn: {payload['comparisons']['usDerivativeBearTurn_vs_usDerivativeBearAdditiveTurn']['reason']}",
            f"- SnapTurn baseline note: {payload['comparisons']['snap_turn_operator_baseline_reference']['reason']}",
            "",
            "## Impulse Reference",
            (
                f"- Failed direct impulse branch reference: same-bar `{payload['impulse_branch_status_reference']['failed_same_bar_bucket']}`, "
                f"delayed `{payload['impulse_branch_status_reference']['failed_delayed_bucket']}`"
            ),
        ]
    )
    return "\n".join(lines) + "\n"
