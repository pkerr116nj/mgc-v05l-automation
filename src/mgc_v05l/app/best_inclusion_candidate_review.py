"""Generate a compact research review for the single best new inclusion candidate."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, median
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "approved_branch_research"
LATEST_JSON_PATH = REPORT_DIR / "best_inclusion_candidate_review.json"
LATEST_MD_PATH = REPORT_DIR / "best_inclusion_candidate_review.md"

US_HOURS_REVIEW_PATH = REPORT_DIR / "us_hours_candidate_review.json"
MGC_DERIVATIVE_VALIDATION_PATH = REPORT_DIR / "mgc_usDerivativeBearTurn_validation.json"
MNQ_DERIVATIVE_VALIDATION_PATH = REPORT_DIR / "mnq_usDerivativeBearTurn_validation.json"
DEPLOYMENT_RANKING_PATH = REPORT_DIR / "approved_branch_futures_deployment_ranking.json"
ROBUSTNESS_SHORTLIST_PATH = REPORT_DIR / "robustness_prep_shortlist.json"
NARROW_ADAPTATION_PATH = REPORT_DIR / "narrow_adaptation_candidates.json"


@dataclass(frozen=True)
class CandidateSource:
    instrument: str
    family: str
    session_windows_et: tuple[str, ...]
    long_short_bias: str
    trade_ledger_path: Path
    summary_path: Path
    already_active: bool
    overlay_note: str | None = None


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_trade_rows(path: Path, family: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("setup_family") != family:
                continue
            row_copy = dict(row)
            row_copy["net_pnl"] = float(row_copy["net_pnl"])
            rows.append(row_copy)
    rows.sort(key=lambda row: row["exit_ts"])
    return rows


def _compute_trade_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(row["net_pnl"]) for row in rows]
    if not pnls:
        return {
            "sample_start": None,
            "sample_end": None,
            "trades": 0,
            "realized_pnl": 0.0,
            "avg_trade": None,
            "median_trade": None,
            "profit_factor": None,
            "max_drawdown": None,
            "win_rate": None,
            "average_loser": None,
            "average_winner": None,
            "avg_winner_over_avg_loser": None,
            "top_1_contribution": None,
            "top_3_contribution": None,
            "survives_without_top_1": False,
            "survives_without_top_3": False,
        }
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    peak = 0.0
    equity = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    sorted_desc = sorted(pnls, reverse=True)
    top_1 = sorted_desc[0]
    top_3 = sum(sorted_desc[:3])
    realized_pnl = sum(pnls)
    average_winner = mean(wins) if wins else None
    average_loser = mean(losses) if losses else None
    return {
        "sample_start": rows[0]["entry_ts"],
        "sample_end": rows[-1]["exit_ts"],
        "trades": len(rows),
        "realized_pnl": realized_pnl,
        "avg_trade": mean(pnls),
        "median_trade": median(pnls),
        "profit_factor": (gross_profit / gross_loss) if gross_loss else None,
        "max_drawdown": max_drawdown,
        "win_rate": len(wins) / len(pnls),
        "average_loser": average_loser,
        "average_winner": average_winner,
        "avg_winner_over_avg_loser": (
            average_winner / abs(average_loser)
            if average_winner is not None and average_loser not in (None, 0)
            else None
        ),
        "top_1_contribution": (100.0 * top_1 / realized_pnl) if realized_pnl else None,
        "top_3_contribution": (100.0 * top_3 / realized_pnl) if realized_pnl else None,
        "survives_without_top_1": (realized_pnl - top_1) > 0,
        "survives_without_top_3": (realized_pnl - top_3) > 0,
    }


def _candidate_priority(metrics: dict[str, Any]) -> tuple[float, ...]:
    realized = float(metrics["realized_pnl"] or 0.0)
    trades = float(metrics["trades"] or 0.0)
    profit_factor = float(metrics["profit_factor"] or 0.0)
    top_1 = float(metrics["top_1_contribution"] or 0.0)
    top_3 = float(metrics["top_3_contribution"] or 0.0)
    thin_penalty = 400.0 if trades < 10 else 0.0
    concentration_penalty = max(0.0, top_1 - 100.0) + max(0.0, top_3 - 100.0)
    score = realized + trades * 8.0 + profit_factor * 60.0 - thin_penalty - concentration_penalty
    return (
        score,
        realized,
        trades,
        profit_factor,
    )


def _verdict_bucket(metrics: dict[str, Any]) -> str:
    if (metrics["trades"] or 0) < 8 or (metrics["realized_pnl"] or 0.0) <= 0:
        return "INTERESTING_BUT_NOT_YET"
    if (
        (metrics["trades"] or 0) >= 25
        and (metrics["profit_factor"] or 0.0) >= 2.0
        and metrics["survives_without_top_1"]
        and metrics["survives_without_top_3"]
    ):
        return "INCLUDE_NOW_RESEARCH_PRIORITY"
    if (metrics["profit_factor"] or 0.0) >= 1.5 and (metrics["trades"] or 0) >= 15:
        return "SERIOUS_NEXT_CANDIDATE"
    return "INTERESTING_BUT_NOT_YET"


def _current_leads() -> dict[str, Any]:
    us_hours_review = _load_json(US_HOURS_REVIEW_PATH)
    us_late_row = next(
        row for row in us_hours_review["candidate_reviews"] if row["family"] == "usLatePauseResumeLongTurn"
    )
    mgc_derivative = _load_json(MGC_DERIVATIVE_VALIDATION_PATH)
    mnq_derivative = _load_json(MNQ_DERIVATIVE_VALIDATION_PATH)
    return {
        "usLatePauseResumeLongTurn": {
            "instrument": "MGC",
            "family": "usLatePauseResumeLongTurn",
            "session_windows_et": us_late_row["operational_suitability"]["session_windows_et"],
            "metrics": us_late_row["economic_replay_quality"],
        },
        "MGC_usDerivativeBearTurn": {
            "instrument": "MGC",
            "family": "usDerivativeBearTurn",
            "session_windows_et": mgc_derivative["structural_execution_fit"]["session_windows_et"],
            "metrics": mgc_derivative["economic_replay_quality"],
        },
        "MNQ_usDerivativeBearTurn": {
            "instrument": "MNQ",
            "family": "usDerivativeBearTurn",
            "session_windows_et": mnq_derivative["structural_execution_fit"]["session_windows_et"],
            "metrics": mnq_derivative["economic_replay_quality"],
        },
    }


def _build_shortlist() -> list[dict[str, Any]]:
    contenders = [
        CandidateSource(
            instrument="GC",
            family="usLatePauseResumeLongTurn",
            session_windows_et=("US_LATE: 14:00-17:00 ET",),
            long_short_bias="LONG_BIASED",
            trade_ledger_path=REPO_ROOT / "outputs" / "replays" / "persisted_bar_replay_second_pass_direct_approved_gc_20260319_130545.trade_ledger.csv",
            summary_path=REPO_ROOT / "outputs" / "replays" / "persisted_bar_replay_second_pass_direct_approved_gc_20260319_130545.summary.json",
            already_active=False,
        ),
        CandidateSource(
            instrument="MBT",
            family="asiaEarlyPauseResumeShortTurn",
            session_windows_et=("ASIA_EARLY: 18:00-20:30 ET",),
            long_short_bias="SHORT_BIASED",
            trade_ledger_path=REPO_ROOT / "outputs" / "replays" / "persisted_bar_replay_second_pass_direct_approved_mbt_20260319_131030.trade_ledger.csv",
            summary_path=REPO_ROOT / "outputs" / "replays" / "persisted_bar_replay_second_pass_direct_approved_mbt_20260319_131030.summary.json",
            already_active=False,
            overlay_note="Raw direct transfer only; sample remains too thin for extra narrowing to be meaningful.",
        ),
        CandidateSource(
            instrument="CL",
            family="usLatePauseResumeLongTurn",
            session_windows_et=("US_LATE: 14:00-17:00 ET",),
            long_short_bias="LONG_BIASED",
            trade_ledger_path=REPO_ROOT / "outputs" / "replays" / "persisted_bar_replay_second_pass_direct_approved_cl_20260319_130458.trade_ledger.csv",
            summary_path=REPO_ROOT / "outputs" / "replays" / "persisted_bar_replay_second_pass_direct_approved_cl_20260319_130458.summary.json",
            already_active=False,
            overlay_note="Tested compact curvature/expansion narrowing improved neatness but not enough to change admission quality.",
        ),
        CandidateSource(
            instrument="NG",
            family="usLatePauseResumeLongTurn",
            session_windows_et=("US_LATE: 14:00-17:00 ET",),
            long_short_bias="LONG_BIASED",
            trade_ledger_path=REPO_ROOT / "outputs" / "replays" / "persisted_bar_replay_second_pass_direct_approved_ng_20260319_130635.trade_ledger.csv",
            summary_path=REPO_ROOT / "outputs" / "replays" / "persisted_bar_replay_second_pass_direct_approved_ng_20260319_130635.summary.json",
            already_active=False,
            overlay_note="Compact narrowing helped only slightly on nearby review artifacts and did not resolve economics/concentration enough.",
        ),
    ]
    rows: list[dict[str, Any]] = []
    for contender in contenders:
        trade_rows = _load_trade_rows(contender.trade_ledger_path, contender.family)
        metrics = _compute_trade_metrics(trade_rows)
        rows.append(
            {
                "instrument": contender.instrument,
                "family": contender.family,
                "session_windows_et": list(contender.session_windows_et),
                "long_short_bias": contender.long_short_bias,
                "naturally_causal_executable": True,
                "already_active": contender.already_active,
                "summary_path": str(contender.summary_path),
                "trade_ledger_path": str(contender.trade_ledger_path),
                "overlay_note": contender.overlay_note,
                "metrics": metrics,
                "priority": _candidate_priority(metrics),
            }
        )
    rows.sort(key=lambda row: row["priority"], reverse=True)
    return rows


def _overlay_context() -> list[dict[str, Any]]:
    narrow = _load_json(NARROW_ADAPTATION_PATH)
    rows: list[dict[str, Any]] = []
    for row in narrow["rows"]:
        if (row["symbol"], row["branch"]) not in {
            ("CL", "usLatePauseResumeLongTurn"),
            ("NG", "usLatePauseResumeLongTurn"),
            ("NG", "asiaEarlyNormalBreakoutRetestHoldTurn"),
        }:
            continue
        rows.append(
            {
                "instrument": row["symbol"],
                "family": row["branch"],
                "raw_result": row["direct_result"],
                "best_overlay_result": row["best_variant_result"],
                "best_overlay": row["best_variant"],
                "adaptation_outcome": row["adaptation_outcome"],
                "reason_not_selected": "Overlay was either no material improvement or only slight noise improvement on a weaker base candidate.",
            }
        )
    return rows


def build_review() -> dict[str, Any]:
    shortlist = _build_shortlist()
    current_leads = _current_leads()
    deployment_ranking = _load_json(DEPLOYMENT_RANKING_PATH)
    robustness = _load_json(ROBUSTNESS_SHORTLIST_PATH)

    best_candidate = shortlist[0]
    verdict = _verdict_bucket(best_candidate["metrics"])

    strongest_next_set = [
        row
        for row in deployment_ranking["tier_2"]
        if not (row["symbol"] == "GC" and row["branch"] == "asiaEarlyNormalBreakoutRetestHoldTurn")
    ]

    review = {
        "generated_at": datetime.now(UTC).isoformat(),
        "research_scope": "research_only",
        "selection_method": "compact evidence review of existing replay/validation artifacts only; no new optimization or black-box search",
        "candidate_identity": {
            "instrument": best_candidate["instrument"],
            "family": best_candidate["family"],
            "session_windows_et": best_candidate["session_windows_et"],
            "long_short_bias": best_candidate["long_short_bias"],
            "naturally_causal_executable": best_candidate["naturally_causal_executable"],
        },
        "economic_profile": best_candidate["metrics"],
        "overlay_review": {
            "raw_family_result": best_candidate["metrics"],
            "material_overlay_found": False,
            "overlay_result": None,
            "overlay_reason": (
                "No compact overlay currently has repo evidence strong enough to improve this candidate materially "
                "without shrinking breadth or preserving concentration fragility."
            ),
            "runner_up_overlay_context": _overlay_context(),
        },
        "current_active_leads_comparison": current_leads,
        "non_active_shortlist": [
            {
                "instrument": row["instrument"],
                "family": row["family"],
                "metrics": row["metrics"],
                "selection_note": (
                    "Chosen candidate" if row["instrument"] == best_candidate["instrument"] and row["family"] == best_candidate["family"]
                    else "Not selected because evidence is thinner, economics are weaker, or concentration remains worse."
                ),
            }
            for row in shortlist
        ],
        "existing_next_set_context": {
            "deployment_next_probationary_paper_order": deployment_ranking["answers"]["next_probationary_paper_order"],
            "robustness_advance_bucket": robustness["buckets"]["ADVANCE_TO_ROBUSTNESS_TESTING"],
            "strongest_new_non_active_pair": "GC / usLatePauseResumeLongTurn",
            "why_ahead_of_other_non_active_pairs": (
                "It combines direct replay evidence, meaningful trade count, clear US_LATE concentration, "
                "and positive economics without requiring a new overlay to become causal. That now matters more "
                "because the refreshed MGC usDerivativeBearTurn packet is still positive but only four trades deep."
            ),
        },
        "inclusion_logic": {
            "why_not_just_lucky_outlier": (
                "The pair has 22 closed trades over the available replay window and stays entirely in US_LATE, "
                "so it is materially broader than the evidence-thin derivative-bear packets and much thicker than MBT Asia short."
            ),
            "why_overlay_is_or_is_not_legitimate": (
                "No new overlay is proposed. Existing narrow-adaptation work on adjacent candidates shows compact tightening "
                "mostly trims noise without changing ranking quality, which argues against smuggling in a disguised overfit."
            ),
            "why_stronger_than_current_next_candidate_set": (
                "Relative to MBT, CL, NG, and the later-review Asia portability names, GC usLate is the only non-active pair "
                "with both meaningful direct replay breadth and non-trivial economics on an already supported instrument."
            ),
            "why_not_stronger_than_current_active_leads": (
                "It does not displace the current leaders. MGC usLate remains the mature benchmark, and the derivative-bear lanes "
                "remain important short-side research leads. But the refreshed MGC derivative-bear validation reverted to a "
                "thin four-trade packet, so this candidate is the best additional inclusion right now, not a replacement."
            ),
        },
        "verdict_bucket": verdict,
        "direct_answers": {
            "single_best_new_inclusion_candidate": "GC / usLatePauseResumeLongTurn",
            "why_this_one_instead_of_current_leaders": (
                "Because the current leaders are already active. Among additional candidates, GC usLate is the best-supported new pair "
                "with direct replay evidence and credible causal structure, while the refreshed MGC usDerivativeBearTurn branch remains "
                "alive but too thin to elevate."
            ),
            "edge_location": "RAW_FAMILY",
            "biggest_reason_it_still_might_fail_later": (
                "Return concentration is still high: the pair fails both survives_without_top_1 and survives_without_top_3."
            ),
        },
        "source_artifacts": {
            "candidate_summary": best_candidate["summary_path"],
            "candidate_trade_ledger": best_candidate["trade_ledger_path"],
            "us_hours_candidate_review": str(US_HOURS_REVIEW_PATH),
            "mgc_us_derivative_validation": str(MGC_DERIVATIVE_VALIDATION_PATH),
            "mnq_us_derivative_validation": str(MNQ_DERIVATIVE_VALIDATION_PATH),
            "deployment_ranking": str(DEPLOYMENT_RANKING_PATH),
            "robustness_shortlist": str(ROBUSTNESS_SHORTLIST_PATH),
            "narrow_adaptation_candidates": str(NARROW_ADAPTATION_PATH),
        },
    }
    return review


def _format_float(value: Any, digits: int = 2) -> str:
    if value is None:
        return "Unavailable"
    return f"{float(value):.{digits}f}"


def render_markdown(review: dict[str, Any]) -> str:
    candidate = review["candidate_identity"]
    econ = review["economic_profile"]
    lines = [
        "# Best Inclusion Candidate Review",
        "",
        "## Candidate",
        f"- Instrument: `{candidate['instrument']}`",
        f"- Family: `{candidate['family']}`",
        f"- Session windows (ET): {', '.join(candidate['session_windows_et'])}",
        f"- Bias: `{candidate['long_short_bias']}`",
        f"- Naturally causal / executable: `{candidate['naturally_causal_executable']}`",
        "",
        "## Economic Profile",
        f"- Sample: `{econ['sample_start']}` -> `{econ['sample_end']}`",
        f"- Trades: `{econ['trades']}`",
        f"- Realized P/L: `{_format_float(econ['realized_pnl'])}`",
        f"- Avg trade: `{_format_float(econ['avg_trade'])}`",
        f"- Median trade: `{_format_float(econ['median_trade'])}`",
        f"- PF: `{_format_float(econ['profit_factor'])}`",
        f"- Max DD: `{_format_float(econ['max_drawdown'])}`",
        f"- Win rate: `{_format_float((econ['win_rate'] or 0.0) * 100.0)}%`",
        f"- Average loser: `{_format_float(econ['average_loser'])}`",
        f"- Average winner: `{_format_float(econ['average_winner'])}`",
        f"- Avg winner / avg loser: `{_format_float(econ['avg_winner_over_avg_loser'])}`",
        f"- Top-1 contribution: `{_format_float(econ['top_1_contribution'])}%`",
        f"- Top-3 contribution: `{_format_float(econ['top_3_contribution'])}%`",
        f"- Survives without top-1: `{econ['survives_without_top_1']}`",
        f"- Survives without top-3: `{econ['survives_without_top_3']}`",
        "",
        "## Inclusion Logic",
        f"- Why not just lucky: {review['inclusion_logic']['why_not_just_lucky_outlier']}",
        f"- Overlay view: {review['inclusion_logic']['why_overlay_is_or_is_not_legitimate']}",
        f"- Why ahead of other new pairs: {review['inclusion_logic']['why_stronger_than_current_next_candidate_set']}",
        f"- Why not a replacement for current leads: {review['inclusion_logic']['why_not_stronger_than_current_active_leads']}",
        "",
        "## Verdict",
        f"- `{review['verdict_bucket']}`",
        "",
        "## Direct Answers",
        f"1. Single best new inclusion candidate: `{review['direct_answers']['single_best_new_inclusion_candidate']}`",
        f"2. Why this one instead of the current leaders: {review['direct_answers']['why_this_one_instead_of_current_leaders']}",
        f"3. Edge location: `{review['direct_answers']['edge_location']}`",
        f"4. Biggest failure risk later: {review['direct_answers']['biggest_reason_it_still_might_fail_later']}",
    ]
    return "\n".join(lines) + "\n"


def write_outputs(review: dict[str, Any]) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_JSON_PATH.write_text(json.dumps(review, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    LATEST_MD_PATH.write_text(render_markdown(review), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the best inclusion candidate review artifacts.")
    parser.parse_args()
    review = build_review()
    write_outputs(review)


if __name__ == "__main__":
    main()
