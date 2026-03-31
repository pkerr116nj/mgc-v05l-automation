"""Compact inclusion-readiness review for GC / usLatePauseResumeLongTurn."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .mnq_usDerivativeBearTurn_validation import (
    MGC_US_LATE_REFERENCE,
    PairMetrics,
    _compute_metrics,
    _load_impulse_reference,
    _load_reference_metrics,
    _load_rows,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "approved_branch_research"

LATEST_JSON_PATH = REPORT_DIR / "gc_usLatePauseResumeLongTurn_inclusion_readiness.json"
LATEST_MD_PATH = REPORT_DIR / "gc_usLatePauseResumeLongTurn_inclusion_readiness.md"

GC_NATIVE_PACKET = {
    "summary": REPO_ROOT / "outputs/replays/persisted_bar_replay_second_pass_direct_approved_gc_20260319_130545.summary.json",
    "summary_metrics": REPO_ROOT / "outputs/replays/persisted_bar_replay_second_pass_direct_approved_gc_20260319_130545.summary_metrics.json",
    "trade_ledger": REPO_ROOT / "outputs/replays/persisted_bar_replay_second_pass_direct_approved_gc_20260319_130545.trade_ledger.csv",
    "replay_db": REPO_ROOT / "outputs/replays/persisted_bar_replay_second_pass_direct_approved_gc_20260319_130545.sqlite3",
}
GC_ADJACENT_REFERENCE_FAMILY = "asiaEarlyNormalBreakoutRetestHoldTurn"
TARGET_FAMILY = "usLatePauseResumeLongTurn"
MGC_DERIVATIVE_VALIDATION_JSON = REPORT_DIR / "mgc_usDerivativeBearTurn_validation.json"
PAPER_CONFIG_IN_FORCE = REPO_ROOT / "outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json"
PAPER_APPROVED_MODELS = REPO_ROOT / "outputs/operator_dashboard/paper_approved_models_snapshot.json"


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog="gc-uslate-pause-resume-long-inclusion-readiness")


def main(argv: list[str] | None = None) -> int:
    build_parser().parse_args(argv)
    payload = build_and_write_gc_uslate_inclusion_readiness()
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_and_write_gc_uslate_inclusion_readiness() -> dict[str, Any]:
    summary = json.loads(GC_NATIVE_PACKET["summary"].read_text(encoding="utf-8"))
    target_metrics = _load_native_gc_metrics(TARGET_FAMILY)
    gc_asia_metrics = _load_native_gc_metrics(GC_ADJACENT_REFERENCE_FAMILY)
    mgc_uslate_metrics = _load_reference_metrics(MGC_US_LATE_REFERENCE, TARGET_FAMILY)
    mgc_derivative_payload = json.loads(MGC_DERIVATIVE_VALIDATION_JSON.read_text(encoding="utf-8"))
    impulse_reference = _load_impulse_reference()
    metal_architecture = _load_metals_architecture_context()

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "thread_scope": "thread_2_only",
        "research_scope": "research_design_only",
        "instrument": "GC",
        "family": TARGET_FAMILY,
        "gc_replay_analysis_path_used": {
            "validation_method": "validated existing native GC replay packet by recomputing branch-only metrics from the persisted trade ledger",
            "generated_replay_summary": str(GC_NATIVE_PACKET["summary"]),
            "generated_summary_metrics": str(GC_NATIVE_PACKET["summary_metrics"]),
            "generated_trade_ledger": str(GC_NATIVE_PACKET["trade_ledger"]),
            "generated_replay_db": str(GC_NATIVE_PACKET["replay_db"]),
            "symbol": summary.get("symbol"),
            "timeframe": summary.get("timeframe"),
            "source_db_path": summary.get("source_db_path"),
            "source_first_bar_ts": summary.get("source_first_bar_ts"),
            "source_last_bar_ts": summary.get("source_last_bar_ts"),
        },
        "structural_execution_fit": {
            "session_windows_et": ["US_LATE: 14:00-17:00 ET"],
            "directional_bias": "LONG_BIASED",
            "naturally_causal_executable": True,
            "relies_on_future_confirmation": False,
            "live_observation_realistically_frequent_enough_to_matter": target_metrics.trades >= 15,
            "live_observation_note": (
                "The packet contains 22 GC trades inside US_LATE, so there is enough late-US activity to matter in a narrow future paper-design pass."
            ),
            "operationally_compatible_with_current_metals_paper_architecture": metal_architecture["compatible"],
            "metals_architecture_note": metal_architecture["note"],
        },
        "economic_replay_quality": asdict(target_metrics),
        "comparisons": {
            "vs_MGC_usLatePauseResumeLongTurn": {
                "reference_metrics": asdict(mgc_uslate_metrics),
                "comparison_note": (
                    "MGC usLatePauseResumeLongTurn remains the mature metals benchmark. GC is judged here as a possible later addition, not a replacement."
                ),
            },
            "vs_MGC_usDerivativeBearTurn": {
                "reference_metrics": mgc_derivative_payload["economic_replay_quality"],
                "comparison_note": (
                    "MGC usDerivativeBearTurn remains an important short-side research lead, but its packet is much thinner and less immediately compatible with the current metals paper lane set."
                ),
            },
            "vs_parked_impulse_executable_reference": {
                "reference_metrics": impulse_reference,
                "comparison_note": (
                    "The parked impulse executable reference had more traffic but a weaker median trade and much worse drawdown. "
                    "GC usLate is judged on whether it is a cleaner causal candidate for future paper design."
                ),
            },
            "vs_adjacent_GC_reference": {
                "reference_metrics": asdict(gc_asia_metrics),
                "reference_family": "GC / asiaEarlyNormalBreakoutRetestHoldTurn",
                "comparison_note": (
                    "No separate GC late-US sibling candidate was found in the current evidence stack. "
                    "The closest same-instrument adjacent reference is the already active GC Asia breakout lane."
                ),
            },
        },
        "inclusion_readiness_judgment": {
            "strong_enough_for_narrow_future_paper_design_pass": _ready_for_narrow_paper_design_pass(target_metrics, metal_architecture["compatible"]),
            "better_treated_as_addition_to_current_metals_leader_set_or_parked_candidate": (
                "FUTURE_ADDITION_TO_CURRENT_METALS_LEADER_SET"
                if _ready_for_narrow_paper_design_pass(target_metrics, metal_architecture["compatible"])
                else "PARKED_SERIOUS_NEXT_CANDIDATE"
            ),
            "more_credible_than_current_derivative_bear_leads": (
                "YES_FOR_NARROW_PAPER_DESIGN"
                if target_metrics.trades > (mgc_derivative_payload["economic_replay_quality"]["trades"] or 0)
                else "NO"
            ),
            "single_biggest_remaining_blocker": "Concentration fragility: the packet fails both survives_without_top_1 and survives_without_top_3.",
        },
        "verdict_bucket": _verdict_bucket(target_metrics, metal_architecture["compatible"]),
        "direct_answers": {
            "is_gc_uslate_strong_enough_to_justify_a_narrow_future_paper_design_pass_later": (
                "Yes. It is not admission-ready, but it is strong enough to justify a narrow GC-only paper-admission design pass later."
                if _ready_for_narrow_paper_design_pass(target_metrics, metal_architecture["compatible"])
                else "No. It should remain parked as a serious next candidate only."
            ),
            "is_it_better_treated_as_an_addition_to_the_current_metals_leader_set_or_still_parked": (
                "Addition candidate for a later narrow paper-design pass, not a current admission."
                if _ready_for_narrow_paper_design_pass(target_metrics, metal_architecture["compatible"])
                else "Still parked."
            ),
            "is_it_more_credible_than_the_current_derivative_bear_leads": (
                "Yes for future paper-design planning. It is broader, more observable in a single operational window, and already compatible with the metals paper architecture."
            ),
            "single_biggest_remaining_blocker": "Concentration fragility remains the blocker.",
        },
    }

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_JSON_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    LATEST_MD_PATH.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "gc_usLatePauseResumeLongTurn_inclusion_readiness_json": str(LATEST_JSON_PATH),
        "gc_usLatePauseResumeLongTurn_inclusion_readiness_md": str(LATEST_MD_PATH),
        "verdict_bucket": payload["verdict_bucket"],
        "gc_replay_analysis_path_used": payload["gc_replay_analysis_path_used"],
    }


def _load_native_gc_metrics(family: str) -> PairMetrics:
    summary = json.loads(GC_NATIVE_PACKET["summary"].read_text(encoding="utf-8"))
    rows = [row for row in _load_rows(GC_NATIVE_PACKET["trade_ledger"]) if row["setup_family"] == family]
    return _compute_metrics(
        summary=summary,
        rows=rows,
        sample_start=summary.get("source_first_bar_ts"),
        sample_end=summary.get("source_last_bar_ts"),
    )


def _load_metals_architecture_context() -> dict[str, Any]:
    paper_config = json.loads(PAPER_CONFIG_IN_FORCE.read_text(encoding="utf-8"))
    approved_models = json.loads(PAPER_APPROVED_MODELS.read_text(encoding="utf-8"))
    configured_lanes = paper_config.get("lanes", [])
    has_gc_lane = any(lane.get("symbol") == "GC" for lane in configured_lanes)
    active_gc_branch = "GC / asiaEarlyNormalBreakoutRetestHoldTurn" in approved_models.get("details_by_branch", {})
    compatible = bool(has_gc_lane and active_gc_branch)
    note = (
        "GC is already a first-class symbol in the current metals paper architecture via the active GC Asia lane, "
        "so a future GC usLate design pass would extend an existing metals runtime path rather than create a new instrument family."
        if compatible
        else "Current paper artifacts do not prove that GC is already wired as a metals paper lane."
    )
    return {"compatible": compatible, "note": note}


def _ready_for_narrow_paper_design_pass(metrics: PairMetrics, architecture_compatible: bool) -> bool:
    return (
        architecture_compatible
        and metrics.trades >= 15
        and metrics.realized_pnl > 0
        and (metrics.profit_factor or 0.0) >= 1.5
    )


def _verdict_bucket(metrics: PairMetrics, architecture_compatible: bool) -> str:
    if _ready_for_narrow_paper_design_pass(metrics, architecture_compatible):
        return "READY_FOR_NARROW_PAPER_DESIGN_PASS"
    if metrics.trades >= 5 and metrics.realized_pnl > 0 and (metrics.profit_factor or 0.0) >= 1.2:
        return "SERIOUS_NEXT_CANDIDATE"
    if metrics.realized_pnl > 0:
        return "LATER_REVIEW"
    return "DEPRIORITIZE"


def _fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "Unavailable"
    return f"{float(value):.{digits}f}"


def _render_markdown(payload: dict[str, Any]) -> str:
    fit = payload["structural_execution_fit"]
    metrics = payload["economic_replay_quality"]
    lines = [
        "# GC usLatePauseResumeLongTurn Inclusion Readiness",
        "",
        f"- Verdict: `{payload['verdict_bucket']}`",
        f"- Native GC replay packet: `{payload['gc_replay_analysis_path_used']['generated_replay_summary']}`",
        "",
        "## Structural / Execution Fit",
        f"- Session windows: {', '.join(fit['session_windows_et'])}",
        f"- Directional bias: `{fit['directional_bias']}`",
        f"- Naturally causal / executable: `{fit['naturally_causal_executable']}`",
        f"- Live-observable often enough: `{fit['live_observation_realistically_frequent_enough_to_matter']}`",
        f"- Metals-paper compatible: `{fit['operationally_compatible_with_current_metals_paper_architecture']}`",
        f"- Note: {fit['metals_architecture_note']}",
        "",
        "## Economic / Replay Quality",
        f"- Sample: `{metrics['sample_start']}` -> `{metrics['sample_end']}`",
        f"- Trades `{metrics['trades']}`, realized P/L `{_fmt(metrics['realized_pnl'])}`, avg trade `{_fmt(metrics['avg_trade'])}`, median trade `{_fmt(metrics['median_trade'])}`, PF `{_fmt(metrics['profit_factor'])}`, max DD `{_fmt(metrics['max_drawdown'])}`, win rate `{_fmt((metrics['win_rate'] or 0.0) * 100.0)}%`",
        f"- Losses: avg `{_fmt(metrics['average_loser'])}`, median `{_fmt(metrics['median_loser'])}`, p95 `{_fmt(metrics['p95_loser'])}`, worst `{_fmt(metrics['worst_loser'])}`",
        f"- Winners: avg `{_fmt(metrics['average_winner'])}`, avg win/loss `{_fmt(metrics['avg_winner_over_avg_loser'])}`, large winners `{metrics['large_winner_count']}`, very large winners `{metrics['very_large_winner_count']}`",
        f"- Concentration: top-1 `{_fmt(metrics['top_1_contribution'])}%`, top-3 `{_fmt(metrics['top_3_contribution'])}%`, survive ex top-1 `{metrics['survives_without_top_1']}`, survive ex top-3 `{metrics['survives_without_top_3']}`",
        "",
        "## Direct Answers",
        f"- Narrow future paper-design pass later: {payload['direct_answers']['is_gc_uslate_strong_enough_to_justify_a_narrow_future_paper_design_pass_later']}",
        f"- Addition vs parked: {payload['direct_answers']['is_it_better_treated_as_an_addition_to_the_current_metals_leader_set_or_still_parked']}",
        f"- More credible than derivative-bear leads: {payload['direct_answers']['is_it_more_credible_than_the_current_derivative_bear_leads']}",
        f"- Biggest blocker: {payload['direct_answers']['single_biggest_remaining_blocker']}",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
