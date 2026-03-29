"""Future paper-design plan for GC / usLatePauseResumeLongTurn."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "approved_branch_research"

GC_INCLUSION_READINESS_JSON = REPORT_DIR / "gc_usLatePauseResumeLongTurn_inclusion_readiness.json"
GC_INCLUSION_READINESS_MD = REPORT_DIR / "gc_usLatePauseResumeLongTurn_inclusion_readiness.md"
CROSS_METAL_ANATOMY_JSON = REPORT_DIR / "usLatePauseResumeLongTurn_cross_metal_anatomy.json"
SHARED_REFINEMENT_JSON = REPORT_DIR / "usLatePauseResumeLongTurn_shared_metals_refinement.json"
PAPER_CONFIG_IN_FORCE = REPO_ROOT / "outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json"
PAPER_APPROVED_MODELS = REPO_ROOT / "outputs/operator_dashboard/paper_approved_models_snapshot.json"
PAPER_READINESS = REPO_ROOT / "outputs/operator_dashboard/paper_readiness_snapshot.json"
PAPER_CONFIG_YAML = REPO_ROOT / "config" / "probationary_pattern_engine_paper.yaml"

OUTPUT_JSON = REPORT_DIR / "gc_usLatePauseResumeLongTurn_paper_design_plan.json"
OUTPUT_MD = REPORT_DIR / "gc_usLatePauseResumeLongTurn_paper_design_plan.md"

LANE_ID = "gc_us_late_pause_resume_long"
DISPLAY_NAME = "GC / usLatePauseResumeLongTurn"
SOURCE_FAMILY = "usLatePauseResumeLongTurn"
SESSION_RESTRICTION = "US_LATE"
SYMBOL = "GC"
POINT_VALUE = "100"
FUTURE_CATASTROPHIC_OPEN_LOSS = "-750"
FUTURE_WARNING_OPEN_LOSS = "-500"


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog="gc-uslate-pause-resume-long-paper-design-plan")


def main(argv: list[str] | None = None) -> int:
    _ = build_parser().parse_args(argv)
    payload = build_and_write_gc_uslate_paper_design_plan()
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_and_write_gc_uslate_paper_design_plan() -> dict[str, Any]:
    inclusion = _load_json(GC_INCLUSION_READINESS_JSON)
    anatomy = _load_json(CROSS_METAL_ANATOMY_JSON)
    refinement = _load_json(SHARED_REFINEMENT_JSON)
    paper_config = _load_json(PAPER_CONFIG_IN_FORCE)
    approved_models = _load_json(PAPER_APPROVED_MODELS)
    readiness = _load_json(PAPER_READINESS)

    gc_metrics = inclusion["economic_replay_quality"]
    gc_anatomy = anatomy["metals"]["GC"]
    gc_current_lane = _require_gc_asia_lane(paper_config["lanes"])

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "thread_scope": "thread_2_only",
        "mode": "future_gc_paper_design_planning_only",
        "instrument": SYMBOL,
        "family": SOURCE_FAMILY,
        "exact_evidence_sources_used": {
            "gc_inclusion_readiness_json": str(GC_INCLUSION_READINESS_JSON),
            "gc_inclusion_readiness_md": str(GC_INCLUSION_READINESS_MD),
            "cross_metal_anatomy_json": str(CROSS_METAL_ANATOMY_JSON),
            "shared_metals_refinement_json": str(SHARED_REFINEMENT_JSON),
            "paper_config_in_force_json": str(PAPER_CONFIG_IN_FORCE),
            "paper_approved_models_snapshot_json": str(PAPER_APPROVED_MODELS),
            "paper_readiness_snapshot_json": str(PAPER_READINESS),
            "paper_config_yaml": str(PAPER_CONFIG_YAML),
            "gc_native_replay_summary": inclusion["gc_replay_analysis_path_used"]["generated_replay_summary"],
            "gc_native_trade_ledger": inclusion["gc_replay_analysis_path_used"]["generated_trade_ledger"],
        },
        "admission_design_shape": {
            "future_lane_identity": {
                "lane_id": LANE_ID,
                "display_name": DISPLAY_NAME,
                "symbol": SYMBOL,
                "long_sources": [SOURCE_FAMILY],
                "short_sources": [],
                "session_restriction": SESSION_RESTRICTION,
                "point_value": POINT_VALUE,
                "proposed_catastrophic_open_loss": FUTURE_CATASTROPHIC_OPEN_LOSS,
                "proposed_warning_open_loss": FUTURE_WARNING_OPEN_LOSS,
                "expected_database_url": f"sqlite:///mgc_v05l.probationary.paper__{LANE_ID}.sqlite3",
                "expected_artifacts_dir": f"outputs/probationary_pattern_engine/paper_session/lanes/{LANE_ID}",
            },
            "session_windows_et": ["US_LATE: 14:00-17:00 ET"],
            "source_mapping": {
                "family_source": SOURCE_FAMILY,
                "mapping_note": (
                    "The late-US GC lane would use the existing long-side source family "
                    "`usLatePauseResumeLongTurn` with symbol-level lane isolation, not a new strategy id."
                ),
            },
            "architecture_fit": {
                "fits_current_metals_multi_lane_paper_architecture": True,
                "current_gc_lane_proof": {
                    "existing_gc_lane_id": gc_current_lane["lane_id"],
                    "existing_gc_display_name": gc_current_lane["display_name"],
                    "existing_gc_session_restriction": gc_current_lane["session_restriction"],
                },
                "dashboard_operator_fit_note": (
                    "GC already appears as its own branch row in current approved-model and readiness surfaces, "
                    "so a later GC usLate lane would extend an existing instrument truth chain rather than require dashboard redesign."
                ),
                "future_change_shape": "config_only_likely_sufficient",
                "why_config_only_is_likely_sufficient": [
                    "entry_resolver already emits `usLatePauseResumeLongTurn` as a long source family.",
                    "settings already allow `usLatePauseResumeLongTurn` in approved paper lane sources.",
                    "probationary_runtime already maps `usLatePauseResumeLongTurn` to `enable_us_late_pause_resume_longs`.",
                    "GC is already present as an admitted paper symbol via the current GC Asia lane.",
                ],
                "likely_future_config_only_work": [
                    "append the future GC usLate lane entry to `probationary_paper_lanes_json`",
                    "add a GC late-lane warning entry to `probationary_paper_lane_warning_open_loss_json`",
                    "refresh paper config snapshots and operator surfaces that derive from config-in-force",
                    "add tests proving the lane appears as a distinct GC late-US row beside the existing GC Asia row",
                ],
            },
        },
        "operational_prerequisites": {
            "minimum_evidence_expectations": {
                "replay_trade_count_floor": 20,
                "replay_profit_factor_floor": 1.5,
                "replay_realized_pnl_must_remain_positive": True,
                "reason": (
                    "GC already clears basic breadth and economic thresholds, so the next gate is not mere existence; "
                    "it is whether refreshed evidence remains broad enough and positive enough to justify operational admission work."
                ),
            },
            "concentration_sanity_requirements_before_future_admission_attempt": {
                "must_not_admit_until_any_one_of_these_is_true": [
                    "survives_without_top_1 becomes true in refreshed validation evidence",
                    "top_1_contribution falls to <= 100.0 and top_3_contribution falls to <= 130.0 in refreshed validation evidence",
                ],
                "current_blocking_state": {
                    "top_1_contribution": gc_metrics["top_1_contribution"],
                    "top_3_contribution": gc_metrics["top_3_contribution"],
                    "survives_without_top_1": gc_metrics["survives_without_top_1"],
                    "survives_without_top_3": gc_metrics["survives_without_top_3"],
                },
            },
            "required_live_observation_confidence": [
                "late-US GC signal cadence remains observable enough that the lane would not be a mostly idle branch",
                "observed live-hours behavior still looks like the replay family shape rather than sporadic one-off trend days only",
                "operators can distinguish GC late-US activity cleanly from the existing GC Asia branch without ambiguity",
            ],
            "required_artifact_monitoring_confidence": [
                "lane-specific branch source rows, fills, and session-close review entries remain isolated from the existing GC Asia lane",
                "approved-model and lane-risk surfaces can show a second GC row without attribution leakage",
                "risk/open-loss monitoring remains interpretable when GC has one Asia lane and one US_LATE lane active in the same paper stack",
            ],
            "explicit_do_not_admit_until_conditions": [
                "do not admit until concentration fragility improves or is at least bounded by refreshed validation evidence",
                "do not admit until the GC late-US lane can be monitored as a distinct branch with no cross-lane attribution confusion",
                "do not admit until replay/live-observation confidence says the lane is more than a few standout trend days",
            ],
        },
        "design_risks": [
            {
                "risk": "concentration_fragility",
                "detail": (
                    "GC currently fails both survives_without_top_1 and survives_without_top_3, so admitting too early risks "
                    "treating a thin standout-winner profile as a durable lane."
                ),
            },
            {
                "risk": "weak_middle_of_distribution",
                "detail": (
                    "The middle of the GC trade distribution is weak enough that ordinary trades do not absorb the dependence on standout days."
                ),
                "current_evidence": {
                    "middle_pnl_ex_top3": gc_anatomy["anatomy_buckets"]["mediocre_trades"]["realized_pnl"],
                    "concentration_plain_language": gc_anatomy["concentration_anatomy"]["plain_language"],
                },
            },
            {
                "risk": "poison_loser_sensitivity",
                "detail": (
                    "GC has a harsher poison-loser pocket than MGC, especially when early adverse movement appears before continuation fully develops."
                ),
                "current_evidence": {
                    "fragile_loser_realized_pnl": gc_anatomy["anatomy_buckets"]["fragile_losers"]["realized_pnl"],
                    "fragile_loser_mean_initial_adverse_3bar": gc_anatomy["anatomy_buckets"]["fragile_losers"]["mean_initial_adverse_3bar"],
                    "fragile_loser_mean_entry_efficiency_5": gc_anatomy["anatomy_buckets"]["fragile_losers"]["mean_entry_efficiency_5"],
                },
            },
            {
                "risk": "same_symbol_multi_lane_operator_confusion",
                "detail": (
                    "A later GC late-US lane would sit beside the existing GC Asia breakout lane, so branch naming, lane ids, and evidence isolation must remain explicit."
                ),
            },
            {
                "risk": "lane_interpretation_ambiguity",
                "detail": (
                    "If GC late-US underperforms early in paper, operators need to know whether the issue is concentration fragility, poor fills, or lane-mixing rather than misreading a single noisy session."
                ),
            },
        ],
        "monitoring_validation_plan": {
            "first_metrics_to_watch_if_later_admitted": [
                "raw signal count in US_LATE only",
                "intent and fill cadence relative to replay expectations",
                "realized P/L, median trade, and profit factor",
                "warning-open-loss and catastrophic-open-loss events",
                "concentration behavior in paper: whether top winners dominate immediately",
                "clean vs dirty close-review behavior and unresolved-intent counts",
            ],
            "lane_specific_truth_chain_to_watch": [
                "paper_readiness_snapshot.json lane-risk row for the future GC late lane",
                "paper_approved_models_snapshot.json branch row for GC / usLatePauseResumeLongTurn",
                "paper_lane_activity_snapshot.json branch-specific activity trail",
                "paper_session_close_review_latest.json lane-specific close-review evidence",
                f"outputs/probationary_pattern_engine/paper_session/lanes/{LANE_ID}/branch_sources.jsonl",
                f"outputs/probationary_pattern_engine/paper_session/lanes/{LANE_ID}/rule_blocks.jsonl",
                f"outputs/probationary_pattern_engine/paper_session/lanes/{LANE_ID}/reconciliation_events.jsonl",
            ],
            "validation_questions": [
                "does real paper behavior still resemble a late-US resumed continuation lane rather than isolated trend bursts",
                "does GC late-US remain interpretable next to the existing GC Asia lane",
                "does concentration improve, stay bounded, or worsen in live paper observation",
                "does the lane produce enough observable late-US behavior to justify continued paper residency",
            ],
        },
        "decision_framework": {
            "decision_bucket": "HOLD_AS_NEXT_ADDITION_CANDIDATE",
            "why": (
                "GC / usLatePauseResumeLongTurn is the right next metals addition candidate to hold in reserve, "
                "but concentration fragility still makes immediate admission planning too aggressive."
            ),
        },
        "direct_answers": {
            "is_gc_uslate_the_right_next_metals_addition_candidate": (
                "Yes. It is the cleanest future second-lane addition candidate in the current metals stack, after the existing MGC/PL/GC Asia lanes."
            ),
            "what_must_be_proven_before_it_should_be_admitted": (
                "It still needs concentration sanity, stronger confidence that the weak middle is not overwhelming the edge, "
                "and clear lane-isolated monitoring confidence alongside the existing GC Asia lane."
            ),
            "can_it_likely_be_added_later_with_config_design_work_only_or_would_runtime_changes_be_needed": (
                "Likely config/design work only. Current evidence says the source family, symbol plumbing, runtime approval path, and dashboard truth chain already exist."
            ),
            "single_biggest_reason_to_wait": "Concentration fragility remains the single biggest reason to wait.",
        },
        "current_reference_state": {
            "gc_economic_replay_quality": gc_metrics,
            "current_approved_models_total": readiness["approved_models_total"],
            "current_approved_gc_branch_names": sorted(
                branch for branch in approved_models["details_by_branch"] if branch.startswith("GC / ")
            ),
            "shared_refinement_result": refinement["verdict_bucket"],
            "cross_metal_verdict": anatomy["verdict_bucket"],
        },
    }

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    OUTPUT_MD.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "json_artifact": str(OUTPUT_JSON),
        "markdown_artifact": str(OUTPUT_MD),
        "decision_bucket": payload["decision_framework"]["decision_bucket"],
        "future_lane_id": LANE_ID,
        "display_name": DISPLAY_NAME,
    }


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _require_gc_asia_lane(lanes: list[dict[str, Any]]) -> dict[str, Any]:
    for lane in lanes:
        if lane.get("lane_id") == "gc_asia_early_normal_breakout_retest_hold_long":
            return lane
    raise KeyError("Current GC Asia lane not found in paper config in force.")


def _fmt_num(value: Any, digits: int = 2) -> str:
    if value is None:
        return "Unavailable"
    return f"{float(value):.{digits}f}"


def _render_markdown(payload: dict[str, Any]) -> str:
    shape = payload["admission_design_shape"]
    lane = shape["future_lane_identity"]
    metrics = payload["current_reference_state"]["gc_economic_replay_quality"]
    lines = [
        "# GC usLatePauseResumeLongTurn Future Paper Design Plan",
        "",
        f"- Decision bucket: `{payload['decision_framework']['decision_bucket']}`",
        f"- Planning scope: `{payload['mode']}`",
        f"- Future lane id: `{lane['lane_id']}`",
        f"- Display name: `{lane['display_name']}`",
        "",
        "## Admission Design Shape",
        f"- Session windows: {', '.join(shape['session_windows_et'])}",
        f"- Symbol / source mapping: `{lane['symbol']}` via `{lane['long_sources'][0]}` long source only",
        f"- Future session restriction: `{lane['session_restriction']}`",
        f"- Future point value: `{lane['point_value']}`",
        f"- Proposed catastrophic open loss: `{lane['proposed_catastrophic_open_loss']}`",
        f"- Proposed warning open loss: `{lane['proposed_warning_open_loss']}`",
        f"- Current architecture fit: `{shape['architecture_fit']['fits_current_metals_multi_lane_paper_architecture']}`",
        f"- Future change shape: `{shape['architecture_fit']['future_change_shape']}`",
        "",
        "## Current GC Reference",
        f"- Sample: `{metrics['sample_start']}` -> `{metrics['sample_end']}`",
        f"- Trades `{metrics['trades']}`, realized P/L `{_fmt_num(metrics['realized_pnl'])}`, avg trade `{_fmt_num(metrics['avg_trade'])}`, median trade `{_fmt_num(metrics['median_trade'])}`, PF `{_fmt_num(metrics['profit_factor'])}`, max DD `{_fmt_num(metrics['max_drawdown'])}`",
        f"- Losses: avg `{_fmt_num(metrics['average_loser'])}`, median `{_fmt_num(metrics['median_loser'])}`, p95 `{_fmt_num(metrics['p95_loser'])}`, worst `{_fmt_num(metrics['worst_loser'])}`",
        f"- Winners: avg `{_fmt_num(metrics['average_winner'])}`, avg win/loss `{_fmt_num(metrics['avg_winner_over_avg_loser'])}`",
        f"- Concentration: top-1 `{_fmt_num(metrics['top_1_contribution'])}%`, top-3 `{_fmt_num(metrics['top_3_contribution'])}%`, survive ex top-1 `{metrics['survives_without_top_1']}`, survive ex top-3 `{metrics['survives_without_top_3']}`",
        "",
        "## Prerequisites Before Any Later Admission Attempt",
        f"- Minimum evidence floor: >= `{payload['operational_prerequisites']['minimum_evidence_expectations']['replay_trade_count_floor']}` trades and PF >= `{payload['operational_prerequisites']['minimum_evidence_expectations']['replay_profit_factor_floor']}` in refreshed validation",
        "- Do not admit until concentration sanity improves or is at least bounded by refreshed evidence.",
        "- Do not admit until operators can monitor GC late-US cleanly beside the existing GC Asia lane with no attribution ambiguity.",
        "",
        "## Direct Answers",
        f"- Right next metals addition candidate: {payload['direct_answers']['is_gc_uslate_the_right_next_metals_addition_candidate']}",
        f"- What must still be proven: {payload['direct_answers']['what_must_be_proven_before_it_should_be_admitted']}",
        f"- Runtime change need: {payload['direct_answers']['can_it_likely_be_added_later_with_config_design_work_only_or_would_runtime_changes_be_needed']}",
        f"- Biggest reason to wait: {payload['direct_answers']['single_biggest_reason_to_wait']}",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
