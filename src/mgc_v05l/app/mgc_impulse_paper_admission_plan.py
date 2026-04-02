"""Frozen paper-admission plan for the validated MGC impulse candidate."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .mgc_impulse_burst_continuation_research import OUTPUT_DIR


REPO_ROOT = Path(__file__).resolve().parents[3]
VALIDATION_ARTIFACT_PATH = OUTPUT_DIR / "mgc_impulse_confirmation_validation.json"
FAMILY_NAME = "impulse_burst_continuation"
POPULATION_VARIANT = "breadth_plus_agreement_combo"
CONFIRMATION_VARIANT = "minimal_post_trigger_confirmation_rule"
LANE_ID = "mgc_impulse_burst_continuation_min_confirm"
DISPLAY_NAME = "MGC / impulseBurstContinuationMinimalConfirm"
LONG_SOURCE_ID = "impulseBurstContinuationMinimalConfirmLong"
SHORT_SOURCE_ID = "impulseBurstContinuationMinimalConfirmShort"
SESSION_RESTRICTION = "ALL_SESSIONS"
POINT_VALUE = "10"
TEMPORARY_CATASTROPHIC_OPEN_LOSS = "-500"
MINIMUM_PAPER_OBSERVATION_FILLS = 25
MINIMUM_PAPER_OBSERVATION_DAYS = 10


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    _ = parser.parse_args(argv)
    payload = run_impulse_paper_admission_plan()
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog="mgc-impulse-paper-admission-plan")


def run_impulse_paper_admission_plan() -> dict[str, Any]:
    validation = _load_validation_payload()
    best_variant = _require_best_variant(validation)
    looser_variant = _require_variant(validation, "slightly_looser_confirmation")
    tighter_variant = _require_variant(validation, "slightly_tighter_confirmation")
    mild_retrace_variant = _require_variant(validation, "mild_retrace_sanity_check")

    payload = {
        "mode": "mgc_impulse_paper_admission_plan",
        "family_name": FAMILY_NAME,
        "validated_source_artifact": str(VALIDATION_ARTIFACT_PATH),
        "frozen_candidate_definition": _frozen_candidate_definition(
            validation=validation,
            best_variant=best_variant,
        ),
        "paper_admission_wiring_plan": _paper_admission_wiring_plan(),
        "paper_review_checklist": _paper_review_checklist(
            best_variant=best_variant,
            looser_variant=looser_variant,
            tighter_variant=tighter_variant,
            mild_retrace_variant=mild_retrace_variant,
        ),
        "paper_failure_and_rollback_criteria": _paper_failure_and_rollback_criteria(
            best_variant=best_variant,
            looser_variant=looser_variant,
        ),
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "mgc_impulse_paper_admission_plan.json"
    md_path = OUTPUT_DIR / "mgc_impulse_paper_admission_plan.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": payload["mode"],
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "lane_id": LANE_ID,
        "display_name": DISPLAY_NAME,
        "best_validated_variant": best_variant["variant_name"],
    }


def _load_validation_payload() -> dict[str, Any]:
    if not VALIDATION_ARTIFACT_PATH.exists():
        raise FileNotFoundError(
            "Validation artifact missing; run scripts/run_mgc_impulse_confirmation_validation.sh first."
        )
    return json.loads(VALIDATION_ARTIFACT_PATH.read_text(encoding="utf-8"))


def _require_best_variant(validation: dict[str, Any]) -> dict[str, Any]:
    best_name = str(validation["validation_conclusion"]["best_variant"])
    return _require_variant(validation, best_name)


def _require_variant(validation: dict[str, Any], variant_name: str) -> dict[str, Any]:
    for row in validation.get("variant_results", []):
        if row.get("variant_name") == variant_name:
            return row
    raise KeyError(f"Variant {variant_name} not found in validation artifact.")


def _frozen_candidate_definition(*, validation: dict[str, Any], best_variant: dict[str, Any]) -> dict[str, Any]:
    metrics = best_variant["metrics"]
    lane_definition = {
        "lane_id": LANE_ID,
        "display_name": DISPLAY_NAME,
        "symbol": "MGC",
        "family_name": FAMILY_NAME,
        "population_variant": POPULATION_VARIANT,
        "confirmation_variant": CONFIRMATION_VARIANT,
        "long_source_identifier": LONG_SOURCE_ID,
        "short_source_identifier": SHORT_SOURCE_ID,
    }
    return {
        "candidate_status": "FROZEN_FOR_PAPER_ADMISSION_PLANNING",
        "lane_definition": lane_definition,
        "population_definition": {
            "accepted_event_stream": "raw breadth_plus_agreement_combo population",
            "detection_surface": "1m",
            "context_surface": "5m",
            "window_size_bars": 8,
            "require_volume_expansion": False,
            "base_metrics_and_thresholds": {
                "normalized_move": ">= 1.35",
                "same_direction_share": ">= 0.75",
                "body_dominance": ">= 0.70",
                "path_efficiency": ">= 0.50",
                "largest_bar_share": "<= 0.55",
                "materially_contributing_bars": ">= 3 at material_bar_share_min 0.12",
            },
            "context_filter": {
                "recent_context_bars": 3,
                "normalized_context": ">= 0.35",
                "same_body_share": ">= 0.6667",
            },
        },
        "confirmation_rule": {
            "name": CONFIRMATION_VARIANT,
            "exact_rule": [
                "require new_extension_within_2_bars == true",
                "require confirmation_bar_count_first_3 >= 2",
            ],
            "metric_names_used": [
                "normalized_move",
                "same_direction_share",
                "body_dominance",
                "path_efficiency",
                "largest_bar_share",
                "materially_contributing_bars",
                "new_extension_within_2_bars",
                "confirmation_bar_count_first_3",
                "normalized_context",
                "same_body_share",
            ],
        },
        "session_behavior": {
            "research_rule": "No session gate. Session pockets are descriptive only.",
            "paper_admission_requirement": (
                "Current paper framework requires explicit session_restriction per lane. "
                "Implementation plan uses ALL_SESSIONS as an explicit pass-through marker."
            ),
            "descriptive_pocket_results": best_variant.get("pocket_descriptives", []),
        },
        "explicitly_not_part_of_the_rule": [
            "No anti-late-chase suppression gate in the frozen admitted candidate.",
            "No retrace sanity check in the frozen admitted candidate.",
            "No session-pocket gating in the frozen admitted candidate.",
            "No stop-overlay redesign or fast-failure overlay in the frozen admitted candidate.",
            "No MNQ transfer logic.",
            "No Monte Carlo or bootstrap logic.",
        ],
        "validated_research_profile": {
            "decision_bucket": best_variant["decision_bucket"],
            "sample_start_date": validation["sample_start_date"],
            "sample_end_date": validation["sample_end_date"],
            "history_window_type": validation["history_window_type"],
            "trades": metrics["trades"],
            "realized_pnl": metrics["realized_pnl"],
            "avg_trade": metrics["avg_trade"],
            "median_trade": metrics["median_trade"],
            "profit_factor": metrics["profit_factor"],
            "max_drawdown": metrics["max_drawdown"],
            "win_rate": metrics["win_rate"],
            "average_loser": metrics["average_loser"],
            "median_loser": metrics["median_loser"],
            "p95_loser": metrics["p95_loser"],
            "worst_loser": metrics["worst_loser"],
            "average_winner": metrics["average_winner"],
            "avg_winner_over_avg_loser": metrics["avg_winner_over_avg_loser"],
            "top_1_contribution": metrics["top_1_contribution"],
            "top_3_contribution": metrics["top_3_contribution"],
            "survives_without_top_1": metrics["survives_without_top_1"],
            "survives_without_top_3": metrics["survives_without_top_3"],
            "large_winner_count": metrics["large_winner_count"],
            "very_large_winner_count": metrics["very_large_winner_count"],
        },
    }


def _paper_admission_wiring_plan() -> dict[str, Any]:
    lane_entry = {
        "lane_id": LANE_ID,
        "display_name": DISPLAY_NAME,
        "symbol": "MGC",
        "long_sources": [LONG_SOURCE_ID],
        "short_sources": [SHORT_SOURCE_ID],
        "session_restriction": SESSION_RESTRICTION,
        "point_value": POINT_VALUE,
        "catastrophic_open_loss": TEMPORARY_CATASTROPHIC_OPEN_LOSS,
    }
    return {
        "implementation_scope": "paper-only admission wiring; no live-routing changes",
        "exact_files_to_change": [
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/signals/entry_resolver.py"),
                "purpose": "Emit the new side-specific source identifiers when the frozen impulse rule is true.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/strategy/strategy_engine.py"),
                "purpose": "Evaluate the frozen impulse family and treat ALL_SESSIONS as an explicit pass-through paper restriction.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/config_models/settings.py"),
                "purpose": "Add enable_impulse_burst_continuation_longs/shorts config flags and expose the new approved sources.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/probationary_runtime.py"),
                "purpose": "Map the new long/short source identifiers into APPROVED_LONG_SOURCE_FIELDS and APPROVED_SHORT_SOURCE_FIELDS.",
            },
            {
                "path": str(REPO_ROOT / "config/probationary_pattern_engine_paper.yaml"),
                "purpose": "Append the frozen MGC impulse lane entry to probationary_paper_lanes_json.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/operator_dashboard.py"),
                "purpose": "Ensure promoted paper model source metadata includes the new impulse source identifiers so lane activity and approved-model surfaces remain truthful.",
            },
            {
                "path": str(REPO_ROOT / "tests/unit/test_mgc_v05l_signals.py"),
                "purpose": "Prove the frozen impulse rule emits only the new side-specific source identifiers and remains isolated from existing families.",
            },
            {
                "path": str(REPO_ROOT / "tests/unit/test_mgc_v05l_probationary_runtime.py"),
                "purpose": "Prove the new lane is admitted in paper, uses ALL_SESSIONS, and stays isolated from the existing MGC/PL/GC lanes.",
            },
            {
                "path": str(REPO_ROOT / "tests/unit/test_mgc_v05l_operator_dashboard.py"),
                "purpose": "Prove the admitted lane renders as a distinct paper lane in readiness, approved-model, activity, and session-close views.",
            },
        ],
        "exact_config_entries_to_add": {
            "settings_keys": [
                "enable_impulse_burst_continuation_longs",
                "enable_impulse_burst_continuation_shorts",
            ],
            "paper_lane_entry": lane_entry,
            "derived_lane_database_url": f"sqlite:///./mgc_v05l.probationary.paper__{LANE_ID}.sqlite3",
        },
        "exact_dashboard_operator_surfaces_expected_to_reflect_the_lane": [
            "Paper Mode Status / Readiness",
            "Approved Models in Paper",
            "Live Lane Activity / Evidence",
            "Multi-Lane Session Review",
        ],
        "exact_paper_artifacts_expected_for_the_lane": [
            "./outputs/probationary_pattern_engine/paper_session/operator_status.json",
            "./outputs/probationary_pattern_engine/paper_session/runtime/paper_config_in_force.json",
            "./outputs/probationary_pattern_engine/paper_session/runtime/paper_desk_risk_status.json",
            "./outputs/probationary_pattern_engine/paper_session/runtime/paper_lane_risk_status.json",
            "./outputs/probationary_pattern_engine/paper_session/risk_trigger_events.jsonl",
            f"./outputs/probationary_pattern_engine/paper_session/lanes/{LANE_ID}/operator_status.json",
            f"./outputs/probationary_pattern_engine/paper_session/lanes/{LANE_ID}/branch_sources.jsonl",
            f"./outputs/probationary_pattern_engine/paper_session/lanes/{LANE_ID}/rule_blocks.jsonl",
            f"./outputs/probationary_pattern_engine/paper_session/lanes/{LANE_ID}/reconciliation_events.jsonl",
            "./outputs/operator_dashboard/paper_readiness_snapshot.json",
            "./outputs/operator_dashboard/paper_approved_models_snapshot.json",
            "./outputs/operator_dashboard/paper_lane_activity_snapshot.json",
            "./outputs/operator_dashboard/paper_session_close_review_latest.json",
        ],
        "exact_tests_that_should_pass_after_wiring": [
            "Frozen impulse long source resolves only to impulseBurstContinuationMinimalConfirmLong when the long rule is satisfied.",
            "Frozen impulse short source resolves only to impulseBurstContinuationMinimalConfirmShort when the short rule is satisfied.",
            "Paper lane spec load contains mgc_impulse_burst_continuation_min_confirm and keeps all currently admitted lanes.",
            "The new lane advertises session_restriction == ALL_SESSIONS and catastrophic_open_loss == -500.",
            "Dashboard admitted-lane views show MGC / impulseBurstContinuationMinimalConfirm as a distinct row rather than collapsing into existing MGC families.",
            "Lane activity, session-close, and risk snapshots keep the impulse lane isolated from the three existing MGC lanes.",
        ],
    }


def _paper_review_checklist(
    *,
    best_variant: dict[str, Any],
    looser_variant: dict[str, Any],
    tighter_variant: dict[str, Any],
    mild_retrace_variant: dict[str, Any],
) -> dict[str, Any]:
    best_metrics = best_variant["metrics"]
    looser_metrics = looser_variant["metrics"]
    tighter_metrics = tighter_variant["metrics"]
    mild_metrics = mild_retrace_variant["metrics"]
    return {
        "paper_review_goal": "Confirm that paper behavior matches the validated research shape without relying on session gates or family redesign.",
        "minimum_observation_requirement": {
            "fills": MINIMUM_PAPER_OBSERVATION_FILLS,
            "active_days": MINIMUM_PAPER_OBSERVATION_DAYS,
        },
        "checklist": [
            {
                "topic": "signal_count",
                "review": "Lane should emit real signals consistently enough to avoid a dead-on-arrival paper lane.",
                "research_reference": "115 validated fills over the research window; use rate-normalized comparison rather than exact day matching.",
            },
            {
                "topic": "intents_and_fills",
                "review": "Track signal -> intent -> fill chain and confirm paper mechanics are not starving the family.",
                "research_reference": "The validated family depended on confirmation quality, not on sparse signal count; paper should not collapse to thin fill conversion.",
            },
            {
                "topic": "realized_pnl_quality",
                "review": "Focus on avg trade, median trade, PF, and top-3 concentration rather than raw P/L alone.",
                "research_reference": {
                    "avg_trade": best_metrics["avg_trade"],
                    "median_trade": best_metrics["median_trade"],
                    "profit_factor": best_metrics["profit_factor"],
                    "top_3_contribution": best_metrics["top_3_contribution"],
                },
            },
            {
                "topic": "unrealized_open_risk_at_close",
                "review": "Review whether the lane tends to end flat/clean or leaves unresolved open exposure into session close artifacts.",
                "research_reference": "No session gate was part of the rule, so clean-close behavior must be judged from actual paper evidence.",
            },
            {
                "topic": "clean_vs_dirty_closes",
                "review": "Use session-close review artifacts to confirm the lane is not repeatedly ending with dirty reconciliation or carryover ambiguity.",
                "research_reference": "The candidate is only admission-worthy if the edge survives clean paper mechanics.",
            },
            {
                "topic": "block_frequency",
                "review": "Track operator/risk/rule blocks and ensure the lane is not mostly blocked rather than actually trading.",
                "research_reference": "This family had no session gate and no anti-late-chase gate in its frozen form; excessive block rates are a paper implementation smell.",
            },
            {
                "topic": "loss_containment",
                "review": "Review avg loser, median loser, p95 loser, and worst loser against the validated envelope.",
                "research_reference": {
                    "average_loser": best_metrics["average_loser"],
                    "median_loser": best_metrics["median_loser"],
                    "p95_loser": best_metrics["p95_loser"],
                    "worst_loser": best_metrics["worst_loser"],
                },
            },
            {
                "topic": "research_shape_match",
                "review": "Paper should stay closer to the validated control and mild-retrace neighbor than to the looser noisy neighbor.",
                "research_reference": {
                    "validated_control_profit_factor": best_metrics["profit_factor"],
                    "mild_retrace_neighbor_profit_factor": mild_metrics["profit_factor"],
                    "looser_neighbor_profit_factor": looser_metrics["profit_factor"],
                    "tighter_neighbor_trade_count": tighter_metrics["trades"],
                },
            },
        ],
    }


def _paper_failure_and_rollback_criteria(
    *,
    best_variant: dict[str, Any],
    looser_variant: dict[str, Any],
) -> dict[str, Any]:
    best_metrics = best_variant["metrics"]
    looser_metrics = looser_variant["metrics"]
    return {
        "philosophy": "Rollback if paper mechanics destroy the validated loss quality, concentration sanity, or activity profile.",
        "failure_criteria": [
            {
                "criterion": "materially_worse_fill_behavior",
                "trigger": (
                    "After the minimum observation window, signal activity appears but fill realization is materially below research-normalized expectation "
                    "or unresolved intents dominate the lane evidence."
                ),
            },
            {
                "criterion": "much_worse_loser_distribution",
                "trigger": {
                    "average_loser_floor": round(float(best_metrics["average_loser"]) * 1.5, 4),
                    "p95_loser_floor": round(float(best_metrics["p95_loser"]) * 1.5, 4),
                    "worst_loser_floor": round(float(best_metrics["worst_loser"]) * 1.5, 4),
                },
            },
            {
                "criterion": "research_edge_does_not_survive_paper_mechanics",
                "trigger": {
                    "profit_factor_floor": max(1.3, float(looser_metrics["profit_factor"])),
                    "median_trade_must_remain_positive": True,
                    "top_3_contribution_ceiling": 60.0,
                    "survives_without_top_3_required": True,
                },
            },
            {
                "criterion": "dirty_close_or_open_risk_problems",
                "trigger": (
                    "Repeated dirty closes, repeated ambiguous open-risk ownership, or lane-specific close-review defects that make the session-close bundle untrustworthy."
                ),
            },
            {
                "criterion": "activity_far_below_expectation",
                "trigger": (
                    "Sustained fill pace below roughly 40% of the research-normalized pace after the minimum observation window, unless the lane is visibly rule-blocked for a separate known reason."
                ),
            },
        ],
        "rollback_action": "Remove the lane from probationary_paper_lanes_json, preserve archived paper evidence, and return the family to research-only status.",
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    frozen = payload["frozen_candidate_definition"]
    wiring = payload["paper_admission_wiring_plan"]
    checklist = payload["paper_review_checklist"]
    rollback = payload["paper_failure_and_rollback_criteria"]
    profile = frozen["validated_research_profile"]
    lines = [
        "# MGC Impulse Paper Admission Plan",
        "",
        "## Frozen Candidate",
        "",
        f"- Lane: `{frozen['lane_definition']['display_name']}` (`{frozen['lane_definition']['lane_id']}`)",
        f"- Family: `{frozen['lane_definition']['family_name']}`",
        f"- Population: `{frozen['population_definition']['accepted_event_stream']}`",
        f"- Confirmation rule: `{CONFIRMATION_VARIANT}`",
        f"- Research verdict: `{profile['decision_bucket']}`",
        f"- Sample: `{profile['sample_start_date'] if 'sample_start_date' in profile else payload['frozen_candidate_definition']['validated_research_profile'].get('sample_start_date', '')}`",
        f"- Validated metrics: trades `{profile['trades']}`, pnl `{profile['realized_pnl']}`, PF `{profile['profit_factor']}`, median `{profile['median_trade']}`, top-3 `{profile['top_3_contribution']}`",
        "",
        "### Exact Rule",
        "",
        "- Base population thresholds:",
        f"  - `normalized_move >= 1.35`",
        f"  - `same_direction_share >= 0.75`",
        f"  - `body_dominance >= 0.70`",
        f"  - `path_efficiency >= 0.50`",
        f"  - `largest_bar_share <= 0.55`",
        f"  - `materially_contributing_bars >= 3` at `material_bar_share_min 0.12`",
        "- Context filter:",
        "  - last 3 completed 5m bars",
        "  - `normalized_context >= 0.35`",
        "  - `same_body_share >= 0.6667`",
        "- Confirmation:",
        "  - `new_extension_within_2_bars == true`",
        "  - `confirmation_bar_count_first_3 >= 2`",
        "",
        "### Not In Rule",
        "",
    ]
    for row in frozen["explicitly_not_part_of_the_rule"]:
        lines.append(f"- {row}")
    lines.extend(
        [
            "",
            "## Paper Wiring Plan",
            "",
            f"- Lane config entry: `{json.dumps(wiring['exact_config_entries_to_add']['paper_lane_entry'], sort_keys=True)}`",
            f"- Derived lane DB: `{wiring['exact_config_entries_to_add']['derived_lane_database_url']}`",
            "- Files to change:",
        ]
    )
    for row in wiring["exact_files_to_change"]:
        lines.append(f"  - `{row['path']}`: {row['purpose']}")
    lines.extend(["", "## Paper Review Checklist", ""])
    for row in checklist["checklist"]:
        lines.append(f"- `{row['topic']}`: {row['review']}")
    lines.extend(["", "## Rollback Criteria", ""])
    for row in rollback["failure_criteria"]:
        lines.append(f"- `{row['criterion']}`: {row['trigger']}")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
