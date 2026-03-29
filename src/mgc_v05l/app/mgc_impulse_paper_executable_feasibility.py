"""Executable-feasibility audit for the frozen MGC impulse paper candidate."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .mgc_impulse_burst_continuation_research import OUTPUT_DIR


REPO_ROOT = Path(__file__).resolve().parents[3]
VERDICT = "NOT_EXECUTABLE_AS_FROZEN"
FAMILY_NAME = "impulse_burst_continuation"
LANE_ID = "mgc_impulse_burst_continuation_min_confirm"
DISPLAY_NAME = "MGC / impulseBurstContinuationMinimalConfirm"


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    _ = parser.parse_args(argv)
    payload = run_impulse_paper_executable_feasibility()
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog="mgc-impulse-paper-executable-feasibility")


def run_impulse_paper_executable_feasibility() -> dict[str, Any]:
    payload = {
        "mode": "mgc_impulse_paper_executable_feasibility",
        "family_name": FAMILY_NAME,
        "lane_id": LANE_ID,
        "display_name": DISPLAY_NAME,
        "top_line_verdict": VERDICT,
        "frozen_candidate": {
            "population": "raw breadth_plus_agreement_combo",
            "confirmation": "minimal_post_trigger_confirmation_rule",
            "rule": [
                "require new_extension_within_2_bars",
                "require confirmation_bar_count_first_3 >= 2",
            ],
        },
        "multi_timeframe_feasibility": _multi_timeframe_feasibility(),
        "causality_no_lookahead": _causality_audit(),
        "lane_isolation_proof": _lane_isolation_proof(),
        "minimal_implementation_delta": _minimal_delta(),
        "code_paths_inspected": _code_paths_inspected(),
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "mgc_impulse_paper_executable_feasibility.json"
    md_path = OUTPUT_DIR / "mgc_impulse_paper_executable_feasibility.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": payload["mode"],
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "top_line_verdict": payload["top_line_verdict"],
    }


def _multi_timeframe_feasibility() -> dict[str, Any]:
    return {
        "question": "Can the current paper runtime evaluate 1m detection plus 5m context truthfully and causally for this lane?",
        "answer": "NO",
        "reason": (
            "Current paper lanes poll and process exactly one internal timeframe per lane, and the strategy engine only evaluates signals on the single finalized bar passed into process_bar."
        ),
        "exact_blockers": [
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/probationary_runtime.py"),
                "reference": "ProbationaryPaperLaneRuntime.poll_and_process",
                "detail": "poll_bars(..., internal_timeframe=self.settings.timeframe) fetches only one timeframe for the lane.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/config_models/settings.py"),
                "reference": "StrategySettings.validate_timeframe",
                "detail": "Runtime settings remain locked to a single timeframe value, and the default runtime contract is still one timeframe per lane.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/strategy/strategy_engine.py"),
                "reference": "StrategyEngine.process_bar / _evaluate_signals",
                "detail": "Signal evaluation consumes one finalized bar plus the single-timeframe feature history; there is no live 1m detection + separate 5m context path in the paper runtime.",
            },
        ],
        "conclusion": (
            "The current paper runtime cannot execute the frozen candidate's 1m detection surface plus 5m context filter as written."
        ),
    }


def _causality_audit() -> dict[str, Any]:
    return {
        "question": "Can the frozen confirmation rule be evaluated without future bars beyond the decision point?",
        "answer": "NO",
        "non_causal_components": [
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/mgc_impulse_spike_confirmation_pass.py"),
                "reference": "_passes_confirmation_variant",
                "detail": "The rule explicitly requires new_extension_within_2_bars and confirmation_bar_count_first_3.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/mgc_impulse_spike_subtypes.py"),
                "reference": "_spike_feature_row",
                "detail": "Both metrics are computed from post-trigger bars after the signal index.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/mgc_impulse_spike_subtypes.py"),
                "reference": "_confirmation_bar_count_first_3",
                "detail": "Counts confirming bars in the first 3 bars after the trigger.",
            },
        ],
        "lookahead_verdict": (
            "The frozen validation winner is a research selection rule that uses post-trigger information. It is not causally executable at the original decision instant."
        ),
        "admission_rule": "If any part is non-causal, do not proceed with paper admission.",
    }


def _lane_isolation_proof() -> dict[str, Any]:
    return {
        "question": "Can the new lane remain isolated in source IDs, operator rows, evidence chain, and persistence artifacts?",
        "answer": "YES",
        "existing_support": [
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/probationary_runtime.py"),
                "reference": "_load_probationary_paper_lane_specs / _build_probationary_paper_lane_settings",
                "detail": "Each paper lane is keyed by lane_id, gets its own derived sqlite DB, and writes lane-local artifacts under outputs/.../lanes/<lane_id>.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/probationary_runtime.py"),
                "reference": "ProbationaryLaneStructuredLogger",
                "detail": "Lane events are enriched with lane_id and symbol before being written to lane-local and root artifacts.",
            },
            {
                "path": str(REPO_ROOT / "src/mgc_v05l/app/operator_dashboard.py"),
                "reference": "approved models / lane activity / session lane history payloads",
                "detail": "Dashboard rows are keyed by lane_id, instrument, and source_family, and explicitly state that same-family multi-instrument lanes remain separate with no cross-lane attribution inferred.",
            },
        ],
        "limit": (
            "Lane isolation is feasible once the family is causal and executable, but that does not rescue the current frozen candidate."
        ),
    }


def _minimal_delta() -> dict[str, Any]:
    return {
        "status": "NOT_EXECUTABLE_AS_FROZEN",
        "smallest_honest_next_step": [
            "Redefine the confirmation rule into a causal form: either delayed-confirmation entry after the confirmation bars exist, or same-bar causal proxies that can be known at decision time.",
            "Add a narrow paper-only multi-timeframe execution path or equivalent causal 5m context derivation so the lane can evaluate 1m detection together with 5m context truthfully.",
            "Revalidate the redesigned executable candidate before any paper admission toggle.",
        ],
        "do_not_do_now": [
            "Do not admit mgc_impulse_burst_continuation_min_confirm to paper in its current frozen form.",
            "Do not claim ALL_SESSIONS metadata or lane isolation makes the non-causal rule executable.",
        ],
    }


def _code_paths_inspected() -> list[dict[str, str]]:
    return [
        {
            "path": str(REPO_ROOT / "src/mgc_v05l/app/probationary_runtime.py"),
            "reference": "ProbationaryPaperLaneRuntime.poll_and_process",
            "why_it_matters": "Shows the paper lane polls a single timeframe and processes bars one at a time.",
        },
        {
            "path": str(REPO_ROOT / "src/mgc_v05l/app/probationary_runtime.py"),
            "reference": "_build_probationary_paper_lane_settings",
            "why_it_matters": "Shows lane_id/database/artifact isolation path.",
        },
        {
            "path": str(REPO_ROOT / "src/mgc_v05l/config_models/settings.py"),
            "reference": "StrategySettings.validate_timeframe",
            "why_it_matters": "Shows the runtime contract remains single-timeframe.",
        },
        {
            "path": str(REPO_ROOT / "src/mgc_v05l/strategy/strategy_engine.py"),
            "reference": "process_bar / _evaluate_signals / _apply_runtime_entry_controls",
            "why_it_matters": "Shows signal logic is evaluated on finalized runtime bars, not on post-trigger future bars.",
        },
        {
            "path": str(REPO_ROOT / "src/mgc_v05l/app/mgc_impulse_spike_confirmation_pass.py"),
            "reference": "_passes_confirmation_variant",
            "why_it_matters": "Shows the exact frozen confirmation rule requiring future-bar-derived fields.",
        },
        {
            "path": str(REPO_ROOT / "src/mgc_v05l/app/mgc_impulse_spike_subtypes.py"),
            "reference": "_spike_feature_row / _confirmation_bar_count_first_3",
            "why_it_matters": "Shows new_extension_within_2_bars and confirmation_bar_count_first_3 are computed from post-trigger bars.",
        },
        {
            "path": str(REPO_ROOT / "src/mgc_v05l/app/operator_dashboard.py"),
            "reference": "paper approved-model, lane-activity, and session-lane-history payloads",
            "why_it_matters": "Shows lane-level truth and no cross-lane attribution design for admitted paper lanes.",
        },
    ]


def _render_markdown(payload: dict[str, Any]) -> str:
    mtf = payload["multi_timeframe_feasibility"]
    causal = payload["causality_no_lookahead"]
    isolation = payload["lane_isolation_proof"]
    delta = payload["minimal_implementation_delta"]
    lines = [
        "# MGC Impulse Paper Executable Feasibility",
        "",
        f"## Verdict",
        "",
        f"- `{payload['top_line_verdict']}`",
        "",
        "## Multi-Timeframe Feasibility",
        "",
        f"- Answer: `{mtf['answer']}`",
        f"- Conclusion: {mtf['conclusion']}",
        "",
        "## Causality / No-Lookahead",
        "",
        f"- Answer: `{causal['answer']}`",
        f"- Verdict: {causal['lookahead_verdict']}",
        "",
        "## Lane Isolation",
        "",
        f"- Answer: `{isolation['answer']}`",
        f"- Limit: {isolation['limit']}",
        "",
        "## Minimal Delta",
        "",
        f"- Status: `{delta['status']}`",
    ]
    for row in delta["smallest_honest_next_step"]:
        lines.append(f"- {row}")
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
