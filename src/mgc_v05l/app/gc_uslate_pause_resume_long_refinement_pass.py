"""Narrow refinement pass for GC / usLatePauseResumeLongTurn."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from .approved_branch_research_audit import APPROVED_ONLY_OVERRIDE, _run_replay_for_symbol
from .mnq_usDerivativeBearTurn_validation import (
    REPO_ROOT,
    TIMEFRAME,
    _compute_metrics,
    _load_impulse_reference,
    _load_reference_metrics,
    _load_rows,
)

REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "approved_branch_research"
LATEST_JSON_PATH = REPORT_DIR / "gc_usLatePauseResumeLongTurn_refinement_pass.json"
LATEST_MD_PATH = REPORT_DIR / "gc_usLatePauseResumeLongTurn_refinement_pass.md"

SYMBOL = "GC"
TARGET_FAMILY = "usLatePauseResumeLongTurn"

MGC_US_LATE_REFERENCE = {
    "summary": REPO_ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.summary.json",
    "ledger": REPO_ROOT / "outputs/replays/persisted_bar_replay_us_late_long_pattern_treatment_full_20260317.trade_ledger.csv",
}
GC_VALIDATED_BASELINE = REPORT_DIR / "gc_usLatePauseResumeLongTurn_inclusion_readiness.json"
MGC_DERIVATIVE_REFERENCE = REPORT_DIR / "mgc_usDerivativeBearTurn_validation.json"


@dataclass(frozen=True)
class VariantSpec:
    slug: str
    label: str
    rationale: str
    kind: str
    extra_overrides: dict[str, Any]


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog="gc-uslate-pause-resume-long-refinement-pass")


def main(argv: list[str] | None = None) -> int:
    build_parser().parse_args(argv)
    payload = build_and_write_gc_uslate_refinement_pass()
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_and_write_gc_uslate_refinement_pass() -> dict[str, Any]:
    variants = [
        VariantSpec(
            slug="raw_baseline",
            label="Validated approved direct baseline",
            rationale="The same approved-only direct GC replay shape used by the validated inclusion-readiness packet.",
            kind="raw_baseline",
            extra_overrides={},
        ),
        VariantSpec(
            slug="tight_curvature",
            label="Entry-quality refinement: tighter curvature",
            rationale="Require slightly stronger positive setup/resumption curvature on top of the validated approved-only GC baseline.",
            kind="entry_quality_refinement",
            extra_overrides={
                "us_late_pause_resume_long_setup_curvature_min": 0.20,
                "us_late_pause_resume_long_min_resumption_curvature": 0.20,
            },
        ),
        VariantSpec(
            slug="tight_curvature_and_expansion",
            label="Trap exclusion: tighter curvature plus capped expansion",
            rationale="Keep the same family, but reject more extended late resumptions by pairing slightly stronger curvature with a slightly tighter range-expansion cap on top of the validated approved-only GC baseline.",
            kind="trap_exclusion_refinement",
            extra_overrides={
                "us_late_pause_resume_long_setup_curvature_min": 0.20,
                "us_late_pause_resume_long_min_resumption_curvature": 0.20,
                "us_late_pause_resume_long_max_range_expansion_ratio": 1.15,
            },
        ),
    ]

    variant_rows = [_run_variant(spec) for spec in variants]
    baseline = next(row for row in variant_rows if row["slug"] == "raw_baseline")
    for row in variant_rows:
        row["delta_vs_raw"] = _delta_vs_raw(row["metrics"], baseline["metrics"])

    best_variant = _choose_best_variant(variant_rows, baseline["metrics"])
    verdict = _verdict_bucket(best_variant["slug"], best_variant["metrics"], baseline["metrics"])

    validated_baseline = json.loads(GC_VALIDATED_BASELINE.read_text(encoding="utf-8"))
    mgc_derivative = json.loads(MGC_DERIVATIVE_REFERENCE.read_text(encoding="utf-8"))
    impulse_reference = _load_impulse_reference()
    mgc_uslate = _load_reference_metrics(MGC_US_LATE_REFERENCE, TARGET_FAMILY)

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "thread_scope": "thread_2_only",
        "research_scope": "research_design_only",
        "instrument": SYMBOL,
        "family": TARGET_FAMILY,
        "baseline_alignment_note": "This pass is anchored to the validated approved-direct GC replay packet, not the broader generic usLate replay stack.",
        "approved_only_override_baseline": APPROVED_ONLY_OVERRIDE,
        "refinements_tested": [
            {
                "slug": row["slug"],
                "label": row["label"],
                "kind": row["kind"],
                "rationale": row["rationale"],
                "extra_overrides": row["extra_overrides"],
                "replay_artifacts": row["replay_artifacts"],
            }
            for row in variant_rows
        ],
        "variant_results": [
            {
                "slug": row["slug"],
                "label": row["label"],
                "kind": row["kind"],
                "metrics": row["metrics"],
                "delta_vs_raw": row["delta_vs_raw"],
            }
            for row in variant_rows
        ],
        "best_variant": {
            "slug": best_variant["slug"],
            "label": best_variant["label"],
            "kind": best_variant["kind"],
            "metrics": best_variant["metrics"],
            "delta_vs_raw": best_variant["delta_vs_raw"],
        },
        "comparisons": {
            "vs_raw_gc_baseline": {
                "reference_metrics": baseline["metrics"],
                "comparison_note": "All refinements are judged first against the rerun raw GC native-family baseline.",
            },
            "vs_validated_gc_baseline": {
                "reference_metrics": validated_baseline["economic_replay_quality"],
                "comparison_note": "Validated inclusion-readiness baseline from the prior pass, based on the persisted direct approved GC replay packet.",
            },
            "vs_MGC_usLatePauseResumeLongTurn": {
                "reference_metrics": asdict(mgc_uslate),
                "comparison_note": "Home metals benchmark for whether the GC refinement is approaching admission-quality economics.",
            },
            "vs_MGC_usDerivativeBearTurn": {
                "reference_metrics": mgc_derivative["economic_replay_quality"],
                "comparison_note": "Short-side research lead; included because GC usLate only needs to beat it on design-readiness, not to replace it structurally.",
            },
            "vs_parked_impulse_executable_reference": {
                "reference_metrics": impulse_reference,
                "comparison_note": "Included only as a failed executable reference; GC refinements should remain cleaner and more causal than that parked branch.",
            },
        },
        "direct_answers": {
            "did_any_narrow_refinement_materially_improve_gc_concentration_fragility": _material_improvement_answer(best_variant["slug"], best_variant["metrics"], baseline["metrics"]),
            "is_the_edge_still_mainly_in_the_raw_family_or_in_the_refined_version": _edge_location(
                best_slug=best_variant["slug"],
                verdict=verdict,
            ),
            "did_the_refinement_preserve_causal_cleanliness": "Yes. All tested variants stayed within the same causal family and used only compact on-bar structure filters or the existing 17:55 carryover exclusion.",
            "is_gc_now_closer_to_admission_ready_or_still_only_ready_for_a_narrow_future_paper_design_pass": (
                "Still only ready for a narrow future paper-design pass."
                if verdict != "STRONGER_NARROW_PAPER_DESIGN_CANDIDATE"
                else "Closer to admission-ready, but still not admitted."
            ),
            "single_biggest_blocker_still_remaining": _biggest_blocker(best_variant["metrics"]),
        },
        "verdict_bucket": verdict,
    }

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    LATEST_JSON_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    LATEST_MD_PATH.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "gc_usLatePauseResumeLongTurn_refinement_pass_json": str(LATEST_JSON_PATH),
        "gc_usLatePauseResumeLongTurn_refinement_pass_md": str(LATEST_MD_PATH),
        "verdict_bucket": verdict,
        "best_variant": payload["best_variant"]["slug"],
    }


def _run_variant(spec: VariantSpec) -> dict[str, Any]:
    artifacts = _run_replay_for_symbol(
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        lane="futures",
        label=f"gc_uslate_refinement_{spec.slug}",
        extra_overrides=spec.extra_overrides or None,
    )
    summary_payload = json.loads(artifacts.summary_path.read_text(encoding="utf-8"))
    rows = [row for row in _load_rows(artifacts.trade_ledger_path) if row["setup_family"] == TARGET_FAMILY]
    metrics = asdict(
        _compute_metrics(
            summary=summary_payload,
            rows=rows,
            sample_start=summary_payload["source_first_bar_ts"],
            sample_end=summary_payload["source_last_bar_ts"],
        )
    )
    return {
        "slug": spec.slug,
        "label": spec.label,
        "kind": spec.kind,
        "rationale": spec.rationale,
        "metrics": metrics,
        "extra_overrides": spec.extra_overrides,
        "replay_artifacts": {
            "summary": str(artifacts.summary_path),
            "summary_metrics": str(artifacts.summary_metrics_path),
            "trade_ledger": str(artifacts.trade_ledger_path),
            "replay_db": str(artifacts.replay_db_path),
        },
    }


def _delta_vs_raw(metrics: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    def delta(key: str) -> float | None:
        left = metrics.get(key)
        right = baseline.get(key)
        if left is None or right is None:
            return None
        return round(float(left) - float(right), 4)

    return {
        "trade_count_delta": delta("trades"),
        "pnl_delta": delta("realized_pnl"),
        "avg_trade_delta": delta("avg_trade"),
        "median_trade_delta": delta("median_trade"),
        "profit_factor_delta": delta("profit_factor"),
        "drawdown_delta": delta("max_drawdown"),
        "top_1_contribution_delta": delta("top_1_contribution"),
        "top_3_contribution_delta": delta("top_3_contribution"),
        "survives_without_top_1_changed": metrics.get("survives_without_top_1") != baseline.get("survives_without_top_1"),
        "survives_without_top_3_changed": metrics.get("survives_without_top_3") != baseline.get("survives_without_top_3"),
    }


def _refinement_score(metrics: dict[str, Any], baseline: dict[str, Any], slug: str) -> tuple[float, ...]:
    realized = float(metrics.get("realized_pnl") or 0.0)
    pf = float(metrics.get("profit_factor") or 0.0)
    top1 = float(metrics.get("top_1_contribution") or 999999.0)
    top3 = float(metrics.get("top_3_contribution") or 999999.0)
    drawdown = float(metrics.get("max_drawdown") or 0.0)
    trades = float(metrics.get("trades") or 0.0)

    baseline_realized = float(baseline.get("realized_pnl") or 0.0)
    baseline_top1 = float(baseline.get("top_1_contribution") or 999999.0)
    baseline_top3 = float(baseline.get("top_3_contribution") or 999999.0)

    concentration_improvement = (baseline_top1 - top1) + (baseline_top3 - top3)
    pnl_retention = (realized / baseline_realized) if baseline_realized else 0.0
    score = concentration_improvement + pf * 25.0 - drawdown * 0.02 + trades * 0.5
    if pnl_retention < 0.8:
        score -= 80.0
    if slug == "raw_baseline":
        score -= 5.0
    return (
        score,
        concentration_improvement,
        pnl_retention,
        realized,
        pf,
    )


def _choose_best_variant(rows: list[dict[str, Any]], baseline: dict[str, Any]) -> dict[str, Any]:
    return max(rows, key=lambda row: _refinement_score(row["metrics"], baseline, row["slug"]))


def _material_improvement(metrics: dict[str, Any], baseline: dict[str, Any]) -> bool:
    top1 = float(metrics.get("top_1_contribution") or 999999.0)
    top3 = float(metrics.get("top_3_contribution") or 999999.0)
    base_top1 = float(baseline.get("top_1_contribution") or 999999.0)
    base_top3 = float(baseline.get("top_3_contribution") or 999999.0)
    realized = float(metrics.get("realized_pnl") or 0.0)
    base_realized = float(baseline.get("realized_pnl") or 0.0)
    pf = float(metrics.get("profit_factor") or 0.0)
    base_pf = float(baseline.get("profit_factor") or 0.0)
    return (
        (base_realized > 0 and realized / base_realized >= 0.8)
        and ((base_top1 - top1) >= 15.0 or (base_top3 - top3) >= 20.0 or metrics.get("survives_without_top_1"))
        and pf >= max(1.0, base_pf - 0.2)
    )


def _verdict_bucket(best_slug: str, best_metrics: dict[str, Any], baseline: dict[str, Any]) -> str:
    if best_slug == "raw_baseline":
        return "RAW_BASELINE_REMAINS_BEST"
    if not _material_improvement(best_metrics, baseline):
        return "REFINEMENT_NOT_JUSTIFIED"
    if best_metrics.get("survives_without_top_1") or best_metrics.get("survives_without_top_3"):
        return "STRONGER_NARROW_PAPER_DESIGN_CANDIDATE"
    return "IMPROVED_BUT_STILL_DESIGN_STAGE"


def _material_improvement_answer(best_slug: str, best_metrics: dict[str, Any], baseline: dict[str, Any]) -> str:
    if best_slug == "raw_baseline":
        return "No. The raw baseline remained the best overall form."
    if _material_improvement(best_metrics, baseline):
        return "Yes, but only partially. The best refinement improved concentration enough to matter while staying causal."
    return "No. The tested refinements either failed to reduce concentration enough or gave up too much breadth/economics."


def _edge_location(*, best_slug: str, verdict: str) -> str:
    if verdict in {"RAW_BASELINE_REMAINS_BEST", "REFINEMENT_NOT_JUSTIFIED"}:
        return "RAW_FAMILY"
    if best_slug == "raw_baseline":
        return "RAW_FAMILY"
    return "REFINED_VERSION"


def _biggest_blocker(metrics: dict[str, Any]) -> str:
    if not metrics.get("survives_without_top_1") or not metrics.get("survives_without_top_3"):
        return "Concentration fragility remains the blocker."
    if (metrics.get("trades") or 0) < 15:
        return "Sample breadth is still too thin."
    return "Needs a later narrow paper-design confirmation pass."


def _fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "Unavailable"
    return f"{float(value):.{digits}f}"


def _render_markdown(payload: dict[str, Any]) -> str:
    best = payload["best_variant"]
    lines = [
        "# GC usLatePauseResumeLongTurn Refinement Pass",
        "",
        f"- Verdict: `{payload['verdict_bucket']}`",
        f"- Best variant: `{best['label']}`",
        "",
        "## Variants Tested",
    ]
    for row in payload["variant_results"]:
        metrics = row["metrics"]
        lines.extend(
            [
                f"- `{row['label']}`",
                f"  trades `{metrics['trades']}`, pnl `{_fmt(metrics['realized_pnl'])}`, avg `{_fmt(metrics['avg_trade'])}`, median `{_fmt(metrics['median_trade'])}`, PF `{_fmt(metrics['profit_factor'])}`, max DD `{_fmt(metrics['max_drawdown'])}`, top-1 `{_fmt(metrics['top_1_contribution'])}%`, top-3 `{_fmt(metrics['top_3_contribution'])}%`",
            ]
        )
    lines.extend(
        [
            "",
            "## Direct Answers",
            f"- Material concentration improvement: {payload['direct_answers']['did_any_narrow_refinement_materially_improve_gc_concentration_fragility']}",
            f"- Edge location: `{payload['direct_answers']['is_the_edge_still_mainly_in_the_raw_family_or_in_the_refined_version']}`",
            f"- Causal cleanliness preserved: {payload['direct_answers']['did_the_refinement_preserve_causal_cleanliness']}",
            f"- Admission closeness: {payload['direct_answers']['is_gc_now_closer_to_admission_ready_or_still_only_ready_for_a_narrow_future_paper_design_pass']}",
            f"- Biggest blocker: {payload['direct_answers']['single_biggest_blocker_still_remaining']}",
        ]
    )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
