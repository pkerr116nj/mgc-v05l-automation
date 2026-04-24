"""Evidence-building assessment for insufficient-evidence live families."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..layer1.pilot import run_approved_quant_layer1_pilot_rerun


REPO_ROOT = Path.cwd()
DEFAULT_AUDIT_ROOT = REPO_ROOT / "outputs" / "validation_layer" / "live_candidate_universe_audit"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "validation_layer" / "evidence_building_assessment"

TARGET_FAMILIES: tuple[str, ...] = (
    "index_futures_ny_intraday_forced_core_v2",
    "asia_london_participation_core_v1",
    "failed_move_reversal",
    "gold_forced_session_baseline_v2",
    "breakout_continuation",
)


@dataclass(frozen=True)
class FamilyPlan:
    family_name: str
    strategic_importance: str
    closeness_to_judgeable: str
    evidence_value: str
    retrofit_cost: str
    current_level: int
    target_next_level: int
    next_step: str
    why_now: str
    local_research_artifacts: tuple[str, ...]
    local_code_paths: tuple[str, ...]
    local_optimization_paths: tuple[str, ...] = ()
    family_level_rerun_kind: str | None = None


FAMILY_PLANS: dict[str, FamilyPlan] = {
    "breakout_continuation": FamilyPlan(
        family_name="breakout_continuation",
        strategic_importance="medium_high",
        closeness_to_judgeable="high",
        evidence_value="high",
        retrofit_cost="low",
        current_level=2,
        target_next_level=3,
        next_step="Use the approved-quant evaluator path to regenerate a family-level StrategyBacktest with real trades, then keep it classified as partially judgeable until sample depth improves.",
        why_now="The repo already has a typed evaluator, lane spec, and Layer 1 adapter path, so evidence can improve without a new family-specific framework.",
        local_research_artifacts=(
            "outputs/validation_layer/approved_quant_layer1_pilot/phase2c_breakout_metals_only_us_unknown_baseline/validation_report.json",
        ),
        local_code_paths=(
            "src/mgc_v05l/app/approved_quant_lanes/evaluator.py",
            "src/mgc_v05l/app/approved_quant_lanes/specs.py",
        ),
        family_level_rerun_kind="approved_quant",
    ),
    "failed_move_reversal": FamilyPlan(
        family_name="failed_move_reversal",
        strategic_importance="medium",
        closeness_to_judgeable="high",
        evidence_value="high",
        retrofit_cost="low",
        current_level=2,
        target_next_level=3,
        next_step="Use the approved-quant evaluator path to regenerate a family-level StrategyBacktest with real trades, then decide whether the family deserves broader sample-building work.",
        why_now="Like breakout_continuation, this family already has a compact evaluator-backed research surface instead of only runtime leftovers.",
        local_research_artifacts=(
            "outputs/validation_layer/approved_quant_layer1_pilot/phase2c_failed_core4_plus_qc_no_us_baseline/validation_report.json",
        ),
        local_code_paths=(
            "src/mgc_v05l/app/approved_quant_lanes/evaluator.py",
            "src/mgc_v05l/app/approved_quant_lanes/specs.py",
        ),
        family_level_rerun_kind="approved_quant",
    ),
    "asia_london_participation_core_v1": FamilyPlan(
        family_name="asia_london_participation_core_v1",
        strategic_importance="high",
        closeness_to_judgeable="medium",
        evidence_value="high",
        retrofit_cost="medium",
        current_level=2,
        target_next_level=3,
        next_step="Adapt the candidate-system research artifact into a family-level StrategyBacktest and rerun the local optimization script to preserve real OptimizationRun history.",
        why_now="This family already has a candidate-system archive and a local optimization script, which is the cleanest path toward full optimization-aware skepticism outside ATP.",
        local_research_artifacts=(
            "outputs/reports/asia_london_participation_candidate_system_archive_v2/asia_london_participation_candidate_system_research.json",
            "outputs/reports/asia_london_participation_candidate_admission_archive_v2/asia_london_participation_all_variants_1x.paper_package.json",
        ),
        local_code_paths=(
            "src/mgc_v05l/app/asia_london_participation_candidate_admission_plan.py",
            "src/mgc_v05l/app/asia_london_participation_research.py",
        ),
        local_optimization_paths=("src/mgc_v05l/app/asia_london_participation_optimization.py",),
    ),
    "index_futures_ny_intraday_forced_core_v2": FamilyPlan(
        family_name="index_futures_ny_intraday_forced_core_v2",
        strategic_importance="high",
        closeness_to_judgeable="medium_low",
        evidence_value="high",
        retrofit_cost="medium_high",
        current_level=2,
        target_next_level=3,
        next_step="Build one family-level adapter from the candidate-system research archive into a typed StrategyBacktest, then decide whether preserving full optimization history is worth the additional lift.",
        why_now="It is strategically important and broad, but the current local artifacts are summary-heavy and will need a deliberate family adapter before the machine can judge it honestly.",
        local_research_artifacts=(
            "outputs/reports/index_futures_forced_session_candidate_system_archive_v1/index_futures_forced_session_candidate_system_research.json",
            "outputs/reports/index_futures_forced_session_candidate_admission_archive_v1/index_futures_all_variants_1x.paper_package.json",
        ),
        local_code_paths=(
            "src/mgc_v05l/app/index_futures_forced_session_candidate_system_research.py",
            "src/mgc_v05l/app/index_futures_forced_session_candidate_admission_plan.py",
        ),
    ),
    "gold_forced_session_baseline_v2": FamilyPlan(
        family_name="gold_forced_session_baseline_v2",
        strategic_importance="medium_high",
        closeness_to_judgeable="medium_low",
        evidence_value="medium_high",
        retrofit_cost="medium_high",
        current_level=2,
        target_next_level=3,
        next_step="Build one family-level adapter from the candidate-system portfolio archive into a typed StrategyBacktest and preserve lane-level lineage from the source research artifacts.",
        why_now="The family has strong portfolio-style archives, but they are fragmented across source studies and still need a single contractual Layer 1 family surface.",
        local_research_artifacts=(
            "outputs/reports/gc_mgc_forced_session_candidate_system_archive_v4/gc_mgc_forced_session_candidate_system_research.json",
            "outputs/reports/gc_mgc_forced_session_candidate_admission_archive_v3/gc_1x_all_lanes.paper_package.json",
        ),
        local_code_paths=(
            "src/mgc_v05l/app/gc_mgc_forced_session_candidate_system_research.py",
            "src/mgc_v05l/app/gc_mgc_forced_session_candidate_admission_plan.py",
        ),
    ),
}


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _lane_gap_categories(row: dict[str, Any]) -> list[str]:
    gaps: list[str] = []
    if row.get("sample_evidence_status") == "no_realized_trade_sample":
        gaps.append("no_realized_trade_sample")
    if row.get("sample_evidence_status") == "thin_trade_sample":
        gaps.append("too_thin_sample")
    if row.get("robustness_evidence_status") == "insufficient":
        gaps.append("robustness_prerequisites_not_met")
        gaps.append("too_thin_sample_for_phase4")
    if row.get("optimization_history_status") != "available":
        gaps.append("optimization_history_unavailable")
    blocked_reason = str(row.get("blocked_reason") or "")
    if blocked_reason == "database_missing":
        gaps.append("missing_database_artifact_availability")
    if blocked_reason.startswith("adaptation_error:"):
        gaps.append("incomplete_lineage")
    if row.get("evaluation_status") == "blocked" and not gaps:
        gaps.append("other_concrete_blocker")
    if not gaps:
        gaps.append("no_immediate_gap_classified")
    return gaps


def _lane_judgeability_level(row: dict[str, Any]) -> int:
    if row.get("evaluation_status") == "blocked" and row.get("layer1_status_resolved") == "blocked_missing_evidence":
        return 1
    if row.get("sample_evidence_status") == "no_realized_trade_sample":
        return 2
    if row.get("sample_evidence_status") == "thin_trade_sample":
        return 3
    if row.get("sample_evidence_status") == "substantive_trade_sample" and row.get("optimization_history_status") != "available":
        return 4
    if row.get("sample_evidence_status") == "substantive_trade_sample" and row.get("optimization_history_status") == "available":
        return 5
    return 2


def _level_label(level: int) -> str:
    return {
        1: "not_adaptable",
        2: "adaptable_but_not_judgeable",
        3: "partially_judgeable",
        4: "scientifically_judgeable",
        5: "ready_for_deeper_scientific_review",
    }[level]


def _load_live_rows(audit_root: Path) -> list[dict[str, Any]]:
    manifest_path = audit_root / "live_candidate_universe_manifest.json"
    payload = _load_json(manifest_path)
    return list(payload["rows"])


def _approved_quant_family_reports(root_dir: Path) -> dict[str, dict[str, Any]]:
    artifacts = run_approved_quant_layer1_pilot_rerun(output_dir=root_dir)
    mapping: dict[str, str] = {
        "breakout_continuation": "phase2c.breakout.metals_only.us_unknown.baseline",
        "failed_move_reversal": "phase2c.failed.core4_plus_qc.no_us.baseline",
    }
    reports: dict[str, dict[str, Any]] = {}
    for family, lane_id in mapping.items():
        report_paths = artifacts.reports[lane_id]
        strategy_payload = _load_json(Path(report_paths["strategy_backtest_json"]))
        report_payload = _load_json(Path(report_paths["validation_report_json"]))
        reports[family] = {
            "family_level_status": "partially_judgeable",
            "trade_count": len(strategy_payload.get("trades", [])),
            "bar_count": len(strategy_payload.get("bar_data", [])),
            "overall_status": report_payload["overall_status"],
            "report_paths": report_paths,
            "module_statuses": {
                row["module_name"]: row["status"] for row in report_payload.get("module_results", [])
            },
        }
    return reports


def _family_priority_key(plan: FamilyPlan) -> tuple[int, int, int, int]:
    strategic = {"high": 0, "medium_high": 1, "medium": 2}.get(plan.strategic_importance, 3)
    closeness = {"high": 0, "medium": 1, "medium_low": 2}.get(plan.closeness_to_judgeable, 3)
    value = {"high": 0, "medium_high": 1, "medium": 2}.get(plan.evidence_value, 3)
    cost = {"low": 0, "medium": 1, "medium_high": 2}.get(plan.retrofit_cost, 3)
    return (closeness, cost, strategic, value)


def _family_recommendation(family_record: dict[str, Any]) -> str:
    family = family_record["family_name"]
    current_level = family_record["current_family_level_label"]
    if family_record.get("family_level_uplift") is not None:
        uplift = family_record["family_level_uplift"]
        return (
            f"`{family}` should be upgraded next: we already have a bounded family-level rerun path, and it moves the family from "
            f"`{current_level}` to `{uplift['family_level_status']}` without weakening the machine."
        )
    if family_record["current_family_level"] == 2 and family_record["priority_rank"] <= 3:
        return (
            f"`{family}` is the next planning-grade upgrade candidate: it has meaningful local research artifacts, but still needs a "
            "family-level Layer 1 adapter before the machine can judge it."
        )
    return f"`{family}` is not worth current remediation priority relative to the cleaner upgrade paths above it."


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Targeted Evidence-Building Assessment",
        "",
        "## Summary",
        f"- Generated at: `{payload['generated_at']}`",
        f"- Target families: `{len(payload['target_families'])}`",
        "",
        "## Priority Ranking",
    ]
    for record in payload["priority_ranking"]:
        lines.append(
            f"- `{record['priority_rank']}. {record['family_name']}` current=`{record['current_family_level_label']}` "
            f"next=`{record['target_next_level_label']}` cost=`{record['retrofit_cost']}`"
        )

    lines.extend(["", "## Judgeability Ladder"])
    for level, label in payload["judgeability_ladder"].items():
        lines.append(f"- `{level}`: `{label}`")

    lines.extend(["", "## Family Detail"])
    for record in payload["priority_ranking"]:
        lines.append(f"### {record['family_name']}")
        lines.append(f"- Current level: `{record['current_family_level_label']}`")
        lines.append(f"- Target next level: `{record['target_next_level_label']}`")
        lines.append(f"- Strategic importance: `{record['strategic_importance']}`")
        lines.append(f"- Closeness to judgeable: `{record['closeness_to_judgeable']}`")
        lines.append(f"- Retrofit cost: `{record['retrofit_cost']}`")
        lines.append(f"- Why now: {record['why_now']}")
        lines.append(f"- Minimum next step: {record['minimum_next_step']}")
        gap_counts = record["gap_category_counts"]
        lines.append("- Gap counts:")
        for key, value in sorted(gap_counts.items()):
            lines.append(f"  - `{key}`: `{value}`")
        if record.get("family_level_uplift") is not None:
            uplift = record["family_level_uplift"]
            lines.append(
                f"- Family-level uplift: `{uplift['family_level_status']}` with `{uplift['trade_count']}` trades "
                f"and overall Layer 2 verdict `{uplift['overall_status']}`"
            )
        lines.append(f"- Recommendation: {record['recommendation']}")

    lines.extend(["", "## Immediate Recommendation"])
    lines.extend(f"- {item}" for item in payload["recommendations"])
    return "\n".join(lines) + "\n"


def run_targeted_evidence_building_assessment(
    *,
    output_dir: str | Path | None = None,
    audit_root: str | Path | None = None,
) -> dict[str, Any]:
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_ROOT).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_audit_root = Path(audit_root or DEFAULT_AUDIT_ROOT).resolve()

    live_rows = _load_live_rows(resolved_audit_root)
    family_rows = {
        family: [row for row in live_rows if row["source_family"] == family]
        for family in TARGET_FAMILIES
    }
    approved_quant_reports = _approved_quant_family_reports(resolved_output_dir / "family_reruns" / "approved_quant")

    family_records: list[dict[str, Any]] = []
    for family in TARGET_FAMILIES:
        plan = FAMILY_PLANS[family]
        rows = family_rows[family]
        gap_counter = Counter()
        for row in rows:
            gap_counter.update(_lane_gap_categories(row))
        current_level = min((_lane_judgeability_level(row) for row in rows), default=1)
        record = {
            "family_name": family,
            "live_lane_count": len(rows),
            "lane_inventory": [
                {
                    "item_id": row["item_id"],
                    "taxonomy": row["taxonomy"],
                    "trade_count": row.get("trade_count"),
                    "sample_evidence_status": row.get("sample_evidence_status"),
                    "robustness_evidence_status": row.get("robustness_evidence_status"),
                    "optimization_history_status": row.get("optimization_history_status"),
                    "blocked_reason": row.get("blocked_reason"),
                    "gap_categories": _lane_gap_categories(row),
                }
                for row in rows
            ],
            "gap_category_counts": dict(sorted(gap_counter.items())),
            "current_family_level": current_level,
            "current_family_level_label": _level_label(current_level),
            "target_next_level": plan.target_next_level,
            "target_next_level_label": _level_label(plan.target_next_level),
            "strategic_importance": plan.strategic_importance,
            "closeness_to_judgeable": plan.closeness_to_judgeable,
            "evidence_value": plan.evidence_value,
            "retrofit_cost": plan.retrofit_cost,
            "why_now": plan.why_now,
            "minimum_next_step": plan.next_step,
            "local_research_artifacts": list(plan.local_research_artifacts),
            "local_code_paths": list(plan.local_code_paths),
            "local_optimization_paths": list(plan.local_optimization_paths),
            "family_level_uplift": approved_quant_reports.get(family),
        }
        family_records.append(record)

    family_records.sort(key=lambda row: _family_priority_key(FAMILY_PLANS[row["family_name"]]))
    for index, record in enumerate(family_records, start=1):
        record["priority_rank"] = index
        record["recommendation"] = _family_recommendation(record)

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "target_families": list(TARGET_FAMILIES),
        "audit_root": str(resolved_audit_root),
        "judgeability_ladder": {
            "1": "not_adaptable",
            "2": "adaptable_but_not_judgeable",
            "3": "partially_judgeable",
            "4": "scientifically_judgeable",
            "5": "ready_for_deeper_scientific_review",
        },
        "priority_ranking": family_records,
        "recommendations": [
            "Upgrade `breakout_continuation` next: the repo already supports a bounded family-level rerun and it is the cheapest path to more real evidence.",
            "Upgrade `failed_move_reversal` immediately after breakout_continuation using the same approved-quant path.",
            "Treat `asia_london_participation_core_v1` as the next medium-cost scientific upgrade because it already has a candidate-system archive and a local optimization script.",
            "Defer `index_futures_ny_intraday_forced_core_v2` and `gold_forced_session_baseline_v2` until we are ready to build one family-level adapter each from their summary-heavy candidate-system archives.",
            "Keep ATP live/benchmark surfaces in the operational/debug bucket and keep the ATP promotion/add active candidate retired from promotion consideration.",
        ],
    }

    inventory_path = resolved_output_dir / "targeted_family_evidence_inventory.json"
    inventory_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path = resolved_output_dir / "targeted_family_evidence_inventory.md"
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "root_dir": str(resolved_output_dir),
        "inventory_path": str(inventory_path),
        "markdown_path": str(markdown_path),
        "family_count": len(family_records),
    }
