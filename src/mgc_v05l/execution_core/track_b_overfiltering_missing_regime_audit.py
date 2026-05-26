"""Track B over-filtering, missing-regime, and ATP activation audit.

Read-only diagnostics for deciding whether low trade frequency is driven by
strict live predicates, missing regime coverage, or inactive trend
participation candidates. This module never changes strategy rules or runtime
authority.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime
from .track_b_atomic_io import write_json_atomic


DEFAULT_UNDERPERFORMANCE_REVIEW = (
    Path("outputs")
    / "track_b_execution_core"
    / "diagnostics"
    / "latest_strategy_underperformance_review.json"
)
DEFAULT_ROSTER_CONFIG = Path("config") / "track_b_guarded_paper_roster.json"
DEFAULT_OUTPUT_JSON = (
    Path("outputs")
    / "track_b_execution_core"
    / "diagnostics"
    / "latest_overfiltering_missing_regime_audit.json"
)
DEFAULT_OUTPUT_MD = Path("docs") / "track_b_overfiltering_missing_regime_audit.md"
DEFAULT_MISSED_OPPORTUNITY_DISCOVERY = (
    Path("outputs")
    / "track_b_execution_core"
    / "diagnostics"
    / "latest_missed_opportunity_discovery_layer.json"
)


@dataclass(frozen=True)
class OverfilteringMissingRegimeAuditConfig:
    repo_root: Path = Path(".")
    underperformance_review_json: Path = DEFAULT_UNDERPERFORMANCE_REVIEW
    roster_config_json: Path = DEFAULT_ROSTER_CONFIG
    output_json: Path = DEFAULT_OUTPUT_JSON
    output_md: Path = DEFAULT_OUTPUT_MD
    missed_opportunity_discovery_json: Path = DEFAULT_MISSED_OPPORTUNITY_DISCOVERY


def build_overfiltering_missing_regime_audit(
    *,
    config: OverfilteringMissingRegimeAuditConfig | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_config = config or OverfilteringMissingRegimeAuditConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    repo_root = Path(actual_config.repo_root)
    review_path = _resolve(repo_root, actual_config.underperformance_review_json)
    roster_path = _resolve(repo_root, actual_config.roster_config_json)
    discovery_path = _resolve(repo_root, actual_config.missed_opportunity_discovery_json)
    review = _load_json(review_path)
    roster = _load_json(roster_path)
    discovery = _load_json(discovery_path)
    active_roster = _active_roster_ids(roster, review.get("strategy_inventory"))

    overfiltering = _build_overfiltering_audit(review)
    atp = _build_atp_activation_audit(repo_root=repo_root, active_roster=active_roster)
    missed_moves = _build_missed_move_comparison(review, atp)
    recommendations = _rank_recommendations(overfiltering, atp, missed_moves)
    sub80 = _scope_sub80_mining(review)

    return {
        "schema_version": "track_b_overfiltering_missing_regime_audit_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER_RESEARCH_SHADOW_ONLY",
        "analysis_only": True,
        "dry_run_only": True,
        "not_order_authority": True,
        "not_lifecycle_authority": True,
        "submit_allowed": False,
        "broker_mutation_allowed": False,
        "live_money_route_allowed": False,
        "paper_proof_allowed": False,
        "dashboard_projection_consumed": False,
        "source_review_path": str(review_path),
        "active_guarded_roster_count": len(active_roster),
        "active_guarded_roster": sorted(active_roster),
        "overfiltering_audit": overfiltering,
        "atp_trend_participation_activation_audit": atp,
        "missed_obvious_move_comparison": missed_moves,
        "missed_opportunity_discovery_layer": _discovery_summary(discovery),
        "top_implementation_candidates": recommendations,
        "sub80_fit_mining_scope": sub80,
        "tollgate_before_live_rule_changes": {
            "classification": "STOP_BEFORE_LIVE_RULE_CHANGES",
            "requirements": [
                "Run all recommended variants as shadow/research only first.",
                "Collect forward MFE/MAE and exit-profile attribution by strategy family.",
                "Promote only explainable rule subsets, never raw lower score buckets.",
                "Keep Control Plane, Safe-State, guardian, lifecycle, and managed-exit gates authoritative.",
            ],
        },
    }


def write_overfiltering_missing_regime_audit(
    *,
    audit: Mapping[str, Any],
    config: OverfilteringMissingRegimeAuditConfig | None = None,
) -> tuple[Path, Path]:
    actual_config = config or OverfilteringMissingRegimeAuditConfig()
    repo_root = Path(actual_config.repo_root)
    json_path = _resolve(repo_root, actual_config.output_json)
    md_path = _resolve(repo_root, actual_config.output_md)
    write_json_atomic(json_path, dict(audit))
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(_render_markdown(audit), encoding="utf-8")
    return json_path, md_path


def create_overfiltering_missing_regime_audit(
    *,
    config: OverfilteringMissingRegimeAuditConfig | None = None,
    now: datetime | None = None,
) -> tuple[Path, Path, dict[str, Any]]:
    actual_config = config or OverfilteringMissingRegimeAuditConfig()
    audit = build_overfiltering_missing_regime_audit(config=actual_config, now=now)
    json_path, md_path = write_overfiltering_missing_regime_audit(audit=audit, config=actual_config)
    return json_path, md_path, audit


def _build_overfiltering_audit(review: Mapping[str, Any]) -> dict[str, Any]:
    rejection = review.get("rejection_attribution") if isinstance(review.get("rejection_attribution"), Mapping) else {}
    forward = review.get("forward_outcome_simulation") if isinstance(review.get("forward_outcome_simulation"), Mapping) else {}
    outcome_by_strategy: dict[str, Counter[str]] = defaultdict(Counter)
    for row in _as_list(forward.get("simulations")):
        if not isinstance(row, Mapping):
            continue
        strategy_id = str(row.get("strategy_id") or "UNKNOWN")
        outcome_by_strategy[strategy_id][str(row.get("outcome_classification") or "UNCLEAR")] += 1

    strategies = []
    for row in _as_list(rejection.get("per_strategy")):
        if not isinstance(row, Mapping):
            continue
        strategy_id = str(row.get("strategy_id") or "")
        near = _int(row.get("near_miss_count"), 0)
        one = _int(row.get("one_gate_away_count"), 0)
        two = _int(row.get("two_gate_away_count"), 0)
        evals = _int(row.get("evaluations"), 0)
        outcomes = dict(outcome_by_strategy.get(strategy_id, Counter()))
        strategies.append(
            {
                "strategy_id": strategy_id,
                "evaluations": evals,
                "near_miss_count": near,
                "one_gate_away_count": one,
                "two_gate_away_count": two,
                "near_miss_rate": round(near / evals, 6) if evals else 0.0,
                "ranked_failed_predicates": row.get("top_failed_predicates") or [],
                "ranked_blockers": row.get("ranked_blockers") or [],
                "forward_outcome_counts_after_rejection": outcomes,
                "overfiltering_classification": _classify_overfiltering(row, outcomes),
                "recommended_action": _recommend_overfiltering_action(row, outcomes),
            }
        )
    return {
        "classification": _overall_overfiltering_classification(strategies),
        "total_near_misses": rejection.get("total_near_misses"),
        "total_one_gate_away": rejection.get("total_one_gate_away"),
        "total_two_gate_away": rejection.get("total_two_gate_away"),
        "ranked_global_blockers": rejection.get("ranked_blockers") or [],
        "per_strategy": sorted(
            strategies,
            key=lambda item: (
                -int(item.get("one_gate_away_count") or 0),
                -int(item.get("near_miss_count") or 0),
                str(item.get("strategy_id")),
            ),
        ),
        "forward_outcome_interpretation": forward.get("interpretation"),
        "forward_outcome_counts": forward.get("outcome_counts") or {},
    }


def _build_atp_activation_audit(*, repo_root: Path, active_roster: set[str]) -> dict[str, Any]:
    configs = _discover_atp_configs(repo_root)
    active = []
    inactive = []
    for row in configs:
        strategy_id = str(row.get("standalone_strategy_id") or row.get("lane_id") or "")
        is_active = strategy_id in active_roster or str(row.get("shared_strategy_identity") or "") in active_roster
        payload = {
            **row,
            "guarded_paper_roster_active": is_active,
            "track_b_integrated_status": _track_b_integration_status(row, is_active),
            "inactive_reason": None if is_active else _atp_inactive_reason(row),
        }
        if is_active:
            active.append(payload)
        else:
            inactive.append(payload)
    production_track = [
        row for row in configs if str(row.get("experimental_status") or "").upper() in {"PRODUCTION_TRACK_CANDIDATE", "TRACKED_PAPER_BENCHMARK"}
        or str(row.get("non_approved")).lower() == "false"
    ]
    return {
        "classification": "ATP_TREND_CANDIDATES_EXIST_NOT_GUARDED_ROSTER_ACTIVE" if inactive and not active else "ATP_TREND_ACTIVE_OR_NOT_FOUND",
        "candidate_config_count": len(configs),
        "guarded_roster_active_count": len(active),
        "implemented_config_count": len([row for row in configs if row.get("standalone_strategy_id") or row.get("lane_id")]),
        "production_track_or_benchmark_count": len(production_track),
        "active_candidates": active,
        "inactive_candidates": inactive[:30],
        "research_lineage": [
            "src/mgc_v05l/research/trend_participation/atp_promotion_add_review.py",
            "config/atp_companion_candidate_promotion_1_075r_favorable_only.yaml",
            "outputs/reports/atp_gc_production_track_pilot_review_20260407/gc_atp_production_track_pilot_review.json",
        ],
        "activation_interpretation": (
            "ATP/trend participation has implementation/config lineage, but current guarded PAPER runtime roster does not "
            "include its strategy ids. Treat activation as a Track B integration/shadow-roster task, not predicate loosening."
        ),
    }


def _build_missed_move_comparison(review: Mapping[str, Any], atp: Mapping[str, Any]) -> dict[str, Any]:
    regime = review.get("regime_coverage_audit") if isinstance(review.get("regime_coverage_audit"), Mapping) else {}
    shadow = review.get("ab_shadow_lane_generator_plan") if isinstance(review.get("ab_shadow_lane_generator_plan"), Mapping) else {}
    forward = review.get("forward_outcome_simulation") if isinstance(review.get("forward_outcome_simulation"), Mapping) else {}
    variants = _as_list(shadow.get("generated_shadow_variants"))
    has_late_join = any("LATE_JOIN" in str(row.get("shadow_variant_id") or "") for row in variants if isinstance(row, Mapping))
    has_gap_drift = any("GAP_DRIFT" in str(row.get("shadow_variant_id") or "") for row in variants if isinstance(row, Mapping))
    uncovered = regime.get("uncovered_regime_counts") if isinstance(regime.get("uncovered_regime_counts"), Mapping) else {}
    return {
        "classification": "MISSED_MOVES_MOSTLY_MISSING_REGIME_AND_SHADOW_COVERAGE",
        "current_live_strategy_fit": {
            "observed_by_symbol": regime.get("observed_by_symbol") or {},
            "uncovered_regime_counts": dict(uncovered),
            "interpretation": regime.get("interpretation"),
        },
        "atp_trend_candidate_fit": {
            "candidate_config_count": atp.get("candidate_config_count"),
            "guarded_roster_active_count": atp.get("guarded_roster_active_count"),
            "fit_assessment": "LIKELY_FITS_TREND_PARTICIPATION_BUT_INACTIVE_IN_GUARDED_ROSTER",
        },
        "late_join_drift_shadow_fit": {
            "shadow_available": has_late_join,
            "late_join_strong_drift_count": forward.get("late_join_strong_drift_count"),
            "max_late_join_score": forward.get("max_late_join_score"),
        },
        "gap_drift_continuation_shadow_fit": {
            "shadow_available": has_gap_drift,
            "gap_drift_shadow_classification": regime.get("gap_drift_shadow_classification"),
        },
    }


def _rank_recommendations(
    overfiltering: Mapping[str, Any],
    atp: Mapping[str, Any],
    missed_moves: Mapping[str, Any],
) -> list[dict[str, Any]]:
    recommendations = []
    if _int(atp.get("candidate_config_count"), 0) and not _int(atp.get("guarded_roster_active_count"), 0):
        recommendations.append(
            _candidate(
                "ACTIVATE_EXISTING_ATP_SHADOW",
                "Bring ATP/trend participation candidates into Track B shadow/diagnostic roster first",
                "HIGH",
                "Existing ATP configs and production-track lineage exist, but no ATP id is active in guarded PAPER roster.",
                ["Track B integration audit", "shadow roster isolation", "managed lifecycle/exit profile mapping"],
                "Do not grant submit authority until Control Plane/Safe-State/generation-scoped path is proven for ATP.",
            )
        )
    late_join = missed_moves.get("late_join_drift_shadow_fit") if isinstance(missed_moves.get("late_join_drift_shadow_fit"), Mapping) else {}
    if _int(late_join.get("late_join_strong_drift_count"), 0) > 0:
        recommendations.append(
            _candidate(
                "ADD_B_GRADE_SHADOW",
                "Promote Asian Drift late-join diagnostic into persistent B-grade shadow tracking",
                "HIGH",
                f"{late_join.get('late_join_strong_drift_count')} late-join strong drift observations; max score {late_join.get('max_late_join_score')}.",
                ["forward outcome history", "session anchor policy review"],
                "Missing-anchor state remains non-authoritative.",
            )
        )
    if missed_moves.get("current_live_strategy_fit"):
        recommendations.append(
            _candidate(
                "CREATE_NEW_CONTINUATION_FAMILY",
                "Create gap/drift continuation shadow family before sub-80 mining",
                "HIGH",
                "Current live roster is heavy on snap-turn, retest, and session-window predicates.",
                ["regime labels", "ATP fit comparison", "shadow MFE/MAE tracking"],
                "Continuation can chase; keep shadow-only until exits prove durable.",
            )
        )
    top = _as_list(overfiltering.get("per_strategy"))[:3]
    if top:
        recommendations.append(
            _candidate(
                "TUNE_SPECIFIC_PREDICATE",
                "Instrument top one-gate/two-gate predicates as strategy-local shadows",
                "MEDIUM",
                "Near-miss pressure exists, but forward outcomes are not yet strong enough for live rule loosening.",
                ["per-predicate forward outcome attribution"],
                "Predicate tuning without regime segmentation can add low-quality trade supply.",
            )
        )
    recommendations.append(
        _candidate(
            "DEFER_SUB80_MINING",
            "Scope 70-79 mining after over-filtering and ATP activation are measured",
            "MEDIUM",
            "The >=0.80 broad near universe is already negative after cost; sub-80 should be explainable-rule shadow only.",
            ["bucket instrumentation", "explainable subset rules", "shadow-only runner"],
            "Raw lower-score mining is likely to overfit and degrade quality.",
        )
    )
    return recommendations[:5]


def _scope_sub80_mining(review: Mapping[str, Any]) -> dict[str, Any]:
    threshold = review.get("threshold_pressure_analysis") if isinstance(review.get("threshold_pressure_analysis"), Mapping) else {}
    return {
        "classification": "SUB80_MINING_DEFERRED_UNTIL_OVERFILTERING_AND_MISSING_REGIME_WORK",
        "allowed_scope": "70-79 explainable rule subsets only, shadow-only, no live authority",
        "blocked_scope": "raw score-bucket lowering or live predicate loosening",
        "reason": threshold.get("evidence_summary"),
        "requirements": [
            "Measure ATP/trend participation activation gap first.",
            "Measure late-join and continuation shadows first.",
            "Require forward MFE/MAE and exit attribution by subset.",
        ],
        "submit_allowed": False,
        "broker_mutation_allowed": False,
    }


def _discovery_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not payload:
        return {
            "classification": "MISSED_OPPORTUNITY_DISCOVERY_NOT_AVAILABLE",
            "submit_allowed": False,
            "broker_mutation_allowed": False,
        }
    return {
        "classification": payload.get("classification"),
        "atp_shadow_summary": payload.get("atp_shadow_summary") or {},
        "near_miss_scoring_summary": payload.get("near_miss_scoring_summary") or {},
        "forward_outcome_summary": payload.get("forward_outcome_summary") or {},
        "timestamp_locked_forward_evidence_summary": payload.get("timestamp_locked_forward_evidence_summary") or {},
        "b_grade_missed_winner_family_rankings": payload.get("b_grade_missed_winner_family_rankings") or [],
        "persistent_shadow_families": payload.get("persistent_shadow_families") or [],
        "promotion_gate_recommendations": payload.get("promotion_gate_recommendations") or [],
        "submit_allowed": False,
        "broker_mutation_allowed": False,
    }


def _classify_overfiltering(row: Mapping[str, Any], outcomes: Mapping[str, int]) -> str:
    if int(outcomes.get("MISSED_WINNER", 0)) > 0:
        return "OVERFILTERING_POSSIBLE_MISSED_WINNER"
    if _int(row.get("one_gate_away_count"), 0) > 0:
        return "OVERFILTERING_POSSIBLE_ONE_GATE_AWAY"
    if _int(row.get("two_gate_away_count"), 0) > 0:
        return "OVERFILTERING_POSSIBLE_TWO_GATE_AWAY"
    return "NO_OVERFILTERING_SIGNAL"


def _recommend_overfiltering_action(row: Mapping[str, Any], outcomes: Mapping[str, int]) -> str:
    blockers = json.dumps(row.get("ranked_blockers") or [])
    if int(outcomes.get("MISSED_WINNER", 0)) > 0:
        return "ADD_STRATEGY_LOCAL_SHADOW_FOR_FAILED_PREDICATE"
    if "MISSING_SESSION_ANCHOR" in blockers:
        return "ADD_LATE_JOIN_VARIANT"
    if "BREAKOUT_RETEST_STRICTNESS" in blockers:
        return "ADD_RELAXED_BREAKOUT_RETEST_SHADOW"
    if "SNAP_TURN_REVERSAL_PREDICATE" in blockers:
        return "LEAVE_STRICT_RULE_UNCHANGED_UNTIL_REVERSAL_REGIME"
    return "CONTINUE_OBSERVING"


def _overall_overfiltering_classification(strategies: Sequence[Mapping[str, Any]]) -> str:
    if any(item.get("overfiltering_classification") == "OVERFILTERING_POSSIBLE_MISSED_WINNER" for item in strategies):
        return "OVERFILTERING_REQUIRES_SHADOW_REMEDIATION"
    if any(_int(item.get("one_gate_away_count"), 0) > 0 for item in strategies):
        return "OVERFILTERING_PRESSURE_PRESENT"
    return "OVERFILTERING_NOT_PRIMARY_FROM_CURRENT_EVIDENCE"


def _track_b_integration_status(row: Mapping[str, Any], is_active: bool) -> str:
    if is_active:
        return "GUARDED_PAPER_ROSTER_ACTIVE"
    if str(row.get("non_approved")).lower() == "false" or "PRODUCTION" in str(row.get("experimental_status") or "").upper():
        return "IMPLEMENTED_PRODUCTION_TRACK_NOT_GUARDED_ROSTER_ACTIVE"
    if row.get("standalone_strategy_id") or row.get("lane_id"):
        return "IMPLEMENTED_PROBATIONARY_OR_RESEARCH_CONFIG"
    return "RESEARCH_LINEAGE_ONLY"


def _atp_inactive_reason(row: Mapping[str, Any]) -> str:
    if str(row.get("non_approved")).lower() == "true":
        return "non_approved_or_research_candidate_config"
    if str(row.get("paper_only")).lower() != "true":
        return "not_paper_only_track_b_guarded_candidate"
    return "not_present_in_track_b_guarded_paper_roster"


def _candidate(
    action: str,
    title: str,
    expected_impact: str,
    evidence: str,
    dependencies: Sequence[str],
    risk: str,
) -> dict[str, Any]:
    return {
        "action": action,
        "title": title,
        "expected_impact": expected_impact,
        "evidence": evidence,
        "dependencies": list(dependencies),
        "risk": risk,
        "live_rule_change_allowed": False,
    }


def _discover_atp_configs(repo_root: Path) -> list[dict[str, Any]]:
    configs = []
    for path in sorted((repo_root / "config").glob("*atp_companion*.yaml")):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        lane = _extract_lane_payload(text)
        configs.append(
            {
                "path": str(path),
                "lane_id": lane.get("lane_id"),
                "standalone_strategy_id": lane.get("standalone_strategy_id"),
                "shared_strategy_identity": lane.get("shared_strategy_identity"),
                "strategy_family": lane.get("strategy_family") or _simple_yaml(text, "strategy_family"),
                "strategy_identity_root": lane.get("strategy_identity_root") or _simple_yaml(text, "strategy_identity_root"),
                "symbol": lane.get("symbol"),
                "runtime_kind": lane.get("runtime_kind"),
                "quality_bucket_policy": lane.get("quality_bucket_policy") or _simple_yaml(text, "quality_bucket_policy"),
                "experimental_status": lane.get("experimental_status") or _simple_yaml(text, "experimental_status"),
                "paper_only": lane.get("paper_only"),
                "non_approved": lane.get("non_approved"),
                "candidate_id": lane.get("candidate_id") or _simple_yaml(text, "candidate_id"),
            }
        )
    return configs


def _extract_lane_payload(text: str) -> dict[str, Any]:
    match = re.search(r"probationary_paper_lanes_json:\s*'(?P<payload>\[.*?\])'", text, flags=re.DOTALL)
    if not match:
        return {}
    try:
        payload = json.loads(match.group("payload"))
    except json.JSONDecodeError:
        return {}
    if isinstance(payload, list) and payload and isinstance(payload[0], Mapping):
        return dict(payload[0])
    return {}


def _simple_yaml(text: str, key: str) -> str | None:
    match = re.search(rf"^{re.escape(key)}:\s*[\"']?(?P<value>[^\"'\n#]+)", text, flags=re.MULTILINE)
    return match.group("value").strip() if match else None


def _render_markdown(audit: Mapping[str, Any]) -> str:
    over = audit.get("overfiltering_audit") if isinstance(audit.get("overfiltering_audit"), Mapping) else {}
    atp = (
        audit.get("atp_trend_participation_activation_audit")
        if isinstance(audit.get("atp_trend_participation_activation_audit"), Mapping)
        else {}
    )
    missed = (
        audit.get("missed_obvious_move_comparison")
        if isinstance(audit.get("missed_obvious_move_comparison"), Mapping)
        else {}
    )
    discovery = (
        audit.get("missed_opportunity_discovery_layer")
        if isinstance(audit.get("missed_opportunity_discovery_layer"), Mapping)
        else {}
    )
    lines = [
        "# Track B Over-Filtering / Missing-Regime Audit",
        "",
        f"Generated: `{audit.get('generated_at')}`",
        "",
        "This is research/shadow only. It does not loosen live rules, mutate broker state, restart runtime, invoke paper_proof, or create live-money authority.",
        "",
        "## Summary",
        "",
        f"- Over-filtering: `{over.get('classification')}`",
        f"- ATP/trend participation: `{atp.get('classification')}`",
        f"- Missed-move fit: `{missed.get('classification')}`",
        f"- Discovery layer: `{discovery.get('classification')}`",
        f"- Timestamp-locked evidence: `{discovery.get('timestamp_locked_forward_evidence_summary')}`",
        f"- Persistent shadow families: `{discovery.get('persistent_shadow_families')}`",
        f"- B-grade missed-winner families: `{discovery.get('b_grade_missed_winner_family_rankings')}`",
        f"- Sub-80 mining: `{audit.get('sub80_fit_mining_scope', {}).get('classification') if isinstance(audit.get('sub80_fit_mining_scope'), Mapping) else None}`",
        "",
        "## Over-Filtering Audit",
        "",
        f"- Near misses: `{over.get('total_near_misses')}`",
        f"- One-gate-away: `{over.get('total_one_gate_away')}`",
        f"- Two-gate-away: `{over.get('total_two_gate_away')}`",
        "",
        "| Strategy | Near Misses | One Gate | Two Gate | Classification | Action |",
        "| --- | ---: | ---: | ---: | --- | --- |",
    ]
    for row in _as_list(over.get("per_strategy"))[:20]:
        if not isinstance(row, Mapping):
            continue
        lines.append(
            f"| `{row.get('strategy_id')}` | {row.get('near_miss_count')} | {row.get('one_gate_away_count')} | {row.get('two_gate_away_count')} | `{row.get('overfiltering_classification')}` | `{row.get('recommended_action')}` |"
        )
    lines.extend(
        [
            "",
            "## ATP / Trend Participation Activation",
            "",
            f"- Candidate configs: `{atp.get('candidate_config_count')}`",
            f"- Guarded roster active: `{atp.get('guarded_roster_active_count')}`",
            f"- Interpretation: {atp.get('activation_interpretation')}",
            "",
            "| Config Strategy | Symbol | Status | Why Inactive |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in _as_list(atp.get("inactive_candidates"))[:12]:
        if not isinstance(row, Mapping):
            continue
        strategy_id = row.get("standalone_strategy_id") or row.get("lane_id")
        lines.append(
            f"| `{strategy_id}` | `{row.get('symbol')}` | `{row.get('track_b_integrated_status')}` | {row.get('inactive_reason')} |"
        )
    lines.extend(
        [
            "",
            "## Top Implementation Candidates",
            "",
            "| Rank | Action | Title | Impact | Risk |",
            "| ---: | --- | --- | --- | --- |",
        ]
    )
    for index, row in enumerate(_as_list(audit.get("top_implementation_candidates")), start=1):
        if not isinstance(row, Mapping):
            continue
        lines.append(
            f"| {index} | `{row.get('action')}` | {row.get('title')} | `{row.get('expected_impact')}` | {row.get('risk')} |"
        )
    tollgate = audit.get("tollgate_before_live_rule_changes") if isinstance(audit.get("tollgate_before_live_rule_changes"), Mapping) else {}
    lines.extend(["", "## Tollgate", "", f"Recommendation: `{tollgate.get('classification')}`", ""])
    for req in _as_list(tollgate.get("requirements")):
        lines.append(f"- {req}")
    lines.append("")
    return "\n".join(lines)


def _active_roster_ids(roster: Mapping[str, Any], fallback: Any) -> set[str]:
    ids = {str(item) for item in _as_list(fallback) if str(item)}
    for key in ("enabled_strategy_ids", "strategies", "enabled_strategies", "roster", "strategy_ids"):
        for item in _as_list(roster.get(key)):
            if isinstance(item, Mapping):
                value = item.get("strategy_id") or item.get("standalone_strategy_id") or item.get("id")
            else:
                value = item
            if value:
                ids.add(str(value))
    return ids


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--json", action="store_true", help="Print generated audit JSON.")
    args = parser.parse_args(argv)
    config = OverfilteringMissingRegimeAuditConfig(repo_root=args.repo_root)
    json_path, md_path, audit = create_overfiltering_missing_regime_audit(config=config)
    if args.json:
        print(json.dumps(audit, indent=2, sort_keys=True))
    else:
        print(f"Wrote {json_path}")
        print(f"Wrote {md_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
