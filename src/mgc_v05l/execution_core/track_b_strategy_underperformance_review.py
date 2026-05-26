"""Track B strategy underperformance review diagnostics.

This module is intentionally read-only. It composes existing Track B runtime,
near-miss, shadow, regime, and broker-effect artifacts into one ranked review
board before any live strategy rule changes are considered.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .models import require_aware_datetime
from .track_b_atomic_io import write_json_atomic


DEFAULT_OUTPUT_JSON = (
    Path("outputs")
    / "track_b_execution_core"
    / "diagnostics"
    / "latest_strategy_underperformance_review.json"
)
DEFAULT_OUTPUT_MD = Path("docs") / "track_b_strategy_underperformance_review.md"
DEFAULT_LOOP_EVENTS = (
    Path("outputs")
    / "track_b_execution_core"
    / "p0_observe_only"
    / "p0_observe_only_loop_events.jsonl"
)
DEFAULT_LATEST_LOOP = (
    Path("outputs") / "track_b_execution_core" / "p0_observe_only" / "latest_p0_observe_only_loop.json"
)
DEFAULT_NO_SIGNAL_ROLLUP = (
    Path("outputs")
    / "track_b_execution_core"
    / "diagnostics"
    / "latest_track_b_no_signal_attribution_rollup.json"
)
DEFAULT_P0_NEAR_MISS_AUDIT = (
    Path("outputs")
    / "track_b_execution_core"
    / "diagnostics"
    / "latest_p0_near_miss_predicate_pressure_audit.json"
)
DEFAULT_LATE_JOIN_SHADOW = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_shadow"
    / "latest_late_join_asian_drift_shadow.json"
)
DEFAULT_GAP_DRIFT_SHADOW = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_shadow"
    / "latest_gap_drift_continuation_shadow.json"
)
DEFAULT_P0_NEAR_MISS_SHADOW = (
    Path("outputs") / "track_b_execution_core" / "research_shadow" / "latest_p0_near_miss_shadow.json"
)
DEFAULT_TRADE_LEDGER = (
    Path("outputs") / "track_b_execution_core" / "paper_trade_ledger" / "track_b_paper_trade_ledger.jsonl"
)
DEFAULT_TRADE_SUMMARY = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_trade_ledger"
    / "latest_track_b_paper_trade_summary.json"
)
DEFAULT_MGC_5M = (
    Path("outputs")
    / "track_b_execution_core"
    / "phase1_runtime_market_data"
    / "MGC"
    / "5m"
    / "latest_runtime_candles.json"
)
DEFAULT_MNQ_5M = (
    Path("outputs")
    / "track_b_execution_core"
    / "phase1_runtime_market_data"
    / "MNQ"
    / "5m"
    / "latest_runtime_candles.json"
)
DEFAULT_ROSTER_CONFIG = Path("config") / "track_b_guarded_paper_roster.json"
DEFAULT_ENTRY_ACCEPTANCE_SUMMARY = (
    Path("outputs")
    / "reports"
    / "entry_acceptance_research"
    / "full_history_batch"
    / "combined_cross_instrument_summary.json"
)
DEFAULT_ENTRY_ACCEPTANCE_DECISION = Path("docs") / "track_b_entry_acceptance_candidate_universe_decision.md"
DEFAULT_ATP_PRODUCTION_TRACK_REVIEW = (
    Path("outputs")
    / "reports"
    / "atp_gc_production_track_pilot_review_20260407"
    / "gc_atp_production_track_pilot_review.json"
)
DEFAULT_MISSED_OPPORTUNITY_DISCOVERY = (
    Path("outputs")
    / "track_b_execution_core"
    / "diagnostics"
    / "latest_missed_opportunity_discovery_layer.json"
)
MAX_LOOP_EVENTS = 1000
MAX_LEDGER_ROWS = 500


INFRASTRUCTURE_TOKENS = (
    "broker",
    "control_plane",
    "guardian",
    "safe_state",
    "live_money",
    "paper_proof",
    "snapshot",
    "runtime_generation",
    "duplicate",
    "stale",
    "missing_evidence",
    "authorization",
)
MARKET_LOGIC_TOKENS = (
    "anchor",
    "session",
    "drift",
    "snap",
    "breakout",
    "retest",
    "pause",
    "resume",
    "pullback",
    "range",
    "body",
    "close",
    "curvature",
    "slope",
    "velocity",
    "reversal",
)


@dataclass(frozen=True)
class StrategyUnderperformanceReviewConfig:
    repo_root: Path = Path(".")
    output_json: Path = DEFAULT_OUTPUT_JSON
    output_md: Path = DEFAULT_OUTPUT_MD
    loop_events_jsonl: Path = DEFAULT_LOOP_EVENTS
    latest_loop_json: Path = DEFAULT_LATEST_LOOP
    no_signal_rollup_json: Path = DEFAULT_NO_SIGNAL_ROLLUP
    p0_near_miss_audit_json: Path = DEFAULT_P0_NEAR_MISS_AUDIT
    late_join_shadow_json: Path = DEFAULT_LATE_JOIN_SHADOW
    gap_drift_shadow_json: Path = DEFAULT_GAP_DRIFT_SHADOW
    p0_near_miss_shadow_json: Path = DEFAULT_P0_NEAR_MISS_SHADOW
    trade_ledger_jsonl: Path = DEFAULT_TRADE_LEDGER
    trade_summary_json: Path = DEFAULT_TRADE_SUMMARY
    mgc_5m_json: Path = DEFAULT_MGC_5M
    mnq_5m_json: Path = DEFAULT_MNQ_5M
    roster_config_json: Path = DEFAULT_ROSTER_CONFIG
    entry_acceptance_summary_json: Path = DEFAULT_ENTRY_ACCEPTANCE_SUMMARY
    entry_acceptance_decision_md: Path = DEFAULT_ENTRY_ACCEPTANCE_DECISION
    atp_production_track_review_json: Path = DEFAULT_ATP_PRODUCTION_TRACK_REVIEW
    missed_opportunity_discovery_json: Path = DEFAULT_MISSED_OPPORTUNITY_DISCOVERY
    max_loop_events: int = MAX_LOOP_EVENTS
    max_ledger_rows: int = MAX_LEDGER_ROWS


@dataclass(frozen=True)
class StrategyUnderperformanceReviewResult:
    report_json: Path
    report_md: Path
    report: dict[str, Any]


def build_strategy_underperformance_review(
    *,
    config: StrategyUnderperformanceReviewConfig | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_config = config or StrategyUnderperformanceReviewConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    repo_root = Path(actual_config.repo_root)

    latest_loop_path = _resolve(repo_root, actual_config.latest_loop_json)
    loop_events_path = _resolve(repo_root, actual_config.loop_events_jsonl)
    no_signal_path = _resolve(repo_root, actual_config.no_signal_rollup_json)
    near_miss_path = _resolve(repo_root, actual_config.p0_near_miss_audit_json)
    late_join_shadow_path = _resolve(repo_root, actual_config.late_join_shadow_json)
    gap_drift_shadow_path = _resolve(repo_root, actual_config.gap_drift_shadow_json)
    p0_shadow_path = _resolve(repo_root, actual_config.p0_near_miss_shadow_json)
    trade_ledger_path = _resolve(repo_root, actual_config.trade_ledger_jsonl)
    trade_summary_path = _resolve(repo_root, actual_config.trade_summary_json)
    mgc_5m_path = _resolve(repo_root, actual_config.mgc_5m_json)
    mnq_5m_path = _resolve(repo_root, actual_config.mnq_5m_json)
    roster_path = _resolve(repo_root, actual_config.roster_config_json)
    entry_acceptance_summary_path = _resolve(repo_root, actual_config.entry_acceptance_summary_json)
    entry_acceptance_decision_path = _resolve(repo_root, actual_config.entry_acceptance_decision_md)
    atp_production_track_review_path = _resolve(repo_root, actual_config.atp_production_track_review_json)
    missed_opportunity_discovery_path = _resolve(repo_root, actual_config.missed_opportunity_discovery_json)

    latest_loop = _load_json(latest_loop_path)
    loop_events, malformed_loop_events = _load_jsonl(loop_events_path, limit=actual_config.max_loop_events)
    no_signal_rollup = _load_json(no_signal_path)
    p0_near_miss_audit = _load_json(near_miss_path)
    late_join_shadow = _load_json(late_join_shadow_path)
    gap_drift_shadow = _load_json(gap_drift_shadow_path)
    p0_near_miss_shadow = _load_json(p0_shadow_path)
    trade_summary = _load_json(trade_summary_path)
    trade_rows, malformed_trade_rows = _load_jsonl(trade_ledger_path, limit=actual_config.max_ledger_rows)
    candle_index = {
        "MGC": _load_candles(mgc_5m_path),
        "MNQ": _load_candles(mnq_5m_path),
    }
    roster_payload = _load_json(roster_path)
    entry_acceptance_summary = _load_json(entry_acceptance_summary_path)
    atp_production_track_review = _load_json(atp_production_track_review_path)
    missed_opportunity_discovery = _load_json(missed_opportunity_discovery_path)

    strategy_ids = _strategy_inventory(latest_loop, loop_events, roster_payload, no_signal_rollup)
    rejection = _build_rejection_attribution(strategy_ids, loop_events, no_signal_rollup, p0_near_miss_audit)
    forward = _build_forward_outcome_simulation(
        near_misses=rejection["near_miss_examples"],
        p0_near_miss_audit=p0_near_miss_audit,
        late_join_shadow=late_join_shadow,
        gap_drift_shadow=gap_drift_shadow,
        candle_index=candle_index,
    )
    regime = _build_regime_coverage(strategy_ids, candle_index, p0_near_miss_audit, gap_drift_shadow)
    exit_attribution = _build_exit_position_attribution(trade_rows, trade_summary, candle_index)
    shadow = _build_shadow_lane_plan(rejection, regime, p0_near_miss_audit, p0_near_miss_shadow)
    remediation = _build_remediation_board(rejection, forward, regime, exit_attribution, shadow)
    root_causes = _top_root_causes(rejection, forward, regime, exit_attribution, p0_near_miss_audit)
    promotion_audit = _build_high_quality_strategy_promotion_audit(
        repo_root=repo_root,
        roster_payload=roster_payload,
        strategy_ids=strategy_ids,
        entry_acceptance_summary=entry_acceptance_summary,
        atp_production_track_review=atp_production_track_review,
        entry_acceptance_summary_path=entry_acceptance_summary_path,
        entry_acceptance_decision_path=entry_acceptance_decision_path,
        atp_production_track_review_path=atp_production_track_review_path,
    )
    threshold_audit = _build_threshold_pressure_analysis(promotion_audit)

    report: dict[str, Any] = {
        "schema_version": "track_b_strategy_underperformance_review_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER_RESEARCH_READ_ONLY",
        "analysis_only": True,
        "dry_run_only": True,
        "not_order_authority": True,
        "not_lifecycle_authority": True,
        "submit_allowed": False,
        "broker_mutation_allowed": False,
        "live_money_route_allowed": False,
        "paper_proof_allowed": False,
        "dashboard_projection_consumed": False,
        "input_summary": {
            "strategy_count": len(strategy_ids),
            "loop_event_count": len(loop_events),
            "malformed_loop_event_count": malformed_loop_events,
            "trade_rows_scanned": len(trade_rows),
            "malformed_trade_row_count": malformed_trade_rows,
            "latest_loop_classification": latest_loop.get("classification"),
            "latest_loop_completed_iterations": latest_loop.get("completed_iterations"),
        },
        "strategy_inventory": strategy_ids,
        "rejection_attribution": rejection,
        "forward_outcome_simulation": forward,
        "regime_coverage_audit": regime,
        "exit_position_management_attribution": exit_attribution,
        "high_quality_strategy_promotion_audit": promotion_audit,
        "threshold_pressure_analysis": threshold_audit,
        "missed_opportunity_discovery_layer": _missed_opportunity_summary(missed_opportunity_discovery),
        "ab_shadow_lane_generator_plan": shadow,
        "ranked_remediation_board": remediation,
        "top_underperformance_root_causes": root_causes,
        "top_implementation_recommendations": remediation[:5],
        "tollgate_recommendation": {
            "classification": "STOP_BEFORE_LIVE_RULE_CHANGES",
            "requirements": [
                "Collect forward outcomes for B-grade shadows across multiple sessions.",
                "Require strategy-family review before any predicate loosening becomes A-grade authority.",
                "Keep all B-grade and new-regime variants dry-run only until sample size and exit attribution are adequate.",
                "Confirm broker position guardian and managed-exit idempotency stay clean during any guarded PAPER expansion.",
            ],
        },
        "source_artifact_paths": {
            "latest_loop": str(latest_loop_path),
            "loop_events": str(loop_events_path),
            "no_signal_rollup": str(no_signal_path),
            "p0_near_miss_audit": str(near_miss_path),
            "late_join_shadow": str(late_join_shadow_path),
            "gap_drift_shadow": str(gap_drift_shadow_path),
            "p0_near_miss_shadow": str(p0_shadow_path),
            "trade_ledger": str(trade_ledger_path),
            "trade_summary": str(trade_summary_path),
            "mgc_5m": str(mgc_5m_path),
            "mnq_5m": str(mnq_5m_path),
            "roster_config": str(roster_path),
            "entry_acceptance_summary": str(entry_acceptance_summary_path),
            "entry_acceptance_decision": str(entry_acceptance_decision_path),
            "atp_production_track_review": str(atp_production_track_review_path),
            "missed_opportunity_discovery": str(missed_opportunity_discovery_path),
        },
    }
    return report


def write_strategy_underperformance_review(
    *,
    report: Mapping[str, Any],
    config: StrategyUnderperformanceReviewConfig | None = None,
) -> StrategyUnderperformanceReviewResult:
    actual_config = config or StrategyUnderperformanceReviewConfig()
    repo_root = Path(actual_config.repo_root)
    report_json = _resolve(repo_root, actual_config.output_json)
    report_md = _resolve(repo_root, actual_config.output_md)
    write_json_atomic(report_json, dict(report))
    report_md.parent.mkdir(parents=True, exist_ok=True)
    report_md.write_text(_render_markdown(report), encoding="utf-8")
    return StrategyUnderperformanceReviewResult(report_json=report_json, report_md=report_md, report=dict(report))


def create_strategy_underperformance_review(
    *,
    config: StrategyUnderperformanceReviewConfig | None = None,
    now: datetime | None = None,
) -> StrategyUnderperformanceReviewResult:
    actual_config = config or StrategyUnderperformanceReviewConfig()
    report = build_strategy_underperformance_review(config=actual_config, now=now)
    return write_strategy_underperformance_review(report=report, config=actual_config)


def _build_rejection_attribution(
    strategy_ids: Sequence[str],
    loop_events: Sequence[Mapping[str, Any]],
    no_signal_rollup: Mapping[str, Any],
    p0_near_miss_audit: Mapping[str, Any],
) -> dict[str, Any]:
    per_strategy: dict[str, dict[str, Any]] = {
        strategy_id: {
            "strategy_id": strategy_id,
            "evaluations": 0,
            "signals": 0,
            "near_miss_count": 0,
            "one_gate_away_count": 0,
            "two_gate_away_count": 0,
            "market_logic_reject_count": 0,
            "infrastructure_safety_reject_count": 0,
            "top_failed_predicates": [],
            "ranked_blockers": [],
            "latest_decision": None,
            "latest_failed_predicates": [],
            "recommended_remediation_classification": "KEEP_AS_IS",
        }
        for strategy_id in strategy_ids
    }
    failed_counter_by_strategy: dict[str, Counter[str]] = defaultdict(Counter)
    near_miss_examples: list[dict[str, Any]] = []

    for event in loop_events:
        for row in _as_list(event.get("per_strategy")):
            if not isinstance(row, Mapping):
                continue
            strategy_id = str(row.get("strategy_id") or "").strip()
            if not strategy_id:
                continue
            entry = per_strategy.setdefault(strategy_id, _blank_strategy_rejection(strategy_id))
            entry["evaluations"] += 1
            if row.get("signal_emitted") or str(row.get("decision") or "").upper() == "SIGNAL":
                entry["signals"] += 1
            failed = [str(item) for item in _as_list(row.get("failed_predicates")) if str(item)]
            entry["latest_decision"] = row.get("decision") or row.get("strategy_runtime_verdict")
            entry["latest_failed_predicates"] = failed
            failed_counter_by_strategy[strategy_id].update(failed)
            reject_type = _reject_type(failed, row)
            if reject_type == "INFRASTRUCTURE_OR_SAFETY_REJECT":
                entry["infrastructure_safety_reject_count"] += 1
            else:
                entry["market_logic_reject_count"] += 1
            if 0 < len(failed) <= 2:
                entry["near_miss_count"] += 1
                if len(failed) == 1:
                    entry["one_gate_away_count"] += 1
                if len(failed) == 2:
                    entry["two_gate_away_count"] += 1
                near_miss_examples.append(
                    {
                        "strategy_id": strategy_id,
                        "generated_at": event.get("generated_at"),
                        "failed_predicates": failed,
                        "gate_distance": len(failed),
                        "hypothetical_direction": row.get("signal_direction"),
                        "input_event_path": row.get("input_event_path"),
                        "reject_type": reject_type,
                    }
                )

    for row in _as_list(no_signal_rollup.get("evaluated_decision_bar_records")):
        if not isinstance(row, Mapping):
            continue
        strategy_id = str(row.get("strategy_id") or "").strip()
        if not strategy_id:
            continue
        entry = per_strategy.setdefault(strategy_id, _blank_strategy_rejection(strategy_id))
        entry["evaluations"] += 1
        failed = [str(item) for item in _as_list(row.get("failed_predicates")) if str(item)]
        failed_counter_by_strategy[strategy_id].update(failed)
        count = _int(row.get("failed_predicates_count"), len(failed))
        if 0 < count <= 2:
            entry["near_miss_count"] += 1
            entry["one_gate_away_count"] += 1 if count == 1 else 0
            entry["two_gate_away_count"] += 1 if count == 2 else 0
        reject_type = _reject_type(failed, row)
        if reject_type == "INFRASTRUCTURE_OR_SAFETY_REJECT":
            entry["infrastructure_safety_reject_count"] += 1
        else:
            entry["market_logic_reject_count"] += 1

    asian_drift = p0_near_miss_audit.get("asian_drift") if isinstance(p0_near_miss_audit, Mapping) else {}
    if isinstance(asian_drift, Mapping):
        late_join_count = _int(asian_drift.get("late_join_strong_drift_count"), 0)
        if late_join_count:
            entry = per_strategy.setdefault("asian_drift_v1", _blank_strategy_rejection("asian_drift_v1"))
            entry["near_miss_count"] += late_join_count
            entry["one_gate_away_count"] += late_join_count
            entry["market_logic_reject_count"] += late_join_count
            failed_counter_by_strategy["asian_drift_v1"].update({"missing_18_00_et_session_anchor_context": late_join_count})
            for item in _as_list(asian_drift.get("late_join_events")):
                if isinstance(item, Mapping):
                    near_miss_examples.append(
                        {
                            "strategy_id": "asian_drift_v1",
                            "generated_at": item.get("generated_at"),
                            "failed_predicates": ["missing_18_00_et_session_anchor_context"],
                            "gate_distance": 1,
                            "hypothetical_direction": "LONG",
                            "hypothetical_score": item.get("score"),
                            "reject_type": "MARKET_LOGIC_REJECT",
                            "source": "p0_near_miss_predicate_pressure_audit",
                        }
                    )

    ranked_blockers = Counter()
    for strategy_id, entry in per_strategy.items():
        top_failed = failed_counter_by_strategy[strategy_id].most_common(10)
        entry["top_failed_predicates"] = [{"predicate": key, "count": value} for key, value in top_failed]
        entry["ranked_blockers"] = _rank_strategy_blockers(strategy_id, top_failed)
        entry["recommended_remediation_classification"] = _strategy_remediation_classification(entry)
        for blocker in entry["ranked_blockers"]:
            ranked_blockers[blocker["blocker"]] += int(blocker["count"])

    total_evaluations = sum(_int(item.get("evaluations"), 0) for item in per_strategy.values())
    return {
        "taxonomy_version": "track_b_rejection_taxonomy_v1",
        "total_evaluations": total_evaluations,
        "strategy_count": len(per_strategy),
        "total_near_misses": sum(_int(item.get("near_miss_count"), 0) for item in per_strategy.values()),
        "total_one_gate_away": sum(_int(item.get("one_gate_away_count"), 0) for item in per_strategy.values()),
        "total_two_gate_away": sum(_int(item.get("two_gate_away_count"), 0) for item in per_strategy.values()),
        "market_logic_rejects": sum(_int(item.get("market_logic_reject_count"), 0) for item in per_strategy.values()),
        "infrastructure_safety_rejects": sum(
            _int(item.get("infrastructure_safety_reject_count"), 0) for item in per_strategy.values()
        ),
        "ranked_blockers": [
            {"blocker": key, "count": value} for key, value in ranked_blockers.most_common(12)
        ],
        "per_strategy": sorted(per_strategy.values(), key=lambda item: str(item["strategy_id"])),
        "near_miss_examples": near_miss_examples[-50:],
    }


def _build_forward_outcome_simulation(
    *,
    near_misses: Sequence[Mapping[str, Any]],
    p0_near_miss_audit: Mapping[str, Any],
    late_join_shadow: Mapping[str, Any],
    gap_drift_shadow: Mapping[str, Any],
    candle_index: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for item in near_misses[-30:]:
        strategy_id = str(item.get("strategy_id") or "")
        symbol = _symbol_for_strategy(strategy_id)
        candidates.append(
            {
                "strategy_id": strategy_id,
                "symbol": symbol,
                "timestamp": item.get("generated_at"),
                "direction": item.get("hypothetical_direction") or _default_direction(strategy_id),
                "source": item.get("source") or "near_miss_attribution",
                "score": item.get("hypothetical_score"),
            }
        )

    for shadow_payload, source in ((late_join_shadow, "late_join_asian_drift_shadow"), (gap_drift_shadow, "gap_drift_shadow")):
        if not isinstance(shadow_payload, Mapping) or not shadow_payload:
            continue
        if shadow_payload.get("shadow_classification") and "NO_CANDIDATE" not in str(shadow_payload.get("shadow_classification")):
            candidates.append(
                {
                    "strategy_id": shadow_payload.get("strategy_family") or shadow_payload.get("strategy_id") or source,
                    "symbol": shadow_payload.get("symbol") or "MGC",
                    "timestamp": shadow_payload.get("source_candle_timestamp") or shadow_payload.get("generated_at"),
                    "direction": shadow_payload.get("hypothetical_direction"),
                    "source": source,
                    "score": shadow_payload.get("hypothetical_score"),
                }
            )

    simulations = []
    for candidate in candidates:
        symbol = str(candidate.get("symbol") or _symbol_for_strategy(str(candidate.get("strategy_id") or "")))
        direction = str(candidate.get("direction") or "LONG").upper()
        candles = candle_index.get(symbol) or []
        simulation = simulate_forward_outcome(
            candles=candles,
            timestamp=str(candidate.get("timestamp") or ""),
            direction=direction,
        )
        simulations.append({**candidate, **simulation})

    class_counts = Counter(str(item.get("outcome_classification") or "UNCLEAR") for item in simulations)
    asian = p0_near_miss_audit.get("asian_drift") if isinstance(p0_near_miss_audit, Mapping) else {}
    return {
        "simulator_version": "track_b_forward_rejected_candidate_outcome_v1",
        "candidate_count": len(candidates),
        "simulated_count": len(simulations),
        "outcome_counts": dict(class_counts),
        "late_join_strong_drift_count": _int(asian.get("late_join_strong_drift_count"), 0)
        if isinstance(asian, Mapping)
        else 0,
        "max_late_join_score": asian.get("max_drift_score") if isinstance(asian, Mapping) else None,
        "simulations": simulations[-50:],
        "interpretation": _forward_interpretation(class_counts),
    }


def simulate_forward_outcome(
    *,
    candles: Sequence[Mapping[str, Any]],
    timestamp: str,
    direction: str,
    entry_price: float | None = None,
) -> dict[str, Any]:
    """Return a bounded MFE/MAE simulation for a rejected candidate."""

    ordered = sorted((_normalise_candle(item) for item in candles), key=lambda item: str(item.get("timestamp") or ""))
    ordered = [item for item in ordered if item.get("timestamp") and item.get("close") is not None]
    if not ordered:
        return {"outcome_classification": "UNCLEAR", "reason": "no_candles_available"}
    start_index = _first_candle_index_at_or_after(ordered, timestamp)
    if start_index is None:
        return {"outcome_classification": "UNCLEAR", "reason": "candidate_timestamp_outside_candle_window"}
    entry = float(entry_price if entry_price is not None else ordered[start_index]["close"])
    forward = ordered[start_index + 1 : start_index + 13]
    if not forward:
        return {"outcome_classification": "UNCLEAR", "reason": "no_forward_candles_available", "entry_price": entry}
    long_side = str(direction or "LONG").upper() not in {"SHORT", "SELL"}
    horizons = {5: 1, 15: 3, 30: 6, 60: 12}
    horizon_results: dict[str, dict[str, Any]] = {}
    best_mfe = 0.0
    worst_mae = 0.0
    for minutes, count in horizons.items():
        rows = forward[:count]
        if not rows:
            continue
        highs = [float(row["high"]) for row in rows if row.get("high") is not None]
        lows = [float(row["low"]) for row in rows if row.get("low") is not None]
        closes = [float(row["close"]) for row in rows if row.get("close") is not None]
        if not highs or not lows or not closes:
            continue
        if long_side:
            mfe = max(highs) - entry
            mae = min(lows) - entry
            terminal = closes[-1] - entry
        else:
            mfe = entry - min(lows)
            mae = entry - max(highs)
            terminal = entry - closes[-1]
        best_mfe = max(best_mfe, mfe)
        worst_mae = min(worst_mae, mae)
        horizon_results[f"{minutes}m"] = {
            "mfe": round(mfe, 6),
            "mae": round(mae, 6),
            "terminal": round(terminal, 6),
            "bars_observed": len(rows),
        }
    classification = _classify_forward(best_mfe, worst_mae, horizon_results)
    return {
        "entry_price": entry,
        "mfe": round(best_mfe, 6),
        "mae": round(worst_mae, 6),
        "horizons": horizon_results,
        "outcome_classification": classification,
    }


def _build_regime_coverage(
    strategy_ids: Sequence[str],
    candle_index: Mapping[str, Sequence[Mapping[str, Any]]],
    p0_near_miss_audit: Mapping[str, Any],
    gap_drift_shadow: Mapping[str, Any],
) -> dict[str, Any]:
    observed = {
        symbol: _classify_regime(candles)
        for symbol, candles in candle_index.items()
        if candles
    }
    per_strategy = []
    uncovered_counter: Counter[str] = Counter()
    for strategy_id in strategy_ids:
        intended = _intended_regimes(strategy_id)
        symbol = _symbol_for_strategy(strategy_id)
        actual = observed.get(symbol, {"primary_regime": "UNKNOWN", "secondary_regimes": []})
        actual_regimes = {str(actual.get("primary_regime"))} | {str(item) for item in _as_list(actual.get("secondary_regimes"))}
        covered = bool(actual_regimes & set(intended))
        if not covered:
            uncovered_counter[str(actual.get("primary_regime") or "UNKNOWN")] += 1
        per_strategy.append(
            {
                "strategy_id": strategy_id,
                "symbol": symbol,
                "intended_regimes": intended,
                "observed_primary_regime": actual.get("primary_regime"),
                "observed_secondary_regimes": actual.get("secondary_regimes"),
                "covered_current_regime": covered,
                "coverage_gap": None if covered else f"{strategy_id} is not designed for {actual.get('primary_regime')}.",
            }
        )
    audit_overall = p0_near_miss_audit.get("overall") if isinstance(p0_near_miss_audit, Mapping) else {}
    return {
        "regime_taxonomy_version": "track_b_rough_live_regime_v1",
        "observed_by_symbol": observed,
        "per_strategy": per_strategy,
        "uncovered_regime_counts": dict(uncovered_counter),
        "p0_prior_classifications": audit_overall.get("classifications") if isinstance(audit_overall, Mapping) else [],
        "gap_drift_shadow_classification": gap_drift_shadow.get("shadow_classification")
        if isinstance(gap_drift_shadow, Mapping)
        else None,
        "interpretation": _regime_interpretation(uncovered_counter, audit_overall),
    }


def _build_exit_position_attribution(
    trade_rows: Sequence[Mapping[str, Any]],
    trade_summary: Mapping[str, Any],
    candle_index: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    closed_rows = [row for row in trade_rows if str(row.get("final_lifecycle_state") or row.get("lifecycle_state") or "").upper() == "CLOSED_FLAT" or row.get("exit_fill_confirmed")]
    reports = []
    class_counts: Counter[str] = Counter()
    for row in closed_rows[-75:]:
        symbol = _symbol_from_contract(str(row.get("contract_key") or row.get("symbol") or row.get("local_symbol") or ""))
        direction = "SHORT" if str(row.get("action") or row.get("entry_action") or "").upper() == "SELL" else "LONG"
        entry_price = _float(row.get("entry_fill_price"))
        sim = {}
        if entry_price is not None:
            sim = simulate_forward_outcome(
                candles=candle_index.get(symbol) or [],
                timestamp=str(row.get("entry_timestamp") or row.get("created_at") or ""),
                direction=direction,
                entry_price=entry_price,
            )
        classification = _classify_exit_attribution(row, sim)
        class_counts[classification] += 1
        reports.append(
            {
                "strategy_id": row.get("strategy_id"),
                "lane_id": row.get("lane_id"),
                "contract_key": row.get("contract_key"),
                "entry_timestamp": row.get("entry_timestamp"),
                "exit_timestamp": row.get("exit_timestamp"),
                "entry_fill_price": row.get("entry_fill_price"),
                "exit_fill_price": row.get("exit_fill_price"),
                "entry_order_id": row.get("entry_order_id"),
                "exit_order_id": row.get("exit_order_id"),
                "close_bridge_classification": row.get("close_bridge_classification"),
                "forward_mfe": sim.get("mfe"),
                "forward_mae": sim.get("mae"),
                "attribution_classification": classification,
                "local_regime_thesis": _local_exit_regime_thesis(classification, sim),
            }
        )
    return {
        "broker_effect_trade_count": trade_summary.get("broker_backed_trade_count")
        or trade_summary.get("trade_count")
        or len(trade_rows),
        "closed_trade_count": len(closed_rows),
        "attribution_counts": dict(class_counts),
        "recent_trade_attributions": reports[-25:],
        "top_exit_management_issues": _exit_issue_summary(reports),
    }


def _build_shadow_lane_plan(
    rejection: Mapping[str, Any],
    regime: Mapping[str, Any],
    p0_near_miss_audit: Mapping[str, Any],
    p0_near_miss_shadow: Mapping[str, Any],
) -> dict[str, Any]:
    asian = p0_near_miss_audit.get("asian_drift") if isinstance(p0_near_miss_audit, Mapping) else {}
    variants = []
    if isinstance(asian, Mapping) and _int(asian.get("late_join_strong_drift_count"), 0) > 0:
        variants.append(
            {
                "shadow_variant_id": "ASIAN_DRIFT_LATE_JOIN_B_GRADE_SHADOW_V1",
                "target_strategy_family": "asian_drift",
                "reason": "Strong drift appeared after missing required 18:00 ET anchor context.",
                "submit_allowed": False,
                "expected_impact": "HIGH",
            }
        )
    observed = regime.get("observed_by_symbol") if isinstance(regime, Mapping) else {}
    if isinstance(observed, Mapping) and any(
        str(item.get("primary_regime")) in {"GAP_AND_GO_CONTINUATION", "TREND_PARTICIPATION", "QUIET_DRIFT"}
        for item in observed.values()
        if isinstance(item, Mapping)
    ):
        variants.append(
            {
                "shadow_variant_id": "GAP_DRIFT_CONTINUATION_B_GRADE_SHADOW_V1",
                "target_strategy_family": "gap_drift_continuation",
                "reason": "Live regimes include continuation/drift states not well-covered by snap-turn and retest rules.",
                "submit_allowed": False,
                "expected_impact": "HIGH",
            }
        )
    if _int(rejection.get("total_two_gate_away"), 0) > 0:
        variants.append(
            {
                "shadow_variant_id": "RELAXED_BREAKOUT_WITHOUT_RETEST_B_GRADE_SHADOW_V1",
                "target_strategy_family": "breakout_retest",
                "reason": "Two-gate-away breakout/retest rejects can be tracked without weakening A-grade rules.",
                "submit_allowed": False,
                "expected_impact": "MEDIUM",
            }
        )
    variants.append(
        {
            "shadow_variant_id": "SNAP_TURN_B_GRADE_PRESSURE_SHADOW_V1",
            "target_strategy_family": "snap_turn",
            "reason": "Snap-turn no-trades were often regime-appropriate; collect B-grade pressure only during reversal regimes.",
            "submit_allowed": False,
            "expected_impact": "MEDIUM",
        }
    )
    return {
        "phase": "PHASE_2_SHADOW_ONLY_NOT_LIVE_AUTHORITY",
        "existing_p0_near_miss_shadow_classification": p0_near_miss_shadow.get("shadow_classification")
        if isinstance(p0_near_miss_shadow, Mapping)
        else None,
        "generated_shadow_variants": variants,
        "promotion_policy": "Require forward-outcome evidence and explicit tollgate before any B-grade shadow becomes live A-grade authority.",
        "dry_run_only": True,
        "submit_allowed": False,
        "not_order_authority": True,
    }


def _build_remediation_board(
    rejection: Mapping[str, Any],
    forward: Mapping[str, Any],
    regime: Mapping[str, Any],
    exit_attribution: Mapping[str, Any],
    shadow: Mapping[str, Any],
) -> list[dict[str, Any]]:
    board: list[dict[str, Any]] = []
    ranked_blockers = _as_list(rejection.get("ranked_blockers"))
    if any("anchor" in str(item.get("blocker", "")) for item in ranked_blockers if isinstance(item, Mapping)):
        board.append(
            _remediation(
                "ADD_SHADOW_VARIANT",
                "Asian Drift late-join B-grade shadow with forward outcome tracking",
                "HIGH",
                "Near-miss audit shows missing anchor can be sole blocker during strong drift.",
                ["forward_outcome_sample", "session_anchor_policy_review"],
                "Keep diagnostic-only; do not grant submit authority from missing-anchor state.",
            )
        )
    if str(regime.get("interpretation", "")).find("continuation") >= 0 or "GAP_AND_GO_CONTINUATION" in json.dumps(regime):
        board.append(
            _remediation(
                "ADD_NEW_REGIME_FAMILY",
                "Gap/drift continuation strategy family, shadow first",
                "HIGH",
                "Current roster is heavy on snap-turn/retest/session anchors and misses gap-and-go continuation.",
                ["phase1_5m_regime_labels", "B_grade_shadow_outcomes"],
                "Continuation entries can chase; require MFE/MAE and exit-profile evidence.",
            )
        )
    if _int(rejection.get("infrastructure_safety_rejects"), 0) > 0:
        board.append(
            _remediation(
                "FIX_INFRA_SUPPRESSION",
                "Separate infrastructure/safety rejects from market-logic rejects in status",
                "MEDIUM",
                "Infrastructure rejects are present in attribution counters.",
                ["control_plane_safe_state_current"],
                "Do not bypass Control Plane or Safe-State.",
            )
        )
    if exit_attribution.get("top_exit_management_issues"):
        board.append(
            _remediation(
                "IMPROVE_EXIT_PROFILE",
                "Upgrade exit profile evaluation and order-management attribution",
                "HIGH",
                "Broker-effect trades show exit/order-management evidence gaps and timed-exit roughness.",
                ["managed_exit_idempotency", "modify_in_place_guardian", "continuation_exit_history"],
                "Exit improvements must stay in exit roster/profile architecture.",
            )
        )
    if _int(rejection.get("total_two_gate_away"), 0) > 0:
        board.append(
            _remediation(
                "TUNE_PREDICATE",
                "Track two-gate-away session and retest predicates as B-grade shadows",
                "MEDIUM",
                "Some strategies repeatedly miss by a small number of noncritical predicates.",
                ["near_miss_forward_outcomes"],
                "No live threshold changes until shadows show positive expectancy.",
            )
        )
    for variant in _as_list(shadow.get("generated_shadow_variants")):
        if isinstance(variant, Mapping) and not any(item["title"] == variant.get("shadow_variant_id") for item in board):
            board.append(
                _remediation(
                    "ADD_SHADOW_VARIANT",
                    str(variant.get("shadow_variant_id")),
                    str(variant.get("expected_impact") or "MEDIUM"),
                    str(variant.get("reason") or "Shadow variant recommended by generator."),
                    ["shadow_event_history", "forward_outcome_simulator"],
                    "Shadow only; no submit authority.",
                )
            )
    if not board:
        board.append(
            _remediation(
                "KEEP_AS_IS",
                "Continue guarded PAPER observation until stronger evidence appears",
                "LOW",
                "No dominant underperformance cause was measurable from current artifacts.",
                ["more_live_evaluations"],
                "Risk is opportunity cost from waiting.",
            )
        )
    priority_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    return sorted(board, key=lambda item: (priority_order.get(str(item.get("expected_impact")), 9), str(item.get("title"))))


def _top_root_causes(
    rejection: Mapping[str, Any],
    forward: Mapping[str, Any],
    regime: Mapping[str, Any],
    exit_attribution: Mapping[str, Any],
    p0_near_miss_audit: Mapping[str, Any],
) -> list[dict[str, Any]]:
    causes: list[dict[str, Any]] = []
    asian = p0_near_miss_audit.get("asian_drift") if isinstance(p0_near_miss_audit, Mapping) else {}
    if isinstance(asian, Mapping) and _int(asian.get("late_join_strong_drift_count"), 0) > 0:
        causes.append(
            {
                "root_cause": "ANCHOR_POLICY_TOO_STRICT_FOR_LATE_JOIN_DRIFT",
                "evidence": f"{_int(asian.get('late_join_strong_drift_count'), 0)} strong late-join drift diagnostics; max score {asian.get('max_drift_score')}.",
                "expected_impact": "HIGH",
            }
        )
    if regime.get("uncovered_regime_counts"):
        causes.append(
            {
                "root_cause": "REGIME_COVERAGE_GAP",
                "evidence": f"Observed regimes not covered by some strategies: {regime.get('uncovered_regime_counts')}.",
                "expected_impact": "HIGH",
            }
        )
    if _int(rejection.get("total_near_misses"), 0) > 0:
        causes.append(
            {
                "root_cause": "STRICT_PREDICATE_PRESSURE",
                "evidence": f"{rejection.get('total_near_misses')} one/two-gate-away or special near-miss candidates observed.",
                "expected_impact": "MEDIUM",
            }
        )
    if exit_attribution.get("top_exit_management_issues"):
        causes.append(
            {
                "root_cause": "EXIT_AND_ORDER_MANAGEMENT_ATTRIBUTION_GAP",
                "evidence": str(exit_attribution.get("top_exit_management_issues")),
                "expected_impact": "HIGH",
            }
        )
    if _int(rejection.get("infrastructure_safety_rejects"), 0) > 0:
        causes.append(
            {
                "root_cause": "INFRASTRUCTURE_SUPPRESSION",
                "evidence": f"{rejection.get('infrastructure_safety_rejects')} infrastructure/safety rejects counted.",
                "expected_impact": "MEDIUM",
            }
        )
    return causes[:6]


def _build_high_quality_strategy_promotion_audit(
    *,
    repo_root: Path,
    roster_payload: Mapping[str, Any],
    strategy_ids: Sequence[str],
    entry_acceptance_summary: Mapping[str, Any],
    atp_production_track_review: Mapping[str, Any],
    entry_acceptance_summary_path: Path,
    entry_acceptance_decision_path: Path,
    atp_production_track_review_path: Path,
) -> dict[str, Any]:
    active_roster = _active_roster_ids(roster_payload, strategy_ids)
    entry_strategy_id = "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    acceptance_counts = (
        entry_acceptance_summary.get("acceptance_class_counts")
        if isinstance(entry_acceptance_summary.get("acceptance_class_counts"), Mapping)
        else {}
    )
    aggregate = (
        entry_acceptance_summary.get("aggregate_metrics")
        if isinstance(entry_acceptance_summary.get("aggregate_metrics"), Mapping)
        else {}
    )
    baseline_24 = _metric_bucket(aggregate, "baseline", "24b_next_bar_open_0_5_cost")
    near_24 = _metric_bucket(aggregate, "near_gte_0_80", "24b_next_bar_open_0_5_cost")
    near_12 = _metric_bucket(aggregate, "near_gte_0_80", "12b_next_bar_open_0_5_cost")
    near_count = _int(entry_acceptance_summary.get("near_gte_0_80_episodes"), 0)
    baseline_count = _int(entry_acceptance_summary.get("baseline_episodes"), 0)
    near_ratio = _float(entry_acceptance_summary.get("near_to_baseline_episode_ratio"))

    atp_configs = _discover_atp_companion_configs(repo_root)
    active_atp_ids = sorted(
        {
            item
            for item in active_roster
            if "ATP" in item.upper() or "ACTIVE_TREND_PARTICIPATION" in item.upper()
        }
    )
    atp_monitor = (
        atp_production_track_review.get("benchmark_vs_package_monitor")
        if isinstance(atp_production_track_review.get("benchmark_vs_package_monitor"), Mapping)
        else {}
    )
    historical = (
        atp_monitor.get("historical_expectation")
        if isinstance(atp_monitor.get("historical_expectation"), Mapping)
        else {}
    )

    entry_status = (
        "TRACK_B_GUARDED_PAPER_ACTIVE_EXACT_BASELINE"
        if entry_strategy_id in active_roster
        else "TRACK_B_EXACT_BASELINE_NOT_ACTIVE"
    )
    near_status = "RESEARCH_ONLY_PARKED_NOT_RUNTIME_AUTHORITY"
    if near_24 and _float(near_24.get("average_return")) is not None and _float(near_24.get("average_return")) >= 0:
        near_status = "RESEARCH_ONLY_REQUIRES_PROMOTION_REVIEW"

    exact_family = {
        "family_id": "TRACK_B_ENTRY_ACCEPTANCE_ASIA_EARLY_BREAKOUT_RETEST_HOLD",
        "representative_strategy_id": entry_strategy_id,
        "originating_research_artifact": str(entry_acceptance_summary_path),
        "promotion_decision_artifact": str(entry_acceptance_decision_path),
        "current_status": entry_status,
        "guarded_paper_active": entry_strategy_id in active_roster,
        "large_sample_threshold_work_status": near_status,
        "exact_baseline_episode_count": baseline_count,
        "near_gte_0_80_episode_count": near_count,
        "near_to_baseline_episode_ratio": near_ratio,
        "acceptance_class_counts": dict(acceptance_counts),
        "baseline_24b_metrics": baseline_24,
        "near_gte_0_80_24b_metrics": near_24,
        "near_gte_0_80_12b_metrics": near_12,
        "gating_reason_for_near_expansion": (
            "NEAR >=0.80 expanded trade supply materially but remained negative after cost in the full-history "
            "research summary; it is parked and must remain non-authoritative until forward/shadow evidence changes."
        ),
    }

    atp_family = {
        "family_id": "ATP_COMPANION_V1_ACTIVE_TREND_PARTICIPATION",
        "originating_research_artifact": "src/mgc_v05l/research/trend_participation/atp_promotion_add_review.py",
        "production_track_review_artifact": str(atp_production_track_review_path),
        "current_status": "NOT_ACTIVE_IN_TRACK_B_GUARDED_PAPER_RUNTIME"
        if not active_atp_ids
        else "TRACK_B_GUARDED_PAPER_ACTIVE",
        "guarded_paper_active_strategy_ids": active_atp_ids,
        "discovered_candidate_config_count": len(atp_configs),
        "discovered_candidate_configs": atp_configs[:20],
        "historical_expectation_summary": {
            "benchmark_mgc": historical.get("benchmark_mgc") if isinstance(historical, Mapping) else {},
            "raw_gc_candidate": historical.get("raw_gc_candidate") if isinstance(historical, Mapping) else {},
            "package_exact": historical.get("package_exact") if isinstance(historical, Mapping) else {},
        },
        "promotion_lineage_status": (
            "Probationary/production-track ATP configs exist, including at least one non_approved=false package, "
            "but no ATP strategy id is present in the current guarded PAPER roster."
        ),
    }

    return {
        "audit_version": "track_b_high_quality_strategy_promotion_audit_v1",
        "primary_large_sample_family_match": "TRACK_B_ENTRY_ACCEPTANCE_ASIA_EARLY_BREAKOUT_RETEST_HOLD",
        "primary_finding": (
            "The exact Entry Acceptance baseline is represented in guarded PAPER via "
            f"{entry_strategy_id}; the broad NEAR >=0.80 candidate universe is not active and remains research-only."
        ),
        "active_guarded_paper_roster_count": len(active_roster),
        "active_guarded_paper_roster": sorted(active_roster),
        "families": [exact_family, atp_family],
        "not_live_authority": True,
        "submit_allowed": False,
        "broker_mutation_allowed": False,
    }


def _build_threshold_pressure_analysis(promotion_audit: Mapping[str, Any]) -> dict[str, Any]:
    exact_family = {}
    for item in _as_list(promotion_audit.get("families")):
        if isinstance(item, Mapping) and item.get("family_id") == "TRACK_B_ENTRY_ACCEPTANCE_ASIA_EARLY_BREAKOUT_RETEST_HOLD":
            exact_family = dict(item)
            break
    baseline_24 = exact_family.get("baseline_24b_metrics") if isinstance(exact_family.get("baseline_24b_metrics"), Mapping) else {}
    near_24 = exact_family.get("near_gte_0_80_24b_metrics") if isinstance(exact_family.get("near_gte_0_80_24b_metrics"), Mapping) else {}
    baseline_count = _int(exact_family.get("exact_baseline_episode_count"), 0)
    near_count = _int(exact_family.get("near_gte_0_80_episode_count"), 0)
    ratio = _float(exact_family.get("near_to_baseline_episode_ratio"))

    buckets = [
        {
            "quality_bucket": "AUTHORITATIVE_EXACT_BASELINE",
            "approx_numeric_range": "strict structural rule; not equivalent to a numeric threshold",
            "candidate_count": baseline_count,
            "trade_frequency_impact_vs_exact": "1.0x",
            "forward_outcome_quality": _quality_summary(baseline_24),
            "mfe_mae_tendency": _mfe_mae_summary(baseline_24),
            "expected_degradation_vs_80_plus": "None; this is the retained baseline.",
            "runtime_status": exact_family.get("current_status"),
            "recommendation": "KEEP_80_AUTHORITATIVE",
        },
        {
            "quality_bucket": "80_89_OR_NEAR_GTE_0_80",
            "approx_numeric_range": ">=0.80 near-match universe",
            "candidate_count": near_count,
            "trade_frequency_impact_vs_exact": f"{round(ratio, 2)}x" if ratio is not None else None,
            "forward_outcome_quality": _quality_summary(near_24),
            "mfe_mae_tendency": _mfe_mae_summary(near_24),
            "expected_degradation_vs_80_plus": "Already degraded versus exact baseline; average return and PF are below live promotion thresholds.",
            "runtime_status": "RESEARCH_ONLY_PARKED",
            "recommendation": "DO_NOT_LOWER_AUTHORITATIVE_THRESHOLD",
        },
        {
            "quality_bucket": "70_79",
            "approx_numeric_range": "0.70-0.79",
            "candidate_count": None,
            "trade_frequency_impact_vs_exact": "Expected to exceed NEAR >=0.80 supply, but no current bucket artifact was found.",
            "forward_outcome_quality": "UNMEASURED_IN_CURRENT_ARTIFACTS",
            "mfe_mae_tendency": "UNMEASURED_IN_CURRENT_ARTIFACTS",
            "expected_degradation_vs_80_plus": "Likely worse unless a specific regime filter isolates positive expectancy.",
            "runtime_status": "NOT_RUNTIME_ELIGIBLE",
            "recommendation": "ADD_70_79_SHADOW_ONLY_IF_BUCKET_INSTRUMENTATION_EXISTS",
        },
        {
            "quality_bucket": "60_69",
            "approx_numeric_range": "0.60-0.69",
            "candidate_count": None,
            "trade_frequency_impact_vs_exact": "Unknown and likely large.",
            "forward_outcome_quality": "UNMEASURED_AND_LOW_PRIOR",
            "mfe_mae_tendency": "UNMEASURED_IN_CURRENT_ARTIFACTS",
            "expected_degradation_vs_80_plus": "High expected degradation.",
            "runtime_status": "NOT_RUNTIME_ELIGIBLE",
            "recommendation": "NO_CHANGE",
        },
    ]

    return {
        "analysis_version": "track_b_threshold_pressure_v1",
        "authoritative_threshold_recommendation": "KEEP_80_AUTHORITATIVE",
        "shadow_rollout_recommendation": "ADD_70_79_SHADOW_INSTRUMENTATION_ONLY_AFTER_80_PLUS_REPAIR_OR_SEGMENTATION",
        "evidence_summary": (
            "The broad >=0.80 near-match universe produced about "
            f"{round(ratio, 2)}x more episodes than exact baseline, but the available full-history proxy is negative "
            "after cost. That argues for structured shadows and segmentation, not live threshold lowering."
            if ratio is not None
            else "Current artifacts do not provide enough threshold distribution detail for live threshold lowering."
        ),
        "expected_trade_frequency_increase": {
            "near_gte_0_80_vs_exact": ratio,
            "near_gte_0_80_episode_count": near_count,
            "exact_baseline_episode_count": baseline_count,
            "70_79_vs_exact": None,
        },
        "expected_risk_degradation": {
            "near_gte_0_80": {
                "average_return": near_24.get("average_return"),
                "profit_factor_proxy": near_24.get("profit_factor_proxy"),
                "win_rate": near_24.get("win_rate"),
            },
            "70_79": "UNKNOWN_HIGH_UNTIL_MEASURED",
        },
        "buckets": buckets,
        "recommendation_matrix": [
            {
                "recommendation": "KEEP_80_AUTHORITATIVE",
                "status": "SUPPORTED",
                "reason": "Exact baseline is active and positive in the available full-history summary.",
            },
            {
                "recommendation": "ADD_70_79_SHADOW",
                "status": "CONDITIONAL_DIAGNOSTIC_ONLY",
                "reason": "Useful only to measure missed regimes; not supported as live authority by current evidence.",
            },
            {
                "recommendation": "ADD_LATE_JOIN_VARIANT",
                "status": "SUPPORTED_AS_SHADOW",
                "reason": "Current underperformance review already flags late-join drift as a top missed-regime source.",
            },
            {
                "recommendation": "ADD_CONTINUATION_REGIME_FAMILY",
                "status": "SUPPORTED_AS_SHADOW",
                "reason": "Continuation/drift regimes are not fully covered by the current active roster.",
            },
            {
                "recommendation": "RELAX_SPECIFIC_PREDICATE",
                "status": "NOT_YET_FOR_LIVE",
                "reason": "Predicate relaxation needs bucketed forward-outcome evidence first.",
            },
            {
                "recommendation": "NO_CHANGE",
                "status": "SUPPORTED_FOR_LIVE_AUTHORITY",
                "reason": "Do not change live rules until shadow evidence clears the tollgate.",
            },
        ],
        "not_live_authority": True,
        "submit_allowed": False,
        "broker_mutation_allowed": False,
    }


def _missed_opportunity_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not payload:
        return {
            "classification": "MISSED_OPPORTUNITY_DISCOVERY_NOT_AVAILABLE",
            "integration_note": "Run track_b_missed_opportunity_discovery to populate ATP shadows and general near-miss scoring.",
            "submit_allowed": False,
            "broker_mutation_allowed": False,
        }
    return {
        "classification": payload.get("classification"),
        "atp_shadow_summary": payload.get("atp_shadow_summary") or {},
        "atp_lifecycle_mapping_shadow_summary": payload.get("atp_lifecycle_mapping_shadow_summary") or {},
        "near_miss_scoring_summary": payload.get("near_miss_scoring_summary") or {},
        "forward_outcome_summary": payload.get("forward_outcome_summary") or {},
        "shadow_candidate_maturation_summary": payload.get("shadow_candidate_maturation_summary") or {},
        "timestamp_locked_forward_evidence_summary": payload.get("timestamp_locked_forward_evidence_summary") or {},
        "b_grade_missed_winner_family_rankings": payload.get("b_grade_missed_winner_family_rankings") or [],
        "persistent_shadow_families": payload.get("persistent_shadow_families") or [],
        "promotion_gate_recommendations": payload.get("promotion_gate_recommendations") or [],
        "submit_allowed": False,
        "broker_mutation_allowed": False,
        "not_order_authority": True,
    }


def _render_markdown(report: Mapping[str, Any]) -> str:
    root_causes = _as_list(report.get("top_underperformance_root_causes"))
    recommendations = _as_list(report.get("top_implementation_recommendations"))
    rejection = report.get("rejection_attribution") if isinstance(report.get("rejection_attribution"), Mapping) else {}
    regime = report.get("regime_coverage_audit") if isinstance(report.get("regime_coverage_audit"), Mapping) else {}
    exit_attr = (
        report.get("exit_position_management_attribution")
        if isinstance(report.get("exit_position_management_attribution"), Mapping)
        else {}
    )
    promotion = (
        report.get("high_quality_strategy_promotion_audit")
        if isinstance(report.get("high_quality_strategy_promotion_audit"), Mapping)
        else {}
    )
    threshold = (
        report.get("threshold_pressure_analysis")
        if isinstance(report.get("threshold_pressure_analysis"), Mapping)
        else {}
    )
    discovery = (
        report.get("missed_opportunity_discovery_layer")
        if isinstance(report.get("missed_opportunity_discovery_layer"), Mapping)
        else {}
    )
    lines = [
        "# Track B Strategy Underperformance Review v1",
        "",
        f"Generated: `{report.get('generated_at')}`",
        "",
        "This report is analysis-only. It does not change live rules, grant submit authority, mutate broker state, invoke paper_proof, or consume dashboard projections as authority.",
        "",
        "## Top Root Causes",
        "",
    ]
    if root_causes:
        for item in root_causes:
            if isinstance(item, Mapping):
                lines.append(
                    f"- **{item.get('root_cause')}** ({item.get('expected_impact')}): {item.get('evidence')}"
                )
    else:
        lines.append("- No dominant root cause was measurable from the current artifacts.")
    lines.extend(
        [
            "",
            "## Rejection And Near-Miss Attribution",
            "",
            f"- Total evaluations: `{rejection.get('total_evaluations')}`",
            f"- Total near misses: `{rejection.get('total_near_misses')}`",
            f"- One-gate-away: `{rejection.get('total_one_gate_away')}`",
            f"- Two-gate-away: `{rejection.get('total_two_gate_away')}`",
            f"- Market-logic rejects: `{rejection.get('market_logic_rejects')}`",
            f"- Infrastructure/safety rejects: `{rejection.get('infrastructure_safety_rejects')}`",
            "",
            "| Strategy | Evaluations | Near misses | Top blockers | Recommendation |",
            "| --- | ---: | ---: | --- | --- |",
        ]
    )
    for item in _as_list(rejection.get("per_strategy"))[:30]:
        if not isinstance(item, Mapping):
            continue
        top = ", ".join(str(row.get("predicate")) for row in _as_list(item.get("top_failed_predicates"))[:3] if isinstance(row, Mapping))
        lines.append(
            f"| `{item.get('strategy_id')}` | {item.get('evaluations')} | {item.get('near_miss_count')} | {top or 'none'} | `{item.get('recommended_remediation_classification')}` |"
        )
    lines.extend(
        [
            "",
            "## Regime Coverage",
            "",
            f"- Interpretation: {regime.get('interpretation')}",
            f"- Observed regimes: `{regime.get('observed_by_symbol')}`",
            "",
            "## High-Quality Promotion Audit",
            "",
            f"- Primary match: `{promotion.get('primary_large_sample_family_match')}`",
            f"- Finding: {promotion.get('primary_finding')}",
            "",
            "| Family | Status | Active | Key Evidence |",
            "| --- | --- | --- | --- |",
        ]
    )
    for item in _as_list(promotion.get("families")):
        if not isinstance(item, Mapping):
            continue
        evidence = []
        if item.get("exact_baseline_episode_count") is not None:
            evidence.append(f"exact={item.get('exact_baseline_episode_count')}")
        if item.get("near_gte_0_80_episode_count") is not None:
            evidence.append(f"near>=0.80={item.get('near_gte_0_80_episode_count')}")
        if item.get("discovered_candidate_config_count") is not None:
            evidence.append(f"configs={item.get('discovered_candidate_config_count')}")
        lines.append(
            f"| `{item.get('family_id')}` | `{item.get('current_status')}` | `{item.get('guarded_paper_active') or bool(item.get('guarded_paper_active_strategy_ids'))}` | {', '.join(evidence) or item.get('promotion_lineage_status') or ''} |"
        )
    lines.extend(
        [
            "",
            "## Threshold Pressure",
            "",
            f"- Authoritative recommendation: `{threshold.get('authoritative_threshold_recommendation')}`",
            f"- Shadow recommendation: `{threshold.get('shadow_rollout_recommendation')}`",
            f"- Evidence: {threshold.get('evidence_summary')}",
            "",
            "| Bucket | Count | Frequency Impact | Recommendation |",
            "| --- | ---: | --- | --- |",
        ]
    )
    for item in _as_list(threshold.get("buckets")):
        if not isinstance(item, Mapping):
            continue
        lines.append(
            f"| `{item.get('quality_bucket')}` | {item.get('candidate_count')} | {item.get('trade_frequency_impact_vs_exact')} | `{item.get('recommendation')}` |"
        )
    lines.extend(
        [
            "",
            "## Missed-Opportunity Discovery",
            "",
            f"- Classification: `{discovery.get('classification')}`",
            f"- ATP shadow: `{discovery.get('atp_shadow_summary')}`",
            f"- ATP lifecycle mapping shadow: `{discovery.get('atp_lifecycle_mapping_shadow_summary')}`",
            f"- Near-miss scoring: `{discovery.get('near_miss_scoring_summary')}`",
            f"- Shadow candidate maturation: `{discovery.get('shadow_candidate_maturation_summary')}`",
            f"- Timestamp-locked evidence: `{discovery.get('timestamp_locked_forward_evidence_summary')}`",
            f"- Forward outcomes: `{discovery.get('forward_outcome_summary')}`",
            f"- Persistent shadow families: `{discovery.get('persistent_shadow_families')}`",
            f"- B-grade missed-winner families: `{discovery.get('b_grade_missed_winner_family_rankings')}`",
            "",
            "## Forward Outcome Simulation",
            "",
            f"- Interpretation: {report.get('forward_outcome_simulation', {}).get('interpretation') if isinstance(report.get('forward_outcome_simulation'), Mapping) else None}",
            f"- Outcome counts: `{report.get('forward_outcome_simulation', {}).get('outcome_counts') if isinstance(report.get('forward_outcome_simulation'), Mapping) else {}}`",
            "",
            "## Exit / Position Management Attribution",
            "",
            f"- Closed trades scanned: `{exit_attr.get('closed_trade_count')}`",
            f"- Attribution counts: `{exit_attr.get('attribution_counts')}`",
            f"- Top issues: `{exit_attr.get('top_exit_management_issues')}`",
            "",
            "## Ranked Remediation Board",
            "",
            "| Action | Title | Impact | Dependencies | Risk |",
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for item in recommendations:
        if not isinstance(item, Mapping):
            continue
        deps = ", ".join(str(dep) for dep in _as_list(item.get("dependencies")))
        lines.append(
            f"| `{item.get('action')}` | {item.get('title')} | `{item.get('expected_impact')}` | {deps} | {item.get('risk')} |"
        )
    tollgate = report.get("tollgate_recommendation") if isinstance(report.get("tollgate_recommendation"), Mapping) else {}
    lines.extend(
        [
            "",
            "## Tollgate",
            "",
            f"Recommendation: `{tollgate.get('classification')}`",
            "",
        ]
    )
    for req in _as_list(tollgate.get("requirements")):
        lines.append(f"- {req}")
    lines.append("")
    return "\n".join(lines)


def _strategy_inventory(
    latest_loop: Mapping[str, Any],
    loop_events: Sequence[Mapping[str, Any]],
    roster_payload: Mapping[str, Any],
    no_signal_rollup: Mapping[str, Any],
) -> list[str]:
    ids: set[str] = set()
    for value in _as_list(latest_loop.get("enabled_strategy_ids")):
        if value:
            ids.add(str(value))
    for key in ("enabled_strategy_ids", "strategies", "enabled_strategies", "roster", "strategy_ids"):
        for item in _as_list(roster_payload.get(key)):
            if isinstance(item, Mapping):
                value = item.get("strategy_id") or item.get("id") or item.get("name")
            else:
                value = item
            if value:
                ids.add(str(value))
    for event in loop_events[-200:]:
        for item in _as_list(event.get("per_strategy")):
            if isinstance(item, Mapping) and item.get("strategy_id"):
                ids.add(str(item["strategy_id"]))
    for row in _as_list(no_signal_rollup.get("evaluated_decision_bar_records")):
        if isinstance(row, Mapping) and row.get("strategy_id"):
            ids.add(str(row["strategy_id"]))
    return sorted(ids)


def _blank_strategy_rejection(strategy_id: str) -> dict[str, Any]:
    return {
        "strategy_id": strategy_id,
        "evaluations": 0,
        "signals": 0,
        "near_miss_count": 0,
        "one_gate_away_count": 0,
        "two_gate_away_count": 0,
        "market_logic_reject_count": 0,
        "infrastructure_safety_reject_count": 0,
        "top_failed_predicates": [],
        "ranked_blockers": [],
        "latest_decision": None,
        "latest_failed_predicates": [],
        "recommended_remediation_classification": "KEEP_AS_IS",
    }


def _rank_strategy_blockers(strategy_id: str, top_failed: Sequence[tuple[str, int]]) -> list[dict[str, Any]]:
    blockers = []
    for predicate, count in top_failed[:8]:
        if "anchor" in predicate:
            blocker = "MISSING_SESSION_ANCHOR"
        elif "session" in predicate or "phase" in predicate:
            blocker = "SESSION_WINDOW_OR_PHASE_FILTER"
        elif "retest" in predicate or "breakout" in predicate:
            blocker = "BREAKOUT_RETEST_STRICTNESS"
        elif "snap" in predicate or "reversal" in predicate:
            blocker = "SNAP_TURN_REVERSAL_PREDICATE"
        elif any(token in predicate for token in INFRASTRUCTURE_TOKENS):
            blocker = "INFRASTRUCTURE_OR_SAFETY_GATE"
        else:
            blocker = "MARKET_STRUCTURE_PREDICATE"
        blockers.append({"blocker": blocker, "predicate": predicate, "count": count})
    if not blockers and "SNAP_TURN" in strategy_id:
        blockers.append({"blocker": "NO_REVERSAL_REGIME_OBSERVED", "predicate": "regime_selectivity", "count": 0})
    return blockers


def _strategy_remediation_classification(entry: Mapping[str, Any]) -> str:
    blockers = json.dumps(entry.get("ranked_blockers", []))
    if "INFRASTRUCTURE_OR_SAFETY_GATE" in blockers:
        return "FIX_INFRA_SUPPRESSION"
    if "MISSING_SESSION_ANCHOR" in blockers:
        return "ADD_SHADOW_VARIANT"
    if "BREAKOUT_RETEST_STRICTNESS" in blockers or _int(entry.get("two_gate_away_count"), 0) > 0:
        return "TUNE_PREDICATE"
    if "NO_REVERSAL_REGIME_OBSERVED" in blockers:
        return "KEEP_AS_IS"
    if _int(entry.get("near_miss_count"), 0) > 0:
        return "ADD_SHADOW_VARIANT"
    return "KEEP_AS_IS"


def _reject_type(failed: Sequence[str], row: Mapping[str, Any]) -> str:
    text = " ".join([*failed, str(row.get("decision_reason") or ""), str(row.get("suppression_reason") or "")]).lower()
    if any(token in text for token in INFRASTRUCTURE_TOKENS):
        return "INFRASTRUCTURE_OR_SAFETY_REJECT"
    if any(token in text for token in MARKET_LOGIC_TOKENS):
        return "MARKET_LOGIC_REJECT"
    return "MARKET_LOGIC_REJECT"


def _classify_forward(best_mfe: float, worst_mae: float, horizons: Mapping[str, Any]) -> str:
    terminal_values = [
        float(item.get("terminal"))
        for item in horizons.values()
        if isinstance(item, Mapping) and item.get("terminal") is not None
    ]
    terminal = terminal_values[-1] if terminal_values else 0.0
    if best_mfe >= max(1.0, abs(worst_mae) * 1.5) and terminal > 0:
        return "MISSED_WINNER"
    if worst_mae <= -max(1.0, best_mfe * 1.5) and terminal <= 0:
        return "GOOD_REJECT"
    if best_mfe <= 0 and terminal <= 0:
        return "AVOIDED_LOSER"
    return "UNCLEAR"


def _forward_interpretation(class_counts: Counter[str]) -> str:
    if class_counts.get("MISSED_WINNER", 0) > class_counts.get("GOOD_REJECT", 0):
        return "Rejected candidates include possible missed winners; keep shadows and collect more forward evidence."
    if class_counts.get("GOOD_REJECT", 0) or class_counts.get("AVOIDED_LOSER", 0):
        return "Current strict rules avoided at least some weak forward outcomes."
    return "Forward evidence is currently insufficient or outside the available candle window."


def _classify_regime(candles: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [_normalise_candle(item) for item in candles if isinstance(item, Mapping)]
    rows = [item for item in rows if item.get("open") is not None and item.get("close") is not None]
    if len(rows) < 3:
        return {"primary_regime": "UNKNOWN", "secondary_regimes": [], "reason": "insufficient_candles"}
    first = float(rows[0]["open"])
    last = float(rows[-1]["close"])
    highs = [float(row["high"]) for row in rows if row.get("high") is not None]
    lows = [float(row["low"]) for row in rows if row.get("low") is not None]
    closes = [float(row["close"]) for row in rows]
    total_range = max(highs) - min(lows) if highs and lows else 0.0
    net = last - first
    directionality = abs(net) / total_range if total_range else 0.0
    pullback_count = sum(1 for prev, cur in zip(closes, closes[1:]) if (cur - prev) * net < 0)
    secondary: list[str] = []
    if directionality >= 0.65 and abs(net) >= max(1.0, total_range * 0.45):
        primary = "GAP_AND_GO_CONTINUATION" if abs(closes[1] - closes[0]) >= total_range * 0.2 else "TREND_PARTICIPATION"
    elif directionality >= 0.45 and pullback_count <= max(1, len(rows) // 5):
        primary = "QUIET_DRIFT"
    elif directionality <= 0.2 and total_range > 0:
        primary = "CHOP_NO_TRADE"
    elif pullback_count >= max(2, len(rows) // 4):
        primary = "BREAKOUT_RETEST"
    else:
        primary = "TREND_PARTICIPATION"
    if pullback_count >= 2:
        secondary.append("BREAKOUT_RETEST")
    if directionality >= 0.45:
        secondary.append("TREND_PARTICIPATION")
    return {
        "primary_regime": primary,
        "secondary_regimes": sorted(set(secondary)),
        "bar_count": len(rows),
        "net_change": round(net, 6),
        "total_range": round(total_range, 6),
        "directionality": round(directionality, 4),
        "pullback_count": pullback_count,
        "first_timestamp": rows[0].get("timestamp"),
        "latest_timestamp": rows[-1].get("timestamp"),
    }


def _intended_regimes(strategy_id: str) -> list[str]:
    upper = strategy_id.upper()
    if "ASIAN_DRIFT" in upper or upper == "ASIAN_DRIFT_V1":
        return ["QUIET_DRIFT", "TREND_PARTICIPATION"]
    if "SNAP_TURN" in upper or "DERIVATIVE_BEAR_TURN" in upper:
        return ["SNAP_TURN_REVERSAL"]
    if "BREAKOUT" in upper or "RETEST" in upper:
        return ["BREAKOUT_RETEST", "TREND_PARTICIPATION"]
    if "PAUSE_RESUME" in upper or "PULLBACK" in upper:
        return ["TREND_PARTICIPATION", "BREAKOUT_RETEST"]
    return ["TREND_PARTICIPATION"]


def _regime_interpretation(uncovered_counter: Counter[str], audit_overall: Any) -> str:
    if "GAP_AND_GO_CONTINUATION" in uncovered_counter or "TREND_PARTICIPATION" in uncovered_counter:
        return "Current live sessions show continuation/drift pressure that the roster only partially covers."
    if isinstance(audit_overall, Mapping) and "P0_NO_TRADE_VALID_SELECTIVITY" in _as_list(audit_overall.get("classifications")):
        return "Some no-trade behavior is valid selectivity for missing reversal/retest regimes."
    return "Regime coverage is mixed; continue collecting dry-run regime labels."


def _classify_exit_attribution(row: Mapping[str, Any], sim: Mapping[str, Any]) -> str:
    close_classification = str(row.get("close_bridge_classification") or "")
    if "PERSISTENCE_GAP" in close_classification or row.get("exit_fill_confirmed") and row.get("exit_fill_price") in (None, ""):
        return "ENTRY_GOOD_ORDER_MANAGEMENT_BAD"
    mfe = _float(sim.get("mfe"))
    mae = _float(sim.get("mae"))
    if mfe is not None and mfe > 0 and row.get("exit_fill_price") in (None, ""):
        return "ENTRY_GOOD_EXIT_BAD"
    if mfe is not None and mae is not None and mfe <= 0 and mae < 0:
        return "ENTRY_BAD"
    return "STRATEGY_VALIDATION_INCONCLUSIVE"


def _local_exit_regime_thesis(classification: str, sim: Mapping[str, Any]) -> str:
    if classification == "ENTRY_GOOD_ORDER_MANAGEMENT_BAD":
        return "PROFIT_IMPULSE_HARVEST_OR_ORDER_MANAGEMENT_EVIDENCE_GAP"
    if classification == "ENTRY_GOOD_EXIT_BAD":
        return "THESIS_INTACT_CONTINUATION"
    if classification == "ENTRY_BAD":
        return "THESIS_FAILED_REVERSAL"
    if _float(sim.get("mfe")) and _float(sim.get("mfe")) > 0:
        return "DRIFT_HOLD"
    return "STRATEGY_VALIDATION_INCONCLUSIVE"


def _exit_issue_summary(reports: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(str(item.get("attribution_classification")) for item in reports)
    return [{"issue": key, "count": value} for key, value in counts.most_common() if key != "STRATEGY_VALIDATION_INCONCLUSIVE"]


def _remediation(
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


def _active_roster_ids(roster_payload: Mapping[str, Any], strategy_ids: Sequence[str]) -> set[str]:
    ids = {str(item) for item in strategy_ids if str(item)}
    for key in ("enabled_strategy_ids", "strategies", "enabled_strategies", "roster", "strategy_ids"):
        for item in _as_list(roster_payload.get(key)):
            if isinstance(item, Mapping):
                value = item.get("strategy_id") or item.get("standalone_strategy_id") or item.get("id") or item.get("name")
            else:
                value = item
            if value:
                ids.add(str(value))
    return ids


def _metric_bucket(aggregate: Mapping[str, Any], family: str, horizon: str) -> dict[str, Any]:
    family_payload = aggregate.get(family) if isinstance(aggregate.get(family), Mapping) else {}
    bucket = family_payload.get(horizon) if isinstance(family_payload.get(horizon), Mapping) else {}
    return dict(bucket)


def _quality_summary(metrics: Mapping[str, Any]) -> str:
    if not metrics:
        return "NO_METRICS_AVAILABLE"
    avg = metrics.get("average_return")
    pf = metrics.get("profit_factor_proxy")
    win = metrics.get("win_rate")
    return f"avg_return={avg}, pf={pf}, win_rate={win}"


def _mfe_mae_summary(metrics: Mapping[str, Any]) -> str:
    if not metrics:
        return "NO_MFE_MAE_AVAILABLE"
    return f"avg_mfe={metrics.get('avg_mfe')}, avg_mae={metrics.get('avg_mae')}"


def _discover_atp_companion_configs(repo_root: Path) -> list[dict[str, Any]]:
    config_paths = sorted((repo_root / "config").glob("*atp_companion*.yaml"))
    configs: list[dict[str, Any]] = []
    for path in config_paths:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        lane = _extract_probationary_lane(text)
        configs.append(
            {
                "path": str(path),
                "lane_id": lane.get("lane_id"),
                "standalone_strategy_id": lane.get("standalone_strategy_id"),
                "strategy_family": lane.get("strategy_family") or _extract_simple_yaml_value(text, "strategy_family"),
                "strategy_identity_root": lane.get("strategy_identity_root")
                or _extract_simple_yaml_value(text, "strategy_identity_root"),
                "symbol": lane.get("symbol"),
                "quality_bucket_policy": lane.get("quality_bucket_policy")
                or _extract_simple_yaml_value(text, "quality_bucket_policy"),
                "experimental_status": lane.get("experimental_status")
                or _extract_simple_yaml_value(text, "experimental_status"),
                "paper_only": lane.get("paper_only"),
                "non_approved": lane.get("non_approved"),
                "candidate_id": lane.get("candidate_id") or _extract_simple_yaml_value(text, "candidate_id"),
                "runtime_kind": lane.get("runtime_kind"),
            }
        )
    return configs


def _extract_probationary_lane(text: str) -> dict[str, Any]:
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


def _extract_simple_yaml_value(text: str, key: str) -> str | None:
    match = re.search(rf"^{re.escape(key)}:\s*[\"']?(?P<value>[^\"'\n#]+)", text, flags=re.MULTILINE)
    if not match:
        return None
    return match.group("value").strip()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_jsonl(path: Path, *, limit: int) -> tuple[list[dict[str, Any]], int]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return [], 0
    rows: list[dict[str, Any]] = []
    malformed = 0
    for line in lines[-max(0, int(limit)) :]:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            malformed += 1
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows, malformed


def _load_candles(path: Path) -> list[dict[str, Any]]:
    payload = _load_json(path)
    rows = payload.get("bars") or payload.get("candles") or payload.get("candle_history") or []
    if not isinstance(rows, list):
        return []
    return [_normalise_candle(row) for row in rows if isinstance(row, Mapping)]


def _normalise_candle(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "timestamp": row.get("bar_end") or row.get("end_ts") or row.get("timestamp") or row.get("time"),
        "open": _float(row.get("open") or row.get("o")),
        "high": _float(row.get("high") or row.get("h")),
        "low": _float(row.get("low") or row.get("l")),
        "close": _float(row.get("close") or row.get("c")),
        "volume": _float(row.get("volume") or row.get("v")),
    }


def _first_candle_index_at_or_after(candles: Sequence[Mapping[str, Any]], timestamp: str) -> int | None:
    if not timestamp:
        return max(0, len(candles) - 2) if candles else None
    for index, candle in enumerate(candles):
        if str(candle.get("timestamp") or "") >= timestamp:
            return index
    return None


def _symbol_for_strategy(strategy_id: str) -> str:
    upper = strategy_id.upper()
    if "MNQ" in upper or "NQ" in upper:
        return "MNQ"
    return "MGC"


def _symbol_from_contract(value: str) -> str:
    upper = value.upper()
    if "MNQ" in upper or "NQ" in upper:
        return "MNQ"
    return "MGC"


def _default_direction(strategy_id: str) -> str:
    upper = strategy_id.upper()
    if "SHORT" in upper or "BEAR" in upper:
        return "SHORT"
    return "LONG"


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--json", action="store_true", help="Print the generated report JSON.")
    args = parser.parse_args(argv)
    config = StrategyUnderperformanceReviewConfig(repo_root=args.repo_root)
    result = create_strategy_underperformance_review(config=config)
    if args.json:
        print(json.dumps(result.report, indent=2, sort_keys=True))
    else:
        print(f"Wrote {result.report_json}")
        print(f"Wrote {result.report_md}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
