"""Track B signal amplification operating plan.

This module is read-only. It converts the current strategy-activity evidence
into a concrete operating plan for increasing qualified opportunity capture
without changing thresholds, strategy semantics, or broker routes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable


DEFAULT_DIAGNOSTICS_ROOT = Path("outputs/track_b_execution_core/diagnostics")
DEFAULT_ACTIVITY_CALIBRATION_JSON = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_strategy_activity_calibration.json"
DEFAULT_MISSED_MOVE_JSON = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_missed_move_forensic_replay.json"
DEFAULT_TREND_GAP_JSON = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_trend_continuation_gap_diagnostic.json"


EXPECTED_FREQUENCY_RANGES: dict[str, dict[str, Any]] = {
    "asian_drift_v1": {
        "expected_eligible_bars_per_day": [12, 72],
        "expected_hard_signals_per_day": [0.5, 2.0],
        "expected_managed_paper_trades_per_day": [0.25, 1.0],
        "frequency_basis": "Asia drift state machine should produce occasional entry-armed opportunities when in scope.",
    },
    "ASIA_EARLY_PAUSE_RESUME_SHORT_V1": {
        "expected_eligible_bars_per_day": [12, 48],
        "expected_hard_signals_per_day": [0.25, 1.5],
        "expected_managed_paper_trades_per_day": [0.1, 1.0],
        "frequency_basis": "Session-specific short pause/resume setup; low but nonzero target during Asia early windows.",
    },
    "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1": {
        "expected_eligible_bars_per_day": [12, 48],
        "expected_hard_signals_per_day": [0.25, 1.5],
        "expected_managed_paper_trades_per_day": [0.1, 1.0],
        "frequency_basis": "Session-specific breakout/retest long; should not be daily guaranteed, but repeated zeroes require parity review.",
    },
    "FIRST_BULL_SNAP_TURN_V1": {
        "expected_eligible_bars_per_day": [80, 260],
        "expected_hard_signals_per_day": [1.0, 4.0],
        "expected_managed_paper_trades_per_day": [0.5, 2.0],
        "frequency_basis": "Broad MGC snap-turn family should be one of the louder Track B families.",
    },
    "FIRST_BEAR_SNAP_TURN_V1": {
        "expected_eligible_bars_per_day": [80, 260],
        "expected_hard_signals_per_day": [1.0, 4.0],
        "expected_managed_paper_trades_per_day": [0.5, 2.0],
        "frequency_basis": "Broad MGC snap-turn family should be one of the louder Track B families.",
    },
    "LONDON_LATE_PAUSE_RESUME_SHORT_V1": {
        "expected_eligible_bars_per_day": [12, 60],
        "expected_hard_signals_per_day": [0.25, 1.5],
        "expected_managed_paper_trades_per_day": [0.1, 1.0],
        "frequency_basis": "London-late short setup should be active only inside its phase window.",
    },
    "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1": {
        "expected_eligible_bars_per_day": [12, 60],
        "expected_hard_signals_per_day": [0.25, 1.5],
        "expected_managed_paper_trades_per_day": [0.1, 1.0],
        "frequency_basis": "Asia-late long pullback setup should show occasional candidates in matching flat/pullback regimes.",
    },
    "US_DERIVATIVE_BEAR_TURN_V1": {
        "expected_eligible_bars_per_day": [24, 90],
        "expected_hard_signals_per_day": [0.25, 2.0],
        "expected_managed_paper_trades_per_day": [0.1, 1.0],
        "frequency_basis": "US derivative bear turn is regime-specific but should produce measurable opportunities over active US windows.",
    },
    "US_LATE_PAUSE_RESUME_LONG_V1": {
        "expected_eligible_bars_per_day": [12, 60],
        "expected_hard_signals_per_day": [0.25, 1.5],
        "expected_managed_paper_trades_per_day": [0.1, 1.0],
        "frequency_basis": "US-late long setup should be measured only against its session/phase window.",
    },
    "MNQ_US_DERIVATIVE_BEAR_TURN_V1": {
        "expected_eligible_bars_per_day": [24, 90],
        "expected_hard_signals_per_day": [0.25, 2.0],
        "expected_managed_paper_trades_per_day": [0.1, 1.0],
        "frequency_basis": "US index derivative bear turn is regime-specific but should not be invisible across active US sessions.",
    },
    "MNQ_FIRST_BEAR_SNAP_TURN_V1": {
        "expected_eligible_bars_per_day": [80, 260],
        "expected_hard_signals_per_day": [1.0, 4.0],
        "expected_managed_paper_trades_per_day": [0.5, 2.0],
        "frequency_basis": "Broad MNQ snap-turn family should be a primary activity contributor.",
    },
    "MNQ_FIRST_BULL_SNAP_TURN_V1": {
        "expected_eligible_bars_per_day": [80, 260],
        "expected_hard_signals_per_day": [1.0, 4.0],
        "expected_managed_paper_trades_per_day": [0.5, 2.0],
        "frequency_basis": "Broad MNQ snap-turn family should be a primary activity contributor.",
    },
}


@dataclass(frozen=True)
class TrackBSignalAmplificationPlanConfig:
    repo_root: Path = Path(".")
    activity_calibration_json: Path = DEFAULT_ACTIVITY_CALIBRATION_JSON
    missed_move_json: Path = DEFAULT_MISSED_MOVE_JSON
    trend_gap_json: Path = DEFAULT_TREND_GAP_JSON
    output_json: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_signal_amplification_plan.json"
    output_md: Path = DEFAULT_DIAGNOSTICS_ROOT / "latest_track_b_signal_amplification_plan.md"


@dataclass(frozen=True)
class TrackBSignalAmplificationPlanResult:
    report_json: Path
    report_md: Path
    report: dict[str, Any]


def create_track_b_signal_amplification_plan(
    *,
    config: TrackBSignalAmplificationPlanConfig | None = None,
    now: datetime | None = None,
) -> TrackBSignalAmplificationPlanResult:
    actual_config = config or TrackBSignalAmplificationPlanConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    repo_root = Path(actual_config.repo_root)
    calibration = _load_json(_resolve(repo_root, actual_config.activity_calibration_json))
    missed_move = _load_json(_resolve(repo_root, actual_config.missed_move_json))
    trend_gap = _load_json(_resolve(repo_root, actual_config.trend_gap_json))
    strategies = [item for item in calibration.get("strategies") or [] if isinstance(item, Mapping)]
    expected_frequency = [_expected_frequency_row(strategy) for strategy in strategies]
    snap_turn_table = _snap_turn_failed_predicate_table(strategies)
    report: dict[str, Any] = {
        "schema_version": "track_b_signal_amplification_plan_v1",
        "generated_at": actual_now.isoformat(),
        "phase": "PAPER_STAGE_TRADING_ENGINE",
        "principle": (
            "Increase qualified opportunity capture through repair, parity, controlled variants, "
            "and coverage expansion. Silence is acceptable only when explained by regime, session, "
            "data readiness, or explicit business rules."
        ),
        "non_goals": [
            "Do not loosen thresholds broadly.",
            "Do not bypass signal-to-intent, STRATEGY_MANAGED lifecycle, or broker reconciliation.",
            "Do not register research-only coverage as managed PAPER.",
            "Do not route real strategy signals through paper_proof.",
        ],
        "workstreams": [
            _repair_parity_workstream(strategies),
            _snap_turn_workstream(snap_turn_table),
            _trend_continuation_workstream(missed_move, trend_gap),
        ],
        "expected_frequency_ranges": expected_frequency,
        "active_strategy_roster_gaps": {
            "zero_evaluation_strategies": [row["strategy_id"] for row in expected_frequency if row["observed_eligible_bars_day"] == 0],
            "near_zero_evaluation_strategies": [
                row["strategy_id"] for row in expected_frequency if 0 < row["observed_eligible_bars_day"] <= 5
            ],
            "too_quiet_strategies": [row["strategy_id"] for row in expected_frequency if row["activity_classification"] == "too_quiet"],
            "too_active_strategies": [row["strategy_id"] for row in expected_frequency if row["activity_classification"] == "too_active"],
        },
        "first_repair_candidate": {
            "lane": "Repair / parity amplification",
            "strategy_id": "asian_drift_v1",
            "objective": "Prove an ENTRY_ARMED Asian Drift fixture maps regime to explicit LONG/SHORT and creates signal-to-intent path.",
            "evidence_needed": [
                "ENTRY_ARMED or REQUALIFIED_CANDIDATE state snapshot with ASIA_DRIFT_LONG/ASIA_DRIFT_SHORT.",
                "Rule-runner emits hard LONG/SHORT signal without direction ambiguity.",
                "Signal-to-intent bridge creates STRATEGY_MANAGED intent with no paper_proof fallback.",
                "Session-label parity confirms Track B ASIA scope matches research expectation.",
            ],
        },
        "first_variant_candidate": {
            "lane": "Snap-turn near-miss amplification",
            "strategy_family": "FIRST_*_SNAP_TURN_V1 / MNQ_FIRST_*_SNAP_TURN_V1",
            "objective": "Rank snap-turn candidate/raw predicate failures and add MFE/MAE after near-miss bars before proposing any variant.",
            "evidence_needed": [
                "One-predicate-away and two-predicate-away tables by instrument and side.",
                "Numeric distance from snap/raw/candidate thresholds where feature fields expose thresholds.",
                "Subsequent MFE/MAE or directional excursion over 1, 3, and 6 completed 5m bars.",
                "False-positive review for bars rejected by current hard gates.",
            ],
        },
        "first_coverage_candidate": {
            "lane": "Trend Continuation coverage amplification",
            "strategy_id": "TREND_CONTINUATION_OVERLAY_RESEARCH_V1",
            "candidate_name": "LONG_OPENING_DRIVE_OR_PULLBACK_CONTINUATION_RESEARCH_V1",
            "status": "research_only",
            "managed_paper_eligible": False,
            "objective": "Detect MGC/MNQ long trend-continuation regimes missed by snap/pause/retest/turn families.",
            "evidence_needed": [
                "Missed-rally windows plus quiet/control windows.",
                "Comparison to session-start and US-open buy-and-hold diagnostic baselines.",
                "Replay signal quality before any PAPER registration.",
                "Proof that current enabled strategy families are not designed for the regime.",
            ],
        },
        "source_artifacts": {
            "activity_calibration": str(actual_config.activity_calibration_json),
            "missed_move_forensic_replay": str(actual_config.missed_move_json),
            "trend_continuation_gap": str(actual_config.trend_gap_json),
        },
    }
    output_json = _resolve(repo_root, actual_config.output_json)
    output_md = _resolve(repo_root, actual_config.output_md)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output_md.write_text(_render_markdown(report), encoding="utf-8")
    return TrackBSignalAmplificationPlanResult(report_json=output_json, report_md=output_md, report=report)


def _expected_frequency_row(strategy: Mapping[str, Any]) -> dict[str, Any]:
    strategy_id = str(strategy.get("strategy_id") or "UNKNOWN")
    expected = EXPECTED_FREQUENCY_RANGES.get(
        strategy_id,
        {
            "expected_eligible_bars_per_day": [0, 0],
            "expected_hard_signals_per_day": [0, 0],
            "expected_managed_paper_trades_per_day": [0, 0],
            "frequency_basis": "No explicit expected-frequency range has been declared yet.",
        },
    )
    eligible_raw = strategy.get("strategy_evaluations")
    if eligible_raw is None:
        eligible_raw = strategy.get("evaluated_completed_5m_bars")
    eligible = int(eligible_raw or 0)
    hard_signals = int(strategy.get("hard_signals") or 0)
    managed_trades_raw = strategy.get("meaningful_managed_trade_count")
    if managed_trades_raw is None:
        managed_trades_raw = strategy.get("broker_backed_trade_count")
    managed_trades = int(managed_trades_raw or 0)
    hard_range = expected["expected_hard_signals_per_day"]
    trade_range = expected["expected_managed_paper_trades_per_day"]
    if hard_signals > hard_range[1] or managed_trades > trade_range[1]:
        activity = "too_active"
    elif hard_signals < hard_range[0] or managed_trades < trade_range[0]:
        activity = "too_quiet"
    else:
        activity = "acceptable"
    return {
        "strategy_id": strategy_id,
        "instrument": strategy.get("instrument"),
        "session": strategy.get("session"),
        "side": strategy.get("side"),
        **expected,
        "observed_eligible_bars_day": eligible,
        "observed_hard_signals_day": hard_signals,
        "observed_managed_paper_trades_day": managed_trades,
        "activity_classification": activity,
        "current_diagnostic_classification": strategy.get("classification"),
    }


def _repair_parity_workstream(strategies: list[Mapping[str, Any]]) -> dict[str, Any]:
    near_zero = [str(item.get("strategy_id")) for item in strategies if 0 < int(item.get("strategy_evaluations") or 0) <= 5]
    zero = [str(item.get("strategy_id")) for item in strategies if int(item.get("strategy_evaluations") or 0) == 0]
    return {
        "id": "repair_parity_amplification",
        "title": "Repair / parity amplification",
        "first_concrete_action": "Build an Asian Drift ENTRY_ARMED fixture that proves regime-to-LONG/SHORT signal and STRATEGY_MANAGED intent creation.",
        "tasks": [
            "Asian Drift: prove valid entry-capable fixture maps to explicit LONG/SHORT and creates signal/intent path.",
            "Active roster: inspect enabled strategies with zero or near-zero evaluations.",
            "Session-label parity: compare Track B session labels against Track 1/research expectations for active strategies.",
        ],
        "current_zero_evaluation_strategies": zero,
        "current_near_zero_evaluation_strategies": near_zero,
        "classification_policy": {
            "missing envelope or runtime inclusion": "IMPLEMENTATION_DEFECT",
            "Track B session differs from research session": "PARITY_DEFECT",
            "valid no-signal state": "INTENTIONAL_HARD_GATE",
        },
    }


def _snap_turn_workstream(snap_turn_table: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "id": "snap_turn_near_miss_amplification",
        "title": "Snap-turn near-miss amplification",
        "first_concrete_action": "Produce MGC/MNQ snap-turn failed-predicate table with numeric threshold distance and post-bar MFE/MAE.",
        "ranked_failed_predicate_table": snap_turn_table,
        "required_extensions_before_variant": [
            "Separate one-predicate-away from two-predicate-away.",
            "Add numeric distance from threshold where feature/envelope fields expose actual/required values.",
            "Add subsequent MFE/MAE or directional excursion after near-miss bars.",
            "Classify dominant failures as PARITY_DEFECT, IMPLEMENTATION_DEFECT, INTENTIONAL_HARD_GATE, VARIANT_CANDIDATE, or REJECT_BAD_FALSE_POSITIVE.",
        ],
    }


def _trend_continuation_workstream(missed_move: Mapping[str, Any], trend_gap: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": "trend_continuation_coverage_amplification",
        "title": "Trend Continuation coverage amplification",
        "first_concrete_action": "Define LONG_OPENING_DRIVE_OR_PULLBACK_CONTINUATION_RESEARCH_V1 under TREND_CONTINUATION_OVERLAY_RESEARCH_V1.",
        "candidate_status": "research_only",
        "managed_paper_eligible": False,
        "source_evidence": {
            "missed_move_classification": missed_move.get("classification"),
            "trend_gap_classification": trend_gap.get("classification"),
            "trend_gap_assessment": trend_gap.get("trend_continuation_gap_assessment"),
        },
        "replay_design": [
            "Use retained missed-rally windows for MGC and MNQ.",
            "Use control windows without trend-continuation behavior.",
            "Compare against diagnostic-only session-start and US-open baselines.",
            "Do not register managed PAPER until replay proves signal quality.",
        ],
    }


def _snap_turn_failed_predicate_table(strategies: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for strategy in strategies:
        strategy_id = str(strategy.get("strategy_id") or "")
        if "SNAP_TURN" not in strategy_id:
            continue
        for rank, predicate in enumerate(strategy.get("top_failed_predicates") or [], start=1):
            if not isinstance(predicate, Mapping):
                continue
            reason = str(predicate.get("reason") or "")
            rows.append(
                {
                    "rank": rank,
                    "strategy_id": strategy_id,
                    "instrument": strategy.get("instrument"),
                    "side": strategy.get("side"),
                    "failed_predicate": reason,
                    "count": int(predicate.get("count") or 0),
                    "near_miss_context": {
                        "one_predicate_away": int(strategy.get("one_predicate_away") or 0),
                        "two_predicates_away": int(strategy.get("two_predicates_away") or 0),
                    },
                    "classification": _classify_failed_predicate(reason),
                    "numeric_distance_status": "NOT_AVAILABLE_IN_CURRENT_ROLLUP",
                    "mfe_mae_status": "NOT_AVAILABLE_IN_CURRENT_ROLLUP",
                }
            )
    return sorted(rows, key=lambda item: (-int(item["count"]), str(item["strategy_id"]), int(item["rank"])))


def _classify_failed_predicate(reason: str) -> str:
    lowered = reason.lower()
    if "missing" in lowered or "unavailable" in lowered:
        return "IMPLEMENTATION_DEFECT"
    if "session" in lowered or "phase" in lowered:
        return "INTENTIONAL_HARD_GATE"
    if "candidate" in lowered or "raw" in lowered or "turn" in lowered:
        return "VARIANT_CANDIDATE"
    return "VARIANT_CANDIDATE"


def _render_markdown(report: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Signal Amplification Plan",
        "",
        f"Generated: {report.get('generated_at')}",
        "",
        "Track B is a PAPER-stage trading engine. The operating target is more qualified opportunity capture, not passive observation and not random threshold loosening.",
        "",
        "## First Three Workstreams",
    ]
    for workstream in report.get("workstreams") or []:
        lines.extend(["", f"### {workstream['title']}", "", f"First action: {workstream['first_concrete_action']}"])
        if workstream.get("tasks"):
            lines.append("")
            lines.extend(f"- {task}" for task in workstream["tasks"])
    lines.extend(["", "## First Candidates", ""])
    for key in ("first_repair_candidate", "first_variant_candidate", "first_coverage_candidate"):
        candidate = report[key]
        lines.extend([f"### {candidate['lane']}", "", f"Objective: {candidate['objective']}", "", "Evidence needed:"])
        lines.extend(f"- {item}" for item in candidate.get("evidence_needed") or [])
        lines.append("")
    lines.extend(["## Expected Frequency Matrix", ""])
    lines.append("| Strategy | Instrument | Session | Expected hard signals/day | Expected managed PAPER trades/day | Observed hard signals | Observed managed trades | Classification |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---|")
    for row in report.get("expected_frequency_ranges") or []:
        lines.append(
            "| {strategy_id} | {instrument} | {session} | {hard} | {trades} | {observed_hard_signals_day} | {observed_managed_paper_trades_day} | {activity_classification} |".format(
                strategy_id=row.get("strategy_id"),
                instrument=row.get("instrument"),
                session=row.get("session"),
                hard=_range_text(row.get("expected_hard_signals_per_day")),
                trades=_range_text(row.get("expected_managed_paper_trades_per_day")),
                observed_hard_signals_day=row.get("observed_hard_signals_day"),
                observed_managed_paper_trades_day=row.get("observed_managed_paper_trades_day"),
                activity_classification=row.get("activity_classification"),
            )
        )
    lines.extend(["", "## Snap-Turn Predicate Ranking", ""])
    lines.append("| Strategy | Predicate | Count | Initial classification | Evidence gap |")
    lines.append("|---|---|---:|---|---|")
    for row in (report.get("workstreams") or [])[1].get("ranked_failed_predicate_table") or []:
        lines.append(
            f"| {row['strategy_id']} | {row['failed_predicate']} | {row['count']} | {row['classification']} | numeric distance + MFE/MAE required |"
        )
    lines.append("")
    return "\n".join(lines)


def _range_text(values: Any) -> str:
    if isinstance(values, list) and len(values) == 2:
        return f"{values[0]}-{values[1]}"
    return "NOT_DECLARED"


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"artifact_missing": True, "path": str(path)}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {"artifact_unavailable": True, "path": str(path), "error": str(exc)}
    return payload if isinstance(payload, dict) else {"artifact_unavailable": True, "path": str(path)}


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path
