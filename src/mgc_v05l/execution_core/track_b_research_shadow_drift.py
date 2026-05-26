"""Research-only drift shadows for P0 near-miss regimes.

These builders are intentionally outside strategy authority. They consume
execution-core market-data/control-plane evidence and emit dry-run diagnostic
artifacts for regimes the current P0 strategies may miss.
"""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_phase1_runtime_candle_adapter import (
    PHASE1_RUNTIME_MARKET_DATA_SOURCE_CATEGORY,
    normalize_phase1_runtime_candle_payload,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RESEARCH_SHADOW_OUTPUT_ROOT = Path("outputs/track_b_execution_core/research_shadow")
DEFAULT_LATE_JOIN_SHADOW_LATEST = (
    DEFAULT_RESEARCH_SHADOW_OUTPUT_ROOT / "latest_late_join_asian_drift_shadow.json"
)
DEFAULT_GAP_DRIFT_SHADOW_LATEST = (
    DEFAULT_RESEARCH_SHADOW_OUTPUT_ROOT / "latest_gap_drift_continuation_shadow.json"
)
DEFAULT_P0_NEAR_MISS_SHADOW_LATEST = DEFAULT_RESEARCH_SHADOW_OUTPUT_ROOT / "latest_p0_near_miss_shadow.json"
DEFAULT_LATE_JOIN_SHADOW_EVENTS = DEFAULT_RESEARCH_SHADOW_OUTPUT_ROOT / "late_join_asian_drift_shadow.jsonl"
DEFAULT_GAP_DRIFT_SHADOW_EVENTS = DEFAULT_RESEARCH_SHADOW_OUTPUT_ROOT / "gap_drift_continuation_shadow.jsonl"
DEFAULT_P0_NEAR_MISS_SHADOW_EVENTS = DEFAULT_RESEARCH_SHADOW_OUTPUT_ROOT / "p0_near_miss_shadow.jsonl"
DEFAULT_ASIAN_DRIFT_REPORT = (
    Path("outputs")
    / "track_b_execution_core"
    / "asian_drift_state"
    / "latest_asian_drift_watch_chain_report.json"
)
DEFAULT_P0_LOOP_EVENTS = (
    Path("outputs")
    / "track_b_execution_core"
    / "p0_observe_only"
    / "p0_observe_only_loop_events.jsonl"
)
DEFAULT_MULTI_STRATEGY_CYCLE_ROOT = (
    Path("outputs")
    / "track_b_execution_core"
    / "track_b_multi_strategy_runtime_cycle"
)
DEFAULT_MGC_5M_CANDLES = (
    Path("outputs")
    / "track_b_execution_core"
    / "phase1_runtime_market_data"
    / "MGC"
    / "5m"
    / "latest_runtime_candles.json"
)
DEFAULT_MNQ_5M_CANDLES = (
    Path("outputs")
    / "track_b_execution_core"
    / "phase1_runtime_market_data"
    / "MNQ"
    / "5m"
    / "latest_runtime_candles.json"
)
DEFAULT_SAFE_STATE = (
    Path("outputs")
    / "track_b_execution_core"
    / "safe_state"
    / "latest_runtime_safe_state_envelope.json"
)

LATE_JOIN_ASIAN_DRIFT_SHADOW_CANDIDATE = "LATE_JOIN_ASIAN_DRIFT_SHADOW_CANDIDATE"
LATE_JOIN_ASIAN_DRIFT_SHADOW_NO_CANDIDATE = "LATE_JOIN_ASIAN_DRIFT_SHADOW_NO_CANDIDATE"
LATE_JOIN_ASIAN_DRIFT_BLOCKED_INSUFFICIENT_CONTEXT = "LATE_JOIN_ASIAN_DRIFT_BLOCKED_INSUFFICIENT_CONTEXT"

GAP_DRIFT_CONTINUATION_SHADOW_CANDIDATE = "GAP_DRIFT_CONTINUATION_SHADOW_CANDIDATE"
GAP_DRIFT_CONTINUATION_SHADOW_NO_CANDIDATE = "GAP_DRIFT_CONTINUATION_SHADOW_NO_CANDIDATE"
GAP_DRIFT_CONTINUATION_INSUFFICIENT_DATA = "GAP_DRIFT_CONTINUATION_INSUFFICIENT_DATA"

P0_NEAR_MISS_SHADOW_READY = "P0_NEAR_MISS_SHADOW_READY"
P0_NEAR_MISS_SHADOW_EMPTY = "P0_NEAR_MISS_SHADOW_EMPTY"
P0_ENTRY_GRADE_A = "A_GRADE_STRICT_CANDIDATE"
P0_ENTRY_GRADE_B = "B_GRADE_NEAR_MISS_SHADOW"
P0_ENTRY_GRADE_C = "C_GRADE_DIAGNOSTIC_ONLY"

SAFE_STATE_UNSAFE_FOR_SHADOW = "SAFE_STATE_UNSAFE_FOR_SHADOW_DIAGNOSTIC"
SHADOW_POLICY = "RESEARCH_DIAGNOSTIC_ONLY"

P0_STRATEGY_IDS = (
    "asian_drift_v1",
    "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
    "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
    "MNQ_FIRST_BEAR_SNAP_TURN_V1",
    "MNQ_FIRST_BULL_SNAP_TURN_V1",
)

P0_NEAR_MISS_PROFILES: dict[str, dict[str, Any]] = {
    "asian_drift_v1": {
        "direction": "LONG_OR_SHORT",
        "directional_predicates": {"direction_is_explicit", "in_scope", "session_not_timeout"},
        "noncritical_predicates": {"entry_window_open", "state_is_entry_eligible", "entry_ready"},
        "directional_threshold": 3,
        "requires_score_evidence": True,
        "min_score_evidence": 3.1,
    },
    "ASIA_EARLY_PAUSE_RESUME_SHORT_V1": {
        "direction": "SHORT",
        "directional_predicates": {
            "close_below_fast_ema",
            "close_below_open",
            "close_below_previous_close",
            "derivative_bear_body_ok",
            "derivative_bear_close_weak",
            "derivative_bear_range_ok",
            "derivative_bear_stretch_ok",
            "signal_breaks_prior_1_low",
        },
        "noncritical_predicates": {
            "derivative_phase_asia_early",
            "normalized_curvature_at_or_below_threshold",
            "one_bar_rebound_before_signal",
            "session_asia",
            "setup_bar_curvature_is_flat",
            "signal_range_expansion_below_threshold",
        },
        "directional_threshold": 4,
    },
    "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1": {
        "direction": "LONG",
        "directional_predicates": {
            "breakout_breaks_prior_1_high",
            "signal_retests_and_holds_breakout_level",
        },
        "noncritical_predicates": {
            "asia_early_or_gc_mgc_london_open",
            "breakout_bar_expansion_is_normal",
            "breakout_bar_slope_is_flat",
            "no_first_bull_snap_turn",
        },
        "directional_threshold": 1,
    },
    "MNQ_FIRST_BEAR_SNAP_TURN_V1": {
        "direction": "SHORT",
        "directional_predicates": {
            "bear_snap_body_ok",
            "bear_snap_close_weak",
            "bear_snap_location_ok",
            "bear_snap_range_ok",
            "bear_snap_reversal_bar",
            "bear_snap_up_stretch_ok",
            "bear_snap_velocity_ok",
        },
        "noncritical_predicates": {
            "bear_snap_raw",
            "bear_snap_turn_candidate",
            "first_bear_snap_turn",
            "prior_bars_since_bear_snap_gt_cooldown",
            "session_allowed",
        },
        "directional_threshold": 4,
    },
    "MNQ_FIRST_BULL_SNAP_TURN_V1": {
        "direction": "LONG",
        "directional_predicates": {
            "bull_snap_body_ok",
            "bull_snap_close_strong",
            "bull_snap_downside_stretch_ok",
            "bull_snap_location_ok",
            "bull_snap_range_ok",
            "bull_snap_reversal_bar",
            "bull_snap_velocity_ok",
        },
        "noncritical_predicates": {
            "bull_snap_raw",
            "bull_snap_turn_candidate",
            "first_bull_snap_turn",
            "prior_bars_since_bull_snap_gt_cooldown",
            "session_allowed",
        },
        "directional_threshold": 4,
    },
}


@dataclass(frozen=True)
class TrackBResearchShadowDriftConfig:
    repo_root: Path = REPO_ROOT
    output_root: Path = DEFAULT_RESEARCH_SHADOW_OUTPUT_ROOT
    asian_drift_report_path: Path = DEFAULT_ASIAN_DRIFT_REPORT
    p0_loop_events_path: Path = DEFAULT_P0_LOOP_EVENTS
    multi_strategy_cycle_root: Path = DEFAULT_MULTI_STRATEGY_CYCLE_ROOT
    multi_strategy_cycle_report_path: Path | None = None
    mgc_5m_candles_path: Path = DEFAULT_MGC_5M_CANDLES
    mnq_5m_candles_path: Path = DEFAULT_MNQ_5M_CANDLES
    safe_state_path: Path = DEFAULT_SAFE_STATE
    min_late_join_score: float = 3.1
    min_gap_drift_score: float = 3.0
    min_gap_drift_bars: int = 8
    loop_history_lookback: int = 100

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_late_join_asian_drift_shadow(
    *,
    asian_drift_report: Mapping[str, Any] | None,
    p0_loop_events: Sequence[Mapping[str, Any]] = (),
    source_report_path: Path | str | None = None,
    now: datetime | None = None,
    min_score: float = 3.1,
) -> dict[str, Any]:
    actual_now = _now(now)
    event_evidence = _latest_late_join_event(p0_loop_events, min_score=min_score)
    report_evidence = dict(asian_drift_report or {})
    evidence = report_evidence if _is_late_join(report_evidence, min_score=min_score) else event_evidence
    source_kind = "asian_drift_report" if evidence is report_evidence else "p0_loop_history"
    missing_reasons: list[str] = []

    if not asian_drift_report and not p0_loop_events:
        classification = LATE_JOIN_ASIAN_DRIFT_BLOCKED_INSUFFICIENT_CONTEXT
        missing_reasons.append("missing_asian_drift_report_and_loop_history")
    elif evidence:
        classification = LATE_JOIN_ASIAN_DRIFT_SHADOW_CANDIDATE
    else:
        classification = LATE_JOIN_ASIAN_DRIFT_SHADOW_NO_CANDIDATE

    direction = _text(evidence.get("hypothetical_direction")) or _direction_from_asian_payload(evidence)
    score = _float(
        evidence.get("hypothetical_late_join_score")
        or evidence.get("hypothetical_score")
        or evidence.get("max_late_join_score")
    )
    if classification == LATE_JOIN_ASIAN_DRIFT_SHADOW_CANDIDATE and score is None:
        score = min_score

    source_ts = _text(evidence.get("latest_mgc_5m_candle_timestamp") or evidence.get("source_candle_timestamp"))
    if not source_ts:
        source_ts = _text(evidence.get("generated_at"))

    return {
        **_non_authority_fields(),
        "schema_version": "track_b_late_join_asian_drift_shadow_v1",
        "shadow_evaluation_id": f"late-join-asian-drift-shadow-{uuid.uuid4().hex}",
        "generated_at": actual_now.isoformat(),
        "strategy_family": "ASIAN_DRIFT_LATE_JOIN_RESEARCH",
        "symbol": "MGC",
        "shadow_classification": classification,
        "hypothetical_direction": direction,
        "hypothetical_score": score,
        "missing_authority_reason": _late_join_missing_authority_reason(
            classification=classification,
            evidence=evidence,
            missing_reasons=missing_reasons,
        ),
        "why_not_live_authority": (
            "Missing 18:00 ET Asian Drift session anchor context; this shadow is research-only and cannot "
            "create runtime entry authority."
        ),
        "source_candle_timestamp": source_ts,
        "source_authority_path": None if source_report_path is None else str(source_report_path),
        "source_category": "RESEARCH_SHADOW_FROM_EXECUTION_CORE_EVIDENCE",
        "source_evidence_kind": source_kind if evidence else None,
        "anchor_required": evidence.get("anchor_required", True) if evidence else True,
        "anchor_observed": evidence.get("anchor_observed") if evidence else None,
        "anchor_window_start": evidence.get("anchor_window_start") if evidence else None,
        "anchor_window_end": evidence.get("anchor_window_end") if evidence else None,
        "runtime_context_start": evidence.get("runtime_context_start") if evidence else None,
        "late_join_policy": "DIAGNOSTIC_ONLY",
        "late_join_diagnostic": classification == LATE_JOIN_ASIAN_DRIFT_SHADOW_CANDIDATE,
        "hypothetical_entry_ready": False,
        "entry_ready": False,
        "operator_explanation": _late_join_operator_explanation(classification),
        "p0_loop_history_late_join_count": sum(1 for item in p0_loop_events if _is_late_join(_late_join_view(item), min_score=min_score)),
    }


def build_gap_drift_continuation_shadow(
    *,
    candle_payloads: Mapping[str, Mapping[str, Any]],
    source_paths: Mapping[str, Path | str | None],
    safe_state: Mapping[str, Any] | None = None,
    now: datetime | None = None,
    min_score: float = 3.0,
    min_bars: int = 8,
) -> dict[str, Any]:
    actual_now = _now(now)
    safe_state_payload = dict(safe_state or {})
    safe_state_classification = _text(safe_state_payload.get("safe_state_classification"))
    safe_state_blocks_shadow = safe_state_classification in {"SAFE_STATE_HARD_HOLD", "SAFE_STATE_POSITION_LIMIT_HIT"}
    rows = []
    for symbol, payload in sorted(candle_payloads.items()):
        source_path = source_paths.get(symbol)
        normalized = normalize_phase1_runtime_candle_payload(payload, source_path=source_path)
        rows.append(
            _gap_drift_symbol_row(
                symbol=symbol,
                payload=normalized,
                source_path=source_path,
                min_score=min_score,
                min_bars=min_bars,
                safe_state_blocks_shadow=safe_state_blocks_shadow,
            )
        )
    candidates = [row for row in rows if row.get("shadow_classification") == GAP_DRIFT_CONTINUATION_SHADOW_CANDIDATE]
    if safe_state_blocks_shadow:
        classification = GAP_DRIFT_CONTINUATION_SHADOW_NO_CANDIDATE
        primary = rows[0] if rows else {}
        missing_authority_reason = f"safe_state_classification={safe_state_classification} prevents even shadow candidate labeling."
    elif candidates:
        classification = GAP_DRIFT_CONTINUATION_SHADOW_CANDIDATE
        primary = max(candidates, key=lambda row: _float(row.get("hypothetical_score")) or 0.0)
        missing_authority_reason = "gap_drift_continuation_is_research_shadow_not_strategy_authority"
    elif any(row.get("shadow_classification") == GAP_DRIFT_CONTINUATION_INSUFFICIENT_DATA for row in rows):
        classification = GAP_DRIFT_CONTINUATION_INSUFFICIENT_DATA
        primary = rows[0] if rows else {}
        missing_authority_reason = "insufficient_phase1_runtime_candle_context"
    else:
        classification = GAP_DRIFT_CONTINUATION_SHADOW_NO_CANDIDATE
        primary = rows[0] if rows else {}
        missing_authority_reason = "no_sustained_gap_drift_continuation_pressure_detected"

    return {
        **_non_authority_fields(),
        "schema_version": "track_b_gap_drift_continuation_shadow_v1",
        "shadow_evaluation_id": f"gap-drift-continuation-shadow-{uuid.uuid4().hex}",
        "generated_at": actual_now.isoformat(),
        "strategy_family": "GAP_DRIFT_CONTINUATION_RESEARCH",
        "symbol": primary.get("symbol"),
        "shadow_classification": classification,
        "hypothetical_direction": primary.get("hypothetical_direction"),
        "hypothetical_score": primary.get("hypothetical_score"),
        "missing_authority_reason": missing_authority_reason,
        "why_not_live_authority": (
            "Gap/drift continuation shadow is research-only; no approved P0 runtime strategy, order planner, "
            "or lifecycle authority consumes it."
        ),
        "source_candle_timestamp": primary.get("source_candle_timestamp"),
        "source_authority_path": primary.get("source_authority_path"),
        "source_category": PHASE1_RUNTIME_MARKET_DATA_SOURCE_CATEGORY,
        "safe_state_classification": safe_state_classification,
        "safe_state_shadow_block": safe_state_blocks_shadow,
        "evaluated_symbols": rows,
        "operator_explanation": _gap_drift_operator_explanation(classification, primary),
    }


def build_p0_near_miss_shadow(
    *,
    evaluated_strategies: Sequence[Mapping[str, Any]],
    source_report_path: Path | str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _now(now)
    rows = [_p0_near_miss_strategy_row(row) for row in evaluated_strategies if isinstance(row, Mapping)]
    rows_by_id = {str(row.get("strategy_id")): row for row in rows}
    ordered_rows = [rows_by_id[strategy_id] for strategy_id in P0_STRATEGY_IDS if strategy_id in rows_by_id]
    ordered_rows.extend(row for row in rows if row.get("strategy_id") not in P0_STRATEGY_IDS)
    b_grade_rows = [row for row in ordered_rows if row.get("entry_grade") == P0_ENTRY_GRADE_B]
    a_grade_rows = [row for row in ordered_rows if row.get("entry_grade") == P0_ENTRY_GRADE_A]
    classification = P0_NEAR_MISS_SHADOW_READY if ordered_rows else P0_NEAR_MISS_SHADOW_EMPTY
    primary = max(
        b_grade_rows or a_grade_rows or ordered_rows or [{}],
        key=lambda row: _float(row.get("hypothetical_score")) or 0.0,
    )
    return {
        **_non_authority_fields(),
        "schema_version": "track_b_p0_near_miss_shadow_v1",
        "shadow_evaluation_id": f"p0-near-miss-shadow-{uuid.uuid4().hex}",
        "generated_at": actual_now.isoformat(),
        "strategy_family": "P0_NEAR_MISS_RESEARCH",
        "symbol": primary.get("symbol"),
        "shadow_classification": classification,
        "entry_grade_summary": {
            "a_grade_count": len(a_grade_rows),
            "b_grade_count": len(b_grade_rows),
            "c_grade_count": sum(1 for row in ordered_rows if row.get("entry_grade") == P0_ENTRY_GRADE_C),
        },
        "b_grade_candidate_count": len(b_grade_rows),
        "primary_shadow_strategy_id": primary.get("strategy_id"),
        "primary_entry_grade": primary.get("entry_grade"),
        "hypothetical_direction": primary.get("hypothetical_direction"),
        "hypothetical_score": primary.get("hypothetical_score"),
        "source_candle_timestamp": primary.get("source_candle_timestamp"),
        "source_authority_path": None if source_report_path is None else str(source_report_path),
        "source_category": "EXECUTION_CORE_MULTI_STRATEGY_CYCLE_REPORT",
        "missing_authority_reason": "near_miss_shadow_is_not_live_strategy_authority",
        "why_not_live_authority": (
            "B-grade near-miss rows are research-only. Existing strict A-grade strategy rules remain the only "
            "candidate logic; this artifact cannot enable submit, order planning, lifecycle mutation, or runtime entry."
        ),
        "strategies": ordered_rows,
        "operator_explanation": _p0_near_miss_operator_explanation(b_grade_rows=b_grade_rows),
    }


def write_research_shadow_drift_artifacts(
    *,
    config: TrackBResearchShadowDriftConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _now(now)
    asian_path = config.resolve(config.asian_drift_report_path)
    loop_events_path = config.resolve(config.p0_loop_events_path)
    safe_state_path = config.resolve(config.safe_state_path)
    mgc_path = config.resolve(config.mgc_5m_candles_path)
    mnq_path = config.resolve(config.mnq_5m_candles_path)

    asian_report = _read_json_if_exists(asian_path)
    loop_events = _read_jsonl_if_exists(loop_events_path, limit=config.loop_history_lookback)
    safe_state = _read_json_if_exists(safe_state_path)
    mgc_payload = _read_json_if_exists(mgc_path)
    mnq_payload = _read_json_if_exists(mnq_path)
    cycle_report_path = (
        config.resolve(config.multi_strategy_cycle_report_path)
        if config.multi_strategy_cycle_report_path is not None
        else _latest_multi_strategy_cycle_report_path(config.resolve(config.multi_strategy_cycle_root))
    )
    cycle_report = _read_json_if_exists(cycle_report_path) if cycle_report_path is not None else None

    late_join = build_late_join_asian_drift_shadow(
        asian_drift_report=asian_report,
        p0_loop_events=loop_events,
        source_report_path=asian_path if asian_report else loop_events_path,
        now=actual_now,
        min_score=config.min_late_join_score,
    )
    gap_drift = build_gap_drift_continuation_shadow(
        candle_payloads={"MGC": mgc_payload or {}, "MNQ": mnq_payload or {}},
        source_paths={"MGC": mgc_path, "MNQ": mnq_path},
        safe_state=safe_state,
        now=actual_now,
        min_score=config.min_gap_drift_score,
        min_bars=config.min_gap_drift_bars,
    )
    p0_near_miss = build_p0_near_miss_shadow(
        evaluated_strategies=list((cycle_report or {}).get("evaluated_strategies") or []),
        source_report_path=cycle_report_path,
        now=actual_now,
    )
    paths = write_research_shadow_payloads(
        config=config,
        late_join_shadow=late_join,
        gap_drift_shadow=gap_drift,
        p0_near_miss_shadow=p0_near_miss,
    )
    return {
        "generated_at": actual_now.isoformat(),
        "late_join_asian_drift_shadow": late_join,
        "gap_drift_continuation_shadow": gap_drift,
        "p0_near_miss_shadow": p0_near_miss,
        **paths,
        "read_only": True,
        "research_only": True,
        "submit_allowed": False,
        "broker_mutation_allowed": False,
    }


def write_research_shadow_payloads(
    *,
    config: TrackBResearchShadowDriftConfig,
    late_join_shadow: Mapping[str, Any],
    gap_drift_shadow: Mapping[str, Any],
    p0_near_miss_shadow: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    """Write latest/JSONL research shadow artifacts."""

    output_root = config.resolve(config.output_root)
    late_join_latest = output_root / "latest_late_join_asian_drift_shadow.json"
    gap_drift_latest = output_root / "latest_gap_drift_continuation_shadow.json"
    p0_near_miss_latest = output_root / "latest_p0_near_miss_shadow.json"
    late_join_events = output_root / "late_join_asian_drift_shadow.jsonl"
    gap_drift_events = output_root / "gap_drift_continuation_shadow.jsonl"
    p0_near_miss_events = output_root / "p0_near_miss_shadow.jsonl"

    paths: dict[str, str] = {}
    if late_join_shadow:
        write_json_atomic(late_join_latest, late_join_shadow)
        _append_jsonl(late_join_events, late_join_shadow)
        paths.update(
            {
                "latest_late_join_asian_drift_shadow_path": str(late_join_latest),
                "late_join_asian_drift_shadow_events_path": str(late_join_events),
            }
        )
    if gap_drift_shadow:
        write_json_atomic(gap_drift_latest, gap_drift_shadow)
        _append_jsonl(gap_drift_events, gap_drift_shadow)
        paths.update(
            {
                "latest_gap_drift_continuation_shadow_path": str(gap_drift_latest),
                "gap_drift_continuation_shadow_events_path": str(gap_drift_events),
            }
        )
    if p0_near_miss_shadow is not None:
        write_json_atomic(p0_near_miss_latest, p0_near_miss_shadow)
        _append_jsonl(p0_near_miss_events, p0_near_miss_shadow)
        paths.update(
            {
                "latest_p0_near_miss_shadow_path": str(p0_near_miss_latest),
                "p0_near_miss_shadow_events_path": str(p0_near_miss_events),
            }
        )
    return paths


def _gap_drift_symbol_row(
    *,
    symbol: str,
    payload: Mapping[str, Any],
    source_path: Path | str | None,
    min_score: float,
    min_bars: int,
    safe_state_blocks_shadow: bool,
) -> dict[str, Any]:
    candles = _candles(payload)
    if safe_state_blocks_shadow:
        return _gap_row(
            symbol=symbol,
            classification=GAP_DRIFT_CONTINUATION_SHADOW_NO_CANDIDATE,
            source_path=source_path,
            reason=SAFE_STATE_UNSAFE_FOR_SHADOW,
        )
    if len(candles) < min_bars:
        return _gap_row(
            symbol=symbol,
            classification=GAP_DRIFT_CONTINUATION_INSUFFICIENT_DATA,
            source_path=source_path,
            reason=f"requires_at_least_{min_bars}_completed_5m_candles",
            bar_count=len(candles),
        )
    first = candles[0]
    last = candles[-1]
    first_open = _float(first.get("open"))
    last_close = _float(last.get("close"))
    if first_open is None or last_close is None:
        return _gap_row(
            symbol=symbol,
            classification=GAP_DRIFT_CONTINUATION_INSUFFICIENT_DATA,
            source_path=source_path,
            reason="missing_open_or_close",
            bar_count=len(candles),
        )
    total_delta = last_close - first_open
    direction = "LONG" if total_delta > 0 else "SHORT" if total_delta < 0 else None
    if direction is None:
        return _gap_row(
            symbol=symbol,
            classification=GAP_DRIFT_CONTINUATION_SHADOW_NO_CANDIDATE,
            source_path=source_path,
            reason="flat_window",
            bar_count=len(candles),
        )
    ranges = [max((_float(c.get("high")) or 0.0) - (_float(c.get("low")) or 0.0), 0.0) for c in candles]
    avg_range = sum(ranges) / len(ranges) if ranges else 0.0
    avg_range = avg_range if avg_range > 0 else max(abs(total_delta) / max(len(candles), 1), 0.01)
    directional_bars = 0
    closes = []
    for candle in candles:
        open_value = _float(candle.get("open"))
        close_value = _float(candle.get("close"))
        if open_value is None or close_value is None:
            continue
        closes.append(close_value)
        if direction == "LONG" and close_value >= open_value:
            directional_bars += 1
        if direction == "SHORT" and close_value <= open_value:
            directional_bars += 1
    directional_ratio = directional_bars / len(candles)
    persistence = _close_persistence(closes, direction=direction)
    pullback_ratio = _pullback_ratio(candles, direction=direction, first_open=first_open, last_close=last_close)
    score = abs(total_delta) / avg_range
    score = round(score * 0.65 + directional_ratio * 2.0 + persistence * 1.5 - pullback_ratio, 4)
    candidate = score >= min_score and directional_ratio >= 0.55 and persistence >= 0.5 and pullback_ratio <= 1.5
    return {
        **_gap_row(
            symbol=symbol,
            classification=GAP_DRIFT_CONTINUATION_SHADOW_CANDIDATE
            if candidate
            else GAP_DRIFT_CONTINUATION_SHADOW_NO_CANDIDATE,
            source_path=source_path,
            reason="sustained_gap_drift_continuation_pressure_detected"
            if candidate
            else "gap_drift_score_below_shadow_threshold",
            bar_count=len(candles),
        ),
        "hypothetical_direction": direction,
        "hypothetical_score": score,
        "source_candle_timestamp": _text(last.get("candle_timestamp") or last.get("bar_end") or last.get("timestamp")),
        "first_candle_timestamp": _text(first.get("candle_timestamp") or first.get("bar_end") or first.get("timestamp")),
        "window_close_delta": round(total_delta, 6),
        "average_bar_range": round(avg_range, 6),
        "directional_bar_ratio": round(directional_ratio, 4),
        "close_persistence": round(persistence, 4),
        "pullback_ratio": round(pullback_ratio, 4),
    }


def _p0_near_miss_strategy_row(row: Mapping[str, Any]) -> dict[str, Any]:
    strategy_id = _text(row.get("strategy_id")) or "UNKNOWN_STRATEGY"
    profile = P0_NEAR_MISS_PROFILES.get(strategy_id, {})
    conditions = row.get("rule_conditions")
    rule_conditions = dict(conditions) if isinstance(conditions, Mapping) else {}
    strict_failed = sorted(key for key, value in rule_conditions.items() if value is False)
    noncritical_config = set(profile.get("noncritical_predicates") or set())
    noncritical_failed = sorted(key for key in strict_failed if key in noncritical_config)
    critical_failed = sorted(key for key in strict_failed if key not in noncritical_config)
    directional_keys = set(profile.get("directional_predicates") or set())
    directional_true = sorted(key for key in directional_keys if rule_conditions.get(key) is True)
    directional_false = sorted(key for key in directional_keys if rule_conditions.get(key) is False)
    directional_threshold = int(profile.get("directional_threshold") or max(1, len(directional_keys)))
    directional_confirmation = len(directional_true) >= directional_threshold
    evidence_score = _p0_explicit_score(row)
    if profile.get("requires_score_evidence") is True:
        min_score = float(profile.get("min_score_evidence") or 0.0)
        directional_confirmation = directional_confirmation and (
            row.get("late_join_diagnostic") is True
            or (evidence_score is not None and evidence_score >= min_score)
        )
    signal_emitted = row.get("signal_emitted") is True or _text(row.get("decision")) in {"LONG", "SHORT"}
    if signal_emitted:
        grade = P0_ENTRY_GRADE_A
    elif directional_confirmation and not critical_failed and strict_failed:
        grade = P0_ENTRY_GRADE_B
    else:
        grade = P0_ENTRY_GRADE_C
    direction = _p0_hypothetical_direction(row=row, profile=profile)
    score = evidence_score if evidence_score is not None and grade != P0_ENTRY_GRADE_A else None
    score = score if score is not None else _p0_near_miss_score(
        grade=grade,
        directional_true_count=len(directional_true),
        directional_total=max(len(directional_keys), 1),
        critical_failed_count=len(critical_failed),
        noncritical_failed_count=len(noncritical_failed),
    )
    return {
        **_non_authority_fields(),
        "strategy_id": strategy_id,
        "symbol": _p0_symbol(strategy_id),
        "entry_grade": grade,
        "strict_failed_predicates": strict_failed,
        "noncritical_failed_predicates": noncritical_failed,
        "critical_failed_predicates": critical_failed,
        "directional_confirmation": directional_confirmation,
        "directional_predicates_true": directional_true,
        "directional_predicates_missing": directional_false,
        "directional_confirmation_threshold": directional_threshold,
        "hypothetical_direction": direction,
        "hypothetical_score": score,
        "why_not_live_authority": _p0_why_not_live_authority(grade=grade, strategy_id=strategy_id),
        "decision": row.get("decision"),
        "decision_reason": row.get("decision_reason"),
        "strategy_runtime_verdict": row.get("strategy_runtime_verdict"),
        "signal_emitted": signal_emitted,
        "source_candle_timestamp": row.get("candle_timestamp") or row.get("decision_ts") or row.get("generated_at"),
        "rule_blockers": list(row.get("rule_blockers") or []),
        "late_join_diagnostic": row.get("late_join_diagnostic") is True,
        "late_join_classification": row.get("late_join_classification"),
        "shadow_candidate": grade == P0_ENTRY_GRADE_B,
        "submit_allowed": False,
        "entry_ready": False,
    }


def _p0_hypothetical_direction(*, row: Mapping[str, Any], profile: Mapping[str, Any]) -> str | None:
    explicit = _text(row.get("signal_direction") or row.get("decision"))
    if explicit in {"LONG", "SHORT"}:
        return explicit
    profile_direction = _text(profile.get("direction"))
    if profile_direction in {"LONG", "SHORT"}:
        return profile_direction
    return _direction_from_asian_payload(row)


def _p0_explicit_score(row: Mapping[str, Any]) -> float | None:
    return _float(
        row.get("hypothetical_late_join_score")
        or row.get("hypothetical_score")
        or row.get("long_drift_score")
        or row.get("short_drift_score")
    )


def _p0_near_miss_score(
    *,
    grade: str,
    directional_true_count: int,
    directional_total: int,
    critical_failed_count: int,
    noncritical_failed_count: int,
) -> float:
    if grade == P0_ENTRY_GRADE_A:
        return 100.0
    directional_ratio = directional_true_count / max(directional_total, 1)
    score = 40.0 + directional_ratio * 45.0 - critical_failed_count * 18.0 - noncritical_failed_count * 4.0
    if grade == P0_ENTRY_GRADE_B:
        score = max(score, 60.0)
    if grade == P0_ENTRY_GRADE_C:
        score = min(score, 49.0)
    return round(max(min(score, 99.0), 0.0), 4)


def _p0_symbol(strategy_id: str) -> str:
    if strategy_id.startswith("MNQ_"):
        return "MNQ"
    return "MGC"


def _p0_why_not_live_authority(*, grade: str, strategy_id: str) -> str:
    if grade == P0_ENTRY_GRADE_A:
        return (
            f"{strategy_id} met strict strategy logic, but this shadow artifact is display/research-only and cannot "
            "authorize submit."
        )
    if grade == P0_ENTRY_GRADE_B:
        return (
            f"{strategy_id} is a B-grade near-miss: directional evidence is present but noncritical strict "
            "predicates are missing. Existing A-grade rules remain the only live candidate authority."
        )
    return (
        f"{strategy_id} is C-grade diagnostic context: either directional confirmation is weak or critical "
        "strict predicates are missing."
    )


def _p0_near_miss_operator_explanation(*, b_grade_rows: Sequence[Mapping[str, Any]]) -> str:
    if b_grade_rows:
        strategy_ids = ", ".join(str(row.get("strategy_id")) for row in b_grade_rows[:3])
        return f"B-grade research near-misses observed for {strategy_ids}; no submit authority was created."
    return "No B-grade P0 near-miss shadow candidates were observed."


def _gap_row(
    *,
    symbol: str,
    classification: str,
    source_path: Path | str | None,
    reason: str,
    bar_count: int = 0,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "shadow_classification": classification,
        "missing_authority_reason": reason,
        "source_authority_path": None if source_path is None else str(source_path),
        "source_category": PHASE1_RUNTIME_MARKET_DATA_SOURCE_CATEGORY,
        "bar_count": bar_count,
        "dry_run_only": True,
        "research_only": True,
        "submit_allowed": False,
        "broker_mutation_allowed": False,
        "not_order_authority": True,
        "not_lifecycle_authority": True,
    }


def _latest_late_join_event(events: Sequence[Mapping[str, Any]], *, min_score: float) -> dict[str, Any]:
    for event in reversed(list(events)):
        view = _late_join_view(event)
        if _is_late_join(view, min_score=min_score):
            return view
    return {}


def _late_join_view(event: Mapping[str, Any]) -> dict[str, Any]:
    diagnostic = event.get("late_join_asian_drift_diagnostic")
    if isinstance(diagnostic, Mapping):
        return {
            **dict(diagnostic),
            "generated_at": event.get("generated_at"),
            "latest_mgc_5m_candle_timestamp": event.get("latest_mgc_5m_candle_timestamp"),
        }
    return dict(event)


def _is_late_join(payload: Mapping[str, Any], *, min_score: float) -> bool:
    if payload.get("late_join_diagnostic") is True:
        return True
    if payload.get("late_join_classification") == "ASIAN_DRIFT_LATE_JOIN_STRONG_DRIFT_OBSERVED":
        return True
    score = _float(payload.get("hypothetical_late_join_score") or payload.get("hypothetical_score"))
    return score is not None and score >= min_score and payload.get("anchor_observed") is False


def _direction_from_asian_payload(payload: Mapping[str, Any]) -> str | None:
    explicit = _text(payload.get("signal_direction") or payload.get("dominant_direction"))
    if explicit in {"LONG", "SHORT"}:
        return explicit
    long_score = _float(payload.get("long_drift_score"))
    short_score = _float(payload.get("short_drift_score"))
    if long_score is None and short_score is None:
        return None
    return "LONG" if (long_score or 0.0) >= (short_score or 0.0) else "SHORT"


def _late_join_missing_authority_reason(
    *,
    classification: str,
    evidence: Mapping[str, Any],
    missing_reasons: Sequence[str],
) -> str:
    if missing_reasons:
        return ",".join(missing_reasons)
    if classification == LATE_JOIN_ASIAN_DRIFT_SHADOW_CANDIDATE:
        return _text(evidence.get("missing_anchor_reason")) or "missing_session_anchor_context"
    if classification == LATE_JOIN_ASIAN_DRIFT_BLOCKED_INSUFFICIENT_CONTEXT:
        return "insufficient_asian_drift_context"
    return "strong_missing_anchor_late_join_drift_not_detected"


def _late_join_operator_explanation(classification: str) -> str:
    if classification == LATE_JOIN_ASIAN_DRIFT_SHADOW_CANDIDATE:
        return (
            "Strong post-anchor Asian Drift was observed after the 18:00 ET anchor context was missing; "
            "research shadow records a hypothetical late-join candidate only."
        )
    if classification == LATE_JOIN_ASIAN_DRIFT_BLOCKED_INSUFFICIENT_CONTEXT:
        return "Late-join Asian Drift shadow could not evaluate because required diagnostic context is missing."
    return "Late-join Asian Drift shadow did not detect a strong missing-anchor drift candidate."


def _gap_drift_operator_explanation(classification: str, primary: Mapping[str, Any]) -> str:
    if classification == GAP_DRIFT_CONTINUATION_SHADOW_CANDIDATE:
        return (
            f"{primary.get('symbol')} showed sustained {primary.get('hypothetical_direction')} gap/drift continuation "
            "pressure; shadow records this as research-only coverage gap evidence."
        )
    if classification == GAP_DRIFT_CONTINUATION_INSUFFICIENT_DATA:
        return "Gap/drift continuation shadow could not evaluate all symbols because Phase-1 5m context is insufficient."
    return "Gap/drift continuation shadow did not find a sustained continuation candidate."


def _candles(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    values = payload.get("candles") or payload.get("candle_history") or payload.get("bars") or []
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        return []
    return [dict(item) for item in values if isinstance(item, Mapping) and item.get("completed") is not False]


def _close_persistence(closes: Sequence[float], *, direction: str) -> float:
    if len(closes) < 2:
        return 0.0
    favorable = 0
    for prev, current in zip(closes, closes[1:]):
        if direction == "LONG" and current >= prev:
            favorable += 1
        if direction == "SHORT" and current <= prev:
            favorable += 1
    return favorable / max(len(closes) - 1, 1)


def _pullback_ratio(
    candles: Sequence[Mapping[str, Any]],
    *,
    direction: str,
    first_open: float,
    last_close: float,
) -> float:
    total_move = abs(last_close - first_open)
    if total_move <= 0:
        return 0.0
    if direction == "LONG":
        worst_pullback = max(max(last_close - (_float(c.get("low")) or last_close), 0.0) for c in candles)
    else:
        worst_pullback = max(max((_float(c.get("high")) or last_close) - last_close, 0.0) for c in candles)
    return worst_pullback / total_move


def _non_authority_fields() -> dict[str, Any]:
    return {
        "dry_run_only": True,
        "research_only": True,
        "offline_diagnostic": False,
        "not_runtime_authority": True,
        "not_routing_authority": True,
        "not_broker_truth": True,
        "not_market_data_runtime_truth": True,
        "not_order_authority": True,
        "not_lifecycle_authority": True,
        "submit_allowed": False,
        "broker_mutation_allowed": False,
        "lifecycle_mutation_allowed": False,
        "entry_ready": False,
        "execution_enabled": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "dashboard_projection_consumed": False,
        "shadow_policy": SHADOW_POLICY,
    }


def _read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return payload


def _read_jsonl_if_exists(path: Path, *, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines()[-limit:]:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _latest_multi_strategy_cycle_report_path(root: Path) -> Path | None:
    if not root.exists():
        return None
    paths = list(root.glob("*/track_b_multi_strategy_runtime_cycle_report.json"))
    if not paths:
        return None
    return max(paths, key=lambda path: path.stat().st_mtime)


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(payload), sort_keys=True) + "\n")


def _now(value: datetime | None) -> datetime:
    actual = value or datetime.now(UTC)
    if actual.tzinfo is None:
        return actual.replace(tzinfo=UTC)
    return actual.astimezone(UTC)


def _float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write Track B research-only drift shadow diagnostics.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_RESEARCH_SHADOW_OUTPUT_ROOT)
    parser.add_argument("--asian-drift-report", type=Path, default=DEFAULT_ASIAN_DRIFT_REPORT)
    parser.add_argument("--p0-loop-events", type=Path, default=DEFAULT_P0_LOOP_EVENTS)
    parser.add_argument("--multi-strategy-cycle-root", type=Path, default=DEFAULT_MULTI_STRATEGY_CYCLE_ROOT)
    parser.add_argument("--multi-strategy-cycle-report", type=Path, default=None)
    parser.add_argument("--mgc-5m-candles-json", type=Path, default=DEFAULT_MGC_5M_CANDLES)
    parser.add_argument("--mnq-5m-candles-json", type=Path, default=DEFAULT_MNQ_5M_CANDLES)
    parser.add_argument("--safe-state-json", type=Path, default=DEFAULT_SAFE_STATE)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBResearchShadowDriftConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_root=Path(args.output_root),
        asian_drift_report_path=Path(args.asian_drift_report),
        p0_loop_events_path=Path(args.p0_loop_events),
        multi_strategy_cycle_root=Path(args.multi_strategy_cycle_root),
        multi_strategy_cycle_report_path=None
        if args.multi_strategy_cycle_report is None
        else Path(args.multi_strategy_cycle_report),
        mgc_5m_candles_path=Path(args.mgc_5m_candles_json),
        mnq_5m_candles_path=Path(args.mnq_5m_candles_json),
        safe_state_path=Path(args.safe_state_json),
    )
    payload = write_research_shadow_drift_artifacts(config=config)
    summary = {
        "late_join_asian_drift_shadow_classification": payload["late_join_asian_drift_shadow"].get(
            "shadow_classification"
        ),
        "gap_drift_continuation_shadow_classification": payload["gap_drift_continuation_shadow"].get(
            "shadow_classification"
        ),
        "p0_near_miss_shadow_classification": payload["p0_near_miss_shadow"].get("shadow_classification"),
        "p0_b_grade_candidate_count": payload["p0_near_miss_shadow"].get("b_grade_candidate_count"),
        "late_join_path": payload.get("latest_late_join_asian_drift_shadow_path"),
        "gap_drift_path": payload.get("latest_gap_drift_continuation_shadow_path"),
        "p0_near_miss_path": payload.get("latest_p0_near_miss_shadow_path"),
        "read_only": True,
        "research_only": True,
        "submit_allowed": False,
        "broker_mutation_allowed": False,
    }
    print(json.dumps(payload if args.json else summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
