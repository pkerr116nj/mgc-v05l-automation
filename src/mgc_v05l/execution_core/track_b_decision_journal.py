"""Tiered Track B decision journal.

The journal is deliberately a sidecar to the runtime-cycle report. It records
signals, near-misses, suppressions, safety blocks, and state transitions in
compact JSONL while aggregating ordinary no-setup evaluations. Dashboard hot
paths should read only the latest summary/tail, never the full journal.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable
from .track_b_snap_turn_envelope_producer import (
    BEAR_SNAP_MIN_CLOSE_VS_SLOW_EMA_ATR,
    BEAR_SNAP_REQUIRE_CLOSE_ABOVE_SLOW_EMA,
    BEAR_SNAP_COOLDOWN_BARS,
    BULL_SNAP_MAX_CLOSE_VS_SLOW_EMA_ATR,
    BULL_SNAP_REQUIRE_CLOSE_BELOW_SLOW_EMA,
    MAX_BEAR_SNAP_CLOSE_LOCATION,
    MIN_BEAR_SNAP_BAR_RANGE_ATR,
    MIN_BEAR_SNAP_BODY_ATR,
    MIN_BEAR_SNAP_UP_STRETCH_ATR,
    MIN_BEAR_SNAP_VELOCITY_DELTA_ATR,
    MIN_SNAP_BAR_RANGE_ATR,
    MIN_SNAP_BODY_ATR,
    MIN_SNAP_CLOSE_LOCATION,
    MIN_SNAP_DOWN_STRETCH_ATR,
    MIN_SNAP_VELOCITY_DELTA_ATR,
    SNAP_COOLDOWN_BARS,
)


DEFAULT_TRACK_B_DECISION_JOURNAL_OUTPUT_ROOT = Path("outputs/track_b_execution_core/track_b_decision_journal")
DEFAULT_DECISION_JOURNAL_MAX_BYTES = 50 * 1024 * 1024
DEFAULT_DECISION_JOURNAL_ROTATED_KEEP = 10
DEFAULT_NEAR_MISS_SCORE_THRESHOLD = Decimal("0.80")
DEFAULT_NEAR_MISS_DISTANCE_THRESHOLD = Decimal("0.05")
DEFAULT_NO_SETUP_AGGREGATE_INTERVAL_MINUTES = 15
MAX_HASH_BYTES = 5 * 1024 * 1024


@dataclass(frozen=True)
class TrackBDecisionJournalConfig:
    mode: str = "interesting_only"
    near_miss_score_threshold: Decimal = DEFAULT_NEAR_MISS_SCORE_THRESHOLD
    near_miss_distance_threshold: Decimal = DEFAULT_NEAR_MISS_DISTANCE_THRESHOLD
    no_setup_aggregate_interval_minutes: int = DEFAULT_NO_SETUP_AGGREGATE_INTERVAL_MINUTES
    max_bytes: int = DEFAULT_DECISION_JOURNAL_MAX_BYTES
    rotated_keep: int = DEFAULT_DECISION_JOURNAL_ROTATED_KEEP

    @classmethod
    def from_env(cls) -> "TrackBDecisionJournalConfig":
        return cls(
            mode=os.environ.get("MGC_TRACK_B_DECISION_JOURNAL_MODE", "interesting_only"),
            near_miss_score_threshold=_env_decimal(
                "MGC_TRACK_B_NEAR_MISS_SCORE_THRESHOLD",
                DEFAULT_NEAR_MISS_SCORE_THRESHOLD,
            ),
            near_miss_distance_threshold=_env_decimal(
                "MGC_TRACK_B_NEAR_MISS_DISTANCE_THRESHOLD",
                DEFAULT_NEAR_MISS_DISTANCE_THRESHOLD,
            ),
            no_setup_aggregate_interval_minutes=_env_int(
                "MGC_TRACK_B_NO_SETUP_AGGREGATE_INTERVAL_MINUTES",
                DEFAULT_NO_SETUP_AGGREGATE_INTERVAL_MINUTES,
            ),
            max_bytes=_env_int("MGC_TRACK_B_DECISION_JOURNAL_MAX_BYTES", DEFAULT_DECISION_JOURNAL_MAX_BYTES),
            rotated_keep=_env_int("MGC_TRACK_B_DECISION_JOURNAL_ROTATED_KEEP", DEFAULT_DECISION_JOURNAL_ROTATED_KEEP),
        )


@dataclass(frozen=True)
class TrackBDecisionJournalResult:
    summary_json: Path
    active_journal_jsonl: Path
    heartbeat_jsonl: Path
    aggregate_json: Path
    state_json: Path
    summary: dict[str, Any]


def record_track_b_decision_journal_cycle(
    *,
    runtime_cycle_report: Mapping[str, Any],
    runtime_cycle_report_json: Path | None = None,
    output_root: Path = DEFAULT_TRACK_B_DECISION_JOURNAL_OUTPUT_ROOT,
    config: TrackBDecisionJournalConfig | None = None,
    now: datetime | None = None,
) -> TrackBDecisionJournalResult:
    actual_now = now or _parse_datetime(runtime_cycle_report.get("generated_at")) or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_config = config or TrackBDecisionJournalConfig.from_env()
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    active_journal = root / "track_b_decision_journal.jsonl"
    heartbeat_path = root / "track_b_runtime_heartbeat.jsonl"
    aggregate_path = root / "track_b_no_setup_aggregates.json"
    state_path = root / "track_b_strategy_state.json"
    summary_path = root / "latest_track_b_decision_journal_summary.json"

    rotated_files = []
    rotated_files.extend(_rotate_jsonl_if_needed(active_journal, actual_config.max_bytes, actual_config.rotated_keep))
    rotated_files.extend(_rotate_jsonl_if_needed(heartbeat_path, actual_config.max_bytes, actual_config.rotated_keep))
    active_journal.touch(exist_ok=True)

    aggregates = _read_json_object(aggregate_path)
    previous_state = _read_json_object(state_path)
    full_records: list[dict[str, Any]] = []
    aggregate_updates = 0
    tier_counts: Counter[str] = Counter()

    strategy_details = []
    for strategy in _evaluated_strategies(runtime_cycle_report):
        detail = _strategy_detail(strategy)
        loaded_report = _load_strategy_report(detail.get("strategy_report_json"))
        input_event = _load_input_event(loaded_report)
        detail.update(_detail_from_strategy_report(loaded_report, input_event))
        tier, reason = _classify_strategy_detail(
            detail=detail,
            runtime_cycle_report=runtime_cycle_report,
            previous_state=previous_state,
            config=actual_config,
        )
        tier_counts[tier] += 1
        detail["journal_tier"] = tier
        detail["journal_tier_reason"] = reason
        strategy_details.append(detail)
        if tier == "TIER_1_NO_SETUP_AGGREGATE":
            _update_no_setup_aggregate(aggregates, detail, actual_now, actual_config)
            aggregate_updates += 1
        else:
            full_records.append(_full_record(runtime_cycle_report, detail, runtime_cycle_report_json, actual_now))

    heartbeat = _heartbeat_record(runtime_cycle_report, strategy_details, runtime_cycle_report_json, actual_now)
    _append_jsonl(heartbeat_path, heartbeat)
    for record in full_records:
        _append_jsonl(active_journal, record)

    _write_json(aggregate_path, aggregates)
    _write_json(state_path, _next_strategy_state(previous_state, strategy_details, actual_now))

    summary = {
        "schema_version": "track_b_decision_journal_summary_v1",
        "generated_at": actual_now.isoformat(),
        "decision_journal_mode": actual_config.mode,
        "decision_journal_output_root": str(root),
        "decision_journal_path": str(active_journal),
        "decision_journal_heartbeat_path": str(heartbeat_path),
        "decision_journal_aggregate_path": str(aggregate_path),
        "decision_journal_state_path": str(state_path),
        "dashboard_hot_path_policy": "Dashboard reads latest summary/tail only; it must not scan full or rotated journals.",
        "heartbeat_written": True,
        "full_records_written": len(full_records),
        "ordinary_no_setup_aggregate_updates": aggregate_updates,
        "latest_tier_counts": dict(tier_counts),
        "near_miss_records_written": sum(1 for record in full_records if record["journal_tier"] == "TIER_2_NEAR_MISS"),
        "signal_records_written": sum(1 for record in full_records if record["journal_tier"] == "TIER_3_SIGNAL_TRADE_DECISION"),
        "abnormal_records_written": sum(1 for record in full_records if record["journal_tier"] == "TIER_4_ABNORMAL_SAFETY"),
        "rotated_files": rotated_files,
        "evaluated_strategy_count": len(strategy_details),
        "evaluated_strategy_ids": [detail.get("strategy_id") for detail in strategy_details],
        "strategy_decision_summaries": [_strategy_decision_summary(detail) for detail in strategy_details],
        "submit_attempted": bool(runtime_cycle_report.get("submit_attempted")),
        "broker_state_mutated": bool(runtime_cycle_report.get("broker_state_mutated")),
        "paper_proof_invoked": bool(runtime_cycle_report.get("paper_proof_invoked")),
        "live_money_readiness": bool(runtime_cycle_report.get("live_money_readiness")),
    }
    _write_json(summary_path, summary)
    return TrackBDecisionJournalResult(
        summary_json=summary_path,
        active_journal_jsonl=active_journal,
        heartbeat_jsonl=heartbeat_path,
        aggregate_json=aggregate_path,
        state_json=state_path,
        summary=summary,
    )


def _evaluated_strategies(runtime_cycle_report: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw = runtime_cycle_report.get("evaluated_strategies")
    return list(raw) if isinstance(raw, list) else []


def _strategy_detail(strategy: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "strategy_id": strategy.get("strategy_id"),
        "rule_mode": strategy.get("rule_mode"),
        "strategy_runtime_verdict": strategy.get("strategy_runtime_verdict"),
        "decision": strategy.get("decision"),
        "signal_emitted": bool(strategy.get("signal_emitted")),
        "signal_direction": strategy.get("signal_direction"),
        "signal_source": strategy.get("signal_source"),
        "real_strategy_signal": bool(strategy.get("real_strategy_signal")),
        "primary_blocker": strategy.get("primary_blocker"),
        "strategy_report_json": strategy.get("report_json_path"),
        "paper_eligible": strategy.get("paper_eligible"),
        "live_money_eligible": strategy.get("live_money_eligible"),
    }


def _load_strategy_report(path_value: object) -> dict[str, Any]:
    if not path_value:
        return {}
    path = Path(str(path_value))
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_input_event(strategy_report: Mapping[str, Any]) -> dict[str, Any]:
    path_value = strategy_report.get("input_event_path")
    if not path_value:
        return {}
    path = Path(str(path_value))
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _detail_from_strategy_report(strategy_report: Mapping[str, Any], input_event: Mapping[str, Any]) -> dict[str, Any]:
    conditions = strategy_report.get("rule_conditions") if isinstance(strategy_report.get("rule_conditions"), Mapping) else {}
    total_conditions = len(conditions)
    passed_conditions = sum(1 for value in conditions.values() if value is True)
    failed_conditions = [str(name) for name, value in conditions.items() if value is not True]
    near_miss_score = Decimal(passed_conditions) / Decimal(total_conditions) if total_conditions else Decimal("0")
    predicate_attributions = _snap_turn_predicate_attributions(
        strategy_id=str(strategy_report.get("strategy_registry_id") or strategy_report.get("strategy_id") or ""),
        conditions=conditions,
        input_event=input_event,
    )
    nearest_failed = _nearest_failed_predicate(
        strategy_id=str(strategy_report.get("strategy_registry_id") or strategy_report.get("strategy_id") or ""),
        failed_conditions=failed_conditions,
        input_event=input_event,
        predicate_attributions=predicate_attributions,
    )
    nearest_numeric = _nearest_failed_numeric_predicate(predicate_attributions)
    input_event_path = strategy_report.get("input_event_path")
    return {
        "rule_conditions": dict(conditions),
        "rule_blockers": strategy_report.get("rule_blockers") or [],
        "rule_inputs": strategy_report.get("rule_inputs") or {},
        "decision_reason": strategy_report.get("decision_reason"),
        "near_miss_score": float(near_miss_score),
        "total_predicates": total_conditions,
        "passed_predicates": passed_conditions,
        "failed_predicates": failed_conditions,
        "nearest_failed_predicate": nearest_failed,
        "predicate_attributions": predicate_attributions,
        "nearest_failed_numeric_predicate": nearest_numeric,
        "nearest_failed_numeric_distance": None if nearest_numeric is None else nearest_numeric.get("distance_to_pass"),
        "passed_required_predicate_count": passed_conditions,
        "failed_required_predicate_count": len(failed_conditions),
        "input_event_path": input_event_path,
        "input_event_sha256": _sha256_file(input_event_path),
        "strategy_report_sha256": _sha256_file(strategy_report.get("report_json_path")),
        "latest_evaluated_bar_timestamp": _latest_bar_timestamp(strategy_report, input_event),
        "quote_provider_mode": strategy_report.get("input_quote_provider_mode"),
        "quote_age_seconds": strategy_report.get("quote_age_seconds"),
    }


def _nearest_failed_predicate(
    *,
    strategy_id: str,
    failed_conditions: list[str],
    input_event: Mapping[str, Any],
    predicate_attributions: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    nearest_numeric = _nearest_failed_numeric_predicate(predicate_attributions or [])
    if nearest_numeric is not None:
        return {
            "predicate": nearest_numeric.get("predicate_name"),
            "category": nearest_numeric.get("category"),
            "actual": nearest_numeric.get("actual_value"),
            "required": nearest_numeric.get("required_value"),
            "comparator": nearest_numeric.get("comparator"),
            "distance_to_pass": nearest_numeric.get("distance_to_pass"),
            "normalized_distance_to_pass": nearest_numeric.get("normalized_distance_to_pass"),
        }
    if strategy_id not in {"FIRST_BULL_SNAP_TURN_V1", "FIRST_BEAR_SNAP_TURN_V1"}:
        return {"predicate": failed_conditions[0]} if failed_conditions else None
    metadata = input_event.get("metadata") if isinstance(input_event.get("metadata"), Mapping) else {}
    diagnostics = metadata.get("feature_diagnostics") if isinstance(metadata.get("feature_diagnostics"), Mapping) else {}
    close_location = _decimal_or_none(diagnostics.get("close_location"))
    if close_location is None:
        return {"predicate": failed_conditions[0]} if failed_conditions else None
    if strategy_id == "FIRST_BULL_SNAP_TURN_V1" and "bull_snap_close_strong" in failed_conditions:
        required = MIN_SNAP_CLOSE_LOCATION
        distance = max(Decimal("0"), required - close_location)
        return {
            "predicate": "bull_snap_close_strong",
            "actual": str(close_location),
            "required": str(required),
            "distance_to_pass": str(distance),
        }
    if strategy_id == "FIRST_BEAR_SNAP_TURN_V1" and "bear_snap_close_weak" in failed_conditions:
        required = MAX_BEAR_SNAP_CLOSE_LOCATION
        distance = max(Decimal("0"), close_location - required)
        return {
            "predicate": "bear_snap_close_weak",
            "actual": str(close_location),
            "required": str(required),
            "distance_to_pass": str(distance),
        }
    return {"predicate": failed_conditions[0]} if failed_conditions else None


def _snap_turn_predicate_attributions(
    *,
    strategy_id: str,
    conditions: Mapping[str, Any],
    input_event: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if strategy_id not in {"FIRST_BULL_SNAP_TURN_V1", "FIRST_BEAR_SNAP_TURN_V1"}:
        return []
    metadata = input_event.get("metadata") if isinstance(input_event.get("metadata"), Mapping) else {}
    diagnostics = metadata.get("feature_diagnostics") if isinstance(metadata.get("feature_diagnostics"), Mapping) else {}
    close = _decimal_or_none(input_event.get("close"))
    atr = _decimal_or_none(diagnostics.get("atr"))
    close_location = _decimal_or_none(diagnostics.get("close_location"))
    if strategy_id == "FIRST_BULL_SNAP_TURN_V1":
        features = (
            metadata.get("first_bull_snap_turn_features")
            if isinstance(metadata.get("first_bull_snap_turn_features"), Mapping)
            else {}
        )
        state = (
            metadata.get("first_bull_snap_turn_state")
            if isinstance(metadata.get("first_bull_snap_turn_state"), Mapping)
            else {}
        )
        return _compact_attributions(
            [
                _numeric_attribution(
                    conditions,
                    "bull_snap_downside_stretch_ok",
                    "stretch",
                    diagnostics.get("downside_stretch"),
                    _threshold_times_atr(features.get("bull_snap_min_downside_stretch_atr"), atr, MIN_SNAP_DOWN_STRETCH_ATR),
                    "gte",
                    "high",
                ),
                _numeric_attribution(
                    conditions,
                    "bull_snap_range_ok",
                    "range",
                    diagnostics.get("bar_range"),
                    _threshold_times_atr(features.get("bull_snap_range_threshold_atr"), atr, MIN_SNAP_BAR_RANGE_ATR),
                    "gte",
                    "medium",
                ),
                _numeric_attribution(
                    conditions,
                    "bull_snap_body_ok",
                    "body",
                    diagnostics.get("body_size"),
                    _threshold_times_atr(features.get("bull_snap_body_threshold_atr"), atr, MIN_SNAP_BODY_ATR),
                    "gte",
                    "medium",
                ),
                _numeric_attribution(
                    conditions,
                    "bull_snap_close_strong",
                    "close",
                    close_location,
                    MIN_SNAP_CLOSE_LOCATION,
                    "gte",
                    "high",
                ),
                _numeric_attribution(
                    conditions,
                    "bull_snap_velocity_ok",
                    "velocity",
                    diagnostics.get("velocity_delta"),
                    _threshold_times_atr(features.get("bull_snap_velocity_threshold_atr"), atr, MIN_SNAP_VELOCITY_DELTA_ATR),
                    "gte",
                    "high",
                ),
                _numeric_attribution(
                    conditions,
                    "bull_snap_location_ok",
                    "location",
                    close,
                    _bull_location_required(diagnostics),
                    "lte",
                    "medium",
                ),
                _numeric_attribution(
                    conditions,
                    "prior_bars_since_bull_snap_gt_cooldown",
                    "cooldown",
                    state.get("prior_bars_since_bull_snap"),
                    SNAP_COOLDOWN_BARS,
                    "gt",
                    "medium",
                ),
                _categorical_attribution(conditions, "session_allowed", "session", state.get("session_allowed"), True, "high"),
            ]
        )
    features = (
        metadata.get("first_bear_snap_turn_features")
        if isinstance(metadata.get("first_bear_snap_turn_features"), Mapping)
        else {}
    )
    state = (
        metadata.get("first_bear_snap_turn_state")
        if isinstance(metadata.get("first_bear_snap_turn_state"), Mapping)
        else {}
    )
    return _compact_attributions(
        [
            _numeric_attribution(
                conditions,
                "bear_snap_up_stretch_ok",
                "stretch",
                diagnostics.get("upside_stretch"),
                _threshold_times_atr(features.get("bear_snap_min_upside_stretch_atr"), atr, MIN_BEAR_SNAP_UP_STRETCH_ATR),
                "gte",
                "high",
            ),
            _numeric_attribution(
                conditions,
                "bear_snap_range_ok",
                "range",
                diagnostics.get("bar_range"),
                _threshold_times_atr(features.get("bear_snap_range_threshold_atr"), atr, MIN_BEAR_SNAP_BAR_RANGE_ATR),
                "gte",
                "medium",
            ),
            _numeric_attribution(
                conditions,
                "bear_snap_body_ok",
                "body",
                diagnostics.get("body_size"),
                _threshold_times_atr(features.get("bear_snap_body_threshold_atr"), atr, MIN_BEAR_SNAP_BODY_ATR),
                "gte",
                "medium",
            ),
            _numeric_attribution(
                conditions,
                "bear_snap_close_weak",
                "close",
                close_location,
                MAX_BEAR_SNAP_CLOSE_LOCATION,
                "lte",
                "high",
            ),
            _numeric_attribution(
                conditions,
                "bear_snap_velocity_ok",
                "velocity",
                diagnostics.get("velocity_delta"),
                -_threshold_times_atr(
                    features.get("bear_snap_velocity_threshold_atr"),
                    atr,
                    MIN_BEAR_SNAP_VELOCITY_DELTA_ATR,
                )
                if atr is not None
                else None,
                "lte",
                "high",
            ),
            _numeric_attribution(
                conditions,
                "bear_snap_location_ok",
                "location",
                close,
                _bear_location_required(diagnostics),
                "gte",
                "medium",
            ),
            _numeric_attribution(
                conditions,
                "prior_bars_since_bear_snap_gt_cooldown",
                "cooldown",
                state.get("prior_bars_since_bear_snap"),
                BEAR_SNAP_COOLDOWN_BARS,
                "gt",
                "medium",
            ),
            _categorical_attribution(conditions, "session_allowed", "session", state.get("session_allowed"), True, "high"),
        ]
    )


def _compact_attributions(items: list[dict[str, Any] | None]) -> list[dict[str, Any]]:
    return [item for item in items if item is not None]


def _numeric_attribution(
    conditions: Mapping[str, Any],
    predicate_name: str,
    category: str,
    actual_value: object,
    required_value: object,
    comparator: str,
    importance: str,
) -> dict[str, Any] | None:
    actual = _decimal_or_none(actual_value)
    required = _decimal_or_none(required_value)
    if actual is None or required is None:
        return None
    if comparator == "gte":
        distance = max(Decimal("0"), required - actual)
    elif comparator == "lte":
        distance = max(Decimal("0"), actual - required)
    elif comparator == "gt":
        distance = Decimal("0") if actual > required else required - actual + Decimal("1")
    else:
        return None
    normalized = Decimal("0") if distance == 0 else distance / max(abs(required), Decimal("1"))
    return {
        "predicate_name": predicate_name,
        "category": category,
        "passed": conditions.get(predicate_name) is True,
        "actual_value": str(actual),
        "required_value": str(required),
        "comparator": comparator,
        "distance_to_pass": str(distance),
        "normalized_distance_to_pass": str(normalized),
        "importance": importance,
    }


def _categorical_attribution(
    conditions: Mapping[str, Any],
    predicate_name: str,
    category: str,
    actual_value: object,
    required_value: object,
    importance: str,
) -> dict[str, Any]:
    return {
        "predicate_name": predicate_name,
        "category": category,
        "passed": conditions.get(predicate_name) is True,
        "actual_value": actual_value,
        "required_value": required_value,
        "comparator": "equals",
        "distance_to_pass": None,
        "normalized_distance_to_pass": None,
        "importance": importance,
    }


def _threshold_times_atr(value: object, atr: Decimal | None, default: Decimal) -> Decimal | None:
    if atr is None:
        return None
    return (_decimal_or_none(value) or default) * atr


def _bull_location_required(diagnostics: Mapping[str, Any]) -> Decimal | None:
    slow = _decimal_or_none(diagnostics.get("turn_ema_slow"))
    atr = _decimal_or_none(diagnostics.get("atr"))
    if slow is None:
        return None
    if BULL_SNAP_REQUIRE_CLOSE_BELOW_SLOW_EMA:
        return slow
    return slow + BULL_SNAP_MAX_CLOSE_VS_SLOW_EMA_ATR * atr if atr is not None else slow


def _bear_location_required(diagnostics: Mapping[str, Any]) -> Decimal | None:
    slow = _decimal_or_none(diagnostics.get("turn_ema_slow"))
    atr = _decimal_or_none(diagnostics.get("atr"))
    if slow is None:
        return None
    if BEAR_SNAP_REQUIRE_CLOSE_ABOVE_SLOW_EMA:
        return slow + BEAR_SNAP_MIN_CLOSE_VS_SLOW_EMA_ATR * atr if atr is not None else slow
    if BEAR_SNAP_MIN_CLOSE_VS_SLOW_EMA_ATR > 0 and atr is not None:
        return slow - BEAR_SNAP_MIN_CLOSE_VS_SLOW_EMA_ATR * atr
    return slow


def _nearest_failed_numeric_predicate(predicate_attributions: list[dict[str, Any]]) -> dict[str, Any] | None:
    failed_numeric = [
        item
        for item in predicate_attributions
        if item.get("passed") is False and item.get("distance_to_pass") is not None
    ]
    if not failed_numeric:
        return None
    return min(failed_numeric, key=lambda item: _decimal_or_none(item.get("distance_to_pass")) or Decimal("Infinity"))


def _classify_strategy_detail(
    *,
    detail: Mapping[str, Any],
    runtime_cycle_report: Mapping[str, Any],
    previous_state: Mapping[str, Any],
    config: TrackBDecisionJournalConfig,
) -> tuple[str, str]:
    if _is_tier_3(detail, runtime_cycle_report):
        return "TIER_3_SIGNAL_TRADE_DECISION", "signal, paper eligibility, or broker mutation state requires full record"
    if _is_tier_4(detail, runtime_cycle_report, previous_state):
        return "TIER_4_ABNORMAL_SAFETY", "abnormal safety state, suppression, block, or NOT_READY transition requires full record"
    if _is_near_miss(detail, config):
        return "TIER_2_NEAR_MISS", "near-miss threshold or nearest predicate distance was met"
    return "TIER_1_NO_SETUP_AGGREGATE", "ordinary no-setup evaluation was aggregated"


def _is_tier_3(detail: Mapping[str, Any], runtime_cycle_report: Mapping[str, Any]) -> bool:
    if detail.get("signal_emitted") is True:
        return True
    if str(detail.get("strategy_runtime_verdict") or "").endswith("SIGNAL_READY_NO_SUBMIT"):
        return True
    return any(
        bool(runtime_cycle_report.get(key))
        for key in ("paper_submit_requested", "paper_proof_invoked", "submit_attempted", "broker_state_mutated")
    )


def _is_tier_4(
    detail: Mapping[str, Any],
    runtime_cycle_report: Mapping[str, Any],
    previous_state: Mapping[str, Any],
) -> bool:
    strategy_id = str(detail.get("strategy_id") or "")
    previous = previous_state.get(strategy_id) if isinstance(previous_state.get(strategy_id), Mapping) else {}
    runtime_verdict = str(detail.get("strategy_runtime_verdict") or "")
    primary_blocker = str(detail.get("primary_blocker") or "")
    if runtime_cycle_report.get("live_money_readiness") is True:
        return True
    if runtime_cycle_report.get("suppressed_signals"):
        return True
    if "ARBITRATION_BLOCKED" in str(runtime_cycle_report.get("multi_strategy_runtime_cycle_verdict") or ""):
        return True
    if "BLOCKED" in runtime_verdict or "ERROR" in runtime_verdict:
        return True
    if runtime_verdict == "NOT_READY" and previous.get("was_evaluable") is True:
        return True
    if "stale" in primary_blocker.lower():
        return True
    if "side mismatch" in primary_blocker.lower() or "demo" in primary_blocker.lower():
        return True
    return False


def _is_near_miss(detail: Mapping[str, Any], config: TrackBDecisionJournalConfig) -> bool:
    score = _decimal_or_none(detail.get("near_miss_score")) or Decimal("0")
    if score >= config.near_miss_score_threshold:
        return True
    total = int(detail.get("total_predicates") or 0)
    passed = int(detail.get("passed_predicates") or 0)
    if total >= 4 and passed >= total - 1:
        return True
    nearest = detail.get("nearest_failed_predicate") if isinstance(detail.get("nearest_failed_predicate"), Mapping) else {}
    distance = _decimal_or_none(nearest.get("distance_to_pass"))
    return distance is not None and distance <= config.near_miss_distance_threshold


def _strategy_decision_summary(detail: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "strategy_id": detail.get("strategy_id"),
        "strategy_runtime_verdict": detail.get("strategy_runtime_verdict"),
        "tier_selected": detail.get("journal_tier"),
        "passed_required_predicate_count": detail.get("passed_required_predicate_count") or detail.get("passed_predicates"),
        "failed_required_predicate_count": detail.get("failed_required_predicate_count"),
        "near_miss_score": detail.get("near_miss_score"),
        "nearest_failed_numeric_predicate": detail.get("nearest_failed_numeric_predicate"),
        "nearest_failed_numeric_distance": detail.get("nearest_failed_numeric_distance"),
        "latest_evaluated_bar_timestamp": detail.get("latest_evaluated_bar_timestamp"),
    }


def _update_no_setup_aggregate(
    aggregates: dict[str, Any],
    detail: Mapping[str, Any],
    now: datetime,
    config: TrackBDecisionJournalConfig,
) -> None:
    bucket_start = _bucket_start(now, config.no_setup_aggregate_interval_minutes)
    key = f"{detail.get('strategy_id')}|{bucket_start.isoformat()}"
    item = aggregates.get(key) if isinstance(aggregates.get(key), Mapping) else {}
    count = int(item.get("count") or 0) + 1
    max_score = max(float(item.get("max_near_miss_score") or 0.0), float(detail.get("near_miss_score") or 0.0))
    aggregates[key] = {
        "tier": "TIER_1_NO_SETUP_AGGREGATE",
        "strategy_id": detail.get("strategy_id"),
        "time_bucket_start": bucket_start.isoformat(),
        "time_bucket_end": (bucket_start + timedelta(minutes=config.no_setup_aggregate_interval_minutes)).isoformat(),
        "count": count,
        "top_coarse_blocker": detail.get("primary_blocker") or "conditions_not_met",
        "max_near_miss_score": max_score,
        "latest_near_miss_score": detail.get("near_miss_score"),
        "passed_required_predicate_count": detail.get("passed_required_predicate_count") or detail.get("passed_predicates"),
        "failed_required_predicate_count": detail.get("failed_required_predicate_count"),
        "nearest_failed_numeric_predicate": detail.get("nearest_failed_numeric_predicate"),
        "nearest_failed_numeric_distance": detail.get("nearest_failed_numeric_distance"),
        "tier_selected": detail.get("journal_tier"),
        "latest_evaluated_bar": detail.get("latest_evaluated_bar_timestamp"),
        "latest_strategy_report_json": detail.get("strategy_report_json"),
    }


def _full_record(
    runtime_cycle_report: Mapping[str, Any],
    detail: Mapping[str, Any],
    runtime_cycle_report_json: Path | None,
    now: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_decision_journal_record_v1",
        "generated_at": now.isoformat(),
        "journal_tier": detail.get("journal_tier"),
        "journal_tier_reason": detail.get("journal_tier_reason"),
        "runtime_cycle_id": runtime_cycle_report.get("track_b_multi_strategy_runtime_cycle_id"),
        "runtime_cycle_verdict": runtime_cycle_report.get("multi_strategy_runtime_cycle_verdict"),
        "runtime_cycle_report_json": None if runtime_cycle_report_json is None else str(runtime_cycle_report_json),
        "runtime_cycle_report_sha256": _sha256_file(runtime_cycle_report_json),
        "strategy_id": detail.get("strategy_id"),
        "rule_mode": detail.get("rule_mode"),
        "strategy_runtime_verdict": detail.get("strategy_runtime_verdict"),
        "decision": detail.get("decision"),
        "decision_reason": detail.get("decision_reason"),
        "signal_emitted": detail.get("signal_emitted"),
        "signal_direction": detail.get("signal_direction"),
        "signal_source": detail.get("signal_source"),
        "real_strategy_signal": detail.get("real_strategy_signal"),
        "near_miss_score": detail.get("near_miss_score"),
        "passed_predicates": detail.get("passed_predicates"),
        "total_predicates": detail.get("total_predicates"),
        "failed_predicates": detail.get("failed_predicates"),
        "nearest_failed_predicate": detail.get("nearest_failed_predicate"),
        "nearest_failed_numeric_predicate": detail.get("nearest_failed_numeric_predicate"),
        "nearest_failed_numeric_distance": detail.get("nearest_failed_numeric_distance"),
        "predicate_attributions": detail.get("predicate_attributions"),
        "rule_blockers": detail.get("rule_blockers"),
        "primary_blocker": detail.get("primary_blocker"),
        "strategy_report_json": detail.get("strategy_report_json"),
        "strategy_report_sha256": detail.get("strategy_report_sha256"),
        "input_event_path": detail.get("input_event_path"),
        "input_event_sha256": detail.get("input_event_sha256"),
        "latest_evaluated_bar_timestamp": detail.get("latest_evaluated_bar_timestamp"),
        "candidate_signal_count": len(runtime_cycle_report.get("candidate_signals") or []),
        "suppressed_signal_count": len(runtime_cycle_report.get("suppressed_signals") or []),
        "arbitration_result": runtime_cycle_report.get("arbitration_result") or {},
        "readiness_invoked": bool(runtime_cycle_report.get("readiness_invoked")),
        "paper_proof_invoked": bool(runtime_cycle_report.get("paper_proof_invoked")),
        "submit_attempted": bool(runtime_cycle_report.get("submit_attempted")),
        "broker_state_mutated": bool(runtime_cycle_report.get("broker_state_mutated")),
        "live_money_readiness": bool(runtime_cycle_report.get("live_money_readiness")),
    }


def _heartbeat_record(
    runtime_cycle_report: Mapping[str, Any],
    strategy_details: list[Mapping[str, Any]],
    runtime_cycle_report_json: Path | None,
    now: datetime,
) -> dict[str, Any]:
    counts = Counter(str(detail.get("journal_tier") or "UNKNOWN") for detail in strategy_details)
    return {
        "schema_version": "track_b_runtime_heartbeat_v1",
        "generated_at": now.isoformat(),
        "runtime_cycle_id": runtime_cycle_report.get("track_b_multi_strategy_runtime_cycle_id"),
        "runtime_cycle_verdict": runtime_cycle_report.get("multi_strategy_runtime_cycle_verdict"),
        "runtime_cycle_report_json": None if runtime_cycle_report_json is None else str(runtime_cycle_report_json),
        "evaluated_strategy_count": len(strategy_details),
        "evaluated_strategies": [detail.get("strategy_id") for detail in strategy_details],
        "candidate_signal_count": len(runtime_cycle_report.get("candidate_signals") or []),
        "suppressed_signal_count": len(runtime_cycle_report.get("suppressed_signals") or []),
        "journal_tier_counts": dict(counts),
        "latest_bar_timestamp": _latest_bar_across_details(strategy_details),
        "operator_status_invoked": bool(runtime_cycle_report.get("operator_status_invoked")),
        "submit_attempted": bool(runtime_cycle_report.get("submit_attempted")),
        "broker_state_mutated": bool(runtime_cycle_report.get("broker_state_mutated")),
        "paper_proof_invoked": bool(runtime_cycle_report.get("paper_proof_invoked")),
        "live_money_readiness": bool(runtime_cycle_report.get("live_money_readiness")),
    }


def _next_strategy_state(
    previous_state: Mapping[str, Any],
    strategy_details: list[Mapping[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    next_state = dict(previous_state)
    for detail in strategy_details:
        strategy_id = str(detail.get("strategy_id") or "")
        verdict = str(detail.get("strategy_runtime_verdict") or "")
        next_state[strategy_id] = {
            "last_seen_at": now.isoformat(),
            "last_strategy_runtime_verdict": verdict,
            "was_evaluable": verdict != "NOT_READY",
        }
    return next_state


def _bucket_start(now: datetime, interval_minutes: int) -> datetime:
    interval = max(interval_minutes, 1)
    minute = (now.minute // interval) * interval
    return now.replace(minute=minute, second=0, microsecond=0)


def _latest_bar_timestamp(strategy_report: Mapping[str, Any], input_event: Mapping[str, Any]) -> str | None:
    for key in ("candle_timestamp", "quote_timestamp", "generated_at"):
        value = strategy_report.get(key) or input_event.get(key)
        if value:
            return str(value)
    return None


def _latest_bar_across_details(strategy_details: list[Mapping[str, Any]]) -> str | None:
    values = [str(detail.get("latest_evaluated_bar_timestamp")) for detail in strategy_details if detail.get("latest_evaluated_bar_timestamp")]
    return max(values) if values else None


def _rotate_jsonl_if_needed(path: Path, max_bytes: int, rotated_keep: int) -> list[str]:
    if max_bytes <= 0 or not path.exists() or path.stat().st_size <= max_bytes:
        return []
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S-%f")
    rotated = path.with_name(f"{path.stem}.{stamp}{path.suffix}.gz")
    with path.open("rb") as source, gzip.open(rotated, "wb") as target:
        target.write(source.read())
    path.write_text("", encoding="utf-8")
    deleted = _enforce_retention(path, rotated_keep)
    return [str(rotated), *deleted]


def _enforce_retention(active_path: Path, keep: int) -> list[str]:
    if keep < 0:
        keep = 0
    pattern = f"{active_path.stem}.*{active_path.suffix}.gz"
    rotated = sorted(active_path.parent.glob(pattern), key=lambda item: item.stat().st_mtime, reverse=True)
    deleted: list[str] = []
    for old_path in rotated[keep:]:
        if old_path.parent != active_path.parent:
            continue
        old_path.unlink(missing_ok=True)
        deleted.append(str(old_path))
    return deleted


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(to_jsonable(payload), sort_keys=True) + "\n")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(payload), indent=2, sort_keys=True), encoding="utf-8")


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha256_file(path_value: object) -> str | None:
    if not path_value:
        return None
    path = Path(str(path_value))
    if not path.exists() or not path.is_file():
        return None
    try:
        if path.stat().st_size > MAX_HASH_BYTES:
            return None
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(65536), b""):
                digest.update(chunk)
        return digest.hexdigest()
    except OSError:
        return None


def _env_decimal(key: str, default: Decimal) -> Decimal:
    raw = os.environ.get(key)
    if not raw:
        return default
    return _decimal_or_none(raw) or default


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _parse_datetime(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
