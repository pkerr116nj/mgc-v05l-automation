"""Replay/paper strategy-study artifacts built from persisted runtime truth."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

from sqlalchemy import asc, select

from ..config_models import StrategySettings
from ..domain.enums import OrderIntentType, PositionSide, StrategyStatus
from ..domain.models import Bar, StrategyState
from ..persistence.repositories import (
    RepositorySet,
    decode_fill,
    decode_order_intent,
    decode_strategy_state,
)
from ..persistence.tables import features_table, signals_table, strategy_state_snapshots_table
from ..research.trend_participation.features import build_feature_states
from ..research.trend_participation.models import ResearchBar
from ..research.trend_participation.phase2_continuation import classify_entry_states
from ..research.trend_participation.phase3_timing import classify_timing_states
from ..strategy.strategy_engine import (
    _bar_matches_probationary_session_restriction,
    _gc_mgc_asia_retest_hold_london_open_extension_matches,
)
from ..strategy.trade_state import build_initial_state
from .execution_truth import (
    AUTHORITATIVE_INTRABAR_ENTRY_ONLY,
    BASELINE_NEXT_BAR_OPEN,
    BASELINE_FILL_TRUTH,
    ENRICHED_EXECUTION_TRUTH,
    BASELINE_PARITY_ONLY,
    FULL_AUTHORITATIVE_LIFECYCLE,
    HYBRID_ENTRY_BASELINE_EXIT_TRUTH,
    HYBRID_AUTHORITATIVE_ENTRY_BASELINE_EXIT,
    UNSUPPORTED_ENTRY_MODEL,
    normalize_trade_lifecycle_records,
    resolve_execution_truth,
    ExecutionTruthEmitterContext,
)
from .session_phase_labels import label_session_phase


def _dashboard_summary_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def build_strategy_study_dashboard_meta(meta_payload: dict[str, Any] | None) -> dict[str, Any]:
    meta = dict(meta_payload or {})
    coverage_range = dict(meta.get("coverage_range") or {})
    timeframe_truth = dict(meta.get("timeframe_truth") or {})
    truth_provenance = dict(meta.get("truth_provenance") or {})
    compacted_meta = {
        "study_id": meta.get("study_id"),
        "symbol": meta.get("symbol"),
        "strategy_id": meta.get("strategy_id"),
        "candidate_id": meta.get("candidate_id"),
        "strategy_family": meta.get("strategy_family"),
        "study_mode": meta.get("study_mode"),
        "entry_model": meta.get("entry_model"),
        "active_entry_model": meta.get("active_entry_model"),
        "supported_entry_models": list(meta.get("supported_entry_models") or []),
        "entry_model_supported": meta.get("entry_model_supported"),
        "execution_truth_emitter": meta.get("execution_truth_emitter"),
        "intrabar_execution_authoritative": meta.get("intrabar_execution_authoritative"),
        "authoritative_intrabar_available": meta.get("authoritative_intrabar_available"),
        "authoritative_entry_truth_available": meta.get("authoritative_entry_truth_available"),
        "authoritative_exit_truth_available": meta.get("authoritative_exit_truth_available"),
        "authoritative_trade_lifecycle_available": meta.get("authoritative_trade_lifecycle_available"),
        "pnl_truth_basis": meta.get("pnl_truth_basis"),
        "lifecycle_truth_class": meta.get("lifecycle_truth_class"),
        "unsupported_reason": meta.get("unsupported_reason"),
        "context_resolution": meta.get("context_resolution"),
        "execution_resolution": meta.get("execution_resolution"),
        "coverage_start": meta.get("coverage_start") or coverage_range.get("start_timestamp"),
        "coverage_end": meta.get("coverage_end") or coverage_range.get("end_timestamp"),
        "coverage_range": {
            "start_timestamp": coverage_range.get("start_timestamp"),
            "end_timestamp": coverage_range.get("end_timestamp"),
        },
        "timeframe_truth": {
            "structural_signal_timeframe": timeframe_truth.get("structural_signal_timeframe"),
            "execution_timeframe": timeframe_truth.get("execution_timeframe"),
            "artifact_timeframe": timeframe_truth.get("artifact_timeframe"),
            "execution_timeframe_role": timeframe_truth.get("execution_timeframe_role"),
        },
        "truth_provenance": {
            "run_mode": truth_provenance.get("run_mode"),
            "run_lane": truth_provenance.get("run_lane"),
            "source_artifact": truth_provenance.get("source_artifact"),
        },
        "available_overlay_flags": dict(meta.get("available_overlay_flags") or {}),
    }
    return compacted_meta


def build_strategy_study_dashboard_summary(summary_payload: dict[str, Any] | None) -> dict[str, Any]:
    summary = dict(summary_payload or {})
    closed_trade_rows = list(summary.get("closed_trade_breakdown") or [])
    if not closed_trade_rows:
        if "closed_trade_count" not in summary:
            summary["closed_trade_count"] = 0
        if "calendar_breakdown" not in summary:
            summary["calendar_breakdown"] = []
        return summary

    calendar_by_date: dict[str, dict[str, Any]] = {}
    for row in closed_trade_rows:
        timestamp = str(
            row.get("exit_timestamp")
            or row.get("exit_ts")
            or row.get("entry_timestamp")
            or row.get("entry_ts")
            or ""
        ).strip()
        if not timestamp:
            continue
        date_key = timestamp.split("T", 1)[0].strip()
        if not date_key:
            continue
        bucket = calendar_by_date.setdefault(
            date_key,
            {
                "date": date_key,
                "realized_pnl": Decimal("0"),
                "trade_count": 0,
            },
        )
        pnl_value = _dashboard_summary_decimal(
            row.get("realized_pnl")
            or row.get("pnl_cash")
            or row.get("net_pnl")
            or row.get("pnl_points")
        )
        if pnl_value is not None:
            bucket["realized_pnl"] += pnl_value
        bucket["trade_count"] = int(bucket.get("trade_count") or 0) + 1

    compacted_summary = dict(summary)
    compacted_summary["closed_trade_count"] = len(closed_trade_rows)
    compacted_summary["calendar_breakdown"] = [
        {
            "date": date_key,
            "realized_pnl": str(bucket["realized_pnl"]),
            "trade_count": int(bucket["trade_count"]),
        }
        for date_key, bucket in sorted(calendar_by_date.items())
    ]
    compacted_summary.pop("closed_trade_breakdown", None)
    return compacted_summary


def build_strategy_study(
    *,
    repositories: RepositorySet,
    settings: StrategySettings,
    bars: Sequence[Bar],
    source_bars: Sequence[Bar] | None,
    point_value: Decimal | None,
    standalone_strategy_id: str | None,
    strategy_family: str | None,
    instrument: str,
    run_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a per-bar study payload from persisted replay/paper truth."""
    ordered_bars = list(sorted(bars, key=lambda row: row.end_ts))
    if not ordered_bars:
        return {
            "contract_version": "strategy_study_v2",
            "generated_at": datetime.now().isoformat(),
            "symbol": instrument,
            "timeframe": settings.resolved_artifact_timeframe,
            "standalone_strategy_id": standalone_strategy_id,
            "strategy_family": strategy_family,
            "rows": [],
            "summary": {
                "bar_count": 0,
                "total_trades": 0,
                "long_trades": 0,
                "short_trades": 0,
                "winners": None,
                "losers": None,
                "profit_factor": None,
                "cumulative_realized_pnl": None,
                "cumulative_total_pnl": None,
                "max_run_up": None,
                "max_drawdown": None,
                "most_common_blocker_codes": [],
                "no_trade_regions": [],
                "session_level_behavior": [],
                "closed_trade_breakdown": [],
                "trade_family_breakdown": [],
                "session_trade_breakdown": [],
                "latest_trade_summary": None,
                "atp_summary": {
                    "available": False,
                    "timing_available": False,
                    "unavailable_reason": "No persisted bars are available for this strategy study run.",
                },
                "pnl_supportable": False,
                "pnl_unavailable_reason": "No persisted bars are available for this strategy study run.",
            },
            "field_provenance": _field_provenance(point_value),
            "run_metadata": dict(run_metadata or {}),
        "meta": {
            "artifact_version": "strategy_study_v2",
            "study_mode": settings.environment_mode.value,
            "active_entry_model": BASELINE_NEXT_BAR_OPEN,
            "entry_model": BASELINE_NEXT_BAR_OPEN,
            "supported_entry_models": [BASELINE_NEXT_BAR_OPEN],
            "entry_model_supported": True,
            "execution_truth_emitter": "baseline_parity_emitter",
            "intrabar_execution_authoritative": False,
            "authoritative_intrabar_available": False,
            "authoritative_entry_truth_available": False,
            "authoritative_exit_truth_available": False,
            "authoritative_trade_lifecycle_available": False,
            "pnl_truth_basis": BASELINE_FILL_TRUTH,
            "lifecycle_truth_class": BASELINE_PARITY_ONLY,
            "truth_provenance": _build_truth_provenance(
                run_metadata=run_metadata,
                study_mode=settings.environment_mode.value,
            ),
            "timeframe_truth": {
                "structural_signal_timeframe": settings.resolved_structural_signal_timeframe,
                "execution_timeframe": settings.resolved_execution_timeframe,
                    "artifact_timeframe": settings.resolved_artifact_timeframe,
                    "execution_timeframe_role": settings.execution_timeframe_role.value,
                },
            },
        }

    ordered_source_bars = list(sorted(source_bars or ordered_bars, key=lambda row: row.end_ts))
    feature_by_bar_id = _load_feature_payloads(repositories, [bar.bar_id for bar in ordered_bars])
    signal_by_bar_id = _load_signal_payloads(repositories, [bar.bar_id for bar in ordered_bars])
    state_snapshots = _load_state_snapshots(repositories, standalone_strategy_id)
    order_rows = sorted(repositories.order_intents.list_all(), key=lambda row: str(row.get("created_at") or ""))
    fill_rows = sorted(repositories.fills.list_all(), key=lambda row: str(row.get("fill_timestamp") or ""))
    entry_intents_by_bar_id, exit_intents_by_bar_id = _group_intents(order_rows)
    fills_by_bar_id = _group_fills_by_bar(ordered_bars, fill_rows)
    closed_trades = _pair_closed_trades(order_rows=order_rows, fill_rows=fill_rows, point_value=point_value)
    closed_trades_by_bar_id = _group_closed_trades_by_bar(ordered_bars, closed_trades)
    priced_trade_count = sum(1 for trade in closed_trades if trade.get("net_pnl") is not None)

    initial_state = build_initial_state(ordered_bars[0].start_ts)
    current_state = initial_state
    current_transition_label: str | None = None
    snapshot_index = 0
    realized_running = Decimal("0") if point_value is not None else None
    atp_feature_rows = _build_atp_feature_rows(ordered_bars, ordered_source_bars)
    atp_feature_by_bar_id = {
        ordered_bars[index].bar_id: feature
        for index, feature in enumerate(atp_feature_rows)
        if index < len(ordered_bars)
    }
    atp_entry_states_by_bar_id: dict[str, Any] = {}
    base_rows: list[dict[str, Any]] = []
    blocker_counter: Counter[str] = Counter()

    for bar_index, bar in enumerate(ordered_bars):
        while snapshot_index < len(state_snapshots) and state_snapshots[snapshot_index]["updated_at"] <= bar.end_ts:
            current_state = state_snapshots[snapshot_index]["state"]
            current_transition_label = state_snapshots[snapshot_index]["transition_label"]
            snapshot_index += 1

        feature = feature_by_bar_id.get(bar.bar_id) or {}
        signal = signal_by_bar_id.get(bar.bar_id) or {}
        entry_intents = list(entry_intents_by_bar_id.get(bar.bar_id) or [])
        exit_intents = list(exit_intents_by_bar_id.get(bar.bar_id) or [])
        fills = list(fills_by_bar_id.get(bar.bar_id) or [])
        fill_markers = _build_fill_marker_rows(fills)
        closed_here = list(closed_trades_by_bar_id.get(bar.bar_id) or [])
        realized_delta = _sum_decimal(trade.get("net_pnl") for trade in closed_here) if point_value is not None else None
        if realized_running is not None:
            realized_running += realized_delta or Decimal("0")
        unrealized = _compute_unrealized_pnl(
            state=current_state,
            close_price=bar.close,
            point_value=point_value,
        )
        cumulative_total = realized_running + unrealized if realized_running is not None and unrealized is not None else None
        eligibility = _derive_entry_eligibility(
            bar=bar,
            bar_index=bar_index,
            settings=settings,
            state=current_state,
            signal=signal,
            entry_intents=entry_intents,
        )
        blocker_code = eligibility.get("blocker_code")
        if blocker_code:
            blocker_counter[str(blocker_code)] += 1

        current_position_family = None
        if current_state.position_side is PositionSide.LONG and str(current_state.long_entry_family.value) != "NONE":
            current_position_family = current_state.long_entry_family.value
        elif current_state.position_side is PositionSide.SHORT and str(current_state.short_entry_family.value) != "NONE":
            current_position_family = current_state.short_entry_family.value

        atp_feature = atp_feature_by_bar_id.get(bar.bar_id)
        atp_entry_state = _compute_atp_entry_state(
            feature_rows=atp_feature_rows,
            bar_index=bar_index,
            state=current_state,
        )
        if atp_entry_state is not None:
            atp_entry_states_by_bar_id[bar.bar_id] = atp_entry_state

        base_rows.append(
            {
            "bar_id": bar.bar_id,
            "timestamp": bar.end_ts.isoformat(),
            "start_timestamp": bar.start_ts.isoformat(),
            "end_timestamp": bar.end_ts.isoformat(),
            "session_phase": label_session_phase(bar.end_ts),
            "open": str(bar.open),
            "high": str(bar.high),
            "low": str(bar.low),
            "close": str(bar.close),
            "session_vwap": _decimal_to_str(feature.get("vwap")),
            "atr": _decimal_to_str(feature.get("atr")),
            "position_side": current_state.position_side.value,
            "position_qty": current_state.internal_position_qty,
            "position_phase": None,
            "strategy_status": current_state.strategy_status.value,
            "transition_label": current_transition_label,
            "current_position_family": current_position_family,
            "entry_markers": _build_marker_rows(entry_intents, fills, entry_only=True),
            "exit_markers": _build_marker_rows(exit_intents, fills, exit_only=True),
            "fill_markers": fill_markers,
            "entry_marker": bool(entry_intents) or any(marker["is_entry"] for marker in fill_markers),
            "exit_marker": bool(exit_intents) or any(marker["is_exit"] for marker in fill_markers),
            "fill_marker": bool(fills),
            "realized_pnl": _decimal_to_str(realized_delta),
            "unrealized_pnl": _decimal_to_str(unrealized),
            "cumulative_realized_pnl": _decimal_to_str(realized_running),
            "cumulative_total_pnl": _decimal_to_str(cumulative_total),
            "current_bias_state": _normalize_atp_value(
                feature.get("atp_bias_state") or getattr(atp_feature, "atp_bias_state", None)
            ),
            "current_pullback_state": _normalize_atp_value(
                feature.get("atp_pullback_state") or getattr(atp_feature, "atp_pullback_state", None)
            ),
            "pullback_envelope_band": _normalize_atp_value(
                feature.get("atp_pullback_envelope_state") or getattr(atp_feature, "atp_pullback_envelope_state", None)
            ),
            "pullback_depth_score": _float_or_none(
                feature.get("atp_pullback_depth_score", getattr(atp_feature, "atp_pullback_depth_score", None))
            ),
            "pullback_violence_score": _float_or_none(
                feature.get("atp_pullback_violence_score", getattr(atp_feature, "atp_pullback_violence_score", None))
            ),
            "entry_eligible": bool(eligibility.get("entry_eligible")),
            "entry_blocked": bool(eligibility.get("entry_blocked")),
            "blocker_code": blocker_code,
            "legacy_entry_eligible": bool(eligibility.get("entry_eligible")),
            "legacy_entry_blocked": bool(eligibility.get("entry_blocked")),
            "legacy_blocker_code": blocker_code,
            "latest_signal_side": eligibility.get("candidate_side"),
            "latest_signal_source": eligibility.get("candidate_source"),
            "latest_signal_state": eligibility.get("signal_state"),
            "legacy_latest_signal_side": eligibility.get("candidate_side"),
            "legacy_latest_signal_source": eligibility.get("candidate_source"),
            "legacy_latest_signal_state": eligibility.get("signal_state"),
            "continuation_state": _normalize_atp_value(
                signal.get("atp_continuation_trigger_state") or getattr(atp_entry_state, "continuation_trigger_state", None)
            ),
            "atp_entry_state": _normalize_atp_value(signal.get("atp_entry_state") or getattr(atp_entry_state, "entry_state", None)),
            "atp_entry_ready": bool(
                signal.get("atp_entry_state") == "ENTRY_ELIGIBLE"
                or signal.get("atp_entry_ready") is True
                or getattr(atp_entry_state, "entry_eligible", False)
            ),
            "atp_entry_blocked": bool(
                signal.get("atp_entry_state") == "ENTRY_BLOCKED"
                or signal.get("atp_entry_blocked") is True
                or (
                    getattr(atp_entry_state, "entry_state", None) is not None
                    and getattr(atp_entry_state, "entry_eligible", False) is False
                )
            ),
            "atp_entry_blocker_code": _normalize_atp_value(
                signal.get("atp_primary_blocker") or getattr(atp_entry_state, "primary_blocker", None)
            ),
            "atp_timing_state": _normalize_atp_value(signal.get("atp_timing_state")),
            "atp_timing_confirmed": _bool_or_none(signal.get("atp_timing_confirmed")),
            "atp_timing_executable": _bool_or_none(signal.get("atp_timing_executable")),
            "atp_timing_blocker_code": _normalize_atp_value(signal.get("atp_timing_blocker")),
            "atp_blocker_code": _normalize_atp_value(
                signal.get("atp_timing_blocker") or signal.get("atp_primary_blocker") or getattr(atp_entry_state, "primary_blocker", None)
            ),
            "atp_timing_bar_timestamp": _normalize_atp_value(signal.get("atp_timing_bar_timestamp")),
            "vwap_entry_quality_state": _normalize_atp_value(signal.get("atp_vwap_price_quality_state")),
            "atp_family_name": _normalize_atp_value(
                signal.get("atp_family_name") or getattr(atp_entry_state, "family_name", None)
            ),
            "entry_source_family": eligibility.get("candidate_source") or current_position_family,
            "signal_snapshot": {
                "long_entry_raw": bool(signal.get("long_entry_raw", False)),
                "short_entry_raw": bool(signal.get("short_entry_raw", False)),
                "long_entry": bool(signal.get("long_entry", False)),
                "short_entry": bool(signal.get("short_entry", False)),
                "long_entry_source": signal.get("long_entry_source"),
                "short_entry_source": signal.get("short_entry_source"),
                "recent_long_setup": bool(signal.get("recent_long_setup", False)),
                "recent_short_setup": bool(signal.get("recent_short_setup", False)),
            },
            }
        )

    entry_model = _resolve_study_entry_model(
        settings=settings,
        bars=ordered_bars,
        source_bars=ordered_source_bars,
    )
    atp_timing_by_bar_id = _build_atp_timing_states_by_bar_id(
        bars=ordered_bars,
        source_bars=ordered_source_bars,
        entry_states_by_bar_id=atp_entry_states_by_bar_id,
    )
    rows: list[dict[str, Any]] = []
    for row in base_rows:
        timing_state = atp_timing_by_bar_id.get(str(row["bar_id"]))
        if timing_state is not None:
            row["atp_timing_state"] = _normalize_atp_value(getattr(timing_state, "timing_state", None))
            row["atp_timing_confirmed"] = bool(getattr(timing_state, "timing_confirmed", False))
            row["atp_timing_executable"] = bool(getattr(timing_state, "executable_entry", False))
            row["atp_timing_blocker_code"] = _normalize_atp_value(getattr(timing_state, "primary_blocker", None))
            row["atp_blocker_code"] = row["atp_timing_blocker_code"] or row.get("atp_entry_blocker_code")
            row["atp_timing_bar_timestamp"] = (
                getattr(timing_state, "timing_bar_ts", None).isoformat()
                if getattr(timing_state, "timing_bar_ts", None) is not None
                else None
            )
            row["vwap_entry_quality_state"] = _normalize_atp_value(
                getattr(timing_state, "vwap_price_quality_state", None)
            )
        rows.append(row)

    summary = _build_summary(
        rows=rows,
        closed_trades=closed_trades,
        blocker_counter=blocker_counter,
        point_value=point_value,
        priced_trade_count=priced_trade_count,
    )
    execution_truth = resolve_execution_truth(
        ExecutionTruthEmitterContext(
            settings=settings,
            bars=ordered_bars,
            source_bars=ordered_source_bars,
            rows=rows,
            signal_by_bar_id=signal_by_bar_id,
            feature_by_bar_id={
                **feature_by_bar_id,
                "__atp_entry_states_by_bar_id__": atp_entry_states_by_bar_id,
            },
            point_value=point_value,
            strategy_family=strategy_family,
            standalone_strategy_id=standalone_strategy_id,
            instrument=instrument,
            requested_entry_model=entry_model,
        )
    )
    payload = {
        "contract_version": "strategy_study_v2",
        "generated_at": datetime.now(ordered_bars[-1].end_ts.tzinfo or ordered_bars[-1].start_ts.tzinfo).isoformat(),
        "symbol": instrument,
        "timeframe": settings.resolved_artifact_timeframe,
        "standalone_strategy_id": standalone_strategy_id,
        "strategy_family": strategy_family,
        "point_value": _decimal_to_str(point_value),
        "rows": rows,
        "summary": summary,
        "field_provenance": _field_provenance(point_value),
        "run_metadata": dict(run_metadata or {}),
        "meta": {
            "artifact_version": "strategy_study_v2",
            "study_mode": settings.environment_mode.value,
            "active_entry_model": execution_truth.active_entry_model,
            "entry_model": execution_truth.active_entry_model,
            "supported_entry_models": list(execution_truth.supported_entry_models),
            "entry_model_supported": bool(execution_truth.entry_model_supported),
            "execution_truth_emitter": execution_truth.execution_truth_emitter,
            "intrabar_execution_authoritative": bool(execution_truth.authoritative_intrabar_available),
            "authoritative_intrabar_available": bool(execution_truth.authoritative_intrabar_available),
            "authoritative_entry_truth_available": bool(execution_truth.authoritative_entry_truth_available),
            "authoritative_exit_truth_available": bool(execution_truth.authoritative_exit_truth_available),
            "authoritative_trade_lifecycle_available": bool(execution_truth.authoritative_trade_lifecycle_available),
            "pnl_truth_basis": execution_truth.pnl_truth_basis,
            "lifecycle_truth_class": execution_truth.lifecycle_truth_class,
            "unsupported_reason": execution_truth.unsupported_reason,
            "truth_provenance": _build_truth_provenance(
                run_metadata=run_metadata,
                study_mode=settings.environment_mode.value,
            ),
            "timeframe_truth": {
                "structural_signal_timeframe": settings.resolved_structural_signal_timeframe,
                "execution_timeframe": settings.resolved_execution_timeframe,
                "artifact_timeframe": settings.resolved_artifact_timeframe,
                "execution_timeframe_role": settings.execution_timeframe_role.value,
            },
            "entry_model_capabilities": list(execution_truth.capability_rows),
            "authoritative_execution_events": list(execution_truth.authoritative_execution_events),
            "authoritative_trade_lifecycle_records": list(execution_truth.authoritative_trade_lifecycle_records),
            **dict(execution_truth.meta),
        },
    }
    return payload


def build_strategy_study_v3(
    *,
    repositories: RepositorySet,
    settings: StrategySettings,
    bars: Sequence[Bar],
    source_bars: Sequence[Bar] | None,
    point_value: Decimal | None,
    standalone_strategy_id: str | None,
    strategy_family: str | None,
    instrument: str,
    run_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    legacy_payload = build_strategy_study(
        repositories=repositories,
        settings=settings,
        bars=bars,
        source_bars=source_bars,
        point_value=point_value,
        standalone_strategy_id=standalone_strategy_id,
        strategy_family=strategy_family,
        instrument=instrument,
        run_metadata=run_metadata,
    )
    return convert_strategy_study_v2_to_v3(
        legacy_payload,
        bars=bars,
        source_bars=source_bars,
        settings=settings,
    )


def normalize_strategy_study_payload(
    payload: dict[str, Any] | None,
    *,
    bars: Sequence[Bar] | None = None,
    source_bars: Sequence[Bar] | None = None,
    settings: StrategySettings | None = None,
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    if str(payload.get("contract_version") or "").strip() == "strategy_study_v3":
        return _ensure_strategy_study_v3_compatibility(payload)
    return convert_strategy_study_v2_to_v3(
        payload,
        bars=bars,
        source_bars=source_bars,
        settings=settings,
    )


def convert_strategy_study_v2_to_v3(
    payload: dict[str, Any],
    *,
    bars: Sequence[Bar] | None = None,
    source_bars: Sequence[Bar] | None = None,
    settings: StrategySettings | None = None,
) -> dict[str, Any]:
    rows = list(payload.get("rows") or [])
    summary = dict(payload.get("summary") or {})
    run_metadata = dict(payload.get("run_metadata") or {})
    symbol = str(payload.get("symbol") or run_metadata.get("symbol") or "-")
    strategy_id = (
        payload.get("standalone_strategy_id")
        or run_metadata.get("strategy_id")
        or payload.get("strategy_family")
        or None
    )
    candidate_id = run_metadata.get("candidate_id")
    payload_meta = dict(payload.get("meta") or {})
    payload_timeframe_truth = dict(payload_meta.get("timeframe_truth") or {})
    timeframe = payload.get("timeframe") or (settings.resolved_artifact_timeframe if settings is not None else None)
    structural_signal_timeframe = (
        payload_timeframe_truth.get("structural_signal_timeframe")
        or (settings.resolved_structural_signal_timeframe if settings is not None else timeframe)
    )
    execution_timeframe = (
        payload_timeframe_truth.get("execution_timeframe")
        or (settings.resolved_execution_timeframe if settings is not None else None)
    )
    artifact_timeframe = (
        payload_timeframe_truth.get("artifact_timeframe")
        or (settings.resolved_artifact_timeframe if settings is not None else timeframe)
    )
    execution_timeframe_role = (
        payload_timeframe_truth.get("execution_timeframe_role")
        or (settings.execution_timeframe_role.value if settings is not None else None)
    )
    study_mode = (
        payload_meta.get("study_mode")
        or (settings.environment_mode.value if settings is not None else ("research_execution_mode" if source_bars else "baseline_parity_mode"))
    )
    generated_at = payload.get("generated_at") or datetime.now().isoformat()
    converted_bars = _build_v3_bars(rows=rows, bars=bars, default_timeframe=timeframe)
    execution_detail_enabled = (
        study_mode != "baseline_parity_mode"
        or execution_timeframe_role == "execution_detail_only"
        or (
            execution_timeframe is not None
            and structural_signal_timeframe is not None
            and str(execution_timeframe) != str(structural_signal_timeframe)
        )
    )
    execution_slices = _build_v3_execution_slices(
        source_bars=source_bars if execution_detail_enabled else None,
        context_bars=bars,
        context_resolution=str(structural_signal_timeframe or timeframe or ""),
    )
    entry_model = (
        payload_meta.get("entry_model")
        or _infer_entry_model_from_payload(
            study_mode=study_mode,
            execution_slices=execution_slices,
            context_resolution=str(structural_signal_timeframe or timeframe or ""),
            execution_resolution=execution_timeframe,
        )
    )
    authoritative_execution_events = list(payload_meta.get("authoritative_execution_events") or [])
    authoritative_trade_lifecycle_records = list(payload_meta.get("authoritative_trade_lifecycle_records") or [])
    authoritative_timing_states = list(
        payload_meta.get("authoritative_execution_timing_records")
        or payload_meta.get("authoritative_intrabar_timing_states")
        or []
    )
    authoritative_shadow_trades = list(payload_meta.get("authoritative_intrabar_trades") or [])
    if not authoritative_trade_lifecycle_records and authoritative_shadow_trades:
        authoritative_trade_lifecycle_records = authoritative_shadow_trades
    intrabar_execution_authoritative = bool(
        payload_meta.get("intrabar_execution_authoritative")
        or payload_meta.get("authoritative_intrabar_available")
        or authoritative_execution_events
        or authoritative_trade_lifecycle_records
    )
    pnl_truth_basis = (
        payload_meta.get("pnl_truth_basis")
        or (ENRICHED_EXECUTION_TRUTH if intrabar_execution_authoritative else BASELINE_FILL_TRUTH)
    )
    trade_events = _build_v3_trade_events(
        rows,
        authoritative_timing_states=[] if authoritative_execution_events else authoritative_timing_states,
        authoritative_shadow_trades=[] if authoritative_execution_events else authoritative_trade_lifecycle_records,
        authoritative_execution_events=authoritative_execution_events,
        execution_slices=execution_slices,
        entry_model=entry_model,
    )
    pnl_points = _build_v3_pnl_points(
        rows,
        authoritative_shadow_trades=authoritative_trade_lifecycle_records,
        point_value=payload.get("point_value"),
        pnl_truth_basis=pnl_truth_basis,
    )
    summary = _build_v3_summary(
        summary=summary,
        pnl_points=pnl_points,
        authoritative_timing_states=authoritative_timing_states,
        authoritative_shadow_trades=authoritative_trade_lifecycle_records,
        intrabar_execution_authoritative=intrabar_execution_authoritative,
        pnl_truth_basis=pnl_truth_basis,
    )
    coverage_start = None
    coverage_end = None
    if converted_bars:
        coverage_start = converted_bars[0].get("start_timestamp") or converted_bars[0].get("timestamp")
        coverage_end = converted_bars[-1].get("end_timestamp") or converted_bars[-1].get("timestamp")
    context_resolution = str(structural_signal_timeframe or timeframe or "")
    execution_resolution = execution_timeframe or _infer_execution_resolution(
        source_bars=source_bars,
        context_resolution=context_resolution,
    )
    meta = {
        "study_id": _study_id(
            run_id=run_metadata.get("run_stamp"),
            symbol=symbol,
            strategy_id=strategy_id,
            candidate_id=candidate_id,
            context_resolution=context_resolution or None,
            execution_resolution=execution_resolution,
            entry_model=entry_model,
        ),
        "run_id": run_metadata.get("run_stamp"),
        "symbol": symbol,
        "strategy_id": strategy_id,
        "candidate_id": candidate_id,
        "strategy_family": payload.get("strategy_family"),
        "context_resolution": context_resolution or None,
        "execution_resolution": execution_resolution,
        "coverage_start": coverage_start,
        "coverage_end": coverage_end,
        "coverage_range": {
            "start_timestamp": coverage_start,
            "end_timestamp": coverage_end,
        },
        "artifact_version": "strategy_study_v3",
        "study_mode": study_mode,
        "active_entry_model": entry_model,
        "entry_model": entry_model,
        "supported_entry_models": list(payload_meta.get("supported_entry_models") or [entry_model]),
        "entry_model_supported": bool(payload_meta.get("entry_model_supported", True)),
        "execution_truth_emitter": payload_meta.get("execution_truth_emitter"),
        "intrabar_execution_authoritative": intrabar_execution_authoritative,
        "authoritative_intrabar_available": bool(payload_meta.get("authoritative_intrabar_available", intrabar_execution_authoritative)),
        "authoritative_entry_truth_available": bool(
            payload_meta.get(
                "authoritative_entry_truth_available",
                any(str(event.get("execution_event_type") or "").startswith("ENTRY_") for event in trade_events),
            )
        ),
        "authoritative_exit_truth_available": bool(
            payload_meta.get(
                "authoritative_exit_truth_available",
                any(str(event.get("execution_event_type") or "") == "EXIT_TRIGGERED" for event in trade_events),
            )
        ),
        "authoritative_trade_lifecycle_available": bool(
            payload_meta.get("authoritative_trade_lifecycle_available", authoritative_trade_lifecycle_records)
        ),
        "pnl_truth_basis": pnl_truth_basis,
        "lifecycle_truth_class": payload_meta.get(
            "lifecycle_truth_class",
            _classify_lifecycle_truth_class(
                authoritative_entry_truth_available=bool(
                    payload_meta.get(
                        "authoritative_entry_truth_available",
                        any(str(event.get("execution_event_type") or "").startswith("ENTRY_") for event in trade_events),
                    )
                ),
                authoritative_exit_truth_available=bool(
                    payload_meta.get(
                        "authoritative_exit_truth_available",
                        any(str(event.get("execution_event_type") or "") == "EXIT_TRIGGERED" for event in trade_events),
                    )
                ),
                authoritative_trade_lifecycle_available=bool(
                    payload_meta.get("authoritative_trade_lifecycle_available", authoritative_trade_lifecycle_records)
                ),
                pnl_truth_basis=pnl_truth_basis,
                intrabar_execution_authoritative=intrabar_execution_authoritative,
                entry_model_supported=bool(payload_meta.get("entry_model_supported", True)),
            ),
        ),
        "unsupported_reason": payload_meta.get("unsupported_reason"),
        "entry_model_capabilities": list(payload_meta.get("entry_model_capabilities") or []),
        "truth_provenance": dict(
            payload_meta.get("truth_provenance")
            or _build_truth_provenance(
                run_metadata=run_metadata,
                study_mode=study_mode,
            )
        ),
        "timeframe_truth": {
            "structural_signal_timeframe": structural_signal_timeframe,
            "execution_timeframe": execution_resolution,
            "artifact_timeframe": artifact_timeframe,
            "execution_timeframe_role": execution_timeframe_role,
        },
        "available_overlay_flags": _build_v3_overlay_flags(
            bars=converted_bars,
            pnl_points=pnl_points,
            execution_slices=execution_slices,
        ),
    }
    lifecycle_records = normalize_trade_lifecycle_records(
        authoritative_trade_lifecycle_records,
        entry_model=entry_model,
        pnl_truth_basis=pnl_truth_basis,
        lifecycle_truth_class=str(meta["lifecycle_truth_class"]),
        truth_provenance=dict(meta["truth_provenance"]),
        record_source="STUDY_EXECUTION_TRUTH",
    )
    return _ensure_strategy_study_v3_compatibility(
        {
            "contract_version": "strategy_study_v3",
            "generated_at": generated_at,
            "symbol": symbol,
            "timeframe": timeframe,
            "standalone_strategy_id": payload.get("standalone_strategy_id"),
            "strategy_family": payload.get("strategy_family"),
            "point_value": payload.get("point_value"),
            "meta": meta,
            "bars": converted_bars,
            "trade_events": trade_events,
            "pnl_points": pnl_points,
            "execution_slices": execution_slices,
            "lifecycle_records": lifecycle_records,
            "authoritative_trade_lifecycle_records": lifecycle_records,
            "summary": summary,
            "field_provenance": dict(payload.get("field_provenance") or {}),
            "run_metadata": run_metadata,
        }
    )


def _ensure_strategy_study_v3_compatibility(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    bars = list(normalized.get("bars") or normalized.get("rows") or [])
    normalized["bars"] = bars
    normalized["rows"] = bars
    normalized["trade_events"] = list(normalized.get("trade_events") or [])
    normalized["pnl_points"] = list(normalized.get("pnl_points") or [])
    normalized["execution_slices"] = list(normalized.get("execution_slices") or [])
    lifecycle_records = list(
        normalized.get("lifecycle_records")
        or normalized.get("authoritative_trade_lifecycle_records")
        or []
    )
    normalized["lifecycle_records"] = lifecycle_records
    normalized["authoritative_trade_lifecycle_records"] = lifecycle_records
    meta = dict(normalized.get("meta") or {})
    meta.setdefault("artifact_version", "strategy_study_v3")
    meta.setdefault("symbol", normalized.get("symbol"))
    meta.setdefault("strategy_id", normalized.get("standalone_strategy_id") or normalized.get("strategy_family"))
    meta.setdefault("candidate_id", dict(normalized.get("run_metadata") or {}).get("candidate_id"))
    meta.setdefault("context_resolution", normalized.get("timeframe"))
    context_resolution = meta.get("context_resolution") or normalized.get("timeframe")
    timeframe_truth = dict(meta.get("timeframe_truth") or {})
    execution_resolution = (
        meta.get("execution_resolution")
        or timeframe_truth.get("execution_timeframe")
        or context_resolution
        or normalized.get("timeframe")
    )
    meta["context_resolution"] = context_resolution
    meta["execution_resolution"] = execution_resolution
    meta.setdefault(
        "coverage_range",
        {
            "start_timestamp": bars[0].get("start_timestamp") if bars else None,
            "end_timestamp": bars[-1].get("end_timestamp") if bars else None,
        },
    )
    meta.setdefault("coverage_start", dict(meta.get("coverage_range") or {}).get("start_timestamp"))
    meta.setdefault("coverage_end", dict(meta.get("coverage_range") or {}).get("end_timestamp"))
    meta.setdefault("study_mode", "research_execution_mode" if normalized["execution_slices"] else "baseline_parity_mode")
    meta.setdefault(
        "entry_model",
        _infer_entry_model_from_payload(
            study_mode=str(meta.get("study_mode") or "baseline_parity_mode"),
            execution_slices=normalized["execution_slices"],
            context_resolution=context_resolution,
            execution_resolution=execution_resolution,
        ),
    )
    meta.setdefault("active_entry_model", meta.get("entry_model"))
    trade_events = list(normalized["trade_events"])
    meta.setdefault(
        "intrabar_execution_authoritative",
        any(
            str(event.get("source_resolution") or "") == "INTRABAR"
            and bool(event.get("execution_event_type"))
            for event in trade_events
        ),
    )
    meta.setdefault("authoritative_intrabar_available", bool(meta.get("intrabar_execution_authoritative")))
    meta.setdefault(
        "authoritative_entry_truth_available",
        any(str(event.get("execution_event_type") or "").startswith("ENTRY_") for event in trade_events),
    )
    meta.setdefault(
        "authoritative_exit_truth_available",
        any(str(event.get("execution_event_type") or "") == "EXIT_TRIGGERED" for event in trade_events),
    )
    meta.setdefault(
        "authoritative_trade_lifecycle_available",
        bool(meta.get("authoritative_exit_truth_available"))
        or str(meta.get("pnl_truth_basis") or "") in {ENRICHED_EXECUTION_TRUTH, HYBRID_ENTRY_BASELINE_EXIT_TRUTH},
    )
    if not meta.get("supported_entry_models"):
        meta["supported_entry_models"] = [BASELINE_NEXT_BAR_OPEN]
    if meta.get("entry_model_supported") is None:
        meta["entry_model_supported"] = True
    if not meta.get("execution_truth_emitter"):
        meta["execution_truth_emitter"] = "baseline_parity_emitter"
    meta.setdefault(
        "pnl_truth_basis",
        ENRICHED_EXECUTION_TRUTH if meta.get("intrabar_execution_authoritative") else BASELINE_FILL_TRUTH,
    )
    meta.setdefault(
        "lifecycle_truth_class",
        _classify_lifecycle_truth_class(
            authoritative_entry_truth_available=bool(meta.get("authoritative_entry_truth_available")),
            authoritative_exit_truth_available=bool(meta.get("authoritative_exit_truth_available")),
            authoritative_trade_lifecycle_available=bool(meta.get("authoritative_trade_lifecycle_available")),
            pnl_truth_basis=str(meta.get("pnl_truth_basis") or BASELINE_FILL_TRUTH),
            intrabar_execution_authoritative=bool(meta.get("intrabar_execution_authoritative")),
            entry_model_supported=bool(meta.get("entry_model_supported", True)),
        ),
    )
    meta.setdefault("unsupported_reason", None)
    meta.setdefault("entry_model_capabilities", [])
    meta.setdefault(
        "truth_provenance",
        _build_truth_provenance(
            run_metadata=normalized.get("run_metadata"),
            study_mode=str(meta.get("study_mode") or "baseline_parity_mode"),
        ),
    )
    meta["timeframe_truth"] = {
        "structural_signal_timeframe": timeframe_truth.get("structural_signal_timeframe")
        or context_resolution
        or normalized.get("timeframe"),
        "execution_timeframe": timeframe_truth.get("execution_timeframe")
        or execution_resolution
        or context_resolution
        or normalized.get("timeframe"),
        "artifact_timeframe": timeframe_truth.get("artifact_timeframe")
        or normalized.get("timeframe")
        or context_resolution,
        "execution_timeframe_role": timeframe_truth.get("execution_timeframe_role")
        or ("execution_detail_only" if normalized["execution_slices"] else "matches_signal_evaluation"),
    }
    meta.setdefault(
        "study_id",
        _study_id(
            run_id=dict(normalized.get("run_metadata") or {}).get("run_stamp"),
            symbol=str(meta.get("symbol") or normalized.get("symbol") or "-"),
            strategy_id=meta.get("strategy_id"),
            candidate_id=meta.get("candidate_id"),
            context_resolution=meta.get("context_resolution"),
            execution_resolution=meta.get("execution_resolution"),
            entry_model=meta.get("entry_model"),
        ),
    )
    meta.setdefault(
        "available_overlay_flags",
        _build_v3_overlay_flags(
            bars=bars,
            pnl_points=normalized["pnl_points"],
            execution_slices=normalized["execution_slices"],
        ),
    )
    normalized["meta"] = meta
    return normalized


def compact_strategy_study_payload(
    payload: dict[str, Any],
    *,
    max_bars: int = 4000,
    max_pnl_points: int = 4000,
    max_execution_slices: int = 6000,
) -> dict[str, Any]:
    normalized = normalize_strategy_study_payload(payload) or dict(payload)
    if not normalized:
        return dict(payload)

    bars = list(normalized.get("bars") or [])
    pnl_points = list(normalized.get("pnl_points") or [])
    execution_slices = list(normalized.get("execution_slices") or [])
    trade_events = list(normalized.get("trade_events") or [])

    compacted_bars = _sample_dense_series(
        bars,
        max_items=max_bars,
        anchor_ids={str(event.get("linked_bar_id") or "") for event in trade_events if event.get("linked_bar_id")},
        timestamp_key="timestamp",
    )
    compacted_pnl_points = _sample_dense_series(
        pnl_points,
        max_items=max_pnl_points,
        anchor_ids={str(item.get("bar_id") or "") for item in compacted_bars if item.get("bar_id")},
        item_id_key="bar_id",
        timestamp_key="timestamp",
    )
    compacted_execution_slices = _sample_dense_series(
        execution_slices,
        max_items=max_execution_slices,
        anchor_ids={str(event.get("linked_bar_id") or "") for event in trade_events if event.get("linked_bar_id")},
        item_id_key="linked_bar_id",
        timestamp_key="timestamp",
    )

    compacted = dict(normalized)
    compacted["bars"] = compacted_bars
    compacted["rows"] = compacted_bars
    compacted["pnl_points"] = compacted_pnl_points
    compacted["execution_slices"] = compacted_execution_slices
    meta = dict(compacted.get("meta") or {})
    meta["series_compaction"] = {
        "applied": (
            len(compacted_bars) != len(bars)
            or len(compacted_pnl_points) != len(pnl_points)
            or len(compacted_execution_slices) != len(execution_slices)
        ),
        "bars_original_count": len(bars),
        "bars_compacted_count": len(compacted_bars),
        "pnl_points_original_count": len(pnl_points),
        "pnl_points_compacted_count": len(compacted_pnl_points),
        "execution_slices_original_count": len(execution_slices),
        "execution_slices_compacted_count": len(compacted_execution_slices),
        "trade_events_count": len(trade_events),
        "compaction_policy": "uniform_preserve_edges_and_trade_linked_points",
    }
    compacted["meta"] = meta
    return compacted


def build_strategy_study_preview(payload: dict[str, Any]) -> dict[str, Any] | None:
    normalized = normalize_strategy_study_payload(payload) or dict(payload)
    if not isinstance(normalized, dict):
        return None
    return {
        "contract_version": normalized.get("contract_version"),
        "generated_at": normalized.get("generated_at"),
        "symbol": normalized.get("symbol"),
        "timeframe": normalized.get("timeframe"),
        "standalone_strategy_id": normalized.get("standalone_strategy_id"),
        "strategy_family": normalized.get("strategy_family"),
        "meta": build_strategy_study_dashboard_meta(normalized.get("meta")),
        "summary": build_strategy_study_dashboard_summary(normalized.get("summary")),
    }


def build_strategy_study_catalog_entry(
    *,
    payload: dict[str, Any],
    run_stamp: str,
    run_timestamp: str | None = None,
    manifest_path: str | None = None,
    summary_path: str | None = None,
    strategy_study_json_path: str | None = None,
    strategy_study_markdown_path: str | None = None,
    label: str | None = None,
) -> dict[str, Any] | None:
    normalized = normalize_strategy_study_payload(payload) or dict(payload)
    if not isinstance(normalized, dict):
        return None
    meta = dict(normalized.get("meta") or {})
    compact_meta = build_strategy_study_dashboard_meta(meta)
    summary = build_strategy_study_dashboard_summary(normalized.get("summary"))
    timeframe_truth = dict(meta.get("timeframe_truth") or {})
    coverage = dict(meta.get("coverage_range") or {})
    symbol = str(meta.get("symbol") or normalized.get("symbol") or "-")
    strategy_id = meta.get("strategy_id")
    strategy_family = meta.get("strategy_family") or normalized.get("strategy_family")
    study_mode = str(meta.get("study_mode") or "baseline_parity_mode")
    scope_label = (
        "Research Execution"
        if study_mode == "research_execution_mode"
        else "Live Execution"
        if study_mode == "live_execution_mode"
        else "Legacy Benchmark"
    )
    study_key = str(meta.get("study_id") or f"{run_stamp}:{symbol}:{strategy_id or strategy_family or 'study'}")
    return {
        "study_key": study_key,
        "label": label
        or " / ".join(
            part
            for part in (
                symbol,
                str(strategy_id or strategy_family or "study"),
                study_mode,
                str(meta.get("entry_model") or ""),
            )
            if part
        ),
        "run_stamp": run_stamp,
        "run_timestamp": run_timestamp,
        "symbol": symbol,
        "strategy_id": strategy_id,
        "candidate_id": meta.get("candidate_id"),
        "scope_label": scope_label,
        "strategy_family": strategy_family,
        "contract_version": normalized.get("contract_version"),
        "context_resolution": meta.get("context_resolution"),
        "execution_resolution": meta.get("execution_resolution"),
        "timeframe_truth": timeframe_truth,
        "meta": compact_meta,
        "coverage_start": meta.get("coverage_start") or coverage.get("start_timestamp"),
        "coverage_end": meta.get("coverage_end") or coverage.get("end_timestamp"),
        "study_mode": study_mode,
        "entry_model": meta.get("entry_model"),
        "supported_entry_models": list(meta.get("supported_entry_models") or []),
        "entry_model_supported": bool(meta.get("entry_model_supported", True)),
        "execution_truth_emitter": meta.get("execution_truth_emitter"),
        "intrabar_execution_authoritative": bool(meta.get("intrabar_execution_authoritative")),
        "authoritative_intrabar_available": bool(meta.get("authoritative_intrabar_available")),
        "authoritative_entry_truth_available": bool(meta.get("authoritative_entry_truth_available")),
        "authoritative_exit_truth_available": bool(meta.get("authoritative_exit_truth_available")),
        "authoritative_trade_lifecycle_available": bool(meta.get("authoritative_trade_lifecycle_available")),
        "pnl_truth_basis": meta.get("pnl_truth_basis"),
        "lifecycle_truth_class": meta.get("lifecycle_truth_class"),
        "unsupported_reason": meta.get("unsupported_reason"),
        "active_entry_model": meta.get("active_entry_model") or meta.get("entry_model"),
        "truth_provenance": dict(meta.get("truth_provenance") or {}),
        "entry_model_capabilities": list(meta.get("entry_model_capabilities") or []),
        "available_overlay_flags": dict(meta.get("available_overlay_flags") or {}),
        "closed_trade_count": int(summary.get("closed_trade_count") or 0),
        "artifact_paths": {
            "manifest": manifest_path,
            "summary": summary_path,
            "strategy_study_json": strategy_study_json_path,
            "strategy_study_markdown": strategy_study_markdown_path,
        },
        "summary": summary,
        "study_preview": build_strategy_study_preview(normalized),
    }


def write_strategy_study_json(payload: dict[str, Any], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _sample_dense_series(
    rows: Sequence[dict[str, Any]],
    *,
    max_items: int,
    anchor_ids: set[str] | None = None,
    item_id_key: str = "bar_id",
    timestamp_key: str = "timestamp",
) -> list[dict[str, Any]]:
    items = list(rows or [])
    if max_items <= 0 or len(items) <= max_items:
        return items

    keep_indexes: set[int] = {0, len(items) - 1}
    normalized_anchor_ids = {item for item in (anchor_ids or set()) if item}
    if normalized_anchor_ids:
        for index, row in enumerate(items):
            row_anchor = str(row.get(item_id_key) or "")
            if row_anchor and row_anchor in normalized_anchor_ids:
                keep_indexes.add(index)

    remaining_capacity = max(max_items - len(keep_indexes), 0)
    if remaining_capacity > 0:
        step = max((len(items) - 1) / (remaining_capacity + 1), 1.0)
        for slot in range(1, remaining_capacity + 1):
            keep_indexes.add(min(int(round(step * slot)), len(items) - 1))

    sampled = [items[index] for index in sorted(keep_indexes)]
    if len(sampled) > max_items:
        sampled = sampled[: max_items - 1] + [items[-1]]
    return sorted(sampled, key=lambda row: str(row.get(timestamp_key) or ""))


def render_strategy_study_markdown(payload: dict[str, Any]) -> str:
    normalized_payload = normalize_strategy_study_payload(payload) or dict(payload)
    summary = dict(normalized_payload.get("summary") or {})
    atp_summary = dict(summary.get("atp_summary") or {})
    lines = [
        "# Strategy Study",
        "",
        f"- Symbol: {normalized_payload.get('symbol') or '-'}",
        f"- Timeframe: {normalized_payload.get('timeframe') or '-'}",
        f"- Standalone strategy: {normalized_payload.get('standalone_strategy_id') or '-'}",
        f"- Strategy family: {normalized_payload.get('strategy_family') or '-'}",
        f"- Bars: {summary.get('bar_count') or 0}",
        f"- Total trades: {summary.get('total_trades') or 0}",
        f"- Longs / Shorts: {summary.get('long_trades') or 0} / {summary.get('short_trades') or 0}",
        f"- Winners / Losers: {summary.get('winners') if summary.get('winners') is not None else 'Unavailable'} / {summary.get('losers') if summary.get('losers') is not None else 'Unavailable'}",
        f"- Cumulative realized P/L: {summary.get('cumulative_realized_pnl') or 'Unavailable'}",
        f"- Cumulative total P/L: {summary.get('cumulative_total_pnl') or 'Unavailable'}",
        f"- Max run-up / drawdown: {summary.get('max_run_up') or 'Unavailable'} / {summary.get('max_drawdown') or 'Unavailable'}",
        "",
        "## Common Blockers",
    ]
    blockers = list(summary.get("most_common_blocker_codes") or [])
    if blockers:
        for item in blockers:
            lines.append(f"- {item.get('code')}: {item.get('count')}")
    else:
        lines.append("- No blocker codes were observed.")

    lines.extend(["", "## No-Trade Regions"])
    no_trade_regions = list(summary.get("no_trade_regions") or [])
    if no_trade_regions:
        for region in no_trade_regions:
            lines.append(
                f"- {region.get('start_timestamp')} -> {region.get('end_timestamp')} "
                f"({region.get('bar_count')} bars, session={region.get('session_phase')})"
            )
    else:
        lines.append("- No flat/no-marker regions were detected.")

    lines.extend(["", "## Session Behavior"])
    session_rows = list(summary.get("session_level_behavior") or [])
    if session_rows:
        for row in session_rows:
            lines.append(
                f"- {row.get('session_phase')}: bars={row.get('bar_count')} "
                f"entries={row.get('entry_marked_bars')} exits={row.get('exit_marked_bars')} "
                f"fills={row.get('fill_marked_bars')} blocked={row.get('blocked_bars')} "
                f"net_change={row.get('net_pnl_change') or 'Unavailable'}"
            )
    else:
        lines.append("- No per-session behavior rows are available.")

    if summary.get("pnl_unavailable_reason"):
        lines.extend(["", "## Pricing Note", f"- {summary['pnl_unavailable_reason']}"])
    lines.extend(["", "## ATP Summary"])
    if atp_summary.get("available"):
        bias_state_percent = dict(atp_summary.get("bias_state_percent") or {})
        pullback_state_percent = dict(atp_summary.get("pullback_state_percent") or {})
        lines.extend(
            [
                f"- Bias LONG / SHORT / NEUTRAL: {bias_state_percent.get('LONG_BIAS', 0.0)}% / {bias_state_percent.get('SHORT_BIAS', 0.0)}% / {bias_state_percent.get('NEUTRAL', 0.0)}%",
                f"- Pullback NORMAL / STRETCHED / VIOLENT / NONE: {pullback_state_percent.get('NORMAL_PULLBACK', 0.0)}% / {pullback_state_percent.get('STRETCHED_PULLBACK', 0.0)}% / {pullback_state_percent.get('VIOLENT_PULLBACK_DISQUALIFY', 0.0)}% / {pullback_state_percent.get('NO_PULLBACK', 0.0)}%",
                f"- ATP ready bars: {atp_summary.get('ready_bar_count', 0)}",
                f"- Ready -> timing confirmed: {atp_summary.get('ready_to_timing_confirmed_percent', 0.0)}%",
                f"- Timing confirmed -> executed: {atp_summary.get('timing_confirmed_to_executed_percent', 0.0)}%",
                f"- Ready -> executed: {atp_summary.get('ready_to_executed_percent', 0.0)}%",
            ]
        )
        top_atp_blockers = list(atp_summary.get("top_atp_blocker_codes") or [])
        lines.append("- ATP blockers:")
        if top_atp_blockers:
            lines.extend(f"  - {item.get('code')}: {item.get('count')}" for item in top_atp_blockers)
        else:
            lines.append("  - none")
        no_trade_reasons = list(atp_summary.get("top_no_trade_reasons") or [])
        lines.append("- ATP no-trade reasons:")
        if no_trade_reasons:
            lines.extend(f"  - {item.get('code')}: {item.get('count')}" for item in no_trade_reasons)
        else:
            lines.append("  - none")
    else:
        lines.append(f"- {atp_summary.get('unavailable_reason') or 'ATP state was not available for this run.'}")
    lines.append("")
    return "\n".join(lines)


def write_strategy_study_markdown(payload: dict[str, Any], output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_strategy_study_markdown(payload), encoding="utf-8")
    return path


def _build_v3_bars(
    *,
    rows: Sequence[dict[str, Any]],
    bars: Sequence[Bar] | None,
    default_timeframe: Any,
) -> list[dict[str, Any]]:
    bars_by_id = {bar.bar_id: bar for bar in bars or []}
    converted: list[dict[str, Any]] = []
    for row in rows:
        bar_id = str(row.get("bar_id") or "")
        source_bar = bars_by_id.get(bar_id)
        converted_row = dict(row)
        converted_row.setdefault("timeframe", source_bar.timeframe if source_bar is not None else default_timeframe)
        converted_row.setdefault("volume", source_bar.volume if source_bar is not None else None)
        converted_row.setdefault("session_allowed", source_bar.session_allowed if source_bar is not None else None)
        converted_row.setdefault("source_resolution", "BAR_CONTEXT")
        converted.append(converted_row)
    return converted


def _build_v3_trade_events(
    rows: Sequence[dict[str, Any]],
    *,
    authoritative_execution_events: Sequence[dict[str, Any]] | None = None,
    authoritative_timing_states: Sequence[dict[str, Any]] | None = None,
    authoritative_shadow_trades: Sequence[dict[str, Any]] | None = None,
    execution_slices: Sequence[dict[str, Any]] | None = None,
    entry_model: str | None = None,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    execution_slice_id_by_timestamp = {
        str(slice_row.get("timestamp") or ""): str(slice_row.get("slice_id") or "")
        for slice_row in execution_slices or []
        if slice_row.get("timestamp") and slice_row.get("slice_id")
    }
    linked_bar_id_by_timestamp = {
        str(slice_row.get("timestamp") or ""): str(slice_row.get("linked_bar_id") or "")
        for slice_row in execution_slices or []
        if slice_row.get("timestamp") and slice_row.get("linked_bar_id")
    }
    for row in rows:
        bar_id = str(row.get("bar_id") or "")
        decision_timestamp = _maybe_iso_text(row.get("timestamp") or row.get("end_timestamp"))
        entry_source_family = row.get("entry_source_family")
        for marker in row.get("entry_markers") or []:
            events.append(
                _build_trade_event(
                    event_id=f"{bar_id}:entry_intent:{len(events)}",
                    linked_bar_id=bar_id,
                    event_type="ENTRY_INTENT",
                    side=marker.get("side"),
                    family=entry_source_family or marker.get("reason_code"),
                    reason=marker.get("reason_code"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=_maybe_iso_text(marker.get("timestamp")) or decision_timestamp,
                    source_resolution="BAR_CONTEXT",
                    entry_model=entry_model,
                )
            )
        for marker in row.get("exit_markers") or []:
            events.append(
                _build_trade_event(
                    event_id=f"{bar_id}:exit_intent:{len(events)}",
                    linked_bar_id=bar_id,
                    event_type="EXIT_INTENT",
                    side=marker.get("side"),
                    family=entry_source_family or marker.get("reason_code"),
                    reason=marker.get("reason_code"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=_maybe_iso_text(marker.get("timestamp")) or decision_timestamp,
                    source_resolution="BAR_CONTEXT",
                    entry_model=entry_model,
                )
            )
        for marker in row.get("fill_markers") or []:
            events.append(
                _build_trade_event(
                    event_id=f"{bar_id}:fill:{len(events)}",
                    linked_bar_id=bar_id,
                    event_type="ENTRY_FILL" if marker.get("is_entry") else "EXIT_FILL",
                    side=marker.get("side"),
                    family=entry_source_family,
                    reason=marker.get("intent_type"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=_maybe_iso_text(marker.get("timestamp")) or decision_timestamp,
                    source_resolution="BAR_CONTEXT",
                    entry_model=entry_model,
                )
            )
        if row.get("entry_eligible"):
            events.append(
                _build_trade_event(
                    event_id=f"{bar_id}:legacy_ready",
                    linked_bar_id=bar_id,
                    event_type="LEGACY_ENTRY_ELIGIBLE",
                    side=row.get("latest_signal_side"),
                    family=entry_source_family,
                    reason=row.get("legacy_latest_signal_state"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=decision_timestamp,
                    source_resolution="BAR_CONTEXT",
                    entry_model=entry_model,
                )
            )
        if row.get("entry_blocked"):
            events.append(
                _build_trade_event(
                    event_id=f"{bar_id}:legacy_blocked",
                    linked_bar_id=bar_id,
                    event_type="LEGACY_ENTRY_BLOCKED",
                    side=row.get("latest_signal_side"),
                    family=entry_source_family,
                    reason=row.get("legacy_blocker_code"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=decision_timestamp,
                    source_resolution="BAR_CONTEXT",
                    entry_model=entry_model,
                )
            )
        if row.get("atp_entry_ready"):
            events.append(
                _build_trade_event(
                    event_id=f"{bar_id}:atp_ready",
                    linked_bar_id=bar_id,
                    event_type="ATP_ENTRY_READY",
                    side=row.get("latest_signal_side"),
                    family=row.get("atp_family_name") or entry_source_family,
                    reason=row.get("atp_entry_state"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=decision_timestamp,
                    source_resolution="BAR_CONTEXT",
                    entry_model=entry_model,
                )
            )
        if row.get("atp_entry_blocked"):
            events.append(
                _build_trade_event(
                    event_id=f"{bar_id}:atp_blocked",
                    linked_bar_id=bar_id,
                    event_type="ATP_ENTRY_BLOCKED",
                    side=row.get("latest_signal_side"),
                    family=row.get("atp_family_name") or entry_source_family,
                    reason=row.get("atp_entry_blocker_code"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=decision_timestamp,
                    source_resolution="BAR_CONTEXT",
                    entry_model=entry_model,
                )
            )
        if row.get("atp_timing_state"):
            events.append(
                _build_trade_event(
                    event_id=f"{bar_id}:atp_timing",
                    linked_bar_id=bar_id,
                    linked_subbar_id=row.get("atp_timing_bar_timestamp"),
                    event_type="ATP_TIMING_STATE",
                    side=row.get("latest_signal_side"),
                    family=row.get("atp_family_name") or entry_source_family,
                    reason=row.get("atp_timing_blocker_code") or row.get("atp_timing_state"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=_maybe_iso_text(row.get("atp_timing_bar_timestamp")) or decision_timestamp,
                    source_resolution="INTRABAR" if row.get("atp_timing_bar_timestamp") else "BAR_CONTEXT",
                    entry_model=entry_model,
                )
            )
    for event in authoritative_execution_events or []:
        events.append(dict(event))
    for timing_state in authoritative_timing_states or []:
        linked_bar_id = str(
            timing_state.get("bar_id")
            or linked_bar_id_by_timestamp.get(str(timing_state.get("timing_bar_ts") or ""))
            or _linked_bar_id_for_timestamp(rows=rows, timestamp=_maybe_iso_text(timing_state.get("decision_ts")))
            or ""
        )
        if not linked_bar_id:
            continue
        decision_timestamp = _maybe_iso_text(timing_state.get("decision_ts"))
        event_timestamp = _maybe_iso_text(timing_state.get("timing_bar_ts")) or _maybe_iso_text(timing_state.get("entry_ts")) or decision_timestamp
        linked_subbar_id = (
            execution_slice_id_by_timestamp.get(str(timing_state.get("timing_bar_ts") or ""))
            or _maybe_iso_text(timing_state.get("timing_bar_ts"))
        )
        vwap_at_event = _feature_snapshot_value(timing_state.get("feature_snapshot"), "timing_checks", "bar_vwap")
        if timing_state.get("setup_armed"):
            events.append(
                _build_trade_event(
                    event_id=f"{linked_bar_id}:intrabar_armed:{decision_timestamp}",
                    linked_bar_id=linked_bar_id,
                    linked_subbar_id=linked_subbar_id,
                    event_type="ATP_ENTRY_ARMED",
                    execution_event_type="ENTRY_ARMED",
                    side=timing_state.get("side") or "LONG",
                    family=timing_state.get("family_name"),
                    reason=timing_state.get("context_entry_state"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=event_timestamp,
                    source_resolution="INTRABAR",
                    entry_model=entry_model,
                    event_price=timing_state.get("entry_price"),
                    vwap_at_event=vwap_at_event,
                    acceptance_state=timing_state.get("vwap_price_quality_state"),
                    confirmation_flag=bool(timing_state.get("timing_confirmed")),
                    invalidation_reason=timing_state.get("primary_blocker") if timing_state.get("invalidated_before_entry") else None,
                    truth_authority="AUTHORITATIVE_INTRABAR",
                )
            )
        if timing_state.get("timing_confirmed"):
            events.append(
                _build_trade_event(
                    event_id=f"{linked_bar_id}:intrabar_confirmed:{event_timestamp}",
                    linked_bar_id=linked_bar_id,
                    linked_subbar_id=linked_subbar_id,
                    event_type="ATP_ENTRY_TIMING_CONFIRMED",
                    execution_event_type="ENTRY_CONFIRMED",
                    side=timing_state.get("side") or "LONG",
                    family=timing_state.get("family_name"),
                    reason=timing_state.get("timing_state"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=event_timestamp,
                    source_resolution="INTRABAR",
                    entry_model=entry_model,
                    event_price=timing_state.get("entry_price"),
                    vwap_at_event=vwap_at_event,
                    acceptance_state=timing_state.get("vwap_price_quality_state"),
                    confirmation_flag=True,
                    truth_authority="AUTHORITATIVE_INTRABAR",
                )
            )
        if timing_state.get("invalidated_before_entry"):
            events.append(
                _build_trade_event(
                    event_id=f"{linked_bar_id}:intrabar_invalidated:{event_timestamp}",
                    linked_bar_id=linked_bar_id,
                    linked_subbar_id=linked_subbar_id,
                    event_type="ATP_ENTRY_INVALIDATED",
                    execution_event_type="ENTRY_INVALIDATED",
                    side=timing_state.get("side") or "LONG",
                    family=timing_state.get("family_name"),
                    reason=timing_state.get("primary_blocker") or timing_state.get("timing_state"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=event_timestamp,
                    source_resolution="INTRABAR",
                    entry_model=entry_model,
                    event_price=timing_state.get("entry_price"),
                    vwap_at_event=vwap_at_event,
                    acceptance_state=timing_state.get("vwap_price_quality_state"),
                    confirmation_flag=False,
                    invalidation_reason=timing_state.get("primary_blocker"),
                    truth_authority="AUTHORITATIVE_INTRABAR",
                )
            )
        if timing_state.get("timing_state") == "ATP_TIMING_CHASE_RISK":
            events.append(
                _build_trade_event(
                    event_id=f"{linked_bar_id}:intrabar_chase:{event_timestamp}",
                    linked_bar_id=linked_bar_id,
                    linked_subbar_id=linked_subbar_id,
                    event_type="ATP_ENTRY_CHASE_RISK",
                    execution_event_type="ENTRY_INVALIDATED",
                    side=timing_state.get("side") or "LONG",
                    family=timing_state.get("family_name"),
                    reason=timing_state.get("primary_blocker") or timing_state.get("timing_state"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=event_timestamp,
                    source_resolution="INTRABAR",
                    entry_model=entry_model,
                    event_price=timing_state.get("entry_price"),
                    vwap_at_event=vwap_at_event,
                    acceptance_state=timing_state.get("vwap_price_quality_state"),
                    confirmation_flag=False,
                    invalidation_reason=timing_state.get("primary_blocker"),
                    truth_authority="AUTHORITATIVE_INTRABAR",
                )
            )
    for trade in authoritative_shadow_trades or []:
        entry_timestamp = _maybe_iso_text(trade.get("entry_ts"))
        exit_timestamp = _maybe_iso_text(trade.get("exit_ts"))
        decision_timestamp = _maybe_iso_text(trade.get("decision_ts"))
        entry_bar_id = _linked_bar_id_for_timestamp(rows=rows, timestamp=entry_timestamp) or linked_bar_id_by_timestamp.get(entry_timestamp or "")
        exit_bar_id = _linked_bar_id_for_timestamp(rows=rows, timestamp=exit_timestamp) or linked_bar_id_by_timestamp.get(exit_timestamp or "")
        entry_slice_id = execution_slice_id_by_timestamp.get(entry_timestamp or "") or entry_timestamp
        exit_slice_id = execution_slice_id_by_timestamp.get(exit_timestamp or "") or exit_timestamp
        if entry_bar_id and entry_timestamp:
            events.append(
                _build_trade_event(
                    event_id=f"{entry_bar_id}:intrabar_entry_executed:{entry_timestamp}",
                    linked_bar_id=entry_bar_id,
                    linked_subbar_id=entry_slice_id,
                    event_type="ATP_ENTRY_EXECUTED",
                    execution_event_type="ENTRY_EXECUTED",
                    side=trade.get("side"),
                    family=trade.get("family"),
                    reason=trade.get("decision_id"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=entry_timestamp,
                    source_resolution="INTRABAR",
                    entry_model=entry_model,
                    event_price=trade.get("entry_price"),
                    truth_authority="AUTHORITATIVE_INTRABAR",
                )
            )
        if exit_bar_id and exit_timestamp:
            events.append(
                _build_trade_event(
                    event_id=f"{exit_bar_id}:intrabar_exit_executed:{exit_timestamp}",
                    linked_bar_id=exit_bar_id,
                    linked_subbar_id=exit_slice_id,
                    event_type="ATP_EXIT_EXECUTED",
                    execution_event_type="EXIT_TRIGGERED",
                    side=trade.get("side"),
                    family=trade.get("family"),
                    reason=trade.get("exit_reason"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=exit_timestamp,
                    source_resolution="INTRABAR",
                    entry_model=entry_model,
                    event_price=trade.get("exit_price"),
                    truth_authority="AUTHORITATIVE_INTRABAR",
                )
            )
    return events


def _build_trade_event(
    *,
    event_id: str,
    linked_bar_id: str,
    event_type: str,
    side: Any,
    family: Any,
    reason: Any,
    decision_context_timestamp: str | None,
    event_timestamp: str | None,
    source_resolution: str,
    linked_subbar_id: Any = None,
    execution_event_type: str | None = None,
    entry_model: str | None = None,
    event_price: Any = None,
    vwap_at_event: Any = None,
    acceptance_state: Any = None,
    confirmation_flag: bool | None = None,
    invalidation_reason: Any = None,
    truth_authority: str | None = None,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "linked_bar_id": linked_bar_id or None,
        "linked_subbar_id": linked_subbar_id,
        "event_type": event_type,
        "execution_event_type": execution_event_type,
        "side": side,
        "family": family,
        "reason": reason,
        "source_resolution": source_resolution,
        "decision_context_timestamp": decision_context_timestamp,
        "event_timestamp": event_timestamp,
        "entry_model": entry_model,
        "event_price": event_price,
        "vwap_at_event": vwap_at_event,
        "acceptance_state": acceptance_state,
        "confirmation_flag": confirmation_flag,
        "invalidation_reason": invalidation_reason,
        "truth_authority": truth_authority,
    }


def _build_v3_pnl_points(
    rows: Sequence[dict[str, Any]],
    *,
    authoritative_shadow_trades: Sequence[dict[str, Any]] | None = None,
    point_value: Any = None,
    pnl_truth_basis: str = "BASELINE_FILL_TRUTH",
) -> list[dict[str, Any]]:
    if authoritative_shadow_trades:
        return _build_v3_authoritative_pnl_points(
            rows=rows,
            authoritative_shadow_trades=authoritative_shadow_trades,
            point_value=point_value,
            pnl_truth_basis=pnl_truth_basis,
        )
    return [
        {
            "point_id": f"{row.get('bar_id')}:pnl",
            "bar_id": row.get("bar_id"),
            "timestamp": row.get("timestamp"),
            "realized_pnl": row.get("realized_pnl"),
            "unrealized_pnl": row.get("unrealized_pnl"),
            "cumulative_realized": row.get("cumulative_realized_pnl"),
            "cumulative_total": row.get("cumulative_total_pnl"),
            "position_side": row.get("position_side"),
            "position_qty": row.get("position_qty"),
            "pnl_truth_basis": pnl_truth_basis,
        }
        for row in rows
    ]


def _build_v3_authoritative_pnl_points(
    *,
    rows: Sequence[dict[str, Any]],
    authoritative_shadow_trades: Sequence[dict[str, Any]],
    point_value: Any,
    pnl_truth_basis: str,
) -> list[dict[str, Any]]:
    point_value_decimal = _decimal_or_none(point_value)
    sorted_rows = sorted(rows, key=lambda row: str(row.get("timestamp") or row.get("end_timestamp") or ""))
    sorted_trades = sorted(authoritative_shadow_trades, key=lambda row: str(row.get("entry_ts") or ""))
    points: list[dict[str, Any]] = []
    for row in sorted_rows:
        row_timestamp = _parse_iso_timestamp(row.get("timestamp") or row.get("end_timestamp"))
        if row_timestamp is None:
            continue
        realized_total = Decimal("0")
        unrealized_total = Decimal("0")
        active_trade: dict[str, Any] | None = None
        for trade in sorted_trades:
            trade_entry_ts = _parse_iso_timestamp(trade.get("entry_ts"))
            trade_exit_ts = _parse_iso_timestamp(trade.get("exit_ts"))
            if trade_entry_ts is None or trade_exit_ts is None:
                continue
            trade_pnl = _trade_pnl_decimal(trade=trade, point_value=point_value_decimal)
            if trade_exit_ts <= row_timestamp:
                realized_total += trade_pnl
                continue
            if trade_entry_ts <= row_timestamp < trade_exit_ts:
                active_trade = trade
                close_price = _decimal_or_none(row.get("close"))
                entry_price = _decimal_or_none(trade.get("entry_price"))
                if close_price is not None and entry_price is not None:
                    direction = -1 if str(trade.get("side") or "").upper() == "SHORT" else 1
                    unrealized_points = (close_price - entry_price) * Decimal(direction)
                    unrealized_total = (
                        unrealized_points * point_value_decimal
                        if point_value_decimal is not None
                        else unrealized_points
                    )
                break
        cumulative_total = realized_total + unrealized_total
        points.append(
            {
                "point_id": f"{row.get('bar_id')}:pnl",
                "bar_id": row.get("bar_id"),
                "timestamp": row.get("timestamp"),
                "realized_pnl": _decimal_to_str(realized_total),
                "unrealized_pnl": _decimal_to_str(unrealized_total),
                "cumulative_realized": _decimal_to_str(realized_total),
                "cumulative_total": _decimal_to_str(cumulative_total),
                "position_side": active_trade.get("side") if active_trade is not None else "FLAT",
                "position_qty": 1 if active_trade is not None else 0,
                "pnl_truth_basis": pnl_truth_basis,
            }
        )
    return points


def _build_v3_execution_slices(
    *,
    source_bars: Sequence[Bar] | None,
    context_bars: Sequence[Bar] | None,
    context_resolution: str,
) -> list[dict[str, Any]]:
    ordered_source_bars = list(sorted(source_bars or [], key=lambda row: row.end_ts))
    ordered_context_bars = list(sorted(context_bars or [], key=lambda row: row.end_ts))
    if not ordered_source_bars or not ordered_context_bars:
        return []
    if all(str(bar.timeframe) == str(context_resolution) for bar in ordered_source_bars):
        return []

    slices: list[dict[str, Any]] = []
    context_index = 0
    slices_per_bar: dict[str, int] = defaultdict(int)
    for source_bar in ordered_source_bars:
        while context_index < len(ordered_context_bars) and source_bar.end_ts > ordered_context_bars[context_index].end_ts:
            context_index += 1
        if context_index >= len(ordered_context_bars):
            break
        context_bar = ordered_context_bars[context_index]
        if source_bar.start_ts < context_bar.start_ts or source_bar.end_ts > context_bar.end_ts:
            continue
        slices_per_bar[context_bar.bar_id] += 1
        slices.append(
            {
                "slice_id": f"{context_bar.bar_id}:slice:{slices_per_bar[context_bar.bar_id]}",
                "linked_bar_id": context_bar.bar_id,
                "timestamp": source_bar.end_ts.isoformat(),
                "start_timestamp": source_bar.start_ts.isoformat(),
                "end_timestamp": source_bar.end_ts.isoformat(),
                "timeframe": source_bar.timeframe,
                "open": str(source_bar.open),
                "high": str(source_bar.high),
                "low": str(source_bar.low),
                "close": str(source_bar.close),
                "volume": source_bar.volume,
                "session_phase": label_session_phase(source_bar.end_ts),
                "source_resolution": "INTRABAR",
                "session_vwap": None,
                "acceptance_state": None,
                "vwap_entry_quality_state": None,
            }
        )
    return slices


def _resolve_study_entry_model(
    *,
    settings: StrategySettings,
    bars: Sequence[Bar],
    source_bars: Sequence[Bar],
) -> str:
    if settings.environment_mode.value == "baseline_parity_mode":
        return "BASELINE_NEXT_BAR_OPEN"
    execution_detail_available = (
        settings.execution_timeframe_role.value == "execution_detail_only"
        and settings.resolved_execution_timeframe != settings.resolved_structural_signal_timeframe
        and any(str(bar.timeframe or "").lower() == "1m" for bar in source_bars)
    )
    if execution_detail_available and any(str(bar.timeframe or "") != str(settings.resolved_execution_timeframe) for bar in bars):
        return "CURRENT_CANDLE_VWAP"
    return "BAR_CONTEXT_DEFAULT"


def _infer_entry_model_from_payload(
    *,
    study_mode: str,
    execution_slices: Sequence[dict[str, Any]],
    context_resolution: str | None,
    execution_resolution: str | None,
) -> str:
    if study_mode == "baseline_parity_mode":
        return "BASELINE_NEXT_BAR_OPEN"
    if execution_slices and execution_resolution and context_resolution and execution_resolution != context_resolution:
        return "CURRENT_CANDLE_VWAP"
    return "BAR_CONTEXT_DEFAULT"

def _infer_execution_resolution(*, source_bars: Sequence[Bar] | None, context_resolution: str) -> str | None:
    for bar in source_bars or []:
        timeframe = str(bar.timeframe or "")
        if timeframe and timeframe != context_resolution:
            return timeframe
    return None


def _build_v3_overlay_flags(
    *,
    bars: Sequence[dict[str, Any]],
    pnl_points: Sequence[dict[str, Any]],
    execution_slices: Sequence[dict[str, Any]],
) -> dict[str, bool]:
    return {
        "candles": any(bar.get("open") is not None and bar.get("close") is not None for bar in bars),
        "session_vwap": any(bar.get("session_vwap") is not None for bar in bars),
        "legacy_blockers": any(bar.get("legacy_blocker_code") for bar in bars),
        "atp_context": any(
            bar.get("current_bias_state") or bar.get("current_pullback_state") or bar.get("continuation_state")
            for bar in bars
        ),
        "atp_timing": any(bar.get("atp_timing_state") for bar in bars),
        "position_phase": any(bar.get("position_phase") for bar in bars),
        "rolling_pnl": any(point.get("cumulative_realized") is not None or point.get("cumulative_total") is not None for point in pnl_points),
        "execution_detail": bool(execution_slices),
    }


def _classify_lifecycle_truth_class(
    *,
    authoritative_entry_truth_available: bool,
    authoritative_exit_truth_available: bool,
    authoritative_trade_lifecycle_available: bool,
    pnl_truth_basis: str,
    intrabar_execution_authoritative: bool,
    entry_model_supported: bool,
) -> str:
    if not entry_model_supported or pnl_truth_basis == UNSUPPORTED_ENTRY_MODEL:
        return UNSUPPORTED_ENTRY_MODEL
    if (
        authoritative_entry_truth_available
        and authoritative_exit_truth_available
        and authoritative_trade_lifecycle_available
    ):
        return FULL_AUTHORITATIVE_LIFECYCLE
    if pnl_truth_basis == HYBRID_ENTRY_BASELINE_EXIT_TRUTH:
        return HYBRID_AUTHORITATIVE_ENTRY_BASELINE_EXIT
    if intrabar_execution_authoritative or authoritative_entry_truth_available:
        return AUTHORITATIVE_INTRABAR_ENTRY_ONLY
    return BASELINE_PARITY_ONLY


def _build_truth_provenance(
    *,
    run_metadata: dict[str, Any] | None,
    study_mode: str,
) -> dict[str, Any]:
    metadata = dict(run_metadata or {})
    runtime_context = str(metadata.get("mode") or "REPLAY")
    if runtime_context == "REPLAY":
        run_lane = "BENCHMARK_REPLAY"
    elif runtime_context == "PAPER":
        run_lane = "PAPER_RUNTIME"
    else:
        run_lane = runtime_context
    return {
        "runtime_context": runtime_context,
        "run_lane": run_lane,
        "artifact_context": metadata.get("artifact_context") or "STRATEGY_STUDY",
        "persistence_origin": metadata.get("persistence_origin") or "PERSISTED_RUNTIME_TRUTH",
        "study_mode": study_mode,
        "artifact_rebuilt": bool(metadata.get("artifact_rebuilt")),
    }


def _build_v3_summary(
    *,
    summary: dict[str, Any],
    pnl_points: Sequence[dict[str, Any]],
    authoritative_timing_states: Sequence[dict[str, Any]],
    authoritative_shadow_trades: Sequence[dict[str, Any]],
    intrabar_execution_authoritative: bool,
    pnl_truth_basis: str,
) -> dict[str, Any]:
    updated = dict(summary)
    updated["pnl_truth_basis"] = pnl_truth_basis
    if not intrabar_execution_authoritative:
        return updated
    atp_summary = dict(updated.get("atp_summary") or {})
    ready_rows = [row for row in authoritative_timing_states if str(row.get("context_entry_state") or "") == "ENTRY_ELIGIBLE"]
    timing_confirmed_rows = [row for row in ready_rows if row.get("timing_confirmed") is True]
    executed_rows = [row for row in ready_rows if row.get("entry_executed") is True]
    if authoritative_timing_states:
        atp_summary.update(
            {
                "available": True,
                "timing_available": True,
                "ready_bar_count": len(ready_rows),
                "ready_to_timing_confirmed_percent": _percent(len(timing_confirmed_rows), len(ready_rows)),
                "timing_confirmed_to_executed_percent": _percent(len(executed_rows), len(timing_confirmed_rows)),
                "ready_to_executed_percent": _percent(len(executed_rows), len(ready_rows)),
            }
        )
        updated["atp_summary"] = atp_summary
    if authoritative_shadow_trades:
        trade_pnls = [_trade_pnl_decimal(trade=trade, point_value=None) for trade in authoritative_shadow_trades]
        updated["total_trades"] = len(authoritative_shadow_trades)
        updated["long_trades"] = sum(1 for trade in authoritative_shadow_trades if str(trade.get("side") or "").upper() == "LONG")
        updated["short_trades"] = sum(1 for trade in authoritative_shadow_trades if str(trade.get("side") or "").upper() == "SHORT")
        updated["winners"] = sum(1 for value in trade_pnls if value > 0)
        updated["losers"] = sum(1 for value in trade_pnls if value < 0)
        updated["closed_trade_breakdown"] = _study_closed_trade_breakdown(authoritative_shadow_trades, point_value=None)
        updated["trade_family_breakdown"] = _study_trade_group_breakdown(
            authoritative_shadow_trades,
            label_kind="family",
            point_value=None,
        )
        updated["session_trade_breakdown"] = _study_trade_group_breakdown(
            authoritative_shadow_trades,
            label_kind="session",
            point_value=None,
        )
        updated["latest_trade_summary"] = _study_latest_trade_summary(authoritative_shadow_trades, point_value=None)
        updated["profit_factor"] = _study_profit_factor(authoritative_shadow_trades, point_value=None)
        if pnl_points:
            updated["cumulative_realized_pnl"] = pnl_points[-1].get("cumulative_realized")
            updated["cumulative_total_pnl"] = pnl_points[-1].get("cumulative_total")
            cumulative_values = [
                _decimal_or_none(point.get("cumulative_total"))
                for point in pnl_points
                if point.get("cumulative_total") is not None
            ]
            cumulative_values = [value for value in cumulative_values if value is not None]
            updated["max_run_up"] = _decimal_to_str(max(cumulative_values)) if cumulative_values else None
            updated["max_drawdown"] = _decimal_to_str(min(cumulative_values)) if cumulative_values else None
        updated["pnl_supportable"] = True
        updated["pnl_unavailable_reason"] = None
    return updated


def _study_id(
    *,
    run_id: Any,
    symbol: str,
    strategy_id: Any,
    candidate_id: Any,
    context_resolution: Any,
    execution_resolution: Any,
    entry_model: Any = None,
) -> str:
    parts = [
        str(run_id or "run"),
        str(symbol or "symbol"),
        str(strategy_id or "strategy"),
        str(candidate_id or "candidate"),
        str(context_resolution or "context"),
        str(execution_resolution or "noexec"),
        str(entry_model or "entry"),
    ]
    return ":".join(part.replace(" ", "_") for part in parts)


def _maybe_iso_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _parse_iso_timestamp(value: Any) -> datetime | None:
    text = _maybe_iso_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _trade_pnl_decimal(*, trade: dict[str, Any], point_value: Decimal | None) -> Decimal:
    if point_value is not None and trade.get("pnl_cash") is not None:
        return Decimal(str(trade.get("pnl_cash")))
    if trade.get("pnl_points") is not None:
        return Decimal(str(trade.get("pnl_points")))
    return Decimal("0")


def _linked_bar_id_for_timestamp(*, rows: Sequence[dict[str, Any]], timestamp: str | None) -> str | None:
    event_timestamp = _parse_iso_timestamp(timestamp)
    if event_timestamp is None:
        return None
    for row in rows:
        row_start = _parse_iso_timestamp(row.get("start_timestamp"))
        row_end = _parse_iso_timestamp(row.get("end_timestamp") or row.get("timestamp"))
        if row_start is None or row_end is None:
            continue
        if row_start < event_timestamp <= row_end:
            return str(row.get("bar_id") or "") or None
    return None


def _feature_snapshot_value(snapshot: Any, *path: str) -> Any:
    current = snapshot
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _load_feature_payloads(repositories: RepositorySet, bar_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    if not bar_ids:
        return {}
    with repositories.engine.begin() as connection:
        rows = connection.execute(
            select(features_table).where(features_table.c.bar_id.in_(list(bar_ids)))
        ).mappings().all()
    payloads: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = dict(json.loads(str(row["payload_json"])))
        payloads[str(row["bar_id"])] = payload
    return payloads


def _load_signal_payloads(repositories: RepositorySet, bar_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    if not bar_ids:
        return {}
    with repositories.engine.begin() as connection:
        rows = connection.execute(
            select(signals_table).where(signals_table.c.bar_id.in_(list(bar_ids)))
        ).mappings().all()
    return {
        str(row["bar_id"]): dict(json.loads(str(row["payload_json"])))
        for row in rows
    }


def _load_state_snapshots(repositories: RepositorySet, standalone_strategy_id: str | None) -> list[dict[str, Any]]:
    with repositories.engine.begin() as connection:
        statement = (
            select(
                strategy_state_snapshots_table.c.snapshot_id,
                strategy_state_snapshots_table.c.updated_at,
                strategy_state_snapshots_table.c.transition_label,
                strategy_state_snapshots_table.c.payload_json,
            )
            .order_by(
                asc(strategy_state_snapshots_table.c.updated_at),
                asc(strategy_state_snapshots_table.c.snapshot_id),
            )
        )
        if standalone_strategy_id:
            statement = statement.where(
                (strategy_state_snapshots_table.c.standalone_strategy_id == standalone_strategy_id)
                | (strategy_state_snapshots_table.c.standalone_strategy_id.is_(None))
            )
        rows = connection.execute(statement).mappings().all()
    return [
        {
            "updated_at": datetime.fromisoformat(str(row["updated_at"])),
            "transition_label": row["transition_label"],
            "state": decode_strategy_state(str(row["payload_json"])),
        }
        for row in rows
    ]


def _group_intents(order_rows: Sequence[dict[str, Any]]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    entry_intents_by_bar_id: dict[str, list[dict[str, Any]]] = {}
    exit_intents_by_bar_id: dict[str, list[dict[str, Any]]] = {}
    for row in order_rows:
        intent_type = OrderIntentType(str(row["intent_type"]))
        target = (
            entry_intents_by_bar_id
            if intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN)
            else exit_intents_by_bar_id
        )
        target.setdefault(str(row["bar_id"]), []).append(dict(row))
    return entry_intents_by_bar_id, exit_intents_by_bar_id


def _group_fills_by_bar(bars: Sequence[Bar], fill_rows: Sequence[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_start = {bar.start_ts.isoformat(): bar.bar_id for bar in bars}
    by_end = {bar.end_ts.isoformat(): bar.bar_id for bar in bars}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in fill_rows:
        timestamp = str(row.get("fill_timestamp") or "")
        bar_id = by_start.get(timestamp) or by_end.get(timestamp)
        if bar_id is None:
            continue
        grouped.setdefault(bar_id, []).append(dict(row))
    return grouped


def _pair_closed_trades(
    *,
    order_rows: Sequence[dict[str, Any]],
    fill_rows: Sequence[dict[str, Any]],
    point_value: Decimal | None,
) -> list[dict[str, Any]]:
    fills_by_intent = {str(row["order_intent_id"]): decode_fill(dict(row)) for row in fill_rows}
    joined: list[tuple[dict[str, Any], Any]] = []
    for row in order_rows:
        fill = fills_by_intent.get(str(row["order_intent_id"]))
        if fill is None or fill.fill_price is None:
            continue
        joined.append((dict(row), fill))
    joined.sort(key=lambda pair: pair[1].fill_timestamp)

    closed_trades: list[dict[str, Any]] = []
    open_trade: dict[str, Any] | None = None
    for order_row, fill in joined:
        intent = decode_order_intent(order_row)
        if intent.intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN):
            open_trade = {
                "direction": "LONG" if intent.intent_type is OrderIntentType.BUY_TO_OPEN else "SHORT",
                "entry_bar_id": intent.bar_id,
                "entry_timestamp": fill.fill_timestamp,
                "entry_price": fill.fill_price,
                "qty": intent.quantity,
                "source": intent.reason_code,
            }
            continue
        if open_trade is None:
            continue
        net_pnl = None
        if point_value is not None:
            price_diff = (
                fill.fill_price - open_trade["entry_price"]
                if open_trade["direction"] == "LONG"
                else open_trade["entry_price"] - fill.fill_price
            )
            net_pnl = price_diff * Decimal(open_trade["qty"]) * point_value
        closed_trades.append(
            {
                "direction": open_trade["direction"],
                "entry_bar_id": open_trade["entry_bar_id"],
                "exit_bar_id": intent.bar_id,
                "entry_timestamp": open_trade["entry_timestamp"],
                "exit_timestamp": fill.fill_timestamp,
                "entry_price": open_trade["entry_price"],
                "exit_price": fill.fill_price,
                "qty": open_trade["qty"],
                "source": open_trade["source"],
                "exit_reason": intent.reason_code,
                "net_pnl": net_pnl,
            }
        )
        open_trade = None
    return closed_trades


def _group_closed_trades_by_bar(bars: Sequence[Bar], trades: Sequence[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_start = {bar.start_ts.isoformat(): bar.bar_id for bar in bars}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        bar_id = by_start.get(str(trade.get("exit_timestamp").isoformat() if trade.get("exit_timestamp") else ""))
        if bar_id is None:
            continue
        grouped.setdefault(bar_id, []).append(dict(trade))
    return grouped


def _derive_entry_eligibility(
    *,
    bar: Bar,
    bar_index: int,
    settings: StrategySettings,
    state: StrategyState,
    signal: dict[str, Any],
    entry_intents: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    candidate_side, candidate_source = _candidate_signal(signal)
    final_side = _final_signal_side(signal)
    entry_eligible = bool(candidate_source and entry_intents)
    entry_blocked = False
    blocker_code: str | None = None
    signal_state = "NO_SIGNAL"
    if candidate_source is None:
        return {
            "candidate_side": None,
            "candidate_source": None,
            "entry_eligible": False,
            "entry_blocked": False,
            "blocker_code": None,
            "signal_state": signal_state,
            "transition_label": None,
        }

    if entry_intents:
        signal_state = f"{candidate_side}_INTENT_CREATED"
        return {
            "candidate_side": candidate_side,
            "candidate_source": candidate_source,
            "entry_eligible": True,
            "entry_blocked": False,
            "blocker_code": None,
            "signal_state": signal_state,
            "transition_label": "intent_created",
        }

    runtime_blocker = _runtime_entry_control_blocker(
        bar=bar,
        settings=settings,
        side=candidate_side,
        source=candidate_source,
    )
    if runtime_blocker is not None:
        entry_blocked = True
        blocker_code = runtime_blocker
        signal_state = f"{candidate_side}_BLOCKED"
    elif final_side is not None:
        gating_blocker = _order_creation_blocker(
            bar_index=bar_index,
            settings=settings,
            state=state,
        )
        if gating_blocker is not None:
            entry_blocked = True
            blocker_code = gating_blocker
            signal_state = f"{candidate_side}_BLOCKED"
        else:
            entry_eligible = True
            signal_state = f"{candidate_side}_ELIGIBLE"
    elif _anti_churn_rejected(signal, candidate_side):
        signal_state = f"{candidate_side}_ANTI_CHURN"
    else:
        signal_state = f"{candidate_side}_RAW_ONLY"
    return {
        "candidate_side": candidate_side,
        "candidate_source": candidate_source,
        "entry_eligible": entry_eligible,
        "entry_blocked": entry_blocked,
        "blocker_code": blocker_code,
        "signal_state": signal_state,
        "transition_label": None,
    }


def _candidate_signal(signal: dict[str, Any]) -> tuple[str | None, str | None]:
    if bool(signal.get("long_entry_raw")):
        return "LONG", _candidate_long_source(signal)
    if bool(signal.get("short_entry_raw")):
        return "SHORT", _candidate_short_source(signal)
    return None, None


def _candidate_long_source(signal: dict[str, Any]) -> str | None:
    if bool(signal.get("asia_vwap_long_signal")):
        return "asiaVWAPLongSignal"
    if bool(signal.get("first_bull_snap_turn")):
        return "firstBullSnapTurn"
    if bool(signal.get("midday_pause_resume_long_turn_candidate")):
        return "usMiddayPauseResumeLongTurn"
    if bool(signal.get("us_late_breakout_retest_hold_long_turn_candidate")):
        return "usLateBreakoutRetestHoldTurn"
    if bool(signal.get("us_late_failed_move_reversal_long_turn_candidate")):
        return "usLateFailedMoveReversalLongTurn"
    if bool(signal.get("us_late_pause_resume_long_turn_candidate")):
        return "usLatePauseResumeLongTurn"
    if bool(signal.get("asia_early_normal_breakout_retest_hold_long_turn_candidate")):
        return "asiaEarlyNormalBreakoutRetestHoldTurn"
    if bool(signal.get("asia_early_breakout_retest_hold_long_turn_candidate")):
        return "asiaEarlyBreakoutRetestHoldTurn"
    if bool(signal.get("asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate")):
        return "asiaLateCompressedFlatPullbackPauseResumeLongTurn"
    if bool(signal.get("asia_late_flat_pullback_pause_resume_long_turn_candidate")):
        return "asiaLateFlatPullbackPauseResumeLongTurn"
    if bool(signal.get("asia_late_pause_resume_long_turn_candidate")):
        return "asiaLatePauseResumeLongTurn"
    return None


def _candidate_short_source(signal: dict[str, Any]) -> str | None:
    if bool(signal.get("first_bear_snap_turn")):
        return "firstBearSnapTurn"
    if bool(signal.get("derivative_bear_turn_candidate")):
        return "usDerivativeBearTurn"
    if bool(signal.get("derivative_bear_additive_turn_candidate")):
        return "usDerivativeBearAdditiveTurn"
    if bool(signal.get("midday_compressed_rebound_failed_move_reversal_short_turn_candidate")):
        return "usMiddayCompressedReboundFailedMoveReversalShortTurn"
    if bool(signal.get("midday_compressed_failed_move_reversal_short_turn_candidate")):
        return "usMiddayCompressedFailedMoveReversalShortTurn"
    if bool(signal.get("midday_expanded_pause_resume_short_turn_candidate")):
        return "usMiddayExpandedPauseResumeShortTurn"
    if bool(signal.get("midday_compressed_pause_resume_short_turn_candidate")):
        return "usMiddayCompressedPauseResumeShortTurn"
    if bool(signal.get("midday_pause_resume_short_turn_candidate")):
        return "usMiddayPauseResumeShortTurn"
    if bool(signal.get("london_late_pause_resume_short_turn_candidate")):
        return "londonLatePauseResumeShortTurn"
    if bool(signal.get("asia_early_expanded_breakout_retest_hold_short_turn_candidate")):
        return "asiaEarlyExpandedBreakoutRetestHoldShortTurn"
    if bool(signal.get("asia_early_compressed_pause_resume_short_turn_candidate")):
        return "asiaEarlyCompressedPauseResumeShortTurn"
    if bool(signal.get("asia_early_pause_resume_short_turn_candidate")):
        return "asiaEarlyPauseResumeShortTurn"
    return None


def _final_signal_side(signal: dict[str, Any]) -> str | None:
    if bool(signal.get("long_entry")):
        return "LONG"
    if bool(signal.get("short_entry")):
        return "SHORT"
    return None


def _runtime_entry_control_blocker(
    *,
    bar: Bar,
    settings: StrategySettings,
    side: str | None,
    source: str | None,
) -> str | None:
    if side is None or source is None:
        return None
    if side == "LONG":
        if (
            settings.us_late_pause_resume_long_exclude_1755_carryover
            and source == "usLatePauseResumeLongTurn"
            and bar.end_ts.astimezone(settings.timezone_info).time().strftime("%H:%M:%S") == "16:55:00"
        ):
            return "us_late_1755_carryover_exclusion"
        if (
            settings.probationary_paper_lane_session_restriction
            and not _gc_mgc_asia_retest_hold_london_open_extension_matches(
                bar=bar,
                source=source,
                timezone_info=settings.timezone_info,
            )
            and not _bar_matches_probationary_session_restriction(
                bar,
                settings.probationary_paper_lane_session_restriction,
                settings.timezone_info,
            )
        ):
            return f"probationary_session_restriction_{settings.probationary_paper_lane_session_restriction.lower()}"
        if settings.probationary_enforce_approved_branches and source not in settings.approved_long_entry_sources:
            return "probationary_long_source_not_allowlisted"
        return None

    if (
        settings.probationary_paper_lane_session_restriction
        and not _bar_matches_probationary_session_restriction(
            bar,
            settings.probationary_paper_lane_session_restriction,
            settings.timezone_info,
        )
    ):
        return f"probationary_session_restriction_{settings.probationary_paper_lane_session_restriction.lower()}"
    if settings.probationary_enforce_approved_branches and source not in settings.approved_short_entry_sources:
        return "probationary_short_source_not_allowlisted"
    return None


def _order_creation_blocker(
    *,
    bar_index: int,
    settings: StrategySettings,
    state: StrategyState,
) -> str | None:
    if bar_index + 1 < settings.warmup_bars_required():
        return "warmup_not_complete"
    if state.fault_code:
        return state.fault_code
    if state.operator_halt:
        return "operator_halt"
    if not state.entries_enabled:
        return "entries_disabled"
    if state.position_side is not PositionSide.FLAT:
        return f"position_{state.position_side.value.lower()}"
    if state.strategy_status is not StrategyStatus.READY:
        return f"strategy_status_{state.strategy_status.value.lower()}"
    if state.same_underlying_entry_hold:
        return "same_underlying_entry_hold"
    return None


def _anti_churn_rejected(signal: dict[str, Any], side: str | None) -> bool:
    if side == "LONG":
        return bool(signal.get("long_entry_raw")) and not bool(signal.get("long_entry")) and bool(signal.get("recent_long_setup"))
    if side == "SHORT":
        return bool(signal.get("short_entry_raw")) and not bool(signal.get("short_entry")) and bool(signal.get("recent_short_setup"))
    return False


def _build_marker_rows(
    intents: Sequence[dict[str, Any]],
    fills: Sequence[dict[str, Any]],
    *,
    entry_only: bool = False,
    exit_only: bool = False,
) -> list[dict[str, Any]]:
    markers: list[dict[str, Any]] = []
    fill_lookup = {str(row["order_intent_id"]): dict(row) for row in fills}
    for row in intents:
        intent_type = OrderIntentType(str(row["intent_type"]))
        if entry_only and intent_type not in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN):
            continue
        if exit_only and intent_type not in (OrderIntentType.SELL_TO_CLOSE, OrderIntentType.BUY_TO_CLOSE):
            continue
        markers.append(
            {
                "kind": "intent",
                "order_intent_id": row.get("order_intent_id"),
                "intent_type": intent_type.value,
                "side": "LONG" if intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_CLOSE) else "SHORT",
                "quantity": int(row.get("quantity") or 0),
                "price": fill_lookup.get(str(row.get("order_intent_id")), {}).get("fill_price"),
                "reason_code": row.get("reason_code"),
                "timestamp": row.get("created_at"),
            }
        )
    return markers


def _build_fill_marker_rows(fills: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    markers: list[dict[str, Any]] = []
    for row in fills:
        intent_type = OrderIntentType(str(row["intent_type"]))
        markers.append(
            {
                "kind": "fill",
                "order_intent_id": row.get("order_intent_id"),
                "intent_type": intent_type.value,
                "side": "LONG" if intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_CLOSE) else "SHORT",
                "price": row.get("fill_price"),
                "timestamp": row.get("fill_timestamp"),
                "is_entry": intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN),
                "is_exit": intent_type in (OrderIntentType.SELL_TO_CLOSE, OrderIntentType.BUY_TO_CLOSE),
            }
        )
    return markers


def _compute_unrealized_pnl(
    *,
    state: StrategyState,
    close_price: Decimal,
    point_value: Decimal | None,
) -> Decimal | None:
    if point_value is None:
        return None
    if state.entry_price is None:
        return Decimal("0") if state.position_side is PositionSide.FLAT else None
    quantity = Decimal(abs(state.internal_position_qty))
    if state.position_side is PositionSide.FLAT:
        return Decimal("0")
    if state.position_side is PositionSide.LONG:
        return (close_price - state.entry_price) * quantity * point_value
    if state.position_side is PositionSide.SHORT:
        return (state.entry_price - close_price) * quantity * point_value
    return None


def _build_summary(
    *,
    rows: Sequence[dict[str, Any]],
    closed_trades: Sequence[dict[str, Any]],
    blocker_counter: Counter[str],
    point_value: Decimal | None,
    priced_trade_count: int,
) -> dict[str, Any]:
    long_trades = sum(1 for trade in closed_trades if trade.get("direction") == "LONG")
    short_trades = sum(1 for trade in closed_trades if trade.get("direction") == "SHORT")
    winners = None
    losers = None
    pnl_unavailable_reason = None
    if point_value is None:
        pnl_unavailable_reason = "Point value was not available from persisted replay/runtime configuration, so priced P/L fields are omitted."
    elif priced_trade_count < len(closed_trades):
        pnl_unavailable_reason = "One or more closed trades could not be priced exactly from persisted truth."
    else:
        winners = sum(1 for trade in closed_trades if (trade.get("net_pnl") or Decimal("0")) > 0)
        losers = sum(1 for trade in closed_trades if (trade.get("net_pnl") or Decimal("0")) < 0)

    total_path = [Decimal(str(row["cumulative_total_pnl"])) for row in rows if row.get("cumulative_total_pnl") is not None]
    max_run_up = max(total_path) if total_path else None
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    for value in total_path:
        if value > peak:
            peak = value
        drawdown = peak - value
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    most_common_blocker_codes = [
        {"code": code, "count": count}
        for code, count in blocker_counter.most_common(5)
    ]
    no_trade_regions = _summarize_no_trade_regions(rows)
    session_level_behavior = _summarize_session_behavior(rows)
    last_row = rows[-1] if rows else {}
    atp_summary = _build_atp_summary(rows)
    closed_trade_breakdown = _study_closed_trade_breakdown(closed_trades, point_value=point_value)
    return {
        "bar_count": len(rows),
        "total_trades": len(closed_trades),
        "long_trades": long_trades,
        "short_trades": short_trades,
        "winners": winners,
        "losers": losers,
        "profit_factor": _study_profit_factor(closed_trades, point_value=point_value),
        "cumulative_realized_pnl": last_row.get("cumulative_realized_pnl"),
        "cumulative_total_pnl": last_row.get("cumulative_total_pnl"),
        "max_run_up": _decimal_to_str(max_run_up),
        "max_drawdown": _decimal_to_str(max_drawdown),
        "most_common_blocker_codes": most_common_blocker_codes,
        "most_common_legacy_blocker_codes": most_common_blocker_codes,
        "no_trade_regions": no_trade_regions,
        "session_level_behavior": session_level_behavior,
        "closed_trade_breakdown": closed_trade_breakdown,
        "trade_family_breakdown": _study_trade_group_breakdown(
            closed_trades,
            label_kind="family",
            point_value=point_value,
        ),
        "session_trade_breakdown": _study_trade_group_breakdown(
            closed_trades,
            label_kind="session",
            point_value=point_value,
        ),
        "latest_trade_summary": _study_latest_trade_summary(closed_trades, point_value=point_value),
        "atp_summary": atp_summary,
        "pnl_supportable": point_value is not None,
        "pnl_unavailable_reason": pnl_unavailable_reason,
    }


def _summarize_no_trade_regions(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    regions: list[dict[str, Any]] = []
    active: dict[str, Any] | None = None
    for row in rows:
        idle = (
            not row.get("entry_marker")
            and not row.get("exit_marker")
            and not row.get("fill_marker")
            and str(row.get("position_side") or "FLAT") == "FLAT"
        )
        if idle:
            if active is None:
                active = {
                    "start_timestamp": row.get("timestamp"),
                    "end_timestamp": row.get("timestamp"),
                    "session_phase": row.get("session_phase"),
                    "bar_count": 1,
                }
            else:
                active["end_timestamp"] = row.get("timestamp")
                active["bar_count"] += 1
            continue
        if active is not None and active["bar_count"] >= 2:
            regions.append(dict(active))
        active = None
    if active is not None and active["bar_count"] >= 2:
        regions.append(dict(active))
    regions.sort(key=lambda row: (-int(row["bar_count"]), str(row["start_timestamp"])))
    return regions[:5]


def _study_closed_trade_breakdown(trades: Sequence[dict[str, Any]], *, point_value: Decimal | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, trade in enumerate(sorted(trades, key=lambda row: _study_trade_sort_key(dict(row)), reverse=True), start=1):
        trade_row = dict(trade)
        entry_timestamp = _maybe_iso_text(trade_row.get("entry_timestamp") or trade_row.get("entry_ts"))
        exit_timestamp = _maybe_iso_text(trade_row.get("exit_timestamp") or trade_row.get("exit_ts"))
        realized_pnl = _study_trade_realized_pnl(trade_row, point_value=point_value)
        rows.append(
            {
                "trade_id": trade_row.get("trade_id") or trade_row.get("decision_id") or f"study-trade-{index}",
                "family": _study_trade_family(trade_row),
                "side": trade_row.get("side") or trade_row.get("direction"),
                "entry_timestamp": entry_timestamp,
                "exit_timestamp": exit_timestamp,
                "entry_price": _decimal_to_str(_decimal_or_none(trade_row.get("entry_price"))),
                "exit_price": _decimal_to_str(_decimal_or_none(trade_row.get("exit_price"))),
                "realized_pnl": _decimal_to_str(realized_pnl),
                "exit_reason": trade_row.get("exit_reason") or trade_row.get("primary_exit_reason"),
                "entry_session_phase": trade_row.get("entry_session_phase") or _study_trade_session_phase(entry_timestamp),
                "exit_session_phase": trade_row.get("exit_session_phase") or _study_trade_session_phase(exit_timestamp),
            }
        )
    return rows


def _study_trade_group_breakdown(
    trades: Sequence[dict[str, Any]],
    *,
    label_kind: str,
    point_value: Decimal | None,
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for trade in trades:
        trade_row = dict(trade)
        label = (
            _study_trade_family(trade_row)
            if label_kind == "family"
            else trade_row.get("entry_session_phase")
            or _study_trade_session_phase(_maybe_iso_text(trade_row.get("entry_timestamp") or trade_row.get("entry_ts")))
        )
        label_text = str(label or "").strip()
        if not label_text:
            continue
        payload = grouped.setdefault(
            label_text,
            {
                label_kind: label_text,
                "trade_count": 0,
                "wins": 0,
                "losses": 0,
                "realized_pnl": Decimal("0"),
                "realized_pnl_available": True,
                "latest_trade_timestamp": None,
            },
        )
        payload["trade_count"] += 1
        pnl = _study_trade_realized_pnl(trade_row, point_value=point_value)
        if pnl is None:
            payload["realized_pnl_available"] = False
        else:
            payload["realized_pnl"] += pnl
            if pnl > 0:
                payload["wins"] += 1
            elif pnl < 0:
                payload["losses"] += 1
        latest_trade_timestamp = _study_trade_sort_key(trade_row)
        if latest_trade_timestamp and (
            payload["latest_trade_timestamp"] is None
            or latest_trade_timestamp > payload["latest_trade_timestamp"]
        ):
            payload["latest_trade_timestamp"] = latest_trade_timestamp
    rows: list[dict[str, Any]] = []
    for payload in grouped.values():
        rows.append(
            {
                label_kind: payload[label_kind],
                "trade_count": payload["trade_count"],
                "wins": payload["wins"],
                "losses": payload["losses"],
                "realized_pnl": _decimal_to_str(payload["realized_pnl"]) if payload["realized_pnl_available"] else None,
                "latest_trade_timestamp": payload["latest_trade_timestamp"],
            }
        )
    rows.sort(
        key=lambda row: (
            _decimal_or_none(row.get("realized_pnl")) or Decimal("-999999999"),
            str(row.get(label_kind) or ""),
        ),
        reverse=True,
    )
    return rows


def _study_latest_trade_summary(trades: Sequence[dict[str, Any]], *, point_value: Decimal | None) -> dict[str, Any] | None:
    ordered_rows = _study_closed_trade_breakdown(trades, point_value=point_value)
    return ordered_rows[0] if ordered_rows else None


def _study_profit_factor(trades: Sequence[dict[str, Any]], *, point_value: Decimal | None) -> str | None:
    pnl_values = [_study_trade_realized_pnl(dict(trade), point_value=point_value) for trade in trades]
    if not pnl_values or any(value is None for value in pnl_values):
        return None
    gross_profit = sum((value for value in pnl_values if value and value > 0), Decimal("0"))
    gross_loss = sum((-value for value in pnl_values if value and value < 0), Decimal("0"))
    if gross_loss > 0:
        return _decimal_to_str(gross_profit / gross_loss)
    if gross_profit > 0 and pnl_values:
        return "999"
    return None


def _study_trade_realized_pnl(trade: dict[str, Any], *, point_value: Decimal | None) -> Decimal | None:
    for key in ("net_pnl", "realized_pnl", "pnl_cash"):
        if trade.get(key) not in (None, ""):
            return Decimal(str(trade.get(key)))
    pnl_points = trade.get("pnl_points")
    if pnl_points in (None, ""):
        return None
    pnl_points_decimal = Decimal(str(pnl_points))
    return pnl_points_decimal * point_value if point_value is not None else pnl_points_decimal


def _study_trade_family(trade: dict[str, Any]) -> str | None:
    for key in ("family", "family_name", "signal_family_label", "source"):
        value = str(trade.get(key) or "").strip()
        if value:
            return value
    return None


def _study_trade_session_phase(timestamp: str | None) -> str | None:
    parsed = _parse_iso_timestamp(timestamp)
    return label_session_phase(parsed) if parsed is not None else None


def _study_trade_sort_key(trade: dict[str, Any]) -> str:
    return str(
        _maybe_iso_text(trade.get("exit_timestamp") or trade.get("exit_ts") or trade.get("entry_timestamp") or trade.get("entry_ts"))
        or ""
    )


def _summarize_session_behavior(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(str(row.get("session_phase") or "UNKNOWN"), []).append(dict(row))
    summary_rows: list[dict[str, Any]] = []
    for session_phase, bucket in sorted(buckets.items()):
        first_total = Decimal(str(bucket[0]["cumulative_total_pnl"])) if bucket[0].get("cumulative_total_pnl") is not None else None
        last_total = Decimal(str(bucket[-1]["cumulative_total_pnl"])) if bucket[-1].get("cumulative_total_pnl") is not None else None
        net_change = last_total - first_total if first_total is not None and last_total is not None else None
        summary_rows.append(
            {
                "session_phase": session_phase,
                "bar_count": len(bucket),
                "entry_marked_bars": sum(1 for row in bucket if row.get("entry_marker")),
                "exit_marked_bars": sum(1 for row in bucket if row.get("exit_marker")),
                "fill_marked_bars": sum(1 for row in bucket if row.get("fill_marker")),
                "blocked_bars": sum(1 for row in bucket if row.get("entry_blocked")),
                "eligible_bars": sum(1 for row in bucket if row.get("entry_eligible")),
                "net_pnl_change": _decimal_to_str(net_change),
                "start_timestamp": bucket[0].get("timestamp"),
                "end_timestamp": bucket[-1].get("timestamp"),
            }
        )
    return summary_rows


def _build_atp_summary(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    atp_rows = [row for row in rows if row.get("current_bias_state") is not None or row.get("atp_entry_state") is not None]
    if not atp_rows:
        return {
            "available": False,
            "timing_available": False,
            "unavailable_reason": "ATP feature or entry state truth was not available for this strategy-study run.",
        }

    bias_counter = Counter(str(row.get("current_bias_state") or "NEUTRAL") for row in atp_rows)
    pullback_counter = Counter(str(row.get("current_pullback_state") or "NO_PULLBACK") for row in atp_rows)
    entry_state_counter = Counter(str(row.get("atp_entry_state") or "UNAVAILABLE") for row in atp_rows)
    continuation_counter = Counter(str(row.get("continuation_state") or "UNAVAILABLE") for row in atp_rows)
    atp_blockers = Counter(
        str(row.get("atp_blocker_code"))
        for row in atp_rows
        if row.get("atp_blocker_code")
    )
    no_trade_reasons = Counter(
        _atp_no_trade_reason(row)
        for row in atp_rows
        if _atp_no_trade_reason(row) is not None
    )
    ready_rows = [row for row in atp_rows if row.get("atp_entry_ready") is True]
    timing_rows = [row for row in atp_rows if row.get("atp_timing_state") is not None]
    timing_confirmed_rows = [row for row in timing_rows if row.get("atp_timing_confirmed") is True]
    executed_ready_rows = [row for row in ready_rows if row.get("entry_marker") or row.get("fill_marker")]
    executed_confirmed_rows = [
        row for row in timing_confirmed_rows if row.get("entry_marker") or row.get("fill_marker")
    ]
    vwap_quality_counter = Counter(
        str(row.get("vwap_entry_quality_state"))
        for row in timing_rows
        if row.get("vwap_entry_quality_state") is not None
    )
    timing_state_counter = Counter(
        str(row.get("atp_timing_state"))
        for row in timing_rows
        if row.get("atp_timing_state") is not None
    )
    return {
        "available": True,
        "timing_available": bool(timing_rows),
        "bar_count": len(atp_rows),
        "ready_bar_count": len(ready_rows),
        "bias_state_percent": _counter_percentages(bias_counter, len(atp_rows)),
        "pullback_state_percent": _counter_percentages(pullback_counter, len(atp_rows)),
        "entry_state_percent": _counter_percentages(entry_state_counter, len(atp_rows)),
        "continuation_state_percent": _counter_percentages(continuation_counter, len(atp_rows)),
        "timing_state_percent": _counter_percentages(timing_state_counter, len(timing_rows)),
        "vwap_entry_quality_state_percent": _counter_percentages(vwap_quality_counter, len(timing_rows)),
        "ready_to_timing_confirmed_percent": _percent(len(timing_confirmed_rows), len(ready_rows)),
        "timing_confirmed_to_executed_percent": _percent(len(executed_confirmed_rows), len(timing_confirmed_rows)),
        "ready_to_executed_percent": _percent(len(executed_ready_rows), len(ready_rows)),
        "top_atp_blocker_codes": [
            {"code": code, "count": count}
            for code, count in atp_blockers.most_common(8)
        ],
        "top_no_trade_reasons": [
            {"code": code, "count": count}
            for code, count in no_trade_reasons.most_common(8)
        ],
    }


def _build_atp_feature_rows(bars: Sequence[Bar], source_bars: Sequence[Bar]) -> list[Any]:
    if not bars:
        return []
    research_bars_5m = [_to_research_bar(bar) for bar in bars]
    research_bars_1m = [_to_research_bar(bar) for bar in source_bars if str(bar.timeframe or "").lower() == "1m"]
    return build_feature_states(
        bars_5m=research_bars_5m,
        bars_1m=research_bars_1m,
    )


def _compute_atp_entry_state(
    *,
    feature_rows: Sequence[Any],
    bar_index: int,
    state: StrategyState,
) -> Any | None:
    if bar_index >= len(feature_rows):
        return None
    entry_states = classify_entry_states(
        feature_rows=feature_rows[: bar_index + 1],
        runtime_ready=_atp_runtime_ready(state),
        position_flat=state.position_side is PositionSide.FLAT,
        one_position_rule_clear=state.position_side is PositionSide.FLAT,
    )
    return entry_states[-1] if entry_states else None


def _build_atp_timing_states_by_bar_id(
    *,
    bars: Sequence[Bar],
    source_bars: Sequence[Bar],
    entry_states_by_bar_id: dict[str, Any],
) -> dict[str, Any]:
    minute_bars = [_to_research_bar(bar) for bar in source_bars if str(bar.timeframe or "").lower() == "1m"]
    if not minute_bars or not entry_states_by_bar_id:
        return {}
    ordered_entry_states = [entry_states_by_bar_id[bar.bar_id] for bar in bars if bar.bar_id in entry_states_by_bar_id]
    timing_states = classify_timing_states(entry_states=ordered_entry_states, bars_1m=minute_bars)
    return {
        bar.bar_id: timing_states[index]
        for index, bar in enumerate(bars)
        if index < len(timing_states)
    }


def _to_research_bar(bar: Bar) -> ResearchBar:
    session_label = label_session_phase(bar.end_ts)
    return ResearchBar(
        instrument=bar.symbol,
        timeframe=bar.timeframe,
        start_ts=bar.start_ts,
        end_ts=bar.end_ts,
        open=float(bar.open),
        high=float(bar.high),
        low=float(bar.low),
        close=float(bar.close),
        volume=int(bar.volume),
        session_label=session_label,
        session_segment=_base_session_segment(session_label),
        source="strategy_study",
        provenance="persisted_runtime_truth",
    )


def _base_session_segment(label: str) -> str:
    if label.startswith("ASIA"):
        return "ASIA"
    if label.startswith("LONDON"):
        return "LONDON"
    if label.startswith("US"):
        return "US"
    return "UNKNOWN"


def _atp_runtime_ready(state: StrategyState) -> bool:
    return (
        bool(state.entries_enabled)
        and not bool(state.operator_halt)
        and not bool(state.reconcile_required)
        and not bool(state.fault_code)
    )


def _atp_no_trade_reason(row: dict[str, Any]) -> str | None:
    if row.get("entry_marker") or row.get("fill_marker") or str(row.get("position_side") or "FLAT") != "FLAT":
        return None
    return (
        str(row.get("atp_timing_blocker_code") or "")
        or str(row.get("atp_entry_blocker_code") or "")
        or str(row.get("atp_timing_state") or "")
        or str(row.get("atp_entry_state") or "")
        or str(row.get("legacy_blocker_code") or "")
        or None
    )


def _percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 4)


def _counter_percentages(counter: Counter[str], total: int) -> dict[str, float]:
    if total <= 0:
        return {}
    return {
        str(key): round((value / total) * 100.0, 4)
        for key, value in sorted(counter.items(), key=lambda item: str(item[0]))
    }


def _normalize_atp_value(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _float_or_none(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    numeric = float(value)
    return round(numeric, 4)


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _field_provenance(point_value: Decimal | None) -> dict[str, Any]:
    return {
        "direct_fields": [
            "bar_id",
            "timestamp",
            "start_timestamp",
            "end_timestamp",
            "open",
            "high",
            "low",
            "close",
            "session_vwap",
            "atr",
            "position_side",
            "position_qty",
            "strategy_status",
            "entry_markers",
            "exit_markers",
            "fill_markers",
        ],
        "computed_fields": [
            "realized_pnl",
            "unrealized_pnl",
            "cumulative_realized_pnl",
            "cumulative_total_pnl",
            "legacy_entry_eligible",
            "legacy_entry_blocked",
            "legacy_blocker_code",
            "legacy_latest_signal_state",
            "current_bias_state",
            "current_pullback_state",
            "continuation_state",
            "pullback_envelope_band",
            "pullback_depth_score",
            "pullback_violence_score",
            "atp_entry_state",
            "atp_entry_ready",
            "atp_entry_blocked",
            "atp_entry_blocker_code",
            "atp_timing_state",
            "atp_timing_blocker_code",
            "atp_blocker_code",
            "vwap_entry_quality_state",
        ],
        "omitted_or_unreliable_fields": [
            {
                "field": "position_phase",
                "reason": "No normalized persisted position-phase field exists in the replay/runtime truth model yet.",
            },
            {
                "field": "atp_timing_state",
                "reason": "ATP timing requires persisted 1m source bars aligned to the replay decision bars and is omitted when that truth is unavailable.",
            },
        ],
        "pnl_supportable": point_value is not None,
    }


def _decimal_to_str(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _sum_decimal(values: Iterable[Decimal | None]) -> Decimal:
    total = Decimal("0")
    for value in values:
        if value is not None:
            total += value
    return total
