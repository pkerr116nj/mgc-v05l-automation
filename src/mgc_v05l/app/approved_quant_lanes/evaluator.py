"""Evaluation helpers for approved quant baseline lanes."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from .runtime_boundary import (
    ApprovedQuantFrameSeries,
    build_approved_quant_symbol_store,
    validate_approved_quant_feature_payload,
)
from .specs import ApprovedQuantLaneSpec


@dataclass(frozen=True)
class ApprovedQuantLaneSignal:
    lane_id: str
    lane_name: str
    variant_id: str
    symbol: str
    session_label: str
    signal_timestamp: str
    entry_timestamp_planned: str
    direction: str
    signal_passed_flag: bool
    rejection_reason_code: str | None
    rule_snapshot: dict[str, float | str | bool]


@dataclass(frozen=True)
class ApprovedQuantLaneTrade:
    lane_id: str
    lane_name: str
    variant_id: str
    symbol: str
    session_label: str
    signal_timestamp: str
    entry_timestamp: str
    exit_timestamp: str
    direction: str
    entry_price: float
    stop_price: float
    target_price: float | None
    exit_price: float
    exit_reason: str
    holding_bars: int
    gross_r: float
    net_r_cost_020: float
    net_r_cost_025: float
    mae_r: float
    mfe_r: float
    bars_to_mfe: int
    bars_to_mae: int


def build_symbol_store_for_approved_lanes(
    *,
    database_path,
    execution_timeframe: str,
    specs: tuple[ApprovedQuantLaneSpec, ...],
) -> dict[str, dict[str, Any]]:
    symbols = tuple(sorted({symbol for spec in specs for symbol in spec.symbols}))
    return build_approved_quant_symbol_store(
        database_path=database_path,
        execution_timeframe=execution_timeframe,
        symbols=symbols,
    )


def evaluate_approved_lane(
    *,
    spec: ApprovedQuantLaneSpec,
    symbol_store: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    signals: list[ApprovedQuantLaneSignal] = []
    trades: list[ApprovedQuantLaneTrade] = []
    rejected_reason_counts: dict[str, dict[str, int]] = {}
    for symbol in spec.symbols:
        payload = symbol_store.get(symbol)
        if payload is None:
            continue
        symbol_result = _evaluate_symbol(
            spec=spec,
            symbol=symbol,
            execution=payload["execution"],
            features=payload["features"],
        )
        signals.extend(symbol_result["signals"])
        trades.extend(symbol_result["trades"])
        _merge_reason_counts(rejected_reason_counts, symbol_result["rejected_reason_counts"])

    return {
        "spec": asdict(spec),
        "signals": [asdict(signal) for signal in signals],
        "trades": [asdict(trade) for trade in trades],
        "rejected_reason_counts": rejected_reason_counts,
    }


def _evaluate_symbol(
    *,
    spec: ApprovedQuantLaneSpec,
    symbol: str,
    execution: ApprovedQuantFrameSeries,
    features: list[dict[str, Any]],
) -> dict[str, Any]:
    signals: list[ApprovedQuantLaneSignal] = []
    trades: list[ApprovedQuantLaneTrade] = []
    rejected_reason_counts: dict[str, dict[str, int]] = {}
    next_available_index = 0
    for index, feature in enumerate(features):
        if index + 1 >= len(execution.bars):
            continue
        if not feature.get("ready"):
            continue
        validate_approved_quant_feature_payload(lane_id=spec.lane_id, feature=feature)
        session_label = str(feature["session_label"])
        signal_date = execution.timestamps[index].date().isoformat()
        if index < next_available_index:
            _increment_reason(rejected_reason_counts, signal_date, "position_still_open")
            continue
        rejection_reason = lane_rejection_reason(spec=spec, session_label=session_label, feature=feature)
        if rejection_reason is not None:
            _increment_reason(rejected_reason_counts, signal_date, rejection_reason)
            continue

        entry_index = index + 1
        entry_price = execution.opens[entry_index]
        risk = max(float(feature["risk_unit"]), 1e-6)
        exit_index, exit_price, exit_reason, stop_price, target_price = _resolve_lane_exit(
            spec=spec,
            execution=execution,
            features=features,
            entry_index=entry_index,
            entry_price=entry_price,
            risk=risk,
        )
        mae_r, mfe_r, bars_to_mfe, bars_to_mae = _excursion_stats(
            execution=execution,
                direction=spec.direction,
                entry_index=entry_index,
                exit_index=exit_index,
                entry_price=entry_price,
                risk=risk,
        )
        gross_r = (exit_price - entry_price) / risk if spec.direction == "LONG" else (entry_price - exit_price) / risk
        rule_snapshot = lane_rule_snapshot(spec=spec, session_label=session_label, feature=feature)
        signals.append(
            ApprovedQuantLaneSignal(
                lane_id=spec.lane_id,
                lane_name=spec.lane_name,
                variant_id=spec.variant_id,
                symbol=symbol,
                session_label=session_label,
                signal_timestamp=execution.timestamps[index].isoformat(),
                entry_timestamp_planned=execution.timestamps[entry_index].isoformat(),
                direction=spec.direction,
                signal_passed_flag=True,
                rejection_reason_code=None,
                rule_snapshot=rule_snapshot,
            )
        )
        trades.append(
            ApprovedQuantLaneTrade(
                lane_id=spec.lane_id,
                lane_name=spec.lane_name,
                variant_id=spec.variant_id,
                symbol=symbol,
                session_label=session_label,
                signal_timestamp=execution.timestamps[index].isoformat(),
                entry_timestamp=execution.timestamps[entry_index].isoformat(),
                exit_timestamp=execution.timestamps[exit_index].isoformat(),
                direction=spec.direction,
                entry_price=round(entry_price, 6),
                stop_price=round(stop_price, 6),
                target_price=round(target_price, 6) if target_price is not None else None,
                exit_price=round(exit_price, 6),
                exit_reason=exit_reason,
                holding_bars=max(exit_index - entry_index + 1, 1),
                gross_r=round(gross_r, 6),
                net_r_cost_020=round(gross_r - 0.20, 6),
                net_r_cost_025=round(gross_r - 0.25, 6),
                mae_r=mae_r,
                mfe_r=mfe_r,
                bars_to_mfe=bars_to_mfe,
                bars_to_mae=bars_to_mae,
            )
        )
        next_available_index = exit_index + 1
    return {
        "signals": signals,
        "trades": trades,
        "rejected_reason_counts": rejected_reason_counts,
    }


def lane_rejection_reason(*, spec: ApprovedQuantLaneSpec, session_label: str, feature: dict[str, Any]) -> str | None:
    if session_label not in spec.allowed_sessions:
        return "session_excluded"
    if spec.lane_id == "phase2c.breakout.metals_only.us_unknown.baseline":
        if not bool(feature["regime_up"]):
            return "regime_not_up"
        if float(feature["compression_60"]) > float(spec.params["compression_60_max"]):
            return "compression_60_too_high"
        if float(feature["compression_5"]) > float(spec.params["compression_5_max"]):
            return "compression_5_too_high"
        if float(feature["breakout_up"]) < float(spec.params["breakout_min"]):
            return "breakout_up_too_small"
        if float(feature["close_pos"]) < float(spec.params["close_pos_min"]):
            return "close_pos_too_small"
        if float(feature["slope_60"]) < float(spec.params["slope_60_min"]):
            return "slope_60_too_small"
        return None
    if spec.lane_id == "phase2c.failed.core4_plus_qc.no_us.baseline":
        if not bool(feature["failed_breakout_short"]):
            return "failed_breakout_missing"
        if float(feature["dist_240"]) < float(spec.params["dist_240_extreme"]):
            return "dist_240_not_extended"
        if float(feature["close_pos"]) > float(spec.params["close_pos_max"]):
            return "close_pos_not_low_enough"
        if float(feature["body_r"]) < float(spec.params["body_r_min"]):
            return "body_r_too_small"
        return None
    return "unknown_lane"


def lane_rule_snapshot(*, spec: ApprovedQuantLaneSpec, session_label: str, feature: dict[str, Any]) -> dict[str, float | str | bool]:
    if spec.lane_id == "phase2c.breakout.metals_only.us_unknown.baseline":
        keys = ("regime_up", "compression_60", "compression_5", "breakout_up", "close_pos", "slope_60")
    else:
        keys = ("failed_breakout_short", "dist_240", "close_pos", "body_r")
    snapshot = {"session_label": session_label}
    for key in keys:
        snapshot[key] = feature[key]
    return snapshot


def _resolve_lane_exit(
    *,
    spec: ApprovedQuantLaneSpec,
    execution: ApprovedQuantFrameSeries,
    features: list[dict[str, Any]],
    entry_index: int,
    entry_price: float,
    risk: float,
) -> tuple[int, float, str, float, float | None]:
    stop_price = entry_price - spec.stop_r * risk if spec.direction == "LONG" else entry_price + spec.stop_r * risk
    target_price = None
    if spec.target_r is not None:
        target_price = entry_price + spec.target_r * risk if spec.direction == "LONG" else entry_price - spec.target_r * risk
    last_index = min(entry_index + spec.hold_bars - 1, len(execution.bars) - 1)
    for index in range(entry_index, last_index + 1):
        high = execution.highs[index]
        low = execution.lows[index]
        close = execution.closes[index]
        if spec.direction == "LONG":
            stop_hit = low <= stop_price
            target_hit = target_price is not None and high >= target_price
        else:
            stop_hit = high >= stop_price
            target_hit = target_price is not None and low <= target_price
        if stop_hit and target_hit:
            return index, stop_price, "stop_first_conflict", stop_price, target_price
        if stop_hit:
            return index, stop_price, "stop", stop_price, target_price
        if target_hit and target_price is not None:
            return index, target_price, "target", stop_price, target_price
        if spec.structural_invalidation_r is not None:
            if spec.direction == "LONG" and close <= entry_price - spec.structural_invalidation_r * risk:
                return index, close, "structural_invalidation", stop_price, target_price
            if spec.direction == "SHORT" and close >= entry_price + spec.structural_invalidation_r * risk:
                return index, close, "structural_invalidation", stop_price, target_price
    return last_index, execution.closes[last_index], "time_exit", stop_price, target_price


def _excursion_stats(
    *,
    execution: ApprovedQuantFrameSeries,
    direction: str,
    entry_index: int,
    exit_index: int,
    entry_price: float,
    risk: float,
) -> tuple[float, float, int, int]:
    best_mfe = float("-inf")
    worst_mae = float("inf")
    bars_to_mfe = 0
    bars_to_mae = 0
    for offset, index in enumerate(range(entry_index, exit_index + 1), start=1):
        if direction == "LONG":
            mfe = (execution.highs[index] - entry_price) / risk
            mae = (execution.lows[index] - entry_price) / risk
        else:
            mfe = (entry_price - execution.lows[index]) / risk
            mae = (entry_price - execution.highs[index]) / risk
        if mfe > best_mfe:
            best_mfe = mfe
            bars_to_mfe = offset
        if mae < worst_mae:
            worst_mae = mae
            bars_to_mae = offset
    return round(worst_mae, 6), round(best_mfe, 6), bars_to_mfe, bars_to_mae


def _increment_reason(container: dict[str, dict[str, int]], bucket: str, reason: str) -> None:
    counts = container.setdefault(bucket, {})
    counts[reason] = int(counts.get(reason, 0)) + 1


def _merge_reason_counts(target: dict[str, dict[str, int]], source: dict[str, dict[str, int]]) -> None:
    for bucket, counts in source.items():
        merged = target.setdefault(bucket, {})
        for reason, value in counts.items():
            merged[reason] = int(merged.get(reason, 0)) + int(value)
