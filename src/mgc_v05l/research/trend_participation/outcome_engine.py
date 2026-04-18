"""Authoritative ATP forward-outcome engine and row adapters."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from .models import AtpTimingState, FeatureState, TradeRecord
from .phase2_continuation import atp_phase2_variant
from .phase3_timing import ATP_REPLAY_EXIT_POLICY_FIXED_TARGET, simulate_timed_entries


def generate_atp_trade_records(
    *,
    timing_states: Sequence[AtpTimingState],
    bars_1m: Sequence[Any],
    point_value: float,
    slippage_points: float = 0.25,
    fee_per_trade: float = 1.50,
    feature_rows: Sequence[FeatureState] | None = None,
    exit_policy: str = ATP_REPLAY_EXIT_POLICY_FIXED_TARGET,
    variant_overrides: Mapping[str, Any] | None = None,
) -> list[TradeRecord]:
    return simulate_timed_entries(
        timing_states=timing_states,
        bars_1m=bars_1m,
        point_value=point_value,
        variant=atp_phase2_variant(variant_overrides=variant_overrides),
        slippage_points=slippage_points,
        fee_per_trade=fee_per_trade,
        feature_rows=feature_rows,
        exit_policy=exit_policy,
        variant_overrides=variant_overrides,
    )


def trade_record_to_retest_row(
    trade: TradeRecord,
    *,
    vwap_price_quality_state: str | None = None,
) -> dict[str, Any]:
    return {
        "trade_id": trade.decision_id,
        "entry_timestamp": trade.entry_ts.isoformat(),
        "exit_timestamp": trade.exit_ts.isoformat(),
        "entry_price": round(float(trade.entry_price), 6),
        "exit_price": round(float(trade.exit_price), 6),
        "side": trade.side,
        "family": trade.family,
        "entry_session_phase": trade.session_segment,
        "exit_reason": trade.exit_reason,
        "realized_pnl": round(float(trade.pnl_cash), 6),
        "vwap_price_quality_state": vwap_price_quality_state,
        "trade_record": trade,
    }


def trade_record_to_position_row(trade: TradeRecord) -> dict[str, Any]:
    return {
        "trade_id": trade.decision_id,
        "entry_ts": trade.entry_ts,
        "exit_ts": trade.exit_ts,
        "decision_ts": trade.decision_ts,
        "entry_price": float(trade.entry_price),
        "exit_price": float(trade.exit_price),
        "stop_price": float(trade.stop_price),
        "pnl_cash": float(trade.pnl_cash),
        "mfe_points": float(trade.mfe_points),
        "mae_points": float(trade.mae_points),
        "hold_minutes": float(trade.hold_minutes),
        "bars_held_1m": int(trade.bars_held_1m),
        "side": trade.side,
        "session_segment": trade.session_segment,
        "family": trade.family,
        "exit_reason": trade.exit_reason,
        "added": False,
        "add_pnl_cash": 0.0,
        "add_reason": None,
        "add_price_quality_state": None,
    }


def trade_records_to_retest_rows(
    trades: Sequence[TradeRecord],
    *,
    timing_states_by_decision_id: Mapping[str, AtpTimingState] | None = None,
) -> list[dict[str, Any]]:
    return [
        trade_record_to_retest_row(
            trade,
            vwap_price_quality_state=(
                timing_states_by_decision_id[trade.decision_id].vwap_price_quality_state
                if timing_states_by_decision_id and trade.decision_id in timing_states_by_decision_id
                else None
            ),
        )
        for trade in trades
    ]


def trade_records_to_position_rows(trades: Sequence[TradeRecord]) -> list[dict[str, Any]]:
    return [trade_record_to_position_row(trade) for trade in trades]
