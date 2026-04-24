"""Adapters from existing producer flows into Layer 1 StrategyBacktest contracts."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable

from mgc_v05l.app.approved_quant_lanes.runtime_boundary import APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION
from mgc_v05l.app.approved_quant_lanes.specs import (
    ApprovedQuantLaneSpec,
    approved_quant_lane_scope_fingerprint,
    approved_quant_lane_scope_payload,
)
from mgc_v05l.app.gc_mgc_ny_early_short_forced_session_research import ForcedNyEarlyShortSpec, ForcedSessionTrade
from mgc_v05l.research.trend_participation.models import ResearchBar
from mgc_v05l.research.trend_participation.models import TradeRecord as TrendTradeRecord

from ..data.contracts import EquityPoint, PositionPoint, PriceBar, StrategyBacktest, TradeRecord
from .producer_contract import (
    build_data_provenance,
    build_execution_assumptions,
    build_strategy_backtest_provenance,
    build_trade_validation_metadata,
    full_sample_split_window,
)


def _price_bars_from_research_bars(bars: Iterable[ResearchBar]) -> tuple[PriceBar, ...]:
    return tuple(
        PriceBar(
            timestamp=bar.end_ts,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=float(bar.volume),
            session_label=bar.session_segment,
            metadata={"source": bar.source, "provenance": bar.provenance},
        )
        for bar in sorted(bars, key=lambda item: item.end_ts)
    )


def _build_equity_curve(trades: tuple[TradeRecord, ...], start: datetime, starting_equity: float = 100_000.0) -> tuple[EquityPoint, ...]:
    equity = starting_equity
    curve = [EquityPoint(timestamp=start, equity=equity)]
    for trade in sorted(trades, key=lambda item: item.exit_time):
        equity += trade.net_pnl
        curve.append(EquityPoint(timestamp=trade.exit_time, equity=equity, session_label=trade.session_label))
    return tuple(curve)


def _build_position_series(trades: tuple[TradeRecord, ...], start: datetime) -> tuple[PositionPoint, ...]:
    deltas: dict[datetime, float] = defaultdict(float)
    deltas[start] += 0.0
    for trade in sorted(trades, key=lambda item: item.entry_time):
        signed_qty = trade.qty if trade.direction == "long" else -trade.qty
        deltas[trade.entry_time] += signed_qty
        deltas[trade.exit_time] -= signed_qty

    running_position = 0.0
    positions: list[PositionPoint] = []
    for timestamp in sorted(deltas):
        running_position += deltas[timestamp]
        positions.append(PositionPoint(timestamp=timestamp, position=running_position))
    return tuple(positions)


def build_strategy_backtest_from_trend_participation(
    *,
    strategy_name: str,
    variant_id: str,
    bars_1m: tuple[ResearchBar, ...],
    trades: tuple[TrendTradeRecord, ...],
    slippage_points: float,
    fee_per_trade: float,
    data_version: str,
    code_version: str,
    artifact_references: dict[str, str] | None = None,
) -> StrategyBacktest:
    price_bars = _price_bars_from_research_bars(bars_1m)
    fill_policy = "next_available_bar_after_decision"
    converted_trades = tuple(
        TradeRecord(
            entry_time=trade.entry_ts,
            exit_time=trade.exit_ts,
            direction=trade.side.lower(),
            qty=1.0,
            gross_pnl=trade.gross_pnl_cash,
            net_pnl=trade.pnl_cash,
            mae=trade.mae_points,
            mfe=trade.mfe_points,
            holding_bars=trade.bars_held_1m,
            holding_minutes=trade.hold_minutes,
            session_label=trade.session_segment,
            validation_metadata=build_trade_validation_metadata(
                trade_id=f"{trade.instrument}:{trade.variant_id}:{trade.entry_ts.isoformat()}",
                signal_id=trade.decision_id,
                setup_family=trade.family,
                setup_variant=trade.variant_id,
                decision_time=trade.decision_ts,
                exit_reason=trade.exit_reason,
                execution_model="research_execution",
                fill_policy=fill_policy,
                slippage_cost=trade.slippage_cost,
                fee_cost=trade.fees_paid,
                tags={"reentry_type": trade.reentry_type, "setup_quality_bucket": trade.setup_quality_bucket},
            ),
            metadata={"instrument": trade.instrument, "conflict_outcome": trade.conflict_outcome.value},
        )
        for trade in trades
    )
    start = price_bars[0].timestamp if price_bars else datetime.now(UTC)
    provenance = build_strategy_backtest_provenance(
        producer_id="trend_participation.backtest_decisions_with_audit",
        producer_family="active_trend_participation_engine",
        study_mode="research_execution_mode",
        split_method="full_sample",
        split_windows=(full_sample_split_window(start=start, end=price_bars[-1].timestamp if price_bars else start),),
        execution_assumptions=build_execution_assumptions(
            fill_policy=fill_policy,
            entry_fill_basis="next_bar_open_proxy",
            exit_fill_basis="intrabar_stop_target_with_costs",
            execution_mode="research_execution",
            slippage_model_name="fixed_points_per_trade",
            slippage_model_version="v1",
            slippage_parameters={"points": slippage_points},
            fee_model_name="fixed_dollars_per_trade",
            fee_model_version="v1",
            fee_parameters={"dollars": fee_per_trade},
            conservative_bias="explicit_fee_and_slippage_applied_to_each_trade",
            notes=("trend_participation.backtest_decisions_with_audit",),
        ),
        data_provenance=build_data_provenance(
            source_type="research_bars",
            data_source="trend_participation.synthetic_or_loaded_bars",
            data_version=data_version,
            provenance_id=f"trend_participation:{variant_id}:{data_version}",
            feature_version="trend_participation_feature_stack_v1",
            code_version=code_version,
            coverage_start=price_bars[0].timestamp if price_bars else None,
            coverage_end=price_bars[-1].timestamp if price_bars else None,
        ),
        artifact_references=artifact_references or {},
        tags=("layer1_ready", "trend_participation"),
    )
    return StrategyBacktest(
        strategy_name=strategy_name,
        symbol=bars_1m[0].instrument if bars_1m else "UNKNOWN",
        timeframe="1m",
        parameters={"variant_id": variant_id},
        bar_data=price_bars,
        trades=converted_trades,
        equity_curve=_build_equity_curve(converted_trades, start=start),
        position_series=_build_position_series(converted_trades, start=start),
        provenance=provenance,
        metadata={"pilot_family": "trend_participation"},
    )


def build_strategy_backtest_from_forced_session(
    *,
    strategy_name: str,
    symbol: str,
    spec: ForcedNyEarlyShortSpec,
    session_trades: tuple[ForcedSessionTrade, ...],
    session_bars: dict[str, tuple[ResearchBar, ...]],
    data_version: str,
    code_version: str,
) -> StrategyBacktest:
    all_bars = tuple(
        bar
        for trade_date in sorted(session_bars)
        for bar in session_bars[trade_date]
    )
    price_bars = _price_bars_from_research_bars(all_bars)
    fill_policy = "preferred_trigger_or_fallback_bar_open"
    converted_trades: list[TradeRecord] = []
    for row in session_trades:
        if not row.entered or row.entry_end_ts is None or row.exit_end_ts is None:
            continue
        entry_time = datetime.fromisoformat(row.entry_end_ts)
        exit_time = datetime.fromisoformat(row.exit_end_ts)
        converted_trades.append(
            TradeRecord(
                entry_time=entry_time,
                exit_time=exit_time,
                direction="short",
                qty=1.0,
                gross_pnl=float(row.pnl_points or 0.0),
                net_pnl=float(row.net_pnl_points or 0.0),
                mae=float(row.mae_points or 0.0),
                mfe=float(row.mfe_points or 0.0),
                holding_bars=max(1, int((row.exit_bar_number or row.entry_bar_number or 1) - (row.entry_bar_number or 0))),
                holding_minutes=max(1.0, (exit_time - entry_time).total_seconds() / 60.0),
                session_label="US",
                validation_metadata=build_trade_validation_metadata(
                    trade_id=f"{symbol}:{spec.variant_id}:{row.trade_date}:{entry_time.isoformat()}",
                    signal_id=f"{symbol}:{spec.variant_id}:{row.trade_date}",
                    setup_family="forced_session_short",
                    setup_variant=spec.variant_id,
                    decision_time=entry_time - timedelta(minutes=int(spec.decision_timeframe.removesuffix('m'))),
                    exit_reason=row.exit_reason,
                    execution_model="research_execution",
                    fill_policy=fill_policy,
                    slippage_cost=float(spec.slippage_ticks_per_side * spec.tick_size * 2.0),
                    fee_cost=float(
                        spec.gc_round_turn_commission_dollars if symbol == "GC" else spec.mgc_round_turn_commission_dollars
                    ),
                    tags={"entry_reason": row.entry_reason},
                ),
                metadata=asdict(row),
            )
        )
    mapped_trades = tuple(sorted(converted_trades, key=lambda item: item.entry_time))
    start = price_bars[0].timestamp if price_bars else datetime.now(UTC)
    provenance = build_strategy_backtest_provenance(
        producer_id="gc_mgc_ny_early_short_forced_session_research",
        producer_family="forced_session_research",
        study_mode="research_execution_mode",
        split_method="full_sample",
        split_windows=(full_sample_split_window(start=start, end=price_bars[-1].timestamp if price_bars else start),),
        execution_assumptions=build_execution_assumptions(
            fill_policy=fill_policy,
            entry_fill_basis="session_rule_trigger_or_fallback_open",
            exit_fill_basis="ema_structure_or_time_stop",
            execution_mode="research_execution",
            slippage_model_name="fixed_ticks_per_side",
            slippage_model_version="v1",
            slippage_parameters={"tick_size": spec.tick_size, "ticks_per_side": spec.slippage_ticks_per_side},
            fee_model_name="fixed_round_turn_commission_dollars",
            fee_model_version="v1",
            fee_parameters={
                "gc_round_turn_commission_dollars": spec.gc_round_turn_commission_dollars,
                "mgc_round_turn_commission_dollars": spec.mgc_round_turn_commission_dollars,
            },
            conservative_bias="fallback_entry_and_per_trade_friction_included",
            notes=(spec.description,),
        ),
        data_provenance=build_data_provenance(
            source_type="research_bars",
            data_source="forced_session_research.synthetic_or_loaded_bars",
            data_version=data_version,
            provenance_id=f"forced_session:{symbol}:{spec.variant_id}:{data_version}",
            feature_version=f"forced_session:{spec.decision_timeframe}",
            code_version=code_version,
            coverage_start=price_bars[0].timestamp if price_bars else None,
            coverage_end=price_bars[-1].timestamp if price_bars else None,
        ),
        tags=("layer1_ready", "forced_session"),
    )
    return StrategyBacktest(
        strategy_name=strategy_name,
        symbol=symbol,
        timeframe=spec.decision_timeframe,
        parameters={"variant_id": spec.variant_id, "setup_bar_count": spec.setup_bar_count, "fallback_entry_bar": spec.fallback_entry_bar},
        bar_data=price_bars,
        trades=mapped_trades,
        equity_curve=_build_equity_curve(mapped_trades, start=start),
        position_series=_build_position_series(mapped_trades, start=start),
        provenance=provenance,
        metadata={"pilot_family": "forced_session_research"},
    )


_APPROVED_QUANT_SIGNAL_FIELDS = (
    "lane_id",
    "lane_name",
    "variant_id",
    "symbol",
    "session_label",
    "signal_timestamp",
    "entry_timestamp_planned",
    "direction",
    "signal_passed_flag",
    "rule_snapshot",
)

_APPROVED_QUANT_TRADE_FIELDS = (
    "lane_id",
    "lane_name",
    "variant_id",
    "symbol",
    "session_label",
    "signal_timestamp",
    "entry_timestamp",
    "exit_timestamp",
    "direction",
    "entry_price",
    "stop_price",
    "target_price",
    "exit_price",
    "exit_reason",
    "holding_bars",
    "gross_r",
    "mae_r",
    "mfe_r",
    "bars_to_mfe",
    "bars_to_mae",
)


def _approved_quant_net_field(cost_basis_r: float) -> str:
    cost_tag = int(round(cost_basis_r * 100.0))
    field_name = f"net_r_cost_{cost_tag:03d}"
    if field_name not in {"net_r_cost_020", "net_r_cost_025"}:
        raise ValueError("Approved quant Layer 1 adapter only supports the emitted 0.20R or 0.25R net-cost scenarios.")
    return field_name


def _validate_approved_quant_payload(
    *,
    spec: ApprovedQuantLaneSpec,
    evaluated_lane: dict[str, Any],
    cost_basis_r: float,
) -> None:
    missing_sections = [name for name in ("signals", "trades") if name not in evaluated_lane]
    if missing_sections:
        raise ValueError(
            f"Approved quant Layer 1 adapter requires evaluated lane sections {missing_sections} for {spec.lane_id}."
        )

    signal_missing: list[str] = []
    for index, row in enumerate(evaluated_lane["signals"]):
        missing = sorted(field for field in _APPROVED_QUANT_SIGNAL_FIELDS if field not in row)
        if missing:
            signal_missing.append(f"signals[{index}] missing {', '.join(missing)}")
    trade_missing: list[str] = []
    net_field = _approved_quant_net_field(cost_basis_r)
    required_trade_fields = _APPROVED_QUANT_TRADE_FIELDS + (net_field,)
    for index, row in enumerate(evaluated_lane["trades"]):
        missing = sorted(field for field in required_trade_fields if field not in row)
        if missing:
            trade_missing.append(f"trades[{index}] missing {', '.join(missing)}")
    if signal_missing or trade_missing:
        details = "; ".join(signal_missing + trade_missing)
        raise ValueError(
            "Approved quant producer payload is not validation-ready for "
            f"{spec.lane_id}: {details}"
        )


def _approved_quant_session_label(bar: Any) -> str:
    if bool(getattr(bar, "session_us", False)):
        return "US"
    if bool(getattr(bar, "session_london", False)):
        return "LONDON"
    if bool(getattr(bar, "session_asia", False)):
        return "ASIA"
    return "UNKNOWN"


def _price_bars_from_approved_quant_symbol_store(
    *,
    spec: ApprovedQuantLaneSpec,
    symbol_store: dict[str, dict[str, Any]],
) -> tuple[PriceBar, ...]:
    rows: list[PriceBar] = []
    for symbol in spec.symbols:
        payload = symbol_store.get(symbol)
        if payload is None:
            continue
        execution = payload.get("execution")
        if execution is None:
            continue
        for bar in getattr(execution, "bars", []):
            rows.append(
                PriceBar(
                    timestamp=bar.end_ts,
                    open=float(bar.open),
                    high=float(bar.high),
                    low=float(bar.low),
                    close=float(bar.close),
                    volume=float(bar.volume),
                    session_label=_approved_quant_session_label(bar),
                    metadata={
                        "symbol": symbol,
                        "source": "approved_quant_execution_frame",
                    },
                )
            )
    return tuple(sorted(rows, key=lambda item: (item.timestamp, str(item.metadata.get("symbol", "")))))


def build_strategy_backtest_from_approved_quant_lane(
    *,
    spec: ApprovedQuantLaneSpec,
    evaluated_lane: dict[str, Any],
    symbol_store: dict[str, dict[str, Any]],
    execution_timeframe: str,
    data_version: str,
    code_version: str,
    strategy_name: str | None = None,
    cost_basis_r: float = 0.25,
    artifact_references: dict[str, str] | None = None,
) -> StrategyBacktest:
    _validate_approved_quant_payload(spec=spec, evaluated_lane=evaluated_lane, cost_basis_r=cost_basis_r)
    net_field = _approved_quant_net_field(cost_basis_r)
    price_bars = _price_bars_from_approved_quant_symbol_store(spec=spec, symbol_store=symbol_store)
    if not price_bars and evaluated_lane["trades"]:
        raise ValueError(f"Approved quant Layer 1 adapter requires execution bars for {spec.lane_id}.")

    signals_by_key = {
        (str(row["symbol"]), str(row["signal_timestamp"])): row
        for row in evaluated_lane["signals"]
    }
    fill_policy = "next_bar_open_after_signal"
    converted_trades: list[TradeRecord] = []
    for row in evaluated_lane["trades"]:
        entry_time = datetime.fromisoformat(str(row["entry_timestamp"]))
        exit_time = datetime.fromisoformat(str(row["exit_timestamp"]))
        signal_time = datetime.fromisoformat(str(row["signal_timestamp"]))
        symbol = str(row["symbol"])
        signal_row = signals_by_key.get((symbol, str(row["signal_timestamp"])), {})
        holding_minutes = max(0.0, (exit_time - entry_time).total_seconds() / 60.0)
        converted_trades.append(
            TradeRecord(
                entry_time=entry_time,
                exit_time=exit_time,
                direction=str(row["direction"]).lower(),
                qty=1.0,
                gross_pnl=float(row["gross_r"]),
                net_pnl=float(row[net_field]),
                mae=float(row["mae_r"]),
                mfe=float(row["mfe_r"]),
                holding_bars=int(row["holding_bars"]),
                holding_minutes=holding_minutes,
                session_label=str(row["session_label"]),
                validation_metadata=build_trade_validation_metadata(
                    trade_id=f"{spec.lane_id}:{symbol}:{entry_time.isoformat()}",
                    signal_id=f"{spec.lane_id}:{symbol}:{signal_time.isoformat()}",
                    setup_family=spec.family,
                    setup_variant=spec.variant_id,
                    decision_time=signal_time,
                    exit_reason=str(row["exit_reason"]),
                    execution_model="approved_quant_lane_research",
                    fill_policy=fill_policy,
                    tags={
                        "lane_id": spec.lane_id,
                        "lane_name": spec.lane_name,
                        "rule_snapshot": signal_row.get("rule_snapshot", {}),
                    },
                ),
                metadata={
                    "lane_id": spec.lane_id,
                    "variant_id": spec.variant_id,
                    "symbol": symbol,
                    "entry_price": float(row["entry_price"]),
                    "stop_price": float(row["stop_price"]),
                    "target_price": None if row["target_price"] is None else float(row["target_price"]),
                    "exit_price": float(row["exit_price"]),
                    "bars_to_mfe": int(row["bars_to_mfe"]),
                    "bars_to_mae": int(row["bars_to_mae"]),
                    "cost_basis_r": cost_basis_r,
                },
            )
        )

    mapped_trades = tuple(sorted(converted_trades, key=lambda item: (item.entry_time, item.metadata.get("symbol", ""))))
    if price_bars:
        start = price_bars[0].timestamp
        end = price_bars[-1].timestamp
    elif mapped_trades:
        start = mapped_trades[0].entry_time
        end = mapped_trades[-1].exit_time
    else:
        start = datetime.now(UTC)
        end = start

    scope_fingerprint = approved_quant_lane_scope_fingerprint(spec)
    provenance = build_strategy_backtest_provenance(
        producer_id="approved_quant_lanes.evaluate_approved_lane",
        producer_family="approved_quant_lanes",
        study_mode="approved_quant_lane_research_evaluation",
        split_method="full_sample",
        split_windows=(full_sample_split_window(start=start, end=end),),
        execution_assumptions=build_execution_assumptions(
            fill_policy=fill_policy,
            entry_fill_basis="next_bar_open_from_execution_frame",
            exit_fill_basis=spec.exit_style,
            execution_mode="research_evaluation",
            slippage_model_name="combined_r_cost_haircut",
            slippage_model_version="approved_quant_v1",
            slippage_parameters={
                "deduction_r": cost_basis_r,
                "per_trade": True,
                "includes_fees": True,
            },
            fee_model_name="embedded_in_combined_r_cost",
            fee_model_version="approved_quant_v1",
            fee_parameters={
                "additional_fee_r": 0.0,
                "separately_parameterized": False,
            },
            conservative_bias="net_pnl uses the tougher emitted combined-cost scenario rather than gross-R headline results",
            notes=(
                APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION,
                spec.exit_style,
            ),
        ),
        data_provenance=build_data_provenance(
            source_type="approved_quant_symbol_store",
            data_source="mgc_v05l.app.approved_quant_lanes.runtime_boundary.build_approved_quant_symbol_store",
            data_version=data_version,
            provenance_id=f"approved_quant_lane:{spec.lane_id}:{data_version}",
            feature_version=APPROVED_QUANT_BASELINE_RUNTIME_CONTRACT_VERSION,
            code_version=code_version,
            coverage_start=start,
            coverage_end=end,
            artifact_ids={
                "scope_fingerprint": scope_fingerprint,
                "approval_source": str(spec.approval_source),
            },
            tags=("approved_quant", "layer1_ready"),
        ),
        artifact_references=artifact_references or {},
        tags=("layer1_ready", "approved_quant_lane", spec.family),
    )
    return StrategyBacktest(
        strategy_name=strategy_name or f"approved_quant_lane::{spec.variant_id}",
        symbol=spec.symbols[0] if len(spec.symbols) == 1 else "MULTI",
        timeframe=execution_timeframe,
        parameters={
            "variant_id": spec.variant_id,
            "family": spec.family,
            "symbols": spec.symbols,
            "allowed_sessions": spec.allowed_sessions,
            "hold_bars": spec.hold_bars,
            "stop_r": spec.stop_r,
            "target_r": spec.target_r,
            "gating_mode": spec.gating_mode,
        },
        bar_data=price_bars,
        trades=mapped_trades,
        equity_curve=_build_equity_curve(mapped_trades, start=start),
        position_series=_build_position_series(mapped_trades, start=start),
        provenance=provenance,
        metadata={
            "pilot_family": "approved_quant_lanes",
            "approved_scope": approved_quant_lane_scope_payload(spec),
            "scope_fingerprint": scope_fingerprint,
            "underlying_symbols": spec.symbols,
            "cost_basis_r": cost_basis_r,
            "cost_note": "Approved quant producer emits combined post-cost scenarios rather than separate slippage and fee fields.",
        },
    )
