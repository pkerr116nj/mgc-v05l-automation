"""Active intraday backtest helpers with conservative execution assumptions."""

from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from datetime import timedelta
from statistics import fmean
from typing import Iterable

from .models import PatternVariant, PerformanceSummary, SignalDecision, TradeRecord, VariantExecutionAudit


def backtest_decisions(
    *,
    decisions: Iterable[SignalDecision],
    bars_1m: Iterable,
    variants_by_id: dict[str, PatternVariant],
    point_values: dict[str, float],
    include_shadow_only: bool,
    slippage_points: float = 0.25,
    fee_per_trade: float = 1.50,
) -> list[TradeRecord]:
    trades, _ = backtest_decisions_with_audit(
        decisions=decisions,
        bars_1m=bars_1m,
        variants_by_id=variants_by_id,
        point_values=point_values,
        include_shadow_only=include_shadow_only,
        slippage_points=slippage_points,
        fee_per_trade=fee_per_trade,
    )
    return trades


def backtest_decisions_with_audit(
    *,
    decisions: Iterable[SignalDecision],
    bars_1m: Iterable,
    variants_by_id: dict[str, PatternVariant],
    point_values: dict[str, float],
    include_shadow_only: bool,
    slippage_points: float = 0.25,
    fee_per_trade: float = 1.50,
) -> tuple[list[TradeRecord], list[VariantExecutionAudit]]:
    minute_bars = sorted(bars_1m, key=lambda bar: (bar.instrument, bar.end_ts))
    bars_by_instrument: dict[str, list] = defaultdict(list)
    for bar in minute_bars:
        bars_by_instrument[bar.instrument].append(bar)

    decisions_by_key: dict[tuple[str, str], list[SignalDecision]] = defaultdict(list)
    for decision in decisions:
        if decision.shadow_only and not include_shadow_only:
            continue
        decisions_by_key[(decision.instrument, decision.variant_id)].append(decision)

    trades: list[TradeRecord] = []
    audits: list[VariantExecutionAudit] = []
    for (instrument, variant_id), decision_bucket in decisions_by_key.items():
        variant = variants_by_id[variant_id]
        candidate_bars = bars_by_instrument.get(instrument, [])
        stream_trades, stream_audit = _simulate_variant_stream(
            decisions=sorted(decision_bucket, key=lambda item: item.decision_ts),
            minute_bars=candidate_bars,
            variant=variant,
            point_value=point_values.get(instrument, 5.0),
            slippage_points=slippage_points,
            fee_per_trade=fee_per_trade,
        )
        trades.extend(stream_trades)
        audits.append(stream_audit)
    return trades, audits


def summarize_performance(trades: Iterable[TradeRecord]) -> PerformanceSummary:
    items = list(trades)
    if not items:
        return PerformanceSummary(
            trade_count=0,
            active_days=0,
            trades_per_day=0.0,
            expectancy=0.0,
            expectancy_per_hour=0.0,
            profit_factor=0.0,
            max_drawdown=0.0,
            win_rate=0.0,
            avg_win=0.0,
            avg_loss=0.0,
            avg_hold_minutes=0.0,
            stopout_rate=0.0,
            reentry_trade_count=0,
            reentry_expectancy=0.0,
            net_pnl_cash=0.0,
            gross_profit=0.0,
            gross_loss=0.0,
            gross_pnl_before_cost=0.0,
            total_fees=0.0,
            total_slippage_cost=0.0,
            long_trade_count=0,
            short_trade_count=0,
            by_session={},
            by_regime={},
            by_volatility={},
        )

    pnls = [trade.pnl_cash for trade in items]
    positive = [value for value in pnls if value > 0]
    negative = [value for value in pnls if value < 0]
    gross_profit = sum(positive)
    gross_loss = abs(sum(negative))
    expectancy = sum(pnls) / len(pnls)
    total_hold_minutes = sum(trade.hold_minutes for trade in items)
    expectancy_per_hour = sum(pnls) / max(total_hold_minutes / 60.0, 1e-9)
    win_rate = len(positive) / len(pnls)
    avg_win = fmean(positive) if positive else 0.0
    avg_loss = fmean(abs(value) for value in negative) if negative else 0.0
    avg_hold_minutes = fmean(trade.hold_minutes for trade in items)
    stopout_rate = sum(1 for trade in items if trade.stopout) / len(items)
    reentry_items = [trade for trade in items if trade.is_reentry]
    reentry_expectancy = sum(trade.pnl_cash for trade in reentry_items) / len(reentry_items) if reentry_items else 0.0
    active_days = len({trade.decision_ts.date().isoformat() for trade in items})
    trades_per_day = len(items) / max(active_days, 1)
    net_pnl_cash = sum(pnls)
    gross_pnl_before_cost = sum(trade.gross_pnl_cash for trade in items)
    total_fees = sum(trade.fees_paid for trade in items)
    total_slippage_cost = sum(trade.slippage_cost for trade in items)
    by_session = _bucket_summary(items, key_name="session_segment")
    by_regime = _bucket_summary(items, key_name="regime_bucket")
    by_volatility = _bucket_summary(items, key_name="volatility_bucket")
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)

    return PerformanceSummary(
        trade_count=len(items),
        active_days=active_days,
        trades_per_day=trades_per_day,
        expectancy=expectancy,
        expectancy_per_hour=expectancy_per_hour,
        profit_factor=(gross_profit / gross_loss) if gross_loss > 0 else gross_profit,
        max_drawdown=abs(max_drawdown),
        win_rate=win_rate,
        avg_win=avg_win,
        avg_loss=avg_loss,
        avg_hold_minutes=avg_hold_minutes,
        stopout_rate=stopout_rate,
        reentry_trade_count=len(reentry_items),
        reentry_expectancy=reentry_expectancy,
        net_pnl_cash=net_pnl_cash,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        gross_pnl_before_cost=gross_pnl_before_cost,
        total_fees=total_fees,
        total_slippage_cost=total_slippage_cost,
        long_trade_count=sum(1 for trade in items if trade.side == "LONG"),
        short_trade_count=sum(1 for trade in items if trade.side == "SHORT"),
        by_session=by_session,
        by_regime=by_regime,
        by_volatility=by_volatility,
    )


def rank_variants_for_training(
    *,
    trades_by_variant: dict[str, list[TradeRecord]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for variant_id, trades in trades_by_variant.items():
        metrics = summarize_performance(trades)
        score = (
            metrics.expectancy_per_hour * 0.45
            + metrics.expectancy * 0.75
            + max(metrics.profit_factor - 1.0, 0.0) * 1.2
            + min(metrics.trades_per_day, 10.0) * 0.12
            - min(metrics.max_drawdown / 500.0, 1.5)
            - metrics.stopout_rate * 0.4
        )
        rows.append(
            {
                "variant_id": variant_id,
                "score": score,
                "trade_count": metrics.trade_count,
                "trades_per_day": metrics.trades_per_day,
                "expectancy": metrics.expectancy,
                "expectancy_per_hour": metrics.expectancy_per_hour,
                "profit_factor": metrics.profit_factor,
                "max_drawdown": metrics.max_drawdown,
                "stopout_rate": metrics.stopout_rate,
            }
        )
    rows.sort(key=lambda row: (row["score"], row["trade_count"]), reverse=True)
    return rows


def _simulate_variant_stream(
    *,
    decisions: list[SignalDecision],
    minute_bars: list,
    variant: PatternVariant,
    point_value: float,
    slippage_points: float,
    fee_per_trade: float,
) -> tuple[list[TradeRecord], VariantExecutionAudit]:
    trades: list[TradeRecord] = []
    minute_end_timestamps = [bar.end_ts for bar in minute_bars]
    blocked_until = None
    last_trade_by_setup: dict[str, dict[str, object]] = {}
    structural_candidates = 0
    blocked_cooldown = 0
    blocked_reset = 0
    blocked_reentry_policy = 0
    trigger_missed = 0
    trigger_survived = 0

    for decision in decisions:
        structural_candidates += 1
        if blocked_until is not None and decision.decision_ts < blocked_until:
            blocked_cooldown += 1
            continue
        previous_same_setup = last_trade_by_setup.get(decision.setup_signature)
        if previous_same_setup is not None and not variant.allow_reentry:
            blocked_reentry_policy += 1
            continue
        if (
            previous_same_setup is not None
            and decision.setup_state_signature == previous_same_setup["setup_state_signature"]
            and decision.decision_ts
            < previous_same_setup["exit_ts"] + timedelta(minutes=variant.reset_window_bars_5m * 5)
        ):
            blocked_reset += 1
            continue
        reentry_type = _classify_reentry_type(
            decision=decision,
            variant=variant,
            previous_same_setup=previous_same_setup,
        )
        if reentry_type == "LOCAL_CHURN" and variant.reentry_policy == "structural_only":
            blocked_reentry_policy += 1
            continue
        if reentry_type != "NONE" and not variant.allow_reentry:
            blocked_reentry_policy += 1
            continue
        trade = _simulate_trade(
            decision=decision,
            minute_bars=minute_bars,
            minute_end_timestamps=minute_end_timestamps,
            variant=variant,
            point_value=point_value,
            slippage_points=slippage_points,
            fee_per_trade=fee_per_trade,
            earliest_entry_ts=blocked_until or decision.decision_ts,
            reentry_type=reentry_type,
        )
        if trade is None:
            trigger_missed += 1
            continue
        trades.append(trade)
        trigger_survived += 1
        blocked_until = trade.exit_ts + timedelta(minutes=variant.local_cooldown_bars_1m)
        last_trade_by_setup[decision.setup_signature] = {
            "exit_ts": trade.exit_ts,
            "setup_state_signature": decision.setup_state_signature,
            "exit_reason": trade.exit_reason,
            "decision_ts": decision.decision_ts,
        }
    return trades, VariantExecutionAudit(
        instrument=decisions[0].instrument if decisions else "",
        variant_id=variant.variant_id,
        family=variant.family,
        side=variant.side,
        structural_candidates=structural_candidates,
        blocked_cooldown=blocked_cooldown,
        blocked_reset=blocked_reset,
        blocked_reentry_policy=blocked_reentry_policy,
        trigger_missed=trigger_missed,
        trigger_survived=trigger_survived,
        executed=len(trades),
    )


def _simulate_trade(
    *,
    decision: SignalDecision,
    minute_bars: list,
    minute_end_timestamps: list,
    variant: PatternVariant,
    point_value: float,
    slippage_points: float,
    fee_per_trade: float,
    earliest_entry_ts,
    reentry_type: str,
) -> TradeRecord | None:
    search_ts = max(decision.decision_ts, earliest_entry_ts)
    start_index = bisect_right(minute_end_timestamps, search_ts)
    if start_index >= len(minute_bars):
        return None
    entry_window_bars = minute_bars[start_index : start_index + variant.entry_window_bars_1m]
    entry_window = [(start_index + offset, bar) for offset, bar in enumerate(entry_window_bars)]
    if not entry_window:
        return None

    trigger_price = _entry_trigger_price(decision=decision, variant=variant)
    entry_index = None
    entry_bar = None
    for index, bar in entry_window:
        if decision.side == "LONG" and bar.high >= trigger_price:
            entry_index = index
            entry_bar = bar
            break
        if decision.side == "SHORT" and bar.low <= trigger_price:
            entry_index = index
            entry_bar = bar
            break
    if entry_bar is None or entry_index is None:
        return None
    execution_window_bars = minute_bars[entry_index : entry_index + variant.max_hold_bars_1m + 1]
    execution_window = [(entry_index + offset, bar) for offset, bar in enumerate(execution_window_bars)]

    risk = max(decision.average_range * variant.stop_atr_multiple, 0.25)
    if decision.side == "LONG":
        raw_entry_price = max(entry_bar.open, trigger_price)
        entry_price = raw_entry_price + slippage_points
        stop_price = decision.decision_bar_low - risk
        target_price = entry_price + risk * variant.target_r_multiple if variant.target_r_multiple is not None else None
    else:
        raw_entry_price = min(entry_bar.open, trigger_price)
        entry_price = raw_entry_price - slippage_points
        stop_price = decision.decision_bar_high + risk
        target_price = entry_price - risk * variant.target_r_multiple if variant.target_r_multiple is not None else None

    post_entry_window = [(index, bar) for index, bar in execution_window if index >= entry_index]
    if not post_entry_window:
        return None

    exit_bar = post_entry_window[-1][1]
    raw_exit_price = exit_bar.close
    exit_price = exit_bar.close
    exit_reason = "time_stop"
    mfe_points = 0.0
    mae_points = 0.0
    bars_held = 0

    for relative_index, (_, bar) in enumerate(post_entry_window, start=1):
        bars_held = relative_index
        if decision.side == "LONG":
            mfe_points = max(mfe_points, bar.high - raw_entry_price)
            mae_points = max(mae_points, raw_entry_price - bar.low)
            stop_hit = bar.low <= stop_price
            target_hit = target_price is not None and bar.high >= target_price
            if stop_hit and target_hit:
                exit_bar = bar
                raw_exit_price = stop_price
                exit_price = stop_price - slippage_points
                exit_reason = "stop_first_conflict"
                break
            if stop_hit:
                exit_bar = bar
                raw_exit_price = stop_price
                exit_price = stop_price - slippage_points
                exit_reason = "stop"
                break
            if target_hit and target_price is not None:
                exit_bar = bar
                raw_exit_price = target_price
                exit_price = target_price - slippage_points
                exit_reason = "target"
                break
        else:
            mfe_points = max(mfe_points, raw_entry_price - bar.low)
            mae_points = max(mae_points, bar.high - raw_entry_price)
            stop_hit = bar.high >= stop_price
            target_hit = target_price is not None and bar.low <= target_price
            if stop_hit and target_hit:
                exit_bar = bar
                raw_exit_price = stop_price
                exit_price = stop_price + slippage_points
                exit_reason = "stop_first_conflict"
                break
            if stop_hit:
                exit_bar = bar
                raw_exit_price = stop_price
                exit_price = stop_price + slippage_points
                exit_reason = "stop"
                break
            if target_hit and target_price is not None:
                exit_bar = bar
                raw_exit_price = target_price
                exit_price = target_price + slippage_points
                exit_reason = "target"
                break

    if exit_reason == "time_stop":
        raw_exit_price = exit_bar.close
        if decision.side == "LONG":
            exit_price = exit_bar.close - slippage_points
        else:
            exit_price = exit_bar.close + slippage_points

    gross_pnl_points = raw_exit_price - raw_entry_price if decision.side == "LONG" else raw_entry_price - raw_exit_price
    pnl_points = exit_price - entry_price if decision.side == "LONG" else entry_price - exit_price
    gross_pnl_cash = gross_pnl_points * point_value
    slippage_cost = max(gross_pnl_cash - pnl_points * point_value, 0.0)
    pnl_cash = pnl_points * point_value - fee_per_trade
    stopout = exit_reason in {"stop", "stop_first_conflict"}
    return TradeRecord(
        instrument=decision.instrument,
        variant_id=decision.variant_id,
        family=decision.family,
        side=decision.side,
        live_eligible=decision.live_eligible,
        shadow_only=decision.shadow_only,
        conflict_outcome=decision.conflict_outcome,
        decision_id=decision.decision_id,
        decision_ts=decision.decision_ts,
        entry_ts=entry_bar.end_ts,
        exit_ts=exit_bar.end_ts,
        entry_price=entry_price,
        exit_price=exit_price,
        stop_price=stop_price,
        target_price=target_price,
        pnl_points=pnl_points,
        gross_pnl_cash=gross_pnl_cash,
        pnl_cash=pnl_cash,
        fees_paid=fee_per_trade,
        slippage_cost=slippage_cost,
        mfe_points=mfe_points,
        mae_points=mae_points,
        bars_held_1m=bars_held,
        hold_minutes=float(bars_held),
        exit_reason=exit_reason,
        is_reentry=reentry_type != "NONE",
        reentry_type=reentry_type,
        stopout=stopout,
        setup_signature=decision.setup_signature,
        setup_quality_bucket=decision.setup_quality_bucket,
        session_segment=decision.session_segment,
        regime_bucket=decision.regime_bucket,
        volatility_bucket=decision.volatility_bucket,
    )


def _entry_trigger_price(*, decision: SignalDecision, variant: PatternVariant) -> float:
    reclaim_band = decision.average_range * variant.trigger_reclaim_band_multiple
    if decision.side == "LONG":
        if variant.family == "pullback_continuation":
            return max(decision.decision_bar_open, decision.decision_bar_close) - decision.average_range * 0.05 - reclaim_band * 0.5
        if variant.family == "breakout_continuation":
            return decision.decision_bar_high - reclaim_band
        if variant.family == "pause_resume":
            return decision.decision_bar_high - decision.average_range * 0.15 - reclaim_band
        return max(decision.decision_bar_open, decision.decision_bar_close) - decision.average_range * 0.05 - reclaim_band * 0.5
    if variant.family == "pullback_continuation":
        return min(decision.decision_bar_open, decision.decision_bar_close) + decision.average_range * 0.05 + reclaim_band * 0.5
    if variant.family == "breakout_continuation":
        return decision.decision_bar_low + reclaim_band
    if variant.family == "pause_resume":
        return decision.decision_bar_low + decision.average_range * 0.15 + reclaim_band
    return min(decision.decision_bar_open, decision.decision_bar_close) + decision.average_range * 0.05 + reclaim_band * 0.5


def _classify_reentry_type(
    *,
    decision: SignalDecision,
    variant: PatternVariant,
    previous_same_setup: dict[str, object] | None,
) -> str:
    if previous_same_setup is None or not variant.allow_reentry:
        return "NONE"
    previous_exit_ts = previous_same_setup["exit_ts"]
    previous_state_signature = previous_same_setup["setup_state_signature"]
    previous_exit_reason = str(previous_same_setup["exit_reason"])
    minutes_since_exit = max((decision.decision_ts - previous_exit_ts).total_seconds() / 60.0, 0.0)
    if (
        decision.setup_state_signature != previous_state_signature
        and minutes_since_exit >= max(variant.local_cooldown_bars_1m, 0)
    ):
        return "STRUCTURAL_RESET"
    if previous_exit_reason in {"stop", "stop_first_conflict"} and minutes_since_exit <= 10.0:
        return "LOCAL_CHURN"
    return "LOCAL_CHURN"


def _bucket_summary(trades: list[TradeRecord], *, key_name: str) -> dict[str, dict[str, float]]:
    buckets: dict[str, list[TradeRecord]] = defaultdict(list)
    for trade in trades:
        buckets[str(getattr(trade, key_name))].append(trade)
    summary: dict[str, dict[str, float]] = {}
    for key, bucket in buckets.items():
        pnl_values = [trade.pnl_cash for trade in bucket]
        active_days = len({trade.decision_ts.date().isoformat() for trade in bucket})
        summary[key] = {
            "trade_count": float(len(bucket)),
            "trades_per_day": len(bucket) / max(active_days, 1),
            "expectancy": sum(pnl_values) / len(bucket),
            "win_rate": sum(1 for value in pnl_values if value > 0) / len(bucket),
        }
    return summary
