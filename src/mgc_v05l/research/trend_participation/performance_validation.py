"""Replay/paper ATP performance validation artifacts."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Sequence

from .models import AtpEntryState, AtpTimingState, TradeRecord
from .phase2_continuation import ATP_V1_LONG_CONTINUATION_FAMILY, ENTRY_ELIGIBLE

METRICS_INCLUDED = (
    "total_trades",
    "total_longs",
    "total_shorts",
    "winners",
    "losers",
    "win_rate",
    "gross_profit",
    "gross_loss",
    "net_pnl_cash",
    "profit_factor",
    "average_trade_pnl_cash",
    "average_winner_pnl_cash",
    "average_loser_pnl_cash",
    "max_drawdown",
    "max_run_up",
    "average_bars_in_trade",
    "average_hold_minutes",
    "average_favorable_excursion_points",
    "average_adverse_excursion_points",
    "median_favorable_excursion_points",
    "median_adverse_excursion_points",
    "entries_per_100_bars",
)


def build_atp_performance_validation_report(
    *,
    bar_count: int,
    entry_states: Sequence[AtpEntryState],
    timing_states: Sequence[AtpTimingState],
    atp_trades: Sequence[TradeRecord],
    legacy_proxy_trades: Sequence[TradeRecord],
) -> dict[str, Any]:
    enriched_atp_trades = enrich_atp_trades(
        trades=atp_trades,
        entry_states=entry_states,
        timing_states=timing_states,
    )
    near_miss_rows = [
        state
        for state in timing_states
        if state.context_entry_state == ENTRY_ELIGIBLE and not state.entry_executed
    ]
    atp_summary = _trade_metrics(enriched_atp_trades, bar_count=bar_count)
    legacy_summary = _trade_metrics(legacy_proxy_trades, bar_count=bar_count)
    return {
        "family_name": ATP_V1_LONG_CONTINUATION_FAMILY,
        "metrics_included": list(METRICS_INCLUDED),
        "metrics_omitted": [],
        "support_notes": [
            "Near-miss blocker analysis is frequency-only because blocked rows do not create executed trades.",
        ],
        "atp_phase3_performance": atp_summary,
        "segment_dimensions": [
            "bias_state",
            "pullback_state",
            "timing_state",
            "vwap_price_quality_state",
            "session_segment",
            "entry_family",
        ],
        "segment_breakdowns": {
            "by_bias_state": segment_trade_metrics(enriched_atp_trades, key_name="bias_state", bar_count=bar_count),
            "by_pullback_state": segment_trade_metrics(enriched_atp_trades, key_name="pullback_state", bar_count=bar_count),
            "by_timing_state": segment_trade_metrics(enriched_atp_trades, key_name="timing_state", bar_count=bar_count),
            "by_vwap_price_quality_state": segment_trade_metrics(
                enriched_atp_trades,
                key_name="vwap_price_quality_state",
                bar_count=bar_count,
            ),
            "by_session_segment": segment_trade_metrics(enriched_atp_trades, key_name="session_segment", bar_count=bar_count),
            "by_entry_family": segment_trade_metrics(enriched_atp_trades, key_name="family", bar_count=bar_count),
        },
        "near_miss_breakdown": _near_miss_breakdown(near_miss_rows),
        "trade_distribution_diagnostics": summarize_trade_distribution_diagnostics(enriched_atp_trades),
        "same_window_comparison": {
            "atp_phase3": _comparison_metrics(atp_summary),
            "legacy_replay_proxy": _comparison_metrics(legacy_summary),
            "delta": _comparison_delta(atp_summary, legacy_summary),
        },
    }


def render_atp_performance_validation_markdown(payload: dict[str, Any]) -> str:
    atp = dict(payload.get("atp_phase3_performance") or {})
    comparison = dict(payload.get("same_window_comparison") or {})
    delta = dict(comparison.get("delta") or {})
    vwap_rows = list((payload.get("segment_breakdowns") or {}).get("by_vwap_price_quality_state") or [])
    session_rows = list((payload.get("segment_breakdowns") or {}).get("by_session_segment") or [])
    blockers = list((payload.get("near_miss_breakdown") or {}).get("top_blockers") or [])
    lines = [
        "# ATP Performance Validation",
        "",
        f"- Trades: `{atp.get('total_trades')}`",
        f"- Win rate: `{atp.get('win_rate')}`",
        f"- Net P/L: `{atp.get('net_pnl_cash')}`",
        f"- Profit factor: `{atp.get('profit_factor')}`",
        f"- Avg trade: `{atp.get('average_trade_pnl_cash')}`",
        f"- Max drawdown / run-up: `{atp.get('max_drawdown')}` / `{atp.get('max_run_up')}`",
        "",
        "## Same-Window Comparison",
        f"- Trade count delta vs legacy replay proxy: `{delta.get('trade_count_delta')}`",
        f"- Net P/L delta vs legacy replay proxy: `{delta.get('net_pnl_cash_delta')}`",
        f"- Profit factor delta vs legacy replay proxy: `{delta.get('profit_factor_delta')}`",
        f"- Entries per 100 bars delta vs legacy replay proxy: `{delta.get('entries_per_100_bars_delta')}`",
        "",
        "## VWAP Segments",
    ]
    if not vwap_rows:
        lines.append("- No executed ATP trades were available for VWAP segmentation.")
    else:
        for row in vwap_rows:
            lines.append(
                f"- `{row['segment']}` trades=`{row['total_trades']}` win_rate=`{row['win_rate']}` "
                f"net_pnl=`{row['net_pnl_cash']}` avg_trade=`{row['average_trade_pnl_cash']}` pf=`{row['profit_factor']}`"
            )
    lines.extend(["", "## Session Segments"])
    if not session_rows:
        lines.append("- No executed ATP trades were available for session segmentation.")
    else:
        for row in session_rows:
            lines.append(
                f"- `{row['segment']}` trades=`{row['total_trades']}` net_pnl=`{row['net_pnl_cash']}` "
                f"win_rate=`{row['win_rate']}` pf=`{row['profit_factor']}`"
            )
    lines.extend(["", "## Near-Miss Blockers"])
    if not blockers:
        lines.append("- No near-miss blockers were recorded.")
    else:
        for row in blockers:
            lines.append(f"- `{row['code']}` count=`{row['count']}` percent=`{row['percent_of_near_misses']}`")
    return "\n".join(lines) + "\n"


def write_atp_performance_validation_artifacts(
    *,
    reports_dir: Path,
    payload: dict[str, Any],
) -> tuple[Path, Path]:
    json_path = reports_dir / "atp_phase3_performance_validation.json"
    markdown_path = reports_dir / "atp_phase3_performance_validation.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(render_atp_performance_validation_markdown(payload), encoding="utf-8")
    return json_path, markdown_path


def enrich_atp_trades(
    *,
    trades: Sequence[TradeRecord],
    entry_states: Sequence[AtpEntryState],
    timing_states: Sequence[AtpTimingState],
) -> list[dict[str, Any]]:
    entry_by_key = {(state.instrument, state.decision_ts): state for state in entry_states}
    timing_by_key = {(state.instrument, state.decision_ts): state for state in timing_states}
    rows: list[dict[str, Any]] = []
    for trade in trades:
        key = (trade.instrument, trade.decision_ts)
        entry_state = entry_by_key.get(key)
        timing_state = timing_by_key.get(key)
        rows.append(
            {
                "instrument": trade.instrument,
                "decision_ts": trade.decision_ts,
                "entry_ts": trade.entry_ts,
                "exit_ts": trade.exit_ts,
                "family": trade.family,
                "variant_id": trade.variant_id,
                "side": trade.side,
                "session_segment": trade.session_segment,
                "pnl_cash": trade.pnl_cash,
                "gross_pnl_cash": trade.gross_pnl_cash,
                "hold_minutes": trade.hold_minutes,
                "bars_held_1m": trade.bars_held_1m,
                "mfe_points": trade.mfe_points,
                "mae_points": trade.mae_points,
                "bias_state": entry_state.bias_state if entry_state is not None else None,
                "pullback_state": entry_state.pullback_state if entry_state is not None else None,
                "timing_state": timing_state.timing_state if timing_state is not None else None,
                "vwap_price_quality_state": (
                    timing_state.vwap_price_quality_state if timing_state is not None else None
                ),
            }
        )
    return rows


def _trade_metrics(trades: Sequence[Any], *, bar_count: int) -> dict[str, Any]:
    items = list(trades)
    positive = [_float_value(item, "pnl_cash") for item in items if _float_value(item, "pnl_cash") > 0.0]
    negative = [_float_value(item, "pnl_cash") for item in items if _float_value(item, "pnl_cash") < 0.0]
    net_pnl_cash = round(sum(_float_value(item, "pnl_cash") for item in items), 4)
    gross_profit = round(sum(positive), 4)
    gross_loss = round(abs(sum(negative)), 4)
    average_trade = round(net_pnl_cash / len(items), 4) if items else 0.0
    average_winner = round(sum(positive) / len(positive), 4) if positive else 0.0
    average_loser = round(abs(sum(negative)) / len(negative), 4) if negative else 0.0
    equity = 0.0
    peak = 0.0
    trough = 0.0
    max_drawdown = 0.0
    max_run_up = 0.0
    for item in sorted(items, key=lambda row: _value(row, "entry_ts")):
        equity += _float_value(item, "pnl_cash")
        peak = max(peak, equity)
        trough = min(trough, equity)
        max_drawdown = max(max_drawdown, peak - equity)
        max_run_up = max(max_run_up, equity - trough)
    mfe_values = [_float_value(item, "mfe_points") for item in items]
    mae_values = [_float_value(item, "mae_points") for item in items]
    return {
        "total_trades": len(items),
        "total_longs": sum(1 for item in items if str(_value(item, "side")) == "LONG"),
        "total_shorts": sum(1 for item in items if str(_value(item, "side")) == "SHORT"),
        "winners": len(positive),
        "losers": len(negative),
        "win_rate": round((len(positive) / len(items)) * 100.0, 4) if items else 0.0,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "net_pnl_cash": net_pnl_cash,
        "profit_factor": round((gross_profit / gross_loss), 4) if gross_loss > 0 else round(gross_profit, 4),
        "average_trade_pnl_cash": average_trade,
        "average_winner_pnl_cash": average_winner,
        "average_loser_pnl_cash": average_loser,
        "max_drawdown": round(max_drawdown, 4),
        "max_run_up": round(max_run_up, 4),
        "average_bars_in_trade": round(sum(int(_value(item, "bars_held_1m") or 0) for item in items) / len(items), 4) if items else 0.0,
        "average_hold_minutes": round(sum(_float_value(item, "hold_minutes") for item in items) / len(items), 4) if items else 0.0,
        "average_favorable_excursion_points": round(sum(mfe_values) / len(mfe_values), 4) if mfe_values else 0.0,
        "average_adverse_excursion_points": round(sum(mae_values) / len(mae_values), 4) if mae_values else 0.0,
        "median_favorable_excursion_points": round(median(mfe_values), 4) if mfe_values else None,
        "median_adverse_excursion_points": round(median(mae_values), 4) if mae_values else None,
        "entries_per_100_bars": round((len(items) / bar_count) * 100.0, 4) if bar_count > 0 else 0.0,
    }


def segment_trade_metrics(
    trades: Sequence[dict[str, Any]],
    *,
    key_name: str,
    bar_count: int,
) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        buckets[str(trade.get(key_name) or "UNAVAILABLE")].append(trade)
    rows = []
    for key, bucket in buckets.items():
        metrics = _trade_metrics(bucket, bar_count=bar_count)
        rows.append(
            {
                "segment": key,
                **metrics,
            }
        )
    rows.sort(key=lambda row: (-row["total_trades"], str(row["segment"])))
    return rows


def _near_miss_breakdown(timing_states: Sequence[AtpTimingState]) -> dict[str, Any]:
    blocker_counter = Counter(
        state.primary_blocker
        for state in timing_states
        if state.primary_blocker
    )
    session_counter = Counter(
        state.session_segment
        for state in timing_states
        if state.primary_blocker
    )
    total = len(timing_states)
    return {
        "near_miss_count": total,
        "top_blockers": [
            {
                "code": str(code),
                "count": count,
                "percent_of_near_misses": _percent(count, total),
            }
            for code, count in blocker_counter.most_common(8)
        ],
        "by_session_segment": [
            {
                "session_segment": str(code),
                "count": count,
                "percent_of_near_misses": _percent(count, total),
            }
            for code, count in session_counter.most_common()
        ],
    }


def summarize_trade_distribution_diagnostics(trades: Sequence[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(trades, key=lambda trade: trade["entry_ts"])
    entry_gaps = [
        round((ordered[index]["entry_ts"] - ordered[index - 1]["entry_ts"]).total_seconds() / 60.0, 4)
        for index in range(1, len(ordered))
    ]
    session_buckets: dict[tuple[date, str], int] = Counter(
        (trade["decision_ts"].date(), str(trade.get("session_segment") or "UNKNOWN"))
        for trade in ordered
    )
    max_loss_streak = 0
    current_loss_streak = 0
    loss_streaks: list[int] = []
    for trade in ordered:
        if float(trade["pnl_cash"]) < 0.0:
            current_loss_streak += 1
            max_loss_streak = max(max_loss_streak, current_loss_streak)
        elif current_loss_streak > 0:
            loss_streaks.append(current_loss_streak)
            current_loss_streak = 0
    if current_loss_streak > 0:
        loss_streaks.append(current_loss_streak)
    return {
        "trade_count": len(ordered),
        "max_trades_in_single_session_bucket": max(session_buckets.values(), default=0),
        "session_buckets_with_2_plus_trades": sum(1 for count in session_buckets.values() if count >= 2),
        "session_buckets_with_3_plus_trades": sum(1 for count in session_buckets.values() if count >= 3),
        "max_consecutive_losses": max_loss_streak,
        "loss_streak_count": len(loss_streaks),
        "average_loss_streak_length": round(sum(loss_streaks) / len(loss_streaks), 4) if loss_streaks else 0.0,
        "time_between_trades_minutes": {
            "average": round(sum(entry_gaps) / len(entry_gaps), 4) if entry_gaps else None,
            "median": round(median(entry_gaps), 4) if entry_gaps else None,
            "minimum": round(min(entry_gaps), 4) if entry_gaps else None,
            "maximum": round(max(entry_gaps), 4) if entry_gaps else None,
        },
        "entries_by_session_segment": [
            {
                "session_segment": segment,
                "count": count,
                "percent_of_trades": _percent(count, len(ordered)),
            }
            for segment, count in Counter(
                str(trade.get("session_segment") or "UNKNOWN") for trade in ordered
            ).most_common()
        ],
    }


def _comparison_metrics(summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "trade_count": summary.get("total_trades", 0),
        "net_pnl_cash": summary.get("net_pnl_cash", 0.0),
        "profit_factor": summary.get("profit_factor", 0.0),
        "max_drawdown": summary.get("max_drawdown", 0.0),
        "win_rate": summary.get("win_rate", 0.0),
        "average_trade_pnl_cash": summary.get("average_trade_pnl_cash", 0.0),
        "entries_per_100_bars": summary.get("entries_per_100_bars", 0.0),
    }


def _comparison_delta(atp_summary: dict[str, Any], legacy_summary: dict[str, Any]) -> dict[str, Any]:
    atp = _comparison_metrics(atp_summary)
    legacy = _comparison_metrics(legacy_summary)
    return {
        "trade_count_delta": atp["trade_count"] - legacy["trade_count"],
        "net_pnl_cash_delta": round(atp["net_pnl_cash"] - legacy["net_pnl_cash"], 4),
        "profit_factor_delta": round(atp["profit_factor"] - legacy["profit_factor"], 4),
        "max_drawdown_delta": round(atp["max_drawdown"] - legacy["max_drawdown"], 4),
        "win_rate_delta": round(atp["win_rate"] - legacy["win_rate"], 4),
        "average_trade_pnl_cash_delta": round(
            atp["average_trade_pnl_cash"] - legacy["average_trade_pnl_cash"],
            4,
        ),
        "entries_per_100_bars_delta": round(
            atp["entries_per_100_bars"] - legacy["entries_per_100_bars"],
            4,
        ),
    }


def _percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 4)


def _value(item: Any, key_name: str) -> Any:
    if isinstance(item, dict):
        return item.get(key_name)
    return getattr(item, key_name)


def _float_value(item: Any, key_name: str) -> float:
    return float(_value(item, key_name) or 0.0)
