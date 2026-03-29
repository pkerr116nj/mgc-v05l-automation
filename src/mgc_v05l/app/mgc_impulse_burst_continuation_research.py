"""Research-only first pass for an MGC impulse burst continuation family."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
import statistics
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .session_phase_labels import label_session_phase


REPO_ROOT = Path(__file__).resolve().parents[3]
REPLAY_DB_PATH = REPO_ROOT / "mgc_v05l.replay.sqlite3"
OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "approved_branch_research"
PORTABILITY_AUDIT_PATH = OUTPUT_DIR / "approved_branch_futures_portability_audit.json"
POINT_VALUE = 10.0
ENTRY_OVERLAP_WINDOW_MINUTES = 30
MAX_HOLD_BARS = 8
PULLBACK_LOOKAHEAD_BARS = 4
COMMON_SYMBOL = "MGC"
COMMON_DETECTION_TIMEFRAME = "1m"
COMMON_CONTEXT_TIMEFRAME = "5m"
COMMON_WINDOW_DESCRIPTION = "FULL_OVERLAP_OF_AVAILABLE_1M_AND_5M_MGC_HISTORY"


@dataclass(frozen=True)
class Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class CandidateSpec:
    variant_name: str
    family_variant: str
    window_size: int
    require_volume_expansion: bool
    normalized_move_threshold: float = 1.35
    same_direction_share_min: float = 0.70
    body_dominance_min: float = 0.65
    path_efficiency_min: float = 0.45
    context_move_threshold: float = 0.35
    context_body_share_min: float = 2.0 / 3.0
    min_pullback_retrace: float = 0.10
    max_pullback_retrace: float = 0.35


@dataclass(frozen=True)
class Trade:
    entry_ts: str
    exit_ts: str
    direction: str
    entry_px: float
    exit_px: float
    pnl: float
    hold_bars: int
    signal_phase: str
    signal_bar_ts: str
    max_favorable_move: float
    max_adverse_move: float
    captured_move: float
    false_start: bool


CANDIDATE_SPECS: tuple[CandidateSpec, ...] = (
    CandidateSpec("direct_impulse_continuation_w5", "direct_impulse_continuation", 5, False),
    CandidateSpec("direct_impulse_continuation_w7", "direct_impulse_continuation", 7, False),
    CandidateSpec("direct_impulse_continuation_w8", "direct_impulse_continuation", 8, False),
    CandidateSpec("direct_impulse_continuation_w7_volexp", "direct_impulse_continuation", 7, True),
    CandidateSpec("shallow_pullback_continuation_w5", "shallow_pullback_continuation", 5, False),
    CandidateSpec("shallow_pullback_continuation_w7", "shallow_pullback_continuation", 7, False),
    CandidateSpec("shallow_pullback_continuation_w8", "shallow_pullback_continuation", 8, False),
    CandidateSpec("shallow_pullback_continuation_w7_volexp", "shallow_pullback_continuation", 7, True),
)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    payload = run_impulse_burst_continuation_research(symbol=args.symbol)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mgc-impulse-burst-continuation-research")
    parser.add_argument("--symbol", default=COMMON_SYMBOL)
    return parser


def run_impulse_burst_continuation_research(*, symbol: str) -> dict[str, Any]:
    one_minute_bars = _load_bars(symbol=symbol, timeframe=COMMON_DETECTION_TIMEFRAME)
    five_minute_bars = _load_bars(symbol=symbol, timeframe=COMMON_CONTEXT_TIMEFRAME)
    overlap_start = max(one_minute_bars[0].timestamp, five_minute_bars[0].timestamp)
    overlap_end = min(one_minute_bars[-1].timestamp, five_minute_bars[-1].timestamp)
    one_minute = [bar for bar in one_minute_bars if overlap_start <= bar.timestamp <= overlap_end]
    five_minute = [bar for bar in five_minute_bars if overlap_start <= bar.timestamp <= overlap_end]

    atr_1m = _rolling_atr(one_minute, length=14)
    rv_1m = _rolling_realized_vol(one_minute, length=20)
    vol_baseline_1m = _rolling_mean([bar.volume for bar in one_minute], length=20)
    atr_5m = _rolling_atr(five_minute, length=14)
    context_lookup = _build_latest_context_lookup(one_minute=one_minute, five_minute=five_minute)

    results = []
    for spec in CANDIDATE_SPECS:
        result = _evaluate_candidate(
            bars_1m=one_minute,
            bars_5m=five_minute,
            atr_1m=atr_1m,
            rv_1m=rv_1m,
            vol_baseline_1m=vol_baseline_1m,
            atr_5m=atr_5m,
            context_lookup=context_lookup,
            spec=spec,
        )
        results.append(result)

    promoted_comparison = _load_promoted_mgc_family_comparison(
        overlap_start=overlap_start,
        overlap_end=overlap_end,
    )
    best_candidate = _pick_best_candidate(results)
    comparison_summary = _build_comparison_summary(results=results, promoted=promoted_comparison)
    overall_verdict = best_candidate["decision_bucket"] if best_candidate else "NOT_WORTH_CONTINUING"
    payload = {
        "symbol": symbol,
        "family_name": "impulse_burst_continuation",
        "timeframes": {
            "detection_surface": COMMON_DETECTION_TIMEFRAME,
            "context_surface": COMMON_CONTEXT_TIMEFRAME,
        },
        "sample_start_date": overlap_start.isoformat(),
        "sample_end_date": overlap_end.isoformat(),
        "history_window_type": COMMON_WINDOW_DESCRIPTION,
        "exact_impulse_definitions_tested": _impulse_definition_notes(),
        "candidate_variants_tested": [asdict(spec) for spec in CANDIDATE_SPECS],
        "results": results,
        "best_candidate": best_candidate,
        "comparison_vs_promoted_mgc_families": promoted_comparison,
        "comparison_summary": comparison_summary,
        "overall_family_verdict": overall_verdict,
        "family_conclusion": _family_conclusion(best_candidate=best_candidate, comparison_summary=comparison_summary),
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = OUTPUT_DIR / "mgc_impulse_burst_continuation_research.json"
    md_path = OUTPUT_DIR / "mgc_impulse_burst_continuation_research.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(_render_markdown(payload).strip() + "\n", encoding="utf-8")
    return {
        "mode": "mgc_impulse_burst_continuation_research",
        "artifact_paths": {"json": str(json_path), "markdown": str(md_path)},
        "best_candidate": {
            "variant_name": best_candidate["variant_name"] if best_candidate else None,
            "decision_bucket": best_candidate["decision_bucket"] if best_candidate else None,
        },
        "overall_family_verdict": overall_verdict,
    }


def _load_bars(*, symbol: str, timeframe: str) -> list[Bar]:
    connection = sqlite3.connect(REPLAY_DB_PATH)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            select timestamp, open, high, low, close, volume
            from bars
            where ticker = ? and timeframe = ?
            order by timestamp asc
            """,
            (symbol, timeframe),
        ).fetchall()
    finally:
        connection.close()
    return [
        Bar(
            timestamp=datetime.fromisoformat(str(row["timestamp"])),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"] or 0.0),
        )
        for row in rows
    ]


def _rolling_atr(bars: list[Bar], *, length: int) -> list[float | None]:
    values: list[float | None] = [None] * len(bars)
    true_ranges: list[float] = []
    for index, bar in enumerate(bars):
        prev_close = bars[index - 1].close if index > 0 else bar.close
        true_range = max(bar.high - bar.low, abs(bar.high - prev_close), abs(bar.low - prev_close))
        true_ranges.append(true_range)
        if index >= length - 1:
            values[index] = statistics.fmean(true_ranges[index - length + 1 : index + 1])
    return values


def _rolling_realized_vol(bars: list[Bar], *, length: int) -> list[float | None]:
    values: list[float | None] = [None] * len(bars)
    deltas = [0.0]
    for index in range(1, len(bars)):
        deltas.append(bars[index].close - bars[index - 1].close)
    for index in range(length - 1, len(bars)):
        window = deltas[index - length + 1 : index + 1]
        if len(window) < 2:
            continue
        values[index] = statistics.stdev(window)
    return values


def _rolling_mean(values: list[float], *, length: int) -> list[float | None]:
    result: list[float | None] = [None] * len(values)
    for index in range(length - 1, len(values)):
        result[index] = statistics.fmean(values[index - length + 1 : index + 1])
    return result


def _build_latest_context_lookup(*, one_minute: list[Bar], five_minute: list[Bar]) -> list[int | None]:
    lookup: list[int | None] = [None] * len(one_minute)
    five_index = 0
    latest: int | None = None
    for index, bar in enumerate(one_minute):
        while five_index < len(five_minute) and five_minute[five_index].timestamp <= bar.timestamp:
            latest = five_index
            five_index += 1
        lookup[index] = latest
    return lookup


def _evaluate_candidate(
    *,
    bars_1m: list[Bar],
    bars_5m: list[Bar],
    atr_1m: list[float | None],
    rv_1m: list[float | None],
    vol_baseline_1m: list[float | None],
    atr_5m: list[float | None],
    context_lookup: list[int | None],
    spec: CandidateSpec,
) -> dict[str, Any]:
    raw_events: list[dict[str, Any]] = []
    filtered_events: list[dict[str, Any]] = []
    trades: list[Trade] = []
    bars_scanned = len(bars_1m)
    index = max(spec.window_size, 20)
    next_available_index = index
    while index < len(bars_1m) - MAX_HOLD_BARS - 1:
        if index < next_available_index:
            index += 1
            continue
        impulse = _detect_impulse_event(
            bars=bars_1m,
            atr_1m=atr_1m,
            rv_1m=rv_1m,
            vol_baseline_1m=vol_baseline_1m,
            index=index,
            spec=spec,
        )
        if impulse is None:
            index += 1
            continue
        raw_events.append(impulse)
        if not _passes_context_filter(
            bars_5m=bars_5m,
            atr_5m=atr_5m,
            context_lookup=context_lookup,
            one_minute_index=index,
            direction=impulse["direction"],
            threshold=spec.context_move_threshold,
            body_share_min=spec.context_body_share_min,
        ):
            index += spec.window_size
            continue
        if spec.family_variant == "direct_impulse_continuation":
            trade = _build_direct_trade(bars=bars_1m, signal_index=index, impulse=impulse)
        else:
            trade = _build_shallow_pullback_trade(bars=bars_1m, signal_index=index, impulse=impulse, spec=spec)
        if trade is not None:
            filtered_events.append(impulse)
            trades.append(trade)
            exit_index = _find_bar_index_by_timestamp(bars_1m, trade.exit_ts)
            next_available_index = max(index + spec.window_size, exit_index + 1)
        else:
            next_available_index = index + spec.window_size
        index += spec.window_size

    metrics = _trade_metrics(trades)
    time_distribution = Counter(trade.signal_phase for trade in trades)
    promoted_overlap = _promoted_overlap_signature(trades)
    decision_bucket = _decision_bucket(
        raw_events=len(raw_events),
        filtered_events=len(filtered_events),
        trades=trades,
        metrics=metrics,
    )
    return {
        "variant_name": spec.variant_name,
        "family_variant": spec.family_variant,
        "window_size": spec.window_size,
        "require_volume_expansion": spec.require_volume_expansion,
        "bars_scanned": bars_scanned,
        "raw_impulse_events": len(raw_events),
        "post_filter_events": len(filtered_events),
        "trades": len(trades),
        "realized_pnl": metrics["realized_pnl"],
        "avg_trade": metrics["avg_trade"],
        "median_trade": metrics["median_trade"],
        "profit_factor": metrics["profit_factor"],
        "max_drawdown": metrics["max_drawdown"],
        "top_1_trade_contribution": metrics["top_1_trade_contribution"],
        "top_3_trade_contribution": metrics["top_3_trade_contribution"],
        "survives_without_top_1": metrics["survives_without_top_1"],
        "survives_without_top_3": metrics["survives_without_top_3"],
        "time_of_day_concentration": {
            "distribution": dict(sorted(time_distribution.items())),
            "dominant_phase": _dominant_bucket_from_counter(time_distribution)[0],
            "dominant_phase_share": _dominant_bucket_from_counter(time_distribution)[1],
        },
        "average_move_captured_after_signal": metrics["average_move_captured_after_signal"],
        "false_start_rate": metrics["false_start_rate"],
        "average_bars_held": metrics["average_bars_held"],
        "decision_bucket": decision_bucket,
        "promoted_family_overlap_signature": promoted_overlap,
    }


def _detect_impulse_event(
    *,
    bars: list[Bar],
    atr_1m: list[float | None],
    rv_1m: list[float | None],
    vol_baseline_1m: list[float | None],
    index: int,
    spec: CandidateSpec,
) -> dict[str, Any] | None:
    start = index - spec.window_size + 1
    if start < 0:
        return None
    window = bars[start : index + 1]
    if len(window) != spec.window_size:
        return None
    atr_value = atr_1m[index]
    rv_value = rv_1m[index]
    vol_baseline = vol_baseline_1m[index]
    if atr_value is None or rv_value is None or vol_baseline is None or vol_baseline <= 0:
        return None
    signed_net = window[-1].close - window[0].open
    direction = "LONG" if signed_net > 0 else "SHORT"
    if signed_net == 0:
        return None
    direction_sign = 1.0 if direction == "LONG" else -1.0
    bodies = [bar.close - bar.open for bar in window]
    same_direction_share = sum(1 for body in bodies if direction_sign * body > 0) / len(window)
    body_total = sum(abs(body) for body in bodies)
    if body_total <= 0:
        return None
    body_dominance = sum(max(direction_sign * body, 0.0) for body in bodies) / body_total
    path_length = body_total
    path_efficiency = abs(signed_net) / path_length if path_length else 0.0
    scale = max(atr_value * math.sqrt(spec.window_size), rv_value * math.sqrt(spec.window_size), 0.1)
    normalized_move = abs(signed_net) / scale
    window_volume = statistics.fmean(bar.volume for bar in window)
    volume_ratio = window_volume / vol_baseline if vol_baseline > 0 else 0.0
    if same_direction_share < spec.same_direction_share_min:
        return None
    if body_dominance < spec.body_dominance_min:
        return None
    if path_efficiency < spec.path_efficiency_min:
        return None
    if normalized_move < spec.normalized_move_threshold:
        return None
    if spec.require_volume_expansion and volume_ratio < 1.15:
        return None
    return {
        "signal_ts": window[-1].timestamp.isoformat(),
        "signal_phase": label_session_phase(window[-1].timestamp),
        "direction": direction,
        "burst_size_points": abs(signed_net),
        "normalized_move": normalized_move,
        "same_direction_share": same_direction_share,
        "body_dominance": body_dominance,
        "path_efficiency": path_efficiency,
        "volume_ratio": volume_ratio,
    }


def _passes_context_filter(
    *,
    bars_5m: list[Bar],
    atr_5m: list[float | None],
    context_lookup: list[int | None],
    one_minute_index: int,
    direction: str,
    threshold: float,
    body_share_min: float,
) -> bool:
    latest_five_index = context_lookup[one_minute_index]
    if latest_five_index is None or latest_five_index < 2:
        return False
    if atr_5m[latest_five_index] is None:
        return False
    recent = bars_5m[latest_five_index - 2 : latest_five_index + 1]
    if len(recent) != 3:
        return False
    direction_sign = 1.0 if direction == "LONG" else -1.0
    context_net = direction_sign * (recent[-1].close - recent[0].open)
    same_body_share = sum(1 for bar in recent if direction_sign * (bar.close - bar.open) > 0) / len(recent)
    scale = max(float(atr_5m[latest_five_index]) * math.sqrt(3.0), 0.1)
    normalized_context = context_net / scale
    return normalized_context >= threshold and same_body_share >= body_share_min


def _build_direct_trade(*, bars: list[Bar], signal_index: int, impulse: dict[str, Any]) -> Trade | None:
    entry_index = signal_index + 1
    if entry_index >= len(bars):
        return None
    direction = str(impulse["direction"])
    direction_sign = 1.0 if direction == "LONG" else -1.0
    entry_bar = bars[entry_index]
    exit_index = min(entry_index + MAX_HOLD_BARS - 1, len(bars) - 1)
    adverse_streak = 0
    chosen_exit = exit_index
    for index in range(entry_index, exit_index + 1):
        body = direction_sign * (bars[index].close - bars[index].open)
        if body < 0:
            adverse_streak += 1
        else:
            adverse_streak = 0
        if adverse_streak >= 2:
            chosen_exit = index
            break
    return _finalize_trade(
        bars=bars,
        entry_index=entry_index,
        exit_index=chosen_exit,
        direction=direction,
        signal_phase=str(impulse["signal_phase"]),
        signal_bar_ts=str(impulse["signal_ts"]),
    )


def _build_shallow_pullback_trade(
    *,
    bars: list[Bar],
    signal_index: int,
    impulse: dict[str, Any],
    spec: CandidateSpec,
) -> Trade | None:
    direction = str(impulse["direction"])
    direction_sign = 1.0 if direction == "LONG" else -1.0
    burst_size = float(impulse["burst_size_points"])
    signal_close = bars[signal_index].close
    pullback_seen = False
    for index in range(signal_index + 1, min(len(bars) - 1, signal_index + PULLBACK_LOOKAHEAD_BARS) + 1):
        retrace = direction_sign * (signal_close - bars[index].close)
        retrace_ratio = retrace / burst_size if burst_size > 0 else 0.0
        body = direction_sign * (bars[index].close - bars[index].open)
        if body < 0 and spec.min_pullback_retrace <= retrace_ratio <= spec.max_pullback_retrace:
            pullback_seen = True
            continue
        if pullback_seen and body > 0:
            reextension = direction_sign * (bars[index].close - bars[index - 1].high if direction == "LONG" else bars[index - 1].low - bars[index].close)
            if reextension > 0:
                return _finalize_trade(
                    bars=bars,
                    entry_index=index + 1,
                    exit_index=min(index + MAX_HOLD_BARS, len(bars) - 1),
                    direction=direction,
                    signal_phase=str(impulse["signal_phase"]),
                    signal_bar_ts=str(impulse["signal_ts"]),
                )
        if retrace_ratio > spec.max_pullback_retrace:
            return None
    return None


def _finalize_trade(
    *,
    bars: list[Bar],
    entry_index: int,
    exit_index: int,
    direction: str,
    signal_phase: str,
    signal_bar_ts: str,
) -> Trade | None:
    if entry_index >= len(bars):
        return None
    direction_sign = 1.0 if direction == "LONG" else -1.0
    exit_index = min(max(exit_index, entry_index), len(bars) - 1)
    entry_bar = bars[entry_index]
    exit_bar = bars[exit_index]
    future = bars[entry_index : exit_index + 1]
    if direction == "LONG":
        max_favorable = max(bar.high - entry_bar.open for bar in future)
        max_adverse = max(entry_bar.open - bar.low for bar in future)
    else:
        max_favorable = max(entry_bar.open - bar.low for bar in future)
        max_adverse = max(bar.high - entry_bar.open for bar in future)
    captured_move = direction_sign * (exit_bar.close - entry_bar.open)
    pnl = captured_move * POINT_VALUE
    three_bar_end = min(exit_index, entry_index + 2)
    early_window = bars[entry_index : three_bar_end + 1]
    early_move = direction_sign * (early_window[-1].close - entry_bar.open)
    false_start = early_move <= 0 and pnl <= 0
    return Trade(
        entry_ts=entry_bar.timestamp.isoformat(),
        exit_ts=exit_bar.timestamp.isoformat(),
        direction=direction,
        entry_px=entry_bar.open,
        exit_px=exit_bar.close,
        pnl=round(pnl, 4),
        hold_bars=exit_index - entry_index + 1,
        signal_phase=signal_phase,
        signal_bar_ts=signal_bar_ts,
        max_favorable_move=round(max_favorable, 4),
        max_adverse_move=round(max_adverse, 4),
        captured_move=round(captured_move, 4),
        false_start=false_start,
    )


def _find_bar_index_by_timestamp(bars: list[Bar], timestamp_text: str) -> int:
    target = datetime.fromisoformat(timestamp_text)
    for index, bar in enumerate(bars):
        if bar.timestamp == target:
            return index
    return len(bars) - 1


def _trade_metrics(trades: list[Trade]) -> dict[str, Any]:
    pnls = [trade.pnl for trade in trades]
    gross_wins = sum(value for value in pnls if value > 0)
    gross_losses = sum(value for value in pnls if value < 0)
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in pnls:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    top_1_share = _top_trade_share(pnls, top_n=1)
    top_3_share = _top_trade_share(pnls, top_n=3)
    return {
        "realized_pnl": round(sum(pnls), 4),
        "avg_trade": round(statistics.fmean(pnls), 4) if pnls else None,
        "median_trade": round(statistics.median(pnls), 4) if pnls else None,
        "profit_factor": round(gross_wins / abs(gross_losses), 4) if gross_losses < 0 else None,
        "max_drawdown": round(max_drawdown, 4),
        "top_1_trade_contribution": top_1_share,
        "top_3_trade_contribution": top_3_share,
        "survives_without_top_1": _survives_without_top(pnls, top_n=1),
        "survives_without_top_3": _survives_without_top(pnls, top_n=3),
        "average_move_captured_after_signal": round(statistics.fmean([trade.captured_move for trade in trades]), 4) if trades else None,
        "false_start_rate": round(sum(1 for trade in trades if trade.false_start) / len(trades), 4) if trades else None,
        "average_bars_held": round(statistics.fmean([trade.hold_bars for trade in trades]), 4) if trades else None,
    }


def _top_trade_share(pnls: list[float], *, top_n: int) -> float | None:
    total = sum(pnls)
    if not pnls or total == 0:
        return None
    top = sum(sorted(pnls, reverse=True)[:top_n])
    return round((top / total) * 100, 2)


def _survives_without_top(pnls: list[float], *, top_n: int) -> bool | None:
    if not pnls:
        return None
    revised = sum(pnls) - sum(sorted(pnls, reverse=True)[:top_n])
    return revised > 0


def _dominant_bucket_from_counter(counter: Counter[str]) -> tuple[str | None, float | None]:
    if not counter:
        return None, None
    total = sum(counter.values())
    bucket, count = max(counter.items(), key=lambda item: item[1])
    return bucket, round(count / total, 4) if total else None


def _decision_bucket(*, raw_events: int, filtered_events: int, trades: list[Trade], metrics: dict[str, Any]) -> str:
    if raw_events == 0:
        return "NOT_WORTH_CONTINUING"
    if trades and metrics["realized_pnl"] and metrics["realized_pnl"] > 0 and (metrics["profit_factor"] or 0) >= 1.2 and filtered_events >= 10:
        if metrics["survives_without_top_1"] and (metrics["top_3_trade_contribution"] is None or metrics["top_3_trade_contribution"] < 100):
            return "PROMISING_NEW_FAMILY"
    if filtered_events >= 8 and raw_events >= 12:
        if trades and ((metrics["profit_factor"] or 0) >= 1.0 or metrics["realized_pnl"] > 0):
            return "STRUCTURALLY_REAL_BUT_NEEDS_REFINEMENT"
    if trades and len(trades) < 5:
        return "TOO_THIN"
    if trades and ((metrics["profit_factor"] or 0) < 1.0 or (metrics["false_start_rate"] or 0) >= 0.55):
        return "TOO_NOISY"
    if filtered_events < 5:
        return "TOO_THIN"
    return "NOT_WORTH_CONTINUING"


def _load_promoted_mgc_family_comparison(*, overlap_start: datetime, overlap_end: datetime) -> dict[str, Any]:
    payload = json.loads(PORTABILITY_AUDIT_PATH.read_text(encoding="utf-8"))
    mgc_result = next(result for result in payload["results"] if result["symbol"] == "MGC")
    trade_rows = list(csv.DictReader(Path(mgc_result["artifact_paths"]["trade_ledger"]).open(encoding="utf-8")))
    summary = json.loads(Path(mgc_result["artifact_paths"]["replay_summary"]).read_text(encoding="utf-8"))
    branch_rows = {row["branch"]: row for row in mgc_result["branch_rows"]}
    rows = []
    overlap_trade_times: list[datetime] = []
    for branch, branch_row in branch_rows.items():
        branch_trades = []
        for row in trade_rows:
            if row["setup_family"] != branch:
                continue
            entry_ts = datetime.fromisoformat(row["entry_ts"])
            if overlap_start <= entry_ts <= overlap_end:
                branch_trades.append(row)
                overlap_trade_times.append(entry_ts)
        phase_counts = Counter(row["entry_session_phase"] for row in branch_trades)
        dominant_phase, dominant_share = _dominant_bucket_from_counter(phase_counts)
        pnls = [float(row["net_pnl"]) for row in branch_trades]
        rows.append(
            {
                "branch": branch,
                "trade_density": len(branch_trades),
                "avg_trade": round(statistics.fmean(pnls), 4) if pnls else None,
                "median_trade": round(statistics.median(pnls), 4) if pnls else None,
                "profit_factor": _profit_factor(pnls),
                "max_drawdown": _max_drawdown(pnls),
                "top_1_trade_contribution": _top_trade_share(pnls, top_n=1),
                "top_3_trade_contribution": _top_trade_share(pnls, top_n=3),
                "dominant_phase": dominant_phase,
                "dominant_phase_share": dominant_share,
                "avg_bars_held": round(statistics.fmean([float(row["bars_held"]) for row in branch_trades]), 4) if branch_trades else None,
                "avg_mfe_capture_pct": round(statistics.fmean([float(row["mfe_capture_pct"]) for row in branch_trades]), 4) if branch_trades else None,
                "signal_count_overlap_window": len([bar_id for bar_id in (summary.get("approved_branch_signal_bars") or {}).get(branch, []) if overlap_start <= datetime.fromisoformat(str(bar_id).split("|")[-1].replace("Z", "+00:00")).astimezone(overlap_start.tzinfo) <= overlap_end]),
            }
        )
    return {
        "common_sample_start": overlap_start.isoformat(),
        "common_sample_end": overlap_end.isoformat(),
        "rows": rows,
        "promoted_entry_timestamps": [ts.isoformat() for ts in sorted(overlap_trade_times)],
    }


def _profit_factor(pnls: list[float]) -> float | None:
    wins = sum(value for value in pnls if value > 0)
    losses = sum(value for value in pnls if value < 0)
    if losses >= 0:
        return None
    return round(wins / abs(losses), 4)


def _max_drawdown(pnls: list[float]) -> float:
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in pnls:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return round(max_drawdown, 4)


def _promoted_overlap_signature(trades: list[Trade]) -> dict[str, Any]:
    promoted = _load_promoted_mgc_family_comparison(
        overlap_start=datetime.fromisoformat(trades[0].entry_ts) if trades else datetime.fromisoformat("2026-02-03T15:59:00-05:00"),
        overlap_end=datetime.fromisoformat(trades[-1].exit_ts) if trades else datetime.fromisoformat("2026-03-17T04:30:00-04:00"),
    )
    promoted_entries = [datetime.fromisoformat(value) for value in promoted["promoted_entry_timestamps"]]
    if not trades:
        return {
            "unique_entry_share_vs_promoted_families": None,
            "non_overlapping_trade_count": 0,
            "overlap_window_minutes": ENTRY_OVERLAP_WINDOW_MINUTES,
        }
    unique = 0
    for trade in trades:
        entry_ts = datetime.fromisoformat(trade.entry_ts)
        if not any(abs((entry_ts - promoted_ts).total_seconds()) <= ENTRY_OVERLAP_WINDOW_MINUTES * 60 for promoted_ts in promoted_entries):
            unique += 1
    return {
        "unique_entry_share_vs_promoted_families": round(unique / len(trades), 4),
        "non_overlapping_trade_count": unique,
        "overlap_window_minutes": ENTRY_OVERLAP_WINDOW_MINUTES,
    }


def _pick_best_candidate(results: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not results:
        return None
    ranked = sorted(
        results,
        key=lambda row: (
            {"PROMISING_NEW_FAMILY": 4, "STRUCTURALLY_REAL_BUT_NEEDS_REFINEMENT": 3, "TOO_NOISY": 2, "TOO_THIN": 1, "NOT_WORTH_CONTINUING": 0}[row["decision_bucket"]],
            float(row["realized_pnl"] or 0.0),
            float(row["profit_factor"] or 0.0),
        ),
        reverse=True,
    )
    return ranked[0]


def _build_comparison_summary(*, results: list[dict[str, Any]], promoted: dict[str, Any]) -> dict[str, Any]:
    best = _pick_best_candidate(results)
    if best is None:
        return {}
    promoted_rows = promoted["rows"]
    best_promoted_density = max((row["trade_density"] or 0 for row in promoted_rows), default=0)
    return {
        "best_candidate_vs_promoted_trade_density": {
            "best_candidate_trades": best["trades"],
            "best_promoted_family_trades": best_promoted_density,
        },
        "captures_moves_current_families_miss": (best["promoted_family_overlap_signature"]["unique_entry_share_vs_promoted_families"] or 0) >= 0.5,
        "distinct_entry_share_best_candidate": best["promoted_family_overlap_signature"]["unique_entry_share_vs_promoted_families"],
    }


def _family_conclusion(*, best_candidate: dict[str, Any] | None, comparison_summary: dict[str, Any]) -> str:
    if best_candidate is None:
        return "No research candidate produced usable evidence in the common MGC 1m/5m overlap window."
    if best_candidate["decision_bucket"] == "PROMISING_NEW_FAMILY":
        return "The impulse_burst_continuation family looks promising enough for a second pass and appears to capture a meaningful share of entries not already covered by the promoted MGC families."
    if best_candidate["decision_bucket"] == "STRUCTURALLY_REAL_BUT_NEEDS_REFINEMENT":
        return "The family appears structurally real on MGC, but it still needs refinement before robustness work."
    if best_candidate["decision_bucket"] == "TOO_NOISY":
        return "The family is active, but the current first-pass form is too noisy to justify broad continuation yet."
    if best_candidate["decision_bucket"] == "TOO_THIN":
        return "The family barely triggers on the current MGC overlap window and is too thin for a strong conclusion."
    return "The current first-pass form is not worth continuing without a stronger causal reason."


def _impulse_definition_notes() -> dict[str, Any]:
    return {
        "rolling_windows_tested": [5, 7, 8],
        "raw_impulse_definition": {
            "directional_move": "signed close(last) - open(first) over the 1m burst window",
            "same_direction_share": "share of bars in the burst window whose bodies align with the burst direction",
            "body_dominance": "aligned body sum divided by total absolute body sum over the burst window",
            "path_efficiency": "net signed move divided by total absolute body length over the burst window",
            "normalized_move_scale": "max(1m ATR14 * sqrt(window), 1m realized-vol20 * sqrt(window))",
            "normalized_move_threshold": 1.35,
            "same_direction_share_min": 0.70,
            "body_dominance_min": 0.65,
            "path_efficiency_min": 0.45,
        },
        "five_minute_context_filter": {
            "context_window": "last 3 completed 5m bars at signal time",
            "direction_alignment": "at least 2 of 3 5m bodies align with the impulse direction",
            "normalized_context_threshold": 0.35,
        },
        "optional_volume_expansion": "window average volume divided by trailing 20-bar 1m mean volume must be >= 1.15 when enabled",
        "entry_exit_proxies": {
            "direct": "enter next 1m open after the burst; exit on two consecutive counter-direction 1m bodies or after 8 bars",
            "shallow_pullback": "after the burst, allow a 1-4 bar pullback capped at 35% retrace, then enter on 1m re-extension; exit after up to 8 bars",
        },
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# MGC Impulse Burst Continuation Research",
        "",
        f"Overall verdict: {payload['overall_family_verdict']}",
        "",
        f"- Sample: {payload['sample_start_date']} -> {payload['sample_end_date']}",
        f"- Window type: {payload['history_window_type']}",
        f"- Best candidate: {(payload['best_candidate'] or {}).get('variant_name')}",
        "",
        "## Candidate Results",
        "",
    ]
    for row in payload["results"]:
        lines.append(
            f"- {row['variant_name']}: bucket={row['decision_bucket']}, raw={row['raw_impulse_events']}, post_filter={row['post_filter_events']}, "
            f"trades={row['trades']}, pnl={row['realized_pnl']}, pf={row['profit_factor']}, dd={row['max_drawdown']}, "
            f"top1={row['top_1_trade_contribution']}, top3={row['top_3_trade_contribution']}, false_start={row['false_start_rate']}"
        )
    lines.extend(["", "## Comparison vs Promoted MGC Families", ""])
    for row in payload["comparison_vs_promoted_mgc_families"]["rows"]:
        lines.append(
            f"- {row['branch']}: trades={row['trade_density']}, avg_trade={row['avg_trade']}, pf={row['profit_factor']}, "
            f"dd={row['max_drawdown']}, dominant_phase={row['dominant_phase']} ({row['dominant_phase_share']})"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
